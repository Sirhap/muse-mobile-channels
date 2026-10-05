#!/usr/bin/env python3
"""WeCom smart-robot (API mode, long connection) gateway for Muse.

Maintains the single WebSocket long connection to
wss://openws.work.weixin.qq.com, following the official protocol in
https://developer.work.weixin.qq.com/document/path/101463 :

- subscribe with BotID + Secret (aibot_subscribe)
- app-level heartbeat ping every 30s
- incoming aibot_msg_callback messages are appended to state/inbox.jsonl
- every message gets an immediate ack reply; text "ping" is answered
  directly with "pong" by the gateway (transport self-test)
- replies / proactive messages queued in state/outbox.jsonl by the CLI
  (or the agent) are sent over the same connection:
    mode "reply" -> aibot_respond_msg using the original callback req_id
                    (valid for 24h after the callback)
    mode "send"  -> aibot_send_msg (proactive; the target chat must have
                    messaged the bot before)
- connection state is mirrored to state/status.json

Credentials come from WECOM_BOT_ID / WECOM_BOT_SECRET, or from the env
file ~/.config/wecom-bot/credentials.env (KEY=VALUE lines). The env file
is re-read on every (re)connect, so dropping credentials in place is
enough; no restart needed.
"""

import asyncio
import base64
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import websockets
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from channel_common import (  # noqa: E402
    SEND_MAX_ATTEMPTS,
    allocate_subagent_id,
    append_jsonl as append_jsonl_line,
    complete_jsonl_lines,
    load_json_dict,
    media_filename,
    merge_queue_admin,
    muse_home,
    outbound_file_allowed,
    parse_jsonl_line,
    read_offset,
    retry_backoff_secs,
    subagent_outcome_default,
    trim_mapping,
    atomic_write_text,
    write_offset,
)

WS_URL = "wss://openws.work.weixin.qq.com"
BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
MEDIA_DIR = STATE / "media"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
OUTBOX_RESULTS = STATE / "outbox_results.jsonl"
REQMAP = STATE / "reqmap.json"
CARDS = STATE / "cards.json"
STATUS = STATE / "status.json"
OUTBOX_OFFSET = STATE / "outbox.offset"
OUTBOX_RETRY = STATE / "outbox_retry.json"
OUTBOX_PARTIAL = STATE / "outbox_partial.json"
CANCELLED_FILE = STATE / "cancelled.json"
SEEN_FILE = STATE / "seen_ids.jsonl"
LOCK_FILE = STATE / "gateway.lock"
CRED_FILE = Path(
    os.environ.get("WECOM_CRED_FILE")
    or str(muse_home() / ".config" / "wecom-bot" / "credentials.env")
)

HEARTBEAT_SECS = 30
# Outbound text chunk size (chars), aligned with the OpenClaw plugin's
# TEXT_CHUNK_LIMIT: longer replies are split, first chunk closes the stream,
# the rest follow as active send_msg frames.
CHUNK_LIMIT = 4000


def sanitize_task_id(raw: str) -> str:
    """task_id: only [0-9A-Za-z_-@], max 128 bytes (official card spec)."""
    cleaned = "".join(c if (c.isalnum() and c.isascii()) or c in "_-@" else "_" for c in (raw or ""))
    return cleaned[:128] or f"task_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def build_confirm_card(title: str, desc: str, task_id: str, selection: dict | None = None) -> dict:
    """A button_interaction template card with 确认/取消 buttons.

    Clicks come back as a template_card_event with event_key
    btn_confirm / btn_cancel and this task_id (see handle_event_callback).
    `selection` ({title, options:[text,...]}) adds the optional dropdown
    selector a button_interaction card may carry; the chosen option then
    arrives in the event's selected_items.
    """
    card: dict = {
        "card_type": "button_interaction",
        "main_title": {"title": (title or "请确认")[:26]},
        "button_list": [
            {"text": "确认", "style": 1, "key": "btn_confirm"},
            {"text": "取消", "style": 2, "key": "btn_cancel"},
        ],
        "task_id": sanitize_task_id(task_id),
    }
    if desc:
        card["sub_title_text"] = desc[:112]
    if selection and selection.get("options"):
        opts = _make_options(selection["options"])
        card["button_selection"] = {
            "question_key": "q_select",
            "title": (selection.get("title") or "请选择")[:13],
            "selected_id": opts[0]["id"],
            "option_list": opts,
        }
    return card


DEFAULT_CARD_URL = "https://open.work.weixin.qq.com/"


def _make_options(texts: list) -> list[dict]:
    """Option list with stable ids opt_1..opt_n from plain texts."""
    return [{"id": f"opt_{i + 1}", "text": str(t)[:20]} for i, t in enumerate(texts) if str(t).strip()]


def build_text_notice_card(title: str, desc: str, task_id: str = "", url: str = "") -> dict:
    """text_notice: pure display notification, no buttons."""
    card: dict = {
        "card_type": "text_notice",
        "main_title": {"title": (title or "通知")[:26]},
        # A valid card_action is required or the server rejects the card
        # (same 42045 lesson as card updates, verified live 2026-10-04).
        "card_action": {"type": 1, "url": url or DEFAULT_CARD_URL},
    }
    if desc:
        card["sub_title_text"] = desc[:112]
    if task_id:
        card["task_id"] = sanitize_task_id(task_id)
    return card


def build_news_notice_card(title: str, desc: str, image_url: str, task_id: str = "", url: str = "") -> dict:
    """news_notice: image + title display card (图文展示)."""
    card: dict = {
        "card_type": "news_notice",
        "main_title": {"title": (title or "图文")[:26]},
        "card_image": {"url": image_url, "aspect_ratio": 1.3},
        "card_action": {"type": 1, "url": url or DEFAULT_CARD_URL},
    }
    if desc:
        card["vertical_content_list"] = [{"title": (title or "图文")[:26], "desc": desc[:112]}]
    if task_id:
        card["task_id"] = sanitize_task_id(task_id)
    return card


def build_vote_card(title: str, desc: str, options: list, multi: bool, task_id: str) -> dict:
    """vote_interaction: single/multi choice with a submit button.

    The submit click returns event_key=btn_submit plus selected_items.
    """
    card: dict = {
        "card_type": "vote_interaction",
        "main_title": {"title": (title or "请选择")[:26]},
        "checkbox": {
            "question_key": "q_vote",
            "mode": 1 if multi else 0,
            "option_list": _make_options(options),
        },
        "submit_button": {"text": "提交", "key": "btn_submit"},
        "task_id": sanitize_task_id(task_id),
    }
    if desc:
        card["sub_title_text"] = desc[:112]
    return card


def build_multiple_card(title: str, desc: str, groups: list, task_id: str) -> dict:
    """multiple_interaction: up to 3 dropdown selectors + submit button.

    groups: [{title, options:[text,...]}]. The submit click returns
    event_key=btn_submit plus one selected_item per group.
    """
    select_list = []
    for i, g in enumerate(groups[:3]):
        opts = _make_options(g.get("options") or [])
        if not opts:
            continue
        select_list.append(
            {
                "question_key": f"q_{i + 1}",
                "title": (g.get("title") or f"选择{i + 1}")[:13],
                "selected_id": opts[0]["id"],
                "option_list": opts,
            }
        )
    card: dict = {
        "card_type": "multiple_interaction",
        "main_title": {"title": (title or "请选择")[:26]},
        "select_list": select_list,
        "submit_button": {"text": "提交", "key": "btn_submit"},
        "task_id": sanitize_task_id(task_id),
    }
    if desc:
        card["sub_title_text"] = desc[:112]
    return card


def card_option_texts(card: dict) -> dict:
    """{question_key: {option_id: text}} for every selector on a card,
    persisted at registration so click events can be rendered readable."""
    out: dict = {}

    def _add(qkey: str, opts: list) -> None:
        if qkey and opts:
            out[qkey] = {o.get("id", ""): o.get("text", "") for o in opts}

    cb = card.get("checkbox") or {}
    _add(cb.get("question_key", ""), cb.get("option_list") or [])
    for sel in card.get("select_list") or []:
        _add(sel.get("question_key", ""), sel.get("option_list") or [])
    bs = card.get("button_selection") or {}
    _add(bs.get("question_key", ""), bs.get("option_list") or [])
    return out


def split_chunks(text: str, limit: int = CHUNK_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    rest = text
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(rest[:cut])
        rest = rest[cut:].lstrip("\n")
    if rest:
        chunks.append(rest)
    return chunks


def decrypt_wecom_media(data: bytes, aeskey_b64: str) -> bytes:
    """AES-256-CBC; key = base64decode(aeskey), IV = first 16 key bytes,
    PKCS#7 padding to a 32-byte block (per the official SDK's decryptFile)."""
    key_arg = aeskey_b64.strip()
    key_arg += "=" * (-len(key_arg) % 4)  # callbacks may strip b64 padding
    try:
        key = base64.b64decode(key_arg)
    except Exception:
        key = base64.urlsafe_b64decode(key_arg)
    iv = key[:16]
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    plain = decryptor.update(data) + decryptor.finalize()
    if not plain:
        raise ValueError("empty plaintext")
    pad = plain[-1]
    if pad < 1 or pad > 32 or pad > len(plain) or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError(f"bad PKCS#7 padding: {pad}")
    return plain[:-pad]


def sniff_ext(data: bytes) -> str:
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    if data[:4] == b"%PDF":
        return "pdf"
    if data[:2] == b"PK":
        return "zip"
    if data[:4] == b"\x00\x00\x00\x18" or data[4:8] == b"ftyp":
        return "mp4"
    return "bin"


def media_type_for_path(path: str) -> str:
    ext = Path(path).suffix.lower().lstrip(".")
    if ext in ("png", "jpg", "jpeg", "gif", "webp", "bmp"):
        return "image"
    if ext in ("mp4", "mov"):
        return "video"
    if ext in ("amr",):
        return "voice"
    return "file"


def log(msg: str) -> None:
    print(f"[wecom-gateway {time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ---------- slash commands (gateway-intercepted) ----------
# The user can send /ping, /status, /stop, /new, /jump, /check,
# /queue, /help and /subagent in chat. Commands are intercepted
# after the allowlist +
# dedupe checks and BEFORE the inbox write: they never wake a worker;
# the gateway answers itself through the normal proactive send path.
# Unknown slash text (e.g. /foo) is NOT a command and flows through as
# an ordinary message.
HOOK_STATE_DIR = muse_home() / "hooks" / "state" / "wecom-bot"
CLI_WRAPPER = BASE / "wecom"
CHAN_LABEL = "企业微信"

SLASH_COMMANDS = {"ping", "status", "stop", "new", "jump", "check", "queue", "help", "subagent"}
SLASH_ALIASES = {"插队": "jump", "自检": "check", "命令": "help", "副助手": "subagent"}

# User-facing times in acks are rendered in the user's timezone; the VM
# itself runs UTC, so plain localtime would show times 8h off.
from datetime import datetime as _dt

try:
    from zoneinfo import ZoneInfo as _ZoneInfo

    _DISPLAY_TZ = _ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover - tzdata missing
    _DISPLAY_TZ = None


def _now_str(fmt="%H:%M"):
    if _DISPLAY_TZ is not None:
        return _dt.now(_DISPLAY_TZ).strftime(fmt)
    return time.strftime(fmt)


def parse_slash_command(text):
    """Return (name, arg) for a known slash command, else None.

    Normalizes a leading fullwidth slash and matches the command name
    case-insensitively. arg is the remainder after the command token
    (only /jump uses it)."""
    if not text:
        return None
    t = text.strip()
    if t.startswith("／"):
        t = "/" + t[1:]
    if not t.startswith("/"):
        return None
    parts = t[1:].split(None, 1)
    if not parts:
        return None
    name = SLASH_ALIASES.get(parts[0].lower(), parts[0].lower())
    if name not in SLASH_COMMANDS:
        return None
    return name, (parts[1].strip() if len(parts) > 1 else "")


def _read_json_file(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def _last_jsonl_row(path):
    try:
        last = None
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        last = json.loads(line)
                    except json.JSONDecodeError:
                        pass
        return last
    except OSError:
        return None


def _fmt_hm(ts):
    if isinstance(ts, (int, float)) and ts > 0:
        if _DISPLAY_TZ is not None:
            return _dt.fromtimestamp(ts, _DISPLAY_TZ).strftime("%H:%M")
        return time.strftime("%H:%M", time.localtime(ts))
    return "未知"


def _write_hook_json(hook_state_dir, filename, obj):
    try:
        hook_state_dir.mkdir(parents=True, exist_ok=True)
        tmp = hook_state_dir / (filename + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, hook_state_dir / filename)
        return True
    except OSError:
        return False


def enqueue_queue_admin(hook_state_dir, action, msgids) -> bool:
    """Merge this clear/drop into an unconsumed queue_admin.json."""
    path = hook_state_dir / "queue_admin.json"
    lock_path = hook_state_dir / "queue_admin.lock"
    try:
        hook_state_dir.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+") as lock_handle:
            fcntl.flock(lock_handle, fcntl.LOCK_EX)
            try:
                existing = _read_json_file(path, None)
                merged = merge_queue_admin(
                    existing if isinstance(existing, dict) else None,
                    action,
                    msgids,
                    time.time(),
                )
                return _write_hook_json(hook_state_dir, "queue_admin.json", merged)
            finally:
                fcntl.flock(lock_handle, fcntl.LOCK_UN)
    except OSError:
        return False


def _queue_summary(hook_state_dir):
    """(batch, running_ids, pending_count, detached_count) read from the
    hook state files; tolerant of missing/corrupt files."""
    batch = _read_json_file(hook_state_dir / "active_batch.json", {}) or {}
    if not isinstance(batch, dict):
        batch = {}
    ids = [str(m) for m in (batch.get("msgids") or [])]
    detached = [d for d in (batch.get("detached") or []) if isinstance(d, dict)]
    for d in detached:
        ids += [str(m) for m in (d.get("msgids") or [])]
    pending = _read_json_file(hook_state_dir / "pending.json", {}) or {}
    pcount = len(pending) if isinstance(pending, dict) else 0
    return batch, ids, pcount, len(detached)


def is_stop_request(text):
    # Verbatim port of the hook scripts' matcher (~/hooks/scripts/
    # weixin-inbox.sh, wecom-inbox.sh): a stop/cancel imperative must
    # NOT receive a "queued at position N" soft ack — the hook will
    # force-release it on the next poll, so the ack would mislead.
    # This is a deliberate duplicate of the hook rule; keep in sync.
    t = (text or "").strip().strip("。！!？?，,、 ")
    if not t or len(t) > 8:
        return False
    if t in ("停", "停下", "停一下", "停止", "停手", "取消", "别做了",
             "别弄了", "不用了", "不要了", "stop", "Stop", "STOP"):
        return True
    if t.startswith("别做") and len(t) <= 6:
        return True
    if t.startswith("取消") and len(t) <= 5:
        return True
    if t.startswith("停") and len(t) <= 4 and not t.startswith("停车"):
        return True
    return False


SOFT_ACK_TEMPLATE = "已收到，排队第 {n} 位，当前任务进行中；/jump 插队、/stop 取消"


def soft_ack_text(state_dir, hook_state_dir, text):
    """Busy-queue soft ack for a normal inbound message, or None.

    Returns the ack string only when a batch is in flight, judged from
    the same hook-state data /status reads: active_batch.json carries
    msgids and a valid since, and no bound reply row for any of those
    msgids has landed in the outbox at/after since-2s (the hook's own
    batch-finish rule; detached batches do not count as busy). Stop
    imperatives get no ack. Any state problem -> None (fail-silent:
    the message itself is always queued exactly as before).

    Position N = the hook's pending count + 1 (this message). The
    gateway reads the hook's pending.json snapshot, so messages that
    arrived after the hook's last poll are not registered in it yet
    and a fast burst can repeat the same N — accepted approximation.
    """
    try:
        if is_stop_request(text):
            return None
        batch, _ids, pcount, _dcount = _queue_summary(hook_state_dir)
        ids = [str(m) for m in (batch.get("msgids") or [])]
        if not ids:
            return None
        since = batch.get("since")
        if not isinstance(since, (int, float)) or since <= 0:
            return None
        idset = set(ids)
        try:
            with open(state_dir / "outbox.jsonl", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if row.get("mode") != "reply":
                        continue
                    if str(row.get("msgid") or "") not in idset:
                        continue
                    rt = row.get("queued_at")
                    if not isinstance(rt, (int, float)):
                        rt = row.get("ts")
                    if isinstance(rt, (int, float)) and rt >= since - 2:
                        return None  # batch already finished
        except OSError:
            return None
        return SOFT_ACK_TEMPLATE.format(n=pcount + 1)
    except Exception:
        return None


SLASH_HELP_TEXT = """【命令表】在聊天里直接发，网关秒回、不排队
/ping 连通自检
/status 渠道状态汇总（计数版）
/queue 队列明细（逐条版）
/queue clear 清掉排队中未开工的消息
/queue drop N 只删排队中第 N 条
/jump [文本] 强制插队：不等阈值、不取消长任务（别名 /插队）
/stop 取消正在跑的全部任务
/subagent <任务> 派副助手并行执行（别名 /副助手）；/subagent list 查、/subagent stop <编号> 停
/new 开新话题：清会话上下文
/check 一次性自检，跑完即结束（别名 /自检）
/help 本命令表（别名 /命令）
注：只有以上是命令；其他以 / 开头的话会当普通消息处理。"""


def slash_help_text():
    """The /help reply: the full command table as one fixed text,
    identical on both channels and free of any channel name."""
    return SLASH_HELP_TEXT


def slash_ping_text(chan_label, state_dir, hook_state_dir=None):
    st = _read_json_file(state_dir / "status.json", {}) or {}
    state = st.get("state") or "未知"
    err = st.get("last_error")
    err_part = "无错误" if not err else f"最近错误：{err}"
    now_s = _now_str("%H:%M:%S")
    return f"pong ✅ {chan_label}网关在线（状态 {state}，{err_part}）· {now_s} · 网关直接秒回，未唤醒任务"


def slash_status_text(chan_label, state_dir, hook_state_dir):
    st = _read_json_file(state_dir / "status.json", {}) or {}
    state = st.get("state") or "未知"
    conn = "已连接" if st.get("connected") else "未连接"
    err = st.get("last_error")
    err_part = "无错误" if not err else f"错误：{err}"
    last_in = _last_jsonl_row(state_dir / "inbox.jsonl")
    last_out = _last_jsonl_row(state_dir / "outbox_results.jsonl")
    batch, _ids, pcount, dcount = _queue_summary(hook_state_dir)
    mids = batch.get("msgids") or []
    if mids:
        since = batch.get("since")
        mins = max(0, round((time.time() - float(since)) / 60)) if isinstance(since, (int, float)) else "?"
        run_part = f"正在处理 {len(mids)} 条（已跑约 {mins} 分钟）"
    else:
        run_part = "当前无任务在跑"
    chk = _read_json_file(hook_state_dir / "last_check.json")
    if isinstance(chk, dict) and chk.get("ts"):
        if chk.get("ok"):
            chk_part = f"{_fmt_hm(chk['ts'])} ✅ 全部通过"
        else:
            fails = "、".join(str(x) for x in (chk.get("fails") or [])) or "未知异常"
            chk_part = f"{_fmt_hm(chk['ts'])} ❌ 异常：{fails}"
    else:
        chk_part = "暂无记录"
    lines = [
        f"📊 {chan_label}通道状态",
        f"网关：{state}（{conn}，{err_part}）· 最近入站 {_fmt_hm((last_in or {}).get('ts'))} · 最近出站 {_fmt_hm((last_out or {}).get('ts'))}",
        f"队列：{run_part} · 排队 {pcount} 条 · 并行 {dcount} 条",
        f"上次自检：{chk_part}",
    ]
    return "\n".join(lines)


def slash_run_check(chan_label, state_dir, hook_state_dir):
    """One-shot self-check: returns the report text and records
    last_check.json in the hook state dir for /status to quote. Leaves
    no timers or other residue; never wakes a worker."""
    now = time.time()
    items = []  # (label, ok, detail)

    st = _read_json_file(state_dir / "status.json")
    if not isinstance(st, dict) or not st:
        items.append(("网关状态文件", False, "status.json 缺失或不可解析"))
    else:
        upd = st.get("updated_at")
        fresh = isinstance(upd, (int, float)) and (now - float(upd) <= 90)
        ok = bool(fresh and st.get("connected") and not st.get("last_error"))
        detail = f"状态 {st.get('state')}，更新于 {_fmt_hm(upd)}"
        if not fresh:
            detail += "（已超 90 秒未刷新）"
        if st.get("last_error"):
            detail += f"，last_error={st.get('last_error')}"
        items.append(("网关状态文件", ok, detail))

    row = _last_jsonl_row(state_dir / "outbox_results.jsonl")
    if row is None:
        items.append(("最近发送结果", False, "无发送记录"))
    else:
        items.append(("最近发送结果", row.get("ok") is True,
                      f"{_fmt_hm(row.get('ts'))} ok={row.get('ok')}"))

    trio_ok, trio_detail = True, ""
    for fn in ("active_batch.json", "pending.json"):
        p = hook_state_dir / fn
        if not p.exists():
            trio_ok, trio_detail = False, f"{fn} 缺失"
            break
        if _read_json_file(p, None) is None:
            trio_ok, trio_detail = False, f"{fn} 不可解析"
            break
    if trio_ok:
        try:
            (hook_state_dir / "seen_msgids.txt").read_text(encoding="utf-8")
        except OSError:
            trio_ok, trio_detail = False, "seen_msgids.txt 不可读"
    items.append(("队列状态三件套", trio_ok, trio_detail or "可读可解析"))

    try:
        probe = state_dir / f".check_probe_{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        items.append(("状态目录可写", True, ""))
    except OSError as e:
        items.append(("状态目录可写", False, str(e)))

    cpath = state_dir / "cancelled.json"
    if cpath.exists():
        okc = _read_json_file(cpath, None) is not None
        items.append(("取消登记文件", okc, "" if okc else "cancelled.json 不可解析"))
    else:
        items.append(("取消登记文件", True, "无登记（正常）"))

    fails = [label for label, ok, _d in items if not ok]
    lines = [f"🩺 {chan_label}自检（{_now_str('%H:%M:%S')}）"]
    for label, ok, detail in items:
        lines.append(f"{'✓' if ok else '✗'} {label}" + (f"：{detail}" if detail else ""))
    lines.append("结论：" + ("✅ 全部通过" if not fails else "❌ 异常项：" + "、".join(fails)))
    _write_hook_json(hook_state_dir, "last_check.json",
                     {"ts": int(now), "ok": not fails, "fails": fails})
    return "\n".join(lines)


def _excerpt(text, limit=20):
    t = " ".join((text or "").split())
    return t[:limit] + ("…" if len(t) > limit else "")


def _dur_str(secs):
    secs = max(0, int(secs))
    if secs < 60:
        return f"{secs} 秒"
    return f"约 {round(secs / 60)} 分钟"


def _pending_sorted(hook_state_dir):
    """Pending queue as [(msgid, first_seen)] in arrival order."""
    pending = _read_json_file(hook_state_dir / "pending.json", {}) or {}
    if not isinstance(pending, dict):
        return []
    items = [(str(k), float(v)) for k, v in pending.items()
             if isinstance(v, (int, float))]
    items.sort(key=lambda kv: kv[1])
    return items


def _inbox_texts(state_dir):
    texts = {}
    try:
        with open(state_dir / "inbox.jsonl", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("msgid"):
                    texts[str(e["msgid"])] = e.get("text") or ""
    except OSError:
        pass
    return texts


def slash_queue_text(chan_label, state_dir, hook_state_dir):
    """/queue detail listing: per-message view of the same state that
    /status only counts. Empty sections are omitted."""
    batch, _ids, _p, _d = _queue_summary(hook_state_dir)
    texts = _inbox_texts(state_dir)
    now = time.time()
    active = [str(m) for m in (batch.get("msgids") or [])]
    detached = [d for d in (batch.get("detached") or []) if isinstance(d, dict)]
    pend = _pending_sorted(hook_state_dir)
    if not active and not pend and not detached:
        return "队列空闲：没有正在处理或排队的任务。"
    lines = [f"【任务队列】{chan_label}"]
    if active:
        since = batch.get("since")
        age = _dur_str(now - float(since)) if isinstance(since, (int, float)) else "未知"
        lines.append(f"▶ 正在处理（{len(active)}）：已跑{age}")
        for m in active:
            lines.append(f"·「{_excerpt(texts.get(m, ''))}」")
    if pend:
        lines.append(f"⏳ 排队中（{len(pend)}）：")
        for i, (m, ts) in enumerate(pend, 1):
            lines.append(f"{i}.「{_excerpt(texts.get(m, ''))}」已等 {_dur_str(now - ts)}")
    if detached:
        lines.append(f"⛓ 后台续跑（detached，{len(detached)}）：")
        for d in detached:
            dsince = d.get("since")
            age = _dur_str(now - float(dsince)) if isinstance(dsince, (int, float)) else "未知"
            for m in (d.get("msgids") or []):
                lines.append(f"·「{_excerpt(texts.get(str(m), ''))}」已跑{age}")
    lines.append("（停正在跑的用 /stop；让排队的提前用 /jump）")
    return "\n".join(lines)


def slash_register_cancels(msgids):
    """Register a cancellation for each msgid via this channel's CLI
    (the CLI owns cancelled.json; the gateway never writes it)."""
    ok = fail = 0
    for mid in msgids:
        try:
            r = subprocess.run([str(CLI_WRAPPER), "cancel", "--msgid", str(mid)],
                               capture_output=True, timeout=15)
            if r.returncode == 0:
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
    return ok, fail


# ---------- /subagent: parallel sub-assistant jobs ----------
# The gateway only allocates job ids, records dispatch/stop intents
# and renders the job list; the inbox hook owns subagent_jobs.json
# (dispatch, supervision, retries) — see the hook script. The command
# message itself never enters the inbox, so at dispatch time the
# gateway also registers how to reach this chat for the job msgid
# (weixin: the reply context; wecom: an event-style reqmap entry), or
# the job worker's formal reply would have nowhere to go.

def _append_jsonl_file(path, obj):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        return True
    except OSError:
        return False


def slash_subagent_next_id(state_dir):
    """Allocate the next job id (S<n>), or None if it cannot be saved."""
    return allocate_subagent_id(state_dir)


def _subagent_jobs_read(hook_state_dir):
    """Read the hook-owned subagent_jobs.json (read-only here).
    Returns the jobs dict; {} when no job was ever recorded (file not
    created yet); None when the file exists but is unreadable/corrupt."""
    p = hook_state_dir / "subagent_jobs.json"
    if not p.exists():
        return {}
    data = _read_json_file(p, None)
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), dict):
        return None
    return data["jobs"]


def _job_num(jid):
    try:
        return int(str(jid)[1:])
    except (TypeError, ValueError):
        return 0


_JOB_SHORT_LABELS = {"done": "完成", "failed": "失败", "stopped": "已停止"}
_JOB_ENDED_LABELS = {"done": "已完成", "failed": "已失败", "stopped": "已停止"}


def slash_subagent_list_text(hook_state_dir):
    """/subagent list rendering: running/queued jobs one per line,
    then the most recent 3 finished ones."""
    jobs = _subagent_jobs_read(hook_state_dir)
    if jobs is None:
        return "副助手状态暂不可读，请稍后再试。"
    if not jobs:
        return "暂无副助手任务记录。"
    now = time.time()
    items = sorted(((j, r) for j, r in jobs.items() if isinstance(r, dict)),
                   key=lambda kv: _job_num(kv[0]))
    lines = ["【副助手任务】"]
    active = [(j, r) for j, r in items if r.get("status") in ("running", "queued")]
    for jid, r in active:
        if r.get("status") == "running":
            st = r.get("started_ts")
            age = _dur_str(now - float(st)) if isinstance(st, (int, float)) else "未知"
            lines.append(f"#{jid} 运行中（已跑{age}）：「{_excerpt(r.get('text', ''))}」")
        else:
            st = r.get("ts_dispatched")
            age = _dur_str(now - float(st)) if isinstance(st, (int, float)) else "未知"
            lines.append(f"#{jid} 排队中（已等{age}）：「{_excerpt(r.get('text', ''))}」")
    if not active:
        lines.append("当前没有运行中或排队中的副助手任务。")
    finished = [(j, r) for j, r in items if r.get("status") in _JOB_SHORT_LABELS]
    if finished:
        lines.append("— 最近结束 —")
        for jid, r in finished[-3:]:
            lines.append(f"#{jid} {_JOB_SHORT_LABELS[r['status']]}"
                         f"（{_fmt_hm(r.get('finished_ts'))}）：「{_excerpt(r.get('text', ''))}」")
    return "\n".join(lines)


def _norm_job_id(tok):
    """'S3' / 's3' / '3' -> 'S3'; anything else -> None."""
    t = (tok or "").strip()
    if t[:1] in ("S", "s"):
        t = t[1:]
    if t.isdigit():
        return f"S{int(t)}"
    return None


def slash_subagent_stop_ack(hook_state_dir, job_id):
    """Validate a /subagent stop against the jobs file and, when the
    job is still live, append the stop intent for the hook to enact.
    The jobs file itself is never written from the gateway."""
    jobs = _subagent_jobs_read(hook_state_dir)
    if jobs is None:
        return "副助手状态暂不可读，请稍后再试。"
    rec = jobs.get(job_id)
    if not isinstance(rec, dict):
        return f"没有找到副助手任务 #{job_id}。"
    st = rec.get("status")
    if st in _JOB_ENDED_LABELS:
        return f"副助手 #{job_id} {_JOB_ENDED_LABELS[st]}，无需再停。"
    _append_jsonl_file(hook_state_dir / "subagent_cmd.jsonl",
                       {"type": "stop", "job_id": job_id, "ts": time.time()})
    return f"已提交停止 #{job_id}，副助手会在下一个检查点停下并回一句确认。"



# --- Subagent reply label enforcement (fix #2, 2026-10-04) -----
# See the CLI of this channel for the rationale; the gateway
# re-checks every reply at dispatch so even a bypassed CLI
# cannot deliver an unlabeled or mislabeled job result.

def _forced_subagent_label(jid, content, status=None):
    """Return content whose first line opens with the authoritative
    label 【副助手 #<jid> <outcome>】. A missing label follows the job
    status and the reply text instead of always claiming 完成."""
    text = content or ""
    if not text.strip():
        return text
    first, sep, rest = text.partition("\n")
    head = first.strip()
    outcome = None
    tail = ""
    if head.startswith("【副助手") and "】" in head:
        end = head.index("】")
        inner = head[1:end]
        for oc in ("已停止", "完成", "失败"):
            if oc in inner:
                outcome = oc
                tail = head[end + 1:].strip()
                break
    if outcome is None:
        outcome = subagent_outcome_default(text, status)
        return f"【副助手 #{jid} {outcome}】\n" + text
    new_first = f"【副助手 #{jid} {outcome}】" + (f" {tail}" if tail else "")
    return new_first + (sep + rest if sep else "")


def normalize_subagent_content(msgid, content):
    """Second enforcement layer, right before delivery: if msgid
    belongs to a /subagent job, force the authoritative first-line
    label. Ordinary replies pass through untouched."""
    if not content:
        return content
    jobs = _subagent_jobs_read(HOOK_STATE_DIR)
    if not jobs:
        return content
    for jid, rec in jobs.items():
        if isinstance(rec, dict) and str(rec.get("msgid")) == str(msgid):
            return _forced_subagent_label(str(jid), content, rec.get("status"))
    return content

def load_credentials() -> tuple[str, str, set[str]]:
    bot_id = os.environ.get("WECOM_BOT_ID", "").strip()
    secret = os.environ.get("WECOM_BOT_SECRET", "").strip()
    allow_users = {
        u.strip()
        for u in os.environ.get("WECOM_ALLOW_USERS", "").split(",")
        if u.strip()
    }
    if CRED_FILE.exists():
        try:
            for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                val = val.strip().strip('"').strip("'")
                if key.strip() == "WECOM_BOT_ID" and val:
                    bot_id = val
                elif key.strip() == "WECOM_BOT_SECRET" and val:
                    secret = val
                elif key.strip() == "WECOM_ALLOW_USERS" and val:
                    allow_users = {u.strip() for u in val.split(",") if u.strip()}
        except OSError as e:
            log(f"cannot read credentials file: {e}")
    if bot_id in ("", "YOUR_BOT_ID") or secret in ("", "YOUR_SECRET"):
        return "", "", allow_users
    return bot_id, secret, allow_users


class Gateway:
    def __init__(self) -> None:
        STATE.mkdir(parents=True, exist_ok=True)
        self.ws = None
        self.connected = False
        self.started_at = int(time.time())
        self.msgs_received = 0
        self.msgs_sent = 0
        self.last_error = ""
        self.state = "starting"
        self.pending_responses: dict[str, asyncio.Future] = {}
        self.respond_locks: dict[str, asyncio.Lock] = {}
        self.open_streams: dict[str, dict] = {}
        self._http: httpx.AsyncClient | None = None
        self.allow_users: set[str] = set()
        self.seen_msgids: set[str] = set()
        self.reqmap: dict[str, str] = {}
        self.cards: dict[str, dict] = {}
        self._lock_fd = None
        self._kicked = False
        self._acquire_instance_lock()
        self._load_persisted()

    def _acquire_instance_lock(self) -> None:
        """Refuse a second local process. Two subscriptions kick each other."""
        STATE.mkdir(parents=True, exist_ok=True)
        fd = open(LOCK_FILE, "a+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fd.close()
            raise RuntimeError(f"another gateway instance already holds {LOCK_FILE}")
        fd.seek(0)
        fd.truncate()
        fd.write(str(os.getpid()))
        fd.flush()
        self._lock_fd = fd

    # ---------- persistence helpers ----------

    def _load_persisted(self) -> None:
        if INBOX.exists():
            try:
                for line in INBOX.read_text(encoding="utf-8").splitlines():
                    try:
                        entry = json.loads(line)
                        if entry.get("msgid"):
                            self.seen_msgids.add(entry["msgid"])
                    except json.JSONDecodeError:
                        continue
            except OSError:
                pass
        if SEEN_FILE.exists():
            try:
                for line in SEEN_FILE.read_text(encoding="utf-8").splitlines():
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(entry, dict) and entry.get("msgid"):
                        self.seen_msgids.add(str(entry["msgid"]))
            except OSError:
                pass
        if REQMAP.exists():
            try:
                self.reqmap = json.loads(REQMAP.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                self.reqmap = {}
        if CARDS.exists():
            try:
                self.cards = json.loads(CARDS.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                self.cards = {}

    def _save_cards(self) -> None:
        # keep only the most recent 100 cards
        if len(self.cards) > 100:
            items = list(self.cards.items())[-100:]
            self.cards = dict(items)
        tmp = CARDS.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.cards, ensure_ascii=False), encoding="utf-8")
        tmp.replace(CARDS)

    def _register_card(self, card: dict, chatid: str, msgid: str = "") -> None:
        task_id = card.get("task_id", "")
        if not task_id:
            return
        self.cards[task_id] = {
            "task_id": task_id,
            "title": (card.get("main_title") or {}).get("title", ""),
            "desc": card.get("sub_title_text", ""),
            "card_type": card.get("card_type", ""),
            "options": card_option_texts(card),
            "chatid": chatid,
            "msgid": msgid,
            "status": "pending",
            "created_at": int(time.time()),
        }
        self._save_cards()

    def _mark_seen(self, msgid: str) -> None:
        """Remember msgid in memory and on disk so a restart can dedupe."""
        if not msgid or str(msgid) in self.seen_msgids:
            return
        self.seen_msgids.add(str(msgid))
        try:
            append_jsonl_line(SEEN_FILE, {"msgid": str(msgid)})
        except OSError:
            self.seen_msgids.discard(str(msgid))

    def _save_reqmap(self) -> None:
        # Keep open streams even when the map is trimmed. 200 was short
        # enough to drop a reply route inside the 24-hour answer window.
        self.reqmap = trim_mapping(list(self.reqmap.items()), 2000, set(self.open_streams))
        tmp = REQMAP.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.reqmap, ensure_ascii=False), encoding="utf-8")
        tmp.replace(REQMAP)

    def write_status(self) -> None:
        status = {
            "state": self.state,
            "connected": self.connected,
            "started_at": self.started_at,
            "updated_at": int(time.time()),
            "msgs_received": self.msgs_received,
            "msgs_sent": self.msgs_sent,
            "last_error": self.last_error,
        }
        tmp = STATUS.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(status, ensure_ascii=False, indent=1), encoding="utf-8")
            tmp.replace(STATUS)
        except OSError:
            pass

    @staticmethod
    def append_jsonl(path: Path, obj: dict) -> None:
        append_jsonl_line(path, obj)

    def _event_allowed(self, userid: str) -> bool:
        """Same allowlist as ordinary messages. Empty allowlist allows all."""
        if not self.allow_users:
            return True
        if userid and userid in self.allow_users:
            return True
        log(f"ignored event from non-allowlisted user {userid}")
        return False

    @staticmethod
    def _cancelled_ids() -> set:
        try:
            rows = json.loads(CANCELLED_FILE.read_text(encoding="utf-8"))
            return {str(row.get("msgid")) for row in rows if isinstance(row, dict) and row.get("msgid")}
        except (OSError, json.JSONDecodeError, UnicodeError):
            return set()

    @staticmethod
    def _load_retry() -> dict:
        return load_json_dict(OUTBOX_RETRY)

    @staticmethod
    def _save_retry(data: dict) -> None:
        try:
            atomic_write_text(OUTBOX_RETRY, json.dumps(data))
        except OSError:
            pass

    def _load_partial(self) -> dict:
        return load_json_dict(OUTBOX_PARTIAL)

    def _save_partial(self, data: dict) -> None:
        try:
            atomic_write_text(OUTBOX_PARTIAL, json.dumps(data))
        except OSError:
            pass

    @staticmethod
    def _delivered_reply_before(item: dict) -> bool:
        """Sending-layer late suppression (fix, 2026-10-04 evening,
        mirroring the weixin channel): True when an outbox row queued
        BEFORE this item is a formal reply for the same msgid whose
        delivery result is ok. Drops late update/reply_file rows from
        racing orphan-takeover workers at dispatch time. Only rows
        before this item count, so an update queued before its reply
        (normal progress order) is never suppressed. Fail-open."""
        try:
            item_id = item.get("id", "")
            results: dict[str, dict] = {}
            if OUTBOX_RESULTS.exists():
                for line in OUTBOX_RESULTS.read_text(encoding="utf-8").splitlines():
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if r.get("id"):
                        results[r["id"]] = r
            if not OUTBOX.exists():
                return False
            for line in OUTBOX.read_text(encoding="utf-8").splitlines():
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if o.get("id") == item_id:
                    break
                if o.get("mode") != "reply" or str(o.get("msgid")) != str(item.get("msgid")):
                    continue
                res = results.get(o.get("id"))
                if res is not None and res.get("ok") is True:
                    return True
            return False
        except Exception:
            return False

    # ---------- low-level send / response tracking ----------

    async def send_frame(self, frame: dict, wait_response: bool = False, timeout: float = 10.0):
        req_id = frame.get("headers", {}).get("req_id", "")
        fut = None
        if wait_response and req_id:
            fut = asyncio.get_running_loop().create_future()
            self.pending_responses[req_id] = fut
        await self.ws.send(json.dumps(frame, ensure_ascii=False))
        if fut is not None:
            try:
                return await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                self.pending_responses.pop(req_id, None)
                return {"errcode": -1, "errmsg": "response timeout"}

    @staticmethod
    def new_req_id(prefix: str) -> str:
        return f"{prefix}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

    def _respond_lock(self, req_id: str) -> asyncio.Lock:
        lock = self.respond_locks.get(req_id)
        if lock is None:
            lock = asyncio.Lock()
            self.respond_locks[req_id] = lock
        return lock

    async def respond(self, req_id: str, body: dict, timeout: float = 10.0):
        # All responds for one callback req_id are serialized, mirroring the
        # official SDK's per-req_id reply queue: send one, wait for its ack,
        # then send the next.
        async with self._respond_lock(req_id):
            return await self.send_frame(
                {"cmd": "aibot_respond_msg", "headers": {"req_id": req_id}, "body": body},
                wait_response=True,
                timeout=timeout,
            )

    def reqinfo(self, msgid: str) -> dict:
        v = self.reqmap.get(msgid)
        if isinstance(v, str):  # legacy entries stored a bare req_id
            return {"req_id": v, "stream_id": ""}
        return v or {}

    # ---------- inbound handling ----------

    # ---------- slash commands ----------

    async def _send_slash_ack(self, chatid, chattype, ack, req_id=""):
        """Answer a slash command on the callback req_id when we have one.

        A proactive send alone leaves the callback unanswered. The same
        text is the reply, so it is not also sent a second time.
        """
        if req_id:
            resp = await self.respond(
                req_id, {"msgtype": "markdown", "markdown": {"content": ack}},
            )
        else:
            resp = await self.send_frame(
                {
                    "cmd": "aibot_send_msg",
                    "headers": {"req_id": self.new_req_id("send")},
                    "body": {
                        "chatid": chatid,
                        "chat_type": 2 if chattype == "group" else 1,
                        "msgtype": "markdown",
                        "markdown": {"content": ack},
                    },
                },
                wait_response=True,
            )
        if isinstance(resp, dict) and resp.get("errcode") == 0:
            self.msgs_sent += 1
        elif isinstance(resp, dict) and resp.get("errcode") not in (None, 0):
            log(f"slash ack send failed: errcode={resp.get('errcode')} err={resp.get('errmsg', '')}")

    async def _slash_dispatch(self, text, chatid, chattype, msgid="", req_id=""):
        """Handle a slash command. Returns:
        - None: not a command — caller proceeds with the normal flow.
        - "handled": command done, ack sent — caller returns early.
        - ("rewrite", new_text): /jump with text — side effects done and
          ack sent; caller proceeds with the normal flow using new_text
          as the message text (the /jump message itself becomes a normal
          inbox message under its own msgid).
        Never raises: on an unexpected error it logs and returns None so
        the message falls back to the ordinary worker flow."""
        try:
            parsed = parse_slash_command(text)
            if parsed is None:
                return None
            name, arg = parsed
            ack = None
            if name == "ping":
                ack = slash_ping_text(CHAN_LABEL, STATE, HOOK_STATE_DIR)
            elif name == "help":
                ack = slash_help_text()
            elif name == "status":
                ack = slash_status_text(CHAN_LABEL, STATE, HOOK_STATE_DIR)
            elif name == "check":
                ack = slash_run_check(CHAN_LABEL, STATE, HOOK_STATE_DIR)
            elif name == "stop":
                _b, ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
                if not ids:
                    ack = "当前没有正在跑的任务。"
                else:
                    ok, fail = await asyncio.to_thread(slash_register_cancels, ids)
                    ack = f"已登记取消共 {ok} 条，正在跑的任务会在下一个进度检查点停下并回你一句确认。"
                    if fail:
                        ack += f"（另有 {fail} 条登记失败）"
            elif name == "new":
                _write_hook_json(HOOK_STATE_DIR, "topic_boundary.json", {"ts": time.time()})
                ack = "已开启新话题，之前的对话不会再带入上下文。"
            elif name == "queue":
                qtokens = arg.split()
                if not qtokens:
                    ack = slash_queue_text(CHAN_LABEL, STATE, HOOK_STATE_DIR)
                elif qtokens[0] == "clear":
                    pend = _pending_sorted(HOOK_STATE_DIR)
                    if not pend:
                        ack = "排队中没有消息可清。"
                    else:
                        texts = _inbox_texts(STATE)
                        wrote = enqueue_queue_admin(
                            HOOK_STATE_DIR, "clear", [m for m, _t in pend])
                        excerpts = "、".join(f"「{_excerpt(texts.get(m, ''))}」" for m, _t in pend)
                        if wrote:
                            ack = (f"已提交清除排队消息 {len(pend)} 条：{excerpts}。"
                                   f"约 5 秒内生效。正在跑的任务不受影响。")
                        else:
                            ack = "清除排队失败：暂时写不了队列指令，请稍后再试。"
                elif qtokens[0] == "drop" and len(qtokens) == 2 and qtokens[1].isdigit():
                    pend = _pending_sorted(HOOK_STATE_DIR)
                    n = int(qtokens[1])
                    if n < 1 or n > len(pend):
                        ack = f"排队里没有第 {n} 条（当前共 {len(pend)} 条）。"
                    else:
                        mid = pend[n - 1][0]
                        texts = _inbox_texts(STATE)
                        wrote = enqueue_queue_admin(HOOK_STATE_DIR, "drop", [mid])
                        if wrote:
                            ack = f"已提交删除排队第 {n} 条：「{_excerpt(texts.get(mid, ''))}」，约 5 秒内生效。"
                        else:
                            ack = "删除排队失败：暂时写不了队列指令，请稍后再试。"
                else:
                    return None  # unknown /queue subcommand: not a command
            elif name == "subagent":
                tokens = arg.split(None, 1)
                head = tokens[0].lower() if tokens else ""
                if not arg:
                    ack = "用法：/subagent <任务> 派发；/subagent list 查看；/subagent stop <编号> 停止。"
                elif head == "list":
                    ack = slash_subagent_list_text(HOOK_STATE_DIR)
                elif head == "stop":
                    tok = tokens[1].strip() if len(tokens) > 1 else ""
                    if not tok:
                        ack = "用法：/subagent stop <编号>，如 /subagent stop S3。"
                    else:
                        jid = _norm_job_id(tok)
                        if jid is None:
                            ack = f"没认出这个编号：「{tok}」。编号形如 S3，可用 /subagent list 查看。"
                        else:
                            ack = slash_subagent_stop_ack(HOOK_STATE_DIR, jid)
                else:
                    jid = slash_subagent_next_id(STATE)
                    mid = str(msgid or "")
                    if not jid:
                        ack = "派发失败：暂时无法登记副助手任务，请稍后再试。"
                    elif _append_jsonl_file(HOOK_STATE_DIR / "subagent_requests.jsonl",
                                          {"job_id": jid, "msgid": mid,
                                           "text": arg, "ts": time.time()}):
                        if mid:
                            # Register the reply route for the job msgid
                            # with the event shape (same as a template
                            # card event): the command never enters the
                            # inbox and no stream was opened for it, so
                            # the job worker's formal reply is delivered
                            # as a proactive message to this chat.
                            self.reqmap[mid] = {
                                "req_id": req_id,
                                "stream_id": "",
                                "chatid": chatid,
                                "chattype": chattype,
                                "event": True,
                            }
                            self._save_reqmap()
                        ack = (f"【副助手 #{jid} 已派发】{_excerpt(arg)}。"
                               "后台执行中，主对话不受影响；查进度：/subagent list")
                    else:
                        ack = "派发失败：暂时无法登记副助手任务，请稍后再试。"
            elif name == "jump":
                batch, _ids, pcount, _d = _queue_summary(HOOK_STATE_DIR)
                active = bool(batch.get("msgids"))
                if not active:
                    if arg:
                        await self._send_slash_ack(
                            chatid, chattype,
                            "当前没有任务在跑，这条会直接处理。", req_id)
                        return ("rewrite", arg)
                    ack = "当前没有任务在跑，下一条消息会直接处理。"
                elif arg:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json", {"ts": time.time()})
                    await self._send_slash_ack(
                        chatid, chattype,
                        f"已强制插队：排队的 {pcount + 1} 条立即处理，前面的长任务继续在跑。",
                        req_id)
                    return ("rewrite", arg)
                elif pcount > 0:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json", {"ts": time.time()})
                    ack = f"已强制插队：排队的 {pcount} 条立即处理，前面的长任务继续在跑。"
                else:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json",
                                     {"ts": time.time(), "armed": True})
                    ack = "已武装插队：你下一条消息会立即插队处理，前面的长任务继续在跑。"
            if ack is not None:
                await self._send_slash_ack(chatid, chattype, ack, req_id)
            log(f"slash /{name} handled for chat {chatid}")
            return "handled"
        except Exception as e:  # never let a command break the inbound flow
            log(f"slash dispatch error: {e!r}")
            return None

    def _maybe_soft_ack(self, chatid, chattype, text):
        """Queue the busy soft ack as an unbound outbox "send" row.

        Fire-and-forget: any failure is logged and swallowed so the
        ack can never disturb the inbound flow. The row carries no
        msgid, so it never counts as activity or completion for any
        batch (the standing anti-cross-talk rule for sends)."""
        try:
            ack = soft_ack_text(STATE, HOOK_STATE_DIR, text)
            if not ack:
                return
            self.append_jsonl(OUTBOX, {
                "id": f"softack-{uuid.uuid4().hex}",
                "mode": "send",
                "chatid": chatid,
                "chat_type": 2 if chattype == "group" else 1,
                "content": ack,
            })
            log(f"soft ack queued for chat {chatid}: {ack}")
        except Exception as e:
            log(f"soft ack failed (ignored): {e!r}")

    async def handle_message_callback(self, frame: dict) -> None:
        body = frame.get("body", {}) or {}
        req_id = frame.get("headers", {}).get("req_id", "")
        msgid = body.get("msgid", "")
        if msgid and msgid in self.seen_msgids:
            log(f"duplicate msgid {msgid}, skipped")
            return
        msgtype = body.get("msgtype", "")
        # Parse like the OpenClaw plugin's message-parser: plain text, voice
        # transcription, mixed text/image, quoted content, and media refs.
        text_parts: list[str] = []
        media: list[dict] = []

        def _collect_media(kind: str, node: dict | None) -> None:
            if node and node.get("url"):
                media.append(
                    {"kind": kind, "url": node["url"], "aeskey": node.get("aeskey", "")}
                )

        if msgtype == "mixed":
            for item in (body.get("mixed") or {}).get("msg_item", []) or []:
                itype = item.get("msgtype", "")
                if itype == "text":
                    c = (item.get("text") or {}).get("content", "")
                    if c:
                        text_parts.append(c)
                elif itype == "image":
                    _collect_media("image", item.get("image"))
                elif itype == "video":
                    _collect_media("video", item.get("video"))
        else:
            if (body.get("text") or {}).get("content"):
                text_parts.append(body["text"]["content"])
            if msgtype == "voice" and (body.get("voice") or {}).get("content"):
                text_parts.append(body["voice"]["content"])
            if msgtype == "image":
                _collect_media("image", body.get("image"))
            if msgtype == "file":
                _collect_media("file", body.get("file"))
            if msgtype == "video":
                _collect_media("video", body.get("video"))
        quote = body.get("quote") or {}
        if quote:
            qtext = ""
            if quote.get("msgtype") == "text":
                qtext = (quote.get("text") or {}).get("content", "")
            elif quote.get("msgtype") == "voice":
                qtext = (quote.get("voice") or {}).get("content", "")
            if qtext:
                text_parts.append(f"（引用：{qtext}）")
            _collect_media("image", quote.get("image"))
            _collect_media("file", quote.get("file"))
        for m in media:
            labels = {"image": "[图片]", "file": "[文件]", "video": "[视频]"}
            text_parts.append(labels.get(m["kind"], "[文件]"))
        text = "\n".join(text_parts)
        chattype = body.get("chattype", "")
        from_userid = (body.get("from") or {}).get("userid", "")
        chatid = body.get("chatid") or from_userid
        if self.allow_users and from_userid not in self.allow_users:
            log(f"ignored message from non-allowlisted user {from_userid}")
            return
        slash_rewritten = False
        if msgtype == "text" and not media:
            # Slash commands: intercepted after allowlist + dedupe,
            # before the inbox write — they never wake a worker.
            slash = await self._slash_dispatch(text, chatid, chattype, msgid, req_id)
            if slash == "handled":
                self._mark_seen(msgid)
                self.msgs_received += 1
                self.write_status()
                return
            if isinstance(slash, tuple):
                text = slash[1]
                slash_rewritten = True
        entry = {
            "msgid": msgid,
            "ts": int(time.time()),
            "chattype": chattype,
            "chatid": chatid,
            "from_userid": from_userid,
            "msgtype": msgtype,
            "text": text,
            "media": media,
            "auto_handled": False,
            "raw": body,
        }
        if media:
            await self._download_media(media, msgid)
        stream_id = uuid.uuid4().hex
        # Feedback id for the stream reply (P2): set on the FIRST stream
        # frame only, per the official SDK. Derived from the msgid so a
        # later feedback_event can be traced back to the original message.
        feedback_id = f"fb-{msgid}" if msgid else f"fb-{stream_id}"
        if msgid and req_id:
            self.reqmap[msgid] = {
                "req_id": req_id,
                "stream_id": stream_id,
                "feedback_id": feedback_id,
                "chatid": chatid,
                "chattype": chattype,
            }
            self._save_reqmap()
        self.msgs_received += 1

        stripped = text.strip()
        if msgtype == "text" and stripped.lower() == "ping":
            entry["auto_handled"] = True
            self.append_jsonl(INBOX, entry)
            self._mark_seen(msgid)
            resp = await self.respond(
                req_id, {"msgtype": "markdown", "markdown": {"content": "pong ✅ 网关在线"}}
            )
            log(f"ping -> pong (errcode={resp.get('errcode')})")
            if resp.get("errcode") == 0:
                self.msgs_sent += 1
        else:
            self.append_jsonl(INBOX, entry)
            self._mark_seen(msgid)
            if not slash_rewritten:
                # Soft ack: if a batch is in flight this message just
                # joined the pending queue — acknowledge immediately
                # instead of leaving the user in silence. A /jump
                # rewrite already got its own ack from the slash
                # layer. Never raises; queue semantics are unchanged.
                self._maybe_soft_ack(chatid, chattype, text)
            # Official pattern (SDK example + OpenClaw plugin): open a
            # stream reply immediately whose content is the native think
            # marker "<think></think>" — the WeCom client renders its own
            # thinking animation for it; the agent's final answer later
            # replaces it in place via the same stream_id with finish=true.
            # feedback.id is set here, on the first frame only.
            resp = await self.respond(
                req_id,
                {
                    "msgtype": "stream",
                    "stream": {
                        "id": stream_id,
                        "finish": False,
                        "content": "<think></think>",
                        "feedback": {"id": feedback_id},
                    },
                },
            )
            log(f"msg {msgid} from {from_userid} type={msgtype} stream-start errcode={resp.get('errcode')}")
            if resp.get("errcode") == 0:
                self.msgs_sent += 1
                if msgid:
                    self.open_streams[msgid] = {
                        "req_id": req_id,
                        "stream_id": stream_id,
                        "feedback_id": feedback_id,
                        "since": time.time(),
                    }
        self.write_status()

    async def handle_event_callback(self, frame: dict) -> None:
        body = frame.get("body", {}) or {}
        req_id = frame.get("headers", {}).get("req_id", "")
        event = body.get("event", {}) or {}
        etype = event.get("eventtype", "")
        from_userid = (body.get("from") or {}).get("userid", "")
        log(f"event {etype} from {from_userid}")
        if etype == "enter_chat":
            if not self._event_allowed(from_userid):
                return
            resp = await self.send_frame(
                {
                    "cmd": "aibot_respond_welcome_msg",
                    "headers": {"req_id": req_id},
                    "body": {
                        "msgtype": "text",
                        "text": {"content": "你好，我是 Muse 助手。直接发消息给我就行，发 ping 可以测试连通。"},
                    },
                },
                wait_response=True,
                timeout=5.0,
            )
            if resp.get("errcode") == 0:
                self.msgs_sent += 1
            self.write_status()
        elif etype == "disconnected_event":
            self._kicked = True
            self.last_error = "kicked by a newer connection (disconnected_event)"
            log("WARNING: disconnected_event received; another connection replaced this one")
            self.write_status()
            try:
                await self.ws.close()
            except Exception:
                pass
        elif etype == "template_card_event":
            if not self._event_allowed(from_userid):
                return
            event_msgid = str(body.get("msgid") or "")
            if event_msgid and event_msgid in self.seen_msgids:
                log(f"duplicate event msgid {event_msgid}, skipped")
                return
            # A button on a confirm card was clicked. Record the click as a
            # NON-auto-handled inbox entry so the agent wake hook picks it
            # up, remember how to reach this chat for the agent's follow-up
            # (an event req_id cannot take a normal stream reply, so the
            # reply dispatch falls back to a proactive send for it), and
            # immediately replace the card with a result notice — the
            # official pattern, which must happen within ~5s of the event.
            # WeCom nests the click payload: event.template_card_event.{event_key,task_id}
            tc_event = event.get("template_card_event") or {}
            event_key = tc_event.get("event_key", "") or event.get("event_key", "")
            task_id = tc_event.get("task_id", "") or event.get("task_id", "")
            card = self.cards.get(task_id) or {}
            # Selections (vote / multiple / button dropdown) arrive as
            # selected_items.selected_item[] = {question_key, option_ids:{option_id[]}}
            sel_raw = tc_event.get("selected_items") or {}
            sel_items = []
            if isinstance(sel_raw, dict):
                sel_items = sel_raw.get("selected_item") or sel_raw.get("selected_items") or []
            elif isinstance(sel_raw, list):
                sel_items = sel_raw
            opt_map = card.get("options") or {}
            sel_parts = []
            sel_store = []
            for it in sel_items if isinstance(sel_items, list) else []:
                if not isinstance(it, dict):
                    continue
                qk = it.get("question_key", "")
                oids = it.get("option_ids") or {}
                if isinstance(oids, dict):
                    oids = oids.get("option_id") or []
                elif isinstance(oids, str):
                    oids = [oids]
                texts = [(opt_map.get(qk) or {}).get(o, o) for o in oids]
                sel_store.append({"question_key": qk, "option_ids": list(oids), "texts": texts})
                if texts:
                    sel_parts.append("、".join(texts))
            sel_text = "；".join(sel_parts)
            is_submit = event_key in ("btn_submit", "submit") or card.get("card_type") in (
                "vote_interaction",
                "multiple_interaction",
            )
            if event_key == "btn_confirm":
                decided = "确认"
            elif event_key == "btn_cancel":
                decided = "取消"
            elif is_submit:
                decided = "提交"
            else:
                decided = event_key or "点击"
            if sel_text:
                decided = f"{decided}（选了：{sel_text}）"
            if task_id:
                card.update(
                    {
                        "status": event_key or "clicked",
                        "clicked_by": from_userid,
                        "clicked_at": int(time.time()),
                    }
                )
                if sel_store:
                    card["selected"] = sel_store
                self.cards[task_id] = card
                self._save_cards()
            msgid = body.get("msgid", "")
            chattype = body.get("chattype", "")
            chatid = body.get("chatid") or card.get("chatid") or from_userid
            if msgid and req_id:
                self.reqmap[msgid] = {
                    "req_id": req_id,
                    "stream_id": "",
                    "chatid": chatid,
                    "chattype": chattype,
                    "event": True,
                }
                self._save_reqmap()
            title = card.get("title") or "确认卡片"
            self.append_jsonl(
                INBOX,
                {
                    "msgid": msgid,
                    "ts": int(time.time()),
                    "chattype": chattype,
                    "chatid": chatid,
                    "from_userid": from_userid,
                    "msgtype": "event",
                    "text": f"[卡片点击] 「{title}」 task_id={task_id} 选择={decided} (event_key={event_key})",
                    "media": [],
                    "auto_handled": False,
                    "raw": body,
                },
            )
            self.msgs_received += 1
            self._mark_seen(str(msgid or ""))
            if task_id:
                try:
                    resp = await self.send_frame(
                        {
                            "cmd": "aibot_respond_update_msg",
                            "headers": {"req_id": req_id},
                            "body": {
                                "response_type": "update_template_card",
                                "template_card": {
                                    "card_type": "text_notice",
                                    "main_title": {
                                        "title": "已确认 ✅" if event_key == "btn_confirm" else "已取消 ❌" if event_key == "btn_cancel" else "已提交 ✅" if is_submit else "已收到 ✅"
                                    },
                                    "sub_title_text": (f"「{title}」— 你选了：{sel_text}" if sel_text else f"「{title}」— Muse 已收到，正在处理后续。")[:112],
                                    # text_notice updates are rejected without a
                                    # valid card_action (errcode 42045, verified
                                    # live 2026-10-04: every click update failed
                                    # and the card never changed). A neutral
                                    # same-site URL satisfies the requirement.
                                    "card_action": {"type": 1, "url": "https://open.work.weixin.qq.com/"},
                                    "task_id": task_id,
                                },
                            },
                        },
                        wait_response=True,
                        timeout=5.0,
                    )
                    log(f"template_card_event {task_id} key={event_key}: card update errcode={resp.get('errcode')}")
                except Exception as e:
                    log(f"template_card_event {task_id}: card update failed: {e!r}")
            self.write_status()
        elif etype == "feedback_event":
            if not self._event_allowed(from_userid):
                return
            event_msgid = str(body.get("msgid") or "")
            if event_msgid and event_msgid in self.seen_msgids:
                log(f"duplicate event msgid {event_msgid}, skipped")
                return
            # The user gave feedback (e.g. like/dislike) on a bot reply.
            # Stream replies carry feedback.id="fb-<msgid>" (set on the
            # first stream frame), so the verdict can be traced back. The
            # server payload beyond eventtype is not fully documented (the
            # SDK types FeedbackEventData as eventtype only), so keep the
            # raw event in the entry and surface whatever fields arrived.
            # Recorded as NON-auto-handled so the agent wake hook sees it,
            # same as template_card_event; an event req_id cannot take a
            # stream reply, so register it for proactive follow-up.
            fb = event.get("feedback") or event.get("feedback_event") or {}
            if not isinstance(fb, dict):
                fb = {}
            fb_id = (
                fb.get("id", "")
                or fb.get("feedback_id", "")
                or event.get("feedback_id", "")
                or event.get("id", "")
            )
            fb_type = (
                fb.get("type", "")
                or fb.get("feedback_type", "")
                or event.get("feedback_type", "")
                or event.get("type", "")
            )
            orig_msgid = fb_id[3:] if isinstance(fb_id, str) and fb_id.startswith("fb-") else ""
            extras = {k: v for k, v in event.items() if k != "eventtype"}
            fb_text = f"[用户反馈] feedback_id={fb_id or '?'} type={fb_type or '?'}"
            if orig_msgid:
                fb_text += f" 原消息msgid={orig_msgid}"
            if extras:
                fb_text += f" 原始事件={json.dumps(extras, ensure_ascii=False)[:500]}"
            msgid = body.get("msgid", "")
            chattype = body.get("chattype", "")
            chatid = body.get("chatid") or from_userid
            if msgid and req_id:
                self.reqmap[msgid] = {
                    "req_id": req_id,
                    "stream_id": "",
                    "chatid": chatid,
                    "chattype": chattype,
                    "event": True,
                }
                self._save_reqmap()
            self.append_jsonl(
                INBOX,
                {
                    "msgid": msgid,
                    "ts": int(time.time()),
                    "chattype": chattype,
                    "chatid": chatid,
                    "from_userid": from_userid,
                    "msgtype": "event",
                    "text": fb_text,
                    "media": [],
                    "auto_handled": False,
                    "raw": body,
                },
            )
            self.msgs_received += 1
            self._mark_seen(str(msgid or ""))
            log(f"feedback_event id={fb_id} type={fb_type} orig={orig_msgid}")
            self.write_status()
        else:
            self.append_jsonl(
                INBOX,
                {
                    "msgid": body.get("msgid", ""),
                    "ts": int(time.time()),
                    "chattype": body.get("chattype", ""),
                    "chatid": body.get("chatid") or from_userid,
                    "from_userid": from_userid,
                    "msgtype": "event",
                    "text": "",
                    "auto_handled": True,
                    "raw": body,
                },
            )

    async def reader_loop(self) -> None:
        async for raw in self.ws:
            try:
                frame = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                log(f"unparsable frame: {str(raw)[:200]}")
                continue
            cmd = frame.get("cmd")
            if cmd == "aibot_msg_callback":
                # Spawn as a task: handlers send respond frames and await their
                # responses, which only this loop can read. Awaiting inline
                # deadlocks every respond until timeout.
                asyncio.create_task(self.handle_message_callback(frame))
            elif cmd == "aibot_event_callback":
                asyncio.create_task(self.handle_event_callback(frame))
            else:
                req_id = frame.get("headers", {}).get("req_id", "")
                fut = self.pending_responses.pop(req_id, None)
                if fut is not None and not fut.done():
                    fut.set_result(frame)
                elif frame.get("errcode") not in (None, 0):
                    log(f"async error frame req_id={req_id}: {json.dumps(frame, ensure_ascii=False)[:300]}")

    # ---------- heartbeat ----------

    async def heartbeat_loop(self) -> None:
        missed = 0
        while True:
            await asyncio.sleep(HEARTBEAT_SECS)
            try:
                resp = await self.send_frame(
                    {"cmd": "ping", "headers": {"req_id": self.new_req_id("ping")}},
                    wait_response=True,
                )
                if resp.get("errcode") != 0:
                    # SDK behavior: tolerate 1 missed pong, reconnect on 2nd
                    missed += 1
                    log(f"heartbeat missed ({missed}/2): errcode={resp.get('errcode')}")
                    if missed < 2:
                        continue
                    self.last_error = f"heartbeat errcode={resp.get('errcode')}"
                    self.write_status()
                    return
                missed = 0
            except Exception as e:  # connection likely dead
                log(f"heartbeat exception: {e}")
                return

    # ---------- media ----------

    def _http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None
            self._http = httpx.AsyncClient(proxy=proxy, trust_env=False, follow_redirects=True)
        return self._http

    async def _download_media(self, media: list[dict], msgid: str) -> None:
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        client = self._http_client()
        for i, m in enumerate(media):
            url, aeskey = m.get("url", ""), m.get("aeskey", "")
            if not url or not aeskey:
                continue
            last_error: Exception | None = None
            for _attempt in range(3):
                try:
                    r = await client.get(url, timeout=httpx.Timeout(20.0, connect=10.0))
                    r.raise_for_status()
                    data = r.content
                    if len(data) > 25 * 1024 * 1024:
                        log(f"media {msgid}/{i}: too large ({len(data)} bytes), skipped")
                        last_error = None
                        break
                    plain = decrypt_wecom_media(data, aeskey)
                    path = MEDIA_DIR / media_filename(msgid, i, sniff_ext(plain))
                    path.write_bytes(plain)
                    m["local_path"] = str(path)
                    log(f"media {msgid}/{i}: saved {path} ({len(plain)} bytes)")
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    await asyncio.sleep(1)
            if last_error is not None:
                log(f"media {msgid}/{i}: download/decrypt failed: {last_error!r}")

    async def upload_media(self, data: bytes, mtype: str, filename: str) -> str:
        chunk_size = 512 * 1024
        total_chunks = max(1, (len(data) + chunk_size - 1) // chunk_size)
        if total_chunks > 100:
            raise ValueError("file too large for WeCom upload (>100 chunks)")
        resp = await self.send_frame(
            {
                "cmd": "aibot_upload_media_init",
                "headers": {"req_id": self.new_req_id("upinit")},
                "body": {
                    "type": mtype,
                    "filename": filename,
                    "total_size": len(data),
                    "total_chunks": total_chunks,
                    "md5": hashlib.md5(data).hexdigest(),
                },
            },
            wait_response=True,
            timeout=30.0,
        )
        if resp.get("errcode") != 0:
            raise RuntimeError(f"upload init failed: {resp.get('errcode')} {resp.get('errmsg')}")
        upload_id = (resp.get("body") or {}).get("upload_id", "")
        if not upload_id:
            raise RuntimeError("upload init returned no upload_id")
        for idx in range(total_chunks):
            part = data[idx * chunk_size : (idx + 1) * chunk_size]
            frame = {
                "cmd": "aibot_upload_media_chunk",
                "headers": {"req_id": self.new_req_id("upchunk")},
                "body": {
                    "upload_id": upload_id,
                    "chunk_index": idx,
                    "base64_data": base64.b64encode(part).decode(),
                },
            }
            r = await self.send_frame(frame, wait_response=True, timeout=30.0)
            if r.get("errcode") != 0:  # one retry per chunk, like the SDK
                frame["headers"]["req_id"] = self.new_req_id("upchunk")
                r = await self.send_frame(frame, wait_response=True, timeout=30.0)
            if r.get("errcode") != 0:
                raise RuntimeError(f"upload chunk {idx} failed: {r.get('errcode')} {r.get('errmsg')}")
        resp = await self.send_frame(
            {
                "cmd": "aibot_upload_media_finish",
                "headers": {"req_id": self.new_req_id("upfin")},
                "body": {"upload_id": upload_id},
            },
            wait_response=True,
            timeout=30.0,
        )
        if resp.get("errcode") != 0:
            raise RuntimeError(f"upload finish failed: {resp.get('errcode')} {resp.get('errmsg')}")
        media_id = (resp.get("body") or {}).get("media_id", "")
        if not media_id:
            raise RuntimeError("upload finish returned no media_id")
        return media_id

    async def check_stream_watchdog(self) -> None:
        # Streams auto-expire at 10 min server-side; finish stale ones at ~9
        # min with an explanation instead of leaving a dead "thinking" bubble.
        now = time.time()
        stale = [mid for mid, s in self.open_streams.items() if now - s["since"] > 540]
        for mid in stale:
            s = self.open_streams.pop(mid, None)
            if not s:
                continue
            try:
                await self.respond(
                    s["req_id"],
                    {
                        "msgtype": "stream",
                        "stream": {
                            "id": s["stream_id"],
                            "finish": True,
                            "content": "⏱ 这个任务处理得有点久，我先把这条收尾。有结果了会另发消息告诉你。",
                        },
                    },
                )
                log(f"stream watchdog: finished stale stream for msg {mid}")
            except Exception as e:
                log(f"stream watchdog failed for {mid}: {e}")

    # ---------- outbox ----------

    def _outbox_offset(self) -> int:
        return read_offset(OUTBOX_OFFSET)

    async def outbox_loop(self) -> None:
        """Send outbox rows. A transient failure stays at the head and backs off.

        Dead-letter after SEND_MAX_ATTEMPTS so one poison row cannot block
        the channel, and a single timeout cannot drop the only copy.
        """
        while True:
            await asyncio.sleep(1.0)
            await self.check_stream_watchdog()
            if not OUTBOX.exists():
                continue
            offset = self._outbox_offset()
            try:
                size = OUTBOX.stat().st_size
                if size < offset:
                    offset = 0
                if size == offset:
                    continue
                with OUTBOX.open("rb") as handle:
                    handle.seek(offset)
                    data = handle.read()
            except OSError as e:
                log(f"outbox read error: {e}")
                continue
            consumed = 0
            retry = self._load_retry()
            for raw_line in complete_jsonl_lines(data):
                parsed = parse_jsonl_line(raw_line)
                if parsed is None or parsed.get("__invalid__"):
                    if parsed and parsed.get("__invalid__"):
                        log(f"outbox: skipping bad line at offset {offset + consumed}")
                    consumed += len(raw_line) + 1
                    try:
                        write_offset(OUTBOX_OFFSET, offset + consumed)
                    except OSError:
                        pass
                    continue
                item = parsed
                item_id = str(item.get("id") or "")
                rec = retry.get(item_id) if item_id else None
                if rec and float(rec.get("next") or 0) > time.time():
                    try:
                        write_offset(OUTBOX_OFFSET, offset + consumed)
                    except OSError:
                        pass
                    await asyncio.sleep(min(float(rec["next"]) - time.time(), 30.0))
                    break
                ok = await self.dispatch_outbox_item(item)
                if not ok:
                    attempts = int((rec or {}).get("n") or 0) + 1
                    if item_id and attempts >= SEND_MAX_ATTEMPTS:
                        self.append_jsonl(OUTBOX_RESULTS, {
                            "id": item_id,
                            "mode": item.get("mode", ""),
                            "ts": int(time.time()),
                            "ok": False,
                            "errmsg": f"dead-lettered after {attempts} failed attempts; queue unblocked",
                            "deadletter": True,
                        })
                        log(f"outbox {item.get('mode')} {item_id}: dead-lettered after {attempts} attempts")
                        retry.pop(item_id, None)
                        self._save_retry(retry)
                        ok = True
                    else:
                        if item_id:
                            retry[item_id] = {
                                "n": attempts,
                                "next": time.time() + retry_backoff_secs(attempts),
                            }
                            self._save_retry(retry)
                        try:
                            write_offset(OUTBOX_OFFSET, offset + consumed)
                        except OSError:
                            pass
                        await asyncio.sleep(2.0)
                        break
                if ok:
                    if item_id and item_id in retry:
                        retry.pop(item_id, None)
                        self._save_retry(retry)
                    consumed += len(raw_line) + 1
                    try:
                        write_offset(OUTBOX_OFFSET, offset + consumed)
                    except OSError:
                        pass

    async def dispatch_outbox_item(self, item: dict) -> bool:
        item_id = item.get("id", "")
        mode = item.get("mode", "")
        content = item.get("content", "")
        if mode == "reply":
            content = normalize_subagent_content(item.get("msgid", ""), content)
        result = {"id": item_id, "mode": mode, "ts": int(time.time()), "ok": False}
        _mid = str(item.get("msgid", "") or "")
        if _mid and _mid in self._cancelled_ids():
            result["errmsg"] = "cancelled"
            result["cancelled"] = True
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: skipped, cancelled msgid={_mid}")
            return True
        if mode in ("update", "reply_file") and _mid and self._delivered_reply_before(item):
            result["errmsg"] = "suppressed: msgid already has a delivered formal reply"
            result["suppressed"] = True
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: skipped, already replied msgid={_mid}")
            return False  # consumed by the outbox loop; never reaches the user
        try:
            if mode == "reply" and self.reqinfo(item.get("msgid", "")).get("event"):
                # Follow-up to a template_card_event: the event req_id has
                # no stream to close and cannot take a normal reply, so
                # deliver the answer as proactive message(s) to that chat.
                info = self.reqinfo(item.get("msgid", ""))
                target_chatid = info.get("chatid", "")
                if not target_chatid:
                    result["errmsg"] = f"no chatid stored for event msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply {item_id}: {result['errmsg']}")
                    return False
                chat_type = 2 if info.get("chattype") == "group" else 1
                resp = {"errcode": 0}
                for part in split_chunks(content):
                    resp = await self.send_frame(
                        {
                            "cmd": "aibot_send_msg",
                            "headers": {"req_id": self.new_req_id("send")},
                            "body": {
                                "chatid": target_chatid,
                                "chat_type": chat_type,
                                "msgtype": "markdown",
                                "markdown": {"content": part},
                            },
                        },
                        wait_response=True,
                    )
                    if resp.get("errcode") != 0:
                        break
            elif mode == "reply":
                info = self.reqinfo(item.get("msgid", ""))
                orig_req_id = info.get("req_id", "")
                if not orig_req_id:
                    result["errmsg"] = f"no req_id stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply {item_id}: {result['errmsg']}")
                    return False
                stream_id = info.get("stream_id", "")
                chunks = split_chunks(content)
                resp = None
                if stream_id:
                    # Close the stream opened when the message arrived: this
                    # replaces the "thinking" placeholder with the answer.
                    resp = await self.respond(
                        orig_req_id,
                        {
                            "msgtype": "stream",
                            "stream": {"id": stream_id, "finish": True, "content": chunks[0]},
                        },
                    )
                    if resp.get("errcode") == -1:
                        log(f"outbox reply {item_id}: stream finish ack timed out; retrying the stream, not a second markdown")
                    elif resp.get("errcode") != 0:
                        log(f"outbox reply {item_id}: stream finish failed errcode={resp.get('errcode')}, falling back to markdown")
                        resp = None
                if resp is None:
                    resp = await self.respond(
                        orig_req_id,
                        {"msgtype": "markdown", "markdown": {"content": chunks[0]}},
                    )
                # Overflow chunks (reply longer than CHUNK_LIMIT) follow as
                # active messages to the same chat.
                if resp.get("errcode") == 0 and len(chunks) > 1:
                    target_chatid = info.get("chatid", "")
                    chat_type = 2 if info.get("chattype") == "group" else 1
                    if not target_chatid:
                        resp = {"errcode": -2, "errmsg": "no chatid stored for overflow chunks"}
                    else:
                        for extra in chunks[1:]:
                            resp = await self.send_frame(
                                {
                                    "cmd": "aibot_send_msg",
                                    "headers": {"req_id": self.new_req_id("send")},
                                    "body": {
                                        "chatid": target_chatid,
                                        "chat_type": chat_type,
                                        "msgtype": "markdown",
                                        "markdown": {"content": extra},
                                    },
                                },
                                wait_response=True,
                            )
                            if resp.get("errcode") != 0:
                                break
            elif mode == "update":
                info = self.reqinfo(item.get("msgid", ""))
                orig_req_id = info.get("req_id", "")
                stream_id = info.get("stream_id", "")
                if not orig_req_id or not stream_id:
                    result["errmsg"] = f"no open stream for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox update {item_id}: {result['errmsg']}")
                    return False
                resp = await self.respond(
                    orig_req_id,
                    {
                        "msgtype": "stream",
                        "stream": {"id": stream_id, "finish": False, "content": content},
                    },
                )
            elif mode == "reply_file":
                info = self.reqinfo(item.get("msgid", ""))
                orig_req_id = info.get("req_id", "")
                if not orig_req_id:
                    result["errmsg"] = f"no req_id stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply_file {item_id}: {result['errmsg']}")
                    return False
                fpath = item.get("file_path", "")
                file_path = Path(fpath)
                if not outbound_file_allowed(file_path, CRED_FILE, [muse_home(), STATE, BASE, Path("/tmp")]):
                    result["errmsg"] = f"file path is not allowed: {fpath}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply_file {item_id}: {result['errmsg']}")
                    return True
                data = file_path.read_bytes()
                mtype = media_type_for_path(fpath)
                media_id = await self.upload_media(data, mtype, file_path.name)
                resp = await self.respond(
                    orig_req_id,
                    {"msgtype": mtype, mtype: {"media_id": media_id}},
                )
            elif mode in ("send_text_notice", "send_news_notice", "send_vote", "send_multiple", "send_card"):
                chatid = item.get("chatid", "")
                chat_type = int(item.get("chat_type", 1))
                if mode == "send_text_notice":
                    card = build_text_notice_card(
                        item.get("title", ""), content, item.get("task_id", ""), item.get("url", "")
                    )
                elif mode == "send_news_notice":
                    card = build_news_notice_card(
                        item.get("title", ""),
                        content,
                        item.get("image_url", ""),
                        item.get("task_id", ""),
                        item.get("url", ""),
                    )
                elif mode == "send_vote":
                    card = build_vote_card(
                        item.get("title", ""),
                        content,
                        item.get("options") or [],
                        bool(item.get("multi")),
                        item.get("task_id", ""),
                    )
                elif mode == "send_multiple":
                    card = build_multiple_card(
                        item.get("title", ""), content, item.get("groups") or [], item.get("task_id", "")
                    )
                else:  # send_card: caller supplied a full template_card dict
                    card = item.get("card") or {}
                resp = await self.send_frame(
                    {
                        "cmd": "aibot_send_msg",
                        "headers": {"req_id": self.new_req_id("send")},
                        "body": {
                            "chatid": chatid,
                            "chat_type": chat_type,
                            "msgtype": "template_card",
                            "template_card": card,
                        },
                    },
                    wait_response=True,
                )
                if resp.get("errcode") == 0:
                    self._register_card(card, chatid)
                    result["task_id"] = card.get("task_id", "")
            elif mode == "send_confirm":
                chatid = item.get("chatid", "")
                chat_type = int(item.get("chat_type", 1))
                selection = None
                if item.get("selection_options"):
                    selection = {
                        "title": item.get("selection_title", ""),
                        "options": item.get("selection_options") or [],
                    }
                card = build_confirm_card(
                    item.get("title", "") or "请确认",
                    content,
                    item.get("task_id", ""),
                    selection,
                )
                resp = await self.send_frame(
                    {
                        "cmd": "aibot_send_msg",
                        "headers": {"req_id": self.new_req_id("send")},
                        "body": {
                            "chatid": chatid,
                            "chat_type": chat_type,
                            "msgtype": "template_card",
                            "template_card": card,
                        },
                    },
                    wait_response=True,
                )
                if resp.get("errcode") == 0:
                    self._register_card(card, chatid)
                    result["task_id"] = card["task_id"]
            elif mode == "reply_confirm":
                info = self.reqinfo(item.get("msgid", ""))
                orig_req_id = info.get("req_id", "")
                if not orig_req_id:
                    result["errmsg"] = f"no req_id stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply_confirm {item_id}: {result['errmsg']}")
                    return False
                card = build_confirm_card(
                    item.get("title", "") or "请确认",
                    item.get("desc", ""),
                    item.get("task_id", ""),
                )
                stream_id = info.get("stream_id", "")
                if stream_id:
                    # Close the open stream and attach the card in one
                    # stream_with_template_card reply (official pattern).
                    resp = await self.respond(
                        orig_req_id,
                        {
                            "msgtype": "stream_with_template_card",
                            "stream": {
                                "id": stream_id,
                                "finish": True,
                                "content": content or card["main_title"]["title"],
                            },
                            "template_card": card,
                        },
                    )
                else:
                    resp = await self.respond(
                        orig_req_id,
                        {"msgtype": "template_card", "template_card": card},
                    )
                if resp.get("errcode") == 0:
                    self._register_card(card, info.get("chatid", ""), str(item.get("msgid", "")))
                    result["task_id"] = card["task_id"]
                    self.open_streams.pop(str(item.get("msgid", "")), None)
            elif mode == "send":
                chatid = item.get("chatid", "")
                chat_type = int(item.get("chat_type", 1))
                resp = {"errcode": 0}
                for part in split_chunks(content):
                    resp = await self.send_frame(
                        {
                            "cmd": "aibot_send_msg",
                            "headers": {"req_id": self.new_req_id("send")},
                            "body": {
                                "chatid": chatid,
                                "chat_type": chat_type,
                                "msgtype": "markdown",
                                "markdown": {"content": part},
                            },
                        },
                        wait_response=True,
                    )
                    if resp.get("errcode") != 0:
                        break
            elif mode == "send_file":
                chatid = item.get("chatid", "")
                chat_type = int(item.get("chat_type", 1))
                fpath = item.get("file_path", "")
                file_path = Path(fpath)
                if not outbound_file_allowed(file_path, CRED_FILE, [muse_home(), STATE, BASE, Path("/tmp")]):
                    result["errmsg"] = f"file path is not allowed: {fpath}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox send_file {item_id}: {result['errmsg']}")
                    return True
                data = file_path.read_bytes()
                mtype = media_type_for_path(fpath)
                media_id = await self.upload_media(data, mtype, file_path.name)
                resp = await self.send_frame(
                    {
                        "cmd": "aibot_send_msg",
                        "headers": {"req_id": self.new_req_id("send")},
                        "body": {
                            "chatid": chatid,
                            "chat_type": chat_type,
                            "msgtype": mtype,
                            mtype: {"media_id": media_id},
                        },
                    },
                    wait_response=True,
                )
            else:
                result["errmsg"] = f"unknown mode {mode}"
                self.append_jsonl(OUTBOX_RESULTS, result)
                return False
            result["errcode"] = resp.get("errcode")
            result["errmsg"] = resp.get("errmsg", "")
            result["ok"] = resp.get("errcode") == 0
            if result["ok"]:
                self.msgs_sent += 1
                if mode in ("reply", "reply_file", "reply_confirm"):
                    self.open_streams.pop(str(item.get("msgid", "")), None)
        except Exception as e:
            result["errmsg"] = f"exception: {e}"
        self.append_jsonl(OUTBOX_RESULTS, result)
        log(f"outbox {mode} {item_id}: ok={result['ok']} err={result.get('errmsg', '')}")
        self.write_status()
        return bool(result["ok"])

    # ---------- connection lifecycle ----------

    async def run_session(self, bot_id: str, secret: str) -> str:
        """Returns a short reason string when the session ends."""
        async with websockets.connect(
            WS_URL, open_timeout=15, close_timeout=5, max_size=10 * 1024 * 1024, ping_interval=None
        ) as ws:
            self.ws = ws
            sub_req = self.new_req_id("sub")
            await ws.send(
                json.dumps(
                    {
                        "cmd": "aibot_subscribe",
                        "headers": {"req_id": sub_req},
                        "body": {"bot_id": bot_id, "secret": secret},
                    }
                )
            )
            raw = await asyncio.wait_for(ws.recv(), timeout=12)
            resp = json.loads(raw)
            if resp.get("errcode") != 0:
                self.last_error = f"subscribe errcode={resp.get('errcode')}: {resp.get('errmsg', '')}"
                log(f"subscribe FAILED: {self.last_error}")
                self.state = "auth_failed" if resp.get("errcode") == 853000 else "subscribe_failed"
                self.connected = False
                self.write_status()
                return self.state
            self.connected = True
            self.state = "connected"
            self.last_error = ""
            log("subscribed OK; long connection established")
            self.write_status()
            tasks = [
                asyncio.create_task(self.reader_loop()),
                asyncio.create_task(self.heartbeat_loop()),
                asyncio.create_task(self.outbox_loop()),
            ]
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                for t in done:
                    exc = t.exception()
                    if exc:
                        return f"task error: {exc}"
                return "session ended"
            finally:
                self.connected = False
                self.ws = None

    async def run(self) -> None:
        backoff = 5
        while True:
            bot_id, secret, allow_users = load_credentials()
            self.allow_users = allow_users
            if not bot_id or not secret:
                if self.state != "awaiting_credentials":
                    self.state = "awaiting_credentials"
                    log("no credentials configured yet; waiting (re-checking every 30s)")
                    self.write_status()
                await asyncio.sleep(30)
                continue
            started = time.time()
            try:
                reason = await self.run_session(bot_id, secret)
            except Exception as e:
                reason = f"exception: {e}"
                self.last_error = reason
            lived = time.time() - started
            self.connected = False
            if self.state == "auth_failed":
                # wrong credentials will not fix themselves; also avoids
                # tripping WeCom's subscribe rate protection
                self.state = "auth_failed"
                self.write_status()
                log("auth failed; sleeping 300s before retry (fix credentials to recover sooner: restart service)")
                await asyncio.sleep(300)
                self.state = "reconnecting"
                continue
            self.state = "reconnecting"
            self.write_status()
            if self._kicked:
                reason = "kicked by a newer connection"
                backoff = max(backoff, 60)
                self._kicked = False
            elif lived >= 60:
                backoff = 5
            log(f"session ended ({reason}); reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)


def main() -> None:
    try:
        gw = Gateway()
    except RuntimeError as e:
        log(f"refusing to start: {e}")
        sys.exit(1)
    gw.write_status()
    try:
        asyncio.run(gw.run())
    except KeyboardInterrupt:
        log("stopped by signal")
        gw.state = "stopped"
        gw.write_status()


if __name__ == "__main__":
    sys.exit(main())
