#!/usr/bin/env python3
"""Weixin (personal WeChat) iLink gateway for Muse.

Implements Tencent's iLink bot protocol (the same one the official
@tencent-weixin/openclaw-weixin plugin uses) directly, without OpenClaw:

- long-poll POST /ilink/bot/getupdates for inbound messages
- POST /ilink/bot/sendmessage for replies / proactive sends
- every inbound message must be answered with its context_token echoed back
- errcode -14 means the login session expired; a fresh QR scan is required

Messages land in state/inbox.jsonl; a hook wakes the agent, which replies
through the CLI (outbox.jsonl). Login is a separate one-shot script
(login.py) that stores the bot token in the credentials file.
"""

import asyncio
import base64
import fcntl
import json
import os
import random
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
OUTBOX_RESULTS = STATE / "outbox_results.jsonl"
OUTBOX_OFFSET = STATE / "outbox.offset"
STATUS = STATE / "status.json"
SYNC_FILE = STATE / "sync.json"
CONTEXT_FILE = STATE / "context.json"
LOCK_FILE = STATE / "gateway.lock"
CANCELLED_FILE = STATE / "cancelled.json"
CRED_FILE = Path(
    os.environ.get(
        "ILINK_CRED_FILE", str(Path.home() / ".config" / "weixin-bot" / "credentials.env")
    )
)

DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"
MEDIA_DIR = STATE / "media"
CHANNEL_VERSION = "2.4.9"
CLIENT_VERSION = str((2 << 16) | (4 << 8) | 9)  # 0x00MMNNPP encoding of 2.4.9
BOT_AGENT = "Muse/1.0"
CHUNK_LIMIT = 4000
STALE_TOKEN_ERRCODE = -14


def log(msg: str) -> None:
    print(f"[weixin-gateway {time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ---------- slash commands (gateway-intercepted) ----------
# The user can send /ping, /status, /stop, /new, /jump, /check,
# /queue, /help and /subagent in chat. Commands are intercepted
# after the allowlist +
# dedupe checks and BEFORE the inbox write: they never wake a worker;
# the gateway answers itself through the normal proactive send path.
# Unknown slash text (e.g. /foo) is NOT a command and flows through as
# an ordinary message.
HOOK_STATE_DIR = Path(os.environ.get("HOME") or "/home/hatch") / "hooks" / "state" / "weixin-bot"
CLI_WRAPPER = BASE / "weixin"
CHAN_LABEL = "个人微信"

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


def _queue_position(other_items, running_ids, now):
    """1-based queue position for a newly arrived message, given the
    sender's other still-unanswered messages as (msgid, record) pairs.

    Only messages that are themselves WAITING count as ahead of the
    new one. Messages in service — the in-flight batch (active +
    detached, per the hook state) — are being served, not queued,
    and must not inflate the number (live bug 2026-10-05: a message
    right behind an in-service burst was told it was 2nd, then the
    next 3rd, then 4th in queue, because every unanswered earlier
    message was counted). When the hook snapshot lags and shows no
    batch yet, the earliest burst cluster among the others is the
    one being picked up, so it counts as in service too; every
    later other is genuinely queued ahead of the new message."""
    running = {str(m) for m in (running_ids or [])}
    if not running and other_items:
        t0 = min(float(r.get("ts") or now) for _m, r in other_items)
        running = {str(m) for m, r in other_items
                   if float(r.get("ts") or now) - t0
                   <= BURST_MERGE_WINDOW_SECS}
    ahead = sum(1 for m, _r in other_items if str(m) not in running)
    return ahead + 1


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

# Thinking notice (user request, 2026-10-04): WeCom shows a native
# thinking placeholder via its stream <think></think> first frame;
# personal WeChat / iLink has no in-place stream (a GENERATING->FINISH
# bubble was verified live 2026-10-03 to stick on "thinking" forever),
# so the Weixin equivalent is a standalone fresh message carrying an
# emoji plus a short text, queued as an unbound outbox "send" row the
# moment a normal message arrives — the same fire-and-forget path as
# the soft ack. It carries no msgid, so it never counts as batch
# activity/completion, and the late-suppression gate (which only
# applies to update/reply_file) never touches it.
THINKING_NOTICE_TEXT = "🤔 正在思考中，请稍等…"
# Burst guard: a quick follow-up within the cooldown reuses the
# notice already on screen instead of stacking another one per
# message. Replies normally take longer than this window, so a
# genuinely new round after an answer still gets its own notice in
# the common case.
THINKING_NOTICE_COOLDOWN_SECS = 20

# ---------- scenario-aware arrival feedback (2026-10-04, round 2) ----------
# Round 1 shipped exactly one notice (idle -> "thinking") plus the
# older busy soft ack, and the user called that out: queueing and the
# other situations each need their own truthful signal instead of one
# generic "thinking" slapped on everything. The gateway now classifies
# every normal inbound message into exactly ONE arrival scenario and
# sends at most one immediate notice for it:
#   idle     nothing unanswered       -> THINKING_NOTICE_TEXT
#   queued   a batch is in flight     -> SOFT_ACK_TEMPLATE (position N)
#   merged   burst supplement within BURST_MERGE_WINDOW_SECS of a
#            still-unanswered message -> MERGED_ACK_TEXT (once/window)
#   stop     stop/cancel imperative   -> STOP_ACK_TEXT (round 1 sent
#            NOTHING for these; the hook force-releases them)
#   media    image/file/video, idle   -> MEDIA_ACK_TEMPLATE (what was
#            received, instead of a generic "thinking")
# Two follow-up notices can come later for a QUEUED message, driven by
# _feedback_scan_once polling the hook state (see Gateway):
#   started  it moved pending -> active/detached: STARTED_NOTICE_TEMPLATE
#   waiting  still pending after WAIT_REMIND_SECS (once):
#            WAIT_REMIND_TEMPLATE
# Cooldown fix: the old flat 20s thinking cooldown could eat the
# notice of a genuinely NEW round whose predecessor was answered
# quickly. Tracking is now per-message and a delivered formal reply
# clears that message's record (see _feedback_on_reply), so the idle
# path needs no time cooldown at all; burst control lives in the
# merged branch, which only fires while a message is unanswered.
STOP_ACK_TEXT = "🛑 收到停止请求，正在优先处理。"
MERGED_ACK_TEXT = "📩 已收到补充，会和前面一条一起处理。"
MEDIA_ACK_TEMPLATE = "📎 已收到{what}，正在处理…"
STARTED_NOTICE_TEMPLATE = "▶️ 排到你了，开始处理：「{excerpt}」"
WAIT_REMIND_TEMPLATE = "⏳ 还在排队（第 {n} 位）：前面任务还没结束，已等{dur}；/jump 插队、/stop 取消"
BURST_MERGE_WINDOW_SECS = 8
WAIT_REMIND_SECS = 180


def batch_in_flight(state_dir, hook_state_dir):
    """True when a batch is in flight: active_batch.json carries
    msgids + a valid since, and no bound reply row for any of those
    msgids has landed in the outbox at/after since-2s (the hook's own
    batch-finish rule; detached batches do not count). Shared by the
    soft ack and the arrival-feedback classifier. Fail-silent False."""
    try:
        batch, _ids, _pcount, _dcount = _queue_summary(hook_state_dir)
        ids = [str(m) for m in (batch.get("msgids") or [])]
        if not ids:
            return False
        since = batch.get("since")
        if not isinstance(since, (int, float)) or since <= 0:
            return False
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
                        return False  # batch already finished
        except OSError:
            pass  # no outbox file yet = no reply rows = still in flight
        return True
    except Exception:
        return False


def soft_ack_text(state_dir, hook_state_dir, text, position=None):
    """Busy-queue soft ack for a normal inbound message, or None.

    Returns the ack string only when a batch is in flight (see
    batch_in_flight). Stop imperatives get no ack. Any state problem
    -> None (fail-silent: the message itself is always queued exactly
    as before).

    Position N = the hook's pending count + 1 (this message), unless
    the caller passes a locally computed position. The gateway reads
    the hook's pending.json snapshot, so messages that arrived after
    the hook's last poll are not registered in it yet and a fast burst
    can repeat the same N — the classifier corrects for that with its
    own per-message tracking and passes position explicitly.
    """
    try:
        if is_stop_request(text):
            return None
        if not batch_in_flight(state_dir, hook_state_dir):
            return None
        if position is None:
            _batch, _ids, pcount, _dcount = _queue_summary(hook_state_dir)
            position = pcount + 1
        return SOFT_ACK_TEMPLATE.format(n=position)
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
    """Allocate the next job id (S<n>) from the sequence file in the
    gateway state dir. This process is the only writer of that file,
    so a plain read/increment/replace is race-free here. Ids keep
    increasing across restarts and are never reused."""
    p = state_dir / "subagent_seq.json"
    data = _read_json_file(p, {}) or {}
    n = data.get("next") if isinstance(data, dict) else None
    if not isinstance(n, int) or n < 1:
        n = 1
    try:
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps({"next": n + 1}), encoding="utf-8")
        os.replace(tmp, p)
    except OSError:
        pass
    return f"S{n}"


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

def _forced_subagent_label(jid, content):
    """Return content whose first line opens with the authoritative
    label 【副助手 #<jid> <outcome>】. An existing label keeps its
    outcome word (完成/失败/已停止) and any text after it, with the job
    id corrected to jid; a missing label is prepended as 完成."""
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
        return f"【副助手 #{jid} 完成】\n" + text
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
            return _forced_subagent_label(str(jid), content)
    return content

def base_info() -> dict:
    return {"channel_version": CHANNEL_VERSION, "bot_agent": BOT_AGENT}


def load_credentials() -> dict:
    creds = {
        "token": os.environ.get("ILINK_BOT_TOKEN", "").strip(),
        "bot_id": os.environ.get("ILINK_BOT_ID", "").strip(),
        "user_id": os.environ.get("ILINK_USER_ID", "").strip(),
        "base_url": os.environ.get("ILINK_BASE_URL", "").strip() or DEFAULT_BASE_URL,
    }
    if CRED_FILE.exists():
        try:
            for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key == "ILINK_BOT_TOKEN" and val:
                    creds["token"] = val
                elif key == "ILINK_BOT_ID" and val:
                    creds["bot_id"] = val
                elif key == "ILINK_USER_ID" and val:
                    creds["user_id"] = val
                elif key == "ILINK_BASE_URL" and val:
                    creds["base_url"] = val
        except OSError as e:
            log(f"cannot read credentials file: {e}")
    return creds


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


# ---------- media crypto (AES-128-ECB, per the official plugin) ----------

def parse_aes_key(aes_key_b64: str) -> bytes:
    """CDNMedia.aes_key: base64 of 16 raw bytes, or base64 of a 32-char hex
    string which itself encodes the 16-byte key."""
    import base64 as _b64

    decoded = _b64.b64decode(aes_key_b64)
    if len(decoded) == 16:
        return decoded
    if len(decoded) == 32:
        try:
            return bytes.fromhex(decoded.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            pass
    raise ValueError(f"unsupported aes_key encoding ({len(decoded)} bytes decoded)")


def aes_ecb_decrypt(data: bytes, key: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    dec = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    plain = dec.update(data) + dec.finalize()
    if not plain:
        raise ValueError("empty plaintext")
    pad = plain[-1]
    if pad < 1 or pad > 16 or pad > len(plain) or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError(f"bad PKCS#7 padding: {pad}")
    return plain[:-pad]


def aes_ecb_encrypt(data: bytes, key: bytes) -> bytes:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    pad = 16 - (len(data) % 16)
    padded = data + bytes([pad]) * pad
    enc = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return enc.update(padded) + enc.finalize()


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
    if data[4:8] == b"ftyp":
        return "mp4"
    if data[:9] == b"#!SILK_V3" or data[:10] == b"\x02#!SILK_V3":
        return "silk"
    if data[:5] == b"#!AMR":
        return "amr"
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "wav"
    if data[:3] == b"ID3" or data[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"):
        return "mp3"
    return "bin"


def weixin_media_type(path: str) -> int:
    ext = Path(path).suffix.lower().lstrip(".")
    if ext in ("png", "jpg", "jpeg", "gif", "webp", "bmp"):
        return 1  # image
    if ext in ("mp4", "mov"):
        return 2  # video
    return 3  # file


def filter_markdown_weixin(text: str) -> str:
    """Light markdown adaptation for the WeChat client, which renders only
    a subset of markdown: drop heading markers, unwrap images to links,
    flatten tables and rules, strip raw HTML tags."""
    import re

    out_lines = []
    for line in text.split("\n"):
        s = line.rstrip()
        if re.match(r"^\s*\|?[\s:|-]+\|[\s:|-]*$", s) and "-" in s:
            continue  # table separator row
        if s.strip().startswith("|"):
            cells = [c.strip() for c in s.strip().strip("|").split("|")]
            s = " ｜ ".join(c for c in cells if c)
        if re.match(r"^\s*(-{3,}|\*{3,}|_{3,})\s*$", s):
            continue  # horizontal rule
        s = re.sub(r"^#{1,6}\s+", "", s)
        out_lines.append(s)
    text = "\n".join(out_lines)
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", lambda m: f"{m.group(1)}：{m.group(2)}" if m.group(1) else m.group(2), text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", lambda m: m.group(1) if m.group(1) == m.group(2) else f"{m.group(1)}（{m.group(2)}）", text)
    text = re.sub(r"<[^>]{1,80}>", "", text)
    return text


class Gateway:
    def __init__(self) -> None:
        STATE.mkdir(parents=True, exist_ok=True)
        self.connected = False
        self.started_at = int(time.time())
        self.msgs_received = 0
        self.msgs_sent = 0
        self.last_error = ""
        self.state = "starting"
        self.seen_msgids: set[str] = set()
        self.context: dict[str, dict] = {}
        self.typing_tickets: dict[str, tuple[str, float]] = {}
        self.thinking_notice_at: dict[str, float] = {}
        # Scenario-aware arrival feedback: msgid -> {user, ts, excerpt,
        # queued, queued_at, started_notice, wait_reminded}. A record
        # lives from arrival until its formal reply is delivered (see
        # _feedback_on_reply); it drives the merged/burst branch, the
        # locally corrected queue position, and the started/waiting
        # transition notices from _feedback_scan_once.
        self.feedback_track: dict[str, dict] = {}
        self.merged_notice_at: dict[str, float] = {}
        self.sync_buf = ""
        self._lock_fd = None
        self._acquire_instance_lock()
        self._load_persisted()

    # ---------- persistence ----------

    def _acquire_instance_lock(self) -> None:
        STATE.mkdir(parents=True, exist_ok=True)
        fd = open(LOCK_FILE, "a+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fd.close()
            raise RuntimeError(f"another gateway instance already holds {LOCK_FILE}")
        fd.seek(0); fd.truncate(); fd.write(str(os.getpid())); fd.flush()
        self._lock_fd = fd  # keep open for process lifetime

    @staticmethod
    def _tmp_for(path: Path) -> Path:
        return path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")

    @classmethod
    def _atomic_write(cls, path: Path, text: str) -> None:
        tmp = cls._tmp_for(path)
        try:
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(path)
        finally:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass


    def _load_persisted(self) -> None:
        if INBOX.exists():
            try:
                for line in INBOX.read_text(encoding="utf-8").splitlines():
                    try:
                        entry = json.loads(line)
                        if entry.get("msgid"):
                            self.seen_msgids.add(str(entry["msgid"]))
                    except json.JSONDecodeError:
                        continue
            except OSError:
                pass
        if SYNC_FILE.exists():
            try:
                self.sync_buf = json.loads(SYNC_FILE.read_text(encoding="utf-8")).get("buf", "")
            except (OSError, json.JSONDecodeError):
                self.sync_buf = ""
        if CONTEXT_FILE.exists():
            try:
                self.context = json.loads(CONTEXT_FILE.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                self.context = {}

    def _save_sync(self) -> None:
        self._atomic_write(SYNC_FILE, json.dumps({"buf": self.sync_buf}))

    def _save_context(self) -> None:
        if len(self.context) > 200:
            self.context = dict(list(self.context.items())[-200:])
        self._atomic_write(CONTEXT_FILE, json.dumps(self.context, ensure_ascii=False))

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
        try:
            self._atomic_write(STATUS, json.dumps(status, ensure_ascii=False, indent=1))
        except OSError:
            pass

    @staticmethod
    def append_jsonl(path: Path, obj: dict) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # ---------- HTTP ----------

    def _headers(self, token: str) -> dict:
        uin = base64.b64encode(str(random.getrandbits(32)).encode()).decode()
        headers = {
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "X-WECHAT-UIN": uin,
            "iLink-App-Id": "bot",
            "iLink-App-ClientVersion": CLIENT_VERSION,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _post(self, client: httpx.AsyncClient, base_url: str, endpoint: str,
                    body: dict, token: str, timeout: float) -> dict:
        url = base_url.rstrip("/") + "/" + endpoint
        resp = await client.post(
            url, json=body, headers=self._headers(token),
            timeout=httpx.Timeout(timeout, connect=15.0),
        )
        resp.raise_for_status()
        return resp.json()

    # ---------- inbound ----------

    @staticmethod
    def _extract_text(msg: dict) -> tuple[str, list[dict]]:
        parts: list[str] = []
        media: list[dict] = []
        for item in msg.get("item_list") or []:
            itype = item.get("type")
            if itype == 1:
                t = (item.get("text_item") or {}).get("text", "")
                if t:
                    parts.append(t)
            elif itype == 3:
                v = item.get("voice_item") or {}
                t = v.get("text", "")
                if t:
                    parts.append(t)
                else:
                    parts.append("[语音]")
                # Voice audio is NOT received/downloaded (user request
                # 2026-10-04): only WeChat's own transcription in
                # `text` is used. The original .silk is never fetched
                # from the CDN or stored on disk, to avoid an extra
                # copy of the audio existing / being intercepted.
                pass
            elif itype == 2:
                media.append({"kind": "image", "item": item.get("image_item") or {}})
                parts.append("[图片]")
            elif itype == 4:
                f = item.get("file_item") or {}
                media.append({"kind": "file", "item": f})
                parts.append(f"[文件 {f.get('file_name', '')}]".rstrip())
            elif itype == 5:
                media.append({"kind": "video", "item": item.get("video_item") or {}})
                parts.append("[视频]")
            ref = item.get("ref_msg") or {}
            ref_item = ref.get("message_item") or {}
            if ref_item.get("type") == 1:
                rt = (ref_item.get("text_item") or {}).get("text", "")
                if rt:
                    parts.append(f"（引用：{rt}）")
            elif ref.get("title"):
                parts.append(f"（引用：{ref['title']}）")
        return "\n".join(parts), media

    @staticmethod
    def _sanitize_raw(msg: dict) -> dict:
        """Copy of the inbound message for the inbox log, with voice
        audio download material removed (user request 2026-10-04:
        voice audio is not received/stored). The voice item keeps its
        WeChat transcription text; its `media` node (CDN download URL /
        encrypted param / AES key) is dropped so the log never holds a
        way to fetch the original audio. Other item types are kept."""
        import copy

        raw = copy.deepcopy(msg)
        for item in raw.get("item_list") or []:
            if item.get("type") == 3:
                v = item.get("voice_item") or {}
                v.pop("media", None)
        return raw

    # ---------- slash commands ----------

    async def _send_slash_ack(self, client, creds, msg, from_user, ack):
        resp = await self.send_text(client, creds, from_user, ack,
                                    context_token=msg.get("context_token", ""))
        if isinstance(resp, dict) and resp.get("ret") not in (None, 0):
            log(f"slash ack send failed: ret={resp.get('ret')} err={resp.get('errmsg', '')}")

    async def _slash_dispatch(self, client, creds, msg, text, from_user):
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
                        _write_hook_json(HOOK_STATE_DIR, "queue_admin.json",
                                         {"ts": time.time(), "action": "clear",
                                          "msgids": [m for m, _t in pend]})
                        excerpts = "、".join(f"「{_excerpt(texts.get(m, ''))}」" for m, _t in pend)
                        ack = (f"已提交清除排队消息 {len(pend)} 条：{excerpts}。"
                               f"约 5 秒内生效。正在跑的任务不受影响。")
                elif qtokens[0] == "drop" and len(qtokens) == 2 and qtokens[1].isdigit():
                    pend = _pending_sorted(HOOK_STATE_DIR)
                    n = int(qtokens[1])
                    if n < 1 or n > len(pend):
                        ack = f"排队里没有第 {n} 条（当前共 {len(pend)} 条）。"
                    else:
                        mid = pend[n - 1][0]
                        texts = _inbox_texts(STATE)
                        _write_hook_json(HOOK_STATE_DIR, "queue_admin.json",
                                         {"ts": time.time(), "action": "drop",
                                          "msgids": [mid]})
                        ack = f"已提交删除排队第 {n} 条：「{_excerpt(texts.get(mid, ''))}」，约 5 秒内生效。"
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
                    mid = str(msg.get("message_id", "") or "")
                    if _append_jsonl_file(HOOK_STATE_DIR / "subagent_requests.jsonl",
                                          {"job_id": jid, "msgid": mid,
                                           "text": arg, "ts": time.time()}):
                        if mid:
                            # Register the reply route for the job msgid:
                            # the command never enters the inbox, so
                            # without this the job worker's formal reply
                            # would find no context and never deliver.
                            self.context[mid] = {
                                "from_user_id": from_user,
                                "group_id": msg.get("group_id", ""),
                                "context_token": msg.get("context_token", ""),
                                "client_id": "",
                            }
                            self._save_context()
                        ack = (f"【副助手 #{jid} 已派发】{_excerpt(arg)}。"
                               "后台执行中，主对话不受影响；查进度：/subagent list")
                    else:
                        ack = "派发失败：暂时无法登记副助手任务，请稍后再试。"
            elif name == "jump":
                batch, _ids, pcount, _d = _queue_summary(HOOK_STATE_DIR)
                active = bool(batch.get("msgids"))
                if not active:
                    if arg:
                        await self._send_slash_ack(client, creds, msg, from_user,
                                                   "当前没有任务在跑，这条会直接处理。")
                        return ("rewrite", arg)
                    ack = "当前没有任务在跑，下一条消息会直接处理。"
                elif arg:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json", {"ts": time.time()})
                    await self._send_slash_ack(
                        client, creds, msg, from_user,
                        f"已强制插队：排队的 {pcount + 1} 条立即处理，前面的长任务继续在跑。")
                    return ("rewrite", arg)
                elif pcount > 0:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json", {"ts": time.time()})
                    ack = f"已强制插队：排队的 {pcount} 条立即处理，前面的长任务继续在跑。"
                else:
                    _write_hook_json(HOOK_STATE_DIR, "jump_request.json",
                                     {"ts": time.time(), "armed": True})
                    ack = "已武装插队：你下一条消息会立即插队处理，前面的长任务继续在跑。"
            if ack is not None:
                await self._send_slash_ack(client, creds, msg, from_user, ack)
            log(f"slash /{name} handled for {from_user}")
            return "handled"
        except Exception as e:  # never let a command break the inbound flow
            log(f"slash dispatch error: {e!r}")
            return None

    def _queue_notice(self, from_user, content, prefix):
        """Append one unbound outbox "send" row for a feedback notice.
        Unbound rows never count as batch activity/completion and are
        never touched by the late-suppression gate."""
        self.append_jsonl(OUTBOX, {
            "id": f"{prefix}-{uuid.uuid4().hex}",
            "mode": "send",
            "to_user_id": from_user,
            "content": content,
        })
        log(f"{prefix} notice queued for {from_user}: {content}")
        return True

    def _maybe_soft_ack(self, from_user, text, position=None):
        """Queue the busy soft ack as an unbound outbox "send" row.

        Fire-and-forget: any failure is logged and swallowed so the
        ack can never disturb the inbound flow. The row carries no
        msgid, so it never counts as activity or completion for any
        batch (the standing anti-cross-talk rule for sends).
        Returns True only when an ack was actually queued."""
        try:
            ack = soft_ack_text(STATE, HOOK_STATE_DIR, text, position=position)
            if not ack:
                return False
            return self._queue_notice(from_user, ack, "softack")
        except Exception as e:
            log(f"soft ack failed (ignored): {e!r}")
            return False

    def _maybe_thinking_notice(self, from_user, text):
        """Back-compat wrapper (round 1): queue the idle thinking
        notice unless a stop imperative / cooldown applies. New code
        uses _maybe_arrival_feedback, which classifies all scenarios;
        this stays for any external caller of the round-1 behavior."""
        try:
            if not from_user or is_stop_request(text):
                return False
            now = time.time()
            last = self.thinking_notice_at.get(from_user, 0.0)
            if now - last < THINKING_NOTICE_COOLDOWN_SECS:
                return False
            self.thinking_notice_at[from_user] = now
            return self._queue_notice(from_user, THINKING_NOTICE_TEXT, "thinking")
        except Exception as e:
            log(f"thinking notice failed (ignored): {e!r}")
            return False

    def _maybe_arrival_feedback(self, from_user, text, media=None, msgid="",
                                is_group=False):
        """Scenario-aware arrival feedback (round 2): classify this
        inbound message into exactly one scenario and queue at most
        one immediate notice for it. Also registers the message in
        feedback_track so _feedback_scan_once can later send the
        started / long-wait notices for queued messages, and
        _feedback_on_reply can close the record.

        Returns the scenario name ("idle" / "media" / "queued" /
        "merged" / "stop" / "group" / "suppressed") or None on error.
        Fire-and-forget: never raises, never disturbs the inbound flow.
        Group chats get the busy soft ack only (no thinking/media/
        merged notices — anti-spam), mirroring round 1's 1:1 rule for
        the thinking notice."""
        try:
            if not from_user:
                return None
            now = time.time()
            mid = str(msgid or "")
            other_items = [(m, r) for m, r in self.feedback_track.items()
                           if m != mid and r.get("user") == from_user]
            others = [r for _m, r in other_items]
            rec = {"user": from_user, "ts": now,
                   "excerpt": _excerpt(text or "", 20),
                   "queued": False, "queued_at": 0.0,
                   "started_notice": False, "wait_reminded": False,
                   "kind": None}
            if mid:
                self.feedback_track[mid] = rec
                # prune records that never got a reply (worker died
                # without one, channel restarted in memory, ...) so
                # the dict cannot grow or poison later classifications
                for m in [m for m, r in self.feedback_track.items()
                          if now - r.get("ts", now) > 7200]:
                    self.feedback_track.pop(m, None)
            # stop / cancel imperative: round 1 sent nothing at all.
            if is_stop_request(text):
                rec["kind"] = "stop"
                self._queue_notice(from_user, STOP_ACK_TEXT, "stopack")
                return "stop"
            busy = batch_in_flight(STATE, HOOK_STATE_DIR)
            if busy:
                # Queue position: only messages that are themselves
                # still waiting count as ahead; the in-flight batch is
                # being served, not queued (see _queue_position).
                _b, running_ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
                position = _queue_position(other_items, running_ids, now)
                if self._maybe_soft_ack(from_user, text, position=position):
                    rec["kind"] = "queued"
                    rec["queued"] = True
                    rec["queued_at"] = now
                    return "queued"
                return None
            if others and not is_group:
                first_ts = min(r.get("ts", now) for r in others)
                if now - first_ts <= BURST_MERGE_WINDOW_SECS:
                    # Burst supplement to a message the hook has not
                    # picked up yet: it rides along with that batch.
                    # One notice per window; further burst messages
                    # stay silent instead of stacking.
                    last = self.merged_notice_at.get(from_user, 0.0)
                    if now - last < BURST_MERGE_WINDOW_SECS:
                        rec["kind"] = "suppressed"
                        return "suppressed"
                    self.merged_notice_at[from_user] = now
                    rec["kind"] = "merged"
                    self._queue_notice(from_user, MERGED_ACK_TEXT, "merged")
                    return "merged"
                # Earlier messages still unanswered past the burst
                # window (worker cold-starting / starting): this one
                # queues behind the ones that are themselves waiting,
                # not behind the burst already being picked up.
                _b, running_ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
                position = _queue_position(other_items, running_ids, now)
                # soft_ack_text requires a hook-visible batch, so in
                # this lag window queue the same template directly.
                rec["kind"] = "queued"
                rec["queued"] = True
                rec["queued_at"] = now
                self._queue_notice(
                    from_user, SOFT_ACK_TEMPLATE.format(n=position), "softack")
                return "queued"
            if is_group:
                rec["kind"] = "group"
                return "group"
            if media:
                kinds = {m.get("kind") for m in media if isinstance(m, dict)}
                what = ("图片" if "image" in kinds else
                        "视频" if "video" in kinds else "文件")
                rec["kind"] = "media"
                self._queue_notice(
                    from_user, MEDIA_ACK_TEMPLATE.format(what=what), "media")
                return "media"
            rec["kind"] = "idle"
            self.thinking_notice_at[from_user] = now
            self._queue_notice(from_user, THINKING_NOTICE_TEXT, "thinking")
            return "idle"
        except Exception as e:
            log(f"arrival feedback failed (ignored): {e!r}")
            return None

    def _feedback_on_reply(self, msgid):
        """Close a message's feedback record once its formal reply
        (or reply_file) has been delivered ok. This is what makes the
        next round's idle notice unconditional: no stale cooldown or
        unanswered record survives a delivered answer."""
        try:
            if msgid:
                self.feedback_track.pop(str(msgid), None)
        except Exception:
            pass

    def _feedback_scan_once(self, now=None):
        """Transition notices for queued messages (round 2): poll the
        hook state once and queue, per tracked queued message,
        - a "started" notice when it moves pending -> active (or a
          detached batch — being worked on either way), and
        - one "still waiting" reminder when it has sat in pending for
          WAIT_REMIND_SECS.
        Sends go through the unbound outbox "send" path like every
        other notice. Fail-silent; returns the number queued."""
        try:
            now = now or time.time()
            batch, _ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
            active = {str(m) for m in (batch.get("msgids") or [])}
            for d in (batch.get("detached") or []):
                if isinstance(d, dict):
                    active.update(str(m) for m in (d.get("msgids") or []))
            pend = _pending_sorted(HOOK_STATE_DIR)
            pos_map = {m: i + 1 for i, (m, _t) in enumerate(pend)}
            sent = 0
            for mid, rec in list(self.feedback_track.items()):
                if not rec.get("queued"):
                    continue
                user = rec.get("user", "")
                if not user:
                    continue
                if mid in active and not rec.get("started_notice"):
                    rec["started_notice"] = True
                    if self._queue_notice(
                            user,
                            STARTED_NOTICE_TEMPLATE.format(
                                excerpt=rec.get("excerpt", "")),
                            "started"):
                        sent += 1
                elif mid in pos_map and not rec.get("wait_reminded"):
                    waited = now - float(rec.get("queued_at") or now)
                    if waited >= WAIT_REMIND_SECS:
                        rec["wait_reminded"] = True
                        if self._queue_notice(
                                user,
                                WAIT_REMIND_TEMPLATE.format(
                                    n=pos_map[mid], dur=_dur_str(waited)),
                                "waitremind"):
                            sent += 1
            return sent
        except Exception as e:
            log(f"feedback scan failed (ignored): {e!r}")
            return 0

    async def feedback_watch_loop(self) -> None:
        """Drive _feedback_scan_once every few seconds for the life
        of a session (started alongside the outbox loop)."""
        while True:
            await asyncio.sleep(4.0)
            try:
                await asyncio.to_thread(self._feedback_scan_once)
            except Exception as e:
                log(f"feedback watch failed (ignored): {e!r}")

    async def handle_inbound(self, client: httpx.AsyncClient, creds: dict, msg: dict) -> None:
        owner_id = creds["user_id"]
        if msg.get("message_type") not in (None, 1):
            return  # only user messages
        msgid = str(msg.get("message_id", ""))
        if msgid and msgid in self.seen_msgids:
            return
        from_user = msg.get("from_user_id", "")
        if owner_id and from_user and from_user != owner_id:
            log(f"ignored message from non-owner user {from_user}")
            if msgid:
                self.seen_msgids.add(msgid)
            return
        if msgid:
            self.seen_msgids.add(msgid)
        text, media = self._extract_text(msg)
        slash_rewritten = False
        if not media:
            # Slash commands: intercepted after allowlist + dedupe,
            # before the inbox write — they never wake a worker.
            slash = await self._slash_dispatch(client, creds, msg, text, from_user)
            if slash == "handled":
                self.msgs_received += 1
                self.write_status()
                return
            if isinstance(slash, tuple):
                text = slash[1]
                slash_rewritten = True
        if media:
            await self._download_media(client, media, msgid)
        entry = {
            "msgid": msgid,
            "ts": int(time.time()),
            "from_user_id": from_user,
            "group_id": msg.get("group_id", ""),
            "text": text,
            "media": media,
            "context_token": msg.get("context_token", ""),
            "raw": self._sanitize_raw(msg),
        }
        self.append_jsonl(INBOX, entry)
        client_id = f"muse-{uuid.uuid4().hex}" if msgid else ""
        if msgid:
            self.context[msgid] = {
                "from_user_id": from_user,
                "group_id": msg.get("group_id", ""),
                "context_token": msg.get("context_token", ""),
                "client_id": client_id,
            }
            self._save_context()
        if not slash_rewritten:
            # Scenario-aware arrival feedback: exactly one immediate
            # notice per message, chosen by queue/burst/media/stop
            # state (see _maybe_arrival_feedback). A /jump rewrite
            # already got its own ack from the slash layer. Never
            # raises; queue semantics are unchanged.
            self._maybe_arrival_feedback(
                from_user, text, media=media, msgid=msgid,
                is_group=bool(msg.get("group_id")))
        self.msgs_received += 1
        log(f"msg {msgid} from {from_user}: {text[:60]!r}")
        self.write_status()
        # Feedback while the agent works: the typing indicator only. An
        # in-place GENERATING->FINISH bubble was tried first, but the user's
        # WeChat client never replaced the bubble (stuck on "thinking"
        # forever, verified live 2026-10-03), so final replies go out as
        # fresh messages instead.
        if msgid and from_user and not msg.get("group_id"):
            await self._send_typing(client, creds, from_user, msg.get("context_token", ""), 1)

    # ---------- sending ----------

    async def send_text(self, client: httpx.AsyncClient, creds: dict,
                        to_user_id: str, content: str, context_token: str = "",
                        client_id: str = "", message_state: int = 2) -> dict:
        content = filter_markdown_weixin(content)
        last: dict = {"ret": 0}
        chunks = split_chunks(content)
        for i, part in enumerate(chunks):
            body = {
                "msg": {
                    "from_user_id": "",
                    "to_user_id": to_user_id,
                    "client_id": client_id if (client_id and i == 0) else f"muse-{uuid.uuid4().hex}",
                    "message_type": 2,
                    "message_state": message_state,
                    "item_list": [{"type": 1, "text_item": {"text": part}}],
                },
                "base_info": base_info(),
            }
            if context_token:
                body["msg"]["context_token"] = context_token
            last = await self._post(
                client, creds["base_url"], "ilink/bot/sendmessage", body, creds["token"], 15.0
            )
            if last.get("ret") not in (None, 0):
                return last
            self.msgs_sent += 1
        return last

    # ---------- media & typing ----------

    async def _download_media(self, client: httpx.AsyncClient, media: list[dict], msgid: str) -> None:
        from urllib.parse import quote

        MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        for i, m in enumerate(media):
            if m.get("kind") == "voice":
                # Defense in depth: never download voice audio even if
                # a caller still passes it in (see _extract_text).
                log(f"media {msgid}/{i}: voice audio skipped (not received by policy)")
                continue
            item = m.get("item") or {}
            node = item.get("media") or {}
            url = node.get("full_url", "")
            param = node.get("encrypt_query_param", "")
            if not url and param:
                url = f"{CDN_BASE_URL}/download?encrypted_query_param={quote(param, safe='')}"
            key = None
            try:
                if item.get("aeskey"):
                    key = bytes.fromhex(item["aeskey"])
                elif node.get("aes_key"):
                    key = parse_aes_key(node["aes_key"])
            except Exception as e:
                log(f"media {msgid}/{i}: bad aes key: {e}")
            if not url or key is None:
                continue
            try:
                r = await client.get(url, timeout=httpx.Timeout(30.0, connect=10.0))
                r.raise_for_status()
                data = r.content
                if len(data) > 25 * 1024 * 1024:
                    log(f"media {msgid}/{i}: too large ({len(data)} bytes), skipped")
                    continue
                plain = aes_ecb_decrypt(data, key)
                path = MEDIA_DIR / f"{msgid}-{i}.{sniff_ext(plain)}"
                path.write_bytes(plain)
                m["local_path"] = str(path)
                if m.get("kind") == "voice":
                    m["format"] = path.suffix.lstrip(".")
                    for k in ("playtime", "sample_rate", "encode_type"):
                        if item.get(k) is not None:
                            m[k] = item[k]
                log(f"media {msgid}/{i}: saved {path} ({len(plain)} bytes)")
            except Exception as e:
                log(f"media {msgid}/{i}: download/decrypt failed: {e}")

    async def _get_typing_ticket(self, client: httpx.AsyncClient, creds: dict,
                                 user_id: str, context_token: str) -> str:
        cached = self.typing_tickets.get(user_id)
        if cached and time.time() - cached[1] < 1800:
            return cached[0]
        try:
            resp = await self._post(
                client, creds["base_url"], "ilink/bot/getconfig",
                {"ilink_user_id": user_id, "context_token": context_token,
                 "base_info": base_info()},
                creds["token"], 10.0,
            )
            ticket = resp.get("typing_ticket", "")
            if ticket:
                self.typing_tickets[user_id] = (ticket, time.time())
            return ticket
        except Exception as e:
            log(f"getconfig failed: {e}")
            return ""

    async def _send_typing(self, client: httpx.AsyncClient, creds: dict,
                           user_id: str, context_token: str, status: int) -> None:
        try:
            ticket = await self._get_typing_ticket(client, creds, user_id, context_token)
            if not ticket:
                return
            await self._post(
                client, creds["base_url"], "ilink/bot/sendtyping",
                {"ilink_user_id": user_id, "typing_ticket": ticket,
                 "status": status, "base_info": base_info()},
                creds["token"], 10.0,
            )
        except Exception as e:
            log(f"sendtyping failed (continuing): {e}")

    async def _upload_media(self, client: httpx.AsyncClient, creds: dict,
                            to_user_id: str, data: bytes, media_type: int) -> dict:
        import hashlib as _hl
        from urllib.parse import quote

        filekey = uuid.uuid4().hex
        aeskey = os.urandom(16)
        cipher = aes_ecb_encrypt(data, aeskey)
        resp = await self._post(
            client, creds["base_url"], "ilink/bot/getuploadurl",
            {"filekey": filekey, "media_type": media_type, "to_user_id": to_user_id,
             "rawsize": len(data), "rawfilemd5": _hl.md5(data).hexdigest(),
             "filesize": len(cipher), "no_need_thumb": True,
             "aeskey": aeskey.hex(), "base_info": base_info()},
            creds["token"], 15.0,
        )
        upload_url = (resp.get("upload_full_url") or "").strip()
        if not upload_url and resp.get("upload_param"):
            upload_url = (
                f"{CDN_BASE_URL}/upload?encrypted_query_param="
                f"{quote(resp['upload_param'], safe='')}&filekey={filekey}"
            )
        if not upload_url:
            raise RuntimeError(f"getuploadurl returned no URL: {str(resp)[:200]}")
        r = await client.post(
            upload_url, content=cipher,
            headers={"Content-Type": "application/octet-stream"},
            timeout=httpx.Timeout(60.0, connect=15.0),
        )
        r.raise_for_status()
        download_param = r.headers.get("x-encrypted-param", "")
        if not download_param:
            raise RuntimeError("CDN upload response missing x-encrypted-param")
        return {"download_param": download_param, "aeskey_hex": aeskey.hex(),
                "cipher_size": len(cipher)}

    async def _send_items(self, client: httpx.AsyncClient, creds: dict,
                          to_user_id: str, items: list[dict], context_token: str = "") -> dict:
        body = {
            "msg": {
                "from_user_id": "",
                "to_user_id": to_user_id,
                "client_id": f"muse-{uuid.uuid4().hex}",
                "message_type": 2,
                "message_state": 2,
                "item_list": items,
            },
            "base_info": base_info(),
        }
        if context_token:
            body["msg"]["context_token"] = context_token
        return await self._post(
            client, creds["base_url"], "ilink/bot/sendmessage", body, creds["token"], 15.0
        )

    @staticmethod
    def _media_item(mtype: int, up: dict, filename: str, plain_size: int) -> dict:
        import base64 as _b64

        media = {
            "encrypt_query_param": up["download_param"],
            "aes_key": _b64.b64encode(up["aeskey_hex"].encode()).decode(),
            "encrypt_type": 1,
        }
        if mtype == 1:
            return {"type": 2, "image_item": {"media": media, "mid_size": up["cipher_size"]}}
        if mtype == 2:
            return {"type": 5, "video_item": {"media": media, "video_size": up["cipher_size"]}}
        return {"type": 4, "file_item": {"media": media, "file_name": filename,
                                         "len": str(plain_size)}}

    async def _deliver_file(self, client: httpx.AsyncClient, creds: dict,
                            to_user_id: str, fpath: str, context_token: str = "") -> dict:
        data = Path(fpath).read_bytes()
        mtype = weixin_media_type(fpath)
        up = await self._upload_media(client, creds, to_user_id, data, mtype)
        item = self._media_item(mtype, up, Path(fpath).name, len(data))
        return await self._send_items(client, creds, to_user_id, [item], context_token)

    # ---------- outbox ----------

    def _outbox_offset(self) -> int:
        try:
            return int(OUTBOX_OFFSET.read_text().strip() or 0)
        except (OSError, ValueError):
            return 0

    @staticmethod
    def _cancelled_ids() -> set:
        try:
            rows = json.loads(CANCELLED_FILE.read_text(encoding="utf-8"))
            return {str(r.get("msgid")) for r in rows if isinstance(r, dict) and r.get("msgid")}
        except Exception:
            return set()

    @staticmethod
    def _delivered_reply_before(item: dict) -> bool:
        """Sending-layer late suppression (fix, 2026-10-04 evening):
        True when an outbox row queued BEFORE this item is a formal
        reply for the same msgid whose delivery result is ok. A late
        update/reply_file queued by a racing orphan-takeover worker
        can slip past the CLI gate in the window before the reply's
        result lands; this check runs at dispatch time, after the
        reply has actually been delivered, and drops it. Only rows
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

    async def _typing_best_effort(self, client, creds, to, token_ctx, status):
        """Typing is cosmetic: never let it block or lose a sent reply."""
        try:
            await asyncio.wait_for(
                self._send_typing(client, creds, to, token_ctx, status), timeout=3.0
            )
        except Exception as e:
            log(f"sendtyping best-effort failed (ignored): {e}")

    async def dispatch_outbox_item(self, client: httpx.AsyncClient, creds: dict, item: dict) -> bool:
        """Return True if the item is consumed (success or permanent drop),
        False if it failed transiently and must be retried (offset NOT advanced)."""
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
            return True  # consumed: do not send, do not retry
        if mode in ("update", "reply_file") and _mid and self._delivered_reply_before(item):
            result["errmsg"] = "suppressed: msgid already has a delivered formal reply"
            result["suppressed"] = True
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: skipped, already replied msgid={_mid}")
            return True  # consumed: late duplicate, never reaches the user
        resp = None
        typing_after = None  # (to, token_ctx, status)
        try:
            if mode == "reply":
                info = self.context.get(str(item.get("msgid", "")), {})
                to = info.get("from_user_id", "")
                token_ctx = info.get("context_token", "")
                if not to:
                    result["errmsg"] = f"no context stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply {item_id}: {result['errmsg']}")
                    return True  # permanent: do not retry forever
                resp = await self.send_text(
                    client, creds, to, content, token_ctx, message_state=2,
                )
                if resp.get("ret") in (None, 0):
                    typing_after = (to, token_ctx, 2)
            elif mode == "update":
                info = self.context.get(str(item.get("msgid", "")), {})
                to = info.get("from_user_id", "")
                if not to:
                    result["errmsg"] = f"no context stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox update {item_id}: {result['errmsg']}")
                    return True
                resp = await self.send_text(
                    client, creds, to, content, info.get("context_token", ""),
                    message_state=2,
                )
            elif mode == "reply_file":
                info = self.context.get(str(item.get("msgid", "")), {})
                to = info.get("from_user_id", "")
                token_ctx = info.get("context_token", "")
                if not to:
                    result["errmsg"] = f"no context stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply_file {item_id}: {result['errmsg']}")
                    return True
                fpath = item.get("file_path", "")
                if not Path(fpath).exists():
                    result["errmsg"] = f"file not found: {fpath}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply_file {item_id}: {result['errmsg']}")
                    return True
                caption = content or f"📎 {Path(fpath).name}"
                cap_resp = await self.send_text(
                    client, creds, to, caption, token_ctx, message_state=2,
                )
                if cap_resp.get("ret") not in (None, 0):
                    result["ret"] = cap_resp.get("ret")
                    result["errmsg"] = cap_resp.get("errmsg", "")
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox {mode} {item_id}: ok=False err={result.get('errmsg', '')}")
                    return False
                resp = await self._deliver_file(client, creds, to, fpath, token_ctx)
                if resp.get("ret") in (None, 0):
                    typing_after = (to, token_ctx, 2)
            elif mode == "send":
                resp = await self.send_text(client, creds, item.get("to_user_id", ""), content)
            elif mode == "send_file":
                to = item.get("to_user_id", "")
                fpath = item.get("file_path", "")
                if fpath and not Path(fpath).exists():
                    result["errmsg"] = f"file not found: {fpath}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    return True
                if content:
                    cap_resp = await self.send_text(client, creds, to, content)
                    if cap_resp.get("ret") not in (None, 0):
                        result["ret"] = cap_resp.get("ret")
                        result["errmsg"] = cap_resp.get("errmsg", "")
                        self.append_jsonl(OUTBOX_RESULTS, result)
                        return False
                resp = await self._deliver_file(
                    client, creds, to, item.get("file_path", "")
                )
            else:
                result["errmsg"] = f"unknown mode {mode}"
                self.append_jsonl(OUTBOX_RESULTS, result)
                return True
            result["ret"] = resp.get("ret") if isinstance(resp, dict) else None
            result["errmsg"] = resp.get("errmsg", "") if isinstance(resp, dict) else ""
            result["ok"] = (resp.get("ret") in (None, 0)) if isinstance(resp, dict) else False
        except FileNotFoundError as e:
            result["errmsg"] = f"exception: {e}"
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: ok=False err={result['errmsg']}")
            return True
        except Exception as e:
            result["errmsg"] = f"exception: {e}"
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: ok=False err={result['errmsg']}")
            self.write_status()
            return False  # transient: retry, do not advance offset
        # Persist the result BEFORE any cosmetic typing call, so a hang /
        # session drop during typing can never cause a resend on restart.
        self.append_jsonl(OUTBOX_RESULTS, result)
        log(f"outbox {mode} {item_id}: ok={result['ok']} err={result.get('errmsg', '')}")
        if result["ok"] and mode in ("reply", "reply_file"):
            # The round is closed: drop its arrival-feedback record so
            # the next message classifies as a fresh round.
            self._feedback_on_reply(item.get("msgid", ""))
        self.write_status()
        if typing_after is not None and result["ok"]:
            # Fire-and-forget: offset advancement in outbox_loop must not wait
            # on typing. A hang/cancel here previously lost the offset and
            # caused a resend on restart (issue 1).
            asyncio.create_task(
                self._typing_best_effort(client, creds, typing_after[0], typing_after[1], typing_after[2])
            )
        return bool(result["ok"])

    async def outbox_loop(self, client: httpx.AsyncClient, creds: dict) -> None:
        while True:
            await asyncio.sleep(1.0)
            if not OUTBOX.exists():
                continue
            offset = self._outbox_offset()
            size = OUTBOX.stat().st_size
            if offset > size:
                offset = 0
            if offset >= size:
                continue
            with OUTBOX.open("rb") as f:
                f.seek(offset)
                data = f.read()
            consumed = 0
            for raw_line in data.split(b"\n"):
                if not raw_line.strip():
                    consumed += len(raw_line) + 1
                    continue
                try:
                    item = json.loads(raw_line.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    consumed += len(raw_line) + 1
                    continue
                consumed_ok = await self.dispatch_outbox_item(client, creds, item)
                if not consumed_ok:
                    # Transient failure (incl. chunk ret!=0): do NOT advance
                    # offset past this item; retry it on the next cycle /
                    # after restart instead of silently losing it.
                    try:
                        OUTBOX_OFFSET.write_text(str(offset + consumed))
                    except OSError:
                        pass
                    await asyncio.sleep(2.0)
                    break
                consumed += len(raw_line) + 1
                try:
                    OUTBOX_OFFSET.write_text(str(offset + consumed))
                except OSError:
                    pass

    # ---------- session ----------

    async def run_session(self, client: httpx.AsyncClient, creds: dict) -> str:
        # best-effort start notification
        try:
            await self._post(client, creds["base_url"], "ilink/bot/msg/notifystart",
                             {"base_info": base_info()}, creds["token"], 10.0)
        except Exception as e:
            log(f"notifystart failed (continuing): {e}")
        self.connected = True
        self.state = "connected"
        self.last_error = ""
        log("session started; long-polling getupdates")
        self.write_status()
        outbox_task = asyncio.create_task(self.outbox_loop(client, creds))
        watch_task = asyncio.create_task(self.feedback_watch_loop())
        failures = 0
        try:
            while True:
                try:
                    resp = await self._post(
                        client, creds["base_url"], "ilink/bot/getupdates",
                        {"get_updates_buf": self.sync_buf, "base_info": base_info()},
                        creds["token"], 45.0,
                    )
                except httpx.TimeoutException:
                    continue  # normal long-poll timeout; poll again
                except Exception as e:
                    failures += 1
                    log(f"getupdates error ({failures}): {e}")
                    if failures >= 5:
                        return f"getupdates failed repeatedly: {e}"
                    await asyncio.sleep(min(2 * failures, 10))
                    continue
                failures = 0
                errcode = resp.get("errcode")
                ret = resp.get("ret")
                if (errcode not in (None, 0)) or (ret not in (None, 0)):
                    code = errcode if errcode not in (None, 0) else ret
                    if code == STALE_TOKEN_ERRCODE:
                        # -14 is usually a *soft* expiry (idle session went
                        # stale); the official plugin pauses and retries the
                        # same token, and fresh user activity revives it.
                        # run() probes before escalating to a re-login.
                        self.state = "session_stale"
                        self.last_error = "session stale (errcode -14); probing with same token"
                        self.write_status()
                        log("session stale (-14); handing back for probe-retry")
                        return "stale_token"
                    log(f"getupdates ret={ret} errcode={errcode} errmsg={resp.get('errmsg')}")
                    await asyncio.sleep(3)
                    continue
                new_buf = resp.get("get_updates_buf")
                msgs = resp.get("msgs") or []
                try:
                    for msg in msgs:
                        await self.handle_inbound(client, creds, msg)
                except Exception as e:
                    # Do NOT advance the sync cursor: this batch will be
                    # redelivered on the next poll / after restart. Messages
                    # already written to inbox are deduped via seen_msgids
                    # (reloaded from inbox at startup), so no duplicates.
                    log(f"handle_inbound failed, sync cursor NOT advanced: {e}")
                    await asyncio.sleep(3)
                    continue
                if new_buf is not None:
                    self.sync_buf = new_buf
                    self._save_sync()
        finally:
            outbox_task.cancel()
            watch_task.cancel()
            self.connected = False

    async def run(self) -> None:
        backoff = 5
        stale_hits = 0
        # Explicit proxy from env: httpx trust_env parsing chokes on the
        # bracketed IPv6 entries in NO_PROXY here ("Invalid port ':1]'").
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None
        async with httpx.AsyncClient(proxy=proxy, trust_env=False) as client:
            while True:
                creds = load_credentials()
                if not creds["token"]:
                    if self.state != "awaiting_login":
                        self.state = "awaiting_login"
                        log("no bot token yet; waiting for QR login (re-checking every 15s)")
                        self.write_status()
                    await asyncio.sleep(15)
                    continue
                try:
                    reason = await self.run_session(client, creds)
                except Exception as e:
                    reason = f"exception: {e}"
                    self.last_error = reason
                self.connected = False
                if reason == "stale_token":
                    # Probe with the same token every 10 min (mirrors the
                    # official plugin's pause-and-retry). A message from the
                    # user to the bot usually revives the session server-side,
                    # and queued messages arrive on the next successful poll.
                    stale_hits += 1
                    if stale_hits < 6:
                        self.state = "session_stale"
                        self.write_status()
                        log(f"stale session; probe {stale_hits}/6 in 600s")
                        await asyncio.sleep(600)
                        continue
                    reason = "needs_relogin"
                stale_hits = 0
                if reason == "needs_relogin":
                    self.state = "needs_relogin"
                    self.write_status()
                    # wait until the credentials file changes (fresh login)
                    try:
                        mtime = CRED_FILE.stat().st_mtime
                    except OSError:
                        mtime = 0
                    while True:
                        await asyncio.sleep(10)
                        try:
                            if CRED_FILE.stat().st_mtime != mtime:
                                break
                        except OSError:
                            pass
                    self.state = "reconnecting"
                    backoff = 5
                    continue
                self.state = "reconnecting"
                self.write_status()
                log(f"session ended ({reason}); reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


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
