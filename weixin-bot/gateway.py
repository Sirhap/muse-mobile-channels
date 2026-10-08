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
import hashlib
import json
import os
import random
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import httpx

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from channel_common import (  # noqa: E402
    allocate_subagent_id,
    append_jsonl as append_jsonl_line,
    atomic_write_text,
    complete_jsonl_lines,
    load_json_dict,
    media_filename,
    media_url_allowed,
    merge_queue_admin,
    muse_home,
    outbound_file_allowed,
    parse_feedback_clear_line,
    parse_jsonl_line,
    retry_backoff_secs as _retry_backoff_secs,
    subagent_outcome_default,
    trim_mapping,
    waiting_ahead,
    write_offset,
    read_offset,
)

BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
OUTBOX_RESULTS = STATE / "outbox_results.jsonl"
OUTBOX_OFFSET = STATE / "outbox.offset"
OUTBOX_RETRY = STATE / "outbox_retry.json"
OUTBOX_PARTIAL = STATE / "outbox_partial.json"
OUTBOX_PARKED = STATE / "outbox_parked.json"
# Per-item send retry policy. PARK-AND-CONTINUE since 2026-10-06
# (deep-dive: the strict serial FIFO outbox wedged the whole channel
# behind ONE failing head row four different ways — a formal reply
# in a "prepare failed" episode froze every later message for
# 30+ min; evidence ~/workspace/outbox-wedge-deepdive-2026-10-06):
# any row that fails a dispatch is PARKED — the offset advances
# past it, the rest of the queue keeps flowing in the same cycle,
# and a separate retry lane retries the parked row on its own
# backoff (retry_backoff_secs). Parked state (item payload, attempt
# count, next time, guard streaks) is PERSISTED in OUTBOX_PARKED so
# a gateway restart can no longer reset any counter. Non-formal
# rows are still dead-lettered after SEND_MAX_ATTEMPTS failed
# attempts — but now inside the lane, off the main queue. Formal
# reply/reply_file rows are EXEMPT from the dead-letter (see
# FORMAL_MODES below): parked, they retry forever.
SEND_MAX_ATTEMPTS = 10

# Wedge guard for unbound mode="send" notice rows (2026-10-06):
# decorative notices (softack / thinking / started / waitremind /
# merged / media) carry no information worth 10 attempts, so a
# parked notice row is dropped after 5 consecutive failures. The
# streak lives in the persisted parked record (was process-local
# before park-and-continue; a restart used to reset it). Formal
# rows (reply / reply_file / update / send_file) are never dropped
# by this guard.
SEND_NOTICE_DROP_AFTER_FAILURES = 5

# Wedge guard for mode="send_file" rows whose delivery dies at the
# WeChat CDN upload step (2026-10-06; reworked 2026-10-08): on
# 2026-10-05/06 the CDN upload endpoint (novac2c.cdn.weixin.qq.com)
# returned HTTP 500 for every file size — a provider-side outage —
# and one queued video send_file row sat at the head of the outbox
# in backoff while text notices and a formal reply queued behind
# it. A file that cannot even be uploaded gains nothing from the
# full 10-attempt dead-letter path, so after 3 consecutive
# upload-stage failures the row is consumed — but NO LONGER
# silently (2026-10-08): the user gets a WeChat text notice and the
# file itself is rerouted via the WeCom outbox (the file branch of
# _notify_stuck_formal). Counting was also widened: ANY upload-
# stage failure counts — _deliver_file wraps upload-leg exceptions
# in UploadStageError (marker "upload-stage" in the errmsg),
# including empty transport errors and timeouts that carry no
# message at all; the legacy errmsg pattern (CDN host + 500) still
# counts too. Non-upload send_file failures keep the normal
# backoff / dead-letter policy.
SEND_FILE_UPLOAD_REROUTE_AFTER_FAILURES = 3
UPLOAD_STAGE_MARKER = "upload-stage"


class UploadStageError(RuntimeError):
    """A file delivery failed during the CDN upload leg (before
    sendmessage). Raised by _deliver_file so _note_failure can
    count upload-stage failures regardless of the underlying
    exception's message — some transport errors stringify to ""."""

# Formal replies are NEVER dead-lettered (user decision 2026-10-06,
# superseding the any-row policy above for these modes): a reply or
# reply_file is the answer to something the user asked; silently
# discarding it after 10 transient failures loses the answer without
# anyone knowing. Parked formal rows keep the normal backoff and
# retry indefinitely in the retry lane. If one is still failing
# after FORMAL_STUCK_NOTIFY_ATTEMPTS parked attempts the user is
# told ONCE (see _notify_stuck_formal): in-channel by direct send
# (bypassing the outbox) and cross-channel via the WeCom outbox.
# (Threshold lowered 25 -> 3 with park-and-continue 2026-10-06: in
# the lane a stuck formal row no longer blocks anyone, but the user
# should still hear about it within ~1-2 minutes, not ~106.)
FORMAL_MODES = ("reply", "reply_file")
FORMAL_STUCK_NOTIFY_ATTEMPTS = 3
STUCK_NOTICE_WECOM_OUTBOX = (
    Path(os.environ.get("HOME") or "/home/hatch")
    / "workspace" / "wecom-bot" / "state" / "outbox.jsonl")
STUCK_NOTICE_WECOM_CHATID = os.environ.get(
    "WEIXIN_STUCK_NOTICE_CHATID", "sirhao")

# Large-file compression (user decision 2026-10-06: "太大就压缩发"):
# WeChat CDN uploads of multi-MB files fail intermittently (HTTP 500
# or empty transport errors), so a file larger than
# FILE_COMPRESS_THRESHOLD is compressed BEFORE the first upload —
# video via ffmpeg (<=854px wide, CRF 30), image via PIL (<=1600px,
# JPEG q80) — and the compressed copy is sent instead, with a note
# appended to the caption. The original file is never modified;
# compressed copies are cached under STATE/compressed/ keyed by
# source path+size+mtime. Compression failure or a non-smaller
# result falls back to sending the original.
FILE_COMPRESS_THRESHOLD = 2_000_000
COMPRESSED_DIR = STATE / "compressed"
VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}


def retry_backoff_secs(attempt: int) -> float:
    """Seconds before the next send attempt. See channel_common."""
    return _retry_backoff_secs(attempt)
STATUS = STATE / "status.json"
SYNC_FILE = STATE / "sync.json"
CONTEXT_FILE = STATE / "context.json"
LOCK_FILE = STATE / "gateway.lock"
CANCELLED_FILE = STATE / "cancelled.json"
FEEDBACK_CLEAR_FILE = STATE / "feedback_clear.jsonl"
SEEN_FILE = STATE / "seen_ids.jsonl"
CRED_FILE = Path(
    os.environ.get("ILINK_CRED_FILE")
    or str(muse_home() / ".config" / "weixin-bot" / "credentials.env")
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
# The user can send /ping, /status, /stop, /new, /check,
# /queue, /help and /subagent in chat. Commands are intercepted
# after the allowlist +
# dedupe checks and BEFORE the inbox write: they never wake a worker;
# the gateway answers itself through the normal proactive send path.
# Unknown slash text (e.g. /foo) is NOT a command and flows through as
# an ordinary message.
HOOK_STATE_DIR = muse_home() / "hooks" / "state" / "weixin-bot"
CLI_WRAPPER = BASE / "weixin"
CHAN_LABEL = "个人微信"

SLASH_COMMANDS = {"ping", "status", "stop", "new", "check", "queue", "help", "subagent"}
SLASH_ALIASES = {"自检": "check", "命令": "help", "副助手": "subagent", "新会话": "new"}

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
    (used by /subagent, /queue drop and /new-adjacent flows)."""
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
    """Merge this clear/drop into an unconsumed queue_admin.json.

    Two commands inside the hook's poll window used to overwrite each
    other. The hook still reads one object with action and msgids.
    """
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


def _bridge_queue_lines(channel):
    """Native-bridge queue snapshot: (display_text, active_msgids).

    Reads ~/workspace/native-bridge/state.json; fail-silent so /queue and
    /stop keep working when the bridge is down or has nothing."""
    try:
        st = json.loads(Path(
            "/home/hatch/workspace/native-bridge/state.json"
        ).read_text(encoding="utf-8"))
        snap = (st.get("channels", {}).get(channel, {})
                or {}).get("queue_snapshot", {}) or {}
        act = snap.get("active", [])
        q = snap.get("queued", [])
        if not act and not q:
            return "", []
        text = f"\n原生桥：进行中 {len(act)} 条"
        if act:
            text += "（" + "、".join(
                f"{a.get('lane', 'main')} {a.get('secs', 0)}s"
                for a in act) + "）"
        text += f"，排队 {len(q)} 条"
        return text, [a.get("msgid") for a in act if a.get("msgid")]
    except Exception:
        return "", []


def _fmt_dur(secs):
    secs = int(secs or 0)
    if secs < 60:
        return f"{secs}秒"
    if secs < 3600:
        return f"{secs // 60}分钟"
    return f"{secs // 3600}小时{(secs % 3600) // 60}分"


def _spool_texts(channel):
    """{msgid: text} from the bridge spool (tail-read, fail-silent)."""
    try:
        p = Path(f"/home/hatch/workspace/native-bridge/"
                 f"spool/{channel}.jsonl")
        with open(p, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 524288))
            data = f.read().decode("utf-8", "replace")
        out = {}
        for line in data.splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("msgid"):
                out[r["msgid"]] = r.get("text", "")
        return out
    except Exception:
        return {}


def _task_overview_text(channel, state_dir, hook_state_dir):
    """Unified task board appended to /status: every in-flight or queued
    task across the native bridge, the cold hook path and subagent jobs,
    plus bridge lifetime counters. Fail-silent per section."""
    try:
        lines = []
        st = json.loads(Path(
            "/home/hatch/workspace/native-bridge/state.json"
        ).read_text(encoding="utf-8"))
        ch = st.get("channels", {}).get(channel, {}) or {}
        snap = ch.get("queue_snapshot", {}) or {}
        texts = _spool_texts(channel)
        inbox_texts = _inbox_texts(state_dir)
        for a in snap.get("active", []):
            mid = a.get("msgid", "")
            txt = texts.get(mid) or inbox_texts.get(mid, "")
            lines.append(f"▶ 桥 {_fmt_dur(a.get('secs', 0))} "
                         f"「{_excerpt(txt)}」")
        batch, _ids, pcount, dcount = _queue_summary(hook_state_dir)
        mids = batch.get("msgids") or []
        if mids:
            since = batch.get("since")
            age = (time.time() - float(since)) \
                if isinstance(since, (int, float)) else 0
            lines.append(f"▶ 冷通道 {_fmt_dur(age)} "
                         f"「{_excerpt(inbox_texts.get(mids[0], ''))}」"
                         f"（本批 {len(mids)} 条）")
        qparts = []
        bq = snap.get("queued", [])
        if bq:
            qparts.append(f"桥 {len(bq)} 条")
        if pcount:
            qparts.append(f"冷通道 {pcount} 条")
        if dcount:
            qparts.append(f"冷通道并行 {dcount} 条")
        if qparts:
            lines.append("⏳ 排队：" + "、".join(qparts))
        jobs = _read_json_file(hook_state_dir / "subagent_jobs.json", {}) or {}
        jmap = jobs.get("jobs", {}) if isinstance(jobs, dict) else {}
        running = sum(1 for j in jmap.values()
                      if isinstance(j, dict) and j.get("status") == "running")
        queued = sum(1 for j in jmap.values()
                     if isinstance(j, dict) and j.get("status") == "queued")
        if running or queued:
            lines.append(f"🤖 副助手：运行 {running} · 排队 {queued}")
        proc = ch.get("processed")
        if proc is not None:
            lines.append(f"📈 桥累计：已答 {proc} 条 · "
                         f"回落 {ch.get('fallbacks', 0)} 条 · "
                         f"取消 {ch.get('cancels', 0)} 条")
        if not lines:
            return "\n🧭 任务总览：当前没有进行中或排队的任务"
        return "\n🧭 任务总览\n" + "\n".join(lines)
    except Exception:
        return ""


def _bridge_snapshot(channel):
    """(active_list, queued_list) from the bridge queue_snapshot.
    Fail-silent ([], [])."""
    try:
        st = json.loads(Path(
            "/home/hatch/workspace/native-bridge/state.json"
        ).read_text(encoding="utf-8"))
        snap = (st.get("channels", {}).get(channel, {})
                or {}).get("queue_snapshot", {}) or {}
        return snap.get("active", []) or [], snap.get("queued", []) or []
    except Exception:
        return [], []


def _bridge_queued_rows(channel):
    """[(msgid, text)] for rows currently queued in the native bridge,
    in queue order, from the bridge snapshot + spool texts. Fail-silent."""
    try:
        st = json.loads(Path(
            "/home/hatch/workspace/native-bridge/state.json"
        ).read_text(encoding="utf-8"))
        snap = (st.get("channels", {}).get(channel, {})
                or {}).get("queue_snapshot", {}) or {}
        qids = snap.get("queued", [])
        if not qids:
            return []
        texts = {}
        spool = Path(f"/home/hatch/workspace/native-bridge/"
                     f"spool/{channel}.jsonl")
        for line in spool.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("msgid"):
                texts[r["msgid"]] = r.get("text", "")
        return [(m, texts.get(m, "")) for m in qids]
    except Exception:
        return []


def _bridge_admin(channel, action, msgids):
    """Append a queue-admin instruction for the native bridge."""
    return _append_jsonl_file(
        Path(f"/home/hatch/workspace/native-bridge/"
             f"queue_admin-{channel}.jsonl"),
        {"action": action, "msgids": list(msgids), "ts": time.time()})


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


SOFT_ACK_TEMPLATE = "已收到，排队第 {n} 位，当前任务进行中；/stop 取消"

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

BRIDGE_WAIT_TEMPLATE = "⏳ 还在排队（原生通道第 {n} 位）：前面任务还没结束，已等{dur}；/stop 取消"
MEDIA_ACK_TEMPLATE = "📎 已收到{what}，正在处理…"
STARTED_NOTICE_TEMPLATE = "▶️ 排到你了，开始处理：「{excerpt}」"
WAIT_REMIND_TEMPLATE = "⏳ 还在排队（第 {n} 位）：前面任务还没结束，已等{dur}；/stop 取消"
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
                    if row.get("mode") not in ("reply", "reply_file"):
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

    Position N = the hook's genuinely-waiting pending count + 1
    (this message; in-service messages never count), unless the
    caller passes a locally computed position. The gateway reads
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
            _batch, _ids, _pcount, _dcount = _queue_summary(hook_state_dir)
            pending = _read_json_file(hook_state_dir / "pending.json", {}) or {}
            keys = pending.keys() if isinstance(pending, dict) else []
            position = waiting_ahead(keys, _ids) + 1
        return SOFT_ACK_TEMPLATE.format(n=position)
    except Exception:
        return None


SLASH_HELP_TEXT = """【命令表】在聊天里直接发，网关秒回、不排队
普通消息走原生通道秒回；长任务在原生通道排队，冷通道保底
/ping 连通自检
/status 渠道状态 + 任务总览（谁在跑、谁在排队、副助手、累计）
/queue 队列明细（冷通道 + 原生桥都列）
/queue clear 清掉排队中未开工的消息（桥和冷通道一起清）
/queue drop N 只删冷通道排队第 N 条；/queue drop 桥N 删原生桥排队第 N 条
/stop 取消正在跑的任务
/subagent <任务> 派副助手并行执行（别名 /副助手）；/subagent list 查、/subagent stop <编号> 停
/new 开新会话：换一个全新的原生会话，之前的对话不再带入（别名 /新会话）
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

    try:
        bkey = "weixin" if "weixin" in hook_state_dir.name else "wecom"
        bmode = _read_json_file(
            Path("/home/hatch/workspace/native-bridge/status.json"), {}) or {}
        bts = bmode.get("ts")
        balive = isinstance(bts, (int, float)) and (now - float(bts) <= 30)
        blive = (bmode.get("mode", {}) or {}).get(bkey) == "live"
        items.append(("原生桥", bool(balive),
                      "运行中（live）" if balive and blive else
                      ("运行中（shadow，未接管）" if balive
                       else "无心跳，桥可能未运行")))
    except Exception:
        items.append(("原生桥", False, "状态不可读"))

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
    lines.append("（停正在跑的用 /stop）")
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
    """Allocate the next job id (S<n>), or None if the sequence file
    cannot be replaced. A failed replace must not reuse the same id."""
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
    label 【副助手 #<jid> <outcome>】. An existing label keeps its
    outcome word (完成/失败/已停止) and any text after it, with the job
    id corrected to jid. A missing label uses the job status and the
    reply text so a failed job is not relabeled 完成."""
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
        # Parked-row failure state (wedge guards, retry lane) is NOT
        # kept in process memory: it is persisted per row in
        # OUTBOX_PARKED (see _note_failure), so a gateway restart can
        # no longer reset any failure counter.
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

    def _mark_seen(self, msgid: str) -> None:
        """Remember msgid in memory and on disk.

        Disk is what survives a restart. Slash commands never enter the
        inbox, so this file is the only record that they already ran.
        """
        if not msgid or str(msgid) in self.seen_msgids:
            return
        self.seen_msgids.add(str(msgid))
        try:
            append_jsonl_line(SEEN_FILE, {"msgid": str(msgid)})
        except OSError:
            self.seen_msgids.discard(str(msgid))

    def _route_for(self, msgid: str) -> dict:
        """Reply route from memory, or rebuilt from the inbox tail."""
        info = self.context.get(str(msgid)) or {}
        if info.get("from_user_id"):
            return info
        if not INBOX.exists():
            return info
        try:
            lines = INBOX.read_text(encoding="utf-8").splitlines()[-500:]
        except OSError:
            return info
        for line in reversed(lines):
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(entry.get("msgid") or "") != str(msgid):
                continue
            recovered = {
                "from_user_id": entry.get("from_user_id", ""),
                "group_id": entry.get("group_id", ""),
                "context_token": entry.get("context_token", ""),
                "client_id": "",
            }
            if recovered["from_user_id"]:
                self.context[str(msgid)] = recovered
                return recovered
        return info

    def _save_context(self) -> None:
        pinned = set(self.feedback_track)
        self.context = trim_mapping(list(self.context.items()), 2000, pinned)
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
        append_jsonl_line(path, obj)

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
                ack += _task_overview_text("weixin", STATE, HOOK_STATE_DIR)
            elif name == "check":
                ack = slash_run_check(CHAN_LABEL, STATE, HOOK_STATE_DIR)
            elif name == "stop":
                _b, ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
                _btxt, _bids = _bridge_queue_lines("weixin")
                ids = list(ids) + [i for i in _bids if i not in ids]
                if not ids:
                    ack = "当前没有正在跑的任务。"
                else:
                    ok, fail = await asyncio.to_thread(slash_register_cancels, ids)
                    ack = f"已登记取消共 {ok} 条，正在跑的任务会在下一个进度检查点停下并回你一句确认。"
                    if fail:
                        ack += f"（另有 {fail} 条登记失败）"
            elif name == "new":
                _write_hook_json(HOOK_STATE_DIR, "topic_boundary.json", {"ts": time.time()})
                ack = "已开新会话：之前的对话不会再带入上下文。"
            elif name == "queue":
                qtokens = arg.split()
                if not qtokens:
                    ack = slash_queue_text(CHAN_LABEL, STATE, HOOK_STATE_DIR)
                    ack += _bridge_queue_lines("weixin")[0]
                    for _i, (_m, _t) in enumerate(
                            _bridge_queued_rows("weixin"), 1):
                        ack += f"\n  桥{_i}. 「{_excerpt(_t)}」"
                elif qtokens[0] == "clear":
                    pend = _pending_sorted(HOOK_STATE_DIR)
                    brows = _bridge_queued_rows("weixin")
                    if not pend and not brows:
                        ack = "排队中没有消息可清。"
                    else:
                        parts = []
                        ok_all = True
                        if pend:
                            texts = _inbox_texts(STATE)
                            wrote = enqueue_queue_admin(
                                HOOK_STATE_DIR, "clear", [m for m, _t in pend])
                            if wrote:
                                self._feedback_forget([m for m, _t in pend],
                                                      source="queue-clear")
                                excerpts = "、".join(
                                    f"「{_excerpt(texts.get(m, ''))}」"
                                    for m, _t in pend)
                                parts.append(f"冷通道 {len(pend)} 条：{excerpts}")
                            else:
                                ok_all = False
                        if brows:
                            if _bridge_admin("weixin", "clear",
                                             [m for m, _t in brows]):
                                self._feedback_forget(
                                    [m for m, _t in brows],
                                    source="queue-clear")
                                bexcerpts = "、".join(
                                    f"「{_excerpt(t)}」" for _m, t in brows)
                                parts.append(f"原生桥 {len(brows)} 条：{bexcerpts}")
                            else:
                                ok_all = False
                        if parts:
                            ack = ("已提交清除排队消息（" + "；".join(parts) +
                                   "）。约 5 秒内生效。正在跑的任务不受影响。")
                            if not ok_all:
                                ack += "（部分清除失败，请稍后再试）"
                        else:
                            ack = "清除排队失败：暂时写不了队列指令，请稍后再试。"
                elif qtokens[0] == "drop" and len(qtokens) == 2 and (
                        qtokens[1].startswith("桥")
                        or qtokens[1].lower().startswith("b")):
                    num = qtokens[1][1:]
                    brows = _bridge_queued_rows("weixin")
                    if not num.isdigit() or int(num) < 1 or int(num) > len(brows):
                        ack = (f"原生桥排队里没有这一条（当前共 {len(brows)} 条）。"
                               f"用法：/queue drop 桥1")
                    else:
                        mid, mtext = brows[int(num) - 1]
                        if _bridge_admin("weixin", "drop", [mid]):
                            self._feedback_forget([mid], source="queue-drop")
                            ack = (f"已提交删除原生桥排队第 {int(num)} 条："
                                   f"「{_excerpt(mtext)}」，约 5 秒内生效。")
                        else:
                            ack = "删除排队失败：暂时写不了队列指令，请稍后再试。"
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
                            self._feedback_forget([mid], source="queue-drop")
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
                    mid = str(msg.get("message_id", "") or "")
                    if not jid:
                        ack = "派发失败：暂时无法登记副助手任务，请稍后再试。"
                    elif _append_jsonl_file(HOOK_STATE_DIR / "subagent_requests.jsonl",
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
            "notice": True,
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
                                is_group=False, diverted=False):
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
                           if m != mid and r.get("user") == from_user
                           and not r.get("via_bridge")]
            others = [r for _m, r in other_items]
            rec = {"user": from_user, "ts": now,
                   "excerpt": _excerpt(text or "", 20),
                   "queued": False, "queued_at": 0.0,
                   "started_notice": False, "wait_reminded": False,
                   "kind": None, "via_bridge": bool(diverted)}
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
            if diverted:
                # Bridge lane: the cold classifier must not judge this
                # message (the hook never sees it). No arrival ack is
                # sent on this lane at all (2026-10-08, user order:
                # the 「排队第 N 位（原生通道）」 placeholder receipt
                # goes away) — a message arriving before the running
                # turn starts replying is merged into that turn by the
                # bridge, and one that truly has to wait gets the
                # started notice from the feedback scan when its turn
                # begins. The record is still kept so the scan's
                # started / long-wait notices work. Idle bridge ->
                # fall through to the normal idle tail (thinking
                # notice); the cold busy/merged branches below are
                # guarded by diverted.
                active, queued = _bridge_snapshot("weixin")
                if active or queued:
                    rec["kind"] = "queued"
                    rec["queued"] = True
                    rec["queued_at"] = now
                    return "queued"
                rec["kind"] = "bridge"
            busy = (not diverted) and batch_in_flight(STATE, HOOK_STATE_DIR)
            if busy:
                # Queue position: only messages that are themselves
                # still waiting count as ahead; the in-flight batch is
                # being served, not queued (see _queue_position).
                _b, running_ids, _pending_count, _d = _queue_summary(HOOK_STATE_DIR)
                position = _queue_position(other_items, running_ids, now)
                pending = _read_json_file(HOOK_STATE_DIR / "pending.json", {}) or {}
                if isinstance(pending, dict):
                    position = max(position, waiting_ahead(pending.keys(), running_ids) + 1)
                if self._maybe_soft_ack(from_user, text, position=position):
                    rec["kind"] = "queued"
                    rec["queued"] = True
                    rec["queued_at"] = now
                    return "queued"
                return None
            if others and not is_group and not diverted:
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
                _b, running_ids, _pending_count, _d = _queue_summary(HOOK_STATE_DIR)
                position = _queue_position(other_items, running_ids, now)
                pending = _read_json_file(HOOK_STATE_DIR / "pending.json", {}) or {}
                if isinstance(pending, dict):
                    position = max(position, waiting_ahead(pending.keys(), running_ids) + 1)
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

    def _feedback_forget(self, msgids, source=""):
        """Drop feedback_track records WITHOUT a delivered reply:
        the message was disposed of another way (dropped via /queue
        admin, or skipped by the hook after an outage), so it must
        stop counting as unanswered — otherwise its record lingers
        up to the 7200s prune and inflates later queue positions
        (residue incident 2026-10-05). Returns the number popped.
        Fail-silent."""
        popped = []
        try:
            for m in (msgids or []):
                mid = str(m or "")
                if mid and mid in self.feedback_track:
                    self.feedback_track.pop(mid, None)
                    popped.append(mid)
            if popped:
                log(f"feedback_track forget ({source or 'manual'}): "
                    f"popped {len(popped)} record(s): {','.join(popped)}")
        except Exception as e:
            log(f"feedback forget failed (ignored): {e!r}")
        return len(popped)

    def _consume_feedback_clear(self):
        """Consume STATE/feedback_clear.jsonl (one msgid per line,
        appended by the inbox hook when it drops pending messages
        via /queue admin, and by the main session for hook-side
        skips). The file is renamed aside before reading so a
        concurrent append can never lose ids: an append landing
        after the rename creates a fresh file consumed next scan.
        Any problem -> fail-silent (the records still age out via
        the 7200s prune). Returns the number of records popped."""
        try:
            if not FEEDBACK_CLEAR_FILE.exists():
                return 0
            tmp = FEEDBACK_CLEAR_FILE.with_name(
                f"feedback_clear.consume.{os.getpid()}.tmp")
            try:
                os.replace(FEEDBACK_CLEAR_FILE, tmp)
            except OSError:
                return 0  # vanished between exists() and replace()
            ids = []
            try:
                raw = tmp.read_bytes().decode("utf-8", "replace")
                for line in raw.splitlines():
                    msgid = parse_feedback_clear_line(line)
                    if msgid:
                        ids.append(msgid)
            finally:
                try:
                    tmp.unlink()
                except OSError:
                    pass
            if not ids:
                return 0
            return self._feedback_forget(ids, source="clear-file")
        except Exception as e:
            log(f"feedback clear consume failed (ignored): {e!r}")
            return 0

    def _bridge_fallback_ids(self):
        """Msgids the native bridge fell back to the cold inbox
        (bot-state bridge_fallback.jsonl), mtime-cached. Fail-silent."""
        try:
            p = STATE / "bridge_fallback.jsonl"
            mt = p.stat().st_mtime
            if getattr(self, "_fb_mtime", None) == mt:
                return self._fb_ids
            ids = set()
            for line in p.read_text(encoding="utf-8").splitlines():
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("msgid"):
                    ids.add(str(r["msgid"]))
            self._fb_mtime = mt
            self._fb_ids = ids
            return ids
        except Exception:
            return getattr(self, "_fb_ids", set())

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
            self._consume_feedback_clear()
            batch, _ids, _p, _d = _queue_summary(HOOK_STATE_DIR)
            active = {str(m) for m in (batch.get("msgids") or [])}
            for d in (batch.get("detached") or []):
                if isinstance(d, dict):
                    active.update(str(m) for m in (d.get("msgids") or []))
            pend = _pending_sorted(HOOK_STATE_DIR)
            pos_map = {m: i + 1 for i, (m, _t) in enumerate(pend)}
            b_active, b_queued = _bridge_snapshot("weixin")
            b_active_ids = {str(a.get("msgid", "")) for a in b_active
                            if isinstance(a, dict)}
            b_merged_ids = {str(a.get("msgid", "")) for a in b_active
                            if isinstance(a, dict) and a.get("merged")}
            b_pos = {m: i + 1 for i, m in enumerate(b_queued)}
            sent = 0
            for mid, rec in list(self.feedback_track.items()):
                if not rec.get("queued"):
                    continue
                user = rec.get("user", "")
                if not user:
                    continue
                if rec.get("via_bridge") and \
                        mid in self._bridge_fallback_ids():
                    # The bridge fell back: this message is cold now —
                    # hand its transition notices to the cold machinery.
                    rec["via_bridge"] = False
                    rec["queued"] = True
                    if not rec.get("queued_at"):
                        rec["queued_at"] = now
                if rec.get("via_bridge"):
                    # Bridge-lane transitions come from the bridge
                    # snapshot, not the hook state.
                    if mid in b_merged_ids and not rec.get("started_notice"):
                        # Merged into the running turn: it never
                        # "starts" on its own, so the STARTED notice
                        # would be a false claim — suppress it for
                        # good (2026-10-08, user order).
                        rec["started_notice"] = True
                    elif mid in b_active_ids and not rec.get("started_notice"):
                        rec["started_notice"] = True
                        if self._queue_notice(
                                user,
                                STARTED_NOTICE_TEMPLATE.format(
                                    excerpt=rec.get("excerpt", "")),
                                "started"):
                            sent += 1
                    elif mid in b_pos and not rec.get("wait_reminded"):
                        waited = now - float(rec.get("queued_at") or now)
                        if waited >= WAIT_REMIND_SECS:
                            rec["wait_reminded"] = True
                            if self._queue_notice(
                                    user,
                                    BRIDGE_WAIT_TEMPLATE.format(
                                        n=b_pos[mid],
                                        dur=_dur_str(waited)),
                                    "waitremind"):
                                sent += 1
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
        if not owner_id:
            log(f"ILINK_USER_ID is unset; ignoring message {msgid}")
            self._mark_seen(msgid)
            return
        if from_user and from_user != owner_id:
            log(f"ignored message from non-owner user {from_user}")
            self._mark_seen(msgid)
            return
        text, media = self._extract_text(msg)
        if not media:
            # Slash commands: intercepted after allowlist + dedupe,
            # before the inbox write — they never wake a worker.
            slash = await self._slash_dispatch(client, creds, msg, text, from_user)
            if slash == "handled":
                self._mark_seen(msgid)
                self.msgs_received += 1
                self.write_status()
                return
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
        client_id = f"muse-{uuid.uuid4().hex}" if msgid else ""
        if msgid:
            self.context[msgid] = {
                "from_user_id": from_user,
                "group_id": msg.get("group_id", ""),
                "context_token": msg.get("context_token", ""),
                "client_id": client_id,
            }
            self._save_context()
        # Native bridge divert (2026-10-07): plain owner text messages go
        # to the bridge spool when the divert flag exists; the bridge's
        # formal reply lands in this gateway's outbox as usual, and any
        # bridge failure falls back to the inbox below (cold-start path).
        # Media and group messages keep the inbox path.
        diverted = False
        if (msgid and not media and not msg.get("group_id")
                and os.path.exists(
                    "/home/hatch/workspace/native-bridge/divert-weixin")):
            try:
                self.append_jsonl(
                    Path("/home/hatch/workspace/native-bridge/"
                         "spool/weixin.jsonl"),
                    {"msgid": msgid, "text": text,
                     "from_user": from_user,
                     "ts": int(time.time())})
                diverted = True
                log(f"msg {msgid} diverted to native bridge")
            except Exception as e:
                log(f"native divert failed, using inbox: {e}")
        if not diverted:
            self.append_jsonl(INBOX, entry)
        self._mark_seen(msgid)
        # Scenario-aware arrival feedback: exactly one immediate
        # notice per message, chosen by queue/burst/media/stop
        # state (see _maybe_arrival_feedback). Never raises; queue
        # semantics are unchanged.
        self._maybe_arrival_feedback(
            from_user, text, media=media, msgid=msgid,
            is_group=bool(msg.get("group_id")), diverted=diverted)
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

    def _store_partial(self, item_id: str, rec: dict) -> None:
        """Merge rec into the partial-progress record for one outbox row."""
        if not item_id:
            return
        stored = self._load_partial()
        prev = stored.get(item_id) or {}
        merged = dict(prev) if isinstance(prev, dict) else {}
        merged.update(rec)
        stored[item_id] = merged
        self._save_partial(stored)

    async def send_text(self, client: httpx.AsyncClient, creds: dict,
                        to_user_id: str, content: str, context_token: str = "",
                        client_id: str = "", message_state: int = 2,
                        item_id: str = "") -> dict:
        """Send content in chunks. item_id resumes after a partial success.

        Each chunk keeps the client_id it was first given, and a retry
        starts at the first chunk that did not succeed. That stops a
        long reply from repeating its opening paragraphs.
        """
        content = filter_markdown_weixin(content)
        last: dict = {"ret": 0}
        chunks = split_chunks(content) or [""]
        partial = self._load_partial().get(item_id, {}) if item_id else {}
        if not isinstance(partial, dict):
            partial = {}
        start = int(partial.get("sent_chunks") or 0)
        client_ids = dict(partial.get("client_ids") or {})
        for i, part in enumerate(chunks):
            if i < start:
                continue
            key = str(i)
            cid = client_ids.get(key) or (
                client_id if i == 0 and client_id else f"muse-{uuid.uuid4().hex}"
            )
            client_ids[key] = cid
            if item_id:
                partial["client_ids"] = client_ids
                partial["sent_chunks"] = start
                self._store_partial(item_id, partial)
            body = {
                "msg": {
                    "from_user_id": "",
                    "to_user_id": to_user_id,
                    "client_id": cid,
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
            start = i + 1
            if item_id:
                partial["sent_chunks"] = start
                self._store_partial(item_id, partial)
        return last

    # ---------- media & typing ----------

    async def _download_media(self, client: httpx.AsyncClient, media: list[dict], msgid: str) -> None:
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
            if not media_url_allowed(url):
                log(f"media {msgid}/{i}: refused non-Tencent url host")
                continue
            try:
                r = await client.get(url, timeout=httpx.Timeout(30.0, connect=10.0))
                r.raise_for_status()
                data = r.content
                if len(data) > 25 * 1024 * 1024:
                    log(f"media {msgid}/{i}: too large ({len(data)} bytes), skipped")
                    continue
                plain = aes_ecb_decrypt(data, key)
                path = MEDIA_DIR / media_filename(msgid, i, sniff_ext(plain))
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
            # 300s, not 60: probed live 2026-10-07 — a 1.96MB upload
            # through this egress to the WeChat CDN took ~80s to return
            # 200. At 60s every large-file attempt died as a read
            # timeout (surfacing as an empty exception) or a CDN 500,
            # which wedged a formal video reply for 30+ minutes while
            # small files went through fine.
            timeout=httpx.Timeout(300.0, connect=15.0),
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
        path = Path(fpath)
        if not outbound_file_allowed(path, CRED_FILE, [muse_home(), STATE, BASE, Path("/tmp")]):
            raise PermissionError(f"refusing to send file outside allowed directories: {fpath}")
        data = path.read_bytes()
        mtype = weixin_media_type(fpath)
        try:
            up = await self._upload_media(client, creds, to_user_id, data, mtype)
        except Exception as e:
            # Tag the failure as upload-stage so _note_failure can
            # count it even when str(e) is empty (bare transport
            # errors / timeouts raise messageless exceptions).
            raise UploadStageError(f"{UPLOAD_STAGE_MARKER}: {e}") from e
        item = self._media_item(mtype, up, Path(fpath).name, len(data))
        return await self._send_items(client, creds, to_user_id, [item], context_token)

    # ---------- outbox ----------

    def _outbox_offset(self) -> int:
        return read_offset(OUTBOX_OFFSET)

    @staticmethod
    def _load_retry() -> dict:
        return load_json_dict(OUTBOX_RETRY)

    @staticmethod
    def _save_retry(d: dict) -> None:
        try:
            tmp = OUTBOX_RETRY.with_name(OUTBOX_RETRY.name + ".tmp")
            tmp.write_text(json.dumps(d), encoding="utf-8")
            tmp.replace(OUTBOX_RETRY)
        except OSError:
            pass

    @staticmethod
    def _load_partial() -> dict:
        try:
            d = json.loads(OUTBOX_PARTIAL.read_text(encoding="utf-8"))
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _save_partial(d: dict) -> None:
        try:
            tmp = OUTBOX_PARTIAL.with_name(OUTBOX_PARTIAL.name + ".tmp")
            tmp.write_text(json.dumps(d), encoding="utf-8")
            tmp.replace(OUTBOX_PARTIAL)
        except OSError:
            pass

    def _caption_already_sent(self, item_id: str) -> bool:
        if not item_id:
            return False
        return bool((self._load_partial().get(item_id) or {}).get("caption_sent"))

    def _mark_caption_sent(self, item_id: str) -> None:
        if not item_id:
            return
        d = self._load_partial()
        rec = d.get(item_id) or {}
        rec["caption_sent"] = int(time.time())
        d[item_id] = rec
        self._save_partial(d)

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
                if o.get("mode") not in ("reply", "reply_file") or str(o.get("msgid")) != str(item.get("msgid")):
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

    # ---------- park-and-continue (retry lane) ----------

    @staticmethod
    def _load_parked() -> dict:
        return load_json_dict(OUTBOX_PARKED)

    @staticmethod
    def _save_parked(d: dict) -> None:
        atomic_write_text(OUTBOX_PARKED, json.dumps(d, ensure_ascii=False))

    @staticmethod
    def _last_errmsg(item_id: str) -> str:
        """Errmsg of the most recent result row for item_id (the one
        dispatch_outbox_item just appended). Tail-scans the results
        file; failure paths are rare, so the scan is cheap enough."""
        try:
            lines = OUTBOX_RESULTS.read_bytes().splitlines()[-400:]
            for raw in reversed(lines):
                try:
                    row = json.loads(raw)
                except Exception:
                    continue
                if row.get("id") == item_id:
                    return str(row.get("errmsg") or "")
        except OSError:
            pass
        return ""

    def _note_failure(self, item: dict, errmsg: str) -> tuple[str, bool]:
        """Register one failed dispatch of an outbox row and decide
        its fate. Returns (action, notify): action is "park" (keep
        retrying in the lane), "drop" (wedge guard) or "deadletter"
        (attempt cap); notify is True exactly once for a formal row
        reaching FORMAL_STUCK_NOTIFY_ATTEMPTS.

        ALL failure state is persisted: the parked record in
        OUTBOX_PARKED (item payload, attempt count n, next retry
        time, guard streaks, notified flag) and a mirror in
        OUTBOX_RETRY ({n, next, notified}) so the CLI reply gate
        and status tooling keep seeing the row as still-retrying.
        Terminal actions remove both records."""
        item_id = str(item.get("id") or "")
        mode = item.get("mode", "")
        if not item_id:
            return "park", False
        parked = self._load_parked()
        rec = parked.get(item_id)
        if rec is None:
            rec = {"item": item, "n": 0, "parked_at": int(time.time()),
                   "notice_streak": 0, "cdn_streak": 0, "notified": False,
                   "upload_fail": 0, "rerouted": False}
        rec["item"] = item
        rec["n"] = int(rec.get("n") or 0) + 1
        n = rec["n"]
        rec["next"] = time.time() + retry_backoff_secs(n)
        if mode == "send" and item.get("notice"):
            # Wedge guard: unbound notice rows drop after
            # SEND_NOTICE_DROP_AFTER_FAILURES consecutive failures.
            rec["notice_streak"] = int(rec.get("notice_streak") or 0) + 1
        if mode == "send_file":
            # Upload-stage failures (2026-10-08 widening): the
            # "upload-stage" marker _deliver_file stamps on upload-
            # leg exceptions counts — including empty transport
            # errors — and so does the legacy pattern (errmsg names
            # the CDN host AND a 500). cdn_streak is still kept for
            # observability of the original CDN-500 subset.
            err = errmsg or ""
            is_cdn500 = "cdn.weixin.qq.com" in err and "500" in err
            if is_cdn500:
                rec["cdn_streak"] = int(rec.get("cdn_streak") or 0) + 1
            if is_cdn500 or UPLOAD_STAGE_MARKER in err:
                rec["upload_fail"] = int(rec.get("upload_fail") or 0) + 1
        notify = (mode in FORMAL_MODES
                  and n >= FORMAL_STUCK_NOTIFY_ATTEMPTS
                  and not rec.get("notified"))
        if notify:
            rec["notified"] = True
        # send_file upload-stage reroute (2026-10-08): at the
        # threshold the row is consumed, but the caller must run
        # _notify_stuck_formal (its file branch sends the WeChat
        # text notice + reroutes the file via WeCom) — signalled
        # through the same notify channel, exactly once.
        reroute = (mode == "send_file"
                   and int(rec.get("upload_fail") or 0)
                   >= SEND_FILE_UPLOAD_REROUTE_AFTER_FAILURES
                   and not rec.get("rerouted"))
        if reroute:
            rec["rerouted"] = True
            notify = True
        action = "park"
        if (mode == "send" and item.get("notice")
                and int(rec.get("notice_streak") or 0)
                >= SEND_NOTICE_DROP_AFTER_FAILURES):
            action = "drop"
            log(f"DROPPING unbound send {item_id} after "
                f"{rec['notice_streak']} consecutive failed attempts "
                f"(wedge guard)")
        elif reroute:
            action = "drop"
            log(f"send_file {item_id}: consumed after "
                f"{rec['upload_fail']} consecutive upload-stage "
                f"failures; notifying user + rerouting via WeCom "
                f"(wedge guard, no longer silent)")
        elif mode not in FORMAL_MODES and n >= SEND_MAX_ATTEMPTS:
            action = "deadletter"
            self.append_jsonl(OUTBOX_RESULTS, {
                "id": item_id, "mode": mode, "ts": int(time.time()),
                "ok": False,
                "errmsg": (f"dead-lettered after {n} failed attempts; "
                           "queue unblocked"),
                "deadletter": True})
            log(f"outbox {mode} {item_id}: dead-lettered after "
                f"{n} attempts (retry lane)")
        retry = self._load_retry()
        if action == "park":
            parked[item_id] = rec
            mirror = {"n": n, "next": rec["next"]}
            if rec.get("notified"):
                mirror["notified"] = True
            retry[item_id] = mirror
        else:
            parked.pop(item_id, None)
            retry.pop(item_id, None)
        self._save_parked(parked)
        self._save_retry(retry)
        return action, notify

    def _cleanup_item(self, item_id: str) -> None:
        """A row reached a terminal state (delivered): drop its
        retry mirror, parked record and chunk-partial state."""
        if not item_id:
            return
        retry = self._load_retry()
        if item_id in retry:
            retry.pop(item_id, None)
            self._save_retry(retry)
        parked = self._load_parked()
        if item_id in parked:
            parked.pop(item_id, None)
            self._save_parked(parked)
        partial = self._load_partial()
        if item_id in partial:
            partial.pop(item_id, None)
            self._save_partial(partial)

    async def _notify_stuck_formal(self, client: httpx.AsyncClient,
                                   creds: dict, item: dict,
                                   attempts: int) -> None:
        """A formal reply has failed FORMAL_STUCK_NOTIFY_ATTEMPTS
        times. It is NOT dropped (see FORMAL_MODES) — but the user
        must know. The notice cannot ride the outbox behind the
        stuck row, so it goes (a) in-channel by direct send,
        bypassing the queue, and (b) cross-channel as an appended
        row in the WeCom outbox. Both legs are best-effort;
        failures are only logged. Called once per stuck row (the
        caller persists a notified flag in the retry record)."""
        fpath = str(item.get("file_path") or "")
        is_file = item.get("mode") in ("reply_file", "send_file") \
            and fpath and Path(fpath).exists()
        if is_file:
            # File rows fail on the CDN upload leg (slow/timeout —
            # see _upload_media), NOT on the text leg, so the old
            # "send a message to help recovery" advice was wrong for
            # them, and a bare notice left the user empty-handed.
            # Reroute the file itself via WeCom (below) and say so.
            # Wording follows the three-tier rule (2026-10-08):
            # state what the server did / did not accept, what was
            # rerouted, and what still awaits the user's own
            # confirmation — a server-side ret=0 is never phrased
            # as "delivered".
            if item.get("mode") == "send_file":
                # Terminal reroute: the wedge guard consumed the
                # row after repeated upload-stage failures.
                text = (f"⚠️ 文件「{Path(fpath).name}」发送状态："
                        f"① 服务端：微信上传连续失败 {attempts} 次，"
                        "微信服务端尚未接受这个文件；"
                        "② 转投：已把原文件转投到你的企微；"
                        "③ 待确认：请在企微确认收到——微信这边已停止"
                        "重试，原文件在 VM 上，需要时告诉我再发。")
            else:
                text = (f"⚠️ 文件「{Path(fpath).name}」发送状态："
                        f"① 服务端：微信上传连续失败 {attempts} 次，"
                        "微信服务端尚未接受这个文件；"
                        "② 转投：已把原文件转投到你的企微应急；"
                        "③ 微信这边仍在继续重试、不会丢弃，最终以你"
                        "在微信实际收到为准。")
        else:
            text = (f"⚠️ 微信这边有一条正式回复已连续发送失败 {attempts} 次，"
                    "仍在自动重试、不会丢弃。多半是微信发送通道临时故障；"
                    "你在微信里随便发一句话，有助于恢复发送。")
        try:
            info = self.context.get(str(item.get("msgid", "")), {})
            to = info.get("from_user_id", "") \
                or str(item.get("to_user_id", "") or "")
            if to:
                await self.send_text(
                    client, creds, to, text,
                    info.get("context_token", ""), message_state=2)
        except Exception as e:
            log(f"stuck-formal notice (wechat leg) failed: {e}")
        try:
            if STUCK_NOTICE_WECOM_OUTBOX.exists():
                self.append_jsonl(STUCK_NOTICE_WECOM_OUTBOX, {
                    "mode": "send", "chatid": STUCK_NOTICE_WECOM_CHATID,
                    "chat_type": 1, "content": "【微信通道提醒】" + text,
                    "id": uuid.uuid4().hex[:12],
                    "queued_at": int(time.time())})
                if is_file:
                    self.append_jsonl(STUCK_NOTICE_WECOM_OUTBOX, {
                        "mode": "send_file",
                        "chatid": STUCK_NOTICE_WECOM_CHATID,
                        "chat_type": 1, "file_path": fpath,
                        "id": uuid.uuid4().hex[:12],
                        "queued_at": int(time.time())})
                    log(f"stuck formal {item.get('id')}: file rerouted "
                        f"via WeCom: {fpath}")
        except Exception as e:
            log(f"stuck-formal notice (wecom leg) failed: {e}")
        log(f"stuck formal {item.get('mode')} {item.get('id')}: "
            f"user notified at attempt {attempts}")

    def _compress_file_sync(self, src: Path, level: int = 0) -> Path | None:
        """Compress src into COMPRESSED_DIR and return the new path,
        or None when not applicable / failed / not smaller. Ladder
        (2026-10-08): L0 = video ffmpeg <=854px CRF30 / image PIL
        <=1600px q80; L1 = 640px CRF36 / 1280px q65; L2 = 480px
        CRF42 / 1024px q55. The cache key carries the level so
        rungs never pollute each other. Never modifies the
        original."""
        try:
            level = max(0, min(2, int(level)))
            vwidth, vcrf, imax, iq = (
                (854, 30, 1600, 80) if level == 0 else
                (640, 36, 1280, 65) if level == 1 else
                (480, 42, 1024, 55))
            size = src.stat().st_size
            st = src.stat()
            key = hashlib.sha256(
                f"{src}|{size}|{int(st.st_mtime)}|L{level}".encode()
            ).hexdigest()[:16]
            COMPRESSED_DIR.mkdir(parents=True, exist_ok=True)
            ext = src.suffix.lower()
            if ext in VIDEO_EXTS:
                out = COMPRESSED_DIR / f"{key}.mp4"
                if not (out.exists() and out.stat().st_size > 0):
                    r = subprocess.run(
                        ["ffmpeg", "-y", "-i", str(src),
                         "-vf", f"scale='min({vwidth},iw)':-2",
                         "-c:v", "libx264", "-crf", str(vcrf),
                         "-preset", "veryfast",
                         "-c:a", "aac", "-b:a", "96k",
                         "-movflags", "+faststart", str(out)],
                        capture_output=True, timeout=180)
                    if r.returncode != 0:
                        log(f"ffmpeg compress failed for {src.name}: "
                            f"{r.stderr.decode('utf-8', 'replace')[-200:]}")
                        return None
            elif ext in IMAGE_EXTS:
                out = COMPRESSED_DIR / f"{key}.jpg"
                if not (out.exists() and out.stat().st_size > 0):
                    from PIL import Image
                    with Image.open(src) as im:
                        im = im.convert("RGB")
                        im.thumbnail((imax, imax))
                        im.save(out, "JPEG", quality=iq)
            else:
                return None
            if out.exists() and 0 < out.stat().st_size < size:
                return out
            return None
        except Exception as e:
            log(f"compression failed for {src}: {e}")
            return None

    @staticmethod
    def _compress_level_for(parked_n: int) -> int:
        """Compression ladder rung for a parked row's failure
        count: n>=4 -> L2, n>=2 -> L1, else L0."""
        if parked_n >= 4:
            return 2
        if parked_n >= 2:
            return 1
        return 0

    async def _prepare_file_for_send(self, fpath: str, level: int = 0,
                                     parked_n: int = 0):
        """Return (effective_path, was_compressed). Compression
        triggers when the file is over FILE_COMPRESS_THRESHOLD OR
        this is a retry of an already-failed row (parked_n >= 1),
        at the given ladder level; cached per level; any problem
        falls back to the original path."""
        try:
            p = Path(fpath)
            if (p.exists()
                    and (p.stat().st_size > FILE_COMPRESS_THRESHOLD
                         or parked_n >= 1)
                    and COMPRESSED_DIR not in p.parents):
                out = await asyncio.to_thread(
                    self._compress_file_sync, p, int(level))
                if out is not None:
                    log(f"compressed for send (L{level}): {p.name} "
                        f"{p.stat().st_size} -> {out.stat().st_size} bytes")
                    return str(out), True
        except Exception as e:
            log(f"compression skipped for {fpath}: {e}")
        return fpath, False

    def _parked_n(self, item_id: str) -> int:
        """Current parked failure count for an outbox row (0 when
        it has never failed). Drives the compression ladder."""
        if not item_id:
            return 0
        rec = self._load_parked().get(item_id) or {}
        return int(rec.get("n") or 0)

    @staticmethod
    def _file_result_meta(fpath: str, level: int) -> dict:
        """Result-row fields for a file dispatch (2026-10-08):
        byte size + MD5 of the file actually sent (post-compression
        effective path) and the compression level applied."""
        meta = {"compress_level": int(level)}
        try:
            data = Path(fpath).read_bytes()
            meta["file_bytes"] = len(data)
            meta["file_md5"] = hashlib.md5(data).hexdigest()
        except OSError:
            pass
        return meta

    async def _maybe_notify_level_up(self, client: httpx.AsyncClient,
                                     creds: dict, item_id: str,
                                     to: str, level: int) -> None:
        """Tell the user once per rung when a retry sends the file
        at a higher (more degraded) compression level. The
        notified rungs live in the parked record so restarts and
        repeated dispatches cannot re-send the notice."""
        if level <= 0 or not item_id or not to:
            return
        try:
            parked = self._load_parked()
            rec = parked.get(item_id)
            if rec is None:
                return
            notified = list(rec.get("compress_notified") or [])
            prev = int(rec.get("compress_level") or 0)
            if level <= prev or level in notified:
                return
            rec["compress_level"] = level
            rec["compress_notified"] = notified + [level]
            parked[item_id] = rec
            self._save_parked(parked)
            await self.send_text(
                client, creds, to,
                f"（这个文件已多次发送失败，这次降到 L{level} 档压缩"
                "发送；原文件没动，在 VM 上可取）",
                "", "", 2, f"{item_id}:levelup")
        except Exception as e:
            log(f"level-up notice failed for {item_id}: {e}")

    async def dispatch_outbox_item(self, client: httpx.AsyncClient, creds: dict, item: dict) -> bool:
        """Return True if the item is consumed (delivered or a
        permanent skip: cancelled / suppressed / unroutable),
        False if it failed transiently — the caller then parks it
        in the retry lane via _note_failure (park-and-continue)."""
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
                info = self._route_for(str(item.get("msgid", "")))
                to = info.get("from_user_id", "")
                token_ctx = info.get("context_token", "")
                if not to:
                    result["errmsg"] = f"no context stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox reply {item_id}: {result['errmsg']}")
                    return True
                resp = await self.send_text(
                    client, creds, to, content, token_ctx,
                    info.get("client_id", ""), 2, item_id,
                )
                if resp.get("ret") in (None, 0):
                    typing_after = (to, token_ctx, 2)
            elif mode == "update":
                info = self._route_for(str(item.get("msgid", "")))
                to = info.get("from_user_id", "")
                if not to:
                    result["errmsg"] = f"no context stored for msgid {item.get('msgid')}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    log(f"outbox update {item_id}: {result['errmsg']}")
                    return True
                resp = await self.send_text(
                    client, creds, to, content, info.get("context_token", ""),
                    info.get("client_id", ""), 2, f"{item_id}:update",
                )
            elif mode == "reply_file":
                info = self._route_for(str(item.get("msgid", "")))
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
                orig_name = Path(fpath).name
                pn = self._parked_n(item_id)
                lvl = self._compress_level_for(pn)
                fpath, was_compressed = \
                    await self._prepare_file_for_send(fpath, lvl, pn)
                result.update(self._file_result_meta(
                    fpath, lvl if was_compressed else 0))
                if was_compressed and lvl > 0:
                    await self._maybe_notify_level_up(
                        client, creds, item_id, to, lvl)
                caption = content or f"📎 {orig_name}"
                if was_compressed:
                    caption += "\n（文件较大，已自动压缩发送）"
                # The caption is a separate text message sent BEFORE
                # the file. If the file step then fails transiently,
                # a naive retry re-sends the caption every attempt —
                # on 2026-10-05 the user received the same caption
                # ~17 times while the upload kept returning 500.
                # Send the caption at most once per outbox item.
                if not self._caption_already_sent(item_id):
                    cap_resp = await self.send_text(
                        client, creds, to, caption, token_ctx,
                        "", 2, f"{item_id}:caption",
                    )
                    if cap_resp.get("ret") not in (None, 0):
                        result["ret"] = cap_resp.get("ret")
                        result["errmsg"] = cap_resp.get("errmsg", "")
                        self.append_jsonl(OUTBOX_RESULTS, result)
                        log(f"outbox {mode} {item_id}: ok=False err={result.get('errmsg', '')}")
                        return False
                    self._mark_caption_sent(item_id)
                resp = await self._deliver_file(client, creds, to, fpath, token_ctx)
                if resp.get("ret") in (None, 0):
                    typing_after = (to, token_ctx, 2)
            elif mode == "send":
                resp = await self.send_text(
                    client, creds, item.get("to_user_id", ""), content,
                    "", "", 2, item_id,
                )
            elif mode == "send_file":
                to = item.get("to_user_id", "")
                fpath = item.get("file_path", "")
                if fpath and not Path(fpath).exists():
                    result["errmsg"] = f"file not found: {fpath}"
                    self.append_jsonl(OUTBOX_RESULTS, result)
                    return True
                if fpath:
                    pn = self._parked_n(item_id)
                    lvl = self._compress_level_for(pn)
                    fpath, was_compressed = \
                        await self._prepare_file_for_send(fpath, lvl, pn)
                    result.update(self._file_result_meta(
                        fpath, lvl if was_compressed else 0))
                    if was_compressed:
                        if lvl > 0:
                            await self._maybe_notify_level_up(
                                client, creds, item_id, to, lvl)
                        content = (content + "\n" if content else "") \
                            + "（文件较大，已自动压缩发送）"
                if content and not self._caption_already_sent(item_id):
                    cap_resp = await self.send_text(
                        client, creds, to, content, "", "", 2, f"{item_id}:caption",
                    )
                    if cap_resp.get("ret") not in (None, 0):
                        result["ret"] = cap_resp.get("ret")
                        result["errmsg"] = cap_resp.get("errmsg", "")
                        self.append_jsonl(OUTBOX_RESULTS, result)
                        return False
                    self._mark_caption_sent(item_id)
                resp = await self._deliver_file(
                    client, creds, to, fpath
                )
            else:
                result["errmsg"] = f"unknown mode {mode}"
                self.append_jsonl(OUTBOX_RESULTS, result)
                return True
            result["ret"] = resp.get("ret") if isinstance(resp, dict) else None
            result["errmsg"] = resp.get("errmsg", "") if isinstance(resp, dict) else ""
            result["ok"] = (resp.get("ret") in (None, 0)) if isinstance(resp, dict) else False
        except (FileNotFoundError, PermissionError) as e:
            result["errmsg"] = f"exception: {e}"
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: ok=False err={result['errmsg']}")
            return True
        except Exception as e:
            result["errmsg"] = f"exception: {e}"
            if isinstance(e, UploadStageError):
                result["stage"] = "upload"
            self.append_jsonl(OUTBOX_RESULTS, result)
            log(f"outbox {mode} {item_id}: ok=False err={result['errmsg']}")
            self.write_status()
            # Transient failure: the caller (outbox_loop / retry
            # lane) parks the row via _note_failure; all wedge-guard
            # and dead-letter decisions live there now.
            return False
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
        # Failure bookkeeping (wedge guards, dead-letter, parked
        # retries) is the caller's job now — see _note_failure.
        return bool(result["ok"])

    async def outbox_loop(self, client: httpx.AsyncClient, creds: dict) -> None:
        """Park-and-continue outbox (2026-10-06): the main queue NEVER
        waits behind a failing row. Each cycle: (1) drain the main
        queue — a row that dispatches ok is consumed; a row that
        fails is registered via _note_failure and the offset still
        advances past it (parked / dropped / dead-lettered), so the
        next row goes out in the same cycle; (2) run the retry lane —
        parked rows whose backoff has expired get one dispatch each,
        in due-time order; their fate is again decided by
        _note_failure. Before this, one prepare-failed head row
        froze ALL outbound for 30+ minutes (deep-dive evidence)."""
        while True:
            await asyncio.sleep(1.0)
            # ---- main queue pass ----
            if OUTBOX.exists():
                offset = self._outbox_offset()
                size = OUTBOX.stat().st_size
                if offset > size:
                    offset = 0
                if offset < size:
                    with OUTBOX.open("rb") as f:
                        f.seek(offset)
                        data = f.read()
                    consumed = 0
                    parked_ids = set(self._load_parked().keys())
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
                        if item_id and item_id in parked_ids:
                            # Duplicate row for an already-parked id:
                            # the lane owns it; just consume this copy.
                            consumed += len(raw_line) + 1
                            try:
                                write_offset(OUTBOX_OFFSET, offset + consumed)
                            except OSError:
                                pass
                            continue
                        consumed_ok = await self.dispatch_outbox_item(
                            client, creds, item)
                        if consumed_ok:
                            self._cleanup_item(item_id)
                        else:
                            _action, notify = self._note_failure(
                                item, self._last_errmsg(item_id))
                            if notify:
                                asyncio.create_task(
                                    self._notify_stuck_formal(
                                        client, creds, item,
                                        FORMAL_STUCK_NOTIFY_ATTEMPTS))
                        # Success, park, drop and dead-letter ALL
                        # advance the offset: the main queue moves on
                        # in the same cycle either way.
                        consumed += len(raw_line) + 1
                        try:
                            write_offset(OUTBOX_OFFSET, offset + consumed)
                        except OSError:
                            pass
            # ---- retry lane pass ----
            try:
                parked = self._load_parked()
                now = time.time()
                due = [rec for rec in parked.values()
                       if float(rec.get("next") or 0) <= now]
                due.sort(key=lambda r: float(r.get("next") or 0))
                for rec in due:
                    item = rec.get("item") or {}
                    item_id = str(item.get("id") or "")
                    if not item_id:
                        continue
                    consumed_ok = await self.dispatch_outbox_item(
                        client, creds, item)
                    if consumed_ok:
                        self._cleanup_item(item_id)
                    else:
                        _action, notify = self._note_failure(
                            item, self._last_errmsg(item_id))
                        if notify:
                            asyncio.create_task(
                                self._notify_stuck_formal(
                                    client, creds, item,
                                    FORMAL_STUCK_NOTIFY_ATTEMPTS))
            except Exception as e:
                log(f"retry lane pass failed (continuing): {e}")

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
                    failures += 1
                    log(f"getupdates ret={ret} errcode={errcode} errmsg={resp.get('errmsg')} ({failures})")
                    if failures >= 5:
                        return f"getupdates failed repeatedly: ret={ret} errcode={errcode}"
                    await asyncio.sleep(min(2 * failures, 10))
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
