#!/usr/bin/env python3
"""Native bridge: fast lane for the WeChat/WeCom channels. (v2)

Consumes a per-channel spool (rows the gateway diverts BEFORE the inbox)
and drives native muse.ai turns in per-channel persistent sessions via
the reassembling GW2 client. Finished replies are written into the
channel outbox as formal bound replies. Hard failures BEFORE a turn is
sent (auth/connect/session) fall back to the channel inbox, where the
existing cold-start hook/worker path (the backup) takes over. Failures
after a turn is sent never fall back (would risk double answers).

Lifecycle, deliberately mirroring the official client:
- one session per channel (context continuity); queued rows start
  strictly FIFO when the running turn finishes, exactly like sending
  another message in the official app while it works. There is no
  priority lane and no cut-in (the /jump experiment was removed
  2026-10-07 on the user's "官网是啥就是啥" order).
- /stop works via cancelled.json + chat.cancel; /new
  (topic_boundary.json ts) rotates the session for later turns;
  sessions also auto-rotate at AUTO_ROTATE_TURNS / AUTO_ROTATE_AGE.
- a queue snapshot is kept in state.json for the gateways' /queue.

Modes per channel: live only when flag file enabled-<channel> exists in
this directory; otherwise shadow (outbox/inbox writes go to shadow/).
Credentials: env NATIVE_TOKEN_JSON, else cookies.txt + vm_id.txt in
~/.config/native-bridge/ (auto-renew, primary), else token.json there.
"""
import hashlib, json, os, sys, threading, time, traceback
from datetime import datetime, timezone

BASE = "/home/hatch/workspace/native-bridge"
sys.path.insert(0, "/home/hatch/workspace/native-probe")
from gw2 import GW2  # noqa: E402
from muse_cli.cli import fmt_event, is_reply  # noqa: E402
from muse_cli.gateway import GatewayError  # noqa: E402

CFG = json.load(open(os.path.join(BASE, "config.json")))
CONF_DIR = os.path.expanduser("~/.config/native-bridge")
STATE_F = os.path.join(BASE, "state.json")
STATUS_F = os.path.join(BASE, "status.json")
POLL_SECS = 2
PROG_FIRST_SECS = 120
PROG_EVERY_SECS = 300
ESCALATE_SECS = 1200        # turn age where progress notices turn into
                            # an actionable "looks stuck, /stop it" warning
ACTIVITY_POLL_SECS = 60     # activity.list cadence during a long turn
                            # (P3: coarse progress signals folded into
                            # the existing progress notices only)
IDLE_PUSH_EVERY_SECS = 20   # idle-session scan cadence (see idle_push_scan)
# --- LT2 long-task trailing loop + session self-heal (2026-10-08) -----
# Ported from the reference implementation, muse-cli's chat.py
# Chat.send resident loop (baseline seq, poll history, print each new
# reply as it lands, reset the quiet window after every reply). All
# LT2 behaviour is gated per channel by a longtask-<channel> flag
# file (see longtask_mode); with the flag absent every code path
# below is inert and behaviour is byte-for-byte the pre-LT2 bridge.
TRAILING_QUIET_SECS = 30    # LT2: status completed + this much silence
                            # after the last reply closes the turn
                            # (chat.py's QUIET, stretched for channels)
TRAILING_MAX_SECS = 600     # LT2: hard cap on the trailing window;
                            # later arrivals are the idle scan's job,
                            # continuing on the SAME cursor (spec 3)
LT_EVENT_PROG_MIN_SECS = 60   # LT2: min gap between event-triggered
                                # progress notices for one turn
IDLE_FAIL_ALERT_STREAK = 3    # LT2: idle-scan failures before alerting
IDLE_ALERT_MIN_SECS = 3600    # LT2: min gap between idle-scan alerts
MERGE_CHANNELS = {"weixin", "test", "wecom"}
# Channels in MERGE_CHANNELS fold a queued message into the running
# turn (official-client steering) while that turn's reply has not
# started; other channels keep strict FIFO. The combined answer is
# bound to the turn's first msgid; merged msgids are closed out per
# channel (see _silence_merged): silently via feedback_clear where
# the gateway consumes that file, else with this pointer as their
# bound reply.
MERGE_POINTER_TEXT = "（已并入上一条处理）"
# Channels whose gateway actually consumes feedback_clear.jsonl:
# weixin's gateway folds it into its feedback scan
# (_consume_feedback_clear). The WeCom gateway has NO feedback
# track/scan at all, and every diverted WeCom message holds an open
# think stream that only a bound reply finishes in place — a
# silently-cleared merged msgid would hang until the stream watchdog
# (~540s) closes it with a false "taking too long" bubble. So WeCom
# merged msgids always take the pointer bound reply instead, which
# finishes their own stream in place; there is no scan on that side
# to send anything further for the msgid.
FEEDBACK_CLEAR_CHANNELS = {"weixin", "test"}
AUTO_ROTATE_TURNS = 80
AUTO_ROTATE_AGE = 7 * 86400
NODE_ID = "native-bridge"

# --- /new exit audit (P4, 2026-10-08) ---------------------------------
# When the user rotates with /new, the fresh session must not leak the
# previous session's content (the PREAMBLE_FRESH rule is behavioural;
# this audit is the bridge-side backstop). At the moment session_for
# creates the /new session, the OLD session's history is fingerprinted
# into a frozen corpus (hashes only — the old text itself is never
# persisted). Replies of THAT fresh session are checked in
# deliver_reply before delivery: any verbatim fragment of
# AUDIT_WINDOW+ normalized chars matching the corpus blocks the reply.
# The audit is bound to the exact fresh session id recorded in state
# (fresh_session_id): ordinary continued sessions and auto-rotated
# sessions (80 turns / 7 days) never match it — session_for clears
# fresh_session_id on any non-/new rotation, so the audit lapses as
# soon as the fresh session is itself auto-rotated away.
AUDIT_WINDOW = 24           # min verbatim fragment length that blocks
AUDIT_STEP = 1              # step 1: ANY >=WINDOW fragment is detected,
                            # regardless of alignment
AUDIT_HISTORY_LIMIT = 100   # old-session events pulled at rotation
AUDIT_MAX_FINGERPRINTS = 8000   # corpus cap; when exceeded, the newest
                                # old-session content is kept (older
                                # messages drop out first)
AUDIT_BLOCK_TEXT = (
    "⚠️ 这条回复引用了上一会话的内容，已按新会话规则拦截。"
    "请换个问法，或把需要的背景在新会话里重新发我。")

PREAMBLE = (
    "【渠道桥接说明】你正在通过桥接程序回复手机渠道（微信/企业微信）上的用户。"
    "你的最终回复文本会被原样转发到该渠道，所以：结论先行、简洁自然；"
    "如果生成了要发给用户的文件，在回复末尾单独一行写 [[FILE:文件的绝对路径]]；"
    "不要提及桥接机制。"
    "渠道内不要使用卡片/选项控件提问；需要用户选择时用纯文本编号列出选项。"
    "\n\n用户消息："
)
# A session created by the user's /new carries one extra rule. The
# rotation itself only swaps the session id; the agent's platform
# tools can still READ other sessions, and when the user tested /new
# by asking "what did I just ask you", the fresh session looked the
# old conversation up and quoted it (2026-10-08 02:41, session
# 6bdfcfbe) — to the user, /new had not worked. The ack promises
# earlier conversation is not carried in; this clause makes the
# agent honour that instead of fetching it.
PREAMBLE_FRESH = PREAMBLE.replace(
    "不要提及桥接机制。",
    "不要提及桥接机制。\n"
    "【新会话】用户刚主动开了新会话，这是全新对话：不要查阅、引用或复述"
    "其它会话（包括刚结束的上一个会话）的内容；即使用户问起之前聊过什么，"
    "也只说明这是新会话、之前的内容没有带入，请用户在新会话里重新说明。")

_state_lock = threading.Lock()


def load_state():
    try:
        return json.load(open(STATE_F))
    except (OSError, ValueError):
        return {"channels": {}}


def save_state(st):
    tmp = STATE_F + ".tmp"
    json.dump(st, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_F)


def ch_state(st, ch):
    defaults = {
        "session_id": None, "preamble_done": False,
        "spool_offset": 0, "processed": 0, "fallbacks": 0, "cancels": 0,
        "fallback_streak": 0, "last_fb_alert": 0,
        "turns": [], "last_boundary_ts": None,
        "rotate_main": False, "fresh_start": False,
        "fresh_session_id": None, "audit_fingerprints": [],
        "audit_blocks": 0, "audit_last_block": 0,
        "session_started": {"main": 0},
        "session_turns": {"main": 0},
        "admin_offset": 0,
        "idle_push_seq": 0, "idle_pushed_ids": [],
        "idle_scan_fail_streak": 0, "idle_scan_last_alert": 0,
        "queue_snapshot": {"active": [], "queued": []}}
    c = st["channels"].setdefault(ch, {})
    for k, v in defaults.items():
        c.setdefault(k, v)
    return c


def write_status(extra):
    try:
        cur = json.load(open(STATUS_F))
    except (OSError, ValueError):
        cur = {}
    cur.update(extra)
    cur["ts"] = int(time.time())
    tmp = STATUS_F + ".tmp"
    json.dump(cur, open(tmp, "w"), ensure_ascii=False, indent=1)
    os.replace(tmp, STATUS_F)


def live_mode(ch):
    return os.path.exists(os.path.join(BASE, f"enabled-{ch}"))


def longtask_mode(ch):
    """LT2 gate: the long-task trailing loop / session self-heal /
    event-triggered progress apply only to channels carrying a
    longtask-<channel> flag file (same pattern as enabled-<channel>).
    Only the test channel is flagged during LT2 acceptance."""
    return os.path.exists(os.path.join(BASE, f"longtask-{ch}"))


def is_session_not_found(exc):
    """True when a gateway failure means the session id itself is
    gone server-side (the 2026-10-08 mass-404: every stored sid had
    vanished from sessions.list). Matches either the structured
    GatewayError status or the message text, so stub/plain errors
    with the same meaning also count."""
    if getattr(exc, "status", None) == 404:
        return True
    return "session not found" in str(exc).lower()


def outbox_path(ch):
    if live_mode(ch):
        return os.path.join(CFG["channels"][ch]["bot_state"], "outbox.jsonl")
    return os.path.join(BASE, "shadow", f"{ch}-outbox.jsonl")


def inbox_path(ch):
    if live_mode(ch):
        return os.path.join(CFG["channels"][ch]["bot_state"], "inbox.jsonl")
    return os.path.join(BASE, "shadow", f"{ch}-inbox.jsonl")


def append_jsonl(path, row):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def bridge_row_id(msgid, suffix=""):
    return hashlib.sha256(f"bridge:{msgid}:{suffix}".encode()).hexdigest()[:12]


def outbox_already_has(ch, row_id):
    p = outbox_path(ch)
    try:
        with open(p, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 262144))
            tail = f.read().decode("utf-8", "replace")
        return f'"id": "{row_id}"' in tail
    except OSError:
        return False


def cancelled_ids(ch):
    p = os.path.join(CFG["channels"][ch]["bot_state"], "cancelled.json")
    try:
        data = json.load(open(p))
        return {r.get("msgid") for r in data if isinstance(r, dict)}
    except (OSError, ValueError):
        return set()


def read_signal_ts(path):
    if not path:
        return None
    try:
        return float(json.load(open(path)).get("ts", 0)) or None
    except (OSError, ValueError):
        return None


def token_params():
    raw = os.environ.get("NATIVE_TOKEN_JSON")
    if raw:
        return json.loads(raw)
    p = os.path.join(CONF_DIR, "token.json")
    if os.path.exists(p):
        return json.load(open(p))
    return None


def cookie_params():
    cp = os.path.join(CONF_DIR, "cookies.txt")
    vp = os.path.join(CONF_DIR, "vm_id.txt")
    if not (os.path.exists(cp) and os.path.exists(vp)):
        return None
    from muse_cli.gateway import load_cookies
    cookies = load_cookies(cp)
    if not cookies.strip():
        return None
    return cookies, open(vp).read().strip()


def connect():
    """Build a GW2 and prove it with a cheap call. Raises on failure."""
    if not os.environ.get("NATIVE_TOKEN_JSON"):
        ck = cookie_params()
        if ck:
            try:
                gw = GW2(cookies=ck[0], vm_id=ck[1])
                gw.call_json("sessions.list", timeout=15)
                return gw
            except Exception as e:
                print(f"[bridge] cookie auth failed, trying token file: "
                      f"{type(e).__name__} {str(e)[:120]}", flush=True)
    tp = token_params()
    if tp:
        gw = GW2(cookies="", vm_id=tp["vm_id"],
                 access_token=tp["access_token"], hatch_token=tp["hatch_token"])
        gw.call_json("sessions.list", timeout=15)
        return gw
    raise RuntimeError("no native credential (cookies.txt or token.json)")


def fallback_to_inbox(ch, row, why):
    cfg = CFG["channels"][ch]
    msgid, text = row["msgid"], row.get("text", "")
    if ch == "weixin":
        rec = {"msgid": msgid, "ts": int(time.time()),
               "from_user_id": cfg["from_user_id"], "group_id": "",
               "text": text, "media": [], "context_token": "", "raw": {},
               "via": "native-bridge-fallback", "fallback_why": why}
    elif ch == "wecom":
        rec = {"msgid": msgid, "ts": int(time.time()),
               "chattype": "single", "chatid": cfg["chatid"],
               "from_userid": cfg["from_userid"], "msgtype": "text",
               "text": text, "media": [], "auto_handled": False, "raw": {},
               "via": "native-bridge-fallback", "fallback_why": why}
    else:
        rec = {"msgid": msgid, "ts": int(time.time()), "text": text,
               "via": "native-bridge-fallback", "fallback_why": why}
        append_jsonl(os.path.join(BASE, "shadow", f"{ch}-inbox.jsonl"), rec)
        try:
            append_jsonl(os.path.join(BASE, "shadow",
                                      "bridge_fallback.jsonl"),
                         {"msgid": msgid, "ts": int(time.time())})
        except Exception:
            pass
        print(f"[{ch}] FALLBACK msgid={msgid} why={why}", flush=True)
        return
    append_jsonl(inbox_path(ch), rec)
    try:
        append_jsonl(os.path.join(os.path.dirname(inbox_path(ch)),
                                  "bridge_fallback.jsonl"),
                     {"msgid": msgid, "ts": int(time.time())})
    except Exception:
        pass
    print(f"[{ch}] FALLBACK msgid={msgid} why={why}", flush=True)


def _normalize_audit_text(text):
    """Whitespace-normalize for the audit: collapse every run of
    whitespace (incl. newlines) to a single space and strip."""
    return " ".join((text or "").split())


def _audit_windows(text):
    """Fingerprint hashes of every AUDIT_WINDOW-char window of the
    normalized text (step AUDIT_STEP). Hashes only — the underlying
    text is never stored anywhere by the audit."""
    norm = _normalize_audit_text(text)
    if len(norm) < AUDIT_WINDOW:
        return set()
    return {hashlib.sha256(
        norm[i:i + AUDIT_WINDOW].encode("utf-8")).hexdigest()[:16]
        for i in range(0, len(norm) - AUDIT_WINDOW + 1, AUDIT_STEP)}


def build_audit_corpus(events):
    """Frozen fingerprint corpus from an OLD session's history events
    (user + assistant messages both count). Returns a sorted list of
    window hashes, capped at AUDIT_MAX_FINGERPRINTS; the cap keeps the
    NEWEST old-session content (messages are folded in newest-first,
    so the limit drops the oldest material)."""
    texts = []
    for ev in sorted(events or [], key=lambda e: e.get("seq", 0)):
        if ev.get("event_name") not in ("message.user",
                                        "message.assistant"):
            continue
        t = fmt_event(ev).get("text", "")
        if t and t.strip():
            texts.append(t)
    fps = set()
    for t in reversed(texts):
        norm = _normalize_audit_text(t)
        for i in range(0, len(norm) - AUDIT_WINDOW + 1, AUDIT_STEP):
            if len(fps) >= AUDIT_MAX_FINGERPRINTS:
                return sorted(fps)
            fps.add(hashlib.sha256(
                norm[i:i + AUDIT_WINDOW].encode("utf-8")
            ).hexdigest()[:16])
    return sorted(fps)


def audit_reply(text, fingerprints):
    """Return AUDIT_WINDOW when the reply carries a verbatim fragment
    of >= AUDIT_WINDOW normalized chars present in the fingerprint
    corpus, else 0. [[FILE:...]] lines and the block notice itself
    never participate; shorter overlaps (< AUDIT_WINDOW) and
    paraphrases (no long verbatim run) pass by construction."""
    if not text or not fingerprints:
        return 0
    if text.strip() == AUDIT_BLOCK_TEXT:
        return 0
    kept = [ln for ln in text.splitlines()
            if not (ln.strip().startswith("[[FILE:")
                    and ln.strip().endswith("]]"))]
    fps = _audit_windows("\n".join(kept))
    if not fps:
        return 0
    return AUDIT_WINDOW if fps & set(fingerprints) else 0


def _audit_gate(ch, text):
    """True when this channel's reply must be blocked: the channel's
    CURRENT session is the recorded /new fresh session and the reply
    hits the frozen corpus. On a hit, bumps audit_blocks /
    audit_last_block in state and logs audit_blocked with the
    fragment length only (never the matched text)."""
    with _state_lock:
        cs = ch_state(load_state(), ch)
        fresh_sid = cs.get("fresh_session_id")
        if not fresh_sid or fresh_sid != cs.get("session_id"):
            return False
        fps = cs.get("audit_fingerprints") or []
        if not fps:
            return False
        if not audit_reply(text, fps):
            return False
        st = load_state()
        c = ch_state(st, ch)
        c["audit_blocks"] = (c.get("audit_blocks") or 0) + 1
        c["audit_last_block"] = int(time.time())
        blocks = c["audit_blocks"]
        save_state(st)
    print(f"[{ch}] audit_blocked frag_len={AUDIT_WINDOW} "
          f"blocks={blocks}", flush=True)
    return True


def deliver_reply(ch, msgid, text):
    """/new exit audit first (fresh sessions only; see _audit_gate):
    a blocked reply is never delivered — the fixed block notice goes
    out in its place (and no [[FILE:]] rows either). Everything else
    is delivered by _deliver_reply_raw unchanged."""
    if _audit_gate(ch, text):
        _deliver_reply_raw(ch, msgid, AUDIT_BLOCK_TEXT)
        return
    _deliver_reply_raw(ch, msgid, text)


def _deliver_reply_raw(ch, msgid, text):
    """Split [[FILE:path]] markers; write formal reply (+ reply_file rows)."""
    files, kept = [], []
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("[[FILE:") and s.endswith("]]"):
            p = s[len("[[FILE:"):-2].strip()
            if os.path.isfile(p):
                files.append(p)
                continue
        if s.startswith("[[hatch_widget:") and s.endswith("]]"):
            # Widget tokens never render on the channels (the user
            # sees nothing); drop a token that stands alone on its
            # own line. Inline mentions in prose are kept verbatim.
            continue
        kept.append(line)
    body = "\n".join(kept).strip()
    now = int(time.time())
    if body:
        rid = bridge_row_id(msgid)
        if not outbox_already_has(ch, rid):
            append_jsonl(outbox_path(ch),
                         {"mode": "reply", "msgid": msgid, "content": body,
                          "id": rid, "queued_at": now})
    for i, p in enumerate(files):
        rid = bridge_row_id(msgid, f"file{i}")
        if not outbox_already_has(ch, rid):
            append_jsonl(outbox_path(ch),
                         {"mode": "reply_file", "msgid": msgid,
                          "file_path": p, "content": body if i == 0 else "",
                          "id": rid, "queued_at": now})


def is_turn_reply(ev, baseline):
    """Bridge-local reply test. muse_cli.is_reply rejects any assistant
    event carrying reply_to_message_id (it treats those as proactive
    pushes), but a genuine turn answer gets stamped with one whenever
    the turn was resumed by an internal handoff (e.g. a background
    process completion) — the 2026-10-07 20:47 fix report was lost
    exactly that way (seq 5222, status completed, never delivered).
    Session turns are serialized server-side, so while a bridge turn
    is open, the first completed assistant message after baseline is
    that turn's answer; the other is_reply guards (event name, seq,
    text presence, completed status) still apply."""
    if ev.get("event_name") != "message.assistant":
        return False
    if (ev.get("seq") or 0) <= baseline:
        return False
    p = ev.get("payload", {}) if isinstance(ev.get("payload"), dict) else {}
    if not (p.get("display_text") or p.get("content")):
        return False
    if not p.get("status"):
        return False
    return True


def _activity_ts_epoch(raw):
    """Epoch seconds for an activity timestamp (ISO-8601 string or
    number); None when unparseable."""
    if isinstance(raw, (int, float)):
        return float(raw)
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def activity_summary(ev):
    """One short neutral line for an activity.list event, or "".
    activity.list is an account-wide stream, so summaries state only
    what the record itself carries (a file path in details may be
    quoted verbatim; anything else stays generic)."""
    t = ev.get("type") or ""
    details = ev.get("details") if isinstance(ev.get("details"), dict) \
        else {}
    if t in ("file_created", "file_updated"):
        path = details.get("path") or details.get("file_path") or \
            details.get("file") or ""
        verb = "创建了文件" if t == "file_created" else "更新了文件"
        return f"{verb} {path}".strip() if path else \
            (ev.get("title") or verb)
    if t == "task_running":
        tasks = details.get("tasks") if isinstance(
            details.get("tasks"), list) else []
        for task in tasks:
            if not isinstance(task, dict):
                continue
            if task.get("status") == "completed":
                label = task.get("label") or "子助手"
                prev = (task.get("response_preview") or "").strip()
                first = prev.splitlines()[0].strip() if prev else ""
                s = f"子助手已完成：{label}"
                if first:
                    s += f"：{first[:40]}"
                return s
        return "子助手运行中"
    if t == "web_search":
        title = ev.get("title") or ""
        return f"网页搜索：{title}".strip() if title else "网页搜索"
    return (ev.get("title") or ev.get("status_title") or "")[:80]


def _ev_mid(ev):
    p = ev.get("payload", {}) if isinstance(ev.get("payload"), dict) else {}
    return p.get("message_id") or ev.get("message_id") or ""


class ChannelWorker(threading.Thread):
    def __init__(self, ch):
        super().__init__(daemon=True, name=f"bridge-{ch}")
        self.ch = ch
        self.gw = None
        self.queue = []            # [(row, nbytes)] not yet started
        self.read_offset = None
        self.last_connect_try = 0.0
        self.last_idle_scan = 0.0
        self._turn_reply_marks = []   # [(seq, mid)] delivered as turn replies
        self._turns_dirty = False    # a turn field changed during polling
        self._session_from_stored = False   # LT2: last session_for reused sid

    def log(self, *a):
        print(f"[{self.ch}]", *a, flush=True)

    def cs(self):
        with _state_lock:
            return ch_state(load_state(), self.ch)

    def update_cs(self, **kw):
        with _state_lock:
            st = load_state()
            c = ch_state(st, self.ch)
            c.update(kw)
            save_state(st)

    def ensure_gw(self):
        if self.gw is not None:
            return True
        self.last_connect_try = time.time()
        try:
            self.gw = connect()
            self.log("connected")
            return True
        except Exception as e:
            self.log("connect failed:", type(e).__name__, str(e)[:140])
            self.gw = None
            return False

    def drop_gw(self):
        """Close the current connection, if any. Connections are
        turn-scoped: _start_next connects fresh for every turn and the
        connection is dropped when the turn ends, so the bridge never
        holds an idle connection (an idle one gets reaped somewhere
        along the path — the 2026-10-07 fallback root cause)."""
        gw, self.gw = self.gw, None
        if gw is not None:
            try:
                gw.close()
            except Exception:
                pass

    def new_session(self):
        d = self.gw.call_json("session.start", body={
            "method": "/api/session/start",
            "params": {"origin": "fresh", "lifecycle": "persistent",
                       "title": CFG["channels"][self.ch]["session_title"]}})
        return d.get("session_id")

    def session_for(self, cs):
        """(session_id, preamble_text) — preamble_text is "" when the
        session is already briefed, PREAMBLE for a normal new session,
        PREAMBLE_FRESH when the user rotated via /new."""
        rotate = cs["rotate_main"]
        sid = cs["session_id"]
        # LT2 self-heal needs to know whether the coming turn reuses
        # the STORED sid (only then does a session-not-found mean
        # "stored sid died"); a freshly started sid failing is a
        # different failure and must not trigger the heal retry.
        self._session_from_stored = bool(sid) and not rotate
        if sid and not rotate:
            started = (cs["session_started"].get("main", 0) or 0)
            turns_n = cs["session_turns"].get("main", 0)
            if turns_n >= AUTO_ROTATE_TURNS or \
                    (started and time.time() - started > AUTO_ROTATE_AGE):
                self.log(f"auto-rotating session (turns={turns_n})")
                rotate = True
                self._session_from_stored = False
        if rotate or not sid:
            fresh = bool(cs.get("fresh_start")) and bool(sid)
            old_sid = sid if fresh else None
            sid = self.new_session()
            # /new exit audit (P4): at THIS moment — and only here —
            # fingerprint the just-retired OLD session's history into
            # the frozen corpus. The corpus is frozen at rotation:
            # nothing the user says in the NEW session ever enters it,
            # so a legitimate restatement in the new session can only
            # be blocked by a long verbatim fragment of the OLD one.
            # Only hashes are stored; the old text is never persisted.
            # Any non-/new rotation (first session, auto-rotate)
            # clears the binding, so the audit lapses with the fresh
            # session it was bound to.
            fingerprints = []
            if old_sid:
                try:
                    evs = self.history_events(
                        old_sid, limit=AUDIT_HISTORY_LIMIT)
                    fingerprints = build_audit_corpus(evs)
                    self.log(f"audit corpus built old_sid={old_sid} "
                             f"fingerprints={len(fingerprints)}")
                except Exception as e:
                    self.log("audit corpus build failed:",
                             type(e).__name__, str(e)[:100],
                             "; audit corpus empty")
                    fingerprints = []
            st = dict(cs["session_started"])
            st["main"] = int(time.time())
            tn = dict(cs["session_turns"])
            tn["main"] = 0
            self.update_cs(session_id=sid, preamble_done=False,
                           rotate_main=False, fresh_start=False,
                           fresh_session_id=sid if fresh else None,
                           audit_fingerprints=fingerprints,
                           idle_push_seq=0,
                           session_started=st, session_turns=tn)
            return sid, (PREAMBLE_FRESH if fresh else PREAMBLE)
        return sid, ("" if cs["preamble_done"] else PREAMBLE)

    def _invalidate_session(self, why):
        """LT2 self-heal: forget a server-dead stored session id so
        the next start builds a fresh session instead of reusing a
        corpse (and so the idle scan stops hammering it). Clears the
        /new audit binding with it — it referred to the dead sid."""
        self.log(f"session dead -> healing ({why})")
        self.update_cs(session_id=None, preamble_done=False,
                       fresh_session_id=None, audit_fingerprints=[],
                       idle_push_seq=0)

    def history_events(self, sid, limit=40):
        d = self.gw.call_json("chat.history",
                              body={"limit": limit, "session_id": sid})
        return sorted(d.get("chat_events", []), key=lambda e: e.get("seq", 0))

    def start_turn(self, row, cs):
        sid, preamble = self.session_for(cs)
        evs = self.history_events(sid, limit=5)
        baseline = evs[-1].get("seq", 0) if evs else 0
        text = row.get("text", "")
        if preamble:
            text = preamble + text
        self.gw._open("chat.stream", body={
            "items": [{"type": "text", "text": text}], "node_id": NODE_ID,
            "capabilities": {}, "session_id": sid})
        self.update_cs(preamble_done=True)
        turn = {"msgid": row["msgid"], "lane": "main", "session_id": sid,
                "baseline": baseline, "sent_at": int(time.time()),
                "text": row.get("text", ""),
                "from_user": row.get("from_user", ""),
                "chatid": row.get("chatid", ""),
                "chattype": row.get("chattype", ""), "next_prog": 0,
                "ids": [row["msgid"]], "reply_started": False,
                "sess_status": None, "status_completed_seen": False,
                "activities": [], "activity_seen": [],
                "last_activity_poll": 0, "last_activity": None,
                # LT2 trailing-loop state (inert unless longtask_mode):
                # reply_delivered flips on the first delivered reply,
                # cursor_seq is the unified cursor shared with the
                # idle scan, last_reply_at/trailing_since drive the
                # quiet/max close conditions.
                "reply_delivered": False, "cursor_seq": baseline,
                "last_reply_at": 0, "trailing_since": 0,
                "activity_dirty": False, "last_event_prog": 0}
        self.log(f"turn sent msgid={row['msgid']} "
                 f"baseline={baseline}")
        return turn

    def progress_notice(self, turn, age):
        """One unbound progress message for a long-running turn, so a
        multi-minute native task never looks dead from the user's side.
        Live channels only; needs the route captured at divert time."""
        if not live_mode(self.ch):
            return
        excerpt = (turn.get("text") or "")[:24]
        mins = max(1, round(age / 60))
        phase = "已开始生成" if turn.get("reply_started") else "思考中"
        nq = len(self.queue)
        qtxt = f"后面还排着 {nq} 条。" if nq else ""
        if age >= ESCALATE_SECS:
            # A bare "still working" repeated forever reads as a hang and
            # gives the user nothing to act on. Past the escalation age,
            # say it looks stuck and name the remedy.
            text = (f"⚠️ 这条任务已跑约 {mins} 分钟还没结束（{phase}），"
                    f"可能卡住了：「{excerpt}」\n"
                    f"发 /stop 可以终止它，后面的消息会继续处理。{qtxt}")
        else:
            text = (f"⏳ 还在处理中（已跑约 {mins} 分钟，{phase}）："
                    f"「{excerpt}」{qtxt}")
        # Fold the newest activity signals into THIS notice only —
        # never a separate bubble. Neutral wording: activity.list is
        # an account-wide stream, attribution is by time window only.
        self._poll_activity(turn)
        acts = [a.get("text") for a in (turn.get("activities") or [])
                if isinstance(a, dict) and a.get("text")]
        if acts:
            text += "\n最近动态：" + "；".join(acts[-2:])
        rid = f"bridgeprog-{turn['msgid'][:8]}-{int(age // 60)}"
        if self.ch == "weixin":
            to = turn.get("from_user", "")
            if not to:
                return
            out = {"id": rid, "mode": "send", "to_user_id": to,
                   "content": text, "queued_at": int(time.time())}
        elif self.ch == "wecom":
            cid = turn.get("chatid", "")
            if not cid:
                return
            out = {"id": rid, "mode": "send", "chatid": cid,
                   "chat_type": 2 if turn.get("chattype") == "group" else 1,
                   "content": text, "queued_at": int(time.time())}
        else:
            return
        append_jsonl(outbox_path(self.ch), out)
        self.log(f"progress notice msgid={turn['msgid']} age={age}s")

    def _session_status(self, sid):
        """Authoritative turn state from sessions.get (probe9: its
        status field flips running -> completed the moment a reply
        lands). Returns the status string, or None on ANY failure —
        callers must treat None as "no signal" and fall back to the
        pure history logic."""
        try:
            d = self.gw.call_json("sessions.get",
                                  path_params={"id": sid}, timeout=15)
        except Exception as e:
            self.log("sessions.get failed:", type(e).__name__,
                     str(e)[:80])
            return None
        if isinstance(d, dict):
            return d.get("status") or None
        return None

    def _poll_activity(self, turn):
        """Fold new activity.list events into the turn state (P3).
        Low-frequency (ACTIVITY_POLL_SECS), silent on failure, and
        the events are only ever surfaced inside progress_notice /
        the queue snapshot — never as their own messages. Dedup by
        (timestamp, message_id, type); only events at/after the
        turn's sent_at are collected.
        LT2: returns True when at least one NEW event was folded in
        (the event-triggered progress path in step keys off this);
        every early exit returns False."""
        if self.gw is None:
            return False
        now = time.time()
        if now - (turn.get("last_activity_poll") or 0) \
                < ACTIVITY_POLL_SECS:
            return False
        turn["last_activity_poll"] = int(now)
        # The poll timestamp must persist, or the throttle is lost
        # when step() reloads state and activity.list gets hit on
        # every 2s step instead of every ACTIVITY_POLL_SECS.
        self._turns_dirty = True
        try:
            d = self.gw.call_json("activity.list", timeout=15)
        except Exception:
            return False
        if not isinstance(d, dict):
            return False
        seen = {tuple(k) for k in (turn.get("activity_seen") or [])
                if isinstance(k, (list, tuple))}
        acts = list(turn.get("activities") or [])
        changed = False
        for day in d.get("days", []) or []:
            if not isinstance(day, dict):
                continue
            for ev in day.get("activities", []) or []:
                if not isinstance(ev, dict):
                    continue
                ts = _activity_ts_epoch(ev.get("timestamp"))
                if ts is None or ts < (turn.get("sent_at") or 0):
                    continue
                key = (str(ev.get("timestamp") or ""),
                       str(ev.get("message_id") or ""),
                       str(ev.get("type") or ""))
                if key in seen:
                    continue
                seen.add(key)
                turn.setdefault("activity_seen", []).append(list(key))
                summary = activity_summary(ev)
                if summary:
                    acts.append({"ts": key[0], "text": summary})
                    changed = True
        if changed:
            turn["activities"] = acts[-20:]
            turn["last_activity"] = acts[-1]["text"]
            self._turns_dirty = True
        return changed

    def _best_effort_reply(self, turn):
        """Fresh history read for the abnormal-end path: the LAST
        assistant event after baseline carrying any text, whatever
        its status. Returns (ev, text) or (None, "")."""
        try:
            evs = self.history_events(turn["session_id"])
        except Exception:
            return None, ""
        best_ev, best_text = None, ""
        for ev in evs:
            if ev.get("event_name") != "message.assistant" or \
                    (ev.get("seq") or 0) <= turn["baseline"]:
                continue
            text = fmt_event(ev).get("text", "")
            if text.strip():
                best_ev, best_text = ev, text
        return best_ev, best_text

    def poll_turn(self, turn):
        ids = turn.get("ids") or [turn["msgid"]]
        if any(mid in cancelled_ids(self.ch) for mid in ids):
            try:
                self.gw.call_json("chat.cancel",
                                  body={"session_id": turn["session_id"]},
                                  timeout=15)
            except Exception as e:
                self.log("cancel call failed:", str(e)[:100])
            self.log(f"turn cancelled msgid={turn['msgid']}")
            return "cancelled"
        # LT2 gate: with the flag off, everything below is exactly
        # the pre-LT2 single-reply behaviour.
        gated = longtask_mode(self.ch)
        evs = self.history_events(turn["session_id"])
        started = False
        now_t = time.time()
        for ev in evs:
            seq = ev.get("seq") or 0
            if ev.get("event_name") == "message.assistant" and \
                    seq > turn["baseline"]:
                started = True
            if not is_turn_reply(ev, turn["baseline"]):
                continue
            text = fmt_event(ev).get("text", "")
            if gated and turn.get("reply_delivered"):
                # LT2 trailing segment: a further completed message
                # after the first reply. Deliver it as an unbound
                # idle send (its own dedup id), advance the unified
                # cursor, and stay in the trailing window — the
                # Chat.send pattern of printing each reply as it
                # lands and resetting the quiet window.
                if seq <= (turn.get("cursor_seq") or turn["baseline"]):
                    continue
                mid = _ev_mid(ev)
                self._deliver_idle(text, mid or f"seq{seq}")
                self._turn_reply_marks.append((seq, mid))
                turn["cursor_seq"] = seq
                turn["last_reply_at"] = int(now_t)
                self._turns_dirty = True
                self.log(f"trailing reply delivered "
                         f"msgid={turn['msgid']} seq={seq}")
                continue
            # First completed reply of the turn (the only one the
            # pre-LT2 bridge ever delivered).
            self._turn_reply_marks.append((seq, _ev_mid(ev)))
            deliver_reply(self.ch, turn["msgid"], text)
            # Merged-in messages share this one combined answer
            # (the official client shows a single answer); each
            # is closed out gateway-side by _silence_merged. LT2:
            # this runs exactly once, on the first reply only.
            for mid in ids[1:]:
                self._silence_merged(mid)
            self.log(f"reply delivered msgid={turn['msgid']} "
                     f"lane={turn['lane']} "
                     f"secs={int(time.time()) - turn['sent_at']}")
            if not gated:
                return "done"
            # LT2: do NOT close the turn — enter the trailing
            # window and keep scanning this same history batch for
            # further segments before deciding below.
            turn["reply_delivered"] = True
            turn["cursor_seq"] = seq
            turn["last_reply_at"] = int(now_t)
            turn["trailing_since"] = int(now_t)
            self._turns_dirty = True
        if started and not turn.get("reply_started"):
            turn["reply_started"] = True
            self._turns_dirty = True
        if gated and turn.get("reply_delivered"):
            # LT2 trailing window. New activity still marks the
            # turn dirty so step() can fire an event progress
            # notice. Close conditions: the platform says the
            # session turn is completed AND the quiet window since
            # the last reply has elapsed (TRAILING_QUIET_SECS), or
            # the trailing window hits its hard cap
            # (TRAILING_MAX_SECS) — later arrivals then continue
            # through idle_push_scan on the same unified cursor.
            # A running status never closes on silence alone.
            if self._poll_activity(turn):
                turn["activity_dirty"] = True
                self._turns_dirty = True
            status = self._session_status(turn["session_id"])
            if status is not None and status != turn.get("sess_status"):
                turn["sess_status"] = status
                self._turns_dirty = True
            now2 = time.time()
            if now2 - (turn.get("trailing_since") or now2) \
                    >= TRAILING_MAX_SECS:
                self.log(f"trailing window capped msgid={turn['msgid']}")
                return "done"
            if status == "completed" and \
                    now2 - (turn.get("last_reply_at") or now2) \
                    >= TRAILING_QUIET_SECS:
                self.log(f"trailing window closed msgid={turn['msgid']}")
                return "done"
            return "running"
        # No completed reply delivered (yet). Cross-check the
        # platform's own turn state (sessions.get, probe9) —
        # conservatively: a single "completed" observation never
        # ends the turn (it gets one more poll to let a lagging
        # history catch up); only a SECOND consecutive completed
        # with history still empty is treated as an abnormal end
        # (empty/failed reply), closed out after a best-effort
        # delivery of any assistant text. A failed status query
        # changes nothing at all. LT2: this abnormal-end logic
        # applies only while reply_delivered is False — a turn in
        # its trailing window never reaches here.
        status = self._session_status(turn["session_id"])
        if status is not None and status != turn.get("sess_status"):
            turn["sess_status"] = status
            self._turns_dirty = True
        if status == "completed":
            if turn.get("status_completed_seen"):
                ev, text = self._best_effort_reply(turn)
                if text.strip():
                    if ev is not None:
                        self._turn_reply_marks.append(
                            (ev.get("seq") or 0, _ev_mid(ev)))
                    deliver_reply(self.ch, turn["msgid"], text)
                    for mid in ids[1:]:
                        self._silence_merged(mid)
                self.log(f"turn ended abnormally msgid={turn['msgid']}: "
                         f"sessions.get=completed twice, no completed "
                         f"reply in history; best-effort text "
                         f"{'delivered' if text.strip() else 'none'}")
                return "done"
            turn["status_completed_seen"] = True
            self._turns_dirty = True
        elif status == "running" and turn.get("status_completed_seen"):
            turn["status_completed_seen"] = False
            self._turns_dirty = True
        if self._poll_activity(turn) and gated:
            # LT2 spec 4: fresh activity is a progress event.
            turn["activity_dirty"] = True
            self._turns_dirty = True
        return "running"

    def _deliver_idle(self, text, key):
        """Push one session-originated assistant message the user never
        asked for in a bridge turn (a background-task result, a browser
        result, a proactive report). Such messages used to sit in the
        session history forever: poll_turn only delivers replies to
        turns the bridge itself opened, so a result written during an
        idle gap never reached the channel (lost twice on 2026-10-07:
        00:36 browser verdict, 01:31 browser-install verdict). Sent as
        unbound sends, mirroring progress_notice addressing; [[FILE:]]
        markers are honoured the same way as in turn replies."""
        files, kept = [], []
        for line in text.splitlines():
            s = line.strip()
            if s.startswith("[[FILE:") and s.endswith("]]"):
                p = s[len("[[FILE:"):-2].strip()
                if os.path.isfile(p):
                    files.append(p)
                    continue
            kept.append(line)
        body = "\n".join(kept).strip()
        rid = "bridgeidle-" + hashlib.sha256(
            f"idle:{self.ch}:{key}".encode()).hexdigest()[:12]
        if outbox_already_has(self.ch, rid):
            return
        cfg = CFG["channels"][self.ch]
        now = int(time.time())
        rows = []
        if body:
            r = {"id": rid, "mode": "send", "content": body,
                 "queued_at": now}
            if self.ch == "weixin":
                r["to_user_id"] = cfg.get("from_user_id", "")
            elif self.ch == "wecom":
                r["chatid"] = cfg.get("chatid", "")
                r["chat_type"] = 1
            else:
                r["text"] = body
            rows.append(r)
        for i, p in enumerate(files):
            r = {"id": f"{rid}-f{i}", "mode": "send_file",
                 "file_path": p, "content": body if i == 0 else "",
                 "queued_at": now}
            if self.ch == "weixin":
                r["to_user_id"] = cfg.get("from_user_id", "")
            elif self.ch == "wecom":
                r["chatid"] = cfg.get("chatid", "")
                r["chat_type"] = 1
            rows.append(r)
        for r in rows:
            append_jsonl(outbox_path(self.ch), r)
        if rows:
            self.log(f"idle push key={str(key)[:12]} rows={len(rows)}")

    def _note_idle_scan_failure(self, cs, exc):
        """LT2 (gated): bookkeep one idle-scan failure. A dead
        session used to spam "idle scan failed: session not found"
        into the journal every ~20s forever, unnoticed; other
        failures were equally silent. Session-not-found invalidates
        the stored sid (the scan then skips itself, since sid is
        None) and does NOT count towards the streak; other errors
        accumulate idle_scan_fail_streak, and at >= threshold (with
        an hourly cap) raise ONE visible alert through the channel
        outbox. Returns nothing; callers just drop the connection."""
        err = f"{type(exc).__name__} {str(exc)[:100]}"
        if is_session_not_found(exc):
            self._invalidate_session(f"idle scan: {err}")
            return
        streak = (cs.get("idle_scan_fail_streak") or 0) + 1
        kw = {"idle_scan_fail_streak": streak}
        alert = streak >= IDLE_FAIL_ALERT_STREAK and \
            time.time() - (cs.get("idle_scan_last_alert") or 0) \
            >= IDLE_ALERT_MIN_SECS
        if alert:
            kw["idle_scan_last_alert"] = int(time.time())
        self.update_cs(**kw)
        if alert:
            self._send_alert_text(
                "⚠️ 原生桥空闲扫描连续失败 "
                f"{streak} 次，主动推送可能受影响；"
                "我会继续重试，你不用管。")
            self.log(f"idle scan alert sent streak={streak}")

    def idle_push_scan(self, cs):
        """While no bridge turn is open, watch the channel's own
        session for NEW completed assistant messages and push them to
        the channel. Dedup is two-layered: a persisted seq watermark
        plus the message ids already delivered (turn replies are
        marked in poll_turn and merged by step). The first scan of a
        session only baselines the watermark — history is never
        retro-pushed. Read-only polling reuses/renews a quiet
        connection; sends still always go out on fresh turn-scoped
        connections via _start_next.
        LT2 (gated): failures go through _note_idle_scan_failure —
        a session-not-found invalidates the dead sid so the scan
        stops, other failures streak towards a visible alert, and a
        successful scan clears the streak."""
        now = time.time()
        if now - self.last_idle_scan < IDLE_PUSH_EVERY_SECS:
            return
        self.last_idle_scan = now
        sid = cs.get("session_id")
        if not sid:
            return
        gated = longtask_mode(self.ch)
        if self.gw is None:
            try:
                self.gw = connect()
            except Exception as e:
                self.log("idle scan connect failed:",
                         type(e).__name__, str(e)[:100])
                if gated:
                    self._note_idle_scan_failure(cs, e)
                return
        try:
            evs = self.history_events(sid, limit=40)
        except Exception as e:
            self.log("idle scan failed:", type(e).__name__,
                     str(e)[:100])
            self.drop_gw()
            if gated:
                self._note_idle_scan_failure(cs, e)
            return
        if gated and cs.get("idle_scan_fail_streak"):
            self.update_cs(idle_scan_fail_streak=0)
        if not evs:
            return
        wm = cs.get("idle_push_seq") or 0
        maxseq = evs[-1].get("seq") or 0
        if wm == 0:
            self.update_cs(idle_push_seq=maxseq)
            return
        pushed = list(cs.get("idle_pushed_ids") or [])
        idset = set(pushed)
        new_wm = wm
        changed = False
        for ev in evs:
            seq = ev.get("seq") or 0
            if seq > new_wm:
                new_wm = seq
            if seq <= wm or not is_turn_reply(ev, wm):
                continue
            mid = _ev_mid(ev)
            if mid and mid in idset:
                continue
            text = fmt_event(ev).get("text", "")
            if not text.strip():
                continue
            self._deliver_idle(text, mid or f"seq{seq}")
            if mid:
                idset.add(mid)
                pushed.append(mid)
                changed = True
        if new_wm != wm:
            changed = True
        if changed:
            self.update_cs(idle_push_seq=new_wm,
                           idle_pushed_ids=pushed[-200:])

    def run(self):
        ch = self.ch
        cfg = CFG["channels"][ch]
        spool = os.path.join(BASE, "spool", f"{ch}.jsonl")
        open(spool, "a").close()
        # Baseline the /new boundary at startup: boundaries already on
        # disk predate this process and must not rotate; later ones do.
        cs = self.cs()
        if cs["last_boundary_ts"] is None:
            init = read_signal_ts(cfg.get("boundary_file"))
            self.update_cs(last_boundary_ts=init if init is not None else 0)
        while True:
            try:
                self.step(spool, cfg.get("boundary_file"))
            except Exception:
                traceback.print_exc()
            try:
                cur = json.load(open(STATUS_F))
                if time.time() - (cur.get("ts") or 0) > 10:
                    write_status({})
            except Exception:
                pass
            time.sleep(POLL_SECS)

    def step(self, spool, boundary_f):
        ch = self.ch
        cs = self.cs()
        bts = read_signal_ts(boundary_f)
        if bts is not None and cs["last_boundary_ts"] is not None \
                and bts > cs["last_boundary_ts"]:
            # The rotate flags are PERSISTED with the seen-marker: an
            # earlier version kept them in memory only, so any bridge
            # restart between the user's /new and their next message
            # silently ate the rotation (the ts had already advanced).
            self.update_cs(last_boundary_ts=bts, rotate_main=True,
                           fresh_start=True)
            self.log("topic boundary seen; session rotates on next turn")
        if self.read_offset is None:
            self.read_offset = cs["spool_offset"]
        with open(spool, "rb") as f:
            f.seek(self.read_offset)
            raw_lines = f.readlines()
            self.read_offset = f.tell()
        for raw in raw_lines:
            s = raw.strip()
            if not s:
                continue
            try:
                row = json.loads(s)
            except ValueError:
                self.log("bad spool line skipped")
                continue
            self.queue.append((row, len(raw)))
        turns = list(cs["turns"])
        if turns and self.ensure_gw():
            alive = []
            prog_dirty = False
            for turn in turns:
                try:
                    outcome = self.poll_turn(turn)
                except Exception as e:
                    self.log(f"poll failed msgid={turn['msgid']}: "
                             f"{str(e)[:100]}; will retry")
                    self.drop_gw()
                    alive.append(turn)
                    continue
                if outcome == "running":
                    age = int(time.time()) - turn["sent_at"]
                    nxt = turn.get("next_prog") or 0
                    # LT2 spec 4 (gated): fresh activity is a
                    # progress EVENT — fire the notice now (throttled
                    # to one per LT_EVENT_PROG_MIN_SECS) instead of
                    # waiting for the 120/300 timer, and push the
                    # timed notice out so the two never stack.
                    fired = False
                    if longtask_mode(ch) and turn.get("activity_dirty") \
                            and time.time() - \
                            (turn.get("last_event_prog") or 0) \
                            >= LT_EVENT_PROG_MIN_SECS:
                        turn["activity_dirty"] = False
                        turn["last_event_prog"] = int(time.time())
                        turn["next_prog"] = int(time.time()) \
                            + PROG_EVERY_SECS
                        prog_dirty = True
                        self.progress_notice(turn, age)
                        fired = True
                    if not fired and age >= PROG_FIRST_SECS \
                            and time.time() >= nxt:
                        turn["next_prog"] = int(time.time()) \
                            + PROG_EVERY_SECS
                        prog_dirty = True
                        self.progress_notice(turn, age)
                    alive.append(turn)
                elif outcome == "done":
                    cs2 = self.cs()
                    tn = dict(cs2["session_turns"])
                    tn[turn["lane"]] = tn.get(turn["lane"], 0) + 1
                    self.update_cs(processed=cs2["processed"] + 1,
                                   session_turns=tn, fallback_streak=0)
                elif outcome == "cancelled":
                    self.update_cs(cancels=self.cs()["cancels"] + 1)
            if prog_dirty or self._turns_dirty or \
                    [t["msgid"] for t in alive] != [t["msgid"] for t in turns]:
                self._turns_dirty = False
                self.update_cs(turns=alive)
            if not alive:
                # Turn over — close its connection. The next turn
                # connects fresh; nothing idle is left behind to die.
                self.drop_gw()
        if self._turn_reply_marks:
            # Replies just delivered as turn answers must never be
            # re-pushed by the idle scanner: record their ids and lift
            # the watermark past them.
            marks, self._turn_reply_marks = self._turn_reply_marks, []
            cs_m = self.cs()
            ids = list(cs_m.get("idle_pushed_ids") or [])
            for _seq, mid in marks:
                if mid and mid not in ids:
                    ids.append(mid)
            self.update_cs(
                idle_push_seq=max(cs_m.get("idle_push_seq") or 0,
                                  max(s for s, _m in marks)),
                idle_pushed_ids=ids[-200:])
        # 4b) queue admin from the gateways (/queue clear, /queue drop 桥N):
        # snapshot msgids are removed from the queue; running turns and
        # already-started rows are untouched, same as the hook semantics.
        admin_f = os.path.join(BASE, f"queue_admin-{ch}.jsonl")
        if os.path.exists(admin_f):
            cs = self.cs()
            with open(admin_f, "rb") as f:
                f.seek(cs["admin_offset"])
                admin_lines = f.readlines()
                new_admin_off = f.tell()
            if admin_lines:
                drop_ids = set()
                for raw in admin_lines:
                    try:
                        rec = json.loads(raw)
                        drop_ids.update(rec.get("msgids", []))
                    except ValueError:
                        continue
                if drop_ids and self.queue:
                    keep = []
                    for row, nbytes in self.queue:
                        if row.get("msgid") in drop_ids:
                            self.log(f"queue admin drop msgid={row['msgid']}")
                            cur = self.cs()
                            self.update_cs(
                                spool_offset=cur["spool_offset"] + nbytes)
                            continue
                        keep.append((row, nbytes))
                    self.queue = keep
                self.update_cs(admin_offset=new_admin_off)
        if self.queue:
            cids = cancelled_ids(ch)
            keep = []
            for row, nbytes in self.queue:
                if row.get("msgid") in cids:
                    self.log(f"skip cancelled queued msgid={row['msgid']}")
                    cur = self.cs()
                    self.update_cs(cancels=cur["cancels"] + 1,
                                   spool_offset=cur["spool_offset"] + nbytes)
                    continue
                keep.append((row, nbytes))
            self.queue = keep
        cs = self.cs()
        if self.queue and cs["turns"] and self.ch in MERGE_CHANNELS:
            self._merge_into_active()
            cs = self.cs()
        if self.queue and not cs["turns"]:
            self._start_next()
        cs = self.cs()
        if not cs["turns"] and not self.queue:
            self.idle_push_scan(cs)
        cs = self.cs()
        snap = {
            "active": [{"msgid": mid, "lane": t["lane"],
                        "secs": int(time.time()) - t["sent_at"],
                        "phase": "generating" if t.get("reply_started")
                                 else "thinking",
                        "last_activity": t.get("last_activity"),
                        **({"merged": True} if i else {})}
                       for t in cs["turns"]
                       for i, mid in enumerate(t.get("ids") or [t["msgid"]])],
            "queued": [r.get("msgid") for r, _n in self.queue],
            "updated": int(time.time())}
        if snap["active"] != cs["queue_snapshot"].get("active") or \
                snap["queued"] != cs["queue_snapshot"].get("queued"):
            self.update_cs(queue_snapshot=snap)

    def note_fallback(self, row, nbytes):
        """Bookkeep one pre-send fallback. A bridge whose credentials
        died would otherwise degrade to the slow cold path SILENTLY,
        forever — so a streak of >=3 fallbacks raises ONE user-visible
        alert per hour through the channel's own outbox."""
        cs = self.cs()
        streak = cs.get("fallback_streak", 0) + 1
        kw = {"fallbacks": cs["fallbacks"] + 1,
              "spool_offset": cs["spool_offset"] + nbytes,
              "fallback_streak": streak}
        alert = streak >= 3 and \
            time.time() - cs.get("last_fb_alert", 0) >= 3600
        if alert:
            kw["last_fb_alert"] = int(time.time())
        self.update_cs(**kw)
        if alert:
            self._send_fb_alert(row, streak)

    def _send_alert_text(self, text, row=None):
        """Write one visible alert row into this channel's outbox
        (live addressing when live, the shadow outbox otherwise).
        Generalized from _send_fb_alert's write pattern so LT2's
        idle-scan alert reuses exactly the same delivery path —
        including the gateway's parked-retry handling downstream —
        instead of inventing a second one. row, when given, supplies
        the live route; otherwise the channel config does."""
        rid = f"bridgealert-{int(time.time())}"
        try:
            if live_mode(self.ch):
                out = {"id": rid, "mode": "send", "text": text}
                if self.ch == "wecom":
                    cid = (row or {}).get("chatid") or \
                        CFG["channels"][self.ch].get("chatid", "")
                    if not cid:
                        return
                    out["chatid"] = cid
                    out["chattype"] = (row or {}).get("chattype",
                                                      "single")
                else:
                    to = (row or {}).get("from_user") or \
                        CFG["channels"][self.ch].get("from_user_id", "")
                    if not to:
                        return
                    out["to_user_id"] = to
                append_jsonl(os.path.join(
                    CFG["channels"][self.ch]["bot_state"], "outbox.jsonl"),
                    out)
            else:
                append_jsonl(os.path.join(BASE, "shadow",
                                          f"{self.ch}-outbox.jsonl"),
                             {"id": rid, "mode": "send", "text": text})
        except Exception as e:
            self.log(f"alert send failed: {str(e)[:80]}")

    def _send_fb_alert(self, row, streak):
        text = ("⚠️ 原生通道暂时连不上（已连续失败 "
                f"{streak} 次），消息改走备用通道处理，回复会慢一些；"
                "我会自动重连，你不用管。")
        self._send_alert_text(text, row)
        self.log(f"fallback alert sent streak={streak}")

    def _silence_merged(self, mid):
        """Close a merged message out on the gateway side.
        FEEDBACK_CLEAR_CHANNELS: append its msgid to the channel's
        feedback_clear.jsonl — the same side channel the hooks use
        for messages resolved outside the reply path, consumed by
        the gateway's feedback scan — so it gets no bubble at all;
        if that write fails, fall back to the pointer reply.
        Other channels (wecom): the gateway consumes no such file
        and the message's think stream needs a bound reply to
        finish in place, so send the short pointer reply directly."""
        if self.ch in FEEDBACK_CLEAR_CHANNELS:
            try:
                append_jsonl(os.path.join(
                    CFG["channels"][self.ch]["bot_state"],
                    "feedback_clear.jsonl"), {"msgid": mid})
                return
            except Exception as e:
                self.log("feedback_clear failed:", str(e)[:80],
                         "; pointer reply instead")
        deliver_reply(self.ch, mid, MERGE_POINTER_TEXT)

    def _merge_into_active(self):
        """Fold every queued row into the running turn (MERGE_CHANNELS
        only). The official client steers a running turn with new
        messages, and the protocol allows it: a second chat.stream on
        the same session appends the user message to the SAME turn and
        one combined reply follows (probed live 2026-10-08, session
        204287b5: msg2 landed as a user event 2s after sending, before
        any assistant event). Only while the turn's reply has not
        started streaming — reply_started is refreshed by poll_turn
        every step — after that, rows keep waiting FIFO. A failed
        merge send leaves the row queued (plain FIFO for it)."""
        cs = self.cs()
        if not cs["turns"]:
            return
        turn = cs["turns"][0]
        if turn.get("reply_started"):
            return
        if not self.ensure_gw():
            return
        ids = list(turn.get("ids") or [turn["msgid"]])
        merged = False
        while self.queue:
            row, nbytes = self.queue[0]
            try:
                self.gw._open("chat.stream", body={
                    "items": [{"type": "text",
                               "text": row.get("text", "")}],
                    "node_id": NODE_ID, "capabilities": {},
                    "session_id": turn["session_id"]})
            except Exception as e:
                self.log(f"merge failed msgid={row.get('msgid')}: "
                         f"{type(e).__name__} {str(e)[:100]}; stays queued")
                break
            self.queue.pop(0)
            ids.append(row["msgid"])
            cur = self.cs()
            self.update_cs(spool_offset=cur["spool_offset"] + nbytes)
            self.log(f"merged msgid={row['msgid']} into turn "
                     f"msgid={turn['msgid']}")
            merged = True
        if merged:
            turn["ids"] = ids
            self.update_cs(turns=[turn] + list(cs["turns"][1:]))

    def _start_next(self):
        row, nbytes = self.queue[0]
        # Every turn starts on a FRESH connection (see drop_gw): the
        # history read inside start_turn doubles as the liveness proof
        # of a connection seconds old, so a turn can no longer discover
        # a long-dead idle connection at send time — the failure mode
        # behind the cold-path fallbacks. Cookie/token auth makes the
        # ~2-3s reconnect cheap; that is the whole cost of the fix.
        # LT2 (gated): a pre-send session-not-found against the
        # STORED sid means that sid died server-side; invalidate it
        # and retry THIS row exactly once on a fresh session before
        # considering the fallback path — never a duplicate send,
        # because the failed attempt never opened chat.stream.
        healed = False
        while True:
            self.drop_gw()
            if not self.ensure_gw():
                fallback_to_inbox(self.ch, row, "connect_or_auth_failed")
                self.queue.pop(0)
                self.note_fallback(row, nbytes)
                return
            try:
                turn = self.start_turn(row, self.cs())
            except Exception as e:
                self.log(f"pre-send failure msgid={row.get('msgid')}: "
                         f"{type(e).__name__} {str(e)[:120]}")
                if not healed and longtask_mode(self.ch) \
                        and is_session_not_found(e) \
                        and self._session_from_stored:
                    healed = True
                    self._invalidate_session(
                        f"pre-send msgid={row.get('msgid')}")
                    self.drop_gw()
                    continue
                fallback_to_inbox(self.ch, row,
                                  f"presend_{type(e).__name__}")
                self.queue.pop(0)
                self.note_fallback(row, nbytes)
                self.drop_gw()
                return
            break
        self.queue.pop(0)
        cs = self.cs()
        turns = list(cs["turns"]) + [turn]
        self.update_cs(turns=turns,
                       spool_offset=cs["spool_offset"] + nbytes)


def cmd_daemon():
    write_status({"pid": os.getpid(), "started": int(time.time()),
                  "mode": {ch: ("live" if live_mode(ch) else "shadow")
                           for ch in CFG["channels"]}})
    workers = [ChannelWorker(ch) for ch in CFG["channels"]]
    for w in workers:
        w.start()
    print("native-bridge daemon up", flush=True)
    while True:
        time.sleep(3600)


def cmd_enqueue(argv):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--msgid", required=True)
    ap.add_argument("--text", required=True)
    a = ap.parse_args(argv)
    append_jsonl(os.path.join(BASE, "spool", f"{a.channel}.jsonl"),
                 {"msgid": a.msgid, "text": a.text, "ts": int(time.time())})
    print("enqueued", a.channel, a.msgid)


def cmd_test_turn(argv):
    text = argv[0] if argv else "1+1等于几？只回复数字"
    gw = connect()
    d = gw.call_json("session.start", body={
        "method": "/api/session/start",
        "params": {"origin": "fresh", "lifecycle": "persistent",
                   "title": "native-bridge-test"}})
    sid = d.get("session_id")
    t0 = time.time()
    gw._open("chat.stream", body={
        "items": [{"type": "text", "text": text}], "node_id": NODE_ID,
        "capabilities": {}, "session_id": sid})
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(1)
        evs = sorted(gw.call_json("chat.history", body={
            "limit": 40, "session_id": sid}).get("chat_events", []),
            key=lambda e: e.get("seq", 0))
        for ev in evs:
            if is_turn_reply(ev, 0):
                print(f"reply in {time.time() - t0:.1f}s: "
                      f"{fmt_event(ev).get('text', '')!r}")
                return
    print("no reply within 300s")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "daemon"
    if cmd == "daemon":
        cmd_daemon()
    elif cmd == "enqueue":
        cmd_enqueue(sys.argv[2:])
    elif cmd == "test-turn":
        cmd_test_turn(sys.argv[2:])
    elif cmd == "status":
        try:
            print(open(STATUS_F).read())
        except OSError:
            print("no status yet")
        print(open(STATE_F).read() if os.path.exists(STATE_F) else "{}")
    else:
        print("usage: native_bridge.py [daemon|enqueue|test-turn|status]")
        sys.exit(2)
