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
IDLE_PUSH_EVERY_SECS = 20   # idle-session scan cadence (see idle_push_scan)
MERGE_CHANNELS = {"weixin", "test"}
# Channels in MERGE_CHANNELS fold a queued message into the running
# turn (official-client steering) while that turn's reply has not
# started; other channels keep strict FIFO. Merged msgids get this
# pointer as their bound reply; the combined answer itself is bound
# to the turn's first msgid.
MERGE_POINTER_TEXT = "（已并入上一条处理）"
AUTO_ROTATE_TURNS = 80
AUTO_ROTATE_AGE = 7 * 86400
NODE_ID = "native-bridge"

PREAMBLE = (
    "【渠道桥接说明】你正在通过桥接程序回复手机渠道（微信/企业微信）上的用户。"
    "你的最终回复文本会被原样转发到该渠道，所以：结论先行、简洁自然；"
    "如果生成了要发给用户的文件，在回复末尾单独一行写 [[FILE:文件的绝对路径]]；"
    "不要提及桥接机制。\n\n用户消息："
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
        "rotate_main": False,
        "session_started": {"main": 0},
        "session_turns": {"main": 0},
        "admin_offset": 0,
        "idle_push_seq": 0, "idle_pushed_ids": [],
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


def deliver_reply(ch, msgid, text):
    """Split [[FILE:path]] markers; write formal reply (+ reply_file rows)."""
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
        if sid and not rotate:
            started = (cs["session_started"].get("main", 0) or 0)
            turns_n = cs["session_turns"].get("main", 0)
            if turns_n >= AUTO_ROTATE_TURNS or \
                    (started and time.time() - started > AUTO_ROTATE_AGE):
                self.log(f"auto-rotating session (turns={turns_n})")
                rotate = True
        if rotate or not sid:
            fresh = bool(cs.get("fresh_start")) and bool(sid)
            sid = self.new_session()
            st = dict(cs["session_started"])
            st["main"] = int(time.time())
            tn = dict(cs["session_turns"])
            tn["main"] = 0
            self.update_cs(session_id=sid, preamble_done=False,
                           rotate_main=False, fresh_start=False,
                           idle_push_seq=0,
                           session_started=st, session_turns=tn)
            return sid, (PREAMBLE_FRESH if fresh else PREAMBLE)
        return sid, ("" if cs["preamble_done"] else PREAMBLE)

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
                "ids": [row["msgid"]], "reply_started": False}
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
        if age >= ESCALATE_SECS:
            # A bare "still working" repeated forever reads as a hang and
            # gives the user nothing to act on. Past the escalation age,
            # say it looks stuck and name the remedy.
            text = (f"⚠️ 这条任务已跑约 {mins} 分钟还没结束，可能卡住了："
                    f"「{excerpt}」\n发 /stop 可以终止它，后面的消息会继续处理。")
        else:
            text = f"⏳ 还在处理中（已跑约 {mins} 分钟）：「{excerpt}」"
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
        evs = self.history_events(turn["session_id"])
        started = False
        for ev in evs:
            if ev.get("event_name") == "message.assistant" and \
                    (ev.get("seq") or 0) > turn["baseline"]:
                started = True
            if is_turn_reply(ev, turn["baseline"]):
                self._turn_reply_marks.append(
                    (ev.get("seq") or 0, _ev_mid(ev)))
                reply = fmt_event(ev)
                text = reply.get("text", "")
                deliver_reply(self.ch, turn["msgid"], text)
                # Merged-in messages share this one combined answer
                # and get NO bubble of their own (the official client
                # shows a single answer). Their gateway feedback
                # records are closed silently via feedback_clear.
                for mid in ids[1:]:
                    self._silence_merged(mid)
                self.log(f"reply delivered msgid={turn['msgid']} "
                         f"lane={turn['lane']} "
                         f"secs={int(time.time()) - turn['sent_at']}")
                return "done"
        if started and not turn.get("reply_started"):
            turn["reply_started"] = True
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

    def idle_push_scan(self, cs):
        """While no bridge turn is open, watch the channel's own
        session for NEW completed assistant messages and push them to
        the channel. Dedup is two-layered: a persisted seq watermark
        plus the message ids already delivered (turn replies are
        marked in poll_turn and merged by step). The first scan of a
        session only baselines the watermark — history is never
        retro-pushed. Read-only polling reuses/renews a quiet
        connection; sends still always go out on fresh turn-scoped
        connections via _start_next."""
        now = time.time()
        if now - self.last_idle_scan < IDLE_PUSH_EVERY_SECS:
            return
        self.last_idle_scan = now
        sid = cs.get("session_id")
        if not sid:
            return
        if self.gw is None:
            try:
                self.gw = connect()
            except Exception as e:
                self.log("idle scan connect failed:",
                         type(e).__name__, str(e)[:100])
                return
        try:
            evs = self.history_events(sid, limit=40)
        except Exception as e:
            self.log("idle scan failed:", type(e).__name__,
                     str(e)[:100])
            self.drop_gw()
            return
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
                    if age >= PROG_FIRST_SECS and time.time() >= nxt:
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

    def _send_fb_alert(self, row, streak):
        text = ("⚠️ 原生通道暂时连不上（已连续失败 "
                f"{streak} 次），消息改走备用通道处理，回复会慢一些；"
                "我会自动重连，你不用管。")
        rid = f"bridgealert-{int(time.time())}"
        try:
            if live_mode(self.ch):
                out = {"id": rid, "mode": "send", "text": text}
                if self.ch == "wecom":
                    if not row.get("chatid"):
                        return
                    out["chatid"] = row["chatid"]
                    out["chattype"] = row.get("chattype", "single")
                else:
                    if not row.get("from_user"):
                        return
                    out["to_user_id"] = row["from_user"]
                append_jsonl(os.path.join(
                    CFG["channels"][self.ch]["bot_state"], "outbox.jsonl"),
                    out)
            else:
                append_jsonl(os.path.join(BASE, "shadow",
                                          f"{self.ch}-outbox.jsonl"),
                             {"id": rid, "mode": "send", "text": text})
            self.log(f"fallback alert sent streak={streak}")
        except Exception as e:
            self.log(f"fallback alert failed: {str(e)[:80]}")

    def _silence_merged(self, mid):
        """Close a merged message's gateway feedback record without
        sending it any bubble: append its msgid to the channel's
        feedback_clear.jsonl — the same side channel the hooks use
        for messages resolved outside the reply path, consumed by
        the gateway's feedback scan. If that write fails, fall back
        to a short bound pointer reply so the record still closes."""
        try:
            append_jsonl(os.path.join(
                CFG["channels"][self.ch]["bot_state"],
                "feedback_clear.jsonl"), {"msgid": mid})
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
            fallback_to_inbox(self.ch, row, f"presend_{type(e).__name__}")
            self.queue.pop(0)
            self.note_fallback(row, nbytes)
            self.drop_gw()
            return
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
