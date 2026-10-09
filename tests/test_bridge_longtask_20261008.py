#!/usr/bin/env python3
"""LT2 long-task trailing loop + session self-heal (2026-10-08) — sandbox tests.

Covers the LT2 changes to native_bridge.py (all gated by a per-channel
longtask-<channel> flag file; reference semantics are muse-cli
chat.py's Chat.send resident loop):
  A. Trailing loop: three reply segments in one turn are delivered
     one by one (first as the bound reply, the rest as unbound idle
     sends), the unified cursor advances, and the turn closes only
     after status=completed + TRAILING_QUIET_SECS of silence.
     Re-delivery of an already-pushed segment is deduped. Closing
     consults outbox_results for the bound reply (ok / pending).
  B. There is no wall-clock cap. A turn that has been trailing for
     much longer than the old 600s cap stays open while the last
     reply is fresh. It closes only on completed + quiet. After
     that close, idle_push_scan continues on the SAME cursor and
     pushes a later arrival exactly once.
  C. A new activity event triggers an immediate progress_notice via
     step() (even before PROG_FIRST_SECS) carrying the 最近动态 line;
     with no new event and age < 120s, no notice is sent.
  D. _start_next on a stored sid that 404s (session not found):
     the sid is invalidated, the same row is retried once on a fresh
     session, and no fallback is booked.
  E. idle_push_scan on a 404: the stored sid is invalidated and later
     scans are silent; ordinary scan failures streak, and the 3rd
     raises exactly ONE visible alert (hourly cap).
  F. Gate off (wecom carries no longtask flag in the sandbox):
     poll_turn keeps the pre-LT2 single-reply behaviour.

The real bridge module is imported with channel bot_state, STATE_F,
STATUS_F and BASE redirected into a sandbox (flag files live there,
so no production flag/state is touched), and GW replaced by a stub.
Run: ~/muse-test-venv/bin/python tests/test_bridge_longtask_20261008.py
"""
import copy
import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-longtask-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
TS = SBX / "test_state"
for d in (WX, WC, TS, SBX / "spool", SBX / "shadow"):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
for flag in ("enabled-weixin", "longtask-weixin", "longtask-test"):
    (SBX / flag).touch()

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.CFG["channels"]["test"]["bot_state"] = str(TS)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")
nb.BASE = str(SBX)
GatewayError = nb.GatewayError

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def replies_for(msgid, state_dir=WX):
    return [r for r in read_jsonl(state_dir / "outbox.jsonl")
            if r.get("mode") == "reply" and r.get("msgid") == msgid]


def sends_with(text, path=None):
    rows = read_jsonl(path or (WX / "outbox.jsonl"))
    return [r for r in rows if r.get("mode") == "send"
            and r.get("content") == text]


class StubGW:
    def __init__(self):
        self.history = []
        self.status = "running"
        self.activity = {"days": []}
        self.dead_sids = set()
        self.fail_exc = None
        self.new_sid = "new-sid"
        self.opened = []
        self.history_calls = 0

    def close(self):
        pass

    def _open(self, method, body=None):
        self.opened.append((method, body))
        return "stream-1"

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "chat.history":
            self.history_calls += 1
            sid = (body or {}).get("session_id")
            if sid in self.dead_sids:
                raise GatewayError(404, '{"ok":false,"error":'
                                        '{"message":"session not found"}}')
            if self.fail_exc:
                raise self.fail_exc
            return {"chat_events": list(self.history)}
        if method == "sessions.get":
            return {"session_id": (path_params or {}).get("id"),
                    "status": self.status}
        if method == "activity.list":
            return self.activity
        if method == "session.start":
            return {"session_id": self.new_sid}
        if method == "chat.cancel":
            return {}
        raise AssertionError(f"unexpected call {method}")


def asst(seq, text, status="completed"):
    p = {"display_text": text, "message_id": f"am{seq}"}
    if status is not None:
        p["status"] = status
    return {"event_name": "message.assistant", "seq": seq, "payload": p}


def make_turn(**over):
    t = {"msgid": "m-base", "lane": "main", "session_id": "sid1",
         "baseline": 10, "sent_at": int(time.time()),
         "text": "帮我做件事", "from_user": "u1", "chatid": "",
         "chattype": "", "next_prog": 0, "ids": ["m-base"],
         "reply_started": False, "sess_status": None,
         "status_completed_seen": False, "activities": [],
         "activity_seen": [], "last_activity_poll": 0,
         "last_activity": None, "reply_delivered": False,
         "cursor_seq": 10, "last_reply_at": 0, "trailing_since": 0,
         "activity_dirty": False, "last_event_prog": 0}
    t.update(over)
    t["ids"] = [t["msgid"]]
    return t


def worker_with(ch, stub):
    w = nb.ChannelWorker(ch)
    w.gw = stub
    return w


def set_state(ch, **kw):
    st = nb.load_state()
    c = nb.ch_state(st, ch)
    c.update(kw)
    nb.save_state(st)
    return c


# ---------------------------------------------------------------- 0
check("0 is_session_not_found: GatewayError 404",
      nb.is_session_not_found(GatewayError(404, "whatever")))
check("0 is_session_not_found: message text",
      nb.is_session_not_found(RuntimeError("Session Not Found")))
check("0 is_session_not_found: other errors excluded",
      not nb.is_session_not_found(RuntimeError("boom")))
check("0 longtask gate: weixin/test on, wecom off",
      nb.longtask_mode("weixin") and nb.longtask_mode("test")
      and not nb.longtask_mode("wecom"))
check("0 quiet window aligned to muse-cli 10s",
      nb.TRAILING_QUIET_SECS == 10)
check("0 no trailing hard cap", not hasattr(nb, "TRAILING_MAX_SECS"))
check("0 activity poll inside 10-15s",
      10 <= nb.ACTIVITY_POLL_SECS <= 15)
st0 = {"channels": {}}
c0 = nb.ch_state(st0, "weixin")
check("0 ch_state defaults carry idle-scan fields",
      c0.get("idle_scan_fail_streak") == 0
      and c0.get("idle_scan_last_alert") == 0)

# ---------------------------------------------------------------- A
stub = StubGW()
stub.status = "completed"
stub.history = [asst(11, "第一段")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-A")
check("A first segment -> still running (trailing window open)",
      w.poll_turn(turn) == "running")
check("A first segment delivered as bound reply",
      [r["content"] for r in replies_for("m-A")] == ["第一段"])
check("A reply_delivered set, cursor at 11",
      turn["reply_delivered"] and turn["cursor_seq"] == 11)
stub.history += [asst(12, "第二段"), asst(13, "第三段")]
check("A later segments -> still running inside quiet window",
      w.poll_turn(turn) == "running")
check("A segment 2 pushed as unbound send exactly once",
      len(sends_with("第二段")) == 1)
check("A segment 3 pushed as unbound send exactly once",
      len(sends_with("第三段")) == 1)
check("A trailing sends carry bridgeidle- ids",
      all(r["id"].startswith("bridgeidle-")
          for r in sends_with("第二段") + sends_with("第三段")))
check("A cursor advanced to 13", turn["cursor_seq"] == 13)
check("A trailing marks recorded for the unified flush",
      (12, "am12") in w._turn_reply_marks
      and (13, "am13") in w._turn_reply_marks)
w._deliver_idle("第二段", "am12")
check("A re-delivery of a pushed segment is deduped",
      len(sends_with("第二段")) == 1)
turn["last_reply_at"] = int(time.time()) - nb.TRAILING_QUIET_SECS - 1
seen_ok = {}
orig_ok = nb.outbox_result_ok


def _capture_ok(ch, row_id):
    seen_ok["ch"] = ch
    seen_ok["id"] = row_id
    return True


nb.outbox_result_ok = _capture_ok
try:
    check("A completed + quiet elapsed -> done",
          w.poll_turn(turn) == "done")
finally:
    nb.outbox_result_ok = orig_ok
check("A close checks the bound reply's outbox id",
      seen_ok.get("ch") == "weixin"
      and seen_ok.get("id") == nb.bridge_row_id("m-A"))
check("A no duplicate bound reply after close",
      [r["content"] for r in replies_for("m-A")] == ["第一段"])
(WX / "outbox_results.jsonl").write_text(
    json.dumps({"id": "other", "ok": False}) + "\n"
    + json.dumps({"id": nb.bridge_row_id("m-ok"), "ok": False}) + "\n"
    + json.dumps({"id": nb.bridge_row_id("m-ok"), "ok": True}) + "\n",
    encoding="utf-8")
check("A outbox_result_ok: latest true wins",
      nb.outbox_result_ok("weixin", nb.bridge_row_id("m-ok")) is True)
check("A outbox_result_ok: missing id is pending",
      nb.outbox_result_ok("weixin", "no-such-row") is None)
check("A outbox_result_ok: shadow channel is pending",
      nb.outbox_result_ok("wecom", nb.bridge_row_id("m-ok")) is None)

# ---------------------------------------------------------------- B
# No wall-clock cap: far past the old 600s, a fresh last reply keeps
# the turn open even when status is already completed. Silence then
# closes it, and the idle scan still delivers a post-close arrival
# on the same cursor.
now = int(time.time())
stub = StubGW()
stub.status = "completed"
stub.history = [asst(21, "第一段"), asst(22, "仍在滴入")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-B", reply_delivered=True, cursor_seq=21,
                 last_reply_at=now, trailing_since=now - 5000,
                 sent_at=now - 5100, reply_started=True,
                 hist_seen_seq=21)
check("B long trailing window stays open (no hard cap)",
      w.poll_turn(turn) == "running")
check("B in-window late segment still delivered",
      len(sends_with("仍在滴入")) == 1)
turn["last_reply_at"] = int(time.time()) - nb.TRAILING_QUIET_SECS - 1
check("B completed + quiet elapsed closes without a cap",
      w.poll_turn(turn) == "done")
set_state("weixin", session_id="sid1", idle_push_seq=22,
          idle_pushed_ids=["am21", "am22"])
stub.history.append(asst(23, "更迟到的结果"))
w.last_idle_scan = 0
w.idle_push_scan(w.cs())
check("B idle scan continues on the same cursor and pushes the "
      "post-close arrival", len(sends_with("更迟到的结果")) == 1)
w.last_idle_scan = 0
w.idle_push_scan(w.cs())
check("B idle scan does not re-push it",
      len(sends_with("更迟到的结果")) == 1
      and len(sends_with("仍在滴入")) == 1)

# ---------------------------------------------------------------- C
def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


spool = SBX / "spool" / "weixin.jsonl"
spool.write_text("", encoding="utf-8")
(WX / "outbox.jsonl").unlink(missing_ok=True)
sent = int(time.time()) - 10
ev_file = {"timestamp": iso(sent + 5), "type": "file_created",
           "message_id": "a1", "details": {"path": "/tmp/x/report.txt"}}
stub = StubGW()
stub.status = "running"
stub.activity = {"days": [{"activities": [ev_file]}]}
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-C", sent_at=sent, last_activity_poll=0)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
bodies = [r.get("content", "") for r in rows]
check("C new activity fires a progress notice before 120s",
      any("还在处理中" in b for b in bodies))
check("C event notice carries the folded 最近动态 line",
      any("最近动态：创建了文件 /tmp/x/report.txt" in b for b in bodies))
saved_turn = nb.load_state()["channels"]["weixin"]["turns"][0]
check("C activity_dirty cleared and event throttle stamped",
      saved_turn.get("activity_dirty") is False
      and (saved_turn.get("last_event_prog") or 0) > 0)

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-C2", sent_at=int(time.time()) - 10)
set_state("weixin", turns=[turn], session_id="sid1")
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
check("C no new event + age < 120s -> no progress notice", rows == [])

# ---------------------------------------------------------------- D
set_state("weixin", session_id="dead-sid", preamble_done=True,
          turns=[], idle_push_seq=99)
dead = StubGW()
dead.dead_sids = {"dead-sid"}
good = StubGW()
good.new_sid = "new-sid"
good.history = []
stubs = [dead, good]
orig_connect = nb.connect
nb.connect = lambda: stubs.pop(0)
try:
    w = nb.ChannelWorker("weixin")
    row = {"msgid": "m-D", "text": "问题D", "from_user": "u1"}
    w.queue = [(row, 10)]
    w._start_next()
finally:
    nb.connect = orig_connect
cs = w.cs()
check("D dead stored sid healed to the fresh session",
      cs.get("session_id") == "new-sid")
check("D turn opened on the fresh session",
      len(cs.get("turns") or []) == 1
      and cs["turns"][0]["session_id"] == "new-sid")
check("D chat.stream sent exactly once, on the new sid",
      len(good.opened) == 1
      and good.opened[0][1].get("session_id") == "new-sid")
check("D no fallback booked for the healed row",
      (cs.get("fallbacks") or 0) == 0 and w.queue == [])

# ---------------------------------------------------------------- E
set_state("test", session_id="dead-test", idle_push_seq=5,
          idle_scan_fail_streak=0, idle_scan_last_alert=0)
stub = StubGW()
stub.dead_sids = {"dead-test"}
w = worker_with("test", stub)
w.last_idle_scan = 0
w.idle_push_scan(w.cs())
calls_after_404 = stub.history_calls
check("E idle 404 invalidates the stored sid",
      w.cs().get("session_id") is None)
w.gw = stub
w.last_idle_scan = 0
w.idle_push_scan(w.cs())
check("E later scans are silent (no gw call, no 404 spam)",
      stub.history_calls == calls_after_404)

shadow_test_out = SBX / "shadow" / "test-outbox.jsonl"
shadow_test_out.unlink(missing_ok=True)
set_state("test", session_id="sid-ok", idle_push_seq=5)
bad = StubGW()
bad.fail_exc = RuntimeError("boom")
nb.connect = lambda: bad
try:
    w = nb.ChannelWorker("test")
    for _ in range(3):
        w.last_idle_scan = 0
        w.idle_push_scan(w.cs())
    cs = w.cs()
    alerts = [r for r in read_jsonl(shadow_test_out)
              if r.get("mode") == "send" and "空闲扫描" in
              (r.get("text") or "")]
    check("E ordinary failures streak to 3", cs.get(
        "idle_scan_fail_streak") == 3)
    check("E exactly one alert at the 3rd failure", len(alerts) == 1)
    w.last_idle_scan = 0
    w.idle_push_scan(w.cs())
    alerts = [r for r in read_jsonl(shadow_test_out)
              if r.get("mode") == "send" and "空闲扫描" in
              (r.get("text") or "")]
    check("E 4th failure within the hour sends no second alert",
          len(alerts) == 1
          and w.cs().get("idle_scan_fail_streak") == 4)
    good2 = StubGW()
    w.gw = good2
    w.last_idle_scan = 0
    w.idle_push_scan(w.cs())
    check("E a successful scan clears the streak",
          w.cs().get("idle_scan_fail_streak") == 0)
finally:
    nb.connect = orig_connect

# ---------------------------------------------------------------- F
stub = StubGW()
stub.status = "running"
stub.history = [asst(11, "门控外答案")]
w = worker_with("wecom", stub)
turn = make_turn(msgid="m-F")
check("F gate off: first reply still closes the turn at once",
      w.poll_turn(turn) == "done")
rows = [r for r in read_jsonl(SBX / "shadow" / "wecom-outbox.jsonl")
        if r.get("mode") == "reply" and r.get("msgid") == "m-F"]
check("F gate off: reply delivered", [r["content"] for r in rows]
      == ["门控外答案"])
check("F gate off: no trailing state engaged",
      not turn.get("reply_delivered"))

# ---------------------------------------------------------------- G
# Progress granularity: history delta is a second trigger, throttled
# with activity notices. Trailing sends use the turn's route.
(WX / "outbox.jsonl").unlink(missing_ok=True)
sent = int(time.time()) - 10
stub = StubGW()
stub.status = "running"
stub.history = [{"event_name": "tool.invoke", "seq": 11,
                 "payload": {"name": "shell"}}]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-H", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
check("G history delta fires a progress notice before 120s",
      any("还在处理中" in (r.get("content") or "") for r in rows))

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub.history.append({"event_name": "tool.invoke", "seq": 12,
                     "payload": {"name": "shell"}})
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
saved = nb.load_state()["channels"]["weixin"]["turns"][0]
check("G second history delta inside 60s sends no extra notice",
      rows == [])
check("G throttled history delta stays pending",
      saved.get("history_dirty") is True)
st = nb.load_state()
st["channels"]["weixin"]["turns"][0]["last_event_prog"] = \
    int(time.time()) - nb.LT_EVENT_PROG_MIN_SECS - 1
nb.save_state(st)
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
check("G pending history delta fires once the throttle elapses",
      any("还在处理中" in (r.get("content") or "") for r in rows))

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [{"event_name": "message.user", "seq": 11,
                 "payload": {"display_text": "hello"}}]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-H2", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
rows = [r for r in read_jsonl(WX / "outbox.jsonl") if r.get("mode") == "send"]
check("G user echo is not a progress trigger", rows == [])

stub = StubGW()
stub.status = "running"
ev_late = {"timestamp": iso(int(time.time())), "type": "file_created",
           "message_id": "a-late",
           "details": {"path": "/tmp/x/late.txt"}}
stub.activity = {"days": [{"activities": [ev_late]}]}
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-act", sent_at=int(time.time()) - 30,
                 last_activity_poll=int(time.time()))
check("G activity poll respects the 12s cadence",
      w._poll_activity(turn) is False)
turn["last_activity_poll"] = int(time.time()) - nb.ACTIVITY_POLL_SECS - 1
check("G activity poll runs once the cadence elapses",
      w._poll_activity(turn) is True)

stub = StubGW()
stub.status = "completed"
stub.history = [asst(41, "微首"), asst(42, "微次")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-wxr", from_user="user-from-turn",
                 hist_seen_seq=10)
check("G weixin trailing stays open after the first segments",
      w.poll_turn(turn) == "running")
routed = sends_with("微次")
check("G weixin trailing send uses the turn's from_user",
      len(routed) == 1
      and routed[0].get("to_user_id") == "user-from-turn"
      and routed[0].get("to_user_id")
      != nb.CFG["channels"]["weixin"].get("from_user_id"))

flag = SBX / "longtask-wecom"
flag.touch()
try:
    stub = StubGW()
    stub.status = "completed"
    stub.history = [asst(51, "企微首段"), asst(52, "企微次段")]
    w = worker_with("wecom", stub)
    turn = make_turn(msgid="m-wc", chatid="grp-9", chattype="group",
                     hist_seen_seq=10)
    check("G wecom trailing stays open after the first segments",
          w.poll_turn(turn) == "running")
    wc_rows = [r for r in read_jsonl(SBX / "shadow" / "wecom-outbox.jsonl")
               if r.get("mode") == "send" and r.get("content") == "企微次段"]
    check("G wecom trailing send uses the turn chat",
          len(wc_rows) == 1
          and wc_rows[0].get("chatid") == "grp-9"
          and wc_rows[0].get("chat_type") == 2)
finally:
    flag.unlink(missing_ok=True)
check("G wecom longtask flag removed again",
      not nb.longtask_mode("wecom"))

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
