#!/usr/bin/env python3
"""LT2 long-task trailing loop + session self-heal (2026-10-08) — sandbox tests.

Covers the LT2 changes to native_bridge.py (all gated by a per-channel
longtask-<channel> flag file; reference semantics are muse-cli
chat.py's Chat.send resident loop):
  A. Trailing loop: three reply segments in one turn are delivered
     one by one (first as the bound reply, the rest as unbound idle
     sends), the unified cursor advances, and the turn closes only
     after TWO consecutive polls of status=completed +
     TRAILING_QUIET_SECS of silence with no newer history row. The
     first such poll only arms trailing_quiet_seen. Re-delivery of
     an already-pushed segment is deduped. Closing consults
     outbox_results for the bound reply (ok / pending).
  B. There is no wall-clock cap. A turn that has been trailing for
     much longer than the old 600s cap stays open while the last
     reply is fresh. It closes only on two consecutive
     completed+quiet polls. After that close, idle_push_scan
     continues on the SAME cursor and pushes a later arrival
     exactly once.
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
  I. Trailing close is two beats. One completed+quiet poll arms
     trailing_quiet_seen and stays running. A late history row on
     the next poll is delivered and clears the mark. Two consecutive
     completed+quiet polls with no new row close on the second.
     A return to running, or a failed sessions.get, clears the mark.
     The queued successor does not _start_next until that confirming
     poll. Gate off still closes on the first reply.

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
        self.status_exc = None
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
            if self.status_exc:
                raise self.status_exc
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
         "status_completed_seen": False, "trailing_quiet_seen": False,
         "activities": [],
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
    hist_before = stub.history_calls
    check("A completed + quiet: first poll stays running",
          w.poll_turn(turn) == "running")
    check("A first quiet poll arms trailing_quiet_seen, no close",
          turn.get("trailing_quiet_seen") is True
          and seen_ok == {}
          and stub.history_calls == hist_before + 1)
    check("A second completed + quiet poll -> done",
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
old_id = nb.bridge_row_id("m-old")
pad = "".join(
    json.dumps({"id": f"pad-{i}", "ok": True}, ensure_ascii=False) + "\n"
    for i in range(12000))
(WX / "outbox_results.jsonl").write_text(
    json.dumps({"id": old_id, "ok": False}) + "\n"
    + json.dumps({"id": old_id, "ok": True}) + "\n"
    + pad,
    encoding="utf-8")
check("A outbox_result_ok: verdict older than 256KB still found",
      (WX / "outbox_results.jsonl").stat().st_size > 262144
      and nb.outbox_result_ok("weixin", old_id) is True)

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
check("B completed + quiet: first poll waits",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is True)
check("B second completed + quiet poll closes without a cap",
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


def tool_ev(seq):
    return {"event_name": "tool.invoke", "seq": seq,
            "payload": {"name": "shell"}}


(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [tool_ev(11), asst(12, "最终答案")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-sup", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
sup_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G reply supersedes same-batch tool progress",
      [r["content"] for r in replies_for("m-sup")] == ["最终答案"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in sup_sends))
check("G superseded history_dirty is cleared",
      nb.load_state()["channels"]["weixin"]["turns"][0].get(
          "history_dirty") is False)

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [tool_ev(11), asst(12, "后到的答案")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-sup2", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=11, history_dirty=True,
                 last_event_prog=int(time.time())
                 - nb.LT_EVENT_PROG_MIN_SECS - 1)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
sup_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G a pending dirty flag does not outlive the reply",
      [r["content"] for r in replies_for("m-sup2")] == ["后到的答案"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in sup_sends))

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [asst(11, "先回一段"), tool_ev(12)]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-sup3", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
sup_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G a tool row newer than the reply does not notify",
      [r["content"] for r in replies_for("m-sup3")] == ["先回一段"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in sup_sends))
check("G post-reply history_dirty is cleared",
      nb.load_state()["channels"]["weixin"]["turns"][0].get(
          "history_dirty") is False)

# activity_dirty must follow the same rule as history_dirty: a
# completed reply supersedes activity at or before that reply, and
# step() must not add 「还在处理中」. Activity strictly after
# last_reply_at used to notify; once the bound reply is delivered
# that notice is suppressed too.
(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [asst(12, "正式回复")]
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-actsup", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10, activity_dirty=True,
                 last_event_prog=int(time.time())
                 - nb.LT_EVENT_PROG_MIN_SECS - 1,
                 activities=[{"ts": iso(sent + 1),
                              "text": "创建了文件 /tmp/x/old.txt"}],
                 last_activity="创建了文件 /tmp/x/old.txt")
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
act_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G pending activity_dirty does not outlive the reply",
      [r["content"] for r in replies_for("m-actsup")] == ["正式回复"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in act_sends))
check("G superseded activity_dirty is cleared",
      nb.load_state()["channels"]["weixin"]["turns"][0].get(
          "activity_dirty") is False)

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [asst(12, "带着旧动态的回复")]
stub.activity = {"days": [{"activities": [
    {"timestamp": iso(sent + 1), "type": "file_created",
     "message_id": "old-act",
     "details": {"path": "/tmp/x/old.txt"}}]}]}
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-actsup2", sent_at=sent,
                 last_activity_poll=0, hist_seen_seq=10,
                 last_event_prog=0)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
act_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G same-step activity older than the reply does not notify",
      [r["content"] for r in replies_for("m-actsup2")] == ["带着旧动态的回复"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in act_sends))
check("G same-step superseded activity_dirty is cleared",
      nb.load_state()["channels"]["weixin"]["turns"][0].get(
          "activity_dirty") is False)

(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
stub.status = "running"
stub.history = [asst(12, "先到的回复")]
stub.activity = {"days": []}
w = worker_with("weixin", stub)
turn = make_turn(msgid="m-actnew", sent_at=sent,
                 last_activity_poll=int(time.time()),
                 hist_seen_seq=10)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(str(spool), None)
check("G reply alone sends no activity notice",
      [r["content"] for r in replies_for("m-actnew")] == ["先到的回复"]
      and not any("还在处理中" in (r.get("content") or "")
                  for r in read_jsonl(WX / "outbox.jsonl")
                  if r.get("mode") == "send"))
saved = nb.load_state()["channels"]["weixin"]["turns"][0]
later = int(saved.get("last_reply_at") or 0) + 5
stub.activity = {"days": [{"activities": [
    {"timestamp": iso(later), "type": "file_created",
     "message_id": "new-act",
     "details": {"path": "/tmp/x/after.txt"}}]}]}
saved["last_activity_poll"] = 0
saved["last_event_prog"] = 0
st = nb.load_state()
st["channels"]["weixin"]["turns"][0] = saved
nb.save_state(st)
w.step(str(spool), None)
act_sends = [r for r in read_jsonl(WX / "outbox.jsonl")
             if r.get("mode") == "send"]
check("G activity newer than the reply does not notify",
      not any("还在处理中" in (r.get("content") or "") for r in act_sends))
check("G post-reply activity_dirty is cleared",
      nb.load_state()["channels"]["weixin"]["turns"][0].get(
          "activity_dirty") is False)

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

# ---------------------------------------------------------------- H
# DEF-progress-after-done: once the bound formal reply is out, the
# 120s/300s timer and a later activity/history fold must not send
# 「还在处理中」 for that msgid. WeCom live + longtask, same emitter
# the 2026-10-09 21:35 bubble used. A still-running turn with no
# reply still gets the 12-minute notice, and a trailing segment
# after the formal reply is still delivered.
wc_out = WC / "outbox.jsonl"
wc_spool = SBX / "spool" / "wecom.jsonl"
wc_spool.write_text("", encoding="utf-8")
(SBX / "enabled-wecom").touch()
(SBX / "longtask-wecom").touch()
clip = SBX / "clip.mp4"
clip.write_bytes(b"video")
try:
    check("H wecom live and longtask on",
          nb.live_mode("wecom") and nb.longtask_mode("wecom"))
    wc_out.unlink(missing_ok=True)
    sent_h = int(time.time()) - 12 * 60
    stub = StubGW()
    stub.status = "running"
    w = worker_with("wecom", stub)
    turn = make_turn(msgid="6f3fcd57-run", chatid="grp-live",
                     chattype="group", sent_at=sent_h, next_prog=0,
                     text="生成一段视频")
    w.progress_notice(turn, 12 * 60)
    run_sends = [r for r in read_jsonl(wc_out)
                 if r.get("mode") == "reply_notice"]
    check("H still-running turn still gets the 12-minute notice",
          len(run_sends) == 1
          and run_sends[0].get("msgid") == "6f3fcd57-run"
          and "还在处理中" in (run_sends[0].get("content") or "")
          and "12 分钟" in (run_sends[0].get("content") or "")
          and "6f3fcd57" in (run_sends[0].get("content") or "")
          and not any(r.get("mode") == "send" for r in read_jsonl(wc_out)))

    wc_out.unlink(missing_ok=True)
    done = make_turn(msgid="6f3fcd57-done", chatid="grp-live",
                     chattype="group", sent_at=sent_h, next_prog=0,
                     text="生成一段视频", reply_delivered=True)
    w.progress_notice(done, 12 * 60)
    check("H progress_notice is a no-op after the formal reply",
          read_jsonl(wc_out) == [])

    wc_out.unlink(missing_ok=True)
    formal = "视频已生成\n[[FILE:" + str(clip) + "]]"
    stub.history = [asst(11, formal)]
    turn = make_turn(msgid="6f3fcd57-same", chatid="grp-live",
                     chattype="group", sent_at=sent_h, next_prog=0,
                     text="生成一段视频", hist_seen_seq=10,
                     last_activity_poll=int(time.time()))
    set_state("wecom", turns=[turn], last_boundary_ts=0,
              session_id="sid1")
    w.step(str(wc_spool), None)
    same_rows = read_jsonl(wc_out)
    same_replies = [r for r in same_rows if r.get("mode") == "reply"
                    and r.get("msgid") == "6f3fcd57-same"]
    same_files = [r for r in same_rows if r.get("mode") == "reply_file"
                  and r.get("msgid") == "6f3fcd57-same"]
    same_prog = [r for r in same_rows if r.get("mode") == "reply_notice"
                 and "还在处理中" in (r.get("content") or "")]
    check("H same step delivers the formal text",
          [r.get("content") for r in same_replies] == ["视频已生成"])
    check("H same step still delivers reply_file after that text",
          len(same_files) == 1 and same_files[0].get("file_path") == str(clip)
          and same_files[0].get("content") == "")
    check("H due 12-minute timer does not follow the formal reply",
          same_prog == [])
    saved_same = nb.load_state()["channels"]["wecom"]["turns"][0]
    check("H turn stays open with reply_delivered set",
          saved_same.get("reply_delivered") is True)

    wc_out.unlink(missing_ok=True)
    later_act = int(time.time()) + 60
    stub.history = [asst(11, "视频已生成"), asst(12, "补充说明"),
                    tool_ev(13)]
    stub.activity = {"days": [{"activities": [
        {"timestamp": iso(later_act), "type": "file_created",
         "message_id": "after-done",
         "details": {"path": "/tmp/x/after-done.txt"}}]}]}
    turn = make_turn(msgid="6f3fcd57-later", chatid="grp-live",
                     chattype="group", sent_at=sent_h, next_prog=0,
                     text="生成一段视频", reply_delivered=True,
                     cursor_seq=11, last_reply_at=int(time.time()) - 1,
                     trailing_since=int(time.time()) - 1,
                     hist_seen_seq=11, last_activity_poll=0,
                     last_event_prog=0, activity_dirty=True,
                     history_dirty=True, reply_started=True)
    set_state("wecom", turns=[turn], last_boundary_ts=0,
              session_id="sid1")
    w.step(str(wc_spool), None)
    later_rows = read_jsonl(wc_out)
    later_prog = [r for r in later_rows if r.get("mode") == "reply_notice"
                  and "还在处理中" in (r.get("content") or "")]
    later_tail = [r for r in later_rows if r.get("mode") == "send"
                  and r.get("content") == "补充说明"]
    check("H later timer and newer activity send no progress",
          later_prog == [])
    check("H trailing segment after the formal reply still delivers",
          len(later_tail) == 1 and later_tail[0].get("chatid") == "grp-live")
    saved_later = nb.load_state()["channels"]["wecom"]["turns"][0]
    check("H post-reply progress flags are cleared",
          saved_later.get("reply_delivered") is True
          and saved_later.get("activity_dirty") is False
          and saved_later.get("history_dirty") is False)
finally:
    (SBX / "enabled-wecom").unlink(missing_ok=True)
    (SBX / "longtask-wecom").unlink(missing_ok=True)
check("H wecom flags removed again",
      not nb.live_mode("wecom") and not nb.longtask_mode("wecom"))

# ---------------------------------------------------------------- I
# Two-beat trailing close. History is read once at the top of
# poll_turn; the close decision must not read it again or sleep.
# Section A/B above is the same contract on the original scenarios.


def poll_quiet(worker, turn, stub_gw):
    """One poll_turn, counting history reads and any sleep."""
    slept = []
    real_sleep = time.sleep

    def _trap(secs):
        slept.append(secs)

    time.sleep = _trap
    before = stub_gw.history_calls
    try:
        outcome = worker.poll_turn(turn)
    finally:
        time.sleep = real_sleep
    return outcome, stub_gw.history_calls - before, slept


def quiet_delivered(msgid, seq=11, text="旧回复", sess_status="running"):
    """A trailing turn whose last reply is already past the quiet window."""
    quiet_at = int(time.time()) - nb.TRAILING_QUIET_SECS - 1
    stub_gw = StubGW()
    stub_gw.status = "completed"
    stub_gw.history = [asst(seq, text)]
    worker = worker_with("weixin", stub_gw)
    turn = make_turn(
        msgid=msgid, reply_delivered=True, cursor_seq=seq,
        last_reply_at=quiet_at, trailing_since=quiet_at - 30,
        reply_started=True, hist_seen_seq=seq, sess_status=sess_status,
        last_activity_poll=int(time.time()))
    return stub_gw, worker, turn, quiet_at


stub, w, turn, quiet_at = quiet_delivered("m-i1", text="旧回复静默")
outcome, reads, slept = poll_quiet(w, turn, stub)
check("I1 quiet full + status just completed + no new history -> running",
      outcome == "running")
check("I1 arms trailing_quiet_seen beside sess_status, does not close",
      turn.get("trailing_quiet_seen") is True
      and turn.get("sess_status") == "completed")
check("I1 history read once before the decision, no sleep",
      reads == 1 and slept == [])
check("I1 already-seen reply is not sent again",
      sends_with("旧回复静默") == [])

stub.history.append(asst(12, "补上的尾段"))
outcome, reads, slept = poll_quiet(w, turn, stub)
check("I2 next poll delivers the late segment and stays running",
      outcome == "running" and len(sends_with("补上的尾段")) == 1
      and turn.get("cursor_seq") == 12)
check("I2 last_reply_at refreshed and confirmation cleared",
      turn.get("last_reply_at", 0) > quiet_at
      and not turn.get("trailing_quiet_seen")
      and reads == 1 and slept == [])

stub, w, turn, _quiet = quiet_delivered("m-i3", seq=31, text="静默满无新行")
outcome, reads, slept = poll_quiet(w, turn, stub)
check("I3 first completed+quiet poll stays running",
      outcome == "running" and turn.get("trailing_quiet_seen") is True
      and reads == 1 and slept == [])
outcome, reads, slept = poll_quiet(w, turn, stub)
check("I3 second completed+quiet poll with no new reply is done",
      outcome == "done" and reads == 1 and slept == [])

stub, w, turn, _quiet = quiet_delivered("m-i4", seq=41, text="中途又跑")
check("I4 first completed+quiet poll arms the mark",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is True)
stub.status = "running"
check("I4 status back to running clears the mark and stays open",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is False
      and turn.get("sess_status") == "running")
stub.status = "completed"
check("I4 completed again still needs another poll",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is True)
check("I4 the poll after the fresh arm is done",
      w.poll_turn(turn) == "done")

stub, w, turn, _quiet = quiet_delivered("m-i5", seq=51, text="状态查询失败")
check("I5 arm before the status failure",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is True)
stub.status_exc = RuntimeError("sessions.get down")
before = stub.history_calls
check("I5 sessions.get failure does not close",
      w.poll_turn(turn) == "running")
check("I5 failure clears the confirmation mark, history read once",
      turn.get("trailing_quiet_seen") is False
      and stub.history_calls == before + 1)
stub.status_exc = None
check("I5 a later completed poll must arm again, not close",
      w.poll_turn(turn) == "running"
      and turn.get("trailing_quiet_seen") is True)

(WX / "outbox.jsonl").unlink(missing_ok=True)
quiet_at = int(time.time()) - nb.TRAILING_QUIET_SECS - 1
stub = StubGW()
stub.status = "completed"
stub.history = [asst(61, "确认拍前")]
w = worker_with("weixin", stub)
turn = make_turn(
    msgid="m-i6", reply_delivered=True, cursor_seq=61,
    last_reply_at=quiet_at, trailing_since=quiet_at,
    reply_started=True, hist_seen_seq=61, sess_status="running",
    last_activity_poll=int(time.time()))
set_state("weixin", turns=[turn], last_boundary_ts=0,
          session_id="sid1", spool_offset=0, preamble_done=True)
spool_i = SBX / "spool" / "weixin.jsonl"
spool_i.write_text(
    json.dumps({"msgid": "m-i6-next", "text": "下一条",
                "from_user": "u1"}, ensure_ascii=False) + "\n",
    encoding="utf-8")
w.read_offset = 0
start_calls = []


def _record_start_next():
    start_calls.append(True)


w._start_next = _record_start_next
w.step(str(spool_i), None)
saved = nb.load_state()["channels"]["weixin"]["turns"]
check("I6 hold poll persists the mark and does not _start_next",
      start_calls == []
      and len(saved) == 1
      and saved[0]["msgid"] == "m-i6"
      and saved[0].get("trailing_quiet_seen") is True
      and saved[0].get("sess_status") == "completed"
      and [row.get("msgid") for row, _n in w.queue] == ["m-i6-next"]
      and stub.opened == []
      and stub.history_calls == 1)
w.step(str(spool_i), None)
saved = nb.load_state()["channels"]["weixin"]["turns"]
check("I6 confirming poll is when _start_next may run",
      start_calls == [True] and saved == []
      and stub.history_calls == 2)

check("I7 longtask flag absent on wecom", not nb.longtask_mode("wecom"))
stub = StubGW()
stub.status = "completed"
stub.history = [asst(71, "门控外立刻结束")]
w = worker_with("wecom", stub)
turn = make_turn(msgid="m-i7")
check("I7 gate off: first reply is done immediately",
      w.poll_turn(turn) == "done")
rows = [r for r in read_jsonl(SBX / "shadow" / "wecom-outbox.jsonl")
        if r.get("mode") == "reply" and r.get("msgid") == "m-i7"]
check("I7 gate off: reply delivered, confirmation mark unused",
      [r["content"] for r in rows] == ["门控外立刻结束"]
      and not turn.get("trailing_quiet_seen")
      and not turn.get("reply_delivered"))

set_state("test", session_id="sid-init", preamble_done=True,
          last_boundary_ts=0, rotate_main=False, fresh_start=False,
          session_turns={"main": 0},
          session_started={"main": int(time.time())})
stub = StubGW()
w = worker_with("test", stub)
born = w.start_turn(
    {"msgid": "m-init", "text": "hi", "from_user": "u1"}, w.cs())
check("I new turns persist trailing_quiet_seen beside sess_status",
      born.get("trailing_quiet_seen") is False
      and born.get("sess_status") is None
      and born.get("status_completed_seen") is False)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
