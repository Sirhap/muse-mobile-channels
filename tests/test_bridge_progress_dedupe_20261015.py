#!/usr/bin/env python3
"""Progress-notice dedupe (2026-10-15) — sandbox tests.

Pins the fix for the live duplicate-progress bug (investigation:
~/workspace/dup-stale-investigation-2026-10-15/evidence.md §2):
the bridge queued the same progress bubble twice — Weixin
bridgeprog-75163870-0 written and delivered twice on 2026-10-15
14:39 — because step()'s timed trigger (45s / next_prog) and the
LT2 event trigger (last_event_prog, starting at 0) kept separate
throttle clocks, and progress_notice() was the only outbox write
path without an outbox_already_has idempotency guard.

Covered here:
  S1 same minute bucket: timed fire, then an event trigger inside
     the shared gap -> still ONE row; the dirty flag stays pending;
     a direct same-rid progress_notice call is deduped by the guard.
  S2 different bucket, short gap (the 2026-10-15 15:00 shape: timed
     at age 45s, event at age ~70s, same 「约 1 分钟」 text) -> the
     event trigger does not stack a second bubble.
  S3 event path still fires once the shared gap has elapsed with a
     real pending event, and pushes the timed notice out.
  S4 timed path still refires once PROG_EVERY_SECS has elapsed
     since the last notice (shared gate is not a mute).
  S5 formal-reply path regression: deliver_reply stays exactly
     once, and progress_notice stays silent after the reply.
  S6 WeCom: two direct progress_notice calls with the same age
     produce ONE reply_notice row (guard on the bound path too).

The real bridge module is imported with bot_state / STATE_F /
STATUS_F / BASE redirected into a sandbox; GW is a stub. No
production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_bridge_progress_dedupe_20261015.py
"""
import copy
import importlib.util
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-prog-dedupe-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
for d in (WX, WC, SBX / "spool", SBX / "shadow"):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
for flag in ("enabled-weixin", "enabled-wecom", "longtask-weixin"):
    (SBX / flag).touch()

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")
nb.BASE = str(SBX)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def prog_rows(state_dir=WX):
    return [r for r in read_jsonl(state_dir / "outbox.jsonl")
            if str(r.get("id", "")).startswith("bridgeprog-")]


def replies_for(msgid):
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "reply" and r.get("msgid") == msgid]


class StubGW:
    def __init__(self):
        self.history = []
        self.status = "running"
        self.activity = {"days": []}

    def close(self):
        pass

    def _open(self, method, body=None):
        return "stream-1"

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "chat.history":
            return {"chat_events": list(self.history)}
        if method == "sessions.get":
            return {"session_id": (path_params or {}).get("id"),
                    "status": self.status}
        if method == "activity.list":
            return self.activity
        if method == "chat.cancel":
            return {}
        raise AssertionError(f"unexpected call {method}")


def make_turn(**over):
    t = {"msgid": "m-base", "lane": "main", "session_id": "sid1",
         "baseline": 10, "sent_at": int(time.time()),
         "text": "帮我做件事", "from_user": "u1", "chatid": "",
         "chattype": "", "next_prog": 0, "ids": ["m-base"],
         "reply_started": False, "sess_status": None,
         "status_completed_seen": False, "trailing_quiet_seen": False,
         "activities": [], "activity_seen": [],
         "last_activity_poll": int(time.time()), "last_activity": None,
         "reply_delivered": False, "cursor_seq": 10, "last_reply_at": 0,
         "trailing_since": 0, "activity_dirty": False,
         "history_dirty": False, "last_event_prog": 0, "last_prog_at": 0,
         "hist_seen_seq": 10}
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


def saved_turn(ch="weixin"):
    return nb.load_state()["channels"][ch]["turns"][0]


SPOOL = str(SBX / "spool" / "weixin.jsonl")
Path(SPOOL).write_text("", encoding="utf-8")
now = int(time.time())

# ---------------------------------------------------------------- S1
# Live 14:39 shape: timed trigger fires at age ~50s (bucket 0);
# an LT2 event lands seconds later, still bucket 0.
stub = StubGW()
w = worker_with("weixin", stub)
turn = make_turn(msgid="7516387002942555528", sent_at=now - 50)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(SPOOL, None)
rows = prog_rows()
check("S1 timed trigger queues the first notice",
      len(rows) == 1 and rows[0]["id"] == "bridgeprog-75163870-0"
      and "约 1 分钟" in rows[0]["content"])
sv = saved_turn()
check("S1 timed fire stamps the shared last_prog_at",
      (sv.get("last_prog_at") or 0) > 0
      and abs((sv.get("next_prog") or 0)
              - ((sv.get("last_prog_at") or 0) + nb.PROG_EVERY_SECS)) <= 2)

st = nb.load_state()
st["channels"]["weixin"]["turns"][0]["activity_dirty"] = True
nb.save_state(st)
w.step(SPOOL, None)
check("S1 event trigger inside the shared gap queues no second row",
      len(prog_rows()) == 1)
check("S1 throttled event stays pending (dirty not consumed)",
      saved_turn().get("activity_dirty") is True)

w.progress_notice(saved_turn(), 55)
check("S1 direct same-rid progress_notice is deduped by the guard",
      len(prog_rows()) == 1)

# ---------------------------------------------------------------- S2
# Live 15:00 shape: timed notice at age 45s (bucket 0), event at
# age ~70s would land in bucket 1 with the same 「约 1 分钟」 text.
(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
w = worker_with("weixin", stub)
turn = make_turn(msgid="7516392380879176584", sent_at=now - 70)
w.progress_notice(turn, 45)
check("S2 simulated timed notice queued (bucket 0)",
      len(prog_rows()) == 1
      and prog_rows()[0]["id"] == "bridgeprog-75163923-0")
fired_at = int(time.time()) - 25
turn2 = make_turn(msgid="7516392380879176584", sent_at=now - 70,
                  last_prog_at=fired_at,
                  next_prog=fired_at + nb.PROG_EVERY_SECS,
                  last_event_prog=0, activity_dirty=True)
set_state("weixin", turns=[turn2], last_boundary_ts=0,
          session_id="sid1")
w.step(SPOOL, None)
rows = prog_rows()
check("S2 event 25s after a timed notice does not stack "
      "(different bucket, same text)", len(rows) == 1)
check("S2 the surviving row is the timed one",
      rows and rows[0]["id"].endswith("-0")
      and "约 1 分钟" in rows[0]["content"])

# ---------------------------------------------------------------- S3
# Event path is not muted: a real pending event fires once the
# shared gap since the last notice exceeds LT_EVENT_PROG_MIN_SECS,
# even while the timed notice is not yet due.
(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
w = worker_with("weixin", stub)
last = int(time.time()) - 120
turn = make_turn(msgid="m-dedupe-s3", sent_at=int(time.time()) - 200,
                 last_prog_at=last, last_event_prog=last,
                 next_prog=int(time.time()) + 180, activity_dirty=True)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(SPOOL, None)
rows = prog_rows()
check("S3 pending event fires after the shared gap",
      len(rows) == 1 and rows[0]["id"] == "bridgeprog-m-dedupe-3"
      and "约 3 分钟" in rows[0]["content"])
sv = saved_turn()
check("S3 event fire consumes the dirty flag and restamps",
      sv.get("activity_dirty") is False
      and (sv.get("last_event_prog") or 0) > 0
      and abs((sv.get("last_prog_at") or 0) - int(time.time())) <= 5)
check("S3 event fire pushes the timed notice out",
      (sv.get("next_prog") or 0) >= int(time.time())
      + nb.PROG_EVERY_SECS - 5)

# ---------------------------------------------------------------- S4
# Timed path is not muted either: once PROG_EVERY_SECS has elapsed
# since the last notice and next_prog is due, it refires.
(WX / "outbox.jsonl").unlink(missing_ok=True)
stub = StubGW()
w = worker_with("weixin", stub)
last = int(time.time()) - (nb.PROG_EVERY_SECS + 1)
turn = make_turn(msgid="m-dedupe-s4", sent_at=int(time.time()) - 400,
                 last_prog_at=last, last_event_prog=last,
                 next_prog=int(time.time()) - 1)
set_state("weixin", turns=[turn], last_boundary_ts=0, session_id="sid1")
w.step(SPOOL, None)
rows = prog_rows()
check("S4 timed refire after PROG_EVERY_SECS",
      len(rows) == 1 and rows[0]["id"] == "bridgeprog-m-dedupe-6"
      and "约 7 分钟" in rows[0]["content"])

# ---------------------------------------------------------------- S5
# Formal-reply regression: exactly-once delivery is untouched, and
# progress stays silent once the reply has been delivered.
(WX / "outbox.jsonl").unlink(missing_ok=True)
nb.deliver_reply("weixin", "m-dedupe-s5", "正式答案")
nb.deliver_reply("weixin", "m-dedupe-s5", "正式答案")
check("S5 formal reply still lands exactly once",
      len(replies_for("m-dedupe-s5")) == 1)
w = worker_with("weixin", StubGW())
w.progress_notice(make_turn(msgid="m-dedupe-s5", reply_delivered=True),
                  300)
check("S5 no progress notice after the formal reply",
      prog_rows() == [])

# ---------------------------------------------------------------- S6
# WeCom bound path: the guard applies to reply_notice rows too.
w = worker_with("wecom", StubGW())
turn = make_turn(msgid="88c3db3e-live2", chatid="chatW",
                 chattype="single")
w.progress_notice(turn, 120)
w.progress_notice(turn, 120)
rows = [r for r in read_jsonl(WC / "outbox.jsonl")
        if r.get("mode") == "reply_notice"]
check("S6 wecom same-rid progress_notice lands exactly once",
      len(rows) == 1 and rows[0].get("msgid") == "88c3db3e-live2")

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
