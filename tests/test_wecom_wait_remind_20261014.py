#!/usr/bin/env python3
"""WeCom bridge-lane 180s wait reminder — sandbox simulation (2026-10-14).

Why this file exists: open-issues P1#4's last unproven item. The
started notice got its live proof (PR#6), but the long-wait reminder
(BRIDGE_WAIT_TEMPLATE after WAIT_REMIND_SECS=180 for a record that
was actually queued) never had a real queued>=180s scenario, and the
existing suites only cover it narrowly:

- test_wecom_align_20261009.py scenario (c) covers the core path for
  ONE waiter at position 1 (no reminder at +179s, exactly one at
  +180s, none again at +600s), asserting only the return counts and
  the "排队中" / "第 1 位" / "已等" substrings of the queued row.
- test_wecom_started_20261009.py scenario 1 pins the gate: a record
  registered queued=False never gets a wait reminder, even sitting
  in the queue at +180s.

What this file adds on top (no production code changes):
S1 the full row shape of the wait reminder at position 2: exact
   template text (incl. the duration string), mode reply_notice,
   msgid binding, chatid, chat_type and the waitremind- id prefix;
   repeated scans AT the threshold do not re-fire.
S2 two waiters with staggered queued_at: a scan fires only the one
   past 180s; the second fires later, at its own position; each
   exactly once across many scans.
S3 wait-then-started: a record that already got its wait reminder
   still gets exactly one started notice when its turn begins.
S4 a formal reply delivered AFTER the wait reminder fired clears
   the record (through the real dispatch_outbox_item hook), and a
   later scan cannot fire anything for that msgid again.
S5 a formal reply delivered BEFORE 180s prevents the wait reminder
   entirely, even if the msgid reappears in the queue snapshot.
S6 the queued=False gate, re-pinned inside this file so the wait
   path is self-contained.
S7 the position in the reminder is the position at fire time (a
   waiter that moves from position 2 to position 1 while waiting
   is reminded as 第 1 位).

Clock: no real waiting anywhere. Registration stamps queued_at from
the real clock (the register API takes no time argument), so each
scenario captures the stored queued_at and then drives
_feedback_scan_once with injected `now` offsets — the same hybrid
the 2026-10-09 suites use. The bridge snapshot is an in-process
fake; every gateway state path is rebound under /tmp. No live
sends, no production state.

S8/S9 latch-after-queue regression (bug found while writing this
file, fixed 2026-10-14 in both gateways): _feedback_scan_once used
to latch wait_reminded/started_notice=True BEFORE _queue_notice,
so one transient append failure lost the notice forever. The fix
latches only after the row is queued; S8 (wait) and S9 (started)
stub append_jsonl to raise once and assert the notice is retried
and delivered exactly once by a later scan.

Run: ~/muse-test-venv/bin/python tests/test_wecom_wait_remind_20261014.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/wecom-waitremind-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_waitremind", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


# In-process stand-in for the native-bridge queue_snapshot. Note the
# producer's asymmetry (native_bridge.py queue_snapshot): "active"
# rows are dicts, "queued" rows are bare msgid strings.
FAKE = {"active": [], "queued": []}
wc._bridge_snapshot = lambda channel: (list(FAKE["active"]),
                                       list(FAKE["queued"]))
BUSY = [{"msgid": "OTHER", "lane": "main", "secs": 3}]


def fresh_state(name):
    d = SBX / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    wc.STATE = d
    wc.INBOX = d / "inbox.jsonl"
    wc.OUTBOX = d / "outbox.jsonl"
    wc.OUTBOX_RESULTS = d / "outbox_results.jsonl"
    wc.OUTBOX_OFFSET = d / "outbox.offset"
    wc.OUTBOX_RETRY = d / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = d / "outbox_partial.json"
    wc.OUTBOX_PARKED = d / "outbox_parked.json"
    wc.REQMAP = d / "reqmap.json"
    wc.STATUS = d / "status.json"
    wc.SEEN_FILE = d / "seen_ids.jsonl"
    wc.HOOK_STATE_DIR = d / "hookstate"
    wc.HOOK_STATE_DIR.mkdir(parents=True)
    return d


def make_gateway():
    g = wc.Gateway.__new__(wc.Gateway)
    g.ws = None
    g.connected = True
    g.started_at = int(time.time())
    g.msgs_received = 0
    g.msgs_sent = 0
    g.last_error = ""
    g.state = "connected"
    g.pending_responses = {}
    g.respond_locks = {}
    g.open_streams = {}
    g._http = None
    g.allow_users = set()
    g.seen_msgids = set()
    g.reqmap = {}
    g.cards = {}
    g._lock_fd = None
    g._kicked = False
    g.feedback_track = {}
    g._coalescer = None
    g._coalesce_pending = {}
    return g


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def notice_rows(d):
    return [r for r in read_jsonl(d / "outbox.jsonl")
            if r.get("mode") == "reply_notice"]


def wait_rows(d, msgid=None):
    rows = [r for r in notice_rows(d)
            if str(r.get("id", "")).startswith("waitremind-")]
    if msgid is not None:
        rows = [r for r in rows if r.get("msgid") == msgid]
    return rows


def register_queued(g, msgid, text):
    """Register with a busy bridge snapshot so queued=True, and
    return the stored queued_at (the wait clock's start)."""
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = []
    g._register_bridge_feedback("testuser", "single", msgid, text)
    return g.feedback_track[msgid]["queued_at"]


def scenario_s1():
    d = fresh_state("s1")
    g = make_gateway()
    qa = register_queued(g, "M-W1", "排第二位的长等待")
    rec = g.feedback_track["M-W1"]
    check("S1 register: queued=True, wait_reminded=False, queued_at set",
          rec.get("queued") is True and rec.get("wait_reminded") is False
          and rec.get("queued_at") == qa)
    # Another (untracked) msgid sits ahead: M-W1 is at position 2.
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["OTHERQ", "M-W1"]
    for off in (0, 60, 179):
        n = g._feedback_scan_once(now=qa + off)
        check(f"S1 no reminder at +{off}s", n == 0 and wait_rows(d) == [])
    n = g._feedback_scan_once(now=qa + 180)
    rows = wait_rows(d, "M-W1")
    expected = wc.BRIDGE_WAIT_TEMPLATE.format(n=2, dur=wc._dur_str(180))
    check("S1 exactly one reminder at +180s",
          n == 1 and len(rows) == 1 and len(notice_rows(d)) == 1)
    check("S1 reminder row shape: bound reply_notice, waitremind- id",
          rows and rows[0].get("msgid") == "M-W1"
          and rows[0].get("chatid") == "testuser"
          and rows[0].get("chat_type") == 1)
    check("S1 reminder text is the exact template (position 2, dur)",
          rows and rows[0].get("content") == expected
          and "第 2 位" in rows[0]["content"]
          and "已等约 3 分钟" in rows[0]["content"])
    check("S1 wait_reminded latched", rec.get("wait_reminded") is True)
    for off in (180, 181, 600, 3600):
        n = g._feedback_scan_once(now=qa + off)
        check(f"S1 no re-fire at +{off}s",
              n == 0 and len(wait_rows(d)) == 1)


def scenario_s2():
    d = fresh_state("s2")
    g = make_gateway()
    qa_a = register_queued(g, "M-A", "先到的等待者")
    # Simulate M-A having arrived 120s before M-B: the register API
    # stamps from the real clock, so backdate the stored stamps —
    # the scan's wait math is what is under test.
    g.feedback_track["M-A"]["queued_at"] = qa_a - 120
    g.feedback_track["M-A"]["ts"] = qa_a - 120
    qa_a -= 120
    FAKE["queued"] = ["M-A"]
    qa_b = register_queued(g, "M-B", "后到的等待者")
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-A", "M-B"]
    # At qa_b+60: M-A has waited 180s, M-B only 60s.
    n = g._feedback_scan_once(now=qa_b + 60)
    check("S2 scan fires only the waiter past 180s",
          n == 1 and len(wait_rows(d, "M-A")) == 1
          and wait_rows(d, "M-B") == [])
    row_a = wait_rows(d, "M-A")[0]
    check("S2 first reminder: position 1, waited ~3 min",
          row_a.get("content") == wc.BRIDGE_WAIT_TEMPLATE.format(
              n=1, dur=wc._dur_str(180)))
    # At qa_b+180: M-B reaches the threshold, at position 2.
    n = g._feedback_scan_once(now=qa_b + 180)
    rows_b = wait_rows(d, "M-B")
    check("S2 second waiter fires at its own 180s, position 2",
          n == 1 and len(rows_b) == 1
          and rows_b[0].get("content") == wc.BRIDGE_WAIT_TEMPLATE.format(
              n=2, dur=wc._dur_str(180)))
    n = g._feedback_scan_once(now=qa_b + 900)
    check("S2 no repeats for either waiter",
          n == 0 and len(wait_rows(d)) == 2
          and wait_rows(d, "M-A")[0].get("id") != rows_b[0].get("id"))


def scenario_s3():
    d = fresh_state("s3")
    g = make_gateway()
    qa = register_queued(g, "M-T", "等完还要开始的任务")
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-T"]
    n = g._feedback_scan_once(now=qa + 180)
    check("S3 wait reminder fired while queued",
          n == 1 and len(wait_rows(d, "M-T")) == 1)
    FAKE["active"] = [{"msgid": "M-T", "lane": "main", "secs": 1}]
    FAKE["queued"] = []
    n = g._feedback_scan_once(now=qa + 200)
    started = [r for r in notice_rows(d)
               if str(r.get("id", "")).startswith("started-")]
    check("S3 started notice still fires after the wait reminder",
          n == 1 and len(started) == 1
          and started[0].get("msgid") == "M-T")
    n = g._feedback_scan_once(now=qa + 260)
    check("S3 nothing further on later scans",
          n == 0 and len(notice_rows(d)) == 2)


async def deliver_reply(g, msgid, row_id):
    g.reqmap[msgid] = {"req_id": f"req-{row_id}", "stream_id": "st",
                       "chatid": "testuser", "chattype": "single"}

    async def fake_respond(req_id, body):
        return {"errcode": 0}

    g.respond = fake_respond
    return await g.dispatch_outbox_item(
        {"id": row_id, "mode": "reply", "msgid": msgid,
         "content": "正式答复"})


async def scenario_s4():
    d = fresh_state("s4")
    g = make_gateway()
    qa = register_queued(g, "M-R", "等过又答完的任务")
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-R"]
    n = g._feedback_scan_once(now=qa + 180)
    check("S4 wait reminder fired", n == 1 and len(wait_rows(d)) == 1)
    ok = await deliver_reply(g, "M-R", "row-r")
    check("S4 formal reply delivered ok", ok is True)
    check("S4 record cleared by the reply",
          "M-R" not in g.feedback_track)
    FAKE["queued"] = ["M-R"]
    before = len(notice_rows(d))
    n = g._feedback_scan_once(now=qa + 3600)
    check("S4 no notice after reply, even if msgid reappears queued",
          n == 0 and len(notice_rows(d)) == before)


async def scenario_s5():
    d = fresh_state("s5")
    g = make_gateway()
    qa = register_queued(g, "M-E", "没等满就答完的任务")
    ok = await deliver_reply(g, "M-E", "row-e")
    check("S5 reply before 180s delivered and cleared the record",
          ok is True and "M-E" not in g.feedback_track)
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-E"]
    n = g._feedback_scan_once(now=qa + 600)
    check("S5 wait reminder never fires after an early reply",
          n == 0 and notice_rows(d) == [])


def scenario_s6():
    d = fresh_state("s6")
    g = make_gateway()
    FAKE["active"] = []
    FAKE["queued"] = []
    g._register_bridge_feedback("testuser", "single", "M-I", "空闲登记的任务")
    rec = g.feedback_track["M-I"]
    qa = rec["queued_at"]
    check("S6 idle register: queued=False", rec.get("queued") is False)
    FAKE["queued"] = ["M-I"]
    for off in (180, 3600):
        n = g._feedback_scan_once(now=qa + off)
        check(f"S6 queued=False record: no wait reminder at +{off}s",
              n == 0 and wait_rows(d) == [])


def scenario_s7():
    d = fresh_state("s7")
    g = make_gateway()
    qa = register_queued(g, "M-P", "位置会前进的等待")
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["OTHERQ", "M-P"]
    n = g._feedback_scan_once(now=qa + 60)
    check("S7 no reminder at +60s from position 2",
          n == 0 and wait_rows(d) == [])
    FAKE["queued"] = ["M-P"]
    n = g._feedback_scan_once(now=qa + 180)
    rows = wait_rows(d, "M-P")
    check("S7 reminder uses the position at fire time (第 1 位)",
          n == 1 and len(rows) == 1
          and rows[0].get("content") == wc.BRIDGE_WAIT_TEMPLATE.format(
              n=1, dur=wc._dur_str(180)))


def _fail_once_append(g):
    """Instance-level append_jsonl stub: raises OSError on the first
    call, then delegates to the real class implementation."""
    real = wc.Gateway.append_jsonl
    state = {"calls": 0}

    def stub(path, obj):
        state["calls"] += 1
        if state["calls"] == 1:
            raise OSError("simulated transient append failure")
        return real(path, obj)

    g.append_jsonl = stub
    return state


def scenario_s8():
    d = fresh_state("s8")
    g = make_gateway()
    qa = register_queued(g, "M-F", "落行失败要重试的等待")
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-F"]
    _fail_once_append(g)
    n = g._feedback_scan_once(now=qa + 200)
    check("S8 failed append: scan queues nothing, latch stays unset",
          n == 0 and wait_rows(d) == []
          and g.feedback_track["M-F"].get("wait_reminded") is False)
    n = g._feedback_scan_once(now=qa + 260)
    check("S8 next scan retries and delivers the reminder once",
          n == 1 and len(wait_rows(d, "M-F")) == 1
          and g.feedback_track["M-F"].get("wait_reminded") is True)
    n = g._feedback_scan_once(now=qa + 600)
    check("S8 no duplicate after the retried delivery",
          n == 0 and len(wait_rows(d)) == 1)


def scenario_s9():
    d = fresh_state("s9")
    g = make_gateway()
    qa = register_queued(g, "M-S", "落行失败要重试的开始")
    _fail_once_append(g)
    FAKE["active"] = [{"msgid": "M-S", "lane": "main", "secs": 1}]
    FAKE["queued"] = []
    n = g._feedback_scan_once(now=qa + 10)
    started = [r for r in notice_rows(d)
               if str(r.get("id", "")).startswith("started-")]
    check("S9 failed append: started not queued, latch stays unset",
          n == 0 and started == []
          and g.feedback_track["M-S"].get("started_notice") is False)
    n = g._feedback_scan_once(now=qa + 20)
    started = [r for r in notice_rows(d)
               if str(r.get("id", "")).startswith("started-")]
    check("S9 next scan retries and delivers started exactly once",
          n == 1 and len(started) == 1
          and g.feedback_track["M-S"].get("started_notice") is True)


async def main():
    scenario_s1()
    scenario_s2()
    scenario_s3()
    await scenario_s4()
    await scenario_s5()
    scenario_s6()
    scenario_s7()
    scenario_s8()
    scenario_s9()


asyncio.run(main())

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
