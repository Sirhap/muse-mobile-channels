#!/usr/bin/env python3
"""DEF-wecom-started: one started notice per diverted WeCom turn.

Root cause this pins down: _register_bridge_feedback stores
queued=False when the bridge snapshot is idle (no active turn, empty
queue). _feedback_scan_once used to skip any record that was not
via_bridge AND queued, so an idle→active start never emitted
STARTED_NOTICE_TEMPLATE. A merged active entry must still emit
nothing. A delivered formal reply drops the record, so a later scan
cannot send again. Register itself still writes no arrival ack.

Sandbox only: the bridge snapshot is an in-process fake, and every
gateway state path is rebound under /tmp. No production state.

Run: python3 tests/test_wecom_started_20261009.py
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
SBX = Path("/tmp/wecom-started-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_started", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    """Record one named assertion and print PASS or FAIL."""
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


# In-process stand-in for the native-bridge queue_snapshot.
FAKE = {"active": [], "queued": []}
wc._bridge_snapshot = lambda channel: (
    list(FAKE["active"]), list(FAKE["queued"]))


def fresh_state(name):
    """Point gateway state paths at a fresh sandbox directory."""
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
    """Build a Gateway without taking the process lock or the network."""
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
    """Parse a jsonl file into dicts; missing file is an empty list."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def send_rows(state_dir):
    """Unbound outbox send rows (feedback notices live here)."""
    return [row for row in read_jsonl(state_dir / "outbox.jsonl")
            if row.get("mode") == "send"]


def started_rows(state_dir):
    """Send rows that are the started notice, not a wait reminder."""
    return [row for row in send_rows(state_dir)
            if str(row.get("content", "")).startswith("▶️ 排到你了")]


def expected_started(msgid, text):
    """The exact notice the scan should queue for this msgid."""
    return wc.STARTED_NOTICE_TEMPLATE.format(
        excerpt=wc._excerpt(text, 20), code=str(msgid)[:8])


def scenario_idle_to_active():
    """Idle snapshot at register (queued=False) then the turn starts."""
    state_dir = fresh_state("idle")
    gateway = make_gateway()
    msgid = "M-IDLE01"
    text = "空闲直接开始的长任务"
    FAKE["active"] = []
    FAKE["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", msgid, text)
    rec = gateway.feedback_track.get(msgid) or {}
    print(
        "evidence idle register: "
        f"snapshot active={FAKE['active']!r} queued={FAKE['queued']!r} "
        f"record.queued={rec.get('queued')!r} "
        f"via_bridge={rec.get('via_bridge')!r} "
        f"started_notice={rec.get('started_notice')!r}"
    )
    check("1 register: idle snapshot, record queued=False via_bridge=True",
          rec.get("queued") is False and rec.get("via_bridge") is True
          and rec.get("started_notice") is False)
    check("1 register: no outbox row (no arrival ack)",
          read_jsonl(state_dir / "outbox.jsonl") == [])
    n_idle = gateway._feedback_scan_once(now=time.time() + 1)
    check("1 scan while still idle: 0 notices",
          n_idle == 0 and started_rows(state_dir) == [])

    # In the bridge queue, but this record was not marked queued, and
    # the turn has not started. No started notice and no wait reminder.
    FAKE["queued"] = [msgid]
    queued_at = rec["queued_at"]
    n_q = gateway._feedback_scan_once(now=queued_at + 5)
    n_wait = gateway._feedback_scan_once(now=queued_at + 180)
    check("1 sitting in queue with queued=False: no started, no wait",
          n_q == 0 and n_wait == 0 and send_rows(state_dir) == [])

    FAKE["active"] = [{"msgid": msgid, "lane": "main", "secs": 1}]
    FAKE["queued"] = []
    n_start = gateway._feedback_scan_once(now=time.time() + 10)
    rows = started_rows(state_dir)
    check("1 idle→active: exactly 1 started",
          n_start == 1 and len(rows) == 1
          and rows[0]["content"] == expected_started(msgid, text)
          and rows[0].get("chatid") == "sirhao"
          and str(rows[0].get("id", "")).startswith("started-"))
    n_again = gateway._feedback_scan_once(now=time.time() + 20)
    check("1 second scan: still exactly 1 started",
          n_again == 0 and len(started_rows(state_dir)) == 1)


def scenario_queued_to_active():
    """Busy snapshot at register (queued=True) then this turn starts."""
    state_dir = fresh_state("queued")
    gateway = make_gateway()
    msgid = "M-QUEUE1"
    text = "排队后才开始的长任务"
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 4}]
    FAKE["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", msgid, text)
    rec = gateway.feedback_track.get(msgid) or {}
    print(
        "evidence queued register: "
        f"snapshot active={[a.get('msgid') for a in FAKE['active']]!r} "
        f"queued={FAKE['queued']!r} "
        f"record.queued={rec.get('queued')!r} "
        f"via_bridge={rec.get('via_bridge')!r}"
    )
    check("2 register: busy snapshot, record queued=True",
          rec.get("queued") is True and rec.get("via_bridge") is True)
    check("2 register: no arrival ack while still waiting",
          send_rows(state_dir) == [])
    FAKE["queued"] = [msgid]
    n_wait = gateway._feedback_scan_once(now=time.time() + 5)
    check("2 still queued: no started yet",
          n_wait == 0 and started_rows(state_dir) == [])
    FAKE["active"] = [{"msgid": msgid, "lane": "main", "secs": 1}]
    FAKE["queued"] = []
    n_start = gateway._feedback_scan_once(now=time.time() + 6)
    rows = started_rows(state_dir)
    check("2 queued→active: exactly 1 started",
          n_start == 1 and len(rows) == 1
          and rows[0]["content"] == expected_started(msgid, text))
    n_again = gateway._feedback_scan_once(now=time.time() + 30)
    check("2 second scan: still exactly 1 started",
          n_again == 0 and len(started_rows(state_dir)) == 1)


def scenario_merged():
    """Merged active entries emit no started notice, queued flag or not."""
    state_dir = fresh_state("merged")
    gateway = make_gateway()

    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 3}]
    FAKE["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", "M-MERGE1", "被并入的补充")
    rec_q = gateway.feedback_track["M-MERGE1"]
    check("3a register while busy: queued=True", rec_q.get("queued") is True)
    FAKE["active"] = [
        {"msgid": "OTHER", "lane": "main"},
        {"msgid": "M-MERGE1", "lane": "main", "merged": True},
    ]
    FAKE["queued"] = []
    n1 = gateway._feedback_scan_once(now=time.time() + 5)
    n1b = gateway._feedback_scan_once(now=time.time() + 15)
    check("3a merged after queued register: 0 started, latch set",
          n1 == 0 and n1b == 0
          and rec_q.get("started_notice") is True
          and started_rows(state_dir) == [])

    FAKE["active"] = []
    FAKE["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", "M-MERGE2", "空闲登记后被并入")
    rec_i = gateway.feedback_track["M-MERGE2"]
    print(
        "evidence merged-from-idle register: "
        f"record.queued={rec_i.get('queued')!r} "
        f"via_bridge={rec_i.get('via_bridge')!r}"
    )
    check("3b register while idle: queued=False", rec_i.get("queued") is False)
    FAKE["active"] = [
        {"msgid": "OTHER", "lane": "main"},
        {"msgid": "M-MERGE2", "lane": "main", "merged": True},
    ]
    n2 = gateway._feedback_scan_once(now=time.time() + 5)
    n2b = gateway._feedback_scan_once(now=time.time() + 15)
    check("3b merged after idle register: 0 started, latch set",
          n2 == 0 and n2b == 0
          and rec_i.get("started_notice") is True
          and started_rows(state_dir) == [])


async def scenario_reply_clears():
    """After the formal reply, the track is gone and nothing re-sends."""
    state_dir = fresh_state("reply")
    gateway = make_gateway()
    msgid = "M-REPLY1"
    text = "回复后不能再发开始"
    FAKE["active"] = []
    FAKE["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", msgid, text)
    check("4 register idle: queued=False",
          gateway.feedback_track[msgid].get("queued") is False)
    FAKE["active"] = [{"msgid": msgid, "lane": "main", "secs": 2}]
    FAKE["queued"] = []
    n_start = gateway._feedback_scan_once(now=time.time() + 5)
    check("4 before reply: exactly 1 started",
          n_start == 1 and len(started_rows(state_dir)) == 1)

    gateway.reqmap[msgid] = {
        "req_id": "req-reply", "stream_id": "stream-reply",
        "chatid": "sirhao", "chattype": "single",
    }

    async def fake_respond(req_id, body):
        return {"errcode": 0}

    gateway.respond = fake_respond
    ok = await gateway.dispatch_outbox_item(
        {"id": "row-reply", "mode": "reply", "msgid": msgid,
         "content": "正式答复"})
    check("4 formal reply dispatch ok", ok is True)
    check("4 track cleared by delivered reply",
          msgid not in gateway.feedback_track)
    before = len(started_rows(state_dir))
    FAKE["active"] = [{"msgid": msgid, "lane": "main", "secs": 9}]
    FAKE["queued"] = [msgid]
    n_after = gateway._feedback_scan_once(now=time.time() + 1000)
    check("4 after reply: no re-send",
          n_after == 0 and len(started_rows(state_dir)) == before == 1)


async def main():
    """Run the four started-notice cases."""
    scenario_idle_to_active()
    scenario_queued_to_active()
    scenario_merged()
    await scenario_reply_clears()


asyncio.run(main())

failed = [name for name, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
