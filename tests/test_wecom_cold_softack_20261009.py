#!/usr/bin/env python3
"""WeCom cold-lane soft ack disabled (2026-10-09) — sandbox tests.

User order (voice, 2026-10-09): after a burst of voice messages each
drew an immediate "排队第 N 位" notice while a batch was in flight,
the cold-lane _maybe_soft_ack call in handle_message_callback was
removed. The function and soft_ack_text stay defined (rollback +
direct unit tests), but no inbound cold-lane message may queue a
softack send row any more.

Scenarios:
(a) busy batch in flight + inbound voice message -> message lands in
    the cold inbox, ZERO outbox rows (previously one softack row).
(b) same busy state + a second voice message -> still zero rows
    (the burst no longer stacks one notice per message).
(c) premise check: soft_ack_text itself still returns the template
    for the same busy state, proving (a)/(b) pass because the CALL
    is gone, not because the busy fixture stopped working.

Run: ~/workspace/wecom-bot/.venv/bin/python \
    tests/test_wecom_cold_softack_20261009.py
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
SBX = Path("/tmp/wecom-cold-softack-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_cold", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


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
    hs = d / "hookstate"
    hs.mkdir(parents=True)
    wc.HOOK_STATE_DIR = hs
    # Busy fixture: a batch in flight since now, nothing answered yet.
    now = time.time()
    (hs / "active_batch.json").write_text(json.dumps(
        {"msgids": ["M-RUNNING"], "since": now}))
    (hs / "pending.json").write_text(json.dumps({"M-RUNNING": 1}))
    # soft_ack_text requires the outbox file to exist (else it
    # fail-silences to None); an empty file = no reply rows yet.
    (d / "outbox.jsonl").write_text("")
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


async def scenario_ab():
    d = fresh_state("ab")
    g = make_gateway()

    async def fake_respond(req_id, body):
        return {"errcode": 0}

    g.respond = fake_respond

    def frame(msgid, req, text):
        return {"headers": {"req_id": req},
                "body": {"msgid": msgid, "msgtype": "voice",
                         "chattype": "single", "chatid": "sirhao",
                         "from": {"userid": "sirhao"},
                         "voice": {"content": text}}}

    await g.handle_message_callback(frame("V-1", "req-v1", "第一条语音"))
    check("a: voice message landed in cold inbox",
          [r.get("msgid") for r in read_jsonl(d / "inbox.jsonl")] == ["V-1"])
    check("a: no outbox rows at all while busy (soft ack gone)",
          read_jsonl(d / "outbox.jsonl") == [])

    await g.handle_message_callback(frame("V-2", "req-v2", "第二条语音"))
    check("b: burst second voice also in inbox",
          [r.get("msgid") for r in read_jsonl(d / "inbox.jsonl")]
          == ["V-1", "V-2"])
    check("b: still no outbox rows after burst",
          read_jsonl(d / "outbox.jsonl") == [])

    # (c) premise: the busy fixture WOULD have produced an ack.
    ack = wc.soft_ack_text(wc.STATE, wc.HOOK_STATE_DIR, "你好")
    check("c: soft_ack_text still fires for the same busy state",
          ack is not None and "排队第" in ack)


asyncio.run(scenario_ab())

src = (ROOT / "wecom-bot" / "gateway.py").read_text(encoding="utf-8")
check("source: no _maybe_soft_ack call site in inbound handler",
      "self._maybe_soft_ack(" not in src)
check("source: function kept defined for rollback",
      "def _maybe_soft_ack" in src and "def soft_ack_text" in src)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
