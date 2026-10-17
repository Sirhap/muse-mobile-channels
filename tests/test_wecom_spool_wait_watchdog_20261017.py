#!/usr/bin/env python3
"""WeCom message waiting in the bridge spool past the think-stream
watchdog (~540s) — current behaviour, pinned (todo 31, stage 1).

With bridge merging off, a WeCom message that arrives while a turn
runs waits in the spool until that turn closes. A long turn means the
wait can exceed the gateway's think-stream watchdog. This file
documents what the user sees TODAY; it does not change gateway
behaviour and must be revisited when the gateway side of the queue
redesign lands.

What the code does (wecom-bot/gateway.py), as pinned below:

W1  A diverted message whose think stream is still open (no notice has
    finished it) and older than 540s: check_stream_watchdog finishes
    that stream, once, with the "⏱ 这个任务处理得有点久…" bubble and
    forgets the stream. Nothing happens at 539s, nothing repeats on
    later ticks. This is the "taking too long" bubble the user sees
    while the message is still only waiting.
W2  The bridge's bound formal reply for that msgid arrives after the
    watchdog. The watchdog did not mark the stream committed, so
    dispatch first tries to finish the SAME stream id with the answer
    (in-place rewrite when the platform still accepts it). When the
    platform refuses (callback req_id dead) it tries markdown on the
    req_id, then falls back to a proactive aibot_send_msg to the chat,
    so the answer lands as a NEW bubble under the "⏱" text.
W3  Contrast: when the 180s wait reminder (reply_notice) already
    finished the think stream, the stream is committed and forgotten,
    so the watchdog has nothing to finish at 541s — the user sees the
    reminder, never the "taking too long" bubble.

Every gateway state path is rebound under /tmp and the transport is
faked; no live sends, no production state.
Run: ~/muse-test-venv/bin/python tests/test_wecom_spool_wait_watchdog_20261017.py
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
SBX = Path("/tmp/wecom-spool-watchdog-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_spool_watchdog", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

WATCHDOG_TEXT = "⏱ 这个任务处理得有点久，我先把这条收尾。有结果了会另发消息告诉你。"
WATCHDOG_AFTER_SECS = 540

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


FAKE = {"active": [], "queued": []}
wc._bridge_snapshot = lambda channel: (list(FAKE["active"]),
                                       list(FAKE["queued"]))
BUSY = [{"msgid": "RUNNING", "lane": "main", "secs": 3}]


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


class Transport:
    """Records every frame; respond() answers from a script of errcodes
    (default 0), send_frame() always succeeds."""

    def __init__(self, g, respond_codes=()):
        self.responds = []     # [(req_id, body)]
        self.sends = []        # [frame] (proactive aibot_send_msg)
        self.codes = list(respond_codes)

        async def respond(req_id, body, timeout=10.0):
            self.responds.append((req_id, body))
            code = self.codes.pop(0) if self.codes else 0
            return {"errcode": code, "errmsg": "" if code == 0 else "dead"}

        async def send_frame(frame, wait_response=False, timeout=10.0):
            self.sends.append(frame)
            return {"errcode": 0}

        g.respond = respond
        g.send_frame = send_frame


def open_inbound(g, msgid, age_secs):
    """State the inbound callback leaves behind for a diverted message:
    reqmap entry, open think stream, feedback record. `age_secs` backdates
    the stream's start."""
    g.reqmap[msgid] = {"req_id": f"req-{msgid}", "stream_id": f"st-{msgid}",
                       "chatid": "testuser", "chattype": "single"}
    g.open_streams[msgid] = {"req_id": f"req-{msgid}",
                             "stream_id": f"st-{msgid}",
                             "feedback_id": f"fb-{msgid}",
                             "since": time.time() - age_secs}


async def scenario_w1():
    fresh_state("w1")
    g = make_gateway()
    tr = Transport(g)
    FAKE["active"] = []
    FAKE["queued"] = []
    # Bridge snapshot looked idle at arrival (queued=False): no 180s
    # reminder will ever exist for this record.
    g._register_bridge_feedback("testuser", "single", "M-WAIT", "排在长任务后面")
    open_inbound(g, "M-WAIT", WATCHDOG_AFTER_SECS - 1)
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = ["M-WAIT"]
    check("W1 record was registered queued=False (no wait reminder path)",
          g.feedback_track["M-WAIT"]["queued"] is False)
    await g.check_stream_watchdog()
    check("W1 nothing is sent at 539s (still inside the window)",
          tr.responds == [] and "M-WAIT" in g.open_streams)
    g.open_streams["M-WAIT"]["since"] = time.time() - (WATCHDOG_AFTER_SECS + 1)
    await g.check_stream_watchdog()
    check("W1 at 541s exactly one frame is sent on the callback req_id",
          len(tr.responds) == 1 and tr.responds[0][0] == "req-M-WAIT")
    body = tr.responds[0][1] if tr.responds else {}
    stream = body.get("stream", {})
    check("W1 it finishes the think stream in place with the watchdog text",
          body.get("msgtype") == "stream"
          and stream.get("id") == "st-M-WAIT"
          and stream.get("finish") is True
          and stream.get("content") == WATCHDOG_TEXT)
    check("W1 the watchdog bubble promises a separate message later",
          "有结果了会另发消息告诉你" in stream.get("content", ""))
    check("W1 the stream is forgotten after the bubble",
          "M-WAIT" not in g.open_streams)
    await g.check_stream_watchdog()
    await g.check_stream_watchdog()
    check("W1 later ticks never repeat the bubble", len(tr.responds) == 1)
    check("W1 the message is still only waiting: record and queue intact",
          "M-WAIT" in g.feedback_track
          and g.reqinfo("M-WAIT").get("stream_committed") is None)
    check("W1 watchdog does not touch the proactive channel",
          tr.sends == [])


async def reply_after_watchdog(name, respond_codes):
    d = fresh_state(name)
    g = make_gateway()
    tr = Transport(g)
    FAKE["active"] = []
    FAKE["queued"] = []
    g._register_bridge_feedback("testuser", "single", "M-LATE", "久等后才开始的任务")
    open_inbound(g, "M-LATE", WATCHDOG_AFTER_SECS + 1)
    await g.check_stream_watchdog()
    assert len(tr.responds) == 1
    tr.responds.clear()
    tr.codes = list(respond_codes)
    ok = await g.dispatch_outbox_item(
        {"id": f"bridge-{name}", "mode": "reply", "msgid": "M-LATE",
         "content": "迟到的正式答复"})
    results = [json.loads(l) for l in
               (d / "outbox_results.jsonl").read_text(
                   encoding="utf-8").splitlines() if l.strip()]
    return g, tr, ok, results


async def scenario_w2():
    # Platform still accepts the stream finish: in-place rewrite on
    # the SAME stream id (the watchdog did not commit the stream).
    g, tr, ok, results = await reply_after_watchdog("w2a", [])
    first = tr.responds[0] if tr.responds else (None, {})
    check("W2a reply after watchdog first retries the same stream id",
          first[0] == "req-M-LATE"
          and first[1].get("stream", {}).get("id") == "st-M-LATE"
          and first[1]["stream"].get("finish") is True
          and first[1]["stream"].get("content") == "迟到的正式答复")
    check("W2a accepted -> one respond, no proactive send, result ok",
          ok is True and len(tr.responds) == 1 and tr.sends == []
          and results and results[-1].get("ok") is True)
    check("W2a delivery clears the feedback record",
          "M-LATE" not in g.feedback_track)

    # Platform refuses (callback req_id dead): stream finish fails,
    # markdown on the req_id fails, the answer goes out proactively.
    g, tr, ok, results = await reply_after_watchdog("w2b", [40008, 40008])
    kinds = [b.get("msgtype") for _r, b in tr.responds]
    check("W2b dead req_id: stream finish, then markdown on the req_id",
          kinds == ["stream", "markdown"])
    sent = tr.sends[0] if tr.sends else {}
    check("W2b then ONE proactive aibot_send_msg to the chat carries "
          "the answer as a new bubble",
          len(tr.sends) == 1 and sent.get("cmd") == "aibot_send_msg"
          and sent.get("body", {}).get("chatid") == "testuser"
          and "迟到的正式答复" in json.dumps(sent, ensure_ascii=False))
    check("W2b delivered: result ok, record cleared, still one reply row",
          ok is True and results and results[-1].get("ok") is True
          and len([r for r in results if r.get("mode") == "reply"]) == 1
          and "M-LATE" not in g.feedback_track)


async def scenario_w3():
    d = fresh_state("w3")
    g = make_gateway()
    tr = Transport(g)
    FAKE["active"] = list(BUSY)
    FAKE["queued"] = []
    g._register_bridge_feedback("testuser", "single", "M-REM", "真排队的任务")
    rec = g.feedback_track["M-REM"]
    check("W3 registered behind a running turn: queued=True",
          rec["queued"] is True)
    open_inbound(g, "M-REM", 181)
    FAKE["queued"] = ["M-REM"]
    n = g._feedback_scan_once(now=rec["queued_at"] + wc.WAIT_REMIND_SECS)
    rows = [json.loads(l) for l in
            wc.OUTBOX.read_text(encoding="utf-8").splitlines() if l.strip()]
    check("W3 the 180s wait reminder is queued as a bound reply_notice",
          n == 1 and len(rows) == 1 and rows[0]["mode"] == "reply_notice"
          and rows[0]["msgid"] == "M-REM")
    ok = await g.dispatch_outbox_item(rows[0])
    check("W3 the reminder finishes the think stream in place",
          ok is True and len(tr.responds) == 1
          and tr.responds[0][1]["stream"]["id"] == "st-M-REM"
          and tr.responds[0][1]["stream"]["finish"] is True)
    check("W3 stream committed and forgotten by the reminder",
          g.reqinfo("M-REM").get("stream_committed") is True
          and "M-REM" not in g.open_streams)
    tr.responds.clear()
    await g.check_stream_watchdog()
    check("W3 at 541s the watchdog has nothing to finish: no bubble",
          tr.responds == [])


async def main():
    await scenario_w1()
    await scenario_w2()
    await scenario_w3()


asyncio.run(main())

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
