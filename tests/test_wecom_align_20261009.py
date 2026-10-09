#!/usr/bin/env python3
"""WeCom gateway alignment with Weixin (2026-10-09) — sandbox tests.

Covers the three changes made to wecom-bot/gateway.py:
(a) a diverted (bridge-lane) message produces NO queue-position
    arrival ack any more (the old bridge ack is gone), whether the
    bridge is busy or idle; it is only registered in feedback_track.
(b) queued -> active transition queues exactly one started notice.
(c) still queued after WAIT_REMIND_SECS queues exactly one wait
    reminder; a merged msgid gets no started notice at all.
(d) a delivered formal reply clears the feedback record (through
    the real dispatch_outbox_item hook), so no notice fires after.
(e) outbound coalescer v2 integration: consecutive "send" rows are
    dispatched as one merged send, a barrier (reply) row flushes
    the buffer first and keeps order, a lone send still goes out
    after the idle gap, results are written per ORIGINAL row id,
    and the outbox offset is fully consumed.

The real gateway module is imported with every state path it uses
redirected into a sandbox (HOME is redirected before import, the
module-level state constants are rebound per scenario, and the one
hardcoded native-bridge spool write in the inbound handler is
intercepted), so no production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_wecom_align_20261009.py
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
SBX = Path("/tmp/wecom-align-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_align", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


# Controllable bridge snapshot (read-only production file replaced
# by an in-memory fake inside this test process only).
FAKE = {"active": [], "queued": []}
wc._bridge_snapshot = lambda channel: (list(FAKE["active"]),
                                       list(FAKE["queued"]))

SPOOL_SBX = SBX / "spool-wecom.jsonl"


def fresh_state(name):
    """Point every module-level state path the gateway uses at a
    fresh sandbox directory; return that directory."""
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


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def send_rows(d):
    """Started / wait notices. Bound reply_notice rows, not free sends."""
    return [r for r in read_jsonl(d / "outbox.jsonl")
            if r.get("mode") == "reply_notice"]


# ---------- source-level premises ----------
src = (ROOT / "wecom-bot" / "gateway.py").read_text(encoding="utf-8")
check("source: bridge ack function gone", "_maybe_bridge_ack" not in src)
check("source: bridge ack template gone", "BRIDGE_ACK_TEMPLATE" not in src)
check("source: cold soft ack kept", "_maybe_soft_ack" in src
      and "soft_ack_text" in src)
check("source: coalescer module loaded", wc.Coalescer is not None)


async def scenario_a():
    d = fresh_state("a")
    if SPOOL_SBX.exists():
        SPOOL_SBX.unlink()
    g = make_gateway()

    def recorder(path, obj):
        # The divert write targets a hardcoded production spool
        # path; reroute it to the sandbox. Everything else uses
        # the (already redirected) module constants.
        if "native-bridge" in str(path):
            wc.append_jsonl_line(SPOOL_SBX, obj)
        else:
            wc.append_jsonl_line(path, obj)

    g.append_jsonl = recorder

    async def fake_respond(req_id, body):
        return {"errcode": 0}

    g.respond = fake_respond

    def frame(msgid, req, text):
        return {"headers": {"req_id": req},
                "body": {"msgid": msgid, "msgtype": "text",
                         "chattype": "single", "chatid": "sirhao",
                         "from": {"userid": "sirhao"},
                         "text": {"content": text}}}

    # Bridge busy with another turn: still NO arrival ack row.
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 5}]
    FAKE["queued"] = []
    await g.handle_message_callback(frame("M-NEW", "req-a1", "新问题来了"))
    check("a: diverted row reached the (sandbox) spool",
          [r.get("msgid") for r in read_jsonl(SPOOL_SBX)] == ["M-NEW"])
    check("a: no outbox rows at all while bridge busy",
          read_jsonl(d / "outbox.jsonl") == [])
    check("a: diverted message not written to cold inbox",
          read_jsonl(d / "inbox.jsonl") == [])
    rec = g.feedback_track.get("M-NEW") or {}
    check("a: registered as queued bridge feedback",
          rec.get("queued") is True and rec.get("via_bridge") is True)

    # Bridge idle: also no ack, and the record is not queued.
    FAKE["active"] = []
    FAKE["queued"] = []
    await g.handle_message_callback(frame("M-IDLE", "req-a2", "空闲时的问题"))
    check("a: no outbox rows while bridge idle",
          read_jsonl(d / "outbox.jsonl") == [])
    check("a: idle-bridge record not marked queued",
          (g.feedback_track.get("M-IDLE") or {}).get("queued") is False)


def scenario_bcd():
    d = fresh_state("bcd")
    g = make_gateway()

    # (b) queued -> active: exactly one started notice.
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 3}]
    FAKE["queued"] = []
    g._register_bridge_feedback("sirhao", "single", "M-Q", "排队的问题")
    FAKE["active"] = [{"msgid": "M-Q", "lane": "main", "secs": 1}]
    FAKE["queued"] = []
    n1 = g._feedback_scan_once(now=time.time() + 5)
    rows = send_rows(d)
    check("b: one started notice queued",
          n1 == 1 and len(rows) == 1
          and rows[0]["content"].startswith("▶️ 排到你了，开始处理")
          and "排队的问题" in rows[0]["content"]
          and rows[0].get("mode") == "reply_notice"
          and rows[0].get("msgid") == "M-Q")
    check("b: started notice addressed by chatid",
          rows and rows[0].get("chatid") == "sirhao")
    check("b: started notice is not an unbound send",
          not any(r.get("mode") == "send"
                  for r in read_jsonl(d / "outbox.jsonl")))
    n2 = g._feedback_scan_once(now=time.time() + 30)
    check("b: second scan sends nothing", n2 == 0 and len(send_rows(d)) == 1)

    # (c) still queued after 180s: exactly one wait reminder.
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 3}]
    FAKE["queued"] = []
    g._register_bridge_feedback("sirhao", "single", "M-W", "等很久的问题")
    queued_at = g.feedback_track["M-W"]["queued_at"]
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 200}]
    FAKE["queued"] = ["M-W"]
    before = len(send_rows(d))
    n3 = g._feedback_scan_once(now=queued_at + 179)
    check("c: no wait reminder before 180s",
          n3 == 0 and len(send_rows(d)) == before)
    n4 = g._feedback_scan_once(now=queued_at + 180)
    rows = send_rows(d)
    wait_rows = [r for r in rows if "还在排队" in r.get("content", "")]
    check("c: one wait reminder at 180s",
          n4 == 1 and len(wait_rows) == 1
          and "原生通道第 1 位" in wait_rows[0]["content"]
          and "已等" in wait_rows[0]["content"])
    n5 = g._feedback_scan_once(now=queued_at + 600)
    check("c: wait reminder not repeated",
          n5 == 0 and len([r for r in send_rows(d)
                           if "还在排队" in r.get("content", "")]) == 1)

    # merged msgid: started notice suppressed for good.
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main", "secs": 3}]
    FAKE["queued"] = []
    g._register_bridge_feedback("sirhao", "single", "M-M", "被并入的补充")
    FAKE["active"] = [{"msgid": "OTHER", "lane": "main"},
                      {"msgid": "M-M", "lane": "main", "merged": True}]
    FAKE["queued"] = []
    before = len(send_rows(d))
    n6 = g._feedback_scan_once(now=time.time() + 5)
    check("merged: no started notice, suppression latched",
          n6 == 0 and len(send_rows(d)) == before
          and g.feedback_track["M-M"]["started_notice"] is True)
    return g, d


async def scenario_d(g, d):
    # (d) a delivered formal reply clears the record via the real
    # dispatch hook; afterwards no scan can fire a notice for it.
    g.reqmap["M-Q"] = {"req_id": "req-q", "stream_id": "stream-q",
                       "chatid": "sirhao", "chattype": "single"}

    async def fake_respond(req_id, body):
        return {"errcode": 0}

    g.respond = fake_respond
    ok = await g.dispatch_outbox_item(
        {"id": "row-q", "mode": "reply", "msgid": "M-Q",
         "content": "正式答复"})
    check("d: reply dispatch ok", ok is True)
    check("d: feedback record cleared by delivered reply",
          "M-Q" not in g.feedback_track)
    FAKE["active"] = []
    FAKE["queued"] = ["M-Q"]
    before = len(send_rows(d))
    n = g._feedback_scan_once(now=time.time() + 1000)
    check("d: no notice after the reply", n == 0
          and len(send_rows(d)) == before)


async def scenario_e():
    d = fresh_state("e")
    g = make_gateway()
    dispatched = []

    async def recorder(item):
        dispatched.append(dict(item))
        return True

    async def no_watchdog():
        return None

    g.dispatch_outbox_item = recorder
    g.check_stream_watchdog = no_watchdog
    rows = [
        {"id": "s1", "mode": "send", "chatid": "sirhao",
         "chat_type": 1, "content": "第一段"},
        {"id": "s2", "mode": "send", "chatid": "sirhao",
         "chat_type": 1, "content": "第二段"},
        {"id": "r1", "mode": "reply", "msgid": "M-B", "content": "答复"},
        {"id": "s3", "mode": "send", "chatid": "sirhao",
         "chat_type": 1, "content": "第三段"},
    ]
    wc.OUTBOX.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
        encoding="utf-8")
    wc.OUTBOX_OFFSET.write_text("0")

    async def run():
        task = asyncio.create_task(g.outbox_loop())
        deadline = time.time() + 10
        while time.time() < deadline and len(dispatched) < 3:
            await asyncio.sleep(0.3)
        task.cancel()

    await run()
    check("e: three dispatches (merged pair, barrier reply, lone send)",
          len(dispatched) == 3)
    if len(dispatched) == 3:
        check("e: consecutive sends merged into one, first id kept",
              dispatched[0].get("mode") == "send"
              and dispatched[0].get("content") == "第一段\n第二段"
              and dispatched[0].get("id") == "s1")
        check("e: barrier reply dispatched after the merged flush",
              dispatched[1].get("mode") == "reply"
              and dispatched[1].get("id") == "r1")
        check("e: lone send released after the idle gap",
              dispatched[2].get("mode") == "send"
              and dispatched[2].get("content") == "第三段")
    results = read_jsonl(d / "outbox_results.jsonl")
    check("e: one result row per original id (merged s2 accounted)",
          any(r.get("id") == "s2" and r.get("ok") is True
              and r.get("merged_into") == "s1" for r in results))
    offset = int(wc.OUTBOX_OFFSET.read_text().strip() or 0)
    check("e: outbox fully consumed (offset == size)",
          offset == wc.OUTBOX.stat().st_size)


async def main():
    await scenario_a()
    g, d = scenario_bcd()
    await scenario_d(g, d)
    await scenario_e()


asyncio.run(main())

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
