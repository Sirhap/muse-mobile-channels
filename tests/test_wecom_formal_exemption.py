#!/usr/bin/env python3
"""WeCom formal-reply dead-letter exemption — sandbox tests.

Mirrors the Weixin rule (user decision 2026-10-06): reply and
reply_file rows are NEVER dead-lettered by the WeCom outbox loop;
they keep backoff retries indefinitely. Other modes dead-letter
at SEND_MAX_ATTEMPTS as before. The real gateway module is
imported with state paths redirected to a sandbox and dispatch
stubbed to always fail transiently.
"""
import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

SBX = Path("/tmp/wcformal-sbx")
SBX.mkdir(parents=True, exist_ok=True)
os.environ["HOME"] = str(SBX)

GW_PATH = os.environ.get(
    "MUSE_WECOM_GATEWAY_PY",
    str(Path(__file__).resolve().parent.parent
        / "wecom-bot" / "gateway.py"))
spec = importlib.util.spec_from_file_location("wcgw", GW_PATH)
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def run_scenario(rows, watch_id, min_attempts):
    import shutil
    loop_dir = SBX / "loop"
    shutil.rmtree(loop_dir, ignore_errors=True)
    loop_dir.mkdir(parents=True)
    wc.OUTBOX = loop_dir / "outbox.jsonl"
    wc.OUTBOX_OFFSET = loop_dir / "outbox.offset"
    wc.OUTBOX_RESULTS = loop_dir / "outbox_results.jsonl"
    wc.OUTBOX_RETRY = loop_dir / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = loop_dir / "outbox_partial.json"
    wc.OUTBOX_PARKED = loop_dir / "outbox_parked.json"
    wc.SEND_MAX_ATTEMPTS = 3
    wc.retry_backoff_secs = lambda attempt: 0.05

    g = wc.Gateway.__new__(wc.Gateway)

    async def always_fail(item):
        return False

    async def no_watchdog():
        return None

    g.dispatch_outbox_item = always_fail
    g.check_stream_watchdog = no_watchdog

    wc.OUTBOX.write_text("".join(json.dumps(r) + "\n" for r in rows))
    wc.OUTBOX_OFFSET.write_text("0")

    async def run():
        task = asyncio.create_task(g.outbox_loop())
        deadline = time.time() + 45
        while time.time() < deadline:
            await asyncio.sleep(0.4)
            if wc.OUTBOX_RETRY.exists():
                retry = json.loads(wc.OUTBOX_RETRY.read_text())
                if int((retry.get(watch_id) or {}).get("n") or 0) \
                        >= min_attempts:
                    break
        task.cancel()

    asyncio.run(run())
    results = []
    if wc.OUTBOX_RESULTS.exists():
        results = [json.loads(l) for l in
                   wc.OUTBOX_RESULTS.read_text().splitlines() if l.strip()]
    retry = {}
    if wc.OUTBOX_RETRY.exists():
        retry = json.loads(wc.OUTBOX_RETRY.read_text())
    return results, retry


# A: send row dead-letters; reply row never does
results, retry = run_scenario(
    [{"id": "sendrow", "mode": "send", "chatid": "sirhao",
      "chat_type": 1, "content": "通知"},
     {"id": "replyrow", "mode": "reply", "msgid": "M9",
      "content": "答复"}],
    "replyrow", 6)
dl = [r for r in results if r.get("deadletter")]
check("send row dead-lettered at cap",
      any(r["id"] == "sendrow" for r in dl))
check("reply row NEVER dead-lettered",
      not any(r["id"] == "replyrow" for r in dl))
check("reply row kept retrying (>=6)",
      int((retry.get("replyrow") or {}).get("n") or 0) >= 6)

# B: reply_file row never dead-letters either
results, retry = run_scenario(
    [{"id": "filerow", "mode": "reply_file", "msgid": "M8",
      "file_path": "/tmp/x.png"}],
    "filerow", 6)
dl = [r for r in results if r.get("deadletter")]
check("reply_file row NEVER dead-lettered", not dl)
check("reply_file row kept retrying (>=6)",
      int((retry.get("filerow") or {}).get("n") or 0) >= 6)

# C: park-and-continue — a stuck formal head no longer blocks later rows
def run_park_scenario():
    import shutil
    loop_dir = SBX / "loop"
    shutil.rmtree(loop_dir, ignore_errors=True)
    loop_dir.mkdir(parents=True)
    wc.OUTBOX = loop_dir / "outbox.jsonl"
    wc.OUTBOX_OFFSET = loop_dir / "outbox.offset"
    wc.OUTBOX_RESULTS = loop_dir / "outbox_results.jsonl"
    wc.OUTBOX_RETRY = loop_dir / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = loop_dir / "outbox_partial.json"
    wc.OUTBOX_PARKED = loop_dir / "outbox_parked.json"
    wc.SEND_MAX_ATTEMPTS = 10
    wc.retry_backoff_secs = lambda attempt: 0.05

    g = wc.Gateway.__new__(wc.Gateway)
    dispatched = []
    dispatched_contents = []

    async def fail_only_stuck(item):
        dispatched.append(item.get("id"))
        dispatched_contents.append(item.get("content", ""))
        return item.get("id") != "stuckreply"

    async def no_watchdog():
        return None

    g.dispatch_outbox_item = fail_only_stuck
    g.check_stream_watchdog = no_watchdog
    rows = [
        {"id": "stuckreply", "mode": "reply", "msgid": "M10",
         "content": "卡住的"},
        {"id": "laterow", "mode": "send", "chatid": "sirhao",
         "chat_type": 1, "content": "后面的"},
        {"id": "thirdrow", "mode": "send", "chatid": "sirhao",
         "chat_type": 1, "content": "第三条"},
    ]
    wc.OUTBOX.write_text("".join(json.dumps(r) + "\n" for r in rows))
    wc.OUTBOX_OFFSET.write_text("0")

    async def run():
        task = asyncio.create_task(g.outbox_loop())
        await asyncio.sleep(6)
        task.cancel()

    asyncio.run(run())
    off = int(wc.OUTBOX_OFFSET.read_text())
    parked = json.loads(wc.OUTBOX_PARKED.read_text())
    retry = json.loads(wc.OUTBOX_RETRY.read_text())
    return off, dispatched, parked, retry, dispatched_contents


off, dispatched, parked, retry, dispatched_contents = run_park_scenario()
check("main queue fully consumed despite stuck head",
      off >= wc.OUTBOX.stat().st_size)
# Updated 2026-10-09 (coalescer v2 integrated): the two "send" rows
# behind the stuck head now merge into ONE dispatch under the first
# row's id, so "thirdrow" no longer appears as its own dispatch —
# what matters is that both contents were delivered promptly.
check("rows behind the stuck head were dispatched promptly",
      "laterow" in dispatched
      and any("第三条" in c for c in dispatched_contents))
check("stuck formal row is parked with payload, not dropped",
      "stuckreply" in parked
      and int(parked["stuckreply"].get("n") or 0) >= 2
      and parked["stuckreply"]["item"]["id"] == "stuckreply")
check("parked state survives a fresh instance (restart-proof)",
      "stuckreply" in wc.Gateway._load_parked())
check("delivered rows leave no parked/retry residue",
      "laterow" not in parked and "laterow" not in retry)

# D: soft ack — position counts only the genuinely waiting, and a
# reply_file row finishes the batch (2026-10-07, spec:
# fixable-fixes-spec-2026-10-07.md)
import shutil
HS2 = SBX / "hookstate"
ST2 = SBX / "gwstate"
shutil.rmtree(HS2, ignore_errors=True)
shutil.rmtree(ST2, ignore_errors=True)
HS2.mkdir(parents=True)
ST2.mkdir(parents=True)
NOW2 = time.time()
(HS2 / "active_batch.json").write_text(json.dumps(
    {"msgids": ["M1"], "since": NOW2}))
(HS2 / "pending.json").write_text(json.dumps({"M1": 1, "M2": 1}))
(ST2 / "outbox.jsonl").write_text("")
ack2 = wc.soft_ack_text(ST2, HS2, "你好")
check("soft ack counts only waiting (2nd, not 3rd)",
      ack2 is not None and "第 2 位" in ack2)
(ST2 / "outbox.jsonl").write_text(json.dumps(
    {"id": "rf1", "mode": "reply_file", "msgid": "M1",
     "queued_at": NOW2}) + "\n")
check("reply_file row ends the batch (no soft ack)",
      wc.soft_ack_text(ST2, HS2, "你好") is None)

# E: late suppression after a delivered reply_file
DRB2 = SBX / "drb"
shutil.rmtree(DRB2, ignore_errors=True)
DRB2.mkdir(parents=True)
wc.OUTBOX = DRB2 / "outbox.jsonl"
wc.OUTBOX_RESULTS = DRB2 / "outbox_results.jsonl"
wc.OUTBOX.write_text(
    json.dumps({"id": "file1", "mode": "reply_file", "msgid": "M7"}) + "\n"
    + json.dumps({"id": "late1", "mode": "update", "msgid": "M7"}) + "\n")
wc.OUTBOX_RESULTS.write_text(json.dumps({"id": "file1", "ok": True}) + "\n")
check("late update suppressed after delivered reply_file",
      wc.Gateway._delivered_reply_before({"id": "late1", "msgid": "M7"}) is True)
wc.OUTBOX_RESULTS.write_text(json.dumps({"id": "file1", "ok": False}) + "\n")
check("failed reply_file does NOT suppress the update",
      wc.Gateway._delivered_reply_before({"id": "late1", "msgid": "M7"}) is False)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
