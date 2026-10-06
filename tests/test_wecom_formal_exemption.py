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

fails = [n for n, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
