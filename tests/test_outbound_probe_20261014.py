#!/usr/bin/env python3
"""Outbound probe sandbox tests (2026-10-14, todo 16).

Plants fake gateway state under /tmp and a fake other-channel CLI
that records sends. Covers: healthy, idle, broken streak (alert via
the other channel, exactly once per episode), short burst (no
alert), streak-then-success (episode closes, no alert), stale
history (>24h, no alert), hourly-cadence streak (span rule).

Run: ~/muse-test-venv/bin/python tests/test_outbound_probe_20261014.py
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBE = ROOT / "channel-restore" / "outbound_probe.py"
SBX = Path("/tmp/outbound-probe-sbx")
RESULTS = []
NOW = 1_800_000_000.0


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def build(name):
    ws = SBX / name
    shutil.rmtree(ws, ignore_errors=True)
    for ch in ("weixin", "wecom"):
        (ws / f"{ch}-bot" / "state").mkdir(parents=True)
        (ws / f"{ch}-bot" / "state" / "inbox.jsonl").write_text(
            json.dumps({"from_user_id": f"owner-{ch}"}) + "\n",
            encoding="utf-8")
        cli = ws / f"{ch}-bot" / ch
        cli.write_text(
            "#!/bin/sh\n"
            f'echo "$@" >> "{ws}/sent-{ch}.log"\n'
            "exit 0\n", encoding="utf-8")
        cli.chmod(0o755)
    return ws


def set_results(ws, ch, rows):
    p = ws / f"{ch}-bot" / "state" / "outbox_results.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in rows),
                 encoding="utf-8")


def run(ws, state_name="state.json"):
    env = dict(os.environ)
    r = subprocess.run(
        [sys.executable, str(PROBE), "--ws", str(ws),
         "--state", str(ws / state_name), "--now", str(NOW)],
        capture_output=True, text=True, env=env)
    return r.stdout + r.stderr


def ok(ts):
    return {"id": f"o{ts}", "ok": True, "ts": ts, "errmsg": ""}


def fail(ts, ret=-2):
    return {"id": f"f{ts}", "ok": False, "ts": ts, "ret": ret,
            "errmsg": "prepare failed"}


def sent_lines(ws, ch):
    p = ws / f"sent-{ch}.log"
    return p.read_text(encoding="utf-8").splitlines() if p.exists() else []


# T1 healthy: newest is a success
ws = build("t1")
set_results(ws, "weixin", [fail(NOW - 5000), ok(NOW - 100)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T1 healthy channel: OK, no alert",
      "weixin OK" in out and sent_lines(ws, "wecom") == [])

# T2 idle: no rows at all
ws = build("t2")
set_results(ws, "weixin", [])
set_results(ws, "wecom", [])
out = run(ws)
check("T2 idle channels: OK, no alert",
      out.count("OK") == 2 and sent_lines(ws, "weixin") == [])

# T3 broken streak: 6 failures spanning 300s (count rule), alert once
ws = build("t3")
set_results(ws, "weixin",
            [ok(NOW - 90000)] + [fail(NOW - 1000 + i * 60)
                                 for i in range(6)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T3 broken: alert sent via wecom exactly once",
      "alert=sent via wecom" in out and len(sent_lines(ws, "wecom")) == 1)
check("T3 alert text names the channel and error",
      "weixin" in sent_lines(ws, "wecom")[0]
      and "prepare failed" in sent_lines(ws, "wecom")[0])
out = run(ws)
check("T3 second run: already-sent, no duplicate",
      "alert=already-sent" in out and len(sent_lines(ws, "wecom")) == 1)

# T4 short burst: 3 failures in 20s from one message's retries
ws = build("t4")
set_results(ws, "weixin", [fail(NOW - 30), fail(NOW - 20), fail(NOW - 10)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T4 quick burst of 3: not yet an outage",
      "weixin OK" in out and sent_lines(ws, "wecom") == [])

# T5 streak then success: episode closed by the success
ws = build("t5")
set_results(ws, "weixin",
            [fail(NOW - 5000 + i * 300) for i in range(6)]
            + [ok(NOW - 100)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T5 recovered streak: OK, no alert",
      "weixin OK" in out and sent_lines(ws, "wecom") == [])

# T6 stale history: big streak but newest failure 3 days old
ws = build("t6")
old = NOW - 3 * 86400
set_results(ws, "weixin", [fail(old + i * 300) for i in range(8)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T6 stale streak (>24h): quiet",
      "weixin OK" in out and sent_lines(ws, "wecom") == [])

# T7 hourly cadence: only 3 failures but spanning 2h (span rule)
ws = build("t7")
set_results(ws, "weixin",
            [fail(NOW - 7300), fail(NOW - 3700), fail(NOW - 100)])
set_results(ws, "wecom", [ok(NOW - 50)])
out = run(ws)
check("T7 hourly-cadence streak: alert via span rule",
      "alert=sent via wecom" in out and len(sent_lines(ws, "wecom")) == 1)

failed = [n for n, okk in RESULTS if not okk]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
