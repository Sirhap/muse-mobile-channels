#!/usr/bin/env python3
"""/status full running count (2026-10-13 fix) — sandbox tests.

Bug: slash_status_text counted only the cold channel's active batch,
and _task_overview_text folded detached (background-resumed) batches
into a queue count instead of listing them as running. Result: while
a detached long task or a native-bridge task was actually running,
/status claimed "当前无任务在跑".

Fix (both gateways): the running line counts cold active + detached
msgids + bridge active (via _bridge_snapshot); the overview lists
each detached batch as a running row, mirroring /queue.

Display: that count is its own line ("运行中任务：N 条"), with
"· 最长已跑约 M 分钟" only when an elapsed figure exists. The queue
line is only the pending count and the detached-batch count.

The gateway modules are imported with HOME redirected into a sandbox;
_bridge_snapshot is replaced by an in-memory fake (the overview's
hardcoded bridge state read stays real, but the assertions below only
cover the detached rows, which come from the sandbox hook state).

Run: ~/workspace/wecom-bot/.venv/bin/python tests/test_status_fullcount_20261013.py
(the gateway venv carries httpx/websockets; muse-test-venv does not)
"""
import importlib.util
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/status-fullcount-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_gateway(tag, rel):
    spec = importlib.util.spec_from_file_location(tag, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


NOW = time.time()
FAKE = {"active": [], "queued": []}

for tag, rel, chan, hook_name in (
    ("wcgw_status", "wecom-bot/gateway.py", "wecom", "wecom-bot"),
    ("wxgw_status", "weixin-bot/gateway.py", "weixin", "weixin-bot"),
):
    gw = load_gateway(tag, rel)
    gw._bridge_snapshot = lambda channel, _f=FAKE: (
        list(_f["active"]), list(_f["queued"]))

    base = SBX / tag
    state_dir = base / "state"
    hook_dir = base / "hooks" / "state" / hook_name
    state_dir.mkdir(parents=True)
    hook_dir.mkdir(parents=True)
    (state_dir / "status.json").write_text(json.dumps(
        {"state": "connected", "connected": True}), encoding="utf-8")
    texts = {"D1": "一条后台长任务", "C1": "冷通道任务一",
             "C2": "冷通道任务二", "M1": "混合冷任务"}
    with open(state_dir / "inbox.jsonl", "w", encoding="utf-8") as f:
        for mid, txt in texts.items():
            f.write(json.dumps({"msgid": mid, "text": txt},
                               ensure_ascii=False) + "\n")
    (hook_dir / "pending.json").write_text("{}", encoding="utf-8")

    def set_batch(batch):
        (hook_dir / "active_batch.json").write_text(
            json.dumps(batch), encoding="utf-8")

    def status():
        return gw.slash_status_text("测试", state_dir, hook_dir)

    def mins_since(since_ts):
        return max(0, round((time.time() - since_ts) / 60))

    # A: detached-only — the reported live case. Elapsed is present,
    # so the running line carries the longest-minutes clause.
    FAKE["active"] = []
    detached_since = NOW - 600
    set_batch({"detached": [{"msgids": ["D1"], "since": detached_since}]})
    s = status()
    slines = s.splitlines()
    check(f"{chan} A detached-only status counts it running",
          slines[2] == (
              f"运行中任务：1 条 · 最长已跑约 {mins_since(detached_since)} 分钟")
          and "当前无任务在跑" not in s
          and "正在处理" not in s)
    check(f"{chan} A queue line drops the running clause",
          slines[1].startswith("网关：")
          and slines[3] == "队列：排队 0 条 · 后台续跑 1 批")
    ov = gw._task_overview_text(chan, state_dir, hook_dir)
    check(f"{chan} A overview lists detached as running row",
          "后台续跑" in ov and "一条后台长任务" in ov)
    check(f"{chan} A overview no longer folds detached into queue",
          "冷通道并行" not in ov)

    # B: bridge-only. secs is already an elapsed duration.
    set_batch({})
    FAKE["active"] = [{"msgid": "B1", "secs": 300}]
    s = status()
    slines = s.splitlines()
    check(f"{chan} B bridge-only status counts it running",
          slines[2] == "运行中任务：1 条 · 最长已跑约 5 分钟"
          and "当前无任务在跑" not in s
          and "正在处理" not in s)
    check(f"{chan} B queue line drops the running clause",
          slines[3] == "队列：排队 0 条 · 后台续跑 0 批")

    # C: cold active regression (two msgids in one batch).
    FAKE["active"] = []
    cold_since = NOW - 120
    set_batch({"msgids": ["C1", "C2"], "since": cold_since})
    s = status()
    slines = s.splitlines()
    check(f"{chan} C cold active still counted",
          slines[2] == (
              f"运行中任务：2 条 · 最长已跑约 {mins_since(cold_since)} 分钟")
          and "当前无任务在跑" not in s
          and "正在处理" not in s)

    # D: genuinely idle — zero running tasks, no minutes clause.
    set_batch({})
    s = status()
    slines = s.splitlines()
    check(f"{chan} D idle shows zero running tasks",
          slines[1].startswith("网关：")
          and slines[2] == "运行中任务：0 条"
          and "最长已跑" not in s
          and "当前无任务在跑" not in s
          and "正在处理" not in s)
    check(f"{chan} D queue line is counts only",
          slines[3] == "队列：排队 0 条 · 后台续跑 0 批")

    # E: mixed cold 1 + detached 2 + bridge 1 = 4. Minutes are the
    # longest elapsed (the detached batch), not the shorter ones.
    mixed_since = NOW - 900
    set_batch({"msgids": ["M1"], "since": NOW - 60,
               "detached": [{"msgids": ["D1", "C1"],
                             "since": mixed_since}]})
    FAKE["active"] = [{"msgid": "B1", "secs": 30}]
    s = status()
    slines = s.splitlines()
    check(f"{chan} E mixed sources sum to 4 running",
          slines[2] == (
              f"运行中任务：4 条 · 最长已跑约 {mins_since(mixed_since)} 分钟")
          and "当前无任务在跑" not in s
          and "正在处理" not in s)
    check(f"{chan} E queue line keeps pending and detached counts",
          slines[3] == "队列：排队 0 条 · 后台续跑 1 批")

    # F: running, but no usable timestamp anywhere — omit minutes
    # (the old text would have shown "?").
    FAKE["active"] = [{"msgid": "B1"}]
    set_batch({"msgids": ["C1"]})
    s = status()
    slines = s.splitlines()
    check(f"{chan} F running without elapsed omits minutes",
          slines[2] == "运行中任务：2 条"
          and "最长已跑" not in s
          and "?" not in slines[2]
          and "当前无任务在跑" not in s
          and "正在处理" not in s)
    check(f"{chan} F queue line drops the running clause",
          slines[3] == "队列：排队 0 条 · 后台续跑 0 批")
    FAKE["active"] = []

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  ")
    sys.exit(1)
