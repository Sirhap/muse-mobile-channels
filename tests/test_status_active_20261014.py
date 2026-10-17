#!/usr/bin/env python3
"""/status active-task visibility (todo 6, 2026-10-14) — sandbox tests.

Gap: /status counted running tasks (2026-10-13 fullcount fix) and
the overview listed bridge/cold rows with an excerpt and a running
duration, but nothing anywhere said WHEN a running task last pushed
or showed a sign of life — a task running silently for 20 minutes
looked the same as one that pushed 10 seconds ago.

Change under test:
- Bridge: ChannelWorker._active_summary() builds a compact
  per-channel summary of running turns [{text<=20 chars, run_secs,
  idle_secs}] (idle = newest of latest activity event, last
  progress notice, last reply), capped at 3 rows; run() folds it
  into status.json through write_status(active={ch: rows}), which
  merges per channel so the workers never clobber each other.
- Gateways (both): _task_overview_text prefers those status.json
  rows for its bridge rows (rendered with "· 最后活动 X前") and
  falls back to the state.json snapshot rendering when the field
  is missing/unusable; cold and detached rows gain the same suffix
  from the newest outbox row bound to the batch (gateway outbox
  tail read, _last_outbound_ts), omitted when the batch never
  pushed.

Bridge half follows test_bridge_progress's sandbox (module loaded
with STATE_F/STATUS_F/BASE redirected). Gateway half follows
test_status_fullcount's harness (HOME redirected, _bridge_snapshot
faked) plus an _read_json_file dispatcher that serves a planted
bridge status.json, so the REAL _bridge_status_active code path
(including its row filter) is exercised.

Run: ~/muse-test-venv/bin/python tests/test_status_active_20261014.py
"""
import copy
import importlib.util
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/status-active-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
# NOTE: HOME is redirected only in the gateway half below, AFTER
# the bridge module is loaded: gw2 locates the muse_cli sources
# via expanduser("~/workspace/reference/muse-cli/src"), so an
# early HOME override breaks the bridge import (the progress
# suite never overrides HOME for this reason).

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_module(tag, rel):
    spec = importlib.util.spec_from_file_location(tag, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


# ------------------------------------------------------------------
# Bridge half
# ------------------------------------------------------------------
nb = load_module("native_bridge_active", "native-bridge/native_bridge.py")
nb.CFG = copy.deepcopy(nb.CFG)
for _ch in ("weixin", "wecom", "test"):
    if _ch in nb.CFG["channels"]:
        nb.CFG["channels"][_ch]["bot_state"] = str(SBX / f"{_ch}_state")
nb.STATE_F = str(SBX / "bridge-state.json")
nb.STATUS_F = str(SBX / "bridge-status.json")
nb.BASE = str(SBX)
(SBX / "shadow").mkdir(parents=True, exist_ok=True)

NOW = time.time()
w = nb.ChannelWorker("weixin")


def turn(text, sent_ago, **over):
    t = {"msgid": over.pop("msgid", "m-x"), "lane": "main",
         "sent_at": int(NOW - sent_ago), "text": text,
         "activities": [], "last_event_prog": 0, "last_reply_at": 0}
    t.update(over)
    return t


# B1: three turns, three different "last sign of life" sources.
w.update_cs(turns=[
    turn("做一个讲解桥接项目的长视频任务并配上字幕封面", 600,
         activities=[{"ts": iso(NOW - 120), "text": "网页搜索"}]),
    turn("短任务乙", 90, last_event_prog=int(NOW - 30)),
    turn("短任务丙", 3600, last_reply_at=int(NOW - 45)),
])
rows = w._active_summary()
check("B1 summary has one row per running turn", len(rows) == 3)
check("B1 activity-event turn: run ~600s, idle ~120s",
      abs(rows[0]["run_secs"] - 600) <= 5
      and abs(rows[0]["idle_secs"] - 120) <= 5)
check("B1 progress-notice turn: idle ~30s from last_event_prog",
      abs(rows[1]["idle_secs"] - 30) <= 5)
check("B1 trailing turn: idle ~45s from last_reply_at",
      abs(rows[2]["idle_secs"] - 45) <= 5)
check("B1 text truncated to 20 chars",
      len(rows[0]["text"]) == 20 and rows[1]["text"] == "短任务乙")

# B2: cap at 3 rows.
w.update_cs(turns=[turn(f"任务{i}", 100 + i) for i in range(5)])
rows = w._active_summary()
check("B2 summary capped at 3 rows", len(rows) == 3
      and rows[0]["text"] == "任务0" and rows[2]["text"] == "任务2")

# B3: no turns -> empty summary (rows vanish with the turns list).
w.update_cs(turns=[])
check("B3 no turns -> empty summary", w._active_summary() == [])

# B4: unparseable activity ts falls back to sent_at (idle == run).
w.update_cs(turns=[turn("任务丁", 200,
                        activities=[{"ts": "not-a-time", "text": "x"}])])
rows = w._active_summary()
check("B4 bad activity ts: idle falls back to run age",
      abs(rows[0]["idle_secs"] - rows[0]["run_secs"]) <= 2)
w.update_cs(turns=[])

# B5: write_status merges per-channel active without clobbering.
nb.write_status({"pid": 4242}, active={"weixin": [{"text": "甲",
                                                    "run_secs": 10,
                                                    "idle_secs": 5}]})
nb.write_status({}, active={"wecom": [{"text": "乙", "run_secs": 20,
                                        "idle_secs": 7}]})
st = json.loads(Path(nb.STATUS_F).read_text(encoding="utf-8"))
check("B5 write_status keeps both channels' active rows",
      st["active"]["weixin"][0]["text"] == "甲"
      and st["active"]["wecom"][0]["text"] == "乙")
check("B5 write_status keeps extra keys and fresh ts",
      st.get("pid") == 4242 and abs(st.get("ts", 0) - time.time()) < 10)

# ------------------------------------------------------------------
# Gateway half (both gateways, fullcount-style harness)
# ------------------------------------------------------------------
os.environ["HOME"] = str(SBX)
FAKE_SNAP = {"active": [], "queued": []}
PLANTED = {"status": {}}

for tag, rel, chan, hook_name in (
    ("wcgw_active", "wecom-bot/gateway.py", "wecom", "wecom-bot"),
    ("wxgw_active", "weixin-bot/gateway.py", "weixin", "weixin-bot"),
):
    gw = load_module(tag, rel)
    gw._bridge_snapshot = lambda channel, _f=FAKE_SNAP: (
        list(_f["active"]), list(_f["queued"]))
    real_read = gw._read_json_file

    def dispatch(path, default=None, _real=real_read):
        if "native-bridge/status.json" in str(path):
            return copy.deepcopy(PLANTED["status"])
        return _real(path, default)

    gw._read_json_file = dispatch

    base = SBX / tag
    state_dir = base / "state"
    hook_dir = base / "hooks" / "state" / hook_name
    state_dir.mkdir(parents=True)
    hook_dir.mkdir(parents=True)
    (state_dir / "status.json").write_text(json.dumps(
        {"state": "connected", "connected": True}), encoding="utf-8")
    with open(state_dir / "inbox.jsonl", "w", encoding="utf-8") as f:
        for mid, txt in {"C1": "冷通道任务一", "D1": "后台长任务"}.items():
            f.write(json.dumps({"msgid": mid, "text": txt},
                               ensure_ascii=False) + "\n")
    (hook_dir / "pending.json").write_text("{}", encoding="utf-8")

    def set_batch(batch):
        (hook_dir / "active_batch.json").write_text(
            json.dumps(batch), encoding="utf-8")

    def set_outbox(rows):
        p = state_dir / "outbox.jsonl"
        if rows is None:
            p.unlink(missing_ok=True)
            return
        with open(p, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    def overview():
        return gw._task_overview_text(chan, state_dir, hook_dir)

    set_batch({})
    set_outbox(None)

    # G1: one bridge turn via the status.json summary — all three
    # elements (which / how long / last activity) in one row.
    PLANTED["status"] = {"active": {chan: [
        {"text": "把待办清单移到仓库里", "run_secs": 300,
         "idle_secs": 65}]}}
    ov = overview()
    brow = [l for l in ov.splitlines() if l.startswith("▶ 桥")]
    check(f"{chan} G1 status row renders the bridge task",
          len(brow) == 1 and "把待办清单移到仓库里" in brow[0])
    check(f"{chan} G1 row carries run duration and last activity",
          "5分钟" in brow[0] and "最后活动 1分钟前" in brow[0])

    # G2: three status rows all render; long text truncated by the
    # gateway excerpt (20 chars + …).
    PLANTED["status"] = {"active": {chan: [
        {"text": "甲" * 30, "run_secs": 61, "idle_secs": 10},
        {"text": "乙任务", "run_secs": 120, "idle_secs": 120},
        {"text": "丙任务", "run_secs": 180, "idle_secs": 180}]}}
    ov = overview()
    brow = [l for l in ov.splitlines() if l.startswith("▶ 桥")]
    check(f"{chan} G2 three status rows all render", len(brow) == 3)
    check(f"{chan} G2 long text truncated to 20 chars + ellipsis",
          ("甲" * 20 + "…") in brow[0] and "甲" * 21 not in brow[0])

    # G3: malformed rows are filtered by _bridge_status_active and
    # never crash or render.
    PLANTED["status"] = {"active": {chan: [
        {"bogus": 1}, "junk", None,
        {"text": "正常任务", "run_secs": 30, "idle_secs": 3}]}}
    ov = overview()
    brow = [l for l in ov.splitlines() if l.startswith("▶ 桥")]
    check(f"{chan} G3 malformed rows skipped, valid row renders",
          len(brow) == 1 and "正常任务" in brow[0])

    # G4: status.json without the active field -> snapshot fallback,
    # no last-activity suffix anywhere, no crash.
    PLANTED["status"] = {}
    ov = overview()
    check(f"{chan} G4 missing field falls back without suffix",
          isinstance(ov, str) and "最后活动" not in ov)

    # G5: cold batch — last push from the newest bound outbox row;
    # a newer row for another msgid must not count.
    PLANTED["status"] = {}
    set_batch({"msgids": ["C1"], "since": NOW - 600})
    set_outbox([
        {"mode": "update", "msgid": "C1", "queued_at": NOW - 300},
        {"mode": "update", "msgid": "ZZ", "queued_at": NOW - 5},
        {"mode": "update", "msgid": "C1", "queued_at": NOW - 65},
    ])
    ov = overview()
    crow = [l for l in ov.splitlines() if l.startswith("▶ 冷通道 ")
            ]
    check(f"{chan} G5 cold row gains last-activity from outbox",
          len(crow) == 1 and "最后活动 1分钟前" in crow[0]
          and "冷通道任务一" in crow[0])
    # G5b: batch never pushed -> row renders as before, no suffix.
    set_outbox(None)
    ov = overview()
    crow = [l for l in ov.splitlines() if l.startswith("▶ 冷通道 ")]
    check(f"{chan} G5b cold row without pushes has no suffix",
          len(crow) == 1 and "最后活动" not in crow[0])

    # G6: detached batch — same suffix from its bound outbox rows.
    set_batch({"detached": [{"msgids": ["D1"], "since": NOW - 900}]})
    set_outbox([{"mode": "update", "msgid": "D1",
                 "queued_at": NOW - 125}])
    ov = overview()
    drow = [l for l in ov.splitlines() if "后台续跑" in l]
    check(f"{chan} G6 detached row gains last-activity",
          len(drow) == 1 and "最后活动 2分钟前" in drow[0])
    set_batch({})
    set_outbox(None)
    PLANTED["status"] = {}

    # G7: nothing running anywhere. The running count is its own
    # line (0 条, no minutes) and the queue line has no running clause.
    FAKE_SNAP["active"] = []
    s = gw.slash_status_text("测试", state_dir, hook_dir)
    slines = s.splitlines()
    check(f"{chan} G7 idle status shows zero running tasks",
          slines[1].startswith("网关：")
          and slines[2] == "运行中任务：0 条"
          and slines[3] == "队列：排队 0 条 · 后台续跑 0 批"
          and "最长已跑" not in s
          and "当前无任务在跑" not in s
          and "正在处理" not in s)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  ")
    sys.exit(1)
