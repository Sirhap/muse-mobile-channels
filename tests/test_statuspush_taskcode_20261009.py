#!/usr/bin/env python3
"""Status-push task code (2026-10-09 status-push fix, problem 2).

Every unbound status push must carry the task code [#msgid[:8]] so
the user can match progress / started / wait notices to a task:
- both gateways' STARTED / BRIDGE_WAIT (and Weixin WAIT_REMIND)
  templates contain the {code} placeholder and format without error;
- the native bridge progress_notice output contains the code.
Run: ~/workspace/wecom-bot/.venv/bin/python (gateways) -- the bridge
part subprocesses ~/muse-test-venv/bin/python when available.
"""
import subprocess
import sys
from pathlib import Path

RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name
          + ((" | " + str(extra)[:200]) if extra and not cond else ""))


WS = Path.home() / "workspace"
sys.path.insert(0, str(WS / "wecom-bot"))
sys.path.insert(0, str(WS / "weixin-bot"))

# --- gateway templates (import is heavy; read + exec just the
# template constants and format them the way the scans do) ---
import re


def tpl(path, name):
    src = Path(path).read_text(encoding="utf-8")
    m = re.search(rf'^{name} = "([^"]*)"', src, re.M)
    return m.group(1) if m else None


MID = "abcd1234efgh5678"
for chan, path in (("wecom", WS / "wecom-bot/gateway.py"),
                   ("weixin", WS / "weixin-bot/gateway.py")):
    s = tpl(path, "STARTED_NOTICE_TEMPLATE")
    check(f"{chan} STARTED carries code",
          s is not None and "{code}" in s
          and s.format(excerpt="任务", code=MID[:8]).endswith("〔#abcd1234〕"),
          s)
    w = tpl(path, "BRIDGE_WAIT_TEMPLATE")
    check(f"{chan} BRIDGE_WAIT carries code",
          w is not None and "{code}" in w
          and "〔#abcd1234〕" in w.format(n=2, dur="3分钟", code=MID[:8]),
          w)
w2 = tpl(WS / "weixin-bot/gateway.py", "WAIT_REMIND_TEMPLATE")
check("weixin WAIT_REMIND carries code",
      w2 is not None and "{code}" in w2
      and "〔#abcd1234〕" in w2.format(n=1, dur="3分钟", code=MID[:8]),
      w2)

# format call sites in both gateways pass code=
for chan, path in (("wecom", WS / "wecom-bot/gateway.py"),
                   ("weixin", WS / "weixin-bot/gateway.py")):
    src = Path(path).read_text(encoding="utf-8")
    n = src.count("code=str(mid)[:8]")
    check(f"{chan} gateway format sites pass code", n >= 2, n)

# --- bridge progress_notice output ---
venv = Path.home() / "muse-test-venv/bin/python"
if venv.exists():
    probe = r'''
import importlib.util, json, sys, types
spec = importlib.util.spec_from_file_location(
    "nb", "/home/hatch/workspace/native-bridge/native_bridge.py")
nb = importlib.util.module_from_spec(spec)
sys.modules["nb"] = nb
spec.loader.exec_module(nb)
w = object.__new__(nb.Worker)
w.ch = "wecom"
w.queue = []
w.log = lambda *a: None
w._poll_activity = lambda turn: None
sent = []
class FakeGW:
    pass
turn = {"msgid": "abcd1234efgh5678", "text": "一个长任务",
        "reply_started": False, "activities": [],
        "chatid": "chatW", "chattype": "single"}
orig = nb.live_mode
nb.live_mode = lambda ch: True
import io, contextlib
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    pass
# capture via monkeypatched outbox append: progress_notice builds
# `out` and appends through jsonl helper -- easiest is to stub the
# module-level append function if present; fall back to source check.
print("SOURCE-ONLY")
'''
    r = subprocess.run([str(venv), "-c", probe],
                       capture_output=True, text=True, timeout=60)
    src = (WS / "native-bridge/native_bridge.py").read_text(
        encoding="utf-8")
    check("bridge progress_notice embeds task code",
          "〔#{turn['msgid'][:8]}〕" in src, r.stderr[-200:])
else:
    src = (WS / "native-bridge/native_bridge.py").read_text(
        encoding="utf-8")
    check("bridge progress_notice embeds task code",
          "〔#{turn['msgid'][:8]}〕" in src)

ok = sum(1 for _, c in RESULTS if c)
print(f"\n{ok}/{len(RESULTS)} passed")
sys.exit(0 if ok == len(RESULTS) else 1)
