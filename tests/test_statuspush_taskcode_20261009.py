#!/usr/bin/env python3
"""Status-push format guard — REWRITTEN 2026-10-14.

History: the original 2026-10-09 suite asserted the opposite of what
this one asserts. Back then every unbound status push had to carry a
task code 〔#msgid[:8]〕 so the user could match notices to tasks.
That spec was deliberately superseded by the user-approved copy
optimisation (commit fca83df, 2026-10-10, "文案优化：提示简短化+
文件说明去重", evidence ~/workspace/copy-opt-2026-10-10/evidence.md):
all 12 copy items were shortened and the 〔#编号〕 was removed from
started / queue / wait / bridge-progress notices everywhere, along
with the {code} placeholders and the code= call-site arguments.
The old suite went 0/8 from that commit on and was left untouched,
its rewrite (rather than deletion) decided on 2026-10-14 under the
user's "修复待办bug" order.

New spec guarded here (verified against the live sources on
2026-10-14 before writing a single assertion):
- both gateways' STARTED_NOTICE_TEMPLATE is "▶️ 开始处理：「{excerpt}」"
  and formats with excerpt only — no {code}, no 〔#…〕 in the output;
- both gateways' BRIDGE_WAIT_TEMPLATE (and Weixin's
  WAIT_REMIND_TEMPLATE) is "⏳ 排队中（第 {n} 位，已等{dur}）" and
  formats with n/dur only;
- every .format() call site in both gateways matches the new
  signatures (excerpt= for started, n=/dur= for waits) and no
  code= argument survives anywhere;
- the bridge's ChannelWorker.progress_notice (the nb.Worker class
  the old suite probed no longer exists) emits the short forms
  "⏳ 处理中（约 {mins} 分钟）：「{excerpt}」" (normal) and
  "⚠️ 处理约 {mins} 分钟，可能卡住：「{excerpt}」，/stop 终止"
  (escalated), still folds 最近动态， and its CONTENT never carries
  the task code or the raw msgid prefix. (The internal outbox row id
  "bridgeprog-<msgid[:8]>-<mins>" is addressing, not display text,
  and is intentionally left alone.)

Gateway templates/call sites are checked by source reading (import
of the gateway modules is heavy); the bridge part imports the real
native_bridge module with bot_state/BASE/STATE redirected into a
sandbox, mirroring tests/test_bridge_progress_20261008.py, so no
production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_statuspush_taskcode_20261009.py
"""
import copy
import importlib.util
import json
import re
import sys
from pathlib import Path

RESULTS = []


def check(name, cond, extra=""):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name
          + ((" | " + str(extra)[:200]) if extra and not cond else ""))


REPO = Path(__file__).resolve().parents[1]
WECOM_GATEWAY = REPO / "wecom-bot" / "gateway.py"
WEIXIN_GATEWAY = REPO / "weixin-bot" / "gateway.py"
NATIVE_BRIDGE = REPO / "native-bridge" / "native_bridge.py"

MID = "abcd1234efgh5678"
STARTED_EXPECTED = "▶️ 开始处理：「{excerpt}」"
WAIT_EXPECTED = "⏳ 排队中（第 {n} 位，已等{dur}）"


def tpl(path, name):
    src = Path(path).read_text(encoding="utf-8")
    m = re.search(rf'^{name} = "([^"]*)"', src, re.M)
    return m.group(1) if m else None


def format_callsites(src, template_name):
    """Argument strings of every TEMPLATE.format(...) call, found by
    parenthesis balancing (arguments contain nested calls)."""
    out = []
    for m in re.finditer(rf"{template_name}\.format\(", src):
        depth, i = 1, m.end()
        while i < len(src) and depth:
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
            i += 1
        out.append(src[m.end():i - 1])
    return out


def kwargs_of(argstr):
    return re.findall(r"(\w+)\s*=", argstr)


# --- gateway templates: exact new copy, no task code ----------------
for chan, path in (("wecom", WECOM_GATEWAY),
                   ("weixin", WEIXIN_GATEWAY)):
    s = tpl(path, "STARTED_NOTICE_TEMPLATE")
    check(f"{chan} STARTED is short form, no code",
          s == STARTED_EXPECTED and "{code}" not in s and "〔#" not in s
          and s.format(excerpt="任务") == "▶️ 开始处理：「任务」",
          s)
    w = tpl(path, "BRIDGE_WAIT_TEMPLATE")
    check(f"{chan} BRIDGE_WAIT is short form, no code",
          w == WAIT_EXPECTED and "{code}" not in w and "〔#" not in w
          and w.format(n=2, dur="3分钟") == "⏳ 排队中（第 2 位，已等3分钟）",
          w)
w2 = tpl(WEIXIN_GATEWAY, "WAIT_REMIND_TEMPLATE")
check("weixin WAIT_REMIND is short form, no code",
      w2 == WAIT_EXPECTED and "{code}" not in w2 and "〔#" not in w2
      and w2.format(n=1, dur="3分钟") == "⏳ 排队中（第 1 位，已等3分钟）",
      w2)

# --- gateway call sites match the new signatures --------------------
for chan, path in (("wecom", WECOM_GATEWAY),
                   ("weixin", WEIXIN_GATEWAY)):
    src = Path(path).read_text(encoding="utf-8")
    started = format_callsites(src, "STARTED_NOTICE_TEMPLATE")
    check(f"{chan} STARTED call sites pass excerpt only",
          len(started) >= 1
          and all(kwargs_of(a) == ["excerpt"] for a in started),
          started)
    waits = format_callsites(src, "BRIDGE_WAIT_TEMPLATE")
    if chan == "weixin":
        waits += format_callsites(src, "WAIT_REMIND_TEMPLATE")
    check(f"{chan} WAIT call sites pass n/dur only",
          len(waits) >= 1
          and all(sorted(kwargs_of(a)) == ["dur", "n"] for a in waits),
          waits)
    check(f"{chan} gateway has no code= residue",
          "code=str(mid)[:8]" not in src and "code=" not in
          " ".join(started + waits), src.count("code=str(mid)[:8]"))

# --- bridge progress_notice (real module, sandboxed) ----------------
SBX = Path("/tmp/statuspush-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
for d in (WX, WC):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
(SBX / "shadow").mkdir(parents=True, exist_ok=True)
for _flag in ("enabled-weixin", "enabled-wecom"):
    (SBX / _flag).touch()

spec = importlib.util.spec_from_file_location(
    "native_bridge", NATIVE_BRIDGE)
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)
nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")
nb.BASE = str(SBX)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def make_turn(**over):
    t = {"msgid": MID, "text": "一个长任务", "from_user": "u1",
         "chatid": "chatW", "chattype": "single", "activities": [],
         "reply_started": False}
    t.update(over)
    return t


# weixin: normal progress — short form, excerpt kept, no task code
wx = nb.ChannelWorker("weixin")  # nb.Worker was renamed long ago
wx.progress_notice(make_turn(), 180)
rows = read_jsonl(WX / "outbox.jsonl")
body = rows[-1]["content"] if rows else ""
check("bridge weixin progress is short form, no code",
      len(rows) == 1 and rows[-1]["mode"] == "send"
      and body == "⏳ 处理中（约 3 分钟）：「一个长任务」"
      and "〔#" not in body and MID[:8] not in body,
      body)

# weixin: escalation — stuck wording + /stop remedy, still no code
wx.progress_notice(make_turn(), 1500)
body = read_jsonl(WX / "outbox.jsonl")[-1]["content"]
check("bridge weixin escalation is short form, no code",
      body == "⚠️ 处理约 25 分钟，可能卡住：「一个长任务」，/stop 终止"
      and "〔#" not in body and MID[:8] not in body,
      body)

# weixin: activity fold still appends 最近动态 to the same notice.
# Start from an empty outbox (as the other bridge suites do before
# direct progress_notice calls): since the 2026-10-15 dedupe guard,
# a same-rid re-call is a no-op by design, so the fold check must
# not share an outbox with the same-bucket notice above.
(WX / "outbox.jsonl").unlink(missing_ok=True)
wx.progress_notice(make_turn(activities=[{"text": "正在查资料"}]), 180)
body = read_jsonl(WX / "outbox.jsonl")[-1]["content"]
check("bridge progress folds activity, still no code",
      body == "⏳ 处理中（约 3 分钟）：「一个长任务」\n最近动态：正在查资料"
      and "〔#" not in body,
      body)

# wecom: bound reply_notice carries the same short content
wc = nb.ChannelWorker("wecom")
wc.progress_notice(make_turn(), 180)
rows = read_jsonl(WC / "outbox.jsonl")
body = rows[-1]["content"] if rows else ""
check("bridge wecom progress is short form, no code",
      len(rows) == 1 and rows[-1]["mode"] == "reply_notice"
      and rows[-1]["msgid"] == MID
      and body == "⏳ 处理中（约 3 分钟）：「一个长任务」"
      and "〔#" not in body,
      body)

# source guard: progress_notice body itself has no code interpolation
bsrc = NATIVE_BRIDGE.read_text(encoding="utf-8")
fn = bsrc[bsrc.index("def progress_notice"):bsrc.index("def _session_status")]
check("bridge progress_notice source has no 〔# interpolation",
      "〔#" not in fn and "msgid[:8]}〕" not in fn)

ok = sum(1 for _, c in RESULTS if c)
print(f"\n{ok}/{len(RESULTS)} passed")
sys.exit(0 if ok == len(RESULTS) else 1)
