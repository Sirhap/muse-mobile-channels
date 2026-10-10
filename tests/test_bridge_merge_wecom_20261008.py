#!/usr/bin/env python3
"""WeCom merge alignment (2026-10-08, build P2) — sandbox tests.

The native bridge folds queued messages into the running turn for
MERGE_CHANNELS. A merged msgid must be closed out gateway-side:

- weixin: silently, by appending the msgid to the channel's
  feedback_clear.jsonl, which the Weixin gateway's feedback scan
  consumes (_consume_feedback_clear in weixin-bot/gateway.py);
  a failed write falls back to the bound pointer reply.
- wecom: the WeCom gateway never reads feedback_clear.jsonl
  (it gained a feedback track/scan for started/wait notices on
  2026-10-09, but no clear-file consumption), and every diverted
  message holds an open think stream that only a bound reply
  finishes in place.
  A silently-cleared msgid would hang until the stream watchdog
  (~540s) closes it with a false "taking too long" bubble, so the
  bridge must send the bound pointer reply directly.

The real bridge module is imported with every channel's bot_state
and BASE redirected into a sandbox, so no production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_bridge_merge_wecom_20261008.py
(the bridge venv — the module imports gw2/muse_cli, which need
curl_cffi from that venv).
"""
import copy
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-merge-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
WX2 = SBX / "weixin_state_broken"
for d in (WX, WC, WX2):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
# A directory where the append target should be makes the
# feedback_clear write fail (fallback scenario).
(WX2 / "feedback_clear.jsonl").mkdir(exist_ok=True)

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
# BASE must be sandboxed too (same hole the audit suite hit on
# 2026-10-09, closed the way the progress suite did in 13816b4).
# live_mode() reads enabled-<channel> under BASE. With the flag
# absent, deliver_reply writes BASE/shadow/<channel>-outbox.jsonl
# instead of the channel bot_state outbox. This suite only redirected
# bot_state, so pointer replies never landed in the sandbox outbox:
# 10/13, the three delivery assertions failed. A tree with no
# shadow/ dir raises FileNotFoundError on that write instead. Point
# BASE at the sandbox, mirror the flags the pointer path needs
# (enabled-<channel> present so addressing is live), and provide a
# shadow/ dir. Assertions still read the sandbox bot_state outbox
# and feedback_clear files; live delivery is the behaviour under test.
nb.BASE = str(SBX)
(SBX / "shadow").mkdir(parents=True, exist_ok=True)
for _flag in ("enabled-weixin", "enabled-wecom"):
    (SBX / _flag).touch()

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


# 1. Channel membership: wecom merges now; the silent-close routing
#    still covers only the gateways that consume the file.
check("wecom in MERGE_CHANNELS", "wecom" in nb.MERGE_CHANNELS)
check("weixin+test still in MERGE_CHANNELS",
      {"weixin", "test"} <= nb.MERGE_CHANNELS)
check("wecom NOT in FEEDBACK_CLEAR_CHANNELS",
      "wecom" not in nb.FEEDBACK_CLEAR_CHANNELS)
check("FEEDBACK_CLEAR_CHANNELS == {weixin, test}",
      nb.FEEDBACK_CLEAR_CHANNELS == {"weixin", "test"})

# 2. WeCom merged msgid: pointer bound reply, no feedback_clear file.
wc_worker = nb.ChannelWorker("wecom")
wc_worker._silence_merged("wc-merged-1")
check("wecom: no feedback_clear.jsonl written",
      not (WC / "feedback_clear.jsonl").exists())
wc_rows = read_jsonl(WC / "outbox.jsonl")
check("wecom: exactly one outbox row", len(wc_rows) == 1)
check("wecom: row is bound reply with pointer text",
      len(wc_rows) == 1 and wc_rows[0].get("mode") == "reply"
      and wc_rows[0].get("msgid") == "wc-merged-1"
      and wc_rows[0].get("content") == nb.MERGE_POINTER_TEXT)

# 3. Weixin merged msgid (regression): silent via feedback_clear,
#    no outbox row at all.
wx_worker = nb.ChannelWorker("weixin")
wx_worker._silence_merged("wx-merged-1")
check("weixin: feedback_clear.jsonl has the msgid",
      read_jsonl(WX / "feedback_clear.jsonl") == [{"msgid": "wx-merged-1"}])
check("weixin: no outbox row for merged msgid",
      read_jsonl(WX / "outbox.jsonl") == [])

# 4. Weixin fallback (regression): feedback_clear write fails ->
#    pointer bound reply still closes the record.
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX2)
wx_worker._silence_merged("wx-merged-2")
wx2_rows = read_jsonl(WX2 / "outbox.jsonl")
check("weixin fallback: pointer reply on write failure",
      len(wx2_rows) == 1 and wx2_rows[0].get("mode") == "reply"
      and wx2_rows[0].get("msgid") == "wx-merged-2"
      and wx2_rows[0].get("content") == nb.MERGE_POINTER_TEXT)

# 5. Decision premise (updated 2026-10-09): the WeCom gateway now
#    HAS a feedback scan/track (wecom-align: started/wait notices
#    ported from Weixin), but it still does NOT consume
#    feedback_clear, and a merged msgid's think stream still needs
#    a bound reply to finish in place — so the bridge routing in
#    _silence_merged (pointer reply for wecom) is unchanged. The
#    gateway's scan suppresses the started notice for merged
#    msgids instead (bridge snapshot merged flag).
wcgw = (ROOT / "wecom-bot" / "gateway.py").read_text(encoding="utf-8")
check("premise: wecom gateway has no feedback_clear consumer",
      "feedback_clear" not in wcgw)
check("premise: wecom gateway now has feedback scan/track (2026-10-09)",
      "_feedback_scan_once" in wcgw and "feedback_track" in wcgw)
check("premise: wecom stream watchdog exists (hang would surface)",
      "check_stream_watchdog" in wcgw)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
