#!/usr/bin/env python3
"""Audit removal regression (2026-11-05, user instruction "去掉").

The 2026-10-08 /new exit audit that blocked a fresh-session reply
carrying a >=24-char verbatim fragment of the previous session was
removed. This suite replaces the old blocking expectations:

  1. a /new rotation (session_for fresh path) builds NO fingerprint
     corpus and binds NO audit (fresh_session_id None,
     audit_fingerprints empty);
  2. a fresh-session reply carrying a long verbatim old fragment is
     delivered verbatim, never swapped for a block notice, and
     audit_blocks is not incremented;
  3. ordinary delivery (including [[FILE:]] rows) still works.

The real bridge module is imported with channel bot_state, STATE_F
and STATUS_F redirected into a sandbox; GW is a stub.
Run: ~/muse-test-venv/bin/python tests/test_bridge_audit_20261008.py
"""
import copy
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-audit-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
for d in (WX, WC, SBX / "spool"):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
(SBX / "state.json").unlink(missing_ok=True)

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.load_config()
nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")
nb.BASE = str(SBX)
(SBX / "shadow").mkdir(parents=True, exist_ok=True)
for _flag in ("enabled-weixin", "enabled-wecom"):
    (SBX / _flag).touch()

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    import json
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def replies_for(msgid):
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "reply" and r.get("msgid") == msgid]


def cs():
    return nb.ch_state(nb.load_state(), "weixin")


def set_state(**kw):
    st = nb.load_state()
    c = nb.ch_state(st, "weixin")
    c.update(kw)
    nb.save_state(st)


def usr(seq, text):
    return {"event_name": "message.user", "seq": seq,
            "payload": {"display_text": text, "message_id": f"u{seq}",
                        "role": "user"}}


def asst(seq, text):
    return {"event_name": "message.assistant", "seq": seq,
            "payload": {"display_text": text, "message_id": f"a{seq}",
                        "role": "assistant", "status": "completed"}}


class StubGW:
    def __init__(self):
        self.history_by_sid = {}
        self.new_sids = []

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "session.start":
            return {"session_id": self.new_sids.pop(0)}
        if method == "chat.history":
            sid = (body or {}).get("session_id")
            return {"chat_events": list(self.history_by_sid.get(sid, []))}
        raise AssertionError(f"unexpected call {method}")

    def _open(self, *a, **k):
        raise AssertionError("chat.stream not expected in these tests")


OLD_ASST = "已经记下了你每天早晨空腹服用一粒奥美拉唑并且忌辛辣刺激食物"

# 1: fresh rotation builds no corpus, binds nothing.
set_state(session_id="old-sid", rotate_main=True, fresh_start=True,
          preamble_done=True)
stub = StubGW()
stub.new_sids = ["new-sid-1"]
stub.history_by_sid["old-sid"] = [asst(1, OLD_ASST)]
w = nb.ChannelWorker("weixin")
w.gw = stub
sid, preamble = w.session_for(cs())
check("1 rotation returns the new session id", sid == "new-sid-1")
check("1 fresh rotation uses PREAMBLE_FRESH", preamble == nb.PREAMBLE_FRESH)
c = cs()
check("1 no audit binding after removal",
      c.get("fresh_session_id") is None
      and (c.get("audit_fingerprints") or []) == [])
check("1 no audit helpers remain",
      not hasattr(nb, "audit_reply") and not hasattr(nb, "AUDIT_BLOCK_TEXT"))

# 2: long verbatim old fragment is delivered verbatim, not blocked.
frag = OLD_ASST[:30]
before = cs().get("audit_blocks") or 0
text = f"结论是：{frag}，就这样。"
nb.deliver_reply("weixin", "m-block1", text)
check("2 verbatim old fragment delivered verbatim",
      [r["content"] for r in replies_for("m-block1")] == [text])
check("2 no block counted", cs().get("audit_blocks") == before)

# 3: ordinary delivery still works.
nb.deliver_reply("weixin", "m-plain", "这是新会话里才会出现的独特内容")
check("3 ordinary reply delivered",
      [r["content"] for r in replies_for("m-plain")]
      == ["这是新会话里才会出现的独特内容"])

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
