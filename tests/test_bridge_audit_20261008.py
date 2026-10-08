#!/usr/bin/env python3
"""/new exit audit (2026-10-08, build P4) — sandbox tests.

Covers the native-bridge P4 changes:
  1. a /new rotation (session_for fresh path) builds the frozen
     fingerprint corpus from the OLD session's history only (user +
     assistant), stores hashes only (old text never lands in state),
     and binds the audit to the new fresh session id;
  2. a fresh-session reply carrying a >=24-char verbatim old fragment
     is blocked: the fixed notice is delivered instead (no [[FILE:]]
     rows), audit_blocks / audit_last_block are bumped, and the block
     notice itself never participates in the audit;
  3. a reply overlapping the old corpus by <24 chars passes;
  4. a paraphrase (no long verbatim run) passes;
  5. non-fresh sessions (no binding, or binding left behind by an
     auto-rotation) are never audited, even with a corpus present;
  6. a second /new rotation REPLACES the corpus (old fragments pass,
     new-old fragments block);
  7. the corpus cap holds and keeps the newest old-session content.

The real bridge module is imported with channel bot_state, STATE_F
and STATUS_F redirected into a sandbox; GW is a stub.
Run: ~/muse-test-venv/bin/python tests/test_bridge_audit_20261008.py
"""
import copy
import importlib.util
import json
import random
import sys
import time
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

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def replies_for(msgid):
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "reply" and r.get("msgid") == msgid]


def files_for(msgid):
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "reply_file" and r.get("msgid") == msgid]


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


OLD_USER = "我上周去做了胃镜检查医生说慢性胃炎需要连续吃药三个月调理身体"
OLD_ASST = "已经记下了你每天早晨空腹服用一粒奥美拉唑并且忌辛辣刺激食物"
SYS_TEXT = "系统事件独有的说明文字不应该进入旧语料指纹集合里面去吧"
NEW_ONLY = "这是新会话里才会出现的独特内容跟旧会话完全没有关系哦啊嘿"

# ---------------------------------------------------------------- 1
# Rotation on the fresh path builds the corpus from the OLD session.
set_state(session_id="old-sid", rotate_main=True, fresh_start=True,
          preamble_done=True)
stub = StubGW()
stub.new_sids = ["new-sid-1"]
stub.history_by_sid["old-sid"] = [
    usr(1, OLD_USER),
    asst(2, OLD_ASST),
    {"event_name": "session.note", "seq": 3,
     "payload": {"display_text": SYS_TEXT, "message_id": "s3"}},
]
w = nb.ChannelWorker("weixin")
w.gw = stub
sid, preamble = w.session_for(cs())
check("1 rotation returns the new session id", sid == "new-sid-1")
check("1 fresh rotation uses PREAMBLE_FRESH", preamble == nb.PREAMBLE_FRESH)
c = cs()
check("1 audit bound to fresh session id",
      c.get("fresh_session_id") == "new-sid-1"
      and c.get("session_id") == "new-sid-1")
fps = c.get("audit_fingerprints") or []
check("1 corpus non-empty", len(fps) > 0)
check("1 corpus == fingerprints of old user+assistant only",
      set(fps) == nb._audit_windows(OLD_USER) | nb._audit_windows(OLD_ASST))
check("1 non-message events excluded from corpus",
      not (nb._audit_windows(SYS_TEXT) & set(fps)))
check("1 new-session-only text not in corpus",
      not (nb._audit_windows(NEW_ONLY) & set(fps)))
state_raw = (SBX / "state.json").read_text(encoding="utf-8")
check("1 old session raw text NOT persisted in state",
      OLD_USER not in state_raw and OLD_ASST not in state_raw)

# First deployment / no old session: corpus empty, nothing bound.
set_state(session_id=None, rotate_main=False, fresh_start=False,
          fresh_session_id=None, audit_fingerprints=[])
stub2 = StubGW()
stub2.new_sids = ["first-sid"]
w2 = nb.ChannelWorker("weixin")
w2.gw = stub2
sid2, _ = w2.session_for(cs())
c = cs()
check("1b first session: no binding, empty corpus",
      sid2 == "first-sid" and c.get("fresh_session_id") is None
      and (c.get("audit_fingerprints") or []) == [])
# restore the fresh binding for the delivery tests below
set_state(session_id="new-sid-1", fresh_session_id="new-sid-1",
          audit_fingerprints=fps)

# ---------------------------------------------------------------- 2
# Fresh reply with a >=24-char verbatim old fragment is blocked.
frag = OLD_ASST[:30]
check("2 fragment really is >= window", len(frag) >= nb.AUDIT_WINDOW)
before = cs().get("audit_blocks") or 0
nb.deliver_reply("weixin", "m-block1", f"结论是：{frag}，就这样。")
rows = replies_for("m-block1")
check("2 blocked reply delivers ONLY the fixed notice",
      [r["content"] for r in rows] == [nb.AUDIT_BLOCK_TEXT])
check("2 original text nowhere in outbox",
      all(frag not in (r.get("content") or "")
          for r in read_jsonl(WX / "outbox.jsonl")))
c = cs()
check("2 audit_blocks incremented", c.get("audit_blocks") == before + 1)
check("2 audit_last_block stamped", (c.get("audit_last_block") or 0) > 0)

# Blocked replies leak no files either.
leak = SBX / "leak.txt"
leak.write_text("secret", encoding="utf-8")
nb.deliver_reply("weixin", "m-block2",
                 f"{OLD_USER[:28]}\n[[FILE:{leak}]]")
check("2 blocked reply: notice delivered, no reply_file rows",
      [r["content"] for r in replies_for("m-block2")]
      == [nb.AUDIT_BLOCK_TEXT] and files_for("m-block2") == [])

# [[FILE:]] lines and the block notice itself never participate.
check("2 audit ignores fragment inside a [[FILE:]] line",
      nb.audit_reply(f"全新回复内容哦\n[[FILE:/tmp/{frag}]]", fps) == 0)
check("2 audit ignores the block notice itself",
      nb.audit_reply(nb.AUDIT_BLOCK_TEXT, fps) == 0)

# ---------------------------------------------------------------- 3
# Short overlap (<24 chars) passes.
short = OLD_USER[:20] + "，另外我想问问今晚吃什么比较清淡养胃呢"
before = cs().get("audit_blocks") or 0
nb.deliver_reply("weixin", "m-short", short)
check("3 <24-char overlap delivered verbatim",
      [r["content"] for r in replies_for("m-short")] == [short])
check("3 no new block counted", cs().get("audit_blocks") == before)

# ---------------------------------------------------------------- 4
# Paraphrase passes.
para = "关于之前提到的肠胃不舒服的问题，我想再了解一下日常饮食上要注意些什么"
nb.deliver_reply("weixin", "m-para", para)
check("4 paraphrase delivered verbatim",
      [r["content"] for r in replies_for("m-para")] == [para])
check("4 no new block counted", cs().get("audit_blocks") == before)

# ---------------------------------------------------------------- 5
# Non-fresh sessions are not audited.
set_state(fresh_session_id=None)          # ordinary continued session
nb.deliver_reply("weixin", "m-plain", f"复述一下：{frag}")
check("5 unbound session: fragment delivered",
      [r["content"] for r in replies_for("m-plain")]
      == [f"复述一下：{frag}"])
set_state(session_id="auto-rotated-sid",  # fresh session rotated away
          fresh_session_id="new-sid-1")
nb.deliver_reply("weixin", "m-auto", f"复述一下：{frag}")
check("5 auto-rotated away: audit lapsed, fragment delivered",
      [r["content"] for r in replies_for("m-auto")]
      == [f"复述一下：{frag}"])
check("5 no blocks counted for non-fresh",
      cs().get("audit_blocks") == before)

# ---------------------------------------------------------------- 6
# Second /new rotation replaces the corpus wholesale.
NEW_USER2 = "这轮新会话我们商量了搬家安排打算下个月搬到浦东张江附近居住生活"
NEW_ASST2 = "好的搬家清单已经列好包括打包顺序水电过户和宽带迁移的时间点安排"
set_state(session_id="new-sid-1", rotate_main=True, fresh_start=True,
          fresh_session_id="new-sid-1")
stub3 = StubGW()
stub3.new_sids = ["new-sid-2"]
stub3.history_by_sid["new-sid-1"] = [usr(1, NEW_USER2), asst(2, NEW_ASST2)]
w3 = nb.ChannelWorker("weixin")
w3.gw = stub3
sid3, preamble3 = w3.session_for(cs())
c = cs()
fps3 = c.get("audit_fingerprints") or []
check("6 second rotation binds the newer fresh session",
      sid3 == "new-sid-2" and c.get("fresh_session_id") == "new-sid-2"
      and preamble3 == nb.PREAMBLE_FRESH)
check("6 corpus replaced (old corpus fingerprints gone)",
      set(fps3) == nb._audit_windows(NEW_USER2)
      | nb._audit_windows(NEW_ASST2)
      and not (nb._audit_windows(OLD_ASST) & set(fps3)))
nb.deliver_reply("weixin", "m-oldfrag", f"复述一下：{frag}")
check("6 previous corpus fragment now passes",
      [r["content"] for r in replies_for("m-oldfrag")]
      == [f"复述一下：{frag}"])
before6 = cs().get("audit_blocks") or 0
nb.deliver_reply("weixin", "m-newfrag", f"清单：{NEW_ASST2[:30]}")
check("6 new corpus fragment blocked with notice",
      [r["content"] for r in replies_for("m-newfrag")]
      == [nb.AUDIT_BLOCK_TEXT]
      and cs().get("audit_blocks") == before6 + 1)

# ---------------------------------------------------------------- 7
# Corpus cap: newest old-session content wins.
pool = [chr(0x4E00 + i) for i in range(3000)]
big_old = "".join(random.Random(7).choices(pool, k=20000))
newest = "最新一条旧消息的独特短语用于验证上限时优先保留新内容机制"
corpus = nb.build_audit_corpus([asst(1, big_old), usr(2, newest)])
check("7 corpus size capped", len(corpus) == nb.AUDIT_MAX_FINGERPRINTS)
check("7 newest message fingerprints survive the cap",
      nb._audit_windows(newest) <= set(corpus))

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
