#!/usr/bin/env python3
"""Bridge queue semantics, stage 1 (todo 31) — strict per-message FIFO.

History: this file was created on 2026-10-08 (build P2) to pin the
merge alignment — the bridge folded a message that arrived mid-turn
into the running turn (MERGE_CHANNELS = weixin/test/wecom) and closed
the merged msgid out gateway-side with _silence_merged (feedback_clear
for weixin, the bound pointer reply "（已并入上一条处理）" for wecom).

Stage 1 of the approved queue redesign turns merging off:
MERGE_CHANNELS is an empty set. The merge machinery stays in the
bridge, unreachable, for rollback during the observation window.
This suite now pins the replacement semantics, end to end through
ChannelWorker.step() against a fake platform:

- a message that arrives while a turn runs stays in the spool/queue
  and is NOT sent into the running turn (no second chat.stream);
- when the running turn closes, _start_next starts it as its OWN
  turn, in arrival order;
- every inbound msgid ends with exactly one bound formal reply
  (mode=reply, msgid-bound), or an explicit terminal receipt (the
  pre-send fallback to the inbox);
- the pointer text "（已并入上一条处理）" is never emitted for a
  bridge turn, and weixin's feedback_clear.jsonl is never written.

The retained merge code is still checked directly (_silence_merged
unit cases) and through one rollback case that restores the old
MERGE_CHANNELS value, so the observation window can end in a revert.

The real bridge module is imported with every channel's bot_state,
BASE, STATE_F, STATUS_F and the pending-answers ledger redirected
into a sandbox, so no production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_bridge_merge_wecom_20261008.py
(the bridge venv — the module imports gw2/muse_cli, which need
curl_cffi from that venv).
"""
import copy
import importlib.util
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-merge-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
WX2 = SBX / "weixin_state_broken"
STATE_DIRS = {"weixin": WX, "wecom": WC}


def reset_sandbox():
    """Wipe every file the bridge could write, keep the directories."""
    for d in (SBX, SBX / "spool", SBX / "shadow", WX, WC, WX2):
        d.mkdir(parents=True, exist_ok=True)
        for f in d.iterdir():
            if f.is_file():
                f.unlink()
    # A directory where the append target should be, so the
    # feedback_clear write fails (rollback fallback scenario).
    (WX2 / "feedback_clear.jsonl").mkdir(exist_ok=True)
    for _flag in ("enabled-weixin", "enabled-wecom"):
        (SBX / _flag).touch()


reset_sandbox()
# fallback_to_inbox records the msgid on the shared pending-answers
# ledger; without this the module default (a production path) is used.
os.environ["PENDING_ANSWERS_PATH"] = str(SBX / "pending_answers.jsonl")

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
# BASE must be sandboxed too (same hole the audit suite hit on
# 2026-10-09). live_mode() reads enabled-<channel> under BASE; with
# the flag absent deliver_reply writes BASE/shadow/<channel>-outbox
# instead of the channel bot_state outbox. The spool, the state
# file and the status file live under BASE as well.
nb.BASE = str(SBX)
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


class FakePlatform:
    """Stand-in for the native gateway: one session, an append-only
    history, and a record of every chat.stream the bridge opens."""

    def __init__(self, reject_text=None):
        self.events = []
        self.streams = []          # [(session_id, text)] in send order
        self.seq = 100
        self.answered = 0
        self.reject_text = reject_text

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "session.start":
            return {"session_id": "sid-1"}
        if method == "chat.history":
            return {"chat_events": list(self.events)}
        if method == "sessions.get":
            sent = len(self.streams)
            return {"status": "completed" if self.answered >= sent
                    else "running"}
        if method == "activity.list":
            return {"days": []}
        raise AssertionError(f"unexpected call {method}")

    def _open(self, method, body=None, **_k):
        assert method == "chat.stream", method
        text = body["items"][0]["text"]
        if self.reject_text and self.reject_text in text:
            raise RuntimeError("platform rejected the send")
        self.streams.append((body["session_id"], text))
        self.seq += 1
        self.events.append({
            "event_name": "message.user", "seq": self.seq,
            "payload": {"display_text": text, "role": "user",
                        "message_id": f"u{self.seq}"}})

    def answer(self, text):
        self.seq += 1
        self.answered += 1
        self.events.append({
            "event_name": "message.assistant", "seq": self.seq,
            "payload": {"display_text": text, "role": "assistant",
                        "message_id": f"a{self.seq}",
                        "status": "completed"}})

    def close(self):
        pass


class World:
    """One channel worker over a fresh sandbox and a fake platform."""

    def __init__(self, ch, reject_text=None):
        reset_sandbox()
        self.ch = ch
        self.platform = FakePlatform(reject_text)
        self.worker = nb.ChannelWorker(ch)
        self.worker._acquire_connection = lambda: (self.platform, None)
        self.spool = SBX / "spool" / f"{ch}.jsonl"
        self.spool.touch()
        self.n = 0

    def arrive(self, msgid, text):
        row = {"msgid": msgid, "text": text, "ts": 1760000000 + self.n}
        if self.ch == "wecom":
            row.update(chatid="testuser", chattype="single")
        else:
            row["from_user"] = "wx-user"
        self.n += 1
        with open(self.spool, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def step(self):
        self.worker.step(str(self.spool), None)

    def cs(self):
        return nb.ch_state(nb.load_state(), self.ch)

    def turn_ids(self):
        """The ids carried by the first running turn ([] when none)."""
        turns = self.cs()["turns"]
        return list(turns[0].get("ids") or []) if turns else []

    def active_ids(self):
        return [t["msgid"] for t in self.cs()["turns"]]

    def queued_ids(self):
        return [r["msgid"] for r, _n in self.worker.queue]

    def outbox(self):
        return read_jsonl(STATE_DIRS[self.ch] / "outbox.jsonl")

    def replies(self):
        return [r for r in self.outbox() if r.get("mode") == "reply"]

    def inbox(self):
        return read_jsonl(STATE_DIRS[self.ch] / "inbox.jsonl")


def sent_texts(world):
    return [t for _sid, t in world.platform.streams]


def user_texts(world):
    """Text of each user message, minus the session preamble."""
    out = []
    for t in sent_texts(world):
        for tag in ("TXT-A", "TXT-B", "TXT-C"):
            if tag in t:
                out.append(tag)
    return out


def no_pointer_anywhere(world):
    return not any(nb.MERGE_POINTER_TEXT in (r.get("content") or "")
                   for r in world.outbox()) \
        and not (STATE_DIRS[world.ch] / "feedback_clear.jsonl").exists()


# 1. Configuration: merging is off everywhere; the machinery that a
#    rollback needs is still defined.
check("stage 1: MERGE_CHANNELS is empty", nb.MERGE_CHANNELS == set())
check("rollback machinery still defined",
      callable(nb.ChannelWorker._merge_into_active)
      and callable(nb.ChannelWorker._silence_merged)
      and nb.MERGE_POINTER_TEXT == "（已并入上一条处理）")
check("wecom NOT in FEEDBACK_CLEAR_CHANNELS",
      "wecom" not in nb.FEEDBACK_CLEAR_CHANNELS)
check("FEEDBACK_CLEAR_CHANNELS == {weixin, test}",
      nb.FEEDBACK_CLEAR_CHANNELS == {"weixin", "test"})

for CH in ("wecom", "weixin"):
    # 2. Arrival mid-turn: B waits in the queue and starts as its own
    #    turn after A closes; each msgid gets its own bound reply.
    w = World(CH)
    w.arrive("m-a", "TXT-A")
    w.step()
    check(f"{CH} FIFO: A starts at once as turn 1",
          w.active_ids() == ["m-a"] and user_texts(w) == ["TXT-A"])
    w.arrive("m-b", "TXT-B")
    w.step()
    w.step()
    check(f"{CH} FIFO: B arriving mid-turn is NOT sent into the "
          "running turn", user_texts(w) == ["TXT-A"])
    check(f"{CH} FIFO: B waits in the queue, A still the only turn",
          w.queued_ids() == ["m-b"] and w.active_ids() == ["m-a"])
    check(f"{CH} FIFO: A's turn carries no merged ids",
          w.turn_ids() == ["m-a"])
    snap = w.cs()["queue_snapshot"]
    check(f"{CH} FIFO: snapshot lists B as queued, no merged flag",
          snap["queued"] == ["m-b"]
          and [a["msgid"] for a in snap["active"]] == ["m-a"]
          and not any(a.get("merged") for a in snap["active"]))
    check(f"{CH} FIFO: nothing delivered for B while A runs",
          w.outbox() == [])
    w.platform.answer("ANS-A")
    w.step()
    check(f"{CH} FIFO: A's reply is bound to A alone",
          [(r["msgid"], r["content"]) for r in w.replies()]
          == [("m-a", "ANS-A")])
    check(f"{CH} FIFO: when A closes, B starts as its OWN turn",
          w.active_ids() == ["m-b"] and w.queued_ids() == []
          and user_texts(w) == ["TXT-A", "TXT-B"])
    check(f"{CH} FIFO: B's own turn is a fresh baseline",
          w.turn_ids() == ["m-b"])
    w.platform.answer("ANS-B")
    w.step()
    check(f"{CH} FIFO: replies bound 1:1 by msgid, in arrival order",
          [(r["msgid"], r["content"]) for r in w.replies()]
          == [("m-a", "ANS-A"), ("m-b", "ANS-B")])
    check(f"{CH} FIFO: no active turn and no queue at the end",
          w.active_ids() == [] and w.queued_ids() == []
          and w.cs()["processed"] == 2)
    check(f"{CH} FIFO: no pointer text, no feedback_clear",
          no_pointer_anywhere(w))

    # 3. Two queued messages behind a running turn: three turns, in
    #    arrival order, one at a time.
    w = World(CH)
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.arrive("m-c", "TXT-C")
    w.step()
    check(f"{CH} FIFO x2: both later messages queue, none sent",
          w.queued_ids() == ["m-b", "m-c"]
          and user_texts(w) == ["TXT-A"] and w.active_ids() == ["m-a"])
    w.platform.answer("ANS-A")
    w.step()
    check(f"{CH} FIFO x2: after A only B starts, C keeps waiting",
          w.active_ids() == ["m-b"] and w.queued_ids() == ["m-c"]
          and user_texts(w) == ["TXT-A", "TXT-B"])
    w.step()
    check(f"{CH} FIFO x2: C is not folded into B's running turn",
          user_texts(w) == ["TXT-A", "TXT-B"]
          and w.turn_ids() == ["m-b"])
    w.platform.answer("ANS-B")
    w.step()
    check(f"{CH} FIFO x2: after B closes C starts as its own turn",
          w.active_ids() == ["m-c"] and w.queued_ids() == []
          and user_texts(w) == ["TXT-A", "TXT-B", "TXT-C"])
    w.platform.answer("ANS-C")
    w.step()
    check(f"{CH} FIFO x2: three turns, three bound replies, in order",
          [(r["msgid"], r["content"]) for r in w.replies()]
          == [("m-a", "ANS-A"), ("m-b", "ANS-B"), ("m-c", "ANS-C")])
    check(f"{CH} FIFO x2: exactly one reply per msgid",
          sorted(r["msgid"] for r in w.replies())
          == ["m-a", "m-b", "m-c"])
    check(f"{CH} FIFO x2: no pointer text, no feedback_clear",
          no_pointer_anywhere(w))

    # 4. Terminal receipt: a queued message whose own send is refused
    #    before it leaves (pre-send failure) takes the inbox
    #    fallback — an explicit terminal path, not a pointer reply —
    #    and the next queued message still gets its own turn.
    w = World(CH, reject_text="TXT-B")
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.arrive("m-c", "TXT-C")
    w.step()
    w.platform.answer("ANS-A")
    w.step()
    check(f"{CH} fallback: refused B goes to the inbox with its msgid",
          [r["msgid"] for r in w.inbox()] == ["m-b"]
          and w.inbox()[0].get("via") == "native-bridge-fallback")
    check(f"{CH} fallback: B has no bridge reply and no pointer",
          [r["msgid"] for r in w.replies()] == ["m-a"]
          and no_pointer_anywhere(w))
    w.step()
    check(f"{CH} fallback: C still starts as its own turn after B",
          w.active_ids() == ["m-c"] and user_texts(w) == ["TXT-A", "TXT-C"])
    w.platform.answer("ANS-C")
    w.step()
    check(f"{CH} fallback: A and C answered once each, B fell back once",
          sorted(r["msgid"] for r in w.replies()) == ["m-a", "m-c"]
          and len(w.inbox()) == 1)

# 5. Rollback proof: restoring the old MERGE_CHANNELS value revives
#    the retained merge path (observation-window escape hatch).
nb.MERGE_CHANNELS = {"weixin", "test", "wecom"}
try:
    w = World("wecom")
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.step()
    check("rollback: with MERGE_CHANNELS restored, B merges into A's turn",
          user_texts(w) == ["TXT-A", "TXT-B"]
          and w.turn_ids() == ["m-a", "m-b"]
          and w.queued_ids() == [])
    w.platform.answer("ANS-AB")
    w.step()
    check("rollback: wecom merged msgid closed with the pointer reply",
          [(r["msgid"], r["content"]) for r in w.replies()]
          == [("m-a", "ANS-AB"), ("m-b", nb.MERGE_POINTER_TEXT)])
finally:
    nb.MERGE_CHANNELS = set()
check("rollback restore undone: MERGE_CHANNELS empty again",
      nb.MERGE_CHANNELS == set())

# 6. The retained _silence_merged routing, called directly (the code
#    is unreachable from step() but must stay correct for rollback).
reset_sandbox()
wc_worker = nb.ChannelWorker("wecom")
wc_worker._silence_merged("wc-merged-1")
check("silence_merged wecom: no feedback_clear.jsonl written",
      not (WC / "feedback_clear.jsonl").exists())
wc_rows = read_jsonl(WC / "outbox.jsonl")
check("silence_merged wecom: bound reply with pointer text",
      len(wc_rows) == 1 and wc_rows[0].get("mode") == "reply"
      and wc_rows[0].get("msgid") == "wc-merged-1"
      and wc_rows[0].get("content") == nb.MERGE_POINTER_TEXT)

wx_worker = nb.ChannelWorker("weixin")
wx_worker._silence_merged("wx-merged-1")
check("silence_merged weixin: feedback_clear.jsonl has the msgid",
      read_jsonl(WX / "feedback_clear.jsonl") == [{"msgid": "wx-merged-1"}])
check("silence_merged weixin: no outbox row for merged msgid",
      read_jsonl(WX / "outbox.jsonl") == [])

saved_wx = nb.CFG["channels"]["weixin"]["bot_state"]
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX2)
try:
    wx_worker._silence_merged("wx-merged-2")
finally:
    nb.CFG["channels"]["weixin"]["bot_state"] = saved_wx
wx2_rows = read_jsonl(WX2 / "outbox.jsonl")
check("silence_merged weixin fallback: pointer reply on write failure",
      len(wx2_rows) == 1 and wx2_rows[0].get("mode") == "reply"
      and wx2_rows[0].get("msgid") == "wx-merged-2"
      and wx2_rows[0].get("content") == nb.MERGE_POINTER_TEXT)

# 7. Gateway premises the wecom routing above relies on (and that the
#    watchdog test file pins in behaviour).
wcgw = (ROOT / "wecom-bot" / "gateway.py").read_text(encoding="utf-8")
check("premise: wecom gateway has no feedback_clear consumer",
      "feedback_clear" not in wcgw)
check("premise: wecom stream watchdog exists",
      "check_stream_watchdog" in wcgw)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
