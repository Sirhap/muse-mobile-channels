#!/usr/bin/env python3
"""Bridge queue semantics — merge ON (official-client steering).

History: this file was created on 2026-10-08 (build P2) to pin the
merge alignment — the bridge folds a message that arrives mid-turn
into the running turn (MERGE_CHANNELS = weixin/test/wecom) and closes
the merged msgid out gateway-side with _silence_merged (feedback_clear
for weixin, the bound pointer reply "（已并入上一条处理）" for wecom).

Todo 31 stage 1 (2026-10-17, user-approved) turned merging OFF for
strict per-message FIFO, to fix missed answers; this suite was
rewritten then to pin FIFO. On 2026-11-06 the user ordered the
official behaviour back ("改成和官方一样", "都改"): MERGE_CHANNELS
is {"weixin", "test", "wecom"} again, and this suite pins the merge
semantics, end to end through ChannelWorker.step() against a fake
platform:

- a message that arrives while a turn runs (reply not started yet)
  IS sent into the running turn (a second chat.stream on the same
  session); the combined answer is bound to the turn's first msgid;
- the merged msgid is closed out gateway-side: silently via
  feedback_clear for weixin, with the bound pointer reply for wecom;
- a message that arrives after the reply started stays queued and
  starts as its own turn when the running one closes;
- every inbound msgid is accounted for exactly once: either the
  combined bound reply or the pointer/feedback_clear close-out.

The merge can be switched off again without a code change by
emptying MERGE_CHANNELS (see the disable proof in section 5).

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
# fmt_event 须从参考 SDK 取：把参考 muse-cli 源码放进 sys.path，
# 这样按文件头写的裸命令也能跑，不依赖外部 PYTHONPATH。
_muse_cli_src = ROOT / "reference" / "muse-cli" / "src"
if _muse_cli_src.is_dir() and str(_muse_cli_src) not in sys.path:
    sys.path.insert(0, str(_muse_cli_src))
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
# 2026-11-05 CFG脚手架修复：11月1日重构后CFG不再随导入加载，
# 须显式加载生产配置再深拷贝覆盖沙箱路径；断言一律未动。

nb.load_config()

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

    def start_reply(self, text):
        """An assistant row with text but no completed status: the
        model has started replying (reply_started) without finishing,
        so the turn stays open and later arrivals must queue."""
        self.seq += 1
        self.events.append({
            "event_name": "message.assistant", "seq": self.seq,
            "payload": {"display_text": text, "role": "assistant",
                        "message_id": f"a{self.seq}"}})

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


def feedback_cleared(world):
    """Msgids the bridge closed out silently via feedback_clear."""
    return [r.get("msgid")
            for r in read_jsonl(STATE_DIRS[world.ch] / "feedback_clear.jsonl")]


# 1. Configuration: merging is ON for every channel (official-client
#    steering, re-enabled 2026-11-06 by user order).
check("merge on: MERGE_CHANNELS == {weixin, test, wecom}",
      nb.MERGE_CHANNELS == {"weixin", "test", "wecom"})
check("merge machinery defined",
      callable(nb.ChannelWorker._merge_into_active)
      and callable(nb.ChannelWorker._silence_merged)
      and nb.MERGE_POINTER_TEXT == "（已并入上一条处理）")
check("wecom NOT in FEEDBACK_CLEAR_CHANNELS",
      "wecom" not in nb.FEEDBACK_CLEAR_CHANNELS)
check("FEEDBACK_CLEAR_CHANNELS == {weixin, test}",
      nb.FEEDBACK_CLEAR_CHANNELS == {"weixin", "test"})

for CH in ("wecom", "weixin"):
    # 2. Arrival mid-turn: B folds into A's running turn (reply not
    #    started yet); one combined reply bound to A, B closed out
    #    gateway-side (pointer for wecom, silent feedback_clear for
    #    weixin).
    w = World(CH)
    w.arrive("m-a", "TXT-A")
    w.step()
    check(f"{CH} merge: A starts at once as turn 1",
          w.active_ids() == ["m-a"] and user_texts(w) == ["TXT-A"])
    w.arrive("m-b", "TXT-B")
    w.step()
    w.step()
    check(f"{CH} merge: B arriving mid-turn IS sent into the running turn",
          user_texts(w) == ["TXT-A", "TXT-B"])
    check(f"{CH} merge: B folds in, queue empty, A still the only turn",
          w.queued_ids() == [] and w.active_ids() == ["m-a"])
    check(f"{CH} merge: A's turn carries merged id m-b",
          w.turn_ids() == ["m-a", "m-b"])
    snap = w.cs()["queue_snapshot"]
    check(f"{CH} merge: snapshot lists no queue, active merged",
          snap["queued"] == []
          and [a["msgid"] for a in snap["active"]] == ["m-a", "m-b"]
          and snap["active"][1].get("merged") is True
          and not snap["active"][0].get("merged"))
    check(f"{CH} merge: nothing delivered for B while A runs",
          w.outbox() == [])
    w.platform.answer("ANS-A")
    w.step()
    if CH == "weixin":
        check(f"{CH} merge: combined reply bound to A alone",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A")])
        check(f"{CH} merge: B closed silently via feedback_clear",
              feedback_cleared(w) == ["m-b"])
    else:
        check(f"{CH} merge: combined reply bound to A, B gets pointer",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A"), ("m-b", nb.MERGE_POINTER_TEXT)])
        check(f"{CH} merge: no feedback_clear written",
              not (STATE_DIRS[CH] / "feedback_clear.jsonl").exists())
    check(f"{CH} merge: no active turn and no queue at the end",
          w.active_ids() == [] and w.queued_ids() == []
          # processed counts turns, not msgids: one turn covered m-a+m-b.
          and w.cs()["processed"] == 1)

    # 3. Two queued messages behind a running turn: both fold into
    #    A's turn; one combined reply, both closed out.
    w = World(CH)
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.arrive("m-c", "TXT-C")
    w.step()
    check(f"{CH} merge x2: both later messages fold in, none queued",
          w.queued_ids() == []
          and user_texts(w) == ["TXT-A", "TXT-B", "TXT-C"]
          and w.active_ids() == ["m-a"])
    check(f"{CH} merge x2: turn carries both merged ids",
          w.turn_ids() == ["m-a", "m-b", "m-c"])
    w.platform.answer("ANS-A")
    w.step()
    if CH == "weixin":
        check(f"{CH} merge x2: combined reply bound to A alone",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A")])
        check(f"{CH} merge x2: both closed via feedback_clear",
              feedback_cleared(w) == ["m-b", "m-c"])
    else:
        check(f"{CH} merge x2: combined reply + pointer for each merged",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A"), ("m-b", nb.MERGE_POINTER_TEXT),
                  ("m-c", nb.MERGE_POINTER_TEXT)])
    check(f"{CH} merge x2: no active turn and no queue at the end",
          w.active_ids() == [] and w.queued_ids() == []
          # One turn covered m-a+m-b+m-c.
          and w.cs()["processed"] == 1)

    # 3b. Queued messages fold into the NEXT turn when it starts.
    # B and C arrive after A's reply has started, so they cannot
    # merge into A and must queue. When A closes and B starts as
    # the new turn, C (still queued, arrived before B's reply
    # started) folds into B's turn at once instead of waiting for
    # a turn of its own — one combined reply for B+C.
    w = World(CH)
    w.arrive("m-a", "TXT-A")
    w.step()
    w.platform.start_reply("ANS-A-PARTIAL")
    w.step()
    check(f"{CH} queue-fold: A reply started, turn still open",
          w.active_ids() == ["m-a"]
          and w.cs()["turns"][0].get("reply_started") is True)
    w.arrive("m-b", "TXT-B")
    w.arrive("m-c", "TXT-C")
    w.step()
    check(f"{CH} queue-fold: B and C queue behind started A, "
          "they do NOT merge into it",
          w.queued_ids() == ["m-b", "m-c"]
          and user_texts(w) == ["TXT-A"]
          and w.turn_ids() == ["m-a"])
    w.platform.answer("ANS-A")
    w.step()
    check(f"{CH} queue-fold: when B starts, C folds into B at once",
          w.active_ids() == ["m-b"] and w.queued_ids() == []
          and w.turn_ids() == ["m-b", "m-c"]
          and user_texts(w) == ["TXT-A", "TXT-B", "TXT-C"])
    w.platform.answer("ANS-B")
    w.step()
    if CH == "weixin":
        check(f"{CH} queue-fold: combined reply bound to B alone",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A"), ("m-b", "ANS-B")])
        check(f"{CH} queue-fold: C closed silently via feedback_clear",
              feedback_cleared(w) == ["m-c"])
    else:
        check(f"{CH} queue-fold: combined reply + pointer for C",
              [(r["msgid"], r["content"]) for r in w.replies()]
              == [("m-a", "ANS-A"), ("m-b", "ANS-B"),
                  ("m-c", nb.MERGE_POINTER_TEXT)])
    check(f"{CH} queue-fold: two turns total (A, then B+C), "
          "no queue left",
          w.active_ids() == [] and w.queued_ids() == []
          and w.cs()["processed"] == 2)

    # 4. Terminal receipt: a queued message whose merge send is refused
    #    AFTER the submission was reserved takes the reconciliation
    #    hold — NOT the inbox fallback. Since d73d311 (reliable
    #    channel core) only errors BEFORE reservation may use the
    #    fallback: once _reserve_submission has persisted the turn,
    #    the send outcome is unknown (the platform may have accepted
    #    it before raising), so the bridge preserves evidence instead
    #    of executing twice. The whole turn parks: A's reply is NOT
    #    delivered while the merge is unreconciled (the turn's
    #    content is uncertain), the next queued message does not
    #    start while the hold stands, and the hold notice goes out
    #    exactly once.
    w = World(CH, reject_text="TXT-B")
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.arrive("m-c", "TXT-C")
    w.step()
    pending = w.cs().get("submission_pending") or {}
    check(f"{CH} fallback: refused merge held for reconciliation, "
          "not the inbox",
          pending.get("phase") == "submission_unknown"
          and (pending.get("row") or {}).get("msgid") == "m-b"
          and w.inbox() == [])
    check(f"{CH} fallback: merge held, A still active, C queued, "
          "no reply yet",
          w.active_ids() == ["m-a"] and w.queued_ids() == ["m-c"]
          and w.replies() == [])
    w.platform.answer("ANS-A")
    w.step()
    check(f"{CH} fallback: A's reply blocked while merge unreconciled",
          w.active_ids() == ["m-a"] and w.replies() == []
          and w.queued_ids() == ["m-c"] and w.inbox() == [])
    check(f"{CH} fallback: hold notice sent once",
          len([r for r in w.outbox() if r.get("mode") == "send"
               and "提交结果尚未确认" in (r.get("content") or "")]) == 1)
    w.step()
    check(f"{CH} fallback: C does not start while B is unreconciled",
          w.active_ids() == ["m-a"] and w.queued_ids() == ["m-c"]
          and w.replies() == [])
    w.step()
    check(f"{CH} fallback: A parked, B held once, C still queued",
          w.active_ids() == ["m-a"]
          and sorted(r["msgid"] for r in w.replies()) == []
          and len(w.inbox()) == 0
          and len([r for r in w.outbox() if r.get("mode") == "send"
                   and "提交结果尚未确认" in (r.get("content") or "")]) == 1
          and w.queued_ids() == ["m-c"])

# 5. Disable proof: emptying MERGE_CHANNELS restores strict FIFO
#    (the todo 31 stage-1 behaviour), so the merge can be switched
#    off again without a code change.
nb.MERGE_CHANNELS = set()
try:
    w = World("wecom")
    w.arrive("m-a", "TXT-A")
    w.step()
    w.arrive("m-b", "TXT-B")
    w.step()
    w.step()
    check("disable: with MERGE_CHANNELS empty, B waits in the queue",
          user_texts(w) == ["TXT-A"] and w.queued_ids() == ["m-b"]
          and w.turn_ids() == ["m-a"])
    w.platform.answer("ANS-A")
    w.step()
    check("disable: when A closes, B starts as its own turn",
          w.active_ids() == ["m-b"] and user_texts(w) == ["TXT-A", "TXT-B"])
finally:
    nb.MERGE_CHANNELS = {"weixin", "test", "wecom"}
check("disable restore undone: MERGE_CHANNELS back to merge set",
      nb.MERGE_CHANNELS == {"weixin", "test", "wecom"})

# 6. The _silence_merged routing, called directly.
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
