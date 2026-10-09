#!/usr/bin/env python3
"""Progress visibility (2026-10-08, build P3) — sandbox tests.

Covers the native-bridge P3 changes:
  1. poll_turn cross-checks sessions.get status against history,
     conservatively (history reply = authoritative; two consecutive
     status=completed with no history reply = abnormal end closed
     out with a best-effort delivery; status failure = pure history).
  2. progress_notice carries a phase word (思考中 / 已开始生成),
     the queued-behind count, and the /stop hint at escalation.
  3. activity.list events are collected per turn (time window +
     (timestamp, message_id, type) dedup) and folded into the
     progress notice as a neutral 最近动态 line; failures silent.
  4. queue_snapshot active entries gain phase / last_activity.
  5. PREAMBLE / PREAMBLE_FRESH carry the no-cards rule, and
     deliver_reply strips standalone [[hatch_widget:...]] lines
     while keeping inline mentions.

The real bridge module is imported with channel bot_state, STATE_F
and STATUS_F redirected into a sandbox, and GW replaced by a stub,
so no production state is touched.
Run: ~/muse-test-venv/bin/python tests/test_bridge_progress_20261008.py
"""
import copy
import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-progress-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
for d in (WX, WC, SBX / "spool"):
    d.mkdir(parents=True, exist_ok=True)
    for f in d.iterdir():
        if f.is_file():
            f.unlink()

spec = importlib.util.spec_from_file_location(
    "native_bridge", ROOT / "native-bridge" / "native_bridge.py")
nb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nb)

nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
nb.STATE_F = str(SBX / "state.json")
nb.STATUS_F = str(SBX / "status.json")
# BASE must be sandboxed too (found 2026-10-08 during the LT2
# production rollout): longtask_mode() reads longtask-<channel>
# flag files from BASE, so with the real BASE this suite's section-1
# expectations (gate-off poll_turn semantics) silently depended on
# no production longtask-weixin flag existing. Once that flag was
# created, 1A/1B/1D2/1F failed with correct gate-on trailing
# behaviour. Sandbox BASE from the start so the suite is hermetic.
# The sandbox must mirror the production flags this suite relies
# on: enabled-<channel> present (live addressing in deliver_reply),
# longtask-<channel> absent (gate-off semantics in section 1), and
# a shadow/ dir for the paths BASE-relative helpers can build.
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


def replies_for(msgid):
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "reply" and r.get("msgid") == msgid]


def sends():
    return [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "send"]


class StubGW:
    def __init__(self):
        self.history = []
        self.status = "running"
        self.status_exc = None
        self.activity = {"days": []}
        self.activity_exc = None

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "chat.history":
            return {"chat_events": list(self.history)}
        if method == "sessions.get":
            if self.status_exc:
                raise self.status_exc
            return {"session_id": (path_params or {}).get("id"),
                    "status": self.status}
        if method == "activity.list":
            if self.activity_exc:
                raise self.activity_exc
            return self.activity
        if method == "chat.cancel":
            return {}
        raise AssertionError(f"unexpected call {method}")


def asst(seq, text, status="completed"):
    p = {"display_text": text, "message_id": f"am{seq}"}
    if status is not None:
        p["status"] = status
    return {"event_name": "message.assistant", "seq": seq, "payload": p}


def make_turn(**over):
    t = {"msgid": "m-base", "lane": "main", "session_id": "sid1",
         "baseline": 10, "sent_at": int(time.time()) - 300,
         "text": "帮我做件事", "from_user": "u1", "chatid": "",
         "chattype": "", "next_prog": 0, "ids": ["m-base"],
         "reply_started": False, "sess_status": None,
         "status_completed_seen": False, "activities": [],
         "activity_seen": [], "last_activity_poll": 0,
         "last_activity": None}
    t.update(over)
    t["ids"] = [t["msgid"]]
    return t


def worker_with(stub):
    w = nb.ChannelWorker("weixin")
    w.gw = stub
    return w


# ---------------------------------------------------------------- 1
# A: history reply + status completed -> done on the FIRST poll.
stub = StubGW()
stub.history = [asst(11, "答案A")]
stub.status = "completed"
w = worker_with(stub)
check("1A history reply + status completed -> done first poll",
      w.poll_turn(make_turn(msgid="m-1a")) == "done")
check("1A reply delivered", [r["content"] for r in replies_for("m-1a")]
      == ["答案A"])

# B: history reply + status running -> done (history is authoritative).
stub = StubGW()
stub.history = [asst(11, "答案B")]
stub.status = "running"
w = worker_with(stub)
check("1B history reply + status running -> done",
      w.poll_turn(make_turn(msgid="m-1b")) == "done")
check("1B reply delivered", [r["content"] for r in replies_for("m-1b")]
      == ["答案B"])

# C: no history reply + status running -> running, flag stays clear.
stub = StubGW()
stub.status = "running"
w = worker_with(stub)
turn = make_turn(msgid="m-1c")
check("1C no reply + status running -> running",
      w.poll_turn(turn) == "running")
check("1C completed flag not set", not turn["status_completed_seen"])

# D: no history reply + status completed -> first poll only MARKS,
#    second consecutive completed closes the turn as abnormal end.
stub = StubGW()
stub.status = "completed"
w = worker_with(stub)
turn = make_turn(msgid="m-1d")
check("1D first completed observation -> still running",
      w.poll_turn(turn) == "running")
check("1D completed flag set after first observation",
      turn["status_completed_seen"])
check("1D second consecutive completed -> done (abnormal end)",
      w.poll_turn(turn) == "done")
check("1D no reply fabricated for empty turn", replies_for("m-1d") == [])

# D2: flag set, then history catches up -> normal history delivery.
stub = StubGW()
stub.status = "completed"
w = worker_with(stub)
turn = make_turn(msgid="m-1d2")
check("1D2 first poll marks", w.poll_turn(turn) == "running")
stub.history = [asst(12, "迟到的答案")]
check("1D2 history reply on second poll -> done",
      w.poll_turn(turn) == "done")
check("1D2 reply delivered", [r["content"] for r in replies_for("m-1d2")]
      == ["迟到的答案"])

# E: status completed twice + a status-less assistant text in
#    history (never a completed reply) -> best-effort delivery.
stub = StubGW()
stub.history = [asst(11, "半截回复文本", status=None)]
stub.status = "completed"
w = worker_with(stub)
turn = make_turn(msgid="m-1e")
check("1E first poll running", w.poll_turn(turn) == "running")
check("1E second poll done", w.poll_turn(turn) == "done")
check("1E best-effort text delivered",
      [r["content"] for r in replies_for("m-1e")] == ["半截回复文本"])

# F: sessions.get raising must not affect the turn at all.
stub = StubGW()
stub.status_exc = RuntimeError("boom")
w = worker_with(stub)
turn = make_turn(msgid="m-1f")
check("1F status failure + no reply -> running, no crash",
      w.poll_turn(turn) == "running")
check("1F status failure leaves flag untouched",
      not turn["status_completed_seen"])
stub.history = [asst(11, "纯历史答案")]
check("1F status failure + history reply -> done",
      w.poll_turn(turn) == "done")
check("1F reply delivered", [r["content"] for r in replies_for("m-1f")]
      == ["纯历史答案"])

# ---------------------------------------------------------------- 2+3
def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


SENT = int(time.time()) - 300
ev_old = {"timestamp": iso(SENT - 100), "type": "file_created",
          "message_id": "a0", "details": {"path": "/tmp/x/old.txt"}}
ev_file = {"timestamp": iso(SENT + 10), "type": "file_created",
           "message_id": "a1", "details": {"path": "/tmp/x/report.txt"}}
ev_task = {"timestamp": iso(SENT + 20), "type": "task_running",
           "message_id": "a2", "title": "Ran 2 background tasks",
           "details": {"tasks": [
               {"label": "Starting subagent", "status": "completed",
                "response_preview": "第一行内容\n第二行"}]}}
stub = StubGW()
stub.activity = {"days": [{"activities":
                           [ev_old, ev_file, dict(ev_file), ev_task]}]}
w = worker_with(stub)
turn = make_turn(msgid="m-3a", sent_at=SENT)
w._poll_activity(turn)
texts = [a["text"] for a in turn["activities"]]
check("3 activity: only in-window events collected",
      texts == ["创建了文件 /tmp/x/report.txt",
                "子助手已完成：Starting subagent：第一行内容"])
check("3 activity: last_activity is newest summary",
      turn["last_activity"] == texts[-1])
turn["last_activity_poll"] = 0
w._poll_activity(turn)
check("3 activity: dedup by (timestamp, message_id, type)",
      len(turn["activities"]) == 2)
stub.activity_exc = RuntimeError("down")
turn["last_activity_poll"] = 0
try:
    w._poll_activity(turn)
    act_fail_ok = True
except Exception:
    act_fail_ok = False
check("3 activity: query failure is silent", act_fail_ok)

# Progress notice: phase word + queue count + folded activity line.
(WX / "outbox.jsonl").unlink(missing_ok=True)
w = worker_with(StubGW())
w.queue = [({"msgid": "q1"}, 10), ({"msgid": "q2"}, 10)]
turn = make_turn(msgid="m-2a", from_user="u1")
turn["activities"] = [{"ts": "t", "text": "创建了文件 /tmp/x/report.txt"}]
turn["last_activity_poll"] = int(time.time())
w.progress_notice(turn, 180)
rows = sends()
check("2 notice sent", len(rows) == 1)
body = rows[-1]["content"] if rows else ""
check("2 notice: thinking phase word", "思考中" in body)
check("2 notice: queued-behind count", "后面还排着 2 条" in body)
check("3 notice: activity folded in as 最近动态",
      "最近动态：创建了文件 /tmp/x/report.txt" in body)

w.queue = []
turn["reply_started"] = True
w.progress_notice(turn, 240)
body = sends()[-1]["content"]
check("2 notice: generating phase word", "已开始生成" in body)
check("2 notice: no queue suffix when queue empty", "排着" not in body)

w.progress_notice(turn, 1500)
body = sends()[-1]["content"]
check("2 escalation: keeps /stop hint", "/stop" in body)
check("2 escalation: carries phase word", "已开始生成" in body)

# ---------------------------------------------------------------- 5
CARD_LINE = "渠道内不要使用卡片/选项控件提问；需要用户选择时用纯文本编号列出选项。"
check("5 PREAMBLE has no-cards rule", CARD_LINE in nb.PREAMBLE)
check("5 PREAMBLE_FRESH has no-cards rule", CARD_LINE in nb.PREAMBLE_FRESH)

nb.deliver_reply("weixin", "m-5a",
                 "第一行\n[[hatch_widget:{\"kind\":\"options\"}]]\n"
                 "正文里提到 [[hatch_widget:x]] 要保留\n最后一行")
rows = replies_for("m-5a")
body = rows[0]["content"] if rows else ""
check("5 standalone widget line stripped",
      "[[hatch_widget:{\"kind\"" not in body)
check("5 inline widget mention kept", "[[hatch_widget:x]]" in body)
check("5 surrounding prose kept",
      "第一行" in body and "最后一行" in body)

nb.deliver_reply("weixin", "m-5b", "[[hatch_widget:only]]")
check("5 widget-only reply produces no row", replies_for("m-5b") == [])

# ---------------------------------------------------------------- 4
# Gateway rendering premise: both gateways read the snapshot with
# .get() only (no fixed-schema parsing), so new active fields are
# inert there.
for gw_name in ("weixin-bot", "wecom-bot"):
    src = (ROOT / gw_name / "gateway.py").read_text(encoding="utf-8")
    check(f"4 premise: {gw_name} snapshot access is .get-based",
          "a.get('lane'" in src and "a.get('secs'" in src
          and "a.get(\"msgid\")" in src)

# Snapshot construction through a real step() with a stubbed GW and
# sandboxed BASE/STATE_F.
nb.BASE = str(SBX)
spool = SBX / "spool" / "weixin.jsonl"
spool.write_text("", encoding="utf-8")
stub = StubGW()
stub.status = "running"
w = worker_with(stub)
turn = make_turn(msgid="m-4a", sent_at=int(time.time()) - 65,
                 last_activity="创建了文件 /tmp/x/report.txt")
st = {"channels": {}}
c = nb.ch_state(st, "weixin")
c["turns"] = [turn]
c["last_boundary_ts"] = 0
nb.save_state(st)
w.step(str(spool), None)
snap = nb.load_state()["channels"]["weixin"]["queue_snapshot"]
act = snap.get("active") or []
check("4 snapshot: active entry present", len(act) == 1)
check("4 snapshot: phase thinking before reply starts",
      bool(act) and act[0].get("phase") == "thinking")
check("4 snapshot: last_activity carried through",
      bool(act) and act[0].get("last_activity")
      == "创建了文件 /tmp/x/report.txt")

st = nb.load_state()
st["channels"]["weixin"]["turns"][0]["reply_started"] = True
nb.save_state(st)
w2 = worker_with(stub)
w2.step(str(spool), None)
snap = nb.load_state()["channels"]["weixin"]["queue_snapshot"]
act = snap.get("active") or []
check("4 snapshot: phase generating after reply starts",
      bool(act) and act[0].get("phase") == "generating")

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
