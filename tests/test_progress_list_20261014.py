#!/usr/bin/env python3
"""Progress checklist line in the bridge bubble (todo 9, 2026-10-14).

The bridge folds ONE line — 「清单 k/n：当前项」 — into the
existing progress_notice bubble when the executor published a
checklist at progress_lists/<channel>-<msgid>.json under BASE
(written via native-bridge/progress_list.py). Covers:

  1. no list file -> bubble byte-identical to the pre-change text;
  2. list shapes: not started / in progress / all done / empty /
     corrupt JSON / wrong shape -> line present or silently absent;
  3. fold order with the activity line, escalation bubble, long
     item truncation and whitespace collapsing;
  4. step() removes the list file when the turn closes (done), and
     keeps it while the turn is still running;
  5. progress_list.py CLI: set / done / clear round-trip, atomic
     write leaves no tmp residue, bridge line reflects the update.

Same sandbox shape as test_bridge_progress_20261008.py: the real
module is imported with CFG bot_state, STATE_F, STATUS_F and BASE
redirected under /tmp and GW stubbed.
Run: ~/muse-test-venv/bin/python tests/test_progress_list_20261014.py
"""
import copy
import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/progress-list-sbx")
WX = SBX / "weixin_state"
WC = SBX / "wecom_state"
for d in (WX, WC, SBX / "spool", SBX / "progress_lists"):
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


class StubGW:
    def __init__(self):
        self.history = []
        self.status = "running"
        self.activity = {"days": []}

    def call_json(self, method, path_params=None, body=None, query=None,
                  timeout=30):
        if method == "chat.history":
            return {"chat_events": list(self.history)}
        if method == "sessions.get":
            return {"session_id": (path_params or {}).get("id"),
                    "status": self.status}
        if method == "activity.list":
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
         "activity_seen": [], "last_activity_poll": int(time.time()),
         "last_activity": None}
    t.update(over)
    t["ids"] = [t["msgid"]]
    return t


def worker_with(stub):
    w = nb.ChannelWorker("weixin")
    w.gw = stub
    return w


def notice_body(turn, age=180):
    (WX / "outbox.jsonl").unlink(missing_ok=True)
    w = worker_with(StubGW())
    w.progress_notice(turn, age)
    rows = [r for r in read_jsonl(WX / "outbox.jsonl")
            if r.get("mode") == "send"]
    return rows[-1]["content"] if rows else ""


def put_list(msgid, items):
    p = SBX / "progress_lists" / f"weixin-{msgid}.json"
    p.write_text(json.dumps({"items": items}, ensure_ascii=False),
                 encoding="utf-8")
    return p


def put_raw(msgid, raw):
    p = SBX / "progress_lists" / f"weixin-{msgid}.json"
    p.write_text(raw, encoding="utf-8")
    return p


BASE_BODY = "⏳ 处理中（约 3 分钟）：「帮我做件事」"

# ---------------------------------------------------------------- 1
turn = make_turn(msgid="m-nolist")
check("1 no list file -> bubble is the pre-change text, byte-exact",
      notice_body(turn) == BASE_BODY)
check("1 line helper -> None without a file",
      nb._progress_list_line("weixin", "m-nolist") is None)

# ---------------------------------------------------------------- 2
put_list("m-fresh", [{"text": "查资料", "done": False},
                     {"text": "写草稿", "done": False},
                     {"text": "发文件", "done": False}])
turn = make_turn(msgid="m-fresh")
check("2 not started -> 清单 0/3 with first item",
      notice_body(turn) == BASE_BODY + "\n清单 0/3：查资料")

put_list("m-mid", [{"text": "查资料", "done": True},
                   {"text": "写草稿", "done": True},
                   {"text": "配音合成", "done": False},
                   {"text": "发文件", "done": False}])
turn = make_turn(msgid="m-mid")
check("2 in progress -> 清单 2/4 with first unfinished item",
      notice_body(turn) == BASE_BODY + "\n清单 2/4：配音合成")

put_list("m-alldone", [{"text": "查资料", "done": True},
                       {"text": "发文件", "done": True}])
turn = make_turn(msgid="m-alldone")
check("2 all done -> 清单 2/2 ✓ and no item text",
      notice_body(turn) == BASE_BODY + "\n清单 2/2 ✓")

put_list("m-empty", [])
turn = make_turn(msgid="m-empty")
check("2 empty items -> no line", notice_body(turn) == BASE_BODY)

put_raw("m-corrupt", "{not json")
turn = make_turn(msgid="m-corrupt")
check("2 corrupt JSON -> no line, bubble intact",
      notice_body(turn) == BASE_BODY)

put_raw("m-shape1", json.dumps([{"text": "x", "done": False}]))
turn = make_turn(msgid="m-shape1")
check("2 top-level list (no items key) -> no line",
      notice_body(turn) == BASE_BODY)

put_list("m-shape2", [{"done": True}])
turn = make_turn(msgid="m-shape2")
check("2 item without text -> no line", notice_body(turn) == BASE_BODY)

put_raw("m-gone", json.dumps({"items": [{"text": "x", "done": False}]}))
(SBX / "progress_lists" / "weixin-m-gone.json").unlink()
turn = make_turn(msgid="m-gone")
check("2 file deleted mid-flight -> no line",
      notice_body(turn) == BASE_BODY)

# ---------------------------------------------------------------- 3
put_list("m-order", [{"text": "第一步", "done": True},
                     {"text": "第二步", "done": False}])
turn = make_turn(msgid="m-order")
turn["activities"] = [{"ts": "t", "text": "创建了文件 /tmp/x/report.txt"}]
check("3 fold order: main, 最近动态, then 清单",
      notice_body(turn) == BASE_BODY
      + "\n最近动态：创建了文件 /tmp/x/report.txt"
      + "\n清单 1/2：第二步")

turn = make_turn(msgid="m-order")
body = notice_body(turn, age=1500)
check("3 escalation bubble also carries the list line",
      body.startswith("⚠️") and body.endswith("\n清单 1/2：第二步"))

long_item = "这是一个非常长的清单项文本用来验证截断行为是否符合预期上限"
put_list("m-long", [{"text": long_item, "done": False}])
turn = make_turn(msgid="m-long")
line = nb._progress_list_line("weixin", "m-long")
check("3 long item truncated at 24 chars + ellipsis",
      line == "清单 0/1：" + long_item[:24] + "…")

put_list("m-nl", [{"text": "第一行\n第二行  带空格", "done": False}])
line = nb._progress_list_line("weixin", "m-nl")
check("3 item whitespace collapsed to one line",
      line == "清单 0/1：第一行 第二行 带空格")

# ---------------------------------------------------------------- 4
spool = SBX / "spool" / "weixin.jsonl"
spool.write_text("", encoding="utf-8")


def run_step(turn, stub):
    st = {"channels": {}}
    c = nb.ch_state(st, "weixin")
    c["turns"] = [turn]
    c["last_boundary_ts"] = 0
    nb.save_state(st)
    w = worker_with(stub)
    w.step(str(spool), None)


lp = put_list("m-close", [{"text": "唯一项", "done": False}])
stub = StubGW()
stub.history = [asst(11, "答案")]
stub.status = "completed"
run_step(make_turn(msgid="m-close"), stub)
check("4 turn done via step -> list file removed", not lp.exists())

lp = put_list("m-stay", [{"text": "唯一项", "done": False}])
stub = StubGW()
stub.status = "running"
run_step(make_turn(msgid="m-stay"), stub)
check("4 turn still running via step -> list file kept", lp.exists())

# ---------------------------------------------------------------- 5
CLI = ROOT / "native-bridge" / "progress_list.py"


def cli(*argv):
    return subprocess.run(
        [sys.executable, str(CLI), "--base", str(SBX), *argv],
        capture_output=True, text=True)


r = cli("set", "weixin", "m-cli", "准备素材", "渲染合成", "交付文件")
target = SBX / "progress_lists" / "weixin-m-cli.json"
check("5 CLI set -> file written, rc=0",
      r.returncode == 0 and target.exists())
data = json.loads(target.read_text(encoding="utf-8"))
check("5 CLI set -> all items undone, texts in order",
      [i["text"] for i in data["items"]] == ["准备素材", "渲染合成", "交付文件"]
      and all(i["done"] is False for i in data["items"]))
check("5 CLI set -> no tmp residue",
      [p.name for p in (SBX / "progress_lists").iterdir()
       if ".tmp-" in p.name] == [])
check("5 bridge line reflects the CLI list",
      nb._progress_list_line("weixin", "m-cli") == "清单 0/3：准备素材")

r = cli("done", "weixin", "m-cli", "1")
check("5 CLI done 1 -> rc=0", r.returncode == 0)
check("5 bridge line advances to item 2",
      nb._progress_list_line("weixin", "m-cli") == "清单 1/3：渲染合成")

r = cli("done", "weixin", "m-cli", "9")
check("5 CLI done out of range -> nonzero rc", r.returncode != 0)
check("5 failed done leaves the list untouched",
      nb._progress_list_line("weixin", "m-cli") == "清单 1/3：渲染合成")

r = cli("clear", "weixin", "m-cli")
check("5 CLI clear -> file removed, line gone",
      r.returncode == 0 and not target.exists()
      and nb._progress_list_line("weixin", "m-cli") is None)

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
