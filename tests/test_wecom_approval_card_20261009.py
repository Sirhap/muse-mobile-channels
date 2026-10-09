#!/usr/bin/env python3
"""WeCom approval cards (2026-10-09) — sandbox tests.

User order (voice, 2026-10-09): approval notices must be cards, not
typed-command text. The approval relay now sends WeCom a
button_interaction card (task_id appr-<num>) with three decision
buttons; this suite pins the gateway half:

(a) approval_card_decide maps appr_once/appr_always/appr_deny to
    allow_once/allow_always/deny and appends exactly one row to the
    relay's decisions.jsonl (channel wecom-card).
(b) an unknown approval number submits nothing and reports failure.
(c) non-approval cards and non-decision keys are untouched (None).
(d) end-to-end through handle_event_callback: an appr_always click
    on an appr-3 card writes the decision, marks the inbox row
    auto_handled (no agent wake, no redundant reply), and updates
    the card to 「已批准·永久 ✅」.
(e) an ordinary confirm-card click still takes the old path
    (auto_handled False, no decision written).
(f) a card click for an approval already decided by text is
    idempotent: it reports the decision already in effect with
    ok=True and appends nothing (live #8: text /批准 8 永久 landed
    first, the late card then showed 「提交失败」).
(g) a decision already submitted but not yet consumed by the relay
    blocks a second submit from any path: slash ack says 已提交过,
    a card click shows the first submission's label, and
    decisions.jsonl still holds exactly one row.

Run: ~/workspace/wecom-bot/.venv/bin/python \
    tests/test_wecom_approval_card_20261009.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/wecom-apprcard-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_appr", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


RELAY_DIR = SBX / "approval-relay"
RELAY_DIR.mkdir(parents=True)
wc.APPROVAL_RELAY_DIR = RELAY_DIR
(RELAY_DIR / "state.json").write_text(json.dumps({
    "next_num": 4,
    "items": {"3": {"approval_id": "aid-3", "who": "Shell command",
                    "what": "命令：git push", "status": "pending"}},
}), encoding="utf-8")

# ---------- (a)-(c) helper level ----------
res = wc.approval_card_decide("appr-3", "appr_once")
check("a: appr_once label", res == ("已批准·仅这次 ✅", True))
rows = read_jsonl(RELAY_DIR / "decisions.jsonl")
check("a: one decision row", len(rows) == 1
      and rows[0]["num"] == "3"
      and rows[0]["decision"] == "allow_once"
      and rows[0]["channel"] == "wecom-card")

res = wc.approval_card_decide("appr-999", "appr_deny")
check("b: unknown num fails", res == ("提交失败 ❌", False))
check("b: nothing appended", len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == 1)

check("c: ordinary card untouched",
      wc.approval_card_decide("optcard-test-1", "btn_confirm") is None)
check("c: confirm key on appr card untouched",
      wc.approval_card_decide("appr-3", "btn_confirm") is None)
check("c: bad task id untouched",
      wc.approval_card_decide("appr-x", "appr_once") is None)

# ---------- (f)-(g) idempotency ----------
_orig_state_text = (RELAY_DIR / "state.json").read_text(encoding="utf-8")

(RELAY_DIR / "state.json").write_text(json.dumps({
    "next_num": 8,
    "items": {
        "5": {"approval_id": "aid-5", "who": "Shell command",
              "what": "命令：git push", "status": "decided:allow_always"},
        "6": {"approval_id": "aid-6", "who": "Shell command",
              "what": "命令：git push", "status": "gone"},
        "7": {"approval_id": "aid-7", "who": "Shell command",
              "what": "命令：git push", "status": "pending"},
    }}), encoding="utf-8")
before_rows = len(read_jsonl(RELAY_DIR / "decisions.jsonl"))
res = wc.approval_card_decide("appr-5", "appr_once")
check("f: already-decided click shows in-effect decision",
      res == ("已批准·永久 ✅", True))
check("f: already-decided click appends nothing",
      len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == before_rows)
res = wc.approval_card_decide("appr-6", "appr_once")
check("f: gone approval click fails as expired",
      res == ("已失效 ❌", False))
check("f: gone click appends nothing",
      len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == before_rows)

(RELAY_DIR / "decisions.jsonl").write_text("", encoding="utf-8")
ack = wc.slash_approval_decide_ack("7", "allow_always", "wecom")
check("g: first submit appended", ack.startswith("已提交")
      and len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == 1)
ack = wc.slash_approval_decide_ack("7", "allow_once", "wecom")
check("g: second submit blocked as already-submitted",
      "已提交过" in ack
      and len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == 1)
res = wc.approval_card_decide("appr-7", "appr_deny")
check("g: card click shows first submission, appends nothing",
      res == ("已批准·永久 ✅", True)
      and len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == 1)

(RELAY_DIR / "state.json").write_text(_orig_state_text, encoding="utf-8")

# ---------- (d)-(e) event level ----------
STATE_DIR = SBX / "state"
STATE_DIR.mkdir(parents=True)
wc.STATE = STATE_DIR
wc.INBOX = STATE_DIR / "inbox.jsonl"
wc.CARDS = STATE_DIR / "cards.json"
wc.REQMAP = STATE_DIR / "reqmap.json"
wc.SEEN_FILE = STATE_DIR / "seen_ids.jsonl"
wc.STATUS = STATE_DIR / "status.json"


def make_gateway():
    g = wc.Gateway.__new__(wc.Gateway)
    g.ws = None
    g.connected = True
    g.started_at = int(time.time())
    g.msgs_received = 0
    g.msgs_sent = 0
    g.last_error = ""
    g.state = "connected"
    g.pending_responses = {}
    g.respond_locks = {}
    g.open_streams = {}
    g._http = None
    g.allow_users = set()
    g.seen_msgids = set()
    g.reqmap = {}
    g.cards = {}
    g._lock_fd = None
    g._kicked = False
    g.feedback_track = {}
    g._coalescer = None
    g._coalesce_pending = {}
    g.frames = []

    async def fake_send_frame(frame, wait_response=False, timeout=None):
        g.frames.append(frame)
        return {"errcode": 0}

    g.send_frame = fake_send_frame
    return g


def card_event(msgid, task_id, key):
    return {
        "headers": {"req_id": f"req-{msgid}"},
        "body": {
            "msgid": msgid,
            "chattype": "single",
            "chatid": "sirhao",
            "from": {"userid": "sirhao"},
            "event": {
                "eventtype": "template_card_event",
                "template_card_event": {"event_key": key,
                                        "task_id": task_id},
            },
        },
    }


async def scenario_d():
    # Independent scenario: the relay-side files start empty, as if
    # no earlier scenario had submitted anything for #3.
    (RELAY_DIR / "decisions.jsonl").write_text("", encoding="utf-8")
    g = make_gateway()
    g.cards = {"appr-3": {"task_id": "appr-3", "title": "待审批 #3",
                          "desc": "", "card_type": "button_interaction",
                          "options": {}, "chatid": "sirhao", "msgid": "",
                          "status": "pending",
                          "created_at": int(time.time())}}
    await g.handle_event_callback(card_event("evt-d1", "appr-3", "appr_always"))
    rows = read_jsonl(RELAY_DIR / "decisions.jsonl")
    check("d: decision written", len(rows) == 1
          and rows[-1]["decision"] == "allow_always"
          and rows[-1]["num"] == "3")
    inbox = read_jsonl(wc.INBOX)
    check("d: inbox row auto_handled", len(inbox) == 1
          and inbox[0]["auto_handled"] is True
          and "决定=allow_always" in inbox[0]["text"])
    updates = [f for f in g.frames
               if f["body"].get("response_type") == "update_template_card"]
    check("d: card updated to decision label", len(updates) == 1
          and updates[0]["body"]["template_card"]["main_title"]["title"]
          == "已批准·永久 ✅")
    check("d: card registry marked", g.cards["appr-3"]["status"] == "appr_always")


async def scenario_e():
    g = make_gateway()
    g.cards = {"task-plain": {"task_id": "task-plain", "title": "普通确认卡",
                              "desc": "", "card_type": "button_interaction",
                              "options": {}, "chatid": "sirhao", "msgid": "",
                              "status": "pending",
                              "created_at": int(time.time())}}
    before = len(read_jsonl(RELAY_DIR / "decisions.jsonl"))
    await g.handle_event_callback(
        card_event("evt-e1", "task-plain", "btn_confirm"))
    inbox = read_jsonl(wc.INBOX)
    check("e: ordinary click wakes agent", len(inbox) == 2
          and inbox[-1]["auto_handled"] is False)
    check("e: no decision written",
          len(read_jsonl(RELAY_DIR / "decisions.jsonl")) == before)


asyncio.run(scenario_d())
asyncio.run(scenario_e())

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
sys.exit(1 if failed else 0)
