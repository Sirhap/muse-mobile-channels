#!/usr/bin/env python3
"""Proactive-row delivery guarantees (2026-10-14, audit G1+G2).

G1: rows the bridge marks proactive=True (idle results, trailing
segments, bridge alerts) must park-and-retry like formal rows in
BOTH gateways instead of dead-lettering at SEND_MAX_ATTEMPTS;
ordinary send rows still dead-letter, and the bridge producer
stamps the flag on its idle/alert rows.
G2: the WeCom gateway gains the stuck-formal notice the WeChat
gateway has had: at FORMAL_STUCK_NOTIFY_ATTEMPTS failures of a
formal row it notifies ONCE — an unbound send row in its own
outbox plus a cross-channel row in the WeChat outbox (the file
itself too, for a stuck reply_file).

Run: ~/muse-test-venv/bin/python tests/test_proactive_rows_20261014.py
"""
import importlib.util
import json
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/proactive-rows-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_mod(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def fresh(mod, tag):
    d = SBX / tag
    d.mkdir(parents=True)
    mod.STATE = d
    mod.OUTBOX = d / "outbox.jsonl"
    mod.OUTBOX_RESULTS = d / "outbox_results.jsonl"
    mod.OUTBOX_OFFSET = d / "outbox.offset"
    mod.OUTBOX_RETRY = d / "outbox_retry.json"
    mod.OUTBOX_PARTIAL = d / "outbox_partial.json"
    mod.OUTBOX_PARKED = d / "outbox_parked.json"
    g = mod.Gateway.__new__(mod.Gateway)
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
    return g, d


def results(d):
    p = d / "outbox_results.jsonl"
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8")
            .splitlines() if l.strip()]


def fail_n(mod, g, item, n, errmsg="boom"):
    action = None
    for _ in range(n):
        r = g._note_failure(item, errmsg) \
            if mod.__name__.startswith("wxgw") else g._note_failure(item)
        action = r[0] if isinstance(r, tuple) else r
    return action


# ---------- G1: both gateways ----------
wx = load_mod("wxgw_pro", ROOT / "weixin-bot" / "gateway.py")
wc = load_mod("wcpro", ROOT / "wecom-bot" / "gateway.py")
for mod, tag in ((wx, "wx"), (wc, "wc")):
    cap = mod.SEND_MAX_ATTEMPTS
    g, d = fresh(mod, f"{tag}-pro")
    item = {"id": f"{tag}idle1", "mode": "send", "proactive": True,
            "content": "后台任务结果"}
    act = fail_n(mod, g, item, cap + 2)
    check(f"{tag} G1 proactive send survives cap+2 attempts (park)",
          act == "park"
          and not any(r.get("deadletter") for r in results(d)))
    g, d = fresh(mod, f"{tag}-plain")
    item = {"id": f"{tag}plain1", "mode": "send", "content": "普通气泡"}
    act = fail_n(mod, g, item, cap)
    check(f"{tag} G1 ordinary send still dead-letters at cap",
          act == "deadletter"
          and any(r.get("deadletter") for r in results(d)))
    g, d = fresh(mod, f"{tag}-file")
    item = {"id": f"{tag}file1", "mode": "send_file",
            "proactive": True, "file_path": "/tmp/x.bin"}
    act = fail_n(mod, g, item, cap + 2, errmsg="boom")
    check(f"{tag} G1 proactive send_file survives cap+2 (park)",
          act == "park"
          and not any(r.get("deadletter") for r in results(d)))

# ---------- producer stamps ----------
src = (ROOT / "native-bridge" / "native_bridge.py").read_text(
    encoding="utf-8")
check("producer: idle text row stamped proactive",
      '"mode": "send", "content": body,\n                 "proactive": True'
      in src)
check("producer: idle file row stamped proactive",
      '"proactive": True, "queued_at": now}' in src)
check("producer: alert rows stamped proactive",
      '"text": text, "proactive": True}' in src
      and '"text": text,\n                              "proactive": True}'
      in src)

# ---------- G2: wecom stuck-formal ----------
fake_wx_outbox = SBX / "fake-weixin-outbox.jsonl"
fake_wx_outbox.write_text("", encoding="utf-8")
wc.STUCK_NOTICE_WEIXIN_OUTBOX = fake_wx_outbox

g, d = fresh(wc, "wc-stuck")
item = {"id": "wcformal1", "mode": "reply", "msgid": "m1",
        "chatid": "testuser", "chat_type": 1, "content": "正式回复"}
for _ in range(wc.FORMAL_STUCK_NOTIFY_ATTEMPTS - 1):
    g._note_failure(item)
own = [json.loads(l) for l in (d / "outbox.jsonl").read_text(
    encoding="utf-8").splitlines() if l.strip()] \
    if (d / "outbox.jsonl").exists() else []
check("G2 no notice before the attempt threshold", own == [])
g._note_failure(item)
own = [json.loads(l) for l in (d / "outbox.jsonl").read_text(
    encoding="utf-8").splitlines() if l.strip()]
cross = [json.loads(l) for l in fake_wx_outbox.read_text(
    encoding="utf-8").splitlines() if l.strip()]
check("G2 at threshold: one in-channel send row",
      len(own) == 1 and own[0].get("mode") == "send"
      and "连续发送失败 3 次" in own[0].get("content", ""))
check("G2 at threshold: one cross-channel row in WeChat outbox",
      len(cross) == 1 and cross[0].get("mode") == "send"
      and cross[0].get("to_user_id") == wc.STUCK_NOTICE_WEIXIN_TO
      and "企业微信通道提醒" in cross[0].get("content", ""))
g._note_failure(item)
g._note_failure(item)
own2 = [json.loads(l) for l in (d / "outbox.jsonl").read_text(
    encoding="utf-8").splitlines() if l.strip()]
cross2 = [json.loads(l) for l in fake_wx_outbox.read_text(
    encoding="utf-8").splitlines() if l.strip()]
check("G2 notice fires exactly once per stuck row",
      len(own2) == 1 and len(cross2) == 1)

# stuck reply_file also reroutes the file cross-channel
fpath = SBX / "report.pdf"
fpath.write_bytes(b"%PDF-fake")
g, d = fresh(wc, "wc-stuck-file")
item = {"id": "wcformal2", "mode": "reply_file", "msgid": "m2",
        "chatid": "testuser", "chat_type": 1,
        "file_path": str(fpath)}
fake_wx_outbox.write_text("", encoding="utf-8")
for _ in range(wc.FORMAL_STUCK_NOTIFY_ATTEMPTS):
    g._note_failure(item)
cross = [json.loads(l) for l in fake_wx_outbox.read_text(
    encoding="utf-8").splitlines() if l.strip()]
check("G2 stuck reply_file: text + file rows cross-channel",
      len(cross) == 2
      and cross[1].get("mode") == "send_file"
      and cross[1].get("file_path") == str(fpath))

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
