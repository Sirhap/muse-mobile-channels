#!/usr/bin/env python3
"""Bridge alert empty-bubble fix (2026-10-14) — sandbox tests.

Incident: the user received an EMPTY WeCom message every hour. The
bridge's idle-scan alert writer (_send_alert_text) put the body in
"text", while both gateways dispatch the body from "content" only —
so the alert went out empty and the platform still returned ok=true.
Underneath, the idle scan itself was failing every ~20s because the
muse.ai credential exported 2026-10-07 had been invalidated
(auth/check 401 hatchAdmissionInvalidated; stored token's WebSocket
upgrade refused 403).

Covers:
  A. _send_alert_text writes content == text on live wecom/weixin
     rows and on the shadow row.
  B. WeCom coalescer v2 keeps a text-only send row's body.
  C. WeCom gateway dispatch: a text-only send row is delivered with
     its body; an empty send row is consumed WITHOUT any send_frame.
  D. Weixin gateway dispatch: same fallback and same empty guard.
  E. Credential-class idle-scan failures: first failure alerts ONCE
     with the credential-expired text; backoff blocks further
     connect attempts (no 20s hammering, no second alert); a changed
     credential file re-arms both the retry and the alert.

The gateways and the bridge live in different venvs (gateway deps
vs curl_cffi/noise), so C and D run as subprocesses of this same
file under each gateway's own venv:
  ~/muse-test-venv/bin/python tests/test_bridge_alertfix_20261014.py
"""
import asyncio
import copy
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/bridge-alertfix-sbx")
PART = sys.argv[1] if len(sys.argv) > 1 else "main"

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name, flush=True)


def verdict():
    fails = sum(1 for _, ok_ in RESULTS if not ok_)
    print(f"{len(RESULTS) - fails}/{len(RESULTS)} passed", flush=True)
    return 1 if fails else 0


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------------------- C: wecom
def part_wecom():
    os.environ["HOME"] = str(SBX)
    wc = load("wcgw_alertfix", ROOT / "wecom-bot" / "gateway.py")
    WCD = SBX / "wc_state"
    WCD.mkdir(parents=True, exist_ok=True)
    wc.STATE = WCD
    wc.OUTBOX_RESULTS = WCD / "outbox_results.jsonl"
    wc.OUTBOX_PARTIAL = WCD / "outbox_partial.json"
    wc.STATUS = WCD / "status.json"
    wc.CANCELLED_FILE = WCD / "cancelled.json"

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

        async def fake_send_frame(frame, wait_response=True):
            g.frames.append(frame)
            return {"errcode": 0, "errmsg": "ok"}
        g.send_frame = fake_send_frame
        return g

    g = make_gateway()
    ok = asyncio.run(g.dispatch_outbox_item(
        {"id": "alert-1", "mode": "send", "chatid": "testuser",
         "chat_type": 1, "text": "只有 text 的告警"}))
    sent = [f["body"]["markdown"]["content"] for f in g.frames
            if f.get("body", {}).get("msgtype") == "markdown"]
    check("C wecom dispatch delivers a text-only send with its body",
          ok and sent == ["只有 text 的告警"])

    g = make_gateway()
    ok = asyncio.run(g.dispatch_outbox_item(
        {"id": "alert-2", "mode": "send", "chatid": "testuser",
         "chat_type": 1, "content": ""}))
    res = [r for r in read_jsonl(WCD / "outbox_results.jsonl")
           if r.get("id") == "alert-2"]
    check("C wecom dispatch drops an empty send without sending",
          ok and g.frames == [] and len(res) == 1
          and "empty send" in (res[0].get("errmsg") or ""))
    return verdict()


# ------------------------------------------------------------- D: weixin
def part_weixin():
    os.environ["HOME"] = str(SBX)
    wx = load("wxgw_alertfix", ROOT / "weixin-bot" / "gateway.py")
    WXD = SBX / "wx_state"
    WXD.mkdir(parents=True, exist_ok=True)
    wx.STATE = WXD
    wx.OUTBOX_RESULTS = WXD / "outbox_results.jsonl"
    wx.CANCELLED_FILE = WXD / "cancelled.json"
    wx.STATUS = WXD / "status.json"

    def make_gateway():
        g = wx.Gateway.__new__(wx.Gateway)
        g.state = "connected"
        g.connected = True
        g.started_at = int(time.time())
        g.msgs_received = 0
        g.msgs_sent = 0
        g.last_error = ""
        g.sent_texts = []

        async def fake_send_text(client, creds, to, content, *a, **k):
            g.sent_texts.append(content)
            return {"ret": 0}
        g.send_text = fake_send_text
        return g

    g = make_gateway()
    ok = asyncio.run(g.dispatch_outbox_item(
        None, {}, {"id": "alert-3", "mode": "send", "to_user_id": "u1",
                   "text": "微信只有 text 的告警"}))
    check("D weixin dispatch delivers a text-only send with its body",
          ok and g.sent_texts == ["微信只有 text 的告警"])

    g = make_gateway()
    ok = asyncio.run(g.dispatch_outbox_item(
        None, {}, {"id": "alert-4", "mode": "send", "to_user_id": "u1",
                   "content": ""}))
    res = [r for r in read_jsonl(WXD / "outbox_results.jsonl")
           if r.get("id") == "alert-4"]
    check("D weixin dispatch drops an empty send without sending",
          ok and g.sent_texts == [] and len(res) == 1
          and "empty send" in (res[0].get("errmsg") or ""))
    return verdict()


# ------------------------------------------------------------- A/B/E
def part_main():
    shutil.rmtree(SBX, ignore_errors=True)
    os.environ["HOME"] = str(SBX)
    # gw2 resolves the muse-cli sources from $HOME unless overridden;
    # HOME is the sandbox here, so pin the real source path.
    os.environ["MUSE_CLI_SRC"] = str(ROOT / "reference" / "muse-cli" / "src")
    WX = SBX / "weixin_state"
    WC = SBX / "wecom_state"
    TS = SBX / "test_state"
    CRED = SBX / "creds"
    for d in (WX, WC, TS, SBX / "spool", SBX / "shadow", CRED):
        d.mkdir(parents=True, exist_ok=True)
    for flag in ("enabled-weixin", "enabled-wecom",
                 "longtask-weixin", "longtask-test"):
        (SBX / flag).touch()
    (CRED / "cookies.txt").write_text("dummy=1")
    (CRED / "vm_id.txt").write_text("vm-dummy")
    (CRED / "token.json").write_text("{}")

    nb = load("native_bridge", ROOT / "native-bridge" / "native_bridge.py")
    nb.CFG = copy.deepcopy(nb.CFG)
    nb.CFG["channels"]["weixin"]["bot_state"] = str(WX)
    nb.CFG["channels"]["wecom"]["bot_state"] = str(WC)
    nb.CFG["channels"]["test"]["bot_state"] = str(TS)
    nb.STATE_F = str(SBX / "state.json")
    nb.STATUS_F = str(SBX / "status.json")
    nb.BASE = str(SBX)
    nb.CONF_DIR = str(CRED)

    # ---------------------------------------------------------- A
    for ch, state_dir in (("wecom", WC), ("weixin", WX)):
        w = nb.ChannelWorker(ch)
        w._send_alert_text("⚠️ 测试告警正文")
        rows = [r for r in read_jsonl(state_dir / "outbox.jsonl")
                if r.get("id", "").startswith("bridgealert-")]
        check(f"A {ch} live alert row carries content == text",
              len(rows) == 1 and rows[0].get("content") == "⚠️ 测试告警正文"
              and rows[0].get("text") == "⚠️ 测试告警正文")
    w = nb.ChannelWorker("test")
    w._send_alert_text("⚠️ 影子告警")
    rows = [r for r in read_jsonl(SBX / "shadow" / "test-outbox.jsonl")
            if r.get("id", "").startswith("bridgealert-")]
    check("A shadow alert row carries content == text",
          len(rows) == 1 and rows[0].get("content") == "⚠️ 影子告警"
          and rows[0].get("text") == "⚠️ 影子告警")

    # ---------------------------------------------------------- B
    co_mod = load("coalesce_v2", ROOT / "acceptance-fix-2026-10-08"
                  / "wecom_outbound_coalesce_v2.py")
    co = co_mod.Coalescer()
    co.feed({"id": "x1", "mode": "send", "chatid": "testuser",
             "chat_type": 1, "text": "只有 text 的正文"})
    time.sleep(0.9)
    ready = co.due()
    check("B coalescer keeps a text-only send body",
          len(ready) == 1 and ready[0].get("content") == "只有 text 的正文")

    # ---------------------------------------------------------- E
    check("E is_auth_failure: 401/403 markers, others excluded",
          nb.is_auth_failure(RuntimeError(
              'auth/check -> 401 {"type":"hatchAdmissionInvalidated"}'))
          and nb.is_auth_failure(RuntimeError(
              "CurlError Refused WebSocket upgrade: 403"))
          and not nb.is_auth_failure(RuntimeError("boom"))
          and not nb.is_auth_failure(RuntimeError("connection reset")))

    def set_state(ch, **kw):
        st = nb.load_state()
        c = nb.ch_state(st, ch)
        c.update(kw)
        nb.save_state(st)
        return c

    shadow_out = SBX / "shadow" / "test-outbox.jsonl"
    shadow_out.unlink(missing_ok=True)
    set_state("test", session_id="sid-ok", idle_push_seq=5,
              idle_scan_fail_streak=0, idle_scan_last_alert=0,
              idle_auth_alert_stamp=None)
    connect_calls = {"n": 0}

    def bad_auth_connect():
        connect_calls["n"] += 1
        raise RuntimeError("CurlError Failed to perform, curl: (22) "
                           "Refused WebSocket upgrade: 403")

    orig_connect = nb.connect
    nb.connect = bad_auth_connect
    try:
        w = nb.ChannelWorker("test")
        w.last_idle_scan = 0
        w.idle_push_scan(w.cs())
        alerts = [r for r in read_jsonl(shadow_out)
                  if r.get("mode") == "send"
                  and "凭证已失效" in (r.get("content") or "")]
        check("E first auth failure alerts once, body in content",
              len(alerts) == 1 and connect_calls["n"] == 1)
        for _ in range(3):
            w.last_idle_scan = 0
            w.idle_push_scan(w.cs())
        alerts = [r for r in read_jsonl(shadow_out)
                  if r.get("mode") == "send"
                  and "凭证已失效" in (r.get("content") or "")]
        check("E backoff: no new connect attempt, no second alert",
              connect_calls["n"] == 1 and len(alerts) == 1)
        (CRED / "cookies.txt").write_text("dummy=2; longer=yes")
        w.last_idle_scan = 0
        w.idle_push_scan(w.cs())
        alerts = [r for r in read_jsonl(shadow_out)
                  if r.get("mode") == "send"
                  and "凭证已失效" in (r.get("content") or "")]
        check("E changed credential retries at once, re-alerts once",
              connect_calls["n"] == 2 and len(alerts) == 2)
    finally:
        nb.connect = orig_connect

    rc = verdict()
    # ---------------------------------------------------------- C + D
    for name, venv in (("wecom", ROOT / "wecom-bot" / ".venv" / "bin" / "python"),
                       ("weixin", ROOT / "weixin-bot" / ".venv" / "bin" / "python")):
        p = subprocess.run([str(venv), str(Path(__file__).resolve()), name],
                           capture_output=True, text=True)
        print(p.stdout, end="")
        if p.returncode != 0:
            print(p.stderr[-2000:])
            print(f"FAIL subprocess {name} rc={p.returncode}")
            rc = 1
    return rc


if PART == "wecom":
    sys.exit(part_wecom())
elif PART == "weixin":
    sys.exit(part_weixin())
else:
    sys.exit(part_main())
