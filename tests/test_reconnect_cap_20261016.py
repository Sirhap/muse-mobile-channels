#!/usr/bin/env python3
"""Connection re-establishment stops after 3 consecutive failures.

Each long-lived loop gets its own RECONNECT_MAX_ATTEMPTS (value 3):

- weixin: one episode is run_session returning a getupdates failure
- wecom: one episode is a session that died before 60s, or raised
  before subscribe; auth_failed keeps its 300s pause
- native-bridge: one episode is connect() raising

A success resets the counter. After the third failure the loop parks
in reconnect_exhausted (gateways) or sets the channel flag (bridge)
and does not start a fourth attempt. Run: python3 tests/test_reconnect_cap_20261016.py
"""
import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/reconnect-cap-20261016")
RESULTS = []


class StopLoop(BaseException):
    """Leave run() without looking like a connection error."""


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name, flush=True)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def verdict():
    fails = sum(1 for _, ok in RESULTS if not ok)
    print(f"{len(RESULTS) - fails}/{len(RESULTS)} passed", flush=True)
    return 1 if fails else 0


# ---------------------------------------------------------------- weixin


def _bind_weixin(wx, sbx):
    sbx.mkdir(parents=True, exist_ok=True)
    wx.STATE = sbx
    wx.INBOX = sbx / "inbox.jsonl"
    wx.SEEN_FILE = sbx / "seen_ids.jsonl"
    wx.SYNC_FILE = sbx / "sync.json"
    wx.CONTEXT_FILE = sbx / "context.json"
    wx.LOCK_FILE = sbx / "gateway.lock"
    wx.STATUS = sbx / "status.json"
    wx.CRED_FILE = sbx / "no-such-credentials.env"
    wx.OUTBOX = sbx / "outbox.jsonl"
    wx.OUTBOX_OFFSET = sbx / "outbox.offset"
    wx.OUTBOX_PARKED = sbx / "outbox_parked.json"
    wx.OUTBOX_PARTIAL = sbx / "outbox_partial.json"
    wx.OUTBOX_RESULTS = sbx / "outbox_results.jsonl"
    wx.OUTBOX_RETRY = sbx / "outbox_retry.json"


def _ok_payload():
    return {"errcode": 0, "ret": 0, "msgs": [], "get_updates_buf": "buf"}


def _err_payload():
    return {"errcode": 500, "ret": 500, "errmsg": "down"}


async def _drive_weixin(wx, mode):
    """Run the real getupdates loop against a scripted _post.

    ``mode`` is cap, reset, first, inner, or stale. Sleeps return at
    once so the horizon is a count of wakeups, not wall time.
    """
    sbx = SBX / f"wx-{mode}"
    if sbx.exists():
        for child in sbx.iterdir():
            if child.is_file():
                child.unlink()
    _bind_weixin(wx, sbx)
    wx.load_credentials = lambda: {
        "token": "test-token",
        "bot_id": "bot",
        "user_id": "user",
        "base_url": "https://example.invalid",
    }
    logs = []
    wx.log = logs.append
    g = wx.Gateway()

    async def noop(*_a, **_k):
        return None

    g.outbox_loop = noop
    g.feedback_watch_loop = noop
    box = {
        "getupdates": 0,
        "notifystart": 0,
        "oks": 0,
        "sleeps": [],
    }

    async def fake_post(_client, _base, endpoint, _body, _token, _timeout):
        if "notifystart" in endpoint:
            box["notifystart"] += 1
            return {}
        if "getupdates" not in endpoint:
            raise AssertionError(endpoint)
        box["getupdates"] += 1
        n = box["getupdates"]
        if mode == "cap":
            if n > 20:
                raise StopLoop()
            return _err_payload()
        if mode == "reset":
            if n <= 10:
                return _err_payload()
            if n == 11:
                box["oks"] += 1
                return _ok_payload()
            if n <= 16:
                return _err_payload()
            raise StopLoop()
        if mode == "first":
            if n >= 2:
                raise StopLoop()
            box["oks"] += 1
            return _ok_payload()
        if mode == "inner":
            if n <= 4:
                return _err_payload()
            if n == 5:
                box["oks"] += 1
                return _ok_payload()
            raise StopLoop()
        if mode == "stale":
            if n >= 5:
                raise StopLoop()
            return {"errcode": -14, "ret": -14, "errmsg": "stale"}
        raise AssertionError(mode)

    g._post = fake_post

    async def fast_sleep(secs):
        box["sleeps"].append((g.state, g.reconnect_failures, secs))
        parked = sum(1 for st, _, _ in box["sleeps"]
                     if st == "reconnect_exhausted")
        if mode == "cap" and parked >= 8:
            raise StopLoop()
        if (mode == "reset" and box["oks"]
                and g.state == "reconnecting"
                and g.reconnect_failures == 1):
            raise StopLoop()
        if len(box["sleeps"]) > 80:
            raise StopLoop()

    orig_sleep = wx.asyncio.sleep
    saved_proxy = {}
    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        if key in os.environ:
            saved_proxy[key] = os.environ.pop(key)
    wx.asyncio.sleep = fast_sleep
    try:
        try:
            await g.run()
        except StopLoop:
            pass
    finally:
        wx.asyncio.sleep = orig_sleep
        os.environ.update(saved_proxy)
        if g._lock_fd is not None:
            g._lock_fd.close()
            g._lock_fd = None
    box["logs"] = logs
    box["state"] = g.state
    box["failures"] = g.reconnect_failures
    box["status"] = {}
    if wx.STATUS.exists():
        box["status"] = json.loads(wx.STATUS.read_text(encoding="utf-8"))
    return box


def _exhausted_logs(logs, layer):
    needle = (
        f"{layer} reconnect exhausted after 3 consecutive failed "
        "connection attempts; state=reconnect_exhausted")
    return [line for line in logs if needle in line]


def test_weixin(wx):
    check("weixin RECONNECT_MAX_ATTEMPTS is 3",
          wx.RECONNECT_MAX_ATTEMPTS == 3)

    cap = asyncio.run(_drive_weixin(wx, "cap"))
    check("weixin cap: exactly 3 getupdates sessions (15 errors)",
          cap["notifystart"] == 3 and cap["getupdates"] == 15)
    check("weixin cap: no 4th session inside the park horizon",
          cap["getupdates"] == 15
          and sum(1 for st, _, _ in cap["sleeps"]
                  if st == "reconnect_exhausted") >= 8)
    check("weixin cap: state reconnect_exhausted",
          cap["state"] == "reconnect_exhausted"
          and cap["failures"] == 3
          and cap["status"].get("state") == "reconnect_exhausted"
          and cap["status"].get("connected") is False)
    check("weixin cap: one exhausted log line",
          len(_exhausted_logs(cap["logs"], "weixin-gateway")) == 1)
    reconnecting = [secs for st, _, secs in cap["sleeps"]
                    if st == "reconnecting"]
    check("weixin cap: backoff between attempts stays 5s then 10s",
          reconnecting == [5, 10])
    connected = [secs for st, _, secs in cap["sleeps"] if st == "connected"]
    check("weixin cap: in-session error pacing unchanged",
          connected == [2, 4, 6, 8] * 3)

    reset = asyncio.run(_drive_weixin(wx, "reset"))
    check("weixin reset: fail, fail, succeed, fail leaves counter at 1",
          reset["oks"] == 1
          and reset["failures"] == 1
          and reset["notifystart"] == 3
          and reset["getupdates"] == 16
          and reset["state"] == "reconnecting"
          and not _exhausted_logs(reset["logs"], "weixin-gateway"))

    first = asyncio.run(_drive_weixin(wx, "first"))
    check("weixin first success: one session, counter stays 0",
          first["notifystart"] == 1
          and first["oks"] == 1
          and first["failures"] == 0
          and first["state"] == "connected"
          and not _exhausted_logs(first["logs"], "weixin-gateway"))

    inner = asyncio.run(_drive_weixin(wx, "inner"))
    check("weixin four inner errors are not four episodes",
          inner["notifystart"] == 1
          and inner["oks"] == 1
          and inner["failures"] == 0
          and inner["state"] == "connected"
          and not _exhausted_logs(inner["logs"], "weixin-gateway"))

    stale = asyncio.run(_drive_weixin(wx, "stale"))
    stale_sleeps = [secs for st, _, secs in stale["sleeps"]
                    if st == "session_stale"]
    check("weixin stale-token probes continue past 3 and do not park",
          stale["notifystart"] == 5
          and stale["failures"] == 0
          and stale["state"] != "reconnect_exhausted"
          and stale_sleeps == [600, 600, 600, 600]
          and any("stale session; probe" in line for line in stale["logs"])
          and not _exhausted_logs(stale["logs"], "weixin-gateway"))


# ---------------------------------------------------------------- wecom


def _bind_wecom(wc, sbx):
    sbx.mkdir(parents=True, exist_ok=True)
    wc.STATE = sbx
    wc.INBOX = sbx / "inbox.jsonl"
    wc.SEEN_FILE = sbx / "seen_ids.jsonl"
    wc.REQMAP = sbx / "reqmap.json"
    wc.CARDS = sbx / "cards.json"
    wc.LOCK_FILE = sbx / "gateway.lock"
    wc.STATUS = sbx / "status.json"
    wc.CRED_FILE = sbx / "no-such-credentials.env"
    wc.OUTBOX = sbx / "outbox.jsonl"
    wc.OUTBOX_OFFSET = sbx / "outbox.offset"
    wc.OUTBOX_PARKED = sbx / "outbox_parked.json"


class _Clock:
    def __init__(self):
        self.now = 1_700_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


async def _drive_wecom(wc, script, stop_at, clock):
    """script items: 'raise', 'short', 'long', 'auth'.

    stop_at is the session attempt number that should raise StopLoop
    at entry (the attempt that must not be treated as completed).
    """
    sbx = SBX / f"wc-{'-'.join(script)}-{stop_at}"
    if sbx.exists():
        for child in sbx.iterdir():
            if child.is_file():
                child.unlink()
    _bind_wecom(wc, sbx)
    wc.load_credentials = lambda: ("bot-id", "bot-secret", set())
    logs = []
    wc.log = logs.append
    g = wc.Gateway()
    attempts = {"n": 0}
    sleeps = []

    async def fake_session(_bot_id, _secret):
        attempts["n"] += 1
        if attempts["n"] >= stop_at:
            raise StopLoop()
        kind = script[attempts["n"] - 1]
        if kind == "raise":
            raise RuntimeError("connect refused before subscribe")
        if kind == "short":
            g._subscribed = True
            g.state = "connected"
            clock.advance(10)
            return "session ended"
        if kind == "long":
            g._subscribed = True
            g.state = "connected"
            clock.advance(60)
            return "session ended"
        if kind == "auth":
            g._subscribed = False
            g.state = "auth_failed"
            g.last_error = "subscribe errcode=853000"
            return "auth_failed"
        raise AssertionError(kind)

    g.run_session = fake_session

    async def fast_sleep(secs):
        sleeps.append((g.state, g.reconnect_failures, secs))
        parked = sum(1 for st, _, _ in sleeps if st == "reconnect_exhausted")
        if parked >= 8:
            raise StopLoop()
        if len(sleeps) > 40:
            raise StopLoop()

    orig_sleep = wc.asyncio.sleep
    orig_time = time.time
    time.time = clock
    wc.asyncio.sleep = fast_sleep
    try:
        try:
            await g.run()
        except StopLoop:
            pass
    finally:
        wc.asyncio.sleep = orig_sleep
        time.time = orig_time
        if g._lock_fd is not None:
            g._lock_fd.close()
            g._lock_fd = None
    status = {}
    if wc.STATUS.exists():
        status = json.loads(wc.STATUS.read_text(encoding="utf-8"))
    return {
        "attempts": attempts["n"],
        "sleeps": sleeps,
        "logs": logs,
        "state": g.state,
        "failures": g.reconnect_failures,
        "status": status,
    }


def test_wecom(wc):
    check("wecom RECONNECT_MAX_ATTEMPTS is 3",
          wc.RECONNECT_MAX_ATTEMPTS == 3)

    cap = asyncio.run(_drive_wecom(
        wc, ["raise", "short", "raise"], stop_at=99, clock=_Clock()))
    check("wecom cap: exactly 3 attempts and no 4th in the park horizon",
          cap["attempts"] == 3
          and sum(1 for st, _, _ in cap["sleeps"]
                  if st == "reconnect_exhausted") >= 8)
    check("wecom cap: state reconnect_exhausted",
          cap["state"] == "reconnect_exhausted"
          and cap["failures"] == 3
          and cap["status"].get("state") == "reconnect_exhausted"
          and cap["status"].get("connected") is False)
    check("wecom cap: one exhausted log line",
          len(_exhausted_logs(cap["logs"], "wecom-gateway")) == 1)
    reconnecting = [secs for st, _, secs in cap["sleeps"]
                    if st == "reconnecting"]
    check("wecom cap: backoff between attempts stays 5s then 10s",
          reconnecting == [5, 10])

    reset = asyncio.run(_drive_wecom(
        wc, ["raise", "short", "long", "raise"], stop_at=5, clock=_Clock()))
    check("wecom reset: fail, fail, succeed, fail leaves counter at 1",
          reset["attempts"] == 5
          and reset["failures"] == 1
          and reset["state"] == "reconnecting"
          and not _exhausted_logs(reset["logs"], "wecom-gateway"))
    reset_sleeps = [secs for st, _, secs in reset["sleeps"]
                    if st == "reconnecting"]
    check("wecom reset: a 60s session still restarts backoff at 5s",
          reset_sleeps == [5, 10, 5, 10])

    first = asyncio.run(_drive_wecom(
        wc, ["long"], stop_at=2, clock=_Clock()))
    check("wecom first success: counter stays 0 and reconnects in 5s",
          first["attempts"] == 2
          and first["failures"] == 0
          and first["state"] == "reconnecting"
          and [secs for st, _, secs in first["sleeps"]
               if st == "reconnecting"] == [5]
          and any("reconnecting in 5s" in line for line in first["logs"])
          and not _exhausted_logs(first["logs"], "wecom-gateway"))

    auth = asyncio.run(_drive_wecom(
        wc, ["auth", "auth", "auth", "auth"], stop_at=5, clock=_Clock()))
    auth_sleeps = [secs for st, _, secs in auth["sleeps"]
                   if st == "auth_failed"]
    check("wecom auth_failed still pauses 300s and is not the 3-cap",
          auth["attempts"] == 5
          and auth["failures"] == 0
          and auth["state"] != "reconnect_exhausted"
          and auth_sleeps == [300, 300, 300, 300]
          and sum("sleeping 300s" in line for line in auth["logs"]) == 4
          and not _exhausted_logs(auth["logs"], "wecom-gateway"))


async def _test_wecom_subscribe_flag(wc):
    sbx = SBX / "wc-flag"
    if sbx.exists():
        for child in sbx.iterdir():
            if child.is_file():
                child.unlink()
    _bind_wecom(wc, sbx)
    g = wc.Gateway()

    class Conn:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return False

        async def send(self, _data):
            return None

        async def recv(self):
            return json.dumps(self.payload)

        async def close(self):
            return None

    payloads = [{"errcode": 853000, "errmsg": "bad creds"}]

    def connect(*_a, **_k):
        return Conn(payloads[0])

    orig = wc.websockets.connect
    wc.websockets.connect = connect
    try:
        reason = await g.run_session("bot-id", "secret")
        check("wecom subscribe auth_failed does not mark subscribed",
              reason == "auth_failed"
              and g._subscribed is False
              and g.state == "auth_failed")
        payloads[0] = {"errcode": 0, "errmsg": "ok"}

        async def quick(*_a, **_k):
            return None

        g.reader_loop = quick
        g.heartbeat_loop = quick
        g.outbox_loop = quick
        g.feedback_watch_loop = quick
        reason = await g.run_session("bot-id", "secret")
        check("wecom subscribe success marks the session subscribed",
              reason == "session ended" and g._subscribed is True)
    finally:
        wc.websockets.connect = orig
        if g._lock_fd is not None:
            g._lock_fd.close()


# ---------------------------------------------------------------- bridge


def _load_bridge():
    os.environ.setdefault(
        "MUSE_CLI_SRC",
        os.path.expanduser("~/workspace/reference/muse-cli/src"))
    return load("native_bridge_reconnect_cap",
                ROOT / "native-bridge" / "native_bridge.py")


def _bind_bridge(nb, sbx):
    sbx.mkdir(parents=True, exist_ok=True)
    cred = sbx / "creds"
    cred.mkdir(exist_ok=True)
    (cred / "cookies.txt").write_text("dummy=1\n", encoding="utf-8")
    (cred / "vm_id.txt").write_text("vm\n", encoding="utf-8")
    (cred / "token.json").write_text("{}\n", encoding="utf-8")
    (sbx / "spool").mkdir(exist_ok=True)
    (sbx / "shadow").mkdir(exist_ok=True)
    nb.BASE = str(sbx)
    nb.STATE_F = str(sbx / "state.json")
    nb.STATUS_F = str(sbx / "status.json")
    nb.CONF_DIR = str(cred)
    nb.CFG = json.loads(json.dumps(nb.CFG))
    for ch in nb.CFG["channels"]:
        nb.CFG["channels"][ch]["bot_state"] = str(sbx / ch)
        (sbx / ch).mkdir(exist_ok=True)
    return cred


class _Gw:
    def close(self):
        return None


def test_bridge(nb):
    check("bridge RECONNECT_MAX_ATTEMPTS is 3",
          nb.RECONNECT_MAX_ATTEMPTS == 3)
    sbx = SBX / "bridge"
    if sbx.exists():
        for child in sbx.rglob("*"):
            if child.is_file():
                child.unlink()
    cred = _bind_bridge(nb, sbx)
    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise RuntimeError("connection refused")

    def ok():
        calls["n"] += 1
        return _Gw()

    def auth_boom():
        calls["n"] += 1
        raise RuntimeError("CurlError Refused WebSocket upgrade: 403")

    orig = nb.connect
    try:
        nb.connect = ok
        w = nb.ChannelWorker("test")
        lines = []
        w.log = lambda *a: lines.append(" ".join(str(x) for x in a))
        check("bridge first success: one connect, counter stays 0",
              w.ensure_gw() is True
              and calls["n"] == 1
              and w._reconnect_failures == 0
              and w._reconnect_exhausted is False
              and w.cs().get("reconnect_exhausted") is False
              and any(line == "connected" for line in lines))
        w.drop_gw()

        nb.connect = boom
        calls["n"] = 0
        lines.clear()
        w.ensure_gw()
        w.ensure_gw()
        nb.connect = ok
        w.ensure_gw()
        w.drop_gw()
        nb.connect = boom
        w.ensure_gw()
        check("bridge reset: fail, fail, succeed, fail leaves counter at 1",
              calls["n"] == 4
              and w._reconnect_failures == 1
              and w._reconnect_exhausted is False
              and not any("reconnect exhausted" in line for line in lines))

        calls["n"] = 0
        lines.clear()
        w._reconnect_failures = 0
        nb.connect = boom
        st = nb.load_state()
        nb.ch_state(st, "test")["session_id"] = "sid-1"
        nb.save_state(st)
        w.ensure_gw()
        w.ensure_gw()
        w.last_idle_scan = 0
        w.idle_push_scan(w.cs())
        before = calls["n"]
        for _ in range(10):
            w.ensure_gw()
            w.last_idle_scan = 0
            w.idle_push_scan(w.cs())
        check("bridge cap: 3 connect attempts then no 4th in the horizon",
              before == 3 and calls["n"] == 3)
        check("bridge cap: flag reconnect_exhausted and one log line",
              w._reconnect_exhausted is True
              and w._reconnect_failures == 3
              and w.cs().get("reconnect_exhausted") is True
              and w.cs().get("reconnect_failures") == 3
              and sum("native-bridge reconnect exhausted after 3 "
                      "consecutive failed connection attempts; "
                      "state=reconnect_exhausted" in line
                      for line in lines) == 1)

        # Restart is a new worker: the persisted park is cleared and
        # connect() is attempted again.
        calls["n"] = 0
        w2 = nb.ChannelWorker("test")
        w2.log = lambda *_a: None
        w.ensure_gw()
        w2.ensure_gw()
        check("bridge restart revives connect without a cred change",
              calls["n"] == 1
              and w2._reconnect_failures == 1
              and w2._reconnect_exhausted is False
              and w._reconnect_exhausted is True)

        # Credential-class failures keep the 600s pause, share the
        # cap, and after the cap only a cred-file change connects.
        calls["n"] = 0
        lines.clear()
        nb.connect = auth_boom
        w3 = nb.ChannelWorker("test")
        w3.log = lambda *a: lines.append(" ".join(str(x) for x in a))
        w3.ensure_gw()
        blocked = calls["n"]
        w3.ensure_gw()
        still_blocked = calls["n"]
        paused = w3._auth_connect_blocked()
        w3._auth_blocked_until = 0
        w3.ensure_gw()
        w3._auth_blocked_until = 0
        w3.ensure_gw()
        w3._auth_blocked_until = 0
        w3.ensure_gw()
        after_cap = calls["n"]
        (cred / "cookies.txt").write_text(
            "dummy=2; replaced-credential\n", encoding="utf-8")
        w3.ensure_gw()
        check("bridge auth failures pause 600s, cap at 3, cred change revives",
              blocked == 1
              and still_blocked == 1
              and paused is True
              and after_cap == 3
              and calls["n"] == 4
              and w3._reconnect_failures == 1
              and w3._reconnect_exhausted is False
              and sum("reconnect exhausted" in line for line in lines) == 1
              and any("credential file change" in line for line in lines))
    finally:
        nb.connect = orig


def main():
    wx = load("weixin_gateway_reconnect_cap", ROOT / "weixin-bot" / "gateway.py")
    test_weixin(wx)
    wc = load("wecom_gateway_reconnect_cap", ROOT / "wecom-bot" / "gateway.py")
    test_wecom(wc)
    asyncio.run(_test_wecom_subscribe_flag(wc))
    nb = _load_bridge()
    test_bridge(nb)
    return verdict()


if __name__ == "__main__":
    sys.exit(main())
