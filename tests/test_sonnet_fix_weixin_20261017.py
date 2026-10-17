#!/usr/bin/env python3
"""Regression tests for the 2026-10-17 Weixin gateway defects.

Each check is named W<n> and was written to fail on the pre-fix
gateway. Run: python3 tests/test_sonnet_fix_weixin_20261017.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/sonnet-fix-weixin-20261017")
if SBX.exists():
    shutil.rmtree(SBX)
SBX.mkdir(parents=True)

spec = importlib.util.spec_from_file_location(
    "wxgw_sonnet_20261017", ROOT / "weixin-bot" / "gateway.py")
gw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gw)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name, flush=True)


class StopLoop(BaseException):
    """Leave run() without looking like a connection error."""


def bare():
    g = gw.Gateway.__new__(gw.Gateway)
    g.context = {}
    g.seen_msgids = set()
    g.feedback_track = {}
    g.thinking_notice_at = {}
    g.merged_notice_at = {}
    g.msgs_received = 0
    g.msgs_sent = 0
    g.sync_buf = ""
    g.state = "connected"
    g.connected = True
    g.last_error = ""
    g.write_status = lambda: None
    return g


PATH_ATTRS = (
    "STATE", "INBOX", "SEEN_FILE", "SYNC_FILE", "CONTEXT_FILE", "LOCK_FILE",
    "STATUS", "OUTBOX", "OUTBOX_OFFSET", "OUTBOX_RESULTS", "OUTBOX_RETRY",
    "OUTBOX_PARTIAL", "OUTBOX_PARKED", "COMPRESSED_DIR", "HOOK_STATE_DIR",
    "APPROVAL_RELAY_DIR", "STUCK_NOTICE_WECOM_OUTBOX", "CANCELLED_FILE",
    "FEEDBACK_CLEAR_FILE", "DIVERTED_RAW", "SENT_INDEX", "MEDIA_DIR",
    "CRED_FILE", "POISON_MESSAGES",
)


def bind(name):
    sbx = SBX / name
    if sbx.exists():
        shutil.rmtree(sbx)
    sbx.mkdir(parents=True)
    saved = {attr: getattr(gw, attr) for attr in PATH_ATTRS if hasattr(gw, attr)}
    gw.STATE = sbx
    gw.INBOX = sbx / "inbox.jsonl"
    gw.SEEN_FILE = sbx / "seen_ids.jsonl"
    gw.SYNC_FILE = sbx / "sync.json"
    gw.CONTEXT_FILE = sbx / "context.json"
    gw.LOCK_FILE = sbx / "gateway.lock"
    gw.STATUS = sbx / "status.json"
    gw.OUTBOX = sbx / "outbox.jsonl"
    gw.OUTBOX_OFFSET = sbx / "outbox.offset"
    gw.OUTBOX_RESULTS = sbx / "outbox_results.jsonl"
    gw.OUTBOX_RETRY = sbx / "outbox_retry.json"
    gw.OUTBOX_PARTIAL = sbx / "outbox_partial.json"
    gw.OUTBOX_PARKED = sbx / "outbox_parked.json"
    gw.COMPRESSED_DIR = sbx / "compressed"
    gw.HOOK_STATE_DIR = sbx / "hook"
    gw.APPROVAL_RELAY_DIR = sbx / "approval-relay"
    gw.STUCK_NOTICE_WECOM_OUTBOX = sbx / "wecom-outbox.jsonl"
    gw.CANCELLED_FILE = sbx / "cancelled.json"
    gw.FEEDBACK_CLEAR_FILE = sbx / "feedback_clear.jsonl"
    gw.DIVERTED_RAW = sbx / "diverted_raw.jsonl"
    gw.SENT_INDEX = sbx / "sent_index.jsonl"
    gw.MEDIA_DIR = sbx / "media"
    gw.CRED_FILE = sbx / "credentials.env"
    gw.POISON_MESSAGES = sbx / "poison_messages.jsonl"
    gw.HOOK_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return sbx, saved


def restore(saved):
    for attr, value in saved.items():
        setattr(gw, attr, value)


def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").split("\n"):
        if line.strip():
            rows.append(json.loads(line))
    return rows


# ---------------------------------------------------------------- W1


def test_w1():
    """A raise in the main outbox pass must be logged and not kill the loop."""
    _sbx, saved = bind("w1")
    logs = []
    real_log = gw.log
    real_sleep = asyncio.sleep

    async def fast_sleep(_secs=0):
        await real_sleep(0)

    calls = {"n": 0}

    async def dispatch(self, client, creds, item):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom-main-pass")
        return True

    try:
        gw.log = logs.append
        asyncio.sleep = fast_sleep
        gw.OUTBOX.write_text(
            json.dumps({"id": "row1", "mode": "send", "to_user_id": "u",
                        "content": "hi"}) + "\n",
            encoding="utf-8")
        gw.OUTBOX_OFFSET.write_text("0", encoding="utf-8")
        g = bare()
        g.dispatch_outbox_item = dispatch.__get__(g)
        g._cleanup_item = lambda item_id: None

        async def drive():
            task = asyncio.create_task(g.outbox_loop(None, {}))
            exc = None
            for _ in range(50):
                await real_sleep(0)
                if calls["n"] >= 2 or task.done():
                    break
            if task.done() and not task.cancelled():
                exc = task.exception()
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            return exc

        exc = asyncio.run(drive())
        logged = any("main queue pass failed" in str(line) for line in logs)
        check(
            "W1 main-pass exception is logged and the loop keeps sending",
            calls["n"] >= 2 and logged and exc is None,
        )
        if not (calls["n"] >= 2 and logged and exc is None):
            print(f"  W1 evidence calls={calls['n']} logged={logged} exc={exc!r}")
            print("  W1 logs:", logs[-5:])
    finally:
        gw.log = real_log
        asyncio.sleep = real_sleep
        restore(saved)


# ---------------------------------------------------------------- W2


def test_w2():
    """stale_hits counts consecutive stale endings, not a lifetime total."""
    real_sleep = asyncio.sleep
    real_log = gw.log
    real_load = gw.load_credentials
    saved_proxy = {}

    async def drive(mode):
        sbx, saved = bind(f"w2-{mode}")
        box = {"sleeps": [], "ns": 0, "polls": 0}
        logs = []

        async def fast_sleep(secs=0):
            box["sleeps"].append((g.state, secs))
            if g.state == "needs_relogin":
                raise StopLoop()
            if sum(1 for st, _s in box["sleeps"] if st == "session_stale") >= 6:
                raise StopLoop()
            await real_sleep(0)

        def creds():
            return {
                "token": "test-token",
                "bot_id": "bot",
                "user_id": "user",
                "base_url": "https://example.invalid",
            }

        try:
            gw.log = logs.append
            gw.load_credentials = creds
            asyncio.sleep = fast_sleep
            g = gw.Gateway()
            session_polls = {"n": 0}

            async def hang(*_a, **_k):
                try:
                    while True:
                        await real_sleep(3600)
                except asyncio.CancelledError:
                    return

            g.outbox_loop = hang
            g.feedback_watch_loop = hang

            async def fake_post(_client, _base, endpoint, _body, _token, _timeout):
                if "notifystart" in endpoint:
                    box["ns"] += 1
                    session_polls["n"] = 0
                    return {}
                if "getupdates" not in endpoint:
                    return {"ret": 0}
                session_polls["n"] += 1
                box["polls"] += 1
                if mode == "healthy_then_stale" and session_polls["n"] == 1:
                    return {"errcode": 0, "ret": 0, "msgs": [],
                            "get_updates_buf": "buf"}
                return {"errcode": -14, "ret": -14, "errmsg": "stale"}

            g._post = fake_post
            try:
                await g.run()
            except StopLoop:
                pass
            return {
                "state": g.state,
                "ns": box["ns"],
                "stale_sleeps": [s for st, s in box["sleeps"] if st == "session_stale"],
                "logs": logs,
            }
        finally:
            if g._lock_fd is not None:
                g._lock_fd.close()
                g._lock_fd = None
            restore(saved)

    try:
        for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
            if key in os.environ:
                saved_proxy[key] = os.environ.pop(key)
        healthy = asyncio.run(drive("healthy_then_stale"))
        check(
            "W2 a healthy poll resets the stale streak (no needs_relogin at 6 endings)",
            healthy["state"] != "needs_relogin"
            and len(healthy["stale_sleeps"]) == 6
            and healthy["ns"] == 6,
        )
        if healthy["state"] == "needs_relogin" or len(healthy["stale_sleeps"]) != 6:
            print(
                f"  W2 evidence state={healthy['state']} "
                f"ns={healthy['ns']} stale_sleeps={healthy['stale_sleeps']}"
            )
        consecutive = asyncio.run(drive("consecutive"))
        check(
            "W2 six consecutive stale endings still escalate to needs_relogin",
            consecutive["state"] == "needs_relogin" and consecutive["ns"] == 6,
        )
    finally:
        gw.log = real_log
        gw.load_credentials = real_load
        asyncio.sleep = real_sleep
        os.environ.update(saved_proxy)


# ---------------------------------------------------------------- W3


def test_w3():
    """Ack send failure after a committed slash must not fall through."""
    sbx, saved = bind("w3")
    try:
        g = bare()
        inbox = []
        diverted = []

        def capture_append(path, obj):
            if Path(path) == gw.INBOX:
                inbox.append(obj)
                gw.Gateway.append_jsonl(path, obj)
            elif "spool" in Path(path).parts:
                # Never write to the production bridge spool from a
                # test; record the divert instead.
                diverted.append(obj)
            else:
                gw.Gateway.append_jsonl(path, obj)

        async def boom(*_a, **_k):
            raise RuntimeError("ilink _post failed")

        async def quiet(*_a, **_k):
            return None

        g.append_jsonl = capture_append
        g.send_text = boom
        g._send_typing = quiet
        g._save_context = lambda: None
        g._maybe_arrival_feedback = lambda *_a, **_k: None
        creds = {"user_id": "owner", "token": "t", "base_url": "https://example.invalid"}
        msg = {
            "message_id": "cmd-1",
            "message_type": 1,
            "from_user_id": "owner",
            "context_token": "ctx",
            "item_list": [{"type": 1, "text_item": {"text": "/subagent say hello"}}],
        }
        asyncio.run(g.handle_inbound(None, creds, msg))
        reqs = gw.HOOK_STATE_DIR / "subagent_requests.jsonl"
        req_rows = read_jsonl(reqs)
        fell_through = any("/subagent" in str(row.get("text") or "") for row in inbox)
        check(
            "W3 ack failure after /subagent stays handled (no inbox fallback)",
            len(req_rows) == 1 and not fell_through and "cmd-1" in g.seen_msgids,
        )
        if fell_through or len(req_rows) != 1:
            print(f"  W3 evidence requests={len(req_rows)} inbox={inbox!r} seen={g.seen_msgids}")
    finally:
        restore(saved)


# ---------------------------------------------------------------- W4


def test_w4():
    """ffmpeg failure must not leave a partial that later looks cached."""
    _sbx, saved = bind("w4")
    real_run = gw.subprocess.run
    try:
        g = bare()
        src = SBX / "w4" / "clip.mp4"
        src.write_bytes(b"x" * 5000)

        def fail_run(cmd, **_kwargs):
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"partial")
            return subprocess.CompletedProcess(cmd, 1, b"", b"ffmpeg fail")

        gw.subprocess.run = fail_run
        first = g._compress_file_sync(src)
        leftovers = list(gw.COMPRESSED_DIR.glob("*.mp4")) if gw.COMPRESSED_DIR.exists() else []
        second = g._compress_file_sync(src)
        check(
            "W4 ffmpeg non-zero exit discards the partial (no cache hit)",
            first is None and second is None and leftovers == [],
        )
        if leftovers or second is not None:
            print(f"  W4 evidence first={first!r} second={second!r} leftovers={leftovers}")

        src2 = SBX / "w4" / "clip-timeout.mp4"
        src2.write_bytes(b"y" * 8000)

        def timeout_run(cmd, **_kwargs):
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"partial-timeout")
            raise subprocess.TimeoutExpired(cmd, 180)

        gw.subprocess.run = timeout_run
        timed = g._compress_file_sync(src2)
        left = list(gw.COMPRESSED_DIR.glob("*.mp4")) if gw.COMPRESSED_DIR.exists() else []
        check(
            "W4 ffmpeg timeout discards the partial output",
            timed is None and left == [],
        )
        if left or timed is not None:
            print(f"  W4 timeout evidence timed={timed!r} left={left}")
    finally:
        gw.subprocess.run = real_run
        restore(saved)


# ---------------------------------------------------------------- W5


def test_w5():
    """upload_fail counts consecutive upload-stage failures only."""
    _sbx, saved = bind("w5")
    try:
        g = bare()
        item = {"id": "sf-w5", "mode": "send_file", "to_user_id": "u1",
                "file_path": "/tmp/whatever.bin"}
        g._note_failure(item, "exception: upload-stage: a")
        g._note_failure(item, "sendmessage rejected: ret=-1")
        g._note_failure(item, "exception: upload-stage: b")
        action, notify = g._note_failure(item, "exception: upload-stage: c")
        rec = g._load_parked().get("sf-w5") or {}
        check(
            "W5 a non-upload failure resets the upload streak",
            int(rec.get("upload_fail") or 0) == 2 and action == "park" and notify is False,
        )
        if int(rec.get("upload_fail") or 0) != 2 or action != "park":
            print(f"  W5 evidence upload_fail={rec.get('upload_fail')} action={action} notify={notify}")
        g._cleanup_item("sf-w5")
        action, notify = g._note_failure(item, "exception: upload-stage: d")
        rec = g._load_parked().get("sf-w5") or {}
        check(
            "W5 success clears upload_fail so the next failure starts at 1",
            int(rec.get("upload_fail") or 0) == 1 and action == "park" and notify is False,
        )
    finally:
        restore(saved)


# ---------------------------------------------------------------- W6


def test_w6():
    """cleanup removes suffixed partial-send keys, not only the bare id."""
    _sbx, saved = bind("w6")
    try:
        g = bare()
        g._store_partial("id1", {"sent_chunks": 1})
        g._store_partial("id1:update", {"sent_chunks": 2})
        g._store_partial("id1:caption", {"sent_chunks": 1})
        g._store_partial("id1:levelup", {"sent_chunks": 1})
        g._store_partial("other", {"sent_chunks": 9})
        g._cleanup_item("id1")
        left = g._load_partial()
        check(
            "W6 cleanup drops id:update id:caption and id:levelup",
            "id1" not in left
            and "id1:update" not in left
            and "id1:caption" not in left
            and "id1:levelup" not in left
            and left.get("other", {}).get("sent_chunks") == 9,
        )
        if any(k.startswith("id1") for k in left):
            print(f"  W6 evidence keys={sorted(left)}")
    finally:
        restore(saved)


# ---------------------------------------------------------------- W7


def test_w7():
    """Null message_id must not become the truthy dedupe key 'None'.

    Numeric ids are legitimate (iLink sends message_id as an int):
    they are stringified and deduped like string ids.
    """
    _sbx, saved = bind("w7")
    try:
        g = bare()
        inbox = []
        diverted = []

        def capture_append(path, obj):
            if Path(path) == gw.INBOX:
                inbox.append(obj)
                gw.Gateway.append_jsonl(path, obj)
            elif "spool" in Path(path).parts:
                # Never write to the production bridge spool from a
                # test; record the divert instead.
                diverted.append(obj)
            else:
                gw.Gateway.append_jsonl(path, obj)

        async def quiet(*_a, **_k):
            return None

        g.append_jsonl = capture_append
        g._send_typing = quiet
        g._save_context = lambda: None
        g._maybe_arrival_feedback = lambda *_a, **_k: None
        g._slash_dispatch = quiet
        creds = {"user_id": "owner", "token": "t", "base_url": "https://example.invalid"}

        def msg(mid, text):
            body = {
                "message_type": 1,
                "from_user_id": "owner",
                "item_list": [{"type": 1, "text_item": {"text": text}}],
            }
            if mid is not _MISSING:
                body["message_id"] = mid
            return body

        async def run():
            await g.handle_inbound(None, creds, msg(None, "null-a"))
            await g.handle_inbound(None, creds, msg(None, "null-b"))
            await g.handle_inbound(None, creds, msg(123, "num-a"))
            await g.handle_inbound(None, creds, msg(123, "num-b"))

        asyncio.run(run())
        texts = [row.get("text") for row in inbox]
        dtexts = [row.get("text") for row in diverted]
        check(
            "W7 null ids take the empty-id path; numeric ids stringify, divert and dedupe",
            texts == ["null-a", "null-b"] and dtexts == ["num-a"]
            and "None" not in g.seen_msgids and "123" in g.seen_msgids,
        )
        if texts != ["null-a", "null-b"] or dtexts != ["num-a"]:
            print(f"  W7 evidence texts={texts!r} diverted={dtexts!r} seen={g.seen_msgids}")
    finally:
        restore(saved)


class _Missing:
    pass


_MISSING = _Missing()


# ---------------------------------------------------------------- W8


def test_w8():
    """Persisted-state load skips torn bytes and non-object lines."""
    sbx, saved = bind("w8")
    try:
        gw.INBOX.write_bytes(b'{"msgid":"keep-me"}\n{"msgid":"torn"\xff')
        gw.SEEN_FILE.write_text(
            '{"msgid":"seen-a"}\n[1, 2]\n"nope"\nnull\n{"msgid":"seen-b"}\n',
            encoding="utf-8",
        )
        gw.SYNC_FILE.write_bytes(b'{"buf": "ok"\xff')
        gw.CONTEXT_FILE.write_bytes(b'{"u": 1}\xff')
        g = bare()
        try:
            g._load_persisted()
        except Exception as exc:
            check("W8 persisted load survives torn and non-object lines", False)
            print(f"  W8 evidence {type(exc).__name__}: {exc}")
        else:
            check(
                "W8 persisted load survives torn and non-object lines",
                "keep-me" in g.seen_msgids
                and "seen-a" in g.seen_msgids
                and "seen-b" in g.seen_msgids
                and g.sync_buf == ""
                and g.context == {},
            )
            if "keep-me" not in g.seen_msgids or "seen-b" not in g.seen_msgids:
                print(f"  W8 evidence seen={g.seen_msgids} sync={g.sync_buf!r} ctx={g.context!r}")
        # Same bytes must not crash Gateway().
        gw.LOCK_FILE = sbx / "gateway-ctor.lock"
        try:
            started = gw.Gateway()
        except Exception as exc:
            check("W8 Gateway() construction survives a torn inbox", False)
            print(f"  W8 ctor evidence {type(exc).__name__}: {exc}")
        else:
            check(
                "W8 Gateway() construction survives a torn inbox",
                "keep-me" in started.seen_msgids,
            )
            if started._lock_fd is not None:
                started._lock_fd.close()
                started._lock_fd = None
    finally:
        restore(saved)


# ---------------------------------------------------------------- W9


def test_w9():
    """JSONL readers split on \\n only, so U+2028/U+2029/U+0085 stay inside values."""
    _sbx, saved = bind("w9")
    try:
        weird = "a\u2028b\u2029c\u0085d"
        inbox_row = {"msgid": "m-sep", "text": weird, "from_user_id": "owner"}
        gw.INBOX.write_text(json.dumps(inbox_row, ensure_ascii=False) + "\n", encoding="utf-8")
        g = bare()
        g._load_persisted()
        route = g._route_for("m-sep")
        check(
            "W9 inbox load and route keep a line that contains unicode separators",
            "m-sep" in g.seen_msgids and route.get("from_user_id") == "owner",
        )

        gw.OUTBOX.write_text(
            json.dumps({"id": "text1", "mode": "reply", "msgid": "M",
                        "content": weird}, ensure_ascii=False) + "\n"
            + json.dumps({"id": "file1", "mode": "reply_file", "msgid": "M"}) + "\n",
            encoding="utf-8",
        )
        gw.OUTBOX_RESULTS.write_text(
            json.dumps({"id": "file1", "ok": True, "errmsg": weird}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        delivered = gw.Gateway._delivered_reply_before(
            {"id": "upd1", "mode": "update", "msgid": "M"})
        check(
            "W9 delivered-check sees a result row that contains unicode separators",
            delivered is True,
        )

        state = SBX / "w9" / "out-state"
        state.mkdir(parents=True, exist_ok=True)
        (state / "outbox.jsonl").write_text(
            json.dumps({"msgid": "M", "queued_at": 42, "content": weird},
                       ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        ts = gw._last_outbound_ts(state, ["M"])
        check("W9 outbox tail reader keeps a separator inside a JSONL value", ts == 42)

        quote = gw.Gateway._tail_jsonl_rows(gw.INBOX, 20)
        check(
            "W9 tail jsonl reader returns the separator row",
            any(row.get("msgid") == "m-sep" and row.get("text") == weird for row in quote),
        )

        relay = gw.APPROVAL_RELAY_DIR
        relay.mkdir(parents=True, exist_ok=True)
        (relay / "decisions.offset").write_text("0", encoding="utf-8")
        (relay / "decisions.jsonl").write_text(
            json.dumps({"num": "3", "decision": "deny", "note": weird},
                       ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        check(
            "W9 approval decisions reader keeps a separator inside the row",
            gw._approval_pending_submission("3") == "deny",
        )

        gw.FEEDBACK_CLEAR_FILE.write_text(
            json.dumps({"msgid": "clear-1", "note": weird}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        g.feedback_track = {"clear-1": {"user": "owner"}}
        popped = g._consume_feedback_clear()
        check("W9 feedback-clear reader keeps a separator inside the row", popped == 1)

        (gw.STATE / "bridge_fallback.jsonl").write_text(
            json.dumps({"msgid": "fb-1", "note": weird}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        ids = g._bridge_fallback_ids()
        check("W9 bridge-fallback reader keeps a separator inside the row", "fb-1" in ids)

        gw.OUTBOX_RESULTS.write_text(
            json.dumps({"id": "text1", "ok": True, "errmsg": weird}, ensure_ascii=False) + "\n"
            + json.dumps({"id": "file1", "ok": True, "errmsg": weird}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        same = gw.Gateway._same_reply_text_delivered(
            {"id": "other", "msgid": "M"}, weird)
        check("W9 same-reply reader matches across a separator in the row", same is True)
        if not same:
            print("  W9 same-reply evidence missed the delivered text row")
    finally:
        restore(saved)


# ---------------------------------------------------------------- W10


def test_w10():
    """Unroutable formal rows and refused files get a notice, not a caption."""
    sbx, saved = bind("w10")
    # Outside every file-send root (muse_home, sandbox STATE/BASE, /tmp)
    # on both the dev host and the cloud sandbox; a hardcoded repo path
    # only existed in the sandbox. /var/tmp is outside all roots.
    secret = Path("/var/tmp/.w10-secret.bin")
    try:
        secret.write_bytes(b"secret-bytes")
        g = bare()
        sent = []

        async def fake_send(_c, _creds, to, content, *_a, **_k):
            sent.append((to, content))
            return {"ret": 0}

        g.send_text = fake_send
        gw.STUCK_NOTICE_WECOM_OUTBOX.write_text("", encoding="utf-8")

        async def unroutable():
            return await g.dispatch_outbox_item(
                None, {},
                {"id": "r-noroute", "mode": "reply", "msgid": "missing",
                 "content": "正式回复正文"},
            )

        ok = asyncio.run(unroutable())
        wecom = read_jsonl(gw.STUCK_NOTICE_WECOM_OUTBOX)
        notice_text = " ".join(
            [c for _to, c in sent] + [str(r.get("content") or "") for r in wecom])
        check(
            "W10 unroutable formal reply is consumed with a visible failure notice",
            ok is True
            and "找不到原消息的回复路径" in notice_text
            and "正式回复正文" not in notice_text,
        )
        if "找不到原消息的回复路径" not in notice_text:
            print(f"  W10 unroutable evidence ok={ok} sent={sent!r} wecom={wecom!r}")

        sent.clear()
        gw.STUCK_NOTICE_WECOM_OUTBOX.write_text("", encoding="utf-8")
        g.context = {"M1": {"from_user_id": "u1", "context_token": "tok"}}

        async def refused_file(mode, item_id):
            return await g.dispatch_outbox_item(
                None, {},
                {"id": item_id, "mode": mode, "msgid": "M1",
                 "to_user_id": "u1", "content": "这是说明",
                 "file_path": str(secret)},
            )

        ok_file = asyncio.run(refused_file("reply_file", "rf-deny"))
        captions = [c for _to, c in sent]
        check(
            "W10 reply_file allowlist runs before the caption and explains the refusal",
            ok_file is True
            and not any("这是说明" in c for c in captions)
            and any("不在允许发送的目录" in c for c in captions),
        )
        if any("这是说明" in c for c in captions) or not any("不在允许发送的目录" in c for c in captions):
            print(f"  W10 reply_file evidence ok={ok_file} sent={captions!r}")

        sent.clear()
        ok_send = asyncio.run(refused_file("send_file", "sf-deny"))
        captions = [c for _to, c in sent]
        check(
            "W10 send_file allowlist runs before the caption and explains the refusal",
            ok_send is True
            and not any("这是说明" in c for c in captions)
            and any("不在允许发送的目录" in c for c in captions),
        )
    finally:
        secret.unlink(missing_ok=True)
        restore(saved)


# ---------------------------------------------------------------- W11


def test_w11():
    """Stuck-file notice claims a WeCom divert only after the append succeeds."""
    sbx, saved = bind("w11")
    try:
        payload = sbx / "report.bin"
        payload.write_bytes(b"payload")
        g = bare()
        g.context = {}
        item = {"id": "stuck1", "mode": "send_file", "to_user_id": "u1",
                "file_path": str(payload)}

        # Outbox file absent: divert cannot happen.
        missing = sbx / "no-such-wecom.jsonl"
        gw.STUCK_NOTICE_WECOM_OUTBOX = missing
        sent = []

        async def fake_send(_c, _creds, to, content, *_a, **_k):
            sent.append(content)
            return {"ret": 0}

        g.send_text = fake_send
        asyncio.run(g._notify_stuck_formal(None, {}, item, 3))
        text = sent[0] if sent else ""
        check(
            "W11 missing WeCom outbox does not claim the file was diverted",
            sent and "已把原文件转投" not in text and "尚未接受" in text,
        )
        if "已把原文件转投" in text:
            print(f"  W11 missing-outbox evidence text={text!r}")

        present = sbx / "wecom-outbox.jsonl"
        present.write_text("", encoding="utf-8")
        gw.STUCK_NOTICE_WECOM_OUTBOX = present
        order = []

        async def send_observing(_c, _creds, to, content, *_a, **_k):
            rows = read_jsonl(present)
            order.append((any(r.get("mode") == "send_file" for r in rows), content))
            return {"ret": 0}

        g.send_text = send_observing
        asyncio.run(g._notify_stuck_formal(None, {}, item, 3))
        rows = read_jsonl(present)
        claimed_after = (
            len(order) == 1
            and order[0][0] is True
            and "已把原文件转投" in order[0][1]
        )
        check(
            "W11 divert is appended before the notice claims it",
            claimed_after
            and any(r.get("mode") == "send_file" and r.get("file_path") == str(payload)
                    for r in rows),
        )
        if not claimed_after:
            print(f"  W11 order evidence {order!r}")
    finally:
        restore(saved)


# ---------------------------------------------------------------- W12


def test_w12():
    """Stop ack reports a write failure instead of claiming the stop was submitted."""
    sbx, saved = bind("w12")
    try:
        hook = gw.HOOK_STATE_DIR
        (hook / "subagent_jobs.json").write_text(json.dumps({
            "jobs": {"S3": {"status": "running", "text": "task"}},
        }), encoding="utf-8")
        (hook / "subagent_cmd.jsonl").mkdir()
        ack = gw.slash_subagent_stop_ack(hook, "S3")
        check(
            "W12 stop ack does not say submitted when the append fails",
            "已提交" not in ack and "失败" in ack,
        )
        if "已提交" in ack:
            print(f"  W12 evidence ack={ack!r}")
        shutil.rmtree(hook / "subagent_cmd.jsonl")
        ok_ack = gw.slash_subagent_stop_ack(hook, "S3")
        check(
            "W12 stop ack still says submitted when the append succeeds",
            "已提交停止 #S3" in ok_ack and (hook / "subagent_cmd.jsonl").is_file(),
        )
    finally:
        restore(saved)


# ---------------------------------------------------------------- W13


def test_w13():
    """The first matching approval submission wins."""
    _sbx, saved = bind("w13")
    try:
        relay = gw.APPROVAL_RELAY_DIR
        relay.mkdir(parents=True, exist_ok=True)
        (relay / "decisions.offset").write_text("0", encoding="utf-8")
        (relay / "decisions.jsonl").write_text(
            json.dumps({"num": "8", "decision": "deny"}) + "\n"
            + json.dumps({"num": "8", "decision": "allow_once"}) + "\n"
            + json.dumps({"num": "9", "decision": "allow_always"}) + "\n",
            encoding="utf-8",
        )
        found = gw._approval_pending_submission("8")
        check("W13 approval pending submission returns the first match", found == "deny")
        if found != "deny":
            print(f"  W13 evidence found={found!r}")
    finally:
        restore(saved)


# ---------------------------------------------------------------- W14


def test_w14():
    """Fullwidth digits and superscripts normalize to ASCII before lookup."""
    _sbx, saved = bind("w14")
    try:
        cases = (
            ("a", "１", ("1", "allow_once")),
            ("b", "²", ("2", "allow_always")),
            ("c", "①", ("1", "deny")),
        )
        ok = True
        detail = []
        for name, arg, expect in cases:
            try:
                parsed = gw.parse_approval_slash(name, arg)
                good = parsed == expect and int(parsed[0]) == int(expect[0])
            except Exception as exc:
                parsed = exc
                good = False
            ok = ok and good
            if not good:
                detail.append((name, arg, parsed))
        rejected = gw.parse_approval_slash("a", "١") is None
        check("W14 fullwidth and superscript digits normalize to ASCII", ok)
        check("W14 a non-ASCII digit token is rejected", rejected)
        if detail or not rejected:
            print(f"  W14 evidence bad={detail!r} arabic_rejected={rejected}")

        relay = gw.APPROVAL_RELAY_DIR
        relay.mkdir(parents=True, exist_ok=True)
        (relay / "state.json").write_text(json.dumps({
            "items": {"1": {"status": "pending", "who": "bob", "what": "ship"}},
        }), encoding="utf-8")
        parsed = gw.parse_approval_slash("a", "１")
        if parsed:
            ack = gw.slash_approval_decide_ack(parsed[0], parsed[1], "weixin")
        else:
            ack = ""
        check(
            "W14 /a １ resolves the ASCII approval #1",
            "没有找到" not in ack and "已提交" in ack and "#1" in ack,
        )
    finally:
        restore(saved)


# ---------------------------------------------------------------- W15


def test_w15():
    """Code spans are protected; everything else stays byte-identical."""
    outside = {
        "List<String>": "List",
        "a<b>c": "ac",
        "# Title": "Title",
        "arr[i](x)": "arri（x）",
        "plain [link](http://x.test) tail": "plain link（http://x.test） tail",
        "| a | b |\n|---|---|\n| 1 | 2 |": "a ｜ b\n1 ｜ 2",
        "---": "",
        "no change 你好": "no change 你好",
        "  # not a heading?": "  # not a heading?",
        "###### h6": "h6",
        "text <tag attr='x'> end": "text  end",
        "img ![alt](http://x) done": "img alt：http://x done",
        "same [http://x](http://x)": "same http://x",
        "`# not heading`\n# heading": "`# not heading`\nheading",
    }
    outside_ok = True
    for src, expect in outside.items():
        got = gw.filter_markdown_weixin(src)
        if got != expect:
            outside_ok = False
            print(f"  W15 outside changed {src!r} -> {got!r} expected {expect!r}")
    check("W15 non-code markdown filtering stays byte-identical", outside_ok)

    protected = {
        "see `List<String>` and `arr[i](x)`": "see `List<String>` and `arr[i](x)`",
        "```\n# Title\na<b>c\narr[i](x)\n```": "```\n# Title\na<b>c\narr[i](x)\n```",
        "```python\n# heading\nList<String>\n```\n# Real\nList<String>\narr[i](x)":
            "```python\n# heading\nList<String>\n```\nReal\nList\narri（x）",
        "a `b <c> d` e <f>": "a `b <c> d` e ",
        "# title `List<String>`": "title `List<String>`",
        "before `a[i](x)` after a[i](x)": "before `a[i](x)` after ai（x）",
    }
    protected_ok = True
    for src, expect in protected.items():
        got = gw.filter_markdown_weixin(src)
        if got != expect:
            protected_ok = False
            print(f"  W15 code mangled {src!r} -> {got!r} expected {expect!r}")
    check("W15 fenced and inline code skip heading, html, and link transforms", protected_ok)


# ---------------------------------------------------------------- W17


def test_w17():
    """A delivered formal row counts even when it is queued after this row."""
    _sbx, saved = bind("w17")
    try:
        gw.OUTBOX.write_text(
            json.dumps({"id": "text1", "mode": "reply", "msgid": "M"}) + "\n"
            + json.dumps({"id": "file1", "mode": "reply_file", "msgid": "M"}) + "\n",
            encoding="utf-8",
        )
        gw.OUTBOX_RESULTS.write_text(
            json.dumps({"id": "file1", "ok": True}) + "\n",
            encoding="utf-8",
        )
        hit = gw.Gateway._delivered_reply_before(
            {"id": "text1", "mode": "reply", "msgid": "M"})
        check(
            "W17 a later delivered reply_file does not suppress the parked text",
            hit is False,
        )
        if hit is not False:
            print(f"  W17 evidence delivered_before={hit!r}")

        gw.OUTBOX_RESULTS.write_text(
            json.dumps({"id": "file1", "ok": False}) + "\n",
            encoding="utf-8",
        )
        miss = gw.Gateway._delivered_reply_before(
            {"id": "text1", "mode": "reply", "msgid": "M"})
        check(
            "W17 a later undelivered file does not suppress the text",
            miss is False,
        )

        gw.OUTBOX.write_text(
            json.dumps({"id": "file-first", "mode": "reply_file", "msgid": "M7"}) + "\n"
            + json.dumps({"id": "late1", "mode": "update", "msgid": "M7"}) + "\n",
            encoding="utf-8",
        )
        gw.OUTBOX_RESULTS.write_text(
            json.dumps({"id": "file-first", "ok": True}) + "\n",
            encoding="utf-8",
        )
        late = gw.Gateway._delivered_reply_before({"id": "late1", "msgid": "M7"})
        check("W17 a prior delivered reply_file still suppresses a late update", late is True)
    finally:
        restore(saved)


def main():
    test_w1()
    test_w2()
    test_w3()
    test_w4()
    test_w5()
    test_w6()
    test_w7()
    test_w8()
    test_w9()
    test_w10()
    test_w11()
    test_w12()
    test_w13()
    test_w14()
    test_w15()
    test_w17()
    fails = [name for name, ok in RESULTS if not ok]
    print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
    if fails:
        print("FAILED: " + "; ".join(fails))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
