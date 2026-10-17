#!/usr/bin/env python3
"""Regression checks for the 2026-10-17 WeCom / channel_common defects.

Each check is named C1..C18 and is meant to fail on the pre-fix code.
State paths are sandboxed. Network sends are stubbed.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from channel_common import (  # noqa: E402
    _jsonl_dicts,
    allocate_subagent_id,
    merge_queue_admin,
    reply_block_reason_for_row,
)

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    """Record one named assertion and print it."""
    RESULTS.append((name, bool(cond), detail))
    line = ("PASS " if cond else "FAIL ") + name
    if detail and not cond:
        line += " :: " + detail
    print(line)


def load_gateway():
    """Load wecom-bot/gateway.py under a private module name."""
    spec = importlib.util.spec_from_file_location(
        "wc_sonnet_20261017", ROOT / "wecom-bot" / "gateway.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


wc = load_gateway()
LOGS: list[str] = []
wc.log = lambda msg: LOGS.append(str(msg))


def sandbox_outbox(directory: Path) -> None:
    """Point the gateway's outbox files at a throwaway directory."""
    directory.mkdir(parents=True, exist_ok=True)
    wc.OUTBOX = directory / "outbox.jsonl"
    wc.OUTBOX_OFFSET = directory / "outbox.offset"
    wc.OUTBOX_RESULTS = directory / "outbox_results.jsonl"
    wc.OUTBOX_RETRY = directory / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = directory / "outbox_partial.json"
    wc.OUTBOX_PARKED = directory / "outbox_parked.json"
    wc.CANCELLED_FILE = directory / "cancelled.json"
    wc.STATUS = directory / "status.json"
    wc.SEEN_FILE = directory / "seen_ids.jsonl"
    wc.MEDIA_DIR = directory / "media"
    wc.REQMAP = directory / "reqmap.json"
    wc.STATE = directory


def bare_gateway():
    """A Gateway without the process lock or a live socket."""
    gateway = wc.Gateway.__new__(wc.Gateway)
    gateway.seen_msgids = set()
    gateway.allow_users = {"alice"}
    gateway.reqmap = {}
    gateway.msgs_received = 0
    gateway.msgs_sent = 0
    gateway.open_streams = {}
    gateway.pending_responses = {}
    gateway.respond_locks = {}
    gateway.feedback_track = {}
    gateway.cards = {}
    gateway._http = None
    gateway._coalescer = None
    gateway._coalesce_pending = {}
    gateway.write_status = lambda: None
    gateway._save_reqmap = lambda: None
    return gateway


class _FrameStream:
    """Async iterator of already-encoded websocket frames."""

    def __init__(self, raws: list[str]) -> None:
        self._raws = list(raws)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._raws:
            raise StopAsyncIteration
        return self._raws.pop(0)


def _result_rows() -> list[dict]:
    if not wc.OUTBOX_RESULTS.exists():
        return []
    rows = []
    for line in wc.OUTBOX_RESULTS.read_text(encoding="utf-8").split("\n"):
        if line.strip():
            rows.append(json.loads(line))
    return rows


async def check_c1_reader() -> None:
    """Null headers must not kill the reader; handler errors must be logged."""
    gateway = bare_gateway()
    gateway.ws = _FrameStream([
        json.dumps({"cmd": "aibot_pong", "headers": None, "errcode": 0}),
    ])
    try:
        await gateway.reader_loop()
        survived = True
        err = ""
    except Exception as exc:
        survived = False
        err = repr(exc)
    check(
        "C1 headers-null does not break reader_loop",
        survived,
        err,
    )

    before = len(LOGS)
    gateway.ws = _FrameStream([
        json.dumps({
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-c1"},
            "body": {
                "msgid": "m-c1",
                "msgtype": "mixed",
                "mixed": {"msg_item": ["not-a-dict"]},
                "from": {"userid": "alice"},
            },
        }),
    ])
    await gateway.reader_loop()
    pending = list(getattr(gateway, "_callback_tasks", ()) or ())
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    await asyncio.sleep(0)
    spawned = LOGS[before:]
    logged = any("m-c1" in line and "callback task failed" in line for line in spawned)
    check(
        "C1 handler exception is logged with msgid",
        logged,
        "logs=" + " | ".join(spawned[-4:]),
    )


async def check_c2_chunk_resume() -> None:
    """A confirmed send chunk must not be sent again when a later chunk fails."""
    directory = Path(tempfile.mkdtemp(prefix="c2-"))
    sandbox_outbox(directory)
    gateway = bare_gateway()
    content = ("A" * 4000) + ("B" * 4000)
    calls: list[str] = []

    async def scripted_send(frame, wait_response=False, timeout=10.0):
        part = (((frame.get("body") or {}).get("markdown") or {}).get("content") or "")
        calls.append(part)
        if part.startswith("B"):
            return {"errcode": -1, "errmsg": "response timeout"}
        return {"errcode": 0, "errmsg": ""}

    gateway.send_frame = scripted_send
    item = {
        "id": "send-c2",
        "mode": "send",
        "chatid": "chat-c2",
        "chat_type": 1,
        "content": content,
    }
    first_ok = await gateway.dispatch_outbox_item(item)
    partial = {}
    if wc.OUTBOX_PARTIAL.exists():
        partial = json.loads(wc.OUTBOX_PARTIAL.read_text(encoding="utf-8"))
    sent = int((partial.get("send-c2") or {}).get("sent_chunks") or 0)
    calls.clear()
    second_ok = await gateway.dispatch_outbox_item(item)
    resent_prefix = any(part.startswith("A") for part in calls)
    check(
        "C2 retry does not resend a confirmed chunk",
        first_ok is False and sent == 1 and calls == [("B" * 4000)] and resent_prefix is False,
        f"first_ok={first_ok} sent={sent} second_ok={second_ok} calls={len(calls)} resent_prefix={resent_prefix}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c3_coalesce_offset() -> None:
    """A buffered send must stay in the outbox until it is actually flushed."""
    directory = Path(tempfile.mkdtemp(prefix="c3-"))
    sandbox_outbox(directory)
    row = {
        "id": "send-c3",
        "mode": "send",
        "chatid": "chat-c3",
        "chat_type": 1,
        "content": "only in memory",
    }
    wc.OUTBOX.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
    wc.OUTBOX_OFFSET.write_text("0", encoding="utf-8")
    co_mod = sys.modules.get("wecom_outbound_coalesce_v2")
    clock = {"t": 0.0}
    original_mono = co_mod.time.monotonic if co_mod is not None else None
    if co_mod is not None:
        co_mod.time.monotonic = lambda: clock["t"]

    gateway = bare_gateway()
    dispatched: list[str] = []

    async def record(item):
        dispatched.append(str(item.get("id")))
        return True

    async def no_watchdog():
        return None

    gateway.dispatch_outbox_item = record
    gateway.check_stream_watchdog = no_watchdog
    sleeps = {"n": 0}
    original_sleep = wc.asyncio.sleep

    async def stop_after_feed(_seconds):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise asyncio.CancelledError()

    wc.asyncio.sleep = stop_after_feed
    try:
        await gateway.outbox_loop()
    except asyncio.CancelledError:
        pass
    finally:
        wc.asyncio.sleep = original_sleep
    offset = int(wc.OUTBOX_OFFSET.read_text(encoding="utf-8").strip() or "0")
    check(
        "C3 buffered send does not advance the outbox offset",
        offset == 0 and dispatched == [],
        f"offset={offset} dispatched={dispatched}",
    )

    restarted = bare_gateway()
    restarted.dispatch_outbox_item = record
    restarted.check_stream_watchdog = no_watchdog
    sleeps2 = {"n": 0}

    async def flush_then_stop(_seconds):
        sleeps2["n"] += 1
        if sleeps2["n"] == 2:
            clock["t"] = 5.0
        if sleeps2["n"] >= 3:
            raise asyncio.CancelledError()

    wc.asyncio.sleep = flush_then_stop
    try:
        await restarted.outbox_loop()
    except asyncio.CancelledError:
        pass
    finally:
        wc.asyncio.sleep = original_sleep
        if co_mod is not None and original_mono is not None:
            co_mod.time.monotonic = original_mono
    check(
        "C3 restarted process still delivers the unflushed send",
        dispatched == ["send-c3"],
        f"dispatched={dispatched}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c4_oversize_permanent() -> None:
    """A file over the 100-chunk cap is a permanent failure, not a retry."""
    directory = Path(tempfile.mkdtemp(prefix="c4-"))
    sandbox_outbox(directory)
    payload = directory / "huge.bin"
    payload.write_bytes(b"x")
    gateway = bare_gateway()
    gateway.reqmap = {
        "m-c4": {"req_id": "req-c4", "chatid": "chat-c4", "chattype": "single"},
    }
    notices: list[str] = []

    class _Huge:
        def __len__(self) -> int:
            return 100 * 512 * 1024 + 1

    async def huge_bytes(_path):
        return _Huge(), "file", "huge.bin"

    async def respond(req_id, body, timeout=10.0):
        text = ((body.get("markdown") or {}).get("content") or "")
        notices.append(text)
        return {"errcode": 0}

    async def send_frame(frame, wait_response=False, timeout=10.0):
        text = ((((frame.get("body") or {}).get("markdown") or {}).get("content")) or "")
        notices.append(text)
        return {"errcode": 0}

    gateway._outbound_media_bytes = huge_bytes
    gateway.respond = respond
    gateway.send_frame = send_frame
    item = {
        "id": "file-c4",
        "mode": "reply_file",
        "msgid": "m-c4",
        "file_path": str(payload),
    }
    ok = await gateway.dispatch_outbox_item(item)
    rows = [row for row in _result_rows() if row.get("id") == "file-c4"]
    told = any("huge.bin" in text for text in notices)
    permanent = bool(rows) and rows[-1].get("ok") is False and rows[-1].get("deadletter") is True
    check(
        "C4 oversize upload is permanent and tells the user",
        ok is True and permanent and told,
        f"ok={ok} result={rows[-1] if rows else None} notices={notices}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c5_seen_atomic() -> None:
    """Two in-flight callbacks with one msgid must process it once."""
    directory = Path(tempfile.mkdtemp(prefix="c5-"))
    sandbox_outbox(directory)
    gateway = bare_gateway()
    downloads: list[str] = []

    async def slow_download(_media, msgid):
        downloads.append(str(msgid))
        await asyncio.sleep(0.05)

    async def respond(_req_id, _body, timeout=10.0):
        return {"errcode": 0}

    gateway._download_media = slow_download
    gateway.respond = respond
    gateway.append_jsonl = lambda _path, _obj: None
    frame = {
        "headers": {"req_id": "req-c5"},
        "body": {
            "msgid": "dup-1",
            "msgtype": "image",
            "chattype": "single",
            "from": {"userid": "alice"},
            "image": {"url": "https://weixin.qq.com/a.jpg", "aeskey": "abcd"},
        },
    }
    await asyncio.gather(
        gateway.handle_message_callback(frame),
        gateway.handle_message_callback(dict(frame)),
    )
    check(
        "C5 duplicate msgid is processed once",
        downloads == ["dup-1"],
        f"downloads={downloads}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c6_pending_cleanup() -> None:
    """A failed or cancelled send must drop its pending response entry."""
    gateway = bare_gateway()

    async def broken_send(_payload):
        raise RuntimeError("socket dead")

    gateway.ws = type("WS", (), {"send": staticmethod(broken_send)})()
    try:
        await gateway.send_frame(
            {"headers": {"req_id": "req-c6-send"}},
            wait_response=True,
            timeout=1.0,
        )
    except RuntimeError:
        pass
    leaked_send = "req-c6-send" in gateway.pending_responses

    async def hang_send(_payload):
        await asyncio.Event().wait()

    gateway.ws = type("WS", (), {"send": staticmethod(hang_send)})()
    task = asyncio.create_task(gateway.send_frame(
        {"headers": {"req_id": "req-c6-cancel"}},
        wait_response=True,
        timeout=30.0,
    ))
    for _ in range(20):
        if "req-c6-cancel" in gateway.pending_responses:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    leaked_cancel = "req-c6-cancel" in gateway.pending_responses
    check(
        "C6 pending_responses drops failed and cancelled sends",
        not leaked_send and not leaked_cancel,
        f"leaked_send={leaked_send} leaked_cancel={leaked_cancel} pending={list(gateway.pending_responses)}",
    )


async def check_c7_media_policy() -> None:
    """Inbound media must refuse non-Tencent hosts and must not follow redirects."""
    directory = Path(tempfile.mkdtemp(prefix="c7-"))
    sandbox_outbox(directory)
    gateway = bare_gateway()
    client = gateway._http_client()
    redirects = bool(getattr(client, "follow_redirects", True))
    fetched: list[str] = []

    async def fake_download(_client, url, *_args, **_kwargs):
        fetched.append(url)
        raise RuntimeError("download stub")

    wc.download_inbound_cipher = fake_download
    await gateway._download_media(
        [{"url": "https://evil.example/secret", "aeskey": "abcd"}],
        "m-c7",
    )
    await client.aclose()
    check(
        "C7 media download uses the allowlist and does not follow redirects",
        redirects is False and fetched == [],
        f"follow_redirects={redirects} fetched={fetched}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c8_allowlist_closed() -> None:
    """An empty WECOM_ALLOW_USERS must deny inbound, with one throttled log."""
    directory = Path(tempfile.mkdtemp(prefix="c8-"))
    sandbox_outbox(directory)
    gateway = bare_gateway()
    gateway.allow_users = set()
    before = len(LOGS)
    event_open = gateway._event_allowed("stranger")
    event_again = gateway._event_allowed("stranger")
    fresh = LOGS[before:]
    empty_logs = [line for line in fresh if "WECOM_ALLOW_USERS" in line and "empty" in line]
    inbox: list[dict] = []

    def capture_append(_path, obj):
        inbox.append(obj)

    gateway.append_jsonl = capture_append

    async def no_slash(*_args, **_kwargs):
        return None

    async def respond(_req, _body, timeout=10.0):
        return {"errcode": 0}

    gateway._slash_dispatch = no_slash
    gateway.respond = respond
    await gateway.handle_message_callback({
        "headers": {"req_id": "req-c8"},
        "body": {
            "msgid": "m-c8",
            "msgtype": "text",
            "chattype": "single",
            "from": {"userid": "alice"},
            "text": {"content": "hello"},
        },
    })
    check(
        "C8 empty allowlist denies events and messages",
        event_open is False and event_again is False and len(empty_logs) == 1 and inbox == [],
        f"event={event_open}/{event_again} logs={empty_logs} inbox={len(inbox)}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c9_notify_after_save() -> None:
    """A stuck-formal notice is sent only after the parked record is durable."""
    directory = Path(tempfile.mkdtemp(prefix="c9-"))
    sandbox_outbox(directory)
    gateway = bare_gateway()
    item = {
        "id": "stuck-c9",
        "mode": "reply",
        "msgid": "m-c9",
        "chatid": "chat-c9",
        "content": "answer",
    }
    wc.OUTBOX_PARKED.write_text(json.dumps({
        "stuck-c9": {"item": item, "n": 2, "parked_at": 1, "next": 0},
    }), encoding="utf-8")
    notices: list[int] = []

    def notice(_item, attempts):
        notices.append(attempts)

    gateway._notify_stuck_formal = notice
    original = wc.atomic_write_text

    def fail_parked(path, text, mode=0o644):
        if Path(path) == wc.OUTBOX_PARKED:
            raise OSError("disk full")
        return original(path, text, mode)

    wc.atomic_write_text = fail_parked
    try:
        gateway._note_failure(dict(item))
    finally:
        wc.atomic_write_text = original
    check(
        "C9 stuck notice waits for a successful parked save",
        notices == [],
        f"notices={notices}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def check_c10_reply_file_fallback() -> None:
    """reply_file uses a proactive send when the callback req_id is dead."""
    directory = Path(tempfile.mkdtemp(prefix="c10-"))
    sandbox_outbox(directory)
    payload = directory / "note.txt"
    payload.write_text("hello", encoding="utf-8")
    gateway = bare_gateway()
    gateway.reqmap = {
        "m-c10": {
            "req_id": "req-dead",
            "chatid": "chat-c10",
            "chattype": "single",
        },
    }
    sent: list[dict] = []

    async def respond(_req_id, _body, timeout=10.0):
        return {"errcode": 846001, "errmsg": "req_id expired"}

    async def send_frame(frame, wait_response=False, timeout=10.0):
        sent.append(frame)
        return {"errcode": 0, "errmsg": ""}

    async def upload(_data, _mtype, _name):
        return "media-c10"

    gateway.respond = respond
    gateway.send_frame = send_frame
    gateway.upload_media = upload
    ok = await gateway.dispatch_outbox_item({
        "id": "file-c10",
        "mode": "reply_file",
        "msgid": "m-c10",
        "file_path": str(payload),
    })
    body = (sent[0].get("body") if sent else {}) or {}
    delivered = (
        ok is True
        and sent
        and sent[0].get("cmd") == "aibot_send_msg"
        and (body.get("file") or {}).get("media_id") == "media-c10"
    )
    check(
        "C10 reply_file falls back to a proactive send",
        delivered,
        f"ok={ok} frames={sent}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c11_approval_lock() -> None:
    """A decision append is serialized: a peer holding the lock wins once."""
    directory = Path(tempfile.mkdtemp(prefix="c11-"))
    relay = directory / "relay"
    relay.mkdir()
    (relay / "state.json").write_text(json.dumps({
        "items": {"1": {"status": "pending", "who": "bob", "what": "ship"}},
    }), encoding="utf-8")
    decisions = relay / "decisions.jsonl"
    previous = wc.APPROVAL_RELAY_DIR
    wc.APPROVAL_RELAY_DIR = relay
    lock_path = relay / "decisions.lock"
    holder = lock_path.open("a+")
    import fcntl
    fcntl.flock(holder, fcntl.LOCK_EX)
    outcome: list[str] = []

    def decide() -> None:
        try:
            outcome.append(wc.slash_approval_decide_ack("1", "allow_once", "wecom"))
        except Exception as exc:
            outcome.append(f"error:{exc!r}")

    worker = threading.Thread(target=decide)
    worker.start()
    worker.join(0.4)
    blocked = worker.is_alive()
    with decisions.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps({"num": "1", "decision": "deny", "channel": "other", "ts": 1}) + "\n"
        )
    fcntl.flock(holder, fcntl.LOCK_UN)
    holder.close()
    worker.join(2.0)
    wc.APPROVAL_RELAY_DIR = previous
    lines = [line for line in decisions.read_text(encoding="utf-8").split("\n") if line.strip()]
    check(
        "C11 approval decide waits for the file lock and does not double-append",
        blocked and len(lines) == 1 and lines[0].find('"deny"') >= 0,
        f"blocked={blocked} lines={lines} outcome={outcome}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c12_subagent_seq() -> None:
    """A corrupt sequence file is an error, and the update itself is locked."""
    directory = Path(tempfile.mkdtemp(prefix="c12-"))
    path = directory / "subagent_seq.json"
    path.write_text("{", encoding="utf-8")
    allocated = allocate_subagent_id(directory)
    check(
        "C12 corrupt subagent sequence is not reset to S1",
        allocated is None and path.read_text(encoding="utf-8") == "{",
        f"allocated={allocated!r} file={path.read_text(encoding='utf-8')!r}",
    )

    fresh = Path(tempfile.mkdtemp(prefix="c12b-"))
    import fcntl
    lock_path = fresh / "subagent_seq.lock"
    holder = lock_path.open("a+")
    fcntl.flock(holder, fcntl.LOCK_EX)
    outcome: list[str | None] = []

    def allocate() -> None:
        try:
            outcome.append(allocate_subagent_id(fresh))
        except Exception as exc:
            outcome.append(f"error:{exc!r}")

    worker = threading.Thread(target=allocate)
    worker.start()
    worker.join(0.4)
    blocked = worker.is_alive()
    fcntl.flock(holder, fcntl.LOCK_UN)
    holder.close()
    worker.join(2.0)
    check(
        "C12 subagent id allocation holds the sequence lock",
        blocked and outcome == ["S1"],
        f"blocked={blocked} outcome={outcome}",
    )
    shutil.rmtree(directory, ignore_errors=True)
    shutil.rmtree(fresh, ignore_errors=True)


def check_c13_stale_admin() -> None:
    """An old unconsumed queue-admin file is merged, not discarded."""
    merged = merge_queue_admin(
        {"ts": 1, "action": "drop", "msgids": ["old"]},
        "drop",
        ["new"],
        now=100,
    )
    check(
        "C13 stale unconsumed queue admin is merged",
        merged.get("msgids") == ["old", "new"] and merged.get("action") == "drop",
        f"merged={merged}",
    )


async def check_c14_slash_ack() -> None:
    """An ack failure after a committed slash command must not fall through."""
    directory = Path(tempfile.mkdtemp(prefix="c14-"))
    previous = wc.HOOK_STATE_DIR
    wc.HOOK_STATE_DIR = directory
    gateway = bare_gateway()

    async def boom(*_args, **_kwargs):
        raise RuntimeError("ack transport down")

    gateway._send_slash_ack = boom
    try:
        result = await gateway._slash_dispatch("/new", "chat-c14", "single", "m-c14", "req-c14")
    finally:
        wc.HOOK_STATE_DIR = previous
    boundary = directory / "topic_boundary.json"
    check(
        "C14 slash ack failure stays handled",
        result == "handled" and boundary.exists(),
        f"result={result!r} boundary={boundary.exists()}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c15_fullwidth_digits() -> None:
    """Fullwidth and superscript approval numbers resolve to ASCII digits."""
    fullwidth = wc.parse_approval_slash("a", "１")
    superscript = wc.parse_approval_slash("b", unicodedata.normalize("NFKC", "²") and "²")
    plain = wc.parse_approval_slash("c", "12")
    garbage = wc.parse_approval_slash("a", "１x")
    check(
        "C15 approval numbers normalize to ASCII digits",
        fullwidth == ("1", "allow_once")
        and superscript == ("2", "allow_always")
        and plain == ("12", "deny")
        and garbage is None,
        f"fullwidth={fullwidth!r} superscript={superscript!r} plain={plain!r} garbage={garbage!r}",
    )


def check_c16_formal_order() -> None:
    """A delivered reply_file suppresses an earlier parked text reply."""
    directory = Path(tempfile.mkdtemp(prefix="c16-"))
    sandbox_outbox(directory)
    wc.OUTBOX.write_text(
        json.dumps({"id": "text1", "mode": "reply", "msgid": "M"}) + "\n"
        + json.dumps({"id": "file1", "mode": "reply_file", "msgid": "M"}) + "\n",
        encoding="utf-8",
    )
    wc.OUTBOX_RESULTS.write_text(
        json.dumps({"id": "file1", "mode": "reply_file", "ok": True}) + "\n",
        encoding="utf-8",
    )
    suppressed = wc.Gateway._delivered_reply_before(
        {"id": "text1", "mode": "reply", "msgid": "M"})
    reason = reply_block_reason_for_row(
        "M",
        {"id": "file1", "mode": "reply_file", "msgid": "M", "queued_at": 1},
        {"id": "file1", "ok": True},
        now=10,
    )
    check(
        "C16 delivered reply_file counts regardless of row order",
        suppressed is True and reason is not None and "delivered" in reason,
        f"suppressed={suppressed!r} reason={reason!r}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c17_jsonl_separators() -> None:
    """JSONL readers keep U+2028 / U+2029 / U+0085 inside a JSON string."""
    directory = Path(tempfile.mkdtemp(prefix="c17-"))
    path = directory / "rows.jsonl"
    row = {"msgid": "m-c17", "text": "a\u2028b\u2029c\u0085d"}
    path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
    got = _jsonl_dicts(path)
    check(
        "C17 JSONL read keeps unicode line separators inside strings",
        got == [row],
        f"got={got!r}",
    )
    shutil.rmtree(directory, ignore_errors=True)


def check_c18_idle_gateway() -> None:
    """An idle connected process is not reported dead from status file age."""
    directory = Path(tempfile.mkdtemp(prefix="c18-"))
    state = directory / "state"
    hook = directory / "hook"
    state.mkdir()
    hook.mkdir()
    (hook / "active_batch.json").write_text("{}", encoding="utf-8")
    (hook / "pending.json").write_text("{}", encoding="utf-8")
    (hook / "seen_msgids.txt").write_text("", encoding="utf-8")
    now = time.time()
    (state / "status.json").write_text(json.dumps({
        "state": "connected",
        "connected": True,
        "updated_at": now - 500,
        "last_error": "",
        "pid": os.getpid(),
    }), encoding="utf-8")
    (state / "outbox_results.jsonl").write_text(
        json.dumps({"ok": True, "ts": int(now)}) + "\n",
        encoding="utf-8",
    )
    alive = wc.slash_run_check("企业微信", state, hook)
    alive_line = next((line for line in alive.splitlines() if "网关状态文件" in line), "")
    (state / "status.json").write_text(json.dumps({
        "state": "connected",
        "connected": True,
        "updated_at": now - 500,
        "last_error": "",
        "pid": 2 ** 30,
    }), encoding="utf-8")
    dead = wc.slash_run_check("企业微信", state, hook)
    dead_line = next((line for line in dead.splitlines() if "网关状态文件" in line), "")
    check(
        "C18 idle live gateway is not a stale-file failure",
        alive_line.startswith("✓") and dead_line.startswith("✗") and "已超 90 秒未刷新" in dead_line,
        f"alive={alive_line!r} dead={dead_line!r}",
    )
    shutil.rmtree(directory, ignore_errors=True)


async def async_main() -> None:
    await check_c1_reader()
    await check_c2_chunk_resume()
    await check_c3_coalesce_offset()
    await check_c4_oversize_permanent()
    await check_c5_seen_atomic()
    await check_c6_pending_cleanup()
    await check_c7_media_policy()
    await check_c8_allowlist_closed()
    await check_c10_reply_file_fallback()
    await check_c14_slash_ack()


def main() -> None:
    asyncio.run(async_main())
    check_c9_notify_after_save()
    check_c11_approval_lock()
    check_c12_subagent_seq()
    check_c13_stale_admin()
    check_c15_fullwidth_digits()
    check_c16_formal_order()
    check_c17_jsonl_separators()
    check_c18_idle_gateway()
    failed = [name for name, ok, _detail in RESULTS if not ok]
    print(f"\n== {len(RESULTS) - len(failed)}/{len(RESULTS)} passed ==")
    if failed:
        print("FAILED: " + ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
