#!/usr/bin/env python3
"""Regression tests for the four todo-28 residuals.

Each case is written against the pre-fix gateways and must fail there:
B1 same-mode suppression, B2 merged-send chunk resume, B3 send-id
crash window, B4 WeCom approval/JSONL readers.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from channel_common import reply_block_reason_for_row  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    """Record one named assertion and print it."""
    RESULTS.append((name, bool(cond), detail))
    line = ("PASS " if cond else "FAIL ") + name
    if detail and not cond:
        line += " :: " + detail
    print(line)


def load_module(name: str, path: Path):
    """Load a gateway module under a private name."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


wx = load_module("wx_todo28_residuals", ROOT / "weixin-bot" / "gateway.py")
wc = load_module("wc_todo28_residuals", ROOT / "wecom-bot" / "gateway.py")


def sandbox(mod, directory: Path) -> None:
    """Point one gateway's outbox files at an empty directory."""
    directory.mkdir(parents=True, exist_ok=True)
    mod.OUTBOX = directory / "outbox.jsonl"
    mod.OUTBOX_RESULTS = directory / "outbox_results.jsonl"
    mod.OUTBOX_OFFSET = directory / "outbox.offset"
    mod.OUTBOX_PARTIAL = directory / "outbox_partial.json"
    mod.OUTBOX_PARKED = directory / "outbox_parked.json"
    mod.OUTBOX_RETRY = directory / "outbox_retry.json"
    mod.CANCELLED_FILE = directory / "cancelled.json"
    mod.STATUS = directory / "status.json"
    mod.INBOX = directory / "inbox.jsonl"
    mod.SEEN_FILE = directory / "seen_ids.jsonl"
    mod.REQMAP = directory / "reqmap.json"
    mod.CARDS = directory / "cards.json"
    if hasattr(mod, "COMPRESSED_DIR"):
        mod.COMPRESSED_DIR = directory / "compressed"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    """Replace a JSONL file with these objects."""
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def result_for(mod, item_id: str) -> dict | None:
    """Last result row for item_id, or None."""
    if not mod.OUTBOX_RESULTS.exists():
        return None
    found = None
    for line in mod.OUTBOX_RESULTS.read_text(encoding="utf-8").split("\n"):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("id") == item_id:
            found = row
    return found


def block_reason(enqueue_mode: str, row: dict, result: dict | None, now: float = 10):
    """Call the CLI gate, passing enqueue_mode once the parameter exists."""
    kwargs = {}
    if "enqueue_mode" in inspect.signature(reply_block_reason_for_row).parameters:
        kwargs["enqueue_mode"] = enqueue_mode
    return reply_block_reason_for_row(
        str(row.get("msgid") or ""),
        row,
        result,
        now,
        **kwargs,
    )


def bare_weixin():
    """Weixin gateway object without the process lock."""
    gateway = wx.Gateway.__new__(wx.Gateway)
    gateway.context = {}
    gateway.feedback_track = {}
    gateway.write_status = lambda: None

    async def no_typing(*_args, **_kwargs):
        return None

    gateway._typing_best_effort = no_typing

    async def send_text(self, client, creds, to, content,
                        token_ctx="", client_id="",
                        message_state=2, item_id=""):
        send_text.calls.append(content)
        return {"ret": 0}

    send_text.calls = []
    gateway.send_text = send_text.__get__(gateway)

    async def deliver_file(self, client, creds, to, fpath, token_ctx=""):
        deliver_file.calls.append(fpath)
        return {"ret": 0}

    deliver_file.calls = []
    gateway._deliver_file = deliver_file.__get__(gateway)
    return gateway, send_text.calls, deliver_file.calls


def bare_wecom():
    """WeCom gateway object without the process lock or a socket."""
    gateway = wc.Gateway.__new__(wc.Gateway)
    gateway.reqmap = {}
    gateway.msgs_sent = 0
    gateway.open_streams = {}
    gateway.feedback_track = {}
    gateway.pending_responses = {}
    gateway.respond_locks = {}
    gateway.cards = {}
    gateway._coalescer = None
    gateway._coalesce_pending = {}
    gateway._offset_pending_ids = set()
    gateway.write_status = lambda: None
    gateway._save_reqmap = lambda: None

    async def respond(self, req_id, body, timeout=10.0):
        text = ""
        stream = body.get("stream") if isinstance(body, dict) else None
        if isinstance(stream, dict):
            text = str(stream.get("content") or "")
        markdown = body.get("markdown") if isinstance(body, dict) else None
        if isinstance(markdown, dict):
            text = str(markdown.get("content") or "")
        respond.calls.append(text)
        return {"errcode": 0, "errmsg": ""}

    respond.calls = []
    gateway.respond = respond.__get__(gateway)

    async def upload_media(self, data, mtype, name):
        upload_media.calls.append(name)
        return "media-1"

    upload_media.calls = []
    gateway.upload_media = upload_media.__get__(gateway)

    async def send_frame(self, frame, wait_response=False, timeout=10.0):
        part = (((frame.get("body") or {}).get("markdown") or {}).get("content") or "")
        send_frame.calls.append(part)
        return {"errcode": 0, "errmsg": ""}

    send_frame.calls = []
    gateway.send_frame = send_frame.__get__(gateway)
    return gateway, respond.calls, upload_media.calls, send_frame.calls


def route_weixin(gateway, msgid: str) -> None:
    """Give one msgid a reply route."""
    gateway.context[msgid] = {
        "from_user_id": "u1",
        "context_token": "tok",
        "client_id": "c1",
    }


def route_wecom(gateway, msgid: str) -> None:
    """Give one msgid a callback route."""
    gateway.reqmap[msgid] = {"req_id": "req-1", "stream_id": "stream-1", "chatid": "chat-1"}
    gateway.open_streams[msgid] = {"stream_id": "stream-1"}


async def dispatch_weixin(gateway, item: dict) -> bool:
    """Dispatch one Weixin outbox row."""
    return await gateway.dispatch_outbox_item(None, {}, item)


async def check_b1_weixin(directory: Path) -> None:
    """Weixin delivered-check is same-mode, and updates stay broad."""
    sandbox(wx, directory)
    media = directory / "clip.mp4"
    other = directory / "still.png"
    media.write_bytes(b"clip")
    other.write_bytes(b"still")
    msgid = "b1-wx"
    gateway, sent, files = bare_weixin()
    route_weixin(gateway, msgid)

    write_jsonl(wx.OUTBOX, [
        {"id": "text1", "mode": "reply", "msgid": msgid, "content": "答案正文"},
        {"id": "file1", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
    ])
    write_jsonl(wx.OUTBOX_RESULTS, [
        {"id": "text1", "mode": "reply", "ok": False},
        {"id": "file1", "mode": "reply_file", "ok": True},
    ])
    text_ok = await dispatch_weixin(gateway, {
        "id": "text1", "mode": "reply", "msgid": msgid, "content": "答案正文",
    })
    text_res = result_for(wx, "text1")
    check(
        "B1a weixin parked text is delivered after the companion file",
        text_ok is True
        and text_res is not None
        and text_res.get("ok") is True
        and text_res.get("suppressed") is not True
        and sent == ["答案正文"],
        f"ok={text_ok} result={text_res} sent={sent!r}",
    )

    write_jsonl(wx.OUTBOX, [
        {"id": "r1", "mode": "reply", "msgid": msgid, "content": "第一份"},
        {"id": "r2", "mode": "reply", "msgid": msgid, "content": "第二份"},
    ])
    write_jsonl(wx.OUTBOX_RESULTS, [
        {"id": "r1", "mode": "reply", "ok": True},
    ])
    sent.clear()
    second = await dispatch_weixin(gateway, {
        "id": "r2", "mode": "reply", "msgid": msgid, "content": "第二份",
    })
    second_res = result_for(wx, "r2")
    check(
        "B1b weixin second reply row is suppressed",
        second is True
        and second_res is not None
        and second_res.get("suppressed") is True
        and sent == [],
        f"second={second} result={second_res} sent={sent!r}",
    )

    write_jsonl(wx.OUTBOX, [
        {"id": "f1", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
        {"id": "f2", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
    ])
    write_jsonl(wx.OUTBOX_RESULTS, [
        {"id": "f1", "mode": "reply_file", "ok": True},
    ])
    files.clear()
    dup = await dispatch_weixin(gateway, {
        "id": "f2", "mode": "reply_file", "msgid": msgid,
        "file_path": str(media), "content": "",
    })
    dup_res = result_for(wx, "f2")
    check(
        "B1c weixin duplicate reply_file row is suppressed",
        dup is True
        and dup_res is not None
        and dup_res.get("suppressed") is True
        and files == [],
        f"dup={dup} result={dup_res} files={files!r}",
    )

    write_jsonl(wx.OUTBOX_RESULTS, [
        {"id": "f1", "mode": "reply_file", "ok": True},
    ])
    files.clear()
    distinct = await dispatch_weixin(gateway, {
        "id": "f3", "mode": "reply_file", "msgid": msgid,
        "file_path": str(other), "content": "",
    })
    distinct_res = result_for(wx, "f3")
    check(
        "B1c weixin distinct reply_file paths both deliver",
        distinct is True
        and distinct_res is not None
        and distinct_res.get("ok") is True
        and distinct_res.get("suppressed") is not True
        and files == [str(other)],
        f"distinct={distinct} result={distinct_res} files={files!r}",
    )

    write_jsonl(wx.OUTBOX, [
        {"id": "file-d", "mode": "reply_file", "msgid": msgid, "file_path": str(media)},
        {"id": "upd", "mode": "update", "msgid": msgid, "content": "还在做"},
    ])
    write_jsonl(wx.OUTBOX_RESULTS, [
        {"id": "file-d", "mode": "reply_file", "ok": True},
    ])
    sent.clear()
    update_ok = await dispatch_weixin(gateway, {
        "id": "upd", "mode": "update", "msgid": msgid, "content": "还在做",
    })
    update_res = result_for(wx, "upd")
    check(
        "B1d weixin update after any formal delivery is suppressed",
        update_ok is True
        and update_res is not None
        and update_res.get("suppressed") is True
        and sent == [],
        f"update={update_ok} result={update_res} sent={sent!r}",
    )


async def check_b1_wecom(directory: Path) -> None:
    """WeCom delivered-check matches the Weixin same-mode rule."""
    sandbox(wc, directory)
    media = directory / "clip.mp4"
    other = directory / "still.png"
    media.write_bytes(b"clip")
    other.write_bytes(b"still")
    msgid = "b1-wc"
    gateway, replies, uploads, _frames = bare_wecom()
    route_wecom(gateway, msgid)

    write_jsonl(wc.OUTBOX, [
        {"id": "text1", "mode": "reply", "msgid": msgid, "content": "答案正文"},
        {"id": "file1", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
    ])
    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "text1", "mode": "reply", "ok": False},
        {"id": "file1", "mode": "reply_file", "ok": True},
    ])
    text_ok = await gateway.dispatch_outbox_item({
        "id": "text1", "mode": "reply", "msgid": msgid, "content": "答案正文",
    })
    text_res = result_for(wc, "text1")
    check(
        "B1a wecom parked text is delivered after the companion file",
        text_ok is True
        and text_res is not None
        and text_res.get("ok") is True
        and text_res.get("suppressed") is not True
        and replies == ["答案正文"],
        f"ok={text_ok} result={text_res} replies={replies!r}",
    )

    write_jsonl(wc.OUTBOX, [
        {"id": "r1", "mode": "reply", "msgid": msgid, "content": "第一份"},
        {"id": "r2", "mode": "reply", "msgid": msgid, "content": "第二份"},
    ])
    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "r1", "mode": "reply", "ok": True},
    ])
    replies.clear()
    second = await gateway.dispatch_outbox_item({
        "id": "r2", "mode": "reply", "msgid": msgid, "content": "第二份",
    })
    second_res = result_for(wc, "r2")
    check(
        "B1b wecom second reply row is suppressed",
        second is True
        and second_res is not None
        and second_res.get("suppressed") is True
        and replies == [],
        f"second={second} result={second_res} replies={replies!r}",
    )

    write_jsonl(wc.OUTBOX, [
        {"id": "f1", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
        {"id": "f2", "mode": "reply_file", "msgid": msgid,
         "file_path": str(media), "content": ""},
    ])
    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "f1", "mode": "reply_file", "ok": True},
    ])
    uploads.clear()
    dup = await gateway.dispatch_outbox_item({
        "id": "f2", "mode": "reply_file", "msgid": msgid,
        "file_path": str(media), "content": "",
    })
    dup_res = result_for(wc, "f2")
    check(
        "B1c wecom duplicate reply_file row is suppressed",
        dup is True
        and dup_res is not None
        and dup_res.get("suppressed") is True
        and uploads == [],
        f"dup={dup} result={dup_res} uploads={uploads!r}",
    )

    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "f1", "mode": "reply_file", "ok": True},
    ])
    uploads.clear()
    distinct = await gateway.dispatch_outbox_item({
        "id": "f3", "mode": "reply_file", "msgid": msgid,
        "file_path": str(other), "content": "",
    })
    distinct_res = result_for(wc, "f3")
    check(
        "B1c wecom distinct reply_file paths both deliver",
        distinct is True
        and distinct_res is not None
        and distinct_res.get("ok") is True
        and distinct_res.get("suppressed") is not True
        and uploads == [other.name],
        f"distinct={distinct} result={distinct_res} uploads={uploads!r}",
    )

    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "file-d", "mode": "reply_file", "ok": True},
    ])
    write_jsonl(wc.OUTBOX, [
        {"id": "file-d", "mode": "reply_file", "msgid": msgid, "file_path": str(media)},
        {"id": "upd", "mode": "update", "msgid": msgid, "content": "还在做"},
    ])
    replies.clear()
    update_ok = await gateway.dispatch_outbox_item({
        "id": "upd", "mode": "update", "msgid": msgid, "content": "还在做",
    })
    update_res = result_for(wc, "upd")
    check(
        "B1d wecom update after any formal delivery is suppressed",
        update_ok is True
        and update_res is not None
        and update_res.get("suppressed") is True
        and replies == [],
        f"update={update_ok} result={update_res} replies={replies!r}",
    )


def check_b1_cli_gate() -> None:
    """The CLI gate blocks same-mode rows and still blocks progress broadly."""
    file_row = {"id": "file1", "mode": "reply_file", "msgid": "M", "queued_at": 1}
    reply_row = {"id": "text1", "mode": "reply", "msgid": "M", "queued_at": 1}
    delivered = {"ok": True}
    text_vs_file = block_reason("reply", file_row, delivered)
    file_vs_file = block_reason("reply_file", file_row, delivered)
    text_vs_text = block_reason("reply", reply_row, delivered)
    update_vs_file = block_reason("update", file_row, delivered)
    check(
        "B1 CLI delivered reply_file does not block a text reply",
        text_vs_file is None,
        f"reason={text_vs_file!r}",
    )
    check(
        "B1 CLI delivered reply_file blocks another reply_file",
        file_vs_file is not None and "delivered" in file_vs_file,
        f"reason={file_vs_file!r}",
    )
    check(
        "B1 CLI delivered reply still blocks a text reply",
        text_vs_text is not None and "delivered" in text_vs_text,
        f"reason={text_vs_text!r}",
    )
    check(
        "B1 CLI update is blocked by a delivered reply_file",
        update_vs_file is not None and "delivered" in update_vs_file,
        f"reason={update_vs_file!r}",
    )


async def check_b2_merged_resume(directory: Path) -> None:
    """A merged send that dies after row 1 must not resend row 2's prefix."""
    sandbox(wc, directory)
    gateway, _replies, _uploads, frames = bare_wecom()
    row1 = "A" * 1000
    row2 = "B" * 3500
    merged = row1 + "\n" + row2
    chunks = wc.split_chunks(merged)
    covered = len(chunks[0]) - (len(row1) + 1)
    tail = row2[covered:]

    async def scripted(frame, wait_response=False, timeout=10.0):
        part = (((frame.get("body") or {}).get("markdown") or {}).get("content") or "")
        frames.append(part)
        if part.startswith("A"):
            return {"errcode": 0, "errmsg": ""}
        return {"errcode": -1, "errmsg": "response timeout"}

    gateway.send_frame = scripted
    gateway._coalesce_pending = {
        "s1": {"id": "s1", "mode": "send", "chatid": "chat", "chat_type": 1, "content": row1},
        "s2": {"id": "s2", "mode": "send", "chatid": "chat", "chat_type": 1, "content": row2},
    }
    ready = {
        "id": "s1",
        "mode": "send",
        "chatid": "chat",
        "chat_type": 1,
        "content": merged,
        "merged_ids": ["s1", "s2"],
    }
    ok = await gateway._dispatch_outbox_ready(ready)
    partial = {}
    if wc.OUTBOX_PARTIAL.exists():
        partial = json.loads(wc.OUTBOX_PARTIAL.read_text(encoding="utf-8"))
    sent_chars = int((partial.get("s2") or {}).get("sent_chars") or 0)
    frames.clear()
    await gateway.dispatch_outbox_item({
        "id": "s2", "mode": "send", "chatid": "chat", "chat_type": 1, "content": row2,
    })
    check(
        "B2 merged retry does not resend the delivered prefix of row 2",
        ok is False
        and covered > 0
        and sent_chars == covered
        and frames == [tail]
        and row2 not in frames,
        f"ok={ok} covered={covered} sent_chars={sent_chars} frames={frames!r}",
    )


async def check_b3_send_crash_window(directory: Path) -> None:
    """A flushed send whose offset was not saved must not go out again."""
    sandbox(wc, directory)
    row = {
        "id": "send-b3",
        "mode": "send",
        "chatid": "chat-b3",
        "chat_type": 1,
        "content": "hello once",
    }
    line = json.dumps(row, ensure_ascii=False) + "\n"
    wc.OUTBOX.write_text(line, encoding="utf-8")
    wc.OUTBOX_OFFSET.write_text("0", encoding="utf-8")
    wc.OUTBOX_RESULTS.write_text(
        json.dumps({"id": "send-b3", "mode": "send", "ok": True}) + "\n",
        encoding="utf-8",
    )
    co_mod = sys.modules.get("wecom_outbound_coalesce_v2")
    clock = {"t": 0.0}
    original_mono = co_mod.time.monotonic if co_mod is not None else None
    if co_mod is not None:
        co_mod.time.monotonic = lambda: clock["t"]
    gateway, _replies, _uploads, frames = bare_wecom()

    async def record_send(frame, wait_response=False, timeout=10.0):
        part = (((frame.get("body") or {}).get("markdown") or {}).get("content") or "")
        frames.append(part)
        return {"errcode": 0, "errmsg": ""}

    async def no_watchdog():
        return None

    gateway.send_frame = record_send
    gateway.check_stream_watchdog = no_watchdog
    sleeps = {"n": 0}
    original_sleep = wc.asyncio.sleep

    async def flush_then_stop(_seconds):
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            clock["t"] = 5.0
        if sleeps["n"] >= 3:
            raise asyncio.CancelledError()

    wc.asyncio.sleep = flush_then_stop
    try:
        await gateway.outbox_loop()
    except asyncio.CancelledError:
        pass
    finally:
        wc.asyncio.sleep = original_sleep
        if co_mod is not None and original_mono is not None:
            co_mod.time.monotonic = original_mono
    offset = int(wc.OUTBOX_OFFSET.read_text(encoding="utf-8").strip() or "0")
    check(
        "B3 restarted reader does not redispatch a delivered send",
        frames == [] and offset == len(line.encode("utf-8")),
        f"frames={frames!r} offset={offset} size={len(line.encode('utf-8'))}",
    )


async def check_b3_reply_not_guarded(directory: Path) -> None:
    """The send-id guard must not swallow a formal reply row."""
    sandbox(wc, directory)
    row = {"id": "reply-b3", "mode": "reply", "msgid": "m-b3", "content": "正式回复"}
    wc.OUTBOX.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
    wc.OUTBOX_OFFSET.write_text("0", encoding="utf-8")
    wc.OUTBOX_RESULTS.write_text(
        json.dumps({"id": "reply-b3", "mode": "reply", "ok": True}) + "\n",
        encoding="utf-8",
    )
    gateway, replies, _uploads, _frames = bare_wecom()
    route_wecom(gateway, "m-b3")

    async def no_watchdog():
        return None

    gateway.check_stream_watchdog = no_watchdog
    sleeps = {"n": 0}
    original_sleep = wc.asyncio.sleep

    async def once(_seconds):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise asyncio.CancelledError()

    wc.asyncio.sleep = once
    try:
        await gateway.outbox_loop()
    except asyncio.CancelledError:
        pass
    finally:
        wc.asyncio.sleep = original_sleep
    check(
        "B3 formal reply keeps its current redispatch semantics",
        replies == ["正式回复"],
        f"replies={replies!r}",
    )


def check_b4_approval_and_jsonl(directory: Path) -> None:
    """WeCom approval reader matches Weixin, and JSONL state readers keep separators."""
    relay = directory / "relay"
    relay.mkdir(parents=True, exist_ok=True)
    previous = wc.APPROVAL_RELAY_DIR
    wc.APPROVAL_RELAY_DIR = relay
    try:
        (relay / "decisions.offset").write_text("0", encoding="utf-8")
        (relay / "decisions.jsonl").write_text(
            json.dumps({"num": "8", "decision": "deny"}) + "\n"
            + json.dumps({"num": "8", "decision": "allow_once"}) + "\n",
            encoding="utf-8",
        )
        first = wc._approval_pending_submission("8")
        check(
            "B4 approval pending returns the first matching decision",
            first == "deny",
            f"found={first!r}",
        )
        (relay / "decisions.jsonl").write_text(
            "[]\n"
            + json.dumps({"num": "8", "decision": "deny"}) + "\n",
            encoding="utf-8",
        )
        try:
            guarded = wc._approval_pending_submission("8")
        except Exception as exc:
            guarded = f"EXC {type(exc).__name__}: {exc}"
        check(
            "B4 approval pending skips a non-dict row",
            guarded == "deny",
            f"found={guarded!r}",
        )

        weird = "a\u2028b\u2029c\u0085d"
        (relay / "decisions.jsonl").write_text(
            json.dumps({"num": "3", "decision": "allow_always", "note": weird},
                       ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        try:
            sep = wc._approval_pending_submission("3")
        except Exception as exc:
            sep = f"EXC {type(exc).__name__}: {exc}"
        check(
            "B4 approval reader keeps unicode separators inside the JSONL row",
            sep == "allow_always",
            f"found={sep!r}",
        )
    finally:
        wc.APPROVAL_RELAY_DIR = previous

    sandbox(wc, directory / "state")
    weird = "a\u2028b\u2029c\u0085d"
    wc.INBOX.write_text(
        json.dumps({"msgid": "m-sep", "text": weird}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    wc.SEEN_FILE.write_text(
        json.dumps({"msgid": "seen-sep", "text": weird}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    gateway = wc.Gateway.__new__(wc.Gateway)
    gateway.seen_msgids = set()
    gateway.reqmap = {}
    gateway.cards = {}
    try:
        gateway._load_persisted()
        loaded = "m-sep" in gateway.seen_msgids and "seen-sep" in gateway.seen_msgids
        detail = f"seen={sorted(gateway.seen_msgids)!r}"
    except Exception as exc:
        loaded = False
        detail = f"{type(exc).__name__}: {exc}"
    check(
        "B4 inbox and seen readers keep a msgid across unicode separators",
        loaded,
        detail,
    )

    state = directory / "outbound"
    state.mkdir(parents=True, exist_ok=True)
    (state / "outbox.jsonl").write_text(
        json.dumps({"msgid": "M", "queued_at": 42, "content": weird},
                   ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    try:
        ts = wc._last_outbound_ts(state, ["M"])
    except Exception as exc:
        ts = f"EXC {type(exc).__name__}: {exc}"
    check(
        "B4 outbox tail reader keeps a separator inside a JSONL value",
        ts == 42,
        f"ts={ts!r}",
    )


async def async_main(root: Path) -> None:
    """Run the async residuals."""
    await check_b1_weixin(root / "b1-wx")
    await check_b1_wecom(root / "b1-wc")
    await check_b2_merged_resume(root / "b2")
    await check_b3_send_crash_window(root / "b3-send")
    await check_b3_reply_not_guarded(root / "b3-reply")


def main() -> int:
    """Run every residual check and return a process status."""
    root = Path(tempfile.mkdtemp(prefix="todo28-residuals-"))
    try:
        asyncio.run(async_main(root))
        check_b1_cli_gate()
        check_b4_approval_and_jsonl(root / "b4")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    failed = [name for name, ok, _detail in RESULTS if not ok]
    print(f"\n== {len(RESULTS) - len(failed)}/{len(RESULTS)} passed ==")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
