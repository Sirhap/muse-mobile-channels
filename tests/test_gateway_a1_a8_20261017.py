#!/usr/bin/env python3
"""Regression checks for review findings A1..A8 (gateways, bridge, CLIs).

Every check is meant to fail on the pre-fix code for the stated reason.
State paths are sandboxed and network sends are stubbed.

A1 WeCom reply_file with a missing/unreadable file is terminal and tells the user
A2 WeCom allowlist refusal of a reply_file tells the user
A3 WeCom stuck-formal notice resolves the chat through the request map
A4 corrupt cancelled.json keeps old records and never re-enables delivery
A5 the native bridge appends to JSONL under the shared flock
A6 weixin_cli rejects whitespace-only text like wecom_cli
A7 WeCom inbound parsing skips malformed nodes instead of wedging
A8 weixin login logs no raw platform payload
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fcntl  # noqa: E402

from channel_common import register_cancel  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []
TMP = Path(tempfile.mkdtemp(prefix="gateway-a1-a8-"))


def check(name: str, cond: bool, detail: str = "") -> None:
    """Record one named assertion and print it."""
    RESULTS.append((name, bool(cond), detail))
    line = ("PASS " if cond else "FAIL ") + name
    if detail and not cond:
        line += " :: " + detail
    print(line)


def load_module(name: str, path: Path):
    """Load one source file under a private module name."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


wc = load_module("wc_a1_a8", ROOT / "wecom-bot" / "gateway.py")
wx = load_module("wx_a1_a8", ROOT / "weixin-bot" / "gateway.py")
LOGS: list[str] = []
wc.log = lambda msg: LOGS.append(str(msg))
wx.log = lambda msg: LOGS.append(str(msg))


def sandbox(mod, directory: Path) -> None:
    """Point one gateway's state files at an empty directory."""
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
    mod.STATE = directory
    if hasattr(mod, "COMPRESSED_DIR"):
        mod.COMPRESSED_DIR = directory / "compressed"


def read_rows(path: Path) -> list[dict]:
    """JSONL objects, or [] when the file is absent."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def bare_wecom():
    """A WeCom Gateway without the process lock or a live socket.

    Sends are recorded in ``sent`` as (kind, payload) tuples.
    """
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
    gateway.sent = []

    async def respond(req_id, body, timeout=10.0):
        gateway.sent.append(("respond", body))
        return {"errcode": 0}

    async def send_frame(frame, wait_response=False, timeout=10.0):
        gateway.sent.append(("send_frame", frame.get("body")))
        return {"errcode": 0}

    gateway.respond = respond
    gateway.send_frame = send_frame
    return gateway


def notice_texts(gateway) -> list[str]:
    """Markdown text of every message the gateway sent."""
    texts = []
    for _kind, body in gateway.sent:
        if isinstance(body, dict):
            markdown = body.get("markdown")
            if isinstance(markdown, dict):
                texts.append(str(markdown.get("content", "")))
    return texts


def last_result(item_id: str) -> dict:
    """Newest result row for item_id in the sandboxed WeCom outbox."""
    found: dict = {}
    for row in read_rows(wc.OUTBOX_RESULTS):
        if row.get("id") == item_id:
            found = row
    return found


# ---------------------------------------------------------------- A1 / A2


async def check_a1_a2() -> None:
    """File replies that cannot be sent end terminally and tell the user."""
    sandbox(wc, TMP / "a1")
    wc.CRED_FILE = TMP / "creds" / "wecom.env"
    allowed_dir = TMP / "a1" / "files"
    allowed_dir.mkdir(parents=True, exist_ok=True)

    def routed(gateway, msgid: str) -> None:
        gateway.reqmap[msgid] = {
            "req_id": f"req-{msgid}", "stream_id": "s", "chatid": "alice",
            "chattype": "single"}

    # A1a: the file vanished after the CLI queued the row.
    gateway = bare_wecom()
    routed(gateway, "m-missing")
    missing = allowed_dir / "gone.txt"
    item = {"id": "r-missing", "mode": "reply_file", "msgid": "m-missing",
            "file_path": str(missing)}
    ok = await gateway.dispatch_outbox_item(item)
    row = last_result("r-missing")
    texts = notice_texts(gateway)
    check("A1a missing reply_file is consumed with a terminal row",
          ok is True and row.get("ok") is False and row.get("deadletter") is True,
          f"ok={ok} row={row}")
    check("A1a missing reply_file tells the user the file is gone",
          any("gone.txt" in t and "不在" in t for t in texts), f"texts={texts}")

    # A1b: the file passes the allowlist, then the read raises
    # FileNotFoundError (deleted in between).
    gateway = bare_wecom()
    routed(gateway, "m-race")
    present = allowed_dir / "present.txt"
    present.write_text("x", encoding="utf-8")

    async def vanished(_path):
        raise FileNotFoundError(2, "No such file or directory", str(present))

    gateway._outbound_media_bytes = vanished
    item = {"id": "r-race", "mode": "reply_file", "msgid": "m-race",
            "file_path": str(present)}
    ok = await gateway.dispatch_outbox_item(item)
    row = last_result("r-race")
    texts = notice_texts(gateway)
    check("A1b FileNotFoundError during read is terminal, not parked",
          ok is True and row.get("ok") is False and row.get("deadletter") is True,
          f"ok={ok} row={row}")
    check("A1b FileNotFoundError during read notifies the user",
          any("present.txt" in t for t in texts), f"texts={texts}")

    # A1c: unreadable file.
    gateway = bare_wecom()
    routed(gateway, "m-perm")

    async def denied(_path):
        raise PermissionError(13, "Permission denied", str(present))

    gateway._outbound_media_bytes = denied
    item = {"id": "r-perm", "mode": "reply_file", "msgid": "m-perm",
            "file_path": str(present)}
    ok = await gateway.dispatch_outbox_item(item)
    row = last_result("r-perm")
    texts = notice_texts(gateway)
    check("A1c PermissionError during read is terminal, not parked",
          ok is True and row.get("deadletter") is True, f"ok={ok} row={row}")
    check("A1c PermissionError during read notifies the user",
          any("present.txt" in t for t in texts), f"texts={texts}")

    # A1d: a transient failure still parks (behavior kept).
    gateway = bare_wecom()
    routed(gateway, "m-flaky")

    async def flaky(_path):
        raise RuntimeError("upload backend hiccup")

    gateway._outbound_media_bytes = flaky
    item = {"id": "r-flaky", "mode": "reply_file", "msgid": "m-flaky",
            "file_path": str(present)}
    ok = await gateway.dispatch_outbox_item(item)
    check("A1d a transient error still returns False for the retry lane",
          ok is False and not notice_texts(gateway), f"ok={ok}")

    # A2a: reply_file outside the allowlist.
    gateway = bare_wecom()
    routed(gateway, "m-deny")
    outside = Path("/etc/hosts")
    item = {"id": "r-deny", "mode": "reply_file", "msgid": "m-deny",
            "file_path": str(outside)}
    ok = await gateway.dispatch_outbox_item(item)
    row = last_result("r-deny")
    texts = notice_texts(gateway)
    check("A2a refused reply_file is consumed with an ok=False row",
          ok is True and row.get("ok") is False, f"ok={ok} row={row}")
    check("A2a refused reply_file tells the user about the allowed directories",
          any("hosts" in t and "不在允许发送的目录" in t for t in texts),
          f"texts={texts}")

    # A2b: send_file refusal stays consumed, keeps its result row, and
    # sends nothing.
    gateway = bare_wecom()
    item = {"id": "s-deny", "mode": "send_file", "chatid": "alice",
            "chat_type": 1, "file_path": str(outside)}
    ok = await gateway.dispatch_outbox_item(item)
    row = last_result("s-deny")
    check("A2b refused send_file stays consumed with a result row and no notice",
          ok is True and row.get("ok") is False and not gateway.sent,
          f"ok={ok} row={row} sent={gateway.sent}")


# ---------------------------------------------------------------- A3


def check_a3() -> None:
    """A stuck formal row (msgid only) notifies on the WeCom channel."""
    sandbox(wc, TMP / "a3")
    wc.STUCK_NOTICE_WEIXIN_OUTBOX = TMP / "a3" / "no-weixin-outbox.jsonl"
    gateway = bare_wecom()
    gateway.reqmap["m-room"] = {
        "req_id": "req-room", "stream_id": "s", "chatid": "room9",
        "chattype": "group"}
    gateway.reqmap["m-solo"] = {
        "req_id": "req-solo", "stream_id": "s", "chatid": "alice",
        "chattype": "single"}

    item = {"id": "b1", "mode": "reply", "msgid": "m-room", "content": "hi"}
    for _ in range(wc.FORMAL_STUCK_NOTIFY_ATTEMPTS):
        gateway._note_failure(item)
    rows = [r for r in read_rows(wc.OUTBOX) if str(r.get("id", "")).startswith("stucknote-")]
    check("A3 stuck group reply notifies the group from the request map",
          len(rows) == 1 and rows[0].get("chatid") == "room9"
          and rows[0].get("chat_type") == 2, f"rows={rows}")

    wc.OUTBOX.unlink(missing_ok=True)
    gateway._notify_stuck_formal(
        {"id": "b2", "mode": "reply", "msgid": "m-solo"}, 5)
    rows = read_rows(wc.OUTBOX)
    check("A3 stuck single-chat reply notifies the sender chat",
          len(rows) == 1 and rows[0].get("chatid") == "alice"
          and rows[0].get("chat_type") == 1, f"rows={rows}")

    wc.OUTBOX.unlink(missing_ok=True)
    gateway._notify_stuck_formal(
        {"id": "b3", "mode": "reply", "msgid": "m-room", "chatid": "explicit",
         "chat_type": 1}, 5)
    rows = read_rows(wc.OUTBOX)
    check("A3 an explicit chatid on the row still wins",
          len(rows) == 1 and rows[0].get("chatid") == "explicit", f"rows={rows}")

    wc.OUTBOX.unlink(missing_ok=True)
    gateway._notify_stuck_formal(
        {"id": "b4", "mode": "reply", "msgid": "m-unknown"}, 5)
    check("A3 an unresolvable msgid sends nothing and does not raise",
          not read_rows(wc.OUTBOX))


# ---------------------------------------------------------------- A4


def check_a4_register_cancel() -> None:
    """A corrupt cancelled.json is preserved aside, not overwritten."""
    base = TMP / "a4"
    base.mkdir(parents=True, exist_ok=True)

    clean = base / "clean" / "cancelled.json"
    register_cancel(clean, "m-1")
    register_cancel(clean, "m-2")
    rows = json.loads(clean.read_text(encoding="utf-8"))
    leftovers = list(clean.parent.glob("cancelled.json.corrupt-*"))
    check("A4a normal path appends and creates no backup",
          [r["msgid"] for r in rows] == ["m-1", "m-2"] and not leftovers,
          f"rows={rows} leftovers={leftovers}")

    torn = base / "torn" / "cancelled.json"
    torn.parent.mkdir(parents=True)
    corrupt = ('[{"msgid": "old-1", "ts": 1.0}, {"msgid": "old-2", "ts": 2.0}, '
               '{"msgid": "old-3", "ts"').encode("utf-8")
    torn.write_bytes(corrupt)
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        register_cancel(torn, "new-1")
    backups = list(torn.parent.glob("cancelled.json.corrupt-*"))
    check("A4a corrupt file is preserved byte for byte beside the new file",
          len(backups) == 1 and backups[0].read_bytes() == corrupt,
          f"backups={backups}")
    try:
        rows = json.loads(torn.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        rows = []
    ids = {r.get("msgid") for r in rows if isinstance(r, dict)}
    check("A4a the new cancel is recorded and earlier ids are carried over",
          {"new-1", "old-1", "old-2"} <= ids, f"ids={ids}")

    wrong_shape = base / "shape" / "cancelled.json"
    wrong_shape.parent.mkdir(parents=True)
    wrong_shape.write_text('{"msgid": "kept"}', encoding="utf-8")
    register_cancel(wrong_shape, "new-2")
    check("A4a a valid-JSON non-list file is preserved too",
          len(list(wrong_shape.parent.glob("cancelled.json.corrupt-*"))) == 1)


def check_a4_cancelled_ids() -> None:
    """A transient bad read keeps the last good cancelled set."""
    for label, mod in (("wecom", wc), ("weixin", wx)):
        sandbox(mod, TMP / f"a4-{label}")
        LOGS.clear()
        mod.CANCELLED_FILE.write_text(
            json.dumps([{"msgid": "stopped-1"}, {"msgid": "stopped-2"}]),
            encoding="utf-8")
        first = mod.Gateway._cancelled_ids()
        mod.CANCELLED_FILE.write_text('[{"msgid": "stopped-1"}, {"ms', encoding="utf-8")
        second = mod.Gateway._cancelled_ids()
        check(f"A4b {label}: good read returns the cancelled ids",
              first == {"stopped-1", "stopped-2"}, f"first={first}")
        check(f"A4b {label}: corrupt read falls back to the last good set",
              second == {"stopped-1", "stopped-2"}, f"second={second}")
        check(f"A4b {label}: corrupt read is logged",
              any("cancelled" in line.lower() for line in LOGS), f"logs={LOGS}")
        mod.CANCELLED_FILE.write_text(json.dumps([{"msgid": "stopped-3"}]), encoding="utf-8")
        third = mod.Gateway._cancelled_ids()
        check(f"A4b {label}: a later good read replaces the cached set",
              third == {"stopped-3"}, f"third={third}")
        mod.CANCELLED_FILE.unlink()
        check(f"A4b {label}: a missing file is an empty set, not a stale cache",
              mod.Gateway._cancelled_ids() == set())


# ---------------------------------------------------------------- A5


def load_bridge(base: Path):
    """The real bridge module with BASE and its host imports redirected."""
    stub = base / "pydeps"
    pkg = stub / "muse_cli"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "cli.py").write_text(
        "def fmt_event(ev):\n    return {}\n"
        "def is_reply(ev):\n    return False\n", encoding="utf-8")
    (pkg / "gateway.py").write_text(
        "class GatewayError(Exception):\n    pass\n", encoding="utf-8")
    (stub / "gw2.py").write_text("class GW2:\n    pass\n", encoding="utf-8")
    os.environ["A1A8_BRIDGE_STUB"] = str(stub)
    if str(stub) not in sys.path:
        sys.path.insert(0, str(stub))
    source = (ROOT / "native-bridge" / "native_bridge.py").read_text(encoding="utf-8")
    source = source.replace(
        'BASE = "/home/hatch/workspace/native-bridge"', f"BASE = {str(base)!r}", 1)
    source = source.replace(
        'sys.path.insert(0, "/home/hatch/workspace/native-probe")',
        "sys.path.insert(0, os.environ['A1A8_BRIDGE_STUB'])", 1)
    mod = types.ModuleType("native_bridge_a1a8")
    mod.__file__ = str(ROOT / "native-bridge" / "native_bridge.py")
    sbx_cfg = base / "config.json"
    shutil.copy(ROOT / "native-bridge" / "config.json", sbx_cfg)
    exec(compile(source, mod.__file__, "exec"), mod.__dict__)
    return mod


def check_a5() -> None:
    """The bridge's append waits for a holder of the shared exclusive flock."""
    base = TMP / "a5"
    (base / "shadow").mkdir(parents=True)
    bridge = load_bridge(base)
    target = base / "outbox.jsonl"
    target.write_text("", encoding="utf-8")

    holder = open(target, "a", encoding="utf-8")
    fcntl.flock(holder, fcntl.LOCK_EX)
    done = threading.Event()

    def writer() -> None:
        bridge.append_jsonl(str(target), {"id": "bridge-row", "mode": "reply"})
        done.set()

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    finished_while_locked = done.wait(0.6)
    fcntl.flock(holder, fcntl.LOCK_UN)
    holder.close()
    thread.join(3)
    check("A5 bridge append blocks while another writer holds the lock",
          not finished_while_locked)
    check("A5 bridge append lands once the lock is released",
          done.is_set() and read_rows(target) == [{"id": "bridge-row", "mode": "reply"}],
          f"rows={read_rows(target)}")


# ---------------------------------------------------------------- A6


def run_cli(path: Path, tag: str, argv: list[str]) -> tuple[int, list[dict]]:
    """Run one CLI main() with queue() captured. Returns (exit code, queued)."""
    mod = load_module(tag, path)
    queued: list[dict] = []
    mod.queue = lambda item: (queued.append(item), "q1")[1]
    mod.reply_block_reason = lambda *a, **k: None
    saved = sys.argv
    sys.argv = [str(path)] + argv
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = mod.main()
    finally:
        sys.argv = saved
    return code, queued


def check_a6() -> None:
    """Whitespace-only text is rejected by both CLIs."""
    cases = {
        "reply": ["reply", "--msgid", "m1", "--text", "   \n\t"],
        "send": None,
        "update": ["update", "--msgid", "m1", "--text", "  "],
    }
    wx_cli = ROOT / "weixin-bot" / "weixin_cli.py"
    wc_cli = ROOT / "wecom-bot" / "wecom_cli.py"
    for name, argv in cases.items():
        if name == "send":
            wx_argv = ["send", "--to", "u1", "--text", " \t "]
            wc_argv = ["send", "--chatid", "c1", "--text", " \t "]
        else:
            wx_argv = wc_argv = argv
        code, queued = run_cli(wx_cli, f"wx_cli_{name}", wx_argv)
        check(f"A6 weixin_cli {name} rejects whitespace-only text (exit 2, nothing queued)",
              code == 2 and not queued, f"code={code} queued={queued}")
        code, queued = run_cli(wc_cli, f"wc_cli_{name}", wc_argv)
        check(f"A6 wecom_cli {name} rejects whitespace-only text (exit 2, nothing queued)",
              code == 2 and not queued, f"code={code} queued={queued}")
    code, queued = run_cli(wx_cli, "wx_cli_ok", ["reply", "--msgid", "m1", "--text", " hi "])
    check("A6 weixin_cli still queues real text", code == 0 and len(queued) == 1,
          f"code={code} queued={queued}")


# ---------------------------------------------------------------- A7


async def check_a7() -> None:
    """Malformed nodes in an inbound frame are skipped, not fatal."""
    sandbox(wc, TMP / "a7")
    captured: list[tuple[str, dict]] = []

    def make_gateway():
        gateway = bare_wecom()
        gateway.append_jsonl = lambda path, obj: captured.append((str(path), obj))

        async def no_slash(*_a, **_k):
            return ""

        gateway._slash_dispatch = no_slash
        return gateway

    good_text = {"msgtype": "text", "text": {"content": "hello"}}
    frames = {
        "mixed junk items": {"msgtype": "mixed", "mixed": {"msg_item": [
            "junk", 5, None, {"msgtype": "text", "text": "not-a-dict"},
            {"msgtype": "image", "image": "not-a-dict"}, good_text]}},
        "mixed not a dict": {"msgtype": "mixed", "mixed": "oops"},
        "msg_item not a list": {"msgtype": "mixed", "mixed": {"msg_item": {"a": 1}}},
        "text node not a dict": {"msgtype": "text", "text": "oops"},
        "quote not a dict": {"msgtype": "text", "text": {"content": "hello"}, "quote": "oops"},
        "quote nodes not dicts": {"msgtype": "text", "text": {"content": "hello"},
                                  "quote": {"msgtype": "text", "text": "oops",
                                            "image": "x", "file": ["y"]}},
        "voice node not a dict": {"msgtype": "voice", "voice": "oops"},
        "image node not a dict": {"msgtype": "image", "image": "oops"},
    }
    for index, (label, extra) in enumerate(frames.items()):
        gateway = make_gateway()
        captured.clear()
        msgid = f"a7-{index}"
        body = {"msgid": msgid, "chattype": "single", "chatid": "alice",
                "from": {"userid": "alice"}}
        body.update(extra)
        error = ""
        try:
            await gateway.handle_message_callback(
                {"cmd": "aibot_msg_callback", "headers": {"req_id": f"req-{index}"},
                 "body": body})
        except Exception as exc:  # the defect under test
            error = f"{type(exc).__name__}: {exc}"
        persisted = [obj for _path, obj in captured if obj.get("msgid") == msgid]
        check(f"A7 {label}: no exception, msgid marked seen, frame persisted",
              not error and msgid in gateway.seen_msgids and bool(persisted),
              f"error={error!r} seen={msgid in gateway.seen_msgids} persisted={len(persisted)}")
        if label == "mixed junk items":
            texts = [obj.get("text") for obj in persisted]
            check("A7 the valid text node in a mixed frame is still read",
                  texts and texts[0] == "hello", f"texts={texts}")

    gateway = make_gateway()
    captured.clear()
    error = ""
    try:
        await gateway.handle_message_callback(
            {"cmd": "aibot_msg_callback", "headers": {"req_id": "req-from"},
             "body": {"msgid": "a7-from", "msgtype": "text",
                      "text": {"content": "hi"}, "from": "oops"}})
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    check("A7 a non-dict from node does not raise", not error, error)


# ---------------------------------------------------------------- A8


class _FakeResponse:
    """Minimal httpx response stand-in."""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def run_login(login, post_payload: dict, status_payloads: list) -> str:
    """Run login.main() against canned responses and return its output."""
    statuses = list(status_payloads)

    class FakeClient:
        def __init__(self, *_a, **_k) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc) -> None:
            return None

        async def post(self, *_a, **_k):
            return _FakeResponse(post_payload)

        async def get(self, *_a, **_k):
            nxt = statuses.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return _FakeResponse(nxt)

    saved = login.httpx.AsyncClient
    login.httpx.AsyncClient = FakeClient
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            asyncio.run(login.main(""))
    finally:
        login.httpx.AsyncClient = saved
    return buffer.getvalue()


def check_a8() -> None:
    """The login script's log lines carry no raw payload slices."""
    login = load_module("login_a1a8", ROOT / "weixin-bot" / "login.py")
    sbx = TMP / "a8"
    sbx.mkdir(parents=True)
    login.LOGIN_STATE = sbx / "login.json"
    login.QR_PNG = sbx / "qr.png"
    login.CRED_FILE = sbx / "credentials.env"
    secret = "QRSESSION-SECRET-9f8e7d6c"

    out = run_login(login, {"qrcode": secret, "extra": secret}, [])
    check("A8 unexpected qrcode response logs no raw payload",
          "unexpected qrcode response" in out and secret not in out, out)

    out = run_login(
        login, {"qrcode": secret, "qrcode_img_content": "img"},
        [{"status": "confirmed", "qrcode": secret, "note": secret}])
    check("A8 confirmed-without-token logs no raw payload",
          "confirmed but no bot_token" in out and secret not in out, out)

    out = run_login(
        login, {"qrcode": secret, "qrcode_img_content": "img"},
        [{"status": "mystery", "qrcode": secret, "note": secret},
         {"status": "expired"}])
    check("A8 unknown status logs the status but no raw payload",
          "status=mystery" in out and secret not in out, out)

    async def no_sleep(_secs):
        return None

    saved_sleep = login.asyncio.sleep
    login.asyncio.sleep = no_sleep
    try:
        url_error = RuntimeError(
            f"Client error '404' for url 'https://x/get_qrcode_status?qrcode={secret}'")
        out = run_login(
            login, {"qrcode": secret, "qrcode_img_content": "img"},
            [url_error, {"status": "expired"}])
    finally:
        login.asyncio.sleep = saved_sleep
    check("A8 a failed status poll does not log the qrcode URL",
          "status poll error" in out and secret not in out, out)


def main() -> int:
    """Run every check and return a process status."""
    try:
        asyncio.run(check_a1_a2())
        check_a3()
        check_a4_register_cancel()
        check_a4_cancelled_ids()
        check_a5()
        check_a6()
        asyncio.run(check_a7())
        check_a8()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    failed = [name for name, ok, _detail in RESULTS if not ok]
    print(f"\n== {len(RESULTS) - len(failed)}/{len(RESULTS)} passed ==")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
