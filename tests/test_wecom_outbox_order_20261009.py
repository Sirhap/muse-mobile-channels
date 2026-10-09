#!/usr/bin/env python3
"""DEF-wecom-outbox-order: WeCom UI order follows outbox time order.

Live bug: the server sent started, then progress, then the formal
「视频已生成」, then the mp4. The client showed the formal text directly
under the user message, with started and progress below it, then the
mp4. The formal reply was finishing the think stream opened at inbound
time, so that bubble stayed where it was created. started and progress
were unbound aibot_send_msg rows, created later, so they sat underneath.

The client rule this file applies (WeCom long-connection docs, plus
that live layout):

- the first use of a stream id creates a bubble
- a later frame with the same stream id updates that bubble and does
  not move it
- any other aibot_respond_msg creates a bubble at the end
- aibot_send_msg also creates a bubble at the end

Rewriting the inbound stream is what puts a later answer above earlier
bubbles. This test records the frames dispatch actually sends and
applies that rule. The fixed sequence has no aibot_send_msg, the
inbound stream is finished once with the earliest notice, and every
later step is a new bubble.

Sandbox only. No network, no production state.

Run: python3 tests/test_wecom_outbox_order_20261009.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import sys
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/wecom-outbox-order-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)

spec = importlib.util.spec_from_file_location(
    "wcgw_order", ROOT / "wecom-bot" / "gateway.py")
wc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wc)

RESULTS = []
MSGID = "88c3db3e-live"
ANCHOR = "stream-anchor"
REQ = "req-anchor"
STARTED = "▶️ 排到你了，开始处理：「生成视频」〔#88c3db3e〕"
PROGRESS = "⏳ 还在处理中（已跑约 2 分钟，思考中）：「生成视频」〔#88c3db3e〕"
FORMAL = "视频已生成"
WAIT = "⏳ 还在排队（原生通道第 1 位）：前面任务还没结束，已等3分钟〔#88c3db3e〕"


def check(name, cond):
    """Record one named assertion and print PASS or FAIL."""
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def fresh_state(name):
    """Point gateway state paths at a fresh sandbox directory."""
    directory = SBX / name
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    wc.STATE = directory
    wc.INBOX = directory / "inbox.jsonl"
    wc.OUTBOX = directory / "outbox.jsonl"
    wc.OUTBOX_RESULTS = directory / "outbox_results.jsonl"
    wc.OUTBOX_OFFSET = directory / "outbox.offset"
    wc.OUTBOX_RETRY = directory / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = directory / "outbox_partial.json"
    wc.OUTBOX_PARKED = directory / "outbox_parked.json"
    wc.REQMAP = directory / "reqmap.json"
    wc.STATUS = directory / "status.json"
    wc.SEEN_FILE = directory / "seen_ids.jsonl"
    wc.COMPRESSED_DIR = directory / "compressed"
    wc.MEDIA_DIR = directory / "media"
    return directory


def make_gateway():
    """Build a Gateway without the process lock or the network."""
    gateway = wc.Gateway.__new__(wc.Gateway)
    gateway.ws = None
    gateway.connected = True
    gateway.started_at = int(time.time())
    gateway.msgs_received = 0
    gateway.msgs_sent = 0
    gateway.last_error = ""
    gateway.state = "connected"
    gateway.pending_responses = {}
    gateway.respond_locks = {}
    gateway.open_streams = {
        MSGID: {"req_id": REQ, "stream_id": ANCHOR, "since": time.time()},
    }
    gateway._http = None
    gateway.allow_users = set()
    gateway.seen_msgids = set()
    gateway.reqmap = {
        MSGID: {
            "req_id": REQ,
            "stream_id": ANCHOR,
            "chatid": "sirhao",
            "chattype": "single",
        },
    }
    gateway.cards = {}
    gateway._lock_fd = None
    gateway._kicked = False
    gateway.feedback_track = {}
    gateway._coalescer = None
    gateway._coalesce_pending = {}
    gateway.write_status = lambda: None
    return gateway


def bubble_text(body):
    """Visible text of one WeCom message body."""
    kind = body.get("msgtype")
    if kind == "stream":
        return (body.get("stream") or {}).get("content", "")
    if kind == "markdown":
        return (body.get("markdown") or {}).get("content", "")
    if kind in ("video", "file", "image", "voice"):
        return kind
    return kind or ""


def client_order(frames, anchor_id):
    """Bubbles the WeCom client would show, top to bottom.

    The inbound think stream is already on screen when dispatch runs.
    Same stream id updates that bubble in place. Everything else is a
    new bubble at the bottom, in frame order.
    """
    bubbles = [{"id": anchor_id, "text": "<think></think>"}]
    position = {anchor_id: 0}
    for frame in frames:
        body = frame.get("body") or {}
        if (frame.get("cmd") == "aibot_respond_msg"
                and body.get("msgtype") == "stream"):
            stream = body.get("stream") or {}
            sid = stream.get("id")
            text = stream.get("content", "")
            if sid in position:
                bubbles[position[sid]]["text"] = text
            else:
                position[sid] = len(bubbles)
                bubbles.append({"id": sid, "text": text})
        else:
            bubbles.append({"id": None, "text": bubble_text(body)})
    return [bubble["text"] for bubble in bubbles]


def attach_recorder(gateway):
    """Record every websocket frame and stub media upload."""
    frames = []

    async def fake_send_frame(frame, wait_response=False, timeout=10.0):
        frames.append(frame)
        return {"errcode": 0, "errmsg": "ok"}

    async def fake_upload(data, mtype, filename):
        return "media-order"

    gateway.send_frame = fake_send_frame
    gateway.upload_media = fake_upload
    return frames


def write_jsonl(path, rows):
    """Replace a jsonl file with these rows."""
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


async def dispatch_all(gateway, rows):
    """Dispatch rows in list order. Return the list of ok flags."""
    flags = []
    for row in rows:
        flags.append(await gateway.dispatch_outbox_item(row))
    return flags


def scenario_old_rule_predicts_the_live_bug():
    """The placement rule, fed the old wire, reproduces finish-before-start."""
    old_frames = [
        {"cmd": "aibot_send_msg",
         "body": {"msgtype": "markdown", "markdown": {"content": STARTED}}},
        {"cmd": "aibot_send_msg",
         "body": {"msgtype": "markdown", "markdown": {"content": PROGRESS}}},
        {"cmd": "aibot_respond_msg",
         "headers": {"req_id": REQ},
         "body": {"msgtype": "stream",
                  "stream": {"id": ANCHOR, "finish": True, "content": FORMAL}}},
        {"cmd": "aibot_respond_msg",
         "headers": {"req_id": REQ},
         "body": {"msgtype": "video", "video": {"media_id": "m"}}},
    ]
    shown = client_order(old_frames, ANCHOR)
    print("evidence old wire -> client order:", json.dumps(shown, ensure_ascii=False))
    check("old wire shows the answer above started and progress",
          shown == [FORMAL, STARTED, PROGRESS, "video"])


async def scenario_live_sequence():
    """started, progress, formal text, video. UI order matches that."""
    state_dir = fresh_state("live")
    gateway = make_gateway()
    frames = attach_recorder(gateway)
    clip = state_dir / "clip.mp4"
    clip.write_bytes(b"\x00\x00\x00\x18ftypmp42")
    rows = [
        {"id": "started-1", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": STARTED},
        {"id": "progress-1", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": PROGRESS},
        {"id": "reply-1", "mode": "reply", "msgid": MSGID, "content": FORMAL},
        {"id": "file-1", "mode": "reply_file", "msgid": MSGID,
         "file_path": str(clip)},
    ]
    write_jsonl(wc.OUTBOX, rows)
    flags = await dispatch_all(gateway, rows)
    shown = client_order(frames, ANCHOR)
    print("evidence dispatch frames:")
    for frame in frames:
        body = frame.get("body") or {}
        stream = (body.get("stream") or {})
        print(
            f"  cmd={frame.get('cmd')} req={frame.get('headers', {}).get('req_id')} "
            f"msgtype={body.get('msgtype')} stream={stream.get('id', '')} "
            f"finish={stream.get('finish', '')} text={bubble_text(body)!r}"
        )
    print("evidence new wire -> client order:", json.dumps(shown, ensure_ascii=False))
    check("live sequence: all four deliveries ok", flags == [True, True, True, True])
    check("live sequence: no proactive send in the chain",
          all(frame.get("cmd") == "aibot_respond_msg" for frame in frames)
          and all(frame.get("headers", {}).get("req_id") == REQ for frame in frames))
    anchor_writes = [
        frame for frame in frames
        if (frame.get("body") or {}).get("msgtype") == "stream"
        and (frame["body"]["stream"].get("id") == ANCHOR)
    ]
    check("live sequence: inbound stream finished once, with the started text",
          len(anchor_writes) == 1
          and anchor_writes[0]["body"]["stream"].get("finish") is True
          and anchor_writes[0]["body"]["stream"].get("content") == STARTED)
    later_streams = [
        frame["body"]["stream"]["id"]
        for frame in frames
        if (frame.get("body") or {}).get("msgtype") == "stream"
        and frame["body"]["stream"].get("id") != ANCHOR
    ]
    check("live sequence: progress and the answer are new stream ids",
          len(later_streams) == 2 and len(set(later_streams)) == 2)
    check("live sequence: every stream frame is finished",
          all((frame.get("body") or {}).get("msgtype") != "stream"
              or frame["body"]["stream"].get("finish") is True
              for frame in frames))
    check("live sequence: client order is started, progress, answer, video",
          shown == [STARTED, PROGRESS, FORMAL, "video"])
    file_res = [
        json.loads(line)
        for line in wc.OUTBOX_RESULTS.read_text(encoding="utf-8").splitlines()
        if line.strip() and json.loads(line).get("id") == "file-1"
    ]
    check("live sequence: reply_file after the formal text is delivered",
          file_res and file_res[-1].get("ok") is True
          and file_res[-1].get("suppressed") is not True)
    check("live sequence: anchor flag stored so a later rewrite is refused",
          gateway.reqmap[MSGID].get("stream_committed") is True)


async def scenario_short_reply_still_uses_the_anchor():
    """No earlier notice: the answer still finishes the think stream."""
    fresh_state("short")
    gateway = make_gateway()
    frames = attach_recorder(gateway)
    ok = await gateway.dispatch_outbox_item(
        {"id": "reply-short", "mode": "reply", "msgid": MSGID, "content": FORMAL})
    shown = client_order(frames, ANCHOR)
    print("evidence short reply -> client order:", json.dumps(shown, ensure_ascii=False))
    check("short reply ok and finishes the inbound stream",
          ok is True and len(frames) == 1
          and frames[0]["body"]["stream"]["id"] == ANCHOR
          and frames[0]["body"]["stream"]["finish"] is True
          and frames[0]["body"]["stream"]["content"] == FORMAL)
    check("short reply client order is just the answer",
          shown == [FORMAL])


async def scenario_wait_then_started():
    """The first notice locks the anchor. The next notice is a new bubble."""
    fresh_state("wait")
    gateway = make_gateway()
    frames = attach_recorder(gateway)
    flags = await dispatch_all(gateway, [
        {"id": "wait-1", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": WAIT},
        {"id": "started-2", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": STARTED},
    ])
    shown = client_order(frames, ANCHOR)
    print("evidence wait then started -> client order:",
          json.dumps(shown, ensure_ascii=False))
    check("wait then started both ok", flags == [True, True])
    check("wait locks the anchor; started does not replace it",
          frames[0]["body"]["stream"]["id"] == ANCHOR
          and frames[0]["body"]["stream"]["content"] == WAIT
          and frames[1]["body"]["stream"]["id"] != ANCHOR
          and frames[1]["body"]["stream"]["content"] == STARTED
          and shown == [WAIT, STARTED])


async def scenario_progress_after_done():
    """A progress row queued after a delivered reply is not sent."""
    fresh_state("late")
    gateway = make_gateway()
    frames = attach_recorder(gateway)
    write_jsonl(wc.OUTBOX, [
        {"id": "reply-done", "mode": "reply", "msgid": MSGID, "content": FORMAL},
        {"id": "prog-late", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": PROGRESS},
    ])
    write_jsonl(wc.OUTBOX_RESULTS, [
        {"id": "reply-done", "mode": "reply", "msgid": MSGID, "ok": True},
    ])
    ok = await gateway.dispatch_outbox_item(
        {"id": "prog-late", "mode": "reply_notice", "msgid": MSGID,
         "chatid": "sirhao", "chat_type": 1, "content": PROGRESS})
    late = [
        json.loads(line) for line in wc.OUTBOX_RESULTS.read_text(encoding="utf-8").splitlines()
        if line.strip() and json.loads(line).get("id") == "prog-late"
    ]
    print("evidence late progress result:", json.dumps(late[-1] if late else {}, ensure_ascii=False))
    check("progress after a delivered reply sends nothing",
          ok is True and frames == []
          and late and late[-1].get("suppressed") is True)


def scenario_started_row_is_not_a_queue_ack():
    """The scan still emits one started reply_notice and no arrival ack."""
    fresh_state("scan")
    gateway = make_gateway()
    fake = {"active": [], "queued": []}
    wc._bridge_snapshot = lambda channel: (list(fake["active"]), list(fake["queued"]))
    gateway.feedback_track = {}
    fake["active"] = []
    fake["queued"] = []
    gateway._register_bridge_feedback("sirhao", "single", MSGID, "生成视频")
    queued_rows = [
        json.loads(line) for line in wc.OUTBOX.read_text(encoding="utf-8").splitlines()
    ] if wc.OUTBOX.exists() else []
    check("register writes no arrival ack", queued_rows == [])
    fake["active"] = [{"msgid": MSGID, "lane": "main", "secs": 1}]
    n1 = gateway._feedback_scan_once(now=time.time() + 5)
    n2 = gateway._feedback_scan_once(now=time.time() + 20)
    rows = [
        json.loads(line) for line in wc.OUTBOX.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    print("evidence scan rows:", json.dumps(rows, ensure_ascii=False))
    check("scan emits started exactly once, as reply_notice",
          n1 == 1 and n2 == 0 and len(rows) == 1
          and rows[0].get("mode") == "reply_notice"
          and rows[0].get("msgid") == MSGID
          and rows[0].get("content") == STARTED
          and not rows[0].get("content", "").startswith("排队"))


def load_bridge(base: Path):
    """Load native_bridge with host import paths pointed at stubs.

    progress_notice is the emitter under test. gw2 and muse_cli are
    not installed in this sandbox; the two hardcoded path strings are
    substituted before exec, the same way the reply_file regression
    loads the bridge.
    """
    stub = base / "pydeps"
    pkg = stub / "muse_cli"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "cli.py").write_text(
        "def fmt_event(ev):\n"
        "    payload = ev.get('payload') or {}\n"
        "    return {'text': payload.get('display_text') or payload.get('content') or ''}\n"
        "def is_reply(ev):\n"
        "    return False\n",
        encoding="utf-8",
    )
    (pkg / "gateway.py").write_text(
        "class GatewayError(Exception):\n    pass\n",
        encoding="utf-8",
    )
    (stub / "gw2.py").write_text("class GW2:\n    pass\n", encoding="utf-8")
    os.environ["OUTBOX_ORDER_STUB"] = str(stub)
    source = (ROOT / "native-bridge" / "native_bridge.py").read_text(encoding="utf-8")
    source = source.replace(
        'BASE = "/home/hatch/workspace/native-bridge"',
        f"BASE = {str(base)!r}",
        1,
    )
    source = source.replace(
        'sys.path.insert(0, "/home/hatch/workspace/native-probe")',
        "sys.path.insert(0, os.environ['OUTBOX_ORDER_STUB'])",
        1,
    )
    module = types.ModuleType("native_bridge_order")
    module.__file__ = str(ROOT / "native-bridge" / "native_bridge.py")
    exec(compile(source, module.__file__, "exec"), module.__dict__)
    return module


def scenario_bridge_progress_emitter():
    """WeCom progress rows are reply_notice, and stop after the reply."""
    base = SBX / "bridge"
    if base.exists():
        shutil.rmtree(base)
    state = base / "wecom_state"
    state.mkdir(parents=True)
    (base / "shadow").mkdir()
    shutil.copy(ROOT / "native-bridge" / "config.json", base / "config.json")
    (base / "enabled-wecom").touch()
    bridge = load_bridge(base)
    bridge.CFG = json.loads((base / "config.json").read_text(encoding="utf-8"))
    bridge.CFG["channels"]["wecom"]["bot_state"] = str(state)
    worker = bridge.ChannelWorker.__new__(bridge.ChannelWorker)
    worker.ch = "wecom"
    worker.queue = []
    worker.log = lambda *args: None
    worker._poll_activity = lambda turn: None
    running = {
        "msgid": "88c3db3e-live",
        "text": "生成视频",
        "reply_started": False,
        "activities": [],
        "chatid": "sirhao",
        "chattype": "single",
    }
    worker.progress_notice(running, 120)
    rows = [
        json.loads(line)
        for line in (state / "outbox.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    print("evidence bridge progress row:", json.dumps(rows, ensure_ascii=False))
    check("bridge progress is one reply_notice bound to the msgid",
          len(rows) == 1
          and rows[0].get("mode") == "reply_notice"
          and rows[0].get("msgid") == "88c3db3e-live"
          and rows[0].get("chatid") == "sirhao"
          and "还在处理中" in rows[0].get("content", "")
          and "88c3db3e" in rows[0].get("content", ""))
    (state / "outbox.jsonl").unlink()
    done = dict(running)
    done["reply_delivered"] = True
    worker.progress_notice(done, 12 * 60)
    check("bridge progress_notice writes nothing after the formal reply",
          not (state / "outbox.jsonl").exists())


async def main():
    """Run the ordering proof and the regressions that sit next to it."""
    scenario_old_rule_predicts_the_live_bug()
    await scenario_live_sequence()
    await scenario_short_reply_still_uses_the_anchor()
    await scenario_wait_then_started()
    await scenario_progress_after_done()
    scenario_started_row_is_not_a_queue_ack()
    scenario_bridge_progress_emitter()


asyncio.run(main())

failed = [name for name, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
