#!/usr/bin/env python3
"""A formal text reply and its reply_file both deliver for one msgid.

LT2 live (2026-10-09): bridge row d5ef69115473 (text 「做好了，视频在下面」)
delivered ok, then file0 54e6c0203633 was swallowed with
"suppressed: msgid already has a delivered formal reply".
The media row is part of the same answer and must go out after the text.
A late progress update for that msgid stays suppressed.

State paths are redirected into a sandbox. Network sends are stubbed.
"""
import asyncio
import copy
import importlib.util
import json
import os
import shutil
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/reply-file-after-formal-sbx")
MSGID = "7514310316847596936"
TEXT = "做好了，视频在下面"

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def sandbox_paths(mod, directory):
    """Point one gateway's outbox files at an empty sandbox directory."""
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    mod.OUTBOX = directory / "outbox.jsonl"
    mod.OUTBOX_RESULTS = directory / "outbox_results.jsonl"
    mod.OUTBOX_PARTIAL = directory / "outbox_partial.json"
    mod.OUTBOX_PARKED = directory / "outbox_parked.json"
    mod.OUTBOX_RETRY = directory / "outbox_retry.json"
    mod.CANCELLED_FILE = directory / "cancelled.json"
    mod.STATUS = directory / "status.json"
    if hasattr(mod, "COMPRESSED_DIR"):
        mod.COMPRESSED_DIR = directory / "compressed"


def write_outbox(mod, rows):
    mod.OUTBOX.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def result_for(mod, item_id):
    found = None
    for row in read_jsonl(mod.OUTBOX_RESULTS):
        if row.get("id") == item_id:
            found = row
    return found


def load_bridge(base: Path):
    """Load the real bridge with its host paths pointed at `base`.

    The module hardcodes /home/hatch/workspace for BASE and for the
    gw2/muse_cli imports. Those trees are not in this sandbox, and the
    two path strings are substituted before exec. deliver_reply and
    bridge_row_id are otherwise the source under test.
    """
    stub = SBX / "pydeps"
    pkg = stub / "muse_cli"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "cli.py").write_text(
        "def fmt_event(ev):\n    return {}\n"
        "def is_reply(ev):\n    return False\n",
        encoding="utf-8",
    )
    (pkg / "gateway.py").write_text(
        "class GatewayError(Exception):\n    pass\n",
        encoding="utf-8",
    )
    (stub / "gw2.py").write_text("class GW2:\n    pass\n", encoding="utf-8")
    os.environ["REPLY_FILE_TEST_STUB"] = str(stub)
    if str(stub) not in sys.path:
        sys.path.insert(0, str(stub))
    source = (ROOT / "native-bridge" / "native_bridge.py").read_text(
        encoding="utf-8")
    source = source.replace(
        'BASE = "/home/hatch/workspace/native-bridge"',
        f"BASE = {str(base)!r}",
        1,
    )
    source = source.replace(
        'sys.path.insert(0, "/home/hatch/workspace/native-probe")',
        "sys.path.insert(0, os.environ['REPLY_FILE_TEST_STUB'])",
        1,
    )
    mod = types.ModuleType("native_bridge_rf")
    mod.__file__ = str(ROOT / "native-bridge" / "native_bridge.py")
    exec(compile(source, mod.__file__, "exec"), mod.__dict__)
    return mod


# ---------------------------------------------------------------- bridge rows
BRIDGE = SBX / "bridge"
WX_STATE = BRIDGE / "weixin_state"
if BRIDGE.exists():
    shutil.rmtree(BRIDGE)
WX_STATE.mkdir(parents=True)
(BRIDGE / "shadow").mkdir()
shutil.copy(ROOT / "native-bridge" / "config.json", BRIDGE / "config.json")
nb = load_bridge(BRIDGE)
check(
    "incident ids are the bridge text + file0 pair",
    nb.bridge_row_id(MSGID) == "d5ef69115473"
    and nb.bridge_row_id(MSGID, "file0") == "54e6c0203633",
)

nb.BASE = str(BRIDGE)
nb.STATE_F = str(BRIDGE / "state.json")
nb.STATUS_F = str(BRIDGE / "status.json")
nb.CFG = copy.deepcopy(nb.CFG)
nb.CFG["channels"]["weixin"]["bot_state"] = str(WX_STATE)
(BRIDGE / "enabled-weixin").touch()

clip = BRIDGE / "clip.mp4"
clip.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32)
still = BRIDGE / "still.png"
still.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16)

nb.deliver_reply(
    "weixin",
    MSGID,
    f"{TEXT}\n[[FILE:{clip}]]\n[[FILE:{still}]]",
)
queued = read_jsonl(WX_STATE / "outbox.jsonl")
check("bridge queues text then both files",
      [r.get("mode") for r in queued] == ["reply", "reply_file", "reply_file"])
check("bridge text is the formal reply",
      queued[0].get("id") == "d5ef69115473"
      and queued[0].get("content") == TEXT
      and queued[0].get("msgid") == MSGID)
check("bridge file0 is the incident row and does not repeat the text",
      queued[1].get("id") == "54e6c0203633"
      and queued[1].get("file_path") == str(clip)
      and queued[1].get("content") == ""
      and queued[1].get("msgid") == MSGID)
check("bridge file1 is a second media row for the same msgid",
      queued[2].get("mode") == "reply_file"
      and queued[2].get("msgid") == MSGID
      and queued[2].get("file_path") == str(still)
      and queued[2].get("content") == "")


# ---------------------------------------------------------------- weixin dispatch
gw = load_module("gw_rf", ROOT / "weixin-bot" / "gateway.py")
sandbox_paths(gw, SBX / "weixin")
media = SBX / "weixin" / "clip.mp4"
shutil.copy(clip, media)
extra = SBX / "weixin" / "still.png"
shutil.copy(still, extra)

reply_id = "d5ef69115473"
file_id = "54e6c0203633"
file2_id = nb.bridge_row_id(MSGID, "file1")
update_id = "late-update"
write_outbox(gw, [
    {"id": reply_id, "mode": "reply", "msgid": MSGID, "content": TEXT},
    {"id": file_id, "mode": "reply_file", "msgid": MSGID,
     "file_path": str(media), "content": ""},
    {"id": file2_id, "mode": "reply_file", "msgid": MSGID,
     "file_path": str(extra), "content": ""},
    {"id": update_id, "mode": "update", "msgid": MSGID, "content": "还在做"},
])

sent_texts = []
delivered = []


async def fake_send_text(self, client, creds, to, content,
                         token_ctx="", client_id="",
                         message_state=2, item_id=""):
    sent_texts.append(content)
    return {"ret": 0}


async def fake_deliver(self, client, creds, to, fpath, token_ctx=""):
    delivered.append(fpath)
    return {"ret": 0}


async def no_typing(self, client, creds, to, token_ctx, status):
    return None


g = gw.Gateway.__new__(gw.Gateway)
g.context = {MSGID: {"from_user_id": "u1", "context_token": "tok",
                     "client_id": "c1"}}
g.feedback_track = {}
g.send_text = fake_send_text.__get__(g)
g._deliver_file = fake_deliver.__get__(g)
g._typing_best_effort = no_typing.__get__(g)
g.write_status = lambda: None


async def dispatch_weixin():
    reply_ok = await g.dispatch_outbox_item(
        None, {},
        {"id": reply_id, "mode": "reply", "msgid": MSGID, "content": TEXT})
    file_ok = await g.dispatch_outbox_item(
        None, {},
        {"id": file_id, "mode": "reply_file", "msgid": MSGID,
         "file_path": str(media), "content": ""})
    file2_ok = await g.dispatch_outbox_item(
        None, {},
        {"id": file2_id, "mode": "reply_file", "msgid": MSGID,
         "file_path": str(extra), "content": ""})
    update_consumed = await g.dispatch_outbox_item(
        None, {},
        {"id": update_id, "mode": "update", "msgid": MSGID, "content": "还在做"})
    return reply_ok, file_ok, file2_ok, update_consumed


reply_ok, file_ok, file2_ok, update_consumed = asyncio.run(dispatch_weixin())
reply_res = result_for(gw, reply_id)
file_res = result_for(gw, file_id)
file2_res = result_for(gw, file2_id)
update_res = result_for(gw, update_id)

check("weixin formal text reply ok",
      reply_ok is True and reply_res is not None and reply_res.get("ok") is True)
check("weixin reply_file after that text ok",
      file_ok is True and file_res is not None and file_res.get("ok") is True
      and not file_res.get("suppressed"))
check("weixin reply_file was actually handed to delivery",
      delivered[:1] == [str(media)])
check("weixin file caption is the filename, not a second copy of the text",
      any(t == f"📎 {media.name}" for t in sent_texts)
      and TEXT not in sent_texts[1:])
check("weixin second reply_file for the same msgid also ok",
      file2_ok is True and file2_res is not None and file2_res.get("ok") is True
      and delivered[1:] == [str(extra)])
check("weixin late update still suppressed",
      update_consumed is True and update_res is not None
      and update_res.get("ok") is not True
      and update_res.get("suppressed") is True
      and update_res.get("errmsg") == "suppressed: msgid already has a delivered formal reply")


# ---------------------------------------------------------------- wecom dispatch
wc = load_module("wc_rf", ROOT / "wecom-bot" / "gateway.py")
sandbox_paths(wc, SBX / "wecom")
wc_media = SBX / "wecom" / "clip.mp4"
shutil.copy(clip, wc_media)
write_outbox(wc, [
    {"id": reply_id, "mode": "reply", "msgid": MSGID, "content": TEXT},
    {"id": file_id, "mode": "reply_file", "msgid": MSGID,
     "file_path": str(wc_media), "content": ""},
    {"id": update_id, "mode": "update", "msgid": MSGID, "content": "还在做"},
])

wc_sent = []
wc_uploaded = []


async def fake_respond(self, req_id, body):
    wc_sent.append(body)
    return {"errcode": 0, "errmsg": ""}


async def fake_upload(self, data, mtype, name):
    wc_uploaded.append(name)
    return "media-1"


w = wc.Gateway.__new__(wc.Gateway)
w.reqmap = {MSGID: {"req_id": "req-1", "stream_id": "stream-1"}}
w.msgs_sent = 0
w.open_streams = {MSGID: {"stream_id": "stream-1"}}
w.feedback_track = {}
w.respond = fake_respond.__get__(w)
w.upload_media = fake_upload.__get__(w)
w.write_status = lambda: None


async def dispatch_wecom():
    reply_ok_wc = await w.dispatch_outbox_item(
        {"id": reply_id, "mode": "reply", "msgid": MSGID, "content": TEXT})
    file_ok_wc = await w.dispatch_outbox_item(
        {"id": file_id, "mode": "reply_file", "msgid": MSGID,
         "file_path": str(wc_media), "content": ""})
    update_wc = await w.dispatch_outbox_item(
        {"id": update_id, "mode": "update", "msgid": MSGID, "content": "还在做"})
    return reply_ok_wc, file_ok_wc, update_wc


reply_ok_wc, file_ok_wc, update_wc = asyncio.run(dispatch_wecom())
wc_reply = result_for(wc, reply_id)
wc_file = result_for(wc, file_id)
wc_update = result_for(wc, update_id)

check("wecom formal text reply ok",
      reply_ok_wc is True and wc_reply is not None and wc_reply.get("ok") is True)
check("wecom reply_file after that text ok",
      file_ok_wc is True and wc_file is not None and wc_file.get("ok") is True
      and not wc_file.get("suppressed")
      and wc_uploaded == [wc_media.name])
check("wecom late update still suppressed",
      update_wc is True and wc_update is not None
      and wc_update.get("ok") is not True
      and wc_update.get("suppressed") is True)

fails = [name for name, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
