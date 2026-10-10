#!/usr/bin/env python3
"""Quoted-message extraction completeness (2026-10-11).

Bug: a Weixin message quoting a non-text bubble (image/video/file/
voice/card) reached the bridge with the quote silently dropped —
the user quoted a message, typed one word, and the agent saw only
that word. Weixin _extract_text only surfaced text/title refs;
WeCom's callback only surfaced text/voice quotes. Both now degrade
every other quote shape to a typed marker （引用：[…]).

Weixin side calls Gateway._extract_text directly. WeCom side drives
handle_message_callback on a bare Gateway with state paths rebound
to a sandbox, respond/_download_media stubbed, and the hardcoded
production spool write rerouted (same pattern as
test_wecom_align_20261009.py).

Run: ~/workspace/wecom-bot/.venv/bin/python \
    tests/test_quote_extract_20261011.py
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
SBX = Path("/tmp/quote-extract-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)


def load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


wx = load("wxgw_quote", "weixin-bot/gateway.py")
wc = load("wcgw_quote", "wecom-bot/gateway.py")

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


# ================= Weixin _extract_text =================

def wx_text(msg):
    text, _media = wx.Gateway._extract_text(msg)
    return text


def item_text(t, ref=None):
    it = {"type": 1, "text_item": {"text": t}}
    if ref is not None:
        it["ref_msg"] = ref
    return {"item_list": [it]}


def ref_item(rtype, **nodes):
    ri = {"type": rtype}
    ri.update(nodes)
    return {"message_item": ri}


# 1. plain text quote (historical behaviour preserved)
t = wx_text(item_text("这个", ref_item(1, text_item={"text": "原文内容"})))
check("wx: text quote kept verbatim", "（引用：原文内容）" in t and "这个" in t)

# 2. long text quote truncated at 200 chars + …
long_ref = "啊" * 250
t = wx_text(item_text("长", ref_item(1, text_item={"text": long_ref})))
check("wx: long text quote truncated",
      ("（引用：" + "啊" * 200 + "…）") in t and "啊" * 201 not in t)

# 3. image quote
t = wx_text(item_text("看", ref_item(2, image_item={})))
check("wx: image quote marker", "（引用：[图片]）" in t)

# 4. video quote
t = wx_text(item_text("看", ref_item(5, video_item={})))
check("wx: video quote marker", "（引用：[视频]）" in t)

# 5. file quote, named and unnamed
t = wx_text(item_text("看", ref_item(4, file_item={"file_name": "报告.pdf"})))
check("wx: file quote marker with name", "（引用：[文件 报告.pdf]）" in t)
t = wx_text(item_text("看", ref_item(4, file_item={})))
check("wx: file quote marker unnamed", "（引用：[文件]）" in t)

# 6. voice quote: with and without transcription
t = wx_text(item_text("听", ref_item(3, voice_item={"text": "语音转写稿"})))
check("wx: voice quote uses transcription", "（引用：语音转写稿）" in t)
t = wx_text(item_text("听", ref_item(3, voice_item={})))
check("wx: voice quote marker without transcription",
      "（引用：[语音]）" in t)

# 7. title-only quote (historical behaviour preserved)
t = wx_text(item_text("这个", {"title": "卡片标题"}))
check("wx: title quote kept", "（引用：卡片标题）" in t)

# 8. message-level ref_msg fallback (iLink shape variant)
msg = {"item_list": [{"type": 1, "text_item": {"text": "这个"}}],
       "ref_msg": ref_item(1, text_item={"text": "消息级原文"})}
t = wx_text(msg)
check("wx: message-level ref_msg honoured", "（引用：消息级原文）" in t)

# 8b. item-level ref wins over message-level ref
msg = {"item_list": [{"type": 1, "text_item": {"text": "看"},
                      "ref_msg": ref_item(2, image_item={})}],
       "ref_msg": ref_item(1, text_item={"text": "不应出现"})}
t = wx_text(msg)
check("wx: item-level ref takes precedence",
      "（引用：[图片]）" in t and "不应出现" not in t)

# 9. no quote -> nothing appended
t = wx_text({"item_list": [{"type": 1, "text_item": {"text": "普通消息"}}]})
check("wx: no quote, no marker", "引用" not in t and t == "普通消息")

# 10. ref trace only (msgid, no message_item) -> generic marker
t = wx_text(item_text("这个", {"msgid": "7514000000000000000"}))
check("wx: msgid-only ref degrades to generic marker",
      "（引用：[消息]）" in t)

# 11. unknown quoted type, no title -> generic marker
t = wx_text(item_text("这个", ref_item(99)))
check("wx: unknown ref type degrades to generic marker",
      "（引用：[消息]）" in t)

# 12. malformed ref (message_item not a dict) -> no raise, generic marker
t = wx_text(item_text("这个", {"message_item": ["junk"]}))
check("wx: malformed ref fail-silent with generic marker",
      "（引用：[消息]）" in t and "这个" in t)

# ================= WeCom handle_message_callback =================

SPOOL_SBX = SBX / "spool-wecom.jsonl"


def fresh_state(name):
    d = SBX / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    wc.STATE = d
    wc.INBOX = d / "inbox.jsonl"
    wc.OUTBOX = d / "outbox.jsonl"
    wc.OUTBOX_RESULTS = d / "outbox_results.jsonl"
    wc.OUTBOX_OFFSET = d / "outbox.offset"
    wc.OUTBOX_RETRY = d / "outbox_retry.json"
    wc.OUTBOX_PARTIAL = d / "outbox_partial.json"
    wc.OUTBOX_PARKED = d / "outbox_parked.json"
    wc.REQMAP = d / "reqmap.json"
    wc.STATUS = d / "status.json"
    wc.SEEN_FILE = d / "seen_ids.jsonl"
    wc.HOOK_STATE_DIR = d / "hookstate"
    return d


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
    return g


def read_jsonl(p):
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip()]


async def wecom_case(name, body, expect, absent=None):
    d = fresh_state(name)
    if SPOOL_SBX.exists():
        SPOOL_SBX.unlink()
    g = make_gateway()

    def recorder(path, obj):
        if "native-bridge" in str(path):
            wc.append_jsonl_line(SPOOL_SBX, obj)
        else:
            wc.append_jsonl_line(path, obj)

    g.append_jsonl = recorder

    async def fake_respond(req_id, body_):
        return {"errcode": 0}

    g.respond = fake_respond

    async def fake_download(media, msgid):
        return None

    g._download_media = fake_download
    frame = {"headers": {"req_id": f"req-{name}"}, "body": body}
    await g.handle_message_callback(frame)
    rows = read_jsonl(SPOOL_SBX) + read_jsonl(d / "inbox.jsonl")
    texts = [r.get("text", "") for r in rows if r.get("msgid") == body["msgid"]]
    ok = len(texts) == 1 and expect in texts[0]
    if absent:
        ok = ok and absent not in texts[0]
    check(f"wecom: {name}", ok)


def wbody(msgid, quote, text="同意"):
    b = {"msgid": msgid, "msgtype": "text", "chattype": "single",
         "chatid": "sirhao", "from": {"userid": "sirhao"},
         "text": {"content": text}}
    if quote is not None:
        b["quote"] = quote
    return b


async def wecom_all():
    await wecom_case("quote-text",
                     wbody("Q-TEXT", {"msgtype": "text",
                                      "text": {"content": "原消息"}}),
                     "（引用：原消息）")
    await wecom_case("quote-voice",
                     wbody("Q-VOICE", {"msgtype": "voice",
                                       "voice": {"content": "语音内容"}}),
                     "（引用：语音内容）")
    await wecom_case("quote-image",
                     wbody("Q-IMG", {"msgtype": "image", "image": {}}),
                     "（引用：[图片]）")
    await wecom_case("quote-file",
                     wbody("Q-FILE", {"msgtype": "file", "file": {}}),
                     "（引用：[文件]）")
    await wecom_case("quote-video",
                     wbody("Q-VIDEO", {"msgtype": "video", "video": {}}),
                     "（引用：[视频]）")
    await wecom_case("quote-other",
                     wbody("Q-LINK", {"msgtype": "link"}),
                     "（引用：[消息]）")
    await wecom_case("quote-none",
                     wbody("Q-NONE", None, text="普通消息"),
                     "普通消息", absent="引用")


asyncio.run(wecom_all())

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
