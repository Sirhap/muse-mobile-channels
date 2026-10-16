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

# ---- 2026-10-11 follow-up: iLink shape variants ----
# A live quote still degraded to the generic marker, so the parser
# now tolerates type spellings / container / key variants.

# 13. type as numeric string
t = wx_text(item_text("这个", ref_item("1", text_item={"text": "数字串类型"})))
check("wx: numeric-string type text quote", "（引用：数字串类型）" in t)

# 14. type as name string
t = wx_text(item_text("看", ref_item("image", image_item={})))
check("wx: name-string type image quote", "（引用：[图片]）" in t)

# 15. item_type key instead of type
t = wx_text(item_text("看", {"message_item": {"item_type": 4,
                        "file_item": {"file_name": "表.xlsx"}}}))
check("wx: item_type key file quote", "（引用：[文件 表.xlsx]）" in t)

# 16. item under ref["item"]
t = wx_text(item_text("看", {"item": {"type": 5, "video_item": {}}}))
check("wx: ref['item'] container video quote", "（引用：[视频]）" in t)

# 17. item under ref["item_list"][0]
t = wx_text(item_text("看", {"item_list": [{"type": 2, "image_item": {}}]}))
check("wx: ref['item_list'] container image quote", "（引用：[图片]）" in t)

# 18. ref itself is the item (direct-hanging type + text_item)
t = wx_text(item_text("这个", {"type": 1, "text_item": {"text": "直挂原文"}}))
check("wx: ref-as-item text quote", "（引用：直挂原文）" in t)

# 19. text under ref["text"] (no container at all)
t = wx_text(item_text("这个", {"text": "正文键原文"}))
check("wx: ref['text'] quote", "（引用：正文键原文）" in t)

# 20. text under ref["content"]
t = wx_text(item_text("这个", {"content": "内容键原文"}))
check("wx: ref['content'] quote", "（引用：内容键原文）" in t)

# 21. flat item["text"] on a text item
t = wx_text(item_text("这个", {"message_item": {"type": 1,
                              "text": "扁平文本"}}))
check("wx: flat item text quote", "（引用：扁平文本）" in t)

# 22. file_name at ref level
t = wx_text(item_text("看", {"message_item": {"type": 4},
                             "file_name": "顶层名.pdf"}))
check("wx: ref-level file_name quote", "（引用：[文件 顶层名.pdf]）" in t)

# 23. voice transcription flat on the item
t = wx_text(item_text("听", {"message_item": {"type": "voice",
                              "text": "语音扁平转写"}}))
check("wx: voice flat-text quote", "（引用：语音扁平转写）" in t)

# 24. diverted-raw retention wired into the divert branch.
# handle_inbound needs a full live gateway (client/creds/context),
# so this asserts at source level: the retention constant targets
# state/diverted_raw.jsonl and the write sits inside the divert
# branch, after the spool append.
_src = (ROOT / "weixin-bot" / "gateway.py").read_text(encoding="utf-8")
check("wx: DIVERTED_RAW constant points at state file",
      'DIVERTED_RAW = STATE / "diverted_raw.jsonl"' in _src)
_spool_i = _src.index("spool/weixin.jsonl")
_raw_i = _src.index("self.append_jsonl(DIVERTED_RAW, entry)")
_fb_i = _src.index("if not diverted:", _spool_i)
check("wx: diverted_raw written inside divert branch after spool",
      _spool_i < _raw_i < _fb_i)

# ---- 2026-10-11 final fix: refs carry only an id; resolve locally ----
# Live capture (diverted_raw.jsonl): iLink ref_msg for a quote is
# {"message_item": {"type": 0, "msg_id": "<quoted platform id>", ...}}
# — no content at all. _find_ref_qid extracts the id; _resolve_quote
# recovers the content from the spool / INBOX / sent index.

SAMPLE_REF = {"message_item": {"type": 0,
              "create_time_ms": 1791650680000,
              "update_time_ms": 1791650680000,
              "is_completed": True,
              "msg_id": "〈msgid已打码〉",
              "button_item_list": [], "at_bot_username_list": []}}


def sample_msg(text="测试"):
    return {"item_list": [{"type": 1, "text_item": {"text": text},
                           "ref_msg": SAMPLE_REF}]}


# 25. qid extraction from the real captured structure
check("wx: qid from captured ref shape",
      wx.Gateway._find_ref_qid(sample_msg()) == "〈msgid已打码〉")

# 26. qid from a message-level ref
msg = {"item_list": [{"type": 1, "text_item": {"text": "测试"}}],
       "ref_msg": {"message_item": {"msg_id": "7514000000000000001"}}}
check("wx: qid from message-level ref",
      wx.Gateway._find_ref_qid(msg) == "7514000000000000001")

# 27. no ref / ref without any id key -> ""
check("wx: qid empty without ref",
      wx.Gateway._find_ref_qid(item_text("普通")) == "")
check("wx: qid empty when ref carries no id",
      wx.Gateway._find_ref_qid(item_text("这个", ref_item(2, image_item={}))) == "")

# Fixtures for _resolve_quote / _index_sent in a temp state dir.
QD = SBX / "quote-resolve"
QD.mkdir(parents=True, exist_ok=True)
Q_SPOOL = QD / "spool.jsonl"
Q_INBOX = QD / "inbox.jsonl"
Q_SENT = QD / "sent_index.jsonl"


def write_jsonl(p, rows):
    p.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n"
                         for r in rows), encoding="utf-8")


write_jsonl(Q_SPOOL, [
    {"msgid": "Q-OLD", "text": "旧的一条"},
    {"msgid": "Q-SPOOL", "text": "被引用的原文"},
    {"msgid": "Q-LONG", "text": "长" * 250},
])
write_jsonl(Q_INBOX, [{"msgid": "Q-INBOX", "text": "冷通道原文"}])
write_jsonl(Q_SENT, [
    {"mid": "Q-BOT", "kind": "text", "content": "机器人回复内容", "ts": 1},
    {"mid": "Q-FILE", "kind": "file", "content": "成片.mp4", "ts": 1},
])
_saved_globals = (wx.QUOTE_SPOOL, wx.INBOX, wx.SENT_INDEX)
wx.QUOTE_SPOOL, wx.INBOX, wx.SENT_INDEX = Q_SPOOL, Q_INBOX, Q_SENT
_g = wx.Gateway.__new__(wx.Gateway)

# 28. resolve hits each source; unknown id -> None
check("wx: resolve via spool", _g._resolve_quote("Q-SPOOL") == "（引用：被引用的原文）")
check("wx: resolve via inbox", _g._resolve_quote("Q-INBOX") == "（引用：冷通道原文）")
check("wx: resolve via sent index (text)",
      _g._resolve_quote("Q-BOT") == "（引用：机器人回复内容）")
check("wx: resolve via sent index (file)",
      _g._resolve_quote("Q-FILE") == "（引用：[文件 成片.mp4]）")
check("wx: resolve unknown id -> None", _g._resolve_quote("Q-NOPE") is None)
check("wx: resolve truncates at 200 chars",
      _g._resolve_quote("Q-LONG") == "（引用：" + "长" * 200 + "…）")

# 29. _index_sent probes resp id keys (top level, then data)
Q_SENT2 = QD / "sent_index2.jsonl"
wx.SENT_INDEX = Q_SENT2
_g._index_sent({"ret": 0, "message_id": "M-TOP"}, "text", "顶层文本")
_g._index_sent({"ret": 0, "data": {"msg_id": "M-DATA"}}, "file", "数据.mp4")
_g._index_sent({"ret": 0}, "text", "无ID不写")
_g._index_sent(None, "text", "非dict不写")
_rows = [json.loads(l)
         for l in Q_SENT2.read_text(encoding="utf-8").splitlines()
         if l.strip()]
check("wx: _index_sent writes top-level message_id",
      any(r.get("mid") == "M-TOP" and r.get("kind") == "text"
          and r.get("content") == "顶层文本" for r in _rows))
check("wx: _index_sent writes data.msg_id",
      any(r.get("mid") == "M-DATA" and r.get("kind") == "file"
          for r in _rows))
check("wx: _index_sent skips resp without id", len(_rows) == 2)
wx.SENT_INDEX = Q_SENT

# 30. extract -> generic marker -> resolve -> replace (the exact
# composition handle_inbound performs), plus the unresolvable case.
write_jsonl(Q_SPOOL, [{"msgid": "〈msgid已打码〉", "text": "继续修复"}])
_t, _m = wx.Gateway._extract_text(sample_msg())
check("wx: captured shape alone still generic marker",
      "（引用：[消息]）" in _t)
_res = _g._resolve_quote(wx.Gateway._find_ref_qid(sample_msg()))
if _res:
    _t = _t.replace("（引用：[消息]）", _res)
check("wx: composition resolves quote content", "（引用：继续修复）" in _t)
_t2, _m2 = wx.Gateway._extract_text(sample_msg("测试2"))
_res2 = _g._resolve_quote("7514999999999999999")
check("wx: unresolvable quote keeps generic marker",
      _res2 is None and "（引用：[消息]）" in _t2)

wx.QUOTE_SPOOL, wx.INBOX, wx.SENT_INDEX = _saved_globals

# 31. the resolve hook sits in handle_inbound between extraction
# and entry construction (source-level, like check 24).
_ext_i = _src.index("text, media = self._extract_text(msg)")
_res_i = _src.index("self._resolve_quote(self._find_ref_qid(msg))")
_ent_i = _src.index("entry = {", _ext_i)
check("wx: resolve hook between extract and entry build",
      _ext_i < _res_i < _ent_i)

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
         "chatid": "testuser", "from": {"userid": "testuser"},
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

# I-1 extraction isinstance guards: malformed non-dict nested fields in
# Weixin payloads are treated as absent instead of raising AttributeError.
_malformed_msg = {
    "message_id": "m-malformed-nested",
    "item_list": [
        "not-a-dict-item",
        123,
        {"type": 1, "text_item": "not-a-dict", "ref_msg": "bad-ref"},
        {"type": 2, "image_item": ["not-a-dict"]},
        {"type": 3, "voice_item": 456},
        {"type": 4, "file_item": "not-a-dict"},
        {"type": 5, "video_item": 789},
        {"type": 1, "text_item": {"text": "有效文本"}, "ref_msg": {"title": "引用标题"}},
    ],
    "ref_msg": "non-dict-top-ref",
}
_m_text, _m_media = wx.Gateway._extract_text(_malformed_msg)
_m_sanitized = wx.Gateway._sanitize_raw(_malformed_msg)
_m_qid = wx.Gateway._find_ref_qid(_malformed_msg)
check("weixin I-1 isinstance guards: malformed nested non-dict fields do not raise",
      "有效文本" in _m_text
      and "（引用：引用标题）" in _m_text
      and len(_m_media) == 3
      and isinstance(_m_sanitized, dict)
      and _m_qid == ""
      and wx.Gateway._extract_text({"item_list": "not-a-list"}) == ("", []))

failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
