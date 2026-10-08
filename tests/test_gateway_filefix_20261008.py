#!/usr/bin/env python3
"""Sandbox tests for the 2026-10-08 weixin gateway file-delivery fix:
B1) non-formal send_file rows that keep failing at the upload stage
    are no longer silently dropped after 3 failures - the user gets
    a WeChat text notice and the file is rerouted via the WeCom
    outbox (the file branch of _notify_stuck_formal). Upload-stage
    counting covers marker-tagged exceptions, EMPTY exceptions and
    the legacy CDN-500 errmsg pattern.
A)  compression ladder: parked n>=2 -> L1, n>=4 -> L2; cache keys
    carry the level; retries (n>=1) compress even below the 2MB
    threshold; level-up notices fire at most once per rung.
C2) file result rows carry file_bytes / file_md5 / compress_level.
The real gateway module is imported; network-facing methods are
stubbed on a bare Gateway instance and state paths are redirected
to a sandbox directory (same pattern as test_gateway_formalfix.py).
"""
import asyncio
import hashlib
import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path

SBX = Path("/tmp/gwfilefix-sbx")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True, exist_ok=True)
os.environ["HOME"] = str(SBX)

GW_PATH = os.environ.get(
    "MUSE_GATEWAY_PY",
    str(Path(__file__).resolve().parent.parent
        / "weixin-bot" / "gateway.py"))
spec = importlib.util.spec_from_file_location("gw", GW_PATH)
gw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gw)

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def bare_gateway():
    g = gw.Gateway.__new__(gw.Gateway)
    g.context = {}
    g._send_notice_failures = {}
    g._send_file_failures = {}
    g.write_status = lambda: None
    return g


# Redirect all mutable state into the sandbox.
gw.OUTBOX_RESULTS = SBX / "outbox_results.jsonl"
gw.OUTBOX_PARKED = SBX / "outbox_parked.json"
gw.OUTBOX_RETRY = SBX / "outbox_retry.json"
gw.OUTBOX_PARTIAL = SBX / "outbox_partial.json"
gw.COMPRESSED_DIR = SBX / "compressed"
WECOM_OUT = SBX / "wecom_outbox.jsonl"
WECOM_OUT.write_text("")
gw.STUCK_NOTICE_WECOM_OUTBOX = WECOM_OUT


def result_rows(item_id=None):
    if not gw.OUTBOX_RESULTS.exists():
        return []
    rows = [json.loads(l) for l in
            gw.OUTBOX_RESULTS.read_text().splitlines() if l.strip()]
    if item_id:
        rows = [r for r in rows if r.get("id") == item_id]
    return rows


# ---------- B1: send_file upload failures -> notify + reroute ----------
payload = SBX / "report.bin"
payload.write_bytes(b"payload-bytes" * 100)

sent_texts = []


async def fake_send_text(self, client, creds, to, content,
                         token_ctx="", client_id="",
                         message_state=2, item_id=""):
    sent_texts.append((to, content))
    return {"ret": 0}


async def empty_upload(self, client, creds, to_user_id, data, media_type):
    raise RuntimeError("")  # bare transport error: no message at all


g1 = bare_gateway()
g1.send_text = fake_send_text.__get__(g1)
g1._upload_media = empty_upload.__get__(g1)

item = {"id": "sf-up", "mode": "send_file", "to_user_id": "u1",
        "content": "", "file_path": str(payload)}
actions = []
for _ in range(3):
    ok = asyncio.run(g1.dispatch_outbox_item(None, {}, item))
    assert ok is False
    actions.append(g1._note_failure(item, g1._last_errmsg("sf-up")))

check("upload failure 1+2 park without notify",
      actions[0] == ("park", False) and actions[1] == ("park", False))
check("upload failure 3 consumed WITH notify (not silent)",
      actions[2] == ("drop", True))
rows = result_rows("sf-up")
check("result rows carry upload-stage marker + stage field",
      len(rows) == 3
      and all("upload-stage" in (r.get("errmsg") or "") for r in rows)
      and all(r.get("stage") == "upload" for r in rows))
check("parked/retry records cleaned after terminal reroute",
      "sf-up" not in g1._load_parked() and "sf-up" not in g1._load_retry())

# The notify the caller fires: real _notify_stuck_formal, file branch.
sent_texts.clear()
asyncio.run(g1._notify_stuck_formal(None, {}, item, 3))
wx = [c for (to, c) in sent_texts if to == "u1"]
check("wechat text notice sent to the send_file recipient",
      len(wx) == 1 and "尚未接受" in wx[0] and "转投" in wx[0]
      and "确认" in wx[0] and "已停止重试" in wx[0])
check("notice never claims delivered",
      all("已送达" not in c for (_t, c) in sent_texts))
wecom_rows = [json.loads(l) for l in WECOM_OUT.read_text().splitlines()
              if l.strip()]
check("wecom leg: notice row appended",
      any(r.get("mode") == "send"
          and "【微信通道提醒】" in (r.get("content") or "")
          for r in wecom_rows))
check("wecom leg: original file rerouted as send_file row",
      any(r.get("mode") == "send_file"
          and r.get("file_path") == str(payload) for r in wecom_rows))

# ---------- B1b: counting rules at the _note_failure level ----------
g2 = bare_gateway()
it2 = {"id": "sf-cnt", "mode": "send_file", "to_user_id": "u1",
       "file_path": str(payload)}
a, ntf = g2._note_failure(it2, "exception: upload-stage: ")
rec = g2._load_parked()["sf-cnt"]
check("empty upload-stage exception counts",
      int(rec.get("upload_fail") or 0) == 1 and a == "park" and not ntf)
a, ntf = g2._note_failure(
    it2, "exception: HTTPStatusError 500 for https://cdn.weixin.qq.com/x")
rec = g2._load_parked()["sf-cnt"]
check("legacy CDN-500 errmsg still counts",
      int(rec.get("upload_fail") or 0) == 2
      and int(rec.get("cdn_streak") or 0) == 1)
a, ntf = g2._note_failure(it2, "exception: upload-stage: boom")
check("third counted failure triggers reroute notify",
      a == "drop" and ntf is True)

g3 = bare_gateway()
it3 = {"id": "sf-send", "mode": "send_file", "to_user_id": "u1",
       "file_path": str(payload)}
for _ in range(3):
    a, ntf = g3._note_failure(it3, "sendmessage rejected: ret=-1")
rec = g3._load_parked().get("sf-send") or {}
check("send-stage failures do NOT count as upload failures",
      int(rec.get("upload_fail") or 0) == 0 and a == "park" and not ntf)

# Formal reply_file behaviour unchanged: parks forever, notify once.
g4 = bare_gateway()
it4 = {"id": "rf-formal", "mode": "reply_file", "msgid": "M1",
       "file_path": str(payload)}
acts = [g4._note_failure(it4, "exception: upload-stage: x")
        for _ in range(4)]
check("formal reply_file still parks (never consumed by the guard)",
      all(a == "park" for a, _ in acts))
check("formal notify fires exactly once at the threshold",
      [ntf for _, ntf in acts] == [False, False, True, False])

# ---------- A: compression ladder ----------
check("ladder mapping n->level",
      [gw.Gateway._compress_level_for(n) for n in (0, 1, 2, 3, 4, 9)]
      == [0, 0, 1, 1, 2, 2])

from PIL import Image
big_png = SBX / "big.png"
noise = Image.frombytes("RGB", (2000, 1500), os.urandom(2000 * 1500 * 3))
noise.save(big_png)
big_size = big_png.stat().st_size
check("ladder fixture over threshold", big_size > gw.FILE_COMPRESS_THRESHOLD)

g5 = bare_gateway()
outs = {lvl: g5._compress_file_sync(big_png, lvl) for lvl in (0, 1, 2)}
check("all rungs produced output", all(outs.values()))
check("rung cache paths are distinct (no cross-rung pollution)",
      len({str(outs[l]) for l in outs}) == 3)
for lvl, maxdim in ((0, 1600), (1, 1280), (2, 1024)):
    with Image.open(outs[lvl]) as ci:
        check(f"L{lvl} image within {maxdim}px", max(ci.size) <= maxdim)
check("higher rungs are not larger",
      outs[2].stat().st_size <= outs[0].stat().st_size)
check("same rung hits the same cache entry",
      g5._compress_file_sync(big_png, 1) == outs[1])
check("original untouched by ladder", big_png.stat().st_size == big_size)

# Retry trigger: below-threshold file compresses when parked_n >= 1.
small_png = SBX / "small.png"
Image.frombytes("RGB", (900, 700),
                os.urandom(900 * 700 * 3)).save(small_png)
check("retry fixture below threshold",
      small_png.stat().st_size < gw.FILE_COMPRESS_THRESHOLD)


async def prep(p, level=0, pn=0):
    return await g5._prepare_file_for_send(str(p), level, pn)


check("first attempt below threshold: no compression",
      asyncio.run(prep(small_png)) == (str(small_png), False))
p_retry, was = asyncio.run(prep(small_png, 1, 1))
check("retry (n>=1) below threshold compresses at L1",
      was is True and p_retry.endswith(".jpg"))

# Level-up notice: at most one per rung, persisted in parked record.
g6 = bare_gateway()
g6.send_text = fake_send_text.__get__(g6)
parked = {"lvrow": {"item": {"id": "lvrow"}, "n": 2,
                    "compress_level": 0, "compress_notified": []}}
g6._save_parked(parked)
sent_texts.clear()
asyncio.run(g6._maybe_notify_level_up(None, {}, "lvrow", "u1", 1))
asyncio.run(g6._maybe_notify_level_up(None, {}, "lvrow", "u1", 1))
lv_notices = [c for (_t, c) in sent_texts if "L1 档" in c]
check("level-up notice sent exactly once per rung", len(lv_notices) == 1)
rec = g6._load_parked()["lvrow"]
check("level-up state persisted in parked record",
      rec.get("compress_level") == 1
      and rec.get("compress_notified") == [1])

# ---------- C2: file result rows carry bytes/md5/level ----------
delivered = {}


async def fake_deliver(self, client, creds, to, fpath, token_ctx=""):
    delivered["path"] = fpath
    return {"ret": 0}


g7 = bare_gateway()
g7.send_text = fake_send_text.__get__(g7)
g7._deliver_file = fake_deliver.__get__(g7)
doc = SBX / "doc.bin"
doc.write_bytes(b"hello file" * 50)
ok = asyncio.run(g7.dispatch_outbox_item(
    None, {}, {"id": "sf-meta", "mode": "send_file", "to_user_id": "u1",
               "content": "", "file_path": str(doc)}))
row = result_rows("sf-meta")[-1]
check("send_file dispatch ok", ok is True and row.get("ok") is True)
check("result row has file_bytes of the sent file",
      row.get("file_bytes") == doc.stat().st_size)
check("result row has file_md5 of the sent file",
      row.get("file_md5") == hashlib.md5(doc.read_bytes()).hexdigest())
check("result row has compress_level 0 for uncompressed send",
      row.get("compress_level") == 0)

ok = asyncio.run(g7.dispatch_outbox_item(
    None, {}, {"id": "sf-meta2", "mode": "send_file", "to_user_id": "u1",
               "content": "", "file_path": str(big_png)}))
row = result_rows("sf-meta2")[-1]
comp = Path(delivered["path"])
check("compressed send: meta describes the compressed copy",
      row.get("file_bytes") == comp.stat().st_size
      and row.get("file_md5")
      == hashlib.md5(comp.read_bytes()).hexdigest()
      and row.get("compress_level") == 0 and comp != big_png)

fails = [n for n, ok_ in RESULTS if not ok_]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
