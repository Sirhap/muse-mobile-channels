#!/usr/bin/env python3
"""Sandbox tests for the 2026-10-06 gateway changes (weixin):
1) formal reply/reply_file exempt from the 10-attempt dead-letter;
2) stuck-formal user notification fires exactly once at the threshold;
3) non-formal rows still dead-letter as before;
4) large files are compressed before send (image via PIL, video via
   ffmpeg), caption gains the 已压缩 note, originals untouched.
The real gateway module is imported; network-facing methods are
stubbed on a bare Gateway instance and state paths are redirected
to a sandbox directory.
"""
import asyncio
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

SBX = Path("/tmp/gwfix-sbx")
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
    return g


# ---------- 4a. image compression ----------
from PIL import Image
big_png = SBX / "big.png"
noise = Image.frombytes("RGB", (2000, 1500), os.urandom(2000 * 1500 * 3))
noise.save(big_png)
big_size = big_png.stat().st_size
print("big.png bytes:", big_size)
check("fixture image over threshold", big_size > gw.FILE_COMPRESS_THRESHOLD)

g = bare_gateway()
gw.COMPRESSED_DIR = SBX / "compressed"
out = g._compress_file_sync(big_png)
check("image compressed to jpg", out is not None and out.suffix == ".jpg")
if out:
    with Image.open(out) as ci:
        check("image resized <=1600", max(ci.size) <= 1600)
        check("image format JPEG", ci.format == "JPEG")
    check("image smaller", out.stat().st_size < big_size)
check("original untouched", big_png.stat().st_size == big_size)

# ---------- 4b. video compression (real 3.59MB clip) ----------
src_video = Path(os.environ.get(
    "MUSE_TEST_VIDEO",
    "/home/hatch/workspace/imagine_media/"
    "media-generation-muse-project-intro-part1-0-"
    "99ffee54-cfb8-4e05-958a-c57953c32220.mp4"))
if src_video.exists():
    vout = g._compress_file_sync(src_video)
    check("video compressed", vout is not None)
    if vout:
        print("video:", src_video.stat().st_size, "->",
              vout.stat().st_size)
        check("video smaller", vout.stat().st_size
              < src_video.stat().st_size)
else:
    print("skip video fixture (missing)")

# ---------- 4c. small / non-media fallbacks ----------
small = SBX / "small.jpg"
Image.new("RGB", (100, 100), (9, 9, 9)).save(small)


async def prep(p):
    return await g._prepare_file_for_send(str(p))


check("small file not compressed",
      asyncio.run(prep(small)) == (str(small), False))
text3mb = SBX / "big.txt"
text3mb.write_bytes(b"x" * 3_000_000)
check("non-media not compressed",
      asyncio.run(prep(text3mb)) == (str(text3mb), False))

# ---------- 4d. dispatch: compressed path + caption note ----------
sent_texts = []
delivered = {}


async def fake_send_text(self, client, creds, to, content,
                         token_ctx="", client_id="",
                         message_state=2, item_id=""):
    sent_texts.append(content)
    return {"ret": 0}


async def fake_deliver(self, client, creds, to, fpath, token_ctx=""):
    delivered["path"] = fpath
    return {"ret": 0}


g2 = bare_gateway()
g2.context = {"M1": {"from_user_id": "u1", "context_token": "tok"}}
g2.send_text = fake_send_text.__get__(g2)
g2._deliver_file = fake_deliver.__get__(g2)
g2._caption_already_sent = lambda item_id: False
g2._mark_caption_sent = lambda item_id: None
g2._feedback_on_reply = lambda msgid: None
g2.write_status = lambda: None
g2.append_jsonl = lambda path, obj: None


async def disp(item):
    return await g2.dispatch_outbox_item(None, {}, item)


ok = asyncio.run(disp({"id": "rf1", "mode": "reply_file", "msgid": "M1",
                       "content": "这是说明", "file_path": str(big_png)}))
check("reply_file dispatch ok", ok is True)
check("reply_file delivered compressed path",
      delivered.get("path", "").endswith(".jpg"))
check("caption has compression note",
      any("已自动压缩发送" in t for t in sent_texts))

sent_texts.clear()
delivered.clear()
ok = asyncio.run(disp({"id": "sf1", "mode": "send_file",
                       "to_user_id": "u1", "content": "",
                       "file_path": str(big_png)}))
check("send_file dispatch ok", ok is True)
check("send_file delivered compressed path",
      delivered.get("path", "").endswith(".jpg"))
check("send_file synthetic note caption",
      any(t.strip() == "（文件较大，已自动压缩发送）" for t in sent_texts))

# ---------- 1/2/3. outbox loop: exemption, notify-once, dead-letter ----------
LOOP = SBX / "loop"
import shutil
shutil.rmtree(LOOP, ignore_errors=True)
LOOP.mkdir(parents=True, exist_ok=True)
gw.OUTBOX = LOOP / "outbox.jsonl"
gw.OUTBOX_OFFSET = LOOP / "outbox.offset"
gw.OUTBOX_RESULTS = LOOP / "outbox_results.jsonl"
gw.OUTBOX_RETRY = LOOP / "outbox_retry.json"
gw.OUTBOX_PARTIAL = LOOP / "outbox_partial.json"
gw.SEND_MAX_ATTEMPTS = 3
gw.FORMAL_STUCK_NOTIFY_ATTEMPTS = 5
gw.retry_backoff_secs = lambda attempt: 0.05

g3 = bare_gateway()
notified = []


async def fake_notify(self, client, creds, item, attempts):
    notified.append((item.get("id"), attempts))


g3._notify_stuck_formal = fake_notify.__get__(g3)


async def always_fail(self, client, creds, item):
    return False


g3.dispatch_outbox_item = always_fail.__get__(g3)

rows = [
    {"id": "sendrow", "mode": "send", "to_user_id": "u1", "content": "通知"},
    {"id": "replyrow", "mode": "reply", "msgid": "M9", "content": "答复"},
]
gw.OUTBOX.write_text("".join(json.dumps(r) + "\n" for r in rows))
gw.OUTBOX_OFFSET.write_text("0")


async def run_loop():
    task = asyncio.create_task(g3.outbox_loop(None, {}))
    deadline = time.time() + 60
    while time.time() < deadline:
        await asyncio.sleep(0.5)
        retry = {}
        if gw.OUTBOX_RETRY.exists():
            retry = json.loads(gw.OUTBOX_RETRY.read_text())
        rec = retry.get("replyrow") or {}
        if int(rec.get("n") or 0) >= 7:
            break
    task.cancel()


asyncio.run(run_loop())
results = [json.loads(l) for l in gw.OUTBOX_RESULTS.read_text().splitlines()
           if l.strip()]
retry = json.loads(gw.OUTBOX_RETRY.read_text())
dl = [r for r in results if r.get("deadletter")]
check("send row dead-lettered at cap",
      any(r["id"] == "sendrow" for r in dl))
check("reply row NEVER dead-lettered",
      not any(r["id"] == "replyrow" for r in dl))
check("reply row kept retrying (>=7 attempts)",
      int((retry.get("replyrow") or {}).get("n") or 0) >= 7)
check("notify fired exactly once at attempt 5",
      notified == [("replyrow", 5)])
check("notified flag persisted",
      (retry.get("replyrow") or {}).get("notified") is True)
check("send row retry record cleaned",
      "sendrow" not in retry)

fails = [n for n, ok_ in RESULTS if not ok_]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
