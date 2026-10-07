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
gw.OUTBOX_PARKED = LOOP / "outbox_parked.json"
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

# ---------- 5. park-and-continue: a stuck head row no longer blocks ----------
LOOP2 = SBX / "loop2"
shutil.rmtree(LOOP2, ignore_errors=True)
LOOP2.mkdir(parents=True, exist_ok=True)
gw.OUTBOX = LOOP2 / "outbox.jsonl"
gw.OUTBOX_OFFSET = LOOP2 / "outbox.offset"
gw.OUTBOX_RESULTS = LOOP2 / "outbox_results.jsonl"
gw.OUTBOX_RETRY = LOOP2 / "outbox_retry.json"
gw.OUTBOX_PARTIAL = LOOP2 / "outbox_partial.json"
gw.OUTBOX_PARKED = LOOP2 / "outbox_parked.json"
gw.SEND_MAX_ATTEMPTS = 10
gw.FORMAL_STUCK_NOTIFY_ATTEMPTS = 3

g4 = bare_gateway()
g4._notify_stuck_formal = fake_notify.__get__(g4)
dispatched = []


async def fail_only_stuck(self, client, creds, item):
    dispatched.append(item.get("id"))
    return item.get("id") != "stuckreply"


g4.dispatch_outbox_item = fail_only_stuck.__get__(g4)
rows2 = [
    {"id": "stuckreply", "mode": "reply", "msgid": "M10", "content": "卡住的"},
    {"id": "laterow", "mode": "send", "to_user_id": "u1", "content": "后面的"},
    {"id": "thirdrow", "mode": "send", "to_user_id": "u1", "content": "第三条"},
]
gw.OUTBOX.write_text("".join(json.dumps(r) + "\n" for r in rows2))
gw.OUTBOX_OFFSET.write_text("0")


async def run_loop2():
    task = asyncio.create_task(g4.outbox_loop(None, {}))
    await asyncio.sleep(6)
    task.cancel()


asyncio.run(run_loop2())
off2 = int(gw.OUTBOX_OFFSET.read_text())
check("main queue fully consumed despite stuck head",
      off2 >= gw.OUTBOX.stat().st_size)
check("rows behind the stuck head were dispatched promptly",
      "laterow" in dispatched and "thirdrow" in dispatched)
parked2 = json.loads(gw.OUTBOX_PARKED.read_text())
check("stuck formal row is parked, not dropped",
      "stuckreply" in parked2
      and int(parked2["stuckreply"].get("n") or 0) >= 2
      and parked2["stuckreply"]["item"]["id"] == "stuckreply")
check("parked state survives a fresh instance (restart-proof)",
      "stuckreply" in bare_gateway()._load_parked())
retry2 = json.loads(gw.OUTBOX_RETRY.read_text())
check("retry mirror tracks the parked row",
      int((retry2.get("stuckreply") or {}).get("n") or 0) >= 2)
check("delivered rows leave no parked/retry residue",
      "laterow" not in parked2 and "laterow" not in retry2)

# ---------- 6. reply_file completes the batch + waiting-only position ----------
# (2026-10-07: usable parts adopted from the reviewed
# cursor/worker-liveness branch; spec: fixable-fixes-spec-2026-10-07.md)
HS = SBX / "hookstate"
ST = SBX / "gwstate"
shutil.rmtree(HS, ignore_errors=True)
shutil.rmtree(ST, ignore_errors=True)
HS.mkdir(parents=True)
ST.mkdir(parents=True)
NOW = time.time()
(HS / "active_batch.json").write_text(json.dumps(
    {"msgids": ["M1"], "since": NOW}))
(HS / "pending.json").write_text(json.dumps({"M1": 1, "M2": 1}))

# no reply row yet: batch in flight; the soft-ack position counts
# only the genuinely waiting M2 (old code said 3rd: pending total + 1)
(ST / "outbox.jsonl").write_text("")
check("batch in flight before any reply", gw.batch_in_flight(ST, HS) is True)
ack = gw.soft_ack_text(ST, HS, "你好")
check("soft ack counts only waiting (2nd, not 3rd)",
      ack is not None and "第 2 位" in ack)

# a reply_file row finishes the batch just like a text reply
(ST / "outbox.jsonl").write_text(json.dumps(
    {"id": "rf1", "mode": "reply_file", "msgid": "M1",
     "queued_at": NOW}) + "\n")
check("reply_file row ends the batch (not in flight)",
      gw.batch_in_flight(ST, HS) is False)
check("no soft ack after reply_file finished the batch",
      gw.soft_ack_text(ST, HS, "你好") is None)

# late suppression: a delivered reply_file suppresses a late update
DRB = SBX / "drb"
shutil.rmtree(DRB, ignore_errors=True)
DRB.mkdir(parents=True)
gw.OUTBOX = DRB / "outbox.jsonl"
gw.OUTBOX_RESULTS = DRB / "outbox_results.jsonl"
gw.OUTBOX.write_text(
    json.dumps({"id": "file1", "mode": "reply_file", "msgid": "M7"}) + "\n"
    + json.dumps({"id": "late1", "mode": "update", "msgid": "M7"}) + "\n")
gw.OUTBOX_RESULTS.write_text(json.dumps({"id": "file1", "ok": True}) + "\n")
check("late update suppressed after delivered reply_file",
      gw.Gateway._delivered_reply_before({"id": "late1", "msgid": "M7"}) is True)
gw.OUTBOX_RESULTS.write_text(json.dumps({"id": "file1", "ok": False}) + "\n")
check("failed reply_file does NOT suppress the update",
      gw.Gateway._delivered_reply_before({"id": "late1", "msgid": "M7"}) is False)

fails = [n for n, ok_ in RESULTS if not ok_]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
