#!/usr/bin/env python3
"""WeCom outbound video must fit in 10 * 1024 * 1024 bytes.

DEF-wecom-video-10mb: a 10,541,549-byte video was uploaded and rejected
with errcode 40011 invalid video size. An oversize video is transcoded
under that cap before upload. When compression cannot get there, the
row fails with a clear logged error and a user-visible notice, and the
original is not uploaded. A file already at or under the cap is sent
unchanged. A non-video over the cap is unchanged (that cap is video-only).

State paths point at a sandbox. Network sends are stubbed.
"""
import asyncio
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/wecom-video-10mb-sbx")
MSGID = "8b790ee4-video-10mb"

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_gateway():
    spec = importlib.util.spec_from_file_location(
        "wc_video_10mb", ROOT / "wecom-bot" / "gateway.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def sandbox(mod, directory: Path) -> None:
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
    mod.COMPRESSED_DIR = directory / "compressed"
    mod.COMPRESSED_DIR.mkdir()


def write_outbox(mod, rows) -> None:
    mod.OUTBOX.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def result_for(mod, item_id):
    found = None
    if not mod.OUTBOX_RESULTS.exists():
        return None
    for line in mod.OUTBOX_RESULTS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("id") == item_id:
            found = row
    return found


def make_gateway(mod):
    gateway = mod.Gateway.__new__(mod.Gateway)
    gateway.reqmap = {
        MSGID: {
            "req_id": "req-video",
            "stream_id": "stream-video",
            "chatid": "chat-video",
            "chattype": "single",
        }
    }
    gateway.msgs_sent = 0
    gateway.open_streams = {MSGID: {"stream_id": "stream-video"}}
    gateway.feedback_track = {MSGID: {"chatid": "chat-video"}}
    gateway.write_status = lambda: None
    uploaded = []
    responded = []
    frames = []

    async def fake_upload(self, data, mtype, name):
        uploaded.append({
            "size": len(data),
            "mtype": mtype,
            "name": name,
            "md5": hashlib.md5(data).hexdigest(),
        })
        return "media-video"

    async def fake_respond(self, req_id, body, timeout=10.0):
        responded.append({"req_id": req_id, "body": body})
        return {"errcode": 0, "errmsg": ""}

    async def fake_send(self, frame, wait_response=False, timeout=10.0):
        frames.append(frame)
        return {"errcode": 0, "errmsg": ""}

    gateway.upload_media = fake_upload.__get__(gateway)
    gateway.respond = fake_respond.__get__(gateway)
    gateway.send_frame = fake_send.__get__(gateway)
    return gateway, uploaded, responded, frames


def md5_file(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def generate_oversize_video(dest: Path) -> None:
    """A short noisy clip just over the cap. The first transcode rung
    brings it back under. Generation is about a tenth of a second."""
    proc = subprocess.run(
        [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi",
            "-i", "color=c=black:s=960x540:r=15,noise=alls=100:allf=t",
            "-t", "1",
            "-c:v", "libx264", "-preset", "ultrafast", "-qp", "12",
            "-an", str(dest),
        ],
        capture_output=True,
        timeout=60,
    )
    if proc.returncode != 0 or not dest.exists():
        err = proc.stderr.decode("utf-8", "replace")[-400:]
        raise RuntimeError(f"fixture video failed: {err}")


wc = load_gateway()
sandbox(wc, SBX)
lim = sys.modules["wecom_video_limit"]
CAP = wc.WECOM_VIDEO_MAX_BYTES

logs = []
_orig_log = wc.log


def capture_log(msg):
    logs.append(msg)
    _orig_log(msg)


wc.log = capture_log

# ---------- files already at or under the cap ----------
small = SBX / "small.mp4"
small.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64)
exact = SBX / "exact.mp4"
exact.write_bytes(b"\x00" * CAP)
note = SBX / "notes.zip"
note.write_bytes(b"PK\x03\x04" + b"z" * (CAP + 100))

ran_ffmpeg = {"n": 0}
_orig_run = lim.subprocess.run


def counting_run(*args, **kwargs):
    ran_ffmpeg["n"] += 1
    return _orig_run(*args, **kwargs)


lim.subprocess.run = counting_run
try:
    same = lim.fit_wecom_video(exact, wc.COMPRESSED_DIR)
finally:
    lim.subprocess.run = _orig_run

check("cap constant is 10 * 1024 * 1024", CAP == 10 * 1024 * 1024)
check("file exactly at the cap is not transcoded",
      same == exact and ran_ffmpeg["n"] == 0)
check("exact-cap original untouched", exact.stat().st_size == CAP)

# ---------- oversize video that ffmpeg cannot read ----------
bad = SBX / "bad.mp4"
bad.write_bytes(b"\x00" * (CAP + 1))
bad_err = None
try:
    lim.fit_wecom_video(bad, wc.COMPRESSED_DIR)
except wc.WeComVideoLimitError as exc:
    bad_err = exc

check("unreadable oversize video raises", bad_err is not None)
check("limit error names the 10MB cap and the real size",
      bad_err is not None
      and "exceeds WeCom 10MB limit" in bad_err.errmsg
      and str(CAP + 1) in bad_err.errmsg
      and str(CAP) in bad_err.errmsg
      and "compression could not fit" in bad_err.errmsg)
check("user text says the video exceeds 10MB",
      bad_err is not None
      and "超过企业微信 10MB 上限" in bad_err.user_text
      and bad.name in bad_err.user_text
      and "这次没有发出" in bad_err.user_text)

# ---------- real oversize video compresses under the cap ----------
clip = SBX / "noise.mp4"
generate_oversize_video(clip)
clip_md5 = md5_file(clip)
clip_size = clip.stat().st_size
check("fixture video is over the cap", clip_size > CAP)
fitted = lim.fit_wecom_video(clip, wc.COMPRESSED_DIR)
fitted_size = fitted.stat().st_size
check("compressed video is at or under the cap",
      fitted != clip and 0 < fitted_size <= CAP)
check("compressed video is a different file",
      fitted.resolve() != clip.resolve())
check("source video bytes unchanged", md5_file(clip) == clip_md5)
reused = lim.fit_wecom_video(clip, wc.COMPRESSED_DIR)
check("second fit reuses the cached encode", reused == fitted)

# ---------- dispatch: under-limit video and a non-video still send ----------
g, uploaded, responded, frames = make_gateway(wc)


async def dispatch_ok():
    video_ok = await g.dispatch_outbox_item({
        "id": "small-video",
        "mode": "reply_file",
        "msgid": MSGID,
        "file_path": str(small),
    })
    zip_ok = await g.dispatch_outbox_item({
        "id": "big-zip",
        "mode": "send_file",
        "chatid": "chat-video",
        "chat_type": 1,
        "file_path": str(note),
    })
    exact_ok = await g.dispatch_outbox_item({
        "id": "exact-video",
        "mode": "send_file",
        "chatid": "chat-video",
        "chat_type": 1,
        "file_path": str(exact),
    })
    return video_ok, zip_ok, exact_ok


video_ok, zip_ok, exact_ok = asyncio.run(dispatch_ok())
small_res = result_for(wc, "small-video")
zip_res = result_for(wc, "big-zip")
exact_res = result_for(wc, "exact-video")

check("under-limit reply_file sends",
      video_ok is True and small_res is not None and small_res.get("ok") is True)
check("under-limit reply_file uploads the original bytes",
      uploaded[:1] == [{
          "size": small.stat().st_size,
          "mtype": "video",
          "name": small.name,
          "md5": md5_file(small),
      }])
check("under-limit reply_file is the video, not a limit notice",
      responded[-1]["body"].get("msgtype") == "video")
check("non-video over 10MB still uploads unchanged",
      zip_ok is True
      and zip_res is not None
      and zip_res.get("ok") is True
      and zip_res.get("file_bytes") == note.stat().st_size
      and uploaded[1]["size"] == note.stat().st_size
      and uploaded[1]["mtype"] == "file"
      and uploaded[1]["md5"] == md5_file(note))
check("video exactly at the cap still uploads",
      exact_ok is True
      and exact_res is not None
      and exact_res.get("ok") is True
      and exact_res.get("file_bytes") == CAP
      and uploaded[2]["size"] == CAP
      and uploaded[2]["name"] == exact.name)

# ---------- dispatch: compressible oversize video is what gets uploaded ----------
g2, uploaded2, responded2, frames2 = make_gateway(wc)


async def dispatch_clip():
    return await g2.dispatch_outbox_item({
        "id": "noise-video",
        "mode": "reply_file",
        "msgid": MSGID,
        "file_path": str(clip),
    })


clip_ok = asyncio.run(dispatch_clip())
clip_res = result_for(wc, "noise-video")
check("oversize video reply_file ok after compression",
      clip_ok is True and clip_res is not None and clip_res.get("ok") is True)
check("uploaded video is at or under 10MB and is not the original",
      len(uploaded2) == 1
      and uploaded2[0]["mtype"] == "video"
      and uploaded2[0]["size"] <= CAP
      and uploaded2[0]["size"] == clip_res.get("file_bytes")
      and uploaded2[0]["md5"] != clip_md5
      and uploaded2[0]["name"] == f"{clip.stem}.mp4")
check("compressed send is logged",
      any("wecom video compressed for send" in msg and clip.name in msg
          for msg in logs))
check("source still unchanged after dispatch", md5_file(clip) == clip_md5)
check("successful compressed reply_file does not also send the limit notice",
      responded2[-1]["body"].get("msgtype") == "video"
      and "10MB" not in json.dumps(responded2[-1]["body"], ensure_ascii=False))

# ---------- dispatch: cannot compress -> clear error, no upload ----------
write_outbox(wc, [
    {"id": "bad-file", "mode": "reply_file", "msgid": MSGID,
     "file_path": str(bad)},
    {"id": "bad-update", "mode": "update", "msgid": MSGID,
     "content": "还在做"},
])
g3, uploaded3, responded3, frames3 = make_gateway(wc)
before_logs = len(logs)


async def dispatch_bad():
    file_done = await g3.dispatch_outbox_item({
        "id": "bad-file",
        "mode": "reply_file",
        "msgid": MSGID,
        "file_path": str(bad),
    })
    update_done = await g3.dispatch_outbox_item({
        "id": "bad-update",
        "mode": "update",
        "msgid": MSGID,
        "content": "还在做",
    })
    return file_done, update_done


file_done, update_done = asyncio.run(dispatch_bad())
bad_res = result_for(wc, "bad-file")
update_res = result_for(wc, "bad-update")
notice_bodies = [
    row["body"] for row in responded3
    if row["body"].get("msgtype") == "markdown"
]
check("oversize unreadable video is a terminal failure (not retried)",
      file_done is True
      and bad_res is not None
      and bad_res.get("ok") is False)
check("logged error says the video exceeds the WeCom 10MB limit",
      bad_res is not None
      and "exceeds WeCom 10MB limit" in (bad_res.get("errmsg") or "")
      and "compression could not fit" in (bad_res.get("errmsg") or "")
      and str(bad.stat().st_size) in (bad_res.get("errmsg") or "")
      and any("exceeds WeCom 10MB limit" in msg for msg in logs[before_logs:]))
check("user-visible notice names 10MB and the file",
      len(notice_bodies) == 1
      and "超过企业微信 10MB 上限" in notice_bodies[0]["markdown"]["content"]
      and bad.name in notice_bodies[0]["markdown"]["content"]
      and "这次没有发出" in notice_bodies[0]["markdown"]["content"])
check("oversize video was not uploaded", uploaded3 == [])
check("failed reply_file does not suppress a later progress update",
      update_done is True
      and update_res is not None
      and update_res.get("ok") is True
      and update_res.get("suppressed") is not True
      and any(
          row["body"].get("msgtype") == "stream"
          and row["body"]["stream"].get("finish") is False
          for row in responded3
      ))

# ---------- send_file: same clear failure, proactive notice ----------
g4, uploaded4, responded4, frames4 = make_gateway(wc)


async def dispatch_send_bad():
    return await g4.dispatch_outbox_item({
        "id": "bad-send",
        "mode": "send_file",
        "chatid": "chat-video",
        "chat_type": 1,
        "file_path": str(bad),
    })


send_done = asyncio.run(dispatch_send_bad())
send_res = result_for(wc, "bad-send")
send_notices = []
for frame in frames4:
    body = (frame.get("body") or {})
    if body.get("msgtype") == "markdown":
        send_notices.append(body["markdown"]["content"])
check("send_file over the cap fails clearly and is not uploaded",
      send_done is True
      and send_res is not None
      and send_res.get("ok") is False
      and "exceeds WeCom 10MB limit" in (send_res.get("errmsg") or "")
      and uploaded4 == [])
check("send_file limit notice is proactive markdown",
      len(send_notices) == 1
      and "超过企业微信 10MB 上限" in send_notices[0]
      and bad.name in send_notices[0]
      and frames4[0]["body"].get("chatid") == "chat-video")

fails = [name for name, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
