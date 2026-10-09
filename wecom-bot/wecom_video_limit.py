"""Fit an outbound WeCom video under the 10MB upload cap.

WeCom rejects a video larger than 10 * 1024 * 1024 bytes with errcode
40011 ``invalid video size``. The original file is never modified.
A copy is transcoded until it fits. When every attempt stays over the
cap, or the input cannot be read, ``WeComVideoLimitError`` is raised
so the gateway can tell the user and skip the upload.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

# Hard cap observed live (DEF-wecom-video-10mb): 10,541,549 bytes was
# rejected. 10 * 1024 * 1024 is the largest size we will upload.
WECOM_VIDEO_MAX_BYTES = 10 * 1024 * 1024

# (max width px, CRF, audio bitrate). The first output at or under
# the cap is what we send. Later rungs are smaller.
TRANSCODE_RUNGS: tuple[tuple[int, int, str], ...] = (
    (1280, 26, "96k"),
    (854, 30, "64k"),
    (640, 36, "48k"),
    (480, 42, "32k"),
)

_FFMPEG_TIMEOUT_SECS = 180
# Leave headroom for the container so a bitrate-targeted pass lands
# under the cap instead of a few kilobytes over it.
_BITRATE_BUDGET_RATIO = 0.90


class WeComVideoLimitError(Exception):
    """A video cannot be delivered under the WeCom 10MB cap.

    ``errmsg`` is the operator-facing log line. ``user_text`` is the
    markdown sent in the chat.
    """

    def __init__(self, filename: str, nbytes: int) -> None:
        self.filename = filename
        self.nbytes = nbytes
        self.errmsg = (
            f"video exceeds WeCom 10MB limit "
            f"({filename}, {nbytes} bytes > {WECOM_VIDEO_MAX_BYTES}); "
            f"compression could not fit"
        )
        super().__init__(self.errmsg)

    @property
    def user_text(self) -> str:
        """Chat message: the video is over the WeCom 10MB limit."""
        return (
            f"⚠️ 视频「{self.filename}」超过企业微信 10MB 上限"
            f"（{self.nbytes} 字节 > {WECOM_VIDEO_MAX_BYTES} 字节）。"
            f"已尝试压缩，仍然超过 10MB，这次没有发出。"
            f"请把视频压到 10MB（{WECOM_VIDEO_MAX_BYTES} 字节）以内后再发。"
        )


def _log(msg: str) -> None:
    print(f"[wecom-video] {msg}", flush=True)


def _discard(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _cache_path(src: Path, compressed_dir: Path, tag: str) -> Path:
    """Stable cache name for one source file and one encode settings tag."""
    st = src.stat()
    key = hashlib.sha256(
        f"{src.resolve()}|{st.st_size}|{int(st.st_mtime)}|{tag}".encode()
    ).hexdigest()[:20]
    return compressed_dir / f"{key}.mp4"


def _cached_kind(path: Path) -> str:
    """``fit`` when a cached encode is usable, ``over`` when it is too
    big, ``miss`` when there is nothing to reuse."""
    if not path.exists():
        return "miss"
    size = path.stat().st_size
    if size <= 0:
        _discard(path)
        return "miss"
    if size <= WECOM_VIDEO_MAX_BYTES:
        return "fit"
    return "over"


def _run_ffmpeg(cmd: list[str], out: Path) -> str:
    """Run one ffmpeg command.

    Returns ``ok``, ``unreadable`` (the input cannot be opened; further
    rungs will fail the same way), or ``failed``.
    """
    try:
        result = subprocess.run(
            cmd, capture_output=True, timeout=_FFMPEG_TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        _log(f"ffmpeg timed out writing {out.name}")
        _discard(out)
        return "failed"
    except OSError as exc:
        _log(f"ffmpeg could not start: {exc}")
        _discard(out)
        return "failed"
    if result.returncode == 0 and out.exists() and out.stat().st_size > 0:
        return "ok"
    err = result.stderr.decode("utf-8", "replace")
    _discard(out)
    if "Error opening input" in err or "Invalid data found" in err:
        _log(f"ffmpeg cannot read input: {err[-200:]}")
        return "unreadable"
    _log(f"ffmpeg failed: {err[-200:]}")
    return "failed"


def _encode(
    src: Path,
    out: Path,
    max_width: int,
    video_flags: list[str],
    audio_bitrate: str,
) -> str:
    """Transcode ``src`` to ``out``. Tries to keep audio, then video only.

    Returns the same status strings as ``_run_ffmpeg``.
    """
    vf = f"scale='min({max_width},iw)':-2"
    common = [
        "ffmpeg", "-y", "-i", str(src),
        "-vf", vf,
        *video_flags,
        "-movflags", "+faststart",
    ]
    with_audio = _run_ffmpeg(
        common + ["-c:a", "aac", "-b:a", audio_bitrate, str(out)],
        out,
    )
    if with_audio in ("ok", "unreadable"):
        return with_audio
    return _run_ffmpeg(common + ["-an", str(out)], out)


def _accept(src: Path, out: Path, before: int, label: str) -> Path | None:
    """Return ``out`` when the encode landed on disk at or under the cap."""
    if not out.exists():
        return None
    produced = out.stat().st_size
    _log(f"transcoded {src.name} {label}: {before} -> {produced} bytes")
    if 0 < produced <= WECOM_VIDEO_MAX_BYTES:
        return out
    return None


def _duration_secs(src: Path) -> float | None:
    """Container duration in seconds, or None when ffprobe cannot say."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(src),
            ],
            capture_output=True,
            timeout=30,
            text=True,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        duration = float((result.stdout or "").strip())
    except ValueError:
        return None
    if duration <= 0:
        return None
    return duration


def _bitrate_fit(src: Path, compressed_dir: Path, size: int) -> Path | None:
    """One encode aimed at about 90% of the cap, using the real duration."""
    duration = _duration_secs(src)
    if duration is None:
        return None
    budget_bits = int(WECOM_VIDEO_MAX_BYTES * 8 * _BITRATE_BUDGET_RATIO)
    audio_bps = 32_000
    video_bps = int(budget_bits / duration) - audio_bps
    if video_bps < 40_000:
        video_bps = 40_000
    width = 640 if video_bps >= 150_000 else 480
    tag = f"br-{video_bps}-w{width}"
    out = _cache_path(src, compressed_dir, tag)
    cached = _cached_kind(out)
    if cached == "fit":
        return out
    if cached == "over":
        return None
    status = _encode(
        src,
        out,
        width,
        [
            "-c:v", "libx264",
            "-b:v", str(video_bps),
            "-maxrate", str(video_bps),
            "-bufsize", str(video_bps),
            "-preset", "veryfast",
        ],
        "32k",
    )
    if status != "ok":
        return None
    return _accept(src, out, size, tag)


def fit_wecom_video(src: Path, compressed_dir: Path) -> Path:
    """Return a path whose bytes are at or under ``WECOM_VIDEO_MAX_BYTES``.

    Files that already fit are returned unchanged. Larger files are
    transcoded into ``compressed_dir``. Raises ``WeComVideoLimitError``
    when the result would still exceed the WeCom 10MB limit. The
    source file is not modified.
    """
    src = Path(src)
    try:
        size = src.stat().st_size
    except OSError as exc:
        raise WeComVideoLimitError(Path(src).name, 0) from exc
    if size <= WECOM_VIDEO_MAX_BYTES:
        return src
    compressed_dir = Path(compressed_dir)
    try:
        compressed_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _log(f"cannot create {compressed_dir}: {exc}")
        raise WeComVideoLimitError(src.name, size) from exc
    for level, (width, crf, audio) in enumerate(TRANSCODE_RUNGS):
        tag = f"L{level}-w{width}-crf{crf}-a{audio}"
        out = _cache_path(src, compressed_dir, tag)
        cached = _cached_kind(out)
        if cached == "fit":
            _log(f"reuse {src.name} {tag}: {out.stat().st_size} bytes")
            return out
        if cached == "over":
            continue
        status = _encode(
            src,
            out,
            width,
            ["-c:v", "libx264", "-crf", str(crf), "-preset", "veryfast"],
            audio,
        )
        if status == "unreadable":
            raise WeComVideoLimitError(src.name, size)
        if status != "ok":
            continue
        fitted = _accept(src, out, size, tag)
        if fitted is not None:
            return fitted
    fitted = _bitrate_fit(src, compressed_dir, size)
    if fitted is not None:
        return fitted
    raise WeComVideoLimitError(src.name, size)
