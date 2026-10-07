"""Shared guards for the Weixin and WeCom gateways.

The two processes do not import each other. These helpers are the
pieces both sides must get right: which home directory owns hooks,
how a failed send stays in the outbox, and how queue-admin commands
combine when two of them land before the hook reads the file.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

HATCH_HOME = Path("/home/hatch")
SEND_MAX_ATTEMPTS = 10
REPLY_INFLIGHT_WINDOW_S = 120
QUEUE_ADMIN_FRESH_SECS = 15


def muse_home() -> Path:
    """Directory that owns hooks and config.

    systemd system units set HOME=/root. The install lives under
    /home/hatch, so an explicit MUSE_HOME or that directory wins over
    the process home.
    """
    explicit = os.environ.get("MUSE_HOME", "").strip()
    if explicit:
        return Path(explicit)
    if HATCH_HOME.is_dir():
        return HATCH_HOME
    home = os.environ.get("HOME", "").strip()
    if home and home != "/root":
        return Path(home)
    return HATCH_HOME


def retry_backoff_secs(attempt: int) -> float:
    """Seconds to wait before attempt n (1-based), capped at 300."""
    return min(20.0 * (2 ** max(0, attempt - 1)), 300.0)


def atomic_write_text(path: Path, text: str, mode: int = 0o644) -> None:
    """Replace path with text. A crash cannot leave a truncated target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def read_offset(path: Path) -> int:
    """Byte offset, or 0 when the file is missing or unreadable."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return 0
        return max(0, int(raw))
    except (OSError, ValueError):
        return 0


def write_offset(path: Path, offset: int) -> None:
    """Persist a byte offset with a temp-file replace."""
    atomic_write_text(path, str(int(offset)))


def load_json_dict(path: Path) -> dict:
    """JSON object at path, or {} when missing or corrupt."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return {}
    return data if isinstance(data, dict) else {}


def append_jsonl(path: Path, obj: dict) -> None:
    """Append one JSON line under an exclusive lock and fsync it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def complete_jsonl_lines(data: bytes) -> list[bytes]:
    """Complete lines from a read. A trailing partial line is kept back.

    The caller waits for the next append to finish that line instead of
    skipping it or treating it as a permanent bad record.
    """
    if not data:
        return []
    incomplete = not data.endswith(b"\n")
    parts = data.split(b"\n")
    if incomplete:
        parts = parts[:-1]
    elif parts and parts[-1] == b"":
        # split() keeps an empty segment after the final newline.
        # Counting that segment would push the cursor past the file.
        parts = parts[:-1]
    return parts


def parse_jsonl_line(raw: bytes) -> dict | None:
    """Parse one complete line.

    Blank lines return None. A complete line that is not a JSON object
    returns {"__invalid__": True} so the caller can skip it on purpose.
    """
    if not raw.strip():
        return None
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"__invalid__": True}
    if not isinstance(obj, dict):
        return {"__invalid__": True}
    return obj


def reply_block_reason_for_row(
    msgid: str,
    row: dict,
    result: dict | None,
    now: float,
    retry: dict | None = None,
    window: float = REPLY_INFLIGHT_WINDOW_S,
) -> str | None:
    """Why a second formal reply must not be queued, or None to allow it.

    A transient ok=false result means the gateway is still retrying that
    row. Only a dead-letter, or a retry record that already hit the
    attempt cap, opens the gate for another reply.
    """
    if row.get("mode") != "reply" or str(row.get("msgid")) != str(msgid):
        return None
    row_id = str(row.get("id") or "")
    rec = (retry or {}).get(row_id) if row_id else None
    attempts = int((rec or {}).get("n") or 0)
    if result is None:
        if attempts and attempts < SEND_MAX_ATTEMPTS:
            return (
                f"msgid {msgid} already has a formal reply still retrying "
                f"(outbox id {row_id})"
            )
        age = now - float(row.get("queued_at") or now)
        if age < window:
            return (
                f"msgid {msgid} already has a formal reply queued "
                f"{int(age)}s ago with no delivery result yet "
                f"(likely still in flight, outbox id {row_id})"
            )
        return None
    if result.get("ok") is True:
        return (
            f"msgid {msgid} already has a formal reply that was "
            f"delivered successfully (outbox id {row_id})"
        )
    if result.get("deadletter") is True or attempts >= SEND_MAX_ATTEMPTS:
        return None
    if attempts and attempts < SEND_MAX_ATTEMPTS:
        return (
            f"msgid {msgid} already has a formal reply still retrying "
            f"after a transient failure (outbox id {row_id})"
        )
    # A failed result with no live retry record is in flight only while it
    # is recent. Older ok=false rows are finished history (the previous
    # WeCom gateway consumed those failures) and must not block a new reply.
    stamp = result.get("ts")
    if not isinstance(stamp, (int, float)):
        stamp = row.get("queued_at") or now
    if now - float(stamp) < window:
        return (
            f"msgid {msgid} already has a formal reply still retrying "
            f"after a transient failure (outbox id {row_id})"
        )
    return None


def merge_queue_admin(
    existing: dict | None,
    action: str,
    msgids: list[str],
    now: float,
    fresh_secs: float = QUEUE_ADMIN_FRESH_SECS,
) -> dict:
    """Combine a new clear/drop with an unconsumed queue_admin object.

    The hook reads one JSON object. Two commands a few seconds apart
    used to overwrite each other. A clear covers every listed msgid, so
    it absorbs an unconsumed drop. A file older than fresh_secs is left
    behind: the hook has had time to apply it.
    """
    prev_ids: list[str] = []
    prev_action = ""
    if isinstance(existing, dict):
        ts = existing.get("ts")
        fresh = isinstance(ts, (int, float)) and (now - float(ts)) <= fresh_secs
        if fresh:
            prev_action = str(existing.get("action") or "")
            raw_ids = existing.get("msgids") or []
            if isinstance(raw_ids, list):
                prev_ids = [str(mid) for mid in raw_ids if str(mid)]
    new_ids = [str(mid) for mid in msgids if str(mid)]
    merged_action = "clear" if action == "clear" or prev_action == "clear" else "drop"
    seen: list[str] = []
    for mid in prev_ids + new_ids:
        if mid not in seen:
            seen.append(mid)
    return {"ts": now, "action": merged_action, "msgids": seen}


def parse_feedback_clear_line(line: str) -> str:
    """Msgid from a clear-file line.

    The hook may write a bare msgid or a JSON object with a msgid field.
    A JSON line without that field is ignored.
    """
    text = (line or "").strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(obj, dict) and obj.get("msgid"):
            return str(obj["msgid"])
        return ""
    return text


def safe_child_name(name: str) -> str | None:
    """Single path segment, or None when name would escape a directory."""
    if not name or name in (".", ".."):
        return None
    if "/" in name or "\\" in name or ".." in name:
        return None
    if name != Path(name).name:
        return None
    return name


def safe_child_path(directory: Path, name: str) -> Path | None:
    """directory/name after resolving, or None when it leaves directory."""
    cleaned = safe_child_name(name)
    if cleaned is None:
        return None
    root = directory.resolve()
    path = (root / cleaned).resolve()
    if path.parent != root:
        return None
    return path


def media_filename(msgid: str, index: int, ext: str) -> str:
    """Local media name that cannot contain a path separator."""
    cleaned = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(msgid))[:80]
    ext_clean = "".join(c if c.isalnum() else "" for c in str(ext))[:8]
    return f"{cleaned or 'msg'}-{index}.{ext_clean or 'bin'}"


def media_url_allowed(url: str) -> bool:
    """True when url points at a Tencent host used for WeChat media."""
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    if not host:
        return False
    suffixes = (".weixin.qq.com", ".qq.com")
    return host in ("weixin.qq.com", "qq.com") or host.endswith(suffixes)


def outbound_file_allowed(path: Path, cred_file: Path, roots: list[Path]) -> bool:
    """True when path is an existing file under roots and not a credential.

    Credential directories and .ssh are refused even when they sit under
    an allowed root such as the install home.
    """
    try:
        resolved = path.expanduser().resolve(strict=False)
    except OSError:
        return False
    if not resolved.is_file():
        return False
    if ".ssh" in resolved.parts:
        return False
    parts = resolved.parts
    for index, part in enumerate(parts[:-1]):
        if part == ".config" and parts[index + 1] in ("weixin-bot", "wecom-bot"):
            return False
    try:
        cred = cred_file.expanduser().resolve(strict=False)
    except OSError:
        cred = None
    if cred is not None and (resolved == cred or cred.parent == resolved or cred.parent in resolved.parents):
        return False
    for root in roots:
        try:
            base = root.expanduser().resolve(strict=False)
        except OSError:
            continue
        if resolved == base or base in resolved.parents:
            return True
    return False


def subagent_outcome_default(content: str, status: str | None) -> str:
    """Outcome word when the reply has no 【副助手】 label.

    Text that already says 失败 or 已停止, and a job record in those
    states, must not be rewritten as 完成. A still-running job with no
    failure word keeps 完成, which is the label workers forget on a
    normal answer.
    """
    sample = (content or "")[:200]
    if "已停止" in sample:
        return "已停止"
    if "失败" in sample:
        return "失败"
    if status == "failed":
        return "失败"
    if status == "stopped":
        return "已停止"
    if status == "done":
        return "完成"
    return "完成"


def allocate_subagent_id(state_dir: Path) -> str | None:
    """Next S<n> id, or None when the sequence file cannot be replaced.

    Returning the same id after a failed replace would collide two jobs.
    """
    path = state_dir / "subagent_seq.json"
    data = load_json_dict(path)
    current = data.get("next")
    number = current if isinstance(current, int) and current >= 1 else 1
    try:
        atomic_write_text(path, json.dumps({"next": number + 1}))
    except OSError:
        return None
    return f"S{number}"


def register_cancel(path: Path, msgid: str) -> None:
    """Append msgid to cancelled.json under a lock, via a temp-file replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name("cancelled.lock")
    with lock_path.open("a+") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX)
        try:
            rows: list = []
            if path.exists():
                try:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(loaded, list):
                        rows = loaded
                except (OSError, json.JSONDecodeError, UnicodeError):
                    rows = []
            if not any(isinstance(row, dict) and row.get("msgid") == msgid for row in rows):
                rows.append({"msgid": msgid, "ts": time.time()})
                atomic_write_text(path, json.dumps(rows, ensure_ascii=False))
        finally:
            fcntl.flock(lock_handle, fcntl.LOCK_UN)


def trim_mapping(items: list[tuple[str, object]], cap: int, pinned: set[str]) -> dict:
    """Keep the newest cap entries, always retaining pinned keys."""
    if len(items) <= cap:
        return dict(items)
    drop = len(items) - cap
    kept: list[tuple[str, object]] = []
    for key, value in items:
        if drop > 0 and key not in pinned:
            drop -= 1
            continue
        kept.append((key, value))
    return dict(kept)


def pid_alive(pid: int) -> bool:
    """True when pid is a live process. pid <= 0 is never alive."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def waiting_ahead(pending_ids, in_service_ids) -> int:
    """How many queued messages are actually waiting, not in service.

    The in-flight batch is being handled. Counting it makes a brand-new
    message look like it is already 2nd in line.
    """
    serving = {str(item) for item in in_service_ids}
    return sum(1 for item in pending_ids if str(item) not in serving)


def batch_workers_dead(heartbeat_dir: Path, msgids) -> bool:
    """True when every recorded worker for these messages has exited.

    No worker file means the batch has not reported a process yet, so
    this returns False and the silence timer still applies.
    """
    saw = False
    for msgid in msgids:
        path = heartbeat_dir / f"{msgid}.worker"
        try:
            pid = int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            continue
        saw = True
        if not recorded_worker_is_dead(pid):
            return False
    return saw


def recorded_worker_is_dead(pid: int) -> bool:
    """True when a recorded worker pid has exited.

    pid <= 1 is not a worker handle (missing record, or init). Those
    stay on the silence-timer path so older heartbeats without a
    worker pid are not failed on the first poll.
    """
    if pid <= 1:
        return False
    return not pid_alive(pid)


def read_pid_file(path: Path) -> int | None:
    """Integer pid stored in path, or None."""
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def chunk_slices(chunks: list[str], start: int) -> list[tuple[int, str]]:
    """(index, text) pairs at and after start."""
    return [(index, part) for index, part in enumerate(chunks) if index >= start]
