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
from contextlib import contextmanager
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


def formal_modes_blocking(enqueue_mode: str) -> tuple[str, ...]:
    """Outbox modes that block one new row of ``enqueue_mode``.

    A text reply is blocked only by another text reply. A reply_file
    is blocked only by another reply_file. Progress rows (``update``
    and ``reply_notice``) are blocked once any formal row exists.
    Callers that do not name a mode are checking a text reply.
    """
    if enqueue_mode in ("update", "reply_notice"):
        return FORMAL_REPLY_MODES
    if enqueue_mode == "reply_file":
        return ("reply_file",)
    return ("reply",)


def same_mode_delivery_blocks(candidate: dict, other: dict) -> bool:
    """Whether a delivered ``other`` row suppresses ``candidate``.

    A delivered reply suppresses other reply rows. A delivered
    reply_file suppresses other reply_file rows only when the two
    rows are not distinct files: different non-empty paths are
    separate media in one answer and both still go out. Progress
    rows, including a row that has no mode, are suppressed once any
    formal row for the msgid was delivered. Cross-mode delivery does
    not suppress a reply or a reply_file.
    """
    if not isinstance(candidate, dict) or not isinstance(other, dict):
        return False
    candidate_mode = str(candidate.get("mode") or "")
    other_mode = str(other.get("mode") or "")
    if candidate_mode == "reply":
        return other_mode == "reply"
    if candidate_mode == "reply_file":
        if other_mode != "reply_file":
            return False
        left = str(candidate.get("file_path") or "")
        right = str(other.get("file_path") or "")
        if left and right and left != right:
            return False
        return True
    return other_mode in ("reply", "reply_file")


def reply_block_reason_for_row(
    msgid: str,
    row: dict,
    result: dict | None,
    now: float,
    retry: dict | None = None,
    window: float = REPLY_INFLIGHT_WINDOW_S,
    enqueue_mode: str = "reply",
) -> str | None:
    """Why this existing row blocks a new enqueue, or None to allow it.

    ``enqueue_mode`` selects the same-mode rule: a text reply looks
    only at reply rows, a reply_file looks only at reply_file rows,
    and a progress enqueue looks at both. A transient ok=false result
    means the gateway is still retrying that row. Only a dead-letter,
    or a retry record that already hit the attempt cap, opens the gate
    for another row of the same mode.
    """
    blocking = formal_modes_blocking(enqueue_mode)
    if row.get("mode") not in blocking or str(row.get("msgid")) != str(msgid):
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

    The hook reads one JSON object and deletes the file after applying
    it. Two commands that land before that read used to overwrite each
    other. A clear covers every listed msgid, so it absorbs an
    unconsumed drop. The file's presence is the consumed signal:
    ``fresh_secs`` does not retire an object the hook has not deleted,
    because a slow poll is older than 15s and still has not applied it.
    """
    del fresh_secs  # retained for callers; age is not consumption
    prev_ids: list[str] = []
    prev_action = ""
    if isinstance(existing, dict):
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
    if host in ("weixin.qq.com", "qq.com") or host.endswith(suffixes):
        return True
    # WeCom aibot media is served from the platform's own COS buckets
    # (production shape: ww-aibot-img-<appid>.cos.<region>.myqcloud.com).
    # Only the aibot bucket family is allowed, not arbitrary COS buckets.
    first_label = host.split(".", 1)[0]
    return (first_label.startswith("ww-aibot-")
            and ".cos." in host and host.endswith(".myqcloud.com"))


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


@contextmanager
def exclusive_file_lock(lock_path: Path):
    """Hold an exclusive advisory lock on ``lock_path`` for the block.

    The lock file is created if needed. It serializes cross-process
    read-modify-write of a sibling state file.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield handle
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def allocate_subagent_id(state_dir: Path) -> str | None:
    """Next S<n> id, or None when the sequence file cannot be updated.

    The read and the replace share one file lock. A missing file starts
    at S1. A file that exists but cannot be parsed is an error: treating
    that as an empty map would mint S1 again and collide with a live job.
    Returning the same id after a failed replace would collide two jobs.
    """
    path = state_dir / "subagent_seq.json"
    try:
        with exclusive_file_lock(state_dir / "subagent_seq.lock"):
            if path.exists():
                try:
                    loaded = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError, UnicodeError):
                    return None
                if not isinstance(loaded, dict):
                    return None
                data = loaded
            else:
                data = {}
            current = data.get("next")
            number = current if isinstance(current, int) and current >= 1 else 1
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


def _merge_worker_activity(path: Path, msgids: list[str], now: float,
                             source: str, run_id: str | None = None,
                             progress: str | None = None,
                             hold_secs: float | None = None) -> dict:
    """Read-merge-write worker_activity.json under a lock.

    The file is the worker's own activity ledger for the death watch
    (see death_watch_activity.py, source 2/3): ``{msgid: {"ts":
    epoch, "source": str, "run_id": str|None, "progress": str|None,
    "hold_until": epoch|None}}``. Records written by older writers
    (a bare epoch, or a dict without run_id) are preserved as-is
    apart from the fields this call sets. A corrupt existing file
    is replaced by a fresh map: the ledger is a liveness hint, not
    history, and self-healing beats wedging every later write.
    Returns the records written, keyed by msgid.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name("worker_activity.lock")
    written: dict = {}
    with lock_path.open("a+") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX)
        try:
            data = load_json_dict(path)
            for msgid in msgids:
                mid = str(msgid)
                raw = data.get(mid)
                rec = dict(raw) if isinstance(raw, dict) else {}
                rec["ts"] = now
                rec["source"] = source
                if run_id:
                    rec["run_id"] = run_id
                if progress is not None:
                    rec["progress"] = progress
                if hold_secs is not None:
                    if hold_secs > 0:
                        rec["hold_until"] = now + float(hold_secs)
                    else:
                        rec.pop("hold_until", None)
                data[mid] = rec
                written[mid] = rec
            atomic_write_text(path, json.dumps(data, ensure_ascii=False))
        finally:
            fcntl.flock(lock_handle, fcntl.LOCK_UN)
    return written


def register_worker_activity(path: Path, msgids: list[str],
                             run_id: str, now: float) -> dict:
    """Batch-start self-registration (todo 21, route C mapping a).

    The worker copies the run_id from its own first message header;
    that run_id is the key the platform-check runbook resolves
    against agent.agents spawn metadata.
    """
    return _merge_worker_activity(
        path, msgids, now, source="worker-register", run_id=run_id)


def update_worker_activity(path: Path, msgid: str, now: float,
                           progress: str | None = None,
                           hold_secs: float | None = None) -> dict:
    """One beat (optionally with progress text) or one hold write.

    hold_secs > 0 declares "I will be silent this long on purpose";
    hold_secs == 0 clears an earlier declaration.
    """
    source = "worker-hold" if hold_secs is not None else "worker-beat"
    return _merge_worker_activity(
        path, [msgid], now, source=source, progress=progress,
        hold_secs=hold_secs)


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


def read_pid_file(path: Path) -> int | None:
    """Integer pid stored in path, or None."""
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def waiting_ahead(pending_ids, in_service_ids) -> int:
    """How many queued messages are actually waiting, not in service.

    The in-flight batch is being handled. Counting it makes a brand-new
    message look like it is already 2nd in line. (Adopted 2026-10-07
    from the reviewed cursor/worker-liveness branch; that branch's
    worker-pid liveness parts were rejected, this pure counter was
    not.)
    """
    serving = {str(item) for item in in_service_ids}
    return sum(1 for item in pending_ids if str(item) not in serving)


def chunk_slices(chunks: list[str], start: int) -> list[tuple[int, str]]:
    """(index, text) pairs at and after start."""
    return [(index, part) for index, part in enumerate(chunks) if index >= start]


# --- pending answers (one ledger for every unanswered path) -----------
# covered-drop, fail-stop, and bridge fallback used to leave three
# different traces. The sweep could only see covered_drops.jsonl.
# All three now record here, under the same lock and atomic replace
# as worker_activity.json. covered_drops.jsonl is unchanged.

PENDING_ANSWER_SOURCES = ("covered-drop", "fail-stop", "bridge-fallback")
PENDING_SUMMARY_MAX = 80
PENDING_SUMMARY_EMPTY = "（无文字消息）"
PENDING_ANSWER_EXPOSE_SECS = 12 * 3600
FORMAL_REPLY_MODES = ("reply", "reply_file")


class PendingLedgerError(ValueError):
    """The ledger file exists but is not a JSON object.

    The writer does not replace it. The report fails closed so a
    corrupt file is not announced as "nothing unanswered".
    """


def question_summary(text: str, limit: int = PENDING_SUMMARY_MAX) -> str:
    """One line of the user's question, capped for the sweep report."""
    sample = " ".join(str(text or "").split())
    if not sample:
        return PENDING_SUMMARY_EMPTY
    if len(sample) <= limit:
        return sample
    return sample[:limit]


def pending_answers_path(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Shared ledger path.

    Hooks pass the sibling of their own state directory, which is
    ``$HOME/hooks/state/pending_answers.json``. The bridge, whose unit
    sets ``HOME=/home/hatch``, resolves the same file through here.
    ``PENDING_ANSWERS_PATH`` overrides both.
    """
    if explicit is not None and str(explicit).strip():
        return Path(explicit)
    env = os.environ.get("PENDING_ANSWERS_PATH", "").strip()
    if env:
        return Path(env)
    home = os.environ.get("HOME", "").strip()
    if home and home != "/root":
        return Path(home) / "hooks" / "state" / "pending_answers.json"
    return muse_home() / "hooks" / "state" / "pending_answers.json"


def pending_answers_dismissed_path(ledger: str | os.PathLike[str]) -> Path:
    """Sibling file the sweep reads when the user has written a msgid off."""
    path = Path(ledger)
    return path.with_name("pending_answers_dismissed.json")


def load_pending_ledger(path: str | os.PathLike[str]) -> dict:
    """Channel map ``{channel: {msgid: {source, ts, summary}}}``.

    A missing file is an empty map. A present file that is not a JSON
    object raises PendingLedgerError.
    """
    ledger = Path(path)
    if not ledger.exists():
        return {}
    try:
        raw = ledger.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise PendingLedgerError(str(ledger)) from exc
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PendingLedgerError(str(ledger)) from exc
    if not isinstance(data, dict):
        raise PendingLedgerError(str(ledger))
    return data


def _prepare_pending_row(row: dict) -> dict | None:
    """Normalized row, or None when channel or msgid is blank."""
    channel = str(row.get("channel") or "").strip()
    msgid = str(row.get("msgid") or "").strip()
    source = str(row.get("source") or "").strip()
    if source not in PENDING_ANSWER_SOURCES:
        raise ValueError(f"unknown pending-answer source {source!r}")
    if not channel or not msgid:
        return None
    stamp = row.get("ts")
    if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
        stamp = time.time()
    summary = row.get("summary")
    if summary is None:
        summary = row.get("text") or ""
    return {
        "channel": channel,
        "msgid": msgid,
        "source": source,
        "ts": float(stamp),
        "summary": question_summary(str(summary)),
    }


def record_pending_answers(path: str | os.PathLike[str], rows: list[dict]) -> int:
    """Insert or update pending-answer rows. Returns how many changed.

    The first sighting of a msgid keeps its timestamp. A later path
    for the same msgid replaces ``source`` (and a non-empty summary)
    and leaves that timestamp. The same source twice is a no-op.
    A corrupt ledger is left untouched and raises PendingLedgerError.
    """
    prepared = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = _prepare_pending_row(row)
        if item is not None:
            prepared.append(item)
    if not prepared:
        return 0
    ledger = Path(path)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    lock_path = ledger.with_name(ledger.name + ".lock")
    changed = 0
    with lock_path.open("a+") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX)
        try:
            data = load_pending_ledger(ledger)
            for item in prepared:
                channel_rows = data.get(item["channel"])
                if not isinstance(channel_rows, dict):
                    channel_rows = {}
                    data[item["channel"]] = channel_rows
                previous = channel_rows.get(item["msgid"])
                if isinstance(previous, dict) and previous.get("source"):
                    if previous.get("source") == item["source"]:
                        continue
                    previous["source"] = item["source"]
                    if item["summary"] != PENDING_SUMMARY_EMPTY:
                        previous["summary"] = item["summary"]
                    changed += 1
                    continue
                channel_rows[item["msgid"]] = {
                    "source": item["source"],
                    "ts": item["ts"],
                    "summary": item["summary"],
                }
                changed += 1
            if changed:
                atomic_write_text(
                    ledger, json.dumps(data, ensure_ascii=False) + "\n")
        finally:
            fcntl.flock(lock_handle, fcntl.LOCK_UN)
    return changed


def _jsonl_dicts(path: Path) -> list[dict]:
    """JSON objects from a jsonl file. Missing or unreadable is empty."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return []
    rows: list[dict] = []
    # splitlines() also cuts on U+2028 / U+2029 / U+0085, which
    # json.dumps(ensure_ascii=False) keeps inside a string. JSONL
    # records are separated only by "\n".
    for line in text.split("\n"):
        parsed = parse_jsonl_line(line.rstrip("\r").encode("utf-8"))
        if not parsed or parsed.get("__invalid__"):
            continue
        rows.append(parsed)
    return rows


def delivered_formal_msgids(state_dir: str | os.PathLike[str]) -> set[str]:
    """Msgids whose formal reply or reply_file was delivered ok=true.

    Result rows name the outbox id. The msgid is taken from the result
    when it carries one, otherwise from the outbox row with that id.
    A delivered unbound ``send`` (stall notice, stop notice) is not an
    answer to the question.
    """
    state = Path(state_dir)
    id_to_msgid: dict[str, str] = {}
    for row in _jsonl_dicts(state / "outbox.jsonl"):
        row_id = str(row.get("id") or "")
        if row_id:
            id_to_msgid[row_id] = str(row.get("msgid") or "")
    answered: set[str] = set()
    for row in _jsonl_dicts(state / "outbox_results.jsonl"):
        if row.get("ok") is not True:
            continue
        if row.get("mode") not in FORMAL_REPLY_MODES:
            continue
        msgid = str(row.get("msgid") or "")
        if not msgid:
            msgid = id_to_msgid.get(str(row.get("id") or ""), "")
        if msgid:
            answered.add(msgid)
    return answered


def _inbox_texts(state_dir: Path) -> dict[str, str]:
    """Latest inbox text for each msgid. Missing inbox is an empty map."""
    texts: dict[str, str] = {}
    for row in _jsonl_dicts(state_dir / "inbox.jsonl"):
        msgid = str(row.get("msgid") or "")
        if msgid:
            texts[msgid] = str(row.get("text") or "")
    return texts


def _legacy_covered_rows(
    channel: str,
    hook_dir: Path,
    texts: dict[str, str],
) -> list[dict]:
    """covered_drops.jsonl rows not yet copied into the ledger.

    Read-only. The historical file stays the audit log it already is.
    """
    rows: list[dict] = []
    for record in _jsonl_dicts(hook_dir / "covered_drops.jsonl"):
        stamp = record.get("ts")
        if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
            continue
        msgids = record.get("msgids")
        if not isinstance(msgids, list):
            continue
        for raw in msgids:
            msgid = str(raw or "").strip()
            if not msgid:
                continue
            rows.append({
                "channel": channel,
                "msgid": msgid,
                "source": "covered-drop",
                "ts": float(stamp),
                "summary": question_summary(texts.get(msgid, "")),
            })
    return rows


def load_dismissed_msgids(path: str | os.PathLike[str] | None) -> set[tuple[str, str]]:
    """``(channel, msgid)`` pairs the user wrote off.

    A missing file dismisses nothing. A corrupt file dismisses nothing:
    hiding a still-unanswered question because the write-off file is
    unreadable would repeat the original gap.
    """
    if path is None:
        return set()
    dismissed = Path(path)
    if not dismissed.exists():
        return set()
    try:
        data = json.loads(dismissed.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return set()
    if not isinstance(data, dict):
        return set()
    found: set[tuple[str, str]] = set()
    for channel, msgids in data.items():
        if not isinstance(msgids, list):
            continue
        for raw in msgids:
            msgid = str(raw or "").strip()
            if msgid:
                found.add((str(channel), msgid))
    return found


def _ledger_items(data: dict) -> list[dict]:
    """Flat rows from a ledger object. Odd shapes are skipped."""
    items: list[dict] = []
    for channel, msgids in data.items():
        if not isinstance(msgids, dict):
            continue
        for msgid, record in msgids.items():
            if not isinstance(record, dict):
                continue
            source = str(record.get("source") or "")
            stamp = record.get("ts")
            if source not in PENDING_ANSWER_SOURCES:
                continue
            if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                continue
            msgid_text = str(msgid or "").strip()
            channel_text = str(channel or "").strip()
            if not channel_text or not msgid_text:
                continue
            items.append({
                "channel": channel_text,
                "msgid": msgid_text,
                "source": source,
                "ts": float(stamp),
                "summary": question_summary(str(record.get("summary") or "")),
            })
    return items


def summarize_pending_answers(
    ledger: str | os.PathLike[str],
    *,
    state_dirs: dict[str, str | os.PathLike[str]] | None = None,
    hook_dirs: dict[str, str | os.PathLike[str]] | None = None,
    now: float | None = None,
    min_age_secs: float = 0.0,
    dismissed_path: str | os.PathLike[str] | None = None,
) -> dict[str, list[dict]]:
    """Unanswered questions, grouped by channel. Does not write.

    Each item is ``msgid``, ``source``, ``ts``, ``summary``. ``ts`` is
    when the msgid first landed on the ledger. ``source`` is the latest
    path that recorded it (``covered-drop``, ``fail-stop``, or
    ``bridge-fallback``).

    A msgid drops out once ``state_dirs[channel]`` shows an ok=true
    formal reply, once it is listed in the dismiss file, or while it
    is younger than ``min_age_secs``. The sweep passes
    ``PENDING_ANSWER_EXPOSE_SECS``. Historical covered_drops.jsonl
    rows are folded in from ``hook_dirs`` when that msgid is not
    already on the ledger; the question text is read from that
    channel's inbox.jsonl.
    """
    data = load_pending_ledger(ledger)
    items = _ledger_items(data)
    present = {(item["channel"], item["msgid"]) for item in items}
    states = {key: Path(value) for key, value in (state_dirs or {}).items()}
    for channel, hook_dir in (hook_dirs or {}).items():
        texts = _inbox_texts(states[channel]) if channel in states else {}
        for item in _legacy_covered_rows(channel, Path(hook_dir), texts):
            key = (item["channel"], item["msgid"])
            if key in present:
                continue
            items.append(item)
            present.add(key)
    moment = time.time() if now is None else float(now)
    dismissed = load_dismissed_msgids(dismissed_path)
    answered = {
        channel: delivered_formal_msgids(state)
        for channel, state in states.items()
    }
    grouped: dict[str, list[dict]] = {}
    for item in items:
        channel = item["channel"]
        msgid = item["msgid"]
        if (channel, msgid) in dismissed:
            continue
        if msgid in answered.get(channel, set()):
            continue
        if moment - item["ts"] < min_age_secs:
            continue
        grouped.setdefault(channel, []).append({
            "msgid": msgid,
            "source": item["source"],
            "ts": item["ts"],
            "summary": item["summary"],
        })
    report: dict[str, list[dict]] = {}
    for channel in sorted(grouped):
        rows = grouped[channel]
        rows.sort(key=lambda row: (row["ts"], row["msgid"]))
        report[channel] = rows
    return report
