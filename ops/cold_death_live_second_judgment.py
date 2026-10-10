#!/usr/bin/env python3
"""WeCom online second-judgment cold-death plant.

Default off. Refuses unless both ``DEATH_WATCH_LIVE_PLANT=1`` and
``DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment`` are set. ``--live``
and ``DEATH_WATCH_DRILL_LIVE`` do not turn it on.

Usage (only after the runbook authorization table names WeCom online
second judgment)::

    DEATH_WATCH_LIVE_PLANT=1 \\
    DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment \\
    python3 ops/cold_death_live_second_judgment.py

Operator runbook: ``docs/cold-death-drill-2026-10-09.md`` (线上第二判,
operator script). Writes only ``/home/hatch/hooks/state/wecom-bot`` and
``/home/hatch/workspace/wecom-bot/state``. Does not change
``DEATH_WATCH_SECS`` and does not stop services.

``ops/cold_death_drill.py`` stays sandbox-only. Do not add a live mode
there.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import death_watch_activity


CONFIRM = "wecom-second-judgment"
PLANT_TEXT = "【DRILL 冷判死】演练消息，不是用户任务。"
DRILL_TEXT_PREFIX = "【DRILL"
MSGID_PREFIX = "drill-deathwatch-"
DEATH_WATCH_SECS = 1800
SINCE_MARGIN_SECS = 100
ATTEMPT_AGE_SECS = 60
POLL_WAIT_SECS = 12

HATCH_HOOK_STATE = Path("/home/hatch/hooks/state/wecom-bot")
HATCH_BOT_STATE = Path("/home/hatch/workspace/wecom-bot/state")
HATCH_BRIDGE_STATE = Path("/home/hatch/workspace/native-bridge/state.json")
SNAP_PARENT = Path("/tmp")

BYPASS_FLAGS = ("--live", "--production", "--hatch", "--force", "--yes", "--confirm")


class PlantAbort(Exception):
    """Stop a plant. ``code`` 2 means nothing was planted; 1 means roll back."""

    def __init__(self, message: str, *, code: int = 1, msgid: str = "", snap: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.msgid = msgid
        self.snap = snap


def refuse(message: str) -> None:
    """Exit 2 before any hatch write."""
    print(message, file=sys.stderr)
    print(
        "Refusing. Default is off. "
        "See docs/cold-death-drill-2026-10-09.md (operator script). "
        "ops/cold_death_drill.py has no live mode.",
        file=sys.stderr,
    )
    raise SystemExit(2)


def enforce_gate(argv: list[str]) -> None:
    """Open the plant only for the two live env vars.

    ``--live`` and ``DEATH_WATCH_DRILL_LIVE`` are bypass switches and are
    refused even when the two real variables are also set.
    """
    for arg in argv:
        if arg in BYPASS_FLAGS or arg.startswith("--live"):
            refuse(
                f"{arg} does not enable this plant. "
                "DEATH_WATCH_DRILL_LIVE and --live are refused."
            )
        refuse(
            "This script takes no arguments. "
            "--live and DEATH_WATCH_DRILL_LIVE are refused. "
            "Set DEATH_WATCH_LIVE_PLANT=1 and "
            "DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment."
        )
    if "DEATH_WATCH_DRILL_LIVE" in os.environ:
        refuse(
            "DEATH_WATCH_DRILL_LIVE is set. It cannot bypass the live plant gate."
        )
    if os.environ.get("DEATH_WATCH_LIVE_PLANT") != "1":
        refuse(
            "DEATH_WATCH_LIVE_PLANT is not 1. Default is off. "
            "Set DEATH_WATCH_LIVE_PLANT=1 and "
            "DEATH_WATCH_LIVE_CONFIRM=wecom-second-judgment."
        )
    if os.environ.get("DEATH_WATCH_LIVE_CONFIRM") != CONFIRM:
        refuse(
            "DEATH_WATCH_LIVE_CONFIRM must be exactly wecom-second-judgment."
        )
    if DEATH_WATCH_SECS != 1800:
        refuse("DEATH_WATCH_SECS must stay 1800. This script will not change it.")
    if not PLANT_TEXT.startswith(DRILL_TEXT_PREFIX):
        refuse("Hardcoded plant text lost its 【DRILL prefix.")


def say(message: str) -> None:
    """Print a line before a later sleep can hide buffered stdout."""
    print(message, flush=True)


def last_nonempty_line(path: Path) -> str:
    """Return the last non-empty line, or an empty string."""
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            remaining = handle.tell()
            if remaining == 0:
                return ""
            buf = b""
            while remaining > 0:
                step = min(65536, remaining)
                remaining -= step
                handle.seek(remaining)
                buf = handle.read(step) + buf
                stripped = buf.rstrip(b"\n")
                newline = stripped.rfind(b"\n")
                if newline != -1:
                    return stripped[newline + 1:].decode("utf-8")
            return buf.strip(b"\n").decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise PlantAbort(f"cannot read {path}: {exc}", code=2) from exc


def read_json(path: Path) -> object | None:
    """Load JSON, or None when the file is absent."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PlantAbort(f"cannot parse {path}: {exc}", code=2) from exc


def load_id_lines(path: Path) -> set[str]:
    """Msgids stored one per line. A missing file is an empty set."""
    if not path.exists():
        return set()
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise PlantAbort(f"cannot read {path}: {exc}", code=2) from exc
    return {line.strip() for line in text.splitlines() if line.strip()}


def iter_jsonl(path: Path) -> list[dict[str, object]]:
    """Parse every complete JSON object in a jsonl file."""
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    try:
        with path.open("rb") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PlantAbort(f"cannot parse {path}: {exc}", code=2) from exc
    return rows


def file_contains_bytes(path: Path, needle: bytes) -> bool:
    """True when ``needle`` occurs anywhere in the file."""
    if not path.exists() or not needle:
        return False
    try:
        with path.open("rb") as handle:
            prev = b""
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    return False
                if needle in prev + chunk:
                    return True
                prev = chunk[-len(needle):]
    except OSError as exc:
        raise PlantAbort(f"cannot read {path}: {exc}", code=2) from exc


def batch_occupied(batch: object) -> bool:
    """True when an active batch already holds msgids or detached work."""
    if batch is None:
        return False
    if not isinstance(batch, dict):
        return True
    msgids = batch.get("msgids")
    if msgids not in (None, []):
        return True
    detached = batch.get("detached")
    if detached not in (None, []):
        return True
    return False


def pending_clear(pending: object) -> bool:
    """True when pending is missing or an empty object."""
    if pending is None:
        return True
    return pending == {}


def atomic_write_text(path: Path, text: str) -> None:
    """Replace ``path`` via a temp file on the same directory."""
    tmp = path.with_name(path.name + ".plant.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except OSError as exc:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise PlantAbort(f"failed to write {path}: {exc}", code=1) from exc


def append_bytes(path: Path, payload: bytes) -> None:
    """Append ``payload`` under an exclusive lock and fsync it."""
    try:
        with path.open("ab") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
    except OSError as exc:
        raise PlantAbort(f"failed to append {path}: {exc}", code=1) from exc


def append_msgid_line(path: Path, msgid: str) -> None:
    """Append one msgid, keeping it on its own line."""
    encoded = (msgid + "\n").encode("utf-8")
    try:
        with path.open("ab+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                handle.seek(0, os.SEEK_END)
                if handle.tell() > 0:
                    handle.seek(-1, os.SEEK_END)
                    if handle.read(1) != b"\n":
                        handle.write(b"\n")
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
    except OSError as exc:
        raise PlantAbort(f"failed to append {path}: {exc}", code=1) from exc


def cancelled_msgids(path: Path) -> set[str]:
    """Msgids already recorded in cancelled.json."""
    loaded = read_json(path)
    if loaded is None:
        return set()
    if not isinstance(loaded, list):
        raise PlantAbort(f"{path} is not a list", code=2)
    found: set[str] = set()
    for row in loaded:
        if isinstance(row, dict) and row.get("msgid"):
            found.add(str(row["msgid"]))
    return found


def parked_msgids(path: Path) -> set[str]:
    """Msgids still sitting in the gateway park lane."""
    loaded = read_json(path)
    if loaded is None:
        return set()
    if not isinstance(loaded, dict):
        raise PlantAbort(f"{path} is not an object", code=2)
    found: set[str] = set()
    for record in loaded.values():
        if not isinstance(record, dict):
            continue
        item = record.get("item")
        if isinstance(item, dict) and item.get("msgid"):
            found.add(str(item["msgid"]))
    return found


def delivered_formal_msgids(bot_state: Path) -> set[str]:
    """Msgids whose reply or reply_file has ok=true in outbox_results."""
    id_to_msgid: dict[str, str] = {}
    for row in iter_jsonl(bot_state / "outbox.jsonl"):
        row_id = row.get("id")
        if row_id:
            id_to_msgid[str(row_id)] = str(row.get("msgid") or "")
    delivered: set[str] = set()
    for row in iter_jsonl(bot_state / "outbox_results.jsonl"):
        if not row.get("ok") or row.get("mode") not in ("reply", "reply_file"):
            continue
        msgid = id_to_msgid.get(str(row.get("id") or ""))
        if msgid:
            delivered.add(msgid)
    return delivered


def covering_msgid(bot_state: Path, chatid: str, since: float) -> str | None:
    """A same-chat formal reply delivered for a message after ``since - 2``."""
    delivered = delivered_formal_msgids(bot_state)
    if not delivered:
        return None
    for row in iter_jsonl(bot_state / "inbox.jsonl"):
        if str(row.get("chatid") or "") != chatid:
            continue
        ts = row.get("ts")
        if isinstance(ts, bool) or not isinstance(ts, (int, float)):
            continue
        if float(ts) <= since - 2:
            continue
        msgid = str(row.get("msgid") or "")
        if msgid in delivered:
            return msgid
    return None


def require_hatch_targets(hook_state: Path, bot_state: Path, bridge_state: Path, snap_parent: Path) -> None:
    """Refuse every target that is not the documented hatch path."""
    expected = (
        (hook_state, HATCH_HOOK_STATE),
        (bot_state, HATCH_BOT_STATE),
        (bridge_state, HATCH_BRIDGE_STATE),
        (snap_parent, SNAP_PARENT),
    )
    for got, want in expected:
        if got != want:
            raise PlantAbort(
                f"refusing path {got}; this plant only writes under /home/hatch "
                f"(snapshot under /tmp). Expected {want}.",
                code=2,
            )


def copy_address(row: dict[str, object]) -> dict[str, str]:
    """Copy chat routing off the latest real inbox row. Never invent a chatid."""
    chatid = row.get("chatid")
    chattype = row.get("chattype")
    from_userid = row.get("from_userid")
    if not isinstance(chatid, str) or not chatid.strip():
        raise PlantAbort("last inbox row has no chatid; refusing to invent one", code=2)
    if chattype not in {"single", "group"}:
        raise PlantAbort(
            "last inbox chattype is not single or group; refusing to invent one",
            code=2,
        )
    if not isinstance(from_userid, str) or not from_userid.strip():
        raise PlantAbort("last inbox row has no from_userid", code=2)
    return {"chatid": chatid, "chattype": chattype, "from_userid": from_userid}


def inbox_mark(path: Path) -> tuple[int, str]:
    """Size plus last line, so a message that lands mid-plant is visible."""
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise PlantAbort(f"cannot stat {path}: {exc}", code=2) from exc
    return size, last_nonempty_line(path)


def small_file_text(path: Path) -> str | None:
    """Full text of a small state file, or None when it is absent."""
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise PlantAbort(f"cannot read {path}: {exc}", code=2) from exc


def take_snapshot(hook_state: Path, bot_state: Path, snap_parent: Path) -> Path:
    """Copy hook state and the size notes from the runbook. Does not stop anything."""
    stamp = time.strftime("%Y%m%d%H%M%S")
    snap = snap_parent / f"death-watch-drill-snap-{stamp}-{os.getpid()}"
    try:
        snap.mkdir(parents=True)
        hook_dest = snap / "hook-state"
        hook_dest.mkdir()
    except OSError as exc:
        raise PlantAbort(f"cannot create snapshot {snap}: {exc}", code=2) from exc
    copied = subprocess.run(
        ["cp", "-a", f"{hook_state}/.", str(hook_dest)],
        capture_output=True,
        text=True,
        check=False,
    )
    if copied.returncode != 0:
        raise PlantAbort(
            f"snapshot copy failed: {copied.stderr.strip()}",
            code=2,
        )
    cancelled = bot_state / "cancelled.json"
    if cancelled.exists():
        try:
            (snap / "cancelled.json").write_bytes(cancelled.read_bytes())
        except OSError as exc:
            raise PlantAbort(f"cannot snapshot cancelled.json: {exc}", code=2) from exc
    else:
        (snap / "cancelled.missing").write_text("absent\n", encoding="utf-8")
    size_lines: list[str] = []
    for name in ("inbox.jsonl", "outbox.jsonl", "outbox_results.jsonl"):
        target = bot_state / name
        size = target.stat().st_size if target.exists() else 0
        size_lines.append(f"{size} {target}")
    (snap / "sizes.txt").write_text("\n".join(size_lines) + "\n", encoding="utf-8")
    inbox = bot_state / "inbox.jsonl"
    tail = last_nonempty_line(inbox) if inbox.exists() else ""
    (snap / "inbox-tail.txt").write_text(tail + ("\n" if tail else ""), encoding="utf-8")
    return snap


def assert_drill_tail(line: str, msgid: str) -> dict[str, object]:
    """Require the inbox tail to be this drill sentence. Do not write the batch here."""
    try:
        parsed = json.loads(line)
        text = parsed["text"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise PlantAbort(
            "inbox tail is not a JSON object with text; "
            "aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        ) from exc
    if not isinstance(parsed, dict):
        raise PlantAbort(
            "inbox tail is not an object; aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        )
    if not isinstance(text, str) or not text.startswith("【DRILL"):
        raise PlantAbort(
            "inbox tail text does not start with 【DRILL; "
            "aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        )
    assert json.loads(line)["text"].startswith("【DRILL")
    if text != PLANT_TEXT or parsed.get("msgid") != msgid:
        raise PlantAbort(
            "inbox tail is not the hardcoded drill plant; "
            "aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        )
    return parsed


def write_active_batch_last(
    inbox: Path,
    batch_path: Path,
    msgid: str,
    since: float,
    expected_inbox_size: int,
) -> None:
    """Write active_batch.json only after the drill assert holds."""
    try:
        current_size = inbox.stat().st_size
    except OSError as exc:
        raise PlantAbort(
            f"cannot stat inbox after the plant line: {exc}; "
            "aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        ) from exc
    if current_size != expected_inbox_size:
        raise PlantAbort(
            "inbox size changed after the plant line; "
            "aborting without writing active_batch.json",
            code=1,
            msgid=msgid,
        )
    try:
        line = last_nonempty_line(inbox)
    except PlantAbort as exc:
        exc.code = 1
        exc.msgid = msgid
        raise
    assert_drill_tail(line, msgid)
    try:
        current_batch = read_json(batch_path)
    except PlantAbort as exc:
        exc.code = 1
        exc.msgid = msgid
        raise
    if batch_occupied(current_batch):
        raise PlantAbort(
            "active_batch.json is occupied; not overwriting it",
            code=1,
            msgid=msgid,
        )
    payload = {"msgids": [msgid], "since": since, "detached": []}
    atomic_write_text(batch_path, json.dumps(payload, ensure_ascii=False))


def readonly_checks(
    hook_state: Path,
    bot_state: Path,
    bridge_state: Path,
    msgid: str,
    since: float,
    address: dict[str, str],
) -> None:
    """Stop before any write when the queue is not an empty second-judgment target."""
    if not hook_state.is_dir():
        raise PlantAbort(f"hook state directory is missing: {hook_state}", code=2)
    if not bot_state.is_dir():
        raise PlantAbort(f"wecom-bot state directory is missing: {bot_state}", code=2)
    inbox = bot_state / "inbox.jsonl"
    if not inbox.is_file():
        raise PlantAbort(f"inbox is missing: {inbox}", code=2)
    if batch_occupied(read_json(hook_state / "active_batch.json")):
        raise PlantAbort("active_batch.json already holds msgids or detached work", code=2)
    if not pending_clear(read_json(hook_state / "pending.json")):
        raise PlantAbort("pending.json is not empty", code=2)
    if file_contains_bytes(inbox, msgid.encode("utf-8")):
        raise PlantAbort(f"{msgid} is already in the inbox", code=2)
    if msgid in load_id_lines(hook_state / "seen_msgids.txt"):
        raise PlantAbort(f"{msgid} is already in seen_msgids.txt", code=2)
    if msgid in load_id_lines(hook_state / "carried_msgids.txt"):
        raise PlantAbort(f"{msgid} is already in carried_msgids.txt", code=2)
    if msgid in cancelled_msgids(bot_state / "cancelled.json"):
        raise PlantAbort(f"{msgid} is already in cancelled.json", code=2)
    if msgid in parked_msgids(bot_state / "outbox_parked.json"):
        raise PlantAbort(f"{msgid} is in outbox_parked.json", code=2)
    covered = covering_msgid(bot_state, address["chatid"], since)
    if covered:
        raise PlantAbort(
            f"chat {address['chatid']} already has a delivered formal reply "
            f"after since ({covered}); death watch would drop the plant",
            code=2,
        )
    info = death_watch_activity.assess(
        msgid,
        now=time.time(),
        window=float(DEATH_WATCH_SECS),
        bridge_state_path=str(bridge_state),
        worker_activity_path=str(bot_state / "worker_activity.json"),
    )
    if info.get("alive"):
        raise PlantAbort(
            f"activity probe still calls {msgid} alive ({info.get('source')})",
            code=2,
        )
    degraded = info.get("degraded") or []
    if degraded:
        raise PlantAbort(
            f"activity probe is degraded ({degraded}); that is not a pass",
            code=2,
        )


def plant_wecom_second_judgment(
    hook_state: Path,
    bot_state: Path,
    bridge_state: Path,
    snap_parent: Path,
    *,
    allow_non_hatch: bool = False,
    now: float | None = None,
) -> dict[str, object]:
    """Run the runbook plant. ``active_batch.json`` is the last write.

    ``allow_non_hatch`` exists so tests can exercise the same order on a
    temp tree. The CLI never sets it, and the default refuses every path
    other than the hatch paths above.
    """
    if not allow_non_hatch:
        enforce_gate([])
        require_hatch_targets(hook_state, bot_state, bridge_state, snap_parent)
    if DEATH_WATCH_SECS != 1800 or not PLANT_TEXT.startswith("【DRILL"):
        raise PlantAbort("refusing to plant with a changed death watch or drill text", code=2)

    clock = time.time() if now is None else now
    since = clock - (DEATH_WATCH_SECS + SINCE_MARGIN_SECS)
    attempt_at = clock - ATTEMPT_AGE_SECS
    msgid = f"{MSGID_PREFIX}{int(clock)}-{os.getpid()}"
    if not msgid.startswith(MSGID_PREFIX) or "\n" in msgid:
        raise PlantAbort(f"refusing msgid {msgid!r}", code=2)

    inbox = bot_state / "inbox.jsonl"
    last_row_raw = last_nonempty_line(inbox) if inbox.exists() else ""
    if not last_row_raw:
        raise PlantAbort("inbox has no row to copy chatid from", code=2)
    try:
        last_row = json.loads(last_row_raw)
    except json.JSONDecodeError as exc:
        raise PlantAbort(f"last inbox row is not JSON: {exc}", code=2) from exc
    if not isinstance(last_row, dict):
        raise PlantAbort("last inbox row is not an object", code=2)
    address = copy_address(last_row)

    steps = ["readonly"]
    readonly_checks(hook_state, bot_state, bridge_state, msgid, since, address)
    before_inbox = inbox_mark(inbox)
    before_batch = small_file_text(hook_state / "active_batch.json")
    before_pending = small_file_text(hook_state / "pending.json")

    snap = take_snapshot(hook_state, bot_state, snap_parent)
    steps.append("snapshot")
    outbox_offset = [-1]
    try:
        _plant_after_snapshot(
            hook_state,
            inbox,
            before_inbox,
            before_batch,
            before_pending,
            snap,
            msgid,
            since,
            attempt_at,
            address,
            steps,
            outbox_offset,
        )
    except PlantAbort as exc:
        if not exc.snap:
            exc.snap = str(snap)
        if not exc.msgid:
            exc.msgid = msgid
        raise
    return {
        "msgid": msgid,
        "snap": snap,
        "chatid": address["chatid"],
        "chattype": address["chattype"],
        "since": since,
        "steps": steps,
        "outbox_offset": outbox_offset[0],
    }


def _plant_after_snapshot(
    hook_state: Path,
    inbox: Path,
    before_inbox: tuple[int, str],
    before_batch: str | None,
    before_pending: str | None,
    snap: Path,
    msgid: str,
    since: float,
    attempt_at: float,
    address: dict[str, str],
    steps: list[str],
    outbox_offset: list[int],
) -> None:
    """Merge resume state, append the drill line, assert it, then write the batch."""
    if inbox_mark(inbox) != before_inbox:
        raise PlantAbort(
            "inbox changed during snapshot; stopping before any plant write",
            code=2,
            msgid=msgid,
            snap=str(snap),
        )
    if small_file_text(hook_state / "active_batch.json") != before_batch:
        raise PlantAbort(
            "active_batch.json changed during snapshot; stopping before any plant write",
            code=2,
            msgid=msgid,
            snap=str(snap),
        )
    if small_file_text(hook_state / "pending.json") != before_pending:
        raise PlantAbort(
            "pending.json changed during snapshot; stopping before any plant write",
            code=2,
            msgid=msgid,
            snap=str(snap),
        )

    attempts_path = hook_state / "resume_attempts.json"
    existing = read_json(attempts_path)
    if existing is None:
        merged: dict[str, object] = {}
    elif isinstance(existing, dict):
        merged = dict(existing)
    else:
        raise PlantAbort("resume_attempts.json is not an object; not overwriting it", code=2, msgid=msgid, snap=str(snap))
    merged[msgid] = attempt_at
    atomic_write_text(attempts_path, json.dumps(merged, ensure_ascii=False))
    steps.append("resume_attempts")

    append_msgid_line(hook_state / "seen_msgids.txt", msgid)
    steps.append("seen")
    append_msgid_line(hook_state / "carried_msgids.txt", msgid)
    steps.append("carried")

    plant_row = {
        "msgid": msgid,
        "from_userid": address["from_userid"],
        "chattype": address["chattype"],
        "chatid": address["chatid"],
        "msgtype": "text",
        "text": PLANT_TEXT,
        "ts": since,
        "media": [],
    }
    plant_line = (json.dumps(plant_row, ensure_ascii=False) + "\n").encode("utf-8")
    size_before = inbox.stat().st_size
    append_bytes(inbox, plant_line)
    steps.append("inbox")
    expected_size = size_before + len(plant_line)

    # json.loads(last line)["text"].startswith("【DRILL") or abort
    # without writing active_batch.json.
    line = last_nonempty_line(inbox)
    try:
        assert_drill_tail(line, msgid)
    except PlantAbort as exc:
        exc.code = 1
        exc.msgid = msgid
        exc.snap = str(snap)
        raise
    steps.append("assert_drill")
    outbox = inbox.parent / "outbox.jsonl"
    try:
        outbox_offset[0] = outbox.stat().st_size if outbox.exists() else 0
    except OSError as exc:
        raise PlantAbort(
            f"cannot stat outbox before active_batch.json: {exc}",
            code=1,
            msgid=msgid,
            snap=str(snap),
        ) from exc
    # active_batch.json is the last plant write. The drill assert above
    # already ran; write_active_batch_last repeats it and bails if the
    # tail is no longer the hardcoded 【DRILL sentence.
    write_active_batch_last(inbox, hook_state / "active_batch.json", msgid, since, expected_size)
    steps.append("active_batch")


def rows_after(path: Path, offset: int) -> list[dict[str, object]]:
    """JSON objects appended after ``offset``."""
    if not path.exists():
        return []
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size < offset:
                handle.seek(0)
            else:
                handle.seek(offset)
            data = handle.read()
    except OSError as exc:
        say(f"cannot read {path}: {exc}")
        return []
    rows: list[dict[str, object]] = []
    for raw in data.splitlines():
        if not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def notice_text(row: dict[str, object]) -> str:
    """Operator-visible text on an outbox or failed-notice row."""
    content = row.get("content")
    if isinstance(content, str):
        return content
    text = row.get("text")
    if isinstance(text, str):
        return text
    return ""


def is_stop_notice(row: dict[str, object], msgid: str) -> bool:
    """True when this row is the second-judgment stop notice for the plant."""
    text = notice_text(row)
    if "任务已停止" in text and "DRILL" in text:
        return True
    return bool(msgid) and msgid in text


def report_outbox(bot_state: Path, offset: int, msgid: str) -> None:
    """Print a new stop notice, or tell the operator where to look."""
    rows = [row for row in rows_after(bot_state / "outbox.jsonl", offset) if is_stop_notice(row, msgid)]
    if rows:
        say("outbox stop notice:")
        for row in rows:
            say(json.dumps(row, ensure_ascii=False))
        ids = {str(row.get("id")) for row in rows if row.get("id")}
        for result in rows_after(bot_state / "outbox_results.jsonl", 0):
            if str(result.get("id") or "") in ids:
                say("outbox_results: " + json.dumps(result, ensure_ascii=False))
        return
    failed = read_failed_notices(bot_state / "failed_notices.json", msgid)
    if failed:
        say("stop notice is still in failed_notices.json (CLI send has not succeeded):")
        for row in failed:
            say(json.dumps(row, ensure_ascii=False))
    say(
        "No drill stop notice in the new outbox bytes. Do not plant again. "
        f"Operator: inspect {bot_state / 'outbox.jsonl'} for a send whose content "
        "contains 任务已停止 and DRILL, then "
        f"{bot_state / 'outbox_results.jsonl'} for ok=true on that id. "
        "Evidence list: docs/cold-death-drill-2026-10-09.md. "
        "Do not change DEATH_WATCH_SECS. Do not stop services."
    )


def read_failed_notices(path: Path, msgid: str) -> list[dict[str, object]]:
    """Drill-related failed notices. Read-only; a bad file is reported empty."""
    if not path.exists():
        return []
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        say(f"failed_notices.json is unreadable: {path}")
        return []
    if not isinstance(loaded, list):
        return []
    found: list[dict[str, object]] = []
    for row in loaded:
        if isinstance(row, dict) and is_stop_notice(row, msgid):
            found.append(row)
    return found


def main(argv: list[str]) -> int:
    """Gate, plant, print msgid and snapshot, then one poll."""
    enforce_gate(argv)
    try:
        result = plant_wecom_second_judgment(
            HATCH_HOOK_STATE,
            HATCH_BOT_STATE,
            HATCH_BRIDGE_STATE,
            SNAP_PARENT,
        )
    except PlantAbort as exc:
        print(str(exc), file=sys.stderr)
        if exc.msgid:
            print(f"msgid {exc.msgid}", file=sys.stderr)
        if exc.snap:
            print(f"snap {exc.snap}", file=sys.stderr)
        print(
            "Do not change DEATH_WATCH_SECS. Do not stop services. "
            "If a plant write already landed, roll back with "
            "docs/cold-death-drill-2026-10-09.md and do not run this again.",
            file=sys.stderr,
        )
        return exc.code
    msgid = str(result["msgid"])
    snap = result["snap"]
    say(f"msgid {msgid}")
    say(f"snap {snap}")
    say(
        f"chatid {result['chatid']} chattype {result['chattype']} "
        f"since {result['since']}"
    )
    say("DEATH_WATCH_SECS left at 1800. No services stopped.")
    say(f"waiting {POLL_WAIT_SECS}s for one hook poll")
    time.sleep(POLL_WAIT_SECS)
    report_outbox(HATCH_BOT_STATE, int(result["outbox_offset"]), msgid)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except PlantAbort as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(exc.code)
