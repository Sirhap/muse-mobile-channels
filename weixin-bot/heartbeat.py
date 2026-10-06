#!/usr/bin/env python3
"""Detached heartbeat loop for one batch of msgids.

Spawned by the channel CLI (`heartbeat-start`) when a worker begins
working on a batch. Every 60s it refreshes per-msgid files under
<bot-state>/heartbeats/; the inbox hook counts a fresh heartbeat
file as batch activity, so a worker that is alive but silent
(long tool call, no user-visible progress) is not falsely declared
dead. The heartbeat is purely internal — nothing is sent to the
user.

The loop is deliberately self-terminating so it can never keep a
dead batch "alive" for long:
- the batch disappears from the hook state (finished, failed, or
  replaced) -> exit;
- any of its msgids is cancelled (user /stop or hook fail-stop)
  -> exit;
- a stop marker appears (CLI `heartbeat-stop`) -> exit;
- MAX_AGE_SECS elapse -> exit (a still-working worker restarts it).
On exit it removes only the heartbeat files this process owns.
A newer loop for the same msgid keeps its own pid file, so this
exit cannot delete the successor's beat.

Robustness (hardened 2026-10-05 after a transition incident where
a takeover worker started its heartbeat only after its batch had
already been fail-stopped, and the loop exited instantly without
ever beating):
- GRACE_SECS: at the start the loop beats unconditionally — batch
  state may lag or flap while a wake is being set up, and an early
  state read must never kill the loop before it has beaten once.
- Batch-gone needs ABSENT_CHECKS_TO_EXIT consecutive absent reads
  after the grace window; a single transient read proves nothing.
- An existing but unparseable active_batch.json counts as
  "unknown", never as "gone".
- Every exit is logged (one line) to heartbeats/loop.log so a
  future incident is diagnosable from disk.
- Once a msgid is cancelled, the beat file is not refreshed. The
  grace window still delays the exit so a flap cannot kill the
  loop instantly, but a stale beat lets the hook fail-stop.

Interval/grace/max-age can be overridden via HATCH_HB_INTERVAL,
HATCH_HB_GRACE and HATCH_HB_MAXAGE (seconds) for testing; the
production defaults below are what the CLI relies on.
"""

import json
import os
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from channel_common import read_pid_file, safe_child_path


def _env_secs(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name) or "")
        return value if value > 0 else default
    except ValueError:
        return default


INTERVAL_SECS = _env_secs("HATCH_HB_INTERVAL", 60)
GRACE_SECS = _env_secs("HATCH_HB_GRACE", 150)
MAX_AGE_SECS = _env_secs("HATCH_HB_MAXAGE", 20 * 60)
ABSENT_CHECKS_TO_EXIT = 2


def _read_json(path: Path):
    """(value, state): state is 'ok', 'missing', or 'bad'."""
    try:
        return json.loads(path.read_text(encoding="utf-8")), "ok"
    except FileNotFoundError:
        return None, "missing"
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None, "bad"


def _sleep_until(seconds: float, stop_paths: list[Path]) -> bool:
    """Sleep up to seconds. Return True if a stop marker appears."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if any(path.exists() for path in stop_paths):
            return True
        time.sleep(min(1.0, max(0.0, deadline - time.time())))
    return any(path.exists() for path in stop_paths)


def main() -> int:
    """Run until the batch ends, is cancelled, or is explicitly stopped."""
    state_dir = Path(sys.argv[1])
    hook_state_dir = Path(sys.argv[2])
    msgids = [item for item in sys.argv[3].split(",") if item]
    if not msgids:
        return 2
    hb_dir = state_dir / "heartbeats"
    hb_dir.mkdir(parents=True, exist_ok=True)
    paths: list[tuple[str, Path, Path, Path]] = []
    for msgid in msgids:
        beat = safe_child_path(hb_dir, msgid)
        if beat is None:
            continue
        paths.append((msgid, beat, beat.with_name(beat.name + ".pid"), beat.with_name(beat.name + ".stop")))
    if not paths:
        return 2
    started = time.time()
    absent_checks = 0
    pid = os.getpid()
    # Start record (2026-10-06): exits were already logged, but a
    # loop that died before its first controlled exit left NO trace
    # at all — one batch was fail-stopped as heartbeat-less while
    # its worker insisted heartbeat-start had succeeded, and there
    # was nothing to check. A start line makes "never started"
    # vs "started then vanished" distinguishable.
    try:
        with open(hb_dir / "loop.log", "a", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "ts": started,
                "msgids": [item[0] for item in paths],
                "pid": pid,
                "event": "start",
            }, ensure_ascii=False) + "\n")
    except OSError:
        pass

    def owns(msgid_pid: Path) -> bool:
        return read_pid_file(msgid_pid) == pid

    def cleanup() -> None:
        for _msgid, beat, pid_path, _stop in paths:
            if not owns(pid_path):
                continue
            for target in (beat, pid_path):
                try:
                    target.unlink()
                except OSError:
                    pass

    def bail(reason: str) -> int:
        try:
            with open(hb_dir / "loop.log", "a", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "ts": time.time(),
                    "msgids": [item[0] for item in paths],
                    "pid": pid,
                    "age_secs": round(time.time() - started, 1),
                    "exit": reason,
                }, ensure_ascii=False) + "\n")
        except OSError:
            pass
        cleanup()
        return 0

    stop_paths = [item[3] for item in paths]
    while True:
        now = time.time()
        age = now - started
        if any(path.exists() for path in stop_paths):
            return bail("stop_marker")
        batch, batch_state = _read_json(hook_state_dir / "active_batch.json")
        if batch_state == "bad":
            membership = "unknown"
        elif batch_state == "missing":
            membership = "absent"
        elif isinstance(batch, dict):
            live = {str(item) for item in (batch.get("msgids") or [])}
            for detached in batch.get("detached") or []:
                if isinstance(detached, dict):
                    live |= {str(item) for item in (detached.get("msgids") or [])}
            membership = "live" if any(item[0] in live for item in paths) else "absent"
        else:
            membership = "unknown"
        canc, _state = _read_json(state_dir / "cancelled.json")
        canc_ids: set[str] = set()
        if isinstance(canc, list):
            canc_ids = {
                str(row.get("msgid"))
                for row in canc
                if isinstance(row, dict)
            }
        cancelled = any(item[0] in canc_ids for item in paths)
        if age >= GRACE_SECS:
            if cancelled:
                return bail("cancelled")
            if membership == "absent":
                absent_checks += 1
                if absent_checks >= ABSENT_CHECKS_TO_EXIT:
                    return bail("batch_gone")
            elif membership == "live":
                absent_checks = 0
        if not cancelled:
            for _msgid, beat, pid_path, _stop in paths:
                tmp_pid = pid_path.with_name(f"{pid_path.name}.tmp.{pid}")
                tmp_beat = beat.with_name(f"{beat.name}.tmp.{pid}")
                try:
                    tmp_pid.write_text(str(pid), encoding="utf-8")
                    os.replace(tmp_pid, pid_path)
                    tmp_beat.write_text(str(now), encoding="utf-8")
                    os.replace(tmp_beat, beat)
                except OSError:
                    pass
        if age >= MAX_AGE_SECS:
            return bail("max_age")
        if _sleep_until(INTERVAL_SECS, stop_paths):
            return bail("stop_marker")


if __name__ == "__main__":
    sys.exit(main())
