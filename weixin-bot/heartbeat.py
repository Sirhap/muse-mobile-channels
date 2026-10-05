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
On exit it removes its own heartbeat files.

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

Interval/grace/max-age can be overridden via HATCH_HB_INTERVAL,
HATCH_HB_GRACE and HATCH_HB_MAXAGE (seconds) for testing; the
production defaults below are what the CLI relies on.
"""

import json
import os
import sys
import time
from pathlib import Path


def _env_secs(name: str, default: float) -> float:
    try:
        v = float(os.environ.get(name) or "")
        return v if v > 0 else default
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
    except Exception:
        return None, "bad"


def main() -> int:
    state_dir = Path(sys.argv[1])
    hook_state_dir = Path(sys.argv[2])
    msgids = [m for m in sys.argv[3].split(",") if m]
    if not msgids:
        return 2
    hb_dir = state_dir / "heartbeats"
    hb_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    absent_checks = 0

    def cleanup() -> None:
        for m in msgids:
            for suffix in ("", ".stop"):
                try:
                    (hb_dir / f"{m}{suffix}").unlink()
                except OSError:
                    pass

    def bail(reason: str) -> int:
        try:
            with open(hb_dir / "loop.log", "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": time.time(), "msgids": msgids,
                    "age_secs": round(time.time() - started, 1),
                    "exit": reason}, ensure_ascii=False) + "\n")
        except OSError:
            pass
        cleanup()
        return 0

    while True:
        now = time.time()
        age = now - started
        # Exit: stop marker (explicit instruction — honored at once).
        if any((hb_dir / f"{m}.stop").exists() for m in msgids):
            return bail("stop_marker")
        # Batch membership: 'live' / 'absent' / 'unknown'.
        batch, bstate = _read_json(hook_state_dir / "active_batch.json")
        if bstate == "bad":
            membership = "unknown"
        elif bstate == "missing":
            membership = "absent"
        elif isinstance(batch, dict):
            live = {str(m) for m in (batch.get("msgids") or [])}
            for d in batch.get("detached") or []:
                if isinstance(d, dict):
                    live |= {str(m) for m in (d.get("msgids") or [])}
            membership = "live" if any(m in live for m in msgids) else "absent"
        else:
            membership = "unknown"
        # Cancellation (unreadable file = not cancelled this cycle).
        canc, _ = _read_json(state_dir / "cancelled.json")
        canc_ids = set()
        if isinstance(canc, list):
            canc_ids = {str(r.get("msgid")) for r in canc
                        if isinstance(r, dict)}
        cancelled = any(m in canc_ids for m in msgids)
        if age >= GRACE_SECS:
            if cancelled:
                return bail("cancelled")
            if membership == "absent":
                absent_checks += 1
                if absent_checks >= ABSENT_CHECKS_TO_EXIT:
                    return bail("batch_gone")
            elif membership == "live":
                absent_checks = 0
        # Beat.
        for m in msgids:
            p = hb_dir / m
            tmp = p.with_name(f"{m}.tmp.{os.getpid()}")
            try:
                tmp.write_text(str(now), encoding="utf-8")
                tmp.replace(p)
            except OSError:
                pass
        if age >= MAX_AGE_SECS:
            return bail("max_age")
        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    sys.exit(main())
