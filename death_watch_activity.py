"""Read-only real-activity probe for the cold-channel death watch.

The inbox hooks decide resume-once and cancel from outbound silence.
This module is the check they run first. It does not write state, does
not call Muse, and does not read ``heartbeats/<msgid>`` mtime.

Sources, in order:

1. ``native-bridge/state.json`` turns bound to the msgid (the turn's
   ``msgid`` or its merged ``ids``). A fresh ``activities[].ts`` /
   ``activity_seen`` timestamp, a fresh ``last_reply_at``, or Muse
   ``sess_status == "running"`` together with a fresh
   ``last_activity_poll`` counts as alive. ``last_activity_poll`` by
   itself does not: the bridge updates it on every poll even when
   ``activity.list`` returns nothing. A frozen ``running`` whose poll
   is older than the death-watch window does not count either.
2. ``<bot-state>/worker_activity.json``, a worker-written map
   ``{msgid: {"ts": epoch, "source": "..."}}`` (a bare epoch is also
   accepted). The timestamp inside the file counts. The file's own
   mtime does not. Two optional fields ride on the same record
   (todo 21, routes C+E): ``run_id`` -- the worker's self-registered
   platform run key, written at batch start -- and ``hold_until``
   -- an epoch until which the worker has declared it will be
   silent on purpose (a long operation). A live hold counts as
   alive (source ``worker-hold``) and outranks every other source,
   including a platform terminal verdict: a declared silence is
   never judged dead mid-hold.
3. ``<bot-state>/worker_platform_status.json``, a verdict snapshot
   written by the platform-check agent (see
   docs/worker-platform-check-runbook-2026-10-15.md), a map
   ``{msgid: {"run_id": ..., "status": ..., "agent_updated_at":
   epoch, "checked_at": epoch}}``. A terminal status (completed /
   shutdown / errored / interrupted) whose ``checked_at`` is fresh
   is returned as ``platform_terminal`` evidence -- positive proof
   the worker's platform session has ended. It is NOT an alive
   signal and never sets ``alive`` by itself; the hook only acts on
   it in conjunction with worker-push silence and no delivered
   reply. ``status == "running"`` with a fresh ``agent_updated_at``
   counts as alive (source ``platform-running``), the weakest
   signal. A verdict whose run_id disagrees with the registered
   run_id is ignored (a newer wake re-registered the msgid). A
   stale ``checked_at``, an unknown status, or a missing/corrupt
   file is no signal, exactly like the other sources.
4. A worker handoff record in the same ``worker_activity.json``
   map: ``{msgid: {"handoff": true, "handoff_at": epoch}}``.
   A handoff written within ``HANDOFF_MAX_AGE_SECS`` counts as
   alive (source ``worker-handoff``) and, like a live hold,
   outranks a platform-terminal verdict: the worker declared
   this silence on purpose by transferring the batch to the
   main session, so it must not be resumed or fail-stopped.
   A handoff is bounded, not forever: after 24h it expires and
   the normal death route resumes, so a main session that never
   acts cannot hide a batch indefinitely. A handoff is not a
   formal reply -- the worker must still send one first (see the
   hook definitions' HANDOFF rule); it is only the safety net
   when that reply has not yet landed when the hook polls.

``queue_snapshot``, ``status.json``, and heartbeat files are not
sources.

Failure degradation: a missing file is no signal, not an error. A
path that exists but cannot be parsed is no signal plus a degraded
reason. No positive fresh signal means not alive, and the hook then
uses the existing resume-once / cancel path. One unreadable source
does not hide a fresh signal from the other source.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone


# A timestamp this far in the future is not an observation.
_FUTURE_SKEW_SECS = 120.0

# Source 3 (todo 21): a verdict snapshot is only trusted while its
# checked_at is this fresh; a running verdict only counts as alive
# while the agent record itself updated within this window. The
# platform's terminal flip can lag a real stall by ~550s (spike,
# n=1), so neither value is a death threshold on its own -- death
# still requires the hook's conjunction with push silence.
PLATFORM_VERDICT_MAX_AGE_SECS = 600.0
PLATFORM_RUNNING_FRESH_SECS = 300.0
PLATFORM_TERMINAL_STATUSES = frozenset(
    {"completed", "shutdown", "errored", "interrupted"})

# Source 4 (2026-10-29 handoff fix): a handoff marker is trusted
# for one day. After that the batch returns to the normal death
# route instead of being held forever by a main session that
# never acted.
HANDOFF_MAX_AGE_SECS = 86400.0


def epoch_of(raw: object) -> float | None:
    """Epoch seconds from a number or an ISO-8601 string.

    ``0`` and booleans are not timestamps. Unparseable values are
    ``None``.
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        if raw <= 0:
            return None
        return float(raw)
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _fresh(ts: float | None, now: float, window: float) -> bool:
    """True when ts is inside the death-watch window, allowing slight skew."""
    if ts is None:
        return False
    age = now - ts
    if age < -_FUTURE_SKEW_SECS:
        return False
    return age < window


def _read_object(path: str) -> tuple[dict | None, str | None]:
    """Return (object, degraded_reason).

    A missing path is ``(None, None)``. An unreadable or non-object
    file is ``(None, reason)``.
    """
    if not path:
        return None, None
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None, None
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None, "unreadable:" + os.path.basename(path)
    if not isinstance(data, dict):
        return None, "not-object:" + os.path.basename(path)
    return data, None


def _bound(turn: dict, msgid: str) -> bool:
    """True when this bridge turn is the msgid or has merged it in."""
    if str(turn.get("msgid") or "") == msgid:
        return True
    ids = turn.get("ids")
    if isinstance(ids, list) and any(str(item) == msgid for item in ids):
        return True
    return False


def _activity_times(turn: dict) -> list[float]:
    """Real activity.list timestamps stored on a turn. Poll clocks are not included."""
    found: list[float] = []
    activities = turn.get("activities")
    if isinstance(activities, list):
        for item in activities:
            if not isinstance(item, dict):
                continue
            ts = epoch_of(item.get("ts"))
            if ts is not None:
                found.append(ts)
    seen = turn.get("activity_seen")
    if isinstance(seen, list):
        for item in seen:
            raw = item[0] if isinstance(item, (list, tuple)) and item else None
            ts = epoch_of(raw)
            if ts is not None:
                found.append(ts)
    return found


def _consider(best: tuple[int, float, str] | None,
              priority: int, ts: float, source: str) -> tuple[int, float, str]:
    """Keep the better observation. Lower priority wins; equal priority keeps the newer ts."""
    cand = (priority, ts, source)
    if best is None or priority < best[0] or (priority == best[0] and ts > best[1]):
        return cand
    return best


def _bridge_hit(state: dict, msgid: str, now: float,
                window: float) -> tuple[int, float, str] | None:
    """Best fresh bridge observation for msgid, or None."""
    channels = state.get("channels")
    if not isinstance(channels, dict):
        return None
    best: tuple[int, float, str] | None = None
    for channel in channels.values():
        if not isinstance(channel, dict):
            continue
        turns = channel.get("turns")
        if not isinstance(turns, list):
            continue
        for turn in turns:
            if not isinstance(turn, dict) or not _bound(turn, msgid):
                continue
            for ts in _activity_times(turn):
                if _fresh(ts, now, window):
                    best = _consider(best, 0, ts, "bridge-activity")
            reply_at = epoch_of(turn.get("last_reply_at"))
            if _fresh(reply_at, now, window):
                best = _consider(best, 1, reply_at, "bridge-reply")
            if turn.get("sess_status") == "running":
                polled = epoch_of(turn.get("last_activity_poll"))
                if _fresh(polled, now, window):
                    best = _consider(best, 2, polled, "bridge-running")
    return best


def _worker_ts(data: dict, msgid: str) -> float | None:
    """Epoch from a worker activity record. File mtime is not read."""
    raw = data.get(msgid)
    if isinstance(raw, dict):
        return epoch_of(raw.get("ts"))
    return epoch_of(raw)


def _worker_record(data: dict, msgid: str) -> dict | None:
    """The dict form of a worker activity record, or None."""
    raw = data.get(msgid)
    return raw if isinstance(raw, dict) else None


def _platform_hit(verdicts: dict, msgid: str, now: float,
                  registered_run_id: str | None) -> dict:
    """Read one msgid's platform verdict.

    Returns a dict with ``terminal`` (fresh terminal evidence),
    ``alive_ts`` (fresh running activity, else None) and ``status``
    (the verdict's status word when it was usable, else None). A
    verdict whose run_id disagrees with the registration is not
    usable at all: a newer wake may own this msgid now.
    """
    out = {"terminal": False, "alive_ts": None, "status": None}
    raw = verdicts.get(msgid)
    if not isinstance(raw, dict):
        return out
    verdict_run = raw.get("run_id")
    if (registered_run_id and isinstance(verdict_run, str)
            and verdict_run and verdict_run != registered_run_id):
        return out
    checked = epoch_of(raw.get("checked_at"))
    if not _fresh(checked, now, PLATFORM_VERDICT_MAX_AGE_SECS):
        return out
    status = str(raw.get("status") or "")
    if status in PLATFORM_TERMINAL_STATUSES:
        out["terminal"] = True
        out["status"] = status
        return out
    if status == "running":
        updated = epoch_of(raw.get("agent_updated_at"))
        if _fresh(updated, now, PLATFORM_RUNNING_FRESH_SECS):
            out["alive_ts"] = updated
            out["status"] = status
    return out


def assess(msgid: str, *, now: float, window: float,
           bridge_state_path: str, worker_activity_path: str,
           platform_status_path: str = "") -> dict:
    """Say whether msgid still has real activity inside ``window`` seconds.

    Returns ``alive``, ``source``, ``activity_ts``, and ``degraded``,
    plus the todo-21 fields ``platform_terminal``,
    ``platform_status``, ``worker_run_id``, and
    ``worker_hold_until``, plus the handoff fields
    ``worker_handoff`` and ``worker_handoff_at``. ``alive`` is
    true only for a positive fresh signal (a live worker hold or
    a fresh worker handoff counts). ``platform_terminal``
    is evidence for the hook's early route, never an alive signal.
    Degraded sources contribute no signal.
    """
    msgid = str(msgid)
    degraded: list[str] = []
    bridge, bridge_reason = _read_object(bridge_state_path)
    if bridge_reason:
        degraded.append(bridge_reason)
    worker, worker_reason = _read_object(worker_activity_path)
    if worker_reason:
        degraded.append(worker_reason)
    verdicts, verdict_reason = _read_object(platform_status_path)
    if verdict_reason:
        degraded.append(verdict_reason)

    record = _worker_record(worker, msgid) if isinstance(worker, dict) else None
    registered_run = None
    hold_until = None
    handoff_at = None
    if record is not None:
        run = record.get("run_id")
        if isinstance(run, str) and run:
            registered_run = run
        hold_until = epoch_of(record.get("hold_until"))
        if record.get("handoff") is True:
            handoff_at = epoch_of(record.get("handoff_at"))
            if handoff_at is None:
                handoff_at = epoch_of(record.get("ts"))

    platform = {"terminal": False, "alive_ts": None, "status": None}
    if isinstance(verdicts, dict):
        platform = _platform_hit(verdicts, msgid, now, registered_run)

    handoff_fresh = _fresh(handoff_at, now, HANDOFF_MAX_AGE_SECS)
    extra = {"platform_terminal": platform["terminal"],
             "platform_status": platform["status"],
             "worker_run_id": registered_run,
             "worker_hold_until": hold_until,
             "worker_handoff": handoff_fresh,
             "worker_handoff_at": handoff_at}

    # A live hold outranks everything, terminal verdicts included:
    # the worker declared this silence in advance.
    if hold_until is not None and hold_until > now:
        return {"alive": True, "source": "worker-hold",
                "activity_ts": hold_until, "degraded": degraded, **extra}

    # A fresh handoff outranks a terminal verdict the same way:
    # the worker declared this transfer on purpose, so the batch
    # must not be resumed or fail-stopped while the handoff is
    # still within its day-long window.
    if handoff_fresh:
        return {"alive": True, "source": "worker-handoff",
                "activity_ts": handoff_at, "degraded": degraded, **extra}

    best: tuple[int, float, str] | None = None
    if isinstance(bridge, dict):
        best = _bridge_hit(bridge, msgid, now, window)
    if isinstance(worker, dict):
        ts = _worker_ts(worker, msgid)
        if _fresh(ts, now, window):
            best = _consider(best, 3, ts, "worker-activity")
    if platform["alive_ts"] is not None:
        best = _consider(best, 4, platform["alive_ts"], "platform-running")

    if best is None:
        return {"alive": False, "source": None, "activity_ts": None,
                "degraded": degraded, **extra}
    return {"alive": True, "source": best[2], "activity_ts": best[1],
            "degraded": degraded, **extra}
