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
   mtime does not.

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


def assess(msgid: str, *, now: float, window: float,
           bridge_state_path: str, worker_activity_path: str) -> dict:
    """Say whether msgid still has real activity inside ``window`` seconds.

    Returns ``alive``, ``source``, ``activity_ts``, and ``degraded``.
    ``alive`` is true only for a positive fresh signal. Degraded
    sources contribute no signal.
    """
    degraded: list[str] = []
    bridge, bridge_reason = _read_object(bridge_state_path)
    if bridge_reason:
        degraded.append(bridge_reason)
    worker, worker_reason = _read_object(worker_activity_path)
    if worker_reason:
        degraded.append(worker_reason)

    best: tuple[int, float, str] | None = None
    if isinstance(bridge, dict):
        best = _bridge_hit(bridge, str(msgid), now, window)
    if isinstance(worker, dict):
        ts = _worker_ts(worker, str(msgid))
        if _fresh(ts, now, window):
            best = _consider(best, 3, ts, "worker-activity")

    if best is None:
        return {"alive": False, "source": None, "activity_ts": None,
                "degraded": degraded}
    return {"alive": True, "source": best[2], "activity_ts": best[1],
            "degraded": degraded}
