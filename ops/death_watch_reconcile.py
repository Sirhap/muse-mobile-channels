#!/usr/bin/env python3
"""Read-only observation-window reconciliation for todo 21.

The cold-channel death watch can route early when a platform verdict
says the worker session has ended. This tool reads the hooks state
directory (and, when passed, the bot state directory that actually
holds ``worker_platform_status.json``) and prints a summary. It does
not write state, does not call Muse, and does not change the probe
thresholds or the hook constants it reports.

Usage::

    python3 ops/death_watch_reconcile.py ~/hooks/state \\
        --bot-state ~/workspace

    python3 ops/death_watch_reconcile.py ~/hooks/state/weixin-bot \\
        --bot-state ~/workspace/weixin-bot/state --json

A directory that itself contains ``weixin-bot`` or ``wecom-bot`` is a
parent of channel hook states. Any other directory is one channel.
Verdict, outbox, and worker-activity files are taken from the bot
state when that tree is passed, and from the hooks directory when a
snapshot has copied them there. Hook ledgers (check requests, the
active batch, failed notices) are read from the hooks directory.

Freshness uses ``death_watch_activity``'s own predicate. The silence
gate is read from the inbox hook scripts. Neither number is copied
into a second control knob.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import death_watch_activity as probe


# Phrases the hook appends only when ``_platform_routed_ids`` marked
# the msgid. Resume and fail-stop share the first sentence; fail-stop
# adds the second.
_PLATFORM_NOTICE_MARK = "平台记录显示原执行会话已结束"
_FAIL_STOP_MARK = "自动续跑后仍无推送"

_FORMAL_MODES = frozenset({"reply", "reply_file"})
_CHANNEL_SUBDIRS = ("weixin-bot", "wecom-bot", "weixin", "wecom")
_HOOK_SCRIPTS = ("weixin-inbox.sh", "wecom-inbox.sh")
_GATE_NAMES = (
    "DEATH_WATCH_SECS",
    "EARLY_RESUME_MIN_SILENCE_SECS",
    "EARLY_PLATFORM_MIN_SILENCE_SECS",
    "PLATFORM_CHECK_EVERY_SECS",
)
_GATE_RE = re.compile(
    r"^(" + "|".join(_GATE_NAMES) + r")\s*=\s*(\d+)\s*$",
    re.MULTILINE,
)
# Used only when a hook script cannot be parsed. The report flags
# ``silence_gate_fallback`` so a guessed gate is visible.
_GATE_FALLBACK = {
    "DEATH_WATCH_SECS": 1800,
    "EARLY_RESUME_MIN_SILENCE_SECS": 300,
    "EARLY_PLATFORM_MIN_SILENCE_SECS": 300,
    "PLATFORM_CHECK_EVERY_SECS": 300,
}

_TERMINAL_ORDER = ("completed", "shutdown", "errored", "interrupted")
_STATUS_ORDER = _TERMINAL_ORDER + (
    "running", "unknown", "other", "missing", "invalid",
)
_FRESH_BUCKETS = (
    "lt_60", "60_to_300", "300_to_600", "600_to_1800", "ge_1800",
)
_LEAK_KINDS = (
    "silence_without_verdict",
    "silence_without_registration",
    "verdict_stale_while_silent",
    "verdict_unknown_while_silent",
    "verdict_run_mismatch_while_silent",
    "verdict_other_while_silent",
    "terminal_flip_lag",
)
_CLUE_KINDS = ("terminal_then_formal_reply",) + _LEAK_KINDS

_NOTICE_DEDUPE_SECS = 120.0
# The inbox hook drops platform-check rows once last_request_ts is
# this old. A registration older than the main death watch, and not
# still listed in that ledger or in the active batch, is outside the
# observation window.
_REQUEST_LEDGER_SECS = 7200.0


class ReconcileError(Exception):
    """A caller error. ``code`` is the process exit status."""

    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _safe_resolve(path: Path) -> Path:
    """Absolute path. A broken symlink stays as given."""
    try:
        return path.resolve()
    except OSError:
        return path


def _read_text(path: Path) -> str:
    """Read one text file. ``OSError`` and ``UnicodeError`` propagate."""
    with path.open(encoding="utf-8") as handle:
        return handle.read()


def _read_object(path: Path) -> tuple[dict | None, str | None]:
    """Return ``(object, degraded)``. Missing is ``(None, None)``."""
    try:
        data = json.loads(_read_text(path))
    except FileNotFoundError:
        return None, None
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None, "unreadable:" + path.name
    if not isinstance(data, dict):
        return None, "not-object:" + path.name
    return data, None


def _read_jsonl(path: Path) -> tuple[list[dict], str | None]:
    """Object rows from a jsonl file. Bad lines are skipped and noted."""
    try:
        text = _read_text(path)
    except FileNotFoundError:
        return [], None
    except (OSError, UnicodeError):
        return [], "unreadable:" + path.name
    rows: list[dict] = []
    bad = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            item = json.loads(stripped)
        except json.JSONDecodeError:
            bad += 1
            continue
        if isinstance(item, dict):
            rows.append(item)
        else:
            bad += 1
    if bad:
        return rows, f"partial:{path.name}:{bad}"
    return rows, None


def _row_ts(row: dict) -> float | None:
    """Outbox clock: ``queued_at``, then ``ts``. Non-positive is absent."""
    for key in ("queued_at", "ts"):
        stamp = probe.epoch_of(row.get(key))
        if stamp is not None:
            return stamp
    return None


def _load_script_gates(repo_root: Path) -> tuple[dict[str, dict[str, int]], bool]:
    """Hook integers keyed by script file name.

    ``fallback`` is true when any required assignment could not be
    read. Missing keys then use ``_GATE_FALLBACK`` so the report can
    still name a gate, and the flag tells the reader it was not read
    from the script.
    """
    loaded: dict[str, dict[str, int]] = {}
    fallback = False
    for name in _HOOK_SCRIPTS:
        path = repo_root / "hooks" / "scripts" / name
        found: dict[str, int] = {}
        try:
            text = _read_text(path)
        except (OSError, UnicodeError):
            fallback = True
            text = ""
        for match in _GATE_RE.finditer(text):
            found[match.group(1)] = int(match.group(2))
        if any(key not in found for key in _GATE_NAMES):
            fallback = True
            for key, value in _GATE_FALLBACK.items():
                found.setdefault(key, value)
        loaded[name] = found
    return loaded, fallback


def _script_for_channel(channel: str) -> str:
    """Inbox script whose gates apply to this channel directory."""
    if "wecom" in channel:
        return "wecom-inbox.sh"
    return "weixin-inbox.sh"


def _existing_dirs(candidates: list[Path]) -> tuple[Path, ...]:
    """Directories that exist, in the given order, without duplicates."""
    chosen: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        try:
            is_dir = path.is_dir()
        except OSError:
            continue
        if not is_dir:
            continue
        key = _safe_resolve(path)
        if key in seen:
            continue
        seen.add(key)
        chosen.append(path)
    return tuple(chosen)


def _search_dirs(channel: str, hooks_dir: Path,
                 bot_state: Path | None, *, single: bool) -> tuple[Path, ...]:
    """Bot state first, then the hooks directory.

    Production keeps verdicts and the outbox under the bot state and
    the check-request ledger under the hooks state. A fixture may put
    every file in the hooks directory; that directory is always
    searched. ``bot_state`` itself is searched only for a single
    channel, so a workspace root is not attributed to every channel.
    """
    candidates: list[Path] = []
    if bot_state is not None:
        names = [channel]
        if channel in ("weixin", "wecom"):
            names.append(channel + "-bot")
        for name in names:
            candidates.append(bot_state / name / "state")
            candidates.append(bot_state / name)
        if single:
            candidates.append(bot_state / "state")
            candidates.append(bot_state)
    candidates.append(hooks_dir)
    candidates.append(hooks_dir / "state")
    return _existing_dirs(candidates)


def _discover(hooks_state: Path, bot_state: Path | None) -> list[dict]:
    """One record per channel. A parent directory expands to its children."""
    children: list[tuple[str, Path]] = []
    for name in _CHANNEL_SUBDIRS:
        child = hooks_state / name
        try:
            is_dir = child.is_dir()
        except OSError:
            continue
        if is_dir:
            children.append((name, child))
    single = not children
    if single:
        children = [(hooks_state.name, hooks_state)]
    found = []
    for name, hooks_dir in children:
        found.append({
            "name": name,
            "hooks_dir": hooks_dir,
            "search_dirs": _search_dirs(
                name, hooks_dir, bot_state, single=single),
        })
    return found


def _find_named(dirs: tuple[Path, ...], filename: str) -> list[Path]:
    """Existing files of this name, search order, one path per inode."""
    found: list[Path] = []
    seen: set[Path] = set()
    for directory in dirs:
        path = directory / filename
        try:
            is_file = path.is_file()
        except OSError:
            continue
        if not is_file:
            continue
        key = _safe_resolve(path)
        if key in seen:
            continue
        seen.add(key)
        found.append(path)
    return found


def _first_named(dirs: tuple[Path, ...], filename: str) -> Path | None:
    found = _find_named(dirs, filename)
    if not found:
        return None
    return found[0]


def _empty_counts(keys: tuple[str, ...]) -> dict[str, int]:
    return {key: 0 for key in keys}


def _status_bucket(status: str) -> str:
    """Census bucket for one verdict status word."""
    if status in probe.PLATFORM_TERMINAL_STATUSES:
        return status
    if status == "running":
        return "running"
    if status == "unknown":
        return "unknown"
    if status == "":
        return "missing"
    return "other"


def _fresh_bucket(checked: float | None, now: float) -> str:
    """Bucket name for ``checked_at``.

    ``fresh`` buckets are exactly the ages ``probe._fresh`` accepts
    for ``PLATFORM_VERDICT_MAX_AGE_SECS``. Age equal to the TTL is
    already stale.
    """
    if checked is None:
        return "missing"
    age = now - checked
    if age < -probe._FUTURE_SKEW_SECS:
        return "future_rejected"
    if age < 60:
        return "lt_60"
    if age < 300:
        return "60_to_300"
    if probe._fresh(checked, now, probe.PLATFORM_VERDICT_MAX_AGE_SECS):
        return "300_to_600"
    if age < 1800:
        return "600_to_1800"
    return "ge_1800"


def _worker_view(raw: object) -> dict:
    """Normalize one worker-activity value. Bare epochs have no run id."""
    if isinstance(raw, dict):
        run = raw.get("run_id")
        run_id = run if isinstance(run, str) and run else None
        return {
            "ts": probe.epoch_of(raw.get("ts")),
            "run_id": run_id,
            "hold_until": probe.epoch_of(raw.get("hold_until")),
        }
    return {"ts": probe.epoch_of(raw), "run_id": None, "hold_until": None}


def _run_mismatch(raw: dict, registered: str | None) -> bool:
    """Same ignore rule as ``probe._platform_hit``."""
    verdict_run = raw.get("run_id")
    return bool(
        registered
        and isinstance(verdict_run, str)
        and verdict_run
        and verdict_run != registered
    )


def _verdict_label(raw: object, registered: str | None, now: float) -> str:
    """Why this record is or is not usable terminal evidence.

    Labels that mean the probe would still treat the msgid as a fresh
    terminal or as platform-running are ``usable_terminal`` and
    ``running_fresh``. Everything else is not an early-route signal.
    """
    if raw is None:
        return "absent"
    if not isinstance(raw, dict):
        return "invalid"
    if _run_mismatch(raw, registered):
        return "run_mismatch"
    checked = probe.epoch_of(raw.get("checked_at"))
    status = str(raw.get("status") or "")
    if not probe._fresh(checked, now, probe.PLATFORM_VERDICT_MAX_AGE_SECS):
        if checked is None:
            return "missing_checked_at"
        age = now - checked
        if age < -probe._FUTURE_SKEW_SECS:
            return "future:" + (status or "missing")
        return "stale:" + (status or "missing")
    if status == "running":
        updated = probe.epoch_of(raw.get("agent_updated_at"))
        if probe._fresh(updated, now, probe.PLATFORM_RUNNING_FRESH_SECS):
            return "running_fresh"
        return "running_update_stale"
    if status in probe.PLATFORM_TERMINAL_STATUSES:
        return "usable_terminal"
    if status == "unknown":
        return "unknown"
    if status == "":
        return "missing_status"
    return "other:" + status


def _index_outbox(rows: list[dict]) -> dict[str, list[dict]]:
    """Outbox rows keyed by msgid. Rows without a msgid are not indexed."""
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        msgid = str(row.get("msgid") or "")
        if not msgid:
            continue
        grouped.setdefault(msgid, []).append(row)
    return grouped


def _delivered_formal_ids(outbox_rows: list[dict],
                          result_rows: list[dict]) -> set[str]:
    """Outbox ids whose result row is an ok formal reply."""
    known = {str(row.get("id")) for row in outbox_rows if row.get("id")}
    delivered: set[str] = set()
    for row in result_rows:
        if row.get("ok") is not True:
            continue
        if row.get("mode") not in _FORMAL_MODES:
            continue
        item_id = str(row.get("id") or "")
        if item_id and item_id in known:
            delivered.add(item_id)
    return delivered


def _parked_formals(data: dict | None) -> list[dict]:
    """Formal replies sitting in the gateway park lane."""
    if not isinstance(data, dict):
        return []
    events: list[dict] = []
    for rec in data.values():
        if not isinstance(rec, dict):
            continue
        item = rec.get("item")
        if not isinstance(item, dict):
            continue
        if item.get("mode") not in _FORMAL_MODES:
            continue
        msgid = str(item.get("msgid") or "")
        if not msgid:
            continue
        stamp = _row_ts(item)
        if stamp is None:
            stamp = probe.epoch_of(rec.get("parked_at"))
        if stamp is None:
            continue
        item_id = str(item.get("id") or "") or None
        events.append({
            "msgid": msgid,
            "mode": item.get("mode"),
            "ts": stamp,
            "id": item_id,
            "delivered": False,
            "source": "parked",
        })
    return events


def _formal_replies(msgid: str, outbox_rows: list[dict],
                    parked: list[dict], delivered_ids: set[str]) -> list[dict]:
    """Formal replies for one msgid, outbox then park, de-duplicated by id."""
    events: list[dict] = []
    seen: set[str] = set()
    for row in outbox_rows:
        if str(row.get("msgid") or "") != msgid:
            continue
        if row.get("mode") not in _FORMAL_MODES:
            continue
        stamp = _row_ts(row)
        if stamp is None:
            continue
        item_id = str(row.get("id") or "")
        events.append({
            "mode": row.get("mode"),
            "ts": stamp,
            "id": item_id or None,
            "delivered": bool(item_id and item_id in delivered_ids),
            "source": "outbox",
        })
        if item_id:
            seen.add(item_id)
    for event in parked:
        if event["msgid"] != msgid:
            continue
        item_id = event.get("id") or ""
        if item_id and item_id in seen:
            continue
        events.append({
            "mode": event["mode"],
            "ts": event["ts"],
            "id": event.get("id"),
            "delivered": False,
            "source": "parked",
        })
    events.sort(key=lambda item: (item["ts"], item.get("id") or ""))
    return events


def _batch_groups(batch: dict | None) -> list[dict]:
    """Active batch, then detached batches. Msgids are not repeated."""
    if not isinstance(batch, dict):
        return []
    specs: list[tuple[str, object, object]] = []
    specs.append(("active", batch.get("msgids"), batch.get("since")))
    detached = batch.get("detached")
    if isinstance(detached, list):
        for item in detached:
            if isinstance(item, dict):
                specs.append(("detached", item.get("msgids"), item.get("since")))
    groups: list[dict] = []
    covered: set[str] = set()
    for role, raw_ids, raw_since in specs:
        since = probe.epoch_of(raw_since)
        if since is None:
            continue
        if not isinstance(raw_ids, list):
            continue
        msgids = []
        for raw in raw_ids:
            msgid = str(raw or "")
            if not msgid or msgid in covered:
                continue
            msgids.append(msgid)
            covered.add(msgid)
        if not msgids:
            continue
        groups.append({"role": role, "msgids": msgids, "since": since})
    return groups


def _silence_secs(msgids: list[str], since: float | None,
                  outbox_by_msgid: dict[str, list[dict]],
                  parked: list[dict], now: float) -> float | None:
    """Seconds since the last in-batch push. ``None`` when no clock exists.

    Batch groups follow the hook: the clock starts at ``since`` and
    only outbox rows at or after ``since - 2`` move it. Parked formal
    replies move it too. Synthetic groups have no ``since``; the clock
    is the newest outbox or parked stamp.
    """
    if since is None:
        stamps: list[float] = []
        for msgid in msgids:
            for row in outbox_by_msgid.get(msgid, []):
                stamp = _row_ts(row)
                if stamp is not None:
                    stamps.append(stamp)
            for event in parked:
                if event["msgid"] == msgid:
                    stamps.append(event["ts"])
        if not stamps:
            return None
        return now - max(stamps)
    last = since
    for msgid in msgids:
        for row in outbox_by_msgid.get(msgid, []):
            stamp = _row_ts(row)
            if stamp is not None and stamp >= since - 2 and stamp > last:
                last = stamp
        for event in parked:
            if event["msgid"] == msgid and event["ts"] > last:
                last = event["ts"]
    return now - last


def _request_run(raw: object) -> str | None:
    if not isinstance(raw, dict):
        return None
    run = raw.get("run_id")
    if isinstance(run, str) and run:
        return run
    return None


def _request_stage(msgids: list[str], requests: dict,
                   now: float, every: float) -> str:
    """How long the checker has had a request for this group."""
    ages: list[float] = []
    for msgid in msgids:
        raw = requests.get(msgid)
        if not isinstance(raw, dict):
            continue
        stamp = probe.epoch_of(raw.get("requested_at"))
        if stamp is None:
            stamp = probe.epoch_of(raw.get("last_request_ts"))
        if stamp is None:
            continue
        ages.append(now - stamp)
    if not ages:
        return "no_request"
    if min(ages) < every:
        return "awaiting_checker"
    return "request_unanswered"


def _leak_kind(labels: list[str], has_registration: bool) -> str:
    """One clue kind for a group that has no usable terminal evidence."""
    if not has_registration and all(label == "absent" for label in labels):
        return "silence_without_registration"
    if any(label == "running_update_stale" for label in labels):
        return "terminal_flip_lag"
    if any(label == "run_mismatch" for label in labels):
        return "verdict_run_mismatch_while_silent"
    if any(label == "unknown" for label in labels):
        return "verdict_unknown_while_silent"
    if any(label.startswith("other:") for label in labels):
        return "verdict_other_while_silent"
    stale_marks = ("missing_checked_at", "invalid", "missing_status")
    if any(
        label in stale_marks or label.startswith("stale:")
        or label.startswith("future:")
        for label in labels
    ):
        return "verdict_stale_while_silent"
    if not has_registration:
        return "silence_without_registration"
    return "silence_without_verdict"


def _notice_text(row: dict) -> str:
    for key in ("content", "text"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _notice_kind(text: str) -> str:
    if _FAIL_STOP_MARK in text:
        return "fail_stop"
    return "resume"


def _excerpt(text: str) -> str:
    flat = " ".join(text.split())
    if len(flat) <= 80:
        return flat
    return flat[:80]


def _collect_triggers(channel: str, outbox_rows: list[dict],
                      failed_raw: object) -> tuple[list[dict], str | None]:
    """EARLY_PLATFORM notices. A failed notice already queued is not counted twice."""
    events: list[dict] = []
    seen_ids: set[str] = set()
    for row in outbox_rows:
        if row.get("mode") != "send":
            continue
        text = _notice_text(row)
        if _PLATFORM_NOTICE_MARK not in text:
            continue
        item_id = str(row.get("id") or "")
        events.append({
            "channel": channel,
            "kind": _notice_kind(text),
            "source": "outbox",
            "ts": _row_ts(row),
            "id": item_id or None,
            "text_excerpt": _excerpt(text),
        })
        if item_id:
            seen_ids.add(item_id)
    degraded = None
    notices: list[dict] = []
    if failed_raw is None:
        notices = []
    elif isinstance(failed_raw, list):
        notices = [item for item in failed_raw if isinstance(item, dict)]
    elif isinstance(failed_raw, dict):
        degraded = "not-list:failed_notices.json"
    else:
        degraded = "not-list:failed_notices.json"
    for note in notices:
        text = _notice_text(note)
        if _PLATFORM_NOTICE_MARK not in text:
            continue
        queued_id = str(note.get("queued_id") or "")
        if queued_id and queued_id in seen_ids:
            continue
        note_ts = probe.epoch_of(note.get("ts"))
        duplicate = False
        for event in events:
            if event["source"] != "outbox":
                continue
            if event["text_excerpt"] != _excerpt(text):
                continue
            event_ts = event["ts"] if event["ts"] is not None else 0.0
            stamp = note_ts if note_ts is not None else 0.0
            if abs(event_ts - stamp) <= _NOTICE_DEDUPE_SECS:
                duplicate = True
                break
        if duplicate:
            continue
        events.append({
            "channel": channel,
            "kind": _notice_kind(text),
            "source": "failed_notices",
            "ts": note_ts,
            "id": queued_id or None,
            "text_excerpt": _excerpt(text),
        })
    return events, degraded


def _census_file(path: Path, now: float, worker: dict[str, dict],
                 status_counts: dict[str, int], other_counts: dict[str, int],
                 fresh_counts: dict[str, int],
                 running_counts: dict[str, int]) -> dict:
    """Fold one verdict file into the shared counters. Returns a file row."""
    data, reason = _read_object(path)
    row = {
        "path": str(path),
        "readable": data is not None,
        "degraded": reason,
        "record_count": 0,
    }
    if data is None:
        return row
    for msgid, raw in data.items():
        row["record_count"] += 1
        if not isinstance(raw, dict):
            status_counts["invalid"] += 1
            fresh_counts["missing"] += 1
            continue
        status = str(raw.get("status") or "")
        bucket = _status_bucket(status)
        status_counts[bucket] += 1
        if bucket == "other":
            other_counts[status] = other_counts.get(status, 0) + 1
        fresh_counts[_fresh_bucket(probe.epoch_of(raw.get("checked_at")), now)] += 1
        if status != "running":
            continue
        running_counts["running_records"] += 1
        checked_fresh = probe._fresh(
            probe.epoch_of(raw.get("checked_at")), now,
            probe.PLATFORM_VERDICT_MAX_AGE_SECS)
        updated = probe.epoch_of(raw.get("agent_updated_at"))
        registered = worker.get(str(msgid), {}).get("run_id")
        if _run_mismatch(raw, registered):
            running_counts["run_mismatch"] += 1
            continue
        if not checked_fresh:
            running_counts["checked_stale"] += 1
            continue
        if updated is None:
            running_counts["checked_fresh_update_missing"] += 1
        elif probe._fresh(updated, now, probe.PLATFORM_RUNNING_FRESH_SECS):
            running_counts["checked_fresh_update_fresh"] += 1
        else:
            running_counts["checked_fresh_update_stale"] += 1
    return row


def _preferred_verdicts(paths: list[Path]) -> tuple[dict[str, dict], str | None, list[str]]:
    """Msgid map from the first readable file. Later files stay in the census only."""
    degraded: list[str] = []
    for path in paths:
        data, reason = _read_object(path)
        if reason:
            degraded.append(reason)
        if not isinstance(data, dict):
            continue
        chosen: dict[str, dict] = {}
        for msgid, raw in data.items():
            if isinstance(raw, dict):
                chosen[str(msgid)] = raw
        return chosen, str(path), degraded
    return {}, None, degraded


def _alive_veto(msgids: list[str], worker: dict[str, dict],
                labels: dict[str, str], bridge: dict | None,
                now: float, window: float) -> str | None:
    """Reason the hook would not be waiting on a terminal verdict."""
    for msgid in msgids:
        hold = worker.get(msgid, {}).get("hold_until")
        if isinstance(hold, float) and hold > now:
            return "live_hold"
    for msgid in msgids:
        stamp = worker.get(msgid, {}).get("ts")
        if probe._fresh(stamp, now, window):
            return "worker_activity_fresh"
    for msgid in msgids:
        if labels.get(msgid) == "usable_terminal":
            return "usable_terminal"
        if labels.get(msgid) == "running_fresh":
            return "platform_running_fresh"
    if isinstance(bridge, dict):
        for msgid in msgids:
            if probe._bridge_hit(bridge, msgid, now, window) is not None:
                return "bridge_alive"
    return None


def _has_formal(msgids: list[str], outbox_rows: list[dict],
                parked: list[dict], delivered_ids: set[str],
                since: float | None) -> bool:
    """True when a formal reply lands inside this batch's clock.

    Replies older than ``since - 2`` belong to an earlier batch. The
    hook uses that same margin when it moves ``last_activity``.
    """
    for msgid in msgids:
        for event in _formal_replies(msgid, outbox_rows, parked, delivered_ids):
            if since is None or event["ts"] >= since - 2:
                return True
    return False


def _synthetic_groups(covered: set[str], worker: dict[str, dict],
                      requests: dict) -> list[dict]:
    """Msgids that are registered or requested but not in the active batch."""
    wanted = set(worker) | {str(key) for key in requests if str(key)}
    wanted -= covered
    wanted.discard("")
    groups: list[dict] = []
    for msgid in sorted(wanted):
        view = worker.get(msgid)
        has_run = bool(view and view.get("run_id")) or _request_run(requests.get(msgid)) is not None
        if not has_run and msgid not in requests:
            continue
        groups.append({"role": "registered", "msgids": [msgid], "since": None})
    return groups


def _route_window(silence: float, gates: dict[str, int]) -> float:
    """Match the hook: early floor until the 1800s watch, then the full window."""
    death = float(gates["DEATH_WATCH_SECS"])
    early = float(gates["EARLY_RESUME_MIN_SILENCE_SECS"])
    if silence < death:
        return early
    return death


def _reconcile_channel(channel: dict, gates: dict[str, int],
                       bridge: dict | None, bridge_unchecked: bool,
                       now: float) -> dict:
    """Census, triggers, and clues for one channel. Reads files only."""
    dirs = channel["search_dirs"]
    name = channel["name"]
    degraded: list[str] = []

    worker_path = _first_named(dirs, "worker_activity.json")
    worker_raw, worker_reason = ({}, None)
    if worker_path is not None:
        worker_raw, worker_reason = _read_object(worker_path)
        if worker_reason:
            degraded.append(worker_reason)
    worker: dict[str, dict] = {}
    if isinstance(worker_raw, dict):
        for msgid, raw in worker_raw.items():
            worker[str(msgid)] = _worker_view(raw)

    verdict_paths = _find_named(dirs, "worker_platform_status.json")
    status_counts = _empty_counts(_STATUS_ORDER)
    other_counts: dict[str, int] = {}
    fresh_counts = _empty_counts(_FRESH_BUCKETS + ("missing", "future_rejected"))
    running_counts = {
        "running_records": 0,
        "checked_fresh_update_fresh": 0,
        "checked_fresh_update_stale": 0,
        "checked_fresh_update_missing": 0,
        "checked_stale": 0,
        "run_mismatch": 0,
    }
    verdict_files = [
        _census_file(path, now, worker, status_counts, other_counts,
                     fresh_counts, running_counts)
        for path in verdict_paths
    ]
    for item in verdict_files:
        if item["degraded"]:
            degraded.append(item["degraded"])
    preferred, preferred_path, preferred_notes = _preferred_verdicts(verdict_paths)
    for note in preferred_notes:
        if note not in degraded:
            degraded.append(note)

    outbox_path = _first_named(dirs, "outbox.jsonl")
    outbox_rows: list[dict] = []
    if outbox_path is not None:
        outbox_rows, outbox_reason = _read_jsonl(outbox_path)
        if outbox_reason:
            degraded.append(outbox_reason)
    results_path = _first_named(dirs, "outbox_results.jsonl")
    result_rows: list[dict] = []
    if results_path is not None:
        result_rows, results_reason = _read_jsonl(results_path)
        if results_reason:
            degraded.append(results_reason)
    parked_path = _first_named(dirs, "outbox_parked.json")
    parked_raw = None
    if parked_path is not None:
        parked_raw, parked_reason = _read_object(parked_path)
        if parked_reason:
            degraded.append(parked_reason)
    parked = _parked_formals(parked_raw)
    delivered_ids = _delivered_formal_ids(outbox_rows, result_rows)
    outbox_by_msgid = _index_outbox(outbox_rows)

    batch_path = _first_named(dirs, "active_batch.json")
    batch = None
    if batch_path is not None:
        batch, batch_reason = _read_object(batch_path)
        if batch_reason:
            degraded.append(batch_reason)
    request_path = _first_named(dirs, "platform_check_requests.json")
    request_doc = None
    if request_path is not None:
        request_doc, request_reason = _read_object(request_path)
        if request_reason:
            degraded.append(request_reason)
    requests: dict = {}
    if isinstance(request_doc, dict) and isinstance(request_doc.get("requests"), dict):
        requests = request_doc["requests"]
    elif isinstance(request_doc, dict) and request_doc:
        degraded.append("not-requests:platform_check_requests.json")

    failed_path = _first_named(dirs, "failed_notices.json")
    failed_raw: object = None
    if failed_path is not None:
        try:
            failed_raw = json.loads(_read_text(failed_path))
        except FileNotFoundError:
            failed_raw = None
        except (OSError, json.JSONDecodeError, UnicodeError):
            degraded.append("unreadable:failed_notices.json")
            failed_raw = None
    triggers, trigger_reason = _collect_triggers(name, outbox_rows, failed_raw)
    if trigger_reason:
        degraded.append(trigger_reason)

    usable: list[str] = []
    for msgid, raw in preferred.items():
        registered = worker.get(msgid, {}).get("run_id")
        hit = probe._platform_hit({msgid: raw}, msgid, now, registered)
        if hit.get("terminal"):
            usable.append(msgid)
    usable.sort()

    false_positives: list[dict] = []
    for msgid, raw in preferred.items():
        status = str(raw.get("status") or "")
        if status not in probe.PLATFORM_TERMINAL_STATUSES:
            continue
        checked = probe.epoch_of(raw.get("checked_at"))
        if checked is None:
            continue
        later = [
            event for event in _formal_replies(
                msgid, outbox_rows, parked, delivered_ids)
            if event["ts"] > checked
        ]
        if not later:
            continue
        false_positives.append({
            "kind": "terminal_then_formal_reply",
            "channel": name,
            "msgid": msgid,
            "verdict_status": status,
            "verdict_path": preferred_path,
            "checked_at": checked,
            "run_id": raw.get("run_id") if isinstance(raw.get("run_id"), str) else None,
            "replies": later,
        })

    groups = _batch_groups(batch if isinstance(batch, dict) else None)
    covered = {msgid for group in groups for msgid in group["msgids"]}
    groups.extend(_synthetic_groups(covered, worker, requests))

    silence_gate = float(gates["EARLY_PLATFORM_MIN_SILENCE_SECS"])
    check_every = float(gates["PLATFORM_CHECK_EVERY_SECS"])
    death_watch = float(gates["DEATH_WATCH_SECS"])
    leaks: list[dict] = []
    for group in groups:
        msgids = group["msgids"]
        silence = _silence_secs(
            msgids, group["since"], outbox_by_msgid, parked, now)
        if group["since"] is None and (silence is None or silence < silence_gate):
            # A registration with no outbox row still has a worker timestamp.
            view = worker.get(msgids[0], {})
            if silence is None and view.get("ts") is not None:
                silence = now - view["ts"]
            elif silence is None:
                raw_request = requests.get(msgids[0])
                if isinstance(raw_request, dict):
                    requested = probe.epoch_of(raw_request.get("requested_at"))
                    if requested is None:
                        requested = probe.epoch_of(raw_request.get("last_request_ts"))
                    recorded = raw_request.get("silence_secs")
                    if (
                        requested is not None
                        and isinstance(recorded, (int, float))
                        and not isinstance(recorded, bool)
                    ):
                        silence = float(recorded) + (now - requested)
        if silence is None or silence < silence_gate:
            continue
        has_request = any(isinstance(requests.get(msgid), dict) for msgid in msgids)
        if group["role"] == "registered" and not has_request and silence >= death_watch:
            # Not in a live batch and already past the main watch.
            # The early route is no longer the path that would speak.
            continue
        if (
            group["role"] == "registered"
            and has_request
            and _request_stage(msgids, requests, now, _REQUEST_LEDGER_SECS)
            == "request_unanswered"
            and silence >= _REQUEST_LEDGER_SECS
        ):
            continue
        if _has_formal(msgids, outbox_rows, parked, delivered_ids, group["since"]):
            continue
        labels = {}
        run_ids = {}
        for msgid in msgids:
            registered = worker.get(msgid, {}).get("run_id")
            labels[msgid] = _verdict_label(preferred.get(msgid), registered, now)
            run_id = registered or _request_run(requests.get(msgid))
            if run_id:
                run_ids[msgid] = run_id
        window = _route_window(silence, gates)
        veto = _alive_veto(msgids, worker, labels, bridge, now, window)
        if veto is not None:
            continue
        kind = _leak_kind(list(labels.values()), bool(run_ids))
        stage = None
        if kind in ("silence_without_verdict", "silence_without_registration"):
            stage = _request_stage(msgids, requests, now, check_every)
        leaks.append({
            "kind": kind,
            "channel": name,
            "role": group["role"],
            "msgids": list(msgids),
            "silence_secs": silence,
            "silence_gate_secs": silence_gate,
            "also_past_death_watch": silence >= death_watch,
            "verdicts": labels,
            "run_ids": run_ids,
            "stage": stage,
            "bridge_unchecked": bridge_unchecked,
            "request_counts": {
                msgid: int(requests[msgid].get("count") or 0)
                for msgid in msgids
                if isinstance(requests.get(msgid), dict)
            },
        })

    def _path_or_none(path: Path | None) -> str | None:
        if path is None:
            return None
        return str(path)

    return {
        "name": name,
        "inputs": {
            "verdicts": [str(path) for path in verdict_paths],
            "worker_activity": _path_or_none(worker_path),
            "outbox": _path_or_none(outbox_path),
            "outbox_results": _path_or_none(results_path),
            "outbox_parked": _path_or_none(parked_path),
            "active_batch": _path_or_none(batch_path),
            "platform_check_requests": _path_or_none(request_path),
            "failed_notices": _path_or_none(failed_path),
        },
        "verdict_files": verdict_files,
        "verdict_record_count": sum(item["record_count"] for item in verdict_files),
        "status_counts": status_counts,
        "other_status_counts": other_counts,
        "freshness": fresh_counts,
        "running_freshness": running_counts,
        "usable_terminal_msgids": usable,
        "preferred_verdict_path": preferred_path,
        "triggers": triggers,
        "false_positive_clues": false_positives,
        "false_negative_clues": leaks,
        "degraded": degraded,
        "gates": gates,
    }


def _sum_counts(rows: list[dict], key: str, fields: tuple[str, ...] | list[str]) -> dict[str, int]:
    total = _empty_counts(tuple(fields))
    for row in rows:
        counts = row.get(key) or {}
        for field in fields:
            total[field] += int(counts.get(field) or 0)
    return total


def _threshold_block(script_gates: dict[str, dict[str, int]],
                     channels: list[dict], fallback: bool) -> dict:
    """Top-level gates. A key is null when the channel scripts disagree."""
    per_channel = {
        channel["name"]: {
            "script": _script_for_channel(channel["name"]),
            "death_watch_secs": channel["gates"]["DEATH_WATCH_SECS"],
            "early_resume_min_silence_secs": channel["gates"]["EARLY_RESUME_MIN_SILENCE_SECS"],
            "early_platform_min_silence_secs": channel["gates"]["EARLY_PLATFORM_MIN_SILENCE_SECS"],
            "platform_check_every_secs": channel["gates"]["PLATFORM_CHECK_EVERY_SECS"],
        }
        for channel in channels
    }
    agreed: dict[str, int | None] = {}
    scripts = [script_gates[name] for name in _HOOK_SCRIPTS]
    for key in _GATE_NAMES:
        values = {item[key] for item in scripts}
        agreed[key] = next(iter(values)) if len(values) == 1 else None
    return {
        "verdict_ttl_secs": probe.PLATFORM_VERDICT_MAX_AGE_SECS,
        "running_fresh_secs": probe.PLATFORM_RUNNING_FRESH_SECS,
        "future_skew_secs": probe._FUTURE_SKEW_SECS,
        "terminal_statuses": sorted(probe.PLATFORM_TERMINAL_STATUSES),
        "death_watch_secs": agreed["DEATH_WATCH_SECS"],
        "early_resume_min_silence_secs": agreed["EARLY_RESUME_MIN_SILENCE_SECS"],
        "early_platform_min_silence_secs": agreed["EARLY_PLATFORM_MIN_SILENCE_SECS"],
        "platform_check_every_secs": agreed["PLATFORM_CHECK_EVERY_SECS"],
        "silence_gates_agree": all(value is not None for value in agreed.values()),
        "silence_gate_fallback": fallback,
        "silence_gate_source": [
            "hooks/scripts/" + name for name in _HOOK_SCRIPTS
        ],
        "per_channel": per_channel,
    }


def _definitions() -> dict[str, str]:
    """Statistical definitions, kept next to the code that implements them."""
    return {
        "verdict_file": (
            "worker_platform_status.json。一个渠道一份快照，按 msgid 覆盖写，不是历史。"
            "同一渠道若搜到多份，条目分布按每份累加；线索和「探针视为平台终态」"
            "只看搜索顺序里的第一份（bot state 优先于 hooks 目录）。"
        ),
        "terminal_status": "completed、shutdown、errored、interrupted。unknown 与组外词不是终态。",
        "fresh_verdict": "checked_at 的年龄小于裁决 TTL，且不早于未来容差。年龄等于 TTL 算过期。",
        "running_fresh": "status 为 running，且 checked_at 未过期，且 agent_updated_at 的年龄小于 running 新鲜窗口。",
        "early_platform_trigger": (
            "outbox 里还留着的 mode=send 行，或 failed_notices 里尚未对上 "
            "outbox id 的条目，文本含「平台记录显示原执行会话已结束」。"
            "续跑通知与停止通知各算一次。outbox 被截断后会计少。"
        ),
        "false_positive": "快照里的终态裁决之后，同一 msgid 仍有 reply 或 reply_file（含停在 park 车道的正式回复）。",
        "false_negative": "自推静默达到 EARLY_PLATFORM_MIN_SILENCE_SECS，没有正式回复，没有活 hold，没有早期窗口内的工人/桥/running 鲜活信号，也没有可用终态裁决。已有正式回复行的批次不算漏报。",
        "awaiting_checker": "漏报里裁决缺失、且核查请求年龄小于 PLATFORM_CHECK_EVERY_SECS 的条目。checker 可能还没跑到，不计入硬漏报。",
        "terminal_flip_lag": "裁决 checked_at 仍新鲜，但 status 仍是 running 且 agent_updated_at 已不新鲜，同时自推已经静默超过提前门。",
    }


def reconcile(hooks_state: str | Path, *, bot_state: str | Path | None = None,
              bridge_state: str | Path | None = None, now: float | None = None,
              repo_root: str | Path | None = None) -> dict:
    """Build the observation report. Reads the given trees and returns a dict.

    Raises ``ReconcileError`` when ``hooks_state`` is not a directory.
    """
    hooks_path = Path(hooks_state)
    try:
        hooks_ok = hooks_path.is_dir()
    except OSError as exc:
        raise ReconcileError(f"无法读取 hooks 状态目录: {hooks_path}") from exc
    if not hooks_ok:
        raise ReconcileError(f"hooks 状态目录不存在: {hooks_path}")
    bot_path = Path(bot_state) if bot_state else None
    if bot_path is not None:
        try:
            bot_ok = bot_path.is_dir()
        except OSError as exc:
            raise ReconcileError(f"无法读取 bot 状态目录: {bot_path}") from exc
        if not bot_ok:
            raise ReconcileError(f"bot 状态目录不存在: {bot_path}")
    moment = time.time() if now is None else float(now)
    root = Path(repo_root) if repo_root is not None else _REPO_ROOT
    script_gates, fallback = _load_script_gates(root)

    bridge: dict | None = None
    bridge_unchecked = True
    bridge_degraded: list[str] = []
    bridge_text = None
    if bridge_state:
        bridge_file = Path(bridge_state)
        bridge_text = str(bridge_file)
        data, reason = _read_object(bridge_file)
        if reason:
            bridge_degraded.append(reason)
        elif data is not None:
            bridge = data
            bridge_unchecked = False
        else:
            bridge_degraded.append("missing:" + bridge_file.name)
            bridge_unchecked = True

    channels_in = _discover(hooks_path, bot_path)
    channels = []
    for channel in channels_in:
        script = _script_for_channel(channel["name"])
        gates = script_gates.get(script) or _GATE_FALLBACK
        channels.append(_reconcile_channel(
            channel, gates, bridge, bridge_unchecked, moment))

    status_counts = _sum_counts(channels, "status_counts", _STATUS_ORDER)
    fresh_fields = list(_FRESH_BUCKETS) + ["missing", "future_rejected"]
    freshness = _sum_counts(channels, "freshness", fresh_fields)
    running_fields = [
        "running_records", "checked_fresh_update_fresh",
        "checked_fresh_update_stale", "checked_fresh_update_missing",
        "checked_stale", "run_mismatch",
    ]
    running = _sum_counts(channels, "running_freshness", running_fields)
    verdict_files = [item for channel in channels for item in channel["verdict_files"]]
    triggers = [item for channel in channels for item in channel["triggers"]]
    false_positives = [
        item for channel in channels for item in channel["false_positive_clues"]
    ]
    false_negatives = [
        item for channel in channels for item in channel["false_negative_clues"]
    ]
    triggers.sort(key=lambda item: (item["channel"], item["ts"] or 0.0, item["kind"], item["text_excerpt"]))
    false_positives.sort(key=lambda item: (item["channel"], item["msgid"], item["checked_at"]))
    false_negatives.sort(key=lambda item: (item["kind"], item["channel"], ",".join(item["msgids"])))

    clue_counts = _empty_counts(_CLUE_KINDS)
    for item in false_positives:
        clue_counts[item["kind"]] = clue_counts.get(item["kind"], 0) + 1
    for item in false_negatives:
        clue_counts[item["kind"]] = clue_counts.get(item["kind"], 0) + 1
    hard_negatives = [
        item for item in false_negatives if item.get("stage") != "awaiting_checker"
    ]
    fresh_total = (
        freshness["lt_60"] + freshness["60_to_300"] + freshness["300_to_600"]
    )
    stale_total = freshness["600_to_1800"] + freshness["ge_1800"]
    other_counts: dict[str, int] = {}
    for channel in channels:
        for status, count in channel["other_status_counts"].items():
            other_counts[status] = other_counts.get(status, 0) + count

    degraded: list[str] = list(bridge_degraded)
    for channel in channels:
        for item in channel["degraded"]:
            label = channel["name"] + ":" + item
            if label not in degraded:
                degraded.append(label)

    resume_count = sum(1 for item in triggers if item["kind"] == "resume")
    fail_count = sum(1 for item in triggers if item["kind"] == "fail_stop")
    pending_count = sum(1 for item in triggers if item["source"] == "failed_notices")

    return {
        "read_only": True,
        "as_of": moment,
        "hooks_state": str(hooks_path),
        "bot_state": str(bot_path) if bot_path is not None else None,
        "bridge_state": bridge_text,
        "bridge_unchecked": bridge_unchecked,
        "thresholds": _threshold_block(script_gates, channels, fallback),
        "definitions": _definitions(),
        "verdict_file_count": len(verdict_files),
        "verdict_files": verdict_files,
        "verdict_record_count": sum(channel["verdict_record_count"] for channel in channels),
        "status_counts": status_counts,
        "terminal_status_counts": {key: status_counts[key] for key in _TERMINAL_ORDER},
        "other_status_counts": other_counts,
        "usable_terminal_count": sum(
            len(channel["usable_terminal_msgids"]) for channel in channels
        ),
        "freshness": {
            "fresh": fresh_total,
            "stale": stale_total,
            "missing": freshness["missing"],
            "future_rejected": freshness["future_rejected"],
            "buckets": {key: freshness[key] for key in _FRESH_BUCKETS},
        },
        "running_freshness": running,
        "early_platform": {
            "trigger_count": len(triggers),
            "resume_notice_count": resume_count,
            "fail_stop_notice_count": fail_count,
            "pending_unsent_count": pending_count,
            "events": triggers,
        },
        "false_positive_clues": false_positives,
        "false_negative_clues": false_negatives,
        "false_negative_hard_count": len(hard_negatives),
        "clue_counts": clue_counts,
        "degraded": degraded,
        "channels": [
            {
                "name": channel["name"],
                "inputs": channel["inputs"],
                "verdict_record_count": channel["verdict_record_count"],
                "usable_terminal_msgids": channel["usable_terminal_msgids"],
                "preferred_verdict_path": channel["preferred_verdict_path"],
                "trigger_count": len(channel["triggers"]),
                "degraded": channel["degraded"],
            }
            for channel in channels
        ],
    }


def _fmt_secs(value: float) -> str:
    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return f"{value:.1f}"


def _fmt_map(counts: dict[str, int], keys: tuple[str, ...]) -> str:
    return "，".join(f"{key} {counts.get(key, 0)}" for key in keys)


def render_text(report: dict) -> str:
    """Chinese summary of a ``reconcile`` report."""
    thresholds = report["thresholds"]
    fresh = report["freshness"]
    buckets = fresh["buckets"]
    running = report["running_freshness"]
    early = report["early_platform"]
    lines = [
        "冷通道平台裁决观察窗对账",
        f"对账时刻 {_fmt_secs(report['as_of'])}",
        f"hooks 状态 {report['hooks_state']}",
        "bot 状态 " + (report["bot_state"] or "（未指定，只在 hooks 目录内查找裁决与 outbox）"),
        "",
        "口径：裁决文件是 worker_platform_status.json 快照。终态只计 "
        "completed、shutdown、errored、interrupted。新鲜指 checked_at "
        f"年龄小于裁决 TTL {_fmt_secs(thresholds['verdict_ttl_secs'])} 秒，"
        f"且不早于未来 {_fmt_secs(thresholds['future_skew_secs'])} 秒；"
        "年龄等于 TTL 算过期。EARLY_PLATFORM 触发按通知文本"
        "「平台记录显示原执行会话已结束」计数，只计 outbox 里还留着的 "
        "send 行，与尚未入 outbox 的 failed_notices 去重；outbox 被截断后会计少。"
        "误报线索：终态裁决之后仍有 reply "
        "或 reply_file。漏报线索：自推静默达到 hook 的提前门，且没有正式回复、"
        "没有活 hold、没有鲜活信号、也没有可用终态裁决。核查请求还在"
        "节流窗内的缺失裁决另列，不计入硬漏报。",
        "",
        "阈值（只读现有常量，本工具不修改）",
        f"  裁决 TTL {_fmt_secs(thresholds['verdict_ttl_secs'])} 秒"
        "（death_watch_activity.PLATFORM_VERDICT_MAX_AGE_SECS）",
        f"  running 新鲜 {_fmt_secs(thresholds['running_fresh_secs'])} 秒"
        "（death_watch_activity.PLATFORM_RUNNING_FRESH_SECS）",
    ]
    early_gate = thresholds["early_platform_min_silence_secs"]
    if early_gate is None:
        lines.append("  提前静默门 两个 hook 脚本不一致，见各渠道")
    else:
        lines.append(
            f"  提前静默门 {_fmt_secs(early_gate)} 秒"
            "（hooks/scripts/*-inbox.sh EARLY_PLATFORM_MIN_SILENCE_SECS）"
        )
    if thresholds["silence_gate_fallback"]:
        lines.append("  静默门有缺项，缺的项用脚本初值占位，并已标记 silence_gate_fallback")
    lines.extend([
        "",
        f"裁决文件 {report['verdict_file_count']}，裁决条目 {report['verdict_record_count']}",
        "终态分布 " + _fmt_map(report["terminal_status_counts"], _TERMINAL_ORDER),
        "其余状态 " + _fmt_map(
            report["status_counts"],
            ("running", "unknown", "other", "missing", "invalid"),
        ),
        "探针视为平台终态 "
        f"{report['usable_terminal_count']}"
        "（checked_at 未过期且 run_id 未被判定不符；"
        "活 hold 与正式回复仍会挡住提前路由）",
    ])
    if any(len(channel["inputs"]["verdicts"]) > 1 for channel in report["channels"]):
        lines.append(
            "同一渠道有多份裁决文件：条目分布按每份累加；"
            "线索与上面的可用终态只采用第一份（bot state 优先）。"
        )
    lines.extend([
        "",
        "裁决新鲜度（checked_at）",
        f"  未过期 {fresh['fresh']}（0–60 秒 {buckets['lt_60']}，"
        f"60–300 秒 {buckets['60_to_300']}，300–600 秒 {buckets['300_to_600']}）",
        f"  已过期 {fresh['stale']}（600–1800 秒 {buckets['600_to_1800']}，"
        f"≥1800 秒 {buckets['ge_1800']}）",
        f"  无 checked_at {fresh['missing']}",
        f"  未来超出容差 {fresh['future_rejected']}",
        "",
        "running 新鲜度（仅 status=running）",
        f"  条目 {running['running_records']}，裁决未过期且 agent_updated_at 未过期 "
        f"{running['checked_fresh_update_fresh']}，裁决未过期但更新过期 "
        f"{running['checked_fresh_update_stale']}，更新缺失 "
        f"{running['checked_fresh_update_missing']}，裁决本身过期 "
        f"{running['checked_stale']}，run_id 不符 {running['run_mismatch']}",
        "",
        f"EARLY_PLATFORM 触发 {early['trigger_count']}"
        f"（续跑通知 {early['resume_notice_count']}，"
        f"停止通知 {early['fail_stop_notice_count']}，"
        f"尚未入 outbox {early['pending_unsent_count']}）",
        "",
        f"疑似误报 {len(report['false_positive_clues'])}",
    ])
    if not report["false_positive_clues"]:
        lines.append("  （无）")
    for clue in report["false_positive_clues"]:
        reply_bits = []
        for reply in clue["replies"]:
            delivered = "已送达" if reply["delivered"] else "已入队"
            reply_bits.append(
                f"{reply['mode']} {_fmt_secs(reply['ts'])} {delivered}"
            )
        lines.append(
            f"  {clue['channel']} {clue['msgid']} {clue['verdict_status']} "
            f"checked_at {_fmt_secs(clue['checked_at'])} 之后 "
            + "；".join(reply_bits)
        )
    awaiting = [
        item for item in report["false_negative_clues"]
        if item.get("stage") == "awaiting_checker"
    ]
    lines.extend([
        "",
        f"疑似漏报 {report['false_negative_hard_count']}"
        f"（另有 {len(awaiting)} 条仍在核查节流窗内）",
    ])
    shown = report["false_negative_clues"]
    if not shown:
        lines.append("  （无）")
    for clue in shown:
        stage = clue.get("stage")
        stage_text = ""
        if stage == "awaiting_checker":
            stage_text = "，核查请求未超过节流窗"
        elif stage == "request_unanswered":
            stage_text = "，核查请求已超过节流窗仍无可用终态"
        elif stage == "no_request":
            stage_text = "，未见核查请求"
        past = ""
        if clue.get("also_past_death_watch"):
            past = "，已超过主判死窗口，原 1800 秒路径会接管"
        verdicts = "，".join(
            f"{msgid}:{label}" for msgid, label in sorted(clue["verdicts"].items())
        )
        lines.append(
            f"  {clue['channel']} {','.join(clue['msgids'])} "
            f"{clue['kind']} 静默 {_fmt_secs(clue['silence_secs'])} 秒"
            f"{stage_text}{past}（{verdicts}）"
        )
    if report["degraded"]:
        lines.extend(["", "读取降级"])
        for item in report["degraded"]:
            lines.append(f"  {item}")
    if report["bridge_unchecked"]:
        lines.extend([
            "",
            "未读取桥 state。桥侧仍鲜活的批次不会被排除，漏报线索可能偏多。",
        ])
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry. Prints the report and does not write state files."""
    parser = argparse.ArgumentParser(
        description="只读对账冷通道平台裁决观察窗（待办 21）。不写任何状态文件。",
    )
    parser.add_argument(
        "hooks_state",
        help="hooks 状态目录。含 weixin-bot/wecom-bot 时按渠道展开，否则视为单渠道",
    )
    parser.add_argument(
        "--bot-state",
        default="",
        help="bot 状态根。生产环境裁决与 outbox 在 <bot>/<channel>/state，不在 hooks 目录",
    )
    parser.add_argument(
        "--bridge-state",
        default="",
        help="可选 native-bridge state.json。给出后，桥侧仍鲜活的批次不计入漏报",
    )
    parser.add_argument(
        "--now",
        type=float,
        default=None,
        help="对账时刻的 epoch 秒。缺省为当前时间",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="输出 JSON。缺省输出中文汇总",
    )
    parser.add_argument(
        "--repo-root",
        default="",
        help=argparse.SUPPRESS,
    )
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        return int(code)
    try:
        report = reconcile(
            args.hooks_state,
            bot_state=args.bot_state or None,
            bridge_state=args.bridge_state or None,
            now=args.now,
            repo_root=args.repo_root or None,
        )
    except ReconcileError as exc:
        print(str(exc), file=sys.stderr)
        return exc.code
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
