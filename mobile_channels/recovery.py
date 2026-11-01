"""Explicit, audited recovery of a stopped legacy bridge's uncertain submission.

This module never queries a platform or invents correlation. Its caller must
supply evidence from the original execution and compare the inspected digest.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def bridge_lock(base: Path) -> Iterator[None]:
    """Exclude a running bridge and other recovery writers without waiting."""
    base = Path(base)
    descriptor = os.open(base / '.runtime.lock', os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError('Bridge is running or another recovery writer owns its lock') from error
        yield
    finally:
        os.close(descriptor)


def pending_digest(pending: dict) -> str:
    """Fingerprint the complete inspected pending record, including route evidence."""
    if not isinstance(pending, dict):
        raise ValueError('pending submission must be an object')
    raw = json.dumps(pending, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(raw).hexdigest()


def _state(base: Path) -> dict:
    """Require an existing parseable state; recovery never resets corrupt state."""
    with (base / 'state.json').open(encoding='utf-8') as source:
        state = json.load(source)
    if not isinstance(state, dict) or not isinstance(state.get('channels'), dict):
        raise ValueError('Invalid legacy bridge state')
    return state


def inspect_pending(base: Path, channel: str) -> dict:
    """Return operator evidence and its CAS digest without changing state."""
    state = _state(Path(base))
    pending = state['channels'].get(channel, {}).get('submission_pending')
    if not isinstance(pending, dict):
        raise ValueError('No pending submission for this channel')
    return {'channel': channel, 'digest': pending_digest(pending), 'pending': pending}


def _save_state(base: Path, state: dict) -> None:
    """Commit state and its audit together, syncing the rename before success."""
    temporary = base / f'.recovery-{uuid.uuid4().hex}.tmp'
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as target:
            json.dump(state, target, ensure_ascii=False, indent=1)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, base / 'state.json')
        directory = os.open(base, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def resolve_pending(base: Path, channel: str, *, msgid: str, session_id: str,
                    digest: str, outcome: str, evidence: str) -> dict:
    """Resolve an inspected intent under exclusive ownership and exact identity.

    ``accepted`` resumes the original turn for polling. ``not_submitted`` places
    the original row in state.recovery_queue, leaving the spool offset intact.
    The bridge must consume this queue atomically with its next submission or
    fallback commit. No arbitrary replacement route or text is accepted.
    """
    if outcome not in ('accepted', 'not_submitted'):
        raise ValueError('outcome must be accepted or not_submitted')
    if not all(isinstance(value, str) and value.strip() for value in
               (channel, msgid, session_id, digest, evidence)):
        raise ValueError('channel, original identities, digest and evidence are required')
    base = Path(base)
    with bridge_lock(base):
        state = _state(base)
        channel_state = state['channels'].get(channel)
        if not isinstance(channel_state, dict):
            raise ValueError('Unknown channel')
        pending = channel_state.get('submission_pending')
        if not isinstance(pending, dict) or pending_digest(pending) != digest:
            raise ValueError('Pending state changed; inspect it again before resolving')
        row, turn = pending.get('row'), pending.get('turn')
        if (not isinstance(row, dict) or not isinstance(turn, dict)
                or row.get('msgid') != msgid or turn.get('session_id') != session_id):
            raise ValueError('Original message or session does not match inspected evidence')
        if not isinstance(channel_state.get('turns', []), list):
            raise ValueError('Invalid running turns')
        operation = uuid.uuid4().hex
        turns = list(channel_state.get('turns') or [])
        if outcome == 'accepted':
            matches = [index for index, item in enumerate(turns)
                       if item.get('msgid') == turn.get('msgid')]
            if pending.get('kind') == 'merge':
                if len(matches) != 1 or turns[matches[0]].get('session_id') != session_id:
                    raise ValueError('Original merge turn is missing or changed')
                turns[matches[0]] = dict(turn)
            else:
                if matches:
                    raise ValueError('Task already exists in running turns')
                turns.append(dict(turn))
            channel_state['turns'] = turns
            channel_state['preamble_done'] = True
        else:
            queue = channel_state.setdefault('recovery_queue', [])
            if not isinstance(queue, list):
                raise ValueError('Invalid recovery queue')
            queue.append({'recovery_id': operation, 'row': dict(row)})
        channel_state['submission_pending'] = None
        channel_state.setdefault('submission_recovery_audit', []).append({
            'recovery_id': operation, 'msgid': msgid, 'session_id': session_id,
            'previous_digest': digest, 'outcome': outcome, 'evidence': evidence,
            'resolved_at': time.time(),
        })
        _save_state(base, state)
        return {'recovery_id': operation, 'channel': channel, 'msgid': msgid,
                'outcome': outcome, 'requeued': outcome == 'not_submitted'}
