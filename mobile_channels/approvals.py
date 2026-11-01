"""Approval authorization and transactional decisions, independent of transport."""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from typing import Mapping

from .models import OwnershipError, StateError
from .storage import Store

DECISIONS = frozenset(('allow_once', 'allow_always', 'deny'))


def authorized_operator(item: Mapping, channel: str, chat_id: str, operator_id: str,
                        administrators: Mapping | None = None) -> bool:
    """Authorize an explicit source scope or configured administrator route.

    User allowlists are not administrator grants. A channel name or approval
    number alone never provides authority. The environment mapping is trusted
    deployment configuration, not a field accepted from an inbound message.
    """
    if not all(isinstance(value, str) and value for value in (channel, chat_id, operator_id)):
        return False
    scope = item.get('decision_scope') or {}
    if (isinstance(scope, dict) and scope.get('channel') == channel
            and scope.get('chat_id') == chat_id and isinstance(scope.get('operator_ids'), list)
            and operator_id in scope['operator_ids']):
        return True
    if administrators is None:
        try:
            administrators = json.loads(os.environ.get('APPROVAL_ADMINS_JSON', '{}'))
        except (ValueError, TypeError):
            return False
    routes = administrators.get(channel, {}) if isinstance(administrators, dict) else {}
    operators = routes.get(chat_id, []) if isinstance(routes, dict) else []
    return isinstance(operators, list) and operator_id in operators


def decision_allowed(item: Mapping, row: Mapping, now: float, administrators: Mapping | None = None) -> bool:
    """Validate trusted inbound identity, decision, and current expiry together."""
    channel = str(row.get('channel') or '').removesuffix('-card')
    expires = item.get('expires_ms')
    try:
        live = expires is not None and float(expires) > now * 1000
    except (ValueError, TypeError):
        return False
    return (live and row.get('decision') in DECISIONS and authorized_operator(
        item, channel, row.get('chat_id', ''), row.get('operator_id', ''), administrators))


@dataclass(frozen=True)
class DecisionClaim:
    """Capability binding the one chosen decision to a persisted attempt."""

    approval_id: str
    attempt_id: str
    decision: str
    operator_id: str


class ApprovalStore:
    """One decision attempt per approval; uncertain submission requires evidence."""

    def __init__(self, store: Store):
        """Initialize extension tables in the core database."""
        self.store, self.connection = store, store.connection
        with store.transaction():
            self.connection.execute('''CREATE TABLE IF NOT EXISTS approvals (
                id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
                operator_ids TEXT NOT NULL, expires_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','decision_unknown','decided','expired','rejected')),
                decision TEXT, attempt_id TEXT, operator_id TEXT, evidence TEXT)''')

    def register(self, approval_id: str, conversation_id: str, operator_ids: list[str], expires_at: float) -> None:
        """Bind authorization immutably; duplicate platform events cannot widen it."""
        if not approval_id or not operator_ids or any(not isinstance(value, str) or not value for value in operator_ids):
            raise ValueError('approval identity and explicit operators are required')
        if expires_at <= self.store.clock():
            raise ValueError('approval must not already be expired')
        operators = json.dumps(sorted(set(operator_ids)))
        with self.store.transaction():
            existing = self.connection.execute('SELECT * FROM approvals WHERE id=?', (approval_id,)).fetchone()
            if existing:
                if (existing['conversation_id'], existing['operator_ids'], existing['expires_at']) != (conversation_id, operators, expires_at):
                    raise StateError('approval scope cannot change')
                return
            self.connection.execute('INSERT INTO approvals(id,conversation_id,operator_ids,expires_at,state) VALUES (?,?,?,?,?)',
                                    (approval_id, conversation_id, operators, expires_at, 'pending'))

    def claim_decision(self, approval_id: str, conversation_id: str, operator_id: str, decision: str) -> DecisionClaim:
        """Persist unknown before calling the platform, after route/operator checks."""
        if decision not in DECISIONS:
            raise ValueError('unsupported decision')
        with self.store.transaction():
            row = self.connection.execute('SELECT * FROM approvals WHERE id=?', (approval_id,)).fetchone()
            if row is None or row['conversation_id'] != conversation_id or operator_id not in json.loads(row['operator_ids']):
                raise PermissionError('approval operator or conversation is unauthorized')
            if row['expires_at'] <= self.store.clock():
                raise StateError('approval expired')
            if row['state'] != 'pending':
                raise StateError('approval already claimed; query its current result')
            attempt = uuid.uuid4().hex
            self.connection.execute("UPDATE approvals SET state='decision_unknown',decision=?,attempt_id=?,operator_id=? WHERE id=?",
                                    (decision, attempt, operator_id, approval_id))
            return DecisionClaim(approval_id, attempt, decision, operator_id)

    def resolve(self, claim: DecisionClaim, *, accepted: bool, evidence: str) -> None:
        """Resolve the exact chosen attempt; never automatically submit again."""
        if not evidence or not isinstance(accepted, bool):
            raise ValueError('explicit platform evidence and outcome are required')
        with self.store.transaction():
            row = self.connection.execute('SELECT * FROM approvals WHERE id=?', (claim.approval_id,)).fetchone()
            if (row is None or row['state'] != 'decision_unknown' or row['attempt_id'] != claim.attempt_id
                    or row['decision'] != claim.decision or row['operator_id'] != claim.operator_id):
                raise OwnershipError('decision attempt is no longer current')
            self.connection.execute('UPDATE approvals SET state=?,evidence=? WHERE id=?',
                                    ('decided' if accepted else 'rejected', evidence, claim.approval_id))

    def status(self, approval_id: str) -> dict:
        """Return scoped decision metadata, without raw operation contents."""
        row = self.connection.execute('SELECT id,conversation_id,expires_at,state,decision,attempt_id,operator_id,evidence FROM approvals WHERE id=?',
                                      (approval_id,)).fetchone()
        if row is None:
            raise KeyError(approval_id)
        return dict(row)
