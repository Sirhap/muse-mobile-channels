"""Muse execution adapter with persisted sessions and explicit run correlation.

The legacy stream API does not promise a run identifier. A stream accepted
without such an identifier remains unknown; this adapter never associates an
arbitrary later assistant message with the task merely by its history position.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Callable

from .runtime import ExecutionContext, ExecutionOutcome
from .storage import Store

SCHEMA = '''
CREATE TABLE IF NOT EXISTS executor_sessions (
    conversation_id TEXT NOT NULL REFERENCES conversations(id), generation INTEGER NOT NULL,
    session_id TEXT NOT NULL UNIQUE, PRIMARY KEY(conversation_id,generation)
);
CREATE TABLE IF NOT EXISTS executor_runs (
    attempt_id TEXT PRIMARY KEY REFERENCES execution_attempts(id),
    session_id TEXT NOT NULL, platform_run_id TEXT, status TEXT NOT NULL,
    created_at REAL NOT NULL
);
'''


def validate_transport_source() -> Path:
    """Resolve an explicit transport source without reading credentials or networking.

    Wheels contain the core package only. Their deployments must supply the
    separately installed legacy transport via MUSE_BRIDGE_SOURCE or workspace.
    A configured but missing path never falls back to a different installation.
    """
    explicit = os.environ.get('MUSE_BRIDGE_SOURCE')
    workspace = os.environ.get('MUSE_WORKSPACE')
    if explicit:
        path = Path(explicit).expanduser()
    elif workspace:
        path = Path(workspace).expanduser() / 'native-bridge/native_bridge.py'
    else:
        path = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError('Muse transport source is unavailable; set MUSE_BRIDGE_SOURCE '
                                'or MUSE_WORKSPACE to the installed bridge source')
    if path.suffix != '.py':
        raise ValueError('Muse transport source must be a Python file')
    return path


def bridge_client():
    """Load the configured optional transport only at the platform boundary."""
    path = validate_transport_source()
    spec = importlib.util.spec_from_file_location('_core_muse_transport', path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Muse transport source has no Python loader')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.connect()


class MuseExecutor:
    """Bind sessions to conversation generations and query only a platform run ID."""

    def __init__(self, store: Store, client_factory: Callable = bridge_client,
                 node_id: str = 'native-bridge'):
        """Use a short-lived client per operation and never persist credentials."""
        self.store, self.client_factory, self.node_id = store, client_factory, node_id
        with store.transaction():
            for statement in SCHEMA.split(';'):
                if statement.strip():
                    store.connection.execute(statement)

    def _session(self, client, context: ExecutionContext) -> str:
        """Create a separate external session for each bound conversation generation."""
        key = (context.claim.conversation_id, context.claim.generation)
        row = self.store.connection.execute(
            'SELECT session_id FROM executor_sessions WHERE conversation_id=? AND generation=?', key
        ).fetchone()
        if row:
            return row['session_id']
        response = client.call_json('session.start', body={
            'method': '/api/session/start',
            'params': {'origin': 'fresh', 'lifecycle': 'persistent',
                       'title': f'mobile-core:{key[0]}:{key[1]}'},
        }, timeout=15)
        session_id = response.get('session_id') if isinstance(response, dict) else None
        if not isinstance(session_id, str) or not session_id:
            raise ValueError('platform did not return a session identity')
        with self.store.transaction():
            self.store.connection.execute('INSERT INTO executor_sessions VALUES (?,?,?)', (*key, session_id))
        return session_id

    def _validate_owner(self, context: ExecutionContext, *, submitting: bool = False) -> None:
        """Recheck fenced ownership immediately before an external operation.

        This prevents work already known to be expired or cancelled from being
        sent. It cannot revoke a request after it has left this process.
        """
        with self.store.transaction():
            task = self.store._owned(context.claim)
            if submitting and task['cancel_requested']:
                raise PermissionError('task cancellation was requested before submission')

    def submit(self, context: ExecutionContext) -> ExecutionOutcome:
        """Persist the external session binding before opening exactly one stream."""
        client = None
        try:
            client = self.client_factory()
        except (ImportError, ModuleNotFoundError):
            return ExecutionOutcome('failed', evidence='executor_dependency_unavailable')
        try:
            existing = self.store.connection.execute(
                'SELECT * FROM executor_runs WHERE attempt_id=?', (context.claim.attempt_id,)
            ).fetchone()
            if existing:
                return ExecutionOutcome('unknown', evidence='attempt_already_has_external_binding')
            self._validate_owner(context, submitting=True)
            sid = self._session(client, context)
            with self.store.transaction():
                task = self.store._owned(context.claim)
                if task['cancel_requested']:
                    return ExecutionOutcome('not_submitted', evidence='cancelled_before_stream')
                self.store.connection.execute(
                    'INSERT INTO executor_runs VALUES (?,?,?,?,?)',
                    (context.claim.attempt_id, sid, None, 'submission_unknown', self.store.clock()),
                )
            self._validate_owner(context, submitting=True)
            response = client._open('chat.stream', body={
                'items': [{'type': 'text', 'text': context.text}], 'node_id': self.node_id,
                'capabilities': {}, 'session_id': sid,
            })
            # Do not invent support for correlation: only an explicit platform
            # run ID returned by this call makes automated polling safe.
            run_id = response.get('run_id') if isinstance(response, dict) else None
            if not isinstance(run_id, str) or not run_id:
                return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
            with self.store.transaction():
                self.store.connection.execute(
                    "UPDATE executor_runs SET platform_run_id=?,status='accepted' WHERE attempt_id=?",
                    (run_id, context.claim.attempt_id),
                )
            return ExecutionOutcome('accepted', run_id=run_id)
        except Exception:
            return ExecutionOutcome('unknown')
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def _binding(self, context: ExecutionContext, run_id: str | None):
        """Reject missing or caller-replaced platform execution identities."""
        row = self.store.connection.execute(
            'SELECT * FROM executor_runs WHERE attempt_id=?', (context.claim.attempt_id,)
        ).fetchone()
        if row is None or not row['platform_run_id']:
            return None
        if run_id is not None and run_id != row['platform_run_id']:
            raise ValueError('platform run identity does not match this attempt')
        return row

    def query(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Read only events explicitly correlated with the original platform run."""
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        client = self.client_factory()
        try:
            self._validate_owner(context)
            response = client.call_json('chat.history', body={
                'limit': 100, 'session_id': binding['session_id'],
            }, timeout=15)
            events = response.get('chat_events', [])
            for event in sorted(events, key=lambda value: value.get('seq', 0)):
                payload = event.get('payload') or {}
                if (event.get('run_id') or payload.get('run_id')) != binding['platform_run_id']:
                    continue
                if payload.get('status') in ('cancelled', 'canceled'):
                    return ExecutionOutcome('cancelled', evidence='platform_run_cancelled')
                if payload.get('status') in ('failed', 'error'):
                    return ExecutionOutcome('failed', evidence='platform_run_failed')
                if event.get('event_name') == 'message.assistant' and payload.get('status') in ('completed', 'complete', 'succeeded'):
                    text = payload.get('display_text')
                    if isinstance(text, str):
                        return ExecutionOutcome('succeeded', text=text)
            return ExecutionOutcome('running')
        finally:
            client.close()

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Request cancellation, then require a correlated terminal platform event."""
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown')
        client = self.client_factory()
        try:
            self._validate_owner(context)
            client.call_json('chat.cancel', body={'session_id': binding['session_id']}, timeout=15)
        finally:
            client.close()
        return self.query(context, run_id)
