"""Muse execution adapter with persisted sessions and explicit run correlation.

The legacy stream API does not promise a run identifier: ``_open`` returns
only a per-connection stream id, never a run id (proven live 2026-11-06).
Correlation therefore rests on three facts recorded before the reply can
exist: the session is dedicated to one conversation generation, the highest
history sequence seen before sending (the baseline), and confirmation that
the sent message itself was recorded in that session's history. A reply is
accepted only when it is a ``message.assistant`` event after the baseline,
with an empty ``reply_to_message_id`` and a completed status -- the
discriminator the reference client uses to tell genuine replies apart from
proactive pushes. A submission whose recording was never confirmed stays
unknown, and its history is never trusted.
"""
from __future__ import annotations

import importlib.util
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Callable

from .gadget_client import GadgetAuthError, GadgetClient, GadgetCredentials, GadgetError, GadgetNetworkError
from .runtime import ExecutionContext, ExecutionOutcome
from .storage import Store

SCHEMA = '''
CREATE TABLE IF NOT EXISTS executor_sessions (
    conversation_id TEXT NOT NULL REFERENCES conversations(id), generation INTEGER NOT NULL,
    session_id TEXT NOT NULL UNIQUE, PRIMARY KEY(conversation_id,generation)
);
CREATE TABLE IF NOT EXISTS gadget_sessions (
    conversation_id TEXT NOT NULL, generation INTEGER NOT NULL,
    session_id TEXT NOT NULL UNIQUE, PRIMARY KEY(conversation_id,generation)
);
CREATE TABLE IF NOT EXISTS executor_runs (
    attempt_id TEXT PRIMARY KEY REFERENCES execution_attempts(id),
    session_id TEXT NOT NULL, platform_run_id TEXT, status TEXT NOT NULL,
    created_at REAL NOT NULL, track TEXT NOT NULL DEFAULT 'native',
    baseline_seq INTEGER
);
'''


def ensure_executor_schema(store: Store) -> None:
    """确保执行器表结构存在，并向前兼容升级已有表结构以支持双轨 track 字段。"""
    with store.transaction():
        for statement in SCHEMA.split(';'):
            if statement.strip():
                store.connection.execute(statement)
        columns = [row['name'] for row in store.connection.execute("PRAGMA table_info(executor_runs)").fetchall()]
        if 'track' not in columns:
            store.connection.execute("ALTER TABLE executor_runs ADD COLUMN track TEXT NOT NULL DEFAULT 'native'")
        if 'baseline_seq' not in columns:
            store.connection.execute("ALTER TABLE executor_runs ADD COLUMN baseline_seq INTEGER")


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
    """Bind sessions to conversation generations and correlate by recorded history.

    Each conversation generation gets its own side session. A submission is
    accepted only after the sent message is confirmed recorded in that
    session's history; the reply is then matched by sequence after the
    pre-send baseline, an empty reply-to, and a completed status.
    """

    def __init__(self, store: Store, client_factory: Callable = bridge_client,
                 node_id: str = 'native-bridge', confirm_timeout: float = 10.0,
                 poll_interval: float = 1.0):
        """Use a short-lived client per operation and never persist credentials.

        The submit call keeps its client open until the sent message is
        confirmed recorded: closing right after ``_open`` drops the stream
        before the server records it (proven live 2026-11-06).
        """
        self.store, self.client_factory, self.node_id = store, client_factory, node_id
        self.confirm_timeout, self.poll_interval = confirm_timeout, poll_interval
        ensure_executor_schema(store)

    def _baseline(self, client, session_id: str) -> int:
        """Highest history sequence in the session before this attempt sends."""
        response = client.call_json('chat.history', body={
            'limit': 1, 'session_id': session_id,
        }, timeout=15)
        events = response.get('chat_events', []) if isinstance(response, dict) else []
        return max((event.get('seq', 0) for event in events), default=0)

    def _confirm_recorded(self, client, session_id: str, baseline: int) -> bool:
        """Poll history on the same connection until a new event appears.

        ``_open`` is fire-and-forget: the frame alone does not prove the
        message landed. Only a history event after the baseline proves it.
        """
        deadline = time.time() + self.confirm_timeout
        while True:
            response = client.call_json('chat.history', body={
                'limit': 10, 'session_id': session_id,
            }, timeout=15)
            events = response.get('chat_events', []) if isinstance(response, dict) else []
            if any(event.get('seq', 0) > baseline for event in events):
                return True
            if time.time() >= deadline:
                return False
            time.sleep(self.poll_interval)

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
        """Persist the external session binding before opening exactly one stream.

        The stream API returns no run id (``_open`` yields only a
        per-connection stream id), so the run handle stored here is that
        stream id; correlation itself rests on the recorded baseline, not on
        the handle. The client stays open until the sent message is confirmed
        recorded in history -- closing right after ``_open`` drops the stream.
        A submission whose recording was never confirmed stays unknown and its
        history is never trusted; the ambiguous row is preserved, never
        deleted, so the attempt cannot be silently sent twice.
        """
        client = None
        try:
            client = self.client_factory()
        except (ImportError, ModuleNotFoundError):
            return ExecutionOutcome('failed', evidence='executor_dependency_unavailable')
        try:
            existing = self.store.connection.execute(
                'SELECT * FROM executor_runs WHERE attempt_id=?', (context.claim.attempt_id,)
            ).fetchone()
            if existing and existing['status'] != 'failed_superseded':
                return ExecutionOutcome('unknown', evidence='attempt_already_has_external_binding')
            self._validate_owner(context, submitting=True)
            sid = self._session(client, context)
            try:
                baseline = self._baseline(client, sid)
            except Exception:
                return ExecutionOutcome('unknown', evidence='baseline_unavailable')
            with self.store.transaction():
                task = self.store._owned(context.claim)
                if task['cancel_requested']:
                    return ExecutionOutcome('not_submitted', evidence='cancelled_before_stream')
                self.store.connection.execute(
                    "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) "
                    "VALUES (?,?,?,?,?,'native') "
                    "ON CONFLICT(attempt_id) DO UPDATE SET session_id=excluded.session_id, "
                    "platform_run_id=excluded.platform_run_id, status=excluded.status, "
                    "created_at=excluded.created_at, track=excluded.track",
                    (context.claim.attempt_id, sid, None, 'submission_unknown', self.store.clock()),
                )
            self._validate_owner(context, submitting=True)
            stream = client._open('chat.stream', body={
                'items': [{'type': 'text', 'text': context.text}], 'node_id': self.node_id,
                'capabilities': {}, 'session_id': sid,
            })
            if isinstance(stream, dict) and isinstance(stream.get('run_id'), str) and stream['run_id']:
                run_id = stream['run_id']
            elif isinstance(stream, int):
                run_id = f'stream:{stream}'
            else:
                return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
            if not self._confirm_recorded(client, sid, baseline):
                return ExecutionOutcome('unknown', evidence='submission_not_recorded')
            with self.store.transaction():
                self.store.connection.execute(
                    "UPDATE executor_runs SET platform_run_id=?,baseline_seq=?,status='accepted' WHERE attempt_id=?",
                    (run_id, baseline, context.claim.attempt_id),
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
        """Read only replies correlated with the confirmed submission.

        The binding guarantees the sent message was recorded; the reply is
        the first completed assistant event after the recorded baseline, in
        this attempt's dedicated session, with an empty reply-to (genuine
        replies, not proactive pushes). Events at or before the baseline --
        including any earlier attempt's output in this session -- are ignored.
        """
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
            baseline = binding['baseline_seq'] or 0
            for event in sorted(events, key=lambda value: value.get('seq', 0)):
                if event.get('seq', 0) <= baseline:
                    continue
                if event.get('event_name') != 'message.assistant':
                    continue
                payload = event.get('payload') or {}
                if event.get('reply_to_message_id') or payload.get('reply_to_message_id'):
                    continue
                if payload.get('status') in ('cancelled', 'canceled'):
                    return ExecutionOutcome('cancelled', evidence='platform_run_cancelled')
                if payload.get('status') in ('failed', 'error'):
                    return ExecutionOutcome('failed', evidence='platform_run_failed')
                if payload.get('status') in ('completed', 'complete', 'succeeded'):
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


class CircuitBreaker:
    """轻量级熔断器，连续失败达到阈值时触发熔断，冷却后进入半开试探。"""

    def __init__(
        self,
        failure_threshold: int = 3,
        cooldown_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock
        self.failure_count = 0
        self._state = "CLOSED"
        self.last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def status(self) -> str:
        """返回当前熔断器状态：'CLOSED'、'OPEN' 或 'HALF_OPEN'。"""
        with self._lock:
            if self._state == "OPEN":
                if self.clock() - self.last_failure_time >= self.cooldown_seconds:
                    return "HALF_OPEN"
            return self._state

    def can_execute(self) -> bool:
        """判断当前是否允许执行。"""
        state = self.status
        return state in ("CLOSED", "HALF_OPEN")

    def record_success(self) -> None:
        """执行成功，重置失败计数并闭合熔断器。"""
        with self._lock:
            self.failure_count = 0
            self._state = "CLOSED"

    def record_failure(self) -> None:
        """执行失败，记录时间并增加计数，若达到阈值或处于半开状态则开启熔断。"""
        with self._lock:
            was_half_open = (
                self._state == "OPEN"
                and (self.clock() - self.last_failure_time >= self.cooldown_seconds)
            )
            self.failure_count += 1
            self.last_failure_time = self.clock()
            if was_half_open or self.failure_count >= self.failure_threshold:
                self._state = "OPEN"


class GadgetExecutor:
    """Gadget side-session executor: one worker process per turn, real Noise protocol.

    The worker (mobile_channels/gadget_worker/turn.ts) owns the Noise
    connection: it sends the message -- which creates the side chat -- then
    subscribes and follows the bound reply to completion, recording its
    outcome in a per-attempt result file. This executor only spawns the
    worker, polls the file, and kills the worker on cancel. It never
    touches the cookie track: the Gadget track stays usable when the bridge
    credentials are dead, which is the point of the track.

    Sessions are dedicated per conversation generation and persisted in
    gadget_sessions (separate from the native track's executor_sessions, so
    a fallback never reuses the other track's session). A session id is only
    a name until the worker's first send creates it server-side.
    """

    def __init__(
        self,
        store: Store,
        client: GadgetClient | None = None,
        client_factory: Callable[[], GadgetClient] | None = None,
    ):
        self.store = store
        self._client = client
        self.client_factory = client_factory
        ensure_executor_schema(store)

    @property
    def client(self) -> GadgetClient:
        if self._client is not None:
            return self._client
        if self.client_factory is not None:
            return self.client_factory()
        self._client = GadgetClient()
        return self._client

    def is_configured(self) -> bool:
        """Check the pairing credentials are installed and complete."""
        try:
            return self.client.is_configured()
        except Exception:
            return False

    def _session(self, context: ExecutionContext) -> str:
        """Dedicated side-session id for this conversation generation, persisted.

        No network call happens here: the id is only a name until the
        worker's first send creates the session server-side.
        """
        key = (context.claim.conversation_id, context.claim.generation)
        row = self.store.connection.execute(
            'SELECT session_id FROM gadget_sessions WHERE conversation_id=? AND generation=?', key
        ).fetchone()
        if row:
            return row['session_id']
        session_id = str(uuid.uuid4())
        with self.store.transaction():
            self.store.connection.execute(
                'INSERT OR IGNORE INTO gadget_sessions VALUES (?,?,?)', (*key, session_id))
            row = self.store.connection.execute(
                'SELECT session_id FROM gadget_sessions WHERE conversation_id=? AND generation=?', key
            ).fetchone()
        return row['session_id']

    @staticmethod
    def _worker_pid(platform_run_id: str | None) -> int | None:
        """Parse the 'worker:<pid>' handle stored as the run id."""
        if not isinstance(platform_run_id, str) or not platform_run_id.startswith('worker:'):
            return None
        try:
            return int(platform_run_id.split(':', 1)[1])
        except ValueError:
            return None

    def _validate_owner(self, context: ExecutionContext, *, submitting: bool = False) -> None:
        """Recheck fenced ownership immediately before an external operation."""
        with self.store.transaction():
            task = self.store._owned(context.claim)
            if submitting and task['cancel_requested']:
                raise PermissionError('task cancellation was requested before submission')

    def _binding(self, context: ExecutionContext, run_id: str | None):
        """Validate and fetch the attempt's persisted execution binding."""
        row = self.store.connection.execute(
            'SELECT * FROM executor_runs WHERE attempt_id=?', (context.claim.attempt_id,)
        ).fetchone()
        if row is None or not row['platform_run_id']:
            return None
        if run_id is not None and run_id != row['platform_run_id']:
            raise ValueError('platform run identity does not match this attempt')
        return row

    def _drop_placeholder(self, attempt_id: str) -> None:
        """Remove the pre-send placeholder row; only valid when nothing was sent."""
        with self.store.transaction():
            self.store.connection.execute(
                'DELETE FROM executor_runs WHERE attempt_id=? AND platform_run_id IS NULL',
                (attempt_id,)
            )

    def submit(self, context: ExecutionContext) -> ExecutionOutcome:
        """Spawn the Gadget worker for this attempt and record track='gadget'."""
        if not self.is_configured():
            return ExecutionOutcome('not_submitted', evidence='gadget_credentials_missing')

        try:
            existing = self.store.connection.execute(
                'SELECT * FROM executor_runs WHERE attempt_id=?', (context.claim.attempt_id,)
            ).fetchone()
            if existing and existing['status'] != 'failed_superseded':
                return ExecutionOutcome('unknown', evidence='attempt_already_has_external_binding')

            self._validate_owner(context, submitting=True)
            session_id = self._session(context)

            with self.store.transaction():
                task = self.store._owned(context.claim)
                if task['cancel_requested']:
                    return ExecutionOutcome('not_submitted', evidence='cancelled_before_submission')
                self.store.connection.execute(
                    "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) "
                    "VALUES (?,?,?,?,?,'gadget') "
                    "ON CONFLICT(attempt_id) DO UPDATE SET session_id=excluded.session_id, "
                    "platform_run_id=excluded.platform_run_id, status=excluded.status, "
                    "created_at=excluded.created_at, track=excluded.track",
                    (context.claim.attempt_id, session_id, None, 'submission_unknown', self.store.clock()),
                )

            self._validate_owner(context, submitting=True)
            proc = self.client.start_turn(context.claim.attempt_id, session_id, context.text)
            run_id = f"worker:{proc.pid}"

            with self.store.transaction():
                self.store.connection.execute(
                    "UPDATE executor_runs SET platform_run_id=?,status='accepted' WHERE attempt_id=?",
                    (run_id, context.claim.attempt_id),
                )
            return ExecutionOutcome('accepted', run_id=run_id)
        except PermissionError:
            self._drop_placeholder(context.claim.attempt_id)
            return ExecutionOutcome('not_submitted', evidence='cancelled_before_submission')
        except (GadgetAuthError, GadgetNetworkError, GadgetError) as exc:
            # The worker never started, so nothing was sent: the placeholder
            # row may go. Any failure after the spawn keeps its row.
            self._drop_placeholder(context.claim.attempt_id)
            return ExecutionOutcome('unknown', evidence=str(exc))
        except Exception as exc:
            # Unknown failure with a placeholder row in place. The worker may
            # have started: preserve the ambiguous binding, never delete it.
            return ExecutionOutcome('unknown', evidence=str(exc))

    def query(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Read the worker's result file; a live worker with no file yet is running."""
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')

        try:
            result = self.client.read_result(context.claim.attempt_id)
        except Exception as exc:
            return ExecutionOutcome('unknown', evidence=str(exc))
        if result is None:
            pid = self._worker_pid(binding['platform_run_id'])
            if pid is not None and self.client.worker_alive(pid):
                return ExecutionOutcome('running')
            return ExecutionOutcome('unknown', evidence='worker_exited_without_result')

        state = result.get('state')
        mid = result.get('message_id')
        message_id = mid if isinstance(mid, str) and mid else None
        if state == 'running':
            return ExecutionOutcome('running', message_id=message_id)
        if state == 'succeeded':
            text = result.get('text')
            if not isinstance(text, str) or not text:
                return ExecutionOutcome('unknown', evidence='worker_succeeded_without_text', message_id=message_id)
            return ExecutionOutcome('succeeded', text=text, message_id=message_id)
        if state == 'failed':
            return ExecutionOutcome('failed', evidence=str(result.get('evidence') or 'platform_turn_failed'), message_id=message_id)
        if state == 'cancelled':
            return ExecutionOutcome('cancelled', evidence=str(result.get('evidence') or 'platform_turn_cancelled'), message_id=message_id)
        if state == 'unknown':
            return ExecutionOutcome('unknown', evidence=str(result.get('evidence') or 'worker_unknown'), message_id=message_id)
        return ExecutionOutcome('unknown', evidence=f'unrecognized_result_state_{state}', message_id=message_id)

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Kill the worker and report the turn cancelled."""
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        pid = self._worker_pid(binding['platform_run_id'])
        if pid is not None:
            try:
                self.client.stop_turn(pid)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
        result = self.client.read_result(context.claim.attempt_id)
        if isinstance(result, dict) and result.get('state') == 'cancelled':
            return ExecutionOutcome('cancelled', evidence=str(result.get('evidence') or 'platform_turn_cancelled'))
        return ExecutionOutcome('cancelled', evidence='worker_terminated')

    def close(self) -> None:
        """Close the underlying client; live workers keep running so their results survive."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass

    def __enter__(self) -> GadgetExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class DualTrackExecutor:
    """双轨高可用执行器，实现 Executor 协议。

    组合 GadgetExecutor（主轨）与 MuseExecutor（备轨）。
    优先使用 Gadget 侧会话，遇故障或凭证缺失时无缝降级至 Native 备轨。
    根据 attempt_id 在 executor_runs 中绑定的 track 精确分发 query 与 cancel，严禁跨轨串话。
    """

    def __init__(
        self,
        store: Store,
        primary: GadgetExecutor | None = None,
        fallback: MuseExecutor | None = None,
        circuit: CircuitBreaker | None = None,
    ):
        self.store = store
        ensure_executor_schema(store)
        self.primary = primary or GadgetExecutor(store)
        self.fallback = fallback or MuseExecutor(store)
        self.circuit = circuit or CircuitBreaker()

    def current_track(self, attempt_id: str) -> str | None:
        """根据 attempt_id 查询当前任务绑定的执行轨 ('gadget' 或 'native')。"""
        row = self.store.connection.execute(
            'SELECT track FROM executor_runs WHERE attempt_id=?', (attempt_id,)
        ).fetchone()
        if row and row['track']:
            return row['track']
        return None

    def circuit_status(self) -> str:
        """获取当前熔断器状态。"""
        return self.circuit.status

    def submit(self, context: ExecutionContext) -> ExecutionOutcome:
        """提交任务：优先主轨，故障/熔断时平滑降级至备轨。"""
        # 1. 检查主轨凭证与熔断状态
        if not self.primary.is_configured() or not self.circuit.can_execute():
            return self.fallback.submit(context)

        # 2. 尝试主轨提交
        try:
            outcome = self.primary.submit(context)
            if outcome.state == 'not_submitted':
                # 若任务已被显式取消，无需向备轨降级重试
                if outcome.evidence in ('cancelled_before_submission', 'cancelled_before_stream'):
                    return outcome
                self.circuit.record_failure()
                return self.fallback.submit(context)
            elif outcome.state == 'accepted':
                self.circuit.record_success()
                return outcome
            elif outcome.state in ('failed', 'unknown'):
                row = self.store.connection.execute(
                    'SELECT platform_run_id FROM executor_runs WHERE attempt_id=?',
                    (context.claim.attempt_id,)
                ).fetchone()
                if not row or not row['platform_run_id']:
                    self.circuit.record_failure()
                    return self.fallback.submit(context)
            return outcome
        except Exception:
            self.circuit.record_failure()
            return self.fallback.submit(context)

    def query(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """根据持久化绑定的 track 严格分发，严禁跨轨串话。"""
        track = self.current_track(context.claim.attempt_id)
        if track == 'gadget':
            try:
                outcome = self.primary.query(context, run_id)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
            if outcome.state == 'failed' and not outcome.message_id:
                # Primary died before its message reached the session (the
                # worker result carries no message_id), so a native retry
                # cannot double-deliver: supersede the gadget binding and
                # fall back. A failed result WITH a message_id means the
                # message was sent and a reply may exist -- never re-send.
                self.circuit.record_failure()
                with self.store.transaction():
                    self.store.connection.execute(
                        "UPDATE executor_runs SET status='failed_superseded' WHERE attempt_id=?",
                        (context.claim.attempt_id,),
                    )
                fallback_outcome = self.fallback.submit(context)
                if fallback_outcome.state == 'accepted' and fallback_outcome.run_id:
                    # The fallback bound a new run for this attempt. The
                    # CoreRunner state machine binds one run per attempt at
                    # submit time and cannot accept a second 'accepted'
                    # outcome during the query phase (mark_submitted
                    # requires the 'reconciling' state), so rebind the
                    # attempt to the native run and report 'running': the
                    # runner keeps polling the live run on the next tick.
                    self.store.rebind_run(context.claim, fallback_outcome.run_id)
                    return ExecutionOutcome('running')
                return fallback_outcome
            return outcome
        elif track == 'native':
            try:
                return self.fallback.query(context, run_id)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
        else:
            return ExecutionOutcome('unknown', evidence='no_track_binding')

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """根据持久化绑定的 track 严格分发取消请求。"""
        track = self.current_track(context.claim.attempt_id)
        if track == 'gadget':
            try:
                return self.primary.cancel(context, run_id)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
        elif track == 'native':
            try:
                return self.fallback.cancel(context, run_id)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
        else:
            return ExecutionOutcome('unknown', evidence='no_track_binding')

    def close(self) -> None:
        """关闭底层资源。"""
        if hasattr(self.primary, 'close'):
            try:
                self.primary.close()
            except Exception:
                pass
        if hasattr(self.fallback, 'close'):
            try:
                self.fallback.close()
            except Exception:
                pass

    def __enter__(self) -> DualTrackExecutor:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
