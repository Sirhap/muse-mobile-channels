"""Muse execution adapter with persisted sessions and explicit run correlation.

The legacy stream API does not promise a run identifier: ``_open`` returns
only a per-connection stream id, never a run id (proven live 2026-11-06).
Correlation therefore rests on three facts recorded before the reply can
exist: the session is dedicated to one conversation generation, the highest
history sequence seen before sending (the baseline), and confirmation that
the sent message itself was recorded in that session's history. A reply is
accepted when it is a ``message.assistant`` event after the baseline with a
completed status. No reply_to exclusion: a genuine turn answer gets stamped
with reply_to_message_id whenever the turn was resumed by an internal
handoff, and the legacy bridge counts it (native_bridge.is_turn_reply,
2026-10-07 20:47 live loss). A submission whose recording was never confirmed stays
unknown, and its history is never trusted.
"""
from __future__ import annotations

import base64
import importlib.util
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .gadget_client import GadgetAuthError, GadgetClient, GadgetCredentials, GadgetError, GadgetNetworkError
from .runtime import ExecutionContext, ExecutionOutcome
from .storage import Store

# History window for reply_started / query. The fold window is early in
# the turn, so 100 events usually cover it, but a long session can push
# the reply-start event out of the latest 100; 500 gives headroom. The
# history API has no after_seq cursor, so this is a plain limit.
HISTORY_LIMIT = 500

# How many extra history fetch rounds MuseExecutor.query waits through
# after the first completed assistant message, one poll_interval apart,
# while new completed messages keep arriving. A turn can legitimately
# split its answer across several assistant messages (proven live
# 2026-11-06: the video reference + link arrived 1.44s after the first
# message, after the executor had already settled). The loop stops early
# the moment a round adds no new completed message, so a single-message
# turn costs exactly one extra poll cycle.
QUERY_SETTLE_ROUNDS = 3

# [[FILE:/absolute/path]] markers, legacy bridge format
# (native_bridge.py _deliver_reply_raw / _deliver_idle). Executors parse
# them out of reply text into ExecutionOutcome.media; text never retains
# a marker once parsed.
FILE_MARKER_RE = re.compile(r"\[\[FILE:([^\]\n]+)\]\]")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".mkv", ".avi"}
IMAGE_MIME_TYPES = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
    ".gif": "image/gif", ".webp": "image/webp",
}
# Same per-image cap as reference/muse-client/src/message.ts.
MAX_IMAGE_BYTES = 2 * 1024 * 1024

# Evidence text for a Gadget cancel. There is no server-side cancel in
# the reference client (no /chat/cancel endpoint; NoiseConnection.open
# only aborts the local request), so killing the worker stops local
# delivery only -- the server turn may keep running. Callers must relay
# this honestly instead of claiming the turn stopped.
GADGET_CANCEL_LOCAL_ONLY = "local_worker_terminated_server_turn_may_continue"


def media_kind_for_path(path: str) -> str:
    """Classify a media path by extension: image, video, or generic file."""
    suffix = Path(path).suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    return "file"


def extract_file_media(text: str) -> tuple[str, list[dict]]:
    """Split [[FILE:path]] markers out of reply text into media items.

    Returns (clean_text, media) where each media item is
    {"kind", "path", "filename"}. Markers with a relative path are
    dropped from the text but produce no media item (only absolute
    paths are deliverable). Duplicates collapse to one item per path.
    """
    if not isinstance(text, str) or not text:
        return "", []
    media: list[dict] = []
    seen: set[str] = set()

    def _replace(match: re.Match) -> str:
        path = match.group(1).strip()
        if path.startswith("/") and path not in seen:
            seen.add(path)
            media.append({"kind": media_kind_for_path(path), "path": path,
                          "filename": Path(path).name})
        return ""

    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        marker = FILE_MARKER_RE.fullmatch(stripped)
        if marker is not None:
            # A marker-only line vanishes entirely (legacy bridge
            # behaviour); inline markers are removed in place.
            _replace(marker)
            continue
        lines.append(FILE_MARKER_RE.sub(_replace, line).rstrip())
    clean = "\n".join(lines).strip()
    return clean, media


def merge_media(*lists: list[dict] | None) -> list[dict]:
    """Merge media lists, deduplicated by path, first occurrence wins."""
    merged: list[dict] = []
    seen: set[str] = set()
    for items in lists:
        for item in items or []:
            if not isinstance(item, dict):
                continue
            path = item.get("path")
            if not isinstance(path, str) or not path or path in seen:
                continue
            seen.add(path)
            merged.append({"kind": item.get("kind") or media_kind_for_path(path),
                           "path": path,
                           "filename": item.get("filename") or Path(path).name})
    return merged


def build_stream_items(text: str, media: list[dict] | None) -> list[dict]:
    """Build chat.stream items for the Native track from text + inbound media.

    Image schema is copied from reference/muse-client/src/message.ts
    chatRequest (the only documented image shape for /chat/stream);
    muse-cli's chat.py itself only ever sends text items, and the Native
    gateway's _open passes the body JSON through verbatim, so these items
    ride the same path. Raises ValueError with a machine-readable
    evidence string for anything that must not be silently dropped:
    non-image media (no video/file item type exists for this endpoint),
    unknown image mime, missing file, or oversize image. Callers fail
    the submit before opening any stream.
    """
    items: list[dict] = []
    if isinstance(text, str) and text.strip():
        items.append({"type": "text", "text": text})
    for entry in media or []:
        if not isinstance(entry, dict):
            raise ValueError("native_media_invalid_entry")
        path = entry.get("path")
        filename = entry.get("filename") or (Path(path).name if isinstance(path, str) else "")
        kind = entry.get("kind") or (media_kind_for_path(path) if isinstance(path, str) else "file")
        if kind != "image":
            raise ValueError(f"native_media_unsupported:{kind}:{filename}")
        mime = IMAGE_MIME_TYPES.get(Path(path).suffix.lower()) if isinstance(path, str) else None
        if mime is None:
            raise ValueError(f"native_media_unsupported_mime:{filename}")
        try:
            data = Path(path).read_bytes()
        except OSError:
            raise ValueError(f"native_media_missing:{path}") from None
        if len(data) > MAX_IMAGE_BYTES:
            raise ValueError(f"native_media_too_large:{filename}")
        items.append({"type": "image", "mime_type": mime, "filename": filename,
                      "data_base64": base64.b64encode(data).decode("ascii")})
    if not items:
        raise ValueError("native_media_empty_message")
    return items

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

    def _confirm_recorded(self, client, session_id: str, baseline: int) -> tuple[bool, int]:
        """Poll history on the same connection until a new event appears.

        ``_open`` is fire-and-forget: the frame alone does not prove the
        message landed. Only a history event after the baseline proves it.
        Returns (confirmed, polls) so an unconfirmed submission can carry
        a countable unknown instead of a bare one.
        """
        deadline = time.time() + self.confirm_timeout
        polls = 0
        while True:
            polls += 1
            response = client.call_json('chat.history', body={
                'limit': 10, 'session_id': session_id,
            }, timeout=15)
            events = response.get('chat_events', []) if isinstance(response, dict) else []
            if any(event.get('seq', 0) > baseline for event in events):
                return True, polls
            if time.time() >= deadline:
                return False, polls
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
            try:
                items = build_stream_items(
                    context.text, getattr(context, 'media', None))
            except ValueError as exc:
                # Unsupported / missing / oversize media: fail before any
                # stream opens, so nothing is half-sent and nothing is
                # silently dropped.
                return ExecutionOutcome('failed', evidence=str(exc))
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
                'items': items, 'node_id': self.node_id,
                'capabilities': {}, 'session_id': sid,
            })
            if isinstance(stream, dict) and isinstance(stream.get('run_id'), str) and stream['run_id']:
                run_id = stream['run_id']
            elif isinstance(stream, int):
                run_id = f'stream:{stream}'
            else:
                return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
            confirmed, polls = self._confirm_recorded(client, sid, baseline)
            if not confirmed:
                return ExecutionOutcome(
                    'unknown', evidence=f'submission_not_recorded polls={polls}')
            with self.store.transaction():
                self.store.connection.execute(
                    "UPDATE executor_runs SET platform_run_id=?,baseline_seq=?,status='accepted' WHERE attempt_id=?",
                    (run_id, baseline, context.claim.attempt_id),
                )
            return ExecutionOutcome('accepted', run_id=run_id)
        except Exception as exc:
            # A bare unknown is uncountable upstream: carry what failed.
            return ExecutionOutcome(
                'unknown', evidence=f"submit_failed:{type(exc).__name__}:{exc}"[:200])
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
        """Read all replies correlated with the confirmed submission.

        The binding guarantees the sent message was recorded; the reply
        is every completed assistant event after the recorded baseline,
        in this attempt's dedicated session. A turn may legitimately
        split its answer across several assistant messages (the video
        reference + link arrived 1.44s after the first message live),
        so the first completed message does NOT settle the turn: after
        it appears, up to QUERY_SETTLE_ROUNDS extra history fetches run
        one poll_interval apart, stopping early when a round adds no new
        completed message, and all collected texts + [[FILE:]] media are
        merged before the outcome is returned. No reply_to exclusion: a
        genuine turn answer gets stamped with reply_to_message_id
        whenever the turn was resumed by an internal handoff, and the
        legacy bridge counts it (native_bridge.is_turn_reply, 2026-10-07
        20:47 live loss). Events at or before the baseline -- including
        any earlier attempt's output in this session -- are ignored.
        """
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        client = self.client_factory()
        try:
            self._validate_owner(context)
            baseline = binding['baseline_seq'] or 0
            texts: list[str] = []
            media: list[dict] = []
            seen_seqs: set[int] = set()
            terminal: str | None = None
            for round_no in range(QUERY_SETTLE_ROUNDS):
                new_found = False
                response = client.call_json('chat.history', body={
                    'limit': HISTORY_LIMIT, 'session_id': binding['session_id'],
                }, timeout=15)
                events = response.get('chat_events', []) if isinstance(response, dict) else []
                for event in sorted(events, key=lambda value: value.get('seq', 0)):
                    if event.get('seq', 0) <= baseline:
                        continue
                    if event.get('event_name') != 'message.assistant':
                        continue
                    payload = event.get('payload') or {}
                    if payload.get('status') in ('cancelled', 'canceled'):
                        terminal = terminal or 'cancelled'
                        continue
                    if payload.get('status') in ('failed', 'error'):
                        terminal = terminal or 'failed'
                        continue
                    if payload.get('status') not in ('completed', 'complete', 'succeeded'):
                        continue
                    seq = event.get('seq', 0)
                    if seq in seen_seqs:
                        continue
                    seen_seqs.add(seq)
                    new_found = True
                    text = payload.get('display_text')
                    if isinstance(text, str) and text:
                        clean, found = extract_file_media(text)
                        if clean:
                            texts.append(clean)
                        media = merge_media(media, found)
                if not texts and terminal is None:
                    return ExecutionOutcome('running')
                if not new_found:
                    break
                if round_no < QUERY_SETTLE_ROUNDS - 1:
                    # A new message arrived this round; wait one poll
                    # cycle for a possible follow-up before settling.
                    time.sleep(self.poll_interval)
            if texts:
                return ExecutionOutcome('succeeded', text="\n".join(texts), media=media)
            if terminal == 'cancelled':
                return ExecutionOutcome('cancelled', evidence='platform_run_cancelled')
            return ExecutionOutcome('failed', evidence='platform_run_failed')
        except Exception as exc:
            return ExecutionOutcome(
                'unknown', evidence=f"query_failed:{type(exc).__name__}:{exc}"[:200])
        finally:
            client.close()

    def reply_started(self, context: ExecutionContext, run_id: str | None) -> bool:
        """True when the live turn's reply has started generating.

        Any assistant message after the submission baseline — any status,
        not just completed — means the model has begun its reply, so a
        fold would arrive too late to be integrated. Fail safe: when the
        binding is missing or history cannot be read, report started so
        the caller does NOT fold.
        """
        try:
            self._validate_owner(context)
        except Exception:
            return True
        binding = self._binding(context, run_id)
        if binding is None:
            return True
        client = self.client_factory()
        try:
            response = client.call_json('chat.history', body={
                'limit': HISTORY_LIMIT, 'session_id': binding['session_id'],
            }, timeout=15)
            events = response.get('chat_events', []) if isinstance(response, dict) else []
            baseline = binding['baseline_seq'] or 0
            for event in events:
                if event.get('seq', 0) <= baseline:
                    continue
                if event.get('event_name') != 'message.assistant':
                    continue
                # No reply_to exclusion: a genuine answer gets stamped
                # with reply_to_message_id when the turn was resumed by
                # an internal handoff (legacy poll_turn counts any
                # assistant event after baseline as started).
                return True
            return False
        except Exception:
            return True
        finally:
            client.close()

    def fold(self, context: ExecutionContext, run_id: str | None, text: str) -> ExecutionOutcome:
        """Send follow-up text into the live session (official-client steering).

        Opens a new stream on the same session with the follow-up text,
        mirroring the legacy bridge's _merge_into_active. The text is
        only reported accepted after history confirms it was recorded;
        otherwise the outcome is unknown and the caller must NOT mark
        the message merged.
        """
        if not isinstance(text, str) or not text:
            return ExecutionOutcome('failed', evidence='fold_text_empty')
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        client = self.client_factory()
        try:
            self._validate_owner(context)
            sid = binding['session_id']
            try:
                pre = self._baseline(client, sid)
            except Exception:
                return ExecutionOutcome('unknown', evidence='baseline_unavailable')
            client._open('chat.stream', body={
                'items': [{'type': 'text', 'text': text}], 'node_id': self.node_id,
                'capabilities': {}, 'session_id': sid,
            })
            confirmed, polls = self._confirm_recorded(client, sid, pre)
            if not confirmed:
                return ExecutionOutcome(
                    'unknown', evidence=f'fold_not_recorded polls={polls}')
            return ExecutionOutcome('accepted', run_id=run_id)
        except Exception as exc:
            return ExecutionOutcome(
                'unknown', evidence=f"fold_failed:{type(exc).__name__}:{exc}"[:200])
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

    History fallback (2026-11-07): the worker waits on the live
    /chat/subscribe stream for a task.status terminal event, which arrived
    ~200s late in production (turns of 213-304s while the reply was
    already in history). Gadget Noise /chat/history is 403 (endpoint not
    in the device-token ACL), but the Cookie bridge reads a Gadget-created
    session's history fine, so when history_client_factory is set,
    query() settles a still-running worker from history once a completed
    assistant message appears after this attempt's sent message. The
    fallback is best-effort and never raises: without a factory, without
    an anchor, or on any history failure, query() keeps the old
    running/failed path, so the Gadget track stays usable when the bridge
    credentials are dead.
    """

    def __init__(
        self,
        store: Store,
        client: GadgetClient | None = None,
        client_factory: Callable[[], GadgetClient] | None = None,
        history_client_factory: Callable[[], Any] | None = None,
    ):
        self.store = store
        self._client = client
        self.client_factory = client_factory
        # Optional Cookie-bridge client factory for the history fallback
        # (bridge_client by default in DualTrackExecutor; None disables it).
        self.history_client_factory = history_client_factory
        # Per-attempt query poll counts, so an unknown outcome can carry
        # countable evidence (polls=N) instead of a bare unknown.
        self._query_counts: dict[str, int] = {}
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
        if getattr(context, 'media', None):
            # The Gadget worker's sendMessage is text-only; a media task
            # must never be silently text-stripped here. DualTrack routes
            # media tasks to the Native track before reaching this point;
            # a direct Gadget submit fails explicitly instead.
            return ExecutionOutcome(
                'failed', evidence='gadget_media_not_supported: gadget worker sends text only')

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
        """Read the worker's result file; a live worker with no file yet is running.

        The worker merges every bound reply of the turn into one result
        (text + media); [[FILE:]] markers are parsed into outcome.media
        here as well, so a stale worker build can never leak a marker
        into delivered text. A dead worker whose result file still says
        'running' is a determinable terminal failure (no reply will ever
        be collected on this track), not 'running' forever; its
        message_id is preserved so DualTrack never re-sends a message
        that already landed. A dead worker with no result file at all is
        genuinely unknown -- but its evidence carries the poll count.
        """
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        attempt_id = context.claim.attempt_id
        self._query_counts[attempt_id] = self._query_counts.get(attempt_id, 0) + 1
        polls = self._query_counts[attempt_id]

        try:
            result = self.client.read_result(attempt_id)
        except Exception as exc:
            return ExecutionOutcome(
                'unknown', evidence=f"read_result_failed:{type(exc).__name__}:{exc}"[:200])
        if result is None:
            pid = self._worker_pid(binding['platform_run_id'])
            if pid is not None and self._worker_alive(pid):
                return ExecutionOutcome('running')
            return ExecutionOutcome(
                'unknown', evidence=f'worker_exited_without_result polls={polls}')

        state = result.get('state')
        mid = result.get('message_id')
        message_id = mid if isinstance(mid, str) and mid else None
        if state == 'running':
            settled = self._settle_from_history(binding, message_id)
            if settled is not None:
                return settled
            pid = self._worker_pid(binding['platform_run_id'])
            if pid is not None and not self._worker_alive(pid):
                # The driver is dead mid-turn: the reply will never be
                # collected. Terminal failure, with the message id kept
                # so the fallback path cannot double-deliver.
                return ExecutionOutcome(
                    'failed', evidence=f'worker_died_mid_turn polls={polls}',
                    message_id=message_id)
            return ExecutionOutcome('running', message_id=message_id)
        if state == 'succeeded':
            text = result.get('text')
            if not isinstance(text, str) or not text:
                return ExecutionOutcome(
                    'unknown', evidence=f'worker_succeeded_without_text polls={polls}',
                    message_id=message_id)
            clean_text, marker_media = extract_file_media(text)
            media = merge_media(result.get('media'), marker_media)
            return ExecutionOutcome(
                'succeeded', text=clean_text, message_id=message_id, media=media)
        if state == 'failed':
            return ExecutionOutcome('failed', evidence=str(result.get('evidence') or 'platform_turn_failed'), message_id=message_id)
        if state == 'cancelled':
            return ExecutionOutcome('cancelled', evidence=str(result.get('evidence') or 'platform_turn_cancelled'), message_id=message_id)
        if state == 'unknown':
            return ExecutionOutcome(
                'unknown',
                evidence=f"{result.get('evidence') or 'worker_unknown'} polls={polls}",
                message_id=message_id)
        return ExecutionOutcome(
            'unknown', evidence=f'unrecognized_result_state_{state} polls={polls}',
            message_id=message_id)

    def _settle_from_history(self, binding, ack_message_id: str | None):
        """Settle a still-running worker from Cookie-bridge history, if possible.

        Anchors on this attempt's sent user message (its message id is the
        worker result's message_id) and accepts completed assistant messages
        after it -- the same correlation MuseExecutor.query uses, minus the
        pre-send baseline (the anchor plays that role). Returns a succeeded
        outcome, or None when the fallback cannot settle yet. Never raises:
        any failure returns None and the caller keeps the old path.
        """
        if self.history_client_factory is None or not ack_message_id:
            return None
        session_id = binding['session_id'] if binding is not None else None
        if not session_id:
            return None
        try:
            client = self.history_client_factory()
        except Exception:
            return None
        try:
            texts, media, last_id = self._collect_completed_after_anchor(
                client, session_id, ack_message_id)
        except Exception:
            return None
        finally:
            try:
                close = getattr(client, 'close', None)
                if callable(close):
                    close()
            except Exception:
                pass
        if not texts:
            return None
        # The reply is already collected from authoritative history: stop
        # the still-waiting worker so it does not linger on the late
        # subscription terminal. Best effort; a failure here changes
        # nothing -- the worker's own result is ignored once settled.
        pid = self._worker_pid(binding['platform_run_id'])
        if pid is not None:
            try:
                self.client.stop_turn(pid)
            except Exception:
                pass
        return ExecutionOutcome(
            'succeeded', text='\n'.join(texts), message_id=last_id, media=media)

    def _collect_completed_after_anchor(self, client, session_id: str, ack_message_id: str):
        """Fetch history until a settle round adds no new completed reply.

        Returns (texts, media, last_message_id); empty texts means no
        completed reply after the anchor yet. Raises on transport failure.
        Mirrors MuseExecutor.query's settle rounds: a turn may split its
        answer across several assistant messages, so after the first
        completed message, up to QUERY_SETTLE_ROUNDS extra fetches run one
        second apart, stopping early when a round adds nothing new.
        """
        texts: list[str] = []
        media: list[dict] = []
        seen_seqs: set[int] = set()
        last_id: str | None = None
        anchor_seq: int | None = None
        for round_no in range(QUERY_SETTLE_ROUNDS):
            response = client.call_json('chat.history', body={
                'limit': HISTORY_LIMIT, 'session_id': session_id,
            }, timeout=15)
            events = response.get('chat_events', []) if isinstance(response, dict) else []
            if anchor_seq is None:
                for event in events:
                    if (event.get('event_name') == 'message.user'
                            and event.get('message_id') == ack_message_id):
                        anchor_seq = event.get('seq', 0)
                        break
                if anchor_seq is None:
                    return [], [], None
            new_found = False
            for event in sorted(events, key=lambda value: value.get('seq', 0)):
                if event.get('seq', 0) <= anchor_seq:
                    continue
                if event.get('event_name') != 'message.assistant':
                    continue
                payload = event.get('payload') or {}
                if payload.get('status') not in ('completed', 'complete', 'succeeded'):
                    continue
                seq = event.get('seq', 0)
                if seq in seen_seqs:
                    continue
                seen_seqs.add(seq)
                new_found = True
                text = payload.get('display_text')
                if isinstance(text, str) and text:
                    clean, found = extract_file_media(text)
                    if clean:
                        texts.append(clean)
                    media = merge_media(media, found)
                mid = event.get('message_id')
                if isinstance(mid, str) and mid:
                    last_id = mid
            if not texts:
                return [], [], None
            if not new_found:
                break
            if round_no < QUERY_SETTLE_ROUNDS - 1:
                time.sleep(1.0)
        return texts, media, last_id

    def _worker_alive(self, pid: int) -> bool:
        """True when the worker is alive; an unreadable state fails safe to alive.

        Uses the bound client's own liveness check (the real GadgetClient
        reads /proc; test fakes keep an in-memory set).
        """
        alive = getattr(self.client, "worker_alive", None)
        if callable(alive):
            try:
                return bool(alive(pid))
            except Exception:
                return True
        try:
            return GadgetClient.worker_alive(pid)
        except Exception:
            return True

    def reply_started(self, context: ExecutionContext, run_id: str | None) -> bool:
        """True when the live turn's reply has started generating.

        The worker records reply_started in its result file the first
        time it observes an assistant message with text. A terminal
        result also means the reply started (it finished). Fail safe:
        when the binding is missing or the result cannot be read,
        report started so the caller does NOT fold.
        """
        try:
            self._validate_owner(context)
        except Exception:
            return True
        binding = self._binding(context, run_id)
        if binding is None:
            return True
        try:
            result = self.client.read_result(context.claim.attempt_id)
        except Exception:
            return True
        if result is None:
            # No result file yet. The worker writes its running
            # placeholder shortly after spawn (connect + sendMessage).
            # If the attempt is old enough that it should have written
            # one, no file means the worker died before writing and
            # the turn is dead — do NOT fold (the combined reply will
            # never come). A fresh attempt may simply not have written
            # yet; folding is allowed then.
            try:
                age = self.store.clock() - float(binding['created_at'] or 0)
            except Exception:
                age = 0.0
            return age >= 30.0
        if result.get('reply_started') is True:
            return True
        if result.get('state') in ('succeeded', 'failed', 'cancelled', 'unknown'):
            return True
        return False

    def fold(self, context: ExecutionContext, run_id: str | None, text: str) -> ExecutionOutcome:
        """Send follow-up text into the live side session (official-client steering).

        Spawns a send-only worker that sends the text to the same side
        session and exits without subscribing, so it never races the
        running turn's own worker. The fold is only reported accepted
        when the send-only worker's result carries a message id (the
        send landed); otherwise the outcome is unknown and the caller
        must NOT mark the message merged.
        """
        if not isinstance(text, str) or not text:
            return ExecutionOutcome('failed', evidence='fold_text_empty')
        self._validate_owner(context)
        binding = self._binding(context, run_id)
        if binding is None:
            return ExecutionOutcome('unknown', evidence='platform_run_identity_unavailable')
        try:
            session_id = self._session(context)
            result = self.client.send_only(session_id, text)
        except Exception as exc:
            return ExecutionOutcome('unknown', evidence=str(exc))
        if isinstance(result, dict) and result.get('state') == 'sent':
            mid = result.get('message_id')
            if isinstance(mid, str) and mid:
                return ExecutionOutcome('accepted', run_id=run_id, message_id=mid)
        return ExecutionOutcome('unknown', evidence='fold_send_unconfirmed')

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Kill the local worker and report the cancel honestly.

        There is no server-side cancel in the reference client (checked
        reference/muse-client/src/client.ts + connection.ts: the only
        cancellation is a local AbortSignal on the Noise request, and no
        /chat/cancel endpoint exists), so killing the worker stops local
        delivery only -- the server turn may keep running and its reply
        will simply never be collected here. The outcome is therefore
        'cancelled' with evidence that says exactly that, never a bare
        claim that the turn stopped.
        """
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
            evidence = str(result.get('evidence') or 'platform_turn_cancelled')
            if 'server_turn_may_continue' not in evidence:
                evidence = f"{evidence}; server_turn_may_continue"
            return ExecutionOutcome('cancelled', evidence=evidence)
        return ExecutionOutcome('cancelled', evidence=GADGET_CANCEL_LOCAL_ONLY)

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
        self.primary = primary or GadgetExecutor(store, history_client_factory=bridge_client)
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
        """提交任务：优先主轨，故障/熔断时平滑降级至备轨。

        带媒体的任务不走 Gadget 轨：Gadget worker 只发纯文本，直接送
        过去会把媒体静默丢掉，所以媒体任务直走 Native 备轨（它按
        reference/muse-client message.ts 的图片 schema 发图）。
        """
        # Media tasks never touch the Gadget track: its worker sends
        # text only, so routing them there would silently drop the media.
        if getattr(context, 'media', None):
            return self.fallback.submit(context)
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
                try:
                    fallback_outcome = self.fallback.submit(context)
                except Exception as exc:
                    # The fallback never bound a run: fail explicitly with
                    # evidence instead of bubbling into an unknown wedge.
                    return ExecutionOutcome(
                        'failed',
                        evidence=f"fallback_submit_failed:{type(exc).__name__}:{exc}"[:200])
                if fallback_outcome.state == 'accepted' and fallback_outcome.run_id:
                    # The fallback bound a new run for this attempt. The
                    # CoreRunner state machine binds one run per attempt at
                    # submit time and cannot accept a second 'accepted'
                    # outcome during the query phase (mark_submitted
                    # requires the 'reconciling' state), so rebind the
                    # attempt to the native run and report 'running': the
                    # runner keeps polling the live run on the next tick.
                    try:
                        self.store.rebind_run(context.claim, fallback_outcome.run_id)
                    except Exception as exc:
                        # The native run is live but the runner can no
                        # longer poll it (the attempt still points at the
                        # dead gadget run). Fail explicitly with evidence
                        # instead of letting the exception wedge the task
                        # in unknown forever.
                        return ExecutionOutcome(
                            'failed',
                            evidence=f"fallback_rebind_failed:{type(exc).__name__}:{exc}"[:200])
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

    def reply_started(self, context: ExecutionContext, run_id: str | None) -> bool:
        """Route the reply-started check to the track bound to this attempt.

        Fail safe: no track binding means the reply is treated as started,
        so the caller does NOT fold.
        """
        track = self.current_track(context.claim.attempt_id)
        if track == 'gadget':
            try:
                return self.primary.reply_started(context, run_id)
            except Exception:
                return True
        elif track == 'native':
            try:
                return self.fallback.reply_started(context, run_id)
            except Exception:
                return True
        else:
            return True

    def fold(self, context: ExecutionContext, run_id: str | None, text: str) -> ExecutionOutcome:
        """Route the fold send to the track bound to this attempt."""
        track = self.current_track(context.claim.attempt_id)
        if track == 'gadget':
            try:
                return self.primary.fold(context, run_id, text)
            except Exception as exc:
                return ExecutionOutcome('unknown', evidence=str(exc))
        elif track == 'native':
            try:
                return self.fallback.fold(context, run_id, text)
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
