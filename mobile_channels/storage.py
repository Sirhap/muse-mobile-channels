"""SQLite task authority with short transactions and no transport side effects.

Call ``begin_submission`` before contacting an executor. Once that transaction
commits, a crash is treated as an uncertain external result, never as permission
to submit again. This store is currently opt-in and does not replace live JSONL
queues automatically.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from .models import Claim, InboundMessage, IngestResult, OwnershipError, StateError

LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 1
ACTIVE = ("claimed", "running", "reconciling")
TERMINAL = ("succeeded", "failed", "cancelled")
# Consecutive-unknown budget for one execution attempt: an executor that
# keeps answering 'unknown' is not making progress. Ten polls is enough
# to ride out transient stalls (a poll is normally seconds apart) while
# bounding the hang to a small multiple of a normal turn; 900 seconds
# matches the Gadget turn timeout, so a turn that never resolves within
# one full timeout budget is declared failed instead of hanging forever.
UNKNOWN_MAX_POLLS = 10
UNKNOWN_MAX_SECONDS = 900.0
# A reconciling task with no bound execution may only wait one full
# Gadget timeout (900s) for reconciliation before it is failed; a bound
# run stays reconciling so its result can still be recovered.
RECONCILING_MAX_AGE_SECONDS = 900.0
RECEIPT_KINDS = ("started", "wait", "failed", "cancelled", "fallback",
                 "delivery_failed")
FAILED_RECEIPT_TEXT = "处理失败，请再发一次。"
CANCELLED_RECEIPT_TEXT = "已取消。"
FALLBACK_RECEIPT_TEXT = "主通道失败，正在走备用通道，请稍等。"
# Only explain a fallback when the primary burned real waiting time
# first. A fast primary failure (auth/token, circuit open) falls back
# within seconds and the backup answer lands almost as fast as a normal
# turn, so the receipt would be pure noise.
FALLBACK_RECEIPT_MIN_WAIT_SECS = 15.0
DELIVERY_FAILED_RECEIPT_TEXT = "回复已生成但发送失败，请再发一次。"
# Bound reply for a message folded into a running turn (official-client
# steering, matching native-bridge MERGE_POINTER_TEXT): the merged
# msgid gets this short pointer instead of its own full reply, so the
# gateway's think stream finishes in place and the user sees the
# message was absorbed into the running turn's combined answer.
MERGE_POINTER_TEXT = "（已并入上一条处理）"
SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY, applied_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY, channel TEXT NOT NULL, account_id TEXT NOT NULL,
    chat_type TEXT NOT NULL, chat_id TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
    UNIQUE(channel, account_id, chat_type, chat_id)
);
CREATE TABLE IF NOT EXISTS inbound_messages (
    id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
    external_id TEXT NOT NULL, sender_id TEXT NOT NULL, text TEXT NOT NULL,
    received_at REAL NOT NULL, UNIQUE(conversation_id, external_id)
);
CREATE TABLE IF NOT EXISTS tasks (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
    message_id TEXT NOT NULL UNIQUE REFERENCES inbound_messages(id),
    conversation_id TEXT NOT NULL REFERENCES conversations(id), generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN
        ('queued','claimed','running','reconciling','succeeded','failed','cancelled')),
    cancel_requested INTEGER NOT NULL DEFAULT 0, token INTEGER NOT NULL DEFAULT 0,
    owner TEXT, lease_until REAL, attempt_id TEXT, result TEXT,
    media_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS task_queue ON tasks(state, sequence);
CREATE INDEX IF NOT EXISTS task_conversation ON tasks(conversation_id, sequence);
CREATE TABLE IF NOT EXISTS execution_attempts (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
    owner TEXT NOT NULL, token INTEGER NOT NULL, submission TEXT NOT NULL
        CHECK(submission IN ('not_submitted','unknown','accepted')),
    run_id TEXT, created_at REAL NOT NULL, UNIQUE(task_id,token)
);
CREATE TABLE IF NOT EXISTS task_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
    event TEXT NOT NULL, created_at REAL NOT NULL, evidence TEXT
);
CREATE TABLE IF NOT EXISTS outbound_messages (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
    conversation_id TEXT NOT NULL REFERENCES conversations(id), generation INTEGER NOT NULL,
    text TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','sending','retry_wait','delivered','delivery_unknown','rejected','cancelled')),
    created_at REAL NOT NULL, media_json TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS task_receipts (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
    kind TEXT NOT NULL, text TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','delivered','failed')),
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS receipt_task_kind ON task_receipts(task_id, kind);
"""


def _identifier() -> str:
    """Generate an opaque identifier independent of user-controlled routes."""
    return uuid.uuid4().hex


class Store:
    """One connection per worker; SQLite serializes writes across processes."""

    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time):
        """Open a local database; refuse schemas produced by newer code."""
        self.clock = clock
        if str(path) != ":memory:":
            try:
                descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(descriptor)
        self.connection = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        self.connection.row_factory = sqlite3.Row
        try:
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA busy_timeout=5000")
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=FULL")
            with self._transaction():
                version = self.connection.execute("PRAGMA user_version").fetchone()[0]
                if version > SCHEMA_VERSION:
                    raise StateError(f"unsupported schema version: {version}")
                # executescript commits implicitly, so execute individual DDL here.
                for statement in SCHEMA.split(";"):
                    if statement.strip():
                        self.connection.execute(statement)
                self._migrate_columns()
                self.connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations VALUES (?,?)",
                    (SCHEMA_VERSION, self.clock()),
                )
                self.connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        except BaseException:
            self.connection.close()
            raise

    def __enter__(self) -> Store:
        """Allow explicit deterministic connection cleanup."""
        return self

    def __exit__(self, *args: object) -> None:
        """Close the connection after the caller's operation completes."""
        self.close()

    def close(self) -> None:
        """Release this connection; committed WAL data remains durable."""
        self.connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Reserve the writer before evaluating invariants, then commit once."""
        nested = self.connection.in_transaction
        savepoint = "nested_" + uuid.uuid4().hex
        self.connection.execute(f"SAVEPOINT {savepoint}" if nested else "BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute(f"RELEASE {savepoint}" if nested else "COMMIT")
        except BaseException:
            if nested:
                self.connection.execute(f"ROLLBACK TO {savepoint}")
                self.connection.execute(f"RELEASE {savepoint}")
            else:
                self.connection.execute("ROLLBACK")
            raise

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Expose a short extension-table transaction; nested operations use savepoints."""
        with self._transaction():
            yield

    def _migrate_columns(self) -> None:
        """Add media_json to tables created before this column existed.

        Runs inside the constructor transaction; CREATE TABLE IF NOT
        EXISTS leaves an old table untouched, so the column is added
        explicitly when a PRAGMA probe shows it missing.
        """
        for table in ("tasks", "outbound_messages"):
            columns = {row["name"] for row in self.connection.execute(
                f"PRAGMA table_info({table})")}
            if "media_json" not in columns:
                self.connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN media_json TEXT NOT NULL DEFAULT '[]'")

    def _insert_receipt(self, task_id: str, conversation_id: str, kind: str,
                        text: str) -> str:
        """Insert one receipt row; the caller must hold the transaction."""
        receipt_id = _identifier()
        self.connection.execute(
            "INSERT INTO task_receipts(id,task_id,conversation_id,kind,text,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (receipt_id, task_id, conversation_id, kind, text, self.clock()),
        )
        return receipt_id

    def add_receipt(self, task_id: str, conversation_id: str, kind: str,
                    text: str) -> str:
        """Write an instantaneous user notice, idempotently.

        A second pending receipt for the same task_id+kind is a duplicate
        and returns the existing id instead of inserting. 'wait' is the
        exception: queue position changes over time, so repeated wait
        receipts are allowed (callers still check before writing).
        """
        if kind not in RECEIPT_KINDS:
            raise ValueError(f"unknown receipt kind: {kind}")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("receipt text is required")
        with self._transaction():
            if kind != "wait":
                existing = self.connection.execute(
                    "SELECT id FROM task_receipts WHERE task_id=? AND kind=? "
                    "AND state='pending' ORDER BY created_at LIMIT 1",
                    (task_id, kind),
                ).fetchone()
                if existing is not None:
                    return existing["id"]
            return self._insert_receipt(task_id, conversation_id, kind, text)

    def _merged_tasks(self, task_id: str) -> list[dict]:
        """Tasks folded into ``task_id`` that only ever got a pointer reply.

        Primary source is task_events ('merged_into' with the running task
        as evidence). As a fallback, gateway_routes.merged_ids_json lists
        the external ids coalesced into the running task; those resolve
        back to tasks through inbound_messages in the same conversation.
        """
        merged: dict[str, dict] = {}
        for row in self.connection.execute(
                "SELECT task_id FROM task_events WHERE event='merged_into' AND evidence=?",
                (task_id,)).fetchall():
            merged[row["task_id"]] = {"task_id": row["task_id"]}
        tables = {r[0] for r in self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "gateway_routes" in tables:
            route = self.connection.execute(
                "SELECT merged_ids_json FROM gateway_routes WHERE task_id=?",
                (task_id,)).fetchone()
            if route is not None:
                try:
                    external_ids = json.loads(route["merged_ids_json"] or "[]")
                except (ValueError, TypeError):
                    external_ids = []
                conv = self.connection.execute(
                    "SELECT conversation_id FROM tasks WHERE id=?", (task_id,)).fetchone()
                if conv is not None and isinstance(external_ids, list):
                    for external_id in external_ids:
                        found = self.connection.execute(
                            "SELECT t.id FROM tasks t JOIN inbound_messages m "
                            "ON m.id=t.message_id WHERE m.conversation_id=? "
                            "AND m.external_id=? AND t.id!=?",
                            (conv["conversation_id"], str(external_id), task_id)).fetchone()
                        if found is not None:
                            merged[found["id"]] = {"task_id": found["id"]}
        result = []
        for merged_id in merged:
            row = self.connection.execute(
                "SELECT conversation_id FROM tasks WHERE id=?", (merged_id,)).fetchone()
            if row is not None:
                result.append({"task_id": merged_id,
                               "conversation_id": row["conversation_id"]})
        return result

    def _notify_merged(self, task_id: str, kind: str, text: str) -> None:
        """Give every task folded into ``task_id`` its own receipt.

        The merged task's own id is the receipt's task_id, so its
        delivery route is independent of the main task's outcome.
        """
        for merged in self._merged_tasks(task_id):
            self._insert_receipt(merged["task_id"], merged["conversation_id"],
                                 kind, text)

    def _fail_task_in_transaction(self, task_id: str, conversation_id: str,
                                  evidence: str, event: str = "confirmed_failed") -> None:
        """Mark a task failed with its receipt; caller holds the transaction."""
        self.connection.execute(
            "UPDATE tasks SET state='failed',owner=NULL,lease_until=NULL WHERE id=?",
            (task_id,),
        )
        self._event(task_id, event, evidence)
        self._insert_receipt(task_id, conversation_id, "failed", FAILED_RECEIPT_TEXT)
        self._notify_merged(task_id, "failed", FAILED_RECEIPT_TEXT)

    def note_unknown(self, claim: Claim) -> bool:
        """Record one 'unknown' poll for this attempt; True when the budget is spent.

        Counts unknown polls per attempt (evidence carries the attempt id,
        so a re-claim starts a fresh budget) and the wall-clock span since
        the first unknown poll of this attempt. The budget is spent at
        10 polls or 900 seconds, whichever comes first.
        """
        with self._transaction():
            row = self._owned(claim)
            self._event(claim.task_id, "unknown_polled", claim.attempt_id)
            rows = self.connection.execute(
                "SELECT created_at FROM task_events WHERE task_id=? AND event='unknown_polled' "
                "AND evidence=? ORDER BY created_at",
                (claim.task_id, claim.attempt_id),
            ).fetchall()
            if len(rows) >= UNKNOWN_MAX_POLLS:
                return True
            if rows and self.clock() - rows[0]["created_at"] >= UNKNOWN_MAX_SECONDS:
                return True
            return False

    def _event(self, task_id: str, event: str, evidence: str | None = None) -> None:
        """Record a metadata-only transition inside the current transaction."""
        self.connection.execute(
            "INSERT INTO task_events(task_id,event,created_at,evidence) VALUES (?,?,?,?)",
            (task_id, event, self.clock(), evidence),
        )

    def ingest(self, message: InboundMessage, initial_state: str = "queued") -> IngestResult:
        """Commit a route, message, and one task atomically and idempotently.

        Shadow imports may use ``reconciling`` for records whose execution status
        cannot be proven. Such tasks block execution until explicitly reconciled.
        """
        if initial_state not in ("queued", "reconciling"):
            raise ValueError("initial_state must be queued or reconciling")
        if not all(isinstance(value, str) and value.strip() for value in (
            message.channel, message.account_id, message.chat_type, message.chat_id,
            message.message_id, message.sender_id,
        )):
            raise ValueError("message identities must be nonempty strings")
        if not isinstance(message.text, str):
            raise ValueError("message text must be a string")
        with self._transaction():
            self.connection.execute(
                "INSERT OR IGNORE INTO conversations(id,channel,account_id,chat_type,chat_id) "
                "VALUES (?,?,?,?,?)",
                (_identifier(), message.channel, message.account_id, message.chat_type, message.chat_id),
            )
            conversation = self.connection.execute(
                "SELECT * FROM conversations WHERE channel=? AND account_id=? AND chat_type=? AND chat_id=?",
                (message.channel, message.account_id, message.chat_type, message.chat_id),
            ).fetchone()
            existing = self.connection.execute(
                "SELECT t.id FROM inbound_messages m JOIN tasks t ON t.message_id=m.id "
                "WHERE m.conversation_id=? AND m.external_id=?",
                (conversation["id"], message.message_id),
            ).fetchone()
            if existing:
                return IngestResult(conversation["id"], existing["id"], False)
            message_id, task_id = _identifier(), _identifier()
            self.connection.execute(
                "INSERT INTO inbound_messages VALUES (?,?,?,?,?,?)",
                (message_id, conversation["id"], message.message_id, message.sender_id,
                 message.text, self.clock() if message.received_at is None else message.received_at),
            )
            self.connection.execute(
                "INSERT INTO tasks(id,message_id,conversation_id,generation,state) VALUES (?,?,?,?,?)",
                (task_id, message_id, conversation["id"], conversation["generation"], initial_state),
            )
            self._event(task_id, f"ingested:{initial_state}")
            return IngestResult(conversation["id"], task_id, True)

    def new_generation(self, conversation_id: str) -> int:
        """Advance context without mutating older tasks or their reply routes."""
        with self._transaction():
            updated = self.connection.execute(
                "UPDATE conversations SET generation=generation+1 WHERE id=? RETURNING generation",
                (conversation_id,),
            ).fetchone()
            if updated is None:
                raise KeyError(conversation_id)
            return updated[0]

    def _expire_leases(self) -> None:
        """Requeue only attempts proven unsubmitted; reserve uncertain sessions.

        A reconciling task whose execution was never bound (no run id)
        and whose attempt started over 900 seconds ago is failed instead
        of being parked in reconciling forever: there is nothing to
        reconcile against. A bound run stays reconciling so its result
        can still be recovered. Ownerless reconciling tasks (lease
        already cleared) get the same age check via the orphan sweep.
        """
        rows = self.connection.execute(
            "SELECT t.id,a.submission,a.run_id,a.created_at,t.cancel_requested,"
            "t.conversation_id FROM tasks t "
            "JOIN execution_attempts a ON a.id=t.attempt_id "
            "WHERE t.state IN ('claimed','running','reconciling') AND t.lease_until<=?",
            (self.clock(),),
        ).fetchall()
        for row in rows:
            if row["submission"] == "not_submitted":
                state = "cancelled" if row["cancel_requested"] else "queued"
                self.connection.execute(
                    "UPDATE tasks SET state=?,owner=NULL,lease_until=NULL,token=token+1 WHERE id=?",
                    (state, row["id"]),
                )
                self._event(row["id"], f"lease_expired:{state}")
            elif (row["submission"] == "unknown" and row["run_id"] is None
                    and self.clock() - row["created_at"] >= RECONCILING_MAX_AGE_SECONDS):
                self._fail_task_in_transaction(
                    row["id"], row["conversation_id"],
                    "reconciling_expired_without_execution_binding",
                    event="reconciling_expired_failed")
            else:
                self.connection.execute(
                    "UPDATE tasks SET state='reconciling',owner=NULL,lease_until=NULL,token=token+1 WHERE id=?",
                    (row["id"],),
                )
                self._event(row["id"], "lease_expired:reconciling")
        orphans = self.connection.execute(
            "SELECT t.id,t.conversation_id,a.created_at FROM tasks t "
            "JOIN execution_attempts a ON a.id=t.attempt_id "
            "WHERE t.state='reconciling' AND t.owner IS NULL AND t.lease_until IS NULL "
            "AND a.submission='unknown' AND a.run_id IS NULL",
        ).fetchall()
        for row in orphans:
            if self.clock() - row["created_at"] >= RECONCILING_MAX_AGE_SECONDS:
                self._fail_task_in_transaction(
                    row["id"], row["conversation_id"],
                    "reconciling_expired_without_execution_binding",
                    event="reconciling_expired_failed")

    def claim(self, owner: str, concurrency: int = 4, lease_seconds: float = 60) -> Claim | None:
        """Claim the oldest eligible task while enforcing session and global limits."""
        if not owner or concurrency < 1 or lease_seconds <= 0:
            raise ValueError("owner, positive concurrency and lease_seconds are required")
        with self._transaction():
            self._expire_leases()
            count = self.connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE state IN ('claimed','running','reconciling')"
            ).fetchone()[0]
            if count >= concurrency:
                return None
            row = self.connection.execute(
                "SELECT t.* FROM tasks t WHERE t.state='queued' AND NOT EXISTS ("
                "SELECT 1 FROM tasks earlier WHERE earlier.conversation_id=t.conversation_id "
                "AND (earlier.state IN ('claimed','running','reconciling') "
                "OR (earlier.state='queued' AND earlier.sequence<t.sequence))) "
                "ORDER BY t.sequence LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            attempt_id, token = _identifier(), row["token"] + 1
            self.connection.execute(
                "INSERT INTO execution_attempts VALUES (?,?,?,?,?,?,?)",
                (attempt_id, row["id"], owner, token, "not_submitted", None, self.clock()),
            )
            self.connection.execute(
                "UPDATE tasks SET state='claimed',owner=?,lease_until=?,attempt_id=?,token=? WHERE id=?",
                (owner, self.clock() + lease_seconds, attempt_id, token, row["id"]),
            )
            self._event(row["id"], "claimed")
            return Claim(row["id"], row["conversation_id"], row["generation"], owner, attempt_id, token)

    def _owned(self, claim: Claim) -> sqlite3.Row:
        """Reject expired, forged, terminal, or superseded worker capabilities."""
        row = self.connection.execute("SELECT * FROM tasks WHERE id=?", (claim.task_id,)).fetchone()
        if (row is None or row["state"] not in ACTIVE or row["owner"] != claim.owner
                or row["attempt_id"] != claim.attempt_id or row["token"] != claim.token
                or row["conversation_id"] != claim.conversation_id or row["generation"] != claim.generation
                or row["lease_until"] is None or row["lease_until"] <= self.clock()):
            raise OwnershipError("worker ownership is expired or superseded")
        return row

    def claim_reconciliation(self, task_id: str, owner: str, lease_seconds: float = 60) -> Claim | None:
        """Acquire exclusive query-only responsibility for an uncertain execution.

        This capability allows recording a verified run/result, but cannot start
        another external submission. Adapters must query the existing execution.
        """
        if not owner or lease_seconds <= 0:
            raise ValueError("owner and positive lease_seconds are required")
        with self._transaction():
            self._expire_leases()
            row = self.connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["state"] != "reconciling" or row["owner"] is not None:
                return None
            other_owner = self.connection.execute(
                "SELECT 1 FROM tasks WHERE conversation_id=? AND id!=? AND owner IS NOT NULL "
                "AND state IN ('claimed','running','reconciling') LIMIT 1",
                (row["conversation_id"], task_id),
            ).fetchone()
            if other_owner:
                return None
            attempt_id, token = row["attempt_id"], row["token"] + 1
            if attempt_id is None:
                attempt_id = _identifier()
                self.connection.execute(
                    "INSERT INTO execution_attempts VALUES (?,?,?,?,?,?,?)",
                    (attempt_id, task_id, owner, token, "unknown", None, self.clock()),
                )
            else:
                self.connection.execute(
                    "UPDATE execution_attempts SET owner=?,token=? WHERE id=?", (owner, token, attempt_id)
                )
            self.connection.execute(
                "UPDATE tasks SET owner=?,lease_until=?,token=?,attempt_id=? WHERE id=?",
                (owner, self.clock() + lease_seconds, token, attempt_id, task_id),
            )
            self._event(task_id, "reconciliation_claimed")
            return Claim(task_id, row["conversation_id"], row["generation"], owner, attempt_id, token)

    def confirm_not_submitted(self, claim: Claim, evidence: str) -> None:
        """Release uncertain work only after definitive executor rejection/query.

        ``evidence`` is a caller-provided non-sensitive reason code; a timeout or
        a missing heartbeat is not evidence that an execution was never accepted.
        """
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError("definitive evidence reason is required")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] != "reconciling":
                raise StateError("only uncertain submissions can be released")
            state = "cancelled" if row["cancel_requested"] else "queued"
            self.connection.execute(
                "UPDATE execution_attempts SET submission='not_submitted' WHERE id=?", (claim.attempt_id,)
            )
            self.connection.execute(
                "UPDATE tasks SET state=?,owner=NULL,lease_until=NULL,token=token+1 WHERE id=?",
                (state, claim.task_id),
            )
            self._event(claim.task_id, "confirmed_not_submitted", evidence)

    def renew(self, claim: Claim, lease_seconds: float = 60) -> None:
        """Extend a valid worker lease; never revive an expired capability."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        with self._transaction():
            self._owned(claim)
            self.connection.execute("UPDATE tasks SET lease_until=? WHERE id=?",
                                    (self.clock() + lease_seconds, claim.task_id))

    def begin_submission(self, claim: Claim) -> None:
        """Persist uncertainty before an external call to close the crash window."""
        with self._transaction():
            row = self._owned(claim)
            if row["state"] != "claimed" or row["cancel_requested"]:
                raise StateError("task cannot start submission")
            self.connection.execute("UPDATE tasks SET state='reconciling' WHERE id=?", (claim.task_id,))
            self.connection.execute("UPDATE execution_attempts SET submission='unknown' WHERE id=?",
                                    (claim.attempt_id,))
            self._event(claim.task_id, "submission_started")

    def mark_submitted(self, claim: Claim, run_id: str) -> None:
        """Record the executor's accepted run after the uncertainty marker."""
        if not run_id:
            raise ValueError("run_id must be nonempty")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] != "reconciling":
                raise StateError("begin_submission must precede executor acceptance")
            existing = self.connection.execute(
                "SELECT run_id FROM execution_attempts WHERE id=?", (claim.attempt_id,)
            ).fetchone()["run_id"]
            if existing is not None and existing != run_id:
                raise StateError("reconciliation cannot replace the bound executor run")
            self.connection.execute("UPDATE tasks SET state='running' WHERE id=?", (claim.task_id,))
            self.connection.execute(
                "UPDATE execution_attempts SET submission='accepted',run_id=? WHERE id=?",
                (run_id, claim.attempt_id),
            )
            self._event(claim.task_id, "submitted")

    def rebind_run(self, claim: Claim, run_id: str) -> None:
        """Replace the bound run after a query-time track fallback.

        The previous run is proven dead (its message never reached the
        session) and the fallback executor has already bound this attempt
        to the new run in executor_runs. This syncs the attempt's run_id
        to that binding so the runner's next query polls the live run.
        It is an explicit, audited rebind, not a silent overwrite: the
        task must be running/reconciling and the new run must match the
        current executor binding. (Proven live 2026-11-06: returning the
        fallback submit's 'accepted' outcome from query crashed the
        runner, because mark_submitted only accepts a run while the task
        is 'reconciling'.)
        """
        if not run_id:
            raise ValueError("run_id must be nonempty")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] not in ("running", "reconciling"):
                raise StateError("run can only be rebound for a running task")
            binding = self.connection.execute(
                "SELECT platform_run_id FROM executor_runs WHERE attempt_id=?",
                (claim.attempt_id,),
            ).fetchone()
            if binding is None or binding["platform_run_id"] != run_id:
                raise StateError("rebind run must match the current executor binding")
            self.connection.execute(
                "UPDATE execution_attempts SET run_id=? WHERE id=?",
                (run_id, claim.attempt_id),
            )
            self._event(claim.task_id, "run_rebound")
            # The fallback track is now live. Tell the user the primary
            # failed and the backup is running instead of leaving them
            # waiting in silence -- but only when the primary actually
            # made them wait. A fast primary failure (auth/token error,
            # circuit open) falls back within seconds and the backup
            # answer lands almost as fast as a normal turn, so a
            # "primary failed" receipt is pure noise; only explain the
            # delay when the primary burned real waiting time first.
            # Idempotent: one pending fallback receipt per task.
            attempt = self.connection.execute(
                "SELECT created_at FROM execution_attempts WHERE id=?",
                (claim.attempt_id,),
            ).fetchone()
            elapsed = self.clock() - float(attempt["created_at"]) if attempt else 0.0
            if elapsed < FALLBACK_RECEIPT_MIN_WAIT_SECS:
                return
            existing = self.connection.execute(
                "SELECT id FROM task_receipts WHERE task_id=? AND kind='fallback' "
                "AND state='pending' LIMIT 1", (claim.task_id,)).fetchone()
            if existing is None:
                self._insert_receipt(claim.task_id, row["conversation_id"],
                                     "fallback", FALLBACK_RECEIPT_TEXT)

    def foldable(self, owner: str) -> dict | None:
        """Find a queued task that can fold into a running turn owned by ``owner``.

        Returns {'running': <running task row>, 'queued': <queued task row>,
        'queued_text': str, 'run_id': str|None} for the oldest queued task
        whose conversation has a live turn (state 'running' or
        'reconciling', i.e. submitted with a bound run/session) owned by
        this runner, or None when no such pair exists. The caller must
        still ask the executor whether that turn's reply has started;
        folding is only safe while it has not.
        """
        row = self.connection.execute(
            "SELECT q.id AS qid, q.message_id AS qmsg, q.conversation_id AS conv, "
            "r.id AS rid, r.attempt_id AS rattempt, r.owner AS rowner "
            "FROM tasks q JOIN tasks r ON r.conversation_id=q.conversation_id "
            "WHERE q.state='queued' AND r.state IN ('running','reconciling') "
            "AND r.owner=? AND r.sequence<q.sequence "
            "ORDER BY q.sequence LIMIT 1",
            (owner,),
        ).fetchone()
        if row is None:
            return None
        running = self.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (row["rid"],)).fetchone()
        queued = self.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (row["qid"],)).fetchone()
        qtext = self.connection.execute(
            "SELECT text FROM inbound_messages WHERE id=?",
            (row["qmsg"],)).fetchone()
        run_id = self.connection.execute(
            "SELECT run_id FROM execution_attempts WHERE id=?",
            (row["rattempt"],)).fetchone()
        return {
            "running": dict(running),
            "queued": dict(queued),
            "queued_text": qtext["text"] if qtext else "",
            "run_id": run_id["run_id"] if run_id else None,
        }

    def mark_merged(self, queued_task_id: str, running_task_id: str) -> str:
        """Complete a queued task as merged into a running turn.

        The queued task must still be 'queued' and unclaimed, and the
        running task must still be active; otherwise nothing is changed
        and a StateError is raised (the caller must re-check). The merged
        task is completed with the pointer text as its result, so the
        delivery path sends the short pointer reply bound to the merged
        msgid instead of a full answer. The fold itself (sending the text
        to the live session) must already have succeeded before this is
        called; this only records the outcome.
        """
        with self._transaction():
            row = self.connection.execute(
                "SELECT * FROM tasks WHERE id=?", (queued_task_id,)).fetchone()
            if row is None:
                raise KeyError(queued_task_id)
            if row["state"] != "queued" or row["attempt_id"] is not None:
                raise StateError("only an unclaimed queued task can be merged")
            running = self.connection.execute(
                "SELECT * FROM tasks WHERE id=?", (running_task_id,)).fetchone()
            if running is None or running["state"] not in ("running", "reconciling"):
                raise StateError("merge target is no longer a live turn")
            outbound_id = self._complete_result(row, MERGE_POINTER_TEXT, "merged")
            self._event(queued_task_id, "merged_into", running_task_id)
            return outbound_id

    def _complete_result(self, row: sqlite3.Row, text: str, event: str,
                         media: list | None = None) -> str:
        """Save a result, bound outbox and audit within the caller's transaction.

        Media is persisted verbatim (storage never parses it) on both the
        task and its outbound message. If the text claims a link/file/
        video/image was sent but carries neither a URL nor media, log a
        warning with the task id instead of silently marking it
        delivered: the claim may be false, but delivery is not blocked
        on the suspicion alone.
        """
        media_json = json.dumps(media or [], ensure_ascii=False)
        if (any(word in text for word in ("链接", "文件", "视频", "图片", "图像"))
                and not any(marker in text for marker in ("http://", "https://", "www."))
                and not (media or [])):
            LOGGER.warning(
                "result claims a link/file but has neither URL nor media: task_id=%s",
                row["id"])
        outbound_id = _identifier()
        self.connection.execute(
            "INSERT INTO outbound_messages(id,task_id,conversation_id,generation,text,created_at,media_json) "
            "VALUES (?,?,?,?,?,?,?)",
            (outbound_id, row["id"], row["conversation_id"], row["generation"], text,
             self.clock(), media_json),
        )
        self.connection.execute(
            "UPDATE tasks SET state='succeeded',result=?,media_json=?,owner=NULL,lease_until=NULL WHERE id=?",
            (text, media_json, row["id"]),
        )
        self._event(row["id"], event)
        return outbound_id

    def complete(self, claim: Claim, text: str, media: list | None = None) -> str:
        """Commit execution result and bound outbox together, using fenced ownership."""
        if not isinstance(text, str):
            raise ValueError("result text must be a string")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] not in ("running", "reconciling"):
                raise StateError("cannot complete an unsubmitted task")
            return self._complete_result(row, text, "succeeded", media=media)

    def complete_command(self, task_id: str, text: str) -> str:
        """Complete an unclaimed local command atomically, without an executor.

        Call inside the ingress transaction so command effects, route metadata,
        task result and outbox either all commit or all roll back together.
        """
        if not isinstance(text, str):
            raise ValueError("command result must be a string")
        with self._transaction():
            row = self.connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["state"] != "queued" or row["attempt_id"] is not None:
                raise StateError("only unclaimed commands without execution attempts can complete locally")
            # /new advances generation within this same ingress transaction.
            # Its immediate receipt belongs to the new context, unlike a late
            # result from an execution that began in an earlier generation.
            self.connection.execute(
                "UPDATE tasks SET generation=(SELECT generation FROM conversations WHERE id=tasks.conversation_id) "
                "WHERE id=?", (task_id,),
            )
            row = self.connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            return self._complete_result(row, text, "command_succeeded")

    def fail(self, claim: Claim, evidence: str) -> None:
        """Record a confirmed execution failure; absence of a heartbeat is insufficient."""
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError("confirmed failure evidence is required")
        with self._transaction():
            row = self._owned(claim)
            self._fail_task_in_transaction(
                claim.task_id, row["conversation_id"], evidence)

    def request_cancel(self, task_id: str, conversation_id: str) -> bool:
        """Request cancellation within an already-authorized conversation scope.

        Transport adapters must authorize the operator before calling this API.
        Submitted tasks retain their position until the executor confirms cancel.
        """
        with self._transaction():
            row = self.connection.execute(
                "SELECT * FROM tasks WHERE id=? AND conversation_id=?", (task_id, conversation_id)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["state"] in TERMINAL:
                return False
            was_queued = row["state"] == "queued"
            self.connection.execute(
                "UPDATE tasks SET cancel_requested=1,state=CASE WHEN state='queued' THEN 'cancelled' "
                "ELSE state END WHERE id=?", (task_id,),
            )
            self._event(task_id, "cancel_requested")
            if was_queued:
                # Queued tasks cancel immediately with no executor round
                # trip, so the receipt is written here, not at confirm.
                self._insert_receipt(task_id, row["conversation_id"],
                                     "cancelled", CANCELLED_RECEIPT_TEXT)
            return True

    def confirm_cancel(self, claim: Claim) -> None:
        """Commit executor cancellation confirmation for the current attempt."""
        with self._transaction():
            row = self._owned(claim)
            if not row["cancel_requested"]:
                raise StateError("cancellation was not requested")
            self.connection.execute(
                "UPDATE tasks SET state='cancelled',owner=NULL,lease_until=NULL WHERE id=?", (claim.task_id,)
            )
            self._event(claim.task_id, "cancelled")
            self._insert_receipt(claim.task_id, row["conversation_id"],
                                 "cancelled", CANCELLED_RECEIPT_TEXT)
            self._notify_merged(claim.task_id, "cancelled", CANCELLED_RECEIPT_TEXT)

    def confirm_platform_cancel(self, claim: Claim, evidence: str) -> None:
        """Save the original run's confirmed cancellation, including external UI stops.

        A platform terminal event is independent of a local cancel request.
        The adapter must correlate the evidence with this attempt's original run;
        fenced ownership still prevents stale events replacing a saved result.
        """
        if not isinstance(evidence, str) or not evidence.strip():
            raise ValueError("platform cancellation evidence is required")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] not in ("running", "reconciling"):
                raise StateError("platform cancellation requires a submitted or uncertain task")
            self.connection.execute(
                "UPDATE tasks SET state='cancelled',owner=NULL,lease_until=NULL WHERE id=?", (claim.task_id,)
            )
            self._event(claim.task_id, "platform_cancelled", evidence)
            self._insert_receipt(claim.task_id, row["conversation_id"],
                                 "cancelled", CANCELLED_RECEIPT_TEXT)
            self._notify_merged(claim.task_id, "cancelled", CANCELLED_RECEIPT_TEXT)

    def task(self, task_id: str) -> dict:
        """Return a detached task snapshot for diagnostics and adapters."""
        row = self.connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        return dict(row)

    def snapshot(self) -> dict:
        """Return metadata counts, without message bodies or credentials."""
        counts = dict(self.connection.execute("SELECT state,COUNT(*) FROM tasks GROUP BY state"))
        return {"schema_version": SCHEMA_VERSION, "tasks": counts,
                "conversations": self.connection.execute("SELECT COUNT(*) FROM conversations").fetchone()[0],
                "outbound": dict(self.connection.execute(
                    "SELECT state,COUNT(*) FROM outbound_messages GROUP BY state"))}
