"""SQLite task authority with short transactions and no transport side effects.

Call ``begin_submission`` before contacting an executor. Once that transaction
commits, a crash is treated as an uncertain external result, never as permission
to submit again. This store is currently opt-in and does not replace live JSONL
queues automatically.
"""
from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from .models import Claim, InboundMessage, IngestResult, OwnershipError, StateError

SCHEMA_VERSION = 1
ACTIVE = ("claimed", "running", "reconciling")
TERMINAL = ("succeeded", "failed", "cancelled")
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
    owner TEXT, lease_until REAL, attempt_id TEXT, result TEXT
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
    created_at REAL NOT NULL
);
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
        """Requeue only attempts proven unsubmitted; reserve uncertain sessions."""
        rows = self.connection.execute(
            "SELECT t.id,a.submission,t.cancel_requested FROM tasks t "
            "JOIN execution_attempts a ON a.id=t.attempt_id "
            "WHERE t.state IN ('claimed','running','reconciling') AND t.lease_until<=?",
            (self.clock(),),
        ).fetchall()
        for row in rows:
            if row["submission"] == "not_submitted":
                state = "cancelled" if row["cancel_requested"] else "queued"
            else:
                state = "reconciling"
            self.connection.execute(
                "UPDATE tasks SET state=?,owner=NULL,lease_until=NULL,token=token+1 WHERE id=?",
                (state, row["id"]),
            )
            self._event(row["id"], f"lease_expired:{state}")

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

    def _complete_result(self, row: sqlite3.Row, text: str, event: str) -> str:
        """Save a result, bound outbox and audit within the caller's transaction."""
        outbound_id = _identifier()
        self.connection.execute(
            "INSERT INTO outbound_messages(id,task_id,conversation_id,generation,text,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (outbound_id, row["id"], row["conversation_id"], row["generation"], text, self.clock()),
        )
        self.connection.execute(
            "UPDATE tasks SET state='succeeded',result=?,owner=NULL,lease_until=NULL WHERE id=?",
            (text, row["id"]),
        )
        self._event(row["id"], event)
        return outbound_id

    def complete(self, claim: Claim, text: str) -> str:
        """Commit execution result and bound outbox together, using fenced ownership."""
        if not isinstance(text, str):
            raise ValueError("result text must be a string")
        with self._transaction():
            row = self._owned(claim)
            if row["state"] not in ("running", "reconciling"):
                raise StateError("cannot complete an unsubmitted task")
            return self._complete_result(row, text, "succeeded")

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
            self._owned(claim)
            self.connection.execute(
                "UPDATE tasks SET state='failed',owner=NULL,lease_until=NULL WHERE id=?", (claim.task_id,)
            )
            self._event(claim.task_id, "confirmed_failed", evidence)

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
            self.connection.execute(
                "UPDATE tasks SET cancel_requested=1,state=CASE WHEN state='queued' THEN 'cancelled' "
                "ELSE state END WHERE id=?", (task_id,),
            )
            self._event(task_id, "cancel_requested")
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
