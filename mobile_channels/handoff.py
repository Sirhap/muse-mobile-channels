"""Durable Core-owned hook execution, separate from legacy inbox writers."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import secrets
import sqlite3

from .modes import ChannelLease, ModeError
from .runtime import ExecutionContext, ExecutionOutcome
from .storage import Store


SCHEMA = """CREATE TABLE IF NOT EXISTS core_hook_handoffs (
    attempt_id TEXT PRIMARY KEY REFERENCES execution_attempts(id),
    task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
    handoff_id TEXT NOT NULL UNIQUE, capability TEXT NOT NULL,
    route_json TEXT NOT NULL, state TEXT NOT NULL,
    worker_id TEXT, result_json TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
)"""


class HookHandoffExecutor:
    """Queue only proven-unsent tasks and retain every offered execution identity."""

    def __init__(self, store: Store, modes: str | Path | None = None):
        self.store = store
        self.modes = modes or os.environ.get("MUSE_CHANNEL_MODES")
        database = store.connection.execute("PRAGMA database_list").fetchone()["file"]
        self.database = Path(database).resolve() if database else None

    @contextmanager
    def _scope(self, channel: str, *, require_enabled: bool = False):
        """Use the same core mode lease and database/account binding as the runner."""
        if not self.modes:
            raise ModeError("core hook handoff requires a channel modes file")
        with ChannelLease(Path(self.modes), channel, "core") as lease:
            if Path(lease.settings["database"]).resolve() != self.database:
                raise ModeError("hook handoff database does not match the channel")
            if require_enabled and lease.settings.get("hook_handoff") is not True:
                raise ModeError("hook handoff is not enabled for this channel")
            yield lease.settings

    def enabled(self, context: ExecutionContext) -> bool:
        """Require explicit opt-in for this exact account before queueing work."""
        try:
            with self._scope(context.channel, require_enabled=True) as settings:
                return settings["account_id"] == context.account_id
        except (OSError, ValueError, KeyError, ModeError):
            return False

    def submit(self, context: ExecutionContext, evidence: str) -> ExecutionOutcome:
        """Commit an immutable local offer before any hook can execute the task."""
        with self._scope(context.channel, require_enabled=True) as settings, self.store.transaction():
            task = self.store._owned(context.claim)
            if task["cancel_requested"]:
                return ExecutionOutcome("not_submitted", evidence="cancelled_before_handoff")
            binding = self.store.connection.execute(
                "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,),
            ).fetchone()
            if binding is not None and binding["track"] == "hooks":
                return ExecutionOutcome("running", evidence="hook_handoff_already_queued")
            if binding is not None and binding["platform_run_id"] and binding["status"] != "failed_superseded":
                return ExecutionOutcome("unknown", evidence="hook_handoff_refused_existing_run")
            route = dict(self.store.connection.execute(
                "SELECT t.id AS task_id,t.generation,c.channel,c.account_id,c.chat_type,c.chat_id,"
                "m.external_id AS external_message_id,m.sender_id,m.text "
                "FROM tasks t JOIN conversations c ON c.id=t.conversation_id "
                "JOIN inbound_messages m ON m.id=t.message_id WHERE t.id=?", (task["id"],),
            ).fetchone())
            if route["channel"] != context.channel or route["account_id"] != settings["account_id"]:
                raise ModeError("hook handoff task route does not match the channel account")
            route["media"] = list(context.media)
            route["not_submitted_evidence"] = evidence
            run_id = f"hook:{context.claim.attempt_id}"
            self.store.connection.execute(SCHEMA)
            self.store.connection.execute(
                "INSERT INTO core_hook_handoffs "
                "(attempt_id,task_id,handoff_id,capability,route_json,state,created_at) "
                "VALUES (?,?,?,?,?,'queued',?)",
                (context.claim.attempt_id, task["id"], run_id, secrets.token_urlsafe(32),
                 json.dumps(route, ensure_ascii=False), self.store.clock()),
            )
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id,session_id,platform_run_id,status,created_at,track) "
                "VALUES (?,'core-hooks',?,'queued',?,'hooks') ON CONFLICT(attempt_id) DO UPDATE SET "
                "session_id='core-hooks',platform_run_id=excluded.platform_run_id,status='queued',track='hooks'",
                (context.claim.attempt_id, run_id, self.store.clock()),
            )
        return ExecutionOutcome("running", evidence="hook_handoff_queued_waiting_receipt")

    def offer(self, channel: str) -> dict | None:
        """Emit each offer once. Lost output remains uncertain and is never replayed."""
        with self._scope(channel, require_enabled=True) as settings, self.store.transaction():
            table = self.store.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='core_hook_handoffs'",
            ).fetchone()
            if table is None:
                return None
            rows = self.store.connection.execute(
                "SELECT h.* FROM core_hook_handoffs h JOIN tasks t ON t.id=h.task_id "
                "JOIN conversations c ON c.id=t.conversation_id "
                "WHERE h.state='queued' AND t.state IN ('running','reconciling') "
                "AND t.attempt_id=h.attempt_id "
                "AND t.cancel_requested=0 ORDER BY h.created_at,h.handoff_id",
            ).fetchall()
            for row in rows:
                route = json.loads(row["route_json"])
                if route["channel"] != channel or route["account_id"] != settings["account_id"]:
                    continue
                self.store.connection.execute(
                    "UPDATE core_hook_handoffs SET state='offered' WHERE attempt_id=?", (row["attempt_id"],),
                )
                return {"handoff_id": row["handoff_id"], "capability": row["capability"], "task": route,
                        "database": str(self.database), "modes": str(Path(self.modes).resolve())}
        return None

    def _receipt(self, handoff_id: str, capability: str):
        """Authenticate the exact offered attempt without accepting caller routes."""
        row = self.store.connection.execute(
            "SELECT * FROM core_hook_handoffs WHERE handoff_id=?", (handoff_id,),
        ).fetchone()
        if row is None or not secrets.compare_digest(row["capability"], capability):
            raise ValueError("hook handoff capability is invalid")
        return row

    def acknowledge(self, handoff_id: str, capability: str, worker_id: str) -> dict:
        """Record one platform worker's receipt before it starts user operations."""
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("a real hook worker identity is required")
        with self.store.transaction():
            row = self._receipt(handoff_id, capability)
            route = json.loads(row["route_json"])
            with self._scope(route["channel"]):
                if row["worker_id"] is not None and row["worker_id"] != worker_id:
                    raise ValueError("hook handoff already belongs to another worker")
                if row["state"] not in ("offered", "acknowledged"):
                    raise ValueError("hook handoff cannot be acknowledged in its current state")
                self.store.connection.execute(
                    "UPDATE core_hook_handoffs SET state='acknowledged',worker_id=? WHERE handoff_id=?",
                    (worker_id, handoff_id),
                )
        return self.status(handoff_id, capability)

    def status(self, handoff_id: str, capability: str) -> dict:
        """Return cancellation and execution state for the bound worker only."""
        row = self._receipt(handoff_id, capability)
        task = self.store.connection.execute(
            "SELECT t.cancel_requested,t.state,t.generation,c.generation AS current_generation "
            "FROM tasks t JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?", (row["task_id"],),
        ).fetchone()
        cancelled = (bool(row["cancel_requested"]) or task is None or bool(task["cancel_requested"])
                     or task["state"] not in ("running", "reconciling"))
        return {"state": row["state"], "cancel_requested": cancelled,
                "worker_id": row["worker_id"], "task": json.loads(row["route_json"])}

    def complete(self, handoff_id: str, capability: str, worker_id: str, *,
                 state: str, text: str = "", media: list | None = None, evidence: str = "") -> None:
        """Persist a result for Core delivery, without writing a legacy outbox."""
        if state not in ("succeeded", "failed", "cancelled"):
            raise ValueError("hook result must be an explicit terminal state")
        if not isinstance(text, str) or not isinstance(media or [], list):
            raise ValueError("hook result text or media is invalid")
        if (state == "succeeded" and not (text.strip() or media)) or (state != "succeeded" and not evidence):
            raise ValueError("hook result requires content or terminal evidence")
        result = json.dumps({"state": state, "text": text, "media": media or [], "evidence": evidence},
                            sort_keys=True, ensure_ascii=False)
        with self.store.transaction():
            row = self._receipt(handoff_id, capability)
            with self._scope(json.loads(row["route_json"])["channel"]):
                if row["worker_id"] != worker_id:
                    raise ValueError("hook result worker does not match its receipt")
                if row["result_json"] == result:
                    return
                if row["state"] != "acknowledged":
                    raise ValueError("hook result is already final or was never acknowledged")
                self.store.connection.execute(
                    "UPDATE core_hook_handoffs SET state=?,result_json=? WHERE handoff_id=?",
                    (state, result, handoff_id),
                )

    def query(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Reconcile local queue, worker receipt and result against the same attempt."""
        with self.store.transaction():
            self.store._owned(context.claim)
            row = self.store.connection.execute(
                "SELECT * FROM core_hook_handoffs WHERE attempt_id=?", (context.claim.attempt_id,),
            ).fetchone()
            if row is None:
                return ExecutionOutcome("unknown", evidence="hook_handoff_binding_missing")
            attempt = self.store.connection.execute(
                "SELECT run_id FROM execution_attempts WHERE id=?", (context.claim.attempt_id,),
            ).fetchone()
            if run_id not in (None, row["handoff_id"], attempt["run_id"]):
                raise ValueError("hook handoff run does not match the original attempt")
            if row["state"] in ("queued", "offered"):
                return ExecutionOutcome("running", evidence=f"hook_handoff_{row['state']}_waiting_receipt")
            if attempt["run_id"] is None and row["worker_id"]:
                return ExecutionOutcome("accepted", run_id=row["handoff_id"])
            if attempt["run_id"] != row["handoff_id"] and row["worker_id"]:
                self.store.rebind_run(context.claim, row["handoff_id"])
            if row["result_json"]:
                result = json.loads(row["result_json"])
                return ExecutionOutcome(result["state"], text=result["text"], media=result["media"],
                                        evidence=result["evidence"] or "hook_worker_result")
            return ExecutionOutcome("running", evidence="hook_worker_acknowledged")

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Cancel an unoffered item, otherwise await the worker's cancellation result."""
        with self.store.transaction():
            self.store._owned(context.claim)
            row = self.store.connection.execute(
                "SELECT * FROM core_hook_handoffs WHERE attempt_id=?", (context.claim.attempt_id,),
            ).fetchone()
            if row is None:
                return ExecutionOutcome("unknown", evidence="hook_handoff_binding_missing")
            attempt = self.store.connection.execute(
                "SELECT run_id FROM execution_attempts WHERE id=?", (context.claim.attempt_id,),
            ).fetchone()
            if run_id not in (None, row["handoff_id"], attempt["run_id"]):
                raise ValueError("hook handoff run does not match the original attempt")
            self.store.connection.execute(
                "UPDATE core_hook_handoffs SET cancel_requested=1 WHERE attempt_id=?", (context.claim.attempt_id,),
            )
            if row["state"] == "queued":
                self.store.connection.execute(
                    "UPDATE core_hook_handoffs SET state='cancelled' WHERE attempt_id=?", (context.claim.attempt_id,),
                )
                return ExecutionOutcome("cancelled", evidence="hook_handoff_cancelled_before_offer")
        return self.query(context, run_id)


def main(argv: list[str] | None = None) -> int:
    """Expose local hook receipts and results as a capability-scoped CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("poll", "ack", "status", "complete"))
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--modes", type=Path, default=os.environ.get("MUSE_CHANNEL_MODES"))
    parser.add_argument("--channel", choices=("wecom", "weixin", "feishu"), default="wecom")
    parser.add_argument("--handoff-id")
    parser.add_argument("--capability")
    parser.add_argument("--worker-id")
    parser.add_argument("--state", choices=("succeeded", "failed", "cancelled"), default="succeeded")
    parser.add_argument("--text-file", type=Path)
    parser.add_argument("--media-json", type=Path)
    parser.add_argument("--evidence", default="")
    args = parser.parse_args(argv)
    try:
        if not args.database.is_file():
            raise ValueError("core hook database does not exist")
        with Store(args.database) as store:
            executor = HookHandoffExecutor(store, args.modes)
            if args.operation == "poll":
                result = executor.offer(args.channel)
            else:
                if not args.handoff_id or not args.capability:
                    raise ValueError("handoff identity and capability are required")
                if args.operation == "ack":
                    result = executor.acknowledge(args.handoff_id, args.capability, args.worker_id)
                elif args.operation == "complete":
                    executor.complete(args.handoff_id, args.capability, args.worker_id, state=args.state,
                                      text=args.text_file.read_text() if args.text_file else "",
                                      media=json.loads(args.media_json.read_text()) if args.media_json else [],
                                      evidence=args.evidence)
                    result = {"recorded": True}
                else:
                    result = executor.status(args.handoff_id, args.capability)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, ModeError):
        print(json.dumps({"error": "core_hook_handoff_failed_check_local_configuration"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
