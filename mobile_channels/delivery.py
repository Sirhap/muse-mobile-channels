"""Transactional delivery claims; uncertain platform outcomes never auto-retry."""
from __future__ import annotations

from dataclasses import dataclass

from .models import OwnershipError, StateError
from .storage import Store

SCHEMA = """
CREATE TABLE IF NOT EXISTS outbound_parts (
    id TEXT PRIMARY KEY, outbound_id TEXT NOT NULL REFERENCES outbound_messages(id),
    part_index INTEGER NOT NULL, text TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending','sending','retry_wait','delivered','delivery_unknown','rejected')),
    owner TEXT, token INTEGER NOT NULL DEFAULT 0, lease_until REAL,
    retry_at REAL NOT NULL DEFAULT 0, receipt TEXT,
    UNIQUE(outbound_id,part_index)
);
CREATE INDEX IF NOT EXISTS delivery_queue ON outbound_parts(state,retry_at);
CREATE TABLE IF NOT EXISTS outbound_rendered_payloads (
    part_id TEXT PRIMARY KEY REFERENCES outbound_parts(id),
    text TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbound_attempts (
    part_id TEXT NOT NULL REFERENCES outbound_parts(id), token INTEGER NOT NULL,
    owner TEXT NOT NULL, started_at REAL NOT NULL, outcome TEXT NOT NULL,
    PRIMARY KEY(part_id,token)
);
CREATE TABLE IF NOT EXISTS delivery_resolutions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    part_id TEXT NOT NULL REFERENCES outbound_parts(id), state TEXT NOT NULL,
    evidence TEXT NOT NULL, created_at REAL NOT NULL
);
"""


@dataclass(frozen=True)
class DeliveryClaim:
    """An owned part and its immutable route, read from the bound conversation."""

    part_id: str
    outbound_id: str
    owner: str
    token: int
    text: str
    channel: str
    account_id: str
    chat_type: str
    chat_id: str
    generation: int


class DeliveryStore:
    """Manage delivery through the task store's connection; perform no network IO."""

    def __init__(self, store: Store):
        """Create delivery tables in the same local transactional database."""
        self.store = store
        self.connection = store.connection
        with store.transaction():
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    self.connection.execute(statement)

    def prepare(self, outbound_id: str, chunks: list[str]) -> list[str]:
        """Save stable ordered part IDs once, preserving the original message text."""
        if not chunks or any(not isinstance(text, str) for text in chunks):
            raise ValueError("at least one text chunk is required")
        with self.store.transaction():
            message = self.connection.execute(
                "SELECT * FROM outbound_messages WHERE id=?", (outbound_id,)
            ).fetchone()
            if message is None:
                raise KeyError(outbound_id)
            if "".join(chunks) != message["text"]:
                raise ValueError("chunks must preserve the bound result text")
            existing = self.connection.execute(
                "SELECT id,text FROM outbound_parts WHERE outbound_id=? ORDER BY part_index", (outbound_id,)
            ).fetchall()
            if existing:
                if [row["text"] for row in existing] != chunks:
                    raise StateError("prepared segmentation is immutable")
                return [row["id"] for row in existing]
            if message["state"] != "pending":
                raise StateError("cannot prepare a nonpending outbound message")
            ids = [f"{outbound_id}:{index}" for index in range(len(chunks))]
            self.connection.executemany(
                "INSERT INTO outbound_parts(id,outbound_id,part_index,text) VALUES (?,?,?,?)",
                [(part_id, outbound_id, index, chunks[index]) for index, part_id in enumerate(ids)],
            )
            return ids

    def _aggregate(self, outbound_id: str) -> None:
        """Mark a message delivered only when every prepared part is delivered."""
        states = {row[0] for row in self.connection.execute(
            "SELECT state FROM outbound_parts WHERE outbound_id=?", (outbound_id,)
        )}
        if states == {"delivered"}:
            state = "delivered"
        else:
            state = next((value for value in ("delivery_unknown", "rejected", "sending", "retry_wait")
                          if value in states), "pending")
        self.connection.execute("UPDATE outbound_messages SET state=? WHERE id=?", (state, outbound_id))

    def _expire(self) -> None:
        """A lost sender may have sent successfully; reserve its part for review."""
        rows = self.connection.execute(
            "SELECT * FROM outbound_parts WHERE state='sending' AND lease_until<=?", (self.store.clock(),)
        ).fetchall()
        for row in rows:
            self.connection.execute(
                "UPDATE outbound_parts SET state='delivery_unknown',owner=NULL,lease_until=NULL,token=token+1 "
                "WHERE id=?", (row["id"],),
            )
            self.connection.execute(
                "UPDATE outbound_attempts SET outcome='delivery_unknown' WHERE part_id=? AND token=?",
                (row["id"], row["token"]),
            )
            self._aggregate(row["outbound_id"])

    def claim(self, owner: str, lease_seconds: float = 60, *,
              channel: str | None = None, account_id: str | None = None) -> DeliveryClaim | None:
        """Persist one sending attempt with conversation FIFO and optional account scope."""
        if not owner or lease_seconds <= 0:
            raise ValueError("owner and positive lease_seconds are required")
        if (channel is None) != (account_id is None):
            raise ValueError('channel and account_id filters must be supplied together')
        with self.store.transaction():
            self._expire()
            row = self.connection.execute(
                "SELECT p.*,c.channel,c.account_id,c.chat_type,c.chat_id,m.generation, "
                "c.generation AS current_generation "
                "FROM outbound_parts p JOIN outbound_messages m ON m.id=p.outbound_id "
                "JOIN conversations c ON c.id=m.conversation_id JOIN tasks task ON task.id=m.task_id "
                "WHERE p.state IN ('pending','retry_wait') AND p.retry_at<=? "
                "AND (? IS NULL OR (c.channel=? AND c.account_id=?)) "
                "AND m.state NOT IN ('cancelled','rejected','delivery_unknown') AND NOT EXISTS ("
                "SELECT 1 FROM outbound_parts prior WHERE prior.outbound_id=p.outbound_id "
                "AND prior.part_index<p.part_index AND prior.state!='delivered') "
                "AND NOT EXISTS (SELECT 1 FROM outbound_messages prior_message "
                "JOIN tasks prior_task ON prior_task.id=prior_message.task_id "
                "WHERE prior_message.conversation_id=m.conversation_id "
                "AND prior_task.sequence<task.sequence "
                "AND prior_message.state NOT IN ('delivered','cancelled','rejected')) "
                "ORDER BY task.sequence,p.part_index LIMIT 1",
                (self.store.clock(), channel, channel, account_id),
            ).fetchone()
            if row is None:
                return None
            rendered = self.connection.execute(
                "SELECT text FROM outbound_rendered_payloads WHERE part_id=?", (row["id"],)
            ).fetchone()
            if rendered is None:
                # Preserve payloads already attempted by earlier versions. New
                # parts acquire their source label once, immediately before IO.
                text = row["text"]
                if (row["token"] == 0 and row["part_index"] == 0
                        and row["generation"] < row["current_generation"]):
                    text = "补充此前对话的结果：\n" + text
                self.connection.execute("INSERT INTO outbound_rendered_payloads VALUES (?,?)",
                                        (row["id"], text))
            else:
                text = rendered["text"]
            token = row["token"] + 1
            self.connection.execute(
                "UPDATE outbound_parts SET state='sending',owner=?,token=?,lease_until=? WHERE id=?",
                (owner, token, self.store.clock() + lease_seconds, row["id"]),
            )
            self.connection.execute(
                "INSERT INTO outbound_attempts VALUES (?,?,?,?,?)",
                (row["id"], token, owner, self.store.clock(), "sending"),
            )
            self._aggregate(row["outbound_id"])
            return DeliveryClaim(row["id"], row["outbound_id"], owner, token, text,
                                 row["channel"], row["account_id"], row["chat_type"], row["chat_id"],
                                 row["generation"])

    def _owned(self, claim: DeliveryClaim):
        """Validate current sending ownership and never trust caller-supplied route data."""
        row = self.connection.execute("SELECT * FROM outbound_parts WHERE id=?", (claim.part_id,)).fetchone()
        if (row is None or row["outbound_id"] != claim.outbound_id or row["state"] != "sending"
                or row["owner"] != claim.owner or row["token"] != claim.token
                or row["lease_until"] <= self.store.clock()):
            raise OwnershipError("delivery ownership is expired or superseded")
        return row

    def _finish(self, claim: DeliveryClaim, state: str, receipt: str | None = None,
                retry_after: float = 0, evidence: str | None = None) -> None:
        """Atomically save the owned part result, attempt outcome, and message status."""
        with self.store.transaction():
            self._owned(claim)
            self.connection.execute(
                "UPDATE outbound_parts SET state=?,owner=NULL,lease_until=NULL,receipt=?,retry_at=? WHERE id=?",
                (state, receipt, self.store.clock() + retry_after, claim.part_id),
            )
            self.connection.execute(
                "UPDATE outbound_attempts SET outcome=? WHERE part_id=? AND token=?",
                (state, claim.part_id, claim.token),
            )
            if evidence:
                self.connection.execute("INSERT INTO delivery_resolutions(part_id,state,evidence,created_at) "
                                        "VALUES (?,?,?,?)",
                                        (claim.part_id, state, evidence, self.store.clock()))
            self._aggregate(claim.outbound_id)

    def delivered(self, claim: DeliveryClaim, receipt: str) -> None:
        """Commit a confirmed platform acceptance; require an explicit receipt."""
        if not receipt:
            raise ValueError("delivery receipt is required")
        self._finish(claim, "delivered", receipt=receipt)

    def retry(self, claim: DeliveryClaim, evidence: str, retry_after: float = 30) -> None:
        """Retry only a confirmed temporary failure, never a timeout of unknown outcome."""
        if not evidence or retry_after < 0:
            raise ValueError("failure evidence and nonnegative retry delay are required")
        self._finish(claim, "retry_wait", retry_after=retry_after, evidence=evidence)

    def reject(self, claim: DeliveryClaim, evidence: str) -> None:
        """Record a confirmed permanent platform rejection for operator review."""
        if not evidence:
            raise ValueError("rejection evidence is required")
        self._finish(claim, "rejected", evidence=evidence)

    def unknown(self, claim: DeliveryClaim) -> None:
        """Preserve a timeout or disconnected send as uncertain without retrying it."""
        self._finish(claim, "delivery_unknown")

    def resolve_unknown(self, part_id: str, state: str, evidence: str, receipt: str | None = None) -> None:
        """Apply an explicit platform query/operator resolution; never infer success.

        Choosing retry_wait acknowledges possible duplication when no platform
        evidence can determine the outcome. The caller must expose that choice.
        """
        if state not in ("delivered", "retry_wait", "rejected") or not evidence:
            raise ValueError("explicit resolution state and evidence are required")
        if state == "delivered" and not receipt:
            raise ValueError("confirmed delivery receipt is required")
        with self.store.transaction():
            self._expire()
            row = self.connection.execute("SELECT * FROM outbound_parts WHERE id=?", (part_id,)).fetchone()
            if row is None:
                raise KeyError(part_id)
            if row["state"] != "delivery_unknown":
                raise StateError("part is not awaiting delivery resolution")
            self.connection.execute(
                "UPDATE outbound_parts SET state=?,receipt=?,retry_at=0,token=token+1 WHERE id=?",
                (state, receipt, part_id),
            )
            self.connection.execute("INSERT INTO delivery_resolutions(part_id,state,evidence,created_at) "
                                    "VALUES (?,?,?,?)", (part_id, state, evidence, self.store.clock()))
            self._aggregate(row["outbound_id"])
