"""Immutable values exchanged with the transactional task store."""
from dataclasses import dataclass


@dataclass(frozen=True)
class InboundMessage:
    """Validated incoming route and content; external IDs never become paths."""

    channel: str
    account_id: str
    chat_type: str
    chat_id: str
    message_id: str
    sender_id: str
    text: str
    received_at: float | None = None


@dataclass(frozen=True)
class IngestResult:
    """Committed identity returned after deduplication and task creation."""

    conversation_id: str
    task_id: str
    created: bool


@dataclass(frozen=True)
class Claim:
    """Fenced ownership capability required for every worker state change."""

    task_id: str
    conversation_id: str
    generation: int
    owner: str
    attempt_id: str
    token: int


class OwnershipError(RuntimeError):
    """The worker lease or fencing capability is no longer valid."""


class StateError(ValueError):
    """An operation is inconsistent with the committed task state."""
