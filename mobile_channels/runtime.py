"""Small execution and delivery loops with explicit external outcome semantics."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from .delivery import DeliveryClaim, DeliveryStore
from .models import Claim, OwnershipError
from .storage import Store


@dataclass(frozen=True)
class ExecutionContext:
    """Database-bound input supplied to one fenced execution attempt."""

    claim: Claim
    text: str
    external_message_id: str
    sender_id: str
    channel: str
    account_id: str
    chat_type: str
    chat_id: str


@dataclass(frozen=True)
class ExecutionOutcome:
    """Explicit executor evidence; an exception or missing evidence means unknown."""

    state: str
    run_id: str | None = None
    text: str | None = None
    evidence: str | None = None
    # Platform message id when the attempt's message provably reached a
    # session (set by GadgetExecutor from the worker result). DualTrack
    # uses it to decide whether a failed primary may safely fall back:
    # no message id means the send never completed, so a native retry
    # cannot double-deliver.
    message_id: str | None = None


class Executor(Protocol):
    """Transport boundary: query and cancellation may only affect the original run."""

    def submit(self, context: ExecutionContext) -> ExecutionOutcome:
        """Submit once after the caller has committed its uncertainty marker."""
        ...

    def query(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Query this attempt; never create a replacement execution."""
        ...

    def cancel(self, context: ExecutionContext, run_id: str | None) -> ExecutionOutcome:
        """Confirm actual cancellation or return unknown/running, not an assumed stop."""
        ...


class CoreRunner:
    """Advance one task per tick; every external call happens outside transactions."""

    def __init__(self, store: Store, executor: Executor, owner: str,
                 concurrency: int = 4, lease_seconds: float = 60):
        """Use a stable process owner and bounded adapter network timeouts."""
        if not owner or concurrency < 1 or lease_seconds <= 0:
            raise ValueError('owner, positive concurrency and lease are required')
        self.store, self.executor, self.owner = store, executor, owner
        self.concurrency, self.lease_seconds = concurrency, lease_seconds
        self._last_polled_sequence = 0

    def _context(self, claim: Claim) -> ExecutionContext:
        """Read immutable route and content from the claimed database task."""
        row = self.store.connection.execute(
            'SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id '
            'FROM tasks t JOIN inbound_messages m ON m.id=t.message_id '
            'JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?', (claim.task_id,)
        ).fetchone()
        return ExecutionContext(claim, row['text'], row['external_id'], row['sender_id'],
                                row['channel'], row['account_id'], row['chat_type'], row['chat_id'])

    def _active_claim(self) -> Claim | None:
        """Renew our original attempt or acquire query-only ownership after expiry."""
        # tick() has already called Store.claim(), which reconciles expired
        # leases even when the global concurrency limit is full.
        rows = self.store.connection.execute(
            "SELECT * FROM tasks WHERE state IN ('claimed','running','reconciling') "
            'ORDER BY (sequence<=?),sequence', (self._last_polled_sequence,)
        ).fetchall()
        for row in rows:
            if row['owner'] == self.owner:
                self._last_polled_sequence = row['sequence']
                return Claim(row['id'], row['conversation_id'], row['generation'],
                             self.owner, row['attempt_id'], row['token'])
            if row['owner'] is None and row['state'] == 'reconciling':
                claim = self.store.claim_reconciliation(row['id'], self.owner, self.lease_seconds)
                if claim:
                    self._last_polled_sequence = row['sequence']
                    return claim
        return None

    def _apply(self, claim: Claim, outcome: ExecutionOutcome) -> str:
        """Persist only outcomes backed by the adapter's explicit evidence."""
        if outcome.state == 'succeeded' and isinstance(outcome.text, str):
            self.store.complete(claim, outcome.text)
        elif outcome.state == 'accepted' and outcome.run_id:
            self.store.mark_submitted(claim, outcome.run_id)
        elif outcome.state == 'not_submitted' and outcome.evidence:
            self.store.confirm_not_submitted(claim, outcome.evidence)
        elif outcome.state == 'failed' and outcome.evidence:
            self.store.fail(claim, outcome.evidence)
        elif outcome.state == 'cancelled' and outcome.evidence:
            self.store.confirm_platform_cancel(claim, outcome.evidence)
        elif outcome.state not in ('running', 'unknown'):
            raise ValueError('executor returned an outcome without required evidence')
        return outcome.state

    def tick(self) -> str:
        """Execute a new submission or poll one known attempt; never resubmit unknowns."""
        claim = self.store.claim(self.owner, self.concurrency, self.lease_seconds)
        if claim is None:
            claim = self._active_claim()
        if claim is None:
            return 'idle'
        self.store.renew(claim, self.lease_seconds)
        context = self._context(claim)
        task = self.store.task(claim.task_id)
        attempt = self.store.connection.execute(
            'SELECT * FROM execution_attempts WHERE id=?', (claim.attempt_id,)
        ).fetchone()
        # A recovered, proven-unsubmitted claim can be submitted once. A cancel
        # recorded before submission terminates locally without contacting Muse.
        if task['state'] == 'claimed' and attempt['submission'] == 'not_submitted':
            if task['cancel_requested']:
                self.store.confirm_cancel(claim)
                return 'cancelled'
            self.store.begin_submission(claim)
            try:
                outcome = self.executor.submit(context)
            except Exception:
                return 'unknown'
        else:
            try:
                operation = self.executor.cancel if task['cancel_requested'] else self.executor.query
                outcome = operation(context, attempt['run_id'])
            except Exception:
                return 'unknown'
        if not isinstance(outcome, ExecutionOutcome):
            return 'unknown'
        try:
            return self._apply(claim, outcome)
        except OwnershipError:
            # A slow call outlived its lease. Its committed attempt remains for
            # reconciliation; an old worker cannot overwrite the replacement.
            return 'ownership_lost'


@dataclass(frozen=True)
class SendOutcome:
    """Platform acceptance, proven rejection, or an uncertain delivery result."""

    state: str
    evidence: str | None = None
    receipt: str | None = None
    retry_after: float = 30


class DeliveryRunner:
    """Prepare stable text segments and invoke an account-bound transport once."""

    def __init__(self, store: Store, transport: Callable[[DeliveryClaim], SendOutcome],
                 owner: str, chunk_limit: int = 1200, lease_seconds: float = 60, *,
                 channel: str | None = None, account_id: str | None = None):
        """A transport must verify it handles the claim's channel and account."""
        if chunk_limit < 1 or lease_seconds <= 0 or not owner:
            raise ValueError('positive chunk_limit/lease and owner are required')
        if (channel is None) != (account_id is None):
            raise ValueError('channel and account_id filters must be supplied together')
        self.channel, self.account_id = channel, account_id
        self.store, self.transport, self.owner = store, transport, owner
        self.delivery = DeliveryStore(store)
        self.chunk_limit, self.lease_seconds = chunk_limit, lease_seconds

    def prepare(self) -> None:
        """Persist deterministic segmentation for all new execution results."""
        for row in self.store.connection.execute(
            "SELECT m.id,m.text FROM outbound_messages m WHERE m.state='pending' "
            'AND NOT EXISTS (SELECT 1 FROM outbound_parts p WHERE p.outbound_id=m.id)'
        ).fetchall():
            chunks = [row['text'][start:start + self.chunk_limit]
                      for start in range(0, len(row['text']), self.chunk_limit)] or ['']
            self.delivery.prepare(row['id'], chunks)

    def tick(self) -> str:
        """Persist sending before IO, and treat unexpected exceptions as unknown."""
        self.prepare()
        claim = self.delivery.claim(self.owner, self.lease_seconds,
                                    channel=self.channel, account_id=self.account_id)
        if claim is None:
            return 'idle'
        try:
            outcome = self.transport(claim)
        except Exception:
            outcome = SendOutcome('unknown')
        try:
            if not isinstance(outcome, SendOutcome):
                self.delivery.unknown(claim)
                return 'unknown'
            if outcome.state == 'delivered' and outcome.receipt:
                self.delivery.delivered(claim, outcome.receipt)
            elif outcome.state == 'retry' and outcome.evidence:
                self.delivery.retry(claim, outcome.evidence, outcome.retry_after)
            elif outcome.state == 'rejected' and outcome.evidence:
                self.delivery.reject(claim, outcome.evidence)
            else:
                self.delivery.unknown(claim)
                return 'unknown'
        except OwnershipError:
            return 'ownership_lost'
        return outcome.state
