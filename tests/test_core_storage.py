"""Fault, restart, and competing-worker contracts for the portable SQLite core."""
from __future__ import annotations

import concurrent.futures
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from mobile_channels.models import InboundMessage, OwnershipError, StateError
from mobile_channels.storage import Store


def message(mid: str, chat: str = "alice", channel: str = "weixin") -> InboundMessage:
    """Construct a deterministic incoming message without transport dependencies."""
    return InboundMessage(channel, "bot", "single", chat, mid, chat, "hello")


class StorageTests(unittest.TestCase):
    """Assert externally meaningful safety contracts across real SQLite connections."""

    def setUp(self):
        """Use a local temporary database and a controllable clock."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "core.sqlite3"
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)

    def test_platform_cancel_requires_evidence_and_preserves_completion(self):
        """Platform facts require fenced ownership and cannot erase a saved result."""
        item = self.store.ingest(message('external-cancel'))
        claim = self.store.claim('worker')
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, 'original-run')
        with self.assertRaises(StateError):
            self.store.confirm_cancel(claim)
        with self.assertRaises(ValueError):
            self.store.confirm_platform_cancel(claim, '')
        self.store.complete(claim, 'already complete')
        with self.assertRaises(OwnershipError):
            self.store.confirm_platform_cancel(claim, 'late_platform_cancel')
        self.assertEqual(self.store.task(item.task_id)['state'], 'succeeded')

    def test_platform_cancel_rejects_stale_and_unsubmitted_worker(self):
        """Old worker evidence and unsubmitted claims cannot cancel another attempt."""
        item = self.store.ingest(message('fenced-cancel'))
        old = self.store.claim('old', lease_seconds=10)
        with self.assertRaises(StateError):
            self.store.confirm_platform_cancel(old, 'platform_cancelled')
        self.store.begin_submission(old)
        self.now += 11
        recovered = self.store.claim_reconciliation(item.task_id, 'new')
        with self.assertRaises(OwnershipError):
            self.store.confirm_platform_cancel(old, 'platform_cancelled')
        self.store.confirm_platform_cancel(recovered, 'platform_cancelled')
        self.assertEqual(self.store.task(item.task_id)['state'], 'cancelled')

    def test_complete_command_commits_result_outbox_and_event_atomically(self):
        """Local command receipts bind the current generation and share Store invariants."""
        item = self.store.ingest(message('command'))
        self.store.new_generation(item.conversation_id)
        with self.store.transaction():
            outbound = self.store.complete_command(item.task_id, 'command done')
        row = self.store.connection.execute('SELECT * FROM outbound_messages WHERE id=?', (outbound,)).fetchone()
        self.assertEqual((row['generation'], row['conversation_id'], row['text']),
                         (1, item.conversation_id, 'command done'))
        self.assertEqual(self.store.task(item.task_id)['state'], 'succeeded')
        self.assertEqual(self.store.task(item.task_id)['generation'], 1)
        self.assertEqual(self.store.connection.execute(
            "SELECT COUNT(*) FROM task_events WHERE task_id=? AND event='command_succeeded'",
            (item.task_id,)).fetchone()[0], 1)
        with self.assertRaises(StateError):
            self.store.complete_command(item.task_id, 'duplicate')

    def test_complete_command_rejects_claimed_task_and_rolls_back_write_failure(self):
        """A local command cannot steal work or commit without its outbound result."""
        item = self.store.ingest(message('claimed-command'))
        self.store.claim('worker')
        with self.assertRaises(StateError):
            self.store.complete_command(item.task_id, 'not allowed')
        other = self.store.ingest(message('write-failure', chat='bob'))
        self.store.connection.execute("CREATE TRIGGER fail_command BEFORE INSERT ON outbound_messages "
                                      "BEGIN SELECT RAISE(ABORT,'simulated failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.complete_command(other.task_id, 'lost reply')
        self.assertEqual(self.store.task(other.task_id)['state'], 'queued')
        self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM outbound_messages').fetchone()[0], 0)

    def test_connection_pragmas_and_schema_survive_reopen(self):
        """Durability and referential integrity are configured for every connection."""
        with Store(self.path) as reopened:
            for pragma, expected in (("foreign_keys", 1), ("synchronous", 2),
                                     ("busy_timeout", 5000), ("journal_mode", "wal")):
                self.assertEqual(reopened.connection.execute(f"PRAGMA {pragma}").fetchone()[0], expected)
            self.assertEqual(reopened.snapshot()["schema_version"], 1)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_reject_future_schema(self):
        """Older code never silently opens a newer schema."""
        self.store.connection.execute("PRAGMA user_version=999")
        with self.assertRaises(StateError):
            Store(self.path)

    def test_parallel_ingestion_has_one_message_and_task(self):
        """Two independent workers racing on one external ID commit one task."""
        barrier = threading.Barrier(2)

        def ingest_once():
            """Coordinate writes from separate SQLite connections."""
            with Store(self.path) as other:
                barrier.wait(timeout=5)
                return other.ingest(message("m1"))

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: ingest_once(), range(2)))
        self.assertEqual(sum(result.created for result in results), 1)
        self.assertEqual(results[0].task_id, results[1].task_id)
        self.assertEqual(self.store.snapshot()["tasks"], {"queued": 1})

    def test_transaction_rolls_back_entire_ingestion(self):
        """A failing task insertion cannot leave an accepted message without work."""
        self.store.connection.execute(
            "CREATE TRIGGER break_ingest BEFORE INSERT ON tasks BEGIN "
            "SELECT RAISE(ABORT,'disk fault'); END"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.ingest(message("m1"))
        self.assertEqual(self.store.snapshot()["conversations"], 0)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM inbound_messages").fetchone()[0], 0)

    def test_routes_do_not_share_messages_or_generations(self):
        """Identical external IDs on different routes stay isolated after /new."""
        first = self.store.ingest(message("same"))
        second = self.store.ingest(message("same", chat="bob"))
        third = self.store.ingest(message("same", channel="wecom"))
        self.assertEqual(len({first.task_id, second.task_id, third.task_id}), 3)
        self.assertEqual(self.store.new_generation(first.conversation_id), 1)
        later = self.store.ingest(message("later"))
        self.assertEqual(self.store.task(first.task_id)["generation"], 0)
        self.assertEqual(self.store.task(later.task_id)["generation"], 1)
        self.assertEqual(self.store.task(second.task_id)["generation"], 0)

    def test_fifo_session_exclusion_and_global_concurrency(self):
        """Other sessions progress while an earlier task blocks its own session."""
        first = self.store.ingest(message("a1"))
        self.store.ingest(message("a2"))
        second = self.store.ingest(message("b1", "bob"))
        self.assertEqual(self.store.claim("w1", concurrency=2).task_id, first.task_id)
        self.assertEqual(self.store.claim("w2", concurrency=2).task_id, second.task_id)
        self.assertIsNone(self.store.claim("w3", concurrency=2))

    def test_competing_claims_have_one_owner_per_session(self):
        """Two scheduler connections cannot bypass session exclusion."""
        self.store.ingest(message("a1"))
        self.store.ingest(message("a2"))
        barrier = threading.Barrier(2)

        def claim_once(owner):
            """Race scheduler transactions after both connections are open."""
            with Store(self.path) as other:
                barrier.wait(timeout=5)
                return other.claim(owner)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim_once, ("worker1", "worker2")))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_unsubmitted_expiry_requeues_and_fences_old_worker(self):
        """Only proven-unsubmitted work is automatically recoverable after a crash."""
        item = self.store.ingest(message("a1"))
        old = self.store.claim("old", lease_seconds=10)
        self.now += 11
        with Store(self.path, clock=lambda: self.now) as restarted:
            new = restarted.claim("new")
            self.assertEqual(new.task_id, item.task_id)
            self.assertGreater(new.token, old.token)
            with self.assertRaises(OwnershipError):
                self.store.begin_submission(old)

    def test_unknown_submission_never_requeues_on_restart(self):
        """Crashing in the external call window reserves the task for reconciliation."""
        item = self.store.ingest(message("a1"))
        self.store.ingest(message("a2"))
        claim = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(claim)
        self.now += 11
        with Store(self.path, clock=lambda: self.now) as restarted:
            self.assertIsNone(restarted.claim("new"))
            self.assertEqual(restarted.task(item.task_id)["state"], "reconciling")
        with self.assertRaises(OwnershipError):
            self.store.complete(claim, "late result")

    def test_accepted_expiry_never_requeues(self):
        """A missing worker heartbeat does not imply its platform run stopped."""
        item = self.store.ingest(message("a1"))
        claim = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "external-run")
        self.now += 11
        self.assertIsNone(self.store.claim("new"))
        self.assertEqual(self.store.task(item.task_id)["state"], "reconciling")

    def test_completion_and_outbox_roll_back_together(self):
        """A failed outbox write cannot publish a completed execution."""
        item = self.store.ingest(message("a1"))
        claim = self.store.claim("worker")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run")
        self.store.connection.execute(
            "CREATE TRIGGER break_outbox BEFORE INSERT ON outbound_messages BEGIN "
            "SELECT RAISE(ABORT,'disk fault'); END"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.complete(claim, "answer")
        self.assertEqual(self.store.task(item.task_id)["state"], "running")
        self.store.connection.execute("DROP TRIGGER break_outbox")
        outbound_id = self.store.complete(claim, "answer")
        row = self.store.connection.execute("SELECT * FROM outbound_messages WHERE id=?", (outbound_id,)).fetchone()
        self.assertEqual(row["conversation_id"], item.conversation_id)
        self.assertEqual(row["state"], "pending")
        self.assertEqual(self.store.task(item.task_id)["state"], "succeeded")
        with self.assertRaises(OwnershipError):
            self.store.complete(claim, "duplicate")

    def test_cancel_is_scoped_and_does_not_overwrite_completed_result(self):
        """A cancellation request does not erase a confirmed completion."""
        item = self.store.ingest(message("a1"))
        with self.assertRaises(KeyError):
            self.store.request_cancel(item.task_id, "other-conversation")
        claim = self.store.claim("worker")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run")
        self.store.request_cancel(item.task_id, item.conversation_id)
        self.store.complete(claim, "already finished")
        self.assertFalse(self.store.request_cancel(item.task_id, item.conversation_id))
        with self.assertRaises(OwnershipError):
            self.store.confirm_cancel(claim)

    def test_cancellation_holds_session_until_confirmation(self):
        """A requested cancellation cannot release an external run prematurely."""
        item = self.store.ingest(message("a1"))
        next_item = self.store.ingest(message("a2"))
        claim = self.store.claim("worker")
        self.store.begin_submission(claim)
        self.store.request_cancel(item.task_id, item.conversation_id)
        self.assertIsNone(self.store.claim("other"))
        self.store.confirm_cancel(claim)
        self.assertEqual(self.store.claim("other").task_id, next_item.task_id)

    def test_cancel_before_submission_blocks_external_call(self):
        """Cancellation arriving after claim is checked before external submission."""
        item = self.store.ingest(message("a1"))
        claim = self.store.claim("worker")
        self.store.request_cancel(item.task_id, item.conversation_id)
        with self.assertRaises(StateError):
            self.store.begin_submission(claim)
        self.store.confirm_cancel(claim)

    def test_shadow_uncertainty_is_not_executable(self):
        """Imported records without sufficient evidence cannot create side effects."""
        self.store.ingest(message("legacy"), initial_state="reconciling")
        self.assertIsNone(self.store.claim("worker"))

    def test_later_uncertain_task_blocks_earlier_queued_task(self):
        """An imported active run reserves its session regardless of arrival ordering."""
        self.store.ingest(message("earlier-queued"))
        self.store.ingest(message("later-unknown"), initial_state="reconciling")
        other = self.store.ingest(message("other-session", chat="bob"))
        self.assertEqual(self.store.claim("worker").task_id, other.task_id)
        self.assertIsNone(self.store.claim("second-worker"))

    def test_reconciliation_serializes_distinct_tasks_in_same_session(self):
        """Two imported uncertain tasks cannot own the same Muse session concurrently."""
        first = self.store.ingest(message("unknown-1"), initial_state="reconciling")
        second = self.store.ingest(message("unknown-2"), initial_state="reconciling")
        claim = self.store.claim_reconciliation(first.task_id, "first-reconciler")
        self.assertIsNone(self.store.claim_reconciliation(second.task_id, "second-reconciler"))
        self.store.complete(claim, "recovered")
        self.assertIsNotNone(self.store.claim_reconciliation(second.task_id, "second-reconciler"))

    def test_reconciliation_recovers_existing_result_without_resubmission(self):
        """A new worker can query a lost run and commit its result exactly once."""
        item = self.store.ingest(message("a1"))
        old = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(old)
        self.now += 11
        recovered = self.store.claim_reconciliation(item.task_id, "reconciler")
        self.assertIsNotNone(recovered)
        self.assertIsNone(self.store.claim_reconciliation(item.task_id, "other"))
        with self.assertRaises(StateError):
            self.store.begin_submission(recovered)
        self.store.mark_submitted(recovered, "existing-run")
        self.store.complete(recovered, "recovered answer")
        self.assertEqual(self.store.snapshot()["outbound"], {"pending": 1})

    def test_definitive_rejection_releases_work_with_new_fencing_token(self):
        """Only explicit rejection evidence can make uncertain work executable again."""
        item = self.store.ingest(message("a1"), initial_state="reconciling")
        recovered = self.store.claim_reconciliation(item.task_id, "reconciler")
        with self.assertRaises(ValueError):
            self.store.confirm_not_submitted(recovered, "")
        self.store.confirm_not_submitted(recovered, "platform-confirmed-rejected")
        fresh = self.store.claim("executor")
        self.assertEqual(fresh.task_id, item.task_id)
        self.assertGreater(fresh.token, recovered.token)
        with self.assertRaises(OwnershipError):
            self.store.complete(recovered, "stale")

    def test_reconciliation_cannot_replace_bound_run(self):
        """Lease recovery preserves the original platform execution identity."""
        item = self.store.ingest(message("a1"))
        old = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(old)
        self.store.mark_submitted(old, "original-run")
        self.now += 11
        recovered = self.store.claim_reconciliation(item.task_id, "new")
        with self.assertRaises(StateError):
            self.store.mark_submitted(recovered, "different-run")
        self.store.mark_submitted(recovered, "original-run")

    def test_confirmed_failure_requires_evidence_and_releases_session(self):
        """Confirmed failure is durable and cannot be inferred from a missing heartbeat."""
        item = self.store.ingest(message("a1"))
        next_item = self.store.ingest(message("a2"))
        claim = self.store.claim("worker")
        with self.assertRaises(ValueError):
            self.store.fail(claim, "")
        self.store.fail(claim, "executor-confirmed-terminal-failure")
        self.assertEqual(self.store.task(item.task_id)["state"], "failed")
        self.assertEqual(self.store.claim("next").task_id, next_item.task_id)
        self.assertEqual(self.store.connection.execute(
            "SELECT evidence FROM task_events WHERE task_id=? AND event='confirmed_failed'",
            (item.task_id,),
        ).fetchone()[0], "executor-confirmed-terminal-failure")


if __name__ == "__main__":
    unittest.main()
