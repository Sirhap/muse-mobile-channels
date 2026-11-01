"""Delivery crashes and unknown results cannot silently create duplicate sends."""
import concurrent.futures
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from mobile_channels.delivery import DeliveryStore
from mobile_channels.models import InboundMessage, OwnershipError, StateError
from mobile_channels.storage import Store


class DeliveryTests(unittest.TestCase):
    """Exercise real SQLite storage with no transport or production dependencies."""

    def setUp(self):
        """Create one finished task with a pending bound reply."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "delivery.sqlite3"
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.item = self.store.ingest(InboundMessage("weixin", "bot", "single", "alice", "m1", "alice", "hi"))
        claim = self.store.claim("executor")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run-1")
        self.outbound = self.store.complete(claim, "hello world")
        self.delivery = DeliveryStore(self.store)
        self.parts = self.delivery.prepare(self.outbound, ["hello ", "world"])

    def message_state(self):
        """Read the aggregate reply state independently of the delivery API."""
        return self.store.connection.execute(
            "SELECT state FROM outbound_messages WHERE id=?", (self.outbound,)
        ).fetchone()[0]

    def test_parts_have_stable_ids_and_immutable_text(self):
        """Retrying preparation preserves IDs and cannot change established segmentation."""
        self.assertEqual(self.delivery.prepare(self.outbound, ["hello ", "world"]), self.parts)
        with self.assertRaises(StateError):
            self.delivery.prepare(self.outbound, ["hello world"])
        with self.assertRaises(ValueError):
            self.delivery.prepare(self.outbound, ["replacement"])

    def test_route_generation_and_all_parts_required_for_delivery(self):
        """A newer conversation generation cannot redirect an already-bound reply."""
        self.store.new_generation(self.item.conversation_id)
        first = self.delivery.claim("sender")
        self.assertEqual((first.channel, first.account_id, first.chat_id, first.generation),
                         ("weixin", "bot", "alice", 0))
        self.assertIsNone(self.delivery.claim("other"))
        self.delivery.delivered(first, "receipt-1")
        self.assertEqual(self.message_state(), "pending")
        second = self.delivery.claim("sender")
        self.assertEqual(second.part_id, self.parts[1])
        self.delivery.delivered(second, "receipt-2")
        self.assertEqual(self.message_state(), "delivered")
        self.assertIsNone(self.delivery.claim("sender"))

    def test_send_attempt_is_committed_before_transport(self):
        """Another process observes sending before any external request is needed."""
        claim = self.delivery.claim("sender")
        with Store(self.path) as other:
            row = other.connection.execute("SELECT * FROM outbound_attempts").fetchone()
            self.assertEqual((row["part_id"], row["outcome"]), (claim.part_id, "sending"))

    def test_timeout_never_retries_without_explicit_resolution(self):
        """Unknown platform acceptance blocks this part and all later message parts."""
        claim = self.delivery.claim("sender")
        self.delivery.unknown(claim)
        self.now += 1000
        self.assertIsNone(self.delivery.claim("other"))
        self.assertEqual(self.message_state(), "delivery_unknown")
        with self.assertRaises(ValueError):
            self.delivery.resolve_unknown(claim.part_id, "retry_wait", "")
        self.delivery.resolve_unknown(claim.part_id, "retry_wait", "operator-accepts-duplicate-risk")
        new = self.delivery.claim("other")
        self.assertEqual(new.part_id, claim.part_id)
        self.assertGreater(new.token, claim.token)
        with self.assertRaises(OwnershipError):
            self.delivery.delivered(claim, "late")

    def test_crashed_sender_is_unknown_on_restart_not_retried(self):
        """Lease loss after a possible send never authorizes another network request."""
        old = self.delivery.claim("sender", lease_seconds=10)
        self.now += 11
        with Store(self.path, clock=lambda: self.now) as reopened:
            delivery = DeliveryStore(reopened)
            self.assertIsNone(delivery.claim("restarted"))
            self.assertEqual(reopened.snapshot()["outbound"], {"delivery_unknown": 1})
        with self.assertRaises(OwnershipError):
            self.delivery.delivered(old, "late")

    def test_confirmed_temporary_failure_honors_delay_and_stable_identity(self):
        """Explicit rejection can retry after its delay without changing segment IDs."""
        old = self.delivery.claim("sender")
        self.delivery.retry(old, "rate-limited-with-no-send", retry_after=30)
        self.assertEqual(self.message_state(), "retry_wait")
        self.assertIsNone(self.delivery.claim("other"))
        self.now += 31
        new = self.delivery.claim("other")
        self.assertEqual(new.part_id, old.part_id)
        self.assertGreater(new.token, old.token)

    def test_permanent_failure_is_visible_and_not_automatically_retried(self):
        """A permanent platform rejection stays visible instead of disappearing."""
        claim = self.delivery.claim("sender")
        self.delivery.reject(claim, "platform-recipient-disabled")
        self.now += 1000
        self.assertEqual(self.message_state(), "rejected")
        self.assertIsNone(self.delivery.claim("other"))

    def test_platform_query_can_confirm_unknown_delivery(self):
        """A recovered receipt advances parts without resending the uncertain part."""
        claim = self.delivery.claim("sender")
        self.delivery.unknown(claim)
        self.delivery.resolve_unknown(claim.part_id, "delivered", "receipt-query", "platform-receipt")
        self.assertEqual(self.delivery.claim("sender").part_id, self.parts[1])

    def test_failed_receipt_commit_rolls_back_part_and_aggregate(self):
        """A database fault leaves sending unresolved rather than reporting delivered."""
        claim = self.delivery.claim("sender")
        self.store.connection.execute(
            "CREATE TRIGGER fault_receipt BEFORE UPDATE ON outbound_attempts BEGIN "
            "SELECT RAISE(ABORT,'disk-fault'); END"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.delivery.delivered(claim, "receipt")
        part_state = self.store.connection.execute(
            "SELECT state FROM outbound_parts WHERE id=?", (claim.part_id,)
        ).fetchone()[0]
        self.assertEqual(part_state, "sending")
        self.assertEqual(self.message_state(), "sending")

    def finish_another(self, message_id, chat="alice", channel="weixin", account="bot"):
        """Complete another real task so its pending answer competes in delivery."""
        self.store.ingest(InboundMessage(channel, account, "single", chat, message_id, chat, "next"))
        claim = self.store.claim("executor")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, f"run-{message_id}")
        outbound = self.store.complete(claim, f"answer-{message_id}")
        self.delivery.prepare(outbound, [f"answer-{message_id}"])
        return outbound

    def test_unknown_prior_message_blocks_its_conversation_but_not_others(self):
        """A later answer cannot leap over an unknown send in the same chat."""
        later = self.finish_another("m2")
        other = self.finish_another("m3", chat="bob")
        first = self.delivery.claim("sender")
        self.delivery.unknown(first)
        next_claim = self.delivery.claim("sender")
        self.assertEqual(next_claim.outbound_id, other)
        self.delivery.delivered(next_claim, "other-receipt")
        self.assertIsNone(self.delivery.claim("sender"))
        self.delivery.resolve_unknown(first.part_id, "delivered", "queried", "receipt")
        tail = self.delivery.claim("sender")
        self.assertEqual(tail.part_id, self.parts[1])
        self.delivery.delivered(tail, "tail-receipt")
        self.assertEqual(self.delivery.claim("sender").outbound_id, later)

    def test_account_filter_never_claims_other_channel_or_account(self):
        """Independent gateway workers sharing one database cannot steal replies."""
        wecom = self.finish_another("wc", channel="wecom")
        alternate = self.finish_another("alternate", account="other-bot")
        claim = self.delivery.claim("wc-sender", channel="wecom", account_id="bot")
        self.assertEqual(claim.outbound_id, wecom)
        self.assertEqual((claim.channel, claim.account_id), ("wecom", "bot"))
        self.assertIsNone(self.delivery.claim("wc-sender", channel="wecom", account_id="bot"))
        other = self.delivery.claim("other", channel="weixin", account_id="other-bot")
        self.assertEqual(other.outbound_id, alternate)
        original = self.delivery.claim("wx", channel="weixin", account_id="bot")
        self.assertEqual(original.outbound_id, self.outbound)
        with self.assertRaises(ValueError):
            self.delivery.claim("bad-filter", channel="weixin")

    def test_competing_senders_cannot_claim_same_part(self):
        """Independent connections cannot both send the same logical segment."""
        barrier = threading.Barrier(2)

        def claim_once(owner):
            """Race actual writer transactions on separate connections."""
            with Store(self.path) as store:
                delivery = DeliveryStore(store)
                barrier.wait(timeout=5)
                return delivery.claim(owner)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim_once, ("one", "two")))
        self.assertEqual(sum(claim is not None for claim in claims), 1)


if __name__ == "__main__":
    unittest.main()
