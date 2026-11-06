"""Reproduction tests for core fix wave: runtime receipts/unknown + delivery.

Each test fails on the pre-fix code and passes after the fix. Temporary
SQLite databases only; production state is never touched.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.delivery import DeliveryClaim, DeliveryStore
from mobile_channels.models import InboundMessage
from mobile_channels.runtime import (CoreRunner, DeliveryRunner, ExecutionOutcome,
                                     SendOutcome)
from mobile_channels.storage import Store


def message(mid: str, chat: str = "alice", received_at: float | None = None):
    return InboundMessage("weixin", "bot", "single", chat, mid, chat, "hello",
                          received_at=received_at)


class RuntimeFixTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fix-runtime.sqlite3"
        self.now = 1000.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.executor = Mock()
        self.executor.submit.return_value = ExecutionOutcome("accepted", run_id="run1")
        self.executor.query.return_value = ExecutionOutcome("succeeded", text="answer")
        self.executor.reply_started.return_value = False
        self.executor.fold.return_value = ExecutionOutcome("accepted")

    def receipts(self, task_id=None, kind=None):
        sql = "SELECT * FROM task_receipts"
        args: list = []
        conds = []
        if task_id is not None:
            conds.append("task_id=?")
            args.append(task_id)
        if kind is not None:
            conds.append("kind=?")
            args.append(kind)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        return self.store.connection.execute(sql, args).fetchall()

    def test_outcome_media_defaults_empty(self):
        self.assertEqual(ExecutionOutcome("succeeded", text="x").media, [])

    def test_started_receipt_on_claim_not_on_receive(self):
        item = self.store.ingest(message("m1"))
        self.assertEqual(self.receipts(item.task_id, "started"), [])
        runner = CoreRunner(self.store, self.executor, "worker")
        self.assertEqual(runner.tick(), "accepted")
        rows = self.receipts(item.task_id, "started")
        self.assertEqual(len(rows), 1)
        self.assertIn("处理", rows[0]["text"])

    def test_merged_task_never_gets_started_receipt(self):
        first = self.store.ingest(message("m1"))
        second = self.store.ingest(message("m2"))
        runner = CoreRunner(self.store, self.executor, "worker")
        self.assertEqual(runner.tick(), "accepted")
        self.assertEqual(runner.tick(), "folded")
        self.assertEqual(self.store.task(second.task_id)["state"], "succeeded")
        self.assertEqual(self.receipts(second.task_id, "started"), [])
        self.assertEqual(len(self.receipts(first.task_id, "started")), 1)

    def test_wait_receipt_after_180s_with_live_position(self):
        first = self.store.ingest(message("m1", received_at=1000.0))
        second = self.store.ingest(message("m2", received_at=800.0))
        runner = CoreRunner(self.store, self.executor, "worker")
        self.now = 1000.0
        # First tick claims the first task; the second has queued 200s.
        self.assertEqual(runner.tick(), "accepted")
        rows = self.receipts(second.task_id, "wait")
        self.assertEqual(len(rows), 1)
        self.assertIn("2", rows[0]["text"])
        # Second tick must not duplicate the wait receipt.
        runner.tick()
        self.assertEqual(len(self.receipts(second.task_id, "wait")), 1)

    def test_unknown_threshold_fails_task_after_10_polls(self):
        item = self.store.ingest(message("m1"))
        self.executor.query.return_value = ExecutionOutcome("unknown")
        runner = CoreRunner(self.store, self.executor, "worker")
        self.assertEqual(runner.tick(), "accepted")
        result = "unknown"
        for _ in range(10):
            result = runner.tick()
        self.assertEqual(result, "failed")
        self.assertEqual(self.store.task(item.task_id)["state"], "failed")
        self.assertEqual(len(self.receipts(item.task_id, "failed")), 1)

    def test_failed_outcome_writes_failed_receipt_via_runner(self):
        item = self.store.ingest(message("m1"))
        self.executor.query.return_value = ExecutionOutcome(
            "failed", evidence="gadget-dead")
        runner = CoreRunner(self.store, self.executor, "worker")
        runner.tick()
        self.assertEqual(runner.tick(), "failed")
        self.assertEqual(len(self.receipts(item.task_id, "failed")), 1)

    def test_succeeded_outcome_persists_media(self):
        item = self.store.ingest(message("m1"))
        media = [{"kind": "video", "path": "/home/hatch/workspace/v.mp4",
                  "filename": "v.mp4"}]
        self.executor.query.return_value = ExecutionOutcome(
            "succeeded", text="视频在这里", media=media)
        runner = CoreRunner(self.store, self.executor, "worker")
        runner.tick()
        self.assertEqual(runner.tick(), "succeeded")
        import json
        task_row = self.store.task(item.task_id)
        self.assertEqual(json.loads(task_row["media_json"]), media)


class DeliveryFixTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fix-delivery.sqlite3"
        self.now = 1000.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.item = self.store.ingest(message("m1"))
        claim = self.store.claim("executor")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run-1")
        media = [{"kind": "image", "path": "/home/hatch/workspace/x.png",
                  "filename": "x.png"}]
        self.media = media
        self.outbound = self.store.complete(claim, "hello world", media=media)
        self.delivery = DeliveryStore(self.store)

    def receipts(self, task_id=None, kind=None):
        sql = "SELECT * FROM task_receipts"
        args: list = []
        conds = []
        if task_id is not None:
            conds.append("task_id=?")
            args.append(task_id)
        if kind is not None:
            conds.append("kind=?")
            args.append(kind)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        return self.store.connection.execute(sql, args).fetchall()

    def finish_another(self, mid, text="answer"):
        self.store.ingest(message(mid))
        claim = self.store.claim("executor")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, f"run-{mid}")
        outbound = self.store.complete(claim, text)
        DeliveryStore(self.store).prepare(outbound, [text])
        return outbound

    def test_delivery_claim_carries_media(self):
        self.assertEqual(DeliveryClaim("p", "o", "w", 1, "t", "weixin", "bot",
                                       "single", "alice", 0).media, [])
        parts = self.delivery.prepare(self.outbound, ["hello ", "world"])
        first = self.delivery.claim("sender")
        self.assertEqual(first.media, self.media)
        self.delivery.delivered(first, "r1")
        second = self.delivery.claim("sender")
        self.assertEqual(second.media, [])
        cols = {r["name"] for r in self.store.connection.execute(
            "PRAGMA table_info(outbound_parts)")}
        self.assertIn("media_json", cols)

    def test_delivery_prepare_persists_media_on_parts(self):
        parts = self.delivery.prepare(self.outbound, ["hello world"])
        row = self.store.connection.execute(
            "SELECT media_json FROM outbound_parts WHERE id=?", (parts[0],)).fetchone()
        import json
        self.assertEqual(json.loads(row["media_json"]), self.media)

    def test_media_does_not_participate_in_text_chunking(self):
        self.delivery.prepare(self.outbound, ["hello ", "world"])
        runner = DeliveryRunner(self.store, Mock(return_value=SendOutcome(
            "delivered", receipt="r")), "sender", chunk_limit=5)
        # prepare() must not re-chunk media into text parts.
        runner.prepare()
        texts = [r["text"] for r in self.store.connection.execute(
            "SELECT text FROM outbound_parts ORDER BY part_index")]
        self.assertEqual(texts, ["hello ", "world"])

    def test_rejected_delivery_writes_delivery_failed_receipt(self):
        self.delivery.prepare(self.outbound, ["hello world"])
        sender = Mock(return_value=SendOutcome("rejected", evidence="bad-recipient"))
        runner = DeliveryRunner(self.store, sender, "sender")
        self.assertEqual(runner.tick(), "rejected")
        rows = self.receipts(self.item.task_id, "delivery_failed")
        self.assertEqual(len(rows), 1)
        self.assertIn("发送失败", rows[0]["text"])

    def test_delivery_unknown_auto_rejects_after_10_minutes(self):
        later = self.finish_another("m2")
        self.delivery.prepare(self.outbound, ["hello world"])
        first = self.delivery.claim("sender")
        self.delivery.unknown(first)
        self.now += 100  # within window: still blocked, still unknown
        self.assertIsNone(self.delivery.claim("sender"))
        self.assertEqual(self.store.connection.execute(
            "SELECT state FROM outbound_messages WHERE id=?", (self.outbound,)
        ).fetchone()[0], "delivery_unknown")
        self.now += 600  # now 700s old: sweep on next claim rejects it
        nxt = self.delivery.claim("sender")
        self.assertIsNotNone(nxt)
        self.assertEqual(nxt.outbound_id, later)
        self.assertEqual(self.store.connection.execute(
            "SELECT state FROM outbound_messages WHERE id=?", (self.outbound,)
        ).fetchone()[0], "rejected")
        self.assertEqual(len(self.receipts(self.item.task_id, "delivery_failed")), 1)
        self.delivery.delivered(nxt, "later-receipt")

    def test_delivery_unknown_rejects_after_one_retry_still_fails(self):
        self.delivery.prepare(self.outbound, ["hello world"])
        first = self.delivery.claim("sender")
        self.delivery.unknown(first)
        self.delivery.resolve_unknown(first.part_id, "retry_wait",
                                      "operator-accepts-duplicate-risk")
        retry = self.delivery.claim("sender")
        self.assertEqual(retry.part_id, first.part_id)
        self.delivery.unknown(retry)
        self.now += 10  # far inside the 10-minute window, but 2nd unknown
        self.delivery.claim("sender")  # sweep
        self.assertEqual(self.store.connection.execute(
            "SELECT state FROM outbound_messages WHERE id=?", (self.outbound,)
        ).fetchone()[0], "rejected")
        self.assertEqual(len(self.receipts(self.item.task_id, "delivery_failed")), 1)


if __name__ == "__main__":
    unittest.main()
