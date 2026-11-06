"""Reproduction tests for core fix wave (receipts, media, unknown, merged).

Each test fails on the pre-fix code and passes after the fix. All state
uses a temporary SQLite database; production state is never touched.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from mobile_channels.models import InboundMessage
from mobile_channels.storage import Store


def message(mid: str, chat: str = "alice", channel: str = "weixin",
            received_at: float | None = None) -> InboundMessage:
    return InboundMessage(channel, "bot", "single", chat, mid, chat, "hello",
                          received_at=received_at)


class ReceiptStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fix.sqlite3"
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)

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

    def running_task(self, mid="m1"):
        item = self.store.ingest(message(mid))
        claim = self.store.claim("worker")
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run-1")
        return item, claim

    def test_schema_has_task_receipts_and_media_columns(self):
        cols = {r["name"] for r in
                self.store.connection.execute("PRAGMA table_info(tasks)")}
        self.assertIn("media_json", cols)
        cols = {r["name"] for r in self.store.connection.execute(
            "PRAGMA table_info(outbound_messages)")}
        self.assertIn("media_json", cols)
        tables = {r[0] for r in self.store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("task_receipts", tables)

    def test_old_database_gains_media_columns_on_open(self):
        old_path = Path(self.temp.name) / "old.sqlite3"
        conn = sqlite3.connect(str(old_path))
        conn.execute(
            "CREATE TABLE tasks (sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
            "id TEXT NOT NULL UNIQUE, message_id TEXT NOT NULL UNIQUE, "
            "conversation_id TEXT NOT NULL, generation INTEGER NOT NULL, "
            "state TEXT NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0, "
            "token INTEGER NOT NULL DEFAULT 0, owner TEXT, lease_until REAL, "
            "attempt_id TEXT, result TEXT)")
        conn.execute(
            "CREATE TABLE outbound_messages (id TEXT PRIMARY KEY, "
            "task_id TEXT NOT NULL UNIQUE, conversation_id TEXT NOT NULL, "
            "generation INTEGER NOT NULL, text TEXT NOT NULL, "
            "state TEXT NOT NULL DEFAULT 'pending', created_at REAL NOT NULL)")
        conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
        conn.close()
        with Store(old_path) as reopened:
            cols = {r["name"] for r in reopened.connection.execute(
                "PRAGMA table_info(tasks)")}
            self.assertIn("media_json", cols)
            cols = {r["name"] for r in reopened.connection.execute(
                "PRAGMA table_info(outbound_messages)")}
            self.assertIn("media_json", cols)

    def test_add_receipt_idempotent_except_wait(self):
        item = self.store.ingest(message("m1"))
        first = self.store.add_receipt(item.task_id, item.conversation_id,
                                       "failed", "处理失败，请再发一次。")
        second = self.store.add_receipt(item.task_id, item.conversation_id,
                                        "failed", "处理失败，请再发一次。")
        self.assertEqual(first, second)
        self.assertEqual(len(self.receipts(item.task_id, "failed")), 1)
        self.store.add_receipt(item.task_id, item.conversation_id,
                               "wait", "排队第 1 位")
        self.store.add_receipt(item.task_id, item.conversation_id,
                               "wait", "排队第 1 位")
        self.assertEqual(len(self.receipts(item.task_id, "wait")), 2)

    def test_fail_writes_failed_receipt(self):
        item, claim = self.running_task()
        self.store.fail(claim, "executor-confirmed-terminal-failure")
        rows = self.receipts(item.task_id, "failed")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["conversation_id"], item.conversation_id)
        self.assertEqual(rows[0]["state"], "pending")
        self.assertIn("失败", rows[0]["text"])
        self.assertNotIn("成功", rows[0]["text"])

    def test_fail_notifies_merged_tasks(self):
        main, claim = self.running_task("main")
        merged = self.store.ingest(message("merged-msg"))
        self.store.mark_merged(merged.task_id, main.task_id)
        self.store.fail(claim, "executor-confirmed-terminal-failure")
        self.assertEqual(len(self.receipts(main.task_id, "failed")), 1)
        merged_rows = self.receipts(merged.task_id, "failed")
        self.assertEqual(len(merged_rows), 1)
        self.assertEqual(merged_rows[0]["conversation_id"], merged.conversation_id)

    def test_confirm_cancel_writes_cancelled_receipt(self):
        item, claim = self.running_task()
        self.store.request_cancel(item.task_id, item.conversation_id)
        self.store.confirm_cancel(claim)
        rows = self.receipts(item.task_id, "cancelled")
        self.assertEqual(len(rows), 1)
        self.assertIn("取消", rows[0]["text"])

    def test_confirm_cancel_notifies_merged_tasks(self):
        main, claim = self.running_task("main")
        merged = self.store.ingest(message("merged-msg"))
        self.store.mark_merged(merged.task_id, main.task_id)
        self.store.request_cancel(main.task_id, main.conversation_id)
        self.store.confirm_cancel(claim)
        self.assertEqual(len(self.receipts(merged.task_id, "cancelled")), 1)

    def test_confirm_platform_cancel_writes_cancelled_receipt(self):
        item, claim = self.running_task()
        self.store.confirm_platform_cancel(claim, "platform_run_cancelled")
        rows = self.receipts(item.task_id, "cancelled")
        self.assertEqual(len(rows), 1)

    def test_request_cancel_queued_task_writes_cancelled_receipt(self):
        item = self.store.ingest(message("m1"))
        self.assertTrue(self.store.request_cancel(item.task_id, item.conversation_id))
        self.assertEqual(self.store.task(item.task_id)["state"], "cancelled")
        self.assertEqual(len(self.receipts(item.task_id, "cancelled")), 1)

    def test_complete_persists_media(self):
        item, claim = self.running_task()
        media = [{"kind": "image", "path": "/home/hatch/workspace/x.png",
                  "filename": "x.png"}]
        self.store.complete(claim, "here is the picture", media=media)
        task_row = self.store.task(item.task_id)
        self.assertEqual(json.loads(task_row["media_json"]), media)
        out = self.store.connection.execute(
            "SELECT media_json FROM outbound_messages WHERE task_id=?",
            (item.task_id,)).fetchone()
        self.assertEqual(json.loads(out["media_json"]), media)

    def test_complete_warns_when_text_claims_file_but_has_no_url_or_media(self):
        item, claim = self.running_task()
        with self.assertLogs("mobile_channels.storage", level="WARNING") as logs:
            self.store.complete(claim, "文件已发给你，点链接查看")
        self.assertTrue(any(item.task_id in line for line in logs.output))

    def test_complete_no_warning_when_media_present(self):
        item, claim = self.running_task()
        media = [{"kind": "file", "path": "/home/hatch/workspace/f.pdf",
                  "filename": "f.pdf"}]
        with self.assertNoLogs("mobile_channels.storage", level="WARNING"):
            self.store.complete(claim, "文件已发给你", media=media)

    def test_rebind_writes_fallback_receipt(self):
        item, claim = self.running_task()
        self.store.connection.execute(
            "CREATE TABLE IF NOT EXISTS executor_runs (attempt_id TEXT PRIMARY KEY, "
            "session_id TEXT NOT NULL, platform_run_id TEXT, status TEXT NOT NULL, "
            "created_at REAL NOT NULL, track TEXT NOT NULL DEFAULT 'native', "
            "baseline_seq INTEGER)")
        self.store.connection.execute(
            "INSERT INTO executor_runs VALUES (?,?,?,?,?,?,?)",
            (claim.attempt_id, "sess", "run-2", "accepted", self.now, "native", 0))
        # The primary burned real waiting time (20s) before failing, so the
        # user gets an explanation for the delay.
        self.now = 120.0
        self.store.rebind_run(claim, "run-2")
        rows = self.receipts(item.task_id, "fallback")
        self.assertEqual(len(rows), 1)
        self.assertIn("备用", rows[0]["text"])

    def test_rebind_skips_fallback_receipt_when_fast(self):
        item, claim = self.running_task()
        self.store.connection.execute(
            "CREATE TABLE IF NOT EXISTS executor_runs (attempt_id TEXT PRIMARY KEY, "
            "session_id TEXT NOT NULL, platform_run_id TEXT, status TEXT NOT NULL, "
            "created_at REAL NOT NULL, track TEXT NOT NULL DEFAULT 'native', "
            "baseline_seq INTEGER)")
        self.store.connection.execute(
            "INSERT INTO executor_runs VALUES (?,?,?,?,?,?,?)",
            (claim.attempt_id, "sess", "run-2", "accepted", self.now, "native", 0))
        # Fast primary failure (5s): the backup answer lands almost as
        # fast as a normal turn, so no alarming "primary failed" receipt.
        self.now = 105.0
        self.store.rebind_run(claim, "run-2")
        rows = self.receipts(item.task_id, "fallback")
        self.assertEqual(len(rows), 0)

    def test_note_unknown_threshold_by_count(self):
        _, claim = self.running_task()
        for _ in range(9):
            self.assertFalse(self.store.note_unknown(claim))
        self.assertTrue(self.store.note_unknown(claim))

    def test_note_unknown_threshold_by_elapsed_time(self):
        item = self.store.ingest(message("m1"))
        claim = self.store.claim("worker", lease_seconds=2000)
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run-1")
        self.assertFalse(self.store.note_unknown(claim))
        self.now += 901
        self.assertTrue(self.store.note_unknown(claim))

    def test_reconciling_without_binding_fails_after_900s(self):
        item = self.store.ingest(message("m1"))
        claim = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(claim)
        self.now += 11
        self.assertIsNone(self.store.claim("new"))
        self.assertEqual(self.store.task(item.task_id)["state"], "reconciling")
        self.now += 900  # total 911s since submission started
        self.assertIsNone(self.store.claim("new"))
        self.assertEqual(self.store.task(item.task_id)["state"], "failed")
        self.assertEqual(len(self.receipts(item.task_id, "failed")), 1)

    def test_reconciling_with_binding_stays_reconciling(self):
        item = self.store.ingest(message("m1"))
        claim = self.store.claim("old", lease_seconds=10)
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, "run-1")
        self.now += 1000
        self.assertIsNone(self.store.claim("new"))
        self.assertEqual(self.store.task(item.task_id)["state"], "reconciling")


if __name__ == "__main__":
    unittest.main()
