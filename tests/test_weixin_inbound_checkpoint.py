"""Exercise real iLink session checkpoints under local persistence failures."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("weixin_checkpoint", ROOT / "weixin-bot/gateway.py")
wx = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(wx)


class InboundCheckpointTests(unittest.IsolatedAsyncioTestCase):
    """Run real polling and inbound routing with isolated files and transport."""

    def setUp(self):
        """Replace all persistence and background activity with sandbox resources."""
        temporary = tempfile.TemporaryDirectory(prefix="weixin-checkpoint-")
        self.addCleanup(temporary.cleanup)
        state = Path(temporary.name)
        for name, filename in {
            "STATE": ".", "INBOX": "inbox.jsonl", "SEEN_FILE": "seen.jsonl",
            "SYNC_FILE": "sync.json", "CONTEXT_FILE": "context.json",
            "QUOTE_SPOOL": "spool.jsonl", "POISON_MESSAGES": "poison.jsonl",
            "DIVERTED_RAW": "diverted.jsonl",
        }.items():
            patcher = patch.object(wx, name, state / filename)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.gateway = wx.Gateway.__new__(wx.Gateway)
        self.gateway.seen_msgids = set()
        self.gateway.context = {}
        self.gateway.feedback_track = {}
        self.gateway.sync_buf = "old"
        self.gateway.msgs_received = 0
        self.gateway.write_status = lambda: None
        self.gateway._maybe_arrival_feedback = lambda *args, **kwargs: None
        self.gateway._send_typing = AsyncMock()
        self.gateway._slash_dispatch = AsyncMock(return_value=None)
        self.gateway.outbox_loop = AsyncMock()
        self.gateway.feedback_watch_loop = AsyncMock()
        self.credentials = {"base_url": "https://example.invalid", "token": "test", "user_id": "owner"}
        wx.SYNC_FILE.write_text(json.dumps({"buf": "old"}))

    def message(self, identifier):
        """Return one ordinary protocol message with a stable server identity."""
        return {"message_id": identifier, "from_user_id": "owner", "item_list": []}

    async def poll(self, messages):
        """Offer a batch, then end the real session via its stale-token path."""
        self.gateway._post = AsyncMock(side_effect=[
            {}, {"errcode": 0, "msgs": messages, "get_updates_buf": "new"},
            {"errcode": wx.STALE_TOKEN_ERRCODE},
        ])
        with patch.object(wx.os.path, "exists", return_value=False):
            await self.gateway.run_session(None, self.credentials)
        await asyncio.sleep(0)

    async def test_inbox_io_error_keeps_checkpoint_and_replay_deduplicates_prefix(self):
        """A failed second write preserves the batch and retries only missing work."""
        original = self.gateway.append_jsonl

        def fail_second(path, row):
            if path == wx.INBOX and row.get("msgid") == "2":
                raise OSError("temporary inbox failure")
            original(path, row)

        with patch.object(self.gateway, "append_jsonl", side_effect=fail_second):
            try:
                await self.poll([self.message(1), self.message(2)])
            except OSError:
                pass
        self.assertEqual(self.gateway.sync_buf, "old")
        self.assertEqual(json.loads(wx.SYNC_FILE.read_text())["buf"], "old")
        self.assertFalse(wx.POISON_MESSAGES.exists())
        await self.poll([self.message(1), self.message(2)])
        rows = [json.loads(line) for line in wx.INBOX.read_text().splitlines()]
        self.assertEqual([row["msgid"] for row in rows], ["1", "2"])
        self.assertEqual(self.gateway.sync_buf, "new")

    async def test_quarantine_failure_keeps_checkpoint(self):
        """A malformed payload may be skipped only after its quarantine is saved."""
        with patch.object(self.gateway, "append_jsonl", side_effect=OSError("disk full")):
            try:
                await self.poll([None])
            except OSError:
                pass
        self.assertEqual(self.gateway.sync_buf, "old")
        self.assertEqual(json.loads(wx.SYNC_FILE.read_text())["buf"], "old")

    async def test_malformed_payload_is_quarantined_without_blocking_valid_message(self):
        """Deterministic parse failure is isolated before the next message runs."""
        await self.poll([None, self.message(1)])
        self.assertEqual(self.gateway.sync_buf, "new")
        self.assertTrue(wx.POISON_MESSAGES.exists())
        self.assertEqual(json.loads(wx.INBOX.read_text())["msgid"], "1")

    async def test_unknown_error_is_retryable_without_quarantine(self):
        """Unexpected execution failures must not silently discard an inbound message."""
        for error_type in (RuntimeError, ValueError, KeyError, TypeError, AttributeError):
            with self.subTest(error=error_type.__name__):
                with patch.object(self.gateway, "handle_inbound", side_effect=error_type("unavailable")):
                    with self.assertRaises(error_type):
                        await self.poll([self.message(1)])
                self.assertEqual(self.gateway.sync_buf, "old")
                self.assertFalse(wx.POISON_MESSAGES.exists())

    async def test_command_write_failure_is_not_reclassified_as_ordinary_work(self):
        """A failed command keeps the cursor and never enters the ordinary inbox."""
        self.gateway._slash_dispatch = wx.Gateway._slash_dispatch.__get__(self.gateway)
        message = self.message(1)
        message["item_list"] = [{"type": 1, "text_item": {"text": "/ping"}}]
        with patch.object(wx, "slash_ping_text", side_effect=OSError("command state unavailable")):
            with self.assertRaises(OSError):
                await self.poll([message])
        self.assertEqual(self.gateway.sync_buf, "old")
        self.assertFalse(wx.INBOX.exists())
        self.assertFalse(wx.POISON_MESSAGES.exists())

    async def test_checkpoint_failure_restores_memory_cursor(self):
        """The next poll cannot use a cursor that failed to reach disk."""
        with patch.object(self.gateway, "_save_sync", side_effect=OSError("checkpoint failure")):
            with self.assertRaises(OSError):
                await self.poll([self.message(1)])
        self.assertEqual(self.gateway.sync_buf, "old")
        self.assertEqual(json.loads(wx.SYNC_FILE.read_text())["buf"], "old")

    async def test_seen_marker_failure_does_not_duplicate_committed_inbox_on_replay(self):
        """The inbox is sufficient evidence even when the secondary seen file fails."""
        original = wx.append_jsonl_line

        def fail_seen(path, row):
            if path == wx.SEEN_FILE:
                raise OSError("seen unavailable")
            original(path, row)

        with patch.object(wx, "append_jsonl_line", side_effect=fail_seen):
            await self.poll([self.message(1), self.message(1)])
        self.assertEqual(len(wx.INBOX.read_text().splitlines()), 1)

    def test_restart_recovers_bridge_spool_deduplication(self):
        """A diverted message remains committed after a failed checkpoint and restart."""
        wx.QUOTE_SPOOL.write_text(json.dumps({"msgid": "1", "text": "hello"}) + "\n")
        self.gateway._load_persisted()
        self.assertIn("1", self.gateway.seen_msgids)


if __name__ == "__main__":
    unittest.main()
