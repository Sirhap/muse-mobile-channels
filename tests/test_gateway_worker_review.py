"""Pending execution review remains visible and cancellable only by its owner."""
from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ReviewCommandTests(unittest.TestCase):
    """Use real slash dispatchers with temporary ledgers and no network clients."""

    def setUp(self):
        """Keep source messages, review records and command side effects isolated."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)

    def gateway(self, channel):
        """Load either gateway and replace its channel/bridge filesystem boundaries."""
        spec = importlib.util.spec_from_file_location(f"review_{channel}", ROOT / f"{channel}-bot/gateway.py")
        gateway = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gateway)
        state = self.base / channel / "state"
        hooks = self.base / channel / "hooks"
        state.mkdir(parents=True)
        hooks.mkdir(parents=True)
        gateway.STATE, gateway.HOOK_STATE_DIR = state, hooks
        gateway._bridge_queue_lines = lambda _channel: ("", [])
        gateway._bridge_snapshot = lambda _channel: ([], [])
        gateway._bridge_active_detail = lambda _channel: []
        gateway._bridge_queued_rows = lambda _channel: []
        gateway._bridge_status_active = lambda _channel: []
        gateway._spool_texts = lambda _channel: {}
        return gateway

    def plant(self, gateway, channel, *, status="pending", actor="owner", chat="room", mid="original"):
        """Review does not carry authority; the original source message supplies it."""
        records = {mid: {"status": status, "run_id": "event-hook:original"}}
        (gateway.HOOK_STATE_DIR / "worker_review.json").write_text(json.dumps({"tasks": records}))
        row = {"msgid": mid, "text": "original task", "ts": 100}
        if channel == "weixin":
            row.update(from_user_id=actor, group_id=chat)
        else:
            row.update(from_userid=actor, chatid=chat, chattype="group")
        (gateway.STATE / "inbox.jsonl").write_text(json.dumps(row) + "\n")

    def dispatch(self, gateway, channel, text, *, actor="owner", chat="room"):
        """Capture only acknowledgements and cancellation subprocess invocations."""
        acknowledgements = []

        async def acknowledge(*args):
            """Read the channel-specific ack argument without sending a message."""
            acknowledgements.append(args[-1] if channel == "weixin" else args[2])

        sink = type("Sink", (), {})()
        sink._send_slash_ack = acknowledge
        sink._feedback_forget = lambda *args, **kwargs: None
        process = Mock(return_value=Mock(returncode=0))
        with patch.object(gateway.subprocess, "run", process):
            if channel == "weixin":
                result = asyncio.run(gateway.Gateway._slash_dispatch(
                    sink, None, None, {"group_id": chat, "from_user_id": actor}, text, actor))
            else:
                result = asyncio.run(gateway.Gateway._slash_dispatch(
                    sink, text, chat, "group", "command", "", actor))
        self.assertEqual(result, "handled")
        self.assertEqual(len(acknowledgements), 1)
        return acknowledgements[0], process

    def test_pending_only_stop_registers_cancel_without_claiming_running(self):
        """A task removed from active scheduling still accepts its owner's stop."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                gateway = self.gateway(channel)
                self.plant(gateway, channel)
                text, process = self.dispatch(gateway, channel, "/stop")
                process.assert_called_once()
                self.assertEqual(process.call_args.args[0][-2:], ["--msgid", "original"])
                self.assertIn("待核查", text)
                self.assertIn("仅登记取消", text)
                self.assertNotIn("1 条正在跑", text)
                self.assertNotIn("当前没有正在跑", text)

    def test_review_stop_requires_both_chat_and_original_actor(self):
        """Being another sender in the same group does not own the pending task."""
        for channel in ("weixin", "wecom"):
            for actor, chat in (("outsider", "room"), ("owner", "elsewhere"), ("", "room")):
                with self.subTest(channel=channel, actor=actor, chat=chat):
                    gateway = self.gateway(f"{channel}")
                    self.plant(gateway, channel)
                    _text, process = self.dispatch(gateway, channel, "/stop", actor=actor, chat=chat)
                    process.assert_not_called()
                    # A reused fixture directory is unnecessary between subcases.
                    shutil.rmtree(self.base / channel)

    def test_pending_without_original_source_and_resolved_records_cannot_cancel(self):
        """A ledger entry by itself cannot grant cancellation authority."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel, status="resolved")
            _text, process = self.dispatch(gateway, channel, "/stop")
            process.assert_not_called()
            self.plant(gateway, channel)
            (gateway.STATE / "inbox.jsonl").unlink()
            _text, process = self.dispatch(gateway, channel, "/stop")
            process.assert_not_called()

    def test_queue_and_status_show_review_separately_from_running_and_idle(self):
        """A pending task is neither an active execution nor a normal queued item."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel)
            queue, _process = self.dispatch(gateway, channel, "/queue")
            self.assertIn("待核查", queue)
            self.assertNotIn("队列空闲", queue)
            status, _process = self.dispatch(gateway, channel, "/status")
            self.assertIn("待核查任务：1 条", status)
            self.assertIn("运行中任务：0 条", status)
            self.assertNotIn("当前没有进行中或排队", status)
            other, _process = self.dispatch(gateway, channel, "/queue", actor="outsider")
            self.assertNotIn("待核查", other)

    def test_queue_admin_does_not_clear_or_drop_review(self):
        """Review must not re-enter the list of never-started queued work."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel)
            for command in ("/queue clear", "/queue drop 1"):
                _text, process = self.dispatch(gateway, channel, command)
                process.assert_not_called()
                self.assertFalse((gateway.HOOK_STATE_DIR / "queue_admin.json").exists())
                self.assertEqual(json.loads((gateway.HOOK_STATE_DIR / "worker_review.json").read_text())["tasks"]["original"]["status"], "pending")

    def test_empty_queue_keeps_existing_idle_reply(self):
        """The new review section does not invent work on a fresh installation."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            queue, _process = self.dispatch(gateway, channel, "/queue")
            self.assertIn("队列空闲", queue)
            text, process = self.dispatch(gateway, channel, "/stop")
            self.assertIn("当前没有正在跑", text)
            process.assert_not_called()

    def test_stale_active_and_queue_copies_cannot_bypass_review_authorization(self):
        """A crash after the review commit cannot leak authority through old batch state."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel)
            (gateway.HOOK_STATE_DIR / "active_batch.json").write_text(json.dumps({
                "msgids": ["original"], "detached": [{"msgids": ["original"]}]}))
            (gateway.HOOK_STATE_DIR / "pending.json").write_text(json.dumps({"original": 100}))
            _text, process = self.dispatch(gateway, channel, "/stop", actor="outsider")
            process.assert_not_called()
            text, _process = self.dispatch(gateway, channel, "/status")
            self.assertIn("运行中任务：0 条", text)
            self.assertIn("待核查任务：1 条", text)
            _text, process = self.dispatch(gateway, channel, "/queue clear")
            process.assert_not_called()
            self.assertFalse((gateway.HOOK_STATE_DIR / "queue_admin.json").exists())

    def test_conflicting_source_identities_cannot_authorize_pending_cancel(self):
        """Duplicate inbox IDs with different routes are not usable ownership proof."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel)
            path = gateway.STATE / "inbox.jsonl"
            original = json.loads(path.read_text().strip())
            conflict = dict(original)
            conflict["from_user_id" if channel == "weixin" else "from_userid"] = "outsider"
            path.write_text(json.dumps(original) + "\n" + json.dumps(conflict) + "\n")
            _text, process = self.dispatch(gateway, channel, "/stop")
            process.assert_not_called()

    def test_direct_chat_review_matches_original_sender_and_private_route(self):
        """Private chat commands work without requiring a group ID."""
        for channel in ("weixin", "wecom"):
            gateway = self.gateway(channel)
            self.plant(gateway, channel, chat="owner")
            path = gateway.STATE / "inbox.jsonl"
            source = json.loads(path.read_text().strip())
            if channel == "weixin":
                source["group_id"] = ""
            else:
                source["chattype"] = "single"
            path.write_text(json.dumps(source) + "\n")
            ids = gateway._scoped_worker_review_ids(gateway.STATE, gateway.HOOK_STATE_DIR,
                                                    channel, "owner", "owner", "single")
            self.assertEqual(ids, ["original"])
            wrong = gateway._scoped_worker_review_ids(gateway.STATE, gateway.HOOK_STATE_DIR,
                                                      channel, "owner", "owner", "group")
            self.assertEqual(wrong, [])


if __name__ == "__main__":
    unittest.main()
