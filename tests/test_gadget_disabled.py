"""Gadget is opt-out per channel: an explicit gadget_enabled=false in the
channel modes file must route Core submissions straight to Native (Cookie),
with the hook handoff still catching proven-unsent Native failures.
Old gadget-bound attempts must still query/cancel on the Gadget track.
"""
import dataclasses
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from mobile_channels.executors import DualTrackExecutor, GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.runtime import ExecutionContext, ExecutionOutcome
from mobile_channels.storage import Store


class GadgetDisabledTests(unittest.TestCase):
    """Temp store + temp modes file; no network, no platform, no production state."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "core.sqlite3"
        self.store = Store(self.database)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.modes = self.root / "modes.json"
        self._write_modes(gadget_enabled=False, hook_handoff=True)
        environment = patch.dict(os.environ, {"MUSE_CHANNEL_MODES": str(self.modes)})
        environment.start()
        self.addCleanup(environment.stop)
        self.primary = Mock(spec=GadgetExecutor)
        self.primary.is_configured.return_value = True
        self.fallback = Mock(spec=MuseExecutor)
        self.fallback.submit.return_value = ExecutionOutcome("accepted", run_id="native_run_1")
        self.executor = DualTrackExecutor(self.store, primary=self.primary, fallback=self.fallback)

    def _write_modes(self, gadget_enabled=None, hook_handoff=True):
        settings = {"mode": "core", "database": str(self.database), "account_id": "bot",
                    "allowed_senders": ["user_1"], "hook_handoff": hook_handoff}
        if gadget_enabled is not None:
            settings["gadget_enabled"] = gadget_enabled
        self.modes.write_text(json.dumps({"channels": {"wecom": settings}}))

    def _rebuild_executor(self):
        self.executor = DualTrackExecutor(self.store, primary=self.primary, fallback=self.fallback)

    def _create_context(self, msg_id="m1", media=None) -> ExecutionContext:
        self.ingress.receive(msg_id, "user_1", f"chat_{msg_id}", "single", f"text for {msg_id}")
        claim = self.store.claim("worker_1", concurrency=10, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        context = ExecutionContext(
            claim=claim, text=row["text"], external_message_id=row["external_id"],
            sender_id=row["sender_id"], channel=row["channel"], account_id=row["account_id"],
            chat_type=row["chat_type"], chat_id=row["chat_id"],
        )
        if media is not None:
            context = dataclasses.replace(context, media=media)
        return context

    def test_disabled_text_goes_native_without_touching_gadget(self):
        context = self._create_context("m_disabled_text")
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.primary.submit.assert_not_called()
        self.fallback.submit.assert_called_once()

    def test_disabled_image_goes_native_without_touching_gadget(self):
        context = self._create_context("m_disabled_image",
                                       media=[{"kind": "image", "path": "/tmp/x.jpg", "filename": "x.jpg"}])
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.primary.submit.assert_not_called()
        self.fallback.submit.assert_called_once()

    def test_key_absent_keeps_gadget(self):
        self._write_modes(gadget_enabled=None)
        self._rebuild_executor()
        self.primary.submit.return_value = ExecutionOutcome("accepted", run_id="gadget_run_1")
        context = self._create_context("m_absent_key")
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.primary.submit.assert_called_once()
        self.fallback.submit.assert_not_called()

    def test_no_modes_file_keeps_gadget(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MUSE_CHANNEL_MODES", None)
            executor = DualTrackExecutor(self.store, primary=self.primary, fallback=self.fallback)
        self.primary.submit.return_value = ExecutionOutcome("accepted", run_id="gadget_run_1")
        context = self._create_context("m_no_modes")
        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.primary.submit.assert_called_once()

    def test_disabled_native_failure_still_hands_off(self):
        self.fallback.submit.return_value = ExecutionOutcome(
            "not_submitted", evidence="executor_connect_failed:ConnectionError")
        context = self._create_context("m_disabled_handoff")
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "running")
        self.assertEqual(outcome.evidence, "hook_handoff_queued_waiting_receipt")
        self.primary.submit.assert_not_called()
        row = self.store.connection.execute(
            "SELECT state FROM core_hook_handoffs WHERE attempt_id=?",
            (context.claim.attempt_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["state"], "queued")

    def test_disabled_still_queries_old_gadget_binding(self):
        context = self._create_context("m_old_gadget")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) "
                "VALUES (?,?,?,?,?,?)",
                (context.claim.attempt_id, "s1", "gadget_run_1", "accepted", 100.0, "gadget"),
            )
        self.primary.query.return_value = ExecutionOutcome("accepted", run_id="gadget_run_1")
        outcome = self.executor.query(context, "gadget_run_1")
        self.assertEqual(outcome.state, "accepted")
        self.primary.query.assert_called_once()


if __name__ == "__main__":
    unittest.main()
