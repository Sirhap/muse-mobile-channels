"""Reuse a CF socket without replaying an uncertain chat submission."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "native-bridge/native_bridge.py"


class CFConnectionReuseTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("cf_reuse_bridge", SOURCE)
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bridge.BASE = str(self.root)
        self.bridge.STATE_F = str(self.root / "state.json")
        self.bridge.CFG = {"channels": {"weixin": {}}}
        self.bridge.cancelled_ids = lambda channel: set()
        self.bridge.read_signal_ts = lambda path: None
        self.spool = self.root / "spool" / "weixin.jsonl"
        self.spool.parent.mkdir()
        self.spool.write_bytes(b'{"msgid":"message","text":"hello"}\n')
        self.worker = self.bridge.ChannelWorker("weixin")
        self.worker.session_for = lambda cs: ("session", "")
        self.worker.idle_push_scan = Mock()
        self.worker._send_alert_text = Mock(return_value=True)
        self.worker.log = Mock()
        self.worker.queue = [({"msgid": "message", "text": "hello"}, self.spool.stat().st_size)]
        self.warm = Mock()
        self.warm.call_json.return_value = {"chat_events": []}
        self.worker.gw = self.warm
        patcher = patch.dict(os.environ, {"MUSE_CF_RELAY_CONFIG": "/private/relay.json"})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fallback = patch.object(self.bridge, "fallback_to_inbox", Mock())
        self.fallback_mock = self.fallback.start()
        self.addCleanup(self.fallback.stop)

    def test_warm_socket_checks_history_before_opening_one_stream(self):
        def opened(*args, **kwargs):
            self.warm.call_json.assert_called_once()
            self.assertIsNotNone(self.worker.cs()["submission_pending"])
        self.warm._open.side_effect = opened
        with patch.object(self.bridge, "connect") as connect:
            self.worker._start_next()
        connect.assert_not_called()
        self.warm.close.assert_not_called()
        self.warm._open.assert_called_once()
        self.assertIsNone(self.worker.cs()["submission_pending"])

    def test_dead_warm_socket_reconnects_once_before_reservation(self):
        self.warm.call_json.side_effect = ConnectionError("socket closed")
        fresh = Mock()
        fresh.call_json.return_value = {"chat_events": []}
        with patch.object(self.bridge, "connect", return_value=fresh) as connect:
            self.worker._start_next()
        self.warm.close.assert_called_once()
        self.warm._open.assert_not_called()
        connect.assert_called_once()
        fresh._open.assert_called_once()
        self.fallback_mock.assert_not_called()

    def test_both_preflight_reads_fail_without_sending(self):
        self.warm.call_json.side_effect = ConnectionError("socket closed")
        fresh = Mock()
        fresh.call_json.side_effect = ConnectionError("still offline")
        with patch.object(self.bridge, "connect", return_value=fresh) as connect:
            self.worker._start_next()
        connect.assert_called_once()
        self.warm._open.assert_not_called()
        fresh._open.assert_not_called()
        self.fallback_mock.assert_called_once()

    def test_unknown_warm_send_never_reconnects_or_replays(self):
        self.warm._open.side_effect = TimeoutError("submission unknown")
        with patch.object(self.bridge, "connect") as connect:
            self.worker._start_next()
            self.worker._start_next()
        connect.assert_not_called()
        self.warm._open.assert_called_once()
        self.assertIsNotNone(self.worker.cs()["submission_pending"])
        self.fallback_mock.assert_not_called()

    def test_reservation_write_failure_does_not_retry(self):
        original = self.worker.update_cs
        def failed(**values):
            if values.get("submission_pending"):
                raise OSError("storage unavailable")
            return original(**values)
        self.worker.update_cs = failed
        with patch.object(self.bridge, "connect") as connect:
            with self.assertRaises(self.bridge.SubmissionPersistenceError):
                self.worker._start_next()
        connect.assert_not_called()
        self.warm._open.assert_not_called()
        self.fallback_mock.assert_not_called()

    def test_completed_cf_turn_retains_the_socket_for_next_turn(self):
        self.worker.queue = []
        self.worker.read_offset = self.spool.stat().st_size
        self.worker.update_cs(turns=[{"msgid": "old", "lane": "main", "sent_at": 1}])
        self.worker.poll_turn = Mock(return_value="done")
        self.worker.step(str(self.spool), None)
        self.warm.close.assert_not_called()
        self.assertIs(self.worker.gw, self.warm)
        self.assertEqual(self.worker.cs()["turns"], [])

    def test_other_transports_keep_the_fresh_connection_policy(self):
        self.worker.ch = "wecom"
        self.assertFalse(self.worker._reuse_cf_connection())
        self.worker.ch = "weixin"
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(self.worker._reuse_cf_connection())
