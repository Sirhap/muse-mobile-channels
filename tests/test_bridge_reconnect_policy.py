"""Connection handshakes must not hide repeated failed bridge operations."""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'


class ReconnectPolicyTests(unittest.TestCase):
    """Exercise the actual acquisition and polling failure bookkeeping."""

    def setUp(self):
        spec = importlib.util.spec_from_file_location('reconnect_policy', SOURCE)
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bridge.BASE = self.temp.name
        self.bridge.STATE_F = str(Path(self.temp.name) / 'state.json')
        self.worker = self.bridge.ChannelWorker('test')
        self.worker._send_alert_text = Mock(return_value=True)
        self.now = 1000
        self.time = patch.object(self.bridge.time, 'time', side_effect=lambda: self.now)
        self.time.start()
        self.addCleanup(self.time.stop)
        self.stamp = patch.object(self.bridge, 'cred_stamp', return_value='unchanged')
        self.stamp.start()
        self.addCleanup(self.stamp.stop)

    def test_failed_connect_waits_before_retry(self):
        with patch.object(self.bridge, 'connect', side_effect=ConnectionError('offline')) as connect:
            self.assertFalse(self.worker.ensure_gw())
            self.assertFalse(self.worker.ensure_gw())
        self.assertEqual(connect.call_count, 1)

    def test_handshake_success_does_not_reset_operation_failure_limit(self):
        with patch.object(self.bridge, 'connect', side_effect=lambda: Mock()) as connect:
            for _ in range(3):
                self.assertTrue(self.worker.ensure_gw())
                self.worker._note_reconnect_failure(ConnectionError('history offline'))
                self.worker.drop_gw()
                self.now += 100
            self.assertFalse(self.worker.ensure_gw())
        self.assertEqual(connect.call_count, 3)
        self.assertTrue(self.worker.cs()['reconnect_exhausted'])

    def test_actual_step_poll_failure_preserves_submitted_turn(self):
        spool = Path(self.temp.name) / 'spool.jsonl'
        spool.touch()
        turn = {'msgid': 'submitted', 'lane': 'main', 'sent_at': 1}
        self.worker.update_cs(turns=[turn])
        self.worker.poll_turn = Mock(side_effect=ConnectionError('poll disconnected'))
        with patch.object(self.bridge, 'connect', side_effect=lambda: Mock()) as connect:
            for _ in range(4):
                self.worker.step(str(spool), None)
                self.now += 100
        self.assertEqual(connect.call_count, 3)
        self.assertEqual(self.worker.poll_turn.call_count, 3)
        # The turn is preserved, and a poll failure marks it reply_started
        # (fail safe: unknown means do not fold into it). Regression for
        # the parallel-review minor 4.
        self.assertEqual(self.worker.cs()['turns'],
                         [dict(turn, reply_started=True)])
        self.worker._send_alert_text.assert_called_once()
        self.assertEqual(self.worker._send_alert_text.call_args.args[1],
                         dict(turn, reply_started=True))
        self.assertTrue(self.worker.cs()['reconnect_exhausted'])

    def test_real_idle_success_clears_failure_budget(self):
        self.worker.update_cs(session_id='existing-run')
        self.worker._note_reconnect_failure(ConnectionError('offline'))
        self.now += 100
        self.worker.history_events = Mock(return_value=[])
        with patch.object(self.bridge, 'connect', return_value=Mock()), patch.object(self.bridge, 'longtask_mode', return_value=False):
            self.worker.idle_push_scan(self.worker.cs())
        self.assertEqual(self.worker._reconnect_failures, 0)

    def test_actual_idle_rpc_failure_parks_even_when_handshakes_work(self):
        self.worker.update_cs(session_id='existing-run')
        self.worker.history_events = Mock(side_effect=ConnectionError('history offline'))
        with patch.object(self.bridge, 'connect', side_effect=lambda: Mock()) as connect, patch.object(self.bridge, 'longtask_mode', return_value=False):
            for _ in range(4):
                self.worker.idle_push_scan(self.worker.cs())
                self.now += 100
        self.assertEqual(connect.call_count, 3)
        self.assertEqual(self.worker.history_events.call_count, 3)
        self.assertTrue(self.worker.cs()['reconnect_exhausted'])

    def test_failed_pause_notice_retries_without_reconnecting(self):
        self.worker._send_alert_text = Mock(side_effect=[False, True])
        with patch.object(self.bridge, 'connect', side_effect=ConnectionError('offline')) as connect:
            for _ in range(3):
                self.worker.ensure_gw()
                self.now += 100
            self.worker.ensure_gw()
            self.worker.ensure_gw()
        self.assertEqual(connect.call_count, 3)
        self.assertEqual(self.worker._send_alert_text.call_count, 2)

    def test_credential_change_releases_park(self):
        with patch.object(self.bridge, 'connect', side_effect=ConnectionError('offline')):
            for _ in range(3):
                self.worker.ensure_gw()
                self.now += 100
        with patch.object(self.bridge, 'cred_stamp', return_value='new'), patch.object(self.bridge, 'connect', return_value=Mock()) as connect:
            self.assertTrue(self.worker.ensure_gw())
            connect.assert_called_once()

    def test_idle_failure_notice_is_not_repeated_hourly(self):
        self.worker._send_alert_text = Mock(return_value=True)
        for _ in range(8):
            self.worker._note_idle_scan_failure(self.worker.cs(), ConnectionError('offline'))
            self.now += 4000
        self.assertEqual(self.worker._send_alert_text.call_count, 1)

    def test_fallback_notice_once_until_actual_recovery(self):
        self.worker._send_fb_alert = Mock(return_value=True)
        for index in range(8):
            self.worker.note_fallback({'msgid': str(index)}, 1)
            self.now += 4000
        self.assertEqual(self.worker._send_fb_alert.call_count, 1)
        self.worker._note_reconnect_success()
        for index in range(3):
            self.worker.note_fallback({'msgid': str(index)}, 1)
        self.assertEqual(self.worker._send_fb_alert.call_count, 2)

    def test_failed_notice_can_retry_without_claiming_notified(self):
        self.worker._send_fb_alert = Mock(side_effect=[False, True])
        for index in range(4):
            self.worker.note_fallback({'msgid': str(index)}, 1)
        self.assertEqual(self.worker._send_fb_alert.call_count, 2)

    def test_idle_and_fallback_share_outage_notice(self):
        self.worker._send_alert_text = Mock(return_value=True)
        for index in range(3):
            self.worker.note_fallback({'msgid': str(index)}, 1)
        for _ in range(4):
            self.worker._note_idle_scan_failure(self.worker.cs(), ConnectionError('offline'))
        self.assertEqual(self.worker._send_alert_text.call_count, 1)

    def test_successful_operation_clears_retry_budget(self):
        self.worker._note_reconnect_failure(ConnectionError('offline'))
        self.worker._note_reconnect_success()
        self.assertEqual(self.worker._reconnect_failures, 0)
        with patch.object(self.bridge, 'connect', return_value=Mock()):
            self.assertTrue(self.worker.ensure_gw())


if __name__ == '__main__':
    unittest.main()
