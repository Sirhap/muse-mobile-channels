"""A started stream is never retried through another execution path."""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'


class SubmissionRecoveryTests(unittest.TestCase):
    """Drive real submission boundaries with injected transport/storage faults."""

    def setUp(self):
        spec = importlib.util.spec_from_file_location('submission_bridge', SOURCE)
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bridge.BASE = str(self.root)
        self.bridge.STATE_F = str(self.root / 'state.json')
        self.bridge.CFG = {'channels': {'test': {}}}
        self.bridge.cancelled_ids = lambda channel: set()
        self.bridge.read_signal_ts = lambda path: None
        self.spool = self.root / 'spool.jsonl'
        self.spool.write_bytes(b'{"msgid":"m1","text":"execute"}\n')
        self.gateway = Mock()
        self.worker = self.new_worker()
        self.worker.queue = [({'msgid': 'm1', 'text': 'execute', 'from_user': 'alice'}, self.spool.stat().st_size)]
        self.fallback = patch.object(self.bridge, 'fallback_to_inbox', Mock())
        self.fallback_mock = self.fallback.start()
        self.addCleanup(self.fallback.stop)

    def new_worker(self):
        worker = self.bridge.ChannelWorker('test')
        worker.ensure_gw = lambda: (setattr(worker, 'gw', self.gateway) or True)
        worker.session_for = lambda cs: ('sid', '')
        worker.history_events = lambda sid, limit: []
        worker.idle_push_scan = Mock()
        worker._send_alert_text = Mock(return_value=True)
        return worker

    def test_stream_accepted_then_timeout_never_falls_back_or_replays(self):
        self.gateway._open.side_effect = TimeoutError('accepted but response lost')
        self.worker._start_next()
        self.fallback_mock.assert_not_called()
        self.assertIsNotNone(self.worker.cs().get('submission_pending'))
        restarted = self.new_worker()
        restarted.step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)
        self.fallback_mock.assert_not_called()
        self.assertEqual(restarted.cs()['spool_offset'], self.spool.stat().st_size)

    def test_post_stream_save_failure_leaves_durable_intent(self):
        original = self.worker.update_cs
        def failing(**changes):
            if changes.get('submission_pending', 'missing') is None:
                raise OSError('failed confirmation save')
            return original(**changes)
        self.worker.update_cs = failing
        self.worker._start_next()
        self.assertEqual(self.gateway._open.call_count, 1)
        self.fallback_mock.assert_not_called()
        self.assertIsNotNone(self.worker.cs().get('submission_pending'))
        self.new_worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)

    def test_intent_save_failure_prevents_external_submission(self):
        with patch.object(self.worker, 'update_cs', side_effect=OSError('disk full')):
            with self.assertRaises(self.bridge.SubmissionPersistenceError):
                self.worker._start_next()
        self.gateway._open.assert_not_called()
        self.fallback_mock.assert_not_called()
        self.assertEqual(len(self.worker.queue), 1)

    def test_confirmed_turn_and_spool_are_committed_together(self):
        self.worker._start_next()
        state = self.worker.cs()
        self.assertIsNone(state.get('submission_pending'))
        self.assertEqual(state['turns'][0]['msgid'], 'm1')
        self.assertEqual(state['spool_offset'], self.spool.stat().st_size)
        self.assertEqual(self.worker.queue, [])

    def test_preparation_failure_can_use_fallback(self):
        self.worker.history_events = Mock(side_effect=ConnectionError('not submitted'))
        self.worker._start_next()
        self.gateway._open.assert_not_called()
        self.fallback_mock.assert_called_once()

    def test_corrupt_existing_state_does_not_replay_spool(self):
        """Lost intent evidence must never be interpreted as an empty queue."""
        Path(self.bridge.STATE_F).write_text('{broken')
        with self.assertRaises(ValueError):
            self.new_worker().step(str(self.spool), None)
        self.gateway._open.assert_not_called()
        self.fallback_mock.assert_not_called()

    def test_unknown_is_not_cleared_by_cancel_or_new_boundary(self):
        """Routing controls cannot prove that the original execution stopped."""
        self.gateway._open.side_effect = TimeoutError('unknown')
        self.worker._start_next()
        self.bridge.cancelled_ids = lambda channel: {'m1'}
        self.bridge.read_signal_ts = lambda path: 9999999999
        restarted = self.new_worker()
        restarted.step(str(self.spool), 'boundary')
        self.assertIsNotNone(restarted.cs()['submission_pending'])
        self.assertEqual(self.gateway._open.call_count, 1)
        restarted.idle_push_scan.assert_not_called()

    def test_merge_timeout_preserves_reserved_row_and_existing_turn(self):
        """Legacy merge retries must not append a message twice."""
        turn = {'msgid': 'old', 'ids': ['old'], 'session_id': 'sid', 'lane': 'main'}
        self.worker.update_cs(turns=[turn])
        self.gateway._open.side_effect = TimeoutError('merge accepted, response lost')
        self.worker._merge_into_active()
        self.fallback_mock.assert_not_called()
        state = self.worker.cs()
        self.assertEqual(state['submission_pending']['turn']['ids'], ['old', 'm1'])
        self.new_worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)

    def test_process_exit_after_intent_before_send_keeps_session_reserved(self):
        """Even an unsent reservation cannot be assumed safe after a crash."""
        class ProcessExit(BaseException):
            """Represent termination outside normal exception handling."""
        self.gateway._open.side_effect = ProcessExit()
        with self.assertRaises(ProcessExit):
            self.worker._start_next()
        self.assertIsNotNone(self.worker.cs()['submission_pending'])
        self.new_worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)
        self.fallback_mock.assert_not_called()

    def test_merge_confirmation_failure_does_not_repeat_send(self):
        """The persisted reservation survives a lost merge confirmation write."""
        self.worker.update_cs(turns=[{'msgid': 'old', 'ids': ['old'], 'session_id': 'sid'}])
        original = self.worker.update_cs
        def fail_confirmation(**changes):
            if changes.get('submission_pending', 'absent') is None:
                raise OSError('merge confirmation fault')
            return original(**changes)
        self.worker.update_cs = fail_confirmation
        self.worker._merge_into_active()
        self.assertIsNotNone(self.worker.cs()['submission_pending'])
        self.new_worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)

    def test_directory_sync_failure_after_rename_cannot_trigger_send(self):
        """An unconfirmed directory commit still leaves conservative evidence."""
        original_fsync = self.bridge.os.fsync
        count = 0
        def fail_directory(descriptor):
            nonlocal count
            count += 1
            if count == 2:
                raise OSError('directory sync fault')
            return original_fsync(descriptor)
        with patch.object(self.bridge.os, 'fsync', side_effect=fail_directory):
            self.worker._start_next()
        self.gateway._open.assert_not_called()
        self.fallback_mock.assert_not_called()
        self.assertIsNotNone(self.worker.cs()['submission_pending'])
        self.new_worker().step(str(self.spool), None)
        self.gateway._open.assert_not_called()

    def test_pending_summary_does_not_appear_idle(self):
        """Both persisted queue diagnostics and heartbeat report uncertainty."""
        self.gateway._open.side_effect = TimeoutError('unknown')
        self.worker._start_next()
        self.assertEqual(self.worker.cs()['queue_snapshot']['active'][0]['phase'], 'submission_unknown')
        self.assertEqual(self.worker._active_summary()[0]['phase'], 'submission_unknown')

    def test_failed_cancel_does_not_report_cancelled(self):
        self.bridge.cancelled_ids = lambda channel: {'m1'}
        self.worker.gw = self.gateway
        self.gateway.call_json.side_effect = TimeoutError('cancel unknown')
        with self.assertRaises(TimeoutError):
            self.worker.poll_turn({'msgid': 'm1', 'ids': ['m1'], 'session_id': 'sid'})


if __name__ == '__main__':
    unittest.main()
