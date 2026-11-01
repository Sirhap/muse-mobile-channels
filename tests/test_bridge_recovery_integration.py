"""Stopped-bridge resolutions survive daemon restart without duplicate execution."""
import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mobile_channels.recovery import bridge_lock, inspect_pending, resolve_pending

SOURCE = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'


class StopDaemon(BaseException):
    """Leave the infinite supervisor loop without swallowing its resource cleanup."""


class BridgeRecoveryIntegrationTests(unittest.TestCase):
    """Drive actual bridge queue methods, state files, recovery tools and locks."""

    def setUp(self):
        spec = importlib.util.spec_from_file_location('bridge_recovery_integration', SOURCE)
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.bridge.BASE = str(self.base)
        self.bridge.STATE_F = str(self.base / 'state.json')
        self.bridge.STATUS_F = str(self.base / 'status.json')
        self.bridge.CFG = {'channels': {'test': {}}}
        self.bridge.cancelled_ids = lambda channel: set()
        self.bridge.read_signal_ts = lambda path: None
        self.spool = self.base / 'spool.jsonl'
        self.row = {'msgid': 'm', 'text': 'work', 'from_user': 'original-user'}
        self.spool.write_text(json.dumps(self.row) + '\n')
        self.turn = {'msgid': 'm', 'ids': ['m'], 'session_id': 'sid', 'baseline': 2,
                     'lane': 'main', 'sent_at': int(time.time())}
        pending = {'row': self.row, 'turn': self.turn, 'kind': 'turn'}
        self.bridge.save_state({'channels': {'test': {'turns': [], 'submission_pending': pending,
                                                     'spool_offset': self.spool.stat().st_size}}})
        self.gateway = Mock()

    def resolve(self, outcome):
        """Use the current exact operator evidence to resolve the original intent."""
        digest = inspect_pending(self.base, 'test')['digest']
        return resolve_pending(self.base, 'test', msgid='m', session_id='sid', digest=digest,
                               outcome=outcome, evidence='original platform request inspected')

    def worker(self):
        """Replace network only while retaining actual queue and submission code."""
        worker = self.bridge.ChannelWorker('test')
        worker.ensure_gw = lambda: (setattr(worker, 'gw', self.gateway) or True)
        worker.session_for = lambda cs: ('sid', '')
        worker.history_events = lambda sid, limit: []
        worker.poll_turn = Mock(return_value='running')
        worker.idle_push_scan = Mock()
        worker._send_alert_text = Mock(return_value=True)
        return worker

    def test_accepted_resolution_polls_original_without_submission(self):
        self.resolve('accepted')
        worker = self.worker()
        worker.step(str(self.spool), None)
        worker.poll_turn.assert_called_once()
        self.gateway._open.assert_not_called()
        self.assertEqual(worker.cs()['turns'][0]['session_id'], 'sid')

    def test_not_submitted_resolution_restarts_once_without_offset_rewind(self):
        before = self.spool.stat().st_size
        self.resolve('not_submitted')
        worker = self.worker()
        worker.step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)
        self.assertEqual(worker.cs()['recovery_queue'], [])
        self.assertEqual(worker.cs()['spool_offset'], before)
        self.worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)

    def test_recovery_reservation_failure_cannot_lose_row(self):
        self.resolve('not_submitted')
        worker = self.worker()
        original = worker.update_cs

        def failed_reservation(**changes):
            if changes.get('submission_pending'):
                raise OSError('disk failure')
            return original(**changes)

        worker.update_cs = failed_reservation
        with self.assertRaises(self.bridge.SubmissionPersistenceError):
            worker.step(str(self.spool), None)
        self.assertEqual(len(worker.cs()['recovery_queue']), 1)
        self.gateway._open.assert_not_called()
        self.worker().step(str(self.spool), None)
        self.assertEqual(self.gateway._open.call_count, 1)

    def test_cancel_recovered_queue_removes_persisted_row(self):
        self.resolve('not_submitted')
        self.bridge.cancelled_ids = lambda channel: {'m'}
        worker = self.worker()
        worker.step(str(self.spool), None)
        self.assertEqual(worker.cs()['recovery_queue'], [])
        self.assertEqual(worker.queue, [])
        self.gateway._open.assert_not_called()
        self.assertEqual(worker.cs()['spool_offset'], self.spool.stat().st_size)

    def test_fallback_recovery_write_failure_replays_only_idempotent_handoff(self):
        self.resolve('not_submitted')
        worker = self.worker()
        worker.ensure_gw = lambda: False
        with patch.object(worker, 'note_fallback', side_effect=OSError('state failure')):
            with self.assertRaises(OSError):
                worker.step(str(self.spool), None)
        self.assertEqual(len(worker.cs()['recovery_queue']), 1)
        restarted = self.worker()
        restarted.ensure_gw = lambda: False
        restarted.step(str(self.spool), None)
        rows = (self.base / 'shadow/test-inbox.jsonl').read_text().splitlines()
        self.assertEqual(len(rows), 1)
        self.assertEqual(restarted.cs()['recovery_queue'], [])

    def test_fallback_uses_original_route_and_not_configured_owner(self):
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                directory = self.base / channel
                directory.mkdir()
                self.bridge.CFG['channels'][channel] = {'bot_state': str(directory),
                    'from_user_id': 'configured-owner', 'chatid': 'configured-chat',
                    'from_userid': 'configured-owner'}
                (self.base / f'enabled-{channel}').touch()
                row = {'msgid': 'm', 'text': 'original', 'from_user': 'original-user',
                       'from_userid': 'original-user', 'chatid': 'original-group', 'chattype': 'group'}
                self.bridge.fallback_to_inbox(channel, row, 'confirmed not submitted')
                self.bridge.fallback_to_inbox(channel, row, 'same handoff retried')
                rows = [json.loads(line) for line in (directory / 'inbox.jsonl').read_text().splitlines()]
                self.assertEqual(len(rows), 1)
                if channel == 'weixin':
                    self.assertEqual(rows[0]['from_user_id'], 'original-user')
                else:
                    self.assertEqual(rows[0]['chatid'], 'original-group')
                    self.assertEqual(rows[0]['chattype'], 'group')

    def test_daemon_owns_state_lock_and_releases_it_on_exit(self):
        workers = []
        def worker(channel):
            item = Mock()
            workers.append(channel)
            return item
        def sleep(_seconds):
            with self.assertRaises(RuntimeError):
                with bridge_lock(self.base):
                    pass
            raise StopDaemon()
        with patch.object(self.bridge, 'ChannelWorker', worker), patch.object(self.bridge.time, 'sleep', sleep):
            with self.assertRaises(StopDaemon):
                self.bridge.cmd_daemon()
        self.assertEqual(workers, ['test'])
        with bridge_lock(self.base):
            pass

    def test_daemon_skips_core_channel_under_lifetime_mode_lease(self):
        self.bridge.CFG = {'channels': {'weixin': {}}}
        mode = self.base / 'modes.json'
        mode.write_text(json.dumps({'channels': {'weixin': {'mode': 'core', 'database': '/tmp/core.db',
                                  'account_id': 'bot', 'allowed_senders': ['alice']}}}))
        worker = Mock()
        with patch.dict('os.environ', {'MUSE_CHANNEL_MODES': str(mode)}), patch.object(
                self.bridge, 'ChannelWorker', worker), patch.object(
                self.bridge.time, 'sleep', side_effect=StopDaemon()):
            with self.assertRaises(StopDaemon):
                self.bridge.cmd_daemon()
        worker.assert_not_called()


if __name__ == '__main__':
    unittest.main()
