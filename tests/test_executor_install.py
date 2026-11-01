"""Installed core resolves its separately provided transport before consuming work."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels import executors


class ExecutorInstallTests(unittest.TestCase):
    """Exercise source lookup without credentials, imports or network operations."""

    def setUp(self):
        """Create an isolated installation layout."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_wheel_without_external_transport_fails_before_loading(self):
        """A package-only layout gives an actionable startup error."""
        with patch.dict('os.environ', {}, clear=True), patch.object(executors, '__file__', str(self.root / 'site-packages/mobile_channels/executors.py')):
            with self.assertRaisesRegex(FileNotFoundError, 'MUSE_BRIDGE_SOURCE'):
                executors.validate_transport_source()

    def test_explicit_path_resolves_without_import_side_effects(self):
        """Validation does not execute a configured transport module."""
        path = self.root / 'transport.py'
        path.write_text("raise AssertionError('must not import at validation')")
        with patch.dict('os.environ', {'MUSE_BRIDGE_SOURCE': str(path)}, clear=True):
            self.assertEqual(executors.validate_transport_source(), path.resolve())

    def test_workspace_transport_is_used(self):
        """A deployed workspace can supply the existing bridge source."""
        path = self.root / 'native-bridge/native_bridge.py'
        path.parent.mkdir()
        path.write_text('')
        with patch.dict('os.environ', {'MUSE_WORKSPACE': str(self.root)}, clear=True):
            self.assertEqual(executors.validate_transport_source(), path.resolve())

    def test_invalid_explicit_path_does_not_fall_back(self):
        """A typo cannot silently execute the checkout's different transport."""
        with patch.dict('os.environ', {'MUSE_BRIDGE_SOURCE': str(self.root / 'missing.py')}, clear=True):
            with self.assertRaises(FileNotFoundError):
                executors.validate_transport_source()


class ExecutorOwnershipTests(unittest.TestCase):
    """Slow setup cannot let an expired or cancelled executor submit new work."""

    def run_scenario(self, setup_effect):
        """Drive the actual CoreRunner and MuseExecutor with a delayed session start."""
        from mobile_channels.models import InboundMessage
        from mobile_channels.runtime import CoreRunner
        from mobile_channels.storage import Store
        with tempfile.TemporaryDirectory() as directory:
            clock = [100.0]
            with Store(Path(directory) / 'core.sqlite3', clock=lambda: clock[0]) as store:
                item = store.ingest(InboundMessage('wecom', 'bot', 'single', 'alice',
                                                   'message', 'alice', 'perform work'))
                calls = []

                class Client:
                    """A local fake that delays setup but never performs network IO."""

                    def call_json(self, method, **kwargs):
                        setup_effect(store, item, clock)
                        return {'session_id': 'session'}

                    def _open(self, *args, **kwargs):
                        calls.append('stream')
                        return {'run_id': 'run'}

                    def close(self):
                        return None

                runner = CoreRunner(store, executors.MuseExecutor(store, Client),
                                    'worker', lease_seconds=60)
                state = runner.tick()
                return calls, store.task(item.task_id), state

    def test_expired_owner_cannot_open_stream_after_slow_session_creation(self):
        """Fencing applies before submission, not only when saving its result."""
        def expire(store, item, clock):
            clock[0] += 100
        calls, task, state = self.run_scenario(expire)
        self.assertEqual(calls, [])
        self.assertEqual(task['state'], 'reconciling')
        self.assertEqual(state, 'unknown')

    def test_cancellation_during_session_creation_does_not_submit(self):
        """A new cancel request wins before any user task is sent externally."""
        def cancel(store, item, clock):
            store.request_cancel(item.task_id, item.conversation_id)
        calls, task, state = self.run_scenario(cancel)
        self.assertEqual(calls, [])
        self.assertEqual(task['state'], 'cancelled')
        self.assertEqual(state, 'not_submitted')


if __name__ == '__main__':
    unittest.main()
