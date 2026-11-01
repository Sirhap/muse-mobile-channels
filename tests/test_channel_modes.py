"""Mode transitions cannot run concurrently with participating channel writers."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from mobile_channels.modes import ChannelLease, ModeError, write_modes


class ChannelModeTests(unittest.TestCase):
    """Use real flock descriptors rather than mocking the cutover invariant."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'modes.json'
        self.legacy = {'channels': {'weixin': {'mode': 'legacy'}}}
        write_modes(self.path, self.legacy)

    def test_running_writer_blocks_cutover(self):
        with ChannelLease(self.path, 'weixin', 'legacy'):
            with self.assertRaises(ModeError):
                write_modes(self.path, self.legacy)
        write_modes(self.path, self.legacy)

    def test_core_blocks_legacy_writer(self):
        core = {'channels': {'weixin': {'mode': 'core', 'database': '/tmp/example.sqlite3',
                                       'account_id': 'bot', 'allowed_senders': ['alice']}}}
        write_modes(self.path, core)
        with self.assertRaises(ModeError):
            ChannelLease(self.path, 'weixin', 'legacy')
        with ChannelLease(self.path, 'weixin') as lease:
            self.assertEqual(lease.mode, 'core')

    def test_core_mode_prevents_real_legacy_hooks_and_cli_writes(self):
        """Entrypoints exit before loading hook runtime or touching outbox files."""
        root = Path(__file__).resolve().parents[1]
        core = {'mode': 'core', 'database': str(Path(self.temp.name) / 'core.sqlite3'),
                'account_id': 'bot', 'allowed_senders': ['alice']}
        write_modes(self.path, {'channels': {'weixin': dict(core), 'wecom': dict(core)}})
        environment = dict(os.environ, MUSE_CHANNEL_MODES=str(self.path), PYTHONPATH=str(root))
        environment.pop('HATCH_HOOK_RUNTIME', None)
        environment.pop('MUSE_HOOK_MODE_GUARDED', None)
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                result = subprocess.run(['bash', str(root / f'hooks/scripts/{channel}-inbox.sh')],
                                        env=environment, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                self.assertEqual(result.stdout, b'')
                result = subprocess.run([sys.executable, str(root / f'{channel}-bot/{channel}_cli.py'),
                                         'send', '--text', 'must not send'],
                                        env=environment, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 2, result.stderr.decode())
                self.assertIn(b'legacy command disabled', result.stderr)

    def test_invalid_configuration_fails_closed_and_releases_lock(self):
        self.path.write_text('{broken')
        with self.assertRaises(ValueError):
            ChannelLease(self.path, 'weixin')
        write_modes(self.path, self.legacy)


if __name__ == '__main__':
    unittest.main()
