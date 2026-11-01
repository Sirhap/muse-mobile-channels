"""Deterministic core rejections must not wedge a whole iLink poll batch."""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mobile_channels.gateway import GatewayBinding
from mobile_channels.storage import Store

ROOT = Path(__file__).resolve().parents[1]


class CoreIngressRejectionTests(unittest.IsolatedAsyncioTestCase):
    """Run the actual batch and callback with a real isolated core database."""

    def setUp(self):
        """Create independent transport state, mode and core authority."""
        self.temp = tempfile.TemporaryDirectory(prefix='core-ingress-rejection-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.database = self.base / 'core.sqlite3'
        with Store(self.database):
            pass
        mode = self.base / 'modes.json'
        mode.write_text(json.dumps({'channels': {'weixin': {
            'mode': 'core', 'database': str(self.database), 'account_id': 'bot',
            'allowed_senders': ['alice'],
        }}}))
        environment = patch.dict('os.environ', {'MUSE_CHANNEL_MODES': str(mode)})
        environment.start()
        self.addCleanup(environment.stop)
        spec = importlib.util.spec_from_file_location('core_rejection_weixin', ROOT / 'weixin-bot/gateway.py')
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        for name, filename in {'STATE': '.', 'SYNC_FILE': 'sync.json', 'POISON_MESSAGES': 'poison.jsonl',
                               'CONTEXT_FILE': 'context.json'}.items():
            patcher = patch.object(self.module, name, self.base / filename)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.gateway = self.module.Gateway.__new__(self.module.Gateway)
        self.gateway._core_binding = GatewayBinding('weixin')
        self.addCleanup(self.gateway._core_binding.close)
        self.gateway.context = {}
        self.gateway.feedback_track = {}
        self.gateway.seen_msgids = set()
        self.gateway.sync_buf = 'old'
        self.gateway.msgs_received = 0
        self.gateway.write_status = Mock()
        self.module.SYNC_FILE.write_text(json.dumps({'buf': 'old'}))

    @staticmethod
    def message(identifier='valid'):
        """Construct a real protocol-shaped owner text message."""
        return {'message_id': identifier, 'message_type': 1, 'from_user_id': 'alice',
                'context_token': 'private-token',
                'item_list': [{'type': 1, 'text_item': {'text': 'private question'}}]}

    async def batch(self, messages):
        """Drive the real checkpoint boundary without network activity."""
        await self.gateway._process_update_batch(None, {'user_id': 'alice'},
            {'msgs': messages, 'get_updates_buf': 'new'})

    def task_count(self):
        """Read committed task count independently of gateway memory."""
        with sqlite3.connect(self.database) as connection:
            return connection.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]

    async def test_missing_id_is_recorded_before_following_valid_message(self):
        """A deterministic identity error cannot starve later valid work."""
        invalid = self.message()
        del invalid['message_id']
        with patch.object(self.module, 'log') as log:
            await self.batch([invalid, self.message()])
        self.assertEqual(self.task_count(), 1)
        self.assertEqual(self.gateway.sync_buf, 'new')
        self.assertEqual(len(self.module.POISON_MESSAGES.read_text().splitlines()), 1)
        logged = ' '.join(str(call) for call in log.call_args_list)
        self.assertNotIn('private question', logged)
        self.assertNotIn('private-token', logged)

    async def test_unauthorized_core_sender_is_reliably_recorded(self):
        """A narrower core allowlist rejects the owner without holding the poll."""
        self.gateway._core_binding.settings['allowed_senders'] = ['bob']
        await self.batch([self.message('denied')])
        self.assertEqual(self.task_count(), 0)
        self.assertEqual(self.gateway.sync_buf, 'new')
        self.assertTrue(self.module.POISON_MESSAGES.exists())
        self.gateway._core_binding.settings['allowed_senders'] = ['alice']
        await self.batch([self.message('valid')])
        self.assertEqual(self.task_count(), 1)

    async def test_unsupported_group_rejection_does_not_block_private_message(self):
        """Explicitly unsupported routing is input rejection, not a storage retry."""
        group = dict(self.message('group'), group_id='room')
        await self.batch([group, self.message()])
        self.assertEqual(self.task_count(), 1)
        self.assertEqual(self.gateway.sync_buf, 'new')

    async def test_quarantine_failure_preserves_checkpoint_and_replay(self):
        """Rejected input may be skipped only after its evidence is durable."""
        invalid = self.message(None)
        with patch.object(self.gateway, 'append_jsonl', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                await self.batch([invalid, self.message()])
        self.assertEqual(self.gateway.sync_buf, 'old')
        self.assertEqual(self.task_count(), 0)
        await self.batch([invalid, self.message()])
        self.assertEqual(self.task_count(), 1)

    async def test_storage_errors_are_not_reclassified_as_bad_input(self):
        """Even ValueError from storage remains retryable rather than quarantined."""
        for error in (OSError('disk full'), sqlite3.OperationalError('database locked'), ValueError('storage contract error')):
            with self.subTest(error=type(error).__name__):
                with patch.object(Store, 'ingest', side_effect=error):
                    with self.assertRaises(type(error)):
                        await self.batch([self.message()])
                self.assertEqual(self.gateway.sync_buf, 'old')
                self.assertEqual(self.task_count(), 0)
                self.assertFalse(self.module.POISON_MESSAGES.exists())
        await self.batch([self.message()])
        self.assertEqual(self.task_count(), 1)


if __name__ == '__main__':
    unittest.main()
