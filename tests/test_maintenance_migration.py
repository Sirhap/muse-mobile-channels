"""Backups capture WAL state; migration never schedules unexplained old work."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.maintenance import backup_database, check_database, restore_database
from mobile_channels.migration import migrate_snapshot
from mobile_channels.models import InboundMessage
from mobile_channels.storage import Store


class MaintenanceTests(unittest.TestCase):
    """Use real SQLite files, committed WAL transactions and invalid snapshots."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'live.sqlite3'
        self.store = Store(self.source)
        self.addCleanup(self.store.close)
        self.item = self.store.ingest(InboundMessage('weixin', 'bot', 'single', 'alice', 'm1', 'alice', 'hello'))

    def test_backup_and_restore_preserve_committed_wal(self):
        backup = self.root / 'backup.sqlite3'
        backup_database(self.source, backup)
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        restored = self.root / 'restored.sqlite3'
        restore_database(backup, restored)
        with Store(restored) as result:
            self.assertEqual(result.task(self.item.task_id)['state'], 'queued')
        self.assertEqual(check_database(restored)['integrity'], 'ok')

    def test_existing_target_and_source_cannot_be_overwritten(self):
        for target in (self.source, self.root / 'important.txt'):
            if target != self.source:
                target.write_bytes(b'keep')
            before = target.read_bytes()
            with self.assertRaises(FileExistsError):
                backup_database(self.source, target)
            self.assertEqual(target.read_bytes(), before)

    def test_invalid_source_or_restore_does_not_publish_output(self):
        broken = self.root / 'broken.sqlite3'
        broken.write_bytes(b'not sqlite')
        target = self.root / 'restored.sqlite3'
        with self.assertRaises(sqlite3.DatabaseError):
            restore_database(broken, target)
        self.assertFalse(target.exists())
        with self.assertRaises(sqlite3.OperationalError):
            backup_database(self.root / 'missing.sqlite3', target)
        self.assertFalse(target.exists())
        self.assertFalse(list(self.root.glob('.sqlite-backup-*')))

    def test_publish_race_cannot_replace_newly_created_target(self):
        target = self.root / 'backup.sqlite3'
        original_link = __import__('os').link

        def raced_link(source, destination):
            target.write_bytes(b'another writer')
            return original_link(source, destination)

        with patch('mobile_channels.maintenance.os.link', side_effect=raced_link):
            with self.assertRaises(FileExistsError):
                backup_database(self.source, target)
        self.assertEqual(target.read_bytes(), b'another writer')


class MigrationTests(unittest.TestCase):
    """Inspect migration data and preserved evidence rather than mirroring loops."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / 'snapshot'
        self.inbox = self.workspace / 'weixin-bot/state/inbox.jsonl'
        self.inbox.parent.mkdir(parents=True)
        self.destination = self.root / 'preview.sqlite3'
        self.rows = [{'msgid': 'a', 'from_user_id': 'alice', 'text': 'hello'},
                     {'msgid': 'a', 'from_user_id': 'bob', 'text': 'other chat'},
                     {'msgid': 'bad-route', 'text': 'cannot guess target'},
                     {'msgid': 'command', 'auto_handled': True}]
        self.inbox.write_text(''.join(json.dumps(row) + '\n' for row in self.rows) + '{"partial":')
        bridge = self.workspace / 'native-bridge'
        bridge.mkdir()
        (bridge / 'state.json').write_text(json.dumps({'channels': {'weixin': {
            'spool_offset': 20, 'spool_tombstones': [[50, 70]], 'submission_pending': {'msgid': 'unknown'}}}}))
        (self.inbox.parent / 'outbox_partial.json').write_text('{"parts_sent":1}')
        (self.inbox.parent / 'outbox_parked.json').write_text('{"still_sending":true}')

    def migrate(self, **kwargs):
        options = dict(accounts={'weixin': 'explicit-bot'}, snapshot_confirmed=True)
        options.update(kwargs)
        return migrate_snapshot(self.workspace, self.destination, **options)

    def test_imported_tasks_are_isolated_and_never_executable(self):
        original = self.inbox.read_bytes()
        report = self.migrate()
        self.assertEqual(report['imported_tasks'], 2)
        self.assertEqual(report['unsupported_records'], 2)
        self.assertFalse(report['executable'])
        with Store(self.destination) as store:
            self.assertEqual(store.snapshot()['tasks'], {'reconciling': 2})
            self.assertEqual(store.snapshot()['conversations'], 2)
            self.assertIsNone(store.claim('worker'))
            raw = [row[0] for row in store.connection.execute('SELECT payload FROM shadow_records')]
            self.assertTrue(any(b'spool_tombstones' in item for item in raw))
            self.assertTrue(any(b'parts_sent' in item for item in raw))
            self.assertTrue(any(b'still_sending' in item for item in raw))
        self.assertEqual(self.inbox.read_bytes(), original)

    def test_confirmation_and_explicit_account_required(self):
        for options in ({'snapshot_confirmed': False}, {'accounts': {}}, {'accounts': {'weixin': ''}}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.migrate(**options)
        self.assertFalse(self.destination.exists())

    def test_migration_cannot_write_into_the_legacy_snapshot(self):
        self.destination = self.workspace / 'native-bridge/new-state.sqlite3'
        with self.assertRaises(ValueError):
            self.migrate()
        self.assertFalse(self.destination.exists())

    def test_existing_target_cannot_be_modified_by_rerun(self):
        self.migrate()
        original = self.destination.read_bytes()
        with self.assertRaises(FileExistsError):
            self.migrate()
        self.assertEqual(self.destination.read_bytes(), original)

    def test_invalid_route_is_preserved_as_evidence_not_guessed(self):
        self.inbox.write_text(json.dumps({'msgid': 'a', 'text': 'no route'}) + '\n')
        report = self.migrate()
        self.assertEqual(report['imported_tasks'], 0)
        self.assertEqual(report['unsupported_records'], 1)
        with Store(self.destination) as store:
            self.assertEqual(store.snapshot()['conversations'], 0)
            self.assertTrue(store.connection.execute('SELECT COUNT(*) FROM shadow_records').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
