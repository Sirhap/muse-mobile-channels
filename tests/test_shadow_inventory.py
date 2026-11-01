"""Shadow inventory must preserve evidence and never mutate legacy queues."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.config import RuntimeConfig
from mobile_channels.shadow import import_shadow


class ShadowInventoryTests(unittest.TestCase):
    """Exercise real temporary files and SQLite without external dependencies."""

    def setUp(self):
        """Create an isolated workspace and a separately located database."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'weixin-bot/state/inbox.jsonl'
        self.source.parent.mkdir(parents=True)
        self.database = self.root / 'shadow.sqlite3'

    def test_rerun_preserves_partial_invalid_and_original_bytes(self):
        """Repeated import retains partial evidence without duplicate revisions."""
        data = b'{"msgid":"one"}\nnot-json\n{"msgid":'
        self.source.write_bytes(data)
        first = import_shadow(self.root, self.database)
        second = import_shadow(self.root, self.database)
        self.assertEqual((first['complete'], first['invalid'], first['partial']), (1, 1, 1))
        self.assertEqual(second['inserted_revisions'], 0)
        self.assertEqual(self.source.read_bytes(), data)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM shadow_records').fetchone()[0], 3)
            tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('tasks', tables)

    def test_tail_completion_and_truncation_retain_previous_revisions(self):
        """Byte offsets alone must not confuse rewritten or extended files."""
        for data in (b'{"id":', b'{"id":1}\n', b'{"id":2}\n'):
            self.source.write_bytes(data)
            self.assertEqual(import_shadow(self.root, self.database)['inserted_revisions'], 1)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM shadow_sources').fetchone()[0], 3)

    def test_database_cannot_replace_source(self):
        """Invalid configuration is rejected before SQLite touches the source."""
        self.source.write_bytes(b'keep me')
        with self.assertRaises(ValueError):
            import_shadow(self.root, self.source)
        self.assertEqual(self.source.read_bytes(), b'keep me')

    def test_database_hardlink_cannot_modify_legacy_source(self):
        """A different filename referring to the same inode is still a source."""
        import os
        self.source.write_bytes(b'')
        os.link(self.source, self.database)
        with self.assertRaises(ValueError):
            import_shadow(self.root, self.database)
        self.assertEqual(self.source.read_bytes(), b'')

    def test_source_disappearance_rolls_back_the_snapshot(self):
        """A disappeared file cannot silently turn a partial snapshot successful."""
        self.source.write_bytes(b'{"id":1}\n')
        second = self.source.with_name('outbox.jsonl')
        second.write_bytes(b'{"id":2}\n')
        original_stat = Path.stat
        target = second.resolve()

        def disappeared(path, *args, **kwargs):
            """Simulate unlink after opening the second source."""
            if path == target:
                raise FileNotFoundError(str(path))
            return original_stat(path, *args, **kwargs)

        with patch.object(Path, 'stat', disappeared):
            with self.assertRaises(RuntimeError):
                import_shadow(self.root, self.database)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM shadow_sources').fetchone()[0], 0)

    def test_configuration_does_not_create_state(self):
        """Resolving startup configuration is safe during module imports."""
        config = RuntimeConfig.from_env({'MUSE_WORKSPACE': str(self.root / 'unused')})
        self.assertEqual(config.database, (self.root / 'unused/state/core-shadow.sqlite3').resolve())
        self.assertFalse(config.workspace.exists())


if __name__ == '__main__':
    unittest.main()
