"""Readiness checks must never activate preview state or fabricate live acceptance."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.readiness import ReadinessError, ensure_executable_database, inspect_readiness
from mobile_channels.shadow import import_shadow
from mobile_channels.storage import Store


class CoreReadinessTests(unittest.TestCase):
    """Use real core and inventory databases without platform credentials."""

    def setUp(self):
        """Create initialized experimental state and an explicit account mode."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / 'core.sqlite3'
        with Store(self.database):
            pass
        self.modes = self.root / 'modes.json'
        self.modes.write_text(json.dumps({'channels': {'wecom': {
            'mode': 'core', 'account_id': 'bot', 'allowed_senders': ['alice'],
            'database': str(self.database.resolve())}}}))

    def test_new_core_database_allows_experiment_without_certifying_production(self):
        """Empty initialized state is usable but not a fake live acceptance signal."""
        ensure_executable_database(self.database)
        with patch.dict('os.environ', {'MUSE_BRIDGE_SOURCE': str(self.root / 'missing.py')}, clear=True):
            report = inspect_readiness(self.database, self.modes)
        self.assertTrue(report['experimental_database_allowed'])
        self.assertFalse(report['ready'])
        codes = {item['code'] for item in report['blockers']}
        self.assertIn('platform_run_correlation_unverified', codes)
        self.assertIn('transport_source_unavailable', codes)

    def test_preview_cannot_be_activated_even_if_report_flag_is_hand_edited(self):
        """A preview marker requires a real finalizer, not an executable boolean."""
        with sqlite3.connect(self.database) as connection:
            connection.execute('CREATE TABLE migration_report (id INTEGER PRIMARY KEY, report TEXT)')
            connection.execute('INSERT INTO migration_report VALUES (1,?)', (json.dumps({'executable': True, 'unsupported_records': 0}),))
        with self.assertRaises(ReadinessError):
            ensure_executable_database(self.database)
        report = inspect_readiness(self.database, self.modes)
        self.assertFalse(report['experimental_database_allowed'])
        self.assertIn('database_not_executable', {item['code'] for item in report['blockers']})

    def test_inventory_and_missing_paths_never_become_an_authority(self):
        """Read-only checks do not create a database when an operator mistypes it."""
        absent = self.root / 'absent.sqlite3'
        with self.assertRaises(sqlite3.Error):
            ensure_executable_database(absent)
        self.assertFalse(absent.exists())
        shadow = self.root / 'shadow.sqlite3'
        workspace = self.root / 'snapshot'
        workspace.mkdir()
        import_shadow(workspace, shadow)
        with self.assertRaises(ReadinessError):
            ensure_executable_database(shadow)

    def test_account_configuration_and_ownership_are_required(self):
        """A valid database does not excuse missing or mismatched account scopes."""
        self.modes.write_text(json.dumps({'channels': {'wecom': {'mode': 'core'}}}))
        report = inspect_readiness(self.database, self.modes)
        self.assertIn('channel_modes_invalid', {item['code'] for item in report['blockers']})

    def test_preview_evidence_is_not_mutated_by_inspection(self):
        """Malformed migration metadata is refused without repair or reset writes."""
        with sqlite3.connect(self.database) as connection:
            connection.execute('CREATE TABLE migration_report (id INTEGER PRIMARY KEY, report TEXT)')
            connection.execute('INSERT INTO migration_report VALUES (1,?)', ('broken-json',))
        with self.assertRaises(ReadinessError):
            ensure_executable_database(self.database)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute('SELECT report FROM migration_report').fetchone()[0], 'broken-json')


if __name__ == '__main__':
    unittest.main()
