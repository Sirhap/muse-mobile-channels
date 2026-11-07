"""Read-only setup reports reveal missing dependencies without revealing secrets."""
import json
from pathlib import Path
import tempfile
import unittest


class DoctorTests(unittest.TestCase):
    def test_missing_install_is_actionable_and_never_creates_runtime_files(self):
        from mobile_channels.doctor import inspect_installation
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = inspect_installation(root / 'workspace', root / 'home')
            self.assertFalse(report['ready'])
            self.assertTrue(report['checks'])
            self.assertFalse((root / 'workspace').exists())
            self.assertFalse((root / 'home').exists())

    def test_credential_contents_never_appear_in_permission_reports(self):
        from mobile_channels.doctor import inspect_installation
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            credentials = root / 'home/.config/native-bridge'
            credentials.mkdir(parents=True)
            secret = 'never-print-this-credential'
            for name in ('cookies.txt', 'vm_id.txt', 'token.json'):
                (credentials / name).write_text(secret)
                (credentials / name).chmod(0o644)
            report = inspect_installation(root / 'workspace', root / 'home')
            self.assertNotIn(secret, json.dumps(report))
            self.assertTrue(any(check['code'] == 'native_permissions' and check['status'] == 'failed'
                                for check in report['checks']))

    def test_malformed_modes_report_no_secret_content(self):
        from mobile_channels.doctor import inspect_installation
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            modes = root / 'modes.json'
            modes.write_text('malformed-private-content')
            report = inspect_installation(root / 'workspace', root / 'home', modes)
            self.assertNotIn('malformed-private-content', json.dumps(report))
            self.assertTrue(any(check['code'] == 'channel_modes' and check['status'] == 'failed'
                                for check in report['checks']))


if __name__ == '__main__':
    unittest.main()
