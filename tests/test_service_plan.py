"""Generate reviewable systemd files without writing to the host service directory."""
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ServicePlanTests(unittest.TestCase):
    def test_custom_home_plan_contains_only_selected_channels_and_never_overwrites(self):
        """A fresh install can inspect every path before applying its services."""
        from mobile_channels.install import write_service_plan
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / 'install home'
            destination = Path(directory) / 'plan'
            report = write_service_plan(ROOT, home, destination, channels=['wecom'])
            self.assertTrue((destination / 'wecom-bot.service').is_file())
            self.assertFalse((destination / 'weixin-bot.service').exists())
            self.assertFalse((destination / 'muse-core-run.service').exists())
            service = (destination / 'native-bridge.service').read_text()
            self.assertNotIn('/home/hatch', service)
            self.assertIn(str(home / '.config/native-bridge/config.json'), service)
            self.assertIn(str(home / 'workspace/native-bridge'), service)
            self.assertIn(str(home / 'muse-test-venv/bin/python'), service)
            self.assertIn('install.sh', report['files'])
            subprocess.run(['bash', '-n', str(destination / 'install.sh')], check=True)
            with self.assertRaises(FileExistsError):
                write_service_plan(ROOT, home, destination, channels=['wecom'])


if __name__ == '__main__':
    unittest.main()
