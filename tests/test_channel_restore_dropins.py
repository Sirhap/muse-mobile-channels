"""Exercise restore.sh against an isolated workspace and fake systemd."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from mobile_channels.storage import Store


ROOT = Path(__file__).resolve().parents[1]


class ChannelRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name) / 'install home'
        self.home.mkdir()
        self.ws = self.home / 'workspace'
        self.units = self.home / 'units'
        self.units.mkdir()
        self.calls = self.home / 'systemctl.log'
        self.calls.touch()
        for service in ('wecom-bot', 'weixin-bot', 'native-bridge', 'approval-relay', 'channel-restore'):
            target = self.ws / service
            target.mkdir(parents=True)
            for unit in (ROOT / service).glob('*.service'):
                shutil.copyfile(unit, target / unit.name)
        for name in ('approval-relay.service.d', 'wecom-bot.service.d'):
            shutil.copytree(ROOT / 'channel-restore' / name, self.ws / 'channel-restore' / name)
        shutil.copyfile(ROOT / 'channel-restore/channel-restore.timer',
                        self.ws / 'channel-restore/channel-restore.timer')
        for relative in ('workspace/wecom-bot/.venv/bin/python', 'muse-test-venv/bin/python'):
            executable = self.home / relative
            executable.parent.mkdir(parents=True)
            executable.write_text('#!/bin/sh\nexit 0\n')
            executable.chmod(0o700)
        credentials = self.home / '.config/wecom-bot/credentials.env'
        credentials.parent.mkdir(parents=True)
        credentials.touch()
        (self.ws / 'native-bridge/status.json').write_text(json.dumps({'ts': 4102444800}))
        restore = (ROOT / 'channel-restore/restore.sh').read_text()
        self.script = self.ws / 'channel-restore/restore.sh'
        self.script.write_text(restore.replace('/etc/systemd/system', str(self.units)))
        self.script.chmod(0o700)

    def configure_core(self, mode='core'):
        state = self.ws / 'state'
        state.mkdir()
        database = state / 'core.sqlite3'
        with Store(database):
            pass
        (state / 'channel-modes.json').write_text(json.dumps({
            'channels': {'wecom': {'mode': mode, 'database': str(database),
                                   'account_id': 'bot', 'allowed_senders': ['tester']}},
        }))

    def restore(self, failed_restart=False, failed_reload=False):
        """Run the real restore entry point with a service that stays active."""
        return subprocess.run(
            ['bash', '-c', '''
                systemctl() {
                  printf '%s\\n' "$*" >> "$TEST_SYSTEMCTL_LOG"
                  case "$1" in
                    restart) [[ "$TEST_FAILED_RESTART" != 1 || "$2" != wecom-bot ]] ;;
                    daemon-reload) [[ "$TEST_FAILED_RELOAD" != 1 ]] ;;
                    *) return 0 ;;
                  esac
                }
                sleep() { :; }
                flock() { return 0; }
                export -f systemctl sleep flock
                exec bash "$TEST_RESTORE"
            '''],
            env={**os.environ, 'MUSE_HOME': str(self.home),
                 'PYTHONPATH': str(ROOT), 'TEST_SYSTEMCTL_LOG': str(self.calls),
                 'TEST_RESTORE': str(self.script),
                 'TEST_FAILED_RESTART': str(int(failed_restart)),
                 'TEST_FAILED_RELOAD': str(int(failed_reload))},
            capture_output=True, text=True, timeout=15,
        )

    def test_only_configured_gateway_is_restored_and_legacy_gets_no_core_units(self):
        for mode in (None, 'legacy'):
            with self.subTest(mode=mode):
                if mode is not None:
                    self.configure_core(mode)
                result = self.restore()
                self.assertFalse((self.units / 'weixin-bot.service').exists())
                self.assertFalse((self.units / 'muse-core-run.service').exists())
                self.assertFalse((self.units / 'wecom-bot.service.d/core-mode.conf').exists())
                self.assertFalse((self.units / 'approval-relay.service.d/modes.conf').exists())
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_core_dropins_use_the_deployment_path_and_retry_failed_restart(self):
        self.configure_core()
        first = self.restore(failed_restart=True)
        self.assertNotEqual(first.returncode, 0, first.stdout + first.stderr)
        second = self.restore()
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        third = self.restore()
        self.assertEqual(third.returncode, 0, third.stdout + third.stderr)
        calls = self.calls.read_text().splitlines()
        self.assertEqual(calls.count('restart wecom-bot'), 2, calls)
        self.assertEqual(calls.count('restart approval-relay'), 1, calls)
        for name in ('wecom-bot.service.d/core-mode.conf', 'approval-relay.service.d/modes.conf'):
            self.assertIn(str(self.ws / 'state/channel-modes.json'), (self.units / name).read_text())
        self.assertTrue((self.units / 'muse-core-run.service').is_file())

    def test_reload_failure_is_reported_and_retried(self):
        self.configure_core()
        first = self.restore(failed_reload=True)
        self.assertNotEqual(first.returncode, 0)
        second = self.restore()
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertIn('restart wecom-bot', self.calls.read_text().splitlines())

    def test_invalid_core_configuration_never_starts_legacy_writers(self):
        self.configure_core()
        (self.ws / 'state/core.sqlite3').unlink()
        result = self.restore()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.units / 'wecom-bot.service').exists())
        self.assertFalse(any(line.startswith(('start ', 'restart ')) for line in self.calls.read_text().splitlines()))

    def test_unit_only_reload_failure_survives_next_restore(self):
        self.assertEqual(self.restore().returncode, 0)
        unit = self.ws / 'wecom-bot/wecom-bot.service'
        unit.write_text(unit.read_text() + '\n# changed unit\n')
        first = self.restore(failed_reload=True)
        self.assertNotEqual(first.returncode, 0)
        self.calls.write_text('')
        second = self.restore()
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertIn('daemon-reload', self.calls.read_text().splitlines())

    def test_core_unit_uses_the_deployed_home_and_configured_database(self):
        self.configure_core()
        path = self.ws / 'state/channel-modes.json'
        document = json.loads(path.read_text())
        database = self.ws / 'state/custom.sqlite3'
        (self.ws / 'state/core.sqlite3').rename(database)
        document['channels']['wecom']['database'] = str(database)
        path.write_text(json.dumps(document))
        result = self.restore()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        unit = (self.units / 'muse-core-run.service').read_text()
        self.assertNotIn('/home/hatch', unit)
        self.assertIn(str(self.home / 'muse-test-venv/bin/python'), unit)
        self.assertIn(str(database), unit)


if __name__ == '__main__':
    unittest.main()
