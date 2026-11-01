"""A live-mode process heartbeat must not mask a parked or unverified bridge."""
from __future__ import annotations

import importlib.util
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def load_gateway(channel):
    """Load the actual channel gateway without constructing a live client."""
    spec = importlib.util.spec_from_file_location(f'{channel}_check_health', ROOT / f'{channel}-bot/gateway.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BridgeCheckHealthTests(unittest.TestCase):
    """Use in-memory status fixtures and temporary gateway/hook state only."""

    def report(self, channel, bridge_state, *, mode='live', age=0):
        """Run the real /check and select its bridge diagnostic row."""
        module = load_gateway(channel)
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / 'state'
            state.mkdir()
            hook = Path(directory) / f'{channel}-bot'
            hook.mkdir()
            real_read = module._read_json_file

            def read(path, default=None):
                """Return only explicit fixtures for all bridge production paths."""
                if path.parent.name == 'native-bridge':
                    if path.name == 'status.json':
                        return {'ts': int(time.time()) - age, 'mode': {channel: mode}}
                    if path.name == 'state.json':
                        return {'channels': {channel: bridge_state}}
                    raise AssertionError(f'unexpected bridge read: {path}')
                return real_read(path, default)

            with patch.object(module, '_read_json_file', side_effect=read):
                text = module.slash_run_check('测试', state, hook)
        return next(line for line in text.splitlines() if line.startswith(('✓', '✗')) and '原生桥' in line)

    def test_exhaustion_is_failed_despite_fresh_live_heartbeat(self):
        """Both channels expose the persisted pause instead of green live mode."""
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                line = self.report(channel, {'reconnect_exhausted': True, 'reconnect_failures': 3})
                self.assertTrue(line.startswith('✗'), line)
                self.assertIn('已暂停', line)
                self.assertIn('3', line)

    def test_uncertain_submission_is_visible_and_not_treated_as_offline_retry(self):
        """An uncertain external execution must request reconciliation, not resubmission."""
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                line = self.report(channel, {'submission_pending': {'msgid': 'original'},
                                             'reconnect_exhausted': True})
                self.assertTrue(line.startswith('✗'), line)
                self.assertIn('提交结果待核查', line)
                self.assertIn('勿重复提交', line)

    def test_live_heartbeat_without_transport_evidence_is_unverified(self):
        """An online worker cannot prove platform connectivity by its own heartbeat."""
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                line = self.report(channel, {})
                self.assertTrue(line.startswith('✗'), line)
                self.assertIn('连接未验证', line)

    def test_retries_remain_failed(self):
        """An outage before exhaustion is visible while the process stays alive."""
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                line = self.report(channel, {'reconnect_failures': 2})
                self.assertTrue(line.startswith('✗'), line)
                self.assertIn('2', line)

    def test_shadow_and_stale_stay_failed(self):
        """Deployment mode and stale process status keep their existing diagnostics."""
        for channel in ('weixin', 'wecom'):
            with self.subTest(channel=channel):
                self.assertIn('shadow', self.report(channel, {}, mode='shadow'))
                self.assertIn('无心跳', self.report(channel, {}, age=120))


if __name__ == '__main__':
    unittest.main()
