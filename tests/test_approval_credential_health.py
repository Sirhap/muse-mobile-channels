"""Exercise persistent relay authentication recovery without platform credentials."""
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

from credential_health import append_health_notice_once

ROOT = Path(__file__).resolve().parents[1]


def load_relay():
    """Load the real relay with all optional transport imports still deferred."""
    spec = importlib.util.spec_from_file_location(
        'credential_health_relay', ROOT / 'approval-relay/approval_relay.py')
    relay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(relay)
    return relay


class RelayCredentialHealthTests(unittest.TestCase):
    """Use real persistence and fake time to cover retries across restarts."""

    def setUp(self):
        """Create isolated credentials, state, and notification destinations."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.now = 1000
        self.gateway = Mock()
        self.gateway.call_json.return_value = {'pending': []}
        self.notify = Mock()
        self.environment = patch.dict(os.environ, {'APPROVAL_NOTIFY_CHANNELS': 'weixin'})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.relay = self.module()

    def module(self):
        """Simulate a process restart while retaining the sandbox's durable state."""
        relay = load_relay()
        relay.BASE = str(self.base)
        relay.CONF = str(self.base / 'credentials')
        relay.STATE_F = str(self.base / 'state.json')
        relay.STATUS_F = str(self.base / 'status.json')
        relay.DECISIONS_F = str(self.base / 'decisions.jsonl')
        relay.OFFSET_F = str(self.base / 'decisions.offset')
        relay.connect = Mock(return_value=self.gateway)
        relay.notify = self.notify
        return relay

    def cycle(self):
        """Run one production cycle against the controlled wall clock."""
        with patch.object(self.relay.time, 'time', return_value=self.now):
            self.relay.cycle()

    def health(self):
        """Read exactly what another process would load after a restart."""
        return json.loads((self.base / 'state.json').read_text())['credential_health']

    def fail_auth(self):
        """Inject the observed service-side session rejection, without secrets."""
        self.relay.connect.side_effect = RuntimeError('auth/check 401 hatchAdmissionInvalidated')

    def test_auth_failure_never_notifies_user_and_retries_after_600_seconds(self):
        """2026-11-07: a busy polling loop must neither reconnect nor notify the user."""
        self.fail_auth()
        self.cycle()
        self.now += 599
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 1)
        self.assertEqual(self.notify.call_count, 0)
        self.now += 1
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 2)
        self.assertEqual(self.notify.call_count, 0)

    def test_restart_retains_failure_budget_and_declared_outage_notice(self):
        """A supervisor restart cannot reset a rejected credential's retry budget.

        2026-11-07 debounce: the third consecutive failure declares the
        outage and queues exactly one user notice; the restart must not
        re-declare it or retry the connection.
        """
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
        self.assertEqual(self.notify.call_count, 1)
        self.relay = self.module()
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 0)
        self.assertEqual(self.notify.call_count, 1)
        self.assertTrue(self.health()['reconnect_exhausted'])

    def test_failed_first_probe_after_credential_change_keeps_original_outage(self):
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
        failed_id = self.notify.call_args.kwargs['event_id']

        credentials = Path(self.relay.CONF)
        credentials.mkdir()
        (credentials / 'cookies.txt').write_text('replacement')
        self.cycle()
        self.now += 600
        self.relay.connect.side_effect = None
        self.cycle()

        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.notify.call_count, 2)
        self.assertEqual(self.notify.call_args.kwargs['event_id'],
                         failed_id.removesuffix('-failed') + '-recovered')

    def test_only_cookie_or_vm_change_revives_parked_relay(self):
        """Changing the unused static token must not trigger another cookie request.

        2026-11-07 debounce: the declared outage queues one failed notice;
        the recovery after a credential change queues one recovered notice.
        """
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
        self.assertEqual(self.notify.call_count, 1)
        credentials = Path(self.relay.CONF)
        credentials.mkdir()
        (credentials / 'token.json').write_text('{}')
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 3)
        (credentials / 'cookies.txt').write_text('placeholder')
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 4)
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.notify.call_count, 2)

    def test_successful_rpc_recovers_without_user_notice_despite_later_decision_error(self):
        """A later local processing error cannot erase actual authentication evidence."""
        self.fail_auth()
        self.cycle()
        self.now += 600
        self.relay.connect.side_effect = None
        self.relay.consume_decisions = Mock(side_effect=OSError('disk failure'))
        self.cycle()
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.notify.call_count, 0)
        self.assertEqual(self.health()['last_verified_at'], self.now)
        self.assertEqual(json.loads((self.base / 'status.json').read_text())['active_source'], 'cookies')

    def test_failed_health_notice_is_never_queued_to_user(self):
        """2026-11-07: a failed cycle keeps its retry budget but queues no user notice."""
        self.fail_auth()
        self.notify.side_effect = OSError('no channel')
        self.cycle()
        self.assertEqual(self.notify.call_count, 0)
        self.assertEqual(self.health()['pending_notices'], [])
        self.relay = self.module()
        self.fail_auth()
        self.notify.side_effect = None
        self.cycle()
        self.assertEqual(self.notify.call_count, 0)
        self.assertEqual(self.relay.connect.call_count, 0)
        self.assertEqual(self.health()['failures'], 1)

    def test_network_retry_budget_declares_outage_on_third_failure(self):
        """Transport errors have their own diagnosis and bounded retry cadence.

        2026-11-07 debounce: the first two failures are jitter (no user
        notice); the third consecutive failure declares the outage and
        queues exactly one notice.
        """
        self.relay.connect.side_effect = TimeoutError('transport timeout')
        self.cycle()
        self.now += 4
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 1)
        self.assertEqual(self.notify.call_count, 0)
        self.now += 1
        self.cycle()
        self.now += 10
        self.cycle()
        self.now += 600
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 3)
        self.assertTrue(self.health()['reconnect_exhausted'])
        self.assertEqual(self.health()['failure_kind'], 'network')
        self.assertEqual(self.notify.call_count, 1)

    def test_close_failure_does_not_erase_health_or_status(self):
        """Client cleanup cannot turn successful verification into a missing status."""
        self.gateway.close.side_effect = OSError('close failed')
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertTrue((self.base / 'status.json').exists())

    def test_approval_notice_failure_is_not_credential_failure(self):
        """Poll result processing errors must not enter credential backoff."""
        self.gateway.call_json.return_value = {'pending': [{
            'approval_id': 'one', 'registered_command': 'echo test',
        }]}
        self.notify.side_effect = OSError('outbox missing')
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.health()['failures'], 0)

    def test_health_failure_below_threshold_is_recorded_without_user_notice(self):
        """2026-11-07 debounce: one failure counts toward the retry budget
        but declares no outage and queues no user notice."""
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.health()['pending_notices'], [])
        self.assertEqual(self.health()['failures'], 1)
        self.assertFalse(self.health()['outage_active'])
        self.assertEqual(self.notify.call_count, 0)

    def test_third_consecutive_failure_declares_outage_and_notifies_once(self):
        """2026-11-07 debounce: the third consecutive failure declares the
        outage and queues exactly one user notice."""
        self.fail_auth()
        self.cycle()
        self.now += 600
        self.cycle()
        self.now += 600
        self.cycle()
        self.assertEqual(self.health()['failures'], 3)
        self.assertTrue(self.health()['outage_active'])
        self.assertEqual(self.notify.call_count, 1)

    def test_invalid_configuration_uses_auth_backoff_without_leaking_error(self):
        """Malformed or missing files pause safely without copying their contents."""
        for error in (ValueError('secret malformed cookie'), FileNotFoundError('secret path')):
            with self.subTest(error=type(error).__name__):
                (self.base / 'state.json').unlink(missing_ok=True)
                self.relay = self.module()
                self.relay.connect.side_effect = error
                self.cycle()
                self.now += 599
                self.cycle()
                self.assertEqual(self.relay.connect.call_count, 1)
                self.assertEqual(self.health()['failure_kind'], 'configuration')
                self.assertNotIn('secret', (self.base / 'status.json').read_text())
                self.assertNotIn('secret', (self.base / 'state.json').read_text())

    def test_rpc_rejection_requires_verification_before_recovery(self):
        """A newly constructed gateway alone never counts as restored authentication."""
        self.gateway.call_json.side_effect = RuntimeError('auth/check 401 hatchAdmissionInvalidated')
        self.cycle()
        self.assertEqual(self.health()['state'], 'failed')
        self.assertIsNone(self.health()['last_verified_at'])
        self.assertIsNone(json.loads((self.base / 'status.json').read_text())['active_source'])

    def test_rpc_error_object_does_not_count_as_verified(self):
        """Transports returning an error object must not falsely restore listening."""
        self.gateway.call_json.return_value = {'error': {'message': 'secret service error'}}
        self.cycle()
        self.assertEqual(self.health()['state'], 'failed')
        self.assertIsNone(self.health()['last_verified_at'])
        self.assertNotIn('secret', (self.base / 'status.json').read_text())

    def test_vm_change_also_revives_parked_relay(self):
        """Both files actually used by the relay participate in credential recovery."""
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
        credentials = Path(self.relay.CONF)
        credentials.mkdir()
        (credentials / 'vm_id.txt').write_text('new-vm')
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 4)
        self.assertEqual(self.health()['state'], 'healthy')

    def test_new_outage_after_recovery_sends_no_user_notice(self):
        """2026-11-07: a second outage after recovery also stays log-only."""
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.health()['state'], 'failed')
        self.now += 600
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.health()['state'], 'failed')
        self.assertEqual(self.notify.call_count, 0)
        self.assertEqual(self.health()['pending_notices'], [])

    def test_recovery_with_changed_credentials_sends_no_user_notice(self):
        """2026-11-07: recovery on fresh cookies is silent to the user."""
        self.fail_auth()
        self.notify.side_effect = OSError('channel offline')
        self.cycle()
        credentials = Path(self.relay.CONF)
        credentials.mkdir()
        (credentials / 'cookies.txt').write_text('replacement')
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.health()['pending_notices'], [])
        self.notify.reset_mock(side_effect=True)
        self.cycle()
        self.assertEqual(self.notify.call_count, 0)
        self.assertEqual(self.health()['pending_notices'], [])

    def test_recovery_after_transient_rejection_sends_no_user_notice(self):
        """2026-11-07: a transient rejection followed by recovery is silent to the user."""
        self.fail_auth()
        self.notify.side_effect = OSError('channel offline')
        self.cycle()
        self.assertEqual(self.health()['pending_notices'], [])
        self.now += 600
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.health()['pending_notices'], [])
        self.assertEqual(self.notify.call_count, 0)

    def test_failed_cycle_with_real_notify_writes_no_health_outbox_row(self):
        """2026-11-07: with the real notify path, a failed cycle writes no health row."""
        path = self.base / 'weixin-outbox.jsonl'
        self.relay.notify = load_relay().notify
        self.relay.notify.__globals__['TEST'] = False
        self.relay.notify.__globals__['CHANNELS']['weixin']['outbox'] = str(path)
        self.fail_auth()
        self.cycle()
        self.assertFalse(path.exists())
        self.assertEqual(self.health()['pending_notices'], [])
        self.relay = self.module()
        real_notify = load_relay().notify
        real_notify.__globals__['TEST'] = False
        real_notify.__globals__['CHANNELS']['weixin']['outbox'] = str(path)
        self.relay.notify = real_notify
        self.fail_auth()
        self.cycle()
        self.assertFalse(path.exists())
        self.assertEqual(self.health()['pending_notices'], [])

    def test_empty_or_nonstring_cookies_never_reach_gateway(self):
        """An invalid parser result cannot become a network authentication request."""
        relay = load_relay()
        relay.CONF = str(self.base)
        relay.GW2 = Mock()
        (self.base / 'vm_id.txt').write_text('vm')
        gateway = types.ModuleType('muse_cli.gateway')
        gateway.load_cookies = Mock()
        with patch.dict(sys.modules, {'muse_cli': types.ModuleType('muse_cli'),
                                      'muse_cli.gateway': gateway}):
            for cookies in ('', '  ', {}, {'cookie': 'value'}, []):
                gateway.load_cookies.return_value = cookies
                with self.subTest(cookie_type=type(cookies).__name__), self.assertRaises(ValueError):
                    relay.connect()
            relay.GW2.assert_not_called()
            gateway.load_cookies.return_value = 'cookie=value'
            (self.base / 'vm_id.txt').write_text('  ')
            with self.assertRaises(ValueError):
                relay.connect()
            relay.GW2.assert_not_called()


class StableHealthNotificationTests(unittest.TestCase):
    """Health events retain channel-specific ids while normal approvals stay random."""

    def test_health_event_ids_are_stable_for_each_channel(self):
        """Repeated outbox writes can be deduplicated by the downstream gateway."""
        relay = load_relay()
        with tempfile.TemporaryDirectory() as directory:
            relay.TEST = False
            for channel in relay.CHANNEL_ORDER:
                relay.CHANNELS[channel]['outbox'] = str(Path(directory) / (channel + '.jsonl'))
            with patch.object(relay, 'lease_from_env', return_value=None):
                for _ in range(2):
                    relay.notify('health', 'health', channels=['weixin', 'wecom'], event_id='incident-1')
            ids = []
            for channel in relay.CHANNEL_ORDER:
                rows = [json.loads(line) for line in Path(relay.CHANNELS[channel]['outbox']).read_text().splitlines()]
                self.assertEqual(len(rows), 1)
                ids.append(rows[0]['id'])
            self.assertNotEqual(*ids)

    def test_helper_rejects_corrupt_or_partial_outbox_without_appending(self):
        """Even a matching id before a bad tail cannot bypass outbox validation."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'outbox.jsonl'
            for content in ('{"id":"event"}\n{', '{broken}\n', '[]\n', '{"id":"other"}'):
                with self.subTest(content=content):
                    path.write_text(content)
                    with self.assertRaises(ValueError):
                        append_health_notice_once(path, {'id': 'event', 'content': 'notice'})
                    self.assertEqual(path.read_text(), content)

    def test_helper_resyncs_existing_event_after_failed_fsync(self):
        """A previous write without durability acknowledgement must be synced again."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'outbox.jsonl'
            with patch('credential_health.os.fsync', side_effect=OSError('fsync failed')):
                with self.assertRaises(OSError):
                    append_health_notice_once(path, {'id': 'event'})
            with patch('credential_health.os.fsync', wraps=os.fsync) as sync:
                append_health_notice_once(path, {'id': 'event'})
                self.assertGreaterEqual(sync.call_count, 1)
            self.assertEqual(len(path.read_text().splitlines()), 1)

    def test_helper_serializes_concurrent_writers(self):
        """Independent writer handles share the same file lock and stable event id."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'outbox.jsonl'
            with ThreadPoolExecutor(max_workers=4) as workers:
                results = [workers.submit(append_health_notice_once, path, {'id': 'one'})
                           for _ in range(8)]
                for result in results:
                    result.result()
            self.assertEqual(len(path.read_text().splitlines()), 1)


if __name__ == '__main__':
    unittest.main()
