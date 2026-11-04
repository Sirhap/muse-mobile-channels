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

    def test_auth_failure_notifies_once_and_retries_after_600_seconds(self):
        """A busy polling loop must neither reconnect nor repeatedly notify."""
        self.fail_auth()
        self.cycle()
        self.now += 599
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 1)
        self.assertEqual(self.notify.call_count, 1)
        self.now += 1
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 2)
        self.assertEqual(self.notify.call_count, 1)

    def test_restart_retains_failure_budget_and_notice_deduplication(self):
        """A supervisor restart cannot reset a rejected credential's retry budget."""
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
        self.relay = self.module()
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 0)
        self.assertEqual(self.notify.call_count, 1)
        self.assertTrue(self.health()['reconnect_exhausted'])

    def test_only_cookie_or_vm_change_revives_parked_relay(self):
        """Changing the unused static token must not trigger another cookie request."""
        self.fail_auth()
        for _ in range(3):
            self.cycle()
            self.now += 600
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

    def test_successful_rpc_recovers_once_despite_later_decision_error(self):
        """A later local processing error cannot erase actual authentication evidence."""
        self.fail_auth()
        self.cycle()
        self.now += 600
        self.relay.connect.side_effect = None
        self.relay.consume_decisions = Mock(side_effect=OSError('disk failure'))
        self.cycle()
        self.cycle()
        self.assertEqual(self.health()['state'], 'healthy')
        self.assertEqual(self.notify.call_count, 2)
        self.assertEqual(self.health()['last_verified_at'], self.now)
        self.assertEqual(json.loads((self.base / 'status.json').read_text())['active_source'], 'cookies')

    def test_pending_health_notice_survives_failure_and_uses_stable_id(self):
        """An unavailable outbox retains the same event for subsequent retries."""
        self.fail_auth()
        self.notify.side_effect = OSError('no channel')
        self.cycle()
        first_id = self.notify.call_args.kwargs['event_id']
        self.relay = self.module()
        self.fail_auth()
        self.notify.side_effect = None
        self.cycle()
        self.assertEqual(first_id, self.notify.call_args.kwargs['event_id'])
        self.assertEqual(self.relay.connect.call_count, 0)
        self.assertEqual(self.health()['failures'], 1)

    def test_network_retry_budget_uses_short_backoff_and_never_claims_expiry(self):
        """Transport errors have their own diagnosis and bounded retry cadence."""
        self.relay.connect.side_effect = TimeoutError('transport timeout')
        self.cycle()
        self.now += 4
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 1)
        self.now += 1
        self.cycle()
        self.now += 10
        self.cycle()
        self.now += 600
        self.cycle()
        self.assertEqual(self.relay.connect.call_count, 3)
        self.assertTrue(self.health()['reconnect_exhausted'])
        self.assertEqual(self.health()['failure_kind'], 'network')
        self.assertNotIn('凭证已失效', self.notify.call_args.args[0])

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

    def test_health_notification_is_committed_before_outbox(self):
        """Crash recovery can find the event even when sending interrupts the process."""
        self.fail_auth()
        def check_durable(*args, **kwargs):
            """Inspect persistence at the actual notification boundary."""
            self.assertTrue(self.health()['pending_notices'])
            self.assertEqual(self.health()['pending_notices'][0]['id'], kwargs['event_id'])
        self.notify.side_effect = check_durable
        self.cycle()

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

    def test_new_outage_on_same_credentials_has_a_new_stable_event(self):
        """A recovery ends notification deduplication for the previous outage."""
        self.fail_auth()
        self.cycle()
        original = self.notify.call_args.kwargs['event_id']
        self.now += 600
        self.relay.connect.side_effect = None
        self.cycle()
        self.fail_auth()
        self.cycle()
        self.assertEqual(self.notify.call_count, 3)
        self.assertNotEqual(original, self.notify.call_args.kwargs['event_id'])

    def test_failed_notice_is_preserved_before_recovery_with_changed_credentials(self):
        """Outbox downtime must not erase an older fault when fresh cookies work."""
        self.fail_auth()
        self.notify.side_effect = OSError('channel offline')
        self.cycle()
        fault_id = self.health()['pending_notices'][0]['id']
        credentials = Path(self.relay.CONF)
        credentials.mkdir()
        (credentials / 'cookies.txt').write_text('replacement')
        self.relay.connect.side_effect = None
        self.cycle()
        pending = self.health()['pending_notices']
        self.assertEqual(len(pending), 2)
        self.assertEqual(pending[0]['id'], fault_id)
        self.assertIn('恢复', pending[1]['text'])
        self.notify.reset_mock(side_effect=True)
        self.cycle()
        self.assertEqual([call.kwargs['event_id'] for call in self.notify.call_args_list],
                         [event['id'] for event in pending])
        self.assertEqual(self.health()['pending_notices'], [])

    def test_failed_notice_is_preserved_before_same_credential_recovery(self):
        """A transient rejection also retains its fault event ahead of recovery."""
        self.fail_auth()
        self.notify.side_effect = OSError('channel offline')
        self.cycle()
        fault = self.health()['pending_notices'][0]
        self.now += 600
        self.relay.connect.side_effect = None
        self.cycle()
        self.assertEqual(self.health()['pending_notices'][0], fault)
        self.assertEqual(len(self.health()['pending_notices']), 2)

    def test_ack_save_failure_then_restart_does_not_append_duplicate(self):
        """Replaying a durable event after outbox acceptance leaves one physical row."""
        path = self.base / 'weixin-outbox.jsonl'
        self.relay.notify = load_relay().notify
        self.relay.notify.__globals__['TEST'] = False
        self.relay.notify.__globals__['CHANNELS']['weixin']['outbox'] = str(path)
        self.fail_auth()
        save = self.relay.save_state
        def fail_ack(state):
            """Fail exactly after notification removed the pending event in memory."""
            if state['credential_health']['failure_notice_created'] and not state['credential_health']['pending_notices']:
                raise OSError('ack failed')
            save(state)
        with patch.object(self.relay, 'save_state', side_effect=fail_ack):
            with self.assertRaises(OSError):
                self.cycle()
        self.assertEqual(len(path.read_text().splitlines()), 1)
        self.relay = self.module()
        real_notify = load_relay().notify
        real_notify.__globals__['TEST'] = False
        real_notify.__globals__['CHANNELS']['weixin']['outbox'] = str(path)
        self.relay.notify = real_notify
        self.fail_auth()
        self.cycle()
        self.assertEqual(len(path.read_text().splitlines()), 1)
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
