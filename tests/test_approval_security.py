"""Approval decisions require a bound operator and durable pre-submit ownership."""
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mobile_channels.approvals import ApprovalStore
from mobile_channels.models import InboundMessage, StateError
from mobile_channels.storage import Store

ROOT = Path(__file__).resolve().parents[1]


def load_relay():
    """Import the real relay without any production platform installation."""
    spec = importlib.util.spec_from_file_location('security_relay', ROOT / 'approval-relay/approval_relay.py')
    relay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(relay)
    return relay


class ApprovalSecurityTests(unittest.TestCase):
    """Exercise real file persistence and platform-call failure boundaries."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.relay = load_relay()
        self.relay.STATE_F = str(self.base / 'state.json')
        self.relay.DECISIONS_F = str(self.base / 'decisions.jsonl')
        self.relay.OFFSET_F = str(self.base / 'decisions.offset')
        self.relay.notify = Mock()
        self.state = {'next_num': 2, 'items': {'1': {
            'approval_id': 'approval', 'status': 'pending', 'expires_ms': 9999999999999,
            'decision_scope': {'channel': 'wecom', 'chat_id': 'room', 'operator_ids': ['alice']}}}}
        self.row = {'num': '1', 'decision': 'allow_once', 'channel': 'wecom',
                    'chat_id': 'room', 'operator_id': 'alice', 'ts': 1}
        self.environment = patch.dict(os.environ, {'APPROVAL_ADMINS_JSON': '{}'})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write(self, **changes):
        self.relay.save_state(self.state)
        self.base.joinpath('decisions.jsonl').write_text(json.dumps(dict(self.row, **changes)) + '\n')

    def test_unknown_operator_and_cross_chat_do_not_call_platform(self):
        for fields in ({'operator_id': 'mallory'}, {'chat_id': 'other'}, {'operator_id': ''}):
            with self.subTest(fields=fields):
                self.write(**fields)
                self.base.joinpath('decisions.offset').unlink(missing_ok=True)
                gateway = Mock()
                self.relay.consume_decisions(gateway, self.state)
                gateway.call_json.assert_not_called()

    def test_expired_and_missing_expiry_do_not_call_platform(self):
        for expires in (None, 1):
            with self.subTest(expires=expires):
                self.state['items']['1']['expires_ms'] = expires
                self.write()
                self.base.joinpath('decisions.offset').unlink(missing_ok=True)
                gateway = Mock()
                self.relay.consume_decisions(gateway, self.state)
                gateway.call_json.assert_not_called()

    def test_submit_is_unknown_on_disk_before_platform_and_timeout_never_repeats(self):
        self.write()
        def lost_response(*args, **kwargs):
            self.assertEqual(json.loads(self.base.joinpath('state.json').read_text())['items']['1']['status'], 'decision_unknown')
            raise TimeoutError('platform may have accepted')
        gateway = Mock()
        gateway.call_json.side_effect = lost_response
        self.relay.consume_decisions(gateway, self.state)
        restarted = self.relay.load_state()
        self.base.joinpath('decisions.offset').write_text('0')
        self.relay.consume_decisions(gateway, restarted)
        self.assertEqual(gateway.call_json.call_count, 1)
        self.assertEqual(restarted['items']['1']['status'], 'decision_unknown')

    def test_failed_precommit_prevents_external_decision(self):
        self.write()
        gateway = Mock()
        with patch.object(self.relay, 'save_state', side_effect=OSError('disk fault')):
            with self.assertRaises(OSError):
                self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_not_called()

    def mode_file(self, mode):
        """Write an explicit isolated mode declaration for cutover checks."""
        path = self.base / 'modes.json'
        path.write_text(json.dumps({'channels': {'wecom': {
            'mode': mode, 'database': str(self.base / 'core.sqlite3'),
            'account_id': 'bot', 'allowed_senders': ['alice']}}}))
        return path

    def test_core_mode_disables_legacy_approval_submission_and_notifications(self):
        """A switched channel cannot be written by the old approval relay."""
        self.write()
        mode_path = self.mode_file('core')
        gateway = Mock()
        with patch.dict(os.environ, {'MUSE_CHANNEL_MODES': str(mode_path)}):
            self.relay.consume_decisions(gateway, self.state)
            relay = load_relay()
            relay.BASE, relay.TEST = str(self.base), True
            with self.assertRaises(OSError):
                relay.notify('private', 'notice', channels=['wecom'])
        gateway.call_json.assert_not_called()
        self.assertFalse((self.base / 'shadow/wecom-outbox.jsonl').exists())

    def test_mode_lease_is_held_through_external_submission(self):
        """A cutover cannot race an approval already leaving the process."""
        from mobile_channels.modes import ModeError, write_modes
        self.write()
        mode_path = self.mode_file('legacy')
        gateway = Mock()
        def deciding(*args, **kwargs):
            with self.assertRaises(ModeError):
                write_modes(mode_path, {'channels': {'wecom': {'mode': 'core'}}})
        gateway.call_json.side_effect = deciding
        with patch.dict(os.environ, {'MUSE_CHANNEL_MODES': str(mode_path)}):
            self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_called_once()

    def test_stale_consumer_cannot_override_persisted_unknown_attempt(self):
        """A second consumer with stale pending memory must consult durable state."""
        self.write()
        stale = json.loads(json.dumps(self.state))
        gateway = Mock()
        gateway.call_json.side_effect = TimeoutError('uncertain')
        self.relay.consume_decisions(gateway, self.state)
        self.base.joinpath('decisions.offset').write_text('0')
        self.relay.consume_decisions(gateway, stale)
        self.assertEqual(gateway.call_json.call_count, 1)

    def test_scoped_notice_is_sent_only_to_original_route(self):
        """A scoped approval must not leak its operation text to another channel."""
        relay = load_relay()
        relay.BASE = str(self.base)
        relay.TEST = True
        relay.notify('private command', 'scope', item=self.state['items']['1'])
        self.assertFalse((self.base / 'shadow/weixin-outbox.jsonl').exists())
        row = json.loads((self.base / 'shadow/wecom-outbox.jsonl').read_text())
        self.assertEqual(row['chatid'], 'room')

    def test_explicit_notification_targets_cannot_override_bound_scope(self):
        """A caller-selected channel cannot redirect a private approval notice."""
        relay = load_relay()
        relay.BASE = str(self.base)
        relay.TEST = True
        relay.notify('private command', 'scope-override', channels=['weixin'],
                     item=self.state['items']['1'])
        self.assertFalse((self.base / 'shadow/weixin-outbox.jsonl').exists())
        row = json.loads((self.base / 'shadow/wecom-outbox.jsonl').read_text())
        self.assertEqual(row['chatid'], 'room')

    def test_malformed_scope_never_falls_back_to_broadcast(self):
        """A supplied but invalid private scope must stop notification delivery."""
        relay = load_relay()
        relay.BASE = str(self.base)
        relay.TEST = True
        for scope in ({}, {'channel': 'wecom'}, 'invalid'):
            with self.subTest(scope=scope), self.assertRaises(OSError):
                relay.notify('private command', 'invalid-scope', item={'decision_scope': scope})
        self.assertFalse((self.base / 'shadow').exists())

    def test_gateway_text_and_card_reject_unauthorized_route(self):
        """The real helpers reject before any row is queued or prior state revealed."""
        for channel in ('weixin', 'wecom'):
            spec = importlib.util.spec_from_file_location('approval_gateway_' + channel, ROOT / (channel + '-bot/gateway.py'))
            gateway = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(gateway)
            gateway.APPROVAL_RELAY_DIR = self.base
            self.relay.save_state(self.state)
            self.base.joinpath('decisions.jsonl').write_text('')
            ack = gateway.slash_approval_decide_ack('1', 'allow_once', channel, 'mallory', 'room')
            self.assertIn('无法提交', ack)
            self.assertEqual(self.base.joinpath('decisions.jsonl').read_text(), '')
            self.assertNotIn('#1', gateway.slash_approvals_text('mallory', 'room'))
            if channel == 'wecom':
                label, accepted = gateway.approval_card_decide('appr-1', 'appr_once', 'alice', 'another-room')
                self.assertFalse(accepted)

    def test_complete_jsonl_prefix_only_advances_offset(self):
        self.relay.save_state(self.state)
        self.base.joinpath('decisions.jsonl').write_text(json.dumps(self.row))
        gateway = Mock()
        self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_not_called()
        self.assertEqual(self.base.joinpath('decisions.offset').read_text(), '0')


class CoreApprovalTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(':memory:', clock=lambda: 100)
        self.addCleanup(self.store.close)
        message = InboundMessage('wecom', 'bot', 'group', 'room', 'm1', 'alice', 'work')
        self.conversation = self.store.ingest(message).conversation_id
        self.approvals = ApprovalStore(self.store)
        self.approvals.register('a1', self.conversation, ['alice'], 200)

    def test_scope_and_operator_are_checked_before_claim(self):
        for conversation, operator in ((self.conversation, 'mallory'), ('elsewhere', 'alice')):
            with self.assertRaises(PermissionError):
                self.approvals.claim_decision('a1', conversation, operator, 'allow_once')
        self.assertEqual(self.approvals.status('a1')['state'], 'pending')

    def test_claim_cannot_change_decision_or_be_replayed(self):
        claim = self.approvals.claim_decision('a1', self.conversation, 'alice', 'deny')
        with self.assertRaises(StateError):
            self.approvals.claim_decision('a1', self.conversation, 'alice', 'allow_always')
        self.approvals.resolve(claim, accepted=True, evidence='platform approval result')
        self.assertEqual(self.approvals.status('a1')['decision'], 'deny')

    def test_unknown_attempt_survives_database_reopen(self):
        """Decision choice is durable even if the submitting process disappears."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'approvals.sqlite3'
            with Store(path, clock=lambda: 100) as store:
                route = store.ingest(InboundMessage('wecom', 'bot', 'group', 'room', 'm', 'alice', 'work'))
                approvals = ApprovalStore(store)
                approvals.register('durable', route.conversation_id, ['alice'], 200)
                approvals.claim_decision('durable', route.conversation_id, 'alice', 'deny')
            with Store(path, clock=lambda: 101) as restarted:
                approvals = ApprovalStore(restarted)
                with self.assertRaises(StateError):
                    approvals.claim_decision('durable', route.conversation_id, 'alice', 'allow_once')
                self.assertEqual(approvals.status('durable')['state'], 'decision_unknown')

    def test_scope_cannot_be_widened_by_duplicate_registration(self):
        with self.assertRaises(StateError):
            self.approvals.register('a1', self.conversation, ['alice', 'mallory'], 200)


if __name__ == '__main__':
    unittest.main()
