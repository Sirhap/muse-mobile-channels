"""Legacy recovery requires exact evidence and exclusive ownership."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.recovery import bridge_lock, inspect_pending, resolve_pending


class RecoveryTests(unittest.TestCase):
    """Exercise actual state files, locks, identity checks and atomic commits."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.pending = {'kind': 'turn', 'row': {'msgid': 'm', 'from_user': 'alice', 'text': 'work'},
                        'turn': {'msgid': 'm', 'session_id': 'sid', 'baseline': 10}, 'nbytes': 50}
        self.state = {'channels': {'weixin': {'submission_pending': self.pending,
                       'turns': [], 'spool_offset': 50}}}
        self.write(self.state)

    def write(self, state):
        """Store the isolated original fixture."""
        (self.base / 'state.json').write_text(json.dumps(state))

    def resolve(self, outcome='accepted', **kwargs):
        """Resolve using exactly the state previously inspected by this operator."""
        args = dict(msgid='m', session_id='sid', digest=inspect_pending(self.base, 'weixin')['digest'],
                    outcome=outcome, evidence='operator checked original request correlation')
        args.update(kwargs)
        return resolve_pending(self.base, 'weixin', **args)

    def read(self):
        """Return committed channel state."""
        return json.loads((self.base / 'state.json').read_text())['channels']['weixin']

    def test_accepted_restores_original_turn_and_bound_route(self):
        self.resolve()
        state = self.read()
        self.assertIsNone(state['submission_pending'])
        self.assertEqual(state['turns'], [self.pending['turn']])
        self.assertEqual(state['spool_offset'], 50)
        self.assertEqual(state['submission_recovery_audit'][0]['outcome'], 'accepted')
        self.assertEqual((self.base / 'state.json').stat().st_mode & 0o777, 0o600)

    def test_not_submitted_requeues_without_rewinding_or_appending_spool(self):
        result = self.resolve('not_submitted')
        state = self.read()
        self.assertEqual(state['recovery_queue'][0]['row'], self.pending['row'])
        self.assertEqual(state['recovery_queue'][0]['recovery_id'], result['recovery_id'])
        self.assertEqual(state['spool_offset'], 50)
        self.assertFalse((self.base / 'spool').exists())

    def test_running_bridge_lock_blocks_recovery(self):
        with bridge_lock(self.base):
            with self.assertRaises(RuntimeError):
                self.resolve()
        self.assertEqual(self.read()['submission_pending'], self.pending)

    def test_stale_identity_digest_and_missing_evidence_rejected(self):
        for kwargs in ({'msgid': 'other'}, {'session_id': 'other'}, {'digest': 'stale'}, {'evidence': ''}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.resolve(**kwargs)
        self.assertEqual(self.read()['submission_pending'], self.pending)

    def test_write_failure_preserves_original_pending_and_audit(self):
        with patch('mobile_channels.files.os.replace', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.resolve()
        self.assertEqual(self.read()['submission_pending'], self.pending)
        self.assertNotIn('submission_recovery_audit', self.read())

    def test_repeat_resolution_cannot_repeat_requeue(self):
        digest = inspect_pending(self.base, 'weixin')['digest']
        self.resolve('not_submitted')
        with self.assertRaises(ValueError):
            resolve_pending(self.base, 'weixin', msgid='m', session_id='sid', digest=digest,
                            outcome='not_submitted', evidence='same operator decision')
        self.assertEqual(len(self.read()['recovery_queue']), 1)

    def test_merge_acceptance_replaces_only_matching_original_turn(self):
        self.state['channels']['weixin']['turns'] = [{'msgid': 'old', 'session_id': 'sid', 'ids': ['old']}]
        self.pending['kind'] = 'merge'
        self.pending['turn'].update(msgid='old', ids=['old', 'm'])
        self.write(self.state)
        self.resolve()
        self.assertEqual(self.read()['turns'][0]['ids'], ['old', 'm'])

    def test_corrupt_state_is_never_reinitialized(self):
        (self.base / 'state.json').write_text('{broken')
        with self.assertRaises(ValueError):
            inspect_pending(self.base, 'weixin')
        self.assertEqual((self.base / 'state.json').read_text(), '{broken')


if __name__ == '__main__':
    unittest.main()
