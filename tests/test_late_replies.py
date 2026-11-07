"""Late reply fragments retain their original route and durable delivery identity."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.delivery import DeliveryStore
from mobile_channels.models import InboundMessage
from mobile_channels.replies import Replies
from mobile_channels.storage import Store


def history_event(seq, kind, message_id, text='', status='completed', parent=None):
    """Describe platform history independently of the continuation collector."""
    payload = {'status': status, 'display_text': text}
    if parent is not None:
        payload['reply_to_message_id'] = parent
    return {'seq': seq, 'event_name': kind, 'message_id': message_id, 'payload': payload}


class LateReplyTests(unittest.TestCase):
    """Use real SQLite and delivery claims with an isolated history boundary."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 100.0
        self.path = Path(self.temp.name) / 'core.sqlite3'
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.replies = Replies(self.store, lease_seconds=15)
        self.delivery = DeliveryStore(self.store)
        self.sequence = 0
        self.first, self.claim, self.outbound = self.completed_task()
        self.events = [
            history_event(10, 'message.user', 'u1'),
            history_event(11, 'message.assistant', 'a1', 'first'),
        ]

    def completed_task(self, chat_id='alice', descriptor=None):
        """Commit a real execution and watch through their transaction boundary."""
        self.sequence += 1
        item = self.store.ingest(InboundMessage(
            'wecom', 'bot', 'single', chat_id, f'm{self.sequence}', 'alice', 'question'))
        claim = self.store.claim('executor')
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, f'run-{self.sequence}')
        descriptor = descriptor or {
            'session_id': f'session-{self.sequence}', 'track': 'gadget',
            'baseline_seq': 9, 'anchor_seq': 10, 'ack_id': 'u1',
            'seen_ids': ['a1'], 'linked_ids': ['u1', 'a1'], 'max_seq': 11,
        }
        with self.store.transaction():
            outbound = self.store.complete(claim, 'first')
            self.replies.register(item.task_id, claim.attempt_id, descriptor)
        return item, claim, outbound

    def drain(self):
        """Confirm the pending transport parts and return their immutable claims."""
        sent = []
        while (claim := self.delivery.claim('sender')) is not None:
            sent.append(claim)
            self.delivery.delivered(claim, 'accepted')
        return sent

    def test_appends_late_attachment_after_initial_delivery_once_across_restart(self):
        self.delivery.prepare(self.outbound, ['first'])
        self.assertEqual([claim.text for claim in self.drain()], ['first'])
        self.events.append(history_event(12, 'message.assistant', 'a2',
                                         '[[FILE:/workspace/report.pdf]]', parent='u1'))
        self.assertEqual(self.replies.tick(lambda watch: self.events), 'appended')
        with Store(self.path, clock=lambda: self.now) as restarted:
            watcher = Replies(restarted)
            self.now += 10
            self.assertEqual(watcher.tick(lambda watch: self.events), 'polled')
        sent = self.drain()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0].text, '')
        self.assertEqual(sent[0].media, [
            {'kind': 'file', 'path': '/workspace/report.pdf', 'filename': 'report.pdf'},
        ])
        self.assertEqual(sent[0].chat_id, 'alice')
        self.assertEqual(self.delivery.prepare(self.outbound, ['first']), [self.outbound + ':0'])

    def test_appends_before_initial_parts_are_prepared(self):
        self.events.append(history_event(12, 'message.assistant', 'a2', 'second', parent='a1'))
        self.assertEqual(self.replies.tick(lambda watch: self.events), 'appended')
        self.assertEqual([claim.text for claim in self.drain()], ['first', 'second'])

    def test_new_generation_keeps_original_route_and_labels_late_fragment(self):
        self.delivery.prepare(self.outbound, ['first'])
        self.drain()
        self.store.new_generation(self.first.conversation_id)
        self.events.append(history_event(12, 'message.assistant', 'a2', 'late', parent='u1'))
        self.replies.tick(lambda watch: self.events)
        sent = self.drain()
        self.assertEqual((sent[0].chat_id, sent[0].generation), ('alice', 0))
        self.assertEqual(sent[0].text, '补充此前对话的结果：\nlate')

    def test_foreign_user_boundary_blocks_parentless_replies_but_keeps_old_ancestry(self):
        self.events.extend([
            history_event(12, 'message.user', 'u2'),
            history_event(13, 'message.assistant', 'a2', 'unrelated'),
            history_event(14, 'message.assistant', 'a3', 'old continuation', parent='u1'),
        ])
        self.replies.tick(lambda watch: self.events)
        self.assertEqual([claim.text for claim in self.drain()], ['first', 'old continuation'])
        self.now += 10
        # The next user anchor may age out of the history window.
        self.events = [history_event(15, 'message.assistant', 'a4', 'still unrelated')]
        self.replies.tick(lambda watch: self.events)
        self.assertEqual(self.drain(), [])

    def test_active_reply_can_complete_at_the_same_sequence(self):
        self.events.append(history_event(12, 'message.assistant', 'a2', 'partial', status='running', parent='u1'))
        self.assertEqual(self.replies.tick(lambda watch: self.events), 'polled')
        self.now += 10
        self.events[-1] = history_event(12, 'message.assistant', 'a2', 'complete', parent='u1')
        self.assertEqual(self.replies.tick(lambda watch: self.events), 'appended')
        self.assertEqual([claim.text for claim in self.drain()], ['first', 'complete'])

    def test_failed_fetch_is_bounded_and_does_not_starve_other_watches(self):
        second, _, _ = self.completed_task(chat_id='bob')
        visited = []
        def fail_history(watch):
            visited.append(watch['task_id'])
            raise ConnectionError('offline')
        self.assertEqual(self.replies.tick(fail_history), 'unknown')
        self.assertEqual(self.replies.tick(fail_history), 'unknown')
        self.assertEqual(visited, [self.first.task_id, second.task_id])
        self.assertEqual(self.replies.tick(fail_history), 'idle')

    def test_another_watcher_cannot_fetch_the_same_live_lease(self):
        with Store(self.path, clock=lambda: self.now) as other:
            competing = Replies(other)
            def history(watch):
                self.assertEqual(competing.tick(lambda row: self.fail('duplicate history query')), 'idle')
                return self.events
            self.assertEqual(self.replies.tick(history), 'polled')

    def test_expired_fetch_cannot_append_after_another_watcher_takes_over(self):
        self.events.append(history_event(12, 'message.assistant', 'a2', 'late', parent='u1'))
        with Store(self.path, clock=lambda: self.now) as other:
            competing = Replies(other)
            def slow_history(watch):
                self.now += 16
                self.assertEqual(competing.tick(lambda row: self.events), 'appended')
                return self.events
            self.assertEqual(self.replies.tick(slow_history), 'ownership_lost')
        self.assertEqual([claim.text for claim in self.drain()], ['first', 'late'])

    def test_unknown_delivery_is_not_retried_when_another_fragment_arrives(self):
        self.delivery.prepare(self.outbound, ['first'])
        claim = self.delivery.claim('sender')
        self.delivery.unknown(claim)
        self.events.append(history_event(12, 'message.assistant', 'a2', 'second', parent='u1'))
        self.replies.tick(lambda watch: self.events)
        self.assertEqual(self.drain(), [])

    def test_watch_registration_rolls_back_with_a_failed_completion_transaction(self):
        before = self.store.connection.execute('SELECT COUNT(*) FROM executor_reply_watches').fetchone()[0]
        with self.assertRaisesRegex(RuntimeError, 'abort'):
            with self.store.transaction():
                self.completed_task(chat_id='rollback')
                raise RuntimeError('abort')
        after = self.store.connection.execute('SELECT COUNT(*) FROM executor_reply_watches').fetchone()[0]
        self.assertEqual(after, before)

    def test_watch_expiry_ends_history_queries(self):
        self.now += 901
        self.assertEqual(self.replies.tick(lambda watch: self.fail('expired watch queried')), 'idle')
        self.assertEqual(self.store.connection.execute('PRAGMA user_version').fetchone()[0], 1)

    def test_late_fragment_precedes_unsent_newer_task_in_the_same_conversation(self):
        self.delivery.prepare(self.outbound, ['first'])
        self.drain()
        second, _, newer = self.completed_task(chat_id='alice')
        self.delivery.prepare(newer, ['first'])
        self.events.append(history_event(12, 'message.assistant', 'a2', 'late', parent='u1'))
        self.replies.tick(lambda watch: self.events)
        sent = self.drain()
        self.assertEqual([claim.text for claim in sent], ['late', 'first'])
        self.assertEqual([claim.outbound_id for claim in sent], [self.outbound, newer])

    def test_repeated_file_path_in_two_fragments_is_uploaded_once(self):
        marker = '[[FILE:/workspace/report.pdf]]'
        self.events.extend([
            history_event(12, 'message.assistant', 'a2', marker, parent='u1'),
            history_event(13, 'message.assistant', 'a3', marker, parent='u1'),
        ])
        self.replies.tick(lambda watch: self.events)
        sent = self.drain()
        self.assertEqual(len(sent), 2)
        self.assertEqual(len(sent[1].media), 1)
        self.assertEqual(json.loads(self.store.task(self.first.task_id)['media_json']), sent[1].media)

    def test_failed_append_does_not_advance_seen_checkpoint_or_change_prepared_parts(self):
        self.delivery.prepare(self.outbound, ['first'])
        self.drain()
        self.events.append(history_event(12, 'message.assistant', 'a2', 'late', parent='u1'))
        with patch.object(self.replies.delivery, '_aggregate', side_effect=RuntimeError('write failed')):
            self.assertEqual(self.replies.tick(lambda watch: self.events), 'unknown')
        self.assertEqual(self.store.task(self.first.task_id)['result'], 'first')
        self.assertEqual(self.drain(), [])
        self.now += 10
        self.assertEqual(self.replies.tick(lambda watch: self.events), 'appended')
        self.assertEqual([claim.text for claim in self.drain()], ['late'])


if __name__ == '__main__':
    unittest.main()
