"""Late answers retain their route and explain which conversation they answer."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.delivery import DeliveryStore
from mobile_channels.models import InboundMessage
from mobile_channels.runtime import DeliveryRunner, SendOutcome
from mobile_channels.storage import Store

LATE_PREFIX = '补充此前对话的结果：\n'


class LateDeliveryTests(unittest.TestCase):
    """Drive the real sender loop across context changes and durable retries."""

    def setUp(self):
        """Create a finished old-context task and an isolated SQLite database."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'core.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.item, self.outbound = self.answer('one', 'alice', '原答案正文')

    def answer(self, message_id, chat_id, text):
        """Persist a completed task using the same public execution contract."""
        item = self.store.ingest(InboundMessage('wecom', 'bot', 'single', chat_id,
                                               message_id, chat_id, 'original question'))
        claim = self.store.claim('executor')
        self.store.begin_submission(claim)
        self.store.mark_submitted(claim, 'run-' + message_id)
        return item, self.store.complete(claim, text)

    def test_actual_transport_labels_prepared_answer_after_new_context(self):
        """The send-time comparison catches /new after segmentation as well."""
        sender = Mock(return_value=SendOutcome('delivered', receipt='received'))
        runner = DeliveryRunner(self.store, sender, 'sender')
        runner.prepare()
        self.store.new_generation(self.item.conversation_id)
        self.assertEqual(runner.tick(), 'delivered')
        sent = sender.call_args.args[0]
        self.assertEqual(sent.text, LATE_PREFIX + '原答案正文')
        self.assertEqual((sent.chat_id, sent.generation), ('alice', 0))
        original = self.store.connection.execute('SELECT text FROM outbound_messages WHERE id=?',
                                                 (self.outbound,)).fetchone()[0]
        self.assertEqual(original, '原答案正文')

    def test_only_first_segment_is_labelled(self):
        """A long result does not repeat the source notice on every segment."""
        self.store.new_generation(self.item.conversation_id)
        sender = Mock(return_value=SendOutcome('delivered', receipt='received'))
        runner = DeliveryRunner(self.store, sender, 'sender', chunk_limit=3)
        while runner.tick() != 'idle':
            pass
        texts = [call.args[0].text for call in sender.call_args_list]
        self.assertEqual(texts, [LATE_PREFIX + '原答案', '正文'])
        self.assertEqual(''.join(texts).removeprefix(LATE_PREFIX), '原答案正文')

    def test_another_conversation_rotation_does_not_label_current_answer(self):
        """Changing another chat cannot change this reply's rendered content."""
        other = self.store.ingest(InboundMessage('wecom', 'bot', 'single', 'bob',
                                                'other', 'bob', 'question'))
        self.store.new_generation(other.conversation_id)
        sender = Mock(return_value=SendOutcome('delivered', receipt='received'))
        self.assertEqual(DeliveryRunner(self.store, sender, 'sender').tick(), 'delivered')
        self.assertEqual(sender.call_args.args[0].text, '原答案正文')

    def test_retry_keeps_frozen_payload_after_further_rotations_and_restart(self):
        """Stable part IDs must keep the exact same payload across explicit retries."""
        self.store.new_generation(self.item.conversation_id)
        sender = Mock(return_value=SendOutcome('retry', evidence='not accepted', retry_after=1))
        self.assertEqual(DeliveryRunner(self.store, sender, 'sender').tick(), 'retry')
        first = sender.call_args.args[0]
        self.store.new_generation(self.item.conversation_id)
        self.now += 2
        with Store(self.path, clock=lambda: self.now) as restarted:
            receiver = Mock(return_value=SendOutcome('delivered', receipt='received'))
            self.assertEqual(DeliveryRunner(restarted, receiver, 'next').tick(), 'delivered')
            later = receiver.call_args.args[0]
        self.assertEqual((first.part_id, first.text), (later.part_id, later.text))
        self.assertEqual(later.text.count(LATE_PREFIX), 1)

    def test_existing_unlabelled_send_is_not_rewritten_after_context_rotation(self):
        """A previously attempted payload cannot silently change under its stable ID."""
        sender = Mock(return_value=SendOutcome('retry', evidence='not accepted', retry_after=1))
        self.assertEqual(DeliveryRunner(self.store, sender, 'sender').tick(), 'retry')
        first = sender.call_args.args[0]
        self.store.new_generation(self.item.conversation_id)
        self.now += 2
        receiver = Mock(return_value=SendOutcome('delivered', receipt='received'))
        DeliveryRunner(self.store, receiver, 'next').tick()
        self.assertEqual(receiver.call_args.args[0].text, first.text)
        self.assertEqual(first.text, '原答案正文')


if __name__ == '__main__':
    unittest.main()
