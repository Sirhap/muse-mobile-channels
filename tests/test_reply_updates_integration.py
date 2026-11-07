"""Recover delayed Muse fragments through the normal Core and delivery loops."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from mobile_channels.executors import DualTrackExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.runtime import CoreRunner, DeliveryRunner, SendOutcome
from mobile_channels.storage import Store


class ReplyUpdatesIntegrationTests(unittest.TestCase):
    def test_late_fragment_is_delivered_once_to_its_original_chat_after_restart(self):
        """A later history message survives restart and preserves the original route."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'core.sqlite3'
            now = [100.0]
            history = []

            class Client:
                def call_json(self, method, **kwargs):
                    if method == 'session.start':
                        return {'session_id': 'side-1'}
                    return {'chat_events': list(history)}

                def _open(self, *args, **kwargs):
                    history.append({'seq': 10, 'event_name': 'message.user', 'message_id': 'u-1'})
                    return 7

                def close(self):
                    pass

            def executor(store):
                primary = Mock()
                primary.is_configured.return_value = False
                return DualTrackExecutor(store, primary=primary,
                                         fallback=MuseExecutor(store, client_factory=Client))

            sent = []

            def send(claim):
                sent.append((claim.chat_id, claim.text))
                return SendOutcome('delivered', receipt=claim.part_id)

            with Store(database, clock=lambda: now[0]) as store:
                CoreIngress(store, 'wecom', 'bot', {'alice'}).receive('m1', 'alice', 'chat-1', 'single', 'question')
                runner = CoreRunner(store, executor(store), 'first')
                self.assertEqual(runner.tick(), 'accepted')
                history.append({'seq': 11, 'event_name': 'message.assistant', 'message_id': 'a-1',
                                'payload': {'status': 'completed', 'display_text': 'first answer',
                                            'reply_to_message_id': 'u-1'}})
                with patch('mobile_channels.executors.time.sleep'):
                    self.assertEqual(runner.tick(), 'succeeded')
                self.assertEqual(DeliveryRunner(store, send, 'delivery').tick(), 'delivered')
            history.append({'seq': 12, 'event_name': 'message.assistant', 'message_id': 'a-2',
                            'payload': {'status': 'completed', 'display_text': 'later fragment',
                                        'reply_to_message_id': 'u-1'}})
            now[0] += 20
            with Store(database, clock=lambda: now[0]) as reopened:
                self.assertEqual(executor(reopened).poll_replies(), 'appended')
                self.assertEqual(DeliveryRunner(reopened, send, 'delivery').tick(), 'delivered')
                now[0] += 20
                executor(reopened).poll_replies()
                self.assertEqual(DeliveryRunner(reopened, send, 'delivery').tick(), 'idle')
            self.assertEqual(sent, [('chat-1', 'first answer'), ('chat-1', 'later fragment')])


if __name__ == '__main__':
    unittest.main()
