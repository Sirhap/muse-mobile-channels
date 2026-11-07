"""Prove remote cancellation with correlated history before claiming it stopped."""
import unittest

import test_gadget_executor as fixtures


class GadgetServerCancelTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.TestGadgetHistoryFallback()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.context = self.fixture._get_context()
        self.fixture._submit_running(self.context)
        self.fixture.executor.server_cancel = True
        self.calls = []

    def history(self, foreign=False, cancelled=True):
        def call(method, **kwargs):
            self.calls.append(method)
            if method == 'chat.cancel':
                return {}
            events = [
                fixtures._history_event(10, 'message.user', 'u-ack-1'),
                {'seq': 11, 'event_name': 'task.status', 'message_id': 'status-1',
                 'payload': {'task_id': 'task-1', 'status': 'running'}},
            ]
            if foreign:
                events.append(fixtures._history_event(12, 'message.user', 'u-next'))
            if cancelled and 'chat.cancel' in self.calls:
                events.append({'seq': 13, 'event_name': 'task.status', 'message_id': 'status-2',
                               'payload': {'task_id': 'task-1', 'status': 'cancelled'}})
            return {'chat_events': events}
        self.fixture.history.call_json = call

    def test_remote_cancel_requires_the_original_task_terminal(self):
        self.history()
        outcome = self.fixture.executor.cancel(self.context, 'worker:1001')
        self.assertEqual(outcome.state, 'cancelled')
        self.assertEqual(outcome.evidence, 'platform_turn_cancelled_via_native')
        self.assertIn('chat.cancel', self.calls)
        self.assertEqual(self.fixture.fake_client.stopped, [1001])

    def test_cancel_ack_without_terminal_reports_local_only(self):
        self.history(cancelled=False)
        outcome = self.fixture.executor.cancel(self.context, 'worker:1001')
        self.assertIn('server_turn_may_continue', outcome.evidence)
        self.assertIn('chat.cancel', self.calls)

    def test_foreign_newer_user_message_prevents_session_cancel(self):
        self.history(foreign=True)
        outcome = self.fixture.executor.cancel(self.context, 'worker:1001')
        self.assertNotIn('chat.cancel', self.calls)
        self.assertIn('server_turn_may_continue', outcome.evidence)

    def test_remote_failure_preserves_local_only_cancellation(self):
        def failed_history(*args, **kwargs):
            raise ConnectionError('unavailable')
        self.fixture.history.call_json = failed_history
        outcome = self.fixture.executor.cancel(self.context, 'worker:1001')
        self.assertIn('server_turn_may_continue', outcome.evidence)
        self.assertEqual(self.fixture.fake_client.stopped, [1001])

    def test_remote_cancel_remains_opt_in(self):
        self.fixture.executor.server_cancel = False
        self.history()
        outcome = self.fixture.executor.cancel(self.context, 'worker:1001')
        self.assertEqual(self.calls, [])
        self.assertIn('server_turn_may_continue', outcome.evidence)


if __name__ == '__main__':
    unittest.main()
