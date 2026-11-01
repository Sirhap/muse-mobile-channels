"""End-to-end sandbox contracts for ingress, execution, restart and delivery."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.executors import MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.runtime import CoreRunner, DeliveryRunner, ExecutionOutcome, SendOutcome
from mobile_channels.storage import Store


class RuntimeTests(unittest.TestCase):
    """Drive actual SQLite and runtime loops without platform credentials."""

    def setUp(self):
        """Create isolated state with a controllable lease clock."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'runtime.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, 'wecom', 'bot', {'alice', 'bob'})
        self.executor = Mock()
        self.executor.submit.return_value = ExecutionOutcome('accepted', run_id='run1')
        self.executor.query.return_value = ExecutionOutcome('succeeded', text='answer')

    def receive(self, mid='m1', sender='alice', chat='chat1'):
        """Use the validated ingress rather than creating task rows directly."""
        return self.ingress.receive(mid, sender, chat, 'single', 'question')

    def test_platform_cancellation_without_local_request_releases_session(self):
        """A terminal event from Muse UI cancellation is valid without phone /stop."""
        first = self.receive()
        runner = CoreRunner(self.store, self.executor, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        self.executor.query.return_value = ExecutionOutcome('cancelled', evidence='platform_run_cancelled')
        self.assertEqual(runner.tick(), 'cancelled')
        task = self.store.task(first.task_id)
        self.assertEqual((task['state'], task['cancel_requested']), ('cancelled', 0))
        event = self.store.connection.execute(
            "SELECT evidence FROM task_events WHERE task_id=? AND event='platform_cancelled'",
            (first.task_id,)).fetchone()
        self.assertEqual(event['evidence'], 'platform_run_cancelled')
        next_task = self.receive('next')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(self.store.task(next_task.task_id)['state'], 'running')

    def test_platform_cancel_is_recovered_after_worker_restart(self):
        """Lease recovery accepts the original run's cancelled terminal evidence."""
        first = self.receive()
        runner = CoreRunner(self.store, self.executor, 'first', lease_seconds=10)
        runner.tick()
        self.now += 11
        self.executor.query.return_value = ExecutionOutcome('cancelled', evidence='platform_run_cancelled')
        with Store(self.path, clock=lambda: self.now) as reopened:
            self.assertEqual(CoreRunner(reopened, self.executor, 'second').tick(), 'cancelled')
            self.assertEqual(reopened.task(first.task_id)['state'], 'cancelled')
        self.executor.submit.assert_called_once()

    def test_full_ingress_execution_delivery_and_dedup(self):
        """A real task progresses once and its answer retains the original route."""
        task = self.receive()
        self.assertFalse(self.receive().created)
        runner = CoreRunner(self.store, self.executor, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(runner.tick(), 'succeeded')
        self.assertEqual(runner.tick(), 'idle')
        sender = Mock(return_value=SendOutcome('delivered', receipt='platform-1'))
        delivery = DeliveryRunner(self.store, sender, 'sender')
        self.assertEqual(delivery.tick(), 'delivered')
        self.assertEqual(delivery.tick(), 'idle')
        sent = sender.call_args.args[0]
        self.assertEqual((sent.channel, sent.account_id, sent.chat_id, sent.text),
                         ('wecom', 'bot', 'chat1', 'answer'))
        self.assertEqual(self.store.task(task.task_id)['state'], 'succeeded')
        self.assertEqual(self.store.snapshot()['outbound'], {'delivered': 1})
        self.executor.submit.assert_called_once()

    def test_unauthorized_and_empty_allowlist_fail_before_persistence(self):
        """An untrusted sender cannot create an executable task or discover routes."""
        with self.assertRaises(PermissionError):
            self.receive(sender='mallory')
        with self.assertRaises(ValueError):
            CoreIngress(self.store, 'wecom', 'bot', set())
        self.assertEqual(self.store.snapshot()['tasks'], {})

    def test_submit_timeout_never_resubmits_after_restart(self):
        """A process dying around submission transfers only query responsibility."""
        task = self.receive()
        self.executor.submit.side_effect = TimeoutError('accepted but response lost')
        self.executor.query.return_value = ExecutionOutcome('unknown')
        self.assertEqual(CoreRunner(self.store, self.executor, 'first', lease_seconds=10).tick(), 'unknown')
        self.now += 11
        with Store(self.path, clock=lambda: self.now) as reopened:
            runner = CoreRunner(reopened, self.executor, 'second')
            self.assertEqual(runner.tick(), 'unknown')
            self.assertEqual(reopened.task(task.task_id)['state'], 'reconciling')
        self.executor.submit.assert_called_once()
        self.executor.query.assert_called_once()

    def test_running_result_recovered_without_second_submission(self):
        """Persisted accepted run identity is used by a new execution process."""
        task = self.receive()
        runner = CoreRunner(self.store, self.executor, 'first', lease_seconds=10)
        runner.tick()
        self.now += 11
        with Store(self.path, clock=lambda: self.now) as reopened:
            self.assertEqual(CoreRunner(reopened, self.executor, 'second').tick(), 'succeeded')
        self.assertEqual(self.executor.query.call_args.args[1], 'run1')
        self.executor.submit.assert_called_once()
        self.assertEqual(self.store.task(task.task_id)['state'], 'succeeded')

    def test_parallel_conversations_poll_fairly(self):
        """An indefinitely active first conversation cannot block another session."""
        first = self.receive()
        second = self.receive('m2', sender='bob', chat='chat2')
        self.executor.query.return_value = ExecutionOutcome('running')
        runner = CoreRunner(self.store, self.executor, 'worker', concurrency=2)
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(runner.tick(), 'accepted')
        runner.tick()
        runner.tick()
        queried = {call.args[0].claim.task_id for call in self.executor.query.call_args_list}
        self.assertEqual(queried, {first.task_id, second.task_id})
        contexts = [call.args[0] for call in self.executor.submit.call_args_list]
        self.assertEqual(len({context.claim.conversation_id for context in contexts}), 2)

    def test_cancel_timeout_does_not_release_session(self):
        """A failed cancel cannot make the next queued task execute concurrently."""
        first = self.receive()
        second = self.receive('m2')
        runner = CoreRunner(self.store, self.executor, 'worker')
        runner.tick()
        self.store.request_cancel(first.task_id, first.conversation_id)
        self.executor.cancel.side_effect = TimeoutError('cancel result unknown')
        self.assertEqual(runner.tick(), 'unknown')
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')
        self.assertEqual(self.store.task(second.task_id)['state'], 'queued')
        self.executor.submit.assert_called_once()

    def test_delivery_timeout_stays_unknown_after_restart(self):
        """No platform receipt must never silently become permission to send again."""
        self.receive()
        runner = CoreRunner(self.store, self.executor, 'worker')
        runner.tick()
        runner.tick()
        sender = Mock(side_effect=TimeoutError('send may have succeeded'))
        self.assertEqual(DeliveryRunner(self.store, sender, 'sender').tick(), 'unknown')
        with Store(self.path) as reopened:
            self.assertEqual(DeliveryRunner(reopened, sender, 'new-sender').tick(), 'idle')
        sender.assert_called_once()
        self.assertEqual(self.store.snapshot()['outbound'], {'delivery_unknown': 1})

    def test_lease_expiry_during_submission_cannot_commit_stale_result(self):
        """A slow call preserves evidence while its old owner loses commit rights."""
        self.receive()

        def delayed(context):
            self.now += 61
            return ExecutionOutcome('accepted', run_id='original-run')

        self.executor.submit.side_effect = delayed
        self.assertEqual(CoreRunner(self.store, self.executor, 'worker').tick(), 'ownership_lost')
        self.assertEqual(self.store.snapshot()['tasks'], {'reconciling': 1})

    def test_delivery_runner_is_bound_to_channel_and_account(self):
        """The account filter reaches the runtime's actual database claim."""
        self.receive()
        runner = CoreRunner(self.store, self.executor, 'worker')
        runner.tick()
        runner.tick()
        wrong_sender = Mock(return_value=SendOutcome('delivered', receipt='wrong'))
        self.assertEqual(DeliveryRunner(self.store, wrong_sender, 'wx',
                                       channel='weixin', account_id='bot').tick(), 'idle')
        wrong_sender.assert_not_called()
        correct_sender = Mock(return_value=SendOutcome('delivered', receipt='right'))
        self.assertEqual(DeliveryRunner(self.store, correct_sender, 'wc',
                                       channel='wecom', account_id='bot').tick(), 'delivered')
        correct_sender.assert_called_once()

    def test_muse_missing_run_correlation_never_uses_random_history(self):
        """A stream with no platform run ID remains unknown even if history has text."""
        self.receive()
        client = Mock()
        client.call_json.return_value = {'session_id': 'dedicated-session'}
        client._open.return_value = None
        adapter = MuseExecutor(self.store, client_factory=lambda: client)
        runner = CoreRunner(self.store, adapter, 'worker')
        self.assertEqual(runner.tick(), 'unknown')
        client.call_json.return_value = {'chat_events': [{
            'event_name': 'message.assistant', 'seq': 5,
            'payload': {'status': 'completed', 'display_text': 'not correlated'},
        }]}
        self.assertEqual(runner.tick(), 'unknown')
        client._open.assert_called_once()
        self.assertEqual(self.store.snapshot()['outbound'], {})

    def test_muse_correlated_run_completes_only_its_own_result(self):
        """A dedicated session does not excuse accepting unrelated assistant output."""
        self.receive()
        client = Mock()
        client.call_json.return_value = {'session_id': 'dedicated-session'}
        client._open.return_value = {'run_id': 'verified-run'}
        adapter = MuseExecutor(self.store, client_factory=lambda: client)
        runner = CoreRunner(self.store, adapter, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        client.call_json.return_value = {'chat_events': [
            {'event_name': 'message.assistant', 'seq': 1, 'run_id': 'other',
             'payload': {'status': 'completed', 'display_text': 'wrong'}},
            {'event_name': 'message.assistant', 'seq': 2, 'run_id': 'verified-run',
             'payload': {'status': 'completed', 'display_text': 'correct'}},
        ]}
        self.assertEqual(runner.tick(), 'succeeded')
        self.assertEqual(self.store.connection.execute('SELECT text FROM outbound_messages').fetchone()[0], 'correct')

    def test_new_generation_uses_a_new_muse_session(self):
        """/new changes context without overwriting older task/session bindings."""
        first = self.receive()
        client = Mock()
        client.call_json.return_value = {'session_id': 'old-session'}
        client._open.return_value = {'run_id': 'old-run'}
        adapter = MuseExecutor(self.store, client_factory=lambda: client)
        runner = CoreRunner(self.store, adapter, 'worker')
        runner.tick()
        client.call_json.return_value = {'chat_events': [{'event_name': 'message.assistant', 'run_id': 'old-run',
                                                         'payload': {'status': 'completed', 'display_text': 'done'}}]}
        runner.tick()
        self.store.new_generation(first.conversation_id)
        self.receive('m2')
        client.call_json.return_value = {'session_id': 'new-session'}
        client._open.return_value = {'run_id': 'new-run'}
        self.assertEqual(runner.tick(), 'accepted')
        bindings = self.store.connection.execute('SELECT generation,session_id FROM executor_sessions ORDER BY generation').fetchall()
        self.assertEqual([tuple(row) for row in bindings], [(0, 'old-session'), (1, 'new-session')])


if __name__ == '__main__':
    unittest.main()
