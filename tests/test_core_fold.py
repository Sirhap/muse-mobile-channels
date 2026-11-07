"""Core fold (official-client steering): a message arriving while its
conversation has a live turn whose reply has not started yet is sent
into that session and closed out with the pointer reply, so the running
turn's single combined answer covers both.

Covers Store.foldable / mark_merged, CoreRunner tick integration, and
the fold + reply_started of both executors (Muse native track and
Gadget track). Run: ~/muse-test-venv/bin/python tests/test_core_fold.py
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mobile_channels.executors import DualTrackExecutor, GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.models import StateError
from mobile_channels.runtime import CoreRunner, ExecutionOutcome
from mobile_channels.storage import MERGE_POINTER_TEXT, Store


class FoldTests(unittest.TestCase):
    """Drive actual SQLite and the runner with a mock executor."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'fold.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, 'wecom', 'bot', {'alice'})
        self.executor = Mock()
        self.executor.submit.return_value = ExecutionOutcome('accepted', run_id='run1')
        self.executor.query.return_value = ExecutionOutcome('running')
        self.executor.reply_started.return_value = False
        self.executor.fold.return_value = ExecutionOutcome('accepted', run_id='run1')

    def receive(self, mid, text='question', chat='chat1'):
        return self.ingress.receive(mid, 'alice', chat, 'single', text)

    def running_pair(self):
        """Ingest A, submit it (running), then ingest B behind it."""
        first = self.receive('m-a', 'first')
        runner = CoreRunner(self.store, self.executor, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')
        second = self.receive('m-b', 'second')
        return runner, first, second

    def test_foldable_finds_queued_behind_running(self):
        runner, first, second = self.running_pair()
        pair = self.store.foldable('worker')
        self.assertIsNotNone(pair)
        self.assertEqual(pair['running']['id'], first.task_id)
        self.assertEqual(pair['queued']['id'], second.task_id)
        self.assertEqual(pair['queued_text'], 'second')
        self.assertEqual(pair['run_id'], 'run1')

    def test_foldable_none_without_running_turn(self):
        self.receive('m-a', 'first')
        self.assertIsNone(self.store.foldable('worker'))

    def test_foldable_none_for_other_owner(self):
        runner, first, second = self.running_pair()
        self.assertIsNone(self.store.foldable('someone-else'))

    def test_mark_merged_completes_with_pointer(self):
        runner, first, second = self.running_pair()
        outbound_id = self.store.mark_merged(second.task_id, first.task_id)
        task = self.store.task(second.task_id)
        self.assertEqual(task['state'], 'succeeded')
        self.assertEqual(task['result'], MERGE_POINTER_TEXT)
        row = self.store.connection.execute(
            "SELECT text FROM outbound_messages WHERE id=?", (outbound_id,)).fetchone()
        self.assertEqual(row['text'], MERGE_POINTER_TEXT)
        event = self.store.connection.execute(
            "SELECT evidence FROM task_events WHERE task_id=? AND event='merged_into'",
            (second.task_id,)).fetchone()
        self.assertEqual(event['evidence'], first.task_id)

    def test_mark_merged_refuses_dead_target(self):
        runner, first, second = self.running_pair()
        # Block the fold so this tick completes A instead.
        self.executor.reply_started.return_value = True
        self.executor.query.return_value = ExecutionOutcome('succeeded', text='done')
        self.assertEqual(runner.tick(), 'succeeded')
        self.assertEqual(self.store.task(first.task_id)['state'], 'succeeded')
        with self.assertRaises(StateError):
            self.store.mark_merged(second.task_id, first.task_id)
        self.assertEqual(self.store.task(second.task_id)['state'], 'queued')

    def test_tick_folds_when_reply_not_started(self):
        runner, first, second = self.running_pair()
        self.assertEqual(runner.tick(), 'folded')
        self.executor.reply_started.assert_called_once()
        self.executor.fold.assert_called_once()
        folded_text = self.executor.fold.call_args.args[2]
        self.assertEqual(folded_text, 'second')
        self.assertEqual(self.store.task(second.task_id)['state'], 'succeeded')
        self.assertEqual(self.store.task(second.task_id)['result'], MERGE_POINTER_TEXT)
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')

    def test_tick_skips_fold_when_reply_started(self):
        runner, first, second = self.running_pair()
        self.executor.reply_started.return_value = True
        result = runner.tick()
        self.assertEqual(result, 'running')
        self.executor.fold.assert_not_called()
        self.assertEqual(self.store.task(second.task_id)['state'], 'queued')
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')

    def test_tick_skips_fold_when_send_unconfirmed(self):
        runner, first, second = self.running_pair()
        self.executor.fold.return_value = ExecutionOutcome('unknown', evidence='fold_not_recorded')
        result = runner.tick()
        self.assertEqual(result, 'running')
        self.assertEqual(self.store.task(second.task_id)['state'], 'queued')
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')

    def test_folded_message_gets_own_turn_after_running_closes(self):
        """A message that arrived after the reply started is not lost:
        it keeps its own turn once the running one closes."""
        runner, first, second = self.running_pair()
        self.executor.reply_started.return_value = True
        self.assertEqual(runner.tick(), 'running')
        self.executor.query.return_value = ExecutionOutcome('succeeded', text='answer-a')
        self.assertEqual(runner.tick(), 'succeeded')
        self.assertEqual(self.store.task(first.task_id)['state'], 'succeeded')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(self.store.task(second.task_id)['state'], 'running')


def fake_bridge_client(seed=(), session_ids=('sid-1',), record=True):
    """Scripted bridge client: session.start hands out ids, _open records
    the sent message in history when ``record`` is true, chat.history
    returns the shared event list."""
    state = {'events': [dict(e) for e in seed],
             'seq': max((e.get('seq', 0) for e in seed), default=0),
             'sessions': list(session_ids)}
    client = Mock()

    def call_json(method, body=None, **kwargs):
        if method == 'session.start':
            sid = state['sessions'][0]
            if len(state['sessions']) > 1:
                state['sessions'].pop(0)
            return {'session_id': sid}
        return {'chat_events': [dict(e) for e in state['events']]}

    def _open(*args, **kwargs):
        body = kwargs.get('body', {}) or {}
        items = body.get('items') or []
        if record:
            state['seq'] += 1
            state['events'].append({
                'event_name': 'message.user', 'seq': state['seq'],
                'payload': {'content': items[0].get('text', '') if items else ''}})
        return 7

    client.call_json.side_effect = call_json
    client._open.side_effect = _open
    client.close.return_value = None
    return client, state


def assistant_event(state, text, status='completed'):
    state['seq'] += 1
    event = {'event_name': 'message.assistant', 'seq': state['seq'],
             'payload': {'status': status, 'display_text': text}}
    state['events'].append(event)
    return event


class MuseFoldTests(unittest.TestCase):
    """Native track: fold opens a stream on the same session; reply_started
    reads history for any assistant message after the baseline."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'fold-native.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, 'wecom', 'bot', {'alice'})
        self.client, self.state = fake_bridge_client()
        self.executor = MuseExecutor(self.store, client_factory=lambda: self.client)

    def submit_one(self):
        first = self.ingress.receive('m-a', 'alice', 'chat1', 'single', 'first')
        runner = CoreRunner(self.store, self.executor, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        task = self.store.task(first.task_id)
        self.assertEqual(task['state'], 'running')
        claim = self.store.connection.execute(
            "SELECT * FROM execution_attempts WHERE task_id=?", (first.task_id,)).fetchone()
        return first, claim['run_id']

    def test_late_reply_to_a_confirmed_fold_remains_bound_after_completion(self):
        """A merged user message remains a reply ancestor after its first result."""
        original_open = self.client._open.side_effect

        def open_with_identity(*args, **kwargs):
            stream = original_open(*args, **kwargs)
            event = self.state['events'][-1]
            event['message_id'] = f"u-{event['seq']}"
            return stream

        self.client._open.side_effect = open_with_identity
        primary = Mock()
        primary.is_configured.return_value = False
        dual = DualTrackExecutor(self.store, primary=primary, fallback=self.executor)
        runner = CoreRunner(self.store, dual, 'worker')
        first = self.ingress.receive('m-a', 'alice', 'chat1', 'single', 'first')
        self.assertEqual(runner.tick(), 'accepted')
        self.ingress.receive('m-b', 'alice', 'chat1', 'single', 'fold this')
        self.assertEqual(runner.tick(), 'folded')
        self.assertEqual(self.store.folded_platform_messages(first.task_id), {'u-2'})
        initial = assistant_event(self.state, 'first answer')
        initial['message_id'] = 'a-1'
        initial['payload']['reply_to_message_id'] = 'u-1'
        with patch('mobile_channels.executors.time.sleep'):
            self.assertEqual(runner.tick(), 'succeeded')
        late = assistant_event(self.state, 'answer to folded input')
        late['message_id'] = 'a-2'
        late['payload']['reply_to_message_id'] = 'u-2'
        self.now += 20
        self.assertEqual(dual.poll_replies(), 'appended')
        self.assertEqual(self.store.task(first.task_id)['result'],
                         'first answer\nanswer to folded input')

    def test_fold_sends_to_same_session(self):
        first, run_id = self.submit_one()
        claim = self.store.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (first.task_id,)).fetchone()
        from mobile_channels.models import Claim
        real_claim = Claim(claim['id'], claim['conversation_id'], claim['generation'],
                           claim['owner'], claim['attempt_id'], claim['token'])
        context = CoreRunner(self.store, self.executor, 'worker')._context(real_claim)
        outcome = self.executor.fold(context, run_id, 'second')
        self.assertEqual(outcome.state, 'accepted')
        self.assertEqual(outcome.run_id, run_id)
        opens = [c for c in self.client._open.call_args_list]
        self.assertEqual(len(opens), 2)
        fold_body = opens[1].kwargs['body']
        self.assertEqual(fold_body['session_id'], 'sid-1')
        self.assertEqual(fold_body['items'][0]['text'], 'second')

    def test_reply_started_false_then_true(self):
        first, run_id = self.submit_one()
        claim = self.store.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (first.task_id,)).fetchone()
        from mobile_channels.models import Claim
        real_claim = Claim(claim['id'], claim['conversation_id'], claim['generation'],
                           claim['owner'], claim['attempt_id'], claim['token'])
        context = CoreRunner(self.store, self.executor, 'worker')._context(real_claim)
        self.assertFalse(self.executor.reply_started(context, run_id))
        assistant_event(self.state, 'partial', status='running')
        self.assertTrue(self.executor.reply_started(context, run_id))

    def test_reply_to_stamped_answer_counts(self):
        """A handoff-resumed answer carries reply_to_message_id; it is
        still this turn's answer. The legacy bridge counts it
        (native_bridge.is_turn_reply, 2026-10-07 20:47 live loss: seq
        5222, status completed, never delivered). Regression for the
        parallel-review Major 2: the core used to skip reply_to events
        in both reply_started and query, so such a turn could be folded
        into after it answered and its query never completed."""
        first, run_id = self.submit_one()
        claim = self.store.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (first.task_id,)).fetchone()
        from mobile_channels.models import Claim
        real_claim = Claim(claim['id'], claim['conversation_id'], claim['generation'],
                           claim['owner'], claim['attempt_id'], claim['token'])
        context = CoreRunner(self.store, self.executor, 'worker')._context(real_claim)
        self.assertFalse(self.executor.reply_started(context, run_id))
        event = assistant_event(self.state, 'RESUMED-ANSWER', status='completed')
        event['reply_to_message_id'] = 'u1'
        event['payload']['reply_to_message_id'] = 'u1'
        self.assertTrue(self.executor.reply_started(context, run_id))
        outcome = self.executor.query(context, run_id)
        self.assertEqual(outcome.state, 'succeeded')
        self.assertEqual(outcome.text, 'RESUMED-ANSWER')


class FakeGadgetClient:
    """In-memory stand-in for GadgetClient, with the fold send_only."""

    def __init__(self, configured=True):
        self.configured = configured
        self.results = {}
        self.sent_only = []
        self.started = []
        self._next_pid = 1000

    def is_configured(self):
        return self.configured

    def start_turn(self, attempt_id, session_id, text, timeout_seconds=None):
        self._next_pid += 1
        self.started.append((attempt_id, session_id, text))
        return Mock(pid=self._next_pid)

    def send_only(self, session_id, text, timeout_seconds=60):
        self.sent_only.append((session_id, text))
        return {'state': 'sent', 'message_id': 'fold-msg-1', 'session_id': session_id}

    def read_result(self, attempt_id):
        return self.results.get(attempt_id)

    def worker_alive(self, pid):
        return True

    def stop_turn(self, proc, *, wait_seconds=5.0):
        return True

    def close(self):
        pass


class GadgetFoldTests(unittest.TestCase):
    """Gadget track: fold uses a send-only worker; reply_started reads the
    worker result file's reply_started flag."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'fold-gadget.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, 'wecom', 'bot', {'alice'})
        self.fake = FakeGadgetClient()
        self.executor = GadgetExecutor(self.store, client=self.fake)

    def submit_one(self):
        first = self.ingress.receive('m-a', 'alice', 'chat1', 'single', 'first')
        runner = CoreRunner(self.store, self.executor, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(self.store.task(first.task_id)['state'], 'running')
        claim = self.store.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (first.task_id,)).fetchone()
        from mobile_channels.models import Claim
        real_claim = Claim(claim['id'], claim['conversation_id'], claim['generation'],
                           claim['owner'], claim['attempt_id'], claim['token'])
        context = CoreRunner(self.store, self.executor, 'worker')._context(real_claim)
        run_id = self.store.connection.execute(
            "SELECT run_id FROM execution_attempts WHERE task_id=?",
            (first.task_id,)).fetchone()['run_id']
        return first, real_claim, context, run_id

    def test_fold_uses_send_only_on_same_session(self):
        first, real_claim, context, run_id = self.submit_one()
        outcome = self.executor.fold(context, run_id, 'second')
        self.assertEqual(outcome.state, 'accepted')
        self.assertEqual(outcome.message_id, 'fold-msg-1')
        self.assertEqual(len(self.fake.sent_only), 1)
        session_id, text = self.fake.sent_only[0]
        self.assertEqual(text, 'second')
        # The send went to the same side session the turn created.
        self.assertEqual(session_id, self.fake.started[0][1])

    def test_reply_started_from_worker_result(self):
        first, real_claim, context, run_id = self.submit_one()
        attempt_id = real_claim.attempt_id
        self.assertFalse(self.executor.reply_started(context, run_id))
        self.fake.results[attempt_id] = {'state': 'running', 'message_id': 'u1'}
        self.assertFalse(self.executor.reply_started(context, run_id))
        self.fake.results[attempt_id] = {'state': 'running', 'message_id': 'u1',
                                         'reply_started': True}
        self.assertTrue(self.executor.reply_started(context, run_id))
        self.fake.results[attempt_id] = {'state': 'succeeded', 'text': 'done',
                                         'message_id': 'a1'}
        self.assertTrue(self.executor.reply_started(context, run_id))

    def test_reply_started_old_attempt_without_result_is_started(self):
        """No result file on a fresh attempt means the worker just
        spawned (fold allowed). No result file on an attempt old enough
        that the worker should have written its running placeholder
        means the worker died before writing: fail safe, report
        started so the caller does NOT fold into a dead turn.
        Regression for the parallel-review minor 3."""
        first, real_claim, context, run_id = self.submit_one()
        self.assertFalse(self.executor.reply_started(context, run_id))
        self.now = 200.0  # 100s after submit; worker should have written by now
        self.assertTrue(self.executor.reply_started(context, run_id))


class DualTrackFoldTests(unittest.TestCase):
    """DualTrack routes fold/reply_started to the track bound to the attempt."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'fold-dual.sqlite3'
        self.now = 100.0
        self.store = Store(self.path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, 'wecom', 'bot', {'alice'})
        self.gadget = FakeGadgetClient()
        self.native_client, self.native_state = fake_bridge_client()
        self.dual = DualTrackExecutor(
            self.store,
            primary=GadgetExecutor(self.store, client=self.gadget),
            fallback=MuseExecutor(self.store, client_factory=lambda: self.native_client),
        )

    def test_fold_routes_to_gadget_when_bound(self):
        first = self.ingress.receive('m-a', 'alice', 'chat1', 'single', 'first')
        runner = CoreRunner(self.store, self.dual, 'worker')
        self.assertEqual(runner.tick(), 'accepted')
        self.assertEqual(self.dual.current_track(
            self.store.connection.execute(
                "SELECT attempt_id FROM tasks WHERE id=?", (first.task_id,)).fetchone()['attempt_id']), 'gadget')
        claim = self.store.connection.execute(
            "SELECT * FROM tasks WHERE id=?", (first.task_id,)).fetchone()
        from mobile_channels.models import Claim
        real_claim = Claim(claim['id'], claim['conversation_id'], claim['generation'],
                           claim['owner'], claim['attempt_id'], claim['token'])
        context = CoreRunner(self.store, self.dual, 'worker')._context(real_claim)
        run_id = self.store.connection.execute(
            "SELECT run_id FROM execution_attempts WHERE task_id=?",
            (first.task_id,)).fetchone()['run_id']
        outcome = self.dual.fold(context, run_id, 'second')
        self.assertEqual(outcome.state, 'accepted')
        self.assertEqual(len(self.gadget.sent_only), 1)
        # The native track was never touched: submit went gadget, fold went gadget.
        self.assertEqual(self.native_client._open.call_count, 0)

    def test_reply_started_unknown_without_binding(self):
        first = self.ingress.receive('m-a', 'alice', 'chat1', 'single', 'first')
        from mobile_channels.models import Claim
        claim = Claim(first.task_id, 'conv', 0, 'worker', 'no-attempt', 0)
        context = CoreRunner(self.store, self.dual, 'worker')._context(claim)
        # No track binding: fail safe, treated as started.
        self.assertTrue(self.dual.reply_started(context, None))


if __name__ == '__main__':
    unittest.main()
