"""Repro/fix tests: Gadget cancel honesty (item 2) and rebind failure (item 5).

Item 2 (P0-4): GadgetExecutor.cancel only SIGTERMs the local worker and then
reported 'cancelled' / 'worker_terminated' -- as if the server turn had
stopped. The reference client has no server-side cancel endpoint (checked
reference/muse-client/src/client.ts + connection.ts: only a local
AbortSignal on the Noise request), so the outcome must say the local worker
is gone while the server turn may continue.

Item 5 (P2-17): DualTrackExecutor.query called Store.rebind_run outside any
try; a StateError there bubbled up and the task wedged in unknown forever.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.executors import DualTrackExecutor, GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.models import StateError
from mobile_channels.runtime import CoreRunner, ExecutionContext
from mobile_channels.storage import Store


def make_context(store, owner="worker_1"):
    claim = store.claim(owner, concurrency=1, lease_seconds=60)
    assert claim is not None
    store.begin_submission(claim)
    row = store.connection.execute(
        "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
        "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
        "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
        (claim.task_id,),
    ).fetchone()
    return ExecutionContext(
        claim=claim, text=row["text"], external_message_id=row["external_id"],
        sender_id=row["sender_id"], channel=row["channel"], account_id=row["account_id"],
        chat_type=row["chat_type"], chat_id=row["chat_id"],
    )


class FakeWorkerClient:
    def __init__(self):
        self.results = {}
        self.alive = set()
        self.started = []
        self.stopped = []
        self._next_pid = 1000

    def is_configured(self):
        return True

    def start_turn(self, attempt_id, session_id, text, timeout_seconds=None):
        self._next_pid += 1
        self.alive.add(self._next_pid)
        self.started.append((attempt_id, session_id, text))

        class P:
            def __init__(self, pid):
                self.pid = pid

        return P(self._next_pid)

    def read_result(self, attempt_id):
        return self.results.get(attempt_id)

    def worker_alive(self, pid):
        return pid in self.alive

    def stop_turn(self, proc, *, wait_seconds=5.0):
        pid = proc.pid if hasattr(proc, "pid") else int(proc)
        self.stopped.append(pid)
        self.alive.discard(pid)
        return True

    def close(self):
        pass


class FakeNativeClient:
    def __init__(self, history_script):
        self._script = [list(s) for s in history_script]
        self._idx = 0
        self.opened_bodies = []

    def call_json(self, method, body=None, **kwargs):
        if method == "session.start":
            return {"session_id": "sess-1"}
        if self._idx < len(self._script):
            events = self._script[self._idx]
            self._idx += 1
        else:
            events = self._script[-1]
        return {"chat_events": [dict(e) for e in events]}

    def _open(self, *args, **kwargs):
        self.opened_bodies.append(kwargs.get("body", {}) or {})
        return 7

    def close(self):
        pass


def user_event(seq):
    return {"event_name": "message.user", "seq": seq, "payload": {"content": "ping"}}


class TestGadgetCancelHonesty(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")

    def test_cancel_reports_local_only_server_turn_may_continue(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        self.assertEqual(ex.submit(ctx).state, "accepted")
        out = ex.cancel(ctx, "worker:1001")
        self.assertEqual(out.state, "cancelled")
        self.assertIn("server_turn_may_continue", out.evidence or "")
        self.assertEqual(fake.stopped, [1001])

    def test_cancel_with_worker_cancelled_result_keeps_worker_evidence(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        ex.submit(ctx)
        fake.results[ctx.claim.attempt_id] = {
            "state": "cancelled",
            "evidence": "worker_terminated_local_only_server_turn_may_continue",
        }
        out = ex.cancel(ctx, "worker:1001")
        self.assertEqual(out.state, "cancelled")
        self.assertIn("server_turn_may_continue", out.evidence or "")

    def test_cancel_does_not_confirm_when_worker_is_still_alive(self):
        fake = FakeWorkerClient()
        fake.stop_turn = Mock(return_value=False)
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        submitted = ex.submit(ctx)
        self.assertEqual(ex.cancel(ctx, submitted.run_id).state, "unknown")

    def test_local_only_cancel_receipt_explains_server_may_continue(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        submitted = ex.submit(ctx)
        self.store.mark_submitted(ctx.claim, submitted.run_id)
        self.store.request_cancel(ctx.claim.task_id, ctx.claim.conversation_id)
        runner = CoreRunner(self.store, ex, "worker_1")
        self.assertEqual(runner.tick(), "cancelled")
        receipt = self.store.connection.execute("SELECT text FROM task_receipts").fetchone()
        self.assertIn("Muse 端可能继续执行", receipt["text"])


class TestRebindFailure(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")

    def _dual_with_gadget_failed(self):
        fake = FakeWorkerClient()
        primary = GadgetExecutor(self.store, client=fake)
        native_client = FakeNativeClient([
            [], [user_event(1)],
            [user_event(1)],
        ])
        fallback = MuseExecutor(self.store, client_factory=lambda: native_client)
        dual = DualTrackExecutor(self.store, primary=primary, fallback=fallback)
        ctx = make_context(self.store)
        self.assertEqual(primary.submit(ctx).state, "accepted")
        fake.results[ctx.claim.attempt_id] = {"state": "failed", "evidence": "boom"}
        return dual, ctx, fake

    def test_rebind_state_error_preserves_native_run_for_recovery(self):
        """A local rebind error cannot invalidate the accepted Native run."""
        dual, ctx, _ = self._dual_with_gadget_failed()
        self.store.mark_submitted(ctx.claim, "worker:1001")
        real_rebind = self.store.rebind_run

        def _boom(claim, run_id):
            raise StateError("run can only be rebound for a running task")

        self.store.rebind_run = _boom
        try:
            out = dual.query(ctx, "worker:1001")
        finally:
            self.store.rebind_run = real_rebind
        self.assertEqual(out.state, "unknown")
        self.assertIn("rebind", (out.evidence or "").lower())
        self.assertEqual(dual.query(ctx, "worker:1001").state, "running")

    def test_fallback_submit_exception_becomes_failed_not_exception(self):
        """A fallback submit that raises must also surface as failed + evidence."""
        dual, ctx, _ = self._dual_with_gadget_failed()
        dual.fallback.submit = Mock(side_effect=RuntimeError("native exploded"))
        out = dual.query(ctx, "worker:1001")
        self.assertEqual(out.state, "failed")
        self.assertIn("fallback_submit_failed", out.evidence or "")

    def test_recover_native_run_after_ownership_expires_during_rebind(self):
        """A confirmed fallback already has a durable run and must not resend."""
        now = [1000.0]
        self.store.clock = lambda: now[0]
        dual, ctx, _ = self._dual_with_gadget_failed()
        self.store.mark_submitted(ctx.claim, "worker:1001")
        submit = dual.fallback.submit
        def submit_then_expire(context):
            outcome = submit(context)
            now[0] += 61
            return outcome
        dual.fallback.submit = Mock(side_effect=submit_then_expire)
        runner = CoreRunner(self.store, dual, "worker_1")
        self.assertEqual(runner.tick(), "ownership_lost")
        self.assertEqual(runner.tick(), "running")
        self.assertEqual(dual.fallback.submit.call_count, 1)
        run_id = self.store.connection.execute("SELECT run_id FROM execution_attempts").fetchone()["run_id"]
        self.assertEqual(run_id, "stream:7")


if __name__ == "__main__":
    unittest.main()
