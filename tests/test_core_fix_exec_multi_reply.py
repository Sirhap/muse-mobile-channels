"""Repro/fix tests: executor collects ALL completed assistant messages in a turn.

Item 1 (P0-6 query part): MuseExecutor.query returned on the FIRST completed
assistant message, so a [[FILE:...]] video reference and link sitting in the
SECOND assistant message of the same turn were silently dropped (proven live:
executor settled 1.44s before the second message landed). GadgetExecutor.query
had the same single-message shape via the worker's last-reply-only result.

Item 3 (executor side): bare `unknown` outcomes with no evidence, and a dead
worker whose result file still says 'running' was reported 'running' forever.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mobile_channels.executors import GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.runtime import ExecutionContext, ExecutionOutcome
from mobile_channels.storage import Store


def make_context(store, ingress_owner="worker_1"):
    claim = store.claim(ingress_owner, concurrency=1, lease_seconds=60)
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


class FakeNativeClient:
    """Scripted gateway: history snapshots are consumed one per chat.history call."""

    def __init__(self, history_script):
        # history_script: list of event-lists; each chat.history call pops the
        # next snapshot, then keeps returning the last one.
        self._script = [list(s) for s in history_script]
        self._idx = 0
        self.opened_bodies = []
        self.closed = False

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
        body = kwargs.get("body", {}) or {}
        self.opened_bodies.append(body)
        return 7

    def close(self):
        self.closed = True


def assistant(seq, text, status="completed"):
    return {"event_name": "message.assistant", "seq": seq,
            "payload": {"status": status, "display_text": text}}


def user_event(seq):
    return {"event_name": "message.user", "seq": seq, "payload": {"content": "ping"}}


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


class TestNativeMultiReply(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.video = Path(self.temp_dir.name) / "clip.mp4"
        self.video.write_bytes(b"fake-video-bytes")

    def _submit(self, client):
        ctx = make_context(self.store)
        ex = MuseExecutor(self.store, client_factory=lambda: client)
        outcome = ex.submit(ctx)
        self.assertEqual(outcome.state, "accepted")
        return ctx, ex

    def test_query_merges_all_completed_messages_and_media(self):
        """Two completed assistant messages: text merged, [[FILE:]] -> media, marker gone."""
        marker = f"[[FILE:{self.video}]]"
        client = FakeNativeClient([
            [],  # baseline
            [user_event(1)],  # confirm recorded
            [user_event(1), assistant(2, "第一段"), assistant(3, f"{marker}\nhttps://example.invalid/v")],
            [user_event(1), assistant(2, "第一段"), assistant(3, f"{marker}\nhttps://example.invalid/v")],
        ])
        ctx, ex = self._submit(client)
        out = ex.query(ctx, "stream:7")
        self.assertEqual(out.state, "succeeded")
        self.assertIn("第一段", out.text)
        self.assertIn("https://example.invalid/v", out.text)
        self.assertNotIn("[[FILE:", out.text)
        self.assertEqual(out.media, [{"kind": "video", "path": str(self.video), "filename": "clip.mp4"}])

    def test_query_waits_one_more_poll_cycle_for_a_late_second_message(self):
        """First fetch shows only message 1; a second message arrives one poll
        cycle later. The query must still merge it instead of settling early."""
        marker = f"[[FILE:{self.video}]]"
        client = FakeNativeClient([
            [],
            [user_event(1)],
            [user_event(1), assistant(2, "第一段")],
            [user_event(1), assistant(2, "第一段"), assistant(3, marker)],
            [user_event(1), assistant(2, "第一段"), assistant(3, marker)],
        ])
        ctx, ex = self._submit(client)
        out = ex.query(ctx, "stream:7")
        self.assertEqual(out.state, "succeeded")
        self.assertIn("第一段", out.text)
        self.assertEqual(len(out.media), 1)
        self.assertEqual(out.media[0]["kind"], "video")

    def test_query_single_message_still_succeeds_without_extra_wait_forever(self):
        client = FakeNativeClient([
            [],
            [user_event(1)],
            [user_event(1), assistant(2, "only one")],
            [user_event(1), assistant(2, "only one")],
        ])
        ctx, ex = self._submit(client)
        out = ex.query(ctx, "stream:7")
        self.assertEqual(out.state, "succeeded")
        self.assertEqual(out.text, "only one")
        self.assertEqual(out.media, [])

    def test_query_keeps_polling_through_one_quiet_round(self):
        """The second part may arrive between 1s and 2s after the first."""
        user = user_event(1)
        first = assistant(2, "first part")
        client = FakeNativeClient([
            [], [user], [user, first], [user, first],
            [user, first, assistant(3, f"[[FILE:{self.video}]]")],
        ])
        ctx, ex = self._submit(client)
        with patch("mobile_channels.executors.time.sleep"):
            out = ex.query(ctx, "stream:7")
        self.assertEqual(out.state, "succeeded")
        self.assertEqual(out.media[0]["path"], str(self.video))

    def test_query_succeeds_with_media_only(self):
        """An attachment is a deliverable reply even when it has no caption."""
        user = user_event(1)
        client = FakeNativeClient([
            [], [user], [user, assistant(2, f"[[FILE:{self.video}]]")],
        ])
        ctx, ex = self._submit(client)
        with patch("mobile_channels.executors.time.sleep"):
            out = ex.query(ctx, "stream:7")
        self.assertEqual((out.state, out.text), ("succeeded", ""))
        self.assertEqual(out.media[0]["path"], str(self.video))

    def test_completed_reply_retains_history_identity_for_late_followups(self):
        """The reply cursor identifies already-delivered messages after restart."""
        user = user_event(10)
        user["message_id"] = "u-current"
        first = assistant(11, "first answer")
        first["message_id"] = "a-first"
        client = FakeNativeClient([[], [user], [user, first]])
        ctx, ex = self._submit(client)
        with patch("mobile_channels.executors.time.sleep"):
            out = ex.query(ctx, "stream:7")
        self.assertEqual(out.reply_watch["ack_id"], "u-current")
        self.assertEqual(out.reply_watch["seen_ids"], ["a-first"])
        self.assertEqual(out.reply_watch["session_id"], "sess-1")

    def test_query_does_not_finish_while_another_segment_is_active(self):
        """History still exposes ongoing reply generation after the first part."""
        user = user_event(1)
        events = [user, assistant(2, "first"), assistant(3, "working", "running")]
        client = FakeNativeClient([[], [user], events])
        ctx, ex = self._submit(client)
        with patch("mobile_channels.executors.time.sleep"):
            self.assertEqual(ex.query(ctx, "stream:7").state, "running")


class TestUnknownEvidence(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")

    def test_native_submit_without_run_id_is_unknown_with_evidence(self):
        """_open returning no run id: unknown must carry evidence, never bare."""
        client = Mock()
        client.call_json.return_value = {"session_id": "sess-1"}
        client._open.return_value = None
        ctx = make_context(self.store)
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "unknown")
        self.assertTrue(out.evidence, "unknown without evidence is uncountable")

    def test_native_submit_exception_carries_evidence(self):
        """A failure before opening the stream proves no user task was sent."""
        client = Mock()
        client.call_json.side_effect = RuntimeError("boom-history")
        ctx = make_context(self.store)
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "not_submitted")
        self.assertEqual(out.evidence, "submit_failed:RuntimeError")

    def test_gadget_dead_worker_with_running_placeholder_is_failed_not_running(self):
        """Worker died mid-turn (result file still 'running', has message_id):
        the turn determinably ends -- failed, not 'running' forever."""
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        out = ex.submit(ctx)
        self.assertEqual(out.state, "accepted")
        fake.results[ctx.claim.attempt_id] = {"state": "running", "message_id": "m-1"}
        fake.alive.discard(1001)
        q = ex.query(ctx, "worker:1001")
        self.assertEqual(q.state, "failed")
        self.assertIn("worker_died_mid_turn", q.evidence or "")
        self.assertEqual(q.message_id, "m-1")

    def test_gadget_dead_worker_without_any_result_is_unknown_with_poll_evidence(self):
        """No result file at all: send state is genuinely unknowable -- unknown,
        but the evidence must carry the poll count for upstream counting."""
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        ex.submit(ctx)
        fake.alive.discard(1001)
        q = ex.query(ctx, "worker:1001")
        self.assertEqual(q.state, "unknown")
        self.assertIn("polls=", q.evidence or "")


class TestGadgetQueryMedia(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.video = Path(self.temp_dir.name) / "clip.mp4"
        self.video.write_bytes(b"fake-video-bytes")

    def test_gadget_succeeded_result_media_survives_and_markers_stripped(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        ex.submit(ctx)
        fake.results[ctx.claim.attempt_id] = {
            "state": "succeeded",
            "text": f"第一段\n[[FILE:{self.video}]]\nhttps://example.invalid/v",
            "message_id": "m-1",
            "media": [{"kind": "video", "path": str(self.video), "filename": "clip.mp4"}],
        }
        q = ex.query(ctx, "worker:1001")
        self.assertEqual(q.state, "succeeded")
        self.assertNotIn("[[FILE:", q.text)
        self.assertIn("第一段", q.text)
        self.assertEqual(q.media, [{"kind": "video", "path": str(self.video), "filename": "clip.mp4"}])

    def test_gadget_succeeded_result_accepts_media_without_caption(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = make_context(self.store)
        submitted = ex.submit(ctx)
        fake.results[ctx.claim.attempt_id] = {
            "state": "succeeded", "text": "", "message_id": "m-1",
            "media": [{"kind": "video", "path": str(self.video), "filename": "clip.mp4"}],
        }
        q = ex.query(ctx, submitted.run_id)
        self.assertEqual((q.state, q.text), ("succeeded", ""))
        self.assertEqual(q.media[0]["path"], str(self.video))


if __name__ == "__main__":
    unittest.main()
