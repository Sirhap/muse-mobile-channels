"""Repro/fix tests: inbound media into the executors (item 6) + outcome media.

Item 6 (P0-5 executor side): another agent adds ExecutionContext.media
(list[dict], same shape as the outcome media below); this file attaches it
with object.__setattr__ until that field lands, and asserts the executor
contract:

- MuseExecutor.submit sends image media with the text to the Native bridge
  (chat.stream items, image schema copied from reference/muse-client
  message.ts chatRequest; muse-cli chat.py itself only ever sends text
  items, and the Native _open passes the body JSON through verbatim).
- Video/file media on the Native track is NOT silently dropped: submit
  fails explicitly before opening any stream.
- GadgetExecutor.submit never silently drops media either: it fails
  explicitly (the Gadget worker's sendMessage is text-only), and
  DualTrackExecutor routes media-bearing tasks straight to the Native track.
- ExecutionOutcome carries media: list[dict] {kind, path, filename};
  [[FILE:...]] markers are parsed out of text into media, never left in text.
"""
from __future__ import annotations

import base64
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from mobile_channels.executors import DualTrackExecutor, GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.runtime import ExecutionContext, ExecutionOutcome
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


def with_media(ctx, media):
    object.__setattr__(ctx, "media", list(media))
    return ctx


class TestOutcomeMedia(unittest.TestCase):
    def test_outcome_media_defaults_to_empty_list(self):
        out = ExecutionOutcome("succeeded", text="hi")
        self.assertEqual(out.media, [])

    def test_outcome_carries_media(self):
        out = ExecutionOutcome("succeeded", text="hi", media=[
            {"kind": "image", "path": "/tmp/x.png", "filename": "x.png"}])
        self.assertEqual(out.media[0]["kind"], "image")


class TestNativeMediaSubmit(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.png = Path(self.temp_dir.name) / "shot.png"
        self.png.write_bytes(b"\x89PNG\r\n\x1a\nfake-png-bytes")
        self.mp4 = Path(self.temp_dir.name) / "clip.mp4"
        self.mp4.write_bytes(b"fake-video-bytes")

    def _client(self):
        return FakeNativeClient([[], [user_event(1)], [user_event(1)]])

    def test_image_media_sent_with_text_as_stream_items(self):
        client = self._client()
        ctx = with_media(make_context(self.store), [
            {"kind": "image", "path": str(self.png), "filename": "shot.png"}])
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "accepted")
        self.assertEqual(len(client.opened_bodies), 1)
        items = client.opened_bodies[0]["items"]
        self.assertEqual(items[0], {"type": "text", "text": "ping"})
        image_items = [i for i in items if i.get("type") == "image"]
        self.assertEqual(len(image_items), 1)
        self.assertEqual(image_items[0]["mime_type"], "image/png")
        self.assertEqual(image_items[0]["filename"], "shot.png")
        self.assertEqual(base64.b64decode(image_items[0]["data_base64"]),
                         b"\x89PNG\r\n\x1a\nfake-png-bytes")

    def test_video_media_fails_explicitly_without_opening_stream(self):
        client = self._client()
        ctx = with_media(make_context(self.store), [
            {"kind": "video", "path": str(self.mp4), "filename": "clip.mp4"}])
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "failed")
        self.assertIn("video", out.evidence or "")
        self.assertEqual(client.opened_bodies, [])

    def test_missing_image_file_fails_explicitly(self):
        client = self._client()
        ctx = with_media(make_context(self.store), [
            {"kind": "image", "path": str(Path(self.temp_dir.name) / "gone.png"),
             "filename": "gone.png"}])
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "failed")
        self.assertEqual(client.opened_bodies, [])

    def test_no_media_sends_text_only_as_before(self):
        client = self._client()
        ctx = make_context(self.store)
        out = MuseExecutor(self.store, client_factory=lambda: client).submit(ctx)
        self.assertEqual(out.state, "accepted")
        self.assertEqual(client.opened_bodies[0]["items"],
                         [{"type": "text", "text": "ping"}])


class TestGadgetMediaRouting(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.png = Path(self.temp_dir.name) / "shot.png"
        self.png.write_bytes(b"\x89PNG\r\n\x1a\nfake-png-bytes")

    def test_gadget_submit_with_media_fails_explicitly_and_spawns_nothing(self):
        fake = FakeWorkerClient()
        ex = GadgetExecutor(self.store, client=fake)
        ctx = with_media(make_context(self.store), [
            {"kind": "image", "path": str(self.png), "filename": "shot.png"}])
        out = ex.submit(ctx)
        self.assertEqual(out.state, "failed")
        self.assertIn("gadget_media", out.evidence or "")
        self.assertEqual(fake.started, [])

    def test_dual_track_routes_media_tasks_straight_to_native(self):
        fake = FakeWorkerClient()
        primary = GadgetExecutor(self.store, client=fake)
        native_client = FakeNativeClient([[], [user_event(1)], [user_event(1)]])
        fallback = MuseExecutor(self.store, client_factory=lambda: native_client)
        dual = DualTrackExecutor(self.store, primary=primary, fallback=fallback)
        ctx = with_media(make_context(self.store), [
            {"kind": "image", "path": str(self.png), "filename": "shot.png"}])
        out = dual.submit(ctx)
        self.assertEqual(out.state, "accepted")
        self.assertEqual(fake.started, [])
        self.assertEqual(len(native_client.opened_bodies), 1)
        items = native_client.opened_bodies[0]["items"]
        self.assertTrue(any(i.get("type") == "image" for i in items))


if __name__ == "__main__":
    unittest.main()
