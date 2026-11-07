"""Gadget worker-transport executor tests: pairing directory, worker lifecycle, dual track.

The Gadget track speaks the real Noise protocol through a short-lived worker
process (mobile_channels/gadget_worker/turn.ts), not the HTTP API the old
client pretended existed. These tests cover, with the worker seam faked or
stubbed (no network, no real pairing):

- GadgetCredentials: pairing-directory completeness (three files, 0600 by handover);
- GadgetClient: spawning the worker, reading its result file, killing it, and
  the guarantee that credential values never appear in the worker argv;
- GadgetExecutor: submit/query/cancel lifecycle, persisted gadget_sessions,
  placeholder-row cleanup only when nothing was sent, unknown on ambiguity;
- CircuitBreaker state transitions (unchanged);
- DualTrackExecutor: default gadget route, credential-missing fallback,
  upstream-error circuit trip, strict per-track query/cancel binding;
- CoreRunner end-to-end: tick() drives claimed -> running -> succeeded;
- Schema: legacy executor_runs gains track/baseline_seq; gadget_sessions is created.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.executors import (
    CircuitBreaker,
    DualTrackExecutor,
    GadgetExecutor,
    MuseExecutor,
    bridge_client,
    ensure_executor_schema,
)
from mobile_channels.gadget_client import (
    GadgetAuthError,
    GadgetClient,
    GadgetCredentials,
    GadgetError,
    GadgetNetworkError,
)
from mobile_channels.ingress import CoreIngress
from mobile_channels.models import Claim  # noqa: F401  (kept for parity with the old suite)
from mobile_channels.runtime import CoreRunner, ExecutionContext, ExecutionOutcome
from mobile_channels.storage import Store


STUB_WORKER = '''#!/usr/bin/env python3
"""Stub Gadget worker for tests: records argv, writes a canned result."""
import json
import os
import sys
import time


def main():
    args = sys.argv[1:]
    vals = {}
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            vals[args[i][2:]] = args[i + 1]
            i += 2
        else:
            i += 1
    out = vals["out"]
    with open(out + ".argv.json", "w", encoding="utf-8") as handle:
        json.dump(sys.argv, handle)
    if os.environ.get("GADGET_STUB_MODE", "succeed") == "sleep":
        time.sleep(120)
        return 0
    result = {"state": "succeeded", "text": "stub reply",
              "message_id": "stub-msg-1", "session_id": vals.get("session")}
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(result, handle)
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


class FakeProc:
    """Stand-in for subprocess.Popen: only the pid matters to the executor."""

    def __init__(self, pid: int):
        self.pid = pid


class FakeWorkerClient:
    """In-memory stand-in for GadgetClient: no processes, results driven by the test."""

    def __init__(self, configured: bool = True):
        self.configured = configured
        self.started: list[tuple[str, str, str]] = []
        self.results: dict[str, dict] = {}
        self.stopped: list[int] = []
        self.alive: set[int] = set()
        self.start_error: Exception | None = None
        self._next_pid = 1000

    def is_configured(self) -> bool:
        return self.configured

    def start_turn(self, attempt_id: str, session_id: str, text: str, timeout_seconds=None):
        if self.start_error is not None:
            raise self.start_error
        self._next_pid += 1
        pid = self._next_pid
        self.started.append((attempt_id, session_id, text))
        self.alive.add(pid)
        return FakeProc(pid)

    def read_result(self, attempt_id: str):
        return self.results.get(attempt_id)

    def result_path(self, attempt_id: str) -> Path:
        return Path(f"/nonexistent/gadget-{attempt_id}.json")

    def worker_alive(self, pid: int) -> bool:
        return pid in self.alive

    def stop_turn(self, proc, *, wait_seconds: float = 5.0) -> bool:
        pid = proc.pid if hasattr(proc, "pid") else int(proc)
        self.stopped.append(pid)
        self.alive.discard(pid)
        return True

    def close(self) -> None:
        pass


def write_pairing(directory: Path, token: str = "tok_test_secret_123") -> None:
    """Create the three pairing files the reference client reads."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "identity.json").write_text('{"device": "test"}', encoding="utf-8")
    (directory / "pairing.json").write_text('{"devices": []}', encoding="utf-8")
    (directory / "sdk_token").write_text(token, encoding="utf-8")


class TestGadgetCredentials(unittest.TestCase):
    """Pairing-directory completeness: three files, nothing else is trusted."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.base = Path(self.temp_dir.name)

    def test_default_directory_unconfigured_when_missing(self):
        """A directory that does not exist is not configured."""
        creds = GadgetCredentials(directory=self.base / "no-such-dir")
        self.assertFalse(creds.is_configured())

    def test_complete_directory_is_configured(self):
        """All three pairing files present means configured."""
        pairing = self.base / "pair"
        write_pairing(pairing)
        self.assertTrue(GadgetCredentials(directory=pairing).is_configured())

    def test_missing_one_file_is_not_configured(self):
        """Two of three files is not enough: the client cannot authenticate."""
        pairing = self.base / "pair"
        write_pairing(pairing)
        (pairing / "sdk_token").unlink()
        self.assertFalse(GadgetCredentials(directory=pairing).is_configured())

    def test_directory_pointing_at_a_file_is_not_configured(self):
        """A path that is a file, not a directory, is not configured and never raises."""
        target = self.base / "not-a-dir"
        target.write_text("x", encoding="utf-8")
        self.assertFalse(GadgetCredentials(directory=target).is_configured())

    def test_default_directory_is_the_installed_pairing(self):
        """The default points at the installed pairing directory, not a JSON config."""
        creds = GadgetCredentials.default()
        self.assertEqual(creds.directory, Path.home() / ".config" / "MuseGadgetPair")


class TestGadgetClient(unittest.TestCase):
    """Real GadgetClient against a stub worker script: spawn, read, kill, argv hygiene."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.base = Path(self.temp_dir.name)
        self.pairing = self.base / "pair"
        write_pairing(self.pairing)
        self.run_dir = self.base / "runs"
        self.stub = self.base / "stub_worker.py"
        self.stub.write_text(STUB_WORKER, encoding="utf-8")
        self.client = GadgetClient(
            credentials=GadgetCredentials(directory=self.pairing),
            run_dir=self.run_dir,
            worker_script=self.stub,
            runner=sys.executable,
        )
        self.addCleanup(self.client.close)
        old_mode = os.environ.pop("GADGET_STUB_MODE", None)

        def _restore_stub_mode():
            if old_mode is None:
                os.environ.pop("GADGET_STUB_MODE", None)
            else:
                os.environ["GADGET_STUB_MODE"] = old_mode

        self.addCleanup(_restore_stub_mode)

    def test_start_turn_writes_result_and_message_file(self):
        """The stub worker runs, writes its result, and the client reads it back."""
        proc = self.client.start_turn("att_1", "sess_1", "hello gadget")
        self.assertIsInstance(proc, subprocess.Popen)
        deadline = time.time() + 10
        result = None
        while time.time() < deadline:
            result = self.client.read_result("att_1")
            if result is not None:
                break
            time.sleep(0.1)
        proc.wait(timeout=10)
        self.assertEqual(result, {"state": "succeeded", "text": "stub reply",
                                  "message_id": "stub-msg-1", "session_id": "sess_1"})
        message_path = self.run_dir / "gadget-att_1.msg"
        self.assertEqual(message_path.read_text(encoding="utf-8"), "hello gadget")

    def test_worker_argv_never_carries_credential_values(self):
        """The spawn argv holds the pairing directory path, never the token value."""
        proc = self.client.start_turn("att_2", "sess_2", "secret check")
        proc.wait(timeout=10)
        argv = json.loads((self.run_dir / "gadget-att_2.json.argv.json").read_text(encoding="utf-8"))
        joined = " ".join(argv)
        self.assertIn(str(self.pairing), joined)
        self.assertNotIn("tok_test_secret_123", joined)
        # The message travels by file, not inline in argv.
        self.assertNotIn("secret check", joined)
        self.assertIn("--message-file", argv)

    def test_start_turn_without_credentials_raises_auth(self):
        """A missing pairing directory fails fast as an auth error, before spawning."""
        empty = GadgetClient(
            credentials=GadgetCredentials(directory=self.base / "empty"),
            run_dir=self.run_dir,
            worker_script=self.stub,
            runner=sys.executable,
        )
        with self.assertRaises(GadgetAuthError):
            empty.start_turn("att_3", "sess_3", "hi")

    def test_start_turn_with_missing_runner_raises_network(self):
        """A worker that cannot start is a network/transport error, not a crash."""
        broken = GadgetClient(
            credentials=GadgetCredentials(directory=self.pairing),
            run_dir=self.run_dir,
            worker_script=self.stub,
            runner="/nonexistent/bun",
        )
        with self.assertRaises(GadgetNetworkError):
            broken.start_turn("att_4", "sess_4", "hi")

    def test_stop_turn_kills_sleeping_worker(self):
        """A live worker is SIGTERMed as a group and reported gone."""
        os.environ["GADGET_STUB_MODE"] = "sleep"
        proc = self.client.start_turn("att_5", "sess_5", "hi")
        self.assertTrue(self.client.worker_alive(proc.pid))
        self.assertTrue(self.client.stop_turn(proc))
        self.assertFalse(self.client.worker_alive(proc.pid))
        proc.wait(timeout=10)

    def test_read_result_missing_and_corrupt_return_none(self):
        """No file yet, or an unreadable file, both read as None (still running)."""
        self.assertIsNone(self.client.read_result("att_missing"))
        bad = self.run_dir / "gadget-att_bad.json"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        bad.write_text("{not json", encoding="utf-8")
        self.assertIsNone(self.client.read_result("att_bad"))

    def test_close_marks_closed(self):
        """After close(), start_turn refuses to spawn."""
        self.client.close()
        with self.assertRaises(GadgetError):
            self.client.start_turn("att_6", "sess_6", "hi")


class TestGadgetExecutor(unittest.TestCase):
    """GadgetExecutor lifecycle against the fake worker client."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.fake_client = FakeWorkerClient(configured=True)
        self.executor = GadgetExecutor(self.store, client=self.fake_client)

    def _get_context(self) -> ExecutionContext:
        claim = self.store.claim("worker_1", concurrency=1, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_unconfigured_credentials_returns_not_submitted(self):
        """When the pairing is missing, submit returns not_submitted and writes nothing."""
        executor = GadgetExecutor(self.store, client=FakeWorkerClient(configured=False))
        context = self._get_context()
        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "gadget_credentials_missing")
        runs = self.store.connection.execute("SELECT * FROM executor_runs").fetchall()
        self.assertEqual(len(runs), 0)

    def test_full_lifecycle_submit_query_cancel(self):
        """submit -> query(running) -> query(succeeded) -> cancel, with a persisted session."""
        context = self._get_context()
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "worker:1001")
        self.assertEqual(len(self.fake_client.started), 1)
        attempt_id, session_id, text = self.fake_client.started[0]
        self.assertEqual(attempt_id, context.claim.attempt_id)
        self.assertEqual(text, "ping")

        row = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["track"], "gadget")
        self.assertEqual(row["platform_run_id"], "worker:1001")
        self.assertEqual(row["status"], "accepted")
        self.assertEqual(row["session_id"], session_id)
        saved = self.store.connection.execute(
            "SELECT session_id FROM gadget_sessions WHERE conversation_id=? AND generation=?",
            (context.claim.conversation_id, context.claim.generation),
        ).fetchone()
        self.assertEqual(saved["session_id"], session_id)

        q1 = self.executor.query(context, "worker:1001")
        self.assertEqual(q1.state, "running")

        self.fake_client.results[context.claim.attempt_id] = {
            "state": "succeeded", "text": "pong from gadget", "message_id": "m1",
        }
        q2 = self.executor.query(context, "worker:1001")
        self.assertEqual(q2.state, "succeeded")
        self.assertEqual(q2.text, "pong from gadget")

        c = self.executor.cancel(context, "worker:1001")
        self.assertEqual(c.state, "cancelled")
        self.assertEqual(c.evidence, "local_worker_terminated_server_turn_may_continue")
        self.assertEqual(self.fake_client.stopped, [1001])

    def test_session_reused_within_generation_and_split_by_new_generation(self):
        """One session per (conversation, generation); /new gets a new session."""
        from dataclasses import replace

        context = self._get_context()
        first = self.executor._session(context)
        self.assertEqual(self.executor._session(context), first)

        gen1 = ExecutionContext(
            claim=replace(context.claim, generation=1),
            text=context.text,
            external_message_id=context.external_message_id,
            sender_id=context.sender_id,
            channel=context.channel,
            account_id=context.account_id,
            chat_type=context.chat_type,
            chat_id=context.chat_id,
        )
        second = self.executor._session(gen1)
        self.assertNotEqual(second, first)
        self.assertEqual(self.executor._session(gen1), second)

    def test_spawn_failure_drops_placeholder_and_returns_unknown(self):
        """A worker that never started sent nothing: unknown, and no placeholder row."""
        self.fake_client.start_error = GadgetNetworkError("cannot start Gadget worker: no bun")
        context = self._get_context()
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "unknown")
        self.assertIn("cannot start Gadget worker", outcome.evidence)
        rows = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_query_worker_died_without_result_is_unknown(self):
        """A dead worker with no result file is unknown, never a guessed failure."""
        context = self._get_context()
        self.executor.submit(context)
        self.fake_client.alive.discard(1001)
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "unknown")
        self.assertEqual(q.evidence, "worker_exited_without_result polls=1")

    def test_query_unrecognized_result_state_is_unknown(self):
        """A result file with an unrecognized state is unknown, not running."""
        context = self._get_context()
        self.executor.submit(context)
        self.fake_client.results[context.claim.attempt_id] = {"state": "alien_unrecognized_status"}
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "unknown")
        self.assertEqual(q.evidence, "unrecognized_result_state_alien_unrecognized_status polls=1")

    def test_query_failed_result_maps_evidence(self):
        """A worker-reported failure surfaces its evidence, not a generic error."""
        context = self._get_context()
        self.executor.submit(context)
        self.fake_client.results[context.claim.attempt_id] = {
            "state": "failed", "evidence": "Muse /chat/stream returned HTTP 500",
        }
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "failed")
        self.assertEqual(q.evidence, "Muse /chat/stream returned HTTP 500")


class TestCircuitBreaker(unittest.TestCase):
    """Use case 4: CircuitBreaker state transitions (success reset, trip, half-open)."""

    def setUp(self):
        self.now = 1000.0

    def test_circuit_state_transitions(self):
        cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=30.0, clock=lambda: self.now)

        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        cb.record_failure()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        cb.record_failure()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        cb.record_success()
        self.assertEqual(cb.status, "CLOSED")
        self.assertEqual(cb.failure_count, 0)

        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        self.now += 20.0
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        self.now += 11.0
        self.assertEqual(cb.status, "HALF_OPEN")
        self.assertTrue(cb.can_execute())

        cb.record_success()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.now += 31.0
        self.assertEqual(cb.status, "HALF_OPEN")
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())


class TestDualTrackExecutor(unittest.TestCase):
    """Use cases 5-8: DualTrackExecutor default route, fallback, circuit trip, binding."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "dual.sqlite3"
        self.now = 100.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})

        self.primary = Mock(spec=GadgetExecutor)
        self.primary.is_configured.return_value = True

        self.fallback = Mock(spec=MuseExecutor)

        self.circuit = CircuitBreaker(failure_threshold=3, cooldown_seconds=30.0, clock=lambda: self.now)

        self.dual_executor = DualTrackExecutor(
            store=self.store,
            primary=self.primary,
            fallback=self.fallback,
            circuit=self.circuit,
        )

    def _create_context(self, msg_id="m1", chat_id=None) -> ExecutionContext:
        chat = chat_id or f"chat_{msg_id}"
        self.ingress.receive(msg_id, "user_1", chat, "single", f"text for {msg_id}")
        claim = self.store.claim("worker_1", concurrency=10, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_use_case_5_default_gadget_track(self):
        """Use case 5: DualTrackExecutor defaults to the Gadget track."""
        context = self._create_context("m_primary")
        self.primary.submit.return_value = ExecutionOutcome("accepted", run_id="gadget_run_1")

        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                (context.claim.attempt_id, "s1", "gadget_run_1", "accepted", self.now, "gadget"),
            )

        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "gadget_run_1")
        self.primary.submit.assert_called_once_with(context)
        self.fallback.submit.assert_not_called()
        self.assertEqual(self.dual_executor.current_track(context.claim.attempt_id), "gadget")

    def test_use_case_6_credentials_missing_fallback_to_native(self):
        """Use case 6: missing pairing falls back to the Native track transparently."""
        self.primary.is_configured.return_value = False
        context = self._create_context("m_unconf")
        self.fallback.submit.return_value = ExecutionOutcome("accepted", run_id="native_run_1")

        def fallback_submit_side_effect(ctx):
            with self.store.transaction():
                self.store.connection.execute(
                    "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                    (ctx.claim.attempt_id, "s_nat", "native_run_1", "accepted", self.now, "native"),
                )
            return ExecutionOutcome("accepted", run_id="native_run_1")

        self.fallback.submit.side_effect = fallback_submit_side_effect

        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "native_run_1")
        self.primary.submit.assert_not_called()
        self.fallback.submit.assert_called_once_with(context)
        self.assertEqual(self.dual_executor.current_track(context.claim.attempt_id), "native")

    def test_use_case_7_upstream_error_trips_circuit_and_falls_back(self):
        """Use case 7: upstream errors trip the circuit and fall back to Native."""
        context1 = self._create_context("m_err1")
        self.primary.submit.side_effect = GadgetNetworkError("Connection refused")

        def fallback_side_effect(ctx):
            with self.store.transaction():
                self.store.connection.execute(
                    "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                    (ctx.claim.attempt_id, "s_nat", f"native_{ctx.claim.attempt_id}", "accepted", self.now, "native"),
                )
            return ExecutionOutcome("accepted", run_id=f"native_{ctx.claim.attempt_id}")

        self.fallback.submit.side_effect = fallback_side_effect

        outcome1 = self.dual_executor.submit(context1)
        self.assertEqual(outcome1.state, "accepted")
        self.assertEqual(self.circuit.failure_count, 1)
        self.fallback.submit.assert_called_once()

        context2 = self._create_context("m_err2")
        self.dual_executor.submit(context2)
        self.assertEqual(self.circuit.failure_count, 2)

        context3 = self._create_context("m_err3")
        self.dual_executor.submit(context3)
        self.assertEqual(self.circuit.failure_count, 3)
        self.assertEqual(self.dual_executor.circuit_status(), "OPEN")

        self.primary.submit.reset_mock()
        context4 = self._create_context("m_err4")
        outcome4 = self.dual_executor.submit(context4)
        self.assertEqual(outcome4.state, "accepted")
        self.primary.submit.assert_not_called()

    def test_use_case_8_query_strictly_bound_to_track(self):
        """Use case 8: query/cancel stay on the bound track even after the circuit opens."""
        context1 = self._create_context("m_task1")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                (context1.claim.attempt_id, "s_gadget", "gadget_run_1", "accepted", self.now, "gadget"),
            )
        self.assertEqual(self.dual_executor.current_track(context1.claim.attempt_id), "gadget")

        context2 = self._create_context("m_task2")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                (context2.claim.attempt_id, "s_native", "native_run_2", "accepted", self.now, "native"),
            )
        self.assertEqual(self.dual_executor.current_track(context2.claim.attempt_id), "native")

        self.circuit.record_failure()
        self.circuit.record_failure()
        self.circuit.record_failure()
        self.assertEqual(self.dual_executor.circuit_status(), "OPEN")

        self.primary.query.return_value = ExecutionOutcome("succeeded", text="gadget answer")
        q1 = self.dual_executor.query(context1, "gadget_run_1")
        self.assertEqual(q1.state, "succeeded")
        self.assertEqual(q1.text, "gadget answer")
        self.primary.query.assert_called_once_with(context1, "gadget_run_1")
        self.fallback.query.assert_not_called()

        self.fallback.query.return_value = ExecutionOutcome("succeeded", text="native answer")
        q2 = self.dual_executor.query(context2, "native_run_2")
        self.assertEqual(q2.state, "succeeded")
        self.assertEqual(q2.text, "native answer")
        self.fallback.query.assert_called_once_with(context2, "native_run_2")

        self.primary.cancel.return_value = ExecutionOutcome("cancelled", evidence="cancelled")
        c1 = self.dual_executor.cancel(context1, "gadget_run_1")
        self.assertEqual(c1.state, "cancelled")
        self.primary.cancel.assert_called_once_with(context1, "gadget_run_1")

    def test_cancel_requested_before_submission_does_not_fallback(self):
        """A cancel requested before submission returns not_submitted without falling back."""
        context = self._create_context("m_cancel_first")
        self.primary.submit.return_value = ExecutionOutcome("not_submitted", evidence="cancelled_before_submission")
        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "cancelled_before_submission")
        self.fallback.submit.assert_not_called()

    def test_query_and_cancel_without_track_binding(self):
        """Query/cancel on an unbound attempt returns unknown with no_track_binding."""
        context = self._create_context("m_no_track")
        q = self.dual_executor.query(context, "run_dummy")
        self.assertEqual(q.state, "unknown")
        self.assertEqual(q.evidence, "no_track_binding")

        c = self.dual_executor.cancel(context, "run_dummy")
        self.assertEqual(c.state, "unknown")
        self.assertEqual(c.evidence, "no_track_binding")


    def test_query_time_fallback_when_gadget_fails_before_send(self):
        """A Gadget turn that fails before its message reaches the session
        falls back to Native instead of failing the task.

        Proves the live WeCom trial failure (task 1: the worker died at
        15s in the token refresh, result carries no message_id) would now
        get a Native reply. Uses the real Native executor (scripted
        bridge), because a Mock fallback hides the runner contract: the
        fallback submit's 'accepted' outcome cannot be returned from
        query -- the CoreRunner state machine binds one run per attempt
        at submit time -- so query rebinds the attempt to the Native run
        and reports 'running' (proven live 2026-11-06: returning
        'accepted' crashed the runner in mark_submitted).
        """
        context = self._create_context("m_fallback_before_send")
        fake = FakeWorkerClient(configured=True)
        primary = GadgetExecutor(self.store, client=fake)
        fallback = MuseExecutor(
            self.store, client_factory=lambda: self._scripted_bridge_client())
        dual_executor = DualTrackExecutor(
            store=self.store, primary=primary, fallback=fallback,
        )
        submitted = dual_executor.submit(context)
        self.assertEqual(submitted.state, "accepted")
        # Mirror what the runner does after a submit: bind the gadget run
        # and move the task to 'running' (the query-phase state).
        self.store.mark_submitted(context.claim, submitted.run_id)
        fake.results[context.claim.attempt_id] = {
            "state": "failed",
            "evidence": "The operation timed out.",
            "session_id": "s1",
        }
        outcome = dual_executor.query(context, submitted.run_id)
        self.assertEqual(outcome.state, "running")
        row = self.store.connection.execute(
            "SELECT status, track, platform_run_id FROM executor_runs WHERE attempt_id=?",
            (context.claim.attempt_id,),
        ).fetchone()
        self.assertEqual((row["status"], row["track"]), ("accepted", "native"))
        attempt = self.store.connection.execute(
            "SELECT run_id FROM execution_attempts WHERE id=?",
            (context.claim.attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["run_id"], row["platform_run_id"])
        # A follow-up query polls the Native run (no reply recorded yet).
        followup = dual_executor.query(context, attempt["run_id"])
        self.assertEqual(followup.state, "running")

    def test_no_fallback_when_gadget_failed_after_send(self):
        """A Gadget turn that failed AFTER its message reached the session
        must not fall back: the reply may exist and a Native retry would
        double-deliver."""
        context = self._create_context("m_no_fallback_after_send")
        fake = FakeWorkerClient(configured=True)
        primary = GadgetExecutor(self.store, client=fake)
        fallback = Mock(spec=MuseExecutor)
        dual_executor = DualTrackExecutor(
            store=self.store, primary=primary, fallback=fallback,
        )
        submitted = dual_executor.submit(context)
        self.assertEqual(submitted.state, "accepted")
        fake.results[context.claim.attempt_id] = {
            "state": "failed",
            "evidence": "subscription_ended_before_reply_completed",
            "message_id": "msg-123",
            "session_id": "s1",
        }
        outcome = dual_executor.query(context, submitted.run_id)
        self.assertEqual(outcome.state, "failed")
        fallback.submit.assert_not_called()
        row = self.store.connection.execute(
            "SELECT status, track FROM executor_runs WHERE attempt_id=?",
            (context.claim.attempt_id,),
        ).fetchone()
        self.assertEqual((row["status"], row["track"]), ("accepted", "gadget"))

    def _scripted_bridge_client(self):
        """Fake native bridge: session.start hands out one id, _open records
        the sent message in history, chat.history returns the event list."""
        state = {"events": [], "seq": 0}
        client = Mock()

        def call_json(method, body=None, **kwargs):
            if method == "session.start":
                return {"session_id": "native-session"}
            return {"chat_events": [dict(e) for e in state["events"]]}

        def _open(*args, **kwargs):
            body = kwargs.get("body", {}) or {}
            items = body.get("items") or []
            state["seq"] += 1
            state["events"].append({
                "event_name": "message.user", "seq": state["seq"],
                "payload": {"content": items[0].get("text", "") if items else ""},
            })
            return 7

        client.call_json.side_effect = call_json
        client._open.side_effect = _open
        client.close.return_value = None
        return client

    def test_native_submit_rebinds_superseded_gadget_row(self):
        """MuseExecutor.submit accepts a failed_superseded gadget row and
        rebinds it to the Native track; a live gadget row still refuses a
        second binding."""
        context = self._create_context("m_rebind")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                (context.claim.attempt_id, "gadget-session", "worker:1001", "failed_superseded", self.now, "gadget"),
            )
        adapter = MuseExecutor(self.store, client_factory=lambda: self._scripted_bridge_client())
        outcome = adapter.submit(context)
        self.assertEqual(outcome.state, "accepted")
        row = self.store.connection.execute(
            "SELECT session_id, track, status FROM executor_runs WHERE attempt_id=?",
            (context.claim.attempt_id,),
        ).fetchone()
        self.assertEqual((row["session_id"], row["track"], row["status"]),
                         ("native-session", "native", "accepted"))

        live = self._create_context("m_rebind_live")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?,?,?,?,?,?)",
                (live.claim.attempt_id, "gadget-session", "worker:1002", "accepted", self.now, "gadget"),
            )
        refused = adapter.submit(live)
        self.assertEqual(refused.state, "unknown")
        self.assertEqual(refused.evidence, "attempt_already_has_external_binding")

class TestEdgeCasesAndHttpErrors(unittest.TestCase):
    """Boundary cases: client_factory, run-id binding, worker start failures."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "edge.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)

    def test_executor_with_client_factory(self):
        """GadgetExecutor lazily obtains its client from client_factory."""
        mock_client = Mock(spec=GadgetClient)
        mock_client.is_configured.return_value = True
        executor = GadgetExecutor(self.store, client_factory=lambda: mock_client)
        self.assertTrue(executor.is_configured())
        self.assertIs(executor.client, mock_client)

    def test_query_mismatched_run_id_raises_value_error(self):
        """A query with a run_id that does not match the binding raises ValueError."""
        ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        ingress.receive("msg_mis", "user_1", "chat_mis", "single", "text")
        claim = self.store.claim("w1", concurrency=1, lease_seconds=60)
        self.store.begin_submission(claim)

        executor = GadgetExecutor(self.store, client=FakeWorkerClient(configured=True))

        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at, track) VALUES (?, 's_1', 'run_orig', 'accepted', 100.0, 'gadget')",
                (claim.attempt_id,)
            )

        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        ctx = ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

        with self.assertRaises(ValueError):
            executor.query(ctx, "run_different")

    def test_client_start_failure_mappings(self):
        """Missing pairing maps to AuthError; an unstartable worker maps to NetworkError."""
        base = Path(self.temp_dir.name)
        stub = base / "stub.py"
        stub.write_text(STUB_WORKER, encoding="utf-8")

        unconfigured = GadgetClient(
            credentials=GadgetCredentials(directory=base / "empty"),
            run_dir=base / "runs",
            worker_script=stub,
            runner=sys.executable,
        )
        with self.assertRaises(GadgetAuthError):
            unconfigured.start_turn("a1", "s1", "hi")

        pairing = base / "pair"
        write_pairing(pairing)
        broken = GadgetClient(
            credentials=GadgetCredentials(directory=pairing),
            run_dir=base / "runs",
            worker_script=stub,
            runner="/nonexistent/bun",
        )
        with self.assertRaises(GadgetNetworkError):
            broken.start_turn("a2", "s2", "hi")


class TestCoreRunnerIntegration(unittest.TestCase):
    """Use case 9: CoreRunner end-to-end, tick() driving claimed -> running -> succeeded."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "e2e.sqlite3"
        self.now = 100.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"alice"})

        self.fake_client = FakeWorkerClient(configured=True)
        self.primary = GadgetExecutor(self.store, client=self.fake_client)
        self.fallback = Mock(spec=MuseExecutor)
        self.dual_executor = DualTrackExecutor(
            store=self.store,
            primary=self.primary,
            fallback=self.fallback,
        )

    def test_full_pipeline_claimed_running_succeeded(self):
        """runner.tick() drives the task from claimed to running to succeeded."""
        ingest = self.ingress.receive("msg_100", "alice", "chat_alice", "single", "What is dual-track?")
        self.assertTrue(ingest.created)

        runner = CoreRunner(self.store, self.dual_executor, "worker_e2e")

        state1 = runner.tick()
        self.assertEqual(state1, "accepted")
        task = self.store.task(ingest.task_id)
        self.assertEqual(task["state"], "running")

        run_row = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (task["attempt_id"],)
        ).fetchone()
        self.assertIsNotNone(run_row)
        self.assertEqual(run_row["track"], "gadget")
        self.assertEqual(run_row["platform_run_id"], "worker:1001")

        state2 = runner.tick()
        self.assertEqual(state2, "running")
        self.assertEqual(self.store.task(ingest.task_id)["state"], "running")

        self.fake_client.results[task["attempt_id"]] = {
            "state": "succeeded",
            "text": "Dual-track is high-availability architecture.",
            "message_id": "m100",
        }

        state3 = runner.tick()
        self.assertEqual(state3, "succeeded")
        task_final = self.store.task(ingest.task_id)
        self.assertEqual(task_final["state"], "succeeded")
        self.assertEqual(task_final["result"], "Dual-track is high-availability architecture.")

        outbound = self.store.connection.execute(
            "SELECT * FROM outbound_messages WHERE task_id=?", (ingest.task_id,)
        ).fetchone()
        self.assertIsNotNone(outbound)
        self.assertEqual(outbound["text"], "Dual-track is high-availability architecture.")

        state4 = runner.tick()
        self.assertEqual(state4, "idle")


class TestSchemaBackwardCompatibility(unittest.TestCase):
    """Legacy executor_runs gains track/baseline_seq; gadget_sessions is created."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "legacy.sqlite3"

    def test_legacy_table_auto_migrated_with_track_column(self):
        """A legacy table without track/baseline_seq is migrated in place."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE executor_runs (
                attempt_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                platform_run_id TEXT,
                status TEXT NOT NULL,
                created_at REAL NOT NULL
            );
        """)
        conn.commit()
        conn.close()

        store = Store(self.db_path)
        self.addCleanup(store.close)
        ensure_executor_schema(store)

        columns = [row["name"] for row in store.connection.execute("PRAGMA table_info(executor_runs)").fetchall()]
        self.assertIn("track", columns)
        self.assertIn("baseline_seq", columns)

        store.connection.execute(
            "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at) "
            "VALUES ('att_old', 's_old', 'r_old', 'accepted', 100.0)"
        )
        row = store.connection.execute("SELECT track FROM executor_runs WHERE attempt_id='att_old'").fetchone()
        self.assertEqual(row["track"], "native")

        tables = [r["name"] for r in store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        self.assertIn("gadget_sessions", tables)


class TestSkepticalHardeningAndEdgeCases(unittest.TestCase):
    """Hardening: credential hygiene, spawn failures, half-open reset, real fallback."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "hardened.sqlite3"
        self.now = 5000.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})

    def _create_context(self, msg_id="m_hard") -> ExecutionContext:
        self.ingress.receive(msg_id, "user_1", f"chat_{msg_id}", "single", f"text {msg_id}")
        claim = self.store.claim("w_hard", concurrency=1, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_worker_argv_never_carries_credential_values(self):
        """The spawned worker gets the pairing directory path, never a credential value."""
        base = Path(self.temp_dir.name)
        pairing = base / "pair"
        write_pairing(pairing, token="tok_live_value_never_in_argv")
        run_dir = base / "runs"
        stub = base / "stub.py"
        stub.write_text(STUB_WORKER, encoding="utf-8")
        client = GadgetClient(
            credentials=GadgetCredentials(directory=pairing),
            run_dir=run_dir,
            worker_script=stub,
            runner=sys.executable,
        )
        proc = client.start_turn("att_h", "sess_h", "hello")
        proc.wait(timeout=10)
        argv = json.loads((run_dir / "gadget-att_h.json.argv.json").read_text(encoding="utf-8"))
        joined = " ".join(argv)
        self.assertIn(str(pairing), joined)
        self.assertNotIn("tok_live_value_never_in_argv", joined)

    def test_worker_spawn_oserror_is_network_error(self):
        """A worker that cannot spawn raises GadgetNetworkError naming the runner."""
        base = Path(self.temp_dir.name)
        pairing = base / "pair"
        write_pairing(pairing)
        client = GadgetClient(
            credentials=GadgetCredentials(directory=pairing),
            run_dir=base / "runs",
            worker_script=base / "stub.py",
            runner="/nonexistent/bun",
        )
        with self.assertRaises(GadgetNetworkError) as ctx:
            client.start_turn("att_x", "sess_x", "hi")
        self.assertIn("/nonexistent/bun", str(ctx.exception))

    def test_circuit_breaker_half_open_failure_resets_cooldown(self):
        """A half-open probe failure re-opens the circuit and restarts the cooldown."""
        now = 1000.0
        cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0, clock=lambda: now)

        for _ in range(5):
            cb.record_failure()
        self.assertEqual(cb.status, "OPEN")

        now += 35.0
        self.assertEqual(cb.status, "HALF_OPEN")

        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")

        now += 15.0
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        now += 16.0
        self.assertEqual(cb.status, "HALF_OPEN")
        self.assertTrue(cb.can_execute())

    def test_gadget_executor_pre_submission_cancellation_cleans_db(self):
        """A cancel requested before submission leaves no placeholder row."""
        context = self._create_context("m_cancel_clean")
        self.store.request_cancel(context.claim.task_id, context.claim.conversation_id)

        executor = GadgetExecutor(self.store, client=FakeWorkerClient(configured=True))

        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "cancelled_before_submission")

        rows = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_gadget_executor_standalone_network_failure_returns_unknown(self):
        """A real GadgetExecutor whose worker cannot start returns unknown, never raises."""
        context = self._create_context("m_net_fail")
        fake = FakeWorkerClient(configured=True)
        fake.start_error = GadgetNetworkError("Gateway Timeout 504")
        executor = GadgetExecutor(self.store, client=fake)

        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "unknown")
        self.assertIn("Gateway Timeout 504", outcome.evidence)

        rows = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_dual_track_real_gadget_executor_upstream_failure_falls_back_to_native(self):
        """End-to-end fallback: a real GadgetExecutor that cannot spawn falls back to Native."""
        context = self._create_context("m_dual_real_fallback")

        fake = FakeWorkerClient(configured=True)
        fake.start_error = GadgetNetworkError("Connection refused by upstream")
        primary = GadgetExecutor(self.store, client=fake)

        fallback = Mock(spec=MuseExecutor)
        fallback.submit.return_value = ExecutionOutcome("accepted", run_id="native_real_run_1")

        dual_executor = DualTrackExecutor(
            store=self.store,
            primary=primary,
            fallback=fallback,
        )

        outcome = dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "native_real_run_1")
        self.assertEqual(dual_executor.circuit_status(), "CLOSED")
        self.assertEqual(dual_executor.circuit.failure_count, 1)
        fallback.submit.assert_called_once_with(context)


class FakeHistoryClient:
    """In-memory Cookie-bridge history: call_json('chat.history') only.

    Events mirror the live shape (2026-11-07 probe): top-level seq /
    event_name / message_id, status and display_text inside payload.
    """

    def __init__(self, events=None, error=None):
        self.events = list(events or [])
        self.error = error
        self.calls = []
        self.closed = False

    def call_json(self, method, body=None, timeout=None):
        self.calls.append((method, body))
        if self.error is not None:
            raise self.error
        assert method == "chat.history"
        return {"chat_events": [dict(event) for event in self.events]}

    def close(self):
        self.closed = True


def _history_event(seq, kind, message_id, status=None, text=""):
    payload = {"status": status, "display_text": text} if status is not None or text else {}
    return {"seq": seq, "event_name": kind, "message_id": message_id, "payload": payload}


class TestGadgetHistoryFallback(unittest.TestCase):
    """History fallback (2026-11-07): Gadget Noise /chat/history is 403
    (endpoint not in the device-token ACL), but the Cookie bridge reads a
    Gadget-created session's history fine (live probe: completed '收到'
    on the first poll). When the worker is still running on a late
    subscription terminal but history already shows the completed reply,
    query() settles from history instead of waiting ~200s."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")
        self.fake_client = FakeWorkerClient(configured=True)
        self.history = FakeHistoryClient()
        self.executor = GadgetExecutor(
            self.store, client=self.fake_client,
            history_client_factory=lambda: self.history)

    def _get_context(self) -> ExecutionContext:
        claim = self.store.claim("worker_1", concurrency=1, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def _submit_running(self, context):
        """submit() then a worker result of running with an ack message id."""
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.fake_client.results[context.claim.attempt_id] = {
            "state": "running", "message_id": "u-ack-1",
        }
        return outcome

    def test_query_settles_from_history_while_worker_running(self):
        """Worker still running + history completed -> succeeded, worker stopped."""
        context = self._get_context()
        self._submit_running(context)
        self.history.events = [
            _history_event(10, "message.user", "u-ack-1"),
            _history_event(11, "message.assistant", "a-1", "completed", "pong via history"),
        ]
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "succeeded")
        self.assertEqual(q.text, "pong via history")
        self.assertEqual(q.message_id, "a-1")
        self.assertEqual(self.fake_client.stopped, [1001])
        self.assertTrue(self.history.closed)

    def test_query_ignores_stale_completed_below_anchor(self):
        """A completed reply from a previous attempt (seq <= anchor) is not ours."""
        context = self._get_context()
        self._submit_running(context)
        self.history.events = [
            _history_event(9, "message.assistant", "a-old", "completed", "old reply"),
            _history_event(10, "message.user", "u-ack-1"),
        ]
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "running")
        self.assertEqual(self.fake_client.stopped, [])

    def test_query_running_when_reply_not_completed_yet(self):
        """An assistant message still generating (no completed status) settles nothing."""
        context = self._get_context()
        self._submit_running(context)
        self.history.events = [
            _history_event(10, "message.user", "u-ack-1"),
            _history_event(11, "message.assistant", "a-1", "running", "half"),
        ]
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "running")
        self.assertEqual(self.fake_client.stopped, [])

    def test_query_running_when_anchor_missing(self):
        """No user event with the ack id yet (send not recorded) -> running, no guess."""
        context = self._get_context()
        self._submit_running(context)
        self.history.events = [
            _history_event(10, "message.user", "u-someone-else"),
        ]
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "running")

    def test_query_running_when_history_unavailable(self):
        """A history failure (404 / auth / network) never breaks the running path."""
        context = self._get_context()
        self._submit_running(context)
        self.history.error = RuntimeError("session not found")
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "running")
        self.assertEqual(self.fake_client.stopped, [])

    def test_query_running_without_history_factory(self):
        """Without a history factory the fallback is disabled: plain running."""
        context = self._get_context()
        executor = GadgetExecutor(self.store, client=self.fake_client)
        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.fake_client.results[context.claim.attempt_id] = {
            "state": "running", "message_id": "u-ack-1",
        }
        q = executor.query(context, "worker:1001")
        self.assertEqual(q.state, "running")

    def test_query_merges_split_replies(self):
        """A turn split across two completed assistant messages merges, in seq order."""
        context = self._get_context()
        self._submit_running(context)
        self.history.events = [
            _history_event(10, "message.user", "u-ack-1"),
            _history_event(11, "message.assistant", "a-1", "completed", "first part"),
            _history_event(12, "message.assistant", "a-2", "completed", "second part"),
        ]
        q = self.executor.query(context, "worker:1001")
        self.assertEqual(q.state, "succeeded")
        self.assertEqual(q.text, "first part\nsecond part")
        self.assertEqual(q.message_id, "a-2")

    def test_submit_does_not_call_history(self):
        """The fallback anchors on the ack message id at query time; submit stays history-free."""
        context = self._get_context()
        self.executor.submit(context)
        self.assertEqual(self.history.calls, [])

    def test_dual_track_default_wires_history_factory(self):
        """Production construction DualTrackExecutor(store) enables the fallback."""
        dual = DualTrackExecutor(store=self.store)
        self.assertIs(dual.primary.history_client_factory, bridge_client)


if __name__ == "__main__":
    unittest.main()
