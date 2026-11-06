"""Preserve execution attempts and fence retired workers before heartbeat I/O."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import channel_common as common
import death_watch_activity as probe

RUN = "event-hook:first"
NEXT = "event-hook:second"
START = 1_800_000_000.0


class RegistrationHistoryTests(unittest.TestCase):
    """Exercise actual ledger transactions with private temporary state only."""

    def setUp(self):
        """Make each test independent from production files and the real clock."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "worker_activity.json"

    def record(self):
        """Read what a later worker process would load."""
        return json.loads(self.path.read_text())["m1"]

    def takeover(self):
        """Create two executions for one message without any real processes."""
        common.register_worker_activity(self.path, ["m1"], RUN, START)
        common.update_worker_activity(self.path, "m1", START + 10, progress="old progress",
                                      hold_secs=2000, handoff=True)
        common.register_worker_activity(self.path, ["m1"], NEXT, START + 20)

    def test_new_run_archives_old_claims_without_inheriting_them(self):
        """A new attempt must not inherit another worker's declared activity."""
        self.takeover()
        current = self.record()
        self.assertEqual(current["run_id"], NEXT)
        self.assertTrue(current["requires_run_id"])
        for key in ("progress", "hold_until", "handoff", "handoff_at"):
            self.assertNotIn(key, current)
        previous = current["previous_runs"][-1]
        self.assertEqual(previous["run_id"], RUN)
        self.assertEqual(previous["progress"], "old progress")
        self.assertEqual(previous["hold_until"], START + 2010)
        self.assertEqual(previous["replaced_at"], START + 20)
        self.assertNotIn("previous_runs", previous)
        self.assertIn(RUN, current["retired_run_ids"])

    def test_same_run_renewal_preserves_activity_and_original_registration(self):
        """Renewing the detached heartbeat is not a fresh work observation."""
        common.register_worker_activity(self.path, ["m1"], RUN, START)
        common.update_worker_activity(self.path, "m1", START + 5,
                                      progress="working", hold_secs=3000, handoff=True)
        common.register_worker_activity(self.path, ["m1"], RUN, START + 780)
        current = self.record()
        self.assertEqual(current["ts"], START + 5)
        self.assertEqual(current["source"], "worker-handoff")
        self.assertEqual(current["progress"], "working")
        self.assertEqual(current["hold_until"], START + 3005)
        self.assertEqual(current["handoff_at"], START + 5)
        self.assertEqual(current["registered_at"], START)
        self.assertEqual(current["registration_seen_at"], START + 780)

    def test_repeated_heartbeat_registration_does_not_keep_worker_alive(self):
        """An empty renewal loop must eventually age out of real-activity checks."""
        common.register_worker_activity(self.path, ["m1"], RUN, START)
        for elapsed in range(780, 10000, 780):
            common.register_worker_activity(self.path, ["m1"], RUN, START + elapsed)
        result = probe.assess("m1", now=START + 10000, window=1800,
                              bridge_state_path=str(self.directory / "absent.json"),
                              worker_activity_path=str(self.path))
        self.assertFalse(result["alive"])
        self.assertEqual(self.record()["ts"], START)

    def test_retired_register_and_untagged_or_wrong_updates_are_rejected(self):
        """Old worker writes cannot be attributed to the replacement worker."""
        self.takeover()
        before = self.path.read_bytes()
        for run in (None, RUN, "event-hook:unknown"):
            with self.subTest(run=run), self.assertRaises(ValueError):
                common.update_worker_activity(self.path, "m1", START + 30,
                                              progress="late old work", run_id=run)
            self.assertEqual(self.path.read_bytes(), before)
        with self.assertRaises(ValueError):
            common.register_worker_activity(self.path, ["m1"], RUN, START + 40)
        self.assertEqual(self.path.read_bytes(), before)
        common.update_worker_activity(self.path, "m1", START + 50,
                                      progress="new work", run_id=NEXT)
        self.assertEqual(self.record()["progress"], "new work")

    def test_takeover_batch_is_all_or_nothing(self):
        """A later conflicting message cannot leave earlier batch IDs registered."""
        self.takeover()
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            common.register_worker_activity(self.path, ["brand-new", "m1"], RUN, START + 50)
        self.assertEqual(self.path.read_bytes(), before)

    def test_history_is_bounded_but_retired_id_fencing_survives_trimming(self):
        """Retaining eight detailed snapshots cannot permit older workers to return."""
        common.register_worker_activity(self.path, ["m1"], RUN, START)
        for index in range(12):
            common.register_worker_activity(self.path, ["m1"], f"event-hook:run-{index}", START + index + 1)
        current = self.record()
        self.assertEqual(len(current["previous_runs"]), 8)
        self.assertEqual(len(current["retired_run_ids"]), 12)
        self.assertIn(RUN, current["retired_run_ids"])
        with self.assertRaises(ValueError):
            common.register_worker_activity(self.path, ["m1"], RUN, START + 20)

    def test_legacy_updates_remain_compatible_before_takeover(self):
        """Old-format activity records can still receive their original CLI calls."""
        self.path.write_text(json.dumps({"m1": START - 1, "other": START - 3}))
        common.update_worker_activity(self.path, "m1", START, progress="legacy")
        common.register_worker_activity(self.path, ["m1"], RUN, START + 10)
        common.update_worker_activity(self.path, "m1", START + 20, progress="bound legacy")
        self.assertEqual(self.record()["run_id"], RUN)
        self.assertEqual(self.record()["progress"], "bound legacy")
        self.assertEqual(json.loads(self.path.read_text())["other"], START - 3)

    def test_explicit_wrong_run_is_rejected_even_before_takeover(self):
        """An explicit ownership claim must always match the registered execution."""
        common.register_worker_activity(self.path, ["m1"], RUN, START)
        with self.assertRaises(ValueError):
            common.update_worker_activity(self.path, "m1", START + 10,
                                          progress="wrong", run_id=NEXT)

    def test_registration_context_serializes_heartbeat_and_takeover(self):
        """Another run cannot acquire a message between ownership and heartbeat I/O."""
        started, finished = threading.Event(), threading.Event()
        errors = []

        def take_over():
            """Try to replace ownership from an independent lock descriptor."""
            started.set()
            try:
                common.register_worker_activity(self.path, ["m1"], NEXT, START + 5)
            except BaseException as error:
                errors.append(error)
            finally:
                finished.set()

        with common.worker_activity_registration(self.path, ["m1"], RUN, START):
            thread = threading.Thread(target=take_over)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(finished.wait(0.05))
        thread.join(2)
        self.assertTrue(finished.is_set())
        self.assertFalse(errors)
        self.assertEqual(self.record()["run_id"], NEXT)


class CliRegistrationTests(unittest.TestCase):
    """Run the real parsers with process operations replaced at their boundaries."""

    def setUp(self):
        """Create isolated paths for each channel's actual CLI module."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def cli(self, channel):
        """Load a channel entrypoint without copying or changing runtime code."""
        source = ROOT / f"{channel}-bot" / f"{channel}_cli.py"
        spec = importlib.util.spec_from_file_location(f"history_{channel}", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.STATE = self.directory / channel
        module.STATE.mkdir(exist_ok=True)
        module.HOOK_STATE = module.STATE / "hooks"
        module.WORKER_ACTIVITY = module.STATE / "worker_activity.json"
        return module

    def run_cli(self, cli, *arguments):
        """Capture parser results while proving no detached process is started."""
        with patch.object(sys, "argv", ["channel", *arguments]), \
                patch.object(cli.time, "time", return_value=START + 50), \
                patch.object(cli.subprocess, "Popen") as spawned, \
                contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            code = cli.main()
        return code, spawned

    def test_conflicting_heartbeat_has_no_stop_or_process_side_effect(self):
        """Retired, unknown-owner and untagged calls fail before heartbeat writes."""
        for channel in ("weixin", "wecom"):
            for run in (RUN, None):
                with self.subTest(channel=channel, run=run):
                    cli = self.cli(channel)
                    common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], RUN, START)
                    common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], NEXT, START + 1)
                    directory = cli.STATE / "heartbeats"
                    directory.mkdir(exist_ok=True)
                    (directory / "m1.pid").write_text("99999999")
                    before = cli.WORKER_ACTIVITY.read_bytes()
                    arguments = ["heartbeat-start", "--msgids", "new,m1"]
                    if run:
                        arguments += ["--run-id", run]
                    code, spawned = self.run_cli(cli, *arguments)
                    self.assertEqual(code, 1)
                    spawned.assert_not_called()
                    self.assertEqual(cli.WORKER_ACTIVITY.read_bytes(), before)
                    self.assertFalse((directory / "m1.stop").exists())
                    # Reset only fixture state between channel cases.
                    cli.WORKER_ACTIVITY.unlink()

    def test_current_run_can_start_and_update_after_takeover(self):
        """CLI --run-id must reach activity updates, not just registration."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], RUN, START)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], NEXT, START + 1)
                code, spawned = self.run_cli(cli, "heartbeat-start", "--msgids", "m1", "--run-id", NEXT)
                self.assertEqual(code, 0)
                spawned.assert_called_once()
                code, _ = self.run_cli(cli, "worker-activity", "--msgid", "m1", "--run-id", NEXT,
                                       "--beat", "--progress", "current run")
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(cli.WORKER_ACTIVITY.read_text())["m1"]["progress"], "current run")
                code, _ = self.run_cli(cli, "worker-activity", "--msgid", "m1", "--run-id", RUN, "--beat")
                self.assertEqual(code, 1)

    def test_registration_io_failure_does_not_report_heartbeat_started(self):
        """A failed durable registration must fail before spawning a heartbeat."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                with patch.object(common, "atomic_write_text", side_effect=OSError("disk unavailable")):
                    code, spawned = self.run_cli(cli, "heartbeat-start", "--msgids", "m1", "--run-id", RUN)
                self.assertEqual(code, 1)
                spawned.assert_not_called()

    def test_process_start_failure_is_not_reported_as_started(self):
        """A durable registration remains observable when spawning feedback fails."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                with patch.object(sys, "argv", ["channel", "heartbeat-start", "--msgids", "m1",
                                                "--run-id", RUN]), \
                        patch.object(cli.time, "time", return_value=START), \
                        patch.object(cli.subprocess, "Popen", side_effect=OSError("spawn unavailable")), \
                        contextlib.redirect_stdout(io.StringIO()) as output, \
                        contextlib.redirect_stderr(io.StringIO()):
                    code = cli.main()
                self.assertEqual(code, 1)
                self.assertNotIn("heartbeat_started", output.getvalue())
                record = json.loads(cli.WORKER_ACTIVITY.read_text())["m1"]
                self.assertEqual(record["run_id"], RUN)
                self.assertEqual(record["source"], "worker-register")
                self.assertNotIn("progress", record)

    def test_heartbeat_stop_requires_owner_and_never_registers(self):
        """A stale worker cannot stop its replacement's feedback or claim a new run."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], RUN, START)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], NEXT, START + 1)
                before = cli.WORKER_ACTIVITY.read_bytes()
                for run in (None, RUN, "event-hook:unseen"):
                    arguments = ["heartbeat-stop", "--msgids", "m1"]
                    if run:
                        arguments += ["--run-id", run]
                    code, spawned = self.run_cli(cli, *arguments)
                    self.assertEqual(code, 1)
                    spawned.assert_not_called()
                    self.assertEqual(cli.WORKER_ACTIVITY.read_bytes(), before)
                    self.assertFalse((cli.STATE / "heartbeats/m1.stop").exists())
                code, _ = self.run_cli(cli, "heartbeat-stop", "--msgids", "m1", "--run-id", NEXT)
                self.assertEqual(code, 0)
                self.assertTrue((cli.STATE / "heartbeats/m1.stop").exists())
                self.assertEqual(cli.WORKER_ACTIVITY.read_bytes(), before)

    def test_heartbeat_stop_write_failure_is_not_reported_as_stopped(self):
        """Stopping transport feedback must not hide a failed marker write."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], RUN, START)
                with patch.object(Path, "write_text", side_effect=OSError("marker unavailable")), \
                        patch.object(sys, "argv", ["channel", "heartbeat-stop", "--msgids", "m1",
                                                   "--run-id", RUN]), \
                        contextlib.redirect_stdout(io.StringIO()) as output, \
                        contextlib.redirect_stderr(io.StringIO()):
                    code = cli.main()
                self.assertEqual(code, 1)
                self.assertNotIn("heartbeat_stopped", output.getvalue())

    def test_legacy_heartbeat_stop_remains_available_before_takeover(self):
        """Existing one-attempt callers need no new flag until ownership changes."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                cli = self.cli(channel)
                common.register_worker_activity(cli.WORKER_ACTIVITY, ["m1"], RUN, START)
                code, _ = self.run_cli(cli, "heartbeat-stop", "--msgids", "m1")
                self.assertEqual(code, 0)
                self.assertTrue((cli.STATE / "heartbeats/m1.stop").exists())


if __name__ == "__main__":
    unittest.main()
