"""Exercise an opt-in hook handoff through the real Core runner and mailbox."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from mobile_channels.executors import DualTrackExecutor, GadgetExecutor, MuseExecutor
from mobile_channels.ingress import CoreIngress
from mobile_channels.modes import ModeError
from mobile_channels.runtime import CoreRunner, ExecutionOutcome
from mobile_channels.storage import Store


class CoreHookHandoffTests(unittest.TestCase):
    """Use local receipts and task routes without network or platform hooks."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "core.sqlite3"
        self.store = Store(self.database)
        self.addCleanup(self.store.close)
        self.modes = self.root / "modes.json"
        self.modes.write_text(json.dumps({"channels": {"wecom": {
            "mode": "core", "database": str(self.database), "account_id": "bot",
            "allowed_senders": ["alice"], "hook_handoff": True,
        }}}))
        environment = patch.dict(os.environ, {"MUSE_CHANNEL_MODES": str(self.modes)})
        environment.start()
        self.addCleanup(environment.stop)
        self.primary = Mock(spec=GadgetExecutor)
        self.primary.is_configured.return_value = False
        self.native = Mock(spec=MuseExecutor)
        self.native.submit.return_value = ExecutionOutcome("not_submitted", evidence="native_auth_unavailable")
        self.executor = DualTrackExecutor(self.store, primary=self.primary, fallback=self.native)
        self.runner = CoreRunner(self.store, self.executor, "core-owner")
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"alice"})
        self.task = self.ingress.receive("external-1", "alice", "room-1", "group", "请完成任务")

    def test_proven_unsent_task_waits_for_hook_receipt_and_core_delivers_result(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.assertEqual(self.runner.tick(), "running")
        self.assertEqual(self.store.task(self.task.task_id)["state"], "reconciling")
        mailbox = HookHandoffExecutor(self.store, self.modes)
        offered = mailbox.offer("wecom")
        self.assertEqual(offered["task"]["chat_id"], "room-1")
        self.assertEqual(offered["task"]["external_message_id"], "external-1")
        self.assertIsNone(mailbox.offer("wecom"))
        mailbox.acknowledge(offered["handoff_id"], offered["capability"], "hook-run-1")
        self.assertEqual(self.runner.tick(), "accepted")
        mailbox.complete(offered["handoff_id"], offered["capability"], "hook-run-1",
                         state="succeeded", text="任务结果")
        self.assertEqual(self.runner.tick(), "succeeded")
        outbound = self.store.connection.execute("SELECT text FROM outbound_messages").fetchall()
        self.assertEqual([row["text"] for row in outbound], ["任务结果"])
        self.native.submit.assert_called_once()

    def test_unknown_native_submission_never_reaches_hooks(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.native.submit.return_value = ExecutionOutcome("unknown", evidence="send_timeout")
        self.assertEqual(self.runner.tick(), "unknown")
        self.assertIsNone(HookHandoffExecutor(self.store, self.modes).offer("wecom"))

    def test_new_context_preserves_a_queued_handoff_and_original_route(self):
        from mobile_channels.handoff import HookHandoffExecutor
        self.assertEqual(self.runner.tick(), "running")
        self.store.new_generation(self.task.conversation_id)
        mailbox = HookHandoffExecutor(self.store, self.modes)
        offer = mailbox.offer("wecom")
        self.assertIsNotNone(offer)
        status = mailbox.acknowledge(offer["handoff_id"], offer["capability"], "original-worker")
        self.assertFalse(status["cancel_requested"])
        self.assertEqual(status["task"]["generation"], 0)
        self.assertEqual(self.runner.tick(), "accepted")
        mailbox.complete(offer["handoff_id"], offer["capability"], "original-worker",
                         state="succeeded", text="old context answer")
        self.assertEqual(self.runner.tick(), "succeeded")

    def test_disabled_hook_fails_unsent_task_without_unbounded_requeue(self):
        document = json.loads(self.modes.read_text())
        document["channels"]["wecom"]["hook_handoff"] = False
        self.modes.write_text(json.dumps(document))
        self.assertEqual(self.runner.tick(), "failed")
        self.assertEqual(self.runner.tick(), "idle")

    def test_query_time_gadget_failure_can_handoff_without_repeating_native(self):
        from mobile_channels.handoff import HookHandoffExecutor
        self.primary.is_configured.return_value = True

        def submit(context):
            self.store.connection.execute(
                "INSERT INTO executor_runs(attempt_id,session_id,platform_run_id,status,created_at,track) "
                "VALUES (?,'gadget-session','worker:1001','accepted',?,'gadget')",
                (context.claim.attempt_id, self.store.clock()))
            return ExecutionOutcome("accepted", run_id="worker:1001")

        self.primary.submit.side_effect = submit
        self.primary.query.return_value = ExecutionOutcome("failed", evidence="worker_never_sent")
        self.assertEqual(self.runner.tick(), "accepted")
        self.assertEqual(self.runner.tick(), "running")
        mailbox = HookHandoffExecutor(self.store, self.modes)
        offer = mailbox.offer("wecom")
        mailbox.acknowledge(offer["handoff_id"], offer["capability"], "fallback-worker")
        self.assertEqual(self.runner.tick(), "running")
        mailbox.complete(offer["handoff_id"], offer["capability"], "fallback-worker",
                         state="succeeded", text="recovered reply")
        self.assertEqual(self.runner.tick(), "succeeded")
        self.native.submit.assert_called_once()

    def test_native_auth_failure_before_send_can_be_handed_to_hooks(self):
        from mobile_channels.handoff import HookHandoffExecutor

        def unavailable_transport():
            raise ConnectionError("credentials unavailable")

        self.executor.fallback = MuseExecutor(self.store, client_factory=unavailable_transport)
        self.assertEqual(self.runner.tick(), "running")
        self.assertIsNotNone(HookHandoffExecutor(self.store, self.modes).offer("wecom"))

    def test_opt_in_is_required_even_when_native_proves_unsent(self):
        from mobile_channels.handoff import HookHandoffExecutor

        modes = json.loads(self.modes.read_text())
        modes["channels"]["wecom"].pop("hook_handoff")
        self.modes.write_text(json.dumps(modes))
        self.assertEqual(self.runner.tick(), "failed")
        with self.assertRaises(ModeError):
            HookHandoffExecutor(self.store, self.modes).offer("wecom")

    def test_lost_offer_is_not_repeated_after_restart(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.runner.tick()
        first = HookHandoffExecutor(self.store, self.modes).offer("wecom")
        self.assertIsNotNone(first)
        for _ in range(12):
            self.assertEqual(self.runner.tick(), "running")
        with Store(self.database) as reopened:
            self.assertIsNone(HookHandoffExecutor(reopened, self.modes).offer("wecom"))
        self.assertEqual(self.store.task(self.task.task_id)["state"], "reconciling")

    def test_cancelled_before_offer_never_wakes_a_worker(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.runner.tick()
        self.store.request_cancel(self.task.task_id, self.task.conversation_id)
        self.assertEqual(self.runner.tick(), "cancelled")
        self.assertIsNone(HookHandoffExecutor(self.store, self.modes).offer("wecom"))

    def test_cancellation_is_visible_in_the_first_hook_receipt(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.runner.tick()
        mailbox = HookHandoffExecutor(self.store, self.modes)
        offered = mailbox.offer("wecom")
        self.store.request_cancel(self.task.task_id, self.task.conversation_id)
        receipt = mailbox.acknowledge(offered["handoff_id"], offered["capability"], "hook-run-1")

        self.assertTrue(receipt["cancel_requested"])
        mailbox.complete(offered["handoff_id"], offered["capability"], "hook-run-1",
                         state="cancelled", evidence="worker_stopped_before_execution")
        self.runner.tick()
        self.assertEqual(self.runner.tick(), "cancelled")

    def test_receipts_are_capability_scoped_and_final_result_is_immutable(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.runner.tick()
        mailbox = HookHandoffExecutor(self.store, self.modes)
        offered = mailbox.offer("wecom")
        handoff_id, capability = offered["handoff_id"], offered["capability"]
        with self.assertRaises(ValueError):
            mailbox.acknowledge(handoff_id, "forged", "hook-run-1")
        mailbox.acknowledge(handoff_id, capability, "hook-run-1")
        mailbox.acknowledge(handoff_id, capability, "hook-run-1")
        with self.assertRaises(ValueError):
            mailbox.acknowledge(handoff_id, capability, "another-run")
        mailbox.complete(handoff_id, capability, "hook-run-1", state="succeeded", text="first result")
        mailbox.complete(handoff_id, capability, "hook-run-1", state="succeeded", text="first result")
        with self.assertRaises(ValueError):
            mailbox.complete(handoff_id, capability, "hook-run-1", state="succeeded", text="changed result")
        self.assertEqual(self.runner.tick(), "accepted")
        self.assertEqual(self.runner.tick(), "succeeded")
        self.assertEqual(self.store.connection.execute("SELECT text FROM outbound_messages").fetchone()[0], "first result")

    def test_legacy_mode_consumer_never_offers_core_work(self):
        from mobile_channels.handoff import HookHandoffExecutor

        self.runner.tick()
        modes = json.loads(self.modes.read_text())
        modes["channels"]["wecom"]["mode"] = "legacy"
        self.modes.write_text(json.dumps(modes))
        with self.assertRaises(ModeError):
            HookHandoffExecutor(self.store, self.modes).offer("wecom")

    def test_cancel_rejects_a_foreign_run_before_persisting_request(self):
        self.runner.tick()
        claim = self.runner._active_claim()
        context = self.runner._context(claim)

        with self.assertRaises(ValueError):
            self.executor.cancel(context, "hook:foreign")
        row = self.store.connection.execute("SELECT cancel_requested FROM core_hook_handoffs").fetchone()
        self.assertFalse(row[0])

    def _poll_script(self, home, **overrides):
        """Run the real hook from an unrelated cwd with a minimal runtime env."""
        runtime = self.root / "hook-runtime.sh"
        runtime.write_text('silent() { printf "silent\\n%s\\n" "$2"; }\n'
                           'wake() { printf "wake\\n%s\\n" "$2"; }\n')
        environment = {
            "HOME": str(home), "PATH": os.environ.get("PATH", os.defpath),
            "HATCH_HOOK_RUNTIME": str(runtime), "PYTHONDONTWRITEBYTECODE": "1",
            **overrides,
        }
        script = Path(__file__).resolve().parents[1] / "hooks/scripts/core-handoff.sh"
        return subprocess.run(["bash", str(script)], cwd=self.root, env=environment,
                              capture_output=True, text=True, timeout=15)

    def test_hook_runtime_defaults_offer_once_without_muse_environment(self):
        """A clean hook runtime can import the project and use production paths."""
        home = self.root / "hook home"
        workspace = home / "workspace"
        state = workspace / "state"
        state.mkdir(parents=True)
        (workspace / "mobile_channels").symlink_to(
            Path(__file__).resolve().parents[1] / "mobile_channels", target_is_directory=True)
        (state / "core.sqlite3").symlink_to(self.database)
        (state / "channel-modes.json").symlink_to(self.modes)
        interpreter = home / "muse-test-venv/bin/python"
        interpreter.parent.mkdir(parents=True)
        interpreter.symlink_to(sys.executable)
        self.runner.tick()

        first = self._poll_script(home)
        self.assertEqual(first.returncode, 0, first.stderr)
        event, payload = first.stdout.splitlines()
        self.assertEqual(event, "wake")
        self.assertEqual(json.loads(payload)["task"]["external_message_id"], "external-1")
        second = self._poll_script(home)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(second.stdout, "silent\n{}\n")
        row = self.store.connection.execute("SELECT state FROM core_hook_handoffs").fetchone()
        self.assertEqual(row[0], "offered")

    def test_hook_runtime_preserves_explicit_paths_and_pythonpath(self):
        """Custom installs still work when HOME/workspace does not exist."""
        self.runner.tick()
        result = self._poll_script(
            self.root / "custom home",
            MUSE_CORE_HANDOFF_DATABASE=str(self.database), MUSE_CHANNEL_MODES=str(self.modes),
            MUSE_CORE_HANDOFF_PYTHON=sys.executable,
            PYTHONPATH=str(Path(__file__).resolve().parents[1]),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[0], "wake")


if __name__ == "__main__":
    unittest.main()
