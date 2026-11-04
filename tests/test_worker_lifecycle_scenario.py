"""Exercise real channel CLIs and death-watch evidence through a long-task lifecycle."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import death_watch_activity as probe

START = 1_800_000_000.0
RUN = "event-hook:11111111-2222-3333-4444-555555555555"
WINDOW = 1800.0


def load_cli(channel: str):
    """Import the actual CLI while leaving its filesystem and process boundaries injectable."""
    source = ROOT / f"{channel}-bot" / f"{channel}_cli.py"
    spec = importlib.util.spec_from_file_location(f"{channel}_lifecycle_cli", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class WorkerLifecycleTests(unittest.TestCase):
    """Connect actual ledger writes, CLI renewal, formal reply and bounded handoff protection."""

    def make_cli(self, directory: Path, channel: str):
        """Route every mutable CLI path to this test's private temporary directory."""
        cli = load_cli(channel)
        cli.STATE = directory / channel / "state"
        cli.STATE.mkdir(parents=True)
        cli.HOOK_STATE = directory / channel / "hook"
        cli.HOOK_STATE.mkdir()
        cli.WORKER_ACTIVITY = cli.STATE / "worker_activity.json"
        cli.OUTBOX = cli.STATE / "outbox.jsonl"
        cli.OUTBOX_RESULTS = cli.STATE / "outbox_results.jsonl"
        cli.RESULTS = cli.OUTBOX_RESULTS
        cli.OUTBOX_RETRY = cli.STATE / "outbox_retry.json"
        return cli

    def run_cli(self, cli, now: float, *arguments: str):
        """Execute real argument handling and writes without launching detached processes."""
        with patch.object(sys, "argv", ["channel", *arguments]), \
                patch.object(cli.time, "time", return_value=now), \
                patch.object(cli.subprocess, "Popen") as spawned, \
                patch("mobile_channels.modes.lease_from_env", return_value=None), \
                contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()) as errors:
            result = cli.main()
        self.assertEqual(result, 0, errors.getvalue())
        return spawned

    def assess(self, cli, now: float, *, terminal: bool = False):
        """Run the production probe against the CLI's own ledger and a bound platform verdict."""
        verdict = cli.STATE / "worker_platform_status.json"
        if terminal:
            verdict.write_text(json.dumps({"m1": {
                "run_id": RUN, "status": "shutdown",
                "agent_updated_at": now - 1, "checked_at": now,
            }}), encoding="utf-8")
        else:
            verdict.unlink(missing_ok=True)
        return probe.assess(
            "m1", now=now, window=WINDOW,
            bridge_state_path=str(cli.STATE / "bridge.json"),
            worker_activity_path=str(cli.WORKER_ACTIVITY),
            platform_status_path=str(verdict),
        )

    def test_progress_hold_renewal_and_formal_handoff_cross_fifteen_minutes(self):
        """Renewal must preserve hold/run binding; handoff must protect and eventually expire."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel), tempfile.TemporaryDirectory() as temporary:
                cli = self.make_cli(Path(temporary), channel)
                first = self.run_cli(cli, START, "heartbeat-start", "--msgids", "m1",
                                     "--run-id", RUN)
                first.assert_called_once()
                self.run_cli(cli, START + 120, "worker-activity", "--msgid", "m1",
                             "--beat", "--progress", "等待长工具完成")
                self.run_cli(cli, START + 180, "worker-activity", "--msgid", "m1",
                             "--hold-secs", "2400")
                renewed = self.run_cli(cli, START + 780, "heartbeat-start", "--msgids", "m1",
                                       "--run-id", RUN)
                renewed.assert_called_once()
                ledger = json.loads(cli.WORKER_ACTIVITY.read_text(encoding="utf-8"))["m1"]
                self.assertEqual(ledger["run_id"], RUN)
                self.assertEqual(ledger["progress"], "等待长工具完成")
                self.assertEqual(ledger["hold_until"], START + 2580)

                info = self.assess(cli, START + 960, terminal=True)
                self.assertTrue(info["alive"])
                self.assertTrue(info["platform_terminal"])
                self.assertEqual(info["source"], "worker-hold")
                # Even stale registration must not defeat the still-live declared silence.
                self.assertEqual(self.assess(cli, START + 2500)["source"], "worker-hold")
                self.run_cli(cli, START + 2581, "worker-activity", "--msgid", "m1",
                             "--hold-secs", "0")
                self.run_cli(cli, START + 2590, "worker-activity", "--msgid", "m1",
                             "--beat", "--progress", "整理主会话交接")
                self.run_cli(cli, START + 2700, "reply", "--msgid", "m1", "--text",
                             "已登记移交，等待主会话处理，尚未完成。")
                rows = [json.loads(line) for line in cli.OUTBOX.read_text(encoding="utf-8").splitlines()]
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["mode"], "reply")
                self.assertEqual(rows[0]["msgid"], "m1")
                self.assertTrue(rows[0]["id"])
                self.run_cli(cli, START + 2700, "worker-activity", "--msgid", "m1", "--handoff")
                info = self.assess(cli, START + 7000, terminal=True)
                self.assertTrue(info["alive"])
                self.assertEqual(info["source"], "worker-handoff")
                self.assertEqual(info["worker_run_id"], RUN)
                self.assertTrue(info["platform_terminal"])
                expired = self.assess(cli, START + 2700 + 86401, terminal=True)
                self.assertFalse(expired["alive"])
                self.assertFalse(expired["worker_handoff"])
                self.assertTrue(expired["platform_terminal"])

    def test_expired_hold_and_stale_activity_cannot_keep_an_abandoned_worker_alive(self):
        """The same real registration/hold path must stop protecting when work evidence ages out."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel), tempfile.TemporaryDirectory() as temporary:
                cli = self.make_cli(Path(temporary), channel)
                self.run_cli(cli, START, "heartbeat-start", "--msgids", "m1", "--run-id", RUN)
                self.run_cli(cli, START + 120, "worker-activity", "--msgid", "m1",
                             "--beat", "--progress", "准备长工具")
                self.run_cli(cli, START + 180, "worker-activity", "--msgid", "m1",
                             "--hold-secs", "900")
                self.assertTrue(self.assess(cli, START + 1000)["alive"])
                # A heartbeat file's fresh mtime does not replace actual worker activity.
                (cli.STATE / "heartbeats" / "m1").write_text("running", encoding="utf-8")
                info = self.assess(cli, START + 180 + WINDOW + 1, terminal=True)
                self.assertFalse(info["alive"])
                self.assertTrue(info["platform_terminal"])
                self.assertEqual(info["worker_run_id"], RUN)


if __name__ == "__main__":
    unittest.main()
