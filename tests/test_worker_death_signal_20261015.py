#!/usr/bin/env python3
"""Todo 21 (routes C+E): platform-terminal probe source, worker hold,
and the worker_activity writers.

Covers death_watch_activity source 3 (verdict snapshot), the
worker-hold veto, channel_common's register/beat/hold writers
(roundtrip + old-format compatibility), and both CLIs'
worker-activity subcommand against a copied sandbox tree.

Run: ~/muse-test-venv/bin/python tests/test_worker_death_signal_20261015.py
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import channel_common as cc
import death_watch_activity as probe


NOW = 1_800_000_000.0
WINDOW = 1800.0
RUN = "event-hook:11111111-2222-3333-4444-555555555555"


class ProbePlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.bridge = self.dir / "state.json"
        self.worker = self.dir / "worker_activity.json"
        self.platform = self.dir / "worker_platform_status.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assess(self, msgid: str = "m1") -> dict:
        return probe.assess(
            msgid, now=NOW, window=WINDOW,
            bridge_state_path=str(self.bridge),
            worker_activity_path=str(self.worker),
            platform_status_path=str(self.platform),
        )

    def write_worker(self, data) -> None:
        self.worker.write_text(json.dumps(data), encoding="utf-8")

    def write_verdict(self, data) -> None:
        self.platform.write_text(json.dumps(data), encoding="utf-8")

    def verdict(self, **kw) -> dict:
        base = {"run_id": RUN, "status": "shutdown",
                "agent_updated_at": NOW - 700, "checked_at": NOW - 10}
        base.update(kw)
        return {"m1": base}

    def test_terminal_verdict_is_evidence_not_alive(self) -> None:
        # Registration ts is older than the assess window, so source
        # 2 contributes nothing here; only the verdict speaks.
        self.write_worker({"m1": {"ts": NOW - 2000,
                                  "source": "worker-register",
                                  "run_id": RUN}})
        self.write_verdict(self.verdict())
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertTrue(info["platform_terminal"])
        self.assertEqual(info["platform_status"], "shutdown")
        self.assertEqual(info["worker_run_id"], RUN)

    def test_each_terminal_status_counts(self) -> None:
        for status in ("completed", "shutdown", "errored", "interrupted"):
            self.write_verdict(self.verdict(status=status))
            self.assertTrue(self.assess()["platform_terminal"], status)

    def test_running_fresh_is_alive(self) -> None:
        self.write_verdict(self.verdict(status="running",
                                         agent_updated_at=NOW - 30))
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "platform-running")
        self.assertFalse(info["platform_terminal"])

    def test_running_stale_update_is_no_signal(self) -> None:
        self.write_verdict(self.verdict(status="running",
                                         agent_updated_at=NOW - 400))
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertFalse(info["platform_terminal"])

    def test_stale_verdict_is_ignored(self) -> None:
        self.write_verdict(self.verdict(checked_at=NOW - 601))
        info = self.assess()
        self.assertFalse(info["platform_terminal"])
        self.assertFalse(info["alive"])

    def test_corrupt_verdict_degrades_without_crash(self) -> None:
        self.platform.write_text("{broken", encoding="utf-8")
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertFalse(info["platform_terminal"])
        self.assertIn("unreadable:worker_platform_status.json",
                      info["degraded"])

    def test_run_id_mismatch_verdict_ignored(self) -> None:
        self.write_worker({"m1": {"ts": NOW - 2000,
                                  "source": "worker-register",
                                  "run_id": RUN}})
        self.write_verdict({"m1": {"run_id": "event-hook:someone-else",
                                   "status": "shutdown",
                                   "agent_updated_at": NOW - 700,
                                   "checked_at": NOW - 10}})
        info = self.assess()
        self.assertFalse(info["platform_terminal"])

    def test_verdict_without_registration_still_counts(self) -> None:
        # No worker record at all: the verdict is keyed by msgid and
        # carries the run_id itself -- usable evidence.
        self.write_verdict(self.verdict())
        self.assertTrue(self.assess()["platform_terminal"])

    def test_unknown_status_word_is_no_signal(self) -> None:
        self.write_verdict(self.verdict(status="unknown"))
        info = self.assess()
        self.assertFalse(info["platform_terminal"])
        self.assertFalse(info["alive"])
        self.assertIsNone(info["platform_status"])

    def test_live_hold_vetoes_terminal_verdict(self) -> None:
        self.write_worker({"m1": {"ts": NOW - 900, "source": "worker-hold",
                                  "run_id": RUN,
                                  "hold_until": NOW + 600}})
        self.write_verdict(self.verdict())
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "worker-hold")
        self.assertTrue(info["platform_terminal"])
        self.assertEqual(info["worker_hold_until"], NOW + 600)

    def test_expired_hold_does_not_veto(self) -> None:
        self.write_worker({"m1": {"ts": NOW - 2000, "source": "worker-hold",
                                  "run_id": RUN,
                                  "hold_until": NOW - 5}})
        self.write_verdict(self.verdict())
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertTrue(info["platform_terminal"])

    def test_fresh_handoff_vetoes_terminal_verdict(self) -> None:
        # 2026-10-29: a worker that handed the batch to the main
        # session must not be resumed or fail-stopped, even when
        # its platform session has ended.
        self.write_worker({"m1": {"ts": NOW - 900, "source": "worker-handoff",
                                  "run_id": RUN,
                                  "handoff": True, "handoff_at": NOW - 100}})
        self.write_verdict(self.verdict())
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "worker-handoff")
        self.assertTrue(info["platform_terminal"])
        self.assertTrue(info["worker_handoff"])

    def test_expired_handoff_does_not_veto(self) -> None:
        # Handoff is bounded to 24h: after that the normal death
        # route resumes, so a main session that never acts cannot
        # hide a batch forever.
        self.write_worker({"m1": {"ts": NOW - 90000, "source": "worker-handoff",
                                  "run_id": RUN,
                                  "handoff": True,
                                  "handoff_at": NOW - 86401}})
        self.write_verdict(self.verdict())
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertTrue(info["platform_terminal"])
        self.assertFalse(info["worker_handoff"])

    def test_old_formats_still_work(self) -> None:
        # Bare epoch record: ts still counts, no run_id, no hold.
        self.write_worker({"m1": NOW - 40})
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "worker-activity")
        self.assertIsNone(info["worker_run_id"])
        # Dict without run_id + a terminal verdict for another msgid.
        self.write_worker({"m1": {"ts": NOW - 40, "source": "worker"}})
        self.write_verdict({"other": {"status": "shutdown",
                                      "agent_updated_at": NOW - 9,
                                      "checked_at": NOW - 5}})
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertFalse(info["platform_terminal"])


class WriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "worker_activity.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def read(self) -> dict:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def test_register_beat_hold_roundtrip(self) -> None:
        cc.register_worker_activity(self.path, ["m1", "m2"], RUN, NOW)
        data = self.read()
        self.assertEqual(data["m1"]["run_id"], RUN)
        self.assertEqual(data["m1"]["source"], "worker-register")
        self.assertEqual(data["m2"]["ts"], NOW)
        cc.update_worker_activity(self.path, "m1", NOW + 120,
                                  progress="渲染中")
        data = self.read()
        self.assertEqual(data["m1"]["ts"], NOW + 120)
        self.assertEqual(data["m1"]["progress"], "渲染中")
        self.assertEqual(data["m1"]["run_id"], RUN)  # beat keeps it
        cc.update_worker_activity(self.path, "m1", NOW + 180,
                                  hold_secs=900)
        data = self.read()
        self.assertEqual(data["m1"]["hold_until"], NOW + 180 + 900)
        cc.update_worker_activity(self.path, "m1", NOW + 200, hold_secs=0)
        self.assertNotIn("hold_until", self.read()["m1"])

    def test_handoff_roundtrip(self) -> None:
        cc.register_worker_activity(self.path, ["m1"], RUN, NOW)
        cc.update_worker_activity(self.path, "m1", NOW + 60,
                                  handoff=True)
        data = self.read()
        self.assertTrue(data["m1"]["handoff"])
        self.assertEqual(data["m1"]["handoff_at"], NOW + 60)
        self.assertEqual(data["m1"]["source"], "worker-handoff")
        self.assertEqual(data["m1"]["run_id"], RUN)
        cc.update_worker_activity(self.path, "m1", NOW + 70,
                                  handoff=False)
        data = self.read()
        self.assertNotIn("handoff", data["m1"])
        self.assertNotIn("handoff_at", data["m1"])

    def test_register_preserves_other_records(self) -> None:
        self.path.write_text(json.dumps({"old": NOW - 5000,
                                         "m9": {"ts": NOW - 5,
                                                "source": "worker"}}),
                             encoding="utf-8")
        cc.register_worker_activity(self.path, ["m1"], RUN, NOW)
        data = self.read()
        self.assertEqual(data["old"], NOW - 5000)  # bare epoch untouched
        self.assertEqual(data["m9"]["source"], "worker")

    def test_corrupt_existing_file_self_heals(self) -> None:
        self.path.write_text("{nope", encoding="utf-8")
        cc.register_worker_activity(self.path, ["m1"], RUN, NOW)
        self.assertEqual(self.read()["m1"]["run_id"], RUN)


class CliTests(unittest.TestCase):
    """Both CLIs' worker-activity subcommand in a copied sandbox tree."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def build_tree(self, bot: str, cli_name: str) -> Path:
        tree = self.root / bot
        (tree / "state").mkdir(parents=True)
        shutil.copy(ROOT / "channel_common.py", self.root / "channel_common.py")
        shutil.copy(ROOT / bot / cli_name, tree / cli_name)
        # heartbeat.py stubbed: registration must not spawn a real loop.
        (tree / "heartbeat.py").write_text("pass\n", encoding="utf-8")
        return tree / cli_name

    def run_cli(self, cli: Path, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(cli), *argv],
            capture_output=True, text=True, timeout=30)

    def check_cli(self, bot: str, cli_name: str) -> None:
        cli = self.build_tree(bot, cli_name)
        state = self.root / bot / "state" / "worker_activity.json"
        r = self.run_cli(cli, "heartbeat-start", "--msgids", "m1,m2",
                         "--run-id", RUN)
        self.assertEqual(r.returncode, 0, r.stderr)
        data = json.loads(state.read_text(encoding="utf-8"))
        self.assertEqual(data["m1"]["run_id"], RUN)
        self.assertEqual(data["m2"]["run_id"], RUN)
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1",
                         "--beat", "--progress", "阶段二")
        self.assertEqual(r.returncode, 0, r.stderr)
        data = json.loads(state.read_text(encoding="utf-8"))
        self.assertEqual(data["m1"]["progress"], "阶段二")
        self.assertEqual(data["m1"]["run_id"], RUN)
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1",
                         "--hold-secs", "600")
        self.assertEqual(r.returncode, 0, r.stderr)
        data = json.loads(state.read_text(encoding="utf-8"))
        self.assertGreater(data["m1"]["hold_until"], time.time())
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1",
                         "--handoff")
        self.assertEqual(r.returncode, 0, r.stderr)
        data = json.loads(state.read_text(encoding="utf-8"))
        self.assertTrue(data["m1"]["handoff"])
        self.assertGreater(data["m1"]["handoff_at"], 0)
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1",
                         "--beat", "--handoff")
        self.assertEqual(r.returncode, 2)  # exactly one mode required
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1")
        self.assertEqual(r.returncode, 2)  # exactly one mode required
        r = self.run_cli(cli, "worker-activity", "--msgid", "m1",
                         "--register")
        self.assertEqual(r.returncode, 2)  # register needs --run-id

    def test_weixin_cli(self) -> None:
        self.check_cli("weixin-bot", "weixin_cli.py")

    def test_wecom_cli(self) -> None:
        self.check_cli("wecom-bot", "wecom_cli.py")


if __name__ == "__main__":
    unittest.main()
