"""A platform verdict can prove only the currently registered worker run."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import death_watch_activity as probe

NOW = 1_800_000_000.0
RUN = "event-hook:current"


class VerdictBindingTests(unittest.TestCase):
    """Exercise the real read-only probe with isolated registration snapshots."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def assess(self, worker, verdict):
        """Write explicit test evidence, leaving unrelated sources absent."""
        worker_path = self.directory / "worker_activity.json"
        verdict_path = self.directory / "worker_platform_status.json"
        worker_path.write_text(json.dumps(worker), encoding="utf-8")
        verdict_path.write_text(json.dumps(verdict), encoding="utf-8")
        return probe.assess("m1", now=NOW, window=1800,
                            bridge_state_path=str(self.directory / "absent.json"),
                            worker_activity_path=str(worker_path),
                            platform_status_path=str(verdict_path))

    def verdict(self, **changes):
        """Return a fresh terminal observation tied to the current run."""
        return {"m1": dict(run_id=RUN, status="shutdown", checked_at=NOW - 5,
                           agent_updated_at=NOW - 800, **changes)}

    def test_verdict_without_current_registration_is_not_terminal_proof(self):
        info = self.assess({}, self.verdict())
        self.assertFalse(info["platform_terminal"])
        self.assertEqual(info["platform_verdict_reason"], "unbound")

    def test_verdict_without_run_id_cannot_terminate_a_registered_run(self):
        for missing in (None, "", 123):
            with self.subTest(run_id=missing):
                verdict = self.verdict()
                verdict["m1"]["run_id"] = missing
                info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
                self.assertFalse(info["platform_terminal"])
                self.assertEqual(info["platform_verdict_reason"], "unbound")

    def test_mismatched_run_cannot_supply_running_or_terminal_evidence(self):
        for status in ("shutdown", "running"):
            with self.subTest(status=status):
                verdict = self.verdict()
                verdict["m1"].update(run_id="event-hook:retired", status=status,
                                     agent_updated_at=NOW - 5)
                info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
                self.assertFalse(info["platform_terminal"])
                self.assertFalse(info["alive"])
                self.assertEqual(info["platform_verdict_reason"], "run_mismatch")

    def test_matched_terminal_retains_only_safe_verification_metadata(self):
        verdict = self.verdict()
        verdict["m1"]["private_detail"] = "SHOULD_NOT_ESCAPE"
        info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
        self.assertTrue(info["platform_terminal"])
        self.assertEqual(info["platform_checked_at"], NOW - 5)
        self.assertEqual(info["platform_verdict_reason"], "verified")
        self.assertNotIn("SHOULD_NOT_ESCAPE", json.dumps(info))

    def test_stale_unknown_and_stale_running_remain_unverified(self):
        cases = (({"checked_at": NOW - 601}, "stale"),
                 ({"status": "unknown"}, "unknown"),
                 ({"status": "running", "agent_updated_at": NOW - 301}, "running_stale"))
        for changes, reason in cases:
            with self.subTest(reason=reason):
                verdict = self.verdict()
                verdict["m1"].update(changes)
                info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
                self.assertFalse(info["platform_terminal"])
                self.assertFalse(info["alive"])
                self.assertEqual(info["platform_verdict_reason"], reason)

    def test_missing_verdict_has_explicit_reason(self):
        info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, {})
        self.assertFalse(info["platform_terminal"])
        self.assertEqual(info["platform_verdict_reason"], "missing")

    def test_running_without_current_registration_is_not_liveness(self):
        """A stale run keyed only by msgid cannot protect a newer unknown execution."""
        verdict = self.verdict()
        verdict["m1"].update(status="running", agent_updated_at=NOW - 5)
        for worker in ({}, {"m1": {"run_id": " ", "ts": NOW - 2000}}):
            with self.subTest(worker=worker):
                info = self.assess(worker, verdict)
                self.assertFalse(info["alive"])
                self.assertFalse(info["platform_terminal"])
                self.assertEqual(info["platform_verdict_reason"], "unbound")
                self.assertIsNone(info["platform_checked_at"])

    def test_bound_fresh_running_evidence_remains_alive(self):
        """Strict binding does not reject a legitimate current running observation."""
        verdict = self.verdict()
        verdict["m1"].update(status="running", agent_updated_at=NOW - 5)
        info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "platform-running")
        self.assertFalse(info["platform_terminal"])
        self.assertEqual(info["platform_verdict_reason"], "verified")

    def test_future_verdict_is_not_terminal_proof(self):
        """Future timestamps beyond allowed skew are not observations."""
        verdict = self.verdict()
        verdict["m1"]["checked_at"] = NOW + 121
        info = self.assess({"m1": {"run_id": RUN, "ts": NOW - 2000}}, verdict)
        self.assertFalse(info["platform_terminal"])
        self.assertEqual(info["platform_verdict_reason"], "stale")


if __name__ == "__main__":
    unittest.main()
