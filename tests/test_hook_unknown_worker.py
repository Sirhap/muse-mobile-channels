"""Unknown platform executions must enter review without duplicate execution."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ops.cold_death_drill import Case, entry, prepare_sandbox, write_json, write_jsonl

RUN = "event-hook:review-original"


class UnknownWorkerTests(unittest.TestCase):
    """Run actual hook programs against fake inboxes, routes and CLI processes."""

    def make_case(self, channel, *, resumed=False, run_id=RUN, detached=False):
        """Prepare a stale registration and isolate every production path."""
        temporary = tempfile.TemporaryDirectory(prefix="unknown-worker-")
        self.addCleanup(temporary.cleanup)
        sandbox = Path(temporary.name)
        prepare_sandbox(sandbox, channel)
        case = Case(sandbox, channel)
        case.reset()
        record = {"ts": time.time() - 1900, "source": "worker-register"}
        if run_id:
            record["run_id"] = run_id
        case.plant("original", 4000, "test unknown worker", resume_attempt=resumed,
                   worker_activity={"original": record}, detached=detached)
        if resumed:
            write_json(case.hookst / "recovery_attempts.json", {"original": {
                "source_run_id": "event-hook:earlier-execution", "attempted_at": time.time() - 60}})
        return case

    def reviews(self, case):
        """Read the durable review ledger, as a later hook invocation would."""
        return json.loads((case.hookst / "worker_review.json").read_text())["tasks"]

    def verdict(self, case, *, run_id=RUN, status="shutdown", age=0):
        """Write a platform snapshot with an explicit execution binding."""
        write_json(case.bot / "worker_platform_status.json", {"original": {
            "run_id": run_id, "status": status, "checked_at": time.time() - age,
            "agent_updated_at": time.time() - 1900}})

    def assert_reviewed(self, case):
        """A stale task is visible, blocked from duplicate scheduling and not cancelled."""
        self.assertEqual(case.returncode, 0, case.stderr)
        self.assertNotIn("CALL: cancel", case.calls())
        self.assertNotIn("DECISION wake", case.stdout)
        self.assertEqual(self.reviews(case)["original"]["status"], "pending")
        self.assertIn("original", self.reviews(case))

    def test_unknown_first_and_second_loss_never_resume_or_cancel(self):
        """Silence and an expired registration cannot prove execution termination."""
        for channel in ("weixin", "wecom"):
            for resumed in (False, True):
                with self.subTest(channel=channel, resumed=resumed):
                    case = self.make_case(channel, resumed=resumed)
                    before = case.attempts()
                    case.run()
                    self.assert_reviewed(case)
                    case.run()
                    self.assert_reviewed(case)
                    self.assertEqual(case.attempts(), before)
                    self.assertFalse(case.batch().get("msgids"))
                    rows = [json.loads(line) for line in (case.bot / "outbox.jsonl").read_text().splitlines()]
                    self.assertEqual(len(rows), 1)
                    self.assertIn("待核查", rows[0]["content"])

    def test_missing_mismatched_stale_or_unknown_verdict_stays_in_review(self):
        """Only a fresh terminal verdict bound to the registered run is decisive."""
        for channel in ("weixin", "wecom"):
            for run_id, verdict_run, status, age in (
                (None, RUN, "shutdown", 0), (RUN, "other", "shutdown", 0),
                (RUN, RUN, "shutdown", 601), (RUN, RUN, "unknown", 0),
                (RUN, "", "shutdown", 0)):
                with self.subTest(channel=channel, run_id=run_id, verdict_run=verdict_run, age=age):
                    case = self.make_case(channel, run_id=run_id)
                    self.verdict(case, run_id=verdict_run, status=status, age=age)
                    case.run()
                    self.assert_reviewed(case)

    def test_fresh_same_run_terminal_retains_bounded_recovery(self):
        """Confirmed termination still resumes once and stops after a second loss."""
        for channel in ("weixin", "wecom"):
            for resumed in (False, True):
                with self.subTest(channel=channel, resumed=resumed):
                    case = self.make_case(channel, resumed=resumed)
                    self.verdict(case)
                    case.run()
                    self.assertEqual(case.returncode, 0, case.stderr)
                    self.assertEqual("CALL: cancel" in case.calls(), resumed)
                    self.assertEqual("DECISION wake" in case.stdout, not resumed)

    def test_pending_task_can_recover_after_terminal_evidence_arrives(self):
        """Review does not erase execution identity or permanently suppress recovery."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                case.run()
                self.assert_reviewed(case)
                self.verdict(case)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("DECISION wake", case.stdout)
                self.assertIn("original", case.attempts())
                self.assertEqual(self.reviews(case)["original"]["status"], "resolved")

    def test_pending_does_not_starve_or_capture_new_messages(self):
        """An old unknown task must not take a fresh message's worker slot."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                write_json(case.hookst / "starvation.json", {"original": {"carried": 4, "last_wake": 1}})
                case.run()
                self.assert_reviewed(case)
                rows = [json.loads(line) for line in (case.bot / "inbox.jsonl").read_text().splitlines()]
                rows.append(entry(channel, "fresh", time.time(), "new unrelated question"))
                write_jsonl(case.bot / "inbox.jsonl", rows)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("DECISION wake", case.stdout)
                self.assertEqual(case.batch().get("msgids"), ["fresh"])
                self.assertNotIn("CALL: cancel", case.calls())

    def test_detached_unknown_task_is_also_blocked(self):
        """Moving a batch to detached cannot bypass the review policy."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel, detached=True, resumed=True)
                case.run()
                self.assert_reviewed(case)
                self.assertFalse(case.batch().get("detached"))

    def test_missing_probe_does_not_turn_unknown_into_failure(self):
        """A broken diagnostic dependency cannot authorize cancellation."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel, resumed=True)
                with patch.dict(os.environ, {"DEATH_WATCH_ACTIVITY_PY": str(case.sandbox / "missing.py")}):
                    case.run()
                self.assert_reviewed(case)

    def test_original_hook_is_parseable_by_system_bash(self):
        """The production hook must be runnable by macOS Bash 3 without test rewrites."""
        import subprocess
        for channel in ("weixin", "wecom"):
            result = subprocess.run(["bash", "-n", str(ROOT / "hooks/scripts" / f"{channel}-inbox.sh")],
                                    capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_late_formal_reply_resolves_only_after_delivery(self):
        """Waiting for review must not cancel a still-pending formal delivery."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel, resumed=True)
                case.run()
                self.assert_reviewed(case)
                with (case.bot / "outbox.jsonl").open("a") as output:
                    output.write(json.dumps({"id": "late-formal", "mode": "reply", "msgid": "original",
                                             "content": "done", "queued_at": time.time()}) + "\n")
                case.run()
                self.assert_reviewed(case)
                write_jsonl(case.bot / "outbox_results.jsonl", [
                    {"id": "late-formal", "mode": "reply", "ok": True, "ts": time.time()}])
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertEqual(self.reviews(case)["original"]["resolution"], "reply_delivered")
                self.assertNotIn("original", case.attempts())
                self.assertNotIn("CALL: cancel", case.calls())

    def test_explicit_cancellation_resolves_review_without_replay(self):
        """An operator stop remains authoritative even with an old resume queue."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                case.run()
                write_json(case.bot / "cancelled.json", [{"msgid": "original", "ts": time.time()}])
                write_json(case.hookst / "resume_queue.json", {"original": time.time()})
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertEqual(self.reviews(case)["original"]["resolution"], "cancelled")
                self.assertNotIn("DECISION wake", case.stdout)

    def test_crash_between_review_commit_and_batch_clear_cannot_resume_twice(self):
        """A task present in both ledgers must receive one decision per poll."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                case.run()
                write_json(case.hookst / "active_batch.json", {
                    "msgids": ["original"], "since": time.time() - 4000, "detached": []})
                self.verdict(case)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("DECISION wake", case.stdout)
                self.assertNotIn("CALL: cancel", case.calls())

    def test_old_recovery_budget_survives_a_delayed_terminal_verdict(self):
        """Waiting more than two hours cannot reset the once-only recovery cap."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                write_json(case.hookst / "resume_attempts.json", {"original": time.time() - 9000})
                write_json(case.hookst / "recovery_attempts.json", {"original": {
                    "source_run_id": "event-hook:earlier-execution", "attempted_at": time.time() - 9000}})
                case.run()
                self.assert_reviewed(case)
                self.verdict(case)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("CALL: cancel", case.calls())
                self.assertNotIn("DECISION wake", case.stdout)

    def test_failed_review_notice_retries_once_with_stable_id(self):
        """An invalid outbox cannot erase review or manufacture a sent notice."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                (case.bot / "outbox.jsonl").write_text('{"unfinished":')
                case.run()
                self.assert_reviewed(case)
                review = self.reviews(case)["original"]
                self.assertFalse(review["notice_enqueued"])
                qid = review["queued_id"]
                (case.bot / "outbox.jsonl").write_text("")
                case.run()
                case.run()
                rows = (case.bot / "outbox.jsonl").read_text().splitlines()
                self.assertEqual(len(rows), 1)
                self.assertEqual(json.loads(rows[0])["id"], qid)
                write_jsonl(case.bot / "outbox_results.jsonl", [{"id": qid, "ok": True}])
                case.run()
                self.assertTrue(self.reviews(case)["original"]["notice_delivered"])

    def test_fresh_bound_progress_vetoes_immediate_recovery(self):
        """Pending review must still respect recent real worker pushes."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                case.run()
                self.verdict(case)
                with (case.bot / "outbox.jsonl").open("a") as output:
                    output.write(json.dumps({"id": "real-progress", "mode": "update", "msgid": "original",
                                             "content": "working", "queued_at": time.time()}) + "\n")
                case.run()
                self.assert_reviewed(case)

    def test_platform_requests_restart_for_new_run_and_after_throttle_window(self):
        """Review keeps asking for evidence instead of spending two requests forever."""
        for channel in ("weixin", "wecom"):
            for stale_window in (False, True):
                with self.subTest(channel=channel, stale_window=stale_window):
                    case = self.make_case(channel)
                    write_json(case.hookst / "platform_check_requests.json", {"requests": {"original": {
                        "run_id": RUN if stale_window else "old-run", "count": 2,
                        "window_started_at": time.time() - (601 if stale_window else 30),
                        "last_request_ts": time.time() - 30, "requested_at": time.time() - 30}}})
                    case.run()
                    self.assert_reviewed(case)
                    request = json.loads((case.hookst / "platform_check_requests.json").read_text())["requests"]["original"]
                    self.assertEqual(request["run_id"], RUN)
                    self.assertEqual(request["count"], 1)

    def test_corrupt_review_or_missing_helper_fails_closed(self):
        """A broken review dependency cannot silently grant replay permission."""
        for channel in ("weixin", "wecom"):
            for missing in (False, True):
                with self.subTest(channel=channel, missing=missing):
                    case = self.make_case(channel, resumed=True)
                    if not missing:
                        (case.hookst / "worker_review.json").write_text('{"tasks":{"original":"broken"}}')
                    environment = {"PENDING_ANSWERS_PY": str(case.sandbox / "not-installed.py")} if missing else {}
                    with patch.dict(os.environ, environment):
                        case.run()
                    self.assertNotEqual(case.returncode, 0)
                    self.assertNotIn("CALL: cancel", case.calls())
                    self.assertNotIn("DECISION wake", case.stdout)

    def test_old_terminal_cannot_be_used_as_replacement_execution_termination(self):
        """Slow replacement startup must not reuse the original terminal twice."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                self.verdict(case)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("DECISION wake", case.stdout)
                primary = json.loads((case.hookst / "recovery_attempts.json").read_text())
                self.assertEqual(primary["original"]["source_run_id"], RUN)
                write_json(case.hookst / "active_batch.json", {
                    "msgids": ["original"], "since": time.time() - 400, "detached": []})
                self.verdict(case)
                case.run()
                self.assert_reviewed(case)
                self.assertEqual(self.reviews(case)["original"]["reason"], "recovery_already_consumed_for_run")
                # Only an independently registered, subsequently ended new run
                # can prove that the once-only recovery has itself ended.
                new_run = "event-hook:replacement-execution"
                write_json(case.bot / "worker_activity.json", {"original": {
                    "run_id": new_run, "ts": time.time() - 1900, "source": "worker-register"}})
                self.verdict(case, run_id=new_run)
                case.run()
                self.assertEqual(case.returncode, 0, case.stderr)
                self.assertIn("CALL: cancel", case.calls())
                self.assertNotIn("DECISION wake", case.stdout)

    def test_legacy_float_budget_without_source_identity_never_authorizes_stop(self):
        """Historical budgets cannot identify which execution was actually resumed."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                write_json(case.hookst / "resume_attempts.json", {"original": time.time() - 60})
                self.verdict(case)
                case.run()
                self.assert_reviewed(case)
                self.assertEqual(self.reviews(case)["original"]["reason"], "recovery_run_identity_unknown")

    def test_corrupt_or_unreadable_recovery_authority_never_dispatches(self):
        """Budget read failures cannot silently reset the once-only allowance."""
        for channel in ("weixin", "wecom"):
            for filename in ("resume_attempts.json", "recovery_attempts.json"):
                for bad_state in ("directory", "invalid_json", "invalid_shape"):
                    with self.subTest(channel=channel, filename=filename, bad_state=bad_state):
                        case = self.make_case(channel)
                        path = case.hookst / filename
                        if bad_state == "directory":
                            path.mkdir()
                        else:
                            path.write_text("{" if bad_state == "invalid_json" else "[]")
                        self.verdict(case)
                        case.run()
                        self.assertNotEqual(case.returncode, 0)
                        self.assertNotIn("DECISION wake", case.stdout)
                        self.assertNotIn("CALL: cancel", case.calls())
                        self.assertEqual(case.batch()["msgids"], ["original"])

    def test_replay_queue_commit_failure_cannot_reauthorize_the_original_run(self):
        """A reserved budget survives a later disk error without issuing a wake."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                queue = case.hookst / "resume_queue.json"
                queue.mkdir()
                self.verdict(case)
                case.run()
                self.assertNotEqual(case.returncode, 0)
                self.assertNotIn("DECISION wake", case.stdout)
                self.assertNotIn("CALL: cancel", case.calls())
                primary = json.loads((case.hookst / "recovery_attempts.json").read_text())
                self.assertEqual(primary["original"]["source_run_id"], RUN)
                queue.rmdir()
                case.run()
                self.assert_reviewed(case)

    def test_terminal_flag_without_registered_identity_is_not_authority(self):
        """An old/incomplete probe contract cannot grant unbound replay permission."""
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                case = self.make_case(channel)
                probe = case.sandbox / "old_probe.py"
                probe.write_text("def assess(*args, **kwargs):\n"
                                 "    return {'alive': False, 'platform_terminal': True}\n")
                with patch.dict(os.environ, {"DEATH_WATCH_ACTIVITY_PY": str(probe)}):
                    case.run()
                self.assert_reviewed(case)


if __name__ == "__main__":
    unittest.main()
