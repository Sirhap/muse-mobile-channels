#!/usr/bin/env python3
"""Observation-window reconciliation for the platform death verdict.

Synthetic fixtures only. The script must not write into the fixture,
and the probe thresholds stay at the deployed initial values.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ops"))

import death_watch_activity as probe
import death_watch_reconcile as tool


NOW = 1_800_000_000.0
RUN = "event-hook:11111111-2222-3333-4444-555555555555"
OTHER_RUN = "event-hook:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
MARK = "平台记录显示原执行会话已结束"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")


def _snapshot(root: Path) -> dict[str, bytes]:
    found: dict[str, bytes] = {}
    for path in root.rglob("*"):
        if path.is_file():
            found[path.relative_to(root).as_posix()] = path.read_bytes()
    return found


def _verdict(status: str, checked: float, **extra: object) -> dict:
    row = {
        "run_id": RUN,
        "status": status,
        "agent_updated_at": NOW - 700,
        "checked_at": checked,
    }
    row.update(extra)
    return row


class ReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.hooks = self.root / "hooks"
        self.bot = self.root / "bot"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def reconcile(self, hooks: Path | None = None, **kwargs: object) -> dict:
        return tool.reconcile(hooks or self.hooks, now=NOW, **kwargs)

    def test_thresholds_stay_at_deployed_initial_values(self) -> None:
        # These are the initial verdict TTL and running-freshness
        # windows. The reconciler only reads them.
        self.assertEqual(probe.PLATFORM_VERDICT_MAX_AGE_SECS, 600.0)
        self.assertEqual(probe.PLATFORM_RUNNING_FRESH_SECS, 300.0)
        self.assertEqual(
            set(probe.PLATFORM_TERMINAL_STATUSES),
            {"completed", "shutdown", "errored", "interrupted"},
        )
        (self.hooks / "weixin-bot").mkdir(parents=True)
        report = self.reconcile()
        thresholds = report["thresholds"]
        self.assertEqual(thresholds["verdict_ttl_secs"], 600.0)
        self.assertEqual(thresholds["running_fresh_secs"], 300.0)
        self.assertEqual(thresholds["early_platform_min_silence_secs"], 300)
        self.assertEqual(thresholds["platform_check_every_secs"], 300)
        self.assertEqual(thresholds["death_watch_secs"], 1800)
        self.assertFalse(thresholds["silence_gate_fallback"])
        self.assertTrue(thresholds["silence_gates_agree"])

    def test_source_never_writes_state(self) -> None:
        source = (ROOT / "ops" / "death_watch_reconcile.py").read_text(encoding="utf-8")
        self.assertNotIn(".write_text", source)
        self.assertNotIn(".write_bytes", source)
        self.assertNotIn("os.replace", source)
        self.assertNotIn("mkdir", source)
        self.assertIsNone(re.search(r"open\([^)]*['\"][wa]", source))

    def test_empty_directory_reports_zeros(self) -> None:
        (self.hooks / "weixin-bot").mkdir(parents=True)
        (self.hooks / "wecom-bot").mkdir(parents=True)
        report = self.reconcile()
        self.assertEqual(report["verdict_file_count"], 0)
        self.assertEqual(report["verdict_record_count"], 0)
        self.assertEqual(sum(report["terminal_status_counts"].values()), 0)
        self.assertEqual(report["early_platform"]["trigger_count"], 0)
        self.assertEqual(report["false_positive_clues"], [])
        self.assertEqual(report["false_negative_clues"], [])
        self.assertEqual(report["false_negative_hard_count"], 0)
        self.assertTrue(report["read_only"])

    def test_status_and_freshness_distribution(self) -> None:
        weixin = self.hooks / "weixin-bot"
        wecom = self.hooks / "wecom-bot"
        _write_json(weixin / "worker_platform_status.json", {
            "m_fresh_30": _verdict("completed", NOW - 30),
            "m_fresh_120": _verdict("shutdown", NOW - 120),
            "m_fresh_450": _verdict("errored", NOW - 450),
            "m_stale_600": _verdict("interrupted", NOW - 600),
            "m_stale_1800": _verdict("shutdown", NOW - 1800),
            "m_missing": {"status": "completed", "run_id": RUN},
            "m_future": _verdict("shutdown", NOW + 1000),
            "m_running_fresh": _verdict(
                "running", NOW - 10, agent_updated_at=NOW - 20),
            "m_running_update_stale": _verdict(
                "running", NOW - 10, agent_updated_at=NOW - 400),
            "m_running_checked_stale": _verdict(
                "running", NOW - 700, agent_updated_at=NOW - 10),
            "m_unknown": _verdict("unknown", NOW - 15),
            "m_other": _verdict("weird", NOW - 15),
            "m_invalid": "not-a-record",
        })
        _write_json(wecom / "worker_platform_status.json", {
            "c1": _verdict("shutdown", NOW - 5),
        })
        report = self.reconcile()
        self.assertEqual(report["verdict_file_count"], 2)
        self.assertEqual(report["verdict_record_count"], 14)
        self.assertEqual(report["terminal_status_counts"], {
            "completed": 2,
            "shutdown": 4,
            "errored": 1,
            "interrupted": 1,
        })
        self.assertEqual(report["status_counts"]["running"], 3)
        self.assertEqual(report["status_counts"]["unknown"], 1)
        self.assertEqual(report["status_counts"]["other"], 1)
        self.assertEqual(report["status_counts"]["invalid"], 1)
        self.assertEqual(report["other_status_counts"], {"weird": 1})
        fresh = report["freshness"]
        self.assertEqual(fresh["buckets"], {
            "lt_60": 6,
            "60_to_300": 1,
            "300_to_600": 1,
            "600_to_1800": 2,
            "ge_1800": 1,
        })
        self.assertEqual(fresh["fresh"], 8)
        self.assertEqual(fresh["stale"], 3)
        self.assertEqual(fresh["missing"], 2)
        self.assertEqual(fresh["future_rejected"], 1)
        running = report["running_freshness"]
        self.assertEqual(running["running_records"], 3)
        self.assertEqual(running["checked_fresh_update_fresh"], 1)
        self.assertEqual(running["checked_fresh_update_stale"], 1)
        self.assertEqual(running["checked_stale"], 1)
        self.assertEqual(running["checked_fresh_update_missing"], 0)
        usable = set(report["channels"][0]["usable_terminal_msgids"]) | set(
            report["channels"][1]["usable_terminal_msgids"])
        self.assertEqual(usable, {"m_fresh_30", "m_fresh_120", "m_fresh_450", "c1"})
        self.assertEqual(report["usable_terminal_count"], 4)
        # Age equal to the TTL is already invisible to the probe.
        self.assertNotIn("m_stale_600", usable)
        text = tool.render_text(report)
        self.assertIn("裁决文件 2", text)
        self.assertIn("completed 2", text)
        self.assertIn("EARLY_PLATFORM 触发 0", text)

    def test_early_platform_notices_are_deduped(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_jsonl(channel / "outbox.jsonl", [
            {"id": "abc", "mode": "send", "queued_at": NOW - 30,
             "content": "♻️ 任务中断，已自动续跑：「视频」（" + MARK + "）"},
            {"id": "plain", "mode": "send", "queued_at": NOW - 20,
             "content": "普通通知，不是判死"},
            {"id": "quoted", "mode": "reply", "msgid": "m9",
             "queued_at": NOW - 15, "content": "用户复述 " + MARK},
        ])
        _write_json(channel / "failed_notices.json", [
            {"ts": NOW - 30, "to": "u", "queued_id": "abc",
             "text": "⚠️ 任务已停止：「视频」（" + MARK + "，自动续跑后仍无推送；非确认失败）"},
            {"ts": NOW - 5, "to": "u",
             "text": "⚠️ 任务已停止：「另一条」（" + MARK + "，自动续跑后仍无推送；非确认失败）"},
        ])
        report = self.reconcile()
        early = report["early_platform"]
        self.assertEqual(early["trigger_count"], 2)
        self.assertEqual(early["resume_notice_count"], 1)
        self.assertEqual(early["fail_stop_notice_count"], 1)
        self.assertEqual(early["pending_unsent_count"], 1)
        self.assertEqual(
            [event["source"] for event in early["events"]],
            ["outbox", "failed_notices"],
        )
        self.assertEqual(report["false_positive_clues"], [])

    def test_same_notice_text_is_not_counted_twice(self) -> None:
        channel = self.hooks / "weixin-bot"
        text = "♻️ 续跑（" + MARK + "）"
        _write_jsonl(channel / "outbox.jsonl", [
            {"id": "s1", "mode": "send", "queued_at": NOW - 10, "content": text},
        ])
        _write_json(channel / "failed_notices.json", [
            {"ts": NOW - 12, "to": "u", "text": text},
        ])
        report = self.reconcile()
        self.assertEqual(report["early_platform"]["trigger_count"], 1)

    def test_terminal_then_formal_reply_is_false_positive(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "worker_platform_status.json", {
            "m1": _verdict("shutdown", NOW - 50),
            "m_before": _verdict("completed", NOW - 10),
            "m_equal": _verdict("errored", NOW - 40),
            "m_update": _verdict("interrupted", NOW - 30),
        })
        _write_jsonl(channel / "outbox.jsonl", [
            {"id": "r1", "mode": "reply", "msgid": "m1",
             "queued_at": NOW - 10, "content": "做完了"},
            {"id": "r2", "mode": "reply_file", "msgid": "m1",
             "queued_at": NOW - 5, "content": "文件"},
            {"id": "old", "mode": "reply", "msgid": "m_before",
             "queued_at": NOW - 20, "content": "更早的回复"},
            {"id": "eq", "mode": "reply", "msgid": "m_equal",
             "queued_at": NOW - 40, "content": "同一秒"},
            {"id": "up", "mode": "update", "msgid": "m_update",
             "queued_at": NOW - 1, "content": "进度"},
        ])
        _write_jsonl(channel / "outbox_results.jsonl", [
            {"id": "r1", "mode": "reply", "ok": True, "ts": NOW - 8},
            {"id": "r2", "mode": "reply_file", "ok": False, "ts": NOW - 4},
        ])
        before = _snapshot(self.hooks)
        report = self.reconcile()
        self.assertEqual(_snapshot(self.hooks), before)
        clues = report["false_positive_clues"]
        self.assertEqual([clue["msgid"] for clue in clues], ["m1"])
        self.assertEqual(clues[0]["verdict_status"], "shutdown")
        self.assertEqual(
            [(reply["mode"], reply["delivered"]) for reply in clues[0]["replies"]],
            [("reply", True), ("reply_file", False)],
        )
        info = probe.assess(
            "m1", now=NOW, window=1800.0,
            bridge_state_path=str(channel / "missing-bridge.json"),
            worker_activity_path=str(channel / "worker_activity.json"),
            platform_status_path=str(channel / "worker_platform_status.json"),
        )
        self.assertTrue(info["platform_terminal"])
        self.assertIn("m1", report["channels"][0]["usable_terminal_msgids"])

    def test_parked_formal_reply_after_verdict_is_false_positive(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "worker_platform_status.json", {
            "m1": _verdict("shutdown", NOW - 80),
        })
        _write_json(channel / "outbox_parked.json", {
            "p1": {"item": {"id": "p1", "mode": "reply", "msgid": "m1",
                            "queued_at": NOW - 20, "content": "补上"},
                   "parked_at": NOW - 15, "n": 2},
        })
        report = self.reconcile()
        clues = report["false_positive_clues"]
        self.assertEqual(len(clues), 1)
        self.assertEqual(clues[0]["replies"][0]["source"], "parked")
        self.assertFalse(clues[0]["replies"][0]["delivered"])

    def test_silence_without_verdict_is_false_negative(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["m1"], "since": NOW - 500,
            "detached": [{"msgids": ["m2"], "since": NOW - 450}],
        })
        _write_json(channel / "worker_activity.json", {
            "m1": {"ts": NOW - 500, "source": "worker-register", "run_id": RUN},
            "m2": {"ts": NOW - 450, "source": "worker-register", "run_id": RUN},
        })
        report = self.reconcile()
        kinds = {(clue["role"], clue["kind"], clue["stage"])
                 for clue in report["false_negative_clues"]}
        self.assertEqual(kinds, {
            ("active", "silence_without_verdict", "no_request"),
            ("detached", "silence_without_verdict", "no_request"),
        })
        self.assertEqual(report["false_negative_hard_count"], 2)
        self.assertTrue(report["false_negative_clues"][0]["bridge_unchecked"])

    def test_request_stage_splits_hard_and_awaiting(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "worker_activity.json", {
            "wait": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "late": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
        })
        _write_json(channel / "platform_check_requests.json", {
            "requests": {
                "wait": {"run_id": RUN, "count": 1, "requested_at": NOW - 10,
                         "last_request_ts": NOW - 10, "silence_secs": 490},
                "late": {"run_id": RUN, "count": 2, "requested_at": NOW - 400,
                         "last_request_ts": NOW - 400, "silence_secs": 100},
            },
            "updated": NOW - 10,
        })
        report = self.reconcile()
        by_id = {clue["msgids"][0]: clue for clue in report["false_negative_clues"]}
        self.assertEqual(by_id["wait"]["stage"], "awaiting_checker")
        self.assertEqual(by_id["late"]["stage"], "request_unanswered")
        self.assertEqual(by_id["late"]["request_counts"], {"late": 2})
        self.assertEqual(report["false_negative_hard_count"], 1)

    def test_usable_terminal_hold_running_and_reply_suppress_leak(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["terminal"], "since": NOW - 500,
            "detached": [
                {"msgids": ["held"], "since": NOW - 500},
                {"msgids": ["running"], "since": NOW - 500},
                {"msgids": ["answered"], "since": NOW - 500},
                {"msgids": ["freshbeat"], "since": NOW - 500},
            ],
        })
        _write_json(channel / "worker_activity.json", {
            "terminal": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "held": {"ts": NOW - 500, "run_id": RUN, "source": "worker-hold",
                     "hold_until": NOW + 30},
            "running": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "answered": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "freshbeat": {"ts": NOW - 40, "run_id": RUN, "source": "worker"},
        })
        _write_json(channel / "worker_platform_status.json", {
            "terminal": _verdict("shutdown", NOW - 10),
            "held": _verdict("shutdown", NOW - 10),
            "running": _verdict("running", NOW - 10, agent_updated_at=NOW - 20),
        })
        _write_jsonl(channel / "outbox.jsonl", [
            {"id": "ans", "mode": "reply", "msgid": "answered",
             "queued_at": NOW - 400, "content": "已经答了"},
        ])
        report = self.reconcile()
        self.assertEqual(report["false_negative_clues"], [])
        self.assertEqual(
            report["channels"][0]["usable_terminal_msgids"],
            ["held", "terminal"],
        )
        running = probe.assess(
            "running", now=NOW, window=300.0,
            bridge_state_path=str(channel / "no-bridge.json"),
            worker_activity_path=str(channel / "worker_activity.json"),
            platform_status_path=str(channel / "worker_platform_status.json"),
        )
        self.assertTrue(running["alive"])
        self.assertEqual(running["source"], "platform-running")
        self.assertFalse(running["platform_terminal"])

    def test_stale_unknown_mismatch_and_flip_lag(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["stale", "unknown", "mismatch", "lag", "noreg"],
            "since": NOW - 500,
        })
        _write_json(channel / "worker_activity.json", {
            "stale": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "unknown": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "mismatch": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
            "lag": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
        })
        _write_json(channel / "worker_platform_status.json", {
            "stale": _verdict("shutdown", NOW - 600),
            "unknown": _verdict("unknown", NOW - 20, agent_updated_at=0),
            "mismatch": _verdict("completed", NOW - 10, run_id=OTHER_RUN),
            "lag": _verdict("running", NOW - 10, agent_updated_at=NOW - 300),
        })
        report = self.reconcile()
        by_id = {clue["msgids"][0]: clue["kind"] for clue in report["false_negative_clues"]}
        # The batch is one group, so one clue covers every msgid. The
        # most specific label in the group wins: running update is
        # exactly 300s old, which the probe does not call fresh.
        self.assertEqual(by_id, {"stale": "terminal_flip_lag"})
        self.assertIn("lag", report["false_negative_clues"][0]["verdicts"])
        self.assertEqual(
            report["false_negative_clues"][0]["verdicts"]["lag"],
            "running_update_stale",
        )
        self.assertEqual(
            report["false_negative_clues"][0]["verdicts"]["stale"],
            "stale:shutdown",
        )
        self.assertEqual(
            report["false_negative_clues"][0]["verdicts"]["mismatch"],
            "run_mismatch",
        )
        stale = probe.assess(
            "stale", now=NOW, window=1800.0,
            bridge_state_path=str(channel / "no-bridge.json"),
            worker_activity_path=str(channel / "worker_activity.json"),
            platform_status_path=str(channel / "worker_platform_status.json"),
        )
        self.assertFalse(stale["platform_terminal"])
        mismatch = probe.assess(
            "mismatch", now=NOW, window=1800.0,
            bridge_state_path=str(channel / "no-bridge.json"),
            worker_activity_path=str(channel / "worker_activity.json"),
            platform_status_path=str(channel / "worker_platform_status.json"),
        )
        self.assertFalse(mismatch["platform_terminal"])

    def test_separate_batches_keep_their_own_leak_kind(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["noreg"], "since": NOW - 400,
            "detached": [
                {"msgids": ["stale"], "since": NOW - 400},
                {"msgids": ["mismatch"], "since": NOW - 400},
                {"msgids": ["unknown"], "since": NOW - 400},
            ],
        })
        _write_json(channel / "worker_activity.json", {
            "stale": {"ts": NOW - 400, "run_id": RUN, "source": "worker-register"},
            "mismatch": {"ts": NOW - 400, "run_id": RUN, "source": "worker-register"},
            "unknown": {"ts": NOW - 400, "run_id": RUN, "source": "worker-register"},
        })
        _write_json(channel / "worker_platform_status.json", {
            "stale": _verdict("shutdown", NOW - 601),
            "mismatch": _verdict("completed", NOW - 10, run_id=OTHER_RUN),
            "unknown": _verdict("unknown", NOW - 20),
        })
        report = self.reconcile()
        by_id = {clue["msgids"][0]: clue["kind"] for clue in report["false_negative_clues"]}
        self.assertEqual(by_id["noreg"], "silence_without_registration")
        self.assertEqual(by_id["stale"], "verdict_stale_while_silent")
        self.assertEqual(by_id["mismatch"], "verdict_run_mismatch_while_silent")
        self.assertEqual(by_id["unknown"], "verdict_unknown_while_silent")

    def test_reply_older_than_the_batch_does_not_answer_it(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["m1"], "since": NOW - 400,
        })
        _write_json(channel / "worker_activity.json", {
            "m1": {"ts": NOW - 400, "run_id": RUN, "source": "worker-register"},
        })
        _write_jsonl(channel / "outbox.jsonl", [
            {"id": "old", "mode": "reply", "msgid": "m1",
             "queued_at": NOW - 1000, "content": "上一批的回复"},
        ])
        report = self.reconcile()
        self.assertEqual(len(report["false_negative_clues"]), 1)
        self.assertEqual(
            report["false_negative_clues"][0]["kind"],
            "silence_without_verdict",
        )

    def test_bridge_activity_suppresses_leak_only_when_provided(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "active_batch.json", {
            "msgids": ["m1"], "since": NOW - 400,
        })
        _write_json(channel / "worker_activity.json", {
            "m1": {"ts": NOW - 400, "run_id": RUN, "source": "worker-register"},
        })
        bridge = self.root / "state.json"
        _write_json(bridge, {"channels": {"weixin": {"turns": [{
            "msgid": "m1",
            "activities": [{"ts": NOW - 10, "text": "网页搜索"}],
        }]}}})
        without = self.reconcile()
        self.assertEqual(len(without["false_negative_clues"]), 1)
        self.assertTrue(without["bridge_unchecked"])
        with_bridge = self.reconcile(bridge_state=bridge)
        self.assertEqual(with_bridge["false_negative_clues"], [])
        self.assertFalse(with_bridge["bridge_unchecked"])

    def test_leftover_registration_outside_the_watch_is_not_a_leak(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "worker_activity.json", {
            "old": {"ts": NOW - 5000, "run_id": RUN, "source": "worker-register"},
            "open": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
        })
        report = self.reconcile()
        self.assertEqual(
            [clue["msgids"] for clue in report["false_negative_clues"]],
            [["open"]],
        )

    def test_bot_state_is_preferred_for_clues(self) -> None:
        hooks_channel = self.hooks / "weixin-bot"
        bot_channel = self.bot / "weixin-bot" / "state"
        _write_json(hooks_channel / "worker_platform_status.json", {
            "m1": _verdict("shutdown", NOW - 20),
        })
        _write_json(hooks_channel / "active_batch.json", {
            "msgids": ["m1"], "since": NOW - 500,
        })
        _write_json(hooks_channel / "platform_check_requests.json", {
            "requests": {"m1": {"run_id": RUN, "count": 1,
                                "requested_at": NOW - 400,
                                "last_request_ts": NOW - 400,
                                "silence_secs": 100}},
            "updated": NOW - 400,
        })
        _write_jsonl(hooks_channel / "outbox.jsonl", [
            {"id": "late", "mode": "reply", "msgid": "m1",
             "queued_at": NOW - 5, "content": "若用了 hooks 里的终态，这会算误报"},
        ])
        _write_json(bot_channel / "worker_platform_status.json", {
            "m1": _verdict("running", NOW - 10, agent_updated_at=NOW - 15),
        })
        _write_json(bot_channel / "worker_activity.json", {
            "m1": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
        })
        report = self.reconcile(bot_state=self.bot)
        self.assertEqual(report["verdict_file_count"], 2)
        self.assertEqual(report["status_counts"]["shutdown"], 1)
        self.assertEqual(report["status_counts"]["running"], 1)
        self.assertEqual(report["false_positive_clues"], [])
        self.assertEqual(report["false_negative_clues"], [])
        inputs = report["channels"][0]["inputs"]
        self.assertTrue(inputs["verdicts"][0].endswith(
            "bot/weixin-bot/state/worker_platform_status.json"))
        self.assertTrue(inputs["platform_check_requests"].endswith(
            "hooks/weixin-bot/platform_check_requests.json"))
        self.assertTrue(inputs["worker_activity"].endswith(
            "bot/weixin-bot/state/worker_activity.json"))

    def test_corrupt_verdict_file_degrades_without_writing(self) -> None:
        channel = self.hooks / "weixin-bot"
        path = channel / "worker_platform_status.json"
        path.parent.mkdir(parents=True)
        path.write_text("{", encoding="utf-8")
        before = _snapshot(self.hooks)
        report = self.reconcile()
        self.assertEqual(_snapshot(self.hooks), before)
        self.assertEqual(report["verdict_file_count"], 1)
        self.assertEqual(report["verdict_record_count"], 0)
        self.assertFalse(report["verdict_files"][0]["readable"])
        self.assertTrue(any("unreadable:worker_platform_status.json" in item
                            for item in report["degraded"]))

    def test_missing_directory_exits_without_creating_it(self) -> None:
        missing = self.root / "missing-hooks"
        err = StringIO()
        with redirect_stderr(err):
            code = tool.main([str(missing), "--now", str(NOW)])
        self.assertEqual(code, 2)
        self.assertFalse(missing.exists())
        self.assertIn("不存在", err.getvalue())

    def test_cli_json_matches_library(self) -> None:
        channel = self.hooks / "weixin-bot"
        _write_json(channel / "worker_platform_status.json", {
            "m1": _verdict("shutdown", NOW - 15),
        })
        buf = StringIO()
        with redirect_stdout(buf):
            code = tool.main([
                str(self.hooks), "--json", "--now", str(NOW),
            ])
        self.assertEqual(code, 0)
        payload = json.loads(buf.getvalue())
        self.assertEqual(payload["verdict_file_count"], 1)
        self.assertEqual(payload["terminal_status_counts"]["shutdown"], 1)
        before = _snapshot(self.hooks)
        script = ROOT / "ops" / "death_watch_reconcile.py"
        proc = subprocess.run(
            [sys.executable, str(script), str(self.hooks),
             "--json", "--now", str(NOW)],
            check=False, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        again = json.loads(proc.stdout)
        self.assertEqual(again["verdict_record_count"], 1)
        self.assertEqual(_snapshot(self.hooks), before)

    def test_unreadable_gate_script_is_flagged(self) -> None:
        empty = self.root / "empty-repo"
        empty.mkdir()
        (self.hooks / "weixin-bot").mkdir(parents=True)
        _write_json(self.hooks / "weixin-bot" / "active_batch.json", {
            "msgids": ["m1"], "since": NOW - 500,
        })
        _write_json(self.hooks / "weixin-bot" / "worker_activity.json", {
            "m1": {"ts": NOW - 500, "run_id": RUN, "source": "worker-register"},
        })
        report = self.reconcile(repo_root=empty)
        self.assertTrue(report["thresholds"]["silence_gate_fallback"])
        self.assertEqual(report["thresholds"]["early_platform_min_silence_secs"], 300)
        self.assertEqual(len(report["false_negative_clues"]), 1)


if __name__ == "__main__":
    unittest.main()
