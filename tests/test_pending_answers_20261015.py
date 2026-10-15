#!/usr/bin/env python3
"""Unified pending-answer ledger (todo 23).

The sweep used to read only covered_drops.jsonl. covered-drop,
fail-stop, and bridge fallback now share pending_answers.json, and
ops/pending_answers_report.py is the read-only view.

Library cases talk to channel_common directly. Hook cases run the
repo inbox scripts in a sandbox (same shape as
test_hook_replyfile_completion.py). Bridge cases load native_bridge
with BASE pointed at the sandbox so no hatch state is touched.

Run: python3 tests/test_pending_answers_20261015.py
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import channel_common as cc

NOW = 1_800_000_000.0
SCRIPTS = ROOT / "hooks" / "scripts"
REPORT = ROOT / "ops" / "pending_answers_report.py"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _row(channel: str, msgid: str, source: str, ts: float, summary: str) -> dict:
    return {
        "channel": channel,
        "msgid": msgid,
        "source": source,
        "ts": ts,
        "summary": summary,
    }


class LedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.ledger = self.dir / "pending_answers.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_three_sources_share_one_file_and_keep_the_first_time(self) -> None:
        """A later path updates source and leaves the original timestamp."""
        first = cc.record_pending_answers(self.ledger, [
            _row("weixin", "m1", "bridge-fallback", NOW, "桥上的问题"),
            _row("wecom", "m2", "covered-drop", NOW + 5, "企微被盖住的问题"),
        ])
        self.assertEqual(first, 2)
        second = cc.record_pending_answers(self.ledger, [
            _row("weixin", "m1", "bridge-fallback", NOW + 50, "桥上的问题"),
            _row("weixin", "m1", "fail-stop", NOW + 80, "桥上的问题，后来判死"),
        ])
        self.assertEqual(second, 1)
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        self.assertEqual(set(data), {"weixin", "wecom"})
        self.assertEqual(data["weixin"]["m1"]["source"], "fail-stop")
        self.assertEqual(data["weixin"]["m1"]["ts"], NOW)
        self.assertEqual(data["weixin"]["m1"]["summary"], "桥上的问题，后来判死")
        self.assertEqual(data["wecom"]["m2"]["source"], "covered-drop")
        leftovers = list(self.dir.glob("pending_answers.json.tmp.*"))
        self.assertEqual(leftovers, [])

    def test_blank_msgid_and_unknown_source(self) -> None:
        """A blank msgid is skipped. An unknown source writes nothing."""
        self.assertEqual(cc.record_pending_answers(self.ledger, [
            _row("weixin", "  ", "fail-stop", NOW, "空 id"),
        ]), 0)
        self.assertFalse(self.ledger.exists())
        with self.assertRaises(ValueError):
            cc.record_pending_answers(self.ledger, [
                _row("weixin", "m1", "covered_drops", NOW, "错名"),
            ])
        self.assertFalse(self.ledger.exists())

    def test_summary_collapses_whitespace_and_caps_length(self) -> None:
        text = "第一行\n第二行 " + ("问" * 90)
        cc.record_pending_answers(self.ledger, [
            _row("weixin", "m1", "fail-stop", NOW, text),
            _row("wecom", "m2", "covered-drop", NOW, "   \n"),
        ])
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        summary = data["weixin"]["m1"]["summary"]
        self.assertNotIn("\n", summary)
        self.assertTrue(summary.startswith("第一行 第二行"))
        self.assertEqual(len(summary), cc.PENDING_SUMMARY_MAX)
        self.assertEqual(data["wecom"]["m2"]["summary"], cc.PENDING_SUMMARY_EMPTY)

    def test_corrupt_ledger_is_not_replaced(self) -> None:
        self.ledger.write_text("{", encoding="utf-8")
        with self.assertRaises(cc.PendingLedgerError):
            cc.record_pending_answers(self.ledger, [
                _row("weixin", "m1", "fail-stop", NOW, "不能覆盖坏账"),
            ])
        self.assertEqual(self.ledger.read_text(encoding="utf-8"), "{")
        with self.assertRaises(cc.PendingLedgerError):
            cc.summarize_pending_answers(self.ledger, state_dirs={}, now=NOW)

    def test_concurrent_writers_keep_both_msgids(self) -> None:
        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def write(msgid: str) -> None:
            try:
                barrier.wait(timeout=5)
                cc.record_pending_answers(self.ledger, [
                    _row("weixin", msgid, "fail-stop", NOW, msgid),
                ])
            except BaseException as exc:  # noqa: BLE001 — surface any thread failure
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(mid,)) for mid in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(errors, [])
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        self.assertEqual(set(data["weixin"]), {"a", "b"})


class ReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.ledger = self.dir / "pending_answers.json"
        self.wx = self.dir / "weixin-state"
        self.wc = self.dir / "wecom-state"
        self.wx_hook = self.dir / "weixin-hook"
        self.wc_hook = self.dir / "wecom-hook"
        for path in (self.wx, self.wc, self.wx_hook, self.wc_hook):
            path.mkdir()
        cc.record_pending_answers(self.ledger, [
            _row("weixin", "old-drop", "covered-drop", NOW - 90_000, "微信旧漏答"),
            _row("weixin", "fresh", "fail-stop", NOW - 10, "微信刚判死"),
            _row("weixin", "answered", "fail-stop", NOW - 90_000, "微信已答"),
            _row("wecom", "fallback", "bridge-fallback", NOW - 90_000, "企微回退未答"),
            _row("wecom", "dismissed", "covered-drop", NOW - 90_000, "企微已销账"),
        ])

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _report(self, **kwargs) -> dict:
        params = {
            "state_dirs": {"weixin": self.wx, "wecom": self.wc},
            "hook_dirs": {"weixin": self.wx_hook, "wecom": self.wc_hook},
            "now": NOW,
            "min_age_secs": 0,
        }
        params.update(kwargs)
        return cc.summarize_pending_answers(self.ledger, **params)

    def test_groups_unanswered_rows_and_drops_a_delivered_formal_reply(self) -> None:
        _write_jsonl(self.wx / "outbox.jsonl", [
            {"id": "r-ok", "mode": "reply", "msgid": "answered"},
            {"id": "r-file", "mode": "reply_file", "msgid": "other"},
            {"id": "notice", "mode": "send", "msgid": "old-drop"},
        ])
        _write_jsonl(self.wx / "outbox_results.jsonl", [
            {"id": "r-ok", "mode": "reply", "ok": True, "ts": NOW - 10},
            {"id": "r-file", "mode": "reply_file", "ok": False, "ts": NOW - 10},
            {"id": "notice", "mode": "send", "ok": True, "ts": NOW - 10},
            {"id": "late", "mode": "reply", "ok": False, "msgid": "fresh", "ts": NOW},
        ])
        _write_jsonl(self.wc / "outbox.jsonl", [])
        _write_jsonl(self.wc / "outbox_results.jsonl", [
            {"id": "bubble", "mode": "reply_notice", "ok": True, "msgid": "fallback"},
        ])
        report = self._report()
        self.assertEqual(
            [item["msgid"] for item in report["weixin"]],
            ["old-drop", "fresh"],
        )
        self.assertEqual(report["weixin"][0]["source"], "covered-drop")
        self.assertEqual(report["weixin"][0]["summary"], "微信旧漏答")
        self.assertEqual(report["weixin"][0]["ts"], NOW - 90_000)
        wecom = {item["msgid"]: item for item in report["wecom"]}
        self.assertEqual(wecom["fallback"]["source"], "bridge-fallback")
        self.assertEqual(wecom["fallback"]["summary"], "企微回退未答")
        self.assertIn("dismissed", wecom)
        self.assertNotIn("answered", [item["msgid"] for item in report["weixin"]])

    def test_reply_file_delivery_and_the_twelve_hour_gate(self) -> None:
        _write_jsonl(self.wx / "outbox.jsonl", [
            {"id": "file-1", "mode": "reply_file", "msgid": "old-drop"},
        ])
        _write_jsonl(self.wx / "outbox_results.jsonl", [
            {"id": "file-1", "mode": "reply_file", "ok": True},
        ])
        report = self._report(min_age_secs=cc.PENDING_ANSWER_EXPOSE_SECS)
        self.assertEqual([item["msgid"] for item in report["weixin"]], ["answered"])
        self.assertNotIn("fresh", [item["msgid"] for item in report.get("weixin", [])])
        self.assertEqual(
            [item["msgid"] for item in report["wecom"]],
            ["dismissed", "fallback"],
        )

    def test_dismiss_file_and_legacy_covered_drops(self) -> None:
        (self.dir / "pending_answers_dismissed.json").write_text(
            json.dumps({"wecom": ["dismissed"], "weixin": []}),
            encoding="utf-8",
        )
        _write_jsonl(self.wx_hook / "covered_drops.jsonl", [{
            "msgids": ["legacy-1", "old-drop"],
            "since": NOW - 100_000,
            "last_activity": NOW - 99_000,
            "covered_by": "later",
            "ts": NOW - 80_000,
        }])
        _write_jsonl(self.wx / "inbox.jsonl", [
            {"msgid": "legacy-1", "text": "历史问题全文", "ts": NOW - 100_000},
        ])
        report = self._report(
            min_age_secs=cc.PENDING_ANSWER_EXPOSE_SECS,
            dismissed_path=self.dir / "pending_answers_dismissed.json",
        )
        weixin = {item["msgid"]: item for item in report["weixin"]}
        self.assertIn("legacy-1", weixin)
        self.assertEqual(weixin["legacy-1"]["source"], "covered-drop")
        self.assertEqual(weixin["legacy-1"]["summary"], "历史问题全文")
        self.assertEqual(weixin["legacy-1"]["ts"], NOW - 80_000)
        self.assertEqual(weixin["old-drop"]["summary"], "微信旧漏答")
        self.assertNotIn("fresh", weixin)
        self.assertNotIn("dismissed", [item["msgid"] for item in report["wecom"]])

    def test_corrupt_dismiss_file_does_not_hide_rows(self) -> None:
        dismissed = self.dir / "pending_answers_dismissed.json"
        dismissed.write_text("{", encoding="utf-8")
        report = self._report(dismissed_path=dismissed, min_age_secs=0)
        ids = [item["msgid"] for item in report["wecom"]]
        self.assertIn("dismissed", ids)

    def test_report_command_is_read_only(self) -> None:
        before = self.ledger.read_bytes()
        stamp = self.ledger.stat().st_mtime_ns
        proc = subprocess.run(
            [
                sys.executable,
                str(REPORT),
                "--ledger", str(self.ledger),
                "--state", f"weixin={self.wx}",
                "--state", f"wecom={self.wc}",
                "--hook", f"weixin={self.wx_hook}",
                "--min-age", "0",
                "--now", str(NOW),
                "--dismissed", str(self.dir / "missing-dismissed.json"),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(proc.stdout)
        self.assertEqual(
            [item["msgid"] for item in report["weixin"]],
            ["answered", "old-drop", "fresh"],
        )
        self.assertEqual(self.ledger.read_bytes(), before)
        self.assertEqual(self.ledger.stat().st_mtime_ns, stamp)
        self.assertFalse((self.dir / "missing-dismissed.json").exists())

    def test_default_age_hides_a_fresh_row_and_corrupt_ledger_exits_2(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                str(REPORT),
                "--ledger", str(self.ledger),
                "--state", f"weixin={self.wx}",
                "--state", f"wecom={self.wc}",
                "--now", str(NOW),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(proc.stdout)
        self.assertNotIn("fresh", [item["msgid"] for item in report["weixin"]])
        self.assertEqual(proc.stderr, "")
        self.ledger.write_text("[]", encoding="utf-8")
        bad = subprocess.run(
            [sys.executable, str(REPORT), "--ledger", str(self.ledger),
             "--state", f"weixin={self.wx}", "--now", str(NOW)],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn("unreadable", bad.stderr)


def _hook_entry(channel: str, msgid: str, ts: float, text: str) -> dict:
    if channel == "weixin":
        return {
            "msgid": msgid,
            "from_user_id": "user-1",
            "text": text,
            "ts": ts,
            "media": [],
        }
    return {
        "msgid": msgid,
        "from_userid": "testuser",
        "chattype": "single",
        "chatid": "testuser",
        "msgtype": "text",
        "text": text,
        "ts": ts,
        "media": [],
    }


class HookLedgerTests(unittest.TestCase):
    """The inbox scripts write the ledger without changing covered_drops."""

    def _run(self, channel: str, name: str, setup) -> dict:
        sbx = Path(tempfile.mkdtemp(prefix=f"pending-{channel}-{name}-"))
        self.addCleanup(shutil.rmtree, sbx, True)
        bot = "weixin-bot" if channel == "weixin" else "wecom-bot"
        cli = "/home/hatch/workspace/weixin-bot/weixin" if channel == "weixin" \
            else "/home/hatch/workspace/wecom-bot/wecom"
        script = "weixin-inbox.sh" if channel == "weixin" else "wecom-inbox.sh"
        bot_state = sbx / bot / "state"
        hook_state = sbx / "home" / "hooks" / "state" / bot
        bot_state.mkdir(parents=True)
        hook_state.mkdir(parents=True)
        (sbx / "cli.log").write_text("", encoding="utf-8")
        src = (SCRIPTS / script).read_text(encoding="utf-8")
        src = src.replace(f"/home/hatch/workspace/{bot}/state", str(bot_state))
        src = src.replace(cli, str(sbx / "stubcli"))
        (sbx / "hook.sh").write_text(src, encoding="utf-8")
        (sbx / "runtime.sh").write_text(
            'silent() { echo "DECISION silent: $1"; }\n'
            'wake() { echo "DECISION wake: $1"; }\n'
            'log() { :; }\n',
            encoding="utf-8",
        )
        (sbx / "stubcli").write_text(
            "#!/usr/bin/env bash\n"
            f'echo "CALL: $*" >> {sbx / "cli.log"}\n'
            'if [[ "$1" == "cancelled" ]]; then echo "[]"; fi\n'
            "exit 0\n",
            encoding="utf-8",
        )
        os.chmod(sbx / "stubcli", 0o755)
        (bot_state / "cancelled.json").write_text("[]", encoding="utf-8")
        setup(bot_state, hook_state)
        env = dict(os.environ)
        env["HOME"] = str(sbx / "home")
        env["HATCH_HOOK_RUNTIME"] = str(sbx / "runtime.sh")
        env["HATCH_HOOK_DRY_RUN"] = "0"
        env["PENDING_ANSWERS_PY"] = str(ROOT / "channel_common.py")
        env["DEATH_WATCH_ACTIVITY_PY"] = str(sbx / "missing-probe.py")
        env["DEATH_WATCH_BRIDGE_STATE"] = str(sbx / "missing-bridge.json")
        env.pop("PENDING_ANSWERS_PATH", None)
        proc = subprocess.run(
            ["bash", str(sbx / "hook.sh")],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            proc.returncode, 0,
            (proc.stderr or proc.stdout)[-1500:],
        )
        ledger = sbx / "home" / "hooks" / "state" / "pending_answers.json"
        covered = hook_state / "covered_drops.jsonl"
        batch_path = hook_state / "active_batch.json"
        batch = json.loads(batch_path.read_text(encoding="utf-8")) if batch_path.exists() else {}
        return {
            "log": (sbx / "cli.log").read_text(encoding="utf-8"),
            "ledger": ledger,
            "covered": covered,
            "batch": batch,
            "stdout": proc.stdout,
        }

    def test_covered_drop_keeps_the_audit_log_and_records_the_ledger(self) -> None:
        moment = time.time()
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel):
                def plant(bot_state: Path, hook_state: Path, moment=moment) -> None:
                    original = "covered-orig"
                    follow = "covered-follow"
                    _write_jsonl(bot_state / "inbox.jsonl", [
                        _hook_entry(channel, original, moment - 1900, "为什么查不到？"),
                        _hook_entry(channel, follow, moment - 40, "那你再看看"),
                    ])
                    _write_jsonl(bot_state / "outbox.jsonl", [{
                        "id": "cov-1",
                        "mode": "reply",
                        "msgid": follow,
                        "queued_at": moment - 30,
                    }])
                    _write_jsonl(bot_state / "outbox_results.jsonl", [{
                        "id": "cov-1",
                        "mode": "reply",
                        "ok": True,
                        "ts": moment - 20,
                    }])
                    (hook_state / "seen_msgids.txt").write_text(
                        f"{original}\n{follow}\n", encoding="utf-8")
                    (hook_state / "carried_msgids.txt").write_text(
                        f"{original}\n{follow}\n", encoding="utf-8")
                    (hook_state / "active_batch.json").write_text(json.dumps({
                        "msgids": [original],
                        "since": moment - 1900,
                        "detached": [],
                    }), encoding="utf-8")

                result = self._run(channel, "covered", plant)
                self.assertNotIn("CALL: cancel", result["log"])
                self.assertFalse(result["batch"].get("msgids"))
                covered_rows = [
                    json.loads(line)
                    for line in result["covered"].read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
                self.assertEqual(len(covered_rows), 1)
                self.assertEqual(set(covered_rows[0]), {
                    "msgids", "since", "last_activity", "covered_by", "ts",
                })
                self.assertEqual(covered_rows[0]["msgids"], ["covered-orig"])
                self.assertEqual(covered_rows[0]["covered_by"], "covered-follow")
                ledger = json.loads(result["ledger"].read_text(encoding="utf-8"))
                record = ledger[channel]["covered-orig"]
                self.assertEqual(record["source"], "covered-drop")
                self.assertEqual(record["summary"], "为什么查不到？")
                self.assertNotIn("covered-follow", ledger[channel])

    def test_second_loss_records_fail_stop_and_first_loss_does_not(self) -> None:
        moment = time.time()
        for channel in ("weixin", "wecom"):
            with self.subTest(channel=channel, phase="fail-stop"):
                def plant_stop(bot_state: Path, hook_state: Path, moment=moment) -> None:
                    _write_jsonl(bot_state / "inbox.jsonl", [
                        _hook_entry(channel, "dead-1", moment - 1900, "判死后还没人答"),
                    ])
                    (hook_state / "seen_msgids.txt").write_text("dead-1\n", encoding="utf-8")
                    (hook_state / "carried_msgids.txt").write_text("dead-1\n", encoding="utf-8")
                    (hook_state / "active_batch.json").write_text(json.dumps({
                        "msgids": ["dead-1"],
                        "since": moment - 1900,
                        "detached": [],
                    }), encoding="utf-8")
                    (hook_state / "resume_attempts.json").write_text(
                        json.dumps({"dead-1": moment - 60}), encoding="utf-8")

                result = self._run(channel, "fail", plant_stop)
                self.assertIn("CALL: cancel --msgid dead-1", result["log"])
                self.assertFalse(result["covered"].exists())
                self.assertFalse(result["batch"].get("msgids"))
                ledger = json.loads(result["ledger"].read_text(encoding="utf-8"))
                record = ledger[channel]["dead-1"]
                self.assertEqual(record["source"], "fail-stop")
                self.assertEqual(record["summary"], "判死后还没人答")

            with self.subTest(channel=channel, phase="first-loss"):
                def plant_first(bot_state: Path, hook_state: Path, moment=moment) -> None:
                    _write_jsonl(bot_state / "inbox.jsonl", [
                        _hook_entry(channel, "live-1", moment - 1900, "第一次丢失会续跑"),
                    ])
                    (hook_state / "seen_msgids.txt").write_text("live-1\n", encoding="utf-8")
                    (hook_state / "carried_msgids.txt").write_text("live-1\n", encoding="utf-8")
                    (hook_state / "active_batch.json").write_text(json.dumps({
                        "msgids": ["live-1"],
                        "since": moment - 1900,
                        "detached": [],
                    }), encoding="utf-8")

                result = self._run(channel, "first", plant_first)
                self.assertNotIn("CALL: cancel", result["log"])
                self.assertIn("已自动续跑", result["log"])
                self.assertFalse(result["ledger"].exists())
                self.assertFalse(result["covered"].exists())

    def test_ledger_failure_still_writes_covered_drops(self) -> None:
        moment = time.time()
        with tempfile.TemporaryDirectory() as tmp:
            broken = Path(tmp) / "broken_channel_common.py"
            broken.write_text(
                "def record_pending_answers(path, rows):\n"
                "    raise RuntimeError('ledger down')\n",
                encoding="utf-8",
            )
            sbx = Path(tmp) / "sbx"
            # Reuse the runner, but point the module at the broken file
            # by patching the environment inside a local copy of the call.
            channel = "weixin"
            bot = "weixin-bot"
            bot_state = sbx / bot / "state"
            hook_state = sbx / "home" / "hooks" / "state" / bot
            bot_state.mkdir(parents=True)
            hook_state.mkdir(parents=True)
            (sbx / "cli.log").write_text("", encoding="utf-8")
            src = (SCRIPTS / "weixin-inbox.sh").read_text(encoding="utf-8")
            src = src.replace("/home/hatch/workspace/weixin-bot/state", str(bot_state))
            src = src.replace("/home/hatch/workspace/weixin-bot/weixin", str(sbx / "stubcli"))
            (sbx / "hook.sh").write_text(src, encoding="utf-8")
            (sbx / "runtime.sh").write_text(
                'silent() { echo "DECISION silent: $1"; }\n'
                'wake() { echo "DECISION wake: $1"; }\n'
                'log() { :; }\n',
                encoding="utf-8",
            )
            (sbx / "stubcli").write_text(
                "#!/usr/bin/env bash\n"
                f'echo "CALL: $*" >> {sbx / "cli.log"}\n'
                "exit 0\n",
                encoding="utf-8",
            )
            os.chmod(sbx / "stubcli", 0o755)
            (bot_state / "cancelled.json").write_text("[]", encoding="utf-8")
            _write_jsonl(bot_state / "inbox.jsonl", [
                _hook_entry(channel, "covered-orig", moment - 1900, "为什么查不到？"),
                _hook_entry(channel, "covered-follow", moment - 40, "后续"),
            ])
            _write_jsonl(bot_state / "outbox.jsonl", [{
                "id": "cov-1", "mode": "reply", "msgid": "covered-follow",
                "queued_at": moment - 30,
            }])
            _write_jsonl(bot_state / "outbox_results.jsonl", [{
                "id": "cov-1", "mode": "reply", "ok": True, "ts": moment - 20,
            }])
            (hook_state / "seen_msgids.txt").write_text(
                "covered-orig\ncovered-follow\n", encoding="utf-8")
            (hook_state / "carried_msgids.txt").write_text(
                "covered-orig\ncovered-follow\n", encoding="utf-8")
            (hook_state / "active_batch.json").write_text(json.dumps({
                "msgids": ["covered-orig"], "since": moment - 1900, "detached": [],
            }), encoding="utf-8")
            env = dict(os.environ)
            env["HOME"] = str(sbx / "home")
            env["HATCH_HOOK_RUNTIME"] = str(sbx / "runtime.sh")
            env["HATCH_HOOK_DRY_RUN"] = "0"
            env["PENDING_ANSWERS_PY"] = str(broken)
            env["DEATH_WATCH_ACTIVITY_PY"] = str(sbx / "missing-probe.py")
            env["DEATH_WATCH_BRIDGE_STATE"] = str(sbx / "missing-bridge.json")
            proc = subprocess.run(
                ["bash", str(sbx / "hook.sh")],
                env=env, capture_output=True, text=True, timeout=60, check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr[-1500:])
            covered = (hook_state / "covered_drops.jsonl").read_text(encoding="utf-8")
            self.assertIn("covered-orig", covered)
            self.assertIn("covered-follow", covered)
            self.assertFalse(
                (sbx / "home" / "hooks" / "state" / "pending_answers.json").exists()
            )


def _load_bridge(base: Path):
    """Exec native_bridge with BASE and imports pointed at the sandbox."""
    stub = base / "pydeps"
    package = stub / "muse_cli"
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "cli.py").write_text(
        "def fmt_event(ev):\n    return {}\n"
        "def is_reply(ev):\n    return False\n",
        encoding="utf-8",
    )
    (package / "gateway.py").write_text(
        "class GatewayError(Exception):\n    pass\n",
        encoding="utf-8",
    )
    (stub / "gw2.py").write_text("class GW2:\n    pass\n", encoding="utf-8")
    os.environ["PENDING_BRIDGE_STUB"] = str(stub)
    source = (ROOT / "native-bridge" / "native_bridge.py").read_text(encoding="utf-8")
    source = source.replace(
        'BASE = "/home/hatch/workspace/native-bridge"',
        f"BASE = {str(base)!r}",
        1,
    )
    source = source.replace(
        'sys.path.insert(0, "/home/hatch/workspace/native-probe")',
        "sys.path.insert(0, os.environ['PENDING_BRIDGE_STUB'])",
        1,
    )
    module = types.ModuleType("native_bridge_pending")
    module.__dict__["__file__"] = str(ROOT / "native-bridge" / "native_bridge.py")
    exec(compile(source, module.__dict__["__file__"], "exec"), module.__dict__)
    return module


class BridgeFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name) / "bridge"
        self.base.mkdir()
        (self.base / "shadow").mkdir()
        shutil.copy(ROOT / "native-bridge" / "config.json", self.base / "config.json")
        self.ledger = Path(self.tmp.name) / "pending_answers.json"
        self._old_path = os.environ.get("PENDING_ANSWERS_PATH")
        os.environ["PENDING_ANSWERS_PATH"] = str(self.ledger)
        self.nb = _load_bridge(self.base)
        self.assertIsNotNone(self.nb._CHANNEL_COMMON)

    def tearDown(self) -> None:
        if self._old_path is None:
            os.environ.pop("PENDING_ANSWERS_PATH", None)
        else:
            os.environ["PENDING_ANSWERS_PATH"] = self._old_path
        self.tmp.cleanup()

    def test_weixin_and_wecom_fallbacks_share_the_ledger(self) -> None:
        wx = self.base / "wx-state"
        wc = self.base / "wc-state"
        wx.mkdir()
        wc.mkdir()
        (self.base / "enabled-weixin").write_text("", encoding="utf-8")
        (self.base / "enabled-wecom").write_text("", encoding="utf-8")
        self.nb.CFG["channels"]["weixin"]["bot_state"] = str(wx)
        self.nb.CFG["channels"]["wecom"]["bot_state"] = str(wc)
        self.nb.fallback_to_inbox(
            "weixin", {"msgid": "wx-fb", "text": "微信桥回退的问题"}, "connect refused")
        self.nb.fallback_to_inbox(
            "weixin", {"msgid": "wx-fb", "text": "微信桥回退的问题"}, "connect refused")
        self.nb.fallback_to_inbox(
            "wecom", {"msgid": "wc-fb", "text": "企微桥回退的问题"}, "no session")
        inbox = [
            json.loads(line)
            for line in (wx / "inbox.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(inbox), 2)
        self.assertEqual(inbox[0]["via"], "native-bridge-fallback")
        self.assertEqual(inbox[0]["fallback_why"], "connect refused")
        fallback_log = (wx / "bridge_fallback.jsonl").read_text(encoding="utf-8")
        self.assertIn("wx-fb", fallback_log)
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        self.assertEqual(set(data["weixin"]), {"wx-fb"})
        self.assertEqual(data["weixin"]["wx-fb"]["source"], "bridge-fallback")
        self.assertEqual(data["weixin"]["wx-fb"]["summary"], "微信桥回退的问题")
        self.assertEqual(data["weixin"]["wx-fb"]["ts"], inbox[0]["ts"])
        self.assertEqual(data["wecom"]["wc-fb"]["source"], "bridge-fallback")
        self.assertEqual(data["wecom"]["wc-fb"]["summary"], "企微桥回退的问题")
        self.assertTrue((wc / "bridge_fallback.jsonl").exists())

    def test_shadow_channel_records_and_a_ledger_error_still_hands_off(self) -> None:
        self.nb.fallback_to_inbox(
            "test", {"msgid": "t-1", "text": "影子渠道的问题"}, "auth")
        shadow = (self.base / "shadow" / "test-inbox.jsonl").read_text(encoding="utf-8")
        self.assertIn("native-bridge-fallback", shadow)
        self.assertIn("t-1", (self.base / "shadow" / "bridge_fallback.jsonl").read_text())
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        self.assertEqual(data["test"]["t-1"]["source"], "bridge-fallback")

        def explode(path, rows):
            raise RuntimeError("ledger down")

        self.nb._CHANNEL_COMMON.record_pending_answers = explode
        self.nb.fallback_to_inbox(
            "test", {"msgid": "t-2", "text": "第二条仍要回退"}, "auth")
        shadow_rows = (self.base / "shadow" / "test-inbox.jsonl").read_text(encoding="utf-8")
        self.assertIn("t-2", shadow_rows)
        data = json.loads(self.ledger.read_text(encoding="utf-8"))
        self.assertNotIn("t-2", data.get("test", {}))


if __name__ == "__main__":
    unittest.main()
