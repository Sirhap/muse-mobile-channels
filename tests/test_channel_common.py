"""Tests for the shared gateway guards."""

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "weixin-bot"))

from channel_common import (  # noqa: E402
    allocate_subagent_id,
    complete_jsonl_lines,
    media_filename,
    media_url_allowed,
    merge_queue_admin,
    muse_home,
    outbound_file_allowed,
    parse_feedback_clear_line,
    parse_jsonl_line,
    reply_block_reason_for_row,
    safe_child_name,
    subagent_outcome_default,
    trim_mapping,
    write_offset,
    read_offset,
)
import login  # noqa: E402
from login import status_url  # noqa: E402


class MuseHomeTests(unittest.TestCase):
    def test_muse_home_env_wins_over_root(self) -> None:
        old_home = os.environ.get("HOME")
        old_muse = os.environ.get("MUSE_HOME")
        try:
            os.environ["HOME"] = "/root"
            os.environ["MUSE_HOME"] = "/tmp/muse-home-test"
            self.assertEqual(muse_home(), Path("/tmp/muse-home-test"))
        finally:
            if old_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = old_home
            if old_muse is None:
                os.environ.pop("MUSE_HOME", None)
            else:
                os.environ["MUSE_HOME"] = old_muse

    def test_root_home_falls_back_to_hatch(self) -> None:
        old_home = os.environ.get("HOME")
        old_muse = os.environ.get("MUSE_HOME")
        try:
            os.environ.pop("MUSE_HOME", None)
            os.environ["HOME"] = "/root"
            if not Path("/home/hatch").is_dir():
                self.assertEqual(muse_home(), Path("/home/hatch"))
        finally:
            if old_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = old_home
            if old_muse is None:
                os.environ.pop("MUSE_HOME", None)
            else:
                os.environ["MUSE_HOME"] = old_muse


class OutboxTests(unittest.TestCase):
    def test_incomplete_tail_is_kept_back(self) -> None:
        lines = complete_jsonl_lines(b'{"a": 1}\n{"b":')
        self.assertEqual(lines, [b'{"a": 1}'])

    def test_terminated_file_has_no_phantom_line(self) -> None:
        lines = complete_jsonl_lines(b'{"a": 1}\n')
        self.assertEqual(lines, [b'{"a": 1}'])
        self.assertEqual(sum(len(line) + 1 for line in lines), len(b'{"a": 1}\n'))

    def test_complete_invalid_line_is_marked(self) -> None:
        parsed = parse_jsonl_line(b"not-json")
        self.assertEqual(parsed, {"__invalid__": True})

    def test_offset_replace_is_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "outbox.offset"
            write_offset(path, 42)
            self.assertEqual(read_offset(path), 42)
            self.assertTrue(path.read_text(encoding="utf-8").strip())

    def test_recent_failure_blocks_a_second_reply(self) -> None:
        reason = reply_block_reason_for_row(
            "m1",
            {"id": "row", "mode": "reply", "msgid": "m1", "queued_at": 9_990},
            {"id": "row", "ok": False, "ts": 9_990},
            now=10_000,
        )
        self.assertIn("still retrying", reason or "")

    def test_old_failure_without_retry_allows_another_reply(self) -> None:
        reason = reply_block_reason_for_row(
            "m1",
            {"id": "row", "mode": "reply", "msgid": "m1", "queued_at": 0},
            {"id": "row", "ok": False, "ts": 0},
            now=10_000,
        )
        self.assertIsNone(reason)

    def test_active_retry_record_keeps_blocking(self) -> None:
        reason = reply_block_reason_for_row(
            "m1",
            {"id": "row", "mode": "reply", "msgid": "m1", "queued_at": 0},
            {"id": "row", "ok": False, "ts": 0},
            now=10_000,
            retry={"row": {"n": 2, "next": 10_100}},
        )
        self.assertIn("still retrying", reason or "")

    def test_dead_letter_allows_another_reply(self) -> None:
        reason = reply_block_reason_for_row(
            "m1",
            {"id": "row", "mode": "reply", "msgid": "m1", "queued_at": 0},
            {"id": "row", "ok": False, "deadletter": True},
            now=10_000,
        )
        self.assertIsNone(reason)

    def test_delivered_reply_blocks(self) -> None:
        reason = reply_block_reason_for_row(
            "m1",
            {"id": "row", "mode": "reply", "msgid": "m1"},
            {"id": "row", "ok": True},
            now=10,
        )
        self.assertIn("delivered successfully", reason or "")


class QueueAdminTests(unittest.TestCase):
    def test_second_drop_keeps_both_msgids(self) -> None:
        merged = merge_queue_admin(
            {"ts": 100, "action": "drop", "msgids": ["a"]},
            "drop",
            ["b"],
            now=105,
        )
        self.assertEqual(merged["action"], "drop")
        self.assertEqual(merged["msgids"], ["a", "b"])

    def test_clear_absorbs_an_unconsumed_drop(self) -> None:
        merged = merge_queue_admin(
            {"ts": 100, "action": "drop", "msgids": ["a"]},
            "clear",
            ["b"],
            now=105,
        )
        self.assertEqual(merged["action"], "clear")
        self.assertEqual(merged["msgids"], ["a", "b"])

    def test_stale_file_is_not_merged(self) -> None:
        merged = merge_queue_admin(
            {"ts": 1, "action": "clear", "msgids": ["old"]},
            "drop",
            ["new"],
            now=100,
        )
        self.assertEqual(merged["msgids"], ["new"])
        self.assertEqual(merged["action"], "drop")


class SafetyTests(unittest.TestCase):
    def test_msgid_cannot_escape_directory(self) -> None:
        self.assertIsNone(safe_child_name("../.ssh/authorized_keys"))
        self.assertIsNone(safe_child_name("a/b"))
        self.assertEqual(safe_child_name("msg-1"), "msg-1")

    def test_media_filename_has_no_slash(self) -> None:
        name = media_filename("../../etc/passwd", 0, "png")
        self.assertNotIn("/", name)
        self.assertTrue(name.endswith(".png"))

    def test_media_url_allowlist(self) -> None:
        self.assertTrue(media_url_allowed("https://novac2c.cdn.weixin.qq.com/c2c/x"))
        self.assertFalse(media_url_allowed("https://evil.example/secret"))
        self.assertFalse(media_url_allowed("not a url"))

    def test_credentials_file_is_not_sendable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cred_dir = root / ".config" / "weixin-bot"
            cred_dir.mkdir(parents=True)
            cred = cred_dir / "credentials.env"
            cred.write_text("TOKEN=secret\n", encoding="utf-8")
            note = root / "workspace" / "note.txt"
            note.parent.mkdir()
            note.write_text("hello\n", encoding="utf-8")
            self.assertFalse(outbound_file_allowed(cred, cred, [root]))
            other = root / ".config" / "wecom-bot" / "credentials.env"
            other.parent.mkdir(parents=True)
            other.write_text("SECRET=1\n", encoding="utf-8")
            self.assertFalse(outbound_file_allowed(other, cred, [root]))
            self.assertTrue(outbound_file_allowed(note, cred, [root]))

    def test_feedback_clear_accepts_json_or_bare_msgid(self) -> None:
        self.assertEqual(parse_feedback_clear_line("abc"), "abc")
        self.assertEqual(parse_feedback_clear_line('{"msgid": "m9"}'), "m9")
        self.assertEqual(parse_feedback_clear_line('{"other": 1}'), "")

    def test_failed_job_is_not_labeled_success(self) -> None:
        self.assertEqual(subagent_outcome_default("任务失败了", None), "失败")
        self.assertEqual(subagent_outcome_default("done", "failed"), "失败")
        self.assertEqual(subagent_outcome_default("回答正文", "running"), "完成")

    def test_sequence_failure_does_not_reuse_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            first = allocate_subagent_id(state)
            second = allocate_subagent_id(state)
            self.assertEqual(first, "S1")
            self.assertEqual(second, "S2")
            self.assertEqual(json.loads((state / "subagent_seq.json").read_text()), {"next": 3})

    def test_trim_keeps_pinned_routes(self) -> None:
        items = [(str(i), i) for i in range(5)]
        kept = trim_mapping(items, 2, {"0"})
        self.assertIn("0", kept)
        self.assertIn("4", kept)
        self.assertNotIn("1", kept)

    def test_pending_qrcode_keeps_the_session_that_asked_for_a_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "login.json"
            path.write_text(json.dumps({
                "status": "need_verify_code",
                "qrcode": "qr-session",
                "ts": time.time(),
            }), encoding="utf-8")
            previous = login.LOGIN_STATE
            login.LOGIN_STATE = path
            try:
                self.assertEqual(login.pending_qrcode(), "qr-session")
            finally:
                login.LOGIN_STATE = previous

    def test_verify_code_is_on_the_status_url(self) -> None:
        url = status_url("qr-session", "2468")
        self.assertIn("qrcode=qr-session", url)
        self.assertIn("verify_code=2468", url)
        self.assertNotIn("verify_code", status_url("qr-session", ""))


if __name__ == "__main__":
    unittest.main()
