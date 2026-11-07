"""复现：企微 core 模式 /a /b /c 决定全断 + 卡片点击被丢弃。

只用临时目录与临时 core 库，绝不碰生产库。
跑红 = 问题存在；修复后应全绿。
"""
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mobile_channels.gateway import GatewayBinding
from mobile_channels.storage import Store


def load_wecom_gateway():
    spec = importlib.util.spec_from_file_location(
        "wecom_core_fix_test", ROOT / "wecom-bot/gateway.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_pending_state():
    """三个待审批，均为 wecom 单聊 alice 的显式 scope。"""
    return {"next_num": 4, "items": {
        str(n): {"approval_id": f"aid-{n}", "status": "pending",
                 "expires_ms": 9999999999999,
                 "decision_scope": {"channel": "wecom", "chat_id": "alice",
                                    "operator_ids": ["alice"]}}
        for n in (1, 2, 3)}}


class CoreSlashDecisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "core.sqlite3"
        with Store(self.database):
            pass
        modes = self.root / "modes.json"
        modes.write_text(json.dumps({"channels": {
            "wecom": {"mode": "core", "database": str(self.database),
                      "account_id": "wecom-account",
                      "allowed_senders": ["alice", "admin"],
                      "administrators": ["admin"]}}}))
        env = mock.patch.dict(os.environ, {"MUSE_CHANNEL_MODES": str(modes)})
        env.start()
        self.addCleanup(env.stop)
        self.relay_dir = self.root / "approval-relay"
        self.relay_dir.mkdir()
        (self.relay_dir / "state.json").write_text(
            json.dumps(make_pending_state(), ensure_ascii=False))
        (self.relay_dir / "decisions.jsonl").write_text("")
        import mobile_channels.gateway as core_gateway
        patcher = mock.patch.object(core_gateway, "APPROVAL_RELAY_DIR",
                                    self.relay_dir, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.binding = GatewayBinding("wecom")
        self.addCleanup(self.binding.close)

    def decisions(self):
        path = self.relay_dir / "decisions.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in
                path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def reply_text(self):
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute(
                "SELECT text FROM outbound_messages ORDER BY rowid")]
        return rows[-1]["text"] if rows else ""

    def test_a_writes_allow_once_row(self):
        self.binding.receive(message_id="m-a1", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/a 1")
        rows = self.decisions()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["num"], "1")
        self.assertEqual(rows[0]["decision"], "allow_once")
        self.assertEqual(rows[0]["channel"], "wecom")
        self.assertEqual(rows[0]["operator_id"], "alice")
        self.assertEqual(rows[0]["chat_id"], "alice")
        self.assertIn("已提交", self.reply_text())

    def test_typed_decision_waits_for_relay_before_taking_sqlite_writer_lock(self):
        from mobile_channels import gateway
        original_lock = gateway._decisions_lock

        @contextmanager
        def relay_finishes_before_gateway_takes_decision_lock():
            # The relay may be writing an approval notice while holding
            # decisions.lock. Its SQLite writer must still be able to enter.
            with sqlite3.connect(self.database, timeout=0) as relay_database:
                relay_database.execute("BEGIN IMMEDIATE")
            with original_lock():
                yield

        with mock.patch.object(gateway, "_decisions_lock", relay_finishes_before_gateway_takes_decision_lock):
            self.binding.receive(message_id="m-ordered", sender_id="alice",
                                 chat_id="alice", chat_type="single", text="/a 1")

        self.assertEqual([row["decision"] for row in self.decisions()], ["allow_once"])

    def test_b_writes_allow_always_row(self):
        self.binding.receive(message_id="m-b2", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/b 2")
        rows = self.decisions()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["decision"], "allow_always")

    def test_c_writes_deny_row(self):
        self.binding.receive(message_id="m-c3", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/c 3")
        rows = self.decisions()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["decision"], "deny")

    def test_missing_number_writes_nothing(self):
        self.binding.receive(message_id="m-a0", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/a")
        self.assertEqual(self.decisions(), [])
        self.assertIn("/a N", self.reply_text())

    def test_unknown_number_writes_nothing(self):
        self.binding.receive(message_id="m-a9", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/a 9")
        self.assertEqual(self.decisions(), [])
        self.assertIn("没有找到审批 #9", self.reply_text())

    def test_duplicate_decision_writes_only_one_row(self):
        self.binding.receive(message_id="m-d1", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/a 1")
        self.binding.receive(message_id="m-d2", sender_id="alice",
                             chat_id="alice", chat_type="single", text="/a 1")
        self.assertEqual(len(self.decisions()), 1)
        self.assertIn("已提交过", self.reply_text())

    def test_unauthorized_operator_writes_nothing(self):
        # admin 是允许发消息的人，但不在该审批的 scope 操作者里。
        self.binding.receive(message_id="m-u1", sender_id="admin",
                             chat_id="alice", chat_type="single", text="/a 1")
        self.assertEqual(self.decisions(), [])
        self.assertIn("无法提交", self.reply_text())


class CoreCardClickTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.module = load_wecom_gateway()
        self.relay_dir = self.root / "approval-relay"
        self.relay_dir.mkdir()
        state = {"next_num": 4, "items": {
            str(n): {"approval_id": f"aid-{n}", "status": "pending",
                     "expires_ms": 9999999999999,
                     "decision_scope": {"channel": "wecom", "chat_id": "room",
                                        "operator_ids": ["alice"]}}
            for n in (1, 2, 3)}}
        (self.relay_dir / "state.json").write_text(
            json.dumps(state, ensure_ascii=False))
        (self.relay_dir / "decisions.jsonl").write_text("")
        patcher = mock.patch.object(self.module, "APPROVAL_RELAY_DIR",
                                    self.relay_dir)
        patcher.start()
        self.addCleanup(patcher.stop)
        gateway = self.module.Gateway.__new__(self.module.Gateway)
        from types import SimpleNamespace
        gateway._core_binding = SimpleNamespace(enabled=True)
        gateway.allow_users = {"alice"}
        gateway.cards = {}
        self.gateway = gateway

    def decisions(self):
        path = self.relay_dir / "decisions.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in
                path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def card_frame(self, event_key="appr_always", task_id="appr-2"):
        return {"headers": {"req_id": "r1"}, "body": {
            "msgid": "ev1", "chatid": "room", "chattype": "single",
            "from": {"userid": "alice"},
            "event": {"eventtype": "template_card_event",
                      "template_card_event": {"event_key": event_key,
                                              "task_id": task_id}}}}

    async def test_approval_card_click_writes_decision_row(self):
        await self.gateway.handle_event_callback(self.card_frame())
        rows = self.decisions()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["num"], "2")
        self.assertEqual(rows[0]["decision"], "allow_always")
        self.assertEqual(rows[0]["channel"], "wecom-card")
        self.assertEqual(rows[0]["operator_id"], "alice")
        self.assertEqual(rows[0]["chat_id"], "room")

    async def test_non_approval_event_in_core_mode_writes_nothing(self):
        frame = {"headers": {"req_id": "r2"}, "body": {
            "msgid": "ev2", "chatid": "room", "chattype": "single",
            "from": {"userid": "alice"},
            "event": {"eventtype": "enter_chat"}}}
        await self.gateway.handle_event_callback(frame)
        self.assertEqual(self.decisions(), [])

    async def test_unknown_card_key_writes_nothing(self):
        await self.gateway.handle_event_callback(
            self.card_frame(event_key="btn_confirm", task_id="appr-2"))
        self.assertEqual(self.decisions(), [])


if __name__ == "__main__":
    unittest.main()
