"""复现：consume_decisions 的 ModeError 静默丢行 + notify 失败中断整轮 poll。

只用临时目录与临时 core 库，绝不碰生产库。
跑红 = 问题存在；修复后应全绿。
"""
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_relay():
    spec = importlib.util.spec_from_file_location(
        "approval_consume_core_fix", ROOT / "approval-relay/approval_relay.py")
    relay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(relay)
    return relay


def require_approval_engine(relay):
    class _RequireApproval:
        def evaluate(self, record, what=""):
            return types.SimpleNamespace(
                action=relay.PolicyAction.REQUIRE_APPROVAL,
                risk_level=relay.RiskLevel.NORMAL,
                is_high_risk=False,
                rule_name="normal_approval",
                reason="test stub: manual approval required",
            )
    return _RequireApproval()


class ConsumeOffsetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.relay = load_relay()
        self.relay.STATE_F = str(self.base / "state.json")
        self.relay.DECISIONS_F = str(self.base / "decisions.jsonl")
        self.relay.OFFSET_F = str(self.base / "decisions.offset")
        self.relay.notify = mock.Mock()
        self.state = {"next_num": 2, "items": {"1": {
            "approval_id": "approval", "status": "pending",
            "expires_ms": 9999999999999,
            "decision_scope": {"channel": "wecom", "chat_id": "room",
                               "operator_ids": ["alice"]}}}}
        self.row = {"num": "1", "decision": "allow_once", "channel": "wecom",
                    "chat_id": "room", "operator_id": "alice", "ts": 1}
        env = mock.patch.dict(os.environ, {"APPROVAL_ADMINS_JSON": "{}"})
        env.start()
        self.addCleanup(env.stop)

    def write(self):
        self.relay.save_state(self.state)
        (self.base / "decisions.jsonl").write_text(
            json.dumps(self.row) + "\n")

    def offset(self):
        path = self.base / "decisions.offset"
        return path.read_text().strip() if path.exists() else None

    def test_mode_error_row_is_retained_for_retry(self):
        # modes 文件非法（core 缺 database）：lease 读不到，该行必须保留。
        modes = self.base / "modes.json"
        modes.write_text(json.dumps({"channels": {
            "wecom": {"mode": "core", "account_id": "bot",
                      "allowed_senders": ["alice"]}}}))
        self.write()
        gateway = mock.Mock()
        with mock.patch.dict(os.environ,
                             {"MUSE_CHANNEL_MODES": str(modes)}):
            self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_not_called()
        self.assertEqual(self.offset(), "0")
        self.assertEqual(
            (self.base / "decisions.jsonl").read_text(),
            json.dumps(self.row) + "\n")

    def test_retained_row_is_consumed_after_modes_fixed(self):
        modes = self.base / "modes.json"
        modes.write_text(json.dumps({"channels": {
            "wecom": {"mode": "core", "account_id": "bot",
                      "allowed_senders": ["alice"]}}}))
        self.write()
        gateway = mock.Mock()
        with mock.patch.dict(os.environ,
                             {"MUSE_CHANNEL_MODES": str(modes)}):
            self.relay.consume_decisions(gateway, self.state)
        self.assertEqual(self.offset(), "0")
        # 修好 modes 文件（core 配上真实临时库），下轮应正常消费该行。
        db = self.base / "core.sqlite3"
        from mobile_channels.storage import Store
        with Store(db):
            pass
        modes.write_text(json.dumps({"channels": {
            "wecom": {"mode": "core", "database": str(db),
                      "account_id": "bot", "allowed_senders": ["alice"],
                      "administrators": ["alice"]}}}))
        with mock.patch.dict(os.environ,
                             {"MUSE_CHANNEL_MODES": str(modes)}):
            self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_called_once()
        self.assertEqual(self.offset(),
                         str(len(json.dumps(self.row) + "\n")))
        self.assertEqual(self.state["items"]["1"]["status"],
                         "decided:allow_once")

    def test_normal_consumption_still_advances_offset(self):
        self.write()
        gateway = mock.Mock()
        env = mock.patch.dict(os.environ, {})
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("MUSE_CHANNEL_MODES", None)
        self.relay.consume_decisions(gateway, self.state)
        gateway.call_json.assert_called_once()
        self.assertEqual(self.offset(),
                         str(len(json.dumps(self.row) + "\n")))


class PollOnceIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.relay = load_relay()
        self.relay.TEST = False
        self.relay.STATE_F = str(self.base / "state.json")
        self.relay.DECISIONS_F = str(self.base / "decisions.jsonl")
        self.relay.OFFSET_F = str(self.base / "decisions.offset")
        for channel in ("weixin", "wecom"):
            self.relay.CHANNELS[channel]["outbox"] = str(
                self.base / channel / "outbox.jsonl")
            (self.base / channel).mkdir()
        # 企微 core 但 database 指向目录：写表必然失败；微信 legacy 正常。
        modes = self.base / "modes.json"
        modes.write_text(json.dumps({"channels": {
            "weixin": {"mode": "legacy"},
            "wecom": {"mode": "core", "database": str(self.base),
                      "account_id": "wecom-bot",
                      "allowed_senders": ["alice"],
                      "administrators": ["alice"]}}}))
        env = mock.patch.dict(
            os.environ, {"MUSE_CHANNEL_MODES": str(modes)})
        env.start()
        self.addCleanup(env.stop)

    def test_failed_wecom_notice_does_not_stop_weixin_notice(self):
        gateway = mock.Mock()
        response = {"pending_approvals": [
            {"approval_id": "aid-1", "registered_command": "echo one",
             "display_name": "op1",
             "request_expires_at_ms": 9999999999999,
             "decision_scope": {"channel": "wecom", "chat_id": "room",
                                "operator_ids": ["alice"]}},
            {"approval_id": "aid-2", "registered_command": "echo two",
             "display_name": "op2",
             "request_expires_at_ms": 9999999999999,
             "decision_scope": {"channel": "weixin", "chat_id": "w1",
                                "operator_ids": ["alice"]}},
        ]}
        st = {"next_num": 1, "items": {}}
        # 不抛异常才算修复：旧代码在第一条通知失败时整轮中断。
        self.relay.poll_once(gateway, st, response=response,
                             policy_engine=require_approval_engine(self.relay))
        self.assertFalse(st["items"]["1"]["notified"])
        self.assertEqual(st["items"]["1"]["status"], "pending")
        self.assertTrue(st["items"]["2"]["notified"])
        weixin_outbox = Path(self.relay.CHANNELS["weixin"]["outbox"])
        self.assertTrue(weixin_outbox.exists())
        self.assertEqual(len(weixin_outbox.read_text().splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
