"""审批通知的单渠道部署与存储故障回归，全部使用临时目录。"""

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


def load_relay():
    """导入真实审批模块，仅替换本机未安装的平台传输依赖。"""
    gateway = types.ModuleType("muse_cli.gateway")
    gateway.load_cookies = mock.Mock()
    gateway.GatewayError = RuntimeError
    transport = types.ModuleType("gw2")
    transport.GW2 = mock.Mock()
    spec = importlib.util.spec_from_file_location(
        "approval_notify_resilience", ROOT / "approval-relay/approval_relay.py")
    relay = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {
        "gw2": transport, "muse_cli": types.ModuleType("muse_cli"),
        "muse_cli.gateway": gateway,
    }):
        spec.loader.exec_module(relay)
    return relay


class ApprovalNotifyResilienceTests(unittest.TestCase):
    """缺失或故障的通道不能阻止健康通道，也不能伪造通知成功。"""

    def setUp(self):
        """每个用例获得独立状态与 outbox，关闭 shadow 模式。"""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.relay = load_relay()
        self.relay.TEST = False
        self.relay.STATE_F = str(self.base / "state.json")
        self.relay.DECISIONS_F = str(self.base / "decisions.jsonl")
        for channel in self.relay.CHANNEL_ORDER:
            self.relay.CHANNELS[channel]["outbox"] = str(
                self.base / channel / "outbox.jsonl")
        env = mock.patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop("APPROVAL_NOTIFY_CHANNELS", None)

    def rows(self, channel):
        """读取本次沙箱实际写入的通知。"""
        path = Path(self.relay.CHANNELS[channel]["outbox"])
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def test_only_wecom_installed(self):
        """默认广播遇到未安装的微信后仍写入企微。"""
        (self.base / "wecom").mkdir()
        self.relay.notify("pending approval", "one")
        self.assertEqual(len(self.rows("wecom")), 1)
        self.assertFalse((self.base / "weixin").exists())

    def test_first_channel_io_failure_does_not_block_second(self):
        """单个通道没有写权限时仍尝试剩余通道。"""
        for channel in self.relay.CHANNEL_ORDER:
            (self.base / channel).mkdir()
        real_open = open

        def fail_weixin(path, *args, **kwargs):
            """仅注入微信 outbox 的写入故障。"""
            if str(path) == self.relay.CHANNELS["weixin"]["outbox"]:
                raise PermissionError("injected outbox failure")
            return real_open(path, *args, **kwargs)

        with mock.patch("builtins.open", side_effect=fail_weixin):
            self.relay.notify("pending approval", "io")
        self.assertEqual(len(self.rows("wecom")), 1)

    def test_only_weixin_installed(self):
        """后续企微目录缺失不能推翻微信通知的成功结果。"""
        (self.base / "weixin").mkdir()
        self.relay.notify("pending approval", "weixin-only")
        self.assertEqual(len(self.rows("weixin")), 1)
        self.assertFalse((self.base / "wecom").exists())

    def test_all_targets_unavailable_raises(self):
        """全部写入失败必须报告失败，让轮询保留重试机会。"""
        with self.assertRaises(OSError):
            self.relay.notify("pending approval", "none")

    def test_explicit_source_is_not_broadcast_to_other_channel(self):
        """显式来源失败时不能扩大通知目标。"""
        (self.base / "wecom").mkdir()
        with self.assertRaises(OSError):
            self.relay.notify("pending approval", "source",
                              item={"source_channel": "weixin"})
        self.assertEqual(self.rows("wecom"), [])

    def test_successful_channel_keeps_card_payload(self):
        """单企微部署继续生成审批按钮卡片。"""
        (self.base / "wecom").mkdir()
        self.relay.send_approval_notice("1", {
            "who": "test", "what": "命令：echo test", "expires_ms": None,
        })
        row, = self.rows("wecom")
        self.assertEqual(row["mode"], "send_card")
        self.assertIn("card", row)

    def test_failed_poll_retries_after_storage_recovers(self):
        """整轮通知失败后同一状态仍未通知，恢复目录后可重试一次。"""
        gateway = mock.Mock()
        gateway.call_json.return_value = {"pending_approvals": [{
            "approval_id": "approval-1", "registered_command": "echo test",
            "display_name": "test",
        }]}
        state = {"next_num": 1, "items": {}}
        with self.assertRaises(OSError):
            self.relay.poll_once(gateway, state)
        self.assertFalse(state["items"]["1"]["notified"])
        (self.base / "wecom").mkdir()
        self.relay.poll_once(gateway, state)
        self.assertTrue(state["items"]["1"]["notified"])
        self.relay.poll_once(gateway, state)
        self.assertEqual(len(self.rows("wecom")), 1)

    def test_promoted_group_retries_after_storage_recovers(self):
        """原审批结束后，分组审批的独立通知失败仍能重试。"""
        gateway = mock.Mock()
        gateway.call_json.return_value = {"pending_approvals": [{
            "approval_id": "child", "registered_command": "echo child",
        }]}
        state = {"next_num": 3, "items": {
            "1": {"approval_id": "primary", "status": "decided:allow_once"},
            "2": {"approval_id": "child", "status": "pending", "notified": True,
                  "grouped_with": "1", "who": "test", "what": "命令：echo child"},
        }}
        with self.assertRaises(OSError):
            self.relay.poll_once(gateway, state)
        self.assertFalse(state["items"]["2"]["notified"])
        (self.base / "wecom").mkdir()
        self.relay.poll_once(gateway, state)
        self.assertTrue(state["items"]["2"]["notified"])
        self.assertEqual(len(self.rows("wecom")), 1)


if __name__ == "__main__":
    unittest.main()
