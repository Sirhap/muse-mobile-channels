"""Idle gateway status advances only after actual Tencent acknowledgments."""
import asyncio
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]


def load(channel):
    spec = importlib.util.spec_from_file_location(channel + "_health", ROOT / (channel + "-bot") / "gateway.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GatewayHealthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.status = Path(directory.name) / "status.json"

    def session(self, module):
        gateway = module.Gateway.__new__(module.Gateway)
        gateway.connected = True
        gateway.state = "connected"
        gateway.started_at = 1
        gateway.msgs_received = gateway.msgs_sent = 0
        gateway.last_platform_ok_at = None
        gateway.last_error = ""
        return gateway

    async def test_weixin_empty_poll_publishes_a_fresh_confirmation(self):
        module = load("weixin")
        gateway = self.session(module)
        gateway.sync_buf = "cursor"
        gateway.outbox_loop = AsyncMock()
        gateway.feedback_watch_loop = AsyncMock()
        gateway._process_update_batch = AsyncMock()
        gateway._post = AsyncMock(side_effect=[{}, {"errcode": 0, "msgs": []}, asyncio.CancelledError])
        with patch.object(module, "STATUS", self.status), self.assertRaises(asyncio.CancelledError):
            await gateway.run_session(None, {"base_url": "https://test.invalid", "token": "fixture"})
        written = json.loads(self.status.read_text())
        self.assertTrue(written["connected"])
        self.assertIsInstance(written["last_platform_ok_at"], int)
        self.assertEqual(written["last_platform_ok_at"], written["updated_at"])
        gateway._process_update_batch.assert_awaited_once()

    async def test_wecom_successful_pong_publishes_a_fresh_confirmation(self):
        module = load("wecom")
        gateway = self.session(module)
        gateway.send_frame = AsyncMock(return_value={"errcode": 0})
        with patch.object(module, "STATUS", self.status), patch.object(
            module.asyncio, "sleep", AsyncMock(side_effect=[None, asyncio.CancelledError])
        ), self.assertRaises(asyncio.CancelledError):
            await gateway.heartbeat_loop()
        written = json.loads(self.status.read_text())
        self.assertTrue(written["connected"])
        self.assertIsInstance(written["last_platform_ok_at"], int)
        self.assertEqual(written["last_platform_ok_at"], written["updated_at"])
        self.assertEqual(gateway.send_frame.await_args.args[0]["cmd"], "ping")
