"""Wave-2 gateway fixes: repro tests, written before the fix.

Each test pins the broken behaviour first (fails on the old code), then
passes after the fix. All state is a temporary SQLite database; the
production database is never touched.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mobile_channels.gateway import GatewayBinding
from mobile_channels.models import InboundMessage
from mobile_channels.storage import Store

VOICE_TEXT = "没听清，语音转写失败，请再发一次文字。"
DOWNLOAD_FAIL_TEXT = "附件下载失败，请再发一次。"


def load_gateway(channel):
    spec = importlib.util.spec_from_file_location(
        f"{channel}_wave2_test", ROOT / f"{channel}-bot/gateway.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_binding_case(test, channel="wecom"):
    test.temp = tempfile.TemporaryDirectory()
    test.addCleanup(test.temp.cleanup)
    test.root = Path(test.temp.name)
    test.database = test.root / "core.sqlite3"
    with Store(test.database):
        pass
    modes = test.root / "modes.json"
    modes.write_text(json.dumps({"channels": {
        ch: {"mode": "core", "database": str(test.database),
             "account_id": f"{ch}-account", "allowed_senders": ["alice", "admin"],
             "administrators": ["admin"]}
        for ch in ("weixin", "wecom")
    }}))
    env = patch.dict("os.environ", {"MUSE_CHANNEL_MODES": str(modes)})
    env.start()
    test.addCleanup(env.stop)
    test.binding = GatewayBinding(channel)
    test.addCleanup(test.binding.close)
    return test.binding


class BindingCase(unittest.TestCase):
    def setUp(self):
        make_binding_case(self, "wecom")
        self.png = self.root / "shot.png"
        self.png.write_bytes(b"\x89PNG\r\n\x1a\nfake-png-bytes")

    def rows(self, table):
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]

    def outbound_texts(self):
        return [row["text"] for row in self.rows("outbound_messages")]

    def receive(self, text="hello", message_id="m1", sender="alice",
                chat="alice", media=None):
        return self.binding.receive(
            message_id=message_id, sender_id=sender, chat_id=chat,
            chat_type="single", text=text, media=media)

    def image_media(self, path=None):
        return [{"kind": "image", "path": str(path or self.png),
                 "filename": "shot.png", "mime": "image/png"}]


class TestInboundMediaGate(BindingCase):
    """Item 1: inbound media must reach the executor, not a blanket refusal."""

    def test_image_media_passes_gate_and_reaches_context(self):
        self.receive(text="看看这张图", message_id="img1",
                     media=self.image_media())
        self.assertEqual(self.rows("tasks")[0]["state"], "queued")
        self.assertEqual(self.outbound_texts(), [])
        route = self.rows("gateway_routes")[0]
        stored = json.loads(route["media_json"])
        self.assertEqual(stored[0]["path"], str(self.png))
        with Store(self.database) as store:
            claim = store.claim("executor")
            from mobile_channels.runtime import CoreRunner
            runner = CoreRunner(store, Mock(), "executor")
            context = runner._context(claim)
        self.assertEqual(context.media[0]["path"], str(self.png))
        self.assertEqual(context.media[0]["kind"], "image")

    def test_image_waiting_behind_a_live_turn_keeps_its_own_attachment(self):
        from mobile_channels.runtime import CoreRunner, ExecutionOutcome

        self.receive(text="first turn", message_id="first")
        executor = Mock()
        executor.submit.side_effect = [ExecutionOutcome("accepted", run_id="first-run"),
                                      ExecutionOutcome("accepted", run_id="image-run")]
        executor.fold.return_value = ExecutionOutcome("accepted")
        executor.reply_started.return_value = False
        executor.query.return_value = ExecutionOutcome("succeeded", text="first reply")
        with Store(self.database) as store:
            runner = CoreRunner(store, executor, "executor")
            self.assertEqual(runner.tick(), "accepted")
            image = self.receive(text="看看这张图", message_id="img1", media=self.image_media())
            self.assertEqual(runner.tick(), "succeeded")
            executor.fold.assert_not_called()
            self.assertEqual(store.task(image.task_id)["state"], "queued")
            self.assertEqual(runner.tick(), "accepted")
            self.assertEqual(executor.submit.call_args.args[0].media, self.image_media())

    def test_video_media_rejected_with_explicit_reason(self):
        self.receive(text="看看视频", message_id="vid1", media=[
            {"kind": "video", "path": str(self.root / "clip.mp4"),
             "filename": "clip.mp4"}])
        self.assertEqual(self.rows("tasks")[0]["state"], "succeeded")
        reply = self.outbound_texts()[0]
        self.assertIn("视频", reply)
        self.assertIn("未执行", reply)

    def test_file_media_rejected_with_explicit_reason(self):
        self.receive(text="看看文件", message_id="file1", media=[
            {"kind": "file", "path": str(self.root / "doc.pdf"),
             "filename": "doc.pdf"}])
        reply = self.outbound_texts()[0]
        self.assertIn("文件", reply)
        self.assertIn("未执行", reply)

    def test_oversize_image_rejected_with_explicit_reason(self):
        big = self.root / "big.png"
        big.write_bytes(b"\x00" * (2 * 1024 * 1024 + 1))
        self.receive(text="大图", message_id="big1", media=[
            {"kind": "image", "path": str(big), "filename": "big.png"}])
        reply = self.outbound_texts()[0]
        self.assertIn("2 MiB", reply)
        self.assertIn("未执行", reply)

    def test_image_without_local_path_rejected_not_silently_queued(self):
        self.receive(text="图", message_id="nopath1", media=[
            {"kind": "image", "filename": "shot.png"}])
        reply = self.outbound_texts()[0]
        self.assertIn("未执行", reply)
        self.assertNotIn("附件信息已保存", reply)


class TestReceiptAndNoticeDelivery(BindingCase):
    """Item 2: task_receipts / approval_notices must actually be sent."""

    def _task_and_conv(self, message_id="job1"):
        self.receive(text="do work", message_id=message_id)
        task = self.rows("tasks")[0]
        return task["id"], task["conversation_id"]

    def test_started_receipt_delivered(self):
        task_id, conv_id = self._task_and_conv()
        with Store(self.database) as store:
            store.add_receipt(task_id, conv_id, "started", "正在处理…")
        sent = []
        self.binding.deliver_receipts(
            Store(self.database),
            lambda chat_id, chat_type, text, token: sent.append(text) or True)
        self.assertEqual(sent, ["正在处理…"])
        self.assertEqual(self.rows("task_receipts")[0]["state"], "delivered")

    def test_receipts_are_delivered_only_by_the_owning_channel_and_account(self):
        weixin = GatewayBinding("weixin")
        self.addCleanup(weixin.close)
        item = weixin.receive(message_id="wx1", sender_id="alice",
                              chat_id="wechat-user", chat_type="single", text="job")
        with Store(self.database) as store:
            store.add_receipt(item.task_id, item.conversation_id, "started", "WeChat receipt")
            sender = Mock(return_value=True)
            self.binding.deliver_receipts(store, sender)
            sender.assert_not_called()
            with patch.object(weixin, "account_id", "different-account"):
                weixin.deliver_receipts(store, sender)
                sender.assert_not_called()
            weixin.deliver_receipts(store, sender)
            sender.assert_called_once_with("wechat-user", "single", "WeChat receipt", "")

    def test_started_receipt_skipped_when_task_terminal(self):
        task_id, conv_id = self._task_and_conv()
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
            store.complete(claim, "done")
            store.add_receipt(task_id, conv_id, "started", "正在处理…")
        sent = []
        with Store(self.database) as store:
            self.binding.deliver_receipts(
                store, lambda *a: sent.append(a) or True)
        self.assertEqual(sent, [])
        self.assertEqual(self.rows("task_receipts")[0]["state"], "delivered")

    def test_wait_receipt_skipped_when_task_terminal(self):
        task_id, conv_id = self._task_and_conv()
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
            store.complete(claim, "done")
            store.add_receipt(task_id, conv_id, "wait", "前面还有 0 个任务在排队")
        sent = []
        with Store(self.database) as store:
            self.binding.deliver_receipts(
                store, lambda *a: sent.append(a) or True)
        self.assertEqual(sent, [])
        self.assertEqual(self.rows("task_receipts")[0]["state"], "delivered")

    def test_failed_receipt_send_leaves_pending(self):
        task_id, conv_id = self._task_and_conv()
        with Store(self.database) as store:
            store.add_receipt(task_id, conv_id, "failed", "处理失败，请再发一次。")
        with Store(self.database) as store:
            self.binding.deliver_receipts(store, lambda *a: False)
        self.assertEqual(self.rows("task_receipts")[0]["state"], "pending")

    def _insert_notice(self, row_id, channel, chat_id, text, state):
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS approval_notices ("
                "id TEXT PRIMARY KEY, channel TEXT NOT NULL, "
                "chat_id TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
                "state TEXT NOT NULL DEFAULT 'pending', created_at REAL NOT NULL)")
            connection.execute(
                "INSERT INTO approval_notices VALUES (?,?,?,?,?,?)",
                (row_id, channel, chat_id, text, state, 1.0))

    def test_distinct_approval_notice_events_can_have_identical_text(self):
        self._insert_notice("down-1", "wecom", "alice", "审批连接断开", "delivered")
        self._insert_notice("down-2", "wecom", "alice", "审批连接断开", "pending")
        sent = []
        with Store(self.database) as store:
            for _ in range(2):
                self.binding.deliver_approval_notices(
                    store, lambda *a: sent.append(a[2]) or True)
        self.assertEqual(sent, ["审批连接断开"])
        states = {r["id"]: r["state"] for r in self.rows("approval_notices")}
        self.assertEqual(states, {"down-1": "delivered", "down-2": "delivered"})

    def test_old_random_notice_ids_still_deduplicate_replayed_text(self):
        self._insert_notice("appr-new1-aabbccdd", "wecom", "alice", "待审批 #1", "delivered")
        self._insert_notice("appr-new1-11223344", "wecom", "alice", "待审批 #1", "pending")
        self._insert_notice("appr-core-new1-" + "a" * 32, "wecom", "alice", "待审批 #1", "pending")
        sender = Mock(return_value=True)
        with Store(self.database) as store:
            counts = self.binding.deliver_approval_notices(store, sender)
        sender.assert_called_once_with("alice", "single", "待审批 #1", "")
        self.assertEqual(counts, {"sent": 1, "skipped": 1, "failed": 0})


class TestCommands(BindingCase):
    """Item 4: /help, /queue clear/drop, distinct /status /queue, /check, /ping."""

    def test_help_lists_all_commands(self):
        self.receive(text="/help", message_id="help1")
        reply = self.outbound_texts()[0]
        for cmd in ("/ping", "/status", "/queue", "/queue clear",
                    "/queue drop", "/check", "/new", "/stop",
                    "/a", "/b", "/c"):
            self.assertIn(cmd, reply)

    def test_queue_clear_cancels_queued(self):
        self.receive(text="job one", message_id="q1")
        self.receive(text="job two", message_id="q2")
        self.receive(text="/queue clear", message_id="q3")
        states = [r["state"] for r in self.rows("tasks")]
        self.assertEqual(states.count("cancelled"), 2)
        self.assertIn("2", self.outbound_texts()[-1])
        self.assertIn("排队", self.outbound_texts()[-1])

    def test_queue_drop_n_drops_only_that_task(self):
        self.receive(text="job one", message_id="d1")
        self.receive(text="job two", message_id="d2")
        self.receive(text="job three", message_id="d3")
        self.receive(text="/queue drop 2", message_id="d4")
        tasks = {r["id"]: r for r in self.rows("tasks")}
        by_text = {}
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            for row in connection.execute(
                    "SELECT t.id, m.text FROM tasks t JOIN inbound_messages m "
                    "ON m.id=t.message_id"):
                by_text[row["text"]] = row["id"]
        self.assertEqual(tasks[by_text["job two"]]["state"], "cancelled")
        self.assertEqual(tasks[by_text["job one"]]["state"], "queued")
        self.assertEqual(tasks[by_text["job three"]]["state"], "queued")

    def test_queue_drop_number_matches_list_when_a_task_is_running(self):
        self.receive(text="running job", message_id="q-active")
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
        first = self.receive(text="first queued", message_id="q-first")
        second = self.receive(text="second queued", message_id="q-second")
        self.receive(text="/queue", message_id="q-view")
        queue_line = next(line for line in self.outbound_texts()[-1].splitlines()
                          if line.endswith("second queued"))
        shown_index = queue_line.split(".", 1)[0]
        self.receive(text=f"/queue drop {shown_index}", message_id="q-drop")
        with Store(self.database) as store:
            self.assertEqual(store.task(first.task_id)["state"], "queued")
            self.assertEqual(store.task(second.task_id)["state"], "cancelled")

    def test_queue_drop_with_extra_spaces_parsed(self):
        self.receive(text="job one", message_id="s1")
        self.receive(text="/queue   drop   1", message_id="s2")
        self.assertEqual(
            [r["state"] for r in self.rows("tasks")].count("cancelled"), 1)

    def test_queue_drop_invalid_index_replies_without_cancelling(self):
        self.receive(text="job one", message_id="i1")
        self.receive(text="/queue drop 9", message_id="i2")
        self.assertEqual(
            [r["state"] for r in self.rows("tasks")].count("cancelled"), 0)
        self.assertIn("1", self.outbound_texts()[-1])

    def test_status_and_queue_return_different_text(self):
        self.receive(text="job one", message_id="v1")
        self.receive(text="/status", message_id="v2")
        status_reply = self.outbound_texts()[-1]
        self.receive(text="/queue", message_id="v3")
        queue_reply = self.outbound_texts()[-1]
        self.assertNotEqual(status_reply, queue_reply)
        self.assertIn("job one", queue_reply)
        self.assertIn("网关", status_reply)

    def test_check_reports_each_item(self):
        self.receive(text="/check", message_id="c1")
        reply = self.outbound_texts()[0]
        for item in ("网关连接", "core 库", "执行器"):
            self.assertIn(item, reply)
        self.assertIn("通过", reply)

    def test_ping_includes_status_and_last_error(self):
        self.receive(text="work", message_id="p1")
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
            store.fail(claim, "boom-evidence-xyz")
        self.receive(text="/ping", message_id="p2")
        reply = self.outbound_texts()[-1]
        self.assertIn("pong", reply)
        self.assertIn("boom-evidence-xyz", reply)

    def test_stop_no_active_tasks(self):
        self.receive(text="/stop", message_id="stop0")
        self.assertEqual(self.outbound_texts()[0], "当前没有正在跑的任务。")

    def test_stop_queued_only_wording(self):
        self.receive(text="job one", message_id="st1")
        self.receive(text="job two", message_id="st2")
        self.receive(text="/stop", message_id="st3")
        reply = self.outbound_texts()[-1]
        self.assertIn("已立即取消 2 个排队任务", reply)
        self.assertNotIn("已登记", reply)


class TestStopMixed(BindingCase):
    """Item 7: /stop wording splits immediately-cancelled from pending-cancel."""

    def test_stop_mixed_queued_and_running(self):
        self.receive(text="job one", message_id="m1")
        self.receive(text="job two", message_id="m2")
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
        self.receive(text="/stop", message_id="m3")
        reply = self.outbound_texts()[-1]
        self.assertIn("已立即取消 1 个排队任务", reply)
        self.assertIn("已登记 1 个正在跑的任务", reply)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Adapter-level tests: real wecom/weixin callbacks (voice + media download),
# and the real delivery loop (outbound media).
# ---------------------------------------------------------------------------


class AdapterCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        make_binding_case(self, "wecom")
        self.png = self.root / "shot.png"
        self.png.write_bytes(b"\x89PNG\r\n\x1a\nfake-png-bytes")
        self.modules = {ch: load_gateway(ch) for ch in ("weixin", "wecom")}
        self.gateways = {}
        for channel, module in self.modules.items():
            home_patcher = patch.object(module, "muse_home", return_value=self.root)
            home_patcher.start()
            self.addCleanup(home_patcher.stop)
            state = self.root / channel
            state.mkdir(exist_ok=True)
            for key, filename in {
                    "STATE": ".", "INBOX": "inbox.jsonl",
                    "OUTBOX": "outbox.jsonl", "SEEN_FILE": "seen.jsonl",
                    "CONTEXT_FILE": "context.json",
                    "QUOTE_SPOOL": "spool.jsonl",
                    "BRIDGE_SPOOL": "spool.jsonl"}.items():
                if hasattr(module, key):
                    patcher = patch.object(module, key, state / filename)
                    patcher.start()
                    self.addCleanup(patcher.stop)
            gateway = module.Gateway.__new__(module.Gateway)
            gateway._core_binding = GatewayBinding(channel)
            self.addCleanup(gateway._core_binding.close)
            gateway.seen_msgids = set()
            gateway.context = {}
            gateway.msgs_received = 0
            gateway.msgs_sent = 0
            gateway.allow_users = {"alice", "admin"}
            gateway.write_status = Mock()
            gateway._slash_dispatch = AsyncMock(
                side_effect=AssertionError("legacy command dispatcher used"))
            gateway.append_jsonl = Mock(
                side_effect=AssertionError("legacy queue used"))
            gateway._commit_inbound_seen = Mock(
                side_effect=AssertionError("legacy dedup used"))
            gateway._save_context = Mock()
            gateway.respond = AsyncMock(return_value={"errcode": 0})
            gateway.send_text = AsyncMock(return_value={"ret": 0})
            gateway.send_frame = AsyncMock(return_value={"errcode": 0})
            self.gateways[channel] = gateway

    def rows(self, table):
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]

    async def run_delivery(self, channel, terminal, ticks=6, *, client=None, credentials=None):
        binding = self.gateways[channel]._core_binding
        original_sleep = asyncio.sleep
        ready = asyncio.Event()
        completed = 0

        async def bounded_sleep(delay):
            nonlocal completed
            completed += 1
            if completed >= ticks and terminal():
                ready.set()
                await original_sleep(60)
            await original_sleep(0.001)

        with patch("mobile_channels.gateway.asyncio.sleep",
                   side_effect=bounded_sleep):
            task = asyncio.create_task(
                binding.delivery_loop(self.gateways[channel], client, credentials))
            try:
                await asyncio.wait_for(ready.wait(), timeout=5)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task


class TestAdapterMediaDownload(AdapterCase):
    """Item 1 (gateway side): download before core.receive, full media kept."""

    async def test_wecom_core_downloads_media_before_receive(self):
        gateway = self.gateways["wecom"]
        order = []

        async def fake_download(media, msgid):
            order.append("download")
            media[0]["local_path"] = str(self.png)

        gateway._download_media = AsyncMock(side_effect=fake_download)
        orig_receive = gateway._core_binding.receive

        def spy_receive(*args, **kwargs):
            order.append("receive")
            return orig_receive(*args, **kwargs)

        gateway._core_binding.receive = spy_receive
        frame = {"headers": {"req_id": "req-img"}, "body": {
            "msgid": "wimg1", "msgtype": "image",
            "image": {"url": "https://example.invalid/x", "aeskey": "k"},
            "from": {"userid": "alice"}, "chatid": "alice",
            "chattype": "single"}}
        await gateway.handle_message_callback(frame)
        self.assertEqual(order, ["download", "receive"])
        stored = json.loads(self.rows("gateway_routes")[0]["media_json"])
        self.assertEqual(stored[0]["kind"], "image")
        self.assertEqual(stored[0]["path"], str(self.png))
        self.assertEqual(stored[0]["filename"], "shot.png")

    async def test_weixin_core_downloads_media_before_receive(self):
        gateway = self.gateways["weixin"]
        order = []

        async def fake_download(client, media, msgid):
            order.append("download")
            media[0]["local_path"] = str(self.png)

        gateway._download_media = AsyncMock(side_effect=fake_download)
        orig_receive = gateway._core_binding.receive

        def spy_receive(*args, **kwargs):
            order.append("receive")
            return orig_receive(*args, **kwargs)

        gateway._core_binding.receive = spy_receive
        message = {"message_id": "ximg1", "message_type": 1,
                   "from_user_id": "alice", "context_token": "tok",
                   "item_list": [{"type": 2, "image_item": {"media": {}}}]}
        await gateway.handle_inbound(None, {"user_id": "alice"}, message)
        self.assertEqual(order, ["download", "receive"])
        stored = json.loads(self.rows("gateway_routes")[0]["media_json"])
        self.assertEqual(stored[0]["path"], str(self.png))

    async def test_wecom_download_failure_gets_explicit_reply_no_task(self):
        gateway = self.gateways["wecom"]
        gateway._download_media = AsyncMock(return_value=None)
        frame = {"headers": {"req_id": "req-fail"}, "body": {
            "msgid": "wfail1", "msgtype": "image",
            "image": {"url": "https://example.invalid/x", "aeskey": "k"},
            "from": {"userid": "alice"}, "chatid": "alice",
            "chattype": "single"}}
        await gateway.handle_message_callback(frame)
        self.assertEqual(self.rows("tasks"), [])
        gateway.respond.assert_awaited_once()
        body = gateway.respond.call_args.args[1]
        self.assertIn("下载失败", body["markdown"]["content"])

    async def test_weixin_download_failure_gets_explicit_reply_no_task(self):
        gateway = self.gateways["weixin"]
        gateway._download_media = AsyncMock(return_value=None)
        message = {"message_id": "xfail1", "message_type": 1,
                   "from_user_id": "alice", "context_token": "tok",
                   "item_list": [{"type": 2, "image_item": {"media": {}}}]}
        await gateway.handle_inbound(None, {"user_id": "alice"}, message)
        self.assertEqual(self.rows("tasks"), [])
        gateway.send_text.assert_awaited_once()
        self.assertIn("下载失败", gateway.send_text.call_args.args[3])


class TestVoice(AdapterCase):
    """Item 5: untranscribed voice gets an explicit reply, never silence."""

    async def test_wecom_voice_without_transcription_replies_directly(self):
        gateway = self.gateways["wecom"]
        frame = {"headers": {"req_id": "req-v"}, "body": {
            "msgid": "wv1", "msgtype": "voice", "voice": {},
            "from": {"userid": "alice"}, "chatid": "alice",
            "chattype": "single"}}
        await gateway.handle_message_callback(frame)
        self.assertEqual(self.rows("tasks"), [])
        gateway.respond.assert_awaited_once()
        body = gateway.respond.call_args.args[1]
        self.assertIn("没听清", body["markdown"]["content"])

    async def test_wecom_voice_with_transcription_flows_to_core(self):
        gateway = self.gateways["wecom"]
        frame = {"headers": {"req_id": "req-vt"}, "body": {
            "msgid": "wv2", "msgtype": "voice",
            "voice": {"content": "你好"},
            "from": {"userid": "alice"}, "chatid": "alice",
            "chattype": "single"}}
        await gateway.handle_message_callback(frame)
        self.assertEqual(len(self.rows("tasks")), 1)
        gateway.respond.assert_not_called()

    async def test_weixin_voice_without_transcription_replies_directly(self):
        gateway = self.gateways["weixin"]
        message = {"message_id": "xv1", "message_type": 1,
                   "from_user_id": "alice", "context_token": "tok",
                   "item_list": [{"type": 3, "voice_item": {}}]}
        await gateway.handle_inbound(None, {"user_id": "alice"}, message)
        self.assertEqual(self.rows("tasks"), [])
        self.assertEqual(self.rows("outbound_messages"), [])
        gateway.send_text.assert_awaited_once()
        self.assertIn("没听清", gateway.send_text.call_args.args[3])

    async def test_weixin_voice_with_transcription_flows_to_core(self):
        gateway = self.gateways["weixin"]
        message = {"message_id": "xv2", "message_type": 1,
                   "from_user_id": "alice", "context_token": "tok",
                   "item_list": [{"type": 3, "voice_item": {"text": "你好"}}]}
        await gateway.handle_inbound(None, {"user_id": "alice"}, message)
        self.assertEqual(len(self.rows("tasks")), 1)
        gateway.send_text.assert_not_called()

    def test_weixin_extract_text_has_no_voice_placeholder(self):
        module = self.modules["weixin"]
        text, media = module.Gateway._extract_text(
            {"item_list": [{"type": 3, "voice_item": {}}]})
        self.assertNotIn("[语音]", text)
        self.assertEqual(text, "")


class TestWecomOutboundMedia(AdapterCase):
    """Item 3: DeliveryClaim.media is really uploaded and sent on WeCom."""

    async def _complete_with_media(self, media):
        binding = self.gateways["wecom"]._core_binding
        binding.receive(message_id="job1", sender_id="alice", chat_id="alice",
                        chat_type="single", text="画一张图")
        with Store(self.database) as store:
            claim = store.claim("executor")
            store.begin_submission(claim)
            store.mark_submitted(claim, "run-1")
            store.complete(claim, "结果文字", media=media)

    async def test_wecom_delivery_sends_text_before_media(self):
        gateway = self.gateways["wecom"]
        order = []

        async def fake_upload(data, mtype, filename):
            order.append(("upload", mtype, filename))
            return "media-123"

        async def fake_frame(frame, wait_response=False, timeout=10.0):
            order.append(("frame", frame["body"]["msgtype"]))
            return {"errcode": 0}

        gateway.upload_media = AsyncMock(side_effect=fake_upload)
        gateway.send_frame = AsyncMock(side_effect=fake_frame)
        await self._complete_with_media(
            [{"kind": "image", "path": str(self.png), "filename": "shot.png"}])
        await self.run_delivery(
            "wecom",
            lambda: self.rows("outbound_messages")[0]["state"] == "delivered")
        self.assertEqual(order[0], ("upload", "image", "shot.png"))
        self.assertEqual(order[1], ("frame", "markdown"))
        self.assertEqual(order[2], ("frame", "image"))
        image_frame = gateway.send_frame.call_args_list[1].args[0]
        self.assertEqual(image_frame["body"]["image"], {"media_id": "media-123"})
        self.assertEqual(image_frame["body"]["chatid"], "alice")

    async def test_wecom_upload_failure_retries_without_text(self):
        gateway = self.gateways["wecom"]
        gateway.upload_media = AsyncMock(
            side_effect=RuntimeError("upload boom"))
        await self._complete_with_media(
            [{"kind": "image", "path": str(self.png), "filename": "shot.png"}])
        await self.run_delivery(
            "wecom",
            lambda: self.rows("outbound_parts")[0]["state"] == "retry_wait")
        gateway.send_frame.assert_not_called()
        self.assertEqual(self.rows("outbound_parts")[0]["state"], "retry_wait")

    async def test_wecom_outbound_media_cannot_upload_a_link_to_credentials(self):
        gateway = self.gateways["wecom"]
        gateway.upload_media = AsyncMock(return_value="must-not-upload")
        credentials = self.root / "private" / "credentials.env"
        credentials.parent.mkdir()
        credentials.write_text("FAKE_TEST_SECRET=private")
        alias = self.root / "result.txt"
        alias.symlink_to(credentials)
        with patch.object(self.modules["wecom"], "CRED_FILE", credentials):
            await self._complete_with_media([{"kind": "file", "path": str(alias)}])
            await self.run_delivery("wecom", lambda: self.rows("outbound_messages")[0]["state"]
                                    in ("delivered", "rejected"))
        self.assertEqual(self.rows("outbound_messages")[0]["state"], "rejected")
        gateway.upload_media.assert_not_called()

    async def test_wecom_missing_media_file_rejected_with_receipt(self):
        gateway = self.gateways["wecom"]
        gateway.upload_media = AsyncMock(return_value="media-123")
        await self._complete_with_media(
            [{"kind": "image", "path": str(self.root / "gone.png"),
              "filename": "gone.png"}])
        await self.run_delivery(
            "wecom",
            lambda: self.rows("outbound_messages")[0]["state"] == "rejected")
        # The claim's own text frame must never go out, but the
        # delivery_failed receipt for the rejection IS sent (item 2).
        texts = [c.args[0]["body"].get("markdown", {}).get("content", "")
                 for c in gateway.send_frame.call_args_list]
        self.assertNotIn("结果文字", texts)
        self.assertIn("回复已生成但发送失败，请再发一次。", texts)
        receipts = self.rows("task_receipts")
        self.assertTrue(any(r["kind"] == "delivery_failed" for r in receipts))
