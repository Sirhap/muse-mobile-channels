"""入站连发防打碎合并器（Inbound Coalescer）全量单元测试套件。

覆盖场景：
1. 单条普通消息测试：静默 2.0s 后正常出队，字段保持原貌；
2. 多条文本快速合并测试：短时间内发送多条文本，按 \\n 拼接，首条 ID 为主 ID，merged_ids 完整记录；
3. 图文混发合并测试：先发提示文本后发多张图片附件，合并为带有完整文本与媒体数组的单个实体；
4. 最大等待时间硬上限保护：高频持续发送（每 0.5s），达到 6.0s 上限时强制冲刷出队，防止长时间饥饿；
5. 命令屏障（Barrier）测试：收到以 / 开头的控制指令立即冲刷先前积攒的消息，命令本身零等待独立立即执行；
6. 多会话/多联系人并发隔离测试：不同频道、群聊或用户的消息独立计时、独立合并，互不串联；
7. 空消息/仅附件边界测试：无文字纯附件连发，媒体列表正确合并并赋予规范占位符 [附件]；
8. 网关集成与路由表联动测试：GatewayBinding.receive 完整支持 merged_ids 与 CoalescedInbound，数据库路由对账完备；
9. 优雅停机清空测试：flush_all 冲刷所有会话且重置内部状态。

全部使用 Python 标准库，零外部依赖，可在毫秒级运行。
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from mobile_channels.coalesce import (
    IDLE_GAP_SECS,
    MAX_WAIT_SECS,
    CoalescedInbound,
    InboundCoalescer,
    InboundMessageItem,
)
from mobile_channels.gateway import GatewayBinding
from mobile_channels.storage import Store


class _SimulatedClock:
    """可精确控制的手动推进虚拟时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.current_time = float(start)

    def __call__(self) -> float:
        return self.current_time

    def set(self, timestamp: float) -> None:
        self.current_time = float(timestamp)

    def advance(self, delta: float) -> None:
        self.current_time += float(delta)


def _msg(
    message_id: str,
    text: str = "",
    channel: str = "weixin",
    account_id: str = "acc1",
    sender_id: str = "user_alice",
    chat_id: str = "chat_1",
    chat_type: str = "single",
    context_token: str = "",
    media: list | None = None,
    received_at: float | None = None,
) -> InboundMessageItem:
    """快速构造入站消息测试对象。"""
    return InboundMessageItem(
        channel=channel,
        account_id=account_id,
        message_id=message_id,
        sender_id=sender_id,
        chat_id=chat_id,
        chat_type=chat_type,
        text=text,
        context_token=context_token,
        media=list(media or []),
        received_at=received_at,
    )


class InboundCoalescerTests(unittest.TestCase):
    """入站合并器算法核心单元测试。"""

    def setUp(self) -> None:
        self.clock = _SimulatedClock(100.0)
        self.coalescer = InboundCoalescer(
            idle_gap_secs=2.0,
            max_wait_secs=6.0,
            clock=self.clock,
        )

    def test_single_message_emitted_after_idle_gap(self) -> None:
        """用例 1：单条普通消息测试，满 2.0s 静默后正常出队，字段保持原貌。"""
        self.clock.set(100.0)
        msg = _msg("m1", text="你好，在吗？", context_token="ctx_token_1")
        # 1. 投入普通消息，返回值必为空列表
        ready = self.coalescer.feed(msg)
        self.assertEqual(ready, [])
        self.assertEqual(len(self.coalescer), 1)
        self.assertEqual(self.coalescer.pending_count(), 1)

        # 2. 静默时间未满 2.0s（推进 1.9s），due 应无产出
        self.clock.advance(1.9)
        self.assertEqual(self.coalescer.due(), [])

        # 3. 满 2.0s（推进至 102.1s，停顿 2.1s），due 正常冲刷出队
        self.clock.advance(0.2)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)
        item = ready[0]

        self.assertEqual(item.message_id, "m1")
        self.assertEqual(item.text, "你好，在吗？")
        self.assertEqual(item.channel, "weixin")
        self.assertEqual(item.account_id, "acc1")
        self.assertEqual(item.sender_id, "user_alice")
        self.assertEqual(item.chat_id, "chat_1")
        self.assertEqual(item.chat_type, "single")
        self.assertEqual(item.context_token, "ctx_token_1")
        self.assertEqual(item.merged_ids, ["m1"])
        self.assertEqual(item.media, [])
        self.assertFalse(item.is_command)
        self.assertEqual(len(self.coalescer), 0)

    def test_multiple_text_burst_merged_into_one(self) -> None:
        """用例 2：多条文本快速合并测试，重置静默窗口并在到期后平滑换行拼接。"""
        # t=100.0: 发送 "第1条"
        self.clock.set(100.0)
        self.assertEqual(self.coalescer.feed(_msg("m1", text="第一行内容")), [])

        # t=100.8: 发送 "第2条"（停顿 0.8s < 2.0s，静默计时器重置）
        self.clock.set(100.8)
        self.assertEqual(self.coalescer.feed(_msg("m2", text="第二行补充说明")), [])

        # t=101.5: 发送 "第3条"（停顿 0.7s < 2.0s，静默计时器再次重置）
        self.clock.set(101.5)
        self.assertEqual(self.coalescer.feed(_msg("m3", text="第三行结束语")), [])

        # t=102.5: 距离最后一条消息仅过去 1.0s，未达 2.0s，不应冲刷
        self.clock.set(102.5)
        self.assertEqual(self.coalescer.due(), [])

        # t=103.6: 距离最后一条已过去 2.1s（> 2.0s），触发冲刷
        self.clock.set(103.6)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)

        merged = ready[0]
        self.assertEqual(merged.message_id, "m1")  # 首条 ID 作为主 ID
        self.assertEqual(merged.text, "第一行内容\n第二行补充说明\n第三行结束语")
        self.assertEqual(merged.merged_ids, ["m1", "m2", "m3"])
        self.assertEqual(len(self.coalescer), 0)

    def test_text_and_media_burst_merged(self) -> None:
        """用例 3：图文混发合并测试，提示文本与附件数组完整合并。"""
        self.clock.set(200.0)
        # 先发提示文本
        self.assertEqual(self.coalescer.feed(_msg("m1", text="请帮我查看以下报错截图")), [])

        # 紧接着发送两张图片
        self.clock.advance(0.5)
        self.assertEqual(self.coalescer.feed(_msg("m2", text="", media=[{"kind": "image", "url": "http://img/1.png"}])), [])

        self.clock.advance(0.6)
        self.assertEqual(self.coalescer.feed(_msg("m3", text="", media=[{"kind": "image", "url": "http://img/2.png"}])), [])

        # 等待静默窗口结束
        self.clock.advance(2.1)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)

        merged = ready[0]
        self.assertEqual(merged.message_id, "m1")
        self.assertEqual(merged.text, "请帮我查看以下报错截图")
        self.assertEqual(len(merged.media), 2)
        self.assertEqual(merged.media[0]["url"], "http://img/1.png")
        self.assertEqual(merged.media[1]["url"], "http://img/2.png")
        self.assertEqual(merged.merged_ids, ["m1", "m2", "m3"])

    def test_max_wait_cap_forces_flush_during_continuous_stream(self) -> None:
        """用例 4：最大等待时间硬上限保护，即使高频连发无停顿，达 6.0s 硬上限时强制出队。"""
        self.clock.set(300.0)
        flushed_mid_stream: list[CoalescedInbound] = []

        # 每 0.5s 发送一条消息，持续 20 条（跨越 10 秒），静默窗口（2.0s）从未自然触发
        for i in range(20):
            current_t = 300.0 + i * 0.5
            self.clock.set(current_t)
            # feed 消息
            self.coalescer.feed(_msg(f"m_{i}", text=f"片段_{i}"))
            # 轮询 due
            due_items = self.coalescer.due()
            if due_items:
                flushed_mid_stream.extend(due_items)

        # 验证在连续发送中途由于达到 6.0s 上限至少触发了一次强制冲刷
        self.assertTrue(len(flushed_mid_stream) >= 1, "持续发送过程中必须因达到 max_wait_secs 强制冲刷")
        first_batch = flushed_mid_stream[0]
        # 第一批消息应涵盖从开始到达到 6.0s 上限时的连续片段
        self.assertEqual(first_batch.message_id, "m_0")
        self.assertTrue(len(first_batch.merged_ids) >= 12)  # 6.0s / 0.5s = 12 条

        # 随后流结束，等待静默超时，剩余消息也应完整合并出队
        self.clock.advance(2.5)
        remaining = self.coalescer.due()
        all_flushed = flushed_mid_stream + remaining
        all_merged_ids = [mid for batch in all_flushed for mid in batch.merged_ids]

        # 验证 20 条消息全部完整输出，无遗漏、无乱序
        self.assertEqual(all_merged_ids, [f"m_{i}" for i in range(20)])
        self.assertEqual(len(self.coalescer), 0)

    def test_command_barrier_flushes_pending_and_executes_immediately(self) -> None:
        """用例 5：命令屏障测试，收到 / 开头指令立即冲刷已挂起消息，命令独立优先返回。"""
        self.clock.set(400.0)
        self.assertEqual(self.coalescer.feed(_msg("m1", text="请帮我计算下")), [])
        self.clock.advance(0.5)
        self.assertEqual(self.coalescer.feed(_msg("m2", text="算了先不要了")), [])

        # 此时收到 /stop 命令屏障
        self.clock.advance(0.2)
        cmd_msg = _msg("m3", text="/stop")
        result = self.coalescer.feed(cmd_msg)

        # 必须立刻返回两个就绪项：
        # 第 1 项：被冲刷的前序合并文本
        # 第 2 项：独立执行的控制指令
        self.assertEqual(len(result), 2)
        flushed_text = result[0]
        command_item = result[1]

        self.assertEqual(flushed_text.message_id, "m1")
        self.assertEqual(flushed_text.text, "请帮我计算下\n算了先不要了")
        self.assertEqual(flushed_text.merged_ids, ["m1", "m2"])
        self.assertFalse(flushed_text.is_command)

        self.assertEqual(command_item.message_id, "m3")
        self.assertEqual(command_item.text, "/stop")
        self.assertEqual(command_item.merged_ids, ["m3"])
        self.assertTrue(command_item.is_command)

        # 缓冲区已被清空，后续 due 无产出
        self.assertEqual(len(self.coalescer), 0)
        self.clock.advance(3.0)
        self.assertEqual(self.coalescer.due(), [])

    def test_command_barrier_without_pending_messages(self) -> None:
        """用例 5 补充：在没有任何挂起消息时收到命令，仅单项零等待立即返回。"""
        self.clock.set(450.0)
        cmd_msg = _msg("m_new", text="/new")
        result = self.coalescer.feed(cmd_msg)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].message_id, "m_new")
        self.assertEqual(result[0].text, "/new")
        self.assertTrue(result[0].is_command)
        self.assertEqual(len(self.coalescer), 0)

    def test_multi_session_concurrency_isolation(self) -> None:
        """用例 6：多会话/多联系人并发隔离测试，不同会话独立计时、独立冲刷。"""
        self.clock.set(500.0)
        # 会话 A: Alice 在群 1
        msg_a1 = _msg("a1", text="Alice 说第一句", sender_id="alice", chat_id="group_1", chat_type="group")
        # 会话 B: Bob 在群 1
        msg_b1 = _msg("b1", text="Bob 插了一句", sender_id="bob", chat_id="group_1", chat_type="group")
        # 会话 C: Charlie 私聊
        msg_c1 = _msg("c1", text="Charlie 私聊消息", sender_id="charlie", chat_id="charlie", chat_type="single")

        self.assertEqual(self.coalescer.feed(msg_a1), [])
        self.assertEqual(self.coalescer.feed(msg_b1), [])
        self.assertEqual(self.coalescer.feed(msg_c1), [])
        self.assertEqual(len(self.coalescer), 3)

        # 1.0s 后，Alice 连发第二句
        self.clock.advance(1.0)
        msg_a2 = _msg("a2", text="Alice 补充一句", sender_id="alice", chat_id="group_1", chat_type="group")
        self.assertEqual(self.coalescer.feed(msg_a2), [])

        # 再过 1.2s（总共 2.2s），Bob 和 Charlie 已静默超过 2.0s，但 Alice 距离最后一条仅 1.2s
        self.clock.advance(1.2)
        due_items = self.coalescer.due()
        self.assertEqual(len(due_items), 2)
        senders = {item.sender_id for item in due_items}
        self.assertEqual(senders, {"bob", "charlie"})

        # 此时 Alice 依然在缓冲区中
        self.assertEqual(len(self.coalescer), 1)

        # Alice 再次静默 1.0s（距第二句满 2.2s），正常出队
        self.clock.advance(1.0)
        alice_ready = self.coalescer.due()
        self.assertEqual(len(alice_ready), 1)
        self.assertEqual(alice_ready[0].sender_id, "alice")
        self.assertEqual(alice_ready[0].text, "Alice 说第一句\nAlice 补充一句")
        self.assertEqual(alice_ready[0].merged_ids, ["a1", "a2"])
        self.assertEqual(len(self.coalescer), 0)

    def test_media_only_placeholder_text_and_empty_handling(self) -> None:
        """用例 7：空消息/仅附件边界测试，纯附件设置占位文本 [附件] 并合并列表。"""
        self.clock.set(600.0)
        # 两条纯图片无文字消息
        img1 = _msg("img1", text="", media=[{"kind": "image", "file_id": "f1"}])
        img2 = _msg("img2", text="   ", media=[{"kind": "image", "file_id": "f2"}])

        self.assertEqual(self.coalescer.feed(img1), [])
        self.clock.advance(0.5)
        self.assertEqual(self.coalescer.feed(img2), [])

        self.clock.advance(2.1)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)

        merged = ready[0]
        self.assertEqual(merged.message_id, "img1")
        self.assertEqual(merged.text, "[附件]")
        self.assertEqual(len(merged.media), 2)
        self.assertEqual(merged.media[0]["file_id"], "f1")
        self.assertEqual(merged.media[1]["file_id"], "f2")
        self.assertEqual(merged.merged_ids, ["img1", "img2"])

    def test_flush_all_for_graceful_shutdown(self) -> None:
        """用例 9：优雅停机清空测试，flush_all 即时冲刷所有会话。"""
        self.clock.set(700.0)
        self.coalescer.feed(_msg("s1_m1", text="会话1", chat_id="chat_1"))
        self.coalescer.feed(_msg("s2_m1", text="会话2", chat_id="chat_2"))
        self.assertEqual(len(self.coalescer), 2)

        flushed = self.coalescer.flush_all()
        self.assertEqual(len(flushed), 2)
        self.assertEqual(len(self.coalescer), 0)
        self.assertEqual(self.coalescer.pending_count(), 0)


    def test_duplicate_message_id_deduplicated_in_buffer(self) -> None:
        """用例 10：重复消息去重测试，网络抖动重放同一 message_id 不堆叠文本与附件。"""
        self.clock.set(800.0)
        msg1 = _msg("dup_1", text="这是一条测试消息", media=[{"kind": "image", "url": "http://img/1.png"}])
        self.assertEqual(self.coalescer.feed(msg1), [])
        self.assertEqual(self.coalescer.pending_count(), 1)

        # 0.3s 后同一条消息重发
        self.clock.advance(0.3)
        self.assertEqual(self.coalescer.feed(msg1), [])
        # 挂起数量依然为 1，未被重复添加
        self.assertEqual(self.coalescer.pending_count(), 1)

        # 推进到静默窗口结束
        self.clock.advance(2.1)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)
        item = ready[0]
        self.assertEqual(item.text, "这是一条测试消息")
        self.assertEqual(len(item.media), 1)
        self.assertEqual(item.merged_ids, ["dup_1"])

    def test_text_stripping_newlines_in_merge(self) -> None:
        """用例 11：换行与空白修剪测试，防止连发自带换行符导致合并后产生多余空行。"""
        self.clock.set(900.0)
        self.coalescer.feed(_msg("m1", text="第一行内容\n"))
        self.clock.advance(0.5)
        self.coalescer.feed(_msg("m2", text="第二行内容\r\n"))
        self.clock.advance(0.5)
        self.coalescer.feed(_msg("m3", text="第三行内容"))

        self.clock.advance(2.1)
        ready = self.coalescer.due()
        self.assertEqual(len(ready), 1)
        self.assertEqual(ready[0].text, "第一行内容\n第二行内容\n第三行内容")

    def test_from_dict_defaults_and_resilience(self) -> None:
        """用例 12：from_dict 边界与容错测试，验证私聊 chat_id 默认、时间戳别名与单对象媒体兼容。"""
        # 1. 私聊缺失 chat_id，默认指向 sender_id
        item1 = InboundMessageItem.from_dict({
            "channel": "weixin",
            "account_id": "acc1",
            "msgid": "m1",
            "from_user_id": "user_123",
            "chat_type": "single",
            "text": "测试私聊",
            "ts": 1700000000,
            "media": {"kind": "image", "url": "http://img.png"},
        })
        self.assertEqual(item1.chat_id, "user_123")
        self.assertEqual(item1.message_id, "m1")
        self.assertEqual(item1.sender_id, "user_123")
        self.assertEqual(item1.received_at, 1700000000.0)
        self.assertEqual(len(item1.media), 1)

        # 2. 非法时间戳字符串不导致异常
        item2 = InboundMessageItem.from_dict({
            "received_at": "invalid_timestamp",
        })
        self.assertIsNone(item2.received_at)

    def test_flush_session_by_key_and_kwargs(self) -> None:
        """用例 13：flush_session 单会话精确冲刷测试（支持元组键与命名参数）。"""
        self.clock.set(1000.0)
        self.coalescer.feed(_msg("m1", text="会话1消息", chat_id="c1", sender_id="u1"))
        self.coalescer.feed(_msg("m2", text="会话2消息", chat_id="c2", sender_id="u2"))
        self.assertEqual(len(self.coalescer), 2)

        # 冲刷会话 1（使用命名参数）
        s1 = self.coalescer.flush_session(channel="weixin", account_id="acc1", chat_id="c1", sender_id="u1")
        self.assertIsNotNone(s1)
        self.assertEqual(s1.text, "会话1消息")
        self.assertEqual(len(self.coalescer), 1)

        # 冲刷会话 2（使用元组键）
        key2 = ("weixin", "acc1", "c2", "u2")
        s2 = self.coalescer.flush_session(key2)
        self.assertIsNotNone(s2)
        self.assertEqual(s2.text, "会话2消息")
        self.assertEqual(len(self.coalescer), 0)

    def test_session_stats_and_pending_sessions(self) -> None:
        """用例 14：可观测性接口测试，pending_sessions 与 session_stats 输出准确数据。"""
        self.clock.set(1100.0)
        self.coalescer.feed(_msg("m1", text="测试", chat_id="c1"))
        self.clock.advance(1.0)

        sessions = self.coalescer.pending_sessions()
        self.assertEqual(len(sessions), 1)
        key = sessions[0]

        stats = self.coalescer.session_stats()
        self.assertIn(key, stats)
        self.assertEqual(stats[key]["count"], 1)
        self.assertEqual(stats[key]["started_at"], 1100.0)
        self.assertEqual(stats[key]["last_at"], 1100.0)
        self.assertAlmostEqual(stats[key]["idle_for"], 1.0)
        self.assertAlmostEqual(stats[key]["waited"], 1.0)

    def test_high_concurrency_scaling(self) -> None:
        """用例 15：千级并发性能测试，1000 个独立会话在 O(1) 字典管理下毫秒级批量冲刷。"""
        self.clock.set(2000.0)
        for i in range(1000):
            msg = _msg(f"msg_{i}", text=f"内容_{i}", chat_id=f"chat_{i}", sender_id=f"user_{i}")
            self.coalescer.feed(msg)

        self.assertEqual(len(self.coalescer), 1000)
        self.assertEqual(self.coalescer.pending_count(), 1000)

        # 推进时钟至超时
        self.clock.advance(2.5)
        start_t = time.perf_counter()
        ready = self.coalescer.due()
        duration = time.perf_counter() - start_t

        self.assertEqual(len(ready), 1000)
        self.assertEqual(len(self.coalescer), 0)
        # 1000 个会话的冲刷应在 50ms 内完成
        self.assertTrue(duration < 0.05, f"1000 会话冲刷耗时过长: {duration:.4f}s")


class GatewayCoalescerIntegrationTests(unittest.TestCase):
    """网关适配层与权威数据库路由表集成测试。"""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.database = self.root / "core.sqlite3"

        with Store(self.database):
            pass

        self.modes_file = self.root / "modes.json"
        self.modes_file.write_text(json.dumps({
            "channels": {
                "weixin": {
                    "mode": "core",
                    "database": str(self.database),
                    "account_id": "weixin-account",
                    "allowed_senders": ["alice", "admin"],
                    "administrators": ["admin"],
                }
            }
        }))

        env_patcher = patch.dict("os.environ", {"MUSE_CHANNEL_MODES": str(self.modes_file)})
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        self.binding = GatewayBinding("weixin")
        self.addCleanup(self.binding.close)

    def test_gateway_receive_with_merged_ids_persists_in_routes_and_externals(self) -> None:
        """用例 8：验证网关 receive 记录 merged_ids 并完整写入路由及外部索引表。"""
        # 模拟合并器产物
        coalesced = CoalescedInbound(
            channel="weixin",
            account_id="weixin-account",
            message_id="ext_m1",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="第一段\n第二段",
            context_token="tok_123",
            media=[],
            merged_ids=["ext_m1", "ext_m2", "ext_m3"],
            received_at=1700000001.5,
        )

        item = self.binding.receive(coalesced)
        self.assertTrue(item.created)

        # 检查数据库权威记录
        conn = sqlite3.connect(self.database)
        try:
            conn.row_factory = sqlite3.Row

            # 1. 验证 tasks 表
            tasks = conn.execute("SELECT * FROM tasks").fetchall()
            self.assertEqual(len(tasks), 1)
            task_id = tasks[0]["id"]
            self.assertEqual(task_id, item.task_id)

            # 2. 验证 gateway_routes 表包含 merged_ids_json
            route = conn.execute("SELECT * FROM gateway_routes WHERE task_id=?", (task_id,)).fetchone()
            self.assertIsNotNone(route)
            self.assertEqual(route["external_id"], "ext_m1")
            self.assertEqual(route["context_token"], "tok_123")
            merged_ids = json.loads(route["merged_ids_json"])
            self.assertEqual(merged_ids, ["ext_m1", "ext_m2", "ext_m3"])

            # 3. 验证 gateway_route_externals 索引表已登记全部 3 个外部 ID
            external_rows = conn.execute(
                "SELECT external_id FROM gateway_route_externals WHERE task_id=? ORDER BY external_id",
                (task_id,)
            ).fetchall()
            indexed_externals = sorted([r["external_id"] for r in external_rows])
            self.assertEqual(indexed_externals, ["ext_m1", "ext_m2", "ext_m3"])

            # 4. 验证 inbound_messages 表中保留了首条消息的 received_at
            msg_row = conn.execute("SELECT received_at FROM inbound_messages WHERE id=(SELECT message_id FROM tasks WHERE id=?)", (task_id,)).fetchone()
            self.assertEqual(msg_row["received_at"], 1700000001.5)
        finally:
            conn.close()

    def test_find_task_by_external_id(self) -> None:
        """用例 16：根据被合并的任意外部 ID 反查权威 task_id。"""
        coalesced = CoalescedInbound(
            channel="weixin",
            account_id="weixin-account",
            message_id="prim_100",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="主消息文本",
            merged_ids=["prim_100", "sec_101", "sec_102"],
        )
        item = self.binding.receive(coalesced)
        self.assertTrue(item.created)

        # 通过主 ID 反查
        self.assertEqual(self.binding.find_task_by_external_id("prim_100"), item.task_id)
        # 通过被合并的次级 ID 反查
        self.assertEqual(self.binding.find_task_by_external_id("sec_101"), item.task_id)
        self.assertEqual(self.binding.find_task_by_external_id("sec_102"), item.task_id)
        # 查询不存在的 ID 返回 None
        self.assertIsNone(self.binding.find_task_by_external_id("non_existent_id"))

    def test_secondary_merged_message_replay_deduplication(self) -> None:
        """用例 17：被合并的次级消息遭遇网络重放时，网关直接拦截排重，绝不重复创建 Task。"""
        coalesced = CoalescedInbound(
            channel="weixin",
            account_id="weixin-account",
            message_id="msg_primary",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="合并后的第一句和第二句",
            merged_ids=["msg_primary", "msg_secondary"],
        )
        first_item = self.binding.receive(coalesced)
        self.assertTrue(first_item.created)
        primary_task_id = first_item.task_id

        # 模拟稍后通道因网络超时重试推送了次级消息 msg_secondary
        replay_item = self.binding.receive(
            message_id="msg_secondary",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="第二句单独重发",
        )
        # 断言：已排重，没有创建新 Task，且返回的 task_id 指向原主任务
        self.assertFalse(replay_item.created)
        self.assertEqual(replay_item.task_id, primary_task_id)

        # 检查数据库总任务数依然为 1
        conn = sqlite3.connect(self.database)
        try:
            tasks = conn.execute("SELECT * FROM tasks").fetchall()
            self.assertEqual(len(tasks), 1)
        finally:
            conn.close()

    def test_batch_with_secondary_id_already_indexed_deduplicates(self) -> None:
        """用例 18：若新到达的批次中包含已被合并索引的次级消息 ID，整体批次被幂等排重。"""
        coalesced = CoalescedInbound(
            channel="weixin",
            account_id="weixin-account",
            message_id="batch1_prim",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="批次1",
            merged_ids=["batch1_prim", "batch1_sec"],
        )
        first_item = self.binding.receive(coalesced)
        self.assertTrue(first_item.created)

        # 批次2：主 ID 是新的 batch2_new，但 merged_ids 里包含已被索引的 batch1_sec
        batch2 = CoalescedInbound(
            channel="weixin",
            account_id="weixin-account",
            message_id="batch2_new",
            sender_id="alice",
            chat_id="alice",
            chat_type="single",
            text="批次2重叠",
            merged_ids=["batch2_new", "batch1_sec"],
        )
        second_item = self.binding.receive(batch2)
        self.assertFalse(second_item.created)
        self.assertEqual(second_item.task_id, first_item.task_id)

    def test_gateway_create_coalescer_factory(self) -> None:
        """验证网关实例上的 create_coalescer 工厂方法。"""
        coalescer = self.binding.create_coalescer(idle_gap_secs=1.5, max_wait_secs=5.0)
        self.assertEqual(coalescer.idle_gap_secs, 1.5)
        self.assertEqual(coalescer.max_wait_secs, 5.0)


if __name__ == "__main__":
    unittest.main()

