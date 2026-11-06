"""并发一致性与事务所有权围栏专项对抗性测试 (Concurrency & Fencing Tests)。

覆盖要点：
1. InboundCoalescer 在多线程并发 feed、due、flush 时的线程安全与数据无丢失；
2. CircuitBreaker 在多线程并发 record_failure 与 can_execute 时的状态与计数原子性；
3. DualTrackExecutor 在并发提交下的所有权围栏与降级轨隔离；
4. 审批决断文件与游标在排他锁下的并发消费原子性与零重复决断。
"""
from __future__ import annotations

import concurrent.futures
import itertools
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from mobile_channels.coalesce import InboundCoalescer, InboundMessageItem
from mobile_channels.executors import CircuitBreaker, DualTrackExecutor, GadgetExecutor, MuseExecutor, ensure_executor_schema
from mobile_channels.gadget_client import GadgetClient
from mobile_channels.ingress import CoreIngress
from mobile_channels.models import Claim
from mobile_channels.runtime import ExecutionContext, ExecutionOutcome
from mobile_channels.storage import Store


class TestInboundCoalescerConcurrency(unittest.TestCase):
    """InboundCoalescer 线程并发安全性测试。"""

    def test_multithreaded_feed_and_due_no_race_or_data_loss(self):
        """多线程高并发 feed 与 due 并发交替执行，验证无锁死、无字典迭代破坏、数据完整。"""
        coalescer = InboundCoalescer(idle_gap_secs=0.05, max_wait_secs=0.2)
        total_messages_per_thread = 50
        num_threads = 8
        received_items: list = []
        receive_lock = threading.Lock()
        stop_poller = threading.Event()

        def poller():
            while not stop_poller.is_set():
                ready = coalescer.due()
                if ready:
                    with receive_lock:
                        received_items.extend(ready)
                time.sleep(0.01)

        poller_thread = threading.Thread(target=poller, daemon=True)
        poller_thread.start()

        def worker(thread_idx: int):
            for i in range(total_messages_per_thread):
                msg = InboundMessageItem(
                    channel="feishu",
                    account_id="bot1",
                    message_id=f"msg-{thread_idx}-{i}",
                    sender_id=f"user_{thread_idx}",
                    chat_id=f"chat_{thread_idx}",
                    text=f"hello from thread {thread_idx} msg {i}",
                )
                flushed = coalescer.feed(msg)
                if flushed:
                    with receive_lock:
                        received_items.extend(flushed)
                time.sleep(0.001)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 等待静默窗口到期冲刷完毕
        time.sleep(0.3)
        stop_poller.set()
        poller_thread.join(timeout=1.0)

        # 冲刷剩余所有
        remaining = coalescer.flush_all()
        with receive_lock:
            received_items.extend(remaining)

        # 统计所有被合并收集的消息 ID
        collected_msg_ids = set()
        for item in received_items:
            for mid in item.merged_ids:
                collected_msg_ids.add(mid)

        expected_total = num_threads * total_messages_per_thread
        self.assertEqual(len(collected_msg_ids), expected_total,
                         f"所有线程发送的消息 ID 应全部被完整合并收集，无任何丢失（期望 {expected_total}，实际 {len(collected_msg_ids)}）")


class TestCircuitBreakerConcurrency(unittest.TestCase):
    """CircuitBreaker 线程并发安全性测试。"""

    def test_multithreaded_failures_atomic_trip(self):
        """多个线程同时记录失败，失败计数原子递增，熔断器准确开启。"""
        cb = CircuitBreaker(failure_threshold=20, cooldown_seconds=60.0)

        def report_failure():
            cb.record_failure()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(report_failure) for _ in range(50)]
            concurrent.futures.wait(futures)

        self.assertEqual(cb.failure_count, 50)
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

    def test_multithreaded_success_resets_breaker(self):
        """多线程并发重置熔断器，状态原子闭合且计数清零。"""
        cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=60.0)
        cb.failure_count = 10
        cb._state = "OPEN"

        def report_success():
            cb.record_success()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(report_success) for _ in range(20)]
            concurrent.futures.wait(futures)

        self.assertEqual(cb.failure_count, 0)
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())


class TestDualTrackFencingConcurrency(unittest.TestCase):
    """DualTrackExecutor 并发提交与所有权围栏测试。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_concurrency.db"
        self.store = Store(str(self.db_path))
        ensure_executor_schema(self.store)

    def tearDown(self):
        self.store.close()
        self.temp_dir.cleanup()

    def test_concurrent_submits_strictly_fenced(self):
        """多线程并发提交任务，不同会话与任务的所有权和 track 严格隔离。"""
        # 创建多个任务
        tasks_count = 6
        claims = []
        for i in range(tasks_count):
            ingress = CoreIngress(self.store, "feishu", "bot", {f"user_{i}"})
            ingress.receive(f"msg_{i}", f"user_{i}", f"chat_{i}", "single", f"prompt {i}")
            claim = self.store.claim(f"worker_{i}", concurrency=10, lease_seconds=60)
            self.assertIsNotNone(claim)
            self.store.begin_submission(claim)
            claims.append(claim)

        # Mock GadgetClient: start_turn hands back a live worker per attempt.
        mock_gadget_client = MagicMock(spec=GadgetClient)
        mock_gadget_client.is_configured.return_value = True
        pid_counter = itertools.count(2000)

        def _fake_start(attempt_id, session_id, text, timeout_seconds=None):
            return MagicMock(pid=next(pid_counter))

        mock_gadget_client.start_turn.side_effect = _fake_start

        primary = GadgetExecutor(self.store, client=mock_gadget_client)
        fallback = MagicMock(spec=MuseExecutor)
        dual = DualTrackExecutor(self.store, primary=primary, fallback=fallback)

        def submit_task(claim: Claim):
            thread_store = Store(str(self.db_path))
            try:
                primary = GadgetExecutor(thread_store, client=mock_gadget_client)
                fallback = MagicMock(spec=MuseExecutor)
                dual = DualTrackExecutor(thread_store, primary=primary, fallback=fallback)
                ctx = ExecutionContext(
                    claim=claim,
                    text=f"prompt for {claim.task_id}",
                    external_message_id=f"ext_{claim.task_id}",
                    sender_id="user_0",
                    channel="feishu",
                    account_id="bot",
                    chat_type="single",
                    chat_id="chat_0",
                )
                return dual.submit(ctx)
            finally:
                thread_store.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(submit_task, claims))

        # 验证全部成功以 gadget 轨提交
        for outcome in results:
            self.assertEqual(outcome.state, "accepted")
            self.assertTrue(outcome.run_id.startswith("worker:"))

        # 检查数据库持久化的 track
        for claim in claims:
            track = dual.current_track(claim.attempt_id)
            self.assertEqual(track, "gadget")


from unittest.mock import Mock, patch
from tests.test_approval_security import load_relay


class TestApprovalDecisionsConcurrency(unittest.TestCase):
    """审批决断并发消费与容错测试。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.base = Path(self.temp_dir.name)
        self.relay = load_relay()
        self.relay.STATE_F = str(self.base / "state.json")
        self.relay.DECISIONS_F = str(self.base / "decisions.jsonl")
        self.relay.OFFSET_F = str(self.base / "decisions.offset")
        self.relay.notify = Mock()
        self.state = {
            "next_num": 2,
            "items": {
                "1": {
                    "approval_id": "aid_concurrent_1",
                    "status": "pending",
                    "expires_ms": 9999999999999,
                    "decision_scope": {
                        "channel": "feishu",
                        "chat_id": "chat_1",
                        "operator_ids": ["user_alice"],
                    },
                }
            },
        }
        self.relay.save_state(self.state)

    def test_missing_fields_in_decision_row_does_not_raise_key_error(self):
        """测试决断行缺少 operator_id 或 chat_id 时安全捕获并拒绝，不抛 KeyError。"""
        # 写入一条残缺字段的决断
        malformed_row = {
            "num": "1",
            "decision": "allow_once",
            "channel": "feishu",
            # 缺失 operator_id, chat_id
        }
        decisions_file = self.base / "decisions.jsonl"
        decisions_file.write_text(json.dumps(malformed_row) + "\n", encoding="utf-8")

        mock_gw = Mock()
        self.relay.consume_decisions(mock_gw, self.state)
        mock_gw.call_json.assert_not_called()

        # 验证 offset 正常安全推进（未卡死）
        offset = int((self.base / "decisions.offset").read_text(encoding="utf-8").strip())
        self.assertGreater(offset, 0)

    def test_concurrent_consume_decisions_single_execution(self):
        """多个线程同时调用 consume_decisions，排他锁与状态流转确保平台 RPC 仅被调用一次。"""
        valid_row = {
            "num": "1",
            "decision": "allow_once",
            "channel": "feishu",
            "chat_id": "chat_1",
            "operator_id": "user_alice",
            "ts": time.time(),
        }
        decisions_file = self.base / "decisions.jsonl"
        decisions_file.write_text(json.dumps(valid_row) + "\n", encoding="utf-8")

        call_counter = 0
        call_lock = threading.Lock()

        def mock_call_json(*args, **kwargs):
            nonlocal call_counter
            with call_lock:
                call_counter += 1
            time.sleep(0.02)
            return {"status": "ok"}

        mock_gw = Mock()
        mock_gw.call_json.side_effect = mock_call_json

        def run_consume():
            # 每个线程独立加载 state 模拟并发工作流
            local_state = self.relay.load_state()
            self.relay.consume_decisions(mock_gw, local_state)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(run_consume) for _ in range(5)]
            concurrent.futures.wait(futures)

        # 平台 RPC 必须有且仅有 1 次调用
        self.assertEqual(call_counter, 1, f"并发消费下平台 RPC 必须被且仅被调用一次，实际调用了 {call_counter} 次")

        # 持久化状态已更新为 decided:allow_once
        final_state = self.relay.load_state()
        self.assertEqual(final_state["items"]["1"]["status"], "decided:allow_once")


if __name__ == "__main__":
    unittest.main()

