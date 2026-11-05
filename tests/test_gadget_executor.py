"""官方 Gadget 双轨执行器与客户端完备自动化测试套件。

覆盖：
- 用例 1：GadgetCredentials 凭证加载与环境变量覆盖；
- 用例 2：GadgetClient 侧会话创建与隔离验证（多会话不同 side_session_id）；
- 用例 3：GadgetExecutor 完整的 submit/query/cancel 生命周期；
- 用例 4：CircuitBreaker 熔断器状态流转（成功重置、连续失败熔断、冷却后半开恢复）；
- 用例 5：DualTrackExecutor 默认走 Gadget 轨；
- 用例 6：DualTrackExecutor 凭证缺失时透明降级至 Native 轨；
- 用例 7：DualTrackExecutor 发生上游异常时触发熔断降级；
- 用例 8：已在 Gadget 轨提交的任务，即使后续备轨被激活，query 依然严格绑定 Gadget 轨（防止跨轨串话）；
- 用例 9：与 CoreRunner 的端到端集成测试（验证通过 runner.tick() 驱动从 claimed -> running -> succeeded 的全流程）；
- 补充用例：数据库模式向前兼容性升级验证（无 track 列迁移）。
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from mobile_channels.executors import (
    CircuitBreaker,
    DualTrackExecutor,
    GadgetExecutor,
    MuseExecutor,
    ensure_executor_schema,
)
from mobile_channels.gadget_client import (
    GadgetAuthError,
    GadgetCredentials,
    GadgetError,
    GadgetClient,
    GadgetNetworkError,
)
from mobile_channels.ingress import CoreIngress
from mobile_channels.models import Claim
from mobile_channels.runtime import CoreRunner, ExecutionContext, ExecutionOutcome
from mobile_channels.storage import Store


class TestGadgetCredentials(unittest.TestCase):
    """用例 1：GadgetCredentials 凭证加载与环境变量覆盖。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.config_path = Path(self.temp_dir.name) / "gadget.json"

    def test_default_unconfigured(self):
        """当文件不存在且环境变量为空时，凭证未就绪。"""
        creds = GadgetCredentials.from_env_or_file(self.config_path, env={})
        self.assertFalse(creds.is_configured())
        self.assertIsNone(creds.device_id)
        self.assertIsNone(creds.token)

    def test_load_from_json_file(self):
        """能够正确从本地 JSON 配置文件加载凭证。"""
        data = {
            "device_id": "device_1001",
            "token": "tok_secret_abc",
            "endpoint": "https://custom.gadget.internal",
        }
        self.config_path.write_text(json.dumps(data), encoding="utf-8")
        creds = GadgetCredentials.from_env_or_file(self.config_path, env={})
        self.assertTrue(creds.is_configured())
        self.assertEqual(creds.device_id, "device_1001")
        self.assertEqual(creds.token, "tok_secret_abc")
        self.assertEqual(creds.endpoint, "https://custom.gadget.internal")

    def test_env_overrides_file(self):
        """环境变量能够优先覆盖配置文件中的内容。"""
        data = {
            "device_id": "device_file",
            "token": "tok_file",
            "endpoint": "https://file.gadget.internal",
        }
        self.config_path.write_text(json.dumps(data), encoding="utf-8")
        env = {
            "MUSE_GADGET_DEVICE_ID": "device_env",
            "MUSE_GADGET_TOKEN": "tok_env",
            "MUSE_GADGET_ENDPOINT": "https://env.gadget.internal",
        }
        creds = GadgetCredentials.from_env_or_file(self.config_path, env=env)
        self.assertTrue(creds.is_configured())
        self.assertEqual(creds.device_id, "device_env")
        self.assertEqual(creds.token, "tok_env")
        self.assertEqual(creds.endpoint, "https://env.gadget.internal")

    def test_is_configured_boundary_conditions(self):
        """测试不同缺失字段组合下 is_configured 的边界条件。"""
        self.assertFalse(GadgetCredentials(device_id=None, token="tok").is_configured())
        self.assertFalse(GadgetCredentials(device_id="dev", token=None).is_configured())
        self.assertFalse(GadgetCredentials(device_id="   ", token="tok").is_configured())
        self.assertFalse(GadgetCredentials(device_id="dev", token="  ").is_configured())
        self.assertTrue(GadgetCredentials(device_id="dev", token="tok").is_configured())


class TestGadgetClient(unittest.TestCase):
    """用例 2：GadgetClient 侧会话创建与隔离验证（多会话不同 side_session_id）。"""

    def setUp(self):
        self.creds = GadgetCredentials(device_id="dev_test", token="tok_test")

    def test_side_session_creation_and_isolation(self):
        """验证侧会话的专属格式与多会话、多年级之间的上下文隔离。"""
        client = GadgetClient(credentials=self.creds)
        # 同一会话多次获取，返回一致
        s1_a = client.create_or_get_side_session("conv_1", 0)
        s1_b = client.create_or_get_side_session("conv_1", 0)
        self.assertEqual(s1_a, s1_b)
        self.assertEqual(s1_a, "gadget:side:conv_1:0")

        # 不同会话，返回不同 ID
        s2 = client.create_or_get_side_session("conv_2", 0)
        self.assertEqual(s2, "gadget:side:conv_2:0")
        self.assertNotEqual(s1_a, s2)

        # 相同会话，不同 generation，返回不同 ID
        s1_gen1 = client.create_or_get_side_session("conv_1", 1)
        self.assertEqual(s1_gen1, "gadget:side:conv_1:1")
        self.assertNotEqual(s1_a, s1_gen1)

    def test_client_transport_mocking(self):
        """验证注入 transport_func 时的发送、轮询与取消方法。"""
        mock_transport = Mock()
        mock_transport.side_effect = lambda action, payload: {
            "send_message": {"run_id": "run_999"},
            "get_run_status": {"status": "completed", "display_text": "hello from gadget"},
            "cancel_run": {"success": True},
        }.get(action, {})

        client = GadgetClient(credentials=self.creds, transport_func=mock_transport)
        sid = client.create_or_get_side_session("c1", 0)
        run_id = client.send_message(sid, "hello")
        self.assertEqual(run_id, "run_999")

        status = client.get_run_status(sid, run_id)
        self.assertEqual(status["status"], "completed")
        self.assertEqual(status["display_text"], "hello from gadget")

        cancelled = client.cancel_run(sid, run_id)
        self.assertTrue(cancelled)

        client.close()
        with self.assertRaises(GadgetError):
            client.send_message(sid, "after close")


class TestGadgetExecutor(unittest.TestCase):
    """用例 3：GadgetExecutor 完整的 submit/query/cancel 生命周期。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "test.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        self.ingest = self.ingress.receive("msg_1", "user_1", "chat_1", "single", "ping")

        self.mock_transport = Mock()
        self.client = GadgetClient(
            credentials=GadgetCredentials("dev1", "tok1"),
            transport_func=self.mock_transport,
        )
        self.executor = GadgetExecutor(self.store, client=self.client)

    def _get_context(self) -> ExecutionContext:
        claim = self.store.claim("worker_1", concurrency=1, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_unconfigured_credentials_returns_not_submitted(self):
        """当未配置有效凭证时，submit 立即返回 not_submitted 且不写入 executor_runs。"""
        unconfigured_client = GadgetClient(credentials=GadgetCredentials())
        executor = GadgetExecutor(self.store, client=unconfigured_client)
        context = self._get_context()
        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "gadget_credentials_missing")
        runs = self.store.connection.execute("SELECT * FROM executor_runs").fetchall()
        self.assertEqual(len(runs), 0)

    def test_full_lifecycle_submit_query_cancel(self):
        """验证 submit -> query(running) -> query(succeeded) 与 cancel 全生命周期。"""
        self.mock_transport.side_effect = lambda action, payload: {
            "send_message": {"run_id": "run_gadget_1"},
            "get_run_status": {"status": "running", "display_text": None},
            "cancel_run": {"success": True},
        }.get(action, {})

        context = self._get_context()
        # 1. 提交任务
        outcome = self.executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "run_gadget_1")

        # 检查数据库记录
        row = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["track"], "gadget")
        self.assertEqual(row["platform_run_id"], "run_gadget_1")
        self.assertEqual(row["status"], "accepted")

        # 2. 轮询 running 状态
        q1 = self.executor.query(context, "run_gadget_1")
        self.assertEqual(q1.state, "running")

        # 3. 轮询 succeeded 状态
        self.mock_transport.side_effect = lambda action, payload: {
            "get_run_status": {"status": "completed", "display_text": "pong from gadget"},
        }.get(action, {})
        q2 = self.executor.query(context, "run_gadget_1")
        self.assertEqual(q2.state, "succeeded")
        self.assertEqual(q2.text, "pong from gadget")

        # 4. 取消
        self.mock_transport.side_effect = lambda action, payload: {
            "cancel_run": {"success": True},
            "get_run_status": {"status": "cancelled"},
        }.get(action, {})
        c = self.executor.cancel(context, "run_gadget_1")
        self.assertEqual(c.state, "cancelled")
        self.assertEqual(c.evidence, "platform_run_cancelled")


class TestCircuitBreaker(unittest.TestCase):
    """用例 4：CircuitBreaker 熔断器状态流转（成功重置、连续失败熔断、冷却后半开恢复）。"""

    def setUp(self):
        self.now = 1000.0

    def test_circuit_state_transitions(self):
        cb = CircuitBreaker(failure_threshold=3, cooldown_seconds=30.0, clock=lambda: self.now)

        # 初始状态
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        # 1 次失败
        cb.record_failure()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        # 2 次失败
        cb.record_failure()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        # 成功重置
        cb.record_success()
        self.assertEqual(cb.status, "CLOSED")
        self.assertEqual(cb.failure_count, 0)

        # 连续 3 次失败触发熔断
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        # 冷却未过（+20s）
        self.now += 20.0
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        # 冷却到达（+31s），进入 HALF_OPEN
        self.now += 11.0
        self.assertEqual(cb.status, "HALF_OPEN")
        self.assertTrue(cb.can_execute())

        # 半开状态试探成功 -> 恢复 CLOSED
        cb.record_success()
        self.assertEqual(cb.status, "CLOSED")
        self.assertTrue(cb.can_execute())

        # 再次熔断后，半开状态试探失败 -> 立即重回 OPEN
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.now += 31.0
        self.assertEqual(cb.status, "HALF_OPEN")
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())


class TestDualTrackExecutor(unittest.TestCase):
    """用例 5 ~ 8：DualTrackExecutor 默认路由、降级、熔断与防串话绑定。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "dual.sqlite3"
        self.now = 100.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})

        # Mock primary (Gadget)
        self.primary = Mock(spec=GadgetExecutor)
        self.primary.is_configured.return_value = True

        # Mock fallback (Native)
        self.fallback = Mock(spec=MuseExecutor)

        # Circuit
        self.circuit = CircuitBreaker(failure_threshold=3, cooldown_seconds=30.0, clock=lambda: self.now)

        self.dual_executor = DualTrackExecutor(
            store=self.store,
            primary=self.primary,
            fallback=self.fallback,
            circuit=self.circuit,
        )

    def _create_context(self, msg_id="m1", chat_id=None) -> ExecutionContext:
        chat = chat_id or f"chat_{msg_id}"
        self.ingress.receive(msg_id, "user_1", chat, "single", f"text for {msg_id}")
        claim = self.store.claim("worker_1", concurrency=10, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_use_case_5_default_gadget_track(self):
        """用例 5：DualTrackExecutor 默认走 Gadget 轨。"""
        context = self._create_context("m_primary")
        self.primary.submit.return_value = ExecutionOutcome("accepted", run_id="gadget_run_1")

        # 模拟 GadgetExecutor 成功写入 executor_runs
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs VALUES (?,?,?,?,?,?)",
                (context.claim.attempt_id, "s1", "gadget_run_1", "accepted", self.now, "gadget"),
            )

        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "gadget_run_1")
        self.primary.submit.assert_called_once_with(context)
        self.fallback.submit.assert_not_called()
        self.assertEqual(self.dual_executor.current_track(context.claim.attempt_id), "gadget")

    def test_use_case_6_credentials_missing_fallback_to_native(self):
        """用例 6：DualTrackExecutor 凭证缺失时透明降级至 Native 轨。"""
        self.primary.is_configured.return_value = False
        context = self._create_context("m_unconf")
        self.fallback.submit.return_value = ExecutionOutcome("accepted", run_id="native_run_1")

        # 模拟 fallback 写入 native 记录
        def fallback_submit_side_effect(ctx):
            with self.store.transaction():
                self.store.connection.execute(
                    "INSERT INTO executor_runs VALUES (?,?,?,?,?,?)",
                    (ctx.claim.attempt_id, "s_nat", "native_run_1", "accepted", self.now, "native"),
                )
            return ExecutionOutcome("accepted", run_id="native_run_1")

        self.fallback.submit.side_effect = fallback_submit_side_effect

        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "native_run_1")
        self.primary.submit.assert_not_called()
        self.fallback.submit.assert_called_once_with(context)
        self.assertEqual(self.dual_executor.current_track(context.claim.attempt_id), "native")

    def test_use_case_7_upstream_error_trips_circuit_and_falls_back(self):
        """用例 7：DualTrackExecutor 发生上游异常时触发熔断降级。"""
        # 1. 第一次任务发生上游网络异常
        context1 = self._create_context("m_err1")
        self.primary.submit.side_effect = GadgetNetworkError("Connection refused")

        def fallback_side_effect(ctx):
            with self.store.transaction():
                self.store.connection.execute(
                    "INSERT INTO executor_runs VALUES (?,?,?,?,?,?)",
                    (ctx.claim.attempt_id, "s_nat", f"native_{ctx.claim.attempt_id}", "accepted", self.now, "native"),
                )
            return ExecutionOutcome("accepted", run_id=f"native_{ctx.claim.attempt_id}")

        self.fallback.submit.side_effect = fallback_side_effect

        outcome1 = self.dual_executor.submit(context1)
        self.assertEqual(outcome1.state, "accepted")
        self.assertEqual(self.circuit.failure_count, 1)
        self.fallback.submit.assert_called_once()

        # 2. 第二次与第三次失败触发熔断
        context2 = self._create_context("m_err2")
        self.dual_executor.submit(context2)
        self.assertEqual(self.circuit.failure_count, 2)

        context3 = self._create_context("m_err3")
        self.dual_executor.submit(context3)
        self.assertEqual(self.circuit.failure_count, 3)
        self.assertEqual(self.dual_executor.circuit_status(), "OPEN")

        # 3. 第四次任务在 OPEN 状态下直接走 Native，根本不调用 primary
        self.primary.submit.reset_mock()
        context4 = self._create_context("m_err4")
        outcome4 = self.dual_executor.submit(context4)
        self.assertEqual(outcome4.state, "accepted")
        self.primary.submit.assert_not_called()

    def test_use_case_8_query_strictly_bound_to_track(self):
        """用例 8：已在 Gadget 轨提交的任务，即使后续备轨被激活，query 依然严格绑定 Gadget 轨（防止跨轨串话）。"""
        # 任务 1：在 Gadget 轨提交成功
        context1 = self._create_context("m_task1")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs VALUES (?,?,?,?,?,?)",
                (context1.claim.attempt_id, "s_gadget", "gadget_run_1", "accepted", self.now, "gadget"),
            )
        self.assertEqual(self.dual_executor.current_track(context1.claim.attempt_id), "gadget")

        # 任务 2：在 Native 轨提交成功
        context2 = self._create_context("m_task2")
        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs VALUES (?,?,?,?,?,?)",
                (context2.claim.attempt_id, "s_native", "native_run_2", "accepted", self.now, "native"),
            )
        self.assertEqual(self.dual_executor.current_track(context2.claim.attempt_id), "native")

        # 人为触发熔断开启
        self.circuit.record_failure()
        self.circuit.record_failure()
        self.circuit.record_failure()
        self.assertEqual(self.dual_executor.circuit_status(), "OPEN")

        # 对任务 1 进行 query，必须路由至 primary (Gadget)
        self.primary.query.return_value = ExecutionOutcome("succeeded", text="gadget answer")
        q1 = self.dual_executor.query(context1, "gadget_run_1")
        self.assertEqual(q1.state, "succeeded")
        self.assertEqual(q1.text, "gadget answer")
        self.primary.query.assert_called_once_with(context1, "gadget_run_1")
        self.fallback.query.assert_not_called()

        # 对任务 2 进行 query，必须路由至 fallback (Native)
        self.fallback.query.return_value = ExecutionOutcome("succeeded", text="native answer")
        q2 = self.dual_executor.query(context2, "native_run_2")
        self.assertEqual(q2.state, "succeeded")
        self.assertEqual(q2.text, "native answer")
        self.fallback.query.assert_called_once_with(context2, "native_run_2")

        # 对任务 1 发起 cancel，同样必须严格路由至 primary
        self.primary.cancel.return_value = ExecutionOutcome("cancelled", evidence="cancelled")
        c1 = self.dual_executor.cancel(context1, "gadget_run_1")
        self.assertEqual(c1.state, "cancelled")
        self.primary.cancel.assert_called_once_with(context1, "gadget_run_1")

    def test_cancel_requested_before_submission_does_not_fallback(self):
        """若任务在提交前已被请求取消，返回 not_submitted 且不应降级重试备轨。"""
        context = self._create_context("m_cancel_first")
        self.primary.submit.return_value = ExecutionOutcome("not_submitted", evidence="cancelled_before_submission")
        outcome = self.dual_executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "cancelled_before_submission")
        self.fallback.submit.assert_not_called()

    def test_query_and_cancel_without_track_binding(self):
        """对未绑定的 attempt_id 查询或取消，返回 unknown 且 evidence 为 no_track_binding。"""
        context = self._create_context("m_no_track")
        q = self.dual_executor.query(context, "run_dummy")
        self.assertEqual(q.state, "unknown")
        self.assertEqual(q.evidence, "no_track_binding")

        c = self.dual_executor.cancel(context, "run_dummy")
        self.assertEqual(c.state, "unknown")
        self.assertEqual(c.evidence, "no_track_binding")


class TestEdgeCasesAndHttpErrors(unittest.TestCase):
    """测试边界场景、异常转换以及 client_factory 机制。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "edge.sqlite3"
        self.store = Store(self.db_path)
        self.addCleanup(self.store.close)

    def test_executor_with_client_factory(self):
        """测试 GadgetExecutor 支持通过 client_factory 惰性提供客户端。"""
        mock_client = Mock(spec=GadgetClient)
        mock_client.is_configured.return_value = True
        executor = GadgetExecutor(self.store, client_factory=lambda: mock_client)
        self.assertTrue(executor.is_configured())
        self.assertIs(executor.client, mock_client)

    def test_query_mismatched_run_id_raises_value_error(self):
        """查询时若传入与 attempt 绑定不一致的 run_id，必须抛出 ValueError。"""
        ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})
        ingress.receive("msg_mis", "user_1", "chat_mis", "single", "text")
        claim = self.store.claim("w1", concurrency=1, lease_seconds=60)
        self.store.begin_submission(claim)

        client = GadgetClient(credentials=GadgetCredentials("d", "t"), transport_func=lambda a, p: {})
        executor = GadgetExecutor(self.store, client=client)

        with self.store.transaction():
            self.store.connection.execute(
                "INSERT INTO executor_runs VALUES (?, 's_1', 'run_orig', 'accepted', 100.0, 'gadget')",
                (claim.attempt_id,)
            )

        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        ctx = ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

        with self.assertRaises(ValueError):
            executor.query(ctx, "run_different")

    def test_client_http_error_mappings(self):
        """验证 GadgetClient 能够将标准库 HTTP 401 映射为 AuthError，网络错误映射为 NetworkError。"""
        import io
        import urllib.error
        from unittest.mock import patch

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok", "http://fake.endpoint"))

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.HTTPError("http://fake", 401, "Unauthorized", {}, io.BytesIO(b""))
            with self.assertRaises(GadgetAuthError):
                client.send_message("sid", "hi")

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.URLError("Connection reset")
            with self.assertRaises(GadgetNetworkError):
                client.get_run_status("sid", "r1")


class TestCoreRunnerIntegration(unittest.TestCase):
    """用例 9：与 CoreRunner 的端到端集成测试（通过 runner.tick() 全流程驱动）。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "e2e.sqlite3"
        self.now = 100.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"alice"})

        # 使用真实 GadgetClient（注入 mock transport）
        self.mock_transport = Mock()
        self.gadget_client = GadgetClient(
            credentials=GadgetCredentials("dev_real", "tok_real"),
            transport_func=self.mock_transport,
        )
        self.primary = GadgetExecutor(self.store, client=self.gadget_client)
        self.fallback = Mock(spec=MuseExecutor)
        self.dual_executor = DualTrackExecutor(
            store=self.store,
            primary=self.primary,
            fallback=self.fallback,
        )

    def test_full_pipeline_claimed_running_succeeded(self):
        """验证 runner.tick() 驱动任务从 claimed 到 running 再到 succeeded 的端到端流程。"""
        # 1. 模拟上游消息摄入
        ingest = self.ingress.receive("msg_100", "alice", "chat_alice", "single", "What is dual-track?")
        self.assertTrue(ingest.created)

        # 2. 设置 transport 响应：提交阶段返回 run_id
        self.mock_transport.side_effect = lambda action, payload: {
            "send_message": {"run_id": "gadget_run_e2e_100"},
            "get_run_status": {"status": "running"},
        }.get(action, {})

        runner = CoreRunner(self.store, self.dual_executor, "worker_e2e")

        # 3. 第一次 tick()：认领并提交，状态变为 accepted
        state1 = runner.tick()
        self.assertEqual(state1, "accepted")
        task = self.store.task(ingest.task_id)
        self.assertEqual(task["state"], "running")

        # 检查 executor_runs 记录
        run_row = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (task["attempt_id"],)
        ).fetchone()
        self.assertIsNotNone(run_row)
        self.assertEqual(run_row["track"], "gadget")
        self.assertEqual(run_row["platform_run_id"], "gadget_run_e2e_100")

        # 4. 第二次 tick()：轮询仍为 running
        state2 = runner.tick()
        self.assertEqual(state2, "running")
        self.assertEqual(self.store.task(ingest.task_id)["state"], "running")

        # 5. 修改 transport 响应为 completed
        self.mock_transport.side_effect = lambda action, payload: {
            "get_run_status": {
                "status": "completed",
                "display_text": "Dual-track is high-availability architecture.",
            },
        }.get(action, {})

        # 6. 第三次 tick()：轮询成功，任务流转至 succeeded 并写入出站消息
        state3 = runner.tick()
        self.assertEqual(state3, "succeeded")
        task_final = self.store.task(ingest.task_id)
        self.assertEqual(task_final["state"], "succeeded")
        self.assertEqual(task_final["result"], "Dual-track is high-availability architecture.")

        outbound = self.store.connection.execute(
            "SELECT * FROM outbound_messages WHERE task_id=?", (ingest.task_id,)
        ).fetchone()
        self.assertIsNotNone(outbound)
        self.assertEqual(outbound["text"], "Dual-track is high-availability architecture.")

        # 7. 第四次 tick()：队列清空，进入 idle
        state4 = runner.tick()
        self.assertEqual(state4, "idle")


class TestSchemaBackwardCompatibility(unittest.TestCase):
    """验证从没有 track 字段的旧表结构升级时的向前兼容性。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "legacy.sqlite3"

    def test_legacy_table_auto_migrated_with_track_column(self):
        """模拟旧版数据库只有 (attempt_id, session_id, platform_run_id, status, created_at)。"""
        # 手动创建旧版表
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE executor_runs (
                attempt_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                platform_run_id TEXT,
                status TEXT NOT NULL,
                created_at REAL NOT NULL
            );
        """)
        conn.commit()
        conn.close()

        # 打开 Store 并运行 ensure_executor_schema
        store = Store(self.db_path)
        self.addCleanup(store.close)
        ensure_executor_schema(store)

        # 验证 track 字段已被平滑追加，且默认值为 'native'
        columns = [row["name"] for row in store.connection.execute("PRAGMA table_info(executor_runs)").fetchall()]
        self.assertIn("track", columns)

        # 验证旧记录插入和默认值生效
        store.connection.execute(
            "INSERT INTO executor_runs (attempt_id, session_id, platform_run_id, status, created_at) "
            "VALUES ('att_old', 's_old', 'r_old', 'accepted', 100.0)"
        )
        row = store.connection.execute("SELECT track FROM executor_runs WHERE attempt_id='att_old'").fetchone()
        self.assertEqual(row["track"], "native")


class TestSkepticalHardeningAndEdgeCases(unittest.TestCase):
    """深层健壮性验证：覆盖非字符串凭证类型、URL 编码、异常保留、HALF_OPEN 重置与真实双轨降级。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "hardened.sqlite3"
        self.now = 5000.0
        self.store = Store(self.db_path, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.ingress = CoreIngress(self.store, "wecom", "bot", {"user_1"})

    def _create_context(self, msg_id="m_hard") -> ExecutionContext:
        self.ingress.receive(msg_id, "user_1", f"chat_{msg_id}", "single", f"text {msg_id}")
        claim = self.store.claim("w_hard", concurrency=1, lease_seconds=60)
        self.assertIsNotNone(claim)
        self.store.begin_submission(claim)
        row = self.store.connection.execute(
            "SELECT m.text,m.external_id,m.sender_id,c.channel,c.account_id,c.chat_type,c.chat_id "
            "FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "JOIN conversations c ON c.id=t.conversation_id WHERE t.id=?",
            (claim.task_id,),
        ).fetchone()
        return ExecutionContext(
            claim=claim,
            text=row["text"],
            external_message_id=row["external_id"],
            sender_id=row["sender_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            chat_type=row["chat_type"],
            chat_id=row["chat_id"],
        )

    def test_credentials_with_non_string_types(self):
        """非字符串凭证字段（如 JSON 中的数字 ID）能安全转换并不发生 AttributeError。"""
        # 1. 纯数字对象直接传入 dataclass 时，is_configured 安全返回 False，不抛出 AttributeError
        numeric_creds = GadgetCredentials(device_id=12345, token=67890)  # type: ignore
        self.assertFalse(numeric_creds.is_configured())

        # 2. 从 JSON 文件读取数字类型时，from_env_or_file 自动将其转换为合法字符串
        json_file = Path(self.temp_dir.name) / "numeric.json"
        json_file.write_text(json.dumps({"device_id": 88888, "token": 99999}), encoding="utf-8")
        loaded_creds = GadgetCredentials.from_env_or_file(json_file, env={})
        self.assertTrue(loaded_creds.is_configured())
        self.assertEqual(loaded_creds.device_id, "88888")
        self.assertEqual(loaded_creds.token, "99999")

    def test_url_quoting_for_spaces_and_unicode_in_session_id(self):
        """当 session_id 或 run_id 包含空格或中文时，URL 经安全编码，不触发 urllib InvalidURL。"""
        from unittest.mock import MagicMock, patch

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok", "https://gadget.muse.local"))
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps({"run_id": "run_ok_1"}).encode("utf-8")
            mock_resp.__enter__.return_value = mock_resp
            mock_urlopen.return_value = mock_resp

            # session_id 包含中文与空格
            run_id = client.send_message("gadget:side:测试 1:0", "hello")
            self.assertEqual(run_id, "run_ok_1")

            # 验证请求 URL 已被有效编码，不含未转义的空格或非法字符
            req = mock_urlopen.call_args[0][0]
            self.assertNotIn(" ", req.full_url)
            self.assertIn("https://gadget.muse.local/api/sessions/", req.full_url)

    def test_transport_func_internal_type_error_preserved(self):
        """当用户注入的 transport_func 内部抛出 TypeError 时，原始异常与调用栈不被吞没。"""
        def bad_transport(action, payload):
            return None + 1  # 内部抛出 TypeError

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok"), transport_func=bad_transport)
        with self.assertRaises(TypeError) as ctx:
            client.send_message("sid", "hi")
        self.assertIn("unsupported operand type", str(ctx.exception))

    def test_circuit_breaker_half_open_failure_resets_cooldown(self):
        """半开状态试探失败后，熔断器立即重回 OPEN 并重启冷却计时。"""
        now = 1000.0
        cb = CircuitBreaker(failure_threshold=5, cooldown_seconds=30.0, clock=lambda: now)

        # 触发熔断
        for _ in range(5):
            cb.record_failure()
        self.assertEqual(cb.status, "OPEN")

        # 冷却结束，进入 HALF_OPEN
        now += 35.0
        self.assertEqual(cb.status, "HALF_OPEN")

        # 半开试探失败
        cb.record_failure()
        self.assertEqual(cb.status, "OPEN")

        # 冷却重新开始：经过 15s 后仍应是 OPEN，而不能过早回到 HALF_OPEN
        now += 15.0
        self.assertEqual(cb.status, "OPEN")
        self.assertFalse(cb.can_execute())

        # 再次经过 16s（累计 31s），再次进入 HALF_OPEN
        now += 16.0
        self.assertEqual(cb.status, "HALF_OPEN")
        self.assertTrue(cb.can_execute())

    def test_gadget_executor_pre_submission_cancellation_cleans_db(self):
        """在提交前被请求取消时，GadgetExecutor 清理临时行，不留 platform_run_id IS NULL 的僵尸记录。"""
        context = self._create_context("m_cancel_clean")
        # 标记任务取消
        self.store.request_cancel(context.claim.task_id, context.claim.conversation_id)

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok"), transport_func=lambda a, p: {})
        executor = GadgetExecutor(self.store, client=client)

        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "not_submitted")
        self.assertEqual(outcome.evidence, "cancelled_before_submission")

        # 确认 executor_runs 中不存在任何残留行
        rows = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_gadget_executor_standalone_network_failure_returns_unknown(self):
        """真实 GadgetExecutor 在独立运行时若发生网络异常，返回 unknown 并不抛出未捕获异常。"""
        context = self._create_context("m_net_fail")

        def failing_transport(action, payload):
            raise GadgetNetworkError("Gateway Timeout 504")

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok"), transport_func=failing_transport)
        executor = GadgetExecutor(self.store, client=client)

        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "unknown")
        self.assertIn("Gateway Timeout 504", outcome.evidence)

        # 临时记录已被安全清理
        rows = self.store.connection.execute(
            "SELECT * FROM executor_runs WHERE attempt_id=?", (context.claim.attempt_id,)
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_gadget_executor_query_unrecognized_status_returns_unknown(self):
        """当平台返回非预期或未识别的状态字符串时，query 返回 unknown 而非假定仍在 running。"""
        context = self._create_context("m_corrupt_status")
        mock_transport = Mock()
        mock_transport.side_effect = lambda action, payload: {
            "send_message": {"run_id": "run_corrupt_1"},
            "get_run_status": {"status": "alien_unrecognized_status"},
        }.get(action, {})

        client = GadgetClient(credentials=GadgetCredentials("dev", "tok"), transport_func=mock_transport)
        executor = GadgetExecutor(self.store, client=client)

        outcome = executor.submit(context)
        self.assertEqual(outcome.state, "accepted")

        q_outcome = executor.query(context, "run_corrupt_1")
        self.assertEqual(q_outcome.state, "unknown")
        self.assertEqual(q_outcome.evidence, "unrecognized_status_alien_unrecognized_status")

    def test_dual_track_real_gadget_executor_upstream_failure_falls_back_to_native(self):
        """端到端真实降级：当主轨真实 GadgetExecutor 遭遇网络异常时，平滑降级至 Native 备轨成功提交。"""
        context = self._create_context("m_dual_real_fallback")

        # 真实 Gadget 客户端抛出网络异常
        def broken_transport(action, payload):
            raise GadgetNetworkError("Connection refused by upstream")

        primary_client = GadgetClient(credentials=GadgetCredentials("dev", "tok"), transport_func=broken_transport)
        primary = GadgetExecutor(self.store, client=primary_client)

        # 模拟 Native 备轨成功执行
        fallback = Mock(spec=MuseExecutor)
        fallback.submit.return_value = ExecutionOutcome("accepted", run_id="native_real_run_1")

        dual_executor = DualTrackExecutor(
            store=self.store,
            primary=primary,
            fallback=fallback,
        )

        outcome = dual_executor.submit(context)
        self.assertEqual(outcome.state, "accepted")
        self.assertEqual(outcome.run_id, "native_real_run_1")
        self.assertEqual(dual_executor.circuit_status(), "CLOSED")
        self.assertEqual(dual_executor.circuit.failure_count, 1)
        fallback.submit.assert_called_once_with(context)


if __name__ == "__main__":
    unittest.main()
