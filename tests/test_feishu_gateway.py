"""飞书（Feishu/Lark）官方通道适配完备自动化测试套件。

覆盖：
1. 飞书凭证加载、Token 自动刷新与热重载；
2. 单聊与群聊消息入站解析、白名单过滤；
3. 入站连发防打碎合并器（InboundCoalescer）在飞书通道的集成与屏障冲刷；
4. 原生请求级 uuid 幂等发送与 Core 投递链路；
5. 飞书交互卡片构建（普通/高危）与 card.action.trigger 直连 ApprovalStore 决断；
6. 群聊破坏性管理指令权限隔离（非管理员拦截）；
7. Core 模式安全隔离与 CLI 状态查询。
所有测试使用标准库与沙箱 Mock，无需任何外部网络或公网连接。
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

from mobile_channels.approvals import ApprovalStore
from mobile_channels.gateway import GatewayBinding
from mobile_channels.models import InboundMessage
from mobile_channels.modes import write_modes
from mobile_channels.storage import Store

# 导入飞书模块
import importlib.util

ROOT = Path(__file__).resolve().parents[1]
spec_cards = importlib.util.spec_from_file_location("feishu_cards", ROOT / "feishu-bot/feishu_cards.py")
feishu_cards = importlib.util.module_from_spec(spec_cards)
spec_cards.loader.exec_module(feishu_cards)

spec_gw = importlib.util.spec_from_file_location("feishu_gateway", ROOT / "feishu-bot/gateway.py")
feishu_gateway = importlib.util.module_from_spec(spec_gw)
spec_gw.loader.exec_module(feishu_gateway)

FeishuGateway = feishu_gateway.FeishuGateway
FeishuCredentials = feishu_gateway.FeishuCredentials


class FeishuGatewayTests(unittest.IsolatedAsyncioTestCase):
    """飞书通道自动化测试集。"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="feishu-test-")
        self.addCleanup(self.temp_dir.cleanup)
        self.base = Path(self.temp_dir.name)

        # 准备沙箱配置文件
        self.cred_file = self.base / "credentials.env"
        self.cred_file.write_text(
            "FEISHU_APP_ID=cli_test_appid_12345\nFEISHU_APP_SECRET=test_secret_67890\n",
            encoding="utf-8",
        )

        self.db_path = self.base / "core.sqlite3"
        with Store(self.db_path):
            pass

        self.modes_file = self.base / "modes.json"
        self.modes_config = {
            "channels": {
                "feishu": {
                    "mode": "core",
                    "database": str(self.db_path),
                    "account_id": "feishu_bot_main",
                    "allowed_senders": ["ou_user_alice", "ou_user_bob"],
                    "administrators": ["ou_user_alice"],
                }
            }
        }
        write_modes(self.modes_file, self.modes_config)

        # 环境变量补丁
        self.env_patch = patch.dict(
            os.environ,
            {
                "FEISHU_CRED_FILE": str(self.cred_file),
                "MUSE_CHANNEL_MODES": str(self.modes_file),
            },
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

        # Mock 飞书开放平台传输层
        self.mock_sent_requests: list[dict] = []

        def mock_sender(method, path, body=None, query=None, headers=None):
            req_info = {
                "method": method,
                "path": path,
                "body": body,
                "query": query,
                "headers": headers,
            }
            self.mock_sent_requests.append(req_info)
            if path == "/auth/v3/tenant_access_token/internal":
                return {
                    "code": 0,
                    "msg": "ok",
                    "tenant_access_token": "t-test-token-valid-abc",
                    "expire": 7200,
                }
            if path == "/im/v1/messages" and method == "POST":
                return {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "message_id": f"om_msg_{uuid.uuid4().hex[:12]}",
                        "uuid": (body or {}).get("uuid", ""),
                    },
                }
            if path.startswith("/im/v1/messages/") and method == "PATCH":
                return {"code": 0, "msg": "success", "data": {"updated": True}}
            return {"code": 0, "msg": "ok"}

        self.mock_sender = mock_sender

        # 重定向 gateway 内部路径到临时目录
        self.patch_state = patch.object(feishu_gateway, "STATE_DIR", self.base / "state")
        self.patch_lock = patch.object(feishu_gateway, "LOCK_FILE", self.base / "state" / "gateway.lock")
        self.patch_inbox = patch.object(feishu_gateway, "INBOX_FILE", self.base / "state" / "inbox.jsonl")
        self.patch_state.start()
        self.patch_lock.start()
        self.patch_inbox.start()
        self.addCleanup(self.patch_state.stop)
        self.addCleanup(self.patch_lock.stop)
        self.addCleanup(self.patch_inbox.stop)

    # 1. 凭证加载、Token 维护与热重载
    async def test_credentials_token_and_hot_reload(self):
        """测试飞书凭证加载、Token 获取与运行中热重载。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)
        self.assertEqual(gw.credentials.app_id, "cli_test_appid_12345")
        self.assertEqual(gw.credentials.app_secret, "test_secret_67890")

        # 获取 Token
        token = await gw.get_tenant_access_token()
        self.assertEqual(token, "t-test-token-valid-abc")
        self.assertTrue(gw.credentials.token_expires_at > time.time())

        # 修改凭证文件并热重载
        self.cred_file.write_text(
            "FEISHU_APP_ID=cli_new_app_999\nFEISHU_APP_SECRET=new_secret_888\n",
            encoding="utf-8",
        )
        gw.credentials.reload()
        self.assertEqual(gw.credentials.app_id, "cli_new_app_999")
        self.assertEqual(gw.credentials.app_secret, "new_secret_888")

    # 2. 单聊白名单过滤与消息接收
    async def test_p2p_message_allowlist_filtering(self):
        """测试单聊消息白名单拦截与合法放行。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # 1. 构造未授权用户的单聊消息
        unauth_envelope = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_evil_stranger"}},
                "message": {
                    "message_id": "om_unauth_001",
                    "chat_id": "oc_chat_direct_stranger",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "Hello, Muse!"}),
                },
            },
        }
        res_unauth = await gw.handle_event(unauth_envelope)
        self.assertEqual(res_unauth["code"], 403)
        self.assertEqual(res_unauth["msg"], "unauthorized sender")

        # 2. 构造合法白名单用户 alice 的单聊消息
        auth_envelope = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_auth_001",
                    "chat_id": "oc_chat_alice",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "测试正常消息"}),
                },
            },
        }
        res_auth = await gw.handle_event(auth_envelope)
        self.assertEqual(res_auth["code"], 0)

        # 连发合并器中已有 1 条挂起消息
        self.assertEqual(gw.coalescer.pending_count(), 1)

    # 3. 入站连发防打碎合并与命令屏障
    async def test_inbound_coalesce_and_command_barrier(self):
        """测试飞书入站多条消息连发合并与以 / 开头的命令屏障即时冲刷。"""
        fake_clock_time = 1000.0

        def fake_clock():
            nonlocal fake_clock_time
            return fake_clock_time

        gw = FeishuGateway(cred_file=self.cred_file, clock=fake_clock, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)
        gw.coalescer._clock = fake_clock

        # Alice 连续发送第 1 句和第 2 句短文本
        for i, text in enumerate(["第一句话：请帮我检查", "第二句话：服务器状态", "第三句话：特别是磁盘容量"]):
            fake_clock_time += 0.5
            env = {
                "header": {"event_type": "im.message.receive_v1"},
                "event": {
                    "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                    "message": {
                        "message_id": f"om_msg_part_{i+1}",
                        "chat_id": "oc_chat_alice",
                        "chat_type": "p2p",
                        "content": json.dumps({"text": text}),
                    },
                },
            }
            await gw.handle_event(env)

        # 连发 3 条消息均在静默缓冲区中
        self.assertEqual(gw.coalescer.pending_count(), 3)

        # 时间前进 2.1 秒（超过 2.0s 静默窗口）并触发 due 冲刷至 Core 权威数据库
        fake_clock_time += 2.1
        ready = gw.coalescer.due(fake_clock_time)
        self.assertEqual(len(ready), 1)

        coalesced_msg = ready[0]
        self.assertEqual(coalesced_msg.channel, "feishu")
        self.assertEqual(coalesced_msg.sender_id, "ou_user_alice")
        self.assertIn("第一句话：请帮我检查\n第二句话：服务器状态\n第三句话：特别是磁盘容量", coalesced_msg.text)
        self.assertEqual(coalesced_msg.merged_ids, ["om_msg_part_1", "om_msg_part_2", "om_msg_part_3"])

        # 写入数据库
        await gw._dispatch_inbound(coalesced_msg)

        # 测试命令屏障：先放入一条普通消息，随后立即发送 /ping
        fake_clock_time += 10.0
        env_msg = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_pending_before_cmd",
                    "chat_id": "oc_chat_alice",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "挂起内容"}),
                },
            },
        }
        await gw.handle_event(env_msg)
        self.assertEqual(gw.coalescer.pending_count(), 1)

        # 发送 /ping 命令（命令屏障立即出队并写入 Core）
        env_cmd = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_cmd_ping",
                    "chat_id": "oc_chat_alice",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "/ping"}),
                },
            },
        }
        await gw.handle_event(env_cmd)

        # 命令屏障立即清空了挂起缓冲区
        self.assertEqual(gw.coalescer.pending_count(), 0)

        # 在权威 Core 数据库中核查
        with Store(self.db_path) as store:
            rows = store.connection.execute("SELECT external_id, text FROM inbound_messages ORDER BY received_at ASC").fetchall()
            ext_ids = [r["external_id"] for r in rows]
            texts = [r["text"] for r in rows]
            self.assertIn("om_cmd_ping", ext_ids)
            self.assertIn("/ping", texts)

    # 4. 群聊破坏性管理指令权限隔离
    async def test_group_admin_permission_isolation(self):
        """测试群聊中非管理员发送 /stop 或 /new 被安全拒绝，管理员可正常触发。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # Bob 不是管理员，在群聊中尝试发送 /stop
        bob_env = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_bob"}},
                "message": {
                    "message_id": "om_bob_stop",
                    "chat_id": "oc_group_1001",
                    "chat_type": "group",
                    "content": json.dumps({"text": "/stop"}),
                },
            },
        }
        res_bob = await gw.handle_event(bob_env)
        self.assertEqual(res_bob["code"], 403)
        self.assertEqual(res_bob["msg"], "admin command rejected")

        # 检查是否发出了拦截告警
        self.assertTrue(any("需要管理员权限" in str(r.get("body", {}).get("content", "")) for r in self.mock_sent_requests))

        # Alice 是管理员，在群聊中发送 /new
        alice_env = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_alice_new",
                    "chat_id": "oc_group_1001",
                    "chat_type": "group",
                    "content": json.dumps({"text": "/new"}),
                },
            },
        }
        res_alice = await gw.handle_event(alice_env)
        self.assertEqual(res_alice["code"], 0)

        # 检查权威数据库已接收外部消息
        with Store(self.db_path) as store:
            row = store.connection.execute("SELECT text FROM inbound_messages WHERE external_id='om_alice_new'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["text"], "/new")

    # 5. 原生请求级 uuid 幂等发送
    async def test_outbound_uuid_idempotency(self):
        """测试出站请求携带原生 uuid 幂等键，重试发送使用相同 UUID。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        test_uuid = str(uuid.uuid4())
        resp1 = await gw.send_text(
            chat_id="oc_group_demo",
            text="首次出站测试",
            chat_type="group",
            uuid=test_uuid,
        )
        self.assertEqual(resp1["code"], 0)
        self.assertEqual(self.mock_sent_requests[-1]["body"]["uuid"], test_uuid)

        # 模拟重试同一条出站消息，必须保持同一个 uuid
        resp2 = await gw.send_text(
            chat_id="oc_group_demo",
            text="首次出站测试",
            chat_type="group",
            uuid=test_uuid,
        )
        self.assertEqual(resp2["code"], 0)
        self.assertEqual(self.mock_sent_requests[-1]["body"]["uuid"], test_uuid)

    # 6. 飞书交互卡片构建与直连 ApprovalStore 决断
    async def test_feishu_cards_and_approval_store_decision(self):
        """测试飞书普通卡片与高危卡片构建，以及卡片点击直连 ApprovalStore 事务决断。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # 1. 验证普通审批卡片结构
        normal_card = feishu_cards.build_approval_card(
            approval_id="aid-feishu-001",
            operator_ids=["ou_user_alice"],
            prompt="systemctl reload nginx",
            reason="常规配置文件重载",
            is_high_risk=False,
        )
        self.assertEqual(normal_card["header"]["template"], "blue")
        self.assertIn("aid-feishu-001", str(normal_card))
        self.assertIn("systemctl reload nginx", str(normal_card))

        # 2. 验证高危审批卡片结构（🚨 [高危审批] 红色主题）
        high_risk_card = feishu_cards.build_approval_card(
            approval_id="aid-feishu-002",
            operator_ids=["ou_user_alice"],
            prompt="rm -rf /var/lib/data/critical",
            reason="高危核心目录清空",
            is_high_risk=True,
        )
        self.assertEqual(high_risk_card["header"]["template"], "red")
        self.assertIn("🚨 [高危审批]", high_risk_card["header"]["title"]["content"])

        # 3. 初始化并注册审批至权威数据库
        with Store(self.db_path) as store:
            res = store.ingest(InboundMessage(
                channel="feishu",
                account_id="feishu_bot_main",
                chat_type="single",
                chat_id="ou_user_alice",
                message_id="om_approval_seed",
                sender_id="ou_user_alice",
                text="申请执行操作",
            ))
            app_store = ApprovalStore(store)
            app_store.register(
                approval_id="aid-feishu-001",
                conversation_id=res.conversation_id,
                operator_ids=["ou_user_alice"],
                expires_at=time.time() + 600,
            )

        # 4. 模拟非授权人员 ou_user_bob 点击卡片按钮
        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            ok_bob, msg_bob, card_bob = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-feishu-001", "action": "allow_once"},
                operator_id="ou_user_bob",
            )
            self.assertFalse(ok_bob)
            self.assertIn("不在该操作的授权审批人名单中", str(card_bob))

        # 5. 模拟授权人员 ou_user_alice 点击 [单次放行] 按钮
        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            ok_alice, msg_alice, card_alice = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-feishu-001", "action": "allow_once"},
                operator_id="ou_user_alice",
            )
            self.assertTrue(ok_alice)
            self.assertIn("已授权单次执行", str(card_alice))

            # 验证数据库状态已转变为 decided
            status = app_store.status("aid-feishu-001")
            self.assertEqual(status["state"], "decided")
            self.assertEqual(status["decision"], "allow_once")
            self.assertEqual(status["operator_id"], "ou_user_alice")
            self.assertIn("feishu_card_click:ou_user_alice", status["evidence"])

        # 6. 模拟重复点击（已被决断）
        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            ok_dup, msg_dup, card_dup = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-feishu-001", "action": "deny"},
                operator_id="ou_user_alice",
            )
            self.assertFalse(ok_dup)
            self.assertIn("重复操作无效", str(card_dup))

    # 7. GatewayBinding 对 feishu 渠道的投递集成
    async def test_gateway_binding_feishu_delivery(self):
        """测试 GatewayBinding.delivery_loop 支持 feishu 渠道发送并自动附加 uuid 幂等键。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # 摄取一条 /ping 指令消息，触发生成一条 pending 出站消息
        env_cmd = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_cmd_ping_delivery",
                    "chat_id": "oc_chat_alice",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "/ping"}),
                },
            },
        }
        await gw.handle_event(env_cmd)

        with Store(self.db_path) as store:
            pending = store.connection.execute("SELECT * FROM outbound_messages WHERE state='pending'").fetchone()
            self.assertIsNotNone(pending)

        # 启动一轮 delivery_loop 任务并在完成投递后停止
        delivery_task = asyncio.create_task(gw._core_binding.delivery_loop(gw))
        try:
            for _ in range(50):
                await asyncio.sleep(0.05)
                with Store(self.db_path) as store:
                    delivered = store.connection.execute("SELECT * FROM outbound_messages WHERE state='delivered'").fetchall()
                    if delivered:
                        break
        finally:
            delivery_task.cancel()
            try:
                await delivery_task
            except asyncio.CancelledError:
                pass

        with Store(self.db_path) as store:
            delivered_rows = store.connection.execute("SELECT * FROM outbound_messages WHERE state='delivered'").fetchall()
            self.assertEqual(len(delivered_rows), 1)

        # 验证发送的 HTTP 请求带有的 uuid 结构
        self.assertTrue(len(self.mock_sent_requests) > 0)
        sent_req = next(r for r in reversed(self.mock_sent_requests) if r["path"] == "/im/v1/messages")
        self.assertTrue(sent_req["body"]["uuid"].startswith("muse:") or len(sent_req["body"]["uuid"]) == 36)
        self.assertIn("pong", sent_req["body"]["content"])

    # 8. Core 模式下对 CLI Legacy 写的保护与拦截
    def test_cli_legacy_interception_under_core_mode(self):
        """测试当飞书处于 Core 模式时，feishu_cli 拒绝直接向 legacy outbox 写信。"""
        import subprocess

        cmd = [
            "python3",
            str(ROOT / "feishu-bot/feishu_cli.py"),
            "send",
            "--chat-id",
            "ou_user_alice",
            "--text",
            "测试发送内容",
        ]
        result = subprocess.run(
            cmd,
            env=dict(os.environ, MUSE_CHANNEL_MODES=str(self.modes_file)),
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("legacy command disabled", result.stderr)

    # 9. 飞书卡片审批已过期与异常输入边界测试
    async def test_feishu_card_expired_and_invalid_inputs(self):
        """测试已过期的审批单点击被拒绝，缺失参数被拦截。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        with Store(self.db_path) as store:
            res = store.ingest(InboundMessage(
                channel="feishu",
                account_id="feishu_bot_main",
                chat_type="single",
                chat_id="ou_user_alice",
                message_id="om_expired_seed",
                sender_id="ou_user_alice",
                text="即将过期的操作",
            ))
            app_store = ApprovalStore(store)
            # 注册一个 1 秒后即过期的审批单
            app_store.register(
                approval_id="aid-expired-999",
                conversation_id=res.conversation_id,
                operator_ids=["ou_user_alice"],
                expires_at=time.time() + 0.05,
            )

        # 等待其过期
        await asyncio.sleep(0.06)

        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            ok, msg, card = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-expired-999", "action": "allow_once"},
                operator_id="ou_user_alice",
            )
            self.assertFalse(ok)
            self.assertIn("已被处理或已失效", msg)

        # 缺失参数
        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            ok_bad, msg_bad, card_bad = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"action": "allow_once"},  # 缺少 approval_id
                operator_id="ou_user_alice",
            )
            self.assertFalse(ok_bad)
            self.assertIn("缺少", msg_bad)

    # 10. 长连接未知事件容错与心跳事件
    async def test_websocket_heartbeat_and_unknown_event_resilience(self):
        """测试网关对心跳 Ping/Pong 与未知事件帧的安全兼容性。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # 未知事件
        unknown_envelope = {
            "header": {"event_type": "some.future.unsupported.event"},
            "event": {"random_data": 12345},
        }
        resp = await gw.handle_event(unknown_envelope)
        self.assertEqual(resp["code"], 0)
        self.assertEqual(resp["msg"], "event ignored")

    # 11. 优雅关机保证连发缓冲区零消息丢失
    async def test_graceful_shutdown_flushes_pending_coalesce_buffer(self):
        """测试网关在进程退出或 close 时，强制冲刷所有缓冲区内挂起的消息，杜绝消息丢失。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)

        # 写入一条未超时的入站消息（处于 2.0s 连发等待中）
        env = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_shutdown_flush_test",
                    "chat_id": "oc_chat_alice",
                    "chat_type": "p2p",
                    "content": json.dumps({"text": "关机前最后一刻发送的消息"}),
                },
            },
        }
        await gw.handle_event(env)
        self.assertEqual(gw.coalescer.pending_count(), 1)

        # 此时关闭网关
        gw.close()
        self.assertEqual(gw.coalescer.pending_count(), 0)

        # 验证该消息已安全入库 Core 权威表
        with Store(self.db_path) as store:
            row = store.connection.execute(
                "SELECT text FROM inbound_messages WHERE external_id='om_shutdown_flush_test'"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["text"], "关机前最后一刻发送的消息")

    # 12. 飞书 WebSocket 长连接真实握手与帧流转测试
    async def test_websocket_handshake_and_frame_streaming(self):
        """测试 WebSocket 长连接建立、心跳 Ping/Pong 与入站帧、卡片点击回执全双工交互。"""
        # 准备入站消息与卡片点击事件帧
        frames_to_send = [
            json.dumps({"type": "ping"}),
            json.dumps({
                "header": {"event_type": "im.message.receive_v1", "event_id": "evt_ws_msg_1"},
                "event": {
                    "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                    "message": {
                        "message_id": "om_ws_stream_1",
                        "chat_id": "oc_chat_alice",
                        "chat_type": "p2p",
                        "content": json.dumps({"text": "WebSocket 实时推送消息"}),
                    },
                },
            }),
        ]

        class MockWS:
            def __init__(self, frames):
                self.frames = list(frames)
                self.sent = []
                self.closed = False

            async def __aiter__(self):
                for f in self.frames:
                    yield f

            async def send(self, msg):
                self.sent.append(msg)

            async def close(self):
                self.closed = True

        mock_ws = MockWS(frames_to_send)

        class MockWSContext:
            def __init__(self, ws):
                self.ws = ws

            async def __aenter__(self):
                return self.ws

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                await self.ws.close()

        gw = FeishuGateway(
            cred_file=self.cred_file,
            transport_sender=self.mock_sender,
            ws_connect_factory=lambda url: MockWSContext(mock_ws),
        )
        self.addCleanup(gw.close)
        gw._running = True

        # 启动一轮 ws_loop 处理所有帧
        ws_task = asyncio.create_task(gw.ws_loop())
        for _ in range(50):
            if any("pong" in s for s in mock_ws.sent) and gw.coalescer.pending_count() >= 1:
                break
            await asyncio.sleep(0.02)
        gw._running = False
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

        # 验证心跳 Pong 响应帧已送出
        self.assertTrue(any("pong" in s for s in mock_ws.sent))
        # 验证消息已进入连发合并器
        self.assertEqual(gw.coalescer.pending_count(), 1)
        # 验证 status.json 已更新
        status_file = self.base / "state" / "status.json"
        self.assertTrue(status_file.is_file())
        status_data = json.loads(status_file.read_text(encoding="utf-8"))
        self.assertEqual(status_data["channel"], "feishu")
        self.assertEqual(status_data["inbound_count"], 1)

    # 13. card.action.trigger 顶层全流程事件处理与卡片 PATCH 更新
    async def test_card_action_trigger_handle_event_full_flow(self):
        """测试通过 handle_event 顶层分发处理 card.action.trigger 并就地 PATCH 卡片。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # 注册审批单
        with Store(self.db_path) as store:
            res = store.ingest(InboundMessage(
                channel="feishu",
                account_id="feishu_bot_main",
                chat_type="single",
                chat_id="ou_user_alice",
                message_id="om_approval_seed_event",
                sender_id="ou_user_alice",
                text="申请执行重启",
            ))
            app_store = ApprovalStore(store)
            app_store.register(
                approval_id="aid-feishu-event-001",
                conversation_id=res.conversation_id,
                operator_ids=["ou_user_alice"],
                expires_at=time.time() + 600,
            )

        # 构造顶层 card.action.trigger 事件
        event_payload = {
            "header": {"event_type": "card.action.trigger", "event_id": "evt_card_click_1"},
            "event": {
                "operator": {"open_id": "ou_user_alice"},
                "action": {"value": {"approval_id": "aid-feishu-event-001", "action": "allow_once"}},
                "context": {"open_message_id": "om_card_msg_to_patch_001"},
            },
        }

        resp = await gw.handle_event(event_payload)
        self.assertEqual(resp["code"], 0)
        self.assertIn("allow_once", resp["msg"])
        self.assertIn("card", resp)

        # 验证 PATCH 请求已向飞书发送
        patch_reqs = [r for r in self.mock_sent_requests if r["method"] == "PATCH"]
        self.assertTrue(len(patch_reqs) > 0)
        self.assertIn("om_card_msg_to_patch_001", patch_reqs[-1]["path"])

        # 验证数据库状态已转变为 decided
        with Store(self.db_path) as store:
            app_store = ApprovalStore(store)
            st = app_store.status("aid-feishu-event-001")
            self.assertEqual(st["state"], "decided")
            self.assertEqual(st["decision"], "allow_once")

    # 14. 并发审批决断冲突处理（首人成功，第二人返回最新已决断卡片）
    async def test_concurrent_approval_conflict_returns_decided_card(self):
        """测试高频并发点击场景下，第一人成功决断，第二人遭遇 StateError 时就地呈现已决断卡片。"""
        with Store(self.db_path) as store:
            res = store.ingest(InboundMessage(
                channel="feishu",
                account_id="feishu_bot_main",
                chat_type="single",
                chat_id="ou_user_alice",
                message_id="om_approval_race_seed",
                sender_id="ou_user_alice",
                text="申请部署新版本",
            ))
            app_store = ApprovalStore(store)
            app_store.register(
                approval_id="aid-race-001",
                conversation_id=res.conversation_id,
                operator_ids=["ou_user_alice", "ou_user_bob"],
                expires_at=time.time() + 600,
            )

            # Alice 先点击 allow_once 成功
            ok1, msg1, card1 = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-race-001", "action": "allow_once"},
                operator_id="ou_user_alice",
            )
            self.assertTrue(ok1)

            # Bob 几乎同时点击 deny，发生冲突
            ok2, msg2, card2 = feishu_cards.handle_card_action(
                approval_store=app_store,
                action_value={"approval_id": "aid-race-001", "action": "deny"},
                operator_id="ou_user_bob",
            )
            self.assertFalse(ok2)
            # 应当返回已决断卡片而非通用错误信息
            self.assertIn("已由 `ou_user_alice` 决断为 [allow_once]", str(card2))

    # 15. Legacy 模式下发件箱轮询与结果记录
    async def test_legacy_mode_outbox_loop_and_results(self):
        """测试在 Legacy 模式下，网关能够消费 state/outbox.jsonl 并将执行结果写入 state/outbox_results.jsonl。"""
        # 构造 Legacy 模式的 modes 配置
        legacy_modes_file = self.base / "legacy_modes.json"
        write_modes(legacy_modes_file, {"channels": {"feishu": {"mode": "legacy"}}})

        with patch.dict(os.environ, {"MUSE_CHANNEL_MODES": str(legacy_modes_file)}):
            gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
            self.addCleanup(gw.close)
            self.assertFalse(gw.is_core_mode)

            # 模拟向 outbox.jsonl 写入待发送消息
            outbox_file = self.base / "state" / "outbox.jsonl"
            outbox_file.parent.mkdir(parents=True, exist_ok=True)
            row = {
                "id": "out_msg_legacy_001",
                "channel": "feishu",
                "mode": "send",
                "chat_id": "oc_chat_alice",
                "chat_type": "single",
                "msg_type": "text",
                "text": "Legacy 发件箱测试消息",
                "timestamp": time.time(),
            }
            outbox_file.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")

            # 启动一轮 outbox_loop
            gw._running = True
            loop_task = asyncio.create_task(gw.outbox_loop())
            await asyncio.sleep(0.1)
            gw._running = False
            try:
                await loop_task
            except asyncio.CancelledError:
                pass

            # 验证消息已发送
            self.assertTrue(any("Legacy 发件箱测试消息" in str(r.get("body", {}).get("content", "")) for r in self.mock_sent_requests))

            # 验证 outbox_results.jsonl 中已包含记录
            results_file = self.base / "state" / "outbox_results.jsonl"
            self.assertTrue(results_file.is_file())
            results_content = results_file.read_text(encoding="utf-8")
            self.assertIn("out_msg_legacy_001", results_content)
            self.assertIn('"success": true', results_content)

    # 16. 思考状态即时反馈与独立执行兼容性
    async def test_thinking_feedback_flow_without_import_error(self):
        """测试在开启即时思考反馈时，网关收到消息正常发出思考卡片，无任何导入或运行时错误。"""
        with patch.dict(os.environ, {"FEISHU_ENABLE_THINKING_FEEDBACK": "1"}):
            gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
            self.addCleanup(gw.close)

            envelope = {
                "header": {"event_type": "im.message.receive_v1"},
                "event": {
                    "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                    "message": {
                        "message_id": "om_thinking_test_1",
                        "chat_id": "oc_chat_alice",
                        "chat_type": "p2p",
                        "content": json.dumps({"text": "请帮我分析日志"}),
                    },
                },
            }
            res = await gw.handle_event(envelope)
            self.assertEqual(res["code"], 0)

            # 检查是否成功发出思考卡片
            thinking_reqs = [
                r for r in self.mock_sent_requests
                if r.get("body", {}).get("msg_type") == "interactive"
                and "thinking-om_thinking_test_1" in str(r.get("body", {}).get("uuid", ""))
            ]
            self.assertEqual(len(thinking_reqs), 1)


    # 17. 交互卡片决断写入 decisions.jsonl 中转文件测试
    async def test_card_action_writes_to_decisions_jsonl(self):
        """测试卡片按钮点击时，决断信息被正确原子写入 decisions.jsonl 供 relay 消费。"""
        relay_dir = self.base / "approval-relay"
        decisions_file = relay_dir / "decisions.jsonl"
        decisions_lock = relay_dir / "decisions.lock"

        with patch.object(feishu_gateway, "APPROVAL_RELAY_DIR", relay_dir), \
             patch.object(feishu_gateway, "DECISIONS_FILE", decisions_file), \
             patch.object(feishu_gateway, "DECISIONS_LOCK", decisions_lock):

            gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
            self.addCleanup(gw.close)

            # 准备一个在 ApprovalStore 中的待决审批
            with Store(self.db_path) as s:
                ingest_res = s.ingest(InboundMessage(
                    channel="feishu",
                    account_id="feishu_bot_main",
                    chat_type="group",
                    chat_id="oc_chat_room_01",
                    message_id="om_approval_seed_17",
                    sender_id="ou_user_alice",
                    text="测试审批",
                ))
                appr_store = ApprovalStore(s)
                appr_store.register(
                    approval_id="aid-relay-test-01",
                    conversation_id=ingest_res.conversation_id,
                    operator_ids=["ou_user_alice"],
                    expires_at=time.time() + 600,
                )

            card_envelope = {
                "header": {"event_type": "card.action.trigger"},
                "event": {
                    "operator": {"open_id": "ou_user_alice"},
                    "action": {
                        "value": {
                            "action": "allow_once",
                            "approval_id": "aid-relay-test-01",
                            "tool_name": "bash",
                            "risk_level": "L1",
                        }
                    },
                    "context": {
                        "open_message_id": "om_card_msg_999",
                        "open_chat_id": "oc_chat_room_01",
                    },
                },
            }

            res = await gw.handle_event(card_envelope)
            self.assertEqual(res["code"], 0)

            # 校验 decisions.jsonl 已存在并被写入
            self.assertTrue(decisions_file.is_file())
            content = decisions_file.read_text(encoding="utf-8")
            self.assertIn('"num": "aid-relay-test-01"', content)
            self.assertIn('"decision": "allow_once"', content)
            self.assertIn('"channel": "feishu"', content)
            self.assertIn('"operator_id": "ou_user_alice"', content)
            self.assertIn('"chat_id": "oc_chat_room_01"', content)

    # 18. 群聊发信人白名单统一鉴权测试
    async def test_group_unauthorized_sender_rejected(self):
        """测试在配置了 allowed_senders 时，群聊中未在白名单的陌生人发信被拒绝。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        envelope = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_unknown_intruder"}},
                "message": {
                    "message_id": "om_intruder_msg",
                    "chat_id": "oc_group_001",
                    "chat_type": "group",
                    "content": json.dumps({"text": "Hello bot"}),
                },
            },
        }

        res = await gw.handle_event(envelope)
        self.assertEqual(res["code"], 403)
        self.assertEqual(res["msg"], "unauthorized sender")

    # 19. 群聊管理命令带参数前缀识别测试
    async def test_group_admin_command_prefix_arguments(self):
        """测试群聊中非管理员发送带参数的 /stop now 或 /new task，被安全识别为管理指令并拦截。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)

        # ou_user_bob 在 allowed_senders 中但不在 administrators 中
        envelope = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_bob"}},
                "message": {
                    "message_id": "om_bob_stop_args",
                    "chat_id": "oc_group_001",
                    "chat_type": "group",
                    "content": json.dumps({"text": "/stop immediately please"}),
                },
            },
        }

        res = await gw.handle_event(envelope)
        self.assertEqual(res["code"], 403)
        self.assertEqual(res["msg"], "admin command rejected")

        # 管理员 ou_user_alice 发送带参数的 /new project_1 应该被正常接受
        envelope_admin = {
            "header": {"event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_user_alice"}},
                "message": {
                    "message_id": "om_alice_new_args",
                    "chat_id": "oc_group_001",
                    "chat_type": "group",
                    "content": json.dumps({"text": "/new feature-branch-xyz"}),
                },
            },
        }

        res_admin = await gw.handle_event(envelope_admin)
        self.assertEqual(res_admin["code"], 0)
        self.assertEqual(res_admin["msg"], "message accepted")

    # 20. 跨会话卡片核销防越权测试
    async def test_cross_conversation_card_action_guard(self):
        """测试卡片若被转发到其他群聊核销，因会话 ID 不一致被跨会话安全守卫拦截。"""
        with Store(self.db_path) as s:
            ingest_res = s.ingest(InboundMessage(
                channel="feishu",
                account_id="feishu_bot_main",
                chat_type="group",
                chat_id="oc_original_group",
                message_id="om_approval_seed_20",
                sender_id="ou_user_alice",
                text="测试越权审批",
            ))
            appr_store = ApprovalStore(s)
            appr_store.register(
                approval_id="aid-cross-conv-01",
                conversation_id=ingest_res.conversation_id,
                operator_ids=["ou_user_alice"],
                expires_at=time.time() + 600,
            )

            # 在不同的会话 oc_foreign_group 中尝试点击核销
            ok, msg, _ = feishu_cards.handle_card_action(
                approval_store=appr_store,
                action_value={
                    "action": "deny",
                    "approval_id": "aid-cross-conv-01",
                },
                operator_id="ou_user_alice",
                conversation_id="oc_foreign_group",
            )
            self.assertFalse(ok)
            self.assertIn("跨会话操作未授权", msg)

    # 21. Token 401 失效自愈与自动重试测试
    async def test_token_401_auto_refresh_recovery(self):
        """测试当飞书 API 返回 401 Token 失效时，网关自动清空凭证缓存并强制重刷重试成功。"""
        call_count = 0

        async def mock_sender(method, path, body=None, query=None, headers=None):
            nonlocal call_count
            call_count += 1
            if "/auth/v3/tenant_access_token" in path:
                return {
                    "code": 0,
                    "tenant_access_token": f"mock_token_v{call_count}",
                    "expire": 7200,
                }
            if path == "/im/v1/messages":
                # 第一次遇到 401
                if call_count <= 2:
                    return {"code": 99991663, "msg": "tenant_access_token is invalid"}
                # 第二次重试成功
                return {"code": 0, "msg": "ok", "data": {"message_id": "om_healed_001"}}
            return {"code": 0}

        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=mock_sender)
        self.addCleanup(gw.close)

        res = await gw.send_text("oc_chat_room_01", "测试自愈消息")
        self.assertEqual(res["code"], 0)
        self.assertEqual(res["data"]["message_id"], "om_healed_001")
        # 验证至少请求过一次 token、一次 401、一次刷新 token、一次重发
        self.assertGreaterEqual(call_count, 3)

    # 22. 子任务异常隔离与独立恢复测试
    async def test_subtask_supervisor_exception_isolation(self):
        """测试子任务异常退出后由监督循环独立恢复，不影响其他协程。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)
        gw._running = True

        runs = 0

        async def flaky_task():
            nonlocal runs
            runs += 1
            if runs == 1:
                raise RuntimeError("网络偶发波动异常")
            # 第二次正常执行后主动退出
            gw._running = False

        # 启动监督循环，不应向外抛出 RuntimeError
        await gw._supervised_loop("test_flaky", flaky_task)
        self.assertEqual(runs, 2)

    # 23. 飞书卡片决断中转与 relay.consume_decisions 端到端集成测试
    async def test_feishu_decision_consumed_by_approval_relay_end_to_end(self):
        """测试飞书卡片产生的决断能被 approval-relay.consume_decisions 顺利消费并向平台提交 decide。"""
        import sys
        relay_path = str(feishu_gateway.REPO_ROOT / "approval-relay")
        if relay_path not in sys.path:
            sys.path.insert(0, relay_path)
        import approval_relay as relay

        relay_dir = self.base / "approval-relay"
        decisions_file = relay_dir / "decisions.jsonl"
        decisions_lock = relay_dir / "decisions.lock"
        offset_file = relay_dir / "decisions.offset"
        state_file = relay_dir / "state.json"

        # 准备 relay 内部状态
        st = {
            "next_num": 2,
            "items": {
                "1": {
                    "approval_id": "aid-feishu-e2e",
                    "who": "运维执行",
                    "what": "systemctl restart nginx",
                    "expires_ms": (time.time() + 600) * 1000,
                    "status": "pending",
                    "decision_scope": {
                        "channel": "feishu",
                        "chat_id": "oc_chat_room_01",
                        "operator_ids": ["ou_user_alice"],
                    },
                }
            },
        }

        with patch.object(feishu_gateway, "APPROVAL_RELAY_DIR", relay_dir), \
             patch.object(feishu_gateway, "DECISIONS_FILE", decisions_file), \
             patch.object(feishu_gateway, "DECISIONS_LOCK", decisions_lock), \
             patch.object(relay, "BASE", str(relay_dir)), \
             patch.object(relay, "STATE_F", str(state_file)), \
             patch.object(relay, "STATUS_F", str(relay_dir / "status.json")), \
             patch.object(relay, "DECISIONS_F", str(decisions_file)), \
             patch.object(relay, "OFFSET_F", str(offset_file)):

            relay.save_state(st)

            gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
            self.addCleanup(gw.close)

            # 飞书触发卡片点击（以 approval_id 为 num）
            card_envelope = {
                "header": {"event_type": "card.action.trigger"},
                "event": {
                    "operator": {"open_id": "ou_user_alice"},
                    "action": {
                        "value": {
                            "action": "allow_once",
                            "approval_id": "aid-feishu-e2e",
                        }
                    },
                    "context": {
                        "open_message_id": "om_card_msg_e2e",
                        "open_chat_id": "oc_chat_room_01",
                    },
                },
            }
            res = await gw.handle_event(card_envelope)
            self.assertEqual(res["code"], 0)

            # 模拟 relay.consume_decisions 消费
            class MockPlatformGW:
                def __init__(self):
                    self.calls = []
                def call_json(self, method, path_params=None, body=None, timeout=None):
                    self.calls.append((method, path_params, body))
                    return {"ok": True}

            mock_platform = MockPlatformGW()
            relay.consume_decisions(mock_platform, st)

            # 断言：已成功调用平台 egress.approval.decide
            self.assertEqual(len(mock_platform.calls), 1)
            method, path_params, body = mock_platform.calls[0]
            self.assertEqual(method, "egress.approval.decide")
            self.assertEqual(path_params, {"approval_id": "aid-feishu-e2e"})
            self.assertEqual(body, {"decision": "allow_once"})
            self.assertEqual(st["items"]["1"]["status"], "decided:allow_once")

    # 24. 非 Core 模式下卡片点击与 build_decided_card 兼容性测试
    async def test_non_core_card_action_trigger_returns_valid_card(self):
        """测试网关处于非 Core 模式（Legacy/Relay 模式）时，卡片点击就地决断卡片正常生成，无 TypeError。"""
        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=self.mock_sender)
        self.addCleanup(gw.close)
        gw.is_core_mode = False

        card_envelope = {
            "header": {"event_type": "card.action.trigger"},
            "event": {
                "operator": {"open_id": "ou_user_alice"},
                "action": {
                    "value": {
                        "action": "allow_once",
                        "approval_id": "aid-non-core-01",
                        "tool_name": "bash",
                        "risk_level": "L2",
                    }
                },
                "context": {
                    "open_message_id": "om_msg_non_core",
                    "open_chat_id": "oc_chat_room_01",
                },
            },
        }
        res = await gw.handle_event(card_envelope)
        self.assertEqual(res["code"], 0)
        self.assertIn("card", res)
        # 卡片包含审批单号与状态
        card_content = json.dumps(res["card"], ensure_ascii=False)
        self.assertIn("aid-non-core-01", card_content)
        self.assertIn("bash", card_content)

    # 25. Legacy 发件箱支持 content 字段与 send_card 卡片派发
    async def test_legacy_outbox_content_and_card_dispatch(self):
        """测试 _dispatch_legacy_outbox_row 兼容 content 字段并正确调用 send_card。"""
        sent_messages = []
        async def mock_sender(method, path, body=None, query=None, headers=None):
            sent_messages.append((method, path, body))
            if "/auth/v3/tenant_access_token" in path:
                return {"code": 0, "tenant_access_token": "mock_token_leg", "expire": 7200}
            return {"code": 0, "data": {"message_id": "om_out_001"}}

        gw = FeishuGateway(cred_file=self.cred_file, transport_sender=mock_sender)
        self.addCleanup(gw.close)

        # 1. 验证 content 字段作为文本发送
        row_text = {"id": "msg_c_1", "chat_id": "oc_room_1", "content": "hello from content"}
        await gw._dispatch_legacy_outbox_row(row_text)
        self.assertEqual(sent_messages[-1][2]["content"], '{"text": "hello from content"}')

        # 2. 验证 mode=send_card 触发卡片发送
        card_obj = {"elements": [{"tag": "markdown", "content": "card body"}]}
        row_card = {"id": "msg_c_2", "chat_id": "oc_room_1", "mode": "send_card", "card": card_obj}
        await gw._dispatch_legacy_outbox_row(row_card)
        self.assertEqual(sent_messages[-1][2]["msg_type"], "interactive")
        self.assertEqual(json.loads(sent_messages[-1][2]["content"]), card_obj)


if __name__ == "__main__":
    unittest.main()


