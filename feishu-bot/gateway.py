"""飞书（Feishu/Lark）官方长连接网关核心守护实现。

功能特性：
1. 完全免公网穿透：客户端主动向外发起 WebSocket 长连接订阅事件；
2. 凭证动态读取与热重载：从 ~/.config/feishu-bot/credentials.env 读取凭证，支持运行中热更新；
3. 原生请求级 uuid 幂等：所有出站消息携带确定性 UUID 幂等键，重试天然安全；
4. 入站连发防打碎合并：集成 InboundCoalescer（2.0s 静默窗口，6.0s 硬上限，/ 指令即时屏障）；
5. 交互卡片一键审批：收到 card.action.trigger 直连 ApprovalStore 事务完成就地决断并更新卡片；
6. 双模式支持：Core 模式（GatewayBinding）与 Legacy 模式（state/inbox.jsonl, outbox.jsonl）平滑兼容；
7. 纯标准库与轻量依赖解耦：支持测试环境全量 Mock 与零外部公网连通依赖。
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

# 保证项目根路径在 sys.path 中
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# 保证当前模块目录在 sys.path 中，支持直接执行 python gateway.py
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

try:
    import websockets
except ImportError:
    websockets = None

from channel_common import append_jsonl, exclusive_file_lock
from mobile_channels.coalesce import CoalescedInbound, InboundCoalescer, InboundMessageItem
from mobile_channels.gateway import GatewayBinding


def jsonl_text_lines(text: str) -> list[str]:
    """按行切分 JSONL 文本并剔除空行。"""
    return [ln for ln in text.splitlines() if ln.strip()]


# 导入卡片构建与事件处理
try:
    from .feishu_cards import (
        build_approval_card,
        build_decided_card,
        build_message_card,
        build_thinking_card,
        handle_card_action,
    )
except ImportError:
    from feishu_cards import (
        build_approval_card,
        build_decided_card,
        build_message_card,
        build_thinking_card,
        handle_card_action,
    )

LOGGER = logging.getLogger("feishu_gateway")

BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
INBOX_FILE = STATE_DIR / "inbox.jsonl"
OUTBOX_FILE = STATE_DIR / "outbox.jsonl"
OUTBOX_RESULTS_FILE = STATE_DIR / "outbox_results.jsonl"
OUTBOX_OFFSET_FILE = STATE_DIR / "outbox.offset"
LOCK_FILE = STATE_DIR / "gateway.lock"
STATUS_FILE = STATE_DIR / "status.json"

APPROVAL_RELAY_DIR = REPO_ROOT / "approval-relay"
DECISIONS_LOCK = APPROVAL_RELAY_DIR / "decisions.lock"
DECISIONS_FILE = APPROVAL_RELAY_DIR / "decisions.jsonl"
RELAY_STATE_FILE = APPROVAL_RELAY_DIR / "state.json"


def record_approval_decision_to_relay(
    action_value: Mapping[str, Any] | str,
    operator_id: str,
    chat_id: str = "",
) -> bool:
    """将移动端飞书卡片决断以排他锁安全写入 approval-relay/decisions.jsonl。

    由 relay 主循环统一向 Muse 平台提交权威决断（egress.approval.decide），
    解决仅本地 SQLite 改态导致的虚假批准断层。
    """
    if isinstance(action_value, str):
        try:
            payload = json.loads(action_value)
        except Exception:
            payload = {}
    elif isinstance(action_value, Mapping):
        payload = dict(action_value)
    else:
        payload = {}

    decision = str(payload.get("action") or payload.get("decision") or "").strip()
    approval_id = str(payload.get("approval_id") or "").strip()
    num = str(payload.get("num") or "").strip()

    if not decision or decision not in ("allow_once", "allow_always", "deny"):
        return False

    # 若未显式携带 num，尝试从 approval-relay/state.json 中反查 num
    if not num and approval_id and RELAY_STATE_FILE.is_file():
        try:
            st = json.loads(RELAY_STATE_FILE.read_text(encoding="utf-8"))
            for item_num, it in (st.get("items") or {}).items():
                if it.get("approval_id") == approval_id:
                    num = str(item_num)
                    break
        except Exception as err:
            LOGGER.warning("读取 approval-relay 状态反查 num 异常: %s", err)

    if not num and approval_id:
        num = approval_id

    if not num:
        return False

    decision_row = {
        "num": str(num),
        "approval_id": str(approval_id),
        "decision": decision,
        "channel": "feishu",
        "operator_id": operator_id,
        "chat_id": chat_id,
        "ts": time.time(),
    }

    try:
        DECISIONS_LOCK.parent.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(DECISIONS_LOCK):
            offset = 0
            offset_file = APPROVAL_RELAY_DIR / "decisions.offset"
            if offset_file.is_file():
                try:
                    offset = int(offset_file.read_text(encoding="utf-8").strip())
                except Exception:
                    offset = 0
            if DECISIONS_FILE.is_file():
                try:
                    with open(DECISIONS_FILE, "rb") as f:
                        f.seek(max(0, offset))
                        raw = f.read().decode("utf-8", "replace")
                        for line in jsonl_text_lines(raw):
                            try:
                                r = json.loads(line)
                                if str(r.get("num")) == str(num) and r.get("decision") == decision:
                                    return True
                            except Exception:
                                continue
                except Exception:
                    pass
            append_jsonl(DECISIONS_FILE, decision_row)
            return True
    except Exception as err:
        LOGGER.error("写入 decisions.jsonl 失败: %s", err)
        return False


DEFAULT_CRED_FILE = Path.home() / ".config" / "feishu-bot" / "credentials.env"
OPEN_FEISHU_BASE = "https://open.feishu.cn/open-apis"


def parse_env_file(path: Path) -> dict[str, str]:
    """解析简单的 KEY=VALUE 环境变量文件。"""
    if not path.is_file():
        return {}
    res: dict[str, str] = {}
    try:
        content = path.read_text(encoding="utf-8")
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k:
                    res[k] = v
    except OSError as err:
        LOGGER.warning("读取凭证文件失败: %s (%s)", path, err)
    return res


class FeishuCredentials:
    """飞书凭证管理器，支持运行时热重载与 Token 生命周期维护。"""

    def __init__(self, cred_file: Path | None = None):
        self.cred_file = (
            Path(cred_file).expanduser().resolve()
            if cred_file
            else Path(os.environ.get("FEISHU_CRED_FILE", str(DEFAULT_CRED_FILE))).expanduser().resolve()
        )
        self.app_id: str = ""
        self.app_secret: str = ""
        self.verification_token: str = ""
        self.encrypt_key: str = ""
        self.tenant_access_token: str = ""
        self.token_expires_at: float = 0.0
        self.reload()

    def reload(self) -> None:
        """从文件或环境变量热重载凭证。"""
        data = parse_env_file(self.cred_file)
        self.app_id = data.get("FEISHU_APP_ID") or os.environ.get("FEISHU_APP_ID", "")
        self.app_secret = data.get("FEISHU_APP_SECRET") or os.environ.get("FEISHU_APP_SECRET", "")
        self.verification_token = data.get("FEISHU_VERIFICATION_TOKEN") or os.environ.get("FEISHU_VERIFICATION_TOKEN", "")
        self.encrypt_key = data.get("FEISHU_ENCRYPT_KEY") or os.environ.get("FEISHU_ENCRYPT_KEY", "")

    def is_valid(self) -> bool:
        """检查凭证是否完备。"""
        return bool(self.app_id.strip() and self.app_secret.strip())


class FeishuGateway:
    """飞书长连接网关核心守护类。"""

    def __init__(
        self,
        cred_file: Path | None = None,
        clock: Callable[[], float] | None = None,
        transport_sender: Callable[..., Any] | None = None,
        ws_url: str | None = None,
        ws_connect_factory: Callable[[str], Any] | None = None,
    ):
        """初始化飞书网关。

        :param cred_file: 可选凭证文件路径
        :param clock: 可选时钟注入
        :param transport_sender: 可选传输层发送方法注入（用于测试和沙箱 Mock）
        :param ws_url: 可选静态 WebSocket 连接 URL 覆盖
        :param ws_connect_factory: 可选 WebSocket 连接工厂注入（用于沙箱与单测 Mock）
        """
        self._clock = clock or time.time
        self.credentials = FeishuCredentials(cred_file)
        self._transport_sender = transport_sender
        self._ws_url = ws_url
        self._ws_connect_factory = ws_connect_factory
        self._lock_fd = None
        self._running = False
        self._stopping = False
        self.connected = False
        self.state = "idle"
        self.last_error = ""
        self.ws = None
        self._token_lock = asyncio.Lock()
        self._inbound_count = 0
        self._outbound_count = 0
        self._custom_approval_store = None

        # 初始化 Core 模式绑定
        self._core_binding = GatewayBinding("feishu")
        self.is_core_mode = self._core_binding.enabled

        # 初始化入站连发防打碎合并器（2.0s 空闲间隔，6.0s 最大等待）
        self.coalescer = InboundCoalescer(idle_gap_secs=2.0, max_wait_secs=6.0)

        # 内存已处理消息 ID 去重缓存（避免网络重复帧处理）
        self._seen_message_ids: set[str] = set()

        # 飞书出站请求历史统计（按 uuid 索引）
        self._sent_requests: dict[str, dict[str, Any]] = {}

        # 允许发送者名单与管理员名单
        self.allowed_senders: set[str] = set()
        self.administrators: set[str] = set()
        self._load_access_control()

    def _load_access_control(self) -> None:
        """加载权限控制名单。优先从 Core 模式设置中读取，其次从环境变量读取。"""
        if self.is_core_mode and self._core_binding.settings:
            settings = self._core_binding.settings
            self.allowed_senders = set(settings.get("allowed_senders", []))
            self.administrators = set(settings.get("administrators", []))
        else:
            raw_allowed = os.environ.get("FEISHU_ALLOWED_SENDERS", "")
            self.allowed_senders = {s.strip() for s in raw_allowed.split(",") if s.strip()}
            raw_admins = os.environ.get("FEISHU_ADMINISTRATORS", "")
            self.administrators = {s.strip() for s in raw_admins.split(",") if s.strip()}

    def acquire_instance_lock(self) -> None:
        """获取实例排他文件锁，杜绝多进程冲突。"""
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        lock_file = STATE_DIR / "gateway.lock"
        fd = open(lock_file, "a+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fd.close()
            raise RuntimeError(f"另一个飞书网关实例已持有锁: {lock_file}")
        fd.seek(0)
        fd.truncate()
        fd.write(str(os.getpid()))
        fd.flush()
        self._lock_fd = fd

    def release_instance_lock(self) -> None:
        """释放实例排他文件锁。"""
        if self._lock_fd:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                self._lock_fd.close()
            except Exception:
                pass
            self._lock_fd = None

    def write_status(self) -> None:
        """将网关运行时健康状态原子刷写到 state/status.json。"""
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            status_data = {
                "channel": "feishu",
                "mode": "core" if self.is_core_mode else "legacy",
                "gateway_running": self._running,
                "connected": self.connected,
                "state": self.state,
                "pid": os.getpid(),
                "app_id": self.credentials.app_id,
                "inbound_count": self._inbound_count,
                "outbound_count": self._outbound_count,
                "pending_coalesce_count": self.coalescer.pending_count(),
                "last_error": self.last_error,
                "updated_at": self._clock(),
            }
            status_file = STATE_DIR / "status.json"
            tmp_file = STATE_DIR / f"status.json.tmp.{os.getpid()}"
            tmp_file.write_text(json.dumps(status_data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_file.replace(status_file)
        except Exception as err:
            LOGGER.warning("刷写 status.json 失败: %s", err)

    # ---------- 飞书开放平台 OpenAPI 鉴权与交互 ----------

    async def get_tenant_access_token(self, force_refresh: bool = False) -> str:
        """获取并自动维护 tenant_access_token 的生命周期（带 Lock 防并发击穿）。"""
        now = self._clock()
        if (
            not force_refresh
            and self.credentials.tenant_access_token
            and self.credentials.token_expires_at > now + 300
        ):
            return self.credentials.tenant_access_token

        async with self._token_lock:
            now = self._clock()
            if (
                not force_refresh
                and self.credentials.tenant_access_token
                and self.credentials.token_expires_at > now + 300
            ):
                return self.credentials.tenant_access_token

            if not self.credentials.is_valid():
                raise ValueError("飞书凭证缺失或未配置 (FEISHU_APP_ID / FEISHU_APP_SECRET)")

            # 若注入了 Mock 发送器，优先通过注入器获取
            if self._transport_sender:
                token_resp = await self._invoke_sender(
                    "POST",
                    "/auth/v3/tenant_access_token/internal",
                    body={
                        "app_id": self.credentials.app_id,
                        "app_secret": self.credentials.app_secret,
                    },
                )
            else:
                token_resp = await asyncio.to_thread(self._fetch_token_sync)

            code = token_resp.get("code")
            if code != 0 and "tenant_access_token" not in token_resp:
                raise RuntimeError(f"获取飞书 tenant_access_token 失败: {token_resp}")

            self.credentials.tenant_access_token = token_resp["tenant_access_token"]
            expire_secs = float(token_resp.get("expire", 7200))
            self.credentials.token_expires_at = now + expire_secs
            return self.credentials.tenant_access_token

    def _fetch_token_sync(self) -> dict[str, Any]:
        """通过同步标准库 HTTP 获取 token（运行在工作线程中）。"""
        url = f"{OPEN_FEISHU_BASE}/auth/v3/tenant_access_token/internal"
        payload = json.dumps(
            {
                "app_id": self.credentials.app_id,
                "app_secret": self.credentials.app_secret,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.URLError as err:
            LOGGER.error("请求飞书 Token 网络异常: %s", err)
            return {"code": -1, "msg": str(err)}

    async def _invoke_sender(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """调用传输层发送（优先调用注入的 mock 发送器，兜底使用标准 HTTP）。"""
        if self._transport_sender:
            res = self._transport_sender(method, path, body=body, query=query, headers=headers)
            if asyncio.iscoroutine(res):
                return await res
            return res

        return await asyncio.to_thread(
            self._http_request_sync, method, path, body, query, headers
        )

    def _http_request_sync(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """通过标准库执行 HTTP 请求。"""
        url = f"{OPEN_FEISHU_BASE}{path}"
        if query:
            url += f"?{urllib.parse.urlencode(query)}"

        req_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            req_headers.update(headers)

        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body else None
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                res_body = resp.read().decode("utf-8")
                return json.loads(res_body)
        except urllib.error.HTTPError as err:
            with err:
                try:
                    err_content = err.read().decode("utf-8")
                    return json.loads(err_content)
                except Exception:
                    return {"code": err.code, "msg": str(err)}
        except urllib.error.URLError as err:
            return {"code": -1, "msg": str(err)}

    # ---------- 原生请求级 uuid 幂等消息发送 ----------

    async def send_message(
        self,
        receive_id: str,
        receive_id_type: str = "chat_id",
        msg_type: str = "text",
        content: str | dict[str, Any] = "",
        *,
        uuid_str: str | None = None,
    ) -> dict[str, Any]:
        """向飞书 OpenAPI 发送消息，携带原生 uuid 参数进行强幂等去重。

        :param receive_id: 接收方 ID（群聊 chat_id 或用户 open_id）
        :param receive_id_type: 'chat_id' | 'open_id' | 'user_id'
        :param msg_type: 'text' | 'interactive' (卡片) 等
        :param content: 消息内容（JSON 字符串或字典）
        :param uuid_str: 唯一幂等 UUID 键
        :return: 飞书 OpenAPI 响应结果字典
        """
        token = await self.get_tenant_access_token()
        effective_uuid = uuid_str or str(uuid.uuid4())

        payload_content = (
            json.dumps(content, ensure_ascii=False)
            if isinstance(content, (dict, list))
            else str(content)
        )

        body: dict[str, Any] = {
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": payload_content,
            "uuid": effective_uuid,
        }

        query = {"receive_id_type": receive_id_type}
        headers = {"Authorization": f"Bearer {token}"}

        self._sent_requests[effective_uuid] = {
            "receive_id": receive_id,
            "msg_type": msg_type,
            "uuid": effective_uuid,
            "timestamp": self._clock(),
        }

        resp = await self._invoke_sender(
            "POST",
            "/im/v1/messages",
            body=body,
            query=query,
            headers=headers,
        )
        if isinstance(resp, dict) and (resp.get("code") in (401, 99991663, 99991668) or resp.get("status") == 401):
            LOGGER.warning("飞书 Token 校验失败 (code=%s)，强制重刷并重试一次...", resp.get("code"))
            self.credentials.tenant_access_token = ""
            new_token = await self.get_tenant_access_token(force_refresh=True)
            headers["Authorization"] = f"Bearer {new_token}"
            resp = await self._invoke_sender(
                "POST",
                "/im/v1/messages",
                body=body,
                query=query,
                headers=headers,
            )
        return resp

    async def send_text(
        self,
        chat_id: str,
        text: str,
        *,
        chat_type: str = "single",
        uuid: str | None = None,
    ) -> dict[str, Any]:
        """向指定聊天发送文本消息（对齐 GatewayBinding 与 CoreDelivery 接口）。"""
        receive_id_type = "chat_id" if chat_type == "group" or chat_id.startswith("oc_") else "open_id"
        content_dict = {"text": text}
        return await self.send_message(
            receive_id=chat_id,
            receive_id_type=receive_id_type,
            msg_type="text",
            content=content_dict,
            uuid_str=uuid,
        )

    async def send_card(
        self,
        chat_id: str,
        card: dict[str, Any],
        *,
        chat_type: str = "single",
        uuid: str | None = None,
    ) -> dict[str, Any]:
        """发送交互式卡片消息。"""
        receive_id_type = "chat_id" if chat_type == "group" or chat_id.startswith("oc_") else "open_id"
        return await self.send_message(
            receive_id=chat_id,
            receive_id_type=receive_id_type,
            msg_type="interactive",
            content=card,
            uuid_str=uuid,
        )

    async def patch_card(
        self,
        message_id: str,
        card: dict[str, Any],
    ) -> dict[str, Any]:
        """就地更新已发送的飞书卡片消息（PATCH /im/v1/messages/{message_id}）。"""
        token = await self.get_tenant_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        body = {
            "content": json.dumps(card, ensure_ascii=False),
        }
        resp = await self._invoke_sender(
            "PATCH",
            f"/im/v1/messages/{message_id}",
            body=body,
            headers=headers,
        )
        if isinstance(resp, dict) and (resp.get("code") in (401, 99991663, 99991668) or resp.get("status") == 401):
            LOGGER.warning("飞书 Token 校验失败 (code=%s)，强制重刷并重试一次...", resp.get("code"))
            self.credentials.tenant_access_token = ""
            new_token = await self.get_tenant_access_token(force_refresh=True)
            headers["Authorization"] = f"Bearer {new_token}"
            resp = await self._invoke_sender(
                "PATCH",
                f"/im/v1/messages/{message_id}",
                body=body,
                headers=headers,
            )
        return resp

    # ---------- 事件解析、连发合并与派发分发 ----------

    async def handle_event(self, event_envelope: dict[str, Any]) -> dict[str, Any]:
        """处理飞书长连接接收到的顶层事件包装帧。

        :param event_envelope: 飞书推送的事件 JSON 结构
        :return: 确认响应字典
        """
        header = event_envelope.get("header") or {}
        event_type = header.get("event_type") or event_envelope.get("type") or ""

        # 1. 消息接收事件：im.message.receive_v1
        if event_type == "im.message.receive_v1":
            event_body = event_envelope.get("event") or {}
            return await self._handle_message_receive(event_body, header)

        # 2. 交互卡片按钮点击触发：card.action.trigger
        if event_type == "card.action.trigger":
            event_body = event_envelope.get("event") or {}
            return await self._handle_card_action_trigger(event_body, header)

        # 其他未捕获事件默认快速确认成功
        return {"code": 0, "msg": "event ignored"}

    async def _handle_message_receive(
        self, event: dict[str, Any], header: dict[str, Any]
    ) -> dict[str, Any]:
        """处理入站消息事件，完成去重、白名单过滤并喂入连发防打碎合并器。"""
        message_data = event.get("message") or {}
        sender_data = event.get("sender") or {}
        sender_ids = sender_data.get("sender_id") or {}

        message_id = str(message_data.get("message_id") or "")
        chat_id = str(message_data.get("chat_id") or "")
        raw_chat_type = str(message_data.get("chat_type") or "p2p")
        chat_type = "single" if raw_chat_type in ("p2p", "single") else "group"

        # 提取发信人标识（优先 open_id，其次 user_id）
        sender_id = str(
            sender_ids.get("open_id") or sender_ids.get("user_id") or ""
        )

        # 网络去重：如果该消息已经处理过，直接幂等返回
        if message_id and message_id in self._seen_message_ids:
            return {"code": 0, "msg": "duplicate message ignored"}

        if message_id:
            self._seen_message_ids.add(message_id)

        # 解析消息文本内容
        raw_content = message_data.get("content") or "{}"
        text_content = ""
        if isinstance(raw_content, str):
            try:
                parsed_c = json.loads(raw_content)
                text_content = parsed_c.get("text", "")
            except (ValueError, TypeError):
                text_content = raw_content
        elif isinstance(raw_content, dict):
            text_content = raw_content.get("text", "")

        # 权限校验：单聊与群聊统一校验 allowed_senders 白名单（若配置了白名单）
        if self.allowed_senders and sender_id not in self.allowed_senders:
            LOGGER.warning("拒绝未授权发信者: %s (chat_type=%s)", sender_id, chat_type)
            return {"code": 403, "msg": "unauthorized sender"}

        # 群聊管理指令鉴权：若群聊中发出 /new, /stop 等管理命令，必须是管理员（Fail-Closed 安全策略）
        clean_text = text_content.strip()
        cmd_first = clean_text.split()[0].lower() if clean_text.split() else ""
        if chat_type == "group" and cmd_first in ("/new", "/stop", "/新会话", "/停止"):
            if not self.administrators or sender_id not in self.administrators:
                LOGGER.warning("群聊非管理员拒绝执行管理指令: %s (%s)", sender_id, cmd_first)
                await self.send_text(
                    chat_id,
                    "⚠️ 该群聊管理命令需要管理员权限，未执行操作。",
                    chat_type="group",
                )
                return {"code": 403, "msg": "admin command rejected"}

        # 构造入站片段
        inbound_item = InboundMessageItem(
            channel="feishu",
            account_id=self.credentials.app_id or "feishu_bot",
            message_id=message_id,
            sender_id=sender_id,
            chat_id=chat_id,
            chat_type=chat_type,
            text=text_content,
            received_at=self._clock(),
        )

        # 喂入合并器
        flushed_items = self.coalescer.feed(inbound_item, now=self._clock())

        # 若是命令屏障或立即冲刷出的消息，直接派发
        for item in flushed_items:
            await self._dispatch_inbound(item)

        # 收到消息自动回复即时打字中反馈（非命令且为首条触发时）
        if not flushed_items and not text_content.strip().startswith("/"):
            await self._send_thinking_feedback_if_needed(chat_id, chat_type, message_id)

        return {"code": 0, "msg": "message accepted"}

    async def _send_thinking_feedback_if_needed(
        self, chat_id: str, chat_type: str, message_id: str
    ) -> None:
        """根据环境配置决定是否发送即时思考中反馈。"""
        if os.environ.get("FEISHU_ENABLE_THINKING_FEEDBACK", "0") == "1":
            thinking_card = build_thinking_card(task_id=message_id)
            await self.send_card(
                chat_id=chat_id,
                card=thinking_card,
                chat_type=chat_type,
                uuid=f"thinking-{message_id}",
            )

    async def _handle_card_action_trigger(
        self, event: dict[str, Any], header: dict[str, Any]
    ) -> dict[str, Any]:
        """处理交互卡片按钮点击回调（鉴权校验通过后再写 decisions.jsonl，严防脏决断）。"""
        operator_info = event.get("operator") or {}
        operator_id = str(
            operator_info.get("open_id") or operator_info.get("user_id") or ""
        )

        action_data = event.get("action") or {}
        action_value = action_data.get("value") or {}
        context = event.get("context") or {}
        open_message_id = str(context.get("open_message_id") or "")
        open_chat_id = str(context.get("open_chat_id") or context.get("chat_id") or "")

        # 1. 尝试调用 ApprovalStore 进行本地 SQLite 事务核销与状态就地变更
        approval_store = None
        is_unknown_item = False
        if self._custom_approval_store is not None:
            approval_store = self._custom_approval_store
            ok, msg, updated_card = handle_card_action(
                approval_store=approval_store,
                action_value=action_value,
                operator_id=operator_id,
                conversation_id=open_chat_id,
            )
            if not ok and "不存在" in msg:
                is_unknown_item = True
        elif self.is_core_mode and hasattr(self._core_binding, "database"):
            from mobile_channels.approvals import ApprovalStore
            from mobile_channels.storage import Store

            with Store(self._core_binding.database) as core_store:
                approval_store = ApprovalStore(core_store)
                ok, msg, updated_card = handle_card_action(
                    approval_store=approval_store,
                    action_value=action_value,
                    operator_id=operator_id,
                    conversation_id=open_chat_id,
                )
            if not ok and "不存在" in msg:
                is_unknown_item = True
        else:
            # 非 Core 模式（Legacy 模式）
            ok, msg, updated_card = True, "审批中转模式", {}

        # 2. 如果本地 ApprovalStore 成功核销，或审批单不在 Core 数据库（属于 relay 托管的中转审批单），写入 decisions.jsonl
        # 注意：若审批单在 Core 数据库存在但鉴权被拦截（如跨会话越权、非授权人员），坚决不向中转队列写入脏决断！
        if ok or is_unknown_item:
            relay_written = record_approval_decision_to_relay(
                action_value, operator_id, chat_id=open_chat_id
            )
            if not ok and is_unknown_item and relay_written:
                ok = True
                decision_str = str(action_value.get("action") or action_value.get("decision") or "unknown")
                msg = f"审批决断已提交至中转队列: {decision_str}"
                approval_id_str = str(action_value.get("approval_id") or action_value.get("num") or "未知审批")
                reason_str = f"由 `{operator_id}` 于飞书移动端卡片即时授权"
                updated_card = build_decided_card(
                    approval_id=approval_id_str,
                    operator_id=operator_id,
                    decision=decision_str,
                    reason=reason_str,
                    tool_name=action_value.get("tool_name"),
                    risk_level=action_value.get("risk_level"),
                )
            elif not updated_card or not self.is_core_mode:
                decision_str = str(action_value.get("action") or action_value.get("decision") or "unknown")
                msg = f"审批决断已提交至中转队列: {decision_str}"
                approval_id_str = str(action_value.get("approval_id") or action_value.get("num") or "未知审批")
                reason_str = f"由 `{operator_id}` 于飞书移动端卡片即时授权"
                updated_card = build_decided_card(
                    approval_id=approval_id_str,
                    operator_id=operator_id,
                    decision=decision_str,
                    reason=reason_str,
                    tool_name=action_value.get("tool_name"),
                    risk_level=action_value.get("risk_level"),
                )

        # 若有对应 message_id，就地 PATCH 更新卡片界面
        if open_message_id and updated_card:
            try:
                await self.patch_card(open_message_id, updated_card)
            except Exception as err:
                LOGGER.warning("PATCH 就地更新卡片失败: %s", err)

        return {
            "code": 0 if ok else -1,
            "msg": msg,
            "card": updated_card,
        }

    async def _dispatch_inbound(self, item: CoalescedInbound) -> None:
        """将合并后的原子入站消息提交至 Core 数据库或 Legacy 文件队列。"""
        if self.is_core_mode:
            self._core_binding.receive(coalesced=item)
        else:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            row = item.to_dict()
            inbox_file = STATE_DIR / "inbox.jsonl"
            with inbox_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # ---------- 连发定时冲刷与长连接生命周期轮询 ----------

    async def coalesce_poll_loop(self) -> None:
        """连发合并超时驱动轮询协程（每 200ms 检查一次 due 到期会话）。"""
        while self._running:
            try:
                now = self._clock()
                ready_items = self.coalescer.due(now=now)
                for item in ready_items:
                    await self._dispatch_inbound(item)
            except asyncio.CancelledError:
                break
            except Exception as err:
                LOGGER.exception("合并器 due 轮询异常: %s", err)
            await asyncio.sleep(0.2)

    # ---------- Legacy 发件箱处理循环 ----------

    async def outbox_loop(self) -> None:
        """Legacy 模式下的发件箱轮询派发循环（消费 state/outbox.jsonl）。"""
        while self._running:
            try:
                outbox_file = STATE_DIR / "outbox.jsonl"
                offset_file = STATE_DIR / "outbox.offset"
                if not outbox_file.is_file():
                    await asyncio.sleep(0.5)
                    continue

                offset = 0
                if offset_file.is_file():
                    try:
                        offset = int(offset_file.read_text(encoding="utf-8").strip())
                    except ValueError:
                        offset = 0

                size = outbox_file.stat().st_size
                if size < offset:
                    offset = 0

                if size == offset:
                    await asyncio.sleep(0.5)
                    continue

                with outbox_file.open("r", encoding="utf-8") as f:
                    f.seek(offset)
                    lines = f.readlines()
                    new_offset = f.tell()

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue

                    await self._dispatch_legacy_outbox_row(row)
                    self._outbound_count += 1

                offset_file.write_text(str(new_offset), encoding="utf-8")
                self.write_status()
            except asyncio.CancelledError:
                break
            except Exception as err:
                LOGGER.exception("Legacy 发件箱派发异常: %s", err)
                await asyncio.sleep(1.0)

    async def _dispatch_legacy_outbox_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """执行单条 Legacy 出站消息的发送，并记录结果到 state/outbox_results.jsonl。"""
        out_id = str(row.get("id") or uuid.uuid4())
        mode = row.get("mode", "send")
        chat_id = str(row.get("chat_id") or row.get("chatid") or "")
        chat_type = str(row.get("chat_type") or "single")
        msg_type = str(row.get("msg_type") or "text")
        text = str(row.get("text") or row.get("content") or "")
        card = row.get("card")

        if (msg_type == "interactive" or mode == "send_card") and card:
            res = await self.send_card(chat_id, card, chat_type=chat_type, uuid=out_id)
        else:
            res = await self.send_text(chat_id, text, chat_type=chat_type, uuid=out_id)

        result_entry = {
            "id": out_id,
            "mode": mode,
            "chat_id": chat_id,
            "result": res,
            "success": isinstance(res, dict) and res.get("code") == 0,
            "timestamp": self._clock(),
        }
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        results_file = STATE_DIR / "outbox_results.jsonl"
        with results_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(result_entry, ensure_ascii=False) + "\n")
        return res

    # ---------- WebSocket 官方长连接支持 ----------

    async def get_ws_url(self) -> str:
        """获取飞书长连接 WSS URL。支持动态握手获取或环境变量/构造参数覆盖。"""
        if self._ws_url:
            return self._ws_url
        env_url = os.environ.get("FEISHU_WS_URL")
        if env_url:
            return env_url

        # 向飞书开放平台请求长连接节点路由
        try:
            resp = await self._invoke_sender(
                "POST",
                "/callback/ws/endpoint",
                body={
                    "AppID": self.credentials.app_id,
                    "AppSecret": self.credentials.app_secret,
                },
            )
            if resp.get("code") == 0 and resp.get("data", {}).get("URL"):
                return resp["data"]["URL"]
        except Exception as err:
            LOGGER.warning("动态获取 WebSocket endpoint 失败，使用默认节点: %s", err)

        return "wss://msg-frontier.feishu.cn/ws/v2"

    async def _process_ws_frame(self, ws: Any, raw_message: str | bytes) -> None:
        """解析并分发从 WebSocket 接收到的原始协议帧。"""
        if isinstance(raw_message, bytes):
            try:
                text = raw_message.decode("utf-8")
            except UnicodeDecodeError:
                LOGGER.warning("收到无法解码的二进制帧，长度: %d", len(raw_message))
                return
        else:
            text = raw_message

        try:
            payload = json.loads(text)
        except Exception:
            if text.strip().lower() == "ping":
                if hasattr(ws, "send"):
                    send_fn = ws.send("pong")
                    if asyncio.iscoroutine(send_fn):
                        await send_fn
            return

        if not isinstance(payload, dict):
            return

        # 检查是否为心跳 ping 帧
        if payload.get("type") == "ping" or payload.get("cmd") == "ping":
            pong_resp = json.dumps({"type": "pong", "timestamp": self._clock()})
            if hasattr(ws, "send"):
                send_fn = ws.send(pong_resp)
                if asyncio.iscoroutine(send_fn):
                    await send_fn
            return

        resp = await self.handle_event(payload)
        self._inbound_count += 1
        self.write_status()

        # 针对交互卡片或携带 req_id 的事件推送，回送确认帧
        header = payload.get("header") or {}
        event_type = header.get("event_type") or payload.get("type") or ""
        msg_id = header.get("event_id") or payload.get("req_id")
        if event_type == "card.action.trigger" or msg_id:
            ack_frame = {
                "code": resp.get("code", 0),
                "msg": resp.get("msg", "ok"),
            }
            if msg_id:
                ack_frame["req_id"] = msg_id
            if "card" in resp:
                ack_frame["card"] = resp["card"]
            try:
                if hasattr(ws, "send"):
                    send_fn = ws.send(json.dumps(ack_frame, ensure_ascii=False))
                    if asyncio.iscoroutine(send_fn):
                        await send_fn
            except Exception as err:
                LOGGER.warning("发送 WebSocket 回执失败: %s", err)

    async def ws_loop(self) -> None:
        """飞书 WebSocket 长连接客户端循环，支持自动重连、心跳保活与事件接收分发。"""
        retry_delay = 1.0
        while self._running:
            if not self.credentials.is_valid():
                self.state = "missing_credentials"
                self.connected = False
                self.write_status()
                await asyncio.sleep(2.0)
                continue

            try:
                ws_url = await self.get_ws_url()
                self.state = "connecting"
                self.write_status()

                # 优先使用注入的 ws_connect_factory
                connect_cm = None
                if self._ws_connect_factory:
                    connect_cm = self._ws_connect_factory(ws_url)
                elif websockets:
                    connect_cm = websockets.connect(
                        ws_url,
                        open_timeout=15,
                        close_timeout=5,
                        ping_interval=30,
                        ping_timeout=10,
                    )

                if connect_cm is None:
                    self.state = "websockets_unavailable"
                    self.write_status()
                    await asyncio.sleep(5.0)
                    continue

                async with connect_cm as ws:
                    self.ws = ws
                    self.connected = True
                    self.state = "connected"
                    self.last_error = ""
                    retry_delay = 1.0  # 稳定建连成功，立即将退避重置为 1.0s
                    self.write_status()
                    # 循环接收帧
                    async for raw_message in ws:
                        if not self._running:
                            break
                        await self._process_ws_frame(ws, raw_message)

                # 会话正常结束或服务端断开连接，等待后重连（带 Full Jitter 随机抖动）
                if self._running:
                    self.connected = False
                    self.state = "disconnected"
                    self.write_status()
                    jittered_delay = random.uniform(0.5 * retry_delay, retry_delay)
                    await asyncio.sleep(jittered_delay)
                    retry_delay = min(retry_delay * 2, 30.0)

            except asyncio.CancelledError:
                break
            except Exception as err:
                self.connected = False
                self.state = "disconnected"
                self.last_error = f"{type(err).__name__}: {err}"
                self.write_status()
                jittered_delay = random.uniform(0.5 * retry_delay, retry_delay)
                LOGGER.warning("飞书 WebSocket 异常断开: %s，将在 %.1fs 后重连", err, jittered_delay)
                await asyncio.sleep(jittered_delay)
                retry_delay = min(retry_delay * 2, 30.0)
            finally:
                self.connected = False
                self.ws = None
                self.write_status()

    async def _supervised_loop(self, name: str, coro_fn: Callable[[], Any]) -> None:
        """独立监督循环：隔离子任务异常，防止单个子任务异常导致全网关联动自杀。"""
        while self._running:
            try:
                await coro_fn()
                if not self._running:
                    break
            except asyncio.CancelledError:
                break
            except Exception as err:
                LOGGER.error("子任务 [%s] 异常退出: %s，将在 1.0s 后自动恢复", name, err)
                if not self._running:
                    break
                await asyncio.sleep(1.0)

    async def start(self) -> None:
        """启动飞书网关服务。"""
        self.acquire_instance_lock()
        self._running = True
        self.state = "starting"
        self.write_status()
        LOGGER.info("飞书网关已启动 (模式: %s)", "Core" if self.is_core_mode else "Legacy")

        # 注册 POSIX 停机信号处理器，平滑冲刷缓冲区
        loop = asyncio.get_running_loop()
        stop_signals = (signal.SIGTERM, signal.SIGINT)
        for sig in stop_signals:
            try:
                loop.add_signal_handler(sig, lambda: self.close())
            except (NotImplementedError, RuntimeError):
                pass

        async def run_poll():
            await self.coalesce_poll_loop()

        async def run_outbound():
            if self.is_core_mode:
                await self._core_binding.delivery_loop(self)
            else:
                await self.outbox_loop()

        async def run_ws():
            await self.ws_loop()

        self._tasks = [
            asyncio.create_task(self._supervised_loop("coalesce_poll", run_poll)),
            asyncio.create_task(self._supervised_loop("outbound", run_outbound)),
            asyncio.create_task(self._supervised_loop("websocket", run_ws)),
        ]

        try:
            # 移除 FIRST_EXCEPTION 联动自杀，所有子任务独立异常恢复
            await asyncio.gather(*self._tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            for t in self._tasks:
                if not t.done():
                    t.cancel()
            self.close()

    def close(self) -> None:
        """优雅停机：强制冲刷剩余缓冲区，释放锁和资源。"""
        if self._stopping:
            return
        self._stopping = True
        self._running = False
        self.connected = False
        self.state = "stopped"

        # 显式取消所有后台任务，解除长连接阻塞挂起
        for t in getattr(self, "_tasks", []):
            if not t.done():
                t.cancel()

        try:
            remaining = self.coalescer.flush_all()
            for item in remaining:
                if self.is_core_mode:
                    self._core_binding.receive(coalesced=item)
                else:
                    STATE_DIR.mkdir(parents=True, exist_ok=True)
                    inbox_file = STATE_DIR / "inbox.jsonl"
                    with inbox_file.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
        except Exception as err:
            LOGGER.warning("优雅停机冲刷异常: %s", err)

        if self._core_binding:
            self._core_binding.close()
        self.write_status()
        self.release_instance_lock()
        LOGGER.info("飞书网关已优雅关闭")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    gateway = FeishuGateway()
    try:
        asyncio.run(gateway.start())
    except (KeyboardInterrupt, SystemExit):
        gateway.close()
