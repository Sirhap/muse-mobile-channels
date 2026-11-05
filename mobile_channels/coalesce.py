"""入站连发防打碎合并器 (Inbound Coalescer)。

在移动即时通讯（微信、企业微信）场景中，用户往往习惯在数秒内连续发送 2~4 条短消息
或多张截图。本模块在消息接入层实现通用的入站静默窗口合并机制，将短时间内的连发
消息与附件合并为单一原子 Task，避免在核心引擎生成多个并发 Task 导致上下文撕裂。

核心规则：
1. 聚合分组键：(channel, account_id, chat_id, sender_id)
2. 静默等待间隔：IDLE_GAP_SECS = 2.0s
3. 最大等待上限：MAX_WAIT_SECS = 6.0s
4. 命令屏障：以 / 开头的控制指令（如 /stop, /new, /ping）立即冲刷该会话挂起的所有消息，
   命令本身零等待独立立即执行。
5. 内容聚合：文本按换行符 \\n 拼接，媒体列表追加合并，保留首条元数据，并在 merged_ids 中记录所有合并的消息 ID。
6. 纯 Python 标准库实现，零外部依赖。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

# 默认静默等待间隔（秒）：用户停顿超过 2 秒判定该段表达结束
IDLE_GAP_SECS: float = 2.0

# 默认最大等待硬上限（秒）：连续发送最长积攒不超过 6 秒，强制冲刷防止饥饿
MAX_WAIT_SECS: float = 6.0


@dataclass
class InboundMessageItem:
    """标准化入站消息片段。"""

    channel: str
    account_id: str
    message_id: str
    sender_id: str
    chat_id: str
    chat_type: str = "single"
    text: str = ""
    context_token: str = ""
    media: list[dict[str, Any]] = field(default_factory=list)
    received_at: float | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> InboundMessageItem:
        """从字典载荷构造标准化入站消息片段。"""
        channel = str(data.get("channel") or "")
        account_id = str(data.get("account_id") or "")
        message_id = str(data.get("message_id") or data.get("msgid") or data.get("id") or "")
        sender_id = str(data.get("sender_id") or data.get("from_userid") or data.get("from_user_id") or "")
        chat_type = str(data.get("chat_type") or data.get("chattype") or "single")
        chat_id = str(data.get("chat_id") or data.get("chatid") or data.get("group_id") or "")
        # 单聊场景下若未显式指定 chat_id，则默认指向发信人
        if not chat_id and chat_type == "single" and sender_id:
            chat_id = sender_id
        text = str(data.get("text") or data.get("content") or "")
        context_token = str(data.get("context_token") or "")
        raw_media = data.get("media")
        if isinstance(raw_media, dict):
            media = [raw_media]
        elif isinstance(raw_media, (list, tuple)):
            media = list(raw_media)
        else:
            media = []

        raw_ts = data.get("received_at") or data.get("timestamp") or data.get("ts") or data.get("create_time")
        received_at: float | None = None
        if raw_ts is not None:
            try:
                received_at = float(raw_ts)
            except (ValueError, TypeError):
                received_at = None

        return cls(
            channel=channel,
            account_id=account_id,
            message_id=message_id,
            sender_id=sender_id,
            chat_id=chat_id,
            chat_type=chat_type,
            text=text,
            context_token=context_token,
            media=media,
            received_at=received_at,
        )


@dataclass
class CoalescedInbound:
    """合并完成后的原子入站任务对象。"""

    channel: str
    account_id: str
    message_id: str
    sender_id: str
    chat_id: str
    chat_type: str = "single"
    text: str = ""
    context_token: str = ""
    media: list[dict[str, Any]] = field(default_factory=list)
    merged_ids: list[str] = field(default_factory=list)
    received_at: float | None = None
    is_command: bool = False

    def to_dict(self) -> dict[str, Any]:
        """导出为标准字典形式。"""
        return {
            "channel": self.channel,
            "account_id": self.account_id,
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "text": self.text,
            "context_token": self.context_token,
            "media": list(self.media),
            "merged_ids": list(self.merged_ids),
            "received_at": self.received_at,
            "is_command": self.is_command,
        }


@dataclass
class _PendingBuffer:
    """单个会话的挂起消息缓冲区。"""

    key: tuple[str, str, str, str]
    items: list[InboundMessageItem] = field(default_factory=list)
    started_at: float = 0.0
    last_at: float = 0.0


def _session_key(item: InboundMessageItem) -> tuple[str, str, str, str]:
    """生成会话分组键：(channel, account_id, chat_id, sender_id)。"""
    return (item.channel, item.account_id, item.chat_id, item.sender_id)


def _is_command_barrier(item: InboundMessageItem) -> bool:
    """检查入站消息是否为控制指令屏障（以 '/' 开头）。"""
    text = item.text.strip() if item.text else ""
    return bool(text.startswith("/"))


class InboundCoalescer:
    """入站静默窗口合并器。

    基于空闲静默窗口（Idle Gap）与最大等待硬上限（Max Wait Cap）算法，
    纯标准库实现，无锁无全局状态污染，支持时钟注入与全量确定性单测。
    """

    def __init__(
        self,
        idle_gap_secs: float = IDLE_GAP_SECS,
        max_wait_secs: float = MAX_WAIT_SECS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """初始化合并器。

        :param idle_gap_secs: 静默等待间隔（秒），默认 2.0s
        :param max_wait_secs: 最大等待硬上限（秒），默认 6.0s
        :param clock: 可注入的时钟函数，默认使用 time.monotonic
        """
        self.idle_gap_secs = float(idle_gap_secs)
        self.max_wait_secs = float(max_wait_secs)
        self._clock = clock or time.monotonic
        self._lock = threading.RLock()
        self._pending: dict[tuple[str, str, str, str], _PendingBuffer] = {}

    def _now(self, now: float | None = None) -> float:
        """获取当前时间戳。"""
        return self._clock() if now is None else float(now)

    def feed(
        self,
        item: InboundMessageItem | dict[str, Any],
        now: float | None = None,
    ) -> list[CoalescedInbound]:
        """接收一条入站消息。

        - 普通消息：暂存进入对应会话缓冲区，重置静默计时器，返回空列表 []；
        - 控制指令（命令屏障）：立即冲刷该会话先前已挂起的所有普通消息，
          随后将该控制指令作为独立项紧随其后返回（零延迟、不积攒）。

        :param item: 入站消息片段对象或字典
        :param now: 可选的显式时间戳（便于测试）
        :return: 已经就绪可派发的 CoalescedInbound 列表
        """
        if isinstance(item, dict):
            msg_item = InboundMessageItem.from_dict(item)
        elif isinstance(item, InboundMessageItem):
            msg_item = item
        else:
            raise TypeError(f"InboundCoalescer.feed 期望 InboundMessageItem 或 dict，实际收到 {type(item).__name__}")

        with self._lock:
            current_time = self._now(now)
            key = _session_key(msg_item)

            # 检查命令屏障
            if _is_command_barrier(msg_item):
                flushed: list[CoalescedInbound] = []
                if key in self._pending:
                    flushed.append(self._emit(key))

                # 命令本身独立出队，不入缓冲区
                cmd_media = list(msg_item.media) if msg_item.media else []
                cmd_inbound = CoalescedInbound(
                    channel=msg_item.channel,
                    account_id=msg_item.account_id,
                    message_id=msg_item.message_id,
                    sender_id=msg_item.sender_id,
                    chat_id=msg_item.chat_id,
                    chat_type=msg_item.chat_type,
                    text=msg_item.text,
                    context_token=msg_item.context_token,
                    media=cmd_media,
                    merged_ids=[msg_item.message_id] if msg_item.message_id else [],
                    received_at=msg_item.received_at or current_time,
                    is_command=True,
                )
                flushed.append(cmd_inbound)
                return flushed

            # 普通消息加入缓冲区
            buf = self._pending.get(key)
            if buf is None:
                buf = _PendingBuffer(
                    key=key,
                    items=[msg_item],
                    started_at=current_time,
                    last_at=current_time,
                )
                self._pending[key] = buf
            else:
                # 重放去重：若当前会话缓冲区中已存在相同外部消息 ID，避免重复堆叠
                if msg_item.message_id and any(it.message_id == msg_item.message_id for it in buf.items):
                    return []
                buf.items.append(msg_item)
                buf.last_at = current_time

            return []

    def due(self, now: float | None = None) -> list[CoalescedInbound]:
        """时间轮询驱动，检查所有会话缓冲区并冲刷到期的会话。

        冲刷条件（满足其一即到期）：
        1. 空闲静默超时：now - last_at >= idle_gap_secs
        2. 最大等待上限：now - started_at >= max_wait_secs

        :param now: 可选的显式时间戳（便于测试）
        :return: 到期的 CoalescedInbound 列表，按首条消息进入顺序排列
        """
        with self._lock:
            current_time = self._now(now)
            ready: list[CoalescedInbound] = []
            for key in list(self._pending.keys()):
                buf = self._pending.get(key)
                if buf is None:
                    continue
                idle_for = current_time - buf.last_at
                waited = current_time - buf.started_at
                if idle_for >= self.idle_gap_secs or waited >= self.max_wait_secs:
                    ready.append(self._emit(key))
            return ready

    def flush_all(self) -> list[CoalescedInbound]:
        """清空并立即冲刷所有挂起的会话（用于优雅停机或强制释放）。

        :return: 所有挂起会话合并后的 CoalescedInbound 列表
        """
        with self._lock:
            ready: list[CoalescedInbound] = []
            for key in list(self._pending.keys()):
                if key in self._pending:
                    ready.append(self._emit(key))
            return ready

    def flush_session(
        self,
        key: tuple[str, str, str, str] | None = None,
        *,
        channel: str | None = None,
        account_id: str | None = None,
        chat_id: str | None = None,
        sender_id: str | None = None,
    ) -> CoalescedInbound | None:
        """冲刷指定单个会话。

        :param key: 会话键 (channel, account_id, chat_id, sender_id)
        :return: 合并项，若该会话无挂起消息则返回 None
        """
        if key is None:
            if channel is not None and account_id is not None and chat_id is not None and sender_id is not None:
                key = (channel, account_id, chat_id, sender_id)
            else:
                raise ValueError("flush_session 需要提供 key 元组或完整的 (channel, account_id, chat_id, sender_id) 参数")
        with self._lock:
            if key in self._pending:
                return self._emit(key)
            return None

    def pending_count(self) -> int:
        """获取当前所有缓冲区中累计挂起的消息总数。"""
        with self._lock:
            return sum(len(buf.items) for buf in self._pending.values())

    def pending_sessions(self) -> list[tuple[str, str, str, str]]:
        """获取当前存在挂起消息的会话键列表。"""
        with self._lock:
            return list(self._pending.keys())

    def session_stats(self, now: float | None = None) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        """获取各会话缓冲区的运行统计信息（用于可观测性排查）。"""
        with self._lock:
            current_time = self._now(now)
            stats: dict[tuple[str, str, str, str], dict[str, Any]] = {}
            for key, buf in self._pending.items():
                stats[key] = {
                    "count": len(buf.items),
                    "started_at": buf.started_at,
                    "last_at": buf.last_at,
                    "idle_for": max(0.0, current_time - buf.last_at),
                    "waited": max(0.0, current_time - buf.started_at),
                }
            return stats

    def __len__(self) -> int:
        """获取当前挂起的会话数。"""
        with self._lock:
            return len(self._pending)

    def _emit(self, key: tuple[str, str, str, str]) -> CoalescedInbound:
        """从挂起缓冲区弹出指定会话并生成合并结果。"""
        buf = self._pending.pop(key)
        first_item = buf.items[0]

        # 1. 收集所有外部消息 ID（保持次序并过滤空值与重复值）
        raw_ids = [item.message_id for item in buf.items if item.message_id]
        merged_ids = list(dict.fromkeys(raw_ids))

        # 2. 合并所有媒体附件
        combined_media: list[dict[str, Any]] = []
        for item in buf.items:
            if item.media:
                combined_media.extend(item.media)

        # 3. 聚合文本内容
        raw_texts = [item.text for item in buf.items if isinstance(item.text, str) and item.text.strip()]
        # 过滤纯占位符，若有实质内容则以实质内容为主；修剪各消息首尾多余换行防止空行
        substantive_texts = [t.rstrip("\r\n") for t in raw_texts if t.strip() != "[附件]"]

        if substantive_texts:
            merged_text = "\n".join(substantive_texts)
        elif raw_texts:
            merged_text = "[附件]"
        elif combined_media:
            # 纯媒体且无文本，赋予合规占位符
            merged_text = "[附件]"
        else:
            merged_text = ""

        # 4. 上下文凭证：优先保留首个非空的 context_token
        context_token = next((item.context_token for item in buf.items if item.context_token), first_item.context_token)

        # 5. 接收时间戳：优先首条消息的 received_at
        received_at = first_item.received_at if first_item.received_at is not None else buf.started_at

        return CoalescedInbound(
            channel=first_item.channel,
            account_id=first_item.account_id,
            message_id=first_item.message_id,
            sender_id=first_item.sender_id,
            chat_id=first_item.chat_id,
            chat_type=first_item.chat_type,
            text=merged_text,
            context_token=context_token,
            media=combined_media,
            merged_ids=merged_ids,
            received_at=received_at,
            is_command=False,
        )
