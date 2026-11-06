#!/usr/bin/env python3
"""飞书网关命令行运维与发送测试工具。

支持排队发送消息、卡片、回复用户消息及查看网关运行状态。
当飞书渠道处于 Core 模式时，自动拒绝直接操作 Legacy 发件箱，保障权威数据库一致性。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

# 确保导入路径
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from channel_common import (
    append_jsonl,
    load_json_dict,
    pid_alive,
    read_pid_file,
)

BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
RESULTS = STATE / "outbox_results.jsonl"
STATUS = STATE / "status.json"
LOCK_FILE = STATE / "gateway.lock"


def read_text_arg(args: argparse.Namespace) -> str:
    """读取文本参数或文本文件内容。"""
    if getattr(args, "text_file", None):
        return Path(args.text_file).read_text(encoding="utf-8")
    return getattr(args, "text", "") or ""


def read_card_arg(args: argparse.Namespace) -> dict | None:
    """读取卡片 JSON 文件内容。"""
    if getattr(args, "card_file", None):
        try:
            content = Path(args.card_file).read_text(encoding="utf-8")
            return json.loads(content)
        except Exception as e:
            print(f"读取卡片文件失败: {e}", file=sys.stderr)
            return None
    return None


def main() -> int:
    """主入口函数，先执行渠道模式租约检查，再解析并执行命令。"""
    from mobile_channels.modes import ModeError, lease_from_env

    try:
        mode_lease = lease_from_env("feishu", "legacy")
    except (ModeError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"legacy command disabled: {error}", file=sys.stderr)
        return 2

    parser = argparse.ArgumentParser(
        prog="feishu_cli",
        description="飞书长连接网关运维工具",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # 1. send 子命令
    p_send = sub.add_parser("send", help="主动向群聊或个人发送消息或卡片")
    p_send.add_argument("--chat-id", "--chatid", dest="chat_id", required=True, help="飞书 chat_id 或 open_id")
    p_send.add_argument("--chat-type", default="single", choices=["single", "group"], help="聊天类型 (single / group)")
    p_send.add_argument("--text", help="待发送的纯文本内容")
    p_send.add_argument("--text-file", help="从文件读取待发送内容")
    p_send.add_argument("--card-file", help="待发送的飞书卡片 JSON 文件路径")
    p_send.add_argument("--uuid", help="显式指定的幂等 UUID 键")

    # 2. reply 子命令
    p_reply = sub.add_parser("reply", help="针对特定外部消息进行回复")
    p_reply.add_argument("--msgid", required=True, help="被回复的消息 ID")
    p_reply.add_argument("--chat-id", "--chatid", dest="chat_id", help="会话 chat_id")
    p_reply.add_argument("--text", help="回复文本")
    p_reply.add_argument("--text-file", help="从文件读取回复文本")

    # 3. status 子命令
    sub.add_parser("status", help="查看网关进程与连接状态")

    # 4. inbox / outbox / results 队列查看子命令
    for queue_name in ("inbox", "outbox", "results"):
        pq = sub.add_parser(queue_name, help=f"查看最新 {queue_name} 记录")
        pq.add_argument("--limit", type=int, default=10, help="返回最新记录条数")

    args = parser.parse_args()

    # 处理 status 命令
    if args.cmd == "status":
        pid = read_pid_file(LOCK_FILE) if LOCK_FILE.exists() else None
        alive = pid_alive(pid) if pid is not None else False
        status_info = {
            "channel": "feishu",
            "gateway_running": alive,
            "pid": pid,
            "lock_file": str(LOCK_FILE),
            "state_dir": str(STATE),
        }
        if STATUS.is_file():
            try:
                status_info["runtime_status"] = json.loads(STATUS.read_text(encoding="utf-8"))
            except Exception:
                status_info["runtime_status"] = "corrupted"
        print(json.dumps(status_info, ensure_ascii=False, indent=2))
        return 0

    # 处理队列查看命令
    if args.cmd in ("inbox", "outbox", "results"):
        target_path = {
            "inbox": INBOX,
            "outbox": OUTBOX,
            "results": RESULTS,
        }[args.cmd]
        if not target_path.is_file():
            print("(empty)")
            return 0
        try:
            lines = target_path.read_text(encoding="utf-8").splitlines()[-args.limit:]
            for line in lines:
                print(line)
        except OSError as e:
            print(f"读取队列失败: {e}", file=sys.stderr)
            return 1
        return 0

    # 处理 send 命令
    if args.cmd == "send":
        text = read_text_arg(args)
        card = read_card_arg(args)
        if not text and not card:
            print("错误: 必须提供 --text、--text-file 或 --card-file 载荷", file=sys.stderr)
            return 2

        outbox_row: dict = {
            "id": args.uuid or str(uuid.uuid4()),
            "channel": "feishu",
            "mode": "send",
            "chat_id": args.chat_id,
            "chat_type": args.chat_type,
            "timestamp": time.time(),
        }
        if card:
            outbox_row["msg_type"] = "interactive"
            outbox_row["card"] = card
        else:
            outbox_row["msg_type"] = "text"
            outbox_row["text"] = text

        STATE.mkdir(parents=True, exist_ok=True)
        append_jsonl(OUTBOX, outbox_row)
        print(json.dumps({"queued": outbox_row["id"], "chat_id": args.chat_id}, ensure_ascii=False))
        return 0

    # 处理 reply 命令
    if args.cmd == "reply":
        text = read_text_arg(args)
        if not text:
            print("错误: 回复内容不能为空", file=sys.stderr)
            return 2
        reply_row = {
            "id": str(uuid.uuid4()),
            "channel": "feishu",
            "mode": "reply",
            "msgid": args.msgid,
            "chat_id": getattr(args, "chat_id", None) or "",
            "text": text,
            "timestamp": time.time(),
        }
        STATE.mkdir(parents=True, exist_ok=True)
        append_jsonl(OUTBOX, reply_row)
        print(json.dumps({"queued_reply": reply_row["id"], "msgid": args.msgid}, ensure_ascii=False))
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
