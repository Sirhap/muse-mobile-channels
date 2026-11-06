"""飞书（Feishu/Lark）交互卡片构建器与卡片事件决断处理器。

提供标准化审批交互卡片、🚨 [高危审批] 告警卡片、思考状态卡片以及
卡片点击（card.action.trigger）直连 ApprovalStore 的事务授权决断。
"""
from __future__ import annotations

import json
import time
from typing import Any, Mapping

from mobile_channels.approvals import ApprovalStore, DECISIONS, DecisionClaim
from mobile_channels.models import StateError


def build_approval_card(
    approval_id: str,
    operator_ids: list[str],
    prompt: str,
    *,
    reason: str | None = None,
    is_high_risk: bool = False,
    action_url: str | None = None,
    expires_at: float | None = None,
) -> dict[str, Any]:
    """构建飞书审批交互卡片（支持普通审批与 🚨 [高危审批] 样式）。

    :param approval_id: 权威审批单号
    :param operator_ids: 授权审批操作人列表
    :param prompt: 待审批的操作或命令描述
    :param reason: 审批安全策略判定原因或操作说明
    :param is_high_risk: 是否为高危敏感操作（渲染红色告警标识与警示文案）
    :param action_url: 可选的详情跳转链接
    :param expires_at: 审批截止有效时间戳（秒）
    :return: 飞书卡片 JSON 结构字典
    """
    card_title = "🚨 [高危审批] 敏感操作授权申请" if is_high_risk else "Muse 运维审批申请"
    header_template = "red" if is_high_risk else "blue"

    # 构建 Markdown 主体内容
    operators_str = "、".join(operator_ids) if operator_ids else "未指定（仅限管理员）"
    expires_str = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expires_at))
        if expires_at
        else "长期有效"
    )

    content_lines = [
        f"**审批单号**：`{approval_id}`",
        f"**授权人员**：{operators_str}",
        f"**有效期限**：{expires_str}",
        f"**操作内容**：\n```bash\n{prompt.strip()}\n```",
    ]

    if reason:
        prefix = "⚠️ **安全警示**：" if is_high_risk else "ℹ️ **原因说明**："
        content_lines.append(f"{prefix}{reason.strip()}")

    if is_high_risk:
        content_lines.append("🔴 **警告**：该操作涉及核心基础设施或危险指令，请严格核实后再授权！")

    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": "\n".join(content_lines),
        },
        {"tag": "hr"},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "单次放行 (Once)"},
                    "type": "primary",
                    "value": {
                        "approval_id": approval_id,
                        "action": "allow_once",
                    },
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "永久允许 (Always)"},
                    "type": "danger" if is_high_risk else "primary",
                    "value": {
                        "approval_id": approval_id,
                        "action": "allow_always",
                    },
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "拒绝操作 (Deny)"},
                    "type": "default",
                    "value": {
                        "approval_id": approval_id,
                        "action": "deny",
                    },
                },
            ],
        },
    ]

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": card_title},
            "template": header_template,
        },
        "elements": elements,
    }


def build_decided_card(
    approval_id: str,
    decision: str,
    operator_id: str,
    *,
    reason: str | None = None,
    decided_at: float | None = None,
    tool_name: str | None = None,
    risk_level: str | None = None,
) -> dict[str, Any]:
    """构建审批决断完成后的静态归档卡片（就地更新，隐藏交互按钮）。

    :param approval_id: 审批单号
    :param decision: 决断结果 ('allow_once', 'allow_always', 'deny')
    :param operator_id: 实际操作人 ID
    :param reason: 可选决断附带说明
    :param decided_at: 决断时间戳
    :param tool_name: 可选触发操作工具名
    :param risk_level: 可选安全风险等级
    :return: 飞书卡片 JSON 结构字典
    """
    decision_map = {
        "allow_once": ("✅ 已授权单次执行 (allow_once)", "green"),
        "allow_always": ("🛡️ 已授权永久放行 (allow_always)", "blue"),
        "deny": ("❌ 已拒绝操作 (deny)", "grey"),
    }
    decision_text, template = decision_map.get(decision, (f"决断完成 ({decision})", "grey"))
    ts_str = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(decided_at or time.time())
    )

    lines = [
        f"**审批单号**：`{approval_id}`",
        f"**处理状态**：{decision_text}",
        f"**处理人员**：`{operator_id}`",
        f"**处理时间**：{ts_str}",
    ]
    if tool_name:
        lines.append(f"**操作工具**：`{tool_name}`")
    if risk_level:
        lines.append(f"**风险等级**：`{risk_level}`")
    if reason:
        lines.append(f"**处理说明**：{reason.strip()}")

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"审批结果：{decision_text}"},
            "template": template,
        },
        "elements": [
            {
                "tag": "markdown",
                "content": "\n".join(lines),
            }
        ],
    }


def build_thinking_card(
    task_id: str,
    stage: str = "thinking",
    title: str = "正在思考中...",
) -> dict[str, Any]:
    """构建即时打字机/思考中状态反馈卡片。

    :param task_id: 任务唯一标识
    :param stage: 当前阶段标识
    :param title: 卡片标题提示
    :return: 飞书卡片 JSON 结构字典
    """
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "wathet",
        },
        "elements": [
            {
                "tag": "markdown",
                "content": f"⏳ 任务 `{task_id}` 正在处理中，Muse 正在组织推理与规划，请稍候...",
            }
        ],
    }


def build_message_card(title: str, content: str, template: str = "blue") -> dict[str, Any]:
    """构建标准文本展示卡片。

    :param title: 卡片标题
    :param content: Markdown 格式文本内容
    :param template: 卡片头部主题色
    :return: 飞书卡片 JSON 结构字典
    """
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": template,
        },
        "elements": [
            {
                "tag": "markdown",
                "content": content,
            }
        ],
    }


def handle_card_action(
    approval_store: ApprovalStore,
    action_value: Mapping[str, Any] | str,
    operator_id: str,
    conversation_id: str | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    """接收飞书 card.action.trigger 事件并直连 ApprovalStore 完成事务授权。

    :param approval_store: 核心权威审批存储
    :param action_value: 卡片按钮携带的载荷字典或 JSON 字符串
    :param operator_id: 点击按钮的操作人 ID
    :param conversation_id: 会话 ID（若未提供则从审批记录中反查）
    :return: (是否成功, 提示文本, 就地更新的卡片对象)
    """
    if isinstance(action_value, str):
        try:
            payload = json.loads(action_value)
        except (ValueError, TypeError):
            payload = {}
    elif isinstance(action_value, Mapping):
        payload = dict(action_value)
    else:
        payload = {}

    approval_id = str(payload.get("approval_id") or payload.get("num") or "").strip()
    decision = str(payload.get("action") or payload.get("decision") or "").strip()

    if not approval_id or not decision:
        return (
            False,
            "缺少 approval_id 或 action 参数",
            build_message_card("操作失败", "卡片数据格式异常，未包含有效的审批参数。", "red"),
        )

    if decision not in DECISIONS:
        return (
            False,
            f"未知的决断类型: {decision}",
            build_message_card("操作失败", f"无效的决断类型 `{decision}`。", "red"),
        )

    try:
        current_status = approval_store.status(approval_id)
    except KeyError:
        return (
            False,
            f"审批单 {approval_id} 不存在",
            build_message_card("审批不存在", f"审批单 `{approval_id}` 不存在或已被清理。", "grey"),
        )

    registered_conv_id = current_status.get("conversation_id")
    conv_matched = False
    if conversation_id and registered_conv_id:
        if str(conversation_id).strip() == str(registered_conv_id).strip():
            conv_matched = True
        elif hasattr(approval_store, "connection"):
            # 兼容外部传入 chat_id 进行会话归属验证（通过 conversations 表查询绑定的实际 chat_id）
            try:
                row = approval_store.connection.execute(
                    "SELECT chat_id FROM conversations WHERE id=?",
                    (registered_conv_id,),
                ).fetchone()
                if row and str(row["chat_id"]).strip() == str(conversation_id).strip():
                    conv_matched = True
            except Exception:
                pass
    elif not conversation_id:
        conv_matched = True

    if conversation_id and not conv_matched:
        return (
            False,
            "跨会话操作未授权",
            build_message_card("操作被拦截", "您当前所在的会话与该审批单绑定的授权会话不一致，已被安全拦截。", "red"),
        )
    target_conv_id = registered_conv_id or conversation_id
    if not target_conv_id:
        return (
            False,
            "无法匹配会话 ID",
            build_message_card("操作失败", "无法定位审批所属会话。", "red"),
        )

    # 检查状态与有效期（对齐 Store 权威时钟）
    now = (
        approval_store.store.clock()
        if hasattr(approval_store, "store") and hasattr(approval_store.store, "clock")
        else time.time()
    )
    expires_at = current_status.get("expires_at")
    if expires_at and expires_at <= now:
        return (
            False,
            f"该审批已被处理或已失效（当前已过期）",
            build_message_card("审批已过期", f"审批单 `{approval_id}` 已过期失效，无法再执行操作。", "grey"),
        )

    state = current_status.get("state")
    if state != "pending":
        existing_dec = current_status.get("decision") or state
        existing_op = current_status.get("operator_id") or "未知"
        return (
            False,
            f"该审批已被处理或已失效（当前状态: {state}）",
            build_decided_card(
                approval_id,
                existing_dec if existing_dec in DECISIONS else "deny",
                existing_op,
                reason=f"重复操作无效，该审批单已由 `{existing_op}` 决断为 [{existing_dec}]",
            ),
        )

    # 提交决断事务
    try:
        claim: DecisionClaim = approval_store.claim_decision(
            approval_id=approval_id,
            conversation_id=target_conv_id,
            operator_id=operator_id,
            decision=decision,
        )
        # 飞书卡片实时授权，证据直接记录平台事件与操作人
        evidence = f"feishu_card_click:{operator_id}"
        approval_store.resolve(claim, accepted=True, evidence=evidence)
    except PermissionError as err:
        return (
            False,
            f"无权执行该审批: {err}",
            build_message_card("权限不足", f"您（`{operator_id}`）不在该操作的授权审批人名单中。", "red"),
        )
    except StateError as err:
        # 并发冲突场景：其他操作人抢先完成了决断，反查最新状态并就地呈现已决断卡片
        try:
            latest = approval_store.status(approval_id)
            latest_state = latest.get("state")
            latest_decision = latest.get("decision") or latest_state
            latest_op = latest.get("operator_id") or "其他管理员"
            if latest_state != "pending":
                return (
                    False,
                    f"该审批已被处理或已失效（当前状态: {latest_state}）",
                    build_decided_card(
                        approval_id,
                        latest_decision if latest_decision in DECISIONS else "deny",
                        latest_op,
                        reason=f"重复操作已拦截，该审批单已由 `{latest_op}` 决断为 [{latest_decision}]",
                    ),
                )
        except Exception:
            pass
        return (
            False,
            f"该审批已被处理或已失效: {err}",
            build_message_card("操作无效", f"审批状态已发生变化或已过期: {err}", "grey"),
        )
    except Exception as err:
        return (
            False,
            f"决断提交失败: {err}",
            build_message_card("处理异常", f"审批状态扭转失败: {type(err).__name__} - {err}", "red"),
        )

    updated_card = build_decided_card(
        approval_id,
        decision,
        operator_id,
        reason=f"由 `{operator_id}` 于飞书移动端卡片即时授权",
    )
    return True, f"已成功决断: {decision}", updated_card
