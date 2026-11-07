"""Account-bound gateway integration; legacy queues never receive core messages."""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path

from .approvals import decision_allowed
from .ingress import CoreIngress
from .models import IngestResult
from .modes import lease_from_env
from .runtime import DeliveryRunner, SendOutcome
from .storage import Store

LOGGER = logging.getLogger(__name__)

# Approval relay handover: the same decisions.jsonl the legacy gateways
# write (see wecom-bot/weixin-bot gateway.py). The relay consumes it by
# byte offset, so core /a /b /c and card clicks must use the identical
# row shape and the identical first-submission-wins dedup.
APPROVAL_RELAY_DIR = Path(os.environ.get(
    "APPROVAL_RELAY_DIR", "/home/hatch/workspace/approval-relay"))
APPROVAL_COMMAND_DECISIONS = {"/a": "allow_once", "/b": "allow_always",
                              "/c": "deny"}
APPROVAL_DECISION_LABELS = {"allow_once": "批准（仅这次）",
                            "allow_always": "批准（永久）",
                            "deny": "拒绝"}
APPROVAL_USAGE = "a=仅这次  b=永久  c=拒绝\n回 /a N｜/b N｜/c N"
APPROVAL_WRITE_FAILED = "提交失败：暂时写不了审批指令，请稍后再试。"
LEGACY_RANDOM_NOTICE_ID = re.compile(r"appr-.+-[0-9a-f]{8}")

# Same shape as approval-relay's APPROVAL_NOTICES_DDL: the relay writes
# pending rows here for core-mode channels, and the core delivery loop
# consumes them. Created by GatewayBinding so the table exists even
# when the relay has never written to this database.
APPROVAL_NOTICES_DDL = (
    "CREATE TABLE IF NOT EXISTS approval_notices ("
    "id TEXT PRIMARY KEY, channel TEXT NOT NULL, "
    "chat_id TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
    "state TEXT NOT NULL DEFAULT 'pending' "
    "CHECK(state IN ('pending','delivered','failed')), "
    "created_at REAL NOT NULL)")

# WeCom outbound media cap for the upload path (upload_media itself
# only refuses past ~50 MiB / 100 chunks; the 10 MiB cap is the
# documented platform limit for a single media message).
WECOM_OUTBOUND_MEDIA_MAX_BYTES = 10 * 1024 * 1024
# Legacy WeChat allows 180s compression, 15s upload negotiation and
# 300s CDN upload per file. Keep the owned delivery lease beyond that work.
WEIXIN_MEDIA_PREPARE_SECONDS = 600


class _PermanentMediaError(Exception):
    """An outbound media item that can never succeed (missing file, over cap)."""


def _approval_items():
    """Pending approval items from the relay state, {} when unreadable."""
    try:
        with open(APPROVAL_RELAY_DIR / "state.json",
                  encoding="utf-8") as source:
            state = json.load(source)
    except (OSError, ValueError):
        return {}
    items = state.get("items") if isinstance(state, dict) else None
    if not isinstance(items, dict):
        return {}
    return {n: it for n, it in items.items() if isinstance(it, dict)}


def _approval_ascii_number(token: str) -> str | None:
    """Unsigned ASCII integer, after NFKC (fullwidth digits fold)."""
    norm = unicodedata.normalize("NFKC", str(token or "")).strip()
    if not norm or not norm.isascii() or not norm.isdigit():
        return None
    return norm


def _approval_pending_submission(num):
    """Decision already written for num but not yet consumed by the relay.

    The relay consumes decisions.jsonl by byte offset and only then flips
    state.json to decided:*, so a decision submitted once still reads
    "pending" until the relay's next cycle. The first submission wins;
    later submits for the same num must not append a duplicate row.
    """
    try:
        off = int((APPROVAL_RELAY_DIR / "decisions.offset")
                  .read_text().strip())
    except (OSError, ValueError):
        off = 0
    try:
        with open(APPROVAL_RELAY_DIR / "decisions.jsonl", "rb") as f:
            f.seek(max(0, off))
            raw = f.read().decode("utf-8", "replace")
    except OSError:
        return None
    for line in raw.split("\n"):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("num")) == str(num) and row.get("decision"):
            return row["decision"]
    return None


@contextmanager
def _decisions_lock():
    """Hold the relay decisions lock for a check-then-append pair."""
    lock_path = APPROVAL_RELAY_DIR / "decisions.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield handle
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def _append_decision_row(row):
    """Append one decision row; False when the file cannot be written."""
    try:
        path = APPROVAL_RELAY_DIR / "decisions.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return True
    except OSError:
        return False


def _approval_decide_ack(num, decision, channel, operator_id="",
                         chat_id=""):
    """Record an authorized decision while the caller holds the decisions lock.

    Same row shape and dedup as the legacy gateways'
    slash_approval_decide_ack, so the relay's consume_decisions can
    consume it unchanged.
    """
    items = _approval_items()
    it = items.get(str(num))
    if it is None:
        return f"没有找到审批 #{num}。"
    decision_row = {"num": str(num), "decision": decision, "channel": channel,
                    "operator_id": operator_id, "chat_id": chat_id,
                    "ts": time.time()}
    if not decision_allowed(it, decision_row, time.time()):
        return "无法提交：操作者、会话或审批有效期未通过验证，请在原审批会话或 Muse 应用里处理。"
    if it.get("status") == "decision_unknown":
        return f"审批 #{num} 的提交结果待核查，请勿重复提交。"
    if it.get("status") != "pending":
        return f"审批 #{num} 已经处理过了。"
    label = APPROVAL_DECISION_LABELS[decision]
    prev = _approval_pending_submission(num)
    if prev is not None:
        prev_label = APPROVAL_DECISION_LABELS.get(prev, prev)
        return (f"审批 #{num} 已提交过（{prev_label}），"
                f"等执行结果就行，不用重复提交。")
    if _append_decision_row(decision_row):
        return f"已提交{label} #{num}：{it.get('who', '')}。审批中转确认后会通知你；当前尚未确认生效。"
    return APPROVAL_WRITE_FAILED


class GatewayBinding:
    """Own one ingress/delivery channel under an explicit coordinated mode lease."""

    def __init__(self, channel: str):
        """Validate mode configuration without affecting unconfigured deployments."""
        self.channel = channel
        self.lease = lease_from_env(channel)
        self.enabled = self.lease is not None and self.lease.mode == 'core'
        if self.enabled:
            self.settings = self.lease.settings
            self.database = Path(self.settings['database'])
            # Activation never silently creates an empty authority over old work.
            if not self.database.is_file():
                raise ValueError('initialize and reconcile the core database before activation')
            from .readiness import ensure_executable_database
            ensure_executable_database(self.database)
            self.account_id = self.settings['account_id']
            with Store(self.database) as store:
                store.connection.execute('''CREATE TABLE IF NOT EXISTS gateway_routes (
                    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
                    external_id TEXT NOT NULL, context_token TEXT NOT NULL DEFAULT '',
                    media_json TEXT NOT NULL DEFAULT '[]',
                    merged_ids_json TEXT NOT NULL DEFAULT '[]')''')
                cols = [c[1] for c in store.connection.execute("PRAGMA table_info(gateway_routes)").fetchall()]
                if 'merged_ids_json' not in cols:
                    store.connection.execute("ALTER TABLE gateway_routes ADD COLUMN merged_ids_json TEXT NOT NULL DEFAULT '[]'")
                self._has_merged_ids_col = True
                store.connection.execute('''CREATE TABLE IF NOT EXISTS gateway_route_externals (
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    external_id TEXT NOT NULL,
                    PRIMARY KEY (task_id, external_id))''')
                store.connection.execute('''CREATE INDEX IF NOT EXISTS idx_gateway_route_externals_ext ON gateway_route_externals(external_id)''')
                store.connection.execute(APPROVAL_NOTICES_DDL)

    def close(self) -> None:
        """Release the mode lease when the gateway process stops."""
        if self.lease:
            self.lease.close()

    def create_coalescer(self, idle_gap_secs: float = 2.0, max_wait_secs: float = 6.0):
        """为当前网关创建一个入站连发防打碎合并器。

        注意：本合并器有意不接线进生产入站路径（2026-11-06 决定，
        选项 B：不接）。理由：(1) _pending 是纯内存字典，网关重启会
        静默丢弃已缓冲未冲刷的消息，补持久化缓冲/关机 flush 是另一套
        crash 一致性工作，半吊子接线比不接更危险；(2) 官方对齐的折叠
        已覆盖连发场景——跑着的回合会并入后续消息，排队消息会随新回
        合折叠，剩下的缺口只是空闲时 2 秒内的连发；(3) 接线会给每条
        消息加 2 秒延迟，收益小于风险。工厂保留，供将来持久化缓冲
        落地后复用。
        """
        from .coalesce import InboundCoalescer
        return InboundCoalescer(idle_gap_secs=idle_gap_secs, max_wait_secs=max_wait_secs)

    def find_task_by_external_id(self, external_id: str, conversation_id: str | None = None) -> str | None:
        """根据外部消息 ID（包括被合并的次级消息 ID）反查权威 task_id。

        外部消息 ID 只在同一会话内唯一（与 Store.ingest 的
        UNIQUE(conversation_id, external_id) 一致），所以去重调用
        （receive）必须传 conversation_id 限定在同一个 conversation
        内；不限定会把另一会话/另一渠道的同 ID 消息误判为重放，直接
        吞掉新任务。无会话作用域的直接反查（例如测试与排查）退回全局
        查找，只作定位、不作去重依据。
        """
        if not external_id or not str(external_id).strip():
            return None
        external_id = str(external_id).strip()
        with Store(self.database) as store:
            if conversation_id:
                row = store.connection.execute(
                    'SELECT r.task_id FROM gateway_route_externals r JOIN tasks t ON t.id=r.task_id '
                    'WHERE r.external_id=? AND t.conversation_id=? LIMIT 1',
                    (external_id, conversation_id)
                ).fetchone()
                if row:
                    return row['task_id']
                row = store.connection.execute(
                    'SELECT r.task_id FROM gateway_routes r JOIN tasks t ON t.id=r.task_id '
                    'WHERE r.external_id=? AND t.conversation_id=? LIMIT 1',
                    (external_id, conversation_id)
                ).fetchone()
                return row['task_id'] if row else None
            row = store.connection.execute(
                'SELECT task_id FROM gateway_route_externals WHERE external_id=? LIMIT 1',
                (external_id,)
            ).fetchone()
            if row:
                return row['task_id']
            row = store.connection.execute(
                'SELECT task_id FROM gateway_routes WHERE external_id=? LIMIT 1',
                (external_id,)
            ).fetchone()
            return row['task_id'] if row else None

    def receive(self, coalesced=None, *, message_id: str | None = None, sender_id: str | None = None,
                chat_id: str | None = None, chat_type: str | None = None, text: str | None = None,
                context_token: str = '', media: list | None = None, merged_ids: list[str] | None = None,
                received_at: float | None = None):
        """Atomically accept content, commands and route metadata, deduplicating replay.

        Unsupported capabilities are explicit replies rather than silent fallback
        to another scheduler. Media metadata is preserved for operator recovery.
        When merged_ids are provided, all associated external IDs are indexed in
        gateway_route_externals and preserved in gateway_routes.
        """
        if coalesced is not None:
            message_id = coalesced.message_id
            sender_id = coalesced.sender_id
            chat_id = coalesced.chat_id
            chat_type = coalesced.chat_type
            text = coalesced.text
            context_token = coalesced.context_token or context_token
            media = coalesced.media if coalesced.media is not None else media
            merged_ids = coalesced.merged_ids if coalesced.merged_ids is not None else merged_ids
            received_at = coalesced.received_at if coalesced.received_at is not None else received_at

        raw_ids = (merged_ids or ([message_id] if message_id is not None else []))
        all_merged_ids: list[str] = []
        for mid in raw_ids:
            s = str(mid).strip()
            if s and s not in all_merged_ids:
                all_merged_ids.append(s)
        if message_id is not None and str(message_id).strip() and str(message_id).strip() not in all_merged_ids:
            all_merged_ids.insert(0, str(message_id).strip())

        # 幂等性防御：检查主消息 ID 以及所有关联合并 ID 是否已被索引（包含作为次级合并消息），若已存在直接返回既有 Task。
        # 作用域限定在当前会话：外部 ID 只在会话内唯一，跨会话/跨渠道的同 ID 是不同消息，不得互相吞掉。
        # 会话行尚不存在时（全新会话）不可能已有任务，直接跳过预查、走 CoreIngress 建会话；
        # 这里绝不能退回全局反查，否则另一会话的同 ID 会把新消息误判为重放。
        conversation_id = None
        with Store(self.database) as store:
            conv = store.connection.execute(
                'SELECT id FROM conversations WHERE channel=? AND account_id=? AND chat_type=? AND chat_id=?',
                (self.channel, self.account_id, str(chat_type or 'single'), str(chat_id or '')),
            ).fetchone()
            conversation_id = conv['id'] if conv else None
        if conversation_id is not None:
            for mid in all_merged_ids:
                existing_task_id = self.find_task_by_external_id(mid, conversation_id)
                if existing_task_id is not None:
                    with Store(self.database) as store:
                        row = store.connection.execute('SELECT conversation_id FROM tasks WHERE id=?', (existing_task_id,)).fetchone()
                        conv_id = row['conversation_id'] if row else ''
                    return IngestResult(conv_id, existing_task_id, False)

        tokens = unicodedata.normalize("NFKC", text or "").lower().split()
        with ExitStack() as stack:
            approval_write_failed = False
            if (len(tokens) > 1 and tokens[0] in APPROVAL_COMMAND_DECISIONS
                    and _approval_ascii_number(tokens[1]) is not None):
                # The relay takes the decisions lock before writing its
                # SQLite notices. Use the same order for slash decisions.
                try:
                    stack.enter_context(_decisions_lock())
                except OSError:
                    approval_write_failed = True
            store = stack.enter_context(Store(self.database))
            stack.enter_context(store.transaction())
            ingress = CoreIngress(store, self.channel, self.account_id, self.settings['allowed_senders'])
            effective_text = text if (isinstance(text, str) and text.strip()) else ('[附件]' if media else (text or ''))
            item = ingress.receive(
                str(message_id),
                str(sender_id or ''),
                str(chat_id or ''),
                str(chat_type or 'single'),
                effective_text,
                received_at=received_at,
            )
            if not item.created:
                return item

            # 写入 gateway_routes
            if getattr(self, '_has_merged_ids_col', True):
                store.connection.execute(
                    'INSERT INTO gateway_routes (task_id, external_id, context_token, media_json, merged_ids_json) VALUES (?,?,?,?,?)',
                    (item.task_id, str(message_id), context_token, json.dumps(media or [], ensure_ascii=False), json.dumps(all_merged_ids, ensure_ascii=False))
                )
            else:
                store.connection.execute(
                    'INSERT INTO gateway_routes (task_id, external_id, context_token, media_json) VALUES (?,?,?,?)',
                    (item.task_id, str(message_id), context_token, json.dumps(media or [], ensure_ascii=False))
                )

            # 记录所有关联外部消息 ID 至 gateway_route_externals 索引表
            for ext_id in all_merged_ids:
                store.connection.execute(
                    'INSERT OR IGNORE INTO gateway_route_externals (task_id, external_id) VALUES (?,?)',
                    (item.task_id, ext_id)
                )

            reply = (APPROVAL_WRITE_FAILED if approval_write_failed
                     else self._command(store, item, tokens, media))
            if reply is not None:
                store.complete_command(item.task_id, reply)
            return item

    # Image suffixes the Native track accepts (same set as
    # executors.IMAGE_MIME_TYPES) and its per-image cap. The gate below
    # mirrors build_stream_items so a rejection is explicit here instead
    # of a failed submit there.
    _MEDIA_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
    _MAX_IMAGE_BYTES = 2 * 1024 * 1024

    def _media_gate_reply(self, media: list | None) -> str | None:
        """Refuse media the executors cannot run, naming the reason.

        Images pass (Native sends them; size/mime are checked here).
        Video and files have no Native image item and Gadget is
        text-only, so they are refused outright rather than silently
        dropped. A whole request is refused when any item is unusable,
        so the user never gets a half-executed turn.
        """
        if not media:
            return None
        problems: list[str] = []
        for entry in media:
            if not isinstance(entry, dict):
                problems.append("未知附件暂不支持执行")
                continue
            kind = str(entry.get("kind") or entry.get("type") or "").lower()
            filename = str(entry.get("filename") or entry.get("name") or "附件")
            if kind == "image":
                path = entry.get("path") or entry.get("local_path") or ""
                if not path:
                    return (f"图片「{filename}」还没下载到本地，未执行该请求，"
                            "请再发一次。")
                suffix = Path(str(path)).suffix.lower()
                if suffix not in self._MEDIA_IMAGE_SUFFIXES:
                    problems.append(f"图片「{filename}」格式 {suffix or '未知'} 不支持")
                    continue
                try:
                    size = Path(str(path)).stat().st_size
                except OSError:
                    return (f"图片「{filename}」本地文件无法读取，未执行该请求，"
                            "请再发一次。")
                if size > self._MAX_IMAGE_BYTES:
                    return (f"图片「{filename}」有 {size} 字节，超过 2 MiB 上限，"
                            "未执行；请发一张小一点的图片。")
            elif kind == "video":
                problems.append(f"视频「{filename}」暂不支持执行")
            elif kind == "file":
                problems.append(f"文件「{filename}」暂不支持执行")
            elif kind == "voice":
                problems.append("语音暂不支持执行")
            elif kind:
                problems.append(f"{kind}「{filename}」暂不支持执行")
            else:
                problems.append(f"「{filename}」类型未知，暂不支持执行")
        if problems:
            return "；".join(problems) + "，未执行该请求。"
        return None

    def _task_counts(self, store, conversation_id, exclude_task_id):
        """State counts for one conversation, excluding the command task itself."""
        rows = store.connection.execute(
            "SELECT state,COUNT(*) AS n FROM tasks WHERE conversation_id=? AND id!=? GROUP BY state",
            (conversation_id, exclude_task_id)).fetchall()
        return {row["state"]: row["n"] for row in rows}

    def _active_rows(self, store, item):
        """Queued/claimed/running/reconciling tasks in this chat, oldest first."""
        return store.connection.execute(
            "SELECT t.id, t.state, m.text FROM tasks t JOIN inbound_messages m ON m.id=t.message_id "
            "WHERE t.conversation_id=? AND t.id!=? AND t.state IN ('queued','claimed','running','reconciling') "
            "ORDER BY t.sequence",
            (item.conversation_id, item.task_id)).fetchall()

    def _ping_reply(self, store, item) -> str:
        """Pong plus live status and the most recent failure evidence."""
        counts = self._task_counts(store, item.conversation_id, item.task_id)
        queued = counts.get("queued", 0)
        active = sum(counts.get(state, 0) for state in ("claimed", "running", "reconciling"))
        failed = counts.get("failed", 0)
        last_error = store.connection.execute(
            "SELECT evidence FROM task_events WHERE task_id IN "
            "(SELECT id FROM tasks WHERE conversation_id=?) "
            "AND event='confirmed_failed' AND evidence IS NOT NULL AND evidence<>'' "
            "ORDER BY created_at DESC LIMIT 1",
            (item.conversation_id,)).fetchone()
        text = f"pong ✅ 网关在线，排队 {queued} 个，执行中 {active} 个，失败 {failed} 个。"
        if last_error is not None:
            text += f"最近失败：{str(last_error['evidence'])[:80]}"
        else:
            text += "最近无失败记录。"
        return text

    def _status_reply(self, store, item) -> str:
        """Connection + task counts; distinct from the /queue detail list."""
        counts = self._task_counts(store, item.conversation_id, item.task_id)
        queued = counts.get("queued", 0)
        active = sum(counts.get(state, 0) for state in ("claimed", "running", "reconciling"))
        done = counts.get("succeeded", 0)
        failed = counts.get("failed", 0)
        cancelled = counts.get("cancelled", 0)
        mode = "core" if self.enabled else "未启用"
        return (f"网关在线（{mode} 模式）。当前聊天任务：排队 {queued} 个，"
                f"执行中 {active} 个，已完成 {done} 个，失败 {failed} 个，"
                f"已取消 {cancelled} 个。")

    def _queue_reply(self, store, item) -> str:
        """One line per live task with its queue position and a text snippet."""
        rows = self._active_rows(store, item)
        if not rows:
            return "当前没有排队或正在跑的任务。"
        labels = {"queued": "排队中", "claimed": "已认领", "running": "执行中", "reconciling": "对账中"}
        lines = ["当前聊天排队明细："]
        queue_index = 0
        for row in rows:
            prefix = "-"
            if row["state"] == "queued":
                queue_index += 1
                prefix = f"{queue_index}."
            snippet = (row["text"] or "").strip().split("\n")[0][:24]
            lines.append(f"{prefix} [{labels.get(row['state'], row['state'])}] {snippet}")
        return "\n".join(lines)

    def _queue_clear_reply(self, store, item) -> str:
        """Cancel every queued task in this chat (running ones are untouched)."""
        queued = [row for row in self._active_rows(store, item) if row["state"] == "queued"]
        for row in queued:
            store.request_cancel(row["id"], item.conversation_id)
        if not queued:
            return "排队里没有可取消的任务。"
        return f"已取消 {len(queued)} 个排队任务。"

    def _queue_drop_reply(self, store, item, args) -> str:
        """Drop the Nth queued task (1-based, oldest first)."""
        queued = [row for row in self._active_rows(store, item) if row["state"] == "queued"]
        num = _approval_ascii_number(args[0]) if args else None
        if num is None or not 1 <= int(num) <= len(queued):
            return (f"排队里只有 {len(queued)} 个任务，"
                    "用法：/queue drop N，N 要在 1～排队任务数之间。")
        row = queued[int(num) - 1]
        store.request_cancel(row["id"], item.conversation_id)
        snippet = (row["text"] or "").strip().split("\n")[0][:24]
        return f"已丢弃排队第 {num} 个任务：{snippet}"

    def _stop_reply(self, store, item) -> str:
        """Split the wording by what actually happened.

        Queued tasks cancel synchronously inside request_cancel, so they
        are reported as already cancelled; live tasks only get a cancel
        request the executor must confirm. Zero live tasks says so
        plainly instead of claiming "0 个活动任务" were registered.
        """
        rows = self._active_rows(store, item)
        queued = [row for row in rows if row["state"] == "queued"]
        active = [row for row in rows if row["state"] != "queued"]
        for row in rows:
            store.request_cancel(row["id"], item.conversation_id)
        if not rows:
            return "当前没有正在跑的任务。"
        parts = []
        if queued:
            parts.append(f"已立即取消 {len(queued)} 个排队任务")
        if active:
            parts.append(f"已登记 {len(active)} 个正在跑的任务的取消请求，等待执行方确认")
        return "；".join(parts) + "。"

    def _check_reply(self, store) -> str:
        """A real self-check: each item is probed, not asserted.

        The executor item probes the executor's persistent state
        (execution_attempts readable); it is not a liveness probe of a
        runner process, and the wording says record-table, not "alive".
        """
        results = []
        if self.enabled and self.lease is not None:
            results.append(("网关连接", True, "core 模式已启用"))
        else:
            results.append(("网关连接", False, "core 未启用"))
        try:
            store.connection.execute("SELECT 1").fetchone()
            results.append(("core 库", True, "可读"))
        except Exception as exc:
            results.append(("core 库", False, f"不可读：{exc}"[:80]))
        try:
            count = store.connection.execute(
                "SELECT COUNT(*) FROM execution_attempts").fetchone()[0]
            results.append(("执行器", True, f"执行记录表可读，共 {count} 条执行记录"))
        except Exception as exc:
            results.append(("执行器", False, f"执行记录表不可读：{exc}"[:80]))
        try:
            pending = store.connection.execute(
                "SELECT COUNT(*) FROM approval_notices WHERE state='pending'").fetchone()[0]
            results.append(("审批通知", True, f"{pending} 条待发送"))
        except Exception:
            results.append(("审批通知", False, "审批通知表不可读"))
        lines = ["自检结果："]
        for name, ok, detail in results:
            lines.append(f"{name}：{'通过' if ok else '失败'}（{detail}）")
        return "\n".join(lines)

    def _command(self, store, item, tokens: list[str], media: list | None = None) -> str | None:
        """Execute only scoped core commands; never call global legacy administration.

        Commands are parsed as (command word, args), never exact
        full-string matched, so "/queue drop 2" and extra spaces work.
        A message with media is gated only when it is not a command:
        a command with an attachment still runs the command.
        """
        word = tokens[0] if tokens else ""
        args = tokens[1:]
        if word in ("/new", "/新会话", "/stop", "/停止") or (
                word in ("/queue", "/队列") and args and args[0] in ("clear", "drop")):
            scope = store.connection.execute(
                'SELECT c.chat_type,m.sender_id FROM tasks t JOIN conversations c ON c.id=t.conversation_id '
                'JOIN inbound_messages m ON m.id=t.message_id WHERE t.id=?', (item.task_id,)).fetchone()
            if scope["chat_type"] == "group" and scope["sender_id"] not in self.settings.get("administrators", []):
                return "该群聊管理命令需要管理员权限，未修改任务或上下文。"
        if word in ("/ping", "ping"):
            return self._ping_reply(store, item)
        if word in ("/new", "/新会话"):
            store.new_generation(item.conversation_id)
            return "已开启新会话。"
        if word in ("/stop", "/停止"):
            return self._stop_reply(store, item)
        if word in ("/help", "/帮助"):
            return ("可用命令：\n"
                    "/ping 查看网关状态\n"
                    "/status 任务与连接状态\n"
                    "/queue 查看排队明细\n"
                    "/queue clear 清空本会话排队任务\n"
                    "/queue drop N 丢弃排队第 N 个任务\n"
                    "/check 自检（网关连接、core 库、执行器）\n"
                    "/new 开启新会话\n"
                    "/stop 取消当前聊天正在跑的任务\n"
                    "/a N 批准（仅这次） /b N 批准（永久） /c N 拒绝")
        if word in ("/status", "/状态"):
            return self._status_reply(store, item)
        if word in ("/queue", "/队列"):
            if args and args[0] == "clear":
                return self._queue_clear_reply(store, item)
            if args and args[0] == "drop":
                return self._queue_drop_reply(store, item, args[1:])
            if args:
                return "用法：/queue 查看排队明细；/queue clear 清空排队；/queue drop N 丢弃排队第 N 个任务。"
            return self._queue_reply(store, item)
        if word in ("/check", "/自检"):
            return self._check_reply(store)
        if word in APPROVAL_COMMAND_DECISIONS:
            num = _approval_ascii_number(args[0] if args else "")
            if num is None:
                return APPROVAL_USAGE
            scope = store.connection.execute(
                'SELECT c.chat_type, c.chat_id, m.sender_id FROM tasks t JOIN conversations c ON c.id=t.conversation_id '
                'JOIN inbound_messages m ON m.id=t.message_id WHERE t.id=?', (item.task_id,)).fetchone()
            operator_id = scope["sender_id"] if scope else ""
            chat_id = scope["chat_id"] if scope else ""
            return _approval_decide_ack(num, APPROVAL_COMMAND_DECISIONS[word],
                                        self.channel, operator_id, chat_id)
        if word.startswith("/"):
            return "新核心当前支持 /ping /status /queue /new /stop /help /check /a /b /c。该命令尚未迁移，未执行；请联系管理员。"
        return self._media_gate_reply(media)

    @staticmethod
    def _mark_receipt_delivered(store, receipt_id) -> None:
        store.connection.execute(
            "UPDATE task_receipts SET state='delivered' WHERE id=? AND state='pending'",
            (receipt_id,))

    @staticmethod
    def _mark_notice_delivered(store, notice_id) -> None:
        store.connection.execute(
            "UPDATE approval_notices SET state='delivered' WHERE id=? AND state='pending'",
            (notice_id,))

    def deliver_receipts(self, store, sender) -> dict:
        """Send pending task_receipts through sender; never raise.

        sender(chat_id, chat_type, text, context_token) -> bool.
        Started/wait receipts for an already-terminal task are skipped
        (marked delivered) so a stale "正在处理" never lands after the
        final answer. A failed send stays pending for the next tick.
        Returns {"sent", "skipped", "failed"} counts.
        """
        counts = {"sent": 0, "skipped": 0, "failed": 0}
        try:
            rows = store.connection.execute(
                "SELECT r.id,r.task_id,r.kind,r.text,c.chat_id,c.chat_type,"
                "t.state AS task_state FROM task_receipts r "
                "JOIN conversations c ON c.id=r.conversation_id "
                "JOIN tasks t ON t.id=r.task_id AND t.conversation_id=c.id "
                "WHERE r.state='pending' AND c.channel=? AND c.account_id=? "
                "ORDER BY r.created_at,r.id", (self.channel, self.account_id)).fetchall()
        except Exception:
            LOGGER.exception("receipt delivery list failed")
            return counts
        for row in rows:
            if row["kind"] in ("started", "wait") and row["task_state"] in ("succeeded", "failed", "cancelled"):
                self._mark_receipt_delivered(store, row["id"])
                counts["skipped"] += 1
                continue
            try:
                route = store.connection.execute(
                    "SELECT context_token FROM gateway_routes WHERE task_id=?",
                    (row["task_id"],)).fetchone()
            except Exception:
                route = None
            token = route["context_token"] if route is not None else ""
            try:
                ok = bool(sender(row["chat_id"], row["chat_type"], row["text"], token))
            except Exception:
                LOGGER.exception("receipt delivery send failed")
                ok = False
            if ok:
                self._mark_receipt_delivered(store, row["id"])
                counts["sent"] += 1
            else:
                counts["failed"] += 1
        return counts

    def deliver_approval_notices(self, store, sender) -> dict:
        """Send this channel's pending approval_notices through sender.

        Event IDs are unique, and distinct events can repeat the same text.
        Old random IDs retain text deduplication against other old rows.
        A failed send stays pending for the next tick.
        Returns {"sent", "skipped", "failed"} counts.
        """
        counts = {"sent": 0, "skipped": 0, "failed": 0}
        try:
            rows = store.connection.execute(
                "SELECT id,chat_id,text FROM approval_notices "
                "WHERE channel=? AND state='pending' ORDER BY created_at,id",
                (self.channel,)).fetchall()
        except Exception:
            return counts
        for row in rows:
            if LEGACY_RANDOM_NOTICE_ID.fullmatch(row["id"]):
                previous = store.connection.execute(
                    "SELECT id FROM approval_notices WHERE channel=? AND chat_id=? "
                    "AND text=? AND state='delivered'",
                    (self.channel, row["chat_id"], row["text"]))
                if any(LEGACY_RANDOM_NOTICE_ID.fullmatch(notice["id"]) for notice in previous):
                    self._mark_notice_delivered(store, row["id"])
                    counts["skipped"] += 1
                    continue
            if not (row["chat_id"] or "").strip():
                counts["failed"] += 1
                continue
            conv = store.connection.execute(
                "SELECT chat_type FROM conversations WHERE channel=? AND account_id=? AND chat_id=? LIMIT 1",
                (self.channel, self.account_id, row["chat_id"])).fetchone()
            chat_type = conv["chat_type"] if conv is not None else "single"
            try:
                ok = bool(sender(row["chat_id"], chat_type, row["text"], ""))
            except Exception:
                LOGGER.exception("approval notice delivery send failed")
                ok = False
            if ok:
                self._mark_notice_delivered(store, row["id"])
                counts["sent"] += 1
            else:
                counts["failed"] += 1
        return counts

    async def _upload_outbound_media(self, gateway, item):
        """Upload one outbound media item; returns (media_id, kind).

        Rejects disallowed paths and files over the upload limit before
        sending anything. Upload failures can retry cleanly.
        """
        kind = str((item or {}).get("kind") or "").lower()
        if kind not in ("image", "video", "file"):
            raise _PermanentMediaError(f"unsupported outbound media kind: {kind or 'unknown'}")
        path = (item or {}).get("path")
        if not isinstance(path, str) or not Path(path).is_absolute():
            raise _PermanentMediaError("outbound media has no local path")
        filename = str((item or {}).get("filename") or Path(path).name)

        def read_allowed_file() -> bytes:
            """Validate the resolved file and read at most the upload limit plus one byte."""
            try:
                file_path = Path(path).resolve(strict=True)
                if not gateway.allows_outbound_file(file_path):
                    raise _PermanentMediaError("outbound media path is not allowed")
                with file_path.open("rb") as source:
                    return source.read(WECOM_OUTBOUND_MEDIA_MAX_BYTES + 1)
            except (FileNotFoundError, RuntimeError, ValueError):
                raise _PermanentMediaError("outbound media path could not be resolved") from None

        data = await asyncio.to_thread(read_allowed_file)
        if len(data) > WECOM_OUTBOUND_MEDIA_MAX_BYTES:
            raise _PermanentMediaError(
                f"outbound media {filename} exceeds the 10 MiB WeCom upload cap")
        media_id = await gateway.upload_media(data, kind, filename)
        return media_id, kind

    async def delivery_loop(self, gateway, client=None, credentials=None) -> None:
        """Use the actual channel transport while SQLite owns sending attempts."""
        loop = asyncio.get_running_loop()
        owner = f'gateway-{self.channel}-{uuid.uuid4().hex}'

        async def send(claim, route=None):
            """Send to the immutable database route without using a legacy outbox."""
            if claim.channel != self.channel or claim.account_id != self.account_id:
                raise PermissionError('delivery belongs to another account')
            if self.channel == 'weixin':
                if route is None:
                    return SendOutcome('rejected', evidence='no authenticated inbound reply route')
                media_items = []
                for item in claim.media:
                    try:
                        media_items.append(await asyncio.wait_for(
                            gateway.prepare_core_media(client, credentials, claim.chat_id, item),
                            timeout=WEIXIN_MEDIA_PREPARE_SECONDS))
                    except (FileNotFoundError, PermissionError, ValueError) as exc:
                        return SendOutcome('rejected', evidence=f'weixin_media_invalid:{type(exc).__name__}')
                    except Exception as exc:
                        return SendOutcome('retry', evidence=f'weixin_upload_failed:{type(exc).__name__}', retry_after=30)
                success = True
                if claim.text.strip() or not media_items:
                    result = await gateway.send_text(client, credentials, claim.chat_id, claim.text,
                                                     context_token=route['context_token'],
                                                     client_id='core-' + hashlib.sha256(claim.part_id.encode()).hexdigest()[:24])
                    success = isinstance(result, dict) and bool(result) and ('ret' in result or 'errcode' in result) and result.get('ret') in (None, 0) and result.get('errcode') in (None, 0)
                if success:
                    for index, media in enumerate(media_items):
                        media_client_id = 'core-' + hashlib.sha256(f'{claim.part_id}:media:{index}'.encode()).hexdigest()[:24]
                        result = await gateway._send_items(client, credentials, claim.chat_id, [media],
                                                           context_token=route['context_token'],
                                                           client_id=media_client_id)
                        if not (isinstance(result, dict) and bool(result)
                                and ('ret' in result or 'errcode' in result)
                                and result.get('ret') in (None, 0) and result.get('errcode') in (None, 0)):
                            success = False
                            break
            elif self.channel == 'feishu':
                # 原生请求级 uuid 幂等键：由 part_id 生成确定性 UUID
                part_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"muse:claim:{claim.part_id}"))
                if hasattr(gateway, 'send_text'):
                    result = await gateway.send_text(
                        claim.chat_id,
                        claim.text,
                        chat_type=claim.chat_type,
                        uuid=part_uuid,
                    )
                elif hasattr(gateway, 'send_message'):
                    receive_id_type = 'chat_id' if claim.chat_type == 'group' or claim.chat_id.startswith('oc_') else 'open_id'
                    result = await gateway.send_message(
                        receive_id=claim.chat_id,
                        receive_id_type=receive_id_type,
                        msg_type='text',
                        content=json.dumps({'text': claim.text}, ensure_ascii=False),
                        uuid=part_uuid,
                    )
                else:
                    result = {'code': -1, 'msg': 'unsupported feishu gateway'}
                success = isinstance(result, dict) and result.get('code') == 0
            else:
                # Outbound media on WeCom. Order is deliberate: (1) upload
                # everything BEFORE any platform send, so a transient
                # upload failure can retry cleanly — nothing was sent
                # yet, so no duplicate. A permanent failure (missing
                # file / over cap / unknown kind) is a rejection, which
                # writes the delivery_failed receipt through the
                # existing reject path. (2) Text first, so the
                # explanation precedes the files it refers to. (3) One
                # media frame per item. A media send that fails after
                # the text is 'unknown', never a blind retry: the text
                # is already out, so retrying would duplicate it, and
                # the part goes to delivery_unknown for operator
                # resolution like any other uncertain send.
                media_ids: list[tuple[str, str]] = []
                if claim.media:
                    for item in claim.media:
                        try:
                            media_ids.append(await self._upload_outbound_media(gateway, item))
                        except _PermanentMediaError as exc:
                            return SendOutcome('rejected', evidence=str(exc)[:200])
                        except Exception as exc:
                            return SendOutcome('retry', evidence=f'wecom_upload_failed:{type(exc).__name__}:{exc}'[:200], retry_after=30)
                success = True
                if claim.text.strip():
                    result = await gateway.send_frame({'cmd': 'aibot_send_msg',
                        'headers': {'req_id': 'core-' + hashlib.sha256(claim.part_id.encode()).hexdigest()[:24]},
                        'body': {'chatid': claim.chat_id, 'chat_type': 2 if claim.chat_type == 'group' else 1,
                                 'msgtype': 'markdown', 'markdown': {'content': claim.text}}}, wait_response=True)
                    success = isinstance(result, dict) and result.get('errcode') == 0
                if success:
                    for media_id, kind in media_ids:
                        result = await gateway.send_frame({'cmd': 'aibot_send_msg',
                            'headers': {'req_id': 'core-' + hashlib.sha256((claim.part_id + kind).encode()).hexdigest()[:24]},
                            'body': {'chatid': claim.chat_id, 'chat_type': 2 if claim.chat_type == 'group' else 1,
                                     'msgtype': kind, kind: {'media_id': media_id}}}, wait_response=True)
                        if not (isinstance(result, dict) and result.get('errcode') == 0):
                            success = False
                            break
            if success:
                return SendOutcome('delivered', receipt=json.dumps({'part_id': claim.part_id, 'platform': self.channel}))
            # A generic nonzero response does not establish whether delivery happened.
            return SendOutcome('unknown')

        async def send_plain(chat_id, chat_type, text, context_token=None):
            """Send one plain-text notice (receipt / approval) on this channel.

            No claim, no part: the caller owns the retry decision (a
            failed notice stays pending). Returns True only on a
            confirmed platform acceptance.
            """
            if self.channel == 'weixin':
                result = await gateway.send_text(client, credentials, chat_id, text,
                                                 context_token=context_token or '',
                                                 client_id='core-receipt-' + uuid.uuid4().hex[:24])
                return (isinstance(result, dict) and bool(result)
                        and ('ret' in result or 'errcode' in result)
                        and result.get('ret') in (None, 0) and result.get('errcode') in (None, 0))
            if self.channel == 'feishu':
                part_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"muse:receipt:{uuid.uuid4().hex}"))
                if hasattr(gateway, 'send_text'):
                    result = await gateway.send_text(chat_id, text, chat_type=chat_type, uuid=part_uuid)
                elif hasattr(gateway, 'send_message'):
                    receive_id_type = 'chat_id' if chat_type == 'group' or chat_id.startswith('oc_') else 'open_id'
                    result = await gateway.send_message(
                        receive_id=chat_id, receive_id_type=receive_id_type,
                        msg_type='text', content=json.dumps({'text': text}, ensure_ascii=False),
                        uuid=part_uuid)
                else:
                    return False
                return isinstance(result, dict) and result.get('code') == 0
            result = await gateway.send_frame({'cmd': 'aibot_send_msg',
                'headers': {'req_id': 'core-receipt-' + uuid.uuid4().hex[:24]},
                'body': {'chatid': chat_id, 'chat_type': 2 if chat_type == 'group' else 1,
                         'msgtype': 'markdown', 'markdown': {'content': text}}}, wait_response=True)
            return isinstance(result, dict) and result.get('errcode') == 0

        def tick():
            """Use a connection owned by this thread and a bounded channel call."""
            with Store(self.database) as store:
                def transport(claim):
                    route = None
                    timeout = 30
                    if self.channel == 'weixin':
                        route = store.connection.execute(
                            'SELECT r.context_token FROM gateway_routes r JOIN outbound_messages o ON o.task_id=r.task_id WHERE o.id=?',
                            (claim.outbound_id,),
                        ).fetchone()
                        if claim.media:
                            timeout += len(claim.media) * (WEIXIN_MEDIA_PREPARE_SECONDS + 30)
                            with store.transaction():
                                runner.delivery._owned(claim)
                                store.connection.execute(
                                    "UPDATE outbound_parts SET lease_until=? WHERE id=? AND token=?",
                                    (store.clock() + timeout + 30, claim.part_id, claim.token))
                    future = asyncio.run_coroutine_threadsafe(send(claim, route), loop)
                    try:
                        return future.result(timeout=timeout)
                    except BaseException:
                        future.cancel()
                        raise
                runner = DeliveryRunner(store, transport, owner, chunk_limit=1000,
                                        channel=self.channel, account_id=self.account_id)
                outcome = runner.tick()

                def sender(chat_id, chat_type, text, context_token):
                    future = asyncio.run_coroutine_threadsafe(
                        send_plain(chat_id, chat_type, text, context_token), loop)
                    try:
                        return bool(future.result(timeout=30))
                    except BaseException:
                        future.cancel()
                        return False

                # Receipts and approval notices ride the same loop as
                # outbound messages, after them, so a final answer is
                # never stuck behind a "正在处理" notice. quiet_feedback
                # stays untouched: no legacy feedback loop is revived.
                try:
                    self.deliver_receipts(store, sender)
                except Exception:
                    LOGGER.exception('core receipt delivery tick failed')
                try:
                    self.deliver_approval_notices(store, sender)
                except Exception:
                    LOGGER.exception('core approval notice delivery tick failed')
                return outcome

        while True:
            try:
                await asyncio.to_thread(tick)
            except Exception:
                LOGGER.exception('core delivery tick failed')
            await asyncio.sleep(1)

    async def quiet_feedback(self) -> None:
        """Keep session supervision alive without running legacy feedback writers."""
        while True:
            await asyncio.sleep(60)
