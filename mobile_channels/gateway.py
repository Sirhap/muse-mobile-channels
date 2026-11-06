"""Account-bound gateway integration; legacy queues never receive core messages."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from pathlib import Path

from .ingress import CoreIngress
from .models import IngestResult
from .modes import lease_from_env
from .runtime import DeliveryRunner, SendOutcome
from .storage import Store

LOGGER = logging.getLogger(__name__)


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

    def close(self) -> None:
        """Release the mode lease when the gateway process stops."""
        if self.lease:
            self.lease.close()

    def create_coalescer(self, idle_gap_secs: float = 2.0, max_wait_secs: float = 6.0):
        """为当前网关创建一个入站连发防打碎合并器。"""
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

        with Store(self.database) as store, store.transaction():
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

            reply = self._command(store, item, text or '', bool(media))
            if reply is not None:
                store.complete_command(item.task_id, reply)
            return item

    def _command(self, store, item, text: str, has_media: bool) -> str | None:
        """Execute only scoped core commands; never call global legacy administration."""
        if has_media:
            return '附件信息已保存，但新核心尚未启用媒体执行适配，未执行该请求。请联系管理员处理附件。'
        command = text.strip().lower()
        if command in ('/new', '/新会话', '/stop', '/停止'):
            scope = store.connection.execute(
                'SELECT c.chat_type,m.sender_id FROM tasks t JOIN conversations c ON c.id=t.conversation_id '
                'JOIN inbound_messages m ON m.id=t.message_id WHERE t.id=?', (item.task_id,)).fetchone()
            if scope['chat_type'] == 'group' and scope['sender_id'] not in self.settings.get('administrators', []):
                return '该群聊管理命令需要管理员权限，未修改任务或上下文。'
        if command in ('/ping', 'ping'):
            return 'pong ✅ 网关在线'
        if command in ('/new', '/新会话'):
            store.new_generation(item.conversation_id)
            return '已开启新会话。'
        if command in ('/stop', '/停止'):
            rows = store.connection.execute(
                "SELECT id FROM tasks WHERE conversation_id=? AND id!=? AND state IN ('queued','claimed','running','reconciling')",
                (item.conversation_id, item.task_id),
            ).fetchall()
            for row in rows:
                store.request_cancel(row['id'], item.conversation_id)
            return f'已登记当前聊天 {len(rows)} 个活动任务的取消请求，等待执行方确认。'
        if command in ('/status', '/状态', '/queue', '/队列', '/check', '/自检'):
            counts = dict(store.connection.execute("SELECT state,COUNT(*) FROM tasks WHERE conversation_id=? AND id!=? GROUP BY state", (item.conversation_id, item.task_id)))
            return '当前聊天任务状态：' + (json.dumps(counts, ensure_ascii=False) if counts else '无任务') + '。网关连接和外部执行状态须分别核查。'
        if command.startswith('/'):
            return '新核心当前支持 /ping /status /queue /new /stop。该命令尚未迁移，未执行；请联系管理员。'
        return None

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
                result = await gateway.send_text(client, credentials, claim.chat_id, claim.text,
                                                 context_token=route['context_token'],
                                                 client_id='core-' + hashlib.sha256(claim.part_id.encode()).hexdigest()[:24])
                success = isinstance(result, dict) and bool(result) and ('ret' in result or 'errcode' in result) and result.get('ret') in (None, 0) and result.get('errcode') in (None, 0)
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
                result = await gateway.send_frame({'cmd': 'aibot_send_msg',
                    'headers': {'req_id': 'core-' + hashlib.sha256(claim.part_id.encode()).hexdigest()[:24]},
                    'body': {'chatid': claim.chat_id, 'chat_type': 2 if claim.chat_type == 'group' else 1,
                             'msgtype': 'markdown', 'markdown': {'content': claim.text}}}, wait_response=True)
                success = isinstance(result, dict) and result.get('errcode') == 0
            if success:
                return SendOutcome('delivered', receipt=json.dumps({'part_id': claim.part_id, 'platform': self.channel}))
            # A generic nonzero response does not establish whether delivery happened.
            return SendOutcome('unknown')

        def tick():
            """Use a connection owned by this thread and a bounded channel call."""
            with Store(self.database) as store:
                def transport(claim):
                    route = None
                    if self.channel == 'weixin':
                        route = store.connection.execute(
                            'SELECT r.context_token FROM gateway_routes r JOIN outbound_messages o ON o.task_id=r.task_id WHERE o.id=?',
                            (claim.outbound_id,),
                        ).fetchone()
                    future = asyncio.run_coroutine_threadsafe(send(claim, route), loop)
                    try:
                        return future.result(timeout=30)
                    except BaseException:
                        future.cancel()
                        raise
                return DeliveryRunner(store, transport, owner, chunk_limit=1000,
                                      channel=self.channel, account_id=self.account_id).tick()

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
