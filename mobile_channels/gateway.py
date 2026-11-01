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
                    media_json TEXT NOT NULL DEFAULT '[]')''')

    def close(self) -> None:
        """Release the mode lease when the gateway process stops."""
        if self.lease:
            self.lease.close()

    def receive(self, *, message_id: str, sender_id: str, chat_id: str,
                chat_type: str, text: str, context_token: str = '', media: list | None = None):
        """Atomically accept content, commands and route metadata, deduplicating replay.

        Unsupported capabilities are explicit replies rather than silent fallback
        to another scheduler. Media metadata is preserved for operator recovery.
        """
        with Store(self.database) as store, store.transaction():
            ingress = CoreIngress(store, self.channel, self.account_id, self.settings['allowed_senders'])
            item = ingress.receive(str(message_id), sender_id, chat_id, chat_type, text or '[附件]')
            if not item.created:
                return item
            store.connection.execute('INSERT INTO gateway_routes VALUES (?,?,?,?)',
                                     (item.task_id, str(message_id), context_token, json.dumps(media or [], ensure_ascii=False)))
            reply = self._command(store, item, text, bool(media))
            if reply is not None:
                task = store.task(item.task_id)
                store.connection.execute("UPDATE tasks SET state='succeeded',result=? WHERE id=?", (reply, item.task_id))
                store.connection.execute('INSERT INTO outbound_messages(id,task_id,conversation_id,generation,text,created_at) VALUES (?,?,?,?,?,?)',
                                         (uuid.uuid4().hex, item.task_id, item.conversation_id, task['generation'], reply, store.clock()))
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
            generation = store.new_generation(item.conversation_id)
            return f'已开启当前聊天的新上下文（第 {generation} 代）。旧任务保留原回复路由。'
        if command in ('/stop', '/停止'):
            rows = store.connection.execute("SELECT id FROM tasks WHERE conversation_id=? AND state IN ('claimed','running','reconciling')", (item.conversation_id,)).fetchall()
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

        async def send(claim):
            """Send to the immutable database route without using a legacy outbox."""
            if claim.channel != self.channel or claim.account_id != self.account_id:
                raise PermissionError('delivery belongs to another account')
            if self.channel == 'weixin':
                with Store(self.database) as store:
                    route = store.connection.execute('SELECT r.context_token FROM gateway_routes r JOIN outbound_messages o ON o.task_id=r.task_id WHERE o.id=?', (claim.outbound_id,)).fetchone()
                if route is None:
                    return SendOutcome('rejected', evidence='no authenticated inbound reply route')
                result = await gateway.send_text(client, credentials, claim.chat_id, claim.text,
                                                 context_token=route['context_token'],
                                                 client_id='core-' + hashlib.sha256(claim.part_id.encode()).hexdigest()[:24])
                success = isinstance(result, dict) and bool(result) and ('ret' in result or 'errcode' in result) and result.get('ret') in (None, 0) and result.get('errcode') in (None, 0)
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
            def transport(claim):
                future = asyncio.run_coroutine_threadsafe(send(claim), loop)
                try:
                    return future.result(timeout=30)
                except BaseException:
                    future.cancel()
                    raise
            with Store(self.database) as store:
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
