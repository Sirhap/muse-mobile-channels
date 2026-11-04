"""Actual gateway callbacks must preserve the core's transactional route boundary."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mobile_channels.gateway import GatewayBinding
from mobile_channels.storage import Store
from mobile_channels.models import InboundMessage


def load_gateway(channel):
    """Load the real protocol adapter without constructing network resources."""
    spec = importlib.util.spec_from_file_location(f'{channel}_core_binding_test', ROOT / f'{channel}-bot/gateway.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GatewayCoreBindingTests(unittest.IsolatedAsyncioTestCase):
    """Exercise real ingress callbacks against one isolated SQLite authority."""

    def setUp(self):
        """Create a mode document, temporary state and actual account bindings."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / 'core.sqlite3'
        with Store(self.database):
            pass
        modes = self.root / 'modes.json'
        modes.write_text(json.dumps({'channels': {
            channel: {'mode': 'core', 'database': str(self.database),
                      'account_id': f'{channel}-account', 'allowed_senders': ['alice', 'admin'],
                      'administrators': ['admin']}
            for channel in ('weixin', 'wecom')
        }}))
        environment = patch.dict('os.environ', {'MUSE_CHANNEL_MODES': str(modes)})
        environment.start()
        self.addCleanup(environment.stop)
        self.modules = {channel: load_gateway(channel) for channel in ('weixin', 'wecom')}
        self.gateways = {}
        for channel, module in self.modules.items():
            state = self.root / channel
            state.mkdir()
            for key, filename in {'STATE': '.', 'INBOX': 'inbox.jsonl', 'OUTBOX': 'outbox.jsonl',
                                  'SEEN_FILE': 'seen.jsonl', 'CONTEXT_FILE': 'context.json',
                                  'QUOTE_SPOOL': 'spool.jsonl', 'BRIDGE_SPOOL': 'spool.jsonl'}.items():
                if hasattr(module, key):
                    patcher = patch.object(module, key, state / filename)
                    patcher.start()
                    self.addCleanup(patcher.stop)
            gateway = module.Gateway.__new__(module.Gateway)
            gateway._core_binding = GatewayBinding(channel)
            self.addCleanup(gateway._core_binding.close)
            gateway.seen_msgids = set()
            gateway.context = {}
            gateway.msgs_received = 0
            gateway.msgs_sent = 0
            gateway.allow_users = {'alice', 'admin'}
            gateway.write_status = Mock()
            gateway._slash_dispatch = AsyncMock(side_effect=AssertionError('legacy command dispatcher used'))
            gateway.append_jsonl = Mock(side_effect=AssertionError('legacy queue used'))
            gateway._commit_inbound_seen = Mock(side_effect=AssertionError('legacy dedup used'))
            gateway._save_context = Mock()
            self.gateways[channel] = gateway

    async def inbound(self, channel, identifier='m1', text='hello', chat='alice', sender='alice'):
        """Pass transport-shaped content into the actual channel callback."""
        gateway = self.gateways[channel]
        if channel == 'weixin':
            message = {'message_id': identifier, 'message_type': 1,
                       'from_user_id': sender, 'context_token': 'bound-token',
                       'item_list': [{'type': 1, 'text_item': {'text': text}}]}
            await gateway.handle_inbound(None, {'user_id': 'alice'}, message)
        else:
            frame = {'headers': {'req_id': 'request-' + identifier}, 'body': {
                'msgid': identifier, 'msgtype': 'text', 'text': {'content': text},
                'from': {'userid': sender}, 'chatid': chat,
                'chattype': 'single' if chat == sender else 'group'}}
            await gateway.handle_message_callback(frame)

    def rows(self, table):
        """Inspect local authority records without relying on legacy result files."""
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(f'SELECT * FROM {table}')]

    async def test_actual_callbacks_write_only_core_and_bind_accounts(self):
        """Two channel adapters accept text without writing any legacy queue."""
        for channel in self.modules:
            await self.inbound(channel)
            self.gateways[channel]._slash_dispatch.assert_not_called()
            self.assertFalse(self.modules[channel].INBOX.exists())
        routes = self.rows('conversations')
        self.assertEqual({row['account_id'] for row in routes}, {'weixin-account', 'wecom-account'})
        self.assertEqual(len(self.rows('tasks')), 2)

    async def test_identical_ids_in_distinct_chats_are_not_globally_deduplicated(self):
        """An external message ID belongs to its original conversation, not a channel-wide set."""
        await self.inbound('wecom', chat='room-one')
        await self.inbound('wecom', chat='room-two')
        self.assertEqual(len(self.rows('tasks')), 2)
        self.assertEqual({row['chat_id'] for row in self.rows('conversations')}, {'room-one', 'room-two'})

    async def test_old_seen_markers_do_not_swallow_core_messages(self):
        """SQLite acceptance is authoritative even if the legacy snapshot contains the ID."""
        for channel in self.modules:
            self.gateways[channel].seen_msgids.add('m1')
            await self.inbound(channel)
        self.assertEqual(len(self.rows('tasks')), 2)

    async def test_replayed_new_command_changes_generation_once(self):
        """Command state, message identity and acknowledgement commit together."""
        for channel in self.modules:
            await self.inbound(channel, identifier='new', text='/new')
            await self.inbound(channel, identifier='new', text='/new')
        self.assertEqual({row['generation'] for row in self.rows('conversations')}, {1})
        self.assertEqual(len(self.rows('tasks')), 2)
        self.assertEqual(len(self.rows('outbound_messages')), 2)
        for gateway in self.gateways.values():
            gateway._slash_dispatch.assert_not_called()

    async def test_unsupported_command_is_explicit_reply_not_legacy_execution(self):
        """Old global administration cannot silently operate on a core account."""
        await self.inbound('wecom', text='/subagent run something')
        reply = self.rows('outbound_messages')[0]['text']
        self.assertIn('尚未迁移', reply)
        self.assertIn('未执行', reply)
        self.assertEqual(self.rows('tasks')[0]['state'], 'succeeded')

    async def test_route_write_failure_rolls_back_acceptance_and_allows_retry(self):
        """Failure after ingress creation cannot strand a task without its reply route."""
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TRIGGER break_route BEFORE INSERT ON gateway_routes BEGIN SELECT RAISE(ABORT,'route unavailable'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            await self.inbound('wecom')
        self.assertEqual(self.rows('tasks'), [])
        self.assertEqual(self.rows('inbound_messages'), [])
        with sqlite3.connect(self.database) as connection:
            connection.execute('DROP TRIGGER break_route')
        await self.inbound('wecom')
        self.assertEqual(len(self.rows('tasks')), 1)
        self.assertEqual(len(self.rows('gateway_routes')), 1)

    async def test_command_outbox_failure_rolls_back_generation(self):
        """A failed acknowledgement cannot commit /new and then repeat it on retry."""
        with sqlite3.connect(self.database) as connection:
            connection.execute("CREATE TRIGGER break_reply BEFORE INSERT ON outbound_messages BEGIN SELECT RAISE(ABORT,'reply unavailable'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            await self.inbound('weixin', text='/new')
        self.assertEqual(self.rows('conversations'), [])
        with sqlite3.connect(self.database) as connection:
            connection.execute('DROP TRIGGER break_reply')
        await self.inbound('weixin', text='/new')
        self.assertEqual(self.rows('conversations')[0]['generation'], 1)

    async def test_group_allowlisted_member_cannot_reset_context(self):
        """Sender allowlisting grants messaging permission, not group administration."""
        await self.inbound('wecom', text='/new', chat='shared-room')
        self.assertEqual(self.rows('conversations')[0]['generation'], 0)
        self.assertIn('需要管理员权限', self.rows('outbound_messages')[0]['text'])
        self.gateways['wecom']._slash_dispatch.assert_not_called()

    async def test_configured_group_administrator_can_reset_context_once(self):
        """Explicit administrator identity allows the same scoped, replay-safe command."""
        await self.inbound('wecom', identifier='reset', text='/new', chat='shared-room', sender='admin')
        await self.inbound('wecom', identifier='reset', text='/new', chat='shared-room', sender='admin')
        self.assertEqual(self.rows('conversations')[0]['generation'], 1)
        self.assertEqual(len(self.rows('outbound_messages')), 1)
        self.assertIn('已开启当前聊天', self.rows('outbound_messages')[0]['text'])

    async def test_new_receipt_uses_current_context_without_late_result_prefix(self):
        """A context reset receipt belongs to the context it just created."""
        from mobile_channels.runtime import DeliveryRunner, SendOutcome
        await self.inbound('wecom', identifier='reset-receipt', text='/new')
        sent = []
        def transport(claim):
            sent.append(claim)
            return SendOutcome('delivered', receipt='confirmed')
        with Store(self.database) as store:
            self.assertEqual(DeliveryRunner(store, transport, 'sender').tick(), 'delivered')
        self.assertEqual(sent[0].generation, 1)
        self.assertTrue(sent[0].text.startswith('已开启当前聊天的新上下文'))
        self.assertNotIn('补充此前对话的结果', sent[0].text)

    async def test_group_member_cannot_cancel_active_task(self):
        """A nonadministrator command leaves a running group execution intact."""
        await self.inbound('wecom', identifier='work', chat='shared-room')
        with Store(self.database) as store:
            claim = store.claim('executor')
            store.begin_submission(claim)
            store.mark_submitted(claim, 'original-run')
        await self.inbound('wecom', identifier='stop', text='/stop', chat='shared-room')
        active = next(row for row in self.rows('tasks') if row['id'] == claim.task_id)
        self.assertEqual(active['cancel_requested'], 0)
        self.assertEqual(active['state'], 'running')
        self.assertIn('需要管理员权限', self.rows('outbound_messages')[0]['text'])

    async def test_group_administrator_stop_cannot_cancel_other_conversation(self):
        """Authorized group administration remains limited to the original chat."""
        await self.inbound('wecom', identifier='first', chat='room-one')
        await self.inbound('wecom', identifier='second', chat='room-two')
        with Store(self.database) as store:
            claims = [store.claim('executor-one'), store.claim('executor-two')]
            for index, claim in enumerate(claims):
                store.begin_submission(claim)
                store.mark_submitted(claim, f'original-run-{index}')
        await self.inbound('wecom', identifier='stop', text='/stop', chat='room-one', sender='admin')
        conversations = {row['id']: row['chat_id'] for row in self.rows('conversations')}
        running = {conversations[row['conversation_id']]: row for row in self.rows('tasks') if row['state'] == 'running'}
        self.assertEqual(running['room-one']['cancel_requested'], 1)
        self.assertEqual(running['room-two']['cancel_requested'], 0)
        self.assertIn('1 个活动任务', self.rows('outbound_messages')[0]['text'])

    async def run_delivery(self, channel, terminal, *, client=None, credentials=None, ticks=3):
        """Drive real thread-to-event-loop delivery, then cancel at a completed tick."""
        binding = self.gateways[channel]._core_binding
        original_sleep = asyncio.sleep
        ready = asyncio.Event()
        completed = 0

        async def bounded_sleep(delay):
            """Accelerate loop intervals without replacing actual delivery work."""
            nonlocal completed
            completed += 1
            if completed >= ticks and terminal():
                ready.set()
                await original_sleep(60)
            await original_sleep(0.001)

        with patch('mobile_channels.gateway.asyncio.sleep', side_effect=bounded_sleep):
            task = asyncio.create_task(binding.delivery_loop(self.gateways[channel], client, credentials))
            try:
                await asyncio.wait_for(ready.wait(), timeout=5)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

    async def test_actual_weixin_delivery_preserves_context_and_accepts_success_codes(self):
        """Both documented success fields confirm the original private reply route."""
        gateway = self.gateways['weixin']
        for index, response in enumerate(({'ret': 0}, {'errcode': 0})):
            with self.subTest(response=response):
                await self.inbound('weixin', identifier=f'ping-{index}', text='/ping')
                pending = next(row for row in self.rows('outbound_messages') if row['state'] == 'pending')
                gateway.send_text = AsyncMock(return_value=response)
                client, credentials = object(), {'token': 'sandbox-token'}
                await self.run_delivery('weixin', lambda: next(row for row in self.rows('outbound_messages')
                    if row['id'] == pending['id'])['state'] == 'delivered',
                    client=client, credentials=credentials)
                gateway.send_text.assert_awaited_once()
                arguments = gateway.send_text.call_args
                self.assertIs(arguments.args[0], client)
                self.assertIs(arguments.args[1], credentials)
                self.assertEqual(arguments.args[2], 'alice')
                self.assertEqual(arguments.kwargs['context_token'], 'bound-token')
                self.assertTrue(arguments.kwargs['client_id'].startswith('core-'))
                self.assertFalse(self.modules['weixin'].OUTBOX.exists())

    async def test_actual_wecom_delivery_keeps_group_route_and_platform_receipt(self):
        """A core reply uses its immutable group target through the real adapter loop."""
        await self.inbound('wecom', text='/ping', chat='bound-room')
        gateway = self.gateways['wecom']
        gateway.send_frame = AsyncMock(return_value={'errcode': 0})
        await self.run_delivery('wecom', lambda: self.rows('outbound_messages')[0]['state'] == 'delivered')
        gateway.send_frame.assert_awaited_once()
        call = gateway.send_frame.call_args
        self.assertTrue(call.kwargs['wait_response'])
        self.assertEqual(call.args[0]['body']['chatid'], 'bound-room')
        self.assertEqual(call.args[0]['body']['chat_type'], 2)
        self.assertTrue(call.args[0]['headers']['req_id'].startswith('core-'))
        self.assertFalse(self.modules['wecom'].OUTBOX.exists())

    async def test_actual_delivery_timeout_is_unknown_and_not_repeated(self):
        """Several real delivery ticks cannot retry an unconfirmed platform send."""
        for channel in self.modules:
            with self.subTest(channel=channel):
                await self.inbound(channel, text='/ping')
                pending = next(row for row in self.rows('outbound_messages') if row['state'] == 'pending')
                gateway = self.gateways[channel]
                transport = AsyncMock(side_effect=TimeoutError('response lost after send'))
                setattr(gateway, 'send_text' if channel == 'weixin' else 'send_frame', transport)
                await self.run_delivery(channel, lambda: next(row for row in self.rows('outbound_messages')
                    if row['id'] == pending['id'])['state'] == 'delivery_unknown', ticks=4)
                transport.assert_awaited_once()
                attempts = self.rows('outbound_attempts')
                target_parts = {row['id'] for row in self.rows('outbound_parts') if row['outbound_id'] == pending['id']}
                self.assertEqual(len([row for row in attempts if row['part_id'] in target_parts]), 1)

    async def test_actual_delivery_does_not_claim_other_channels_or_accounts(self):
        """A shared database never lets one gateway reject or send another account's work."""
        for channel in self.modules:
            await self.inbound(channel, text='/ping')
        with Store(self.database) as store:
            foreign = store.ingest(InboundMessage('wecom', 'other-account', 'single',
                'other-user', 'foreign-id', 'other-user', 'foreign request'))
            claim = store.claim('foreign-executor')
            self.assertEqual(claim.task_id, foreign.task_id)
            store.begin_submission(claim)
            store.mark_submitted(claim, 'foreign-run')
            foreign_outbound = store.complete(claim, 'foreign result')
        gateway = self.gateways['wecom']
        gateway.send_frame = AsyncMock(return_value={'errcode': 0})
        await self.run_delivery('wecom', lambda: any(row['state'] == 'delivered' for row in self.rows('outbound_messages')))
        gateway.send_frame.assert_awaited_once()
        states = {row['task_id']: row['state'] for row in self.rows('outbound_messages')}
        self.assertEqual(states[foreign.task_id], 'pending')
        weixin_conversation = next(row['id'] for row in self.rows('conversations') if row['channel'] == 'weixin')
        weixin_task = next(row['id'] for row in self.rows('tasks') if row['conversation_id'] == weixin_conversation)
        self.assertEqual(states[weixin_task], 'pending')
        parts = self.rows('outbound_parts')
        self.assertTrue(all(row['owner'] is None for row in parts if row['outbound_id'] == foreign_outbound))

    async def test_nonowner_is_rejected_before_core_ingress(self):
        """Protocol authentication remains in force in the alternate mode."""
        await self.inbound('wecom', sender='intruder')
        self.assertEqual(self.rows('tasks'), [])

    async def test_stop_command_cancels_queued_tasks_immediately(self):
        """A /stop command immediately cancels queued tasks in the same conversation without cancelling itself."""
        await self.inbound('wecom', identifier='queued-1', text='first job')
        await self.inbound('wecom', identifier='queued-2', text='second job')
        await self.inbound('wecom', identifier='stop-cmd', text='/stop')
        tasks = {row['id']: row for row in self.rows('tasks')}
        self.assertEqual(len(tasks), 3)
        cancelled = [row for row in tasks.values() if row['state'] == 'cancelled']
        succeeded = [row for row in tasks.values() if row['state'] == 'succeeded']
        self.assertEqual(len(cancelled), 2)
        self.assertEqual(len(succeeded), 1)
        self.assertIn('2 个活动任务', self.rows('outbound_messages')[0]['text'])

    async def test_empty_text_without_media_is_rejected_not_queued_as_attachment(self):
        """Empty text without media raises IngressValidationError instead of queuing a bogus [附件] task."""
        from mobile_channels.ingress import IngressValidationError
        binding = self.gateways['wecom']._core_binding
        with self.assertRaises(IngressValidationError):
            binding.receive(
                message_id='empty-no-media',
                sender_id='alice',
                chat_id='alice',
                chat_type='single',
                text='',
                media=None,
            )
        self.assertEqual(self.rows('tasks'), [])
        item = binding.receive(
            message_id='whitespace-with-media',
            sender_id='alice',
            chat_id='alice',
            chat_type='single',
            text='   ',
            media=[{'type': 'file', 'name': 'doc.pdf'}],
        )
        self.assertTrue(item.created)
        self.assertIn('附件信息已保存', self.rows('outbound_messages')[0]['text'])


if __name__ == '__main__':
    unittest.main()
