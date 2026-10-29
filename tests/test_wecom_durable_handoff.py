"""入站和出站消息持久化交接失败的沙箱回归测试。"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('wecom_durable_handoff', ROOT / 'wecom-bot/gateway.py')
wc = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(wc)


class EndTick(BaseException):
    """在指定轮数之后停止真实的出站循环。"""


class DurableHandoffTests(unittest.IsolatedAsyncioTestCase):
    """隔离测试实际回调和合并队列，不访问网络。"""

    def setUp(self):
        """将测试路径使用的状态文件全部重定向到临时目录。"""
        self.temp = tempfile.TemporaryDirectory(prefix='wecom-durable-test-')
        self.addCleanup(self.temp.cleanup)
        state = Path(self.temp.name)
        names = {'STATE': '.', 'INBOX': 'inbox.jsonl', 'SEEN_FILE': 'seen.jsonl',
                 'REQMAP': 'reqmap.json', 'CARDS': 'cards.json', 'BRIDGE_SPOOL': 'spool.jsonl',
                 'OUTBOX': 'outbox.jsonl',
                 'OUTBOX_OFFSET': 'offset', 'OUTBOX_RESULTS': 'results.jsonl',
                 'OUTBOX_PARKED': 'parked.json', 'OUTBOX_RETRY': 'retry.json',
                 'OUTBOX_PARTIAL': 'partial.json', 'CANCELLED_FILE': 'cancelled.json',
                 'STATUS': 'status.json', 'MEDIA_DIR': 'media'}
        for key, filename in names.items():
            patcher = patch.object(wc, key, state / filename)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.gateway = wc.Gateway.__new__(wc.Gateway)
        for key, value in {'seen_msgids': set(), 'allow_users': {'alice'}, 'reqmap': {},
                           'open_streams': {}, 'cards': {}, 'feedback_track': {},
                           'msgs_received': 0, 'msgs_sent': 0, '_coalesce_pending': {},
                           '_coalescer': None}.items():
            setattr(self.gateway, key, value)
        self.gateway.write_status = lambda: None

        async def no_slash(*args):
            return None

        async def respond(*args, **kwargs):
            return {'errcode': 0}

        self.gateway._slash_dispatch = no_slash
        self.gateway.respond = respond
        self.gateway.check_stream_watchdog = no_slash

    def frame(self, media=False):
        """生成白名单内的模拟消息，可选择携带图片。"""
        body = {'msgid': 'm1', 'msgtype': 'text', 'text': {'content': 'hello'},
                'chattype': 'single', 'from': {'userid': 'alice'}}
        if media:
            body.update(msgtype='image', image={'url': 'https://example.invalid/media', 'aeskey': 'unused'})
        return {'headers': {'req_id': 'req1'}, 'body': body}

    async def tick(self):
        """控制调度，只运行一轮真实的出站循环。"""
        original_sleep = asyncio.sleep
        count = 0

        async def sleep_once(delay):
            nonlocal count
            count += 1
            if count > 1:
                raise EndTick()
            await original_sleep(0)

        with patch.object(wc.asyncio, 'sleep', sleep_once):
            with self.assertRaises(EndTick):
                await self.gateway.outbox_loop()

    async def test_inbound_route_failure_does_not_commit_seen(self):
        """路由写入失败后的重试必须能够进入收件箱。"""
        with patch.object(self.gateway, '_save_reqmap', side_effect=OSError('route write failed')):
            with self.assertRaises(OSError):
                await self.gateway.handle_message_callback(self.frame())
        self.assertNotIn('m1', self.gateway.seen_msgids)
        await self.gateway.handle_message_callback(self.frame())
        self.assertEqual(len(wc.INBOX.read_text().splitlines()), 1)
        self.assertIn('m1', self.gateway.seen_msgids)
        restarted = wc.Gateway.__new__(wc.Gateway)
        restarted.seen_msgids = set()
        restarted.reqmap = {}
        restarted.cards = {}
        restarted._load_persisted()
        self.assertIn('m1', restarted.seen_msgids)

    async def test_seen_marker_failure_does_not_duplicate_durable_inbox(self):
        """收件箱已写入时，标记文件故障不会导致重复入队，重启也能恢复去重。"""
        with patch.object(wc, 'append_jsonl_line', side_effect=self.fail_seen_write):
            await self.gateway.handle_message_callback(self.frame())
            await self.gateway.handle_message_callback(self.frame())
        self.assertEqual(len(wc.INBOX.read_text().splitlines()), 1)
        self.assertIn('m1', self.gateway.seen_msgids)
        self.gateway.seen_msgids.clear()
        self.gateway._load_persisted()
        self.assertIn('m1', self.gateway.seen_msgids)

    async def test_diverted_seen_failure_recovers_from_spool_after_restart(self):
        """桥队列已写入时，即使标记文件故障，重启后也不会重复转投。"""
        original_exists = wc.os.path.exists

        def divert_exists(path):
            return True if str(path).endswith('/divert-wecom') else original_exists(path)

        with patch.object(wc.os.path, 'exists', side_effect=divert_exists):
            with patch.object(wc, 'append_jsonl_line', side_effect=self.fail_seen_write):
                await self.gateway.handle_message_callback(self.frame())
        self.assertFalse(wc.INBOX.exists())
        self.assertEqual(len(wc.BRIDGE_SPOOL.read_text().splitlines()), 1)

        self.gateway.seen_msgids.clear()
        self.gateway._load_persisted()
        self.assertIn('m1', self.gateway.seen_msgids)
        await self.gateway.handle_message_callback(self.frame())
        self.assertEqual(len(wc.BRIDGE_SPOOL.read_text().splitlines()), 1)

    def fail_seen_write(self, path, row):
        """仅让去重标记写入失败，收件箱和桥队列仍真实落盘。"""
        if path == wc.SEEN_FILE:
            raise OSError('seen write failed')
        from channel_common import append_jsonl
        append_jsonl(path, row)

    async def test_concurrent_media_duplicate_waits_for_failed_first_attempt(self):
        """并发到达的重复回调可以接续首次处理的失败。"""
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def download(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await release.wait()
                raise OSError('media preparation failed')

        self.gateway._download_media = download
        first = asyncio.create_task(self.gateway.handle_message_callback(self.frame(True)))
        await entered.wait()
        second = asyncio.create_task(self.gateway.handle_message_callback(self.frame(True)))
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertIsInstance(results[0], OSError)
        self.assertIsNone(results[1])
        self.assertTrue(wc.INBOX.exists())
        self.assertEqual(len(wc.INBOX.read_text().splitlines()), 1)

    async def test_slash_duplicate_runs_side_effect_once(self):
        """并发的斜杠命令仍然只执行一次副作用。"""
        calls = 0

        async def slash(*args):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return 'handled'

        self.gateway._slash_dispatch = slash
        await asyncio.gather(*(self.gateway.handle_message_callback(self.frame()) for _ in range(2)))
        self.assertEqual(calls, 1)
        self.assertIn('m1', self.gateway.seen_msgids)

    async def test_formal_park_failure_keeps_cursor_and_recovers(self):
        """正式回复的重试交接失败时不能消费源队列行。"""
        row = {'id': 'formal1', 'mode': 'reply', 'msgid': 'm1', 'content': 'answer'}
        wc.OUTBOX.write_text(json.dumps(row) + '\n')
        attempts = []

        async def send(item):
            attempts.append(item)
            return False

        self.gateway.dispatch_outbox_item = send
        self.gateway._get_coalescer = lambda: None
        with patch.object(self.gateway, '_save_parked', return_value=False):
            await self.tick()
        self.assertEqual(self.gateway._outbox_offset(), 0)
        await self.tick()
        self.assertIn('formal1', self.gateway._load_parked())
        self.assertEqual(self.gateway._outbox_offset(), wc.OUTBOX.stat().st_size)

    async def test_merged_main_failure_retains_originals(self):
        """屏障触发合并发送后，必须保留原始消息直到全部重试记录写入成功。"""
        rows = [{'id': 'send1', 'mode': 'send', 'chatid': 'chat', 'content': 'first'},
                {'id': 'send2', 'mode': 'send', 'chatid': 'chat', 'content': 'second'},
                {'id': 'barrier', 'mode': 'reply', 'msgid': 'm1', 'content': 'answer'}]
        wc.OUTBOX.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        attempts = []

        async def send(item):
            attempts.append(item)
            return False

        self.gateway.dispatch_outbox_item = send
        with patch.object(self.gateway, '_save_parked', return_value=False):
            await self.tick()
        self.assertEqual(self.gateway._outbox_offset(), 0)
        self.assertEqual(set(self.gateway._coalesce_pending), {'send1', 'send2'})
        await self.tick()
        self.assertEqual(set(self.gateway._load_parked()), {'send1', 'send2', 'barrier'})
        self.assertEqual(self.gateway._outbox_offset(), wc.OUTBOX.stat().st_size)
        self.assertEqual(len([row for row in attempts if row['mode'] == 'send']), 1)

    async def test_due_flush_failure_preserves_payload_and_does_not_resend(self):
        """定时取出的合并消息在落盘失败之后仍可恢复，且不重复发送。"""
        rows = [{'id': 'due1', 'mode': 'send', 'chatid': 'chat', 'content': 'first'},
                {'id': 'due2', 'mode': 'send', 'chatid': 'chat', 'content': 'second'}]
        wc.OUTBOX.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        attempts = []

        async def send(item):
            attempts.append(item)
            return False

        self.gateway.dispatch_outbox_item = send
        await self.tick()
        for bucket in self.gateway._coalescer._pending.values():
            bucket.last_at -= 10
        with patch.object(self.gateway, '_save_parked', return_value=False):
            await self.tick()
        self.assertEqual(self.gateway._outbox_offset(), 0)
        self.assertEqual(set(self.gateway._coalesce_pending), {'due1', 'due2'})
        await self.tick()
        self.assertEqual(set(self.gateway._load_parked()), {'due1', 'due2'})
        self.assertEqual(self.gateway._outbox_offset(), wc.OUTBOX.stat().st_size)
        self.assertEqual(len(attempts), 1)

    async def test_partial_merged_bookkeeping_preserves_barrier(self):
        """合并消息部分记录已落盘后失败，恢复时保留屏障且不重复网络请求。"""
        rows = [{'id': 'part1', 'mode': 'send', 'chatid': 'chat', 'content': 'first'},
                {'id': 'part2', 'mode': 'send', 'chatid': 'chat', 'content': 'second'},
                {'id': 'barrier', 'mode': 'reply', 'msgid': 'm1', 'content': 'answer'}]
        wc.OUTBOX.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        attempts = []

        async def send(item):
            attempts.append(item)
            return False

        self.gateway.dispatch_outbox_item = send
        original_save = self.gateway._save_parked
        saves = 0

        def fail_second(data):
            nonlocal saves
            saves += 1
            return False if saves == 2 else original_save(data)

        with patch.object(self.gateway, '_save_parked', side_effect=fail_second):
            await self.tick()
        self.assertEqual(self.gateway._outbox_offset(), 0)
        self.assertEqual(set(self.gateway._load_parked()), {'part1'})
        await self.tick()
        parked = self.gateway._load_parked()
        self.assertEqual(set(parked), {'part1', 'part2', 'barrier'})
        self.assertEqual(parked['part1']['n'], 1)
        self.assertEqual(self.gateway._outbox_offset(), wc.OUTBOX.stat().st_size)
        self.assertEqual([row['id'] for row in attempts], ['part1', 'barrier'])

    async def test_retry_lane_storage_failure_preserves_existing_payload(self):
        """重试循环的状态写入失败不会删除已保存的消息载荷。"""
        item = {'id': 'retry1', 'mode': 'reply', 'msgid': 'm1', 'content': 'answer'}
        wc.OUTBOX_PARKED.write_text(json.dumps({'retry1': {
            'item': item, 'n': 1, 'next': 0, 'parked_at': 1}}))
        attempts = []

        async def send(row):
            attempts.append(row)
            return False

        self.gateway.dispatch_outbox_item = send
        with patch.object(self.gateway, '_save_parked', return_value=False):
            await self.tick()
        self.assertEqual(self.gateway._load_parked()['retry1']['item'], item)
        await self.tick()
        self.assertEqual(self.gateway._load_parked()['retry1']['n'], 2)
        self.assertEqual(len(attempts), 2)


if __name__ == '__main__':
    unittest.main()
