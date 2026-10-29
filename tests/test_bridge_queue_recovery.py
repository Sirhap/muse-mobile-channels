"""桥队列删除、取消与重启恢复的沙箱回归测试。

运行：python3 tests/test_bridge_queue_recovery.py。
加载时只排除外部平台导入及部署配置读取，原始队列和状态函数保持不变。
"""
import ast
import json
import tempfile
import time
import types
import unittest
from pathlib import Path


def load_bridge():
    """加载桥代码，无须凭证或外部 Muse SDK。"""
    source = Path(__file__).resolve().parents[1] / 'native-bridge/native_bridge.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    tree.body = [
        node for node in tree.body
        if not (isinstance(node, ast.ImportFrom)
                and node.module in ('gw2', 'muse_cli.cli', 'muse_cli.gateway'))
        and not (isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == 'CFG'
                         for target in node.targets))
    ]
    module = types.ModuleType('queue_recovery_bridge')
    module.__file__ = str(source)
    exec(compile(tree, str(source), 'exec'), module.__dict__)
    return module


class QueueRecoveryTests(unittest.TestCase):
    """使用真实沙箱队列和状态文件检查 worker 重启恢复。"""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.bridge = load_bridge()
        self.bridge.BASE = str(self.base)
        self.bridge.STATE_F = str(self.base / 'state.json')
        self.bridge.CFG = {'channels': {'test': {}}}
        self.bridge.read_signal_ts = lambda path: None
        self.cancelled = set()
        self.bridge.cancelled_ids = lambda channel: self.cancelled
        self.spool = self.base / 'spool.jsonl'
        self.admin = self.base / 'queue_admin-test.jsonl'
        self.worker = self.new_worker()
        self.worker.update_cs(turns=[self.turn('RUN')])

    @staticmethod
    def turn(msgid):
        """构造尚未达到进度通知时间的运行中回合。"""
        return {'msgid': msgid, 'ids': [msgid], 'lane': 'main',
                'session_id': 'sandbox-session', 'sent_at': int(time.time())}

    def new_worker(self):
        """创建真实 worker，仅替换与外部平台交互的操作。"""
        worker = self.bridge.ChannelWorker('test')
        worker.ensure_gw = lambda: True
        worker.drop_gw = lambda: None
        worker.poll_turn = lambda turn: 'running'
        worker.start_turn = lambda row, state: self.turn(row['msgid'])
        worker.idle_push_scan = lambda state: None
        return worker

    def write_spool(self, texts=('A', 'B', 'C')):
        """写入完整 UTF-8 行并保留准确字节数。"""
        self.raws = [(json.dumps({'msgid': mid, 'text': text}, ensure_ascii=False)
                      + '\n').encode('utf-8') for mid, text in zip('ABC', texts)]
        self.spool.write_bytes(b''.join(self.raws))

    def step(self, worker=None):
        """执行一次沙箱轮询并返回可见的 FIFO 队列。"""
        worker = worker or self.worker
        worker.step(str(self.spool), None)
        return [row['msgid'] for row, _ in worker.queue]

    def drop(self, *msgids):
        """追加与网关格式一致的队列管理指令。"""
        with self.admin.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps({'action': 'drop', 'msgids': msgids}) + '\n')

    def test_non_head_drop_survives_restart(self):
        self.write_spool()
        self.drop('B')
        self.assertEqual(self.step(), ['A', 'C'])
        self.assertEqual(self.step(self.new_worker()), ['A', 'C'])

    def test_unequal_unicode_rows_preserve_exact_restart_boundary(self):
        self.write_spool(('短', '不同长度' * 30, 'tail'))
        self.drop('B')
        self.assertEqual(self.step(), ['A', 'C'])
        self.assertEqual(self.worker.cs()['spool_offset'], 0)
        self.assertEqual(self.step(self.new_worker()), ['A', 'C'])

    def test_cancelled_non_head_stays_removed_after_signal_expires(self):
        self.write_spool(('first', 'cancel me please', 'third'))
        self.cancelled.add('B')
        self.assertEqual(self.step(), ['A', 'C'])
        self.cancelled.clear()
        restarted = self.new_worker()
        self.assertEqual(self.step(restarted), ['A', 'C'])
        self.assertEqual(restarted.cs()['cancels'], 1)

    def test_normal_fifo_works_with_legacy_state(self):
        self.write_spool()
        state = self.bridge.load_state()
        state['channels']['test'].pop('spool_tombstones', None)
        self.bridge.save_state(state)
        self.assertEqual(self.step(), ['A', 'B', 'C'])
        self.worker.update_cs(turns=[])
        self.assertEqual(self.step(), ['B', 'C'])
        self.assertEqual(self.worker.cs()['turns'][0]['msgid'], 'A')
        self.assertEqual(self.worker.cs()['spool_offset'], len(self.raws[0]))

    def test_consuming_prefix_reclaims_tombstone_and_starts_next_live_row(self):
        self.write_spool(('first', 'discard' * 15, 'last'))
        self.drop('B')
        self.step()
        self.worker.update_cs(turns=[])
        self.assertEqual(self.step(), ['C'])
        state = self.worker.cs()
        self.assertEqual(state['spool_offset'], sum(map(len, self.raws[:2])))
        self.assertEqual(state.get('spool_tombstones', []), [])
        restarted = self.new_worker()
        self.assertEqual(self.step(restarted), ['C'])
        restarted.update_cs(turns=[])
        self.assertEqual(self.step(restarted), [])
        self.assertEqual(restarted.cs()['turns'][0]['msgid'], 'C')
        self.assertEqual(restarted.cs()['spool_offset'], self.spool.stat().st_size)

    def test_drop_prefix_advances_only_contiguous_prefix(self):
        self.write_spool(('prefix', 'middle' * 12, 'tail'))
        self.drop('A', 'C')
        self.assertEqual(self.step(), ['B'])
        self.assertEqual(self.worker.cs()['spool_offset'], len(self.raws[0]))
        self.assertEqual(self.step(self.new_worker()), ['B'])

    def test_drop_all_drains_spool_and_allows_later_messages(self):
        self.write_spool()
        self.drop('A', 'B', 'C')
        self.assertEqual(self.step(), [])
        self.assertEqual(self.worker.cs()['spool_offset'], self.spool.stat().st_size)
        self.assertEqual(self.worker.cs().get('spool_tombstones', []), [])
        with self.spool.open('ab') as handle:
            handle.write(b'{"msgid":"D","text":"new"}\n')
        self.assertEqual(self.step(self.new_worker()), ['D'])

    def test_successive_admin_and_cancel_ranges_survive_restart(self):
        self.write_spool(('first', 'middle' * 15, 'last' * 4))
        self.drop('C')
        self.assertEqual(self.step(), ['A', 'B'])
        self.cancelled.add('B')
        self.assertEqual(self.step(), ['A'])
        self.cancelled.clear()
        restarted = self.new_worker()
        self.assertEqual(self.step(restarted), ['A'])
        restarted.update_cs(turns=[])
        self.assertEqual(self.step(restarted), [])
        self.assertEqual(restarted.cs()['spool_offset'], self.spool.stat().st_size)
        self.assertEqual(restarted.cs()['spool_tombstones'], [])

    def test_fallback_consumption_reclaims_adjacent_deleted_row(self):
        self.write_spool(('first', 'middle' * 15, 'last'))
        self.drop('B')
        self.step()
        fallbacks = []
        self.bridge.fallback_to_inbox = lambda channel, row, why: fallbacks.append(row)
        self.worker.ensure_gw = lambda: False
        self.worker.update_cs(turns=[])
        self.assertEqual(self.step(), ['C'])
        self.assertEqual([row['msgid'] for row in fallbacks], ['A'])
        self.assertEqual(self.worker.cs()['fallbacks'], 1)
        restarted = self.new_worker()
        self.assertEqual(self.step(restarted), [])
        self.assertEqual(restarted.cs()['turns'][0]['msgid'], 'C')

    def test_retained_merge_rollback_consumes_ranges_correctly(self):
        self.write_spool(('first', 'middle' * 15, 'last'))
        self.drop('B')
        self.step()
        self.worker.gw = types.SimpleNamespace(_open=lambda *args, **kwargs: None)
        self.worker._merge_into_active()
        self.assertEqual(self.worker.queue, [])
        self.assertEqual(self.worker.cs()['turns'][0]['ids'], ['RUN', 'A', 'C'])
        self.assertEqual(self.worker.cs()['spool_offset'], self.spool.stat().st_size)
        self.assertEqual(self.step(self.new_worker()), [])

    def test_failed_state_save_does_not_consume_admin_or_remove_memory_rows(self):
        self.write_spool()
        self.step()
        self.drop('B')
        save_state = self.bridge.save_state

        def fail_save(state):
            """模拟持久化失败，确认删除指令仍可在重启后重试。"""
            raise OSError('sandbox write failure')

        self.bridge.save_state = fail_save
        with self.assertRaises(OSError):
            self.step()
        self.bridge.save_state = save_state
        self.assertEqual([row['msgid'] for row, _ in self.worker.queue], list('ABC'))
        self.assertEqual(self.worker.cs()['admin_offset'], 0)
        self.assertEqual(self.step(self.new_worker()), ['A', 'C'])


if __name__ == '__main__':
    unittest.main()
