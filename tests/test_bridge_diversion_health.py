"""Bridge failure routing must preserve unresolved work and queued messages."""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from channel_common import bridge_diversion_state


class BridgeDiversionTests(unittest.TestCase):
    """Use actual files and per-channel heartbeat rather than process-global age."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.status = self.root / 'status.json'
        self.state = self.root / 'state.json'
        self.spool = self.root / 'spool.jsonl'
        self.now = int(time.time())
        self.write_state()
        self.write_status()

    def write_state(self, **values):
        data = {'turns': [], 'recovery_queue': [], 'spool_offset': 0,
                'submission_pending': None, 'reconnect_exhausted': False}
        data.update(values)
        self.state.write_text(json.dumps({'channels': {'weixin': data}}))

    def write_status(self, **values):
        data = {'pid': os.getpid(), 'ts': self.now, 'mode': {'weixin': 'live'},
                'worker_heartbeat': {'weixin': self.now, 'wecom': self.now}}
        data.update(values)
        self.status.write_text(json.dumps(data))

    def route(self):
        return bridge_diversion_state(self.status, self.state, self.spool, 'weixin', now=self.now)

    def test_healthy_bridge_receives_even_with_active_work(self):
        self.write_state(turns=[{'msgid': 'm'}])
        self.assertEqual(self.route(), 'ready')

    def test_missing_corrupt_or_unreadable_state_stays_blocked(self):
        self.state.unlink()
        self.assertEqual(self.route(), 'blocked')
        self.state.write_text('{broken')
        self.assertEqual(self.route(), 'blocked')
        with patch.object(Path, 'open', side_effect=PermissionError('denied')):
            self.assertEqual(self.route(), 'blocked')

    def test_known_empty_state_and_unavailable_bridge_goes_to_inbox(self):
        for status in ({'pid': os.getpid(), 'mode': {'weixin': 'live'}, 'worker_heartbeat': {'weixin': self.now - 46}},
                       {'pid': -1, 'mode': {'weixin': 'live'}, 'worker_heartbeat': {'weixin': self.now}},
                       {'pid': os.getpid(), 'mode': {'weixin': 'shadow'}, 'worker_heartbeat': {'weixin': self.now}},
                       {'pid': os.getpid(), 'mode': {'weixin': 'live'}, 'worker_heartbeat': {'wecom': self.now}}):
            with self.subTest(status=status):
                self.write_status(**status)
                self.assertEqual(self.route(), 'unavailable')
        self.write_state(reconnect_exhausted=True)
        self.write_status()
        self.assertEqual(self.route(), 'unavailable')

    def test_unavailable_with_pending_active_or_backlog_is_blocked(self):
        self.write_status(worker_heartbeat={'weixin': self.now - 100, 'wecom': self.now})
        for values in ({'submission_pending': {'row': {'msgid': 'x'}}},
                       {'turns': [{'msgid': 'x'}]}, {'recovery_queue': [{'msgid': 'x'}]}):
            with self.subTest(values=values):
                self.write_state(**values)
                self.assertEqual(self.route(), 'blocked')
        self.write_state()
        self.spool.write_bytes(b'{partial')
        self.assertEqual(self.route(), 'blocked')

    def test_pending_even_with_healthy_heartbeat_is_blocked(self):
        self.write_state(submission_pending={'row': {'msgid': 'x'}})
        self.assertEqual(self.route(), 'blocked')

    def test_independent_channel_heartbeat(self):
        self.write_status(worker_heartbeat={'weixin': self.now - 46, 'wecom': self.now})
        self.assertEqual(self.route(), 'unavailable')

class GatewayRoutingTests(unittest.IsolatedAsyncioTestCase):
    """Exercise the actual inbound callbacks with health decisions and disk failures."""

    async def test_weixin_unavailable_routes_to_inbox(self):
        from test_weixin_inbound_checkpoint import InboundCheckpointTests, wx
        case = InboundCheckpointTests('test_inbox_io_error_keeps_checkpoint_and_replay_deduplicates_prefix')
        case.setUp()
        try:
            with patch.object(wx.os.path, 'exists', return_value=True), patch.object(wx, 'bridge_diversion_state', return_value='unavailable'):
                await case.gateway.handle_inbound(None, case.credentials, case.message(10))
            self.assertTrue(wx.INBOX.exists())
            self.assertFalse(wx.QUOTE_SPOOL.exists())
        finally:
            case.doCleanups()

    async def test_wecom_unavailable_routes_to_inbox(self):
        from test_wecom_durable_handoff import DurableHandoffTests, wc
        case = DurableHandoffTests('test_inbound_route_failure_does_not_commit_seen')
        case.setUp()
        try:
            with patch.object(wc.os.path, 'exists', return_value=True), patch.object(wc, 'bridge_diversion_state', return_value='unavailable'):
                await case.gateway.handle_message_callback(case.frame())
            self.assertTrue(wc.INBOX.exists())
            self.assertFalse(wc.BRIDGE_SPOOL.exists())
        finally:
            case.doCleanups()
            case.temp.cleanup()

    async def test_healthy_wecom_uses_spool(self):
        from test_wecom_durable_handoff import DurableHandoffTests, wc
        case = DurableHandoffTests('test_inbound_route_failure_does_not_commit_seen')
        case.setUp()
        try:
            with patch.object(wc.os.path, 'exists', return_value=True), patch.object(wc, 'bridge_diversion_state', return_value='ready'):
                await case.gateway.handle_message_callback(case.frame())
            self.assertTrue(wc.BRIDGE_SPOOL.exists())
            self.assertFalse(wc.INBOX.exists())
        finally:
            case.doCleanups()
            case.temp.cleanup()

    async def test_blocked_spool_failure_never_falls_back_or_marks_seen(self):
        from test_wecom_durable_handoff import DurableHandoffTests, wc
        case = DurableHandoffTests('test_inbound_route_failure_does_not_commit_seen')
        case.setUp()
        try:
            original = case.gateway.append_jsonl
            def fail_spool(path, row):
                if path == wc.BRIDGE_SPOOL:
                    raise OSError('disk full')
                return original(path, row)
            with patch.object(wc.os.path, 'exists', return_value=True), patch.object(wc, 'bridge_diversion_state', return_value='blocked'), patch.object(case.gateway, 'append_jsonl', side_effect=fail_spool):
                with self.assertRaises(OSError):
                    await case.gateway.handle_message_callback(case.frame())
            self.assertFalse(wc.INBOX.exists())
            self.assertNotIn('m1', case.gateway.seen_msgids)
        finally:
            case.doCleanups()
            case.temp.cleanup()


class RouteReplayTests(unittest.TestCase):
    """Retries cannot put one inbound message on two execution lanes."""

    def test_existing_cross_lane_message_and_partial_tail(self):
        from channel_common import bridge_message_exists
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            inbox, spool = base / "inbox", base / "spool"
            spool.write_text(json.dumps({"msgid": "m", "chatid": "alice"}) + "\n")
            self.assertTrue(bridge_message_exists(inbox, spool, "m", "wecom", "alice"))
            self.assertFalse(bridge_message_exists(inbox, spool, "m", "wecom", "bob"))
            inbox.write_bytes(b'{"msgid":')
            with self.assertRaises(ValueError):
                bridge_message_exists(inbox, spool, "m", "wecom", "alice")

    def test_route_lock_blocks_competing_worker_until_handoff_is_committed(self):
        import threading
        from channel_common import bridge_route_lock
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            entered, released = threading.Event(), threading.Event()
            def worker():
                with bridge_route_lock(base, "weixin"):
                    entered.set()
                released.set()
            with bridge_route_lock(base, "weixin"):
                thread = threading.Thread(target=worker)
                thread.start()
                self.assertFalse(entered.wait(0.1))
            self.assertTrue(released.wait(2))
            thread.join()


if __name__ == '__main__':
    unittest.main()
