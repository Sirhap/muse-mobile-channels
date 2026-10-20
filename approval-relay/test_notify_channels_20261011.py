#!/usr/bin/env python3
"""prefer_source_else_both: resolve_notify_channels + notify targets.

Run: APPROVAL_RELAY_TEST=1 python3 test_notify_channels_20261011.py
"""
import json
import os
import sys
import tempfile
import time
import unittest

os.environ["APPROVAL_RELAY_TEST"] = "1"
os.environ.pop("APPROVAL_NOTIFY_CHANNELS", None)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import approval_relay as relay  # noqa: E402


class NotifyChannelsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = self.tmp.name
        self._orig = (
            relay.STATE_F, relay.STATUS_F, relay.BASE, relay.DECISIONS_F,
        )
        relay.BASE = base
        relay.STATE_F = os.path.join(base, "state.json")
        relay.STATUS_F = os.path.join(base, "status.json")
        relay.DECISIONS_F = os.path.join(base, "decisions.jsonl")
        open(relay.DECISIONS_F, "w").close()
        os.environ.pop("APPROVAL_NOTIFY_CHANNELS", None)

    def tearDown(self):
        (relay.STATE_F, relay.STATUS_F, relay.BASE, relay.DECISIONS_F) = self._orig
        os.environ.pop("APPROVAL_NOTIFY_CHANNELS", None)
        self.tmp.cleanup()

    def shadow_rows(self, channel):
        path = os.path.join(relay.BASE, "shadow", f"{channel}-outbox.jsonl")
        if not os.path.exists(path):
            return []
        return [json.loads(line) for line in open(path, encoding="utf-8")
                if line.strip()]

    def write_decision(self, channel, ts=None):
        row = {"num": "1", "decision": "allow_once", "channel": channel,
               "ts": time.time() if ts is None else ts}
        with open(relay.DECISIONS_F, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def test_p0_source_channel_weixin(self):
        got = relay.resolve_notify_channels(
            item={"source_channel": "weixin", "who": "Shell"})
        self.assertEqual(got, ["weixin"])

    def test_p1_sticky_wecom_fresh(self):
        self.write_decision("wecom")
        got = relay.resolve_notify_channels(item={"who": "Shell"})
        self.assertEqual(got, ["wecom"])

    def test_no_evidence_both(self):
        got = relay.resolve_notify_channels(item={"who": "Shell"})
        self.assertEqual(got, ["weixin", "wecom"])

    def test_sticky_expired_both(self):
        self.write_decision("wecom", ts=time.time() - 3600)
        got = relay.resolve_notify_channels(item={})
        self.assertEqual(got, ["weixin", "wecom"])

    def test_illegal_telegram_both(self):
        got = relay.resolve_notify_channels(
            item={"source_channel": "telegram"})
        self.assertEqual(got, ["weixin", "wecom"])

    def test_notify_explicit_weixin_only(self):
        relay.notify("hello", "t1", channels=["weixin"])
        self.assertEqual(len(self.shadow_rows("weixin")), 1)
        self.assertEqual(len(self.shadow_rows("wecom")), 0)

    def test_notify_none_no_evidence_both(self):
        relay.notify("hello", "t2", channels=None, item={})
        self.assertEqual(len(self.shadow_rows("weixin")), 1)
        self.assertEqual(len(self.shadow_rows("wecom")), 1)

    def test_env_overrides_sticky(self):
        self.write_decision("wecom")
        os.environ["APPROVAL_NOTIFY_CHANNELS"] = "weixin"
        got = relay.resolve_notify_channels(item={})
        self.assertEqual(got, ["weixin"])

    def test_p0_beats_env(self):
        os.environ["APPROVAL_NOTIFY_CHANNELS"] = "wecom"
        got = relay.resolve_notify_channels(
            item={"channel": "weixin"})
        self.assertEqual(got, ["weixin"])

    def test_send_approval_notice_respects_source(self):
        item = {"who": "Shell command", "what": "命令：echo hi",
                "expires_ms": None, "source_channel": "weixin"}
        relay.send_approval_notice(9, item)
        self.assertEqual(len(self.shadow_rows("weixin")), 1)
        self.assertEqual(len(self.shadow_rows("wecom")), 0)
        self.assertIn("回 /a 9", self.shadow_rows("weixin")[0]["content"])


if __name__ == "__main__":
    unittest.main()
