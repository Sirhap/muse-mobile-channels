#!/usr/bin/env python3
"""Tests for the approval relay empty-content fix (2026-10-08).

Incident: pending approvals are sparse when first emitted, so the
relay notified "未知请求方" with an empty body and the user approved
blind. These tests pin the repair: enrich from the detail endpoint,
hold the notification while content is still empty, and notify once
the enrichment succeeds on a later poll.

Run: APPROVAL_RELAY_TEST=1 python -m unittest test_approval_relay
"""
import json
import os
import sys
import tempfile
import unittest

os.environ["APPROVAL_RELAY_TEST"] = "1"
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import approval_relay as relay  # noqa: E402

FULL_RECORD = {
    "approval_id": "aid-full",
    "display_name": "Shell command",
    "policy_subject": "tool.exec",
    "registered_command": "cd ~/workspace/secure-drop && bash open-entry.sh --template single --ttl 30",
    "observed_command": "ssh -p 443 a.pinggy.io",
    "scheme": "tcp",
    "method": "CONNECT",
    "host": "a.pinggy.io",
    "port": 443,
    "path": "a.pinggy.io:443",
    "reason": "endpoint a.pinggy.io:443 is not covered by any allow rule",
    "rich_explanation": "建立远程访问连接",
    "request_expires_at_ms": 1791458503312,
}

SPARSE_RECORD = {
    "approval_id": "aid-sparse",
    "request_expires_at_ms": 1791458503312,
}


class StubGateway:
    """Minimal gateway stub dispatching on the RPC method name."""

    def __init__(self, pending, detail=None, detail_error=None):
        self._pending = pending
        self._detail = detail
        self._detail_error = detail_error
        self.detail_calls = 0

    def call_json(self, method, path_params=None, body=None, timeout=None):
        if method == "egress.approvals":
            return {"pending_approvals": self._pending}
        if method == "egress.approval":
            self.detail_calls += 1
            if self._detail_error is not None:
                raise self._detail_error
            return {"approval": self._detail}
        raise AssertionError(f"unexpected method {method}")


class RelayTestCase(unittest.TestCase):
    """Base case isolating relay state and shadow outboxes in a tmp dir."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = self.tmp.name
        self._orig = (relay.STATE_F, relay.STATUS_F, relay.BASE)
        relay.STATE_F = os.path.join(base, "state.json")
        relay.STATUS_F = os.path.join(base, "status.json")
        relay.BASE = base
        self.state = {"next_num": 1, "items": {}}

    def tearDown(self):
        relay.STATE_F, relay.STATUS_F, relay.BASE = self._orig
        self.tmp.cleanup()

    def shadow_rows(self, channel):
        path = os.path.join(self.tmp.name, "shadow", f"{channel}-outbox.jsonl")
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]


class SummarizeTests(unittest.TestCase):
    """summarize() must surface command, target, and reason."""

    def test_full_record(self):
        who, what = relay.summarize(FULL_RECORD)
        self.assertEqual(who, "Shell command")
        self.assertIn("open-entry.sh", what)
        self.assertIn("a.pinggy.io:443", what)
        self.assertIn("not covered by any allow rule", what)
        self.assertTrue(relay.has_content(who, what))

    def test_nested_approval_wrapper(self):
        who, what = relay.summarize({"approval": FULL_RECORD})
        self.assertEqual(who, "Shell command")
        self.assertIn("a.pinggy.io:443", what)

    def test_sparse_record_has_no_content(self):
        who, what = relay.summarize(SPARSE_RECORD)
        self.assertEqual(who, "未知请求方")
        self.assertFalse(relay.has_content(who, what))


class PollTests(RelayTestCase):
    """poll_once() enrichment, holding, and retry behaviour."""

    def test_sparse_pending_enriched_from_detail(self):
        gateway = StubGateway([SPARSE_RECORD],
                              detail={**FULL_RECORD,
                                      "approval_id": "aid-sparse"})
        relay.poll_once(gateway, self.state)
        rows = self.shadow_rows("weixin")
        self.assertEqual(len(rows), 1)
        self.assertIn("Shell command", rows[0]["content"])
        self.assertIn("a.pinggy.io:443", rows[0]["content"])
        item = self.state["items"]["1"]
        self.assertTrue(item["notified"])
        self.assertNotEqual(item["what"], "")

    def test_empty_after_detail_holds_notification_then_retries(self):
        failing = StubGateway([SPARSE_RECORD],
                              detail_error=RuntimeError("boom"))
        relay.poll_once(failing, self.state)
        self.assertEqual(self.shadow_rows("weixin"), [])
        self.assertFalse(self.state["items"]["1"]["notified"])

        succeeding = StubGateway(
            [SPARSE_RECORD],
            detail={**FULL_RECORD, "approval_id": "aid-sparse"})
        relay.poll_once(succeeding, self.state)
        rows = self.shadow_rows("weixin")
        self.assertEqual(len(rows), 1)
        self.assertIn("open-entry.sh", rows[0]["content"])

    def test_full_pending_notifies_without_detail_fetch(self):
        gateway = StubGateway([FULL_RECORD], detail=None)
        relay.poll_once(gateway, self.state)
        rows = self.shadow_rows("weixin")
        self.assertEqual(len(rows), 1)
        self.assertIn("Shell command", rows[0]["content"])
        self.assertIn("a.pinggy.io:443", self.state["items"]["1"]["what"])
        self.assertEqual(gateway.detail_calls, 0)

    def test_notified_item_is_not_renotified(self):
        gateway = StubGateway([FULL_RECORD],
                              detail=FULL_RECORD)
        relay.poll_once(gateway, self.state)
        relay.poll_once(gateway, self.state)
        self.assertEqual(len(self.shadow_rows("weixin")), 1)


if __name__ == "__main__":
    unittest.main()
