#!/usr/bin/env python3
"""Approval card regressions with explicit identities and independent grants.

Similar text cannot authorize a different platform approval ID. Only explicit
source scopes may group notifications; every actual decision remains separately
validated and persisted. Test fixtures use future expiry and configured admins.
"""
import json
import os
import sys
import tempfile
import types
import time
import unittest

os.environ["APPROVAL_RELAY_TEST"] = "1"
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import approval_relay as relay  # noqa: E402

def require_approval_engine():
    """Stub policy engine returning REQUIRE_APPROVAL for every item.

    The default engine auto-approves everything since the 2026-11-06
    all-allow policy, so notification/grouping tests inject this stub to
    exercise the notify path (the old behaviour for these records).
    """
    class _RequireApproval:
        def evaluate(self, record, what=""):
            return types.SimpleNamespace(
                action=relay.PolicyAction.REQUIRE_APPROVAL,
                risk_level=relay.RiskLevel.NORMAL,
                is_high_risk=False,
                rule_name="normal_approval",
                reason="test stub: manual approval required",
            )
    return _RequireApproval()



def rec(aid, method="CONNECT", scheme="tcp", host="github.com",
        port=443, cmd="cd ~/workspace && git push origin main"):
    return {
        "approval_id": aid,
        "display_name": "Shell command",
        "registered_command": cmd,
        "scheme": scheme,
        "method": method,
        "host": host,
        "port": port,
        "path": f"{host}:{port}",
        "reason": f"endpoint {host}:{port} is not covered by any allow rule",
        "rich_explanation": "Push commits to GitHub",
        "request_expires_at_ms": 9999999999999,
    }


class StubGateway:
    def __init__(self, pending):
        self._pending = pending
        self.decide_calls = []

    def call_json(self, method, path_params=None, body=None, timeout=None):
        if method == "egress.approvals":
            return {"pending_approvals": self._pending}
        if method == "egress.approval":
            return {"approval": {}}
        if method == "egress.approval.decide":
            self.decide_calls.append((path_params["approval_id"],
                                      body["decision"]))
            return {}
        raise AssertionError(f"unexpected method {method}")


class CardGroupTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = self.tmp.name
        self._orig = (relay.STATE_F, relay.STATUS_F, relay.BASE,
                      relay.DECISIONS_F, relay.OFFSET_F)
        relay.STATE_F = os.path.join(base, "state.json")
        relay.STATUS_F = os.path.join(base, "status.json")
        relay.BASE = base
        relay.DECISIONS_F = os.path.join(base, "decisions.jsonl")
        relay.OFFSET_F = os.path.join(base, "decisions.offset")
        self.state = {"next_num": 1, "items": {}}
        self.admin_before = os.environ.get("APPROVAL_ADMINS_JSON")
        os.environ["APPROVAL_ADMINS_JSON"] = json.dumps({"wecom": {"room": ["alice"]}})

    def tearDown(self):
        (relay.STATE_F, relay.STATUS_F, relay.BASE,
         relay.DECISIONS_F, relay.OFFSET_F) = self._orig
        self.tmp.cleanup()
        if self.admin_before is None:
            os.environ.pop("APPROVAL_ADMINS_JSON", None)
        else:
            os.environ["APPROVAL_ADMINS_JSON"] = self.admin_before

    def rows(self, channel):
        path = os.path.join(self.tmp.name, "shadow",
                            f"{channel}-outbox.jsonl")
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as fh:
            return [json.loads(l) for l in fh if l.strip()]

    def cards(self):
        return [r for r in self.rows("wecom") if r.get("mode") == "send_card"]


class SparseDetailGateway:
    """Pending entries are sparse; details carry the full record."""

    def __init__(self, sparse_list, details):
        self._pending = sparse_list
        self._details = details

    def call_json(self, method, path_params=None, body=None, timeout=None):
        if method == "egress.approvals":
            return {"pending_approvals": self._pending}
        if method == "egress.approval":
            return {"approval": self._details[path_params["approval_id"]]}
        raise AssertionError(f"unexpected method {method}")


class SparseGroupingTests(CardGroupTestCase):
    def test_sparse_enrichment_does_not_create_authorization_scope(self):
        sparse = [{"approval_id": "aid-a",
                   "request_expires_at_ms": 9999999999999},
                  {"approval_id": "aid-b",
                   "request_expires_at_ms": 9999999999999}]
        details = {"aid-a": rec("aid-a", method="CONNECT", scheme="tcp"),
                   "aid-b": rec("aid-b", method="GET", scheme="https")}
        relay.poll_once(SparseDetailGateway(sparse, details), self.state, policy_engine=require_approval_engine())
        self.assertNotEqual(self.state["items"]["1"].get("group_key"), "")
        self.assertEqual(self.state["items"]["1"].get("group_key"),
                         self.state["items"]["2"].get("group_key"))
        self.assertNotIn("grouped_with", self.state["items"]["2"])
        self.assertEqual(len(self.cards()), 2)


class CardFormTests(CardGroupTestCase):
    def test_wecom_gets_card_weixin_gets_text(self):
        relay.poll_once(StubGateway([rec("aid-1")]), self.state, policy_engine=require_approval_engine())
        cards = self.cards()
        self.assertEqual(len(cards), 1)
        card = cards[0]["card"]
        self.assertEqual(card["card_type"], "button_interaction")
        self.assertEqual(card["task_id"], "appr-1")
        self.assertIn("待审批 #1", card["main_title"]["title"])
        keys = [b["key"] for b in card["button_list"]]
        self.assertEqual(keys, ["appr_once", "appr_always", "appr_deny"])
        wx = [r for r in self.rows("weixin") if r.get("mode") == "send"]
        self.assertEqual(len(wx), 1)
        self.assertIn("回 /a 1｜/b 1｜/c 1", wx[0]["content"])

    def test_card_carries_target_and_reason(self):
        card = relay.build_approval_card(
            "7", "Shell command",
            "命令：git push\n目标：tcp CONNECT github.com:443\n"
            "说明：Push commits\n原因：endpoint not covered",
            9999999999999)
        fields = {h["keyname"]: h["value"]
                  for h in card["horizontal_content_list"]}
        self.assertEqual(fields["目标"], "tcp CONNECT github.com:443")
        self.assertIn("endpoint", fields["原因"])


class GroupingTests(CardGroupTestCase):
    def test_unbound_similar_requests_remain_independent(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self.assertEqual(len(self.cards()), 2)
        self.assertEqual(len(self.rows("weixin")), 2)
        item2 = self.state["items"]["2"]
        self.assertNotIn("grouped_with", item2)
        self.assertTrue(item2["notified"])

    def test_different_target_not_grouped(self):
        gw = StubGateway([rec("aid-a"),
                          rec("aid-c", host="example.com")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self.assertEqual(len(self.cards()), 2)
        self.assertNotIn("grouped_with", self.state["items"]["2"])

    def test_decision_does_not_authorize_other_approval_ids(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        with open(relay.DECISIONS_F, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"num": "1", "decision": "allow_once",
                                 "channel": "wecom-card", "chat_id": "room", "operator_id": "alice"}) + "\n")
        relay.consume_decisions(gw, self.state)
        self.assertEqual(sorted(gw.decide_calls),
                         [("aid-a", "allow_once")])
        self.assertEqual(self.state["items"]["1"]["status"],
                         "decided:allow_once")
        self.assertEqual(self.state["items"]["2"]["status"], "pending")
        done = [r for r in self.rows("wecom")
                if "已批准" in r.get("content", "")]
        self.assertEqual(len(done), 1)
        self.assertNotIn("另 1 条", done[0]["content"])

    def test_grouped_item_promoted_when_primary_leaves(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self.assertEqual(len(self.cards()), 2)
        # Primary decided elsewhere (e.g. in the Muse app) before
        # the relay consumed any channel decision for the group.
        self.state["items"]["1"]["status"] = "gone"
        relay.poll_once(StubGateway([rec("aid-b", method="GET",
                                         scheme="https")]), self.state, policy_engine=require_approval_engine())
        self.assertEqual(len(self.cards()), 2)
        item2 = self.state["items"]["2"]
        self.assertNotIn("grouped_with", item2)
        self.assertTrue(item2["notified"])


class IdempotencyTests(CardGroupTestCase):
    def _decide(self, gw, num, decision):
        with open(relay.DECISIONS_F, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"num": num, "decision": decision,
                                 "channel": "wecom", "chat_id": "room", "operator_id": "alice"}) + "\n")
        relay.consume_decisions(gw, self.state)

    def test_duplicate_decision_is_silent(self):
        gw = StubGateway([rec("aid-a")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self._decide(gw, "1", "allow_always")
        self._decide(gw, "1", "allow_always")
        self.assertEqual(gw.decide_calls, [("aid-a", "allow_always")])
        for ch in ("wecom",):
            contents = [r.get("content", "") for r in self.rows(ch)]
            self.assertFalse(any("已经处理过了" in c for c in contents),
                             f"{ch} got a dup notice: {contents}")
            self.assertEqual(
                len([c for c in contents if "已批准" in c]), 1)

    def test_legacy_text_similarity_does_not_supply_authorization(self):
        self.state["items"]["1"] = {
            "approval_id": "aid-old", "who": "Shell command",
            "what": ("命令：cd ~/workspace && git push origin main\n"
                     "目标：tcp CONNECT github.com:443\n"
                     "说明：Push commits to GitHub\n"
                     "原因：endpoint github.com:443 is not covered"),
            "expires_ms": 9999999999999, "status": "pending",
            "first_seen": 1791540000, "notified": True, "group_key": ""}
        self.state["next_num"] = 2
        gw = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self.assertNotIn("grouped_with", self.state["items"]["2"])
        self.assertEqual(len(self.cards()), 1)
        self.assertEqual(len(self.rows("weixin")), 1)

    def test_sequential_always_requires_explicit_new_decision(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self.assertEqual(len(self.cards()), 1)
        self._decide(gw, "1", "allow_always")
        gw2 = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw2, self.state, policy_engine=require_approval_engine())
        item2 = self.state["items"]["2"]
        self.assertEqual(item2["status"], "pending")
        self.assertNotIn("auto_decided_from", item2)
        self.assertEqual(gw2.decide_calls, [])
        self.assertEqual(len(self.cards()), 2)
        self.assertEqual(len(self.rows("weixin")), 2)

    def test_sequential_always_outside_window_notifies(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp")])
        relay.poll_once(gw, self.state, policy_engine=require_approval_engine())
        self._decide(gw, "1", "allow_always")
        self.state["items"]["1"]["decided_at"] -= (
            relay.RECENT_ALWAYS_WINDOW_SECS + 3600)
        gw2 = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw2, self.state, policy_engine=require_approval_engine())
        self.assertEqual(self.state["items"]["2"]["status"], "pending")
        self.assertEqual(gw2.decide_calls, [])
        self.assertEqual(len(self.cards()), 2)


if __name__ == "__main__":
    unittest.main()
