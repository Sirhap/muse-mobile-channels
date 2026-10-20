#!/usr/bin/env python3
"""Approval relay: card notices + same-request grouping (2026-10-09).

User order (voice, 2026-10-09): approvals must arrive as WeCom
cards, not typed-command text, and near-identical approvals spawned
by one operation (e.g. one git push -> CONNECT + GET + retry, which
pushed #3/#4/#5 and #6/#7 as separate texts to BOTH channels) must
not flood the user.

Pinned behaviour:
(a) a new pending approval notifies WeCom as a send_card row whose
    card is a button_interaction with the three decision keys
    appr_once/appr_always/appr_deny and task_id appr-<num>; WeChat
    (no template cards) keeps the text form.
(b) a second pending approval with the same command + host:port
    (different scheme/method) is grouped under the first: no second
    notice on either channel, item marked grouped_with.
(c) a different host:port is NOT grouped — separate card.
(d) deciding the primary applies the same decision to grouped
    siblings via egress.approval.decide, and the done notice counts
    them.
(e) a grouped item whose primary leaves pending without it (here:
    decided before the grouping existed) is promoted into its own
    notice on the next poll instead of expiring unseen.
(f) a duplicate decision row for an already-decided item is
    consumed silently: no second decide call, no dup notice pushed
    to either channel (live #8: text first, late card second).
(g) a legacy primary whose group_key stayed empty still groups a
    sibling, via the key derived from its stored notice text.
(h) a same-group approval arriving shortly AFTER an allow_always
    decision (sequential sibling, live #8 -> #9) is auto-decided
    allow_always without another card; outside the window it is
    notified normally.

Run: APPROVAL_RELAY_TEST=1 python -m unittest \
    test_approval_card_group_20261009
"""
import json
import os
import sys
import tempfile
import unittest

os.environ["APPROVAL_RELAY_TEST"] = "1"
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import approval_relay as relay  # noqa: E402


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
        "request_expires_at_ms": 1791540222300,
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

    def tearDown(self):
        (relay.STATE_F, relay.STATUS_F, relay.BASE,
         relay.DECISIONS_F, relay.OFFSET_F) = self._orig
        self.tmp.cleanup()

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
    def test_sparse_records_group_after_enrichment(self):
        sparse = [{"approval_id": "aid-a",
                   "request_expires_at_ms": 1791540222300},
                  {"approval_id": "aid-b",
                   "request_expires_at_ms": 1791540222300}]
        details = {"aid-a": rec("aid-a", method="CONNECT", scheme="tcp"),
                   "aid-b": rec("aid-b", method="GET", scheme="https")}
        relay.poll_once(SparseDetailGateway(sparse, details), self.state)
        self.assertNotEqual(self.state["items"]["1"].get("group_key"), "")
        self.assertEqual(self.state["items"]["1"].get("group_key"),
                         self.state["items"]["2"].get("group_key"))
        self.assertEqual(self.state["items"]["2"].get("grouped_with"), "1")
        self.assertEqual(len(self.cards()), 1)


class CardFormTests(CardGroupTestCase):
    def test_wecom_gets_card_weixin_gets_text(self):
        relay.poll_once(StubGateway([rec("aid-1")]), self.state)
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
        self.assertIn("/批准 1", wx[0]["content"])

    def test_card_carries_target_and_reason(self):
        card = relay.build_approval_card(
            "7", "Shell command",
            "命令：git push\n目标：tcp CONNECT github.com:443\n"
            "说明：Push commits\n原因：endpoint not covered",
            1791540222300)
        fields = {h["keyname"]: h["value"]
                  for h in card["horizontal_content_list"]}
        self.assertEqual(fields["目标"], "tcp CONNECT github.com:443")
        self.assertIn("endpoint", fields["原因"])


class GroupingTests(CardGroupTestCase):
    def test_same_command_and_target_groups(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state)
        self.assertEqual(len(self.cards()), 1)
        self.assertEqual(len(self.rows("weixin")), 1)
        item2 = self.state["items"]["2"]
        self.assertEqual(item2.get("grouped_with"), "1")
        self.assertTrue(item2["notified"])

    def test_different_target_not_grouped(self):
        gw = StubGateway([rec("aid-a"),
                          rec("aid-c", host="example.com")])
        relay.poll_once(gw, self.state)
        self.assertEqual(len(self.cards()), 2)
        self.assertNotIn("grouped_with", self.state["items"]["2"])

    def test_decision_applies_to_grouped_siblings(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state)
        with open(relay.DECISIONS_F, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"num": "1", "decision": "allow_once",
                                 "channel": "wecom-card"}) + "\n")
        relay.consume_decisions(gw, self.state)
        self.assertEqual(sorted(gw.decide_calls),
                         [("aid-a", "allow_once"), ("aid-b", "allow_once")])
        self.assertEqual(self.state["items"]["1"]["status"],
                         "decided:allow_once")
        self.assertEqual(self.state["items"]["2"]["status"],
                         "decided:allow_once")
        done = [r for r in self.rows("weixin")
                if "已批准" in r.get("content", "")]
        self.assertEqual(len(done), 1)
        self.assertIn("另 1 条", done[0]["content"])

    def test_grouped_item_promoted_when_primary_leaves(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp"),
                          rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state)
        self.assertEqual(len(self.cards()), 1)
        # Primary decided elsewhere (e.g. in the Muse app) before
        # the relay consumed any channel decision for the group.
        self.state["items"]["1"]["status"] = "gone"
        relay.poll_once(StubGateway([rec("aid-b", method="GET",
                                         scheme="https")]), self.state)
        self.assertEqual(len(self.cards()), 2)
        item2 = self.state["items"]["2"]
        self.assertNotIn("grouped_with", item2)
        self.assertTrue(item2["notified"])


class IdempotencyTests(CardGroupTestCase):
    def _decide(self, gw, num, decision):
        with open(relay.DECISIONS_F, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"num": num, "decision": decision,
                                 "channel": "wecom"}) + "\n")
        relay.consume_decisions(gw, self.state)

    def test_duplicate_decision_is_silent(self):
        gw = StubGateway([rec("aid-a")])
        relay.poll_once(gw, self.state)
        self._decide(gw, "1", "allow_always")
        self._decide(gw, "1", "allow_always")
        self.assertEqual(gw.decide_calls, [("aid-a", "allow_always")])
        for ch in ("weixin", "wecom"):
            contents = [r.get("content", "") for r in self.rows(ch)]
            self.assertFalse(any("已经处理过了" in c for c in contents),
                             f"{ch} got a dup notice: {contents}")
            self.assertEqual(
                len([c for c in contents if "已批准" in c]), 1)

    def test_legacy_primary_groups_via_what_key(self):
        self.state["items"]["1"] = {
            "approval_id": "aid-old", "who": "Shell command",
            "what": ("命令：cd ~/workspace && git push origin main\n"
                     "目标：tcp CONNECT github.com:443\n"
                     "说明：Push commits to GitHub\n"
                     "原因：endpoint github.com:443 is not covered"),
            "expires_ms": 1791540222300, "status": "pending",
            "first_seen": 1791540000, "notified": True, "group_key": ""}
        self.state["next_num"] = 2
        gw = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw, self.state)
        self.assertEqual(self.state["items"]["2"].get("grouped_with"),
                         "1")
        self.assertEqual(self.cards(), [])
        self.assertEqual(self.rows("weixin"), [])

    def test_sequential_always_auto_decides(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp")])
        relay.poll_once(gw, self.state)
        self.assertEqual(len(self.cards()), 1)
        self._decide(gw, "1", "allow_always")
        gw2 = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw2, self.state)
        item2 = self.state["items"]["2"]
        self.assertEqual(item2["status"], "decided:allow_always")
        self.assertEqual(item2.get("auto_decided_from"), "1")
        self.assertEqual(gw2.decide_calls,
                         [("aid-b", "allow_always")])
        self.assertEqual(len(self.cards()), 1)
        self.assertEqual(len(self.rows("weixin")), 2)  # new1 + done1 only

    def test_sequential_always_outside_window_notifies(self):
        gw = StubGateway([rec("aid-a", method="CONNECT", scheme="tcp")])
        relay.poll_once(gw, self.state)
        self._decide(gw, "1", "allow_always")
        self.state["items"]["1"]["decided_at"] -= (
            relay.RECENT_ALWAYS_WINDOW_SECS + 3600)
        gw2 = StubGateway([rec("aid-b", method="GET", scheme="https")])
        relay.poll_once(gw2, self.state)
        self.assertEqual(self.state["items"]["2"]["status"], "pending")
        self.assertEqual(gw2.decide_calls, [])
        self.assertEqual(len(self.cards()), 2)


if __name__ == "__main__":
    unittest.main()
