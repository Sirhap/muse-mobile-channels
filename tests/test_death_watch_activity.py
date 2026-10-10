#!/usr/bin/env python3
"""Real-activity probe used before cold-channel resume and cancel.

The hook integration lives in ops/cold_death_drill.py. This file
checks the probe's own choices: which timestamp counts, which
binding counts, and how a bad file degrades.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import death_watch_activity as probe


NOW = 1_800_000_000.0
WINDOW = 1800.0


def _state(turn: dict) -> dict:
    return {"channels": {"weixin": {"turns": [turn]}}}


class AssessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.bridge = self.dir / "state.json"
        self.worker = self.dir / "worker_activity.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assess(self, msgid: str = "m1") -> dict:
        return probe.assess(
            msgid, now=NOW, window=WINDOW,
            bridge_state_path=str(self.bridge),
            worker_activity_path=str(self.worker),
        )

    def write_bridge(self, turn: dict) -> None:
        self.bridge.write_text(json.dumps(_state(turn)), encoding="utf-8")

    def test_missing_files_are_not_alive(self) -> None:
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertEqual(info["degraded"], [])

    def test_activity_timestamp_beats_outbox_shaped_silence(self) -> None:
        self.write_bridge({
            "msgid": "m1",
            "ids": ["m1"],
            "activities": [{"ts": NOW - 20, "text": "更新了文件 /tmp/x"}],
            "last_activity": "this text is not a timestamp",
        })
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "bridge-activity")

    def test_iso_activity_and_merged_id(self) -> None:
        self.write_bridge({
            "msgid": "root",
            "ids": ["root", "m1"],
            "activities": [{"ts": "2026-10-09T12:00:00Z", "text": "网页搜索"}],
            "activity_seen": [["2026-10-09T12:00:10Z", "ev", "web_search"]],
        })
        # NOW is far after 2026-10-09, so that ISO stamp is stale.
        info = self.assess()
        self.assertFalse(info["alive"])
        self.write_bridge({
            "msgid": "root",
            "ids": ["root", "m1"],
            "activity_seen": [[NOW - 5, "ev", "web_search"]],
        })
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "bridge-activity")

    def test_reply_and_running_priority(self) -> None:
        self.write_bridge({
            "msgid": "m1",
            "sess_status": "running",
            "last_activity_poll": NOW - 10,
            "last_reply_at": NOW - 15,
            "activities": [{"ts": NOW - 8, "text": "子助手运行中"}],
        })
        info = self.assess()
        self.assertEqual(info["source"], "bridge-activity")
        self.write_bridge({
            "msgid": "m1",
            "sess_status": "completed",
            "last_reply_at": NOW - 15,
            "activities": [],
        })
        self.assertEqual(self.assess()["source"], "bridge-reply")
        self.write_bridge({
            "msgid": "m1",
            "sess_status": "running",
            "last_activity_poll": NOW - 12,
            "activities": [],
        })
        self.assertEqual(self.assess()["source"], "bridge-running")

    def test_frozen_running_and_poll_clock_alone_are_dead(self) -> None:
        self.write_bridge({
            "msgid": "m1",
            "sess_status": "running",
            "last_activity_poll": NOW - WINDOW - 1,
            "activities": [],
        })
        self.assertFalse(self.assess()["alive"])
        self.write_bridge({
            "msgid": "m1",
            "sess_status": "completed",
            "last_activity_poll": NOW - 5,
            "activities": [],
        })
        self.assertFalse(self.assess()["alive"])

    def test_worker_timestamp_not_file_mtime(self) -> None:
        self.worker.write_text(json.dumps({
            "m1": {"ts": NOW - 40, "source": "worker"},
        }), encoding="utf-8")
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "worker-activity")
        self.worker.write_text(json.dumps({"m1": NOW - WINDOW - 5}),
                               encoding="utf-8")
        self.assertFalse(self.assess()["alive"])

    def test_bridge_source_preferred_when_both_fresh(self) -> None:
        self.write_bridge({
            "msgid": "m1",
            "activities": [{"ts": NOW - 50, "text": "网页搜索"}],
        })
        self.worker.write_text(json.dumps({"m1": NOW - 1}), encoding="utf-8")
        self.assertEqual(self.assess()["source"], "bridge-activity")

    def test_corrupt_bridge_degrades_without_hiding_worker(self) -> None:
        self.bridge.write_text("{", encoding="utf-8")
        info = self.assess()
        self.assertFalse(info["alive"])
        self.assertIn("unreadable:state.json", info["degraded"])
        self.worker.write_text(json.dumps({"m1": NOW - 3}), encoding="utf-8")
        info = self.assess()
        self.assertTrue(info["alive"])
        self.assertEqual(info["source"], "worker-activity")
        self.assertIn("unreadable:state.json", info["degraded"])

    def test_snapshot_and_status_text_are_not_activity(self) -> None:
        self.bridge.write_text(json.dumps({
            "channels": {"weixin": {
                "turns": [],
                "queue_snapshot": {
                    "active": [{"msgid": "m1", "last_activity": "thinking",
                                "secs": 10}],
                    "updated": NOW - 5,
                },
            }},
        }), encoding="utf-8")
        self.assertFalse(self.assess()["alive"])

    def test_far_future_timestamp_is_ignored(self) -> None:
        self.write_bridge({
            "msgid": "m1",
            "activities": [{"ts": NOW + 10_000, "text": "nope"}],
        })
        self.assertFalse(self.assess()["alive"])


if __name__ == "__main__":
    unittest.main()
