"""复现：企微 core 模式下审批通知仍写死信 outbox.jsonl。

只用临时目录与临时 core 库，绝不碰生产库。
跑红 = 问题存在；修复后应全绿。
"""
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent

APPROVAL_NOTICES_DDL = (
    "CREATE TABLE IF NOT EXISTS approval_notices ("
    "id TEXT PRIMARY KEY, channel TEXT NOT NULL, "
    "chat_id TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
    "state TEXT NOT NULL DEFAULT 'pending' "
    "CHECK(state IN ('pending','delivered','failed')), "
    "created_at REAL NOT NULL)")


def load_relay():
    spec = importlib.util.spec_from_file_location(
        "approval_notify_core_fix", ROOT / "approval-relay/approval_relay.py")
    relay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(relay)
    return relay


class CoreNotifyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.relay = load_relay()
        self.relay.TEST = False
        self.relay.BASE = str(self.base)
        self.relay.STATE_F = str(self.base / "state.json")
        self.relay.DECISIONS_F = str(self.base / "decisions.jsonl")
        self.relay.OFFSET_F = str(self.base / "decisions.offset")
        for channel in ("weixin", "wecom"):
            self.relay.CHANNELS[channel]["outbox"] = str(
                self.base / channel / "outbox.jsonl")
            (self.base / channel).mkdir()
        self.relay.CHANNELS["wecom"]["row"] = {"chatid": "sirhao",
                                               "chat_type": 1}
        self.db = self.base / "core.sqlite3"
        from mobile_channels.storage import Store
        with Store(self.db):
            pass
        self.modes = self.base / "modes.json"

    def write_modes(self, wecom_mode):
        self.modes.write_text(json.dumps({"channels": {
            "weixin": {"mode": "legacy"},
            "wecom": {"mode": wecom_mode,
                      "database": str(self.db),
                      "account_id": "wecom-bot",
                      "allowed_senders": ["alice"],
                      "administrators": ["alice"]}}}))

    def env(self):
        patcher = mock.patch.dict(
            os.environ, {"MUSE_CHANNEL_MODES": str(self.modes)})
        patcher.start()
        self.addCleanup(patcher.stop)

    def notice_rows(self):
        if not self.db.exists():
            return []
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            try:
                return [dict(r) for r in conn.execute(
                    "SELECT * FROM approval_notices")]
            except sqlite3.OperationalError:
                return []

    def outbox_rows(self, channel):
        path = Path(self.relay.CHANNELS[channel]["outbox"])
        if not path.exists():
            return []
        return [json.loads(line) for line in
                path.read_text(encoding="utf-8").splitlines()]

    def test_wecom_core_notice_goes_to_table_not_outbox(self):
        self.write_modes("core")
        self.env()
        item = {"decision_scope": {"channel": "wecom", "chat_id": "room"}}
        self.relay.notify("待审批 #1\n回 /a 1｜/b 1｜/c 1", "new1",
                          approval={"num": "1", "who": "op",
                                    "what": "命令：echo hi",
                                    "expires_ms": None,
                                    "is_high_risk": False},
                          item=item)
        rows = self.notice_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel"], "wecom")
        self.assertEqual(rows[0]["chat_id"], "room")
        self.assertIn("/a 1", rows[0]["text"])
        self.assertEqual(rows[0]["state"], "pending")
        self.assertEqual(self.outbox_rows("wecom"), [])

    def test_wecom_core_notice_uses_env_chatid_without_scope(self):
        self.write_modes("core")
        self.env()
        self.relay.notify("待审批 #1\n回 /a 1", "new1", channels=["wecom"])
        rows = self.notice_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["chat_id"], "sirhao")

    def test_test_mode_uses_shadow_even_with_core_configuration(self):
        self.write_modes("core")
        self.env()
        self.relay.TEST = True

        self.relay.notify("测试通知", "test", channels=["wecom"])

        self.assertEqual(self.notice_rows(), [])
        shadow = self.base / "shadow" / "wecom-outbox.jsonl"
        self.assertEqual(json.loads(shadow.read_text())["text"], "测试通知")

    def test_core_health_notice_replays_by_event_id(self):
        self.write_modes("core")
        self.env()

        for event_id in ("first-failed", "first-failed", "second-failed"):
            self.relay.notify("审批监听连接异常", "credential", channels=["wecom"], event_id=event_id)

        self.assertEqual({row["id"] for row in self.notice_rows()},
                         {"appr-health-first-failed-wecom", "appr-health-second-failed-wecom"})

    def test_pending_approval_replays_by_platform_id(self):
        self.write_modes("core")
        self.env()
        item = {"approval_id": "platform-id-1", "who": "op", "what": "命令：echo hi",
                "decision_scope": {"channel": "wecom", "chat_id": "room"}}

        self.relay.send_approval_notice("1", item)
        self.relay.send_approval_notice("1", item)
        self.relay.send_approval_notice("1", dict(item, approval_id="platform-id-2"))

        rows = self.notice_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["text"], rows[1]["text"])

    def test_core_notice_without_route_is_retained_for_retry(self):
        self.write_modes("core")
        self.env()
        self.relay.CHANNELS["wecom"]["row"]["chatid"] = ""

        with self.assertRaises(OSError):
            self.relay.notify("审批通知", "new1", channels=["wecom"])
        self.assertEqual(self.notice_rows(), [])

    def test_wecom_legacy_mode_still_uses_outbox(self):
        self.write_modes("legacy")
        self.env()
        self.relay.notify("待审批 #1", "new1", channels=["wecom"])
        self.assertEqual(len(self.outbox_rows("wecom")), 1)
        self.assertEqual(self.notice_rows(), [])

    def test_weixin_legacy_unaffected_when_wecom_is_core(self):
        self.write_modes("core")
        self.env()
        self.relay.notify("待审批 #1", "new1", channels=["weixin"])
        self.assertEqual(len(self.outbox_rows("weixin")), 1)
        self.assertEqual(self.notice_rows(), [])

    def test_core_write_failure_raises_oserror(self):
        # database 指向一个目录：建表/写入必然失败，调用方必须能重试。
        self.modes.write_text(json.dumps({"channels": {
            "weixin": {"mode": "legacy"},
            "wecom": {"mode": "core", "database": str(self.base),
                      "account_id": "wecom-bot",
                      "allowed_senders": ["alice"],
                      "administrators": ["alice"]}}}))
        self.env()
        with self.assertRaises(OSError):
            self.relay.notify("待审批 #1", "new1", channels=["wecom"])
        self.assertEqual(self.outbox_rows("wecom"), [])

    def test_table_creation_is_idempotent(self):
        self.write_modes("core")
        self.env()
        with sqlite3.connect(self.db) as conn:
            conn.execute(APPROVAL_NOTICES_DDL)
            conn.execute(
                "INSERT INTO approval_notices "
                "(id, channel, chat_id, text, state, created_at) "
                "VALUES ('seed', 'wecom', 'room', 'old', 'delivered', 1.0)")
            conn.commit()
        self.relay.notify("待审批 #1", "new1", channels=["wecom"])
        rows = self.notice_rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["id"] for r in rows}, {"seed",
                         next(r["id"] for r in rows if r["id"] != "seed")})
        self.assertEqual(
            next(r for r in rows if r["id"] == "seed")["state"], "delivered")

    def test_no_modes_env_means_legacy_outbox(self):
        # 未配 MUSE_CHANNEL_MODES 时沿用旧行为（部署方须给 relay 配该变量）。
        self.write_modes("core")
        patcher = mock.patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("MUSE_CHANNEL_MODES", None)
        self.relay.notify("待审批 #1", "new1", channels=["wecom"])
        self.assertEqual(len(self.outbox_rows("wecom")), 1)
        self.assertEqual(self.notice_rows(), [])

    def test_legacy_outboxes_follow_relocated_relay_module(self):
        """A custom home can deploy the relay without retaining hatch paths."""
        deployment = self.base / "custom-home" / "workspace"
        relay_path = deployment / "approval-relay" / "approval_relay.py"
        relay_path.parent.mkdir(parents=True)
        relay_path.write_bytes((ROOT / "approval-relay" / "approval_relay.py").read_bytes())
        spec = importlib.util.spec_from_file_location("relocated_approval_relay", relay_path)
        relocated = importlib.util.module_from_spec(spec)

        with mock.patch.object(sys, "path", list(sys.path)):
            spec.loader.exec_module(relocated)

        for channel in ("weixin", "wecom"):
            self.assertEqual(Path(relocated.CHANNELS[channel]["outbox"]),
                             deployment.resolve() / f"{channel}-bot" / "state" / "outbox.jsonl")


if __name__ == "__main__":
    unittest.main()
