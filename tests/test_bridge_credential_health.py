"""Exercise source fallback and durable alerts through actual bridge workers."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from credential_health import credential_fingerprint, failure_kind


class CredentialHealthTests(unittest.TestCase):
    """Never read real credentials or require the external Muse SDK."""

    def setUp(self):
        """Use a real isolated state file and injected source parameters."""
        spec = importlib.util.spec_from_file_location("credential_bridge", ROOT / "native-bridge/native_bridge.py")
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bridge.BASE = self.temp.name
        self.bridge.CONF_DIR = self.temp.name
        self.bridge.STATE_F = str(Path(self.temp.name) / "state.json")
        self.worker = self.bridge.ChannelWorker("test")
        self.worker._send_alert_text = Mock(return_value=True)
        self.cookie = patch.object(self.bridge, "cookie_params", return_value=("private", "vm"))
        self.cookie_params = self.cookie.start()
        self.addCleanup(self.cookie.stop)
        self.token = patch.object(self.bridge, "token_params", return_value={
            "vm_id": "vm", "access_token": "private", "hatch_token": "private"})
        self.token_params = self.token.start()
        self.addCleanup(self.token.stop)
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def connect(self, *clients):
        """Run the actual worker acquisition with deterministic SDK clients."""
        for client in clients:
            if isinstance(client.call_json.return_value, Mock):
                client.call_json.return_value = {"sessions": []}
        with patch.object(self.bridge, "gateway_class", return_value=Mock(side_effect=clients)):
            return self.worker._acquire_connection()

    def health(self):
        """Read durable health, including notification acknowledgements."""
        return self.worker.cs()["credential_health"]

    def test_cookie_parse_error_still_uses_token(self):
        """A malformed cookie file cannot prevent the independent token path."""
        self.cookie_params.side_effect = ValueError("sensitive invalid cookie")
        token = Mock()
        client, error = self.connect(token)
        self.assertIs(client, token)
        self.assertIsNone(error)
        self.token_params.assert_called_once()
        self.assertEqual(self.health()["sources"]["cookies"]["failure_kind"], "configuration")
        self.assertEqual(self.worker.cs()["active_source"], "token")
        self.worker._send_alert_text.assert_called_once()
        self.assertIn("token 回退验证通过", self.worker._send_alert_text.call_args.args[0])
        self.assertNotIn("sensitive", Path(self.bridge.STATE_F).read_text())

    def test_cookie_auth_failure_closes_before_token_fallback(self):
        """A refused authenticated RPC leaves no abandoned client behind."""
        cookie, token = Mock(), Mock()
        cookie.call_json.side_effect = RuntimeError("auth/check -> 401 secret")
        self.assertIs(self.connect(cookie, token)[0], token)
        cookie.close.assert_called_once()
        token.close.assert_not_called()
        self.assertEqual(self.health()["sources"]["cookies"]["status"], "failed")

    def test_both_sources_fail_close_and_raise_safe_auth_error(self):
        """Neither the source reports nor propagated error expose SDK bodies."""
        cookie, token = Mock(), Mock()
        for client in (cookie, token):
            client.call_json.side_effect = RuntimeError("HTTP 403 PRIVATE_TOKEN")
        gateway, error = self.connect(cookie, token)
        self.assertIsNone(gateway)
        self.assertEqual(failure_kind(error), "auth")
        self.assertNotIn("PRIVATE_TOKEN", str(error))
        self.assertEqual(self.worker.cs()["active_source"], None)
        self.assertEqual(self.worker._send_alert_text.call_count, 1)
        cookie.close.assert_called_once()
        token.close.assert_called_once()

    def test_environment_token_skips_cookies(self):
        """The existing explicit token override stays authoritative."""
        with patch.dict(os.environ, {"NATIVE_TOKEN_JSON": "present"}):
            token = Mock()
            self.assertIs(self.connect(token)[0], token)
        self.cookie_params.assert_not_called()
        self.assertEqual(self.health()["sources"]["token"]["origin"], "environment")

    def test_token_success_does_not_resolve_cookie_outage(self):
        """RPC success bookkeeping cannot erase the independent Cookie failure."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        self.worker._note_reconnect_success()
        self.connect(Mock())
        self.assertTrue(self.health()["incident"]["active"])
        self.assertEqual(self.worker._send_alert_text.call_count, 1)

    def test_cookie_success_emits_one_recovery(self):
        """The required primary source must actually pass its RPC first."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        self.cookie_params.side_effect = None
        self.connect(Mock())
        self.connect(Mock())
        self.assertFalse(self.health()["incident"]["active"])
        self.assertEqual(self.worker._send_alert_text.call_count, 2)
        self.assertIn("真实消息收发仍需", self.worker._send_alert_text.call_args.args[0])

    def test_fault_dedup_survives_worker_restart(self):
        """Process startup does not generate a second credential incident."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        incident_id = self.health()["incident"]["id"]
        self.worker = self.bridge.ChannelWorker("test")
        self.worker._send_alert_text = Mock(return_value=True)
        self.connect(Mock())
        self.assertEqual(self.health()["incident"]["id"], incident_id)
        self.worker._send_alert_text.assert_not_called()

    def test_failed_alert_retries_with_same_id(self):
        """A failed outbox write does not acknowledge notification delivery."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.worker._send_alert_text.side_effect = [False, True]
        self.connect(Mock())
        pending = self.health()["pending_notices"][0]["id"]
        self.worker._flush_credential_notices()
        self.assertEqual(self.health()["pending_notices"], [])
        self.assertEqual(self.worker._send_alert_text.call_args.kwargs["event_id"], pending)

    def test_network_error_does_not_claim_invalid_credentials(self):
        """A transient timeout is managed by the existing reconnect budget."""
        cookie, token = Mock(), Mock()
        cookie.call_json.side_effect = TimeoutError("private endpoint")
        token.call_json.side_effect = TimeoutError("private endpoint")
        self.connect(cookie, token)
        self.assertNotIn("incident", self.health())
        self.worker._send_alert_text.assert_not_called()

    def test_new_outage_after_recovery_can_notify(self):
        """Recovery closes the old generation instead of silencing all future failures."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        first = self.health()["incident"]["id"]
        self.cookie_params.side_effect = None
        self.connect(Mock())
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        self.assertNotEqual(self.health()["incident"]["id"], first)
        self.assertEqual(self.worker._send_alert_text.call_count, 3)

    def test_token_validation_rejects_empty_required_fields(self):
        """Malformed input is reported as configuration instead of a network fault."""
        self.token.stop()
        with patch.dict(os.environ, {"NATIVE_TOKEN_JSON": json.dumps({"vm_id": "vm"})}):
            with self.assertRaises(ValueError):
                self.bridge.token_params()

    def test_unrelated_numbers_are_not_authentication_errors(self):
        """Numeric IDs in unrelated errors must not trigger a credential incident."""
        self.assertEqual(failure_kind(RuntimeError("task 401 failed after 403 seconds")), "network")
        self.assertEqual(failure_kind(RuntimeError("HTTP 401")), "auth")

    def test_file_replacement_changes_metadata_fingerprint(self):
        """Atomic replacement is detectable even for same-size restored files."""
        cookie = Path(self.temp.name) / "cookies.txt"
        cookie.write_text("one")
        original = cookie.stat()
        first = credential_fingerprint(self.temp.name, (cookie.name,))
        replacement = cookie.with_suffix(".new")
        replacement.write_text("two")
        os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
        os.replace(replacement, cookie)
        self.assertNotEqual(first, credential_fingerprint(self.temp.name, (cookie.name,)))

    def test_degraded_to_unavailable_notifies_the_change_once(self):
        """A previously working fallback cannot fail silently behind the first alert."""
        self.cookie_params.side_effect = ValueError("bad cookie")
        self.connect(Mock())
        token = Mock()
        token.call_json.side_effect = RuntimeError("HTTP 401")
        self.connect(token)
        self.worker._notify_reconnect_pause()
        self.assertEqual(self.worker._send_alert_text.call_count, 2)
        self.assertIn("token 回退也已验证失败", self.worker._send_alert_text.call_args.args[0])
        self.assertEqual(self.worker.cs()["active_source"], None)
        self.assertEqual(self.health()["incident"]["severity"], "unavailable")

    def test_actual_operation_auth_failure_updates_sources_immediately(self):
        """A history/poll auth error must be visible before the 600-second pause."""
        self.connect(Mock())
        self.worker._note_reconnect_failure(RuntimeError("auth/check -> 401"))
        self.assertEqual(self.worker.cs()["credential_sources"]["cookies"]["status"], "failed")
        self.assertIsNone(self.worker.cs()["active_source"])
        self.worker._send_alert_text.assert_called_once()
        self.assertGreater(self.worker._auth_blocked_until, 0)

    def test_notification_ack_failure_does_not_append_duplicate_event(self):
        """Crash after outbox commit is safe even for unbound WeChat send rows."""
        (Path(self.temp.name) / "shadow").mkdir()
        self.worker._send_alert_text = self.bridge.ChannelWorker._send_alert_text.__get__(self.worker)
        self.cookie_params.side_effect = ValueError("bad cookie")
        real_save = self.bridge.save_state

        def fail_ack(state):
            """Fail only after the event has physically entered the outbox."""
            health = state["channels"]["test"].get("credential_health", {})
            if health.get("incident", {}).get("fault_queued"):
                raise OSError("ack save unavailable")
            real_save(state)

        with patch.object(self.bridge, "save_state", side_effect=fail_ack):
            self.connect(Mock())
        self.assertEqual(len(self.health()["pending_notices"]), 1)
        self.worker = self.bridge.ChannelWorker("test")
        self.worker._flush_credential_notices()
        self.assertEqual(self.health()["pending_notices"], [])
        rows = (Path(self.temp.name) / "shadow/test-outbox.jsonl").read_text().splitlines()
        self.assertEqual(len(rows), 1)

    def test_partial_cookie_files_are_configuration_error_before_sdk_import(self):
        """Incomplete deployment input cannot be mistaken for a missing SDK."""
        self.cookie.stop()
        (Path(self.temp.name) / "vm_id.txt").write_text("vm")
        with self.assertRaises(ValueError):
            self.bridge.cookie_params()

    def test_unsuccessful_rpc_response_is_not_verification(self):
        """Explicit RPC errors and empty protocol results cannot prove recovery."""
        for response in (None, {"error": "PRIVATE"}, {"ok": False}):
            with self.subTest(response=response):
                cookie, token = Mock(), Mock()
                cookie.call_json.return_value = response
                self.worker._next_connect_at = 0
                self.assertIs(self.connect(cookie, token)[0], token)
                cookie.close.assert_called_once()
                self.assertEqual(self.worker.cs()["credential_sources"]["cookies"]["status"], "failed")

    def test_disappearing_previously_verified_cookie_is_not_silent(self):
        """Token fallback must not hide removal of a previously working source."""
        self.connect(Mock())
        self.cookie_params.return_value = None
        self.connect(Mock())
        self.assertEqual(self.worker.cs()["credential_sources"]["cookies"]["status"], "missing")
        self.assertTrue(self.health()["incident"]["active"])
        self.worker._send_alert_text.assert_called_once()

    def test_initial_token_only_configuration_does_not_claim_cookie_expired(self):
        """An intentionally absent primary is distinguished from a lost one."""
        self.cookie_params.return_value = None
        self.connect(Mock())
        self.assertNotIn("incident", self.health())
        self.worker._send_alert_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()
