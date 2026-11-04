"""Read-only probes must expose source-specific failures without credential leaks."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "native-bridge/check_credentials.py"


def load_diagnostics():
    """Import the CLI without resolving its optional platform dependencies."""
    spec = importlib.util.spec_from_file_location("credential_diagnostics_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CredentialDiagnosticsTests(unittest.TestCase):
    """Exercise real probe boundaries using isolated SDK substitutes only."""

    def setUp(self):
        """Supply recognizable fake secrets so leak assertions cover error paths."""
        self.diagnostics = load_diagnostics()
        self.bridge = Mock()
        self.bridge.cookie_params.return_value = ("COOKIE_SECRET", "VM_SECRET")
        self.bridge.token_params.return_value = {
            "vm_id": "VM_SECRET", "access_token": "ACCESS_SECRET", "hatch_token": "HATCH_SECRET"}
        self.clients = []

        def create(**kwargs):
            """Remember each independent client without using any real network."""
            client = Mock()
            client.call_json.return_value = {}
            self.clients.append(client)
            return client

        self.bridge.gateway_class.return_value.side_effect = create
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        os.environ.pop("NATIVE_TOKEN_JSON", None)

    def test_all_checks_are_separate_and_close_their_clients(self):
        """Cookie and token probes cannot reuse a connection that hides source identity."""
        report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertEqual([(row["source"], row["check"]) for row in report["checks"]],
                         [("cookies", "bridge"), ("cookies", "approval"), ("token", "bridge")])
        self.assertTrue(all(row["status"] == "healthy" for row in report["checks"]))
        self.assertTrue(all(row["origin"] == "file" for row in report["checks"]))
        self.assertEqual(len(self.clients), 3)
        for client, method in zip(self.clients, ("sessions.list", "egress.approvals", "sessions.list")):
            client.call_json.assert_called_once_with(method, timeout=20)
            client.close.assert_called_once_with()

    def test_token_success_does_not_mask_cookie_auth_failure(self):
        """A successful fallback leaves Cookie and approval paths visibly failed."""
        def create(**kwargs):
            """Only Cookie construction fails in this two-source fixture."""
            if kwargs["cookies"]:
                raise RuntimeError("auth/check 401 COOKIE_SECRET")
            client = Mock()
            client.call_json.return_value = {}
            return client

        self.bridge.gateway_class.return_value.side_effect = create
        report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertEqual([row["status"] for row in report["checks"]], ["failed", "failed", "healthy"])
        self.assertEqual([row["failure_kind"] for row in report["checks"]], ["auth", "auth", None])
        self.assertNotIn("SECRET", json.dumps(report))

    def test_cookie_parse_failure_still_checks_token(self):
        """Bad Cookie input does not stop independent token diagnostics."""
        self.bridge.cookie_params.side_effect = ValueError("COOKIE_SECRET")
        report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertEqual([row["status"] for row in report["checks"]], ["failed", "failed", "healthy"])
        self.assertEqual(report["checks"][0]["failure_kind"], "configuration")
        self.bridge.token_params.assert_called_once_with()

    def test_approval_failure_does_not_change_cookie_bridge_verdict(self):
        """Approval RPC health is checked independently from sessions.list."""
        bridge_client, approval_client = Mock(), Mock()
        bridge_client.call_json.return_value = {}
        approval_client.call_json.side_effect = TimeoutError("HATCH_SECRET")
        self.bridge.gateway_class.return_value.side_effect = [bridge_client, approval_client]
        report = self.diagnostics.run_checks("cookies", bridge=self.bridge)
        self.assertEqual([row["status"] for row in report["checks"]], ["healthy", "failed"])
        self.assertEqual(report["checks"][1]["failure_kind"], "network")
        bridge_client.close.assert_called_once()
        approval_client.close.assert_called_once()

    def test_missing_sources_do_not_resolve_sdk(self):
        """An unconfigured machine reports missing inputs without importing the SDK."""
        self.bridge.cookie_params.return_value = None
        self.bridge.token_params.return_value = None
        report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertEqual([row["status"] for row in report["checks"]], ["missing"] * 3)
        self.bridge.gateway_class.assert_not_called()

    def test_missing_sdk_is_safe_dependency_failure(self):
        """Dependency errors are actionable but contain no module error text."""
        self.bridge.gateway_class.side_effect = ImportError("SDK_SECRET")
        report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertTrue(all(row["failure_kind"] == "dependency" for row in report["checks"]))
        self.assertNotIn("SDK_SECRET", json.dumps(report))

    def test_bridge_loader_failure_is_safe_and_structured(self):
        """A damaged or unavailable helper still produces a machine-readable report."""
        with patch.object(self.diagnostics, "_bridge_module", side_effect=ImportError("SDK_SECRET")):
            report = self.diagnostics.run_checks()
        self.assertEqual(len(report["checks"]), 3)
        self.assertTrue(all(row["status"] == "failed" and row["failure_kind"] == "dependency"
                            for row in report["checks"]))
        self.assertNotIn("SDK_SECRET", json.dumps(report))

    def test_malformed_token_does_not_construct_client(self):
        """Missing mandatory values fail before any network operation."""
        for params in ({"vm_id": "VM_SECRET"}, [], {"vm_id": "", "access_token": "x", "hatch_token": "x"}):
            with self.subTest(params=params):
                self.bridge.token_params.return_value = params
                report = self.diagnostics.run_checks("token", bridge=self.bridge)
                self.assertEqual(report["checks"][0]["failure_kind"], "configuration")
        self.bridge.gateway_class.assert_not_called()

    def test_real_token_helper_uses_environment_before_file(self):
        """Probe token loading has the same explicit environment priority as the bridge."""
        bridge = self.diagnostics._bridge_module()
        token = {"vm_id": "ENV_VM", "access_token": "ENV_ACCESS", "hatch_token": "ENV_HATCH"}
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "token.json").write_text("not JSON", encoding="utf-8")
            bridge.CONF_DIR = directory
            bridge.GW2 = Mock()
            bridge.GW2.return_value.call_json.return_value = {}
            with patch.dict(os.environ, {"NATIVE_TOKEN_JSON": json.dumps(token)}):
                report = self.diagnostics.run_checks("token", bridge=bridge)
        bridge.GW2.assert_called_once_with(cookies="", **token)
        self.assertEqual(report["checks"][0]["status"], "healthy")
        self.assertEqual(report["checks"][0]["origin"], "environment")

    def test_explicit_cookie_check_ignores_environment_token(self):
        """Environment token cannot make a Cookie-only diagnostic appear healthy."""
        self.bridge.cookie_params.side_effect = ValueError("COOKIE_SECRET")
        with patch.dict(os.environ, {"NATIVE_TOKEN_JSON": "ENV_SECRET"}):
            report = self.diagnostics.run_checks("cookies", bridge=self.bridge)
        self.bridge.token_params.assert_not_called()
        self.assertTrue(all(row["origin"] == "file" and row["status"] == "failed" for row in report["checks"]))

    def test_credential_directory_override_is_used_by_loaded_bridge(self):
        """An explicit diagnostic directory cannot accidentally target real credentials."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, {"NATIVE_BRIDGE_CREDENTIALS": directory}):
            bridge = self.diagnostics._bridge_module()
            self.assertEqual(Path(bridge.CONF_DIR), Path(directory))

    def test_sdk_output_and_exception_text_are_suppressed(self):
        """Unexpected third-party logging must not escape the allowlisted report."""
        def create(**kwargs):
            """Simulate a chatty SDK that prints secrets before raising."""
            print("stdout COOKIE_SECRET")
            print("stderr ACCESS_SECRET", file=sys.stderr)
            raise RuntimeError("upgrade: 403 HATCH_SECRET")

        self.bridge.gateway_class.return_value.side_effect = create
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            report = self.diagnostics.run_checks(bridge=self.bridge)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn("SECRET", json.dumps(report))

    def test_cleanup_failure_keeps_original_rpc_result(self):
        """Failed close neither masks authentication failure nor fabricates recovery."""
        client = Mock()
        client.call_json.side_effect = RuntimeError("auth/check 401 COOKIE_SECRET")
        client.close.side_effect = RuntimeError("CLOSE_SECRET")
        self.bridge.gateway_class.return_value.side_effect = None
        self.bridge.gateway_class.return_value.return_value = client
        report = self.diagnostics.run_checks("token", bridge=self.bridge)
        self.assertEqual(report["checks"][0]["failure_kind"], "auth")
        self.assertTrue(report["checks"][0]["cleanup_failed"])
        self.assertNotIn("SECRET", json.dumps(report))

    def test_main_returns_failure_for_missing_or_cleanup_error(self):
        """CLI exit status distinguishes fully verified health from partial results."""
        for status, cleanup, expected in (("healthy", False, 0), ("failed", False, 1),
                                          ("missing", False, 1), ("healthy", True, 1)):
            row = {"source": "token", "origin": "file", "check": "bridge", "status": status,
                   "checked_at": 1, "failure_kind": None}
            if cleanup:
                row["cleanup_failed"] = True
            with self.subTest(status=status, cleanup=cleanup), patch.object(
                    self.diagnostics, "run_checks", return_value={"checks": [row]}):
                output = io.StringIO()
                with redirect_stdout(output):
                    code = self.diagnostics.main(["--json", "--source", "token"])
                self.assertEqual(code, expected)
                self.assertEqual(json.loads(output.getvalue()), {"checks": [row]})

    def test_explicit_rpc_error_is_not_healthy_or_exposed(self):
        """An error payload cannot serve as successful authentication evidence."""
        for response in ({"error": {"message": "PRIVATE_SECRET"}}, {"ok": False}):
            with self.subTest(response=response):
                client = Mock()
                client.call_json.return_value = response
                self.bridge.gateway_class.return_value.side_effect = None
                self.bridge.gateway_class.return_value.return_value = client
                report = self.diagnostics.run_checks("token", bridge=self.bridge)
                self.assertEqual(report["checks"][0]["status"], "failed")
                self.assertEqual(report["checks"][0]["failure_kind"], "network")
                self.assertNotIn("PRIVATE_SECRET", json.dumps(report))
                client.close.assert_called_once()

    def test_sessions_reject_null_and_scalars_but_accept_direct_list(self):
        """Unknown SDK envelopes stay compatible without treating no response as success."""
        for response, expected in ((None, "failed"), ("PRIVATE_SECRET", "failed"),
                                   (False, "failed"), (200, "failed"), ([], "healthy"),
                                   ([{"id": "PRIVATE_SECRET"}], "healthy")):
            with self.subTest(response=response):
                client = Mock()
                client.call_json.return_value = response
                self.bridge.gateway_class.return_value.side_effect = None
                self.bridge.gateway_class.return_value.return_value = client
                report = self.diagnostics.run_checks("token", bridge=self.bridge)
                self.assertEqual(report["checks"][0]["status"], expected)
                self.assertNotIn("PRIVATE_SECRET", json.dumps(report))
                client.close.assert_called_once()

    def test_approval_requires_object_while_bridge_accepts_list(self):
        """Approval health must satisfy the relay's existing object response contract."""
        for response in (None, [], "PRIVATE_SECRET"):
            with self.subTest(response=response):
                bridge_client, approval_client = Mock(), Mock()
                bridge_client.call_json.return_value = []
                approval_client.call_json.return_value = response
                self.bridge.gateway_class.return_value.side_effect = [bridge_client, approval_client]
                report = self.diagnostics.run_checks("cookies", bridge=self.bridge)
                self.assertEqual([row["status"] for row in report["checks"]], ["healthy", "failed"])
                self.assertNotIn("PRIVATE_SECRET", json.dumps(report))
                approval_client.close.assert_called_once()

    def test_cli_runs_without_sdk_or_workspace_writes(self):
        """The actual entrypoint works from any cwd against an empty credential directory."""
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, HOME=directory, NATIVE_BRIDGE_CREDENTIALS=directory,
                               PYTHONDONTWRITEBYTECODE="1")
            environment.pop("NATIVE_TOKEN_JSON", None)
            result = subprocess.run([sys.executable, str(SCRIPT), "--json"], cwd=directory,
                                    env=environment, capture_output=True, text=True, timeout=10)
            files = list(Path(directory).iterdir())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(files, [])
        self.assertEqual(result.stderr, "")
        self.assertTrue(all(row["status"] == "missing" for row in json.loads(result.stdout)["checks"]))


if __name__ == "__main__":
    unittest.main()
