#!/usr/bin/env python3
"""Fresh-process connect regression (2026-11-05, fix/bridge-credential-health).

aa10d39 rewrote connect() so _connect_source() read cookie_params()
before gateway_class() had ever run. load_cookies needs muse_cli, and
muse_cli only becomes importable as a side effect of importing gw2, so
in a fresh process the cookies source died with ModuleNotFoundError,
the token fallback got a bare-Bearer 403, and connect() raised
CredentialAuthError although the cookies were fine. The 151 unit tests
missed it because the test harness preloads the SDK path.

These tests spawn a FRESH interpreter (isolated mode, no PYTHONPATH,
cwd=/tmp, only the native-bridge dir on sys.path) so the production
import order is exercised exactly. They are live probes: they need the
real credential files in ~/.config/native-bridge and are skipped when
those files are absent. Run:
  ~/muse-test-venv/bin/python -m unittest tests/test_bridge_connect_fresh_process.py -v
"""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BRIDGE_DIR = str(ROOT / "native-bridge")
CONF_DIR = Path(os.path.expanduser("~/.config/native-bridge"))

_FRESH_PREAMBLE = (
    "import json, sys; sys.path.insert(0, sys.argv[1]); "
    "import native_bridge as nb; "
)


def _fresh_env():
    env = {k: v for k, v in os.environ.items()
           if k not in ("PYTHONPATH", "PYTHONSTARTUP", "MUSE_CLI_SRC")}
    return env


def _run_fresh(script, timeout=120):
    """Run script in a fresh isolated interpreter; return (rc, stdout, stderr)."""
    proc = subprocess.run(
        [sys.executable, "-I", "-c", _FRESH_PREAMBLE + script, BRIDGE_DIR],
        cwd="/tmp", env=_fresh_env(), capture_output=True, text=True,
        timeout=timeout)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _credentials_present():
    return (CONF_DIR / "cookies.txt").is_file() and (CONF_DIR / "vm_id.txt").is_file()


class FreshProcessConnectTests(unittest.TestCase):
    def test_cookie_params_without_gateway_class(self):
        """cookie_params() must not need gateway_class() to have run first."""
        if not _credentials_present():
            self.skipTest("no cookies.txt/vm_id.txt in ~/.config/native-bridge")
        rc, out, err = _run_fresh(
            "params = nb.cookie_params(); "
            "print(json.dumps({'ok': isinstance(params, tuple) and len(params) == 2 "
            "and bool(params[0].strip()) and bool(params[1].strip())}))")
        self.assertEqual(rc, 0, f"fresh cookie_params crashed: {err[-500:]}")
        self.assertEqual(json.loads(out), {"ok": True}, f"unexpected output: {out} {err[-500:]}")

    def test_connect_without_gateway_class(self):
        """connect() in a fresh process must reach a healthy source."""
        if not _credentials_present():
            self.skipTest("no cookies.txt/vm_id.txt in ~/.config/native-bridge")
        rc, out, err = _run_fresh(
            "gw = nb.connect(); print('CONNECT_OK'); gw.close()")
        self.assertEqual(rc, 0, f"fresh connect() crashed: {err[-500:]}")
        self.assertEqual(out, "CONNECT_OK", f"unexpected output: {out} {err[-500:]}")


if __name__ == "__main__":
    unittest.main()
