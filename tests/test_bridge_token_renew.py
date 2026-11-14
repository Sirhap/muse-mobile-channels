"""Daily bridge-token renewal as an installable systemd timer.

Covers the bridge-token-renew units, their wiring in ops/install.sh, and
the run.sh wrapper (renew_token.py exit codes + post-renewal verification).
The wrapper is exercised with a stub python so no live credentials are
touched; the real renewal is verified separately by running run.sh once.
"""
from pathlib import Path
import os
import stat
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
RENEW_DIR = ROOT / "bridge-token-renew"


def _render(text: str, home: Path) -> str:
    return text.replace("/home/hatch", str(home))


class RenewUnitTests(unittest.TestCase):
    def test_service_and_timer_render_for_any_home(self):
        """Units carry no hard-coded install home after rendering."""
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "install home"
            service = _render((RENEW_DIR / "bridge-token-renew.service").read_text(), home)
            timer = _render((RENEW_DIR / "bridge-token-renew.timer").read_text(), home)
            self.assertNotIn("/home/hatch", service)
            self.assertNotIn("/home/hatch", timer)
            self.assertIn("Type=oneshot", service)
            self.assertIn(f"ExecStart={home}/workspace/bridge-token-renew/run.sh", service)
            self.assertIn(f"Environment=HOME={home}", service)
            self.assertIn("OnCalendar=*-*-* 04:54:00", timer)
            self.assertIn("Persistent=true", timer)
            self.assertIn("Unit=bridge-token-renew.service", timer)
            self.assertIn("WantedBy=timers.target", timer)

    def test_install_script_wires_the_timer(self):
        """ops/install.sh always installs both units; the service is timer-driven."""
        text = (ROOT / "ops/install.sh").read_text()
        self.assertIn('add_unit "bridge-token-renew/bridge-token-renew.service"', text)
        self.assertIn('add_unit "bridge-token-renew/bridge-token-renew.timer"', text)
        self.assertIn("bridge-token-renew.service) return 1", text)
        subprocess.run(["bash", "-n", str(ROOT / "ops/install.sh")], check=True)

    def test_run_sh_is_executable_and_parses(self):
        run_sh = RENEW_DIR / "run.sh"
        self.assertTrue(bool(run_sh.stat().st_mode & stat.S_IXUSR))
        subprocess.run(["bash", "-n", str(run_sh)], check=True)


class RunShTests(unittest.TestCase):
    def _run_with_stub(self, renew_rc, check_rc):
        """Run run.sh with a stub python; return (exit code, stub call log)."""
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "muse-test-venv" / "bin").mkdir(parents=True)
            bridge = home / "workspace" / "native-bridge"
            bridge.mkdir(parents=True)
            (bridge / "renew_token.py").write_text("# stub\n")
            (bridge / "check_credentials.py").write_text("# stub\n")
            log = home / "stub.log"
            stub = home / "muse-test-venv" / "bin" / "python"
            stub.write_text(
                "#!/usr/bin/env bash\n"
                f'echo "$@" >> "{log}"\n'
                'for a in "$@"; do\n'
                f'  case "$a" in *renew_token.py) exit {renew_rc};; *check_credentials.py) exit {check_rc};; esac\n'
                "done\n"
                "exit 0\n"
            )
            stub.chmod(0o755)
            env = {"HOME": str(home), "PATH": "/usr/bin:/bin"}
            proc = subprocess.run(
                ["bash", str(RENEW_DIR / "run.sh")],
                capture_output=True, text=True, env=env, timeout=15)
            calls = log.read_text() if log.exists() else ""
            return proc.returncode, calls

    def test_success_runs_verification(self):
        rc, calls = self._run_with_stub(renew_rc=0, check_rc=0)
        self.assertEqual(rc, 0)
        self.assertIn("renew_token.py", calls)
        self.assertIn("check_credentials.py", calls)

    def test_skip_exits_zero_without_verification(self):
        rc, calls = self._run_with_stub(renew_rc=2, check_rc=0)
        self.assertEqual(rc, 0)
        self.assertIn("renew_token.py", calls)
        self.assertNotIn("check_credentials.py", calls)

    def test_renewal_failure_exits_one(self):
        rc, _ = self._run_with_stub(renew_rc=1, check_rc=0)
        self.assertEqual(rc, 1)

    def test_failed_verification_exits_one(self):
        rc, _ = self._run_with_stub(renew_rc=0, check_rc=1)
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
