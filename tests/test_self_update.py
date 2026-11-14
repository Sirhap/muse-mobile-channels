"""Tests for the scheduled self-updater (ops/self_update.py).

All tests run in temp dirs with fake releases; nothing here touches the
network, a real GitHub Release, or a real systemd service. The apply-path
test always passes --no-restart so no production service can be restarted.
"""

import io
import json
import socket
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ops"))

import self_update  # noqa: E402

UPDATER = [sys.executable, str(ROOT / "ops" / "self_update.py")]


def make_tarball(path, version, extra_files=None, version_content=None,
                 prefix=None):
    """Build a fake release tarball. extra_files: {member: content}."""
    prefix = prefix or f"muse-mobile-channels-{version}/"
    with tarfile.open(path, "w:gz") as archive:
        content = (version_content if version_content is not None
                   else version + "\n")
        data = io.BytesIO(content.encode("utf-8"))
        info = tarfile.TarInfo(prefix + "VERSION")
        info.size = len(data.getvalue())
        archive.addfile(info, data)
        for name, text in (extra_files or {}).items():
            data = io.BytesIO(text.encode("utf-8"))
            info = tarfile.TarInfo(prefix + name)
            info.size = len(data.getvalue())
            archive.addfile(info, data)
    return path


def make_install_dir(base, version="2026.11.14.1", with_git=True):
    """A fake deployed checkout: VERSION + one code file + a state file."""
    install = Path(base) / "checkout"
    install.mkdir()
    (install / "VERSION").write_text(version + "\n", encoding="utf-8")
    (install / "app.py").write_text("print('old')\n", encoding="utf-8")
    state_dir = install / "state"
    state_dir.mkdir()
    (state_dir / "core.sqlite3").write_text("STATE-MUST-SURVIVE",
                                            encoding="utf-8")
    (install / "local-notes.txt").write_text("untracked user file",
                                             encoding="utf-8")
    if with_git:
        subprocess.run(["git", "init", "-q", str(install)], check=True,
                       timeout=30)
        subprocess.run(["git", "-C", str(install), "config", "user.email",
                        "test@example.com"], check=True, timeout=30)
        subprocess.run(["git", "-C", str(install), "config", "user.name",
                        "Test"], check=True, timeout=30)
        subprocess.run(["git", "-C", str(install), "add", "VERSION", "app.py",
                        "state/core.sqlite3"], check=True, timeout=30)
        subprocess.run(["git", "-C", str(install), "commit", "-qm", "init"],
                       check=True, timeout=30)
    return install


class TestVersionCompare(unittest.TestCase):
    def test_newer_detected(self):
        parse = self_update.parse_version
        self.assertLess(parse("2026.11.14.1"), parse("2026.11.14.2"))
        self.assertLess(parse("2026.11.14.9"), parse("2026.11.15.1"))
        self.assertFalse(parse("2026.11.15.1") <= parse("2026.11.14.9"))

    def test_tag_to_version(self):
        self.assertEqual(self_update.tag_to_version("v2026.11.14.1"),
                         (2026, 11, 14, 1))
        with self.assertRaises(ValueError):
            self_update.tag_to_version("v0.1.0")


class TestTarballVerify(unittest.TestCase):
    def test_good_tarball_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            tarball = Path(tmp) / "good.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         {"app.py": "print('new')\n"})
            self_update.verify_tarball(tarball, "2026.11.15.1")

    def test_tarball_with_top_level_dir_entry_passes(self):
        # git archive emits the top-level directory itself as a member
        # without a trailing slash; that entry must be accepted.
        with tempfile.TemporaryDirectory() as tmp:
            tarball = Path(tmp) / "good.tar.gz"
            prefix = "muse-mobile-channels-2026.11.15.1/"
            with tarfile.open(tarball, "w:gz") as archive:
                info = tarfile.TarInfo(prefix.rstrip("/"))
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
                data = io.BytesIO(b"2026.11.15.1\n")
                info = tarfile.TarInfo(prefix + "VERSION")
                info.size = len(data.getvalue())
                archive.addfile(info, data)
            self_update.verify_tarball(tarball, "2026.11.15.1")

    def test_wrong_version_content_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tarball = Path(tmp) / "bad.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         version_content="2026.11.14.1\n")
            with self.assertRaises(self_update.UpdateError):
                self_update.verify_tarball(tarball, "2026.11.15.1")

    def test_forbidden_members_rejected(self):
        for bad_member in ["cookies.txt", "token.json", "state/core.sqlite3",
                           "pairing.json", "id_rsa", "creds.sqlite3",
                           "keys/server.pem"]:
            with tempfile.TemporaryDirectory() as tmp:
                tarball = Path(tmp) / "bad.tar.gz"
                make_tarball(tarball, "2026.11.15.1",
                             {bad_member: "secret"})
                with self.assertRaises(self_update.UpdateError,
                                      msg=bad_member):
                    self_update.verify_tarball(tarball, "2026.11.15.1")

    def test_member_outside_prefix_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            tarball = Path(tmp) / "bad.tar.gz"
            make_tarball(tarball, "2026.11.15.1", prefix="other-dir/")
            with self.assertRaises(self_update.UpdateError):
                self_update.verify_tarball(tarball, "2026.11.15.1")

    def test_missing_version_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            tarball = Path(tmp) / "bad.tar.gz"
            with tarfile.open(tarball, "w:gz") as archive:
                data = io.BytesIO(b"x")
                info = tarfile.TarInfo("muse-mobile-channels-2026.11.15.1/x")
                info.size = 1
                archive.addfile(info, data)
            with self.assertRaises(self_update.UpdateError):
                self_update.verify_tarball(tarball, "2026.11.15.1")


class TestLatestReleaseLookup(unittest.TestCase):
    def _fake_response(self, payload):
        response = mock.MagicMock()
        response.read.return_value = json.dumps(payload).encode("utf-8")
        response.__enter__.return_value = response
        return response

    def test_parses_tag_and_asset(self):
        payload = {
            "tag_name": "v2026.11.15.1",
            "assets": [{"name": "muse-mobile-channels-2026.11.15.1.tar.gz",
                        "browser_download_url": "https://example/x.tar.gz"}],
        }
        with mock.patch("urllib.request.urlopen",
                        return_value=self._fake_response(payload)):
            version, tag, url = self_update.get_latest_release("o/r")
        self.assertEqual((version, tag), ("2026.11.15.1", "v2026.11.15.1"))
        self.assertEqual(url, "https://example/x.tar.gz")

    def test_missing_asset_is_error(self):
        payload = {"tag_name": "v2026.11.15.1", "assets": []}
        with mock.patch("urllib.request.urlopen",
                        return_value=self._fake_response(payload)):
            with self.assertRaises(self_update.UpdateError):
                self_update.get_latest_release("o/r")

    def test_no_releases_yet_is_clear_error(self):
        import urllib.error
        error = urllib.error.HTTPError("url", 404, "Not Found", None, None)
        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaisesRegex(self_update.UpdateError,
                                        "no GitHub Release exists yet"):
                self_update.get_latest_release("o/r")


class TestUpdaterFlow(unittest.TestCase):
    def _run(self, install, *extra):
        return subprocess.run(
            [*UPDATER, "--install-dir", str(install), *extra],
            capture_output=True, text=True, timeout=120)

    def test_already_latest_is_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            tarball = Path(tmp) / "new.tar.gz"
            make_tarball(tarball, "2026.11.14.1")
            proc = self._run(install, "--fake-latest-version", "2026.11.14.1",
                             "--fake-tarball", str(tarball))
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("already on latest", proc.stdout)
            self.assertEqual((install / "app.py").read_text(),
                             "print('old')\n")

    def test_check_only_changes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            tarball = Path(tmp) / "new.tar.gz"
            make_tarball(tarball, "2026.11.15.1")
            proc = self._run(install, "--check-only",
                             "--fake-latest-version", "2026.11.15.1",
                             "--fake-tarball", str(tarball))
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("2026.11.14.1 -> 2026.11.15.1", proc.stdout)
            self.assertEqual((install / "VERSION").read_text(),
                             "2026.11.14.1\n")

    def test_dry_run_changes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            tarball = Path(tmp) / "new.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         {"app.py": "print('new')\n"})
            proc = self._run(install, "--dry-run",
                             "--fake-latest-version", "2026.11.15.1",
                             "--fake-tarball", str(tarball))
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("dry-run", proc.stdout)
            self.assertEqual((install / "VERSION").read_text(),
                             "2026.11.14.1\n")
            self.assertEqual((install / "app.py").read_text(),
                             "print('old')\n")

    def test_dirty_tree_skips_and_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            (install / "app.py").write_text("print('local edit')\n",
                                            encoding="utf-8")
            tarball = Path(tmp) / "new.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         {"app.py": "print('new')\n"})
            proc = self._run(install, "--dry-run",
                             "--fake-latest-version", "2026.11.15.1",
                             "--fake-tarball", str(tarball))
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("uncommitted", proc.stdout)
            self.assertEqual((install / "app.py").read_text(),
                             "print('local edit')\n")

    def test_apply_updates_code_preserves_state_and_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            tarball = Path(tmp) / "new.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         {"app.py": "print('new')\n"})
            proc = self._run(install, "--no-restart",
                             "--fake-latest-version", "2026.11.15.1",
                             "--fake-tarball", str(tarball))
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertEqual((install / "VERSION").read_text(),
                             "2026.11.15.1\n")
            self.assertEqual((install / "app.py").read_text(),
                             "print('new')\n")
            # State, untracked user files, and .git all survive.
            self.assertEqual((install / "state" / "core.sqlite3").read_text(),
                             "STATE-MUST-SURVIVE")
            self.assertEqual((install / "local-notes.txt").read_text(),
                             "untracked user file")
            self.assertTrue((install / ".git").is_dir())
            # A backup of the old version sits next to the install.
            backups = list(Path(tmp).glob("checkout.bak-2026.11.14.1-*"))
            self.assertEqual(len(backups), 1)
            self.assertEqual((backups[0] / "app.py").read_text(),
                             "print('old')\n")
            self.assertIn("2026.11.14.1 -> 2026.11.15.1", proc.stdout)
            self.assertTrue((install / "self-update.log").is_file())

    def test_apply_skips_socket_files_in_backup(self):
        # Regression: a stale Unix socket in the tree used to crash
        # backup_install (copytree raises on sockets) and abort the whole
        # update before anything was applied (live 2026-11-14: two stale
        # probe-ui sockets under ts-spaces/ killed the backup step).
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            sock_path = install / "scratch.sock"
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(sock_path))
                tarball = Path(tmp) / "new.tar.gz"
                make_tarball(tarball, "2026.11.15.1",
                             {"app.py": "print('new')\n"})
                proc = self._run(install, "--no-restart",
                                 "--fake-latest-version", "2026.11.15.1",
                                 "--fake-tarball", str(tarball))
            finally:
                sock.close()
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertEqual((install / "VERSION").read_text(),
                             "2026.11.15.1\n")
            backups = list(Path(tmp).glob("checkout.bak-2026.11.14.1-*"))
            self.assertEqual(len(backups), 1)
            self.assertFalse((backups[0] / "scratch.sock").exists())

    def test_apply_rejects_bad_tarball(self):
        with tempfile.TemporaryDirectory() as tmp:
            install = make_install_dir(tmp)
            tarball = Path(tmp) / "bad.tar.gz"
            make_tarball(tarball, "2026.11.15.1",
                         {"cookies.txt": "secret"})
            proc = self._run(install, "--no-restart",
                             "--fake-latest-version", "2026.11.15.1",
                             "--fake-tarball", str(tarball))
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual((install / "VERSION").read_text(),
                             "2026.11.14.1\n")
            self.assertEqual((install / "app.py").read_text(),
                             "print('old')\n")


class TestRestartSkip(unittest.TestCase):
    def test_unknown_services_are_skipped(self):
        log = io.StringIO()
        with mock.patch("sys.stdout", log):
            ok = self_update.restart_services(
                ["definitely-not-a-real-service-xyz"], None)
        self.assertTrue(ok)
        self.assertIn("not installed; skipping", log.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
