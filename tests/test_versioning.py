"""Tests for date-based versioning (VERSION file + bump script)."""

import datetime
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ops"))

import bump_version  # noqa: E402


class TestParseVersion(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(bump_version.parse_version("2026.11.14.1"),
                         (2026, 11, 14, 1))
        self.assertEqual(bump_version.parse_version("2026.11.14.12"),
                         (2026, 11, 14, 12))

    def test_invalid(self):
        for bad in ["0.1.0", "2026.11.14", "2026.11.14.0", "2026.13.01.1",
                    "2026.02.30.1", "v2026.11.14.1", "2026.11.14.1\nx", ""]:
            with self.assertRaises(ValueError, msg=bad):
                bump_version.parse_version(bad)

    def test_roundtrip(self):
        for text in ["2026.11.14.1", "2027.01.31.9"]:
            version = bump_version.parse_version(text)
            self.assertEqual(bump_version.format_version(version), text)


class TestComparison(unittest.TestCase):
    def test_ordering(self):
        parse = bump_version.parse_version
        self.assertLess(parse("2026.11.14.1"), parse("2026.11.14.2"))
        self.assertLess(parse("2026.11.14.2"), parse("2026.11.15.1"))
        self.assertLess(parse("2026.11.14.1"), parse("2027.01.01.1"))
        self.assertEqual(parse("2026.11.14.1"), parse("2026.11.14.1"))
        self.assertGreater(parse("2026.11.15.1"), parse("2026.11.14.9"))


class TestNextVersion(unittest.TestCase):
    def test_same_day_increments(self):
        today = datetime.date(2026, 11, 14)
        self.assertEqual(
            bump_version.next_version("2026.11.14.1", today),
            (2026, 11, 14, 2))
        self.assertEqual(
            bump_version.next_version("2026.11.14.9", today),
            (2026, 11, 14, 10))

    def test_new_day_resets(self):
        today = datetime.date(2026, 11, 15)
        self.assertEqual(
            bump_version.next_version("2026.11.14.3", today),
            (2026, 11, 15, 1))


class TestBumpScript(unittest.TestCase):
    def _stage(self, tmp):
        """Copy the bump script into tmp/ops/ so it edits tmp/VERSION.

        The script resolves VERSION from its own file location, so the
        copy — never the real ops/bump_version.py — must be executed.
        """
        import shutil
        ops_dir = Path(tmp) / "ops"
        ops_dir.mkdir()
        script = ops_dir / "bump_version.py"
        shutil.copy(ROOT / "ops" / "bump_version.py", script)
        return script

    def _run_bump(self, script, args):
        return subprocess.run(
            [sys.executable, str(script), *args],
            capture_output=True, text=True, timeout=30)

    def test_bump_writes_next_version_for_today(self):
        with tempfile.TemporaryDirectory() as tmp:
            version_file = Path(tmp) / "VERSION"
            version_file.write_text("2026.11.14.1\n", encoding="utf-8")
            script = self._stage(tmp)
            proc = self._run_bump(script, ["--date", "2026-11-14"])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(version_file.read_text(encoding="utf-8"),
                             "2026.11.14.2\n")
            self.assertIn("2026.11.14.2", proc.stdout)

    def test_bump_new_day_resets_sequence(self):
        with tempfile.TemporaryDirectory() as tmp:
            version_file = Path(tmp) / "VERSION"
            version_file.write_text("2026.11.14.3\n", encoding="utf-8")
            script = self._stage(tmp)
            proc = self._run_bump(script, ["--date", "2026-11-15"])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(version_file.read_text(encoding="utf-8"),
                             "2026.11.15.1\n")

    def test_check_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            version_file = Path(tmp) / "VERSION"
            version_file.write_text("2026.11.14.1\n", encoding="utf-8")
            script = self._stage(tmp)
            proc = self._run_bump(script, ["--date", "2026-11-14", "--check"])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(version_file.read_text(encoding="utf-8"),
                             "2026.11.14.1\n")
            self.assertIn("2026.11.14.2", proc.stdout)

    def test_set_validates(self):
        with tempfile.TemporaryDirectory() as tmp:
            version_file = Path(tmp) / "VERSION"
            version_file.write_text("2026.11.14.1\n", encoding="utf-8")
            script = self._stage(tmp)
            proc = self._run_bump(script, ["--set", "0.1.0"])
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(version_file.read_text(encoding="utf-8"),
                             "2026.11.14.1\n")

    def test_repo_version_file_is_valid(self):
        # The real repo VERSION must parse and match the date-based scheme.
        version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
        parsed = bump_version.parse_version(version)
        self.assertEqual(bump_version.format_version(parsed), version)


if __name__ == "__main__":
    unittest.main(verbosity=2)
