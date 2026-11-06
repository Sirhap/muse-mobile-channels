"""Unittest wrapper for the Gadget worker bun tests (items 1 + 4).

Runs tests/gadget_turn.test.ts (turn.ts helpers + send-failure branches
with a mocked reference client) through bun and fails when any bun test
fails. The bun file itself holds the real assertions.
"""
from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path


def _resolve_bun() -> str:
    found = shutil.which("bun")
    if found:
        return found
    for candidate in ("/opt/hatch-image/bin/bun", Path.home() / ".bun" / "bin" / "bun"):
        if Path(candidate).is_file():
            return str(candidate)
    return "bun"


class TestGadgetTurnTs(unittest.TestCase):
    def test_bun_turn_tests_pass(self):
        root = Path(__file__).resolve().parents[1]
        proc = subprocess.run(
            [_resolve_bun(), "test", str(root / "tests" / "gadget_turn.test.ts")],
            cwd=root, capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(proc.returncode, 0,
                         f"bun test failed:\n{proc.stdout}\n{proc.stderr}")


if __name__ == "__main__":
    unittest.main()
