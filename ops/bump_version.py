#!/usr/bin/env python3
"""Bump the project version (date-based, YYYY.MM.DD.N).

The VERSION file at the repo root is the single source of truth for the
project version. Run this BEFORE committing a release: it writes the new
version into VERSION; the release is fixed by committing VERSION and
pushing tag ``v<version>``.

Rules (user-set 2026-11-14):
- The version is fixed at commit time, never by a timer. The updater only
  compares versions; it never invents one.
- Format is date-based: YYYY.MM.DD.N, N = sequence within that day.
- Same day as the current version -> N+1; a new day -> N=1.

Usage:
    python3 ops/bump_version.py            # bump to next version for today
    python3 ops/bump_version.py --date 2026-11-15
    python3 ops/bump_version.py --set 2026.11.14.2
    python3 ops/bump_version.py --check    # print next version, do not write
"""

from __future__ import annotations

import argparse
import datetime
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "VERSION"

VERSION_RE = re.compile(r"^(\d{4})\.(\d{2})\.(\d{2})\.(\d+)$")


def parse_version(text: str) -> tuple[int, int, int, int]:
    """Parse 'YYYY.MM.DD.N' into a comparable tuple; raise ValueError."""
    match = VERSION_RE.match(text.strip())
    if not match:
        raise ValueError(f"bad version {text!r}: expected YYYY.MM.DD.N")
    year, month, day, seq = (int(part) for part in match.groups())
    datetime.date(year, month, day)  # validates the calendar date
    if seq < 1:
        raise ValueError(f"bad version {text!r}: sequence must be >= 1")
    return year, month, day, seq


def format_version(version: tuple[int, int, int, int]) -> str:
    year, month, day, seq = version
    return f"{year:04d}.{month:02d}.{day:02d}.{seq}"


def next_version(current: str, today: datetime.date) -> tuple[int, int, int, int]:
    """Next version for ``today`` given the current version string."""
    year, month, day, seq = parse_version(current)
    if (year, month, day) == (today.year, today.month, today.day):
        return year, month, day, seq + 1
    return today.year, today.month, today.day, 1


def read_version() -> str:
    return VERSION_FILE.read_text(encoding="utf-8").strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=None,
                        help="pretend today is YYYY-MM-DD (default: real today)")
    parser.add_argument("--set", default=None,
                        help="write this exact version instead of bumping")
    parser.add_argument("--check", action="store_true",
                        help="print the next version without writing")
    args = parser.parse_args(argv)

    if args.date is not None:
        try:
            today = datetime.date.fromisoformat(args.date)
        except ValueError:
            print(f"bump_version: bad --date {args.date!r}", file=sys.stderr)
            return 2
    else:
        today = datetime.date.today()

    try:
        current = read_version()
        if args.set is not None:
            new = parse_version(args.set)
        else:
            new = next_version(current, today)
    except (OSError, ValueError) as exc:
        print(f"bump_version: {exc}", file=sys.stderr)
        return 1

    new_text = format_version(new)
    if args.check:
        print(f"current: {current} -> next: {new_text}")
        return 0
    VERSION_FILE.write_text(new_text + "\n", encoding="utf-8")
    print(f"bumped VERSION: {current} -> {new_text}")
    print(f"then: git add VERSION && git commit -m \"Release {new_text}\" "
          f"&& git tag v{new_text} && git push origin main v{new_text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
