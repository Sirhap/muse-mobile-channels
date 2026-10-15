#!/usr/bin/env python3
"""Read-only pending-answer report for the missed-answer sweep.

Prints one JSON object grouped by channel. Each item has msgid,
source (covered-drop, fail-stop, or bridge-fallback), ts, and a
question summary. The command does not write the ledger, the dismiss
file, or any channel state.

Default age gate is 12 hours. A msgid already delivered as an ok=true
formal reply (reply or reply_file) is omitted. Pass ``--min-age 0``
to list every still-unanswered row.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from channel_common import (  # noqa: E402
    PENDING_ANSWER_EXPOSE_SECS,
    PendingLedgerError,
    muse_home,
    pending_answers_dismissed_path,
    pending_answers_path,
    summarize_pending_answers,
)


def _split_pair(text: str) -> tuple[str, Path]:
    """channel=/path, or a usage error."""
    channel, sep, raw = text.partition("=")
    channel = channel.strip()
    raw = raw.strip()
    if not sep or not channel or not raw:
        raise ValueError(f"expected channel=/path, got {text!r}")
    return channel, Path(raw)


def _default_dirs(ledger: Path) -> tuple[dict[str, Path], dict[str, Path]]:
    """Production bot-state and hook-state directories for both channels."""
    home = muse_home()
    hook_root = ledger.parent
    states: dict[str, Path] = {}
    hooks: dict[str, Path] = {}
    for channel, bot in (("weixin", "weixin-bot"), ("wecom", "wecom-bot")):
        states[channel] = home / "workspace" / bot / "state"
        hooks[channel] = hook_root / bot
    return states, hooks


def main(argv: list[str] | None = None) -> int:
    """Print the report. 0 when it was read, 2 when the request is bad."""
    parser = argparse.ArgumentParser(description=(
        "List unanswered msgids from the shared pending-answers ledger. "
        "Read-only."
    ))
    parser.add_argument(
        "--ledger",
        help="pending_answers.json (default: $HOME/hooks/state/pending_answers.json)",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=[],
        help="channel=/bot/state (outbox.jsonl, outbox_results.jsonl, inbox.jsonl). "
             "Repeat per channel. Replaces the weixin/wecom defaults.",
    )
    parser.add_argument(
        "--hook",
        action="append",
        default=[],
        help="channel=/hook/state, for legacy covered_drops.jsonl. "
             "Repeat per channel. Replaces the defaults.",
    )
    parser.add_argument(
        "--min-age",
        type=float,
        default=PENDING_ANSWER_EXPOSE_SECS,
        help="seconds since the row was first recorded (default 12h)",
    )
    parser.add_argument("--now", type=float, default=None, help="epoch override")
    parser.add_argument(
        "--dismissed",
        help="write-off file (default: pending_answers_dismissed.json beside the ledger)",
    )
    args = parser.parse_args(argv)
    try:
        ledger = Path(args.ledger) if args.ledger else pending_answers_path()
        if args.state:
            states = dict(_split_pair(item) for item in args.state)
        else:
            states, _hooks = _default_dirs(ledger)
        if args.hook:
            hooks = dict(_split_pair(item) for item in args.hook)
        elif args.state:
            hooks = {}
        else:
            _states, hooks = _default_dirs(ledger)
        dismissed = (
            Path(args.dismissed) if args.dismissed
            else pending_answers_dismissed_path(ledger)
        )
        report = summarize_pending_answers(
            ledger,
            state_dirs=states,
            hook_dirs=hooks,
            now=args.now,
            min_age_secs=args.min_age,
            dismissed_path=dismissed,
        )
    except PendingLedgerError as exc:
        print(f"pending-answers ledger unreadable: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"pending-answers report failed: {exc}", file=sys.stderr)
        return 2
    json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
