#!/usr/bin/env python3
"""Sandbox-only cold-death drill for the inbox hooks.

Default off. Exits before creating a directory unless both
``DEATH_WATCH_DRILL=1`` and ``DEATH_WATCH_DRILL_CONFIRM=sandbox-only``
are set. Copies ``hooks/scripts/*-inbox.sh`` into ``/tmp``, rewrites
hatch paths, and stubs the channel CLI. There is no live mode.

The operator runbook is ``docs/cold-death-drill-2026-10-09.md``.
Do not run this, and do not plant hatch state, until that runbook's
authorization table is filled in.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


CONFIRM = "sandbox-only"
DEATH_WATCH_SECS = 1800
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = REPO_ROOT / "hooks" / "scripts"

CHANNEL_SPEC = {
    "weixin": {
        "script": "weixin-inbox.sh",
        "bot_dir": "weixin-bot",
        "hatch_state": "/home/hatch/workspace/weixin-bot/state",
        "hatch_cli": "/home/hatch/workspace/weixin-bot/weixin",
    },
    "wecom": {
        "script": "wecom-inbox.sh",
        "bot_dir": "wecom-bot",
        "hatch_state": "/home/hatch/workspace/wecom-bot/state",
        "hatch_cli": "/home/hatch/workspace/wecom-bot/wecom",
    },
}


def refuse(message: str) -> None:
    """Exit 2 without touching a sandbox or a hatch path."""
    print(message, file=sys.stderr)
    print(
        "Refusing. This drill stays off until the boss authorizes it. "
        "See docs/cold-death-drill-2026-10-09.md. "
        "There is no live mode in this script.",
        file=sys.stderr,
    )
    raise SystemExit(2)


def enforce_gate(argv: list[str]) -> str:
    """Return weixin, wecom, or both. Exit 2 when the sandbox gate is shut.

    ``--live`` and ``DEATH_WATCH_DRILL_LIVE`` are refused on purpose so a
    future edit cannot grow a production path without changing this gate.
    """
    for arg in argv:
        if arg in {"--live", "--production", "--hatch"} or arg.startswith("--live"):
            refuse("This script has no live mode and will not target hatch.")
        if "/home/hatch" in arg:
            refuse("Refusing an argument that points at /home/hatch.")
    channel = "both"
    if argv:
        if len(argv) == 2 and argv[0] == "--channel" and argv[1] in CHANNEL_SPEC:
            channel = argv[1]
        elif len(argv) == 2 and argv[0] == "--channel" and argv[1] == "both":
            channel = "both"
        else:
            refuse(
                "Usage: DEATH_WATCH_DRILL=1 DEATH_WATCH_DRILL_CONFIRM=sandbox-only "
                "python3 ops/cold_death_drill.py [--channel weixin|wecom|both]"
            )
    if os.environ.get("DEATH_WATCH_DRILL_LIVE"):
        refuse("DEATH_WATCH_DRILL_LIVE is set. This script cannot target hatch.")
    if os.environ.get("DEATH_WATCH_DRILL") != "1":
        refuse(
            "DEATH_WATCH_DRILL is not 1. Default is off. "
            "Set DEATH_WATCH_DRILL=1 and DEATH_WATCH_DRILL_CONFIRM=sandbox-only."
        )
    if os.environ.get("DEATH_WATCH_DRILL_CONFIRM") != CONFIRM:
        refuse(
            "DEATH_WATCH_DRILL_CONFIRM must be exactly sandbox-only. "
            "Any other confirm word, including live or production, is refused."
        )
    return channel


def rewrite_hook(src: str, spec: dict[str, str], bot_state: Path, cli: Path) -> str:
    """Point one copied hook at the sandbox. Fail if a hatch path remains."""
    rewritten = src.replace(spec["hatch_state"], str(bot_state))
    rewritten = rewritten.replace(spec["hatch_cli"], str(cli))
    if "/home/hatch" in rewritten:
        raise RuntimeError("rewrite left a /home/hatch path in the hook copy")
    if "DEATH_WATCH_SECS = 1800" not in rewritten:
        raise RuntimeError(
            "refusing to run a copy whose DEATH_WATCH_SECS is not 1800"
        )
    return rewritten


def entry(channel: str, msgid: str, ts: float, text: str) -> dict[str, object]:
    """One synthetic inbox row. Addressing is fake and sandbox-local."""
    if channel == "weixin":
        return {
            "msgid": msgid,
            "from_user_id": "drill-user",
            "text": text,
            "ts": ts,
            "media": [],
        }
    if channel == "wecom":
        return {
            "msgid": msgid,
            "from_userid": "drill-user",
            "chattype": "single",
            "chatid": "drill-chat",
            "msgtype": "text",
            "text": text,
            "ts": ts,
            "media": [],
        }
    raise RuntimeError(f"unknown channel {channel}")


def write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


class Case:
    """One sandbox plant plus the hook run's observable output."""

    def __init__(self, sandbox: Path, channel: str) -> None:
        self.sandbox = sandbox
        self.channel = channel
        self.spec = CHANNEL_SPEC[channel]
        self.bot = sandbox / self.spec["bot_dir"] / "state"
        self.hookst = sandbox / "home" / "hooks" / "state" / self.spec["bot_dir"]
        self.stdout = ""
        self.stderr = ""
        self.returncode = 0

    def reset(self) -> None:
        if self.bot.exists():
            shutil.rmtree(self.bot)
        if self.hookst.exists():
            shutil.rmtree(self.hookst)
        self.bot.mkdir(parents=True)
        self.hookst.mkdir(parents=True)
        (self.sandbox / "cli.log").write_text("", encoding="utf-8")
        write_json(self.bot / "cancelled.json", [])
        write_json(self.hookst / "pending.json", {})

    def plant(
        self,
        msgid: str,
        since_ago: float,
        text: str,
        outbox: list[dict[str, object]] | None = None,
        results: list[dict[str, object]] | None = None,
        parked: dict[str, object] | None = None,
        extra_entries: list[dict[str, object]] | None = None,
        resume_attempt: bool = False,
        heartbeat: bool = False,
    ) -> None:
        """Seed a silent-looking batch. Msgids are marked seen and carried."""
        now = time.time()
        since = now - since_ago
        rows = [entry(self.channel, msgid, since, text)]
        ids = [msgid]
        for extra in extra_entries or []:
            rows.append(extra)
            ids.append(str(extra["msgid"]))
        write_jsonl(self.bot / "inbox.jsonl", rows)
        write_jsonl(self.bot / "outbox.jsonl", outbox or [])
        if results is not None:
            write_jsonl(self.bot / "outbox_results.jsonl", results)
        if parked is not None:
            write_json(self.bot / "outbox_parked.json", parked)
        (self.hookst / "seen_msgids.txt").write_text(
            "".join(mid + "\n" for mid in ids), encoding="utf-8")
        (self.hookst / "carried_msgids.txt").write_text(
            "".join(mid + "\n" for mid in ids), encoding="utf-8")
        write_json(self.hookst / "active_batch.json", {
            "msgids": [msgid],
            "since": since,
            "detached": [],
        })
        if resume_attempt:
            write_json(self.hookst / "resume_attempts.json", {msgid: now - 60})
        if heartbeat:
            hb = self.bot / "heartbeats" / msgid
            hb.parent.mkdir(parents=True, exist_ok=True)
            hb.write_text("tick", encoding="utf-8")
            os.utime(hb, (now, now))

    def run(self) -> None:
        env = dict(os.environ)
        env["HOME"] = str(self.sandbox / "home")
        env["HATCH_HOOK_RUNTIME"] = str(self.sandbox / "runtime.sh")
        env["HATCH_HOOK_DRY_RUN"] = "0"
        proc = subprocess.run(
            ["bash", str(self.sandbox / "hook.sh")],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.stdout = proc.stdout
        self.stderr = proc.stderr
        self.returncode = proc.returncode

    def calls(self) -> str:
        return (self.sandbox / "cli.log").read_text(encoding="utf-8")

    def batch(self) -> dict[str, object]:
        path = self.hookst / "active_batch.json"
        if not path.exists():
            return {}
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}

    def covered(self) -> str:
        path = self.hookst / "covered_drops.jsonl"
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")


def prepare_sandbox(sandbox: Path, channel: str) -> None:
    """Install the rewritten hook, a no-op runtime, and a logging CLI stub."""
    spec = CHANNEL_SPEC[channel]
    src = (SCRIPTS / spec["script"]).read_text(encoding="utf-8")
    bot_state = sandbox / spec["bot_dir"] / "state"
    cli = sandbox / "stubcli"
    (sandbox / "hook.sh").write_text(
        rewrite_hook(src, spec, bot_state, cli), encoding="utf-8")
    (sandbox / "runtime.sh").write_text(
        'silent() { echo "DECISION silent: $1"; }\n'
        'wake() { echo "DECISION wake: $1"; }\n'
        'log() { :; }\n',
        encoding="utf-8",
    )
    (sandbox / "stubcli").write_text(
        "#!/usr/bin/env bash\n"
        f'echo "CALL: $*" >> {sandbox / "cli.log"}\n'
        'if [[ "$1" == "cancelled" ]]; then echo "[]"; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    os.chmod(sandbox / "stubcli", 0o755)
    bot_state.mkdir(parents=True, exist_ok=True)


def check(results: list[tuple[str, bool]], name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)
    if not cond and detail:
        print(detail[:800])


def assert_quiet(results: list[tuple[str, bool]], case: Case, name: str) -> None:
    """No cancel, no resume notice, no stop notice, hook stayed up."""
    log = case.calls()
    check(results, f"{name} hook exit 0", case.returncode == 0, case.stderr)
    check(results, f"{name} no cancel", "CALL: cancel" not in log, log)
    check(results, f"{name} no resume notice", "自动续跑一次" not in log, log)
    check(results, f"{name} no stop notice", "任务已停止" not in log, log)
    check(results, f"{name} no legacy failure wording", "任务执行失败" not in log, log)
    check(results, f"{name} no stall notice", "⏳ 提醒" not in log, log)


def run_channel(sandbox: Path, channel: str, results: list[tuple[str, bool]]) -> None:
    """Seven plants per channel. See the runbook scenario table."""
    prepare_sandbox(sandbox, channel)
    case = Case(sandbox, channel)
    now = time.time()
    prefix = f"{channel}"

    case.reset()
    case.plant("drill-deathwatch-young", 30, "【DRILL 冷判死】young")
    case.run()
    assert_quiet(results, case, f"{prefix} hold-young-batch")
    check(
        results,
        f"{prefix} hold-young-batch still active",
        case.batch().get("msgids") == ["drill-deathwatch-young"],
        json.dumps(case.batch(), ensure_ascii=False),
    )

    case.reset()
    case.plant(
        "drill-deathwatch-push",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】push",
        outbox=[{
            "id": "UP1",
            "mode": "update",
            "msgid": "drill-deathwatch-push",
            "queued_at": now - 60,
            "content": "still pushing",
        }],
    )
    case.run()
    assert_quiet(results, case, f"{prefix} hold-recent-push")
    check(
        results,
        f"{prefix} hold-recent-push still active",
        case.batch().get("msgids") == ["drill-deathwatch-push"],
        json.dumps(case.batch(), ensure_ascii=False),
    )

    case.reset()
    case.plant(
        "drill-deathwatch-parked",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】parked",
        outbox=[{
            "id": "PK1",
            "mode": "reply",
            "msgid": "drill-deathwatch-parked",
            "queued_at": now - (DEATH_WATCH_SECS + 90),
        }],
        results=[{
            "id": "PK1",
            "mode": "reply",
            "ok": False,
            "errmsg": "prepare failed",
            "ts": now - 100,
        }],
        parked={
            "PK1": {
                "item": {
                    "id": "PK1",
                    "mode": "reply",
                    "msgid": "drill-deathwatch-parked",
                },
                "n": 1,
                "next": now + 100,
            }
        },
    )
    case.run()
    assert_quiet(results, case, f"{prefix} hold-parked-reply")
    check(
        results,
        f"{prefix} hold-parked-reply still active",
        case.batch().get("msgids") == ["drill-deathwatch-parked"],
        json.dumps(case.batch(), ensure_ascii=False),
    )

    follow = entry(channel, "drill-deathwatch-follow", now - 100, "后续一句")
    case.reset()
    case.plant(
        "drill-deathwatch-covered",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】covered",
        outbox=[{
            "id": "COV1",
            "mode": "reply",
            "msgid": "drill-deathwatch-follow",
            "queued_at": now - 90,
        }],
        results=[{
            "id": "COV1",
            "mode": "reply",
            "ok": True,
            "ts": now - 80,
        }],
        extra_entries=[follow],
    )
    case.run()
    assert_quiet(results, case, f"{prefix} drop-covered-followup")
    check(
        results,
        f"{prefix} drop-covered-followup recorded",
        "drill-deathwatch-covered" in case.covered(),
        case.covered(),
    )
    check(
        results,
        f"{prefix} drop-covered-followup batch cleared",
        not case.batch().get("msgids"),
        json.dumps(case.batch(), ensure_ascii=False),
    )

    case.reset()
    case.plant(
        "drill-deathwatch-heartbeat",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】heartbeat",
        heartbeat=True,
    )
    case.run()
    log = case.calls()
    check(results, f"{prefix} heartbeat-ignored hook exit 0", case.returncode == 0, case.stderr)
    check(results, f"{prefix} heartbeat-ignored resumes", "自动续跑一次" in log, log)
    check(results, f"{prefix} heartbeat-ignored no cancel", "CALL: cancel" not in log, log)
    check(results, f"{prefix} heartbeat-ignored no stop", "任务已停止" not in log, log)
    check(
        results,
        f"{prefix} heartbeat-ignored rearmed",
        case.batch().get("msgids") == ["drill-deathwatch-heartbeat"],
        json.dumps(case.batch(), ensure_ascii=False),
    )
    check(
        results,
        f"{prefix} heartbeat-ignored woke",
        "DECISION wake" in case.stdout,
        case.stdout,
    )

    case.reset()
    case.plant(
        "drill-deathwatch-first",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】first",
    )
    case.run()
    log = case.calls()
    check(results, f"{prefix} first-loss hook exit 0", case.returncode == 0, case.stderr)
    check(results, f"{prefix} first-loss resume notice", "自动续跑一次" in log and "DRILL" in log, log)
    check(results, f"{prefix} first-loss no cancel", "CALL: cancel" not in log, log)
    check(results, f"{prefix} first-loss no stop notice", "任务已停止" not in log, log)
    check(results, f"{prefix} first-loss no legacy wording", "任务执行失败" not in log, log)
    check(
        results,
        f"{prefix} first-loss rearmed",
        case.batch().get("msgids") == ["drill-deathwatch-first"],
        json.dumps(case.batch(), ensure_ascii=False),
    )
    attempts = json.loads((case.hookst / "resume_attempts.json").read_text(encoding="utf-8"))
    check(
        results,
        f"{prefix} first-loss recorded attempt",
        "drill-deathwatch-first" in attempts,
        json.dumps(attempts, ensure_ascii=False),
    )

    case.reset()
    case.plant(
        "drill-deathwatch-second",
        DEATH_WATCH_SECS + 100,
        "【DRILL 冷判死】second",
        resume_attempt=True,
    )
    case.run()
    log = case.calls()
    check(results, f"{prefix} second-loss hook exit 0", case.returncode == 0, case.stderr)
    check(
        results,
        f"{prefix} second-loss cancel",
        "CALL: cancel --msgid drill-deathwatch-second" in log,
        log,
    )
    check(
        results,
        f"{prefix} second-loss stop notice",
        "任务已停止" in log and "DRILL" in log,
        log,
    )
    check(results, f"{prefix} second-loss no resume notice", "自动续跑一次" not in log, log)
    check(results, f"{prefix} second-loss no legacy wording", "任务执行失败" not in log, log)
    check(results, f"{prefix} second-loss no wake", "DECISION silent" in case.stdout, case.stdout)
    check(
        results,
        f"{prefix} second-loss batch cleared",
        not case.batch().get("msgids"),
        json.dumps(case.batch(), ensure_ascii=False),
    )
    notices_path = case.hookst / "failed_notices.json"
    notices = json.loads(notices_path.read_text(encoding="utf-8")) if notices_path.exists() else []
    check(
        results,
        f"{prefix} second-loss notice not left pending",
        notices == [],
        json.dumps(notices, ensure_ascii=False),
    )


def channels_of(selector: str) -> list[str]:
    if selector == "both":
        return ["weixin", "wecom"]
    if selector in CHANNEL_SPEC:
        return [selector]
    raise RuntimeError(f"unknown channel selector {selector}")


def main(argv: list[str]) -> int:
    selector = enforce_gate(argv)
    print("SANDBOX ONLY. No live hatch. No production config changes.")
    print("Do not treat this run as the production drill in the runbook.")
    root = Path(tempfile.mkdtemp(prefix="death-watch-drill-"))
    resolved = root.resolve()
    if not str(resolved).startswith("/tmp/"):
        shutil.rmtree(root, ignore_errors=True)
        refuse(f"sandbox root {resolved} is not under /tmp")
    results: list[tuple[str, bool]] = []
    keep = os.environ.get("DEATH_WATCH_DRILL_KEEP") == "1"
    try:
        for channel in channels_of(selector):
            sandbox = root / channel
            sandbox.mkdir()
            run_channel(sandbox, channel, results)
    except Exception as exc:
        print(f"FAIL drill raised {type(exc).__name__}: {exc}", file=sys.stderr)
        keep = True
        results.append(("drill exception", False))
    failed = [name for name, ok in results if not ok]
    print(f"\n== {len(results) - len(failed)}/{len(results)} passed ==")
    if failed or keep:
        print(f"sandbox left at {root}")
    else:
        shutil.rmtree(root, ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except SystemExit:
        raise
    except Exception as exc:
        print(f"cold-death drill failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
