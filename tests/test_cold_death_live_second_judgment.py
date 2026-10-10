#!/usr/bin/env python3
"""Gates and plant order for the WeCom second-judgment script.

The plant runs against a temp tree. Nothing here sets the live env and
then points the CLI at /home/hatch.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from io import StringIO
from contextlib import redirect_stdout
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import ops.cold_death_live_second_judgment as plant  # noqa: E402


SCRIPT = REPO / "ops" / "cold_death_live_second_judgment.py"
DRILL_TEXT = "【DRILL 冷判死】演练消息，不是用户任务。"


def run_cli(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the operator script. The child env is explicit."""
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def base_env() -> dict[str, str]:
    """A copy of the environment with the live gates removed."""
    env = dict(os.environ)
    for key in (
        "DEATH_WATCH_LIVE_PLANT",
        "DEATH_WATCH_LIVE_CONFIRM",
        "DEATH_WATCH_DRILL_LIVE",
    ):
        env.pop(key, None)
    return env


def live_env() -> dict[str, str]:
    """Both gates open. Callers still must not aim this at hatch."""
    env = base_env()
    env["DEATH_WATCH_LIVE_PLANT"] = "1"
    env["DEATH_WATCH_LIVE_CONFIRM"] = "wecom-second-judgment"
    return env


def scaffold(tmp: Path, chatid: str = "chat-auth", chattype: str = "single") -> tuple[Path, Path, Path]:
    """Minimal wecom hook + bot state with one real inbox row."""
    tmp.mkdir(parents=True, exist_ok=True)
    hook = tmp / "hook"
    bot = tmp / "bot"
    bridge = tmp / "native-bridge" / "state.json"
    hook.mkdir()
    bot.mkdir()
    bridge.parent.mkdir()
    row = {
        "msgid": "real-1",
        "from_userid": "user-a",
        "chattype": chattype,
        "chatid": chatid,
        "msgtype": "text",
        "text": "你好",
        "ts": 1_700_000_000,
        "media": [],
    }
    (bot / "inbox.jsonl").write_text(
        json.dumps(row, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (bot / "outbox.jsonl").write_text('{"mode":"send","content":"leave-me"}\n', encoding="utf-8")
    (hook / "pending.json").write_text("{}", encoding="utf-8")
    (hook / "resume_attempts.json").write_text(
        json.dumps({"keep-me": 1_799_999_000}),
        encoding="utf-8",
    )
    (hook / "seen_msgids.txt").write_bytes(b"old-msg")
    return hook, bot, bridge


def test_constants_and_source() -> None:
    """The sentence, the watch, and the service boundary stay fixed."""
    assert plant.PLANT_TEXT == DRILL_TEXT
    assert plant.PLANT_TEXT.startswith("【DRILL")
    assert plant.DEATH_WATCH_SECS == 1800
    source = SCRIPT.read_text(encoding="utf-8")
    assert source.count("DEATH_WATCH_SECS =") == 1
    assert "systemctl" not in source
    assert '["cp", "-a"' in source
    assert 'assert json.loads(line)["text"].startswith("【DRILL")' in source
    batch_write = source.index("atomic_write_text(batch_path")
    tail_assert = source.index('assert json.loads(line)["text"].startswith("【DRILL")')
    assert tail_assert < batch_write


def test_gate_default_off() -> None:
    """Missing env, a bad confirm word, and bypass flags all exit 2."""
    missing = run_cli([], base_env())
    assert missing.returncode == 2
    assert "Default is off" in missing.stderr

    one = base_env()
    one["DEATH_WATCH_LIVE_PLANT"] = "1"
    only_plant = run_cli([], one)
    assert only_plant.returncode == 2

    wrong = live_env()
    wrong["DEATH_WATCH_LIVE_CONFIRM"] = "sandbox-only"
    assert run_cli([], wrong).returncode == 2

    assert run_cli(["--live"], live_env()).returncode == 2
    assert run_cli(["--live=1"], live_env()).returncode == 2
    assert run_cli(["--production"], live_env()).returncode == 2
    assert run_cli(["--channel", "wecom"], live_env()).returncode == 2

    bypass = live_env()
    bypass["DEATH_WATCH_DRILL_LIVE"] = "1"
    drilled = run_cli([], bypass)
    assert drilled.returncode == 2
    assert "DEATH_WATCH_DRILL_LIVE" in drilled.stderr


def test_cli_does_not_accept_hatch_override(tmp: Path) -> None:
    """A path argument is not a way to retarget the plant."""
    refused = run_cli([str(tmp)], live_env())
    assert refused.returncode == 2
    assert not (tmp / "active_batch.json").exists()


def test_non_hatch_paths_refused(tmp: Path) -> None:
    """The default plant entry refuses a temp tree even when the gates are open."""
    hook, bot, bridge = scaffold(tmp)
    saved = {
        key: os.environ.get(key)
        for key in ("DEATH_WATCH_LIVE_PLANT", "DEATH_WATCH_LIVE_CONFIRM", "DEATH_WATCH_DRILL_LIVE")
    }
    os.environ["DEATH_WATCH_LIVE_PLANT"] = "1"
    os.environ["DEATH_WATCH_LIVE_CONFIRM"] = "wecom-second-judgment"
    os.environ.pop("DEATH_WATCH_DRILL_LIVE", None)
    try:
        try:
            plant.plant_wecom_second_judgment(hook, bot, bridge, tmp / "snaps")
        except plant.PlantAbort as exc:
            assert exc.code == 2
            assert "/home/hatch" in str(exc)
        else:
            raise AssertionError("non-hatch plant should have been refused")
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    assert not (hook / "active_batch.json").exists()
    assert json.loads((hook / "resume_attempts.json").read_text(encoding="utf-8")) == {
        "keep-me": 1_799_999_000
    }


def test_closed_gate_refuses_before_paths(tmp: Path) -> None:
    """Without the env gates, plant() exits before creating a snapshot."""
    hook, bot, bridge = scaffold(tmp)
    try:
        plant.plant_wecom_second_judgment(hook, bot, bridge, tmp / "snaps")
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("closed gate should exit 2")
    assert not (hook / "active_batch.json").exists()
    assert list((tmp / "snaps").glob("*")) == [] if (tmp / "snaps").exists() else True


def test_happy_path_order(tmp: Path) -> None:
    """Readonly, snapshot, merge, seen/carried, inbox, assert, batch last."""
    hook, bot, bridge = scaffold(tmp, chatid="room-9", chattype="group")
    outbox_before = (bot / "outbox.jsonl").read_text(encoding="utf-8")
    now = 1_800_000_000.0
    result = plant.plant_wecom_second_judgment(
        hook, bot, bridge, tmp / "snaps", allow_non_hatch=True, now=now,
    )
    assert result["steps"] == [
        "readonly",
        "snapshot",
        "resume_attempts",
        "seen",
        "carried",
        "inbox",
        "assert_drill",
        "active_batch",
    ]
    msgid = str(result["msgid"])
    assert msgid.startswith("drill-deathwatch-")
    assert result["chatid"] == "room-9"
    assert result["chattype"] == "group"
    snap = Path(str(result["snap"]))
    assert (snap / "hook-state").is_dir()
    assert (snap / "sizes.txt").is_file()
    assert (snap / "inbox-tail.txt").read_text(encoding="utf-8").startswith("{")

    attempts = json.loads((hook / "resume_attempts.json").read_text(encoding="utf-8"))
    assert attempts["keep-me"] == 1_799_999_000
    assert attempts[msgid] == now - 60

    seen = (hook / "seen_msgids.txt").read_text(encoding="utf-8")
    assert seen == f"old-msg\n{msgid}\n"
    carried = (hook / "carried_msgids.txt").read_text(encoding="utf-8")
    assert carried == f"{msgid}\n"

    last = plant.last_nonempty_line(bot / "inbox.jsonl")
    row = json.loads(last)
    assert row["text"] == DRILL_TEXT
    assert row["text"].startswith("【DRILL")
    assert row["msgid"] == msgid
    assert row["chatid"] == "room-9"
    assert row["chattype"] == "group"
    assert row["from_userid"] == "user-a"
    assert row["msgtype"] == "text"
    assert row["media"] == []
    assert row["ts"] == now - (1800 + 100)

    batch = json.loads((hook / "active_batch.json").read_text(encoding="utf-8"))
    assert batch == {"msgids": [msgid], "since": row["ts"], "detached": []}
    assert (bot / "outbox.jsonl").read_text(encoding="utf-8") == outbox_before
    assert result["outbox_offset"] == len(outbox_before.encode("utf-8"))
    assert json.loads((hook / "pending.json").read_text(encoding="utf-8")) == {}


def test_bad_tail_does_not_write_batch(tmp: Path) -> None:
    """A tail that fails the drill assert leaves active_batch.json unwritten."""
    hook, bot, bridge = scaffold(tmp)
    original = plant.append_bytes

    def bad_append(path: Path, payload: bytes) -> None:
        original(path, b'{"msgid":"drill-deathwatch-x","text":"not a drill"}\n')

    plant.append_bytes = bad_append
    try:
        try:
            plant.plant_wecom_second_judgment(
                hook, bot, bridge, tmp / "snaps", allow_non_hatch=True, now=1_800_000_000.0,
            )
        except plant.PlantAbort as exc:
            assert exc.code == 1
            assert "active_batch" in str(exc)
        else:
            raise AssertionError("bad tail should abort")
    finally:
        plant.append_bytes = original
    assert not (hook / "active_batch.json").exists()
    attempts = json.loads((hook / "resume_attempts.json").read_text(encoding="utf-8"))
    assert "keep-me" in attempts


def test_assert_helper_blocks_batch_write(tmp: Path) -> None:
    """write_active_batch_last itself refuses a non-drill tail."""
    tmp.mkdir(parents=True, exist_ok=True)
    inbox = tmp / "inbox.jsonl"
    batch = tmp / "active_batch.json"
    msgid = "drill-deathwatch-9"
    inbox.write_text(
        json.dumps({"msgid": msgid, "text": "【DRILL 别的句子"}) + "\n",
        encoding="utf-8",
    )
    try:
        plant.write_active_batch_last(inbox, batch, msgid, 10.0, inbox.stat().st_size)
    except plant.PlantAbort as exc:
        assert exc.code == 1
    else:
        raise AssertionError("prefix-only text should abort")
    assert not batch.exists()

    good = {"msgid": msgid, "text": DRILL_TEXT}
    inbox.write_text(json.dumps(good, ensure_ascii=False) + "\n", encoding="utf-8")
    plant.write_active_batch_last(inbox, batch, msgid, 10.0, inbox.stat().st_size)
    written = json.loads(batch.read_text(encoding="utf-8"))
    assert written["msgids"] == [msgid]
    assert written["detached"] == []


def test_occupied_batch_and_cover_stop_early(tmp: Path) -> None:
    """A busy queue or a covering reply does not merge resume_attempts."""
    hook, bot, bridge = scaffold(tmp)
    (hook / "active_batch.json").write_text(
        json.dumps({"msgids": ["real-1"], "since": 1, "detached": []}),
        encoding="utf-8",
    )
    try:
        plant.plant_wecom_second_judgment(
            hook, bot, bridge, tmp / "snaps", allow_non_hatch=True, now=1_800_000_000.0,
        )
    except plant.PlantAbort as exc:
        assert exc.code == 2
    else:
        raise AssertionError("occupied batch should abort")
    assert json.loads((hook / "resume_attempts.json").read_text(encoding="utf-8")) == {
        "keep-me": 1_799_999_000
    }
    assert not list((tmp / "snaps").glob("death-watch-drill-snap-*"))

    hook2, bot2, bridge2 = scaffold(tmp / "cover", chatid="chat-auth")
    now = 1_800_000_000.0
    since = now - 1900
    follow = {
        "msgid": "real-follow",
        "from_userid": "user-a",
        "chattype": "single",
        "chatid": "chat-auth",
        "msgtype": "text",
        "text": "后一句",
        "ts": since,
        "media": [],
    }
    with (bot2 / "inbox.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(follow, ensure_ascii=False) + "\n")
    (bot2 / "outbox.jsonl").write_text(
        json.dumps({"id": "q1", "mode": "reply", "msgid": "real-follow", "content": "ok"}) + "\n",
        encoding="utf-8",
    )
    (bot2 / "outbox_results.jsonl").write_text(
        json.dumps({"id": "q1", "ok": True, "mode": "reply", "ts": now}) + "\n",
        encoding="utf-8",
    )
    try:
        plant.plant_wecom_second_judgment(
            hook2, bot2, bridge2, tmp / "snaps2", allow_non_hatch=True, now=now,
        )
    except plant.PlantAbort as exc:
        assert "delivered formal reply" in str(exc)
    else:
        raise AssertionError("covering reply should abort")
    assert not (hook2 / "active_batch.json").exists()


def test_degraded_probe_aborts(tmp: Path) -> None:
    """An unreadable bridge is not treated as a pass."""
    hook, bot, bridge = scaffold(tmp)
    bridge.write_text("{", encoding="utf-8")
    try:
        plant.plant_wecom_second_judgment(
            hook, bot, bridge, tmp / "snaps", allow_non_hatch=True, now=1_800_000_000.0,
        )
    except plant.PlantAbort as exc:
        assert "degraded" in str(exc)
    else:
        raise AssertionError("degraded probe should abort")
    assert not (hook / "active_batch.json").exists()


def test_outbox_report(tmp: Path) -> None:
    """After the poll window the script prints a notice or operator instructions."""
    tmp.mkdir(parents=True, exist_ok=True)
    bot = tmp / "bot"
    bot.mkdir()
    msgid = "drill-deathwatch-77-1"
    old = '{"mode":"send","content":"older"}\n'
    notice = {
        "id": "n1",
        "mode": "send",
        "content": "⚠️ 任务已停止（续跑后仍没再收到它本人的推送）：「【DRILL 冷判死】演练消息，不是用户」",
    }
    (bot / "outbox.jsonl").write_text(
        old + json.dumps(notice, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (bot / "outbox_results.jsonl").write_text(
        json.dumps({"id": "n1", "ok": True, "mode": "send"}) + "\n",
        encoding="utf-8",
    )
    buf = StringIO()
    with redirect_stdout(buf):
        plant.report_outbox(bot, len(old.encode("utf-8")), msgid)
    printed = buf.getvalue()
    assert "任务已停止" in printed
    assert "DRILL" in printed
    assert '"ok": true' in printed

    empty = tmp / "empty"
    empty.mkdir()
    (empty / "outbox.jsonl").write_text(old, encoding="utf-8")
    buf = StringIO()
    with redirect_stdout(buf):
        plant.report_outbox(empty, len(old.encode("utf-8")), msgid)
    missed = buf.getvalue()
    assert "Do not plant again" in missed
    assert "DEATH_WATCH_SECS" in missed
    assert "Do not stop services" in missed


def main() -> None:
    """Run every case against its own temp directory."""
    import tempfile

    test_constants_and_source()
    test_gate_default_off()
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        test_cli_does_not_accept_hatch_override(root / "cli")
        test_non_hatch_paths_refused(root / "nonhatch")
        test_closed_gate_refuses_before_paths(root / "closed")
        test_happy_path_order(root / "happy")
        test_bad_tail_does_not_write_batch(root / "badtail")
        test_assert_helper_blocks_batch_write(root / "assert")
        test_occupied_batch_and_cover_stop_early(root / "busy")
        test_degraded_probe_aborts(root / "degraded")
        test_outbox_report(root / "outbox")
    print("cold-death live second-judgment checks passed")


if __name__ == "__main__":
    main()
