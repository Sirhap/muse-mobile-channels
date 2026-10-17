#!/usr/bin/env python3
"""Sandbox checks for the fencing experiment's channel and switch gates.

The live test-channel directory is not planted. The one invocation
that sets the enable variable and passes ``--channel test`` stops on
the missing test state directory and must not create it. A separate
case plants a drill row under a temporary directory and restores it.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import ops.fencing_dual_deliver_experiment as exp  # noqa: E402
from channel_common import reply_block_reason_for_row  # noqa: E402


SCRIPT = REPO / "ops" / "fencing_dual_deliver_experiment.py"
ENV_NAME = exp.ENV_NAME
REAL_CHANNELS = ("weixin", "wecom", "production", "prod", "hatch", "live", "both")
BAD_SWITCHES = ("", "0", "true", "yes", "1 ", "on")
RESULTS: list[tuple[str, bool]] = []
BEFORE_SNAPS = set(Path("/tmp").glob("fencing-dual-deliver-snap-*"))
WATCHED_PATHS = (
    Path("/home/hatch"),
    REPO / "weixin-bot" / "state",
    REPO / "wecom-bot" / "state",
)
BEFORE_PATHS = {p: p.exists() for p in WATCHED_PATHS}


def check(name: str, cond: bool) -> None:
    """Record one assertion and print it."""
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def run_cli(args: list[str], switch: str | None = None, extra: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run the operator script. The child env is explicit."""
    env = dict(os.environ)
    env.pop(ENV_NAME, None)
    for key in exp.PATH_OVERRIDE_ENV:
        env.pop(key, None)
    if switch is not None:
        env[ENV_NAME] = switch
    if extra:
        env.update(extra)
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_real_channels_rejected_with_switch_on() -> None:
    """A real channel is refused even when the enable variable is 1."""
    for name in REAL_CHANNELS:
        proc = run_cli(["--channel", name], switch="1")
        check(
            f"real channel {name} rejected",
            proc.returncode == 2
            and name in proc.stderr
            and "一律拒绝" in proc.stderr
            and "渠道门拒绝" in proc.stderr
            and "开关门拒绝" not in proc.stderr,
        )


def test_real_channel_rejected_with_switch_off() -> None:
    """Weixin is refused before the switch is considered."""
    proc = run_cli(["--channel", "weixin"])
    check(
        "weixin rejected while switch is unset",
        proc.returncode == 2
        and "weixin" in proc.stderr
        and "渠道门拒绝" in proc.stderr
        and "开关门拒绝" not in proc.stderr,
    )


def test_channel_forms_rejected() -> None:
    """Only the two-token ``--channel test`` form opens the channel gate."""
    cases = (
        ("no args", []),
        ("unknown channel", ["--channel", "foo"]),
        ("wrong case", ["--channel", "Test"]),
        ("glued flag", ["--channel=test"]),
        ("glued weixin", ["--channel=weixin"]),
        ("bare weixin", ["weixin"]),
        ("extra token", ["--channel", "test", "extra"]),
        ("live flag", ["--live"]),
        ("force beside test", ["--channel", "test", "--force"]),
        ("hatch path", ["--channel", "test", "/home/hatch/workspace/weixin-bot/state"]),
    )
    for label, args in cases:
        proc = run_cli(args, switch="1")
        check(
            f"channel form refused: {label}",
            proc.returncode == 2
            and "渠道门拒绝" in proc.stderr
            and "开关门拒绝" not in proc.stderr,
        )


def test_switch_rejected_for_test_channel() -> None:
    """``--channel test`` still refuses until the variable is exactly 1."""
    missing = run_cli(["--channel", "test"])
    check(
        "unset switch refused",
        missing.returncode == 2
        and "开关门拒绝" in missing.stderr
        and "默认拒绝" in missing.stderr
        and ENV_NAME in missing.stderr,
    )
    for value in BAD_SWITCHES:
        proc = run_cli(["--channel", "test"], switch=value)
        check(
            f"switch value {value!r} refused",
            proc.returncode == 2
            and "开关门拒绝" in proc.stderr
            and "默认拒绝" in proc.stderr,
        )


def test_path_override_refused() -> None:
    """An env path cannot retarget the plant onto a real channel."""
    target = REPO / "weixin-bot" / "state"
    proc = run_cli(
        ["--channel", "test"],
        switch="1",
        extra={"FENCING_STATE_DIR": str(target)},
    )
    check(
        "FENCING_STATE_DIR refused",
        proc.returncode == 2
        and "FENCING_STATE_DIR" in proc.stderr
        and "渠道门拒绝" in proc.stderr
        and target.exists() == BEFORE_PATHS[target],
    )


def test_open_gate_returns_test_without_writing() -> None:
    """The pure gate check accepts test and does not create state."""
    os.environ.pop(ENV_NAME, None)
    channel = exp.evaluate_gates(["--channel", "test"], {ENV_NAME: "1"})
    check("open gate returns test", channel == "test")
    check("open gate does not create test state", not Path("/tmp/bridge-test").exists())


def test_run_experiment_switch_recheck() -> None:
    """A direct call refuses when the process env switch is off."""
    os.environ.pop(ENV_NAME, None)
    try:
        exp.run_experiment()
    except exp.ExperimentAbort as exc:
        check("direct run without switch uses code 2", exc.code == 2)
        check("direct run without switch says default refuse", "默认拒绝" in str(exc))
        return
    check("direct run without switch raises", False)


def test_open_gate_missing_dir_does_not_plant() -> None:
    """Both gates open, the test directory is absent: exit 3, no create."""
    proc = run_cli(["--channel", "test"], switch="1")
    check(
        "missing test dir exits 3",
        proc.returncode == 3 and "未创建目录" in proc.stderr and "未栽种" in proc.stderr,
    )
    check("missing test dir is not created", not Path("/tmp/bridge-test").exists())


def test_residual_and_delivered_reply() -> None:
    """The production fence blocks a delivered reply and skips reply_file."""
    check(
        "delivered reply_file blocks a later reply",
        exp.reply_file_is_uncovered() is False,
    )
    reason = reply_block_reason_for_row(
        "m1",
        {"id": "row-1", "mode": "reply", "msgid": "m1"},
        {"id": "row-1", "ok": True},
        now=10,
    )
    check(
        "delivered mode=reply still blocks",
        reason is not None and "delivered successfully" in reason,
    )


def test_temp_plant_rolls_back() -> None:
    """Plant and restore under a temp directory. The test channel is not used."""
    with tempfile.TemporaryDirectory() as raw:
        state = Path(raw) / "state"
        state.mkdir()
        original = '{"id":"keep","mode":"send","content":"leave"}\n'
        (state / "outbox.jsonl").write_text(original, encoding="utf-8")
        snap = Path(raw) / "snap"
        saved = exp.snapshot_files(state, snap)
        before = exp.line_count(state / "outbox.jsonl")
        msgid, planted_id = exp.plant_delivered_reply(state)
        reason = exp.block_reason(state, msgid) or ""
        check(
            "temp plant is blocked as delivered",
            "delivered successfully" in reason and planted_id in reason,
        )
        check("temp plant keeps a single msgid row", exp.count_msgid(state, msgid) == 1)
        check(
            "temp plant adds one outbox line",
            exp.line_count(state / "outbox.jsonl") == before + 1,
        )
        check("temp restore reports no error", exp.restore_files(state, saved) is None)
        check("temp drill id is gone", not exp.outbox_contains_id(state, planted_id))
        check(
            "temp original row is unchanged",
            (state / "outbox.jsonl").read_text(encoding="utf-8") == original,
        )
        (state / "outbox.jsonl").write_bytes(b'{"id":"partial"')
        try:
            exp.require_clean_jsonl(state / "outbox.jsonl")
        except exp.ExperimentAbort as exc:
            check("partial tail is refused", exc.code == 3)
        else:
            check("partial tail is refused", False)
        (state / "outbox.jsonl").write_text(
            '{"id":"drill-fencing-old","mode":"reply"}\n',
            encoding="utf-8",
        )
        try:
            exp.ensure_no_prior_drill(state)
        except exp.ExperimentAbort as exc:
            check("prior drill is refused", exc.code == 3 and "未追加" in str(exc))
        else:
            check("prior drill is refused", False)


def test_source_has_no_live_side_door() -> None:
    """The operator script does not grow a service or CLI side path."""
    source = SCRIPT.read_text(encoding="utf-8")
    check(
        "script source has no live side door",
        "systemctl" not in source
        and "subprocess" not in source
        and "weixin_cli" not in source
        and "wecom_cli" not in source
        and "import gateway" not in source,
    )


def test_workspace_untouched() -> None:
    """The suite must not leave a plant, a snapshot, or a channel state dir."""
    after = set(Path("/tmp").glob("fencing-dual-deliver-snap-*"))
    check("no new snapshot directory", after == BEFORE_SNAPS)
    check(
        "hatch home unchanged",
        Path("/home/hatch").exists() == BEFORE_PATHS[Path("/home/hatch")],
    )
    check(
        "weixin state dir unchanged",
        (REPO / "weixin-bot" / "state").exists()
        == BEFORE_PATHS[REPO / "weixin-bot" / "state"],
    )
    check(
        "wecom state dir unchanged",
        (REPO / "wecom-bot" / "state").exists()
        == BEFORE_PATHS[REPO / "wecom-bot" / "state"],
    )


def main() -> int:
    """Run every gate case and print the pass count."""
    os.environ.pop(ENV_NAME, None)
    cases = (
        test_real_channels_rejected_with_switch_on,
        test_real_channel_rejected_with_switch_off,
        test_channel_forms_rejected,
        test_switch_rejected_for_test_channel,
        test_path_override_refused,
        test_open_gate_returns_test_without_writing,
        test_run_experiment_switch_recheck,
        test_open_gate_missing_dir_does_not_plant,
        test_residual_and_delivered_reply,
        test_temp_plant_rolls_back,
        test_source_has_no_live_side_door,
        test_workspace_untouched,
    )
    for fn in cases:
        try:
            fn()
        except Exception as exc:
            check(f"{fn.__name__} raised", False)
            print(f"ERROR {fn.__name__}: {exc}")
    passed = sum(1 for _, ok in RESULTS if ok)
    total = len(RESULTS)
    print(f"fencing dual-deliver gate checks: {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
