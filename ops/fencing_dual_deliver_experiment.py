#!/usr/bin/env python3
"""Test-channel live plant for the mode=reply delivered fence.

Default off. Exits 2 unless both of these are true:

- ``FENCING_DUAL_DELIVER_EXPERIMENT=1``
- the only arguments are ``--channel test``

Weixin, WeCom, and any other channel argument are refused even when
the variable is set. The script never imports a gateway module and
never calls a channel CLI.

Operator runbook:
``docs/fencing-dual-deliver-live-experiment-2026-10-15.md``.

Do not run the command in that runbook until its authorization table
is filled in. A missing test state directory aborts before any write.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from collections.abc import Mapping
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from channel_common import (  # noqa: E402
    REPLY_INFLIGHT_WINDOW_S,
    append_jsonl,
    load_json_dict,
    reply_block_reason_for_row,
)

ENV_NAME = "FENCING_DUAL_DELIVER_EXPERIMENT"
ALLOWED_CHANNEL = "test"
EXPECTED_TEST_STATE = Path("/tmp/bridge-test/state")
DOC_REL = "docs/fencing-dual-deliver-live-experiment-2026-10-15.md"
DRILL_PREFIX = "drill-fencing-"
DRILL_TEXT = "【DRILL fencing】双投实验，不是用户任务。"
SNAP_PREFIX = "fencing-dual-deliver-snap-"
STATE_FILES = ("outbox.jsonl", "outbox_results.jsonl", "outbox_retry.json")
PATH_OVERRIDE_ENV = ("FENCING_STATE_DIR", "FENCING_DUAL_DELIVER_STATE")
REAL_CHANNELS = frozenset({
    "weixin",
    "wecom",
    "production",
    "prod",
    "hatch",
    "live",
    "both",
})
BYPASS_FLAGS = frozenset({
    "--live",
    "--production",
    "--hatch",
    "--force",
    "--yes",
    "--confirm",
})
FORBIDDEN_MARKERS = ("weixin-bot", "wecom-bot")


class GateRefusal(Exception):
    """A channel or switch check failed. Nothing has been planted."""

    def __init__(self, message: str, kind: str) -> None:
        super().__init__(message)
        if kind not in ("channel", "switch"):
            raise ValueError(f"unknown gate kind: {kind}")
        self.kind = kind


class ExperimentAbort(Exception):
    """Stop the plant. ``code`` 3 means nothing was written; 1 means roll back."""

    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def format_refusal(exc: GateRefusal) -> str:
    """Prefix a refusal with the gate that fired."""
    kind = exc.kind
    if kind == "channel":
        return f"渠道门拒绝：{exc}"
    if kind == "switch":
        return f"开关门拒绝：{exc}"
    raise RuntimeError(f"unknown gate kind: {kind}")


def refuse(exc: GateRefusal) -> None:
    """Exit 2 before any state directory is touched."""
    print(format_refusal(exc), file=sys.stderr)
    print(
        f"拒绝运行。默认关闭。方案：{DOC_REL}",
        file=sys.stderr,
    )
    raise SystemExit(2)


def _real_channel_token(argv: list[str]) -> str | None:
    """Return the first real-channel or bypass token, if any."""
    for arg in argv:
        if arg in BYPASS_FLAGS or arg.startswith("--live"):
            return arg
        bare = arg.split("=", 1)[1] if arg.startswith("--channel=") else arg
        if bare in REAL_CHANNELS:
            return bare
        if "/home/hatch" in arg:
            return arg
        for marker in FORBIDDEN_MARKERS:
            if marker in arg:
                return arg
    return None


def evaluate_gates(argv: list[str], env: Mapping[str, str]) -> str:
    """Return ``test`` when both gates are open.

    Channel is decided first, so a real channel is refused while the
    enable variable is still unset. Path-override variables are refused
    as a channel retarget.
    """
    for key in PATH_OVERRIDE_ENV:
        if env.get(key):
            raise GateRefusal(
                f"环境变量 {key} 不能改写栽种目录，一律拒绝。",
                "channel",
            )
    hit = _real_channel_token(argv)
    if hit is not None:
        raise GateRefusal(
            f"真实渠道或旁路参数 {hit} 一律拒绝。本实验只接受 --channel test。",
            "channel",
        )
    if argv != ["--channel", ALLOWED_CHANNEL]:
        raise GateRefusal(
            "渠道参数必须恰好是 --channel test。其他渠道一律拒绝。",
            "channel",
        )
    if env.get(ENV_NAME) != "1":
        raise GateRefusal(
            f"{ENV_NAME} 未设为 1。默认拒绝运行。",
            "switch",
        )
    return ALLOWED_CHANNEL


def forbid_production_path(path: Path) -> None:
    """Reject a resolved path that is not the pinned test state directory."""
    text = str(path)
    if text == "/home/hatch" or text.startswith("/home/hatch/"):
        raise ExperimentAbort("解析路径落在 /home/hatch，拒绝栽种。", code=3)
    for marker in FORBIDDEN_MARKERS:
        if marker in text:
            raise ExperimentAbort(
                f"解析路径含 {marker}，拒绝栽种。",
                code=3,
            )
    if path != EXPECTED_TEST_STATE:
        raise ExperimentAbort(
            f"test 状态目录解析为 {text}，不是 {EXPECTED_TEST_STATE}。拒绝栽种。",
            code=3,
        )


def resolve_test_state() -> Path:
    """Load the pinned test bot_state. Missing directories are not created."""
    cfg_path = _REPO_ROOT / "native-bridge" / "config.json"
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError) as exc:
        raise ExperimentAbort(f"无法读取 test 渠道配置：{exc}", code=3) from exc
    try:
        raw = cfg["channels"]["test"]["bot_state"]
    except (KeyError, TypeError) as exc:
        raise ExperimentAbort(
            f"配置里没有 channels.test.bot_state：{exc}",
            code=3,
        ) from exc
    if raw != str(EXPECTED_TEST_STATE):
        raise ExperimentAbort(
            f"channels.test.bot_state 是 {raw!r}，不是 {EXPECTED_TEST_STATE}。拒绝栽种。",
            code=3,
        )
    path = Path(raw)
    forbid_production_path(path.resolve())
    if not path.is_dir():
        raise ExperimentAbort(
            f"test 渠道状态目录不存在：{path}。未创建目录，未栽种。",
            code=3,
        )
    forbid_production_path(path.resolve())
    return path


def reply_file_is_uncovered(msgid: str = "drill-fencing-residual") -> bool:
    """True when a delivered reply_file row does not block a later reply.

    This is the known narrow residual of the queue-time fence. The check
    is in memory and does not read or write a state directory.
    """
    reason = reply_block_reason_for_row(
        msgid,
        {
            "id": "file-row",
            "mode": "reply_file",
            "msgid": msgid,
            "queued_at": 0,
        },
        {"id": "file-row", "ok": True, "ts": 0},
        now=10_000,
    )
    return reason is None


def _read_text(path: Path) -> str:
    """Read a state file, or empty when it is absent."""
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def line_count(path: Path) -> int:
    """Count JSONL lines. A missing file has zero lines."""
    text = _read_text(path)
    if not text:
        return 0
    return len(text.splitlines())


def require_clean_jsonl(path: Path) -> None:
    """Refuse to append when the last line would stick to a partial tail."""
    if not path.exists():
        return
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ExperimentAbort(f"无法检查 {path}：{exc}", code=3) from exc
    if data and not data.endswith(b"\n"):
        raise ExperimentAbort(
            f"{path} 末尾不是完整行。拒绝栽种，避免粘连。",
            code=3,
        )


def ensure_no_prior_drill(state: Path) -> None:
    """Abort when a previous plant is still in the test outbox."""
    for name in ("outbox.jsonl", "outbox_results.jsonl"):
        path = state / name
        require_clean_jsonl(path)
        try:
            if path.exists() and DRILL_PREFIX.encode("utf-8") in path.read_bytes():
                raise ExperimentAbort(
                    f"{path} 已有 {DRILL_PREFIX} 残留。先按方案清理。本次未追加新行。",
                    code=3,
                )
        except OSError as exc:
            raise ExperimentAbort(f"无法检查残留：{exc}", code=3) from exc


def snapshot_files(state: Path, snap: Path) -> dict[str, bytes | None]:
    """Copy the three outbox files. Missing files are recorded as absent."""
    snap.mkdir(parents=True, exist_ok=False)
    saved: dict[str, bytes | None] = {}
    for name in STATE_FILES:
        src = state / name
        try:
            if src.exists():
                data = src.read_bytes()
                (snap / name).write_bytes(data)
                saved[name] = data
            else:
                saved[name] = None
        except OSError as exc:
            raise ExperimentAbort(f"快照 {src} 失败：{exc}", code=3) from exc
    manifest = {
        "existed": {name: saved[name] is not None for name in STATE_FILES},
        "planted_id": None,
        "msgid": None,
    }
    (snap / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return saved


def restore_files(state: Path, saved: dict[str, bytes | None]) -> str | None:
    """Put the snapshotted bytes back. Return an error string, or None."""
    problems: list[str] = []
    for name, data in saved.items():
        dest = state / name
        try:
            if data is None:
                if dest.exists():
                    dest.unlink()
            else:
                tmp = dest.with_name(dest.name + ".fencing-restore")
                tmp.write_bytes(data)
                os.replace(tmp, dest)
        except OSError as exc:
            problems.append(f"{name}: {exc}")
    if problems:
        return "; ".join(problems)
    return None


def plant_delivered_reply(state: Path) -> tuple[str, str]:
    """Append one delivered mode=reply row. Do not append a second reply."""
    msgid = f"{DRILL_PREFIX}{int(time.time())}"
    row_id = f"{DRILL_PREFIX}{uuid.uuid4().hex[:12]}"
    now = int(time.time())
    append_jsonl(
        state / "outbox.jsonl",
        {
            "id": row_id,
            "mode": "reply",
            "msgid": msgid,
            "content": DRILL_TEXT,
            "queued_at": now,
            "drill": True,
        },
    )
    append_jsonl(
        state / "outbox_results.jsonl",
        {
            "id": row_id,
            "mode": "reply",
            "msgid": msgid,
            "ok": True,
            "ts": now,
            "drill": True,
        },
    )
    return msgid, row_id


def block_reason(state: Path, msgid: str) -> str | None:
    """Scan ``state`` the way the channel CLIs scan their own outbox.

    A read error aborts. It is not treated as an open gate.
    """
    results: dict[str, dict] = {}
    try:
        results_path = state / "outbox_results.jsonl"
        if results_path.exists():
            for line in results_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict) and row.get("id"):
                    results[str(row["id"])] = row
        retry = load_json_dict(state / "outbox_retry.json")
        outbox = state / "outbox.jsonl"
        if not outbox.exists():
            return None
        now = time.time()
        for line in outbox.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            reason = reply_block_reason_for_row(
                msgid,
                row,
                results.get(str(row.get("id") or "")),
                now,
                retry,
                REPLY_INFLIGHT_WINDOW_S,
            )
            if reason:
                return reason
    except OSError as exc:
        raise ExperimentAbort(
            f"读取 test 状态失败，按未放行处理并回滚：{exc}",
            code=1,
        ) from exc
    return None


def count_msgid(state: Path, msgid: str) -> int:
    """How many outbox rows carry this msgid."""
    path = state / "outbox.jsonl"
    total = 0
    for line in _read_text(path).splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and str(row.get("msgid") or "") == msgid:
            total += 1
    return total


def outbox_contains_id(state: Path, row_id: str) -> bool:
    """True when either planted file still contains this id."""
    for name in ("outbox.jsonl", "outbox_results.jsonl"):
        if row_id and row_id in _read_text(state / name):
            return True
    return False


def _write_manifest(snap: Path, saved: dict[str, bytes | None], msgid: str, planted_id: str) -> None:
    """Record which files existed and which drill id was planted."""
    manifest = {
        "existed": {name: saved[name] is not None for name in STATE_FILES},
        "planted_id": planted_id,
        "msgid": msgid,
    }
    (snap / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run_experiment() -> dict:
    """Plant one delivered reply on the test channel and observe the fence.

    The second reply is never queued. Callers must already have passed
    ``evaluate_gates``; this function checks the enable variable again
    so a direct call cannot plant while the switch is off.
    """
    if os.environ.get(ENV_NAME) != "1":
        raise ExperimentAbort(
            f"{ENV_NAME} 未设为 1。默认拒绝运行。",
            code=2,
        )
    state = resolve_test_state()
    ensure_no_prior_drill(state)
    snap = Path("/tmp") / f"{SNAP_PREFIX}{time.strftime('%Y%m%d%H%M%S')}-{os.getpid()}"
    saved = snapshot_files(state, snap)
    msgid = ""
    planted_id = ""
    reason = ""
    residual_open = False
    passed = False
    observed_rows = 0
    error: ExperimentAbort | None = None
    try:
        before = line_count(state / "outbox.jsonl")
        msgid, planted_id = plant_delivered_reply(state)
        _write_manifest(snap, saved, msgid, planted_id)
        after = line_count(state / "outbox.jsonl")
        if after != before + 1:
            raise ExperimentAbort("栽种后 outbox 行数不是加一，停止。", code=1)
        found = block_reason(state, msgid)
        reason = found or ""
        observed_rows = count_msgid(state, msgid)
        if observed_rows != 1:
            raise ExperimentAbort("观察期间出现第二次投递行。", code=1)
        residual_open = reply_file_is_uncovered(msgid)
        passed = "delivered successfully" in reason and planted_id in reason
    except ExperimentAbort as exc:
        error = exc
    except Exception as exc:
        error = ExperimentAbort(f"实验中断：{exc}", code=1)
    finally:
        restore_error = restore_files(state, saved)
    if restore_error:
        raise ExperimentAbort(f"回滚失败：{restore_error}", code=1)
    if planted_id and outbox_contains_id(state, planted_id):
        raise ExperimentAbort("回滚后栽种行仍在。", code=1)
    report = {
        "channel": ALLOWED_CHANNEL,
        "msgid": msgid,
        "planted_id": planted_id,
        "second_reply_queued": observed_rows > 1,
        "block_reason": reason,
        "residual_reply_file_uncovered": residual_open,
        "restored": True,
        "pass": passed and error is None,
        "snapshot": str(snap),
    }
    try:
        (snap / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        raise ExperimentAbort(f"回滚已完成，但报告没写上：{exc}", code=1) from exc
    if error is not None:
        raise error
    if not passed:
        raise ExperimentAbort(
            f"第二次 mode=reply 没有被已送达抑制挡住。原因={reason!r}。栽种已回滚。",
            code=1,
        )
    return report


def main(argv: list[str] | None = None) -> int:
    """Open the gates, then run the plant. Gate failures exit 2."""
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        evaluate_gates(args, os.environ)
    except GateRefusal as exc:
        refuse(exc)
    try:
        report = run_experiment()
    except ExperimentAbort as exc:
        print(str(exc), file=sys.stderr)
        return exc.code
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(1)
