"""Gadget side-session transport: one worker process per turn, real Noise protocol.

The HTTP protocol this module used to speak (gadget.muse.local plus
/api/sessions) does not exist upstream -- proven 2026-11-06 against the
official SDK, the wong2 client and the live pairing flow. The real path is
the Noise session in the reference client (reference/muse-client, a faithful
port of the official SDK): sendMessage creates the side chat, then a live
subscription follows the bound reply. A turn is therefore driven by a
short-lived worker process (mobile_channels/gadget_worker/turn.ts) that owns
one Noise connection and records its outcome in a result file; this module
spawns, reads and kills that worker. It never handles credential values --
only the pairing directory path, which the worker reads itself.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

# Files the reference client reads from the pairing directory, installed at
# 0600 by the pairing handover (see ~/.config/MuseGadgetPair).
PAIRING_FILES = ("identity.json", "pairing.json", "sdk_token")

# States the worker writes into its result file.
RESULT_STATES = ("running", "succeeded", "failed", "cancelled", "unknown")


class GadgetError(Exception):
    """Gadget transport generic error."""


class GadgetAuthError(GadgetError):
    """Gadget pairing credentials missing or unusable."""


class GadgetNetworkError(GadgetError):
    """Gadget worker could not start, or the upstream service failed."""


def _resolve_runner() -> str:
    """Find bun without assuming it is on PATH (systemd units have a thin PATH)."""
    found = shutil.which("bun")
    if found:
        return found
    for candidate in ("/opt/hatch-image/bin/bun", Path.home() / ".bun" / "bin" / "bun"):
        if Path(candidate).is_file():
            return str(candidate)
    return "bun"


@dataclass
class GadgetCredentials:
    """Pairing credentials: a directory, never the values themselves.

    The directory holds the three files the reference client reads. Nothing
    in this module reads or logs their contents.
    """

    directory: Path | str | None = None

    def __post_init__(self) -> None:
        if self.directory is None:
            self.directory = Path.home() / ".config" / "MuseGadgetPair"
        self.directory = Path(self.directory).expanduser()

    def is_configured(self) -> bool:
        """True only when the pairing directory holds all three files."""
        try:
            directory = Path(self.directory)
            return directory.is_dir() and all((directory / name).is_file() for name in PAIRING_FILES)
        except Exception:
            return False

    @classmethod
    def default(cls) -> "GadgetCredentials":
        """The installed pairing directory."""
        return cls()


class GadgetClient:
    """Spawns one worker process per Gadget turn and reads its result file.

    The worker is the only component that speaks the Noise protocol. This
    client owns the process lifecycle and the per-attempt files:
    ``gadget-<attempt_id>.json`` (result), ``.msg`` (message text) and
    ``.log`` (worker stderr). Result files are written atomically by the
    worker (temp file plus rename), so a reader never sees a partial file.
    """

    def __init__(
        self,
        credentials: GadgetCredentials | None = None,
        *,
        run_dir: Path | str | None = None,
        worker_script: Path | str | None = None,
        runner: str | None = None,
        turn_timeout_seconds: int = 900,
    ):
        self.credentials = credentials if credentials is not None else GadgetCredentials.default()
        self.run_dir = Path(run_dir).expanduser() if run_dir is not None else Path.home() / ".config" / "muse-gadget-runs"
        if worker_script is not None:
            self.worker_script = Path(worker_script).expanduser()
        else:
            self.worker_script = Path(__file__).resolve().parent / "gadget_worker" / "turn.ts"
        self.runner = runner or _resolve_runner()
        self.turn_timeout_seconds = turn_timeout_seconds
        self._closed = False

    def is_configured(self) -> bool:
        """Pairing credentials are installed and complete."""
        return self.credentials.is_configured()

    def _paths(self, attempt_id: str) -> tuple[Path, Path, Path]:
        """(result, message, log) paths for one attempt; the result path is derivable later."""
        base = self.run_dir / f"gadget-{attempt_id}"
        return base.with_suffix(".json"), base.with_suffix(".msg"), base.with_suffix(".log")

    def result_path(self, attempt_id: str) -> Path:
        """Where the worker records this attempt's outcome."""
        return self._paths(attempt_id)[0]

    def start_turn(
        self,
        attempt_id: str,
        session_id: str,
        text: str,
        timeout_seconds: int | None = None,
    ) -> subprocess.Popen:
        """Spawn the worker for one turn; returns the child process.

        Raises GadgetAuthError when the pairing is missing and
        GadgetNetworkError when the worker cannot start. A spawned worker
        owns the send: if this returns, the turn is live and the caller must
        poll read_result (or stop_turn) instead of resending.
        """
        if self._closed:
            raise GadgetError("GadgetClient is closed")
        if not self.is_configured():
            raise GadgetAuthError("Gadget pairing credentials are not installed")
        result_path, message_path, log_path = self._paths(attempt_id)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.run_dir, 0o700)
        except OSError:
            pass
        message_path.write_text(text, encoding="utf-8")
        os.chmod(message_path, 0o600)
        log_file = open(log_path, "ab")
        try:
            proc = subprocess.Popen(
                [self.runner, str(self.worker_script),
                 "--session", session_id,
                 "--message-file", str(message_path),
                 "--out", str(result_path),
                 "--timeout", str(int(timeout_seconds or self.turn_timeout_seconds)),
                 "--credentials", str(self.credentials.directory)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=log_file,
                start_new_session=True,
                close_fds=True,
            )
        except OSError as exc:
            raise GadgetNetworkError(f"cannot start Gadget worker: {exc}") from exc
        finally:
            log_file.close()
        return proc

    def read_result(self, attempt_id: str) -> dict | None:
        """The worker's latest recorded outcome, or None when it has written nothing yet."""
        try:
            raw = self.result_path(attempt_id).read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None
        try:
            data = json.loads(raw)
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def worker_alive(pid: int) -> bool:
        """True when the process exists and is not a zombie.

        A zombie still answers os.kill(pid, 0), so the /proc state is
        checked: 'Z' means the worker already exited and only awaits
        reaping by its parent.
        """
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        try:
            with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as handle:
                stat = handle.read()
            state = stat.rsplit(")", 1)[1].split()[0]
            return state != "Z"
        except (FileNotFoundError, IndexError, ValueError, OSError):
            return True

    def stop_turn(self, proc: subprocess.Popen | int, *, wait_seconds: float = 5.0) -> bool:
        """SIGTERM the worker's process group, then SIGKILL. True when it is gone.

        The worker writes a cancelled result on SIGTERM when it can; a
        SIGKILL leaves no file, which the executor reads as a cancelled turn
        through its own binding, not as a failure.
        """
        pid = proc.pid if isinstance(proc, subprocess.Popen) else int(proc)
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                return True
        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            if not self.worker_alive(pid):
                return True
            time.sleep(0.1)
        try:
            os.killpg(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        return not self.worker_alive(pid)

    def close(self) -> None:
        """Mark closed; live workers are left running so their results survive a restart."""
        self._closed = True

    def __enter__(self) -> "GadgetClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
