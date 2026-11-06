"""Explicit channel modes and process leases for safe single-writer cutovers."""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path


class ModeError(RuntimeError):
    """An incompatible writer or invalid channel mode prevents startup."""


class ModeDisabled(ModeError):
    """A correctly configured channel intentionally disables this legacy writer."""


def validate_settings(channel: str, settings: dict) -> None:
    """Reject malformed modes before publishing a configuration to any writer."""
    if channel not in ('weixin', 'wecom', 'feishu') or not isinstance(settings, dict):
        raise ModeError('invalid channel settings')
    if settings.get('mode') not in ('legacy', 'core'):
        raise ModeError('channel mode must be legacy or core')
    if settings['mode'] == 'core':
        if not isinstance(settings.get('database'), str) or not Path(settings['database']).is_absolute():
            raise ModeError('database must be an absolute path')
        if not isinstance(settings.get('account_id'), str) or not settings['account_id'].strip():
            raise ModeError('explicit account_id is required')
        administrators = settings.get('administrators', [])
        if not isinstance(administrators, list) or not all(isinstance(value, str) and value.strip() for value in administrators):
            raise ModeError('administrators must be an explicit list of identities')
        if not isinstance(settings.get('allowed_senders'), list) or not settings['allowed_senders'] or not all(
                isinstance(item, str) and item.strip() for item in settings['allowed_senders']):
            raise ModeError('allowed_senders must be a nonempty list of identities')


class ChannelLease:
    """Hold a shared configuration lock for the complete writer lifetime.

    Mode changes require the exclusive lock. All writers participating in a
    cutover must be upgraded first; old binaries cannot honor these leases.
    """

    def __init__(self, path: Path, channel: str, expected: str | None = None):
        """Read one validated mode under its shared lease, failing closed."""
        self.path = Path(path).resolve()
        self.channel = channel
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.with_suffix(self.path.suffix + '.lock').open('a+')
        try:
            fcntl.flock(self.handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
            self.document = json.loads(self.path.read_text())
            self.settings = self.document['channels'][channel]
            validate_settings(channel, self.settings)
            self.mode = self.settings['mode']
            if expected is not None and self.mode != expected:
                raise ModeDisabled(f'{channel} uses {self.mode}; {expected} writer is disabled')
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release the writer lease deterministically."""
        self.handle.close()

    def __enter__(self) -> ChannelLease:
        """Use leases around short-lived commands and hook invocations."""
        return self

    def __exit__(self, *args: object) -> None:
        """Release after command completion, including errors."""
        self.close()


def lease_from_env(channel: str, expected: str | None = None) -> ChannelLease | None:
    """Legacy deployments remain explicit opt-out until their coordinated upgrade."""
    value = os.environ.get('MUSE_CHANNEL_MODES')
    return ChannelLease(Path(value), channel, expected) if value else None


def write_modes(path: Path, document: dict) -> None:
    """Atomically change modes only while all participating writers are stopped.

    This function is a local configuration primitive. Operators must separately
    drain or reconcile in-flight work and verify migration reports first.
    """
    path = Path(path).resolve()
    if not isinstance(document, dict) or not isinstance(document.get('channels'), dict):
        raise ModeError('channels object is required')
    for channel, settings in document['channels'].items():
        validate_settings(channel, settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(path.suffix + '.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ModeError('writers are still running; stop all channel writers before changing mode') from error
        from .files import atomic_text
        atomic_text(path, json.dumps(document, ensure_ascii=False, indent=2), mode=0o600)


def run_legacy_hook(channel: str, script: str, arguments: list[str]) -> int:
    """Hold the lease in a wrapper process while the legacy hook executes."""
    try:
        lease = lease_from_env(channel, 'legacy')
        with lease if lease is not None else nullcontext():
            environment = dict(os.environ, MUSE_HOOK_MODE_GUARDED='1')
            return subprocess.run(['bash', script, *arguments], env=environment, check=False).returncode
    except ModeDisabled:
        # A core channel has no work for an old hook. No wake payload is emitted.
        return 0


if __name__ == '__main__':
    try:
        raise SystemExit(run_legacy_hook(sys.argv[1], sys.argv[2], sys.argv[3:]))
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print(f'channel mode guard failed: {type(error).__name__}', file=sys.stderr)
        raise SystemExit(1)
