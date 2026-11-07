"""Read-only checks for local dependencies, credential files and Core setup."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys

from .modes import validate_settings
from .readiness import ensure_executable_database


def inspect_installation(workspace: Path, home: Path, modes: Path | None = None) -> dict:
    """Report actionable checks without importing transports or reading credentials."""
    workspace, home = Path(workspace).expanduser().resolve(), Path(home).expanduser().resolve()
    checks = []

    def add(code: str, ok: bool, action: str, *, optional: bool = False) -> None:
        checks.append({'code': code, 'status': 'ok' if ok else 'optional' if optional else 'failed',
                       'action': '' if ok else action})

    add('python', sys.version_info >= (3, 12), 'Use Python 3.12 or newer.')
    add('workspace', (workspace / 'native-bridge/native_bridge.py').is_file(),
        'Clone the channel repository into the deployment workspace.')
    for name in ('httpx', 'websockets', 'cryptography', 'curl_cffi', 'noise', 'google.protobuf'):
        try:
            available = importlib.util.find_spec(name) is not None
        except (ImportError, ModuleNotFoundError, ValueError):
            available = False
        add('dependency_' + name, available, 'Install the channel dev dependencies and Muse SDK in this Python environment.')
    native_source = Path(os.environ.get('MUSE_CLI_SRC', str(workspace / 'reference/muse-cli/src'))).expanduser()
    add('native_sdk', (native_source / 'muse_cli/gateway.py').is_file(),
        'Install the pinned Muse CLI source or configure MUSE_CLI_SRC.')
    native = home / '.config/native-bridge'
    credential_files = [native / name for name in ('cookies.txt', 'vm_id.txt', 'token.json')]
    add('native_credentials', all(path.is_file() for path in credential_files[:2]) or credential_files[2].is_file(),
        'Import current browser credentials using the credential recovery guide.')
    existing = [path for path in credential_files if path.is_file()]
    try:
        permissions = (not existing or (native.stat().st_mode & 0o077) == 0) and all(
            (path.stat().st_mode & 0o077) == 0 for path in existing)
    except OSError:
        permissions = False
    add('native_permissions', permissions, 'Set credential directory to 0700 and files to 0600.')
    gadget = home / '.config/MuseGadgetPair'
    pairing = [gadget / name for name in ('pairing.json', 'identity.json', 'sdk_token')]
    gadget_configured = all(path.is_file() for path in pairing)
    add('gadget_credentials', gadget_configured, 'Pair once on macOS and install the three device files.', optional=True)
    bun = shutil.which('bun') or str(home / '.bun/bin/bun')
    add('gadget_runtime', Path(bun).is_file(), 'Install Bun 1.4.4 for Gadget.', optional=not gadget_configured)
    add('gadget_sdk', (workspace / 'reference/muse-client/src/client.ts').is_file(),
        'Install the pinned Gadget SDK described in extensions/gadget-token/README.md.', optional=not gadget_configured)
    target = Path(modes).expanduser().resolve() if modes else workspace / 'state/channel-modes.json'
    if target.is_file():
        try:
            document = json.loads(target.read_text())
            channels = document['channels']
            if not isinstance(channels, dict) or not channels:
                raise ValueError('empty modes')
            for channel, settings in channels.items():
                validate_settings(channel, settings)
                if settings['mode'] == 'core':
                    ensure_executable_database(Path(settings['database']))
            add('channel_modes', True, '')
        except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error):
            add('channel_modes', False, 'Validate the modes file and initialize or restore its exact Core database.')
    else:
        add('channel_modes', False, 'Legacy mode is active by default. Configure modes explicitly to enable Core.', optional=True)
    return {'ready': not any(check['status'] == 'failed' for check in checks),
            'network_checked': False, 'credentials_authenticated': False, 'checks': checks}
