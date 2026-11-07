"""Prepare service units for review before applying them to a Linux host."""
from __future__ import annotations

from pathlib import Path
import shlex
import subprocess


def _systemd_quote(value: str) -> str:
    """Escape a literal systemd value, including paths containing spaces."""
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%') + '"'


def write_service_plan(source: Path, home: Path, destination: Path, *,
                       channels: list[str]) -> dict:
    """Create a new review directory containing selected units and an apply script."""
    if not channels or any(channel not in ('wecom', 'weixin') for channel in channels):
        raise ValueError('select wecom and/or weixin')
    source, home, destination = (Path(path).expanduser().resolve()
                                 for path in (source, home, destination))
    workspace = home / 'workspace'
    templates = [f'{channel}-bot/{channel}-bot.service' for channel in sorted(set(channels))]
    templates += ['native-bridge/native-bridge.service', 'approval-relay/approval-relay.service',
                  'channel-restore/channel-restore.service', 'channel-restore/channel-restore.timer']
    contents = {}
    for relative in templates:
        template = source / relative
        try:
            result = subprocess.run(
                ['bash', '-c', 'source "$1"; render_unit "$2" "$3"', 'service-plan',
                 str(source / 'channel-restore/restore.sh'), str(template), str(workspace)],
                check=True, capture_output=True, text=True, timeout=15)
        except (subprocess.SubprocessError, OSError) as error:
            raise ValueError(f'cannot render service template {template.name}') from error
        text = result.stdout
        if template.name == 'native-bridge.service':
            overrides = (
                'Environment=' + _systemd_quote(f'NATIVE_BRIDGE_CONFIG={home}/.config/native-bridge/config.json') + '\n'
                'Environment=' + _systemd_quote(f'NATIVE_BRIDGE_HOME={workspace}/native-bridge') + '\n'
            )
            text = text.replace('[Service]\n', '[Service]\n' + overrides, 1)
        contents[template.name] = text
    selected = [f'{channel}-bot.service' for channel in sorted(set(channels))]
    selected += ['native-bridge.service', 'approval-relay.service', 'channel-restore.timer']
    filenames = ' '.join(shlex.quote(name) for name in contents)
    enabled = ' '.join(shlex.quote(name) for name in selected)
    contents['install.sh'] = (
        '#!/usr/bin/env bash\nset -euo pipefail\n'
        'cd "$(dirname "${BASH_SOURCE[0]}")"\n'
        '[[ "$(uname -s)" == Linux ]] || { echo "Linux with systemd is required"; exit 1; }\n'
        f'sudo install -m 0644 {filenames} /etc/systemd/system/\n'
        'sudo systemctl daemon-reload\n'
        f'sudo systemctl enable --now {enabled}\n'
    )
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name, content in contents.items():
        path = destination / name
        with path.open('x', encoding='utf-8') as output:
            output.write(content)
        path.chmod(0o700 if name.endswith('.sh') else 0o600)
    return {'directory': str(destination), 'files': sorted(contents),
            'apply_command': f'bash {shlex.quote(str(destination / "install.sh"))}',
            'service_changes_applied': False}
