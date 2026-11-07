#!/usr/bin/env bash
# channel-restore: idempotent self-heal for the WeCom + Weixin gateways.
#
# Covers the known VM-replacement failure mode: systemd units live in
# /etc/systemd/system and do NOT survive a VM replacement, while the
# workspace copies do. This script, for each gateway service:
#   1. reinstalls the unit from the workspace copy if it is missing or
#      differs, then daemon-reloads
#   2. reinstalls the service's drop-in overrides (core-mode env) the
#      same way; a changed drop-in restarts the service, because a
#      running process keeps the environment it started with and would
#      otherwise stay silently legacy
#   3. enables the service if needed and starts it if not active
#   4. verifies the service is active and reports the gateway state
# It also reinstalls its own service and timer. Those units are what
# invoke this script; a replacement VM otherwise loses the schedule.
# Safe to run any time: when everything is healthy it changes
# nothing. Triggers, in order (2026-10-11, method 1): PRIMARY is the
# wecom/weixin inbox hook poll (~5s, driven from outside systemd on
# the surviving 100G volume) — hook-heal.sh in this directory checks
# the unit files on every poll and calls this script when one is
# missing or differs, so a VM replacement that deletes the timer is
# healed within seconds-to-minutes. BACKUP is channel-restore.timer
# every 5 minutes. The read-only detector remains
# channel-restore/liveness-probe.sh (HEARTBEAT.md); the probe does
# not call this script.
set -uo pipefail

# systemd system units set HOME=/root. The install home is /home/hatch
# unless MUSE_HOME points somewhere else. Do not follow root's HOME.
resolve_install_home() {
  if [[ -n "${MUSE_HOME:-}" ]]; then
    printf '%s\n' "$MUSE_HOME"
    return
  fi
  if [[ -d /home/hatch/workspace ]]; then
    printf '%s\n' /home/hatch
    return
  fi
  if [[ "${HOME:-}" == "/root" || -z "${HOME:-}" ]]; then
    printf '%s\n' /home/hatch
    return
  fi
  printf '%s\n' "$HOME"
}

core_mode_requested() {
  # An absent file or an explicit legacy mode is a normal opt-out.
  local modes="$1/state/channel-modes.json"
  [[ -e "$modes" ]] || return 1
  python3 - "$modes" <<'PYEOF'
import json, sys
from pathlib import Path

path = Path(sys.argv[1])
sys.path.insert(0, str(path.parents[1]))
try:
    from mobile_channels.modes import ModeError, validate_settings
except ImportError:
    print('channel-restore: FAIL cannot validate core configuration', file=sys.stderr)
    sys.exit(2)

try:
    channels = json.loads(path.read_text())['channels']
    wecom = channels.get('wecom')
    if wecom is None:
        sys.exit(1)
    validate_settings('wecom', wecom)
    if wecom['mode'] != 'core':
        sys.exit(1)
    if not Path(wecom['database']).is_file():
        raise ModeError('core database does not exist')
except (AttributeError, KeyError, ModeError, OSError, TypeError, ValueError):
    print('channel-restore: FAIL invalid WeCom core configuration or missing database', file=sys.stderr)
    sys.exit(2)
PYEOF
}

gateway_selected() {
  # An existing service should still be repaired if its credentials are lost.
  [[ -f "$2/.config/$1/credentials.env" || -f "$3/$1.service" ]]
}

render_dropin() {
  # Preserve local overrides while adapting the shipped mode path to this home.
  local content modes="$2/state/channel-modes.json"
  content="$(cat "$1")" || return 1
  modes="${modes//\\/\\\\}"
  modes="${modes//\"/\\\"}"
  printf '%s\n' "${content//\/home\/hatch\/workspace\/state\/channel-modes.json/"$modes"}"
}

render_unit() {
  # Render shipped absolute paths; preserve locally edited command arguments.
  python3 - "$1" "$2" <<'PYEOF'
import json, shlex, sys
from pathlib import Path

source, workspace = map(Path, sys.argv[1:])
home = str(workspace.parent)
database = None
if source.name == 'muse-core-run.service':
    database = json.loads((workspace / 'state/channel-modes.json').read_text())['channels']['wecom']['database']

def quote(value):
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%') + '"'

for line in source.read_text().splitlines():
    key, separator, value = line.partition('=')
    if separator and ('/home/hatch' in value or (key == 'ExecStart' and database is not None)):
        if key == 'ExecStart':
            arguments = [argument.replace('/home/hatch', home) for argument in shlex.split(value)]
            if database is not None:
                arguments[arguments.index('--database') + 1] = database
            value = ' '.join(quote(argument) for argument in arguments)
        elif key == 'Environment':
            value = quote(value.strip('"').replace('/home/hatch', home))
        elif key in ('EnvironmentFile', 'WorkingDirectory'):
            # systemd rejects quotes here: a quoted WorkingDirectory is
            # not absolute (fatal, unit will not start) and a quoted
            # EnvironmentFile is silently ignored, so keep them bare.
            value = value.strip('"').replace('/home/hatch', home)
        else:
            value = value.replace('/home/hatch', home)
        line = key + '=' + value
    print(line)
PYEOF
}

install_unit() {
  local src="$1"
  local name content temporary
  name="$(basename "$src")"
  local dst="/etc/systemd/system/$name"
  if [[ ! -f "$src" ]]; then
    echo "channel-restore: FAIL unit copy missing: $src"
    return 1
  fi
  content="$(render_unit "$src" "$WS")" || return 1
  if [[ ! -f "$dst" ]] || ! printf '%s\n' "$content" | cmp -s - "$dst"; then
    touch "/etc/systemd/system/.muse-reload-pending" || return 1
    temporary="$(mktemp "/etc/systemd/system/.muse-unit.XXXXXX")" || return 1
    if ! printf '%s\n' "$content" > "$temporary" ||
        ! chmod 0644 "$temporary" || ! mv -f "$temporary" "$dst"; then
      rm -f "$temporary"
      return 1
    fi
    echo "channel-restore: installed $name"
  fi
  return 0
}

reload_units() {
  # Retain the reload obligation when daemon-reload fails after a copy.
  local marker="/etc/systemd/system/.muse-reload-pending"
  [[ -f "$marker" ]] || return 0
  systemctl daemon-reload && rm -f "$marker"
}

install_dropins() {
  # Save the restart obligation before replacing files so a failed restart
  # or interrupted restore is retried even when the files now match.
  local svc="$1" srcdir="$WS/channel-restore/$1.service.d"
  local dstdir="/etc/systemd/system/$svc.service.d"
  local src dst content temporary
  [[ "$CORE_CONFIGURED" == "1" && -d "$srcdir" ]] || return 0
  for src in "$srcdir"/*.conf; do
    [[ -f "$src" ]] || continue
    dst="$dstdir/$(basename "$src")"
    content="$(render_dropin "$src" "$WS")" || return 1
    if [[ ! -f "$dst" ]] || ! printf '%s\n' "$content" | cmp -s - "$dst"; then
      mkdir -p "$dstdir" && touch "$dstdir/.muse-restart-pending" || return 1
      temporary="$(mktemp "$dstdir/.muse-dropin.XXXXXX")" || return 1
      if ! printf '%s\n' "$content" > "$temporary" ||
          ! chmod 0644 "$temporary" || ! mv -f "$temporary" "$dst"; then
        rm -f "$temporary"
        return 1
      fi
      echo "$svc: drop-in (re)installed: $(basename "$src")"
    fi
  done
  return 0
}

apply_pending_dropins() {
  local svc="$1" marker="/etc/systemd/system/$1.service.d/.muse-restart-pending"
  [[ "$CORE_CONFIGURED" == "1" && -f "$marker" ]] || return 0
  if ! systemctl daemon-reload || ! systemctl restart "$svc"; then
    echo "$svc: FAIL could not apply drop-in; will retry"
    return 1
  fi
  sleep 3
  if ! systemctl is-active --quiet "$svc"; then
    echo "$svc: FAIL inactive after drop-in restart; will retry"
    return 1
  fi
  rm -f "$marker" || return 1
  echo "$svc: restarted (drop-in changed)"
}

ensure_hatch_tree() {
  local path="$1"
  local owner=""
  [[ -e "$path" ]] || return 0
  if ! id hatch >/dev/null 2>&1; then
    return 0
  fi
  owner="$(stat -c '%U' "$path" 2>/dev/null || stat -f '%Su' "$path" 2>/dev/null || true)"
  if [[ -n "$owner" && "$owner" != "hatch" ]]; then
    chown -R hatch:hatch "$path" && echo "channel-restore: chown hatch $path"
  fi
}

heal_one() {
  local svc="$1" proj="$2" cred="$3"
  local src="$WS/$proj/$svc.service"

  if [[ ! -x "$WS/$proj/.venv/bin/python" ]]; then
    echo "$svc: FAIL venv python missing at $WS/$proj/.venv/bin/python"
    return 1
  fi
  if [[ ! -f "$cred" ]]; then
    echo "$svc: WARN credentials file missing: $cred (gateway will fail to connect)"
  fi
  if [[ ! -f "$src" ]]; then
    echo "$svc: FAIL workspace unit copy missing: $src"
    return 1
  fi
  install_unit "$src" || return 1
  install_dropins "$svc" || return 1
  reload_units || return 1
  # Only the first time the tree is still root-owned. Repeating chown
  # on a large media directory every 5 minutes stalls the healer.
  ensure_hatch_tree "$WS/$proj/state"
  ensure_hatch_tree "$(dirname "$cred")"
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  apply_pending_dropins "$svc" || return 1
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
    sleep 3
  fi
  if systemctl is-active --quiet "$svc" 2>/dev/null; then
    local state="?"
    state="$(python3 - "$WS/$proj/state/status.json" <<'PYEOF' 2>/dev/null || echo '?'
import json, sys
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        print(json.load(f).get("state", "?"))
except Exception:
    print("?")
PYEOF
)"
    echo "$svc: active (gateway state: $state)"
    return 0
  fi
  echo "$svc: FAIL still not active; see: journalctl -u $svc -n 20"
  return 1
}

heal_bridge() {
  # native-bridge: fast lane for the channels (cold path stays backup).
  # Different shape from heal_one: venv lives at $HOME_DIR/muse-test-venv,
  # credentials are the native token/cookies under .config/native-bridge.
  local WS="$1" HOME_DIR="$2"
  local svc=native-bridge
  local src="$WS/native-bridge/native-bridge.service"
  if [[ ! -x "$HOME_DIR/muse-test-venv/bin/python" ]]; then
    echo "$svc: FAIL venv python missing at $HOME_DIR/muse-test-venv/bin/python"
    return 1
  fi
  if [[ ! -f "$HOME_DIR/.config/native-bridge/token.json" && ! -f "$HOME_DIR/.config/native-bridge/cookies.txt" ]]; then
    echo "$svc: WARN no native credential (token.json/cookies.txt) - bridge will fall back to cold path"
  fi
  if [[ ! -f "$src" ]]; then
    echo "$svc: FAIL workspace unit copy missing: $src"
    return 1
  fi
  install_unit "$src" || return 1
  reload_units || return 1
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
    sleep 3
  fi
  if systemctl is-active --quiet "$svc" 2>/dev/null; then
    # Unit-active is not the same as working: the bridge refreshes
    # status.json every step (~10s), so a stale heartbeat under a live
    # process means wedged workers. The gateways have a watchdog hook;
    # this timer is the bridge's only supervisor — restart it.
    local age
    age="$(python3 - "$WS/native-bridge/status.json" <<'PYEOF' 2>/dev/null || echo 999999
import json, sys, time
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        print(int(time.time()) - int(json.load(f).get("ts", 0)))
except Exception:
    print(999999)
PYEOF
)"
    if [[ "$age" =~ ^[0-9]+$ ]] && [[ "$age" -gt 180 ]]; then
      echo "$svc: active but status stale (${age}s) - restarting"
      systemctl restart "$svc" 2>/dev/null && echo "$svc: restarted"
      sleep 3
    else
      echo "$svc: active (status age ${age}s)"
    fi
    return 0
  fi
  echo "$svc: FAIL still not active; see: journalctl -u $svc -n 20"
  return 1
}

main() {
  local HOME_DIR
  HOME_DIR="$(resolve_install_home)"
  local WS="$HOME_DIR/workspace"
  local UNIT_DIR="/etc/systemd/system"
  local FAIL=0
  local CORE_CONFIGURED=0
  # The timer and inbox hooks can call restore concurrently.
  exec 8>"$UNIT_DIR/.muse-channel-restore.lock" || return 1
  flock -n -E 75 8 || { [[ "$?" == "75" ]] && return 0; return 1; }
  if core_mode_requested "$WS"; then
    CORE_CONFIGURED=1
  elif [[ "$?" != "1" ]]; then
    return 1
  fi

  install_unit "$WS/channel-restore/channel-restore.service" || FAIL=1
  install_unit "$WS/channel-restore/channel-restore.timer" || FAIL=1
  reload_units || return 1
  if ! systemctl is-enabled --quiet channel-restore.timer 2>/dev/null; then
    systemctl enable channel-restore.timer >/dev/null 2>&1 && echo "channel-restore: timer enabled"
  fi
  if ! systemctl is-active --quiet channel-restore.timer 2>/dev/null; then
    systemctl start channel-restore.timer >/dev/null 2>&1 && echo "channel-restore: timer started"
  fi

  if [[ "$CORE_CONFIGURED" == "1" ]]; then
    heal_core_run "$WS" "$HOME_DIR" || FAIL=1
  fi
  if [[ "$CORE_CONFIGURED" == "1" ]] || gateway_selected wecom-bot "$HOME_DIR" "$UNIT_DIR"; then
    heal_one wecom-bot wecom-bot "$HOME_DIR/.config/wecom-bot/credentials.env" || FAIL=1
  fi
  if gateway_selected weixin-bot "$HOME_DIR" "$UNIT_DIR"; then
    heal_one weixin-bot weixin-bot "$HOME_DIR/.config/weixin-bot/credentials.env" || FAIL=1
  fi
  heal_bridge "$WS" "$HOME_DIR" || FAIL=1
  heal_relay "$WS" "$HOME_DIR" || FAIL=1

  # Outbound delivery probe (2026-10-14): process-alive is not the
  # same as can-deliver — a channel whose sends keep failing (e.g.
  # iLink ret=-2) stays green here without it. Advisory only.
  python3 "$WS/channel-restore/outbound_probe.py" --ws "$WS" || true

  if [[ "$FAIL" == "0" ]]; then
    echo "channel-restore: all gateways healthy"
  else
    echo "channel-restore: FAILURES above"
  fi
  exit "$FAIL"
}


heal_relay() {
  # approval-relay: egress approvals -> channel notices + decisions.
  local WS="$1" HOME_DIR="$2"
  local svc=approval-relay
  local src="$WS/approval-relay/approval-relay.service"
  if [[ ! -f "$src" ]]; then
    echo "$svc: FAIL workspace unit copy missing: $src"
    return 1
  fi
  install_unit "$src" || return 1
  reload_units || return 1
  install_dropins "$svc" || return 1
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  apply_pending_dropins "$svc" || return 1
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
  fi
  systemctl is-active --quiet "$svc" 2>/dev/null && echo "$svc: active"
}

heal_core_run() {
  # muse-core-run: WeCom core-mode runner (DualTrackExecutor: Gadget
  # primary, Native fallback). Added 2026-11-06 after a VM replacement
  # (boot 14:37) wiped /etc/systemd/system and this unit was the only
  # one channel-restore did not cover, so the core trial sat dead
  # until the missing unit was noticed. Unit copy lives in this
  # directory (channel-restore) so it survives with the rest.
  local WS="$1" HOME_DIR="$2"
  local svc=muse-core-run
  local src="$WS/channel-restore/muse-core-run.service"
  if [[ ! -x "$HOME_DIR/muse-test-venv/bin/python" ]]; then
    echo "$svc: FAIL venv python missing at $HOME_DIR/muse-test-venv/bin/python"
    return 1
  fi
  if [[ ! -f "$src" ]]; then
    echo "$svc: FAIL workspace unit copy missing: $src"
    return 1
  fi
  install_unit "$src" || return 1
  reload_units || return 1
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
    sleep 3
  fi
  systemctl is-active --quiet "$svc" 2>/dev/null && echo "$svc: active"
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
