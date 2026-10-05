#!/usr/bin/env bash
# channel-restore: idempotent self-heal for the WeCom + Weixin gateways.
#
# Covers the known VM-replacement failure mode: systemd units live in
# /etc/systemd/system and do NOT survive a VM replacement, while the
# workspace copies do. This script, for each gateway service:
#   1. reinstalls the unit from the workspace copy if it is missing or
#      differs, then daemon-reloads
#   2. enables the service if needed and starts it if not active
#   3. verifies the service is active and reports the gateway state
# It also reinstalls its own service and timer. Those units are what
# invoke this script; a replacement VM otherwise loses the schedule.
# Safe to run any time (also on a 5-minute timer via
# channel-restore.timer): when everything is healthy it changes nothing.
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

install_unit() {
  local src="$1"
  local name
  name="$(basename "$src")"
  local dst="/etc/systemd/system/$name"
  if [[ ! -f "$src" ]]; then
    echo "channel-restore: FAIL unit copy missing: $src"
    return 1
  fi
  if [[ ! -f "$dst" ]] || ! cmp -s "$src" "$dst"; then
    cp "$src" "$dst" && echo "channel-restore: installed $name"
    RELOAD=1
  fi
  return 0
}

heal_one() {
  local svc="$1" proj="$2" cred="$3"
  local src="$WS/$proj/$svc.service"
  local dst="/etc/systemd/system/$svc.service"

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
  if [[ ! -f "$dst" ]] || ! cmp -s "$src" "$dst"; then
    cp "$src" "$dst" && echo "$svc: unit (re)installed from workspace copy"
    RELOAD=1
  fi
  if [[ "$RELOAD" == "1" ]]; then
    systemctl daemon-reload
    RELOAD=0
  fi
  if id hatch >/dev/null 2>&1; then
    chown -R hatch:hatch "$WS/$proj/state" 2>/dev/null || true
  fi
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
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

main() {
  local HOME_DIR
  HOME_DIR="$(resolve_install_home)"
  local WS="$HOME_DIR/workspace"
  local FAIL=0
  local RELOAD=0

  install_unit "$WS/channel-restore/channel-restore.service" || FAIL=1
  install_unit "$WS/channel-restore/channel-restore.timer" || FAIL=1
  if [[ "$RELOAD" == "1" ]]; then
    systemctl daemon-reload
    RELOAD=0
  fi
  if ! systemctl is-enabled --quiet channel-restore.timer 2>/dev/null; then
    systemctl enable channel-restore.timer >/dev/null 2>&1 && echo "channel-restore: timer enabled"
  fi
  if ! systemctl is-active --quiet channel-restore.timer 2>/dev/null; then
    systemctl start channel-restore.timer >/dev/null 2>&1 && echo "channel-restore: timer started"
  fi

  heal_one wecom-bot wecom-bot "$HOME_DIR/.config/wecom-bot/credentials.env" || FAIL=1
  heal_one weixin-bot weixin-bot "$HOME_DIR/.config/weixin-bot/credentials.env" || FAIL=1

  if [[ "$FAIL" == "0" ]]; then
    echo "channel-restore: all gateways healthy"
  else
    echo "channel-restore: FAILURES above"
  fi
  exit "$FAIL"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
