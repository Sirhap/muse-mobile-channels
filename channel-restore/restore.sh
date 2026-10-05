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
# Safe to run any time (also on a 5-minute timer via
# channel-restore.timer): when everything is healthy it changes nothing.
set -uo pipefail

HOME_DIR="${HOME:-/home/hatch}"
WS="$HOME_DIR/workspace"
FAIL=0

heal_one() {
  local svc="$1" proj="$2" cred="$3"
  local src="$WS/$proj/$svc.service"
  local dst="/etc/systemd/system/$svc.service"

  if [[ ! -x "$WS/$proj/.venv/bin/python" ]]; then
    echo "$svc: WARN venv python missing at $WS/$proj/.venv/bin/python"
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
    systemctl daemon-reload
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

heal_one wecom-bot wecom-bot "$HOME_DIR/.config/wecom-bot/credentials.env" || FAIL=1
heal_one weixin-bot weixin-bot "$HOME_DIR/.config/weixin-bot/credentials.env" || FAIL=1

if [[ "$FAIL" == "0" ]]; then
  echo "channel-restore: all gateways healthy"
else
  echo "channel-restore: FAILURES above"
fi
exit "$FAIL"
