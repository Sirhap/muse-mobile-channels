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

install_dropins() {
  # Sync a service's systemd drop-in overrides from the workspace copy.
  # A drop-in carries the MUSE_CHANNEL_MODES env that selects core mode;
  # without it the service stays active yet silently legacy (seen
  # 2026-11-07: the 12:43 replacement wiped /etc drop-ins and both
  # services ran legacy for 24 minutes). Sets DROPIN_CHANGED=1 (and
  # RELOAD=1) when any file was (re)installed, so the caller can
  # daemon-reload and restart the service. Services without a workspace
  # drop-in dir (e.g. weixin-bot, legacy by design) are untouched.
  # $1 = service name, $2 = workspace drop-in dir, relative to $WS.
  local svc="$1" srcdir="$WS/$2"
  local dstdir="/etc/systemd/system/$svc.service.d"
  [[ -d "$srcdir" ]] || return 0
  mkdir -p "$dstdir"
  local src dst
  for src in "$srcdir"/*.conf; do
    [[ -f "$src" ]] || continue
    dst="$dstdir/$(basename "$src")"
    if [[ ! -f "$dst" ]] || ! cmp -s "$src" "$dst"; then
      cp "$src" "$dst" && echo "$svc: drop-in (re)installed: $(basename "$src")"
      DROPIN_CHANGED=1
      RELOAD=1
    fi
  done
  return 0
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
  # Drop-in overrides (core-mode env) live beside the unit copy in
  # channel-restore/. A changed drop-in must restart the service: a
  # running process keeps the environment it started with, so install
  # + reload alone would leave it silently legacy.
  DROPIN_CHANGED=0
  install_dropins "$svc" "channel-restore/$svc.service.d"
  if [[ "$RELOAD" == "1" ]]; then
    systemctl daemon-reload
    RELOAD=0
  fi
  # Only the first time the tree is still root-owned. Repeating chown
  # on a large media directory every 5 minutes stalls the healer.
  ensure_hatch_tree "$WS/$proj/state"
  ensure_hatch_tree "$(dirname "$cred")"
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
    sleep 3
  elif [[ "$DROPIN_CHANGED" == "1" ]]; then
    systemctl restart "$svc" 2>/dev/null && echo "$svc: restarted (drop-in changed)"
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
  local dst="/etc/systemd/system/$svc.service"
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
  local FAIL=0
  local RELOAD=0
  local DROPIN_CHANGED=0

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
  heal_bridge "$WS" "$HOME_DIR" || FAIL=1
  heal_relay "$WS" "$HOME_DIR" || FAIL=1
  heal_core_run "$WS" "$HOME_DIR" || FAIL=1

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
  local dst="/etc/systemd/system/$svc.service"
  if [[ ! -f "$src" ]]; then
    echo "$svc: FAIL workspace unit copy missing: $src"
    return 1
  fi
  local changed=0
  if [[ ! -f "$dst" ]] || ! cmp -s "$src" "$dst"; then
    cp "$src" "$dst" && echo "$svc: unit (re)installed from workspace copy"
    changed=1
  fi
  DROPIN_CHANGED=0
  install_dropins "$svc" "channel-restore/$svc.service.d"
  if [[ "$changed" == "1" || "$DROPIN_CHANGED" == "1" ]]; then
    systemctl daemon-reload
  fi
  if ! systemctl is-enabled --quiet "$svc" 2>/dev/null; then
    systemctl enable "$svc" >/dev/null 2>&1 && echo "$svc: enabled"
  fi
  if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
    systemctl start "$svc" 2>/dev/null && echo "$svc: started"
  elif [[ "$DROPIN_CHANGED" == "1" ]]; then
    systemctl restart "$svc" 2>/dev/null && echo "$svc: restarted (drop-in changed)"
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
  local dst="/etc/systemd/system/$svc.service"
  if [[ ! -x "$HOME_DIR/muse-test-venv/bin/python" ]]; then
    echo "$svc: FAIL venv python missing at $HOME_DIR/muse-test-venv/bin/python"
    return 1
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
  systemctl is-active --quiet "$svc" 2>/dev/null && echo "$svc: active"
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
