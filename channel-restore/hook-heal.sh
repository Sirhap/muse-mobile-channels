#!/usr/bin/env bash
# hook-heal.sh — method-1 primary trigger for channel self-heal
# (2026-10-11, user-approved: inbox polling primary, timer backup).
#
# Called at the top of every wecom/weixin inbox hook poll (~5s, from
# outside the VM's systemd, files on the surviving 100G volume).
# A VM replacement wipes /etc/systemd/system — including
# channel-restore.timer, the old trigger — but it cannot wipe this
# caller, so the heal no longer waits for a heartbeat check or a
# human to notice.
#
# Behaviour:
#   * Every call does only a cheap file check: the six systemd units
#     against their workspace copies. All present and identical (the
#     normal case) -> exit 0 immediately, nothing is changed, no
#     systemctl call is made.
#   * A missing or differing unit -> heal attempt: run the existing
#     idempotent restore.sh, which reinstalls the units and starts
#     whatever is down. Attempts are serialised with flock (the two
#     channel hooks poll concurrently) and throttled to at most one
#     per THROTTLE_SECS, so a restore that cannot fix things cannot
#     hammer systemd on every 5s poll.
#   * At most once per THROTTLE_SECS a deeper check also runs:
#     channel-restore.timer and the four services enabled/active.
#     Anything down -> same heal attempt. restore.sh stays the
#     single healing implementation; this script only triggers it.
#   * channel-restore.timer keeps running as the backup trigger.
#
# Fail-silent by design: any error here exits 0 and never disturbs
# the caller's inbox poll. Test hooks: HOOK_HEAL_WS, HOOK_HEAL_UNIT_DIR,
# HOOK_HEAL_STATE_DIR, HOOK_HEAL_RESTORE, HOOK_HEAL_SYSTEMCTL and
# HOOK_HEAL_THROTTLE_SECS override the production paths.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/restore.sh"
INSTALL_HOME="$(resolve_install_home)"
WS="${HOOK_HEAL_WS:-$INSTALL_HOME/workspace}"
UNIT_DIR="${HOOK_HEAL_UNIT_DIR:-/etc/systemd/system}"
STATE_DIR="${HOOK_HEAL_STATE_DIR:-$INSTALL_HOME/hooks/state/channel-restore}"
RESTORE="${HOOK_HEAL_RESTORE:-$WS/channel-restore/restore.sh}"
SYSTEMCTL="${HOOK_HEAL_SYSTEMCTL:-systemctl}"
THROTTLE_SECS="${HOOK_HEAL_THROTTLE_SECS:-120}"
LOCK_FILE="$STATE_DIR/hook-heal.lock"
ATTEMPT_STAMP="$STATE_DIR/hook-heal.last-attempt"
DEEP_STAMP="$STATE_DIR/hook-heal.last-deepcheck"
LOG_FILE="$STATE_DIR/hook-heal.log"

UNITS=(
  "wecom-bot/wecom-bot.service"
  "weixin-bot/weixin-bot.service"
  "native-bridge/native-bridge.service"
  "approval-relay/approval-relay.service"
  "channel-restore/channel-restore.service"
  "channel-restore/channel-restore.timer"
)
SERVICES=(wecom-bot weixin-bot native-bridge approval-relay)

# Drop-in overrides (core-mode env) that must survive a VM replacement
# alongside the units. A service can be active yet silently legacy
# without its drop-in (seen 2026-11-07: the 12:43 replacement wiped
# /etc drop-ins, both services stayed active in legacy mode), so these
# are checked on every poll too. Paths are /etc-relative; the workspace
# copy lives under channel-restore/<same path>.
DROPINS=(
  "wecom-bot.service.d/core-mode.conf"
  "approval-relay.service.d/modes.conf"
)

dropins_unhealthy() {
  local rel src dst
  for rel in "${DROPINS[@]}"; do
    src="$WS/channel-restore/$rel"
    dst="$UNIT_DIR/$rel"
    [[ -f "$src" ]] || continue
    if [[ -f "${dst%/*}/.muse-restart-pending" || ! -f "$dst" ]] ||
        ! render_dropin "$src" "$WS" | cmp -s - "$dst"; then
      core_mode_requested "$WS" 2>/dev/null
      return $?
    fi
  done
  src="$WS/channel-restore/muse-core-run.service"
  if [[ -f "$src" ]] && core_mode_requested "$WS" 2>/dev/null; then
    render_unit "$src" "$WS" | cmp -s - "$UNIT_DIR/muse-core-run.service" || return 0
  fi
  return 1
}

service_selected() {
  case "$1" in
    wecom-bot|weixin-bot) gateway_selected "$1" "${WS%/workspace}" "$UNIT_DIR" ;;
    *) return 0 ;;
  esac
}

units_unhealthy() {
  local rel src dst
  [[ -f "$UNIT_DIR/.muse-reload-pending" ]] && return 0
  for rel in "${UNITS[@]}"; do
    src="$WS/$rel"
    dst="$UNIT_DIR/$(basename "$rel")"
    [[ -f "$src" ]] || continue
    service_selected "$(basename "$rel" .service)" || continue
    [[ -f "$dst" ]] || return 0
    render_unit "$src" "$WS" | cmp -s - "$dst" || return 0
  done
  return 1
}

stamp_age() {
  local f="$1" now mtime
  [[ -f "$f" ]] || { echo 999999; return; }
  now="$(date +%s)"
  mtime="$(stat -c %Y "$f" 2>/dev/null || stat -f %m "$f" 2>/dev/null || echo 0)"
  echo $((now - mtime))
}

deep_unhealthy() {
  # Only called when the deep-check stamp is stale. A systemctl that
  # cannot answer (bus down etc.) is not evidence of a wipe: report
  # healthy here and let the probe/heartbeat path judge that case.
  "$SYSTEMCTL" is-active --quiet channel-restore.timer 2>/dev/null || {
    "$SYSTEMCTL" show -p LoadState channel-restore.timer >/dev/null 2>&1 || return 1
    return 0
  }
  "$SYSTEMCTL" is-enabled --quiet channel-restore.timer 2>/dev/null || return 0
  local svc
  for svc in "${SERVICES[@]}"; do
    service_selected "$svc" || continue
    "$SYSTEMCTL" is-active --quiet "$svc" 2>/dev/null || return 0
  done
  if core_mode_requested "$WS" 2>/dev/null; then
    "$SYSTEMCTL" is-active --quiet muse-core-run 2>/dev/null || return 0
  fi
  return 1
}

main() {
  mkdir -p "$STATE_DIR" 2>/dev/null || exit 0
  local need_heal=0
  if units_unhealthy || dropins_unhealthy; then
    need_heal=1
  elif [[ "$(stamp_age "$DEEP_STAMP")" -ge "$THROTTLE_SECS" ]]; then
    touch "$DEEP_STAMP" 2>/dev/null || true
    if deep_unhealthy; then
      need_heal=1
    fi
  fi
  [[ "$need_heal" == "1" ]] || exit 0
  # Serialise + throttle heal attempts across both channel hooks.
  exec 9>"$LOCK_FILE" 2>/dev/null || exit 0
  flock -n 9 || exit 0
  [[ "$(stamp_age "$ATTEMPT_STAMP")" -ge "$THROTTLE_SECS" ]] || exit 0
  # Re-check inside the lock: the other hook may have just healed.
  if ! units_unhealthy && ! dropins_unhealthy && ! deep_unhealthy; then
    exit 0
  fi
  touch "$ATTEMPT_STAMP" 2>/dev/null || true
  [[ -x "$RESTORE" ]] || exit 0
  {
    echo "[$(date '+%F %T %Z')] hook-heal: unit/service unhealthy, running restore.sh"
    "$RESTORE" 2>&1
    echo "[$(date '+%F %T %Z')] hook-heal: restore.sh exit=$?"
  } >>"$LOG_FILE" 2>&1 || true
  exit 0
}

main "$@"
