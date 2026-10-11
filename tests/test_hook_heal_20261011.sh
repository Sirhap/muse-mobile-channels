#!/usr/bin/env bash
# Sandbox self-test for hook-heal.sh: fake workspace, fake unit dir,
# fake systemctl and a counting fake restore.sh. Production /etc and
# the real services are never touched.
set -uo pipefail
HELPER="/home/hatch/workspace/channel-restore/hook-heal.sh"
ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "pass: $1"; }
bad(){ FAIL=$((FAIL+1)); echo "FAIL: $1"; }

WS="$ROOT/ws"; UNITS="$ROOT/etc"; STATE="$ROOT/state"; BIN="$ROOT/bin"
mkdir -p "$WS"/{wecom-bot,weixin-bot,native-bridge,approval-relay,channel-restore} "$UNITS" "$STATE" "$BIN"
for rel in wecom-bot/wecom-bot.service weixin-bot/weixin-bot.service native-bridge/native-bridge.service approval-relay/approval-relay.service channel-restore/channel-restore.service channel-restore/channel-restore.timer; do
  echo "unit $rel" > "$WS/$rel"
  echo "unit $rel" > "$UNITS/$(basename "$rel")"
done
cat > "$BIN/restore" <<'EOF'
#!/usr/bin/env bash
echo x >> "$FAKE_COUNT_FILE"
# Simulate the heal: restore any missing/differing unit files.
for rel in wecom-bot/wecom-bot.service weixin-bot/weixin-bot.service native-bridge/native-bridge.service approval-relay/approval-relay.service channel-restore/channel-restore.service channel-restore/channel-restore.timer; do
  cp "$FAKE_WS/$rel" "$FAKE_UNIT_DIR/$(basename "$rel")"
done
echo "fake restore: healed"
EOF
cat > "$BIN/systemctl" <<'EOF'
#!/usr/bin/env bash
# FAKE_DOWN names a unit that reports inactive/not-enabled for
# is-active/is-enabled; `show` always succeeds (systemd bus is up).
if [[ "${1:-}" == "show" ]]; then echo "LoadState=loaded"; exit 0; fi
for a in "$@"; do
  if [[ -n "${FAKE_DOWN:-}" && "$a" == "$FAKE_DOWN" ]]; then exit 1; fi
done
exit 0
EOF
chmod +x "$BIN/restore" "$BIN/systemctl"
export FAKE_COUNT_FILE="$ROOT/count" FAKE_WS="$WS" FAKE_UNIT_DIR="$UNITS"
: > "$FAKE_COUNT_FILE"
run_heal(){ env HOOK_HEAL_WS="$WS" HOOK_HEAL_UNIT_DIR="$UNITS" HOOK_HEAL_STATE_DIR="$STATE" HOOK_HEAL_RESTORE="$BIN/restore" HOOK_HEAL_SYSTEMCTL="$BIN/systemctl" FAKE_DOWN="${FAKE_DOWN:-}" bash "$HELPER"; }
calls(){ wc -l < "$FAKE_COUNT_FILE" | tr -d ' '; }
backdate(){ touch -d "$1 seconds ago" "$2"; }

# 1. Healthy files, deep-check stamp fresh -> no restore call.
touch "$STATE/hook-heal.last-deepcheck"
run_heal; [[ "$(calls)" == "0" ]] && ok "healthy-no-call" || bad "healthy-no-call calls=$(calls)"

# 2. One unit missing -> restore called exactly once, unit restored.
rm "$UNITS/native-bridge.service"
run_heal; [[ "$(calls)" == "1" && -f "$UNITS/native-bridge.service" ]] && ok "missing-unit-healed" || bad "missing-unit-healed calls=$(calls)"

# 3. Missing again but attempt throttled (120s) -> no second call.
rm "$UNITS/native-bridge.service"
run_heal; [[ "$(calls)" == "1" ]] && ok "throttled-second-attempt" || bad "throttled-second-attempt calls=$(calls)"

# 4. Throttle expired -> heals again.
backdate 300 "$STATE/hook-heal.last-attempt"
run_heal; [[ "$(calls)" == "2" && -f "$UNITS/native-bridge.service" ]] && ok "heal-after-throttle" || bad "heal-after-throttle calls=$(calls)"

# 5. Files healthy but timer down on a stale deep check -> heal attempt.
backdate 300 "$STATE/hook-heal.last-attempt"; backdate 300 "$STATE/hook-heal.last-deepcheck"
FAKE_DOWN="channel-restore.timer" run_heal
[[ "$(calls)" == "3" ]] && ok "deep-check-timer-down" || bad "deep-check-timer-down calls=$(calls)"

# 6. Differing unit content counts as unhealthy.
echo tampered > "$UNITS/wecom-bot.service"
backdate 300 "$STATE/hook-heal.last-attempt"
run_heal; [[ "$(calls)" == "4" ]] && grep -q '^unit ' "$UNITS/wecom-bot.service" && ok "differing-unit-healed" || bad "differing-unit-healed calls=$(calls)"

echo "test_hook_heal: PASS=$PASS FAIL=$FAIL"
[[ "$FAIL" == "0" ]]
