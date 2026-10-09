#!/usr/bin/env bash
# Read-only liveness probe for the channel-restore timer and the
# gateways. VM replacement deletes /etc/systemd units, including the
# timer that would run restore.sh, so this check is meant to be
# started from outside the VM (SSH) or by the heartbeat checklist in
# HEARTBEAT.md. It never installs, enables, starts, or restarts units.
#
# Exit 0: every check passed.
# Exit 1: at least one check failed (missing timer, gateway unhealthy).
# Exit 2: the probe could not check (bad args, wrong MUSE_HOME,
#         systemd bus down, ssh failure). Not a wipe result.
set -uo pipefail

MUSE_HOME="${MUSE_HOME:-/home/hatch}"
BRIDGE_MAX_AGE=180
SSH_TARGET=""
SSH_CONNECT_TIMEOUT="${SSH_CONNECT_TIMEOUT:-10}"
SELF_TEST=0
FAILS=0

usage() {
  cat <<'EOF'
usage: liveness-probe.sh [--ssh USER@HOST] [--muse-home DIR]
                         [--bridge-max-age SECS] [--self-test]

Checks channel-restore.timer, the gateway units, gateway status.json,
and native-bridge status.json. With --ssh, the same checks run on
HOST via ssh (BatchMode); the script is sent on stdin and does not
need to be installed there.

Does not heal. Recovery is: sudo MUSE_HOME=... channel-restore/restore.sh
See HEARTBEAT.md.
EOF
}

ok() {
  printf 'OK   %s\n' "$*"
}

bad() {
  printf 'FAIL %s\n' "$*"
  FAILS=$((FAILS + 1))
}

# pattern is an anchored ERE. Paths and ssh targets are checked
# separately so a muse-home slash is not also permission for ssh.
require_charset() {
  local label="$1" value="$2" pattern="$3"
  if [[ ! "$value" =~ $pattern ]]; then
    echo "liveness-probe: $label has characters this probe will not forward" >&2
    exit 2
  fi
}

# Print LoadState, ActiveState, UnitFileState. Callers invoke this
# inside $(...), so a real exit would only leave the subshell; a dead
# bus is reported as a PROBE_ERROR line and the caller stops.
show_unit() {
  local unit="$1" err out
  err=$(mktemp)
  if ! out=$(systemctl show -p LoadState -p ActiveState -p UnitFileState --no-pager "$unit" 2>"$err"); then
    if grep -q -e "Failed to connect to bus" -e "not been booted with systemd" "$err"; then
      cat "$err" >&2
      rm -f "$err"
      echo "liveness-probe: systemd bus unavailable; this is a probe error, not a missing unit" >&2
      printf 'PROBE_ERROR\n'
      return 0
    fi
    if [[ -z "$out" ]]; then
      cat "$err" >&2
      rm -f "$err"
      echo "liveness-probe: systemctl show $unit failed" >&2
      printf 'PROBE_ERROR\n'
      return 0
    fi
  fi
  rm -f "$err"
  printf '%s\n' "$out"
}

# $(show_unit) cannot exit the probe. A bus failure is a first line
# of PROBE_ERROR; stop before later units are called missing.
abort_if_probe_error() {
  local show="$1"
  if [[ "${show%%$'\n'*}" == "PROBE_ERROR" ]]; then
    exit 2
  fi
}

field_of() {
  local key="$1" line
  while IFS= read -r line; do
    if [[ "$line" == "$key="* ]]; then
      printf '%s\n' "${line#"$key"=}"
      return 0
    fi
  done
  printf '\n'
  return 0
}

check_loaded_active() {
  local unit="$1" show load active file
  show=$(show_unit "$unit")
  abort_if_probe_error "$show"
  load=$(printf '%s\n' "$show" | field_of LoadState)
  active=$(printf '%s\n' "$show" | field_of ActiveState)
  file=$(printf '%s\n' "$show" | field_of UnitFileState)
  if [[ "$load" != "loaded" || "$active" != "active" ]]; then
    bad "unit $unit LoadState=${load:-empty} ActiveState=${active:-empty} UnitFileState=${file:-empty}"
    return
  fi
  ok "unit $unit active"
}

check_timer() {
  local show load active file
  show=$(show_unit "channel-restore.timer")
  abort_if_probe_error "$show"
  load=$(printf '%s\n' "$show" | field_of LoadState)
  active=$(printf '%s\n' "$show" | field_of ActiveState)
  file=$(printf '%s\n' "$show" | field_of UnitFileState)
  if [[ "$load" != "loaded" || "$active" != "active" || "$file" != "enabled" ]]; then
    bad "unit channel-restore.timer LoadState=${load:-empty} ActiveState=${active:-empty} UnitFileState=${file:-empty}"
    return
  fi
  ok "unit channel-restore.timer loaded/enabled/active"
}

check_oneshot_installed() {
  local show load
  show=$(show_unit "channel-restore.service")
  abort_if_probe_error "$show"
  load=$(printf '%s\n' "$show" | field_of LoadState)
  if [[ "$load" != "loaded" ]]; then
    bad "unit channel-restore.service LoadState=${load:-empty} (oneshot; inactive between runs is normal)"
    return
  fi
  ok "unit channel-restore.service loaded (oneshot)"
}

# One line: "ok <age> <connected 0|1> <error 0|1> <state>", or
# missing / bad / nofield. Exit status is python's.
read_status() {
  python3 - "$1" "$2" <<'PY'
import json
import re
import sys
import time

path, kind = sys.argv[1], sys.argv[2]


def age_of(value):
    """Seconds since a unix timestamp. Bool is not a timestamp."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(time.time() - float(value))


try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
except FileNotFoundError:
    print("missing")
    raise SystemExit(0)
except (OSError, json.JSONDecodeError, UnicodeError):
    print("bad")
    raise SystemExit(0)

if not isinstance(data, dict):
    print("bad")
    raise SystemExit(0)

if kind == "gateway":
    age = age_of(data.get("updated_at"))
    if age is None:
        print("nofield")
        raise SystemExit(0)
    connected = "1" if data.get("connected") is True else "0"
    last_error = data.get("last_error")
    text = last_error if isinstance(last_error, str) else ""
    has_error = "1" if text.strip() else "0"
    state = re.sub(r"[^A-Za-z0-9_.-]", "_", str(data.get("state") or "unknown"))
    print(f"ok {age} {connected} {has_error} {state}")
    raise SystemExit(0)

if kind == "bridge":
    age = age_of(data.get("ts"))
    if age is None:
        print("nofield")
        raise SystemExit(0)
    print(f"ok {age}")
    raise SystemExit(0)

print("bad")
raise SystemExit(2)
PY
}

check_gateway() {
  local name="$1" path="$2" line token age conn err state detail
  if ! line=$(read_status "$path" gateway); then
    echo "liveness-probe: python failed reading $path" >&2
    exit 2
  fi
  token=${line%% *}
  if [[ "$token" != "ok" ]]; then
    bad "status $name $token ($path)"
    return
  fi
  read -r token age conn err state <<<"$line"
  if [[ "$conn" == "1" ]]; then
    detail="connected=yes"
  else
    detail="connected=no"
  fi
  if [[ "$err" == "1" ]]; then
    detail="$detail last_error=set"
  else
    detail="$detail last_error=clear"
  fi
  detail="state=$state $detail age=${age}s"
  # Idle Weixin/WeCom sessions do not rewrite updated_at. Age is
  # printed so the log shows it; it is not a failure by itself.
  if [[ "$conn" != "1" || "$state" != "connected" || "$err" != "0" ]]; then
    bad "status $name $detail"
    return
  fi
  ok "status $name $detail"
}

check_bridge() {
  local path="$1" line token age
  if ! line=$(read_status "$path" bridge); then
    echo "liveness-probe: python failed reading $path" >&2
    exit 2
  fi
  token=${line%% *}
  if [[ "$token" != "ok" ]]; then
    bad "status native-bridge $token ($path)"
    return
  fi
  read -r token age <<<"$line"
  if [[ "$age" -lt 0 ]]; then
    bad "status native-bridge ts is ${age}s ahead of this clock"
    return
  fi
  if [[ "$age" -gt "$BRIDGE_MAX_AGE" ]]; then
    bad "status native-bridge stale age=${age}s (max ${BRIDGE_MAX_AGE}s)"
    return
  fi
  ok "status native-bridge age=${age}s"
}

check_workspace_copies() {
  local root="$MUSE_HOME/workspace"
  local restore="$root/channel-restore/restore.sh"
  local timer_copy="$root/channel-restore/channel-restore.timer"
  local relay="$root/approval-relay/approval-relay.service"
  if [[ ! -x "$restore" ]]; then
    bad "workspace restore.sh missing or not executable ($restore)"
  else
    ok "workspace restore.sh executable"
  fi
  if [[ ! -f "$timer_copy" ]]; then
    bad "workspace channel-restore.timer copy missing ($timer_copy)"
  else
    ok "workspace channel-restore.timer present"
  fi
  if [[ ! -f "$relay" ]]; then
    bad "workspace approval-relay.service copy missing ($relay)"
  else
    ok "workspace approval-relay.service present"
  fi
}

run_checks() {
  local root="$MUSE_HOME/workspace"
  if [[ ! -d "$root/channel-restore" ]]; then
    echo "liveness-probe: no channel-restore tree under $root (wrong host or MUSE_HOME?)" >&2
    exit 2
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "liveness-probe: python3 is required to read status.json" >&2
    exit 2
  fi
  check_workspace_copies
  check_timer
  check_oneshot_installed
  check_loaded_active "weixin-bot.service"
  check_loaded_active "wecom-bot.service"
  check_loaded_active "native-bridge.service"
  check_loaded_active "approval-relay.service"
  check_gateway "weixin-bot" "$root/weixin-bot/state/status.json"
  check_gateway "wecom-bot" "$root/wecom-bot/state/status.json"
  check_bridge "$root/native-bridge/status.json"
  if [[ "$FAILS" -eq 0 ]]; then
    echo "liveness-probe: OK"
    exit 0
  fi
  echo "liveness-probe: FAIL $FAILS"
  exit 1
}

run_remote() {
  local quoted_home quoted_age
  if [[ ! -f "$0" ]]; then
    echo "liveness-probe: cannot read $0 to send it over ssh" >&2
    exit 2
  fi
  require_charset "ssh target" "$SSH_TARGET" '^[A-Za-z0-9._@:-]+$'
  require_charset "muse-home" "$MUSE_HOME" '^[A-Za-z0-9._/-]+$'
  if [[ ! "$SSH_CONNECT_TIMEOUT" =~ ^[0-9]+$ ]]; then
    echo "liveness-probe: SSH_CONNECT_TIMEOUT must be an integer" >&2
    exit 2
  fi
  quoted_home=$(printf '%q' "$MUSE_HOME")
  quoted_age=$(printf '%q' "$BRIDGE_MAX_AGE")
  # stdin is the script. Remote bash does not need a checkout.
  # ssh uses 255 for its own failures and otherwise returns the remote
  # probe status (0 healthy, 1 failed checks, 2 probe error).
  local rc=0
  ssh \
    -o BatchMode=yes \
    -o "ConnectTimeout=${SSH_CONNECT_TIMEOUT}" \
    "$SSH_TARGET" \
    "bash -s -- --muse-home ${quoted_home} --bridge-max-age ${quoted_age}" \
    < "$0" || rc=$?
  if [[ "$rc" -eq 255 ]]; then
    echo "liveness-probe: ssh failed" >&2
    exit 2
  fi
  exit "$rc"
}

self_test() {
  local root bin cases=0
  root=$(mktemp -d)
  bin="$root/bin"
  mkdir -p "$bin"
  # Expand the path now. A RETURN/EXIT trap that closes over the local
  # name runs too late and would see an empty root.
  trap "rm -rf $(printf '%q' "$root")" EXIT

  cat >"$bin/systemctl" <<'EOF'
#!/usr/bin/env bash
if [[ "${LIVENESS_BUS_DOWN:-}" == "1" ]]; then
  echo "System has not been booted with systemd as init system (PID 1). Can't operate." >&2
  echo "Failed to connect to bus: Host is down" >&2
  exit 1
fi
if [[ "${1:-}" != "show" ]]; then
  echo "unexpected systemctl invocation: $*" >&2
  exit 1
fi
unit="${!#}"
file="${LIVENESS_FIXTURE:?}/$unit"
if [[ ! -f "$file" ]]; then
  printf '%s\n' "LoadState=not-found" "ActiveState=inactive" "UnitFileState="
  exit 0
fi
cat "$file"
EOF
  cat >"$bin/ssh" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" > "${LIVENESS_SSH_CAPTURE:?}/args"
cat > "${LIVENESS_SSH_CAPTURE}/stdin"
exit 9
EOF
  chmod +x "$bin/systemctl" "$bin/ssh"

  assert_case() {
    local name="$1" want="$2"
    shift 2
    local out rc
    cases=$((cases + 1))
    out=$("$@" 2>&1) && rc=0 || rc=$?
    if [[ "$rc" -ne "$want" ]]; then
      echo "self-test FAIL $name: exit $rc want $want" >&2
      printf '%s\n' "$out" >&2
      exit 1
    fi
    if [[ "$name" == "healthy" ]]; then
      grep -q "liveness-probe: OK" <<<"$out" || {
        echo "self-test FAIL $name: missing OK summary" >&2
        exit 1
      }
      grep -q "unit channel-restore.timer loaded/enabled/active" <<<"$out" || {
        echo "self-test FAIL $name: timer not reported healthy" >&2
        exit 1
      }
      # Stale-but-connected gateway status must not fail the probe.
      grep -q "status weixin-bot state=connected" <<<"$out" || {
        echo "self-test FAIL $name: weixin status missing" >&2
        exit 1
      }
    fi
    if [[ "$name" == "timer-missing" ]]; then
      grep -q "FAIL unit channel-restore.timer" <<<"$out" || {
        echo "self-test FAIL $name: timer failure not reported" >&2
        printf '%s\n' "$out" >&2
        exit 1
      }
    fi
    if [[ "$name" == "gateway-down" ]]; then
      grep -q "FAIL status wecom-bot" <<<"$out" || {
        echo "self-test FAIL $name: wecom status not failed" >&2
        exit 1
      }
    fi
    if [[ "$name" == "bridge-stale" ]]; then
      grep -q "FAIL status native-bridge stale" <<<"$out" || {
        echo "self-test FAIL $name: bridge staleness not failed" >&2
        exit 1
      }
    fi
    if [[ "$name" == "bus-down" ]]; then
      grep -q "systemd bus unavailable" <<<"$out" || {
        echo "self-test FAIL $name: bus error not distinguished from a wipe" >&2
        exit 1
      }
      grep -q "FAIL unit" <<<"$out" && {
        echo "self-test FAIL $name: bus down was reported as missing units" >&2
        exit 1
      }
    fi
    echo "self-test pass $name"
  }

  write_tree() {
    local home="$1" weixin_age="$2" wecom_state="$3" wecom_conn="$4" bridge_ts="$5"
    mkdir -p \
      "$home/workspace/channel-restore" \
      "$home/workspace/approval-relay" \
      "$home/workspace/weixin-bot/state" \
      "$home/workspace/wecom-bot/state" \
      "$home/workspace/native-bridge"
    printf '%s\n' '#!/usr/bin/env bash' >"$home/workspace/channel-restore/restore.sh"
    chmod +x "$home/workspace/channel-restore/restore.sh"
    printf '%s\n' '[Timer]' >"$home/workspace/channel-restore/channel-restore.timer"
    printf '%s\n' '[Service]' >"$home/workspace/approval-relay/approval-relay.service"
    python3 - "$home" "$weixin_age" "$wecom_state" "$wecom_conn" "$bridge_ts" <<'PY'
import json
import sys
import time

home, weixin_age, wecom_state, wecom_conn, bridge_ts = sys.argv[1:]
now = int(time.time())
weixin = {
    "state": "connected",
    "connected": True,
    "updated_at": now - int(weixin_age),
    "last_error": "",
}
wecom = {
    "state": wecom_state,
    "connected": wecom_conn == "yes",
    "updated_at": now,
    "last_error": "" if wecom_state == "connected" else "down",
}
bridge = {"ts": int(bridge_ts) if bridge_ts != "now" else now}
with open(home + "/workspace/weixin-bot/state/status.json", "w", encoding="utf-8") as handle:
    json.dump(weixin, handle)
with open(home + "/workspace/wecom-bot/state/status.json", "w", encoding="utf-8") as handle:
    json.dump(wecom, handle)
with open(home + "/workspace/native-bridge/status.json", "w", encoding="utf-8") as handle:
    json.dump(bridge, handle)
PY
  }

  write_units() {
    local dir="$1" omit_timer="${2:-}"
    mkdir -p "$dir"
    printf '%s\n' "LoadState=loaded" "ActiveState=active" "UnitFileState=enabled" \
      >"$dir/weixin-bot.service"
    cp "$dir/weixin-bot.service" "$dir/wecom-bot.service"
    cp "$dir/weixin-bot.service" "$dir/native-bridge.service"
    cp "$dir/weixin-bot.service" "$dir/approval-relay.service"
    printf '%s\n' "LoadState=loaded" "ActiveState=inactive" "UnitFileState=static" \
      >"$dir/channel-restore.service"
    if [[ "$omit_timer" != "omit-timer" ]]; then
      printf '%s\n' "LoadState=loaded" "ActiveState=active" "UnitFileState=enabled" \
        >"$dir/channel-restore.timer"
    fi
  }

  local healthy_home="$root/healthy-home"
  local down_home="$root/down-home"
  local stale_home="$root/stale-home"
  local units_ok="$root/units-ok"
  local units_notimer="$root/units-notimer"
  write_tree "$healthy_home" 10000 connected yes now
  write_tree "$down_home" 5 reconnecting no now
  write_tree "$stale_home" 5 connected yes 0
  write_units "$units_ok"
  write_units "$units_notimer" omit-timer

  assert_case healthy 0 \
    env PATH="$bin:$PATH" LIVENESS_FIXTURE="$units_ok" LIVENESS_BUS_DOWN= \
    bash "$0" --muse-home "$healthy_home"
  assert_case timer-missing 1 \
    env PATH="$bin:$PATH" LIVENESS_FIXTURE="$units_notimer" LIVENESS_BUS_DOWN= \
    bash "$0" --muse-home "$healthy_home"
  assert_case gateway-down 1 \
    env PATH="$bin:$PATH" LIVENESS_FIXTURE="$units_ok" LIVENESS_BUS_DOWN= \
    bash "$0" --muse-home "$down_home"
  assert_case bridge-stale 1 \
    env PATH="$bin:$PATH" LIVENESS_FIXTURE="$units_ok" LIVENESS_BUS_DOWN= \
    bash "$0" --muse-home "$stale_home" --bridge-max-age 180
  assert_case bus-down 2 \
    env PATH="$bin:$PATH" LIVENESS_FIXTURE="$units_ok" LIVENESS_BUS_DOWN=1 \
    bash "$0" --muse-home "$healthy_home"
  assert_case wrong-home 2 \
    env PATH="$bin:$PATH" LIVENESS_BUS_DOWN= \
    bash "$0" --muse-home "$root/no-such-home"
  assert_case ssh-bad-target 2 \
    env PATH="$bin:$PATH" \
    bash "$0" --ssh 'hatch@vm;id' --muse-home /home/hatch

  local ssh_cap="$root/ssh-cap"
  mkdir -p "$ssh_cap"
  cases=$((cases + 1))
  env PATH="$bin:$PATH" LIVENESS_SSH_CAPTURE="$ssh_cap" \
    bash "$0" --ssh hatch@vm.example --muse-home /home/hatch --bridge-max-age 180 \
    >/dev/null 2>&1 && rc=0 || rc=$?
  if [[ "$rc" -ne 9 ]]; then
    echo "self-test FAIL ssh-wrapper: exit $rc want 9" >&2
    exit 1
  fi
  if grep -q -- "--ssh" "$ssh_cap/args"; then
    echo "self-test FAIL ssh-wrapper: remote command still contains --ssh" >&2
    exit 1
  fi
  grep -q "BatchMode=yes" "$ssh_cap/args" || {
    echo "self-test FAIL ssh-wrapper: BatchMode not set" >&2
    exit 1
  }
  grep -q "hatch@vm.example" "$ssh_cap/args" || {
    echo "self-test FAIL ssh-wrapper: target missing" >&2
    exit 1
  }
  grep -q -- "--muse-home /home/hatch" "$ssh_cap/args" || {
    echo "self-test FAIL ssh-wrapper: muse-home not forwarded" >&2
    cat "$ssh_cap/args" >&2
    exit 1
  }
  grep -q "bash -s --" "$ssh_cap/args" || {
    echo "self-test FAIL ssh-wrapper: remote bash -s missing" >&2
    exit 1
  }
  grep -q "liveness probe" "$ssh_cap/stdin" || {
    echo "self-test FAIL ssh-wrapper: script body was not piped" >&2
    exit 1
  }
  echo "self-test pass ssh-wrapper"
  echo "self-test: OK $cases cases"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh)
      SSH_TARGET="${2:-}"
      if [[ -z "$SSH_TARGET" ]]; then
        echo "liveness-probe: --ssh needs a target" >&2
        exit 2
      fi
      shift 2
      ;;
    --muse-home)
      MUSE_HOME="${2:-}"
      if [[ -z "$MUSE_HOME" ]]; then
        echo "liveness-probe: --muse-home needs a directory" >&2
        exit 2
      fi
      shift 2
      ;;
    --bridge-max-age)
      BRIDGE_MAX_AGE="${2:-}"
      shift 2
      ;;
    --self-test)
      SELF_TEST=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "liveness-probe: unknown argument $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$BRIDGE_MAX_AGE" =~ ^[0-9]+$ ]] || [[ "$BRIDGE_MAX_AGE" -lt 1 ]]; then
  echo "liveness-probe: --bridge-max-age must be a positive integer" >&2
  exit 2
fi

if [[ "$SELF_TEST" -eq 1 ]]; then
  if [[ -n "$SSH_TARGET" ]]; then
    echo "liveness-probe: --self-test does not take --ssh" >&2
    exit 2
  fi
  self_test
  exit 0
fi

if [[ -n "$SSH_TARGET" ]]; then
  run_remote
fi

run_checks
