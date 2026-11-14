#!/usr/bin/env bash
# Install the muse-mobile-channels systemd units (README step 7, scripted).
#
# The unit files in this repo hard-code /home/hatch as the install user's
# home. This script renders each unit for the target home, installs it to
# /etc/systemd/system, and enables it -- the manual sed + cp from step 7,
# with the home resolution and the restart-on-change done for you.
#
# Idempotent: re-running with no unit changes does daemon-reload/restart
# nothing; a unit whose installed copy differs is replaced, and the service
# is restarted only if it is currently active.
#
# Usage (run as root, e.g. via sudo):
#   sudo ops/install.sh [--home DIR] [--only weixin|wecom] [--with-core]
#                       [--no-enable] [--dry-run]
#   --home DIR        install for this home (default: $SUDO_USER's home,
#                     else the invoking user's home; under sudo $HOME is
#                     /root, which is almost never what you want)
#   --only weixin|wecom
#                     install only one gateway side; the shared units
#                     (native-bridge, approval-relay, channel-restore,
#                     self-update, bridge-token-renew) are always installed
#   --with-core       also install and enable muse-core-run.service
#                     (WeCom core-mode runner; off by default, as in step 7)
#   --no-enable       install the files but do not enable/start anything
#   --dry-run         print what would be done, change nothing
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST=/etc/systemd/system

HOME_DIR=""
ONLY=""
WITH_CORE=0
NO_ENABLE=0
DRY_RUN=0

usage() {
    sed -n '2,22p' "${BASH_SOURCE[0]}"
    exit "${1:-0}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --home) HOME_DIR="$2"; shift 2 ;;
        --home=*) HOME_DIR="${1#--home=}"; shift ;;
        --only) ONLY="$2"; shift 2 ;;
        --only=*) ONLY="${1#--only=}"; shift ;;
        --with-core) WITH_CORE=1; shift ;;
        --no-enable) NO_ENABLE=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage 0 ;;
        *) echo "unknown option: $1" >&2; usage 1 ;;
    esac
done

case "$ONLY" in
    ""|weixin|wecom) ;;
    *) echo "--only must be weixin or wecom" >&2; exit 1 ;;
esac

# The units hard-code /home/hatch; resolve the real target home. Under sudo
# $HOME is /root, so prefer the sudoing user's passwd entry.
if [ -z "$HOME_DIR" ]; then
    if [ -n "${SUDO_USER:-}" ]; then
        HOME_DIR="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    else
        HOME_DIR="$(getent passwd "$(id -un)" | cut -d: -f6)"
    fi
fi
if [ -z "$HOME_DIR" ] || [ ! -d "$HOME_DIR" ]; then
    echo "cannot resolve a target home directory (use --home DIR)" >&2
    exit 1
fi
case "$HOME_DIR" in
    *'|'*|*$'\n'*) echo "home directory contains a character sed cannot use as a delimiter: $HOME_DIR" >&2; exit 1 ;;
esac

if [ "$(id -u)" -ne 0 ] && [ "$DRY_RUN" -eq 0 ]; then
    echo "run as root (sudo) to install units into $DEST" >&2
    exit 1
fi
echo "target home: $HOME_DIR (repo: $ROOT)"

# unit file (relative to repo root) -> enable it?
declare -a UNITS=()
add_unit() { UNITS+=("$1"); }
if [ "$ONLY" != "wecom" ]; then add_unit "weixin-bot/weixin-bot.service"; fi
if [ "$ONLY" != "weixin" ]; then add_unit "wecom-bot/wecom-bot.service"; fi
add_unit "native-bridge/native-bridge.service"
add_unit "approval-relay/approval-relay.service"
add_unit "channel-restore/channel-restore.service"
add_unit "channel-restore/channel-restore.timer"
add_unit "self-update/self-update.service"
add_unit "self-update/self-update.timer"
add_unit "bridge-token-renew/bridge-token-renew.service"
add_unit "bridge-token-renew/bridge-token-renew.timer"
if [ "$WITH_CORE" -eq 1 ]; then add_unit "channel-restore/muse-core-run.service"; fi

# Units with an [Install] section get enabled; the .service files without
# one (channel-restore, self-update, bridge-token-renew) are driven by
# their .timer.
enable_name() {
    local base
    base="$(basename "$1")"
    case "$base" in
        channel-restore.service|self-update.service|bridge-token-renew.service) return 1 ;;
        *) return 0 ;;
    esac
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
CHANGED=()
INSTALLED=()

for rel in "${UNITS[@]}"; do
    src="$ROOT/$rel"
    [ -f "$src" ] || { echo "missing unit file: $src" >&2; exit 1; }
    rendered="$TMP/$(basename "$rel")"
    sed "s|/home/hatch|$HOME_DIR|g" "$src" > "$rendered"
    # When the target home is /home/hatch itself there is nothing to
    # substitute, so every path legitimately still contains it.
    if [ "$HOME_DIR" != "/home/hatch" ] && grep -q "/home/hatch" "$rendered"; then
        echo "rendered $rel still contains /home/hatch; refusing" >&2
        exit 1
    fi
    dest="$DEST/$(basename "$rel")"
    if [ "$DRY_RUN" -eq 1 ]; then
        if [ -f "$dest" ] && cmp -s "$rendered" "$dest"; then
            echo "dry-run: $rel already installed unchanged"
        else
            echo "dry-run: install $rel -> $dest"
        fi
        continue
    fi
    if [ -f "$dest" ] && cmp -s "$rendered" "$dest"; then
        INSTALLED+=("$(basename "$rel") (unchanged)")
        continue
    fi
    install -m 0644 "$rendered" "$dest"
    INSTALLED+=("$(basename "$rel")")
    CHANGED+=("$(basename "$rel")")
done

if [ "$DRY_RUN" -eq 1 ]; then
    echo "dry-run: nothing written; add --no-enable to skip enabling, or drop --dry-run to install"
    exit 0
fi

if [ "${#CHANGED[@]}" -gt 0 ]; then
    systemctl daemon-reload
    echo "reloaded systemd (${CHANGED[*]} changed)"
    for unit in "${CHANGED[@]}"; do
        if systemctl is-active -q "$unit" 2>/dev/null; then
            systemctl restart "$unit"
            echo "restarted $unit (unit file changed)"
        fi
    done
fi

if [ "$NO_ENABLE" -eq 0 ]; then
    TO_ENABLE=()
    for rel in "${UNITS[@]}"; do
        if enable_name "$rel"; then TO_ENABLE+=("$(basename "$rel")"); fi
    done
    # shellcheck disable=SC2086
    systemctl enable --now ${TO_ENABLE[@]}
    echo "enabled: ${TO_ENABLE[*]}"
fi

echo "installed: ${INSTALLED[*]}"
echo "home: $HOME_DIR"
