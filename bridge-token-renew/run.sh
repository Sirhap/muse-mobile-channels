#!/usr/bin/env bash
# Daily bridge renewal: renew token.json + cookie rotation, then verify.
#
# Mirrors the bridge-token-daily-renew scheduled task: run renew_token.py,
# and on success verify all three legs with check_credentials.py.
# renew_token.py exit codes: 0 renewed, 1 auth/network failure or failed
# verification (files restored), 2 skipped because the disk files are
# unused (NATIVE_TOKEN_JSON set, or no hatch_sess in cookies.txt). A skip
# is not a failure: it exits 0 here so the timer stays green.
# A failure means the cookie is probably dead; that is the signal to
# re-export muse-credentials.zip, not something to retry harder.
# Nothing in this script prints credential values.
set -u

PY="$HOME/muse-test-venv/bin/python"
BRIDGE_DIR="$HOME/workspace/native-bridge"

[ -x "$PY" ] || { echo "bridge-token-renew: python not found: $PY" >&2; exit 1; }
[ -f "$BRIDGE_DIR/renew_token.py" ] || { echo "bridge-token-renew: missing $BRIDGE_DIR/renew_token.py" >&2; exit 1; }
[ -f "$BRIDGE_DIR/check_credentials.py" ] || { echo "bridge-token-renew: missing $BRIDGE_DIR/check_credentials.py" >&2; exit 1; }

"$PY" "$BRIDGE_DIR/renew_token.py"
rc=$?
if [ "$rc" -eq 2 ]; then
    echo "bridge-token-renew: skipped (renew_token.py exit 2: disk files unused)"
    exit 0
fi
if [ "$rc" -ne 0 ]; then
    echo "bridge-token-renew: renewal failed (exit $rc); credentials untouched or restored, re-export needed if the cookie is dead" >&2
    exit 1
fi
"$PY" "$BRIDGE_DIR/check_credentials.py" --json
