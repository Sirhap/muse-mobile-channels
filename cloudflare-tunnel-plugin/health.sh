#!/usr/bin/env bash
# Health check for the Cloudflare tunnel plugin.
# PASS/FAIL lines on stdout; exit 0 only when every layer is healthy.
set -uo pipefail

PKG="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
if [ ! -f "$PKG/plugin.conf" ]; then
  echo "plugin.conf missing: cp plugin.example.conf plugin.conf and fill in your values first" >&2
  exit 1
fi
source "$PKG/plugin.conf"

fail=0
say() { echo "$1: $2"; [ "$1" = "FAIL" ] && fail=1; }

UNITS="intro-site.service cloudflared-relay.service cloudflared-tunnel.service"
[ "${ENABLE_PINGGY:-0}" = "1" ] && UNITS="$UNITS pinggy-tunnel.service"
for unit in $UNITS; do
  if systemctl is-active --quiet "$unit"; then
    say PASS "$unit active"
  else
    say FAIL "$unit not active"
  fi
done

if curl -s -o /dev/null --max-time 5 "http://127.0.0.1:$LOCAL_PORT/"; then
  say PASS "local site responds on $LOCAL_PORT"
else
  say FAIL "local site not responding on $LOCAL_PORT"
fi

# No pipe into grep -q here: under `set -o pipefail`, grep -q closing
# the pipe early SIGPIPEs journalctl and the check false-fails.
tunnel_logs="$(journalctl -u cloudflared-tunnel.service --since "-15min" --no-pager 2>/dev/null || true)"
case "$tunnel_logs" in
  *"Registered tunnel connection"*)
    say PASS "tunnel connection registered in the last 15 min" ;;
  *)
    say FAIL "no registered tunnel connection in the last 15 min (may still be retrying through the slow proxy)" ;;
esac

want="$(sha256sum "$SITE_DIR/index.html" 2>/dev/null | cut -d' ' -f1)"
got=""
code=""
# This host's own egress path to the public name is flaky (documented
# truncated bodies), so pass if any try returns 200 with exact bytes.
for _ in 1 2 3 4 5 6; do
  tmp="$(mktemp)"
  code="$(curl -s -o "$tmp" -w "%{http_code}" --max-time 15 "https://$HOSTNAME/" || echo 000)"
  if [ -s "$tmp" ]; then
    got="$(sha256sum "$tmp" | cut -d' ' -f1)"
    rm -f "$tmp"
    [ "$code" = "200" ] && [ "$got" = "$want" ] && break
  else
    rm -f "$tmp"
  fi
  sleep 4
done
if [ -n "$want" ] && [ "$got" = "$want" ] && [ "$code" = "200" ]; then
  say PASS "public https://$HOSTNAME/ serves the exact site bytes"
else
  say FAIL "public https://$HOSTNAME/ did not return the site bytes (http $code, want sha ${want:0:12}, got ${got:0:12})"
fi

exit $fail
