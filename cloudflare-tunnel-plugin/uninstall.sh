#!/usr/bin/env bash
# Stops and removes the plugin's systemd units. Credentials, the
# cloudflared config and this package are left in place.
set -uo pipefail

for unit in cloudflared-tunnel.service cloudflared-relay.service intro-site.service pinggy-tunnel.service; do
  systemctl disable --now "$unit" 2>/dev/null || true
  rm -f "/etc/systemd/system/$unit"
  echo "removed $unit"
done
systemctl daemon-reload
echo "credentials and package files were kept."
