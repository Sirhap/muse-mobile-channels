# Cloudflare Tunnel Plugin

Publishes a local static site at a fixed public hostname through a
Cloudflare named tunnel, from a host whose outbound traffic is forced
through an HTTP egress proxy that hijacks direct TCP.

## How it works

1. `intro-site.service` serves `SITE_DIR` on `127.0.0.1:LOCAL_PORT`.
2. `cloudflared-relay.service` runs `relay.py`: loopback listeners
   17844/17845 are wired to Cloudflare edge region1/region2. For every
   connection it opens a proxy CONNECT to `region:7844` and splices
   bytes, so cloudflared's TLS to the edge passes end to end. The
   legacy 7844 listener maps destination IPs by re-resolving the
   region hostnames on demand — there is no hardcoded IP table (the
   edge fake IPs change with every environment generation; a stale
   table broke this stack twice).
3. `cloudflared-tunnel.service` runs cloudflared with
   `--edge 127.0.0.1:17844 --edge 127.0.0.1:17845`, so its edge
   connections land on the relay.
4. Optional `pinggy-tunnel.service` is a backup ingress with a
   rotating URL (free anonymous sessions last ~60 minutes).

## Install / heal

First time on a new machine: `cp plugin.example.conf plugin.conf`
and fill in your tunnel name, hostname and site dir (`plugin.conf`
is machine-local and not tracked in git). Then:

`./install.sh` — idempotent. Renders units from `plugin.conf` into
/etc/systemd/system and starts the stack. A VM replacement wipes the
units; re-running install.sh (the heartbeat check does this) restores
everything, because all assets live in this directory plus
~/.cloudflared and ~/.config/cloudflared-relay.env.

`./health.sh` — layered check: units active, local site up, tunnel
connection registered recently, and the public hostname serving the
exact bytes of `SITE_DIR/index.html`.

`./uninstall.sh` — stops and removes the units; credentials stay.

## First-time setup (once per Cloudflare account)

`cloudflared tunnel login`, `cloudflared tunnel create <name>`,
`cloudflared tunnel route dns <name> <hostname>`, then write
`~/.cloudflared/config-<name>.yml` (see config.example.yml) and put
the egress proxy URL in `~/.config/cloudflared-relay.env` (mode 600,
keys HTTPS_PROXY/https_proxy). The installer never creates or prints
credentials.

## Known behaviour

The egress proxy is slow to establish CONNECT to port 7844; after a
restart the tunnel may need a few minutes of automatic retries before
it registers. That is expected, not a failure.
