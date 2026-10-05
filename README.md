# Muse mobile channels (WeChat + WeCom)

Dual mobile channels for Muse, running in parallel on one VM:

- `weixin-bot/` — personal WeChat gateway via Tencent's iLink bot protocol
  (`gateway.py`), plus its CLI (`weixin_cli.py`, wrapper `weixin`) and QR
  login helper (`login.py`). systemd unit: `weixin-bot.service`.
- `wecom-bot/` — WeCom (企业微信) smart-robot gateway over the official
  long-connection WebSocket (`gateway.py`), plus its CLI (`wecom_cli.py`,
  wrapper `wecom`). systemd unit: `wecom-bot.service`.
- `channel-restore/` — self-heal for both gateways: `restore.sh` reinstalls /
  restarts the systemd units if they vanish (e.g. after a VM replacement);
  `channel-restore.timer` runs it every 5 minutes.
- `slash-commands-2026-10-04.md` — the in-chat slash command table
  (/ping /status /queue /jump /stop /new /help /subagent) implemented by the
  gateways.

The inbox hooks that wake the agent per message live outside this repo in
`~/hooks/` (scripts + definitions), managed by the Muse runtime.

## Secrets

No credentials are in this repo. Each gateway reads its credentials from an
env file outside the tree:

- Weixin: `~/.config/weixin-bot/credentials.env` (`ILINK_CRED_FILE`)
- WeCom: `~/.config/wecom-bot/credentials.env` (`WECOM_CRED_FILE`)

Runtime state (`state/`), virtualenvs (`.venv/`), and backups (`*.bak-*`)
are gitignored.

## Run

```sh
cd weixin-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cd ../wecom-bot && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
# then install the systemd units from each bot dir / channel-restore/
```

Syntax-verified 2026-10-04: all Python files compile, shell scripts pass
`bash -n`, hook JSON parses.
