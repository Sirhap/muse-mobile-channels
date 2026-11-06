# Gadget token extension

Minimal code to **get and keep a Muse Gadget device token**, extracted from
[wong2/muse-client](https://github.com/wong2/muse-client) (`reference/muse-client`,
vendored in this repo, Apache-2.0). Only the token flow is kept; chat, Noise
transport, VM lookup and the Bluetooth pairing protocol are intentionally
**not** here. Run it with `bun` (the project's Gadget runtime).

## The token flow in 30 seconds

```
browser: https://gadgets.muse.ai/settings/sdk-tokens   -> SDK token (mgst_...)
   |  (one token per device; associated with the device WHEN IT PAIRS)
   v
pair (macOS + Bluetooth, one time)         -> pairing.json + identity.json
   |  access_token + refresh_token, device identity (MAC)
   v
refresh (anywhere, pure HTTPS)             -> fresh access_token + refresh_token
   POST /device_token/refresh, Bearer hatch_refresh:<refresh_token>
```

- **Get an SDK token**: open `https://gadgets.muse.ai/settings/sdk-tokens`
  in a browser and copy the token. One token per device.
- **Pair (one time, macOS only)**: the SDK token becomes device credentials
  (`pairing.json` + `identity.json`) through Bluetooth pairing. This
  extension does **not** reimplement Bluetooth pairing; see `pair` below.
- **Refresh (ongoing, anywhere)**: the stored `refresh_token` rotates the
  `access_token` over HTTPS. This is what keeps an already-paired device
  working, and it runs on Linux.

## What is in here

| File | From wong2/muse-client | What it does |
|---|---|---|
| `src/errors.ts` | `src/errors.ts` (verbatim) | `MuseError`, `MuseHttpError`, `MuseProtocolError` |
| `src/account.ts` | `src/api.ts`, token part only | `MuseAccount.refresh()` (POST `/device_token/refresh`); `listVMs` deliberately dropped |
| `src/credentials.ts` | `src/credentials.ts` | `loadCredentials` / `saveCredentials` / `unpair` / `defaultCredentialsDirectory` (same file formats) |
| `src/sdk-token.ts` | `src/pairing.ts` (`validateSDKToken` only) | SDK-token format check + the token URL |
| `src/index.ts` | — | Re-exports the public surface |
| `cli.ts` | — | `refresh` / `status` / `pair` / `unpair` |

Dropped from the source on purpose: `listVMs` / `MuseVM`, chat (`client.ts`,
`turn.ts`, `turn-stream.ts`, `message.ts`, `events.ts`), Noise transport
(`connection.ts`, `noise/`), and the Bluetooth pairing protocol
(`pairing/controller.ts`, `pairing/protocol.ts`, `native/macos/`). If you need
those, use the full `reference/muse-client`.

Notes kept from the project: the REST timeout is 60s, not the upstream 15s,
because the egress proxy here is slow (a 15s timeout killed a live Gadget
turn on 2026-11-06). Token values are never logged; `status` shows metadata
only.

## Usage

```sh
# Where the credentials live. This project keeps them at ~/.config/MuseGadgetPair/
# (0600 files, 0700 directory). Pass --credentials if yours are elsewhere.
CRED=~/.config/MuseGadgetPair

# Show token metadata (never prints a token value)
bun extensions/gadget-token/cli.ts status --credentials $CRED

# Rotate the access token with the stored refresh token (saves back to pairing.json)
bun extensions/gadget-token/cli.ts refresh --credentials $CRED

# Remove pairing.json (keeps identity.json + sdk_token; does not revoke the device)
bun extensions/gadget-token/cli.ts unpair --credentials $CRED
```

### Pairing a new device

Pairing needs **macOS + Bluetooth** and cannot run on this Linux machine.

1. Open `https://gadgets.muse.ai/settings/sdk-tokens` in a browser, copy the SDK token.
2. On a Mac, pair with the full client (vendored at `reference/muse-client`):
   ```sh
   cd reference/muse-client && bun run cli pair
   ```
   Paste the SDK token when prompted, allow Bluetooth access, open
   Muse > Settings > Devices > Add Device (Developer mode enabled), and wait
   for `Paired successfully`.
3. Copy the resulting files to this machine:
   `pairing.json`, `identity.json`, `sdk_token` → the credentials directory
   (e.g. `~/.config/MuseGadgetPair/`). Keep them 0600, directory 0700.

`bun extensions/gadget-token/cli.ts pair` validates the SDK-token format
first, then prints these same steps (it does not pair by itself). Changing
the SDK token, or revoking the token used for pairing, requires unpairing
and pairing again; refreshing does not change the SDK token bound to the
device.

## Use it from code

```ts
import { homedir } from 'node:os';
import { join } from 'node:path';
import { MuseAccount, loadCredentials, saveCredentials } from './src/index.js';

// Node does not expand `~` in a string literal; build the path from homedir.
const dir = process.env.MUSE_GADGET_CREDENTIALS
  ?? join(homedir(), '.config', 'MuseGadgetPair');
const account = new MuseAccount({
  credentials: await loadCredentials(dir),
  onCredentials: (next) => saveCredentials(next, dir),
});
const rotated = await account.refresh(); // fresh access_token + refresh_token, saved
```

## Security

- Never pass a token on a command line (it lands in shell history / process
  lists). The CLI reads an SDK token from a file (`--sdk-token-file`) or
  stdin only.
- Credential files are 0600, directory 0700. Do not commit them, paste them
  into chat, or copy them into the repo.
