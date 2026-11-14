# Changelog

All notable changes to this project. Versions are date-based (`YYYY.MM.DD.N`,
see `VERSION`); a release is cut by pushing tag `v<version>`, which triggers
the GitHub Actions release workflow (verify version, run tests, build
sdist/wheel + deployment tarball, create the GitHub Release). The scheduled
updater (`ops/self_update.py`, daily 03:30) only compares versions and pulls
a new release when the version differs.

## 2026.11.14.4 — 2026-11-14

- Added `ops/install.sh`: one-command install of the systemd units, replacing
  the manual `sed` + `cp` step 7 in the README (same units, same enable list;
  `--dry-run` previews, `--only weixin|wecom` installs one channel,
  `--with-core` also installs `muse-core-run.service`). Verified: dry-run
  reports all installed units unchanged on this machine, rendered units for
  another home contain no `/home/hatch`, `systemd-analyze verify` clean.
- Added this changelog; releases previously shipped with auto-generated notes
  only.
- Synced the six repo `.service` files to the installed copies (quoting-only
  drift: installed `Environment`/`ExecStart` values are quoted,
  `WorkingDirectory` stays bare per the 08dda0d rule). No functional change;
  it makes `ops/install.sh` correctly report "already installed unchanged".

## 2026.11.14.3 — 2026-11-14

- Fixed: the updater's backup step crashed (`shutil.copytree` raises on Unix
  sockets) when the tree held stale socket files, aborting the whole update
  before anything was applied. `backup_install` now skips non-regular files
  (sockets, fifos, device nodes) as well as caches/state
  (`ops/self_update.py`, commit `5228357`).
- Added a regression test that binds a real Unix socket in the fake install
  and asserts the update completes and the backup omits it
  (`tests/test_self_update.py`, now 19 tests).

## 2026.11.14.2 — 2026-11-14

- Comment-only: documented why retrying a Gadget send after HTTP 404 cannot
  double-deliver — the 404 proves the first send never landed
  (`mobile_channels/executors.py`, commit `835398d`).

## 2026.11.14.1 — 2026-11-14

First versioned release.

- Versioning: `VERSION` is the single source of truth (`YYYY.MM.DD.N`),
  `pyproject.toml` reads it dynamically, `ops/bump_version.py` sets it
  before a release; pushing tag `v<version>` verifies the version, runs the
  tests, builds sdist/wheel + deployment tarball, and creates the GitHub
  Release (commits `676236f`, `6326a06`, `920b000`).
- Self-update: `ops/self_update.py` + `self-update.service`/`.timer` check
  the latest release daily at 03:30 and, only on a version change, download,
  verify, back up, unpack, and restart the five gateway services; never
  touches credentials, config, or state.
- Bridge token renewal: `native-bridge/renew_token.py` renews the token daily
  while the cookie is alive and writes rotated session tickets back into the
  cookie jar (commits `4ac25b3`, `5b3418b`).
- WeCom: Gadget send confirmed undelivered (HTTP 404, no message id) now
  falls back to Native, and retries once on a fresh Gadget session in the
  same turn (commits `647d5db`, `e7e6a2b`).
