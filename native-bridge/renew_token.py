#!/usr/bin/env python3
"""Daily self-renewal of the bridge token leg (token.json) while cookies are alive.

The cookie leg cannot be renewed from here: /api/auth/check and /api/hatch/token
only mint tokens, they never return a new cookie. What this script does is keep
the token leg fresh so it stops dying ~24h after every export:

1. POST /api/auth/check with the full Cookie string (no Bearer) -> access_token.
2. POST /api/hatch/token with Bearer + Cookie -> hatch_token.
3. Atomically rewrite token.json (0600) with the new tokens and fresh issued_at.

Safety rules:
- Never overwrites token.json unless BOTH calls succeed. A cookie 401 leaves the
  old file untouched (a dead cookie means re-export, not a blank token file).
- Never prints credential values; only step names, HTTP statuses and timestamps.
- If NATIVE_TOKEN_JSON is set, the bridge ignores the disk file, so renewing it
  is useless: warn and exit 2 instead of writing.
- Run once a day from cron while the cookie is alive. After the cookie dies this
  script fails every run until a fresh export is installed; that failure is the
  signal to re-export, not something to retry harder.

Usage: ~/muse-test-venv/bin/python ~/workspace/native-bridge/renew_token.py
Exit codes: 0 renewed, 1 auth/network failure (file untouched), 2 skipped
(environment override makes the disk file unused).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests as rq

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36")

CRED_DIR = Path(os.environ.get(
    "NATIVE_BRIDGE_CREDENTIALS", Path.home() / ".config" / "native-bridge"))


class RenewError(RuntimeError):
    """Renewal failed; the on-disk token file was not touched."""


def _hatch_headers(cookies: str, access_token: str | None = None) -> dict:
    headers = {
        "User-Agent": UA,
        "Referer": "https://muse.ai/",
        "Origin": "https://muse.ai",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": cookies,
    }
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def fetch_access_token(cookies: str) -> str:
    resp = rq.post("https://muse.ai/api/auth/check",
                   headers=_hatch_headers(cookies),
                   data=b"", impersonate="chrome", timeout=20)
    if resp.status_code != 200:
        raise RenewError(f"auth/check -> HTTP {resp.status_code}")
    return resp.json()["access_token"]


def fetch_hatch_token(cookies: str, access_token: str, vm_id: str) -> str:
    resp = rq.post(
        "https://muse.ai/api/hatch/token",
        headers=_hatch_headers(cookies, access_token),
        json={"vmAddress": f"wss://{vm_id}.metaaivm.com/", "vmName": vm_id},
        impersonate="chrome", timeout=20)
    if resp.status_code != 200:
        raise RenewError(f"hatch/token -> HTTP {resp.status_code}")
    return resp.json()["token"]


def load_cookie_header() -> str:
    """Convert cookies.txt to a 'name=value; ...' header, like the bridge does.

    The file is Netscape jar format (tab-separated, with expiry columns); the
    raw file must never be sent as the Cookie header, the server rejects it.
    This reuses the reference client's parser, the same one native_bridge.py
    uses in cookie_params().
    """
    src = os.environ.get(
        "MUSE_CLI_SRC",
        str(Path(__file__).resolve().parents[1] / "reference" / "muse-cli" / "src"))
    if src not in sys.path:
        sys.path.insert(0, src)
    from muse_cli.gateway import load_cookies
    return load_cookies(str(CRED_DIR / "cookies.txt"))


def main() -> int:
    if os.environ.get("NATIVE_TOKEN_JSON"):
        print("renew_token: skipped, NATIVE_TOKEN_JSON overrides the disk token file")
        return 2
    cookies = load_cookie_header()
    vm_id = (CRED_DIR / "vm_id.txt").read_text().strip()
    if "hatch_sess=" not in cookies:
        print("renew_token: skipped, cookies.txt has no hatch_sess")
        return 2
    now_utc = datetime.now(timezone.utc)
    try:
        access_token = fetch_access_token(cookies)
        hatch_token = fetch_hatch_token(cookies, access_token, vm_id)
    except RenewError as exc:
        print(f"renew_token: FAILED {exc} (token.json untouched, re-export needed"
              " if the cookie is dead)")
        return 1
    except Exception as exc:  # network etc.: never blame the credentials
        print(f"renew_token: FAILED network/error {type(exc).__name__}"
              " (token.json untouched)")
        return 1
    payload = {
        "vm_id": vm_id,
        "access_token": access_token,
        "hatch_token": hatch_token,
        "issued_at": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "issued_at_local": now_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S %z"),
        "exp": None,
    }
    token_path = CRED_DIR / "token.json"
    fd, tmp = tempfile.mkstemp(dir=CRED_DIR, prefix=".token.json.")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, token_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    print(f"renew_token: renewed token.json issued_at={payload['issued_at']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
