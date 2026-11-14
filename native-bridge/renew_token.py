#!/usr/bin/env python3
"""Daily renewal of the bridge token leg AND the cookie leg.

Proven 2026-11-14 by live probe (header names + expiry only, no values):
every POST /api/auth/check returns Set-Cookie headers that DELETE the old
session cookie (max-age=0) and SET a fresh one with a different value and
max-age=2592000 (30 days), plus the same delete+set pair for the gateway
cookie (max-age=31536000). A browser applies those headers automatically on
every call, so its cookie never ages past 30 days from last use. Our exported
cookies.txt was frozen at export time because nothing ever wrote the rotation
back -- that is the whole 30-day-vs-7-day gap. The fix is to do what the
browser does: capture Set-Cookie from auth/check and write it back.

Each run:
1. POST /api/auth/check with the full Cookie string (no Bearer) ->
   access_token, plus the Set-Cookie rotation.
2. Immediately save any rotated cookies into cookies.txt (Netscape jar,
   atomic replace, 0600), verify with check_credentials --source cookies,
   and restore the backup on verification failure.
3. POST /api/hatch/token with Bearer + Cookie -> hatch_token.
4. Atomically rewrite token.json (0600) with the new tokens and fresh
   issued_at.

Safety rules:
- Never prints credential values; only step names, cookie names, HTTP
  statuses and timestamps.
- A cookie 401 leaves both files untouched (a dead cookie means re-export,
  not a blank file). A failed cookie write is rolled back from the backup.
- If NATIVE_TOKEN_JSON is set, the bridge ignores the disk files, so
  renewing them is useless: warn and exit 2 instead of writing.
- Run once a day from cron while the cookie is alive. After the cookie dies
  this script fails every run until a fresh export is installed; that failure
  is the signal to re-export, not something to retry harder.

Usage: ~/muse-test-venv/bin/python ~/workspace/native-bridge/renew_token.py
Exit codes: 0 renewed, 1 auth/network failure or failed verification
(files restored), 2 skipped (environment override makes disk files unused).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from curl_cffi import requests as rq

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36")

CRED_DIR = Path(os.environ.get(
    "NATIVE_BRIDGE_CREDENTIALS", Path.home() / ".config" / "native-bridge"))
CHECK_SCRIPT = Path(__file__).with_name("check_credentials.py")


class RenewError(RuntimeError):
    """Renewal failed; the on-disk files were not touched."""


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


def fetch_access_token(cookies: str) -> tuple[str, list[str]]:
    """Return (access_token, raw Set-Cookie header values)."""
    resp = rq.post("https://muse.ai/api/auth/check",
                   headers=_hatch_headers(cookies),
                   data=b"", impersonate="chrome", timeout=20)
    if resp.status_code != 200:
        raise RenewError(f"auth/check -> HTTP {resp.status_code}")
    try:
        raw = resp.headers.get_list("set-cookie")
    except Exception:
        raw = [resp.headers.get("set-cookie", "")]
    return resp.json()["access_token"], [h for h in raw if h]


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


def parse_rotation(raw_headers: list[str]) -> dict[str, tuple[str, int | None]]:
    """Apply Set-Cookie headers in order; return {name: (value, max_age)}.

    A max-age=0 entry deletes the old value; a later set for the same name
    wins, exactly like a browser jar. Entries without a usable value are
    ignored. Only names and expiries leave this function, never values.
    """
    rotated: dict[str, tuple[str, int | None]] = {}
    deleted: set[str] = set()
    for raw in raw_headers:
        parts = [p.strip() for p in raw.split(";")]
        if not parts or "=" not in parts[0]:
            continue
        name, value = parts[0].split("=", 1)
        name = name.strip()
        max_age: int | None = None
        for attr in parts[1:]:
            if "=" in attr:
                key, val = attr.split("=", 1)
                if key.strip().lower() == "max-age":
                    try:
                        max_age = int(val.strip())
                    except ValueError:
                        max_age = None
        if max_age == 0:
            deleted.add(name)
            rotated.pop(name, None)
        elif value:
            rotated[name] = (value, max_age)
            deleted.discard(name)
    return rotated


def apply_cookie_rotation(rotated: dict[str, tuple[str, int | None]],
                          now: float) -> list[str]:
    """Write rotated values back into the Netscape jar, atomically.

    Only names already in the jar are updated; anything else in the response
    is ignored. Returns the rotated cookie names (no values). The previous
    jar is kept at cookies.txt.bak (0600) for rollback.
    """
    jar_path = CRED_DIR / "cookies.txt"
    lines = jar_path.read_text().splitlines()
    changed: list[str] = []
    out: list[str] = []
    for line in lines:
        if not line or line.startswith("#"):
            out.append(line)
            continue
        cols = line.split("\t")
        if len(cols) >= 7 and cols[5] in rotated:
            value, max_age = rotated[cols[5]]
            cols[6] = value
            if max_age is not None:
                cols[4] = str(int(now) + max_age)
            changed.append(cols[5])
        out.append("\t".join(cols))
    if not changed:
        return []
    backup = CRED_DIR / "cookies.txt.bak"
    backup.write_bytes(jar_path.read_bytes())
    os.chmod(backup, 0o600)
    fd, tmp = tempfile.mkstemp(dir=CRED_DIR, prefix=".cookies.txt.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write("\n".join(out) + "\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, jar_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return changed


def verify_cookies() -> bool:
    """Probe the cookie leg exactly like the bridge does; True if healthy."""
    try:
        proc = subprocess.run(
            [sys.executable, str(CHECK_SCRIPT), "--source", "cookies",
             "--json"],
            capture_output=True, text=True, timeout=60)
        data = json.loads(proc.stdout)
        return bool(data.get("checks")) and all(
            c.get("status") == "healthy" for c in data["checks"])
    except Exception:
        return False


def write_token_file(payload: dict) -> None:
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


def main() -> int:
    if os.environ.get("NATIVE_TOKEN_JSON"):
        print("renew_token: skipped, NATIVE_TOKEN_JSON overrides the disk files")
        return 2
    cookies = load_cookie_header()
    vm_id = (CRED_DIR / "vm_id.txt").read_text().strip()
    if "hatch_sess=" not in cookies:
        print("renew_token: skipped, cookies.txt has no hatch_sess")
        return 2
    now_utc = datetime.now(timezone.utc)
    try:
        access_token, raw_set_cookie = fetch_access_token(cookies)
    except RenewError as exc:
        print(f"renew_token: FAILED {exc} (files untouched, re-export needed"
              " if the cookie is dead)")
        return 1
    except Exception as exc:  # network etc.: never blame the credentials
        print(f"renew_token: FAILED network/error {type(exc).__name__}"
              " (files untouched)")
        return 1
    # Save the rotation first: the server already issued fresh cookies, and
    # losing them would strand us on the old value.
    rotated = parse_rotation(raw_set_cookie)
    changed: list[str] = []
    if rotated:
        try:
            changed = apply_cookie_rotation(rotated, time.time())
        except Exception as exc:
            print(f"renew_token: FAILED cookie write {type(exc).__name__}"
                  " (token.json untouched)")
            return 1
        if changed and not verify_cookies():
            backup = CRED_DIR / "cookies.txt.bak"
            jar_path = CRED_DIR / "cookies.txt"
            jar_path.write_bytes(backup.read_bytes())
            os.chmod(jar_path, 0o600)
            print("renew_token: FAILED cookie verification, restored backup"
                  " (token.json untouched)")
            return 1
    try:
        hatch_token = fetch_hatch_token(cookies, access_token, vm_id)
    except RenewError as exc:
        print(f"renew_token: FAILED {exc} (cookies already rotated and saved,"
              " token.json untouched)")
        return 1
    except Exception as exc:
        print(f"renew_token: FAILED network/error {type(exc).__name__}"
              " (cookies already rotated and saved, token.json untouched)")
        return 1
    payload = {
        "vm_id": vm_id,
        "access_token": access_token,
        "hatch_token": hatch_token,
        "issued_at": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "issued_at_local": now_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S %z"),
        "exp": None,
    }
    write_token_file(payload)
    print(f"renew_token: renewed token.json issued_at={payload['issued_at']}"
          + (f" cookies_rotated={','.join(changed)}" if changed
             else " cookies_unchanged"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
