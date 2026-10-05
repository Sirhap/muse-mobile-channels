#!/usr/bin/env python3
"""One-shot iLink QR login for the Weixin gateway.

Fetches a bot QR code, renders it to login-qr.png, polls its status until the
user confirms in WeChat, then writes the bot token into the credentials file
(mode 600). Run it again whenever the gateway reports needs_relogin.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

import httpx

try:
    import qrcode as qr_lib
except ImportError:
    qr_lib = None

BASE = Path(__file__).resolve().parent
QR_PNG = BASE / "login-qr.png"
LOGIN_STATE = BASE / "state" / "login.json"
CRED_FILE = Path(
    os.environ.get(
        "ILINK_CRED_FILE", str(Path.home() / ".config" / "weixin-bot" / "credentials.env")
    )
)
BASE_URL = "https://ilinkai.weixin.qq.com"
BOT_TYPE = "3"


def note(msg: str) -> None:
    print(f"[weixin-login {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def save_login_state(obj: dict) -> None:
    """Write login state mode 0600. The qrcode session string can mint a token."""
    LOGIN_STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = LOGIN_STATE.with_name(f"{LOGIN_STATE.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(LOGIN_STATE)
    os.chmod(LOGIN_STATE, 0o600)


def write_credentials(token: str, bot_id: str, user_id: str) -> None:
    CRED_FILE.parent.mkdir(parents=True, exist_ok=True)
    lines = {}
    if CRED_FILE.exists():
        for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                lines[k.strip()] = v.strip()
    lines["ILINK_BOT_TOKEN"] = token
    if bot_id:
        lines["ILINK_BOT_ID"] = bot_id
    if user_id:
        lines["ILINK_USER_ID"] = user_id
    content = "".join(f"{k}={v}\n" for k, v in lines.items())
    tmp = CRED_FILE.with_name(f"{CRED_FILE.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    tmp.write_text(content, encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(CRED_FILE)
    os.chmod(CRED_FILE, 0o600)


def status_url(qrcode: str, verify_code: str = "") -> str:
    """Status poll URL. verify_code is included only when the user supplied one."""
    query = {"qrcode": qrcode}
    if verify_code:
        query["verify_code"] = verify_code
    return f"{BASE_URL}/ilink/bot/get_qrcode_status?{urlencode(query)}"


async def main(verify_code: str = "") -> int:
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None
    async with httpx.AsyncClient(proxy=proxy, trust_env=False) as client:
        resp = await client.post(
            f"{BASE_URL}/ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}",
            json={"local_token_list": []},
            headers={"Content-Type": "application/json"},
            timeout=httpx.Timeout(20.0, connect=15.0),
        )
        resp.raise_for_status()
        data = resp.json()
        qrcode = data.get("qrcode", "")
        img_content = data.get("qrcode_img_content", "")
        if not qrcode or not img_content:
            note(f"unexpected qrcode response: {json.dumps(data)[:300]}")
            return 1
        save_login_state({"status": "waiting_scan", "qrcode": qrcode, "ts": int(time.time())})
        try:
            if qr_lib is None:
                raise ImportError("qrcode is not installed")
            img = qr_lib.make(img_content)
            img.save(str(QR_PNG))
            os.chmod(QR_PNG, 0o600)
            note(f"QR code rendered to {QR_PNG}")
        except Exception as e:
            note(f"QR render failed ({e}); image was not written")
        note("waiting for scan... (valid about 5 minutes)")

        deadline = time.time() + 5 * 60
        while time.time() < deadline:
            try:
                r = await client.get(status_url(qrcode, verify_code), timeout=httpx.Timeout(40.0, connect=15.0))
                r.raise_for_status()
                st = r.json()
            except httpx.TimeoutException:
                continue
            except Exception as e:
                note(f"status poll error (retrying): {e}")
                await asyncio.sleep(3)
                continue
            status = st.get("status", "")
            if status == "confirmed":
                token = st.get("bot_token", "")
                bot_id = st.get("ilink_bot_id", "")
                user_id = st.get("ilink_user_id", "")
                if not token:
                    note(f"confirmed but no bot_token: {json.dumps(st)[:300]}")
                    return 1
                write_credentials(token, bot_id, user_id)
                save_login_state(
                    {"status": "confirmed", "bot_id": bot_id, "ts": int(time.time())}
                )
                note("LOGIN CONFIRMED; credentials saved")
                return 0
            if status in ("expired", "timeout"):
                save_login_state({"status": status, "ts": int(time.time())})
                note(f"QR code {status}; run login again for a fresh one")
                return 2
            if status == "need_verify_code":
                save_login_state({"status": "need_verify_code", "ts": int(time.time())})
                note("WeChat asks for a verify code: rerun with the code as argv[1]")
                return 3
            if status and status not in ("wait", "scanned"):
                note(f"status={status} raw={json.dumps(st)[:200]}")
            save_login_state(
                {"status": status or "wait", "qrcode": qrcode, "ts": int(time.time())}
            )
        note("login timed out after 5 minutes")
        return 2


if __name__ == "__main__":
    vc = sys.argv[1] if len(sys.argv) > 1 else ""
    if vc:
        print("verify-code flow: submitting the code with the new QR session")
    sys.exit(asyncio.run(main(vc)))
