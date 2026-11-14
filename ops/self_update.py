#!/usr/bin/env python3
"""Scheduled self-updater: check the latest GitHub Release, update if newer.

Compares the latest release version against the local VERSION file; only
when the version differs does it download the release tarball, back up the
current install, unpack it, and restart the gateway services.

Safety (these are load-bearing, do not weaken them):
- Never touches user credentials, config, or state. The release tarball
  holds code only (it is built with ``git archive``); state/ and
  ~/.config are never in the archive and never written by this script.
- Skips, and logs, when the local git tree has uncommitted tracked changes.
- Verifies the tarball before unpacking: every member under one top-level
  directory, VERSION inside matches the expected version, no credential /
  database / key member names, no absolute paths or ``..``.
- ``--dry-run`` and ``--check-only`` never change anything. ``--no-restart``
  skips the service restarts (for tests and manual runs).

Exit codes: 0 = up to date, updated OK, or skipped (dirty tree);
1 = error (network, download, verify, unpack, restart failure).

Usage:
    python3 ops/self_update.py                      # full check + update
    python3 ops/self_update.py --check-only        # just compare versions
    python3 ops/self_update.py --dry-run           # report what would happen
    python3 ops/self_update.py --install-dir /path/to/checkout
    python3 ops/self_update.py --verify-tarball file.tar.gz --expect-version X.Y.Z.N
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_REPO = "Sirhap/muse-mobile-channels"
VERSION_RE = re.compile(r"^(\d{4})\.(\d{2})\.(\d{2})\.(\d+)$")

# Services restarted after a successful update, when installed.
SERVICES = ["muse-core-run", "wecom-bot", "weixin-bot", "approval-relay",
            "native-bridge"]

# Member names that must never appear in a release tarball. Matched against
# the basename of each member, plus a few path patterns.
FORBIDDEN_BASENAMES = {
    "cookies.txt", "cookies.json", "token.json", "vm_id.txt",
    "pairing.json", "identity.json", "sdk_token", "pair.lock",
    "credentials.env", ".env", "decisions.jsonl", "decisions.lock",
    "decisions.offset", "state.json", "status.json",
}
# Private-key filenames have no suffix (id_rsa, id_ed25519, ...); .env
# variants and credential/secret JSON files are forbidden whatever the
# exact name.
FORBIDDEN_NAME_PREFIXES = ("id_rsa", "id_ed25519", "id_ecdsa", "id_dsa",
                           ".env.")
FORBIDDEN_NAME_SUBSTRINGS = ("credentials", "secret")
FORBIDDEN_SUFFIXES = (".sqlite3", ".sqlite3-wal", ".sqlite3-shm", ".db",
                      ".pem", ".key", ".p12", ".pfx", ".der")
FORBIDDEN_PATH_PARTS = {"state", ".git", "__pycache__", ".venv", "venv",
                        "node_modules"}


class UpdateError(RuntimeError):
    """A failed step; files may be untouched (see the message)."""


def parse_version(text: str) -> tuple[int, int, int, int]:
    """Parse 'YYYY.MM.DD.N' into a comparable tuple; raise ValueError."""
    match = VERSION_RE.match(text.strip())
    if not match:
        raise ValueError(f"bad version {text!r}: expected YYYY.MM.DD.N")
    year, month, day, seq = (int(part) for part in match.groups())
    datetime.date(year, month, day)  # validates the calendar date
    if seq < 1:
        raise ValueError(f"bad version {text!r}: sequence must be >= 1")
    return year, month, day, seq


def format_version(version: tuple[int, int, int, int]) -> str:
    year, month, day, seq = version
    return f"{year:04d}.{month:02d}.{day:02d}.{seq}"


def tag_to_version(tag: str) -> tuple[int, int, int, int]:
    """'v2026.11.14.1' -> (2026, 11, 14, 1)."""
    return parse_version(tag[1:] if tag.startswith("v") else tag)


def log(message: str, log_file: Path | None = None) -> None:
    stamp = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] self-update: {message}"
    print(line, flush=True)
    if log_file is not None:
        try:
            with log_file.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError:
            pass


def read_local_version(install_dir: Path) -> str:
    version_file = install_dir / "VERSION"
    try:
        return version_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise UpdateError(f"cannot read local VERSION at {version_file}: {exc}")


def is_tree_dirty(install_dir: Path) -> bool:
    """True if tracked files have uncommitted changes. No .git -> False.

    Untracked files never count: local logs, backups and user files must not
    block an update.
    """
    if not (install_dir / ".git").exists():
        return False
    proc = subprocess.run(
        ["git", "-C", str(install_dir), "status", "--porcelain=v1",
         "--untracked-files=no"],
        capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise UpdateError(f"git status failed: {proc.stderr.strip()}")
    return bool(proc.stdout.strip())


def _github_request(url: str, token: str | None) -> bytes:
    headers = {"User-Agent": "muse-mobile-channels-self-update",
               "Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise UpdateError("no GitHub Release exists yet for this repo")
        raise UpdateError(f"GitHub API HTTP {exc.code} for {url}")
    except (urllib.error.URLError, TimeoutError) as exc:
        raise UpdateError(f"GitHub API unreachable: {exc}")


def get_latest_release(repo: str, token: str | None = None
                       ) -> tuple[str, str, str]:
    """Return (version, tag, tarball asset URL) for the latest release."""
    raw = _github_request(
        f"https://api.github.com/repos/{repo}/releases/latest", token)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise UpdateError(f"bad JSON from GitHub API: {exc}")
    tag = data.get("tag_name", "")
    try:
        version = format_version(tag_to_version(tag))
    except ValueError:
        raise UpdateError(f"latest release tag {tag!r} is not a version tag")
    asset_name = f"muse-mobile-channels-{version}.tar.gz"
    asset_url = None
    for asset in data.get("assets", []):
        if asset.get("name") == asset_name:
            asset_url = asset.get("browser_download_url")
            break
    if not asset_url:
        raise UpdateError(f"latest release {tag} has no {asset_name} asset")
    return version, tag, asset_url


def download(url: str, dest: Path, token: str | None = None) -> None:
    headers = {"User-Agent": "muse-mobile-channels-self-update"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as resp, \
                dest.open("wb") as handle:
            shutil.copyfileobj(resp, handle)
    except (urllib.error.URLError, TimeoutError) as exc:
        raise UpdateError(f"download failed for {url}: {exc}")


def _forbidden_reason(member_name: str) -> str | None:
    """Why this tarball member is forbidden, or None if it is fine."""
    if member_name.startswith("/") or ".." in Path(member_name).parts:
        return "absolute path or '..' escape"
    parts = Path(member_name).parts
    if any(part in FORBIDDEN_PATH_PARTS for part in parts):
        return "forbidden path component"
    base = parts[-1] if parts else ""
    if base in FORBIDDEN_BASENAMES:
        return f"forbidden file name {base!r}"
    if base.startswith(FORBIDDEN_NAME_PREFIXES):
        return f"forbidden file name prefix for {base!r}"
    if base.endswith(FORBIDDEN_SUFFIXES):
        return f"forbidden file suffix for {base!r}"
    if base.endswith(".json") and any(
            sub in base for sub in FORBIDDEN_NAME_SUBSTRINGS):
        return f"forbidden credential/secret file {base!r}"
    return None


def verify_tarball(tarball: Path, expected_version: str) -> None:
    """Raise UpdateError unless the tarball is safe and matches the version.

    Rules: every member under one top-level directory
    ``muse-mobile-channels-<version>/``; that directory holds a VERSION file
    whose content equals ``expected_version``; no forbidden members.
    """
    prefix = f"muse-mobile-channels-{expected_version}/"
    try:
        archive = tarfile.open(tarball, "r:gz")
    except (tarfile.TarError, OSError) as exc:
        raise UpdateError(f"cannot open tarball {tarball}: {exc}")
    with archive:
        members = archive.getmembers()
        if not members:
            raise UpdateError("tarball is empty")
        for member in members:
            if not member.name.startswith(prefix):
                raise UpdateError(
                    f"tarball member outside {prefix!r}: {member.name!r}")
            reason = _forbidden_reason(member.name)
            if reason is not None:
                raise UpdateError(
                    f"tarball member rejected ({reason}): {member.name!r}")
        version_member = prefix + "VERSION"
        try:
            extracted = archive.extractfile(version_member)
        except KeyError:
            raise UpdateError(f"tarball has no {version_member}")
        if extracted is None:
            raise UpdateError(f"tarball member {version_member} unreadable")
        content = extracted.read().decode("utf-8", "replace").strip()
        if content != expected_version:
            raise UpdateError(
                f"tarball VERSION is {content!r}, expected "
                f"{expected_version!r}")


def backup_install(install_dir: Path, old_version: str) -> Path:
    """Copy the code tree (no .git, no state, no caches) next to the install."""
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = install_dir.parent / f"{install_dir.name}.bak-{old_version}-{stamp}"
    ignore = shutil.ignore_patterns(
        ".git", "state", "__pycache__", "*.pyc", ".venv", "venv",
        "dist", "build", "*.egg-info", "*.log")
    shutil.copytree(install_dir, backup, ignore=ignore)
    return backup


def apply_tarball(tarball: Path, install_dir: Path) -> None:
    """Unpack a verified tarball over the install dir (no deletions)."""
    with tempfile.TemporaryDirectory(prefix="self-update-") as tmp:
        staging = Path(tmp)
        with tarfile.open(tarball, "r:gz") as archive:
            archive.extractall(staging, filter="data")
        roots = [child for child in staging.iterdir() if child.is_dir()]
        if len(roots) != 1:
            raise UpdateError(
                f"tarball must unpack to one directory, got {len(roots)}")
        shutil.copytree(roots[0], install_dir, dirs_exist_ok=True)


def _service_installed(service: str) -> bool:
    proc = subprocess.run(
        ["systemctl", "show", "-p", "LoadState", "--value",
         f"{service}.service"],
        capture_output=True, text=True, timeout=30)
    return proc.returncode == 0 and proc.stdout.strip() == "loaded"


def restart_services(services: list[str], log_file: Path | None) -> bool:
    """Restart each installed service; return False if any restart failed."""
    if shutil.which("systemctl") is None:
        log("systemctl not available; restart the gateway services manually",
            log_file)
        return True
    ok = True
    for service in services:
        if not _service_installed(service):
            log(f"service {service} not installed; skipping restart", log_file)
            continue
        proc = subprocess.run(
            ["systemctl", "restart", f"{service}.service"],
            capture_output=True, text=True, timeout=120)
        if proc.returncode == 0:
            log(f"restarted {service}.service", log_file)
        else:
            log(f"FAILED to restart {service}.service: "
                f"{proc.stderr.strip()}", log_file)
            ok = False
    return ok


def run(install_dir: Path, repo: str, token: str | None, *,
        check_only: bool = False, dry_run: bool = False,
        no_restart: bool = False,
        fake_latest_version: str | None = None,
        fake_tarball: Path | None = None) -> int:
    log_file = install_dir / "self-update.log"
    local_version = read_local_version(install_dir)
    log(f"local version {local_version} at {install_dir}", log_file)
    if (fake_latest_version is None) != (fake_tarball is None):
        raise UpdateError("--fake-latest-version and --fake-tarball go together")
    if fake_latest_version is not None:
        latest_version = format_version(parse_version(fake_latest_version))
        asset_url = f"fake://{fake_tarball}"
    else:
        latest_version, tag, asset_url = get_latest_release(repo, token)
        log(f"latest release {tag} -> version {latest_version}", log_file)
    if parse_version(latest_version) <= parse_version(local_version):
        log(f"already on latest version {local_version}; nothing to do",
            log_file)
        return 0
    log(f"new version available: {local_version} -> {latest_version}",
        log_file)
    if check_only:
        return 0
    if is_tree_dirty(install_dir):
        log("local tree has uncommitted tracked changes; "
            "skipping update (commit or stash them first)", log_file)
        return 0
    if dry_run:
        log(f"dry-run: would download {asset_url}, back up {local_version}, "
            f"unpack {latest_version}, restart gateway services", log_file)
        return 0
    with tempfile.TemporaryDirectory(prefix="self-update-dl-") as tmp:
        tarball = Path(tmp) / f"muse-mobile-channels-{latest_version}.tar.gz"
        if fake_tarball is not None:
            shutil.copyfile(fake_tarball, tarball)
        else:
            log(f"downloading {asset_url}", log_file)
            download(asset_url, tarball, token)
        verify_tarball(tarball, latest_version)
        log(f"tarball verified for version {latest_version}", log_file)
        backup = backup_install(install_dir, local_version)
        log(f"backed up {local_version} to {backup}", log_file)
        apply_tarball(tarball, install_dir)
        log(f"unpacked {latest_version} over {install_dir}", log_file)
    if no_restart:
        log("restart skipped (--no-restart); restart services manually",
            log_file)
        return 0
    if not restart_services(SERVICES, log_file):
        log("update applied but a service restart failed; "
            "check the services before relying on the new version", log_file)
        return 1
    log(f"updated {local_version} -> {latest_version}", log_file)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install-dir", default=None,
                        help="deployed checkout (default: repo root)")
    parser.add_argument("--repo", default=DEFAULT_REPO,
                        help="owner/repo for releases")
    parser.add_argument("--check-only", action="store_true",
                        help="only compare versions, change nothing")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would happen, change nothing")
    parser.add_argument("--no-restart", action="store_true",
                        help="do not restart services after unpacking")
    parser.add_argument("--fake-latest-version", default=None,
                        help="test hook: pretend this is the latest version")
    parser.add_argument("--fake-tarball", default=None,
                        help="test hook: use this tarball instead of downloading")
    parser.add_argument("--verify-tarball", default=None,
                        help="only verify a tarball, then exit")
    parser.add_argument("--expect-version", default=None,
                        help="expected version for --verify-tarball")
    args = parser.parse_args(argv)

    if args.verify_tarball is not None:
        if not args.expect_version:
            parser.error("--verify-tarball needs --expect-version")
        try:
            verify_tarball(Path(args.verify_tarball), args.expect_version)
        except UpdateError as exc:
            print(f"verify-tarball: FAIL: {exc}", file=sys.stderr)
            return 1
        print(f"verify-tarball: OK for version {args.expect_version}")
        return 0

    install_dir = (Path(args.install_dir).resolve() if args.install_dir
                   else Path(__file__).resolve().parent.parent)
    token = os.environ.get("GH_TOKEN") or None
    try:
        return run(
            install_dir, args.repo, token,
            check_only=args.check_only, dry_run=args.dry_run,
            no_restart=args.no_restart,
            fake_latest_version=args.fake_latest_version,
            fake_tarball=(Path(args.fake_tarball)
                          if args.fake_tarball else None))
    except UpdateError as exc:
        log_file = install_dir / "self-update.log"
        log(f"ERROR: {exc}", log_file)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
