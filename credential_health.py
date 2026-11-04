"""Credential metadata and safe failure categories shared by legacy services.

These helpers perform no network requests and never return exception text or
credential contents. Callers own their state, retry budgets and notifications.
"""
from __future__ import annotations

import hashlib
import json
import fcntl
import os
from copy import deepcopy
from pathlib import Path
import re
import uuid


class CredentialAuthError(RuntimeError):
    """An authentication rejection whose upstream response has been redacted."""


def credential_fingerprint(directory: str | Path, names: tuple[str, ...]) -> str:
    """Identify a file set by metadata, including atomic file replacement."""
    entries = []
    for name in names:
        try:
            metadata = (Path(directory) / name).stat()
            entries.append((name, metadata.st_mtime_ns, metadata.st_size,
                            metadata.st_ino, metadata.st_ctime_ns))
        except OSError:
            entries.append((name, "unavailable"))
    return hashlib.sha256(json.dumps(entries).encode("utf-8")).hexdigest()


def is_auth_failure(exc: Exception) -> bool:
    """Recognize authentication errors only at a credential/RPC boundary."""
    if any(kind.__name__ in {"AuthError", "AuthenticationError", "CredentialAuthError"}
           for kind in type(exc).__mro__):
        return True
    for name in ("status", "status_code"):
        if getattr(exc, name, None) in (401, 403, "401", "403"):
            return True
    message = str(exc)
    return ("hatchAdmissionInvalidated" in message
            or bool(re.search(r"\b(?:HTTP|status|auth/check|upgrade)\b[^\n]{0,40}\b(?:401|403)\b",
                              message, re.IGNORECASE)))


def failure_kind(exc: Exception) -> str:
    """Classify a failure without exposing tokens, request data or SDK errors."""
    if is_auth_failure(exc):
        return "auth"
    if isinstance(exc, ImportError) or isinstance(exc.__cause__, ImportError):
        return "dependency"
    if isinstance(exc, (ValueError, TypeError, KeyError, FileNotFoundError,
                        PermissionError, UnicodeError)):
        return "configuration"
    return "network"


def observe_connection(previous: dict, reports: list[dict], selected: str | None,
                       now: int) -> dict:
    """Merge source probes and retain a Cookie outage across token successes.

    This is a pure state transition. Pending events are committed by the caller
    before enqueueing and acknowledged separately, using their stable IDs.
    """
    health = deepcopy(previous)
    sources = health.setdefault("sources", {})
    pending = health.setdefault("pending_notices", [])
    for report in reports:
        name = report["source"]
        entry = sources.setdefault(name, {})
        entry.update(report)
        if report["status"] == "healthy":
            entry["last_verified_at"] = report["checked_at"]
    health["active_source"] = selected
    if selected is not None:
        health["last_verified_at"] = now
    cookie = sources.get("cookies", {})
    cookie_bad = (cookie.get("status") == "failed"
                  and cookie.get("failure_kind") in {"auth", "configuration"}) or (
                      cookie.get("status") == "missing" and cookie.get("last_verified_at") is not None)
    all_bad = selected is None and any(
        report.get("failure_kind") in {"auth", "configuration"}
        or report.get("status") == "missing" for report in reports)
    incident = health.get("incident") or {}
    if (cookie_bad or all_bad) and not incident.get("active"):
        incident = {"id": uuid.uuid4().hex, "active": True,
                    "required_source": "cookies" if cookie_bad else "any",
                    "fault_queued": False,
                    "severity": "degraded" if cookie_bad and selected == "token" else "unavailable"}
        health["incident"] = incident
        if cookie_bad and selected == "token":
            text = ("⚠️ 原生桥 Cookie 凭证验证失败，当前 token 回退验证通过；"
                    "Cookie 路径尚未恢复，审批中继可能受影响。")
        else:
            text = "⚠️ 原生桥登录凭证不可用，当前没有验证通过的原生连接。"
        text += ("请在已登录 Muse 的浏览器安装凭证导出扩展，重新导出 "
                 "muse-credentials.zip，以文件交给 Muse 核对并安装；只换 token 不能恢复 Cookie。"
                 "同一故障只提醒一次，连续失败 3 次后暂停自动连接。")
        pending.append({"id": f"credential-{incident['id']}-fault", "kind": "fault",
                        "incident_id": incident["id"], "text": text})
    elif incident.get("active") and selected is None and reports and incident.get("severity") == "degraded":
        incident["severity"] = "unavailable"
        pending.append({"id": f"credential-{incident['id']}-unavailable", "kind": "escalation",
                        "incident_id": incident["id"],
                        "text": "⚠️ 原生桥状态变化：先前可用的 token 回退也已验证失败，当前没有验证通过的原生连接。请用浏览器扩展重新导出完整 muse-credentials.zip 交给 Muse 核对、安装和验活；已提交任务不重复提交。"})
    elif incident.get("active") and selected is not None:
        cookie_verified = any(report["source"] == "cookies" and report["status"] == "healthy"
                              for report in reports)
        if not cookie_bad and (incident["required_source"] == "any" or cookie_verified):
            incident["active"] = False
            pending.append({"id": f"credential-{incident['id']}-recovery", "kind": "recovery",
                            "incident_id": incident["id"],
                            "text": "原生桥凭证验证已恢复（sessions.list 成功）。这只确认认证连接；真实消息收发仍需入站消息和回复投递结果验证。"})
    return health


def append_health_notice_once(path: str | Path, row: dict) -> None:
    """Durably append one stable event id under the outbox writer lock.

    An existing row is re-synced before acknowledging a retry, including when a
    previous append reached the file but its fsync failed. Malformed JSONL and
    partial final records block writes instead of joining unrelated messages.
    The caller owns parent-directory creation and channel/mode authorization.
    """
    if not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"]:
        raise ValueError("health notice requires a stable event id")
    destination = Path(path)
    with destination.open("a+", encoding="utf-8", newline="") as output:
        fcntl.flock(output, fcntl.LOCK_EX)
        output.seek(0)
        found = False
        for line in output:
            if not line.endswith("\n"):
                raise ValueError("outbox has an incomplete record")
            try:
                previous = json.loads(line)
            except ValueError as exc:
                raise ValueError("outbox has an invalid record") from exc
            if not isinstance(previous, dict):
                raise ValueError("outbox record must be an object")
            if previous.get("id") == row["id"]:
                found = True
        if not found:
            output.seek(0, os.SEEK_END)
            output.write(json.dumps(row, ensure_ascii=False) + "\n")
        output.flush()
        os.fsync(output.fileno())
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
