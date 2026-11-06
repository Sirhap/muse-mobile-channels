#!/usr/bin/env python3
"""Probe each credential path without changing bridge or approval runtime state.

This command prints only fixed diagnostic fields, never SDK responses, VM IDs,
credential values, exception messages, or approval contents.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import time


def _bridge_module():
    """Load bridge helpers on demand without requiring an installed Muse SDK."""
    path = Path(__file__).with_name("native_bridge.py")
    spec = importlib.util.spec_from_file_location("credential_diagnostic_bridge", path)
    if spec is None or spec.loader is None:
        raise ImportError("bridge helpers unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _failure_kind(exc: Exception) -> str:
    """Use the shared sanitized classification without exposing the exception."""
    root = str(Path(__file__).resolve().parents[1])
    if root not in sys.path:
        sys.path.insert(0, root)
    from credential_health import failure_kind
    return failure_kind(exc)


def _client_for_source(bridge, source: str):
    """Construct one source explicitly, preserving the token environment override."""
    if source == "cookies":
        params = bridge.cookie_params()
        if params is None:
            return None
        cookies, vm_id = params
        if not isinstance(cookies, str) or not cookies.strip():
            raise ValueError("invalid cookie credential")
        if not isinstance(vm_id, str) or not vm_id.strip():
            raise ValueError("invalid VM binding")
        return bridge.gateway_class()(cookies=cookies, vm_id=vm_id)
    params = bridge.token_params()
    if params is None:
        return None
    names = ("vm_id", "access_token", "hatch_token")
    if not isinstance(params, dict) or any(
            not isinstance(params.get(name), str) or not params[name].strip()
            for name in names):
        raise ValueError("invalid token credential")
    return bridge.gateway_class()(cookies="", **{name: params[name] for name in names})


def _origin(source: str) -> str:
    """Identify the input channel without returning the credential itself."""
    return "environment" if source == "token" and os.environ.get("NATIVE_TOKEN_JSON") else "file"


def _validate_response(response, check: str) -> None:
    """Reject explicit RPC errors without assuming a sessions.list envelope shape."""
    # Approval consumers require an object; the external SDK's sessions.list
    # envelope is not vendored, so both an object and a direct list are allowed.
    allowed = (dict, list) if check == "bridge" else (dict,)
    if not isinstance(response, allowed):
        raise RuntimeError("credential probe received an invalid RPC response")
    if isinstance(response, dict) and (response.get("error") or response.get("ok") is False):
        raise RuntimeError("credential probe RPC returned an error")


def _probe(bridge, source: str, check: str) -> dict:
    """Run one isolated read-only RPC and always release the constructed client."""
    result = {"source": source, "origin": _origin(source), "check": check, "status": "failed",
              "checked_at": int(time.time()), "failure_kind": None}
    client = None
    # Optional SDKs may log errors themselves; only our allowlisted result is public.
    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        try:
            client = _client_for_source(bridge, source)
            if client is None:
                result.update(status="missing", failure_kind="configuration")
            else:
                method = "sessions.list" if check == "bridge" else "egress.approvals"
                response = client.call_json(method, timeout=20)
                _validate_response(response, check)
                result["status"] = "healthy"
        except Exception as exc:
            result["failure_kind"] = _failure_kind(exc)
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    # Keep the RPC verdict, but do not report the command as fully OK.
                    result["cleanup_failed"] = True
    return result


def run_checks(source: str = "all", *, bridge=None) -> dict:
    """Return independent Cookie/bridge, Cookie/approval and token/bridge verdicts."""
    if source not in ("all", "cookies", "token"):
        raise ValueError("unsupported credential source")
    checks = []
    if source in ("all", "cookies"):
        checks.extend((("cookies", "bridge"), ("cookies", "approval")))
    if source in ("all", "token"):
        checks.append(("token", "bridge"))
    if bridge is None:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            try:
                bridge = _bridge_module()
            except Exception as exc:
                kind = _failure_kind(exc)
                return {"checks": [{"source": name, "origin": _origin(name), "check": check,
                                    "status": "failed", "checked_at": int(time.time()),
                                    "failure_kind": kind} for name, check in checks]}
    return {"checks": [_probe(bridge, name, check) for name, check in checks]}


def main(argv=None) -> int:
    """Render sanitized diagnostics and fail if any requested check is incomplete."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("all", "cookies", "token"), default="all")
    parser.add_argument("--json", action="store_true", dest="as_json")
    arguments = parser.parse_args(argv)
    report = run_checks(arguments.source)
    if arguments.as_json:
        print(json.dumps(report, ensure_ascii=False))
    else:
        for item in report["checks"]:
            detail = item["failure_kind"] or "verified"
            if item.get("cleanup_failed"):
                detail += ", cleanup_failed"
            print(f"{item['source']}/{item['check']}: {item['status']} ({detail})")
    return int(any(item["status"] != "healthy" or item.get("cleanup_failed")
                   for item in report["checks"]))


if __name__ == "__main__":
    raise SystemExit(main())
