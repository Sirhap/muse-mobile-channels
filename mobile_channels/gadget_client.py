"""官方 Gadget 侧会话（Side Session）协议客户端与凭证管理。

纯 Python 3 标准库实现，支持通过环境变量、本地配置文件加载凭证，
支持创建/绑定隔离侧会话、发送任务、轮询执行状态及取消。
"""
from __future__ import annotations

import inspect
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


class GadgetError(Exception):
    """Gadget 协议客户端通用异常。"""


class GadgetAuthError(GadgetError):
    """Gadget 凭证缺失或认证鉴权失败。"""


class GadgetNetworkError(GadgetError):
    """Gadget 网络传输或上游服务异常。"""


@dataclass
class GadgetCredentials:
    """Gadget 官方 SDK 鉴权凭证。"""

    device_id: str | None = None
    token: str | None = None
    endpoint: str | None = None

    def is_configured(self) -> bool:
        """检查凭证是否完备可用（需要 device_id 和 token 均为有效非空字符串）。"""
        return bool(
            isinstance(self.device_id, str) and self.device_id.strip() and
            isinstance(self.token, str) and self.token.strip()
        )

    @classmethod
    def from_env_or_file(
        cls,
        file_path: str | Path | None = None,
        env: Mapping[str, str] | None = None,
    ) -> GadgetCredentials:
        """从配置文件与环境变量加载凭证，环境变量具有更高优先级。"""
        device_id: str | None = None
        token: str | None = None
        endpoint: str | None = "https://gadget.muse.local"

        # 1. 尝试从本地配置文件加载
        if file_path is None:
            path = Path.home() / ".config" / "muse" / "gadget.json"
        else:
            path = Path(file_path).expanduser()

        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    raw_dev = data.get("device_id")
                    raw_tok = data.get("token")
                    raw_ep = data.get("endpoint")
                    device_id = str(raw_dev) if raw_dev is not None else None
                    token = str(raw_tok) if raw_tok is not None else None
                    endpoint = str(raw_ep) if raw_ep is not None else endpoint
            except Exception:
                pass

        # 2. 环境变量覆盖
        environ = os.environ if env is None else env
        if "MUSE_GADGET_DEVICE_ID" in environ and environ["MUSE_GADGET_DEVICE_ID"]:
            device_id = str(environ["MUSE_GADGET_DEVICE_ID"])
        if "MUSE_GADGET_TOKEN" in environ and environ["MUSE_GADGET_TOKEN"]:
            token = str(environ["MUSE_GADGET_TOKEN"])
        if "MUSE_GADGET_ENDPOINT" in environ and environ["MUSE_GADGET_ENDPOINT"]:
            endpoint = str(environ["MUSE_GADGET_ENDPOINT"])

        return cls(device_id=device_id, token=token, endpoint=endpoint)


class GadgetClient:
    """Gadget 官方侧会话协议客户端（纯 Python 3 标准库）。"""

    def __init__(
        self,
        credentials: GadgetCredentials | None = None,
        transport_func: Callable[..., Any] | None = None,
    ):
        """初始化客户端，可注入 transport_func 以供测试 Mock。"""
        self.credentials = credentials if credentials is not None else GadgetCredentials.from_env_or_file()
        self.transport_func = transport_func
        self._sessions: dict[tuple[str, int], str] = {}
        self._closed = False

    def is_configured(self) -> bool:
        """检查凭证是否有效。"""
        return self.credentials.is_configured()

    def _call_transport(self, action: str, payload: dict) -> Any:
        """调用注入的 transport_func，根据签名精准分发，不吞没回调内部的 TypeError。"""
        if self.transport_func is None:
            raise GadgetError("未配置 transport_func")
        try:
            sig = inspect.signature(self.transport_func)
            params = list(sig.parameters.values())
            if len(params) == 1 and params[0].kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                return self.transport_func(payload)
        except (ValueError, TypeError):
            pass
        return self.transport_func(action, payload)

    def create_or_get_side_session(self, conversation_id: str, generation: int) -> str:
        """生成/申请专属侧会话 ID（如 gadget:side:{conversation_id}:{generation}）。"""
        if self._closed:
            raise GadgetError("GadgetClient 已关闭")
        key = (conversation_id, generation)
        if key in self._sessions:
            return self._sessions[key]

        session_id = f"gadget:side:{conversation_id}:{generation}"
        if self.transport_func is not None:
            try:
                res = self._call_transport("create_session", {
                    "conversation_id": conversation_id,
                    "generation": generation,
                    "session_id": session_id,
                })
                if isinstance(res, dict) and res.get("session_id"):
                    session_id = str(res["session_id"])
                elif isinstance(res, str) and res:
                    session_id = res
            except Exception:
                pass

        self._sessions[key] = session_id
        return session_id

    def send_message(self, session_id: str, text: str, attachments: list | None = None) -> str:
        """向侧会话发送消息并返回 run_id。"""
        if self._closed:
            raise GadgetError("GadgetClient 已关闭")
        payload = {
            "session_id": session_id,
            "text": text,
            "attachments": attachments or [],
        }
        if self.transport_func is not None:
            res = self._call_transport("send_message", payload)
            if isinstance(res, str) and res:
                return res
            if isinstance(res, dict) and res.get("run_id"):
                return str(res["run_id"])
            raise GadgetError(f"transport 返回了无效的响应: {res}")

        if not self.credentials.is_configured():
            raise GadgetAuthError("Gadget 凭证未配置")

        safe_session_id = urllib.parse.quote(str(session_id), safe=":")
        res = self._http_post(f"/api/sessions/{safe_session_id}/messages", payload)
        run_id = res.get("run_id") if isinstance(res, dict) else None
        if not isinstance(run_id, str) or not run_id:
            raise GadgetError("平台未返回有效的 run_id")
        return run_id

    def get_run_status(self, session_id: str, run_id: str) -> dict:
        """获取侧会话任务状态及输出文本。"""
        if self._closed:
            raise GadgetError("GadgetClient 已关闭")
        payload = {"session_id": session_id, "run_id": run_id}
        if self.transport_func is not None:
            res = self._call_transport("get_run_status", payload)
            if isinstance(res, dict):
                return res
            raise GadgetError(f"transport 返回了无效的状态响应: {res}")

        if not self.credentials.is_configured():
            raise GadgetAuthError("Gadget 凭证未配置")

        safe_session_id = urllib.parse.quote(str(session_id), safe=":")
        safe_run_id = urllib.parse.quote(str(run_id), safe="")
        res = self._http_get(f"/api/sessions/{safe_session_id}/runs/{safe_run_id}")
        if isinstance(res, dict):
            return res
        raise GadgetError("平台返回了非 JSON 格式的状态响应")

    def cancel_run(self, session_id: str, run_id: str) -> bool:
        """向侧会话发送取消任务请求。"""
        if self._closed:
            raise GadgetError("GadgetClient 已关闭")
        payload = {"session_id": session_id, "run_id": run_id}
        if self.transport_func is not None:
            res = self._call_transport("cancel_run", payload)
            if isinstance(res, bool):
                return res
            if isinstance(res, dict):
                return bool(res.get("cancelled", res.get("success", True)))
            return True

        if not self.credentials.is_configured():
            raise GadgetAuthError("Gadget 凭证未配置")

        safe_session_id = urllib.parse.quote(str(session_id), safe=":")
        safe_run_id = urllib.parse.quote(str(run_id), safe="")
        res = self._http_post(f"/api/sessions/{safe_session_id}/runs/{safe_run_id}/cancel", {})
        return bool(res.get("cancelled", res.get("success", True)))

    def close(self) -> None:
        """关闭客户端。"""
        self._closed = True
        self._sessions.clear()

    def __enter__(self) -> GadgetClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _http_post(self, path: str, payload: dict) -> dict:
        """基于标准库 urllib 发起 POST 请求。"""
        endpoint = (self.credentials.endpoint or "https://gadget.muse.local").rstrip("/")
        url = f"{endpoint}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.credentials.token}",
                "X-Device-ID": str(self.credentials.device_id or ""),
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                raise GadgetAuthError(f"Gadget 认证失败 ({err.code}): {err.reason}") from err
            raise GadgetNetworkError(f"Gadget HTTP 错误 ({err.code}): {err.reason}") from err
        except Exception as err:
            raise GadgetNetworkError(f"Gadget 网络异常: {err}") from err

    def _http_get(self, path: str) -> dict:
        """基于标准库 urllib 发起 GET 请求。"""
        endpoint = (self.credentials.endpoint or "https://gadget.muse.local").rstrip("/")
        url = f"{endpoint}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.credentials.token}",
                "X-Device-ID": str(self.credentials.device_id or ""),
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                raise GadgetAuthError(f"Gadget 认证失败 ({err.code}): {err.reason}") from err
            raise GadgetNetworkError(f"Gadget HTTP 错误 ({err.code}): {err.reason}") from err
        except Exception as err:
            raise GadgetNetworkError(f"Gadget 网络异常: {err}") from err
