#!/usr/bin/env python3
"""Slash command wording against what the gateways actually do.

Todo 25 (2026-10-15). Help text, receipts, and a few command results
must describe the real action. Approval letters stay
allow_once / allow_always / deny. No sockets and no real bridge files.

Run: python3 tests/test_slash_commands_audit_20261015.py
"""
import asyncio
import importlib.util
import inspect
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/slash-audit-20261015")
shutil.rmtree(SBX, ignore_errors=True)
SBX.mkdir(parents=True)
os.environ["HOME"] = str(SBX)
os.environ["MUSE_HOME"] = str(SBX)

RESULTS = []


def check(name, cond):
    """Record one assertion and print PASS or FAIL."""
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def load_gateway(mod_name, rel_path):
    """Load a gateway module without executing its __main__ block."""
    spec = importlib.util.spec_from_file_location(mod_name, ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


wx = load_gateway("wxgw_slash_audit", "weixin-bot/gateway.py")
wc = load_gateway("wcgw_slash_audit", "wecom-bot/gateway.py")


class _WxSink:
    """Captures the ack the Weixin slash handler would have sent."""

    def __init__(self):
        self.acks = []
        self.feedback_track = {}

    async def _send_slash_ack(self, client, creds, msg, from_user, ack):
        self.acks.append(ack)

    _feedback_forget = wx.Gateway._feedback_forget


class _WcSink:
    """Captures the ack the WeCom slash handler would have sent."""

    def __init__(self):
        self.acks = []
        self.feedback_track = {}

    async def _send_slash_ack(self, chatid, chattype, ack, req_id=""):
        self.acks.append(ack)

    _feedback_forget = wc.Gateway._feedback_forget


async def dispatch_weixin(text, msg=None):
    """Run the real Weixin slash handler with no network send."""
    sink = _WxSink()
    result = await wx.Gateway._slash_dispatch(
        sink, None, None, msg or {}, text, "user")
    return result, sink


async def dispatch_wecom(text, msgid=""):
    """Run the real WeCom slash handler with no network send."""
    sink = _WcSink()
    result = await wc.Gateway._slash_dispatch(
        sink, text, "chat", "single", msgid, "")
    return result, sink


def bind_sandbox(gateway, tag):
    """Point one gateway's slash writes at the sandbox."""
    base = SBX / tag
    state = base / "state"
    hook = base / "hooks"
    state.mkdir(parents=True, exist_ok=True)
    hook.mkdir(parents=True, exist_ok=True)
    gateway.STATE = state
    gateway.HOOK_STATE_DIR = hook
    return state, hook


def silence_bridge(gateway):
    """Make every bridge read look empty, without touching disk."""
    gateway._bridge_queue_lines = lambda channel: ("", [])
    gateway._bridge_queued_rows = lambda channel: []
    gateway._bridge_snapshot = lambda channel: ([], [])
    gateway._spool_texts = lambda channel: {}


def plant_bridge(gateway, summary, active, queued):
    """Plant an in-memory bridge queue for /queue rendering."""
    gateway._bridge_queue_lines = lambda channel: (summary, [r["msgid"] for r in active])
    gateway._bridge_queued_rows = lambda channel: [(m, t) for m, t in queued]
    gateway._bridge_snapshot = lambda channel: (list(active), [m for m, _t in queued])
    texts = {r["msgid"]: r.get("text", "") for r in active}
    texts.update({m: t for m, t in queued})
    gateway._spool_texts = lambda channel: texts


def check_shared_wording():
    """Help and the shared receipts are the same string on both sides."""
    help_wx = wx.slash_help_text()
    help_wc = wc.slash_help_text()
    check("help text identical", help_wx == help_wc)
    check("new receipts identical",
          wx.SLASH_NEW_ACK == wc.SLASH_NEW_ACK
          and wx.SLASH_NEW_FAIL_ACK == wc.SLASH_NEW_FAIL_ACK)
    for name in ("slash_stop_ack", "slash_queue_report",
                 "slash_subagent_dispatched_ack", "slash_subagent_stop_ack",
                 "_bridge_active_detail"):
        check(f"source identical {name}",
              inspect.getsource(getattr(wx, name))
              == inspect.getsource(getattr(wc, name)))
    check("help describes lazy /new",
          "此刻不建新会话" in help_wx
          and "下一条走原生桥的文字才会新建" in help_wx
          and "已开新会话" not in help_wx
          and "换一个全新的原生会话" not in help_wx)
    check("help lists bridge drop alias bN and english approval",
          "bN" in help_wx and "/approve" in help_wx and "/deny" in help_wx
          and "/approvals" in help_wx)
    check("help no longer promises immediate subagent execution",
          "并行执行" not in help_wx and "名额满了会先排队" in help_wx)
    check("help keeps the approval legend contract",
          "a=仅这次  b=永久  c=拒绝" in help_wx
          and "回 /a N｜/b N｜/c N" in help_wx
          and "/批 N（仅这次）" in help_wx
          and "/批 N 永" in help_wx
          and "/拒 N" in help_wx
          and "/批准" in help_wx
          and "/拒绝" in help_wx
          and "永也可写永久" in help_wx
          and "/批准 N 批准第 N 条" not in help_wx)
    check("letter decisions unchanged",
          wx.APPROVAL_LETTER_DECISIONS == wc.APPROVAL_LETTER_DECISIONS
          == {"a": "allow_once", "b": "allow_always", "c": "deny"}
          and wx.parse_approval_slash("a", "7") == ("7", "allow_once")
          and wx.parse_approval_slash("b", "7") == ("7", "allow_always")
          and wx.parse_approval_slash("c", "7") == ("7", "deny")
          and wc.parse_approval_slash("b", "7 永") == ("7", "allow_always")
          and wx.parse_approval_slash("approve", "7 永") == ("7", "allow_always")
          and wx.parse_approval_slash("deny", "7") == ("7", "deny"))


def check_stop_receipts():
    """Each side's sentence appears only when that side has work."""
    cold = wx.slash_stop_ack(2, 0, 2, 0)
    bridge = wx.slash_stop_ack(0, 1, 1, 0)
    both = wx.slash_stop_ack(1, 1, 2, 1)
    failed = wx.slash_stop_ack(1, 1, 0, 2)
    check("cold stop names the progress check and not the bridge",
          "下一次进度检查" in cold and "不再另发一句确认" not in cold
          and "回你一句确认" not in cold)
    check("bridge stop does not promise a second confirmation",
          "不再另发一句确认" in bridge and "下一次进度检查" not in bridge)
    check("mixed stop names both and reports a partial failure",
          "下一次进度检查" in both and "不再另发一句确认" in both
          and "另有 1 条登记失败" in both)
    check("total registration failure is not described as done",
          failed.startswith("取消没有登记上") and "已登记取消" not in failed)
    check("wecom stop receipt matches",
          wc.slash_stop_ack(2, 0, 2, 0) == cold)


def check_status_label(gateway, label):
    """The summary counts detached batches as 后台续跑, not 并行."""
    state, hook = bind_sandbox(gateway, label + "-status")
    (state / "status.json").write_text(
        json.dumps({"state": "connected", "connected": True}),
        encoding="utf-8")
    (hook / "active_batch.json").write_text(
        json.dumps({"detached": [{"msgids": ["D1"], "since": time.time()}]}),
        encoding="utf-8")
    (hook / "pending.json").write_text("{}", encoding="utf-8")
    text = gateway.slash_status_text("测试", state, hook)
    check(f"{label} status says 后台续跑 batches",
          "后台续跑 1 批" in text and "并行" not in text)
    overview = gateway._task_overview_text(label, state, hook)
    check(f"{label} overview keeps cold rows when bridge state is missing",
          "后台续跑" in overview and "桥状态暂时读不到" in overview)


def bridge_line(text):
    """The 原生桥 item row from a /check report.

    The conclusion line can also mention 原生桥 when that item failed,
    so only the ✓/✗ item row counts.
    """
    rows = [line for line in text.splitlines()
            if line.startswith(("✓", "✗")) and "原生桥" in line]
    return rows[0] if len(rows) == 1 else ""


def check_bridge_item(gateway, label, key):
    """Shadow with a fresh heartbeat is a failed check; live passes.

    The channel key is taken from the hook directory name
    (``weixin-bot`` / ``wecom-bot``), same as the gateway.
    """
    state, _hook = bind_sandbox(gateway, label + "-check")
    hook = state.parent / f"{key}-bot"
    hook.mkdir(parents=True, exist_ok=True)
    (state / "status.json").write_text("{}", encoding="utf-8")
    orig = gateway._read_json_file

    def wrapped(path, default=None):
        if str(path).endswith("native-bridge/status.json"):
            return planted["body"]
        return orig(path, default)

    planted = {"body": {}}
    gateway._read_json_file = wrapped
    try:
        now = time.time()
        planted["body"] = {"ts": now, "mode": {key: "shadow"}}
        shadow = bridge_line(gateway.slash_run_check("测试", state, hook))
        planted["body"] = {"ts": now, "mode": {key: "live"}}
        live = bridge_line(gateway.slash_run_check("测试", state, hook))
        planted["body"] = {"ts": now - 120, "mode": {key: "live"}}
        stale = bridge_line(gateway.slash_run_check("测试", state, hook))
    finally:
        gateway._read_json_file = orig
    check(f"{label} shadow bridge is not a pass",
          shadow.startswith("✗") and "shadow" in shadow)
    check(f"{label} live bridge passes",
          live.startswith("✓") and "live" in live)
    check(f"{label} stale bridge heartbeat fails",
          stale.startswith("✗") and "无心跳" in stale)


async def check_commands(gateway, dispatch, label):
    """Drive the real slash handler for the receipts that changed."""
    state, hook = bind_sandbox(gateway, label + "-cmd")
    silence_bridge(gateway)
    real_write = gateway._write_hook_json

    result, sink = await dispatch("/new")
    boundary = json.loads((hook / "topic_boundary.json").read_text(encoding="utf-8"))
    check(f"{label} /new writes a boundary and does not claim a session",
          result == "handled" and sink.acks == [gateway.SLASH_NEW_ACK]
          and isinstance(boundary.get("ts"), (int, float))
          and "已开新会话" not in sink.acks[0])

    result, sink = await dispatch("/新会话")
    check(f"{label} /新会话 uses the same receipt",
          result == "handled" and sink.acks == [gateway.SLASH_NEW_ACK])

    def fail_write(*_args, **_kwargs):
        return False

    gateway._write_hook_json = fail_write
    result, sink = await dispatch("/new")
    check(f"{label} /new says so when the boundary cannot be written",
          result == "handled" and sink.acks == [gateway.SLASH_NEW_FAIL_ACK])
    gateway._write_hook_json = real_write

    result, sink = await dispatch("/queue")
    check(f"{label} empty /queue says idle once",
          result == "handled"
          and sink.acks == ["队列空闲：没有正在处理或排队的任务。"])

    plant_bridge(
        gateway,
        "\n原生桥：进行中 1 条（main 12s），排队 1 条",
        [{"msgid": "B1", "lane": "main", "secs": 12, "text": "正在跑的桥消息"}],
        [("Q1", "排队的桥消息")],
    )
    result, sink = await dispatch("/queue")
    ack = sink.acks[0] if sink.acks else ""
    check(f"{label} bridge work is not introduced as idle",
          result == "handled" and "队列空闲" not in ack
          and "冷通道：没有正在处理或排队的任务" in ack
          and "正在跑的桥消息" in ack and "桥1." in ack
          and "排队的桥消息" in ack)
    silence_bridge(gateway)

    (hook / "pending.json").write_text(
        json.dumps({"C1": time.time() - 30}), encoding="utf-8")
    (state / "inbox.jsonl").write_text(
        json.dumps({"msgid": "C1", "text": "冷通道排队的一句"}) + "\n",
        encoding="utf-8")
    cold_sink = _WxSink() if label == "weixin" else _WcSink()
    cold_sink.feedback_track = {"C1": {"queued": True}, "KEEP": {}}
    if label == "weixin":
        result = await gateway.Gateway._slash_dispatch(
            cold_sink, None, None, {}, "/queue drop 1", "user")
    else:
        result = await gateway.Gateway._slash_dispatch(
            cold_sink, "/queue drop 1", "chat", "single", "", "")
    check(f"{label} cold drop names the cold queue and forgets feedback",
          result == "handled" and cold_sink.acks
          and "冷通道排队第 1 条" in cold_sink.acks[0]
          and "C1" not in cold_sink.feedback_track
          and "KEEP" in cold_sink.feedback_track)

    gateway._bridge_queued_rows = lambda channel: [("B9", "桥上排队")]
    gateway._bridge_admin = lambda channel, action, msgids: True

    class _TrackSink(_WxSink if label == "weixin" else _WcSink):
        def __init__(self):
            super().__init__()
            self.feedback_track = {"B9": {"via_bridge": True}, "KEEP": {}}

    sink = _TrackSink()
    if label == "weixin":
        result = await gateway.Gateway._slash_dispatch(
            sink, None, None, {}, "/queue drop b1", "user")
    else:
        result = await gateway.Gateway._slash_dispatch(
            sink, "/queue drop b1", "chat", "single", "", "")
    check(f"{label} /queue drop b1 drops the bridge row and its feedback",
          result == "handled" and sink.acks
          and "原生桥排队第 1 条" in sink.acks[0]
          and "B9" not in sink.feedback_track
          and "KEEP" in sink.feedback_track)

    result, sink = await dispatch("/queue drop")
    check(f"{label} incomplete /queue drop is still not a command",
          result is None and sink.acks == [])
    result, sink = await dispatch("/foo")
    check(f"{label} unknown slash is still not a command",
          result is None and sink.acks == [])

    gateway.slash_register_cancels = lambda msgids: (len(list(msgids)), 0)
    gateway._queue_summary = lambda hook_dir: ({}, ["C1"], 0, 0)
    gateway._bridge_queue_lines = lambda channel: ("", [])
    result, sink = await dispatch("/stop")
    check(f"{label} /stop cold-only receipt",
          result == "handled" and sink.acks
          and "下一次进度检查" in sink.acks[0]
          and "不再另发一句确认" not in sink.acks[0])

    gateway._queue_summary = lambda hook_dir: ({}, [], 0, 0)
    gateway._bridge_queue_lines = lambda channel: ("", ["B1"])
    result, sink = await dispatch("/stop")
    check(f"{label} /stop bridge-only receipt",
          result == "handled" and sink.acks
          and "不再另发一句确认" in sink.acks[0]
          and "下一次进度检查" not in sink.acks[0])

    gateway._queue_summary = lambda hook_dir: ({}, [], 0, 0)
    gateway._bridge_queue_lines = lambda channel: ("", [])
    result, sink = await dispatch("/stop")
    check(f"{label} /stop with nothing running",
          result == "handled" and sink.acks == ["当前没有正在跑的任务。"])

    result, sink = await dispatch("/subagent 帮我记一笔")
    ack = sink.acks[0] if sink.acks else ""
    check(f"{label} /subagent registers instead of claiming it is running",
          result == "handled" and ack.startswith("【副助手 #S1 已登记】")
          and "已派发" not in ack and "后台执行中" not in ack
          and (hook / "subagent_requests.jsonl").exists())

    result, sink = await dispatch("/subagent")
    check(f"{label} bare /subagent usage says 登记",
          result == "handled" and sink.acks
          and "登记" in sink.acks[0] and "派发" not in sink.acks[0])

    (hook / "subagent_jobs.json").write_text(json.dumps({
        "jobs": {
            "S2": {"status": "queued", "text": "还在排"},
            "S3": {"status": "running", "text": "正在做"},
        }
    }), encoding="utf-8")
    result, sink = await dispatch("/subagent stop S2")
    check(f"{label} stopping a queued subagent does not promise a result",
          result == "handled" and sink.acks
          and "还在排队" in sink.acks[0] and "已停止" not in sink.acks[0])
    result, sink = await dispatch("/subagent stop 3")
    check(f"{label} stopping a running subagent names the stopped result",
          result == "handled" and sink.acks
          and "【副助手 #S3 已停止】" in sink.acks[0])


async def main():
    check_shared_wording()
    check_stop_receipts()
    check_status_label(wx, "weixin")
    check_status_label(wc, "wecom")
    check_bridge_item(wx, "weixin", "weixin")
    check_bridge_item(wc, "wecom", "wecom")
    await check_commands(wx, dispatch_weixin, "weixin")
    await check_commands(wc, dispatch_wecom, "wecom")
    failed = [name for name, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    if failed:
        print("FAILED:")
        for name in failed:
            print("  " + name)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
