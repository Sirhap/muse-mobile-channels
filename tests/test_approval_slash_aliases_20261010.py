#!/usr/bin/env python3
"""Short approval slash aliases for the Weixin and WeCom gateways.

The typed commands must be short enough for personal WeChat, which
has no button card:

- ``/批 N`` approves once (allow_once)
- ``/批 N 永`` approves always (allow_always); ``永久`` still counts
- ``/拒 N`` denies

``/批准``, ``/拒绝``, ``/审批`` and the English approve/deny/approvals
commands stay valid. A missing or non-numeric N gets the short usage
line and writes no decision. This file never opens a socket and never
reads the hatch relay; both gateways are pointed at a temp directory.

Run: python3 tests/test_approval_slash_aliases_20261010.py
"""
import asyncio
import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SBX = Path("/tmp/approval-slash-alias-sbx")
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
    """Load a gateway module without executing its ``__main__`` block."""
    spec = importlib.util.spec_from_file_location(mod_name, ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_jsonl(path):
    """Return decoded JSONL rows, or an empty list when the file is absent."""
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


wx = load_gateway("wxgw_alias", "weixin-bot/gateway.py")
wc = load_gateway("wcgw_alias", "wecom-bot/gateway.py")


def seed_relay(gateway, relay_dir):
    """Point one gateway at a temp relay with pending items 15-40."""
    relay_dir.mkdir(parents=True, exist_ok=True)
    items = {
        str(n): {"approval_id": f"aid-{n}", "who": "Shell command",
                 "what": "命令：echo hi", "status": "pending"}
        for n in range(15, 41)
    }
    (relay_dir / "state.json").write_text(
        json.dumps({"next_num": 41, "items": items}), encoding="utf-8")
    (relay_dir / "decisions.jsonl").write_text("", encoding="utf-8")
    gateway.APPROVAL_RELAY_DIR = relay_dir


WX_RELAY = SBX / "weixin-relay"
WC_RELAY = SBX / "wecom-relay"
seed_relay(wx, WX_RELAY)
seed_relay(wc, WC_RELAY)


class _WxSink:
    """Captures the ack the Weixin slash handler would have sent."""

    def __init__(self):
        self.acks = []

    async def _send_slash_ack(self, client, creds, msg, from_user, ack):
        self.acks.append(ack)


class _WcSink:
    """Captures the ack the WeCom slash handler would have sent."""

    def __init__(self):
        self.acks = []

    async def _send_slash_ack(self, chatid, chattype, ack, req_id=""):
        self.acks.append(ack)


async def dispatch_weixin(text):
    """Run the real Weixin slash handler with no network send."""
    sink = _WxSink()
    result = await wx.Gateway._slash_dispatch(
        sink, None, None, {}, text, "user")
    return result, sink.acks


async def dispatch_wecom(text):
    """Run the real WeCom slash handler with no network send."""
    sink = _WcSink()
    result = await wc.Gateway._slash_dispatch(sink, text, "chat", "single")
    return result, sink.acks


# (text, num, decision, ack prefix). Same table for both channels.
OK_CASES = [
    ("/批 15", "15", "allow_once", "已提交批准（仅这次）"),
    ("/批 16 永", "16", "allow_always", "已提交批准（永久）"),
    ("/批 17 永久", "17", "allow_always", "已提交批准（永久）"),
    ("/批  18   永", "18", "allow_always", "已提交批准（永久）"),
    ("/拒 19", "19", "deny", "已提交拒绝"),
    ("/批准 20", "20", "allow_once", "已提交批准（仅这次）"),
    ("/批准 21 永久", "21", "allow_always", "已提交批准（永久）"),
    ("/批准 22 永", "22", "allow_always", "已提交批准（永久）"),
    ("/拒绝 23", "23", "deny", "已提交拒绝"),
    ("/approve 24", "24", "allow_once", "已提交批准（仅这次）"),
    ("/approve 25 永", "25", "allow_always", "已提交批准（永久）"),
    ("/approve 26 永久", "26", "allow_always", "已提交批准（永久）"),
    ("/deny 27", "27", "deny", "已提交拒绝"),
    ("/APPROVE 28", "28", "allow_once", "已提交批准（仅这次）"),
    ("/DENY 29", "29", "deny", "已提交拒绝"),
    ("／批 30", "30", "allow_once", "已提交批准（仅这次）"),
    ("／拒 31", "31", "deny", "已提交拒绝"),
    # 「永」 is exact. A longer word must not silently become always.
    ("/批 32 永远", "32", "allow_once", "已提交批准（仅这次）"),
    ("/审批", None, None, None),
    ("/approvals", None, None, None),
]

USAGE_CASES = [
    "/批",
    "/拒",
    "/批准",
    "/拒绝",
    "/approve",
    "/deny",
    "/批 abc",
    "/批 -1",
    "/批 1.5",
    "/批 15a",
    "/拒 xyz",
    "/拒绝 -3",
    "/批 15永",
    "/批准 15永久",
]

NOT_COMMANDS = ["/批15", "/批准15", "/拒15", "/foo", "批 15", "/批次 1"]


async def check_channel(label, gateway, relay_dir, dispatch, channel):
    """Walk the command table against one gateway's real slash handler."""
    for text, num, decision, prefix in OK_CASES:
        before = len(read_jsonl(relay_dir / "decisions.jsonl"))
        result, acks = await dispatch(text)
        rows = read_jsonl(relay_dir / "decisions.jsonl")
        if num is None:
            check(f"{label} {text} lists pending with short footer",
                  result == "handled"
                  and len(acks) == 1
                  and gateway.APPROVAL_LIST_FOOTER in acks[0]
                  and "/批准" not in acks[0]
                  and "#15" in acks[0]
                  and len(rows) == before)
            continue
        check(f"{label} {text} -> {decision}",
              result == "handled"
              and len(acks) == 1
              and acks[0].startswith(prefix)
              and f"#{num}" in acks[0]
              and len(rows) == before + 1
              and rows[-1]["num"] == num
              and rows[-1]["decision"] == decision
              and rows[-1]["channel"] == channel)

    for text in USAGE_CASES:
        before = len(read_jsonl(relay_dir / "decisions.jsonl"))
        result, acks = await dispatch(text)
        rows = read_jsonl(relay_dir / "decisions.jsonl")
        check(f"{label} usage {text}",
              result == "handled"
              and acks == [gateway.APPROVAL_USAGE]
              and len(rows) == before)

    for text in NOT_COMMANDS:
        before = len(read_jsonl(relay_dir / "decisions.jsonl"))
        result, acks = await dispatch(text)
        rows = read_jsonl(relay_dir / "decisions.jsonl")
        check(f"{label} not a command {text}",
              result is None and acks == [] and len(rows) == before)


def check_prompts():
    """User-visible help and usage lines lead with the short commands."""
    help_wx = wx.slash_help_text()
    help_wc = wc.slash_help_text()
    check("help text identical on both channels", help_wx == help_wc)
    check("help shows /批 N", "/批 N 批准第 N 条（仅这次）" in help_wx)
    check("help shows /批 N 永", "/批 N 永 永久批准同类" in help_wx)
    check("help shows /拒 N", "/拒 N 拒绝" in help_wx)
    check("help keeps long forms as aliases",
          "别名 /批准、/拒绝" in help_wx and "永也可写永久" in help_wx)
    check("help no longer leads with the long command",
          "/批准 N 批准第 N 条" not in help_wx)
    check("usage line is the short form and matches",
          wx.APPROVAL_USAGE == wc.APPROVAL_USAGE
          == "用法：/批 N（仅这次）、/批 N 永、/拒 N；N 发 /审批 查看。")
    check("list footer is the short form and matches",
          wx.APPROVAL_LIST_FOOTER == wc.APPROVAL_LIST_FOOTER
          == "回 /批 N（仅这次）｜/批 N 永｜/拒 N")
    check("aliases map short and long forms",
          wx.SLASH_ALIASES["批"] == wc.SLASH_ALIASES["批"] == "approve"
          and wx.SLASH_ALIASES["批准"] == "approve"
          and wx.SLASH_ALIASES["拒"] == wc.SLASH_ALIASES["拒"] == "deny"
          and wx.SLASH_ALIASES["拒绝"] == "deny"
          and wx.SLASH_ALIASES["审批"] == "approvals")
    check("wecom card protocol keys unchanged",
          wc.APPROVAL_CARD_KEYS == {
              "appr_once": "allow_once",
              "appr_always": "allow_always",
              "appr_deny": "deny",
          })
    check("wecom card failure hint uses /批",
          wc.APPROVAL_CARD_TEXT_FALLBACK
          == "请改用文字 /批 或在 Muse 应用里处理。"
          and "/批准" not in wc.APPROVAL_CARD_TEXT_FALLBACK)


async def main():
    check_prompts()
    await check_channel("weixin", wx, WX_RELAY, dispatch_weixin, "weixin")
    await check_channel("wecom", wc, WC_RELAY, dispatch_wecom, "wecom")
    failed = [name for name, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    if failed:
        print("FAILED:")
        for name in failed:
            print("  " + name)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
