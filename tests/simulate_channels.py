#!/usr/bin/env python3
"""模拟微信 / 企业微信收发消息的 test 进程。

默认双渠道（weixin + wecom），不连真实平台、不动生产状态、不跑
真实 Muse、不执行桥任务。用法：

    # 默认：双渠道各跑一套固定场景（/ping、/new、普通文本、重复去重、
    # 非白名单丢弃、模拟 worker 回复后用户侧收消息）
    ~/workspace/wecom-bot/.venv/bin/python tests/simulate_channels.py

    # 只跑一个渠道，或只发一条自定义消息
    ~/workspace/wecom-bot/.venv/bin/python tests/simulate_channels.py --channels weixin
    ~/workspace/wecom-bot/.venv/bin/python tests/simulate_channels.py --message "你好"

    # 交互进程：逐行输入，逐行看模拟收发结果，空行或 quit 退出
    ~/workspace/wecom-bot/.venv/bin/python tests/simulate_channels.py --interactive

    # 保留沙箱目录以便事后翻 inbox / outbox / spool
    ~/workspace/wecom-bot/.venv/bin/python tests/simulate_channels.py --keep

必须用带 httpx + websockets 的 venv 跑（例如 wecom-bot/.venv 或
muse-test-venv），系统 python3 缺这两个包，网关模块 import 就会失败。

实现方式（和 tests/test_slash_commands_audit_20261015.py 同一路）：
- 不实例化 Gateway.__init__（那会抢生产 gateway.lock），而是用
  object.__new__ 造一个假网关对象，只填 handle_inbound /
  handle_message_callback 用到的属性；
- 把网关模块的 STATE / INBOX / OUTBOX / spool / seen / hook 目录
  等全局量全部重绑到 /tmp 下的沙箱，生产文件只读桥 state.json
  的部分被桩成空（/queue、/stop 的桥快照），其余一律不碰生产；
- 新版分流（2eeeb02 起）：网关按 QUOTE_SPOOL/BRIDGE_SPOOL 的
  父目录结构推导桥 BASE（spool/ 上一级），再在锁内查
  divert-<channel> 标志、bridge_diversion_state 和 .fallback
  标记。沙箱因此按生产结构搭一个假桥 BASE：
  <sbx>/<channel>-bridge/{divert-<channel>,status.json,
  state.json,spool/<channel>.jsonl}，status.json 里 pid 是
  本进程（alive）、worker_heartbeat 是当前时间、mode=live，
  state.json 里 turns/recovery_queue 为空、spool_offset=0、
  submission_pending=None，才能走到 ready→spool；心跳过期
  且无在途/积压时则走 unavailable→inbox 并写 fallback 标记；
- 平台发送全部桩掉：微信的 _send_slash_ack / _send_typing、企微的
  respond / send_frame 只记录、不发包；微信的 _maybe_arrival_feedback
  也桩掉，避免它往沙箱 outbox 写额外的到达通知，干扰回执断言；
- “收到回复”那一步是模拟的：test 进程扮演 worker，往沙箱 outbox
  写一条 formal reply 行，再扮演用户侧客户端读出它。真实网关的
  outbox_loop（把 outbox 行真正发到微信/企微平台）不在本进程覆盖
  范围内，见文末“未覆盖”。

生产形状（直接取自真实流量，不是凭空编的）：
- 微信 iLink：message_id 是 int（2026-10-17 待办 28 的 W7 教训，
  int 必须转 str，不能走空 id 路径），from_user_id 必须是 owner，
  文本在 item_list[type=1].text_item.text 里；
- 企微：frame.body.msgid 是 str，msgtype=text，text.content，
  from.userid 必须在 allowlist 里，否则 fail-closed 直接丢弃。
"""

import argparse
import asyncio
import importlib.util
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

WX_OWNER = "sim-owner"
WC_USER = "sim-user"


def check(name, cond, detail=""):
    ok = bool(cond)
    print(("PASS " if ok else "FAIL ") + name + (f"  {detail}" if detail else ""))
    return ok


def read_jsonl(path):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def append_jsonl(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_gateway(mod_name, rel_path):
    spec = importlib.util.spec_from_file_location(mod_name, ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def rebind(mod, state_dir, hook_dir, spool_path):
    """把网关模块的状态全局量重绑到沙箱。生产文件一个也不写。"""
    mod.STATE = state_dir
    mod.INBOX = state_dir / "inbox.jsonl"
    mod.OUTBOX = state_dir / "outbox.jsonl"
    mod.OUTBOX_RESULTS = state_dir / "outbox_results.jsonl"
    mod.SEEN_FILE = state_dir / "seen_ids.jsonl"
    mod.STATUS = state_dir / "status.json"
    mod.HOOK_STATE_DIR = hook_dir
    # 桥快照只读生产 state.json，且 fail-silent；沙箱里直接桩成空，
    # 避免 /status、/queue 的断言被生产在途任务干扰。
    mod._bridge_queue_lines = lambda channel: ("", [])
    mod._bridge_queued_rows = lambda channel: []
    mod._bridge_snapshot = lambda channel: ([], [])
    return state_dir, hook_dir


def make_bridge_base(sbx, channel, heartbeat_age=0, base=None):
    """按生产结构搭沙箱桥 BASE，返回 (base, spool_path)。

    heartbeat_age=0 表示心跳就是现在（ready）；>45 表示过期，
    且 turns/积压为空时 bridge_diversion_state 应判 unavailable。
    """
    import os
    base = base or sbx / f"{channel}-bridge"
    spool_dir = base / "spool"
    spool_dir.mkdir(parents=True, exist_ok=True)
    (base / f"divert-{channel}").touch(exist_ok=True)
    now = int(time.time())
    (base / "status.json").write_text(json.dumps({
        "pid": os.getpid(), "ts": now,
        "mode": {channel: "live"},
        "worker_heartbeat": {channel: now - heartbeat_age},
    }), encoding="utf-8")
    (base / "state.json").write_text(json.dumps({
        "channels": {channel: {
            "turns": [], "recovery_queue": [], "spool_offset": 0,
            "submission_pending": None, "reconnect_exhausted": False,
        }},
    }), encoding="utf-8")
    return base, spool_dir / f"{channel}.jsonl"


def set_bridge_heartbeat(base, channel, age):
    """改沙箱桥心跳年龄（秒），不碰其他状态。"""
    status = json.loads((base / "status.json").read_text(encoding="utf-8"))
    status["worker_heartbeat"][channel] = int(time.time()) - age
    (base / "status.json").write_text(json.dumps(status), encoding="utf-8")


# ---------------------------------------------------------------- 微信
def make_weixin(sbx, tag="weixin", heartbeat_age=0):
    mod = load_gateway(f"wxgw_sim_{tag}", "weixin-bot/gateway.py")
    state_dir = sbx / tag / "state"
    hook_dir = sbx / tag / "hooks"
    state_dir.mkdir(parents=True, exist_ok=True)
    hook_dir.mkdir(parents=True, exist_ok=True)
    bridge_base, spool_path = make_bridge_base(
        sbx, "weixin", heartbeat_age, base=sbx / f"{tag}-bridge")
    rebind(mod, state_dir, hook_dir, spool_path)
    mod.QUOTE_SPOOL = spool_path
    mod.DIVERTED_RAW = state_dir / "diverted_raw.jsonl"
    mod.SENT_INDEX = state_dir / "sent_index.jsonl"
    mod.CONTEXT_FILE = state_dir / "context.json"
    mod.SYNC_FILE = state_dir / "sync.json"
    gw = object.__new__(mod.Gateway)
    gw.connected = False
    gw.started_at = int(time.time())
    gw.msgs_received = 0
    gw.msgs_sent = 0
    gw.last_error = ""
    gw.state = "starting"
    gw.seen_msgids = set()
    gw.context = {}
    gw.typing_tickets = {}
    gw.thinking_notice_at = {}
    gw.feedback_track = {}
    gw.merged_notice_at = {}
    gw.sync_buf = ""
    gw._core_binding = None
    gw._lock_fd = None
    gw.acks = []

    async def _send_slash_ack(client, creds, msg, from_user, ack):
        gw.acks.append(ack)

    async def _send_typing(client, creds, user_id, context_token, status):
        return None

    async def _download_media(client, media, msgid):
        return None

    gw._send_slash_ack = _send_slash_ack
    gw._send_typing = _send_typing
    gw._download_media = _download_media
    # 到达反馈只记内存、不写 outbox，避免干扰回执断言。
    gw._maybe_arrival_feedback = lambda *a, **k: "simulated"
    return mod, gw, state_dir


def weixin_msg(msgid, text):
    """生产形状：message_id 是 int，文本在 item_list 里。"""
    return {
        "message_id": msgid,
        "message_type": 1,
        "from_user_id": WX_OWNER,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
    }


async def run_weixin(mod, gw, text, msgid):
    before_acks = len(gw.acks)
    await mod.Gateway.handle_inbound(
        gw, None, {"user_id": WX_OWNER}, weixin_msg(msgid, text)
    )
    return gw.acks[before_acks:]


# ---------------------------------------------------------------- 企微
def make_wecom(sbx, tag="wecom", heartbeat_age=0):
    mod = load_gateway(f"wcgw_sim_{tag}", "wecom-bot/gateway.py")
    state_dir = sbx / tag / "state"
    hook_dir = sbx / tag / "hooks"
    state_dir.mkdir(parents=True, exist_ok=True)
    hook_dir.mkdir(parents=True, exist_ok=True)
    bridge_base, spool_path = make_bridge_base(
        sbx, "wecom", heartbeat_age, base=sbx / f"{tag}-bridge")
    rebind(mod, state_dir, hook_dir, spool_path)
    mod.BRIDGE_SPOOL = spool_path
    mod.REQMAP = state_dir / "reqmap.json"
    mod.CARDS = state_dir / "cards.json"
    mod.MEDIA_DIR = state_dir / "media"
    mod.COMPRESSED_DIR = state_dir / "compressed"
    gw = object.__new__(mod.Gateway)
    gw.ws = None
    gw.connected = False
    gw.started_at = int(time.time())
    gw.msgs_received = 0
    gw.msgs_sent = 0
    gw.last_error = ""
    gw.state = "starting"
    gw.pending_responses = {}
    gw.respond_locks = {}
    gw.open_streams = {}
    gw._http = None
    gw.allow_users = {WC_USER}
    gw.seen_msgids = set()
    gw.reqmap = {}
    gw.cards = {}
    gw.feedback_track = {}
    gw._coalescer = None
    gw._coalesce_pending = {}
    gw._lock_fd = None
    gw._kicked = False
    gw.reconnect_failures = 0
    gw._subscribed = False
    gw._core_binding = None
    gw._inbound_locks = {}
    gw.frames = []

    async def respond(req_id, body, timeout=10.0):
        gw.frames.append({"req_id": req_id, "body": body})
        return {"errcode": 0}

    async def send_frame(frame, wait_response=True, timeout=5.0):
        gw.frames.append(frame)
        return {"errcode": 0}

    async def _download_media(media, msgid):
        return None

    gw.respond = respond
    gw.send_frame = send_frame
    gw._download_media = _download_media
    return mod, gw, state_dir


def wecom_frame(msgid, text, userid=WC_USER, req_id="sim-req-1"):
    return {
        "headers": {"req_id": req_id},
        "body": {
            "msgid": msgid,
            "msgtype": "text",
            "text": {"content": text},
            "from": {"userid": userid},
            "chatid": userid,
            "chattype": "single",
        },
    }


async def run_wecom(mod, gw, text, msgid, userid=WC_USER):
    before = len(gw.frames)
    await mod.Gateway.handle_message_callback(gw, wecom_frame(msgid, text, userid))
    return gw.frames[before:]


# ---------------------------------------------------------------- 场景
def simulate_worker_reply(state_dir, msgid, text):
    """test 进程扮演 worker + 用户侧客户端：写 formal reply，再读回它。

    这一步是模拟的，不是网关 outbox_loop 的真实平台发送。
    """
    append_jsonl(
        state_dir / "outbox.jsonl",
        {"mode": "reply", "msgid": msgid, "text": text, "ts": int(time.time())},
    )
    rows = read_jsonl(state_dir / "outbox.jsonl")
    got = [r for r in rows if r.get("msgid") == msgid and r.get("mode") == "reply"]
    return got[-1]["text"] if got else ""


async def scenario(channel, sbx):
    print(f"\n== {channel} ==")
    results = []
    if channel == "weixin":
        mod, gw, state = make_weixin(sbx)
        # 1. /ping：网关本地命令，不进 inbox、不唤 worker。
        acks = await run_weixin(mod, gw, "/ping", 100001)
        results.append(check("/ping 有回执", acks, f"acks={acks}"))
        # 2. /new：写 hook topic_boundary，回执必须是短句。
        acks = await run_weixin(mod, gw, "/new", 100002)
        boundary = (sbx / "weixin" / "hooks" / "topic_boundary.json").exists()
        results.append(check("/new 回执是短句且写了边界", acks == ["已开启新会话。"] and boundary, f"acks={acks}"))
        # 3. 普通文本：生产 divert 标志存在时走桥 spool（已重绑到沙箱）。
        await run_weixin(mod, gw, "模拟消息 hello", 100003)
        spool = read_jsonl(sbx / "weixin-bridge" / "spool" / "weixin.jsonl")
        inbox = read_jsonl(state / "inbox.jsonl")
        results.append(check("普通文本进 spool（divert）", any(r.get("msgid") == "100003" for r in spool), f"spool={len(spool)} inbox={len(inbox)}"))
        # 4. 重复 msgid：去重，不能再进一条。
        n_before = len(spool) + len(inbox)
        await run_weixin(mod, gw, "模拟消息 hello", 100003)
        n_after = len(read_jsonl(sbx / "weixin-bridge" / "spool" / "weixin.jsonl")) + len(read_jsonl(state / "inbox.jsonl"))
        results.append(check("重复 msgid 被去重", n_after == n_before, f"{n_before}->{n_after}"))
        # 5. 非 owner：直接丢弃。
        await mod.Gateway.handle_inbound(
            gw, None, {"user_id": WX_OWNER},
            {**weixin_msg(100004, "陌生人消息"), "from_user_id": "not-owner"},
        )
        n_after2 = len(read_jsonl(sbx / "weixin-bridge" / "spool" / "weixin.jsonl")) + len(read_jsonl(state / "inbox.jsonl"))
        results.append(check("非 owner 被丢弃", n_after2 == n_before, f"{n_before}->{n_after2}"))
        # 6. 模拟回复收消息。
        got = simulate_worker_reply(state, "100003", "模拟回复：已收到 hello")
        results.append(check("用户侧收到模拟回复", got == "模拟回复：已收到 hello", f"got={got}"))
        # 7-8. 新版降级（C01/C02 沙箱版，不碰生产桥）：用一个全新
        # 沙箱桥 BASE，心跳过期 100 秒、无在途、无积压。
        mod2, gw2, state2 = make_weixin(sbx, tag="weixin-fallback", heartbeat_age=100)
        fb_base = sbx / "weixin-fallback-bridge"
        fb_spool = fb_base / "spool" / "weixin.jsonl"
        await run_weixin(mod2, gw2, "降级测试一", 100101)
        fb_inbox = read_jsonl(state2 / "inbox.jsonl")
        results.append(check("桥不可用时进 inbox 且写 fallback 标记",
            not fb_spool.exists() and any(r.get("msgid") == "100101" for r in fb_inbox)
            and (fb_base / ".fallback-weixin").exists(),
            f"spool_exists={fb_spool.exists()} inbox={len(fb_inbox)}"))
        # 桥心跳恢复后，标记仍在，第二条仍走 inbox，不回 spool。
        set_bridge_heartbeat(fb_base, "weixin", 0)
        await run_weixin(mod2, gw2, "降级测试二", 100102)
        fb_inbox2 = read_jsonl(state2 / "inbox.jsonl")
        results.append(check("标记在时桥恢复也不回 spool",
            not fb_spool.exists() and any(r.get("msgid") == "100102" for r in fb_inbox2),
            f"spool_exists={fb_spool.exists()} inbox={len(fb_inbox2)}"))
        # C03 沙箱版：在路由锁保护下解除标记（模拟维护操作），
        # 第三条只进 spool，不进 inbox。
        (fb_base / ".fallback-weixin").unlink()
        await run_weixin(mod2, gw2, "降级解除后测试", 100103)
        fb_inbox3 = read_jsonl(state2 / "inbox.jsonl")
        fb_spool3 = read_jsonl(fb_spool)
        results.append(check("解除标记后第三条只进 spool",
            any(r.get("msgid") == "100103" for r in fb_spool3)
            and not any(r.get("msgid") == "100103" for r in fb_inbox3),
            f"spool={len(fb_spool3)} inbox={len(fb_inbox3)}"))
        # B02：两条可区分问题，各自进 spool，不串不重复。
        await run_weixin(mod, gw, "问题A MUSE-TEST-WX-01", 100201)
        await run_weixin(mod, gw, "问题B MUSE-TEST-WX-02", 100202)
        spool2 = read_jsonl(sbx / "weixin-bridge" / "spool" / "weixin.jsonl")
        results.append(check("B02 两条问题各进 spool 一次",
            any(r.get("msgid") == "100201" for r in spool2)
            and any(r.get("msgid") == "100202" for r in spool2)
            and sum(1 for r in spool2 if r.get("msgid") in ("100201", "100202")) == 2,
            f"spool={len(spool2)}"))
        # B03：/new 后发新问题，边界已写，新问题进 spool。
        acks = await run_weixin(mod, gw, "/new", 100203)
        await run_weixin(mod, gw, "新主题问题 MUSE-TEST-WX-03", 100204)
        spool3 = read_jsonl(sbx / "weixin-bridge" / "spool" / "weixin.jsonl")
        boundary2 = (sbx / "weixin" / "hooks" / "topic_boundary.json").exists()
        results.append(check("B03 /new 后新问题进 spool",
            acks == ["已开启新会话。"] and boundary2
            and any(r.get("msgid") == "100204" for r in spool3),
            f"acks={acks}"))
        # B04：无在途任务时 /stop 不谎称已停止。
        acks = await run_weixin(mod, gw, "/stop", 100205)
        results.append(check("B04 无任务时 /stop 不谎称停止",
            acks == ["当前没有正在跑的任务。"], f"acks={acks}"))
    else:
        mod, gw, state = make_wecom(sbx)
        # 1. /ping：企微走 respond 回 pong（frame 里能看到）。
        frames = await run_wecom(mod, gw, "ping", "sim-wc-1")
        results.append(check("/ping 回 pong 帧", "pong" in json.dumps(frames, ensure_ascii=False), f"frames={len(frames)}"))
        # 2. /new：同微信，短句 + 边界文件。
        frames = await run_wecom(mod, gw, "/new", "sim-wc-2")
        boundary = (sbx / "wecom" / "hooks" / "topic_boundary.json").exists()
        text = json.dumps(frames, ensure_ascii=False)
        results.append(check("/new 有回执且写了边界", "已开启新会话。" in text and boundary, f"frames={len(frames)}"))
        # 3. 普通文本：divert spool（沙箱）+ stream 帧。
        # 注意：wecom 的 ping 特判会在 inbox 留一条 auto_handled 的
        # sim-wc-1，所以这里只断言 spool 里有 sim-wc-3，不数 inbox 总数。
        frames = await run_wecom(mod, gw, "模拟消息 hello", "sim-wc-3")
        spool = read_jsonl(sbx / "wecom-bridge" / "spool" / "wecom.jsonl")
        inbox = read_jsonl(state / "inbox.jsonl")
        results.append(check("普通文本进 spool（divert）", any(r.get("msgid") == "sim-wc-3" for r in spool), f"spool={len(spool)} inbox={len(inbox)} frames={len(frames)}"))
        # 4. 重复 msgid 去重。
        n_before = len(spool) + len(inbox)
        await run_wecom(mod, gw, "模拟消息 hello", "sim-wc-3")
        n_after = len(read_jsonl(sbx / "wecom-bridge" / "spool" / "wecom.jsonl")) + len(read_jsonl(state / "inbox.jsonl"))
        results.append(check("重复 msgid 被去重", n_after == n_before, f"{n_before}->{n_after}"))
        # 5. 非白名单：fail-closed 丢弃。
        await run_wecom(mod, gw, "陌生人消息", "sim-wc-4", userid="not-allowlisted")
        n_after2 = len(read_jsonl(sbx / "wecom-bridge" / "spool" / "wecom.jsonl")) + len(read_jsonl(state / "inbox.jsonl"))
        results.append(check("非白名单被丢弃", n_after2 == n_before, f"{n_before}->{n_after2}"))
        # 6. 模拟回复收消息。
        got = simulate_worker_reply(state, "sim-wc-3", "模拟回复：已收到 hello")
        results.append(check("用户侧收到模拟回复", got == "模拟回复：已收到 hello", f"got={got}"))
        # 7-8. 新版降级（C01/C02 沙箱版）：全新沙箱桥 BASE，心跳过期。
        mod2, gw2, state2 = make_wecom(sbx, tag="wecom-fallback", heartbeat_age=100)
        fb_base = sbx / "wecom-fallback-bridge"
        fb_spool = fb_base / "spool" / "wecom.jsonl"
        await run_wecom(mod2, gw2, "降级测试一", "sim-fb-1")
        fb_inbox = read_jsonl(state2 / "inbox.jsonl")
        results.append(check("桥不可用时进 inbox 且写 fallback 标记",
            not fb_spool.exists() and any(r.get("msgid") == "sim-fb-1" for r in fb_inbox)
            and (fb_base / ".fallback-wecom").exists(),
            f"spool_exists={fb_spool.exists()} inbox={len(fb_inbox)}"))
        set_bridge_heartbeat(fb_base, "wecom", 0)
        await run_wecom(mod2, gw2, "降级测试二", "sim-fb-2")
        fb_inbox2 = read_jsonl(state2 / "inbox.jsonl")
        results.append(check("标记在时桥恢复也不回 spool",
            not fb_spool.exists() and any(r.get("msgid") == "sim-fb-2" for r in fb_inbox2),
            f"spool_exists={fb_spool.exists()} inbox={len(fb_inbox2)}"))
        (fb_base / ".fallback-wecom").unlink()
        await run_wecom(mod2, gw2, "降级解除后测试", "sim-fb-3")
        fb_inbox3 = read_jsonl(state2 / "inbox.jsonl")
        fb_spool3 = read_jsonl(fb_spool)
        results.append(check("解除标记后第三条只进 spool",
            any(r.get("msgid") == "sim-fb-3" for r in fb_spool3)
            and not any(r.get("msgid") == "sim-fb-3" for r in fb_inbox3),
            f"spool={len(fb_spool3)} inbox={len(fb_inbox3)}"))
        await run_wecom(mod, gw, "问题A MUSE-TEST-WC-01", "sim-wc-201")
        await run_wecom(mod, gw, "问题B MUSE-TEST-WC-02", "sim-wc-202")
        spool2 = read_jsonl(sbx / "wecom-bridge" / "spool" / "wecom.jsonl")
        results.append(check("B02 两条问题各进 spool 一次",
            any(r.get("msgid") == "sim-wc-201" for r in spool2)
            and any(r.get("msgid") == "sim-wc-202" for r in spool2)
            and sum(1 for r in spool2 if r.get("msgid") in ("sim-wc-201", "sim-wc-202")) == 2,
            f"spool={len(spool2)}"))
        frames = await run_wecom(mod, gw, "/new", "sim-wc-203")
        await run_wecom(mod, gw, "新主题问题 MUSE-TEST-WC-03", "sim-wc-204")
        spool3 = read_jsonl(sbx / "wecom-bridge" / "spool" / "wecom.jsonl")
        boundary2 = (sbx / "wecom" / "hooks" / "topic_boundary.json").exists()
        results.append(check("B03 /new 后新问题进 spool",
            "已开启新会话。" in json.dumps(frames, ensure_ascii=False) and boundary2
            and any(r.get("msgid") == "sim-wc-204" for r in spool3),
            f"frames={len(frames)}"))
        frames = await run_wecom(mod, gw, "/stop", "sim-wc-205")
        text = json.dumps(frames, ensure_ascii=False)
        results.append(check("B04 无任务时 /stop 不谎称停止",
            "当前没有正在跑的任务" in text and "已停止" not in text,
            f"frames={len(frames)}"))
    print(f"-- {channel}: {sum(results)}/{len(results)} 通过，沙箱在 {sbx / channel}")
    return results


async def single_message(channels, sbx, text):
    for channel in channels:
        print(f"\n== {channel} 发：{text} ==")
        if channel == "weixin":
            mod, gw, state = make_weixin(sbx)
            acks = await run_weixin(mod, gw, text, 200001)
            print(f"回执 acks: {acks}")
            print(f"spool: {read_jsonl(sbx / 'weixin-bridge' / 'spool' / 'weixin.jsonl')}")
            print(f"inbox: {read_jsonl(state / 'inbox.jsonl')}")
        else:
            mod, gw, state = make_wecom(sbx)
            frames = await run_wecom(mod, gw, text, "sim-single-1")
            print(f"平台帧: {json.dumps(frames, ensure_ascii=False)[:500]}")
            print(f"spool: {read_jsonl(sbx / 'wecom-bridge' / 'spool' / 'wecom.jsonl')}")
            print(f"inbox: {read_jsonl(state / 'inbox.jsonl')}")


async def interactive(channels, sbx):
    print("交互模式：每行是一条模拟消息，发给默认双渠道；空行或 quit 退出。")
    print("斜杠如 /ping、/new 会走网关本地命令；普通文本会进沙箱 spool/inbox。")
    n = 0
    while True:
        try:
            line = input("sim> ").strip()
        except EOFError:
            break
        if not line or line.lower() == "quit":
            break
        n += 1
        for channel in channels:
            if channel == "weixin":
                mod, gw, state = make_weixin(sbx)
                acks = await run_weixin(mod, gw, line, 300000 + n)
                print(f"[weixin] 回执: {acks}  msgs_received={gw.msgs_received}")
            else:
                mod, gw, state = make_wecom(sbx)
                frames = await run_wecom(mod, gw, line, f"sim-int-{n}")
                print(f"[wecom] 平台帧数: {len(frames)}  msgs_received={gw.msgs_received}")
    print(f"交互结束，共 {n} 条，沙箱在 {sbx}")


def main():
    ap = argparse.ArgumentParser(description="模拟微信/企微收发消息（沙箱，不动生产）")
    ap.add_argument("--channels", default="weixin,wecom", help="默认 weixin,wecom，可写 weixin 或 wecom")
    ap.add_argument("--message", default="", help="只发一条自定义消息，不跑固定场景")
    ap.add_argument("--interactive", action="store_true", help="交互进程模式")
    ap.add_argument("--outdir", default="", help="沙箱目录，默认 /tmp/channel-sim-XXXX")
    ap.add_argument("--keep", action="store_true", help="跑完不删沙箱")
    args = ap.parse_args()
    channels = [c.strip() for c in args.channels.split(",") if c.strip()]
    for c in channels:
        if c not in ("weixin", "wecom"):
            print(f"未知渠道: {c}（只支持 weixin / wecom）")
            return 2
    if args.outdir:
        sbx = Path(args.outdir)
        sbx.mkdir(parents=True, exist_ok=True)
    else:
        sbx = Path(tempfile.mkdtemp(prefix="channel-sim-"))
    print(f"沙箱: {sbx}（生产状态不动，跑完{'保留' if args.keep or args.outdir else '删除'}）")
    try:
        if args.interactive:
            asyncio.run(interactive(channels, sbx))
            return 0
        if args.message:
            asyncio.run(single_message(channels, sbx, args.message))
            return 0
        all_results = []
        for channel in channels:
            all_results.extend(asyncio.run(scenario(channel, sbx)))
        print(f"\n总计: {sum(all_results)}/{len(all_results)} 通过")
        return 0 if all(all_results) else 1
    finally:
        if not args.keep and not args.outdir:
            shutil.rmtree(sbx, ignore_errors=True)
            print(f"沙箱已清理: {sbx}")


if __name__ == "__main__":
    raise SystemExit(main())
