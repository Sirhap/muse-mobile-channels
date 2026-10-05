#!/usr/bin/env python3
"""CLI for the WeCom gateway: queue replies / proactive sends, inspect state.

The gateway daemon owns the single long connection; this CLI only talks
to it through files in state/ (outbox.jsonl is consumed by the daemon,
results land in outbox_results.jsonl).
"""

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
RESULTS = STATE / "outbox_results.jsonl"
STATUS = STATE / "status.json"
HOOK_STATE = Path.home() / "hooks" / "state" / "wecom-bot"


def read_text_arg(args) -> str:
    if getattr(args, "text_file", None):
        return Path(args.text_file).read_text(encoding="utf-8")
    return args.text or ""


# --- Reply idempotency guard (fix #1, 2026-10-04) -----------------------
# A formal reply (mode="reply") for a msgid may be queued only once while
# a previous one is delivered-ok or still plausibly in flight. This stops
# parallel/orphan-takeover workers from each submitting their own formal
# reply to the same user message. Delivery results live in
# outbox_results.jsonl, keyed by the outbox row's "id", with an "ok" flag
# (same shape as the weixin channel; errcode/errmsg are informational).
REPLY_INFLIGHT_WINDOW_S = 120


def reply_block_reason(msgid: str) -> str | None:
    """Return a human-readable block reason, or None if the reply may go.

    Fail-open: any error reading state returns None (with a stderr
    warning) so a broken guard never wedges legitimate replies.
    """
    try:
        results: dict[str, dict] = {}
        if RESULTS.exists():
            for line in RESULTS.read_text(encoding="utf-8").splitlines():
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("id"):
                    results[r["id"]] = r
        now = time.time()
        if OUTBOX.exists():
            for line in OUTBOX.read_text(encoding="utf-8").splitlines():
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if o.get("mode") != "reply" or str(o.get("msgid")) != str(msgid):
                    continue
                res = results.get(o.get("id"))
                if res is not None:
                    if res.get("ok") is True:
                        return (
                            f"msgid {msgid} already has a formal reply that was "
                            f"delivered successfully (outbox id {o.get('id')})"
                        )
                    # ok=false: a failed delivery may legitimately be retried.
                else:
                    age = now - float(o.get("queued_at") or now)
                    if age < REPLY_INFLIGHT_WINDOW_S:
                        return (
                            f"msgid {msgid} already has a formal reply queued "
                            f"{int(age)}s ago with no delivery result yet "
                            f"(likely still in flight, outbox id {o.get('id')})"
                        )
    except Exception as e:  # guard must never block on its own failure
        print(f"warning: reply idempotency check failed ({e}); allowing reply",
              file=sys.stderr)
    return None



# --- Subagent reply label enforcement (fix #2, 2026-10-04) -----
# A formal reply bound to a /subagent job msgid MUST open with
# 【副助手 #S<n> 完成/失败/已停止】. Workers kept forgetting it
# (S1/S3 arrived unlabeled), so it is enforced here at the
# delivery entrance, and again in the gateway before sending.
# Non-job replies are never touched. Fail-open on read errors.

def _forced_subagent_label(jid, content):
    """Return content whose first line opens with the authoritative
    label 【副助手 #<jid> <outcome>】. An existing label keeps its
    outcome word (完成/失败/已停止) and any text after it, with the job
    id corrected to jid; a missing label is prepended as 完成."""
    text = content or ""
    if not text.strip():
        return text
    first, sep, rest = text.partition("\n")
    head = first.strip()
    outcome = None
    tail = ""
    if head.startswith("【副助手") and "】" in head:
        end = head.index("】")
        inner = head[1:end]
        for oc in ("已停止", "完成", "失败"):
            if oc in inner:
                outcome = oc
                tail = head[end + 1:].strip()
                break
    if outcome is None:
        return f"【副助手 #{jid} 完成】\n" + text
    new_first = f"【副助手 #{jid} {outcome}】" + (f" {tail}" if tail else "")
    return new_first + (sep + rest if sep else "")


def _subagent_job_id_for_msgid(msgid):
    """Authoritative job id for a msgid from the hook-owned jobs file
    (read-only here); None for ordinary (non-job) msgids."""
    try:
        p = Path.home() / "hooks" / "state" / BASE.name / "subagent_jobs.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        jobs = data.get("jobs") if isinstance(data, dict) else None
        if isinstance(jobs, dict):
            for jid, rec in jobs.items():
                if isinstance(rec, dict) and str(rec.get("msgid")) == str(msgid):
                    return str(jid)
    except Exception:
        pass
    return None


def normalize_subagent_reply(msgid, content):
    jid = _subagent_job_id_for_msgid(msgid)
    if not jid:
        return content
    return _forced_subagent_label(jid, content)

def queue(item: dict) -> str:
    item = dict(item)
    item["id"] = uuid.uuid4().hex[:12]
    item["queued_at"] = int(time.time())
    STATE.mkdir(parents=True, exist_ok=True)
    with OUTBOX.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")
    return item["id"]


def main() -> int:
    p = argparse.ArgumentParser(prog="wecom")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("reply", help="reply to a received message (by msgid)")
    pr.add_argument("--msgid", required=True)
    pr.add_argument("--text")
    pr.add_argument("--text-file")
    pr.add_argument("--force", action="store_true",
                    help="bypass the duplicate-reply idempotency check")

    ps = sub.add_parser("send", help="proactively send a markdown message to a chat")
    ps.add_argument("--chatid", required=True)
    ps.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    ps.add_argument("--text")
    ps.add_argument("--text-file")

    pu = sub.add_parser("update", help="refresh the open stream bubble for a message (progress)")
    pu.add_argument("--msgid", required=True)
    pu.add_argument("--text")
    pu.add_argument("--text-file")
    pu.add_argument("--force", action="store_true",
                    help="bypass the late-update suppression check")

    pf = sub.add_parser("reply-file", help="reply to a message with an image/file upload")
    pf.add_argument("--msgid", required=True)
    pf.add_argument("--file", required=True)
    pf.add_argument("--force", action="store_true",
                    help="bypass the late-reply suppression check")

    psf = sub.add_parser("send-file", help="proactively send an image/file to a chat")
    psf.add_argument("--chatid", required=True)
    psf.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    psf.add_argument("--file", required=True)

    pc = sub.add_parser("confirm", help="proactively send a 确认/取消 template card to a chat")
    pc.add_argument("--chatid", required=True)
    pc.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pc.add_argument("--title", required=True)
    pc.add_argument("--text", help="card description (sub_title_text)")
    pc.add_argument("--task-id")
    pc.add_argument("--select-title", help="optional dropdown selector title on the button card")
    pc.add_argument("--select-options", help="dropdown options, comma-separated texts")

    pct = sub.add_parser("card-text", help="proactively send a text_notice （文字通知） card")
    pct.add_argument("--chatid", required=True)
    pct.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pct.add_argument("--title", required=True)
    pct.add_argument("--text", help="card description (sub_title_text)")
    pct.add_argument("--url")
    pct.add_argument("--task-id")

    pcnws = sub.add_parser("card-news", help="proactively send a news_notice （图文展示） card")
    pcnws.add_argument("--chatid", required=True)
    pcnws.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pcnws.add_argument("--title", required=True)
    pcnws.add_argument("--text", help="card body text")
    pcnws.add_argument("--image-url", required=True)
    pcnws.add_argument("--url")
    pcnws.add_argument("--task-id")

    pcv = sub.add_parser("card-vote", help="proactively send a vote_interaction （投票选择） card")
    pcv.add_argument("--chatid", required=True)
    pcv.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pcv.add_argument("--title", required=True)
    pcv.add_argument("--text", help="card description (sub_title_text)")
    pcv.add_argument("--options", required=True, help="option texts, comma-separated")
    pcv.add_argument("--multi", action="store_true", help="allow multiple choices")
    pcv.add_argument("--task-id")

    pcm = sub.add_parser("card-multiple", help="proactively send a multiple_interaction （多项选择） card")
    pcm.add_argument("--chatid", required=True)
    pcm.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pcm.add_argument("--title", required=True)
    pcm.add_argument("--text", help="card description (sub_title_text)")
    pcm.add_argument("--group", action="append", required=True,
                     help="one selector group: '标题=选项1,选项2' (repeat up to 3)")
    pcm.add_argument("--task-id")

    pcj = sub.add_parser("card-json", help="proactively send a template card from a JSON file (full card dict)")
    pcj.add_argument("--chatid", required=True)
    pcj.add_argument("--chat-type", type=int, default=1, choices=[1, 2])
    pcj.add_argument("--file", required=True)

    prc = sub.add_parser("reply-confirm", help="reply to a message with a 确认/取消 template card")
    prc.add_argument("--msgid", required=True)
    prc.add_argument("--title", required=True)
    prc.add_argument("--text", help="stream text shown with the card")
    prc.add_argument("--desc", help="card description (sub_title_text)")
    prc.add_argument("--task-id")

    pcn = sub.add_parser("cancel", help="register a user cancellation for a running task (by msgid)")
    pcn.add_argument("--msgid", required=True)
    sub.add_parser("cancelled", help="list registered cancellations")
    phb = sub.add_parser("heartbeat-start", help="start the internal heartbeat for a batch")
    phb.add_argument("--msgids", required=True,
                     help="comma-separated msgids of the batch being worked on")
    phs = sub.add_parser("heartbeat-stop", help="stop the internal heartbeat for a batch")
    phs.add_argument("--msgids", required=True,
                     help="comma-separated msgids whose heartbeat should stop")
    sub.add_parser("status", help="show gateway status")

    pi = sub.add_parser("inbox", help="show recent inbox entries")
    pi.add_argument("--limit", type=int, default=10)

    prr = sub.add_parser("results", help="show recent outbox results")
    prr.add_argument("--limit", type=int, default=10)

    args = p.parse_args()

    if args.cmd == "reply":
        content = read_text_arg(args)
        if not content.strip():
            print("empty reply text", file=sys.stderr)
            return 2
        if not args.force:
            reason = reply_block_reason(args.msgid)
            if reason:
                print(f"ERROR: duplicate reply blocked: {reason}. "
                      "Do NOT send a second formal reply to the same message. "
                      "If you have genuinely new supplementary content, deliver "
                      "it with the `send` command instead, prefixed as a late "
                      "supplement (e.g. 「补充：…」). "
                      "To force this reply anyway, re-run with --force.",
                      file=sys.stderr)
                return 3
        content = normalize_subagent_reply(args.msgid, content)
        qid = queue({"mode": "reply", "msgid": args.msgid, "content": content})
        print(f"queued reply id={qid} for msgid={args.msgid}")
        return 0

    if args.cmd == "send":
        content = read_text_arg(args)
        if not content.strip():
            print("empty message text", file=sys.stderr)
            return 2
        qid = queue({"mode": "send", "chatid": args.chatid, "chat_type": args.chat_type, "content": content})
        print(f"queued send id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "update":
        content = read_text_arg(args)
        if not content.strip():
            print("empty update text", file=sys.stderr)
            return 2
        # Late-update suppression (fix, 2026-10-04 evening, mirroring
        # the weixin channel): a progress update for a msgid that
        # already has a delivered (or in-flight) formal reply is by
        # definition late — the visible "补答" duplicate. The formal
        # reply gate alone could not stop it because updates were
        # never gated. Reuse the reply gate's state check.
        if not args.force:
            reason = reply_block_reason(args.msgid)
            if reason:
                print(f"ERROR: late update blocked: {reason}. "
                      "The message is already answered; do NOT send "
                      "progress updates for it. If you have genuinely "
                      "new supplementary content, deliver it with the "
                      "`send` command instead, prefixed as a late "
                      "supplement (e.g. 「补充：…」).",
                      file=sys.stderr)
                return 3
        qid = queue({"mode": "update", "msgid": args.msgid, "content": content})
        print(f"queued update id={qid} for msgid={args.msgid}")
        return 0

    if args.cmd == "reply-file":
        if not Path(args.file).exists():
            print(f"file not found: {args.file}", file=sys.stderr)
            return 2
        # Same late suppression as update: a reply_file is a formal
        # closing delivery and must not land after a formal reply.
        if not args.force:
            reason = reply_block_reason(args.msgid)
            if reason:
                print(f"ERROR: late reply-file blocked: {reason}. "
                      "The message is already answered; deliver any "
                      "genuinely new file with `send-file` instead.",
                      file=sys.stderr)
                return 3
        qid = queue({"mode": "reply_file", "msgid": args.msgid, "file_path": args.file})
        print(f"queued reply-file id={qid} for msgid={args.msgid}")
        return 0

    if args.cmd == "send-file":
        if not Path(args.file).exists():
            print(f"file not found: {args.file}", file=sys.stderr)
            return 2
        qid = queue({"mode": "send_file", "chatid": args.chatid, "chat_type": args.chat_type, "file_path": args.file})
        print(f"queued send-file id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "confirm":
        item = {"mode": "send_confirm", "chatid": args.chatid, "chat_type": args.chat_type,
                "title": args.title, "content": args.text or ""}
        if args.task_id:
            item["task_id"] = args.task_id
        if args.select_options:
            item["selection_title"] = args.select_title or "请选择"
            item["selection_options"] = [s.strip() for s in args.select_options.replace("，", ",").split(",") if s.strip()]
        qid = queue(item)
        print(f"queued confirm id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "card-text":
        item = {"mode": "send_text_notice", "chatid": args.chatid, "chat_type": args.chat_type,
                "title": args.title, "content": args.text or ""}
        if args.url:
            item["url"] = args.url
        if args.task_id:
            item["task_id"] = args.task_id
        qid = queue(item)
        print(f"queued card-text id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "card-news":
        item = {"mode": "send_news_notice", "chatid": args.chatid, "chat_type": args.chat_type,
                "title": args.title, "content": args.text or "", "image_url": args.image_url}
        if args.url:
            item["url"] = args.url
        if args.task_id:
            item["task_id"] = args.task_id
        qid = queue(item)
        print(f"queued card-news id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "card-vote":
        item = {"mode": "send_vote", "chatid": args.chatid, "chat_type": args.chat_type,
                "title": args.title, "content": args.text or "",
                "options": [s.strip() for s in args.options.replace("，", ",").split(",") if s.strip()],
                "multi": bool(args.multi)}
        if args.task_id:
            item["task_id"] = args.task_id
        qid = queue(item)
        print(f"queued card-vote id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "card-multiple":
        groups = []
        for g in args.group:
            gtitle, _, gopts = g.partition("=")
            groups.append({"title": gtitle.strip(),
                           "options": [s.strip() for s in gopts.replace("，", ",").split(",") if s.strip()]})
        item = {"mode": "send_multiple", "chatid": args.chatid, "chat_type": args.chat_type,
                "title": args.title, "content": args.text or "", "groups": groups}
        if args.task_id:
            item["task_id"] = args.task_id
        qid = queue(item)
        print(f"queued card-multiple id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "card-json":
        try:
            card = json.loads(Path(args.file).read_text(encoding="utf-8"))
        except Exception as e:
            print(f"bad card json: {e}", file=sys.stderr)
            return 2
        item = {"mode": "send_card", "chatid": args.chatid, "chat_type": args.chat_type, "card": card}
        qid = queue(item)
        print(f"queued card-json id={qid} to chatid={args.chatid}")
        return 0

    if args.cmd == "reply-confirm":
        item = {"mode": "reply_confirm", "msgid": args.msgid,
                "title": args.title, "content": args.text or "", "desc": args.desc or ""}
        if args.task_id:
            item["task_id"] = args.task_id
        qid = queue(item)
        print(f"queued reply-confirm id={qid} for msgid={args.msgid}")
        return 0

    if args.cmd == "cancel":
        cpath = STATE / "cancelled.json"
        rows = []
        if cpath.exists():
            try:
                rows = json.loads(cpath.read_text(encoding="utf-8"))
            except Exception:
                rows = []
        if not any(r.get("msgid") == args.msgid for r in rows):
            rows.append({"msgid": args.msgid, "ts": time.time()})
            cpath.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        print(json.dumps({"cancelled": args.msgid}, ensure_ascii=False))
    if args.cmd == "cancelled":
        cpath = STATE / "cancelled.json"
        rows = []
        if cpath.exists():
            try:
                rows = json.loads(cpath.read_text(encoding="utf-8"))
            except Exception:
                rows = []
        print(json.dumps(rows, ensure_ascii=False))
    if args.cmd == "heartbeat-start":
        # Spawn the detached heartbeat loop for this batch (see
        # heartbeat.py). While it runs, the inbox hook counts the
        # batch as alive even if the worker sends no progress: death
        # is judged by heartbeat + outbox, not by silence alone.
        import subprocess
        msgids = [m.strip() for m in args.msgids.split(",") if m.strip()]
        if not msgids:
            print("no msgids given", file=sys.stderr)
            return 2
        hb_dir = STATE / "heartbeats"
        hb_dir.mkdir(parents=True, exist_ok=True)
        for m in msgids:
            try:
                (hb_dir / f"{m}.stop").unlink()
            except OSError:
                pass
        subprocess.Popen(
            [sys.executable, str(BASE / "heartbeat.py"),
             str(STATE), str(HOOK_STATE), ",".join(msgids)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True)
        print(json.dumps({"heartbeat_started": msgids}, ensure_ascii=False))
        return 0
    if args.cmd == "heartbeat-stop":
        msgids = [m.strip() for m in args.msgids.split(",") if m.strip()]
        hb_dir = STATE / "heartbeats"
        for m in msgids:
            try:
                (hb_dir / f"{m}.stop").write_text(str(time.time()), encoding="utf-8")
            except OSError:
                pass
        print(json.dumps({"heartbeat_stopped": msgids}, ensure_ascii=False))
        return 0
    if args.cmd == "status":
        if STATUS.exists():
            print(STATUS.read_text(encoding="utf-8"))
        else:
            print("no status yet (gateway not started)")
        return 0

    if args.cmd in ("inbox", "results"):
        path = INBOX if args.cmd == "inbox" else RESULTS
        if not path.exists():
            print("(empty)")
            return 0
        lines = path.read_text(encoding="utf-8").splitlines()[-args.limit:]
        for line in lines:
            print(line)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
