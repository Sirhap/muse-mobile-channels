#!/usr/bin/env python3
"""CLI for the Weixin gateway: queue replies/sends, inspect state."""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from channel_common import (  # noqa: E402
    append_jsonl,
    load_json_dict,
    muse_home,
    pid_alive,
    read_pid_file,
    register_cancel,
    reply_block_reason_for_row,
    safe_child_name,
    subagent_outcome_default,
)

BASE = Path(__file__).resolve().parent
STATE = BASE / "state"
INBOX = STATE / "inbox.jsonl"
OUTBOX = STATE / "outbox.jsonl"
OUTBOX_RESULTS = STATE / "outbox_results.jsonl"
OUTBOX_RETRY = STATE / "outbox_retry.json"
STATUS = STATE / "status.json"
LOGIN_STATE = STATE / "login.json"
HOOK_STATE = muse_home() / "hooks" / "state" / "weixin-bot"


def read_text_arg(args) -> str:
    if args.text_file:
        return Path(args.text_file).read_text(encoding="utf-8")
    return args.text or ""


# --- Reply idempotency guard (fix #1, 2026-10-04) -----------------------
# A formal reply (mode="reply") for a msgid may be queued only once while
# a previous one is delivered-ok or still plausibly in flight. This stops
# parallel/orphan-takeover workers from each submitting their own formal
# reply to the same user message. Delivery results live in
# outbox_results.jsonl, keyed by the outbox row's "id", with an "ok" flag.
REPLY_INFLIGHT_WINDOW_S = 120


def reply_block_reason(msgid: str) -> str | None:
    """Return a human-readable block reason, or None if the reply may go.

    Fail-open: any error reading state returns None (with a stderr
    warning) so a broken guard never wedges legitimate replies.
    """
    try:
        results: dict[str, dict] = {}
        if OUTBOX_RESULTS.exists():
            for line in OUTBOX_RESULTS.read_text(encoding="utf-8").splitlines():
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get("id"):
                    results[r["id"]] = r
        now = time.time()
        retry = load_json_dict(OUTBOX_RETRY)
        if OUTBOX.exists():
            for line in OUTBOX.read_text(encoding="utf-8").splitlines():
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                reason = reply_block_reason_for_row(
                    msgid, o, results.get(o.get("id")), now, retry,
                    REPLY_INFLIGHT_WINDOW_S,
                )
                if reason:
                    return reason
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

def _forced_subagent_label(jid, content, status=None):
    """Return content whose first line opens with the authoritative
    label 【副助手 #<jid> <outcome>】. A missing label follows the job
    status and the reply text instead of always claiming 完成."""
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
        outcome = subagent_outcome_default(text, status)
        return f"【副助手 #{jid} {outcome}】\n" + text
    new_first = f"【副助手 #{jid} {outcome}】" + (f" {tail}" if tail else "")
    return new_first + (sep + rest if sep else "")


def _subagent_job_id_for_msgid(msgid):
    """Authoritative job id for a msgid from the hook-owned jobs file
    (read-only here); None for ordinary (non-job) msgids."""
    try:
        p = HOOK_STATE / "subagent_jobs.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        jobs = data.get("jobs") if isinstance(data, dict) else None
        if isinstance(jobs, dict):
            for jid, rec in jobs.items():
                if isinstance(rec, dict) and str(rec.get("msgid")) == str(msgid):
                    return str(jid), rec.get("status")
    except Exception:
        pass
    return None


def normalize_subagent_reply(msgid, content):
    found = _subagent_job_id_for_msgid(msgid)
    if not found:
        return content
    jid, status = found
    return _forced_subagent_label(jid, content, status)

def queue(item: dict) -> str:
    """Append one outbox row under the shared jsonl lock."""
    item_id = uuid.uuid4().hex[:12]
    item["id"] = item_id
    item["queued_at"] = int(time.time())
    append_jsonl(OUTBOX, item)
    return item_id


def tail_jsonl(path: Path, n: int) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines()[-n:]:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def main() -> int:
    p = argparse.ArgumentParser(prog="weixin")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("reply")
    pr.add_argument("--msgid", required=True)
    pr.add_argument("--text")
    pr.add_argument("--text-file")
    pr.add_argument("--force", action="store_true",
                    help="bypass the duplicate-reply idempotency check")

    ps = sub.add_parser("send")
    ps.add_argument("--to", required=True, dest="to_user_id")
    ps.add_argument("--text")
    ps.add_argument("--text-file")

    pu = sub.add_parser("update")
    pu.add_argument("--msgid", required=True)
    pu.add_argument("--text")
    pu.add_argument("--text-file")
    pu.add_argument("--force", action="store_true",
                    help="bypass the late-update suppression check")

    pf = sub.add_parser("reply-file")
    pf.add_argument("--msgid", required=True)
    pf.add_argument("--file", required=True)
    pf.add_argument("--text", help="caption used to close the thinking bubble")
    pf.add_argument("--force", action="store_true",
                    help="bypass the late-reply suppression check")

    psf = sub.add_parser("send-file")
    psf.add_argument("--to", required=True, dest="to_user_id")
    psf.add_argument("--file", required=True)
    psf.add_argument("--text", help="optional caption sent before the file")

    sub.add_parser("status")
    sub.add_parser("login-status")

    pi = sub.add_parser("inbox")
    pi.add_argument("--limit", type=int, default=5)

    pz = sub.add_parser("results")
    pz.add_argument("--limit", type=int, default=5)
    pcn = sub.add_parser("cancel")
    pcn.add_argument("--msgid", required=True)
    sub.add_parser("cancelled")

    phb = sub.add_parser("heartbeat-start")
    phb.add_argument("--msgids", required=True,
                     help="comma-separated msgids of the batch being worked on")
    phs = sub.add_parser("heartbeat-stop")
    phs.add_argument("--msgids", required=True,
                     help="comma-separated msgids whose heartbeat should stop")


    args = p.parse_args()

    if args.cmd == "reply":
        content = read_text_arg(args)
        if not content:
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
        item_id = queue({"mode": "reply", "msgid": args.msgid, "content": content})
        print(f"queued reply id={item_id} for msgid={args.msgid}")
    elif args.cmd == "send":
        content = read_text_arg(args)
        if not content:
            print("empty send text", file=sys.stderr)
            return 2
        item_id = queue({"mode": "send", "to_user_id": args.to_user_id, "content": content})
        print(f"queued send id={item_id} to {args.to_user_id}")
    elif args.cmd == "update":
        content = read_text_arg(args)
        if not content:
            print("empty update text", file=sys.stderr)
            return 2
        # Late-update suppression (fix, 2026-10-04 evening): a progress
        # update for a msgid that already has a delivered (or in-flight)
        # formal reply is by definition late — it is the visible
        # "补答" duplicate the user kept receiving when an orphan
        # takeover raced a still-alive original worker (the formal
        # reply gate alone could not stop it, because updates were
        # never gated). Reuse the reply gate's state check.
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
        item_id = queue({"mode": "update", "msgid": args.msgid, "content": content})
        print(f"queued update id={item_id} for msgid={args.msgid}")
    elif args.cmd == "reply-file":
        if not Path(args.file).exists():
            print(f"file not found: {args.file}", file=sys.stderr)
            return 2
        # Same late suppression as update: a reply_file is a formal
        # closing delivery, so it must not land after a formal reply.
        if not args.force:
            reason = reply_block_reason(args.msgid)
            if reason:
                print(f"ERROR: late reply-file blocked: {reason}. "
                      "The message is already answered; deliver any "
                      "genuinely new file with `send-file` instead.",
                      file=sys.stderr)
                return 3
        item = {"mode": "reply_file", "msgid": args.msgid, "file_path": args.file}
        if args.text:
            item["content"] = args.text
        item_id = queue(item)
        print(f"queued reply-file id={item_id} for msgid={args.msgid}")
    elif args.cmd == "send-file":
        if not Path(args.file).exists():
            print(f"file not found: {args.file}", file=sys.stderr)
            return 2
        item = {"mode": "send_file", "to_user_id": args.to_user_id, "file_path": args.file}
        if args.text:
            item["content"] = args.text
        item_id = queue(item)
        print(f"queued send-file id={item_id} to {args.to_user_id}")
    elif args.cmd == "cancel":
        register_cancel(STATE / "cancelled.json", args.msgid)
        print(json.dumps({"cancelled": args.msgid}, ensure_ascii=False))
    elif args.cmd == "cancelled":
        cpath = STATE / "cancelled.json"
        rows = []
        if cpath.exists():
            try:
                rows = json.loads(cpath.read_text(encoding="utf-8"))
            except Exception:
                rows = []
        print(json.dumps(rows, ensure_ascii=False))
    elif args.cmd == "heartbeat-start":
        # Spawn the detached heartbeat loop for this batch (see
        # heartbeat.py). While it runs, the inbox hook counts the
        # batch as alive even if the worker sends no progress: death
        # is judged by heartbeat + outbox, not by silence alone.
        msgids = [m.strip() for m in args.msgids.split(",") if m.strip()]
        if not msgids:
            print("no msgids given", file=sys.stderr)
            return 2
        if any(safe_child_name(m) is None for m in msgids):
            print("refusing msgid that is not a single path segment", file=sys.stderr)
            return 2
        hb_dir = STATE / "heartbeats"
        hb_dir.mkdir(parents=True, exist_ok=True)
        for m in msgids:
            try:
                (hb_dir / f"{m}.stop").write_text(str(time.time()), encoding="utf-8")
            except OSError:
                pass
        deadline = time.time() + 3
        while time.time() < deadline:
            alive = False
            for m in msgids:
                pid = read_pid_file(hb_dir / f"{m}.pid") or 0
                if pid_alive(pid):
                    alive = True
            if not alive:
                break
            time.sleep(0.1)
        for m in msgids:
            try:
                (hb_dir / f"{m}.stop").unlink()
            except OSError:
                pass
        worker_pid = os.getppid()
        for m in msgids:
            try:
                (hb_dir / f"{m}.worker").write_text(str(worker_pid), encoding="utf-8")
            except OSError:
                pass
        subprocess.Popen(
            [sys.executable, str(BASE / "heartbeat.py"),
             str(STATE), str(HOOK_STATE), ",".join(msgids), str(worker_pid)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, start_new_session=True)
        print(json.dumps({"heartbeat_started": msgids, "worker_pid": worker_pid}, ensure_ascii=False))
    elif args.cmd == "heartbeat-stop":
        msgids = [m.strip() for m in args.msgids.split(",") if m.strip()]
        if any(safe_child_name(m) is None for m in msgids):
            print("refusing msgid that is not a single path segment", file=sys.stderr)
            return 2
        hb_dir = STATE / "heartbeats"
        hb_dir.mkdir(parents=True, exist_ok=True)
        for m in msgids:
            try:
                (hb_dir / f"{m}.stop").write_text(str(time.time()), encoding="utf-8")
            except OSError:
                pass
        print(json.dumps({"heartbeat_stopped": msgids}, ensure_ascii=False))
    elif args.cmd == "status":
        if STATUS.exists():
            print(STATUS.read_text(encoding="utf-8"))
        else:
            print('{"state": "not started"}')
    elif args.cmd == "login-status":
        if LOGIN_STATE.exists():
            print(LOGIN_STATE.read_text(encoding="utf-8"))
        else:
            print('{"status": "no login attempted"}')
    elif args.cmd == "inbox":
        for e in tail_jsonl(INBOX, args.limit):
            print(json.dumps(e, ensure_ascii=False))
    elif args.cmd == "results":
        for e in tail_jsonl(OUTBOX_RESULTS, args.limit):
            print(json.dumps(e, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
