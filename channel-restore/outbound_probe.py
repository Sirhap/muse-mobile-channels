#!/usr/bin/env python3
"""Outbound delivery probe for channel-restore (2026-10-14, todo 16).

Why this exists: channel-restore proved the gateway *processes* are
alive, but nothing watched whether they can still *deliver*. On
2026-10-13 23:59 UTC the WeChat gateway's unbound sends started
failing with iLink ret=-2 "prepare failed" and kept failing for 8+
hours while every service check stayed green; the fault was only
noticed because the bridge's hourly credential alert happened to
ride that channel. This probe closes that blind spot.

Rule (per channel, from state/outbox_results.jsonl): walk newest ->
oldest; the failure streak is the run of ok=false rows before the
first ok=true. The channel is BROKEN when the streak has >= 3
failures, the newest failure is <= 24h old, and the streak either
spans >= 900s or has >= 5 failures (a lone burst of 3 quick retries
of one message is not yet an outage; an hourly-cadence sender
reaches span quickly, a retrying gateway reaches count quickly).

Alerting: once per episode (episode = streak start ts), delivered
through the OTHER channel's CLI (never through the broken one), to
the channel owner (last from_user_id in that channel's inbox). A
delivered ok clears nothing; the episode closes when a success
becomes the newest result, after which a new streak can alert
again. Undeliverable alerts retry at most every 30 minutes and are
always echoed to stdout so the restore journal carries them.

Advisory only: always exits 0. `--dry-run` prints decisions as JSON
and never sends or writes state.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

CHANNELS = ("weixin", "wecom")
MIN_STREAK = 3
MIN_STREAK_BIG = 5
MIN_SPAN_SECS = 900
FRESH_SECS = 86400  # a broken channel goes quiet once its senders
# give up or dead-letter (the 2026-10-13 fault's newest attempt was
# 3.6h old when this probe first ran); recovery is proven by a
# success row, not by silence, but week-old history stays quiet.
TAIL_ROWS = 300
ALERT_RETRY_SECS = 1800


def read_tail(path: Path, n: int) -> list:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows = []
    for line in lines[-n:]:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def assess(rows: list, now: float) -> dict:
    """Return the channel verdict for a result-row tail."""
    streak = []
    for row in reversed(rows):
        if row.get("ok") is True:
            break
        if row.get("ok") is False:
            streak.append(row)
    if not streak:
        return {"broken": False, "streak": 0}
    newest_ts = float(streak[0].get("ts") or 0)
    oldest_ts = float(streak[-1].get("ts") or 0)
    span = newest_ts - oldest_ts
    broken = (
        len(streak) >= MIN_STREAK
        and now - newest_ts <= FRESH_SECS
        and (span >= MIN_SPAN_SECS or len(streak) >= MIN_STREAK_BIG)
    )
    sample = next((r for r in streak
                   if r.get("ret") is not None
                   or r.get("errcode") is not None), streak[0])
    return {
        "broken": broken,
        "streak": len(streak),
        "span": int(span),
        "episode": oldest_ts,
        "newest_fail_ts": newest_ts,
        "sample_err": sample.get("errmsg") or "",
        "sample_ret": sample.get("ret", sample.get("errcode")),
    }


def owner_of(ws: Path, channel: str) -> str:
    rows = read_tail(ws / f"{channel}-bot" / "state" / "inbox.jsonl", 50)
    for row in reversed(rows):
        uid = row.get("from_user_id") or row.get("from_userid")
        if uid:
            return str(uid)
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ws", default=str(Path(__file__).resolve().parent.parent))
    ap.add_argument("--state", default=str(Path(__file__).resolve().parent
                                           / "outbound_probe_state.json"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--now", type=float, default=0)
    args = ap.parse_args()

    ws = Path(args.ws)
    now = args.now or time.time()
    state_path = Path(args.state)
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            state = {}
    except (OSError, json.JSONDecodeError):
        state = {}

    verdicts = {}
    for ch in CHANNELS:
        rows = read_tail(ws / f"{ch}-bot" / "state" / "outbox_results.jsonl",
                         TAIL_ROWS)
        verdicts[ch] = assess(rows, now)

    dirty = False
    for ch in CHANNELS:
        v = verdicts[ch]
        other = "wecom" if ch == "weixin" else "weixin"
        rec = state.get(ch) or {}
        if not v["broken"]:
            if rec:
                state.pop(ch, None)
                dirty = True
            print(f"outbound-probe: {ch} OK")
            continue
        episode = v["episode"]
        already = rec.get("episode") == episode and rec.get("alerted")
        retry_due = now - float(rec.get("last_try") or 0) >= ALERT_RETRY_SECS
        if already:
            print(f"outbound-probe: {ch} BROKEN streak={v['streak']} "
                  f"span={v['span']}s alert=already-sent")
            continue
        if not retry_due and not args.dry_run:
            print(f"outbound-probe: {ch} BROKEN streak={v['streak']} "
                  f"span={v['span']}s alert=retry-throttled")
            continue
        text = (f"⚠️ 出站探针：{ch} 渠道发送连续失败 {v['streak']} 次"
                f"（跨度约 {max(1, v['span'] // 60)} 分钟，最近错误 "
                f"ret={v['sample_ret']} {v['sample_err']}），服务进程本身"
                f"仍 active。可能是渠道侧发送会话失效，请留意。")
        decision = {"channel": ch, "via": other, "episode": episode,
                    "streak": v["streak"], "span": v["span"], "text": text}
        if args.dry_run:
            print(json.dumps(decision, ensure_ascii=False))
            continue
        owner = owner_of(ws, other)
        cli = ws / f"{other}-bot" / other
        sent = False
        if owner and cli.exists() and not verdicts[other]["broken"]:
            # The two CLIs take different recipient flags.
            if other == "wecom":
                argv = [str(cli), "send", "--chatid", owner,
                        "--chat-type", "1", "--text", text]
            else:
                argv = [str(cli), "send", "--to", owner, "--text", text]
            try:
                r = subprocess.run(argv, capture_output=True, timeout=30)
                sent = r.returncode == 0
            except Exception:
                sent = False
        state[ch] = {"episode": episode, "last_try": now,
                     "alerted": bool(sent)}
        dirty = True
        print(f"outbound-probe: {ch} BROKEN streak={v['streak']} "
              f"span={v['span']}s alert={'sent via ' + other if sent else 'UNDELIVERED'}")
    if dirty and not args.dry_run:
        try:
            state_path.write_text(json.dumps(state, ensure_ascii=False,
                                             indent=1), encoding="utf-8")
        except OSError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
