#!/usr/bin/env python3
"""reply_file completion fix — sandbox tests (both channels).

Regression suite for the 2026-10-06 incident in which an image
delivered via reply_file was later fail-stopped as if unanswered,
because hook supervision only counted mode=="reply" as completion.

Extended 2026-10-07 for delivered-only completion: a batch counts
as answered only when the gateway actually DELIVERED the reply
(ok=true in outbox_results.jsonl); a merely queued reply that was
cancelled/suppressed no longer closes the batch, and a formal
reply still retrying in the gateway park lane counts as activity.

S9  active batch + DELIVERED reply_file row, aged past cap
    -> batch silently done: no cancel, no failure send
S10 detached batch + DELIVERED reply_file row -> dropped silently
S11 msgid answered only via reply_file + starvation carried=3
    -> no solo wake (reply_file counts as bound)
S12 active batch + reply_file queued but result cancelled, old
    -> NOT completion (queued-but-undelivered still never counts
    as answered); and since the 2026-10-09 deathwatch order a
    batch with no heartbeat and no outbox activity for
    DEATH_WATCH_SECS (1800) is judged interrupted: fail-stop
    (cancel + one failure notice, batch closed). The 2026-10-07
    no-cap order still protects LIVE batches — no duration cap —
    only life-sign silence is judged.
S13 active batch + formal reply parked in the retry lane
    -> batch stays alive past cap: no fail-stop, not completed
S14 detached batch + DELIVERED bound send row -> dropped silently
S15 detached batch + reply queued but cancelled, old
    -> deathwatch fires before silent retirement: fail-stop
    (cancel + failure notice, detached dropped), NOT retired
    to the graveyard
S1c control: silent batch, no outbox rows -> deathwatch
    fail-stop at 1800s of life-sign silence (2026-10-09 order,
    superseding the no-judgement part of the 2026-10-07 order)

Fixture ages were rescaled on 2026-10-07 when the user raised
BATCH_CAP_SECS 600 -> 3600: "aged past cap" fixtures now use
3700s / 5000s so every scenario keeps its original meaning.
Those ages also exceed DEATH_WATCH_SECS (1800), so the three
scenarios above assert the deathwatch outcomes.

The hook scripts are copied into a sandbox with state paths and the
channel CLI redirected to a stub, then executed for real.
Hook script location: MUSE_HOOK_SCRIPTS_DIR env, defaulting to the
repo's hooks/scripts directory.
"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

NOW = time.time()
RESULTS = []
SCRIPTS = Path(os.environ.get(
    "MUSE_HOOK_SCRIPTS_DIR",
    str(Path(__file__).resolve().parent.parent / "hooks" / "scripts")))


def run_channel(chan, botdir, hookst, script_name, cli_relpath, entry_fn):
    SBX = f"/tmp/rfsbx-{chan}"
    BOT = f"{SBX}/{botdir}/state"
    HOOKST = f"{SBX}/home/hooks/state/{hookst}"

    def reset():
        shutil.rmtree(SBX, ignore_errors=True)
        os.makedirs(BOT, exist_ok=True)
        os.makedirs(HOOKST, exist_ok=True)
        open(f"{SBX}/cli.log", "w").close()
        src = (SCRIPTS / script_name).read_text(encoding="utf-8")
        src = src.replace(f"/home/hatch/workspace/{botdir}/state", BOT)
        src = src.replace(cli_relpath, f"{SBX}/stubcli")
        w("hook.sh", src)
        w("runtime.sh",
          'silent() { echo "DECISION silent: $1"; }\n'
          'wake() { echo "DECISION wake: $1"; }\n'
          'log() { :; }\n')
        w("stubcli",
          '#!/usr/bin/env bash\n'
          'echo "CALL: $*" >> ' + f"{SBX}/cli.log" + '\n'
          'if [[ "$1" == "cancelled" ]]; then echo "[]"; fi\n'
          'exit 0\n')
        os.chmod(f"{SBX}/stubcli", 0o755)
        w(f"{botdir}/state/cancelled.json", [])

    def w(rel, content):
        path = os.path.join(SBX, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str)
                    else json.dumps(content, ensure_ascii=False))

    def inbox(rows):
        w(f"{botdir}/state/inbox.jsonl",
          "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))

    def outbox(rows):
        w(f"{botdir}/state/outbox.jsonl",
          "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))

    def results(rows):
        w(f"{botdir}/state/outbox_results.jsonl",
          "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))

    def parked(obj):
        w(f"{botdir}/state/outbox_parked.json", obj)

    def hookst_w(name, content):
        w(f"home/hooks/state/{hookst}/{name}", content)

    def run_hook():
        env = dict(os.environ, HOME=f"{SBX}/home",
                   HATCH_HOOK_RUNTIME=f"{SBX}/runtime.sh",
                   HATCH_HOOK_DRY_RUN="0")
        r = subprocess.run(["bash", f"{SBX}/hook.sh"], env=env,
                           capture_output=True, text=True, timeout=60)
        return r.stdout + r.stderr

    def calls():
        return open(f"{SBX}/cli.log").read()

    def no_cancel_calls():
        # Since the 2026-10-07 stall-notice deployment a long-silent
        # batch legitimately triggers ONE cli "send" (the stall
        # notice); the no-fail-stop contract is that no "cancel" call
        # ever happens for these batches.
        return "CALL: cancel" not in calls()

    def retired():
        p = os.path.join(HOOKST, "detached_retired.jsonl")
        return open(p).read() if os.path.exists(p) else ""

    def batch():
        return json.loads(open(f"{HOOKST}/active_batch.json").read())

    def check(name, cond):
        RESULTS.append((f"{chan} {name}", bool(cond)))
        print(("PASS " if cond else "FAIL ") + f"{chan} {name}")

    # S9 active batch completed by DELIVERED reply_file
    reset()
    inbox([entry_fn("F1", NOW - 3700)])
    hookst_w("seen_msgids.txt", "F1\n")
    hookst_w("carried_msgids.txt", "F1\n")
    hookst_w("active_batch.json", {"msgids": ["F1"], "since": NOW - 3700, "detached": []})
    outbox([{"id": "RF1", "mode": "reply_file", "msgid": "F1", "queued_at": NOW - 3650}])
    results([{"id": "RF1", "mode": "reply_file", "ok": True, "ts": NOW - 3640}])
    out = run_hook()
    check("S9 no fail-stop calls", calls() == "")
    check("S9 silent", "DECISION silent" in out)
    check("S9 batch cleared", not batch().get("msgids"))

    # S10 detached batch completed by DELIVERED reply_file
    reset()
    inbox([entry_fn("F2", NOW - 3700)])
    hookst_w("seen_msgids.txt", "F2\n")
    hookst_w("carried_msgids.txt", "F2\n")
    hookst_w("active_batch.json",
             {"msgids": [], "since": NOW - 3700,
              "detached": [{"msgids": ["F2"], "since": NOW - 3700}]})
    outbox([{"id": "RF2", "mode": "reply_file", "msgid": "F2", "queued_at": NOW - 3650}])
    results([{"id": "RF2", "mode": "reply_file", "ok": True, "ts": NOW - 3640}])
    out = run_hook()
    check("S10 no fail-stop calls", calls() == "")
    check("S10 detached dropped", not batch().get("detached"))

    # S12 queued-but-cancelled reply is NOT completion -> fail-stop
    reset()
    inbox([entry_fn("F5", NOW - 5000)])
    hookst_w("seen_msgids.txt", "F5\n")
    hookst_w("carried_msgids.txt", "F5\n")
    hookst_w("active_batch.json", {"msgids": ["F5"], "since": NOW - 5000, "detached": []})
    outbox([{"id": "RF5", "mode": "reply_file", "msgid": "F5", "queued_at": NOW - 4950}])
    results([{"id": "RF5", "mode": "reply_file", "ok": False,
              "errmsg": "cancelled", "ts": NOW - 4940}])
    out = run_hook()
    # Deathwatch (2026-10-09): last life sign is the queued reply
    # row 4950s ago, no heartbeat -> judged interrupted: the batch
    # is fail-stopped (cancel + failure notice), never completed.
    check("S12 deathwatch fail-stop on cancelled reply",
          "CALL: cancel" in calls() and "任务执行失败" in calls())
    check("S12 batch closed by fail-stop, not completed",
          not batch().get("msgids"))

    # S13 formal reply parked in the retry lane keeps the batch alive
    reset()
    inbox([entry_fn("F6", NOW - 3700)])
    hookst_w("seen_msgids.txt", "F6\n")
    hookst_w("carried_msgids.txt", "F6\n")
    hookst_w("active_batch.json", {"msgids": ["F6"], "since": NOW - 3700, "detached": []})
    outbox([{"id": "PK6", "mode": "reply", "msgid": "F6", "queued_at": NOW - 3650}])
    results([{"id": "PK6", "mode": "reply", "ok": False,
              "errmsg": "prepare failed", "ts": NOW - 300}])
    parked({"PK6": {"item": {"id": "PK6", "mode": "reply", "msgid": "F6"},
                    "n": 3, "next": NOW + 100}})
    out = run_hook()
    check("S13 no fail-stop while reply parked", calls() == "")
    check("S13 batch still active (not completed)",
          batch().get("msgids") == ["F6"])

    # S14 detached batch completed by DELIVERED bound send row
    reset()
    inbox([entry_fn("F7", NOW - 3700)])
    hookst_w("seen_msgids.txt", "F7\n")
    hookst_w("carried_msgids.txt", "F7\n")
    hookst_w("active_batch.json",
             {"msgids": [], "since": NOW - 3700,
              "detached": [{"msgids": ["F7"], "since": NOW - 3700}]})
    outbox([{"id": "SD7", "mode": "send", "msgid": "F7", "queued_at": NOW - 3650}])
    results([{"id": "SD7", "mode": "send", "ok": True, "ts": NOW - 3640}])
    out = run_hook()
    check("S14 no fail-stop calls", calls() == "")
    check("S14 detached dropped", not batch().get("detached"))

    # S15 detached reply queued but cancelled, old -> fail-stop
    reset()
    inbox([entry_fn("F8", NOW - 5000)])
    hookst_w("seen_msgids.txt", "F8\n")
    hookst_w("carried_msgids.txt", "F8\n")
    hookst_w("active_batch.json",
             {"msgids": [], "since": NOW - 5000,
              "detached": [{"msgids": ["F8"], "since": NOW - 5000}]})
    outbox([{"id": "RF8", "mode": "reply", "msgid": "F8", "queued_at": NOW - 4950}])
    results([{"id": "RF8", "mode": "reply", "ok": False,
              "errmsg": "cancelled", "ts": NOW - 4940}])
    out = run_hook()
    # Deathwatch (2026-10-09) supersedes silent retirement: a
    # detached batch with no life sign for 1800s is fail-stopped
    # (cancel + failure notice) BEFORE the 3600s retire window,
    # so the user is told instead of the batch vanishing quietly.
    check("S15 detached deathwatch fail-stop on cancelled reply",
          "CALL: cancel" in calls() and "任务执行失败" in calls())
    check("S15 detached dropped, NOT silently retired",
          not batch().get("detached") and "F8" not in retired())

    # S11 reply_file counts as bound for starvation
    reset()
    inbox([entry_fn("F3", NOW - 5000)])
    hookst_w("seen_msgids.txt", "F3\n")
    hookst_w("carried_msgids.txt", "F3\n")
    hookst_w("starvation.json", {"F3": {"carried": 3, "soloed": False}})
    outbox([{"mode": "reply_file", "msgid": "F3", "queued_at": NOW - 4900}])
    out = run_hook()
    check("S11 no starvation solo wake", "DECISION silent" in out)

    # S1c control: silent batch still fail-stops
    reset()
    inbox([entry_fn("D1", NOW - 3700)])
    hookst_w("seen_msgids.txt", "D1\n")
    hookst_w("carried_msgids.txt", "D1\n")
    hookst_w("active_batch.json", {"msgids": ["D1"], "since": NOW - 3700, "detached": []})
    out = run_hook()
    # Deathwatch (2026-10-09): 3700s with no heartbeat and no
    # outbox activity = no life sign -> fail-stop (cancel +
    # failure notice, batch closed). Live batches remain
    # uncapped; only silence is judged.
    check("S1c silent batch fail-stopped by deathwatch",
          "CALL: cancel" in calls() and "任务执行失败" in calls()
          and not batch().get("msgids"))


def wx_entry(mid, ts, text="发个图片"):
    return {"msgid": mid, "from_user_id": "user1", "text": text,
            "ts": ts, "media": []}


def wc_entry(mid, ts, text="发个图片"):
    return {"msgid": mid, "from_userid": "sirhao", "chattype": "single",
            "chatid": "sirhao", "msgtype": "text", "text": text,
            "ts": ts, "media": []}


run_channel("weixin", "weixin-bot", "weixin-bot",
            "weixin-inbox.sh",
            "/home/hatch/workspace/weixin-bot/weixin", wx_entry)
run_channel("wecom", "wecom-bot", "wecom-bot",
            "wecom-inbox.sh",
            "/home/hatch/workspace/wecom-bot/wecom", wc_entry)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
