#!/usr/bin/env python3
"""reply_file completion fix — sandbox tests (both channels).

Regression suite for the 2026-10-06 incident in which an image
delivered via reply_file was later fail-stopped as if unanswered,
because hook supervision only counted mode=="reply" as completion.

S9  active batch + queued reply_file row, aged past cap
    -> batch silently done: no cancel, no failure send
S10 detached batch + queued reply_file row -> dropped silently
S11 msgid answered only via reply_file + starvation carried=3
    -> no solo wake (reply_file counts as bound)
S1c control: silent batch, no outbox rows -> fail-stop still fires

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

    def batch():
        return json.loads(open(f"{HOOKST}/active_batch.json").read())

    def check(name, cond):
        RESULTS.append((f"{chan} {name}", bool(cond)))
        print(("PASS " if cond else "FAIL ") + f"{chan} {name}")

    # S9 active batch completed by reply_file
    reset()
    inbox([entry_fn("F1", NOW - 700)])
    hookst_w("seen_msgids.txt", "F1\n")
    hookst_w("carried_msgids.txt", "F1\n")
    hookst_w("active_batch.json", {"msgids": ["F1"], "since": NOW - 700, "detached": []})
    outbox([{"mode": "reply_file", "msgid": "F1", "queued_at": NOW - 650}])
    out = run_hook()
    check("S9 no fail-stop calls", calls() == "")
    check("S9 silent", "DECISION silent" in out)
    check("S9 batch cleared", not batch().get("msgids"))

    # S10 detached batch completed by reply_file
    reset()
    inbox([entry_fn("F2", NOW - 700)])
    hookst_w("seen_msgids.txt", "F2\n")
    hookst_w("carried_msgids.txt", "F2\n")
    hookst_w("active_batch.json",
             {"msgids": [], "since": NOW - 700,
              "detached": [{"msgids": ["F2"], "since": NOW - 700}]})
    outbox([{"mode": "reply_file", "msgid": "F2", "queued_at": NOW - 650}])
    out = run_hook()
    check("S10 no fail-stop calls", calls() == "")
    check("S10 detached dropped", not batch().get("detached"))

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
    inbox([entry_fn("D1", NOW - 700)])
    hookst_w("seen_msgids.txt", "D1\n")
    hookst_w("carried_msgids.txt", "D1\n")
    hookst_w("active_batch.json", {"msgids": ["D1"], "since": NOW - 700, "detached": []})
    out = run_hook()
    check("S1c fail-stop still fires", "cancel --msgid D1" in calls()
          and calls().count("CALL: send") == 1)


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
