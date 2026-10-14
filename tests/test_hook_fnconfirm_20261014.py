#!/usr/bin/env python3
"""Fail-stop notice delivery confirmation — hook sandbox (2026-10-14).

Covers the audit-G3 fix in the production inbox hooks: a fail-stop
notice is no longer discharged when the channel CLI merely queues
it. It stays in failed_notices.json (24h retention, was 1h) until
the gateway's outbox_results row for its queued id says ok=true; a
dead-lettered row re-queues it; an in-flight row is left alone.

N1 aged 2h notice, CLI queues -> kept, queued_id recorded
N2 queued_id delivered ok -> notice discharged, no resend
N3 queued_id dead-lettered -> re-queued with a fresh id
N4 queued_id still retrying -> kept untouched, no resend
N5 notice older than 24h -> filtered out

Harness follows deathwatch-early-2026-10-14/run_scenarios.py: the
hook's embedded python is extracted from the PRODUCTION scripts in
~/hooks/scripts and run against planted state with a stub CLI.

Run: ~/muse-test-venv/bin/python tests/test_hook_fnconfirm_20261014.py
"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

SBX = Path("/tmp/hook-fnconfirm-sbx")
SCRIPTS = {"weixin": Path.home() / "hooks/scripts/weixin-inbox.sh",
           "wecom": Path.home() / "hooks/scripts/wecom-inbox.sh"}
RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)


def extract(channel):
    lines = SCRIPTS[channel].read_text(encoding="utf-8").splitlines()
    start = next(i for i, l in enumerate(lines)
                 if l.rstrip().endswith("<<'PYEOF'"))
    end = next(i for i in range(start + 1, len(lines))
               if lines[i] == "PYEOF")
    out = SBX / f"{channel}_hook.py"
    out.write_text("\n".join(lines[start + 1:end]) + "\n",
                   encoding="utf-8")
    return out


def setup(name, channel, notices, results_rows):
    d = SBX / name
    shutil.rmtree(d, ignore_errors=True)
    bot, st = d / "bot", d / "state"
    (bot / "heartbeats").mkdir(parents=True)
    st.mkdir(parents=True)
    (bot / "inbox.jsonl").write_text("", encoding="utf-8")
    (bot / "outbox.jsonl").write_text("", encoding="utf-8")
    (bot / "outbox_results.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in results_rows),
        encoding="utf-8")
    (bot / "outbox_parked.json").write_text("{}", encoding="utf-8")
    (bot / "cancelled.json").write_text("[]", encoding="utf-8")
    (bot / "bridge_state.json").write_text(
        json.dumps({"channels": {}}), encoding="utf-8")
    for fname in ("seen_msgids.txt", "carried_msgids.txt",
                  "cleared_msgids.txt"):
        (st / fname).write_text("", encoding="utf-8")
    for fname in ("pending.json", "jump_request.json",
                  "topic_boundary.json", "orphan_claims.json",
                  "queue_admin.json", "starvation.json",
                  "subagent_jobs.json", "context_notice.json",
                  "active_batch.json"):
        (st / fname).write_text("{}", encoding="utf-8")
    (st / "subagent_requests.jsonl").write_text("", encoding="utf-8")
    (st / "subagent_cmd.jsonl").write_text("", encoding="utf-8")
    (st / "failed_notices.json").write_text(
        json.dumps(notices, ensure_ascii=False), encoding="utf-8")
    stub = d / "stubcli.sh"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$CLI_LOG\"\n"
        "n=$(cat \"$STUB_N\" 2>/dev/null || echo 0); n=$((n+1))\n"
        "echo $n > \"$STUB_N\"\n"
        "echo \"queued send id=stub-$n to x\"\n"
        "exit 0\n", encoding="utf-8")
    stub.chmod(0o755)
    return d


def run(d, py):
    bot, st = d / "bot", d / "state"
    args = [str(st / "seen_msgids.txt"), str(bot / "inbox.jsonl"),
            str(bot / "outbox.jsonl"), str(st / "pending.json"),
            str(st / "active_batch.json"), str(st / "jump_request.json"),
            str(st / "topic_boundary.json"),
            str(st / "orphan_claims.json"), str(st / "queue_admin.json"),
            str(st / "starvation.json"), str(st / "subagent_jobs.json"),
            str(st / "subagent_requests.jsonl"),
            str(st / "subagent_cmd.jsonl"), str(d / "stubcli.sh"),
            str(st / "context_notice.json"),
            str(st / "carried_msgids.txt"),
            str(st / "cleared_msgids.txt")]
    env = dict(os.environ)
    env.pop("HATCH_HOOK_DRY_RUN", None)
    env["CLI_LOG"] = str(d / "cli.log")
    env["STUB_N"] = str(d / "stub_n.txt")
    env["DEATH_WATCH_ACTIVITY_PY"] = str(
        Path.home() / "workspace/death_watch_activity.py")
    env["DEATH_WATCH_BRIDGE_STATE"] = str(bot / "bridge_state.json")
    r = subprocess.run([sys.executable, str(py)] + args,
                       capture_output=True, text=True, env=env,
                       timeout=120)
    assert r.returncode == 0, r.stderr[-1200:]
    notices = json.loads((st / "failed_notices.json").read_text(
        encoding="utf-8"))
    log = (d / "cli.log").read_text(encoding="utf-8") \
        if (d / "cli.log").exists() else ""
    return notices, log


def notice(channel, **kw):
    base = {"ts": time.time() - 7200, "text": "⚠️ 任务已停止：演练"}
    if channel == "wecom":
        base.update({"chatid": "testuser", "chat_type": 1})
    else:
        base.update({"to": "userA"})
    base.update(kw)
    return base


def main():
    SBX.mkdir(parents=True, exist_ok=True)
    for channel in ("wecom", "weixin"):
        py = extract(channel)
        tag = "wc" if channel == "wecom" else "wx"
        # N1: aged 2h, not yet queued -> queued now, kept with id
        d = setup(f"{tag}n1", channel, [notice(channel)], [])
        notices, log = run(d, py)
        check(f"{tag} N1 aged notice queued and kept with queued_id",
              len(notices) == 1
              and notices[0].get("queued_id") == "stub-1"
              and log.count("send") == 1)
        # N2: delivered ok -> discharged, no resend
        d = setup(f"{tag}n2", channel,
                  [notice(channel, queued_id="q-ok")],
                  [{"id": "q-ok", "ok": True, "ts": time.time()}])
        notices, log = run(d, py)
        check(f"{tag} N2 delivered notice discharged silently",
              notices == [] and "send" not in log)
        # N3: dead-lettered -> re-queued with fresh id
        d = setup(f"{tag}n3", channel,
                  [notice(channel, queued_id="q-dead")],
                  [{"id": "q-dead", "ok": False, "deadletter": True,
                    "ts": time.time()}])
        notices, log = run(d, py)
        check(f"{tag} N3 dead-lettered notice re-queued",
              len(notices) == 1
              and notices[0].get("queued_id") == "stub-1"
              and log.count("send") == 1)
        # N4: still retrying -> untouched
        d = setup(f"{tag}n4", channel,
                  [notice(channel, queued_id="q-fly")],
                  [{"id": "q-fly", "ok": False, "ts": time.time()}])
        notices, log = run(d, py)
        check(f"{tag} N4 in-flight notice left alone",
              len(notices) == 1
              and notices[0].get("queued_id") == "q-fly"
              and "send" not in log)
        # N5: older than 24h -> filtered
        d = setup(f"{tag}n5", channel,
                  [notice(channel, ts=time.time() - 90000)], [])
        notices, log = run(d, py)
        check(f"{tag} N5 notice older than 24h dropped",
              notices == [] and "send" not in log)


main()
failed = [n for n, ok in RESULTS if not ok]
print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
if failed:
    print("FAILED:", failed)
    sys.exit(1)
