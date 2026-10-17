#!/usr/bin/env python3
"""Channel hook fixes B1-B3 (both inbox hooks), sandbox tests.

B1 a detached batch that dies while another batch is in flight puts
   its msgids on the orphan list, so the "wake the orphans now and
   demote the in-flight batch" branch is reachable.
B2 a subagent failure notice goes to the job's own inbox row (found
   by the job's msgid), not to the latest inbox row.
B3 a formal answer delivered purely as reply_file shows up as an
   assistant turn in the next round's history.

The hook scripts are copied into a sandbox with state paths and the
channel CLI redirected to a stub, then executed for real. Hook script
location: MUSE_HOOK_SCRIPTS_DIR env, defaulting to hooks/scripts.
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

CHANNELS = {
    "weixin": {"botdir": "weixin-bot", "script": "weixin-inbox.sh",
               "cli": "/home/hatch/workspace/weixin-bot/weixin"},
    "wecom": {"botdir": "wecom-bot", "script": "wecom-inbox.sh",
              "cli": "/home/hatch/workspace/wecom-bot/wecom"},
}


def entry(chan, mid, ts, who, text="任务"):
    """Inbox row for `who` (a distinct chat/user per value)."""
    if chan == "weixin":
        return {"msgid": mid, "from_user_id": who, "text": text,
                "ts": ts, "media": []}
    return {"msgid": mid, "from_userid": who, "chattype": "single",
            "chatid": who, "msgtype": "text", "text": text,
            "ts": ts, "media": []}


class Sandbox:
    """One channel hook sandbox."""

    def __init__(self, chan):
        cfg = CHANNELS[chan]
        self.chan = chan
        self.botdir = cfg["botdir"]
        self.sbx = f"/tmp/chfix-{chan}"
        self.bot = f"{self.sbx}/{self.botdir}/state"
        self.hookst = f"{self.sbx}/home/hooks/state/{self.botdir}"
        shutil.rmtree(self.sbx, ignore_errors=True)
        os.makedirs(self.bot)
        os.makedirs(self.hookst)
        open(f"{self.sbx}/cli.log", "w").close()
        src = (SCRIPTS / cfg["script"]).read_text(encoding="utf-8")
        src = src.replace(f"/home/hatch/workspace/{self.botdir}/state",
                          self.bot)
        src = src.replace(cfg["cli"], f"{self.sbx}/stubcli")
        src = src.replace("/home/hatch/workspace/channel-restore/hook-heal.sh",
                          f"{self.sbx}/no-heal.sh")
        self.write("hook.sh", src)
        self.write("runtime.sh",
                   'silent() { echo "DECISION silent: $1"; }\n'
                   'wake() { echo "DECISION wake: $1"; '
                   'echo "PAYLOAD $2"; }\n'
                   'log() { :; }\n')
        self.write("stubcli",
                   '#!/usr/bin/env bash\n'
                   'echo "CALL: $*" >> ' + f"{self.sbx}/cli.log" + '\n'
                   'if [[ "$1" == "cancelled" ]]; then echo "[]"; fi\n'
                   'exit 0\n')
        os.chmod(f"{self.sbx}/stubcli", 0o755)
        self.write(f"{self.botdir}/state/cancelled.json", [])

    def write(self, rel, content):
        path = os.path.join(self.sbx, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str)
                    else json.dumps(content, ensure_ascii=False))

    def rows(self, name, rows):
        self.write(f"{self.botdir}/state/{name}.jsonl",
                   "".join(json.dumps(r, ensure_ascii=False) + "\n"
                           for r in rows))

    def state(self, name, content):
        self.write(f"home/hooks/state/{self.botdir}/{name}", content)

    def mark_seen(self, *mids):
        text = "".join(m + "\n" for m in mids)
        self.state("seen_msgids.txt", text)
        self.state("carried_msgids.txt", text)

    def run(self):
        env = dict(os.environ, HOME=f"{self.sbx}/home",
                   HATCH_HOOK_RUNTIME=f"{self.sbx}/runtime.sh",
                   HATCH_HOOK_DRY_RUN="0",
                   DEATH_WATCH_ACTIVITY_PY=f"{self.sbx}/none.py")
        r = subprocess.run(["bash", f"{self.sbx}/hook.sh"], env=env,
                           capture_output=True, text=True, timeout=60)
        return r.stdout + r.stderr

    def calls(self):
        return open(f"{self.sbx}/cli.log").read()

    def batch(self):
        with open(f"{self.hookst}/active_batch.json", encoding="utf-8") as f:
            return json.load(f)


def payload_of(out):
    for line in out.splitlines():
        if line.startswith("PAYLOAD "):
            return json.loads(line[len("PAYLOAD "):])
    return None


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name)
    if not cond and detail:
        print("  " + detail.replace("\n", "\n  ")[:1500])


def test_b1(chan):
    """Dead detached batch while a live batch is in flight."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "D1", NOW - 3700, "u1", "老任务"),
                      entry(chan, "A1", NOW - 60, "u1", "新任务")])
    sb.mark_seen("D1", "A1")
    sb.state("active_batch.json",
             {"msgids": ["A1"], "since": NOW - 60,
              "detached": [{"msgids": ["D1"], "since": NOW - 3700}]})
    sb.rows("outbox", [{"id": "U1", "mode": "update", "msgid": "A1",
                        "content": "进度", "queued_at": NOW - 30}])
    out = sb.run()
    pay = payload_of(out)
    orphans = [o.get("msgid") for o in (pay or {}).get("orphaned", [])]
    check(f"B1 {chan} dead detached batch wakes its orphans immediately",
          pay is not None and orphans == ["D1"], out)
    batch = sb.batch()
    detached_ids = [m for d in batch.get("detached") or []
                    for m in d.get("msgids") or []]
    check(f"B1 {chan} in-flight batch is demoted to detached",
          "A1" in detached_ids and batch.get("msgids") == ["D1"],
          json.dumps(batch))
    check(f"B1 {chan} resume notice still sent once",
          sb.calls().count("已自动续跑") == 1, sb.calls())


def test_b1_no_orphans_no_demotion(chan):
    """Control: a live detached batch never triggers the branch."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "D1", NOW - 300, "u1", "老任务"),
                      entry(chan, "A1", NOW - 60, "u1", "新任务")])
    sb.mark_seen("D1", "A1")
    sb.state("active_batch.json",
             {"msgids": ["A1"], "since": NOW - 60,
              "detached": [{"msgids": ["D1"], "since": NOW - 300}]})
    sb.rows("outbox", [{"id": "U1", "mode": "update", "msgid": "A1",
                        "content": "进度", "queued_at": NOW - 30}])
    out = sb.run()
    check(f"B1 {chan} control: live detached batch -> silent, batch intact",
          "DECISION silent" in out and sb.batch().get("msgids") == ["A1"],
          out)


def test_b2(chan):
    """Failure notice addressed from the job's own msgid row."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "J1", NOW - 3000, "jobowner", "派发的任务"),
                      entry(chan, "L1", NOW - 100, "latestuser", "别的人")])
    sb.mark_seen("J1", "L1")
    sb.state("subagent_jobs.json", {
        "jobs": {"S1": {"msgid": "J1", "status": "running",
                        "text": "跑个活", "wakes": 2,
                        "last_wake_ts": NOW - 1500,
                        "ts_dispatched": NOW - 3000}},
        "pending_notices": [], "req_offset": 0, "cmd_offset": 0})
    out = sb.run()
    sends = [l for l in sb.calls().splitlines()
             if l.startswith("CALL: send") and "副助手失败" in l]
    check(f"B2 {chan} failure notice goes to the job's own row",
          len(sends) == 1 and "jobowner" in sends[0]
          and "latestuser" not in sends[0], sb.calls() + out)


def test_b2_fallback(chan):
    """Job row missing -> latest row is still the fallback recipient."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "L1", NOW - 100, "latestuser", "别的人")])
    sb.mark_seen("L1")
    sb.state("subagent_jobs.json", {
        "jobs": {"S1": {"msgid": "GONE", "status": "running",
                        "text": "跑个活", "wakes": 2,
                        "last_wake_ts": NOW - 1500,
                        "ts_dispatched": NOW - 3000}},
        "pending_notices": [], "req_offset": 0, "cmd_offset": 0})
    sb.run()
    sends = [l for l in sb.calls().splitlines()
             if l.startswith("CALL: send") and "副助手失败" in l]
    check(f"B2 {chan} fallback: unknown job row -> latest row",
          len(sends) == 1 and "latestuser" in sends[0], sb.calls())


def test_b3(chan):
    """A reply_file-only answer becomes an assistant history turn."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "H1", NOW - 600, "u1", "发我报告"),
                      entry(chan, "N1", NOW - 5, "u1", "再来一个")])
    sb.mark_seen("H1")
    sb.rows("outbox", [{"id": "RF1", "mode": "reply_file", "msgid": "H1",
                        "file_path": "/tmp/out/report-final.pdf",
                        "content": "", "queued_at": NOW - 590}])
    sb.rows("outbox_results", [{"id": "RF1", "mode": "reply_file",
                                "ok": True, "ts": NOW - 580}])
    out = sb.run()
    pay = payload_of(out)
    hist = (pay or {}).get("history") or []
    assistant = [h["text"] for h in hist if h.get("role") == "assistant"]
    check(f"B3 {chan} reply_file-only answer appears as assistant turn",
          any("report-final.pdf" in t for t in assistant),
          json.dumps(hist, ensure_ascii=False))


def test_b3_text_plus_file(chan):
    """Reply text is kept verbatim when a file row follows it."""
    sb = Sandbox(chan)
    sb.rows("inbox", [entry(chan, "H1", NOW - 600, "u1", "发我报告"),
                      entry(chan, "N1", NOW - 5, "u1", "再来一个")])
    sb.mark_seen("H1")
    sb.rows("outbox", [
        {"id": "R1", "mode": "reply", "msgid": "H1",
         "content": "报告在这里", "queued_at": NOW - 595},
        {"id": "RF1", "mode": "reply_file", "msgid": "H1",
         "file_path": "/tmp/out/report-final.pdf",
         "content": "", "queued_at": NOW - 590}])
    pay = payload_of(sb.run())
    assistant = [h["text"] for h in (pay or {}).get("history") or []
                 if h.get("role") == "assistant"]
    check(f"B3 {chan} text reply plus file keeps one assistant turn "
          "with both",
          len(assistant) == 1 and "报告在这里" in assistant[0]
          and "report-final.pdf" in assistant[0], str(assistant))


for _chan in CHANNELS:
    test_b1(_chan)
    test_b1_no_orphans_no_demotion(_chan)
    test_b2(_chan)
    test_b2_fallback(_chan)
    test_b3(_chan)
    test_b3_text_plus_file(_chan)

fails = [n for n, ok in RESULTS if not ok]
print(f"\n== {len(RESULTS) - len(fails)}/{len(RESULTS)} passed ==")
sys.exit(1 if fails else 0)
