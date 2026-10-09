#!/usr/bin/env bash
# Poll the Weixin (iLink) gateway inbox for new messages and wake an agent
# to process and reply to them. Includes recent same-chat history so the
# agent can resolve follow-ups ("那第二个呢") across wakes.
#
# Gather-during-cold-start queue (revised 2026-10-04 at the user's
# correction): the FIRST message of a burst wakes the agent IMMEDIATELY —
# there is no quiet-period wait before the wake, because that wait stacked
# on top of the cold start and made even a single message slower. While
# that batch is cold-starting / being processed (an "active batch"),
# further messages are held in a pending queue: the waiting happens
# DURING the cold start instead of before it. As soon as the active
# batch's reply lands in the outbox, the gathered messages are flushed
# together in ONE follow-up wake. If no reply ever appears, the
# queue is still released by preemption (PREEMPT_WAIT_SECS /
# BATCH_MAX_SECS below): queued messages cut in to a fresh worker
# and the silent batch is parked in the detached list. (Until
# 2026-10-07 a hard silence cap (BATCH_CAP_SECS) also fail-stopped
# such batches; the user ordered that judgement removed — see the
# constant's history note.)
#
# Orphan handover (added 2026-10-04 after the 15:17 incident, where the
# worker holding a long task died mid-flight and its answer only landed
# 36 minutes later, out of order): the cap clock runs from the batch's
# LAST outbox activity — "update" progress notes count as activity, so
# a live worker keeps its batch by reporting progress — and when a
# batch goes a full cap window with no reply and no activity, its
# messages are declared orphaned and handed to the next wake (payload
# key "orphaned"), or re-woken on their own if nobody new is waiting.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

INBOX="/home/hatch/workspace/weixin-bot/state/inbox.jsonl"
OUTBOX="/home/hatch/workspace/weixin-bot/state/outbox.jsonl"
STATE_DIR="$HOME/hooks/state/weixin-bot"
SEEN="$STATE_DIR/seen_msgids.txt"
CARRIED="$STATE_DIR/carried_msgids.txt"
CLEARED="$STATE_DIR/cleared_msgids.txt"
PENDING="$STATE_DIR/pending.json"
BATCH="$STATE_DIR/active_batch.json"
JUMP="$STATE_DIR/jump_request.json"
BOUNDARY="$STATE_DIR/topic_boundary.json"
QA="$STATE_DIR/queue_admin.json"
CLAIMS="$STATE_DIR/orphan_claims.json"
STARVE="$STATE_DIR/starvation.json"
CONTEXT="$STATE_DIR/context_notice.json"
SUBJOBS="$STATE_DIR/subagent_jobs.json"
SUBREQ="$STATE_DIR/subagent_requests.jsonl"
SUBCMD="$STATE_DIR/subagent_cmd.jsonl"
CLI="/home/hatch/workspace/weixin-bot/weixin"
mkdir -p "$STATE_DIR"
touch "$SEEN" "$CARRIED" "$CLEARED"

if [[ ! -f "$INBOX" ]]; then
  silent "weixin inbox does not exist yet" '{}'
  exit 0
fi

PAYLOAD="$(python3 - "$SEEN" "$INBOX" "$OUTBOX" "$PENDING" "$BATCH" "$JUMP" "$BOUNDARY" "$CLAIMS" "$QA" "$STARVE" "$SUBJOBS" "$SUBREQ" "$SUBCMD" "$CLI" "$CONTEXT" "$CARRIED" "$CLEARED" <<'PYEOF'
import json, os, subprocess, sys, time

seen_path, inbox_path, outbox_path, pending_path, batch_path, jump_path, boundary_path, claims_path, qa_path, starve_path, subjobs_path, subreq_path, subcmd_path, cli_path, context_path, carried_path, cleared_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10], sys.argv[11], sys.argv[12], sys.argv[13], sys.argv[14], sys.argv[15], sys.argv[16], sys.argv[17]
BATCH_CAP_SECS = float("inf")
# Silence cap history: 180 -> 360 (2026-10-04 evening) -> 600
# (2026-10-05, user spec) -> 3600 (2026-10-07, user spec) ->
# NO CAP (2026-10-07, user spec, later the same day: "没有上限"
# — a batch must never be auto-judged dead, auto-cancelled or
# sent a failure notice for silence; it ends only when its
# formal reply is delivered. Accepted cost: a truly stuck
# worker hangs indefinitely. Queued messages still cut in via
# preemption (PREEMPT_WAIT_SECS / BATCH_MAX_SECS unchanged),
# so a zombie batch never blocks new questions; it lingers in
# state until its reply lands or the user intervenes.
# float("inf") keeps both comparisons below well-formed and
# simply never true; the fail-stop block is dormant, not
# deleted, so the policy can be reinstated by restoring a
# finite value. Two changes came with the 600 bump:
# (1) workers now run an internal heartbeat (heartbeat.py, started
# via the CLI at batch start) whose per-msgid files count as batch
# activity, so the cap no longer judges life by visible progress
# alone; (2) hitting the cap no longer orphans the batch into a
# late takeover answer — the batch is FAILED (msgids cancelled,
# one failure notice sent, retry is the user's call; see the
# fail-stop block below). Queued-message preemption
# (PREEMPT_WAIT_SECS) is unchanged, so new messages still cut in
# just as fast; only the presumed-dead judgement waits longer.
# (2026-10-09: that judgement now lives in DEATH_WATCH_SECS
# below; BATCH_CAP_SECS itself stays float("inf") as above.)
DEATH_WATCH_SECS = 1800
# Death watch (2026-10-09, semantics reworked the same evening
# by the status-push fix): this is NOT a task-duration cap. A
# batch whose worker keeps pushing keeps running without limit,
# exactly as the 2026-10-07 "没有上限" order requires. What it
# watches is total loss of the worker's OWN pushes: only bound
# outbox rows (update/reply) written by the worker itself count
# as life -- the detached heartbeat.py file no longer counts
# (user order: liveness must be the worker pushing for itself,
# not a clock/file inference). When no worker push has arrived
# for DEATH_WATCH_SECS the batch is routed, in order: covered
# by a delivered follow-up reply -> silent drop; first loss ->
# auto-resume ONCE via the orphan re-wake path plus one factual
# notice; second loss -> fail-stop (cancel + ONE factual stop
# notice stating the last real push, never an inferred
# "execution failed" verdict).
CLAIM_TTL_SECS = 600
# Preemption: queued messages must not wait behind a heartbeating long
# batch forever. If the oldest queued message has waited
# PREEMPT_WAIT_SECS, or the batch itself is older than BATCH_MAX_SECS,
# the queued messages are force-released to a fresh worker and the old
# batch moves to a supervised "detached" list inside the batch state
# (still orphan-watched each poll, dropped when its reply lands).
PREEMPT_WAIT_SECS = 180
BATCH_MAX_SECS = 600
MAX_BATCH = 10
HISTORY_TOTAL_BUDGET = 30000
HISTORY_SUMMARY_MAX = 4000
HISTORY_NOTICE_STEP_TURNS = 20
# History budget (2026-10-04, user-approved): was last-24-turns fixed.
# Now char-budget based: keep recent turns verbatim until the 30000-char
# total budget fills; older turns are proactively compressed into a
# digest summary (see build_history). Overflow used to add a
# context_notice to the payload for the worker to surface once;
# since 2026-10-08 (weixin only, user order) the notice is computed
# for throttling bookkeeping but never put in the payload.
dry = os.environ.get("HATCH_HOOK_DRY_RUN") == "1"
now = time.time()
try:
    with open(seen_path, encoding="utf-8") as f:
        seen = set(l.strip() for l in f if l.strip())
except OSError:
    seen = set()

def _load_id_set(path):
    try:
        with open(path, encoding="utf-8") as f:
            return set(l.strip() for l in f if l.strip())
    except OSError:
        return set()

# carried: msgids some wake has taken responsibility for (flushed as
# a batch or carried as orphans). cleared: msgids the user removed
# via /queue admin. Both ledgers exist for the lost-message
# reconciliation further down.
carried = _load_id_set(carried_path)
cleared = _load_id_set(cleared_path)

# Cancelled msgids (bot-state cancelled.json, written by the CLI):
# never resurrected by reconciliation and never solo-woken by the
# starvation guard — a cancelled task stays cancelled.
_cancelled_ids = set()
try:
    with open(os.path.join(os.path.dirname(inbox_path), "cancelled.json"),
              encoding="utf-8") as f:
        for _r in json.load(f):
            if isinstance(_r, dict) and _r.get("msgid"):
                _cancelled_ids.add(str(_r["msgid"]))
except (OSError, json.JSONDecodeError):
    pass


def _hb_ts(mid):
    # DEPRECATED as liveness evidence (2026-10-09, user order: "不能
    # 凭着代码、凭着时间去判断他到底有没有活着，得让他实际去推送"):
    # heartbeat.py is a detached代打卡 process -- it ticks even when
    # the worker itself is gone. Since this fix, ONLY the worker's own
    # bound outbox pushes (update/reply rows) count as signs of life;
    # this helper is kept for reference and is no longer consulted
    # by the death watch or the stall notice.
    try:
        return os.path.getmtime(os.path.join(
            os.path.dirname(inbox_path), "heartbeats", str(mid)))
    except OSError:
        return 0.0

def load_jsonl(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except OSError:
        pass
    return rows

entries = []
_entry_ids = set()
for _e in load_jsonl(inbox_path):
    _mid = _e.get("msgid")
    if _mid and _mid not in _entry_ids:
        _entry_ids.add(_mid)
        entries.append(_e)

# --- pending queue: register unseen messages ---
try:
    with open(pending_path, encoding="utf-8") as f:
        pending = json.load(f)
    if not isinstance(pending, dict):
        pending = {}
except (OSError, json.JSONDecodeError):
    pending = {}
pending = {str(k): float(v) for k, v in pending.items() if isinstance(v, (int, float))}

for e in entries:
    mid = e["msgid"]
    if mid not in seen and mid not in pending:
        ts = e.get("ts")
        queued = float(ts) if isinstance(ts, (int, float)) and ts > 0 else now
        pending[mid] = min(queued, now)

entry_ids = {e["msgid"] for e in entries}
pending = {k: v for k, v in pending.items() if k in entry_ids and k not in seen}

# --- /queue admin requests (written by the gateway) ---
# {ts, msgids:[...]}: drop exactly those msgids from the pending queue
# (/queue clear / /queue drop). A listed msgid that is no longer
# pending is ignored; messages that arrived after the request was
# written are never in the list and are never touched. Removed
# messages are also marked seen, so they can never re-register from
# the inbox on a later poll — cleared means cleared. Running and
# detached batches are not affected (their msgids are never pending).
qa_ts = 0.0
qa_msgids = []
try:
    with open(qa_path, encoding="utf-8") as f:
        _qa = json.load(f)
    if isinstance(_qa, dict):
        _qt = _qa.get("ts")
        qa_ts = float(_qt) if isinstance(_qt, (int, float)) else 0.0
        qa_msgids = [str(m) for m in (_qa.get("msgids") or [])]
except (OSError, json.JSONDecodeError):
    qa_ts = 0.0
if qa_ts > 0 and not dry:
    _qa_removed = [m for m in qa_msgids if m in pending]
    if _qa_removed:
        for m in _qa_removed:
            pending.pop(m, None)
        try:
            with open(seen_path, "a", encoding="utf-8") as f:
                for m in _qa_removed:
                    f.write(m + "\n")
            seen.update(_qa_removed)
            try:
                with open(cleared_path, "a", encoding="utf-8") as f:
                    for m in _qa_removed:
                        f.write(m + "\n")
                cleared.update(_qa_removed)
            except OSError:
                pass
        except OSError:
            pass
        # Tell the gateway to drop these msgids from its in-memory
        # feedback_track too (its queue-position ledger): this
        # hook-side drop is invisible to the gateway, and a lingering
        # record inflates later queue positions until the 7200s
        # prune (residue incident 2026-10-05). One msgid per line;
        # the gateway consumes the file in its feedback scan.
        # Fail-silent: a failed write only delays the cleanup.
        try:
            with open(os.path.join(os.path.dirname(inbox_path),
                                   "feedback_clear.jsonl"),
                      "a", encoding="utf-8") as f:
                for m in _qa_removed:
                    f.write(m + "\n")
        except OSError:
            pass
    try:
        with open(qa_path, encoding="utf-8") as f:
            _cur_qa = json.load(f)
        if isinstance(_cur_qa, dict) and float(_cur_qa.get("ts") or 0) == qa_ts:
            os.unlink(qa_path)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass

pending_entries = [e for e in entries if e["msgid"] in pending]

# (argv slot 6 historically carried the /jump flag path. /jump was
# removed 2026-10-07 — official-client behaviour only, no cut-ins —
# and nothing writes or reads the flag any more. The slot is kept so
# the positional arg indices below do not shift.)

# --- topic boundary (/new): history only includes post-boundary turns ---
boundary_ts = 0.0
try:
    with open(boundary_path, encoding="utf-8") as f:
        _bnd = json.load(f)
    if isinstance(_bnd, dict):
        _bt = _bnd.get("ts")
        boundary_ts = float(_bt) if isinstance(_bt, (int, float)) else 0.0
except (OSError, json.JSONDecodeError):
    boundary_ts = 0.0

# --- active batch: is the previous wake still being worked on? ---
batch = {}
try:
    with open(batch_path, encoding="utf-8") as f:
        batch = json.load(f)
    if not isinstance(batch, dict):
        batch = {}
except (OSError, json.JSONDecodeError):
    batch = {}
batch_ids = set(batch.get("msgids") or [])
batch_since = batch.get("since")
batch_since = float(batch_since) if isinstance(batch_since, (int, float)) else 0.0
batch_active = bool(batch_ids) and batch_since > 0

# --- delivered-reply evidence (2026-10-07) ---
# Batch completion counts only replies the gateway actually
# DELIVERED (ok=true rows in outbox_results.jsonl, mapped back to
# msgids through the queue rows' ids) -- a reply that was merely
# queued and later cancelled/suppressed must not silently close
# its batch. And a reply still retrying in the gateway's park
# lane (state/outbox_parked.json) counts as batch activity: its
# worker HAS answered, delivery is the gateway's job, and the
# batch must not be fail-stopped out from under the lane (the
# gateway would then suppress the parked answer as cancelled).
_delivered_modes = {}
_delivered_reply_ts = {}
_parked_msgids = set()
_parked_mtime = 0.0
try:
    _id2msgid = {}
    for _qr in load_jsonl(outbox_path):
        if _qr.get("id"):
            _id2msgid[str(_qr.get("id"))] = str(_qr.get("msgid") or "")
    _state_dir = os.path.dirname(outbox_path)
    for _rr in load_jsonl(os.path.join(_state_dir, "outbox_results.jsonl")):
        if _rr.get("ok") and _rr.get("mode") in ("reply", "reply_file", "send"):
            _m = _id2msgid.get(str(_rr.get("id") or ""))
            if _m:
                _delivered_modes.setdefault(_m, set()).add(_rr.get("mode"))
                if _rr.get("mode") in ("reply", "reply_file"):
                    _rts = _rr.get("ts")
                    if isinstance(_rts, (int, float)) and _rts > 0:
                        if float(_rts) > _delivered_reply_ts.get(_m, 0.0):
                            _delivered_reply_ts[_m] = float(_rts)
except (OSError, json.JSONDecodeError, AttributeError):
    _delivered_modes = {}
    _delivered_reply_ts = {}
try:
    _ppath = os.path.join(os.path.dirname(outbox_path), "outbox_parked.json")
    _parked_mtime = os.path.getmtime(_ppath)
    with open(_ppath, encoding="utf-8") as _pf:
        for _prec in (json.load(_pf) or {}).values():
            _pm = ((_prec.get("item") or {}).get("msgid")
                   if isinstance(_prec, dict) else None)
            if _pm:
                _parked_msgids.add(str(_pm))
except (OSError, json.JSONDecodeError, AttributeError):
    _parked_msgids = set()
    _parked_mtime = 0.0

# --- worker-push liveness helpers (2026-10-09 status-push fix) ---
# Signs of life are ONLY the worker's own bound outbox rows: an
# "update" progress push or a formal reply queued by the worker
# itself, in its own loop, about its own batch. The detached
# heartbeat.py timestamp is NOT evidence (see _hb_ts note).
_inbox_by_id = {}
for _e in entries:
    if _e.get("msgid"):
        _inbox_by_id[str(_e["msgid"])] = _e


def _row_ts(r):
    for k in ("queued_at", "ts"):
        v = r.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return 0.0


def _last_push(ids):
    """(ts, text) of the newest bound outbox row for ids -- the last
    real push the worker itself made. (0.0, "") when it never pushed."""
    _ids = {str(m) for m in ids}
    _best = (0.0, "")
    for _r in load_jsonl(outbox_path):
        if str(_r.get("msgid") or "") in _ids:
            _rt = _row_ts(_r)
            if _rt > _best[0]:
                _best = (_rt, str(_r.get("content") or _r.get("text") or ""))
    return _best


def _chatkey_of(e):
    return str(e.get("group_id") or e.get("from_user_id") or "")


def _covered_by_followup(ids, since, last_act):
    """Anti-false-failure guard (real incident 2026-10-09 18:35: a
    task's completion report was delivered as the reply to the user's
    NEXT message, and 30s later the original msgid was declared
    "任务执行失败" because no reply was bound to IT). If a formal
    reply to a follow-up message from the same chat -- a message that
    arrived after this batch started -- was DELIVERED after this
    batch's last own push, the work has been reported back; the batch
    must be dropped silently, never failed. Returns the covering
    msgid or None."""
    _ids = {str(m) for m in ids}
    _chats = {_chatkey_of(_inbox_by_id[m]) for m in _ids
              if m in _inbox_by_id}
    for _m2, _rts in _delivered_reply_ts.items():
        if _m2 in _ids or _rts <= last_act:
            continue
        _e2 = _inbox_by_id.get(_m2)
        if _e2 is None:
            continue
        _ts2 = _e2.get("ts")
        if not (isinstance(_ts2, (int, float)) and float(_ts2) > since - 2):
            continue
        if _chats and _chatkey_of(_e2) not in _chats:
            continue
        return _m2
    return None


def _log_covered(ids, since, last_act, covering):
    try:
        with open(os.path.join(os.path.dirname(batch_path),
                               "covered_drops.jsonl"),
                  "a", encoding="utf-8") as _cf:
            _cf.write(json.dumps(
                {"msgids": sorted(str(m) for m in ids), "since": since,
                 "last_activity": last_act, "covered_by": covering,
                 "ts": now}, ensure_ascii=False) + "\n")
    except OSError:
        pass


_resume_attempts_path = os.path.join(os.path.dirname(pending_path),
                                     "resume_attempts.json")
_resume_queue_path = os.path.join(os.path.dirname(pending_path),
                                  "resume_queue.json")


def _load_json_obj(path):
    try:
        with open(path, encoding="utf-8") as f:
            _v = json.load(f)
        return _v if isinstance(_v, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_json_obj(path, obj):
    try:
        _tmp = path + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(_tmp, path)
    except OSError:
        pass


_resume_attempts = {str(k): float(v)
                    for k, v in _load_json_obj(_resume_attempts_path).items()
                    if isinstance(v, (int, float)) and now - float(v) < 7200}
_resume_queue = _load_json_obj(_resume_queue_path)
_resume_queue = {str(k): float(v) for k, v in _resume_queue.items()
                 if isinstance(v, (int, float)) and now - float(v) < 7200}
_resume_notice_entries = []


def _route_death(e):
    """First loss of worker pushes -> resume once (queue the msgid
    for an orphan re-wake + one factual user notice). Second loss ->
    fail-stop via failed_entries. Returns "resume" or "fail"."""
    _mid = str(e["msgid"])
    if _resume_attempts.get(_mid):
        return "fail"
    _resume_attempts[_mid] = now
    _save_json_obj(_resume_attempts_path, _resume_attempts)
    if _mid not in _resume_queue:
        _resume_queue[_mid] = now
        _save_json_obj(_resume_queue_path, _resume_queue)
    _resume_notice_entries.append(e)
    return "resume"

batch_done = False
orphaned_entries = []
orphan_info = []
failed_entries = []
if batch_active:
    # A batch ends when a formal reply to any of its messages lands.
    # Standalone "send" rows carry no msgid (they are proactive
    # messages), so they never count here: with detached batches
    # running in parallel, an unrelated send could otherwise fake
    # this batch's completion and strip its orphan protection.
    # Progress
    # "update" notes do NOT end it, but they DO count as activity:
    # the cap clock runs from the batch's last activity, not from its
    # start, so a live worker on a long task keeps its batch by
    # reporting progress, while a worker that goes silent hits
    # the death watch and its
    # batch is failed (fail-stop, no late takeover answer).
    outbox_rows = load_jsonl(outbox_path)

    def row_ts(r):
        for k in ("queued_at", "ts"):
            v = r.get(k)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
        return 0.0

    # reply_file counts as completion too: a worker that answers
    # with a file (image/video) HAS answered. (Missed until
    # 2026-10-06: an image batch delivered via reply_file at 01:44
    # was fail-stopped at 02:03 as if unanswered, because only
    # mode=="reply" marked the batch done.)
    batch_replied = False
    last_activity = batch_since
    for r in outbox_rows:
        rt = row_ts(r)
        if rt >= batch_since - 2 and r.get("msgid") in batch_ids:
            if rt > last_activity:
                last_activity = rt
    # NOTE (2026-10-09 status-push fix): the detached heartbeat
    # file is deliberately NOT folded into last_activity any more --
    # only the worker's own bound outbox pushes above count as life.
    if any({"reply", "reply_file"} & _delivered_modes.get(str(_m), set())
           for _m in batch_ids):
        batch_replied = True
    if _parked_msgids & {str(_m) for _m in batch_ids}:
        if _parked_mtime > last_activity:
            last_activity = _parked_mtime
    if batch_replied:
        batch_done = True
        for _m in batch_ids:
            if _resume_attempts.pop(str(_m), None) is not None:
                _save_json_obj(_resume_attempts_path, _resume_attempts)
            if _resume_queue.pop(str(_m), None) is not None:
                _save_json_obj(_resume_queue_path, _resume_queue)
    elif now - last_activity >= DEATH_WATCH_SECS:
        # Loss of life signs (2026-10-09 status-push fix): no
        # formal reply and no push from the worker ITSELF for a full
        # death-watch window. Since 2026-10-07 BATCH_CAP_SECS is
        # float("inf") and never gates this; the watch keys on the
        # worker's own pushes only, never on task age and never on
        # the detached heartbeat file. Routing, in order:
        # (1) covered by a follow-up reply -> drop silently;
        # (2) first loss -> auto-resume once (orphan re-wake);
        # (3) second loss -> fail-stop (cancel + factual notice).
        batch_done = True
        by_id = {e["msgid"]: e for e in entries}
        _cov = _covered_by_followup(batch_ids, batch_since,
                                    last_activity)
        if _cov:
            _log_covered(batch_ids, batch_since, last_activity, _cov)
        else:
            for mid in batch.get("msgids") or []:
                e = by_id.get(mid)
                if e is not None and _route_death(e) == "fail":
                    failed_entries.append(e)

def is_stop_request(text):
    # Conservative matcher for stop/cancel imperatives: short
    # messages only, so ordinary questions that merely mention
    # cancellation never trip it.
    t = (text or "").strip().strip("。！!？?，,、 ")
    if not t or len(t) > 8:
        return False
    if t in ("停", "停下", "停一下", "停止", "停手", "取消", "别做了",
             "别弄了", "不用了", "不要了", "stop", "Stop", "STOP"):
        return True
    if t.startswith("别做") and len(t) <= 6:
        return True
    if t.startswith("取消") and len(t) <= 5:
        return True
    if t.startswith("停") and len(t) <= 4 and not t.startswith("停车"):
        return True
    return False


detached = batch.get("detached") or []
if not isinstance(detached, list):
    detached = []
detached_orig = list(detached)
detached_activity = []
detached_orphan_sources = []
preempt_info = None
detached_dirty = False
retire_rows = []
DETACHED_RETIRE_SECS = 3600

if detached:
    # Batches preempted earlier keep running in parallel; supervise
    # them here: drop one when its reply lands, FAIL its messages
    # when it goes silent past the same cap as a normal batch
    # (fail-stop, same rule as the active batch).
    drows = load_jsonl(outbox_path)

    def dts(r):
        for k in ("queued_at", "ts"):
            v = r.get(k)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
        return 0.0

    by_id = {e["msgid"]: e for e in entries}
    kept_detached = []
    for d in detached:
        if not isinstance(d, dict):
            continue
        dids = set(d.get("msgids") or [])
        dsince = d.get("since")
        dsince = float(dsince) if isinstance(dsince, (int, float)) else 0.0
        if not dids or dsince <= 0:
            continue
        dreplied = False
        dlast = dsince
        for r in drows:
            rt = dts(r)
            if r.get("msgid") in dids and rt >= dsince - 2:
                if rt > dlast:
                    dlast = rt
        # Detached batches follow the same worker-push-only
        # liveness rule as the active batch (2026-10-09 fix): the
        # detached heartbeat file no longer feeds dlast.
        if any({"reply", "reply_file", "send"}
               & _delivered_modes.get(str(_m), set()) for _m in dids):
            dreplied = True
        if _parked_msgids & {str(_m) for _m in dids}:
            if _parked_mtime > dlast:
                dlast = _parked_mtime
        if dreplied:
            for _m in dids:
                if _resume_attempts.pop(str(_m), None) is not None:
                    _save_json_obj(_resume_attempts_path,
                                   _resume_attempts)
                if _resume_queue.pop(str(_m), None) is not None:
                    _save_json_obj(_resume_queue_path, _resume_queue)
            continue
        if now - dlast >= DEATH_WATCH_SECS:
            # Death watch (2026-10-09, reworked by the same day's
            # status-push fix): the worker's own pushes gone for
            # DEATH_WATCH_SECS -- same routing as the active batch:
            # covered -> silent drop; first loss -> resume once;
            # second loss -> fail-stop (cancel + factual notice).
            # This runs BEFORE the retire branch below, so a lost
            # detached batch is reported instead of silently
            # retiring at DETACHED_RETIRE_SECS with no notice.
            _cov = _covered_by_followup(dids, dsince, dlast)
            if _cov:
                _log_covered(dids, dsince, dlast, _cov)
                continue
            for mid in d.get("msgids") or []:
                e = by_id.get(mid)
                if (e is not None and e not in failed_entries
                        and _route_death(e) == "fail"):
                    failed_entries.append(e)
            continue
        if now - dlast >= DETACHED_RETIRE_SECS:
            # Zombie retirement (2026-10-07): no delivered reply, no
            # bound outbox activity, no heartbeat, no parked row for
            # over an hour. Every sign of life feeds dlast, and a live
            # worker heartbeats every 60s, so this batch's worker is
            # gone. Under the no-cap policy nothing else ever retires
            # it, yet it keeps inflating queue displays and position
            # counts. Retire it to the graveyard file — a late reply
            # would still deliver via the outbox; only the supervision
            # and display tracking stop.
            retire_rows.append({"msgids": sorted(str(m) for m in dids),
                                "since": dsince, "last_activity": dlast,
                                "retired_ts": now})
            continue
        if now - dlast >= BATCH_CAP_SECS:
            # Fail-stop: the detached batch is dead. Fail its
            # messages (the fail-stop block cancels + notifies) and
            # drop the entry — it is NOT kept for orphan delivery.
            for mid in d.get("msgids") or []:
                e = by_id.get(mid)
                if e is not None and e not in failed_entries:
                    failed_entries.append(e)
            continue
        detached_activity.append((list(dids), dsince, dlast))
        kept_detached.append(d)
    detached = kept_detached

if retire_rows and not dry:
    try:
        with open(os.path.join(os.path.dirname(batch_path),
                               "detached_retired.jsonl"),
                  "a", encoding="utf-8") as _rf:
            for _rr2 in retire_rows:
                _rf.write(json.dumps(_rr2, ensure_ascii=False) + "\n")
    except OSError:
        pass

# --- resume queue merge (2026-10-09 status-push fix) ---
# Msgids routed to a one-time auto-resume wait in resume_queue.json
# until a wake actually carries them (the claims-recording block at
# the end removes them once emitted). Merging here puts them on the
# normal orphan path: claims, starvation accounting and the payload's
# "orphaned" list all apply, with resume=True marking the reason.
if _resume_queue:
    _rq_by_id = {e["msgid"]: e for e in entries}
    for _mid in list(_resume_queue):
        _e = _rq_by_id.get(_mid) or _rq_by_id.get(str(_mid))
        if _e is None or _e in orphaned_entries:
            continue
        orphaned_entries.append(_e)
        _ts = _e.get("ts")
        _waited = (round((now - float(_ts)) / 60, 1)
                   if isinstance(_ts, (int, float)) and _ts else None)
        orphan_info.append(
            {"msgid": _e["msgid"],
             "text": (_e.get("text") or "")[:500],
             "waited_mins": _waited, "resume": True,
             "resume_note": "上一名 worker 已中断（没再收到它本人的"
                            "推送）：先检查已有产物和进度，从断点续做，"
                            "不要从头重做；这是唯一一次自动续跑，"
                            "完成后照常正式回复本条。"})

# --- orphan claims: atomic takeover guard ---
# When a wake hands orphaned messages to a worker they are claimed
# for CLAIM_TTL_SECS (recorded in the state-write phase below);
# while a claim is fresh the same messages are not re-orphaned to
# another worker. Without this, several wakes racing during one
# long takeover each decided the messages were unanswered and the
# user received duplicate late answers (observed live 2026-10-04:
# one message answered five times). A claim only delays recovery:
# if the claiming worker truly died, the orphans resurface when
# the claim expires.
claims = {}
try:
    with open(claims_path, encoding="utf-8") as f:
        _cl = json.load(f)
    if isinstance(_cl, dict):
        claims = {str(k): float(v) for k, v in _cl.items() if isinstance(v, (int, float))}
except (OSError, json.JSONDecodeError):
    claims = {}
claims = {k: v for k, v in claims.items() if now - v < CLAIM_TTL_SECS}
if orphaned_entries:
    _unclaimed = [e for e in orphaned_entries if e["msgid"] not in claims]
    if len(_unclaimed) != len(orphaned_entries):
        _keep_ids = {e["msgid"] for e in _unclaimed}
        orphaned_entries = _unclaimed
        orphan_info = [o for o in orphan_info if o.get("msgid") in _keep_ids]
        if not orphaned_entries:
            detached_orphan_sources = []


# --- stall notice (NOT a fail-stop) ---
# Since the 2026-10-07 no-cap order a silent batch is never
# cancelled -- but silence itself became invisible: a worker that
# vanished left the user with no reply and no signal at all (real
# incident 2026-10-07: homepage-image batch last activity 16:31,
# heartbeat aged out 16:45, user still uninformed at 16:52).
# So a batch (active or detached) with no delivered reply, no bound
# bound outbox pushes from the worker itself for
# STALL_NOTICE_SECS gets ONE
# unbound stall notice per hour. The batch is NOT cancelled, NOT
# orphaned, and the notice (unbound, no msgid) never counts as
# batch activity.
STALL_NOTICE_SECS = 1200
if not dry:
    _stall_candidates = []
    if batch_active and not batch_done:
        if now - last_activity >= STALL_NOTICE_SECS:
            _stall_candidates.append(
                (list(batch_ids), batch_since, last_activity))
    for _dids, _dsince, _dlast in detached_activity:
        if now - _dlast >= STALL_NOTICE_SECS:
            _stall_candidates.append((_dids, _dsince, _dlast))
    if _stall_candidates:
        _sn_path = os.path.join(os.path.dirname(pending_path),
                                "stall_notices.json")
        try:
            with open(_sn_path, encoding="utf-8") as f:
                _sn = json.load(f)
            if not isinstance(_sn, dict):
                _sn = {}
        except (OSError, json.JSONDecodeError):
            _sn = {}
        _sn = {k: float(v) for k, v in _sn.items()
               if isinstance(v, (int, float)) and now - float(v) < 7200}
        _sn_by_id = {e["msgid"]: e for e in entries}
        _sn_dirty = False
        for _mids, _since, _last in _stall_candidates:
            _key = str(int(_since)) + ":" + ",".join(
                str(m) for m in _mids)
            if now - _sn.get(_key, 0) < 3600:
                continue
            _e0 = None
            for _m in _mids:
                _e0 = _sn_by_id.get(_m) or _sn_by_id.get(str(_m))
                if _e0 is not None:
                    break
            if _e0 is None:
                continue
            _mins = int((now - _last) // 60)
            _t = (_e0.get("text") or "").strip() or "（无文字消息）"
            _code = str(_e0.get("msgid") or "")[:8]
            _lp_ts, _lp_text = _last_push(_mids)
            _lp_when = (time.strftime("%H:%M",
                                      time.localtime(_lp_ts))
                        if _lp_ts else
                        time.strftime("%H:%M", time.localtime(_last)))
            _lp_ex = ("「" + _lp_text[:30] + "」") if _lp_text else ""
            _notice = ("⏳ 提醒：任务「" + _t[:20] + "」〔#" + _code
                       + "〕最后一次本人推送是 " + _lp_when + _lp_ex
                       + "，之后已约 " + str(_mins)
                       + " 分钟没再收到它的推送（也没报错）。"
                         "它本人再推送就继续跑、没有时长上限；"
                         "若满 30 分钟仍无它本人的推送，我会先自动"
                         "续跑一次并如实告诉你，不会假装还在跑。"
                         "要取消回 /stop。")
            _sent = False
            try:
                r = subprocess.run([cli_path, "send", "--to",
                                    str(_e0.get("from_user_id") or ""),
                                    "--text", _notice],
                                   capture_output=True, timeout=20)
                _sent = r.returncode == 0
            except Exception:
                _sent = False
            if _sent:
                _sn[_key] = now
                _sn_dirty = True
        if _sn_dirty:
            try:
                with open(_sn_path, "w", encoding="utf-8") as f:
                    json.dump(_sn, f, ensure_ascii=False)
            except OSError:
                pass

# --- resume notices (2026-10-09 status-push fix) ---
# One factual notice per msgid routed to auto-resume this poll:
# what happened is stated as fact (last real worker push, then
# nothing), never as an inferred verdict. Fail-silent like the
# stall notices; a failed send does not block the resume itself.
if not dry and _resume_notice_entries:
    for _re in _resume_notice_entries:
        _rmid = str(_re.get("msgid") or "")
        _lp_ts, _lp_text = _last_push([_rmid])
        _lp_when = (time.strftime("%H:%M", time.localtime(_lp_ts))
                    if _lp_ts else "无记录")
        _lp_ex = ("「" + _lp_text[:30] + "」") if _lp_text else ""
        _rt = (_re.get("text") or "").strip() or "（无文字消息）"
        _rnotice = ("♻️ 原任务已中断，已自动续跑一次：「" + _rt[:20]
                    + "」〔#" + _rmid[:8] + "〕（它最后一次本人推送是 "
                    + _lp_when + _lp_ex
                    + "，之后没再收到；续跑的 worker 会先查已有产物"
                      "再接着做，不从头重来。）")
        try:
                r = subprocess.run([cli_path, "send", "--to",
                                    str(_re.get("from_user_id") or ""),
                                    "--text", _rnotice],
                                   capture_output=True, timeout=20)
        except Exception:
            pass

# --- fail-stop execution ---
# For every batch declared dead above: cancel each msgid through
# the channel CLI (the gateway + CLI gates then suppress any late
# send from a worker that revives), and send the user ONE failure
# notice per poll via the CLI's unbound send. If the notice cannot
# be queued (CLI error), it is kept in failed_notices.json and
# retried on later polls for up to an hour, so the user always
# learns the task died. Retrying is the user's call — nothing here
# ever re-dispatches the work.
if not dry:
    _fn_path = os.path.join(os.path.dirname(pending_path), "failed_notices.json")
    _fn_pending = []
    try:
        with open(_fn_path, encoding="utf-8") as f:
            _pn = json.load(f)
        if isinstance(_pn, list):
            _fn_pending = [n for n in _pn if isinstance(n, dict)
                           and now - float(n.get("ts") or 0) < 3600]
    except (OSError, json.JSONDecodeError):
        _fn_pending = []
    if failed_entries:
        for e in failed_entries:
            try:
                subprocess.run([cli_path, "cancel", "--msgid", str(e["msgid"])],
                               capture_output=True, timeout=20)
            except Exception:
                pass
        # Same-poll guard: later sections (starvation, reconciliation)
        # consult the in-memory cancelled set loaded at poll start.
        _cancelled_ids.update(str(e["msgid"]) for e in failed_entries)
        _parts = []
        for e in failed_entries[:3]:
            _t = (e.get("text") or "").strip() or "（无文字消息）"
            _mid = str(e.get("msgid") or "")
            _lp_ts, _lp_text = _last_push([_mid])
            if _lp_ts:
                _lp = ("（最后推送 "
                       + time.strftime("%H:%M",
                                       time.localtime(_lp_ts))
                       + ("「" + _lp_text[:24] + "」" if _lp_text
                          else "")
                       + "，之后未再收到）")
            else:
                _lp = "（无它本人的推送记录）"
            _parts.append("「" + _t[:20] + "」〔#" + _mid[:8] + "〕"
                          + _lp)
        _to = next((e.get("from_user_id") for e in failed_entries
                    if e.get("from_user_id")), "")
        if _to:
            _fn_pending.append({
                "ts": now, "to": _to,
                "text": "⚠️ 任务已停止（续跑后仍没再收到它本人的推送）："
                        + "、".join(_parts)
                        + "。这不是已确认的执行失败，是它不再推送、"
                          "已按规矩停止等待"
                        + "。需要重试直接告诉我。"})
    if _fn_pending:
        _fn_still = []
        for n in _fn_pending:
            try:
                r = subprocess.run([cli_path, "send", "--to", str(n["to"]),
                                    "--text", str(n["text"])],
                                   capture_output=True, timeout=20)
                if r.returncode == 0:
                    continue
            except Exception:
                pass
            _fn_still.append(n)
        try:
            with open(_fn_path, "w", encoding="utf-8") as f:
                json.dump(_fn_still, f, ensure_ascii=False)
        except OSError:
            pass

# --- lost-message reconciliation ---
# Failure mode observed live 2026-10-05: the platform suspended
# this hook for 18 minutes (an invalid poll-interval definition),
# and when it restored the definition it BASELINED the waiting
# inbox into seen_msgids without emitting any wake. Four real user
# messages were in seen, had never been carried into a wake, and
# had no bound outbox row — every later poll called them "already
# seen" and they were only answered because a worker dug them out
# of the inbox by hand. Defence: an inbox entry that is in seen
# but was NEVER carried (carried ledger), was NOT /queue-cleared
# (cleared ledger), has no bound outbox row, is not in the live
# batch / detached / pending sets, is not already claimed, and
# arrived within LOST_WINDOW_SECS is resurrected as an orphan and
# rides the normal orphan path (claims, late-answer labelling and
# all). Batch members answered by a sibling's reply are in the
# carried ledger, so they can never resurrect; the window keeps
# ancient history out if a ledger is ever lost.
LOST_WINDOW_SECS = 3 * 3600
_live_ids = set(batch_ids)
for _d in detached:
    if isinstance(_d, dict):
        _live_ids |= {str(m) for m in (_d.get("msgids") or [])}
_orphan_ids = {e["msgid"] for e in orphaned_entries}
_lost_cands = []
for e in entries:
    mid = e["msgid"]
    if (mid in seen and mid not in carried and mid not in cleared
            and mid not in _cancelled_ids
            and mid not in _orphan_ids and mid not in pending
            and mid not in _live_ids and mid not in claims):
        ts = e.get("ts")
        if (isinstance(ts, (int, float)) and ts > 0
                and now - float(ts) <= LOST_WINDOW_SECS):
            _lost_cands.append(e)
if _lost_cands:
    _bound_ids = set()
    for r in load_jsonl(outbox_path):
        if r.get("mode") in ("reply", "reply_file", "update", "send_file") and r.get("msgid"):
            _bound_ids.add(str(r["msgid"]))
    for e in _lost_cands[:MAX_BATCH]:
        if e["msgid"] in _bound_ids:
            continue
        orphaned_entries.append(e)
        ts = e.get("ts")
        waited = round((now - float(ts)) / 60, 1) if isinstance(ts, (int, float)) and ts else None
        orphan_info.append({"msgid": e["msgid"], "text": (e.get("text") or "")[:500], "waited_mins": waited})

# Flush immediately when no batch is in flight; otherwise hold the
# messages until the in-flight batch finishes (waiting overlaps the
# cold start / processing instead of preceding it).
new = []
waiting = False
if dry:
    new = pending_entries[:MAX_BATCH]
elif batch_active and not batch_done:
    oldest_wait = (now - min(pending.values())) if pending else 0.0
    batch_age = now - batch_since
    if not pending_entries and detached_orphan_sources:
        # A detached (parallel) worker died while this batch is in
        # flight and nobody new is queued: wake a worker for the
        # orphaned messages now and demote this batch to the
        # detached list, mirroring preemption.
        new = orphaned_entries[:MAX_BATCH]
        detached.append({"msgids": list(batch.get("msgids") or []), "since": batch_since})
    if pending_entries and any(is_stop_request(e.get("text")) for e in pending_entries):
        # A stop/cancel imperative never waits in line: force-release
        # it immediately so a worker can register the cancellation
        # and confirm it to the user.
        preempt_info = {"still_running_msgids": list(batch.get("msgids") or []),
                        "batch_age_mins": round(batch_age / 60, 1),
                        "reason": "stop_request"}
    elif pending_entries and oldest_wait >= PREEMPT_WAIT_SECS:
        preempt_info = {"still_running_msgids": list(batch.get("msgids") or []),
                        "batch_age_mins": round(batch_age / 60, 1),
                        "reason": "queued_too_long"}
    elif pending_entries and batch_age >= BATCH_MAX_SECS:
        preempt_info = {"still_running_msgids": list(batch.get("msgids") or []),
                        "batch_age_mins": round(batch_age / 60, 1),
                        "reason": "batch_max_age"}
    if preempt_info:
        # Force-release the queued messages to a fresh worker while
        # the long batch keeps running; the old batch moves to the
        # detached list and stays supervised above.
        new = pending_entries[:MAX_BATCH]
        detached.append({"msgids": list(batch.get("msgids") or []), "since": batch_since})
    else:
        waiting = True
else:
    new = pending_entries[:MAX_BATCH]
    if not new and orphaned_entries:
        # Nobody new is waiting, but the previous worker died holding
        # unanswered messages: re-wake those messages themselves so a
        # fresh worker takes over the orphaned work.
        new = orphaned_entries[:MAX_BATCH]

if new and detached_orphan_sources:
    # This wake carries the detached orphans: their source batches
    # leave the detached list. (On a hold poll the sources stay
    # tracked and the orphans are re-derived next poll, so they can
    # never be dropped silently.)
    detached = [d for d in detached if d not in detached_orphan_sources]
detached_dirty = detached != detached_orig

# --- starvation guard (added 2026-10-04, fix 3 of the reply
# diagnosis): a real message can be carried into wake after wake
# without ever being answered — every worker answers orphans first
# and never reaches it (observed live: one message dispatched >=4
# times with zero bound outbox rows). Count carries per msgid in
# starvation.json: a "carry" is one emitted wake whose answer set
# (batch messages + the orphans the payload carries) includes the
# msgid while it still has NO bound outbox row (a reply/update/
# send_file row carrying its msgid; standalone sends carry no msgid
# and never count). Once a msgid has been carried STARVE_THRESHOLD
# times unanswered, the next wake this poll can legitimately
# produce is hijacked into a SOLO wake carrying only that message,
# marked with payload key "starved_priority", and its counter
# resets (if the solo worker still does not answer, it accumulates
# again and re-triggers — it never monopolises the channel). A solo
# wake does NOT merge this round's orphans into its payload (they
# wait for the next round under the original rules), but their
# msgids still ride the new batch's keep-list in the state write
# below, so they can never be lost. Exemptions: a msgid whose batch
# is in flight and not silent (active batch unfinished, or a
# detached batch that has not gone silent) is being handled — it is
# neither counted nor triggered; likewise a msgid under a fresh
# orphan claim already has a worker on it. A solo never creates a
# wake out of thin air: it only replaces a wake this poll produced
# anyway, or fires while messages sit queued (in which case the
# in-flight batch is demoted to detached, like a preemption).
# Counting happens only on polls that actually emit a wake; dry
# runs never touch any of this.
STARVE_THRESHOLD = 2
starved_priority = None
if not dry:
    try:
        with open(starve_path, encoding="utf-8") as f:
            _sv = json.load(f)
        if not isinstance(_sv, dict):
            _sv = {}
    except (OSError, json.JSONDecodeError):
        _sv = {}
    starve = {}
    for _k, _v in _sv.items():
        if isinstance(_v, dict):
            try:
                starve[str(_k)] = {"carried": int(_v.get("carried") or 0),
                                   "last_wake": float(_v.get("last_wake") or 0.0)}
            except (TypeError, ValueError):
                pass
    _starve_orig = {k: dict(v) for k, v in starve.items()}
    bound_ids = set()
    for _r in load_jsonl(outbox_path):
        if _r.get("mode") in ("reply", "reply_file", "update", "send_file") and _r.get("msgid"):
            bound_ids.add(str(_r["msgid"]))
    inflight_ids = set()
    if batch_active and not batch_done:
        inflight_ids |= {str(m) for m in (batch.get("msgids") or [])}
    _silent_detached_ids = set()
    for _d in detached_orphan_sources:
        _silent_detached_ids |= {str(m) for m in (_d.get("msgids") or [])}
    for _d in detached:
        if isinstance(_d, dict):
            inflight_ids |= {str(m) for m in (_d.get("msgids") or [])}
    inflight_ids -= _silent_detached_ids
    _by_id = {str(e["msgid"]): e for e in entries}
    solo_mid = None
    if new or pending_entries:
        _cands = []
        for _mid, _rec in starve.items():
            if (_rec["carried"] >= STARVE_THRESHOLD and _mid not in bound_ids
                    and _mid not in inflight_ids and _mid not in claims
                    and _mid not in _cancelled_ids
                    and _mid in _by_id):
                _cands.append((_mid, _rec))
        if _cands:
            # One solo per poll: highest carry count, then the one
            # waiting longest since its last carry, then msgid for
            # determinism. The rest stay armed for later rounds.
            _cands.sort(key=lambda kv: (-kv[1]["carried"], kv[1]["last_wake"], kv[0]))
            solo_mid = _cands[0][0]
    if solo_mid is not None:
        starved_priority = {"msgid": solo_mid,
                            "carried": starve[solo_mid]["carried"]}
        new = [_by_id[solo_mid]]
        orphan_info = []
        waiting = False
        del starve[solo_mid]
        if batch_active and not batch_done:
            # The solo wake takes over the batch slot: demote the
            # in-flight batch to the detached list, unless a
            # preemption / orphan-delivery path already did so.
            _batch_key = ({str(m) for m in (batch.get("msgids") or [])}, batch_since)
            _already = any(
                isinstance(_d, dict)
                and ({str(m) for m in (_d.get("msgids") or [])},
                     float(_d.get("since") or 0.0)) == _batch_key
                for _d in detached)
            if not _already:
                detached.append({"msgids": list(batch.get("msgids") or []),
                                 "since": batch_since})
                detached_dirty = detached != detached_orig
    if new:
        # Count this wake's actual answer set. A msgid that meanwhile
        # has any bound row has been answered/handled: drop its
        # counter. The solo message itself was just reset and is not
        # counted for its own solo wake.
        _answer_ids = []
        for _e in new:
            _answer_ids.append(str(_e["msgid"]))
        for _o in orphan_info:
            if str(_o["msgid"]) not in _answer_ids:
                _answer_ids.append(str(_o["msgid"]))
        for _mid in _answer_ids:
            if _mid == solo_mid:
                continue
            if _mid in bound_ids:
                starve.pop(_mid, None)
            elif _mid in inflight_ids:
                pass
            else:
                _rec = starve.get(_mid) or {"carried": 0, "last_wake": 0.0}
                _rec["carried"] = int(_rec.get("carried") or 0) + 1
                _rec["last_wake"] = now
                starve[_mid] = _rec
    for _mid in [m for m in starve if m in bound_ids]:
        del starve[_mid]
    if starve != _starve_orig:
        try:
            tmp = starve_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(starve, f)
            os.replace(tmp, starve_path)
        except OSError:
            pass

if not dry:
    flushed = {e["msgid"] for e in new}
    pending = {k: v for k, v in pending.items() if k not in flushed}
    if new:
        # Entries that did not fit into this flush start a fresh wait
        # clock against the batch just created; otherwise a >MAX_BATCH
        # backlog would instantly re-trigger preemption on the next
        # poll and thrash the brand-new batch into the detached list.
        for k in pending:
            pending[k] = now
    try:
        tmp = pending_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pending, f)
        os.replace(tmp, pending_path)
    except OSError:
        pass
    if new:
        with open(seen_path, "a", encoding="utf-8") as f:
            for e in new:
                f.write(e["msgid"] + "\n")
        # Carried ledger: every msgid this wake takes responsibility
        # for (the flushed messages plus any orphans riding along).
        # The lost-message reconciliation uses it to tell "answered
        # as part of a batch" apart from "swallowed unseen".
        try:
            _keep = [e["msgid"] for e in new]
            for e in orphaned_entries:
                if e["msgid"] not in _keep:
                    _keep.append(e["msgid"])
            with open(carried_path, "a", encoding="utf-8") as f:
                for m in _keep:
                    f.write(m + "\n")
            carried.update(_keep)
            if os.path.getsize(carried_path) > 131072:
                _alive = {e["msgid"] for e in entries} | set(_keep)
                with open(carried_path, "w", encoding="utf-8") as f:
                    for m in sorted(carried):
                        if m in _alive:
                            f.write(m + "\n")
        except OSError:
            pass
        try:
            keep = [e["msgid"] for e in new]
            for e in orphaned_entries:
                if e["msgid"] not in keep:
                    keep.append(e["msgid"])
            tmp = batch_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                state = {"msgids": keep, "since": now}
                if detached:
                    state["detached"] = detached
                json.dump(state, f)
            os.replace(tmp, batch_path)
        except OSError:
            pass
    elif batch_active and batch_done:
        # Batch finished with nothing queued: release the slot so the
        # next message wakes immediately.
        try:
            tmp = batch_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"detached": detached} if detached else {}, f)
            os.replace(tmp, batch_path)
        except OSError:
            pass

if not dry and detached_dirty and not new and not (batch_active and batch_done):
    # The detached supervision above dropped or orphaned entries, but
    # the path taken this poll (e.g. gathering/hold) writes no batch
    # state of its own; persist the supervision result so dropped
    # batches do not resurrect from the stale state file next poll.
    try:
        cur = {}
        if batch_active and not batch_done:
            cur = {"msgids": list(batch.get("msgids") or []), "since": batch_since}
        if detached:
            cur["detached"] = detached
        tmp = batch_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cur, f)
        os.replace(tmp, batch_path)
    except OSError:
        pass

if not dry and new and orphan_info:
    # Record takeover claims for the orphans this wake carries, so
    # later polls do not hand the same messages to another worker.
    # Resume msgids carried by this wake leave the resume queue:
    # their one auto-resume has now actually been dispatched.
    for o in orphan_info:
        claims[o["msgid"]] = now
        if o.get("resume") and _resume_queue.pop(
                str(o["msgid"]), None) is not None:
            _save_json_obj(_resume_queue_path, _resume_queue)
    try:
        tmp = claims_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(claims, f)
        os.replace(tmp, claims_path)
    except OSError:
        pass

# --- subagent jobs (/subagent): isolated parallel background jobs ---
# The gateway intercepts /subagent commands and appends dispatch
# intents to subagent_requests.jsonl (stop intents to
# subagent_cmd.jsonl); this script is the ONLY writer of
# subagent_jobs.json. A job lives completely outside the normal
# queue: it never enters pending / active_batch, is never counted
# by the starvation guard, and never displaces a normal batch — a
# job wake is emitted only on a poll where the normal flow produced
# no wake of its own (polls are ~5s apart, so a busy burst delays a
# job dispatch by a poll at most). Supervision mirrors the orphan
# rules: a bound reply row for the job msgid ends the job,
    # with the outcome parsed from the reply's first line (fix #3),
# bound update rows count as heartbeat activity, JOB_SILENCE_SECS
# of silence earns ONE claim-guarded re-wake (payload key
# "job_retry"), and a second silent window fails the job; the hook
    # then queues the failure notice itself via the CLI in the same
    # poll (fix #4), falling back to a pending notice that leaves
# a pending notice that rides the next NORMAL wake (payload key
# "subagent_notices") for that worker to announce. At most
# JOB_MAX_RUNNING jobs run at once per channel; the rest stay
# queued FIFO. Stopping a queued job retires it at once; stopping a
# running job registers a CLI cancellation for its msgid and frees
# the slot immediately (the worker closes with a brief reply at its
# next cancellation check; if it never does, the slot stays free
# and the job is never re-woken).
# Silence window widened 180->360 (2026-10-04 fix): a real job took
# ~8 min and was false-killed at 180s x2 while still alive. Job
# workers must heartbeat with an `update` at least every ~2 min;
# bound update rows count as activity, so a heartbeating job is
# never judged dead, while a truly silent job still fails after
# two windows (~12 min worst case).
JOB_SILENCE_SECS = 360
JOB_MAX_RUNNING = 2
job_wake = None
sub_notices_out = None
if not dry:
    subjobs = {"jobs": {}, "pending_notices": [], "req_offset": 0, "cmd_offset": 0}
    try:
        with open(subjobs_path, encoding="utf-8") as f:
            _sj = json.load(f)
        if isinstance(_sj, dict):
            subjobs = _sj
    except (OSError, json.JSONDecodeError):
        pass
    jobs = subjobs.get("jobs")
    if not isinstance(jobs, dict):
        jobs = {}
    notices = subjobs.get("pending_notices")
    if not isinstance(notices, list):
        notices = []
    subjobs["jobs"] = jobs
    subjobs["pending_notices"] = notices
    jobs_dirty = False

    def _job_num(jid):
        try:
            return int(str(jid)[1:])
        except (TypeError, ValueError):
            return 0

    # Ingest dispatch requests (dedupe by job_id and by msgid: a
    # replayed request line must never create a second job).
    req_rows = load_jsonl(subreq_path)
    req_off = subjobs.get("req_offset")
    req_off = req_off if isinstance(req_off, int) and req_off >= 0 else 0
    if req_off > len(req_rows):
        req_off = 0
    known_job_msgids = {str(r.get("msgid")) for r in jobs.values() if isinstance(r, dict)}
    for row in req_rows[req_off:]:
        jid = str(row.get("job_id") or "")
        mid = str(row.get("msgid") or "")
        if jid and mid and jid not in jobs and mid not in known_job_msgids:
            tsd = row.get("ts")
            jobs[jid] = {
                "msgid": mid,
                "text": row.get("text") or "",
                "ts_dispatched": float(tsd) if isinstance(tsd, (int, float)) else now,
                "status": "queued",
                "started_ts": None,
                "last_wake_ts": None,
                "last_activity_ts": None,
                "finished_ts": None,
                "wakes": 0,
            }
            known_job_msgids.add(mid)
            jobs_dirty = True
    if req_off != len(req_rows):
        subjobs["req_offset"] = len(req_rows)
        jobs_dirty = True

    # Ingest stop commands.
    cmd_rows = load_jsonl(subcmd_path)
    cmd_off = subjobs.get("cmd_offset")
    cmd_off = cmd_off if isinstance(cmd_off, int) and cmd_off >= 0 else 0
    if cmd_off > len(cmd_rows):
        cmd_off = 0
    for row in cmd_rows[cmd_off:]:
        if row.get("type") != "stop":
            continue
        rec = jobs.get(str(row.get("job_id") or ""))
        if not isinstance(rec, dict):
            continue
        if rec.get("status") == "queued":
            rec["status"] = "stopped"
            rec["finished_ts"] = now
            jobs_dirty = True
        elif rec.get("status") == "running":
            try:
                subprocess.run([cli_path, "cancel", "--msgid", str(rec.get("msgid") or "")],
                               capture_output=True, timeout=15)
            except Exception:
                pass
            rec["status"] = "stopped"
            rec["finished_ts"] = now
            jobs_dirty = True
    if cmd_off != len(cmd_rows):
        subjobs["cmd_offset"] = len(cmd_rows)
        jobs_dirty = True

    # Supervise running jobs against the outbox.
    job_rows = load_jsonl(outbox_path)

    def _job_row_ts(r):
        for k in ("queued_at", "ts"):
            v = r.get(k)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
        return 0.0

    retry_candidate = None
    failed_now = []
    for jid in sorted(jobs, key=_job_num):
        rec = jobs[jid]
        if not isinstance(rec, dict) or rec.get("status") != "running":
            continue
        mid = str(rec.get("msgid") or "")
        replied = False
        reply_content = ""
        last_act = rec.get("last_activity_ts")
        last_act = float(last_act) if isinstance(last_act, (int, float)) else 0.0
        for r in job_rows:
            if str(r.get("msgid") or "") != mid:
                continue
            if r.get("mode") == "reply":
                replied = True
                reply_content = str(r.get("content") or "")
            if r.get("mode") in ("reply", "update", "send_file", "reply_file"):
                rt = _job_row_ts(r)
                if rt > last_act:
                    last_act = rt
        if replied:
            # Outcome from the reply's first line (fix #3, 2026-10-04):
            # a first line saying 失败 means the job FAILED even though
            # a reply landed; previously every reply was booked as
            # done, so /subagent list showed fake successes (S4).
            # 已停止 maps to stopped; anything else (incl. an
            # unlabeled reply) is done, matching the label the
            # delivery layer forces onto job replies.
            _first = ""
            for _line in str(reply_content).splitlines():
                if _line.strip():
                    _first = _line.strip()
                    break
            _head = _first.split("】")[0] if _first.startswith("【副助手") else ""
            if "失败" in _head:
                rec["status"] = "failed"
                rec["outcome"] = "failed"
            elif "已停止" in _head:
                rec["status"] = "stopped"
                rec["outcome"] = "stopped"
            else:
                rec["status"] = "done"
                rec["outcome"] = "done"
            rec["finished_ts"] = now
            jobs_dirty = True
            continue
        if last_act and last_act != rec.get("last_activity_ts"):
            rec["last_activity_ts"] = last_act
            jobs_dirty = True
        wake_ts = rec.get("last_wake_ts")
        wake_ts = float(wake_ts) if isinstance(wake_ts, (int, float)) else now
        if now - max(wake_ts, last_act) >= JOB_SILENCE_SECS:
            if int(rec.get("wakes") or 0) >= 2:
                rec["status"] = "failed"
                rec["outcome"] = "failed"
                rec["finished_ts"] = now
                failed_now.append(jid)
                jobs_dirty = True
            elif retry_candidate is None:
                claim_ts = claims.get(mid)
                if not (isinstance(claim_ts, (int, float))
                        and now - claim_ts < CLAIM_TTL_SECS and claim_ts > wake_ts):
                    retry_candidate = jid


    # Failure notices go out IMMEDIATELY (fix #4, 2026-10-04): in the
    # same poll that declares a job failed, the hook itself queues a
    # proactive failure notice via the channel CLI, instead of
    # leaving it to ride the next normal wake (that arrived ~6 min
    # late in production and could contradict a late completion).
    # Only when the send cannot be queued (no known recipient / CLI
    # error) does the old pending-notice ride-along remain, as a
    # fallback, so a failure is never silently dropped.
    for _fjid in failed_now:
        _frec = jobs.get(_fjid) or {}
        _ftext = (f"【副助手 #{_fjid} 失败】该任务两次无响应，已放弃。"
                  f"（任务：「{str(_frec.get('text') or '')[:40]}」）")
        _sent = False
        if entries:
            _to = entries[-1].get("from_user_id") or ""
            if _to:
                try:
                    _r = subprocess.run([cli_path, "send", "--to", str(_to),
                                         "--text", _ftext],
                                        capture_output=True, timeout=15)
                    _sent = _r.returncode == 0
                except Exception:
                    _sent = False
        if not _sent:
            if not any(isinstance(n, dict) and n.get("job_id") == _fjid for n in notices):
                notices.append({"job_id": _fjid, "outcome": "failed"})
            jobs_dirty = True

    # Emit at most one job wake per poll, and only when the normal
    # flow produced no wake of its own. A due retry outranks a fresh
    # dispatch. State changes for the emitted wake are applied (and
    # persisted below) only here, so a deferred wake never leaves
    # phantom state behind.
    if not new:
        pick = retry_candidate
        if pick is None:
            running_n = sum(1 for r in jobs.values()
                            if isinstance(r, dict) and r.get("status") == "running")
            if running_n < JOB_MAX_RUNNING:
                for jid in sorted(jobs, key=_job_num):
                    if isinstance(jobs[jid], dict) and jobs[jid].get("status") == "queued":
                        pick = jid
                        break
        if pick is not None:
            rec = jobs[pick]
            is_retry = pick == retry_candidate and retry_candidate is not None
            rec["status"] = "running"
            if not isinstance(rec.get("started_ts"), (int, float)):
                rec["started_ts"] = now
            rec["last_wake_ts"] = now
            rec["wakes"] = int(rec.get("wakes") or 0) + 1
            jobs_dirty = True
            if is_retry:
                claims[str(rec.get("msgid") or "")] = now
                try:
                    tmp = claims_path + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(claims, f)
                    os.replace(tmp, claims_path)
                except OSError:
                    pass
            job_wake = (pick, rec, is_retry)

    # Failure notices ride the next normal wake; the hook clears
    # them as that wake is emitted (the worker announces them).
    if new and notices:
        sub_notices_out = [{"id": n.get("job_id"), "outcome": n.get("outcome")}
                           for n in notices if isinstance(n, dict)]
        notices.clear()
        jobs_dirty = True

    if jobs_dirty:
        try:
            tmp = subjobs_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(subjobs, f, ensure_ascii=False)
            os.replace(tmp, subjobs_path)
        except OSError:
            pass

out = [
    {
        "msgid": e.get("msgid"),
        "from_user_id": e.get("from_user_id"),
        "group_id": e.get("group_id"),
        "text": (e.get("text") or "")[:2000],
        "media": e.get("media") or [],
    }
    for e in new
]

def build_history(new_list):
    def chatkey(e):
        return e.get("group_id") or e.get("from_user_id") or ""

    primary = chatkey(new_list[0])
    new_ids = {e["msgid"] for e in new_list}
    replies = {}
    for r in load_jsonl(outbox_path):
        if r.get("mode") == "reply" and r.get("msgid"):
            replies[r["msgid"]] = r.get("content", "")
    def compress(text, limit=1200):
        # Over-long turns: keep head+tail with an explicit omission marker
        # instead of a hard cut that silently loses the ending.
        if len(text) <= limit:
            return text
        omitted = len(text) - 700 - 400
        return text[:700] + f"\n…[已压缩省略约{omitted}字]…\n" + text[-400:]

    turns = []
    for e in entries:
        if chatkey(e) != primary:
            continue
        mid = e.get("msgid")
        if not mid or mid in new_ids:
            continue
        ets = e.get("ts")
        if boundary_ts and isinstance(ets, (int, float)) and float(ets) <= boundary_ts:
            continue  # /new topic boundary: pre-boundary turns stay out
        t = (e.get("text") or "").strip()
        if t:
            turns.append({"role": "user", "text": compress(t), "ts": e.get("ts", 0)})
        rep = (replies.get(mid) or "").strip()
        if rep:
            turns.append({"role": "assistant", "text": compress(rep), "ts": e.get("ts", 0) + 1})
    ordered = sorted(turns, key=lambda x: x["ts"])
    total_chars = sum(len(t["text"]) for t in ordered)
    meta = {"budget_chars": HISTORY_TOTAL_BUDGET,
            "total_chars_before": total_chars, "overflow": False,
            "summarized_turns": 0, "verbatim_turns": len(ordered),
            "verbatim_chars": total_chars, "summary_chars": 0}
    if total_chars <= HISTORY_TOTAL_BUDGET:
        return [{"role": t["role"], "text": t["text"]} for t in ordered], meta
    # Overflow: proactively compress older turns into a digest summary.
    # Recent turns stay verbatim within (budget - summary reserve).
    verbatim_budget = HISTORY_TOTAL_BUDGET - HISTORY_SUMMARY_MAX
    recent = []
    acc = 0
    for t in reversed(ordered):
        ln = len(t["text"])
        if recent and acc + ln > verbatim_budget:
            break
        recent.append(t)
        acc += ln
    recent.reverse()
    older = ordered[:len(ordered) - len(recent)] if recent else list(ordered)
    header = (f"[更早对话摘要｜上下文共{total_chars}字，已超3万字上限，"
              f"已将更早的{len(older)}条自动压缩为摘要；最近{len(recent)}条保留原文] ")
    usable = max(0, HISTORY_SUMMARY_MAX - len(header))
    bits = []
    if older and usable > 0:
        per_est = usable // len(older) - 8
        if per_est >= 24:
            per = min(300, per_est)
            src = older
        else:
            # Too many older turns to cover all: keep the most recent
            # older turns in the digest; earliest are dropped from the
            # digest (their facts, if durable, live in MEMORY.md).
            keep_n = max(1, usable // 32)
            src = older[-keep_n:]
            per = 24
            header += f"(其中最早{len(older) - len(src)}条已进一步省略) "
            usable = max(0, HISTORY_SUMMARY_MAX - len(header))
        for t in src:
            who = "用户" if t["role"] == "user" else "Muse"
            one = " ".join(t["text"].split())[:per]
            bits.append(f"{who}: {one}")
    digest = "；".join(bits)[:usable]
    summary_text = header + digest
    meta["overflow"] = True
    meta["summarized_turns"] = len(older)
    meta["verbatim_turns"] = len(recent)
    meta["verbatim_chars"] = acc
    meta["summary_chars"] = len(summary_text)
    hist = [{"role": "user", "text": summary_text}]
    hist += [{"role": t["role"], "text": t["text"]} for t in recent]
    return hist, meta


history = []
history_meta = {"budget_chars": HISTORY_TOTAL_BUDGET, "total_chars_before": 0,
                "overflow": False, "summarized_turns": 0, "verbatim_turns": 0,
                "verbatim_chars": 0, "summary_chars": 0}
context_notice = None
if new:
    history, history_meta = build_history(new)
    if history_meta.get("overflow") and not dry:
        # Notify-once throttling: first overflow for this topic, then
        # only after HISTORY_NOTICE_STEP_TURNS more turns get summarized.
        _ctx = {}
        try:
            with open(context_path, encoding="utf-8") as f:
                _ctx = json.load(f)
            if not isinstance(_ctx, dict):
                _ctx = {}
        except (OSError, json.JSONDecodeError):
            _ctx = {}
        if float(_ctx.get("boundary_ts") or 0) != float(boundary_ts or 0):
            _ctx = {}
        _last_sum = _ctx.get("last_summarized_turns")
        _due = (not isinstance(_last_sum, (int, float))
                or int(history_meta["summarized_turns"]) - int(_last_sum) >= HISTORY_NOTICE_STEP_TURNS)
        if _due:
            context_notice = {
                "budget_chars": HISTORY_TOTAL_BUDGET,
                "total_chars_before": history_meta["total_chars_before"],
                "summarized_turns": history_meta["summarized_turns"],
                "verbatim_turns": history_meta["verbatim_turns"],
                "message": (f"上下文已超3万字（共{history_meta['total_chars_before']}字），"
                            f"更早的{history_meta['summarized_turns']}条已自动压缩为摘要，"
                            f"最近{history_meta['verbatim_turns']}条保留原文，话题未结束也可继续聊。"),
            }
            try:
                tmp = context_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump({"boundary_ts": float(boundary_ts or 0),
                               "last_summarized_turns": int(history_meta["summarized_turns"]),
                               "last_total_chars": int(history_meta["total_chars_before"]),
                               "last_notified_ts": now}, f)
                os.replace(tmp, context_path)
            except OSError:
                pass

payload = {"count": len(new), "messages": out, "history": history,
           "orphaned": orphan_info, "preempted": preempt_info,
           "starved_priority": starved_priority,
           "debounce_waiting": waiting, "queued": len(pending_entries),
           "history_meta": history_meta}
_resumed_ids = [str(o.get("msgid")) for o in orphan_info
                if o.get("resume")]
if _resumed_ids:
    payload["resumed_msgids"] = _resumed_ids
if failed_entries:
    # Informational only: these batches were declared dead this
    # poll and fail-stopped by the hook itself (cancelled + failure
    # notice sent). They are NOT work for this wake.
    payload["failed_tasks"] = [{"msgid": e["msgid"],
                                "text": (e.get("text") or "")[:200]}
                               for e in failed_entries]
# 2026-10-08 (user order, weixin only): compression still happens
# and history_meta still reports it, but the notice is never
# surfaced to the user - context_notice is deliberately NOT
# added to the payload on this channel.
if sub_notices_out:
    payload["subagent_notices"] = sub_notices_out
if job_wake is not None:
    _jid, _jrec, _is_retry = job_wake
    _pseudo = {"msgid": _jrec.get("msgid"), "text": _jrec.get("text") or "",
               "ts": _jrec.get("ts_dispatched") or now, "media": []}
    if entries:
        _last_e = entries[-1]
        for _k in ("from_user_id", "group_id", "from_userid",
                   "chattype", "chatid", "msgtype"):
            if _k in _last_e:
                _pseudo[_k] = _last_e[_k]
    payload = {"count": 1,
               "messages": [{
                   "msgid": _pseudo.get("msgid"),
                   "from_user_id": _pseudo.get("from_user_id"),
                   "group_id": _pseudo.get("group_id"),
                   "text": (_pseudo.get("text") or "")[:2000],
                   "media": [],
               }],
               "history": build_history([_pseudo])[0],
               "orphaned": [], "preempted": None, "starved_priority": None,
               "debounce_waiting": False, "queued": len(pending_entries),
               "kind": "subagent_job",
               "subagent_job": {"id": _jid, "msgid": _jrec.get("msgid"),
                                "text": _jrec.get("text") or ""}}
    if _is_retry:
        payload["job_retry"] = True
print(json.dumps(payload, ensure_ascii=False))
PYEOF
)"

COUNT="$(printf '%s' "$PAYLOAD" | jq -r '.count // 0')"
if [[ "$COUNT" == "0" ]]; then
  if [[ "$(printf '%s' "$PAYLOAD" | jq -r '.debounce_waiting // false')" == "true" ]]; then
    silent "gathering: message(s) queued while current batch is in flight" '{}'
  else
    silent "no new weixin messages" '{}'
  fi
else
  wake "new Weixin message(s) for the agent" "$PAYLOAD"
fi
