"""Durable review for executions whose end has not been confirmed.

Review is neither successful handoff nor worker liveness. It removes permission
to automatically replay/cancel an unknown execution while leaving new work free
to run. All notification routes come from the original inbox row.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import uuid

_ROOT = str(Path(__file__).resolve().parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from channel_common import atomic_write_text
from credential_health import append_health_notice_once


class WorkerReview:
    """Persist review decisions before they can affect scheduler ownership."""

    def __init__(self, path, outbox, channel, now, *, dry=False):
        """Load the channel-local review ledger; corruption fails closed."""
        self.path = Path(path)
        self.outbox = Path(outbox)
        self.channel = channel
        self.now = now
        self.dry = dry
        try:
            self.state = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self.state = {"tasks": {}}
        if not isinstance(self.state, dict) or not isinstance(self.state.get("tasks"), dict):
            raise ValueError("invalid worker review ledger")
        if any(not isinstance(record, dict) or record.get("status") not in {"pending", "resolved"}
               for record in self.state["tasks"].values()):
            raise ValueError("invalid worker review task")

    @property
    def pending_ids(self):
        """Return blocked message IDs without treating review as live activity."""
        return {mid for mid, record in self.state["tasks"].items()
                if isinstance(record, dict) and record.get("status") == "pending"}

    def _save(self):
        """Replace private state and sync its directory before scheduler changes."""
        if self.dry:
            return
        atomic_write_text(self.path, json.dumps(self.state, ensure_ascii=False), mode=0o600)
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def record(self, msgid, info, last_activity):
        """Retain why an execution needs inspection, including its current run."""
        mid = str(msgid)
        record = self.state["tasks"].get(mid) or {}
        run = info.get("worker_run_id") or record.get("run_id")
        if record.get("status") != "pending" or record.get("run_id") != run:
            record = {"msgid": mid, "run_id": run, "first_seen_at": self.now,
                      "notice_id": f"worker-review-{self.channel}-{uuid.uuid4().hex}",
                      "notice_enqueued": False, "notice_delivered": False,
                      "notice_attempt": 0}
        record.update(status="pending", last_seen_at=self.now,
                      silence_secs=max(0, int(self.now - last_activity)),
                      last_activity_at=last_activity, source=info.get("source"),
                      reason=info.get("platform_verdict_reason") or "platform_state_unknown",
                      degraded=list(info.get("degraded") or []))
        self.state["tasks"][mid] = record
        self._save()

    def resolve(self, msgid, reason):
        """Release review only after a separate affirmative resolution signal."""
        record = self.state["tasks"].get(str(msgid))
        if isinstance(record, dict) and record.get("status") == "pending":
            record.update(status="resolved", resolved_at=self.now, resolution=reason)
            self._save()

    def flush_notices(self, entries, results):
        """Queue a stable review notice and track its real delivery result."""
        if self.dry:
            return
        by_id = {}
        for row in results:
            by_id.setdefault(str(row.get("id") or ""), []).append(row)
        for mid in self.pending_ids:
            record = self.state["tasks"][mid]
            if record.get("notice_delivered"):
                continue
            qid = record.get("queued_id")
            verdicts = by_id.get(qid, []) if qid else []
            if any(row.get("ok") is True for row in verdicts):
                record["notice_delivered"] = True
                self._save()
                continue
            if qid and any(row.get("deadletter") for row in verdicts):
                # A confirmed unsent dead letter may retry under a new queue ID.
                record["notice_attempt"] = int(record.get("notice_attempt", 0)) + 1
                record.update(notice_enqueued=False, queued_id=None)
                self._save()
            if record.get("notice_enqueued"):
                continue
            entry = entries.get(mid) or {}
            summary = " ".join(str(entry.get("text") or "（无文字任务）").split())[:20]
            text = ("⚠️ 任务「" + summary + "」长时间没有新的进度，执行状态待核查。已登记待核查，"
                    "等待主会话或管理员接手；尚未确认原执行结束，不会自动重派或取消。")
            row = {"id": f"{record['notice_id']}-{record.get('notice_attempt', 0)}",
                   "mode": "send", "queued_at": int(self.now), "content": text}
            if self.channel == "weixin":
                target = entry.get("from_user_id")
                if not target:
                    record["notice_error"] = "route_missing"
                    self._save()
                    continue
                row["to_user_id"] = target
            elif self.channel == "wecom":
                target = entry.get("chatid")
                if not target:
                    record["notice_error"] = "route_missing"
                    self._save()
                    continue
                row.update(chatid=target, chat_type=2 if entry.get("chattype") == "group" else 1)
            else:
                raise ValueError("unsupported review channel")
            record["queued_id"] = row["id"]
            self._save()
            try:
                append_health_notice_once(self.outbox, row)
            except (OSError, ValueError):
                record["notice_error"] = "outbox_unavailable"
                self._save()
                continue
            record["notice_enqueued"] = True
            record.pop("notice_error", None)
            self._save()
