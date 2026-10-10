"""WeCom outbound coalescer v2 (idle-gap).

Loaded by path from ``wecom-bot/gateway.py`` (``_load_coalescer_cls``).
The gateway feeds every main-queue row through ``feed`` and, once per
outbox tick, releases quiet buffers with ``due``.

Contract this module implements:

* ``Coalescer()`` takes no arguments.
* ``feed`` buffers a ``mode=="send"`` row that has an ``id``, grouped
  by ``(chatid, chat_type)``. Consecutive sends for that chat join
  ``content`` with a newline and ``feed`` returns ``[]``.
* Any other row is a barrier (``reply``, ``reply_notice``, a send
  with no id, …). Pending merged sends are flushed first — every
  open chat, because a barrier often carries no chat id — and the
  barrier itself is last.
* A flushed send keeps the first row's id and fields, sets
  ``content`` to the joined text, and sets ``merged_ids`` to the
  original ids in arrival order. The gateway writes the extra
  ``merged_into`` result rows from ``merged_ids``.
* ``due`` flushes buffers whose idle gap or max-wait has elapsed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# The outbox loop sleeps 1.0s and calls due() before reading new
# rows. 0.8s sits at the low end of the 0.8–1.5s band so the lone
# send in the alignment test is released on the next tick even when
# that sleep returns a little early, and still inside the 10s budget.
IDLE_GAP_SECS = 0.8

# A chat that keeps receiving sends inside the idle gap still
# flushes, so a burst cannot sit in the buffer forever.
MAX_WAIT_SECS = 8.0


def _session_key(item: dict) -> tuple:
    """Group sends that belong to the same WeCom chat.

    ``chat_type`` is kept as stored (int ``1`` and str ``"1"`` stay
    distinct) so two producers cannot be merged by accident.
    """
    return (str(item.get("chatid") or ""), item.get("chat_type"))


def _content_text(item: dict) -> str:
    """Send body as text. A missing body joins as an empty segment."""
    content = item.get("content")
    if content is None:
        return ""
    return str(content)


@dataclass
class _PendingSend:
    """One chat's not-yet-flushed send fragments, in arrival order."""

    first_item: dict
    ids: list[str] = field(default_factory=list)
    contents: list[str] = field(default_factory=list)
    started_at: float = 0.0
    last_at: float = 0.0


class Coalescer:
    """Idle-gap coalescer for unbound WeCom ``send`` rows.

    Construct with ``Coalescer()``. ``feed`` and ``due`` are synchronous;
    the gateway awaits only the dispatch of the rows they return.
    """

    def __init__(self) -> None:
        self._pending: dict[tuple, _PendingSend] = {}
        self._order: list[tuple] = []

    def feed(self, item: dict) -> list[dict]:
        """Buffer a send, or flush every pending send and return a barrier.

        Returns ready rows in dispatch order. A buffered send contributes
        nothing until a barrier or ``due``. The barrier object is the
        same dict the caller passed in.
        """
        if not isinstance(item, dict):
            raise TypeError(
                f"Coalescer.feed expects a dict, got {type(item).__name__}"
            )
        if item.get("mode") == "send" and item.get("id"):
            self._append_send(item)
            return []
        ready = self._flush_all()
        ready.append(item)
        return ready

    def due(self) -> list[dict]:
        """Flush buffers past the idle gap or the max-wait cap.

        Chats that are due are released in the order they were first
        buffered. A chat still inside both windows stays buffered.
        """
        now = time.monotonic()
        ready: list[dict] = []
        for key in list(self._order):
            pending = self._pending.get(key)
            if pending is None:
                continue
            idle_for = now - pending.last_at
            waited = now - pending.started_at
            if idle_for >= IDLE_GAP_SECS or waited >= MAX_WAIT_SECS:
                ready.append(self._emit(key))
        return ready

    def _append_send(self, item: dict) -> None:
        """Append one send onto its chat buffer, opening the chat if needed."""
        key = _session_key(item)
        now = time.monotonic()
        row_id = str(item["id"])
        text = _content_text(item)
        pending = self._pending.get(key)
        if pending is None:
            self._pending[key] = _PendingSend(
                first_item=dict(item),
                ids=[row_id],
                contents=[text],
                started_at=now,
                last_at=now,
            )
            self._order.append(key)
            return
        pending.ids.append(row_id)
        pending.contents.append(text)
        pending.last_at = now

    def _emit(self, key: tuple) -> dict:
        """Pop one chat buffer and return the merged send row."""
        pending = self._pending.pop(key)
        self._order.remove(key)
        ready = dict(pending.first_item)
        ready["mode"] = "send"
        ready["id"] = pending.ids[0]
        ready["content"] = "\n".join(pending.contents)
        ready["merged_ids"] = list(pending.ids)
        return ready

    def _flush_all(self) -> list[dict]:
        """Emit every open chat buffer, oldest chat first."""
        ready: list[dict] = []
        for key in list(self._order):
            if key in self._pending:
                ready.append(self._emit(key))
        return ready
