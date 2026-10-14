#!/usr/bin/env python3
"""Tests for the v2 (idle-gap) WeCom outbound coalescer.

Adapted 2026-10-14 to the production-integrated API of
``wecom_outbound_coalesce_v2.py`` (the API ``wecom-bot/gateway.py``
actually loads via ``_load_coalescer_cls``). The original version of
this file was written against the draft API and died with
``TypeError: Coalescer.__init__() got an unexpected keyword
argument 'idle_ms'``.

API differences the adaptation is built on:

* ``Coalescer()`` takes NO arguments. The idle gap and the max-wait
  cap are module constants (``IDLE_GAP_SECS`` / ``MAX_WAIT_SECS``),
  so tests set those constants instead of constructor kwargs.
* ``feed(item)`` and ``due()`` take no ``now`` argument; the module
  reads ``time.monotonic()`` itself. Tests therefore swap the
  module's ``time`` reference for a fake clock (see ``rig``) rather
  than passing timestamps in — the ordering/idle/max-wait semantics
  under test are unchanged.
* A flushed merged send keeps the FIRST row's ``id`` and carries
  ``merged_ids`` in arrival order; the gateway accounts one result
  row per original id from ``merged_ids`` (extra rows carry
  ``merged_into`` = first id). Tests assert both fields.

Original-case mapping (old draft API -> this file):

* test_three_sends_within_idle_gap_merge_into_one  -> same name
* test_idle_gap_splits_batches                     -> same name
* test_arrivals_reset_idle_clock_unlike_v1_fixed_window -> same name
* test_max_wait_cap_flushes_despite_continuous_arrivals -> same name
* test_single_send_released_after_idle             -> same name
* test_different_chatid_never_merges               -> same name
* test_reply_is_barrier_and_flushes_first          -> same name
* test_send_file_is_barrier_media_flushes_immediately -> same name

UNMAPPED original cases (dropped, with reasons — not silently):

* test_max_chars_splits_before_overflow: the production module has
  no ``max_chars`` knob and no size-based splitting at all; the
  constructor takes no arguments, so there is no API surface left
  to pin this behaviour against. (The gateway enforces message size
  limits elsewhere, outside the coalescer.)
* test_max_parts_splits: same reason — no ``max_parts`` knob and no
  part-count splitting exists in the production module; a buffer
  ends only on the idle gap, the max-wait cap, or a barrier.

Added cases pinning behaviour the production module documents but
the draft tests never covered: a barrier flushes EVERY open chat,
a ``send`` row without an ``id`` is a barrier, grouping is by
``(chatid, chat_type)`` with the type kept as stored, and a row
whose body lives in ``text`` still merges with its text intact
(2026-10-14 alert fix in ``_content_text``).
"""
import contextlib
import sys
from types import SimpleNamespace

sys.path.insert(0, "/home/hatch/workspace/acceptance-fix-2026-10-08")
import wecom_outbound_coalesce_v2 as mod


class _Clock:
    """Manually advanced monotonic clock for the module under test."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def set(self, t):
        self.now = float(t)


@contextlib.contextmanager
def rig(idle_secs=1.0, max_wait_secs=60.0):
    """A Coalescer on a fake clock with the given module constants.

    Restores the module's ``time`` reference and both constants on
    exit, so tests stay independent and production defaults
    (IDLE_GAP_SECS=0.8, MAX_WAIT_SECS=8.0) are never left mutated.
    """
    clock = _Clock()
    old_time = mod.time
    old_idle, old_max = mod.IDLE_GAP_SECS, mod.MAX_WAIT_SECS
    mod.time = SimpleNamespace(monotonic=clock.monotonic)
    mod.IDLE_GAP_SECS = idle_secs
    mod.MAX_WAIT_SECS = max_wait_secs
    try:
        yield mod.Coalescer(), clock
    finally:
        mod.time = old_time
        mod.IDLE_GAP_SECS, mod.MAX_WAIT_SECS = old_idle, old_max


def row(i, chat="testuser", mode="send", text=None, chat_type=None):
    r = {"id": i, "mode": mode, "chatid": chat,
         "content": text if text is not None else f"part-{i}"}
    if chat_type is not None:
        r["chat_type"] = chat_type
    return r


def test_three_sends_within_idle_gap_merge_into_one():
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        assert c.feed(row("a", text="A")) == []
        clock.set(0.4)
        assert c.feed(row("b", text="B")) == []
        clock.set(0.8)
        assert c.feed(row("c", text="C")) == []
        clock.set(1.9)  # 1.1s after last part
        out = c.due()
        assert len(out) == 1, out
        assert out[0]["content"] == "A\nB\nC"
        assert out[0]["merged_ids"] == ["a", "b", "c"]
        # Per-original accounting anchor: first row's id wins.
        assert out[0]["id"] == "a"


def test_idle_gap_splits_batches():
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="A"))
        clock.set(1.2)  # idle gap reached -> A goes alone
        out = c.due()
        assert len(out) == 1 and out[0]["content"] == "A"
        clock.set(1.3)
        c.feed(row("b", text="B"))
        clock.set(2.4)
        out = c.due()
        assert len(out) == 1 and out[0]["content"] == "B"


def test_arrivals_reset_idle_clock_unlike_v1_fixed_window():
    # v1 flushed 4s after the FIRST part no matter what arrived.
    # v2: parts every 0.6s keep the buffer alive past 4s total.
    with rig(idle_secs=1.0, max_wait_secs=60.0) as (c, clock):
        t = 0.0
        for i in range(8):
            clock.set(t)
            assert c.feed(row(str(i), text=f"p{i}")) == []
            clock.set(t + 0.5)
            assert c.due() == []  # never idle for a full 1s
            t += 0.6
        clock.set(t + 1.1)
        out = c.due()
        assert len(out) == 1, out
        assert out[0]["merged_ids"] == [str(i) for i in range(8)]


def test_max_wait_cap_flushes_despite_continuous_arrivals():
    with rig(idle_secs=1.0, max_wait_secs=5.0) as (c, clock):
        t = 0.0
        flushed = []
        flushed_mid_stream = []
        for i in range(20):
            clock.set(t)
            flushed.extend(c.feed(row(str(i), text=f"p{i}")))
            out = c.due()
            flushed.extend(out)
            if out:
                flushed_mid_stream.extend(out)
            t += 0.6
        # Something must have flushed at the 5s cap even though the
        # stream never went idle; the rest follows after.
        assert flushed_mid_stream, \
            "max-wait cap never flushed a continuous stream"
        clock.set(t + 2)
        flushed.extend(c.due())
        sent_ids = [i for o in flushed for i in o["merged_ids"]]
        assert sent_ids == [str(i) for i in range(20)]  # order, no loss


def test_single_send_released_after_idle():
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="only"))
        clock.set(0.9)
        assert c.due() == []
        clock.set(1.0)
        out = c.due()
        assert len(out) == 1 and out[0]["content"] == "only"
        clock.set(5.0)
        assert c.due() == []  # due() is idempotent


def test_different_chatid_never_merges():
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", chat="x", text="A"))
        clock.set(0.2)
        c.feed(row("b", chat="y", text="B"))
        clock.set(2.0)
        out = c.due()
        assert len(out) == 2
        assert {o["content"] for o in out} == {"A", "B"}


def test_reply_is_barrier_and_flushes_first():
    with rig(idle_secs=60.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="A"))
        clock.set(0.1)
        out = c.feed(row("r", mode="reply", text="R"))
        assert [o["content"] for o in out] == ["A", "R"]
        assert out[0]["merged_ids"] == ["a"] and out[0]["id"] == "a"
        assert out[1]["mode"] == "reply"


def test_send_file_is_barrier_media_flushes_immediately():
    with rig(idle_secs=60.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="A"))
        clock.set(0.1)
        out = c.feed(row("f", mode="send_file", text=""))
        assert [o["mode"] for o in out] == ["send", "send_file"]
        assert out[0]["content"] == "A"


def test_barrier_flushes_every_open_chat():
    # Production contract: a barrier often carries no chat id, so it
    # flushes every pending buffer (oldest chat first), itself last.
    with rig(idle_secs=60.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", chat="x", text="A"))
        clock.set(0.1)
        c.feed(row("b", chat="y", text="B"))
        clock.set(0.2)
        out = c.feed(row("r", mode="reply", text="R"))
        assert [o["content"] for o in out] == ["A", "B", "R"]


def test_send_without_id_is_barrier():
    # Only a send WITH an id buffers; an id-less send flushes first
    # and then passes through itself.
    with rig(idle_secs=60.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="A"))
        clock.set(0.1)
        noid = {"mode": "send", "chatid": "testuser", "content": "N"}
        out = c.feed(noid)
        assert [o["content"] for o in out] == ["A", "N"]
        assert out[1] is noid  # barrier object passes through as-is


def test_grouping_includes_chat_type_as_stored():
    # _session_key keeps chat_type as stored: int 1 and str "1" are
    # different groups and must never merge into one send.
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        c.feed(row("a", text="A", chat_type=1))
        clock.set(0.2)
        c.feed(row("b", text="B", chat_type="1"))
        clock.set(2.0)
        out = c.due()
        assert len(out) == 2
        assert {o["content"] for o in out} == {"A", "B"}


def test_row_text_field_fallback_merges():
    # _content_text falls back to ``text`` (2026-10-14 alert fix):
    # merging such a row must keep its body, not empty it.
    with rig(idle_secs=1.0) as (c, clock):
        clock.set(0.0)
        c.feed({"id": "a", "mode": "send", "chatid": "testuser",
                "text": "alert body"})
        clock.set(0.2)
        c.feed(row("b", text="B"))
        clock.set(2.0)
        out = c.due()
        assert len(out) == 1
        assert out[0]["content"] == "alert body\nB"
        assert out[0]["merged_ids"] == ["a", "b"]


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"{len(fns)} tests passed")
