#!/usr/bin/env python3
"""Publish / update a turn's progress checklist for the native bridge.

The bridge (native_bridge.py, todo 9) folds ONE summary line —
「清单 k/n：当前未完成的项」 — into the existing progress bubble
when a checklist file exists for the turn. The file lives at

    <native-bridge>/progress_lists/<channel>-<msgid>.json

with shape {"items": [{"text": "...", "done": false}, ...]} and is
always replaced atomically (tmp file in the same dir + rename), so
the bridge never reads a half-written list. A missing, corrupt or
empty list simply means the bubble carries no checklist line.

Usage (one line per update, safe to call from any executor):

    progress_list.py set weixin <msgid> "第一项" "第二项" ...
    progress_list.py done weixin <msgid> 2     # mark item 2 (1-based) done
    progress_list.py clear weixin <msgid>      # remove the list

Importable API: write_list(base, channel, msgid, items),
mark_done(base, channel, msgid, n), clear_list(base, channel, msgid).
`base` is the native-bridge directory (the one holding state.json);
the CLI defaults it to this script's own directory, override with
--base.
"""
import argparse
import json
import os
import sys


def _atomic_write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_list(base, channel, msgid, items):
    """Replace the turn's checklist. items: [{"text", "done"}, ...]."""
    clean = [{"text": str(it["text"]), "done": bool(it.get("done"))}
             for it in items]
    _atomic_write(_path(base, channel, msgid), {"items": clean})


def read_list(base, channel, msgid):
    with open(_path(base, channel, msgid), encoding="utf-8") as f:
        return json.load(f)["items"]


def mark_done(base, channel, msgid, n):
    """Mark item n (1-based) done, preserving the rest of the list."""
    items = read_list(base, channel, msgid)
    if not 1 <= n <= len(items):
        raise SystemExit(f"item {n} out of range (1..{len(items)})")
    items[n - 1]["done"] = True
    write_list(base, channel, msgid, items)


def clear_list(base, channel, msgid):
    try:
        os.unlink(_path(base, channel, msgid))
    except OSError:
        pass


def _path(base, channel, msgid):
    # Same convention as native_bridge.progress_list_path; spelled
    # out here so this helper stays importable/runnable without
    # pulling in the whole bridge module.
    root = base or os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "progress_lists",
                        f"{channel}-{msgid}.json")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default=None,
                    help="native-bridge dir (default: this script's dir)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_set = sub.add_parser("set", help="replace the list (all undone)")
    p_set.add_argument("channel")
    p_set.add_argument("msgid")
    p_set.add_argument("items", nargs="+")
    p_done = sub.add_parser("done", help="mark item N (1-based) done")
    p_done.add_argument("channel")
    p_done.add_argument("msgid")
    p_done.add_argument("n", type=int)
    p_clear = sub.add_parser("clear", help="remove the list")
    p_clear.add_argument("channel")
    p_clear.add_argument("msgid")
    args = ap.parse_args(argv)

    if args.cmd == "set":
        write_list(args.base, args.channel, args.msgid,
                   [{"text": t, "done": False} for t in args.items])
    elif args.cmd == "done":
        mark_done(args.base, args.channel, args.msgid, args.n)
    elif args.cmd == "clear":
        clear_list(args.base, args.channel, args.msgid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
