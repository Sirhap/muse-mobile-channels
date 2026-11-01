"""Read-only legacy inventory, deliberately separate from executable task state.

Records preserve byte ranges and digests, including partial/corrupt input. No
legacy message is inferred to be safe to execute. Reconciliation is an explicit
prerequisite before any future production migration.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

CHANNELS = ('weixin', 'wecom')
STATE_NAMES = (
    'inbox.jsonl', 'outbox.jsonl', 'outbox_results.jsonl', 'seen_ids.jsonl',
    'outbox.offset', 'outbox_retry.json', 'outbox_partial.json',
    'outbox_parked.json', 'cancelled.json', 'reqmap.json',
)


def source_paths(workspace: Path) -> list[Path]:
    """Enumerate only known state sources, never credentials or media content."""
    paths = [workspace / f'{ch}-bot' / 'state' / name for ch in CHANNELS for name in STATE_NAMES]
    paths += [workspace / 'native-bridge' / 'state.json', workspace / 'approval-relay' / 'state.json',
              workspace / 'approval-relay' / 'decisions.jsonl', workspace / 'approval-relay' / 'decisions.offset']
    paths += [workspace / 'native-bridge' / 'spool' / f'{ch}.jsonl' for ch in CHANNELS]
    paths += [workspace / 'native-bridge' / f'queue_admin-{ch}.jsonl' for ch in CHANNELS]
    return paths


def _records(data: bytes, jsonl: bool) -> list[tuple[int, int, str, bytes]]:
    """Retain all bytes; incomplete JSONL tails never count as complete rows."""
    rows = []
    offset = 0
    for raw in data.splitlines(keepends=True) if jsonl else [data]:
        end = offset + len(raw)
        kind = 'complete'
        if jsonl and not raw.endswith(b'\n'):
            kind = 'partial'
        else:
            try:
                json.loads(raw)
            except (ValueError, UnicodeError):
                kind = 'invalid'
        rows.append((offset, end, kind, raw))
        offset = end
    return rows


def import_shadow(workspace: Path, database: Path) -> dict:
    """Capture idempotent file revisions and classify bytes, without scheduling work.

    Run against a stopped-writer snapshot for authoritative comparisons. A file
    changing during a read aborts the transaction. Separate files cannot form a
    coherent live snapshot; the report always requires reconciliation.
    """
    workspace = workspace.resolve()
    database = database.resolve()
    sources = source_paths(workspace)
    if database in {path.resolve() for path in sources} or (database.exists() and any(
            path.exists() and os.path.samefile(database, path) for path in sources)):
        raise ValueError('Shadow database must not replace a legacy source')
    database.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600) if not database.exists() else None
    if descriptor is not None:
        os.close(descriptor)
    connection = sqlite3.connect(database, timeout=5)
    try:
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA synchronous=FULL')
        connection.executescript('''
            CREATE TABLE IF NOT EXISTS shadow_sources (
                path TEXT NOT NULL, digest TEXT NOT NULL, size INTEGER NOT NULL,
                PRIMARY KEY(path, digest)
            );
            CREATE TABLE IF NOT EXISTS shadow_records (
                path TEXT NOT NULL, digest TEXT NOT NULL, start INTEGER NOT NULL,
                end INTEGER NOT NULL, kind TEXT NOT NULL, payload BLOB NOT NULL,
                PRIMARY KEY(path, digest, start),
                FOREIGN KEY(path, digest) REFERENCES shadow_sources(path, digest)
            );
        ''')
        report = {'mode': 'shadow', 'reconciliation_required': True,
                  'files': 0, 'missing': 0, 'inserted_revisions': 0,
                  'complete': 0, 'partial': 0, 'invalid': 0}
        with connection:
            for path in sources:
                try:
                    handle = path.open('rb')
                except FileNotFoundError:
                    report['missing'] += 1
                    continue
                with handle:
                    before = os.fstat(handle.fileno())
                    data = handle.read()
                    after = os.fstat(handle.fileno())
                    try:
                        current = path.stat()
                    except FileNotFoundError as error:
                        raise RuntimeError(f'Legacy source disappeared while reading: {path.name}') from error
                stamp = lambda value: (value.st_ino, value.st_size, value.st_mtime_ns)
                if stamp(before) != stamp(after) or stamp(after) != stamp(current):
                    raise RuntimeError(f'Legacy source changed while reading: {path.name}')
                digest = hashlib.sha256(data).hexdigest()
                source = str(path)
                cursor = connection.execute('INSERT OR IGNORE INTO shadow_sources VALUES (?, ?, ?)', (source, digest, len(data)))
                report['inserted_revisions'] += cursor.rowcount
                report['files'] += 1
                for start, end, kind, raw in _records(data, path.suffix == '.jsonl'):
                    report[kind] += 1
                    connection.execute('INSERT OR IGNORE INTO shadow_records VALUES (?, ?, ?, ?, ?, ?)',
                                       (source, digest, start, end, kind, raw))
        return report
    finally:
        connection.close()
