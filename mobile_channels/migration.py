"""Conservative migration preview from a stopped legacy snapshot into a new DB.

All imported tasks remain reconciling. Byte evidence for offsets, tombstones,
partial sends, approvals and malformed records remains in shadow tables; this
preview never treats unexplained old state as safe to execute or delivered.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Mapping

from .maintenance import backup_database
from .models import InboundMessage
from .shadow import import_shadow
from .storage import Store


def _message(channel: str, account: str, row: dict) -> InboundMessage:
    """Require an explicit account and original route, never administrator defaults."""
    mid = row.get('msgid')
    sender = (row.get('from_user_id') or row.get('from_user')) if channel == 'weixin' else row.get('from_userid')
    if channel == 'weixin':
        chat = row.get('group_id') or sender
        kind = 'group' if row.get('group_id') else 'single'
    else:
        chat, kind = row.get('chatid'), row.get('chattype')
    if kind not in ('single', 'group') or not all(isinstance(value, str) and value.strip()
            for value in (account, mid, sender, chat)):
        raise ValueError('Incomplete original account or conversation route')
    text = row.get('text', '')
    if not isinstance(text, str):
        raise ValueError('Invalid legacy message text')
    return InboundMessage(channel, account, kind, chat, mid, sender, text)


def migrate_snapshot(workspace: Path, destination: Path, *, accounts: Mapping[str, str],
                     snapshot_confirmed: bool = False) -> dict:
    """Publish a nonexecuting migration preview only after all local writes succeed.

    The operator must supply a stopped-writer, internally consistent snapshot.
    This cannot be established by inspecting file timestamps across files.
    A new target is required. Nothing edits or executes the legacy snapshot.
    """
    if not snapshot_confirmed:
        raise ValueError('Confirm that workspace is a stopped-writer snapshot')
    workspace, destination = Path(workspace).resolve(), Path(destination).absolute()
    if destination.resolve().is_relative_to(workspace):
        raise ValueError('Migration output must be outside the read-only legacy snapshot')
    if destination.exists() or destination.is_symlink():
        raise FileExistsError('Migration destination must be new')
    if not accounts or any(channel not in ('weixin', 'wecom') or not isinstance(account, str)
                           or not account.strip() for channel, account in accounts.items()):
        raise ValueError('Explicit nonempty account identifiers are required')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.migration-', dir=destination.parent) as directory:
        temporary = Path(directory) / 'preview.sqlite3'
        report = import_shadow(workspace, temporary)
        report.update(mode='migration_preview', imported_tasks=0, duplicate_messages=0,
                      unsupported_records=0, auto_handled=0, executable=False,
                      selected_channels=sorted(accounts))
        with Store(temporary) as store:
            store.connection.execute('''CREATE TABLE migration_sources (
                path TEXT NOT NULL, digest TEXT NOT NULL, start INTEGER NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(id), PRIMARY KEY(path,digest,start))''')
            sources = store.connection.execute(
                'SELECT path,digest,start,kind,payload FROM shadow_records ORDER BY path,start').fetchall()
            for source in sources:
                path = Path(source['path'])
                channel = next((name for name in accounts if
                    path == workspace / f'{name}-bot/state/inbox.jsonl' or
                    path == workspace / f'native-bridge/spool/{name}.jsonl'), None)
                if channel is None:
                    continue
                if source['kind'] != 'complete':
                    report['unsupported_records'] += 1
                    continue
                row = json.loads(source['payload'])
                if not isinstance(row, dict):
                    report['unsupported_records'] += 1
                    continue
                if row.get('auto_handled'):
                    report['auto_handled'] += 1
                    continue
                try:
                    message = _message(channel, accounts[channel], row)
                except ValueError:
                    report['unsupported_records'] += 1
                    continue
                result = store.ingest(message, initial_state='reconciling')
                report['imported_tasks' if result.created else 'duplicate_messages'] += 1
                store.connection.execute('INSERT INTO migration_sources VALUES (?,?,?,?)',
                    (source['path'], source['digest'], source['start'], result.task_id))
            store.connection.execute('''CREATE TABLE migration_report (
                id INTEGER PRIMARY KEY CHECK(id=1), report TEXT NOT NULL)''')
            store.connection.execute('INSERT INTO migration_report VALUES (1,?)',
                                     (json.dumps(report, sort_keys=True),))
        backup_database(temporary, destination)
    return report
