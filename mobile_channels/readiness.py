"""Read-only cutover checks; experimental core initialization is not live acceptance."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .executors import validate_transport_source
from .storage import SCHEMA_VERSION


class ReadinessError(RuntimeError):
    """An inventory database cannot be activated as an executable authority."""


def _open_readonly(path: Path) -> sqlite3.Connection:
    """Open an existing SQLite file without creating an authority on typo paths."""
    connection = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA query_only=ON')
    return connection


def ensure_executable_database(path: Path) -> None:
    """Reject shadow inventories and unfinalized migration previews before startup.

    A newly initialized core database may run experimental text workflows. This
    check does not certify production readiness, platform capability or absence
    of in-flight work. No migration finalizer currently exists, so any database
    containing the preview marker is refused, even if its JSON is hand-edited.
    """
    connection = _open_readonly(path)
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'migration_report' in tables:
            raise ReadinessError('Migration preview is nonexecuting; reconcile it with a verified migration finalizer before activation')
        required = {'schema_migrations', 'conversations', 'inbound_messages', 'tasks',
                    'execution_attempts', 'task_events', 'outbound_messages'}
        if not required <= tables:
            raise ReadinessError('Database is not an initialized executable core; shadow inventory alone cannot be activated')
        if connection.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION:
            raise ReadinessError('Core schema version does not match this runtime')
        if [row[0] for row in connection.execute('PRAGMA quick_check')] != ['ok']:
            raise ReadinessError('Core database integrity check failed')
        if connection.execute('PRAGMA foreign_key_check').fetchone() is not None:
            raise ReadinessError('Core database contains invalid foreign key references')
    finally:
        connection.close()


def inspect_readiness(database: Path, modes: Path) -> dict:
    """Report blockers using local metadata only, without credentials or network IO.

    No local flag can assert that the current legacy Muse SDK actually supplies
    verifiable run correlation. Live platform acceptance remains a blocker until
    a dedicated, evidence-bearing capability check is implemented and exercised.
    """
    blockers = []
    warnings = ['Media execution and unported legacy commands require compatibility acceptance before cutover']
    report = {'ready': False, 'experimental_database_allowed': False,
              'run_correlation_verified': False, 'blockers': blockers, 'warnings': warnings,
              'configured_accounts': 0}
    try:
        ensure_executable_database(database)
        report['experimental_database_allowed'] = True
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        blockers.append({'code': 'database_not_executable', 'detail': str(error)})
    scopes = set()
    try:
        document = json.loads(Path(modes).read_text(encoding='utf-8'))
        channels = document.get('channels') if isinstance(document, dict) else None
        if not isinstance(channels, dict) or not channels:
            raise ValueError('nonempty channels mode configuration is required')
        for channel, settings in channels.items():
            if channel not in ('weixin', 'wecom') or not isinstance(settings, dict):
                raise ValueError('unsupported channel or invalid mode settings')
            if settings.get('mode') == 'legacy':
                continue
            if settings.get('mode') != 'core':
                raise ValueError('channel mode must be legacy or core')
            account, senders = settings.get('account_id'), settings.get('allowed_senders')
            if not isinstance(account, str) or not account.strip():
                raise ValueError('core accounts require an explicit account_id')
            if (not isinstance(senders, list) or not senders or
                    any(not isinstance(sender, str) or not sender.strip() for sender in senders)):
                raise ValueError('core accounts require explicit allowed_senders')
            target = settings.get('database')
            if not isinstance(target, str) or not Path(target).is_absolute() or Path(target).resolve() != Path(database).resolve():
                raise ValueError('configured account database must match the inspected absolute path')
            scopes.add((channel, account))
        if not scopes:
            raise ValueError('no core accounts are configured')
        report['configured_accounts'] = len(scopes)
    except (OSError, ValueError, TypeError, KeyError) as error:
        blockers.append({'code': 'channel_modes_invalid', 'detail': str(error)})
    if report['experimental_database_allowed']:
        connection = _open_readonly(database)
        try:
            routes = set(map(tuple, connection.execute('SELECT DISTINCT channel,account_id FROM conversations')))
            if routes - scopes:
                blockers.append({'code': 'unowned_accounts', 'detail': 'Database includes accounts outside the configured core scopes'})
            uncertain = connection.execute("SELECT COUNT(*) FROM tasks WHERE state='reconciling'").fetchone()[0]
            if uncertain:
                blockers.append({'code': 'execution_reconciliation_required', 'count': uncertain})
            pending = connection.execute("SELECT COUNT(*) FROM outbound_messages WHERE state='delivery_unknown'").fetchone()[0]
            if pending:
                blockers.append({'code': 'delivery_reconciliation_required', 'count': pending})
        finally:
            connection.close()
    try:
        validate_transport_source()
    except (OSError, ValueError) as error:
        blockers.append({'code': 'transport_source_unavailable', 'detail': str(error)})
    blockers.append({'code': 'platform_run_correlation_unverified',
                     'detail': 'Real Muse run correlation and Linux/channel acceptance have not been verified; local unit tests do not authorize production cutover'})
    return report
