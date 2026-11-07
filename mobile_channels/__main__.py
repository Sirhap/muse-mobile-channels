"""Local runtime, evidence-based recovery and no-overwrite migration operations."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .config import RuntimeConfig
from .storage import Store


def main(argv: list[str] | None = None) -> int:
    """Run only explicitly selected local operations; mutations require exact scope."""
    config = RuntimeConfig.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    shadow = commands.add_parser('shadow-import')
    shadow.add_argument('--workspace', type=Path, default=config.workspace)
    shadow.add_argument('--database', type=Path, default=config.database)
    initialize = commands.add_parser('core-init')
    initialize.add_argument('--database', type=Path, default=config.database)
    run = commands.add_parser('core-run')
    run.add_argument('--database', type=Path, required=True)
    run.add_argument('--modes', type=Path, required=True)
    run.add_argument('--once', action='store_true')
    readiness = commands.add_parser('core-readiness')
    readiness.add_argument('--database', type=Path, required=True)
    readiness.add_argument('--modes', type=Path, required=True)
    doctor = commands.add_parser('doctor')
    doctor.add_argument('--workspace', type=Path, default=config.workspace)
    doctor.add_argument('--home', type=Path, default=Path.home())
    doctor.add_argument('--modes', type=Path)
    plan = commands.add_parser('service-plan')
    plan.add_argument('--workspace', type=Path, default=config.workspace)
    plan.add_argument('--home', type=Path, default=Path.home())
    plan.add_argument('--output', type=Path, required=True)
    plan.add_argument('--channels', nargs='+', choices=('weixin', 'wecom'), required=True)
    for name in ('database-check', 'database-backup', 'database-restore'):
        operation = commands.add_parser(name)
        operation.add_argument('--source', type=Path, required=True)
        if name != 'database-check':
            operation.add_argument('--destination', type=Path, required=True)
    migration = commands.add_parser('migration-preview')
    migration.add_argument('--workspace', type=Path, required=True)
    migration.add_argument('--database', type=Path, required=True)
    migration.add_argument('--accounts', type=Path, required=True, help='JSON channel to account mapping')
    migration.add_argument('--stopped-snapshot', action='store_true')
    for name in ('bridge-inspect', 'bridge-resolve'):
        operation = commands.add_parser(name)
        operation.add_argument('--base', type=Path, required=True)
        operation.add_argument('--channel', choices=('weixin', 'wecom'), required=True)
        if name == 'bridge-resolve':
            for key in ('msgid', 'session-id', 'digest', 'evidence-file'):
                operation.add_argument('--' + key, required=True)
            operation.add_argument('--outcome', choices=('accepted', 'not_submitted'), required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'shadow-import':
            from .shadow import import_shadow
            report = import_shadow(args.workspace, args.database)
        elif args.command == 'core-init':
            args.database.parent.mkdir(parents=True, exist_ok=True)
            with Store(args.database) as store:
                report = store.snapshot()
        elif args.command == 'core-readiness':
            from .readiness import inspect_readiness
            report = inspect_readiness(args.database, args.modes)
        elif args.command == 'doctor':
            from .doctor import inspect_installation
            report = inspect_installation(args.workspace, args.home, args.modes)
        elif args.command == 'service-plan':
            from .install import write_service_plan
            report = write_service_plan(args.workspace, args.home, args.output, channels=args.channels)
        elif args.command.startswith('database-'):
            from .maintenance import check_database, backup_database, restore_database
            functions = {'database-check': check_database, 'database-backup': backup_database,
                         'database-restore': restore_database}
            report = functions[args.command](args.source, *([] if args.command == 'database-check' else [args.destination]))
        elif args.command == 'migration-preview':
            from .migration import migrate_snapshot
            accounts = json.loads(args.accounts.read_text())
            report = migrate_snapshot(args.workspace, args.database, accounts=accounts,
                                      snapshot_confirmed=args.stopped_snapshot)
        elif args.command.startswith('bridge-'):
            from .recovery import inspect_pending, resolve_pending
            if args.command == 'bridge-inspect':
                report = inspect_pending(args.base, args.channel)
            else:
                report = resolve_pending(args.base, args.channel, msgid=args.msgid,
                    session_id=args.session_id, digest=args.digest, outcome=args.outcome,
                    evidence=Path(args.evidence_file).read_text())
        else:
            report = run_core(args.database, args.modes, args.once)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, sqlite3.Error) as error:
        print(f'{args.command} failed: {error}', file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


def poll_reply_updates(database: Path) -> str:
    """Keep slow history reads on a separate, short-lived SQLite connection."""
    from .executors import DualTrackExecutor
    with Store(database) as store:
        return DualTrackExecutor(store).poll_replies()


def run_core(database: Path, modes: Path, once: bool = False) -> dict:
    """Hold all account mode leases while advancing the opt-in execution core."""
    from .executors import DualTrackExecutor, validate_transport_source
    from .modes import ChannelLease, ModeError
    from .runtime import CoreRunner
    from .readiness import ensure_executable_database
    ensure_executable_database(database)
    validate_transport_source()
    document = json.loads(modes.read_text())
    with ExitStack() as stack:
        scopes = set()
        for channel, settings in document['channels'].items():
            if settings['mode'] == 'core':
                lease = stack.enter_context(ChannelLease(modes, channel, 'core'))
                if Path(lease.settings['database']).resolve() != database.resolve():
                    raise ModeError('all runner accounts must use the requested database')
                scopes.add((channel, lease.settings['account_id']))
        if not scopes:
            raise ModeError('no core channels configured')
        store = stack.enter_context(Store(database))
        routes = set(map(tuple, store.connection.execute('SELECT DISTINCT channel,account_id FROM conversations')))
        if not routes <= scopes:
            raise ModeError('database contains accounts not owned by this runner configuration')
        # Dual track: Gadget side chats first, native MuseExecutor as fallback.
        # Without this the core path would never exercise the Gadget track.
        runner = CoreRunner(store, DualTrackExecutor(store, modes=modes), f'core-{os.getpid()}')
        observer = stack.enter_context(ThreadPoolExecutor(max_workers=1, thread_name_prefix='muse-replies'))
        pending_update = None
        while True:
            state = runner.tick()
            if once:
                return dict(store.snapshot(), tick=state)
            if pending_update is not None and pending_update.done():
                try:
                    pending_update.result()
                except Exception:
                    logging.getLogger(__name__).warning('reply history observation failed; it will retry')
                pending_update = None
            if pending_update is None:
                pending_update = observer.submit(poll_reply_updates, database)
            time.sleep(1)


if __name__ == '__main__':
    raise SystemExit(main())
