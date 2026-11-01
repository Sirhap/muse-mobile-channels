"""SQLite backup, integrity validation and no-overwrite recovery operations."""
from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path


def _readonly(path: Path) -> sqlite3.Connection:
    """Open an existing source read-only, including committed WAL content."""
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)


def check_database(path: Path) -> dict:
    """Validate database pages and foreign keys without creating or modifying it."""
    connection = _readonly(Path(path))
    try:
        integrity = [row[0] for row in connection.execute('PRAGMA integrity_check')]
        foreign_keys = connection.execute('PRAGMA foreign_key_check').fetchall()
        if integrity != ['ok'] or foreign_keys:
            raise ValueError('Database integrity or foreign key validation failed')
        return {'integrity': 'ok', 'foreign_key_violations': 0,
                'schema_version': connection.execute('PRAGMA user_version').fetchone()[0]}
    finally:
        connection.close()


def backup_database(source: Path, destination: Path) -> dict:
    """Copy a consistent SQLite snapshot and publish it without overwriting files.

    The backup API captures WAL transactions. A same-filesystem hard link
    publishes the verified temporary file atomically only if the target is new.
    """
    source, destination = Path(source).resolve(), Path(destination).absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f'Destination already exists: {destination.name}')
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f'.sqlite-backup-{uuid.uuid4().hex}.tmp'
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        original = _readonly(source)
        try:
            copy = sqlite3.connect(temporary, timeout=5)
            try:
                original.backup(copy)
            finally:
                copy.close()
        finally:
            original.close()
        report = check_database(temporary)
        with temporary.open('rb') as data:
            os.fsync(data.fileno())
        os.link(temporary, destination)
        directory = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return dict(report, destination=str(destination))
    finally:
        temporary.unlink(missing_ok=True)


def restore_database(backup: Path, destination: Path) -> dict:
    """Restore into a new path; replacing an active database is never permitted."""
    check_database(backup)
    return backup_database(backup, destination)
