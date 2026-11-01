"""Explicit local configuration for the new core; importing has no side effects."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class RuntimeConfig:
    """Paths supplied at startup, independent of the machine's deployment layout."""

    workspace: Path
    database: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RuntimeConfig:
        """Resolve paths without creating directories or opening production files."""
        values = os.environ if env is None else env
        workspace = Path(values.get('MUSE_WORKSPACE', str(Path.home() / 'workspace'))).expanduser().resolve()
        database = Path(values.get('MUSE_CORE_DB', str(workspace / 'state' / 'core-shadow.sqlite3'))).expanduser().resolve()
        return cls(workspace, database)
