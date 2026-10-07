"""
scistack — project & environment tooling for SciStack scientific pipelines.

Top-level modules:

- :mod:`scistack.uv_wrapper` — thin wrapper around the ``uv`` CLI
- :mod:`scistack.project` — project scaffolder
- :mod:`scistack.user_config` — user-global configuration (coming in Phase 5)
"""

from scistack.project import scaffold_project, validate_project_name
from scistack.user_config import (
    Tap,
    UserConfig,
    add_tap,
    list_taps,
    load_config,
    refresh_tap,
    remove_tap,
)
from scistack.uv_wrapper import (
    AddResult,
    LockedPackage,
    RemoveResult,
    SyncResult,
    UvNotFoundError,
    add,
    is_lockfile_stale,
    read_lockfile,
    remove,
    sync,
)

# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scistack")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    # uv wrapper
    "sync",
    "add",
    "remove",
    "read_lockfile",
    "is_lockfile_stale",
    "SyncResult",
    "AddResult",
    "RemoveResult",
    "LockedPackage",
    "UvNotFoundError",
    # project scaffolder
    "scaffold_project",
    "validate_project_name",
    # user config
    "load_config",
    "add_tap",
    "remove_tap",
    "list_taps",
    "refresh_tap",
    "Tap",
    "UserConfig",
]
