"""
scistack — project tooling for SciStack scientific pipelines.

- ``scistack init`` (:mod:`scistack.__main__`): a thin front end over
  ``scidb.project.init_project``, the one owner of creating a project.
- :mod:`scistack.user_config` — user-global configuration (library taps).

SciStack does not manage Python environments (no uv, 2026-10-08): a project
is an ordinary package, installed with whatever tool the user prefers.
"""

from scistack.user_config import (
    Tap,
    UserConfig,
    add_tap,
    list_taps,
    load_config,
    refresh_tap,
    remove_tap,
)

# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scistack")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    # user config
    "load_config",
    "add_tap",
    "remove_tap",
    "list_taps",
    "refresh_tap",
    "Tap",
    "UserConfig",
]
