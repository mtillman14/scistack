"""
Project-open startup diagnostics (Phase 8).

Checks that run when the GUI opens a project and record a structured
:class:`StartupError` the frontend surfaces, instead of failing silently.
Today: :func:`check_windows_config_paths`. (The uv lockfile check that used
to live here was removed with uv itself, 2026-10-08: SciStack does not
manage environments.)

This module deliberately doesn't know about JSON-RPC or HTTP. Both server
modes run the checks during their startup sequence; both then report
``get_startup_errors()`` through their own transport (``get_info`` for
FastAPI, the same ``get_info`` handler for JSON-RPC). Keeping the state here
means the two transports share one source of truth and the frontend's
rendering logic doesn't need to know which mode it's in.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Error records
# ---------------------------------------------------------------------------
@dataclass
class StartupError:
    """One problem detected during project open.

    ``kind`` is a stable string identifier the frontend can switch on
    ("windows_config_paths", ...). ``message`` is the
    short headline. ``details`` is the optional long-form output (paths,
    traceback, etc.) — shown inside an expandable section in the dialog.
    ``blocking`` hints to the frontend whether this should block all
    interaction (the default) or just show a dismissable toast.
    """

    kind: str
    message: str
    details: str = ""
    blocking: bool = True

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "message": self.message,
            "details": self.details,
            "blocking": self.blocking,
        }


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# Populated by the startup checks below.
# Read by the /api/info endpoint so the frontend can display errors.
_startup_errors: list[StartupError] = []


def get_startup_errors() -> list[StartupError]:
    """Return a *copy* of the startup errors accumulated so far."""
    return list(_startup_errors)


def clear_startup_errors() -> None:
    """Drop any recorded startup errors. Intended for tests and fresh restarts."""
    _startup_errors.clear()


def _record(err: StartupError) -> None:
    """Append an error to the module-level list, deduping by ``kind``."""
    # If the same kind is recorded twice, overwrite the earlier one so the
    # user sees the most recent message (e.g. after a retry).
    for i, existing in enumerate(_startup_errors):
        if existing.kind == err.kind:
            _startup_errors[i] = err
            return
    _startup_errors.append(err)


# ---------------------------------------------------------------------------
# Cross-platform config paths
# ---------------------------------------------------------------------------
def check_windows_config_paths(config) -> "StartupError | None":
    """Report scistack.toml paths that were written on Windows.

    A config is a shared, committed file: the same project opened on Windows
    and on macOS reads the same ``modules``/``entities_file`` values. Written
    with backslashes, those values do not error on POSIX -- ``\\`` is a legal
    filename character, so they quietly name the wrong thing.
    ``scifor.discovery.resolve_config_path`` now reads them correctly, but a
    *relative* one has usually already caused a file to be created at the
    literal path (a ``src\\scistack_entities.toml`` in the project root), and
    an *absolute* one (``Y:\\LabMembers\\...``) cannot be salvaged here at
    all -- there is no such drive on this machine.

    Non-blocking: everything still opens, and the affected paths are named
    rather than guessed at.
    """
    from scifor.discovery import is_windows_absolute, read_scistack_section

    from scistack_gui.config import locate_config_at

    project_root = getattr(config, "project_root", None)
    if project_root is None or os.sep == "\\":
        return None

    toml_path = locate_config_at(project_root)
    if toml_path is None:
        return None
    section = read_scistack_section(toml_path) or {}

    windows_values = sorted(_windows_path_values(section))
    if not windows_values:
        return None

    unresolvable = [v for v in windows_values if is_windows_absolute(v)]
    lines = [f"  {v}" for v in windows_values]
    if unresolvable:
        lines.append("")
        lines.append(
            "These name a Windows drive and cannot resolve on this machine; "
            "re-add them in 📁 Paths:"
        )
        lines.extend(f"  {v}" for v in unresolvable)
    err = StartupError(
        kind="windows_config_paths",
        message=(
            f"{toml_path} contains {len(windows_values)} path(s) written with "
            f"Windows separators. Relative ones are read correctly, but a file "
            f"may have been created at the literal path (check for a name "
            f"containing a backslash in {project_root}); absolute ones "
            f"(Y:\\...) cannot resolve here at all."
        ),
        details="\n".join(lines),
        blocking=False,
    )
    _record(err)
    logger.warning("[startup] %s\n%s", err.message, err.details)
    return err


def _windows_path_values(section: dict) -> set[str]:
    """Every string in *section* that looks like a Windows path."""
    found: set[str] = set()

    def visit(value) -> None:
        if isinstance(value, str):
            if "\\" in value:
                found.add(value)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for item in value.values():
                visit(item)

    visit(section)
    return found
