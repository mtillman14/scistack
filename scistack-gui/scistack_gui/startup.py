"""
Project-open startup diagnostics (Phase 8).

When the GUI opens a project, a few things need to happen before the user
starts interacting with it. Today the only startup task is a stale-lockfile
check: if ``pyproject.toml`` is newer than ``uv.lock``, we run ``uv sync``
silently. On success, the user never knows it happened. On failure, we
record a structured error that the frontend surfaces as a blocking dialog
— nothing the user does should silently run against a broken venv.

This module deliberately doesn't know about JSON-RPC or HTTP. Both server
modes call :func:`check_lockfile_staleness` during their startup sequence;
both then report ``get_startup_errors()`` through their own transport
(``get_info`` for FastAPI, the same ``get_info`` handler for JSON-RPC).
Keeping the state here means the two transports share one source of truth
and the frontend's rendering logic doesn't need to know which mode it's in.

Usage (from a server entry point)::

    from scistack_gui import startup
    startup.check_lockfile_staleness(project_root)
    # ... later, when the frontend asks for /api/info ...
    errors = startup.get_startup_errors()
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
    ("lockfile_sync_failed", "uv_not_installed", ...). ``message`` is the
    short headline. ``details`` is the optional long-form output (uv stderr,
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
# Populated by check_lockfile_staleness (and any future startup checks).
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
# Stale lockfile handling
# ---------------------------------------------------------------------------
def check_lockfile_staleness(project_root: Path) -> StartupError | None:
    """Inspect ``project_root`` for a stale ``uv.lock`` and sync if needed.

    Returns the :class:`StartupError` that was recorded, or ``None`` if
    everything is fine (or the check was skipped because the directory is
    not a project).

    The behaviour, spelled out:

    * No ``pyproject.toml`` in ``project_root`` → silent no-op, returns
      ``None``. This covers ad-hoc ``.duckdb`` files that live outside a
      scaffolded project.
    * ``scistack`` package not importable → silent no-op with a debug log.
      The GUI has to work even when the scaffolding layer is absent.
    * Lockfile is fresh → silent no-op, returns ``None``.
    * Lockfile is stale → run ``uv sync``. On success, silent. On failure,
      record a ``StartupError`` and return it so the caller can decide
      whether to log / propagate further.
    * ``uv`` not installed → record a ``StartupError(kind="uv_not_installed")``.

    Any unexpected exception from the scistack layer is caught and logged
    (but not recorded as a startup error) — we'd rather open the project
    than block on a best-effort check.
    """
    logger.info("[startup] checking lockfile staleness for project: %s", project_root)
    project_root = Path(project_root)
    pyproject = project_root / "pyproject.toml"
    if not pyproject.exists():
        logger.debug(
            "[startup] no pyproject.toml at %s — skipping lockfile staleness check",
            project_root,
        )
        return None
    logger.info("[startup] found pyproject.toml, proceeding with staleness check")

    logger.info("[startup] attempting to import scistack.uv_wrapper")
    try:
        from scistack.uv_wrapper import (
            UvNotFoundError,
            is_lockfile_stale,
            sync,
        )

        logger.info("[startup] successfully imported scistack.uv_wrapper")
    except ImportError:
        logger.debug(
            "[startup] scistack package not importable — skipping lockfile staleness check"
        )
        return None

    logger.info("[startup] checking if lockfile is stale")
    try:
        stale = is_lockfile_stale(project_root)
        logger.info(
            "[startup] lockfile staleness check result: %s",
            "STALE" if stale else "FRESH",
        )
    except Exception:  # pragma: no cover — defensive
        logger.debug("[startup] is_lockfile_stale raised unexpectedly", exc_info=True)
        return None

    if not stale:
        logger.debug(
            "[startup] uv.lock at %s is up to date, no sync needed", project_root
        )
        return None

    logger.info("[startup] uv.lock is stale in %s — running uv sync", project_root)
    try:
        logger.info("[startup] executing uv sync with 300s timeout")
        result = sync(project_root, timeout=300.0)
        logger.info(
            "[startup] uv sync completed with exit code %d",
            result.returncode if hasattr(result, "returncode") else 0,
        )
    except UvNotFoundError as e:
        logger.warning("[startup] uv is not installed: %s", e)
        err = StartupError(
            kind="uv_not_installed",
            message=(
                "uv is not installed. Install it from "
                "https://github.com/astral-sh/uv to enable automatic "
                "lockfile synchronisation."
            ),
            details=str(e),
        )
        _record(err)
        return err
    except Exception as e:
        logger.warning("uv sync raised unexpectedly: %s", e, exc_info=True)
        err = StartupError(
            kind="lockfile_sync_failed",
            message="Failed to run 'uv sync' on project open.",
            details=repr(e),
        )
        _record(err)
        return err

    if result.ok:
        logger.info("[startup] uv sync succeeded on project open")
        return None

    logger.warning(
        "[startup] uv sync failed on project open (exit %d):\n%s",
        result.returncode,
        result.combined_output,
    )
    err = StartupError(
        kind="lockfile_sync_failed",
        message=(
            f"'uv sync' failed with exit code {result.returncode}. "
            "The project's virtual environment may be out of sync with "
            "pyproject.toml. Fix the errors below and restart the project."
        ),
        details=result.combined_output,
    )
    _record(err)
    return err


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
