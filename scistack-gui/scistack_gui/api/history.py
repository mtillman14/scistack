"""Undo / redo — the handler table for both transports (``api/handlers.py``).

    GET  /api/history/status   history_status — undoable methods + counts
    POST /api/history/undo     history_undo   — restore a change's before side
    POST /api/history/redo     history_redo   — restore a change's after side

The records themselves are ``scistack_gui.history``; the order in which a
user undoes them is the frontend's (one stack per surface). See
``docs/claude/undo-redo.md``.

After a successful undo/redo this module does what the original edit's
caller would have seen happen: registries reloaded from disk when a source
or config file was restored, then ``dag_updated`` broadcast so every view
refetches, plus ``history_applied`` for views that track something other
than the canvas.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui import history
from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


class ChangeRef(BaseModel):
    change_id: str


def _status() -> dict:
    """Which methods record undo history (``{name: label}``), plus counts.

    The Handler table is the one owner of "is this undoable"; the frontend
    reads it from here rather than keeping a copy.
    """
    from scistack_gui.api.tables import ALL_HANDLERS

    return {
        "methods": {h.name: h.label for h in ALL_HANDLERS if h.undoable},
        **history.status(),
    }


def _reload_after_restore(files: list[str]) -> None:
    """Make discovery see the restored files, exactly as after a GUI write:
    drop stale bytecode and the mtime-keyed config caches (two writes in one
    filesystem tick read as one version), re-read the config, reload both
    registries."""
    from scidb import aliases, colors

    from scistack_gui.api.project import _reload_config_and_rescan
    from scistack_gui.services import plot_service
    from scistack_gui.services.target_file_service import _invalidate_bytecode

    for f in files:
        if f.endswith(".py"):
            _invalidate_bytecode(Path(f))
    aliases.clear_cache()
    colors.clear_cache()
    plot_service.forget_resolved()
    try:
        _reload_config_and_rescan()
    except Exception:
        logger.exception("[history] registry reload after restore failed")


def _finish(direction: str, result: dict) -> dict:
    if result.get("status") != "ok":
        return result
    if result.get("reload"):
        _reload_after_restore(result.get("files") or [])
    from scistack_gui.api.ws import push_message

    push_message({"type": "dag_updated"})
    push_message(
        {
            "type": "history_applied",
            "direction": direction,
            "change_id": result["change_id"],
            "method": result.get("method"),
        }
    )
    return result


def _undo(req: ChangeRef) -> dict:
    return _finish("undo", history.undo(req.change_id))


def _redo(req: ChangeRef) -> dict:
    return _finish("redo", history.redo(req.change_id))


# needs_db=False and holds_db_lock=False: `scistack_gui.history` takes the
# connection itself, only when a record has rows, so an undo of a file-only
# change (a project alias) works with no database and never waits on MATLAB.
_SELF = {"needs_db": False, "holds_db_lock": False}

HISTORY_HANDLERS: tuple[Handler, ...] = (
    Handler("history_status", "/history/status", None, _status, http_method="GET", **_SELF),
    Handler("history_undo", "/history/undo", ChangeRef, _undo, undoable=False, **_SELF),
    Handler("history_redo", "/history/redo", ChangeRef, _redo, undoable=False, **_SELF),
)

install_routes(router, HISTORY_HANDLERS)
