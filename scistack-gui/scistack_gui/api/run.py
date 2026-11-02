"""
POST /api/run, the cancel routes and the MATLAB run routes: the handler
table for both transports (``api/handlers.py``). The runs themselves are
``services/run_service.py``.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services.run_service import (
    RunRequest,
    cancel_run,
    force_cancel_run,
    get_matlab_engine_status,
    restart_matlab_engine,
    start_matlab_sidecar_run,
    start_run,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# The handler table — both transports (api/handlers.py)
# ---------------------------------------------------------------------------
#
#     POST /api/run                          start_run
#     POST /api/run/{run_id}/cancel          cancel_run
#     POST /api/run/{run_id}/force-cancel    force_cancel_run
#     GET  /api/matlab-engine                get_matlab_engine_status
#     POST /api/matlab-engine/restart        restart_matlab_engine
#     (RPC only)                             generate_matlab_command,
#                                            generate_matlab_pipeline_command,
#                                            start_matlab_sidecar_run
#
# The three RPC-only methods exist for the VS Code extension's dagPanel.ts,
# which owns the MathWorks-terminal dispatch and the sidecar fallback
# ladder; the browser has no host that could act on them.


class RunRef(BaseModel):
    run_id: str


class SchemaLevelRequest(BaseModel):
    """What the settings panel asks: the level a Run of this node would use,
    given its current (possibly unsaved) stated value."""

    function_name: str
    node_id: str | None = None
    stated: list[str] | None = None


class MatlabCommandRequest(BaseModel):
    """The command generators read the raw request dict (``params``) as the
    single-run/pipeline-run requests do — every field the picker sends is
    forwarded. Declared open so the model does not have to repeat them."""

    model_config = {"extra": "allow"}

    function_name: str | None = None
    pipeline_id: str | None = None


class SidecarRunRequest(BaseModel):
    command: str
    run_id: str
    warnings: list[str] | None = None

class MatlabTerminalRunRequest(BaseModel):
    """A run the host handed to the MathWorks terminal, for us to watch.

    The host cannot see the run end — that is the whole problem — so it
    tells us the run_id and we watch the markers and the database lock. See
    ``scistack_gui.matlab.run_watch``.
    """

    run_id: str
    #: What to call it in the log. Optional; the run_id is a usable fallback.
    label: str | None = None
    #: The node a single-node run was started from (failures land on it).
    node_id: str | None = None


def _watch_matlab_terminal_run(db, req: MatlabTerminalRunRequest) -> dict:
    """Start watching a terminal-dispatched MATLAB run.

    Needs the database only for its PATH — the watcher polls markers beside
    it and probes its file lock, both from its own thread, and must not hold
    the connection while it does (that would be holding it against MATLAB
    for the whole run, which is what ``holds_db_lock=False`` prevents).
    """
    from scistack_gui.runtime.db import get_db_path
    from scistack_gui.matlab.run_watch import track

    return track(req.run_id, str(get_db_path()), req.label or req.run_id, req.node_id)


def _stop_watching_matlab_run(req: RunRef) -> dict:
    """The user pressed "stop waiting": drop the watcher, emit no verdict."""
    from scistack_gui.matlab.run_watch import stop

    return {"ok": stop(req.run_id)}


def _matlab_run_state() -> dict:
    """Every run being watched, and what the watcher believes about it.

    The diagnostics surface for "why is this node still spinning?" — there
    is no other way to see it, since the run lives in another process behind
    a terminal nothing can query.
    """
    from scistack_gui.matlab.run_watch import tracked_runs

    return {"runs": tracked_runs()}


def _start_run(db, req: RunRequest, *, transport: str) -> dict:
    return start_run(db, req, transport=transport)


def _cancel_run(req: RunRef) -> dict:
    logger.info("[api/run] cancel_run for run_id=%s", req.run_id)
    result = cancel_run(req.run_id)
    logger.debug("[api/run] cancel_run result: %s (run_id=%s)", result, req.run_id)
    return result


def _force_cancel_run(req: RunRef) -> dict:
    logger.info("[api/run] force_cancel_run for run_id=%s", req.run_id)
    result = force_cancel_run(req.run_id)
    logger.debug("[api/run] force_cancel_run result: %s (run_id=%s)", result, req.run_id)
    return result


def _get_matlab_engine_status() -> dict:
    return get_matlab_engine_status()


def _restart_matlab_engine() -> dict:
    return restart_matlab_engine()


def _get_schema_level(db, req: SchemaLevelRequest) -> dict:
    from scistack_gui.services.execution_service import node_schema_level

    return node_schema_level(db, req.node_id, req.function_name, req.stated)


def _generate_matlab_command(db, req: MatlabCommandRequest) -> dict:
    from scistack_gui.services.matlab_command_service import generate_matlab_command

    if not req.function_name:
        raise ValueError("generate_matlab_command needs a function_name")
    return generate_matlab_command(req.function_name, db, req.model_dump())


def _generate_matlab_pipeline_command(db, req: MatlabCommandRequest) -> dict:
    from scistack_gui.services.matlab_command_service import (
        generate_matlab_pipeline_command,
    )

    if not req.pipeline_id:
        raise ValueError("generate_matlab_pipeline_command needs a pipeline_id")
    return generate_matlab_pipeline_command(req.pipeline_id, db, req.model_dump())


def _start_matlab_sidecar_run(req: SidecarRunRequest) -> dict:
    return start_matlab_sidecar_run(req.command, req.run_id, req.warnings)


_NO_DB = {"needs_db": False}

RUN_HANDLERS: tuple[Handler, ...] = (
    Handler("start_run", "/run", RunRequest, _start_run, http_errors={ValueError: 400}, wants_transport=True, undoable=False),
    Handler("cancel_run", "/run/{run_id}/cancel", RunRef, _cancel_run, body=False, **_NO_DB, undoable=False),
    Handler("force_cancel_run", "/run/{run_id}/force-cancel", RunRef, _force_cancel_run, body=False, **_NO_DB, undoable=False),
    Handler("get_matlab_engine_status", "/matlab-engine", None, _get_matlab_engine_status, http_method="GET", **_NO_DB),
    Handler("restart_matlab_engine", "/matlab-engine/restart", None, _restart_matlab_engine, **_NO_DB, undoable=False),
    Handler("get_schema_level", "/schema-level", SchemaLevelRequest, _get_schema_level, undoable=False),
    Handler("generate_matlab_command", None, MatlabCommandRequest, _generate_matlab_command, undoable=False),
    Handler("generate_matlab_pipeline_command", None, MatlabCommandRequest, _generate_matlab_pipeline_command, undoable=False),
    Handler("start_matlab_sidecar_run", None, SidecarRunRequest, _start_matlab_sidecar_run, **_NO_DB, undoable=False),
    # Terminal-run tracking. `watch` needs the db PATH only, and declares
    # holds_db_lock=False so the dispatch does not hold the connection
    # MATLAB is about to want; the other two never touch the database.
    Handler(
        "watch_matlab_terminal_run", None, MatlabTerminalRunRequest,
        _watch_matlab_terminal_run, holds_db_lock=False, undoable=False,
    ),
    Handler("stop_watching_matlab_run", None, RunRef, _stop_watching_matlab_run, **_NO_DB, undoable=False),
    # RPC-only: the terminal tier exists only under the VS Code host, so the
    # browser build has nothing to show here (its MATLAB runs go through the
    # sidecar, which Python drives and already reports on).
    Handler("get_matlab_run_state", None, None, _matlab_run_state, **_NO_DB, undoable=False),
)

install_routes(router, RUN_HANDLERS)
