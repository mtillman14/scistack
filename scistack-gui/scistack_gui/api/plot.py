"""
Plot Studio — the handler table for both transports.

Each :class:`~scistack_gui.api.handlers.Handler` below is one Plot Studio
operation, declared ONCE: ``server.py`` takes its JSON-RPC method from
:func:`rpc_methods` and its lock policy from :func:`self_managed`;
:func:`install_routes` builds the HTTP route (mounted under ``/api``). The
frontend's route map (``frontend/src/api.ts``) names the same paths, and
``tests/test_api_handlers.py`` checks that it does.

    POST /api/plot/describe                 plot_describe
    POST /api/plot/capabilities             plot_capabilities
    POST /api/plot/variant-graph            plot_variant_graph
    POST /api/plot/grouping-graph           plot_grouping_graph
    POST /api/plot/grouping-columns         plot_grouping_columns
    POST /api/plot/grouping-default-variant plot_grouping_default_variant
    POST /api/plot/locations                plot_location_tree
    POST /api/plot/resolve                  plot_resolve
    POST /api/plot/export                   plot_export
    POST /api/plot/add-to-pipeline          plot_add_to_pipeline
    POST /api/plot/variant-sets             plot_variant_sets_save
    POST /api/plot/save                     plot_save_start
    POST /api/plot/invalidate               plot_invalidate
    POST /api/client-error                  report_client_error

Lock policy (``holds_db_lock``): a plot resolve spends nearly all of its
time in pandas and matplotlib, with the database touched only while the
variable frames load. Holding the file lock for the rest of it blocked
MATLAB for the full duration — the 2026-09-11 log shows one 31-second hold
for work that needed the database for well under a second of it. So every
handler whose service takes the connection itself declares
``holds_db_lock=False``; a handler declared so that then forgets to wrap
its own access fails with a closed connection rather than silently working,
because the JSON-RPC server closes the connection whenever the refcount
hits zero. The two that keep the blanket hold: ``plot_add_to_pipeline``
writes source files and reloads the registry through services that reach
the database by their own routes; ``plot_invalidate`` only drops a dict.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import plot_service
from scistack_gui.services.client_errors import report_client_error

logger = logging.getLogger(__name__)

router = APIRouter()

_BAD_REQUEST = {ValueError: 400, KeyError: 400}
#: The plotting packages are optional; a missing one is "not implemented".
_NOT_INSTALLED = {RuntimeError: 501}


class DescribeRequest(BaseModel):
    variable: str | None = None
    refresh: bool | None = False
    # Set to plot a CSV instead of the project database (the standalone path).
    csv_path: str | None = None


class SpecRequest(BaseModel):
    spec: dict
    max_points: int | None = None
    #: Which figure of an ITERATE fan-out to render; None renders all of them.
    figure_index: int | None = None
    csv_path: str | None = None
    #: The preview's size for label/legend decisions: {"mode": "export"} or
    #: {"mode": "pane", "width_px", "height_px"} (plot_service._render_preview).
    preview: dict | None = None
    #: Re-render the last resolve of this exact request (a pane resize).
    reuse_resolved: bool = False


class ExportRequest(BaseModel):
    spec: dict
    csv_path: str | None = None
    function_name: str | None = None
    output_variable: str | None = None
    path_template: str | None = None
    finalized: bool | None = True


class VariantGraphRequest(BaseModel):
    variable: str
    # Every function node on the canvas, so nodes outside this variable's chain
    # can still list the versions they have run.
    functions: list[str] | None = None
    csv_path: str | None = None


class GroupingGraphRequest(BaseModel):
    variable: str
    csv_path: str | None = None


class GroupingColumnsRequest(BaseModel):
    #: The figure's measure — what is being grouped.
    variable: str
    #: The variable whose columns are being listed — what it is grouped BY.
    group_variable: str
    csv_path: str | None = None


class GroupingDefaultVariantRequest(BaseModel):
    group_variable: str
    csv_path: str | None = None


class LocationTreeRequest(BaseModel):
    variable: str
    # The plotting layer's column-keyed variant selection ({"Code:bandpass":
    # "v1"}). Omitted on the canvas path, where no spec is open: the service
    # falls back to the same default a panel opens on.
    selection: dict | None = None
    problems_only: bool | None = False
    csv_path: str | None = None


class SaveRequest(BaseModel):
    spec: dict
    path: str
    dpi: int | None = 200
    #: Which figure of an ITERATE fan-out to save; None saves all of them.
    #: This also decides what `path` means — one file, or the folder N files
    #: go into. See `plot_service.save_figure`.
    figure_index: int | None = None
    #: png / svg / pdf / eps — validated against what matplotlib can write.
    image_format: str | None = None
    csv_path: str | None = None
    #: Client-chosen job id, so the panel can adopt it before the request
    #: leaves; None lets the server mint one.
    job_id: str | None = None
    #: "image" (the figure) or "data" (the plot's long table as CSV).
    what: str | None = "image"
    #: For "data": a key from the capability report's `data_export.depths`;
    #: None is the plotted sample.
    depth: str | None = None
    #: For "data": one column per struct field (default) or a ColName column.
    fields_as_columns: bool | None = True


class VariantSetsRequest(BaseModel):
    variable: str
    variant_sets: list[dict] | None = None


class ClientErrorRequest(BaseModel):
    where: str | None = "webview"
    message: str | None = ""
    stack: str | None = None
    component_stack: str | None = None


# --- the calls ---------------------------------------------------------------


def _describe(db, req: DescribeRequest) -> dict:
    return plot_service.describe(
        db, req.variable, refresh=bool(req.refresh), csv_path=req.csv_path
    )


def _capabilities(db, req: SpecRequest) -> dict:
    return plot_service.capabilities_for(db, req.spec, csv_path=req.csv_path)


def _variant_graph(db, req: VariantGraphRequest) -> dict:
    return plot_service.variant_graph(
        db, req.variable, functions=req.functions or [], csv_path=req.csv_path
    )


def _grouping_graph(db, req: GroupingGraphRequest) -> dict:
    return plot_service.grouping_graph(db, req.variable, csv_path=req.csv_path)


def _grouping_columns(db, req: GroupingColumnsRequest) -> dict:
    return plot_service.grouping_columns(
        db, req.variable, req.group_variable, csv_path=req.csv_path
    )


def _grouping_default_variant(db, req: GroupingDefaultVariantRequest) -> dict:
    return plot_service.grouping_default_variant(
        db, req.group_variable, csv_path=req.csv_path
    )


def _location_tree(db, req: LocationTreeRequest) -> dict:
    return plot_service.location_tree(
        db,
        req.variable,
        selection=req.selection,
        problems_only=bool(req.problems_only),
        csv_path=req.csv_path,
    )


def _resolve(db, req: SpecRequest) -> dict:
    return plot_service.resolve_figures(
        db,
        req.spec,
        max_points=req.max_points,
        figure_index=req.figure_index,
        csv_path=req.csv_path,
        preview=req.preview,
        reuse_resolved=req.reuse_resolved,
    )


def _export(db, req: ExportRequest) -> dict:
    return plot_service.export_code(
        db,
        req.spec,
        function_name=req.function_name,
        output_variable=req.output_variable,
        path_template=req.path_template,
        finalized=req.finalized is not False,
        csv_path=req.csv_path,
    )


def _add_to_pipeline(db, req: ExportRequest) -> dict:
    return plot_service.add_to_pipeline(
        db,
        req.spec,
        function_name=req.function_name,
        output_variable=req.output_variable,
        path_template=req.path_template,
        finalized=req.finalized is not False,
    )


def _variant_sets_save(db, req: VariantSetsRequest) -> dict:
    """Persist a plot's named variant pins as statements about the plotted
    variable — the `variant_selection` aspect of the intent store."""
    return plot_service.save_variant_sets(db, req.variable, list(req.variant_sets or []))


def _save_start(db, req: SaveRequest) -> dict:
    """Start a save job. Returns a job id immediately; progress arrives as
    ``plot_save_progress`` / ``plot_save_complete`` / ``plot_save_failed``
    notifications.

    One method for one figure and for all of them — ``figure_index`` is the
    only difference, and neither fits a request/response budget: a single
    full-resolution figure is ~12 minutes of work (scidb.log 2026-09-11). The
    separate ``save-all`` is gone with the synchronous save it complemented.
    See ``plot_service.start_save_job``.
    """
    return plot_service.start_save_job(
        db,
        req.spec,
        req.path,
        dpi=req.dpi if req.dpi is not None else 200,
        figure_index=req.figure_index,
        image_format=req.image_format,
        csv_path=req.csv_path,
        # The client may name the job so it can adopt the id before the request
        # leaves — a fast save can finish before the response arrives, and a
        # panel that learns the id from the response drops those messages.
        job_id=req.job_id,
        # "image" (default) or "data" — the plot's long table as CSV, at
        # `depth` (a key from the capability report's data_export.depths).
        what=req.what or "image",
        depth=req.depth,
        # "One column per field" for a struct variable; the panel's checkbox
        # defaults to on.
        fields_as_columns=req.fields_as_columns is not False,
    )


def _invalidate(db) -> dict:
    return plot_service.invalidate(db)


def _client_error(req: ClientErrorRequest) -> dict:
    """The webview caught a render error; write it into the shared log."""
    return report_client_error(req.model_dump())


# ``db_optional=True`` on every row whose request model carries a
# ``csv_path``: with one set, the service builds a ``CsvSource`` and the
# database is never opened (each entry point is already written as
# ``db_connection(..., needed=not csv_path)``). That declaration is what lets
# a **plot-only** server — started with no ``--db`` for Explorer ▸ Plot CSV —
# serve these methods at all: the dispatch would otherwise call ``get_db()``
# and raise before the handler ever saw the request saying it needed none.
#
# ``plot_add_to_pipeline``, ``plot_variant_sets_save`` and ``plot_invalidate``
# are deliberately NOT marked: writing an endpoint into the project, saving
# variant sets and dropping the cache are all things a CSV has nowhere to put.
PLOT_HANDLERS: tuple[Handler, ...] = (
    Handler(
        "plot_describe", "/plot/describe", DescribeRequest, _describe,
        holds_db_lock=False, http_errors=_NOT_INSTALLED, db_optional=True,
    ),
    Handler(
        "plot_capabilities", "/plot/capabilities", SpecRequest, _capabilities,
        holds_db_lock=False, http_errors=_BAD_REQUEST, db_optional=True,
    ),
    Handler(
        "plot_variant_graph", "/plot/variant-graph", VariantGraphRequest,
        _variant_graph, holds_db_lock=False, http_errors=_BAD_REQUEST,
        db_optional=True,
    ),
    # Read-only picker calls that take the connection inside the service for
    # exactly as long as the query needs it. `plot_grouping_columns` is the
    # one that can cost real time (one DISTINCT per column of a wide sheet).
    Handler(
        "plot_grouping_graph", "/plot/grouping-graph", GroupingGraphRequest,
        _grouping_graph, holds_db_lock=False, http_errors=_BAD_REQUEST,
        db_optional=True,
    ),
    Handler(
        "plot_grouping_columns", "/plot/grouping-columns",
        GroupingColumnsRequest, _grouping_columns,
        holds_db_lock=False, http_errors=_BAD_REQUEST, db_optional=True,
    ),
    Handler(
        "plot_grouping_default_variant", "/plot/grouping-default-variant",
        GroupingDefaultVariantRequest, _grouping_default_variant,
        holds_db_lock=False, http_errors=_BAD_REQUEST, db_optional=True,
    ),
    Handler(
        "plot_location_tree", "/plot/locations", LocationTreeRequest,
        _location_tree, holds_db_lock=False,
        http_errors={**_BAD_REQUEST, **_NOT_INSTALLED}, db_optional=True,
    ),
    Handler(
        "plot_resolve", "/plot/resolve", SpecRequest, _resolve,
        holds_db_lock=False, http_errors=_BAD_REQUEST, db_optional=True,
    ),
    Handler(
        "plot_export", "/plot/export", ExportRequest, _export,
        holds_db_lock=False, http_errors=_BAD_REQUEST, db_optional=True,
    ),
    Handler(
        "plot_add_to_pipeline", "/plot/add-to-pipeline", ExportRequest,
        _add_to_pipeline, http_errors=_BAD_REQUEST,
    ),
    Handler(
        "plot_variant_sets_save", "/plot/variant-sets", VariantSetsRequest,
        _variant_sets_save,
    ),
    # Spawns a thread and returns; the HANDLER touches nothing. Its worker
    # takes the connection through `save_figure` -> `_load` on its own
    # schedule, which is the point — a save must not hold the DuckDB file
    # for the minutes it spends in pandas and matplotlib.
    Handler(
        "plot_save_start", "/plot/save", SaveRequest, _save_start,
        holds_db_lock=False, http_errors={**_BAD_REQUEST, OSError: 400},
        db_optional=True,
    ),
    Handler("plot_invalidate", "/plot/invalidate", None, _invalidate),
    # Touches no database at all — it only writes a log line, and must still
    # work while MATLAB holds the file (that is exactly when a webview crash
    # is worth hearing about).
    Handler(
        "report_client_error", "/client-error", ClientErrorRequest,
        _client_error, holds_db_lock=False, needs_db=False,
    ),
)

install_routes(router, PLOT_HANDLERS)
