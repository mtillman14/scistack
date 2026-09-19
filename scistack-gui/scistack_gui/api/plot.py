"""
Plot Studio API (HTTP transport).

Thin wrappers over ``services.plot_service`` — the JSON-RPC handlers in
``server.py`` call the same functions, so the browser GUI and the VS Code
extension cannot diverge.

    POST /api/plot/describe        — catalog + default spec for a variable
    POST /api/plot/capabilities    — available plot kinds for a role assignment
    POST /api/plot/locations       — per-location status tree for one variable
    POST /api/plot/resolve         — plotly figure dicts for the panel
    POST /api/plot/export          — generated plot_ function + for_each call
    POST /api/plot/add-to-pipeline — write the endpoint into the project
    POST /api/plot/invalidate      — drop cached frames after a run
    POST /api/client-error         — a webview error boundary's report
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui.db import get_db
from scistack_gui.services import plot_service
from scistack_gui.services.client_errors import report_client_error

logger = logging.getLogger(__name__)

router = APIRouter()


class DescribeRequest(BaseModel):
    variable: str | None = None
    refresh: bool = False
    # Set to plot a CSV instead of the project database (the standalone path).
    csv_path: str | None = None


class SpecRequest(BaseModel):
    spec: dict
    max_points: int | None = None
    #: Which figure of an ITERATE fan-out to render; None renders all of them.
    figure_index: int | None = None
    csv_path: str | None = None


class ExportRequest(BaseModel):
    spec: dict
    csv_path: str | None = None
    function_name: str | None = None
    output_variable: str | None = None
    path_template: str | None = None
    finalized: bool = True


@router.post("/plot/describe")
def plot_describe(req: DescribeRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    try:
        return plot_service.describe(
            db, req.variable, refresh=req.refresh, csv_path=req.csv_path
        )
    except RuntimeError as exc:  # plotting packages not installed
        raise HTTPException(status_code=501, detail=str(exc))


@router.post("/plot/capabilities")
def plot_capabilities(req: SpecRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    try:
        return plot_service.capabilities_for(db, req.spec, csv_path=req.csv_path)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class VariantGraphRequest(BaseModel):
    variable: str
    # Every function node on the canvas, so nodes outside this variable's chain
    # can still list the versions they have run.
    functions: list[str] = []
    csv_path: str | None = None


@router.post("/plot/variant-graph")
def plot_variant_graph(
    req: VariantGraphRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.variant_graph(
            db, req.variable, functions=req.functions, csv_path=req.csv_path
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class GroupingGraphRequest(BaseModel):
    variable: str
    csv_path: str | None = None


@router.post("/plot/grouping-graph")
def plot_grouping_graph(
    req: GroupingGraphRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.grouping_graph(db, req.variable, csv_path=req.csv_path)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class GroupingColumnsRequest(BaseModel):
    #: The figure's measure — what is being grouped.
    variable: str
    #: The variable whose columns are being listed — what it is grouped BY.
    group_variable: str
    csv_path: str | None = None


@router.post("/plot/grouping-columns")
def plot_grouping_columns(
    req: GroupingColumnsRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.grouping_columns(
            db, req.variable, req.group_variable, csv_path=req.csv_path
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class GroupingDefaultVariantRequest(BaseModel):
    group_variable: str
    csv_path: str | None = None


@router.post("/plot/grouping-default-variant")
def plot_grouping_default_variant(
    req: GroupingDefaultVariantRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.grouping_default_variant(
            db, req.group_variable, csv_path=req.csv_path
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class LocationTreeRequest(BaseModel):
    variable: str
    # The plotting layer's column-keyed variant selection ({"Code:bandpass":
    # "v1"}). Omitted on the canvas path, where no spec is open: the service
    # falls back to the same default a panel opens on.
    selection: dict | None = None
    problems_only: bool = False
    csv_path: str | None = None


@router.post("/plot/locations")
def plot_locations(
    req: LocationTreeRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.location_tree(
            db,
            req.variable,
            selection=req.selection,
            problems_only=req.problems_only,
            csv_path=req.csv_path,
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:  # plotting packages not installed
        raise HTTPException(status_code=501, detail=str(exc))


@router.post("/plot/resolve")
def plot_resolve(req: SpecRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    try:
        return plot_service.resolve_figures(
            db,
            req.spec,
            max_points=req.max_points,
            figure_index=req.figure_index,
            csv_path=req.csv_path,
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/plot/export")
def plot_export(req: ExportRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    try:
        return plot_service.export_code(
            db,
            req.spec,
            function_name=req.function_name,
            output_variable=req.output_variable,
            path_template=req.path_template,
            finalized=req.finalized,
            csv_path=req.csv_path,
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/plot/add-to-pipeline")
def plot_add_to_pipeline(
    req: ExportRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return plot_service.add_to_pipeline(
            db,
            req.spec,
            function_name=req.function_name,
            output_variable=req.output_variable,
            path_template=req.path_template,
            finalized=req.finalized,
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


class SaveRequest(BaseModel):
    spec: dict
    path: str
    dpi: int = 200
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
    what: str = "image"
    #: For "data": a key from the capability report's `data_export.depths`;
    #: None is the plotted sample.
    depth: str | None = None
    #: For "data": one column per struct field (default) or a ColName column.
    fields_as_columns: bool = True


@router.post("/plot/save")
def plot_save(req: SaveRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    """Start a save job. Returns a job id immediately; progress arrives as
    ``plot_save_progress`` / ``plot_save_complete`` / ``plot_save_failed``
    notifications.

    One route for one figure and for all of them — ``figure_index`` is the only
    difference, and neither fits a request/response budget. The separate
    ``/plot/save-all`` is gone with the synchronous save it complemented; a save
    at full resolution is minutes of work (see ``plot_service.start_save_job``).
    """
    try:
        return plot_service.start_save_job(
            db,
            req.spec,
            req.path,
            dpi=req.dpi,
            figure_index=req.figure_index,
            image_format=req.image_format,
            csv_path=req.csv_path,
            job_id=req.job_id,
            what=req.what,
            depth=req.depth,
            fields_as_columns=req.fields_as_columns,
        )
    except (ValueError, KeyError, OSError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/plot/invalidate")
def plot_invalidate(db: DatabaseManager = Depends(get_db)) -> dict:
    return plot_service.invalidate(db)


class ClientErrorRequest(BaseModel):
    where: str = "webview"
    message: str = ""
    stack: str | None = None
    component_stack: str | None = None


@router.post("/client-error")
def client_error(req: ClientErrorRequest) -> dict:
    """The webview caught a render error; write it into the shared log."""
    return report_client_error(req.model_dump())
