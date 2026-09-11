"""
Plot Studio API (HTTP transport).

Thin wrappers over ``services.plot_service`` — the JSON-RPC handlers in
``server.py`` call the same functions, so the browser GUI and the VS Code
extension cannot diverge.

    POST /api/plot/describe        — catalog + default spec for a variable
    POST /api/plot/capabilities    — available plot kinds for a role assignment
    POST /api/plot/resolve         — plotly figure dicts for the panel
    POST /api/plot/export          — generated plot_ function + for_each call
    POST /api/plot/add-to-pipeline — write the endpoint into the project
    POST /api/plot/invalidate      — drop cached frames after a run
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui.db import get_db
from scistack_gui.services import plot_service

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
    csv_path: str | None = None


@router.post("/plot/save")
def plot_save(req: SaveRequest, db: DatabaseManager = Depends(get_db)) -> dict:
    try:
        return plot_service.save_figure(
            db, req.spec, req.path, dpi=req.dpi, csv_path=req.csv_path
        )
    except (ValueError, KeyError, OSError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/plot/invalidate")
def plot_invalidate(db: DatabaseManager = Depends(get_db)) -> dict:
    return plot_service.invalidate(db)
