"""
StatStudio — the handler table for both transports (see ``api/plot.py``).

    POST /api/stats/open                stats_open
    POST /api/stats/compute             stats_compute
    POST /api/stats/export              stats_export
    POST /api/stats/divergence          stats_divergence
    POST /api/stats/sync                stats_sync
    POST /api/stats/overlay             stats_overlay_bars
    POST /api/stats/overlay-for-plot    stats_overlay_for_plot
    POST /api/stats/linked              stats_linked
    POST /api/stats/saved/list          stats_saved_list
    POST /api/stats/saved/save          stats_saved_save
    POST /api/stats/saved/rename        stats_saved_rename
    POST /api/stats/saved/hide          stats_saved_hide
    POST /api/stats/saved/history       stats_saved_history
    POST /api/stats/presets/list        stats_preset_list
    POST /api/stats/presets/save        stats_preset_save
    POST /api/stats/presets/apply       stats_preset_apply
    POST /api/stats/presets/hide        stats_preset_hide
    POST /api/stats/add-to-pipeline     stats_add_to_pipeline

Lock policy, as for plots: every handler whose service loads tables takes
the connection itself, only for the load (``holds_db_lock=False``).
Divergence and sync compare two specs and never touch the database.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import stats_service

logger = logging.getLogger(__name__)

router = APIRouter()

_BAD_REQUEST = {ValueError: 400, KeyError: 400}


class OpenRequest(BaseModel):
    stats_id: str | None = None
    version: int | None = None
    seed: dict | None = None
    plot_id: str | None = None
    variable: str | None = None
    csv_path: str | None = None


class ComputeRequest(BaseModel):
    spec: dict
    csv_path: str | None = None


class ExportRequest(BaseModel):
    spec: dict
    fmt: str
    which: str
    csv_path: str | None = None
    path: str | None = None


class PairRequest(BaseModel):
    stats_spec: dict
    plot_spec: dict
    direction: str | None = None
    csv_path: str | None = None


class OverlayForPlotRequest(BaseModel):
    plot_id: str
    plot_spec: dict


class LinkedRequest(BaseModel):
    plot_id: str


class SavedSaveRequest(BaseModel):
    name: str
    spec: dict
    overwrite: bool | None = True
    current_stats_id: str | None = None


class SavedIdRequest(BaseModel):
    stats_id: str
    name: str | None = None
    hidden: bool | None = True


class PresetSaveRequest(BaseModel):
    name: str
    spec: dict


class PresetIdRequest(BaseModel):
    preset_id: str
    spec: dict | None = None
    hidden: bool | None = True


class AddRequest(BaseModel):
    spec: dict
    function_name: str | None = None
    output_variable: str | None = None
    finalized: bool | None = True


def _open(db, req: OpenRequest) -> dict:
    return stats_service.open_stats(db, stats_id=req.stats_id, version=req.version, seed=req.seed,
                                    plot_id=req.plot_id, variable=req.variable, csv_path=req.csv_path)


def _compute(db, req: ComputeRequest) -> dict:
    return stats_service.compute(db, req.spec, csv_path=req.csv_path)


def _export(db, req: ExportRequest) -> dict:
    return stats_service.export(db, req.spec, req.fmt, req.which, csv_path=req.csv_path, path=req.path)


def _divergence(req: PairRequest) -> dict:
    return stats_service.divergence(req.stats_spec, req.plot_spec)


def _sync(req: PairRequest) -> dict:
    return stats_service.sync(req.stats_spec, req.plot_spec, req.direction or "")


def _overlay(db, req: PairRequest) -> dict:
    return stats_service.overlay_bars(db, req.stats_spec, req.plot_spec, csv_path=req.csv_path)


def _overlay_for_plot(db, req: OverlayForPlotRequest) -> dict:
    return stats_service.overlay_for_plot(db, req.plot_id, req.plot_spec)


def _linked(db, req: LinkedRequest) -> dict:
    return stats_service.linked(db, req.plot_id)


def _saved_list(db) -> dict:
    return stats_service.saved_list(db)


def _saved_save(db, req: SavedSaveRequest) -> dict:
    return stats_service.saved_save(db, req.name, req.spec, overwrite=req.overwrite is not False,
                                    current_stats_id=req.current_stats_id)


def _saved_rename(db, req: SavedIdRequest) -> dict:
    return stats_service.saved_rename(db, req.stats_id, req.name or "")


def _saved_hide(db, req: SavedIdRequest) -> dict:
    return stats_service.saved_hide(db, req.stats_id, req.hidden is not False)


def _saved_history(db, req: SavedIdRequest) -> dict:
    return stats_service.saved_history(db, req.stats_id)


def _preset_list(db) -> dict:
    return stats_service.presets_list(db)


def _preset_save(db, req: PresetSaveRequest) -> dict:
    return stats_service.presets_save(db, req.name, req.spec)


def _preset_apply(db, req: PresetIdRequest) -> dict:
    return stats_service.presets_apply(db, req.preset_id, req.spec or {})


def _preset_hide(db, req: PresetIdRequest) -> dict:
    return stats_service.presets_hide(db, req.preset_id, req.hidden is not False)


def _add_to_pipeline(db, req: AddRequest) -> dict:
    return stats_service.add_to_pipeline(db, req.spec, function_name=req.function_name,
                                         output_variable=req.output_variable, finalized=req.finalized is not False)


STATS_HANDLERS: tuple[Handler, ...] = (
    Handler("stats_open", "/stats/open", OpenRequest, _open, holds_db_lock=False,
            http_errors=_BAD_REQUEST, db_optional=True, undoable=False),
    Handler("stats_compute", "/stats/compute", ComputeRequest, _compute, holds_db_lock=False,
            http_errors=_BAD_REQUEST, db_optional=True, undoable=False),
    Handler("stats_export", "/stats/export", ExportRequest, _export, holds_db_lock=False,
            http_errors={**_BAD_REQUEST, OSError: 400}, db_optional=True, undoable=False),
    Handler("stats_divergence", "/stats/divergence", PairRequest, _divergence, needs_db=False,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_sync", "/stats/sync", PairRequest, _sync, needs_db=False,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_overlay_bars", "/stats/overlay", PairRequest, _overlay, holds_db_lock=False,
            http_errors=_BAD_REQUEST, db_optional=True, undoable=False),
    Handler("stats_overlay_for_plot", "/stats/overlay-for-plot", OverlayForPlotRequest, _overlay_for_plot,
            holds_db_lock=False, http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_linked", "/stats/linked", LinkedRequest, _linked, http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_saved_list", "/stats/saved/list", None, _saved_list, undoable=False),
    Handler("stats_saved_save", "/stats/saved/save", SavedSaveRequest, _saved_save,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_saved_rename", "/stats/saved/rename", SavedIdRequest, _saved_rename,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_saved_hide", "/stats/saved/hide", SavedIdRequest, _saved_hide,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_saved_history", "/stats/saved/history", SavedIdRequest, _saved_history,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_preset_list", "/stats/presets/list", None, _preset_list, undoable=False),
    Handler("stats_preset_save", "/stats/presets/save", PresetSaveRequest, _preset_save,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_preset_apply", "/stats/presets/apply", PresetIdRequest, _preset_apply,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_preset_hide", "/stats/presets/hide", PresetIdRequest, _preset_hide,
            http_errors=_BAD_REQUEST, undoable=False),
    Handler("stats_add_to_pipeline", "/stats/add-to-pipeline", AddRequest, _add_to_pipeline,
            http_errors=_BAD_REQUEST, undoable=True, undo_label="add stats to pipeline"),
)

install_routes(router, STATS_HANDLERS)
