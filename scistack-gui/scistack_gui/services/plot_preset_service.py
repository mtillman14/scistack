"""
Plot presets — Plot Studio's named figure settings, applied to any variable.

An adapter only: what a preset owns, storing it and applying it all belong to
``scistackplot.presets`` and ``scistackplotdb.presets`` (CLAUDE.md NOTE 3).
This module turns their results into JSON and loads the target variable's
table with the panel's cached source (``plot_service.get_source``), so an
applied preset is checked against the same frames the panel draws from.

Like saved plots, applying a preset does NOT write the variable's Variants
pins: the panel persists ``variant_sets`` itself, and a preset never changes
them anyway (they are the target's data).

Presets need a database, so none of these calls accept a ``csv_path``.
See ``.claude/plan-plot-presets.md`` and ``docs/claude/plot-presets.md``.
"""

from __future__ import annotations

import logging

from scistack_gui.services import plot_service

logger = logging.getLogger(__name__)


def list_presets(db, shape: str | None = None) -> dict:
    """Every visible preset, each with a warning when it was made on a
    variable of a different shape than the panel's (*shape*)."""
    from scistackplot import shape_warning
    from scistackplotdb import list_presets as _list

    presets = [
        {**p.to_dict(), "shape_warning": shape_warning(p.made_on_shape, shape)}
        for p in _list(db)
    ]
    logger.info(
        "[preset] %d preset(s) listed for a %s panel (%d with a shape warning)",
        len(presets),
        shape or "?",
        sum(1 for p in presets if p["shape_warning"]),
    )
    return {"presets": presets}


def save(
    db,
    name: str,
    spec: dict,
    *,
    made_on_shape: str | None = None,
    overwrite: bool = True,
    current_preset_id: str | None = None,
) -> dict:
    """Save the panel's settings, without its data, as the preset *name*.

    Returns ``{"ok": True, "preset", "presets"}``. With ``overwrite=False``,
    another preset's name is a question: ``{"ok": False, "exists", "message"}``.
    """
    from scistackplotdb import PresetExists, save_preset

    try:
        info = save_preset(
            db,
            name,
            spec,
            made_on_shape=made_on_shape,
            overwrite=overwrite,
            current_preset_id=current_preset_id,
        )
    except PresetExists as exists:
        return {"ok": False, "exists": exists.existing.to_dict(), "message": str(exists)}
    return {"ok": True, "preset": info.to_dict(), **list_presets(db, made_on_shape)}


def apply(db, preset_id: str, spec: dict, version: int | None = None) -> dict:
    """Apply a preset to the panel's variable: the new spec, the notes, and
    the capability report for it, in one round trip.

    *spec* is the panel's CURRENT spec. Its data, variant pins, title, y label
    and y limits are kept; every other setting becomes the preset's. The
    database is held only while the preset row and the data frames load.
    """
    from scistackplot import capabilities
    from scistackplotdb import apply_preset_to

    from scistack_gui.db import db_connection

    target = plot_service._spec_from_payload(spec)
    loaded: dict = {}

    with db_connection("plot_preset_apply"):
        source = plot_service.get_source(db)

        def table_for(applied):
            loaded["table"] = plot_service._table_for(source, applied)
            return loaded["table"]

        result = apply_preset_to(db, preset_id, target, version=version, table_for=table_for)

    caps = None
    table = loaded.get("table")
    if table is not None:
        try:
            caps = capabilities(result.spec, table)
        except Exception:
            # The panel re-asks on its first resolve; the figure says why.
            logger.warning(
                "[preset] capability report failed for %r on %s; the panel will "
                "ask again",
                result.info.name,
                target.y_measure,
                exc_info=True,
            )
    logger.info(
        "[preset] applied %r v%d to %s: %d note(s)%s",
        result.info.name,
        result.info.version,
        target.y_measure,
        len(result.notes),
        "" if caps is not None else " (no capability report)",
    )
    return {"preset": result.to_dict(), "capabilities": caps}


def rename(db, preset_id: str, name: str, shape: str | None = None) -> dict:
    from scistackplotdb import rename_preset

    info = rename_preset(db, preset_id, name)
    return {"preset": info.to_dict(), **list_presets(db, shape)}


def hide(db, preset_id: str, hidden: bool = True, shape: str | None = None) -> dict:
    """"Remove" a preset: hidden from the list, every row kept."""
    from scistackplotdb import hide_preset

    info = hide_preset(db, preset_id, hidden=hidden)
    return {"preset": info.to_dict(), **list_presets(db, shape)}


def history(db, preset_id: str) -> dict:
    """Every version of one preset, newest first."""
    from scistackplotdb import preset_history

    return {
        "preset_id": preset_id,
        "versions": [v.to_dict() for v in preset_history(db, preset_id)],
    }
