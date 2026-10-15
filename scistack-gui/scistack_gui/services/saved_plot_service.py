"""
Saved plots — Plot Studio's named, per-variable figures.

An adapter only: storage, naming rules, versioning, restoring drifted
settings and reconciling them with today's data all belong to
``scistackplotdb.saved`` and ``scistackplot.restore`` (CLAUDE.md NOTE 3).
This module turns their results into JSON and loads the data table with the
panel's cached source (``plot_service.get_source``), so a saved plot opens on
the same frames the panel draws from.

What this module deliberately does NOT do: write the variable's Variants pins
(`variant_selection` in the intent store) when a plot is opened. Neither does
the panel any more (2026-10-15): opening a saved plot re-baselines its pin
effect instead of persisting the opened rows, because a reopened plot's rows
can carry a FROZEN code version (docs/claude/variant-row-selection.md) that
must not become every new plot's default. Only a user's edit of the Variants
section writes pins.

Saved plots need a database, so none of these calls accept a ``csv_path``.
See ``.claude/plan-saved-plots.md``.
"""

from __future__ import annotations

import logging

from scistack_gui.services import plot_service

logger = logging.getLogger(__name__)


def list_plots(db, variable: str) -> dict:
    """The newest version of each visible saved plot of *variable*."""
    from scistackplotdb import list_saved_plots

    plots = [p.to_dict() for p in list_saved_plots(db, variable)]
    logger.info("[saved_plot] %s: %d saved plot(s)", variable, len(plots))
    return {"variable": variable, "plots": plots}


def save(
    db,
    variable: str,
    name: str,
    spec: dict,
    view: dict | None,
    *,
    overwrite: bool = True,
    current_plot_id: str | None = None,
) -> dict:
    """Save the panel's spec and view settings under *name*.

    Returns ``{"ok": True, "plot", "plots"}``: the saved version and the
    refreshed list, so the panel updates its rail from one reply.

    With ``overwrite=False``, a name belonging to a DIFFERENT plot than
    *current_plot_id* is not an error but a question. The reply is
    ``{"ok": False, "exists": <that plot>, "message"}``, and the panel asks
    before resending with ``overwrite=True``.
    """
    from scistackplotdb import SavedPlotExists, save_plot

    source = plot_service.get_source(db)

    def table_for(parsed_spec):
        # The table the panel draws from (cached), so the save freezes the
        # code version the user is looking at (scistackplotdb.saved.freeze_code).
        return plot_service._table_for(source, parsed_spec)

    try:
        info = save_plot(
            db,
            variable,
            name,
            spec,
            view or {},
            overwrite=overwrite,
            current_plot_id=current_plot_id,
            table_for=table_for,
        )
    except SavedPlotExists as exists:
        return {"ok": False, "exists": exists.existing.to_dict(), "message": str(exists)}
    return {"ok": True, "plot": info.to_dict(), **list_plots(db, variable)}


def open_plot(db, plot_id: str, version: int | None = None) -> dict:
    """Open a saved plot: its restored spec, view settings and notes, plus the
    capability report for that spec, in one round trip.

    The database is held only while the stored row and the data frames load.
    Computing the capability report runs on the in-memory table after release,
    the same split as ``plot_service._load``.
    """
    from scistackplot import capabilities
    from scistackplotdb import load_saved_plot

    from scistack_gui.db import db_connection

    loaded: dict = {}

    with db_connection("plot_saved_open"):
        source = plot_service.get_source(db)

        def table_for(spec):
            loaded["table"] = plot_service._table_for(source, spec)
            return loaded["table"]

        saved = load_saved_plot(db, plot_id, version=version, table_for=table_for)

    caps = None
    table = loaded.get("table")
    if saved.spec.custom is not None:
        # Drawn by a plot_ function: its report needs the node's wiring
        # (custom_plot_service.capabilities_for), which the panel's first
        # resolve asks for. The variable report would offer built-in kinds.
        table = None
    if table is not None:
        try:
            caps = capabilities(saved.spec, table)
        except Exception:
            # The panel re-asks for capabilities on its first resolve anyway.
            # A spec that cannot be reported on here will say why there, on
            # the figure, which is where the user is looking.
            logger.warning(
                "[saved_plot] capability report failed for %s / %r; the panel "
                "will ask again",
                saved.info.variable,
                saved.info.name,
                exc_info=True,
            )
    logger.info(
        "[saved_plot] opened %s / %r v%d: %d note(s)%s",
        saved.info.variable,
        saved.info.name,
        saved.info.version,
        len(saved.notes),
        "" if caps is not None else " (no capability report)",
    )
    return {"plot": saved.to_dict(), "capabilities": caps}


def follow_latest(db, plot_id: str) -> dict:
    """Answer "switch to latest" for a saved plot whose data has a newer code
    version: appends a version whose rows follow latest from now on
    (``scistackplotdb.follow_latest_saved_plot``), then reopens it so the panel
    gets the new spec and capabilities in the same reply as ``open_plot``."""
    from scistackplotdb import follow_latest_saved_plot

    from scistack_gui.db import db_connection

    with db_connection("plot_saved_follow_latest"):
        info = follow_latest_saved_plot(db, plot_id)
        plots = list_plots(db, info.variable)
    logger.info(
        "[saved_plot] %s / %r switched to follow latest (now v%d)",
        info.variable,
        info.name,
        info.version,
    )
    return {**open_plot(db, plot_id), **plots}


def rename(db, plot_id: str, name: str) -> dict:
    from scistackplotdb import rename_saved_plot

    info = rename_saved_plot(db, plot_id, name)
    return {"plot": info.to_dict(), **list_plots(db, info.variable)}


def hide(db, plot_id: str, hidden: bool = True) -> dict:
    """"Remove" a saved plot: hidden from the list, every row kept."""
    from scistackplotdb import hide_saved_plot

    info = hide_saved_plot(db, plot_id, hidden=hidden)
    return {"plot": info.to_dict(), **list_plots(db, info.variable)}


def history(db, plot_id: str) -> dict:
    """Every version of one plot, newest first."""
    from scistackplotdb import saved_plot_history

    return {"plot_id": plot_id, "versions": [v.to_dict() for v in saved_plot_history(db, plot_id)]}
