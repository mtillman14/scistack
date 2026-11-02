"""
The shared design of a linked figure + table, for both studios
(docs/claude/studio-unification.md, Stage 6).

Storage and the rules live in ``scistackplotdb.designs`` (the one owner).
This adapter is what the webviews call:

* ``update(plot_id, spec)``: a studio edited a linked design. Writes a new
  version when the design fields changed, and says so, so the webview
  broadcasts only real changes (an echo from the other studio stops here).
* ``apply(plot_id, spec)``: the other studio heard ``design_changed`` and
  takes the stored design onto its own spec (design fields only).
* ``write_if_linked``: saving a figure or a table writes the design too.

Nothing here applies to an unlinked plot: its saved envelope stays the
owner of its design.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _plot_spec(payload: dict):
    from scistackplot import PlotSpec

    if not isinstance(payload, dict):
        raise ValueError("A design must be a plot spec (a table of settings).")
    return PlotSpec.from_dict(payload)


def write_if_linked(db, plot_id: str | None, spec_payload: dict, *, route: str) -> dict:
    """Write *spec_payload*'s design as *plot_id*'s shared design when the
    plot is linked. ``{"linked", "changed", "version"}``."""
    from scistackplotdb import is_linked, save_design

    if not plot_id or not is_linked(db, plot_id):
        logger.debug("[design] %s: %s is not linked; nothing written", route, (plot_id or "-")[:8])
        return {"linked": False, "changed": False, "version": None}
    stored, changed = save_design(db, plot_id, _plot_spec(spec_payload))
    logger.info("[design] %s: %s %s (version %d)", route, plot_id[:8], "changed" if changed else "unchanged",
                stored.version)
    return {"linked": True, "changed": changed, "version": stored.version}


def update(db, plot_id: str, spec_payload: dict) -> dict:
    """A studio's live edit of a linked design (debounced by the webview)."""
    return write_if_linked(db, plot_id, spec_payload, route="design_update")


def apply(db, plot_id: str, spec_payload: dict) -> dict:
    """*spec_payload* with *plot_id*'s shared design on it.
    ``{"spec", "notes", "version"}``; unchanged, version None, when unlinked."""
    from scistackplotdb import apply_design

    merged, notes, version = apply_design(db, plot_id, _plot_spec(spec_payload))
    logger.info("[design] design_apply %s: version %s, %d note(s)", plot_id[:8], version, len(notes))
    return {"spec": merged.to_dict(), "notes": [n.to_dict() for n in notes], "version": version}
