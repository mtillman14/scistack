"""
The shared design of a linked figure and table: one owner
(docs/claude/studio-unification.md, user decision 2026-11-01).

A saved plot and the StatStudio tables linked to it used to each keep their
own copy of the design, with "Sync to…" banners to copy one onto the other.
Now the design of a linked pair lives in ONE row set here, keyed by the
saved plot's ``plot_id``. Either studio writes it; both read it on open, and
an edit in one reaches the other through the GUI's broadcast.

What the design is, field by field, is still
:data:`scistackplot.design.DESIGN_FIELDS`. This store keeps a whole
``PlotSpec`` dict (so it reads back through ``restore_spec``) but only its
design fields are ever applied (:func:`apply_design`, through
``copy_design``). Display settings stay with each artifact.

**A design exists only once something is linked.** :func:`link_design` is
called when a StatStudio table is set up from a saved plot. An unlinked plot
has no row here and its saved envelope stays the owner of its design.

Rows are the shared ``VersionedStore`` (append-only, never deleted): the
item's NAME is the plot id, so "the design of plot X" is a name lookup, and
each change is a new version. A write that would not change the design
(same ``design_fingerprint``) appends nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scistacklog import Log

from ._versioned import VersionedStore

LAYER = "scistackplotdb"

TABLE = "_plot_design"

ENVELOPE_FORMAT = 1


class DesignError(ValueError):
    """A design that cannot be stored or read as asked."""


STORE = VersionedStore(
    table=TABLE,
    id_column="design_id",
    scope_column=None,
    noun="shared design",
    tag="design",
    error=DesignError,
)


@dataclass(frozen=True)
class StoredDesign:
    plot_id: str
    design_id: str
    version: int
    #: The stored spec (a full PlotSpec; only its design fields apply).
    spec: Any
    fingerprint: str


def _row_for(db, plot_id: str):
    if not plot_id:
        return None
    return STORE.visible_by_name(db, None, plot_id)


def load_design(db, plot_id: str) -> StoredDesign | None:
    """The newest shared design of saved plot *plot_id*, or None when it is
    not linked to anything (or the row cannot be read)."""
    from scistackplot import restore_spec
    from scistackplot.design import design_fingerprint

    row = _row_for(db, plot_id)
    if row is None:
        return None
    _, text = STORE.load(db, row[0])
    envelope = STORE.parse_envelope(text, f"design of {plot_id[:8]}")
    raw = envelope.get("spec")
    if not isinstance(raw, dict):
        Log.warn("[design] %s: stored design has no spec; ignoring it", plot_id[:8], layer=LAYER)
        return None
    restored = restore_spec(raw, fallback_measure=(raw.get("measures") or [None])[0])
    for note in restored.notes:
        Log.info("[design] %s: restore note: %s", plot_id[:8], getattr(note, "message", note), layer=LAYER)
    spec = restored.spec
    return StoredDesign(plot_id, row[0], row[3], spec, design_fingerprint(spec))


def is_linked(db, plot_id: str | None) -> bool:
    return bool(plot_id) and _row_for(db, plot_id) is not None


def save_design(db, plot_id: str, spec) -> tuple[StoredDesign, bool]:
    """Write *spec*'s design as the shared design of *plot_id*.

    Returns ``(stored, changed)``. When the design fields are the same as
    the newest stored version, nothing is appended and ``changed`` is
    False, so a broadcast echo between two open studios stops here.
    """
    from scistackplot.design import design_fingerprint

    if not plot_id:
        raise DesignError("A shared design needs the saved plot it belongs to.")
    fingerprint = design_fingerprint(spec)
    current = load_design(db, plot_id)
    if current is not None and current.fingerprint == fingerprint:
        Log.debug("[design] %s: unchanged (fingerprint %s); nothing appended", plot_id[:8], fingerprint,
                  layer=LAYER)
        return current, False
    envelope = {"format": ENVELOPE_FORMAT, "plot_id": plot_id, "spec": spec.to_dict()}
    row, is_new = STORE.save(db, None, plot_id, envelope, overwrite=True, current_id=None,
                             on_clash=lambda r: DesignError(f"design clash for {plot_id}"))
    Log.info("[design] %s: %s version %d (fingerprint %s)", plot_id[:8],
             "linked, first" if is_new else "changed, new", row[3], fingerprint, layer=LAYER)
    return StoredDesign(plot_id, row[0], row[3], spec, fingerprint), True


def link_design(db, plot_id: str, spec) -> StoredDesign:
    """Make *plot_id*'s design shared, starting from *spec* when nothing is
    stored yet. An already-linked plot keeps its stored design."""
    current = load_design(db, plot_id)
    if current is not None:
        Log.info("[design] %s: already linked (version %d)", plot_id[:8], current.version, layer=LAYER)
        return current
    stored, _ = save_design(db, plot_id, spec)
    return stored


def apply_design(db, plot_id: str | None, spec) -> tuple[Any, list, int | None]:
    """*spec* with the shared design of *plot_id* copied onto it
    (``copy_design``: design fields only, display kept). Unchanged, with
    version None, when *plot_id* is not linked."""
    from scistackplot.design import copy_design

    stored = load_design(db, plot_id) if plot_id else None
    if stored is None:
        return spec, [], None
    merged, notes = copy_design(stored.spec, spec)
    Log.info("[design] %s: applied version %d onto a %s spec (%d note(s))", plot_id[:8], stored.version,
             getattr(spec.kind, "value", spec.kind), len(notes), layer=LAYER)
    return merged, notes, stored.version
