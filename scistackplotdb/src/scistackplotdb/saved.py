"""
Saved plots: named, per-variable Plot Studio figures kept in the project
database.

A saved plot is **display intent**. It changes nothing a run computes, so it is
not an aspect of the GUI's intent store (docs/claude/intent-and-fact.md §1).
It lives here, in the layer that knows both the database and the spec, so a
script can reopen a figure the GUI saved::

    from scistackplotdb import list_saved_plots, load_saved_plot
    saved = load_saved_plot(db, list_saved_plots(db, "StepLength")[0].plot_id)
    render(ScidbSource(db).get_table(saved.spec.variant_variables()), saved.spec)

**One table, append-only.** ``_saved_plot`` holds one row per *version*.
Saving under a name that is already showing appends a version to that plot;
the newest is what the list shows and what opens. Nothing is ever deleted
(feedback_never_delete_mark_hidden): "remove" sets ``hidden`` on every
version of the plot, and the rows stay.

``plot_id`` is the plot's identity, stable across renames. A name is a label.
Keying by name would make rename an update of an identity column, and would
make "a new plot reusing a removed plot's name" silently inherit its history.

**The stored spec is always the current format.** :func:`save_plot` reads the
incoming spec strictly, so what lands in the table is exactly what this
version writes. Old formats are only ever *read*, by
:func:`scistackplot.restore_spec`. The stored copy is never rewritten on
open; only an explicit save writes. See ``.claude/plan-saved-plots.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from scistacklog import Log

from ._versioned import MAX_NAME_LENGTH, VersionedStore  # noqa: F401  (re-exported)

LAYER = "scistackplotdb"

TABLE = "_saved_plot"

#: The envelope's layout version. Written on every save and logged on every
#: load; nothing branches on it (salvage, never migrate).
ENVELOPE_FORMAT = 1


class SavedPlotError(ValueError):
    """A save, rename or load that cannot be done as asked."""


class SavedPlotExists(SavedPlotError):
    """``save_plot(overwrite=False)`` found ANOTHER visible plot with that name.

    ``existing`` is that plot, so the caller can ask the user and retry with
    ``overwrite=True``. The name comparison stays here, where names are
    normalised, instead of in each caller.
    """

    def __init__(self, existing: "SavedPlotInfo"):
        super().__init__(
            f"{existing.variable} already has a saved plot named "
            f"{existing.name!r} (version {existing.version})."
        )
        self.existing = existing


@dataclass(frozen=True)
class SavedPlotInfo:
    """One version of a saved plot, without its contents: a list row."""

    plot_id: str
    variable: str
    name: str
    version: int
    saved_at: str
    hidden: bool = False

    def to_dict(self) -> dict:
        return {
            "plot_id": self.plot_id,
            "variable": self.variable,
            "name": self.name,
            "version": self.version,
            "saved_at": self.saved_at,
            "hidden": self.hidden,
        }


@dataclass(frozen=True)
class SavedPlot:
    """A saved plot, opened: the restored spec plus what did not survive."""

    info: SavedPlotInfo
    spec: Any  # scistackplot.PlotSpec
    #: The panel's own view settings (preview mode, aspect choice, figure
    #: index), stored verbatim. The frontend owns their meaning and reads them
    #: as leniently as the spec is read here.
    view: dict = field(default_factory=dict)
    #: ``scistackplot.RestoreNote`` s from restoring the spec and, when a table
    #: was given, from reconciling it with today's data.
    notes: list = field(default_factory=list)
    #: The envelope format this version was written in.
    stored_format: Any = None
    #: Code versions in today's data NEWER than the ones this plot was saved
    #: with: ``[{row, column, function, frozen, newer, newest, locations}]``
    #: (:func:`thaw_code`). Non-empty is the GUI's cue to ask "switch to
    #: latest?". Empty for a plot that follows latest or recorded no version.
    newer: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            **self.info.to_dict(),
            "spec": self.spec.to_dict(),
            "view": dict(self.view),
            "notes": [note.to_dict() for note in self.notes],
            "stored_format": self.stored_format,
            "newer": [dict(entry) for entry in self.newer],
        }


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

#: The rules (names, versions, hiding, never deleting) live in `_versioned`,
#: shared with presets so the two stores cannot drift apart.
STORE = VersionedStore(
    table=TABLE,
    id_column="plot_id",
    scope_column="variable",
    noun="saved plot",
    tag="saved_plot",
    error=SavedPlotError,
)


def ensure_table(db) -> None:
    """Create ``_saved_plot`` if absent. Called by every write."""
    STORE.ensure_table(db)


def _table_exists(db) -> bool:
    """Reads never create the table: listing an untouched database must not
    write to it."""
    return STORE.table_exists(db)


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def save_plot(
    db,
    variable: str,
    name: str,
    spec,
    view: dict | None = None,
    *,
    overwrite: bool = True,
    current_plot_id: str | None = None,
    table=None,
    table_for: Callable[[Any], Any] | None = None,
) -> SavedPlotInfo:
    """Save *spec* under *name* for *variable*, returning the new version.

    **Freezes the code it drew.** Given the table the plot draws from
    (*table*, or *table_for* to load it from the spec), every variant row not
    following latest records the function hashes of its rows
    (:func:`freeze_code`), so the plot reopens on that version after the code
    changes. Without a table nothing is frozen and the plot reopens on current
    code; that is logged.

    A visible plot of that name gets a new version, so the previous one is
    kept as history. Otherwise a new plot is started, even when a removed
    (hidden) plot had that name, because a new plot does not inherit an old
    plot's history.

    With ``overwrite=False``, adding a version to an existing plot is allowed
    only when that plot is *current_plot_id* (the one the user has open).
    Saving onto any OTHER plot's name raises :class:`SavedPlotExists` so the
    caller can ask first.

    *spec* is a ``PlotSpec`` or its dict. A dict is read **strictly**: the
    panel always sends the current format, and a spec that does not parse is
    a bug to surface, not something to store.
    """
    from scistackplot import PlotSpec, __version__ as plot_version

    variable = _check_text(variable, "variable")
    name = check_name(name)
    if isinstance(spec, dict):
        try:
            spec = PlotSpec.from_dict(spec)
        except Exception as exc:
            raise SavedPlotError(f"The plot's settings could not be read: {exc}") from exc
    if view is None:
        view = {}
    if not isinstance(view, dict):
        raise SavedPlotError(f"view must be a table of settings, got {type(view).__name__}")

    if table is None and table_for is not None:
        try:
            table = table_for(spec)
        except Exception as exc:
            Log.warn(
                "[saved_plot] %s / %r: the data could not be loaded to freeze its "
                "code version (%s); saving unfrozen",
                variable,
                name,
                exc,
                layer=LAYER,
            )
    if table is not None:
        frozen_code = freeze_code(db, spec, table)
    else:
        frozen_code = {}
        Log.info(
            "[saved_plot] %s / %r: no table given, so no code version is frozen "
            "(the plot reopens on current code)",
            variable,
            name,
            layer=LAYER,
        )

    envelope = {
        "format": ENVELOPE_FORMAT,
        "spec": _strip_frozen(spec).to_dict(),
        "view": view,
        "saved_with": {"scistackplot": plot_version},
        FROZEN_KEY: frozen_code,
    }
    row, _new = STORE.save(
        db,
        variable,
        name,
        envelope,
        overwrite=overwrite,
        current_id=current_plot_id,
        on_clash=lambda clash: SavedPlotExists(_info(clash)),
    )
    return _info(row)


def rename_saved_plot(db, plot_id: str, new_name: str) -> SavedPlotInfo:
    """Rename a plot (every version). Refused when another visible plot of
    the same variable already has that name."""
    return _info(STORE.rename(db, plot_id, new_name))


def hide_saved_plot(db, plot_id: str, hidden: bool = True) -> SavedPlotInfo:
    """Remove a plot from the list, keeping every row. ``hidden=False``
    brings it back."""
    return _info(STORE.set_hidden(db, plot_id, hidden))


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def list_saved_plots(db, variable: str, *, include_hidden: bool = False) -> list[SavedPlotInfo]:
    """The newest version of each saved plot of *variable*, newest first."""
    return [_info(row) for row in STORE.list_rows(db, variable, include_hidden=include_hidden)]


def saved_plot_history(db, plot_id: str) -> list[SavedPlotInfo]:
    """Every version of one plot, newest first."""
    return [_info(row) for row in STORE.history(db, plot_id)]


def load_saved_plot(
    db,
    plot_id: str,
    *,
    version: int | None = None,
    table=None,
    table_for: Callable[[Any], Any] | None = None,
) -> SavedPlot:
    """Open a saved plot (its newest version unless *version* is given).

    The spec is read with :func:`scistackplot.restore_spec`, so a plot saved
    by an older version of the plotting layer still opens. Settings that no
    longer exist or no longer parse come back as notes.

    To also reconcile the spec with today's data, pass either *table* (the
    ``LongTable`` the plot will draw from) or *table_for*, a callable that
    loads that table from the RESTORED spec. The table depends on the spec
    (its variant rows and factor variables decide what gets loaded), so a
    caller that cannot know the spec in advance hands over the loader. The
    GUI does this with its cached source. A loader that raises does not fail
    the open: the plot opens unreconciled, with a note saying why.

    Never rewrites the stored copy: a later build may salvage more of it.
    """
    from scistackplot import NoteKind, RestoreNote, reconcile, restore_spec

    row, text = STORE.load(db, plot_id, version)
    info = _info(row)
    envelope = STORE.parse_envelope(
        text, f"{info.variable} / {info.name!r} version {info.version}"
    )

    restored = restore_spec(envelope.get("spec"), fallback_measure=info.variable)
    spec, notes = restored.spec, list(restored.notes)
    if table is None and table_for is not None:
        try:
            table = table_for(spec)
        except Exception as exc:
            Log.warn(
                "[saved_plot] %s / %r: today's data could not be loaded for "
                "reconciling (%s); opening unreconciled",
                info.variable,
                info.name,
                exc,
                layer=LAYER,
            )
            notes.append(
                RestoreNote(
                    "",
                    NoteKind.NOT_IN_DATA,
                    f"today's data could not be loaded, so the settings were "
                    f"not checked against it: {exc}",
                )
            )
    newer: list = []
    frozen_code = envelope.get(FROZEN_KEY)
    if table is not None and isinstance(frozen_code, dict) and frozen_code:
        # Before reconcile, so it checks the roles against the table the
        # FROZEN rows derive (docs/claude/variant-row-selection.md).
        spec, newer, thaw_notes = thaw_code(
            db, spec, table, frozen_code, label=f"{info.variable} / {info.name!r}"
        )
        notes.extend(thaw_notes)
    elif not frozen_code:
        # Saved before freezing existed, or with no table: opens on current
        # code with no prompt. Nothing is back-filled (no migrations).
        Log.info(
            "[saved_plot] %s / %r: no frozen code version stored; opening on "
            "current code",
            info.variable,
            info.name,
            layer=LAYER,
        )
    if table is not None:
        reconciled = reconcile(spec, table)
        spec, notes = reconciled.spec, notes + list(reconciled.notes)

    view = envelope.get("view")
    if not isinstance(view, dict):
        Log.warn(
            "[saved_plot] %s / %r: stored view settings are not a table (%r); "
            "opening with the panel's defaults",
            info.variable,
            info.name,
            view,
            layer=LAYER,
        )
        view = {}

    stored_format = envelope.get("format")
    Log.info(
        "[saved_plot] opened %s / %r version %d (envelope format %r, saved with %s): "
        "%d note(s)",
        info.variable,
        info.name,
        info.version,
        stored_format,
        envelope.get("saved_with"),
        len(notes),
        layer=LAYER,
    )
    return SavedPlot(
        info=info,
        spec=spec,
        view=view,
        notes=notes,
        stored_format=stored_format,
        newer=newer,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def check_name(name: Any) -> str:
    """A saved plot's name, trimmed, or :class:`SavedPlotError` saying why not.

    The one statement of what a name may be (``VersionedStore.check_name``);
    the GUI asks the backend rather than repeating the rule.
    """
    return STORE.check_name(name)


def _check_text(value: Any, what: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SavedPlotError(f"A saved plot needs a {what}.")
    return value.strip()


def _info(row) -> SavedPlotInfo:
    plot_id, variable, name, version, saved_at, hidden = row
    return SavedPlotInfo(
        plot_id=str(plot_id),
        variable=str(variable),
        name=str(name),
        version=int(version),
        saved_at=str(saved_at),
        hidden=bool(hidden),
    )


# ---------------------------------------------------------------------------
# Frozen code versions (docs/claude/variant-row-selection.md)
# ---------------------------------------------------------------------------
#
# A saved plot remembers WHICH CODE drew it. At save time every variant row
# that does not follow latest records the function hashes of the rows it drew
# (``frozen_code`` in the envelope). On open, those hashes become today's
# ordinals in ``VariantSet.frozen`` — so the figure reopens on the version it
# was saved with — and anything newer is reported as ``SavedPlot.newer`` for
# the GUI's "switch to latest?" prompt. Hashes, not ordinals, are stored
# because ordinals renumber after a real Variants-popup delete.

#: Envelope key: ``{str(row_index): {function_name: [function_hash, ...]}}``.
FROZEN_KEY = "frozen_code"


def _row_masks(spec, table) -> list[tuple[int, Any]]:
    """``[(row_index, mask)]`` — the rows each variant row DRAWS, by the same
    owner and first-match-wins order the figure uses
    (``scistackplot.variants.row_mask`` / ``apply_variant_sets``).

    With no row concrete, the figure draws an empty row's effective selection;
    that is frozen as row 0 (synthesised on open if the spec has none).
    """
    import pandas as pd
    from scistackplot.spec import VariantSet
    from scistackplot.variants import defined_sets, is_stated, row_mask

    frame = table.frame
    if not defined_sets(spec.variant_sets):
        first = spec.variant_sets[0] if spec.variant_sets else VariantSet()
        return [(0, row_mask(table, spec, first))]
    claimed = pd.Series(False, index=frame.index)
    out = []
    for index, variant in enumerate(spec.variant_sets):
        if not is_stated(variant, index):
            continue
        fresh = row_mask(table, spec, variant) & ~claimed
        claimed |= fresh
        out.append((index, fresh))
    return out


def freeze_code(db, spec, table) -> dict[str, dict[str, list[str]]]:
    """The envelope's ``frozen_code``: per variant row not following latest,
    every function in the drawn records' chains and the hash(es) it ran.

    Works with ONE version too (the usual state at save time): there is no
    ``Code:`` column then, but the chain still names the hash, and that is
    what lets the plot reopen on it after a second version appears.
    """
    from scidb.provenance_query import code_versions_batch

    frame = table.frame
    if "record_id" not in frame.columns:
        Log.warn(
            "[saved_plot] %r: the table has no record_id column, so no code "
            "version can be frozen — the plot will reopen on current code",
            getattr(table, "name", None),
            layer=LAYER,
        )
        return {}
    rows = _row_masks(spec, table)
    follow = {
        index for index, variant in enumerate(spec.variant_sets) if variant.follow_latest
    }
    wanted = {index: mask for index, mask in rows if index not in follow}
    record_ids = sorted(
        {
            str(rid)
            for mask in wanted.values()
            for rid in frame.loc[mask, "record_id"].dropna().unique()
        }
    )
    chains = code_versions_batch(db._duck, record_ids) if record_ids else {}
    frozen: dict[str, dict[str, list[str]]] = {}
    for index, mask in wanted.items():
        by_fn: dict[str, set] = {}
        for rid in frame.loc[mask, "record_id"].dropna().unique():
            for fn_name, fn_hash in (chains.get(str(rid)) or {}).items():
                by_fn.setdefault(fn_name, set()).add(fn_hash)
        if by_fn:
            frozen[str(index)] = {fn: sorted(hashes) for fn, hashes in sorted(by_fn.items())}
    Log.info(
        "[saved_plot] froze code for %d row(s) over %d record(s): %s%s",
        len(frozen),
        len(record_ids),
        {
            index: {fn: [h[:12] for h in hashes] for fn, hashes in fns.items()}
            for index, fns in frozen.items()
        },
        f" (rows following latest: {sorted(follow)})" if follow else "",
        layer=LAYER,
    )
    return frozen


def _strip_frozen(spec):
    """The spec as stored: ``frozen`` ordinals are derived on open from the
    envelope's hashes, never stored (an ordinal can renumber)."""
    from dataclasses import replace

    return replace(
        spec,
        variant_sets=[replace(v, frozen={}) for v in spec.variant_sets],
    )


def _code_columns(table) -> dict[str, str]:
    """``{function_name: column}`` for every code axis the table carries."""
    out = {}
    for factor in table.factors:
        origin = factor.origin or {}
        if origin.get("kind") == "code" and origin.get("function"):
            out[origin["function"]] = factor.name
    return out


def thaw_code(db, spec, table, frozen_code: dict, *, label: str = ""):
    """``(spec, newer, notes)`` — apply a stored freeze to today's data.

    For each frozen row and function that is CURRENTLY an axis (two or more
    versions, so a ``Code:`` column), the stored hashes become today's
    ordinals in ``VariantSet.frozen``. A single-version function needs
    nothing. ``newer`` lists, per row and function, the versions present in
    the data that are newer than every frozen one — what the GUI asks about.

    A frozen hash that no longer exists (its version was deleted) is NOT
    silently replaced: it gets a note, and the row falls back to the
    remaining frozen versions or, with none left, to current code — which the
    note says, and ``newer`` offers to make permanent.
    """
    from dataclasses import replace

    from scidb.provenance_query import function_versions
    from scistackplot import NoteKind, RestoreNote
    from scistackplot.spec import VariantSet
    from scistackplot.variants import _ordinal_rank

    if not frozen_code:
        return spec, [], []
    columns = _code_columns(table)
    fn_names = sorted({fn for fns in frozen_code.values() for fn in fns})
    versions = function_versions(db._duck, fn_names)
    sets = list(spec.variant_sets)
    newer: list[dict] = []
    notes: list = []
    frame = table.frame
    levels = [key for key in table.schema_levels if key in frame.columns]

    for raw_index, fns in sorted(frozen_code.items(), key=lambda kv: int(kv[0])):
        index = int(raw_index)
        if index >= len(sets):
            if index == 0 and not sets:
                sets.append(VariantSet())
            else:
                continue
        variant = sets[index]
        if variant.follow_latest:
            continue
        frozen: dict[str, list[str]] = {}
        for fn_name, hashes in fns.items():
            by_hash = {v["function_hash"]: v["version"] for v in versions.get(fn_name, [])}
            ordinals = [by_hash[h] for h in hashes if h in by_hash]
            missing = [h for h in hashes if h not in by_hash]
            if missing:
                notes.append(
                    RestoreNote(
                        f"variant_sets[{index}]",
                        NoteKind.NOT_IN_DATA,
                        f"the {fn_name} version(s) this plot was saved with "
                        f"({', '.join(h[:12] for h in missing)}) no longer exist"
                        + ("" if ordinals else "; showing the current version instead"),
                    )
                )
            column = columns.get(fn_name)
            if column is None:
                continue  # one version today: nothing to choose between
            if ordinals:
                frozen[column] = sorted(set(ordinals), key=_ordinal_rank)
            present = frame[column].dropna().astype(str)
            top = max((_ordinal_rank(o) for o in ordinals), default=0)
            later = sorted(
                {level for level in present.unique() if _ordinal_rank(level) > top},
                key=_ordinal_rank,
            )
            if later:
                at = frame[frame[column].astype(str).isin(later)]
                newer.append(
                    {
                        "row": index,
                        "column": column,
                        "function": fn_name,
                        "frozen": list(frozen.get(column, [])),
                        "newer": later,
                        "newest": later[-1],
                        "locations": int(len(at[levels].drop_duplicates())) if levels else int(len(at)),
                    }
                )
        if frozen:
            sets[index] = replace(variant, frozen=frozen)

    Log.info(
        "[saved_plot] %s: frozen code -> %s; newer: %s",
        label,
        {i: v.frozen for i, v in enumerate(sets) if v.frozen} or "nothing to pin",
        [f"row {n['row']} {n['function']} {n['frozen']} < {n['newer']}" for n in newer]
        or "none",
        layer=LAYER,
    )
    return replace(spec, variant_sets=sets), newer, notes


def follow_latest_saved_plot(db, plot_id: str, *, table=None) -> SavedPlotInfo:
    """Answer "switch to latest" for a saved plot: append a version whose rows
    follow latest code from now on (never frozen again, never asked again).

    Each row drops its frozen pins AND its own code choices (``Code:``/``Run:``
    keys and the latest flag) — a row still saying ``Code:f = v1`` would keep
    drawing v1. What remains (branch params, the variable) is kept, and the
    row then draws current code by the effective-selection rule. Appends, as
    any re-save does; older versions stay.
    """
    from dataclasses import replace

    from scistackplot import restore_spec
    from scistackplot.spec import VariantSet
    from scistackplot.variants import is_function_axis

    from .load import LATEST_COLUMN

    row, text = STORE.load(db, plot_id, None)
    info = _info(row)
    envelope = STORE.parse_envelope(text, f"{info.variable} / {info.name!r}")
    spec = restore_spec(envelope.get("spec"), fallback_measure=info.variable).spec
    sets = list(spec.variant_sets) or [VariantSet()]
    sets = [
        replace(
            variant,
            frozen={},
            follow_latest=True,
            selection={
                key: value
                for key, value in variant.selection.items()
                if not is_function_axis(key) and key != LATEST_COLUMN
            },
        )
        for variant in sets
    ]
    spec = replace(spec, variant_sets=sets)
    view = envelope.get("view") if isinstance(envelope.get("view"), dict) else {}
    new_info = save_plot(
        db,
        info.variable,
        info.name,
        spec,
        view,
        overwrite=True,
        current_plot_id=plot_id,
        table=table,
    )
    Log.info(
        "[saved_plot] %s / %r now follows latest code (version %d -> %d)",
        info.variable,
        info.name,
        info.version,
        new_info.version,
        layer=LAYER,
    )
    return new_info
