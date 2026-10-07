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

    def to_dict(self) -> dict:
        return {
            **self.info.to_dict(),
            "spec": self.spec.to_dict(),
            "view": dict(self.view),
            "notes": [note.to_dict() for note in self.notes],
            "stored_format": self.stored_format,
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
) -> SavedPlotInfo:
    """Save *spec* under *name* for *variable*, returning the new version.

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

    envelope = {
        "format": ENVELOPE_FORMAT,
        "spec": spec.to_dict(),
        "view": view,
        "saved_with": {"scistackplot": plot_version},
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
        info=info, spec=spec, view=view, notes=notes, stored_format=stored_format
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
