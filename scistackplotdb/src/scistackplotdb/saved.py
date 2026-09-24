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

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from scistacklog import Log

LAYER = "scistackplotdb"

TABLE = "_saved_plot"

#: The envelope's layout version. Written on every save and logged on every
#: load; nothing branches on it (salvage, never migrate).
ENVELOPE_FORMAT = 1

#: Longest name accepted. A name is a list label, not a description.
MAX_NAME_LENGTH = 120


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


def ensure_table(db) -> None:
    """Create ``_saved_plot`` if absent. Called by every write."""
    db._duck._execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            plot_id       VARCHAR NOT NULL,
            variable      VARCHAR NOT NULL,
            name          VARCHAR NOT NULL,
            version       INTEGER NOT NULL,
            saved_at      VARCHAR NOT NULL,
            hidden        BOOLEAN NOT NULL DEFAULT FALSE,
            envelope_json VARCHAR NOT NULL,
            PRIMARY KEY (plot_id, version)
        )
    """)


def _table_exists(db) -> bool:
    """Reads never create the table: listing an untouched database must not
    write to it."""
    row = db._duck._fetchone(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [TABLE],
    )
    return bool(row and row[0])


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
    try:
        text = json.dumps(envelope, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise SavedPlotError(f"The plot's settings are not storable as JSON: {exc}") from exc

    ensure_table(db)
    current = _visible_by_name(db, variable, name)
    if current is not None and not overwrite and current.plot_id != current_plot_id:
        Log.info(
            "[saved_plot] save of %s / %r refused: another plot (%s) has that "
            "name and overwrite was not confirmed",
            variable,
            name,
            current.plot_id[:8],
            layer=LAYER,
        )
        raise SavedPlotExists(current)
    plot_id = current.plot_id if current else uuid.uuid4().hex
    saved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # One statement: the next version number is decided inside the insert, so
    # two saves racing on one plot cannot both claim the same version.
    db._duck._execute(
        f"""
        INSERT INTO {TABLE} (plot_id, variable, name, version, saved_at, hidden, envelope_json)
        SELECT ?, ?, ?, COALESCE(MAX(version), 0) + 1, ?, FALSE, ?
        FROM {TABLE} WHERE plot_id = ?
        """,
        [plot_id, variable, name, saved_at, text, plot_id],
    )
    info = _latest_info(db, plot_id)
    Log.info(
        "[saved_plot] saved %s / %r as version %d (%s, %d bytes, envelope format %d)",
        variable,
        name,
        info.version,
        "new plot" if current is None else f"plot {plot_id[:8]}",
        len(text),
        ENVELOPE_FORMAT,
        layer=LAYER,
    )
    return info


def rename_saved_plot(db, plot_id: str, new_name: str) -> SavedPlotInfo:
    """Rename a plot (every version). Refused when another visible plot of
    the same variable already has that name."""
    new_name = check_name(new_name)
    info = _require(db, plot_id)
    clash = _visible_by_name(db, info.variable, new_name)
    if clash is not None and clash.plot_id != plot_id:
        raise SavedPlotError(
            f"{info.variable} already has a saved plot named {new_name!r}."
        )
    db._duck._execute(f"UPDATE {TABLE} SET name = ? WHERE plot_id = ?", [new_name, plot_id])
    Log.info(
        "[saved_plot] renamed %s / %r -> %r (plot %s)",
        info.variable,
        info.name,
        new_name,
        plot_id[:8],
        layer=LAYER,
    )
    return _latest_info(db, plot_id)


def hide_saved_plot(db, plot_id: str, hidden: bool = True) -> SavedPlotInfo:
    """Remove a plot from the list, keeping every row. ``hidden=False``
    brings it back."""
    info = _require(db, plot_id)
    if not hidden:
        clash = _visible_by_name(db, info.variable, info.name)
        if clash is not None and clash.plot_id != plot_id:
            raise SavedPlotError(
                f"{info.variable} already has a saved plot named {info.name!r}; "
                f"rename one of them first."
            )
    db._duck._execute(
        f"UPDATE {TABLE} SET hidden = ? WHERE plot_id = ?", [bool(hidden), plot_id]
    )
    Log.info(
        "[saved_plot] %s %s / %r (plot %s)",
        "hid" if hidden else "unhid",
        info.variable,
        info.name,
        plot_id[:8],
        layer=LAYER,
    )
    return _latest_info(db, plot_id)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def list_saved_plots(db, variable: str, *, include_hidden: bool = False) -> list[SavedPlotInfo]:
    """The newest version of each saved plot of *variable*, newest first."""
    if not _table_exists(db):
        return []
    rows = db._duck._fetchall(
        f"""
        SELECT plot_id, variable, name, version, saved_at, hidden
        FROM {TABLE} AS t
        WHERE variable = ?
          AND version = (SELECT MAX(version) FROM {TABLE} WHERE plot_id = t.plot_id)
          {"" if include_hidden else "AND NOT hidden"}
        ORDER BY saved_at DESC, name
        """,
        [variable],
    )
    plots = [_info(row) for row in rows]
    Log.debug(
        "[saved_plot] %s: %d saved plot(s) listed%s",
        variable,
        len(plots),
        " (hidden included)" if include_hidden else "",
        layer=LAYER,
    )
    return plots


def saved_plot_history(db, plot_id: str) -> list[SavedPlotInfo]:
    """Every version of one plot, newest first."""
    if not _table_exists(db):
        return []
    rows = db._duck._fetchall(
        f"""
        SELECT plot_id, variable, name, version, saved_at, hidden
        FROM {TABLE} WHERE plot_id = ? ORDER BY version DESC
        """,
        [plot_id],
    )
    return [_info(row) for row in rows]


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

    if not _table_exists(db):
        raise SavedPlotError(f"No saved plot {plot_id!r}: nothing has been saved yet.")
    if version is None:
        row = db._duck._fetchone(
            f"""
            SELECT plot_id, variable, name, version, saved_at, hidden, envelope_json
            FROM {TABLE} WHERE plot_id = ? ORDER BY version DESC LIMIT 1
            """,
            [plot_id],
        )
    else:
        row = db._duck._fetchone(
            f"""
            SELECT plot_id, variable, name, version, saved_at, hidden, envelope_json
            FROM {TABLE} WHERE plot_id = ? AND version = ?
            """,
            [plot_id, int(version)],
        )
    if row is None:
        which = f"version {version} of " if version is not None else ""
        raise SavedPlotError(f"No saved plot {which}{plot_id!r}.")
    info = _info(row[:6])
    envelope = _parse_envelope(row[6], info)

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

    The one statement of what a name may be; the GUI asks the backend rather
    than repeating the rule.
    """
    if not isinstance(name, str):
        raise SavedPlotError("A saved plot needs a name.")
    name = " ".join(name.split())  # trim, and no tabs/newlines in a list label
    if not name:
        raise SavedPlotError("A saved plot needs a name.")
    if len(name) > MAX_NAME_LENGTH:
        raise SavedPlotError(
            f"A saved plot's name is at most {MAX_NAME_LENGTH} characters "
            f"(this one is {len(name)})."
        )
    return name


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


def _visible_by_name(db, variable: str, name: str) -> SavedPlotInfo | None:
    """The visible plot of *variable* called *name*, if any."""
    if not _table_exists(db):
        return None
    row = db._duck._fetchone(
        f"""
        SELECT plot_id, variable, name, version, saved_at, hidden
        FROM {TABLE}
        WHERE variable = ? AND name = ? AND NOT hidden
        ORDER BY version DESC LIMIT 1
        """,
        [variable, name],
    )
    return _info(row) if row else None


def _latest_info(db, plot_id: str) -> SavedPlotInfo:
    row = db._duck._fetchone(
        f"""
        SELECT plot_id, variable, name, version, saved_at, hidden
        FROM {TABLE} WHERE plot_id = ? ORDER BY version DESC LIMIT 1
        """,
        [plot_id],
    )
    if row is None:
        raise SavedPlotError(f"No saved plot {plot_id!r}.")
    return _info(row)


def _require(db, plot_id: str) -> SavedPlotInfo:
    if not _table_exists(db):
        raise SavedPlotError(f"No saved plot {plot_id!r}: nothing has been saved yet.")
    return _latest_info(db, plot_id)


def _parse_envelope(text: str, info: SavedPlotInfo) -> dict:
    """The stored envelope as a dict. A row that is not one opens on
    defaults, via ``restore_spec``'s fallback, rather than failing the open."""
    try:
        envelope = json.loads(text)
    except (TypeError, ValueError) as exc:
        Log.warn(
            "[saved_plot] %s / %r version %d: stored envelope is not JSON (%s)",
            info.variable,
            info.name,
            info.version,
            exc,
            layer=LAYER,
        )
        return {}
    if not isinstance(envelope, dict):
        Log.warn(
            "[saved_plot] %s / %r version %d: stored envelope is %s, not a table",
            info.variable,
            info.name,
            info.version,
            type(envelope).__name__,
            layer=LAYER,
        )
        return {}
    return envelope
