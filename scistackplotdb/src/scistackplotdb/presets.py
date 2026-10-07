"""
Plot presets: named figure settings, without the data, kept in the project
database and applied to any variable.

A saved plot (:mod:`scistackplotdb.saved`) belongs to one variable. A preset
belongs to the project: it is the saved plot's settings with everything that
belongs to the variable taken out (``scistackplot.presets`` decides what that
is, once). Apply it to another variable and the figure is drawn the same way::

    from scistackplotdb import ScidbSource, apply_preset_to, list_presets
    source = ScidbSource(db)
    preset = list_presets(db)[0]
    applied = apply_preset_to(
        db, preset.preset_id, "StepWidth",
        table_for=lambda s: source.get_table(s.variant_variables()),
    )

Storage follows the saved-plot rules exactly, through the same
:class:`~scistackplotdb._versioned.VersionedStore`: one append-only table,
``_plot_preset``; the id is the identity and the name a label (unique across
the project); re-saving a visible name appends a version; "remove" hides,
nothing is ever deleted; save is strict, open salvages, and the stored copy
is never rewritten on open. See ``docs/claude/plot-presets.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from scistacklog import Log

from ._versioned import VersionedStore

LAYER = "scistackplotdb"

TABLE = "_plot_preset"

#: The envelope's layout version. Written on every save and logged on every
#: load; nothing branches on it (salvage, never migrate).
ENVELOPE_FORMAT = 1


class PresetError(ValueError):
    """A save, rename, load or apply that cannot be done as asked."""


class PresetExists(PresetError):
    """``save_preset(overwrite=False)`` found ANOTHER visible preset with that
    name. ``existing`` is that preset, so the caller can ask and retry."""

    def __init__(self, existing: "PresetInfo"):
        super().__init__(
            f"There is already a preset named {existing.name!r} "
            f"(version {existing.version})."
        )
        self.existing = existing


@dataclass(frozen=True)
class PresetInfo:
    """One version of a preset, without its settings: a list row."""

    preset_id: str
    name: str
    version: int
    saved_at: str
    hidden: bool = False
    #: The variable the settings were taken from, and its shape ("scalar",
    #: "1d", ...), for "made on StepLength (scalar)". Display only: nothing
    #: about applying depends on them.
    made_on: str | None = None
    made_on_shape: str | None = None

    def to_dict(self) -> dict:
        return {
            "preset_id": self.preset_id,
            "name": self.name,
            "version": self.version,
            "saved_at": self.saved_at,
            "hidden": self.hidden,
            "made_on": self.made_on,
            "made_on_shape": self.made_on_shape,
        }


@dataclass(frozen=True)
class Preset:
    """A preset, opened: its stored template (a plain dict, as stored)."""

    info: PresetInfo
    template: Any
    stored_format: Any = None


@dataclass(frozen=True)
class AppliedPreset:
    """A preset applied to one variable: the spec to draw, plus what did not
    carry over (``scistackplot.RestoreNote`` s)."""

    info: PresetInfo
    spec: Any  # scistackplot.PlotSpec
    notes: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            **self.info.to_dict(),
            "spec": self.spec.to_dict(),
            "notes": [note.to_dict() for note in self.notes],
        }


STORE = VersionedStore(
    table=TABLE,
    id_column="preset_id",
    scope_column=None,
    noun="preset",
    tag="preset",
    error=PresetError,
)


def ensure_table(db) -> None:
    """Create ``_plot_preset`` if absent. Called by every write."""
    STORE.ensure_table(db)


def _table_exists(db) -> bool:
    return STORE.table_exists(db)


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def save_preset(
    db,
    name: str,
    spec,
    *,
    made_on_shape: str | None = None,
    overwrite: bool = True,
    current_preset_id: str | None = None,
) -> PresetInfo:
    """Save *spec*'s settings, without its data, as the preset *name*.

    *spec* is a ``PlotSpec`` or its dict, read **strictly** (as a saved plot
    is). What is stored is ``scistackplot.make_template(spec)``.
    *made_on_shape* is the variable's raw shape, for the list's display.

    ``overwrite`` / ``current_preset_id`` behave as in
    :func:`scistackplotdb.save_plot`: saving onto ANOTHER preset's name with
    ``overwrite=False`` raises :class:`PresetExists`.
    """
    from scistackplot import PlotSpec, __version__ as plot_version, make_template

    name = STORE.check_name(name)
    if isinstance(spec, dict):
        try:
            spec = PlotSpec.from_dict(spec)
        except Exception as exc:
            raise PresetError(f"The plot's settings could not be read: {exc}") from exc
    if made_on_shape is not None and not isinstance(made_on_shape, str):
        raise PresetError(f"made_on_shape must be text, got {type(made_on_shape).__name__}")

    envelope = {
        "format": ENVELOPE_FORMAT,
        "template": make_template(spec),
        "made_on": {"variable": spec.y_measure, "shape": made_on_shape},
        "saved_with": {"scistackplot": plot_version},
    }
    row, _new = STORE.save(
        db,
        None,
        name,
        envelope,
        overwrite=overwrite,
        current_id=current_preset_id,
        on_clash=lambda clash: PresetExists(_info(clash)),
    )
    return _info(row, envelope)


def rename_preset(db, preset_id: str, new_name: str) -> PresetInfo:
    """Rename a preset (every version). Refused when another visible preset
    already has that name."""
    STORE.rename(db, preset_id, new_name)
    return _newest_info(db, preset_id)


def hide_preset(db, preset_id: str, hidden: bool = True) -> PresetInfo:
    """Remove a preset from the list, keeping every row. ``hidden=False``
    brings it back."""
    STORE.set_hidden(db, preset_id, hidden)
    return _newest_info(db, preset_id)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


def list_presets(db, *, include_hidden: bool = False) -> list[PresetInfo]:
    """The newest version of each preset, newest first."""
    return [
        _info(row[:6], STORE.parse_envelope(row[6], repr(row[2])))
        for row in STORE.list_rows(db, include_hidden=include_hidden, with_envelope=True)
    ]


def preset_history(db, preset_id: str) -> list[PresetInfo]:
    """Every version of one preset, newest first."""
    return [
        _info(row[:6], STORE.parse_envelope(row[6], repr(row[2])))
        for row in STORE.history(db, preset_id, with_envelope=True)
    ]


def load_preset(db, preset_id: str, *, version: int | None = None) -> Preset:
    """The stored template of one preset version (the newest by default),
    exactly as stored. :func:`apply_preset_to` is what reads it."""
    row, text = STORE.load(db, preset_id, version)
    envelope = STORE.parse_envelope(text, f"{row[2]!r} version {row[3]}")
    info = _info(row, envelope)
    return Preset(
        info=info,
        template=envelope.get("template"),
        stored_format=envelope.get("format"),
    )


def apply_preset_to(
    db,
    preset_id: str,
    target,
    *,
    version: int | None = None,
    table=None,
    table_for: Callable[[Any], Any] | None = None,
) -> AppliedPreset:
    """Apply a preset to a variable.

    *target* is the panel's current ``PlotSpec`` for that variable (its data,
    variant pins, title, y label and y limits are kept; every other setting
    is the preset's), or a variable name. A name with a *table* starts from
    ``scistackplot.default_spec(table, name)``, as a fresh panel would; a
    name alone starts from a bare spec with no variant pins.

    *table* / *table_for* check the result against the target's data, as in
    :func:`scistackplot.apply_preset`. Never rewrites the stored preset.
    """
    from scistackplot import PlotSpec, apply_preset, default_spec

    if isinstance(target, str):
        target = default_spec(table, target) if table is not None else PlotSpec(measures=[target])
    preset = load_preset(db, preset_id, version=version)
    result = apply_preset(
        preset.template,
        target,
        table=table,
        table_for=table_for,
        name=preset.info.name,
    )
    Log.info(
        "[preset] %r version %d (envelope format %r, made on %s) applied to %s: "
        "%d note(s)",
        preset.info.name,
        preset.info.version,
        preset.stored_format,
        preset.info.made_on,
        result.spec.y_measure,
        len(result.notes),
        layer=LAYER,
    )
    return AppliedPreset(info=preset.info, spec=result.spec, notes=list(result.notes))


def find_preset(db, name: str) -> PresetInfo | None:
    """The visible preset called *name*, if any. Names are labels; keep the
    ``preset_id`` once found."""
    row = STORE.visible_by_name(db, None, STORE.check_name(name))
    return _newest_info(db, row[0]) if row else None


def check_preset_name(name: Any) -> str:
    """A preset's name, trimmed, or :class:`PresetError` saying why not."""
    return STORE.check_name(name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _newest_info(db, preset_id: str) -> PresetInfo:
    row, text = STORE.load(db, preset_id)
    return _info(row, STORE.parse_envelope(text, f"{row[2]!r} version {row[3]}"))


def _info(row, envelope: dict | None = None) -> PresetInfo:
    preset_id, _scope, name, version, saved_at, hidden = row
    made_on = (envelope or {}).get("made_on")
    if not isinstance(made_on, dict):
        made_on = {}
    variable, shape = made_on.get("variable"), made_on.get("shape")
    return PresetInfo(
        preset_id=str(preset_id),
        name=str(name),
        version=int(version),
        saved_at=str(saved_at),
        hidden=bool(hidden),
        made_on=variable if isinstance(variable, str) else None,
        made_on_shape=shape if isinstance(shape, str) else None,
    )
