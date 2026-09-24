"""
Reading a stored :class:`~scistackplot.spec.PlotSpec` that may predate the
current shape of the spec — salvage, never migrate.

A saved plot outlives the version of this package that wrote it, and the spec
changes shape as the plotting layer grows: settings are renamed, moved into a
nested block, or given new values. :meth:`PlotSpec.from_dict` is strict and
raises on the first unknown key, which is right for a hand-written spec and
wrong for a figure a user saved last month.

So :func:`restore_spec` reads **field by field**. It starts from the spec's
own defaults, keeps every setting that still parses, falls back to the default
for any setting that does not, and records a :class:`RestoreNote` for each
thing it could not keep. There is no per-version upgrade code on purpose
(feedback_beta_no_deprecation). A renamed setting is reported as "no longer a
setting" and takes its default. The caller never rewrites the stored copy on
open, so nothing is lost for a later reader.

The walk is driven by the dataclasses' own type hints rather than a list of
fields kept here. A field added to the spec is restored with no change to this
module, and there is no second statement of the spec's shape to drift.

:func:`reconcile` is the second half: a spec that parses can still name things
today's DATA no longer has (a factor that was renamed, a shown-sample key at a
level the variable moved away from). That depends on a table, so it is a
separate step.

See ``docs/claude/saved-plots.md`` and ``.claude/plan-saved-plots.md``.
"""

from __future__ import annotations

import types
import typing
from dataclasses import MISSING, dataclass, field, fields, is_dataclass, replace
from enum import Enum
from typing import Any

from scistacklog import Log

from .spec import PlotSpec
from .table import LongTable

LAYER = "scistackplot"


class NoteKind(str, Enum):
    """What happened to one stored setting."""

    #: A key the spec no longer has (renamed or removed). Ignored.
    UNKNOWN_SETTING = "unknown_setting"
    #: A known setting whose stored value no longer parses. Default used.
    INVALID_VALUE = "invalid_value"
    #: One entry of a list or table (a filter, a role) that could not be read.
    DROPPED_ENTRY = "dropped_entry"
    #: The plot's primary measure could not be read and was supplied by the caller.
    MEASURE_REPLACED = "measure_replaced"
    #: The setting names something today's data does not have (:func:`reconcile`).
    NOT_IN_DATA = "not_in_data"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class RestoreNote:
    """One setting that did not come back as it was saved.

    ``path`` is dotted from the top of the spec: ``style.width``,
    ``roles.subject``, ``filters[2]``.
    """

    path: str
    kind: NoteKind
    message: str
    #: The stored value, as it was stored.
    value: Any = None

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "kind": str(self.kind),
            "message": self.message,
            "value": self.value,
        }


@dataclass(frozen=True)
class Restored:
    """A usable spec plus everything that did not survive the trip."""

    spec: PlotSpec
    notes: list[RestoreNote] = field(default_factory=list)


class RestoreError(ValueError):
    """The stored spec has no usable measure and the caller supplied none."""


#: Marks a value that could not be read. Never escapes this module.
_INVALID = object()


def restore_spec(raw: Any, *, fallback_measure: str | None = None) -> Restored:
    """Read a stored spec dict, keeping every setting that still parses.

    ``fallback_measure`` is the measure to plot when the stored one is missing
    or unreadable. The saved-plot store knows which variable a plot was saved
    under, so it always supplies one. Without it, an unusable measure raises
    :class:`RestoreError`, because a plot of nothing is not a restore.

    Never raises for anything else: every other problem becomes a
    :class:`RestoreNote` and a default.
    """
    notes: list[RestoreNote] = []
    if not isinstance(raw, dict):
        if fallback_measure is None:
            raise RestoreError(
                f"A stored plot spec must be a table of settings, got "
                f"{type(raw).__name__}."
            )
        notes.append(
            RestoreNote(
                "",
                NoteKind.INVALID_VALUE,
                f"the stored settings are not a table ({type(raw).__name__}); "
                f"opening with every setting at its default",
                _jsonable(raw),
            )
        )
        raw = {}

    raw = dict(raw)
    measures = raw.get("measures")
    if not _usable_measures(measures):
        if fallback_measure is None:
            raise RestoreError(
                f"The stored plot spec names no usable measure ({measures!r})."
            )
        notes.append(
            RestoreNote(
                "measures",
                NoteKind.MEASURE_REPLACED,
                f"the stored measure {measures!r} could not be read; plotting "
                f"{fallback_measure!r}",
                _jsonable(measures),
            )
        )
        raw["measures"] = [fallback_measure]

    spec, reason = _read_dataclass(PlotSpec, raw, "", notes)
    if spec is _INVALID:  # pragma: no cover - measures were made usable above
        raise RestoreError(f"The stored plot spec could not be read: {reason}")

    absent = [
        f.name for f in fields(PlotSpec) if f.init and f.name not in raw
    ]
    Log.debug(
        "[restore] settings absent from the stored spec (defaults used): %s",
        absent or "none",
        layer=LAYER,
    )
    _log_notes("restore", notes)
    Log.info(
        "[restore] %s: %d stored setting(s), %d note(s)",
        spec.y_measure,
        len(raw),
        len(notes),
        layer=LAYER,
    )
    return Restored(spec=spec, notes=notes)


def reconcile(spec: PlotSpec, table: LongTable) -> Restored:
    """Check a restored spec against today's data.

    Reports what the spec names that the data no longer has, and removes it
    only where nothing downstream would tolerate it:

    * **roles** are reported and left in place. ``strip_answered_roles`` drops
      them at every resolve (it owns that rule, through
      :func:`~scistackplot.variants.stale_role_names`), and keeping them means
      the role comes back if the factor does;
    * **show_sample** keys and **sample_color** are removed, because
      ``roles.validate`` refuses a name that is no factor and the figure would
      not draw at all;
    * a missing **measure** is reported. There is nothing to substitute.
    """
    from .groups import apply_level_groups
    from .variants import apply_variant_sets, stale_role_names

    notes: list[RestoreNote] = []
    if spec.y_measure not in table.measure_names:
        notes.append(
            RestoreNote(
                "measures",
                NoteKind.NOT_IN_DATA,
                f"the measure {spec.y_measure!r} is not in today's data",
                spec.y_measure,
            )
        )

    try:
        derived = apply_level_groups(spec, apply_variant_sets(spec, table))
    except Exception as exc:  # the figure will report this itself; don't mask it
        Log.warn(
            "[reconcile] could not derive the variant/level-group table (%s); "
            "checking against the raw table",
            exc,
            layer=LAYER,
        )
        derived = table

    def is_factor(name: str) -> bool:
        return table.has_factor(name) or derived.has_factor(name)

    _answered, missing = stale_role_names(spec, table, derived)
    for name in missing:
        notes.append(
            RestoreNote(
                f"roles.{name}",
                NoteKind.NOT_IN_DATA,
                f"no factor named {name!r} in today's data; its role "
                f"({spec.roles[name]}) does not apply",
                str(spec.roles[name]),
            )
        )

    shown = [name for name in spec.show_sample if is_factor(name)]
    for name in spec.show_sample:
        if name not in shown:
            notes.append(
                RestoreNote(
                    "show_sample",
                    NoteKind.NOT_IN_DATA,
                    f"no factor named {name!r} in today's data; it is no "
                    f"longer shown as a sample",
                    name,
                )
            )
    sample_color = spec.sample_color
    if sample_color is not None and not is_factor(sample_color):
        notes.append(
            RestoreNote(
                "sample_color",
                NoteKind.NOT_IN_DATA,
                f"no factor named {sample_color!r} in today's data; sample "
                f"points take their mark's colour",
                sample_color,
            )
        )
        sample_color = None

    notes.extend(_stale_aliases(spec, table, derived))

    if shown != spec.show_sample or sample_color != spec.sample_color:
        spec = replace(spec, show_sample=shown, sample_color=sample_color)
    _log_notes("reconcile", notes)
    return Restored(spec=spec, notes=notes)


def _stale_aliases(spec: PlotSpec, table: LongTable, derived: LongTable) -> list[RestoreNote]:
    """The plot's own aliases that name nothing in today's data — REPORTED,
    never removed: an alias is inert when its level is absent, and the level
    may come back when a filter or the data changes."""
    columns = {group.label: group.factor_name for group in spec.factor_variables}
    measures = set(table.measure_names)
    notes: list[RestoreNote] = []
    for thing, alias in spec.aliases.items():
        factor = columns.get(thing, thing)
        source = derived if derived.has_factor(factor) else table
        if not source.has_factor(factor):
            if thing not in measures:
                notes.append(
                    RestoreNote(
                        f"aliases.{thing}",
                        NoteKind.NOT_IN_DATA,
                        f"no factor or measure named {thing!r} in today's data; "
                        f"its alias is kept but shows nowhere",
                        thing,
                    )
                )
            continue
        present = {str(level) for level in source.factor(factor).levels}
        absent = [level for level in alias.levels if level not in present]
        if absent:
            notes.append(
                RestoreNote(
                    f"aliases.{thing}.levels",
                    NoteKind.NOT_IN_DATA,
                    f"{len(absent)} aliased level(s) of {thing!r} are not in "
                    f"today's data ({', '.join(repr(a) for a in absent[:5])}); kept",
                    absent,
                )
            )
    return notes


# ---------------------------------------------------------------------------
# The type-driven walk
# ---------------------------------------------------------------------------


def _read_dataclass(
    cls: type, raw: Any, path: str, notes: list[RestoreNote]
) -> tuple[Any, str | None]:
    """An instance of *cls* from *raw*, or ``(_INVALID, reason)``.

    Notes about the fields inside are added to *notes* only when the instance
    is built. An entry that is dropped as a whole reports one note (by its
    caller), not one per field of something the user will not see.
    """
    if not isinstance(raw, dict):
        return _INVALID, f"expected a table of settings, got {type(raw).__name__}"
    hints = _hints(cls)
    known = {f.name: f for f in fields(cls) if f.init}
    local: list[RestoreNote] = []
    kwargs: dict[str, Any] = {}
    for key, value in raw.items():
        sub = _join(path, str(key))
        spec_field = known.get(key)
        if spec_field is None:
            local.append(
                RestoreNote(
                    sub,
                    NoteKind.UNKNOWN_SETTING,
                    f"{key!r} is no longer a setting; ignored",
                    _jsonable(value),
                )
            )
            continue
        parsed, reason = _read_value(hints[key], value, sub, local)
        if parsed is _INVALID:
            if _required(spec_field):
                return _INVALID, f"its required setting {key!r} {reason}"
            local.append(
                RestoreNote(
                    sub,
                    NoteKind.INVALID_VALUE,
                    f"{reason}; using the default ({_default_of(spec_field)!r})",
                    _jsonable(value),
                )
            )
            continue
        kwargs[key] = parsed
    missing = [name for name, f in known.items() if _required(f) and name not in kwargs]
    if missing:
        return _INVALID, f"it has no {', '.join(repr(m) for m in missing)}"
    try:
        built = cls(**kwargs)
    except Exception as exc:
        return _INVALID, f"it could not be built ({exc})"
    # A class with its own wire format (LocationFilter drops malformed pairs,
    # FactorVariable normalises its selection) owns that normalisation. Pass
    # the salvaged object through it so the walk never produces something the
    # class's own reader would have read differently.
    if hasattr(cls, "from_dict") and hasattr(built, "to_dict"):
        try:
            built = cls.from_dict(built.to_dict())
        except Exception as exc:
            Log.debug(
                "[restore] %s: %s.from_dict refused the salvaged value (%s); "
                "kept as salvaged",
                path or "<spec>",
                cls.__name__,
                exc,
                layer=LAYER,
            )
    notes.extend(local)
    return built, None


def _read_value(
    hint: Any, raw: Any, path: str, notes: list[RestoreNote]
) -> tuple[Any, str | None]:
    """*raw* read as *hint*, or ``(_INVALID, reason)``."""
    origin = typing.get_origin(hint)

    if origin in (typing.Union, types.UnionType):
        args = typing.get_args(hint)
        if raw is None and type(None) in args:
            return None, None
        reasons = []
        for arg in args:
            if arg is type(None):
                continue
            parsed, reason = _read_value(arg, raw, path, notes)
            if parsed is not _INVALID:
                return parsed, None
            reasons.append(reason)
        return _INVALID, "; ".join(r for r in reasons if r)

    if origin is list:
        if not isinstance(raw, (list, tuple)):
            return _INVALID, f"expected a list, got {type(raw).__name__}"
        (item_hint,) = typing.get_args(hint) or (Any,)
        items = []
        for i, item in enumerate(raw):
            parsed, reason = _read_value(item_hint, item, f"{path}[{i}]", notes)
            if parsed is _INVALID:
                notes.append(
                    RestoreNote(
                        f"{path}[{i}]",
                        NoteKind.DROPPED_ENTRY,
                        f"entry dropped: {reason}",
                        _jsonable(item),
                    )
                )
                continue
            items.append(parsed)
        return items, None

    if origin is dict:
        if not isinstance(raw, dict):
            return _INVALID, f"expected a table, got {type(raw).__name__}"
        _key_hint, value_hint = typing.get_args(hint) or (str, Any)
        out = {}
        for key, value in raw.items():
            sub = _join(path, str(key))
            parsed, reason = _read_value(value_hint, value, sub, notes)
            if parsed is _INVALID:
                notes.append(
                    RestoreNote(
                        sub,
                        NoteKind.DROPPED_ENTRY,
                        f"entry dropped: {reason}",
                        _jsonable(value),
                    )
                )
                continue
            out[str(key)] = parsed
        return out, None

    if origin is tuple:
        # FactorVariable.variant: a dict on the wire, normalised to a tuple of
        # pairs by the class's own __post_init__. The class decides; the walk
        # only refuses what cannot be a selection at all.
        if isinstance(raw, (dict, list, tuple)):
            return raw, None
        return _INVALID, f"expected a selection, got {type(raw).__name__}"

    if hint is Any:
        return raw, None
    if isinstance(hint, type) and is_dataclass(hint):
        return _read_dataclass(hint, raw, path, notes)
    if isinstance(hint, type) and issubclass(hint, Enum):
        try:
            return hint(raw), None
        except (ValueError, TypeError):
            allowed = ", ".join(repr(str(member.value)) for member in hint)
            return _INVALID, f"{raw!r} is not one of {allowed}"
    if hint is bool:
        if isinstance(raw, bool):
            return raw, None
        return _INVALID, f"expected true/false, got {raw!r}"
    if hint is int:
        if isinstance(raw, int) and not isinstance(raw, bool):
            return raw, None
        if isinstance(raw, float) and raw.is_integer():
            return int(raw), None
        return _INVALID, f"expected a whole number, got {raw!r}"
    if hint is float:
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return float(raw), None
        return _INVALID, f"expected a number, got {raw!r}"
    if hint is str:
        if isinstance(raw, str):
            return raw, None
        # A level typed as a number in a hand-edited file. A number cannot
        # carry a zero pad, so its text form is unambiguous.
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return str(raw), None
        return _INVALID, f"expected text, got {raw!r}"
    # A type this walk has not been taught. Refusing it would silently default
    # a setting that may be fine; taking it as-is lets the class decide.
    Log.debug("[restore] %s: no reader for type %r; kept as stored", path, hint, layer=LAYER)
    return raw, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HINTS: dict[type, dict[str, Any]] = {}


def _hints(cls: type) -> dict[str, Any]:
    if cls not in _HINTS:
        _HINTS[cls] = typing.get_type_hints(cls)
    return _HINTS[cls]


def _required(spec_field) -> bool:
    return spec_field.default is MISSING and spec_field.default_factory is MISSING


def _default_of(spec_field) -> Any:
    if spec_field.default is not MISSING:
        value = spec_field.default
    elif spec_field.default_factory is not MISSING:
        value = spec_field.default_factory()
    else:
        return None
    return str(value) if isinstance(value, Enum) else value


def _usable_measures(measures: Any) -> bool:
    return (
        isinstance(measures, list)
        and bool(measures)
        and all(isinstance(m, str) and m for m in measures)
    )


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _jsonable(value: Any) -> Any:
    """The stored value as it can travel back to the GUI."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return repr(value)


def _log_notes(step: str, notes: list[RestoreNote]) -> None:
    for note in notes:
        Log.warn(
            "[%s] %s (%s): %s", step, note.path or "<spec>", note.kind, note.message,
            layer=LAYER,
        )
