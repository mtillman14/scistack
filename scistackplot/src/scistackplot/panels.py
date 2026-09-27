"""
Per-panel overrides: one faceted panel's own y limits and y-axis title — the
ONE owner of which override belongs to which panel.

A :class:`PanelOverride` names its panel by the panel's FACET values only
(``Panel.key``), never by grid position and never by the ITERATE values of
the figure it sits in. So an override on ``ColName=RQUAD`` applies to the
RQUAD panel of every subject's figure, and survives a change of grid size,
filters or variants (plan D1, ``.claude/plan-per-panel-overrides.md``).

Values are stored as TEXT (:func:`panel_key_text`): ``"01"`` stays ``"01"``
and a NaN level is ``"nan"``. That is the same spelling the exported code's
``axes_dict`` lookup uses, so the preview and the export match on one string.

Readers — ``ylimits.limits_for`` (limits), ``render.base.panel_y_title``
(title), codegen and the GUI — ask :func:`override_for`; none of them match
keys themselves. An override whose panel is not drawn (filtered out, level
renamed, factor no longer FACET) is kept in the spec and does nothing
(:func:`unmatched` reports it). See docs/claude/per-panel-overrides.md.

Pure, no pandas or matplotlib import.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable

from scistacklog import Log

if TYPE_CHECKING:  # pragma: no cover
    from .spec import PlotSpec

LAYER = "scistackplot"

#: ``StyleOptions.y_titles``: which panels of a faceted grid draw their y
#: title. ``every_panel`` is the default (a faceted panel's y title is its
#: facet values, i.e. its identity); ``first_column`` keeps it only on panels
#: with nothing directly to their left, the same rule as the tick numbers.
Y_TITLES_EVERY_PANEL = "every_panel"
Y_TITLES_FIRST_COLUMN = "first_column"
Y_TITLES = (Y_TITLES_EVERY_PANEL, Y_TITLES_FIRST_COLUMN)


def panel_key_text(value: Any) -> str:
    """One facet value as text: ``str`` of the level, a missing/NaN level as
    ``"nan"``.

    The one spelling shared by the stored match, the preview and the exported
    code (whose ``axes_dict`` lookup passes each key through ``str``, so a
    NaN level has to be written as ``str(nan)``).
    """
    if value is None:
        return "nan"
    if isinstance(value, float) and math.isnan(value):
        return "nan"
    return str(value)


def y_titles_problem(value: object) -> str | None:
    """Why *value* is not a usable ``StyleOptions.y_titles``, or None."""
    if value in Y_TITLES:
        return None
    return f"must be one of {list(Y_TITLES)}, not {value!r}"


@dataclass(frozen=True)
class PanelOverride:
    """One panel's own settings, over the figure's.

    ``match`` is the panel's facet values as text (:func:`panel_key_text`),
    ``{factor: level}``. Every field left ``None`` inherits:

    - ``y_minimum`` / ``y_maximum`` beat ``PlotSpec.y_axis`` end by end;
      both set means the data is never consulted for this panel.
    - ``y_label`` replaces the facet text drawn as the panel's y title; shown
      exactly as typed (no aliases).
    - ``y_label_hidden``: ``True`` hides the title, ``False`` shows it even
      where ``StyleOptions.y_titles`` would hide it, ``None`` follows the
      grid. Its own flag, never ``y_label == ""``: an emptied text box means
      "inherit", and typed text survives a hide/show.
    """

    match: dict[str, str] = field(default_factory=dict)
    y_minimum: float | None = None
    y_maximum: float | None = None
    y_label: str | None = None
    y_label_hidden: bool | None = None

    @property
    def is_empty(self) -> bool:
        """Whether this override changes nothing (every field inherits)."""
        return (
            self.y_minimum is None
            and self.y_maximum is None
            and self.y_label is None
            and self.y_label_hidden is None
        )

    def matches(self, key: dict[str, Any]) -> bool:
        """Exact match on every facet factor: same factors, same text.

        A partial key never matches — ``{ColName: RQUAD}`` is not the panel
        ``{ColName: RQUAD, Side: L}`` — and an empty ``match`` matches nothing
        (an unfaceted panel takes no override; the figure's own settings
        already cover it).
        """
        if not self.match or set(self.match) != set(key):
            return False
        return all(self.match[name] == panel_key_text(key[name]) for name in key)

    def describe(self) -> str:
        """``ColName=RQUAD`` — for logs and notes."""
        return ", ".join(f"{name}={text}" for name, text in self.match.items())

    def to_dict(self) -> dict:
        return {
            "match": dict(self.match),
            "y_minimum": self.y_minimum,
            "y_maximum": self.y_maximum,
            "y_label": self.y_label,
            "y_label_hidden": self.y_label_hidden,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PanelOverride":
        hidden = raw.get("y_label_hidden")
        return cls(
            match={str(name): str(text) for name, text in (raw.get("match") or {}).items()},
            y_minimum=_as_float(raw.get("y_minimum")),
            y_maximum=_as_float(raw.get("y_maximum")),
            y_label=None if raw.get("y_label") is None else str(raw["y_label"]),
            y_label_hidden=None if hidden is None else bool(hidden),
        )

    @classmethod
    def for_key(cls, key: dict[str, Any], **fields: Any) -> "PanelOverride":
        """An override for the panel with facet values *key* (raw values,
        turned into text here — the only place that does it)."""
        return cls(match={str(n): panel_key_text(v) for n, v in key.items()}, **fields)


def override_for(spec: "PlotSpec", key: dict[str, Any]) -> PanelOverride | None:
    """The override that applies to the panel with facet values *key*, or None.

    The ONLY matcher. Empty overrides are skipped. When several match (a
    hand-edited spec; the GUI upserts), the LAST wins and a WARN says so.
    """
    found = [
        override
        for override in spec.panel_overrides
        if not override.is_empty and override.matches(key)
    ]
    if not found:
        return None
    if len(found) > 1:
        Log.warn(
            "panel overrides: %d entries match panel %s; the last one is used",
            len(found),
            found[-1].describe(),
            layer=LAYER,
        )
    return found[-1]


def overrides_by_key(
    spec: "PlotSpec", factors: list[str]
) -> dict[tuple[str, ...], PanelOverride]:
    """Every non-empty override that can match a panel faceted by *factors*,
    keyed by its values as text in *factors*' order — the table the exported
    code looks panels up in. The same rule as :meth:`PanelOverride.matches`
    (exactly these factors) and :func:`override_for` (the last one wins)."""
    found: dict[tuple[str, ...], PanelOverride] = {}
    for override in spec.panel_overrides:
        if override.is_empty or not factors or set(override.match) != set(factors):
            continue
        found[tuple(override.match[name] for name in factors)] = override
    return found


def unmatched(spec: "PlotSpec", keys: Iterable[dict[str, Any]]) -> list[PanelOverride]:
    """The non-empty overrides that match none of *keys* (the panels drawn):
    kept in the spec, inert. For the log line and the GUI's "not in this
    figure" group."""
    keys = list(keys)
    return [
        override
        for override in spec.panel_overrides
        if not override.is_empty and not any(override.matches(key) for key in keys)
    ]


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)
