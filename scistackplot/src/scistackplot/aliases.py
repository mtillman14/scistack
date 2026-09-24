"""
What every level and name READS AS in a figure — the ONE owner.

Two layers of display aliases, first match wins:

1. the plot's own (:attr:`PlotSpec.aliases`, stored with the saved plot);
2. the project's (``[aliases]`` in the project config; ``scidb.aliases`` owns
   that grammar and a table carries a live reader of it,
   :meth:`LongTable.project_aliases`);

and then the raw text. A plot entry of ``""`` means "show the raw text here
even though the project aliases it".

Applied ONCE, when a figure is built (``reduce._build_figure``), into a
:class:`DisplayText` that rides on the :class:`ResolvedPlot`. From there on a
renderer asks it for text and never stringifies a level itself: the old
``str(level)`` at each drawing site was one owner of "how a level reads" per
site. Identity never changes — panel keys, colour levels, composed x keys,
offsets and y limits all stay raw; only the text drawn for them is aliased.
That is also why aliasing costs no re-reduce: ``aliases`` is a
plan-irrelevant field, and a table's project reader is read per resolve.

One rule makes "rename the data" and "relabel the display" the same thing,
which the generated code relies on: within a factor, the text is
ONE-TO-ONE over the levels present (:func:`check_distinct`). Two levels that
would read the same are refused, naming where each alias came from.

See docs/claude/plot-text-and-labels.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Iterable

from scistacklog import Log

from .roles import RoleError
from .spec import Alias, PlotSpec
from .table import LongTable

LAYER = "scistackplot"

#: Where an alias came from — in logs and in the clash message.
PLOT = "plot"
PROJECT = "project"
RAW = "raw"

#: How dash ids are composed (``reduce.SERIES_SEPARATOR``). Restated rather
#: than imported: ``reduce`` imports this module.
_DASH_SEPARATOR = " | "


class AliasError(RoleError):
    """Two levels of one factor would read the same. A RoleError, so every
    caller that already reports an invalid spec reports this too."""


@dataclass(frozen=True)
class DisplayText:
    """The merged aliases of one figure, and how each thing reads.

    Empty (the default) reads everything raw — which is what a
    ``ResolvedPlot`` built without a spec's aliases, in a test say, gets.
    """

    #: ``{thing: (text, origin)}`` — a thing's own display name.
    names: dict[str, tuple[str, str]] = field(default_factory=dict)
    #: ``{thing: {level text: (text, origin)}}``.
    levels: dict[str, dict[str, tuple[str, str]]] = field(default_factory=dict)
    #: ``{factor: lookup keys}`` for factors whose alias is keyed differently
    #: from their column: a grouping column is the factor ``Sex`` but the
    #: entry ``Demographics.Sex`` (``FactorVariable.label``), tried first.
    keys: dict[str, tuple[str, ...]] = field(default_factory=dict)
    #: The figure's roles, so a renderer asks "the colour level's text"
    #: without knowing which factor is the colour (set per figure).
    x_layers: tuple[str, ...] = ()
    color: str | None = None
    sample_color: str | None = None
    #: The uncoloured series layers, OUTERMOST first — the order dash ids are
    #: composed in (``reduce._series_key``).
    dash_layers: tuple[str, ...] = ()

    # ---- lookups -------------------------------------------------------

    def _keys(self, thing: str) -> tuple[str, ...]:
        return self.keys.get(thing, (thing,))

    def _level_entry(self, factor: str | None, value: Any) -> "tuple[str, str] | None":
        if factor is None:
            return None
        text = _raw(value)
        for key in self._keys(factor):
            found = self.levels.get(key, {}).get(text)
            if found is not None:
                return found
        return None

    def level(self, factor: str | None, value: Any) -> str:
        """What ``value`` of ``factor`` reads as."""
        found = self._level_entry(factor, value)
        return found[0] if found is not None else _raw(value)

    def level_origin(self, factor: str | None, value: Any) -> str:
        found = self._level_entry(factor, value)
        return found[1] if found is not None else RAW

    def name(self, thing: str | None, default: str = "") -> str:
        """What ``thing`` (a factor or a measure) is called, else ``default``."""
        if thing is None:
            return default
        for key in self._keys(thing):
            if key in self.names:
                return self.names[key][0]
        return default

    def name_origin(self, thing: str | None) -> str:
        if thing is None:
            return RAW
        for key in self._keys(thing):
            if key in self.names:
                return self.names[key][1]
        return RAW

    def entry_key(self, thing: str) -> str:
        """The key an alias for ``thing`` is written under — a grouping
        column's qualified ``Variable.Column``, else the thing itself — so
        the GUI writes where the lookup reads first."""
        return self._keys(thing)[0]

    # ---- the figure's roles --------------------------------------------

    def color_level(self, value: Any) -> str:
        return self.level(self.color, value)

    def sample_level(self, value: Any) -> str:
        return self.level(self.sample_color, value)

    def x_tick(self, value: Any) -> str:
        """A single-layer categorical axis' tick (a nested axis' ticks come
        from :meth:`x_plan`)."""
        return self.level(self.x_layers[-1] if self.x_layers else None, value)

    def dash_id(self, sid: Any) -> str:
        """A composed dash id, part by part. Raw when it does not split into
        one part per layer (a level containing the separator)."""
        text = _raw(sid)
        parts = text.split(_DASH_SEPARATOR)
        if not self.dash_layers or len(parts) != len(self.dash_layers):
            return text
        return _DASH_SEPARATOR.join(
            self.level(layer, part) for layer, part in zip(self.dash_layers, parts)
        )

    def panel_title(self, key: dict[str, Any]) -> str:
        """A panel's facet values, the way ``Panel.title`` joins them."""
        return " · ".join(self.level(factor, value) for factor, value in key.items())

    def figure_title(self, key: dict[str, Any]) -> str:
        """An ITERATE figure's ``name=value`` title."""
        return ", ".join(
            f"{self.name(factor, factor)}={self.level(factor, value)}"
            for factor, value in key.items()
        )

    def x_plan(self, plan):
        """A nested axis' plan with its TEXT aliased: ticks by the innermost
        layer, each bracket row by its own (``XGroup.depth`` indexes
        ``x_layers``, outermost first). ``order`` — the composed keys the
        marks are placed by — is untouched. A copy: the plan is not mutated."""
        if plan is None or not self.x_layers or not self.levels:
            return plan
        leaf = self.x_layers[-1]
        ticks = [self.level(leaf, label) if label else label for label in plan.tick_labels]
        groups = [
            replace(group, label=self.level(self._layer(group.depth), group.label))
            for group in plan.groups
        ]
        return replace(plan, tick_labels=ticks, groups=groups)

    def _layer(self, depth: int) -> str | None:
        return self.x_layers[depth] if 0 <= depth < len(self.x_layers) else None

    def for_figure(
        self,
        *,
        x_layers: Iterable[str] = (),
        color: str | None = None,
        sample_color: str | None = None,
        dash_layers: Iterable[str] = (),
    ) -> "DisplayText":
        """This text, told which factor plays which role in one figure."""
        return replace(
            self,
            x_layers=tuple(x_layers),
            color=color,
            sample_color=sample_color,
            dash_layers=tuple(dash_layers),
        )


def _raw(value: Any) -> str:
    return "" if value is None else str(value)


# ---------------------------------------------------------------------------
# Building it
# ---------------------------------------------------------------------------


def display_text(spec: PlotSpec, table: LongTable) -> DisplayText:
    """The plot's aliases over the project's, for ``table``'s factors.

    Reads the project layer NOW (``table.project_aliases()``), so an edit to
    the project config reaches the next resolve with nothing rebuilt.
    """
    names, levels = merge(table.project_aliases(), spec.aliases)
    return DisplayText(names=names, levels=levels, keys=_lookup_keys(spec))


def merge(
    project: dict[str, Alias], plot: dict[str, Alias]
) -> tuple[dict[str, tuple[str, str]], dict[str, dict[str, tuple[str, str]]]]:
    """Plot over project, field by field, with each entry's origin.

    A plot ``name`` or level of ``""`` REMOVES the project's (show raw); a plot
    field left out keeps it.
    """
    names: dict[str, tuple[str, str]] = {}
    levels: dict[str, dict[str, tuple[str, str]]] = {}
    for thing in dict.fromkeys([*project, *plot]):
        mine = plot.get(thing)
        theirs = project.get(thing)
        if mine is not None and mine.name is not None:
            if mine.name:
                names[thing] = (mine.name, PLOT)
        elif theirs is not None and theirs.name:
            names[thing] = (theirs.name, PROJECT)

        merged = {
            level: (text, PROJECT)
            for level, text in (theirs.levels if theirs else {}).items()
            if text
        }
        for level, text in (mine.levels if mine else {}).items():
            if text:
                merged[level] = (text, PLOT)
            else:
                merged.pop(level, None)
        if merged:
            levels[thing] = merged
    return names, levels


def _lookup_keys(spec: PlotSpec) -> dict[str, tuple[str, ...]]:
    """A grouping column's entries: its qualified label first, then its bare
    factor name (``Demographics.Sex``, then ``Sex``)."""
    keys: dict[str, tuple[str, ...]] = {}
    for grouping in spec.factor_variables:
        factor = grouping.factor_name
        qualified = grouping.label
        keys[factor] = (qualified, factor) if qualified != factor else (factor,)
    return keys


def check_distinct(text: DisplayText, table: LongTable, factors: Iterable[str | None]) -> None:
    """Refuse a figure in which two levels of one drawn factor read the same.

    Over the factor's levels in the TABLE, so an alias for a level that is not
    in the data never clashes, and an alias that equals another level's raw
    text does. Raises :class:`AliasError` naming every clash and its origins.
    """
    problems: list[str] = []
    for factor in dict.fromkeys(f for f in factors if f):
        if not table.has_factor(factor):
            continue
        seen: dict[str, Any] = {}
        for value in table.factor(factor).levels:
            shown = text.level(factor, value)
            if shown in seen and _raw(seen[shown]) != _raw(value):
                other = seen[shown]
                problems.append(
                    f"{factor}: {_raw(other)!r} ({text.level_origin(factor, other)}) and "
                    f"{_raw(value)!r} ({text.level_origin(factor, value)}) would both "
                    f"read {shown!r}"
                )
            else:
                seen.setdefault(shown, value)
    if problems:
        raise AliasError(
            "Two levels would read the same, so the figure could not tell them "
            "apart — " + "; ".join(problems) + ". Give one of each pair a "
            "different alias (the plot's Aliases, or [aliases.<name>.levels] in "
            "scistack.toml)."
        )


def log_summary(text: DisplayText, table: LongTable) -> None:
    """What the aliases did to this table's factors, once per resolve.

    INFO names every factor with an alias entry — how many of its levels
    read differently, from which layer — and every alias naming no level in
    the data (usually a text mismatch: ``1`` vs ``"01"``). DEBUG: the map.
    """
    if not text.levels and not text.names:
        return
    parts: list[str] = []
    unused: list[str] = []
    for info in table.factors:
        keys = text._keys(info.name)
        entries: dict[str, tuple[str, str]] = {}
        for key in reversed(keys):  # the first key wins, as in lookup
            entries.update(text.levels.get(key, {}))
        if not entries:
            continue
        present = {_raw(value) for value in info.levels}
        hits = [level for level in entries if level in present]
        by_origin = {
            origin: sum(1 for level in hits if entries[level][1] == origin)
            for origin in (PROJECT, PLOT)
        }
        parts.append(
            f"{info.name} {len(hits)}/{len(present)} "
            f"(project {by_origin[PROJECT]}, plot {by_origin[PLOT]})"
        )
        unused.extend(f"{info.name}={level!r}" for level in entries if level not in present)
    named = [f"{thing} ({origin})" for thing, (_, origin) in text.names.items()]
    Log.info(
        "aliases: levels %s; names %s%s",
        ", ".join(parts) or "none",
        ", ".join(named) or "none",
        (
            f"; {len(unused)} alias(es) name no level in the data ({', '.join(unused[:5])})"
            if unused
            else ""
        ),
        layer=LAYER,
    )
    Log.debug("aliases: names=%s levels=%s", text.names, text.levels, layer=LAYER)


#: Levels listed per factor for the GUI. Past this the list says it is cut;
#: 500 subjects are aliased in the project config, not typed into a panel.
MAX_LABELABLE_LEVELS = 200


def labelable(
    text: DisplayText,
    table: LongTable,
    *,
    measure: str,
    factors: Iterable[tuple[str | None, str]],
) -> list[dict]:
    """What the GUI's Labels section offers for one figure — Python's list,
    so the panel never works out which factors a figure draws.

    ``factors`` is ``(factor, role)`` in the figure's order; repeats and
    factors the table does not have are dropped. Each entry carries the key an
    alias is written under (``DisplayText.entry_key``), the thing's name and
    every level of it as ``{raw, text, origin}`` (origin ``plot`` /
    ``project`` / ``raw``), and ``truncated`` past
    :data:`MAX_LABELABLE_LEVELS`. The measure comes first, name only.
    """
    measure_default = table.measure(measure).display if measure in table.measure_names else measure
    entries = [
        {
            "factor": measure,
            "key": text.entry_key(measure),
            "role": "measure",
            "name": {
                "raw": measure_default,
                "text": text.name(measure, measure_default),
                "origin": text.name_origin(measure),
            },
            "levels": [],
            "truncated": False,
        }
    ]
    seen: set[str] = set()
    for factor, role in factors:
        if not factor or factor in seen or not table.has_factor(factor):
            continue
        seen.add(factor)
        info = table.factor(factor)
        values = list(info.levels)
        entries.append(
            {
                "factor": factor,
                "key": text.entry_key(factor),
                "role": role,
                "name": {
                    "raw": info.display,
                    "text": text.name(factor, info.display),
                    "origin": text.name_origin(factor),
                },
                "levels": [
                    {
                        "raw": _raw(value),
                        "text": text.level(factor, value),
                        "origin": text.level_origin(factor, value),
                    }
                    for value in values[:MAX_LABELABLE_LEVELS]
                ],
                "truncated": len(values) > MAX_LABELABLE_LEVELS,
            }
        )
    return entries


def name_text(spec: PlotSpec, table: LongTable, thing: str, default: str) -> str:
    """One name, merged afresh — for a caller outside a built figure
    (``reduce.x_axis_title`` as ``codegen`` calls it)."""
    return display_text(spec, table).name(thing, default)
