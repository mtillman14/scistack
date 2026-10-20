"""The statistical design of one stratum, derived from its rows (S2, P4, S13).

Factor kinds are **derived, with evidence, and overridable**:

- **within**: every unit appears at two or more of the factor's levels;
- **between**: every unit sits at exactly one level;
- **mixed_membership**: some units at one level, some at several. For a
  schema key nested BELOW the unit (session under subject) the schema
  decides: it is within, and the units at one level are missing data
  (``basis="schema"``, an ``incomplete_within`` warning; Stage E,
  2026-10-19). Any other factor stays mixed and the tests refuse it until the
  analysis overrides the kind (P4); ``catalog.needed_action`` turns that into
  the GUI's decision banner. Incomplete units are never dropped silently:
  see ``Analysis.missing`` and ``exclusions``.

With no experimental unit (nothing collapsed, or pooled), every factor is
between, and the design carries a ``no_unit`` warning: the rows are treated as
independent, which may be pseudoreplication.

**Tiers** (S13): the *primary* comparisons are the pairs of the innermost
grouping layer's levels, inside each combination of the outer layers (simple
effects). The *secondary* results are the omnibus model's main effects and
interactions. Primary comparisons are always run, whatever the interaction
shows.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from scistacklog import Log

from .tables import UNIT, AnalysisFrame

LAYER = "scistackstats"

WITHIN, BETWEEN, MIXED = "within", "between", "mixed_membership"


@dataclass
class Factor:
    name: str
    levels: list[Any]
    kind: str
    derived: str
    overridden: bool = False
    #: Units seen at several levels / at exactly one level.
    units_multi: int = 0
    units_single: int = 0
    #: What decided ``derived``: "data" (the rows) or "schema" (a key nested
    #: below the unit, Stage E).
    basis: str = "data"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "levels": [str(v) for v in self.levels], "kind": self.kind,
            "derived": self.derived, "overridden": self.overridden,
            "units_multi": self.units_multi, "units_single": self.units_single, "basis": self.basis,
        }


@dataclass
class Design:
    dv: str
    y: str
    stratum: dict[str, Any]
    #: Innermost first.
    factors: list[Factor]
    unit: str | None
    n_units: int | None
    n_rows: int
    warnings: list[dict[str, str]] = field(default_factory=list)
    #: Units missing some within-cell combination (:func:`incomplete_units`).
    incomplete_units: list[str] = field(default_factory=list)
    #: Combinations of the factors' levels with no rows (:func:`empty_cells`).
    empty_cells: list[dict[str, str]] = field(default_factory=list)

    @property
    def between(self) -> list[str]:
        return [f.name for f in self.factors if f.kind == BETWEEN]

    @property
    def within(self) -> list[str]:
        return [f.name for f in self.factors if f.kind == WITHIN]

    @property
    def mixed(self) -> list[str]:
        return [f.name for f in self.factors if f.kind == MIXED]

    def factor(self, name: str) -> Factor:
        return next(f for f in self.factors if f.name == name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dv": self.dv, "stratum": {k: str(v) for k, v in self.stratum.items()},
            "factors": [f.to_dict() for f in self.factors], "unit": self.unit,
            "n_units": self.n_units, "n_rows": self.n_rows, "warnings": list(self.warnings),
            "incomplete_units": list(self.incomplete_units),
            "empty_cells": [dict(c) for c in self.empty_cells],
        }


def _classify(rows: pd.DataFrame, name: str) -> tuple[str, int, int]:
    per_unit = rows.groupby(UNIT)[name].nunique()
    multi = int((per_unit > 1).sum())
    single = int((per_unit == 1).sum())
    if multi == 0:
        return BETWEEN, multi, single
    if single == 0:
        return WITHIN, multi, single
    return MIXED, multi, single


def derive_design(
    frame: AnalysisFrame,
    stratum: dict[str, Any],
    rows: pd.DataFrame,
    overrides: dict[str, str] | None = None,
) -> Design:
    """The design of one stratum's rows (module docstring)."""
    overrides = dict(overrides or {})
    factors: list[Factor] = []
    warnings: list[dict[str, str]] = []
    for name in frame.factors:
        levels = [lv for lv in frame.levels[name] if lv in set(rows[name])]
        if frame.unit is None:
            derived, multi, single = BETWEEN, 0, 0
        else:
            derived, multi, single = _classify(rows, name)
        basis = "data"
        if derived == MIXED and name in getattr(frame, "below_unit", ()):
            # Stage E (2026-10-19): the rows can't tell "missing data" from
            # "a group", but the schema can. A key nested below the unit
            # (session under subject) is repeated within each unit by
            # construction, so a unit at one level is missing data.
            derived, basis = WITHIN, "schema"
            warnings.append({
                "code": "incomplete_within",
                "message": f"{name} is nested below {frame.unit} in the schema, so it is within-subject; "
                           f"{single} {frame.unit}(s) have only one {name} (missing data, not a group)",
            })
        kind = overrides.get(name, derived)
        factors.append(Factor(name, levels, kind, derived, name in overrides, multi, single, basis))
        Log.debug(
            "[design] %s %s: %s from the %s (units at several levels %d, at one %d)%s",
            frame.dv.key, name, derived, basis, multi, single,
            f" -> overridden to {kind}" if name in overrides else "", layer=LAYER,
        )
        if derived == MIXED and name not in overrides:
            warnings.append({
                "code": "mixed_membership",
                "message": f"{name}: {multi} unit(s) at several levels and {single} at one; "
                           f"set it to within or between",
            })
    if frame.unit is None:
        warnings.append({"code": "no_unit", "message": "no experimental unit: rows are treated as independent"})
    for stale in sorted(set(overrides) - {f.name for f in factors}):
        warnings.append({"code": "stale_override", "message": f"override for {stale!r} names no grouping layer"})
    design = Design(
        dv=frame.dv.key, y=frame.y, stratum=dict(stratum), factors=factors, unit=frame.unit,
        n_units=int(rows[UNIT].nunique()) if frame.unit is not None else None,
        n_rows=int(len(rows)), warnings=warnings,
    )
    design.incomplete_units = incomplete_units(design, rows)
    design.empty_cells = empty_cells(design, rows)
    Log.info(
        "[design] %s %s: within=%s, between=%s, mixed=%s, unit=%s, n=%s, incomplete=%d, empty cells=%d",
        frame.dv.key, stratum or "(one stratum)", design.within, design.between, design.mixed,
        frame.unit, design.n_units if design.n_units is not None else design.n_rows,
        len(design.incomplete_units), len(design.empty_cells), layer=LAYER,
    )
    if design.empty_cells:
        Log.info("[design] %s %s: no rows for %s", frame.dv.key, stratum or "(one stratum)",
                 cells_text(design.empty_cells), layer=LAYER)
    return design


def incomplete_units(design: Design, rows: pd.DataFrame) -> list[str]:
    """Units missing at least one combination of the within factors' levels,
    in unit order. The one owner of "who is incomplete": the recommendation
    (a mixed model when the policy is use_all), the complete-cases filter
    and the GUI banner all read it."""
    within = [f for f in design.factors if f.kind == WITHIN]
    if design.unit is None or not within or UNIT not in rows.columns:
        return []
    n_cells = 1
    for f in within:
        n_cells *= len(f.levels)
    cells = rows.groupby(UNIT)[[f.name for f in within]].apply(lambda g: len(g.drop_duplicates()))
    return [str(u) for u, n in cells.items() if n < n_cells]


def empty_cells(design: Design, rows: pd.DataFrame) -> list[dict[str, str]]:
    """The combinations of the factors' levels that have no rows, outer
    factor first in each dict. The one owner of "which cells are empty".

    With two or more factors, a model with every interaction (the default
    omnibus) has a column per combination, so an empty one makes it rank
    deficient and csv-stats refuses it (scidb.log 2026-10-20: one session
    with no Traditional subjects, "rank deficient (54 of 60 columns)").
    One factor can't have an empty cell: its levels come from the rows.
    """
    if len(design.factors) < 2:
        return []
    return [c["cell"] for c in cell_counts(design, rows) if c["rows"] == 0]


def cell_counts(design: Design, rows: pd.DataFrame) -> list[dict[str, Any]]:
    """Every combination of the factors' levels with its row and unit
    counts, outer factor first: ``{"cell": {factor: level}, "rows": n,
    "units": n}`` (``units`` is None with no unit). The one owner of "how
    many are in each cell", read by :func:`empty_cells` and the wizard's
    "Is anything missing?" grid."""
    if not design.factors:
        return []
    outer_first = design.factors[::-1]
    names = [f.name for f in outer_first]
    keyed = rows[names].astype(str)
    row_n = keyed.value_counts().to_dict()
    unit_n: dict[tuple, int] = {}
    if design.unit is not None and UNIT in rows.columns:
        unit_n = keyed.assign(_u=rows[UNIT].values).groupby(names)["_u"].nunique().to_dict()
    out = []
    for combo in itertools.product(*([str(v) for v in f.levels] for f in outer_first)):
        key = combo if len(combo) > 1 else combo[0]
        out.append({
            "cell": dict(zip(names, combo)),
            "rows": int(row_n.get(combo, 0)),
            "units": int(unit_n.get(key, 0)) if design.unit is not None else None,
        })
    return out


def cells_text(cells: list[dict[str, str]], limit: int = 6) -> str:
    """``session=MO12 / group=Traditional, …`` for logs and evidence."""
    shown = [" / ".join(f"{k}={v}" for k, v in c.items()) for c in cells[:limit]]
    more = f" (+{len(cells) - limit} more)" if len(cells) > limit else ""
    return ", ".join(shown) + more


@dataclass
class Tiers:
    #: The innermost grouping layer, or None with no layers.
    primary: str | None
    #: One ``{outer factor: level}`` per combination of the outer layers that
    #: has rows; ``[{}]`` when the primary is the only layer.
    contexts: list[dict[str, Any]]
    #: Level pairs ``(a, b)`` of the primary, in level order.
    pairs: list[tuple[Any, Any]]
    #: The omnibus terms: every main effect and interaction, outer first.
    secondary: list[str]


def comparison_tiers(design: Design, rows: pd.DataFrame) -> Tiers:
    if not design.factors:
        return Tiers(primary=None, contexts=[], pairs=[], secondary=[])
    primary = design.factors[0]
    outer = design.factors[1:][::-1]
    contexts: list[dict[str, Any]] = []
    if outer:
        for combo in itertools.product(*(f.levels for f in outer)):
            context = dict(zip([f.name for f in outer], combo))
            mask = pd.Series(True, index=rows.index)
            for name, level in context.items():
                mask &= rows[name] == level
            if mask.any():
                contexts.append(context)
    else:
        contexts = [{}]
    pairs = list(itertools.combinations(primary.levels, 2))
    names = [f.name for f in design.factors][::-1]
    secondary = [":".join(c) for r in range(1, len(names) + 1) for c in itertools.combinations(names, r)]
    return Tiers(primary=primary.name, contexts=contexts, pairs=pairs, secondary=secondary)
