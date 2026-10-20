"""The rows a statistic sees — one owner for Descriptives and Comparisons.

:func:`analysis_frame` turns ``(StatsSpec, DV, LongTable)`` into an
:class:`AnalysisFrame`. The rows come from ``scistackplot.plot_data``, the
same reduction the figure draws (P1): filters, combines, variants, the cell
collapse and the collapse chain down to the sample. So when a stats spec's
design matches its plot's, a table cell and a bar summarise the same rows.

On top of the rows it says what the stats layer needs to know about them:

- ``factors``: the grouping layers, **innermost first** (the plot's own
  ``PlotSpec.ordered_groups``). They are the model's factors (S2); the
  innermost one is the primary comparison (S13).
- ``strata``: separate figures (ITERATE) and panels (FACET). Each
  combination is analysed on its own.
- ``unit``: the experimental unit, the outermost collapsed key (the plot's
  sample). The ``UNIT`` column identifies a unit by that key *and* every
  schema key above it, so subject "01" at two sites is two units. None when
  nothing is collapsed or the collapse is pooled: then there is no unit, and
  rows are independent observations (the runner warns).
- ``levels``: the level order of every factor, from the data's own order.
  Comparisons read direction from it (a − b, C7).

The rows are read at :data:`ANALYSIS_KIND` (a bar), so the figure's kind
never changes a number. Display settings of the design carrier are reset.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any

import pandas as pd
from scistacklog import Log

from scistackplot import LongTable, PlotKind, PlotSpec, Role, plot_data
from scistackplot.groups import apply_level_groups
from scistackplot.roles import collapse_steps, complete_roles, fanout_keys
from scistackplot.spec import Filter
from scistackplot.variants import apply_variant_sets

from .spec import DV, StatsSpec

LAYER = "scistackstats"

#: The kind the stats layer reads rows at. A bar draws the sample with no
#: extra step (``roles.CollapseSteps``) and turns a 1-D measure into its cell
#: statistic, like every scalar kind.
ANALYSIS_KIND = PlotKind.BAR
#: The kind an SPM analysis reads curves at (no cell collapse; S4).
CURVE_KIND = PlotKind.BAND
#: Column holding the composite experimental-unit id.
UNIT = "__unit__"


class AnalysisFrameError(ValueError):
    """The rows for one DV could not be built; the message says why."""


@dataclass
class AnalysisFrame:
    dv: DV
    #: The value column in ``rows``.
    y: str
    rows: pd.DataFrame
    #: Grouping layers, innermost first.
    factors: list[str]
    strata: list[str]
    #: The sample key the unit is named after (e.g. "subject"), or None.
    unit: str | None
    #: The schema keys composing ``UNIT``, outermost first.
    unit_keys: list[str]
    levels: dict[str, list[Any]]
    #: The analysis spec the rows were read with (for logs and codegen).
    spec: PlotSpec
    notes: list[str] = field(default_factory=list)
    #: Collapsed keys averaged away before the unit, deepest first (the
    #: caption's "trials averaged within subject first").
    averaged: list[str] = field(default_factory=list)
    #: Variant axes among the factors and strata (S3: what a sweep varies).
    variant_axes: list[str] = field(default_factory=list)
    #: Factors that are schema keys nested BELOW the unit (session under
    #: subject). Each unit can have several of their levels by construction,
    #: so they are within-subject; a unit at only one level is missing data,
    #: not a group (Stage E, design.derive_design).
    below_unit: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """What a caption needs to say about these rows."""
        return {
            "dv": self.dv.key, "variable": self.dv.variable, "unit": self.unit,
            "unit_keys": list(self.unit_keys), "averaged": list(self.averaged),
            "factors": list(self.factors), "strata": list(self.strata), "notes": list(self.notes),
            "variant_axes": list(self.variant_axes),
        }

    @property
    def outer_first(self) -> list[str]:
        return list(reversed(self.factors))

    def stratum_groups(self) -> list[tuple[dict[str, Any], pd.DataFrame]]:
        """``(key, rows)`` per stratum, in level order; one ``({}, rows)`` when
        there are no strata."""
        if not self.strata:
            return [({}, self.rows)]
        out = []
        for key, part in self.rows.groupby(self.strata, sort=False, dropna=False):
            key = key if isinstance(key, tuple) else (key,)
            out.append((dict(zip(self.strata, key)), part))
        order = {name: {lv: i for i, lv in enumerate(self.levels.get(name, []))} for name in self.strata}
        out.sort(key=lambda item: [order[n].get(item[0][n], len(order[n])) for n in self.strata])
        return out


def _natural(value: Any):
    return [int(p) if p.isdigit() else p.lower() for p in re.split(r"(\d+)", str(value))]


def _field_factor(table: LongTable) -> str | None:
    names = [f.name for f in table.field_factors if f.name in table.frame.columns]
    return names[0] if names else None


def analysis_spec(design: PlotSpec, dv: DV, table: LongTable, *, sweep: bool = False,
                  curves: bool = False) -> tuple[PlotSpec, list[str]]:
    """The design carrier, pointed at ``dv`` and read at the analysis kind.

    ``sweep`` (S3): every variant of every variant axis is kept, and each axis
    without a role of its own separates the analyses (ITERATE), so each
    variant is its own stratum. An axis the design already groups by keeps
    its role (comparing variants is a design choice).

    A DV with ``columns`` keeps only those fields of a multi-column Variable,
    and its field factor separates them (ITERATE): each column is its own DV
    (S16).
    """
    if design.comparison is not None and design.comparison.active:
        raise AnalysisFrameError(
            f"{dv.key}: the design compares to a reference level; statistics on compared values "
            f"are not designed yet (S9)"
        )
    notes: list[str] = []
    spec = replace(
        design,
        measures=[dv.variable],
        kind=CURVE_KIND if curves else ANALYSIS_KIND,
        custom=None,
        color=None,
        show_sample=[],
        sample_color=None,
        difference_bars=[],
        panel_overrides=[],
    )
    if sweep:
        axes = [f.name for f in table.variant_factors]
        roles = dict(spec.roles)
        for name in axes:
            if roles.get(name) not in (Role.GROUP, Role.FACET):
                roles[name] = Role.ITERATE
        spec = replace(spec, roles=roles, variant_sets=[])
        notes.append(f"variant sweep over {axes}: each variant analysed separately" if axes
                     else "variant sweep: this variable has no variant axes")
    if dv.columns is not None:
        field_name = _field_factor(table)
        if field_name is None:
            raise AnalysisFrameError(
                f"{dv.key}: {dv.variable} has no columns to choose from (it is not a multi-column variable)"
            )
        present = {str(v) for v in table.frame[field_name].dropna().unique()}
        missing = [c for c in dv.columns if c not in present]
        if missing:
            raise AnalysisFrameError(f"{dv.key}: no column(s) {missing} in {dv.variable}")
        role = spec.roles.get(field_name)
        if role not in (None, Role.ITERATE):
            notes.append(f"{field_name} was {role}; each chosen column is its own DV, so it separates them")
        spec = replace(
            spec,
            roles={**spec.roles, field_name: Role.ITERATE},
            groups=[g for g in spec.groups if g != field_name],
            filters=[*spec.filters, Filter(column=field_name, include=list(dv.columns))],
        )
    return spec, notes


def _level_order(name: str, table: LongTable, values: pd.Series) -> list[Any]:
    present = list(pd.unique(values.dropna()))
    if table.has_factor(name):
        known = [lv for lv in table.factor(name).levels if lv in set(present)]
        rest = sorted((lv for lv in present if lv not in set(known)), key=_natural)
        return known + rest
    return sorted(present, key=_natural)


def analysis_frame(stats_spec: StatsSpec, dv: DV, table: LongTable, *, curves: bool = False) -> AnalysisFrame:
    """The rows and their structure for one DV (module docstring).

    ``curves`` (SPM, S4): one row per unit per cell with its whole 1-D curve,
    from ``scistackplot.export.sample_curves`` (the band figure's own
    reduction), instead of the scalar rows ``plot_data`` gives.
    """
    from scistackplot.export import sample_curves
    from scistackplot.shape import Shape

    if curves and dv.variable in table.measure_names and table.shape_of(dv.variable) is not Shape.SERIES_1D:
        raise AnalysisFrameError(f"{dv.key}: SPM needs a 1-D measure; {dv.variable} is {table.shape_of(dv.variable)}")
    spec, notes = analysis_spec(stats_spec.design, dv, table, sweep=stats_spec.variant_mode == "sweep", curves=curves)
    derived = apply_level_groups(spec, apply_variant_sets(spec, table))
    try:
        rows = sample_curves(spec, table) if curves else plot_data(spec, table, fields_as_columns=False)
    except ValueError as exc:
        raise AnalysisFrameError(f"{dv.key}: {exc}") from exc
    roles = complete_roles(spec, derived)
    factors = [n for n in spec.ordered_groups(roles, derived.factor_depths) if n in rows.columns]
    strata = [n for n in fanout_keys(spec, derived) if n in rows.columns]
    strata += [n for n, r in roles.items() if r is Role.FACET and n in rows.columns and n not in strata]
    steps = collapse_steps(spec, roles, derived)
    unit = steps.sample[0] if len(steps.sample) == 1 and not spec.aggregate.pooled else None
    if unit is not None and unit not in rows.columns:
        notes.append(f"the unit {unit!r} is not a column of the rows; treating rows as independent")
        unit = None
    if unit is None:
        reason = "pooled (weight by N)" if spec.aggregate.pooled else "nothing is collapsed"
        notes.append(f"no experimental unit ({reason}): rows are treated as independent observations")
    unit_keys: list[str] = []
    rows = rows.copy()
    below_unit: list[str] = []
    if unit is not None:
        depths = derived.factor_depths
        unit_depth = depths.get(unit)
        unit_keys = [
            name for name in derived.factor_names
            if name in rows.columns and derived.is_schema_key(name) and name != unit
            and unit_depth is not None and depths.get(name) is not None and depths[name] < unit_depth
        ] + [unit]
        rows[UNIT] = rows[unit_keys].astype(str).agg(" / ".join, axis=1)
        below_unit = [
            name for name in factors
            if derived.is_schema_key(name) and unit_depth is not None
            and depths.get(name) is not None and depths[name] > unit_depth
        ]
    levels = {name: _level_order(name, derived, rows[name]) for name in [*factors, *strata]}
    frame = AnalysisFrame(
        dv=dv, y=dv.variable, rows=rows, factors=factors, strata=strata, unit=unit,
        unit_keys=unit_keys, levels=levels, spec=spec, notes=notes,
        averaged=list(steps.pre) if unit is not None else [],
        variant_axes=[f.name for f in derived.variant_factors if f.name in (*factors, *strata)],
        below_unit=below_unit,
    )
    Log.info(
        "[stats rows] %s: %d row(s), factors (inner first) %s, strata %s, unit %s%s",
        dv.key, len(rows), factors, strata, " / ".join(unit_keys) if unit_keys else "none",
        "".join(f"; {n}" for n in notes),
        layer=LAYER,
    )
    return frame


@dataclass
class CovariateValues:
    """One picked covariate's values, keyed by the schema keys it is
    recorded at (``source.covariate_values``), or why it could not load."""

    name: str
    label: str
    keys: list[str] = field(default_factory=list)
    frame: pd.DataFrame | None = None
    error: str | None = None


def load_covariates(source: Any, stats_spec: StatsSpec) -> dict[str, CovariateValues]:
    """``{factor_name: CovariateValues}`` for every analysis' covariates,
    through the source (variant pin, one value per location: the grouping
    loader's own rules, ``covariate_values``). A source without that method
    or a variable it cannot load is an ``error``, never an exception: the
    model check reports it."""
    out: dict[str, CovariateValues] = {}
    for analysis in stats_spec.comparisons:
        for fv in analysis.covariates:
            if fv.factor_name in out:
                continue
            entry = CovariateValues(name=fv.factor_name, label=fv.label)
            loader = getattr(source, "covariate_values", None)
            if loader is None:
                entry.error = "this data source has no variables to join as covariates"
            else:
                try:
                    entry.keys, entry.frame = loader(fv)
                except (ValueError, KeyError) as exc:
                    entry.error = str(exc)
            if entry.error:
                Log.warn("[stats rows] covariate %s not loaded: %s", fv.label, entry.error, layer=LAYER)
            else:
                Log.info("[stats rows] covariate %s: %d value(s) keyed by %s", fv.label, len(entry.frame),
                         entry.keys, layer=LAYER)
            out[fv.factor_name] = entry
    return out


def join_covariates(rows: pd.DataFrame, names: list[str],
                    values: dict[str, CovariateValues]) -> tuple[pd.DataFrame, list[str]]:
    """The rows with each covariate merged on per row, and the problems.

    A covariate must be recorded at or above the rows' level: every key it is
    recorded by is a column of the rows, so each row gets ONE value. One
    recorded deeper (per trial, while the rows are cell means) varies within
    a row and is refused with that reason; averaging it is not offered yet.
    A row the covariate has no value for keeps its place with NaN, and
    csv-stats leaves it out and names it (never silently).
    """
    problems: list[str] = []
    for name in names:
        entry = values.get(name)
        if entry is None or entry.error:
            problems.append(f"covariate {name}: {entry.error if entry else 'not loaded'}")
            continue
        if name in rows.columns:
            problems.append(f"covariate {name}: the rows already have a column {name!r}")
            continue
        missing_keys = [k for k in entry.keys if k not in rows.columns]
        if missing_keys:
            problems.append(
                f"covariate {entry.label} is recorded per {' × '.join(entry.keys)}, but the analysis rows have no "
                f"{', '.join(missing_keys)} column (averaged away): a covariate must be recorded at or above the "
                f"rows' level")
            continue
        right = entry.frame[[*entry.keys, name]].copy()
        right[name] = pd.to_numeric(right[name], errors="coerce")
        if right[name].notna().sum() == 0 and entry.frame[name].notna().any():
            problems.append(f"covariate {entry.label} is not numeric; a covariate is a number (group by it instead)")
            continue
        # Keys compared as text: the rows' keys may be categorical, the
        # source's plain strings or numbers.
        tmp = [f"__cov_key{i}" for i in range(len(entry.keys))]
        left = rows.copy()
        for t, k in zip(tmp, entry.keys):
            left[t] = rows[k].astype(str)
            right[t] = right[k].astype(str)
        joined = left.merge(right[[*tmp, name]], on=tmp, how="left").drop(columns=tmp)
        joined.index = rows.index if len(joined) == len(rows) else joined.index
        if len(joined) != len(rows):
            problems.append(f"covariate {entry.label}: more than one value per {' × '.join(entry.keys)}")
            continue
        missing = int(joined[name].isna().sum())
        if missing:
            Log.warn("[stats rows] covariate %s has no value for %d of %d row(s): the mixed model leaves them out "
                     "and names them", entry.label, missing, len(joined), layer=LAYER)
        rows = joined
    return rows, problems


def table_request(stats_spec: StatsSpec, dv: DV) -> dict[str, Any]:
    """The ``DataSource.get_table`` arguments that load one DV's table."""
    spec = replace(stats_spec.design, measures=[dv.variable])
    return {
        "measures": spec.variant_variables(),
        "x_measure": spec.x_measure,
        "factor_variables": list(spec.factor_variables),
    }


def load_tables(source: Any, stats_spec: StatsSpec) -> dict[str, LongTable]:
    """``{dv.variable: LongTable}`` through any scistackplot ``DataSource``."""
    out: dict[str, LongTable] = {}
    for dv in stats_spec.targets:
        if dv.variable in out:
            continue
        request = table_request(stats_spec, dv)
        out[dv.variable] = source.get_table(
            request["measures"], x_measure=request["x_measure"], factor_variables=request["factor_variables"]
        )
    return out
