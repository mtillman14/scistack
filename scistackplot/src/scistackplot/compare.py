"""
Compare to reference: re-express the plotted measure relative to one level of a
grouping layer: sessions 2-4 as a difference from session 1, or as a % change.

The ONE owner of what a comparison means (plan
``.claude/plan-compare-to-reference.md``, doc
``docs/claude/compare-to-reference.md``). This module decides:

* whether a figure can carry one (:func:`unavailable`);
* whether the sample is **paired** (each subject against its own reference)
  or compared **per summary** (each row against the reference mark's centre),
  by the same rule "Show sample" uses to join points into lines
  (``roles.line_recurrence``);
* the baseline (:func:`baseline`) and the formula (:func:`apply`);
* what gets dropped, and the y-axis title (:func:`y_title`).

Every chain that builds sample rows calls this module and never computes a
baseline of its own: the preview (``reduce._build_figure``), the 1-D pandas
reducer, the y limits (``ylimits._reduced_extents``), "Save data"
(``export.plot_data``) and, as emitted pandas, the exported code (``codegen``).
The numpy reducer hands a compared figure to the pandas reference rather than
restating any of it.

The transform runs on the SAMPLE rows, after the pre-collapse (trials averaged
within each subject), before the marks' statistic. The reference level keeps
its marks: paired, every reference row is exactly 0; per summary, the
reference rows keep their own spread around a centre of 0 (user decision D4,
2026-09-27).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import pandas as pd
from scistacklog import Log

from .numeric import coerce_numeric
from .panels import panel_key_text

if TYPE_CHECKING:  # pragma: no cover
    from .spec import PlotSpec, Role
    from .table import LongTable

LAYER = "scistackplot"

#: The joined baseline's column while a frame is being compared. Never
#: survives :func:`apply`.
_REF = "__compare_ref"
#: A constant key used when nothing else separates the baseline (one
#: reference value for the whole frame).
_ALL = "__compare_all"
#: How many units a WARN names before it says "+N more".
_NAMED_UNITS = 10

#: Why a unit was dropped: the keys of ``Outcome.dropped``, shown by the GUI.
NO_REFERENCE = "no reference value"
RAGGED = "a 1-D length that differs from its reference"
NOT_POSITIVE = "% change is undefined for a reference <= 0"


class CompareMode(str, Enum):
    """The formula. The reference level lands at 0 in both."""

    DIFFERENCE = "difference"  # x - ref
    PERCENT = "percent"        # 100 * (x - ref) / ref

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Comparison:
    """``PlotSpec.comparison``: one reference level of one grouping layer.

    ``level`` is text (``panels.panel_key_text``), so ``"01"`` stays ``"01"``
    and the comparison survives filters and reordering. ``active=False`` keeps
    the settings while drawing the raw values, like ``LevelGroup.active``.
    """

    layer: str
    level: str
    mode: CompareMode = CompareMode.DIFFERENCE
    active: bool = True

    def to_dict(self) -> dict:
        return {
            "layer": self.layer,
            "level": self.level,
            "mode": str(self.mode),
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "Comparison":
        return cls(
            layer=str(raw["layer"]),
            level=str(raw["level"]),
            mode=CompareMode(raw.get("mode", CompareMode.DIFFERENCE)),
            active=bool(raw.get("active", True)),
        )


# ---------------------------------------------------------------------------
# Whether a figure can carry one
# ---------------------------------------------------------------------------


def unavailable(
    spec: "PlotSpec", table: "LongTable", roles: dict[str, "Role"], groups: list[str]
) -> str | None:
    """Why ``spec.comparison`` cannot apply to this figure, or None.

    The one statement of what a comparison needs, shared by ``validate``
    (which logs it, and draws the raw values) and the capability report
    (which shows it). The comparison is then INERT, never refused, so moving a
    factor out of the grouping can never make the spec invalid, the same
    policy as ``show_sample``. ``groups``: the grouping layers as completed
    (``complete_assignment(...).groups``).
    """
    comparison = spec.comparison
    if comparison is None:
        return "No comparison is set."
    if not comparison.active:
        return "The comparison is switched off."
    reason = figure_unavailable(spec, table, groups)
    if reason is not None:
        return reason
    if comparison.layer not in groups:
        return (
            f"{comparison.layer} is not a grouping layer (grouping: "
            f"{', '.join(groups) or 'none'}). The reference is a level of a layer "
            f"that separates the marks."
        )
    if not table.has_factor(comparison.layer):
        return f"{comparison.layer} is not a factor of this data."
    levels = [panel_key_text(v) for v in table.factor(comparison.layer).levels]
    if comparison.level not in levels:
        return f"{comparison.layer} has no level {comparison.level!r} in this data."
    return None



def figure_unavailable(spec: "PlotSpec", table: "LongTable", groups: list[str]) -> str | None:
    """Why this FIGURE cannot carry any comparison, whatever its layer and
    level, or None: the part of :func:`unavailable` the capability report
    shows before anything is picked."""
    from .cell import effective_shape
    from .shape import Shape

    if spec.x_measure is not None:
        return "An x-y plot has no reference level to compare with."
    if effective_shape(spec, table) is Shape.MATRIX_2D:
        return "A 2-D measure has no grouping layer to compare along."
    if spec.style.log_y:
        return (
            "A log y axis cannot show a difference or a change, which can be 0 or "
            "negative. Turn off the log axis to compare."
        )
    if not groups:
        return "Nothing is grouped, so there is no level to compare with. Add a grouping layer."
    return None


def comparison_summary(
    spec: "PlotSpec", roles: dict[str, "Role"], table: "LongTable", groups: list[str], shape: Any
) -> dict:
    """The Compare section of the capability report: whether the figure can
    carry a comparison and why not, the layers and levels it may name (every
    grouping layer, levels as text), the default a fresh one opens with (plan
    D9: the innermost tick, else the outermost series layer; its first level),
    and the current one's state (applied, paired and why; or inert and why).
    The panel displays this and derives nothing."""
    from .roles import grouping_layers

    reason = figure_unavailable(spec, table, groups)
    layers = [
        {
            "name": name,
            "levels": [panel_key_text(v) for v in table.factor(name).levels],
        }
        for name in groups
        if table.has_factor(name)
    ]
    default_layer = None
    if reason is None and layers:
        reading = grouping_layers(spec, table, roles, shape=shape)
        candidates = [reading.span] if reading.span else list(reversed(reading.series))
        default_layer = next(
            (name for name in candidates if name in groups), layers[0]["name"]
        )
    default_levels = next(
        (entry["levels"] for entry in layers if entry["name"] == default_layer), []
    )
    comparison = spec.comparison
    state: dict[str, Any] = {"set": comparison is not None}
    if comparison is not None:
        inert = unavailable(spec, table, roles, groups)
        plan = plan_comparison(spec, roles, table) if inert is None else None
        state.update(
            {
                "active": comparison.active,
                "inert": inert if comparison.active else None,
                "paired": plan.paired if plan is not None else None,
                "reason": plan.reason if plan is not None else None,
                "description": plan.describe() if plan is not None else None,
            }
        )
    return {
        "available": reason is None,
        "reason": reason,
        "layers": layers,
        "default_layer": default_layer,
        "default_level": default_levels[0] if default_levels else None,
        "modes": [str(mode) for mode in CompareMode],
        "state": state,
    }

# ---------------------------------------------------------------------------
# The plan: paired or per summary
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ComparisonPlan:
    """A comparison resolved against one spec and table.

    ``paired`` is True when each sample unit has its own reference row (a
    subject has a value at every session); ``reason`` says why, in the words
    ``roles.line_recurrence`` uses for "Show sample" lines. ``factors`` are the
    table's factor names; ``sample`` the sample keys the per-summary baseline
    averages over.
    """

    layer: str
    level: str
    mode: CompareMode
    paired: bool
    reason: str
    factors: tuple[str, ...]
    sample: tuple[str, ...]
    statistic: str = "mean"

    def key_columns(self, frame: pd.DataFrame, index_column: str | None) -> list[str]:
        """What one baseline value is keyed by, among ``frame``'s columns:
        every factor except the reference layer, and (per summary) except the
        sample keys, plus a 1-D measure's position."""
        keys = [
            name
            for name in self.factors
            if name in frame.columns
            and name != self.layer
            and (self.paired or name not in self.sample)
        ]
        if index_column and index_column in frame.columns and index_column not in keys:
            keys.append(index_column)
        return keys

    def describe(self) -> str:
        """``% change from session 1, paired by subject`` — for logs, the
        GUI note and "Save data"'s chain text."""
        what = "difference from" if self.mode is CompareMode.DIFFERENCE else "% change from"
        how = (
            f"paired by {' x '.join(self.sample)}"
            if self.paired and self.sample
            else "paired (each mark is one value)"
            if self.paired
            else f"from the {self.statistic} of {self.layer} {self.level}"
        )
        return f"{what} {self.layer} {self.level}, {how}"


def plan_comparison(
    spec: "PlotSpec", roles: dict[str, "Role"], table: "LongTable"
) -> ComparisonPlan | None:
    """The comparison this figure applies, or None (none set, switched off, or
    inert: :func:`unavailable`, logged at DEBUG).

    Pairing is ``roles.line_recurrence`` over the sample keys across the
    reference layer: True means paired; False or "cannot be told" means per
    summary. Nothing collapsed is trivially paired: each mark is one row.
    """
    if spec.comparison is None or not spec.comparison.active:
        return None
    from .roles import collapse_steps, complete_assignment, line_recurrence

    groups = complete_assignment(spec, table).groups
    reason = unavailable(spec, table, roles, groups)
    if reason is not None:
        Log.debug("compare: inert — %s", reason, layer=LAYER)
        return None
    comparison = spec.comparison
    steps = collapse_steps(spec, roles, table)
    sample = list(steps.sample)
    if not sample:
        paired, why = True, "Nothing is collapsed, so each mark is one value."
    else:
        others = [name for name in groups if name != comparison.layer]
        recurs, why = line_recurrence(table, sample, comparison.layer, others)
        paired = bool(recurs)
    return ComparisonPlan(
        layer=comparison.layer,
        level=comparison.level,
        mode=comparison.mode,
        paired=paired,
        reason=why,
        factors=tuple(table.factor_names),
        sample=tuple(sample),
        statistic=str(spec.aggregate.statistic),
    )


# ---------------------------------------------------------------------------
# Baseline and formula
# ---------------------------------------------------------------------------


@dataclass
class Baseline:
    """The reference values, keyed by :meth:`ComparisonPlan.key_columns` of
    the SAMPLE rows they were built from. A deeper frame (the "Show sample"
    trials) is compared against the same baseline, joined on these keys, so a
    trial is measured against its own subject's reference."""

    keys: list[str]
    #: One row per key combination, the reference value in ``_REF``.
    values: pd.DataFrame
    #: ``{reason: [unit text, ...]}`` for the units the baseline itself
    #: excluded (a % change from a reference <= 0).
    excluded: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class Outcome:
    """What one :func:`apply` did, for the log and the GUI note."""

    rows_in: int = 0
    rows_out: int = 0
    #: ``{reason: [unit text, ...]}``.
    dropped: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "dropped": {reason: list(units) for reason, units in self.dropped.items()},
        }


def _is_reference(frame: pd.DataFrame, plan: ComparisonPlan) -> pd.Series:
    return frame[plan.layer].map(panel_key_text) == plan.level


def _unit_text(row: tuple, keys: list[str]) -> str:
    return ", ".join(f"{k}={panel_key_text(v)}" for k, v in zip(keys, row, strict=True)) or "all"


def baseline(
    sample: pd.DataFrame,
    plan: ComparisonPlan,
    measure: str,
    index_column: str | None = None,
) -> Baseline:
    """The reference values of ``sample`` (the SAMPLE rows).

    Paired: each unit's own reference row (several rows for one key, which
    the chain should never leave, are averaged, with a WARN). Per summary: the
    reference mark's centre (``Aggregation.statistic``) over its sample rows.
    A % change drops a baseline <= 0 (undefined), naming the units.
    """
    keys = plan.key_columns(sample, index_column)
    reference = sample.loc[_is_reference(sample, plan), [*keys, measure]].copy()
    reference[measure] = coerce_numeric(reference[measure])
    reference = reference.dropna(subset=[measure])
    if not keys:
        reference[_ALL] = 0
        keys_used = [_ALL]
    else:
        keys_used = keys
    grouped = reference.groupby(keys_used, dropna=False, sort=False)[measure]
    if plan.paired:
        counts = grouped.size()
        repeated = counts[counts > 1]
        if len(repeated):
            Log.warn(
                "compare: %d unit(s) have several %s %s rows; their mean is the "
                "reference (the sample should hold one row per unit): %s",
                len(repeated),
                plan.layer,
                plan.level,
                _name_units(list(repeated.index), keys_used),
                layer=LAYER,
            )
        values = grouped.mean()
    elif plan.statistic == "median":
        values = grouped.median()
    else:
        values = grouped.mean()
    values = values.rename(_REF).reset_index()
    excluded: dict[str, list[str]] = {}
    if plan.mode is CompareMode.PERCENT:
        bad = ~(values[_REF] > 0)
        if bad.any():
            unit_keys = [k for k in keys_used if k not in (index_column, _ALL)]
            units = sorted(
                {
                    _unit_text(tuple(row), unit_keys)
                    for row in values.loc[bad, unit_keys].itertuples(index=False)
                }
            )
            excluded[NOT_POSITIVE] = units
            values = values.loc[~bad]
    return Baseline(keys=keys_used, values=values.reset_index(drop=True), excluded=excluded)


def apply(
    frame: pd.DataFrame,
    plan: ComparisonPlan,
    base: Baseline,
    measure: str,
    index_column: str | None = None,
    *,
    what: str = "sample",
    quiet: bool = False,
) -> tuple[pd.DataFrame, Outcome]:
    """``frame`` with ``measure`` compared against ``base``.

    A **series** (one row of a scalar measure; every position of one 1-D
    record) is dropped whole when any of its positions has no baseline, or
    when it has a different number of positions from its baseline (user rule:
    mismatched 1-D lengths are refused, never resampled). Drops are WARNs
    naming the units, unless ``quiet`` (the y-limit pass, which mirrors a
    figure that already warned).
    """
    outcome = Outcome(rows_in=len(frame))
    work = frame.copy()
    keys = list(base.keys)
    if keys == [_ALL]:
        work[_ALL] = 0
    work[measure] = coerce_numeric(work[measure])
    work = work.merge(base.values, on=keys, how="left", sort=False)

    series_keys = [
        name for name in plan.factors if name in work.columns and name != index_column
    ]
    unit_keys = [k for k in keys if k not in (index_column, _ALL)]
    missing = work[_REF].isna()
    if index_column and index_column in work.columns:
        # 1-D: a series must have exactly its baseline's positions — no more
        # (a position with no reference) and no fewer (the baseline is longer).
        series_ids = (
            work.groupby(series_keys, dropna=False, sort=False).ngroup()
            if series_keys
            else pd.Series(0, index=work.index)
        )
        rows = series_ids.map(series_ids.value_counts())
        with_ref = (~missing).groupby(series_ids).transform("sum")
        base_positions = _positions_per_unit(work, base, index_column)
        no_reference = with_ref == 0
        ragged = ~no_reference & ((with_ref != rows) | (rows != base_positions))
        _record(outcome, work, no_reference, unit_keys, NO_REFERENCE)
        _record(outcome, work, ragged, unit_keys, RAGGED)
        keep = ~(no_reference | ragged)
    else:
        _record(outcome, work, missing, unit_keys, NO_REFERENCE)
        keep = ~missing
    # A unit the baseline excluded (a % change from a reference <= 0) has no
    # reference row either; it is reported once, under the baseline's reason.
    excluded = {unit for units in base.excluded.values() for unit in units}
    if excluded and NO_REFERENCE in outcome.dropped:
        rest = [unit for unit in outcome.dropped[NO_REFERENCE] if unit not in excluded]
        if rest:
            outcome.dropped[NO_REFERENCE] = rest
        else:
            del outcome.dropped[NO_REFERENCE]
    for reason, units in base.excluded.items():
        outcome.dropped.setdefault(reason, []).extend(units)

    work = work.loc[keep].copy()
    ref = work[_REF].to_numpy(dtype=float)
    values = work[measure].to_numpy(dtype=float)
    if plan.mode is CompareMode.PERCENT:
        work[measure] = 100.0 * (values - ref) / ref
    else:
        work[measure] = values - ref
    work = work.drop(columns=[c for c in (_REF, _ALL) if c in work.columns])
    work = work.reset_index(drop=True)
    outcome.rows_out = len(work)
    _log(plan, outcome, what, quiet)
    return work, outcome


def compare_sample(
    sample: pd.DataFrame,
    plan: ComparisonPlan,
    measure: str,
    index_column: str | None = None,
    *,
    what: str = "sample",
    quiet: bool = False,
) -> tuple[pd.DataFrame, Baseline, Outcome]:
    """:func:`baseline` of ``sample`` then :func:`apply` to it: the call every
    chain makes on its sample rows. The baseline is returned so a deeper
    overlay frame can be compared against the same values."""
    base = baseline(sample, plan, measure, index_column)
    Log.debug(
        "compare: baseline keyed by %s, %d value(s) from %d sample row(s)",
        base.keys,
        len(base.values),
        len(sample),
        layer=LAYER,
    )
    compared, outcome = apply(sample, plan, base, measure, index_column, what=what, quiet=quiet)
    return compared, base, outcome


def _positions_per_unit(
    work: pd.DataFrame, base: Baseline, index_column: str
) -> pd.Series:
    """How many positions each row's baseline has (its unit's reference
    length), aligned to ``work``'s rows."""
    unit_columns = [k for k in base.keys if k != index_column]
    if not unit_columns:
        return pd.Series(len(base.values), index=work.index)
    counts = (
        base.values.groupby(unit_columns, dropna=False, sort=False)
        .size()
        .rename("__compare_n")
        .reset_index()
    )
    joined = work[unit_columns].merge(counts, on=unit_columns, how="left", sort=False)
    return pd.Series(joined["__compare_n"].fillna(0).to_numpy(), index=work.index)


def _record(
    outcome: Outcome, work: pd.DataFrame, mask: pd.Series, unit_keys: list[str], reason: str
) -> None:
    if not mask.any():
        return
    if unit_keys:
        units = sorted(
            {
                _unit_text(tuple(row), unit_keys)
                for row in work.loc[mask.to_numpy(), unit_keys].itertuples(index=False)
            }
        )
    else:
        units = ["all"]
    outcome.dropped.setdefault(reason, []).extend(units)


def _name_units(units: list[Any], keys: list[str]) -> str:
    texts = [
        _unit_text(u if isinstance(u, tuple) else (u,), keys) for u in units[:_NAMED_UNITS]
    ]
    more = f" +{len(units) - _NAMED_UNITS} more" if len(units) > _NAMED_UNITS else ""
    return "; ".join(texts) + more


def _log(plan: ComparisonPlan, outcome: Outcome, what: str, quiet: bool) -> None:
    summary = "compare (%s): %s — %s; %d -> %d row(s)"
    args = (what, plan.describe(), plan.reason, outcome.rows_in, outcome.rows_out)
    if quiet:
        Log.debug(summary, *args, layer=LAYER)
    else:
        Log.info(summary, *args, layer=LAYER)
    for reason, units in outcome.dropped.items():
        named = "; ".join(units[:_NAMED_UNITS])
        more = f" +{len(units) - _NAMED_UNITS} more" if len(units) > _NAMED_UNITS else ""
        message = "compare (%s): dropped %d unit(s), %s: %s%s"
        if quiet:
            Log.debug(message, what, len(units), reason, named, more, layer=LAYER)
        else:
            Log.warn(message, what, len(units), reason, named, more, layer=LAYER)


# ---------------------------------------------------------------------------
# Words
# ---------------------------------------------------------------------------


def y_title(
    plan: ComparisonPlan | None,
    name: str,
    layer_name: str | None = None,
    level_text: str | None = None,
) -> str:
    """The y-axis title for measure ``name`` (already aliased), or ``name``
    itself when no comparison applies (none set, off, or inert). A typed
    ``style.y_label`` is the caller's to prefer. Read by
    ``reduce._labels_for`` AND ``codegen``, so the preview and the export
    title the axis identically.

    ``layer_name`` / ``level_text``: the reference layer and level as they
    READ (aliased); the raw ones when omitted.
    """
    if plan is None:
        return name
    layer = layer_name or plan.layer
    level = level_text or plan.level
    if plan.mode is CompareMode.PERCENT:
        return f"{name}, % change from {layer} {level}"
    return f"Δ {name} (from {layer} {level})"


def comparison_title(plan: ComparisonPlan | None, name: str, text: Any, table: "LongTable") -> str:
    """:func:`y_title` with the reference layer and level as they READ in this
    figure (``aliases.DisplayText``). The one call both the preview
    (``reduce._labels_for``) and the export (``codegen``) make."""
    if plan is None:
        return name
    layer = table.factor(plan.layer) if table.has_factor(plan.layer) else None
    return y_title(
        plan,
        name,
        layer_name=text.name(plan.layer, layer.display if layer is not None else plan.layer),
        level_text=text.level(plan.layer, plan.level),
    )


def comparison_meta(plan: ComparisonPlan, outcome: Outcome | None) -> dict:
    """What Plot Studio's Compare section shows about the figure as drawn
    (``layout.meta.comparison``): the plan in words, paired or not and why,
    and what was dropped."""
    return {
        "layer": plan.layer,
        "level": plan.level,
        "mode": str(plan.mode),
        "paired": plan.paired,
        "reason": plan.reason,
        "description": plan.describe(),
        "outcome": outcome.to_dict() if outcome is not None else None,
    }
