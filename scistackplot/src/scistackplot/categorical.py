"""Text (categorical) measures: the one owner of how text becomes numbers.

A text or boolean measure (``Shape.CATEGORICAL``) has no numeric axis, so
before anything else looks at it this module rewrites it into numbers, and
everything downstream — roles, grouping, facets, compare, "Show sample",
y limits, the renderers, codegen and "Save data" — sees an ordinary scalar
measure. It is the fifth derived table (``docs/claude/synthetic-factors.md``),
and like ``cell.apply_cell_collapse`` it rewrites the MEASURE rather than
adding only a factor. See ``docs/claude/categorical-measures.md``.

**The kind implies the mode** (:func:`mode_for_kind`), so no state can ask for
a count drawn as category positions:

* ``COUNT`` / ``PROPORTION`` (bar, horizontal bar, heatmap, proportion
  spaghetti) — **one-hot**: every record becomes one row per category of its
  field, holding 1 (or 100, for a %) where the record IS that category and 0
  elsewhere, with a synthetic :data:`LEVEL_FACTOR` naming the category. The
  mean of those rows is a proportion, their sum a count.
* ``VALUE`` (strip, spaghetti) — **category positions**: each value becomes
  its index in the measure's level order, and the renderers label the axis
  with the category text (``MeasureInfo.levels``).

**Collapse means pool** for every text plot (user, 2026-11-01). A collapsed
key's records merge into one bucket — never an average of averages — so
:func:`pooled_reduction` tells ``reduce._collapse_levels`` (the one function
every collapse chain runs through) to drop the keys in ONE groupby, summing
for a count and averaging the indicators for a proportion. Which keys are
pooled and which is the sample is ``roles.collapse_steps``'s, which asks
:func:`categorical_mode`.

Order in ``reduce._build_plan_timed``: after the cell collapse (a text
measure is never 1-D) and before ``temporal``, ``validate`` and the filters,
for the reasons the cell collapse gives. A filter on the measure itself is a
filter on RECORDS (the text), so its include / exclude lists are applied here,
before the one-hot, and consumed: after the rewrite the column holds 0/1.
"""

from __future__ import annotations

import time
from dataclasses import replace

import numpy as np
import pandas as pd
from scistacklog import Log

from .shape import Shape
from .spec import CategoricalStat, Filter, PlotKind, PlotSpec, Role
from .table import MISSING_LEVEL, FactorInfo, LongTable, category_levels, category_text

LAYER = "scistackplot"

#: The synthetic factor naming each category of a text measure in COUNT /
#: PROPORTION mode. Displayed as the measure's own name (an axis of
#: "Outcome" categories), so the column name only matters to a spec: the
#: role ``{"Level": "group"}`` puts the categories on x.
LEVEL_FACTOR = "Level"

#: More distinct values than this and a text measure is free text or an
#: identifier, not categories: 400 bars of one record each is not a figure.
#: The same threshold refuses a text column as a GROUPING
#: (``scistackplotdb.source.MAX_GROUP_LEVELS`` is this constant), because it
#: is the same question — "are these categories?".
MAX_CATEGORY_LEVELS = 50

#: A proportion is drawn as a %: each record contributes 100 or 0, so every
#: mean, error band, comparison and y limit is in percent with no rescaling.
PERCENT = 100.0

#: The kinds a text measure is offered (``capability.available_plots``).
#: Summary kinds draw counts or proportions; value kinds draw the categories.
SUMMARY_CATEGORICAL_KINDS = (PlotKind.BAR, PlotKind.HBAR, PlotKind.HEATMAP)
VALUE_CATEGORICAL_KINDS = (PlotKind.STRIP, PlotKind.SPAGHETTI)
CATEGORICAL_KINDS = (*SUMMARY_CATEGORICAL_KINDS, *VALUE_CATEGORICAL_KINDS)

#: Each mode in the words the axis and the log use.
_STAT_WORDS = {
    CategoricalStat.COUNT: "count",
    CategoricalStat.PROPORTION: "%",
    CategoricalStat.VALUE: "categories",
}


# ---------------------------------------------------------------------------
# Which mode
# ---------------------------------------------------------------------------


def mode_for_kind(kind: PlotKind, stat: CategoricalStat) -> CategoricalStat:
    """What a text measure drawn as ``kind`` is drawn as, given the spec's
    ``stat``. THE rule: a strip draws categories; a spaghetti draws the
    categories unless a proportion is asked for (one line per unit through
    its %); every summary kind draws a count or a proportion, a ``VALUE``
    stat reading as a count there."""
    if kind is PlotKind.STRIP:
        return CategoricalStat.VALUE
    if kind is PlotKind.SPAGHETTI:
        return (
            CategoricalStat.PROPORTION
            if stat is CategoricalStat.PROPORTION
            else CategoricalStat.VALUE
        )
    return CategoricalStat.COUNT if stat is CategoricalStat.VALUE else stat


def is_categorical_measure(table: LongTable, measure: str | None) -> bool:
    """Whether ``measure`` is a text measure — raw, or already rewritten."""
    if not measure or measure not in table.measure_names:
        return False
    info = table.measure(measure)
    return info.categorical is not None or info.shape is Shape.CATEGORICAL


def categorical_mode(spec: PlotSpec, table: LongTable) -> CategoricalStat | None:
    """The mode ``spec`` draws its text measure in, or None for a numeric
    measure. Answers the same before and after :func:`apply_categorical`:
    on a rewritten table the measure says what it was rewritten into.

    Read by ``roles.collapse_steps`` (count pools everything, value pools
    into the sample), :func:`pooled_reduction`, the capability report and
    codegen — which never sees a rewritten table, so it has to be answerable
    from the raw one too."""
    if not spec.measures or spec.x_measure is not None:
        return None
    if not is_categorical_measure(table, spec.y_measure):
        return None
    info = table.measure(spec.y_measure)
    if info.categorical is not None:
        return CategoricalStat(info.categorical)
    return mode_for_kind(spec.kind, spec.categorical.stat)


def pooled_reduction(spec: PlotSpec, table: LongTable) -> str | None:
    """How a collapsed key is POOLED for this spec's text measure: ``"sum"``
    (a count), ``"mean"`` (a proportion: the mean of 0/100 indicators), or
    None for a numeric measure, whose chain averages nested as ever.

    ``reduce._collapse_levels`` asks this, so every chain — the marks', the
    "Show sample" overlay's, the y limits' and "Save data"'s — pools the same
    way. ``VALUE`` pools by drawing every record, which is not a reduction:
    its chain never runs a step (``roles.collapse_steps``)."""
    mode = categorical_mode(spec, table)
    if mode is CategoricalStat.COUNT:
        return "sum"
    if mode is CategoricalStat.PROPORTION:
        return "mean"
    return None


# ---------------------------------------------------------------------------
# What a text measure cannot do
# ---------------------------------------------------------------------------


def free_text_reason(table: LongTable, measure: str | None) -> str | None:
    """Why ``measure`` holds too many distinct values to draw as categories,
    or None. Shown in place of the figure, never a silent drop."""
    if not measure or measure not in table.measure_names:
        return None
    info = table.measure(measure)
    if info.shape is not Shape.CATEGORICAL and info.categorical is None:
        return None
    if len(info.levels) <= MAX_CATEGORY_LEVELS:
        return None
    return (
        f"{info.display} holds {len(info.levels)} distinct text values — free "
        f"text or an identifier, not categories (at most {MAX_CATEGORY_LEVELS} "
        f"are drawn as categories). Group by it, or filter it to fewer values."
    )


def measure_filters(spec: PlotSpec) -> tuple[list[Filter], list[Filter]]:
    """``(on the measure, every other)``: the spec's filters split by whether
    they name the plotted text measure. The first are RECORD filters,
    applied to the text before the rewrite; codegen emits them in the same
    place for the same reason."""
    if not spec.measures:
        return [], list(spec.filters)
    own = [f for f in spec.filters if f.column == spec.y_measure]
    return own, [f for f in spec.filters if f.column != spec.y_measure]


def categorical_problem(spec: PlotSpec, table: LongTable) -> str | None:
    """Why ``spec`` cannot draw its text measure, or None. ``roles.validate``
    raises it; the capability report greys the kinds out with it.

    Judged on either table — raw (the capability report's per-kind loop) or
    rewritten (``validate`` runs after :func:`apply_categorical`)."""
    if spec.measures and spec.x_measure is not None and is_categorical_measure(table, spec.y_measure):
        return (
            f"{spec.y_measure} holds text, which has no values to pair with an x "
            f"measure. Clear the X variable, or group by {spec.y_measure} instead."
        )
    mode = categorical_mode(spec, table)
    if mode is None:
        return None
    reason = free_text_reason(table, spec.y_measure)
    if reason is not None:
        return reason
    if spec.kind not in CATEGORICAL_KINDS:
        return (
            f"{spec.y_measure} holds text: it is drawn as a count or % bar, a "
            f"heatmap, a strip of its categories or a spaghetti — not a "
            f"{spec.kind}."
        )
    own, _rest = measure_filters(spec)
    if any(f.minimum is not None or f.maximum is not None for f in own):
        return (
            f"{spec.y_measure} holds text, so it is filtered by choosing its "
            f"categories, not by a numeric range."
        )
    level = _level_name(spec, table)
    # The category factor is no key to pool records by: it is answered in
    # VALUE mode (dropped) and refused below in PROPORTION mode.
    collapsed = [
        name for name, role in spec.roles.items()
        if role is Role.COLLAPSE and name != level and table.has_factor(name)
    ]
    if mode is CategoricalStat.PROPORTION and not collapsed:
        return (
            "A % per unit needs a sample: collapse at least one factor. Its "
            "records are pooled, and the LAST collapsed key is the units the "
            "% is averaged over (one % per subject, say). With nothing "
            "collapsed, draw a count."
        )
    if mode is CategoricalStat.PROPORTION and spec.roles.get(level) is Role.COLLAPSE:
        return (
            f"Collapsing the categories of {spec.y_measure} would make every % "
            f"100. Filter it to the categories you want, or draw a count."
        )
    if mode is CategoricalStat.VALUE and spec.kind is PlotKind.SPAGHETTI and collapsed:
        return (
            f"A spaghetti of {spec.y_measure}'s categories joins ONE category "
            f"per line at each x, and collapsing {', '.join(collapsed)} pools "
            f"several there — text cannot be averaged. Group or separate "
            f"{', '.join(collapsed)} instead, or draw the % of one category "
            f"(Proportion)."
        )
    return None


def _level_name(spec: PlotSpec, table: LongTable) -> str:
    """The name the category factor has (or will have) in this table."""
    if table.has_factor(LEVEL_FACTOR) and table.measure(spec.y_measure).categorical:
        return LEVEL_FACTOR
    name = LEVEL_FACTOR
    while name in table.frame.columns and not table.has_factor(name):
        name += "_"
    return name


def synthetic_factor_names(spec: PlotSpec, table: LongTable) -> list[str]:
    """The factor :func:`apply_categorical` will add to ``table`` for ``spec``
    — ``[LEVEL_FACTOR]`` for a raw text measure, else ``[]``. So a role for
    it is not mistaken for a stale one before the rewrite has run
    (``variants.stale_role_names``)."""
    if not spec.measures or spec.y_measure not in table.measure_names:
        return []
    if table.measure(spec.y_measure).shape is not Shape.CATEGORICAL:
        return []
    return [_level_name(spec, table)]


def _without_level(spec: PlotSpec, name: str) -> PlotSpec:
    """``spec`` with every mention of the category factor ``name`` removed."""
    if name not in spec.roles and name not in spec.groups and spec.color != name:
        return spec
    return replace(
        spec,
        roles={k: v for k, v in spec.roles.items() if k != name},
        groups=[g for g in spec.groups if g != name],
        color=None if spec.color == name else spec.color,
        show_sample=[s for s in spec.show_sample if s != name],
    )


def measure_label(display: str, mode: CategoricalStat, level: str | None) -> str | None:
    """What the y axis of a rewritten text measure says it holds. None keeps
    the measure's own name (category positions are its own values)."""
    if mode is CategoricalStat.COUNT:
        return f"Count of {display} = {level}" if level is not None else "Count"
    if mode is CategoricalStat.PROPORTION:
        return f"% {display} = {level}" if level is not None else "% of records"
    return None


# ---------------------------------------------------------------------------
# The derived table
# ---------------------------------------------------------------------------


def apply_categorical(spec: PlotSpec, table: LongTable) -> tuple[PlotSpec, LongTable]:
    """``(spec, table)`` with the text measure rewritten into numbers, or both
    unchanged for a numeric measure (or a table already rewritten).

    The returned spec has the measure's include / exclude filters consumed
    (applied to the text here); a range filter on it is left for
    ``validate`` to refuse with :func:`categorical_problem`'s reason.
    """
    mode = categorical_mode(spec, table)
    if mode is None:
        return spec, table
    if table.measure(spec.y_measure).categorical is not None:
        # Already rewritten (codegen re-resolves the table it rewrote): that
        # rewrite applied the measure's record filters, so they are consumed
        # here too rather than run again on numbers.
        return _consumed(spec, table, mode), table

    started = time.perf_counter()
    measure = spec.y_measure
    info = table.measure(measure)
    frame = table.frame
    text = category_text(frame[measure])
    levels = list(info.levels) or category_levels(text)
    # Text the table's level list does not name (it was built by a source
    # that ordered a subset): appended in natural order, never dropped.
    extra = [level for level in category_levels(text) if level not in set(levels)]
    levels = [*levels, *extra]

    own, _rest = measure_filters(spec)
    keep = _record_mask(own, text)
    if not bool(keep.all()):
        frame, text = frame[keep], text[keep]
    spec = _consumed(spec, table, mode)

    rows_in = len(frame)
    if mode is CategoricalStat.VALUE:
        out, new_factors = _category_positions(frame, text, measure, levels), list(table.factors)
        detail = f"{len(levels)} categor(ies) as positions 0..{max(len(levels) - 1, 0)}"
    else:
        out, new_factors, detail = _one_hot(spec, table, frame, text, levels, mode)

    measures = [
        replace(
            m,
            shape=Shape.SCALAR,
            categorical=mode,
            levels=levels,
            label=measure_label(m.display, mode, spec.categorical.level) or m.label,
        )
        if m.name == measure
        else m
        for m in table.measures
    ]
    # INFO, once per plan: what the figure's numbers ARE. A count and a % of
    # the same measure look alike on screen; this line says which, how many
    # records went in, and how many rows the one-hot made of them.
    Log.info(
        "categorical %r as %s: %s; %d record(s) -> %d row(s)%s in %.3fs",
        measure,
        _STAT_WORDS[mode],
        detail,
        rows_in,
        len(out),
        f", {len(table.frame) - rows_in} filtered by category" if rows_in != len(table.frame) else "",
        time.perf_counter() - started,
        layer=LAYER,
    )
    return spec, replace(table, frame=out, factors=new_factors, measures=measures)


def _consumed(spec: PlotSpec, table: LongTable, mode: CategoricalStat) -> PlotSpec:
    """``spec`` as the rewritten table answers it: the measure's include /
    exclude filters applied (a range filter is kept for ``validate`` to
    refuse), and — with no category factor in this mode (the categories are
    the y axis, or one category was chosen) — every role for that factor
    dropped as answered, not warned about downstream as stale."""
    own, rest = measure_filters(spec)
    spec = replace(
        spec,
        filters=[
            *rest,
            *[
                replace(f, include=None, exclude=None)
                for f in own
                if f.minimum is not None or f.maximum is not None
            ],
        ],
    )
    if mode is CategoricalStat.VALUE or spec.categorical.level is not None:
        spec = _without_level(spec, _level_name(spec, table))
    return spec


def _record_mask(own: list[Filter], text: pd.Series) -> pd.Series:
    """Which records the filters ON the text measure keep (include / exclude
    by category text). One rule for the preview and the code it emits."""
    keep = pd.Series(True, index=text.index)
    for flt in own:
        if flt.include is not None:
            keep &= text.isin([str(v) for v in flt.include])
        if flt.exclude is not None:
            keep &= ~text.isin([str(v) for v in flt.exclude])
    return keep


def _observed_pairs(
    work: pd.DataFrame,
    fields: list[str],
    name: str,
    level: str | None,
    levels: list[str],
) -> pd.DataFrame:
    """Every (field, category) pair the data holds — the rows each record is
    one-hot over — in field then category order. ``work`` carries the
    category text in `_TEXT`. Shared by the preview's one-hot and the code
    that repeats it, which states these pairs as a literal so every figure
    of an exported fan-out offers the same categories as the preview."""
    observed = work[[*fields, _TEXT]].drop_duplicates().rename(columns={_TEXT: name})
    if level is not None:
        observed = observed[observed[name] == level]
    rank = {lv: i for i, lv in enumerate(levels)}
    return (
        observed.assign(_rank=observed[name].map(rank))
        .sort_values([*fields, "_rank"], kind="stable")
        .drop(columns="_rank")
        .reset_index(drop=True)
    )


def _category_positions(
    frame: pd.DataFrame, text: pd.Series, measure: str, levels: list[str]
) -> pd.DataFrame:
    """VALUE mode: each value's index in ``levels`` (0 = the first category)."""
    codes = pd.Categorical(text, categories=levels).codes.astype("float64")
    codes[codes < 0] = np.nan
    out = frame.copy()
    out[measure] = codes
    return out


def _one_hot(
    spec: PlotSpec,
    table: LongTable,
    frame: pd.DataFrame,
    text: pd.Series,
    levels: list[str],
    mode: CategoricalStat,
) -> tuple[pd.DataFrame, list[FactorInfo], str]:
    """COUNT / PROPORTION: one row per (record, category of its field).

    Per FIELD: a melted wide variable (``Gait (text)`` with ``Outcome`` and
    ``Side``) one-hots each field over its own categories only, so
    ``Outcome``'s records never make zero rows for ``Side``'s categories.
    Within a field every category the field holds anywhere gets a row for
    every record — a subject with no falls has a 0 for "fall", which is what
    makes the sum a count and the mean a proportion."""
    measure = spec.y_measure
    level = spec.categorical.level
    name = _level_name(spec, table)
    fields = [f.name for f in table.factors if f.is_field and f.name in frame.columns]
    work = frame.assign(**{_TEXT: text.to_numpy()})
    observed = _observed_pairs(work, fields, name, level, levels)
    if fields:
        out = work.merge(observed, on=fields, how="inner", sort=False)
    else:
        out = work.merge(observed, how="cross")
    scale = PERCENT if mode is CategoricalStat.PROPORTION else 1.0
    out[measure] = (out[_TEXT] == out[name]).astype("float64") * scale
    out = out.drop(columns=[_TEXT])

    factors = list(table.factors)
    if level is not None:
        # One category: a constant factor is no factor (synthetic-factors.md
        # invariant 6), and the axis title already names the category.
        out = out.drop(columns=[name])
        detail = f"one category ({level!r})"
        if observed.empty:
            Log.warn(
                "categorical %r: category %r is not in the data — nothing to draw "
                "(categories: %s)",
                measure,
                level,
                levels[:12],
                layer=LAYER,
            )
    else:
        present = [lv for lv in levels if lv in set(observed[name])]
        factors.append(
            FactorInfo(
                name=name,
                levels=present,
                label=table.measure(measure).display,
            )
        )
        detail = f"{len(present)} categor(ies) one-hot as factor {name!r}"
    return out.reset_index(drop=True), factors, detail


#: The working column holding each record's category text during the one-hot.
_TEXT = "__category_text"


# ---------------------------------------------------------------------------
# The same rewrite, as code (codegen)
# ---------------------------------------------------------------------------


def categorical_code(spec: PlotSpec, table: LongTable) -> list[str]:
    """The lines a generated ``plot_`` function runs to make the same numbers
    :func:`apply_categorical` makes — emitted by its owner, so the export is
    the preview (``spec`` and ``table`` BEFORE the rewrite). ``[]`` for a
    numeric measure.

    Order, as in ``apply_categorical``: the category text, the filters on the
    measure (records), then the one-hot or the positions. The same two pandas
    calls name the text (`table.category_text`)."""
    mode = categorical_mode(spec, table)
    if mode is None or table.measure(spec.y_measure).categorical is not None:
        return []
    y = spec.y_measure
    info = table.measure(y)
    levels = [str(level) for level in info.levels]
    lines = [
        f"# {y} holds text: drawn as {_STAT_WORDS[mode]} "
        f"(collapsed keys are pooled, never averaged)",
        f"_text = df[{y!r}].astype(object).where(df[{y!r}].notna(), "
        f"{MISSING_LEVEL!r}).astype(str)",
    ]
    own, _rest = measure_filters(spec)
    for flt in own:
        if flt.include is not None:
            lines.append(f"df, _text = df[_text.isin({[str(v) for v in flt.include]!r})], "
                         f"_text[_text.isin({[str(v) for v in flt.include]!r})]")
        if flt.exclude is not None:
            lines.append(f"df, _text = df[~_text.isin({[str(v) for v in flt.exclude]!r})], "
                         f"_text[~_text.isin({[str(v) for v in flt.exclude]!r})]")
    if mode is CategoricalStat.VALUE:
        lines.extend(
            [
                f"_categories = {levels!r}",
                "# each category's position on the y axis (0 = the first)",
                f"df[{y!r}] = pd.Categorical(_text, categories=_categories).codes.astype(float)",
                f"df.loc[df[{y!r}] < 0, {y!r}] = float(\"nan\")",
                "",
            ]
        )
        return lines

    name = _level_name(spec, table)
    fields = [f.name for f in table.factors if f.is_field and f.name in table.frame.columns]
    level = spec.categorical.level
    # The preview's pairs, stated: the figures of a fan-out each see a subset
    # of the records, and must still offer every category the plot holds.
    # The pairs of the records the measure's filters keep, as the preview
    # one-hots after filtering them.
    text = category_text(table.frame[y])
    kept = _record_mask(own, text)
    pairs = _observed_pairs(
        table.frame.loc[kept, fields].assign(**{_TEXT: text[kept].to_numpy()}),
        fields, name, level, levels,
    )
    literal = {column: [str(v) for v in pairs[column]] for column in pairs.columns}
    scale = PERCENT if mode is CategoricalStat.PROPORTION else 1.0
    merge = (
        f"df = df.merge(_observed, on={fields!r}, how=\"inner\", sort=False)"
        if fields
        else 'df = df.merge(_observed, how="cross")'
    )
    lines.extend(
        [
            "# one row per (record, category of its field): 1 where the record "
            "IS that category, else 0"
            + (" — x100, a %" if scale == PERCENT else ""),
            f"df[{_TEXT!r}] = _text.to_numpy()",
            f"_observed = pd.DataFrame({literal!r})",
            merge,
            f"df[{y!r}] = (df[{_TEXT!r}] == df[{name!r}]).astype(float) * {scale!r}",
            f"df = df.drop(columns=[{_TEXT!r}" + (f", {name!r}" if level is not None else "") + "])",
            "",
        ]
    )
    return lines
