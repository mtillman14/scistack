"""Collapsing a 1-D measure to a scalar, so the scalar plot kinds apply to it.

A per-trial vector of step lengths is a scalar question wearing a vector: the
comparison the scientist wants is *across trials*, and the samples inside one
trial are a detail of how it was recorded. So a scalar-only kind selected on a
1-D measure means "reduce each cell to one value first", after which the measure
IS a scalar and nothing downstream needs to know otherwise.

That last clause is the design. This is a **derived table** — the third of the
family, after ``variants.apply_variant_sets`` and ``groups.apply_level_groups``
(``docs/claude/synthetic-factors.md``) — and the only one that rewrites the
MEASURE rather than adding a factor. The rejected alternative was to leave the
data alone and thread an "effective shape" through the fifteen-odd call sites
that ask ``table.shape_of(...)``: ``validate``, ``complete_roles``,
``capabilities``, ``grouping_summary``, ``reduce``, ``ylimits``, four helpers in
``codegen``. Each would then be free to disagree about what it is plotting, and
a disagreement here is not an exception — it is a figure drawn one way and
exported another.

**The kind decides.** There is no collapse toggle, so ``collapse=on`` with
``kind=line`` — a state a checkbox would allow and then have to adjudicate —
cannot be expressed. Only the statistic is a field (``PlotSpec.collapse_statistic``).

**Order matters** and is fixed in ``reduce._build_plan_timed``: variants, level
groups, THIS, then ``validate`` / ``complete_roles`` / ``apply_filters``.
Before ``validate`` so that the rules refusing a factor on X (a 1-D measure's x
axis is its own index) permit one once the measure is genuinely scalar, without
being touched; before the filters so a range filter on the measure filters the
collapsed value, which is the only thing it could mean; before the y-limit pass
so extents are taken over one float per record instead of every sample.

See ``docs/claude/measure-shape-and-collapse.md``.
"""

from __future__ import annotations

import time
from dataclasses import replace

import pandas as pd
from scistacklog import Log

from .series_stats import cell_arrays, collapse_cells
from .shape import Shape
from .spec import SCALAR_KINDS, PlotSpec, Statistic
from .table import LongTable

LAYER = "scistackplot"


def collapses(spec: PlotSpec, table: LongTable) -> bool:
    """Whether ``spec`` asks for this table's y measure to be collapsed.

    True when a scalar kind has been selected for a 1-D measure. A relational
    plot (``x_measure``) is excluded: there the pairing is per sample and the
    kind list is already ``[SCATTER, LINE]``, so collapsing both measures to one
    point per record is a coherent but different feature.
    """
    if spec.x_measure is not None:
        return False
    if spec.kind not in SCALAR_KINDS:
        return False
    if not spec.measures or spec.y_measure not in table.measure_names:
        return False
    return table.shape_of(spec.y_measure) is Shape.SERIES_1D


def effective_shape(spec: PlotSpec, table: LongTable) -> Shape:
    """The shape the FIGURE is drawn from — ``SCALAR`` when collapsing.

    ``table.shape_of`` answers what the data holds; this answers what the plot
    is. They differ only while a collapse is in effect, and the capability
    report deliberately carries both (the kind list is computed from the raw
    shape, or LINE and BAND would vanish the moment a violin was selected).
    """
    if collapses(spec, table):
        return Shape.SCALAR
    return table.shape_of(spec.y_measure) if spec.measures else Shape.UNKNOWN


def apply_collapse(spec: PlotSpec, table: LongTable) -> LongTable:
    """Reduce the y measure's 1-D cells to one value each, or return ``table``.

    Returns the table untouched unless :func:`collapses` — a project that never
    puts a vector on a violin pays nothing.
    """
    if not collapses(spec, table):
        return table

    measure = spec.y_measure
    statistic = spec.collapse_statistic
    started = time.perf_counter()
    frame = table.frame

    if table.measure(measure).exploded:
        collapsed, rows, samples = _collapse_exploded(frame, table, measure, statistic)
    else:
        collapsed, rows, samples = _collapse_cells(frame, measure, statistic)

    empty = int(collapsed[measure].isna().sum()) if len(collapsed) else 0

    # INFO, not DEBUG: this is the line that says a violin is drawn from means
    # rather than from samples, and — with the sample count — the one that tells
    # "the vectors were empty" apart from "the filter removed everything". One
    # line per resolve; the phase timer beside it reports the same elapsed.
    Log.info(
        "collapsed 1-D measure %r to scalar by %s: %d row(s) from %d sample(s) "
        "in %.3fs",
        measure,
        statistic,
        rows,
        samples,
        time.perf_counter() - started,
        layer=LAYER,
    )
    if empty:
        # Kept as NaN rows, never dropped: dropping would change the level
        # counts the factor summary reports, so "this trial recorded nothing"
        # would read as "this trial does not exist".
        Log.warn(
            "%d of %d row(s) of %r collapsed to NaN (empty or all-NaN cells) — "
            "kept as gaps",
            empty,
            rows,
            measure,
            layer=LAYER,
        )

    measures = [
        replace(info, shape=Shape.SCALAR, exploded=False)
        if info.name == measure
        else info
        for info in table.measures
    ]
    return replace(
        table,
        frame=collapsed,
        measures=measures,
        # The within-observation axis belongs to the samples that are now gone.
        # Left in place it would be an ordinary column of a scalar table, i.e. a
        # factor-shaped thing with one level per sample position.
        index_column=None,
    )


def _collapse_cells(
    frame: pd.DataFrame, measure: str, statistic: Statistic
) -> tuple[pd.DataFrame, int, int]:
    """The normal path: one array per row becomes one float per row."""
    arrays = cell_arrays(frame[measure])
    values = collapse_cells(arrays, statistic)
    out = frame.copy()
    out[measure] = values
    samples = int(sum(a.size for a in arrays))
    return out, len(out), samples


def _collapse_exploded(
    frame: pd.DataFrame, table: LongTable, measure: str, statistic: Statistic
) -> tuple[pd.DataFrame, int, int]:
    """The pre-exploded path: one row per SAMPLE becomes one row per record.

    A source may hand over a 1-D measure already exploded, with a real
    within-observation axis of its own (time in seconds, percent of cycle) —
    ``LongTable.index_column``. No shipped source does today, which is exactly
    why this is written and tested rather than left to raise: the failure would
    surface as a violin of every sample of every trial, which looks like a
    plausible figure.
    """
    index_column = table.index_column
    keys = [
        column
        for column in frame.columns
        if column != measure and column != index_column
    ]
    if not keys:
        value = (
            frame[measure].median() if statistic is Statistic.MEDIAN
            else frame[measure].mean()
        )
        out = pd.DataFrame({measure: [value]})
        return out, 1, len(frame)

    grouped = frame.groupby(keys, dropna=False, sort=False)[measure]
    aggregated = (grouped.median() if statistic is Statistic.MEDIAN else grouped.mean())
    out = aggregated.reset_index()
    return out, len(out), len(frame)


def collapse_note(spec: PlotSpec, table: LongTable) -> str | None:
    """One line naming the collapse, for a figure's notes, or None.

    The figure must say that its points are summaries — a violin of trial means
    and a violin of raw samples look identical and mean entirely different
    things.
    """
    if not collapses(spec, table):
        return None
    return (
        f"Each {spec.y_measure} vector collapsed to its "
        f"{spec.collapse_statistic} before plotting."
    )
