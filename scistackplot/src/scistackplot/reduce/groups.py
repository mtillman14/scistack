"""
Combines: a factor derived by combining another factor's levels, which then
**replaces** it.

``stim1..stim4 -> STIM, sham -> SHAM`` — a factor you can group, colour or
facet by, without editing any data and without the database having recorded
it. The combined factor holds its source's *slot*: the role, the grouping
position and the colour the source had. The source stays in the table only so
its rows can be averaged within each combined level — ``roles.complete_assignment``
collapses it — and the panel shows it only as the other choice in the
combine's dropdown (user decision 2026-09-26,
``.claude/plot-studio-combine-section.md``). Keeping the source alongside a
coarser label is "Group by…" on a recorded variable, not a combine.

This is a **derived table**, the same shape as
:func:`~scistackplot.reduce.variants.apply_variant_sets`: the spec decides it, so it is
recomputed wherever the spec is read, and everything downstream sees one
ordinary factor. See ``docs/claude/synthetic-factors.md`` for why that matters
and for the call sites this has to be wired into.
"""

from __future__ import annotations

from dataclasses import replace

from scistacklog import Log

from ..model.spec import LevelGroup, PlotSpec
from ..model.table import FactorInfo, LongTable
from .reduction import CombineLevels, ReductionPlan

LAYER = "scistackplot"

#: How far above its source a combined factor sits in the schema hierarchy.
#: Any value in (0, 1) keeps it strictly between the source and the next key
#: up, which is all a depth is ever used for (it is only compared).
COMBINED_DEPTH_OFFSET = 0.5


def active_level_groups(spec: PlotSpec) -> list[LevelGroup]:
    """The combines that apply: named, sourced, active — and only the FIRST
    active one per source.

    The one statement of "which combines are in effect", read by
    :func:`combine_steps` (so :func:`apply_level_groups` and ``codegen`` apply
    exactly the same ones). Two active combines of one source is refused by
    ``roles.validate``; here the later one is skipped so a panel that has
    not validated yet still draws something.
    """
    chosen: list[LevelGroup] = []
    sources: dict[str, str] = {}
    for group in spec.level_groups:
        if not (group.name and group.source and group.active):
            continue
        if group.source in sources:
            Log.warn(
                "combine %r skipped: %r is already replaced by %r",
                group.name,
                group.source,
                sources[group.source],
                layer=LAYER,
            )
            continue
        sources[group.source] = group.name
        chosen.append(group)
    return chosen


def combine_steps(spec: PlotSpec) -> ReductionPlan:
    """The combines in effect as reduction steps, in spec order: what
    :func:`apply_level_groups` applies and ``codegen`` prints
    (``export.reduction_code.emit_combine``)."""
    return ReductionPlan(tuple(CombineLevels.of(group) for group in active_level_groups(spec)))


def combine_alternatives(spec: PlotSpec) -> dict[str, list[str]]:
    """``{source: [source, combine, ...]}`` for every source with a combine,
    active or not, in declaration order — what that slot's dropdown offers."""
    slots: dict[str, list[str]] = {}
    for group in spec.level_groups:
        if not (group.name and group.source):
            continue
        choices = slots.setdefault(group.source, [group.source])
        if group.name not in choices:
            choices.append(group.name)
    return slots


def apply_level_groups(spec: PlotSpec, table: LongTable) -> LongTable:
    """Add one factor per active combine in ``spec``, replacing its source.

    Returns ``table`` untouched when no combine is active, so a project that
    never combines anything pays nothing. Each combine is a
    :class:`~scistackplot.reduce.reduction.CombineLevels` step: the rule is
    the step's, and the export prints the same step.
    """
    steps = combine_steps(spec).of_type(CombineLevels)
    if not steps:
        return table

    frame = table.frame
    factors = list(table.factors)

    for step in steps:
        reason = step.skip_reason(frame.columns)
        if reason is not None:
            Log.warn("combine %r skipped: %s", step.name, reason, layer=LAYER)
            continue
        frame, dropped = step.apply(frame)
        if dropped:
            # "Just these two groups, ignore the rest" is the common intent;
            # keeping them as NaN would make them a silent extra series.
            Log.info(
                "combine %r dropped %d row(s) whose %s was not in the mapping",
                step.name,
                dropped,
                step.source,
                layer=LAYER,
            )
        # Declared order, not the data's: a filter that removed a group's last
        # row must not reorder a legend.
        present = set(frame[step.name].dropna().astype(str))
        levels = [level for level in step.declared_levels() if str(level) in present]
        at = next(i for i, f in enumerate(factors) if f.name == step.source)
        source = factors[at]
        depth = (
            source.depth - COMBINED_DEPTH_OFFSET if source.depth is not None else None
        )
        factors[at] = replace(source, combined_into=step.name)
        factors.append(
            FactorInfo(
                name=step.name,
                levels=levels,
                depth=depth,
                combined_from=step.source,
            )
        )
        Log.info(
            "combine %r replaces %r (depth %s -> %s): %d level(s) -> %s",
            step.name,
            step.source,
            source.depth,
            depth,
            len(frame[step.source].unique()),
            levels,
            layer=LAYER,
        )

    return replace(table, frame=frame, factors=factors)
