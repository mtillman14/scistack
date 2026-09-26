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
:func:`~scistackplot.variants.apply_variant_sets`: the spec decides it, so it is
recomputed wherever the spec is read, and everything downstream sees one
ordinary factor. See ``docs/claude/synthetic-factors.md`` for why that matters
and for the call sites this has to be wired into.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
from scistacklog import Log

from .spec import LevelGroup, PlotSpec
from .table import FactorInfo, LongTable

LAYER = "scistackplot"

#: How far above its source a combined factor sits in the schema hierarchy.
#: Any value in (0, 1) keeps it strictly between the source and the next key
#: up, which is all a depth is ever used for (it is only compared).
COMBINED_DEPTH_OFFSET = 0.5


def active_level_groups(spec: PlotSpec) -> list[LevelGroup]:
    """The combines that apply: named, sourced, active — and only the FIRST
    active one per source.

    The one statement of "which combines are in effect", read by
    :func:`apply_level_groups` and by ``codegen`` (the export must apply
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


def effective_mapping(group: LevelGroup) -> dict[str, str]:
    """``{source level as text: combined level}``, empty labels removed.

    An empty label means "in no bucket" — the editor shows it as (drop) —
    never a bucket literally named ``""``. Read by :func:`_apply_one` and by
    ``codegen``, so the export combines exactly as the preview does.
    """
    return {
        str(key): value
        for key, value in group.mapping.items()
        if value is not None and str(value) != ""
    }


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
    never combines anything pays nothing.
    """
    groups = active_level_groups(spec)
    if not groups:
        return table

    frame = table.frame
    factors = list(table.factors)

    for group in groups:
        if group.source not in frame.columns:
            # A spec outlives the table it was written against — the source
            # factor may have been filtered away or belong to another variable.
            Log.warn(
                "combine %r reads %r, which this table has no column for "
                "— skipped",
                group.name,
                group.source,
                layer=LAYER,
            )
            continue
        if group.name in frame.columns:
            # Writing over an existing column would silently replace real
            # data — the source itself, if the combine is named after it.
            Log.warn(
                "combine %r skipped: %r is already a column of this table — "
                "give the combine another name",
                group.name,
                group.name,
                layer=LAYER,
            )
            continue
        frame, levels = _apply_one(frame, group)
        at = next(i for i, f in enumerate(factors) if f.name == group.source)
        source = factors[at]
        depth = (
            source.depth - COMBINED_DEPTH_OFFSET if source.depth is not None else None
        )
        factors[at] = replace(source, combined_into=group.name)
        factors.append(
            FactorInfo(
                name=group.name,
                levels=levels,
                depth=depth,
                combined_from=group.source,
            )
        )
        Log.info(
            "combine %r replaces %r (depth %s -> %s): %d level(s) -> %s",
            group.name,
            group.source,
            source.depth,
            depth,
            len(frame[group.source].unique()),
            levels,
            layer=LAYER,
        )

    return replace(table, frame=frame, factors=factors)


def _apply_one(frame: pd.DataFrame, group: LevelGroup) -> tuple[pd.DataFrame, list]:
    """Map one source column into a new column, and report its level order."""
    mapping = effective_mapping(group)
    mapped = frame[group.source].astype(str).map(mapping)

    if group.unmatched is None:
        # Drop rows the mapping did not name. "Just these two groups, ignore
        # the rest" is the common intent, and the alternative — keeping them as
        # NaN — makes them a silent extra series in every legend.
        keep = mapped.notna()
        dropped = int((~keep).sum())
        if dropped:
            Log.info(
                "combine %r dropped %d row(s) whose %s was not in the "
                "mapping",
                group.name,
                dropped,
                group.source,
                layer=LAYER,
            )
        frame = frame[keep]
        mapped = mapped[keep]
    else:
        mapped = mapped.fillna(group.unmatched)

    frame = frame.assign(**{group.name: mapped})

    # Declared order: the order the user wrote the buckets in, then the
    # catch-all last. Reading it off the data would reorder a legend whenever a
    # filter happened to remove a group's last row.
    order: list = []
    for value in mapping.values():
        if value not in order:
            order.append(value)
    if group.unmatched is not None and group.unmatched not in order:
        order.append(group.unmatched)
    present = set(frame[group.name].dropna().astype(str))
    return frame, [level for level in order if str(level) in present]
