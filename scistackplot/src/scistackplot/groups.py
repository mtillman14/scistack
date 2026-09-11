"""
Derived grouping factors: bucketing a factor's levels into a new factor.

``session ∈ {pre, post1, post2}`` becomes ``Phase ∈ {baseline, post}`` — a
factor you can colour or facet by, without editing any data and without the
database having to have recorded it.

This is a **derived table**, the same shape as
:func:`~scistackplot.variants.apply_variant_sets`: the spec decides it, so it is
recomputed wherever the spec is read, and everything downstream sees one
ordinary factor with no idea it was synthesized. See
``docs/claude/synthetic-factors.md`` for why that matters and for the three call
sites this has to be wired into.

The source factor **stays**. Unlike a variant selection — where keeping the
column would state the same thing twice and ``roles.validate`` would refuse the
figure — ``session`` and ``Phase`` are independently useful: sessions along x,
phases in colour. Neither is a variant factor, so no pooling guard is involved.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
from scistacklog import Log

from .spec import LevelGroup, PlotSpec
from .table import FactorInfo, LongTable

LAYER = "scistackplot"


def apply_level_groups(spec: PlotSpec, table: LongTable) -> LongTable:
    """Add one factor per :class:`~scistackplot.spec.LevelGroup` in ``spec``.

    Returns ``table`` untouched when the spec defines none, so a project that
    never buckets anything pays nothing.
    """
    groups = [group for group in spec.level_groups if group.name and group.source]
    if not groups:
        return table

    frame = table.frame
    factors = list(table.factors)

    for group in groups:
        if group.source not in frame.columns:
            # A spec outlives the table it was written against — the source
            # factor may have been filtered away or belong to another variable.
            Log.warn(
                "level group %r reads %r, which this table has no column for "
                "— skipped",
                group.name,
                group.source,
                layer=LAYER,
            )
            continue
        frame, levels = _apply_one(frame, group)
        factors.append(FactorInfo(name=group.name, levels=levels))
        Log.info(
            "level group %r: %d level(s) of %r -> %s",
            group.name,
            len(group.mapping),
            group.source,
            levels,
            layer=LAYER,
        )

    return replace(table, frame=frame, factors=factors)


def _apply_one(frame: pd.DataFrame, group: LevelGroup) -> tuple[pd.DataFrame, list]:
    """Map one source column into a new column, and report its level order."""
    mapping = {str(key): value for key, value in group.mapping.items()}
    mapped = frame[group.source].astype(str).map(mapping)

    if group.unmatched is None:
        # Drop rows the mapping did not name. "Just these two groups, ignore
        # the rest" is the common intent, and the alternative — keeping them as
        # NaN — makes them a silent extra series in every legend.
        keep = mapped.notna()
        dropped = int((~keep).sum())
        if dropped:
            Log.info(
                "level group %r dropped %d row(s) whose %s was not in the "
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
