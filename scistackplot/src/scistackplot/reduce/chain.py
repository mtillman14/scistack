"""The collapse chain as reduction steps: which passes a set of keys makes.

``roles.collapse_steps`` says WHICH keys are collapsed (deepest first) and
which is the sample; this module turns a list of keys into the passes that
collapse them (``reduction.CollapseLevels``). One pass per key, nested —
trial within subject, then subject: each subject counts once however many
trials it has — or one pass dropping them all when ``pooled`` ("weight by
N"). A **text measure** always pools — "Collapse means pool" for every text
plot (user, 2026-11-01) — summing its 0/1 indicators for a count and
averaging them for a % (``categorical.pooled_reduction``).

Read by ``resolve._collapse_levels`` (the marks, the "Show sample" overlay,
the y limits, "Save data") and by codegen, which prints the same passes, so
the export collapses exactly as the preview does.
"""

from __future__ import annotations

from typing import Iterable

from ..model.spec import PlotSpec
from ..model.table import LongTable
from .categorical import pooled_reduction
from .reduction import CollapseLevels


def collapse_passes(
    keys: Iterable[str],
    spec: PlotSpec,
    table: LongTable,
    index_column: str | None,
    *,
    pooled: bool = False,
    factors: Iterable[str] | None = None,
    carry: Iterable[str] = (),
) -> list[CollapseLevels]:
    """The passes that collapse ``keys`` (empty when there are none).

    ``factors``: the columns a pass may group on, in order (default: the
    table's factors). ``carry``: columns composed from kept factors that a
    pass must keep (codegen's nested-x column). The measures are the y
    measure and, on an x-y plot, the x measure.
    """
    keys = list(keys)
    if not keys:
        return []
    reduction = pooled_reduction(spec, table)
    how = reduction or "mean"
    groups = [keys] if (pooled or reduction is not None) else [[key] for key in keys]
    measures = tuple(m for m in dict.fromkeys([spec.y_measure, spec.x_measure]) if m)
    names = tuple(factors if factors is not None else table.factor_names)
    return [
        CollapseLevels(
            dropped=tuple(group),
            factors=names,
            measures=measures,
            how=how,
            index_column=index_column,
            carry=tuple(carry),
        )
        for group in groups
    ]
