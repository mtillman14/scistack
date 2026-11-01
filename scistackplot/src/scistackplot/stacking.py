"""Stacked bars: the one owner of when bars stack and where each one starts.

``BarOptions.stack`` piles the COLOUR layer's bars on one tick instead of
giving each colour level its own (colour is otherwise paint on a tick —
``roles.GroupingLayers``). With a text measure's % per category on colour,
that is the 100% stacked bar.

Three readers, one rule each:

* ``roles.grouping_layers`` takes the coloured layer out of the ticks when
  :func:`stacks` (so the colour levels share a position);
* both renderers start each bar at :func:`stack_bases` and draw no error bar;
* ``ylimits`` spans :func:`stacked_bounds` — the pile, not one bar.
"""

from __future__ import annotations

from typing import Any, Sequence

import numpy as np
import pandas as pd

from .spec import BAR_KINDS, PlotKind, PlotSpec


def stacks(spec: PlotSpec, kind: PlotKind | None = None) -> bool:
    """Whether this spec's bars stack: a bar kind with ``bar.stack`` on.
    Without a coloured layer there is nothing to stack, which the callers
    see as one bar per tick — the same picture either way."""
    return (kind or spec.kind) in BAR_KINDS and spec.bar.stack


def _rank(values: pd.Series, order: Sequence[Any] | None) -> pd.Series:
    """Each value's place in ``order`` (as text), unknowns after, in order of
    appearance — the legend's order, so the pile reads like the legend."""
    position = {str(level): index for index, level in enumerate(order or [])}
    fallback = len(position)
    return values.map(lambda v: position.get(str(v), fallback))


def stack_bases(
    frame: pd.DataFrame,
    x: str,
    y: str,
    color: str | None,
    color_order: Sequence[Any] | None = None,
) -> np.ndarray:
    """Where each row's bar starts, aligned with ``frame``'s rows.

    Positive values pile up from zero and negative ones down from it,
    separately, in colour order at each ``x`` — so a pile never hides a bar
    behind another. A missing value is an empty bar (height 0)."""
    if frame.empty:
        return np.zeros(0)
    values = pd.to_numeric(frame[y], errors="coerce").fillna(0.0)
    order = (
        _rank(frame[color], color_order)
        if color and color in frame.columns
        else pd.Series(0, index=frame.index)
    )
    work = pd.DataFrame(
        {"x": frame[x].astype(str).to_numpy(), "rank": order.to_numpy(), "v": values.to_numpy()},
        index=np.arange(len(frame)),
    ).sort_values(["x", "rank"], kind="stable")
    positive = work["v"].clip(lower=0.0)
    negative = work["v"].clip(upper=0.0)
    below_pos = positive.groupby(work["x"], sort=False).cumsum() - positive
    below_neg = negative.groupby(work["x"], sort=False).cumsum() - negative
    work["base"] = np.where(work["v"] >= 0, below_pos, below_neg)
    return work["base"].sort_index().to_numpy(dtype=float)


def stacked_bounds(centre: pd.Series, by: list[str]) -> pd.DataFrame:
    """``(low, high)`` of every pile: ``centre`` indexed by the grouping
    (panel, ticks and colour), summed over the colour within each of ``by``
    (panel + ticks). Negative and positive parts pile separately, as
    :func:`stack_bases` draws them; zero is always in a pile's span."""
    values = centre.fillna(0.0)
    frame = values.rename("v").reset_index()
    frame["pos"] = frame["v"].clip(lower=0.0)
    frame["neg"] = frame["v"].clip(upper=0.0)
    if by:
        piles = frame.groupby(by, dropna=False, sort=False)[["pos", "neg"]].sum().reset_index()
    else:
        piles = frame[["pos", "neg"]].sum().to_frame().T
    return piles.rename(columns={"neg": "low", "pos": "high"})
