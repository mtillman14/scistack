"""
The spaghetti kind's one rule that both renderers and codegen must agree on.

A spaghetti plot joins each replicate's markers across the x positions — one
polyline per subject, so within an intervention group you read how every
subject moved between sessions. The markers of different subjects at one
position would sit on top of each other, and random jitter (STRIP's answer) is
not available here: a line has to end ON its marker, and a jitter drawn per
point would put the two in different places.

So the offset is **deterministic and per series**: each series gets one small
horizontal shift it keeps at every x position, which separates the subjects
and keeps each polyline parallel to where it would have been. The same
function decides it for the matplotlib figure, the plotly preview and the
generated seaborn code (which receives the result as a literal, because an
exported endpoint must not depend on this package at run time).
"""

from __future__ import annotations

from typing import Any, Iterable

from .table import natural_sort_key

#: Half-width, in x positions, of the band the series offsets span. The gap
#: between two categorical positions is 1.0; 0.2 either side leaves room for
#: a tick label to still read as belonging to its own cluster.
SPAGHETTI_SPREAD = 0.2


def series_offsets(series_ids: Iterable[Any]) -> dict[str, float]:
    """Horizontal offset per series id, evenly spaced across the spread.

    Natural-sorted so ``"02"`` lands beside ``"01"`` however the rows arrived,
    and symmetric about zero so a cluster stays centred on its tick. One series
    needs no separation and sits exactly on the tick.
    """
    ids = sorted({str(value) for value in series_ids}, key=natural_sort_key)
    if len(ids) <= 1:
        return {value: 0.0 for value in ids}
    step = (2.0 * SPAGHETTI_SPREAD) / (len(ids) - 1)
    return {
        value: round(-SPAGHETTI_SPREAD + index * step, 6)
        for index, value in enumerate(ids)
    }


def overlay_offsets(series_ids: Iterable[Any], n_slots: int = 1) -> dict[str, float]:
    """Offsets for a "Show sample" overlay: :func:`series_offsets` shrunk to
    fit INSIDE the mark the points belong to.

    A bar or box occupies its whole tick (``render.base.MARK_SPAN`` — colour
    is paint, so nothing dodges), and ``n_slots = 1`` is exactly the
    spaghetti band, ±0.2 inside a 0.8-wide mark. On a spaghetti the marks
    are points on lines spread over that same band, so the points of one
    line must stay inside the gap to the next: ``n_slots`` is then the
    number of lines and the spread is divided by it. The same rule applies
    whether or not the points are joined, since a joined point must still
    end on its own marker.
    """
    scale = 1.0 / max(int(n_slots), 1)
    return {
        key: round(value * scale, 6)
        for key, value in series_offsets(series_ids).items()
    }
