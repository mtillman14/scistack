"""How big is this frame, really — in cells AND in samples.

A row count is the wrong unit for a table whose cells hold signals. 4190 rows of
a 200-sample array and 4190 rows of a 250,000-sample array are the same row
count and four orders of magnitude apart in work, and every load/reduce log line
in this package reported only rows. On 2026-09-13 that made a plot that never
returned indistinguishable, from the log alone, from a plot over a small table
(.claude/plot-at-scale-plan.md §1).

So one helper, used by every layer that reports frame size, so `load_variable`
in ``scistackplotdb`` and the reduce path here cannot describe the same frame in
different units.

**Cost.** ``len()`` on a list or ndarray is O(1), so measuring is one pass over
the cells of the measure columns — cheap enough to run unconditionally on a load
that is already touching every one of those cells. It deliberately does NOT sum
``nbytes`` or walk into the values: the point is to size the work, not to audit
memory exactly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    import pandas as pd

#: Rough bytes per element for a Python ``list[float]`` cell: the float object
#: (24 on CPython 64-bit) plus the list's pointer slot (8). Used only to turn a
#: sample count into an order-of-magnitude memory figure in a log line, which is
#: what distinguishes "big" from "hopeless" at a glance.
PYLIST_BYTES_PER_SAMPLE = 32

#: Bytes per element once a cell is a float64 ndarray. The gap between this and
#: :data:`PYLIST_BYTES_PER_SAMPLE` is the prize for fetching via Arrow.
NUMPY_BYTES_PER_SAMPLE = 8


def cell_samples(value: Any) -> int:
    """Number of samples in one cell: its length, or 1 for a scalar.

    ``None`` and strings count as 0 — neither is a sample, and a string's length
    would be a character count masquerading as one.
    """
    if value is None or isinstance(value, (str, bytes)):
        return 0
    try:
        return len(value)
    except TypeError:
        return 1  # a scalar is one sample


def frame_extent(frame: "pd.DataFrame", columns: list[str] | None = None) -> dict:
    """``{rows, cells, samples, est_bytes, boxed}`` for ``frame``.

    ``columns`` limits the scan to the measure columns worth measuring; omitted,
    every column is scanned, which is only appropriate for a small frame.

    ``boxed`` is True when the first sampled cell is a Python ``list``/``tuple``
    rather than an ndarray — i.e. the payload is carried as boxed Python floats.
    That single flag is what says whether an Arrow fetch path would help, so it
    is worth a word in the log even though it is only a sample.
    """
    rows = len(frame)
    names = [c for c in (columns or list(frame.columns)) if c in frame.columns]
    cells = 0
    samples = 0
    boxed = False
    seen_a_cell = False
    for name in names:
        series = frame[name]
        for value in series.to_numpy():
            if value is None:
                continue
            n = cell_samples(value)
            if n == 0:
                continue
            cells += 1
            samples += n
            if not seen_a_cell:
                boxed = isinstance(value, (list, tuple))
                seen_a_cell = True
    per_sample = PYLIST_BYTES_PER_SAMPLE if boxed else NUMPY_BYTES_PER_SAMPLE
    return {
        "rows": rows,
        "cells": cells,
        "samples": samples,
        "est_bytes": samples * per_sample,
        "boxed": boxed,
    }


def format_extent(extent: dict) -> str:
    """One log-line fragment: ``rows=419 cells=4190 samples=1.1e+09 ~34.0GB boxed``.

    Samples go in scientific notation because the interesting range spans six
    orders of magnitude and the exact digits never matter.
    """
    gb = extent["est_bytes"] / 1024**3
    size = f"~{gb:.1f}GB" if gb >= 0.1 else f"~{extent['est_bytes'] / 1024**2:.0f}MB"
    return (
        f"rows={extent['rows']} cells={extent['cells']} "
        f"samples={extent['samples']:.3g} {size}"
        + (" boxed" if extent["boxed"] else " ndarray")
    )
