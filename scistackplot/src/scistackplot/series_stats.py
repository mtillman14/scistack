"""Per-position statistics over 1-D cells, in numpy, without a long frame.

The reductions a plot needs from a 1-D measure are all "over positions": the
min/max of every sample, one row per sample for a line, the mean across
replicates at each position, mean ± SD across replicates at each position. The
reference path (``reduce._explode_1d`` + pandas groupby) answers them by first
building one row per SAMPLE — 174 million rows for the variable that motivated
this, 110 s to build and minutes to group — to draw 20,000 points.

Measured on that data, one 17 M-sample column (``.claude/plan-plot-minimal-load-
examples.md`` §8): min/max over the flat buffer 0.015 s, explode by
concatenate 0.39 s, per-subject mean/SD per position 0.75 s — against 4.6 s /
4.2 s in DuckDB SQL and ~37 s in pandas. So the fast implementation is numpy over
the cells the source already loaded, and DuckDB is left to SELECT rows.

**Semantics are pandas'**, pinned by ``scistackplotdb/tests/test_reducer_parity``:
``nanmean``/``nanmedian`` skip NaN as ``groupby().mean()/median()`` do;
``nanstd(ddof=1)`` is NaN for one replicate and is filled with 0.0 as
``std(ddof=1).fillna(0.0)`` was; ``count`` is the non-NaN count; quantiles
interpolate linearly, pandas' default. A position no replicate reaches (ragged
cells) has count 0 and is dropped — the pandas group never existed.

Cells are padded and reduced along axis 0 — but in **position blocks**, and only
the cells that reach a block take part in it. The first real run (2026-09-13
19:31) padded all 4,190 cells of one group to the longest cell, 325,855
samples against a mean of 41,565: a 10.2 GiB allocation, 87 % of it NaN, and
``MemoryError``. Blocking bounds the working set to ``PAD_BUDGET`` elements
whatever the group, and a ragged group costs what its samples cost. The
numbers are identical to the one-shot pad: the same nan-reductions over the
same values, position by position.
"""

from __future__ import annotations

import warnings
from typing import Any

import numpy as np

from .spec import ErrorBand, Statistic


def cell_array(value: Any) -> np.ndarray:
    """One cell as a 1-D float64 array; empty for None, NaN, or a non-sequence."""
    if value is None:
        return np.empty(0)
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            return np.empty(0)
        return value.astype("float64", copy=False).ravel()
    if isinstance(value, (list, tuple)):
        try:
            return np.asarray(value, dtype="float64").ravel()
        except (TypeError, ValueError):
            return np.empty(0)
    return np.empty(0)


def cell_arrays(values) -> list[np.ndarray]:
    """Every cell of a column as :func:`cell_array` — one call per CELL."""
    return [cell_array(v) for v in np.asarray(values, dtype=object)]


#: Elements per padded block — 32 M float64 is 256 MB. The block WIDTH follows
#: from it and the number of cells that reach the block, so a group of 4,190
#: cells is reduced ~7,600 positions at a time and a group of 20 cells in one go.
PAD_BUDGET = 32_000_000


def pad(arrays: list[np.ndarray]) -> np.ndarray:
    """``(len(arrays), longest)`` float64, NaN beyond each cell's own length.

    The one-shot form, for tests and small groups; the reductions below never
    call it on a whole group (see :func:`blocks`).
    """
    width = max((a.size for a in arrays), default=0)
    out = np.full((len(arrays), width), np.nan)
    for i, a in enumerate(arrays):
        out[i, : a.size] = a
    return out


def blocks(arrays: list[np.ndarray], budget: int | None = None):
    """Yield ``(start, padded)`` over position blocks: ``padded`` holds the
    samples at positions ``start .. start + padded.shape[1]`` of every cell
    that reaches ``start`` (shorter cells are simply absent from the block,
    which is exactly what NaN padding would have contributed: nothing).

    Cells are visited in ascending length so the members of a block are a
    suffix of the sorted list — one ``searchsorted``, no per-cell test.
    """
    budget = PAD_BUDGET if budget is None else budget  # read at call time: tests shrink it
    lengths = np.fromiter((a.size for a in arrays), dtype=np.int64, count=len(arrays))
    width = int(lengths.max()) if lengths.size else 0
    if not width:
        return
    order = np.argsort(lengths, kind="stable")
    sorted_arrays = [arrays[i] for i in order]
    sorted_lengths = lengths[order]
    start = 0
    while start < width:
        first = int(np.searchsorted(sorted_lengths, start, side="right"))
        members = sorted_arrays[first:]
        if not members:
            break
        block = max(256, budget // len(members))
        stop = min(start + block, width)
        padded = np.full((len(members), stop - start), np.nan)
        for i, a in enumerate(members):
            segment = a[start:stop]
            padded[i, : segment.size] = segment
        yield start, padded
        start = stop


def explode(
    arrays: list[np.ndarray], *, max_points: int | None = None
) -> tuple[np.ndarray, np.ndarray, np.ndarray, int, int]:
    """``(row, pos, val, total, stride)``: one entry per non-NaN sample.

    Exactly the rows ``reduce._explode_1d`` builds — the cell's row index, the
    0-based position within the cell (a NaN leaves a GAP, positions are not
    renumbered), the value — record-major, in the same order. ``total`` is how
    many rows that is; with ``max_points`` every ``stride``-th of them is kept,
    ``stride = total // max_points`` exactly as ``reduce._downsample`` strides
    the exploded frame — so the caller gathers labels for the kept rows only.
    """
    if not arrays:
        return np.empty(0, np.int64), np.empty(0, np.int64), np.empty(0), 0, 1
    lengths = np.fromiter((a.size for a in arrays), dtype=np.int64, count=len(arrays))
    flat = np.concatenate(arrays) if lengths.sum() else np.empty(0)
    starts = np.cumsum(lengths) - lengths
    row = np.repeat(np.arange(len(arrays), dtype=np.int64), lengths)
    pos = np.arange(flat.size, dtype=np.int64) - np.repeat(starts, lengths)
    keep = np.flatnonzero(~np.isnan(flat))
    total = int(keep.size)
    stride = 1
    if max_points is not None and total > max_points:
        stride = max(1, total // max_points)
        keep = keep[::stride]
    return row[keep], pos[keep], flat[keep], total, stride


def _width(arrays: list[np.ndarray]) -> int:
    return max((a.size for a in arrays), default=0)


def position_mean(arrays: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """``(mean, count)`` per position across ``arrays`` — the AGGREGATE collapse."""
    width = _width(arrays)
    mean = np.full(width, np.nan)
    count = np.zeros(width, dtype=np.int64)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN positions
        for start, padded in blocks(arrays):
            stop = start + padded.shape[1]
            count[start:stop] = np.sum(~np.isnan(padded), axis=0)
            mean[start:stop] = np.nanmean(padded, axis=0)
    return mean, count


def position_stats(
    arrays: list[np.ndarray], statistic: Statistic, error: ErrorBand
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """``(centre, low, high, count)`` per position — what a BAND/BAR draws.

    pandas twin: ``ylimits.spread_bounds`` (also what ``reduce._summarize`` draws), statistic by
    statistic (the reduction plan's §4 table).
    """
    width = _width(arrays)
    centre = np.full(width, np.nan)
    low = np.full(width, np.nan)
    high = np.full(width, np.nan)
    count = np.zeros(width, dtype=np.int64)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for start, padded in blocks(arrays):
            stop = start + padded.shape[1]
            n = np.sum(~np.isnan(padded), axis=0)
            count[start:stop] = n
            if statistic is Statistic.MEDIAN:
                c = np.nanmedian(padded, axis=0)
            else:
                c = np.nanmean(padded, axis=0)
            centre[start:stop] = c
            if error is ErrorBand.NONE:
                low[start:stop] = c
                high[start:stop] = c
            elif error is ErrorBand.IQR:
                low[start:stop] = np.nanpercentile(padded, 25, axis=0)
                high[start:stop] = np.nanpercentile(padded, 75, axis=0)
            else:
                sd = np.nanstd(padded, axis=0, ddof=1)
                sd = np.where(n >= 2, sd, 0.0)  # std(ddof=1).fillna(0.0)
                denominator = np.where(n > 0, n, 1)  # count.where(count > 0, 1)
                if error is ErrorBand.SD:
                    spread = sd
                elif error is ErrorBand.SEM:
                    spread = sd / np.sqrt(denominator)
                else:  # CI95
                    spread = 1.96 * sd / np.sqrt(denominator)
                low[start:stop] = c - spread
                high[start:stop] = c + spread
    return centre, low, high, count


def collapse_cells(arrays: list[np.ndarray], statistic: Statistic) -> np.ndarray:
    """One value per CELL: the centre of each cell's own samples.

    The reduction behind "plot this 1-D variable as a violin" — a per-trial
    vector of step lengths becomes one step length per trial, after which the
    measure is an ordinary scalar (``cell.apply_cell_collapse``,
    ``docs/claude/measure-shape-and-collapse.md``).

    Unlike everything else in this module this reduces ALONG each cell rather
    than across cells at a position, so there is nothing to pad and no block
    budget to respect: each cell is one ``nanmean``/``nanmedian`` over its own
    buffer. An empty or all-NaN cell yields NaN — the caller keeps that row and
    counts it rather than dropping it, so "this trial recorded nothing" cannot
    read as "this trial does not exist".

    Semantics are pandas' (``Series.mean()``/``.median()``): NaN samples are
    skipped, not propagated.
    """
    out = np.full(len(arrays), np.nan)
    reduce_fn = np.nanmedian if statistic is Statistic.MEDIAN else np.nanmean
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN cells
        for i, cell in enumerate(arrays):
            if cell.size:
                out[i] = reduce_fn(cell)
    return out
