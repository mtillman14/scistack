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

Cells are padded to the group's longest cell before reducing along axis 0. That
is the simple, exact form; a ``bincount`` form would avoid the pad for very
ragged groups (8 k–326 k samples was measured) and is the noted follow-up.
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


def pad(arrays: list[np.ndarray]) -> np.ndarray:
    """``(len(arrays), longest)`` float64, NaN beyond each cell's own length."""
    width = max((a.size for a in arrays), default=0)
    out = np.full((len(arrays), width), np.nan)
    for i, a in enumerate(arrays):
        out[i, : a.size] = a
    return out


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


def position_mean(arrays: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """``(mean, count)`` per position across ``arrays`` — the AGGREGATE collapse."""
    padded = pad(arrays)
    count = np.sum(~np.isnan(padded), axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN positions
        mean = np.nanmean(padded, axis=0) if padded.size else np.empty(0)
    return mean, count


def position_stats(
    arrays: list[np.ndarray], statistic: Statistic, error: ErrorBand
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """``(centre, low, high, count)`` per position — what a BAND/BAR draws.

    pandas twin: ``reduce._summarize`` / ``ylimits._spread``, statistic by
    statistic (the reduction plan's §4 table).
    """
    padded = pad(arrays)
    if not padded.size:
        empty = np.empty(0)
        return empty, empty, empty, np.empty(0, dtype=np.int64)
    count = np.sum(~np.isnan(padded), axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        if statistic is Statistic.MEDIAN:
            centre = np.nanmedian(padded, axis=0)
        else:
            centre = np.nanmean(padded, axis=0)
        if error is ErrorBand.NONE:
            low = centre
            high = centre
        elif error is ErrorBand.IQR:
            low = np.nanpercentile(padded, 25, axis=0)
            high = np.nanpercentile(padded, 75, axis=0)
        else:
            sd = np.nanstd(padded, axis=0, ddof=1)
            sd = np.where(count >= 2, sd, 0.0)  # std(ddof=1).fillna(0.0)
            n = np.where(count > 0, count, 1)  # count.where(count > 0, 1)
            if error is ErrorBand.SD:
                spread = sd
            elif error is ErrorBand.SEM:
                spread = sd / np.sqrt(n)
            else:  # CI95
                spread = 1.96 * sd / np.sqrt(n)
            low = centre - spread
            high = centre + spread
    return centre, low, high, count
