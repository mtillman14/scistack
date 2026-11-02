"""How a plot DRAWS a 95% confidence interval. The formula is not defined here.

The formula belongs to ``csvstats.descriptive``, the one owner of every
descriptive statistic in scistack (user decision 2026-10-17). It is the
t-based interval of the mean, ``centre ± t(0.975, n - 1) · SD / √n``.
StatStudio's tables and csv-stats' test results read the same functions, so a
figure's error bar and a table's CI cannot disagree.

This module adds only the drawing policy. csv-stats returns NaN when there
are fewer than two values, and a plot draws that as a zero-width bar,
matching the ``std(ddof=1).fillna(0.0)`` its SD bars already use.

Consumers: ``ylimits.spread_bounds`` / ``ylimits._summary_bounds`` (pandas;
what ``reduce._summarize`` draws), ``series_stats.position_stats`` (the numpy
twin), and ``codegen``, whose exported code calls ``csvstats.descriptive.
ci_interval`` as seaborn's ``errorbar=``.

Before 2026-10-17 the preview drew ``1.96 · SEM`` and the export drew
seaborn's bootstrap ``("ci", 95)``: two answers for one spec.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from csvstats.descriptive import QUANTILE_METHOD, SD_DDOF, ci_half_width, sem
from scistacklog import Log

LAYER = "scistackplot"

#: SD_DDOF / QUANTILE_METHOD are csv-stats' conventions, re-exported for the plot's
#: own groupby/nan reductions so they name the choice instead of restating it.
__all__ = ["SD_DDOF", "QUANTILE_METHOD", "ci95_spread", "sem_spread"]


def _zero_where_undefined(half: Any) -> Any:
    """NaN (no interval below two values) drawn as a zero-width bar."""
    if isinstance(half, pd.Series):
        return half.fillna(0.0)
    if np.ndim(half) == 0:
        return 0.0 if np.isnan(half) else float(half)
    return np.nan_to_num(half, nan=0.0)


def sem_spread(sd: Any, count: Any) -> Any:
    """Half-width of the SEM bar: csv-stats' SEM, 0 where it has none."""
    return _zero_where_undefined(sem(sd, count))


def ci95_spread(sd: Any, count: Any) -> Any:
    """Half-width of the CI95 bar: csv-stats' value, 0 where it has none.

    Takes and returns a scalar, numpy array or pandas Series (index kept).
    """
    half = ci_half_width(sd, count)
    counts = np.asarray(count, dtype=float)
    if counts.size:
        valid = counts[counts >= 2]
        if valid.size:
            Log.debug(
                "spread ci95: t-based (csvstats.descriptive), n %d..%d",
                int(valid.min()),
                int(valid.max()),
                layer=LAYER,
            )
    return _zero_where_undefined(half)
