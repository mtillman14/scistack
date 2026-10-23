"""
Measure shape classification.

The *shape* of a measure — scalar, 1-D series, 2-D matrix — is the single
input that decides which plot kinds are even meaningful for it (see
``capability.available_plots``). Classification is deliberately based on
**observed values**, not on declared dtypes or SQL type-name strings: a pandas
object column holding numpy arrays and a DuckDB LIST column both arrive here
as "a cell that contains a sequence", and the value is the only reliable
common ground. (The GUI's pre-existing ``_numeric_plot_kind`` in
``scistack_gui/api/variables.py`` made the same call for the same reason.)
"""

from __future__ import annotations

from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from .temporal import is_temporal_value


class Shape(str, Enum):
    """What one cell of a measure column holds."""

    SCALAR = "scalar"
    SERIES_1D = "1d"
    MATRIX_2D = "2d"
    CATEGORICAL = "categorical"
    UNKNOWN = "unknown"

    def __str__(self) -> str:  # keeps f-strings and log lines readable
        return self.value


def _is_number(value: Any) -> bool:
    # bool is an int subclass; a column of True/False is categorical, not scalar.
    if isinstance(value, bool) or isinstance(value, np.bool_):
        return False
    return isinstance(value, (int, float, np.integer, np.floating))


def classify_value(value: Any) -> Shape:
    """Classify a single cell value."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return Shape.UNKNOWN
    # bool is an int subclass, so this must precede the numeric check: a
    # True/False column groups data, it is not something to plot on an axis.
    if isinstance(value, (bool, np.bool_)):
        return Shape.CATEGORICAL
    if _is_number(value):
        return Shape.SCALAR
    # A datetime is one value per row, by the same rules as a number (user,
    # 2026-10-22): plotted on a time axis (scistackplot.temporal).
    if is_temporal_value(value):
        return Shape.SCALAR
    if isinstance(value, str):
        return Shape.CATEGORICAL
    if isinstance(value, np.ndarray):
        if value.ndim == 1:
            return Shape.SERIES_1D if value.size else Shape.UNKNOWN
        if value.ndim == 2:
            return Shape.MATRIX_2D
        return Shape.UNKNOWN
    if isinstance(value, (list, tuple)):
        if not value:
            return Shape.UNKNOWN
        first = value[0]
        if _is_number(first):
            return Shape.SERIES_1D
        if isinstance(first, (list, tuple, np.ndarray)):
            return Shape.MATRIX_2D
        return Shape.UNKNOWN
    return Shape.UNKNOWN


def classify_column(series: pd.Series) -> Shape:
    """
    Classify a whole measure column.

    A column is uniformly typed in every source we support (a DuckDB column,
    or a DataFrame column built from one), so the first non-null value decides.
    We still scan a few values rather than exactly one: a column can lead with
    nulls, and an all-null column must classify as UNKNOWN rather than crash.
    """
    if series is None or len(series) == 0:
        return Shape.UNKNOWN

    if pd.api.types.is_bool_dtype(series):
        return Shape.CATEGORICAL
    if pd.api.types.is_numeric_dtype(series):
        return Shape.SCALAR
    if isinstance(series.dtype, pd.CategoricalDtype):
        return Shape.CATEGORICAL

    for value in series.head(_SCAN_LIMIT):
        shape = classify_value(value)
        if shape is not Shape.UNKNOWN:
            return shape
    return Shape.UNKNOWN


#: How many leading values to inspect before giving up on a column. Small on
#: purpose — this runs on every measure of every table the GUI describes.
_SCAN_LIMIT = 20


def is_plottable(shape: Shape) -> bool:
    """Whether any plot kind can render this shape."""
    return shape in (Shape.SCALAR, Shape.SERIES_1D, Shape.MATRIX_2D)
