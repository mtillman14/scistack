"""Whether a for_columns group has any input at a combination.

THE owner of "missing" for the no-data rule (``.claude/plan-for-columns-
groups.md``): a group whose every value, in every row and every column, is
missing is not passed to the function, and nothing is saved for it there.

Missing means pandas ``isna`` (``None``, ``NaN``, ``NaT``, ``pd.NA``) and,
unlike the storage-side helpers (sciduckdb ``_is_missing_cell``, where ``""``
is real text), an empty or whitespace-only string: a blank cell in a sheet
is no input. Placeholder text such as ``"—"`` is data; turning it into a
missing value is the importer's decision. An array, list or dict cell is
data even when it holds NaN.
"""

from typing import Any


def is_missing_value(value: Any) -> bool:
    """Is one cell missing (no input)?"""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict, set)):
        return False
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return False
    except ImportError:  # pragma: no cover - numpy is a pandas dependency
        pass
    try:
        import pandas as pd

        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def frame_has_no_data(frame: Any) -> bool:
    """Is every cell of *frame* (a DataFrame, or a Series) missing? A frame
    with no rows has no data."""
    import pandas as pd

    if isinstance(frame, pd.Series):
        frame = frame.to_frame()
    if len(frame) == 0:
        return True
    for col in frame.columns:
        series = frame[col]
        if pd.api.types.is_numeric_dtype(series.dtype) or pd.api.types.is_datetime64_any_dtype(
            series.dtype
        ):
            if not series.isna().all():
                return False
            continue
        if not all(is_missing_value(v) for v in series):
            return False
    return True
