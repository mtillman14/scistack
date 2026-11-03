"""THE rule for "does this row's key equal this combo value" (one owner).

Spec: docs/claude/key-match-cases.json, read by scifor/tests/test_key_match_cases.py
and scimatlab/tests/matlab/scifor/TestKeyMatchCases.m. Consumers:

* ``scifor.foreach._filter_df_for_combo`` — Python's per-combo row filter,
* ``scifor.csv_export._filter_rows_by_metadata`` — the flat-table export,
* ``scifor.matlab_loop._key_mask`` — MATLAB's loop (planned in Python).

The rule is MATLAB's, adopted for both languages on 2026-11-02 (plan
.claude/plan-p6-behaviour.md stage 2):

* a **numeric** column (ints, floats, bools) compares numbers; a text value
  is parsed (``"1"`` and ``"01"`` match ``1``), an unparsable one matches
  nothing;
* a **text** column compares text exactly; a number value is spelled as
  MATLAB's ``string()`` does (``1`` -> ``"1"``, ``1.5`` -> ``"1.5"``,
  ``True`` -> ``"true"``), so ``"01"`` never matches ``1`` — zero padding is
  part of the value;
* a missing cell (None / NaN) matches nothing;
* any other column (datetimes, ...) compares by plain equality.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from scistacklog import Log

LAYER = "scifor"


def key_text(value: Any) -> str:
    """``string(value)`` as MATLAB spells it, for comparing a value with a
    text key column: whole numbers in full, other numbers with 5 significant
    digits, logicals as ``true``/``false``."""
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, np.bool_)):
        return "true" if value else "false"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        f = float(value)
        if math.isnan(f):
            return "NaN"
        if math.isinf(f):
            return "Inf" if f > 0 else "-Inf"
        if f == int(f) and abs(f) < 1e15:
            return str(int(f))
        return "%.5g" % f
    return str(value)


def key_number(value: Any) -> float:
    """A combo value compared with a numeric key column (MATLAB parses text
    with ``str2double``; anything unparsable is NaN and matches nothing)."""
    if isinstance(value, (bool, np.bool_)):
        return float(value)
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return math.nan
    return math.nan


def numeric_mask(numbers: np.ndarray, value: Any) -> np.ndarray:
    """Rows of a numeric key column (float64, NaN = missing) equal to *value*."""
    return np.asarray(numbers, dtype="float64") == key_number(value)


def text_mask(texts: np.ndarray, value: Any) -> np.ndarray:
    """Rows of a text key column (object array of str / None) equal to *value*."""
    return np.asarray(texts, dtype=object) == key_text(value)


def column_kind(series) -> str:
    """``"numeric"``, ``"text"`` or ``"other"`` for a pandas key column."""
    import pandas as pd

    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype) or (
        pd.api.types.is_numeric_dtype(dtype) and not pd.api.types.is_complex_dtype(dtype)
    ):
        return "numeric"
    if isinstance(dtype, pd.CategoricalDtype) or pd.api.types.is_string_dtype(dtype):
        return "text"
    if dtype == object:
        inferred = pd.api.types.infer_dtype(series, skipna=True)
        if inferred in ("integer", "floating", "mixed-integer-float", "boolean", "decimal"):
            return "numeric"
        if inferred in ("string", "empty", "mixed", "mixed-integer", "bytes"):
            return "text"
    return "other"


def series_mask(series, value: Any) -> np.ndarray:
    """Rows of a pandas key column equal to combo *value*, by the rule above."""
    import pandas as pd

    if _is_missing(value):
        return np.zeros(len(series), dtype=bool)
    kind = column_kind(series)
    if kind == "numeric":
        try:
            numbers = series.astype("float64").to_numpy()
        except (TypeError, ValueError):
            numbers = pd.to_numeric(series, errors="coerce").to_numpy(
                dtype="float64", na_value=np.nan
            )
        return numeric_mask(numbers, value)
    if kind == "text":
        text = key_text(value)
        values = series.astype(object).to_numpy()
        if pd.api.types.infer_dtype(values, skipna=True) in ("string", "empty"):
            return values == text
        # An object column holding numbers/bools: spell each cell as MATLAB
        # would, so 1 in an object column still matches "1".
        return np.array(
            [False if _is_missing(v) else key_text(v) == text for v in values],
            dtype=bool,
        )
    return (series == value).to_numpy(dtype=bool, na_value=False)


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        return bool(v != v)  # NaN / NaT
    except (TypeError, ValueError):
        return False


_warned: set = set()


def warn_if_type_mismatch(column: str, series, value: Any) -> None:
    """A combo matched 0 rows of *column* and its value's type differs from
    the column's kind: say so once per (column, kind, value type). This is the
    diagnostic for "my function was skipped" when a key was saved as text in
    one place and as a number in another."""
    kind = column_kind(series)
    value_kind = (
        "numeric" if isinstance(value, (bool, int, float, np.bool_, np.integer, np.floating))
        else "text" if isinstance(value, str) else type(value).__name__
    )
    if kind == value_kind or kind == "other":
        return
    token = (column, kind, value_kind)
    if token in _warned:
        return
    _warned.add(token)
    Log.warn(
        f"[key_match] column {column!r} is {kind} ({series.dtype}) but the combo "
        f"value {value!r} is {value_kind}: 0 rows matched. Numeric columns parse "
        f"text values; text columns compare exact text (\"01\" != \"1\"). "
        f"Further mismatches for this column are not repeated.",
        layer=LAYER,
    )
