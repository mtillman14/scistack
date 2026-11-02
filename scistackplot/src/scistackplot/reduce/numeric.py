"""``pd.to_numeric(errors="coerce")`` that survives numpy scalars.

Once a source hands a 1-D cell over as an ``np.ndarray`` rather than a list,
``DataFrame.explode`` yields one ``np.float64`` OBJECT per sample instead of a
Python float. pandas' ``maybe_convert_numeric`` (pandas 3.x, numpy 2.x) calls
``len()`` on any value that has ``__len__`` — numpy scalars do, and raise
``TypeError: len() of unsized object``. Found 2026-09-13 the moment the DuckDB
fetch stopped boxing (``scistackplotdb`` ``test_load_fetch.py``); every
``to_numeric`` downstream of an explode or over a cell column goes through here.

The fast path is also the point: a column of numpy scalars casts to float64 in
one C-level pass, where ``to_numeric`` was inspecting each object.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def coerce_numeric(values: pd.Series) -> pd.Series:
    """``values`` as float64, non-numeric entries NaN — never raising on a
    numpy scalar, and never stacking a column of array cells into a matrix."""
    if pd.api.types.is_numeric_dtype(values):
        return values
    raw = values.to_numpy()
    sample = _first_present(raw)
    if isinstance(sample, (np.generic, int, float)) and not isinstance(sample, bool):
        # Scalars (numpy or Python): one vectorised cast. Anything that is not
        # a number in the column raises here and falls through to the coercing
        # path below, which is what `errors="coerce"` promised.
        try:
            return pd.Series(raw.astype("float64"), index=values.index)
        except (TypeError, ValueError):
            pass
    return pd.to_numeric(values.map(_unboxed), errors="coerce")


def _first_present(raw: np.ndarray) -> Any:
    for value in raw:
        if value is None:
            continue
        if isinstance(value, float) and value != value:
            continue
        return value
    return None


def _unboxed(value: Any) -> Any:
    """A numpy scalar as its Python value; a list-like cell as None.

    ``to_numeric(errors="coerce")`` turned a list cell into NaN; an ndarray
    cell must land in the same place rather than in ``len()``.
    """
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (list, tuple, np.ndarray)):
        return None
    return value
