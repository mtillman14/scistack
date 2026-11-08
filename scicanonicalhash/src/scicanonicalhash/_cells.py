"""What is inside one cell: its numbers, and whether they look wrong.

PRIVATE: no user calls this. It is the one owner of "read a cell's numeric
elements" and "is this cell blank / empty / all NaN / partly NaN /
constant", shared by its two consumers, which cannot import each other:

* ``scidb.inspect.cells`` (``scidb nans``, ``Inspector.cell_profile``, the
  data preview's "Inside cells");
* ``scistackplot.reduce.series_stats.cell_array`` (every 1-D plot and the
  cell collapse) and the plot's "Not plotted" NaN counts.

It lives here because both already depend on scicanonicalhash and neither
on the other (decided 2026-11-08, ``.claude/plan-not-plotted-collapsed-nans.md``).
numpy is imported here, not by ``scicanonicalhash/__init__``, so importing
the package stays free. No logging: the callers log.

The reading rule (:func:`numeric_elements`):

* a numeric ndarray (bool, int, unsigned, float) is its elements, flattened;
* a list / tuple is read as ``np.asarray`` reads it, by the same rule
  (rectangular nesting is a matrix; ragged nesting is not numeric);
* an object array or list holding only numbers, bools and ``None`` is its
  elements, ``None`` read as NaN;
* an EMPTY array or list is an array with no elements;
* everything else is not numeric: text (even ``"1"``), complex, datetimes,
  dicts, ragged nesting, and every scalar (a 0-d array included).

A scistackplot export cannot import this, so ``export.reduction_code``
prints the same rule; ``scistackplot/tests/test_reduction_code.py`` runs the
printed helper over :data:`EDGE_CASES` against this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

BLANK = "blank"
EMPTY = "empty"
ALL_NAN = "all_nan"
PARTIAL_NAN = "partial_nan"
CONSTANT = "constant"
#: Every problem kind, worst first.
PROBLEM_KINDS = (ALL_NAN, PARTIAL_NAN, BLANK, EMPTY, CONSTANT)

#: The one wording of each problem kind.
PROBLEM_LABELS = {
    ALL_NAN: "all NaN",
    PARTIAL_NAN: "partly NaN",
    BLANK: "blank",
    EMPTY: "empty array",
    CONSTANT: "constant",
}

_NUMERIC_KINDS = "biuf"
_NUMBER_TYPES = (bool, int, float, np.bool_, np.integer, np.floating)


def numeric_elements(value: Any) -> np.ndarray | None:
    """The cell's numeric elements as a flat float array, or None when the
    cell is not a numeric array (see the module docstring for the rule)."""
    if isinstance(value, np.ndarray):
        arr = value
    elif isinstance(value, (list, tuple)):
        if not value:
            return np.asarray([], dtype=float)
        try:
            arr = np.asarray(value)
        except (TypeError, ValueError):  # ragged nesting (numpy >= 1.24)
            return None
    else:
        return None
    if arr.ndim == 0:
        return None  # a scalar
    if arr.size == 0:
        return np.asarray([], dtype=float)
    kind = arr.dtype.kind
    if kind in _NUMERIC_KINDS:
        return arr.astype(float, copy=False).ravel()
    if kind == "O":
        # Checked per TYPE, not per element in Python: a 17 M-sample column
        # of lists with NULLs reaches here, and set(map(type, ...)) runs in C.
        flat = arr.ravel()
        types = set(map(type, flat))
        if not all(t is type(None) or issubclass(t, _NUMBER_TYPES) for t in types):
            return None
        if type(None) in types:
            flat = flat.copy()
            flat[flat == None] = np.nan  # noqa: E711 (elementwise on an object array)
        return flat.astype(float)
    return None


def nan_count(elements: np.ndarray) -> tuple[int, int]:
    """``(NaN elements, elements)`` of :func:`numeric_elements`' result."""
    return int(np.isnan(elements).sum()), int(elements.size)


def is_blank(value: Any) -> bool:
    """A missing scalar: None, NaN (a 0-d NaN array too), pandas NA / NaT."""
    if value is None:
        return True
    if isinstance(value, np.ndarray):
        if value.ndim != 0 or value.dtype.kind not in "fc":
            return False
        return bool(np.isnan(value))
    if isinstance(value, (float, np.floating)):
        return bool(np.isnan(value))
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        return False
    return value is pd.NA or value is pd.NaT


def classify(value: Any) -> tuple[str | None, int, int]:
    """``(problem kind or None, elements, NaN elements)`` for one cell."""
    if is_blank(value):
        return BLANK, 0, 0
    arr = numeric_elements(value)
    if arr is None:
        return None, 0, 0
    nan, n = nan_count(arr)
    if n == 0:
        return EMPTY, 0, 0
    if nan == n:
        return ALL_NAN, n, nan
    if nan:
        return PARTIAL_NAN, n, nan
    if n >= 2 and float(np.nanmin(arr)) == float(np.nanmax(arr)):
        return CONSTANT, n, nan
    return None, n, nan


@dataclass
class CellStats:
    """Counts over the cells of one column (or one column at one value)."""

    cells: int = 0
    array_cells: int = 0
    elements: int = 0
    nan_elements: int = 0
    min_length: int | None = None
    max_length: int | None = None
    kinds: dict[str, int] = field(default_factory=lambda: {k: 0 for k in PROBLEM_KINDS})

    def add(self, value: Any) -> str | None:
        """Count one cell; returns its problem kind (or None)."""
        kind, n, nan = classify(value)
        self.cells += 1
        if kind != BLANK and (n or kind == EMPTY):
            self.array_cells += 1
            self.elements += n
            self.nan_elements += nan
            self.min_length = n if self.min_length is None else min(self.min_length, n)
            self.max_length = n if self.max_length is None else max(self.max_length, n)
        if kind is not None:
            self.kinds[kind] += 1
        return kind

    @property
    def problems(self) -> int:
        return sum(self.kinds.values())

    def describe(self) -> str:
        """One line: what is inside, and what looks wrong."""
        parts = []
        if self.array_cells:
            lengths = (
                f"length {self.min_length}" if self.min_length == self.max_length
                else f"lengths {self.min_length}-{self.max_length}"
            )
            parts.append(f"{self.array_cells} of {self.cells} cells are arrays ({lengths})")
            if self.nan_elements:
                share = 100.0 * self.nan_elements / self.elements if self.elements else 0.0
                parts.append(
                    f"{self.nan_elements} of {self.elements} values are NaN ({share:.1f}%)"
                )
        problems = [
            f"{self.kinds[k]} {PROBLEM_LABELS[k]}" for k in PROBLEM_KINDS if self.kinds[k]
        ]
        if problems:
            parts.append("cells: " + ", ".join(problems))
        return "; ".join(parts) if parts else "nothing suspect"

    def to_dict(self) -> dict:
        return {
            "cells": self.cells,
            "array_cells": self.array_cells,
            "elements": self.elements,
            "nan_elements": self.nan_elements,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "kinds": dict(self.kinds),
            "problems": self.problems,
            "description": self.describe(),
        }


def cell_stats(values) -> CellStats:
    """:class:`CellStats` over an iterable of cells."""
    stats = CellStats()
    for v in values:
        stats.add(v)
    return stats


#: Cells that once read differently in scidb, the plot and the export, with
#: the elements every reader must now return (None = not numeric). Shared
#: so the owner's tests and the export-parity test check the same cases.
EDGE_CASES: list[tuple[str, Any, list[float] | None]] = [
    ("float array", np.array([1.0, 2.0]), [1.0, 2.0]),
    ("int list", [1, 2, 3], [1.0, 2.0, 3.0]),
    ("bool array", np.array([True, False]), [1.0, 0.0]),
    ("list with None", [1.0, None, 3.0], [1.0, float("nan"), 3.0]),
    ("object array of numbers", np.array([1, 2.5], dtype=object), [1.0, 2.5]),
    ("matrix as nested lists", [[1, 2], [3, 4]], [1.0, 2.0, 3.0, 4.0]),
    ("2-D array", np.array([[1.0, 2.0], [3.0, 4.0]]), [1.0, 2.0, 3.0, 4.0]),
    ("empty list", [], []),
    ("empty array", np.array([]), []),
    ("0-d array", np.array(7.0), None),
    ("scalar", 5.0, None),
    ("None", None, None),
    ("text", "abc", None),
    ("numeric text list", ["1", "2"], None),
    ("text array", np.array(["a", "b"]), None),
    ("complex array", np.array([1 + 2j, 3 + 0j]), None),
    ("datetime array", np.array(["2026-01-01", "2026-01-02"], dtype="datetime64[D]"), None),
    ("ragged nesting", [[1, 2], [3]], None),
    ("dict", {"a": 1}, None),
]
