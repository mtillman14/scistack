"""The ``Row`` factor — one owner for splitting "combine rows" cells into rows.

A variable saved with ``on_duplicate="combine"`` holds, per record, one value
PER SOURCE ROW in each ``row_list`` column (``sciduckdb.row_lists``):
``FIN = [6573…, 6597…]`` for a subject who appeared twice in the input. Those
elements are observations (one hospital stay each), not samples along an
axis, so drawing them as a 1-D series put them against an "index" x axis and
let the scalar kinds only average them (2026-10-30,
``.claude/plan-row-list-plots.md``).

:func:`explode_row_lists` turns each element into its own row and numbers it
with a synthetic factor, :data:`ROW_FACTOR`, 1-based in source-row order. The
combine rule keeps every list column aligned (position i is "source row i"),
so the same Row number names the same source row in every field.

**View only.** The Row factor exists in a plot's long table and never in the
database: a save-side Row was considered and rejected by the user (it would
turn accidental duplicates into silent replicates).

Who calls this:

* ``scistackplotdb.ScidbSource`` on the interactive path, after the struct
  melt;
* ``codegen`` emits the same reshape inline (a generated endpoint imports
  only numpy/pandas/seaborn), driven by :func:`row_factor` — the parity test
  in ``scistackplotdb/tests/test_fanout_parity.py`` holds the two together
  (docs/claude/synthetic-factors.md invariant 4).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from ..model.table import FactorInfo, LongTable

#: The factor's name. A schema key of the same name is never shadowed:
#: :func:`explode_row_lists` appends ``_`` until the name is free.
ROW_FACTOR = "Row"

#: ``FactorInfo.origin["kind"]`` of the Row factor. Consumers of ``origin``
#: act on ``"code"``/``"param"`` only, so this kind is inert to them.
ROW_LIST_ORIGIN = "row_list"


@dataclass
class RowSplit:
    """What :func:`explode_row_lists` did, for the log and the table."""

    frame: pd.DataFrame
    #: The Row column's name, or None when no cell held more than one element
    #: — a one-level factor is a constant, not a factor (synthetic-factors.md
    #: invariant 6), so the column is dropped, but the cells are still
    #: unwrapped to scalars.
    row_factor: str | None
    #: The row-list fields that were split (all of them for a one-column
    #: variable, whose list is ``[measure]``).
    fields: list[str] = field(default_factory=list)
    cells: int = 0
    rows_in: int = 0
    rows_out: int = 0
    max_rows: int = 0


def is_list_cell(value: Any) -> bool:
    """A cell holding a row list's elements: an array or a list/tuple. A
    string is iterable but is ONE value."""
    return isinstance(value, (list, tuple, np.ndarray)) and not isinstance(value, str)


def _elements(value: Any) -> list:
    if is_list_cell(value):
        items = list(np.asarray(value, dtype=object).ravel())
        # A row list never stores an empty list (the combine makes at least
        # one element), but if one arrives it stays one missing row rather
        # than vanishing — never drop data silently.
        return items or [None]
    return [value]


def element_values(series: pd.Series) -> pd.Series:
    """Every element of a row-list column as one flat Series — what a
    caller classifies to decide whether the column is plottable (a VARCHAR
    row list is text, not a 1-D series)."""
    return pd.Series(
        [item for value in series for item in _elements(value)], dtype=object
    )


def _as_numbers(values: pd.Series) -> pd.Series:
    """float64 when every non-missing element is a number, else unchanged.

    Exploding goes through object dtype; a numeric field must come back as
    one float buffer or it would classify by its first cell and reduce
    slowly. Anything else (a datetime, text) is left for the caller's own
    classification."""
    present = values.dropna()
    if len(present) and all(
        isinstance(v, (int, float, np.integer, np.floating))
        and not isinstance(v, (bool, np.bool_))
        for v in present
    ):
        return pd.to_numeric(values).astype("float64")
    return values


def explode_row_lists(
    frame: pd.DataFrame,
    measure: str,
    row_list_fields: list[str],
    *,
    field_factor: str | None = None,
) -> RowSplit:
    """One row per element of every row-list cell of ``measure``.

    ``frame`` is long: one ``measure`` column, plus ``field_factor`` (the
    struct's ``ColName``) when the variable was melted. Rows whose field is
    in ``row_list_fields`` are split; rows of other fields, and one-element
    cells, become one row with Row 1. Without a ``field_factor`` the measure
    column itself is the row list (``row_list_fields == [measure]``).

    Pure: the input frame is not modified.
    """
    if field_factor is not None:
        split = frame[field_factor].isin(row_list_fields).to_numpy()
    else:
        split = np.full(len(frame), measure in row_list_fields)

    cells = [
        _elements(value) if flag else [value]
        for value, flag in zip(frame[measure].to_numpy(dtype=object), split)
    ]
    counts = [len(c) for c in cells]
    max_rows = max(counts, default=0)

    name = ROW_FACTOR
    while name in frame.columns:  # never shadow a schema key
        name += "_"

    out = frame.copy()
    out[measure] = pd.Series(cells, index=frame.index, dtype=object)
    out[name] = pd.Series(
        [[str(i) for i in range(1, n + 1)] for n in counts],
        index=frame.index,
        dtype=object,
    )
    out = out.explode([measure, name], ignore_index=True)
    out[measure] = _as_numbers(out[measure])

    row_factor: str | None = name
    if max_rows < 2:
        out = out.drop(columns=[name])
        row_factor = None
    return RowSplit(
        frame=out,
        row_factor=row_factor,
        fields=list(row_list_fields),
        cells=int(split.sum()),
        rows_in=len(frame),
        rows_out=len(out),
        max_rows=max_rows,
    )


def row_origin(fields: list[str], field_factor: str | None) -> dict:
    """``FactorInfo.origin`` of the Row factor: which fields were split, and
    by which field factor — what codegen needs to emit the same reshape."""
    return {
        "kind": ROW_LIST_ORIGIN,
        "fields": list(fields),
        "field_factor": field_factor,
    }


def row_factor(table: "LongTable") -> "FactorInfo | None":
    """The table's Row factor, or None. The one lookup; nothing matches the
    name."""
    for factor in table.factors:
        if (factor.origin or {}).get("kind") == ROW_LIST_ORIGIN:
            return factor
    return None
