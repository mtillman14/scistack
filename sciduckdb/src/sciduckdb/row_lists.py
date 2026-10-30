"""Row-list storage — the one owner of the ``row_list`` column kind.

A ``row_list`` column holds, per record, ONE VALUE PER SOURCE ROW: the
result of combining several table rows that share one schema location
(``on_duplicate="combine"``; the rule is ``scifor.combine_duplicate_rows``,
see docs/claude/duplicate-location-vs-stale.md). It is stored as a DuckDB
list of scalars (``DOUBLE[]``, ``BIGINT[]``, ``BOOLEAN[]``, ``VARCHAR[]``)
and its dtype metadata is::

    {"python_type": "row_list", "elem_type": "DOUBLE"}

What makes it a kind of its own (and not an ``ndarray`` column):

- A one-element cell ``[v]`` LOADS AS ``v``: most locations had one row, and
  they read exactly as they did before the column became a row list.
- A scalar saved into a row_list column is stored as ``[v]``. The kind is
  sticky: once a column needed a list, it stays a row list.
- A stored scalar column is converted in place the first time a save needs
  a list in it (``convert_column_to_row_list``), every old value wrapped, so
  every old record loads unchanged ("lazy conversion").
- row_list vs a genuine array column is never the same column: a column of
  real vectors must never be unwrapped.

Which columns of a save are row lists is decided by the caller
(``infer_data_columns(row_list_columns=...)``): no Python type marks a cell.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ROW_LIST_PTYPE = "row_list"

#: DuckDB element type -> the Python type a loaded element is restored to.
_ELEM_PTYPES = {
    "DOUBLE": "float",
    "BIGINT": "int",
    "BOOLEAN": "bool",
    "VARCHAR": "str",
}


class RowListTypeError(ValueError):
    """Values that cannot share one row_list column (mixed type classes, or a
    kind a row list cannot hold yet, such as datetimes)."""


def row_list_meta(elem_type: str) -> dict:
    """The column metadata of a row_list column of ``elem_type`` elements."""
    return {"python_type": ROW_LIST_PTYPE, "elem_type": elem_type.upper()}


def is_row_list_meta(meta: Any) -> bool:
    """Whether a column's dtype metadata describes a row_list column."""
    return isinstance(meta, dict) and meta.get("python_type") == ROW_LIST_PTYPE


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA:
        return True
    if isinstance(value, (float, np.floating)):
        return bool(np.isnan(value))
    return False


def _elements(value: Any) -> list:
    """A cell as its list of elements: a scalar is one element, a list /
    tuple / 1-D array its items. A masked (NULL) element becomes None."""
    if isinstance(value, np.ma.MaskedArray):
        mask = np.ma.getmaskarray(value)
        return [None if m else v for v, m in zip(value.data.tolist(), mask.tolist())]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _python_scalar(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return value


def _type_class(value: Any) -> str:
    """``bool`` / ``int`` / ``float`` / ``str`` for an element a row list can
    hold; raises RowListTypeError for anything else."""
    if isinstance(value, (bool, np.bool_)):
        return "bool"
    if isinstance(value, (int, np.integer)):
        return "int"
    if isinstance(value, (float, np.floating)):
        return "float"
    if isinstance(value, str):
        return "str"
    import datetime as _dt

    if isinstance(value, (_dt.date, _dt.datetime, pd.Timestamp, np.datetime64)):
        raise RowListTypeError(
            "datetime values cannot be combined into a row list yet"
        )
    raise RowListTypeError(
        f"a {type(value).__name__} value cannot be stored in a row list"
    )


def row_list_column_type(cells: Iterable, col_name: Any = None) -> tuple[str, dict]:
    """The DuckDB type and metadata of a row_list column, judged over EVERY
    element of every cell of the save (scalars and lists alike).

    all ints -> BIGINT[]; any float (or a missing element among numbers) ->
    DOUBLE[]; all bools -> BOOLEAN[]; all strings -> VARCHAR[]; no element
    at all -> DOUBLE[]. Mixing classes raises RowListTypeError.
    """
    classes: set[str] = set()
    has_missing = False
    for cell in cells:
        for e in _elements(cell):
            e = _python_scalar(e)
            if _is_missing(e):
                has_missing = True
                continue
            classes.add(_type_class(e))
    numeric = classes & {"int", "float"}
    if len(classes - numeric) + (1 if numeric else 0) > 1:
        raise RowListTypeError(
            f"column {col_name!r} mixes {sorted(classes)} values; a row list "
            f"holds one kind"
        )
    if classes == {"str"}:
        elem = "VARCHAR"
    elif classes == {"bool"}:
        elem = "BOOLEAN"
    elif classes == {"int"} and not has_missing:
        elem = "BIGINT"
    else:
        elem = "DOUBLE"
    return f"{elem}[]", row_list_meta(elem)


def row_list_to_storage(value: Any, meta: dict) -> "list | None":
    """The stored form of one cell of a row_list column: a scalar -> ``[v]``,
    a list -> itself; missing elements -> NULL. A missing scalar cell is a
    NULL cell (loads back as missing)."""
    from .sciduckdb import _str_to_storage

    if not isinstance(value, (list, tuple, np.ndarray)) and _is_missing(
        _python_scalar(value)
    ):
        return None
    elem = meta.get("elem_type", "DOUBLE")
    out = []
    for e in _elements(value):
        e = _python_scalar(e)
        if _is_missing(e):
            out.append(None)
        elif elem == "VARCHAR":
            out.append(_str_to_storage(e))
        elif elem == "DOUBLE":
            out.append(float(e))
        elif elem == "BIGINT":
            out.append(int(e))
        else:
            out.append(e)
    return out


def _restore_element(value: Any, elem_ptype: str) -> Any:
    value = _python_scalar(value)
    if _is_missing(value):
        return None if elem_ptype == "str" else np.nan
    if elem_ptype == "float":
        return float(value)
    if elem_ptype == "int":
        return int(value)
    if elem_ptype == "bool":
        return bool(value)
    return str(value)


def unwrap_row_list(value: Any, meta: dict) -> Any:
    """A stored row_list cell as the user sees it: ONE element -> that
    value, several -> a Python list in row order. NULL (cell or element) ->
    NaN, or None in a string column."""
    elem_ptype = _ELEM_PTYPES.get(meta.get("elem_type", "DOUBLE"), "float")
    if value is None or (
        not isinstance(value, (list, tuple, np.ndarray)) and _is_missing(value)
    ):
        return None if elem_ptype == "str" else np.nan
    elems = [_restore_element(e, elem_ptype) for e in _elements(value)]
    if not elems:
        return None if elem_ptype == "str" else np.nan
    if len(elems) == 1:
        return elems[0]
    return elems


def count_row_list_cells(values: Iterable) -> tuple[int, int]:
    """``(single, multi)``: how many stored cells hold one element (load as a
    scalar) and how many hold several (load as a list). For the load log."""
    single = multi = 0
    for v in values:
        if isinstance(v, (list, tuple, np.ndarray)):
            if len(v) > 1:
                multi += 1
            else:
                single += 1
    return single, multi


def _depth(ddb_type: str) -> int:
    t = ddb_type.strip().upper()
    depth = 0
    while t.endswith("[]"):
        depth += 1
        t = t[:-2].strip()
    return depth


def _base(ddb_type: str) -> str:
    t = ddb_type.strip().upper()
    while t.endswith("[]"):
        t = t[:-2].strip()
    return t


def row_list_change(
    stored_meta: "dict | None",
    stored_type: str,
    in_meta: "dict | None",
    in_type: str,
) -> "tuple[str, str] | None":
    """What a save does to a stored column when either side is a row list.

    Returns ``None`` when neither is (the caller's ordinary rules apply),
    else ``(action, elem_type)``:

    - ``("same", elem)``: store into the row_list column as it is (scalars
      are wrapped on the way in);
    - ``("convert", elem)``: the stored SCALAR column becomes ``elem[]``,
      every stored value wrapped (lazy conversion);
    - ``("alter", elem)``: the stored row_list's elements widen (BIGINT ->
      DOUBLE) or become text;
    - ``("refuse", reason)``: a genuine array column on either side, or
      element types the scalar rules refuse.

    The element types are judged by sciduckdb's ``column_type_change``
    (same / widen / as_text / refuse), exactly as for scalar columns.
    """
    from .sciduckdb import column_type_change

    s_rl = is_row_list_meta(stored_meta)
    i_rl = is_row_list_meta(in_meta)
    if not s_rl and not i_rl:
        return None
    if not s_rl and _depth(stored_type) != 0:
        return ("refuse", f"the stored column holds arrays ({stored_type})")
    if not i_rl and _depth(in_type) != 0:
        return ("refuse", f"this save's values are arrays ({in_type})")
    s_elem = (stored_meta or {}).get("elem_type") if s_rl else _base(stored_type)
    i_elem = (in_meta or {}).get("elem_type") if i_rl else _base(in_type)
    s_elem = (s_elem or "DOUBLE").upper()
    i_elem = (i_elem or "DOUBLE").upper()
    change = column_type_change(s_elem, i_elem)
    if change == "refuse":
        return ("refuse", f"{i_elem} values cannot go into a {s_elem} column")
    target = i_elem if change == "widen" else s_elem
    if target not in _ELEM_PTYPES:
        return ("refuse", f"a row list cannot hold {target} values")
    if not s_rl:
        return ("convert", target)
    if target != s_elem:
        return ("alter", target)
    return ("same", s_elem)


def convert_column_to_row_list(
    duck, table: str, col: str, elem_type: str, *, from_row_list: bool = False
) -> int:
    """Convert stored column ``col`` of ``table`` IN PLACE to ``elem_type[]``.

    From a scalar column every value is wrapped (``7`` -> ``[7]``, NULL stays
    NULL), so every stored record loads exactly as before; from a row_list
    column (``from_row_list``) the elements are cast (BIGINT[] -> DOUBLE[]).
    Record ids and content hashes are untouched: the loaded values are the
    same. The caller owns the transaction, the variable's view and its
    ``_variables.dtype`` metadata (they must change together).

    Returns how many stored rows held a value.
    """
    t0 = time.perf_counter()
    n = duck._fetchall(f'SELECT COUNT("{col}") FROM "{table}"')[0][0]
    if from_row_list:
        using = f'CAST("{col}" AS {elem_type}[])'
    else:
        using = (
            f'CASE WHEN "{col}" IS NULL THEN NULL '
            f'ELSE [CAST("{col}" AS {elem_type})] END'
        )
    duck._execute(
        f'ALTER TABLE "{table}" ALTER COLUMN "{col}" '
        f"SET DATA TYPE {elem_type}[] USING {using}"
    )
    logger.info(
        "row_list: %s.%s -> %s[] (%s), %d stored row(s) %s in %.3fs",
        table, col, elem_type,
        "elements cast" if from_row_list else "converted in place",
        n, "cast" if from_row_list else "wrapped",
        time.perf_counter() - t0,
    )
    return int(n)
