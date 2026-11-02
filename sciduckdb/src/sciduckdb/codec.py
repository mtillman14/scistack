"""The codec: Python values to queryable DuckDB columns and back.

Pure functions, no connection. ``infer_data_columns`` picks a DuckDB type per
column and records restore metadata; ``value_to_storage_row`` /
``dataframe_to_storage_rows`` / ``bulk_df_to_storage_rows`` encode;
``storage_to_python*`` / ``restore_types`` decode. Datetimes and row lists
have their own owners (``sciduckdb.datetimes``, ``sciduckdb.row_lists``).
"""

import datetime
import json
import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from scicanonicalhash import is_missing_datetime

from .datetimes import (
    DATETIME_PTYPE,
    INSTANT_TYPE,
    column_unit,
    datetime_meta,
    datetime_series_to_storage,
    datetime_to_storage,
    is_datetime_meta,
    storage_column_names,
    storage_column_types,
    tz_column_name,
)
from .row_lists import (
    ROW_LIST_PTYPE,
    count_row_list_cells,
    is_row_list_meta,
    row_list_column_type,
    row_list_to_storage,
    unwrap_row_list,
)

logger = logging.getLogger("sciduck")


def _schema_str(value):
    """Stringify a schema key value, converting whole-number floats to int.

    Schema keys are stored as VARCHAR.  str(1.0) → "1.0" but str(1) → "1".
    MATLAB sends all numbers as float, so without this conversion queries
    and cache lookups fail because "1.0" ≠ "1".
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


# ---------------------------------------------------------------------------
# Type mapping helpers
# ---------------------------------------------------------------------------


def _numpy_dtype_to_duckdb(dtype: np.dtype) -> str:
    """Map a numpy scalar dtype to a DuckDB type string."""
    kind = dtype.kind
    size = dtype.itemsize
    if kind == "f":
        return "FLOAT" if size <= 4 else "DOUBLE"
    if kind in ("i", "u"):
        mapping = {1: "TINYINT", 2: "SMALLINT", 4: "INTEGER", 8: "BIGINT"}
        base = mapping.get(size, "BIGINT")
        if kind == "u":
            return "U" + base
        return base
    if kind == "b":
        return "BOOLEAN"
    if kind in ("U", "S", "O"):
        return "VARCHAR"
    if kind == "M":
        return "TIMESTAMP"
    if kind == "m":
        return "INTERVAL"
    return "VARCHAR"


def _infer_duckdb_type(value: Any) -> tuple[str, dict]:
    """
    Infer the DuckDB column type and a metadata dict for round-trip
    restoration from a single Python/numpy value.

    Returns (duckdb_type_str, metadata_dict).
    """
    meta: dict = {}

    # --- numpy arrays ---
    if isinstance(value, np.ndarray):
        base = _numpy_dtype_to_duckdb(value.dtype)
        meta["python_type"] = "ndarray"
        meta["numpy_dtype"] = str(value.dtype)
        meta["ndim"] = value.ndim
        meta["shape_hint"] = list(value.shape)
        if value.ndim == 1:
            return f"{base}[]", meta
        if value.ndim == 2:
            meta["shape_hint"] = [None, value.shape[1]]  # rows vary, cols fixed
            return f"{base}[][]", meta
        # 3-D+ : store as JSON
        meta["python_type"] = "ndarray_json"
        return "VARCHAR", meta

    # --- Python scalars ---
    if isinstance(value, bool):
        meta["python_type"] = "bool"
        return "BOOLEAN", meta
    if isinstance(value, int):
        meta["python_type"] = "int"
        return "BIGINT", meta
    if isinstance(value, float):
        meta["python_type"] = "float"
        return "DOUBLE", meta
    if isinstance(value, str):
        meta["python_type"] = "str"
        return "VARCHAR", meta

    # --- Python lists ---
    if isinstance(value, list):
        meta["python_type"] = "list"
        if len(value) > 0:
            inner = value[0]
            # Check for homogeneous list
            if isinstance(inner, list):
                if not all(isinstance(v, list) for v in value):
                    raise TypeError(
                        "Heterogeneous lists are not supported. "
                        "All elements must be the same type."
                    )
                meta["nested"] = True
                return "DOUBLE[][]", meta
            if isinstance(inner, np.ndarray):
                if not all(isinstance(v, np.ndarray) for v in value):
                    raise TypeError(
                        "Heterogeneous lists are not supported. "
                        "All elements must be the same type."
                    )
                meta["nested"] = True
                meta["contains_ndarray"] = True
                meta["ndarray_dtype"] = str(inner.dtype)
                return "DOUBLE[][]", meta
            if isinstance(inner, (int, float)):
                if not all(isinstance(v, (int, float)) for v in value):
                    raise TypeError(
                        "Heterogeneous lists are not supported. "
                        "All elements must be the same type."
                    )
                return "DOUBLE[]", meta
            if isinstance(inner, str):
                if not all(isinstance(v, str) for v in value):
                    raise TypeError(
                        "Heterogeneous lists are not supported. "
                        "All elements must be the same type."
                    )
                return "VARCHAR[]", meta
        return "VARCHAR[]", meta

    # --- dict → JSON ---
    if isinstance(value, dict):
        meta["python_type"] = "dict"
        # Track ndarray values for restoration
        ndarray_keys = {}
        for k, v in value.items():
            if isinstance(v, np.ndarray):
                ndarray_keys[k] = {
                    "dtype": str(v.dtype),
                    "shape": list(v.shape),
                }
        if ndarray_keys:
            meta["ndarray_keys"] = ndarray_keys
        return "JSON", meta

    # --- datetime (pd.NaT and np.datetime64 included) ---
    # TIMESTAMP_NS holding UTC, plus the <col>__tz companion added by
    # storage_column_types — owner sciduckdb.datetimes.
    if isinstance(value, (datetime.datetime, pd.Timestamp, np.datetime64)):
        return INSTANT_TYPE, datetime_meta()
    if isinstance(value, datetime.date):
        meta["python_type"] = "date"
        return "DATE", meta
    if isinstance(value, (datetime.timedelta, pd.Timedelta)):
        meta["python_type"] = "INTERVAL"
        return "INTERVAL", meta

    # --- pandas categorical (shouldn't normally arrive here, but handle) ---
    if isinstance(value, pd.Categorical):
        meta["python_type"] = "categorical"
        return "VARCHAR", meta

    # --- fallback: JSON-serialize ---
    meta["python_type"] = "json_fallback"
    return "VARCHAR", meta


def _convert_for_json(value: Any) -> Any:
    """Recursively convert ndarrays/DataFrames to lists for JSON serialization."""
    if isinstance(value, pd.DataFrame):
        return _convert_for_json(value.to_dict("list"))
    if isinstance(value, pd.Series):
        return value.tolist()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, dict):
        return {k: _convert_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_for_json(v) for v in value]
    return value


def _str_to_storage(value: Any) -> Any:
    """The stored form of one cell of a VARCHAR column — the one owner.

    Empty (None / NaN / pd.NA / NaT) -> NULL, so a blank never becomes the
    text 'nan'. A non-string (a number arriving in a text column, or a
    batch mixing numbers and strings) -> its text, integral floats without
    '.0' (``_schema_str``: 123.0 -> "123", as a CSV wrote it).
    """
    if isinstance(value, np.generic):
        value = value.item()
    if _is_missing_cell(value):
        return None
    if isinstance(value, str):
        return value
    return _schema_str(value)


def _python_to_storage(value: Any, meta: dict) -> Any:
    """Convert a Python value to its DuckDB-storable form."""
    ptype = meta.get("python_type", "")

    if ptype == ROW_LIST_PTYPE:
        return row_list_to_storage(value, meta)

    # infer_data_columns unwraps length-1 arrays to scalars when it picks the
    # column type, e.g. {"x": np.array([1.0])} -> DOUBLE column with
    # python_type="float". Mirror that unwrap here so the stored value is a
    # scalar matching the column; otherwise the row carries a DOUBLE[] into a
    # DOUBLE column and DuckDB rejects the cast (DOUBLE[] -> DOUBLE).
    if ptype in ("float", "int", "bool", "str"):
        if isinstance(value, np.ndarray) and value.size == 1:
            value = value.item()
        elif isinstance(value, np.generic):
            value = value.item()

    if ptype == "str":
        return _str_to_storage(value)

    if ptype == "ndarray":
        arr = value
        # Scalar in a column typed as ndarray (e.g. ragged vectors): wrap as 1-element list
        if not isinstance(arr, np.ndarray):
            return [arr]
        if arr.ndim == 1:
            return arr.tolist()
        if arr.ndim == 2:
            return [row.tolist() for row in arr]

    if ptype == "ndarray_json":
        return json.dumps(value.tolist())

    if ptype == "dict":
        return json.dumps(_convert_for_json(value))

    if ptype == "json_fallback":
        return json.dumps(_convert_for_json(value))

    if ptype == "list":
        # Convert ndarrays within list to nested lists
        if meta.get("contains_ndarray"):
            return [v.tolist() if isinstance(v, np.ndarray) else v for v in value]
        return value  # DuckDB handles native lists

    return value


def array_from_storage(value: Any, dtype: np.dtype) -> np.ndarray:
    """``np.asarray`` for a stored list value that may carry NULL elements.

    DuckDB hands a LIST holding NULL elements back as a ``numpy.ma.MaskedArray``.
    ``np.asarray`` on that DROPS the mask and exposes DuckDB's fill buffer, so a
    NaN saved from MATLAB came back as ``0`` (GAITRiteLoaded.L_StepLengths_GR,
    2026-09-15) — silently, through every load path, because they all funnel
    here. NULL in storage means NaN in Python: fill the masked slots with NaN,
    upcasting to float64 when the declared dtype (int/bool) cannot carry one.
    """
    if isinstance(value, np.ma.MaskedArray) and np.ma.is_masked(value):
        n_null = int(np.ma.count_masked(value))
        if dtype.kind != "f":
            logger.debug(
                "NULL list element(s) in a %s column: upcast to float64 so they "
                "can be restored as NaN",
                dtype,
            )
            dtype = np.dtype("float64")
        logger.debug("restored %d NULL list element(s) as NaN", n_null)
        return np.ma.filled(value.astype(dtype), np.nan)

    # Never cast NaN into an integer dtype: np.asarray([1., nan], dtype=int64)
    # yields INT_MIN silently. This fires when the DECLARED dtype cannot hold a
    # NaN the value actually has — either a column that acquired a NULL after
    # its dtype was recorded, or a second restoration pass over an array this
    # function already upcast (scidb's multi-column load runs restore_types AND
    # then storage_to_python on the same cell, so restoration must be idempotent).
    arr = np.asarray(value)
    if dtype.kind != "f" and arr.dtype.kind == "f" and arr.size and np.isnan(arr).any():
        logger.debug(
            "array holds NaN but is declared %s: keeping float64 rather than "
            "casting NaN to an integer",
            dtype,
        )
        return arr.astype(np.float64)
    return np.asarray(value, dtype=dtype)


def count_null_list_elements(values) -> int:
    """Number of NULL elements across stored list values (masked slots).

    ``values`` is any iterable of cells as DuckDB returned them — a pandas
    Series, a list, or a column of an object-dtype DataFrame. Nested lists
    (2-D ``ndarray`` columns arrive as an ndarray of per-row arrays) are
    counted one level down. Loaders use this to log how many NaN were
    restored, at the seam where the corruption used to happen.
    """
    n = 0
    for v in values:
        if isinstance(v, np.ma.MaskedArray):
            n += int(np.ma.count_masked(v))
        elif isinstance(v, np.ndarray) and v.dtype == object:
            n += sum(
                int(np.ma.count_masked(row))
                for row in v
                if isinstance(row, np.ma.MaskedArray)
            )
    return n


def count_nan_array_elements(values) -> int:
    """Number of NaN elements across in-memory array cells about to be saved.

    The save-side twin of :func:`count_null_list_elements`: DuckDB's pandas
    scanner stores these as NULL, and the loader restores them as NaN. Logged
    at save time so a NaN-vs-NULL question is answerable from scidb.log.
    """
    n = 0
    for v in values:
        if isinstance(v, np.ndarray) and v.dtype.kind == "f" and v.size:
            n += int(np.isnan(v).sum())
        elif isinstance(v, list) and v:
            try:
                arr = np.asarray(v, dtype=float)
            except (TypeError, ValueError):
                continue
            n += int(np.isnan(arr).sum())
    return n


def storage_to_python(value: Any, meta: dict) -> Any:
    """Restore a stored DuckDB value back to its original Python type."""
    if meta.get("empty"):
        # An undecided (all-empty) column: missing, as pandas reads a blank.
        return np.nan
    ptype = meta.get("python_type", "")

    if ptype == ROW_LIST_PTYPE:
        return unwrap_row_list(value, meta)

    if ptype == "ndarray":
        dtype = np.dtype(meta.get("numpy_dtype", "float64"))
        ndim = meta.get("ndim", 1)
        if ndim >= 2:
            # DuckDB returns an ndarray of ndarrays, or one 2-D masked array
            # when an element is NULL. The NaN restoration engages ONLY when a
            # mask is actually present: matrix shape through this branch is
            # load-bearing (TestEndToEnd.test_matrix_through_pipeline,
            # TestFromPython.test_numpy_2d_orientation_is_not_transposed), so
            # NaN-free data must take the exact path it always took.
            if isinstance(value, np.ma.MaskedArray):
                return array_from_storage(value, dtype)
            if any(
                isinstance(row, np.ma.MaskedArray) and np.ma.is_masked(row)
                for row in value
            ):
                return np.stack([array_from_storage(row, dtype) for row in value])
            return np.stack([np.asarray(row) for row in value]).astype(dtype)
        return array_from_storage(value, dtype)

    if ptype == "ndarray_json":
        dtype = np.dtype(meta.get("numpy_dtype", "float64"))
        return np.array(json.loads(value), dtype=dtype)

    if ptype == "dict":
        if isinstance(value, str):
            result = json.loads(value)
        else:
            result = value  # DuckDB JSON type may already return dict
        # Restore ndarray values if metadata exists
        ndarray_keys = meta.get("ndarray_keys", {})
        for k, arr_meta in ndarray_keys.items():
            if k in result:
                dtype = np.dtype(arr_meta.get("dtype", "float64"))
                result[k] = np.array(result[k], dtype=dtype)
        return result

    if ptype == "json_fallback":
        return json.loads(value)

    if ptype == "list":
        # DuckDB may return ndarray; convert back to list
        if meta.get("contains_ndarray"):
            # Restore as list of ndarrays
            dtype = np.dtype(meta.get("ndarray_dtype", "float64"))
            return [array_from_storage(v, dtype) for v in value]
        if isinstance(value, np.ndarray):
            if meta.get("nested"):
                return [v.tolist() if isinstance(v, np.ndarray) else v for v in value]
            return value.tolist()
        return value

    if ptype == "int":
        return int(value) if value is not None else None

    if ptype == "float":
        return float(value) if value is not None else None

    if ptype == "bool":
        return bool(value) if value is not None else None

    if ptype == "str":
        return str(value) if value is not None else None

    if ptype == DATETIME_PTYPE:
        # The zone is folded in by merge_tz_columns (it needs the companion
        # column); here only a missing value is normalised: NULL is NaT.
        return pd.NaT if _is_missing_cell(value) else value

    return value


def storage_to_python_column(series: "pd.Series", meta: dict) -> "pd.Series":
    """Vectorized column-level dispatch of storage_to_python.

    Applied once per column in bulk loads instead of once per cell (N records ×
    M columns calls vs N×M calls for the per-element path).  Pass-through types
    (float, int, bool, str) are returned unchanged — DuckDB already emits the
    right pandas dtype for those.  Complex types use pd.Series.apply, which is
    still faster than an explicit Python for-loop.
    """
    if meta.get("empty"):
        # An undecided (all-empty) column: missing, as pandas reads a blank.
        return pd.Series(np.nan, index=series.index, dtype=float, name=series.name)
    ptype = meta.get("python_type", "")

    # Scalar types: DuckDB already returns the right pandas dtype — no-op.
    # Datetimes too: merge_tz_columns has already restored them.
    if ptype in ("float", "int", "bool", "str", "", DATETIME_PTYPE):
        return series

    # JSON blob types: decode string once per cell, but at column granularity.
    if ptype in ("dict", "json_fallback"):
        return series.apply(lambda v: json.loads(v) if isinstance(v, str) else v)

    if ptype == ROW_LIST_PTYPE:
        single, multi = count_row_list_cells(series)
        logger.info(
            "row_list column %r: %d cell(s) loaded as one value, %d as a list "
            "of combined rows", series.name, single, multi,
        )
        out = np.empty(len(series), dtype=object)
        for i, v in enumerate(series.tolist()):
            out[i] = unwrap_row_list(v, meta)
        return pd.Series(out, index=series.index, name=series.name, dtype=object)

    # All remaining types (ndarray, ndarray_json, list, …): delegate per-element.
    # NULL list elements are restored as NaN inside storage_to_python; the
    # caller aggregates count_null_list_elements() into one INFO line per load.
    return series.apply(lambda v: storage_to_python(v, meta))


def restore_types(df: pd.DataFrame, dtype_meta: dict) -> pd.DataFrame:
    """Restore the Python types of a non-DataFrame variable's data columns, in place.

    ``df`` holds stored rows (one per record) as DuckDB returned them, after
    ``merge_tz_columns``. NULL list elements come back as NaN; their count is
    logged once per call.
    """
    columns_meta = dtype_meta.get("columns", {})
    null_counts: dict = {}
    for col_name, col_meta in columns_meta.items():
        if col_name in df.columns:
            n_null = count_null_list_elements(df[col_name])
            if n_null:
                null_counts[col_name] = n_null
            df[col_name] = [
                storage_to_python(df[col_name].iloc[i], col_meta)
                for i in range(len(df))
            ]
    if null_counts:
        logger.debug(
            "restore_types: restored NULL list element(s) as NaN: %s", null_counts
        )
    return df


def _flatten_dict(d, _prefix=()):
    """Flatten a nested dict into {dot.separated.key: leaf_value} pairs.
    Returns (flat_dict, path_map) where path_map maps each dot-key
    to its tuple-of-keys path for faithful reconstruction."""
    flat = {}
    paths = {}
    for k, v in d.items():
        current = _prefix + (k,)
        if isinstance(v, dict):
            sub_flat, sub_paths = _flatten_dict(v, current)
            flat.update(sub_flat)
            paths.update(sub_paths)
        else:
            dot_key = ".".join(current)
            flat[dot_key] = v
            paths[dot_key] = list(current)
    return flat, paths


def unflatten_dict(flat, path_map):
    """Reconstruct a nested dict from flat dot-keys using stored path_map."""
    result = {}
    for dot_key, value in flat.items():
        path = path_map.get(dot_key, dot_key.split("."))
        current = result
        for key in path[:-1]:
            current = current.setdefault(key, {})
        current[path[-1]] = value
    return result


# ---------------------------------------------------------------------------
# Column inference & storage-row helpers (module-level, used by SciDuck and
# DatabaseManager)
# ---------------------------------------------------------------------------


def _is_missing_cell(value: Any) -> bool:
    """True for a scalar missing marker (None, NaN, pd.NA, NaT); never for
    an array/list cell, which is data even when it holds NaN."""
    if value is None:
        return True
    if isinstance(value, (np.ndarray, list, tuple, dict)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


# Column metadata of a DataFrame column that is empty (NULL / NaN / None /
# NaT) in every row of every record of a save: no type is decided yet. It is
# stored as an all-NULL VARCHAR placeholder and loads back as NaN; the first
# save that brings values decides the type (scidb's _reconcile_column_types
# converts the stored column then). The flag describes the data, never a
# promise: a later save re-derives it.
_EMPTY_COLUMN_META = {"python_type": "str", "empty": True}

# pd.api.types.infer_dtype verdicts (skipna=True) for the non-missing cells
# of an object column, mapped to the DuckDB type the batch decides.
_INFERRED_KIND_TYPES = {
    "string": ("VARCHAR", "str"),
    "boolean": ("BOOLEAN", "bool"),
    "integer": ("BIGINT", "int"),
    "floating": ("DOUBLE", "float"),
    "mixed-integer-float": ("DOUBLE", "float"),
    "decimal": ("DOUBLE", "float"),
}


def _batch_column_type(cells: pd.Series, col_name: Any) -> tuple[str, dict]:
    """The DuckDB type of one DataFrame column, judged over the WHOLE batch.

    ``cells`` is the column across every record of one save (one run's
    output: every iteration, every schema level). History, scistack.log
    2026-10-22: ``pd.read_csv`` of a CERNER pull gave IP_SPINAL_STIMULATION
    (mostly blank, some 'Y') object dtype; typing from row 0 read NaN and
    made it DOUBLE, then 'Y' failed the insert. The re-run spread the CSV one
    record per subject and the first subject was blank, so the judgement
    must cross records, not just rows. The rule:

    - every cell empty -> undecided (``_EMPTY_COLUMN_META``, VARCHAR placeholder)
    - every non-empty cell numeric -> BIGINT (all integers) / DOUBLE
    - any non-empty cell a string -> VARCHAR (numbers in it stored as text)
    - array / list / dict / datetime cells -> the first non-empty cell's
      type, as before
    - a datetime64 column (naive or tz-aware), or an object column whose
      every cell is NaT -> datetime even when it holds no value: the dtype
      already says what it is, and a datetime column needs its __tz
      companion from the first save (sciduckdb.datetimes)
    """
    if pd.api.types.is_datetime64_any_dtype(cells.dtype):
        return INSTANT_TYPE, datetime_meta(column_unit(cells.dtype))
    if cells.dtype != object:
        present = cells[cells.notna()]
        if present.empty:
            return "VARCHAR", dict(_EMPTY_COLUMN_META)
        cell_val = present.iloc[0]
        if isinstance(cell_val, np.generic):
            cell_val = cell_val.item()
        return _infer_duckdb_type(cell_val)

    all_cells = cells.tolist()
    present = [v for v in all_cells if not _is_missing_cell(v)]
    if not present:
        if all_cells and all(is_missing_datetime(v) for v in all_cells):
            return INSTANT_TYPE, datetime_meta()
        return "VARCHAR", dict(_EMPTY_COLUMN_META)
    first = present[0]
    if isinstance(first, np.generic):
        first = first.item()
    if any(isinstance(v, (np.ndarray, list, tuple, dict)) for v in present):
        # to_python.m sends array cells as Python lists (via .tolist()).
        # Normalise to ndarray so _infer_duckdb_type handles them correctly.
        if isinstance(first, list) and len(first) > 0:
            first = np.asarray(first)
        return _infer_duckdb_type(first)
    kind = pd.api.types.infer_dtype(present, skipna=True)
    if kind == "integer" and len(present) < len(cells):
        # Integers with blanks: a BIGINT column cannot hold NaN; pandas
        # makes the same call (int column + NaN -> float64).
        return "DOUBLE", {"python_type": "float"}
    if kind in _INFERRED_KIND_TYPES:
        ddb_type, ptype = _INFERRED_KIND_TYPES[kind]
        return ddb_type, {"python_type": ptype}
    if any(isinstance(v, str) for v in present):
        logger.info(
            "infer_data_columns: column %r holds strings mixed with other "
            "values (%s) -> VARCHAR, non-strings stored as text", col_name, kind,
        )
        return "VARCHAR", {"python_type": "str"}
    return _infer_duckdb_type(first)


def _batch_frame(sample: pd.DataFrame, more_frames: list | tuple) -> pd.DataFrame:
    """``sample`` plus every DataFrame of ``more_frames``, as one frame (one
    concat; a column a record lacks reads as missing there)."""
    frames = [f for f in more_frames if isinstance(f, pd.DataFrame) and len(f)]
    if not frames:
        return sample
    t0 = time.perf_counter()
    big = pd.concat([sample, *frames], ignore_index=True, sort=False)
    logger.debug(
        "infer_data_columns: typed %d column(s) over %d record(s), %d row(s) "
        "in %.3fs", len(sample.columns), len(frames) + 1, len(big),
        time.perf_counter() - t0,
    )
    return big


def infer_data_columns(
    sample_value: Any,
    data_col_name: str | None = None,
    *,
    more_frames: list | tuple = (),
    row_list_columns: "list | tuple | set" = (),
    more_values: list | tuple = (),
) -> tuple[dict, dict]:
    """
    From a sample data value, return:
      - data_col_types: dict of {col_name: duckdb_type_str}
      - dtype_meta: metadata dict for round-trip restoration

    ``more_frames``: the rest of a DataFrame batch. Each column's type is
    judged over the sample AND these (see ``_batch_column_type``).

    ``row_list_columns``: columns the caller combined (``on_duplicate=
    "combine"``) into row lists (sciduckdb.row_lists): typed as ``<elem>[]``
    over every element of the batch, i.e. the sample plus ``more_frames``
    (DataFrame mode) or ``more_values`` (the other values of a dict /
    single-column batch). In single-column mode any name marks the one data
    column, whatever the source table called it.

    ``data_col_types`` are the STORED columns: each datetime column is
    followed by its ``<col>__tz`` companion (sciduckdb.datetimes), which
    ``dtype_meta["columns"]`` never lists.
    """
    # DataFrame mode: each DataFrame column → its own DuckDB column.
    # One DuckDB row is stored per DataFrame row; the column type reflects
    # the individual cell value type (independent of table height).
    if isinstance(sample_value, pd.DataFrame):
        col_types = {}
        meta = {
            "mode": "dataframe",
            "columns": {},
            "df_columns": list(sample_value.columns),
        }
        batch = _batch_frame(sample_value, more_frames)
        for col_name in sample_value.columns:
            if len(batch) == 0:
                ddb_type = "VARCHAR"
                col_meta = {"python_type": "str"}
            elif col_name in row_list_columns:
                ddb_type, col_meta = row_list_column_type(
                    batch[col_name].tolist(), col_name
                )
            else:
                ddb_type, col_meta = _batch_column_type(batch[col_name], col_name)
            col_types[col_name] = ddb_type
            meta["columns"][col_name] = col_meta
        return storage_column_types(col_types, meta), meta

    # Dict mode: each key → its own DuckDB column (nested dicts are flattened)
    if isinstance(sample_value, dict):
        has_nested = any(isinstance(v, dict) for v in sample_value.values())
        if has_nested:
            flat, path_map = _flatten_dict(sample_value)
        else:
            flat = sample_value
            path_map = {k: [k] for k in sample_value}
        col_types = {}
        meta = {"mode": "multi_column", "columns": {}}
        if has_nested:
            meta["nested"] = True
            meta["path_map"] = path_map
        for col_name, val in flat.items():
            # Unwrap length-1 arrays to scalars before type inference
            if isinstance(val, np.ndarray) and val.size == 1:
                val = val.item()
            if col_name in row_list_columns and not has_nested:
                ddb_type, col_meta = row_list_column_type(
                    [val] + [v.get(col_name) for v in more_values if isinstance(v, dict)],
                    col_name,
                )
            else:
                ddb_type, col_meta = _infer_duckdb_type(val)
            col_types[col_name] = ddb_type
            meta["columns"][col_name] = col_meta
        return storage_column_types(col_types, meta), meta

    # Single-column mode — use provided name or default to "value"
    col_name = data_col_name or "value"
    if row_list_columns:
        ddb_type, col_meta = row_list_column_type(
            [sample_value, *more_values], col_name
        )
    else:
        ddb_type, col_meta = _infer_duckdb_type(sample_value)
    meta = {"mode": "single_column", "columns": {col_name: col_meta}}
    return storage_column_types({col_name: ddb_type}, meta), meta


# DuckDB type categories used by _storage_signature.  Records can only share a
# column if their values reduce to the same (category, array-depth) signature;
# this is intentionally coarse so benign coercions (BIGINT -> DOUBLE) are
# allowed while shape changes (scalar vs vector) and category changes
# (DOUBLE vs VARCHAR) are rejected.
_NUMERIC_DDB_TYPES = {
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
}
# Temporal types are each their own category. One shared "temporal" bucket
# made column_type_change call TIMESTAMP vs TIMESTAMP_NS (or even INTERVAL)
# "same", letting DuckDB cast a save into a column whose values mean
# something else (datetime round-trip work, 2026-10-22).
_TEMPORAL_DDB_TYPES = {"DATE", "TIME", "TIMESTAMP", INSTANT_TYPE, "TIMESTAMPTZ", "INTERVAL"}


def _storage_signature(ddb_type: str) -> tuple[str, int]:
    """Reduce a DuckDB column type string to (base_category, array_depth).

    array_depth counts trailing ``[]`` (0 = scalar, 1 = vector, 2 = matrix).
    base_category is a coarse bucket so that, e.g., ``BIGINT`` and ``DOUBLE``
    are both ``numeric`` (DuckDB coerces them into one column) but ``DOUBLE``
    and ``VARCHAR`` differ.  This is the unit of comparison for deciding
    whether two records can be stored in the same batch column.
    """
    t = ddb_type.strip().upper()
    depth = 0
    while t.endswith("[]"):
        depth += 1
        t = t[:-2].strip()
    if t in _NUMERIC_DDB_TYPES or t.startswith("DECIMAL"):
        category = "numeric"
    elif t == "VARCHAR":
        category = "string"
    elif t == "JSON":
        category = "json"
    elif t in _TEMPORAL_DDB_TYPES:
        category = "temporal:" + t
    else:
        category = t
    return (category, depth)


_INTEGER_DDB_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
}
_FLOAT_DDB_TYPES = {"FLOAT", "REAL", "DOUBLE"}


def column_type_change(stored_type: str, incoming_type: str) -> str:
    """What a save of ``incoming_type`` values does to a stored column of
    ``stored_type`` that already holds values — the one owner of the rule.

    - ``"same"``: the values fit as they are (equal types, or one numeric
      kind into a wider one: BIGINT values into DOUBLE).
    - ``"widen"``: an integer column receives fractions; the column becomes
      DOUBLE (user decision D1, 2026-10-22: no stored number changes value).
    - ``"as_text"``: a VARCHAR column receives numbers; they are stored as
      text (user decision D2, 2026-10-22).
    - ``"refuse"``: anything else (text into a numeric column, scalar vs
      array, temporal vs number). The save is refused, nothing written.

    A stored column that is still EMPTY takes any type; that check needs the
    data, so it is the caller's (scidb ``_reconcile_column_types``).
    """
    s = stored_type.strip().upper()
    i = incoming_type.strip().upper()
    if s == i:
        return "same"
    s_cat, s_depth = _storage_signature(s)
    i_cat, i_depth = _storage_signature(i)
    if s_depth != i_depth:
        return "refuse"
    if s_depth == 0 and s in _INTEGER_DDB_TYPES and (
        i in _FLOAT_DDB_TYPES or i.startswith("DECIMAL")
    ):
        return "widen"
    if s_cat == i_cat:
        return "same"
    if s_depth == 0 and s_cat == "string" and i_cat == "numeric":
        return "as_text"
    return "refuse"


def record_schema_mismatch(
    ref_col_types: dict, rec_col_types: dict, *, row_list_columns=()
) -> str | None:
    """Return a human-readable reason a record can't join the batch, or None.

    A record "fits" the batch schema when it has exactly the reference column
    set and every column's storage signature matches.  This is the predicate
    used by save_batch to skip (with a warning) records that would otherwise
    abort the atomic batch insert:

      * empty/partial dicts  -> missing keys
      * unexpected dict keys -> extra keys
      * a scalar where the column stores a vector (or vice versa),
        or a string where the column is numeric -> signature mismatch

    A ``row_list_columns`` column (sciduckdb.row_lists) takes a scalar or a
    flat list alike: its element types were judged over the whole batch.
    """
    ref_keys = set(ref_col_types)
    rec_keys = set(rec_col_types)
    if ref_keys != rec_keys:
        missing = sorted(ref_keys - rec_keys)
        extra = sorted(rec_keys - ref_keys)
        parts = []
        if missing:
            parts.append(f"missing keys {missing}")
        if extra:
            parts.append(f"unexpected keys {extra}")
        if not parts:
            parts.append("key set differs from batch schema")
        return "; ".join(parts)
    for col in ref_col_types:
        if col in row_list_columns:
            if _storage_signature(rec_col_types[col])[1] > 1:
                return (
                    f"column '{col}' is a row list of single values but the "
                    f"record value is {rec_col_types[col]}"
                )
            continue
        ref_sig = _storage_signature(ref_col_types[col])
        rec_sig = _storage_signature(rec_col_types[col])
        if ref_sig != rec_sig:
            return (
                f"column '{col}' shape/type mismatch: batch column is "
                f"{ref_col_types[col]} but record value is {rec_col_types[col]}"
            )
    return None


def dataframe_to_storage_rows(df: pd.DataFrame, dtype_meta: dict) -> list:
    """Convert a DataFrame to a list of per-row storage values.

    Returns a list of lists: one inner list per DataFrame row, each containing
    one storage-ready value per STORED column (``storage_column_types``
    order: a datetime column's value is followed by its zone label).
    """
    col_metas = dtype_meta["columns"]
    rows = []
    for i in range(len(df)):
        row = []
        for col, col_meta in col_metas.items():
            cell_val = df[col].iloc[i]
            if is_datetime_meta(col_meta):
                row.extend(datetime_to_storage(cell_val))
                continue
            if isinstance(cell_val, np.generic):
                cell_val = cell_val.item()
            # to_python.m sends array cells as Python lists (via .tolist()).
            # Normalise to ndarray so _python_to_storage handles them correctly.
            if isinstance(cell_val, list) and len(cell_val) > 0:
                cell_val = np.asarray(cell_val)
            row.append(_python_to_storage(cell_val, col_meta))
        rows.append(row)
    return rows


def bulk_df_to_storage_rows(df_list: list, record_ids: list, dtype_meta: dict) -> list:
    """Bulk convert N DataFrames to (record_id, ...storage_values) rows.

    Equivalent to calling dataframe_to_storage_rows N times and assembling
    (record_id, ...) tuples, but processes each column as a whole to avoid
    O(N×C) per-cell pandas iloc overhead.

    Falls back to the per-row path when DataFrame schemas differ.
    """
    if not df_list:
        return []

    col_metas = dtype_meta["columns"]
    first_cols = list(df_list[0].columns)

    # Fall back to per-row path if schemas differ (shouldn't happen in normal use).
    if not all(list(df.columns) == first_cols for df in df_list):
        rows: list = []
        for rid, df in zip(record_ids, df_list, strict=False):
            for storage_row in dataframe_to_storage_rows(df, dtype_meta):
                rows.append((rid,) + tuple(storage_row))
        return rows

    # Build flat record_id list: one entry per storage row (multi-row records
    # contribute len(df) entries, typical 1-row records contribute 1).
    expanded_rids = []
    for rid, df in zip(record_ids, df_list, strict=False):
        expanded_rids.extend([rid] * len(df))

    # Concat once so column operations don't cross DataFrame boundaries.
    big_df = pd.concat(df_list, ignore_index=True)

    # Build per-column storage arrays using column-level operations.
    col_arrays: dict = {}
    nan_counts: dict = {}
    dt_counts: dict = {}
    for col, col_meta in col_metas.items():
        ptype = col_meta.get("python_type", "")
        raw = big_df[col]

        if ptype == DATETIME_PTYPE:
            instants, labels, dt_counts[col] = datetime_series_to_storage(raw.tolist())
            col_arrays[col] = instants
            col_arrays[tz_column_name(col)] = labels
        elif ptype == "ndarray":
            vals = raw.to_numpy()
            n_nan = count_nan_array_elements(vals)
            if n_nan:
                nan_counts[col] = n_nan
            col_arrays[col] = [
                v.tolist()
                if isinstance(v, np.ndarray)
                else (v if isinstance(v, list) else [v])
                for v in vals
            ]
        elif ptype == ROW_LIST_PTYPE:
            col_arrays[col] = [row_list_to_storage(v, col_meta) for v in raw.tolist()]
        elif ptype in ("dict", "json_fallback"):
            col_arrays[col] = [json.dumps(_convert_for_json(v)) for v in raw.to_numpy()]
        elif ptype == "list":
            if col_meta.get("contains_ndarray"):
                col_arrays[col] = [
                    [e.tolist() if isinstance(e, np.ndarray) else e for e in v]
                    for v in raw.to_numpy()
                ]
            else:
                col_arrays[col] = raw.tolist()
        elif ptype == "str":
            col_arrays[col] = [_str_to_storage(v) for v in raw.tolist()]
        else:
            # Scalar types (float, int, bool …): tolist() converts numpy
            # scalars to Python builtins, which is what DuckDB expects.
            col_arrays[col] = raw.tolist()

    if nan_counts:
        # DuckDB's pandas scanner stores these as NULL; the loader restores
        # them as NaN (array_from_storage). Said once per batch so a
        # NaN-vs-NULL question is answerable from the log.
        logger.info(
            "array column(s) contain NaN — stored as NULL, reload as NaN: %s",
            nan_counts,
        )
    if dt_counts:
        mixed = [c for c, n in dt_counts.items() if n["naive"] and n["aware"]]
        logger.log(
            logging.INFO if mixed else logging.DEBUG,
            "datetime column(s) stored as UTC TIMESTAMP_NS + zone column: %s%s",
            dt_counts,
            f" — {mixed} mix naive and tz-aware values (NULL zone = naive)"
            if mixed else "",
        )

    cols_in_order = storage_column_names(dtype_meta)
    n = len(big_df)
    return [
        (expanded_rids[i],) + tuple(col_arrays[col][i] for col in cols_in_order)
        for i in range(n)
    ]


def value_to_storage_row(value: Any, dtype_meta: dict) -> list:
    """Convert a data value to a list of storage-ready column values.

    For DataFrames use dataframe_to_storage_rows() instead. One value per
    STORED column: a datetime is followed by its zone label.
    """
    mode = dtype_meta.get("mode", "single_column")
    col_metas = dtype_meta["columns"]

    def cell(val, col_meta) -> list:
        if is_datetime_meta(col_meta):
            return list(datetime_to_storage(val))
        return [_python_to_storage(val, col_meta)]

    if mode == "multi_column":
        if dtype_meta.get("nested"):
            flat, _ = _flatten_dict(value)
        else:
            flat = value
        return [v for col in col_metas for v in cell(flat[col], col_metas[col])]
    else:
        # Single column — get the one key (could be "value" or a named column)
        col_name = next(iter(col_metas))
        col_meta = col_metas[col_name]
        return cell(value, col_meta)

