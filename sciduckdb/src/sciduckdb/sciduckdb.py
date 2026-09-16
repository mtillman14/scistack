"""
SciDuck — A thin DuckDB layer for managing versioned scientific data.

Each variable is stored in its own table. Variables are associated with a
hierarchical dataset schema (e.g. subject → session → trial) and can be
saved at any level of that hierarchy. Multiple versions of each variable
are supported natively.

All data — including arrays — is stored in queryable DuckDB types (LIST,
nested LIST, JSON) so the database can be inspected with DBeaver or any
DuckDB-compatible viewer.
"""

import datetime
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger("sciduck")


def _truncate_sql(sql: str, limit: int = 200) -> str:
    """Collapse whitespace and cap length for log lines."""
    flat = " ".join(sql.split())
    if len(flat) > limit:
        return flat[:limit] + "...(truncated)"
    return flat


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

    # --- datetime ---
    if isinstance(value, (datetime.datetime, pd.Timestamp)):
        meta["python_type"] = "datetime"
        return "TIMESTAMP", meta
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


def _python_to_storage(value: Any, meta: dict) -> Any:
    """Convert a Python value to its DuckDB-storable form."""
    ptype = meta.get("python_type", "")

    # _infer_data_columns unwraps length-1 arrays to scalars when it picks the
    # column type, e.g. {"x": np.array([1.0])} -> DOUBLE column with
    # python_type="float". Mirror that unwrap here so the stored value is a
    # scalar matching the column; otherwise the row carries a DOUBLE[] into a
    # DOUBLE column and DuckDB rejects the cast (DOUBLE[] -> DOUBLE).
    if ptype in ("float", "int", "bool", "str"):
        if isinstance(value, np.ndarray) and value.size == 1:
            value = value.item()
        elif isinstance(value, np.generic):
            value = value.item()

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
    # function already upcast (SciDuck.load runs _restore_types AND then
    # _storage_to_python on the same cell, so restoration must be idempotent).
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


def _storage_to_python(value: Any, meta: dict) -> Any:
    """Restore a stored DuckDB value back to its original Python type."""
    ptype = meta.get("python_type", "")

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

    return value


def _storage_to_python_column(series: "pd.Series", meta: dict) -> "pd.Series":
    """Vectorized column-level dispatch of _storage_to_python.

    Applied once per column in bulk loads instead of once per cell (N records ×
    M columns calls vs N×M calls for the per-element path).  Pass-through types
    (float, int, bool, str) are returned unchanged — DuckDB already emits the
    right pandas dtype for those.  Complex types use pd.Series.apply, which is
    still faster than an explicit Python for-loop.
    """
    ptype = meta.get("python_type", "")

    # Scalar types: DuckDB already returns the right pandas dtype — no-op.
    if ptype in ("float", "int", "bool", "str", ""):
        return series

    # JSON blob types: decode string once per cell, but at column granularity.
    if ptype in ("dict", "json_fallback"):
        return series.apply(lambda v: json.loads(v) if isinstance(v, str) else v)

    # All remaining types (ndarray, ndarray_json, list, …): delegate per-element.
    # NULL list elements are restored as NaN inside _storage_to_python; the
    # caller aggregates count_null_list_elements() into one INFO line per load.
    return series.apply(lambda v: _storage_to_python(v, meta))


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


def _unflatten_dict(flat, path_map):
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


def _infer_data_columns(
    sample_value: Any, data_col_name: str | None = None
) -> tuple[dict, dict]:
    """
    From a sample data value, return:
      - data_col_types: dict of {col_name: duckdb_type_str}
      - dtype_meta: metadata dict for round-trip restoration
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
        for col_name in sample_value.columns:
            col_series = sample_value[col_name]
            if len(sample_value) == 0:
                ddb_type = "VARCHAR"
                col_meta = {"python_type": "str"}
            else:
                cell_val = col_series.iloc[0]
                if isinstance(cell_val, np.generic):
                    cell_val = cell_val.item()
                # to_python.m sends array cells as Python lists (via .tolist()).
                # Normalise to ndarray so _infer_duckdb_type handles them correctly.
                if isinstance(cell_val, list) and len(cell_val) > 0:
                    cell_val = np.asarray(cell_val)
                ddb_type, col_meta = _infer_duckdb_type(cell_val)
            col_types[col_name] = ddb_type
            meta["columns"][col_name] = col_meta
        return col_types, meta

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
            ddb_type, col_meta = _infer_duckdb_type(val)
            col_types[col_name] = ddb_type
            meta["columns"][col_name] = col_meta
        return col_types, meta

    # Single-column mode — use provided name or default to "value"
    col_name = data_col_name or "value"
    ddb_type, col_meta = _infer_duckdb_type(sample_value)
    meta = {"mode": "single_column", "columns": {col_name: col_meta}}
    return {col_name: ddb_type}, meta


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
_TEMPORAL_DDB_TYPES = {"DATE", "TIME", "TIMESTAMP", "INTERVAL"}


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
        category = "temporal"
    else:
        category = t
    return (category, depth)


def _record_schema_mismatch(ref_col_types: dict, rec_col_types: dict) -> str | None:
    """Return a human-readable reason a record can't join the batch, or None.

    A record "fits" the batch schema when it has exactly the reference column
    set and every column's storage signature matches.  This is the predicate
    used by save_batch to skip (with a warning) records that would otherwise
    abort the atomic batch insert:

      * empty/partial dicts  -> missing keys
      * unexpected dict keys -> extra keys
      * a scalar where the column stores a vector (or vice versa),
        or a string where the column is numeric -> signature mismatch
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
        ref_sig = _storage_signature(ref_col_types[col])
        rec_sig = _storage_signature(rec_col_types[col])
        if ref_sig != rec_sig:
            return (
                f"column '{col}' shape/type mismatch: batch column is "
                f"{ref_col_types[col]} but record value is {rec_col_types[col]}"
            )
    return None


def _dataframe_to_storage_rows(df: pd.DataFrame, dtype_meta: dict) -> list:
    """Convert a DataFrame to a list of per-row storage values.

    Returns a list of lists: one inner list per DataFrame row, each containing
    one storage-ready value per column in the order defined by dtype_meta["columns"].
    """
    col_metas = dtype_meta["columns"]
    rows = []
    for i in range(len(df)):
        row = []
        for col, col_meta in col_metas.items():
            cell_val = df[col].iloc[i]
            if isinstance(cell_val, np.generic):
                cell_val = cell_val.item()
            # to_python.m sends array cells as Python lists (via .tolist()).
            # Normalise to ndarray so _python_to_storage handles them correctly.
            if isinstance(cell_val, list) and len(cell_val) > 0:
                cell_val = np.asarray(cell_val)
            row.append(_python_to_storage(cell_val, col_meta))
        rows.append(row)
    return rows


def _bulk_df_to_storage_rows(df_list: list, record_ids: list, dtype_meta: dict) -> list:
    """Bulk convert N DataFrames to (record_id, ...storage_values) rows.

    Equivalent to calling _dataframe_to_storage_rows N times and assembling
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
            for storage_row in _dataframe_to_storage_rows(df, dtype_meta):
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
    for col, col_meta in col_metas.items():
        ptype = col_meta.get("python_type", "")
        raw = big_df[col]

        if ptype == "ndarray":
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
        else:
            # Scalar types (float, int, str, bool …): tolist() converts numpy
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

    cols_in_order = list(col_metas.keys())
    n = len(big_df)
    return [
        (expanded_rids[i],) + tuple(col_arrays[col][i] for col in cols_in_order)
        for i in range(n)
    ]


def _value_to_storage_row(value: Any, dtype_meta: dict) -> list:
    """Convert a data value to a list of storage-ready column values.

    For DataFrames use _dataframe_to_storage_rows() instead.
    """
    mode = dtype_meta.get("mode", "single_column")
    col_metas = dtype_meta["columns"]

    if mode == "multi_column":
        if dtype_meta.get("nested"):
            flat, _ = _flatten_dict(value)
        else:
            flat = value
        return [_python_to_storage(flat[col], col_metas[col]) for col in col_metas]
    else:
        # Single column — get the one key (could be "value" or a named column)
        col_name = next(iter(col_metas))
        col_meta = col_metas[col_name]
        return [_python_to_storage(value, col_meta)]


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


def schema_keys_from_db(db_path: str | Path) -> list[str]:
    """Read the dataset schema keys stored in an existing database.

    Opens ``db_path`` read-only, reads the ``_schema`` table's key columns
    (everything except ``schema_id``/``schema_level``, in ordinal order), and
    closes the connection. This lets tools open a database without knowing
    its schema keys in advance (e.g. the ``scidb`` CLI).

    Raises ValueError if the file has no ``_schema`` table (not a
    scidb/sciduckdb database) and whatever duckdb raises if the file does not
    exist or is locked.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = '_schema' "
            "AND column_name NOT IN ('schema_id', 'schema_level') "
            "ORDER BY ordinal_position"
        ).fetchall()
        has_table = (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = '_schema'"
            ).fetchall()[0][0]
            > 0
        )
    finally:
        con.close()
    if not has_table:
        raise ValueError(
            f"{db_path} has no _schema table — not a scidb/sciduckdb database"
        )
    return [r[0] for r in rows]


class SciDuck:
    """
    A thin DuckDB layer for managing versioned, schema-aware scientific data.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB database file.  Use ":memory:" for in-memory.
    dataset_schema : list of str
        Ordered hierarchy, e.g. ["subject", "session", "trial"].
    read_only : bool
        Open the underlying DuckDB connection read-only. No DDL is executed
        (the database must already exist); DuckDB rejects every write on the
        connection, so a read-only SciDuck can never contend for the write
        lock beyond DuckDB's shared read lock.
    """

    def __init__(
        self, db_path: str | Path, dataset_schema: list[str], read_only: bool = False
    ):
        self.db_path = str(db_path)
        self.dataset_schema = list(dataset_schema)
        self.read_only = bool(read_only)
        self._lock = threading.Lock()
        # Thread id currently holding an open BEGIN...COMMIT/ROLLBACK transaction
        # on `self.con`, or None. DuckDB connections have exactly one transaction
        # context; `_lock` only serializes individual statements (see `_execute`
        # docstring), so a statement from another thread can still land *inside*
        # this transaction between `_begin()` and `_commit()`. Tracking the owner
        # lets us log that interleaving instead of just seeing the downstream
        # "Current transaction is aborted" failure with no context.
        self._tx_owner: int | None = None
        logger.debug(
            "DuckDB lock ACQUIRED (read_only=%s): %s", self.read_only, self.db_path
        )
        self.con = duckdb.connect(self.db_path, read_only=self.read_only)
        if self.read_only:
            self._validate_schema_columns()
        else:
            self._init_metadata_tables()

    # ------------------------------------------------------------------
    # Thin internal interface (future backend swap point)
    # ------------------------------------------------------------------

    def _recover_from_autocommit_failure(self) -> None:
        """Best-effort ROLLBACK after a failed autocommit statement.

        DuckDB requires an explicit ROLLBACK once any statement fails —
        even a plain autocommit call outside an explicit BEGIN/COMMIT —
        or every later statement on this connection fails with "Current
        transaction is aborted", regardless of what it does. Observed in
        production: a migration's ``ALTER TABLE ... ADD COLUMN`` failed
        with CatalogException (column already existed) on every restart;
        the exception was caught by the caller, but the connection stayed
        aborted, so the next *unguarded* statement — a completely
        unrelated ``CREATE TABLE IF NOT EXISTS`` — failed too, and the
        shared connection was stuck that way for the rest of the process.
        Must be called only while still holding ``_lock``, and only skips
        the rollback when an explicit transaction is open (``_tx_owner``
        set) — that transaction's rollback is the owning caller's call,
        not ours to issue out from under them.
        """
        if self._tx_owner is not None:
            return
        try:
            self.con.execute("ROLLBACK")
            logger.debug("auto-ROLLBACK issued after autocommit statement failure")
        except Exception:
            pass

    def _execute(self, sql: str, params=None):
        # NOTE: DuckDB's Python connection returns itself from execute(), so
        # execute() and fetchXxx() share the same connection state.  All callers
        # that fetch results must hold _lock for the entire execute+fetch sequence.
        # Use _fetchall / _fetchone / _fetchdf for queries that return rows; call
        # _execute directly (under _lock) only for DDL/DML that needs no fetch.
        #
        # NEVER write `_execute(...).fetchall()` (or .fetchone()/.fetchdf()).
        # _execute releases _lock when this `with` block exits, so the fetch runs
        # UNPROTECTED: a concurrent execute() on the shared connection tears the
        # pending result down mid-fetch and DuckDB raises
        #   INTERNAL Error: Attempted to dereference shared_ptr that is NULL!
        # This is intermittent and load-dependent — it crashed a GUI graph build
        # on 2026-08-25 when GET /path-inputs and GET /pipeline overlapped by 1ms
        # on two FastAPI threadpool threads. test_no_unlocked_fetch_after_execute
        # guards the pattern repo-wide.
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_execute thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s sql=%s",
                thread, waited, self._tx_owner, foreign_tx, _truncate_sql(sql),
            )
            try:
                if params:
                    return self.con.execute(sql, params)
                return self.con.execute(sql)
            except Exception:
                logger.exception(
                    "_execute FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _fetch_table(self, sql: str, params=None) -> tuple[list[str], list[tuple]]:
        """``(column_names, rows)`` with SQL NULL preserved as ``None``.

        ``_fetchdf`` goes through pandas, which has no NULL for a float column:
        every NULL DOUBLE arrives as NaN and becomes indistinguishable from a
        stored NaN. That is fine for computation and wrong for a diagnostic —
        the `scidb sql` renderer printed `nan` for a NULL and sent an
        investigation chasing a save-path difference that did not exist
        (2026-09-15). Callers that must report what the database actually holds
        use this instead.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetch_table thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s sql=%s",
                thread, waited, self._tx_owner, foreign_tx, _truncate_sql(sql),
            )
            try:
                cur = self.con.execute(sql, params) if params else self.con.execute(sql)
                columns = [d[0] for d in (cur.description or [])]
                return columns, cur.fetchall()
            except Exception:
                logger.exception(
                    "_fetch_table FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _executemany(self, sql: str, params_list):
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_executemany thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s "
                "rows=%d sql=%s",
                thread, waited, self._tx_owner, foreign_tx, len(params_list),
                _truncate_sql(sql),
            )
            try:
                return self.con.executemany(sql, params_list)
            except Exception:
                logger.exception(
                    "_executemany FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _bulk_insert(self, table: str, columns, rows, conflict_cols=None) -> None:
        """Insert many rows with a single vectorized ``INSERT ... SELECT``.

        DuckDB's ``executemany`` re-runs the prepared single-row statement once
        per row; against a PRIMARY-KEY / indexed table that is pathologically
        slow (per-row index probe + maintenance), e.g. ~497s for 8k rows into
        ``_record``. This registers the rows as one DataFrame and issues a single
        bulk insert instead (sub-second for the same data).

        ``rows`` is an iterable of value tuples in ``columns`` order. When
        ``conflict_cols`` is given, appends ``ON CONFLICT (...) DO NOTHING`` so
        the insert stays idempotent. Safe to call inside an open transaction
        (the register/insert/unregister run on the same connection).
        """
        rows = list(rows)
        if not rows:
            return
        columns = list(columns)
        df = pd.DataFrame(rows, columns=columns)
        col_str = ", ".join(f'"{c}"' for c in columns)
        sql = f'INSERT INTO "{table}" ({col_str}) SELECT * FROM _bulk_insert_df'
        if conflict_cols:
            conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)
            sql += f" ON CONFLICT ({conflict_str}) DO NOTHING"
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_bulk_insert thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s "
                "table=%s rows=%d",
                thread, waited, self._tx_owner, foreign_tx, table, len(rows),
            )
            self.con.register("_bulk_insert_df", df)
            try:
                self.con.execute(sql)
            except Exception:
                logger.exception(
                    "_bulk_insert FAILED thread=%d tx_owner=%s foreign_tx=%s table=%s",
                    thread, self._tx_owner, foreign_tx, table,
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self.con.unregister("_bulk_insert_df")

    def _begin(self):
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            if self._tx_owner is not None and self._tx_owner != thread:
                logger.warning(
                    "_begin thread=%d waited=%.4fs OVERLAPPING existing "
                    "tx_owner=%d — this connection allows only one open "
                    "transaction; the new BEGIN will nest into the same "
                    "connection context",
                    thread, waited, self._tx_owner,
                )
            else:
                logger.debug("_begin thread=%d waited=%.4fs", thread, waited)
            self.con.execute("BEGIN TRANSACTION")
            self._tx_owner = thread

    def _commit(self):
        thread = threading.get_ident()
        with self._lock:
            logger.debug(
                "_commit thread=%d tx_owner=%s", thread, self._tx_owner,
            )
            if self._tx_owner is not None and self._tx_owner != thread:
                logger.warning(
                    "_commit thread=%d committing a transaction opened by "
                    "thread=%d — statements from either thread may have been "
                    "interleaved into it",
                    thread, self._tx_owner,
                )
            try:
                self.con.execute("COMMIT")
            except Exception:
                logger.exception(
                    "_commit FAILED thread=%d tx_owner=%s", thread, self._tx_owner,
                )
                raise
            finally:
                self._tx_owner = None

    def _rollback(self):
        thread = threading.get_ident()
        with self._lock:
            logger.debug(
                "_rollback thread=%d tx_owner=%s", thread, self._tx_owner,
            )
            try:
                self.con.execute("ROLLBACK")
            finally:
                self._tx_owner = None

    def _fetchall(self, sql: str, params=None) -> list:
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetchall thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s sql=%s",
                thread, waited, self._tx_owner, foreign_tx, _truncate_sql(sql),
            )
            try:
                if params:
                    return self.con.execute(sql, params).fetchall()
                return self.con.execute(sql).fetchall()
            except Exception:
                logger.exception(
                    "_fetchall FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def fetchall(self, sql: str, params=None) -> list:
        """Public alias for _fetchall — accessible from MATLAB (underscore methods are not)."""
        return self._fetchall(sql, params)

    def _fetchall_with_frame(
        self, sql: str, frame, *, view: str = "_query_frame", params=None
    ) -> list:
        """``_fetchall`` with a pandas DataFrame visible to the query as ``view``.

        The read-side twin of ``_bulk_insert``'s register/unregister idiom: the
        frame is registered under the lock, the query runs against it, and it is
        unregistered in ``finally`` so a failing query cannot leak a view into
        the next caller's namespace.

        Exists so a caller can hand DuckDB a small mapping it holds in memory —
        which rows belong to which group, say — and have DuckDB do the heavy
        reduction against the stored columns by joining to it. The alternative,
        pulling the stored columns OUT to reduce them next to the mapping, is
        exactly the pattern that boxed 174 million floats into Python on
        2026-09-13 (scistackplot's ``.claude/plan-duckdb-native-reduction.md``).

        ``view`` is a plain identifier; callers that could run concurrently
        against one connection should pick distinct names, though the lock
        already serialises them.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetchall_with_frame thread=%d waited=%.4fs tx_owner=%s "
                "foreign_tx=%s view=%s rows=%d sql=%s",
                thread, waited, self._tx_owner, foreign_tx, view, len(frame),
                _truncate_sql(sql),
            )
            self.con.register(view, frame)
            try:
                if params:
                    return self.con.execute(sql, params).fetchall()
                return self.con.execute(sql).fetchall()
            except Exception:
                logger.exception(
                    "_fetchall_with_frame FAILED thread=%d tx_owner=%s foreign_tx=%s "
                    "view=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, view, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self.con.unregister(view)

    def _fetchdf_with_frame(
        self, sql: str, frame, *, view: str = "_query_frame", params=None
    ) -> "pd.DataFrame":
        """``_fetchall_with_frame``, returning a DataFrame instead of rows.

        **The difference is the whole point, not a convenience.** ``fetchall()``
        materialises one Python object per value — a boxed float per sample —
        which is the cost this exists to avoid: on 2026-09-13, exploding 174
        million samples into Python took 153 s
        (``.claude/plan-duckdb-native-reduction.md`` §2). ``.df()`` goes through
        Arrow into numpy, so a numeric column arrives as one contiguous buffer
        and never passes through a Python float at all. Use this for any query
        whose result has many ROWS; ``_fetchall`` remains right for results with
        few rows (metadata, aggregates).
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetchdf_with_frame thread=%d waited=%.4fs tx_owner=%s "
                "foreign_tx=%s view=%s rows=%d sql=%s",
                thread, waited, self._tx_owner, foreign_tx, view, len(frame),
                _truncate_sql(sql),
            )
            self.con.register(view, frame)
            try:
                if params:
                    return self.con.execute(sql, params).df()
                return self.con.execute(sql).df()
            except Exception:
                logger.exception(
                    "_fetchdf_with_frame FAILED thread=%d tx_owner=%s foreign_tx=%s "
                    "view=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, view, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self.con.unregister(view)

    def _fetchone(self, sql: str, params=None):
        """First result row, or ``None`` — the locked counterpart to
        ``_fetchall`` for single-row lookups.

        Exists so callers never have to write ``_execute(...).fetchone()``,
        which fetches after the lock has already been released. See the NOTE
        on ``_execute``.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetchone thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s sql=%s",
                thread, waited, self._tx_owner, foreign_tx, _truncate_sql(sql),
            )
            try:
                if params:
                    return self.con.execute(sql, params).fetchone()
                return self.con.execute(sql).fetchone()
            except Exception:
                logger.exception(
                    "_fetchone FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def fetchone(self, sql: str, params=None):
        """Public alias for _fetchone — accessible from MATLAB (underscore methods are not)."""
        return self._fetchone(sql, params)

    def _fetchdf(self, sql: str, params=None) -> pd.DataFrame:
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_fetchdf thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s sql=%s",
                thread, waited, self._tx_owner, foreign_tx, _truncate_sql(sql),
            )
            try:
                if params:
                    return self.con.execute(sql, params).fetchdf()
                return self.con.execute(sql).fetchdf()
            except Exception:
                logger.exception(
                    "_fetchdf FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _table_exists(self, name: str) -> bool:
        rows = self._fetchall(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        )
        return rows[0][0] > 0

    # ------------------------------------------------------------------
    # Metadata table creation
    # ------------------------------------------------------------------

    def _init_metadata_tables(self):
        # --- _schema ---
        schema_cols = ", ".join(f'"{s}" VARCHAR' for s in self.dataset_schema)
        self._execute(f"""
            CREATE TABLE IF NOT EXISTS _schema (
                schema_id INTEGER PRIMARY KEY,
                schema_level VARCHAR NOT NULL,
                {schema_cols}
            )
        """)
        # Create a sequence for schema_id if it doesn't exist
        try:
            self._execute("CREATE SEQUENCE IF NOT EXISTS _schema_id_seq START 1")
        except Exception:
            pass  # sequence already exists

        # --- _variables ---
        self._execute("""
            CREATE TABLE IF NOT EXISTS _variables (
                variable_name VARCHAR PRIMARY KEY,
                schema_level VARCHAR NOT NULL,
                dtype VARCHAR,
                created_at TIMESTAMP DEFAULT current_timestamp,
                description VARCHAR DEFAULT ''
            )
        """)

        # --- _variable_groups ---
        self._execute("""
            CREATE TABLE IF NOT EXISTS _variable_groups (
                group_name VARCHAR NOT NULL,
                variable_name VARCHAR NOT NULL,
                PRIMARY KEY (group_name, variable_name)
            )
        """)

        # Validate schema consistency if _schema already has data
        if self._fetchall("SELECT COUNT(*) FROM _schema")[0][0] > 0:
            self._validate_schema_columns()

    def _validate_schema_columns(self):
        """Check that the existing _schema columns match dataset_schema.

        Read-only companion to _init_metadata_tables: runs the same
        consistency check without any DDL, so it is safe on a read-only
        connection (where the tables must already exist).
        """
        if not self._table_exists("_schema"):
            if self.read_only:
                raise ValueError(
                    f"{self.db_path} has no _schema table — not a scidb/sciduckdb "
                    f"database (read-only open cannot create it)"
                )
            return
        existing_cols = [
            row[0]
            for row in self._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = '_schema' "
                "AND column_name NOT IN ('schema_id', 'schema_level') "
                "ORDER BY ordinal_position"
            )
        ]
        if existing_cols != self.dataset_schema:
            raise ValueError(
                f"Database schema mismatch. "
                f"Existing: {existing_cols}, Provided: {self.dataset_schema}"
            )

    # ------------------------------------------------------------------
    # Schema entry management
    # ------------------------------------------------------------------

    def _schema_key_columns(self, schema_level: str) -> list[str]:
        """Return schema columns from the top down to (and including) schema_level."""
        idx = self.dataset_schema.index(schema_level)
        return self.dataset_schema[: idx + 1]

    def _get_or_create_schema_id(self, schema_level: str, key_values: dict) -> int:
        """Look up or insert a row in _schema.  Return the schema_id."""
        key_cols = [k for k in self.dataset_schema if k in key_values]

        # Build WHERE clause
        conditions = []
        params = [schema_level]
        for col in key_cols:
            conditions.append(f'"{col}" = ?')
            params.append(_schema_str(key_values[col]))
        # Columns above the level that should be NULL are implicit —
        # but to be safe, also require NULLs for levels below.
        for col in self.dataset_schema:
            if col not in key_cols:
                conditions.append(f'"{col}" IS NULL')

        where = " AND ".join(conditions)
        rows = self._fetchall(
            f"SELECT schema_id FROM _schema WHERE schema_level = ? AND {where}",
            params,
        )
        if rows:
            return rows[0][0]

        # Insert new entry — use MAX+1 for consistency with batch path
        new_id = self._fetchall("SELECT COALESCE(MAX(schema_id), 0) + 1 FROM _schema")[
            0
        ][0]
        col_names = ["schema_id", "schema_level"] + key_cols
        placeholders = ", ".join(["?"] * len(col_names))
        col_str = ", ".join(f'"{c}"' for c in col_names)
        values = [new_id, schema_level] + [_schema_str(key_values[c]) for c in key_cols]
        self._execute(
            f"INSERT INTO _schema ({col_str}) VALUES ({placeholders})", values
        )
        return new_id

    def batch_get_or_create_schema_ids(
        self,
        combos: dict,  # {(schema_level, key_tuple): key_values_dict}
    ) -> dict:
        """
        Batch-resolve schema IDs for multiple (schema_level, key_values) combos.

        Instead of N individual SELECT+INSERT round-trips, does:
        1. One SELECT to fetch all existing schema entries
        2. Batch INSERT for missing entries
        3. One SELECT to get IDs for newly inserted entries

        Args:
            combos: dict mapping (schema_level, key_tuple) -> key_values dict

        Returns:
            dict mapping (schema_level, key_tuple) -> schema_id
        """
        if not combos:
            return {}

        result = {}

        # Group combos by (schema_level, key set) for efficient querying
        by_level_and_keys = {}
        for (schema_level, key_tuple), key_values in combos.items():
            group_key = (schema_level, frozenset(key_values.keys()))
            by_level_and_keys.setdefault(group_key, []).append(
                ((schema_level, key_tuple), key_values)
            )

        for (schema_level, key_set), entries in by_level_and_keys.items():
            key_cols = [k for k in self.dataset_schema if k in key_set]
            null_cols = [c for c in self.dataset_schema if c not in key_cols]

            # Build a single query to find all existing matches at this level
            # We fetch all rows for this schema_level and match in Python
            null_conditions = " AND ".join(f'"{col}" IS NULL' for col in null_cols)
            where_clause = "schema_level = ?"
            if null_conditions:
                where_clause += f" AND {null_conditions}"

            col_select = ", ".join(f'"{c}"' for c in key_cols)
            rows = self._fetchall(
                f"SELECT schema_id, {col_select} FROM _schema WHERE {where_clause}",
                [schema_level],
            )

            # Build lookup: tuple of col values -> schema_id
            existing_lookup = {}
            for row in rows:
                sid = row[0]
                row_key = tuple(
                    _schema_str(v) if v is not None else "" for v in row[1:]
                )
                existing_lookup[row_key] = sid

            # Match entries against existing rows
            missing = []  # [(combo_key, key_values), ...]
            for combo_key, key_values in entries:
                match_key = tuple(_schema_str(key_values.get(c, "")) for c in key_cols)
                if match_key in existing_lookup:
                    result[combo_key] = existing_lookup[match_key]
                else:
                    missing.append((combo_key, key_values, match_key))

            # Batch insert missing entries
            if missing:
                # Allocate a block of IDs from current max instead of N nextval() calls
                max_row = self._fetchall(
                    "SELECT COALESCE(MAX(schema_id), 0) FROM _schema"
                )
                first_id = max_row[0][0] + 1

                col_names = ["schema_id", "schema_level"] + key_cols

                insert_rows = []
                for idx, (combo_key, key_values, _) in enumerate(missing):
                    new_id = first_id + idx
                    row = [new_id, schema_level] + [
                        _schema_str(key_values[c]) for c in key_cols
                    ]
                    insert_rows.append(row)
                    result[combo_key] = new_id

                self._bulk_insert("_schema", col_names, insert_rows)

        return result

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save(
        self,
        name: str,
        data: Any,
        schema_level: str | None = None,
        description: str = "",
        force: bool = False,
        **schema_keys,
    ):
        """
        Save a variable to the database.

        Parameters
        ----------
        name : str
            Variable name (becomes the table name).
        data : Any
            The data to save.  Can be:
            - pd.DataFrame with schema-level columns (Mode A)
            - Any Python/numpy object + schema_keys kwargs (Mode B, single entry)
            - dict mapping tuples → values (Mode C, batch)
        schema_level : str, optional
            Which schema level to store at.  Defaults to the lowest level.
        description : str
            Optional description for this variable.
        force : bool
            Deprecated, kept for backward compatibility.
        **schema_keys
            Keyword arguments specifying the schema entry for Mode B.
            e.g. subject="S01", session=1, trial=3.
            Note: all schema key values are coerced to strings before storage.
        """
        # --- Determine save mode ---
        data_col_name = None  # Override for single-column name preservation

        # Mode B: single entry via kwargs
        if schema_keys:
            provided_schema_cols = [k for k in self.dataset_schema if k in schema_keys]
            if schema_level is None:
                schema_level = (
                    provided_schema_cols[-1]
                    if provided_schema_cols
                    else self.dataset_schema[-1]
                )
            if schema_level not in self.dataset_schema:
                raise ValueError(
                    f"schema_level '{schema_level}' not in {self.dataset_schema}"
                )
            key_cols = provided_schema_cols
            entries = [
                (
                    {k: schema_keys[k] for k in key_cols},
                    data,
                )
            ]

        else:
            if schema_level is None:
                schema_level = self.dataset_schema[-1]
            if schema_level not in self.dataset_schema:
                raise ValueError(
                    f"schema_level '{schema_level}' not in {self.dataset_schema}"
                )
            key_cols = self._schema_key_columns(schema_level)

            # Mode A: DataFrame with schema columns
            if isinstance(data, pd.DataFrame) and all(
                c in data.columns for c in key_cols
            ):
                entries, data_col_name = self._entries_from_dataframe(
                    data, key_cols, schema_level
                )

            # Mode C: dict with tuple keys
            elif (
                isinstance(data, dict)
                and data
                and isinstance(next(iter(data.keys())), tuple)
            ):
                entries = []
                for key_tuple, value in data.items():
                    if len(key_tuple) != len(key_cols):
                        raise ValueError(
                            f"Key tuple length {len(key_tuple)} != "
                            f"expected {len(key_cols)} for level '{schema_level}'"
                        )
                    key_dict = dict(zip(key_cols, key_tuple, strict=False))
                    entries.append((key_dict, value))

            else:
                raise ValueError(
                    "Cannot determine save mode.  Provide either:\n"
                    "  (A) a DataFrame with schema-level columns,\n"
                    "  (B) schema key kwargs (e.g. subject='S01', session=1), or\n"
                    "  (C) a dict mapping tuples to values."
                )

        # --- Determine column types from the first entry's data ---
        sample_value = entries[0][1]
        data_col_types, dtype_meta = self._infer_data_columns(
            sample_value, data_col_name
        )

        # --- Ensure the variable table exists ---
        is_dataframe = dtype_meta.get("mode") == "dataframe"
        self._ensure_variable_table(
            name, data_col_types, schema_level, is_dataframe=is_dataframe
        )

        # --- Insert rows (INSERT OR REPLACE for "latest wins" semantics) ---
        col_names = ["schema_id"] + list(data_col_types.keys())
        col_str = ", ".join(f'"{c}"' for c in col_names)
        placeholders = ", ".join(["?"] * len(col_names))

        for key_dict, value in entries:
            schema_id = self._get_or_create_schema_id(schema_level, key_dict)
            if isinstance(value, pd.DataFrame):
                # Delete old rows for this schema_id, then insert one per DataFrame row.
                self._execute(f'DELETE FROM "{name}" WHERE schema_id = ?', [schema_id])
                for storage_row in _dataframe_to_storage_rows(value, dtype_meta):
                    self._execute(
                        f'INSERT INTO "{name}" ({col_str}) VALUES ({placeholders})',
                        [schema_id] + storage_row,
                    )
            else:
                storage_values = self._value_to_storage_row(value, dtype_meta)
                row = [schema_id] + storage_values
                self._execute(
                    f'INSERT OR REPLACE INTO "{name}" ({col_str}) VALUES ({placeholders})',
                    row,
                )

        # --- Register in _variables (one row per variable) ---
        self._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [name, schema_level, json.dumps(dtype_meta), description],
        )

    def _entries_from_dataframe(
        self, df: pd.DataFrame, key_cols: list[str], schema_level: str
    ) -> tuple[list[tuple[dict, Any]], str | None]:
        """
        Convert a DataFrame (Mode A) into a list of (key_dict, row_data) entries.

        Each row in the DataFrame becomes one entry.  The non-schema columns
        become the stored data (as a dict / single value).

        Returns (entries, single_col_name) where single_col_name is set if
        there's exactly one data column (so we can preserve its name).
        """
        data_cols = [c for c in df.columns if c not in key_cols]
        entries = []
        single_col_name = data_cols[0] if len(data_cols) == 1 else None
        for _, row in df.iterrows():
            key_dict = {k: row[k] for k in key_cols}
            if len(data_cols) == 1:
                value = row[data_cols[0]]
                # Convert numpy types to Python types for cleaner handling
                if isinstance(value, (np.integer,)):
                    value = int(value)
                elif isinstance(value, (np.floating,)):
                    value = float(value)
                elif isinstance(value, (np.bool_,)):
                    value = bool(value)
            else:
                value = {c: row[c] for c in data_cols}
            entries.append((key_dict, value))
        return entries, single_col_name

    def _infer_data_columns(
        self, sample_value: Any, data_col_name: str | None = None
    ) -> tuple[dict, dict]:
        """Delegate to module-level _infer_data_columns."""
        return _infer_data_columns(sample_value, data_col_name)

    def _value_to_storage_row(self, value: Any, dtype_meta: dict) -> list:
        """Delegate to module-level _value_to_storage_row."""
        return _value_to_storage_row(value, dtype_meta)

    def _ensure_variable_table(
        self,
        name: str,
        data_col_types: dict,
        schema_level: str,
        is_dataframe: bool = False,
    ):
        """Create the variable table if it doesn't exist."""
        if self._table_exists(name):
            return
        data_cols_sql = ", ".join(
            f'"{col}" {dtype}' for col, dtype in data_col_types.items()
        )
        # DataFrames store one DuckDB row per table row: no unique constraint
        # on schema_id.  Other types use schema_id as a primary key so that
        # INSERT OR REPLACE gives "latest wins" semantics.
        if is_dataframe:
            schema_id_col = "schema_id INTEGER NOT NULL"
        else:
            schema_id_col = "schema_id INTEGER PRIMARY KEY"
        self._execute(f"""
            CREATE TABLE "{name}" (
                {schema_id_col},
                {data_cols_sql}
            )
        """)

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(
        self,
        name: str,
        raw: bool = True,
        **schema_keys,
    ) -> pd.DataFrame | Any:
        """
        Load a variable from the database.

        Parameters
        ----------
        name : str
            Variable name.
        raw : bool
            If True and the result is a single row, return the reconstructed
            Python object instead of a DataFrame.
        **schema_keys
            Optional filters, e.g. subject="S01" to load a subset.

        Returns
        -------
        pd.DataFrame or Python object (if raw=True and single row).
        """
        if not self._table_exists(name):
            raise KeyError(f"Variable '{name}' not found in database.")

        # Get metadata
        rows = self._fetchall(
            "SELECT schema_level, dtype FROM _variables WHERE variable_name = ?",
            [name],
        )
        if not rows:
            raise KeyError(f"Variable '{name}' not found.")
        schema_level, dtype_json = rows[0]
        dtype_meta = json.loads(dtype_json)

        # Select all schema columns so non-contiguous keys appear in results
        all_schema_cols = self.dataset_schema
        schema_select = ", ".join(f's."{c}"' for c in all_schema_cols)
        data_cols = list(dtype_meta["columns"].keys())
        data_select = ", ".join(f'v."{c}"' for c in data_cols)

        sql = (
            f"SELECT {schema_select}, {data_select} "
            f'FROM "{name}" v '
            f"JOIN _schema s ON v.schema_id = s.schema_id"
        )
        params: list = []

        # Apply schema key filters (any valid schema column)
        conditions = []
        for col, val in schema_keys.items():
            if col in all_schema_cols:
                conditions.append(f's."{col}" = ?')
                params.append(_schema_str(val))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        df = self._fetchdf(sql, params or None)

        mode = dtype_meta.get("mode", "single_column")
        columns_meta = dtype_meta.get("columns", {})

        if mode == "dataframe":
            # One DuckDB row per DataFrame row: apply _storage_to_python per cell.
            # Drop schema columns; keep only data columns.
            data_cols = list(columns_meta.keys())
            result = {}
            for c, meta in columns_meta.items():
                if c in df.columns:
                    result[c] = [
                        _storage_to_python(df[c].iloc[i], meta) for i in range(len(df))
                    ]
            df_columns = dtype_meta.get("df_columns", data_cols)
            return pd.DataFrame(result, columns=df_columns)

        # Non-DataFrame: restore types then return raw object if single row
        df = self._restore_types(df, dtype_meta)

        if raw and len(df) == 1:
            if mode == "single_column":
                col_name = next(iter(columns_meta))
                col_meta = columns_meta[col_name]
                raw_val = df[col_name].iloc[0]
                return _storage_to_python(raw_val, col_meta)
            elif mode == "multi_column":
                result = {}
                for c, meta in columns_meta.items():
                    result[c] = _storage_to_python(df[c].iloc[0], meta)
                if dtype_meta.get("nested"):
                    return _unflatten_dict(result, dtype_meta["path_map"])
                return result

        return df

    def _restore_types(self, df: pd.DataFrame, dtype_meta: dict) -> pd.DataFrame:
        """Apply type restoration to data columns of a loaded DataFrame."""
        columns_meta = dtype_meta.get("columns", {})
        null_counts: dict = {}
        for col_name, col_meta in columns_meta.items():
            if col_name in df.columns:
                n_null = count_null_list_elements(df[col_name])
                if n_null:
                    null_counts[col_name] = n_null
                restored = [
                    _storage_to_python(df[col_name].iloc[i], col_meta)
                    for i in range(len(df))
                ]
                df[col_name] = restored
        if null_counts:
            logger.debug(
                "_restore_types: restored NULL list element(s) as NaN: %s", null_counts
            )
        return df

    # ------------------------------------------------------------------
    # List / inspect
    # ------------------------------------------------------------------

    def list_variables(self) -> pd.DataFrame:
        """
        List all variables with their schema level and creation time.
        """
        return self._fetchdf("""
            SELECT variable_name, schema_level, created_at, description
            FROM _variables
            ORDER BY variable_name
        """)

    def list_versions(self, name: str) -> pd.DataFrame:
        """
        List variable metadata and all distinct schema entries saved for it.
        """
        if not self._table_exists(name):
            return pd.DataFrame()
        return self._fetchdf(
            "SELECT v.variable_name, v.schema_level, v.created_at, v.description, "
            "COUNT(d.schema_id) AS num_entries "
            f'FROM _variables v LEFT JOIN "{name}" d ON 1=1 '
            "WHERE v.variable_name = ? "
            "GROUP BY v.variable_name, v.schema_level, v.created_at, v.description",
            [name],
        )

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, name: str):
        """
        Delete a variable, dropping its data table and all metadata records.
        """
        if self._table_exists(name):
            self._execute(f'DROP TABLE "{name}"')
        self._execute("DELETE FROM _variables WHERE variable_name = ?", [name])
        self._execute("DELETE FROM _variable_groups WHERE variable_name = ?", [name])

    # ------------------------------------------------------------------
    # Groups
    # ------------------------------------------------------------------

    def add_to_group(self, group_name: str, variable_names: str | list[str]):
        """Add one or more variables to a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self._execute(
                "INSERT INTO _variable_groups (group_name, variable_name) "
                "VALUES (?, ?) ON CONFLICT DO NOTHING",
                [group_name, vn],
            )

    def remove_from_group(self, group_name: str, variable_names: str | list[str]):
        """Remove one or more variables from a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self._execute(
                "DELETE FROM _variable_groups "
                "WHERE group_name = ? AND variable_name = ?",
                [group_name, vn],
            )

    def list_groups(self) -> list[str]:
        """List all group names."""
        rows = self._fetchall(
            "SELECT DISTINCT group_name FROM _variable_groups ORDER BY group_name"
        )
        return [r[0] for r in rows]

    def get_group(self, group_name: str) -> list[str]:
        """Get all variable names in a group."""
        rows = self._fetchall(
            "SELECT variable_name FROM _variable_groups "
            "WHERE group_name = ? ORDER BY variable_name",
            [group_name],
        )
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def distinct_schema_values(self, key: str) -> list:
        """Return all distinct non-null values for a schema column, sorted."""
        if key not in self.dataset_schema:
            raise ValueError(
                f"'{key}' is not a schema column. Available: {self.dataset_schema}"
            )
        rows = self._fetchall(
            f'SELECT DISTINCT "{key}" FROM _schema '
            f'WHERE "{key}" IS NOT NULL '
            f'ORDER BY "{key}"'
        )
        return [r[0] for r in rows]

    def distinct_schema_combinations(self, keys: list[str]) -> list[tuple]:
        """Return all distinct non-null combinations for multiple schema columns.

        Args:
            keys: List of schema column names to query.

        Returns:
            List of tuples, each tuple being one existing combination of values
            (as strings, since _schema stores VARCHAR columns). Sorted by the
            column order given.
        """
        for k in keys:
            if k not in self.dataset_schema:
                raise ValueError(
                    f"'{k}' is not a schema column. Available: {self.dataset_schema}"
                )
        col_list = ", ".join(f'"{k}"' for k in keys)
        where_clause = " AND ".join(f'"{k}" IS NOT NULL' for k in keys)
        order_clause = ", ".join(f'"{k}"' for k in keys)
        rows = self._fetchall(
            f"SELECT DISTINCT {col_list} FROM _schema "
            f"WHERE {where_clause} "
            f"ORDER BY {order_clause}"
        )
        return [tuple(r) for r in rows]

    # ------------------------------------------------------------------
    # Direct query access
    # ------------------------------------------------------------------

    def query(self, sql: str, params=None) -> pd.DataFrame:
        """Execute arbitrary SQL and return a DataFrame."""
        return self._fetchdf(sql, params)

    # ------------------------------------------------------------------
    # Context manager / cleanup
    # ------------------------------------------------------------------

    def close(self):
        """Close the DuckDB connection."""
        self.con.close()
        logger.debug("DuckDB lock RELEASED: %s", self.db_path)

    def reopen(self):
        """Reopen the DuckDB connection after close(), preserving read_only mode."""
        logger.debug(
            "DuckDB lock ACQUIRED (reopen, read_only=%s): %s",
            getattr(self, "read_only", False),
            self.db_path,
        )
        self.con = duckdb.connect(
            str(self.db_path), read_only=getattr(self, "read_only", False)
        )

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager, closing the DuckDB connection."""
        self.close()

    def __repr__(self):
        try:
            n_vars = self._fetchall(
                "SELECT COUNT(DISTINCT variable_name) FROM _variables"
            )[0][0]
        except Exception:
            n_vars = "?"
        return (
            f"SciDuck(path='{self.db_path}', "
            f"schema={self.dataset_schema}, variables={n_vars})"
        )
