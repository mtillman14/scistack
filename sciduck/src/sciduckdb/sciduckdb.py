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

import duckdb
import pandas as pd
import numpy as np
import json
import datetime
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


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


def _infer_duckdb_type(value: Any) -> Tuple[str, dict]:
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


def _storage_to_python(value: Any, meta: dict) -> Any:
    """Restore a stored DuckDB value back to its original Python type."""
    ptype = meta.get("python_type", "")

    if ptype == "ndarray":
        dtype = np.dtype(meta.get("numpy_dtype", "float64"))
        ndim = meta.get("ndim", 1)
        if ndim >= 2:
            # DuckDB returns ndarray of ndarrays; stack them
            return np.stack([np.asarray(row) for row in value]).astype(dtype)
        return np.asarray(value, dtype=dtype)

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
            if isinstance(value, np.ndarray):
                return [np.asarray(v, dtype=dtype) for v in value]
            return [np.asarray(v, dtype=dtype) for v in value]
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
    sample_value: Any, data_col_name: Optional[str] = None
) -> Tuple[dict, dict]:
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
        return [
            _python_to_storage(flat[col], col_metas[col])
            for col in col_metas
        ]
    else:
        # Single column — get the one key (could be "value" or a named column)
        col_name = next(iter(col_metas))
        col_meta = col_metas[col_name]
        return [_python_to_storage(value, col_meta)]


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class SciDuck:
    """
    A thin DuckDB layer for managing versioned, schema-aware scientific data.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB database file.  Use ":memory:" for in-memory.
    dataset_schema : list of str
        Ordered hierarchy, e.g. ["subject", "session", "trial"].
    """

    def __init__(self, db_path: Union[str, Path], dataset_schema: List[str]):
        self.db_path = str(db_path)
        self.dataset_schema = list(dataset_schema)
        self._lock = threading.Lock()
        self.con = duckdb.connect(self.db_path)
        self._init_metadata_tables()

    # ------------------------------------------------------------------
    # Thin internal interface (future backend swap point)
    # ------------------------------------------------------------------

    def _execute(self, sql: str, params=None):
        # NOTE: DuckDB's Python connection returns itself from execute(), so
        # execute() and fetchXxx() share the same connection state.  All callers
        # that fetch results must hold _lock for the entire execute+fetch sequence.
        # Use _fetchall / _fetchdf for queries that return rows; call _execute
        # directly (under _lock) only for DDL/DML that needs no fetch.
        with self._lock:
            if params:
                return self.con.execute(sql, params)
            return self.con.execute(sql)

    def _executemany(self, sql: str, params_list):
        with self._lock:
            return self.con.executemany(sql, params_list)

    def _begin(self):
        with self._lock:
            self.con.execute("BEGIN TRANSACTION")

    def _commit(self):
        with self._lock:
            self.con.execute("COMMIT")

    def _rollback(self):
        with self._lock:
            self.con.execute("ROLLBACK")

    def _fetchall(self, sql: str, params=None) -> list:
        with self._lock:
            if params:
                return self.con.execute(sql, params).fetchall()
            return self.con.execute(sql).fetchall()

    def fetchall(self, sql: str, params=None) -> list:
        """Public alias for _fetchall — accessible from MATLAB (underscore methods are not)."""
        return self._fetchall(sql, params)

    def _fetchdf(self, sql: str, params=None) -> pd.DataFrame:
        with self._lock:
            if params:
                return self.con.execute(sql, params).fetchdf()
            return self.con.execute(sql).fetchdf()

    def _table_exists(self, name: str) -> bool:
        rows = self._fetchall(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = ?", [name]
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
            existing_cols = [
                row[0] for row in self._fetchall(
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

    def _schema_key_columns(self, schema_level: str) -> List[str]:
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
            f'SELECT schema_id FROM _schema WHERE schema_level = ? AND {where}',
            params,
        )
        if rows:
            return rows[0][0]

        # Insert new entry — use MAX+1 for consistency with batch path
        new_id = self._fetchall(
            "SELECT COALESCE(MAX(schema_id), 0) + 1 FROM _schema"
        )[0][0]
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
            null_conditions = " AND ".join(
                f'"{col}" IS NULL' for col in null_cols
            )
            where_clause = f'schema_level = ?'
            if null_conditions:
                where_clause += f' AND {null_conditions}'

            col_select = ", ".join(f'"{c}"' for c in key_cols)
            rows = self._fetchall(
                f'SELECT schema_id, {col_select} FROM _schema WHERE {where_clause}',
                [schema_level],
            )

            # Build lookup: tuple of col values -> schema_id
            existing_lookup = {}
            for row in rows:
                sid = row[0]
                row_key = tuple(_schema_str(v) if v is not None else "" for v in row[1:])
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
                col_str = ", ".join(f'"{c}"' for c in col_names)

                insert_rows = []
                for idx, (combo_key, key_values, _) in enumerate(missing):
                    new_id = first_id + idx
                    row = [new_id, schema_level] + [
                        _schema_str(key_values[c]) for c in key_cols
                    ]
                    insert_rows.append(row)
                    result[combo_key] = new_id

                # Use DataFrame-based insert for speed
                insert_df = pd.DataFrame(insert_rows, columns=col_names)
                self.con.execute(
                    f"INSERT INTO _schema ({col_str}) SELECT * FROM insert_df"
                )

        return result

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save(
        self,
        name: str,
        data: Any,
        schema_level: Optional[str] = None,
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
                schema_level = provided_schema_cols[-1] if provided_schema_cols else self.dataset_schema[-1]
            if schema_level not in self.dataset_schema:
                raise ValueError(
                    f"schema_level '{schema_level}' not in {self.dataset_schema}"
                )
            key_cols = provided_schema_cols
            entries = [(
                {k: schema_keys[k] for k in key_cols},
                data,
            )]

        else:
            if schema_level is None:
                schema_level = self.dataset_schema[-1]
            if schema_level not in self.dataset_schema:
                raise ValueError(
                    f"schema_level '{schema_level}' not in {self.dataset_schema}"
                )
            key_cols = self._schema_key_columns(schema_level)

            # Mode A: DataFrame with schema columns
            if isinstance(data, pd.DataFrame) and all(c in data.columns for c in key_cols):
                entries, data_col_name = self._entries_from_dataframe(data, key_cols, schema_level)

            # Mode C: dict with tuple keys
            elif isinstance(data, dict) and data and isinstance(next(iter(data.keys())), tuple):
                entries = []
                for key_tuple, value in data.items():
                    if len(key_tuple) != len(key_cols):
                        raise ValueError(
                            f"Key tuple length {len(key_tuple)} != "
                            f"expected {len(key_cols)} for level '{schema_level}'"
                        )
                    key_dict = dict(zip(key_cols, key_tuple))
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
        data_col_types, dtype_meta = self._infer_data_columns(sample_value, data_col_name)

        # --- Ensure the variable table exists ---
        is_dataframe = dtype_meta.get("mode") == "dataframe"
        self._ensure_variable_table(name, data_col_types, schema_level,
                                    is_dataframe=is_dataframe)

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
                    f'INSERT OR REPLACE INTO "{name}" ({col_str}) VALUES ({placeholders})', row
                )

        # --- Register in _variables (one row per variable) ---
        self._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [name, schema_level, json.dumps(dtype_meta), description],
        )

    def _entries_from_dataframe(
        self, df: pd.DataFrame, key_cols: List[str], schema_level: str
    ) -> Tuple[List[Tuple[dict, Any]], Optional[str]]:
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
        self, sample_value: Any, data_col_name: Optional[str] = None
    ) -> Tuple[dict, dict]:
        """Delegate to module-level _infer_data_columns."""
        return _infer_data_columns(sample_value, data_col_name)

    def _value_to_storage_row(self, value: Any, dtype_meta: dict) -> list:
        """Delegate to module-level _value_to_storage_row."""
        return _value_to_storage_row(value, dtype_meta)

    def _ensure_variable_table(self, name: str, data_col_types: dict, schema_level: str,
                               is_dataframe: bool = False):
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
    ) -> Union[pd.DataFrame, Any]:
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
            f'SELECT {schema_select}, {data_select} '
            f'FROM "{name}" v '
            f'JOIN _schema s ON v.schema_id = s.schema_id'
        )
        params: list = []

        # Apply schema key filters (any valid schema column)
        conditions = []
        for col, val in schema_keys.items():
            if col in all_schema_cols:
                conditions.append(f's."{col}" = ?')
                params.append(_schema_str(val))
        if conditions:
            sql += ' WHERE ' + ' AND '.join(conditions)

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
                    result[c] = [_storage_to_python(df[c].iloc[i], meta)
                                 for i in range(len(df))]
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
        for col_name, col_meta in columns_meta.items():
            if col_name in df.columns:
                restored = [
                    _storage_to_python(df[col_name].iloc[i], col_meta)
                    for i in range(len(df))
                ]
                df[col_name] = restored
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
        self._execute(
            "DELETE FROM _variables WHERE variable_name = ?", [name]
        )
        self._execute(
            "DELETE FROM _variable_groups WHERE variable_name = ?", [name]
        )

    # ------------------------------------------------------------------
    # Groups
    # ------------------------------------------------------------------

    def add_to_group(self, group_name: str, variable_names: Union[str, List[str]]):
        """Add one or more variables to a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self._execute(
                "INSERT INTO _variable_groups (group_name, variable_name) "
                "VALUES (?, ?) ON CONFLICT DO NOTHING",
                [group_name, vn],
            )

    def remove_from_group(self, group_name: str, variable_names: Union[str, List[str]]):
        """Remove one or more variables from a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self._execute(
                "DELETE FROM _variable_groups "
                "WHERE group_name = ? AND variable_name = ?",
                [group_name, vn],
            )

    def list_groups(self) -> List[str]:
        """List all group names."""
        rows = self._fetchall(
            "SELECT DISTINCT group_name FROM _variable_groups ORDER BY group_name"
        )
        return [r[0] for r in rows]

    def get_group(self, group_name: str) -> List[str]:
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

    def distinct_schema_values(self, key: str) -> List:
        """Return all distinct non-null values for a schema column, sorted."""
        if key not in self.dataset_schema:
            raise ValueError(
                f"'{key}' is not a schema column. "
                f"Available: {self.dataset_schema}"
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
                    f"'{k}' is not a schema column. "
                    f"Available: {self.dataset_schema}"
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

    def reopen(self):
        """Reopen the DuckDB connection after close()."""
        self.con = duckdb.connect(str(self.db_path))

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