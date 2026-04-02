"""Database connection and management using SciDuck backend."""

import json
import os
import random
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Type, Any

import numpy as np
import pandas as pd

from .exceptions import (
    AmbiguousParamError,
    AmbiguousVersionError,
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
)
from .hashing import generate_record_id, canonical_hash
from .variable import BaseVariable


def _schema_str(value):
    """Stringify a schema key value, converting whole-number floats to int.

    Schema keys are stored as VARCHAR in DuckDB.  str(1.0) → "1.0" but
    str(1) → "1".  MATLAB sends all numbers as float, so without this
    conversion, queries and cache lookups fail because "1.0" ≠ "1".
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _from_schema_str(value):
    """Convert a schema VARCHAR value back to a numeric type if possible.

    Schema keys are stored as VARCHAR, so loaded values are always strings.
    This restores the original type (int or float) so that user-facing
    metadata has the same type as what was originally saved.
    """
    if not isinstance(value, str):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        pass
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value

from sciduckdb import (
    SciDuck,
    _infer_duckdb_type, _python_to_storage, _storage_to_python,
    _infer_data_columns, _value_to_storage_row, _dataframe_to_storage_rows,
    _flatten_dict, _unflatten_dict,
)


def _match_branch_param(branch_params_dict: dict, key: str, value: Any) -> bool:
    """Match a single branch_params filter key/value against a branch_params dict.

    1. Exact match (covers bare dynamic names and namespaced constant names).
    2. Suffix match for bare constant names (e.g. "low_hz" → "bandpass_filter.low_hz").
    Raises AmbiguousParamError if the bare name matches multiple namespaced keys.
    """
    # Exact match
    if key in branch_params_dict:
        return branch_params_dict[key] == value
    # Suffix match
    suffix = f".{key}"
    hits = [(k, v) for k, v in branch_params_dict.items() if k.endswith(suffix)]
    if len(hits) == 1:
        return hits[0][1] == value
    if len(hits) > 1:
        raise AmbiguousParamError(
            f"'{key}' matches multiple branch params: {[h[0] for h in hits]}"
        )
    return False


# Global database instance (thread-local for safety)
_local = threading.local()


def _is_tabular_dict(data):
    """Return True if data is a dict where ALL values are 1D (or Nx1 column-vector) numpy arrays of equal length."""
    if not isinstance(data, dict) or len(data) == 0:
        return False
    lengths = set()
    for k, v in data.items():
        if not isinstance(v, np.ndarray):
            return False
        # Accept 1D arrays, Nx1 column vectors, and 1xN row vectors (from MATLAB)
        if v.ndim == 1:
            lengths.add(v.shape[0])
        elif v.ndim == 2 and v.shape[0] == 1:
            lengths.add(v.shape[1])
        elif v.ndim == 2 and v.shape[1] == 1:
            lengths.add(v.shape[0])
        else:
            return False
    return len(lengths) == 1


def _get_leaf_paths(d, prefix=()):
    """Recursively get all leaf paths in a nested dict.

    A leaf is any value that is NOT a dict.  Returns a list of tuples,
    each tuple being the sequence of keys from root to leaf.
    """
    paths = []
    for key, value in d.items():
        current = prefix + (key,)
        if isinstance(value, dict):
            paths.extend(_get_leaf_paths(value, current))
        else:
            paths.append(current)
    return paths


def _get_nested_value(d, path):
    """Get a value from a nested dict following *path* (tuple of keys)."""
    current = d
    for key in path:
        current = current[key]
    return current


def _set_nested_value(d, path, value):
    """Set a value in a nested dict by *path*, creating intermediate dicts."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def _flatten_struct_columns(df):
    """Flatten DataFrame columns that contain nested dicts into dot-separated columns.

    For each object-dtype column whose first non-null value is a ``dict``,
    recursively extract all leaf paths and create new columns named
    ``"original_col.key1.key2.leaf"``.

    **Leaf handling:**
    - Scalar leaves (int, float, str, bool, None) are stored directly.
    - Array leaves (numpy arrays, Python lists) are serialised to a JSON
      string so every cell in the resulting column is a simple scalar type
      that DuckDB can ingest.

    Returns
    -------
    (flattened_df, struct_columns_info)
        *struct_columns_info* maps each flattened original column name to
        metadata needed by ``_unflatten_struct_columns`` on load.
        Empty dict when no struct columns are found.
    """
    if len(df) == 0:
        return df, {}

    struct_info = {}
    cols_to_drop = []
    new_col_data = {}  # ordered: col_name -> list of values

    for col_idx, col in enumerate(df.columns):
        if df[col].dtype != object:
            continue

        # Find first non-null value
        first_val = None
        for v in df[col]:
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                first_val = v
                break

        if not isinstance(first_val, dict):
            continue

        # This column contains nested dicts — flatten it
        leaf_paths = _get_leaf_paths(first_val)
        if not leaf_paths:
            continue

        array_leaves = {}  # dot_path -> {"dtype": ..., "shape": ...}

        for path in leaf_paths:
            dot_path = ".".join(path)
            flat_col_name = f"{col}.{dot_path}"
            values = []
            for row_val in df[col]:
                if row_val is None or (isinstance(row_val, float) and np.isnan(row_val)):
                    values.append(None)
                    continue
                try:
                    leaf = _get_nested_value(row_val, path)
                except (KeyError, TypeError):
                    values.append(None)
                    continue

                if isinstance(leaf, np.ndarray):
                    # Track array metadata from first occurrence
                    if dot_path not in array_leaves:
                        array_leaves[dot_path] = {
                            "dtype": str(leaf.dtype),
                            "shape": list(leaf.shape),
                        }
                    values.append(json.dumps(leaf.tolist()))
                elif isinstance(leaf, list):
                    if dot_path not in array_leaves:
                        array_leaves[dot_path] = {"dtype": "list"}
                    values.append(json.dumps(leaf))
                else:
                    values.append(leaf)

            new_col_data[flat_col_name] = values

        cols_to_drop.append(col)
        struct_info[col] = {
            "paths": [list(p) for p in leaf_paths],
            "array_leaves": array_leaves,
            "col_position": col_idx,
        }

    if not cols_to_drop:
        return df, {}

    result = df.drop(columns=cols_to_drop)
    for name, values in new_col_data.items():
        result[name] = values

    return result, struct_info


def _unflatten_struct_columns(df, struct_info):
    """Reconstruct nested-dict columns from dot-separated flat columns.

    Inverse of ``_flatten_struct_columns``.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with dot-separated columns produced by ``_flatten_struct_columns``.
    struct_info : dict
        The metadata dict that was stored alongside the data.

    Returns
    -------
    pd.DataFrame with the original nested-dict object columns restored.
    """
    if not struct_info:
        return df

    result = df.copy()

    # Process struct columns in reverse position order so inserts don't shift indices
    for col_name, info in sorted(
        ((k, v) for k, v in struct_info.items() if k != "__list_columns__"),
        key=lambda x: x[1]["col_position"],
        reverse=True,
    ):
        paths = [tuple(p) for p in info["paths"]]
        array_leaves = info.get("array_leaves", {})
        col_position = info["col_position"]

        # Collect all flat column names belonging to this struct
        flat_col_names = [f"{col_name}.{'.'.join(p)}" for p in paths]
        existing_flat = [c for c in flat_col_names if c in result.columns]

        if not existing_flat:
            continue

        # Build nested dicts row by row
        nested_values = []
        n_rows = len(result)

        for row_idx in range(n_rows):
            row_dict = {}
            for path, flat_col in zip(paths, flat_col_names):
                if flat_col not in result.columns:
                    continue
                val = result[flat_col].iloc[row_idx]
                dot_path = ".".join(path)

                # Restore arrays from JSON
                if dot_path in array_leaves and val is not None:
                    arr_meta = array_leaves[dot_path]
                    if isinstance(val, str):
                        parsed = json.loads(val)
                    else:
                        parsed = val
                    if arr_meta.get("dtype") == "list":
                        val = parsed
                    else:
                        val = np.array(parsed, dtype=np.dtype(arr_meta["dtype"]))
                        expected_shape = arr_meta.get("shape")
                        if (expected_shape and list(val.shape) != expected_shape
                                and val.size == np.prod(expected_shape)):
                            val = val.reshape(expected_shape)

                _set_nested_value(row_dict, path, val)
            nested_values.append(row_dict)

        # Drop the flat columns
        result = result.drop(columns=existing_flat)

        # Insert the reconstituted column at its original position
        # (clamped to current column count since other columns may have shifted)
        insert_pos = min(col_position, len(result.columns))
        result.insert(insert_pos, col_name, nested_values)

    # Convert list-valued cells to numpy arrays for MATLAB interop.
    # DuckDB DOUBLE[] columns come back as Python lists; old VARCHAR saves
    # come back as string representations like "[1.0, 2.0, 3.0]".
    for col in result.columns:
        if result[col].dtype != object:
            continue
        first_val = next(
            (v for v in result[col]
             if v is not None and not (isinstance(v, float) and np.isnan(v))),
            None,
        )
        if first_val is None:
            continue

        if isinstance(first_val, (list, np.ndarray)):
            # DuckDB DOUBLE[] returns as lists or numpy arrays — ensure numpy
            result[col] = result[col].apply(
                lambda v: np.array(v, dtype=float) if isinstance(v, list) else v)
        elif isinstance(first_val, str) and first_val.strip().startswith('['):
            # Backwards compat: parse VARCHAR strings from old saves
            def _parse_list_str(v):
                if not isinstance(v, str):
                    return v
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return np.array(parsed, dtype=float)
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
                return v
            result[col] = result[col].apply(_parse_list_str)

    return result


def get_user_id() -> str | None:
    """
    Get the current user ID from environment.

    The user ID is used for attribution in cross-user provenance tracking.
    Set the SCIDB_USER_ID environment variable to identify the current user.

    Returns:
        The user ID string, or None if not set.
    """
    return os.environ.get("SCIDB_USER_ID")


def configure_database(
    dataset_db_path: str | Path,
    dataset_schema_keys: list[str],
) -> "DatabaseManager":
    """
    Configure the global database connection.

    Single-call setup that creates the database, auto-registers all known
    BaseVariable subclasses, and enables thunk caching.

    Args:
        dataset_db_path: Path to the DuckDB database file
        dataset_schema_keys: List of metadata keys that define the dataset schema
            (e.g., ["subject", "visit", "channel"]). These keys identify the
            logical location of data and are used for the folder hierarchy.
            Any metadata keys not in this list are treated as version parameters
            that distinguish different computational versions of the same data.

    Returns:
        The DatabaseManager instance
    """
    db = DatabaseManager(
        dataset_db_path,
        dataset_schema_keys=dataset_schema_keys,
    )
    for cls in BaseVariable._all_subclasses.values():
        db.register(cls)
    db.set_current_db()

    # Propagate schema keys to scifor so that DataFrame detection and
    # distribute=True work identically in DB-backed and standalone modes.
    try:
        import scifor
        scifor.set_schema(list(dataset_schema_keys))
    except ImportError:
        pass

    return db


def get_database() -> "DatabaseManager":
    """
    Get the global database connection.

    Returns:
        The DatabaseManager instance

    Raises:
        DatabaseNotConfiguredError: If configure_database() hasn't been called
    """
    db = getattr(_local, "database", None)
    if db is None:
        raise DatabaseNotConfiguredError(
            "Database not configured. Call configure_database(path) first."
        )
    if getattr(db, "_closed", False):
        db.reopen()
    return db


class DatabaseManager:
    """
    Manages data storage and lineage persistence (both in DuckDB via SciDuck).

    Example:
        db = configure_database("experiment.duckdb", ["subject", "session"])

        RawSignal.save(np.eye(3), subject=1, session=1)
        loaded = RawSignal.load(subject=1, session=1)
    """

    def __init__(
        self,
        dataset_db_path: str | Path,
        dataset_schema_keys: list[str],
    ):
        """
        Initialize database connection.

        Args:
            dataset_db_path: Path to DuckDB database file (created if doesn't exist)
            dataset_schema_keys: List of metadata keys that define the dataset schema
                (e.g., ["subject", "visit", "channel"]). These keys identify the
                logical location of data. Any other metadata keys are treated as
                version parameters.
        """
        self.dataset_db_path = Path(dataset_db_path)

        if isinstance(dataset_schema_keys, (set, frozenset)):
            raise TypeError(
                "dataset_schema_keys must be an ordered sequence (list or tuple), "
                "not a set. Schema key order defines the dataset hierarchy."
            )
        self.dataset_schema_keys = list(dataset_schema_keys)
        self._registered_types: dict[str, Type[BaseVariable]] = {}

        # Initialize SciDuck backend for data storage and lineage (all in DuckDB)
        self._duck = SciDuck(self.dataset_db_path, dataset_schema=dataset_schema_keys)

        # Create metadata tables for type registration (in DuckDB)
        self._ensure_meta_tables()
        self._ensure_record_metadata_table()
        self._ensure_lineage_table()

        self._closed = False # Track connection open/closed state

    def _ensure_meta_tables(self):
        """Create internal metadata tables for type registration."""
        # Registered types table (remains in DuckDB for data type discovery)
        # Note: Only type_name is unique (PRIMARY KEY). table_name is not unique
        # to avoid DuckDB's ON CONFLICT ambiguity with multiple unique constraints.
        self._duck._execute("""
            CREATE TABLE IF NOT EXISTS _registered_types (
                type_name VARCHAR PRIMARY KEY,
                table_name VARCHAR NOT NULL,
                schema_version INTEGER NOT NULL,
                registered_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

    def _ensure_record_metadata_table(self):
        """Create the _record_metadata side table for record-level metadata."""
        self._duck._execute("""
            CREATE TABLE IF NOT EXISTS _record_metadata (
                record_id VARCHAR NOT NULL,
                timestamp VARCHAR NOT NULL,
                variable_name VARCHAR NOT NULL,
                schema_id INTEGER NOT NULL,
                version_keys VARCHAR DEFAULT '{}',
                content_hash VARCHAR,
                lineage_hash VARCHAR,
                schema_version INTEGER,
                user_id VARCHAR,
                branch_params VARCHAR DEFAULT '{}',
                excluded BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (record_id, timestamp)
            )
        """)

    def _ensure_lineage_table(self):
        """Create the _lineage table for computation provenance."""
        self._duck._execute("""
            CREATE TABLE IF NOT EXISTS _lineage (
                output_record_id VARCHAR PRIMARY KEY,
                lineage_hash     VARCHAR NOT NULL,
                target           VARCHAR NOT NULL,
                function_name    VARCHAR NOT NULL,
                function_hash    VARCHAR NOT NULL,
                inputs           VARCHAR NOT NULL DEFAULT '[]',
                constants        VARCHAR NOT NULL DEFAULT '[]',
                timestamp        VARCHAR NOT NULL
            )
        """)

    def _create_variable_view(self, variable_class: Type[BaseVariable]):
        """Create a view joining a variable table with _schema via _record_metadata."""
        table_name = variable_class.table_name()
        view_name = variable_class.view_name()
        schema_cols = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)
        self._duck._execute(f"""
            CREATE OR REPLACE VIEW "{view_name}" AS
            WITH latest_meta AS (
                SELECT record_id, schema_id, version_keys, branch_params, excluded,
                       ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY timestamp DESC) AS rn
                FROM _record_metadata
                WHERE variable_name = '{view_name}'
            )
            SELECT
                t.*,
                s.schema_level, {schema_cols},
                lm.version_keys, lm.branch_params, lm.excluded
            FROM "{table_name}" t
            LEFT JOIN latest_meta lm ON t.record_id = lm.record_id AND lm.rn = 1
            LEFT JOIN _schema s ON lm.schema_id = s.schema_id
        """)

    def _split_metadata(self, flat_metadata: dict) -> dict:
        """
        Split flat metadata into nested schema/version structure.

        Keys in schema_keys go to "schema", all other keys go to "version".
        """
        schema = {}
        version = {}
        for key, value in flat_metadata.items():
            if key in self.dataset_schema_keys:
                schema[key] = value
            else:
                version[key] = value
        return {"schema": schema, "version": version}

    def _infer_schema_level(self, schema_keys: dict) -> str | None:
        """
        Infer the schema level from provided keys.

        Walks dataset_schema_keys top-down. Returns the deepest provided key.
        Keys need not be contiguous — any subset of schema keys is valid.

        Returns None if no schema keys are provided.
        """
        if not schema_keys:
            return None

        level = None
        for key in self.dataset_schema_keys:
            if key in schema_keys:
                level = key
        return level

    def _save_record_metadata(
        self,
        record_id: str,
        timestamp: str,
        variable_name: str,
        schema_id: int,
        version_keys: dict | None,
        content_hash: str | None,
        lineage_hash: str | None,
        schema_version: int,
        user_id: str | None,
        branch_params: dict | None = None,
    ) -> None:
        """Insert a new audit row into _record_metadata. Always inserts (audit trail)."""
        vk_json = json.dumps(version_keys or {}, sort_keys=True)
        bp_json = json.dumps(branch_params or {}, sort_keys=True)
        self._duck._execute(
            """
            INSERT INTO _record_metadata (
                record_id, timestamp, variable_name, schema_id,
                version_keys, content_hash, lineage_hash, schema_version, user_id,
                branch_params
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (record_id, timestamp) DO NOTHING
            """,
            [
                record_id, timestamp, variable_name, schema_id,
                vk_json, content_hash, lineage_hash, schema_version, user_id,
                bp_json,
            ],
        )

    def _save_columnar(
        self,
        record_id: str,
        table_name: str,
        variable_class: Type[BaseVariable],
        df: pd.DataFrame,
        schema_level: str | None,
        schema_keys: dict,
        content_hash: str,
        dict_of_arrays: bool = False,
        ndarray_keys: dict | None = None,
        struct_columns: dict | None = None,
    ) -> int:
        """
        Save a DataFrame into a columnar table identified by record_id.

        Used for custom-serialized data (to_db/from_db), native DataFrames,
        and dict-of-arrays data. The table uses record_id as the row identifier;
        multiple data rows sharing the same record_id are allowed.

        Returns schema_id.
        """
        schema_id = (
            self._duck._get_or_create_schema_id(schema_level, schema_keys)
            if schema_level is not None and schema_keys
            else 0
        )

        # Ensure table exists
        if not self._duck._table_exists(table_name):
            col_defs = []
            for col in df.columns:
                dtype = df[col].dtype
                if pd.api.types.is_integer_dtype(dtype):
                    ddb_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    ddb_type = "DOUBLE"
                elif pd.api.types.is_bool_dtype(dtype):
                    ddb_type = "BOOLEAN"
                elif dtype == object:
                    first_val = next(
                        (v for v in df[col]
                         if v is not None and not (isinstance(v, float) and np.isnan(v))),
                        None,
                    )
                    if isinstance(first_val, np.ndarray) and np.issubdtype(first_val.dtype, np.number):
                        ddb_type = "DOUBLE[]"
                    elif (isinstance(first_val, list) and len(first_val) > 0
                          and all(isinstance(x, (int, float)) for x in first_val)):
                        ddb_type = "DOUBLE[]"
                    else:
                        ddb_type = "VARCHAR"
                else:
                    ddb_type = "VARCHAR"
                col_defs.append(f'"{col}" {ddb_type}')

            data_cols_sql = ", ".join(col_defs)
            self._duck._execute(f"""
                CREATE TABLE "{table_name}" (
                    record_id VARCHAR NOT NULL,
                    {data_cols_sql}
                )
            """)
            self._create_variable_view(variable_class)

        # Only insert if this record_id doesn't already exist
        existing_count = self._duck._fetchall(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE record_id = ?',
            [record_id],
        )[0][0]

        if existing_count == 0:
            insert_df = df.copy()
            insert_df.insert(0, "record_id", record_id)
            col_str = ", ".join(f'"{c}"' for c in insert_df.columns)
            self._duck.con.execute(
                f'INSERT INTO "{table_name}" ({col_str}) SELECT * FROM insert_df'
            )

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        if dict_of_arrays:
            dtype_json = json.dumps({
                "custom": True,
                "dict_of_arrays": True,
                "ndarray_keys": ndarray_keys or {},
            })
        elif struct_columns:
            dtype_json = json.dumps({
                "custom": True,
                "struct_columns": struct_columns,
            })
        else:
            dtype_json = json.dumps({"custom": True})
        self._duck._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, dtype_json, ""],
        )

        return schema_id

    def _save_native(
        self,
        record_id: str,
        table_name: str,
        variable_class: Type[BaseVariable],
        data: Any,
        content_hash: str,
        schema_level: str | None = None,
        schema_keys: dict | None = None,
    ) -> int:
        """
        Save native data as a single record using sciduck's type inference.

        Handles scalars, arrays, lists, dicts (flat & nested), and
        dict-of-arrays.  Each dict key becomes its own DuckDB column;
        vector values become DuckDB array types (e.g. DOUBLE[]).

        The table uses record_id as PRIMARY KEY so identical data is stored once.

        Returns schema_id.
        """
        if schema_level is not None and schema_keys:
            schema_id = self._duck._get_or_create_schema_id(
                schema_level, {k: _schema_str(v) for k, v in schema_keys.items()}
            )
        else:
            schema_id = 0

        data_col_types, dtype_meta = _infer_data_columns(data)
        is_dataframe = isinstance(data, pd.DataFrame)

        # Ensure table exists
        if not self._duck._table_exists(table_name):
            data_cols_sql = ", ".join(f'"{col}" {dtype}' for col, dtype in data_col_types.items())
            if is_dataframe:
                # One DuckDB row per table row: record_id is not unique per row.
                record_id_col = "record_id VARCHAR NOT NULL"
            else:
                record_id_col = "record_id VARCHAR PRIMARY KEY"
            self._duck._execute(f'''
                CREATE TABLE "{table_name}" (
                    {record_id_col},
                    {data_cols_sql}
                )
            ''')
            self._create_variable_view(variable_class)

        if is_dataframe:
            # Idempotency: skip all inserts if this record_id already exists.
            existing_count = self._duck._fetchall(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE record_id = ?',
                [record_id],
            )[0][0]
            if existing_count == 0:
                col_names = ["record_id"] + list(data_col_types.keys())
                col_str = ", ".join(f'"{c}"' for c in col_names)
                placeholders = ", ".join(["?"] * len(col_names))
                for storage_row in _dataframe_to_storage_rows(data, dtype_meta):
                    self._duck._execute(
                        f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders})',
                        [record_id] + storage_row,
                    )
        else:
            storage_values = _value_to_storage_row(data, dtype_meta)
            col_names = ["record_id"] + list(data_col_types.keys())
            col_str = ", ".join(f'"{c}"' for c in col_names)
            placeholders = ", ".join(["?"] * len(col_names))
            self._duck._execute(
                f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders}) '
                f'ON CONFLICT (record_id) DO NOTHING',
                [record_id] + storage_values,
            )

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        self._duck._execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, json.dumps(dtype_meta), ""],
        )

        return schema_id

    def save_batch(
        self,
        variable_class: Type[BaseVariable],
        data_items: list[tuple[Any, dict]],
        profile: bool = False,
    ) -> list[str]:
        """
        Bulk-save a list of (data_value, metadata_dict) pairs for a single variable type.

        Amortizes setup work (registration, table creation) and batches SQL
        operations using DataFrame-based inserts for speed.

        Data is deduplicated by record_id (same content → same record_id → stored once).
        Every call inserts a new (record_id, timestamp) row in _record_metadata for audit.

        Args:
            variable_class: The BaseVariable subclass to save as
            data_items: List of (data_value, flat_metadata_dict) tuples
            profile: If True, print phase-by-phase timing summary

        Returns:
            List of record_ids for each saved item (in input order)
        """
        if not data_items:
            return []

        timings = {}
        t0 = time.perf_counter()

        table_name = self._ensure_registered(variable_class)
        type_name = variable_class.__name__
        schema_version = variable_class.schema_version
        user_id = get_user_id()

        # --- One-time setup from first item ---
        first_data, first_meta = data_items[0]

        data_col_types, dtype_meta = _infer_data_columns(first_data)
        is_dataframe = dtype_meta.get("mode") == "dataframe"

        if not self._duck._table_exists(table_name):
            data_cols_sql = ", ".join(f'"{col}" {dtype}' for col, dtype in data_col_types.items())
            if is_dataframe:
                record_id_col = "record_id VARCHAR NOT NULL"
            else:
                record_id_col = "record_id VARCHAR PRIMARY KEY"
            self._duck._execute(f'''
                CREATE TABLE "{table_name}" (
                    {record_id_col},
                    {data_cols_sql}
                )
            ''')
            self._create_variable_view(variable_class)

        timings["1_setup"] = time.perf_counter() - t0

        # --- Batch schema_id resolution ---
        t1 = time.perf_counter()
        all_nested = []
        unique_schema_combos = {}  # {combo_key: schema_keys_dict}
        for data_val, flat_meta in data_items:
            nested = self._split_metadata(flat_meta)
            all_nested.append(nested)
            schema_keys = nested.get("schema", {})
            schema_level = self._infer_schema_level(schema_keys)
            if schema_level is not None and schema_keys:
                key_tuple = tuple(
                    _schema_str(schema_keys.get(k, "")) for k in self.dataset_schema_keys
                    if k in schema_keys
                )
                combo_key = (schema_level, key_tuple)
                if combo_key not in unique_schema_combos:
                    unique_schema_combos[combo_key] = schema_keys

        timings["2_split_metadata"] = time.perf_counter() - t1

        # Resolve schema_ids for all unique combos (batch)
        t2 = time.perf_counter()
        schema_id_cache = self._duck.batch_get_or_create_schema_ids(
            {k: {col: _schema_str(v) for col, v in vals.items()}
             for k, vals in unique_schema_combos.items()}
        )
        timings["3_schema_resolution"] = time.perf_counter() - t2

        # --- Per-row Python computation (no SQL) ---
        t4 = time.perf_counter()
        timestamp = datetime.now().isoformat()
        record_ids = []
        data_table_rows = []   # (record_id, ...data_cols)
        metadata_rows = []     # tuples for _record_metadata

        for i, (data_val, flat_meta) in enumerate(data_items):
            nested = all_nested[i]
            schema_keys = nested.get("schema", {})
            version_keys = nested.get("version", {})
            schema_level = self._infer_schema_level(schema_keys)

            if schema_level is not None and schema_keys:
                key_tuple = tuple(
                    _schema_str(schema_keys.get(k, "")) for k in self.dataset_schema_keys
                    if k in schema_keys
                )
                schema_id = schema_id_cache[(schema_level, key_tuple)]
            else:
                schema_id = 0

            content_hash = canonical_hash(data_val)
            record_id = generate_record_id(
                class_name=type_name,
                schema_version=schema_version,
                content_hash=content_hash,
                metadata=nested,
            )
            record_ids.append(record_id)

            # DataFrames expand to one DuckDB row per table row.
            if is_dataframe:
                for storage_row in _dataframe_to_storage_rows(data_val, dtype_meta):
                    data_table_rows.append((record_id,) + tuple(storage_row))
            else:
                storage_values = _value_to_storage_row(data_val, dtype_meta)
                data_table_rows.append((record_id,) + tuple(storage_values))

            vk_json = json.dumps(version_keys or {}, sort_keys=True)
            metadata_rows.append((
                record_id, timestamp, type_name, schema_id,
                vk_json, content_hash, None, schema_version, user_id,
            ))

        timings["4_per_row_hashing"] = time.perf_counter() - t4

        # --- Find which data rows are new (dedup check) ---
        t5 = time.perf_counter()
        if is_dataframe:
            # No PRIMARY KEY: filter out rows whose record_id already exists.
            all_new_rids = list({row[0] for row in data_table_rows})
            if all_new_rids:
                placeholders_rids = ", ".join(["?"] * len(all_new_rids))
                existing_rids = {r[0] for r in self._duck._fetchall(
                    f'SELECT DISTINCT record_id FROM "{table_name}" '
                    f'WHERE record_id IN ({placeholders_rids})',
                    all_new_rids,
                )}
            else:
                existing_rids = set()
            new_data_rows = [row for row in data_table_rows if row[0] not in existing_rids]
        else:
            # PRIMARY KEY: ON CONFLICT DO NOTHING handles dedup in the INSERT.
            new_data_rows = data_table_rows

        timings["5_dedup_check"] = time.perf_counter() - t5

        # --- Batch inserts ---
        t6 = time.perf_counter()
        self._duck._begin()
        try:
            if new_data_rows:
                all_columns = ["record_id"] + list(data_col_types.keys())
                data_df = pd.DataFrame(new_data_rows, columns=all_columns)
                col_str = ", ".join(f'"{c}"' for c in all_columns)
                if is_dataframe:
                    self._duck.con.execute(
                        f'INSERT INTO "{table_name}" ({col_str}) SELECT * FROM data_df'
                    )
                else:
                    self._duck.con.execute(
                        f'INSERT INTO "{table_name}" ({col_str}) SELECT * FROM data_df '
                        f'ON CONFLICT (record_id) DO NOTHING'
                    )

            # Always insert metadata rows (audit trail — every execution logged)
            meta_df = pd.DataFrame(
                metadata_rows,
                columns=[
                    "record_id", "timestamp", "variable_name", "schema_id",
                    "version_keys", "content_hash", "lineage_hash",
                    "schema_version", "user_id",
                ],
            )
            self._duck.con.execute(
                "INSERT INTO _record_metadata ("
                "record_id, timestamp, variable_name, schema_id, "
                "version_keys, content_hash, lineage_hash, schema_version, user_id"
                ") SELECT * FROM meta_df "
                "ON CONFLICT (record_id, timestamp) DO NOTHING"
            )

            # Upsert _variables (one row per variable)
            effective_level = (
                self._infer_schema_level(all_nested[0].get("schema", {}))
                or self.dataset_schema_keys[-1]
            )
            self._duck._execute(
                "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
                [type_name, effective_level, json.dumps(dtype_meta), ""],
            )

            self._duck._commit()
        except Exception:
            try:
                self._duck._execute("ROLLBACK")
            except Exception:
                pass
            raise

        timings["6_batch_inserts"] = time.perf_counter() - t6
        timings["total"] = time.perf_counter() - t0

        if profile:
            print(f"\n--- save_batch() profile ({len(data_items)} items, "
                  f"{len(unique_schema_combos)} unique schemas) ---")
            for phase, elapsed in timings.items():
                print(f"  {phase:30s} {elapsed:8.3f}s")
            print()

        return record_ids

    @staticmethod
    def _has_custom_serialization(variable_class: type) -> bool:
        """Check if a BaseVariable subclass overrides to_db or from_db."""
        return "to_db" in variable_class.__dict__ or "from_db" in variable_class.__dict__

    def _find_record(
        self,
        type_name: str,
        record_id: str | None = None,
        nested_metadata: dict | None = None,
        version_id: str = "all",
        branch_params_filter: dict | None = None,
        include_excluded: bool = False,
    ) -> pd.DataFrame:
        """
        Query _record_metadata to find matching records.

        Supports two modes:
        - By record_id: direct primary key lookup (with JOINs for full row data)
        - By metadata: filter by schema keys via JOIN with _schema, optionally
          filter by version_keys JSON, order by timestamp DESC

        version_id controls which versions are returned:
        - "all" (default): no version filtering (return every version)
        - "latest": only the latest row per (variable_name, schema_id, version_keys)

        branch_params_filter: optional dict of branch_params key/value filters
        include_excluded: if False (default), skip records with excluded=TRUE

        Schema key values and version key values may be lists, interpreted as
        "match any" (SQL IN / Python in).

        Returns a DataFrame of matching rows including schema columns and version_keys.
        """
        # Build schema column SELECT list
        schema_col_select = ", ".join(
            f's."{col}"' for col in self.dataset_schema_keys
        )

        excluded_clause = "" if include_excluded else " AND COALESCE(rm.excluded, FALSE) = FALSE"

        if record_id is not None:
            sql = (
                f"SELECT rm.*, {schema_col_select} "
                f"FROM _record_metadata rm "
                f"LEFT JOIN _schema s ON rm.schema_id = s.schema_id "
                f"WHERE rm.record_id = ? AND rm.variable_name = ?{excluded_clause}"
            )
            return self._duck._fetchdf(sql, [record_id, type_name])

        # By metadata
        schema_keys = nested_metadata.get("schema", {}) if nested_metadata else {}
        version_keys = nested_metadata.get("version", {}) if nested_metadata else {}

        conditions = ["rm.variable_name = ?"]
        params: list[Any] = [type_name]

        # Exclude excluded variants by default
        if not include_excluded:
            conditions.append("COALESCE(rm.excluded, FALSE) = FALSE")

        # Filter schema keys via _schema columns in SQL (lists → IN)
        for key, value in schema_keys.items():
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["?"] * len(value))
                conditions.append(f's."{key}" IN ({placeholders})')
                params.extend([_schema_str(v) for v in value])
            else:
                conditions.append(f's."{key}" = ?')
                params.append(_schema_str(value))

        where = " AND ".join(conditions)

        if version_id == "latest":
            # One row per (variable_name, schema_id, version_keys) — latest config only
            partition = "rm.variable_name, rm.schema_id, rm.version_keys"
        else:
            # "all": one row per distinct record_id — deduplicates re-runs of identical
            # data while still returning multiple distinct data records at the same
            # schema location (different content hash → different record_id).
            partition = "rm.record_id"

        sql = (
            f"WITH ranked AS ("
            f"SELECT rm.*, {schema_col_select}, "
            f"ROW_NUMBER() OVER ("
            f"PARTITION BY {partition} "
            f"ORDER BY rm.timestamp DESC"
            f") as rn "
            f"FROM _record_metadata rm "
            f"LEFT JOIN _schema s ON rm.schema_id = s.schema_id "
            f"WHERE {where}"
            f") SELECT * FROM ranked WHERE rn = 1 "
            f"ORDER BY timestamp DESC"
        )
        df = self._duck._fetchdf(sql, params)

        # Filter by version keys via Python-side JSON parsing (lists → in)
        if version_keys and len(df) > 0:
            for key, value in version_keys.items():
                if isinstance(value, (list, tuple)):
                    mask = df["version_keys"].apply(
                        lambda vk, k=key, vals=value: json.loads(vk).get(k) in vals
                        if vk is not None and isinstance(vk, str) else False
                    )
                else:
                    mask = df["version_keys"].apply(
                        lambda vk, k=key, v=value: json.loads(vk).get(k) == v
                        if vk is not None and isinstance(vk, str) else False
                    )
                df = df[mask]

        # Filter by branch_params_filter via Python-side matching.
        # Keys are checked against version_keys first (direct saves store
        # non-schema kwargs there), then fall back to branch_params suffix
        # matching (for_each saves store pipeline params there).
        if branch_params_filter and len(df) > 0:
            for key, value in branch_params_filter.items():
                def _match_row(row, k=key, v=value):
                    bp = json.loads(row["branch_params"] or "{}") if row.get("branch_params") else {}
                    # Check branch_params ambiguity BEFORE version_keys shortcut:
                    # if the bare key is ambiguous across multiple pipeline steps,
                    # raise AmbiguousParamError even if the key also appears in version_keys.
                    if k not in bp:
                        suffix = f".{k}"
                        hits = [bk for bk in bp if bk.endswith(suffix)]
                        if len(hits) > 1:
                            raise AmbiguousParamError(
                                f"'{k}' matches multiple branch params: {hits}"
                            )
                    vk = json.loads(row["version_keys"] or "{}") if row.get("version_keys") else {}
                    if k in vk:
                        return vk[k] == v
                    return _match_branch_param(bp, k, v)
                df = df[df.apply(_match_row, axis=1)]

        return df

    def _reconstruct_metadata_from_row(self, row: pd.Series) -> tuple[dict, dict]:
        """
        Reconstruct flat and nested metadata from a JOINed row.

        The row contains schema columns from _schema and version_keys from
        _variables, which together form the complete metadata.

        Returns (flat_metadata, nested_metadata).
        """
        schema = {}
        for key in self.dataset_schema_keys:
            if key in row.index:
                val = row[key]
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    schema[key] = _from_schema_str(val)

        vk_raw = row.get("version_keys")
        version = {}
        if vk_raw is not None and isinstance(vk_raw, str):
            version = json.loads(vk_raw)

        nested_metadata = {"schema": schema, "version": version}
        flat_metadata = {}
        flat_metadata.update(schema)
        flat_metadata.update(version)
        return flat_metadata, nested_metadata

    def _deserialize_custom_subdf(
        self,
        variable_class: type[BaseVariable],
        sub_df: pd.DataFrame,
        dtype_meta: dict,
    ):
        """Deserialize a sub-DataFrame using custom dtype metadata.

        Handles four sub-paths based on dtype_meta flags:
        - dict_of_arrays: reconstruct dict of numpy arrays
        - from_db: class-level custom deserialization
        - struct_columns: unflatten dot-separated columns
        - raw: return DataFrame as-is

        The sub_df must already have internal columns
        (record_id) dropped.
        """
        if dtype_meta.get("dict_of_arrays"):
            ndarray_keys = dtype_meta.get("ndarray_keys", {})
            data = {}
            for col in sub_df.columns:
                arr = sub_df[col].values
                if col in ndarray_keys:
                    col_meta = ndarray_keys[col]
                    arr = arr.astype(np.dtype(col_meta["dtype"]))
                    orig_shape = col_meta.get("shape")
                    if orig_shape and len(orig_shape) == 2:
                        if orig_shape[0] == 1:
                            arr = arr.reshape(1, -1)
                        elif orig_shape[1] == 1:
                            arr = arr.reshape(-1, 1)
                        else:
                            try:
                                arr = arr.reshape(orig_shape)
                            except ValueError:
                                pass
                data[col] = arr
            return data
        elif self._has_custom_serialization(variable_class):
            return variable_class.from_db(sub_df)
        elif dtype_meta.get("struct_columns"):
            return _unflatten_struct_columns(sub_df, dtype_meta["struct_columns"])
        else:
            return sub_df

    def _load_by_record_row(
        self,
        variable_class: type[BaseVariable],
        row: pd.Series,
        loc: Any = None,
        iloc: Any = None,
    ) -> BaseVariable:
        """
        Load a variable instance given a row from _record_metadata.

        Determines native vs custom deserialization from _variables.dtype,
        loads data from the data table by record_id, and constructs the
        BaseVariable instance.
        """
        type_name = row["variable_name"]
        table_name = type_name + "_data"
        record_id = row["record_id"]
        content_hash = row["content_hash"]
        lineage_hash = row["lineage_hash"]
        # Normalize NaN to None (DuckDB may return NaN for NULL in some contexts)
        if lineage_hash is not None and not isinstance(lineage_hash, str):
            lineage_hash = None
        flat_metadata, nested_metadata = self._reconstruct_metadata_from_row(row)

        # Get dtype from _variables to determine deserialization path
        dtype_rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [type_name],
        )

        if not dtype_rows:
            raise NotFoundError(
                f"No dtype found for {type_name} in _variables"
            )

        dtype_meta = json.loads(dtype_rows[0][0])
        is_custom = dtype_meta.get("custom", False)

        if is_custom:
            # Custom path: query by record_id
            df = self._duck._fetchdf(
                f'SELECT * FROM "{table_name}" WHERE record_id = ?',
                [record_id],
            )
            # Drop record_id column (internal identifier)
            df = df.drop(columns=["record_id"], errors="ignore")

            if loc is not None:
                if not isinstance(loc, (list, range, slice)):
                    loc = [loc]
                df = df.loc[loc]
            elif iloc is not None:
                if not isinstance(iloc, (list, range, slice)):
                    iloc = [iloc]
                df = df.iloc[iloc]

            data = self._deserialize_custom_subdf(variable_class, df, dtype_meta)
        else:
            # Native path: query by record_id, restore type
            row_df = self._duck._fetchdf(
                f'SELECT * FROM "{table_name}" WHERE record_id = ?',
                [record_id],
            )
            row_df = row_df.drop(columns=["record_id"], errors="ignore")

            mode = dtype_meta.get("mode", "single_column")
            columns_meta = dtype_meta.get("columns", {})

            if mode == "dataframe":
                # One DuckDB row per DataFrame row: apply _storage_to_python per cell.
                result = {}
                for c, meta in columns_meta.items():
                    if c in row_df.columns:
                        result[c] = [_storage_to_python(row_df[c].iloc[i], meta)
                                     for i in range(len(row_df))]
                df_columns = dtype_meta.get("df_columns", list(columns_meta.keys()))
                data = pd.DataFrame(result, columns=df_columns)
            else:
                row_df = self._duck._restore_types(row_df, dtype_meta)
                if len(row_df) == 1:
                    if mode == "single_column":
                        col_name = next(iter(columns_meta))
                        data = row_df[col_name].iloc[0]
                    elif mode == "multi_column":
                        result = {}
                        for c, meta in columns_meta.items():
                            result[c] = _storage_to_python(row_df[c].iloc[0], meta)
                        if dtype_meta.get("nested"):
                            data = _unflatten_dict(result, dtype_meta["path_map"])
                        else:
                            data = result
                    else:
                        data = row_df
                else:
                    data = row_df

        instance = variable_class(data)
        instance.record_id = record_id
        instance.metadata = flat_metadata
        instance.content_hash = content_hash
        instance.lineage_hash = lineage_hash
        try:
            bp_raw = row["branch_params"] if "branch_params" in row.index else None
            instance.branch_params = json.loads(bp_raw or "{}") if isinstance(bp_raw, str) else {}
        except Exception:
            instance.branch_params = {}

        return instance

    def register(self, variable_class: Type[BaseVariable]) -> None:
        """
        Register a variable type for storage.

        Args:
            variable_class: The BaseVariable subclass to register
        """
        type_name = variable_class.__name__
        table_name = variable_class.table_name()
        schema_version = variable_class.schema_version

        # Register in metadata table (skip if already registered)
        existing = self._duck._fetchall(
            "SELECT 1 FROM _registered_types WHERE type_name = ?",
            [type_name],
        )
        if not existing:
            self._duck._execute(
                """
                INSERT INTO _registered_types (type_name, table_name, schema_version)
                VALUES (?, ?, ?)
                """,
                [type_name, table_name, schema_version],
            )

        # Cache locally
        self._registered_types[type_name] = variable_class

    def _ensure_registered(
        self, variable_class: Type[BaseVariable], auto_register: bool = True
    ) -> str:
        """
        Ensure a variable type is registered.

        Returns:
            The table name for this variable type
        """
        type_name = variable_class.__name__

        if type_name in self._registered_types:
            return variable_class.table_name()

        # Check database
        rows = self._duck._fetchall(
            "SELECT table_name FROM _registered_types WHERE type_name = ?",
            [type_name],
        )

        if not rows:
            if auto_register:
                self.register(variable_class)
                return variable_class.table_name()
            else:
                raise NotRegisteredError(
                    f"Variable type '{type_name}' is not registered. "
                    f"No data has been saved for this type yet."
                )

        self._registered_types[type_name] = variable_class
        return rows[0][0]

    def save_variable(
        self,
        variable_class: Type[BaseVariable],
        data: Any,
        index: Any = None,
        **metadata,
    ) -> str:
        """
        Save data as a variable.

        Accepts a BaseVariable instance (which may carry a lineage_hash) or
        raw data. For ThunkOutput / lineage-tracked saves, use
        scihist.save_variable() which wraps this method.

        Args:
            variable_class: The BaseVariable subclass to save as
            data: The data to save (BaseVariable or raw data)
            index: Optional index to set on the DataFrame
            **metadata: Addressing metadata (e.g., subject=1, trial=1)

        Returns:
            The record_id of the saved data
        """
        lineage_hash = None
        lineage_dict = None

        try:
            from scilineage.core import LineageFcnResult
            from scilineage.lineage import extract_lineage
            if isinstance(data, LineageFcnResult):
                # Use the invocation hash (not the result hash) so that
                # find_by_lineage(invocation) can look it up via compute_lineage_hash()
                lineage_hash = data.invoked.hash
                lineage_dict = extract_lineage(data).to_dict()
                data = data.data
        except ImportError:
            pass

        if isinstance(data, BaseVariable):
            raw_data = data.data
            lineage_hash = data.lineage_hash
        else:
            raw_data = data

        instance = variable_class(raw_data)

        record_id = self.save(
            instance, metadata, lineage=lineage_dict, lineage_hash=lineage_hash, index=index,
        )

        instance.record_id = record_id
        instance.metadata = metadata
        instance.lineage_hash = lineage_hash

        return record_id

    def save(
        self,
        variable: BaseVariable,
        metadata: dict,
        lineage: dict | None = None,
        lineage_hash: str | None = None,
        pipeline_lineage_hash: str | None = None,
        index: Any = None,
    ) -> str:
        """
        Save a variable to the database.

        Args:
            variable: The variable instance to save
            metadata: Addressing metadata (flat dict)
            lineage: Optional lineage dict with keys 'function_name', 'function_hash',
                'inputs', 'constants'
            lineage_hash: Optional pre-computed lineage hash (stored in DuckDB
                for input classification when this variable is reused later)
            pipeline_lineage_hash: Optional pre-computed lineage hash for cache
                lookup. If None, falls back to lineage_hash.
            index: Optional index to set on the DataFrame

        Returns:
            The record_id of the saved data
        """
        table_name = self._ensure_registered(type(variable))
        type_name = variable.__class__.__name__
        user_id = get_user_id()

        # Extract __branch_params before splitting metadata (it gets its own column)
        branch_params = None
        if isinstance(metadata, dict) and "__branch_params" in metadata:
            bp_raw = metadata["__branch_params"]
            try:
                branch_params = json.loads(bp_raw) if isinstance(bp_raw, str) else (bp_raw or {})
            except (json.JSONDecodeError, TypeError):
                branch_params = {}
            metadata = {k: v for k, v in metadata.items() if k != "__branch_params"}

        # Split metadata
        nested_metadata = self._split_metadata(metadata)

        # Warn if no metadata keys match the schema
        if metadata and not nested_metadata.get("schema"):
            warnings.warn(
                f"None of the metadata keys {list(metadata.keys())} match the "
                f"configured dataset_schema_keys {self.dataset_schema_keys}. "
                f"All keys will be treated as version parameters.",
                UserWarning,
                stacklevel=2,
            )

        # Normalize array.array values to numpy arrays (MATLAB bridge can produce these)
        import array as _array_mod
        if isinstance(variable.data, dict):
            for k, v in variable.data.items():
                if isinstance(v, _array_mod.array):
                    variable.data[k] = np.array(v)

        # Compute content hash
        content_hash = canonical_hash(variable.data)

        # Generate record_id
        record_id = generate_record_id(
            class_name=type_name,
            schema_version=variable.schema_version,
            content_hash=content_hash,
            metadata=nested_metadata,
        )

        # Wrap all writes in a single transaction to avoid repeated
        # WAL checkpoints (each auto-committed statement can trigger a
        # checkpoint/fsync, causing random multi-second stalls).
        self._duck._begin()

        try:
            schema_keys = nested_metadata.get("schema", {})
            version_keys = nested_metadata.get("version", {})
            schema_level = self._infer_schema_level(schema_keys)
            created_at = datetime.now().isoformat()

            if self._has_custom_serialization(type(variable)):
                # Custom serialization: user provides to_db() → DataFrame
                df = variable.to_db()

                if index is not None:
                    index_list = list(index) if not isinstance(index, list) else index
                    if len(index_list) != len(df):
                        raise ValueError(
                            f"Index length ({len(index_list)}) does not match "
                            f"DataFrame row count ({len(df)})"
                        )
                    df.index = index

                schema_id = self._save_columnar(
                    record_id, table_name, type(variable), df,
                    schema_level, schema_keys, content_hash,
                )
            else:
                # ALL other data: scalars, arrays, lists, dicts, dict-of-arrays,
                # and native DataFrames (stored as a single record with array-typed
                # columns, e.g. DOUBLE[], BIGINT[], VARCHAR[]).
                schema_id = self._save_native(
                    record_id, table_name, type(variable), variable.data, content_hash,
                    schema_level=schema_level, schema_keys=schema_keys,
                )

            self._save_record_metadata(
                record_id=record_id,
                timestamp=created_at,
                variable_name=type_name,
                schema_id=schema_id,
                version_keys=version_keys,
                content_hash=content_hash,
                lineage_hash=lineage_hash,
                schema_version=variable.schema_version,
                user_id=user_id,
                branch_params=branch_params,
            )

            # Save lineage if provided
            if lineage is not None:
                effective_plh = pipeline_lineage_hash if pipeline_lineage_hash is not None else lineage_hash
                self._save_lineage(
                    record_id, type_name, lineage, effective_plh, user_id,
                    schema_keys=nested_metadata.get("schema"),
                    output_content_hash=content_hash,
                )

            self._duck._commit()

        except Exception:
            try:
                self._duck._rollback()
            except Exception:
                pass  # Connection may already be closed
            raise

        return record_id

    def _save_lineage(
        self,
        output_record_id: str,
        output_type: str,
        lineage: dict,
        lineage_hash: str | None = None,
        user_id: str | None = None,
        schema_keys: dict | None = None,
        output_content_hash: str | None = None,
    ) -> None:
        """Save one lineage row to DuckDB _lineage table.

        Args:
            lineage: Dict with keys 'function_name', 'function_hash',
                     'inputs', 'constants'.
        """
        lh = lineage_hash or output_record_id
        inputs_json = json.dumps(lineage.get("inputs", []), sort_keys=True)
        constants_json = json.dumps(lineage.get("constants", {}), sort_keys=True)
        timestamp = datetime.now().isoformat()

        self._duck._execute(
            "INSERT INTO _lineage "
            "(output_record_id, lineage_hash, target, function_name, function_hash, "
            " inputs, constants, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (output_record_id) DO NOTHING",
            [output_record_id, lh, output_type, lineage.get("function_name"),
             lineage.get("function_hash"), inputs_json, constants_json, timestamp],
        )

    def _load_with_where(
        self,
        variable_class: Type[BaseVariable],
        metadata: dict,
        table_name: str,
        where,
        version_id: str = "latest",
    ):
        """Load records using where= filter with version_keys-first strategy.

        When data was saved via for_each with a where= condition, the filter
        string is stored as a ``__where`` version key. This method first tries
        to match records by that version key. If no records are found (e.g. data
        was saved directly without for_each), it falls back to schema-level
        filtering via ``where.resolve()``.

        Returns:
            A pandas DataFrame of matching record rows.

        Raises:
            NotFoundError: If no records match either strategy.
        """
        type_name = variable_class.__name__

        # Strategy 1: filter by __where version key
        augmented = dict(metadata)
        augmented["__where"] = where.to_key()
        nested = self._split_metadata(augmented)
        records = self._find_record(type_name, nested_metadata=nested, version_id=version_id)

        if len(records) > 0:
            return records

        # Strategy 2: fallback to schema-level filtering (backward compat)
        # This path is used when data was saved without for_each (no __where
        # version key). Validation errors from where.resolve() propagate
        # normally — only cross-level filtering silently falls through.
        nested = self._split_metadata(metadata)
        records = self._find_record(type_name, nested_metadata=nested, version_id=version_id)
        if len(records) > 0:
            allowed_schema_ids = where.resolve(self, variable_class, table_name)
            records = records[records["schema_id"].isin(allowed_schema_ids)]

        if len(records) == 0:
            raise NotFoundError(
                f"No {type_name} found matching metadata: {metadata} "
                f"with the given where= filter."
            )

        return records

    def load(
        self,
        variable_class: Type[BaseVariable],
        metadata: dict,
        version: str = "latest",
        loc: Any = None,
        iloc: Any = None,
        where=None,
    ) -> BaseVariable:
        """
        Load a single variable matching the given metadata.

        Args:
            variable_class: The type to load
            metadata: Flat metadata dict
            version: "latest" for most recent, or specific record_id
            loc: Optional label-based index selection
            iloc: Optional integer position-based index selection
            where: Optional Filter for restricting which records are loaded.
                When data was saved via for_each with a where= condition, the
                filter is stored as a __where version key. At load time, this
                parameter first tries to match by version key, then falls back
                to schema-level filtering for backward compatibility.

        Returns:
            The matching variable instance
        """
        table_name = self._ensure_registered(variable_class, auto_register=True)

        try:
            if version != "latest" and version is not None:
                # Load by specific record_id — always include excluded for direct lookup
                records = self._find_record(
                    variable_class.__name__, record_id=version, include_excluded=True,
                )
                if len(records) == 0:
                    raise NotFoundError(f"No data found with record_id '{version}'")
            elif where is not None:
                # where= specified: first try version_keys filtering (__where)
                records = self._load_with_where(
                    variable_class, metadata, table_name, where
                )
            else:
                # Load by metadata (latest version per parameter set)
                nested_metadata = self._split_metadata(metadata)
                records = self._find_record(variable_class.__name__, nested_metadata=nested_metadata, version_id="latest")
                if len(records) == 0:
                    raise NotFoundError(
                        f"No {variable_class.__name__} found matching metadata: {metadata}"
                    )

            # Take the first (latest) record
            row = records.iloc[0]
        except NotFoundError:
            raise
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                raise NotFoundError(
                    f"No {variable_class.__name__} found matching metadata: {metadata}"
                )
            raise

        return self._load_by_record_row(variable_class, row, loc=loc, iloc=iloc)

    def load_all(
        self,
        variable_class: Type[BaseVariable],
        metadata: dict,
        version_id: str = "all",
        where=None,
        branch_params_filter: dict | None = None,
    ):
        """
        Load all variables matching the given metadata as a generator.

        Args:
            variable_class: The type to load
            metadata: Flat metadata dict
            version_id: Which versions to return:
                - "all" (default): return every version
                - "latest": return only the latest version per (schema_id, version_keys)
            where: Optional Filter for restricting which records are loaded.
                First tries version_keys filtering (__where), then falls back
                to schema-level filtering for backward compatibility.
            branch_params_filter: Optional dict of branch_params key/value filters.

        Yields:
            BaseVariable instances matching the metadata
        """
        table_name = self._ensure_registered(variable_class, auto_register=True)

        if where is not None:
            # where= specified: first try version_keys filtering (__where)
            try:
                records = self._load_with_where(
                    variable_class, metadata, table_name, where,
                    version_id=version_id,
                )
            except NotFoundError:
                return
        else:
            nested_metadata = self._split_metadata(metadata)
            try:
                records = self._find_record(
                    variable_class.__name__, nested_metadata=nested_metadata,
                    version_id=version_id,
                    branch_params_filter=branch_params_filter,
                )
            except NotFoundError:
                return  # No data

            if len(records) == 0:
                return

        # --- Bulk loading path ---

        # 1. Get dtype from _variables (one row per variable)
        dtype_rows = self._duck._fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [variable_class.__name__],
        )
        if not dtype_rows:
            return
        dtype_meta = json.loads(dtype_rows[0][0])
        is_custom = dtype_meta.get("custom", False)

        # 2. Collect all unique record_ids to fetch
        all_record_ids = records["record_id"].tolist()
        if not all_record_ids:
            return

        # 3. Batch fetch data rows by record_id
        data_lookup: dict[str, Any] = {}  # record_id -> deserialized value

        chunk_size = 500
        for start in range(0, len(all_record_ids), chunk_size):
            chunk = all_record_ids[start:start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))

            if is_custom:
                # Custom (columnar) path: fetch all rows for this chunk
                sql = f'SELECT * FROM "{table_name}" WHERE record_id IN ({placeholders})'
                chunk_df = self._duck._fetchdf(sql, chunk)

                if len(chunk_df) > 0:
                    grouped = chunk_df.groupby("record_id", sort=False)
                    for rid, sub_df in grouped:
                        sub_df = sub_df.drop(
                            columns=["record_id"], errors="ignore"
                        ).reset_index(drop=True)
                        data_lookup[rid] = self._deserialize_custom_subdf(
                            variable_class, sub_df, dtype_meta,
                        )
            else:
                # Native path
                data_cols = list(dtype_meta.get("columns", {}).keys())
                data_select = ", ".join(f'"{c}"' for c in data_cols)
                sql = (
                    f'SELECT record_id, {data_select} FROM "{table_name}" '
                    f'WHERE record_id IN ({placeholders})'
                )
                chunk_df = self._duck._fetchdf(sql, chunk)

                if len(chunk_df) > 0:
                    mode = dtype_meta.get("mode", "single_column")
                    columns_meta = dtype_meta.get("columns", {})

                    if mode == "dataframe":
                        # One DuckDB row per DataFrame row: group by record_id.
                        df_columns = dtype_meta.get("df_columns", list(columns_meta.keys()))
                        for rid, group_df in chunk_df.groupby("record_id", sort=False):
                            group_df = group_df.drop(
                                columns=["record_id"], errors="ignore"
                            ).reset_index(drop=True)
                            result = {}
                            for c, meta in columns_meta.items():
                                if c in group_df.columns:
                                    result[c] = [_storage_to_python(group_df[c].iloc[i], meta)
                                                 for i in range(len(group_df))]
                            data_lookup[rid] = pd.DataFrame(result, columns=df_columns)
                    else:
                        # Non-DataFrame: restore types, then one value per record_id row.
                        restored = chunk_df[data_cols].copy()
                        restored = self._duck._restore_types(restored, dtype_meta)

                        if mode == "single_column":
                            col_name = next(iter(columns_meta))
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = restored[col_name].iloc[i]
                        elif mode == "multi_column":
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                result = {}
                                for c, meta in columns_meta.items():
                                    result[c] = _storage_to_python(restored[c].iloc[i], meta)
                                if dtype_meta.get("nested"):
                                    data_lookup[rid] = _unflatten_dict(result, dtype_meta["path_map"])
                                else:
                                    data_lookup[rid] = result
                        else:
                            col_names = list(columns_meta.keys())
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = {c: restored[c].iloc[i] for c in col_names}

        # 4. Construct instances using itertuples + inline metadata
        schema_keys = self.dataset_schema_keys
        for row in records.itertuples(index=False):
            record_id = row.record_id

            if record_id not in data_lookup:
                continue

            data_value = data_lookup[record_id]
            content_hash = row.content_hash
            lineage_hash = row.lineage_hash
            if lineage_hash is not None and not isinstance(lineage_hash, str):
                lineage_hash = None

            flat_metadata = {}
            for sk in schema_keys:
                val = getattr(row, sk, None)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    flat_metadata[sk] = _from_schema_str(val)
            vk_raw = getattr(row, "version_keys", None)
            if vk_raw is not None and isinstance(vk_raw, str):
                flat_metadata.update(json.loads(vk_raw))

            instance = variable_class(data_value)
            instance.record_id = record_id
            instance.metadata = flat_metadata
            instance.content_hash = content_hash
            instance.lineage_hash = lineage_hash
            bp_raw = getattr(row, "branch_params", None)
            instance.branch_params = json.loads(bp_raw or "{}") if isinstance(bp_raw, str) else {}

            yield instance

    def list_versions(
        self,
        variable_class: Type[BaseVariable],
        include_excluded: bool = False,
        **metadata,
    ) -> list[dict]:
        """
        List all versions at a schema location.

        Args:
            variable_class: The type to query
            include_excluded: If True, include excluded variants in results.
            **metadata: Schema metadata to match; non-schema keys are treated
                as branch_params filters.

        Returns:
            List of dicts with record_id, schema, branch_params, timestamp
            (plus "excluded" bool when include_excluded=True).
        """
        self._ensure_registered(variable_class, auto_register=True)

        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in metadata.items() if k in schema_keys_set}
        branch_params_filter = {k: v for k, v in metadata.items() if k not in schema_keys_set} or None

        nested_metadata = self._split_metadata(schema_metadata)

        try:
            records = self._find_record(
                variable_class.__name__,
                nested_metadata=nested_metadata,
                version_id="all",
                branch_params_filter=branch_params_filter,
                include_excluded=include_excluded,
            )
        except Exception:
            return []

        results = []
        for _, row in records.iterrows():
            _, nested = self._reconstruct_metadata_from_row(row)
            bp_raw = row.get("branch_params") if hasattr(row, 'get') else row["branch_params"]
            bp = json.loads(bp_raw or "{}") if isinstance(bp_raw, str) else {}
            entry = {
                "record_id": row["record_id"],
                "schema": nested.get("schema", {}),
                "branch_params": bp,
                "timestamp": row["timestamp"],
            }
            if include_excluded:
                exc = row.get("excluded") if hasattr(row, 'get') else row["excluded"]
                entry["excluded"] = bool(exc) if exc is not None else False
            results.append(entry)

        # Sort by timestamp descending
        results.sort(key=lambda x: x["timestamp"], reverse=True)
        return results

    def _resolve_record_id(
        self,
        record_id_or_type: "str | Type[BaseVariable]",
        **kwargs,
    ) -> str:
        """Resolve a record_id string or (variable_class, **kwargs) to a single record_id.

        Raises AmbiguousVersionError if multiple records match, NotFoundError if none.
        Always searches including excluded records.
        """
        if isinstance(record_id_or_type, str):
            return record_id_or_type

        variable_class = record_id_or_type
        schema_keys_set = set(self.dataset_schema_keys)
        schema_metadata = {k: v for k, v in kwargs.items() if k in schema_keys_set}
        branch_params_filter = {k: v for k, v in kwargs.items() if k not in schema_keys_set} or None

        nested_metadata = self._split_metadata(schema_metadata)
        records = self._find_record(
            variable_class.__name__,
            nested_metadata=nested_metadata,
            version_id="all",
            branch_params_filter=branch_params_filter,
            include_excluded=True,
        )

        if len(records) == 0:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching: {kwargs}"
            )
        if len(records) > 1:
            ids = records["record_id"].tolist()
            raise AmbiguousVersionError(
                f"{len(records)} records match for {variable_class.__name__} "
                f"with {kwargs}. "
                f"Pass a record_id directly or narrow with more branch parameters. "
                f"Matching record_ids: {ids}"
            )
        return records.iloc[0]["record_id"]

    def exclude_variant(
        self,
        record_id_or_type: "str | Type[BaseVariable]",
        **kwargs,
    ) -> None:
        """Mark a variant as excluded from automatic inclusion in for_each and load().

        Usage:
            db.exclude_variant("abc123")                                  # by record_id
            db.exclude_variant(DetectedSpikes, subject="S01", low_hz=20)  # by params
        """
        record_id = self._resolve_record_id(record_id_or_type, **kwargs)
        self._duck._execute(
            "UPDATE _record_metadata SET excluded = TRUE WHERE record_id = ?",
            [record_id],
        )

    def include_variant(
        self,
        record_id_or_type: "str | Type[BaseVariable]",
        **kwargs,
    ) -> None:
        """Re-include a previously excluded variant.

        Usage:
            db.include_variant("abc123")
            db.include_variant(DetectedSpikes, subject="S01", low_hz=20)
        """
        record_id = self._resolve_record_id(record_id_or_type, **kwargs)
        self._duck._execute(
            "UPDATE _record_metadata SET excluded = FALSE WHERE record_id = ?",
            [record_id],
        )

    def get_provenance(
        self,
        variable_class: Type[BaseVariable] | None,
        version: str | None = None,
        **metadata,
    ) -> dict | None:
        """
        Get the provenance (lineage) of a variable.

        Returns:
            Dict with function_name, function_hash, inputs, constants
            or None if no lineage recorded
        """
        if version:
            record_id = version
        else:
            var = self.load(variable_class, metadata)
            record_id = var.record_id

        rows = self._duck._fetchall(
            "SELECT function_name, function_hash, inputs, constants "
            "FROM _lineage WHERE output_record_id = ?",
            [record_id],
        )
        if not rows:
            return None

        function_name, function_hash, inputs_json, constants_json = rows[0]
        return {
            "function_name": function_name,
            "function_hash": function_hash,
            "inputs": json.loads(inputs_json),
            "constants": json.loads(constants_json),
        }

    def get_provenance_by_schema(self, **schema_keys) -> list[dict]:
        """
        Get all provenance records at a schema location (schema-aware view).

        Args:
            **schema_keys: Schema key filters (e.g., subject="S01", session="1")

        Returns:
            List of lineage record dicts matching the schema keys
        """
        conditions = ["rm.lineage_hash IS NOT NULL"]
        params: list[Any] = []
        for key, value in schema_keys.items():
            conditions.append(f's."{key}" = ?')
            params.append(_schema_str(value))

        where = " AND ".join(conditions)
        rows = self._duck._fetchall(
            f"SELECT l.output_record_id, rm.variable_name, "
            f"l.function_name, l.function_hash, l.inputs, l.constants "
            f"FROM _lineage l "
            f"JOIN _record_metadata rm ON l.output_record_id = rm.record_id "
            f"LEFT JOIN _schema s ON rm.schema_id = s.schema_id "
            f"WHERE {where}",
            params,
        )

        results = []
        for record_id, variable_name, function_name, function_hash, inputs_json, constants_json in rows:
            results.append({
                "output_record_id": record_id,
                "output_type": variable_name,
                "function_name": function_name,
                "function_hash": function_hash,
                "inputs": json.loads(inputs_json),
                "constants": json.loads(constants_json),
            })
        return results

    def get_pipeline_structure(self) -> list[dict]:
        """
        Get the abstract pipeline structure (schema-blind view).

        Returns unique (function_name, function_hash, output_type, input_types)
        combinations, describing how variable types flow through functions
        without reference to specific data instances or schema locations.

        Returns:
            List of dicts with keys: function_name, function_hash, output_type,
            input_types (list of type names)
        """
        rows = self._duck._fetchall(
            "SELECT DISTINCT target, function_name, function_hash, inputs FROM _lineage"
        )
        seen = set()
        results = []
        for target, function_name, function_hash, inputs_json in rows:
            inputs = json.loads(inputs_json)
            input_types = tuple(sorted(
                inp.get("type", inp.get("source_function", "unknown"))
                for inp in inputs
            ))
            key = (function_name, function_hash, target, input_types)
            if key not in seen:
                seen.add(key)
                results.append({
                    "function_name": function_name,
                    "function_hash": function_hash,
                    "output_type": target,
                    "input_types": list(input_types),
                })
        return results

    def list_pipeline_variants(
        self,
        output_type: str | None = None,
    ) -> list[dict]:
        """
        List all distinct pipeline step variants recorded in the database.

        Each entry represents a unique (function, constants, output_type)
        combination — a "branch" of the pipeline. Two for_each runs on the
        same function with different constants produce two separate entries.

        Uses version_keys metadata stored by for_each; does not require the
        scilineage tracking system.

        Args:
            output_type: Optional variable type name to filter results
                         (e.g. "Filtered"). If None, all types are returned.

        Returns:
            List of dicts with keys:
                function_name (str),
                output_type   (str),
                input_types   (dict: param_name → type_name),
                constants     (dict: param_name → value),
                record_count  (int: distinct records for this variant)
        """
        sql = "SELECT variable_name, version_keys, record_id FROM _record_metadata"
        params: list = []
        if output_type is not None:
            sql += " WHERE variable_name = ?"
            params = [output_type]

        rows = self._duck._fetchall(sql, params)

        # Group by (variable_name, version_keys_without___upstream) in Python.
        # __upstream encodes which upstream variant was used (for record_id uniqueness)
        # but should not split pipeline-level grouping — two for_each calls with the
        # same (fn, constants) are the same pipeline step regardless of upstream.
        from collections import defaultdict
        group_record_ids: dict = defaultdict(set)
        group_info: dict = {}

        for variable_name, version_keys_json, record_id in rows:
            vk = json.loads(version_keys_json or "{}") if version_keys_json else {}
            fn_name = vk.get("__fn")
            if not fn_name:
                continue  # Raw .save() record — no function, skip

            inputs_raw = vk.get("__inputs", "{}")
            constants_raw = vk.get("__constants", "{}")
            input_types = (
                json.loads(inputs_raw) if isinstance(inputs_raw, str) else (inputs_raw or {})
            )
            constants = (
                json.loads(constants_raw) if isinstance(constants_raw, str) else (constants_raw or {})
            )

            # Strip __upstream for pipeline-level grouping
            vk_for_group = {k: v for k, v in vk.items() if k != "__upstream"}
            group_key = (variable_name, json.dumps(vk_for_group, sort_keys=True))

            group_record_ids[group_key].add(record_id)
            if group_key not in group_info:
                group_info[group_key] = {
                    "function_name": fn_name,
                    "output_type": variable_name,
                    "input_types": input_types,
                    "constants": constants,
                }

        results = []
        for group_key, record_ids in group_record_ids.items():
            info = group_info[group_key]
            results.append({**info, "record_count": len(record_ids)})

        return results

    def get_upstream_provenance(
        self,
        record_id: str,
        max_depth: int = 20,
    ) -> list[dict]:
        """
        Traverse the full upstream provenance chain for a record.

        Walks backwards through the pipeline: for each record, inspects its
        version_keys (__fn, __inputs) to determine what variable types it was
        derived from, then finds those upstream records at the same schema
        location using branch_params subset matching (the upstream record's
        branch_params must be a subset of the current record's branch_params).

        Does not require the scilineage tracking system; uses version_keys
        and branch_params metadata stored by for_each.

        Args:
            record_id: The record_id to trace backwards from.
            max_depth: Maximum number of hops to follow (guards against cycles).

        Returns:
            Flat list of provenance nodes ordered from the queried record
            outward (BFS order). Each dict has keys:
                record_id     (str),
                variable_type (str),
                schema        (dict),
                branch_params (dict),
                function_name (str | None),
                constants     (dict),
                depth         (int, 0 = queried record),
                inputs        (list of {record_id, param_name, variable_type})
        """
        schema_col_select = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)

        visited: set = set()
        result: list = []
        queue: list = [(record_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)

            # Fetch this record's metadata
            rows = self._duck._fetchdf(
                f"SELECT rm.record_id, rm.variable_name, rm.version_keys, "
                f"rm.branch_params, rm.schema_id, {schema_col_select} "
                f"FROM _record_metadata rm "
                f"LEFT JOIN _schema s ON rm.schema_id = s.schema_id "
                f"WHERE rm.record_id = ? "
                f"ORDER BY rm.timestamp DESC LIMIT 1",
                [current_id],
            )
            if rows.empty:
                continue

            row = rows.iloc[0]
            vk = json.loads(row["version_keys"] or "{}") if row.get("version_keys") else {}
            bp = json.loads(row["branch_params"] or "{}") if row.get("branch_params") else {}
            fn_name = vk.get("__fn")
            input_types: dict = json.loads(vk["__inputs"]) if "__inputs" in vk else {}
            constants: dict = json.loads(vk["__constants"]) if "__constants" in vk else {}

            schema = {}
            for k in self.dataset_schema_keys:
                if k in row.index:
                    val = row[k]
                    if val is not None and not (isinstance(val, float) and pd.isna(val)):
                        schema[k] = _from_schema_str(val)

            schema_id = int(row["schema_id"])

            # For each input type, find the upstream record at the same schema
            # location whose branch_params is a subset of this record's branch_params.
            input_nodes: list = []
            for param_name, type_name in input_types.items():
                candidates = self._duck._fetchdf(
                    "SELECT DISTINCT rm.record_id, rm.branch_params "
                    "FROM _record_metadata rm "
                    "WHERE rm.variable_name = ? AND rm.schema_id = ? "
                    "AND COALESCE(rm.excluded, FALSE) = FALSE",
                    [type_name, schema_id],
                )

                matched_rid = None
                best_match_size = -1
                for _, cand in candidates.iterrows():
                    cand_bp = json.loads(cand["branch_params"] or "{}") if cand["branch_params"] else {}
                    # cand_bp must be a subset of bp (every key in cand_bp matches bp)
                    if all(bp.get(k) == v for k, v in cand_bp.items()):
                        # Prefer the most specific match (most keys)
                        if len(cand_bp) > best_match_size:
                            matched_rid = cand["record_id"]
                            best_match_size = len(cand_bp)

                if matched_rid:
                    input_nodes.append({
                        "record_id": matched_rid,
                        "param_name": param_name,
                        "variable_type": type_name,
                    })

            result.append({
                "record_id": current_id,
                "variable_type": row["variable_name"],
                "schema": schema,
                "branch_params": bp,
                "function_name": fn_name,
                "constants": constants,
                "depth": depth,
                "inputs": input_nodes,
            })

            for inp in input_nodes:
                queue.append((inp["record_id"], depth + 1))

        return result

    def has_lineage(self, record_id: str) -> bool:
        """Check if a variable has lineage information."""
        rows = self._duck._fetchall(
            "SELECT lineage_hash FROM _record_metadata "
            "WHERE record_id = ? AND lineage_hash IS NOT NULL",
            [record_id],
        )
        return len(rows) > 0 and bool(rows[0][0])

    def find_record_id(self, variable_class: type, metadata: dict) -> str | None:
        """Lightweight lookup returning the record_id of the latest record for
        a variable + metadata combination, without loading any data.

        Returns None if no matching record exists.
        """
        nested = self._split_metadata(metadata)
        rows = self._find_record(variable_class.__name__, nested_metadata=nested,
                                 version_id="latest")
        if rows.empty:
            return None
        return rows.iloc[0]["record_id"]

    def get_latest_record_id_for_variant(self, used_record_id: str) -> str | None:
        """Given a record_id, find the most recently saved record that shares the
        same (variable_name, schema_id, version_keys).

        This is the "current latest" for that specific variable variant —
        the same record that load(..., version_id="latest") would return.
        Returns None if the record no longer exists.
        """
        rows = self._duck._fetchdf(
            "SELECT variable_name, schema_id, version_keys "
            "FROM _record_metadata WHERE record_id = ? LIMIT 1",
            [used_record_id],
        )
        if rows.empty:
            return None

        vn = rows.iloc[0]["variable_name"]
        sid = int(rows.iloc[0]["schema_id"])
        vk = rows.iloc[0]["version_keys"]

        latest = self._duck._fetchdf(
            "SELECT record_id FROM _record_metadata "
            "WHERE variable_name = ? AND schema_id = ? AND version_keys IS NOT DISTINCT FROM ? "
            "AND COALESCE(excluded, FALSE) = FALSE "
            "ORDER BY timestamp DESC LIMIT 1",
            [vn, sid, vk],
        )
        if latest.empty:
            return None
        return latest.iloc[0]["record_id"]

    def get_function_hash_for_record(self, record_id: str) -> str | None:
        """Return the function_hash stored in _lineage for a record, or None.

        Used by scihist.for_each's skip_computed check to detect whether the
        function that produced a record has changed since it was saved.
        """
        rows = self._duck._fetchall(
            "SELECT function_hash FROM _lineage WHERE output_record_id = ?",
            [record_id],
        )
        return rows[0][0] if rows and rows[0][0] else None

    def get_lineage_inputs(self, record_id: str) -> list[dict]:
        """Return the list of input descriptors stored in _lineage for a record.

        Each entry is a dict as written by scilineage's ClassifiedInput.to_lineage_dict().
        Entries with ``source_type == "variable"`` carry a ``record_id`` field
        that identifies the exact input record used when this output was saved.

        Returns an empty list if no lineage row exists for the record.
        """
        rows = self._duck._fetchall(
            "SELECT inputs FROM _lineage WHERE output_record_id = ?",
            [record_id],
        )
        if not rows or not rows[0][0]:
            return []
        try:
            return json.loads(rows[0][0])
        except (json.JSONDecodeError, TypeError):
            return []

    # -------------------------------------------------------------------------
    # Export Methods
    # -------------------------------------------------------------------------

    def export_to_csv(
        self,
        variable_class: Type[BaseVariable],
        path: str,
        **metadata,
    ) -> int:
        """Export matching variables to a CSV file."""
        results = list(self.load_all(variable_class, metadata))

        if not results:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching metadata: {metadata}"
            )

        all_dfs = []
        for var in results:
            df = variable_class(var.data).to_db()
            df["_record_id"] = var.record_id
            if var.metadata:
                for key, value in var.metadata.items():
                    df[f"_meta_{key}"] = value
            all_dfs.append(df)

        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv(path, index=False)

        return len(results)

    def find_by_lineage_hash(self, lineage_hash: str) -> list | None:
        """
        Find output values by pipeline lineage hash.

        Low-level lookup used by scihist.find_by_lineage(). Queries
        _record_metadata joined to _lineage for records matching the given hash.

        Args:
            lineage_hash: The pipeline lineage hash to look up

        Returns:
            List of output values if found, None otherwise
        """
        records = self._duck._fetchall(
            "SELECT DISTINCT rm.record_id, rm.variable_name "
            "FROM _record_metadata rm "
            "JOIN _lineage l ON rm.record_id = l.output_record_id "
            "WHERE l.lineage_hash = ?",
            [lineage_hash],
        )
        if not records:
            return None

        results = []
        has_generated = False
        for record_id, variable_name in records:
            # Track generated entries (lineage-only, no data stored)
            if record_id.startswith("generated:"):
                has_generated = True
                continue

            var_class = self._get_variable_class(variable_name)
            if var_class is None:
                return None

            try:
                # Load data from DuckDB
                var = self.load(var_class, {}, version=record_id)
                results.append(var.data)
            except (KeyError, NotFoundError):
                # Record not found
                return None

        if results:
            return results
        if has_generated:
            return [None]
        return None

    def find_by_lineage(self, invocation) -> list | None:
        """
        Find output values by a lineage invocation object.

        Computes the lineage hash from the invocation and delegates to
        find_by_lineage_hash. Accepts any invocation with a
        compute_lineage_hash() method (e.g. LineageFcnInvocation,
        MatlabLineageFcnInvocation).

        Args:
            invocation: An invocation object with compute_lineage_hash()

        Returns:
            List of output values if found, None otherwise
        """
        lineage_hash = invocation.compute_lineage_hash()
        return self.find_by_lineage_hash(lineage_hash)

    def _get_variable_class(self, type_name: str):
        """Get a variable class by name (class name, not table name)."""
        if type_name in self._registered_types:
            return self._registered_types[type_name]

        return BaseVariable.get_subclass_by_name(type_name)

    def distinct_schema_values(self, key: str) -> list:
        """Return all distinct values stored for a schema key.

        Args:
            key: A schema key name (e.g. "subject", "session")

        Returns:
            Sorted list of distinct non-null values for that key
        """
        return self._duck.distinct_schema_values(key)

    def distinct_schema_combinations(self, keys: list[str]) -> list[tuple]:
        """Return all distinct combinations for multiple schema keys.

        Args:
            keys: List of schema key names (e.g. ["subject", "session"])

        Returns:
            List of tuples of distinct non-null value combinations (strings)
        """
        return self._duck.distinct_schema_combinations(keys)

    def list_variables(self) -> "pd.DataFrame":
        """Return all variable types stored in this database.

        Queries the ``_variables`` table and returns a DataFrame with columns:
        ``variable_name``, ``schema_level``, ``created_at``, ``description``.

        Useful for discovering what variable types exist in a database file
        without needing the original Python class definitions.
        """
        return self._duck.list_variables()

    # -------------------------------------------------------------------------
    # Variable Groups
    # -------------------------------------------------------------------------

    @staticmethod
    def _resolve_var_name(v) -> str:
        """Resolve a single variable to its name string.

        Accepts a Python str, a BaseVariable subclass (class object),
        or a MATLAB BaseVariable instance (matlab.object with class name).
        """
        if isinstance(v, str):
            return v
        if isinstance(v, type) and issubclass(v, BaseVariable):
            return v.table_name()
        # MATLAB objects cross the bridge as matlab.object; try str()
        # to extract the class name (e.g. "StepLength").
        s = str(v)
        if s:
            return s
        raise TypeError(
            f"Expected a string or BaseVariable subclass, got {type(v)}"
        )

    @staticmethod
    def _resolve_var_names(variables) -> list:
        """Resolve a single or list/iterable of variables to name strings.

        Each element can be a string, a BaseVariable subclass, or a MATLAB
        object.  Accepts Python lists, MATLAB cell arrays, and MATLAB string
        arrays (any iterable).
        """
        # Scalar: single string or single class
        if isinstance(variables, (str, type)):
            return [DatabaseManager._resolve_var_name(variables)]
        # Any iterable (Python list, MATLAB cell array, MATLAB string array)
        try:
            return [DatabaseManager._resolve_var_name(v) for v in variables]
        except TypeError:
            # Not iterable — treat as a single item
            return [DatabaseManager._resolve_var_name(variables)]

    def add_to_var_group(self, group_name: str, variables):
        """Add one or more variables to a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.add_to_group(group_name, self._resolve_var_names(variables))

    def remove_from_var_group(self, group_name: str, variables):
        """Remove one or more variables from a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.remove_from_group(group_name, self._resolve_var_names(variables))

    def list_var_groups(self) -> list:
        """List all variable group names.

        Returns:
            Sorted list of distinct group names.
        """
        return self._duck.list_groups()

    def get_var_group(self, group_name: str) -> list:
        """Get all variable classes in a variable group.

        Args:
            group_name: Name of the group.

        Returns:
            Sorted list of BaseVariable subclasses in the group.
        """
        names = self._duck.get_group(group_name)
        classes = []
        for name in names:
            cls = BaseVariable.get_subclass_by_name(name)
            if cls is None:
                raise NotRegisteredError(
                    f"Variable '{name}' in group '{group_name}' has no "
                    f"registered BaseVariable subclass."
                )
            classes.append(cls)
        return classes

    def close(self):
        """Close the database connection."""
        self._duck.close()
        # remove global reference
        if getattr(_local, "database", None) is self:
            self._closed = True

    def reopen(self):
        # reopen DuckDB
        if self._duck is None:
            self._duck = SciDuck(self.dataset_db_path, dataset_schema=self.dataset_schema_keys)
        self._closed = False

    def set_current_db(self):
        """Set this DatabaseManager as the active global database."""
        _local.database = self
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
