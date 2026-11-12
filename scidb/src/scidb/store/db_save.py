"""DatabaseManager: saving one record — registration, duplicate-location and column-set checks, columnar and native storage, stored column types.

Moved verbatim out of database.py (grug follow-up): DatabaseManager inherits
this mixin, so every method is still `db.<name>`. Each method's `self` is
annotated as DatabaseManager, so the IDE knows its attributes.
"""

from typing import TYPE_CHECKING

import json
import numpy as np
import pandas as pd
import time
import warnings
from datetime import datetime
from importlib import metadata
from scicanonicalhash import canonical_hash
from scicanonicalhash import generate_record_id
from scidb.model.exceptions import NotRegisteredError
from scidb.model.log import Log
from scidb.model.variable import BaseVariable
from scidb.store.db_helpers import _colliding_locations
from scidb.store.db_helpers import _sample_values
from scidb.store.schema_values import schema_str
from sciduckdb import column_type_change
from sciduckdb import dataframe_to_storage_rows
from sciduckdb import infer_data_columns
from sciduckdb import storage_column_types
from sciduckdb import stored_tz_companions
from sciduckdb import tz_companions
from sciduckdb import user_column_types
from sciduckdb import value_to_storage_row
from typing import Any

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.exceptions import ColumnSetChangedError
from scidb.model.exceptions import ColumnTypeChangedError
from scidb.model.exceptions import DuplicateLocationError
from scidb.provenance.graph import insert_record_entity
from scidb.provenance.save import record_direct_save
class SaveMixin:
    """DatabaseManager: saving one record — registration, duplicate-location and column-set checks, columnar and native storage, stored column types."""

    def _check_duplicate_locations(
        self: "DatabaseManager", type_name: str, data_items: "list[tuple[Any, dict]]"
    ) -> None:
        """Raise DuplicateLocationError when the batch puts two different
        records of one variant at one location (rule: _colliding_locations)."""
        schema_keys = list(self.dataset_schema_keys)
        collisions, stats = _colliding_locations(data_items, schema_keys)
        Log.debug(
            f"save_batch({type_name}): {stats['items']} item(s) -> "
            f"{stats['locations']} location(s), {stats['variant_groups']} "
            f"variant group(s), {stats['no_location']} without schema keys, "
            f"{len(collisions)} duplicate location(s)"
        )
        if not collisions:
            return

        Log.warn(
            f"[batch_save] {type_name}: {len(collisions)} location(s) with more "
            f"than one different record in one save; refusing the save: "
            + "; ".join(
                f"{c['location']} x{c['count']} differ in {c['differing_columns']}"
                for c in collisions
            )
        )
        raise DuplicateLocationError(type_name, collisions, schema_keys)

    def _check_column_set(
        self: "DatabaseManager",
        table_name: str,
        variable_class: type[BaseVariable],
        columns,
        *,
        dtype_meta: dict | None = None,
    ) -> None:
        """The one owner of "may this save write these data columns into the
        variable's existing table". Call it before the create-if-missing
        block of every save path.

        Same columns: nothing to do. Different columns:
        - the table holds no rows (every record was deleted): drop it and its
          view, so the caller's create-if-missing builds it with the new
          columns. Nothing is lost, and it is what makes "delete the old
          records and run again" work;
        - the table holds records: raise ColumnSetChangedError, naming the
          columns and the two ways out. Before 2026-10-01 a NEW column failed
          as an opaque DuckDB Binder Error, and a MISSING one silently
          overwrote the variable's column list, so old records loaded
          without their columns.

        A datetime column's ``__tz`` zone companion (``dtype_meta``'s
        datetime columns, sciduckdb.datetimes) is not a column of its own
        here: when this save turns a still-empty column into a datetime one,
        ``_reconcile_column_types`` adds the companion.
        """
        if not self._duck.table_exists(table_name):
            return
        stored_types = self.stored_data_column_types(table_name)
        existing = list(stored_types)
        new = [str(c) for c in columns]
        if set(existing) == set(new):
            return
        # This save's companions and the stored ones: a datetime column
        # meeting non-datetime values is a TYPE question, answered (and
        # refused) by _reconcile_column_types, not a column-set change.
        companions = set(tz_companions(dtype_meta)) | set(
            stored_tz_companions(stored_types)
        )
        if companions and (set(existing) - companions) == (set(new) - companions):
            Log.debug(
                f"[save] {variable_class.__name__}: column set matches; only "
                f"datetime zone column(s) differ "
                f"({sorted((set(new) ^ set(existing)) & companions)}), "
                f"reconciled with the column types"
            )
            return
        added = [c for c in new if c not in set(existing)]
        removed = [c for c in existing if c not in set(new)]
        n_existing = self._duck.fetchall(
            f'SELECT COUNT(DISTINCT record_id) FROM "{table_name}"'
        )[0][0]
        type_name = variable_class.__name__
        if n_existing == 0:
            Log.info(
                f"[save] {type_name}: its table is empty, so it is rebuilt with "
                f"this save's columns ({len(added)} new, {len(removed)} dropped)"
            )
            self._duck.execute(
                f'DROP VIEW IF EXISTS "{variable_class.view_name()}"'
            )
            self._duck.execute(f'DROP TABLE "{table_name}"')
            return

        err = ColumnSetChangedError(type_name, added, removed, n_existing)
        Log.error(f"[save] {err}")
        raise err

    def _create_variable_view(self: "DatabaseManager", variable_class: type[BaseVariable]):
        """Create a view joining a variable table with _schema via _record.

        ``schema_id`` and the mutable ``excluded`` flag now come straight from the
        content-addressed ``_record`` entity (one row per record_id), so no
        latest-by-timestamp CTE over the save-event log is needed.
        """
        table_name = variable_class.table_name()
        view_name = variable_class.view_name()
        schema_cols = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)
        self._duck.execute(f"""
            CREATE OR REPLACE VIEW "{view_name}" AS
            SELECT
                t.*,
                s.schema_level, {schema_cols},
                r.excluded
            FROM "{table_name}" t
            LEFT JOIN _record r ON t.record_id = r.record_id
            LEFT JOIN _schema s ON r.schema_id = s.schema_id
        """)

    def _save_record_event(
        self: "DatabaseManager",
        record_id: str,
        timestamp: str,
    ) -> None:
        """Append a save-event row to ``_record_save``. Always inserts (audit trail).

        The record's type/schema/content metadata is carried by the ``_record``
        entity row (written alongside by the caller), not here.
        """
        Log.debug(
            f"_save_record_event: record_id={record_id[:12]}, timestamp={timestamp}"
        )
        self._duck.execute(
            """
            INSERT INTO _record_save (record_id, timestamp)
            VALUES (?, ?)
            ON CONFLICT (record_id, timestamp) DO NOTHING
            """,
            [record_id, timestamp],
        )

    def _save_columnar(
        self: "DatabaseManager",
        record_id: str,
        table_name: str,
        variable_class: type[BaseVariable],
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
        # New schema rows can change a key's numeric/string answer.
        self._forget_schema_key_kinds()
        schema_id = (
            self._duck.get_or_create_schema_id(schema_level, schema_keys)
            if schema_level is not None and schema_keys
            else 0
        )

        self._ensure_columnar_table(table_name, variable_class, df)
        self.record_store.insert_columnar_frames(table_name, [(record_id, df)])

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        if dict_of_arrays:
            dtype_json = json.dumps(
                {
                    "custom": True,
                    "dict_of_arrays": True,
                    "ndarray_keys": ndarray_keys or {},
                }
            )
        elif struct_columns:
            dtype_json = json.dumps(
                {
                    "custom": True,
                    "struct_columns": struct_columns,
                }
            )
        else:
            dtype_json = json.dumps({"custom": True})
        self._duck.execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, dtype_json, ""],
        )

        return schema_id

    def stored_data_columns(self: "DatabaseManager", table_name: str) -> list[str]:
        """The data columns of a variable's ``<Type>_data`` table, in stored
        order (``record_id`` excluded); ``[]`` when the table does not exist.

        The one owner of reading them: ``_check_column_set`` compares a save
        against them, and the MATLAB bridge uses them to tell a custom
        (``to_db``) record's stored columns from the metadata columns of a
        loaded input (``scimatlab.bridge.codec_storage_columns``).
        """
        return list(self.stored_data_column_types(table_name))

    def stored_data_column_types(self: "DatabaseManager", table_name: str) -> dict[str, str]:
        """``{column: DuckDB type}`` of a variable's ``<Type>_data`` table, in
        stored order (``record_id`` excluded); ``{}`` when it does not exist.
        The one reader of the stored columns (``stored_data_columns`` is its
        key view)."""
        if not self._duck.table_exists(table_name):
            return {}
        return {
            row[0]: row[1]
            for row in self._duck.fetchall(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ? AND column_name <> 'record_id' "
                "ORDER BY ordinal_position",
                [table_name],
            )
        }

    def _stored_empty_columns(
        self: "DatabaseManager", table_name: str, col_types: dict[str, str]
    ) -> set[str]:
        """Which of ``col_types`` hold no value in any stored row (all NULL,
        or NaN in a float column) — one aggregate query. The stored DATA is
        the owner of "is this column's type still undecided"; no flag is
        persisted that could disagree with it."""
        if not col_types:
            return set()
        exprs = []
        for col, ddb_type in col_types.items():
            cond = f'"{col}" IS NOT NULL'
            if ddb_type.strip().upper() in ("DOUBLE", "FLOAT", "REAL"):
                cond += f' AND NOT isnan("{col}")'
            exprs.append(f"COUNT(*) FILTER (WHERE {cond})")
        counts = self._duck.fetchall(
            f'SELECT {", ".join(exprs)} FROM "{table_name}"'
        )[0]
        return {col for col, n in zip(col_types, counts) if n == 0}

    def _reconcile_column_types(
        self: "DatabaseManager",
        table_name: str,
        variable_class: type[BaseVariable],
        data_col_types: dict,
        dtype_meta: dict,
        frames: list,
    ) -> tuple[dict, dict]:
        """The one owner of "may this save's column TYPES go into the
        variable's existing DataFrame table". Call it right after
        ``_check_column_set`` on every DataFrame save path; store and upsert
        with what it returns.

        Per column (rule for a column with values: sciduckdb
        ``column_type_change``):
        - this save is empty in it -> the stored type and metadata stand;
        - the stored column is still empty -> converted in place to this
          save's type (a blank first CSV load, values in a later one);
        - integer column, fractional values -> widened to DOUBLE;
        - text column, numbers -> stored as text;
        - otherwise -> ColumnTypeChangedError, before anything is changed.

        Returns the MERGED ``(data_col_types, dtype_meta)``: the stored type
        and metadata wherever they stand, so the ``_variables.dtype`` upsert
        never overwrites a decided column with this save's guess. History:
        scistack.log 2026-10-22, IP_SPINAL_STIMULATION blank then 'Y'.
        """
        if dtype_meta.get("mode") != "dataframe" or dtype_meta.get("custom"):
            return data_col_types, dtype_meta
        stored_types = self.stored_data_column_types(table_name)
        if not stored_types:
            return data_col_types, dtype_meta
        t0 = time.perf_counter()
        type_name = variable_class.__name__
        stored_metas = (self.get_dtype_meta(type_name) or {}).get("columns", {})
        in_metas = dtype_meta.get("columns", {})
        # Datetime zone companions follow their column (sciduckdb.datetimes):
        # judged through it, and re-derived from the merged metadata below.
        user_types = user_column_types(data_col_types, dtype_meta)

        changes = {}
        for col, in_type in user_types.items():
            s_type = stored_types.get(col)
            if s_type is None or in_metas.get(col, {}).get("empty"):
                continue
            change = column_type_change(s_type, in_type)
            if change != "same" or stored_metas.get(col, {}).get("empty", col not in stored_metas):
                changes[col] = change
        empty = self._stored_empty_columns(
            table_name, {c: stored_types[c] for c in changes}
        )

        new_types = dict(user_types)
        new_metas = dict(in_metas)
        alters: list[tuple[str, str, bool]] = []  # (column, new type, stored empty)
        conflicts = []
        for col, in_type in user_types.items():
            s_type = stored_types.get(col)
            if s_type is None:
                continue
            s_meta = stored_metas.get(col)
            change = changes.get(col, "same")
            if col in empty:
                if s_type.upper() != in_type.upper():
                    alters.append((col, in_type, True))
                continue  # this save's type and metadata decide the column
            if in_metas.get(col, {}).get("empty") or change in ("same", "as_text"):
                new_types[col] = s_type
                if s_meta is not None and not s_meta.get("empty"):
                    new_metas[col] = s_meta
                if change == "as_text":
                    Log.info(
                        f"[save] {type_name}: column {col} is stored as text; "
                        f"this save's {in_type} values are stored as text too"
                    )
                continue
            if change == "widen":
                alters.append((col, in_type, False))
                continue
            conflicts.append(
                (col, s_type, in_type, _sample_values(frames, col))
            )

        if conflicts:
            n_existing = self._duck.fetchall(
                f'SELECT COUNT(DISTINCT record_id) FROM "{table_name}"'
            )[0][0]
            err = ColumnTypeChangedError(type_name, conflicts, n_existing)
            Log.error(f"[save] {err}")
            raise err

        if alters:
            self._duck.execute(
                f'DROP VIEW IF EXISTS "{variable_class.view_name()}"'
            )
            for col, new_type, was_empty in alters:
                using = f" USING CAST(NULL AS {new_type})" if was_empty else ""
                self._duck.execute(
                    f'ALTER TABLE "{table_name}" ALTER COLUMN "{col}" '
                    f"SET DATA TYPE {new_type}{using}"
                )
                Log.info(
                    f"[save] {type_name}: column {col} "
                    + (
                        f"was empty in every stored record; its type is now "
                        f"{new_type}, decided by this save's values"
                        if was_empty
                        else f"widened {stored_types[col]} -> {new_type} for "
                        f"this save's fractional values"
                    )
                )
            self._create_variable_view(variable_class)

        merged_meta = dict(dtype_meta)
        merged_meta["columns"] = new_metas
        # A column that just became a datetime one (it was empty in every
        # stored record) gets its zone companion now; every other datetime
        # column has had one since its first save.
        missing_tz = [c for c in tz_companions(merged_meta) if c not in stored_types]
        if missing_tz:
            self._duck.execute(
                f'DROP VIEW IF EXISTS "{variable_class.view_name()}"'
            )
            for comp in missing_tz:
                self._duck.execute(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{comp}" VARCHAR'
                )
            self._create_variable_view(variable_class)
            Log.info(
                f"[save] {type_name}: added datetime zone column(s) {missing_tz} "
                f"(NULL = naive) for column(s) that are now datetimes"
            )

        Log.debug(
            f"[save] {type_name}: column types reconciled in "
            f"{time.perf_counter() - t0:.3f}s — {len(user_types)} column(s), "
            f"{len(changes)} checked against stored data, "
            f"{len(empty)} still empty, {len(alters)} altered, "
            f"{len(missing_tz)} zone column(s) added"
        )
        return storage_column_types(new_types, merged_meta), merged_meta

    def _ensure_columnar_table(
        self: "DatabaseManager",
        table_name: str,
        variable_class: type[BaseVariable],
        df: pd.DataFrame,
    ) -> None:
        """Create the columnar (``"custom": True``) data table from *df*'s
        columns if it does not exist yet; check the column set if it does.

        The one owner of the columnar table's shape, shared by ``save`` and
        ``save_batch`` so both paths store a custom-serialized variable alike.
        """
        self._check_column_set(table_name, variable_class, list(df.columns))
        if self._duck.table_exists(table_name):
            return
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
                    (
                        v
                        for v in df[col]
                        if v is not None
                        and not (isinstance(v, float) and np.isnan(v))
                    ),
                    None,
                )
                if isinstance(first_val, np.ndarray) and np.issubdtype(
                    first_val.dtype, np.number
                ):
                    ddb_type = "DOUBLE[]"
                elif (
                    isinstance(first_val, list)
                    and len(first_val) > 0
                    and all(isinstance(x, (int, float)) for x in first_val)
                ):
                    ddb_type = "DOUBLE[]"
                else:
                    ddb_type = "VARCHAR"
            else:
                ddb_type = "VARCHAR"
            col_defs.append(f'"{col}" {ddb_type}')

        data_cols_sql = ", ".join(col_defs)
        self._duck.execute(f"""
            CREATE TABLE "{table_name}" (
                record_id VARCHAR NOT NULL,
                {data_cols_sql}
            )
        """)
        self._create_variable_view(variable_class)
        Log.debug(f"_ensure_columnar_table: created table '{table_name}'")

    def _custom_storage_frame(self: "DatabaseManager", variable: BaseVariable) -> pd.DataFrame:
        """The frame a custom-serialized *variable* is stored (and hashed) as:
        its ``to_db()`` result. THE owner of calling ``to_db`` on save, for
        both ``save`` and ``save_batch``; raises a ``TypeError`` naming the
        class when ``to_db`` does not return a DataFrame."""
        df = variable.to_db()
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"{type(variable).__name__}.to_db() must return a pandas "
                f"DataFrame, got {type(df).__name__}"
            )
        return df

    def _save_native(
        self: "DatabaseManager",
        record_id: str,
        table_name: str,
        variable_class: type[BaseVariable],
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
            self._forget_schema_key_kinds()
            schema_id = self._duck.get_or_create_schema_id(
                schema_level, {k: schema_str(v) for k, v in schema_keys.items()}
            )
        else:
            schema_id = 0

        data_col_types, dtype_meta = infer_data_columns(data)
        is_dataframe = isinstance(data, pd.DataFrame)

        # Ensure table exists, with this save's columns.
        self._check_column_set(
            table_name, variable_class, list(data_col_types), dtype_meta=dtype_meta
        )
        data_col_types, dtype_meta = self._reconcile_column_types(
            table_name, variable_class, data_col_types, dtype_meta, [data]
        )
        if not self._duck.table_exists(table_name):
            data_cols_sql = ", ".join(
                f'"{col}" {dtype}' for col, dtype in data_col_types.items()
            )
            if is_dataframe:
                # One DuckDB row per table row: record_id is not unique per row.
                record_id_col = "record_id VARCHAR NOT NULL"
            else:
                record_id_col = "record_id VARCHAR PRIMARY KEY"
            self._duck.execute(f'''
                CREATE TABLE "{table_name}" (
                    {record_id_col},
                    {data_cols_sql}
                )
            ''')
            self._create_variable_view(variable_class)
            Log.debug(f"_save_native: created table '{table_name}'")

        if is_dataframe:
            # Idempotency: skip all inserts if this record_id already exists.
            existing_count = self._duck.fetchall(
                f'SELECT COUNT(*) FROM "{table_name}" WHERE record_id = ?',
                [record_id],
            )[0][0]
            if existing_count == 0:
                col_names = ["record_id"] + list(data_col_types.keys())
                col_str = ", ".join(f'"{c}"' for c in col_names)
                placeholders = ", ".join(["?"] * len(col_names))
                for storage_row in dataframe_to_storage_rows(data, dtype_meta):
                    self._duck.execute(
                        f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders})',
                        [record_id] + storage_row,
                    )
                Log.debug(
                    f"_save_native: inserted {len(data)} rows (dataframe) into '{table_name}', record_id={record_id[:12]}"
                )
            else:
                Log.debug(
                    f"_save_native: record_id={record_id[:12]} already exists in '{table_name}', skipped"
                )
        else:
            storage_values = value_to_storage_row(data, dtype_meta)
            col_names = ["record_id"] + list(data_col_types.keys())
            col_str = ", ".join(f'"{c}"' for c in col_names)
            placeholders = ", ".join(["?"] * len(col_names))
            self._duck.execute(
                f'INSERT INTO "{table_name}" ({col_str}) VALUES ({placeholders}) '
                f"ON CONFLICT (record_id) DO NOTHING",
                [record_id] + storage_values,
            )
            Log.debug(
                f"_save_native: inserted single record into '{table_name}', record_id={record_id[:12]}"
            )

        # Upsert into _variables (one row per variable)
        effective_level = schema_level or self.dataset_schema_keys[-1]
        self._duck.execute(
            "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
            [variable_class.__name__, effective_level, json.dumps(dtype_meta), ""],
        )

        return schema_id

    @staticmethod
    def _has_custom_serialization(variable_class: type) -> bool:
        """Check if a BaseVariable subclass overrides to_db or from_db."""
        return (
            "to_db" in variable_class.__dict__ or "from_db" in variable_class.__dict__
        )

    def _variable_names_for_validation(self: "DatabaseManager", what: str) -> "list[str] | None":
        """The variable names, or None when they cannot be read right now.

        For the display-only validation of `[aliases]` and `[colors]`, which
        Plot Studio reads lazily, sometimes after the GUI has released this
        connection. A closed connection is asked first (`SciDuck.is_open`)
        rather than queried: querying it made sciduckdb log an ERROR with a
        traceback for a case this code expects and handles (scistack.log
        2026-10-01 22:29, "Connection already closed!").

        The check and the query are one atomic step in sciduckdb
        (`list_variables_if_open`): checking `is_open` here and querying
        after left a window that a concurrent GUI request's close hit
        (scistack.log 2026-10-19 15:08:27).
        """
        duck = getattr(self, "_duck", None)
        if duck is None:
            Log.debug(f"[{what}] no connection; validation deferred to a later access")
            return None
        try:
            frame = duck.list_variables_if_open()
        except Exception as exc:  # a half-open database
            Log.debug(f"[{what}] variable names unavailable for validation: {exc}")
            return None
        if frame is None:
            Log.debug(f"[{what}] connection closed; validation deferred to a later access")
            return None
        return [str(name) for name in frame["variable_name"]]

    def register(self: "DatabaseManager", variable_class: type[BaseVariable]) -> None:
        """
        Register a variable type for storage.

        Args:
            variable_class: The BaseVariable subclass to register
        """
        type_name = variable_class.__name__
        table_name = variable_class.table_name()
        schema_version = variable_class.schema_version

        # Register in metadata table (skip if already registered)
        existing = self._duck.fetchall(
            "SELECT 1 FROM _registered_types WHERE type_name = ?",
            [type_name],
        )
        if not existing:
            self._duck.execute(
                """
                INSERT INTO _registered_types (type_name, table_name, schema_version)
                VALUES (?, ?, ?)
                """,
                [type_name, table_name, schema_version],
            )

        # Cache locally
        self._registered_types[type_name] = variable_class

    def ensure_variable_table(self: "DatabaseManager", variable_class: type[BaseVariable]) -> bool:
        """Give a variable with no records an EMPTY data table and its view,
        so SQL on the view returns no rows instead of "does not exist".
        Returns whether a table was created (False: it already existed).

        The table holds ``record_id`` only: a variable's data columns are
        not known until a value exists. ``_check_column_set`` drops and
        rebuilds an empty table with the first real save's columns, so the
        placeholder never conflicts. scidb's loads answer from the record
        tables and behave the same with or without it (Stage 0 of
        .claude/plan-for-columns-groups.md, 2026-10-28).
        """
        table_name = self._ensure_registered(variable_class)
        if self._duck.table_exists(table_name):
            return False
        self._duck.execute(
            f'CREATE TABLE "{table_name}" (record_id VARCHAR NOT NULL)'
        )
        self._create_variable_view(variable_class)
        Log.debug(f"ensure_variable_table: created empty table '{table_name}'")
        return True

    def _ensure_registered(
        self: "DatabaseManager", variable_class: type[BaseVariable], auto_register: bool = True
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
        rows = self._duck.fetchall(
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
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        data: Any,
        index: Any = None,
        **metadata,
    ) -> str:
        """
        Save data as a variable.

        Accepts a BaseVariable instance or raw data.

        Args:
            variable_class: The BaseVariable subclass to save as
            data: The data to save (BaseVariable or raw data)
            index: Optional index to set on the DataFrame
            **metadata: Addressing metadata (e.g., subject=1, trial=1)

        Returns:
            The record_id of the saved data
        """
        metadata = self.canonicalize_metadata(metadata)
        type_name = variable_class.__name__
        user_keys = {k: v for k, v in metadata.items() if not k.startswith("__")}
        Log.debug(f"save_variable({type_name}): metadata={user_keys}")

        raw_data = data.data if isinstance(data, BaseVariable) else data

        instance = variable_class(raw_data)
        record_id = self.save(instance, metadata, index=index)

        instance.record_id = record_id
        instance.metadata = metadata

        Log.info(f"save_variable({type_name}): saved -> record_id={record_id[:12]}")
        return record_id

    def save(
        self: "DatabaseManager",
        variable: BaseVariable,
        metadata: dict,
        index: Any = None,
    ) -> str:
        """
        Save a variable to the database.

        Args:
            variable: The variable instance to save
            metadata: Addressing metadata (flat dict)
            index: Optional index to set on the DataFrame

        Returns:
            The record_id of the saved data
        """
        table_name = self._ensure_registered(type(variable))
        type_name = variable.__class__.__name__

        # Drop transient for_each bookkeeping that must not become version keys:
        # __branch_params (accumulated constants, now derived from the graph).
        if isinstance(metadata, dict):
            metadata = {k: v for k, v in metadata.items() if k != "__branch_params"}

        # Split metadata
        nested_metadata = self._split_metadata(metadata)

        # Warn if no metadata keys match the schema (skip internal __* keys)
        user_metadata_keys = [k for k in metadata if not k.startswith("__")]
        if user_metadata_keys and not nested_metadata.get("schema"):
            warnings.warn(
                f"None of the metadata keys {user_metadata_keys} match the "
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

        # Compute content hash. A custom-serialized variable is hashed by its
        # STORED form (the to_db() frame), as in save_batch: its value may be an
        # object canonical_hash cannot serialize, and one stored table must mean
        # one record whichever save path wrote it.
        is_custom = self._has_custom_serialization(type(variable))
        custom_df = self._custom_storage_frame(variable) if is_custom else None
        content_hash = canonical_hash(custom_df if is_custom else variable.data)

        # Generate record_id
        record_id = generate_record_id(
            class_name=type_name,
            schema_version=variable.schema_version,
            content_hash=content_hash,
            metadata=nested_metadata,
        )

        serialization = "custom" if is_custom else "native"
        Log.debug(
            f"save({type_name}): record_id={record_id[:12]}, content_hash={content_hash[:12]}, serialization={serialization}"
        )

        # Wrap all writes in a single transaction to avoid repeated
        # WAL checkpoints (each auto-committed statement can trigger a
        # checkpoint/fsync, causing random multi-second stalls).
        try:
            with self._duck.transaction("save"):
                schema_keys = nested_metadata.get("schema", {})
                version_keys = nested_metadata.get("version", {})
                schema_level = self._infer_schema_level(schema_keys)
                created_at = datetime.now().isoformat()

                if is_custom:
                    # Custom serialization: the to_db() frame computed for the hash
                    df = custom_df

                    if index is not None:
                        index_list = list(index) if not isinstance(index, list) else index
                        if len(index_list) != len(df):
                            raise ValueError(
                                f"Index length ({len(index_list)}) does not match "
                                f"DataFrame row count ({len(df)})"
                            )
                        df.index = index

                    schema_id = self._save_columnar(
                        record_id,
                        table_name,
                        type(variable),
                        df,
                        schema_level,
                        schema_keys,
                        content_hash,
                    )
                else:
                    # ALL other data: scalars, arrays, lists, dicts, dict-of-arrays,
                    # and native DataFrames (stored as a single record with array-typed
                    # columns, e.g. DOUBLE[], BIGINT[], VARCHAR[]).
                    schema_id = self._save_native(
                        record_id,
                        table_name,
                        type(variable),
                        variable.data,
                        content_hash,
                        schema_level=schema_level,
                        schema_keys=schema_keys,
                    )

                self._save_record_event(
                    record_id=record_id,
                    timestamp=created_at,
                )

                # Mirror into the bipartite entities table (_record). Covers raw /
                # manually saved records, which have no producing invocation.

                insert_record_entity(
                    self._duck,
                    record_id=record_id,
                    created_at=created_at,
                    type_name=type_name,
                    schema_id=schema_id,
                    content_hash=content_hash,
                    schema_version=variable.schema_version,
                )

                # A save carrying non-schema kwargs: anchor those kwargs in the graph
                # as a synthetic save invocation so they are graph-derivable branch
                # params.
                if version_keys:
                    save_kwargs = {
                        k: v for k, v in version_keys.items() if not str(k).startswith("__")
                    }
                    if save_kwargs:
                        record_direct_save(self._duck, record_id, save_kwargs, created_at)
        except Exception as e:
            Log.error(f"save({type_name}): transaction rolled back: {e}")
            raise
        Log.debug(f"save({type_name}): transaction committed")

        return record_id
