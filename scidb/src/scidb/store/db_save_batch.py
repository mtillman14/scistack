"""DatabaseManager: saving many records in one batch — validate, plan the rows, resolve schema ids, insert, and log the timings.

Moved verbatim out of database.py (grug follow-up): DatabaseManager inherits
this mixin, so every method is still `db.<name>`. Each method's `self` is
annotated as DatabaseManager, so the IDE knows its attributes.
"""

from typing import TYPE_CHECKING

import pandas as pd
import time
from datetime import datetime
from importlib import metadata
from scicanonicalhash import canonical_hash
from scicanonicalhash import frame_path_counts
from scicanonicalhash import generate_record_id
from scidb.model.log import Log
from scidb.model.variable import BaseVariable
from scidb.store.db_helpers import _describe_data
from scidb.store.db_helpers import _log_first_content_hash
from scidb.store.db_helpers import _pyarrow_available
from scidb.store.record_store import BatchRows
from scidb.store.schema_values import schema_str
from sciduckdb import bulk_df_to_storage_rows
from sciduckdb import infer_data_columns
from sciduckdb import record_schema_mismatch
from sciduckdb import value_to_storage_row
from scifor.outputs import is_empty_table
from typing import Any

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


class BatchSaveMixin:
    """DatabaseManager: saving many records in one batch — validate, plan the rows, resolve schema ids, insert, and log the timings."""

    def save_batch(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        data_items: list[tuple[Any, dict]],
        profile: bool = False,
    ) -> list[str]:
        """
        Bulk-save a list of (data_value, metadata_dict) pairs for a single variable type.

        Amortizes setup work (registration, table creation) and batches SQL
        operations using DataFrame-based inserts for speed.

        Data is deduplicated by record_id (same content → same record_id → stored once).
        Every call appends a (record_id, timestamp) save-event row to _record_save.

        Two phases: :meth:`_plan_batch_rows` (metadata canonicalization, type
        reconciliation, record ids and storage rows, as a
        :class:`BatchRows`), then ``RecordStore.write_batch_rows`` (the inserts,
        in one re-entrant transaction).

        Args:
            variable_class: The BaseVariable subclass to save as
            data_items: List of (data_value, flat_metadata_dict) tuples
            profile: If True, print phase-by-phase timing summary

        Returns:
            List of record_ids for each saved item (in input order), None in
            the slot of a record that was skipped.
        """
        if not data_items:
            return []
        rows = self._plan_batch_rows(variable_class, data_items)
        if not rows.writable:
            return rows.aligned_record_ids()
        self.record_store.write_batch_rows(rows)
        self._log_batch_timings(rows, profile)
        return rows.aligned_record_ids()

    def _plan_batch_rows(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        data_items: list[tuple[Any, dict]],
    ) -> "BatchRows":
        """Everything a batch save decides before its inserts.

        Canonicalizes declared-numeric schema keys, converts custom-serialized
        values, infers and reconciles the column types,
        skips records that do not fit the batch schema, prepares the table,
        resolves schema ids, and computes each record's content hash, record
        id and storage row. Raises a duplicate-location error before anything
        is written for two different records of one variant at one location.
        """
        n_original = len(data_items)

        # Declared-numeric schema keys canonicalize at every DB entry point —
        # this covers the MATLAB bridge (save_batch_bridge) as well as the
        # Python for_each save path with one seam.
        if self.dataset_schema_key_types:
            data_items = [(d, self.canonicalize_metadata(m)) for d, m in data_items]

        timings: dict = {}
        t0 = time.perf_counter()
        table_name = self._ensure_registered(variable_class)
        type_name = variable_class.__name__
        rows = BatchRows(
            variable_class=variable_class,
            table_name=table_name,
            n_original=n_original,
            timings=timings,
            t0=t0,
        )

        # --- Custom serialization (to_db/from_db): convert first, then batch ---
        # Every item becomes its to_db() frame BEFORE any hashing or SQL, so the
        # rest of the batch (hashing, record ids, dedup, one bulk insert, the
        # _record/_record_save rows) runs on stored frames exactly as `save`
        # stores one: the columnar ("custom": True) table. Loads then hand each
        # record's frame to from_db (_deserialize_custom_subdf).
        rows.is_custom = self._has_custom_serialization(variable_class)
        if rows.is_custom:
            _t = time.perf_counter()
            data_items = [
                (self._custom_storage_frame(variable_class(d)), m)
                for d, m in data_items
            ]
            timings["to_db"] = time.perf_counter() - _t
            Log.debug(
                f"save_batch({type_name}): custom serialization, to_db() on "
                f"{len(data_items)} item(s) in {timings['to_db']:.3f}s"
            )

        first_data = self._batch_reference_schema(rows, data_items)
        # No usable schema anywhere (e.g. every record is an empty dict): there
        # is nothing to store. Skip the whole batch rather than create an
        # empty-column table or write contentless rows.
        if not rows.data_col_types and not rows.is_dataframe:
            Log.warn(
                f"[batch_save] {type_name}: no usable data schema in any of "
                f"{len(data_items)} record(s) (all empty); nothing saved."
            )
            return rows

        col_types_str = ", ".join(f"{c}: {t}" for c, t in rows.data_col_types.items())
        Log.debug(
            f"save_batch({type_name}): {len(data_items)} items, "
            f"mode={rows.dtype_meta.get('mode', 'single_column')}, "
            f"data: {_describe_data(first_data)}, "
            f"DuckDB columns: [{col_types_str}]"
        )

        data_items = self._validate_batch_items(rows, data_items)
        if not data_items:
            return rows

        # Two different records of one variant at one location: refuse the
        # save before anything (table, schema ids, rows) is written.
        self._check_duplicate_locations(type_name, data_items)
        self._prepare_batch_table(rows, data_items, first_data)
        timings["setup"] = time.perf_counter() - t0

        all_nested = self._resolve_batch_schema_ids(rows, data_items)
        self._compute_batch_rows(rows, data_items, all_nested)
        rows.effective_level = (
            self._infer_schema_level(all_nested[0].get("schema", {}))
            or self.dataset_schema_keys[-1]
        )
        rows.writable = True
        return rows

    def _batch_reference_schema(
        self: "DatabaseManager", rows: "BatchRows", data_items: list
    ) -> Any:
        """Infer the batch's column types from its first usable item, into
        ``rows``; returns that reference item's value."""
        type_name = rows.variable_class.__name__
        first_data = data_items[0][0]
        if not rows.is_custom and is_empty_table(first_data):
            # An empty frame (pd.DataFrame(), or 0 rows) must not define the
            # table: zero columns made CREATE TABLE fail, or a stored table's
            # column set "change". The first non-empty frame does; the empty
            # ones are skipped by _validate_batch_items.
            first_data = next(
                (d for d, _ in data_items if not is_empty_table(d)), first_data
            )
            Log.debug(
                f"save_batch({type_name}): first record is an empty table; "
                f"reference schema taken from "
                + ("the first non-empty one" if not is_empty_table(first_data) else
                   "nothing (every record is empty)")
            )
        if rows.is_custom:
            rows.data_col_types = {c: str(first_data[c].dtype) for c in first_data.columns}
            rows.dtype_meta = {"custom": True}
        else:
            # The rest of the batch types columns that the first frame
            # leaves blank (a spread CSV whose first subject has no value).
            rows.data_col_types, rows.dtype_meta = infer_data_columns(
                first_data,
                more_frames=(
                    [d for d, _ in data_items[1:]]
                    if isinstance(first_data, pd.DataFrame) else ()
                ),
            )
        rows.is_dataframe = rows.dtype_meta.get("mode") == "dataframe"

        # Reference-schema selection: a leading degenerate record (e.g. an
        # empty dict -> no columns) must not define the table schema. Fall back
        # to the first record that yields a usable (non-empty) schema. The
        # degenerate record itself is caught by the validation pass.
        if not rows.data_col_types and not rows.is_dataframe and not rows.is_custom:
            for _dv, _ in data_items:
                _ct, _dm = infer_data_columns(_dv)
                if _ct:
                    first_data = _dv
                    rows.data_col_types, rows.dtype_meta = _ct, _dm
                    rows.is_dataframe = rows.dtype_meta.get("mode") == "dataframe"
                    break
        return first_data

    def _validate_batch_items(
        self: "DatabaseManager", rows: "BatchRows", data_items: list
    ) -> list:
        """The items that fit the batch's columns; the rest are skipped.

        A single record whose value doesn't fit the batch's columns (empty
        dict, missing/extra keys, or a scalar where the column stores a
        vector, etc.) would abort the atomic batch insert and lose ALL saves
        in the batch. Skip such records with a clear per-record warning and
        persist the rest. ``rows.valid_orig_idx`` maps each kept item back to
        its slot in the caller's list, so the returned record ids stay
        aligned (None in each skipped slot).

        DataFrame mode stores one DuckDB row per frame row with its own
        empty-frame handling, so it is left unvalidated here to avoid
        false-positive skips (e.g. an empty frame inferring VARCHAR columns).
        """
        type_name = rows.variable_class.__name__
        if rows.is_dataframe:
            # An EMPTY frame (no rows) has nothing to store: written, it was a
            # record with no data rows. for_each already turns an empty return
            # into "no output" (scifor.outputs.is_empty_table); this catches
            # every other caller of save_batch.
            valid_items = []
            for _idx, (_dv, _fm) in enumerate(data_items):
                if is_empty_table(_dv):
                    rows.skipped = True
                    _meta_str = (
                        ", ".join(
                            f"{k}={v}" for k, v in _fm.items() if not str(k).startswith("__")
                        )
                        or f"item #{_idx}"
                    )
                    Log.warn(
                        f"[batch_save] {type_name}: SKIPPED record ({_meta_str}) — "
                        f"the table is empty (no rows); nothing to save"
                    )
                    continue
                valid_items.append((_dv, _fm))
                rows.valid_orig_idx.append(_idx)
            return valid_items
        valid_items = []
        for _idx, (_dv, _fm) in enumerate(data_items):
            if rows.is_custom:
                # One table per variable: every to_db() frame in the batch
                # must have the first frame's columns, in order.
                _reason = (
                    None
                    if list(_dv.columns) == list(rows.data_col_types)
                    else f"to_db() columns {list(_dv.columns)} differ from "
                    f"the batch's {list(rows.data_col_types)}"
                )
            else:
                _rec_col_types, _ = infer_data_columns(_dv)
                _reason = record_schema_mismatch(rows.data_col_types, _rec_col_types)
            if _reason is None:
                valid_items.append((_dv, _fm))
                rows.valid_orig_idx.append(_idx)
                continue
            rows.skipped = True
            _meta_str = (
                ", ".join(f"{k}={v}" for k, v in _fm.items() if not str(k).startswith("__"))
                or f"item #{_idx}"
            )
            Log.warn(
                f"[batch_save] {type_name}: SKIPPED record ({_meta_str}) — "
                f"incompatible with batch schema and NOT saved: {_reason}"
            )
        if not valid_items:
            Log.warn(
                f"[batch_save] {type_name}: all {rows.n_original} record(s) "
                f"incompatible with the batch schema; nothing saved."
            )
        return valid_items

    def _prepare_batch_table(self: "DatabaseManager", rows: "BatchRows", data_items: list, first_data: Any) -> None:
        """Check the batch against the variable's stored columns (a variable
        keeps the columns of its first save), reconcile column
        types, and create the data table on a variable's first save."""
        variable_class = rows.variable_class
        table_name = rows.table_name
        if rows.is_custom:
            self._ensure_columnar_table(table_name, variable_class, first_data)
            return
        self._check_column_set(
            table_name, variable_class, list(rows.data_col_types), dtype_meta=rows.dtype_meta
        )
        values = [d for d, _ in data_items]
        rows.data_col_types, rows.dtype_meta = self._reconcile_column_types(
            table_name, variable_class, rows.data_col_types, rows.dtype_meta, values
        )
        if self._duck.table_exists(table_name):
            return
        data_cols_sql = ", ".join(f'"{col}" {dtype}' for col, dtype in rows.data_col_types.items())
        record_id_col = (
            "record_id VARCHAR NOT NULL" if rows.is_dataframe else "record_id VARCHAR PRIMARY KEY"
        )
        self._duck.execute(f'''
            CREATE TABLE "{table_name}" (
                {record_id_col},
                {data_cols_sql}
            )
        ''')
        self._create_variable_view(variable_class)

    def _resolve_batch_schema_ids(self: "DatabaseManager", rows: "BatchRows", data_items: list) -> list:
        """Split each item's metadata and resolve every distinct schema
        location to its schema id (one batch query). Returns the split
        metadata per item; the ids land in ``rows.schema_ids``."""
        t1 = time.perf_counter()
        all_nested = []
        unique_schema_combos: dict = {}  # {combo_key: schema_keys_dict}
        for _data_val, flat_meta in data_items:
            # Drop transient for_each bookkeeping that must not become a
            # version key: __branch_params (derived from the graph). The
            # record's producing invocation is a version key on purpose —
            # `__invocation_id`, stamped by the save path — so identical
            # content from different inputs, code or options is a different
            # record; the edges themselves travel typed on the GraphRecord.
            flat_meta_cleaned = {k: v for k, v in flat_meta.items() if k != "__branch_params"}
            nested = self._split_metadata(flat_meta_cleaned)
            all_nested.append(nested)
            combo_key = self._schema_combo_key(nested.get("schema", {}))
            if combo_key is not None and combo_key not in unique_schema_combos:
                unique_schema_combos[combo_key] = nested.get("schema", {})
        rows.timings["split_metadata"] = time.perf_counter() - t1

        t2 = time.perf_counter()
        self._forget_schema_key_kinds()
        rows.schema_ids = self._duck.batch_get_or_create_schema_ids(
            {
                k: {col: schema_str(v) for col, v in vals.items()}
                for k, vals in unique_schema_combos.items()
            }
        )
        rows.unique_schemas = len(unique_schema_combos)
        rows.timings["schema_resolution"] = time.perf_counter() - t2
        return all_nested

    def _schema_combo_key(self: "DatabaseManager", schema_keys: dict) -> "tuple | None":
        """``(schema level, key values)`` naming one schema location, or None
        for a record at no schema level (schema id 0)."""
        schema_level = self._infer_schema_level(schema_keys)
        if schema_level is None or not schema_keys:
            return None
        key_tuple = tuple(
            schema_str(schema_keys.get(k, ""))
            for k in self.dataset_schema_keys
            if k in schema_keys
        )
        return (schema_level, key_tuple)

    def _compute_batch_rows(self: "DatabaseManager", rows: "BatchRows", data_items: list, all_nested: list) -> None:
        """Per-record content hash, record id, storage row and ``_record``
        row (no SQL)."""
        type_name = rows.variable_class.__name__
        schema_version = rows.variable_class.schema_version
        timings = rows.timings
        t4 = time.perf_counter()
        timestamp = datetime.now().isoformat()

        # Arrow fast path for multi_column mode of 1-D arrays: indexes
        # `data_val[col]` per column, which only works when data_val is a dict.
        # Single-column mode stores data_val as a bare ndarray/scalar, so it
        # falls through to the generic value_to_storage_row path.
        if not rows.is_dataframe and rows.dtype_meta.get("mode") == "multi_column":
            col_metas = rows.dtype_meta.get("columns", {})
            if _pyarrow_available() and all(
                m.get("python_type") == "ndarray" and m.get("ndim", 1) == 1
                for m in col_metas.values()
            ):
                rows.arrow_columns = {col: [] for col in rows.data_col_types}

        t4_hash = 0.0
        # Which DataFrame serialization path the hashes took (F30): "fast"
        # skips pandas' per-column Series; "reference" is the public-API path
        # (unaddressable frames, or a signature whose fast path disagreed).
        frames_before = frame_path_counts()
        t4_record_id = 0.0
        t4_storage = 0.0
        t4_meta = 0.0
        # DataFrames are converted to storage rows in bulk after the loop.
        # Per-row dataframe_to_storage_rows (54 iloc calls × 7k records) was
        # the dominant cost; bulk_df_to_storage_rows processes column-by-column.
        df_bulk: list = []
        df_bulk_rids: list = []

        for i, (data_val, _flat_meta) in enumerate(data_items):
            combo_key = self._schema_combo_key(all_nested[i].get("schema", {}))
            schema_id = rows.schema_ids[combo_key] if combo_key is not None else 0

            _t = time.perf_counter()
            content_hash = canonical_hash(data_val)
            t4_hash += time.perf_counter() - _t
            if i == 0:
                _log_first_content_hash(type_name, data_val, content_hash)

            _t = time.perf_counter()
            record_id = generate_record_id(
                class_name=type_name,
                schema_version=schema_version,
                content_hash=content_hash,
                metadata=all_nested[i],
            )
            rows.record_ids.append(record_id)
            t4_record_id += time.perf_counter() - _t

            _t = time.perf_counter()
            if rows.arrow_columns is not None:
                for col in rows.data_col_types:
                    rows.arrow_columns[col].append(data_val[col])
            elif rows.is_custom:
                rows.custom_frames.append((record_id, data_val))
            elif rows.is_dataframe:
                df_bulk.append(data_val)
                df_bulk_rids.append(record_id)
            else:
                storage_values = value_to_storage_row(data_val, rows.dtype_meta)
                rows.data_rows.append((record_id,) + tuple(storage_values))
            t4_storage += time.perf_counter() - _t

            _t = time.perf_counter()
            rows.metadata_rows.append(
                (record_id, timestamp, type_name, schema_id, content_hash, schema_version)
            )
            t4_meta += time.perf_counter() - _t

        if df_bulk:
            _t = time.perf_counter()
            rows.data_rows = bulk_df_to_storage_rows(df_bulk, df_bulk_rids, rows.dtype_meta)
            t4_storage += time.perf_counter() - _t

        timings["per_row_hashing"] = time.perf_counter() - t4
        timings["canonical_hash"] = t4_hash
        frames_after = frame_path_counts()
        rows.hash_paths = (
            f"hash frames fast={frames_after['fast'] - frames_before['fast']} "
            f"reference={frames_after['reference'] - frames_before['reference']}"
        )
        timings["record_id"] = t4_record_id
        timings["storage_row"] = t4_storage
        timings["meta_row"] = t4_meta

    def _log_batch_timings(self: "DatabaseManager", rows: "BatchRows", profile: bool) -> None:
        """The phase breakdown of one batch save.

        At INFO, not DEBUG. This path is measured in tens of seconds on
        payload-heavy batches (77.3s for 419 RawEMG records, 2026-09-13) and a
        single opaque total gives no way to tell hashing from inserts from the
        commit. ~18 phases is far too many for one readable line, so the INFO
        line carries the costliest few and every phase still gets its DEBUG
        line.
        """
        type_name = rows.variable_class.__name__
        n = len(rows.record_ids)
        if rows.is_custom:
            n_total_rows = sum(len(_df) for _, _df in rows.custom_frames)
        elif rows.arrow_columns is not None:
            n_total_rows = len(rows.record_ids)
        else:
            n_total_rows = len(rows.data_rows)
        Log.timings(
            f"save_batch({type_name})",
            rows.timings,
            extra=(
                f"{n} items ({rows.n_new} new rows, {n_total_rows} total storage rows), "
                f"{rows.unique_schemas} schemas, {rows.hash_paths}, "
                f"serialization={'custom' if rows.is_custom else 'native'}"
            ),
            top=6,
        )
        if profile:
            print(f"\n--- save_batch() profile ({n} items, {rows.unique_schemas} unique schemas) ---")
            for phase, elapsed in rows.timings.items():
                print(f"  {phase:30s} {elapsed:8.3f}s")
            print()
