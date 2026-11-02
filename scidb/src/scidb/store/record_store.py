"""``RecordStore``: the SQL that reads and writes a variable's stored records.

One object over one ``SciDuck`` connection, holding the queries against the
storage tables — ``_variables``, ``_registered_types``, the ``<Type>_data``
tables (and their ``information_schema`` entry), ``_record`` / ``_record_save``
and ``_schema`` — that used to be spelled in two places: ``DatabaseManager``
and ``scistackplotdb.load`` (design review 06 PD1). A layer that wants a
variable's records asks this; it does not write its own SELECT.

* **catalog** — :meth:`schema_keys`, :meth:`variable_names`,
  :meth:`table_name`, :meth:`stored_column_types`, :meth:`dtype_meta`;
* **read** — :meth:`long_frame` (every non-excluded record of a variable as one
  frame, the bulk read plotting needs), :meth:`sample_cell`,
  :meth:`column_levels`, :meth:`occupied_keys`;
* **write** — :meth:`write_batch_rows` (the inserts of a planned batch save,
  ``DatabaseManager._plan_batch_rows`` -> :class:`BatchRows`) and
  :meth:`insert_columnar_frames`.

Payload columns are only ever fetched through ``fetchdf`` (one numpy buffer
per cell), never ``fetchall`` (a Python float per sample): measured 18x on
2026-09-13, ``.claude/plot-at-scale-plan.md`` §7.
"""

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from scidb.model.log import Log
from scidb.store.sciduck import SciDuck


@dataclass
class BatchRows:
    """One batch save, planned: everything ``save_batch`` computes before its
    inserts (``DatabaseManager._plan_batch_rows``), and what the inserts
    (:meth:`RecordStore.write_batch_rows`) and the timing line read.

    ``record_ids`` align with the items that passed validation;
    :meth:`aligned_record_ids` maps them back to the caller's order with None
    in each skipped slot. ``writable`` is False when nothing in the batch can
    be stored (no usable schema, or every record skipped).
    """

    variable_class: type
    table_name: str
    n_original: int
    timings: dict
    t0: float
    is_custom: bool = False
    is_dataframe: bool = False
    data_col_types: dict = field(default_factory=dict)
    dtype_meta: dict = field(default_factory=dict)
    #: on_duplicate="combine" row-list columns under their STORED names.
    stored_row_lists: list = field(default_factory=list)
    #: The caller's index of each item that passed validation.
    valid_orig_idx: "list[int]" = field(default_factory=list)
    skipped: bool = False
    #: (schema level, key values) -> schema id
    schema_ids: dict = field(default_factory=dict)
    unique_schemas: int = 0
    record_ids: "list[str]" = field(default_factory=list)
    #: (record_id, *storage values) per data-table row.
    data_rows: "list[tuple]" = field(default_factory=list)
    #: (record_id, timestamp, type, schema_id, content_hash, schema_version)
    metadata_rows: "list[tuple]" = field(default_factory=list)
    #: Arrow fast path (multi-column 1-D arrays): column -> per-record arrays.
    arrow_columns: "dict[str, list] | None" = None
    #: Custom serialization: (record_id, to_db frame) per record.
    custom_frames: list = field(default_factory=list)
    effective_level: "str | None" = None
    hash_paths: str = ""
    n_new: int = 0
    writable: bool = False

    def aligned_record_ids(self) -> "list[str | None]":
        """Record ids in the caller's item order, None where skipped."""
        if not self.writable:
            return [None] * self.n_original
        if not self.skipped:
            return list(self.record_ids)
        aligned: "list[str | None]" = [None] * self.n_original
        for pos, orig_idx in enumerate(self.valid_orig_idx):
            aligned[orig_idx] = self.record_ids[pos]
        return aligned


#: The non-excluded records of ONE variable type (``r.type = ?``), joined to
#: their data table ``t`` and schema location ``s``. Every per-variable read
#: filters on ``_record.type``: ``_registered_types.table_name`` is not
#: unique, so a shared table read without it would mix two variables.
_RECORDS_OF_TYPE = (
    "JOIN _record r ON t.record_id = r.record_id "
    "LEFT JOIN _schema s ON r.schema_id = s.schema_id "
    "WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE"
)


class RecordStore:
    """The storage-table SQL over one connection (module docstring)."""

    def __init__(self, duck: SciDuck):
        self._duck = duck

    @property
    def connection(self) -> SciDuck:
        return self._duck

    # -- catalog ---------------------------------------------------------------

    def schema_keys(self) -> "list[str]":
        """The dataset's schema keys, in order."""
        return list(self._duck.dataset_schema)

    def variable_names(self) -> "list[str]":
        """Every variable with a ``_variables`` row, sorted."""
        rows = self._duck.fetchall("SELECT variable_name FROM _variables ORDER BY variable_name")
        return [row[0] for row in rows]

    def table_name(self, type_name: str) -> str:
        """A variable's data table (``_registered_types``), else ``<Type>_data``."""
        row = self._duck.fetchone(
            "SELECT table_name FROM _registered_types WHERE type_name = ?", [type_name]
        )
        return row[0] if row and row[0] else f"{type_name}_data"

    def stored_column_types(self, type_name: str) -> "dict[str, str]":
        """``{column: DuckDB type}`` of the variable's data table as stored
        (datetime zone companions included, ``record_id`` not), in ordinal
        order."""
        rows = self._duck.fetchall(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? AND column_name != 'record_id' "
            "ORDER BY ordinal_position",
            [self.table_name(type_name)],
        )
        return {row[0]: str(row[1] or "") for row in rows}

    def dtype_meta(self, type_name: str) -> "dict | None":
        """The stored ``dtype`` metadata of a variable (``_variables.dtype``),
        or None when it has none."""
        rows = self._duck.fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?", [type_name]
        )
        if not rows or not rows[0][0]:
            return None
        raw = rows[0][0]
        return json.loads(raw) if isinstance(raw, str) else raw

    # -- read ------------------------------------------------------------------

    def long_frame(self, type_name: str, columns: "Sequence[str]" = ()) -> pd.DataFrame:
        """Every non-excluded record of a variable as one frame:
        ``record_id``, the given data ``columns`` (as stored: a datetime
        column's ``__tz`` companion must be named to come along), then every
        schema key AS TEXT.

        No columns is the metadata-only read: record ids and schema keys, and
        no payload in the SELECT at all. Through ``fetchdf`` always.

        Schema keys are cast to VARCHAR in the query, not stringified after: a
        DataFrame fetch types each column, and an integer key with one NULL
        among its rows would arrive as float64 and stringify as "1.0".
        """
        data_select = "".join(f', t."{column}"' for column in columns)
        schema_select = "".join(
            f', CAST(s."{key}" AS VARCHAR) AS "{key}"' for key in self.schema_keys()
        )
        query = (
            f"SELECT t.record_id{data_select}{schema_select} "
            f'FROM "{self.table_name(type_name)}" t {_RECORDS_OF_TYPE}'
        )
        t0 = time.perf_counter()
        frame = self._duck.fetchdf(query, [type_name])
        Log.debug(
            f"RecordStore.long_frame({type_name}): {len(frame)} record(s), "
            f"{len(columns)} data column(s) in {time.perf_counter() - t0:.3f}s"
        )
        return frame

    def sample_cell(self, type_name: str, column: str) -> Any:
        """One non-NULL value of one data column (of a non-excluded record),
        or None when the column holds none."""
        row = self._duck.fetchone(
            f'SELECT t."{column}" FROM "{self.table_name(type_name)}" t '
            f"{_RECORDS_OF_TYPE} "
            f'AND t."{column}" IS NOT NULL LIMIT 1',
            [type_name],
        )
        return row[0] if row else None

    def column_levels(self, type_name: str, column: str, *, limit: int) -> "list[str]":
        """Distinct values of one data column, as text, at most ``limit``."""
        rows = self._duck.fetchall(
            f'SELECT DISTINCT CAST(t."{column}" AS VARCHAR) AS level '
            f'FROM "{self.table_name(type_name)}" t '
            f"{_RECORDS_OF_TYPE} "
            f'AND t."{column}" IS NOT NULL '
            f"LIMIT {int(limit)}",
            [type_name],
        )
        return [row[0] for row in rows]

    def occupied_keys(self, type_name: str) -> "list[str]":
        """The schema keys a variable's non-excluded records populate, without
        reading any data (COUNT ignores NULLs, so one row of counts says
        exactly which keys are populated)."""
        keys = self.schema_keys()
        if not keys:
            return []
        counts = ", ".join(f'COUNT(s."{key}")' for key in keys)
        row = self._duck.fetchone(
            f"SELECT {counts} FROM _record r "
            f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
            f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE",
            [type_name],
        )
        if row is None:
            return []
        return [key for key, count in zip(keys, row, strict=True) if count]

    # -- write -----------------------------------------------------------------

    def write_batch_rows(self, rows: BatchRows) -> None:
        """The inserts of a planned batch, in one re-entrant transaction:
        the data rows (deduplicated by record id), the ``_record_save``
        audit rows, the ``_record`` entities, and the ``_variables`` row."""
        from scidb.provenance.graph import insert_record_entities

        timings = rows.timings
        table_name = rows.table_name
        type_name = rows.variable_class.__name__

        # --- Find which data rows are new (dedup check) ---
        t5 = time.perf_counter()
        if rows.is_dataframe:
            # No PRIMARY KEY: filter out rows whose record_id already exists.
            all_new_rids = list({row[0] for row in rows.data_rows})
            existing_rids: set = set()
            if all_new_rids:
                placeholders_rids = ", ".join(["?"] * len(all_new_rids))
                existing_rids = {
                    r[0]
                    for r in self._duck.fetchall(
                        f'SELECT DISTINCT record_id FROM "{table_name}" '
                        f"WHERE record_id IN ({placeholders_rids})",
                        all_new_rids,
                    )
                }
            new_data_rows = [row for row in rows.data_rows if row[0] not in existing_rids]
        else:
            # PRIMARY KEY (or the Arrow / columnar insert): ON CONFLICT DO
            # NOTHING handles dedup in the INSERT.
            new_data_rows = rows.data_rows
        timings["dedup_check"] = time.perf_counter() - t5

        t6 = time.perf_counter()
        # Re-entrant: inside a for_each save this joins the call's one
        # transaction, so the data rows commit with their provenance graph.
        with self._duck.transaction(f"save_batch({type_name})"):
            t6a = time.perf_counter()
            if rows.arrow_columns is not None and rows.record_ids:
                rows.n_new = self._insert_arrow_rows(rows, t6a)
            elif rows.is_custom:
                timings["data_df_create"] = time.perf_counter() - t6a
                t6b = time.perf_counter()
                rows.n_new = self.insert_columnar_frames(table_name, rows.custom_frames)
                timings["data_insert"] = time.perf_counter() - t6b
            elif new_data_rows:
                all_columns = ["record_id"] + list(rows.data_col_types.keys())
                timings["data_df_create"] = time.perf_counter() - t6a
                t6b = time.perf_counter()
                self._duck.bulk_insert(
                    table_name,
                    all_columns,
                    new_data_rows,
                    conflict_cols=None if rows.is_dataframe else ["record_id"],
                )
                rows.n_new = len(new_data_rows)
                timings["data_insert"] = time.perf_counter() - t6b
            else:
                timings["data_df_create"] = time.perf_counter() - t6a
                timings["data_insert"] = 0.0

            # Append save-event rows (audit trail — every execution logged).
            # metadata_rows is (record_id, timestamp, variable_name, schema_id,
            # content_hash, schema_version); _record_save keeps only the
            # (record_id, timestamp) save-event columns.
            t6c = time.perf_counter()
            self._duck.bulk_insert(
                "_record_save",
                ["record_id", "timestamp"],
                [(r[0], r[1]) for r in rows.metadata_rows],
                conflict_cols=["record_id", "timestamp"],
            )
            timings["record_save_insert"] = time.perf_counter() - t6c
            # The type/schema/content metadata lives on the bipartite entities
            # table (_record); map metadata_rows to the _record column order.
            t6c2 = time.perf_counter()
            insert_record_entities(
                self._duck,
                [(r[0], r[1], r[2], r[3], r[4], r[5], False) for r in rows.metadata_rows],
            )
            timings["record_entities_insert"] = time.perf_counter() - t6c2
            timings["meta_insert"] = time.perf_counter() - t6c

            # Upsert _variables (one row per variable)
            t6d = time.perf_counter()
            self._duck.execute(
                "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT (variable_name) DO UPDATE SET dtype = excluded.dtype",
                [type_name, rows.effective_level, json.dumps(rows.dtype_meta), ""],
            )
            timings["variables_upsert"] = time.perf_counter() - t6d
            t6e = time.perf_counter()
        timings["commit"] = time.perf_counter() - t6e
        timings["batch_inserts"] = time.perf_counter() - t6
        timings["total"] = time.perf_counter() - rows.t0

    def _insert_arrow_rows(self, rows: BatchRows, t_start: float) -> int:
        """PyArrow fast path: numpy arrays → Arrow buffers → DuckDB (no Python
        list conversion). Returns the number of rows offered."""
        import pyarrow as pa  # optional dependency; the caller checked it exists

        numpy_to_arrow = {
            "float64": pa.float64(),
            "float32": pa.float32(),
            "int64": pa.int64(),
            "int32": pa.int32(),
            "int16": pa.int16(),
            "int8": pa.int8(),
            "uint64": pa.uint64(),
            "uint32": pa.uint32(),
            "uint16": pa.uint16(),
            "uint8": pa.uint8(),
            "bool": pa.bool_(),
        }
        arrow_data = {"record_id": pa.array(rows.record_ids, type=pa.string())}
        for col_name, np_list in (rows.arrow_columns or {}).items():
            col_meta = rows.dtype_meta["columns"][col_name]
            numpy_dtype = col_meta.get("numpy_dtype", "float64")
            arrow_inner = numpy_to_arrow.get(numpy_dtype, pa.float64())
            arrow_data[col_name] = pa.array(np_list, type=pa.list_(arrow_inner))
        arrow_table = pa.table(arrow_data)
        col_str = ", ".join(f'"{c}"' for c in arrow_data)
        rows.timings["data_df_create"] = time.perf_counter() - t_start

        t6b = time.perf_counter()
        self._duck.con.register("arrow_table", arrow_table)
        try:
            self._duck.con.execute(
                f'INSERT INTO "{rows.table_name}" ({col_str}) SELECT * FROM arrow_table '
                f"ON CONFLICT (record_id) DO NOTHING"
            )
        finally:
            self._duck.con.unregister("arrow_table")
        rows.timings["data_insert"] = time.perf_counter() - t6b
        return len(rows.record_ids)

    def insert_columnar_frames(
        self, table_name: str, frames: "list[tuple[str, pd.DataFrame]]"
    ) -> int:
        """Insert ``(record_id, frame)`` pairs into a columnar data table in
        ONE statement, skipping record_ids already stored (one lookup query)
        and repeats within *frames*. Returns the number of records inserted.

        The table has no PRIMARY KEY (one row per frame row), so dedup is
        done here rather than by ``ON CONFLICT``.
        """
        if not frames:
            return 0
        rids = list(dict.fromkeys(rid for rid, _ in frames))
        placeholders = ", ".join(["?"] * len(rids))
        existing = {
            r[0]
            for r in self._duck.fetchall(
                f'SELECT DISTINCT record_id FROM "{table_name}" '
                f"WHERE record_id IN ({placeholders})",
                rids,
            )
        }
        new_parts = []
        seen: "set[str]" = set(existing)
        for rid, df in frames:
            if rid in seen:
                continue
            seen.add(rid)
            part = df.reset_index(drop=True)
            part.insert(0, "record_id", rid)
            new_parts.append(part)
        if not new_parts:
            Log.debug(
                f"_insert_columnar_frames: all {len(rids)} record(s) already in "
                f"'{table_name}', skipped"
            )
            return 0
        insert_df = new_parts[0] if len(new_parts) == 1 else pd.concat(new_parts, ignore_index=True)
        col_str = ", ".join(f'"{c}"' for c in insert_df.columns)
        self._duck.con.register("__columnar_insert", insert_df)
        try:
            self._duck.con.execute(
                f'INSERT INTO "{table_name}" ({col_str}) '
                f"SELECT {col_str} FROM __columnar_insert"
            )
        finally:
            self._duck.con.unregister("__columnar_insert")
        Log.debug(
            f"_insert_columnar_frames: inserted {len(new_parts)} record(s), "
            f"{len(insert_df)} row(s) into '{table_name}' "
            f"({len(existing)} already stored)"
        )
        return len(new_parts)
