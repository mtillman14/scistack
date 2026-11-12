"""DatabaseManager: loading — finding records, rebuilding their metadata and data, and the bulk DataFrame loading engine.

Moved verbatim out of database.py (grug follow-up): DatabaseManager inherits
this mixin, so every method is still `db.<name>`. Each method's `self` is
annotated as DatabaseManager, so the IDE knows its attributes.
"""

from typing import TYPE_CHECKING

import json
import numpy as np
import pandas as pd
import time
from importlib import metadata
from scidb.model.exceptions import NotFoundError
from scidb.model.log import Log
from scidb.model.variable import BaseVariable
from scidb.provenance import query as provenance_query
from scidb.store.db_helpers import _filter_records_by_branch_params
from scidb.store.db_helpers import _unflatten_struct_columns
from scidb.store.schema_values import schema_str
from sciduckdb import count_null_list_elements
from sciduckdb import is_datetime_meta
from sciduckdb import merge_tz_columns
from sciduckdb import restore_types
from sciduckdb import storage_column_names
from sciduckdb import storage_to_python
from sciduckdb import storage_to_python_column
from sciduckdb import unflatten_dict
from typing import Any

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.filters import Filter
from scidb.model.filters import split_schema_key_filters
class LoadMixin:
    """DatabaseManager: loading — finding records, rebuilding their metadata and data, and the bulk DataFrame loading engine."""

    def _any_records_exist(self: "DatabaseManager", type_name: str) -> bool:
        """Return True if any records of this type have ever been saved."""
        rows = self._duck.fetchall(
            "SELECT 1 FROM _record WHERE type = ? LIMIT 1",
            [type_name],
        )
        return len(rows) > 0

    def _find_record(
        self: "DatabaseManager",
        type_name: str,
        record_id: str | None = None,
        nested_metadata: dict | None = None,
        version_id: str = "all",
        branch_params_filter: dict | None = None,
        include_excluded: bool = False,
    ) -> pd.DataFrame:
        """
        Query the _record_save log (joined to _record) to find matching records.

        Supports two modes:
        - By record_id: direct primary key lookup (with JOINs for full row data)
        - By metadata: filter by schema keys via JOIN with _schema, then match
          non-schema metadata + branch_params against the bipartite graph,
          order by timestamp DESC

        version_id controls which versions are returned:
        - "all" (default): no version filtering (return every version)
        - "latest": only the latest row per (variable_name, schema_id, variant),
          where the variant is graph-derived (producing function + accumulated
          constants)
        - any other string: treated as a record_id for direct lookup

        branch_params_filter: optional dict of branch_params key/value filters
        include_excluded: if False (default), skip records with excluded=TRUE

        Schema key values and filter values may be lists, interpreted as
        "match any" (SQL IN / Python in).

        Returns a DataFrame of matching rows including schema columns.
        """
        # Build schema column SELECT list
        schema_col_select = ", ".join(f's."{col}"' for col in self.dataset_schema_keys)

        # The save-event log (_record_save, one row per (record_id, timestamp))
        # joined to the _record entity (one row per record_id) for the
        # type/schema/content/excluded columns the log no longer stores. Aliased
        # back to the legacy column names so downstream row access is unchanged.
        meta_select = (
            "rm.record_id, rm.timestamp, "
            "r.type AS variable_name, r.schema_id, r.content_hash, "
            "r.schema_version, r.excluded"
        )
        meta_from = (
            "FROM _record_save rm "
            "JOIN _record r ON r.record_id = rm.record_id "
            "LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        )

        excluded_clause = (
            "" if include_excluded else " AND COALESCE(r.excluded, FALSE) = FALSE"
        )

        if record_id is not None:
            sql = (
                f"SELECT {meta_select}, {schema_col_select} "
                f"{meta_from}"
                f"WHERE rm.record_id = ? AND r.type = ?{excluded_clause}"
            )
            df = self._duck.fetchdf(sql, [record_id, type_name])
            return self._sort_by_schema_keys(df)

        # Unrecognized version_id → treat as record_id lookup
        if version_id not in ("latest", "all"):
            Log.debug(
                f"_find_record({type_name}): treating version_id={version_id!r} as record_id"
            )
            sql = (
                f"SELECT {meta_select}, {schema_col_select} "
                f"{meta_from}"
                f"WHERE rm.record_id = ? AND r.type = ?{excluded_clause}"
            )
            df = self._duck.fetchdf(sql, [version_id, type_name])
            return self._sort_by_schema_keys(df)

        # By metadata
        schema_keys = nested_metadata.get("schema", {}) if nested_metadata else {}
        version_keys = nested_metadata.get("version", {}) if nested_metadata else {}

        conditions = ["r.type = ?"]
        params: list[Any] = [type_name]

        # Exclude excluded variants by default
        if not include_excluded:
            conditions.append("COALESCE(r.excluded, FALSE) = FALSE")

        # Filter schema keys via _schema columns in SQL (lists → IN)
        for key, value in schema_keys.items():
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["?"] * len(value))
                conditions.append(f's."{key}" IN ({placeholders})')
                params.extend([schema_str(v) for v in value])
            else:
                conditions.append(f's."{key}" = ?')
                params.append(schema_str(value))

        # branch_params_filter is handled entirely in Python (lines below).
        # We do NOT push it to SQL because:
        # 1. Keys stored in version_keys (direct user saves) would be incorrectly
        #    excluded by a branch_params SQL filter before the Python fallback runs.
        # 2. Suffix matching (e.g. "low_hz" → "bandpass.low_hz") cannot be expressed
        #    in SQL without fetching all records anyway.
        # The Python _match_row already checks version_keys first, then branch_params.

        where = " AND ".join(conditions)

        # One row per distinct record_id at the SQL level, at its newest save
        # event (dedups audit re-saves of identical content; the one recency
        # window, provenance_query.LATEST_SAVE_PER_RECORD_SQL). For
        # version_id="latest" the *variant* collapse — keeping the newest
        # record per (fn, accumulated constants, consumed locations) — is done
        # in Python below using the bipartite graph.
        sql = (
            f"WITH ranked AS ("
            f"SELECT {meta_select}, {schema_col_select}, "
            f"{provenance_query.LATEST_SAVE_PER_RECORD_SQL} as rn "
            f"{meta_from}"
            f"WHERE {where}"
            f") SELECT * FROM ranked WHERE rn = 1"
        )
        _t_sql = time.perf_counter()
        df = self._duck.fetchdf(sql, params)
        t_sql = time.perf_counter() - _t_sql
        Log.debug(
            f"_find_record({type_name}): SQL returned {len(df)} records, version_id={version_id}"
        )
        # Collapse to the CURRENT record of each variant when version_id="latest".
        # THE rule is provenance_query.current_record_ids (one owner with node
        # state): a variant is the producing function's name + the accumulated
        # upstream constants + the consumed input schema locations; within one,
        # the newest wins, after the run-option and renamed-function
        # supersession rules. Raw saves are one variant per location.
        t_collapse = 0.0
        rows_before_collapse = len(df)
        if version_id == "latest" and len(df) > 0:
            _t_collapse = time.perf_counter()
            current = provenance_query.current_record_ids(
                self._duck,
                list(zip(df["record_id"], df["variable_name"], df["schema_id"], df["timestamp"])),
                context=f"_find_record({type_name}, latest)",
            )
            if current.dropped_by_family or current.dropped_globally:
                Log.info(
                    f"_find_record({type_name}, latest): run-option supersession "
                    f"superseded {current.dropped_by_family + current.dropped_globally} "
                    f"record(s) across {current.superseded_families} location(s) — an "
                    f"older distribute/as_table run, a since-renamed function, or a newer hand save"
                )
            keep_mask = df["record_id"].isin(current.kept)
            collapsed_count = int((~keep_mask).sum())
            if collapsed_count > 0:
                Log.debug(
                    f"_find_record: collapsed {collapsed_count} non-latest variant row(s)"
                )
            df = df[keep_mask]

            # --- Diagnostic: why do some locations still have >1 record? ---
            # After "latest per (variable, schema_id, variant)" collapse, a schema
            # KEY location should normally map to ONE record. If it doesn't, the
            # survivors differ either by schema_id (two schema_ids allocated for
            # identical key values) or by variant_key (same schema_id but distinct
            # producing fn / branch_params / output_num / consumed input locations).
            # The branch is vectorized and only does work when duplicates remain.
            _sk = self.dataset_schema_keys
            if _sk and len(df) > 1:
                _dupmask = df.duplicated(subset=_sk, keep=False)
                if _dupmask.any():
                    _dup = df[_dupmask]
                    # dropna=False: a record saved above the deepest schema key
                    # has NULL there (a trial-level record in a schema that ends
                    # in cycle), and pandas drops NULL-keyed groups by default —
                    # which made this WARN say "0 location(s) ... Examples:"
                    # with nothing after it, for exactly the rows it exists to
                    # describe (2026-09-14, GAITRiteLoaded).
                    _n_locs = _dup.groupby(_sk, sort=False, dropna=False).ngroups
                    _samples = []
                    for _vals, _g in _dup.groupby(_sk, sort=False, dropna=False):
                        if len(_samples) >= 3:
                            break
                        _sids = sorted(set(_g["schema_id"].tolist()))
                        _rids = _g["record_id"].tolist()
                        if len(_sids) > 1:
                            _kind = f"DISTINCT schema_id for identical keys {_sids}"
                        else:
                            _vks = [current.variant_keys.get(_r) for _r in _rids]
                            _kind = f"same schema_id {_sids[0]}, DISTINCT variant_keys={_vks}"
                        _loc = dict(
                            zip(
                                _sk,
                                _vals if isinstance(_vals, tuple) else (_vals,),
                                strict=False,
                            )
                        )
                        _samples.append(
                            f"{_loc}: {len(_g)} records, record_ids={_rids[:4]} -> {_kind}"
                        )
                    Log.warn(
                        f"_find_record({type_name}, latest): {_n_locs} schema "
                        f"location(s) retain >1 record after collapse — these feed "
                        f"multi-row per-combo tables. Examples: " + "; ".join(_samples)
                    )

            # Apply smart sorting by schema keys (numeric or alphabetic per column)
            df = self._sort_by_schema_keys(df)
            t_collapse = time.perf_counter() - _t_collapse

        t_vk_filter = 0.0

        # Filter by non-schema metadata. Both the "version" portion of the
        # metadata (non-schema kwargs passed plainly, e.g. find_record_id(C,
        # {subject:1, low_hz:20})) and an explicit ``branch_params_filter`` are
        # matched against graph-derived branch params (constants on the producing
        # invocation — incl. direct-save kwargs via the synthetic __save__
        # invocation), with bare-name suffix matching.
        combined_bp = {**version_keys, **(branch_params_filter or {})}
        t_bp_filter = 0.0
        if combined_bp and len(df) > 0:
            _t_bp = time.perf_counter()
            df = _filter_records_by_branch_params(df, combined_bp, self._duck)
            t_bp_filter = time.perf_counter() - _t_bp

        # Apply smart sorting by schema keys before returning
        _t_sort = time.perf_counter()
        result = self._sort_by_schema_keys(df)
        t_sort = time.perf_counter() - _t_sort

        total = t_sql + t_collapse + t_vk_filter + t_bp_filter + t_sort
        Log.info(
            f"[timing] _find_record({type_name}, version_id={version_id!r}): "
            f"total={total:.3f}s, sql={t_sql:.3f}s ({rows_before_collapse} rows), "
            f"collapse={t_collapse:.3f}s, vk_filter={t_vk_filter:.3f}s, "
            f"bp_filter={t_bp_filter:.3f}s, sort={t_sort:.3f}s, "
            f"returned={len(result)} rows"
        )
        return result

    def _reconstruct_metadata_from_row(
        self: "DatabaseManager", row: pd.Series, branch_params: "dict | None" = None
    ) -> tuple[dict, dict]:
        """
        Reconstruct flat and nested metadata for a record.

        Schema keys come from the JOINed _schema columns; the non-schema
        "version" portion is the record's direct-save kwargs, derived from the
        bipartite graph (the synthetic ``__save__`` invocation's constants —
        see :func:`provenance_query.derived_branch_params`). Internal pipeline
        markers (``__fn`` etc.) are not user metadata and are not exposed.

        *branch_params* is the record's already-derived branch params, for a
        caller that batched them (``provenance_query.branch_params_batch``);
        without it this walks the record's ancestry itself, one record at a
        time — the N+1 a loop over rows must not pay (cleanup-audit F3).

        Returns (flat_metadata, nested_metadata).
        """
        schema = {}
        for key in self.dataset_schema_keys:
            if key in row.index:
                val = row[key]
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    schema[key] = self.restore_schema_value(key, val)

        # Direct-save kwargs: __save__.<kwarg> entries in derived branch params.
        version = {}
        rid = row.get("record_id")
        if branch_params is None and rid is not None:
            branch_params = provenance_query.derived_branch_params(self._duck, rid)
        for k, v in (branch_params or {}).items():
            if k.startswith("__save__."):
                version[k[len("__save__.") :]] = v

        nested_metadata = {"schema": schema, "version": version}
        flat_metadata = {}
        flat_metadata.update(schema)
        flat_metadata.update(version)
        return flat_metadata, nested_metadata

    def _deserialize_custom_subdf(
        self: "DatabaseManager",
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

    def _load_with_where(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        metadata: dict,
        table_name: str,
        where,
        version_id: str = "latest",
    ):
        """Load records using where= with **semantic** variant matching.

        ``where=`` is a *variant/computation* selector: its variable-level portion
        identifies which computed variant to return by the **schema_id set it
        resolves to now** (§10 "where= redesign"). Any SchemaKey portion is a *row*
        selector applied on top. The producing run's ``where_clause`` STRING is
        never read here — it survives only for visual inspection (get_execution_audit).

        Matching rule (subset + raw fallback), which collapses the old Strategy 1
        (provenance) and Strategy 2 (schema fallback) into one mechanism:
          - a record **with** a producing invocation matches iff every input
            schema_id that invocation consumed is within the resolved variant set
            ``S_var`` (an aggregation consumed all of ``S_var``; a per-combo output
            consumed a single location in it);
          - a record with **no** producing invocation (raw/direct save) matches iff
            its own schema_id ∈ ``S_var``.
        The result is then intersected with the SchemaKey / pre-resolved row set.

        Returns:
            A pandas DataFrame of matching record rows.

        Raises:
            NotFoundError: If no records match.
        """
        type_name = variable_class.__name__


        # Separate the variant role (variable-level filter → S_var) from the row
        # role (SchemaKey portion, or a Merge constituent's pre-resolved id set).
        # ``variant_validate_coverage`` is False for Merge constituents (the merge
        # path validates coverage once over the merged result, not per constituent).
        variant_validate_coverage = True
        if getattr(where, "_restrict_to_resolved_ids", False):
            # Merge constituent: variant filter carried explicitly; resolve() is the
            # pre-resolved row restriction (full filter, variable-level AND SchemaKey).
            var_filter = getattr(where, "_variable_filter", None)
            row_ids = where.resolve(self, variable_class, table_name)
            variant_validate_coverage = False
        elif isinstance(where, Filter):
            sk_filter, var_filter = split_schema_key_filters(where)
            row_ids = (
                sk_filter.resolve(self, variable_class, table_name)
                if sk_filter is not None
                else None
            )
        elif where is None or hasattr(where, "resolve"):
            var_filter, row_ids = where, None
        else:
            raise TypeError(
                f"load(where=...) expects a Filter (e.g. Var=='x', raw_sql(...)); "
                f"got {type(where).__name__}."
            )

        nested_base = self._split_metadata(metadata)
        records_all = self._find_record(
            type_name, nested_metadata=nested_base, version_id=version_id
        )
        if len(records_all) == 0:
            raise NotFoundError(f"No {type_name} found matching metadata: {metadata}")

        records = records_all

        # --- Variant match (semantic) on the variable-level portion ---
        if var_filter is not None:
            s_var = frozenset(
                var_filter.resolve(
                    self,
                    variable_class,
                    table_name,
                    validate_coverage=variant_validate_coverage,
                )
            )
            # The consumed-input subset test runs at the INPUT variable's level,
            # which the target-resolved ``s_var`` does not capture when the output
            # is coarser than the filter (cross-level where=): there ``resolve``
            # short-circuits a finer filter to "all target ids", so consumed
            # (input-level) ids could never be a subset. Resolve the filter at its
            # OWN granularity for that test; fall back to ``s_var`` for filters with
            # no intrinsic level (raw SQL) and for the same-level case (identical).
            try:
                s_var_native = frozenset(var_filter.resolve_native(self))
            except NotImplementedError:
                s_var_native = s_var
            consumed_map = provenance_query.consumed_input_schema_ids(
                self._duck, records_all["record_id"].tolist()
            )

            fell_back: list[str] = []

            def _variant_match(row, _s=s_var, _sn=s_var_native, _c=consumed_map):
                consumed = _c.get(row["record_id"])
                if consumed is not None:
                    return (
                        consumed <= _sn
                    )  # subset: every consumed loc ∈ S_var (native level)
                fell_back.append(row["record_id"])
                return row["schema_id"] in _s  # raw/direct-save fallback (target level)

            records = records_all[records_all.apply(_variant_match, axis=1)]
            Log.debug(
                f"[_load_with_where] {type_name}: variant match kept {len(records)} "
                f"of {len(records_all)} records (|S_var|={len(s_var)})"
            )
            # The raw/direct-save fallback is correct for a record nothing
            # produced, but it CANNOT narrow a cross-level where=: there
            # ``resolve`` short-circuits a finer filter to "all target ids", so
            # ``schema_id in S_var`` is true for every candidate and the filter
            # silently selects nothing at all. Two variants at one output
            # location then both come back and the caller gets an ambiguous
            # result instead of the one it asked for.
            #
            # Reaching it for a record that HAS a producing invocation means the
            # invocation recorded no variable input edges — which is the only
            # thing that makes this variant identity work. Say so: without this
            # line the symptom is "load returned 2 records" with nothing
            # anywhere connecting it to missing provenance edges.
            if fell_back and s_var_native != s_var:
                Log.warn(
                    f"[_load_with_where] {type_name}: {len(fell_back)} record(s) "
                    f"had no recorded consumed-input locations, so a CROSS-LEVEL "
                    f"where= fell back to matching on the record's own schema_id "
                    f"— which cannot narrow (the filter resolves to every target "
                    f"location). Expect an over-broad result. Record(s): "
                    f"{fell_back[:5]}"
                )

            # A where= that selects a variant is supposed to land on ONE of
            # them. More than one surviving means the subset test admitted
            # records it should have excluded, and the only way to see WHY is
            # the two sets it compared — the filter's own resolved locations
            # against each record's consumed locations. Both are small here (a
            # handful of schema_ids), this fires only on the ambiguous case, and
            # without it the symptom is a bare "load returned 2 records" with
            # nothing to attribute it to.
            if len(records) > 1:
                Log.info(
                    f"[_load_with_where] {type_name}: where= left "
                    f"{len(records)} records AMBIGUOUS (expected one variant). "
                    f"filter={var_filter!r} resolved natively to "
                    f"{sorted(s_var_native)}; target-level S_var="
                    f"{sorted(s_var)}; per-record consumed inputs: "
                    + ", ".join(
                        f"{rid}(schema={sid}) consumed="
                        f"{sorted(consumed_map[rid]) if rid in consumed_map else 'NONE'}"
                        for rid, sid in zip(
                            records["record_id"].tolist(),
                            records["schema_id"].tolist(),
                            strict=False,
                        )
                    )
                )

        # --- Row restriction (SchemaKey portion / pre-resolved Merge ids) ---
        if row_ids is not None:
            _before = len(records)
            records = records[records["schema_id"].isin(row_ids)]
            Log.debug(
                f"[_load_with_where] {type_name}: row restriction {_before} -> "
                f"{len(records)} records ({len(row_ids)} allowed schema_ids)"
            )

        if len(records) == 0:
            raise NotFoundError(
                f"No {type_name} found matching metadata: {metadata} "
                f"with the given where= filter."
            )
        return records

    # -------------------------------------------------------------------------
    # Bulk DataFrame loading engine
    # -------------------------------------------------------------------------

    def _load_as_df_via_iterator(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        metadata: dict,
        *,
        layout: str,
        include_rid: bool,
        include_bp: bool,
        stringify_schema: bool,
        version_id: str,
        where,
        branch_params_filter: dict | None,
    ) -> pd.DataFrame:
        """Slow fallback: load via instance iterator, then assemble DataFrame.

        Mirrors the output shape of the fast path but uses ``db.load()`` so
        it is always correct regardless of dtype or subclass customisation.
        """
        schema_keys_set = set(self.dataset_schema_keys)
        view_name = (
            variable_class.view_name()
            if hasattr(variable_class, "view_name")
            else variable_class.__name__
        )

        loaded = list(
            self.load(
                variable_class,
                metadata,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        )

        if not loaded:
            return pd.DataFrame()

        def _process_meta(var, *, spread: bool) -> dict:
            meta = dict(var.metadata) if var.metadata else {}
            if spread:
                # Drop __* keys and const-param keys (same as _stringify_meta).
                const_val = meta.get("__constants", {})
                if isinstance(const_val, str):
                    try:
                        const_val = json.loads(const_val)
                    except Exception:
                        const_val = {}
                const_keys = set(const_val.keys()) if const_val else set()
                result = {}
                for k, v in meta.items():
                    if k.startswith("__") or k in const_keys:
                        continue
                    result[k] = (
                        schema_str(v)
                        if (k in schema_keys_set and stringify_schema and v is not None)
                        else v
                    )
                return result
            else:
                # Packed: include all metadata (current BaseVariable.load(as_df=True) behaviour).
                return meta

        is_spread = layout == "spread"
        first = loaded[0]

        if hasattr(first, "data") and isinstance(first.data, pd.DataFrame):
            # DataFrame-mode variable
            if layout == "spread":
                all_data: list = []
                all_meta_rows: list = []
                for var in loaded:
                    data_df = var.data
                    meta = _process_meta(var, spread=True)
                    if include_rid:
                        meta["__record_id"] = var.record_id
                    if include_bp:
                        meta["__branch_params"] = json.dumps(var.branch_params or {})
                    nr = len(data_df)
                    for _ in range(nr):
                        all_meta_rows.append(meta)
                    all_data.append(data_df.reset_index(drop=True))
                if not all_meta_rows:
                    return pd.DataFrame()
                combined_meta = pd.DataFrame(all_meta_rows)
                combined_data = pd.concat(all_data, ignore_index=True)
                return pd.concat(
                    [
                        combined_meta.reset_index(drop=True),
                        combined_data.reset_index(drop=True),
                    ],
                    axis=1,
                )
            else:  # packed
                rows = []
                for var in loaded:
                    row = _process_meta(var, spread=False)
                    row["data"] = var.data
                    if include_rid:
                        row["__record_id"] = var.record_id
                    if include_bp:
                        row["__branch_params"] = json.dumps(var.branch_params or {})
                    rows.append(row)
                return pd.DataFrame(rows)
        else:
            # Scalar / array / dict mode
            col_name = "data" if layout == "packed" else view_name
            rows = []
            for var in loaded:
                row = _process_meta(var, spread=is_spread)
                row[col_name] = var.data if hasattr(var, "data") else var
                if include_rid:
                    row["__record_id"] = (
                        var.record_id if hasattr(var, "record_id") else None
                    )
                if include_bp:
                    row["__branch_params"] = json.dumps(
                        var.branch_params if hasattr(var, "branch_params") else {}
                    )
                rows.append(row)
            return pd.DataFrame(rows)

    def _assemble_df_from_records_and_data(
        self: "DatabaseManager",
        records: pd.DataFrame,
        data_df: pd.DataFrame,
        dtype_meta: dict,
        variable_class: type[BaseVariable],
        *,
        layout: str,
        include_rid: bool,
        include_bp: bool,
        stringify_schema: bool,
    ) -> pd.DataFrame:
        """Assemble the output DataFrame from bulk-fetched records and data.

        Fast path — no per-record Python loops, no BaseVariable construction.
        Called only when all dispatch checks pass (native dtype, no subclass overrides).
        """
        mode = dtype_meta.get("mode", "single_column")
        columns_meta = dtype_meta.get("columns", {})
        data_cols = list(columns_meta.keys())
        schema_keys = self.dataset_schema_keys
        view_name = (
            variable_class.view_name()
            if hasattr(variable_class, "view_name")
            else variable_class.__name__
        )

        # -- Apply vectorized type restoration to data columns --
        # NULL list elements come back from DuckDB as masked arrays and are
        # restored as NaN by sciduckdb; count them BEFORE restoration so the
        # log says how many were touched (they used to become 0 — 2026-09-15).
        null_counts: dict = {}
        for col, col_meta in columns_meta.items():
            if col in data_df.columns:
                n_null = count_null_list_elements(data_df[col])
                if n_null:
                    null_counts[col] = n_null
                data_df[col] = storage_to_python_column(data_df[col], col_meta)
        if null_counts:
            Log.info(
                f"load_all_as_df({variable_class.__name__}): restored "
                f"{sum(null_counts.values())} NULL list element(s) as NaN in "
                f"{len(null_counts)} column(s): {null_counts}"
            )

        # -- Build metadata DataFrame --
        meta_dict: dict = {}

        # Always include record_id temporarily for joining; rename/drop at end.
        meta_dict["__record_id"] = records["record_id"].values

        # Schema columns
        for key in schema_keys:
            if key in records.columns:
                col_series = records[key]
                if stringify_schema:
                    # Values come back as strings from DuckDB VARCHAR columns
                    meta_dict[key] = col_series.values
                elif self.schema_key_is_datetime(key):
                    # Object cells: pandas would collapse aware timestamps to
                    # naive UTC, and days and timestamps may mix in one key.
                    cells = np.empty(len(col_series), dtype=object)
                    cells[:] = [
                        None if v is None else self.restore_schema_value(key, v)
                        for v in col_series.tolist()
                    ]
                    meta_dict[key] = cells
                else:
                    meta_dict[key] = col_series.apply(
                        lambda v, _key=key: (
                            self.restore_schema_value(_key, v)
                            if v is not None
                            and not (isinstance(v, float) and pd.isna(v))
                            else None
                        )
                    ).values

        # Non-schema metadata columns: a record's direct-save kwargs, derived
        # from the graph (the synthetic ``__save__`` invocation's constants).
        # Internal pipeline markers (__fn etc.) and fn sweep-constants are not
        # exposed here — the latter appear in the ``__branch_params`` column below.

        # Batched: derive every record's branch params in one closure build,
        # reused for both the __save__ kwarg columns and the __branch_params
        # column (previously two per-record ancestry walks each — the dominant
        # cost when assembling large spread results).
        rec_id_values = records["record_id"].values
        bp_map = provenance_query.branch_params_batch(self._duck, list(rec_id_values))
        per_row_kwargs: list[dict] = []
        kwarg_col_names: dict[str, list] = {}
        for rid in rec_id_values:
            kw = {
                k[len("__save__.") :]: v
                for k, v in bp_map.get(rid, {}).items()
                if k.startswith("__save__.")
            }
            per_row_kwargs.append(kw)
            for name in kw:
                if name not in kwarg_col_names:
                    kwarg_col_names[name] = [None] * len(records)
        for i, kw in enumerate(per_row_kwargs):
            for name, val in kw.items():
                kwarg_col_names[name][i] = val

        meta_dict.update(kwarg_col_names)

        # Branch params column — derived from the bipartite graph (§6) per record,
        # not read from a stored column.
        if include_bp:
            meta_dict["__branch_params"] = [
                json.dumps(bp_map.get(rid, {})) for rid in rec_id_values
            ]

        meta_df = pd.DataFrame(meta_dict)

        # Detect and warn about orphaned records: present in the save log / _record
        # but absent from the data table.  These arise when a save partially failed
        # or when a record was logged without a corresponding data row (e.g. a buggy
        # prior for_each run with an unexpected schema key).
        # Using an inner join below excludes them; we surface a warning so users can
        # investigate the root cause in their database.
        type_name = variable_class.__name__
        data_rids = set(data_df["record_id"].tolist()) if not data_df.empty else set()
        meta_rids = set(meta_df["__record_id"].tolist())
        orphaned = meta_rids - data_rids
        if orphaned:
            sample = sorted(orphaned)[:3]
            Log.warn(
                f"load_all_as_df({type_name}): {len(orphaned)} record(s) exist in "
                f"the save log but have no data in {type_name}_data — "
                f"excluding from results. Sample IDs: {sample}"
                f"{'...' if len(orphaned) > 3 else ''}. "
                f"This usually means a previous save partially failed or used an "
                f"unexpected schema key. Inspect the database for orphaned rows."
            )

        # -- Assemble by storage mode --
        if mode == "dataframe":
            df_columns = dtype_meta.get("df_columns", data_cols)

            if layout == "spread":
                # INNER JOIN meta to data on record_id — excludes orphaned records
                # (those with no data rows) so they never appear as NaN-filled rows.
                result = meta_df.merge(
                    data_df,
                    left_on="__record_id",
                    right_on="record_id",
                    how="inner",
                )
                result = result.drop(columns=["record_id"], errors="ignore")
                if not include_rid:
                    result = result.drop(columns=["__record_id"], errors="ignore")
                return result.reset_index(drop=True)
            else:  # packed
                # Group data by record_id and build a nested DataFrame per record.
                grouped = data_df.groupby("record_id", sort=False)
                packed_map: dict = {}
                for rid, group in grouped:
                    g = group.drop(columns=["record_id"], errors="ignore").reset_index(
                        drop=True
                    )
                    cols_present = [c for c in df_columns if c in g.columns]
                    packed_map[rid] = g[cols_present] if cols_present else g
                meta_df["data"] = meta_df["__record_id"].map(packed_map)
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)

        elif mode == "single_column":
            # data_df: [record_id, value_col] — one row per record.
            col_name = data_cols[0] if data_cols else None
            if col_name is None:
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)

            out_col = "data" if layout == "packed" else view_name
            data_renamed = data_df[["record_id", col_name]].rename(
                columns={"record_id": "__record_id", col_name: out_col}
            )
            # INNER JOIN: excludes orphaned records so they never produce NaN data rows.
            result = meta_df.merge(data_renamed, on="__record_id", how="inner")
            if not include_rid:
                result = result.drop(columns=["__record_id"], errors="ignore")
            return result.reset_index(drop=True)

        elif mode == "multi_column":
            # data_df: [record_id, col1, col2, …] — one row per record.
            if layout == "packed":
                data_only = data_df.drop(columns=["record_id"], errors="ignore")
                data_dicts = data_only.to_dict("records")
                data_map = dict(zip(data_df["record_id"], data_dicts, strict=False))
                meta_df["data"] = meta_df["__record_id"].map(data_map)
                if not include_rid:
                    meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
                return meta_df.reset_index(drop=True)
            else:  # spread
                data_renamed = data_df.rename(columns={"record_id": "__record_id"})
                # INNER JOIN: excludes orphaned records so they never produce NaN data rows.
                result = meta_df.merge(data_renamed, on="__record_id", how="inner")
                if not include_rid:
                    result = result.drop(columns=["__record_id"], errors="ignore")
                return result.reset_index(drop=True)

        else:
            # Unknown mode — return just metadata.
            if not include_rid:
                meta_df = meta_df.drop(columns=["__record_id"], errors="ignore")
            return meta_df.reset_index(drop=True)

    def get_dtype_meta(self: "DatabaseManager", variable_name: str) -> "dict | None":
        """The stored ``dtype`` metadata for a variable type, or ``None``.

        ``dtype_meta`` records how sciduckdb laid the value out on disk —
        ``mode`` (``single_column`` / ``multi_column`` / ``dataframe``),
        the per-column type map, and the ``nested`` / ``custom`` flags. It is
        the only record of the fact that a variable's rows are the fields of
        one dict rather than the columns of one table, which is a distinction
        the loaded DataFrame cannot express on its own.

        Read-only; the query is ``RecordStore.dtype_meta``.
        """
        return self.record_store.dtype_meta(variable_name)

    def mapping_data_columns(self: "DatabaseManager", variable_class) -> "list[str] | None":
        """Column names to reassemble into a dict for a dict-valued variable.

        Returns the ordered dict keys when *variable_class* stores in
        ``multi_column`` mode (one DuckDB column per dict key), otherwise
        ``None``.

        Why callers need it: ``load_all_as_df(layout="spread")`` deliberately
        spreads those keys across top-level columns, because column selection
        (``Type("col")``), ``ColName``, ``Merge``, ``as_table=True``,
        ``share_limits`` and ``for_columns`` all operate on them. The cost is
        that a single record then looks exactly like a one-row table, and the
        per-combo extraction in scifor has no way to tell the two apart —
        so a struct saved from MATLAB (or a dict from Python) came back as a
        1-row table. Compare ``_load_record``'s single-record path, which does
        rebuild the dict, and ``_load_as_df_via_iterator``, which keeps a
        ``nested`` dict in a single column: both already round-trip.

        ``nested`` and ``custom`` layouts are excluded — they never reach the
        spreading fast path (``load_all_as_df`` routes them to the iterator,
        which already returns the value whole).
        """
        name = getattr(variable_class, "__name__", None)
        if not name:
            return None
        dtype_meta = self.get_dtype_meta(name)
        if not dtype_meta:
            return None
        if dtype_meta.get("custom") or dtype_meta.get("nested"):
            return None
        if dtype_meta.get("mode") != "multi_column":
            return None
        return list(dtype_meta.get("columns", {}).keys()) or None

    def data_column_names(self: "DatabaseManager", variable_class: type[BaseVariable]) -> "list[str] | None":
        """The data columns a ``layout="spread"`` load of *variable_class*
        returns, read from storage metadata (``_variables.dtype``) — no data
        is loaded. ``None`` when metadata cannot answer and only a load can.

        THE owner of that answer, mirroring :meth:`load_all_as_df`'s fast path
        and :meth:`_assemble_df_from_records_and_data`: a ``dataframe`` or
        ``multi_column`` type spreads its stored columns in stored order; a
        ``single_column`` type spreads one column named after the variable
        (``view_name``). Whatever routes the load to the iterator fallback
        (custom or nested storage, an overridden ``__init__``, custom
        serialization) answers ``None`` here too, so a caller never gets a
        column list the real load would not produce.

        Unlike reading ``loaded.columns``, this never includes a record's
        direct-save kwarg columns (``version="v1"`` spreads as a ``version``
        column): they are metadata, not data (cleanup-audit F28).
        """
        type_name = variable_class.__name__
        rows = self._duck.fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?", [type_name]
        )
        if not rows:
            return None
        dtype_meta = json.loads(rows[0][0])
        if (
            variable_class.__init__ is not BaseVariable.__init__
            or self._has_custom_serialization(variable_class)
            or dtype_meta.get("custom")
            or dtype_meta.get("nested")
        ):
            return None
        data_cols = list(dtype_meta.get("columns", {}).keys())
        mode = dtype_meta.get("mode", "single_column")
        if mode == "single_column":
            if not data_cols:
                return None
            return [
                variable_class.view_name()
                if hasattr(variable_class, "view_name")
                else type_name
            ]
        if mode in ("dataframe", "multi_column"):
            return data_cols or None
        return None

    def load_all_as_df(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        metadata: dict | None = None,
        *,
        layout: str = "packed",
        include_rid: bool = False,
        include_bp: bool = False,
        stringify_schema: bool = False,
        version_id: str = "latest",
        where=None,
        branch_params_filter: dict | None = None,
    ) -> pd.DataFrame:
        """Bulk load all records of a variable type into a DataFrame.

        Shared engine for two callers:

        * ``BaseVariable.load(as_df=True)`` — ``layout="packed"``, one row per
          record, data stored in a ``"data"`` column.
        * ``foreach._convert_inputs`` — ``layout="spread"``, DataFrame-mode variables
          produce one output row per inner-table row; data columns are spread across
          top-level columns rather than nested.

        Fast path
        ---------
        Fetches records and data in bulk SQL queries and assembles the output with
        vectorised pandas operations.  Applies when all of the following hold:

        * ``variable_class.__init__`` is not overridden (standard construction).
        * ``variable_class`` does not override ``from_db`` (no custom deserialisation).
        * ``dtype_meta`` does not have ``"custom": True`` (native storage only).
        * ``dtype_meta`` does not have ``"nested": True`` (flat multi-column only).

        Fall-back
        ---------
        Any violated condition routes to ``_load_as_df_via_iterator``, which uses the
        ``db.load()`` iterator and assembles row-by-row.  Output shape is identical.

        Parameters
        ----------
        variable_class :
            BaseVariable subclass to load.
        metadata :
            Optional flat metadata dict for filtering (default: load all records).
        layout :
            ``"packed"`` — one output row per record; ``"spread"`` — data columns
            spread, multiple rows per record for DataFrame-mode variables.
        include_rid :
            Add ``"__record_id"`` column to the output.
        include_bp :
            Add ``"__branch_params"`` (JSON string) column to the output.
        stringify_schema :
            Keep schema key values as strings (foreach uses this; notebook users
            generally do not).
        version_id :
            ``"latest"`` or ``"all"`` — passed to ``_find_record``.
        where :
            Optional Filter for restricting records.
        branch_params_filter :
            Optional branch_params key/value filter dict.

        Returns
        -------
        pd.DataFrame
            Empty DataFrame when no matching records exist.
        """
        metadata = self.canonicalize_metadata(metadata or {})
        type_name = variable_class.__name__
        table_name = type_name + "_data"

        # Look up dtype metadata — needed for dispatch and deserialisation.
        dtype_rows = self._duck.fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [type_name],
        )
        if not dtype_rows:
            return pd.DataFrame()
        dtype_meta = json.loads(dtype_rows[0][0])

        # -- Dispatch: fast path or iterator fallback? --
        # Class-introspection bailouts
        if variable_class.__init__ is not BaseVariable.__init__:
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        if self._has_custom_serialization(variable_class):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        # Storage-mode bailouts
        if dtype_meta.get("custom"):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        if dtype_meta.get("nested"):
            return self._load_as_df_via_iterator(
                variable_class,
                metadata,
                layout=layout,
                include_rid=include_rid,
                include_bp=include_bp,
                stringify_schema=stringify_schema,
                version_id=version_id,
                where=where,
                branch_params_filter=branch_params_filter,
            )

        # -- Fast path --
        t0 = time.perf_counter()
        if where is not None:
            try:
                records = self._load_with_where(
                    variable_class,
                    metadata,
                    table_name,
                    where,
                    version_id=version_id,
                )
            except Exception as _exc:
                Log.debug(
                    f"[load_all_as_df] {type_name}: _load_with_where raised "
                    f"{type(_exc).__name__}: {_exc} — returning empty DataFrame"
                )
                return pd.DataFrame()
            # where= and branch_params_filter (Variant) coexist: apply the
            # branch_params filter as a post-step on the where-matched records.
            if branch_params_filter:
                _n_before = len(records)
                records = _filter_records_by_branch_params(
                    records, branch_params_filter, self._duck
                )
                Log.debug(
                    f"[load_all_as_df] {type_name}: branch_params_filter "
                    f"{branch_params_filter} kept {len(records)}/{_n_before} records"
                )
        else:
            nested_metadata = self._split_metadata(metadata)
            records = self._find_record(
                type_name,
                nested_metadata=nested_metadata,
                version_id=version_id,
                branch_params_filter=branch_params_filter,
            )

        if len(records) == 0:
            return pd.DataFrame()

        # Bulk-fetch data rows in chunks to avoid very long IN clauses.
        all_record_ids = records["record_id"].tolist()
        if not dtype_meta.get("columns"):
            return pd.DataFrame()
        # Stored columns: datetime zone companions included, folded back by
        # merge_tz_columns below (owner sciduckdb.datetimes).
        data_cols = storage_column_names(dtype_meta)

        data_select = ", ".join(f'"{c}"' for c in data_cols)
        chunk_size = 500
        chunks: list = []
        t_sql = 0.0
        for start in range(0, len(all_record_ids), chunk_size):
            chunk = all_record_ids[start : start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))
            sql = (
                f'SELECT record_id, {data_select} FROM "{table_name}" '
                f"WHERE record_id IN ({placeholders})"
            )
            _t = time.perf_counter()
            chunk_df = self._duck.fetchdf(sql, chunk)
            t_sql += time.perf_counter() - _t
            if not chunk_df.empty:
                chunks.append(chunk_df)

        if not chunks:
            return pd.DataFrame()

        data_df = (
            pd.concat(chunks, ignore_index=True)
            if len(chunks) > 1
            else chunks[0].copy()
        )
        merge_tz_columns(data_df, dtype_meta, where=f"load_all_as_df({type_name})")

        result = self._assemble_df_from_records_and_data(
            records,
            data_df,
            dtype_meta,
            variable_class,
            layout=layout,
            include_rid=include_rid,
            include_bp=include_bp,
            stringify_schema=stringify_schema,
        )
        t_total = time.perf_counter() - t0
        Log.info(
            f"[timing] load_all_as_df({type_name}): {len(records)} records, "
            f"layout={layout!r}, sql={t_sql:.3f}s, total={t_total:.3f}s"
        )
        return result

    def load(
        self: "DatabaseManager",
        variable_class: type[BaseVariable],
        metadata: dict,
        version_id: str = "all",
        where=None,
        branch_params_filter: dict | None = None,
    ):
        """
        Load variables matching the given metadata as a generator.

        Args:
            variable_class: The type to load
            metadata: Flat metadata dict
            version_id: Which versions to return:
                - "all" (default): return every version
                - "latest": return only the latest version per (schema_id, variant)
                - any other string: treated as a specific record_id
            where: Optional Filter selecting the computation *variant* by the
                schema_id set its variable-level portion resolves to (semantic
                matching by consumed inputs); any SchemaKey portion further
                restricts rows. See ``_load_with_where``.
            branch_params_filter: Optional dict of branch_params key/value filters.

        Yields:
            BaseVariable instances matching the metadata
        """
        metadata = self.canonicalize_metadata(metadata)
        type_name = variable_class.__name__
        user_summary = {k: v for k, v in metadata.items() if not k.startswith("__")}
        Log.debug(f"load({type_name}): metadata={user_summary}")
        _t_load_all_total = time.perf_counter()
        table_name = self._ensure_registered(variable_class, auto_register=False)

        _t_find = time.perf_counter()

        # Specific record_id: bypass normal filtering
        if version_id not in ("all", "latest") and version_id is not None:
            records = self._find_record(
                variable_class.__name__,
                record_id=version_id,
                include_excluded=True,
            )
            if len(records) == 0:
                raise NotFoundError(f"No data found with record_id '{version_id}'")
        elif where is not None:
            # where= specified: semantic variant match + row restriction
            try:
                records = self._load_with_where(
                    variable_class,
                    metadata,
                    table_name,
                    where,
                    version_id=version_id,
                )
            except NotFoundError:
                Log.info(f"load({type_name}): no records found")
                if not self._any_records_exist(type_name):
                    raise NotFoundError(
                        f"Variable type '{type_name}' is registered but has no saved records in this database."
                    )
                return
            # where= and branch_params_filter (Variant) coexist: apply the
            # branch_params filter as a post-step on the where-matched records.
            if branch_params_filter:
                _n_before = len(records)
                records = _filter_records_by_branch_params(
                    records, branch_params_filter, self._duck
                )
                Log.debug(
                    f"load({type_name}): branch_params_filter {branch_params_filter} "
                    f"kept {len(records)}/{_n_before} records"
                )
                if len(records) == 0:
                    return
        else:
            nested_metadata = self._split_metadata(metadata)
            try:
                records = self._find_record(
                    variable_class.__name__,
                    nested_metadata=nested_metadata,
                    version_id=version_id,
                    branch_params_filter=branch_params_filter,
                )
            except NotFoundError:
                Log.info(f"load({type_name}): no records found")
                return  # No data

            if len(records) == 0:
                Log.info(f"load({type_name}): no records found")
                if not self._any_records_exist(type_name):
                    raise NotFoundError(
                        f"Variable type '{type_name}' is registered but has no saved records in this database."
                    )
                return
        t_find = time.perf_counter() - _t_find

        Log.debug(f"load({type_name}): found {len(records)} record(s)")

        # --- Bulk loading path ---

        # 1. Get dtype from _variables (one row per variable)
        _t_dtype = time.perf_counter()
        dtype_rows = self._duck.fetchall(
            "SELECT dtype FROM _variables WHERE variable_name = ?",
            [variable_class.__name__],
        )
        if not dtype_rows:
            return
        dtype_meta = json.loads(dtype_rows[0][0])
        is_custom = dtype_meta.get("custom", False)
        t_dtype = time.perf_counter() - _t_dtype

        # 2. Collect all unique record_ids to fetch
        all_record_ids = records["record_id"].tolist()
        if not all_record_ids:
            return

        # 3. Batch fetch data rows by record_id
        data_lookup: dict[str, Any] = {}  # record_id -> deserialized value

        chunk_size = 500
        t_chunk_sql = 0.0
        t_chunk_deserialize = 0.0
        _t_chunks_total = time.perf_counter()
        for start in range(0, len(all_record_ids), chunk_size):
            chunk = all_record_ids[start : start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))

            if is_custom:
                # Custom (columnar) path: fetch all rows for this chunk
                sql = (
                    f'SELECT * FROM "{table_name}" WHERE record_id IN ({placeholders})'
                )
                _t = time.perf_counter()
                chunk_df = self._duck.fetchdf(sql, chunk)
                t_chunk_sql += time.perf_counter() - _t

                if len(chunk_df) > 0:
                    _t = time.perf_counter()
                    grouped = chunk_df.groupby("record_id", sort=False)
                    for rid, sub_df in grouped:
                        sub_df = sub_df.drop(
                            columns=["record_id"], errors="ignore"
                        ).reset_index(drop=True)
                        data_lookup[rid] = self._deserialize_custom_subdf(
                            variable_class,
                            sub_df,
                            dtype_meta,
                        )
                    t_chunk_deserialize += time.perf_counter() - _t
            else:
                # Native path. Stored columns are selected (datetime zone
                # companions included) and folded back right after the fetch.
                data_cols = list(dtype_meta.get("columns", {}).keys())
                data_select = ", ".join(
                    f'"{c}"' for c in storage_column_names(dtype_meta)
                )
                sql = (
                    f'SELECT record_id, {data_select} FROM "{table_name}" '
                    f"WHERE record_id IN ({placeholders})"
                )
                _t = time.perf_counter()
                chunk_df = self._duck.fetchdf(sql, chunk)
                t_chunk_sql += time.perf_counter() - _t
                merge_tz_columns(chunk_df, dtype_meta, where=f"load({type_name})")

                if len(chunk_df) > 0:
                    _t = time.perf_counter()
                    mode = dtype_meta.get("mode", "single_column")
                    columns_meta = dtype_meta.get("columns", {})

                    if mode == "dataframe":
                        # One DuckDB row per DataFrame row: group by record_id.
                        df_columns = dtype_meta.get(
                            "df_columns", list(columns_meta.keys())
                        )
                        for rid, group_df in chunk_df.groupby("record_id", sort=False):
                            group_df = group_df.drop(
                                columns=["record_id"], errors="ignore"
                            ).reset_index(drop=True)
                            result = {}
                            for c, meta in columns_meta.items():
                                if c in group_df.columns and is_datetime_meta(meta):
                                    # Restored by merge_tz_columns; the Series
                                    # keeps its dtype (unit, zone), a list
                                    # would not — and the hash includes it.
                                    result[c] = group_df[c]
                                elif c in group_df.columns:
                                    # Optimized: use tolist() instead of iloc[i] (5-10x faster)
                                    result[c] = [
                                        storage_to_python(val, meta)
                                        for val in group_df[c].tolist()
                                    ]
                            data_lookup[rid] = pd.DataFrame(result, columns=df_columns)
                    else:
                        # Non-DataFrame: restore types, then one value per record_id row.
                        restored = chunk_df[data_cols].copy()
                        restored = restore_types(restored, dtype_meta)

                        if mode == "single_column":
                            col_name = next(iter(columns_meta))
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = restored[col_name].iloc[i]
                        elif mode == "multi_column":
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                result = {}
                                for c, meta in columns_meta.items():
                                    result[c] = storage_to_python(
                                        restored[c].iloc[i], meta
                                    )
                                if dtype_meta.get("nested"):
                                    data_lookup[rid] = unflatten_dict(
                                        result, dtype_meta["path_map"]
                                    )
                                else:
                                    data_lookup[rid] = result
                        else:
                            col_names = list(columns_meta.keys())
                            for i, rid in enumerate(chunk_df["record_id"].tolist()):
                                data_lookup[rid] = {
                                    c: restored[c].iloc[i] for c in col_names
                                }
                    t_chunk_deserialize += time.perf_counter() - _t

        t_chunks_total = time.perf_counter() - _t_chunks_total
        n_chunks = (len(all_record_ids) + chunk_size - 1) // chunk_size
        _mode_str = "custom" if is_custom else dtype_meta.get("mode", "single_column")
        Log.info(
            f"[timing] load({type_name}): pre-yield setup: "
            f"find={t_find:.3f}s, dtype_lookup={t_dtype:.3f}s, "
            f"chunks_total={t_chunks_total:.3f}s "
            f"(sql={t_chunk_sql:.3f}s, deserialize={t_chunk_deserialize:.3f}s, "
            f"n_chunks={n_chunks}, mode={_mode_str})"
        )

        # 4. Construct instances using itertuples + inline metadata.
        # This is a generator: per-yield body cost is measured cumulatively
        # and emitted in a TOTAL summary after the last yield.
        schema_keys = self.dataset_schema_keys
        n_yielded = 0
        t_yield_body = 0.0
        for row in records.itertuples(index=False):
            _t_body = time.perf_counter()
            record_id = row.record_id

            if record_id not in data_lookup:
                t_yield_body += time.perf_counter() - _t_body
                continue

            data_value = data_lookup[record_id]
            content_hash = row.content_hash

            flat_metadata = {}
            for sk in schema_keys:
                val = getattr(row, sk, None)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    flat_metadata[sk] = self.restore_schema_value(sk, val)

            # branch_params + direct-save kwargs derived from the bipartite graph
            # (§6), not a column. __save__.<kwarg> entries are the record's
            # non-schema save metadata.
            try:

                bp = provenance_query.derived_branch_params(self._duck, record_id)
            except Exception:
                bp = {}
            for k, v in bp.items():
                if k.startswith("__save__."):
                    flat_metadata[k[len("__save__.") :]] = v

            instance = variable_class(data_value)
            instance.record_id = record_id
            instance.metadata = flat_metadata
            instance.content_hash = content_hash
            instance.branch_params = bp

            n_yielded += 1
            t_yield_body += time.perf_counter() - _t_body
            yield instance

        t_total = time.perf_counter() - _t_load_all_total
        _caller_overhead = t_total - t_find - t_chunks_total - t_yield_body - t_dtype
        Log.info(
            f"[timing] load({type_name}): TOTAL={t_total:.3f}s "
            f"(find={t_find:.3f}s, chunks={t_chunks_total:.3f}s, "
            f"yield_body={t_yield_body:.3f}s for {n_yielded} records, "
            f"caller_overhead~={_caller_overhead:.3f}s)"
        )
