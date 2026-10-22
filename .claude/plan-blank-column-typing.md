# Plan: blank DataFrame columns stay untyped until values arrive

Origin: scidb.log 2026-10-22 — `pandas.read_csv` of a CERNER pull,
IP_SPINAL_STIMULATION blank for the first subject(s), then 'Y'. Column typed
DOUBLE from NaN; save failed. Already fixed (uncommitted): error masking
(`SciDuck._unregister_frame`) and cross-record search (`more_frames`).

## Goal (agreed with user)

- A column whose values are ALL empty (NULL / NaN / None / NaT) has no
  decided type.
- "All" = every record of one save: one run -> one `save_batch` per output,
  covering every iteration and every schema level (all subjects, sessions...).
  Never judged per iteration.
- The first save that brings non-empty values decides the type, once:
  all numeric -> DOUBLE (BIGINT if every value is an integer of an int column,
  as today); any string (all strings, or strings mixed with numbers) -> VARCHAR.
- An existing column that is still all empty is converted in place
  (`ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE ... USING NULL`) — exact,
  because it holds only empties. Old rows load back as missing.
- A decided column contradicted by a later save is REFUSED with a clear error
  (user decision (a)): variable, column, stored type, incoming type, sample
  offending values, the record(s) they came from. Nothing is written.
- An all-empty incoming column into a decided stored column is fine: stores
  empties, the stored type stands.

## Owners

1. **Batch typing — sciduckdb `infer_data_columns(first, more_frames=...)`**
   (DataFrame mode). Per column, look at the column across the WHOLE batch
   (one concat, vectorised `pd.api.types.infer_dtype(skipna=True)`), replacing
   the row-0 / first-non-missing pick:
   - `empty` -> VARCHAR placeholder, col meta `{"python_type": "str",
     "empty": True}` (transient: describes THIS save, never trusted from storage)
   - integer/floating/mixed-integer-float -> BIGINT/DOUBLE as today
   - string / mixed-with-strings (scalar cells) -> VARCHAR
   - boolean, datetime, array/list/dict cells -> unchanged existing logic
   INFO log per column decided from a later record or mixed values.
2. **Storage — sciduckdb `_python_to_storage` / `bulk_df_to_storage_rows`**:
   missing -> None (true NULL) for every scalar column; VARCHAR columns `str()`
   non-missing non-string values (mixed batch). Prevents NaN becoming `'nan'`
   text in a VARCHAR column.
3. **Reconcile with the stored table — scidb, new
   `DatabaseManager._reconcile_column_types(table, variable_class,
   data_col_types, dtype_meta) -> (data_col_types, dtype_meta)`**, called right
   after `_check_column_set` on all three save paths (`database.py` ~1565,
   ~1719, ~1953). The stored DATA is the owner of "is this column still
   empty" — no persisted flag, so nothing can diverge and existing tables work:
   - same type category -> nothing
   - incoming empty -> keep stored type + stored col meta
   - type differs and stored column is all empty (one aggregate query, only run
     on a mismatch) -> ALTER to incoming type, INFO log
   - type differs and stored column has values -> raise new
     `ColumnTypeChangedError` (scidb.exceptions, sibling of
     ColumnSetChangedError), ERROR log
   Returns MERGED meta; that merged meta is what the `_variables.dtype` upsert
   writes (today the incoming meta overwrites it blindly).
4. **Load** — an all-NULL column (still undecided) loads as NaN (float), the
   `read_csv` convention.

## Decisions (user, 2026-10-22)

- D1: stored BIGINT, incoming non-integer DOUBLE -> WIDEN to DOUBLE.
- D2: stored VARCHAR, incoming numbers -> STORED AS TEXT (not refused).
  Integral floats as "3" not "3.0" (`_schema_str`).

## Status (implemented 2026-10-22, uncommitted, pytest unrun)

- sciduckdb: `_batch_column_type` + `_batch_frame` (replaced
  `_representative_cell`), `_EMPTY_COLUMN_META`, `_str_to_storage`,
  `column_type_change`, empty -> NaN in both load converters. Integers with
  blanks -> DOUBLE (avoids NaN into BIGINT).
- scidb: `stored_data_column_types`, `_stored_empty_columns`,
  `_reconcile_column_types` on `_save_native` + `save_batch` (custom/to_db
  columnar path NOT covered — it types from pandas dtype itself),
  `ColumnTypeChangedError` exported.
- Widen/convert ALTERs run before the save transaction; a later insert
  failure leaves the converted type (harmless: empty column, or ints->double).

## Logging (NOTE 2)

- INFO: column decided from record k / mixed values -> VARCHAR (sciduckdb)
- INFO: `[save] X: column C was empty in all N stored record(s); now TYPE`
- ERROR: ColumnTypeChangedError message
- DEBUG: per-save reconcile summary (columns checked, mismatches, query time)

## Tests (scidb/tests/test_dataframe_leading_missing_column.py, extend)

- batch: blank first record, 'Y' later -> VARCHAR (exists)
- batch: numbers in one record, strings in another -> VARCHAR, numbers as text
- batch: all blank -> placeholder, `empty` meta, round trip NaN
- two saves: blank then 'Y' -> ALTER to VARCHAR, old rows missing, new 'Y'
- two saves: blank then 2.5/3.0 -> DOUBLE, floats back
- two saves: 2.5 then 'N/A' -> ColumnTypeChangedError, nothing written,
  message names column + types
- two saves: 2.5 then blank -> stays DOUBLE, `_variables.dtype` unchanged
- VARCHAR column never stores the text 'nan'
- single `save()` path and Mode-A path hit the same reconcile (one each)
- D1 widen case

Run: `cd /workspace/scidb && python -m pytest tests/test_dataframe_leading_missing_column.py -v`
and `cd /workspace/sciduckdb && python -m pytest tests -q`
