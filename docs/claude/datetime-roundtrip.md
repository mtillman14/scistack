# Datetime round-trip (naive, tz-aware, NaT)

Built 2026-10-22. Plan: `.claude/plan-datetime-roundtrip.md`. Tests:
`scicanonicalhash/tests/test_datetimes.py`,
`sciduckdb/tests/test_datetime_storage.py`,
`scidb/tests/test_datetime_roundtrip.py`.

## Why

`When.save(pd.Timestamp(...), subject="S01")` raised
`ValueError: Unserializable data type: <class 'pandas.Timestamp'>`. The
record_id hash had no datetime branch, so no datetime scalar could be saved.
Behind that: `np.datetime64` scalars fell to `json_fallback`, aware values lost
their zone in a plain `TIMESTAMP` column, and `_storage_signature` lumped every
temporal type into one category.

## Decisions (user, 2026-10-22)

| # | Decision |
|---|---|
| D1 | `datetime.datetime`, `pd.Timestamp`, `np.datetime64` (any unit) holding the same value hash the same; NaT has one spelling |
| D2 | Keep tz-awareness: no forced UTC conversion, no refusal |
| D3 | Zones are **per value**, not per column |
| D4 | Every datetime column has a `<col>__tz` companion **from its first save**, even all NULL. NULL zone = naive. Naive and aware may mix |
| D5 | The instant is `TIMESTAMP_NS` holding UTC, **not** `TIMESTAMPTZ`. TIMESTAMPTZ keeps only µs, so pandas ns values would be truncated and hash differently after their load |

Why D4: the first save fixes a variable's column set (no schema evolution), so
a companion that first appeared with an aware value would be a
`ColumnSetChangedError`.

## Storage

| column | type | naive value | aware value | NaT |
|---|---|---|---|---|
| `<col>` | `TIMESTAMP_NS` | wall clock | UTC instant | NULL |
| `<col>__tz` | `VARCHAR` | NULL | zone label | NULL |

Values are written as ISO text with 9 fractional digits; DuckDB casts the text to
`TIMESTAMP_NS` on insert, with no digit lost. A `TIMESTAMP_NS` is never
converted through the session `TimeZone`, so no ICU and no machine dependence.
The range is 1677-09-21 to 2262-04-11; values outside it raise
`DatetimeStorageError`.

**Raw-SQL caveat:** a naive row's instant looks like a UTC instant. Filter
on `<col>__tz IS NULL` to tell the two apart.

## Owners

- **`scicanonicalhash.datetimes`**: zone label + hash bytes. They must agree:
  a value hashes the same before its save and after its load, so re-saving a
  loaded value is "0 new rows".
  - `zone_label(v)`: `None` (naive / NaT), IANA name (zoneinfo `key`, pytz
    `zone`), else this value's offset `"+02:00"`; an unnamed zero offset is
    `"UTC"`.
  - `zone_from_label(label)`: the inverse (`UTC` -> `timezone.utc`, offset ->
    `timezone(td)`, name -> `zoneinfo.ZoneInfo`, falling back to pytz).
  - Hash bytes: `datetime:<ns>` (naive), `datetimetz:<utc ns>:<label>`
    (aware), `datetime:NaT`. `datetime64` **arrays** keep their raw-bytes form
    (pinned by `test_hash_byte_identity.py`); only tz-aware DataFrame frames
    (object Timestamps) newly hash.
- **`sciduckdb.datetimes`**: the companion.
  - `storage_column_types(col_types, meta)`: data columns -> stored columns
    (companion right after its column). `infer_data_columns` returns this, so
    every DDL/INSERT column list has it. Raises on a `t` + `t__tz` collision.
  - `storage_column_names(meta)`: what readers SELECT.
  - `datetime_to_storage` / `datetime_series_to_storage`: one cell / column ->
    `(instant text, label)`. Used by `value_to_storage_row`,
    `dataframe_to_storage_rows`, `bulk_df_to_storage_rows`.
  - `merge_tz_columns(df, meta)`: THE load-side fold, called right after the
    fetch in `SciDuck.load`, scidb `load_all_as_df` and `load`. One zone ->
    `datetime64[ns, tz]`; several zones or naive+aware -> object column of
    Timestamps; none -> `datetime64[ns]`.
  - `stored_tz_companions(stored_types)`: recognises a companion from the bare
    table schema (`VARCHAR <x>__tz` beside `TIMESTAMP_NS <x>`). Used by the
    Inspector's `data_columns`, the plot loader's `data_column_types_for` and
    `_check_column_set`.

`dtype_meta["columns"]` **never lists the companion.** Every consumer that
lists a variable's data columns (GUI, `for_columns`, `as_table`) stays
unaware of it.

## scidb column rules

- `_check_column_set`: companions (this save's and the stored ones) are left
  out of the column-set comparison. Datetime vs numbers is a TYPE conflict.
- `_reconcile_column_types`: judges data columns only (`user_column_types`).
  A stored still-empty column that receives datetimes is altered to
  `TIMESTAMP_NS`, and its companion is `ADD COLUMN`ed (INFO line). Returns
  `storage_column_types(merged)`.
- `_storage_signature`: each temporal type is its own category
  (`temporal:TIMESTAMP_NS`, ...): TIMESTAMP vs TIMESTAMP_NS is refused.

## Logging

- Bulk DataFrame save: DEBUG `datetime column(s) stored as UTC TIMESTAMP_NS +
  zone column: {col: {naive, aware, missing}}`. It is INFO when a column mixes
  naive and aware values.
- Load: DEBUG per datetime column (rows, aware count, zones, NaT, dtype).
  WARNING when a datetime column has no companion in the fetch.

## Known limits / follow-ups

- **Tables saved before this change** (a `TIMESTAMP` column, no companion):
  loads read naive, with a WARNING. A new save into one is refused
  (TIMESTAMP vs TIMESTAMP_NS). Beta policy: no migration; delete and re-run.
- `datetime64` **arrays inside one cell** (`TIMESTAMP[]`) are unchanged and
  have no zones.
- Custom `to_db()` frames keep their own column typing (datetime -> VARCHAR).
- Plots: covered (docs/claude/plot-datetimes.md): the plot loader folds the
  zone back, and `PlotSpec.time_zone` chooses local time or one zone.
- MATLAB `datetime`: covered (section "MATLAB" below).
- The column's datetime64 **unit** comes from its first save (`datetime_meta(unit)`).
  A later save in another unit loads in the first one, so those records hash
  differently on reload.

## Unit restoration (fixed 2026-10-22, first test run)

TIMESTAMP_NS fetches as `datetime64[ns]`, but pandas defaults to `[us]` on
some versions, and the DataFrame hash includes the dtype. So a `[us]` column
loaded as `[ns]` made re-saves new records. `_batch_column_type` now records
`"unit"` in a datetime64 column's metadata; `merge_tz_columns` restores it
(`Series.dt.as_unit`). The per-record DataFrame loaders (`SciDuck.load`,
scidb `load`) keep the restored Series instead of rebuilding a list, which
would re-infer the dtype. Scalars hash unit-free, so they need none of this.

## MATLAB (built 2026-10-22, second pass)

A MATLAB datetime array has ONE `TimeZone` (`''` = unzoned = naive). It
crosses as **int64 ns from 1970-01-01** plus a NaT mask plus that TimeZone:

| direction | MATLAB owner | Python owner |
|---|---|---|
| MATLAB -> Python | `+scidb/+internal/datetime_to_python.m` (`convertTo(..., 'epochtime', 'TicksPerSecond', 1e9)`: exact; `posixtime` is a double, ~0.2 us) | `scimatlab.bridge.datetimes_from_matlab(ns, nat, tz, kind)`: `scalar` -> Timestamp/NaT, `column` -> datetime64 Series, `list` -> one Timestamp per record |
| Python -> MATLAB | `+scidb/+internal/datetime_from_python.m` (`datetime(ns, 'ConvertFrom', 'epochtime', ...)`, then `.TimeZone = tz`) | `scimatlab.bridge.datetimes_to_matlab(obj)`: ticks as **raw `<i8` bytes** (`ns_bytes`), because a numeric array through `from_python` becomes double and ~1.8e18 ns is not exact in a double |

- Ticks are the wall clock when unzoned, else the UTC instant (the epoch is
  `datetime(1970,1,1)` or `datetime(1970,1,1,'TimeZone','UTC')`).
- `matlab_zone_label(tz)` maps MATLAB spellings onto the scistack label:
  `''` -> naive, `UTC`/`Z`/`UTCLeapSeconds` -> `"UTC"` (leap-second values
  are converted to UTC first, WARN), `+0200`/`+02` -> `"+02:00"`; an IANA name
  stays itself, and an unknown name raises.
- Sites: `to_python.m` scalars + Nx1 table columns; `BaseVariable.save_from_table`
  datetime data column (`kind='list'`, one record per row); `from_python.m`
  scalar branch, plus a fallback through `bridge.is_datetime_value` when
  `isa` misses a Cython subclass, plus `convert_dataframe`'s datetime64
  branch. `save_batch_bridge` never `.item()`s a datetime; `numpy_scalar_to_python`
  never unwraps a `datetime64` (its `.item()` is a bare int).
- Values holding several zones (an object column) cannot be one MATLAB
  array: `datetimes_to_matlab` returns ok=false and `convert_dataframe`'s
  object branch converts them one at a time (a cell column of datetimes).
- Out of scope (user, 2026-10-22): a datetime ARRAY saved as ONE value keeps
  the old ISO-text form, and WARNs when it is zoned. Use a `to_db`/`from_db`
  class. N x M datetime table variables also stay text.
- A loaded MATLAB datetime has `Format` `yyyy-MM-dd HH:mm:ss.SSSSSSSSS`;
  the original `Format` is not stored.

Tests: `scimatlab/tests/test_bridge_datetimes.py` (Python, no MATLAB) and
`scimatlab/tests/matlab/scidb/TestDatetimeRoundTrip.m`.
