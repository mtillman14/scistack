# Plan: datetime values round-trip (naive, tz-aware, NaT)

## Problem

`When.save(pd.Timestamp(...), subject="S01")` raises
`ValueError: Unserializable data type: <class 'pandas.Timestamp'>` in
`scicanonicalhash._serialize_for_hash`: the record_id hash has no datetime
branch, so no scalar datetime can be saved. Reading the code turned up more
problems behind it:

- `np.datetime64` scalars fall to `json_fallback` in
  `sciduckdb._infer_duckdb_type` (json.dumps can't serialize them).
- tz-aware values go into a plain `TIMESTAMP` column, so the zone is lost.
- `sciduckdb._storage_signature` puts DATE/TIME/TIMESTAMP/INTERVAL in one
  `temporal` category, so `column_type_change` says "same" for TIMESTAMP vs
  TIMESTAMPTZ (DuckDB then casts silently, using the session zone) and even
  for TIMESTAMP vs INTERVAL.
- Nothing tests datetimes at all.

DataFrame columns of `datetime64[ns]` already hash fine (ndarray bytes; NaT is
int64 min), and they are expected to round-trip. That is unverified.

## Decisions

- D1 (user): datetime.datetime, pd.Timestamp and np.datetime64 holding the
  same value hash the same. NaT has one spelling.
- D2 (user): keep tz-awareness. Don't force UTC, don't refuse. The storage is
  TIMESTAMPTZ.
- D3 (user): zones are per VALUE, not per column. An aware column stores
  the UTC instant (TIMESTAMPTZ) plus a hidden companion VARCHAR column
  holding each value's zone as given ("Europe/Berlin", or "+02:00" for a
  fixed offset). Load rebuilds each value in its own zone. A uniform zone
  comes back as `datetime64[ns, tz]`; mixed zones come back as an object
  column of Timestamps (pandas can't hold mixed zones any other way).
- D4 (user): EVERY datetime column has a `<col>__tz` companion from its
  first save, even when it is all NULL. Reason: the first save fixes the
  column set (no schema evolution), so a companion added later would raise
  ColumnSetChangedError. The instant column is always TIMESTAMPTZ.
  - aware value: the UTC instant + its zone name / offset.
  - naive value: its wall clock stored as if it were UTC, zone NULL. NULL
    zone = naive. It loads back naive, with no shift (the session zone is
    pinned to UTC).
  - NaT: NULL in both columns.
  - Naive and aware mixed in one variable are ALLOWED, per value. An INFO
    log line counts each kind. Loading a column with both gives an object
    column (pandas can't mix them).
  - Raw-SQL caveat: a naive row's TIMESTAMPTZ value looks like a UTC instant.
    Filter on `__tz IS NULL` to tell them apart. Documented.
- D5 (user, 2026-10-22): the instant column is TIMESTAMP_NS holding UTC, NOT
  TIMESTAMPTZ. TIMESTAMPTZ keeps only microseconds, so pandas ns values would
  be truncated silently and get a new hash on reload. TIMESTAMP_NS has no
  session-zone conversion at all (no TimeZone pin and no ICU needed). Range
  1677-2262; a value outside it is refused with a clear error. In raw SQL it
  is a plain timestamp that you read as UTC.
- The companion's name has ONE owner (`tz_column_name(col)` + `is_tz_column`
  in sciduckdb). The column-set check, views, loads and `scidb sql/show` all
  call it. A user column already named `<x>__tz` next to a datetime column
  `<x>` -> refused with a clear error.

## Stages

### Stage 1: hash (scicanonicalhash, the owner)
- A datetime branch in `_serialize_for_hash`, placed BEFORE the generic
  fallbacks. Existing bytes don't change (the `test_hash_byte_identity.py`
  pin still holds); the branch only covers types that raise today.
- Naive: `b"datetime:" + ns-since-epoch` (wall clock).
  Aware: `b"datetimetz:" + utc-ns + b":" + zone name`. The zone is part of the
  hash because it is restored on load.
  NaT / `np.datetime64("NaT")`: `b"datetime:NaT"`.
- Object arrays holding Timestamps then hash too (they go through
  `.tolist()`).

### Stage 2: storage (sciduckdb). Superseded in part by D5: read TIMESTAMPTZ below as TIMESTAMP_NS (UTC), with no session-zone pin
- `_infer_duckdb_type`: np.datetime64 scalars -> TIMESTAMP. Aware values ->
  `TIMESTAMPTZ` plus the companion zone column. NaT -> NULL in both columns.
- The DataFrame path (`_batch_column_type`): a `datetime64[ns, tz]` column
  or an object column of aware Timestamps -> TIMESTAMPTZ plus the companion
  column.
- `storage_to_python` / `storage_to_python_column`: aware -> each value
  `tz_convert(its zone)`. NULL -> `pd.NaT`, never None.
- `_storage_signature`: each temporal type gets its own category, so naive
  into aware (and TIMESTAMP into INTERVAL) is refused, not silently cast.
  Owner stays `column_type_change`.

### Stage 3: logging
- Save: DEBUG line with the column type, zone and NaT count. Load: DEBUG line
  with the restored zone and NaT count. They sit at the seams that already
  log NULL counts.

### Stage 4: tests (user runs)
- scicanonicalhash: hash parity across the three types; NaT parity; aware
  values with different zones hash differently; existing bytes unchanged.
- sciduckdb/scidb: a scalar round-trip per type, plus a NaT level among
  valid ones, plus all NaT; a DataFrame datetime64 column with NaT; an aware
  value round-trips with its zone; naive into an aware column -> refused;
  load with a different session zone -> same values.
- "0 new rows" on re-saving a loaded value (record_id stable).

### Stage 5: docs
- `docs/claude/datetime-roundtrip.md`; update the Supported Types list in
  the scicanonicalhash README.

## Out of scope
- MATLAB `datetime` with TimeZone through the bridge (it goes Python -> ISO
  string today); a follow-up.

## Status (2026-10-22)
All stages implemented. Tests written, not run (the user runs them). Design doc: docs/claude/datetime-roundtrip.md.
