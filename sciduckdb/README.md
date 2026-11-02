# SciDuck

The DuckDB layer under `scidb`. It owns three things: the **codec** that turns
Python values into queryable DuckDB columns and back, a **locked connection**
that is safe to share across threads, and the **`_schema` table** of
schema-location ids. `scidb` owns everything else (records, provenance, which
tables exist); application code reaches SciDuck only through `scidb`.

All data, arrays included, is stored in queryable DuckDB types (LIST, nested
LIST, JSON), so the database can be inspected with DBeaver or any
DuckDB-compatible viewer.

## Codec

```python
from sciduckdb import infer_data_columns, value_to_storage_row, restore_types, storage_to_python

col_types, dtype_meta = infer_data_columns(value)   # {column: DuckDB type}, restore metadata
row = value_to_storage_row(value, dtype_meta)        # one row of storage values
# ... after reading the rows back into a DataFrame df:
df = restore_types(df, dtype_meta)                   # Python types restored per column
```

- Scalars, numpy arrays (any dtype and shape), lists, and dicts (flat or
  nested; nested keys become dot-separated columns) store one row per value.
- A DataFrame value stores one DuckDB row per DataFrame row
  (`dataframe_to_storage_rows`), one column per DataFrame column.
- `dtype_meta` records what each column was, so a load is lossless: dtypes,
  shapes and NULL list elements (restored as NaN) survive the round trip.

## Connection

`SciDuck(path, dataset_schema=[...])` opens the file. Every statement runs
under one lock, so a concurrent `close()` cannot land mid-statement. Group
paired work with `with duck.transaction("label"):`; it is re-entrant on the
owning thread and rolls back on any exception.

## Schema ids

`batch_get_or_create_schema_ids` maps schema locations
(`{"subject": "S01", "session": "A"}`) to integer `schema_id`s in `_schema`,
creating missing rows. A key left out is NULL in its row, so non-contiguous
key subsets are distinct locations. All schema key values are stored as
strings.

Variables can also be organised into named groups (`add_to_group`,
`get_group`, `list_groups`, `remove_from_group`).

## Datetimes

A datetime value (`datetime.datetime`, `pd.Timestamp`, `np.datetime64`, NaT) is
stored as two columns: `<col>` `TIMESTAMP_NS` holding the UTC instant (or a
naive value's wall clock), and `<col>__tz` `VARCHAR` holding that value's zone
(`"Europe/Berlin"`, `"UTC"`, `"+02:00"`; NULL = naive). Zones are per value, so
one column may hold several zones and naive values. Loads give back each value
in its own zone. The `__tz` column exists from the column's first save and is
never listed in the variable's data columns. In raw SQL, filter on
`<col>__tz IS NULL` to tell naive rows apart. Owner: `sciduckdb.datetimes`;
design: `docs/claude/datetime-roundtrip.md`.
