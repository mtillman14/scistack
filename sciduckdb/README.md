# SciDuck

The DuckDB layer under `scidb`. It owns two things: the **codec** that turns
Python values into queryable DuckDB columns and back, and a **locked
connection** that is safe to share across threads. It creates no tables:
`scidb` owns every table of a project database (`scidb.tables`) and the
schema-location ids (`scidb.sciduck.SciDuck`, moved out of sciduckdb in P3).
Application code reaches the database only through `scidb`.

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

`DuckConnection(path)` opens the file. Every statement runs
under one lock, so a concurrent `close()` cannot land mid-statement. Group
paired work with `with duck.transaction("label"):`; it is re-entrant on the
owning thread and rolls back on any exception.

The supported interface of a connection (`DuckConnection`, which scidb's
`SciDuck` extends). Other packages call these and nothing with a leading underscore:

| Method | Does |
|---|---|
| `transaction(label)` | context manager: one transaction around the block |
| `execute(sql, params)` | a statement that returns no rows; returns `None` |
| `fetchall` / `fetchone` | rows as tuples (few rows) / the first row or `None` |
| `fetchdf` | rows as a DataFrame (many rows; NULL floats become NaN) |
| `fetch_table` | `(columns, rows)` with NULL kept as `None` (diagnostics) |
| `fetchdf_if_open` | `fetchdf`, or `None` when the connection is closed |
| `table_exists(name)` | whether a table or view exists |
| `bulk_insert` / `bulk_update` | vectorized multi-row writes (one statement) |
| `add_rollback_listener(fn)` | call `fn()` after every outermost ROLLBACK |
| `query(sql)` | `fetchdf` under its user-facing name |
| `is_open`, `close()`, `reopen()` | connection lifetime; also a context manager |

Every fetch holds the lock across execute *and* fetch. `execute` returns
nothing so a result can never be fetched after the lock is released.

Modules: `codec.py` (conversion functions), `connection.py`
(`DuckConnection`), `datetimes.py`, `row_lists.py`. Import from `sciduckdb`, not from the modules.

## Schema ids

Moved to scidb (`scidb.sciduck.SciDuck`): `_schema`, `_variables` and
`_variable_groups` are project tables like any other.

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
