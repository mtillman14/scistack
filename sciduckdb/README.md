# SciDuck

A thin DuckDB layer for managing versioned scientific data.

Each variable is stored in its own table. Variables are associated with a hierarchical dataset schema (e.g. subject -> session -> trial) and can be saved at any level of that hierarchy. Multiple versions of each variable are supported natively.

All data -- including arrays -- is stored in queryable DuckDB types (LIST, nested LIST, JSON) so the database can be inspected with DBeaver or any DuckDB-compatible viewer.

## Usage

```python
from sciduckdb import SciDuck

duck = SciDuck("data.duckdb", dataset_schema=["subject", "session"])
duck.save("MyVar", data, subject="S01", session=1)
loaded = duck.load("MyVar", subject="S01", session=1)
```

## Features

- **Three save modes**: DataFrame with schema columns (Mode A), single entry via kwargs (Mode B), or dict mapping tuples to values (Mode C)
- **Automatic type inference**: Maps Python/numpy types to DuckDB types
- **Round-trip restoration**: Metadata tracks original types for lossless load
- **Version management**: Automatic version numbering, duplicate hash detection
- **Variable groups**: Organize variables into named groups
- **Schema validation**: Validates dataset schema consistency across sessions

Note: all schema key values are coerced to strings before storage.

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
