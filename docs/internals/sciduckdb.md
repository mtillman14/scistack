# sciduckdb

/// info | Internal package
You normally reach storage through [`scidb`](../api/scidb.md) (`BaseVariable.save` / `load`). `sciduckdb` is the layer underneath, and it is also usable on its own when you want versioned, schema-addressed tables without provenance.
///

`sciduckdb` stores each variable in its own DuckDB table, addressed by a hierarchical schema such as `subject → session → trial`. A value can be saved at any level of that hierarchy. Every save is a new version, and earlier versions are kept.

All data is stored as ordinary DuckDB types, including arrays (as `LIST`, nested `LIST` or `JSON`). You can therefore open the file in DBeaver, the `duckdb` CLI, or any other DuckDB client and query it directly. Type information is recorded alongside the data, so values load back as the type they were saved as.

## SciDuck

```python
from sciduckdb import SciDuck

duck = SciDuck("data.duckdb", dataset_schema=["subject", "session"])
duck.save("MyVar", data, subject="S01", session=1)
df = duck.load("MyVar", subject="S01")
```

| Method | Purpose |
|---|---|
| `SciDuck(path, dataset_schema, read_only=False)` | Open or create a database. |
| `save(name, data, schema_level=None, **keys)` | Save in one of three shapes: a DataFrame with schema columns (many rows at once); one value plus `**keys`; or a dict mapping key tuples to values. Saving identical content again is detected. |
| `load(name, raw=True, **keys)` | Load matching rows. Keys you leave out match everything. |
| `list_variables()` / `list_versions(name)` / `delete(name)` | Catalogue and housekeeping. |
| `add_to_group`, `remove_from_group`, `list_groups`, `get_group` | Organise variables into named groups. |
| `distinct_schema_values(key)` / `distinct_schema_combinations(keys)` | Which locations hold data. |
| `query(sql)` → DataFrame, `fetchall(sql)`, `fetchone(sql)` | Run SQL against the connection (thread-safe). |
| `close()` / `reopen()` / `is_open` | Connection lifetime. |

`schema_keys_from_db(path)` reads the schema of an existing file without opening it for writing.

**Conversion helpers.** `storage_to_python`, `value_to_storage_row`, `dataframe_to_storage_rows`, `array_from_storage` and related functions convert between Python values and stored rows. `scidb` uses them, and they are public for tools that read the tables directly.

Schema key values are stored as strings. `scidb`'s `schema_key_types={"trial": "numeric"}` is what makes `"001"` and `1` the same trial.
