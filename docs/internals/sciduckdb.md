# sciduckdb

/// info | Internal package
You reach storage through [`scidb`](../api/scidb.md) (`BaseVariable.save` / `load`). `sciduckdb` is the layer underneath: it owns the codec, the connection and the schema-location ids; `scidb` owns records and provenance.
///

Each variable lives in its own DuckDB table, and every location is a row in `_schema`, addressed by a hierarchical schema such as `subject → session → trial`.

All data is stored as ordinary DuckDB types, including arrays (as `LIST`, nested `LIST` or `JSON`). You can therefore open the file in DBeaver, the `duckdb` CLI, or any other DuckDB client and query it directly. Type information is recorded alongside the data, so values load back as the type they were saved as.

## SciDuck

```python
from sciduckdb import SciDuck

duck = SciDuck("data.duckdb", dataset_schema=["subject", "session"])
with duck.transaction("import"):
    ids = duck.batch_get_or_create_schema_ids(
        {("session", ("S01", "1")): {"subject": "S01", "session": 1}}
    )
```

| Method | Purpose |
|---|---|
| `SciDuck(path, dataset_schema, read_only=False)` | Open or create a database. |
| `transaction(label)` | One transaction around a `with` block; re-entrant, rolls back on any exception. |
| `batch_get_or_create_schema_ids(combos)` | Schema-location ids, creating missing `_schema` rows. |
| `list_variables()` | The registered variables. |
| `add_to_group`, `remove_from_group`, `list_groups`, `get_group` | Organise variables into named groups. |
| `distinct_schema_values(key)` / `distinct_schema_combinations(keys)` | Which locations hold data. |
| `query(sql)` → DataFrame, `fetchall(sql)`, `fetchone(sql)` | Run SQL against the connection (thread-safe). |
| `close()` / `reopen()` / `is_open` | Connection lifetime. |

`schema_keys_from_db(path)` reads the schema of an existing file without opening it for writing.

**Codec.** `infer_data_columns`, `value_to_storage_row`, `dataframe_to_storage_rows`, `restore_types`, `storage_to_python`, `array_from_storage` and related functions convert between Python values and stored rows. `scidb` uses them, and they are public for tools that read the tables directly.

Schema key values are stored as strings. `scidb`'s `schema_key_types={"trial": "numeric"}` is what makes `"001"` and `1` the same trial.
