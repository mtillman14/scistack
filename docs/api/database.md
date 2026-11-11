# Database API

<!-- Ground truth (source/tests win over prose). Verified against scidb/src/scidb/store/database.py:
     configure_database(dataset_db_path, dataset_schema_keys) -> DatabaseManager (2 args, both
       required; auto-registers known subclasses; sets scifor schema; enables caching);
     get_database() -> DatabaseManager (DatabaseNotConfiguredError if not configured);
     using_db(db): context manager, the thread-entry owner (sets current db, restores previous);
     register(variable_class) (idempotent); list_versions(variable_class, include_excluded=False,
       **metadata) -> list[dict] keys record_id/schema/branch_params/timestamp (newest first);
     get_provenance(variable_class, version=None, **metadata) -> {function_name, function_hash,
       inputs, constants} | None; distinct_schema_values(key)->list;
     graph reads are scidb.provenance.query functions (upstream_provenance(db, rid),
       pipeline_structure(duck), has_producing_invocation(duck, rid), ...) or the Inspector;
       export_to_csv(variable_class, path, **metadata)->int; add_to_var_group/remove_from_var_group/
       list_var_groups/get_var_group;
     scidb/__init__ module-level exclude_schema/include_schema/list_exclusions.
     NOTE: configure_database has NO 3rd arg; list_versions keys are timestamp+branch_params
     (not created_at/version); caching is for @lineage_fcn, not @thunk. -->

The database handle (`DatabaseManager`) owns all storage operations. Create it once
with `configure_database()` and reach it anywhere with `get_database()`. For
task-oriented usage see [Database & Configuration](../guide/database.md).

---

## `configure_database()`

```python
configure_database(dataset_db_path, dataset_schema_keys) -> DatabaseManager
```

Opens the DuckDB database, declares the dataset schema, auto-registers all defined
`BaseVariable` subclasses, enables lineage caching, and returns the handle (also
set as the global default). Both arguments are required; there is no third
argument.

- **`dataset_db_path`** — path to the DuckDB file (data *and* lineage).
- **`dataset_schema_keys`** — list of metadata keys that identify a record's
  location; all other metadata is treated as version keys / `branch_params`.

=== "Python"
    ```python
    from scidb import configure_database
    db = configure_database("experiment.duckdb", ["subject", "session"])
    ```
=== "MATLAB"
    ```matlab
    db = scidb.configure_database("experiment.duckdb", ["subject" "session"]);
    ```

!!! tip
    Import `configure_database` from **`scihist`** instead to additionally register
    the database as the lineage cache backend (needed for cache hits). Same
    signature.

---

## `get_database()`

```python
get_database() -> DatabaseManager
```

Returns the global handle, or raises `DatabaseNotConfiguredError` if
`configure_database()` hasn't run.

The handle is per thread. A thread you start yourself has none until it enters
one with `using_db`.

---

## `using_db()`

```python
with using_db(db):
    ...  # get_database() returns db here
```

Makes `db` the current database for the code inside the block, on this thread.
When the block ends, whatever was current before is current again. Use it at
the start of any thread you create that loads or saves data.

```python
import threading
from scidb import configure_database, using_db

db = configure_database("experiment.duckdb", ["subject", "session"])

def work():
    with using_db(db):
        ...  # for_each, .load(), .save() all see db

threading.Thread(target=work).start()
```

---

## `register()`

```python
db.register(variable_class) -> None
```

Registers a variable type and creates its table if needed. Idempotent.
`configure_database` registers all defined subclasses already; call this only for a
class defined afterward (using a type also registers it).

---

## `list_versions()`

```python
db.list_versions(variable_class, include_excluded=False, **metadata) -> list[dict]
```

Every stored version at a location, **newest first**. Each entry has:

| Key | Meaning |
|---|---|
| `record_id` | the version's id |
| `schema` | the location (schema) keys |
| `branch_params` | the variant keys |
| `timestamp` | when it was saved |

Non-schema keyword arguments act as `branch_params` filters. With
`include_excluded=True`, each entry also carries an `excluded` bool.

---

## Provenance queries

```python
db.get_provenance(variable_class, version=None, **metadata) -> dict | None
```

- **`get_provenance`** — lineage of one record (latest matching, or a specific
  `version=`): `{function_name, function_hash, inputs, constants}`, or `None` if it
  was saved without lineage.

Everything else about the graph is read from `scidb.provenance.query` (or the
`Inspector`, `db.inspect`), not from the database handle:

```python
from scidb.provenance import query as provenance_query

provenance_query.upstream_provenance(db, record_id, max_depth=20) -> list[dict]
provenance_query.pipeline(db, record_id) -> {"nodes": [...], "edges": [...]}
provenance_query.pipeline_structure(db._duck) -> list[dict]
provenance_query.has_producing_invocation(db._duck, record_id) -> bool
```

---

## `distinct_schema_values()`

```python
db.distinct_schema_values(key) -> list
```

The distinct values a schema key takes across the database.

---

## `export_to_csv()`

```python
db.export_to_csv(variable_class, path, **metadata) -> int
```

Writes every matching record's `to_db()` rows to `path`, adding `_record_id` and
`_meta_<key>` columns, and returns the number of records. Raises `NotFoundError`
if none match. (For one flat row per location instead, use
[`BaseVariable.to_csv()`](variables.md).)

---

## Variable groups

```python
db.add_to_var_group(group_name, variables) -> None      # classes or name strings
db.remove_from_var_group(group_name, variables) -> None
db.list_var_groups() -> list[str]
db.get_var_group(group_name) -> list[type]              # sorted by class name
```

Named, persisted collections of variable types. Adding the same variable twice is
idempotent.

---

## Exclusions

Module-level functions (not methods) — see [Filtering & Selection](../guide/filters.md):

```python
from scidb import exclude_schema, include_schema, list_exclusions

exclude_schema(subject=1, trial=2, reason="...")   # reason required
include_schema(subject=1, trial=2, reason="...")
list_exclusions()                                  # -> DataFrame
```

---

## Exceptions

All inherit from `SciStackError`: `DatabaseNotConfiguredError`,
`NotRegisteredError`, `NotFoundError`, `ReservedMetadataKeyError`,
`AmbiguousVersionError`, `AmbiguousParamError`.

**See also:** [Variables](variables.md) · [Lineage](lineage.md) ·
[Batch Processing](for-each.md)
