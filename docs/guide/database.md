# Database

The `DatabaseManager` handles all storage operations. SciStack uses DuckDB (via SciDuck) for data and lineage storage.

## Configuration

### Global Database

```python
from scidb import configure_database, get_database

# Configure once at startup
# dataset_schema_keys defines which metadata keys identify dataset location
db = configure_database(
    "experiment.duckdb",
    dataset_schema_keys=["subject", "trial", "condition"],
)

# Access anywhere
db = get_database()
```

### Schema Keys

The `dataset_schema_keys` parameter is **required** and defines which metadata keys identify the "location" in your dataset (e.g., subject, trial, sensor) versus computational variants at that location.

- **Schema keys**: Identify the dataset location (used for table queries)
- **Version keys**: Everything else - distinguish computational variants at the same location

## Registration

Variable types are **auto-registered** on first save or load. Manual registration is optional:

```python
# Automatic (preferred) - registers on first save
MyVariable.save(data, subject=1)

# Manual (optional) - register explicitly
db.register(MyVariable)
```

Registration creates the table if it doesn't exist. Re-registering is safe (idempotent).

## Save Operations

### Basic Save

```python
record_id = MyVariable.save(data, subject=1, trial=1, condition="A")
```

### Saves with Same Content

Saving identical data+metadata produces the same deterministic record_id:

```python
record_id1 = MyVar.save(data, subject=1)
record_id2 = MyVar.save(data, subject=1)  # Same data+metadata
assert record_id1 == record_id2  # Same record_id (deterministic)
```

Note: Both saves insert rows into the database. The record_id is computed deterministically from the content hash and metadata, so it will be the same string.

## Load Operations

### Load Single Result

`load()` returns the **latest version** at the specified schema location:

```python
# Load by schema keys - returns latest version
var = MyVariable.load(subject=1, trial=1, condition="A")

# Partial match on schema - returns latest at that location
var = MyVariable.load(subject=1)
```

### Load by Version Hash

```python
var = MyVariable.load(version="abc123...")
```

### Load All Matching

Use `load_all()` to iterate over all matching records:

```python
# Generator (memory-efficient)
for var in MyVariable.load_all(condition="A"):
    print(var.data)

# Load all into DataFrame
df = MyVariable.load_all(condition="A", as_df=True)
df = MyVariable.load_all(condition="A", as_df=True, include_record_id=True)
```

## Version History

### List All Versions

```python
versions = db.list_versions(MyVariable, subject=1)
for v in versions:
    print(f"{v['record_id'][:16]} - {v['created_at']}")
    print(f"  Schema: {v['schema']}")    # Dataset location keys
    print(f"  Version: {v['version']}")  # Computational variant keys
```

### Load Specific Version

```python
# By record_id
var = MyVariable.load(version="abc123...")

# By metadata (returns latest matching)
var = MyVariable.load(subject=1, trial=1)
```

## Provenance Queries

### What Produced This?

```python
provenance = db.get_provenance(MyVariable, subject=1, stage="processed")
if provenance:
    print(f"Function: {provenance['function_name']}")
    print(f"Inputs: {provenance['inputs']}")
    print(f"Constants: {provenance['constants']}")
```

### Check Lineage Exists

```python
if db.has_lineage(record_id):
    print("This variable was produced by a thunked function")
```

## Cache Operations

See [Caching Guide](caching.md) for details.

Caching is handled automatically through `Thunk.query`, which is set to the `DatabaseManager` instance during `configure_database()`. The `DatabaseManager.find_by_lineage()` method looks up previously computed results by lineage hash, then loads the data from DuckDB.

## Database Schema

SciStack uses a single DuckDB database for both data and lineage:

| Table               | Purpose                     |
|---------------------|-----------------------------|
| `_registered_types` | Type registry               |
| `_schema`           | Dataset schema entries      |
| `_variables`        | Variable version metadata   |
| `_record_metadata`  | Record audit trail          |
| `_lineage`          | Provenance DAG              |
| `_variable_groups`  | Named groups of variables   |
| `{VariableName}_data` | One per registered type   |

## Storage Format

Data is stored using **DuckDB native types** (via SciDuck), providing:

- Native DuckDB types for arrays (LIST), nested arrays (LIST[]), and JSON
- Queryable data visible in DBeaver or any DuckDB-compatible viewer
- Efficient columnar storage
- Custom serialization via `to_db()`/`from_db()` for complex types

## Exceptions

| Exception | Cause |
|-----------|-------|
| `NotRegisteredError` | Loading a type that has never been saved |
| `NotFoundError` | No data matches the query |
| `DatabaseNotConfiguredError` | `get_database()` called before `configure_database()` |
| `ReservedMetadataKeyError` | Using reserved key in metadata |
| `UnsavedIntermediateError` | Strict lineage mode with unsaved intermediates |
