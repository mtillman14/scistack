# API Reference

## Core Classes

### `BaseVariable`

Base class for storable data types. For most data types (scalars, numpy arrays, lists, dicts), no methods need to be overridden — SciDuck handles serialization automatically. Override `to_db()` and `from_db()` only for custom multi-column serialization.

```python
class MyVariable(BaseVariable):
    # schema_version defaults to 1 — only set explicitly for schema migrations

    # Optional — only needed for custom serialization
    def to_db(self) -> pd.DataFrame:
        """Convert self.data to DataFrame."""
        ...

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Any:
        """Convert DataFrame to native type."""
        ...
```

**Instance Attributes:**

| Attribute      | Type           | Description                                            |
| -------------- | -------------- | ------------------------------------------------------ |
| `data`         | `Any`          | The native data                                        |
| `record_id`    | `str \| None`  | Version hash (after save/load)                         |
| `metadata`     | `dict \| None` | Metadata (after save/load)                             |
| `content_hash` | `str \| None`  | Content hash computed from data                        |
| `lineage_hash` | `str \| None`  | Lineage hash (None for raw data, set for thunk output) |

**Class Methods:**

| Method                                                                     | Description                                            |
| -------------------------------------------------------------------------- | ------------------------------------------------------ |
| `save(data, index=None, **metadata)`                                       | Save data to database, returns record_id               |
| `load(version="latest", loc=None, iloc=None, **metadata)`                  | Load single result (latest version at schema location) |
| `load_all(as_df=False, include_record_id=False, **metadata)`               | Load all matching as generator or DataFrame            |
| `list_versions(**metadata)`                                                | List all versions at a schema location                 |
| `save_from_dataframe(df, data_column, metadata_columns, **common_metadata)`| Save each row as separate record                       |
| `table_name()`                                                             | Get DuckDB table name (returns the class name)         |

**Instance Methods:**

| Method         | Description             |
| -------------- | ----------------------- |
| `to_csv(path)` | Export data to CSV file |

---

### `DatabaseManager`

Manages database connection and operations using DuckDB (via SciDuck) for data and lineage storage.

```python
db = DatabaseManager(
    "path/to/db.duckdb",
    dataset_schema_keys=["subject", "trial", "condition"],
)
```

**Constructor Parameters:**

| Parameter              | Type          | Description                                                      |
| ---------------------- | ------------- | ---------------------------------------------------------------- |
| `dataset_db_path`      | `str \| Path` | Path to DuckDB database file                                     |
| `dataset_schema_keys`  | `list[str]`   | **Required.** Metadata keys that identify dataset location       |
| `lineage_mode`         | `str`         | `"strict"` (default) or `"ephemeral"`                            |

**Methods:**

| Method                                                         | Description                                                      |
| -------------------------------------------------------------- | ---------------------------------------------------------------- |
| `register(variable_class)`                                     | Register a variable type (optional, auto-registers on save/load) |
| `save(variable, metadata, lineage=None, lineage_hash=None)`    | Save variable (internal)                                         |
| `save_variable(variable_class, data, index=None, **metadata)`  | Save data with input normalization and lineage extraction         |
| `load(variable_class, metadata, version="latest")`             | Load single variable (latest at schema location)                 |
| `load_all(variable_class, metadata)`                           | Generator yielding all matching variables                        |
| `list_versions(variable_class, **metadata)`                    | List all versions at schema location                             |
| `get_provenance(variable_class, version=None, **metadata)`     | Get immediate lineage info                                       |
| `has_lineage(record_id)`                                       | Check if lineage exists                                          |
| `find_by_lineage(pipeline_thunk)`                              | Find cached outputs by computation lineage                       |
| `save_ephemeral_lineage(ephemeral_id, variable_type, lineage)` | Save ephemeral lineage for unsaved intermediates                 |
| `export_to_csv(variable_class, path, **metadata)`              | Export matching records to CSV                                   |
| `close()`                                                      | Close connections and reset Thunk.query                          |

---

## Configuration Functions

### `configure_database(dataset_db_path, dataset_schema_keys, lineage_mode="strict")`

Configure the global database.

```python
db = configure_database(
    "experiment.duckdb",
    dataset_schema_keys=["subject", "trial", "condition"],
)
```

**Parameters:**

| Parameter              | Type          | Description                                                      |
| ---------------------- | ------------- | ---------------------------------------------------------------- |
| `dataset_db_path`      | `str \| Path` | Path to DuckDB database file                                     |
| `dataset_schema_keys`  | `list[str]`   | **Required.** Metadata keys that identify dataset location       |
| `lineage_mode`         | `str`         | `"strict"` (default) or `"ephemeral"`                            |

**Returns:** `DatabaseManager`

---

### `get_database()`

Get the global database.

```python
db = get_database()
```

**Returns:** `DatabaseManager`
**Raises:** `DatabaseNotConfiguredError` if not configured

---

## Thunk System

### `@thunk(unpack_output=False, unwrap=True)`

Decorator for lineage-tracked functions with automatic caching.

```python
@thunk
def process(data: np.ndarray) -> np.ndarray:
    return data * 2

result = process(data)  # Returns ThunkOutput
result.data  # The actual result
```

**Parameters:**

| Parameter       | Default | Description                                                                                                                          |
| --------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `unpack_output` | `False` | If True, split a returned tuple into separate ThunkOutputs.                                                                          |
| `unwrap`        | `True`  | If True, unwrap `BaseVariable` and `ThunkOutput` inputs to raw data. If False, pass wrapper objects directly (useful for debugging). |

**Automatic caching:**

Results are cached automatically when `Thunk.query` is set (done by `configure_database()`). Once saved, subsequent calls with the same inputs skip execution:

```python
result = process(data)
MyVar.save(result, ...)  # Populates cache

result2 = process(data)  # Cache hit! No execution
```

For multi-output functions, all outputs must be saved before caching takes effect:

```python
@thunk(unpack_output=True)
def split(data):
    return data[:5], data[5:]

left, right = split(data)
LeftVar.save(left, ...)   # Save all outputs
RightVar.save(right, ...)

left2, right2 = split(data)  # Cache hit for both!
```

**Cross-script lineage:**

```python
# step1.py
result = process(raw_data)
Intermediate.save(result, subject=1)

# step2.py
loaded = Intermediate.load(subject=1)

@thunk
def analyze(data):  # Receives raw data (unwrapped from loaded)
    return data.mean()

result = analyze(loaded)  # Pass the variable, lineage is captured
```

**Debugging with unwrap=False:**

```python
@thunk(unwrap=False)
def debug_process(var):
    print(f"Input record_id: {var.record_id}")
    print(f"Input metadata: {var.metadata}")
    return var.data * 2
```

---

### `Thunk`

Wrapper for a function with lineage tracking.

**Attributes:**

| Attribute       | Type       | Description                         |
| --------------- | ---------- | ----------------------------------- |
| `fcn`           | `Callable` | The wrapped function                |
| `unpack_output` | `bool`     | Whether to unpack a returned tuple  |
| `unwrap`        | `bool`     | Whether to unwrap inputs            |
| `hash`          | `str`      | SHA-256 of bytecode + unpack_output |

---

### `PipelineThunk`

A specific invocation with captured inputs.

**Attributes:**

| Attribute | Type                 | Description             |
| --------- | -------------------- | ----------------------- |
| `thunk`   | `Thunk`              | Parent thunk            |
| `inputs`  | `dict`               | Captured inputs         |
| `outputs` | `tuple[ThunkOutput]` | Results after execution |
| `hash`    | `str`                | Hash of thunk + inputs  |

**Methods:**

| Method                   | Description                    |
| ------------------------ | ------------------------------ |
| `compute_lineage_hash()` | Generate lineage lookup hash   |

---

### `ThunkOutput`

Wraps a function output with lineage.

**Attributes:**

| Attribute        | Type            | Description         |
| ---------------- | --------------- | ------------------- |
| `pipeline_thunk` | `PipelineThunk` | Producer            |
| `output_num`     | `int`           | Output index        |
| `data`           | `Any`           | Computed result     |
| `is_complete`    | `bool`          | Whether computed    |
| `hash`           | `str`           | Lineage hash        |

---

## Lineage Functions

### `extract_lineage(thunk_output)`

Extract lineage from a ThunkOutput. Available from `scidb.lineage`.

```python
from scidb.lineage import extract_lineage

lineage = extract_lineage(result)
print(lineage.function_name)
```

**Returns:** `LineageRecord`

---

### `get_raw_value(data)`

Unwrap ThunkOutput to raw value. Available from `scidb.lineage`.

```python
from scidb.lineage import get_raw_value

raw = get_raw_value(thunk_output)  # Returns thunk_output.data
raw = get_raw_value(plain_data)    # Returns plain_data unchanged
```

---

### `LineageRecord`

Provenance data structure.

**Attributes:**

| Attribute       | Type         | Description            |
| --------------- | ------------ | ---------------------- |
| `function_name` | `str`        | Function name          |
| `function_hash` | `str`        | Function bytecode hash |
| `inputs`        | `list[dict]` | Input descriptors      |
| `constants`     | `list[dict]` | Constant descriptors   |

---

## Exceptions

| Exception                    | Description                                  |
| ---------------------------- | -------------------------------------------- |
| `SciStackError`                 | Base exception                               |
| `NotRegisteredError`         | Loading a type that was never saved          |
| `NotFoundError`              | No matching data                             |
| `DatabaseNotConfiguredError` | Global DB not configured                     |
| `ReservedMetadataKeyError`   | Using reserved metadata key                  |
| `UnsavedIntermediateError`   | Strict mode detected unsaved intermediates   |

---

## Reserved Metadata Keys

Cannot be used in `save()` metadata:

- `record_id`
- `id`
- `created_at`
- `schema_version`
- `index`
- `loc`
- `iloc`
