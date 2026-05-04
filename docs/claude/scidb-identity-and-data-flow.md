# SciDB Identity System & Data Flow

## Why this document exists

The concepts of `record_id`, `version_keys`, `branch_params`, `call_id`, `content_hash`, and `schema_id` are central to how scidb saves and loads data. Their relationships are spread across multiple files. This document is a single reference that diagrams how all these concepts flow together: SQL table definitions, how column values are computed, and what the user interacts with directly vs what's internal machinery.

This document does **not** duplicate the step-by-step `for_each` walkthrough (that lives in `scidb-for-each-internals.md`). Instead it focuses on the **identity system** -- how records are identified, versioned, discriminated, and traced through the pipeline.

---

## 1. Overview Diagram

```
USER CODE                    SAVE PATH                         DATABASE
============                 =========                         ========

                                                          +-----------------+
class MyVar(BaseVariable):                                |    _schema      |
                                                          |  schema_id (PK) |
                                                          |  schema_level   |
MyVar.save(data,           flat_metadata                  |  subject        |
  subject=1,           --> _split_metadata() -->           |  session        |
  session="A")             schema: {subject:1,session:"A"} |  trial          |
                           version: {}                    +-----------------+
                                                                 |
                                                                 | schema_id
                                                                 v
                          canonical_hash(data) -->       +-----------------------+
                             content_hash               |  _record_metadata     |
                                                        |  record_id      (PK)  |
                          generate_record_id(           |  timestamp      (PK)  |
                            class, schema_ver,          |  variable_name        |
                            content_hash,               |  schema_id      (FK)  |
                            nested_metadata             |  version_keys         |
                          ) --> record_id               |  content_hash         |
                                                        |  branch_params        |
                                                        |  lineage_hash         |
                                                        |  schema_version       |
                                                        |  user_id              |
                                                        |  excluded             |
                                                        +-----------------------+
                                                                 |
                          data --> _save_native()                | record_id
                              or  _save_columnar()               v
                                                        +-----------------------+
                                                        |  MyVar_data           |
                                                        |  record_id   (PK)     |
                                                        |  value   DOUBLE[]     |
                                                        +-----------------------+


LOAD PATH                                                DATABASE
=========                                                ========

MyVar.load(                 _find_record() builds:
  subject=1,                  PARTITION BY (variable_name,
  session="A")                  schema_id, version_keys)
                              ORDER BY timestamp DESC
                              --> latest record per variant
                                                          |
                              Fetch data from MyVar_data  |
                              by record_id                |
                                                          v
                              _storage_to_python()     BaseVariable
                              from_db()                  instance
                              --> native data


FOR_EACH PATH (orchestration layer on top of save/load)
=============

for_each(fn, inputs, outputs, subject=[], session=[])
  |
  +--> ForEachConfig.to_version_keys()   --> config_keys (__fn, __fn_hash, __inputs, ...)
  +--> ForEachConfig.to_call_id()        --> call_id (16 hex chars, for _for_each_expected)
  +--> _convert_inputs() / load_all()    --> DataFrames with __record_id, __branch_params
  +--> variant tracking (rid_to_bp)      --> maps record_id -> upstream branch_params
  +--> scifor.for_each(fn, ...)          --> result_tbl (DataFrame)
  +--> _save_results()                   --> per-row: merge upstream bp + namespace constants
                                             + add config_keys to save_metadata
                                             + output_obj.save(data, **save_metadata)
```

---

## 2. SQL Tables -- The Storage Layer

### `_schema`

```sql
CREATE TABLE IF NOT EXISTS _schema (
    schema_id    INTEGER PRIMARY KEY,
    schema_level VARCHAR NOT NULL,
    -- One VARCHAR column per configured schema key:
    subject      VARCHAR,
    session      VARCHAR,
    trial        VARCHAR
)
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 520-528.

| Column         | What it stores               | How it's computed                                                                          | Example     |
| -------------- | ---------------------------- | ------------------------------------------------------------------------------------------ | ----------- |
| `schema_id`    | Auto-incrementing PK         | `MAX(schema_id)+1` on insert                                                               | `1`         |
| `schema_level` | Deepest schema key provided  | `_infer_schema_level()` walks `dataset_schema_keys` top-down, returns deepest provided key | `"session"` |
| `subject`      | Subject identifier (VARCHAR) | From user's `subject=` kwarg, stringified via `_schema_str()`                              | `"1"`       |
| `session`      | Session identifier (VARCHAR) | From user's `session=` kwarg                                                               | `"A"`       |
| `trial`        | Trial identifier (VARCHAR)   | NULL if not provided at this schema level                                                  | `NULL`      |

Each row represents a unique **data location** -- a specific combination of schema key values. All values are VARCHAR regardless of original Python type. `_schema_str()` normalizes whole-number floats to ints (`1.0` -> `"1"`) for MATLAB compatibility.

### `_record_metadata`

```sql
CREATE TABLE IF NOT EXISTS _record_metadata (
    record_id       VARCHAR NOT NULL,
    timestamp       VARCHAR NOT NULL,
    variable_name   VARCHAR NOT NULL,
    schema_id       INTEGER NOT NULL,
    version_keys    VARCHAR DEFAULT '{}',
    content_hash    VARCHAR,
    lineage_hash    VARCHAR,
    schema_version  INTEGER,
    user_id         VARCHAR,
    branch_params   VARCHAR DEFAULT '{}',
    excluded        BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (record_id, timestamp)
)
```

Source: `/workspace/scidb/src/scidb/database.py`, lines 582-597.

| Column           | What it stores                                                  | How it's computed                                                               | Example                                       |
| ---------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------- |
| `record_id`      | Content-addressed record identity                               | `generate_record_id(class_name, schema_version, content_hash, nested_metadata)` | `"a3f8b2c1e9d04567"`                          |
| `timestamp`      | When saved                                                      | `datetime.now().isoformat()`                                                    | `"2024-03-15T10:30:00"`                       |
| `variable_name`  | Class name                                                      | `variable.__class__.__name__`                                                   | `"FilteredEMG"`                               |
| `schema_id`      | FK to `_schema`                                                 | `_get_or_create_schema_id(level, keys)`                                         | `1`                                           |
| `version_keys`   | JSON: non-schema metadata distinguishing computational variants | `_split_metadata(metadata)["version"]`, serialized                              | `'{"__fn":"bandpass","__fn_hash":"a1b2..."}'` |
| `content_hash`   | SHA-256 of data content (16 hex)                                | `canonical_hash(data)`                                                          | `"d4e5f6a7b8c9d0e1"`                          |
| `lineage_hash`   | Hash of computation invocation                                  | From `LineageFcnResult.invoked.hash` (scihist path only)                        | `"1234abcd5678ef90"`                          |
| `schema_version` | Variable class schema version                                   | `variable.schema_version`                                                       | `1`                                           |
| `user_id`        | Who saved it                                                    | `os.environ.get("SCIDB_USER_ID")`                                               | `"alice"`                                     |
| `branch_params`  | JSON: accumulated pipeline constants                            | Merged from upstream records + namespaced current constants                     | `'{"bandpass.low_hz":20}'`                    |
| `excluded`       | Soft-delete flag                                                | Set by user via `exclude()`                                                     | `FALSE`                                       |

The composite PK `(record_id, timestamp)` allows the same logical record to be re-saved (audit trail). `version_id="latest"` selects the most recent timestamp.

For a detailed explanation of `version_keys` vs `branch_params`, see the [Identity Hierarchy](#3-the-identity-hierarchy) section below.

### `_variables`

```sql
CREATE TABLE IF NOT EXISTS _variables (
    variable_name VARCHAR PRIMARY KEY,
    schema_level  VARCHAR NOT NULL,
    dtype         VARCHAR,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    description   VARCHAR DEFAULT ''
)
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 536-544.

One row per variable type. The `dtype` column is a JSON dict describing how the variable's data is serialized:

- `{"mode": "single_column", "columns": {"value": {"python_type": "ndarray", ...}}}` for native scalars/arrays
- `{"mode": "multi_column", "columns": {...}}` for dicts with multiple keys
- `{"custom": true}` for classes with `to_db()`/`from_db()` overrides

### `_for_each_expected`

```sql
CREATE TABLE IF NOT EXISTS _for_each_expected (
    function_name  VARCHAR NOT NULL,
    call_id        VARCHAR NOT NULL,
    schema_id      INTEGER NOT NULL,
    branch_params  VARCHAR DEFAULT '{}',
    PRIMARY KEY (function_name, call_id, schema_id, branch_params)
)
```

Source: `/workspace/scidb/src/scidb/database.py`, lines 627-634.

Stores the set of combos that a `for_each` call was _expected_ to produce. Used by `check_node_state()` to detect missing/stale outputs.

| Column          | What it stores                              | How it's computed                        |
| --------------- | ------------------------------------------- | ---------------------------------------- |
| `function_name` | `fn.__name__`                               | From `config_keys["__fn"]`               |
| `call_id`       | Call-site identity (16 hex)                 | `call_id_from_version_keys(config_keys)` |
| `schema_id`     | FK to `_schema`                             | From each combo's schema keys            |
| `branch_params` | JSON: branch_params for this expected combo | From variant expansion                   |

The `call_id` disambiguates when the same function is invoked from multiple for_each() call sites. See [call_id](#call_id) below.

### Data tables (e.g., `FilteredEMG_data`)

Created dynamically per variable type. The table name is `ClassName + "_data"` (via `BaseVariable.table_name()`).

Two formats:

- **Scalar/array (native path)**: `record_id VARCHAR PRIMARY KEY` + data columns with DuckDB-inferred types (e.g., `DOUBLE[]` for numpy arrays, `JSON` for dicts)
- **DataFrame (custom `to_db()` path)**: `record_id VARCHAR NOT NULL` (not unique -- multiple rows per record) + one column per DataFrame column

---

## 3. The Identity Hierarchy

```
                                    DISCRIMINATION HIERARCHY
                                    ========================

                 schema_id                    WHERE the data lives
                    |                         (subject=1, session="A")
                    |
                    v
              variable_name                   WHAT TYPE of data
                    |                         (FilteredEMG, StepLength)
                    |
                    v
              version_keys                    HOW this step was configured
                    |                         (__fn, __fn_hash, __inputs, __constants)
                    |
                    v
              content_hash                    WHAT the data IS
                    |                         (SHA-256 of actual values)
                    |
                    v
               record_id                      UNIQUE identity
                    |                         (combines all above)
                    |
                    v
            branch_params                     PIPELINE HISTORY
                                              (accumulated constants from all
                                               upstream functions, namespaced)

  Separately:
            call_id                           CALL-SITE identity
                                              (derived from version_keys,
                                               used only in _for_each_expected)
```

### Detailed breakdown

| Concept           | Stored in                               | Depends on                                                                                         | Used for                                                                                                               | Scope                                                     |
| ----------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| **schema_id**     | `_schema.schema_id`                     | User-provided schema keys (subject, session, trial)                                                | Locating data within the experimental hierarchy                                                                        | Global -- shared across all variable types                |
| **content_hash**  | `_record_metadata.content_hash`         | Raw data bytes                                                                                     | Content-addressed deduplication; same data -> same hash                                                                | Per-record                                                |
| **version_keys**  | `_record_metadata.version_keys` (JSON)  | Non-schema metadata: `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__upstream`, etc.            | Distinguishing computational variants at the same schema location. Drives `load_all(version_id="latest")` partitioning | Per-record, current step only                             |
| **record_id**     | `_record_metadata.record_id`            | `class_name` + `schema_version` + `content_hash` + `canonical_hash(nested_metadata)`               | Unique identity -- addressing a specific data record                                                                   | Per-record                                                |
| **branch_params** | `_record_metadata.branch_params` (JSON) | Upstream records' branch_params + current function's namespaced constants + dynamic discriminators | Variant tracking across the entire pipeline chain. Keeps upstream variants separated in downstream steps               | Per-record, full pipeline chain                           |
| **call_id**       | `_for_each_expected.call_id`            | `SHA-256` of `{__fn, __inputs, __constants, __where, __distribute, __as_table}` from version_keys  | Disambiguating multiple for_each() call sites using the same function                                                  | Per-call-site, derived (not stored in `_record_metadata`) |

### schema_id

A schema_id represents a unique _location_ in the experimental hierarchy. It's computed by `_get_or_create_schema_id()` in SciDuck:

```
_get_or_create_schema_id("session", {subject: "1", session: "A"})
  1. SELECT schema_id FROM _schema WHERE schema_level='session' AND subject='1' AND session='A' AND trial IS NULL
  2. If not found: INSERT INTO _schema ... VALUES (MAX+1, "session", "1", "A", NULL)
  3. Return schema_id
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 580-615.

### content_hash

A 16-hex-char SHA-256 of the data, computed by `canonical_hash()`:

```python
canonical_hash(obj)   # -> "d4e5f6a7b8c9d0e1"
```

The serialization strategy is type-aware:

- Primitives: JSON encoding
- numpy arrays: `shape + dtype + raw bytes`
- DataFrames: `columns + index + array serialization`
- Dicts: sorted keys, recursive
- Lists/tuples: ordered, recursive

Source: `/workspace/canonical-hash/src/canonicalhash/hashing.py`, lines 13-45.

### version_keys

Non-schema metadata stored as a JSON string in `_record_metadata`. When `for_each` saves, this includes the `ForEachConfig` keys:

```python
ForEachConfig.to_version_keys()  # returns:
{
    "__fn": "bandpass_filter",         # function name
    "__fn_hash": "a1b2c3d4e5f6a7b8",  # SHA-256 of function source (16 hex)
    "__inputs": '{"emg": "RawEMG"}',   # JSON of loadable input types
    "__constants": '{"low_hz": "20"}',  # JSON of constant inputs
    "__where": ...,                     # filter expression key (if any)
    "__distribute": true,               # distribute flag (if true)
    "__as_table": [...],                # as_table columns (if any)
}
```

Additionally, per-record keys may be added:

- `__upstream`: JSON dict of `{__rid_param: record_id}` -- upstream record_ids (added by `_save_results()` to ensure record_id uniqueness across upstream variants)
- `__output_num`: integer position of this output in multi-output functions (used by GUI)

Source: `/workspace/scidb/src/scidb/foreach_config.py`, lines 87-107.

The `load_all(version_id="latest")` query partitions by `(variable_name, schema_id, version_keys)`. This means different `for_each` configurations produce separate "version groups" at the same schema location, and `"latest"` returns the newest within each group.

### record_id

A 16-hex-char content-addressed identity computed by `generate_record_id()`:

```python
generate_record_id(
    class_name="FilteredEMG",       # variable type
    schema_version=1,               # schema version
    content_hash="d4e5f6...",       # from canonical_hash(data)
    metadata={"schema": {...}, "version": {...}},  # nested metadata
)
# Implementation:
#   components = ["class:FilteredEMG", "schema:1", "content:d4e5f6...", "meta:<hash_of_metadata>"]
#   SHA-256("|".join(components))[:16]
```

Source: `/workspace/canonical-hash/src/canonicalhash/hashing.py`, lines 114-147.

**Key property**: Two saves with identical data, identical class, identical schema version, and identical metadata produce the **same** `record_id`. The data table uses `record_id` as PK (or with `ON CONFLICT DO NOTHING`), so the data is stored once. Each save still inserts a new `(record_id, timestamp)` row in `_record_metadata` for audit purposes.

### branch_params

A JSON dict tracking the full pipeline history. Assembled in `_save_results()` (lines 1074-1117):

```
1. Start with upstream branch_params (looked up via __rid_* columns -> rid_to_bp mapping)
2. Add current function's constants, namespaced: fn_name + "." + constant_name
3. Add dynamic discriminators (non-schema, non-__ metadata columns with scalar values)
```

Example after a three-step pipeline:

```
RawEMG -> bandpass(low_hz=20) -> FilteredEMG -> rms(window=100) -> RMS -> normalize(method="zscore") -> NormalizedRMS
```

| Step          | branch_params on output                                                    |
| ------------- | -------------------------------------------------------------------------- |
| FilteredEMG   | `{"bandpass.low_hz": 20}`                                                  |
| RMS           | `{"bandpass.low_hz": 20, "rms.window": 100}`                               |
| NormalizedRMS | `{"bandpass.low_hz": 20, "rms.window": 100, "normalize.method": "zscore"}` |

branch_params accumulates; each step inherits everything upstream and adds its own namespaced constants.

### call_id

A 16-hex-char hash derived from a **subset** of version_keys. Computed by `call_id_from_version_keys()`:

```python
_CALL_ID_INCLUDED_KEYS = ("__fn", "__inputs", "__constants", "__where", "__distribute", "__as_table")

def call_id_from_version_keys(version_keys: dict) -> str:
    keys = {k: version_keys[k] for k in _CALL_ID_INCLUDED_KEYS if k in version_keys}
    return SHA256(json.dumps(keys, sort_keys=True))[:16]
```

Source: `/workspace/scidb/src/scidb/foreach_config.py`, lines 33-56.

**Intentionally excludes `__fn_hash`** so that cosmetic source edits don't change the call-site identity. Two for_each() calls with identical inputs/constants/where/distribute/as_table but different function body text produce the same call_id.

Used only in `_for_each_expected` to disambiguate rows when the same function name is invoked from multiple call sites.

---

## 4. The Save Path -- Step by Step

### Direct save: `MyVar.save(data, subject=1, session="A")`

```
MyVar.save(data, subject=1, session="A")
    |
    v
DatabaseManager.save_variable(MyVar, data, subject=1, session="A")
    |
    +-- If data is LineageFcnResult:
    |     extract lineage_hash, lineage_dict, pipeline_version_keys
    |     data = result.data  (unwrap to raw data)
    |
    +-- If data is BaseVariable instance:
    |     raw_data = data.data
    |
    v
DatabaseManager.save(instance, metadata, lineage=..., lineage_hash=...)
    |
    +-- Extract __branch_params from metadata (gets its own column)
    |
    +-- _split_metadata(metadata)
    |     schema: {subject: 1, session: "A"}     (keys in dataset_schema_keys)
    |     version: {}                              (everything else)
    |
    +-- canonical_hash(variable.data)  -->  content_hash
    |
    +-- generate_record_id(
    |     class_name="MyVar",
    |     schema_version=1,
    |     content_hash=content_hash,
    |     metadata={"schema":{subject:1,session:"A"}, "version":{}}
    |   )  -->  record_id
    |
    +-- BEGIN TRANSACTION
    |
    +-- [custom to_db()]:   _save_columnar(record_id, table, df, ...)
    |   [native]:            _save_native(record_id, table, data, ...)
    |     |
    |     +-- _get_or_create_schema_id("session", {subject:"1", session:"A"})  -->  schema_id
    |     +-- CREATE TABLE IF NOT EXISTS "MyVar_data" (record_id PK, value DOUBLE[])
    |     +-- INSERT INTO "MyVar_data" ... ON CONFLICT DO NOTHING
    |     +-- UPSERT INTO _variables (variable_name, schema_level, dtype, ...)
    |
    +-- _save_record_metadata(
    |     record_id, timestamp, "MyVar", schema_id,
    |     version_keys={}, content_hash, lineage_hash,
    |     schema_version=1, user_id, branch_params=None
    |   )
    |     --> INSERT INTO _record_metadata ... ON CONFLICT DO NOTHING
    |
    +-- [if lineage]: _save_lineage(record_id, ...)
    |
    +-- COMMIT
    |
    v
  return record_id
```

### for_each save: `_save_results(result_tbl, outputs, ...)`

The for_each save path adds config_keys and branch_params tracking on top of the direct save:

```
_save_results(result_tbl, outputs, output_names, config_keys, db, rid_to_bp, rid_keys)
    |
    FOR each row in result_tbl:
    |
    +-- 1. Collect upstream branch_params
    |      FOR each __rid_{param} column:
    |        rid = row[__rid_{param}]
    |        merged_bp.update(rid_to_bp[rid])   # inherit upstream
    |
    +-- 2. Add namespaced constants
    |      FOR each constant in config_keys["__constants"]:
    |        merged_bp[f"{fn_name}.{const_name}"] = const_value
    |
    +-- 3. Add dynamic discriminators
    |      FOR each non-schema, non-__ metadata column with scalar value:
    |        merged_bp[col] = value
    |
    +-- 4. Build save_metadata
    |      save_metadata = {non-__ meta cols from row}
    |      save_metadata.update(config_keys)    # __fn, __fn_hash, __inputs, __constants, ...
    |      save_metadata[direct_constants] = values   # unpack constants as direct keys
    |      save_metadata["__branch_params"] = json.dumps(merged_bp)
    |
    +-- 5. Add __upstream to version_keys
    |      IF rid_keys exist:
    |        save_metadata["__upstream"] = json.dumps({rid_col: rid_val})
    |
    +-- 6. Call output_obj.save(output_value, **save_metadata)
    |      --> enters the direct save path above
    |      --> _split_metadata puts config_keys into version_keys
    |      --> __branch_params becomes the branch_params column
    |
    v
  (saves complete)
```

---

## 5. The Load Path -- Step by Step

### Direct load: `MyVar.load(subject=1, session="A")`

```
MyVar.load(subject=1, session="A")
    |
    v
DatabaseManager.load_variable(MyVar, version="latest", subject=1, session="A")
    |
    +-- _split_metadata({subject:1, session:"A"})
    |     schema: {subject:1, session:"A"}
    |     version: {}
    |
    +-- _find_record("MyVar", nested_metadata=..., version_id="latest")
    |     |
    |     +-- Build SQL:
    |     |     WITH ranked AS (
    |     |       SELECT rm.*, s.subject, s.session,
    |     |         ROW_NUMBER() OVER (
    |     |           PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
    |     |           ORDER BY rm.timestamp DESC
    |     |         ) as rn
    |     |       FROM _record_metadata rm
    |     |       LEFT JOIN _schema s ON rm.schema_id = s.schema_id
    |     |       WHERE rm.variable_name = 'MyVar'
    |     |         AND s.subject = '1' AND s.session = 'A'
    |     |         AND COALESCE(rm.excluded, FALSE) = FALSE
    |     |     ) SELECT * FROM ranked WHERE rn = 1
    |     |
    |     +-- Returns: one row per (variable_name, schema_id, version_keys) group
    |           = one row per computational variant at this schema location
    |
    +-- _load_by_record_row(MyVar, row, loc=None, iloc=None)
    |     |
    |     +-- Look up dtype from _variables
    |     +-- SELECT * FROM "MyVar_data" WHERE record_id = ?
    |     +-- [native]:  _storage_to_python(value, dtype_meta)
    |     |   [custom]:  from_db(df) or _unflatten_struct_columns(df, ...)
    |     +-- Construct BaseVariable instance with data, record_id, metadata, branch_params
    |
    v
  return BaseVariable instance
```

### for_each load: `_convert_inputs()` / `load_all()`

The for_each load path loads **all** records for a variable type, returning a DataFrame with tracking columns:

```
_load_var_type_all(VarClass, db, where)
    |
    +-- _find_record(type_name, version_id="latest")   # no metadata filter = ALL records
    |     PARTITION BY (variable_name, schema_id, version_keys)
    |     --> one latest row per variant per schema location
    |
    +-- For each matched row:
    |     load data from VarClass_data by record_id
    |     extract schema keys, version_keys, branch_params
    |
    +-- Assemble DataFrame:
    |     schema columns (subject, session, ...) from _schema
    |     __record_id column (for variant tracking)
    |     __branch_params column (JSON string)
    |     data column(s)
    |
    v
  return DataFrame

for_each then:
    +-- Builds rid_to_bp mapping: {record_id -> branch_params_dict}
    +-- Renames __record_id -> __rid_{param_name} per input
    +-- Strips __branch_params (now tracked via rid_to_bp)
    +-- Expands base_combos x valid rid-combos -> full_combos
    +-- Passes to scifor.for_each() which filters per-combo
```

---

## 5b. Loading a Specific Version -- Concrete Example

This section walks through a concrete scenario: you've saved `FilteredSignal` with two different `low_hz` values and want to load just one.

### Setup: how the two versions were saved

```python
for_each(
    bandpass_filter,
    inputs={"raw": RawEMG, "low_hz": 20},
    outputs=[FilteredSignal],
    subject=[1, 2],
)
for_each(
    bandpass_filter,
    inputs={"raw": RawEMG, "low_hz": 30},
    outputs=[FilteredSignal],
    subject=[1, 2],
)
```

Here `low_hz` is a **constant input** (non-loadable scalar in `inputs`). After both runs, each `FilteredSignal` record has:

| Field | low_hz=20 run | low_hz=30 run |
|-------|---------------|---------------|
| `version_keys` | `{"__fn": "bandpass_filter", "__fn_hash": "...", "__inputs": '{"raw":"RawEMG"}', "__constants": '{"low_hz": 20}', "low_hz": 20, "__upstream": ...}` | Same structure with `30` |
| `branch_params` | `{"bandpass_filter.low_hz": 20}` | `{"bandpass_filter.low_hz": 30}` |

Note that `low_hz` appears in **three places**: (1) in `__constants` as JSON-encoded config, (2) as a direct key in `version_keys` (unpacked by `_save_results()` line 1131), and (3) namespaced as `bandpass_filter.low_hz` in `branch_params`.

### Case 1: Direct load -- `FilteredSignal.load(subject=1, low_hz=20)`

**Syntax:**

```python
var = FilteredSignal.load(subject=1, low_hz=20)
```

Any keyword argument that is **not** a configured schema key is treated as a **branch_params filter**.

**Internal flow:**

```
FilteredSignal.load(subject=1, low_hz=20)
    |
    +-- Separate schema vs non-schema kwargs:
    |     schema_metadata    = {subject: 1}      # in dataset_schema_keys
    |     branch_params_filter = {low_hz: 20}     # everything else
    |
    +-- _db.load_all(FilteredSignal, schema_metadata,
    |                 version_id="latest",
    |                 branch_params_filter={low_hz: 20})
    |     |
    |     +-- _find_record("FilteredSignal",
    |     |     nested_metadata={"schema":{subject:1}, "version":{}},
    |     |     version_id="latest",
    |     |     branch_params_filter={"low_hz": 20})
    |     |     |
    |     |     +-- SQL query: PARTITION BY (variable_name, schema_id, version_keys)
    |     |     |   → returns one row per computational variant at subject=1
    |     |     |   → TWO rows (low_hz=20 variant + low_hz=30 variant)
    |     |     |
    |     |     +-- branch_params_filter loop (Python-side, per row):
    |     |           for key="low_hz", value=20:
    |     |             1. Check version_keys first:
    |     |                json.loads(row.version_keys).get("low_hz") → 20
    |     |                20 == 20? YES → row matches ✓
    |     |             2. (would check branch_params next, but version_keys matched)
    |     |           → filters DOWN to 1 row (the low_hz=20 variant)
    |     |
    |     +-- Load data from FilteredSignal_data by record_id
    |     +-- Return single BaseVariable instance
    |
    +-- len(results) == 1 → return the variable directly
```

Source: `/workspace/scidb/src/scidb/variable.py` lines 347-370, `/workspace/scidb/src/scidb/database.py` lines 1364-1382.

**Key mechanism**: `_find_record()` checks `version_keys` first (line 1379). Since `low_hz: 20` is a direct key in version_keys (unpacked there by `_save_results`), it matches immediately. If it weren't in version_keys, the fallback would check `branch_params` via suffix matching (`low_hz` → `bandpass_filter.low_hz`).

**What happens if you forget `low_hz`?**

```python
var = FilteredSignal.load(subject=1)
# → AmbiguousVersionError:
#   2 variants exist for FilteredSignal at subject=1.
#   Specify branch parameters to select one:
#     bandpass_filter.low_hz=20  (record_id: 'a3f8b2c1e9d04567')
#     bandpass_filter.low_hz=30  (record_id: 'b4e9c3d2f0a15678')
```

The error message tells you exactly which branch parameters to add. Source: `/workspace/scidb/src/scidb/variable.py` lines 380-391.

### Case 2: for_each input -- automatic variant tracking (no filtering needed)

**Syntax:**

```python
for_each(
    compute_rms,
    inputs={"signal": FilteredSignal, "window": 100},
    outputs=[RMS],
    subject=[1, 2],
)
```

No `low_hz` filtering needed. for_each loads **all** variants and iterates over each one separately.

**Internal flow:**

```
for_each(compute_rms, inputs={"signal": FilteredSignal, ...}, ...)
    |
    +-- _load_var_type_all(FilteredSignal, db, where=None)
    |     |
    |     +-- FilteredSignal.load_all(version_id="latest")
    |     |     → returns ALL variants: low_hz=20 AND low_hz=30, for all subjects
    |     |
    |     +-- Assembles DataFrame:
    |           subject | __record_id | __branch_params                   | <data>
    |           "1"     | "a3f8..."   | '{"bandpass_filter.low_hz": 20}'  | [...]
    |           "1"     | "b4e9..."   | '{"bandpass_filter.low_hz": 30}'  | [...]
    |           "2"     | "c5fa..."   | '{"bandpass_filter.low_hz": 20}'  | [...]
    |           "2"     | "d6ab..."   | '{"bandpass_filter.low_hz": 30}'  | [...]
    |
    +-- Variant tracking:
    |     rid_to_bp = {"a3f8...": {"bandpass_filter.low_hz": 20},
    |                   "b4e9...": {"bandpass_filter.low_hz": 30}, ...}
    |     Rename __record_id → __rid_signal
    |
    +-- Combo expansion:
    |     base_combos: [{subject:"1"}, {subject:"2"}]
    |     × rid variants: 2 per subject
    |     = 4 full combos:
    |       {subject:"1", __rid_signal:"a3f8..."}   ← low_hz=20
    |       {subject:"1", __rid_signal:"b4e9..."}   ← low_hz=30
    |       {subject:"2", __rid_signal:"c5fa..."}   ← low_hz=20
    |       {subject:"2", __rid_signal:"d6ab..."}   ← low_hz=30
    |
    +-- scifor.for_each runs compute_rms 4 times
    |
    +-- _save_results: each RMS output inherits upstream branch_params
          e.g. RMS at subject=1 from low_hz=20 gets:
            branch_params = {"bandpass_filter.low_hz": 20, "compute_rms.window": 100}
```

This is the **normal** pattern. You don't filter inputs; the variant tracking system automatically separates them and propagates the pipeline history forward.

### Case 3: for_each input -- restricting to a subset of variants (KNOWN GAP)

If you have `low_hz=20`, `30`, and `50` variants but only want `20` and `30` as for_each inputs, **there is no first-class syntax for this today**.

You might expect `Fixed` or `EachOf(Fixed(...), Fixed(...))` to work:

```python
# Intuitive but DOES NOT WORK for pipeline variant selection:
for_each(
    compute_rms,
    inputs={"signal": Fixed(FilteredSignal, low_hz=20), "window": 100},
    outputs=[RMS],
    subject=[1, 2],
)

# Also does not work:
for_each(
    compute_rms,
    inputs={
        "signal": EachOf(
            Fixed(FilteredSignal, low_hz=20),
            Fixed(FilteredSignal, low_hz=30),
        ),
        "window": 100,
    },
    outputs=[RMS],
    subject=[1, 2],
)
```

**Why it doesn't work -- two independent reasons:**

**Reason 1: `Fixed` is designed for schema key overrides, not variant selection.**

`Fixed` works by merging its overrides into the combo metadata, then scifor's `_filter_df_for_combo()` filters the DataFrame. But `_filter_df_for_combo()` only filters on **schema keys** (the keys registered via `configure_database()`) -- it ignores all other columns:

```python
# scifor/src/scifor/foreach.py, lines 469-480
def _filter_df_for_combo(df, metadata, schema_keys):
    mask = pd.Series([True] * len(df), index=df.index)
    for key in schema_keys:              # <-- ONLY schema keys
        if key in df.columns and key in metadata:
            mask = mask & (col_vals == meta_val)
    return df[mask]
```

So `Fixed(FilteredSignal, low_hz=20)` adds `low_hz=20` to the effective metadata, but since `low_hz` is not a schema key, `_filter_df_for_combo` never uses it for filtering.

`Fixed` works correctly for its intended purpose: overriding schema dimensions. For example, `Fixed(StepLength, session="BL")` loads all records but filters to `session="BL"` per-combo, which works because `session` IS a schema key.

**Reason 2: Pipeline constants are stripped from loaded DataFrames.**

Even if `_filter_df_for_combo` filtered on non-schema keys, `low_hz` wouldn't be a column in the DataFrame to filter on. When data was saved via `for_each` with `low_hz` as a constant input (`inputs={..., "low_hz": 20}`), `_stringify_meta()` inside `_load_var_type_all()` strips keys that appear in `__constants`:

```python
# scidb/src/scidb/foreach.py, inside _load_var_type_all
def _stringify_meta(meta: dict) -> dict:
    const_keys = set(json.loads(meta.get("__constants", "{}")).keys())
    return {k: v for k, v in meta.items()
            if not k.startswith("__") and k not in const_keys}
    #                                      ^^^^^^^^^^^^^^^^
    #                                      low_hz stripped here
```

This stripping is intentional -- without it, constants from one pipeline step would pollute downstream DataFrames as extra metadata columns. But it means the loaded DataFrame has no `low_hz` column to filter on.

**What works today:**

| Goal | Syntax | Works? |
|------|--------|--------|
| Use all variants (automatic) | `inputs={"signal": FilteredSignal}` | **Yes** -- variant tracking handles everything |
| Override a schema key | `Fixed(StepLength, session="BL")` | **Yes** -- `session` is a schema key |
| Restrict to a specific pipeline variant | `Fixed(FilteredSignal, low_hz=20)` | **No** -- `low_hz` is not a schema key, and is stripped from the DataFrame |
| Iterate over a subset of variants | `EachOf(Fixed(FilteredSignal, low_hz=20), ...)` | **No** -- same problem as above within each EachOf alternative |

**Workaround:** For direct loads, `FilteredSignal.load(subject=1, low_hz=20)` works perfectly via `branch_params_filter`. The gap is specifically in the for_each input path when you want to restrict which variants are loaded.

### Summary: which syntax to use

| Goal | Syntax | Mechanism |
|------|--------|-----------|
| **Load one version directly** | `FilteredSignal.load(subject=1, low_hz=20)` | Non-schema kwargs become `branch_params_filter` in `_find_record()`. Checks `version_keys` first, then `branch_params` with suffix matching. |
| **for_each: use all variants** | `inputs={"signal": FilteredSignal}` | `load_all()` gets all variants; variant tracking expands combos and propagates branch_params downstream. **This is the normal pattern.** |
| **for_each: restrict to one variant** | No first-class syntax | **Known gap.** `Fixed` only filters on schema keys, not pipeline variants. See Case 3 above. |
| **for_each: override schema key** | `inputs={"baseline": Fixed(Signal, session="BL")}` | `Fixed` merges override into combo metadata; scifor filters DataFrame on schema key column. Works as intended. |
| **Load by exact record_id** | `FilteredSignal.load(version="a3f8b2c1e9d04567")` | Bypasses all filtering; direct PK lookup on `_record_metadata`. |

### Branch params filter matching: two-step resolution

When `_find_record()` applies `branch_params_filter`, it checks **two places** per row, in order:

1. **version_keys** (exact key match): `json.loads(row.version_keys).get(key) == value`
2. **branch_params** (exact then suffix match):
   - Exact: `branch_params_dict[key] == value`
   - Suffix: `branch_params_dict["bandpass_filter.low_hz"] == value` when you pass just `low_hz`

If the bare key matches multiple namespaced keys in branch_params (e.g., both `step1.low_hz` and `step2.low_hz`), an `AmbiguousParamError` is raised, telling you to use the fully namespaced key.

Source: `/workspace/scidb/src/scidb/database.py` lines 89-108 (`_match_branch_param`), lines 1364-1382 (`branch_params_filter` loop).

---

## 6. User-Facing API vs Internal Machinery

### User layer (directly invoked by user code)

| API                                                   | Purpose                                         |
| ----------------------------------------------------- | ----------------------------------------------- |
| `configure_database(path, schema_keys)`               | Create/connect to database, set global instance |
| `class MyVar(BaseVariable)`                           | Define a variable type                          |
| `MyVar.save(data, **metadata)`                        | Save data directly                              |
| `MyVar.load(**metadata)`                              | Load one record                                 |
| `MyVar.load_all(**metadata)`                          | Load all matching records                       |
| `for_each(fn, inputs, outputs, **metadata_iterables)` | Batch execute with auto load/save               |

### Internal: for_each orchestration

| Concept                                   | Where                   | Purpose                                                               |
| ----------------------------------------- | ----------------------- | --------------------------------------------------------------------- |
| `ForEachConfig`                           | `foreach_config.py`     | Serializes for_each() config into version_keys                        |
| `to_version_keys()`                       | `foreach_config.py:87`  | Produces `__fn`, `__fn_hash`, `__inputs`, `__constants`, etc.         |
| `to_call_id()`                            | `foreach_config.py:109` | 16-hex hash for \_for_each_expected disambiguation                    |
| `_save_results()`                         | `foreach.py:1043`       | Per-row: merge branch_params, assemble save_metadata, call .save()    |
| Variant tracking (`rid_to_bp`, `__rid_*`) | `foreach.py:321-458`    | Map upstream record_ids to their branch_params, expand combos         |
| `_persist_expected_combos()`              | `foreach.py:462-467`    | Write expected combos to `_for_each_expected` for completeness checks |

### Internal: database layer

| Concept                               | Where                                 | Purpose                                                        |
| ------------------------------------- | ------------------------------------- | -------------------------------------------------------------- |
| `_split_metadata()`                   | `database.py:659`                     | Separate schema keys from version keys                         |
| `_infer_schema_level()`               | `database.py:674`                     | Determine deepest provided schema key                          |
| `_get_or_create_schema_id()`          | `sciduckdb.py:580`                    | Look up or insert `_schema` row                                |
| `generate_record_id()`                | `hashing.py:114`                      | Content-addressed ID from class+schema_ver+content+metadata    |
| `canonical_hash()`                    | `hashing.py:13`                       | SHA-256 of arbitrary Python data                               |
| `_find_record()`                      | `database.py:1251`                    | Query `_record_metadata` with partitioned latest-version logic |
| `_save_native()` / `_save_columnar()` | `database.py:834` / `database.py:725` | Insert data into variable-specific data table                  |
| `_save_record_metadata()`             | `database.py:692`                     | Insert audit row into `_record_metadata`                       |

---

## 7. Edge Cases and Discrimination Gaps

### Same function + same constants + different inputs -> identical branch_params

If `bandpass(low_hz=20)` is called on two different upstream variants that happen to have the same branch_params themselves, the resulting outputs will have **identical `branch_params`**. They are still distinguished by:

- Different `__upstream` in `version_keys` (different upstream record_ids)
- Therefore different `record_id` (because metadata differs)
- The variant expansion system (`rid_per_combo`) tracks them separately via `__rid_*` columns

### \_\_upstream ensures record_id uniqueness across upstream variants

Without `__upstream` in save_metadata, two outputs from the same function with the same constants but different upstream data could produce the same record_id if the output data happens to be identical. The `__upstream` field forces the version_keys (and thus the metadata hash, and thus the record_id) to differ.

Source: `_save_results()`, lines 1136-1146.

### version_keys contains both config and per-record fields

The `version_keys` JSON in `_record_metadata` may contain:

- **Config fields** (from `ForEachConfig`): `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__where`, `__distribute`, `__as_table`
- **Per-record fields**: `__upstream`, `__output_num`, and for direct saves, any non-schema metadata keys

The `call_id_from_version_keys()` function uses a strict allow-list (`_CALL_ID_INCLUDED_KEYS`) to extract only the config fields, ignoring per-record fields. This ensures the call_id is stable even when per-record fields differ.

### Direct saves vs for_each saves

| Aspect           | Direct `.save()`                                                                 | `for_each` save                                                 |
| ---------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| version_keys     | Non-schema metadata from kwargs (or pipeline_version_keys from LineageFcnResult) | `ForEachConfig` keys + `__upstream` + direct constants          |
| branch_params    | `None` (no pipeline tracking)                                                    | Merged upstream + namespaced constants + dynamic discriminators |
| call_id          | N/A                                                                              | Computed and stored in `_for_each_expected`                     |
| Variant tracking | N/A                                                                              | Full `__rid_*` -> `rid_to_bp` system                            |

### Constants in version_keys vs branch_params

Constants appear in **both** places but serve different purposes:

- In `version_keys.__constants`: JSON-encoded dict, part of the computational config fingerprint. Used for partitioning in `load_all(version_id="latest")`.
- In `branch_params.{fn_name}.{const_name}`: Namespaced, part of the pipeline history. Used for variant tracking and downstream propagation.

They are also unpacked as direct keys in `save_metadata` (e.g., `low_hz=20`) so that `_split_metadata()` routes them correctly. If the constant name matches a schema key, it goes into `schema`; otherwise into `version`.

### No for_each input filtering by pipeline variant (known gap)

`Fixed` is designed for schema key overrides (`Fixed(Signal, session="BL")`), not for pipeline variant selection (`Fixed(Signal, low_hz=20)`). There are two reasons:

1. **scifor's `_filter_df_for_combo()` only filters on schema keys**, ignoring non-schema columns in the effective metadata.
2. **`_stringify_meta()` strips constant-input keys** from the loaded DataFrame columns (to prevent pollution in downstream steps), so there's nothing to filter on even if scifor checked non-schema keys.

The direct load path (`FilteredSignal.load(subject=1, low_hz=20)`) does not have this limitation -- it uses `branch_params_filter` in `_find_record()`, which checks both `version_keys` and `branch_params` with suffix matching.

See [Section 5b, Case 3](#case-3-for_each-input----restricting-to-a-subset-of-variants-known-gap) for full details.
