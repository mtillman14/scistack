# Variables API — `BaseVariable`

`BaseVariable` is the foundation of SciStack. Every piece of data you store is an instance of a `BaseVariable` subclass. The subclass name becomes the database table name; the class itself has no required properties or methods for the common case.

## Defining a Variable Type

=== "Python"

    ```python
    from scidb import BaseVariable

    class StepLength(BaseVariable):
        pass  # That's it — no boilerplate needed
    ```

    For custom multi-column serialization (e.g., storing a pandas DataFrame in a structured way), override `to_db()` and `from_db()`:

    ```python
    import pandas as pd
    import numpy as np

    class GaitTable(BaseVariable):

        def to_db(self) -> pd.DataFrame:
            """Convert self.data (a DataFrame) to storage format."""
            return self.data  # already a DataFrame

        @classmethod
        def from_db(cls, df: pd.DataFrame):
            """Convert back from storage."""
            return df
    ```

    `schema_version` defaults to 1. Increment it when you change the structure of a variable type and need to distinguish old records from new ones.

=== "MATLAB"

    ```matlab
    % In StepLength.m:
    classdef StepLength < scidb.BaseVariable
    end

    % In GaitTable.m:
    classdef GaitTable < scidb.BaseVariable
    end
    ```

    Custom serialization is handled on the Python side. In MATLAB, variable type definitions are always empty classdefs.

---

## `save()`

Saves data to the database under the calling variable type. Accepts raw data, a `ThunkOutput` (from a thunked function), or an existing variable instance. Lineage is extracted and stored automatically when saving a `ThunkOutput`.

Returns a `record_id` string — a deterministic hash of the data content and metadata.

=== "Python"

    ```python
    # Save raw data
    record_id = StepLength.save(np.array([0.65, 0.72]), subject=1, session="A")

    # Save a thunk result (lineage tracked automatically)
    result = bandpass_filter(raw_signal, 20, 450)
    record_id = FilteredSignal.save(result, subject=1, session="A")

    # Re-save an existing variable under new metadata
    var = StepLength.load(subject=1, session="A")
    record_id = StepLength.save(var, subject=2, session="A")

    # Save to a specific database (not the global one)
    record_id = StepLength.save(data, db=my_db, subject=1, session="A")
    ```

=== "MATLAB"

    ```matlab
    % Save raw data
    record_id = StepLength().save(data, subject=1, session="A");

    % Save a thunk result (lineage tracked automatically)
    result = filter_fn(raw_signal, 20, 450);
    record_id = FilteredSignal().save(result, subject=1, session="A");

    % Re-save an existing variable under new metadata
    var = StepLength().load(subject=1, session="A");
    record_id = StepLength().save(var, subject=2, session="A");

    % Save to a specific database
    record_id = StepLength().save(data, db=my_db, subject=1, session="A");
    ```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `data` | any | Data to save: raw value, `ThunkOutput`, or existing `BaseVariable` |
| `index` | optional | Label-based index for the stored DataFrame rows (Python only) |
| `db` | optional | `DatabaseManager` to use instead of the global database |
| `**metadata` | keyword args | Addressing metadata (e.g., `subject=1, session="A"`) |

**Returns:** `str` — the `record_id` of the saved record

!!! note "Deterministic record IDs"
    Saving identical data with identical metadata always produces the same `record_id`. This means saving the same data twice is safe — you get the same ID back both times.

---

## `load()`

Loads variable(s) from the database. Returns a **single variable** when exactly one record matches, or a **list** of variables when multiple records match (e.g., when only partial schema keys are given).

=== "Python"

    ```python
    # Single record (all schema keys specified)
    var = StepLength.load(subject=1, session="A")
    print(var.data)          # numpy array
    print(var.metadata)      # {"subject": 1, "session": "A"}
    print(var.record_id)     # "a3f8c2e1..."

    # Multiple records (partial schema keys)
    all_sessions = StepLength.load(subject=1)
    for v in all_sessions:
        print(v.metadata["session"], v.data.shape)

    # Load as DataFrame when multiple records match
    df = StepLength.load(subject=1, as_table=True)

    # Load a specific version by record_id
    var = StepLength.load(version="a3f8c2e1b9d04710...")

    # Load from a specific database
    var = StepLength.load(db=my_db, subject=1, session="A")
    ```

=== "MATLAB"

    ```matlab
    % Single record (all schema keys specified)
    var = StepLength().load(subject=1, session="A");
    disp(var.data);       % numeric array
    disp(var.metadata);   % struct with subject, session fields
    disp(var.record_id);  % "a3f8c2e1..."

    % Multiple records (partial schema keys) — returns array of ThunkOutputs
    all_sessions = StepLength().load(subject=1);
    for i = 1:numel(all_sessions)
        fprintf('%s: %s\n', all_sessions(i).metadata.session, mat2str(size(all_sessions(i).data)));
    end

    % Load as MATLAB table when multiple records match
    tbl = StepLength().load(subject=1, as_table=true);

    % Load a specific version by record_id
    var = StepLength().load(version="a3f8c2e1b9d04710...");

    % Load from a specific database
    var = StepLength().load(db=my_db, subject=1, session="A");
    ```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `version` | `"latest"` | `"latest"` or a specific `record_id` |
| `loc` | `None` | Label-based index selection (Python only) |
| `iloc` | `None` | Integer-position index selection (Python only) |
| `as_table` | `False` | Return a DataFrame/table when multiple records match |
| `db` | `None` | `DatabaseManager` to use instead of the global database |
| `**metadata` | — | Metadata to filter by |

**Returns:** Single variable, list of variables, or DataFrame/table

!!! tip "Loaded variables carry lineage"
    Pass the loaded variable (not `var.data`) to a thunked function to preserve lineage tracking across pipeline steps.

---

## `load_all()`

Loads all matching variables. Returns a memory-efficient generator by default, or a DataFrame with `as_df=True`.

=== "Python"

    ```python
    # Iterate over all records (generator — memory-efficient)
    for var in StepLength.load_all(session="A"):
        print(var.metadata["subject"], var.data)

    # Load all into a DataFrame
    df = StepLength.load_all(as_df=True)
    #   subject  session  data
    #   1        A        [0.65, 0.72]
    #   2        A        [0.71, 0.68]
    #   ...

    # Filter to specific subjects
    for var in StepLength.load_all(subject=[1, 2, 3]):
        process(var.data)

    # Load only the latest version per parameter set
    df = StepLength.load_all(as_df=True, version_id="latest")

    # Include record_id column for traceability
    df = StepLength.load_all(as_df=True, include_record_id=True)
    ```

=== "MATLAB"

    ```matlab
    % Load all records (returns array of ThunkOutput)
    results = StepLength().load_all(session="A");
    for i = 1:numel(results)
        fprintf('%d: %s\n', results(i).metadata.subject, mat2str(size(results(i).data)));
    end

    % Load as MATLAB table
    tbl = StepLength().load_all(as_table=true);

    % Filter to specific subjects (array = "match any")
    results = StepLength().load_all(subject=[1 2 3]);

    % Latest version only
    results = StepLength().load_all(version_id="latest");
    ```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `as_df` | `False` | Return a DataFrame instead of a generator (Python) |
| `as_table` | `false` | Return a MATLAB table (MATLAB) |
| `include_record_id` | `False` | Include `record_id` column in DataFrame (Python, `as_df=True` only) |
| `version_id` | `"all"` | `"all"`, `"latest"`, an integer, or list of integers |
| `db` | `None` | `DatabaseManager` to use instead of the global database |
| `**metadata` | — | Metadata to filter by; list values match any element (OR semantics) |

---

## `list_versions()`

Lists all saved versions at a given schema location. Useful for seeing what computational variants exist (e.g., data computed with different parameter settings).

=== "Python"

    ```python
    versions = StepLength.list_versions(subject=1, session="A")
    for v in versions:
        print(v["record_id"][:16], v["created_at"])
        print("  schema:", v["schema"])    # {"subject": "1", "session": "A"}
        print("  version:", v["version"])  # {"low_hz": "20", "high_hz": "450"}
    ```

=== "MATLAB"

    ```matlab
    versions = StepLength().list_versions(subject=1, session="A");
    for i = 1:numel(versions)
        fprintf('%s  %s\n', versions(i).record_id, versions(i).created_at);
        disp(versions(i).schema);    % struct: subject, session
        disp(versions(i).version);   % struct: version keys
    end
    ```

**Returns (Python):** `list[dict]` with keys `record_id`, `schema`, `version`, `created_at`

**Returns (MATLAB):** struct array with same fields

---

## `save_from_dataframe()` / `save_from_table()`

Bulk-saves each row of a DataFrame/table as a separate database record. Much faster than calling `save()` in a loop. Use this when a DataFrame contains multiple independent data items, each with its own metadata.

=== "Python"

    ```python
    # DataFrame with results for multiple subjects/trials
    #   subject  trial  value
    #   1        1      0.52
    #   1        2      0.61
    #   2        1      0.48

    record_ids = ScalarResult.save_from_dataframe(
        df=results_df,
        data_column="value",
        metadata_columns=["subject", "trial"],
        experiment="exp1",   # common metadata applied to all rows
    )
    # Creates 3 separate database records
    ```

=== "MATLAB"

    ```matlab
    % MATLAB table with same structure
    record_ids = ScalarResult().save_from_table( ...
        results_tbl, ...            % MATLAB table
        "value", ...                % data column name
        ["subject", "trial"], ...   % metadata column names
        experiment="exp1");         % common metadata for all rows
    ```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `df` / `tbl` | DataFrame or MATLAB table |
| `data_column` | Column name containing the data to store |
| `metadata_columns` | Column names to use as per-row metadata |
| `db` | Optional `DatabaseManager` |
| `**common_metadata` | Additional metadata applied to all rows |

**Returns:** list of `record_id` strings

---

## `to_csv()`

Exports a single loaded variable's data to a CSV file.

=== "Python"

    ```python
    var = StepLength.load(subject=1, session="A")
    var.to_csv("step_length_s1_A.csv")
    ```

=== "MATLAB"

    Use Python's `export_to_csv` via the database manager for bulk exports (see [Database API](database.md)).

---

## Loaded Variable Attributes

After `save()` or `load()`, a variable instance has the following attributes:

=== "Python"

    | Attribute | Type | Description |
    |-----------|------|-------------|
    | `data` | any | The native Python data (numpy array, scalar, DataFrame, etc.) |
    | `record_id` | `str` | Deterministic hash identifying this record |
    | `metadata` | `dict` | All metadata key-value pairs |
    | `content_hash` | `str` | SHA-256 hash of the data content |
    | `lineage_hash` | `str \| None` | Hash of the computation that produced this (None for raw saves) |

=== "MATLAB"

    | Property | Type | Description |
    |----------|------|-------------|
    | `data` | MATLAB type | The native MATLAB data (numeric array, table, etc.) |
    | `record_id` | `string` | Deterministic hash identifying this record |
    | `metadata` | `struct` | Metadata key-value pairs |
    | `content_hash` | `string` | SHA-256 hash of the data content |
    | `lineage_hash` | `string` | Hash of the computation that produced this (empty if raw) |

---

## Subclassing for Shared Serialization

When multiple variable types store the same kind of data but should live in separate tables, define a base type with the serialization logic and create empty subclasses:

=== "Python"

    ```python
    class TimeSeries(BaseVariable):
        """Base type for all time series signals (stored as DataFrames)."""

        def to_db(self) -> pd.DataFrame:
            return self.data

        @classmethod
        def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
            return df

    # Each subclass gets its own table and inherits to_db/from_db
    class EMGSignal(TimeSeries):
        pass  # table: EMGSignal

    class ForceSignal(TimeSeries):
        pass  # table: ForceSignal
    ```

=== "MATLAB"

    ```matlab
    % Base type (in TimeSeries.m) — serialization handled by Python
    classdef TimeSeries < scidb.BaseVariable
    end

    % Subclasses in their own files
    classdef EMGSignal < TimeSeries
    end

    classdef ForceSignal < TimeSeries
    end
    ```

---

## Reserved Metadata Keys

These keys are used internally and cannot appear in `save()` metadata:

`record_id`, `id`, `created_at`, `schema_version`, `index`, `loc`, `iloc`

Using them raises `ReservedMetadataKeyError`.
