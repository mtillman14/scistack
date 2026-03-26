# Variables

Variables are the core data type in SciStack. Every piece of data you store is wrapped in a `BaseVariable` subclass.

## Defining a Variable Type

For most data types (scalars, numpy arrays, lists, dicts), **no serialization methods are needed** — SciDuck handles them natively with proper DuckDB types:

```python
from scidb import BaseVariable

class MyVariable(BaseVariable):
    pass  # schema_version defaults to 1
```

### Optional: Custom Serialization

Override `to_db()` and `from_db()` only when you need custom multi-column serialization (e.g., pandas DataFrames, domain-specific objects):

```python
import pandas as pd

class CustomVariable(BaseVariable):

    def to_db(self) -> pd.DataFrame:
        """Convert self.data to a DataFrame for storage."""
        ...

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Any:
        """Convert DataFrame back to native type."""
        ...
```

### Required Components

| Component        | Purpose                                                      |
|------------------|--------------------------------------------------------------|
| `schema_version` | *Optional.* Integer version for schema migrations (defaults to 1) |
| `to_db()`        | *Optional.* Instance method converting `self.data` to `pd.DataFrame` |
| `from_db()`      | *Optional.* Class method converting `pd.DataFrame` to native type    |

## Common Patterns

### Native Storage (No to_db/from_db Needed)

These types are handled automatically by SciDuck:

```python
class ScalarValue(BaseVariable):
    pass

class ArrayValue(BaseVariable):
    pass

class DictValue(BaseVariable):
    pass

# Usage
ScalarValue.save(3.14, subject=1)
ArrayValue.save(np.array([1, 2, 3]), subject=1)
DictValue.save({"key": "value"}, subject=1)
```

### Custom Serialization Examples

#### 1D Arrays with Index

```python
class IndexedArray(BaseVariable):

    def to_db(self) -> pd.DataFrame:
        return pd.DataFrame({
            "index": range(len(self.data)),
            "value": self.data
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        return df.sort_values("index")["value"].values
```

#### 2D Arrays / Matrices

```python
class MatrixValue(BaseVariable):

    def to_db(self) -> pd.DataFrame:
        rows, cols = self.data.shape
        return pd.DataFrame({
            "row": np.repeat(range(rows), cols),
            "col": np.tile(range(cols), rows),
            "value": self.data.flatten()
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        df = df.sort_values(["row", "col"])
        rows = df["row"].max() + 1
        cols = df["col"].max() + 1
        return df["value"].values.reshape(rows, cols)
```

#### DataFrames

```python
class DataFrameValue(BaseVariable):

    def to_db(self) -> pd.DataFrame:
        return self.data  # Already a DataFrame

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df
```

## Specialized Types via Subclassing

When one variable class can represent multiple logical data types, create subclasses to store each in separate tables:

```python
class TimeSeries(BaseVariable):
    pass

# Create specialized types - each gets its own table
class Temperature(TimeSeries):
    """Temperature time series data."""
    pass  # Table: Temperature

class Humidity(TimeSeries):
    """Humidity time series data."""
    pass  # Table: Humidity

class Pressure(TimeSeries):
    """Pressure time series data."""
    pass  # Table: Pressure
```

Each subclass:
- Inherits `to_db()` and `from_db()` from the parent (if defined)
- Gets its own table named after the class (exact class name, e.g., `"Temperature"`)
- Can define custom methods specific to that data type

## Instance Properties

After `save()` or `load()`:

```python
# Save returns the record_id
record_id = MyVariable.save(data, subject=1)

# Load returns a variable instance with populated properties
var = MyVariable.load(subject=1)
var.data          # The native data
var.record_id     # Content hash (set after load)
var.metadata      # Metadata dict (set after load)
var.content_hash  # Hash of data content
var.lineage_hash  # Lineage hash (None for raw data)
```

## Batch Operations: DataFrames with Multiple Records

When a DataFrame contains multiple independent data items (e.g., one row per subject/trial), use `save_from_dataframe()` and `load_all(as_df=True)`:

### Saving Each Row Separately

```python
# DataFrame with results for multiple subjects/trials
#   Subject  Trial  Value
#   1        1      0.52
#   1        2      0.61
#   2        1      0.48
#   2        2      0.55

class ScalarResult(BaseVariable):
    pass

# Save each row as a separate record
record_ids = ScalarResult.save_from_dataframe(
    df=results_df,
    data_column="Value",
    metadata_columns=["Subject", "Trial"],
    experiment="exp1"  # Additional common metadata
)
# Creates 4 separate database records
```

### Loading Back to DataFrame

```python
# Load all records matching criteria
df = ScalarResult.load_all(experiment="exp1", as_df=True)
#   Subject  Trial  data
#   1        1      0.52
#   1        2      0.61
#   2        1      0.48
#   2        2      0.55

# Include record_id for traceability
df = ScalarResult.load_all(experiment="exp1", as_df=True, include_record_id=True)
#   Subject  Trial  data   record_id
#   1        1      0.52   abc123...
#   ...
```

### When to Use Each Pattern

| Scenario | Method |
|----------|--------|
| DataFrame is ONE unit of data (e.g., time series) | `MyVar.save(df, ...)` |
| Each row is SEPARATE data (e.g., subject/trial results) | `MyVar.save_from_dataframe(df, ...)` |
| Load single result (latest version) | `MyVar.load(...)` |
| Load all matching as generator | `MyVar.load_all(...)` |
| Load all matching as DataFrame | `MyVar.load_all(..., as_df=True)` |

## Metadata Reflects Dataset Structure

The metadata keys you use in `save()` should reflect the natural structure of your dataset. Common patterns include subject/trial designs, session-based recordings, or hierarchical experimental structures.

### Example: Subject × Trial Design

```python
class TrialResult(BaseVariable):
    pass

# Save results for each subject and trial
subjects = [1, 2, 3]
trials = ["baseline", "treatment", "followup"]

for subject in subjects:
    for trial in trials:
        # Process data for this subject/trial
        result = analyze_trial(subject, trial)

        # Metadata mirrors dataset structure
        TrialResult.save(result,
            subject=subject,
            trial=trial,
            experiment="exp_2024"
        )

# Later: load specific combinations
baseline_s1 = TrialResult.load(subject=1, trial="baseline")

# Iterate over all baselines (generator)
for var in TrialResult.load_all(trial="baseline"):
    print(var.metadata["subject"], var.data)
```

### Example: Session-Based Recordings

```python
class Recording(BaseVariable):
    pass

sessions = ["morning", "afternoon", "evening"]
days = ["day1", "day2", "day3"]

for day in days:
    for session in sessions:
        data = record_session(day, session)
        Recording.save(data, day=day, session=session, device="sensor_A")
```

The key insight: your metadata structure should make it easy to query the data the way you'll need to access it later.

## Reserved Metadata Keys

These keys cannot be used in metadata:

- `record_id` - Reserved for version hash
- `id` - Reserved for database ID
- `created_at` - Reserved for timestamp
- `schema_version` - Reserved for schema version
- `index` - Reserved for DataFrame index parameter
- `loc` - Reserved for label-based indexing parameter
- `iloc` - Reserved for integer-position indexing parameter

Using these raises `ReservedMetadataKeyError`.

## Variable Groups

Variable groups let you organize variable types into named collections. Groups are stored in the database and persist across sessions.

### Creating / Adding to a Group

You can pass either variable classes or name strings (or a mix):

```python
db = get_database()

# Using classes
db.add_to_var_group("kinematics", StepLength)
db.add_to_var_group("kinematics", [StepLength, StepWidth, StepTime])

# Using name strings
db.add_to_var_group("kinematics", "StepLength")
db.add_to_var_group("kinematics", ["StepLength", "StepWidth", "StepTime"])
```

Adding the same variable to the same group twice is idempotent (no duplicates).

### Listing Groups

```python
# List all group names
groups = db.list_var_groups()
# ["kinematics", "emg", "demographics"]
```

### Getting Variables in a Group

```python
# Get all variable classes in a group
variables = db.get_var_group("kinematics")
# [<class 'StepLength'>, <class 'StepTime'>, <class 'StepWidth'>]

# Use them directly
for var_class in db.get_var_group("raw_signals"):
    for var in var_class.load_all(subject=1):
        process(var.data)
```

The returned list is sorted alphabetically by class name.

### Removing from a Group

Accepts classes or strings, same as `add_to_var_group`:

```python
# Remove a single variable
db.remove_from_var_group("kinematics", StepTime)

# Remove multiple variables
db.remove_from_var_group("kinematics", ["StepLength", "StepWidth"])
```

### MATLAB Usage

Use the `scidb.*` wrapper functions. `add_to_var_group` and `remove_from_var_group` accept a cell array of BaseVariable objects, a cell array of chars, or a string array:

```matlab
% Cell array of BaseVariable objects
scidb.add_to_var_group("kinematics", {StepLength(), StepWidth(), StepTime()})

% Cell array of chars
scidb.add_to_var_group("kinematics", {'StepLength', 'StepWidth', 'StepTime'})

% String array
scidb.add_to_var_group("kinematics", ["StepLength", "StepWidth", "StepTime"])

% Single variable
scidb.add_to_var_group("kinematics", "StepLength")

% Get returns a cell array of BaseVariable instances
vars = scidb.get_var_group("kinematics");
% {[StepLength], [StepTime], [StepWidth]}
for i = 1:numel(vars)
    data = vars{i}.load(subject=1);
end

% List group names and remove
groups = scidb.list_var_groups();
scidb.remove_from_var_group("kinematics", "StepTime")
```
