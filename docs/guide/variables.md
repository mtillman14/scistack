# Defining Variables

<!-- Ground truth (tests/source win over prose). Verified against:
     scidb/src/scidb/variable.py (BaseVariable.schema_version default 1; default to_db/from_db;
       save(data, index=None, db=None, **metadata)->record_id (list when DataFrame has schema-key
       columns); save_from_dataframe(df, data_column, metadata_columns, db=None, **common_metadata)
       ->list[str]; load(as_df=False, version="latest", where=None, db=None, introspect=False,
       **metadata) -> BaseVariable | list | DataFrame; _reserved_keys);
     scidb/src/scidb/database.py:1801-1809 (loaded instance attrs: record_id, metadata,
       content_hash, lineage_hash, branch_params); add_to_var_group/remove_from_var_group/
       list_var_groups/get_var_group;
     scidb/tests/test_integration.py, test_introspect.py, test_load_all_ordering.py
       (TestData.load(version="all")), test_where.py (load(where=...)).
     NOTE: there is NO `load_all` method and NO `include_record_id` param — use load(...)
     with version="all"/where=/as_df=/introspect=. -->

This guide covers the practical mechanics of defining, saving, loading, and
organizing variables. For the conceptual model behind them, see
[Variables & Storage](../concepts/variables.md).

## Define a variable type

A variable is a subclass of `BaseVariable`. Set `schema_version` (it defaults to
`1`, but declaring it makes structural versions explicit):

```python
from scidb import BaseVariable

class StepLength(BaseVariable):
    schema_version = 1
```

Defining the class registers it; `configure_database(...)` auto-registers all
known variable types. Bump `schema_version` whenever you change the data's
structure so old and new records don't collide.

## Native vs. custom serialization

For scalars, numpy arrays, lists, dicts, and pandas DataFrames you write **no
serialization code** — they round-trip natively into queryable DuckDB types:

```python
class ScalarValue(BaseVariable): schema_version = 1
class ArrayValue(BaseVariable):  schema_version = 1

ScalarValue.save(3.14, subject=1)
ArrayValue.save(np.array([1, 2, 3]), subject=1)
```

Override `to_db` / `from_db` only for a custom column layout or a domain-specific
object. If you override one, override both:

```python
class RotationMatrix(BaseVariable):
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        rows, cols = self.data.shape
        return pd.DataFrame({
            "row": np.repeat(range(rows), cols),
            "col": np.tile(range(cols), rows),
            "value": self.data.flatten(),
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        df = df.sort_values(["row", "col"])
        return df["value"].values.reshape(df["row"].max() + 1, df["col"].max() + 1)
```

A MATLAB variable overrides the same pair on its `classdef`: an ordinary `to_db(obj)` returning a `table`, and a `Static` `from_db(tbl)`. See [Saving your own classes](../getting-started/writing-functions.md#saving-your-own-classes).

## Save data

`save` stores a value at the coordinates given by keyword metadata and returns its
`record_id`:

```python
record_id = StepLength.save(np.array([0.65, 0.72]), subject=1, session="A")
```

A few rules:

- **Reserved keys** can't be used as metadata: `record_id`, `id`, `created_at`,
  `schema_version`, `index`, `loc`, `iloc` — using one raises
  `ReservedMetadataKeyError`.
- **A DataFrame with dataset-schema-key columns auto-distributes**: each row is
  saved as its own record and `save` returns a `list` of record ids.
- Re-saving identical data at the same coordinates returns the **same** id (it
  dedups); different data creates a new version.

### Many records from one DataFrame

When each row is an independent item (a value per subject/trial), use
`save_from_dataframe`, naming the data column and which columns are metadata:

```python
record_ids = ScalarValue.save_from_dataframe(
    df=results_df,
    data_column="Value",
    metadata_columns=["subject", "trial"],
    experiment="exp1",   # common metadata applied to every row
)
```

## Load data

`load` returns a single `BaseVariable` when one record matches, a `list` when
several match, or a `DataFrame` with `as_df=True`. It raises `NotFoundError` when
nothing matches. Metadata matching is partial, and a list value means "match
any":

```python
var   = StepLength.load(subject=1, session="A")   # one match -> a variable
many  = StepLength.load(subject=1)                # many matches -> a list
some  = StepLength.load(session=["A", "B"])       # match-any across sessions
frame = StepLength.load(subject=1, as_df=True)    # a DataFrame
```

Use `version=` to control which version(s) you get:

```python
StepLength.load(subject=1, session="A")                  # latest (default)
StepLength.load(subject=1, session="A", version="all")   # every version, as a list
StepLength.load(version=some_record_id)                  # one specific record
```

Filter by other variables' values with `where=` (see
[Filtering & Selection](filters.md)), and pass `introspect=True` to attach
internal fields (`.where`, `.version_mode`) or, in `as_df=True` mode, append
introspection columns (`record_id`, `branch_params`, `content_hash`, …).

## Inspect a loaded variable

A loaded instance carries its value and provenance:

```python
var = StepLength.load(subject=1, session="A")
var.data          # the native value
var.record_id     # content-addressed id
var.metadata      # {"subject": 1, "session": "A"}
var.content_hash  # hash of the data content
var.lineage_hash  # lineage hash (None if saved without lineage)
var.branch_params # constant-variant parameters, if any
```

## Specialized types via subclassing

Each `BaseVariable` subclass gets its **own table**, so subclassing is how you
split one logical kind into separate stored types:

```python
class TimeSeries(BaseVariable):
    schema_version = 1

class Temperature(TimeSeries): pass   # own table
class Humidity(TimeSeries): pass      # own table

Temperature.save(temp_array, sensor=1, day="monday")
Humidity.save(humidity_array, sensor=1, day="monday")
```

Subclasses inherit any custom `to_db` / `from_db` from the parent.

## Organize with variable groups

Group related variable types into named collections that persist in the database.
The methods live on the database handle (`from scidb import get_database`), and
accept variable classes or name strings:

```python
db = get_database()

db.add_to_var_group("kinematics", [StepLength, StepTime])   # classes…
db.add_to_var_group("kinematics", "StepWidth")              # …or names

db.list_var_groups()              # ["kinematics", …]
db.get_var_group("kinematics")    # [<StepLength>, <StepTime>, <StepWidth>] (sorted by name)

db.remove_from_var_group("kinematics", StepTime)
```

Adding the same variable twice is idempotent. Variable groups are also available
from MATLAB through the `scidb.*` wrappers — see [MATLAB Setup](../matlab-setup.md).

**Next:** [Database & Configuration](database.md) ·
[Batch Processing (for_each)](for_each.md) ·
[Filtering & Selection](filters.md) · [API: Variables](../api/variables.md)
