# Variables API — `BaseVariable`

<!-- Ground truth (source/tests win over prose). Verified against:
     scidb/src/scidb/variable.py:
       schema_version: int = 1; _reserved_keys = {record_id,id,created_at,schema_version,
         index,loc,iloc};
       to_db(self)->DataFrame (default wraps {"value":[self.data]}); from_db(cls, df) (default
         unwraps "value"); __class_getitem__ -> ColumnSelection; for_columns(columns=[]);
       save(data, index=None, db=None, **metadata)->str (list[str] if DataFrame has schema-key
         cols); save_from_dataframe(df, data_column, metadata_columns, db=None, **common_metadata)
         ->list[str]; load(as_df=False, version="latest", where=None, db=None, introspect=False,
         **metadata)->BaseVariable|list|DataFrame (NotFoundError on miss); to_csv(filename, *args,
         **kwargs) classmethod (flat, one row per schema_id, .csv required);
     scidb/src/scidb/database.py:1801-1809 loaded instance attrs (record_id, metadata,
       content_hash, lineage_hash, branch_params);
     scimatlab/.../+scidb (classdef MyVar < scidb.BaseVariable; MyVar().save/.load).
     NOTE: NO load_all, NO ThunkOutput, NO include_record_id; saving does NOT record lineage
     (use scihist.save). -->

`BaseVariable` is the base class for every stored value. Subclass it once per kind
of result; the subclass name is its table name. For task-oriented usage see
[Defining Variables](../guide/variables.md); for the model, see
[Variables & Storage](../concepts/variables.md).

## Defining a type

=== "Python"
    ```python
    from scidb import BaseVariable

    class StepLength(BaseVariable):
        schema_version = 1
    ```
=== "MATLAB"
    ```matlab
    % In StepLength.m
    classdef StepLength < scidb.BaseVariable
    end
    ```

**`schema_version: int`** — defaults to `1`. Bump it when the data's structure
changes so old and new records remain distinct.

---

## `save()`

```python
@classmethod
save(data, index=None, db=None, **metadata) -> str | list[str]
```

Stores `data` at the coordinates in `**metadata` and returns the content-addressed
`record_id`. Saving identical data at identical coordinates returns the same id
(dedup); different data creates a new version.

- **`data`** — raw value (scalar, array, list, dict, DataFrame), or an existing
  `BaseVariable` instance to re-save.
- **`index`** — optional index applied to the DataFrame after `to_db()`.
- **`db`** — target a specific database instead of the global default.
- **`**metadata`** — addressing keys. Reserved keys (`record_id`, `id`,
  `created_at`, `schema_version`, `index`, `loc`, `iloc`) raise
  `ReservedMetadataKeyError`.
- **Returns** — `str`, or `list[str]` when `data` is a DataFrame whose columns
  include dataset schema keys (each row saved as its own record).

!!! note
    `save()` stores data only — it does **not** record lineage for a
    `LineageFcnResult`. Use [`scihist.save()`](lineage.md) to persist a tracked
    result with its provenance.

=== "Python"
    ```python
    rid = StepLength.save(np.array([0.65, 0.72]), subject=1, session="A")
    ```
=== "MATLAB"
    ```matlab
    rid = StepLength().save([0.65 0.72], subject=1, session="A");
    ```

---

## `save_from_dataframe()`

```python
@classmethod
save_from_dataframe(df, data_column, metadata_columns, db=None, **common_metadata) -> list[str]
```

Saves each row of `df` as a separate record. `data_column` names the value column,
`metadata_columns` names the per-row metadata columns, and `**common_metadata` is
applied to every row. Returns the list of record ids.

```python
ids = ScalarValue.save_from_dataframe(
    df=results_df, data_column="Value",
    metadata_columns=["subject", "trial"], experiment="exp1",
)
```

---

## `load()`

```python
@classmethod
load(as_df=False, version="latest", where=None, db=None, introspect=False, **metadata)
    -> BaseVariable | list[BaseVariable] | DataFrame
```

Returns a single `BaseVariable` when one record matches, a `list` when several do,
or a `DataFrame` when `as_df=True`. Raises `NotFoundError` if nothing matches.

- **`version`** — `"latest"` (default), `"all"` (every version, as a list), or a
  specific `record_id`.
- **`where`** — a filter on other variables' values (see [Filters](filters.md)).
- **`as_df`** — return a DataFrame (metadata columns + `data`).
- **`introspect`** — attach `.where` / `.version_mode` to instances, or append
  introspection columns (`record_id`, `branch_params`, `content_hash`, …) to a
  DataFrame result.
- **`**metadata`** — addressing keys; matching is partial, and a list value means
  "match any".

=== "Python"
    ```python
    var   = StepLength.load(subject=1, session="A")        # one -> instance
    many  = StepLength.load(subject=1)                     # many -> list
    allv  = StepLength.load(subject=1, version="all")      # full history
    frame = StepLength.load(subject=1, as_df=True)         # DataFrame
    ```
=== "MATLAB"
    ```matlab
    var = StepLength().load(subject=1, session="A");
    ```

There is no `load_all` method — use `load(version="all")` or `load(where=...)`.

### Loaded instance attributes

| Attribute | Meaning |
|---|---|
| `.data` | the native value |
| `.record_id` | content-addressed id |
| `.metadata` | addressing keys, e.g. `{"subject": 1, "session": "A"}` |
| `.content_hash` | hash of the data content |
| `.lineage_hash` | lineage hash, or `None` if saved without lineage |
| `.branch_params` | constant-variant parameters, if any |

---

## `to_csv()`

```python
@classmethod
to_csv(filename, *args, **kwargs) -> None
```

Exports the variable to a flat CSV: one row per schema location, one column per
schema key, plus a value column named after the class. `**kwargs` accepts metadata
filters and `where=`. The filename must end in `.csv`. A record holding a
multi-row table or a bare vector can't be flattened this way and raises
`ValueError`. See [Browsing & Exporting](../guide/browsing.md).

```python
StepLength.to_csv("steps.csv", subject=1, where=Side == "L")
```

---

## `to_db()` / `from_db()`

```python
def to_db(self) -> DataFrame          # default: pd.DataFrame({"value": [self.data]})

@classmethod
def from_db(cls, df) -> Any           # default: unwrap the "value" column
```

Override **both** only for custom multi-column serialization; scalars, arrays,
lists, dicts, and DataFrames round-trip with the defaults.

```python
class RotationMatrix(BaseVariable):
    schema_version = 1
    def to_db(self):
        r, c = self.data.shape
        return pd.DataFrame({"row": np.repeat(range(r), c),
                             "col": np.tile(range(c), r),
                             "value": self.data.flatten()})
    @classmethod
    def from_db(cls, df):
        df = df.sort_values(["row", "col"])
        return df["value"].values.reshape(df["row"].max()+1, df["col"].max()+1)
```

---

## Column selection

Indexing a class selects columns for a `for_each` input (it does not load data by
itself):

| Form | Result |
|---|---|
| `MyVar["col"]` | one column → the function receives an array |
| `MyVar[["a", "b"]]` | a subset → the function receives a DataFrame |
| `MyVar.for_columns()` | run once per column, reassembled into one output |
| `MyVar.for_columns({"A": ["a", "a_date"]})` | run once per group (a DataFrame); each group a named column of the one output |
| `MyVar.for_columns([["a", "a_date"], ["b", "b_date"]])` | run once per group; group *i* saved as `outputs[i]` |

The same indexing builds column filters (`MyVar["col"] != 0`) for `where=` — see
[Filters](filters.md). In MATLAB, use the constructor form `MyVar("col")`.

**See also:** [Database](database.md) · [Batch Processing](for-each.md) ·
[Filters](filters.md)
