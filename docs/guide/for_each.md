# Batch Processing (for_each)

<!-- Ground truth (tests/source win over prose). Verified against:
     scifor/src/scifor/foreach.py for_each(fn, inputs, outputs=None, dry_run=False, as_table=None,
       distribute=False, where=None, **metadata_iterables) -> DataFrame|None
       (standalone: DataFrames/constants only, outputs= are column NAMES (list[str] | int), NO save);
     scidb/src/scidb/foreach.py for_each(fn, inputs, outputs, dry_run=False, save=True,
       as_table=None, db=None, distribute=False, where=None, introspect=False, **iterables);
     scihist/src/scihist/foreach.py for_each(fn, inputs, outputs, dry_run=False, save=True,
       as_table=None, db=None, distribute=False, where=None, skip_computed=True,
       schema_filter=None, schema_level=None, **iterables);
     scidb/tests/test_aggregation.py (iterating a SUBSET of schema keys aggregates lower-level
       records into one call; subject=["S01","S02"] -> 2 rows each summing sessions);
     scidb/tests/test_each_of.py (subject=[] resolves to all DB values; EachOf(a,b) -> variants,
       cartesian across axes; EachOf on inputs and where=);
     scihist/tests/test_merge.py (Merge(A,B), Merge(A, Fixed(...))), test_fixed.py (Fixed(Var,
       session="BL")); scidb/tests/test_for_columns.py (Var.for_columns()).
     NOTE: standalone scifor takes DataFrames + NO outputs=; DB-backed scidb/scihist take
       variable TYPES + outputs=[...]. Constants are plain values in inputs={}. -->

`for_each` runs a function across every combination of your experimental
conditions — loading inputs, iterating, and saving outputs — so you write the
analysis once and never the loops. For the conceptual picture, see
[Architecture & Layers](../concepts/architecture.md).

## Which `for_each` to import

The same idea exists at three layers; pick by what you have:

| Import | Inputs are | Outputs | Use when |
|---|---|---|---|
| `from scifor import for_each` | plain DataFrames + constants | *none* — returns a DataFrame | data is in memory, no database |
| `from scidb import for_each` | variable **types** + constants | `outputs=[...]`, saved to DB | persisted results, no lineage |
| `from scihist import for_each` | variable **types** + constants | `outputs=[...]`, saved + lineage | full pipeline + `skip_computed` |

This guide uses the database-backed form (`scidb` / `scihist`); the standalone
note at the end covers `scifor`.

## The basic call

Replace nested loops:

```python
for subject in [1, 2, 3]:
    for session in ["A", "B"]:
        raw = RawSignal.load(subject=subject, session=session)
        FilteredSignal.save(bandpass(raw.data), subject=subject, session=session)
```

…with one declarative call:

=== "Python"
    ```python
    from scihist import for_each

    for_each(
        bandpass,                          # the function
        inputs={"signal": RawSignal},      # param name -> variable type to load
        outputs=[FilteredSignal],          # output types, saved automatically
        subject=[1, 2, 3],                 # condition iterables (cartesian product)
        session=["A", "B"],
    )
    ```
=== "MATLAB"
    ```matlab
    scidb.for_each(@bandpass, ...
        struct('signal', RawSignal()), ...   % param name -> instance
        {FilteredSignal()}, ...              % output types (cell array)
        subject=[1 2 3], session=["A" "B"]);
    ```

For each of the six combinations, `for_each` loads `RawSignal`, calls `bandpass`
with its data, and saves the return value as `FilteredSignal` at the same
coordinates. A combination with no input data is skipped (logged as `[skip]`)
rather than raising.

## Constants

Any input that isn't a variable type is passed through as a constant — and
recorded as a [variant discriminator](../concepts/caching.md) on the output:

```python
for_each(bandpass, inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 450},
         outputs=[FilteredSignal], subject=[1, 2, 3], session=["A", "B"])
```

## Iterate "all values" with `[]`

An empty iterable means *all values present in the database* for that key,
resolved at call time:

```python
for_each(bandpass, inputs={"signal": RawSignal}, outputs=[FilteredSignal],
         subject=[], session=[])      # every subject/session that has data
```

## Aggregate by iterating a subset of keys

If you iterate over only *some* of the schema keys, each call receives **all**
records at the deeper levels — letting you aggregate. With schema
`["subject", "session"]`, iterating `subject` only gives each call every session
for that subject:

```python
def mean_over_sessions(signal):     # signal holds all of one subject's sessions
    return float(np.mean(signal))

for_each(mean_over_sessions, inputs={"signal": RawSignal}, outputs=[SubjectMean],
         subject=["S01", "S02"])     # 2 calls; each aggregates that subject's sessions
```

## Input wrappers

Variable-type inputs can be wrapped to change *what* gets loaded:

- **`Fixed(Var, session="BL")`** — load this input from fixed metadata instead of
  the current iteration (e.g. always the baseline session):

  ```python
  for_each(change_from_baseline,
           inputs={"baseline": Fixed(StepLength, session="BL"),
                   "current":  StepLength},
           outputs=[Delta], subject=[], session=[])
  ```

- **`Merge(A, B)`** — combine several variables column-wise into one table input.
  Constituents may themselves be `Fixed`/column selections:

  ```python
  inputs={"gait": Merge(GaitData, ForceData)}
  ```

- **Column selection** — `Var["col"]` feeds one column (as an array);
  `Var[["a", "b"]]` feeds a subset (as a DataFrame); `Var.for_columns()` runs the
  function once per column and reassembles the results.

## Variants with `EachOf`

`EachOf(...)` declares alternatives for a parameter — each becomes a separate
saved variant, and multiple `EachOf` axes multiply:

```python
from scidb import EachOf

for_each(analyze,
         inputs={"data": MetricA, "alpha": EachOf(0.05, 0.01)},   # two variants
         outputs=[AnalysisResult], subject=[], session=[])
# 2 alpha values × the subjects with data -> that many result rows
```

`EachOf` also works on `where=` to run a filter several ways in one call.

## Filtering rows

Pass `where=` to restrict which records take part, using variable column filters
(see [Filtering & Selection](filters.md)):

```python
for_each(analyze, inputs={"x": StepLength}, outputs=[Result],
         where=(Side == "L"), subject=[], session=[])
```

## Useful options

- **`skip_computed=True`** (scihist default) — skip combinations whose output is
  already current; set `False` to force a full re-run. See
  [Caching Computations](caching.md).
- **`dry_run=True`** — print what would load, run, and save without executing.
- **`save=False`** — run and return the result DataFrame without writing to the
  database (handy for inspection).
- **`as_table=True`** — keep schema-key columns in the input DataFrames so the
  function can see the current combo's metadata.
- **`distribute=True`** — when a function returns a vector whose elements each
  belong at a deeper schema level, expand them into separate records.

## Standalone with `scifor`

When your data is already in DataFrames and you don't need a database, use
`scifor.for_each`: declare the schema once, pass DataFrames and constants as
`inputs`, and get a result DataFrame back. There is **no** `outputs=` and nothing
is saved:

```python
from scifor import set_schema, for_each

set_schema(["subject", "session"])
results = for_each(my_analysis,
                   inputs={"emg": data_table, "cutoff_hz": 20},
                   subject=[1, 2, 3], session=["pre", "post"])
```

**Next:** [Filtering & Selection](filters.md) ·
[Browsing & Exporting](browsing.md) · [Caching Computations](caching.md) ·
[API: Batch Processing](../api/for-each.md)
