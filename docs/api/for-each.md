# Batch Processing API — `for_each`

<!-- Ground truth (source/tests win over prose). Verified against:
     scifor/src/scifor/foreach.py for_each(fn, inputs, outputs=None, dry_run=False, as_table=None,
       distribute=False, where=None, **metadata_iterables) -> DataFrame|None
       (standalone: DataFrames/constants, outputs= are column NAMES (list[str] | int), NO save);
     scidb/src/scidb/foreach.py for_each(fn, inputs, outputs, dry_run=False, save=True,
       as_table=None, db=None, distribute=False, where=None, introspect=False, **iterables);
     scihist/src/scihist/foreach.py for_each(fn, inputs, outputs, dry_run=False, save=True,
       as_table=None, db=None, distribute=False, where=None, skip_computed=True,
       schema_filter=None, schema_level=None, **iterables);
     wrappers: scidb Fixed/Variant/Merge/ColumnSelection/ColName/EachOf, scifor PathInput/
       PathOutput/Col; BaseVariable.for_columns().
     NOTE: standalone scifor takes DataFrames + NO outputs=; scidb/scihist take variable TYPES
     + outputs=[...]. skip_computed is scihist-only. Empty list [] = all DB values. -->

`for_each` runs a function over every combination of conditions. For concepts and
examples see the [Batch Processing guide](../guide/for_each.md). The same call
exists at three layers — pick by what you pass.

---

## `for_each()` — database-backed (scidb / scihist)

```python
# scidb
for_each(fn, inputs, outputs, dry_run=False, save=True, as_table=None, db=None,
         distribute=False, where=None, introspect=False, **metadata_iterables)

# scihist (adds lineage + staleness)
for_each(fn, inputs, outputs, dry_run=False, save=True, as_table=None, db=None,
         distribute=False, where=None, skip_computed=True, schema_filter=None,
         schema_level=None, **metadata_iterables) -> DataFrame | None
```

| Parameter | Meaning |
|---|---|
| `fn` | the function to run per combination |
| `inputs` | dict: param name → variable **type**, wrapper, or constant value |
| `outputs` | list of output variable types to save results as |
| `**metadata_iterables` | condition lists, e.g. `subject=[1, 2, 3]`; `[]` means *all values in the DB* |
| `dry_run` | print what would load/run/save without executing |
| `save` | if `False`, return the result DataFrame without writing |
| `as_table` | keep schema-key columns in the input DataFrames (`True`, or a list of keys) |
| `db` | target a specific database |
| `distribute` | expand a vector return into deeper-level records |
| `where` | row filter (see [Filters](filters.md)) |
| `introspect` | *(scidb)* attach introspection columns to the result |
| `skip_computed` | *(scihist)* skip already-current combos; default `True` |
| `schema_filter`, `schema_level` | *(scihist)* restrict/aggregate over schema keys |

=== "Python"
    ```python
    from scihist import for_each
    for_each(bandpass, inputs={"signal": RawSignal, "low_hz": 20},
             outputs=[FilteredSignal], subject=[1, 2, 3], session=["A", "B"])
    ```
=== "MATLAB"
    ```matlab
    scidb.for_each(@bandpass, struct('signal', RawSignal(), 'low_hz', 20), ...
        {FilteredSignal()}, subject=[1 2 3], session=["A" "B"]);
    ```

Iterating over only some schema keys aggregates the deeper levels into each call.

---

## `for_each()` — standalone (scifor)

```python
for_each(fn, inputs, outputs=None, dry_run=False, as_table=None, distribute=False,
         where=None, **metadata_iterables) -> DataFrame | None
```

Operates on plain DataFrames and constants — **no** `outputs=`, nothing saved;
returns the result table. Declare the schema first with `set_schema(...)`.

```python
from scifor import set_schema, for_each
set_schema(["subject", "session"])
df = for_each(analyze, inputs={"emg": data_table, "cutoff_hz": 20},
              subject=[1, 2, 3], session=["pre", "post"])
```

---

## Input wrappers

Wrap a variable-type input to change what gets loaded.

### `Fixed`
```python
Fixed(Var, **metadata)
```
Load this input from fixed metadata instead of the current iteration (e.g. a
baseline): `Fixed(StepLength, session="BL")`.

### `Merge`
```python
Merge(A, B, ...)
```
Inner-join several variables column-wise into one table input per iteration.
Constituents may be `Fixed`/column selections. Has `.to_csv(path)` and
`.as_df(where=None, **metadata)` for export (see [Browsing](../guide/browsing.md)).

### Column selection { #column-selection }
```python
Var["col"]          # one column  -> array
Var[["a", "b"]]     # subset      -> DataFrame
Var.for_columns(columns=[])   # run once per column, reassembled into one output
```
The same indexing builds column filters (`Var["col"] != 0`) for `where=`. In
MATLAB use the constructor form `Var("col")`.

### `EachOf`
```python
EachOf(a, b, ...)
```
Declare alternatives for a parameter (variable types, constants, or `where=`
filters). Each becomes a saved variant; multiple `EachOf` axes multiply.

### `Variant`
```python
Variant(Var, **branch_params)
```
Pin an input to a specific `branch_params` variant at load time, e.g.
`Variant(Filtered, low_hz=20)`.

### `PathInput` / `PathOutput`
- **`PathInput`** — locate *existing* files by substituting metadata into a path
  template (discovery, regex, per-combo loading).
- **`PathOutput`** — a pure output path template; substitutes the combo's metadata
  (and the `for_columns` column via `{ColName}`) and passes the resolved path to
  the function to write.

---

## Return value

All three return a pandas DataFrame (one row per combination, metadata columns
then outputs) — or `None` when there is nothing to return. The database-backed
forms also save outputs unless `save=False`.

**See also:** [Variables](variables.md) · [Filters](filters.md) ·
[Database](database.md) · [Guide: Batch Processing](../guide/for_each.md)
