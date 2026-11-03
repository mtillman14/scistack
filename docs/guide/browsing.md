# Browsing & Exporting

<!-- Ground truth (tests/source win over prose). Verified against:
     scidb/src/scidb/model/variable.py:661 BaseVariable.to_csv(filename, *args, **kwargs) CLASSMETHOD —
       flat table, one row per schema_id, value column named after the class; filename must end
       ".csv"; supports **metadata filters and where=; ValueError for multi-row table / bare vector;
     scidb/tests/test_to_csv.py (ScalarValue.to_csv(path); .to_csv(path, subject=1);
       .to_csv(path, where=_Side=="L"); filename must end .csv);
     scidb/src/scidb/store/database.py:4017 db.export_to_csv(variable_class, path, **metadata) -> int
       (raw to_db() rows + _record_id + _meta_<key> columns; NotFoundError if none);
     scifor/src/scifor/merge.py Merge(a,b).to_csv(path, ...) and .as_df(where=None, verbose=False,
       **metadata) — inner-join on shared schema columns; scifor Col("col") filters;
     scifor/tests/test_merge_as_df.py, test_merge_to_csv.py.
     NOTE: to_csv is a CLASSMETHOD (VarClass.to_csv(...)), NOT an instance method var.to_csv();
     data is one DuckDB file with native types (DBeaver-inspectable). Do not invent column
     names for the underlying tables. -->

Your data lives in one DuckDB file with native, queryable types, so you can read
it three ways: browse it directly, export flat CSVs, or pull DataFrames in Python.

## Browse the database directly

Open the `.duckdb` file in [DBeaver](https://dbeaver.io/) or any
DuckDB-compatible viewer. Each variable type has its own table (`{ClassName}_data`),
and because values are stored as native DuckDB types (`LIST`, nested `LIST`,
`JSON`) you can read them without any export step. This is the quickest way to
sanity-check what a pipeline wrote.

## Export a variable to CSV

`to_csv` is a **classmethod on the variable** — call it on the class, not on a
loaded instance. It loads every matching record and writes a flat table: one row
per schema location, one column per schema key, plus a value column named after
the variable.

```python
ScalarValue.to_csv("scalars.csv")                       # all records
ScalarValue.to_csv("subject1.csv", subject=1)           # filter by metadata
ScalarValue.to_csv("left.csv", where=Side == "L")       # filter by value
```

A subject-level variable produces one row per subject (no `trial` column); a
trial-level variable, one row per subject/trial. A few rules from the
implementation:

- The filename **must end in `.csv`** (otherwise `ValueError`).
- The layout is **one row per schema location**. A record may carry multiple
  *columns* (a single-row table writes one column per table column), but a record
  holding a multi-row table or a bare vector can't be flattened this way and
  raises `ValueError`.

### Many records, raw layout

When you want every record's full `to_db()` rows stacked (rather than one row per
location), use the database handle's `export_to_csv`. It writes each record's rows
with added `_record_id` and `_meta_<key>` columns and returns the count:

```python
n = db.export_to_csv(TimeSeries, "all_timeseries.csv", experiment="exp1")
```

It raises `NotFoundError` if nothing matches.

## Export merged variables (joined)

To export several variables side by side, join them with `Merge` and call
`to_csv` on the result. The constituents are joined on their shared schema
columns, so the schema keys aren't duplicated. Every location must be present in
every constituent: a row on one side only is an error naming it rather than a
silent drop (narrow both with metadata or `where=` first). A coarser variable
(one value per subject) repeats across the finer one's rows:

```python
from scidb import Merge

Merge(StepLength, Speed).to_csv("gait.csv")
```

## Pull a DataFrame in Python

For programmatic analysis without a file round-trip, get a DataFrame directly:

- **A merged join** → `Merge(...).as_df()`, which returns the same frame `to_csv`
  would have written, and accepts a `where=` filter:

  ```python
  from scifor import Col
  df = Merge(StepLength, Speed).as_df(where=Col("StepLength") > 0.55)
  ```

- **A single variable** → `load(..., as_df=True)` (see
  [Defining Variables](variables.md)):

  ```python
  df = StepLength.load(subject=1, as_df=True)
  ```

- **A `for_each` result** → run with `save=False` and it returns the result table
  for inspection (see [Batch Processing](for_each.md)).

## Which to use

| Goal | Use |
|---|---|
| Eyeball what's stored | DBeaver / DuckDB viewer on the `.duckdb` file |
| One flat CSV per variable | `VariableClass.to_csv("out.csv", …)` |
| Several variables joined into one CSV | `Merge(A, B).to_csv("out.csv")` |
| Every record's raw rows to CSV | `db.export_to_csv(VariableClass, "out.csv", …)` |
| A DataFrame in Python | `Merge(...).as_df()` · `load(as_df=True)` · `for_each(save=False)` |

**Next:** [Defining Variables](variables.md) ·
[Batch Processing (for_each)](for_each.md) ·
[Filtering & Selection](filters.md) · [Database & Configuration](database.md)
