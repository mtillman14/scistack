# Filtering & Selection

<!-- Ground truth (tests/source win over prose). Verified against:
     scidb/tests/test_filters.py (Side == "L" where Side is a BaseVariable subclass ->
       VariableFilter with .variable_class/.op/.value via metaclass comparison ops; raw_sql,
       schema_key imported from scidb.filters and exported from scidb);
     scidb/tests/test_where.py (where= in load(); ColumnFilter MyVar["col"] == val for tabular
       vars; & | ~ composition; returns filtered list);
     scidb/tests/test_exclusions.py (exclude_schema(**keys, reason=, db=), include_schema,
       list_exclusions; reason required; full-schema-key combo required; persisted);
     scidb/README.md (named filters are first-class values, composable; exclusions hashed into
       version_keys __schema_overrides_hash so adding/removing invalidates cache);
     scidb/src/scidb/__init__.py exports raw_sql, schema_key, exclude_schema, include_schema,
       list_exclusions.
     NOTE: where= works in load() and for_each(), NOT a `load_all` method (no such method).
     api/filters.md is the API-reference companion. -->

Filters let you load and process only the records you want, by the *values of
other variables*. The same `where=` filter works in both `load()` and
[`for_each()`](for_each.md). For the API-level reference, see
[Filters](../api/filters.md).

## Filter on a variable's value

Comparing a variable class to a value builds a filter — no data is fetched yet,
it's just a description of the condition:

```python
Side == "L"        # records where the Side variable equals "L"
Speed > 1.2        # records where Speed exceeds 1.2
StartFoot != "A"
```

Pass it as `where=` to select which records load:

```python
left = StepLength.load(where=Side == "L")            # only left-side records
fast = StepLength.load(where=Speed > 1.2)
```

Here `Side` and `Speed` are `BaseVariable` subclasses; the comparison operators on
the class produce a filter object (with the variable, the operator, and the
value).

## Filter on a column of a tabular variable

For a variable whose data is a table, index a column and compare it:

```python
GaitData["side"] == "L"
GaitData["speed"] >= 1.5
```

The same column expression also drives [column selection](for_each.md) in
`for_each` inputs — `MyVar["col"]` — so one syntax covers both "filter by this
column" and "feed this column".

## Combine and reuse filters

Filters are first-class values: assign them to names, and compose them with `&`
(and), `|` (or), `~` (not):

```python
clean = StepLength != 0                 # a reusable filter
unilateral = (Side == "L") | (Side == "R")

StepLength.load(where=clean & (Speed > 1.2))
StepLength.load(where=~(Side == "L"))

# Reuse the named filter anywhere
for_each(analyze, inputs={"x": StepLength}, outputs=[Result],
         where=clean & unilateral, subject=[], session=[])
```

!!! warning "MATLAB: use `&` / `|`, never `&&` / `||`"
    Filter composition relies on operator overloading, which MATLAB only allows
    for the element-wise `&` and `|`. The short-circuit `&&` / `||` try to convert
    each side to a logical and raise *"Conversion to logical from scidb.Filter is
    not possible"*. Always parenthesize: `(Side() == "L") & (Speed() > 1.2)`.

## Lower-level filter helpers

For cases the comparison syntax doesn't cover, `scidb` exports two helpers:

- **`schema_key(...)`** — filter directly on a dataset schema key.
- **`raw_sql(...)`** — drop down to a raw SQL predicate when you need an
  expression the filter objects can't express.

```python
from scidb import raw_sql, schema_key
```

Reach for these only when the value-based filters above aren't enough.

## Permanent exclusions

A `where=` filter is per-call. To exclude data from **every** analysis — a failed
recording, a withdrawn participant — register a schema-level exclusion instead.
These persist in the database and are applied automatically by `for_each` before
it iterates:

```python
from scidb import exclude_schema, include_schema, list_exclusions

# Exclude a specific combination (a reason is required)
exclude_schema(subject=1, trial=2, reason="equipment malfunction during recording")

# Omit a key to exclude everything under it (e.g. an entire subject)
exclude_schema(subject=3, reason="participant withdrew")

list_exclusions()        # inspect current exclusions (returns a DataFrame)

# Re-include later (logged; the original exclusion row is preserved)
include_schema(subject=1, trial=2, reason="re-reviewed video, recording was valid")

# Re-include part of a wider exclusion: the more specific row wins, and the
# rest of subject 3 stays excluded
include_schema(subject=3, trial=1, reason="trial 1 was recorded before withdrawal")
```

In the GUI, the 📋 Inventory popup has an **Exclude…** / **Include…** button on
every row. Each one asks for a reason and calls these same functions.

Because the exclusion set is hashed into the outputs' version keys, adding or
removing an exclusion **invalidates cached results** — runs that should now
include or drop that data recompute correctly. (See
[Caching Computations](caching.md).) The same functions are available from MATLAB
through `scidb.*` — see [MATLAB Setup](../matlab-setup.md).

**Next:** [Batch Processing (for_each)](for_each.md) ·
[Browsing & Exporting](browsing.md) · [Defining Variables](variables.md) ·
[API: Filters](../api/filters.md)
