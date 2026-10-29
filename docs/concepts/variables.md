# Variables & Storage

<!-- Ground truth (tests win over prose). Verified against:
     scidb/src/scidb/variable.py (BaseVariable: schema_version, save/load signatures,
       default to_db/from_db, __class_getitem__ column selection, _reserved_keys);
     scidb/src/scidb/database.py:515-540 (configure_database auto-registers all known
       subclasses; db.register for classes defined later);
     scidb/tests/test_integration.py (save->record_id, load->.data/.record_id/.metadata,
       version history, content-addressed dedup, native vs custom DataFrame serialization);
     scidb/tests/test_introspect.py (introspect=True attaches .where/.version_mode/
       .content_hash/.branch_params; version="all" -> list);
     scidb/tests/test_constant.py (constant() transparent wrapper);
     sciduckdb/README + tests (one table per variable, queryable DuckDB types, versioning).
     NOTE: scidb round-trips addressing metadata as given (test asserts {"subject": 1} as
     int) — do NOT claim values are coerced to strings at the scidb level. -->

A **variable** in SciStack is a *typed kind of result* — "step length", "raw
EMG", "rotation matrix" — defined once as a class and then saved and loaded by
metadata. The type owns how its data is stored and restored; you address
individual values by the experimental conditions they belong to.

## Defining a variable

A variable is a subclass of `BaseVariable`. The only required attribute is
`schema_version`:

```python
from scidb import BaseVariable

class StepLength(BaseVariable):
    schema_version = 1   # bump whenever the variable's structure changes
```

Defining the class is enough to register it: every subclass is recorded in a
global registry, and `configure_database(...)` **auto-registers all known
subclasses** at setup. (A class defined *after* `configure_database` runs is
registered with `db.register(MyVar)`.)

`schema_version` is a deliberate version stamp on the variable's *shape*. Bump it
when you change how the data is laid out so old and new records are
distinguishable rather than silently mixed.

## Saving and addressing metadata

`save` stores a value and returns its `record_id`:

```python
import numpy as np

record_id = StepLength.save(np.array([0.65, 0.72, 0.68]), subject=1, session="A")
```

The keyword arguments (`subject=1, session="A"`) are **addressing metadata** —
the coordinates a value lives at. They are matched as given on the way back out
(an integer `subject=1` round-trips as `1`, not `"1"`). A handful of names are
reserved and can't be used as metadata: `record_id`, `id`, `created_at`,
`schema_version`, `index`, `loc`, `iloc`.

Saving a `DataFrame` whose columns include dataset schema keys is a shortcut:
each row becomes its own record and `save` returns a `list` of record ids.

## Identity and versioning

A `record_id` is **content-addressed** — derived from the data plus its
metadata. Saving the *same* value at the *same* coordinates yields the *same*
record id, so re-running a pipeline doesn't create duplicate records:

```python
id1 = StepLength.save(42, subject=1, session="A")
id2 = StepLength.save(42, subject=1, session="A")   # id2 == id1
```

Saving *different* data at the same coordinates creates a **new version**. By
default `load` returns the latest version at a location; you can ask for a
specific one or for the whole history:

```python
StepLength.load(subject=1, session="A")                  # latest (default)
StepLength.load(subject=1, session="A", version="all")   # list of every version
StepLength.load(version=some_record_id)                  # one specific record
```

This content-addressed identity is what makes [caching](caching.md) and
[lineage](lineage.md) reliable: "have I already computed this?" reduces to
"does this record id already exist?".

## What `load` gives you back

`load` returns a single `BaseVariable` instance when exactly one record matches,
a `list` when several match, or a `DataFrame` with `as_df=True`. It raises
`NotFoundError` when nothing matches. Metadata matching is partial, and a list
value means "match any" (OR):

```python
var = StepLength.load(subject=1, session="A")
var.data        # the native value (numpy array, scalar, DataFrame, …)
var.record_id   # this record's content-addressed id
var.metadata    # {"subject": 1, "session": "A"}

many = StepLength.load(subject=1)              # list: all sessions for subject 1
either = StepLength.load(session=["A", "B"])   # match-any across sessions
```

Passing `introspect=True` additionally attaches the call-context and internal
fields — `.where`, `.version_mode`, `.content_hash`, `.branch_params` — for
debugging and provenance inspection.

## How data is stored

Storage is handled by `scidb`'s database layer (internally, `sciduckdb` — see
[Internals](../internals/sciduckdb.md)). Each variable type gets **its own DuckDB
table**, and values are stored in *queryable* DuckDB types (`LIST`, nested
`LIST`, `JSON`) — so the database can be opened in DBeaver or any
DuckDB-compatible viewer and read directly, not as opaque blobs.

The bridge between your Python object and a stored row is a pair of methods:

- `to_db(self) -> DataFrame` — how the value becomes rows. The default wraps the
  value as a single `value` column.
- `from_db(cls, df) -> value` — how rows become the value again. The default
  unwraps the `value` column.

For common data — scalars, numpy arrays, lists, dicts, and pandas DataFrames —
the defaults (plus the database layer's native type handling) round-trip
losslessly, so **you override nothing**. Override `to_db`/`from_db` only for a custom
multi-column layout or a domain-specific object — and if you override one, you
must override the other:

```python
class RotationMatrix(BaseVariable):
    schema_version = 1

    def to_db(self):
        return pd.DataFrame({
            "row": [0, 0, 0, 1, 1, 1, 2, 2, 2],
            "col": [0, 1, 2, 0, 1, 2, 0, 1, 2],
            "value": self.data.flatten().tolist(),
        })

    @classmethod
    def from_db(cls, df):
        return df.sort_values(["row", "col"])["value"].values.reshape(3, 3)
```

## Constants vs variables

Not every input to a pipeline is a stored variable. A **constant** — a sampling
rate, a regularization weight — is a tagged literal, created with `constant()`:

```python
from scidb import constant

sampling_rate = constant(1000, description="Hz")
sampling_rate + 1        # 1001  — behaves transparently like the value
```

A `constant` is *transparent*: it supports arithmetic, comparison, and hashing
as if it were the underlying value, but it carries a description and is
recognized by `for_each` as a constant rather than a database-backed variable.
That distinction matters for [lineage](lineage.md) and
[caching](caching.md), which record constants and variables differently.

## Selecting columns

For table-valued variables, indexing a class selects columns to feed into
`for_each` (rather than the whole table):

```python
MyVar["col_a"]            # one column — the function receives an array
MyVar[["col_a", "col_b"]] # a subset — the function receives a DataFrame
MyVar.for_columns()       # run once per column, reassembled into one output
MyVar.for_columns([["a", "a_date"], ["b", "b_date"]])  # columns passed together, one call per group
```

Column groups are explained in
[Columns that belong together](../guide/for_each.md#columns-that-belong-together).

The same indexing also builds column filters (e.g. `MyVar["col_a"] != 0`) used
in `where=` clauses — see [Filtering & Selection](../guide/filters.md).

**Next:** [Lineage & Provenance](lineage.md) ·
[Computation Caching](caching.md) · [API: Variables](../api/variables.md) ·
[Guide: Defining Variables](../guide/variables.md)
