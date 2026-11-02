# Filters API — `where=` Parameter

<!-- Ground truth (source/tests win over prose). Verified against:
     scidb/tests/test_filters.py (Side == "L" where Side is a BaseVariable subclass ->
       VariableFilter with .variable_class/.op/.value via metaclass comparison ops;
       raw_sql, schema_key imported from scidb.model.filters, exported from scidb);
     scidb/tests/test_where.py (where= in load(); ColumnFilter MyVar["col"] == val; & | ~);
     scidb/src/scidb/__init__.py exports raw_sql, schema_key.
     NOTE: where= works in load() and for_each(), NOT a `load_all` method (no such method).
     MATLAB filters use & / | (never && / ||). -->

A `where=` filter restricts which records take part, by the *values of other
variables*. It is accepted by [`load()`](variables.md) and
[`for_each()`](for-each.md). For task usage see
[Filtering & Selection](../guide/filters.md).

---

## Variable-value filters

Comparing a `BaseVariable` subclass to a value builds a filter (no data is fetched
yet):

```python
Side == "L"      # variable equals a value
Speed > 1.2
StartFoot != "A"
```

Supported operators: `==`, `!=`, `<`, `<=`, `>`, `>=`. The result records the
variable, the operator, and the value.

=== "Python"
    ```python
    left = StepLength.load(where=Side == "L")
    ```
=== "MATLAB"
    ```matlab
    left = StepLength().load(where=Side() == "L");
    ```

---

## Column filters

For a table-valued variable, index a column and compare it:

```python
GaitData["side"] == "L"
GaitData["speed"] >= 1.5
```

The same column expression also drives
[column selection](for-each.md#column-selection) in `for_each` inputs.

---

## Composition: `&` `|` `~`

Filters are first-class values — name them and combine them:

```python
clean = StepLength != 0
unilateral = (Side == "L") | (Side == "R")

StepLength.load(where=clean & (Speed > 1.2))
StepLength.load(where=~(Side == "L"))
for_each(analyze, inputs={"x": StepLength}, outputs=[Result],
         where=clean & unilateral, subject=[], session=[])
```

| Operator | Meaning |
|---|---|
| `&` | and |
| `|` | or |
| `~` | not |

!!! danger "MATLAB: `&` / `|`, never `&&` / `||`"
    Composition relies on operator overloading, which MATLAB allows only for the
    element-wise `&` / `|`. The short-circuit `&&` / `||` raise *"Conversion to
    logical from scidb.Filter is not possible"*. Always parenthesize:
    `(Side() == "L") & (Speed() > 1.2)`.

---

## Helpers

```python
from scidb import raw_sql, schema_key
```

- **`schema_key(...)`** — filter directly on a dataset schema key.
- **`raw_sql(...)`** — a raw SQL predicate for expressions the filter objects
  can't represent.

Use these only when the comparison syntax above isn't enough.

---

## Where filters apply

| Call | Effect of `where=` |
|---|---|
| `Var.load(where=...)` | restrict which records load |
| `for_each(..., where=...)` | restrict which records the iteration processes |

For permanent, persisted exclusions across *all* analyses, use
`exclude_schema(...)` instead — see [Filtering & Selection](../guide/filters.md)
and [Database](database.md).

**See also:** [Variables](variables.md) · [Batch Processing](for-each.md) ·
[Guide: Filtering & Selection](../guide/filters.md)
