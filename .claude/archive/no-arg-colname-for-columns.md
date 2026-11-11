# Plan: no-arg `ColName()` resolving to the current `for_columns` column

## Goal

Make `ColName()` (no argument) a **deferred marker** that, inside a
`for_columns` iteration, resolves *per column* to the name of the column
currently being fed to the function. `ColName(df)` / `ColName(MyVar)` keep their
existing static "the one non-schema data column" behavior — unchanged.

Target call (standalone scifor, the user's example):

```python
for_each(anova2way,
    inputs={
        "df": scifor.ColumnSelection(means_df, columns=[], iterate=True),
        "group1_name": "Intervention",
        "group2_name": "PrePost",
        "data_column_name": scifor.ColName(),     # <-- resolves to current column
        "repeated_measures_column": "Subject",
    },
    speed=[],
)
```

## Why the current `ColName` can't do this

- Construction requires a DataFrame (`ColName(df)`); `ColName()` raises.
- Resolution is static and happens at **Step 4** (`_resolve_colnames`), *before*
  the iterate detection (Step 6.5) and the per-column loop — it has no notion of
  "the current column."
- It resolves to the *single* data column and errors on 2+ — but a `for_columns`
  source is wide by definition.

## Design

A deferred `ColName` is one constructed with no argument (`data is None` /
`var_type is None` / empty MATLAB `data`). It is **not** resolved at Step 4;
it survives as a constant input into the per-column loop, where it is replaced
with the current column-name string. A deferred `ColName` only makes sense when
at least one `for_columns` iterate input is present — otherwise it is a hard
error (it must never reach the non-iterate call path).

## Phase 1 — scifor (Python) [primary, covers the user's example]

1. `scifor/src/scifor/colname.py`
   - `__init__(self, data=None)`; add `is_deferred` (`self.data is None`).
   - Update docstring: no-arg form = "current for_columns column."

2. `scifor/src/scifor/foreach.py`
   - `_resolve_colnames` (Step 4): skip deferred markers (leave wrapper in
     place); only resolve `ColName(df)`. Update the Step 4 log to count
     deferred markers separately.
   - Step 6.5: after `iterate_params` is computed, if any deferred `ColName`
     remains in `inputs` **and** `not iterate_params` → raise `ValueError`
     ("ColName() with no argument requires at least one for_columns iterate
     input"). Deferred markers are classified as constants, so they already flow
     into `base_kwargs`.
   - `_run_column_iteration`: at the top of the per-column loop, after
     `call_kwargs = dict(base_kwargs)`, replace any value that
     `isinstance(v, ColName) and v.is_deferred` with `col`. Add a one-line log.

3. Tests — `scifor/tests/test_foreach_standalone.py`:
   - no-arg `ColName()` resolves to the current column across a multi-column
     `for_columns` (assert the function received each column name in order);
   - works alongside `as_table` for the iterate input;
   - works with two zipped iterate inputs (shared column axis);
   - `ColName()` with no iterate input raises `ValueError`;
   - existing `ColName(df)` tests still pass (regression).

## Phase 2 — scidb (Python) [wire through the DB layer]

4. `scidb/src/scidb/colname.py`: `__init__(self, var_type=None)` + `is_deferred`.
5. `scidb/src/scidb/foreach.py` (`_resolve_colname_from_db`, ~1372/1462): if the
   marker is deferred, **do not** hit the DB — substitute a `scifor.ColName()`
   deferred marker so the scifor engine resolves it per column during iteration.
   (Static `ColName(MyVar)` path unchanged.)
6. Tests — `scidb/tests/test_for_columns.py`: no-arg `ColName()` through
   `scidb.for_each` resolves to each column.

## Phase 3 — MATLAB (scifor) [parity]

7. `+scifor/ColName.m`: allow zero-arg construction (`data` defaults to empty);
   `disp` handles the deferred case.
8. `+scifor/for_each.m`:
   - ColName-resolve block (~180): skip when `isempty(var_spec.data)`.
   - Step 6.5 / validation: deferred ColName present but `~has_iterate` → error
     (`scifor:ColName`).
   - `run_column_iteration`: substitute any `base_args` position holding a
     deferred `scifor.ColName` with `char(col)` inside the per-column loop.
9. Tests — `tests/matlab/scifor/`: per-column resolution + no-iterate error.

## Phase 4 — MATLAB (scidb) + bridge [parity]

10. `+scidb/ColName.m`: allow zero-arg.
11. `bridge.py`: describe/reconstruct carries the deferred flag; deferred
    scidb ColName → scifor deferred marker (mirror Phase 2 step 5).
12. Tests — `tests/matlab/scidb/TestForColumns.m`.

## Phase 5 — docs + memory

13. Update `docs/claude/for-columns-iteration.md` (new "Current column name via
    `ColName()`" section).
14. Update memory `project_for_columns_iteration`.

## Verification

User runs the suites (no Python in assistant env):
`scifor` + `scidb` pytest, then the MATLAB `TestScifor` / `TestForColumns`.

## Open choice

Do all phases for full parity, or stop after Phase 1 (standalone scifor Python),
which already satisfies the example call?
