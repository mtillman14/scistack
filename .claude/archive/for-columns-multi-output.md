# Plan: multi-output `for_columns` (N stats per source column)

## Goal
Let an `iterate=True` `ColumnSelection` (a.k.a. `for_columns`) return **multiple
named values per source column** instead of a single scalar. Motivating case:
per metric column, return both the max value and the identity of the "best"
intervention.

## Current behavior / blocker
`scifor/src/scifor/foreach.py::_run_column_iteration` is hardwired to
scalar-per-column:

```python
if isinstance(res, tuple): res = res[0]                 # drops extra values
col_results[col] = res                                   # one scalar per column
return pd.DataFrame({col: [col_results[col]] for col in iterate_columns})
```

Everything downstream (flatten/nested-table save, caching, lineage) already
handles a **wide table of scalar columns**. So the design is: expand to more
*scalar* columns with suffixed names — NOT tuples-in-cells (non-scalar cells
break the DuckDB save layer).

## Design decisions (to confirm with user)
1. **Output layout:** flat suffixed columns `col__<stat>` (preferred — stays
   scalar, reuses existing save path) vs pandas MultiIndex (needs flattening
   anyway). → go flat suffix.
2. **Separator:** `__` (confirm; must not collide with existing column names).
3. **Return contract:** fn returns a dict or pandas Series of named stats. The
   key set must be **identical across every column** → ragged keys are a hard
   error (mirrors the existing column-drift hard error). Scalar return stays
   supported (back-compat: no suffix).

## Changes by layer
- **scifor (Python) — small:** rewrite `_run_column_iteration` to detect a
  dict/Series return, expand each source column into `col__<key>` scalar
  columns, validate identical key set across columns, drop the tuple-collapse.
  Add tests in `scifor/tests/test_foreach_standalone.py`.
- **scidb save — small/verify:** output is still scalar columns (just N*K of
  them) → rides the existing flatten/nested-table save path. Verify; note the
  "first write fixes physical column set" limitation now spans N*K columns.
- **scidb caching/lineage — tiny:** `ColumnSelection.to_key` already encodes
  columns+iterate; return shape isn't keyed. Resolved physical columns change,
  no new machinery. Lineage already collapses to raw values.
- **MATLAB parity — medium (the bulk):** mirror the struct/dict→multi-column
  expansion in `+scifor/for_each.m::run_column_iteration`; update
  `tests/matlab/scidb/TestForColumns.m`.
- **docs — small:** update `docs/claude/for-columns-iteration.md`
  (scalar-per-column → named-stats-per-column).

## Size verdict
- Standalone scifor (Python) only: **small**, one function + validation + tests.
- Full stack incl. MATLAB parity: **medium**, dominated by the MATLAB port.

## Motivating usage after change
```python
best = scifor.for_each(
    lambda v, interv: {"value": float(np.max(v)), "best": interv[int(np.argmax(v))]},
    inputs={"v": scifor.ColumnSelection(deltas_df, columns=columns, iterate=True),
            "interv": deltas_df["intervention"].values},   # constant; fn sees only one col's values
    subject=[], speed=[],
)
# -> columns: StepLength__value, StepLength__best, Cadence__value, ...
```

## Companion change: honor `as_table` for iterate inputs
**Problem:** the iterate path ignores `as_table` entirely. In the per-combo loop
(`foreach.py:317`) iterate inputs are split off before `as_table` is consulted,
and `_run_column_iteration` always passes `df[col].values` (bare array). The
non-iterate `ColumnSelection` path *does* honor it (`foreach.py:685`,
`keep = schema_keys + column_selection`). So an iterate input is stuck in the
`as_table=False` world with no opt-in.

**Fix:** when an iterate input is in `as_table_set`, each per-column call gets
`filtered[all_schema_key_cols + [current_col]]` (a DataFrame) instead of
`df[col].values`. Mirrors `foreach.py:687` (ColumnSelection as_table =
`[c for c in cols if c in schema_keys] + column_selection`).
`_prepare_iterate_df` already returns the full combo-filtered frame, so only
`_run_column_iteration` needs `schema_keys` + as_table membership threaded in.

**What's retained:** ALL schema key columns (the iterated ones — constant per
call but available for reference — plus any deeper non-iterated levels, which
vary) + the one current iterated column. Consistent with ColumnSelection
as_table, **non-schema data columns are dropped** → `intervention` must be a
schema key to ride along. No "generic single-column find" assumption needed.

**Enables the intervention identity lookup directly:** if `intervention` is a
schema key (schema = [subject, speed, intervention], but only subject/speed
iterated), the per-column frame is [subject, speed, intervention, <metric>] with
all intervention rows present, so
`lambda df: df.loc[df[metric].idxmax(), "intervention"]` works WITHOUT the
multi-output feature. Multi-output is still needed to return value AND identity
per column in one call. The two features compose.

**Size:** ~10-15 lines Python + tests; MATLAB parity in
`+scifor/for_each.m::run_column_iteration` is the bulk.

## Open caveat (independent of this change)
`for_columns` only passes one column's *values* (no row labels), so the
intervention labels must come in as a constant input aligned to row order; if
row order isn't stable per combo, argmax→intervention silently misaligns.
