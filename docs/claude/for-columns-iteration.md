# for_columns — Column-Wise Iteration + Reassembly

## Purpose

Iterate a `for_each()` function over each column of a wide-table variable,
feeding one column at a time to the function, and reassemble the per-column
results into a **single** output variable whose data — per schema combo — is a
one-row table with the **same column names** as the source.

This replaces the pattern of one `for_each()` call (and one output variable)
per column.

## Syntax

```python
# All columns (resolved at for_each time)
for_each(col_mean, inputs={"value": GaitData.for_columns()},
         outputs=[DeltaGait], subject=[], session=[])

# Explicit subset
for_each(col_mean, inputs={"value": GaitData.for_columns(["StepLength", "Cadence"])},
         outputs=[DeltaGait], subject=[], session=[])

# Two iterate inputs, zipped by column name (baseline + value)
for_each(mean_change,
         inputs={"baseline": Fixed(GaitData.for_columns(), session="BL"),
                 "value":    GaitData.for_columns()},
         outputs=[DeltaGait], subject=[], session=["FV"])
```

`MyVar.for_columns(columns=[])` returns a `ColumnSelection` with `iterate=True`.
It reuses the existing `ColumnSelection` abstraction — there is **no** new
wrapper type. `MyVar[[...]]` (bracket syntax) remains `iterate=False` and means
"pass the columns as one argument," unchanged.

**All-columns sentinel is `[]` (empty), not `None`.** An empty `columns`
(`for_columns()` / `for_columns([])` / `ColumnSelection(df, iterate=True)`)
means "all data columns", resolved at for_each time. This matches MATLAB
(`string.empty`) and is consistent with scifor's existing "empty list = resolve
all from data" idiom (`subject=[]`). `None` is still accepted as a
backward-compatible alias (every check is "falsy = all", e.g. `not cs.columns`).
The resolution excludes schema keys and internal `__*` columns. It now happens
at **both** layers: scidb (`_resolve_all_columns`, loads the variable) and
standalone scifor (`_resolve_iterate_columns` / `_all_data_columns`, from the
in-memory DataFrame) — and likewise in MATLAB standalone scifor
(`all_data_columns`). Empty `columns` on a **non-iterate** ColumnSelection means
"all data columns as one argument" (equivalent to no selection).

## Semantics

- The function runs once **per column** per schema combo and must take the
  iterated column as a single-column argument (a numpy array, same as
  `MyVar["col"]`).
- **Per-column return → output columns** (see "Multi-output per column" below):
  a **scalar** yields one column named after the source column; a **dict /
  pandas Series / 1-row DataFrame** (struct / 1-row table in MATLAB) yields one
  column per key, named `"<col>__<key>"`. The produced columns are concatenated
  in order into a one-row DataFrame, saved as the single output variable's data
  for that combo.
- When multiple inputs use `for_columns`, they are **zipped by column name** and
  must resolve to the same column set (else `ValueError`). The same column drives
  every iterate input in lockstep (e.g. `baseline[c]` and `value[c]` together).
- **Column drift is a hard error**: the iterate column set is fixed up front, so
  a combo missing one of those columns raises (it does not silently skip).

## as_table interaction

By default each per-column call receives the iterated column as a **bare
array** (numpy in Python / numeric vector in MATLAB), matching single-column
`ColumnSelection` semantics. When the iterate input is named in `as_table`
(either `as_table=True` for all inputs or a list including it), each per-column
call instead receives a **table/DataFrame containing all schema key columns +
the one current column** — mirroring the non-iterate `ColumnSelection` as_table
path (`scifor/foreach.py` `_prepare_input`, MATLAB `prepare_input`).

- Retained: every schema key column (the iterated ones — constant within a call
  but available for reference — plus any deeper non-iterated levels, which
  vary), and the single current iterated column.
- Dropped: non-schema data columns (consistent with how `ColumnSelection`
  as_table already drops unselected columns).
- The per-column frame therefore has exactly **one non-schema column** (the
  current one); the function can locate it via "the column not in the schema."

This enables an argmax→label lookup inside the per-column function: declare the
label column (e.g. `intervention`) as a **schema key** (it need not be
iterated). `_filter_df_for_combo` only filters on schema keys present in the
combo metadata, so a non-iterated schema key keeps all its rows in each slice,
and the function can do `df.loc[df[col].idxmax(), "intervention"]`.

Implementation: `_run_column_iteration` (Python) / `run_column_iteration`
(MATLAB) take `schema_keys` + the as_table set and slice
`df[schema_keys + [col]]` for as_table inputs.

**Through `scidb.for_each`:** wiring is automatic — `scidb.for_each` loads the
iterate input as a `scifor.ColumnSelection(loaded_spread_df, columns,
iterate=True)` (`scidb/foreach.py` `_load_input` ~1710) and delegates to
`scifor.for_each` passing `as_table` (~438). The spread DataFrame carries the
(stringified) schema-key columns, and `configure_database` propagates the
dataset schema to scifor via `scifor.set_schema` (`scidb/database.py` ~548), so
`scifor.get_schema()` inside `_run_column_iteration` returns the right keys and
the `df[schema_keys + [col]]` slice keeps them. To make a within-combo label
(e.g. `intervention`) available for an argmax lookup in scidb, declare it as a
dataset **schema key** (it need not be iterated — aggregation mode keeps its
rows). Non-schema data columns of the variable are dropped, same as standalone.

Tests: `scifor/tests/test_foreach_standalone.py::test_iterate_as_table_*`,
`tests/matlab/scifor/TestSciforForEachFeatures.m::test_iterate_as_table_*`, and
`scidb/tests/test_for_columns.py::TestForColumnsAsTable`.

## Current column name via `ColName()`

A for_columns function often needs to know *which* column it is currently
processing — e.g. to label its output, or to read the right column from an
`as_table` frame — without sniffing "the one non-schema column." The **deferred
(no-arg) `ColName()`** marker provides it:

```python
for_each(analyze,
         inputs={"df": means_df.for_columns(), "col_name": ColName()},
         outputs=[Result], subject=[])
```

```matlab
scidb.for_each(@analyze, ...
    struct('df', MeansVar().for_columns(), 'col_name', scidb.ColName()), ...
    {Result()}, subject=[])
```

On each per-column pass, `ColName()` is replaced with the **string name of the
column currently being iterated**. This is distinct from the *static*
`ColName(df)` / `ColName(MyVar)` form, which resolves **once, up front**, to the
single non-schema data column of a frame (and errors on 2+ columns — so it can't
serve a wide for_columns source).

**Two forms, one class:**

| Form | When resolved | Resolves to |
|------|---------------|-------------|
| `ColName(df)` / `ColName(MyVar)` (static) | up front, Step 4 | the single data column name |
| `ColName()` (deferred) | per-column, in the iteration loop | the current for_columns column |

**Rules:**

- Deferred `ColName()` **requires at least one iterate input**. With no
  for_columns input there is nothing to resolve against, so it is a hard error
  (`ValueError` Python / `scifor:ColName` MATLAB) raised before the run — it must
  never reach the non-iterate call path.
- When multiple iterate inputs are zipped, the column axis is shared, so
  `ColName()` is unambiguous (it equals the single current column name).
- It is classified as a *constant* input (not a data input), so it flows
  untouched into the per-column loop and is substituted there.

**Implementation:**

- Python scifor: `colname.py` (`data=None` default + `is_deferred`);
  `foreach.py` Step 4 (`_resolve_colnames`) skips deferred markers; Step 6.5
  validates "deferred ⇒ iterate present"; `_run_column_iteration` substitutes
  the current `col` for each deferred-marker kwarg.
- Python scidb: `colname.py` (`var_type=None` + `is_deferred`); `foreach.py`
  `_convert_inputs` turns a deferred `scidb.ColName()` into a
  `scifor.ColName()` marker (instead of hitting the DB) so the scifor engine
  resolves it per column.
- MATLAB scifor: `+scifor/ColName.m` (zero-arg + `is_deferred`); `+scifor/for_each.m`
  mirrors the skip / validate / per-column substitute in `run_column_iteration`.
- MATLAB scidb + bridge: `+scidb/ColName.m` (zero-arg); `describe_input_for_python`
  ships `kind='colname'` with a `deferred` flag; `bridge._reconstruct_input_for_keys`
  rebuilds `scidb.ColName()`; `bridge.for_each_describe_loaded_input` describes a
  leftover deferred `scifor.ColName` back as `kind='colname'`, and
  `build_scifor_input_from_desc` rebuilds `scifor.ColName()` MATLAB-side.

Tests: `scifor/tests/test_foreach_standalone.py::test_deferred_colname_*`,
`scidb/tests/test_for_columns.py::TestForColumnsDeferredColName`,
`tests/matlab/scifor/TestSciforForEachFeatures.m::test_deferred_colname_*`,
`tests/matlab/scidb/TestForColumns.m::test_deferred_colname_*`.

## Multi-output per column

A for_columns function may return **more than one named value per source
column**. The return shape is auto-detected (no opt-in flag):

| Return (Python / MATLAB) | Output columns |
|--------------------------|----------------|
| scalar | `<col>` (one column, back-compatible) |
| `dict` / `pandas.Series` / `struct` | `<col>__<key>` per item |
| 1-row `DataFrame` / 1-row `table` | `<col>__<column>` per column |
| multi-row frame/table | **hard error** (`ForColumnsError` / `scifor:for_each:forColumnsBadReturn`) |
| `tuple` | collapsed to first element (use a dict for multiple values) |

Separator is `FOR_COLUMNS_OUTPUT_SEP = "__"` (Python) / `sep = '__'` (MATLAB).
The reassembled row is the **ordered concatenation** of every produced column,
so different source columns may emit **different numbers (and names)** of
outputs within one call. Two produced names colliding on the same output column
is a **hard error** (`ForColumnsError` / `scifor:for_each:forColumnsDuplicate`).

**Hard vs skip:** structural reassembly errors (collision, multi-row return) are
deterministic across combos, so they propagate immediately instead of being
swallowed by the per-combo `[skip]` handler — Python via a dedicated
`ForColumnsError(ValueError)` re-raised before the generic `except`; MATLAB via
an identifier check that `rethrow`s `forColumnsDuplicate`/`forColumnsBadReturn`.

The argmax→label use case combines this with as_table: return
`{"value": v[col].max(), "best": v.loc[v[col].idxmax(), "intervention"]}` to get
`<col>__value` and `<col>__best` per metric column in one call.

**Limitation — output set must be stable across combos.** A variable's physical
columns are fixed on first write, so while the per-column output count may vary
*across source columns within a combo*, the overall produced column set must be
the **same for every schema combo** (else the second save hits a DuckDB binder
error). If your stat set depends on data magnitude, ensure the branch is
column-consistent across combos, or use a distinct output variable.

Implementation: `_run_column_iteration` + `_expand_column_result` (Python,
`scifor/foreach.py`); `run_column_iteration` + `expand_column_result` (MATLAB,
`+scifor/for_each.m`). Tests:
`scifor/tests/test_foreach_standalone.py` (`test_iterate_dict_*`,
`test_iterate_varying_*`, `test_iterate_series_*`, `test_iterate_multi_output_*`,
`test_iterate_duplicate_*`, `test_iterate_multirow_*`),
`tests/matlab/scifor/TestSciforForEachFeatures.m` (`test_iterate_struct_*`,
`test_iterate_varying_*`, `test_iterate_multi_output_*`,
`test_iterate_duplicate_*`, `test_iterate_multirow_*`), and
`scidb/tests/test_for_columns.py::TestForColumnsMultiOutput`.

## Why not EachOf

`EachOf` was considered but mismatches on two axes:
1. It fans **out** into separately-saved variant records; `for_columns` fans
   **in** to one reassembled record/variable.
2. Its axes are a **cartesian product**; `for_columns` is a single **shared,
   zipped** axis across inputs.

## Why wrap-the-function (not expand-and-remerge)

`for_each` already saves whatever `fn` returns as the output's `.data` per combo,
and scifor's flatten path + scidb's flatten save path (`foreach.py` ~2348)
already persist a wide-DataFrame return to **one** table variable. So the
implementation wraps the per-combo work to loop over columns and return the
assembled `1 × N` row — the existing save path does the rest. No bespoke merge or
`save=False` inner passes.

## Implementation map

| Layer | File | Change |
|-------|------|--------|
| scifor | `scifor/column_selection.py` | `ColumnSelection.iterate` flag |
| scifor | `scifor/foreach.py` | Step 6.5 detect iterate inputs + shared column set; per-combo `_run_column_iteration` (loop fn per column → 1×N DataFrame); `_prepare_iterate_df`, `_unwrap_column_selection`; hard drift error |
| scidb | `scidb/column_selection.py` | `iterate` flag, `columns=None`, `to_key`/`__hash__`/`__name__` |
| scidb | `scidb/variable.py` | `BaseVariable.for_columns()` classmethod |
| scidb | `scidb/foreach.py` | Step 1.5 `_resolve_for_columns` (empty `[]`/falsy→all columns + zip-by-name validation, **before** version keys); pass `iterate` through `_load_input`; `_make_raw_value_wrapper` (lineage); dry-run display |
| scifor | `scifor/foreach.py` | `_resolve_iterate_columns` / `_all_data_columns` expand empty `[]`→all columns at Step 6.5 (and non-iterate `_prepare_input`) for standalone use |
| sci-matlab | `+scifor/ColumnSelection.m` | `iterate` property (3rd ctor arg) |
| sci-matlab | `+scifor/for_each.m` | Step 6.5 detect iterate inputs + shared column set; `unwrap_column_selection`, `prepare_iterate_table`, `run_column_iteration` (loop fn per column → 1×N table, collapse per-column `scidb.LineageFcnResult` to `.data`); hard drift error |
| sci-matlab | `+scidb/BaseVariable.m` | `iterate` property + `for_columns(cols)` method |
| sci-matlab | `+scidb/for_each.m` | `describe_input_for_python` ships `iterate` + `columns=None` for all-columns; `build_scifor_input_from_desc` / `coerce_meta_columns` carry `iterate` |
| sci-matlab | `bridge.py` | `_reconstruct_input_for_keys` + `for_each_describe_loaded_input` carry `iterate`/None columns; `for_each_prepare` runs `_resolve_for_columns` (the MATLAB path calls `_for_each_prepare` directly, so resolution must happen in the bridge) |

Resolution happens at **Step 1.5** (top of `for_each`) so the concrete column
set is reflected in version keys (Step 8) and dry-run display (Step 7).

### MATLAB path specifics

MATLAB runs the per-column loop in `+scifor/for_each.m` itself (Python's
sentinel is never called). The bridge's `for_each_prepare` therefore calls
`_resolve_for_columns` **before** `_for_each_prepare` so the resolved column
set drives version keys, exactly mirroring where the Python-only path resolves
it inside `scidb.for_each`. Each per-combo 1×N table is collected in scifor's
**nested** mode (`_nest_table_outputs=true`) and saved through the same path a
table-returning function uses (e.g. `double_table_values`), so the wide table
spreads into one output variable's data. Lineage parity: `run_column_iteration`
collapses a per-column `scidb.LineageFcnResult` to its `.data` (the MATLAB
analogue of `_make_raw_value_wrapper`). Tests: `tests/matlab/scidb/TestForColumns.m`.

## Lineage

Per the design decision, `for_columns` uses **combined-call lineage**: per-column
results are collapsed to raw values (`_make_raw_value_wrapper`) because per-column
`LineageFcnResult` objects cannot live in the reassembled wide DataFrame. Upstream
provenance for the combined output is still recorded at save time from the input
record_ids. There is no per-column function-hash lineage.

## Caching

`ColumnSelection.to_key()` includes `iterate` and the resolved column list, so
changing the iterated column set (including the empty `[]`→all resolution) creates a
new record; an identical re-run is a cache hit.

## Saving: branch_params contains the reassembled column values

`_save_results` feeds every non-schema, non-`__` column through the
dynamic-discriminator loop, so a for_columns output's reassembled column values
(e.g. `{StepLength: 2.0, Cadence: 20.0}`) land in `branch_params`. **This is
intentional, load-bearing flatten behavior** — do NOT "fix" it by excluding the
data columns. For multi-row flatten outputs (a function returning an N-row
DataFrame), scifor spreads the N rows into N result rows and the per-row data
value is the *only* thing distinguishing them, so each row saves as a distinct
record. Excluding the data columns collapses those N records into one (verified by
`test_variant_pinning.py::test_variant_wraps_column_selection`, which checks a
3-row flatten output round-trips as 3 values summing to 120).

For for_columns specifically the output is a single 1×N row per combo, so the
values-in-branch_params is harmless: there is one deterministic record per schema
combo (no variant ambiguity), and an identical re-run still hits cache.

## Known limitations / future work

- **All-columns resolution loads the variable once** (via `_load_var_type_as_spread`)
  just to read column names, in addition to the loop's own load. Acceptable for
  v1; could be optimized with a metadata-only column query.
- **Output column set is fixed by the first save.** A table variable's physical
  columns are created on first write, so re-running the same output variable with a
  *different* column set fails at save (DuckDB binder error). Use a distinct output
  variable per column set, or keep the set stable. (Changing the *function* over the
  same columns is fine — same physical schema, new version key.)
- **MATLAB parity is implemented** (see the implementation map + "MATLAB path
  specifics" above). Verify in MATLAB by running `tests/matlab/scidb/TestForColumns.m`.
- **scihist + for_columns**: on the MATLAB path, a `scidb.LineageFcn` returns a
  per-column `LineageFcnResult` which `run_column_iteration` collapses to its
  raw `.data` value (combined-call lineage, parity with Python). Per-column
  function-hash lineage is intentionally not recorded.
```

## `for_columns` as a run option (2026-09-19)

`iterate` is an execution MODE recorded inside a column list. Its siblings
`distribute` and `as_table` are first class — `_invocation` columns, folded
into `invocation_id`, reported by `pipeline_variants`, shown in the GUI's Run
options — and that is why they never got lost and this one did (it reached
storage as "no selection" on the aggregation save path; see
`docs/claude/input-binding-round-trip.md`).

It now joins them, with ONE deliberate difference:

* **`_invocation.for_columns`** (`VARCHAR[]`) records which params ran once
  per column. Added by an additive backfill, like `_invocation_input.selector`
  before it.
* **`run_options_label`** appends `, for_columns=[param]`, so two records at
  one location — one per-column, one whole-table — are distinguishable in
  `is_latest`, in the `Run:<fn>` plot axis and in `Variant(...,
  run_options=...)`. They were indistinguishable before.
* **`pipeline_variants[].run_options`** reports it, derived from the edges'
  selectors rather than read from the column, so a record written before the
  column existed still reports correctly and the two can never disagree.
* **The GUI's Run options section** states "Run once per column", read-only,
  naming the params and pointing at the Inputs section where it is set.

**Not an identity term.** `compute_invocation_id` already folds in each
input's selector, and `iterate` lives inside that selector — so adding a
separate identity term would count one fact twice and re-identify every
per-column call ever recorded, for no gain. The column is descriptive; the
selector remains the source of truth, and `for_columns` is derived from it.
