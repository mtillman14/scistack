# Plan: opt-in "combine duplicate rows" for table saves

Status: drafted 2026-10-29; revised three times (row_list storage kind, lazy
conversion, scifor split); approved and ALL 7 STAGES BUILT 2026-10-29.
Tests written, NOT run (no Python env here); frontend typechecked, both
bundles rebuilt. See "Build log" at the end.
Related: `docs/claude/duplicate-location-vs-stale.md`, `.claude/plan-duplicate-location-in-batch.md`,
memory `project_duplicate_location_remedy`.

## Problem

A table (CSV / DataFrame / MATLAB table) with two rows for one schema location
(e.g. subject S01 with two hospital stays, schema `[subject, date]`, no `date`
column) is refused by `save_batch` with `DuplicateLocationError`. The user wants
a built-in, opt-in way to save one record per location instead.

## Decisions (user, 2026-10-29)

| Q | Decision |
|---|---|
| Default | **Opt-in** (`on_duplicate="combine"`). Default stays the error: a duplicate row is a data error the user decides on. Combine-as-default was considered and rejected (masks accidental duplicates; row_list everywhere). |
| Granularity | **Per data column** within one saved table. |
| Collapse rule | A column collapses to one value only if ALL its values at that location are equal; otherwise it keeps EVERY row's value, in table order, so position i is "row i" across all list columns. |
| Column type | One type per column. **Lazy:** a column stays its ordinary type until a location in it first needs a multi-value list; then the WHOLE column becomes `row_list` (Q4a) and stays so. |
| Missing values | NaN == NaN (so `[NaN, NaN]` -> `NaN`); `[5, NaN]` stays a list. Nothing dropped. |
| Scope | **Table rows only**, and only scalar cells. Any other colliding item (arrays, non-table outputs) still raises. |
| Record of it | **Log only.** No per-record metadata field. (`row_list` is a per-column storage kind, not a record field.) |
| 1-element lists | A `row_list` cell `[v]` **loads back as plain `v`** (new behaviour; today list columns always load as arrays). |
| Mixed types | **Refused.** Values of one row_list column must be one type class: numeric (int/float/NaN), bool, string. Against an already-stored column the existing rules apply (D1 widen, D2 as_text, else refuse). |
| Layering | **Rule in scifor** (pure table operation), storage in sciduckdb, plumbing + option + variant knowledge in scidb. |

## Behaviour by scenario

D1 CSV, subject 1, column `value`:

| Run | Mode | Rows for subj 1 | Stored column | Stored cell | Loads as |
|---|---|---|---|---|---|
| 1 | either | `7` | `DOUBLE` (unchanged from today) | `7` | `7` |
| 2 | error | `7`, `8` | - | - | `DuplicateLocationError` + popup |
| 2 | combine (e.g. popup "Combine rows") | `7`, `8` | converted in place -> `row_list` (`DOUBLE[]`) | old `[7]`, new `[7,8]` | old `7`, new (latest) `[7,8]` |
| 3 | either, dup row removed again | `7` | stays `row_list` | `[7]` | `7` |

Combine mode with no duplicates stores exactly what the default stores today.

Example with more columns: rows `S01|5|60|A`, `S01|12|60|B`, `S02|7|45|A`
(stay_days, age, ward) -> combined table `S01|[5,12]|60|[A,B]`, `S02|7|45|A`;
`stay_days` and `ward` become row_list columns, `age` stays DOUBLE.

## Ownership (NOTE 3 / NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| "Combine rows that share key columns" (group, collapse-or-list, NaN-aware equality, scalar-cells-only, mixed-type check) | **scifor** `combine_duplicate_rows` | scidb save paths, MATLAB via bridge |
| Which columns identify a location+variant in a table ("key columns") | **scidb** `combine_key_columns` | every scidb call of the scifor rule |
| `on_duplicate` vocabulary + validation | **scidb** `validate_on_duplicate` | Python API, for_each, bridge, GUI config reader |
| `row_list` storage kind (type, store, unwrap, conversion) | **sciduckdb** | scidb save_batch / reconcile / loads |
| Backstop "two different records at one location" | **scidb** `_colliding_locations` (unchanged) | save_batch |
| Popup wording | the exceptions (`DuplicateLocationError`, `CombineMixedTypesError`) | GUI, MATLAB, terminal |

No shared Python type marks a combined cell: scifor returns plain lists plus
a report naming the list columns, and scidb passes those column names to
sciduckdb (`row_list_columns=`). scifor and sciduckdb do not depend on each
other, and nothing has to recognise a cell by its class.

## Design

### 1. Rule owner (scifor): `scifor.combine.combine_duplicate_rows(df, key_columns, value_columns=None)`

Pure pandas, no database. Returns `(combined_df, CombineReport)`.

1. Group rows by `key_columns` (`dropna=False`, so a NULL key groups with NULL;
   order of first appearance kept).
2. Groups of one row: unchanged.
3. Per value column of a multi-row group: all values equal (NaN == NaN) ->
   that value; else the list of every value in row order.
4. Only scalar cells (number, bool, str, None/NaN) may be combined. A
   multi-row group with a non-scalar cell (array, list, dict, DataFrame) ->
   left uncombined (all its rows kept) and reported, so the scidb backstop
   raises `DuplicateLocationError` for it.
5. Mixed-type check per list column across the WHOLE table (list elements and
   the column's scalars): one class of numeric / bool / string ->
   `CombineMixedTypesError(ActionableError)` with `remedy`, before anything is
   saved. Datetime cells in a column that needs a list -> same error, named as
   not supported yet (known limitation, see Risks).
6. `CombineReport`: groups combined (key values + row count), `list_columns`
   (columns where any group produced a multi-value list), `collapsed_columns`,
   `uncombinable` groups. The rule owner logs (scistacklog):
   - DEBUG every call: `combine_duplicate_rows: N row(s) -> M, G group(s) combined, list column(s) [...]`
   - WARN when G > 0: each combined location, row count, list vs collapsed columns.
   - WARN for each uncombinable group (which cell kind blocked it).

`value_columns` default = every column not in `key_columns`.

### 2. Key columns owner (scidb): `combine_key_columns(df, schema_keys, value_columns)`

"Key columns" = every column of the table that is not a value column: the
schema keys present plus every metadata / bookkeeping column (`__rid_*`,
`__vsig_*`, version keys, non-schema metadata). Defined as the complement of
the value columns, so two variants of one subject (different `__rid_*`) are
never merged, and the definition cannot drift from `_colliding_locations`
(which also groups by location + every other metadata key). Datetime NaN-free
equality etc. stays in scifor.

### 3. Storage kind owner (sciduckdb): `python_type="row_list"`

- `infer_data_columns(..., row_list_columns=())` / `_batch_column_type`: a
  named column -> `<elem>[]` with meta `{"python_type": "row_list", "elem": <scalar meta>}`;
  the element type is judged over every scalar and list element of the batch
  (all int -> BIGINT, any float/NaN -> DOUBLE, bool -> BOOLEAN, str -> VARCHAR).
  Single-column mode: the one data column is named by scidb the same way.
- Storage (`_python_to_storage`, `bulk_df_to_storage_rows`,
  `dataframe_to_storage_rows`, `value_to_storage_row`): scalar -> `[v]`,
  list -> list; missing elements -> NULL. Applies to any save into a row_list
  column, so the kind is sticky.
- `record_schema_mismatch(..., row_list_columns=)`: a scalar and a list are
  both valid for a row_list column (validation pass of single/multi-column mode).
- Load: `unwrap_row_list(value, meta)` - length 1 -> scalar, else Python list;
  NULL elements -> NaN (numeric/bool) or None (string). Called from
  `storage_to_python` (and so `storage_to_python_column`, `SciDuck.load`,
  `_restore_types`).
- `column_type_change` + a meta-aware twin `row_list_change(stored_meta, stored_type, in_meta, in_type)`:
  row_list vs `ndarray`/`list` -> refuse both ways; row_list elem vs scalar /
  row_list elem -> the existing scalar rules on the element type (same / widen /
  as_text / refuse).
- `row_list_sql(col)`: the SQL spelling of the unwrap for SQL readers that need
  scalars (`CASE WHEN len(c) = 1 THEN c[1] END` + a list companion); tested
  against `unwrap_row_list`. Used only where the audit (§6) says so.

### 4. Lazy conversion owner (sciduckdb): `convert_column_to_row_list(duck, table, col, elem_type)`

Called by scidb's column-type reconcile (`_reconcile_column_types`, the same
place as the D1 BIGINT->DOUBLE widen), which is extended to run for
single/multi-column modes too, not only DataFrame mode:
- Stored scalar column + incoming row_list -> `ALTER TABLE ... ALTER col SET
  DATA TYPE <elem>[] USING CASE WHEN col IS NULL THEN NULL ELSE [col] END`.
- Stored scalar column `_variables.dtype` meta updated to row_list in the SAME
  transaction as the ALTER (so a later failure of the save cannot leave a
  `DOUBLE[]` column described as `float`; the old values load unchanged either way).
- Stored row_list + incoming scalars (any mode) -> incoming meta becomes the
  stored row_list meta; storage wraps.
- Stored row_list + wider incoming element (BIGINT[] <- fractions) -> ALTER to DOUBLE[].
- Stored record content hashes / record_ids are NOT touched (their loaded
  value is unchanged); verified by test.
- Empty table: takes the new kind directly (existing empty-table rule).
- View dropped and recreated around ALTERs (existing pattern).

### 5. Option plumbing (scidb): `on_duplicate="error" | "combine"`

- `validate_on_duplicate` in scidb (single owner of the vocabulary). Not
  identity-bearing (like `on_error`).
- **Plain save:** `BaseVariable.save(df, on_duplicate=)` combines the DataFrame
  (key columns = schema-key columns present + common metadata is constant) BEFORE
  the single/multi-column split, and passes `row_list_columns` on.
  `save_from_dataframe(..., on_duplicate=)` does the same for direct callers
  (a second pass over an already-unique table is a no-op).
- **for_each:** `for_each(..., on_duplicate=)` -> `_batch_save_outputs`
  combines `result_tbl` BEFORE Phase 1 builds items. Value columns = the output
  columns (normal path) or the flatten data columns (flatten path); key columns
  from `combine_key_columns` (so `__rid_*` / `__vsig_*` keep variants apart).
  The combined row then flows through the existing per-row code: one item, one
  invocation id, one graph record. A multi-value list cell is not a scalar, so
  the dynamic-discriminator step already leaves it out of `__branch_params`.
- `DatabaseManager.save_batch(..., row_list_columns=())`: passes the names to
  inference/validation; `_check_duplicate_locations` still runs (backstop for
  uncombinable groups and for `on_duplicate="error"`).
- **MATLAB:** `scidb.for_each(..., 'on_duplicate','combine')` and
  `save_from_table(..., 'on_duplicate','combine')` -> `bridge.save_batch_bridge(on_duplicate=)`,
  which assembles its columnar input into one table (metadata columns + data,
  or the vertcat DataFrame + row heights), calls the same scifor rule with key
  columns from `combine_key_columns`, and passes `row_list_columns` to
  `save_batch`. Standalone MATLAB scifor (no database) does not get the feature
  (no second owner in MATLAB).
- **GUI (GUI layer only):** node run option `on_duplicate` read through one
  config reader (like `run_failures.on_error_from_config`), passed to Python and
  MATLAB runs; the `DuplicateLocationError` RemedyDialog gets a **"Combine rows"**
  button that sets the option on that node and reruns.

### 6. Reader audit (stage 4, findings recorded here before routing)

Readers that go through `storage_to_python` get the unwrap for free: scidb
`load`, `load_all`, spread layout / `as_table`, for_each inputs, MATLAB loads
(via scidb). Readers that select stored columns with SQL and no dtype meta must
be listed and decided one by one. Preliminary finding (2026-10-29):

- **Plot Studio** (`scistackplotdb/load.py` `load_variable`, `sample_value`,
  `sample_column_value`, `column_levels`, `is_container_type`): reads raw
  DuckDB cells. A row_list column arrives as 1-D arrays (`[7]`, `[5,12]`), is
  classified as a 1-D vector and goes through the existing 1-D collapse
  (mean/median) for scalar kinds. Proposed: keep that (it is consistent: every
  cell is one shape), add a DEBUG naming row_list columns per load, and a test.
  A row_list string column is a container, so it is not offered for grouping.
- Still to audit: stats inputs (`scistackstatsdb`), data inventory, CSV export,
  scidb CLI, scistack-gui direct queries, scidb-net serialization.

Any reader where "treat as 1-D vector" is wrong is raised with the user before routing.

### 7. Switching modes

`error` <-> `combine` never refuses: with lazy conversion a column only becomes
row_list when a list is needed and stays row_list afterwards (§4). The only
refusal left is row_list vs a genuine array column.

### Logging (NOTE 2)

- scifor rule: DEBUG per call, WARN per combined batch, WARN per uncombinable group (§1).
- scidb: DEBUG `on_duplicate=combine: <Type> row_list column(s) [...]` per save call;
  existing `save_batch` duplicate-location DEBUG/WARN lines unchanged.
- sciduckdb: INFO on lazy conversion `X.col: DOUBLE -> row_list (DOUBLE[]), N stored rows wrapped` + timing.
- Load: INFO per load with row_list columns: cells unwrapped vs kept as lists, per column.
- Reader audit: DEBUG at each routed reader naming the row_list columns it handled.

## Stages

1. sciduckdb: row_list kind (inference with `row_list_columns`, storage wrap,
   validation, `unwrap_row_list`, `row_list_change`, `row_list_sql`) +
   `convert_column_to_row_list` + tests.
2. scifor: `combine_duplicate_rows` + `CombineReport` + `CombineMixedTypesError` + tests.
3. scidb: `validate_on_duplicate`, `combine_key_columns`, `save_batch(row_list_columns=)`,
   reconcile extended + conversion call, `BaseVariable.save` / `save_from_dataframe` /
   `for_each` plumbing + tests.
4. Reader audit (findings recorded here) + routing + tests per reader.
5. MATLAB: bridge + `for_each.m` / `save_from_table` name-value + tests.
6. GUI: run option + RemedyDialog button + manual-test doc entry.
7. Docs: `duplicate-location-vs-stale.md` §4/§5 (third remedy), `error-surfacing.md` §8,
   `duckdb-column-types.md` (row_list kind + lazy conversion), scifor README; memory.

## Tests

scifor (`tests/test_combine_duplicate_rows.py`):
- unique keys -> table unchanged, empty report
- identical rows -> one row, scalars
- differing column -> list in row order; equal column -> scalar
- 3 rows `[60,60,61]` -> full list (alignment kept)
- NaN: `[NaN,NaN]` -> NaN; `[5,NaN]` -> list
- NULL key values group together
- non-scalar cell in a duplicate group -> group left uncombined + reported
- mixed types `[5,"x"]` (and list vs scalar of another class in the same column)
  -> `CombineMixedTypesError` with remedy; `[5,5.5]` -> ok
- datetime cell needing a list -> `CombineMixedTypesError` (not supported yet)
- `list_columns` / `collapsed_columns` reported correctly

sciduckdb:
- row_list round trip: `[5,12]` -> list, `[7]` -> `7`, `[NaN]` -> NaN, strings, bools
- scalar saved into an existing row_list column is wrapped, loads as scalar
- `convert_column_to_row_list`: DOUBLE / BIGINT / VARCHAR / BOOLEAN / NULL cells
  wrapped, every old row loads unchanged; failure inside the transaction rolls back
- `row_list_change`: row_list vs ndarray both ways -> refuse; BIGINT[] <- fractions -> widen
- a genuine 1-element `ndarray` column still loads as an array
- `row_list_sql` agrees with `unwrap_row_list` on the same table

scidb (`tests/test_combine_duplicate_locations.py`):
- combine mode, no duplicates -> stored types identical to default mode
- identical rows -> one record, stays scalar column
- differing column -> row_list; other locations load scalar; untouched columns stay scalar type
- D1 scenario: run 1 (error) `7` -> run 2 (combine) `7,8` -> conversion, old
  record loads `7`, latest loads `[7,8]`; run 3 dup removed -> `7`, column stays row_list
- old records' content hashes / record_ids unchanged by conversion
- single-data-column and multi-column paths of `BaseVariable.save`; `save_from_dataframe` direct
- for_each flatten path and normal path with `on_duplicate="combine"`; one graph record per combined record
- variants (different `__rid_*` / branch params) are not combined
- default `"error"` still raises `DuplicateLocationError`
- uncombinable (array-cell) duplicate in combine mode -> still `DuplicateLocationError`
- mixed types -> `CombineMixedTypesError`, nothing written
- invalid `on_duplicate` value -> clear error

Readers: one test per routed reader (from the audit), incl. Plot Studio treating a row_list column as 1-D.
scimatlab: `save_from_table` + `for_each` with `'on_duplicate','combine'` round trip
(S01 list, S02 scalar), incl. the D1 conversion sequence.
GUI: run-option round trip; remedy button sends the option.

## Risks / known limits

- Datetime row_list columns not supported in v1 (datetimes need the `__tz`
  companion per element); refused with a clear message.
- Standalone MATLAB scifor (no database) does not get the feature.
- Plot Studio sees row_list columns as 1-D vectors (§6) - mean/median of a
  multi-stay subject, not each stay as its own point.

## Build log (2026-10-29)

Files:
- sciduckdb: `row_lists.py` (new: kind, `row_list_column_type`,
  `row_list_to_storage`, `unwrap_row_list`, `row_list_change`,
  `convert_column_to_row_list`), `sciduckdb.py` (`infer_data_columns(
  row_list_columns=, more_values=)`, storage branches, `storage_to_python(_column)`
  unwrap + INFO count, `record_schema_mismatch(row_list_columns=)`), exports.
- scifor: `combine.py` (new: `combine_duplicate_rows`, `CombineReport` incl.
  `output_row`, `CombineMixedTypesError(UserValueError)` with remedy), exports, README.
- scidb: `combine.py` (new: `ON_DUPLICATE_VALUES`, `validate_on_duplicate`,
  `combine_key_columns`, `combine_table`, `combine_for_save`); `variable.py`
  (`save`/`save_from_dataframe(on_duplicate=)`); `database.py`
  (`save_batch(row_list_columns=)`, `_reconcile_row_lists` on both save paths);
  `foreach.py` (`for_each(on_duplicate=)`, `_ForEachState.on_duplicate`,
  combine in `_save_results`); `exceptions.py` (third remedy step
  `combine_rows`); exports.
- scimatlab: `bridge.py` (`for_each_save(on_duplicate=)`,
  `save_batch_bridge(on_duplicate=)`, `_combine_bridge_items`);
  `+scidb/for_each.m`, `+scidb/BaseVariable.m` (`'on_duplicate'` name-value).
- GUI: `run_failures.on_duplicate_from_config`; run.py, execution_service,
  matlab_command, code_export pass/emit it; `layout_service.set_on_duplicate`
  + PUT `/layout/{node_id}/on-duplicate`; frontend: run-option checkbox
  **Combine duplicate rows**, RemedyDialog **Combine rows and run again**,
  `RERUN_NODE_EVENT` (FunctionNode re-runs with the option patched in).
- Tests: `scifor/tests/test_combine_duplicate_rows.py`,
  `sciduckdb/tests/test_row_lists.py`,
  `scidb/tests/test_combine_duplicate_locations.py` (+ remedy steps updated in
  `test_duplicate_location_in_batch.py`), `scistackplotdb/tests/test_row_list_columns.py`,
  `scimatlab/tests/test_bridge_combine.py`, MATLAB `TestSaveFromTable.m` (5),
  `TestSpreadParity.m` (2, helper `table_with_duplicate_subject.m`),
  `scistack-gui/tests/test_on_duplicate_option.py`.
- Docs: `duplicate-location-vs-stale.md` §4/§6, `error-surfacing.md` §8,
  `duckdb-column-types.md`, `docs/gui-manual-testing-todo.md` 0zzzzzzt.

Reader audit (§6) findings:
- Through `storage_to_python` (unwrap for free): scidb load / load_all /
  spread + as_table / for_each inputs, scidb CSV export (`var.load`), MATLAB
  loads, SciDuck.load.
- Raw SQL, no dtype meta: Plot Studio `scistackplotdb.load` (load_variable,
  sample_value, sample_column_value, column_levels) and GUI
  `api/variables.get_variable_plot_data` (default mini-plot). Kept as 1-D
  vectors (decision); DEBUG line added in `load_variable`; test added.
- Presence/counts only (no change needed): data inventory, record listings,
  clear_data, variant_delete, rename (copies raw rows), undo/redo snapshots.
- Not reached: scistackstatsdb reads through Plot Studio's source.

Deviations from the plan text:
- `row_list_sql` NOT built: no audited reader needs scalars from SQL.
- `CombineReport.output_row` added (MATLAB bridge maps ids back per input
  row without a second grouping owner).
- A column that is neither key nor value (explicit `value_columns`) keeps
  rows apart like a key (never drop data).
- Non-combined rows keep their exact frames/items (`infer_objects`, original
  bridge items) so their record ids do not change.
- GUI: a dedicated `set_on_duplicate` endpoint (merge one key, like
  `set_shown_argument`) instead of re-sending the whole node config.

Known risks to check when running the tests:
- `ALTER TABLE ... SET DATA TYPE` on a table with `record_id PRIMARY KEY`
  (single-column variables). No precedent in the codebase; the sciduckdb
  conversion tests use PK tables on purpose. If DuckDB refuses, the fix is
  in `convert_column_to_row_list` alone (add/copy/drop/rename).
- MATLAB conversion of a loaded list cell inside a table (TestSpreadParity
  combine test accepts cell or numeric).
