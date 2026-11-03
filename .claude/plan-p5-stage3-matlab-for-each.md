# Plan — P5 stage 3: MATLAB keeps only the per-combo call (M3)

*Parent: `.claude/plan-tidy-p5-cross-language.md` (Stage 3). Index:
`.claude/plan-tidy-00-index.md` (global rules + design bar). Design review:
`docs/design-review/05-scidb-satellites.md` (M3, shared-vs-mirrored table).*

## Goal

`+scifor/for_each.m` (3,914 lines) decides almost everything a run does in
MATLAB: combos, `[]` resolution, distribute target, as_table, locations,
share_limits, column groups, summary text, journal. Python's `scifor.foreach`
decides the same things for Python runs. After this stage **Python decides,
MATLAB applies**: MATLAB describes its inputs once, Python returns one plan,
MATLAB runs the function per combo and reports what happened, Python writes
the summary.

## The shape

```
+scifor/for_each.m
  describe inputs (key columns, column names, kinds)  --1 JSON + 1 numeric buffer-->
                                py.scifor.matlab_loop.plan(...)  -> plan_run (THE planner)
  <-- 1 JSON (combos as value pools, labels, deliveries, groups, ...)
      + int64 buffers (combo index matrix, row indices + offsets) + float64 buffer (limits)
  for each combo: slice inputs by the plan's row indices, call fn, catch, collect
  report the outcome (counts, failures by reason, skips)  --1 JSON-->
                                py.scifor.matlab_loop.assemble(token, ...)  -> _log_summary
  <-- 1 JSON (summary for summary_fn, combine column list)
  build the result tables (MATLAB values)
                                py.scifor.matlab_loop.close(token, error)   -> journal block
```

Crossings per run: plan (1 call), assemble (1), close (1), plus a progress
call only when a progress line is due (time-gated, at most one every 2 s).
No per-row and no per-combo crossing; selections cross as one index buffer.

## Where the owner lives, and why not `scimatlab.bridge`

`scifor.matlab_loop` (new module in **scifor**, not imported by
`scifor/__init__`). Importing anything under `scimatlab` imports scidb
(`scimatlab/__init__` -> `bridge/__init__` -> `entry` -> scidb), and MATLAB
scifor must stay scidb-free (stage 2 made the same call for
`scifor.options`). **Deviation from the brief** ("everything crossing goes
through `@matlab_entry`"): these entries cannot import
`scimatlab.bridge.crossing`. Their wire form is JSON text and `bytes` only,
which by construction carries no datetime/timedelta scalar (the hazard
`@matlab_entry` guards). `test_bridge_plan_roundtrip.py` asserts every
returned value is `str`/`bytes`.

## Responsibility map

| Responsibility in `+scifor/for_each.m` | Owner after this stage | MATLAB keeps |
|---|---|---|
| option values -> MATLAB types | (unchanged) `split_options` | yes (type coercion only) |
| struct-array gotcha (`collapse_struct_array`) | MATLAB | yes (MATLAB-only syntax) |
| EachOf expansion (recursive calls) | MATLAB | yes: alternatives are MATLAB objects; each branch plans in Python |
| output names (`outputs=` / count / nargout) | `matlab_loop.matlab_output_names` (MATLAB contract: N -> N x `'output'`, one table each) | no |
| `schema_keys=` sugar | `plan_run` -> `expand_schema_keys` | no |
| `[]` from table columns | `plan_run` -> `_resolve_empty_iterables` -> `_distinct_values_from_inputs` on surrogate frames | no |
| categorical key column -> numbers when lossless | `matlab_loop._decode_key_column` | no |
| PathInput discovery, Case A/B, static-template drop, "no source" error | `plan_run` -> `resolve_pathinput_discovery` | no |
| declared key types (numeric canonicalisation, string exact match) | `plan_run` -> `_apply_declared_key_types`; per-combo paths resolved in Python | no |
| effective keys (no schema -> iterated keys filter) | `matlab_loop` (MATLAB contract, kept) | no |
| distribute target + conflict with a constant | `plan_run` -> `_distribute_target`; "top of schema" flag via `_iterated_schema_keys` | no |
| schema-key column class capture (round-trip types) | `matlab_loop._column_types` (first-seen class, conflict) | the cast itself (`cast_schema_column`) |
| static `ColName(tbl)` | `plan_run` -> `_resolve_static_colnames` | no |
| data vs constant | `plan_run` -> `_classify_inputs` (surrogates) | no |
| as_table set | `plan_run` -> `_as_table_set` | no |
| for_columns: iterate inputs, all-columns, group equality, combine/split, overlap WARN, ColName()/`{ColName}` checks | `plan_run` -> `_plan_for_columns` | per-group calls, `group_has_no_data`, `expand_column_result` (MATLAB values) |
| combos (Cartesian / discovered / prebuilt) | `plan_run` -> `_build_combos` | builds structs from the plan's pools |
| `locations=` | `plan_run` -> `filter_combos` | no |
| per-combo row selection (schema filter + scidb record ids) | `matlab_loop._row_slices` (MATLAB comparison rule: numeric column vs text value parses) | slices by index |
| NoData when empty | Python decides per input (`no_data_when_empty`) | applies after `where` |
| `where=` | MATLAB (ColFilter compares MATLAB values) | yes (noted: remaining mirror) |
| Merge join / broadcast / constant-key add-back | MATLAB (value operation on MATLAB tables) | yes |
| extraction shape (scalar unwrap, column vector, struct rebuild) | MATLAB (value model) driven by Python's delivery (columns, mapping, as_table) | yes |
| PathInput / Fixed(PathInput) per-combo path | `matlab_loop` (`PathInput.load`, exact-string resolver, Fixed overrides); errors become combo failures | only the scidb `_pathinput_loader` callback (DB policy) |
| PathOutput per-combo path | `matlab_loop` (`PathOutput.resolve`) or scidb's pre-resolved list | `{ColName}` substitution per group |
| share_limits grouping + lookup | `_compute_shared_limits` on surrogate frames of MATLAB-computed per-row extents; `_inject_shared_limits` per combo | per-row extents (MATLAB values), trailing positional args |
| positional capacity for limits, arity WARN | `matlab_loop` (MATLAB-only concept) | no |
| banner, inputs line, options line, value previews | `plan_run` -> `_announce` | no |
| dry run text | `matlab_loop.plan` (captured `_announce` + `_print_dry_run_iteration`) | prints the returned text |
| combo labels (`subject=1, session=A`) | `matlab_loop` (one per combo) | no |
| progress line | `foreach._progress_line` via `matlab_loop.log_progress` | time gate only |
| failure grouping record (first report per identifier) | MATLAB (MException report) | yes (bookkeeping + WARN with `getReport`) |
| end-of-run summary lines, all-failed WARN, output-skip lines, combine no-data lines | `assemble` -> `_log_summary` | no |
| failure groups wire shape | `groups_from_wire` + `ComboFailure.from_dict` + `groups_to_wire` | no |
| `summary_fn` struct | `assemble` returns it | calls `summary_fn` |
| combine column list | `assemble` -> `_combine_output_columns` | reindex rows (missing values by MATLAB type) |
| on_error=stop line | `foreach._stop_line` via `assemble` | rethrows the MException |
| journal block | `matlab_loop.close` | no |
| result tables (flatten/nested, metadata columns, categorical) | MATLAB (MATLAB values) | yes |
| column drift check (an iterate column missing) | `_check_column_drift`, run once by `matlab_loop._run_plan` (a MATLAB table is the same at every combo); refuses the call | no |

**Deviation (assembly):** P5's text has `scifor_assemble` return the result
table. Building the table in Python would push every result value through
`to_python` one at a time (per-row crossings), and the table goes back to
MATLAB (standalone) or to `for_each_save` (scidb, which already does
bulk per-column transfer). Python owns every assembly *decision*; MATLAB
stacks its own values.

## The wire (see `scifor/matlab_loop.py` module docstring for the full text)

- MATLAB -> Python: `plan(description_json, numbers, prebuilt_combos=,
  row_selection=, resolved_path_outputs=, path_inputs=)`.
  - `numbers`: one `bytes` of float64 (numeric key columns, numeric
    iterables, per-row extents), referenced by `[offset, count]`.
  - scidb's `full_combos` / `row_selection` / `resolved_path_outputs` cross
    as the Python objects prepare returned (no MATLAB conversion at all).
- Python -> MATLAB: `{"json", "combo_index" (int32, n_combos x n_keys, 0 =
  absent), "row_index" (int64, 1-based), "row_offsets" (int64, n_slices x
  (n_combos+1)), "limits" (float64, n_combos x n_limits x 2)}`.

## Slices

All four in one session (the user tests once at the end).

- **(a) combos from Python.** `matlab_loop.plan` + surrogates; MATLAB
  builds combos from pools + index matrix; banner, locations, `[]`
  resolution, distribute target in Python. `+scidb/for_each.m` passes
  `full_combos` / `row_selection` / `resolved_path_outputs` through.
- **(b) input preparation from the plan.** Row index slices, deliveries,
  per-combo paths, limits.
- **(c) assembly in Python.** `assemble` / `close`; `ComboFailure.from_dict`.
- **(d) delete the dead MATLAB.** Every local function whose decision moved.

## Logging (every crossing)

- `[matlab-plan] received <n> input(s), <bytes> numeric byte(s), prebuilt=<n>, decode <s>s`
- `[matlab-plan] planned <n> combo(s) x <k> key(s), <s> slice(s), <r> row index(es), <bytes> byte(s) out in <s>s`
- `[matlab-assemble] <n> failure reason(s), <k> skip(s) in <s>s`
- `[matlab-close] journal written|skipped (<reason>)`
- MATLAB: `[timing] matlab_plan: <s>s (describe <s>s, python <s>s, decode <s>s)`,
  `[timing] matlab_loop: <n> combo(s) in <s>s`, `[timing] matlab_assemble: <s>s`.

## Tests

- `scimatlab/tests/test_bridge_plan_roundtrip.py`: drives
  `matlab_loop.plan/assemble/close` with descriptions shaped exactly as
  `for_each.m` sends them (combos, `[]` resolution, locations, distribute,
  row slices, scidb row selection, for_columns groups, share_limits, paths,
  dry run, failures summary, wire types are `str`/`bytes`).
- `scifor/tests/test_failures.py`: `ComboFailure.from_dict` round trip.
- `scimatlab/tests/test_matlab_loop_source.py`: static guard that
  `+scifor/for_each.m` calls the three entries and no longer defines the
  moved functions (slice d), and that `+scidb/for_each.m` no longer converts
  combos per combo.
- MATLAB: `scimatlab/tests/matlab/scifor/TestSciforForEachPlan.m` (plan
  crossings logged, labels, index slicing, prebuilt py combos).

## Progress

- 2026-11-02: all four slices written in one session; nothing run yet (the
  user tests once at the end). pyright: no new errors (numpy/pandas/pytest
  are not installed for pyright, so those imports show as unresolved).
  - (a) `scifor.matlab_loop.plan` + surrogates; MATLAB builds combos from
    pools + an int32 index matrix. `foreach.iterated_schema_keys`,
    `progress_line`, `progress_guards`, `stop_line` factored out so both
    loops print the same text.
  - (b) row slices (int64 buffer), deliveries, per-combo PathInput /
    PathOutput paths (errors become that combo's failure; the identifier
    rule moved to `scifor.pathinput.matlab_error_id`, which `PathInput.m`
    also calls), share_limits from MATLAB per-row extents.
  - (c) `assemble` (`_log_summary`, `ComboFailure.from_dict`, combine
    columns) and `close` (journal). Refused calls come back as
    `{error_id, error_message}` (`refusal_id`) so `scifor:for_each` /
    `scifor:ColName` / `forColumnsGroups` identifiers the MATLAB tests pin
    are kept.
  - (d) 37 MATLAB local functions deleted; `+scifor/for_each.m` 3,914 ->
    ~2,580 lines. `+scidb/for_each.m` no longer converts combos, row
    selection or PathOutput paths per combo.
  - Open: the user's pytest (scifor, scimatlab) and MATLAB suite; one real
    pipeline run with timing compared against a pre-change `scistack.log`.
- 2026-11-02, MATLAB run 1: 432/432 failed on one parse error (a scripted
  edit cut the `describe_input` header; shell expanded perl's `$1`). Fixed;
  guard `test_no_code_between_functions`.
- 2026-11-02, MATLAB run 2: 34 failures, two causes, both fixed:
  - scidb's record-id selection was applied to Fixed / ColumnSelection
    inputs, whose tables have no record-id column (the bridge strips it):
    zero rows, nothing saved. `_Slicer` now ignores a selection for a table
    without ids (DEBUG line). Test:
    `test_record_id_selection_ignored_for_a_table_without_ids`.
  - scidb's PathInput loader raising `scidb:SchemaKeyTypeError` became a
    per-combo failure instead of ending the run. The plan now sends
    `fatal_ids` (`matlab_loop.FATAL_IDS`, the MATLAB spellings of Python's
    `scifor_fatal` / `ForColumnsError` rule); both catch sites in
    `run_combos` rethrow them (replaces the hard-coded for_columns check).
    Test: `test_fatal_identifiers_end_the_run`.
- 2026-11-02, MATLAB run 3: 1 failure, `TestForColumns/test_missing_column_raises`.
  Stage 3 had dropped the column-drift check as moot for fixed MATLAB
  tables, but it is also what rejects a for_columns column the table never
  had. `_run_plan` now calls `_check_column_drift` once on the surrogate
  frames (refusal -> `scifor:for_each`). Test:
  `test_for_columns_missing_column_refuses_the_call`.
- 2026-11-02, MATLAB run 4: all green (pytest and the MATLAB suite). Open: one real pipeline run, timing vs a pre-change `scistack.log`.
