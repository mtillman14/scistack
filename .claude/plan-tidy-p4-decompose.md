# Plan P4 — decompose the giant functions; one owner of "latest"

*Index: `.claude/plan-tidy-00-index.md` (global rules apply). Themes T4 and T5
in `docs/design-review/09-synthesis.md`. P3 is done, so code is already where
it will live. Refactor IDs: R1, R2 (doc 02); W2, W3, W4, W5 (doc 04b); Q1, Q2
(doc 04c); B1 (physical move), B2 (doc 04a); PD1 (doc 06); M2 (doc 05);
G7 (doc 07).*

**Method for every stage.** Behaviour-preserving extraction. Before touching
a giant function, pin its behaviour:
- the package's existing tests must be green;
- add **characterization tests** for any path the stage touches that has
  none. Find one by grepping the tests for the branch's log message.

Each stage is its own session and its own commit. Extract one phase at a
time. Run the user's tests after each phase, not only at the end of the
stage. Keep the existing log lines verbatim: MATLAB timing archives and the
journal grep for some of them (`TOTAL=`, `[batch_save]`, `[provenance]`).

## Stage 1 — scifor `for_each`: plan / run_combo / assemble (R1) + `LoopHooks` (R2)

`scifor/src/scifor/foreach.py` `for_each` runs from ~147 to ~1372 (1,226
lines, 21 params). Its steps are marked `# Step -2 … # Step 7.5`, then the
per-combo loop, then `_results_to_output_dataframe`.

1. `@dataclass class RunPlan` holds everything steps −2 … 7.5 compute:
   - output names, resolved iterables, `distribute_key`;
   - data / constant inputs, the `as_table` set, the for_columns plan;
   - the combo list after locations, shared limits;
   - key types, `on_error`.

   `def plan_run(fn, inputs, outputs, …) -> RunPlan` is steps −2 … 7.5
   moved verbatim. EachOf expansion (step −1) recurses into `for_each`; keep
   that at the top of `for_each`, before `plan_run`.
2. `def run_combo(plan, combo, hooks) -> ComboResult` holds:
   - the per-combo body: cancel check, input prep, NoDataError, PathInput,
     the call, NoOutput, distribute;
   - failures returned as a `ComboFailure` in the result, not raised, except
     under `on_error="stop"`.

   Progress-line state moves into a small `ProgressReporter` object.
3. `def assemble(plan, results) -> DataFrame` covers the summary event plus
   `_results_to_output_dataframe`.
4. `@dataclass class LoopHooks` gathers `all_combos`, `path_input_resolver`,
   `mapping_inputs`, `select_rows`, `keep_no_output`, `log_fn`,
   `progress_fn` and `cancel_check`, with a docstring per field (the meaning
   currently lives in scidb).
   - `for_each(…, hooks: LoopHooks | None = None)` replaces the eight
     `_`-params.
   - Update scidb's `_for_each_execute` (`scidb/foreach.py` ~875–1100),
     which builds them.
5. `for_each` becomes `plan_run` → loop `run_combo` → `assemble`, about 60
   lines. Its public signature is unchanged except the hooks.
6. Tests:
   - `scifor/tests/test_run_plan.py`: `plan_run` on representative inputs
     gives the same combos / names / as_table as before. Characterize first
     on main, then assert after.
   - The existing scifor suite must stay green.
   - Also run `python -m pytest scimatlab/tests/test_matlab_for_each_parity.py -x -q`.
7. User runs `scifor`, `scidb` and `scimatlab`.

## Stage 2 — scidb `for_each` options bundles (W3)

`scidb.for_each` has 28 parameters (`scidb/foreach.py` ~257).

1. Keep the **keyword signature** for users. Internally, group the options at
   the top of `for_each` into:
   - `RunOptions`, which already exists in `foreach_config` and holds the
     identity-bearing options (`as_table`, `distribute`, `across_variants`,
     iteration level);
   - `Selection` options: `where`, `locations`, `schema_filter`, metadata
     iterables;
   - `RunContext`: `step`, `parameter_names`, `finalized`, `pipeline`,
     `_progress_fn`, `_cancel_check`, `_pre_combo_hook`,
     `_inject_combo_metadata`.
2. Pass the bundles to `_for_each_prepare`, `_ForEachState` and
   `_save_results` instead of loose arguments. The review counted 17
   parameters on `_save_results`.
3. **One declaration of the option list.** Make a module-level
   `FOR_EACH_OPTIONS: tuple[OptionSpec, ...]` in `foreach_config` (name, kind,
   default, bundle, `db_only`). Then:
   - a test asserts `inspect.signature(scidb.for_each)` matches it;
   - `scifor.outputs.DB_ONLY_KWARGS` is derived from it. Today scifor owns
     that set and scidb's test checks parity. Pick the owner as follows:
     scifor cannot import scidb, so the declaration lives in **scifor**
     (`scifor/outputs.py` or a new `scifor/options.py`) and scidb extends it.

   P5 uses this for MATLAB `split_options` and GUI MATLAB command generation.
4. User runs `scifor`, `scidb`, `scimatlab` and `scistack-gui`.

## Stage 3 — scidb save: `SavePlan` → `apply` (W2)

`_save_results` (~5724, 712 lines) has `# PHASE 1 … 4` banners. After P0, its
phases 2–4 are in one transaction.

1. `build_save_plan(state, result_tbl, outputs, …) -> SavePlan` is PHASE 1
   moved verbatim: items per output, input edges + selectors per row,
   invocation id per edge set, the selection round-trip guard.
2. `apply_save_plan(plan, db) -> SaveReport` holds phases 2–4 inside the
   transaction: `save_batch` per output, lineage-only `generates_file`
   outputs, `record_run`, source capture.
3. Split `DatabaseManager.save_batch` (624 lines, ~2275) the same way:
   `plan_batch_rows(cls, items) -> BatchRows`, which covers metadata
   canonicalization, type reconciliation, row lists, combine and record ids;
   then `write_batch_rows(rows)`, which does the inserts.
4. Tests:
   - characterize `SavePlan` on a 2-output and a `distribute` run (record ids,
     edge counts);
   - P0's atomicity tests must stay green.
5. User runs `scidb`, `scimatlab` and `scistack-gui`.

## Stage 4 — one owner of the supersession ("latest") rule (Q1)

Five implementations (doc 04c weakness 1):
- `_find_record`'s `ROW_NUMBER()` (~3217);
- `get_latest_record_id_for_variant` (~5690);
- `state._get_latest_record_at_location` (~957);
- `provenance_query.latest_at_location_batch` (~3259, which says it
  "mirrors" the two above, with a NULL `schema_id` difference);
- `variant_identity_batch.is_latest`, the deliberate display rule. Keep it.

1. Owner: `provenance_query.latest_at_location_batch(duck, record_ids) ->
   {rid: {latest_variant, latest_any, …}}`. Make it export the SQL fragment
   it uses (`LATEST_PER_VARIANT_SQL`) for `_find_record`'s in-query use.
2. Rewrite `get_latest_record_id_for_variant` and
   `_get_latest_record_at_location` as one-line calls of the batch function.
3. `_find_record` uses the exported fragment, so one SQL text exists.
4. **NULL `schema_id`.** Decide the behaviour by test, before changing code:
   - write `test_latest_null_schema_id` covering a record with NULL
     `schema_id` (a constant, or a whole-dataset aggregate) under each old
     function;
   - record what each returns today;
   - pick the batch function's behaviour (it is documented), and tell the
     user which caller's behaviour changes. Ask before changing it if any
     GUI-visible state would differ.
5. Log at DEBUG in the owner: `latest_at_location_batch: N rids, M
   superseded`.
6. User runs `scidb` and `scistack-gui`.

## Stage 5 — split `DatabaseManager`; `RecordStore` read half; plot loads through it (B2, Q2, PD1)

`DatabaseManager` (`database.py` ~1252, 91 methods).

1. Create `scidb/record_store.py` with `class RecordStore`, constructed with
   the `DuckConnection`:
   - **read:** `select_records(type, Selection) -> records frame`, from
     `_find_record` and `_load_with_where`;
     `materialize(records) -> data frame`, from
     `_assemble_df_from_records_and_data` and `_load_as_df_via_iterator`;
     `long_frame(type, include_data: bool, with_variants: bool)`, the bulk
     read plotting needs;
   - **write:** stage 3's `plan_batch_rows` / `write_batch_rows`.
2. `DatabaseManager.load`, `load_all_as_df`, `save` and `save_batch` become
   thin delegations with the same signatures. Delete the provenance
   pass-throughs (`get_provenance`, `get_provenance_by_schema`,
   `get_upstream_provenance`, `has_lineage`, `get_pipeline_structure`,
   `find_record_id`, …). Point their callers at `provenance_query` / the
   `Inspector`. Grep each name first: scidb-net was the main external
   caller, and it is gone after P1.
3. Display config methods (`dataset_aliases`, `dataset_colors`,
   `dataset_schema_key_order`) → callers read `scifor.project.*` directly,
   which is their home after P3.
4. **PD1:** `scistackplotdb/load.py` `load_variable` (~513) calls
   `RecordStore.long_frame`. Its SQL against `_registered_types`,
   `_variables`, `information_schema` and the data tables moves into
   `RecordStore`.
   - **Acceptance test:** keep the plot-at-scale behaviour. The
     metadata-only mode must not select data columns, and payload columns go
     through `fetchdf`. Read `.claude/plot-at-scale-plan.md` §7 before
     starting.
   - Keep the `Log.timer("load_variable", …)` phases.
5. User runs `scidb`, `scistackplotdb`, `scistackstatsdb` and
   `scistack-gui`. Then open a large variable in Plot Studio (the user does
   this) and compare `[timing] load_variable` with a pre-change log line.

## Stage 6 — scidb physical subpackages (B1, move half)

With P2's layer test in place, move the clusters into `scidb/model/`,
`scidb/store/`, `scidb/provenance/`, `scidb/run/` and `scidb/inspect/`
(exists). `scidb/project/` exists from P3.

1. Move one cluster per commit, in order model → store → provenance → run.
2. Each move: `git mv`, fix relative imports, update
   `scidb.SUPPORTED_MODULES` and the consumer imports (beta rule: no
   aliases), update the layer map.
3. After each move, try to **delete allow-list lines**. Some lazy imports
   become top-level imports once a cycle is gone. Lower the lazy-import
   baseline.
4. User runs `scidb` plus every consumer package after each cluster.

## Stage 7 — rename "version keys" to "call-site keys" (W5, identifier half)

`grep -rn "version_keys\|version keys\|to_version_keys" --include='*.py' scidb scistack-gui scimatlab scifor`.

1. Rename the identifiers (`to_version_keys` → `to_call_site_keys`,
   `config_keys`, …) consistently. Read `database-model.md` first:
   `version_keys` is gone as a table column. This is only the in-memory
   payload's name. Check that no **stored** JSON key changes. If one does
   (for example, a key inside a stored variant config), STOP and ask: it
   would change ids.
2. User runs `scidb`, `scimatlab` and `scistack-gui`.

## Stage 8 — identity table (W4)

1. Add `docs/claude/identity-table.md`. Rows are facts (function hash, input
   types, input record ids, selectors, constants, `as_table`, `distribute`,
   `across_variants`, iteration level, function name, output types, PathInput
   names). Columns are ids (`record_id`, `invocation_id`, `call_id`,
   `wiring_id`). Each cell says yes / no / when non-empty.
2. Add `scidb/tests/test_identity_table.py`, which varies one fact at a time
   on a tiny run and asserts which ids change, matching the doc's table.
   Parametrize from a dict that mirrors the doc. The dict is the owner; the
   doc says so.
3. User runs `scidb`.

## Stage 9 — big files elsewhere (M2, G7)

1. **M2** `scimatlab/bridge.py` (3,061 lines, 58 functions) →
   `scimatlab/bridge/` with:
   - `entry.py`: the functions MATLAB calls via `py.scimatlab.bridge.*`.
     Re-export them from `bridge/__init__.py`, because the MATLAB call sites
     use `py.scimatlab.bridge.<fn>`. Find them all with
     `grep -rhoE "py\.scimatlab\.bridge\.[a-z_]+" scimatlab/src/scimatlab/matlab | sort -u`,
     and keep every one resolvable.
   - `wire.py`: wrapper reconstruction;
   - `convert.py`: `ndarray_to_buffer`, `split_*`, `convert_nested_*`,
     `datetimes_to_matlab`;
   - `pipeline.py`.

   Rename `MatlabLineageFcn` → `MatlabFunctionIdentity` (the GUI uses it in
   `api/pipeline.py` ~67). User runs `scimatlab`, then the user's MATLAB
   suite (`scimatlab/tests/matlab`).
2. **G7** `scistack-gui/frontend/src/components/PlotStudio/PlotStudio.tsx`
   (7,141 lines, 63 `useState`).
   - Introduce `usePlotSpecReducer` (`useReducer`) holding the spec-shaped
     state, with typed actions per rail group.
   - Split the five rail groups (`project_plot_studio_rail_groups`) into
     components that take `(spec, dispatch)`. Order: extract the reducer
     first (no UI change), then one rail group per commit.
   - `npm test` after each extraction (you may run it). Rebuild both
     bundles (index rule 7).
   - The user checks Plot Studio by hand. Add a manual-test entry per
     extracted group: "every control in <group> still changes the preview".

## Stage 10 — loaded scalars are Python scalars, not numpy scalars

Found in P1 (2026-11-01), when `SciDuck.load` was deleted: a saved Python
`True` / `42` / `3.14` loads from scidb as `np.True_` / `np.int64` /
`np.float64`. Then `x is True` is False, and `json.dumps(x)` raises for
`np.bool_` and `np.int64`. `storage_to_python` already returns the Python
scalar. The numpy type comes back because `sciduckdb.restore_types` assigns
the restored list back into a DataFrame column (`df[col] = [...]`), pandas
re-infers a numpy dtype, and scidb's single-column load then returns
`restored[col].iloc[i]` (`database.py`, the non-DataFrame branch of the
chunked load). The old `SciDuck.load` hid this by running `storage_to_python`
a second time.

**Owner:** `sciduckdb.restore_types` (the codec). Fix it there, not with a
second pass in scidb.

1. **Measure first.** Grep every reader of `restore_types` and every
   single-column read in scidb (`load`, `load_all`, `load_all_as_df`, the
   MATLAB bridge's `from_python` path). Log once per load, at DEBUG, how many
   restored columns hold Python scalars against numpy scalars, so the change
   is visible in `scistack-debug.log`.
2. **Identity check before changing anything.** A re-save of a loaded value
   must keep its record id. Check whether `canonical_hash(np.True_) ==
   canonical_hash(True)` (and the same for `int64`/`int` and
   `float64`/`float`). If they differ, this change alters record ids on
   re-save. Stop and ask the user before going further.
3. **Fix.** In `restore_types`, keep scalar columns whose meta is
   `bool`/`int`/`float`/`str` as an object column of Python values
   (`pd.Series(values, index=df.index, dtype=object)`). Leave array, dict
   and datetime columns as they are. Where whole-table loads
   (`load_all_as_df`) want numpy dtypes for speed, they use
   `storage_to_python_column`, which this stage does not touch. Confirm that
   in step 1.
4. **Tests.**
   - sciduckdb: `test_sciduck.py::TestCodecRoundTrip::test_save_and_load_bool`
     goes back to `assert loaded is True`. Add the same check for int and
     float (`type(loaded) is int` / `float`).
   - scidb: `BaseVariable.save(True)` → `.load().data is True`, and
     `json.dumps(loaded.data)` works.
   - Hash parity: a loaded-then-re-saved scalar keeps its record id.
5. User runs `python -m pytest sciduckdb/tests -x -q`, then
   `python -m pytest scidb/tests -x -q`, then
   `python -m pytest scimatlab/tests -x -q` (MATLAB receives these values
   through `from_python`).

## Progress
- **Stage 1 done (2026-11-01, untested — user runs all suites at the end).**
  `scifor/hooks.py` `LoopHooks`; `foreach.py` `RunPlan`/`ForColumnsPlan`,
  `plan_run`, `run_combo` -> `ComboResult`, `ProgressReporter`, `RunTally`,
  `assemble`. Deviations (design bar): `plan_run` is itself a composition of
  small step functions rather than one verbatim block; `_log_fn` dropped (was
  ignored); a single-underscore kwarg raises `TypeError`; EachOf
  alternatives receive the caller's hooks unchanged (before, the recursion
  dropped `_all_combos`; no caller combined the two). Parity tests exclude
  `hooks` (not a user option). New test: `scifor/tests/test_run_plan.py`.
- **Stage 2 done (2026-11-01, untested).** `scifor/options.py`
  (`OptionSpec`, `FOR_EACH_OPTIONS`, `UNSET`; `DB_ONLY_KWARGS` derived);
  `scidb/foreach_call.py` (`SCIDB_FOR_EACH_OPTIONS`, `RunSelection`,
  `RunContext`, `ForEachCall`). `scidb.for_each` = bundle -> `_run_for_each`
  (registration, step, schema selection, EachOf via `_run_alternatives`) ->
  `_run_concrete`. Deviation: `Selection` is `RunSelection` (name taken by
  `scidb.bindings.Selection`). `_save_results` already took `(tbl, outputs,
  state, db)`, so step 2's "17 parameters" was stale. Test:
  `scidb/tests/test_foreach_options.py`. The scidb lazy-import count dropped by
  5 (local `itertools` imports); baseline left at 517 until stage 6.
- **Stage 3 done (2026-11-01, untested).** `scidb/foreach.py`: `SaveItem`,
  `SavePlan`, `SaveReport`, `build_save_plan`, `apply_save_plan` (+ small
  phase helpers). `scidb/database.py`: `BatchRows`, `save_batch` =
  `_plan_batch_rows` -> `_write_batch_rows` -> `_log_batch_timings`.
  Deviation: `apply_save_plan(plan, state, db)` also takes the state (glue,
  parameter names, endpoint kind, fn for source capture are call facts); the
  JSON-string `__constants` fallback was dropped (config keys are always an
  in-memory dict). Test: `scidb/tests/test_save_plan.py`.
- **Stage 4 done (2026-11-01, untested).** Owner:
  `provenance_query.latest_at_location_batch` (+ `_is_superseded`, DEBUG
  `latest_at_location_batch: N rids, M superseded`) exporting
  `LATEST_SAVE_PER_RECORD_SQL`, used by `_find_record` and the owner's
  candidate query. Deleted `DatabaseManager.get_latest_record_id_for_variant`
  and `state._get_latest_record_at_location` (only caller:
  `_has_superseded_ancestor`, now one owner call). NULL `schema_id` before:
  variant half NULL-safe, any-variant half raised `TypeError`, batch
  `latest_any=None`; now the batch rule everywhere. Not GUI-visible (NULL
  schema_id records — constants, PathInputs, constant glue — are never
  variable inputs). Deviation: step 3's premise was wrong — `_find_record`'s
  `ROW_NUMBER` dedups the save log; its "latest" collapse is the separate
  LOAD rule (consumed locations + run-option families). That second
  definition of "variant" is reported to the user, not changed.
  Test: `scidb/tests/test_latest_null_schema_id.py`.
- **Stage 5 done in part (2026-11-01, untested).** `scidb/record_store.py`
  `RecordStore` (catalog, `long_frame`, `sample_cell`, `column_levels`,
  `occupied_keys`, `dtype_meta`; write: `write_batch_rows`, `BatchRows`,
  `insert_columnar_frames`), `db.record_store`. PD1 done: `scistackplotdb.load`
  has no SQL. Provenance pass-throughs deleted from `DatabaseManager` (tests
  call `provenance_query`). NOT done, by decision: (a) the read half's
  `select_records`/`materialize` move of `_find_record`/`_load_with_where`/
  `_assemble_df_from_records_and_data` — `_find_record`'s variant collapse is
  the LOAD "latest" rule stage 4 found to be a second variant definition, so it
  should move once that is decided; (b) `dataset_aliases`/`dataset_colors`
  stay: they validate the TOML against the dataset, which reading
  `scifor.project` directly would drop; `dataset_schema_key_order` is used by
  scidb's own sorting. Allow-list +1 (`scidb.record_store ->
  scidb.provenance`, the `_record` writer moved with the inserts). User step:
  compare `[timing] load_variable` on a large variable in Plot Studio.
  Tests: `scidb/tests/test_record_store.py`,
  `scistackplotdb/tests/test_load_through_record_store.py`.
- **Stage 6 done (2026-11-01, untested).** All four clusters moved at once
  (deviation: the plan said one cluster per commit with a test run between;
  the user asked for one run at the end). Renames forced by the layout:
  `scidb.provenance` (module) -> `scidb.provenance.graph`,
  `provenance_query` -> `scidb.provenance.query`, `provenance_save` ->
  `scidb.provenance.save`; layer `api` renamed `model`. Every relative import
  in scidb made absolute first; then one scripted rewrite of `scidb.<mod>`,
  `from scidb import <mod>`, and `scidb/src/scidb/<mod>.py` paths across .py,
  .m, .md, .toml, .txt (not `.claude/`, not TS). pyright with
  `reportMissingImports` over scidb, scimatlab, scistackplotdb, statsdb, GUI,
  tests/, scistack, scistacklog tests: no unresolved scidb import.
  `scidb/tests/test_imports.py` static half rewritten over full module names
  (via `tools.import_layers`). Not attempted: deleting allow-list lines /
  hoisting lazy imports (needs test runs). Lower `lazy_baseline` after
  `python -m tools.import_layers scidb/tests/import_layers.toml` reports the
  count (indented import lines fell by 6).
- **Stage 7 done (2026-11-01, untested).** `to_version_keys` ->
  `to_config_keys`, `CallSite.version_keys` -> `call_site_keys`,
  `constant_glue_version_keys` -> `constant_glue_config_keys`, manifest key
  `version_keys` -> `config_keys` (display only: `StepSpec.to_manifest` is
  never stored). Deviation: not "call-site keys" for the whole dict — it holds
  version-only keys too. STOP check: no stored JSON key changes (the nested
  metadata `"version"` key that `generate_record_id` hashes is untouched; local
  `version_keys` variables reading it keep their name). Prose saying "a
  record's version keys" (its non-schema metadata) is still accurate and kept.
- **Stage 8 done (2026-11-01, untested).** `docs/claude/identity-table.md`
  + `scidb/tests/test_identity_table.py` (`IDENTITY_TABLE` owner, `CALLS`
  baseline/variant per fact, a control row proving ids reproduce across two
  databases). Expectations were derived by reading the recipes; a failing row
  means either the recipe or my reading differs — check the recipe before
  changing the dict.
- **Stage 9 done in part (2026-11-01).** M2 done (untested in Python):
  `scimatlab/bridge/{__init__,entry,wire,convert,pipeline}.py`,
  `MatlabFunctionIdentity`, `test_bridge_entry_points.py`; tests importing
  bridge privates now import them from the owning submodule. G7 reducer half
  done and verified: `plotSpecReducer.ts` + test, `npx tsc --noEmit` clean,
  `npm test` 688/688, both bundles rebuilt; manual-test entry added. Deviation:
  the five rail-group component extractions were NOT done — each group's JSX
  reads many component-local values, so the split needs a view-state store
  first (a design step, not a mechanical move). User: run the MATLAB suite
  (`scimatlab/tests/matlab`) for the bridge split.
- **Stage 10 STOPPED at step 2, as the plan requires (2026-11-01).** Read
  from `scicanonicalhash.hashing`: `np.float64` hashes like `float`
  (subclass), but `np.int64` / `np.bool_` take the ndarray branch and hash
  differently from `int` / `bool`. So the fix changes record ids on re-save
  (back to the original save's id, which is the point — today a loaded and
  re-saved `42` forks a record). Done: step 1 measurement
  (`sciduckdb.codec._log_scalar_kinds`, DEBUG `restore_types: scalar cells
  (python, numpy) per column: ...`, `SCALAR_PTYPES`), and the characterization
  test `scicanonicalhash/tests/test_numpy_scalar_hash_parity.py`. Prepared, NOT
  applied: `.claude/p4-checkpoints/stage10-scalar-fix.patch` (step 3). Waiting
  for the user's yes; then apply it and write step 4's tests.
- **Stage 10 done (2026-11-01, untested).** User decision D1 (2026-11-01):
  "Yes, apply it. We want the data to be roundtripped as accurately as
  possible, and certainly to never create a new fork just by roundtripping
  data." Applied `stage10-scalar-fix.patch`: `restore_types` keeps
  bool/int/float/str columns as an object Series of Python scalars. Tests:
  `sciduckdb/tests/test_codec_storage.py` (int/float `type() is`, bool
  `is True`), `scidb/tests/test_scalar_roundtrip.py` (Python type, json.dumps,
  load-then-re-save keeps the record id). The hash-parity characterization
  test stays (it pins why the fix matters).
- **Follow-up (2026-11-01): one variant rule (user decision: "newer
  version").** `provenance_query.variant_keys_batch` is THE variant key
  (function name + accumulated constants + consumed input locations; never the
  function hash, so a code edit re-run supersedes) and
  `provenance_query.current_record_ids` THE "which records are current" rule
  (newest per variant per location, after run-option family, global
  run-option and renamed-function supersession). `_find_record`'s latest
  collapse, `latest_at_location_batch` (variant half),
  `current_records_by_schema_batch` all ask it; `_producing_variant_key`
  deleted. Behaviour change for node state: two records at one location that
  read DIFFERENT input locations are now two variants (before, staleness
  treated the newer as superseding). Kept unchanged and flagged: the
  "latest_any" staleness signal (any newer record of any variant at an input's
  location marks its consumers stale).
- **Fixes from the user's scidb run (2026-11-01):** `_for_each_prepare` copies
  `selection.metadata_iterables` (stage 2 regression: PathInput discovery in one
  EachOf arm mutated the shared dict, so the next arm iterated the first arm's
  subjects); two path-based guards updated for the stage-6 layout
  (`test_constant_glue_provenance` owner path, `test_no_user_identity` globs).
- **Follow-up (2026-11-02): option A + hand saves (user decision).** The
  "any newer record at the location" staleness check (`latest_any`) is gone
  from `latest_at_location_batch`, `_is_superseded` and
  `state._has_superseded_ancestor`: stale means only "the input is no longer
  the current record of its own variant" (ends the constant-sweep ping-pong).
  Added rule 4 to `current_record_ids` (`_replaced_by_hand_saves`): a hand save
  (raw variant, or a `__save__` invocation) supersedes every other variant's
  records saved BEFORE it at its location, so load and node state both treat a
  hand-correction as a replacement; a pipeline run after it coexists. Tests in
  `scidb/tests/test_one_variant_rule.py`.
