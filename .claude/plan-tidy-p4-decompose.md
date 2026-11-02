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
- (none yet)
