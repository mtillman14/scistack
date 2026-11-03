# Plan P5 — cut the copies across MATLAB, TypeScript and generated code

*Index: `.claude/plan-tidy-00-index.md` (global rules apply). Theme T5 in
`docs/design-review/09-synthesis.md`. P4 stages 1–2 must be done: this plan
uses scifor's `plan_run` / `assemble` phase boundaries and the
`FOR_EACH_OPTIONS` declaration. Refactor IDs: M1, M3, M4 (doc 05); G5, G8
(doc 07); P2, P3 (doc 06). Decision D5 applies: codegen keeps emitting
literal seaborn.*

**Pattern.** A rule that must exist in a second language is either:
- **called**: the other language crosses over to Python, when the call is
  rare enough that crossing cost doesn't matter; or
- **fed from one data file**: every language's tests read the same cases,
  when it must run natively.

`docs/claude/location-filter-cases.json` is the existing template.

## Stage 1 — MATLAB calls scifor for the small rules (M1)

1. `scimatlab/src/scimatlab/matlab/+scifor/frame_origin.m`: it runs once per
   failure frame, so crossing cost is irrelevant. It returns
   `char(py.scifor.failures.frame_origin(file))`.
   - The MATLAB-side framework directory (`+scifor`, `+scidb` under
     scimatlab's matlab dir) must be classified as "framework" by Python.
     Check that `scifor.failures.FRAMEWORK_PACKAGES` / `_framework_dirs()`
     covers `scimatlab/src/scimatlab/matlab`. If not, add it there; it is the
     one owner.
   - Keep the MATLAB file as the one-line delegator, with a header that names
     the owner.
2. `+scifor/output_column_name.m`: it runs once per output. It passes the
   class name and the `column_name` constant (if any) to
   `py.scifor.output_column_name`. Python cannot introspect a MATLAB object,
   so MATLAB extracts the two strings and Python applies the rule. If
   Python's function takes an object, add a small
   `scifor.outputs.output_column_name_from(text_or_class_name,
   column_name_override)` that the object version also uses.
3. Tests:
   - `scimatlab/tests/test_bridge_frame_origin.py` (Python side, frames from
     the MATLAB dir);
   - the MATLAB tests in `scimatlab/tests/matlab/`; the user runs them in
     MATLAB.
4. Merge `+scidb/Log.m` (204 lines) and `+scifor/Log.m` (171 lines):
   - keep `+scifor/Log.m`;
   - `+scidb/Log.m` becomes a call-through or is deleted. Check MATLAB user
     docs for `scidb.Log` first; if users call it, keep it as a three-line
     delegator, documented as the same class.

## Stage 2 — one `for_each` option list for MATLAB and the GUI (M4, G5)

P4 stage 2 created `FOR_EACH_OPTIONS`.

1. **MATLAB `split_options`** (`+scidb/for_each.m` ~1990) stops validating
   names itself. It passes every name/value pair to Python
   (`py.scimatlab.bridge.normalize_for_each_options(...)`), which validates
   them against `FOR_EACH_OPTIONS` and returns the normalized dict, or raises
   one error listing the unknown names.
   - Keep MATLAB-only coercions (string arrays → lists) in MATLAB. Move only
     the name/default/kind knowledge.
   - Log at DEBUG in Python: `normalize_for_each_options: <n> options`.
2. **GUI MATLAB command generation** (`scistack_gui/.../matlab_command*`)
   emits options by iterating `FOR_EACH_OPTIONS` (name, MATLAB spelling,
   default-omission rule), not a hand list. Grep the generator for each
   option name to find the hand-written list.
3. Test: a parity test that every `FOR_EACH_OPTIONS` entry is:
   - accepted by `normalize_for_each_options`;
   - either emitted or explicitly skipped by the GUI generator;
   - present in `inspect.signature(scidb.for_each)`.
4. User runs `scimatlab`, `scistack-gui` and the MATLAB suite.

## Stage 3 — MATLAB keeps only the per-combo call (M3)

The MATLAB loop `+scifor/for_each.m` is 3,941 lines; `+scidb/for_each.m`'s
main function is 1,028 lines.

1. After P4 stage 1, Python has `plan_run` and `assemble`. Add bridge entries:
   - `bridge.entry.scifor_plan(...)` returns a wire-form plan: the combo list,
     per-input row selections (by row index or record id), constants, output
     names, `distribute_key`;
   - `bridge.entry.scifor_assemble(plan_token, results)` returns the result
     table.
2. `+scifor/for_each.m` becomes:
   - call `scifor_plan`;
   - for each combo, slice its inputs by the plan's selections, call the
     user's function, catch `MException` →
     `failure_from_mexception`, collect;
   - call `scifor_assemble`.

   Everything else in the file (resolving `[]`, distribute targeting,
   as_table, locations, share_limits, NoOutput rules, column-group
   combining, summary text) is deleted, because Python now does it.
3. Do it in slices, with the parity suite green after each:
   - (a) combos from Python, everything else unchanged;
   - (b) input preparation from the plan;
   - (c) assembly in Python;
   - (d) delete the dead MATLAB.

   Watch transfer costs. Selections should be index vectors (one buffer), not
   per-row crossings (`docs/claude/matlab-bridge-transfer-paths.md`).
4. Tests:
   - `scimatlab/tests/test_matlab_for_each_parity.py` and
     `test_bridge_spread_parity.py` must stay green;
   - add `test_bridge_plan_roundtrip.py`;
   - MATLAB suite by the user, plus one real MATLAB pipeline run (the user)
     with the run timing compared against a pre-change `scistack.log`.
5. Update `docs/design-review/05-scidb-satellites.md`'s shared-vs-mirrored
   table.

## Stage 4 — location-filter semantics from one case file (P3), and the pattern for other rules

Four implementations today:
- `scifor/src/scifor/locations.py`;
- `scistackplot/src/scistackplot/reduce.py` `_location_mask` (~998);
- the emitted mask in `scistackplot/codegen.py`;
- `scistack-gui/frontend/src/components/PlotStudio/locationSelection.ts`.

1. Read `docs/claude/location-filter-cases.json` and
   `location-filter-semantics.md`. Make the JSON the owner of the cases and
   say so in the `.md`.
2. Each implementation's test suite reads the JSON and runs every case:
   - `scifor/tests/test_location_cases.py`;
   - `scistackplot/tests/test_location_cases.py`, which covers `reduce` and
     **executes** the codegen-emitted mask on the same frames;
   - `frontend/src/components/PlotStudio/locationSelection.cases.test.ts`.

   Each loads the file by a repo-relative path.
3. Any case that fails exposes a real divergence. Report it to the user
   before "fixing" one side: the semantics doc decides.
4. Apply the same pattern to the explicit TS mirror `groups.ts`
   (`PlotSpec.ordered_groups`). Generate `ordered-groups-cases.json` from
   Python in a test that writes it when missing and asserts it is current;
   the TS test reads it.

## Stage 5 — generated TypeScript types (G8)

1. Python side: a script `scistack-gui/tools/gen_ts_types.py`, which the
   **user runs**. It emits `frontend/src/generated/api-types.ts` from:
   - every `Handler.params` pydantic model (`model_json_schema()`);
   - `scistackplot.PlotSpec` and `scistackstats.StatsSpec`. These are
     dataclasses: use `pydantic.TypeAdapter(...).json_schema()`, or a small
     dataclass → JSON Schema walker if they hold unsupported types.

   Convert JSON Schema → TS with `json-schema-to-typescript` (npm; you may
   install it as a devDependency and run it).
2. The handler-table test (`scistack-gui/tests/test_api_handlers.py`)
   asserts the generated file is current: it regenerates into a temp dir
   and compares.
3. Migrate the frontend gradually:
   - `callBackend` gets an overload keyed by method name → params/result
     types;
   - replace the hand-written `StatsSpec` (`StatStudio/statsSpec.ts`) and
     `CustomPlotSpec` (in `PlotStudio.tsx`) with the generated types;
   - `npm test`, then rebuild both bundles.
4. The user runs the GUI suite. Commit the generated file.

## Stage 6 — codegen prints `reduce`'s plan (P2)

`scistackplot/codegen.py` (3,758 lines) re-derives the reduction in emitted
pandas/seaborn text (16 `groupby` sites). `reduce.py` has 8.

1. In `reduce.resolve`, record the reduction steps actually applied as a
   `ReductionPlan`: an ordered list of typed steps such as
   `CollapseMean(keys=…)`, `Compare(layer, level, mode)`, `Filter(…)`,
   `LocationMask(…)`, `Bin(…)`. Attach it to `ResolvedPlot`. The renderers
   ignore it.
2. Codegen emits pandas code **from the plan's steps**: one emitter function
   per step type. Reduction is then defined once, in `reduce`, and printed by
   codegen. The drawing half of codegen (seaborn calls) stays: it is a third
   renderer and is kept in line by the existing export-vs-preview parity
   tests.
3. Migrate one step type per commit. After each, run the parity tests
   (`test_legend_parity.py`, `test_compare_export.py`,
   `test_difference_bars_export.py`, `test_codegen*.py`) and confirm that
   **export still equals preview**. This is a standing user rule
   (`feedback_export_matches_preview`): a difference is a codegen bug.
   Never weaken an assertion.
4. User runs `scistackplot`, `scistackplotdb` and `scistack-gui`.

## Progress
- **Stage 1 done (2026-11-02, untested).** `+scifor/frame_origin.m` and
  `+scifor/output_column_name.m` are delegators. Deviation: Python's
  `frame_origin` gained `extra_library_dirs` (MATLAB passes `matlabroot`;
  Python alone has no notion of MATLAB toolbox frames). The MATLAB dir is
  already framework via the `scimatlab` package dir. New text rule
  `scifor.outputs.output_column_name_from(default_name, column_name)`, which
  `output_column_name` also uses. `+scidb/Log.m` is a member-free subclass of
  `scifor.Log` (users call `scidb.Log.set_level`, docs/internals/scimatlab.md);
  SciStack's own .m code now calls `scifor.Log`. Tests:
  `scimatlab/tests/test_bridge_frame_origin.py`,
  `scifor/tests/test_outputs_parity.py` (text-rule cases),
  `test_matlab_py_class_dispatch.py` + `test_matlab_log_api_surface.py`
  (inheritance), MATLAB `TestSciforForEachParity` (frame_origin, scidb.Log).
  Not done: `classify_blame` in `failure_from_mexception.m` is still a MATLAB
  twin (not in this stage's scope).
- **Stage 2 done (2026-11-02, untested).** Deviation: the classifier lives in
  `scifor.options` (`classify_for_each_args`, `MATLAB_ONLY_OPTIONS`,
  `matlab_accepted_options`, `matlab_discarded_options`), not in
  `scimatlab.bridge`: importing `scimatlab` imports scidb, and MATLAB scifor
  must stay scidb-free. Values never cross: MATLAB sends only the NAMES
  (`+scifor/option_pairs.m`), gets one role per name (option / discard /
  internal / axis), and keeps one `case` per option for MATLAB-type coercion.
  Both `split_options` now walk PAIRS (before: element by element, so a
  metadata string VALUE equal to an option name was read as that option).
  Deleted: scidb's `reserved_opts`, scifor's `db_only_kwargs()`, the unused
  `_log_fn`. Typo warning moved to Python (both layers now warn). GUI:
  `MATLAB_OPTION_EMISSION` places every option (run option, binding, or a
  reason it is never emitted); `_format_run_option_pairs` iterates
  `FOR_EACH_OPTIONS` (order now follows the declaration: `as_table` before
  `distribute`). Tests: `scimatlab/tests/test_matlab_for_each_parity.py`
  (rewritten), `scistack-gui/tests/test_matlab_option_parity.py`.
- **Stage 3 done (2026-11-02; pytest + MATLAB suite green after three fix rounds, see the stage plan).** All four
  slices in one session, as the user asked (they test once at the end).
  Detail and the responsibility map: `.claude/plan-p5-stage3-matlab-for-each.md`.
  Owner: `scifor.matlab_loop` (`plan`, `log_progress`, `assemble`, `close`,
  `discard`) runs `scifor.foreach.plan_run` on surrogate frames built from
  MATLAB's description. Deviations: (1) the entries live in scifor, not
  `scimatlab.bridge`, and so do not use `@matlab_entry` (importing scimatlab
  imports scidb); the wire is JSON + bytes only. (2) Assembly decisions move,
  the table stacking stays in MATLAB (per-value `to_python` would be per-row
  crossings). (3) `where=`, the Merge join and extraction shapes stay MATLAB
  (value operations). Behaviour changes for MATLAB (Python's rules now):
  outputs `output_1..N` NOT adopted (MATLAB keeps `output` per table); a
  PathInput with no match fails that combo instead of aborting the run;
  `Fixed(PathInput)` no longer drives discovery; an empty `ColumnSelection()`
  selects every data column; labels/banner/summary text are Python's.
  Tests: `scimatlab/tests/test_bridge_plan_roundtrip.py`,
  `test_matlab_loop_source.py`, `scifor/tests/test_failure_from_dict.py`,
  MATLAB `TestSciforForEachPlan.m`.
- **Stage 4 done (2026-11-02).** Steps 1-3 already held: scifor
  (`test_locations.py`), scistackplot (`test_location_filter.py`: reduce AND
  the executed codegen mask) and the GUI (`locationSelection.test.ts`) all
  load `location-filter-cases.json`; the semantics doc now names the JSON as
  the cases' owner (deviation: existing file names kept, not renamed to
  `test_location_cases.py`). Step 4: `scistackplot/tests/test_ordered_groups_cases.py`
  owns `docs/claude/ordered-groups-cases.json` (writes it when missing,
  asserts it current); `PlotStudio/groups.cases.test.ts` runs `orderGroups`
  on it. The committed JSON's expected values were derived by hand; the TS
  side agrees (npm test 699/699 at that point); the Python test confirms them.
- **Stage 5 started (2026-11-02).** `scistack_gui/api/wire_schema.py`
  (`api_json_schema`: every `Handler.params` model via
  `pydantic.json_schema.models_json_schema`, field titles dropped, methods as
  `properties`), `tools/gen_ts_types.py` (writes the JSON, runs
  `npm run gen:types`), `frontend/scripts/gen-types.mjs`
  (json-schema-to-typescript 15, devDependency), tests:
  `test_api_handlers.py::test_generated_request_schema_is_current` (writes
  the JSON when missing) and `src/generated/generatedTypes.test.ts` (skips
  until the JSON exists). NOT done: (a) the generated files themselves (need
  Python); (b) `PlotSpec`/`StatsSpec` — their wire shape is `to_dict()`
  (enums as text, nested `to_dict`s, nulls dropped), NOT the dataclass, so a
  `TypeAdapter` schema would describe the wrong shape; needs a decision
  (pydantic wire models, or a hand walker that mirrors `to_dict`); (c) the
  typed `callBackend` overload and the `statsSpec.ts`/`CustomPlotSpec`
  replacements (need the generated file to compile against).
  **2026-11-02:** (b) decided: pydantic wire models in
  `scistack_gui/api/spec_wire.py`; see `plan-p5-stage5-spec-wire-models.md`.
- **Stage 6 started (2026-11-02): one step type migrated.**
  `scistackplot/reduce/reduction.py` (`ReductionPlan`, `LocationMask`,
  `ValueFilter`, `filter_steps`); `export/reduction_code.py` (one emitter per
  step, `emit_plan`). `apply_filters` applies `ValueFilter.mask`; codegen
  prints the same steps; `ResolvedPlot.reduction` records them. **Export
  behaviour change (a codegen bug fix under "export equals preview"):** the
  export compared filter levels raw (`df[c].isin([...])`) and bounds raw,
  while the preview compares levels as text and bounds on
  `pd.to_numeric(errors="coerce")`, and skips a missing column; the export now
  does the same. Test: `scistackplot/tests/test_reduction_code.py`. Not
  migrated: cell collapse, categorical, level groups, the collapse chain
  (`groupby` sites), compare, bins.
- **Stage 6, step type 2: combines (2026-11-02, untested).**
  `reduction.CombineLevels` (`of` owns the effective mapping, `skip_reason`,
  `apply`, `declared_levels`); `groups.combine_steps`; `apply_level_groups`
  applies the step; `reduction_code.emit_combine` prints it; the figure's
  `reduction` records the combines, then the filters. `groups.effective_mapping`
  is removed (its rule moved into `CombineLevels.of`). Export bugs fixed by
  the move: a missing source column raised `KeyError` (the preview skipped
  it); a combine named after an existing column overwrote it (the preview
  skipped it); and the export combined AFTER the categorical rewrite and the
  value filters, while the preview combines before both, so a filter on a
  combined factor kept every level in the export. The step is now printed
  where the preview applies it, before the 1-D cell collapse. Tests:
  `scistackplot/tests/test_reduction_combine.py`; `test_groups.py`
  `test_generated_code_reproduces_the_bucketing` now requires the step's exact
  lines. Next: cell collapse, categorical, the collapse chain, compare,
  bins.
- **Stage 6 finished (2026-11-02, untested; pyright: no new errors).**
  - **Cell collapse:** `reduction.CollapseCells`, built by
    `cell.collapse_step`; `apply_cell_collapse` applies it;
    `emit_collapse_cells` prints it. Export bugs fixed:
    - a non-array cell had no samples (NaN) in the preview but was its own
      vector in the export;
    - a text array was a NaN gap in the preview and raised in the export;
    - a pre-exploded measure was regrouped in the preview and left one row
      per sample in the export.

    `codegen._slug` moved to `reduction_code.identifier`, its one owner.
  - **Categorical rewrite:** `reduction.CategoricalRewrite`;
    `categorical.categorical_step` makes every choice (level order, record
    filters, the stated pairs). `categorical_code`, `_one_hot`,
    `_category_positions` and `_record_mask` are removed. Bug fixed: the
    export took the category order from the declared levels only, so
    undeclared categories were NaN. The export's location mask now follows
    the rewrite, as in the preview (the same rows either way).
  - **Compare:** `compare.ComparisonPlan` is the step, and its key rule is
    `keys_among` (`key_columns` reads it). `codegen._compare_lines` moved to
    `reduction_code.emit_compare`. Bug fixed: the reference level is spelled
    as `panel_key_text` spells it, not `pd.isna`.
  - **Collapse chain:** `reduction.CollapseLevels` plus `reduce/chain.py`
    (`collapse_passes`, the one planner); `resolve._collapse_levels` applies
    the passes; codegen prints them for the marks and the overlay
    (`emit_collapse_levels`). Bugs fixed:
    - missing levels were dropped in the export (`dropna`);
    - `groupby([])` raised in the export;
    - a text measure's `final` keys and its overlay now pool as in the
      preview.

    Preview change: a pass reduces the y and x measures only (before:
    every name in `spec.measures`, which only ever holds one).
  - **Bins:** not a step. `bins.py`'s rules are source text the preview
    runs and the export embeds verbatim (`EDGES_SOURCE` / `BARS_SOURCE`).
  - **Recorded plan:** `_Plan.reduction` holds the shared steps (combines,
    cell collapse, categorical, filters). Each figure appends its own (the
    chain's pre passes, the comparison, the final passes;
    `_with_plan_steps`).
  - **Tests:** `test_reduction_{collapse_cells,categorical,compare,collapse_levels}.py`;
    `test_show_sample_codegen.py` pins the new groupby line.

User commands (one package per invocation):

    python -m pytest scifor/tests -x -q
    python -m pytest scimatlab/tests -x -q
    python -m pytest scistackplot/tests -x -q
    python -m pytest scistackplotdb/tests -x -q
    python -m pytest scistack-gui/tests -x -q
    python -m pytest scidb/tests -x -q

then `cd scistack-gui/frontend && npm run gen:types && npm test` (the GUI
pytest writes `api-schema.json` on its first run), and the MATLAB suite
(`scimatlab/tests/matlab/run_tests.m`).

### First test run (2026-11-02, morning)

- `test_split_options_asks_python[scifor]`: the substring check also hit the
  `discard_db_only_kwargs` INFO tag, which deliberately matches Python
  `scifor.outputs`. The check now looks for the word `db_only_kwargs`.
- `test_level_methods_guard_against_double_sprintf[+scidb]`: `+scidb/Log.m`
  has no bodies now (it inherits from `scifor.Log`). The test runs on `+scifor` only.
- scistackplot `clear_plan_cache` / `_build_plan` / `_plan_cache` AttributeErrors:
  a P2 rename leftover. The tests imported the `resolve` FUNCTION as `reduce_mod`.
  Now `import scistackplot.reduce.resolve as reduce_mod` (test_resolve_caching,
  test_ylimits, test_cell_capability, test_display_aliases).
