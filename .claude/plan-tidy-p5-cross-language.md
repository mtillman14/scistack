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
- (none yet)
