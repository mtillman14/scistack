# Pipeline status: did that step work? (GitHub #28)

*Written 2026-11-20. Plan: `.claude/plan-pipeline-introspection.md`. Tests:
`scidb/tests/test_pipeline_status.py`, `scistack-gui/tests/test_step_status_service.py`,
`frontend/src/components/DAG/stepVerdict.test.ts`,
`frontend/src/components/DataView/dataView.test.ts`,
`extension/src/pipelineTreeCore.test.ts`.*

This is the live doc for per-location state and the step verdict. It
supersedes, as GUI entry points, `schema-location-status.md` (the location
picker's four colours), `expected-locations.md`, `data-inventory.md` and the
Coverage popup; those docs stay for the scidb code and CLI they describe.
`function-node-colour.md` still owns the node's green/red; the chip described
here sits on top of it.

## The questions, and the one surface that answers each

| Question | Surface | Owner |
|---|---|---|
| Did that step / run work? | node reason chip, run report "Did it work?", Activity Bar tree | `scidb.provenance.status.step_status` / `run_verdict` |
| Why not, and where? | a reason → the data view filtered to those states | same |
| What data do I have? | the data view (▦ Data), one column per variable | `scidb.provenance.data_view.data_view` |
| What is the value here / what made it? | data view cell → right pane | `inspect.preview` + `data_view.cell_detail` |
| What did this run add? | run report "What's new?" | `scidb.provenance.whats_new` |
| Does anything look wrong? | ⚠ warning state, run report "What looks odd?" | `scidb.provenance.findings` |
| What is left out, why, since when? | ⊘ cells, tree "Exclusions (N)" | `scidb.store.exclusions` |
| Is any of my data from old code? | variable chip "N outdated variants", Clean up outdated… | `inspect.variant_cards.variant_outdated` / `outdated_cleanup` |
| Will my in-progress function work? | 🧪 Try on… (saves nothing) | `scidb.run.try_step` |
| Where did my subjects go? | data view footer drop-off `40 → 38 → 31` | `data_view._dropoff` |

## Vocabulary (one owner each)

**Planned locations** of a step's output (`status.planned_locations`): the
product of the levels of the step's iteration keys, each key's levels every
value it has in `_schema`, taken separately within each stratum of the
**between keys** (`scidb.store.between_keys`, declared explicitly in the
Schema Keys dialog; strata by `inventory.strata_of`). The keys come from the
step's iteration level (`run.schema_level.resolve_schema_level`: stated >
recorded > inputs). Never from the output's own records. Read live on every
call; nothing is cached (Stage 0 decided: measure first, the `[timing]
step_status(<fn>)` line names each phase).

**Location states** (`status.STATES`, glyphs are shapes):

| | state | rule (first match wins) |
|---|---|---|
| ⊘ | excluded | an exclusion governs it: global, scoped to the output, or inherited from an input |
| ✕ | failed | the newest `_run_outcome` failure here is newer than the record here (or there is no record) |
| ↻ | outdated | a record of this step is here, but its `function_hash` is not the current source (code changed) or an input was re-saved since (`superseded_batch`) |
| ⚠ | warning | a record is here with a finding (`findings_for_records`) |
| ✓ | present | a record of this step is here |
| ↓ | missing input | no record, and an input has no record matching here |
| ＋ | new | no record, inputs present, the step ran before, and the inputs here were saved after its last run |
| ○ | not run | no record, inputs present, otherwise |

"Code changed" needs the function object (`fn_registry`); with only a name it
cannot be seen and only re-saved inputs read ↻. MATLAB proxies hash through
`function_hash_for`, as `check_node_state` does.

**Step verdict** (`status.verdict_of`): over the planned locations, worst
state per location across outputs. ✅ succeeded (every counted location ✓),
⚠ succeeded with warnings (every one present, some ⚠), else ✕ not complete
with reasons in the fixed order ⏸ cancelled > ⏸ stopped > ✕ failed > ↓
missing input > ○ not run > ✎ code changed > ↑ inputs changed. Counted =
planned − excluded − new. **＋ new is never part of the word**: it is the
second chip (`＋4 new · run to include`: it says what to do, because a ✅ on a red node otherwise looks contradictory). A step with no level reads "○ not run" with no count,
never "0 of 0". `needs_running` is true for any ✕ ○ ＋ ↻ or a step that never
ran; parity with `check_node_state` is pinned by tests.

Resolved 5 of the plan said "a ✅ step flips to ○ N not run (new
locations)"; the verdict rules ("new data never makes a finished step look
broken") were taken as the later decision: it stays ✅ with `＋N new`, and the
node turns red (needs running). The lifecycle test pins this.

## Scoped exclusions (`variable=`)

`exclude_schema(reason, variable=V, **keys)` / `include_schema(...)` store the
scope in `__scidb_schema_overrides.variable` (NULL = everything). Asked about
V, unscoped rows and V-scoped rows compete under the one most-specific rule
(`most_specific_match`). `governing_exclusions(combos, keys, db, variables)`
asks each scope, everything first; `for_each` passes every variable the step
reads and writes (`foreach_prepare._step_variables`), so a location scoped
out for an input is skipped and its outputs read ⊘ "inherited from <input>".
Presets: `mark_missing_expected` (scoped exclusion, reason "marked missing as
expected") and `mark_present_expected` (scoped include). A dataset with a
schema key named `variable` must use `exclude_schema_dict(..., variable=)`.
MATLAB (2026-11-21): `scidb.exclude_schema(R, 'subject', 1, 'variable', V)`
and `include_schema(...)`, read by `+scidb/+internal/exclusion_args.m` (the
one owner of the pair rules; V a name or a BaseVariable instance; in a
dataset with a schema key `variable` the pair is that key). Tests:
`scimatlab/tests/matlab/scidb/TestScopedExclusions.m`. `list_exclusions`
returns None, never NaN, for NULL cells so the MATLAB table shows `missing`.
The presets: `scidb.mark_missing_expected(V, 'subject', 3)` and
`written = scidb.mark_present_expected(V, ...)`, arguments read by
`+scidb/+internal/mark_args.m`, calling `mark_missing_expected_dict` /
`mark_present_expected_dict` (the owners; the keyword forms wrap them).

## Findings (`scidb.provenance.findings`)

Computed once, in the run, while the values are in memory
(`foreach_save._check_findings` → `RunRecorder.add_findings` → written with
the run id in `finish`). Stored only when something is wrong, as
`_run_outcome` rows with `outcome='finding'`, `record_id`, `finding` (kind).
Kinds: empty, all NaN, NaN introduced (input cells read once per input
variable, only for outputs with NaN), shape mismatch (vs the most common
shape of sibling locations), outlier (`|x − median| / MAD > OUTLIER_MAD`, at
least `OUTLIER_MIN_N` values). Siblings = this run's outputs + the
variable's current records (one load, skipped above `SIBLING_LOAD_MAX`).
Every `_run_outcome` reader that means "why is this missing" skips finding
rows (`latest_outcomes`, the run report's outcome rows). A record's findings
are those of the newest run that wrote any for it. Direct saves get none and
read "not checked".

## What's new (`scidb.provenance.whats_new`)

New = a location whose first `_record_save` of the variable falls in the
run's window (start, start + duration + 1 s, converted from UTC to the save
clock's local time). Written compactly by `inventory.describe_locations`
(`_collapse_rows` + a range merge), so ten trials read `trial=11–20`. Also:
saved again (changed / unchanged), and composition before → after (location
count, levels per key, per-parent child counts).

## Try (`scidb.run.try_step`)

`for_each(save=False)` inside `run_summary.recording_suppressed()`: no
records, no `_run_summary`, no `_run_outcome`. `describe_try` turns the
result table into per-location value previews and runs `check_saved`
without writing. The GUI starts an ordinary run with `run_options.try =
true` (`run_service._RunOpts.try_run`); `run_done.try_results` carries the
report. Python only; MATLAB nodes say so.

## Deleting

`delete_plan` targets may carry `locations` (a `LocationFilter` mapping) that
restricts that target's seed records; downstream, trash and undo are
unchanged. `outdated_cleanup` lists every outdated card (unknown flags are
never candidates) with its `delete_plan` counts and a warning on the default
or pinned variant; the GUI deletes the ticked ones in one `delete_variant`
call (`cleanup_outdated` handler): one tombstone, one Ctrl Z.

## GUI wiring

- `services/step_status_service.attach_step_verdicts` puts `data.verdict`
  (no locations) on every function node and `data.outdated_variants` on
  variable nodes during `build_graph`; the GUI-only reason "✂ input not
  connected" is prepended in the same chip. The graph also carries
  `exclusions` for the tree.
- One read handler `data_view` with parts `view` / `step` / `cell` /
  `outdated` / `cleanup_plan` / `between`; writes: `cleanup_outdated`,
  `set_between_keys`, and `exclude_schema_combo` / `include_schema_combo`
  with an optional `variable`.
- Hidden, not deleted: `SHOW_LEGACY_DATA_ENTRIES` (Inventory, Coverage, View
  Schema Locations) in `DataView/openDataView.ts`; `SHOW_EXPECTED_LOCATIONS`
  (popup, marks, ✕N/🏷N badges, which are no longer fetched) in
  `Expectation/expectationTypes.ts`.
- The data view is its own component (`DataView/DataViewPopup.tsx`), not a
  third mode of `SchemaLocationPicker`; its row/cell rules are `dataView.ts`.
- UI simplification (2026-11-21, `.claude/plan-ui-simplification.md`):
  - The filter row IS the legend: every state from the reply's `states`,
    tooltip `meanings` (`status.MEANINGS`, the one owner;
    `dataView.legendEntries`). ＋ new stays its own state (the node chip and
    its click-through use it).
  - One word in the cell pane: Exclude here… / Exclude V only… (Include
    again… / Include V again… on ⊘). "Mark missing / present as expected"
    are API presets (Python, MATLAB), not GUI buttons.
  - One preview: the cell pane's Preview… opens `DataPreviewPopup` at that
    location (`DataPreviewTarget.location`), mounted inside the data view so
    it stacks on top; the variable menu's "Preview data…" is gone.
  - Function menu: ▶ Run until here and 🧪 Try on… (saves nothing)
    together, a divider, then the read-only entries.
  - Header: 🔄 Refresh Code, Report, ▦ Data, Schema keys, ⋯ (Restart
    Python, Paths, Clear data, Report bug: `HeaderMoreMenu.tsx`, which owns
    those dialogs' open state).
- Activity Bar: `extension/src/pipelineTree.ts`, fed by every root-canvas
  `get_pipeline` reply the host forwards (`dagPanel.ts`), so no endpoint.

## Diagnostics to grep

| Where | Line |
|---|---|
| scidb | `[status] <fn> call_id=…: ✅ Succeeded · … (needs_running=…)` and `[timing] step_status(<fn>)` |
| scidb | `[scidb] schema exclusions: removed N excluded combo(s) …; by scope {…}` |
| scidb | `[findings] N saved record(s): M finding(s) {…}`, `[findings] <fn>: stored N finding(s) for run …` |
| scidb | `[whats_new] run=… <headline>` |
| scidb | `[outdated] <var>: N of M variant(s) outdated` |
| GUI | `[step_status] N verdict(s), F failed, in … ms (slowest …)` |
| GUI | `[data_view] part=… in … ms`; `[run_thread] Try: save off, nothing recorded` |
| browser | `[DataView] open …`, `[DataView] N column(s) …`, `[Try] run_done …` |
| extension | `[tree] <db>: N step(s), M need running`, `[tree] revealStep …` |
