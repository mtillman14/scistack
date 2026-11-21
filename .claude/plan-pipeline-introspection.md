# Plan: pipeline introspection in a scientist's words (GitHub #28)

*Drafted 2026-11-20. Decisions are the user's unless marked (proposed).*

## The problem

The answers mostly exist in scidb, but the GUI spreads them over five
overlapping verdicts and too many buttons named after mechanisms:
node colour (binary; red mixes "never run", "failed", "code edited",
"input re-saved"), `location_states` (View Schema Locations), `expectation`
(Expected locations), 📋 Inventory, ✅ Coverage, plus the run report and the
FailuresPanel. Nothing says "did it work?" in one word.

## The questions a scientist asks (and the one surface that answers each)

| Question | Answer lives in |
|---|---|
| Did that step / run work? | Step verdict (node chip, run report header, Activity Bar tree) |
| Why not, and where? | Verdict reasons → data view filtered to the bad locations |
| What data do I have? | Data view (picker in status mode, one column per variable) |
| What is the value here? | Data view cell → value pane (`scidb.inspect.preview`) |
| What made this number? (code version, settings, which input records) | Data view cell → "made by" section (`trace`) |
| What did this run add or change? | Run report "What's new" (new locations, re-saved locations) |
| Does anything look wrong? | Anomaly findings (empty/all-NaN, NaN introduced, shape mismatch, outlier) |
| What is left out, why, since when? Was it in variant V's computation? | Exclusions list (tree + data view); cell's input list shows exactly which records were used |
| Is any of my data from old code? | `outdated` flag on variants; node chip; bulk clean-up |
| Will my in-progress function work? | Test mode ("Try on…"), nothing saved |
| Where did my subjects go? | Data view column footer counts (40 → 38 → 31), each loss has a reason |

## Vocabulary (one owner each, in scidb unless noted)

**Location**: a schema-key combination.

**Planned locations of a variable** (the denominator, no guessing): the
Cartesian product of the levels of its schema keys, where a key's levels are
every value it has in the database, **taken separately within each stratum of
the between keys**, minus exclusions that govern it (global, or scoped to this
variable, or inherited from an input of the step that makes it).
A subject loaded for one variable but never for another therefore shows as
missing downstream too, which closes the "downstream inherits the loader's
blind spot" hole in `schema-location-status.md`.

**Location state** of (variable, variant, location), the one per-location answer:

| Glyph | State | Meaning |
|---|---|---|
| ✓ | present | a record exists, current, no findings |
| ⚠ | warning | present, with an anomaly finding |
| ↻ | outdated | present, but the code changed or an input was re-saved since |
| ✕ | failed | the last run raised here (error text kept) |
| ↓ | missing input | not computed: input `X` has no data here |
| ＋ | new | before a run: inputs present here, no output yet, and the step has run before (data added since). It is accepted, and the next run processes it. After that run, the run report announces these locations once as new (see "What's new"). It is attention, not a problem. |
| ○ | not run | inputs present, the step has never run here (and the step has never run at all, or ran before without reaching it) |
| ⊘ | excluded | an exclusion governs it (its reason; "inherited from EMG" when it comes from an input) |

Glyphs are shapes, so the states can be told apart without colour.

**Step verdict**, derived from its output variables' location states over the
planned locations (scoped to the run's `locations=` for a run verdict):

- ✅ **Succeeded**: every planned location is ✓.
- ⚠ **Succeeded with warnings**: every planned location present, some ⚠.
- otherwise **Not complete**, with reasons and counts in a fixed priority
  (✕ failed > ↓ missing input > ○ not run > ↻ outdated), plus, for a run only,
  ⏸ cancelled / stopped.
- **＋ new is not part of the verdict word.** It is shown beside it as an
  attention note: "✅ Succeeded · ＋4 new to process". New data never makes a
  finished step look broken. The node still needs running (red = "run me"),
  and the next run includes the new locations by default. The run report's
  "What's new" then lists them as processed.

The verdict is the same word on every surface, because every input to it is
a stored fact (records, `_run_outcome` failures and findings, exclusions,
`_schema`). No surface computes a private version.

scidb owns the wording and the priority (extends `RunSummary.verdict` /
`status_line`). GUI-only reasons (an input edge not connected) are added in
the GUI layer, in the same chip.

## Exclusions absorb "expected" (user)

- `exclude_schema` / `include_schema` gain an optional `variable=`.
  Unscoped = a bad trial, out of everything. Scoped = this variable is
  bad or absent here. Any step reading that variable skips the location,
  and its outputs read ⊘ "inherited from <variable>".
- **Mark missing as expected** = scoped exclusion, reason
  `"marked missing as expected"`. **Mark present as expected** = scoped include,
  reason `"marked present as expected"`. They are presets; the user can still
  exclude or include either way afterwards.
- So marking a location missing **does** remove it from downstream
  computations from now on. That answers the question in #28.
- The Expected-locations subsystem is **hidden from the GUI**: popup, marks,
  shared/per_parent settings, ✕N/🏷N badges, Coverage cells. The scidb code and
  CLI stay (Chesterton's fence; hide, don't delete).

## Between keys (strata)

- A per-key project setting **between** (Schema Keys dialog, stored with the
  key types). One owner in scidb. Declared explicitly, always: no
  suggestion, nothing worked out from the data. An undeclared key is crossed.
- Consumers: planned locations (verdicts), the data view, the Inventory's
  Within default; later, Stat Studio's default classification
  (scistackstatsdb passes it in).
- The owner of strata is `scidb.provenance.inventory._strata`, promoted to
  a public function; nothing new is written for them.

## Anomaly findings (user: these three)

- Empty / all-NaN, NaN introduced (output NaN where the inputs had none),
  shape mismatch vs sibling locations / scalar robust outlier
  (|x − median| / MAD > `OUTLIER_MAD` across sibling locations, only when
  n ≥ `OUTLIER_MIN_N`). The thresholds are constants in one module.
- Uses existing `scidb.inspect.cells` / `describe_value` / scicanonicalhash cell kinds.
- **Computed once, in the run, stored only when something is wrong** (Resolved 1).
  `for_each` already has every output and input in memory at save time. It
  checks them there, and writes one `_run_outcome` row per finding (kind
  `finding`, the finding's text, `run_id`), next to the existing failure
  rows. Healthy records write nothing, so "no shape or size is stored" holds.
  - Outliers and shape mismatch need sibling locations. They are checked at
    the end of the run, over that run's outputs plus the scalar values of the
    variable's current records at the other locations (one batched load, timed).
  - Records saved outside `for_each` (direct saves, raw loads) get no
    findings. Their locations say "not checked", never ✓-with-no-findings.
  - A later run that rewrites a location supersedes its findings, the same
    way it supersedes a failure row (`latest_outcomes`).
  - Every surface (chip, report, data view, tree, Try) reads these rows.
    Try computes the same checks with the same owner, but does not write them.

## Surfaces

### Function node: colour + one reason chip (user)
Green/red keeps meaning "does this need running?". The chip names the top
reason: `✕ 3 failed`, `↓ 2 missing input`, `○ not run`, `✎ code changed`,
`↑ inputs changed`, `⚠ 2 warnings`, `⊘ 4 excluded` (informational).
A second, neutral-coloured chip `＋4 new` appears whenever new input data is
waiting. It is never folded into the reason chip.
Variable node: `2 outdated variants` when it applies.

### Data view = the schema location picker in status mode (user)
- Same `SchemaLocationPicker` (tree + by-key list) and the same
  `locationSelection.ts` rules. A third mode next to Plot Studio and
  run selection: no checkboxes, a "Data" header with a distinct tint, and
  glyphs in cells.
- Columns: "any data" (the old Inventory question), then one narrow column
  per variable in pipeline order (current variant; a variant dropdown per
  column). Footer: per-variable counts, the drop-off from step to step (the old Coverage).
- Click a cell → right pane: value preview, "made by f v3 with low_hz=20 from
  RawEMG@S03/T2 …", run time, findings, failure text, exclusion reason.
  Actions: Exclude here (everything) / Exclude for this variable / Mark
  missing or present as expected / Delete this variant here….
- Replaces Inventory, Coverage, View Schema Locations and Expected locations
  in the GUI (hidden, not deleted). One entry: the header **Data** button,
  and a variable's right-click opens it filtered to that column.

### Run report, reordered around the verdict
1. **Did it work?** The verdict word, the status line, and the reasons, each
   linking to the data view filtered to those locations.
2. **What's new?** Shown prominently, near the top (user, 2026-11-20).
   - **New** = a (output variable, location) whose *first ever* record was saved
     by this run. It is a fact from `_record_save` (the first save at that
     location belongs to this run), never a stored flag. So the emphasis
     appears exactly once. Example: subject 1 had trials 1-10, trials 11-20
     were added, and the run processes only 11-20 (1-10 are already up to date
     and skipped). The report says **"RMS: 10 new locations — subject=1,
     trial=11–20"**. Re-running over 1-20 later shows no new locations.
   - Written compactly with the inventory's collapsed-block rule (one owner,
     `inventory._collapse_rows`), so a block reads as `subject=1, trial=11–20`
     rather than ten lines. Listed in full on expand.
   - **Dataset composition, before → after**, per output variable: location
     count, and per key the number of levels and the per-parent counts that
     changed ("subject 1: 10 → 20 trials"; "subjects: 12 → 13"). A scientist
     always sees the size and shape of their dataset change.
   - Also listed, but less prominently: locations saved again (the inputs
     changed), with whether the value changed.
3. **What looks odd?** Anomaly findings.
4. **What was it run with?** Code version, settings, inputs and variants, exclusions in force.

### Test mode: "▶ Try on…" (user: don't save)
Function node right-click → pick locations (picker in selection mode) → run
that one function there with saving off. The report gets a "Try" tab: outputs
(value preview), findings, printed output, errors, timing. Nothing is written,
not even `_run_summary`. It is for one step only; downstream steps can't
consume it.

### Deleting a variant (user: all four)
- scidb flags **outdated** on a variant card: its `__code__.<fn>` version is not
  the function's current source. MATLAB follows the existing hash-trust rule
  (`trusts_hash`); where it can't be trusted the flag reads "unknown", never a
  guess.
- Card badge "made by filter_emg v1, current code is v3"; variable-node chip.
- From the data view: column header → Delete variant V2…; cell → Delete V2 here….
- **Per-location delete**: `delete_plan` accepts a location selection (the
  same `include`/`exclude_levels` pair) to restrict the seed records; the
  downstream closure, trash and undo are unchanged.
- **Clean up outdated…** per variable (Variants popup) and pipeline-wide
  (Activity tree): a ticked list with record and downstream counts → one
  trash batch → one Ctrl Z. Warns when a ticked variant feeds a saved plot or stat,
  and never deletes the pinned/current variant without saying so.
- All of these go through the one scidb `delete_plan` / `delete_variant`.

### Activity Bar: "SciStack" view container, native TreeView (user)
- One root per open session; under it, the steps in pipeline order with the
  verdict glyph and the reason chip text. Expand → reasons (with locations),
  output variables (counts, outdated variants), and "Exclusions (N)" with reason and time.
- Click → reveal the canvas, select the node, open the data view filtered.
- No session → welcome view: "No pipeline open" + Open Pipeline. The server
  keeps dying with the canvas (DuckDB lock; `matlab-run-database-ownership.md`).
- Data: the step verdicts already built for the canvas, relayed by the
  extension host on `dag_updated` / `run_done`. No new endpoint if the
  graph payload carries them (grug: keep the endpoint count down).

## Stages (one commit each; logging + tests per NOTE 2)

0. **Measure.** Time `location_states` / preview / verdict inputs on a real
   project (`location_states` measured 9.5 s for 419 locations in 2026-09).
   Add per-phase timing logs. Decide caching before building the
   multi-column view.
1. **Scoped exclusions** (`variable=`), the mark presets, `for_each` skipping
   per input variable (hot spot `foreach.py`: touch only `filter_excluded_combos`).
2. **Between keys**: setting, suggestion, public `_strata`.
3. **`location_status` + step verdict** (new owner in `scidb.provenance`). It is
   built from the existing batch primitives. Parity tests: the "needs running"
   bit equals `check_node_state`, and each state matches `check_combo_state`
   on a fixture with one of each.
4. **Findings (computed on demand), outdated flag, "what's new"** facts.
5. **`delete_plan` location targets + bulk outdated.**
6. GUI: node reason chip; run report reorder.
7. GUI: picker status mode + data view; hide Inventory / Coverage / Schema
   Locations / Expected entry points.
8. GUI: delete from the data view; Clean up outdated….
9. GUI + scidb: Try on… (verify `save=False` returns outputs on the Python and MATLAB paths).
10. Extension: Activity Bar tree + welcome view.
11. Docs: one live `docs/claude/pipeline-status.md`; supersede notes on
    `schema-location-status.md`, `expected-locations.md`, `data-inventory.md`,
    `function-node-colour.md`; README index regenerated.

The first milestone (stages 0, 1, 3, 6) already answers "did it work?" everywhere.

## Resolved (user, 2026-11-20)

1. **Data integrity reads the same everywhere** (user, revised). Findings are
   computed once in the run, where the data is already in memory, and stored as
   `_run_outcome` rows only when something is wrong. This costs no extra loads
   at refresh time and adds no new table, and every surface shows the same word.
   (A first answer, "on demand only", was dropped because the chip and the
   report would disagree.)
2. **No conversion of existing Expected marks** (beta: no shims). Old marks
   simply stop affecting anything the GUI shows.
3. **Between keys are declared explicitly, always.** There is no suggestion
   from the data, and nothing is worked out. An undeclared key is crossed.
4. **Inherited exclusion reads ⊘** on the output, with "inherited from <input>".
5. **Aggregating step**: a subject is planned if any of its trials is not
   excluded. **Origin problem** (user): planned locations must never be
   derived from the output's own records.
   - The output's keys come from the step's **iteration level**, the same
     owner `for_each` uses to choose its keys (intent before the first run,
     fact after it). A step whose level can't be resolved reads "○ not run"
     with no counts, never "0 of 0 succeeded".
   - Key levels are read **live** from `_schema` on every verdict. Adding data
     adds planned locations, so a ✅ step flips to "○ 4 not run (new
     locations)". That is correct, and the chip says "new" so it doesn't read as a regression.
   - Any cache (stage 0) is keyed on a `_schema` fingerprint, so it can never
     keep a stale planned set.
   - Test: a never-run step, then a run, then add a subject. The verdict goes
     not run → ✅ → not complete with exactly the new subject's locations.
