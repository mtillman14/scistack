# Plan: Compare to reference (difference / % change) in Plot Studio

Status: APPROVED 2026-09-27 (D4 confirmed: a per-summary reference keeps its spread). All 6 stages built 2026-09-27; frontend tests pass, both bundles rebuilt; Python tests UNRUN (user runs them). Implementation note: D6 refusals became INERT (raw values drawn, reason reported), not validate errors, so a control change can never make the spec invalid, the same policy as show_sample.

## What the user asked for

A control that re-expresses a plotted measure relative to one reference level of
a grouping layer, e.g. `[subject, session]`, sessions 2-4 relative to session 1:

- **Difference:** `x - ref`
- **% change:** `100 * (x - ref) / ref` (ref = 100 %, then minus 100)

The reference level lands at 0. Every existing control (filters, grouping,
facets, collapse, sample, show-sample) still applies.

User decisions (2026-09-27):
- **Paired when the sample can be paired, per summary otherwise.** "Can be
  paired" is the same rule "Show sample" uses to draw lines
  (`roles.line_recurrence`).
- Reference level is user-editable.
- Reference level shows as 0 / an empty tick.
- The y-axis title changes to say what was computed.
- Statistics: deferred. Manual difference bars (clicking two ticks) keep
  working; nothing statistical feeds them while a comparison is active.
- 1-D: pointwise; mismatched lengths are refused (WARN), never resampled.
- Keeping the untouched data for a cheap toggle is an implementation detail
  for me to decide (D5).

## The spec field

```python
class CompareMode(str, Enum):
    DIFFERENCE = "difference"
    PERCENT = "percent"

@dataclass(frozen=True)
class Comparison:
    layer: str                 # a grouping layer (an entry of PlotSpec.groups)
    level: str                 # the reference level, as text (panels.panel_key_text: "01" stays "01")
    mode: CompareMode = CompareMode.DIFFERENCE
    active: bool = True        # off keeps the settings, like LevelGroup.active

PlotSpec.comparison: Comparison | None = None
```

The enum leaves room for a ratio or log ratio later. Neither is built now.

## Decisions

- **D1 One owner: `scistackplot/compare.py`.** It decides the pairing, builds
  the baseline table, applies the formula, and names the y title. Every chain
  that exists today calls it, and none computes a baseline of its own:

  | Consumer | Where it hooks in |
  |---|---|
  | preview, scalar | `reduce._build_figure`, right after `_sample_frame` and before `steps.final` |
  | "Show sample" overlay | `reduce._build_figure`, the overlay frame, using the same baseline table |
  | numpy 1-D paths | `reducer.NumpyReducer.collapse_series` / `summarize_series` / `y_extents`, after `_chain(... steps.pre ...)` |
  | pandas 1-D fallback | `PandasReducer.collapse_series` / `summarize_series` (after `_collapse_levels`) |
  | y limits | `ylimits._reduced_extents`, after the pre-collapse; the overlay too |
  | Save data (CSV) | `export.plot_data`, sample depth; deeper depths use the overlay route |
  | export code | `codegen`, lines emitted after the collapse chain |
  | y-axis title | `compare.y_title`, read by `reduce._labels_for` AND `codegen` (two y-label spellings today; both go through this one) |

- **D2 Pairing = `roles.line_recurrence(table, steps.sample, layer, other ticks)`.**
  - True: **paired**. The baseline key is every factor column of the sample
    rows except `layer` (plus the 1-D index column). Each unit is compared
    with its own reference row.
  - False / None: **per summary**. The baseline key is that set minus the
    sample keys. The baseline is the reference mark's centre
    (`Aggregation.statistic`, mean or median), and each sample row is compared
    with it.
  - Nothing collapsed: trivially paired, since each mark is one row.
  - Pooled (several sample keys): the rule reads the deepest key, as it does
    for Show sample.
  - The reason text is logged at INFO and shown in the GUI note.
- **D3 Where the transform runs: on the SAMPLE rows.** That is after the
  pre-collapse (trials averaged within subject) and before the marks'
  statistic. A deeper "Show sample" row (a trial) joins the baseline on the
  keys it has, so it gets its subject's reference. The mean of transformed
  trials then equals the subject's transformed value for both formulas, since
  the reference is fixed per subject.
- **D4 The reference tick keeps its marks at 0.** Paired: every reference row
  is exactly 0, so bars have zero height, boxes are a flat line and
  spaghetti/sample lines start at 0. The tick stays on the axis, and a
  difference bar can still end on it.
  - Per summary: the reference rows keep their own spread around a centre of
    0. Blanking them would hide the baseline's variability. **Confirm this.**
- **D5 Cheap toggle (answers the user's point 2).** The loaded table is never
  mutated, and `compare` is a pure function of the sample frame. `comparison`
  joins `reduce._PLAN_IRRELEVANT_FIELDS`, so toggling it or changing its level
  reuses the cached plan (loaded, filtered, fanned-out frames). Only
  `_build_figure` and the y limits re-run. The y limits cached on the plan
  must be keyed by the comparison too, or toggling would keep the old axis;
  that gets a test. A new `timing.phase("compare")` measures the cost, and a
  second copy of the data is held only if that turns out slow.
- **D6 Refusals and drops (each one a WARN naming the units):**
  - A unit with no reference row is dropped entirely (all its levels).
  - % change with a reference ≤ 0 drops that unit (paired) or that bracket
    (per summary): % change is undefined there.
  - 1-D with a length that differs from its reference is dropped. Codegen
    emits the same whole-unit drop, so the export equals the preview.
  - A reference level absent from the figure makes the comparison inert in
    that figure, logged. This matches the difference bars' inert rule.
  - `validate` refuses: a layer that is not a grouping layer; a 2-D measure;
    an x-y plot (`x_measure`); `log_y` together with an active comparison
    (differences go negative).
- **D7 Labels.** Difference: `"Δ {y}"`. Percent: `"{y}, % change from {layer} {level}"`.
  Both use aliased names. `style.y_label` still wins. `export.data_export_options`
  describes the transform in its chain text, and the CSV holds the plotted
  (transformed) values, so the CSV is what the figure shows.
- **D8 Statistics.** `capability` reports `comparison.active`, and the future
  statistics → difference-bars feed must check it. There is no feed today, so
  the only code change is recording the rule in `statistics-design.md` as
  deferred. Manual bars are unchanged.
- **D9 Default layer** when the user turns it on: the innermost tick
  (`GroupingLayers.span`), or else the outermost series layer (line/band).
  Default level: that layer's first level in axis order.

## Logging (NOTE 2)

- INFO `compare: session=1 percent, paired by subject (Each subject has a value at every session); units 12, dropped 1 (no reference: [07])`
- WARN for each drop class, listing at most 10 units and then "+N more".
- DEBUG: the baseline key columns and row counts before and after.

## Stages

1. **Spec**: `Comparison`, `CompareMode`, to_dict/from_dict, `validate`,
   `restore_spec` (unknown mode → default + note), `reconcile` (the layer is no
   longer a factor → report it, leave it in place). Tests: round trip plus each
   refusal.
2. **`compare.py`**: `plan_comparison(spec, roles, table, steps) -> ComparisonPlan | None`
   (layer, level, mode, paired, reason, key columns), `baseline(sample, plan, measure, index_column)`,
   `apply_frame(frame, baseline, plan, measure)`, `apply_cells(kept, arrays, plan)`,
   and `y_title(...)`.
   Tests: hand-computed numbers in `test_compare.py`:
   - paired vs per summary, on an unbalanced design where they differ;
   - % change: mean of ratios ≠ ratio of means, pinned;
   - drops (missing reference, reference ≤ 0, ragged 1-D);
   - overlay trials averaging back to their subject's value;
   - `apply_cells` vs `apply_frame` parity.
3. **Wire the consumers** (D1 table), plus the plan-cache/y-limit keying from D5.
   Tests:
   - the drawn marks via `tests/plot_geometry.py` on both backends, with the
     reference tick at 0;
   - y limits cover the transformed values and change on toggle;
   - the CSV equals the drawn sample;
   - a difference bar ending on the reference tick is still placed.
4. **Codegen**: emit the join-and-subtract lines. Export-equals-preview tests
   for scalar and 1-D, paired and per summary.
5. **Capability + GUI**: `capabilities.comparison` (available or reason;
   layers; levels per layer; paired + reason; drops). A **Structure > Compare**
   section after Grouping:
   - Mode: Off / Difference / % change
   - Layer
   - Reference level
   - the always-visible note (paired/per summary + drops)

   Rebuild both vite bundles, run the frontend tests, and add a §0 entry to
   `docs/gui-manual-testing-todo.md`. Update `docs/claude/plot-studio-controls.md`.
6. **Docs**: `docs/claude/compare-to-reference.md` (rule, owners, worked
   example). Record the deferral in `statistics-design.md` (D8).

## Test commands (user runs)

```
pytest scistackplot/tests -q
pytest scistackplotdb/tests -q
```
(one package at a time)
