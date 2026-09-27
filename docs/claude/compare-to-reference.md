# Compare to reference

Plot Studio can draw a measure relative to one level of a grouping layer, for
example sessions 2-4 relative to session 1, as a **difference** (`x - ref`)
or a **% change** (`100 * (x - ref) / ref`). The reference level sits at 0.
Written 2026-09-27. Plan with decisions D1-D9: `.claude/plan-compare-to-reference.md`.

## The field

```python
PlotSpec.comparison: Comparison | None

Comparison(
    layer="session",          # a grouping layer (an entry of PlotSpec.groups)
    level="s1",               # the reference level, as text (panel_key_text: "01" stays "01")
    mode=CompareMode.PERCENT, # or DIFFERENCE
    active=True,              # False keeps the settings and draws raw values
)
```

## The one rule: paired or per summary (D2)

The transform runs on the **sample rows**: after the pre-collapse (trials
averaged within each subject), before the marks' statistic, and before
`steps.final`. Whether the sample can be paired is
`roles.line_recurrence(table, steps.sample, layer, other layers)`. That is
the same rule "Show sample" uses to decide whether to join points into lines.

| Answer | Mode | Baseline keyed by | Reference tick |
|---|---|---|---|
| True: a subject has a value at every session | **paired** | every factor of the sample rows except the layer (+ the 1-D position) | exactly 0, no spread |
| False / None: a trial belongs to one session; pooled with trials | **per summary** | the same, minus the sample keys; the value is the reference mark's centre (`Aggregation.statistic`) | its own spread, centred on 0 (D4) |
| Nothing collapsed | paired, trivially | every other factor | 0 |

Paired and per summary differ in two ways:

- A missing subject: in the fixture of `test_compare.py`, s3 is 12 paired
  vs 12.67 per summary.
- % change: paired draws the mean of the per-subject changes (33.3 %), not
  the change of the means (31.25 %).

A "Show sample" row deeper than the sample (a trial) joins the SAME baseline
on the keys it has, so it is measured against its own subject's reference,
and the trials average back to the subject's value.

## Drops (D6)

Each drop is a WARN naming the units, `Outcome.dropped` in the result, and a
line in the GUI note:

| Reason (`compare.*`) | When |
|---|---|
| `NO_REFERENCE` | the unit has no reference row (e.g. filtered away): all its levels are dropped |
| `NOT_POSITIVE` | % change from a reference <= 0 (undefined). Reported once, not also as `NO_REFERENCE` |
| `RAGGED` | 1-D: the series has a different number of positions from its reference. Never resampled |

## Inert, never refused

`compare.unavailable` is the one statement of why a comparison cannot apply.
When it cannot, the figure draws the **raw values**, `validate` logs an INFO
line, and the capability report's `comparison.state.inert` carries the
reason. The cases:

- no grouping layers;
- the layer is not a grouping layer (now);
- the level is not in the data;
- an x-y plot;
- a 2-D measure;
- `log_y`.

This matches the `show_sample` policy: a control change never makes the spec
invalid.

## Owners

| Concept | Owner | Consumers |
|---|---|---|
| can it apply / why not | `compare.unavailable`, `figure_unavailable` | `roles.validate` (log), `comparison_summary` |
| paired or not, keys | `compare.plan_comparison` → `ComparisonPlan` | everything below |
| baseline + formula + drops | `compare.baseline`, `apply`, `compare_sample` | `reduce._build_figure` (scalar sample, 1-D explode route, overlay), `ylimits._reduced_extents` (quiet), `export.plot_data` |
| the same, as emitted pandas | `codegen._compare_lines` | exported code; `test_compare_export.py` holds it to the preview |
| y title | `compare.y_title` / `comparison_title` | `reduce._labels_for`, `codegen` |
| GUI facts | `compare.comparison_summary` (capabilities), `comparison_meta` (`layout.meta.comparison`) | `CompareSection.tsx` via `compare.ts` |

The numpy reducer does not restate the comparison:
- `NumpyReducer.y_extents` hands a compared figure to the pandas reference;
- a compared 1-D band or bar takes the exploded route rather than the
  nested-cell summary (`summarize_nested` is off). The transport stride then
  runs after the compare, so a stride can never drop a reference position.

## Cheap toggle (D5)

The loaded table is never mutated. `comparison` is in
`reduce._PLAN_IRRELEVANT_FIELDS`, so a toggle or a new level reuses the cached
plan (the filtered, fanned-out frames), and only the figures are rebuilt. The
y limits DO change, so `ylimits.ExtentMode` carries the comparison and the
per-mode memo recomputes them. `test_toggling_reuses_the_plan_but_recomputes_the_limits`
pins both. The `compare` timing phase in `build_figure` measures the cost.

## Statistics (deferred)

Manual difference bars work on a compared figure. A mark stays on the
reference tick, so a bar can end there. Automatic stats → difference bars
must not run while a comparison is active; see statistics-design.md S9.

## Tests

- `scistackplot/tests/test_compare.py`: numbers (paired / per summary /
  pooled / %), drops, overlay, inert cases, titles, plan-cache and limits,
  Save data, capability report, 1-D pointwise and ragged.
- `scistackplot/tests/test_compare_export.py`: exported bar heights,
  sample points and y title equal the preview's.
- Frontend: `compare.test.ts`, `sidebarGroups.test.ts`.
