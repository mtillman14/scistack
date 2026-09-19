# Grouping + Collapse roles (replacing X / COLOR / AGGREGATE / FREE)

**Date:** 2026-09-17 (revised same day: grouping model, innermost-first)
**Status:** approved 2026-09-17 (D1–D6 decided). **ALL 7 STAGES BUILT 2026-09-17/19**, tests unrun (user runs them). Stage 2: `roles.CollapseSteps`/`collapse_steps`/`draws_sample_mean` (pre → sample → final-mean-for-mean-drawing-kinds); `reduce._collapse_levels` replaces `_collapse_aggregates`; `_panel_frame` reads ticks/series through `grouping_layers` (series id = grouping series layers only, BAND gets a SERIES column); `_summarize(series=True)`; `ExtentMode.final_mean`; validate re-run on a plan-cache hit (kind rules); `_warn_if_pooling_variants` deleted; PandasReducer on the chain; 2-D skips the chain (`matrix_mean` pools). Tests translated: resolve, core, x_nesting, cell (was collapse), cell_capability (was collapse_capability), ylimits, filters, groups, location_filter, narration, instrumentation, composed_keys, variant_pin, multi_variable_pooling, resolve_caching, render, spaghetti, legend_parity, fontsize, figsize; new test_nested_collapse. Stage 7 (2026-09-19): `docs/claude/grouping-and-collapse.md` written (the reference), both READMEs rewritten, historical plotting docs bannered, `measure-shape-and-collapse.md` renames applied. Remaining: visual check of the GUI; commit. Stage 6 (2026-09-19): PlotStudio.tsx — `Role` union `group|facet|iterate|collapse`, `Spec.groups/color/cell_statistic/aggregate.pooled`, `Capabilities.cell_collapse/collapse/has_sample`, `kinds[].assignment` applied by `setKind`; `GroupingList` (innermost-first rows, colour radio, ↑ inward / ↓ outward, cap on labelled layers, hint from backend) replaces `XGrouping`; Factors section lists only ungrouped factors; Summary gains "Weight by N" + `sampleNote`; `xLayers.ts` → `groups.ts` (`placeGroupLayer`/`orderGroups`, deeper first, depthless innermost); `rolesAfterPick` moves collapse→iterate; test_plot_service translated (`_with_roles`, `_invalid_spec` = colour naming a non-group); both vite bundles rebuilt 2026-09-19; NOT yet visually checked. Stage 5 (2026-09-19): codegen reads ticks/colour/series through `grouping_layers(..., shape=effective)`, emits the chain as one groupby per key (`# collapse <key> within <kept>`; the sample left to seaborn's estimator/errorbar; pooled emits nothing), `_series` composed outermost-first like `_series_key`, `_dash` + `style=`/`dashes=_dashes` from `DASH_CYCLE` via `_SEABORN_DASHES`, docstring names the chain and the sample; test_codegen translated + 6 new tests. Stage 4 (2026-09-19): `DASH` column (`reduce._series_key` over the uncoloured series layers), figure-wide `ResolvedPlot.dash_styles` from `DASH_CYCLE` (`resolved.py`, with `MPL_DASHES`), `Encoding.dash`, `Labels.dash`; both renderers draw one band/line per series in its dash, legend lists dash ids (`base.dash_levels`, grey entries), `shows_legend` counts dashes; numpy summarize emits DASH too; tests in test_dash_styles.py. Stage 3 (2026-09-19): NumpyReducer runs the chain over ndarray cells (`reducer._chain`, `_stats_by_mark` per (colour, series)); scistackplotdb tests translated (parity cases: nested, pooled, uncoloured series via `Uncoloured` roles dict); `limits_by_scope` keeps the raw path for 2-D.
**Reference doc (to write at the last stage):** `docs/claude/grouping-and-collapse.md`

## Why

The data-reducing roles today are `AGGREGATE` ("Average over") and `FREE`
("Free"). They confuse for three reasons the user hit directly (2026-09-15/17):

1. `FREE` has no meaning of its own — "leave the rows and let the kind
   decide" — so a bar's error bars come from a *FREE* key, not from the key
   the user asked to average.
2. Several `AGGREGATE` keys average in ONE pooled groupby, so a subject with
   more trials weighs more. The published number depends on a detail nobody
   chose.
3. `FREE` is silent, so `iterate_ancestors` silently promotes FREE ancestors of
   an iterated key to ITERATE (`speed=Separate figures, subject=Free` → one
   figure per (subject, speed)). The promotion only exists to defend against
   an unassigned key pooling; with explicit roles it has no job.

And a fourth, found while redesigning: `COLOR` and `FREE` draw the *same*
lines on a 1-D plot, and `COLOR` and an inner x layer draw the *same* dodged
bars. Colour never split data; it labelled a split that already existed.

## The model (this section becomes the docs/claude reference)

Every factor is in exactly one of two panes.

### Grouping pane — "one mark per level combination"

An **ordered list, innermost first**. The first entry is the mark's own
identity; each later entry wraps around it. One entry may be tagged
**colour**: that layer is labelled by a legend instead of by tick / series
label. Which factors go here is one question with two halves (membership and
order), asked in one place.

| kind | grouping layers become | colour tag |
|---|---|---|
| bar / box / violin / strip | nested x ticks — first entry innermost | that layer by legend; the others by tick labels |
| scatter with `x_measure` | one point set per leaf group | point colour |
| line / band (1-D, x = sample index) | one line / band per leaf group | line colour; uncoloured layers get a dash style each (D4) |
| spaghetti | **first entry = the lines**; the rest = x ticks. A line joins a subject's points across the layer just above it | colour on the first entry = per-subject colours |
| heatmap (2-D) | grouping must be empty — use panels / figures | — |

`[subject, session, Intervention]` reads the same for bar and spaghetti: bars
for subjects inside session ticks inside Intervention brackets; or points per
subject joined across sessions inside Intervention brackets. Same list, two
readings — the kind-following pane hint ("one bar per…" / "one line per…")
is the explanation.

Rules:
* Labelled tick layers (grouping layers minus the coloured one) are capped at
  `MAX_X_LAYERS = 3`; a 4th is refused with "colour it, or move it to
  Separate panels".
* Availability: bar, strip, scatter and line need nothing; box / violin / band
  need ≥1 Collapse (a sample, below); spaghetti needs ≥2 grouping layers.
* An empty grouping list on a bar = one bar (x = "").
* `x_measure` set: grouping layers are series, never ticks (x is numeric).

### Factors pane — everything not grouped

| Role | Label | Meaning |
|---|---|---|
| `FACET` | Separate panels | one subplot per level (arranged under Layout) |
| `ITERATE` | Separate figures | one whole figure per level — **the default** |
| `COLLAPSE` | Collapse (average) | average the levels away |

### Collapse

* **Nested, innermost first.** With schema `[subject, session, trial]`,
  `subject=Collapse, trial=Collapse`, grouping `[session]`: first mean over
  trial *within each subject* (grouping on every other factor still present),
  then mean over subject. Unweighted: each subject counts once no matter how
  many trials it has.
* **The outermost collapsed key is the sample.** Its per-level values — after
  every inner collapse — are what the kind's statistic is computed over:
  bar = mean ± error over them, box/violin = their distribution, band = mean
  line ± shading over them, scatter/strip/line = just their mean.
  `Aggregation.statistic` / `.error` (mean/median; SD/SEM/CI95/IQR) keep
  their meaning: centre and spread **over the sample**.
* **No collapsed key → no sample.** A bar of single values has no error bar;
  box / violin / band are unavailable.
* **Pooled mode** (`Aggregation.pooled: bool = False`; GUI checkbox "Weight
  by N (pool all levels before averaging)"): every Collapse key is dropped in
  ONE groupby; the sample is every pooled row. This is today's
  multi-AGGREGATE behaviour, now opt-in and named.
* **Order comes from `LongTable.factor_depths`**, deepest first. A joined
  factor variable (`InterventionGroup`) carries the depth of the key it hangs
  off, so it collapses with it. Field factors (`ColName`) sit inside a record
  and collapse first. No-depth factors last.
* Variant factors: `validate` refuses Collapse on a multi-level variant factor
  (pooling two pipelines is never meant); they default to a coloured grouping
  layer, as they default to COLOR today.
* Same chain sample-by-sample for 1-D measures and element-by-element for 2-D.
* Worked example (unbalanced): subject 01 trials {1, 2, 3}, subject 02 trial
  {9}. Nested: per-subject means 2 and 9 → bar 5.5, SD over {2, 9}. Pooled:
  mean of {1,2,3,9} = 3.75, SD over four values. Both are legitimate; only one
  is the default, and the checkbox says which.

### What disappears

* `Role.X`, `Role.COLOR`, `Role.AGGREGATE`, `Role.FREE`, `REPLICATE_ROLES`,
  `SINGLE_ASSIGNMENT_ROLES`.
* `iterate_ancestors` and `_fanout_notes`. Nothing is silent, so nothing
  needs promoting.
* The X/COLOR/FREE three-way for "one line per subject" on a 1-D plot.

### Defaults

Decided 2026-09-17: open **granular and fast** — the user is shown the
finest split first and pools deliberately.

* Scalar measure: grouping `[deepest schema key present]` — the variable's
  own level, e.g. `trial` for a trial-level variable under
  `[subject, session, speed, trial, cycle]` — uncoloured; **every other
  plain factor ITERATE**. One bar per trial, one figure per
  (subject, session, speed). Nothing is collapsed on opening, so the bar has
  no error bars and box / violin / band are greyed out until the user
  collapses something — the greyed-out reason says exactly that.
  "Deepest present" is read off `LongTable.factor_depths`, one owner.
* 1-D measure: grouping empty, every schema key ITERATE — **one record per
  figure**, the 2026-09-13 rule, kept: it is the same granular-first idea
  and the fast opening.
* 2-D measure: grouping empty (must be), every schema key ITERATE.
* Variant factor → grouping, coloured (extras → FACET, as today). Field /
  `Variable` factor → FACET.
* A factor a saved spec never mentions (one that appeared later) → ITERATE:
  it fans out visibly rather than pooling silently.
* `roles_for_kind` (auto-fix when a kind is chosen): box/violin/band with no
  sample → collapse the **deepest plain non-grouping factor** (with the
  opening state above, that is `speed`, the key just outside the grouped
  `trial`), so the sample is the closest-to-the-data key still available and
  the figure count shrinks by exactly one factor;
  spaghetti with < 2 layers → pull the deepest plain ITERATE factor into
  grouping (innermost position).

## Spec shape

```python
class Role(str, Enum):
    GROUP = "group"        # one mark per level; ordered by PlotSpec.groups
    FACET = "facet"
    ITERATE = "iterate"
    COLLAPSE = "collapse"

PlotSpec.roles: dict[str, Role]
PlotSpec.groups: list[str]          # innermost first; replaces x_layers
PlotSpec.color: str | None          # must be in groups
PlotSpec.aggregate.pooled: bool
```

`ordered_groups(roles, depths)` replaces `ordered_x_layers` with the same
reconciliation (drop names that lost GROUP, append GROUP-holders the list
never mentioned) — default placement by depth, **deeper first** now that the
list is innermost-first. `depthRank` in `xLayers.ts` mirrors it.

## Decisions to confirm before building

- **D1 — clean break.** DECIDED yes (user, 2026-09-17). Old role strings (`x`, `color`, `aggregate`, `free`)
  and `x_layers` fail on load with one message naming the doc. No mapping:
  `free` on a bar meant "sample", which is now `collapse`, and `x_layers` is
  the reverse order — a mapping would be wrong in the common cases. Generated
  endpoints embedding `Role.FREE` need re-exporting.
  (feedback_beta_no_deprecation)
- **D2 — the word "collapse".** DECIDED yes, rename (user, 2026-09-17). `collapse.py` / `PlotSpec.collapse_statistic`
  / `_Plan.collapse` today mean the 1-D→scalar *cell* reduction. Recommend
  renaming to `cell.py` / `cell_statistic` (user-visible in spec TOML) so the
  role owns the word.
- **D3 — pooled checkbox.** DECIDED yes (user, 2026-09-17). Beside the statistic / error dropdowns; it's a
  property of the whole chain.
- **D4 — band per leaf group.** DECIDED (user delegated, 2026-09-17): the
  coloured layer is the colour; every UNCOLOURED grouping layer on a band or
  line is composed into a series id and drawn with a **line dash style** per
  series (solid, dashed, dotted, dash-dot, cycling), legend entries for
  both. That is what a scientist reads off a figure without hovering, it is
  the same in preview and export (seaborn `relplot(kind="line", hue=colour,
  style="_series", estimator=…, errorbar=…)` keeps the estimator, unlike
  `units=`), and nothing is ever refused. Many uncoloured leaf groups (> the
  dash cycle) get a WARN in the narration suggesting Separate panels.
- **D5 — colour on a non-innermost layer.** DECIDED yes (user, 2026-09-17): colour applies to ANY grouping layer (whole outer group one colour):
  build it (cheap, well-defined) rather than restricting the tag.
- **D6 — MATLAB.** CLOSED 2026-09-17: grepped every `.m` under scimatlab
  and scistack-gui for `x_layers` / role strings / `roles` — the only
  `iterate` hits are the `for_each` iteration flag. No MATLAB consumer; the
  Python spec is the single owner.

## Stages

Each stage lands with its tests; the user runs pytest one package at a time.

### Stage 1 — spec + roles vocabulary
`scistackplot/spec.py`, `roles.py`, `capability.py`, `xaxis.py`
- `Role` as above; `groups`, `color`, `Aggregation.pooled`; D2 renames;
  `to_dict`/`from_dict` with the D1 loader error.
- `roles.collapse_order(roles, table) -> list[str]` — ONE owner of "deepest
  first"; `roles.sample_key(roles, table) -> str | None`.
- `roles.tick_layers(spec, table, kind)` / `series_layers(...)` — ONE owner
  of the per-kind reading of the grouping list (ticks vs. series vs.
  spaghetti split), used by reduce, xaxis, ylimits, codegen.
- Delete `iterate_ancestors`; `complete_roles` defaults unmentioned plain
  factors to ITERATE (a factor that appears later fans out visibly rather
  than pooling silently).
- `default_roles`, `roles_for_kind` per Defaults above.
- `validate`: `color ∈ groups`; labelled-layer cap; Collapse refused on
  multi-level variants; grouping refused for `MATRIX_2D`; spaghetti ≥ 2.
- `capability`: `has_sample`; `available_plots`; `why_unavailable` texts;
  `role_label` / `role_hint` for the three Factors-pane roles; a new
  `grouping_hint(kind)` ("one bar per combination, first entry innermost" /
  "one line per combination" / "first entry = the lines"); `ROLE_ORDER`
  shrinks; `grouping_summary` reports the list + colour.
- Logging: DEBUG collapse order + sample key; INFO on every refusal.
- Tests: `test_role_availability.py`, `test_collapse_capability.py`,
  `test_fanout_order.py` (promotion tests → "never promotes"), new
  `test_collapse_order.py`, new `test_grouping_layers.py` (per-kind reading,
  cap counts labelled layers only, colour must be a group).

### Stage 2 — reduction core (pandas path)
`scistackplot/reduce.py`, `ylimits.py`, `xaxis.py`, `series_stats.py`
- `reduce._collapse_levels(frame, spec, roles, table, index_column)`: loop
  over `collapse_order` **except the sample key**, each step
  `groupby(everything else present + index_column).mean()`; pooled mode
  collapses nothing here. Replaces `_collapse_aggregates`.
- `_panel_frame`: `tick_layers` → composed `__x` (innermost-first list
  composed outermost-first for the key, so `_plan_nested_x` / bracket
  drawing are untouched); `series_layers` → `__series`; `COLOR` column from
  `spec.color`. `_summarize` unchanged in shape — it now sees exactly the
  sample rows. Scatter/strip/line with a sample: final mean per leaf group.
- `_build_figure` narration: per-step row counts (`"collapse trial: 480 ->
  120 rows"`) then `"sample = subject (n=10)"`.
- `ylimits`: swap in `_collapse_levels`; `collapse=` flag → "any Collapse
  key"; `spread_bounds` unchanged.
- `_plan_cache_key`: include `pooled`, `groups`, `color`.
- Tests: `test_resolve.py`, `test_core.py`, `test_ylimits.py`,
  `test_x_nesting.py` (innermost-first list → same brackets as before), 
  `test_composed_keys.py`; new `test_nested_collapse.py` on the unbalanced
  fixture: nested ≠ pooled numerically, bar error = SD over per-subject
  means, box distribution = per-subject means, no-collapse bar has
  `Y_LOW == Y_HIGH == Y`, colour on an outer layer.

### Stage 3 — NumpyReducer parity (1-D at scale)
`scistackplot/reducer.py`
- `collapse_series` / `summarize_series` / `y_extents` follow
  `collapse_order` step by step; pooled = today's single pass.
- Tests: `scistackplotdb/tests/test_reducer_parity.py` nested + unbalanced +
  pooled cases, numpy == pandas.

### Stage 4 — renderers
`render/mpl.py`, `render/plotly_.py`, `resolved.py`
- Bars/boxes: nothing new (ticks arrive composed as before). Colour on an
  outer layer (D5). Band / line per series with dash styles (D4), shared cycle constant in `resolved.py`. Spaghetti: series = first layer.
- Tests: `test_render.py`, `test_legend_parity.py`, `test_spaghetti.py`.

### Stage 5 — codegen
`scistackplot/codegen.py`
- `_preamble`: one `groupby(...).mean()` per collapsed key in
  `collapse_order` minus the sample key, each commented; pooled emits nothing
  (seaborn's estimator pools). Seaborn computes `estimator` / `errorbar` over
  the sample rows — same numbers as the preview.
- `_nested_x_layers` / `_SERIES_COLUMN` from `tick_layers` / `series_layers`;
  `hue=spec.color`, `style=_series` for uncoloured layers (D4) — `units=` only for LINE with no sample and SPAGHETTI.
- Docstring names the sample key ("error bars: SD across subject").
- Tests: `test_codegen.py` — generated function run on the unbalanced fixture,
  numbers compared to `resolve`.

### Stage 6 — GUI
`PlotStudio.tsx`, `locationSelection.ts` (+ tests), `xLayers.ts`,
`scistack-gui/tests/test_plot_service.py`
- Grouping section: ordered list (↑/↓, innermost first, hint from
  `grouping_hint(kind)`), a colour radio per row, add/remove moves a factor
  between panes. Factors section: only ungrouped factors, three-entry
  dropdown. "Weight by N" checkbox beside statistic / error.
- `Role` union → `'group' | 'facet' | 'iterate' | 'collapse'`; every
  `?? 'free'` fallback → `'collapse'`; `locationSelection.ts:529`;
  `'averaged away'` summary text.
- `depthRank` flipped (deeper first). Rebuild BOTH vite targets
  (project_frontend_bundle_rebuild).

### Stage 7 — docs + memory
- `docs/claude/grouping-and-collapse.md`: the model section verbatim + the
  worked example.
- READMEs: `scistackplot/README.md` roles table and the "AGGREGATE does not
  count as replicates" paragraph → the sample rule; `scistackplotdb/README.md`
  example spec.
- `docs/claude/spaghetti-plot.md`, `plot-variant-rows.md`,
  `synthetic-factors.md`, `plot-variant-rows.md`: role names where they
  describe behaviour.
- Memory: new `project_grouping_collapse_roles`; update
  `project_scistackplot_layer`, `project_grouping_nesting_and_picker`.

## Test-suite blast radius

45 files reference `Role.FREE` / `Role.AGGREGATE` (list gathered
2026-09-17), and every test that builds `roles={"session": Role.X, ...}` or
`x_layers=[...]` changes shape. Most are fixture rewrites; behaviour changes
are called out per stage, plus `test_narration.py` (promotion note gone),
`test_multi_variable_pooling.py` (refusal wording), `test_variant_pin.py` /
`test_variant_rows_multivariable.py` (variant default = coloured group), and
every `default_roles` / `default_spec` test (scalar no longer opens on X +
COLOR + FREE — it opens on the deepest key, rest ITERATE) — including the GUI-side
`test_plot_service.py` opening-state assertions.

## Risks / things to watch

- **Numbers change** for any saved spec with ≥2 AGGREGATE keys (pooled →
  nested) and for any bar/band whose error came from a FREE key that wasn't
  the only non-channel key. The reference doc must say this.
- `x_layers` was outermost-first; `groups` is innermost-first. Every place
  that composes a key or plans brackets must go through `tick_layers`, which
  returns outermost-first for drawing — never reverse ad hoc.
- The spaghetti "first entry = lines" rule and the `MAX_X_LAYERS` "labelled
  layers only" rule are the two conventions a reader can't derive; both go in
  the doc and in `grouping_hint`.
- The GUI "collapse hint text" for spaghetti (spaghetti plan stage 6) refers
  to the 1-D cell collapse — D2's rename touches it.
