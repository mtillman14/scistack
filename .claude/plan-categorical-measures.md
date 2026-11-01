# Plan: categorical (text) measures in Plot Studio

*2026-11-01. Background: `docs/claude/categorical-measures.md` (the current
state: where text is dropped, and the four copies of the "plottable" rule).*

## Goal

A text or boolean measure can be plotted, and no field of a wide variable is
hidden from the pane. Every plot kind discussed is in scope:

| # | View | Built as |
|---|---|---|
| 1 | Count plot (one bar per level) | BAR / HBAR over the derived **Level** factor, `stat=count` |
| 2 | 100% stacked bar | BAR / HBAR, Level on colour, `stat=percent`, `multiple=stack` |
| 3 | Proportion of one level ± CI | BAR / HBAR, `CategoricalOptions.level="fall"` |
| 4 | Contingency heatmap | HEATMAP in a new "scalar grid" form (two grouping layers) |
| 5 | Categorical strip / dot | STRIP with a category y axis |
| 6 | Categorical spaghetti | SPAGHETTI with a category y axis (ordinal states over sessions) |

## Design: one derived table owns text → numbers

Follow `cell.apply_cell_collapse`: a new derived table rewrites the measure
early, so everything downstream (roles, grouping, facets, compare, Show
sample, y limits, codegen, Save data) sees an ordinary numeric measure.
`scistackplot/categorical.py::apply_categorical(spec, table)` is **the one
owner** of how text becomes numbers. It runs in `reduce._build_plan_timed`
after `apply_cell_collapse` and before `apply_temporal` / `validate`. The
**kind implies the mode**, so there is no toggle:

- **Summary mode** (BAR, HBAR, HEATMAP): one-hot. Each record becomes one row
  per level *of that record's field*, with a 0/1 indicator as the measure and a
  synthetic **`Level`** factor (shadow-safe name, like `ColName`).
  - The mean of a 0/1 indicator is a proportion. Because the collapse chain
    averages deepest first and unweighted, `subject=COLLAPSE` gives each subject's
    proportion, then mean ± CI across subjects, with no new statistics code.
    The proportions of all levels still sum to 1 after nesting (the mean is
    linear), so a stacked bar stays correct.
  - `stat=count`: the **deepest** collapse step sums (counts per unit) and outer
    steps average ("mean number of falls per subject"). With no collapse it is
    the count of records. This needs `Statistic.SUM`, used only by this path.
  - `CategoricalOptions.level`: keep only that level's indicator, so the
    `Level` factor is constant and dropped (invariant 6), and the label reads
    "% fall".
  - One-hot is per field: in a melted wide variable, `Outcome`'s levels never
    produce zero rows for `Side`'s levels.
- **Raw mode** (STRIP, SPAGHETTI): ordinal codes. Each text value maps to its
  index in the measure's level order, and the renderers label the y ticks
  with the level text.
  - **`Role.COLLAPSE` is refused** ("text values cannot be averaged; use a
    proportion bar, or group or separate by that key"). A spaghetti whose
    (line, x) cell holds two or more records is refused the same way, naming
    the extra key.
  - Mode ("most common") as a collapse statistic is **out of scope** and
    listed as an extension.

`PlotSpec.categorical: CategoricalOptions(stat=COUNT|PERCENT, level=None)`,
inert on non-categorical measures. `restore_spec` reads it leniently.

**Level order** for a categorical measure comes from `ScidbSource._ordered`
(declared `[schema_keys]`-style order, then natural sort), stored once on
`MeasureInfo.levels`. The renderers, codegen and the `Level` factor all read
that tuple and never re-sort. `DataFrameSource` fills it with the same natural
sort.

**Free text is refused**: more than `MAX_GROUP_LEVELS` (50) distinct values
gives the reason "free text / an identifier, not categories". It is shown in
the pane, not dropped silently.

## Mixed wide variables (the hidden ColNames)

One melted value column holds one shape, so text fields cannot join the numeric
melt. **Recommendation:** split a mixed wide variable into one catalog measure
per shape group:

- `Gait` keeps the numeric fields under `ColName`, as today.
- `Gait (text)` holds the text fields under its own `ColName`.
- `_melt_fields` partitions the fields, and that partition is the one owner.
  The catalog lists both measures, so every field is reachable.
- The numeric measure's panel shows a note: "Text fields Outcome, Side: plotted
  under Gait (text)". The same rule can later rescue the datetime fields that
  are dropped today (`Gait (time)`), but that is out of scope.
- Carried own-column groupings (`carry=`) are unchanged: a field used as a
  grouping is not also a measure.

*Alternative to confirm at approval:* list every field under one `ColName`
and switch the shape when the location filter selects only text fields. That
needs an "effective shape" decided by the filter, which runs after `validate`.
That is the threading `measure-shape-and-collapse.md` rejected, so it is not
recommended.

## Stages

Each stage adds logging and tests (NOTE 2). The user runs the tests,
one package at a time.

### Stage 0: single owner of "plottable"
- `shape.is_plottable` is the only rule. `ScidbSource.describe`,
  `LongTable.describe` and `plot_service._plottable` call it (delete
  `_plottable`).
- Test: a grep-style guard test (like `test_no_shadowed_definitions.py`)
  that fails if the shape tuple appears inline again, plus one test that the
  three describe payloads agree for every `Shape`.
- No behaviour change yet.

### Stage 1: categorical is plottable; nothing hidden
- `is_plottable` accepts `CATEGORICAL`. `MeasureInfo.levels` is added.
- `_melt_fields` partitions mixed variables (above) and stops the silent drop.
  Each partition is logged at INFO. A text-only wide variable no longer raises.
- Text row-list fields split into rows like numeric ones (`explode_row_lists`),
  then classify as CATEGORICAL.
- `plot_service._ineligible_reason` loses the "holds text" sentence and keeps
  the free-text refusal.
- `capability.available_plots(CATEGORICAL, ...)` returns BAR, HBAR, HEATMAP
  (when its grid rule holds), STRIP and SPAGHETTI. `default_plot` returns BAR
  with `stat=count` and `Level` as the x grouping (D2). Role hints get a
  categorical row.
- Tests: `scistackplotdb/tests/test_categorical_catalog.py` (mixed / text-only
  / free text / row-list), and `scistackplot/tests/test_categorical_capability.py`.

### Stage 2: summary mode: count plot and proportion ± CI (views 1, 3)
- `categorical.py` with `apply_categorical` (one-hot) and `Statistic.SUM` in
  the collapse chain (deepest step only, owned by the collapse-chain code, not
  by `categorical.py`).
- INFO line per resolve: "categorical %r → %d level(s), %d → %d row(s), stat=%s,
  level=%s, %.3fs".
- Compare to reference works unchanged. For percent it is a difference in
  percentage points, and the label says so. A % change of a proportion is
  allowed but labelled.
- Tests: `test_categorical_summary.py` checks hand-computed counts and nested
  proportions (2 subjects × unequal trial counts, so pooled ≠ nested),
  sum-to-one per x group, a single level, and per-field one-hot.

### Stage 3: stacking (view 2)
- `HistMultiple` becomes a shared `Multiple` enum (one vocabulary).
  `PlotSpec.bar.multiple: DODGE|STACK` for BAR/HBAR on any measure.
- Stacked bars draw no error bars: inert, and the reason is reported, like
  comparison inertness.
- Both renderers (`render/mpl.py`, `render/plotly_.py`) stack the colour layer.
- Tests: geometry (`plot_geometry.py` helpers) confirms that the stacked heights
  equal the per-level proportions and the tops reach 100%. Also preview/paper parity.

### Stage 4: scalar-grid heatmap (view 4)
- HEATMAP is offered for a SCALAR measure (so also categorical summary)
  when there are exactly two grouping layers: x = outer, y = inner. Cell value =
  the reduced statistic.
- It also works for numeric measures (mean per subject × session).
- `capability` owns the two-layer rule. `grouping_hint` explains it.
- Tests: the grid matches a pandas crosstab, and the refusal reason with one or
  three layers.

### Stage 5: raw mode: categorical y axis (views 5, 6)
- Ordinal codes in `apply_categorical`. `MeasureInfo.levels` drives the y
  ticks in both renderers. `ylimits` pads by half a category, and a log axis is
  inert with a note.
- The COLLAPSE refusal and the spaghetti multi-record refusal go in `validate`.
- Show sample does not apply (nothing is summarised) and is reported as inert.
- Tests: `test_categorical_raw.py` covers tick labels in level order, the
  refusal reasons, and the spaghetti offsets unchanged.

### Stage 6: export and Save data parity
- `codegen` emits the same one-hot / ordinal code (`pd.get_dummies` per
  field + `groupby` chain with the deepest sum for count; `pd.Categorical`
  codes for raw), stating `order=` from `MeasureInfo.levels`.
- Save data: the value column is named after what it holds
  (`Outcome, % of records`, `Outcome, count per subject`).
- Tests: extend `test_fanout_parity.py`, `test_plot_data.py` and `test_codegen.py`
  with exported-vs-preview tests for every view 1–6.

### Stage 7: GUI
- Measure dropdown: text measures get a "text" badge, and the "(text)" partition
  measures are listed.
- A **Categories** rail group (shown only for categorical measures) has
  Count / Percent and "Show one level: [all | level…]".
- Bar options get Dodge / Stack. Kind grid icons and hints for the categorical
  rows.
- Rebuild both vite targets (frontend bundle trap). Update the GUI manual
  testing doc.
- Tests: node tests for the new rail group's visibility rule.

### Stage 8: docs
- `scistackplot/README.md`: add a "Text measures" section and a categorical row
  in the shape table.
- `scistackplotdb/README.md`: describe the mixed-variable partition.
- Update `docs/claude/categorical-measures.md` to describe the finished state.
  Update the `spec.py` `factor_variables` comment, which still says text is
  "rightly refused as measures".

## Out of scope (listed extensions)
- Mode ("most common") as a collapse statistic for raw categorical views.
- Diverging stacked bars for Likert scales. They need a declared neutral level.
- Mosaic plots and pie charts.
- StatStudio tests for categorical outcomes (chi-square, logistic or ordinal
  mixed models).
- A datetime partition (`Var (time)`) using the same split rule.

## Decisions (user, 2026-11-01)
1. **Mixed wide variables:** split the text fields into `Var (text)`, as recommended.
2. **Default view:** a count bar, with one bar per `Level` (the text categories on
   x) and height = count. (Show sample was moved to the Proportion bar by
   decision 5.)
3. **Booleans** stay categorical (True/False levels).
4. **For every text plot, Collapse means "pool"** (user, 2026-11-01). No
   averaging happens anywhere: a collapsed key merges its records into one
   bucket. The **last** collapsed key is still the sample, as for numeric
   plots: it is what uncertainty is computed over and what Show sample draws.
5. **Two bar types** (user, 2026-11-01):
   - **Count bar**: height = the number of pooled records. There is no
     sample, so it has no error bar and Show sample is not offered. It is the
     default (D2): one bar per `Level` on x.
   - **Proportion bar**: the categorical equivalent of the mean. Everything
     beneath the sample key is pooled, which gives one proportion per sample
     unit (e.g. per subject). The bar is the mean of those proportions ±
     the chosen error band. **Show sample works** and draws the per-unit
     proportions, so the bar is always the mean of its points, as in a numeric
     bar.
   - In the spec both are `BAR` / `HBAR` + `CategoricalOptions.stat`
     (COUNT | PROPORTION), so orientation, stacking, facets and compare are
     shared. The kind grid shows them as two tiles, "Count" and "Proportion".
     `capability` owns the tiles' availability: Proportion needs a sample, like
     box/violin.

## What "Collapse = pool" does to each feature

One rule replaces the per-kind refusals in the earlier draft:

| Feature | Under "pool" |
|---|---|
| Count bar | Count of pooled records per cell. Every role combination works. |
| Proportion bar | Pool beneath the sample key, then mean ± band across units. Show sample at the sample key or any key above the records. A record-level point is a bare 0/1, so record level is not offered. |
| 100% stacked bar | Proportion bar with `multiple=stack`. Error bars are inert (reason reported). |
| Heatmap | Cells are count or proportion over the pooled records. |
| Strip (category y) | Every pooled record is drawn. There is no summary mark, so Show sample is redundant and hidden. |
| Spaghetti (category y) | **The one exception.** A line needs one value per x. If pooling leaves two or more records per (line, x), it is refused with a reason that names the key. The fix is to Group or separate that key, or to use a Proportion spaghetti (below). |
| Proportion spaghetti | Spaghetti in summary mode: one line per unit through its proportion at each x. Pooling works, and this is the repeated-measures view for categorical data. |
| `Level` factor collapsed | Allowed. Pooling the categories gives the total N for a count. For a proportion it would be 100% everywhere, so it is refused with a reason for Proportion only. |
| Value filter on the measure | A level include-list, never a numeric range (unchanged). |

Implementation consequence: `apply_categorical` does the pooling itself (a
`groupby` over the non-collapsed keys plus the sample key, with counts per
level). It hands downstream code one row per (cell, sample unit, Level) with a
count or proportion. The numeric collapse chain then sees only the sample step,
so `Statistic.SUM` and the "deepest step sums" rule are **no longer needed**.
`apply_categorical` is the only owner of categorical aggregation, and its INFO
line reports the pooled keys, the sample key and the number of units per cell.

The stage 2 description above ("one-hot ... deepest step sums") is superseded
by this section. Stage 5 raw mode refuses only the spaghetti case.

## Open questions
1. With no sample (nothing collapsed), should the Proportion bar be
   unavailable (recommended, the same rule as box/violin), or draw a binomial
   (Wilson) CI over the pooled records? A Wilson CI treats every trial as
   independent, which is the pseudoreplication the sample rule exists to
   prevent.

## Status (2026-11-01): implemented, awaiting the user's test run

Deviations from the stages above, decided during implementation:
- **Stacking is `BarOptions.stack: bool`**, not a shared `Multiple` enum
  renamed from `HistMultiple`: a bar has only dodge-or-stack, and renaming
  the histogram's enum touched every histogram reader for no gain. Stacking
  takes the coloured layer out of the ticks (`roles.grouping_layers`) — colour
  is otherwise a tick ("colour is paint").
- **No `Statistic.SUM`**: `reduce._collapse_levels` pools with `sum` / `mean`
  itself, from `categorical.pooled_reduction` (decision 4 made the "deepest
  step sums" rule unnecessary).
- `Level` is dropped from the spec (as answered) in VALUE mode and when one
  category is chosen (`categorical._consumed`).
- The grid heatmap export lays out the first facet only.

Tests to run (one package at a time):
- `pytest scistackplot/tests/test_categorical.py scistackplot/tests/test_categorical_export.py scistackplot/tests/test_plottable_owner.py -x -v`
- `pytest scistackplot/tests -x -q`
- `pytest scistackplotdb/tests/test_categorical_catalog.py scistackplotdb/tests/test_row_list_columns.py scistackplotdb/tests/test_factor_variables.py -x -v`
- `pytest scistackplotdb/tests -x -q`
- `pytest scistack-gui/tests/test_plot_service.py -x -q`
