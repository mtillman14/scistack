# Plan: built-in HISTOGRAM and REGRESSION plot kinds

Drafted 2026-10-22. Status: IMPLEMENTED 2026-10-22 (all stages; recommendations accepted for Q1-Q9). Uncommitted; pytest not yet run.

## What exists already (found while planning)

- `PlotSpec.x_measure` is a working **wide join**: `sources/base.get_table(x_measure=)`
  joins it, the collapse chain averages it with y (`reduce` ~L1181), `_panel_frame`
  writes it to `__x`, and `capability.available_plots(has_x_measure=True)` offers
  SCATTER / LINE. Grouping layers become series, never ticks.
- **No GUI control sets `x_measure`.** It is only typed in `PlotStudio.tsx:717`, and
  `plot_service` forwards it. A regression plot needs an X-variable picker.
- A **datetime x is refused** in `roles.validate` ("not available yet"). That was
  deferred in `docs/claude/plot-datetimes.md` until continuous x-vs-y plots exist,
  which is this work.
- `TemporalAxis` / `ResolvedPlot.temporal` describe a **y** axis only, and
  `apply_temporal` converts only `spec.y_measure`.
- Formula owner: `csvstats.descriptive` owns every descriptive statistic (user decision
  2026-10-17; `spread.py` only adds drawing policy). csv-stats has **no linear
  regression** yet. The editable clone is `/workspace/csv-stats`.

## Owners (NOTE 4: one concept, one owner)

| Concept | Owner (new unless stated) | Consumers |
|---|---|---|
| Linear fit (slope, intercept, R², p, n, optional CI band) | `csvstats.regression.linear_fit` | reduce, codegen's exported code, (later) StatStudio |
| Bin edges (count / width / auto; numeric and datetime) | `scistackplot.bins.bin_edges` | reduce, plot_data, codegen preamble (restated + exec-parity test, the spaghetti precedent) |
| Fit annotation text (`R² = 0.42, p = 0.003`, `p < 0.001` rule) | `scistackplot.bins`'s sibling `scistackplot.fits.annotation_text` | mpl, plotly, codegen, autosize rules |
| Kind requirements | `roles.kind_requirement` (existing) | validate, capability |
| Temporal axis per side | `scistackplot.temporal` (existing, generalised to x and y) | reduce, renderers, codegen |

## Spec changes (`spec.py`)

- `PlotKind.HISTOGRAM = "histogram"`, `PlotKind.REGRESSION = "regression"`. Both join
  `BUILTIN_KINDS` automatically. HISTOGRAM joins `SCALAR_KINDS`, so a 1-D measure
  cell-collapses first. REGRESSION is offered only with `x_measure`.
- `HistogramOptions` (frozen):
  `bins: BinMode = AUTO | COUNT | WIDTH`, `bin_count: int | None`,
  `bin_width: float | str | None` (a number, or a duration text such as `"1h"` /
  `"7D"` on a datetime axis), `bin_range: tuple | None`,
  `stat: count | percent | density`, `multiple: layer | stack | dodge`.
- `RegressionOptions` (frozen): `fit_line: bool = True`, `show_stats: bool = True`,
  `ci_band: bool = False`, `fit_by: color | pooled`.
- `PlotSpec.histogram`, `PlotSpec.regression`. Classify both in
  `presets.FIELD_CLASSES` (travel with a preset) and `design.DESIGN_FIELDS` (display,
  not design). `to_dict` / `from_dict` / `restore_spec` (strict, no migration).

## Stages

### Stage 0: the formula owners (no drawing)

1. `csv-stats/src/csvstats/regression.py`: `linear_fit(x, y, level=0.95) -> LinearFit`.
   Centre x before fitting, because epoch-ms datetimes (~1.7e12) are badly conditioned.
   Return NaN fields with a `reason` for n < 3 or zero variance in x. p comes from
   the slope's t-test (scipy, imported lazily per `test_import_cost.py`). Add
   `band(xs)` for the t-based mean-response CI. Tests: compare against
   `scipy.stats.linregress` and a hand-computed case, plus the degenerate cases.
   Bump csv-stats and the `csv-stats>=` pin in scistackplot.
2. `scistackplot/bins.py`: `bin_edges(values, options, *, temporal) -> BinEdges(edges,
   width, mode, notes)`. AUTO uses `np.histogram_bin_edges(..., "auto")`, the same
   default seaborn uses. COUNT gives N equal bins over the range. WIDTH anchors at
   `floor(min / w) * w`, so a 1-day width starts at midnight wall clock. Parse
   duration text with `pd.Timedelta` into ms. Cap at `MAX_BINS` (e.g. 1000), above
   which the spec is refused with the width that would fit. Tests: each mode, a
   datetime width, anchoring, the cap, all-NaN, a single value.

### Stage 1: datetime x axis (prerequisite for both kinds)

- `apply_temporal` converts `x_measure` as well as y. `ResolvedPlot.temporal` becomes
  `temporal_x` / `temporal_y` (a clean break, per the beta rule), and every reader is
  updated: renderers, codegen, compare, diffbars, export, autosize, plotly
  `plotly_axis`, and mpl `apply_mpl_date_axis` on the x axis.
- Remove the deferred refusal in `roles.validate`. SCATTER and LINE then work with a
  datetime x for free. `plot-datetimes.md` gets updated.
- Tests (`test_temporal.py`): datetime x scatter in both renderers, the codegen date
  axis on x, and both axes temporal at once.

### Stage 2: availability and roles

- HISTOGRAM: scalar (or collapsible 1-D), **no** `x_measure`. Grouping layers are
  overlaid colour series, not ticks. Whether it needs a sample is open question Q4.
- REGRESSION: needs `x_measure`, so `available_plots(has_x_measure=True)` returns
  `[SCATTER, LINE, REGRESSION]`. It needs ≥3 points per fit at draw time; that is
  reported as a note, not a refusal.
- Inert with a reason (one owner each, following the existing pattern): show sample,
  compare, difference bars, spaghetti offsets, and log x on a histogram. Log axes on
  a regression are open question Q8.
- `_GROUPING_HINTS` entries. Tests: `test_role_availability.py` and
  `why_unavailable` texts.

### Stage 3: reduce

- HISTOGRAM: compute the bin edges **once per figure** across panels and colours
  (`reduce._histogram_edges`, carried as `ResolvedPlot.bins`), so panels are
  comparable, as `series_offsets` already is. Each panel's frame becomes a bin table
  (`__x_lo`, `__x_hi`, `__y` = stat, `__color`). This keeps the reduction in resolve
  and the payload small. The y limits then come from the bin table (starting at 0).
- REGRESSION: points are the sample rows as for SCATTER. Add one `linear_fit` per fit
  group (colour level, or pooled) per panel, stored on `Panel.fits:
  list[FitResult(level, fit, x_span, band)]`. The line runs over that group's own x
  range.
- Logging (NOTE 2): INFO for the edges chosen (mode, n, width, range, n values
  binned); INFO per fit (level, n, slope, R², p); WARN for a skipped fit, with the
  reason also in `layout_notes`; DEBUG `[timing]` for the bin and fit spans.

### Stage 4: renderers

- mpl: `ax.stairs` / `ax.bar(align="edge")` per colour; stack and dodge offsets come
  from the bin table. Regression draws a scatter, then `ax.plot` for the line,
  `fill_between` for the band, and `ax.text` for the annotation in the fit's colour,
  stacked at the top-left in axes fraction.
- plotly: bar traces with `x` = bin centre and `width` = bin width (ms on a date axis),
  using the matching `barmode`. Regression adds line traces and annotations at the
  same positions.
- autosize: the annotation joins the "no text into text" and "on canvas" rules.
- Tests: `plot_geometry` parity between the preview and the export (bar edges, line
  endpoints, annotation text).

### Stage 5: codegen (export matches preview)

- Histogram: the preamble restates `bin_edges` in plain numpy. A test execs it against
  the owner (`test_generated_bins_match_the_library_rule`). Then
  `sns.histplot(bins=edges, stat=..., multiple=..., common_norm=<per Q2>)`.
- Regression: `sns.scatterplot`, then per group `csvstats.regression.linear_fit`
  (exported code already imports csvstats for `ci_interval`), `ax.plot`, and `ax.text`.
  Never `sns.regplot`: its bootstrap CI would not be the previewed band.
- Datetime: `conversion_code` for x, and `mpl_date_axis_code` on x.

### Stage 6: Save data

- Histogram: two depths are offered. One is the binned values (sample rows plus a
  `bin` column holding its interval); the other is the bin table itself.
- Regression: the sample rows (x, y), and an optional fit table (level, n, slope,
  intercept, R², p).
- Tests in `test_plot_data.py`.

### Stage 7: GUI (Plot Studio)

- The kind grid gets the two kinds (the backend already lists them and gives reasons).
- **X variable** picker (new): a dropdown of joinable variables (`joinable_with`).
  Clearing it returns to the categorical kinds.
- Histogram section: Bins (Auto / Count / Width). Width takes a number, or a duration
  with a unit selector on a datetime axis. Validated by the backend (`capabilities.
  histogram.problem`). Also Stat and Multiple.
- Regression section: Fit line, Show R² and p, CI band, Fit by.
- `docs/gui-manual-testing-todo.md` entries; rebuild both vite targets; frontend
  unit tests for the pure logic (`histogram.ts`, `regression.ts`).

### Stage 8: docs

- `docs/claude/histogram-and-regression.md`, a scistackplot README section, the
  csv-stats README, and a memory entry.

## Open questions (Q-numbers referenced above)

- Q1 Bins: is one mode active at a time (Auto | Count | Width)? Recommended: yes.
- Q2 Percent/density across colours: normalise per colour level, or over the whole
  panel? Recommended: per level (`common_norm=False`), which compares shapes.
- Q3 Several colour levels: layer (translucent), stack, or dodge as the default?
  Recommended: layer.
- Q4 Must a histogram have a sample (a collapsed factor, like box/violin), drawing one
  count per sample row? Recommended: yes ("every kind draws the sample").
- Q5 Fit grouping: one fit per colour level, or one pooled fit? Recommended: per
  colour level, pooled when uncoloured, with `fit_by` to override.
- Q6 CI band: off by default, t-based from csvstats? Recommended: yes.
- Q7 Datetime bin widths: fixed durations only (s, min, h, D, W), no months?
  Recommended: fixed only for v1.
- Q8 Log axes on a regression: fit raw values, or make log inert? Recommended: inert,
  with a note.
- Q9 Annotation contents: R², p, n; slope off (on a datetime x it would be per ms)?
  Recommended: R², p and n.
