# Histogram and regression: the continuous-x built-in kinds

*Built 2026-10-22. Plan: `.claude/plan-histogram-regression.md`. Tests:
`scistackplot/tests/test_histogram.py`, `test_regression_kind.py`,
`csv-stats/tests/test_regression.py`, `scistackplotdb/tests/test_endpoint.py`,
frontend `continuousKinds.test.ts`.*

## User decisions (2026-10-22)

- Bins: ONE mode at a time — auto (numpy's estimator), a count, or a width.
- Percent / density are normalised per colour level within a panel
  (seaborn `common_norm=False`).
- Several colour levels default to overlaid (`multiple="layer"`); stacked and
  side-by-side are options.
- A histogram draws the SAMPLE, so it needs a collapsed factor, like a box.
- Regression: one fit per colour level, pooled when uncoloured (`fit_by`
  overrides); a t-based CI band, off by default.
- Datetime bin widths are fixed durations only (s, min, h, D, W; no months).
- Log axes are ignored (inert, with a note) on both kinds.
- The annotation is `R² = …, p = …, n = …`; no slope (per ms on a date x).

## Owners

| concept | owner | readers |
|---|---|---|
| the fit (slope, R², p, band) | `csvstats.regression.linear_fit` (csv-stats 0.5.0) | `fits.panel_fits`, the exported code |
| bin edges | `bins.EDGES_SOURCE` (run by `bins.bin_edges`, emitted verbatim by codegen) | reduce, codegen |
| bar heights, stacking, dodging | `bins.bin_table` | reduce → renderers read `__x_lo/__x_hi/__y_base/__y` |
| histogram y range | `bins.histogram_limits` (= matplotlib's bar autoscale) | reduce; codegen restates from drawn patches |
| histogram x range | `render.base.histogram_x_range` (exactly the outer edges, seaborn's sticky edges) | mpl, plotly, codegen |
| annotation text | `fits.STATS_TEXT_SOURCE` (run by `fits.annotation_text`, emitted by codegen) | mpl, plotly, codegen |
| annotation placement | `fits.corner` / `fits.annotation_offsets` (points; preview px = pt) | mpl, plotly, codegen |
| what each kind needs | `roles.kind_requirement` (sample; ≤1 grouping layer) and `roles.continuous_kind_problem` (x measure needed / refused; bin settings) | validate, capability |
| log axes ignored | `roles.continuous_log_inert` | reduce, codegen |
| datetime x | `temporal.apply_temporal_x` → `_Plan.temporal_x` → `ResolvedPlot.temporal_x` | renderers (`apply_mpl_date_axis(ax.xaxis)`, plotly `type: date`), codegen, export |

`CONTINUOUS_X_KINDS = (HISTOGRAM, REGRESSION)` (spec.py): grouping layers are
series, never ticks (`roles.grouping_layers`); Show sample, compare and
difference bars do not apply.

## Histogram path

The measure (y_measure) is binned ALONG x. `_panel_frame` builds the usual
`__y` frame; `reduce._bin_panels` then decides the edges once per FIGURE over
every panel's `__y` and replaces each panel frame with its bar table. The y
axis counts bars (title from `bins.y_title`), the x title is the measure
(`reduce.x_axis_title`). A histogram's one grouping layer is its colour
automatically. For a datetime measure, `_attach_temporal` puts the axis on
`temporal_x` instead of y. Picking Histogram on a scalar measure's untouched
opening state collapses the grouped level (`roles_for_kind` →
`with_requirements_for`).

The y range is per figure (across its panels) — or per panel when the y-limit
scope names a facet — never across a fan-out: each exported endpoint sees one
figure. Pinned y ends win.

## Regression path

An x-y scatter (`x_measure`, the existing wide join) plus
`reduce._fit_panels`, which stores `fits.PanelFit`s on `Panel.fits`. A fit
that cannot be made (n < 3, no x spread) draws nothing and says why in
`layout_notes`.

Neither kind is transport-strided (`max_points`): a stride would change the
counts and the fit.

## Export

- Histogram: `sns.displot(kind="hist", bins=_edges, stat, multiple,
  common_norm=False, shrink=1, alpha, edgecolor, linewidth)`, x limits to the
  edges, y from the drawn patches (×1.05).
- Regression: `sns.relplot(kind="scatter")`, then per panel and colour level
  `linear_fit`, `ax.plot` / `fill_between`, `ax.annotate`. Never
  `sns.regplot` (its bootstrap band is not the preview's).
- **Fixed on the way:** an x-y export's function never took the `df_x` input
  scistackplotdb's `for_each` passes. `function_params` now includes it and
  `_preamble` joins it on the shared schema keys (inner, like
  `hierarchy.join_frames`); a flat table already holding the column skips the
  join (the standalone script passes `df` twice).

## Save data

Two extra depths: `__bars` (one row per bin and colour level: `bin_start`,
`bin_end`, `count`, and the stat when not count) and `__fits` (one row per
fit: `n, slope, intercept, r_squared, p_value, reason`). Both are read off the
resolved figures. A datetime x is written as ISO text.

## GUI

Data > **X variable** (from `describe.joinable_with`; picking one moves a
categorical kind to Regression, clearing moves Regression/Line to Scatter —
`continuousKinds.kindForXMeasure`). Chart > **Histogram** and **Regression**
sections appear for their kind; readouts from `layout.meta.bins` /
`layout.meta.fits`; problems from `capabilities.histogram/regression.problem`.

## Known limits

- Histogram y range is per figure, not across a fan-out.
- 7-day bins anchor on Thursdays (1970-01-01); month bins are not offered.
- seaborn's stacking order (last hue level at the bottom) is relied on, not
  stated; `test_generated_code_draws_the_previewed_bars` covers layered only.
- The annotation can sit over points; pick another corner.
