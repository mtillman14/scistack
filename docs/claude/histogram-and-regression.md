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
| histogram x range | `render.base.univariate_x_range` (exactly the outer slot edges, seaborn's sticky edges; a KDE's curve extent) | mpl, plotly, codegen |
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

## Special slots: single-value bins and the NaN bar (2026-11-01)

Plan: `.claude/plan-histogram-special-bins-and-kde.md`. Tests:
`scistackplot/tests/test_histogram_special_bins.py`.

User decisions:

- A value in `HistogramOptions.exact_values` (e.g. 0) leaves the regular bins
  and gets its own hatched bar LEFT of the range. The bars are ascending, one
  regular bin wide (the median width), after a one-bin gap, each with a tick
  labelled `= 0`.
- `show_nan` (default **on**) adds a hatched `NaN` bar RIGHT of the range. It
  is drawn only when the figure has NaNs. `BinEdges.n_nan` is reported either
  way, so the GUI shows its toggle exactly when it matters.
- Percent is out of every value drawn: regular (in range) + exact + NaN (when
  shown).
- Under **density**, neither special bar is drawn, and a note says why. The
  exact values still stay out of the regular bins.
- Numbers only. On a date axis both are ignored, with a note.

**One owner:** `bins.BARS_SOURCE`, executed by `bins` and emitted verbatim by
codegen next to `EDGES_SOURCE`:

| function | decides |
|---|---|
| `_scistack_specials` | which special slots are drawn (none under density) |
| `_scistack_regular` | which values the regular bins hold (finite, equal to no exact value) |
| `_scistack_slots` | every slot left to right: lefts, rights, kinds (`exact`/`range`/`nan`), labels |
| `_scistack_bar_heights` | one colour level's counts and heights over those slots |
| `_scistack_arrange` | stacking and dodging (moved here out of `bin_table`) |
| `_scistack_hist_ticks` | round-number ticks over the regular range, plus the special ticks; None when there is no special slot |

Readers:

- `BinEdges.slots()`, `.x_range()` and `.ticks()`.
- `bin_table`, which adds the `resolved.BIN_KIND` / `BIN_LABEL` columns.
- `render.base.univariate_x_range` / `histogram_ticks` / `is_special_bin` /
  `SPECIAL_BIN_HATCH` / `SPECIAL_BIN_PATTERN` (used by mpl and plotly).
- The export: `_histogram_edge_lines`, then `_histogram_special_lines`.
- The `__bars` CSV: a `bin_kind` column. An exact bar's start and end are its
  value; the NaN bar has none.

NaN rows now reach `_bin_panels`: `reduce._panel_frame` skips its
`dropna(subset=[Y])` for a histogram.

**Export:** seaborn draws only the regular rows (`data=df[_hist_regular]`).
Percent is drawn as `stat="count", weights="_hist_w"`, where each row weighs
100 / what its panel and colour level draws, because seaborn's own percent
would leave the special slots out of the denominator. The special bars are
then drawn with `ax.bar` per panel from the same source functions, and the
ticks with `set_xticks`.

## Density (KDE) kind (2026-11-01)

`PlotKind.KDE` ("kde", labelled "Density (KDE)" in the GUI). Tests:
`scistackplot/tests/test_kde.py`.

- `spec.UNIVARIATE_KINDS = (HISTOGRAM, KDE)` is the one statement of
  "univariate": the kind draws the sample, refuses an x measure
  (`roles.KDE_NO_X`), takes one grouping layer as its colour
  (`roles.grouping_layers`), puts a datetime measure on a date x axis, and
  titles x with the measure. KDE is also in `CONTINUOUS_X_KINDS`,
  `SCALAR_KINDS` and `SAMPLE_NEEDING_KINDS`.
- **The curve's owner** is `kde.KDE_SOURCE`, `_scistack_kde`: a Gaussian
  kernel with Scott's bandwidth (`std(ddof=1)·n^-1/5`) × `bandwidth_adjust`,
  on 200 points from `min − cut·bw` to `max + cut·bw`. It equals scipy's
  `gaussian_kde(bw_method="scott")` (tested), written in pure numpy. Each
  colour level is normalised within its panel (area 1 before the cut).
- `KdeOptions(bandwidth_adjust=1, cut=0, fill=False)` is plan-irrelevant and
  validated by `kde.kde_problem`.
- `reduce._kde_panels` replaces each panel frame with `kde.curve_table`
  (`__x`, `__y`, `__count`, the colour). It sets the y limits through
  `kde.density_limits` (0 to 5% above the top; per panel when the scope names
  a facet) and `ResolvedPlot.kde_range` (the curves' extent).
- A colour level with fewer than two different values draws nothing, and a
  layout note names it.
- Renderers: `mpl._draw_kde`, and the plotly `mode: lines` trace
  (`fill: tozeroy` when filled). Styling is `kde.LINE_WIDTH` /
  `FILL_ALPHA` / `Y_TITLE`.
- **Export:** `_kde_curve_lines` builds `_curves` in the preamble (the same
  source, grouped by facets + colour). `sns.relplot(kind="line",
  estimator=None, sort=False)` draws it. `_kde_fill_lines` adds the fill and
  `_kde_limit_lines` sets the limits. Seaborn's own KDE is never used.
- GUI: Chart > **Density curve** (`KdeSection`; Smoothing, Extend, Shade),
  using `capabilities.kde` (`capability.kde_summary`).
- Not done: a `__curves` CSV depth, a rug, exact values and NaN on the KDE.
  A pile of zeros reads as a bump on the curve; the histogram's own bar is the
  tool for that.

## Show sample on a histogram / KDE: the rug (2026-11-01)

Plan: `.claude/plan-histogram-rug.md`. Tests: `scistackplot/tests/test_histogram_rug.py`.

What the user asked for: a way to see how the data sits INSIDE each bar, so
they can judge the bin design themselves. No automatic note or hint.

- **Form:** `roles.overlay_is_rug(spec)` (univariate kinds) is the one
  statement that "Show sample" draws a rug, not points. `overlay_unavailable`
  allows these kinds, and `overlay_color` / `overlay_join` are inert on them.
  `ylimits` skips the rug, because it never sets the y range.
- **One owner:** `bins.RUG_SOURCE`, emitted verbatim by codegen.
  - `_scistack_rug_positions`: a tick sits at its value. A value held in a
    single-value bin sits at that bin's slot centre, and NaN draws no tick.
  - `_scistack_rug_counts`: crowding is the number of ticks in the same
    1/`RUG_SLOTS` (400) of the axis. A fraction of the axis, not of the
    screen, so the preview, the saved image and the export shade alike.
  - `_scistack_rug_shade`: 0 to `RUG_SHADES - 1` (8 shades), against the
    level's darkest count over the WHOLE figure.
  - `_scistack_rug_color`: the level's colour mixed with white, from 25% to
    100%. Takes hex or `rgb()`.
- **Rows:** one per colour level, in colour order, `RUG_ROW_HEIGHT` (3% of
  the axes) each, stacked upward from the bottom of the panel. x is in data
  units and y in axes fraction.
- **Readers:** `bins.rug_tables` (called by `reduce._rug_panels` after the
  bins / curves, which sets `Panel.rug`, clears `Panel.sample` and sets
  `ResolvedPlot.rug_scale`) and `render.base.rug_rows` (shared by
  `mpl._draw_rug`, which draws `vlines` on the x-axis transform, and
  `plotly_._rug_traces`, which draws one lines trace per row and shade plus a
  hover markers trace). The export reads `codegen._rug_lines` over `_sample`.
  The capability report gives `sample_overlay.form`, and the meta gives
  `rug.scale` / `rug.slots`.
- **GUI:** the Show sample keys, plus the scale note (`showSample.rugScaleNote`).
  Join, "Colour points by", "Show in legend" and Sample weight are hidden for
  a rug.

## Known limits

- Histogram y range is per figure, not across a fan-out.
- 7-day bins anchor on Thursdays (1970-01-01); month bins are not offered.
- seaborn's stacking order (last hue level at the bottom) is relied on, not
  stated; `test_generated_code_draws_the_previewed_bars` covers layered only.
- The annotation can sit over points; pick another corner.
- **seaborn trap (2026-11-01):** whenever `weights=` is passed (our percent
  histograms), seaborn 0.13 tests `bins == "auto"`. With an ndarray `bins`,
  that comparison raises "truth value of an array is ambiguous". The export
  therefore passes `bins=list(_edges)`; the guard test is
  `test_generated_edges_are_a_list_for_seaborn`.
