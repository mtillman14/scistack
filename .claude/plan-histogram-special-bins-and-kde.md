# Plan: histogram single-value bins, a NaN bar, and a KDE kind

Status: DRAFT 2026-11-01, waiting on the user's answers to the open questions.
Comes before `.claude/plan-plot-omissions.md`. That plan has its surface decided:
a toolbar button and popup, plus an optional footnote that is off by default.
"Show sample" on a histogram is shelved (user, 2026-11-01).

Owner map today (docs/claude/histogram-and-regression.md): `bins.EDGES_SOURCE` / `bin_edges`
own the edges, `bins.bin_table` owns the heights, `bins.histogram_limits` owns the y range, and
`render.base.histogram_x_range` owns the x range. Codegen emits `EDGES_SOURCE` verbatim.

## 1. Single-value ("exact") bins

- `HistogramOptions.exact_values: tuple[float | str, ...] = ()`, for example `(0,)`. Parsed by
  `bins.parse_bound`, so datetime text works too.
- `bins` stays the one owner. Values equal to an exact value (to within 1e-12 relative) are
  taken out BEFORE the regular edges are decided, then counted in their own bar.
- `bin_table` returns those bars with a new `resolved.BIN_KIND` column (`range` / `exact` / `nan`),
  so the renderers, codegen and the CSV `__bars` depth read one table.
- Drawing: see Q1.
- Logging: `histogram bins: exact 0 -> 37 value(s) (of 412)`.
- Tests: a value of 0 is counted once, in the exact bar and never in the range bar.
  Percent heights sum to 100. Preview, mpl and export bars match (`plot_geometry`).

## 2. NaN bar

- `HistogramOptions.show_nan: bool`, default: see Q2. It is on the histogram only.
- Needs the NaN count per panel and colour level. The panel frame `dropna(subset=[Y])` in
  `reduce._panel_frame` is exactly where the rows go today. Its count is recorded there and passed
  to `bin_table`. This is also the first producer for the omissions plan (reason `no_value`), so
  the count has one owner.
- A bar labelled "NaN", placed after the last edge with a one-bin gap. It gets its own tick label,
  and the x range owner widens to include it. The mark is `BIN_KIND == "nan"`, with a hatched fill.
- A NaN bar of zero height is not drawn. The GUI shows the toggle only when the figure has NaNs
  (`layout.meta.bins.n_nan`), like `missing_x`.

## 3. KDE kind

- `PlotKind.DENSITY = "density"`, labelled "Density (KDE)". It is a continuous-x kind beside
  HISTOGRAM in `CONTINUOUS_X_KINDS`, with the same requirements: it draws the sample, and the one
  grouping layer is its colour.
- `DensityOptions(bandwidth_adjust=1.0, fill=False, cut=0, rug=False)`.
- The curve has ONE source. `kde.KDE_SOURCE` (Gaussian, Scott bandwidth × adjust, a 200-point
  grid, normalised per colour level like the histogram) is executed by the preview and emitted
  verbatim by codegen. Codegen draws it with `ax.plot` / `fill_between` on a FacetGrid, never
  `sns.kdeplot` (the same reasoning as regression's band).
- A colour level with fewer than 2 values, or with zero spread, draws nothing. A layout note says why.
- The PSD of a 1-D signal is a different kind and is not part of this plan.

## Decisions (user, 2026-11-01)

- Q1: an exact bar gets its OWN slot LEFT of the range: one regular-bin wide, a one-bin gap,
  hatched, ticked `= 0`. Several exact values go in ascending order. The regular bins next to it
  count only the other values.
- Q2: under the density stat, the exact and NaN bars are INERT, with a note. Their values still
  stay out of the regular bins.
- Q3: percent with the NaN bar on means regular + exact + NaN = 100% of every row. With the NaN bar
  off, it is 100% of the non-NaN values.
- Q4: the NaN bar is ON by default. Its toggle is shown only when the figure has NaNs.
- KDE options: the defaults proposed above. The exact values and NaN do not apply to the KDE in
  this round.

## Status (2026-11-01)

All three are implemented. ALL Python tests pass (user, 2026-11-01, after the seaborn `bins=list(_edges)` fix). The
frontend typechecks, 668/668 node tests pass, and both bundles are rebuilt.
pyright finds no undefined names. Nothing has been checked visually; the
checklist is §0zzzzzzv in `docs/gui-manual-testing-todo.md`. Owner map:
`docs/claude/histogram-and-regression.md`, the "Special slots" and
"Density (KDE)" sections.

Tests to run (one package at a time):

    cd scistackplot && pytest tests/test_histogram_special_bins.py tests/test_kde.py tests/test_histogram.py -q
    cd scistackplot && pytest -q

Changes from the plan:

- The kind is `PlotKind.KDE = "kde"`, not "density", which would clash with
  `HistStat.DENSITY`.
- The NaN count is not yet the omissions plan's producer; it is the bins'
  own `n_nan`.
- `render.base.histogram_x_range` was renamed to `univariate_x_range`.
- Stacking and dodging moved into `BARS_SOURCE`.
