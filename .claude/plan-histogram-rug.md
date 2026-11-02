# Plan: "Show sample" on a histogram / KDE = a crowding-shaded rug

Status: DRAFT 2026-11-01, waiting for the user's approval.

## Goal (user, 2026-11-01)

Show how the data sits INSIDE each bar, so the user can judge the bin design
themselves. For example: values at both ends of a bar but none in the middle
means the bin is too wide. The sample values go on the x axis, never the y axis.
There is no automatic note or suggestion; reading the rug is the user's job.

## Decisions

- D1: a **rug**. One short tick per sample value, at its exact x.
- D2: **one row per colour level**, stacked in colour order.
- D3: **shade = crowding**, light to dark in the level's own colour. Light means
  1 value; dark means the level's most crowded spot.
- D4: no hint or note about the bins.
- D5: the same rug on the **KDE** kind.
- D6: which values. By default, the values the histogram counts (its sample).
  A deeper key ticked in Show sample draws those deeper values instead, which
  is Show sample's existing meaning (`roles.overlay_steps`).
- D7: values in a single-value bar sit under that bar's slot. NaNs have no x,
  so they draw no tick (the NaN bar already counts them).

## Proposed choices (for approval)

- P1, **what "crowded" means**: the number of the level's values in the same
  1/400 of the x axis's width (`RUG_SLOTS = 400`). The window is a fraction of
  the axis, not screen pixels, so the preview, the saved image and the export
  shade identically at any size. At a typical 6-inch-wide plot, 1/400 is about
  1 px.
- P2, **where it is drawn**: inside the axes, along the bottom, like seaborn's
  `rugplot`. Each row is 3% of the axes height and the rows stack upward,
  partly covering the bars' bottom edge. The alternative is a strip below
  zero, which needs a y range extended below 0 with the negative ticks
  hidden. That is fiddly in plotly across facets, so it is not recommended.
- P3, **the scale key**: there is no colourbar in the figure. The GUI's Show
  sample section says "light = 1 value, darkest = N values in 1/400 of the
  axis" for each level, and the plotly hover gives each tick's value and
  count. The exported figure shows only the shading.
- P4, **plotly drawing**: per panel and level, one `shape` path per shade, with
  shades quantised to `RUG_SHADES = 8`. That keeps it to at most 8 shapes per
  row instead of one per value. A hidden `markers` trace carries the hover.

## One owner

`bins.RUG_SOURCE`, emitted verbatim by codegen like `BARS_SOURCE`. It holds:

- `_scistack_rug(values, lo, hi, slots, shades)` → `(xs, counts, shade_index)`.
  It maps each finite value to its slot and counts the slot. The shade is the
  count's place between 1 and the level's maximum, quantised.
- `_scistack_rug_color(base_hex, shade, shades)`: mixes white into the level's
  colour, from a light 25% up to 100%.

`lo`/`hi` are the figure's x range (`univariate_x_range`), so every panel uses
the same slots.

Readers:

- `reduce` attaches `Panel.rug` (`__x`, `__count`, `__shade`, the colour
  column) and logs one INFO line per figure with each level's maximum count.
- `mpl` draws `vlines` with `ax.get_xaxis_transform()`.
- `plotly` uses `yref "y domain"` shapes.
- codegen emits the same source and `vlines`.
- capability: `sample_overlay` becomes available on HISTOGRAM/KDE with its own
  reason text, and the join and sample-colour controls are hidden (they don't
  apply).

## Stages

1. `RUG_SOURCE` + unit tests (slots, counts, shading, the identical-values
   case, the single-value-bar case).
2. `roles.overlay_unavailable` / `OVERLAY_KINDS`: allow the univariate kinds,
   and make join and sample colour inert there. Reuse the overlay's sample
   rows (`sample_panels`). Attach `Panel.rug` in `reduce`, and log it.
3. Renderers (mpl, plotly), with parity tests (`tests/plot_geometry.py`
   read-back).
4. Codegen, with a test that the generated code draws the previewed ticks and
   shades.
5. GUI: the Show sample section on histogram/KDE (key checkboxes only, plus
   the scale note). Rebuild both bundles, add a manual-testing entry and docs.

## Status (2026-11-01)

Approved by the user as written (P1–P4).

DONE, untested, pyright-clean:
- `bins.RUG_SOURCE`, `rug_tables` and `rug_color`.
- `resolved.RUG_SHADE` / `RUG_ROW`, `Panel.rug`, `ResolvedPlot.rug_scale`, and the
  meta `rug`.
- `roles.overlay_is_rug`, with `overlay_unavailable` allowing the univariate kinds;
  `overlay_color` and `overlay_join` are inert on a rug.
- `ylimits` skips rug extents.
- `reduce._rug_panels`, which runs after the bins / curves and clears `Panel.sample`.
- `render.base.rug_rows`, `mpl._draw_rug` and `plotly_._rug_traces`.
- The capability `sample_overlay.form`.

All tests pass (user, 2026-11-01). DONE as well: codegen (`_rug_lines`, with `_sample_draw_lines` returning [] for a rug), tests (`tests/test_histogram_rug.py`), the GUI (rug note, inapplicable controls hidden), both bundles rebuilt (669/669 node tests), docs and the manual-testing entry (§0zzzzzzw). The Python tests have not been run yet:

    cd scistackplot && pytest tests/test_histogram_rug.py tests/test_histogram_special_bins.py tests/test_kde.py tests/test_histogram.py -q
