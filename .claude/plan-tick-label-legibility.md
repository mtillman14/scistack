# Plan: legible x tick labels (auto-fit) + exact saved figure size

Status: approved 2026-09-23. Stages 1, 2 and the sample_in_legend checkbox done (tests pass). Stage 2b done (tests pass). Stage 3 built 2026-09-23: scistackplot.write_figure (figure_file.py) owns render-and-write, no bbox tight, canvas_overflow WARN; GUI plot_service uses it; readout says "exactly"; bundles rebuilt; pytest unrun. DEVIATION: kept tight_layout (not constrained layout) — the fitted layout already keeps content inside the canvas; revisit only if canvas_overflow WARNs appear. Generated-code savefig still uses bbox tight: stage 5. Stage 3 tests pass. Stage 4 done (tests pass). Stage 5 built 2026-09-23: StyleOptions.tick_rotation/tick_font_size/tick_every pins (LabelPolicy.pin_*, _fit_pinned), codegen _fitted_tick_lines (operations on seaborn labels) + _exact_size_layout_lines + script saves without bbox tight, GUI tick controls + hide-legend-labels checkbox + overlap notice; frontend 334/334, bundles rebuilt; pytest unrun. ALL STAGES BUILT. Decision 2026-09-23: nested axes have NO x title.

## Problem

1. Categorical x tick labels (and nested-axis bracket labels) overlap when there
   are many positions, long level names, or a narrow facet column.
2. The saved file is not the size the user asked for: the GUI saves with
   `savefig(..., bbox_inches="tight")` (scistack_gui/services/plot_service.py:1038),
   which trims or grows the canvas around whatever sticks out, including long
   labels and brackets drawn with `clip_on=False`.

## Evidence (spec/images/graph1.png, graph2.png — saved PNGs)

- graph1 (bar, groups=[session, InterventionGroup], colour=session, Show
  sample=subject): the five session tick labels under each bracket run together
  ("BMIPOSTM01FU…"). They are the COLOUR layer, so they repeat the legend.
  The x title "session / InterventionGroup" is drawn BETWEEN the ticks and
  the bracket row, and the bracket rule cuts through it.
- graph2 (2 facet columns, colour=InterventionGroup): ~15 session ticks per
  panel turn into an unreadable smear. The bracket labels "Digitimer Onward Sham" collide
  with each other. The two panels' x titles run into each other across the
  column gap. A 14-entry subject legend takes most of the figure width, which
  is what squeezes the panels in the first place.

Root causes found in code:
- R1 Brackets are placed at a fixed fraction of the axes height
  (`y = -0.10 - 0.07*rows`, mpl.py `_draw_x_groups`). On a short axes that's
  only a few points, so the brackets sit on the tick labels. They're also
  invisible to the axis title's placement, so the title lands on top of them.
- R2 Nothing measures tick, bracket or axis-title text against the room it has.
- R3 The legend is one column, and `MAX_LEGEND_FRACTION = 0.4` lets it take 40%
  of the width. A long Show-sample legend starves the panels.
- R4 Each facet column repeats the same x title and each is sized to its own
  panel, so titles wider than a panel collide with their neighbours.
- R5 `bbox_inches="tight"` then crops/grows the file around all of it.

## What the code does today

- `render/mpl.py::_apply_axes_cosmetics` forces `ax.tick_params(axis="x", rotation=0)`.
- `render/plotly_.py` forces `tickangle: 0` + `automargin: True`, and uses
  `tickmode: "array"`, which turns off plotly's own automatic label thinning.
- These are deliberate: the old rationale was "a figure must not read
  differently just because it gained a facet". This plan replaces that with
  ONE decision per figure, the same for every panel (see D1), so the goal
  still holds.
- Brackets (`_draw_x_groups` / `_add_x_groups`) use fixed offsets
  (`X_GROUP_ROW`) and are never checked for overlap.
- The mpl renderer uses `tight_layout(rect=...)` plus a legend-width hack
  (`_legend_width_fraction`).
- codegen emits seaborn code that does not rotate or resize labels.

## Feasibility verdict

| Target | Can it measure labels? | Verdict |
|---|---|---|
| matplotlib (save, for_each `plot_`) | Yes, exactly: `Text.get_window_extent(renderer)` after a draw | Robust. Measure, decide, apply, lay out again; settles in 1–2 passes |
| plotly preview | Not from Python (plotly.js measures in the browser). plotly.js `tickangle:"auto"` rotates only; it never wraps or shrinks, and category thinning is off under `tickmode:"array"` | Good enough by *estimating* widths with a per-character advance table. Borderline cases may differ from the save; log both |
| generated seaborn code | n/a | Emit the *resolved* decision as constants (rotation/fontsize/labels) |

It works for any figure because the decision depends only on the labels, the
room each one has (slot width), and the font. It doesn't care about plot kind
or data. The hard part is not measuring; it's that slot width depends on the
layout (legend width, y-title width, facet columns). matplotlib gets that for
free after a layout pass. The preview has to estimate it.

## Design (one owner: `scistackplot/ticklabels.py`, pure)

```
fit_labels(labels: list[str], slot_pt: float | list[float], font_pt: float,
           measure: Callable[[str, float], tuple[w, h]],
           policy: LabelPolicy) -> LabelFit
LabelFit: font_pt, rotation (0|45|90), lines: list[str] (with "\n" wraps),
          visible: list[bool], strip: (prefix, suffix), steps_taken: list[str]
```

The steps run from least destructive to most destructive. It stops at the
first step where nothing overlaps.
1. **Remove redundant text** (keeps meaning, always applied first):
   - DROP the shared PREFIX of NUMBERED labels only (every label wholly
     digits or ending in a digit run: `SS01..SS36` -> `01..36`); never a
     suffix, never names (`L_HAM`/`R_HAM` untouched) (user, 2026-09-23).
     One owner `ticklabels.is_numbered`. Cut only at a boundary; not moved
     into the axis title. Brackets and legend keep full names.
   - OPT-IN (`StyleOptions.hide_legend_ticks`, default off): hide tick labels
     of a layer that is also the colour layer while the legend is shown.
2. **Wrap** at natural breaks (`_ - space / .`) into at most 2 lines.
3. **Shrink the font** down to a floor: `max(policy.min_font_pt (default 8),
   0.7 * font_pt)`.
4. **Rotate 45°** (right-anchored, `rotation_mode="anchor"`). Rotated labels
   overlap only when slot < line_height / sin θ, so this almost always fits.
5. **Rotate 90°.**
6. **Thin out labels** (show every k-th): NUMBERED labels only (same
   `is_numbered` test as step 1; user 2026-09-23). First and last label of each
   bracket group always kept. Names stop at 90 degrees with fits=False (WARN).

The same function runs for each bracket row (slot = the span's width;
fallback: shrink, then wrap. Brackets are never rotated).

**One decision per figure.** Measure every visible panel. The most crowded
panel decides for all of them.

**User override:** `StyleOptions.x_labels: "auto" | LabelFit-like dict`
(rotation / font / every). A series of figures that must match can pin the
settings. The GUI "Figure" section shows the chosen result and lets the user
pin it.

## Beyond tick labels (from the evidence)

- **Brackets in points, below the measured tick labels (R1).** Offset each
  bracket row by N points from the bottom of the tick-label bbox (an offset
  transform), never by a fraction of the axes. Then set the x-title
  `labelpad` to clear the lowest bracket row. Bracket labels go through
  `fit_labels` (shrink, then wrap; never rotate).
- **One x title per figure when every column shares it (R4):** `fig.supxlabel`
  instead of one title per panel. Otherwise titles go through the same shrink/wrap.
- **Legend fitting (R3):** a legend on the right is as wide as its widest
  entry or title; adding columns only makes it WIDER. To narrow it: wrap
  the title at " / " (in graph2 "InterventionGroup / subject" is the widest
  line), shrink legend text to the same minimum as tick labels, shorten the
  line samples (`handlelength`), and a SETTING `PlotSpec.sample_in_legend` (built; default True; user
  2026-09-23) to leave the Show-sample block out of the legend. If it still takes more than a set share of the width, move
  it BELOW the panels, where multiple columns do help (they keep it short).
  Log the width it claims.
- **Redundant labels vs the legend:** opt-in only (Q4), see step 1.

## Measuring

- mpl: `measure` = real text extents from the Agg renderer.
  Slot width = axes width / number of positions, read AFTER a layout pass.
- plotly: `measure` = a per-character advance table for the export font
  (DejaVu Sans, matplotlib's default), with a 10% safety margin. The
  slot width is estimated for the EXPORT size (`style.width` in points,
  minus the estimated legend and y-margin, divided by columns and positions),
  so the preview shows what the save will do. This is consistent with
  font_size already being previewed at export scale. The plotly font family
  is set to DejaVu Sans first, so the estimate fits the preview's glyphs too.

## Exact saved size (layer fix, NOTE 3)

- Move saving into scistackplot: `save_figure(resolved, path, dpi)` becomes
  the one owner. The GUI and for_each call it; the GUI stops calling
  `savefig(bbox_inches="tight")` itself.
- mpl renderer switches from `tight_layout(rect=)` + `_legend_width_fraction`
  to `layout="constrained"` + `fig.legend(loc="outside right")`, and bracket
  texts stay in the layout (`in_layout=True`). Then the saved file is
  exactly width×height and the labels fit *inside* it.
- If content still doesn't fit (a huge legend), WARN with the size that
  would be needed. Don't silently grow the file.

## Logging (NOTE 2)

- INFO once per figure: `x labels: 24 positions, slot 31pt, widest 58pt →
  strip "Subject_", rotate 45°, font 11pt (floor 8)`.
- DEBUG: each step tried, with its worst overlap in pt.
- WARN: still overlapping after the last allowed step; preview estimate
  disagrees with the mpl measurement (both decisions logged, at save).
- INFO at save: requested vs actual pixel size of the written file.

## Tests (regression)

- Pure `fit_labels` tests from label lists + fake `measure` (each step,
  floor respected, nominal labels are never thinned, prefix strip keeps labels
  unique).
- mpl property test (works on any figure): after `render_matplotlib`, no two
  visible x tick label bboxes intersect, and no bracket label intersects
  another. Run it over a matrix: 5/20/60 positions × 1/3 facet columns ×
  short/long names × nested axis.
- Saved PNG pixel size == (width·dpi, height·dpi).
- Estimate-vs-measure test: the table-based width is within 10% of Agg for
  a sample label set (keeps the plotly estimate honest).
- codegen: emitted code carries the resolved rotation/fontsize.

## Stages

1. `ticklabels.py` pure module + tests (no renderer changes).
2. mpl: measure → fit → apply, figure-wide; brackets placed in points below the
   measured ticks + title labelpad; shared supxlabel; hide ticks that repeat the legend;
   logging; overlap property test (reproduce graph1/graph2 as fixtures).
2b. (graph2 at 8in cannot fit its session ticks until this lands — test_graph2_at_its_own_width_reports_that_it_cannot_fit pins that and must flip.) Legend fitting (wrap title, shrink text, shorter line samples, `PlotSpec.sample_in_legend` + GUI "Show in legend" checkbox in the Show sample pane (BUILT 2026-09-23; owner `roles.overlay_in_legend`; unticked = legend as though nothing shown), move below when too wide).
3. `save_figure` owner + constrained layout + exact size; GUI and for_each
   use it; size test.
4. plotly: char-advance estimator + export-size slot estimate; drop fixed
   `tickangle: 0`; preview/save disagreement log.
5. codegen emits the resolved decision; `StyleOptions.x_labels` override
   + GUI control; docs/gui-manual-testing-todo.md steps.

## Decisions (user, 2026-09-23)

- Q1: exact saved size. Constrained layout, no bbox tight.
- Q2: thinning allowed.
- Q3: GUI toggle "Preview at: export size | fit pane". Export-size mode fits
  labels at `style.width x style.height`. Fit-pane mode fits at the pane size and
  SHOWS the export size that would reproduce the view (pane px / 72, since
  the preview already reads font_size pt as px), with a "use this size" action.
  The toggle is a GUI view setting, not part of PlotSpec; the fitting function
  takes the size as input either way.
- Q4: hiding legend-duplicate ticks is opt-in.
- Prefix/suffix: dropped, not moved into the axis title.

## Stage 4 design (revised 2026-09-23, supersedes the char-table estimate)

The preview does NOT estimate text. It asks matplotlib for the export's own
decisions at the size being previewed and applies them to plotly:

- `scistackplot.layout_decisions(resolved, width_in, height_in)`: lays the
  figure out with render_matplotlib at that size and returns the tick fit, the
  bracket fit and the legend placement (the Figure attrs from stages 2/2b).
  ONE owner of every label/legend decision; the preview cannot drift from
  the export. Timed and logged (cost: one Agg layout of the downsampled plot).
- `render_plotly(resolved, decisions=..., fixed_size_px=...)` applies them:
  ticktext (stripped/wrapped/thinned), tickangle = -rotation, tick font,
  bracket label font/text, legend font/wrapped title/right-or-below + margins.
  Plotly font family "DejaVu Sans, Arial, sans-serif" (matplotlib's font).
- GUI toggle "Preview at: Export size | Fit pane" (view state, not PlotSpec):
  * Export size (default): decisions at style.width x height; the plotly
    figure is drawn at that size (1pt = 1px, the font_size convention), fixed,
    scrolling if larger than the pane.
  * Fit pane: decisions at pane px / 72 in; plotly autosizes; the Figure size
    section reads "this view would save as W x H in" + "Use this size".
- Resize / mode toggle re-request with `reuse_resolved=true`: plot_service
  keeps the last resolved figure per source and re-renders only. Used ONLY
  when the panel says the spec did not change (a resize), so the cache can
  never serve data the panel was not already showing; invalidate() drops it.
