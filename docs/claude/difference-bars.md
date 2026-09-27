# Difference bars

A bar joining two marks of one panel, with a label over its middle ("*" by
default): how a statistical test's result is drawn. Written 2026-09-27. The
plan is `.claude/plan-difference-bars.md` (decisions D1-D9, with the user's
choices).

## The field

```python
PlotSpec.difference_bars: list[DifferenceBar]
TextSizes.differences: float | None      # the label's size; None = base

DifferenceBar(
    match={"group": "sham", "side": "R"},  # the panel: ITERATE + FACET values, as text
    a={"session": "pre"},                  # one end: {x layer: level text}, every x layer
    b={"session": "post"},                 # the other end (unordered)
    label="*",
)
```

## Identity (D1)

- **A mark is a tick.** Since colour is paint every mark has its own leaf x
  position (`render.base.MARK_SPAN`, nothing dodges), so the bar, box or sample
  point a user clicks is one tick, and a bar's end is a tick.
- **`match` names one figure's panel**: its ITERATE values and FACET values
  (`ResolvedPlot.panel_factors`), spelled by `panels.panel_key_text` (`"01"`
  stays `"01"`). A test result belongs to one set of data, so a bar for
  subject 01 is not drawn in subject 02's figure. Per-panel overrides match
  FACET values only; the difference is deliberate.
- **Ends are `{x layer: text}`, never slot numbers**, so a bar survives
  filters, reordering and new levels. A nested axis' leaf values come from
  `XPlan.leaf_values` (recorded where the axis is composed); nothing splits a
  leaf key. The ONE speller of an end is `diffbars.slot_endpoints`.
- **Inert (D3):** a bar whose panel is not in this figure, or whose end is not
  a tick (or has no mark in this panel), is kept, drawn nowhere and logged.
  `a == b`, ends naming different layers, or no end are `problem()`s: WARN,
  never drawn. The same pair twice (either order): drawn once, last label,
  WARN.

## Owners

| Concept | Owner | Consumers |
|---|---|---|
| what a bar names, which panel/ticks it lands on | `diffbars.bars_for_panel`, `slot_endpoints`, `not_in_figure` | reduce log, placement, meta |
| whether a spec/kind can carry bars | `diffbars.unavailable` (spec) / `carries_difference_bars` (drawn figure) | `capabilities`, both renderers, meta |
| what each mark reaches (the obstacles) | `diffbars.panel_obstacles` | placement |
| stacking + the axis top | `diffbars.place_group` / `place_figure` | mpl, plotly (undecided), codegen via mpl |
| which ranges are "in sync" | `diffbars.range_key` (y-scope values + typed ends) | `placement_groups`, `sibling_floors` |
| other figures' needs (D7) | `diffbars.sibling_floors` + `ResolvedPlot.difference_siblings` (built by `reduce`) | mpl, plotly (undecided) |
| measured panel boxes + label sizes | `mpl._draw_difference_bars` → `DIFF_BARS_ATTR` → `layout_decisions()["difference_bars"]` | plotly (decided), codegen |
| what the GUI shows / picks from | `diffbars.difference_meta` → `layout.meta.difference_bars` | `DifferenceBarsSection.tsx`, PlotStudio picking |
| editing the list, click → tick, bands | `differenceBars.ts` | `DifferenceBarsSection.tsx`, PlotStudio |

## Placement (D5): the math

Everything is in **axis units** (log10 on a log axis, where both backends draw
segments straight) and returned in data units.

1. **Obstacles** (`panel_obstacles`): every drawn thing as a segment
   `(x0, y0)–(x1, y1)` (a flat box when `y0 == y1`, a point when `x0 == x1`)
   plus a **reach in points** in every direction:
   - bar: ±`MARK_SPAN/2`, up to its error bar (from zero on a linear axis);
   - box / violin: ±`MARK_SPAN/2`, up to the highest value (matplotlib's
     violin and plotly's `spanmode: "hard"` stop at the data); a box adds its
     flier radius (`BOX_FLIER_PT`);
   - strip: ±`STRIP_JITTER`, plus the marker radius; scatter: the point;
   - spaghetti: every point at its line's offset (marker radius) and every
     segment of every run (half the line width) — a line sloping into a
     span is cleared where it crosses, not only at ticks;
   - "Show sample": every point at `sample_positions`, and joining segments.
   Arrays (`Obstacles`), because an overlay can be thousands of points.
2. **Scale:** `u = panel_height_pt / (top - bottom)` points per unit; x
   likewise from the panel width and x range.
3. **Stacking** (`_place_once`): narrowest span first, then leftmost. A
   bar's line sits `GAP_PT` (+ half its width) above the highest obstacle
   over its WHOLE span (ticks between its ends included); its label must
   also clear whatever is under the label's own width. Each leg drops to
   `GAP_PT` above what is under its end. Then the bar's whole level (line +
   label height across the full span, and the label's width) becomes an
   obstacle. Inclusive ends, so bars sharing a tick always stack (reserving
   only the label's width let the next bar sit a gap above with a zero-length
   leg — a staircase; fixed 2026-09-27).
4. **The top is a fixed point** (`place_group`): every offset is
   `c / u = c (T - bottom) / H`, so the needed top grows with `T`.
   `T(n+1) = max(T_data, need(T(n)))` rises monotonically and settles while
   the stack's points are fewer than the panel's; `MAX_ITERATIONS` caps it
   with a WARN ("did not settle"). `bar_fraction > CROWDED_FRACTION` WARNs.
   A flat range gets one unit of room (WARN).
5. **A typed Max wins (D6):** the top stays; bars whose label would pass it
   are `unfit`, not drawn, WARNed and listed in the GUI.
6. **Groups:** panels sharing one axis (`shares_y_axis`) are placed together;
   otherwise panels with the same `range_key` are.

## Keeping shared ranges in sync (D7, user choice B, "the cheap way")

Figures of a fan-out that draw the same range (same `range_key`) must end at
the same top when any of them has bars.

- `reduce` attaches `ResolvedPlot.difference_siblings`: the other figures a
  bar names (`figure_has_bars`) that share the scoped ITERATE values. Only
  those are built (`resolve_one`, cached plan); `resolve()` reuses its
  figures. A sibling's own list is empty, so nothing recurses. Not serialised,
  not compared.
- `sibling_floors` places each sibling's bars on THIS figure's measured frames,
  matched by grid cell (same size + grid ⇒ same panel heights, within a
  fraction of a point, which `PAD_PT` absorbs). Only a sibling whose grid or
  tick count differs is laid out for real (`mpl._sibling_placement`,
  memoised per draw). The tops become `top_floors` for `place_figure`; a
  figure without bars is raised to them.
- Known: two synced figures can differ by a fraction of a point, each having
  estimated the other on its own panels.

## Renderers

- **Export (mpl)** `_draw_difference_bars`, after every other layout pass and
  before `_grid_reach`: measures each label (`va="bottom"`, so the descent
  counts) and each panel's box, places, sets each panel's ylim, draws (gids
  `difference-bar` / `difference-label`, `PAPER.text`, zorder 4), lays out
  again and re-places once if a panel's height moved ≥ `DIFF_SETTLED_PT`.
- **Preview (plotly)** draws the export's placement from `layout_decisions`
  (never its own); shapes `difference-bar:<panel>:<n>` (a path: leg, line,
  leg) and annotations `difference-label:<panel>:<n>`, in log10 on a log axis,
  labels escaped (`p < 0.05` is text). Each panel's range is the placement's.
  **Undecided** (library callers): the same placement on panel sizes from
  plotly's own `_frame`/`_cell` and estimated label boxes; meta says
  `estimated`.
- **Dark mode:** `plotTheme.screenFigure` inks shapes/annotations named
  `difference-*`; light is the saved figure, untouched.

## Export code (D8)

The generated function sees one iteration's rows, so placement is baked:
`codegen._difference_bar_export` resolves each figure a bar names
(`reduce.difference_bar_figures`), renders it with the export renderer at the
saved size (siblings included), and writes `_diff_bars` (per panel key:
ticks by the EXPORT's category text + the preview's x as fallback, y, feet,
label y, label), `_diff_ylims` (panels placed on) and `_diff_tops` (shared
tops by y-scope values, for figures without bars). At run time a figure finds
itself by its ITERATE columns in `df` and each panel by `axes_dict`; x comes
from the order the code actually drew (`_x_order`, the nested literal — which
has no spacers — or spaghetti's `_order`). Nothing is emitted when no bar
draws, so the code is byte-identical. A comment says the positions are fitted
to this data. INFO `export difference bars: ...`.

## GUI

Statistics > Difference bars (`DifferenceBarsSection.tsx`; the Statistics
group also shows when only this is available). Panel dropdown; per bar the
ticks' names, a label box and ✕ (removes a setting, never data); "+ Add
difference bar" → `Picking` (`first` → `second` → idle) in PlotStudio:
plotly `onClick`/`onHover` → `panelForAxis` (the point's `xaxis._id`) →
`targetAt` (number: inside `[x0, x1]`, bounds from `diffbars.target_bounds`;
string: category text) → `pickStep` → `addBar`. A second click in another
panel restarts there; Esc cancels. Bands (`highlightShapes`, theme accent,
`opacity`, `yref: "<y> domain"`) are view-only. Console:
`[Plot Studio] difference_bar_picking_started / _pick / _added / _set /
_picking_cancelled`. Manual check: §0zzi in `docs/gui-manual-testing-todo.md`.

## Logs

- INFO per figure (reduce): `difference bars [group=sham]: N resolved (...),
  N unresolved (reasons), N not in this figure, N tick(s) to join`.
- WARN: `... can never be drawn (...)`; `... given more than once`.
- INFO (mpl): `difference bars placed: <panels>: N placed, N unfit, top A -> B
  (k iteration(s), bars x% of the height)[, raised to at least T for other
  figures]`; DEBUG `layout pass n, panel heights moved x pt`, panel heights
  and whether labels were measured.
- INFO: `built N sibling figure(s) that may share this figure's y range`;
  `figure X needs a top of T for a range this figure shares (estimated … |
  laid out …)`.
- WARN: not settled; crowded (> 50%); unfit under a typed Max; sibling with a
  different grid that could not be laid out; log range ≤ 0.
- DEBUG (plotly): `preview difference bars from the export` /
  `difference bars (undecided)`.

## Tests

`scistackplot/tests/test_difference_bars.py` (spec, resolver, capability),
`_placement.py` (pure math, checked in points by `violations`),
`_render.py` (export, read back in display pixels), `_siblings.py` (D7),
`_preview.py` (plotly + meta), `_export.py` (codegen parity).
Frontend: `differenceBars.test.ts`, plus `plotTheme` and `sidebarGroups`.

## Not built

- Running the test itself (the user supplies the result) — see "Next" below.
- A bar between marks in different panels.
- Per-mark picking inside one tick (a tick IS a mark since colour is paint).
- The Panels section's Min/Max placeholders still show the range computed
  before bars raise it (the Y axis readout shows the drawn range).

## Next: bars driven by statistical tests (assessed 2026-09-27)

The goal: the user picks a test, and the pairs it finds significant become
bars automatically. What carries over, and what has to change first.

**Carries over unchanged.** Everything downstream of the bar list is
source-agnostic: `place_group`/`place_figure`, both renderers, dark mode, the
GUI list and meta all take `DifferenceBar(match, a, b, label)` and never ask
where it came from. Labels are free text, measured as drawn ("***",
"p = 0.003"). Identity (a figure's panel + two ticks by level text) is how
pairwise results are keyed. Picking is separate from placement, so manual
bars can live beside derived ones.

**Must change (in this order):**

1. **Intent vs fact.** `spec.difference_bars` stores concrete pairs (facts).
   Test-driven bars must be DERIVED at resolve from a stored request, e.g.
   `PlotSpec.comparisons` (test, which pairs, correction, alpha, p→label
   rule); otherwise a data change leaves stale bars. Keep `difference_bars`
   for manual bars, give every bar an origin (`manual` / `test`), and let the
   user HIDE a derived bar (never delete). Same split as the project's
   intent/fact architecture.
2. **Where tests run.** A test needs the per-sample values at each tick, and
   for paired tests the unit identity (e.g. subject). Panel frames hold only
   centre/low/high, so the hook belongs in `reduce` at the collapse step,
   where `plot_data` and the "Show sample" overlay get their rows.
   Corrections are per family (panel or figure), so the hook takes a whole
   panel's pairs at once. `bars_for_panel` then merges derived + manual.
3. **Which layer runs the statistics.** scistackplot is standalone; scidb
   has `stat_` endpoints and the csv-stats vocabulary. Suggested seam: an
   interface "panel samples -> pairs with p-values", implemented in
   scistackplot and/or fed by a lineage-tracked `stat_` result. Owner
   undecided (user's call).
4. **Range sync (D7).** `figure_has_bars` reads the spec today (free). With
   derived bars, which figures have bars is known only after testing every
   figure that shares the range. Scalar tests on the plan's grouped rows are
   cheap next to layout: test the whole fan-out off the plan, then build and
   place only the figures with bars.
5. **Export (D8).** Baked literals are right for "export exactly this approved
   figure". A pipeline endpoint that should re-test on new data needs the
   generated code to call scistackplot (or carry the placement), not literals.
6. **Many pairs.** All pairs of 6 ticks is 15 bars; placement WARNs when
   crowded or unsettled, but automatic bars need a policy (adjacent only, a
   pair selection, or one "all differ" bracket).
