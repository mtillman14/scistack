# Plan: difference bars (significance brackets)

Status: DECISIONS MADE 2026-09-27 (D1 A, D6 A, D7 B, D8 A, D9-size B). Doc written at Stage 7.
Progress: Stage 1 PASSES 2026-09-27 (diffbars.py, XPlan.leaf_values, PlotSpec.difference_bars,
TextSizes.differences, capabilities["difference_bars"], reduce logs + plan-irrelevant; tests/test_difference_bars.py).
Change from plan: no `Panel.difference_pairs`; readers call `diffbars.bars_for_panel(resolved, panel)`.
Stage 2 PASSES 2026-09-27: diffbars `Obstacles` (segments + pt reach, numpy), `place_group` (skyline + fixed point,
pinned => unfit, top_floor for D7), `panel_obstacles`, `place_figure`; tests/test_difference_bars_placement.py
(whole-span level reservation fix: a bar sharing an end tick used to sit a gap above with a zero leg).
Stage 3 PASSES 2026-09-27: mpl `_draw_difference_bars` (measured frames + label boxes, 2 layout passes,
`DIFF_BARS_ATTR`, gids difference-bar/-label); tests/test_difference_bars_render.py (display-space clearance).
Stage 3b PASSES 2026-09-27 (user: "the cheap way"): `ResolvedPlot.difference_siblings` (reduce builds only
figures a bar names, same scoped ITERATE values); `diffbars.range_key` (scope values + typed ends) now also groups panels
in a figure; `sibling_floors` places siblings on THIS figure's frames by grid cell, real layout only when grid/tick count
differs (`mpl._sibling_placement`, memoised per draw); `place_figure(top_floors=)`. tests/test_difference_bars_siblings.py.
Known: two figures' synced tops can differ by a fraction of a point (each estimates the other on its own panels).
Stage 4 PASSES 2026-09-27: `layout_decisions()["difference_bars"]` (FigurePlacement); plotly `_difference_placement`
(decided = export's; undecided = `_estimated_panel_frames` from `_frame`/`_cell`), shapes `difference-bar:i:n` + annotations
`difference-label:i:n` (log10 on log axes, label text escaped), ranges from placement; `diffbars.difference_meta` ->
`layout.meta.difference_bars` (per panel match, xaxis/yaxis, targets with x0/x1 from `target_bounds`, bars, unresolved, unfit;
not_in_figure; estimated). `diffbars.carries_difference_bars` = renderers' guard. tests/test_difference_bars_preview.py.
For Stage 6: dark-mode recolour of PAPER.text shapes/annotations (plotTheme.screenFigure) and the Y axis/Panels readouts
still show pre-bar limits (they read panel.y_limits) — check.
Stages 5-7 PASS 2026-09-27 (np.float64 repr in baked literals fixed). Stage 5: codegen `_difference_bar_export` (resolve_one + render_mpl per figure a bar
names, `reduce.difference_bar_figures`) + `_difference_bar_lines` (`_diff_bars` by export category text, `_diff_ylims`,
`_diff_tops` by scope values; nothing emitted when no bar draws); tests/test_difference_bars_export.py. Stage 6: npm test
467/467, tsc clean, both vite targets built; `differenceBars.ts` (+test), `DifferenceBarsSection.tsx`, PlotStudio picking
(onClick/onHover, Esc, bands via PLOT_THEMES accent + opacity), screenFigure inks `difference-*`, textSizes `differences`,
statisticsSummary count; Y readout already reads the drawn plotly range; Panels placeholders still pre-bar (doc'd).
GUI §0zzi unchecked. Stage 7: docs/claude/difference-bars.md.

## Goal

On a panel, add bars that join two marks and carry a label ("*" by default),
the usual way a statistical test result is shown. The user adds them in Plot
Studio by clicking marks in the preview. Bars must never overlap each other,
their labels, or any mark (including "Show sample" points), and the y axis
must grow to hold them.

## What the code already gives us

- **A mark is a tick.** Since "colour is paint" (2026-09-21) every colour
  level has its own leaf x position (`render.base.MARK_SPAN`, nothing
  dodges). So on bar/box/violin/strip/scatter-on-categories the thing a user
  clicks (a bar, a box, a sample point) is always one leaf position, and
  "highlight the bar and all its samples" means "highlight that tick". On a
  spaghetti the tick holds every line's point; the endpoint is still the tick.
- Leaf identity: `xaxis.leaf_key(values)` joins the x layers' values;
  `ResolvedPlot.x_layers` names the layers; `x_plan.order` gives the slot
  (spacers included) of each leaf.
- Vertical reach of each mark, per tick, can be read off the resolved panel:
  bar = `max(0, __y_high or __y)`; box / violin = max raw `__y` (matplotlib's
  violin and plotly's `spanmode: "hard"` both stop at the data; whiskers and
  fliers never exceed the max value); scatter/strip/spaghetti = max `__y`;
  plus the panel's `sample` rows at that tick.
- The export measures and the preview draws (docs/claude/plot-panel-spacing.md):
  `mpl.render` lays out, `layout_decisions()` returns what it measured, and
  `plotly_._apply_decisions` applies it. A difference bar needs **points**
  (gap, line, label height) turned into **data units**, and that conversion
  needs the panel's measured height. So placement belongs on that same path.

## Design decisions

- **D1: what a bar names.** `DifferenceBar(match={panel factors as text},
  a={x layer: text}, b={x layer: text}, label="*")`. `match` is the panel's
  **ITERATE + FACET** values (all of `ResolvedPlot.panel_factors`), because a
  test result belongs to one set of data: "pre vs post differs for subject 01"
  says nothing about subject 02. (Contrast per-panel overrides, which match
  FACET only.) Text spelling is `panels.panel_key_text`, so `"01"` stays
  `"01"`. Endpoints are stored as `{layer: text}` dicts, not slot numbers, so
  a bar survives filters, reordering and new levels. A pair is unordered;
  `a == b` is refused.
- **D2: stored where.** `PlotSpec.difference_bars: list[DifferenceBar]`, a
  meaning field (it changes what is drawn). Not in `_PLAN_IRRELEVANT_FIELDS`'
  opposite: like `panel_overrides` it doesn't change the plan, so adding a bar
  only redraws. Round-trips through to_dict/from_dict/TOML/saved plots; a
  missing field is `[]` (no migration).
- **D3: inert bars.** A bar whose panel or either endpoint isn't in this
  figure is kept, drawn nowhere, logged, and listed in the GUI as "not in
  this figure" with a remove button (same as per-panel overrides' D3).
- **D4: where it's offered.** Only on a categorical x axis with a scalar kind
  (bar, box, violin, strip, scatter, spaghetti). Owner:
  `capability.difference_bars_unavailable(...) -> reason | None`, shipped in
  `capabilities`; the GUI shows the reason.
- **D5: placement (the math).** One owner, `scistackplot/diffbars.py`, pure:
  - `slot_tops(resolved, panel) -> {slot: top}` (data units), the reach
    rules above. Spacer and empty slots have no top.
  - `place(tops, pairs, *, bottom, top, panel_height_pt, sizes, log)`
    works in axis units (log10 on a log axis). With `u = H / (T - L)` points
    per unit:
    1. Order bars narrowest span first, then leftmost (the usual reading:
       short comparisons sit low, long ones arch over them).
    2. Keep a skyline `s[slot]`, initialised to the tops.
    3. A bar over slots `[i, j]` sits at `y = max(s[i..j]) + gap/u`: it clears
       every mark *between* its ends too, not just the two it joins. Each leg
       drops to `s[end] + gap/u`, i.e. to just above whatever is under that
       end (the mark, or a lower bar's label).
    4. Then `s[i..j] = y + (line + label_height + label_gap)/u`. Raising the
       whole span (not only the label's width) is what guarantees neither a
       later bar nor its legs touch the label. Inclusive ends mean two bars
       sharing an end slot never sit on one level (they would read as one
       long bar).
    5. Required top `T' = max(s) + pad/u`.
  - The scale depends on the top it produces, so solve by fixed point:
    `T(n+1) = max(T_data, T'(T(n)))`. The sequence only grows and converges
    while the bar stack's points are less than `H`; 50 iterations cap, WARN
    if not converged, WARN when bars take > 50% of the panel height ("the
    figure is too short for N bars").
  - Panels that share a y range in a figure are placed together: the shared
    top is the fixed point over all of them, so they keep sharing.
  - Sizes in points: gap 3, line = the figure's line weight, label = the
    NEW text-size element `differences` (user choice B), label height
    **measured** by matplotlib (the tallest of the labels actually used), pad 2.
    `TextSizes.differences: float | None` (None follows base × MEDIUM, like
    x_ticks), resolved by the one owner `textsize.resolve_sizes`; the GUI's
    Appearance › Text lists it with the other elements (from
    `layout.meta.text_sizes`, so only when the figure has bars or offers them).
- **D6: pinned top.** A typed Max (figure or panel) wins, as everywhere else
  (per-panel D2). Bars are placed on that scale; any bar above the pinned top
  is not drawn and a note under the section says "N bars do not fit under the
  Max you set" (plus WARN). Recommended over silently overriding the typed
  value.
- **D7: across a fan-out (user choice B).** Figures that share a y range
  keep sharing it: every figure in the scope group is raised to the top the
  bars need. Only figures that CARRY bars need a layout to know that (a
  figure with no bars needs nothing above its data), so the cost is one
  layout per figure-with-bars in the group, not one per figure:
  - `diffbars.group_top(scope group)`: for each figure in the group that has
    bars, resolve + lay out at the figure size, run the fixed point, take the
    max over them. Memoised on (plan key, spec fields that affect placement,
    size), so paging through figures computes it once.
  - Every figure in the group then uses that top. A figure with bars
    re-places at the shared top; this still fits, because above the fixed
    point `T ≥ T'(T)` keeps holding (each term grows by c/H < 1 per unit of T).
  - Per-panel limit overrides and a typed Max still win (D6).
  - INFO: `difference bars raised the shared top of N figure(s) to X (set by
    subject=01)`; timing logged, since this is the one place a figure's draw
    depends on other figures.
  - Test: two figures, "Same limits everywhere", bars only in figure 1 →
    both figures' drawn limits equal and hold figure 1's bars.
- **D8: export.** The generated seaborn code is standalone, so placement is
  **baked**: `_diff_bars = {figure/panel key: [(x_a, x_b, y, leg_a, leg_b,
  label_y, label)]}` and `_diff_ylim`, computed by `diffbars` at the export
  size and drawn with plain `ax.plot` / `ax.text`. A comment in the code says
  the positions were fitted to this data. Parity test: exec the code, compare
  every line and label with the preview's placement.
- **D9: label text.** Default "*"; editable per bar in the list ("**",
  "n.s.", "p = 0.03"). Placement measures whatever label is used.

## Pipeline placement (one owner, measured)

1. `reduce` resolves `difference_bars` for each panel into slot pairs
   (`diffbars.resolve_pairs`, logging applied / inert), attached to
   `Panel.difference_pairs`.
2. `mpl.render`, after `tight_layout` + `_fit_x_labels`: reads each axes'
   height in pt, calls `diffbars.place`, sets the ylim, draws the bars, runs
   `tight_layout` once more (tick numbers may widen), re-measures the height
   and re-places if it moved (DEBUG log of both). Stores `DIFF_BARS_ATTR`.
3. `layout_decisions()` returns `difference_bars`: per panel the drawn
   limits and each bar's coordinates in data units.
4. `plotly_` draws them as `shapes` (path, `xref/yref` of the panel) and
   `annotations` for the labels, and uses the decided limits for the panel's
   range. **Undecided fallback** (no decisions): estimate `H` from the style
   height and grid, same `place` function, DEBUG `difference bars (undecided)`.
5. `layout.meta.difference_bars`: per panel `{panel index, match, targets:
   [{slot, x0, x1, leaf key, a/b dict, label}], bars: [...drawn], unfit: N}`,
   plus `inert`.

## GUI (Plot Studio)

- New section **Statistics > Difference bars** (it changes what the figure
  says, so not Appearance). The Statistics group also opens when only this
  section is available.
  - Panel dropdown when there is more than one panel. **Clicking a mark in
    the preview also selects its panel.**
  - List of this panel's bars: "Pre ↔ Post", label box, ✕ (removes the
    setting, not data). Inert bars in a "not in this figure" group.
  - **+ Add difference bar** enters picking mode: "Click the first mark",
    then "Click the second mark"; Esc or Cancel leaves. Clicking the same
    tick twice does nothing; a pair already present is not added twice.
- **Click → tick:** plotly's `onClick` gives the trace's axes and the
  point's x. `differenceBars.ts` `targetAt(meta, axisRef, x)` returns the
  target whose `[x0, x1]` holds x (numeric positions, including sample and
  spaghetti offsets) or whose leaf key equals x (nested axes use category
  names). The bounds come from the backend.
- **Hover highlight (picking mode only):** `onHover` → the same lookup →
  a translucent band shape over that tick's `[x0, x1]` in that panel, full
  panel height (`yref: "y domain"`), so the bar/box and every sample point in
  it read as selected. The first picked tick keeps a stronger band until the
  second is picked. Colours via `var(--ps-*)` tokens (plotTheme test).
  Shapes are added to the view layout only; they're not part of the figure.
- Pure logic in `differenceBars.ts` (+ test, registered in
  `tsconfig.test.json`): `targetAt`, `addBar`, `removeBar`, `setLabel`,
  picking state machine. Logs `[Plot Studio] difference_bar_added/removed`.
- Update `docs/claude/plot-studio-controls.md`, `docs/gui-manual-testing-todo.md`
  (new §), `sidebarGroups.ts` Statistics summary. Rebuild BOTH vite targets.

## Stages

1. **Spec + owner:** `DifferenceBar`, `PlotSpec.difference_bars`,
   `restore` handling, `diffbars.resolve_pairs`, `capability`. Tests
   `tests/test_difference_bars.py`: round trip, `"01"` text, `a == b`
   refused, unordered duplicate WARN, inert logged, capability per kind.
2. **Placement math:** `slot_tops`, `place`, fixed point. Tests on the pure
   function: no two bar+label boxes intersect; no bar/leg/label crosses any
   slot's top (including slots *between* the ends); shared end slots stack;
   spacers; log axis; convergence; pinned top counts unfit bars; the
   too-short WARN. A small geometry checker in `tests/plot_geometry.py`.
3. **Export renderer:** `mpl` draw + measured label height + re-layout.
   Tests: measured in display coords, every bar's and label's window extent
   is above every mark's artist extent in its span and inside the axes; shared
   axes stay shared.
4. **Preview:** decisions + plotly shapes/annotations + undecided fallback +
   meta. Tests: preview coordinates equal the export's decisions; meta
   targets' `[x0, x1]` contain the sample offsets.
5. **Codegen:** baked replay (D8) + parity test.
6. **GUI:** section, picking, hover band, `differenceBars.ts` tests.
7. **Docs:** `docs/claude/difference-bars.md` (identity, math, owners,
   logs), controls map, manual-testing §.

## Logs

- INFO once per figure: `difference bars [subject=01]: 3 placed (muscle=SOL:
  pre–post y 4.1, ...), 1 inert (...), 0 unfit; top raised 3.8 → 5.2`.
- DEBUG: panel height pt used, iterations to converge, height before/after
  the second layout pass.
- WARN: not converged; bars > 50% of panel height; unfit under a pinned Max;
  duplicate pair.

## Not built

- Computing the test itself (the user supplies the result).
- Comparing two marks in different panels.
- Picking by keyboard.

## Commands for the user (tests)
```
cd /workspace/scistackplot && pytest tests/test_difference_bars.py -q
cd /workspace/scistackplot && pytest tests -q
```
Frontend tests I can run myself (node/npm available).
