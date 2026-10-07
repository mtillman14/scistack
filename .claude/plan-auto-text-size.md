# Plan: automatic text size ("as big as fits, within publishable bounds")

Status: BUILT 2026-10-06, all stages. Redesigned mid-build to PER-ELEMENT sizing (user: a long x label must not shrink the y ticks), replacing the single-base search described under Design below. The build is documented in docs/claude/plot-text-and-labels.md "Automatic size"; ADR D-2026-10-06-2. npm tests pass and both bundles are rebuilt; pytest not run yet; GUI §0zzzb unchecked.

## Goal

A new plot picks its text size for the figure it is drawing: the LARGEST size
that still lays out cleanly, never below a legibility floor, never above a
ceiling. "Bigger is better" inside a band.

## Where we are today

- `TextSizes.base` is a fixed 14 pt (`spec.py`), whatever the figure size or
  content. Every other element is derived from `base` by matplotlib's ratios
  in `textsize.resolve_sizes` (the one owner of sizes).
- The saved file is exactly W x H (`write_figure`, no bbox tight), so a point
  in the spec is a point on paper at the stated size. That is what makes
  "publishable size" computable at all.
- Fitting is per element and only goes DOWN: `ticklabels.fit_labels`
  (strip -> wrap -> shrink to max(8, 0.7*font) -> rotate -> thin) and the
  legend fit (LEGEND_BUDGET 0.30, shrink, move below).
- The preview does not estimate text: `render.layout_decisions` (a real mpl
  layout) is the one owner, plotly applies it.

## Design

### Concept: "auto" is the unset state of `base`

`TextSizes.base: float | None = None`. `None` = auto; a number = pinned (today's
behaviour). Same vocabulary as every other TextSizes field ("None = derived").
Clean break per the beta rule: no migration; saved plots that stored 14.0 keep
14.0 (pinned), ones that did not become auto.

### One owner: `scistackplot/autosize.py`

`choose_base(resolved, width_in, height_in, policy, layout_fn) -> AutoSizeResult`

- Pure search; `layout_fn(base) -> LayoutReport` is injected (like
  `fit_labels`' measure), so the search is unit-testable without matplotlib.
- Candidates: ceiling down to floor in 0.5 pt steps, binary-searched (the
  constraints are monotone in base, near enough; verify with a test that scans
  linearly on fixtures and agrees).
- A candidate PASSES when the real mpl layout at that base needs no
  "destructive" fitting (Q3) and text does not eat the plot (Q4):
  1. x ticks: `LabelFit` used no shrink / no thin (rotate per Q3);
  2. brackets fit at their derived size;
  3. legend kept its derived size (no shrink); moving below allowed?
  4. no `GridReach` overlap, no `canvas_overflow`;
  5. data area (sum of axes boxes) >= MIN_DATA_FRACTION of the canvas.
- Nothing passes at the floor -> use the floor and let today's ladder handle
  the rest (it already reports overlaps).
- Result: `chosen`, `floor`, `ceiling`, `binding` (the constraint that stopped
  the next size up, e.g. "x ticks would rotate"), `tried` list, `ms`.

`resolve_sizes` stays the only place ratios are applied: autosize produces a
number, `resolve_sizes(style, auto_base=chosen)` does the rest. Pinned
elements stay pinned; auto only moves the unset ones.

### Where it runs

Inside the matplotlib layout path, once per (resolved plan, W, H, pins):
`layout_decisions` and `write_figure` both call it, so preview, saved file and
codegen agree. Codegen bakes the CHOSEN number into its `rc_params` literal
(export matches preview; the script does not re-search). Cache the result on
`(id(resolved), W, H, text spec)` so re-renders that change nothing cost zero.

### GUI

Font (pt) box empty = auto, placeholder `auto · 11.5 (x ticks)` from
`layout.meta.text_sizes.auto`. Typing a number pins it; clearing returns to
auto. The element grid placeholders already show resolved sizes, so they
update for free.

## Diagnostics (NOTE 2)

- INFO per resolve: `auto_text_size: 11.5 pt in [8, 16] at 7.2 x 4.05 in;
  14.0 failed (x ticks rotate), 12.0 failed (legend shrink); 3 layouts, 240 ms`.
- `layout.meta.text_sizes.auto` carries the same as JSON.
- DEBUG per candidate: the LayoutReport fields.
- Timing per layout so a slow search is visible before it is optimised.

## Tests

- `test_autosize.py` (pure, fake layout_fn): picks largest passing; clamps to
  ceiling; falls to floor; binary == linear scan; pinned base skips search.
- mpl: long tick names -> smaller than short names; 3.5 in figure -> smaller
  than 8 in; huge figure -> ceiling; legend-heavy figure binds on legend;
  data-area constraint binds on a tiny figure.
- Parity: codegen literal == chosen; `layout_decisions` and `write_figure`
  choose the same number.
- Guard: no module other than autosize/textsize computes a base.

## Stages

1. Pure `autosize.choose_base` + tests.
2. `LayoutReport` out of the mpl layout (most data already exists:
   `scistackplot_label_fit`, `scistackplot_legend`, GridReach, canvas_overflow)
   + wire into `layout_decisions`/`write_figure`, log, meta, cache. Measure ms.
3. `TextSizes.base` optional; resolve path; codegen literal.
4. GUI Font box auto state; manual-test doc entry.
5. Docs: plot-text-and-labels.md section, ADR.

## Decisions (user, 2026-10-06)

- Q1 LIVE. Auto re-fits on every size / data / text change until `base` is
  typed; clearing the box returns to auto.
- Q3 Rotation and the legend moving below both count as "too big". They are
  allowed only when the FLOOR still needs them: a candidate above the floor
  that rotates or moves the legend fails, so the search prefers a smaller
  unrotated size all the way down to the floor. Wrapping to 2 lines and
  stripping a numbered prefix are NOT failures (assumption, non-destructive).
  Shrink / thin are failures at every size.
- Q4 (my guess, user delegated): MIN_DATA_FRACTION = 0.55. The axes boxes'
  area must be >= 55% of the canvas. A constant in autosize.py, logged.

## Q2 proposal: the band follows the DESTINATION, not the width

A point size is absolute on paper because the file is exactly W x H. A
journal prints the figure at its stated width, so a fixed point band is right.
A slide is viewed from across a room, so it needs a higher band in points.
Deriving the band from the width guesses the destination: a 7.2 in figure
can be a double-column figure or half a slide. So the destination is stated,
not inferred.

- `TextSizes.target: Literal["print", "slide"] = "print"`. It sits inside
  `style.text`, which presets already classify as template (FIELD_CLASSES),
  so no new classification is needed.
- Bands, owned by `autosize.BANDS`:
  - print: floor 8, ceiling 12. 8 is the current `LabelPolicy.min_font_pt`;
    12 is the usual journal maximum.
  - slide: floor 14, ceiling 28. Slide figures are drawn at slide-scale
    widths such as 13.33 x 7.5 in.
- The floor applies to the SMALLEST derived element, not to `base`. The
  brackets are base x 0.833, so the print floor gives base >= 9.6 and the
  brackets >= 8. Otherwise "8 pt" would quietly mean 6.7 pt brackets.
- GUI: a Print / Slide toggle beside the Font box.
- Bands CONFIRMED (user 2026-10-06). No poster band.
- Picking Slide does NOT change the figure size. 13.33 x 7.5 in is only
  PowerPoint's default 16:9 slide; Google Slides is 10 x 5.625 in, and a figure
  rarely fills the whole slide. The band is honest only when the figure is
  placed at its saved size, so the GUI states that in the Slide tooltip.
