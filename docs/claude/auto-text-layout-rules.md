# Auto text size: the layout rules

*Written 2026-10-14 with the one-dial rebuild (ADR D-2026-10-14-1, plan
`.claude/plan-dimension-text-sizing.md`). Tests pass (user, 2026-10-14). The
model itself (reference x ratio, clamped into per-element bands) is in
`plot-text-and-labels.md`, "Automatic size". This page is about the other
half: how a candidate reference is judged clean or not.*

## The shape of it

```
autosize.settle(resolved)
  └─ choose_reference(text, measure)          # pure: binary search over candidates
       └─ measure(reference)                  # = trial() in settle
            └─ render.mpl.layout_report(resolved at that reference)
                 ├─ render(...)                # the real export layout, fits and all
                 ├─ reads what render recorded: LabelFit (ticks, brackets),
                 │  legend steps, GridReach, canvas overflow
                 └─ _tick_and_title_rules(fig) # rules measured on drawn text
            → LayoutReport
       → LayoutReport.failures()  → () = clean
```

Larger text never makes a layout cleaner, so the clean references form a run
at the small end of the candidate list and a binary search finds the largest
one. The first probe is the top, so a figure that fits costs one layout.

Owners:

| Concept | Owner |
|---|---|
| Which rules exist, their thresholds, the words in the log | `autosize.LayoutReport.failures()` |
| Measuring the drawn-text rules | `render.mpl._tick_and_title_rules` |
| Which tick labels exist at all | `render.mpl.drawn_ticks` |
| Whether an axis is numeric | `render.mpl.is_numeric_axis` |
| What the export's fits decided | `ticklabels.fit_labels`, the legend fit, `GridReach` (unchanged) |

`LayoutReport` holds only measurements; `failures()` is the only place a
measurement becomes a verdict. A new rule is a new field filled by
`layout_report` plus one clause in `failures()`, and nothing else.

## The rules

| # | Rule | `LayoutReport` field(s) | Failure text (log / GUI) |
|---|---|---|---|
| 1a | Categorical x ticks need no shrink, thin or rotation, and fit | `tick_steps`, `ticks_fit` | `x ticks rotate 45°`, `x ticks thinned`, `x ticks overlap` |
| 1b | Bracket rows fit | `bracket_steps`, `brackets_fit` | `brackets shrink` |
| 1c | Legend stays at the right, unshrunk | `legend_steps` | `legend moves below`, `legend shrinks` |
| 1d | Numeric x / y drawn tick labels do not collide | `x_tick_collisions`, `y_tick_collisions` | `y tick labels collide in panel (1,0)` |
| 1e | No text runs into the neighbouring panel | `left_overlap_pt`, `below_overlap_pt` (`GridReach`) | `y-side text overlaps the next panel by 2.3pt` |
| 2 | No text off the canvas | `overflow_by_element`, `unattributed_overflow_in` | `x_label runs 0.20in off the canvas` |
| 3 | Too tall: every panel shows ≥ 3 y tick labels | `fewest_y_ticks` | `panel (4,0) shows 2 y tick label(s) (< 3)` |
| 4 | Too wide: every numeric x axis shows ≥ 3 x tick labels | `fewest_x_ticks` | `panel (0,0) shows 1 x tick label(s) (< 3)` |
| 5 | No y title taller than its panel | `y_title_overruns` | `y title of panel (0,0) is 12.0pt taller than the panel` |

Not failures (`HARMLESS_STEPS`): `strip_prefix`, `wrap`, `wrap_title`,
`short_handles`, `pinned`. Nothing a reader needs is lost.

### Tolerances

| Constant | Value | Used by |
|---|---|---|
| `autosize.OVERLAP_TOLERANCE_PT` | 0.5 pt | 1e (neighbour panels), 5 (title overrun) |
| `mpl.TICK_COLLISION_TOLERANCE_PT` | 0.5 pt | 1d (converted to px at the figure's dpi) |
| `autosize.OVERFLOW_TOLERANCE_IN` | 0.01 in | 2, the same as `figure_file.OVERFLOW_TOLERANCE_IN` |
| `autosize.MIN_TICK_LABELS` | 3 | 3, 4 |
| `autosize.STEP_PT` | 0.5 pt | candidate spacing |

## Why each measured rule is shaped the way it is

**Only drawn labels count.** `drawn_ticks(axis)` returns the major ticks whose
position lies inside the view interval, using matplotlib's own in-view test,
with a visible label. `axis.get_ticklabels()` also returns labels for the
locator's positions just past each end. Those are never drawn but still have
extents. Before 2026-10-14 they read as text running 0.43 in off the canvas and
held a single panel at the floor. Every rule here, the overflow measurement
and `AxisText` all go through `drawn_ticks`.

**Rules 3 and 4 exist because the locator reacts to the font.** matplotlib's
`AutoLocator` asks `Axis.get_tick_space()`, which divides the axis length by
a multiple of the tick label font size. A short panel at a big font gets
`nbins = 1` and draws two numbers, 0 and 1. Nothing collides, but the axis
can no longer be read. The count is of distinct drawn VALUES with non-empty
text, so `label1` and `label2` both drawn count once.

**Numeric versus categorical.** `is_numeric_axis` is false for a
`FixedLocator` (every `set_xticks`, which is how bar, box and heatmap
positions are placed) and for matplotlib's `StrCategoryLocator`. A
categorical x axis is judged by its fit (1a), which already handles shrink,
thin, rotate and wrap, and is never counted by rules 1d or 4. The y axis is
numeric for every kind except the heatmap.

**Switched-off labels are skipped, not counted as zero.** A shared y axis right
of the first column draws no numbers by design (`tick_params(labelleft=False)`).
If an axis has no drawn labels and no major tick label switched on
(`_shows_tick_labels`), it is left out. An axis with labels switched on but
none in view counts 0 and fails rule 3.

**Collision** sorts the drawn label boxes along the axis direction (x0 for x,
y0 for y) and fails if any neighbour overlaps by more than the tolerance.
Rotated or multi-line labels are measured as drawn.

**Rule 5** compares the y title's drawn extent height (a rotated title's
height is its length) with the axes box height. It applies to every visible
panel's y title, not only facet titles. A figure-level `supylabel` is covered
by rule 2 instead.

**Colorbars** (`ax.get_label() == "<colorbar>"`) are not panels and are skipped.

Panel names are `(row,col)` from `ax.get_subplotspec()`. An axes without
one is named `#index`.

## At the bottom

If even the bottom candidate fails, the search keeps it (`AutoTextSize.failing`
holds the rules that still fail), the existing fitting rotates, thins or moves
the legend below, and the INFO line says `at the bottom, still failing: …`.
The GUI shows the same text through `textSizes.autoSummary`. This is the only
case in which the fitting's destructive steps appear on an auto figure.

## Diagnostics

Everything is in `scistack.log`:

- INFO, once per settle: `auto text size: print at W x H in: reference Rpt of
  B-T (at the top | next step up fails: … | at the bottom, still failing: …);
  title=…, y_ticks=10 (clamped floor), …; N layout(s) in M ms`.
- DEBUG, per layout: `auto text size layout k: reference Rpt -> <failures | clean> in M ms`.
- DEBUG, per layout: `auto-size rules: n numeric axis/axes; fewest y ticks
  ('(4,0)', 2), fewest x ticks …; collisions x … y …; y title overruns …`.
- Each trial's own INFO and WARN lines (legend too tall, overlap) are demoted
  to DEBUG under `[auto-size trial reference Rpt]` (`Log.trial`). They describe
  layouts that were thrown away.

To find out why a figure is small, read the INFO line's "next step up fails",
then the DEBUG `layout k` line for the reference one step above the chosen
one.

## Tests

- `scistackplot/tests/test_autosize.py`, Part 1: the search with a fake
  `measure`, and every `failures()` clause. Part 2: real layouts (1-layout
  top, the 2x3 grid at 8x6 and 13.06x8.89, preview/save/codegen agreement).
- `scistackplot/tests/test_autosize_rules.py`: each measured rule on a raw
  matplotlib figure built to break it, plus the numeric-x preview parity.
- `scistackplot/tests/test_drawn_ticks.py`: `drawn_ticks`, phantom overflow,
  the preview copying the export's y text.

## Adding a rule

1. Measure it on the drawn figure in `render/mpl.py`. If it reads ticks, go
   through `drawn_ticks`. Return a field from `_tick_and_title_rules` or set
   it in `layout_report`.
2. Add the field to `LayoutReport` with a neutral default (an empty tuple or
   None), so the pure tests and fake layouts need no change.
3. Add one clause to `failures()` with words a user can act on, naming the panel.
4. Add a raw-figure test in `test_autosize_rules.py` and a `failures()` case
   in `test_autosize.py`.
5. Add the rule to the table above and to "Automatic size" in
   `plot-text-and-labels.md`.
