# Horizontal bar: BAR transposed

*Built 2026-10-26. Plan: `.claude/plan-horizontal-bar.md`. Tests:
`scistackplot/tests/test_horizontal_bar.py`.*

`PlotKind.HBAR` ("Horizontal bar + error" in Plot Studio) draws exactly the
vertical bar plot's data turned 90°: one bar per category, growing right from
zero, with a horizontal error bar.

## User decisions (2026-10-26)

- Nested grouping layers: **brackets at the left** — the innermost layer is
  the y tick labels, each outer layer a vertical rule + label in its own
  column further left.
- Category order: **first level at the top**.
- Faceted panels: the facet text (the panel's "y title") is drawn as the
  **value-axis title under each panel**; the category title stays at the left.
- **No difference bars** (`diffbars.HORIZONTAL_REASON`) — allowed by the user
  as a big lift for a horizontal geometry.

## The one rule: orientation is a drawing concern

Logically, "x" is still the CATEGORY axis and "y" the VALUE axis everywhere
upstream of the renderers: `Encoding`, `x_plan`, `x_order`, `y_limits`,
`PanelOverride.y_minimum`, compare, Show sample, Save data, presets, saved
plots, `StyleOptions.x_label` / `y_label` / `log_y`. Only drawing maps them
onto the screen.

| concept | owner | readers |
|---|---|---|
| "a bar" (summary, error columns, from-zero range, datetime refusal, fill) | `spec.BAR_KINDS` | reduce, ylimits, roles, capability, base.fill_alpha, codegen, both renderers |
| "draws centre ± error" | `spec.SUMMARY_KINDS` (= `BAR_KINDS` + BAND) | reduce, ylimits, codegen |
| orientation | `spec.HORIZONTAL_KINDS` / `spec.is_horizontal(kind)` | render.base, mpl, plotly, codegen, diffbars |
| which cells label the category / value axis | `render.base.shows_category_labels` / `shows_value_labels` | mpl, plotly |
| which side each logical axis is on (GUI wording) | `render.base.axis_sides` → `layout.meta.axis_sides` | Plot Studio (Labels, Y axis section) |
| screen letters in generated code | `codegen._screen_axes(spec)` → `(cat, val)` | `_plot_call` and its helpers |
| stacked-label fitting | `ticklabels.STACKED_TICK_POLICY` / `STACKED_BRACKET_POLICY` | `mpl._fit_category_labels` / `_draw_y_groups` |

`text sizes` stay **physical**: the category labels (on screen y) use
`y_ticks`, the value numbers use `x_ticks`; the category title is drawn at
`y_label`, the value title at `x_label`.

## matplotlib (`render/mpl.py`)

- `_draw_bars` → `barh(height=MARK_SPAN, xerr=…)`.
- `_draw_sample` swaps each point through a local `screen()`; placement is
  `base.sample_positions` unchanged.
- `plt.subplots(sharex=value-sharing, sharey=category-sharing)`.
- `_apply_horizontal_cosmetics` sets categories on y (`set_inverted(True)`,
  never `invert_yaxis()` — that toggles shared axes), value limits / log on x.
- `_fit_category_labels`: the category tick fit on the y axis, measured by
  label HEIGHTS (`_stacked_measure` returns `(h, w)`), positions as negated
  points so they ascend top-down. Recorded as the figure's `"ticks"` — the
  same key a vertical figure's x fit uses — so autosize, the preview and the
  exported code read it unchanged.
- `_draw_y_groups`: `BracketGeometry` read along x (its `row_height_pt` is the
  widest bracket label = one column's width).
- `_collect_x_ticks` and `_shared_x_title` skip horizontal kinds (the bottom
  axis is numeric, and its title names each faceted panel).

## plotly preview (`render/plotly_.py`)

- HBAR is always `_positional_x`: level indices on a linear y axis, range
  reversed `[n - 0.5, -0.5]`. That is what lets the export's fitted labels
  and drawn ticks (`AxisText`) copy across position for position.
- Bar trace: `orientation: "h"`, positions on `y`, values on `x`, `error_x`.
  Hover reads the value as `%{x}` (`_value_hover`).
- `_add_horizontal_axes`, `_add_y_groups` (annotations `xref: paper`,
  `xshift` < 0; rules `xsizemode: pixel`).
- `_apply_decisions` applies the `"ticks"` decision to `yaxis*`.
- Undecided margins: `_left_margin` estimates labels + bracket columns +
  title (`CHAR_WIDTH_EM`); decided, the export's `GridReach` alone.

## Exported code (`codegen.py`)

`sns.catplot(kind="bar", x=<measure>, y=<categories>, orient="h", …)`; then
every value-axis line on x: `xlim`, `sharex=False`, per-panel and override
limits (`set_xlim(left=, right=)`), `xscale`, the autoscale pad
(`autoscale_view(scaley=False)`), facet titles via `set_xlabel`; the alias
relabel and fitted-tick replay on y; the Show-sample overlay plots
`(value, position)`. `set_axis_labels` takes the titles in screen order.
Difference bars are never emitted. As for vertical nested axes, the export
labels a nested axis with composed keys (no bracket drawing in seaborn code).

## Known limits

- Plot Studio's Panels section still says "Y titles" for the per-panel
  names, which a horizontal bar draws at the bottom.
- `StyleOptions.log_x` / `tick_rotation` are ignored on a horizontal bar
  (its category axis takes neither).
- Only bars have a horizontal form; box/violin/strip would reuse
  `is_horizontal` + the stacked fitting if ever wanted.
