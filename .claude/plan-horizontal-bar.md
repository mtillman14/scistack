# Plan: horizontal bar kind (`PlotKind.HBAR`)

Requested 2026-10-26: "another builtin bar graph type, a horizontal bar graph,
with every feature the other built-in kinds have; difference bars may be
omitted if horizontal makes them a big lift."

## User decisions (2026-10-26)

- Nested grouping layers: **brackets at the LEFT** of the category tick
  labels (a vertical rule + label per outer layer, one column per layer).
- Category order: **first level at the top**.
- Faceted panel names: the facet text becomes the **value-axis (x) title
  under each panel**; the category title stays on the left.
- Difference bars: **omitted** for HBAR (inert, with a note), as allowed.

## Design: HBAR is BAR transposed

ONE data path. Logically, "x" stays the category axis and "y" the value
axis everywhere (encoding, `x_plan`, `x_order`, `y_limits`, compare,
Show sample, Save data, presets, saved plots). Only the DRAWING maps logical
axes to screen axes, and one owner states the mapping:

| concept | owner | readers |
|---|---|---|
| "a statistic per category with an error bar" | `spec.BAR_KINDS = (BAR, HBAR)` | reduce, ylimits, roles, capability, base.fill_alpha, codegen |
| "categories run up the screen" | `spec.HORIZONTAL_KINDS` / `spec.is_horizontal(kind)` | render.base, mpl, plotly, codegen |
| which panels label the category / value axis | `render.base.shows_category_labels` / `shows_value_labels` | mpl, plotly |
| difference bars inert on HBAR | `diffbars` reason constant | reduce, codegen, GUI |

## Stages

1. **Kind plumbing** — enum, `BAR_KINDS`, `HORIZONTAL_KINDS`; every
   data-level `kind is PlotKind.BAR` → `in BAR_KINDS`; capability offers HBAR
   wherever BAR; temporal refusal; grouping hint; difference bars inert.
2. **matplotlib** — `barh` with `xerr`; Show sample with swapped
   coordinates; cosmetics (value limits/log/title on x, categories on y,
   inverted so the first level is on top); category tick-label fit on the y
   axis (shrink/thin, measured on heights); nested brackets at the left;
   the numeric-x-axis machinery (`_axis_text`) reads the value axis.
3. **plotly preview** — `orientation: "h"`, `error_x`, category y axis
   (`categoryarray`, `autorange: "reversed"`), value range on x, brackets as
   shapes/annotations on the left, export decisions mapped to y axes,
   margins from the export's measured reach.
4. **codegen** — `sns.catplot(kind="bar", orient="h", x=<measure>,
   y=<categories>)`; every limit / scale / tick / bracket / overlay line
   restated on the swapped axis; no difference bars.
5. **GUI** — kind label "Horizontal bar + error"; summarizing check;
   Axes section names the value axis.
6. **Tests + docs** — `scistackplot/tests/test_horizontal_bar.py`,
   `docs/claude/horizontal-bar.md`, README, GUI manual-testing checklist.

## Diagnostics

`render_mpl` already logs the figure; HBAR adds one INFO line naming the
orientation and the category-label fit (`y category labels: font …, every
…`), and the bracket column widths at DEBUG, so a crowded horizontal axis
can be diagnosed from scidb.log.
