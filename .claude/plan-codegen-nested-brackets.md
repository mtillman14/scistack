# Plan: exported code draws the nested x axis the way Save does

## Problem (confirmed 2026-09-25)
With 2+ factors on x, `render_matplotlib` (Save) and the preview draw:
- ticks with the INNERMOST value only (`XPlan.tick_labels`, aliased),
- a blank spacer slot between groups (`XPlan.order` includes spacers),
- one bracket row per higher layer: a grey rule plus a centred label,
  fitted (`ticklabels.BRACKET_POLICY`), set a measured distance below the tick
  labels (`mpl._draw_x_groups`),
- no x title (`reduce.x_axis_title`),
- `hide_legend_ticks`: a row the legend repeats is blank, with no rules.

The generated seaborn code (`codegen.py`) instead composes `_x = "stim · pre"`,
orders by `_nested_x_order` (spacers DROPPED), and draws those joined keys as
ticks, with no brackets and no gaps. Its own docstring calls the gaps "the one
thing the export cannot reproduce". The tick fit is partial on a nested axis
("rotation, font and every-k only").

## Approach: replay the resolved plan, never re-derive it
`reduce.py` already says the plan is composed once "so codegen can replay the
result instead of re-deriving it". The generated function still cannot import
scistackplot, so the plan and the decisions are emitted as LITERALS, and a
small generated helper draws them.

0. **Greys into `paper.py`** (the part approved earlier): `PAPER.rule` (#888888)
   and `rule_width` (0.8), used by the mpl and plotly bracket rules and the mpl
   "no data" text; `PAPER.key_line` (#555555) for the dash-legend swatch in both
   renderers. The generated bracket helper reads the same values.
1. **Order with spacers:** emit `XPlan.order` mapped to composed `_x` keys,
   with spacers as unique blank categories. seaborn reserves an empty slot for
   an `order=` level that has no rows, which gives the export's gaps. Spaghetti
   (`_xpos`) and the Show-sample overlay (`_x_levels`) read the same list, so
   their positions line up with the bars.
2. **Tick labels:** `set_xticks(range(n))` and `set_xticklabels(plan.tick_labels)`,
   the aliased text `render_matplotlib` uses. The existing
   `_alias_relabel_lines` / `_fitted_tick_lines` then act on the leaf labels, so
   the nested-axis limitation ("rotation, font and every-k only") goes away.
3. **Brackets:** emit `groups` (label as fitted/wrapped, depth, start, end), the
   fitted bracket font (`layout_decisions()["brackets"]`), the hidden depths
   (`hide_legend_ticks`), and a generated `_draw_x_groups(fig, axes, ...)`. That
   helper mirrors `mpl._draw_x_groups`: it measures the tick-label depth at run
   time, then places one rule plus label per group per row, and sets the
   labelpad. The spacing constants (`X_GROUP_GAP_PT`, `X_GROUP_RULE_GAP_PT`,
   `X_GROUP_OVERHANG`) are emitted from mpl.py's own names, never retyped.
4. **Fan-out caveat, kept honest:** like today's `order=`, the plan comes from
   the first figure (the whole table where there is no fan-out). If a fan-out's
   figures have different combinations, the generated docstring says the plan
   is the first figure's. That is the same bargain as `_fitted_tick_lines`.

## Tests (NOTE 2)
- `tests/test_codegen_nested_x.py`: render_matplotlib vs executed generated
  code, read back through `plot_geometry` (`mpl_tick_labels`,
  `mpl_bracket_labels`, `mpl_marks`). The tick labels per position (spacers
  blank), the bracket labels, bar x positions, and bracket rule colour/width
  must be EQUAL. Cases: 2 layers bar, 3 layers box, colour = a layer plus
  `hide_legend_ticks`, spaghetti on a nested axis, Show sample on a nested axis.
- `test_paper_parity.py`: the bracket rules in mpl/plotly/codegen and the
  dash swatch read `PAPER.rule` / `PAPER.key_line`.
- Existing codegen tests that assert composed "a · b" tick labels will change
  on purpose; each change gets a note.

## Logging
The generated code gets a comment naming the replayed plan (layers, n
positions, n spacers, bracket font). codegen logs `generated code: nested x
replayed (N positions, M spacers, K bracket rows)` at debug level.

## Not in scope
Bracket placement is measured at run time in the generated figure, so rows sit
where that figure's tick labels end. That is the same rule Save uses, but on
seaborn's layout rather than render_matplotlib's.
