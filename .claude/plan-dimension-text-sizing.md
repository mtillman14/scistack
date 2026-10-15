# Plan: ratio-based auto text sizing (replace the area rule and the per-element search)

Status: FINAL DRAFT for approval (2026-10-14). Nothing implemented.

## Goals (user, 2026-10-14)

1. Panels take up as much of the canvas as possible (the canvas = the saved
   size, or the pane size in Fit pane). Panel sizing is unchanged.
2. Text is as large as possible within its floor/ceiling, without text running
   into text, keeping the user's intended RELATIVE sizes.

Supersedes ADR D-2026-10-06-2's per-element search ("a long x title must not
shrink the y ticks"): one failing rule anywhere steps ALL auto text down
together, so proportions hold. Record as a new ADR in docs/claude/decisions.md.

## The model (user decisions 2026-10-14: option 3, 0.5 pt steps)

- **Reference size**: one number. Auto searches it; a fixed `TextSizes.base`
  fixes it (manual = anything, no clamping).
- **Intended ratios**, stored, never altered by sizing. ONE owner:
  `DEFAULT_TEXT_RATIOS` in `scistackplot/textsize.py`; a plot's overrides in
  `TextSizes.ratios: dict[str, float]`. Replaces `textsize.MEDIUM/LARGE/SMALL`
  and `autosize.CEILING_RATIOS`, so a fixed base and auto use the same ratios.
- **Per-element band in pt** (option 3): each element's floor and ceiling
  default to the target's band × its ratio; every value user-editable in pt
  (`TextSizes.floors` / `ceilings: dict[str, float]`). Defaults live in the
  same table as the ratios.
- **Drawn size** (derived per layout, never written back):
  `clamp(reference × ratio, element floor, element ceiling)`. A clamp changes
  what is drawn, never the stored ratio.
- A typed pt size for one element stays fixed and is outside the scaling
  (as today's pins).

Default table (user will tune; one place):

| Element | Ratio | Print band (8-12 × ratio) | Slide band (14-28 × ratio) |
|---|---|---|---|
| x_ticks, y_ticks | 1.0 | 8-12 | 14-28 |
| legend | 1.0 | 8-12 | 14-28 |
| groups (bracket rows) | 1.0 | 8-12 | 14-28 |
| differences | 1.0 | 8-12 | 14-28 |
| x_label, y_label | 1.2 | 9.6-14.4 | 16.8-33.6 |
| legend_title | 1.2 | 9.6-14.4 | 16.8-33.6 |
| title | 1.44 | 11.52-17.28 | 20.16-40.32 |

With the defaults the reference runs exactly over the target band (print
12 -> 8), and nothing clamps; clamps only bite once the user edits a ratio or
a pt bound.

## The search (`scistackplot/autosize.py`, one owner as today)

Candidates: the reference from the largest value at which any element is
still below its ceiling, down to the smallest at which any element is still
above its floor, in 0.5 pt steps. Binary search (about 4-6 layouts; first
layout at the top, most figures stop there). A layout is clean when ALL rules
pass:

1. No text collides with text: categorical x tick fit (no shrink/thin/rotate
   unless the reference is at the bottom); NEW numeric x and numeric y
   drawn-label collisions; neighbour-panel overlap (`GridReach`); bracket
   rows; legend fit (no moving below unless at the bottom).
2. No text off the canvas (drawn labels only, `mpl.drawn_ticks`).
3. Too tall: every panel shows at least 3 y tick labels without collision.
4. Too wide: every numeric x axis shows at least 3 x tick labels without
   collision (categorical x: rule 1).
5. NEW: a facet y title no longer than its panel's height.

At the bottom with rules still failing: draw the bottom, let the existing
fitting (rotate/thin/legend below) handle it, and log which rules still fail.

Removed: `MIN_DATA_FRACTION`, `LayoutReport.data_fraction`, `_data_fraction`,
`_Element` per-element search, `CEILING_RATIOS`, per-element blame,
`VERIFY_ROUNDS`. `LayoutReport.blame()` becomes `failures() -> tuple[str, ...]`.

## Stages

1. **Ratios + bands owner** (`textsize.py`, `spec.py`): `DEFAULT_TEXT_RATIOS`,
   per-element default bands, `TextSizes.ratios/floors/ceilings`,
   `drawn_sizes(reference, text)`; fixed `base` uses the ratios. Tests: drawn
   sizes keep ratios; clamping never changes stored ratios; fixed element
   unscaled; user pt bounds honoured; re-settle at a larger size restores
   proportions.
2. **New rules in the layout report** (`render/mpl.py`): numeric x/y
   collisions on `drawn_ticks`; >= 3 drawn y ticks per panel (and numeric x);
   facet y title vs panel height. Tests: numeric-x scatter with long labels;
   short 5x1 panels; long facet y title.
3. **One-dial search** (`autosize.py`): replace the per-element search;
   INFO line: reference, each element's drawn size (+ "clamped"), the rule
   that failed at the next step up; DEBUG per layout. Tests: 2x3 grid of 5
   panels at 8x6 and at 13.06 x 8.89 limited only by a named rule; 1x1 at
   8x6 at the top; layout count; rewrite tests/test_autosize.py.
4. **Preview + export parity**: preview copies the export's numeric x ticks
   (extend `YAxisText` to x); codegen bakes the drawn sizes (`_EXPORT_AUTO`);
   plotly preview applies them. Tests: preview == export sizes and ticks.
5. **Presets / saved plots**: `ratios/floors/ceilings` are settings, carried
   by presets (`presets.FIELD_CLASSES`) and saved plots; drawn sizes never
   stored. Beta: new fields default, no migration.
6. **GUI (Plot Studio Text section)**: per element, edit ratio, floor, ceiling
   (pt) and fixed size; show drawn size and "clamped" / limiting rule from
   `layout.meta.text_sizes.auto`. npm tests; rebuild BOTH vite targets; add a
   docs/gui-manual-testing-todo.md entry.
7. **Docs**: rewrite "Automatic size" in docs/claude/plot-text-and-labels.md;
   ADR; update memory.

## Not changing

Panel sizing, figure size/aspect, the fitting ladder itself, `drawn_ticks`,
`YAxisText` (y), the `print`/`slide` targets.
