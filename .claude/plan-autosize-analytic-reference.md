# Plan: compute the auto text reference from one layout, not a search

Replaces `.claude/plan-autosize-infeasible-short-circuit.md` if approved.

## Idea

Text extents scale linearly with font size. So one layout can tell each rule
not only "pass/fail" but "the largest reference at which I would pass"
(its headroom). The reference = min over rules of that headroom, rounded down
to the 0.5 pt grid, clamped to the band. Panel geometry depends weakly and
monotonically on text size (smaller text -> bigger panels), so a reference
predicted from the top layout is conservative; the final render verifies it.

## Per-rule headroom (measured at reference r0 on layout L0)

| Rule | Headroom formula |
|---|---|
| 1a categorical ticks | r0 * slot_pt / widest_label_pt (upright, at r0's tick size) |
| 1b brackets | same, per bracket row |
| 1c legend stays right | r0 * room_right / legend_width |
| 1d numeric tick collisions | r0 * (gap + label) / label, per adjacent pair |
| 1e neighbour overlap | r0 * reach_room / reach |
| 2 off canvas | r0 * room / extent, per element |
| 3/4 >= 3 tick labels | from matplotlib's get_tick_space formula (axis length / fontsize * k): largest size that keeps nbins >= 3 |
| 5 y title overrun | r0 * panel_height / title_length |

Owner: `autosize.LayoutReport` gains `headroom()` next to `failures()`;
`render.mpl.layout_report` fills the extra raw measurements. No second owner.

## Algorithm

1. Lay out at the top (r0). If clean -> done (1 layout, as today).
2. r1 = floor_to_step(min_rule headroom). Lay out at r1 (this IS the final
   layout; reused, so no extra render).
3. If r1 is clean -> done (2 layouts). If not (hinting, wrap, locator step):
   step down 0.5 pt and repeat. Old binary search stays as the backstop.

## Rules no text size can satisfy

Headroom below the element floor (today's 105-tick plot: ~3.7 pt vs 8 pt
floor). DECISION NEEDED:
- (A) Leave it out of the min: other text keeps its size; fit_labels
  rotates/thins the ticks; it's reported as "x ticks cannot fit at any size".
- (B) Keep today's behaviour: everything goes to the bottom.

## Logging + tests

- INFO: `auto text size: reference Xpt from headroom (limit: <rule> Ypt);
  infeasible: [...]; N layout(s) in M ms`. DEBUG: every rule's headroom.
- Tests: headroom per rule on synthetic figures matches the search's answer
  (parity suite: run old search + new method across existing fixtures, assert
  same reference within 0.5 pt or the new one clean); 105-tick case -> 1–2
  layouts; fits-at-top -> 1 layout.
- Doc: `docs/claude/auto-text-layout-rules.md`.

---

# Critique (2026-11-03): the headroom plan as written is NOT recommended

Holes found (see chat for detail): "size ∝ extent" is false (pad_pt, tick
lengths, clamped elements, pinned sizes are constants -> affine/piecewise);
"narrowest slot x widest label" is wrong for uneven positions (bars per group
differ, "" spacers, group gaps) -> must be per neighbour pair / per bracket
span; harmless wrap is discontinuous and changes margins; the numeric locator
is a step function whose VALUES change label widths (non-monotone: breaks both
the headroom prediction and today's binary search); legend-below is a
threshold that rebuilds the layout; heatmap category labels are never fitted
and no rule sees them (`_collect_x_ticks` returns [] for HEATMAP); HBAR, custom
kinds, colorbars, in-panel text (diff bars, R²/p) and iterated figures each
need their own story. Two floors (autosize element floor vs LabelPolicy
font_floor) = two owners of "smallest tick font".

# Revised order

0. Diagnostics: sub-timers inside `_fit_x_labels` (each fit, each
   tight_layout, `_draw_x_groups`) and inside `_tick_and_title_rules`.
1. Cache the auto-size RESULT on what determines text geometry, not on
   id(resolved): identical requests (plan cache HIT 11:04:13, 11:04:36) and
   paint-only changes (colour, sample_color) re-ran the full 22 s search.
2. Feasibility pre-check from data (pure, no layout): per-pair need at the
   floor font vs distance -> rules no size can satisfy are known before any
   layout. Then decision A/B.
3. Cheaper trials: build the figure once, re-size text per trial (marks,
   subplots, colorbar unchanged). Measure first.
4. Only then consider headroom, and only as the FIRST GUESS of the search
   (verify, fall back), never as the answer.
5. Separate issue: heatmap/HBAR categorical labels need a fit + a rule.
