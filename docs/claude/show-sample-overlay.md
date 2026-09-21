# "Show sample" — the collapsed keys' data drawn on top of the marks

**Since:** 2026-09-19 (plan: `.claude/plan-show-sample.md`). Generalises the
spaghetti idea to every summative categorical kind — and, since 2026-09-21,
to the spaghetti itself (plan: `.claude/plan-colour-is-paint-and-spaghetti-sample.md`).

## On a spaghetti (since 2026-09-21)

A spaghetti's mark is a point on a LINE, shifted from its tick by
`ResolvedPlot.series_offsets`. "Show sample" draws the trials / cycles behind
each point ON that line: `reduce._overlay_frame` adds `__line`
(`resolved.SAMPLE_LINE`) — the marks' own series id recomposed on the
overlay rows (`GroupingLayers.identity`: the lines layer, plus a collapsed
subject drawn one line each) — and `render.base.sample_positions` places each
row at tick + `series_offsets[__line]` + its identity offset, with the
identity offsets scaled to the number of lines (`overlay_offsets(ids,
n_lines)`) so one subject's trials stay inside that subject's band. The join
rule counts the lines layer as a grouping layer (`overlay_join` reads
`ticks + series`), so trials under session ticks are points. Codegen mirrors
it: `_sample["_line"]`, the preamble's `_offset`, `_n_slots = len(_ids)`.
`test_show_sample_spaghetti.py`.

## What it is

A bar, box or violin (and scatter / strip, which draw the sample itself since
2026-09-19, so showing the sample key adds nothing and showing a deeper key
does) averages its collapsed keys away through the collapse chain
(`roles.collapse_order`, deepest first, nested — see
`grouping-and-collapse.md`). "Show sample" **stops that chain early** and
draws what is left as small points inside each mark — the distribution behind
a bar for publication, the raw data behind it for validation. The marks and
their error bars do not change.

Schema `[subject, session, speed, trial, cycle]`, bar, one figure per speed,
bars per session, `subject / trial / cycle` collapsed:

| `show_sample` | each point is | how it is computed |
|---|---|---|
| `[]` | — | today's figure |
| `["subject"]` | one subject's mean | cycles averaged within trials, trials within subjects |
| `["trial"]` | one trial (of one subject) | cycles averaged within each trial |
| `["cycle"]` | one raw cycle | nothing averaged |

**A deeper key implies the shallower collapsed keys.** A trial is a trial
*of* a subject, so `["trial"]` and `["subject", "trial"]` draw the same
overlay; the GUI shows the implied box ticked and locked. Checked names that
are not collapsed right now are **inert** (ignored, listed in the report),
the same contract as `cell_statistic` on a line — the spec never adjudicates
a checkbox state. A name that is no factor at all is refused by `validate`.

## The join rule (automatic, overridable)

`PlotSpec.join_sample: bool | None`, `None` = automatic. The rule compares
hierarchy depths (`LongTable.factor_depths`): when the **deepest shown key**
sits *above* the **deepest grouping layer** (ticks and colour alike), each
point identity recurs at every x position — a subject has a value at every
session — so the points are repeated measures and are joined into a line.
At or below the layers (a trial belongs to one session) nothing is joined. A
key with no depth on either side (a field, a derived bucket, the Variant
axis) cannot be placed and the rule declines: points, with the reason.

A line joins the points sharing every shown key's value (plus the colour
level) across the x positions of one panel — the spaghetti series rule. A
point with no partner has no line. Forced on below the x depth, the same
rule applies (trial 1 of subject 01 in pre and post get joined).

## The overlay's own colour (`PlotSpec.sample_color`, since 2026-09-21)

Bars coloured by intervention group, one colour per **subject** on top:
`sample_color` names a *shown* key whose levels colour the overlay points
and the lines joining them, independently of the marks' colour. `None` =
the mark's colour, as before. Same contract as a `show_sample` entry: a
name that is no factor is refused by `validate`; a factor not shown right
now is **inert** (kept, reported under `sample_overlay.color`) —
`roles.overlay_color(spec, steps)` is the ONE reader, and `reduce`,
`codegen` and `capability` all go through it.

* **Two columns, not one.** `Panel.sample` carries `__color` (the MARK's
  level — what paints a point without an own colour) and
  `__sample_color` (the point's own level). Independence is structural: a
  shown key is collapsed, a coloured layer groups, no factor is both.
* **Palette:** `render.base.SAMPLE_PALETTE` (tab20, twenty entries — a
  subject key has more levels than a grouping layer), indexed by
  `ResolvedPlot.sample_color_order` — the key's declared level order across
  the whole figure, decided once in `reduce` so subject 03 is the same
  colour in every panel (`sample_palette_for`, never a panel's own
  enumeration — the same rule as `palette_for`). Past twenty the colours
  repeat and `reduce` WARNs.
* **Lines cross the marks' colours.** `render.base.sample_groups` is the
  one rule: with an own colour the rows split by `__sample_color` and an
  identity's line runs across the colour levels — from the `pre` tick to
  the `post` tick — each point placed on ITS OWN row's mark
  (`sample_positions` is per-row). Without it the rows split per mark
  colour as they always did, because a line crossing two mark colours has
  no colour to be. This is why Lines / Auto (lines) drew nothing with
  `session` as the coloured layer: every run was one row long.
* **Legend:** the levels become a second block in the one legend, the way
  the dash styles are (`mpl._sample_legend_handles`, plotly `sample:<level>`
  legend groups with `legendrank` 2000, title `session / subject`);
  `shows_legend` counts them. Clicking a subject in plotly hides its points
  and lines in every panel.
* **Export:** `codegen._sample_draw_lines` emits `_sample_palette` over
  `_in_order(_sample[key], declared)`, groups the runs by identity alone
  (not by hue) and appends the levels to seaborn's legend
  (`_sample_legend_lines`, `Line2D` handles, one merged legend).
* **The Auto rule was already right** for a depth-less grouping layer
  (`Demographics.InterventionGroup`): `overlay_join` compares the shown key
  against the layers that HAVE a depth and ignores the rest, so `subject`
  above `session` → lines whichever layer is coloured
  (`test_sample_color.py::test_auto_join_ignores_a_depth_less_grouping_layer`).
* GUI: **Colour points by** under *Join points* (`showSample.ts`:
  `sampleColorChoice` / `sampleColorSetting`, `MARK_COLOR = ""`); the
  report's `color.options` are the shown keys.

## Placement: no random jitter anywhere

A line must end on its own markers, so every overlay point keeps **one
deterministic offset**: `spaghetti.overlay_offsets(ids, n_slots=1)` =
`series_offsets` (natural-sorted, evenly spaced over ±0.2, inside a
0.8-wide mark); on a spaghetti `n_slots` is the number of lines. Decided
once per **figure** (`ResolvedPlot.sample_offsets`), so a subject sits in
the same place in every panel. Hundreds of cycle points fill the mark evenly
and read as a strip.

Position = tick index (+ the line's `series_offsets` shift on a spaghetti) +
the identity's offset. There is no dodge slot: colour is paint
(`grouping-and-collapse.md`), every colour level has its own tick.
`render.base.sample_positions` is the one arithmetic;
`test_show_sample_render.py::test_both_backends_place_every_point_at_the_same_x`
and `test_show_sample_codegen.py::test_generated_points_land_where_the_preview_draws_them`
hold the three readers to it.

## Where things live

| question | owner |
|---|---|
| availability (kind ∈ `OVERLAY_KINDS`, categorical x, scalar drawn shape, something collapsed) | `roles.overlay_unavailable` — shared by `validate`, `reduce`, `ylimits`, `codegen`, `capability` |
| the cut (`averaged`, `shown`) | `roles.overlay_steps` → `OverlaySteps` |
| join decision + reason | `roles.overlay_join` → `OverlayJoin` |
| the hint sentence | `roles.overlay_granularity` |
| the rows | `reduce._build_figure`: `sample_overlay` phase runs `_collapse_levels(frame, averaged, pooled=…)` on the figure's rows BEFORE the marks' chain; `sample_panels` splits by facet → `Panel.sample` (`__x`, `__y`, `__color`, `__series`, the shown key columns) |
| offsets | `spaghetti.overlay_offsets` → `reduce._overlay_offsets` → `ResolvedPlot.sample_offsets` |
| y limits | `ExtentMode.overlay` (memo key) + `ylimits._reduced_extents` unions the overlay's raw extents into every return path |
| mark width | `render.base.MARK_SPAN` / `mark_width` (bars, boxes; plotly traces share `MARK_OFFSET_GROUP`) |
| a spaghetti point's line | `resolved.SAMPLE_LINE`, `reduce._overlay_frame(line_layers=…)`, `render.base.sample_positions`, `codegen._line_layers` |
| drawing | `mpl._draw_sample`; `plotly_._sample_traces` (positional axis, see below) |
| export | `codegen._sample_preamble_lines` (before the emitted chain) + `codegen._sample_draw_lines` (after the `catplot`) |
| report | `capability.sample_overlay_summary` → `sample_overlay {available, reason, factors[{name, checked, shown}], ignored, shown, averaged, join, granularity}` |
| GUI | `PlotStudio.tsx` "Show sample" section; `showSample.ts` (toggle, join choice, ticked/locked) |
| the overlay's own colour | `roles.overlay_color` (active?), `resolved.SAMPLE_COLOR`, `reduce._overlay_frame` (column) + `sample_color_order`, `render.base.sample_groups` / `sample_paint` / `sample_palette_for` / `sample_legend_levels`, `codegen._sample_color_of` / `_sample_legend_lines`, `capability … color {setting, active, options}` |

## Traps

* **plotly draws the whole figure on a positional axis** once an overlay is
  present (`_positional_x`: spaghetti *or* an overlay, on a categorical x).
  The marks are placed by index and get `_level_hover` so hover names the
  level, not the index; `barmode` / `boxmode: "group"` with one shared `offsetgroup`, one mark per position.
  Box/violin hover on a positional figure shows the index — a known
  limitation; the coloured-box alignment was never visually checked.
* **`Weight by N` pools the overlay too** — `_collapse_levels(pooled=True)`
  drops the averaged keys in one groupby, or the points would sit around a
  centre they were never averaged into.
* **The plan cache keys on `show_sample`** (it is not in
  `_PLAN_IRRELEVANT_FIELDS`), so ticking a box rebuilds the plan; the limits
  memo keys on `ExtentMode.overlay` as well.
* **Limits only ever widen.** A box's limits already span the sample rows,
  so showing subjects on a box changes nothing; showing raw trials moves the
  ends only where a raw value lies outside the means.
* **Flat-x export order** (found on the way, not fixed): for a *flat*
  categorical x the export lets seaborn order the ticks by appearance after
  the emitted groupby (alphabetical) while the preview uses the declared
  level order; nested axes emit `order=`. The codegen tests use `s1`/`s2`
  session names to sidestep it.
* **Spaghetti is now a special case**: bar/box + show `subject` + auto-join
  draws what `PlotKind.SPAGHETTI` draws, with a summary behind it. Whether
  SPAGHETTI stays a kind is an open clean-break decision.
* **`tsconfig.test.json` has an explicit include list.** A renamed module
  silently drops out of `npm test` (this happened to `groups.test.ts` after
  the `xLayers` rename; fixed 2026-09-19).
