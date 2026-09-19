# "Show sample" — the collapsed keys' data drawn on top of the marks

**Since:** 2026-09-19 (plan: `.claude/plan-show-sample.md`). Generalises the
spaghetti idea to every summative categorical kind.

## What it is

A bar, box or violin (and scatter / strip, which draw the sample's mean)
averages its collapsed keys away through the collapse chain
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

## Placement: no random jitter anywhere

A line must end on its own markers, so every overlay point keeps **one
deterministic offset**: `spaghetti.overlay_offsets(ids, n_colors)` =
`series_offsets` (natural-sorted, evenly spaced over ±0.2) divided by the
colour count, so the points stay inside their mark's dodge slot. Decided once
per **figure** (`ResolvedPlot.sample_offsets`), so a subject sits in the same
place in every panel. Hundreds of cycle points fill the slot evenly and read
as a strip.

Position = tick index + `dodge_offset(index, n)` (the mark's own slot, as
this panel's marks took them — `render.base.dodge_slots`) + the identity's
offset. `render.base.sample_positions` is the one arithmetic;
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
| dodge arithmetic | `render.base.dodge_width / dodge_offset / dodge_slots` (bars, boxes and points all read it) |
| drawing | `mpl._draw_sample`; `plotly_._sample_traces` (positional axis, see below) |
| export | `codegen._sample_preamble_lines` (before the emitted chain) + `codegen._sample_draw_lines` (after the `catplot`) |
| report | `capability.sample_overlay_summary` → `sample_overlay {available, reason, factors[{name, checked, shown}], ignored, shown, averaged, join, granularity}` |
| GUI | `PlotStudio.tsx` "Show sample" section; `showSample.ts` (toggle, join choice, ticked/locked) |

## Traps

* **plotly draws the whole figure on a positional axis** once an overlay is
  present (`_positional_x`: spaghetti *or* an overlay, on a categorical x).
  The marks are placed by index and get `_level_hover` so hover names the
  level, not the index; `barmode` / `boxmode: "group"` still dodge by trace.
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
