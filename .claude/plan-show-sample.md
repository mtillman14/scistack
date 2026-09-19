# Plan: "Show sample" — draw the collapsed keys' data on top of the plot

*Rewritten 2026-09-19 after the first draft over-generalised.*

## What it is

A summative kind (bar, box, violin; also scatter/strip, which draw the
sample's mean) averages its collapsed keys away deepest-first
(`roles.collapse_order`). "Show sample" stops that chain early and overlays
what is left as small points inside each mark — to see the underlying
distribution (publication) and the raw data (validation).

Example — schema `[subject, session, speed, trial, cycle]`, bar, one figure
per speed, bars per session, `subject / trial / cycle` collapsed:

| Show sample | each point is | how it is computed |
|---|---|---|
| nothing | — | today's figure |
| `subject` | one subject's mean | cycles averaged within trials, trials within subjects |
| `trial` | one trial (of one subject) | cycles averaged within each trial |
| `cycle` | one raw cycle | nothing averaged — the raw data |

Checking `cycle` shows trials and subjects too — they are the identity of a
cycle — so a deeper box implies the shallower ones (the GUI ticks and locks
them). Checking `trial` alone is the middle row. Nothing about the bars,
boxes or error bars changes.

**Rule (one owner, `roles.overlay_steps`):** the overlay chain is the collapse
chain cut before the deepest checked key. Pooled ("Weight by N") pools the
same keys in one groupby. Non-schema factors follow the depth they already
have in the chain (ColName inside a record, a factor variable at its key's
depth, a derived bucket last).

## Spec and rules (scistackplot; the GUI only edits and displays)

* `PlotSpec.show_sample: list[str]` — the checked collapsed keys; empty by
  default, so saved specs are unchanged.
* Available on `OVERLAY_KINDS = (BAR, BOX, VIOLIN, SCATTER, STRIP)` with a
  scalar drawn shape (a cell-collapsed 1-D measure counts) and ≥1 collapsed
  factor. Spaghetti / line / band / heatmap: not now.
* `validate`: each name must be a COLLAPSE factor present in the table;
  refused on other kinds; a kind switch that invalidates them clears them
  (`roles.with_requirements_for`, the existing auto-fix seam).
* `roles.overlay_granularity(...)` — the hint sentence: "one point per
  trial (cycles averaged)".
* Points: deterministic jitter inside the mark's dodge slot, the mark's
  colour with a darker edge, lower alpha, smaller marker; plotly hover names
  the point's keys. No legend entry.
* Y limits include the overlay (`ExtentMode` + `_reduced_extents` union).

## Stages

1. **spec + roles** — `show_sample`, round trip, `OVERLAY_KINDS`,
   `overlay_steps`, `overlay_granularity`, validation and clearing. Tests:
   the three table rows, pooled, non-schema factor depths, refusals.
2. **reduce + ylimits** — keep the pre-chain frame in `_build_figure`; run
   the overlay chain under a `sample_overlay` timing phase (DEBUG rows per
   step, points per panel); facet-split with `_ordered_groups`;
   `Panel.sample` frame (`__x` composed for a nested axis, `__y`, `__color`,
   plus the key columns for hover); `to_dict`. Limits union. Tests pin the
   worked numbers (subject 01 trials {1,2,3}, 02 {9}) for each row.
3. **renderers** — one dodge helper in `render.base` (arithmetic now
   duplicated in `_draw_bars` / `_draw_distribution`), reused by the
   overlay. mpl `_draw_sample_overlay`. plotly: invisible box trace with
   `boxpoints="all"`, `pointpos=0`, `jitter`, `offsetgroup` = colour level
   (plotly's own dodged-strip mechanism); positional-axis fallback if it
   does not line up with bars — first test of the stage.
4. **codegen** — `_sample` built in the preamble by restating the cut chain;
   `sns.stripplot(..., dodge=True, order=, hue_order=, ax=_ax)` per grid
   axes; exec-and-compare test (spaghetti offsets test as template).
5. **capability + GUI** — report key `sample_overlay {available, reason,
   factors: [{name, checked, implied}], granularity}`; PlotStudio "Show
   sample" section under Summary: one checkbox per collapsed factor in chain
   order, deeper ticks lock the shallower ones, hint sentence, greyed with
   reason when unavailable; TS test; both vite bundles rebuilt.
6. **docs** — `docs/claude/show-sample-overlay.md`, README paragraph,
   grouping-and-collapse owner table, memory.


## Joining the points into lines (automatic, overridable)

`PlotSpec.join_sample: bool | None` — `None` (default) means automatic.

**Rule (one owner, `roles.overlay_join(spec, roles, table)`):** compare the
depth of the **deepest checked key** with the depth of the **deepest
grouping layer** (ticks and colour alike; a factor variable carries its
key's depth). Shallower ⇒ the point identity recurs at every x position ⇒
repeated measures ⇒ join. Deeper, equal, or no depth (a derived bucket) ⇒ no
lines. The user can force either way; the report says which applied and why.

| Show sample | deepest shown vs `session` (x) | auto |
|---|---|---|
| `subject` | subject (1) above session (2) | lines, one per subject |
| `subject, trial` | trial (4) below session (2) | no lines (a trial belongs to one session) |
| `cycle` | cycle (5) below | no lines |

A line joins the points that share every checked key's value (plus the
colour level), across the x positions inside one panel — the spaghetti
series rule, panel-constant factors excluded. A point with no partner
simply has no line. When join is forced on below the x depth (trial 1 of
subject 01 in pre and post), the same rule applies: same key values ⇒
joined.

**Offsets, one mechanism for both cases:** a line must end on its own
markers, so overlay points never jitter randomly. Each point identity keeps
one deterministic offset inside its mark's dodge slot —
`spaghetti.series_offsets` scaled to the slot width, decided once per
figure (`ResolvedPlot.series_offsets`, as today). Hundreds of cycles simply
fill the slot evenly, which reads as a strip.

**Consequences for the stages:**

* Stage 1 adds `join_sample`, `overlay_join`, and the join sentence to the
  hint ("lines join each subject across sessions — repeated measures").
* Stage 2 puts the series id on `Panel.sample` (`__series`, composed
  outermost-first like everything else) and computes offsets per figure.
* Stage 3: plotly uses the **positional axis** (`_positional_x`, the
  spaghetti mechanism) whenever a figure carries an overlay — bars and boxes
  draw at integer positions on a linear axis — and the overlay is scatter
  traces in `markers` or `lines+markers` mode. This replaces the
  invisible-box idea: one mechanism, and lines need fractional x anyway.
  mpl: `ax.plot(marker="o")` per series, or `ax.scatter` when not joined.
* Stage 4: `_sample` gets `_xpos` from the spaghetti position rule
  (restated in plain pandas, already exists) and is drawn with
  `sns.lineplot(units="_series", estimator=None, marker="o")` or
  `sns.scatterplot` on each grid axes.
* Stage 5: a three-way control under the checkboxes — Auto (says what it
  chose and why) / Lines / Points.

**Overlay needs a categorical x:** scatter with `x_measure` is excluded
(no dodge slot to place points in).

**Follow-up to decide later, not in this plan:** bar/box + show `subject`
+ join now draws what `PlotKind.SPAGHETTI` draws, plus a summary. Whether
SPAGHETTI stays a kind is a separate (clean-break) decision.

## Out of scope (named)

Overlay on band (1-D); mean ± error
overlay on spaghetti (the inverse, same machinery after Stage 2).

## Status 2026-09-19

All 6 stages built; scistackplot, scistackplotdb and scistack-gui pytest
suites pass, frontend `npm test` 88/88, both vite bundles rebuilt.
Uncommitted. Not yet visually checked in the panel (coloured box + overlay
alignment on plotly's positional axis is the one to look at). Doc:
`docs/claude/show-sample-overlay.md`.
