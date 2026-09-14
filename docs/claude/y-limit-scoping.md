# Y-limit scoping

How `scistackplot` decides what range each panel's y axis spans, why the
decision is made off the **table** rather than off the figures, and where the
two renderers read it from.

Written 2026-09-11, alongside `.claude/plan-plot-save-and-ylimits.md` Stage 5.
Revised 2026-09-14 (`.claude/plan-y-limit-accuracy.md`) after "data is often
cut off vertically": see **The rule that keeps limits honest** and **A kind
switch on a cached plan** below — both were real clipping bugs.

## The control

One field, `PlotSpec.y_axis` (`spec.YAxis`):

```python
YAxis(scope: list[str], minimum: float | None, maximum: float | None)
```

`scope` names the factors that get their **own** limits. That single list
expresses the whole range of what a reader needs:

| `scope` | one range per… |
|---|---|
| `[]` | the whole dataset — every panel of every figure |
| `["subject"]` | subject; all that subject's facets share it |
| `["subject", "ColName"]` | subject **and** facet — per-panel autoscale |

`minimum`/`maximum` override independently, so "floor at zero, compute the
ceiling" is expressible. Both set means the data is never consulted.

It replaced `FacetOptions.share_y`, a boolean that could say only "the facets of
this figure share" or "they do not", with figures always scaled to themselves.
Neither end of what users actually ask for — compare everything on one scale;
show me the shape of this one panel — was reachable.

## The invariant: only panel-separating factors

`scope ⊆ (ITERATE ∪ FACET)`.

Those are the only roles that put data in *different panels*. A COLOR or FREE
factor lives inside one panel, so separating limits by it would ask a single
axis to hold two ranges — there is no figure that satisfies it.

`ylimits.eligible_scope` enforces this by **dropping** ineligible names with a
warning, not by raising. A scope is checkbox state, and a factor that was FACET
a moment ago and is COLOR now should quietly stop separating limits rather than
replace the figure with an error. The GUI only offers eligible factors, so this
guards specs arriving from TOML, from an older session, or from a role the user
has since changed.

## Why the limits come off the table, not the figures

This is the part that shapes the design.

The interactive panel builds **one figure at a time** — `reduce.resolve_one`
exists precisely so a 30-subject fan-out does not pay for 30 figures on every
control change. But a scope of `[]` means "one range across every figure",
including the 29 nobody asked for. Building them to find out would cost exactly
what the Stage 3 performance work removed.

So `ylimits.limits_by_scope` computes from the **table**, before any reduction,
in one pass for the entire fan-out:

- **Non-aggregating kinds** (line, scatter, strip, box, violin): the drawn
  extent *is* the raw extent. Per-cell `np.nanmin`/`nanmax` over the
  **unexploded** arrays — 24 numpy reductions over arrays that already exist,
  where exploding first would build 8.9 M rows to answer the same question.
- **Aggregating kinds** (BAND/BAR with an error band): the drawn extent is
  `centre ± spread`, which can sit outside the data. That genuinely needs the
  reduction, so it explodes — but only to take extremes: one groupby on
  `(scope…, x, colour, index)`, no panel frames, no series keys, no sorting.

There is ONE definition of an error band in pandas, `ylimits.spread_bounds`,
and `reduce._summarize` (the drawing) calls it; the numpy twin is
`series_stats.position_stats`, held to it by the parity suite. Two definitions
would put the limits and the drawing at odds, and the symptom would be a band
clipped by its own axis.

## The rule that keeps limits honest

**The statistic is computed at the granularity it is drawn at; the scope only
decides how the per-panel extents are combined.**

Until 2026-09-14 the aggregated path grouped by `scope + X + COLOR + index`.
With `subject` as FACET and `subject` *unticked*, that pooled every subject
into one mean ± SEM per position — a range that collapses around the grand
mean as n grows — and every subject's own band, drawn from its own rows, fell
outside it. The default scope ticks every panel factor, which is why it only
showed up "in more specific combinations of the checkboxes".

Now both reducers group by **every panel factor** (`ylimits.panel_factors`:
ITERATE ∪ FACET from the *completed* roles, so a promoted ancestor or a
defaulted facet counts) plus colour and position, apply the AGGREGATE collapse
first exactly as the drawing does (`NumpyReducer._panel_series` is shared by
`summarize_series` and `y_extents`), include the centre line (a MEAN with an
IQR band can sit outside its own quartiles), and only then project each panel
key onto the scope and take min/max. Sharing can therefore only *widen* a
panel's range — `test_unticking_a_factor_never_narrows_a_panel_below_its_own_range`.

The invariant every change must keep, parametrised over scope × kind × error
band in `tests/test_ylimits.py`: **nothing a panel draws lies outside its own
`y_limits`.**

Cost: the same samples through the same `position_stats`, with the group set
identical to what the default (all-ticked) scope already produced. Only the
grouping *keys* changed. The `[timing] y_extents(numpy)` line now names the
mode, and the `y limits (…)` INFO line says how many panels folded into how
many groups.

## A kind switch on a cached plan

`kind` is deliberately not in the plan cache key (a kind change must not
re-run variants, filters and the fan-out grouping) — but the limits depend on
it: a band draws `centre ± spread`, a line draws the observations. LINE → BAND
with the default MEAN ± SD was a cache HIT that drew the band inside the
line's limits; BAND → LINE drew the raw traces inside the band's.

`ylimits.ExtentMode` names what the limits depend on beyond the plan's rows
(summary / collapse / from-zero / log), and `_Plan.y_limits_by_mode` memoises
the limits per mode. `reduce._with_y_limits` attaches the right entry on every
plan-cache hit, computing a missing mode once (`y limits: MISS for mode (…)`
at INFO — the one moment a kind switch costs anything, ~the `y_limits` phase
rather than the plan). It is the single field of a cached plan written after
construction, only ever added to, under the cache lock.

The result lives on `_Plan.y_limits`, so `resolve` and `resolve_one` agree by
construction — a figure built alone gets the same numbers as the same figure
built as part of the set. `tests/test_ylimits.py` asserts exactly that.

## Where each layer reads it

```
PlotSpec.y_axis            what the user asked for
  └─ _Plan.y_limits        {scope values: (low, high)} for the whole fan-out
      └─ Panel.y_limits    THE AUTHORITY — what this panel draws
          └─ ResolvedPlot.y_limits   set only when every panel agrees, else None
```

`ResolvedPlot.y_limits` is **derived from the panels, never recomputed**, so the
figure-level number and the panel-level ones cannot disagree. Its `None` is
meaningful: it is how a renderer learns the panels differ.

## The derived axis sharing

`render.base.shares_y_axis(resolved)` — one rule, both renderers:

- `render/mpl.py` passes it as `plt.subplots(sharey=...)`;
- `render/plotly_.py` uses it to decide whether a panel's axis gets
  `"matches": "y"`.

Both mechanisms *tie axes together*, so with per-panel limits they would let
whichever panel drew last overwrite every other panel's range. The sharing is
therefore derived from the outcome, never configured directly.

It also gates `shows_y_labels`. Hiding inner panels' tick labels is only honest
when the hidden numbers would have been identical; a grid of independently
scaled panels labelled down the left column only *reads* as one shared scale,
which is the exact misread per-panel limits exist to avoid.

## Generated endpoints

A generated `plot_` endpoint receives **one iteration's frame**: one figure, no
way to see the others. That decides what `codegen._y_limit_plan` can emit:

| Situation | Emitted |
|---|---|
| scope names every ITERATE factor | nothing — the figure's own data defines it, which is what seaborn already does |
| …and FACET factors too | `facet_kws={"sharey": False}` (seaborn shares y by **default**, so this must be said or the export differs) |
| scope does **not** name every ITERATE factor | a literal `g.set(ylim=(lo, hi))`, computed at generation time |
| `minimum` and `maximum` both set | that literal |

### Why a literal and not `share_limits`

`scifor.for_each` already coordinates ranges across separately-iterated figures
(`share_limits={"df": ["subject"]}`), and it maps onto `scope` exactly — same
grouping, same meaning. It is deliberately not used, because the two answer
different questions:

- a **literal** reproduces the figure the user approved, byte for byte;
- **`share_limits`** recomputes from whatever the data holds at run time, so
  adding a subject would silently rescale a recorded artifact and the export
  would stop matching the preview it came from.

For a lineage-tracked figure the frozen number is the honest one. The spec in
the docstring still carries the scope, so regenerating after new data is one
explicit click. Revisit if a *living* endpoint is ever wanted over a
reproducible one.

## Gotchas

- **`""` is not `0`.** A cleared GUI box arrives as an empty string;
  `float("")` raises and `Number("")` is `0`, either of which would turn
  "compute this" into "pin the axis at zero". `spec._as_float` and the
  frontend's `LimitInput` both handle it.
- **An inverted manual range is ordered, not obeyed.** A minimum above the
  maximum is a typo; matplotlib would flip the axis and draw the figure upside
  down with nothing to say why (`ylimits._ordered`).
- **A panel whose scope group has no data falls back to the global range**, not
  to autoscale — one silently self-scaled panel on a page of shared ones is the
  hardest kind of wrong figure to notice.
- **`y_axis` is in the plan cache key.** Changing a scope rebuilds the plan,
  because the plan carries the limits. It is a checkbox, not a dragged slider,
  so this is per click.
- **Padding is 5%** (`ylimits.PAD_FRACTION`), matching what the old
  `_shared_limits` did, so adopting a scope does not silently re-pad every
  existing figure. On a log axis the 5% is of the log10 range.
- **Log axes.** Limits are carried in DATA units everywhere; plotly's `range`
  on a `type: "log"` axis is in log10 units, so `render.base.axis_range`
  converts at the edge (handing over data units asked for 10^0.95 .. 10^105
  and drew nothing). The computed floor is the smallest *positive* drawn value
  (`ylimits.pair_extent`); a hand-typed floor at or below zero is lifted a
  decade under the ceiling by `render.base.drawable_limits` for both
  renderers rather than passed through as NaN or silently ignored.
- **A bar chart keeps zero in view** (`ExtentMode.from_zero`): bars rise from
  zero, and a range bracketing only the bar tops cut every bar off at its base.
  Linear axes only — on a log axis there is no zero, and folding it in put
  `log10(0)` on the axis.
- **plotly violins span their data** (`spanmode: "hard"`). The default
  ("soft") runs the KDE two bandwidths past the extremes, into the part of the
  axis the limits cut off; matplotlib's `violinplot` spans exactly [min, max].
- **A NaN level is a level.** pandas hands it back as `nan`, and `nan != nan`,
  so the entry the extents wrote could never be found by `limits_for` and the
  panel silently fell back to the global range. Both sides key it through
  `ylimits.hashable` (NaN → None).
- **The GUI's checkboxes come from the figure, not the spec.** `spec.roles`
  never mentions a schema key promoted to ITERATE or a facet the table
  defaulted, so a panel factor with no checkbox was one the user could never
  scale apart. `ResolvedPlot.panel_factors` (also in plotly's `layout.meta`)
  is the list, as resolved.
