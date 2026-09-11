# Y-limit scoping

How `scistackplot` decides what range each panel's y axis spans, why the
decision is made off the **table** rather than off the figures, and where the
two renderers read it from.

Written 2026-09-11, alongside `.claude/plan-plot-save-and-ylimits.md` Stage 5.

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

`_spread` is kept deliberately parallel to `reduce._summarize`. Two definitions
of an error band would put the limits and the drawing at odds, and the symptom
would be a band clipped by its own axis.

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
  existing figure.
