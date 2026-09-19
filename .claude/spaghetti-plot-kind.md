# Spaghetti plot kind (repeated-measures trajectories)

**Date:** 2026-09-16
**Status:** all 7 stages built 2026-09-16, both vite bundles rebuilt; Python tests NOT yet run (user runs them). Doc: docs/claude/spaghetti-plot.md. Bonus fix: nested-x + AGGREGATE export dropped `_x` (codegen keep list).

## The figure

Outer x groups = Intervention (a subject-level factor), inner ticks = session,
one marker per subject per session, one polyline joining each subject's markers
across sessions. Within each group you read how each subject changes.

## What already exists

| Piece | Control |
|---|---|
| Intervention outer / session inner | both `Role.X`; `x_layers` orders them, depth rule puts the subject-level factor outside |
| one point per subject | `subject = Role.FREE` |
| markers | `SCATTER` / `STRIP` |

Missing: the connecting lines. No scalar kind carries series identity; `LINE`
does but is refused for scalar measures and for nested x.

## Decisions (user, 2026-09-16)

1. Name: `PlotKind.SPAGHETTI` (not PAIRED). GUI label "Spaghetti (paired lines)".
2. Colour: COLOR factor if assigned, else one neutral colour. Per-subject colour
   is still reachable by `subject = COLOR`.
3. Jitter: small **deterministic per-series offset** so overlapping subjects
   separate and the line ends land on the markers. Series naturally sorted,
   evenly spaced in `[-SPAGHETTI_SPREAD, +SPAGHETTI_SPREAD]`; one series → 0.
   Offsets are computed ONCE per figure (same subject, same offset in every
   panel), not per panel.
4. Group summary overlay (mean ± error per group): follow-up, not now.
5. Add a nested-x SPAGHETTI test — the BAR nested-x bug went unnoticed for lack
   of exactly that.

## Rules

- Available exactly when a scalar measure has a categorical x and ≥1 FREE
  factor (same condition as box/violin): the FREE factors ARE the line
  identity, the rule `LINE` already uses.
- Series key = every factor in the group except the x layers (all of them, not
  just the outer one). Because a subject belongs to one Intervention, its line
  stays inside its group without special handling; in general lines never
  cross an outer-group boundary because they can only join x positions the
  series actually occupies.
- Kind is in `SCALAR_KINDS`: selected on a 1-D measure it collapses each
  vector first (spaghetti of per-trial medians is a legitimate ask).
- Kind needs replicates: `capability.unavailable_reason` and
  `roles.free_a_factor_for` treat it like the distribution kinds (new
  `REPLICATE_KINDS = DISTRIBUTION_KINDS + (SPAGHETTI,)`).

## Stages

1. **spec / capability / roles** — enum member, `SCALAR_KINDS`,
   `REPLICATE_KINDS`, `available_plots` offers it with replicates.
2. **one owner for offsets** — `scistackplot/spaghetti.py`:
   `SPAGHETTI_SPREAD`, `series_offsets(ids) -> dict[str, float]`.
3. **reduce** — `SERIES` emitted for SPAGHETTI (excluding all x layers),
   `Encoding.series` set, `ResolvedPlot.series_offsets` computed from every
   panel's series ids; in `to_dict`.
4. **renderers**
   - mpl: `_draw_spaghetti` — per colour group, per series: positions
     (`x_positions` + offset), sorted by position, `ax.plot(marker="o")`;
     first series of a colour carries the legend. Flat categorical tick block
     includes SPAGHETTI.
   - plotly: `_spaghetti_traces` — numeric positions + offset,
     `mode="lines+markers"`, hovertext = series id. A category axis cannot take
     fractional positions (plotly stringifies numbers into new categories), so
     for this kind the x axis is **linear** with `tickvals=range(n)`,
     `ticktext` from `x_plan.tick_labels` / `x_order`, `range=[-0.5, n-0.5]`
     — the same geometry the bracket arithmetic (`_add_x_groups`) assumes.
5. **codegen** — preamble builds `_series` (as LINE does) and `_xpos =
   df[x].map({level: i}) + df["_series"].map({id: offset})` with the offsets as
   a literal — under ITERATE the endpoint sees one figure and a set-wide literal
   would bunch its subjects; plot call `sns.relplot(kind="line", x="_xpos",
   units="_series", estimator=None, marker="o")`; `g.set(xticks=..., xticklabels=...)`.
6. **GUI** — `KIND_LABELS.spaghetti`, the collapse hint text; rebuild both
   vite targets.
7. **tests** — `scistackplot/tests/test_spaghetti.py`: availability, series
   key excludes both x layers, offsets deterministic/symmetric/single→0,
   mpl line count = subjects × colour groups and x sorted, plotly
   `lines+markers` + linear axis + tick labels + bracket count under nested x,
   generated code runs and draws the same number of lines; README kind table
   row; `docs/claude/spaghetti-plot.md`.
