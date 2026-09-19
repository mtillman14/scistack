# Spaghetti plot: repeated-measures trajectories on a nested x axis

*Written 2026-09-16, when `PlotKind.SPAGHETTI` was added.*

## The figure, and how the existing controls already compose it

"Group by Intervention (Digitimer / Sham / Onward); within each group one tick
per session; one point per subject per session; a line joining each subject's
points." Three of the four pieces were already controls:

| Piece | Control |
|---|---|
| Intervention outer, session inner | both `Role.GROUP`; `groups` is the ORDER (innermost first — `[subject, session, Intervention]`), and `ordered_groups` places a subject-level factor (depth 1) outside session by default |
| one point per subject, joined into a line | `subject` is the FIRST grouping layer — the lines (2026-09-17 model) |
| trials averaged first | `trial = Role.COLLAPSE` |
| the lines | **nothing** — hence the new kind |

`SCATTER`/`STRIP` draw markers only; `LINE` owns the series machinery but is
refused for scalar measures and for a nested x (`roles.py`, "Nested x grouping
needs a categorical axis"). SPAGHETTI is a scalar kind with LINE's identity.

## Rules (all in scistackplot, none in the GUI)

- **Availability** (`capability.available_plots`): scalar shape, categorical
  x, and ≥2 grouping layers (since 2026-09-17; was ≥1 FREE factor) — the first
  factors *are* the lines. `REPLICATE_KINDS = DISTRIBUTION_KINDS + (SPAGHETTI,)`
  is what `roles._with_replicates_for` and `why_unavailable` consult; it is a
  separate tuple because a spaghetti does not *summarise*.
- **In `SCALAR_KINDS`**: chosen on a 1-D measure it collapses each vector first
  (a spaghetti of per-trial medians is a legitimate ask).
- **Series key** (`reduce._panel_frame`): every table factor present in the
  group EXCEPT (a) all x layers — not just the outer one — and (b) the
  panel-constant factors (the figure's ITERATE keys and its facets). (b) is
  spaghetti-only: keeping them made `"pre | 01"` and `"post | 01"` two series,
  so one subject sat at two different offsets in two facets. LINE keeps its
  historical key.
- **Lines never cross a group boundary** without special handling: a subject
  belongs to one intervention, so its line can only join the x positions it
  occupies. The nested-x test in `test_spaghetti.py` asserts it anyway.

## The offset: one owner, three readers

Random jitter (STRIP) is not available: a line must end on its own marker. So
each series gets ONE deterministic horizontal shift it keeps at every x
position — `scistackplot/spaghetti.py::series_offsets(ids)`: natural-sorted,
evenly spaced over `[-SPAGHETTI_SPREAD, +SPAGHETTI_SPREAD]` (0.2), a single
series at 0. Computed **once per figure** in `reduce._spaghetti_offsets` and
carried as `ResolvedPlot.series_offsets` (also in `to_dict`).

Readers:

1. `render/mpl.py::_draw_spaghetti` — `x_positions()` index + offset, sorted
   by position, `ax.plot(marker="o")`. The flat-categorical tick block lists
   SPAGHETTI alongside SCATTER/STRIP.
2. `render/plotly_.py::_spaghetti_traces` — same arithmetic,
   `mode="lines+markers"`, hover names the level and the series.
3. `codegen._spaghetti_position_lines` — restates the rule in five lines of
   plain pandas (an exported endpoint cannot import this package), builds
   `_xpos`, and the plot call is `sns.relplot(kind="line", units="_series",
   estimator=None, marker="o")` followed by `g.set(xticks=…, xticklabels=…,
   xlim=…)`. Not a frozen literal (unlike the y limits): a literal keyed by
   series id would be figure-set-wide, and under ITERATE the endpoint sees one
   figure's subjects — the preview spreads THOSE across the band.
   `test_generated_offsets_match_the_library_rule` execs the generated
   preamble and compares it with `series_offsets`.

## The plotly trap: a category axis cannot take a fractional position

A number in a category trace is stringified and becomes a NEW category, so
`1.15` would draw as its own tick. For SPAGHETTI (and only when the x is
categorical — `_positional_x`) the axis is **linear** with
`tickvals=range(n)`, `ticktext` from `x_plan.tick_labels` (or `x_order`), and
`range=[-0.5, n-0.5]` — the range a category axis has by default, which is
also the geometry `_add_x_groups` assumes when it places brackets at `i / n`.
`_x_ticks(resolved)` is the one place tick placement is decided for both the
nested and the positional case. STRIP still has no jitter at all in the plotly
preview; if that is ever wanted, this is the mechanism.

## Found on the way: nested x + AGGREGATE export was broken

`codegen._preamble` builds the composed `_x` column BEFORE the averaging
`groupby`, and pandas drops every column the groupby does not name, so the
plot call asked for an `_x` that no longer existed. Pre-existing for every
nested + aggregate export (e.g. bars over Intervention > session with trials
averaged). Fixed by adding `_X_NESTED` to `keep`;
`test_generated_code_keeps_the_nested_axis_through_an_aggregate` pins it.

## Not done (follow-ups the user named)

- Group summary overlay (mean ± error per group on top of the trajectories).
- Colour is the COLOR factor if assigned, else one neutral colour; per-subject
  colour is `subject = COLOR`, nothing special.

**2026-09-19:** "Show sample" (`show-sample-overlay.md`) generalised this —
bar/box + `show_sample=["subject"]` + the automatic join draws these same
lines with a summary behind them. The group-summary overlay above is
therefore available that way round; whether SPAGHETTI stays a kind is open.
