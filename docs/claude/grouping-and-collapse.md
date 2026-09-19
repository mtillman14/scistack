# Grouping and Collapse — how a plot's factors are read

**Since:** 2026-09-17 (plan: `.claude/plan-collapse-separate-roles.md`).
Replaces the `X / COLOR / AGGREGATE / FREE` vocabulary outright; there is no
mapping from the old names (`PlotSpec.from_dict` raises `LegacySpecError`).

## The model in one paragraph

Every factor of a plot is in one of two panes. The **Grouping pane** is an
ordered list, **innermost first**, of the factors that get one *mark* per
level combination — where a mark is whatever the kind draws (a bar, a box, a
line). One entry may be tagged as the **colour**. The **Factors pane** holds
everything else, each with one of three roles: *Separate figures*
(`ITERATE`), *Separate panels* (`FACET`), or *Collapse* (`COLLAPSE` — averaged
away). Collapsed keys average away **deepest first, nested and unweighted**,
and the **last** collapsed key is the *sample* the kind's statistic is
computed over.

```python
Role.GROUP      # one mark per level; ordered by PlotSpec.groups (innermost first)
Role.FACET      # one subplot per level
Role.ITERATE    # one figure per level — the default for an unmentioned factor
Role.COLLAPSE   # averaged away; the outermost collapsed key is the sample

PlotSpec.groups: list[str]      # innermost first
PlotSpec.color: str | None      # must name a grouping layer
Aggregation.pooled: bool        # "weight by N": one groupby over every collapsed key
PlotSpec.cell_statistic         # the 1-D → scalar CELL reduction (was collapse_statistic)
```

## The grouping list, per kind

The list is the user's; the *reading* is the kind's. `roles.grouping_layers`
is the one place the innermost-first spec meets the outermost-first axis —
nobody reverses `groups` ad hoc.

| kind | the layers become | the coloured layer |
|---|---|---|
| bar / box / violin / strip / scatter | nested x ticks, first entry innermost | **not a tick** — it dodges inside its tick and the legend labels it |
| scatter with `x_measure` | one point set per leaf group | point colour |
| line / band (1-D, x = sample index) | one line / band per leaf group | line colour; every *uncoloured* layer gets a **dash style** |
| spaghetti | **first entry = the lines**; the rest are ticks; a line joins its points across the layer just above it | colour on the first entry = per-subject colours |
| heatmap (2-D) | nothing may group — use panels / figures | — |

`[subject, session, Intervention]` reads the same for bar and spaghetti: bars
for subjects inside session ticks inside Intervention brackets; or points per
subject joined across sessions inside Intervention brackets.

Rules that follow:

* `MAX_X_LAYERS = 3` caps the **labelled** tick layers — grouping layers minus
  the coloured one. Colouring a layer makes room for another.
* Series ids (`__series`, and the exported `_series`) are composed
  **outermost first** — `"pre | 01"` — via `reduce._series_key`; codegen
  mirrors it.
* The uncoloured part of a series id is the dash id (`__dash`). Styles are
  assigned once per figure from `resolved.DASH_CYCLE` (six), natural order,
  the same style for a subject in every panel and colour; past six the lines
  still cycle but the legend stops listing them and a WARN suggests Separate
  panels. Spaghetti is never dashed.
* A 1-D measure and an x-y plot may group freely (the layers are series);
  only a 2-D measure refuses.

## The collapse chain

`roles.collapse_order(roles, table)` — deepest first by `factor_depths`;
field factors (`ColName`, inside a record) first of all; a joined factor
variable carries the depth of the key it hangs off; a factor with no depth
(a derived bucket) last. `roles.collapse_steps(spec, roles, table)` splits
that into:

* `pre` — averaged away first, each grouping on every other factor still
  present (`reduce._collapse_levels`, one groupby-mean per key);
* `sample` — the last key (or **all** of them when `pooled`);
* `final` — empty, except for one spaghetti case (below).

**Schema-level parity (user decision, 2026-09-19): every kind draws the
sample.** The collapsed keys mean the same thing whatever the kind; only
the geometry changes. Before this, scatter, strip, line and spaghetti also
averaged the sample (`MEAN_DRAWING_KINDS`, now deleted), so a bar of
subjects and a scatter of subjects were drawn from different rows.

| kind | the sample rows are drawn as |
|---|---|
| bar / band | centre ± spread (`_summarize`; `Aggregation.statistic` / `.error`) |
| box / violin | a distribution |
| scatter / strip | one point per row |
| line | **one polyline per sample level** (`GroupingLayers.units`, seaborn `units=`) inside its colour / dash |
| spaghetti | one polyline per sample level inside its line group, **if the sample recurs across the x ticks**; otherwise each line is the sample's mean (`final`) |
| heatmap | a 2-D measure skips the chain; the panel is the matrix mean |

`GroupingLayers.units` are part of the series id (`identity = units +
series`, composed outermost first: `"groupA | 01"`) and nothing else. They
never get a dash style and never appear in the legend.

**The spaghetti exception** (`roles.spaghetti_sample_repeats`, the same
depth rule as `overlay_join`). A spaghetti line joins one identity across
the ticks. A subject has a value at every session, so collapsed subjects
under session ticks become one line each. A trial belongs to ONE session,
so "trial 1 at pre" and "trial 1 at post" are different trials, and a line
through them would be invented. In that case (the classic "subject lines,
trial collapsed") `collapse_steps` sets `final = sample`, and each line
is drawn through the trial mean. An INFO line says so:
`spaghetti draws the mean of trial per line: A trial belongs to one session…`.

"Save data" (`export.plot_data`, `plot-data-export.md`) writes the sample
rows by default, so the file is the same for every kind.

**Worked example** (unbalanced on purpose — on a balanced design nested and
pooled coincide): subject 01 has trials {1, 2, 3}, subject 02 has {9};
`subject=Collapse, trial=Collapse`, bar.

| | value | error bar |
|---|---|---|
| nested (default) | mean of per-subject means {2, 9} = **5.5** | SD over {2, 9}, n = 2 subjects |
| pooled (`Weight by N`) | mean of {1, 2, 3, 9} = **3.75** | SD over four trials |

Both are legitimate; only one is the default, and the checkbox says which.
`scistackplot/tests/test_nested_collapse.py` pins these numbers, and
`test_codegen.py` pins the export to them.

**No collapsed key → no sample.** A bar of single values has no error bar;
box / violin / band are unavailable (`roles.kind_requirement`, the one
statement of what each kind needs, shared by `validate` and
`capability.why_unavailable`). Spaghetti needs two grouping layers.

**Variants are never replicates.** An unassigned variant factor separates
figures like any unmentioned factor; `validate` refuses `COLLAPSE` on a
multi-level variant factor and on `Variable`.

## Defaults (granular and fast)

* Scalar: the deepest schema key present is grouped (uncoloured), every other
  plain factor separates figures, nothing collapsed — a bar opens without
  error bars and box / violin are greyed out with the reason.
* 1-D: one record per figure (all `ITERATE`, nothing grouped); 2-D likewise.
* A variant factor → innermost coloured grouping layer; fields / `Variable`
  → separate panels.
* A factor a saved spec never mentions → `ITERATE`. This is what let the old
  `iterate_ancestors` promotion be deleted: nothing is silent, so nothing
  needs promoting.
* Kind auto-fix (`roles.with_requirements_for`): choosing box / violin / band
  with no sample collapses the **deepest plain factor not already grouping**
  (`speed` beside a grouped `trial`); spaghetti with < 2 layers pulls the
  deepest iterated factor in as the lines.

## Where things live

| question | owner |
|---|---|
| collapse order / sample / steps | `scistackplot/roles.py` — `collapse_order`, `sample_key`, `collapse_steps` |
| how a kind reads the grouping list | `roles.grouping_layers` → `GroupingLayers(ticks, series, color, units)` |
| one line per sample level (line, spaghetti) | `roles.UNIT_KINDS`, `GroupingLayers.units` / `.identity`, `roles.spaghetti_sample_repeats` |
| the sample rows, one call for figure and CSV | `reduce._sample_frame` (figure) = `export.plot_data` default |
| what each kind needs | `roles.kind_requirement` |
| the chain, run | `reduce._collapse_levels` (pandas), `reducer._chain` (numpy), `codegen._preamble` (emitted), `ylimits._reduced_extents` (limits) |
| dash styles | `resolved.DASH_CYCLE` / `MPL_DASHES`, `reduce._dash_styles`, `codegen._SEABORN_DASHES` |
| grouping placement in the GUI | `scistack-gui/frontend/.../groups.ts` (mirror of `PlotSpec.ordered_groups`) |
| capability report | `kinds[].assignment` (roles + groups + colour), `cell_collapse`, `collapse {order, sample, pooled}`, `has_sample`, `grouping {layers, color, ticks, series, units, labelled_layers, max_labelled_layers, hint}`, `data_export` |
| the collapsed keys drawn as points ("Show sample") | `roles.overlay_steps` / `overlay_join` — see `show-sample-overlay.md` |
| cutting the chain at a key (overlay + CSV depth) | `roles.chain_cut` |

## Traps

* **Numbers changed on 2026-09-19** for scatter / strip / line / spaghetti
  with a collapsed key. They now draw the sample rows (one point or one line
  per subject), not the sample's mean. Only the spaghetti exception above
  still averages.

* **Two things called "collapse".** The *cell* collapse (`cell.py`,
  `cell_statistic`) reduces each 1-D record to one value when a scalar kind
  is chosen; the *collapse role* averages a factor's levels away. "Median
  within each trial, mean across trials" is both at once.
* **Colour is not a tick.** A coloured layer that also composed into the leaf
  key gave `01␟pre` ticks and broke every renderer test; the renderers dodge
  by `COLOR`, so `grouping_layers.ticks` excludes it.
* **Numbers changed on 2026-09-17** for any saved spec that had ≥2
  `AGGREGATE` keys (pooled → nested) or whose error bars came from a `FREE`
  key that was not the only non-channel key.
* **Tests that count records off the opening state** must flatten the fan-out
  first (`scistackplotdb/tests/test_source.py::_one_figure`) — the default
  opens one figure per record.
