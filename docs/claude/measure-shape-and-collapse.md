# Measure shape, and collapsing a 1-D measure to a scalar

*Written 2026-09-14, before the work in `.claude/plan-1d-collapse-to-scalar.md`.
Concerns `scistackplot`, and the Plot Studio panel that renders its capability
report. Companion to `docs/claude/synthetic-factors.md`, which covers the
derived tables that add **factors**; this one covers the derived table that
changes the **measure**.*

## Shape is the second axis of the control surface

`scistackplot` has two questions it answers before anything can be drawn:

1. **What does each factor do?** — one role per factor
   (`docs/claude/synthetic-factors.md`).
2. **What does one cell of the measure hold?** — `Shape.SCALAR`,
   `Shape.SERIES_1D`, `Shape.MATRIX_2D` (`shape.classify_column`).

Shape is what makes the *kind* list mean something. `capability.available_plots`
is a pure function of `(shape, roles)`, and the three rows of its table are three
different pictures:

| shape | no replicates | with replicates |
|---|---|---|
| scalar | scatter, strip | + box, violin, bar + CI |
| 1-D | one line per observation | + mean line, shaded band |
| 2-D | heatmap | mean heatmap |

Shape is classified from **observed values**, never from a declared dtype or a
SQL type name: a pandas object column of ndarrays and a DuckDB `DOUBLE[]` column
are the same thing to a plot, and the value is the only common ground.

## The gap this closes

A 1-D measure had access to exactly two kinds — line and band — because its cells
hold many numbers and every scalar kind needs one number per row. But "one number
per row" is usually one *summary* away: the mean step length of a trial, the peak
of a trace, the average of a per-trial vector. Scientists ask for the violin
across trials, and the vector inside each trial is a detail of how the trial was
recorded, not the thing being compared.

So: **collapse each cell to one value, then treat the measure as an ordinary
scalar.** Nothing about the scalar path changes, which is the entire point.

## The mechanism: a derived table that rewrites the measure

`collapse.apply_collapse(spec, table) -> LongTable` returns a table in which the
y measure's column holds one float per row and whose `MeasureInfo.shape` is
`SCALAR`. It is the third member of the derived-table family:

| derived table | built in | what it changes |
|---|---|---|
| `apply_variant_sets` | `variants.py` | adds the `Variant` factor, consumes variant columns |
| `apply_level_groups` | `groups.py` | adds a bucketed factor, keeps the source factor |
| `apply_collapse` | `collapse.py` | rewrites the **measure**: 1-D cells → scalars |

It obeys the same test as the others (*does the spec decide?* — yes, the plot
kind does), so it is recomputed wherever the spec is read, and a project that
never selects a scalar kind on a 1-D variable pays nothing: it returns `table`
unchanged.

### Why a derived table rather than an "effective shape" parameter

The rejected alternative was to leave the data alone and thread a computed shape
through everything that asks `table.shape_of(...)`: `validate`, `complete_roles`,
`capabilities`, `grouping_summary`, `reduce._build_plan`, `_panel_frame`,
`_encoding_for`, `ylimits`, four helpers in `codegen`. That is roughly fifteen
call sites, each of which would then be free to disagree about what it is
plotting — and the failure mode of a disagreement here is not an exception, it is
a figure drawn one way and exported another.

Rewriting the data once, early, means the question never arises. Downstream code
asks the table what shape the measure is and gets a true answer.

### Order of operations

In `reduce._build_plan_timed`:

```
apply_variant_sets  ->  apply_level_groups  ->  apply_collapse
   ->  strip_answered_roles / validate / complete_roles  ->  apply_filters
```

Three consequences that are load-bearing, not incidental:

- **Before `validate`.** The rules that refuse a factor on X, and refuse nested x
  layers, for a 1-D measure are correct as written — a 1-D measure's x axis *is*
  its within-observation index. Once collapsed, the measure genuinely is scalar
  and those rules should permit both. They do, without being touched.
- **Before `apply_filters`.** A range filter on the measure then filters the
  collapsed value, which is the only thing the user could mean by "step length
  between 0.4 and 0.8" when each record holds a vector of them.
- **Before `y_limits`.** The extent pass runs over one float per record instead of
  every sample. On the variable that motivated `project_plot_at_scale` that is
  419 floats instead of 174 million samples.

## The kind implies the collapse

There is **no collapse toggle**. A scalar-only kind (`SCATTER`, `STRIP`, `BOX`,
`VIOLIN`, `BAR`) selected on a 1-D measure *is* the request to collapse; `LINE`
and `BAND` are the request not to. Only the statistic is a separate field
(`PlotSpec.collapse_statistic`, mean or median).

This is a deliberate rejection of the more obvious design — a "reduce each vector
to one value" checkbox that unlocks the scalar kinds. A checkbox creates states
that have no meaning (`collapse=on` with `kind=line`) and therefore needs a rule
for which control wins, which is a rule the user has to learn. Deriving the
collapse from the kind makes the contradictory state unrepresentable, the same
move as one-role-per-factor.

### The consequence for `available_plots`

The kind list must be computed from the **raw** shape, with `collapsible=True`;
everything else in the capability report is computed from the **collapsed**
table. Getting this backwards is the trap:

> Compute the kind list from the collapsed table and `LINE`/`BAND` disappear the
> moment the user picks a violin — the measure now looks scalar, so the 1-D kinds
> are "not available", and there is no way back to a line except deleting the
> figure.

So `capabilities()` holds both tables at once and is explicit about which
question each one answers. The report carries `shape` (effective — what the
figure is), `raw_shape` (what the data is), and a `collapse` block for the badge
and the statistic dropdown.

## Roles do not survive a shape change unchanged

A 1-D measure opens with every schema key on `ITERATE` — one record per figure,
decided 2026-09-13 for the opening cost. A scalar measure opens with the leading
factor on X, the next on COLOR, the rest FREE.

Those two are incompatible in a way the user feels immediately: with everything
iterated there are no replicates, so box and violin are greyed out with "needs
replicates", and the first click on Violin appears to do nothing. So selecting a
scalar kind **re-defaults the roles** — but only when they are still the
untouched defaults for the old shape (`roles.roles_for_kind`). A user who has
assigned roles by hand keeps them; the rule never overwrites a decision.

Delivered to the GUI as an optional `roles` on each `capabilities.kinds` entry
rather than as an RPC, so that applying it stays synchronous with the click. An
async round trip would race the panel's resolve queue and could land after the
user had set a role, overwriting it — the one thing the rule promises not to do.

Two things had to follow from that, both found by a failing test rather than by
reasoning, and both load-bearing:

**A kind is judged against the roles it brings with it.** Availability is
computed per kind from its own suggestion, falling back to the current roles
when it has none. Otherwise the re-roll is unreachable from the state it exists
for: the opening roles leave no replicates, so box/violin/bar are refused, so
the user cannot click the kind whose selection would have supplied the
replicates. `capabilities.available` is derived from the per-kind entries rather
than computed a second time — two lists that can disagree about one kind is the
bug, not the saving.

**A distribution kind is guaranteed a FREE factor** (`roles._with_replicates_for`).
The plain defaults do not always leave one: with two factors the scalar default
is `subject=X, session=COLOR` (one point per combination) and with one it is
`subject=X` (one point per violin) — the same "the click does nothing visible"
failure by another route. The **innermost** plain factor is freed, because its
levels are replicates of each other; freeing `subject` instead would pool across
people. Only a plain factor is eligible — a FREE variant factor pools two
pipelines' results, which `validate` refuses outright, so freeing one would
answer a greyed-out kind with an error message.

## Invariants

1. **The collapse is per cell, and lives in numpy.** `series_stats.collapse_cells`
   — `nanmean`/`nanmedian` over `cell_array`. Per `project_plot_at_scale`: DuckDB
   selects rows, numpy reduces them; nothing here builds a one-row-per-sample
   frame, which for the motivating variable would be 174 M rows to produce 419
   numbers.

2. **It is not a `Reducer` method.** The `Reducer` protocol exists for reductions
   with two genuinely different implementations — a per-position statistic is an
   exploded pandas groupby in the reference and padded blocks in numpy. A per-cell
   mean has one implementation; both reducers would call the same function, so a
   seam would only create a second definition to keep in sync. If a future
   collapse needs storage-specific work (DuckDB's `list_avg` in the SELECT,
   avoiding the transport entirely), *that* is when it earns a seam.

3. **NaN rows are kept, not dropped.** An empty or all-NaN cell collapses to NaN
   and stays as a row. Dropping would silently change the level counts the factor
   summary reports, so "this trial recorded nothing" would look like "this trial
   does not exist". The count is logged at WARN instead.

4. **The reshape exists twice, and the second copy is tested.** Like every
   reshape on the interactive path (`synthetic-factors.md` invariant 4), the
   collapse must be emitted into the generated endpoint — `codegen` writes a
   `df[measure].map(np.nanmean)` in place of the explode block, before the filter
   lines. A test executes the generated function and compares against the preview;
   asserting on the generated text alone would pass while the figures differed.

5. **The collapse is announced.** `apply_collapse` logs one INFO line per resolve
   (measure, statistic, rows, samples consumed, elapsed) and a `collapse_1d`
   phase in the existing build-plan timer. An empty violin has two very different
   causes — the vectors were empty, or the filter removed everything — and this
   line is what tells them apart without a debugger.

## Scope, and what is deliberately absent

- **Relational plots** (`PlotSpec.x_measure`) do not collapse. There the kind list
  is already `[SCATTER, LINE]` and the pairing is per sample; collapsing both
  measures to one point per record is coherent but is a different feature, so the
  collapse does not fire while `x_measure` is set.
- **2-D measures** do not collapse. A matrix reduces to a scalar only through a
  choice of two axes, which is a question this has no way to ask.
- **Statistics are mean and median only.** "A measure of centre" was the ask, and
  `Statistic` already spells those two. Peak/min/sum are the obvious extensions
  and need a separate enum when they arrive — `aggregate.statistic` (centre
  *across replicates*) must not start offering "max", because that is a different
  question with the same word attached.
- **No pushdown, and no memo.** The collapse runs over the loaded frame, and it
  runs **twice per spec change** — once in `capabilities` (which needs the shape,
  and needs the values only when a range filter names the measure) and once in
  the resolve's plan. Both are visible as the INFO line above, which is the
  point: the two obvious optimizations — memoizing per `(table, measure,
  statistic)`, and DuckDB `list_avg` in the SELECT so the samples are never
  transported — should be justified by a measurement on real data rather than by
  intuition, exactly as the reducer rewrite was.

## See also

- `docs/claude/synthetic-factors.md` — the derived tables that add factors, and
  the three call sites every derived table must be wired into
- `docs/claude/plotting-library-design.md` — the `PlotSpec`-is-the-product idea
- `.claude/plan-plot-minimal-load-examples.md` — the measurements behind
  "DuckDB selects, numpy reduces"
- `.claude/plan-1d-collapse-to-scalar.md` — the work this note precedes
