# The plotting layer: `scistackplot` / `scistackplotdb`

> **Status: IMPLEMENTED, TESTS PASSING (2026-09-06).** All six stages exist:
> `scistackplot/` and `scistackplotdb/` (new sibling packages), the
> `plot_service` + `api/plot.py` + JSON-RPC handlers in scistack-gui, the Plot
> Studio webview panel, and the extension commands. The decision table, module
> layout, staging, and per-fix history live in `.claude/plan-scistackplot.md`.

This is the *interactive* plotting layer. It is distinct from — and builds on —
the `plot_` endpoint machinery described in
[plotting-leaf-nodes.md](plotting-leaf-nodes.md), which already handles
rendering, saving, stamping, and recording a figure once you have written the
plotting function. This layer is about **producing that function** by direct
manipulation of the data, and about giving a scientist a way to look at their
results without writing plotting code at all.

## The central insight: a plotting GUI is a spec builder

The proof of concept in `/workspace/csv-data-visualizer-code` (R/Shiny) looks
like a plotting app. It is not. Every widget in `ui.R` sets exactly one field of
a small object, and `runPlotButton` compiles that object into a ggplot. The
picture is a *view* of the object, not the product.

This is what resolves the apparent tension in the requirement — "plots are
inherently visual, so this is really a GUI project" versus "it must have a
public API and work in a pipeline". Both are true of the same artifact:

- The GUI's product is a **`PlotSpec`**: small, serializable, diffable.
- A `PlotSpec` is exactly what the body of a `plot_` endpoint needs.

So the workflow is: explore interactively → freeze the spec → the spec becomes a
lineage-tracked `plot_` node in the DAG, with all the `finalized`, artifact
stamping, and `scidb report` behavior that already exists. Nothing about the
recording path is new.

## Two layers, mirroring scifor/scidb

| Package | Analogous to | Input | Knows about |
|---|---|---|---|
| `scistackplot` | `scifor` | in-memory long-format DataFrame | pandas, matplotlib/seaborn, plotly. **Nothing about scistack.** |
| `scistackplotdb` | `scidb` | `BaseVariable` (or several) | `scidb` + `scistackplot` |

They are separate distributions rather than additions to `scifor`/`scidb`. The
reason is dependency-shaped, not organizational: `scifor`'s stated selling point
is that it has no dependencies at all, and folding a plotting stack into it (or
matplotlib into `scidb`) would tax every existing user for a feature they may
not want. The naming follows the `scistacklog` precedent — `sciplot` and
`sciviz` are already taken on PyPI.

### The `DataSource` protocol is the compatibility mechanism

`scistackplot` defines a protocol:

```python
class DataSource(Protocol):
    def list_factors(self) -> list[FactorInfo]: ...
    def list_measures(self) -> list[MeasureInfo]: ...   # carries Shape
    def get_table(self, measures, filters) -> pd.DataFrame: ...
```

`scistackplot` ships CSV and DataFrame implementations. `scistackplotdb` ships
the scidb implementation. The GUI talks only to the protocol and never learns
which one it has. That single seam is the entire answer to "standalone yet
highly compatible" — the same application serves a lone CSV file and a full
scidb project, and the standalone claim stays honest because the CSV path is a
first-class entry point rather than a degraded mode.

## The resolve pipeline

```
DataSource ──► LongTable ──► PlotSpec
                                │
                        resolve(spec, table)      pure, deterministic, testable
                                ▼
                          ResolvedPlot            per-panel tidy frames
                         ╱      │      ╲          + encodings + shared limits
                    mpl.py   plotly_.py   codegen.py
                  (export)  (interactive) (source text)
```

`ResolvedPlot` is the non-obvious piece. It would be simpler to compile
`PlotSpec` straight to each renderer, but then the interactive plotly view and
the exported matplotlib figure are two independent implementations of the same
semantics, and they *will* drift — the aggregation, the error-band definition,
and the facet ordering would each be written twice. Putting all the data
reduction above the renderer split means the renderers are dumb translators, the
semantics are tested once against golden `ResolvedPlot` fixtures, and a MATLAB
renderer later is a new leaf rather than a redesign.

Renderer roles are fixed: **plotly for interaction** (in the webview, no image
round-trip), **matplotlib for export and for the pipeline** (the `plot_`
contract already expects a `Figure`).

## The role model

`server.R` spends the bulk of its length keeping four widgets consistent —
`dataReductionFactorsCheckboxGroup`, `tickFactorsselectInput`,
`colorFactorSelectInput`, `facetFactorCheckboxGroup` — with `observeEvent`
handlers that recompute each other's choices via `setdiff`. All four encode one
invariant: **every factor has exactly one role.** Modeling that directly deletes
the bookkeeping.

| Role | Meaning |
|---|---|
| `ITERATE` | separate figure per level (fan-out) |
| `X` | x-axis position |
| `COLOR` | one series/hue per level |
| `FACET` | one subplot per level; the grid arrangement is a separate layout decision |
| `AGGREGATE` | collapse — mean ± error across its levels |
| `FREE` | left as a column; contributes replicate points |

`FACET` accepts several factors at once, unlike `X`/`COLOR`. How the resulting
panels are arranged into rows and columns is deliberately *not* encoded in the
role: `FacetOptions.rows`/`cols` hold matcher rules ("row 1 = names starting
with R") that place each panel by its label. Splitting layout from assignment
is what lets one arrangement be reused across variables — and it expresses
grids the old `FACET_ROW`/`FACET_COL` pair could not, such as left/right x
muscle group from a single field factor.

Both renderers read the grid coordinates `reduce` assigned (`Panel.grid_row`/
`grid_col`); neither derives a layout of its own. The axis rules follow from
that one grid: `base.shows_x_labels` decides tick labels **and** the axis title
together, because they drifted apart when each was computed separately.

Note the correspondence to the existing endpoint rule (D2 in
[endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md)):
`ITERATE` is precisely scidb's *iterated schema keys → separate figure files*,
and every other role operates on *non-iterated keys → DataFrame columns*. The
vocabulary already existed; this layer just makes it directly assignable.

### Plot availability is one pure function

```python
available_plots(shape: Shape, roles: dict[str, Role]) -> list[PlotKind]
default_plot(shape: Shape, roles: dict[str, Role]) -> PlotKind
```

This lives in `scistackplot`, and the GUI renders only what it returns (CLAUDE.md
NOTE 3 — the GUI must not carry plotting policy). The requirement that "more
summative options become available when you iterate at a higher schema level"
is not a special case: a boxplot needs a *distribution*, and a distribution
exists only when some factor is `AGGREGATE` or `FREE` under the current x
grouping. One rule produces the whole table:

| Measure shape | no aggregation | with aggregation |
|---|---|---|
| scalar | scatter (one point per schema ID) | box / violin / bar + CI |
| 1D array | one line per schema ID | mean line + shaded error region |
| 2D | heatmap / image | mean heatmap |
| two measures | x–y scatter | ellipse / regression |

## What the scidb layer has to solve

The long format itself is free: `load(as_df=True)` already returns schema keys
as ordinary columns (the same shape `stat_` functions consume via `as_table`).
The real work in `scistackplotdb` is the five problems a single flat CSV never
had.

**1. Shape classification.** Scalar vs 1D vs 2D per variable, and for 1D,
whether to explode the array into rows with an implicit index column
(`sample`/`percent`) or keep it nested. This determines what
`available_plots()` may offer, so it must be decided before the GUI can render
its controls — and cached, because it is a per-variable property that does not
change between interactions.

**2. Joins across schema depth.** Plotting trial-level `Speed` against
subject-level `Mass` requires broadcasting the shallower variable down the
hierarchy. `scidb` knows the schema hierarchy; the plotting layer must not
guess. The same knowledge gates *which variables may share a plot at all* — the
GUI needs a `joinable_with(var)` query to populate its measure list honestly
instead of offering combinations that cannot be built.

**3. Variants are factors, and this is a correctness trap.** A variable with two
`low_hz` branch-param variants returns **two rows per schema combo** from
`as_df`. If branch_params are treated as ordinary columns — or ignored — two
different pipelines' results get overplotted as if they were replicates of one,
and the resulting figure is quietly wrong. Variants must surface as first-class
factors, assignable to `COLOR`/`FACET_*`/`ITERATE` like any other. Pooling them
requires assigning the factor `AGGREGATE` or `FREE` **deliberately** —
`roles.validate` refuses it for a factor nobody assigned, which is the case that
would be silent. (This replaced a `variant_policy` flag: two switches for one
decision, with no defensible meaning when they disagreed.)

Variants pool **within** a variable, never across two. When a figure draws more
than one variable, `Variable` becomes an ordinary factor that must separate
them — see `scistackplot/tests/test_multi_variable_pooling.py`.

**4. Payload budget.** 1D data across hundreds of trials is megabytes, and it
crosses a webview boundary on every interaction. Aggregation and downsampling
happen server-side, in `resolve()`, before anything is serialized. This is also
why `resolve()` must be cheap and instrumented — see Diagnostics below.

### Ordering trap: zero-padded keys

Schema keys like `"01"` are preserved as strings by project rule. On a
categorical axis, pandas' default lexicographic ordering yields `"1", "10",
"2"` — visibly wrong, and wrong in a way that looks like a data problem rather
than a plotting problem. Factor level ordering must come from scidb's declared
key type (`schema_key_types`), not from the values' natural sort. See
[schema-key-types.md](schema-key-types.md).

## Relationship to `plot_` endpoints, and the two fan-out paths

Export generates **literal seaborn/matplotlib source**, not a call back into
`scistackplot`:

```python
def plot_step_length(df, filename):
    g = sns.catplot(data=df, x="session", y="StepLength",
                    hue="limb", col="group", kind="box")
    return g.figure
```

The alternative — `return scistackplot.render(df, spec_path)` — is more compact
and stays re-editable in the GUI, but it makes every exported pipeline depend on
this package at runtime and hides the figure's definition behind an opaque call.
Literal code matches the project's "minimize lock-in" goal and the precedent in
[gui-export-to-plain-python.md](gui-export-to-plain-python.md). The spec is
emitted as a comment so the GUI can round-trip it back.

The same obligation applies to any reshaping `ScidbSource` does before the
spec sees the data: melting a struct's fields happens in `get_table` on the
interactive path, so `codegen` emits the matching `df.melt(...)` into the
generated function. Reshape in one path only and the exported figure silently
stops being the previewed figure.

There is a subtlety worth stating explicitly, because it is the most likely
place for the two halves of this layer to disagree. **Fan-out happens twice, by
two different mechanisms:**

- Interactively, `ITERATE` roles fan out via a pandas `groupby` inside
  `resolve()`, producing many panels in the studio.
- In the pipeline, `ITERATE` roles become `for_each` iteration kwargs, with
  `PathOutput("plots/{subject}.png")` producing many files.

These must produce the same set of figures. A test asserts panel-set equality
across the two paths; if it ever fails, the exported pipeline is not what the
user previewed, which is the worst failure mode this layer has.

Note that `scistackplot` deliberately does **not** depend on `scifor`, even
though `for_each` is the natural fan-out engine — it would contradict the
standalone requirement for the sake of a `groupby`. The ITERATE-role →
`for_each`-kwarg translation lives in `scistackplotdb`'s codegen, which may
import `scifor` freely. This is the one place the "avoid scifor/scidb
duplication" rule is knowingly relaxed, and it is relaxed at the smallest
possible surface.

## GUI hosting

v1 is a **VS Code webview panel only** ("Plot Studio"), reached by right-clicking
a `VariableNode` in the DAG, from the sidebar variable list, or via a
`SciStack: Plot…` command. Right-clicking a `.csv` in the Explorer routes to the
CSV `DataSource` and requires no project at all — which keeps the standalone path
exercised rather than theoretical.

The panel is backed by the **same** Python process over the existing JSON-RPC
transport (`plot.describe`, `plot.capabilities`, `plot.resolve`, `plot.export`).
This is a requirement, not a convenience: a second DuckDB connection would
reintroduce exactly the lock contention that the MATLAB run-ownership work
resolved — see
[matlab-run-database-ownership.md](matlab-run-database-ownership.md).

A standalone server mode (the direct Shiny equivalent) is deferred past v1. The
`DataSource` seam is what keeps it cheap when it arrives; the React app would
gain a second transport, the pattern `scistack-gui` already uses.

## Diagnostics

Per CLAUDE.md NOTE 2, and because "the plot is slow" is otherwise
unattributable between three very different costs:

- INFO: spec resolved (measure, kind, panel count, row count); export written.
- DEBUG: role assignment, chosen aggregation, per-panel row counts, and
  `[timing]` for **load → resolve → render** as three separate spans.
- WARN on each silent-wrongness trap: variants pooled, factor levels sorted
  lexicographically because no key type was declared, downsampling applied.

Logging goes through `scistacklog` with `layer="scistackplot"` /
`"scistackplotdb"`; see [logging-architecture.md](logging-architecture.md).

## Deliberately out of scope (v1)

- **MATLAB renderer.** Existing MATLAB `plot_` functions are unaffected;
  `ResolvedPlot` is the future integration point.
- **Standalone web server.** Deferred; see GUI hosting above.
- **Shared color scales across iterated figures** — the natural sibling to
  `share_limits`, and named as future work in the endpoints design doc.
  Coordination *across* files remains the one thing a plotting library working
  on a single figure cannot do.
- **A `for_columns` analog** — plotting every column of a multi-column variable,
  mirroring the same gap noted for csv-stats' `data_column="_"`.
