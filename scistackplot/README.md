# scistackplot

## Build the figure by looking at it, then keep it

`scistackplot` turns a long-format table into a figure from a small,
serializable description — a `PlotSpec`. It works standalone on a CSV or a
DataFrame with no database and no configuration, and the same `PlotSpec` is
exactly what the body of a SciDB `plot_` endpoint needs, so an interactive
exploration can be frozen into a lineage-tracked pipeline step.

```bash
pip install scistackplot
```

## The idea

A plotting GUI looks like it produces pictures. It doesn't — it produces a
**specification**, and the picture is a view of it. That is what lets an
inherently visual tool live inside a reproducible pipeline:

```python
import pandas as pd
from scistackplot import DataFrameSource, PlotSpec, Role, PlotKind, render

source = DataFrameSource(pd.read_csv("gait.csv"))
spec = PlotSpec(
    measures=["StepLength"],
    roles={"session": Role.X, "limb": Role.COLOR, "subject": Role.FREE},
    kind=PlotKind.BOX,
)
figure = render(source, spec)
```

## Every factor does exactly one thing

The whole control surface is one rule: each categorical column carries exactly
one role.

| Role | Meaning |
|---|---|
| `X` | x-axis position |
| `COLOR` | one coloured series per level |
| `FACET` | one subplot per level (arranged by `FacetOptions`) |
| `ITERATE` | a separate **figure** per level |
| `AGGREGATE` | collapse — average over this factor |
| `FREE` | keep as replicate rows |

Which plot kinds are available follows from that assignment plus the measure's
shape, through one pure function:

```python
from scistackplot import available_plots, default_plot, Shape

available_plots(Shape.SCALAR, {"session": Role.X})                    # scatter, strip
available_plots(Shape.SCALAR, {"session": Role.X, "trial": Role.FREE})  # + box, violin, bar
```

A distribution needs replicates, and replicates exist only when some factor is
left `FREE`. That single rule produces both defaults and availability:

| Measure shape | no replicates | with replicates |
|---|---|---|
| scalar | scatter | box / violin / bar + CI |
| 1-D array | one line per observation | mean line + shaded error band |
| 2-D | heatmap | mean heatmap |

`AGGREGATE` deliberately does *not* count as replicates: it averages its factor
away before anything is drawn. "Average over trials, then show the spread
across subjects" is `trial=AGGREGATE, subject=FREE`.

## Arranging the subplots

Faceted panels flow in order by default, wrapping at `FacetOptions.wrap`. When
the arrangement matters, describe it with **rules** instead of positions:

```python
from scistackplot import FacetOptions, MatchOp, Matcher, PlotSpec, Role

spec = PlotSpec(
    measures=["RawEMG"],
    roles={"ColName": Role.FACET, "subject": Role.COLOR},
    facet=FacetOptions(
        rows=[Matcher(op=MatchOp.STARTS_WITH, value="R"),
              Matcher(op=MatchOp.STARTS_WITH, value="L")],
        cols=[Matcher(op=MatchOp.ENDS_WITH, value="HAM"),
              Matcher(op=MatchOp.ENDS_WITH, value="TA")],
    ),
)
```

Rules describe a layout rather than a hand-arrangement, so the same
`FacetOptions` applies to any variable whose panels are named the same way.
Ops are `starts_with`, `ends_with`, `contains`, `not_contains`, `equals` and
`regex`; a panel matching no rule lands in a trailing "other" row or column
rather than vanishing.

Each panel is named on its **y axis**, not by a caption above it. A caption
spends a strip of every row of the grid on text; the axis title is room the
panel was already spending, so a 4x3 grid gets that height back for the data.
The generated seaborn code says the same thing (`g.set_titles("")`), because
the export must be the figure you previewed.

## Ordering is not cosmetic

Zero-padded IDs (`"01"`, `"02"`, … `"10"`) sort lexicographically into
1, 10, 2 under pandas' default — visibly wrong on an axis, and wrong in a way
that looks like a data problem. `LongTable` carries each factor's real level
order; sources that know better (SciDB knows its declared `schema_key_types`)
supply it explicitly, and everything else falls back to a natural sort.

## Rendering

Two backends translate the same reduced plot, so the interactive view and the
exported figure cannot disagree:

```python
from scistackplot import resolve, render_matplotlib, render_plotly

resolved = resolve(spec, table)          # all reduction happens here
figure  = render_matplotlib(resolved[0]) # export / pipeline — a Figure
payload = render_plotly(resolved[0])     # interactive — a plotly.js dict
```

`render_plotly` builds plain JSON and needs no plotly package.

## Figure size

`StyleOptions.width` / `height` are inches and size the **saved** figure and
the generated code; the interactive preview fills whatever pane it is in.
`scistackplot.figsize` is the one vocabulary of aspect-ratio presets (`4:3`,
`16:9`, `3:2`, golden, `2:1`, `1:1`, two portrait ratios, `custom`): pick a
ratio, name a width — a journal column is 3.5 in, a double column 7.2 in — and
`height_for` gives the height. `aspect_name` runs the other way, so a reopened
spec reports the ratio it was saved with, and `render_matplotlib` logs the
size it drew at INFO. The Plot Studio panel's "Figure size" section is this
module with a dropdown on it.

## Export: real code, not a call back into this library

```python
from scistackplot import generate_plot_function

print(generate_plot_function(spec, table))
```

```python
def plot_steplength(df, filename):
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns

    g = sns.catplot(
        data=df,
        x='session',
        y='StepLength',
        hue='limb',
        kind="box",
    )
    g.set_axis_labels('session', 'StepLength')
    return g.figure
```

Your pipeline gets ordinary seaborn code it can keep, edit, and read — no
runtime dependency on this package. The spec is embedded in the docstring, so
the GUI can reopen a figure you have since hand-edited.

## Data sources

`DataSource` is a three-method protocol (`describe`, `get_table`,
`joinable_with`). `scistackplot` ships `CsvSource` and `DataFrameSource`;
[`scistackplotdb`](../scistackplotdb/README.md) ships the SciDB one. Anything
consuming the protocol — including the Plot Studio panel in the SciStack GUI —
works identically against a lone CSV and a full project database.

## Relationship to SciDB endpoints

Recording a figure is SciDB's job and is unchanged: name a function `plot_`,
return a Figure, and `finalized=True` stores it as a queryable record with an
embedded provenance stamp. `scistackplot` supplies the body of that function;
`scistackplotdb` generates the `for_each` call around it.

See [`docs/claude/plotting-library-design.md`](../docs/claude/plotting-library-design.md).

## Optional extras

```bash
pip install "scistackplot[mpl]"          # matplotlib + seaborn (export)
pip install "scistackplot[interactive]"  # plotly Figure objects
```
