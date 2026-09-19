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
    roles={"session": Role.GROUP, "limb": Role.GROUP, "subject": Role.COLLAPSE},
    groups=["limb", "session"],   # innermost first: limbs inside each session tick
    color="limb",
    kind=PlotKind.BOX,
)
figure = render(source, spec)
```

## Every factor does exactly one thing

The whole control surface is one rule: each categorical column carries exactly
one role, in one of two panes (`docs/claude/grouping-and-collapse.md`).

| Role | Meaning |
|---|---|
| `GROUP` | one **mark** per level — a bar, a box, a line. Ordered by `PlotSpec.groups`, innermost first; one layer may be the `color` |
| `FACET` | one subplot per level (arranged by `FacetOptions`) |
| `ITERATE` | a separate **figure** per level — the default for an unmentioned factor |
| `COLLAPSE` | averaged away. The **last** collapsed key is the *sample* |

Collapsed keys average away deepest first, nested and unweighted — "trial
within subject, then subject" — so each subject counts once however many
trials it has (`Aggregation(pooled=True)` is the deliberate alternative). What
remains at each mark is the sample: bar and band draw its centre ± spread,
box and violin its distribution, scatter and line its mean.

Which plot kinds are available follows from that assignment plus the measure's
shape, through one pure function:

```python
from scistackplot import available_plots, default_plot, Shape

available_plots(Shape.SCALAR, {"session": Role.GROUP})                          # scatter, strip, bar
available_plots(Shape.SCALAR, {"session": Role.GROUP, "trial": Role.COLLAPSE})  # + box, violin
```

A distribution needs a sample, and a sample exists only when some factor is
collapsed. That single rule produces both defaults and availability:

| Measure shape | no sample | with a sample |
|---|---|---|
| scalar | scatter, or a bar with no error bar | box / violin / bar + CI |
| 1-D array | one line per leaf group | mean line + shaded error band |
| 2-D | heatmap | mean heatmap |

"Average over trials, then show the spread across subjects" is
`trial=COLLAPSE, subject=COLLAPSE`: trial averages within each subject
first, and subject — the last — is the sample the error bars are drawn over.

**Spaghetti** is the repeated-measures view: one marker per level of the
**first** grouping layer at each x position, joined by a line — `groups =
["subject", "session", "Intervention"]` draws every subject's line across
sessions inside their group. It needs two grouping layers, and the same list
read as a bar plot nests subject bars inside session ticks inside Intervention
brackets. Each subject keeps one small deterministic offset at every x
position (never a random jitter — a line must end on its own markers),
decided once per figure by `scistackplot.spaghetti.series_offsets` and read by
both renderers and the generated code.

**Show sample** puts the data behind a summary on top of it. `show_sample =
["trial"]` on that box plot cuts the collapse chain before `trial`: every
trial of every subject is drawn as a small point inside its box (a deeper
key implies the shallower ones — a trial is a trial *of* a subject), and
`["subject"]` shows one point per subject, the sample itself. Points are
joined into lines when they are repeated measures — the shown key sits above
the x axis's key in the schema, so each subject has a value at every session
— and left as points otherwise; `join_sample=True/False` overrides the rule.
The marks never change, the y axis grows to hold the points, and the exported
code draws the same overlay.

## Arranging the subplots

Faceted panels flow in order by default, wrapping at `FacetOptions.wrap`. When
the arrangement matters, describe it with **rules** instead of positions:

```python
from scistackplot import FacetOptions, MatchOp, Matcher, PlotSpec, Role

spec = PlotSpec(
    measures=["RawEMG"],
    roles={"ColName": Role.FACET, "subject": Role.GROUP},
    color="subject",
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

`StyleOptions.font_size` (points, default 14) is matplotlib's `font.size`:
ticks, axis labels, legend and title all scale from it, applied under
`rc_context` so nothing leaks into the next figure. The plotly preview reads
the same number as px, so the change is visible before a save.

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
