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
remains at each mark is the sample, and **every kind draws the sample**:
bar and band draw its centre ± spread, box and violin its distribution,
scatter and strip one point per sample row, line one line per sample level.
(A spaghetti whose sample cannot be joined across its x axis, such as trials
under session ticks, draws each line through their mean.)

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

## Save the data behind a figure

The rows a plot is drawn from are available as a long table, so the
statistics run on exactly what the figure shows:

```python
from scistackplot import plot_data

data = plot_data(spec, table)                  # the plotted sample
data.to_csv("step_length.csv", index=False)
deeper = plot_data(spec, table, depth="trial")  # keep trials, average cycles only
```

Every figure of a Separate-figures fan-out is in one frame, with the figure
keys as columns. The rows come from the same plan and the same sample step
the figure uses: filters, variants and the cell statistic are all applied
already. `data_export_options` lists the depths and the header each one
writes. A struct variable's fields (`ColName`) are written one column per
field by default; pass `fields_as_columns=False` for one row per field.
Scalar plots only; a raw 1-D or 2-D plot is refused with the reason.
See `docs/claude/plot-data-export.md`.

## Reopen a spec an older version wrote

A spec saved last month may name settings this version has renamed or
removed. `PlotSpec.from_dict` is strict and refuses it. `restore_spec`
keeps everything that still reads and tells you what did not:

```python
from scistackplot import reconcile, restore_spec

restored = restore_spec(stored_dict, fallback_measure="StepLength")
for note in restored.notes:           # e.g. "style.widht: 'widht' is no longer a setting; ignored"
    print(note.path, note.kind, note.message)

checked = reconcile(restored.spec, table)   # names today's data no longer has
```

It never migrates. A renamed setting takes its default and gets a note. It
never raises, except when there is no usable measure and no
`fallback_measure`. `reconcile` removes `show_sample` keys and a
`sample_color` that are no longer factors, because the figure would refuse
them. It reports stale roles and leaves them in place, because resolve drops
them itself. See `docs/claude/saved-plots.md`.

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

## Legible x labels

Crowded tick labels are fitted, not left to collide. One pure function,
`scistackplot.fit_labels`, decides for the whole figure (the most crowded
panel decides for every panel), trying the least destructive fix first:

1. **Numbered labels** (every label is, or ends in, a number) drop the prefix
   they share: `SS01 … SS40` → `01 … 40`. Names are never touched.
2. Wrap at `_ - / . space` onto two lines.
3. Shrink the font, never below 8pt or 70% of the tick size.
4. Rotate 45°, then 90°.
5. Numbered labels only: show every k-th, keeping each bracket's first and
   last. Names stop at step 4 and the log WARNs that they still overlap.

The matplotlib export measures the real text; bracket rows are placed a fixed
number of points below the fitted tick labels and fitted too (shrink and wrap
only). A nested axis has **no x title**: its tick and bracket rows already name
every level. `StyleOptions.hide_legend_ticks=True` blanks the labels of a
layer that is also the colour while the legend lists it. Any of the fitted
settings can be fixed instead: `tick_rotation`, `text.x_ticks`,
`tick_every` (None = fitted). A fixed value is kept even where it overlaps, and
the fit reports that it does. The generated seaborn code replays the fit as
operations on seaborn's own labels, and saves without `bbox_inches="tight"`. See
`.claude/plan-tick-label-legibility.md`.

The exported legend fits too. At the right it may take 30% of the width
(`LEGEND_BUDGET`); past that the title wraps at " / ", the line samples
shorten and the text shrinks (never below the tick labels' floor). Still too
wide, or leaving the x labels no room, it moves below the panels in as many
columns as fit. `PlotSpec.sample_in_legend=False` (the Show sample pane's
"Show in legend") leaves the overlay's colours out of it.

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

`write_figure(resolved, path, dpi=200)` writes the file at exactly that size.
There's no whitespace trim: it refuses `bbox_inches`, and the renderer fits the
labels and legend inside the canvas. Anything still drawn past the edge is
measured (`canvas_overflow`) and WARNed. The file is never resized to make room.
The GUI's Save goes through it.

The preview never decides for itself. `layout_decisions(resolved, width_in=,
height_in=)` lays the figure out with matplotlib at a size and returns the tick,
bracket and legend decisions; `render_plotly(resolved, decisions=...,
fixed_size_px=...)` draws exactly those. The Plot Studio previews at the export
size (drawn at Width x Height, 1 pt = 1 px) or at the pane's size ("Fit pane").

`StyleOptions.text` (`TextSizes`) sizes each piece of text separately:
`base` (points, default 14) is matplotlib's `font.size`, and `title`,
`x_label`, `y_label`, `x_ticks`, `y_ticks`, `groups` (the bracket rows),
`legend` and `legend_title` are each either fixed or `None`, which derives
them from `base` with matplotlib's own ratios. Only `base` set draws exactly
what one font knob would. A fixed size is never shrunk by the label or legend
fit, which may still rotate, wrap or move it. `scistackplot.textsize` is the
one owner (`resolve_sizes`, `rc_params`). The export, the generated code and
the plotly preview (as px) all read it, under `rc_context` so nothing leaks
into the next figure. See `docs/claude/plot-text-and-labels.md`.

## What the text reads as: display aliases

A figure shows the data's own names, such as `BL`, `F` and `StepLength`.
An alias says what one reads as, and it changes only the text:

```python
from scistackplot import Alias

spec = PlotSpec(
    measures=["StepLength"],
    roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
    groups=["session"],
    aliases={
        "session": Alias(name="Visit", levels={"BL": "Baseline", "POST": "After"}),
        "StepLength": Alias(name="Step length (cm)"),
    },
)
```

`name` is used where the figure says what a thing is (axis and legend
titles). `levels` is used wherever a value appears: ticks, brackets,
legend entries, panel titles and figure titles.

A scidb-backed table also carries the project's aliases (`[aliases]` in
`scistack.toml`, read live through `scidb.aliases`). The plot's own entries
override them field by field, and `""` shows the raw text again.

Only the text changes. Filters, facet layout rules, the level order and
`plot_data` all keep raw levels, and so does the data the exported code
draws. The exported code writes the merged aliases in as a literal and
relabels what it drew. Two levels of one factor that would read the same are
refused (`AliasError`), because the figure could not tell them apart.
`scistackplot.aliases.DisplayText` (on `ResolvedPlot.text`) is the one owner
of how every level and name reads. See `docs/claude/plot-text-and-labels.md`.

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
