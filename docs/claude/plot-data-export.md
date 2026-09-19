# "Save data" — the long table a plot is drawn from

**Since:** 2026-09-19 (plan: `.claude/plan-plot-data-csv.md`). Read
`grouping-and-collapse.md` first: this feature is defined in its terms.

## Why it exists

The statistics must run on exactly the rows the figure shows. A CSV rebuilt
by hand from the database can differ from the plot in many small ways:
- a filter;
- a variant pin;
- the cell statistic of a 1-D measure;
- nested versus pooled averaging;
- a fan-out the analyst forgot to split by.

"Save data" writes the plot's own rows instead, so the file cannot differ
from the figure.

## What is written

`scistackplot.plot_data(spec, table, depth=None)` returns one long frame for
the **whole fan-out**. Every ITERATE figure is in it, with the figure keys as
ordinary columns.

It starts from the same `reduce._plan` that `resolve` uses. That plan has
already done all of the following:
- folded the variants;
- applied the level groups;
- run the cell collapse;
- applied the filters;
- split the ITERATE fan-out into groups.

Then, per figure, it runs the **same function the figure runs**,
`reduce._sample_frame`, which is the pre-collapse. So the default file is the
sample rows: what a bar summarises, what a box draws, and what a scatter
plots.

Example: `[subject, session, speed, trial, cycle]`, with session grouped,
speed as Separate figures, and subject / trial / cycle collapsed:

| depth | columns | rows |
|---|---|---|
| `subject` (default, the sample) | `subject, session, speed, M` | cycle averaged within trial, then trial within subject |
| `trial` | `subject, session, speed, trial, M` | cycle averaged within trial |
| `cycle` (raw) | `subject, session, speed, trial, cycle, M` | nothing averaged |

A depth is a cut of the one collapse chain, `roles.chain_cut`. It is the same
rule "Show sample" uses: keeping `trial` implies keeping every shallower
collapsed key, because those keys are what identify a trial.

Special cases:
* **Nothing collapsed.** There is one depth (`key: None`, "As plotted"), and
  the rows are the marks.
* **Pooled ("Weight by N").** The sample is every collapsed key at once, so
  the plotted rows already hold every level unaveraged. There is one depth,
  and the picker is skipped.
* **Rows the figure does not draw** (missing measure) are dropped, and the
  count is logged at INFO.
* **Column order.** Factors come first, outermost first by hierarchy depth,
  with depthless factors (Variant, fields, buckets) after them. The
  measure(s) come last, including `x_measure` for an x-y scatter.
* **Row order.** Rows follow the declared level order (`_level_rank`), so
  `"02"` comes before `"10"`. Schema key values are written as their stored
  strings, so `01` stays `01` in the file. A reader must ask for strings
  (`pd.read_csv(..., dtype={"subject": str})`) to keep that.

## Struct / table variables: one column per field

A dict/struct variable with scalar fields reaches the plot **melted**:
`ScidbSource._melt_fields` turns it into one measure column and a `ColName`
field factor (`FactorInfo.is_field`; see `synthetic-factors.md`). The file
spreads that factor back out, **one column per field**, by default
(`fields_as_columns=True`; the GUI checkbox defaults to checked, user
2026-09-19):

```
subject, session, RTA, RMG, LTA        # fields_as_columns=True (default)
subject, session, ColName, Peak        # fields_as_columns=False
```

The rules:
* **Only the field factor is spread.** It is found by its flag, never by
  name, so a `ColName_` renamed to dodge a schema key still works.
* **Only when the chosen depth keeps the field.** Collapsing `ColName`
  averages the fields first, because they sit inside a record. The default
  file then has no field to spread, and the raw depth does.
  `DataDepth.wide_columns` is None exactly when there is nothing to spread;
  `DataExportOptions.field_factor` is set when some depth has something to
  spread. That field decides whether the GUI shows the checkbox.
* **It is a pure reshape** (`export._fields_to_columns`). There is one row per
  combination of the other columns, with a missing key counted as a level. A
  field a record lacks is an empty cell, and no value is averaged, dropped or
  added. Two long rows for one (record, field) is refused.
* **Field columns** follow the declared field order, limited to the fields the
  spec's filters keep.
* **Column names.** A field column is named after the bare field, unless a
  field is named like a factor column, or there is more than one measure (an
  x-y scatter). Then every field column is named `<measure>.<field>`.
* **Logging.** One INFO line records the reshape:
  `[plot-data] one column per ColName: L long row(s) -> R row(s), field column(s) […]`.

## Every kind draws the sample

This feature forced the parity decision recorded in
`grouping-and-collapse.md`. Before it, scatter, strip, line and spaghetti
averaged the sample, so "the data the plot is built from" meant a different
thing for each kind. Now the default CSV is identical whichever kind is on
screen (`test_the_csv_does_not_depend_on_the_kind`).

The one exception is a spaghetti whose sample cannot be joined across its x
axis (trials under a session tick). That kind draws per-line means, but the
CSV still holds the sample rows. The statistics want the sample, and the
drawn mean is recomputable from them.

## What is refused

Scalar plots only (user decision, 2026-09-19). The DRAWN shape decides:
* a 1-D measure drawn by a scalar kind is scalar after the cell collapse, and
  it is exported with each record's cell statistic as its value;
* a raw 1-D (line / band) or 2-D measure is refused by
  `export.data_unavailable` with a reason. The GUI shows that reason as the
  greyed-out button's tooltip.

## Where things live

| question | owner |
|---|---|
| the frame | `scistackplot/export.py` — `plot_data` |
| which depths, which columns, why not | `export.data_export_options(spec, roles, table, shape)`: pure, and read by both `plot_data` and `capability.capabilities` (`data_export` key) |
| the sample rows | `reduce._sample_frame`: one call, figure and CSV |
| the depth cut | `roles.chain_cut` (shared with `overlay_steps`) |
| one column per field | `export._fields_to_columns`, `_wide_header`, `_field_factor` (by `is_field`) |
| writing the file | `scistack_gui/services/plot_service.save_plot_data` |
| the job | `start_save_job(..., what="data", depth=..., fields_as_columns=...)` → the same `plot_save_progress` / `_complete` (+ `rows`) / `_failed` messages as an image save |
| RPC / REST | `plot_save_start` / `POST /api/plot/save` with `what`, `depth`, `fields_as_columns` (default true) |
| the button + chooser | `PlotStudio.tsx`: `saveData`, `openDataSave`, `dataChooser` |

## Tests

* `scistackplot/tests/test_plot_data.py` covers:
  - parity: bar centre and SD recomputed from the CSV, and box / violin /
    scatter / strip rows;
  - the worked example, nested `{2, 9}` and pooled `{1, 2, 3, 9}`;
  - depths re-collapse to the default;
  - fan-out;
  - ordering and zero-padding;
  - refusal;
  - the capability report;
  - the log line;
  - struct variables: wide by default, long on request, wide = pivot of long,
    per-field panel parity, empty cell for a missing field, field filters,
    a collapsed field, and a column-name collision.
* `scistack-gui/tests/test_plot_service.py` ("saving the plot's data")
  covers:
  - the written file equals `plot_data`;
  - folder and suffix handling;
  - refusal as a message;
  - the job's message sequence.

## Logging

* `[plot-data] <measure>: N figure(s), chain cycle -> trial -> subject
  (sample), depth=subject -> R row(s) x C column(s) [cols]` (INFO), plus a
  DEBUG line per figure.
* `[timing] plot_data` phases: `plan`, `collapse`, `assemble`.
* `[plot] saved data of <measure> to <path>: …` (INFO) from the GUI service,
  with its own `save_plot_data` timer (`load`, `plot_data`, `write`).

## Not done (flagged)

* 1-D / 2-D export, which would be long form with a sample index.
* A pipeline counterpart. The exported `plot_` code does not write the CSV.
  The natural follow-up is a `stat_` endpoint that calls `plot_data`, so
  pipeline statistics read the same table.
