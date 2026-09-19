# Plan — "Save plot data": the long table a plot is drawn from, as CSV

Drafted 2026-09-19. Status: **ALL STAGES (0-4) BUILT 2026-09-19, uncommitted.
All Python suites pass (user, 2026-09-19); frontend `npm test` passes; both vite
bundles and `extension/dist/extension.js` rebuilt.**

### Deviations while building
* **Spaghetti exception (Stage 0).** A spaghetti line joins one identity
  across its x ticks. Trials under a session tick do not recur ("trial 1 at
  pre" is not "trial 1 at post"), so the canonical "subject lines, trial
  collapsed" setup cannot draw one line per trial.
  `roles.spaghetti_sample_repeats` uses the same depth rule as
  `overlay_join`: when the sample recurs, each sample level is its own line
  (the user's case A); when it does not, `CollapseSteps.final = sample`
  and each line is the mean, logged at INFO. `final` therefore survives,
  for this one case only. The CSV is still the sample rows.
* `data_export_options(spec, roles, table, shape)` is pure over those
  inputs, not built off a plan, so the capability report does not build a
  plan.
* `chain_cut` shared with `overlay_steps`, as planned. The capability
  report's `grouping` gained `units`.
* The extension's `pick_save_path` (plotPanel.ts and dagPanel.ts) takes
  `filterName`, so the CSV dialog filters on "CSV".
* `plot_save_complete` carries `rows` for a data save.

## Goal

From the Plot Studio, save one CSV holding **exactly** the rows the plot's
marks are computed from, so the statistics run on the same data the figure
shows.

Example: `[subject, session, speed, trial, cycle]`, with session grouped, speed
as separate figures, and subject/trial/cycle collapsed. The default CSV is
`subject, session, speed, <measure>`. cycle and trial have been averaged away
(nested, the same way the plot does it), and subject is the sample. When the
user asks for more detail, the same save can keep trial, or trial and cycle.

## Decisions (user, 2026-09-19)

| Question | Answer |
|---|---|
| Fan-out | **One CSV for all figures.** The figure keys (speed) become columns. |
| Depth | **Pick a depth.** "subject — the plotted sample" (the default) / "down to trial" / "down to cycle (raw)". It uses the same cut rule as Show sample. |
| 1-D / 2-D measures | **Scalar only for now.** A 1-D measure reduced to one number per cell (`cell_statistic`) *is* scalar when drawn, so it qualifies. A raw 1-D or 2-D measure is refused, and the reason is shown. |
| Provenance | **CSV only.** No sidecar file and no comment header. |

## What "the data the plot is built from" means, exactly

The collapse chain is `roles.collapse_steps`, which gives `pre → sample →
final`:

* **Default export = the frame after `pre`, before `final` and before
  `_summarize`.** These are the sample rows that bar/band use for
  centre ± spread, that box/violin draw as a distribution, and that
  scatter/strip/line average into one point.
* **No collapsed key:** there is no sample, so the export is the plotted rows
  unchanged (one row per mark). The only depth offered is "as plotted".
* **Pooled ("weight by N"):** `pre` is empty and every collapsed key is the
  sample, so the export already holds every collapsed level unaveraged. The
  depth picker is hidden, and the reason is logged.
* **Depth = key K:** the chain is cut before K, in the same way as
  `roles.overlay_steps`. Only the keys deeper than K are averaged, nested, as
  the plot averages them.
* Filters, variant folding/pins, the cell collapse, glue and the ITERATE
  fan-out are all applied, because the export starts from the same `_plan` the
  figures use.

**Columns:** figure (ITERATE) keys, then facet keys, grouping keys and the
remaining collapsed keys. These are listed outermost-first in schema order,
followed by the variant/`Variable`/`ColName` factors that are present, followed
by the measure column(s), including `x_measure` for an x-y scatter. Internal
`__*` columns and `__rid_*` are never written. Schema key values are written as
their stored strings, so `"01"` stays `"01"` (see memory
zero-padded-schema-keys). Rows are sorted by the table's declared level order.

## Stage 0 — schema-level parity: every kind draws the sample (user, 2026-09-19)

Until now, scatter, strip, line and spaghetti averaged the sample as well
(`CollapseSteps.final`, `MEAN_DRAWING_KINDS`), so a bar of subjects and a
scatter of subjects did not show the same thing. The user's rule now applies:
**every kind draws the sample rows.**

| kind | the sample (e.g. subject) is drawn as |
|---|---|
| bar / band | centre ± spread |
| box / violin | a distribution |
| scatter / strip | one point per subject |
| line / spaghetti | **one line per subject** (seaborn `units=`), inside its colour/dash |
| heatmap | unchanged: a 2-D measure skips the chain, and the panel is the matrix mean |

* `CollapseSteps.final`, `MEAN_DRAWING_KINDS` and `draws_sample_mean` are
  deleted. This is a clean break (beta).
* `GroupingLayers.units` holds the sample keys that line and spaghetti draw
  one polyline each for. The series id is `units + series`. Dash styles and
  the legend come from `series` only, so subject lines are never dashed or
  listed in the legend.
* `ExtentMode.final_mean` is deleted. For these kinds the y limits are
  computed over the sample rows.
* codegen stops emitting the sample's mean. The `_series` column includes the
  units, and `units=` goes to seaborn.
* There is no "draw the mean" toggle (user: no). Bar and band already draw the
  centre.
* Tests: the mean-pinning tests (`test_nested_collapse`, `test_codegen`) are
  rewritten to pin the new rule. New tests cover one line per subject for line
  and spaghetti, and units never dashed.

## Stage 1 — scistackplot: one owner for the sample frame

* Extract `reduce._sample_frame(frame, steps, spec, table, index_column)` out
  of `_build_figure`, which then calls it. That makes the figure and the export
  share the collapse code, instead of mirroring it.
* Generalise the cut: `roles.chain_cut(order, key) -> (averaged, kept)`.
  `overlay_steps` and the export both call it.
* New `scistackplot/export.py`:
  * `data_depths(spec, table) -> DataExportOptions`, which returns
    `available`, `reason`, `depths: [{key, label, averaged}]` and `default`. It
    is the only statement of what the export offers, and `data_unavailable`
    lives next to it (non-scalar shape → reason).
  * `plot_data(spec, table, *, depth=None) -> pd.DataFrame`. It builds the
    `_plan`, runs `_sample_frame` (or the depth cut) over every figure group,
    stamps the figure-key columns, concatenates, orders columns and rows, and
    drops internals.
* Export both from `scistackplot/__init__.py`.
* **Logging:** `Log.timer("plot_data")` with phases `plan`, `collapse`,
  `assemble`. One INFO line: `[plot-data] <measure>: N figure(s), chain
  cycle -> trial -> subject (sample), depth=subject -> R row(s) x C col(s)
  [cols]`. A DEBUG line gives each figure's row count. Any column dropped as
  internal is logged at DEBUG.
* **Tests** (`scistackplot/tests/test_plot_data.py`). These parity tests are
  the reason the feature exists:
  * Bar: for every figure and panel, grouping the CSV by its non-sample columns
    and applying `Aggregation.statistic`/`.error` gives the resolved panel's
    `__y` / `__y_low` / `__y_high` exactly.
  * Box/violin: the CSV rows equal the panel's distribution rows.
  * Scatter/strip (mean-drawing): the mean of the CSV per mark equals the drawn
    point.
  * The unbalanced worked example from `grouping-and-collapse.md`, with both
    nested and pooled: CSV values {2, 9} nested and {1, 2, 3, 9} pooled.
  * Depth: re-collapsing the depth=trial CSV with `_collapse_levels` gives the
    default CSV.
  * Fan-out: every figure key appears as a column, and the figure count equals
    the number of distinct figure-key combinations.
  * Zero-padded keys survive the round trip through `to_csv` and `read_csv`
    with `dtype=str`.
  * A raw 1-D measure is refused with a reason. A cell-collapsed 1-D measure is
    accepted.
  * The depth picker offers nothing when nothing is collapsed or when pooled.

## Stage 2 — scistack-gui backend

* `plot_service.save_plot_data(db, spec_payload, path, *, depth, csv_path)`
  loads through `_load` (the DB is held for the load only), calls
  `plot_data`, and writes with `to_csv(index=False)`. The destination is
  checked first, as `save_figure` does.
* It runs as a **job**, like the image save. `start_save_job` gains a `kind`
  (`"image"` | `"data"`), and the RPC `plot_save_start` gains `what`. The
  messages are the same (`plot_save_progress/complete/failed`), because a large
  load can still exceed the 30 s RPC clock.
* The capability report gains `data_export` (from `data_depths`), so the GUI
  never works out availability on its own.
* **Logging:** the `[plot] saving data …` line gives the path, depth, row and
  column count, and elapsed time.
* **Tests** (`scistack-gui/tests/test_plot_service.py`): the written file
  equals `plot_data(...)`, the job message sequence is correct, a non-scalar
  spec is refused before any file exists, and `data_export` appears in the
  report.

## Stage 3 — frontend

* Add a **"Save data (CSV)"** button beside Save image. When
  `data_export.available` is false, it is disabled and its tooltip gives the
  reason.
* Clicking opens a small inline chooser before the file picker. The chooser
  has depth radios labelled like "subject — plotted sample (default)", "down to
  trial", and "down to cycle (raw)", and each option lists the columns the file
  will have. It is skipped when there is only one depth.
* After the chooser, `pick_save_path` opens with `formats: ['csv']`, and the
  save follows the same adopt-the-job-id-first flow.
* Rebuild both vite bundles. Add entries to `docs/gui-manual-testing-todo.md`.

## Stage 4 — docs

* Write `docs/claude/plot-data-export.md`, covering the definition above, the
  parity tests, and the depth cut shared with Show sample.
* Add a "Save the data behind a figure" section to the `scistackplot`
  README.
* Update the memory entry.

## Out of scope (flagged)

* 1-D/2-D export (long form with a sample index), per the decision above.
* A `stat_`/pipeline counterpart: exported code does not emit the CSV. The
  natural follow-up is a `stat_` endpoint calling `plot_data`, so the
  pipeline's statistics read the same table.

## Commands (user runs)

```
pytest scistackplot/tests
pytest scistackplotdb/tests
pytest scistack-gui/tests/test_plot_service.py
```
