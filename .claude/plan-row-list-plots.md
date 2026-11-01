# Plan: plotting "combine rows" (row_list) variables

**Status:** steps 1–3 built 2026-10-30 (user: "go ahead with steps 1-3").
The Python tests have not been run yet. See the build log at the end.
Related: `.claude/plan-combine-duplicate-locations.md` §6 (the decision this
revisits), `docs/claude/synthetic-factors.md`, `docs/claude/measure-shape-and-collapse.md`.

## What the log shows (scistack-debug.log, 2026-10-30 17:11–17:14)

Variable `LoadedData_Subject`: 33 subjects × 86 fields, melted into one measure
plus the `ColName` field factor (2838 rows). The save at 16:53:22 combined
34 rows into 33 and made ~all fields `row_list` (FIN, DIAGNOSIS, ADMIT_DT_TM, …).

1. **Line (the default).** `shape=1d kind=line roles={ColName: facet, subject: iterate}`
   - Because the row_list cells arrive as arrays, the whole measure is 1-D.
     A 1-D variable defaults to every key ITERATE, so you get 33 figures, one
     per subject.
   - Each figure draws that subject's FIN cell as a series against its
     element position. That is the "index" x label.
   - Most subjects have one FIN (`1 row(s) -> 1 sample(s)`). A line with one
     vertex draws nothing, and its x range collapses around 0, which is why the
     ticks pile up in the middle.
   - Subjects with no FIN render `0 row(s)`.
   - The y-limits put FIN at about 6.2–6.9 × 10⁶, which matches the
     `offset '1e6'` on the y ticks.
2. **Scatter.** It switches to a scalar kind, so the 1-D cell collapse (mean)
   runs. `apply_cell_collapse` runs on all 2838 rows (every field) before the
   `ColName` filter narrows to FIN. `series_stats.cell_array` then casts a
   text field to float and raises:
   `could not convert string to float: 'Quadriplegia, C5-C7 incomplete  (G82.54)'`
   (runs 0c164454 and e11392c1). The earlier `'6/24/2026'` failures are the
   same cast, on a date-text field.

## Root causes

- **R1 (design).** A row_list element is a source ROW, not a sample of a
  series. Reading it as 1-D gives the wrong axis (index), the wrong default
  roles (everything iterates), and the wrong summary (a mean where you
  expected one point per row). §6 chose "keep 1-D". Real data shows that
  choice is wrong for this case.
- **R2 (bug).** The cell collapse runs before the field-factor filter, and it
  cannot survive a non-numeric field. One text field among 86 breaks every
  scalar kind, even when that field is not selected.
- **R3 (bug).** A one-vertex line has no visible mark, and its x range is
  degenerate.

## Proposal

### 1. Row lists explode into rows (owner: scistackplotdb reader; meaning: sciduckdb)
- At load time `is_row_list_meta` already tells the reader which columns are
  row lists (`_log_row_list_columns`). Explode each row-list cell into one row
  per element, with a synthetic factor **`Row`** (1-based, in source-row
  order). This keeps the combine rule's alignment: position i means row i
  across every list column. A one-element cell becomes one row with Row=1.
- The measure becomes SCALAR, so ordinary roles apply:
  - Scatter: 1–2 points per subject.
  - Lines: x or series = Row joins a subject's two points.
  - The CSV export works.
- `Row` is a synthetic factor like `ColName`: it is listed in
  `synthetic-factors.md`, and `is_field`-style detection has one owner.
- A column of genuine arrays is never exploded. Only `row_list` meta counts.

### 2. Field filter before the cell collapse, and no crash on text (owner: scistackplot)
- Apply the field-factor (`ColName`) include filter before
  `apply_cell_collapse`. It selects which measure is drawn, so it is not a
  value filter, and measure range filters stay after the collapse.
- In `cell_array`, a cell that cannot be numeric is refused per field with a
  readable message ("ColName=DIAGNOSIS holds text, …"), never a raw
  `ValueError` from numpy. Log a WARN with the field, a count and examples.

### 3. Lone points are visible (owner: scistackplot render)
- A line series with a single sample draws a marker.
- A degenerate x range (one distinct x) is padded symmetrically, so ticks
  don't pile up.

### Logging (NOTE 2)
- INFO per load: `exploded row-list column(s) [...]: N cell(s) -> M row(s)`.
- WARN for non-numeric cells, per field.
- DEBUG when a lone-point line gets a marker.

### Tests
- scistackplotdb: a row_list column loads as scalar rows plus `Row`; a
  genuine array column is untouched; one-element cells become a single row.
- scistackplot:
  - With one text field and one numeric field, scatter of the numeric field
    resolves.
  - Selecting the text field gives the readable refusal.
  - A one-sample line has a marker, and the x range is not degenerate.
- gui `test_plot_service`: the capability report for a row_list variable
  offers scalar kinds and `data_export.available`.

## Decisions (user, 2026-10-30)

- **`Row` is view-only.** It exists only in Plot Studio's in-memory table
  and is never saved. A save-side `Row` level that would replace combine
  rows and the duplicate-location error was considered and rejected. The
  reasons:
  - it masks accidental duplicates as replicates (pseudo-replication);
  - positional identity shifts when the input is re-sorted;
  - a Row number means nothing across variables in the project-wide schema.
- **No new remedy in the duplicate-location popup** (an "add a schema level"
  choice was offered and declined).
- In the motivating dataset, FIN is not unique per encounter, so there is no
  meaningful key to save under. Combine rows plus the view-only `Row` stays
  the approach.

## Open points found after the first draft

- **`Row` default role.** Scatter draws one point per sample and averages
  collapsed levels, so `Row` must default to NOT collapsed (it is the sample).
  Otherwise scatter would still draw one mean per subject.
- **Generated code must match the preview.** "Export code" / "Add to
  pipeline" load through scidb, where row lists arrive as plain lists. The
  explode has to have one owner that both the preview and the generated code
  call. First task: find where the generated code loads its data.

## Questions for the user
- **Q1.** Should row-list elements become rows with a `Row` factor
  (recommended), or stay 1-D? Even if they stay 1-D, R2 and R3 get fixed.
- **Q2.** Is `Row` the right factor name?

Answered 2026-10-30: Q1 is yes (rows plus a `Row` factor). Q2: `Row` is kept.

## Build log (2026-10-30)

**Step 1: Row.**
- New module `scistackplot/rows.py`, the one owner of the split:
  `explode_row_lists`, `element_values`, `row_origin`, `row_factor`,
  `ROW_FACTOR`.
- `scistackplotdb.load.VariableFrame.row_list_columns` now comes from
  `_row_list_columns`, which replaced the DEBUG-only logger.
- `sample_value` classifies a row list by one element, so `describe` says
  scalar.
- `ScidbSource._melt_fields` classifies row lists by their elements, so text
  row lists are dropped like text fields.
- `_build_table` splits after the melt or rename, through
  `_split_row_lists` (with an INFO line and a timer). Row gets depth = levels
  + 1 and a `row_list` origin.
- x-y and stacked plots: WARN, not split.
- Codegen: `_row_split_lines`, after the melt.

**Step 2 (changed from the draft).** The field filter was NOT moved before
the collapse. The collapse-before-filters order is load-bearing (range
filters), and codegen mirrors it. The root fix instead:
- `shape.classify_value` treats a text ndarray as not a series, which is the
  same rule as for a list. Text fields never melt in.
- `series_stats.cell_array` returns a gap instead of raising.
- `cell._warn_non_numeric_cells` WARNs with a count and examples.

**Step 3.**
- `render.base.is_lone_point`, `LONE_POINT_MARKER(_PT)` and
  `lone_point_marker_code`. Used by `mpl._draw_lines`, `plotly_._line_traces`
  and codegen (after `relplot`).
- `AxisText.limits` (export x range: linear, non-date axes only) is applied
  by `plotly_._apply_axis_text` when the preview set no range itself. Note:
  this applies to every numeric x axis whose ticks are copied, not only the
  one-point case.

**Tests:** `scistackplot/tests/test_rows.py` (new), and
`scistackplotdb/tests/test_row_list_columns.py` (rewritten, end-to-end
endpoint included), plus `test_an_unavailable_export_logs_why`.
