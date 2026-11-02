# "Not plotted": what a figure leaves out

Built 2026-11-01 from `.claude/plan-plot-omissions.md`. Tests:
`scistackplot/tests/test_omissions.py`, `scistackplotdb/tests/test_not_plotted.py`,
`scidb/tests/test_inventory.py::TestAbsentLocations`, frontend `notPlotted.test.ts`.

The user's rule: data is never dropped silently. Every figure says what it left
out and why.

## Reasons (owner `scistackplot.reduce.omissions`)

| reason | meaning | lost? | decided in |
|---|---|---|---|
| `no_value` | the record exists but its sample value is empty (any of y / x) | yes | `omissions.no_value` on the sample rows |
| `no_record` | the dataset has the location, but this variable has no record there | yes | `scidb.inventory.absent_locations`, then `LongTable.absent`, then `omissions.no_record` |
| `outside_range` | a histogram value outside the typed From / To range | yes | `omissions.outside_range`, using `bins._scistack_regular` |
| `nan_elements` | a 1-D record with NaN entries | no (thinned) | `omissions.nan_elements` on the records |
| `partly_averaged` | a collapsed value averaged from fewer members than exist | no (thinned) | `omissions.partly_averaged`, raw rows vs sample |

Some things are never listed, by design:

- Rows removed by the location filter, Filters or exclusions. `LongTable.absent`
  goes through `reduce.apply_filters`, and `absent_locations` drops excluded
  locations.
- A histogram's NaNs while its NaN bar is shown, because they are drawn.
- Text measures: a missing value is its own category (`table.category_text`),
  so only `no_record` can apply to one.
- A custom `plot_` kind gets `no_value` and `no_record` only.

"A location" is a combination of the variable's schema keys that some `_schema`
row projects onto. It is never the Cartesian product: a combination nobody
ever saved at is not a place.

**Which of the two a NaN is depends on the sample.** A bar whose sample is
trials (trial the only collapsed key) leaves each empty trial out of its
subject's mean: that is `no_value`, one per trial. Only when a key DEEPER
than the sample is averaged away first (trials into subject means, subjects
the sample) is the subject's value `partly_averaged`, and a subject whose
trials are all empty is `no_value`.

## Flow

1. `reduce._build_figure` captures `raw_frame` (before the collapse chain),
   `sample_frame` (after it) and `records_frame` (a 1-D figure). Then
   `_omissions` builds the report, puts it on `ResolvedPlot.omissions`, and
   `omissions.log_report` writes one INFO line, or a WARN when nothing is
   drawn.
2. `OmissionReport` owns the counts, `button_text()` ("Not plotted: 3 of
   120", or "Partly plotted: 2") and `sentence()`. `to_dict` caps the items
   at 5,000 (`truncated` says how many more).
3. `plot_service` puts each figure's report on its payload (`omissions`) and
   the fan-out total on the response (`not_plotted`, via `omissions.combine`).
4. GUI: the toolbar button shows only when `not_plotted.button` is non-empty.
   `NotPlottedPopup` is a table plus "Show as a footnote on the figure"
   (`StyleOptions.not_plotted_note`) and "Save as CSV"
   (`export.NOT_PLOTTED_DEPTH`, via the "Save data" job and
   `export.not_plotted_table`, never truncated).
5. The footnote: `render.base.not_plotted_note` is the one reader. mpl
   (`_draw_not_plotted_note` + `_reserve_bottom`) and plotly (an annotation
   with a grown bottom margin) both draw it, so saved images have it too.

## Known limits

- The **exported code** does not draw the footnote. Its text depends on the
  data the endpoint is run on, and recomputing it in the export would need a
  second owner.
- A 1-D measure **collapsed to one value per record** (cell statistic) reports
  only its empty records as `no_value`. Partial NaNs inside its arrays are not
  listed.
- A **2-D (matrix) heatmap's** NaN cells are not listed. A grid heatmap's
  empty cells are `no_value` on the sample rows.
- `no_record` is attached for single-variable tables only. Stacked variant
  rows of several variables have none.
