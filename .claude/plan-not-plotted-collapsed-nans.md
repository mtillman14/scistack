# Plan: one owner for "what is inside a cell", and "Not plotted" lists collapsed NaNs

Status: BUILT 2026-11-08, pytest passes, GUI unchecked. The owner's home,
`scicanonicalhash`, was decided by the user on 2026-11-08. Found while
building: a THIRD reader (the export's printed helper, `emit_collapse_cells`)
now prints the same rule and is tested over `_cells.EDGE_CASES`; numeric text
lists (`["1", "2"]`) were read as numbers by the plot and are now gaps, like
complex arrays; the non-numeric WARN counts every cell the reader rejects.

## The gap

A 1-D measure (one vector per record: the per-minute values packed into one
array) on a scalar kind (bar, violin, box, strip, scatter) is first reduced to
one value per record (`reduce.cell.apply_cell_collapse`,
`reduction.CollapseCells`, `series_stats.collapse_cells`: `nanmean` /
`nanmedian`). NaN elements are skipped there. A record with 4 of 12 elements
NaN becomes the mean of 8, and nothing says so. Only an empty or all-NaN cell
shows up (as `no_value`, because it collapses to NaN).

`docs/claude/not-plotted.md` "Known limits", second bullet. This is the
user's original incident (per-minute NaNs inside one vector) seen from Plot
Studio instead of from the data.

## The second problem: two readers of a cell, already disagreeing

| Value | scidb `cells.numeric_elements` | scistackplot `series_stats.cell_array` |
|---|---|---|
| list / ndarray of numbers or bools | elements | elements |
| list with `None` | None becomes NaN | None becomes NaN |
| text, dict, other object | not numeric (None) | empty array |
| complex ndarray | not numeric | cast to float, imaginary part silently dropped |
| datetime64 ndarray | not numeric | empty (the cast fails) |
| 0-d numeric ndarray | an array of 1 element | empty (a scalar) |
| empty list / array | empty | empty |

The plot cannot import scidb: it is standalone, with no database. So the
pure part (no I/O, numpy only) moves DOWN, to a package both already depend
on.

## Owner: `scicanonicalhash._cells` (private)

- **Private.** A user never calls it. User-facing surfaces stay where they
  are: `scidb nans`, `Inspector.cell_profile`, the data preview's "Inside
  cells", and Plot Studio's "Not plotted". The module is underscored and not
  in `scicanonicalhash.__all__`. Its docstring names its two consumers.
- **Dependencies.** numpy only, imported by the module. numpy is already
  `scicanonicalhash`'s optional extra, and both consumers install it. Nothing
  imports `_cells` from `scicanonicalhash/__init__.py`, so
  `import scicanonicalhash` costs nothing more. No logging in it: callers log
  (scidb's `cell_profile` timing, scistackplot's collapse line).
- **What moves (from `scidb/inspect/cells.py`):**
  - the kinds (BLANK, EMPTY, ALL_NAN, PARTIAL_NAN, CONSTANT), `PROBLEM_KINDS`,
    `PROBLEM_LABELS`;
  - `numeric_elements`, `_is_blank`, `classify`;
  - `CellStats` (counts plus `describe`, the cell wording), `cell_stats`;
  - new: `nan_count(elements) -> (nan, size)`, the per-cell numbers the plot
    reports.
- **What stays in scidb:** `CellProblem`, `CellProfile`, `cell_profile` (the
  loading, the locations, the logging). `scidb.inspect.cells` imports the
  moved names from the owner and does not redefine them. No re-export shim
  (beta: no deprecation). Callers switch imports in the same change.
- **scistackplot:** `series_stats.cell_array(value)` becomes
  `numeric_elements(value)`, or an empty array when that returns None. Same
  signature and the same 13 call sites, so collapse, 1-D plots and NaN
  reporting read cells one way.

### Decided behaviour for the disagreements (pinned in the owner's tests)

- **0-d array:** a scalar, not an array. This is scistackplot's rule; scidb
  changes, and a 0-d NaN is now `blank`.
- **complex array:** not numeric. This is scidb's rule; the plot changes. A
  complex cell becomes a gap, counted by the existing non-numeric warning
  (`_warn_non_numeric_cells`), instead of losing its imaginary part
  silently.
- **datetime64 array:** not numeric in both (no change).
- **None inside a list:** NaN in both (no change).

## Steps

1. **Name: `cells_owner`.**
   - Create `scicanonicalhash/src/scicanonicalhash/_cells.py` holding the
     moved code.
   - Add the tests `scicanonicalhash/tests/test_cells.py`: each row of the
     table above, each kind, `describe` wording, and `nan_count`.
   - `scidb/inspect/cells.py` imports from the owner. Update `preview.py`,
     `render.py`, `cli.py` and `api.py` imports where they named moved symbols.
   - The existing `scidb/tests/test_cells.py` stays as the consumer test; its
     pure-classification cases move to the owner's tests.
   - scidb already depends on scicanonicalhash. Check that its numpy
     requirement covers the owner (scidb lists numpy itself).
2. **Name: `plot_reads_the_owner`.**
   - `series_stats.cell_array` delegates to `numeric_elements`.
   - Test the complex-cell change and the 0-d case in
     `scistackplot/tests/test_series_stats.py` (or wherever `cell_array` is
     tested).
   - `test_import_cost.py` must still pass.
3. **Name: `cell_collapse_nans`.**
   - Where: `reduction.CollapseCells.apply`.
   - It returns a fourth value, one row per record whose cell had SOME NaN
     elements (`0 < nan < size`, counted by `nan_count`): every column of the
     collapsed row (keys, factors, the collapsed value, so a measure range
     filter still applies) plus `__nan` and `__size`.
   - All-NaN and empty cells are left out: they collapse to NaN and are
     already `no_value`.
   - The exploded source (one row per sample) counts NaN rows per record
     group.
   - The counts are taken in the same loop as the average, from the same
     arrays.
4. **Name: `long_table_side_frame`.**
   - Where: `apply_cell_collapse`.
   - It stores the frame as `LongTable.cell_nans` (None when no cell was
     partial).
   - The INFO line gains "N record(s) averaged over fewer elements (NaN
     inside)".
   - Side frame, not a hidden column: a working column could become a
     grouping key in `_sample_frame`'s chain and split samples. This follows
     the `LongTable.absent` / `no_record` precedent.
5. **Name: `omission_reason`.**
   - Where: `omissions.cell_nans(frame, figure_key, measure, keys, statistic)`.
   - It reuses `NAN_ELEMENTS` (thinned, not lost).
   - Detail text: "4 of 12 elements NaN; the mean of the other 8" ("median"
     for median).
   - Figure wording stays in `omissions`. The counts come from the owner.
6. **Name: `figure_report`.**
   - Where: `resolve._omissions`.
   - Call it on `apply_filters(table.cell_nans, spec)` beside `no_record`,
     for scalar figures, not custom kinds.
   - `partly_averaged` (records averaged into subjects) still applies on top:
     a different fact, so both are listed.
7. **Name: `gui`.** No code. The popup renders reasons by label, the button
   counts thinned items as "Partly plotted", and the CSV reads the report.
   Check by eye.

## Tests

- scicanonicalhash `test_cells.py`: the owner (step 1).
- scistackplot `test_omissions.py`:
  - a violin of a 1-D measure with a partial-NaN cell lists it, with
    "4 of 12" and "mean of the other 8";
  - an all-NaN cell is `no_value` only;
  - a Filter on a factor removes the item;
  - a measure range filter on the collapsed value removes it;
  - median wording;
  - the exploded source;
  - a facet lists only its own records;
  - `cell_nans` is None for clean data.
- scistackplotdb `test_not_plotted.py`: end to end from a saved variable with
  partial-NaN vectors. The item appears, and so does the Not plotted CSV row.
- No parity test between scidb and the plot: there is one reader.
- Order: scicanonicalhash, then scidb, then scistackplot, then
  scistackplotdb, one package at a time.

## Docs

- `docs/claude/not-plotted.md`: move the bullet out of Known limits into the
  reasons table, and name the owner.
- `docs/claude/expected-locations.md` Stage 4 section: the owner moved.
- `docs/gui-manual-testing-todo.md`: a short entry.

## Out of scope

- 2-D matrix heatmap NaN cells (the other known limit).
- Exported code drawing the footnote.
