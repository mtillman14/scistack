# Plan: show what a plot leaves out ("Not plotted")

Status: DRAFT 2026-11-01, waiting on the user's answers to the open questions.

## Problem

Plot Studio leaves data out without saying so (rule: [[feedback_never_drop_data]]):

| # | What is left out | Where it happens today | Reported? |
|---|---|---|---|
| A | A whole row whose value is NaN / NULL / None (numeric or text) | `reduce._panel_frame` `out.dropna(subset=[Y])` (~2316), overlay (~2202), series (~2566), grid heatmap (~2440) | no |
| B | NaN elements inside a 1-D array | `reduce._explode_1d` `dropna(subset=[measure])` (~1185) | no |
| C | NaN members averaged away by a collapse (mean of 3 of 5 trials) | `reduce._collapse_levels` groupby `.agg("mean")` skips NaN | no |
| D | No record at all for a location the dataset has | not in the plot table; `scidb.inventory.schema_inventory` knows | only in the Data inventory popup |
| E | Removed on purpose by the location filter, Filters or schema exclusions | `reduce` location filter (~1029), filters | DEBUG log only |

D and E are different from A–C: E is the user's own choice, so it is listed as "filtered out" and never counted as missing.

## Design (one owner)

- **Owner:** a new pure module `scistackplot.omissions`.
  - `Omission(keys: dict, reason: str, detail: str)`. `reason` is one of
    `no_value` (A), `nan_elements` (B), `collapsed_partial` (C), `no_record` (D), `filtered` (E).
  - `OmissionReport(offered, plotted, items)` has `sentence()` and `n_text()`, worded like `scistackstats.exclusions`. The wording owner is shared, or copied in a deliberate way. Decide this in stage 1, and keep [[feedback_avoid_scifor_scidb_duplication]] in mind.
- **Producers:** the `dropna` / collapse / filter sites above stop dropping in silence. Each one calls the owner and returns what it removed. There is one record per site, and the GUI never derives its own.
- `no_record` (D) comes from `scidb.inventory.schema_inventory` (scistackplotdb is the layer that can call scidb), for the variable plotted, inside the plot's location filter.
- **Carried on** `ResolvedPlot.omissions` (per figure), then the plotly `meta.omissions`.
- **Logging:** one INFO line per resolve, `not plotted: 3 of 120 values (2 NaN, 1 no record) …`. It is a WARN when every value is gone.
- **GUI:** a "Not plotted: 3 of 120" chip in the figure toolbar, shown only when the count is above zero. Clicking it opens a popup table (keys, variable, reason). Use the same pattern as the Data inventory popup, with a CSV export. The panel only displays; the backend decides.
- **Save data (CSV):** optionally writes the omitted rows with a `not_plotted_reason` column.

## Stages

1. The `omissions` owner, plus unit tests (pure, no pandas plumbing).
2. Record A/B/C at the reduce sites, with regression tests. Each one asserts that a NaN row appears in the report and not in the panel.
3. Record E (filters) and D (inventory, in scistackplotdb). Add tests.
4. Carry the report through `ResolvedPlot`, the plotly meta and the RPC. Add the log line.
5. GUI chip and popup, then rebuild both bundles. Add an entry to `docs/gui-manual-testing-todo.md`.
6. CSV option, and docs in `docs/claude/plot-omissions.md`.

## Open questions

1. Which kinds count. Default: all of A–E, reported separately.
2. Is it a list only, or also drawn on the figure (for example a hollow ✕ at the empty tick, or "n = 15/17" under each tick)? Default: list first.
3. Should `filtered` (E) be in the same popup, or left out? Default: in it, as its own section.

## Decisions + status (2026-11-01)

User decisions:

- All reasons are reported separately.
- Surface: a toolbar button and popup, plus an optional footnote, off by
  default.
- Filtered-out rows are left out.
- Custom `plot_` kinds get `no_value` / `no_record` only.
- A histogram's typed range has its own reason, "Outside the bin range".

Status: BUILT, all Python tests pass (user, 2026-11-01). Owner map and known limits are in
`docs/claude/not-plotted.md`. Commands:

    cd scidb && pytest tests/test_inventory.py -q
    cd scistackplot && pytest tests/test_omissions.py -q && pytest -q
    cd scistackplotdb && pytest tests/test_not_plotted.py -q && pytest -q
