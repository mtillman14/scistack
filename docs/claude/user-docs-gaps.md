# User-docs gaps: tested features the user docs don't cover

**As of 2026-11-21, `main` at `b695de40`.** A list of features that exist and
are pinned by tests but have no (or only a one-line) mention in the
**user-facing** docs: `docs/` minus `docs/claude`/`design-review*`, plus the
package READMEs. Almost every item already has an engineering note in
`docs/claude`, so the gap is in what users read, not in what we know.

Relationship to [docs-ground-truth-test-map.md](docs-ground-truth-test-map.md):
that note maps *existing doc topics* → the tests that define them. This note
lists *features with no doc topic yet*. When a gap here gets a user doc, delete
its row here and, if it is a new topic, add it to the ground-truth map.

The anchor for each row is its **test file**: per the ground-truth map, tests
win over prose, so write the user doc from the test, not from memory.

## How this list was made (re-run it, don't trust it blindly)

1. Feature candidates: test file names in every package's `tests/`, the last
   ~40 commit subjects, and the `sub.add_parser(` names in
   `scidb/src/scidb/inspect/cli.py`.
2. For each, a case-insensitive grep of the user docs for the name a *user*
   sees (the kwarg, the GUI label, the CLI word), then reading the test's
   module docstring for anything with ≤1 hit, to drop internal invariants.
3. CLI subcommands: search for `` `name ``, `scidb name` and `scistack name`.
   `docs/api/scidb.md` lists commands in a table without the `scidb ` prefix;
   searching only for `scidb name` gives false gaps.

Shell traps hit while doing it (the shell is zsh): an unquoted
`--include=*.md` fails as "no matches found"; `$U` holding several paths does
not word-split (use an array, `U=(...)`); `*/README.md` inside a string does
not expand; long `.{0,60}` context regexes exceed ugrep's complexity limit
(use `grep -F` for exact checks).

Limits: name matching plus docstrings. A feature that *is* mentioned may still
be described wrongly. The 152 files in `scistack-gui/tests` were not read one
by one; GUI features came from the commit log.

## Gaps

### Checking a pipeline (scidb)

**Closed 2026-11-21.** The user doc for this topic is
`docs/getting-started/checking-your-pipeline.md` (Code) and `.ui.md` (GUI), in
both navs as "Did It Work?". It covers step verdicts, location states, between
keys, `try_step`, the run report, the data view, node chips, outdated cleanup,
scoped exclusions, and the CLI `status FN` / `coverage` / `follow` /
`expected` / `nans` (also added to the `docs/guide/inspect.md` table). Add new
pipeline-checking material to that page, not to a new one.

Still open on this topic:

| Feature | Anchor tests | Engineering note | User doc target |
|---|---|---|---|
| Expected locations: the sharing rule, declared values, excluded beats everything | `test_expectation.py`, `test_coverage.py` | `expected-locations.md` | "Did It Work?" (code page; the GUI popup is hidden) |
| `filter_report`: per location, does a `where=` filter pass, fail, or have no value | `test_filter_report.py`, `scistackplotdb/tests/test_where_check.py` | `where-in-gui.md` | `docs/guide/filters.md` |
| Data preview from code (`scidb.inspect.preview`); the GUI's Preview… is documented | `test_data_preview.py` | `data-inventory.md` | `docs/guide/inspect.md` |
| Failure blame categories (user code / setup / user value / no data / internal) | `scifor/tests/test_blame.py`, `blame_cases` | `error-surfacing.md` | `docs/guide/reading-your-log.md` names it only |
| The `scistack-gui` Activity Bar tree (steps needing a run, Exclusions (N)) | `extension/src/pipelineTreeCore.test.ts` | `pipeline-status.md` | "Did It Work?" GUI page |

### Moving projects between machines and people

**Closed 2026-11-21.** `hide_path_locations` is documented in
`docs/sharing/export-import.md` and `.ui.md` ("Hide where your raw files are",
plus the import's `--path-root` / Choose my folder… note). Machine roots are
in `docs/getting-started/project-files.md` ("Folders that live somewhere
different on each computer").

Found while writing them:

- Machine roots are **not** expanded in `db` (`scidb/inspect/cli.py`
  `_config_db` uses `Path(db)`) or in a PathInput's `root_folder`
  (`PathInput.__init__` uses `Path(root_folder)`). The user doc says so.
  Expanding `root_folder` is the obvious next step if users ask, but it's not
  built.
- A canvas note quoting a folder **refuses** a hiding export
  (`scistack-gui/tests/test_bundle_hide_locations.py`); it is not rewritten.
  Run history free text and logs are rewritten.
- `[outputs] folder` (`scifor/project/outputs.py`) has no section of its own
  in `project-files.md`: an open gap.

### CLI subcommands not named anywhere in the user docs

`intent`, `seams`, `keys`, `key-types`, `set-key`, `set-key-type`, `mark` /
`marks` / `unmark`, `pins`, `tombstones`, `purge-trash`, `restore-deleted`.
(`status FN`, `coverage`, `expected`, `follow` and `nans` were added
2026-11-21.) Anchors: `scidb/tests/test_inspect_cli.py`,
`test_inspect_phase3..6.py`, `test_cli_seams.py`, `test_coverage.py`,
`test_key_rename.py`, `test_schema_key_types.py`. Target: the command table in
`docs/api/scidb.md`.

### GUI (`docs/gui/*.ui.md`)

| Feature | Commit / issue | Engineering note |
|---|---|---|
| Clear All Data, with its backup | `4c15d44c` | `deleting-entries-with-data.md` |
| Merge node on the canvas | `64d82e7c` | `merge-node.md` |
| Delete any sidebar entry with its data; restore with Ctrl Z | `9773ca12` (#26) | `deleting-entries-with-data.md`, `undo-redo.md` |

**Hidden GUI entries go stale silently.** `SHOW_LEGACY_DATA_ENTRIES = false`
(`frontend/src/components/DataView/openDataView.ts`) hides 📋 Inventory,
✅ Coverage and 🗂 View Schema Locations. On 2026-11-21, `process.ui.md`,
`visualize.ui.md`, `best-player.ui.md` and `best-popcorn.ui.md` still sent
readers to them; they now point to ▦ Data. When a GUI entry is hidden, grep
`docs/**/*.ui.md` for its label. Not yet updated: the screenshot
`docs/images/example_best-popcorn/right_click_menu.png` shows the old menu.

Already documented (not gaps): the data view, run report, node chips (all on
"Did It Work?"), deleting a variant (`process.ui.md`, Variants…
→ Delete), `scistack verify` (`docs/sharing/export-import.md`), Show sample,
presets, saved plots, `plot_` / `stat_` endpoints.

### Plotting (scistackplot / scistackplotdb)

| Feature | Anchor tests | Engineering note |
|---|---|---|
| "Not plotted": what a figure leaves out, and why | `scistackplot/tests/test_omissions.py`, `scistackplotdb/tests/test_not_plotted.py` | `not-plotted.md` |
| Dash styles for an uncoloured grouping layer on lines/bands | `test_dash_styles.py` | `plot-colors.md` |
| Saved plots remember which code drew them; "switch to latest" | `scistackplotdb/tests/test_saved_plot_versions.py` | `saved-plots.md` |
| Field coverage warning (field numeric in only a few records) | `scistackplotdb/tests/test_field_coverage.py` | none |
| Mark weights (`sample_weight`, `line_weight`) | `test_mark_weights.py` | `show-sample-overlay.md`, `spaghetti-plot.md` |
| Only in `scistackplot/README.md`, missing from `visualize.ui.md`: histogram (rug, special bins), KDE, Compare to reference, difference bars, per-panel overrides, y limits, figure size | `test_histogram*.py`, `test_kde.py`, `test_compare*.py`, `test_difference_bars*.py`, `test_panel_overrides*.py`, `test_ylimits.py`, `test_figsize.py` | `histogram-and-regression.md`, `compare-to-reference.md`, `difference-bars.md`, `per-panel-overrides.md`, `y-limit-scoping.md`, `figure-size.md` |

### Statistics (scistackstats / scistackstatsdb / csv-stats)

| Feature | Anchor tests | Engineering note |
|---|---|---|
| Variant sweep / specification curve | `scistackstats/tests/test_sweep.py` | `statistics-design.md` |
| Guided wizard and its plain-language wording (`explain`, `guide`) | `test_explain.py` | `statstudio-guided.md` |
| Incomplete units (`Analysis.missing`) and test recommendation | `test_missing_and_recommend.py` | `statistics.md` |
| Covariates for the mixed model | `scistackplotdb/tests/test_covariates.py` | `statistics-design.md` |
| SPM on 1-D curves (one line in `summarize.ui.md`) | `test_spm.py` | `statistics-design.md` |
| No user API page for either package (READMEs are 6 and 4 lines) | `scistackstats/tests/*`, `scistackstatsdb/tests/*` | `statistics-design.md` |

### MATLAB

| Feature | Anchor tests | User doc target |
|---|---|---|
| `scidb.Log` / `scifor.Log` logging API | `scimatlab/tests/test_matlab_log_api_surface.py` | `docs/matlab-setup.md` or `scimatlab/README.md` |

## Tested but internal: no user doc needed

Don't add these to user docs; `docs/claude` covers them. Save plan
(`test_save_plan.py`), `to_key` hashing, record granularity, the one-variant
rule, run origin, recorded output order, orphaned records, the PathInput
listing cache, the `step=` label, function roles, DuckDB transactions and
codec storage.
