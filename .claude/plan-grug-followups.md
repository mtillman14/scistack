# Plan: grug follow-ups (items 5, 7–13 of the 2026-11-10 suggestions)

Context: `.claude/plan-grugbrain-review.md`, `docs/claude/grugbrain-principles.md`.
The user runs all tests. Each stage is its own commit when the user asks.

## Method for the two big splits (7, 8)

Both are **moves, not rewrites**. A script cuts top-level blocks out of the file
verbatim and writes them into new modules. The imports each new module needs are
generated from the names it uses. A function-level dependency check proves the
modules form a DAG (no module imports one above it), so no new import cycles.
The old module re-exports every name that other code or tests import from it, so
`from scidb.run.foreach import _x` keeps working. Checks: pyright for undefined
names (no Python is run), `scidb/tests/test_imports.py` (a fresh interpreter per
module) for cycles, then the user's suites.

The trap with moving code is monkeypatching. A test that patches `module.f` only
reaches callers that look `f` up in that module. Two tests do this:
`foreach._capture_function_source` and `foreach._load_var_type_as_spread`. They
are updated to patch the module that now holds the callers.

## Stages

### prepare_run_split (item 9)
`run_service._prepare_run` (about 230 lines) → `_derive_targets`,
`_resolve_variants`, `_resolve_run_options`, `_resolve_level`. Same log lines,
plus a phase timing line for each.

### foreach_modules (item 8)
`scidb/run/foreach.py` (6,822 lines, 97 functions) → modules by role, lowest first:
`foreach_common` (state types, shared helpers, per-combo resolution),
`foreach_stamping` (endpoint artifact stamping), `foreach_load` (input loading and
conversion), `foreach_save` (save plan and apply), `foreach_prepare` (the
prepare/save seam and expansion). `foreach.py` keeps the entry point, the execute
phase, introspect and the skip hook. The exact assignment is function by function,
whatever the DAG check allows.

### database_mixins (item 7)
`DatabaseManager` (89 methods) → mixins by method cluster, one module each:
`db_schema_keys` (key types, order, aliases, colours, distinct values),
`db_save` (single save, columnar/native storage, column-set checks), `db_save_batch`,
`db_load` (find/load/load_all_as_df and the bulk engine), `db_versions` (versions,
exclusions, variants, provenance lookups). The private module-level helpers move to
`db_helpers`. `database.py` keeps the public functions (`configure_database`,
`get_database`, `using_db`, …), `__init__`, export, variable groups and lifecycle.
The public API is unchanged: same class, same methods.

### lazy_imports_touched (item 10)
In the files the stages above create or touch: hoist the function-level imports
that dodge no cycle. Re-measure the counts as information.

### docs_dedupe (item 11)
For each group in `docs/claude/README.md`, find docs on the same concept. Merge,
or move the superseded one to `docs/claude/archive/` with a pointer at the top of
the live one. Regenerate the index.

### testids (item 12)
Add `data-testid` to the controls the browser tests use (run button, canvas nodes
by kind, Plot Studio open button, rail groups, palette, the backlog features'
controls). Switch the e2e tests to them.

### backlog_e2e (item 5)
Browser regression tests for the older "pytest passes, GUI unchecked" items where
the browser GUI can reach them: expected locations and coverage, the unified
Process/Plot/Stats design row, palette order and delete, move node to canvas. Each
test names its manual-doc item. Extension-only steps stay manual.

### ci_e2e (item 13)
A CI job that installs Playwright and Chromium and runs `scistack-gui/tests/e2e`.
It runs on PRs and on main, and the release workflow requires it.

## Status (2026-11-10, written; tests to be run by the user; uncommitted)

| Stage | Status | Notes |
|---|---|---|
| prepare_run_split | done | `_prepare_run` is about 60 lines over `_derive_targets`, `_resolve_variants`, `_resolve_run_options` (`_RunOpts`), `_drop_hidden_targets`, `_resolve_level`. Each logs `phase prepare.<step> done in N ms`. New test: `test_prepare_steps_are_logged_and_no_targets_ends_the_run`. |
| foreach_modules | done | foreach.py 6,822 → about 1,600 lines: `foreach_common` (679), `foreach_stamping` (516), `foreach_load` (1,088), `foreach_save` (807), `foreach_prepare` (2,474). A function-level check confirmed zero upward references, after it learned to read f-string expressions and quoted annotations. Every moved name is re-exported from foreach.py. Logger name unchanged (`scidb.run.foreach`). Two monkeypatch tests now patch the module that holds the callers (`foreach_save`, `foreach_stamping`). |
| database_mixins | done | database.py 5,345 → 992 lines. Private helpers in `db_helpers` (799). Mixins: `SchemaKeysMixin` (db_schema_keys), `SaveMixin` (db_save), `BatchSaveMixin` (db_save_batch), `LoadMixin` (db_load), `VersionsMixin` (db_versions). Each moved method's `self` is annotated `"DatabaseManager"`, so the IDE keeps autocomplete; pyright reports zero errors in the mixins. `get_aggregated_variants` stays in the core class (it calls the public `aggregate_pipeline_variants`). `test_table_registry` now covers DatabaseManager's files (the per-variable DDL moved to db_save/db_save_batch). |
| lazy_imports_touched | done | 136 → 61 function-level imports in the files above (scidb total 568 → 502). Hoisted only when there is no cycle AND the module is already loaded by `import scidb` (no added import cost). External libraries stay lazy. Skipped by rule: empty-block cases, a plain `import x`, and 7 targets `import scidb` doesn't load. |
| docs_dedupe | done | 9 docs to `docs/claude/archive/`, each with a pointer to its replacement: matlab-gui-support-analysis, gui-vscode-extension, gui-readiness, manual-edges-on-history-nodes, layer-friction-analysis, lineage-design, lineage-wrapper-types, gui-export-to-plain-python, for-each-kwargs (merged into for-each-kwarg-parity). 42 references rewritten. 149 live docs, index regenerated. Kept as pairs by design: statistics-design/statistics, the endpoint design and as-built docs, the variant and filter docs (different questions). |
| testids | done | `data-testid` on canvas nodes (`node-function` / `node-variable` + `data-label`), `run-button`, `open-plot-studio`, `rail-group-<title>`, palette (`palette-search`, `palette-tab-<id>`, `palette-item` + `data-label`, `palette-delete`), `delete-declaration-confirm`, `panel-kind`, `panel-back-to-palette`, `menu-expected-locations`, `expected-locations-popup`, `expected-row` + `data-status`, `coverage-button`, `coverage-popup`, `coverage-row`. Both bundles rebuilt. |
| backlog_e2e | done (6 tests) | `tests/e2e/test_gui_backlog.py` on a real project (scistack.toml + entities file): palette search, palette delete + undo (file byte-for-byte), expected locations, coverage, node panel header, and Plot Studio rail groups (covers the split). **Not automated:** the unified Process/Plot/Stats flow and move-node-to-canvas. Both need VS Code tabs or multi-step canvas drags whose markup I couldn't confirm without running the GUI; they stay manual. |
| ci_e2e | done | `ci.yml` job `gui-e2e`: installs Playwright + Chromium and runs `pytest scistack-gui/tests/e2e` with `SCISTACK_E2E_REQUIRED=1`, so a missing Playwright fails instead of skipping. On failure it uploads the server logs. `tests-passed` and so `publish.yml` require it. |

**Fix (2026-11-11):** `tests/e2e/conftest.py` broke 11 GUI test modules at
collection (`cannot import name 'FilteredSignal' from 'conftest'`). e2e/ has no
`__init__.py`, so pytest imported that conftest as the bare module `conftest`,
which shadowed `tests/conftest.py`. The `page` fixture now lives in `gui_e2e.py`
and each e2e test module imports it. Do not add a conftest.py under e2e/.

**Fix (2026-11-12), scidb suite (62 failures):**
- The lazy-import hoist of `record_run` into `foreach_save` bound the function
  at import, so tests that patch `scidb.provenance.save.record_run` no longer
  reached it (59 hist/provenance failures). It is now called as
  `_provenance_save.record_run(...)`, with a comment. A scan of all 70 hoisted
  names against every test `setattr` found no other case
  (`database.get_database` is patched, but its caller still looks it up in
  `database.py`).
- `test_save_plan.py` and `test_foreach_prepare_stages.py` now patch
  `foreach_save` and `foreach_prepare`, where `build_save_plan`,
  `apply_save_plan` and `_existing_combos` are called.

**e2e (2 failures), diagnostics added, not yet fixed:**
- A `<div>` covered the palette Functions tab for 60 s in the first test. Clicks now go
  through `gui_e2e.click()`, which names the covering element and saves a screenshot.
- The Plot Studio rail never appeared. The test now asserts on the describe
  response (eligibility reason) and dumps page text + screenshot.
- Diagnosed (2026-11-12): (1) the cover was the one-time "Added from recorded
  runs" notice on a fresh project's first load; `open_canvas` now dismisses it
  (`dismiss_graph_notice`, waits up to 3 s). (2) Plot Studio opens in Step by
  step, and rail groups exist only in Advanced; the test switches views first.
  Neither was a GUI bug.
