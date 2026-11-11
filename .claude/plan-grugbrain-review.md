# Plan: grugbrain.dev review of SciStack

Source: https://grugbrain.dev (read via its GitHub source `index.md`, 2026-11-10).
Baseline for "growth" numbers: `docs/claude/architecture-review-2026-09-20.md`.

## Where the codebase already follows grug

- **Testing**: ~9,500 test functions, real DuckDB fixtures, integration suites over
  a real example dataset (aim2), zero `unittest.mock` imports anywhere. Regression
  tests are written per bug (CLAUDE.md NOTE 2). This is the integration-test sweet
  spot grug recommends.
- **Logging**: heavy, structured (`scistacklog`, run journal, `scidb` CLI). Grug's
  "log all major branches" is done.
- **Optimizing**: timing logs and profiling come before optimizing (`timing-logs/`,
  batched provenance hot paths).
- **Chesterton's fence**: `docs/claude`, decisions.md, and the memory notes record
  *why* things exist.
- **Type systems / visitor**: generics are rare (6 files), no visitor pattern, and
  no dense nested ternaries were found.
- **One owner per concept** (NOTE 4) is the same idea as grug's "narrow interface
  hiding complexity".

## Where it does not

| Grug principle | Finding | Evidence (now vs 2026-09-20) |
|---|---|---|
| Complexity is the enemy / saying no | The hot spots keep growing faster than they shrink | `scidb/run/foreach.py` 5,888 → 6,827 lines; `PlotStudio.tsx` 3,916 → 6,970; `DatabaseManager` 73 → 89 methods; `docs/claude` 109 → 166 files |
| Factoring: cut at natural points | Natural cut points are now visible but haven't been cut | `run_service._run_in_thread` is 798 lines; `scidb.for_each` body + `_for_each_prepare`; `database.load` 290 lines |
| APIs: simple things simple, layer the rest | `scidb.for_each` has 26 parameters; 4 are private hooks (`_pre_combo_hook`, `_progress_fn`, `_cancel_check`, `_inject_combo_metadata`) and several are GUI-only (`glue`, `parameter_names`, `step`, `share_limits`, `pipeline`) | `scidb/src/scidb/run/foreach.py` `def for_each` |
| Front end: one component = one understandable place | `PlotStudio.tsx` holds 63 `useState` and 28 `useEffect` in one file | `scistack-gui/frontend/src/components/PlotStudio/PlotStudio.tsx` |
| Concurrency: keep simple; thread-locals are framework code | Current DB lives in `threading.local()`, so every background thread must remember to call `db.set_current_db()` | `scidb/src/scidb/store/database.py:443`, `run_service.py` comments |
| Factoring / import cycles | Function-level imports grew instead of shrinking | scidb 352 → 568, GUI 850 → 1,402 |
| Separation of concerns vs locality | Front end + back end is two codebases joined by about 213 RPC/HTTP handler rows. The VS Code webview forces this split. The cost is endpoint count. | `scistack-gui/scistack_gui/api/*.py` |
| Testing: few e2e tests on key UI paths | No automated GUI end-to-end tests. Key paths rely on `docs/gui-manual-testing-todo.md`, and many items are unchecked. | no Playwright/WebDriver in `scistack-gui` |
| Testing: test behaviour, not implementation | 33 test files read source text (`getsource` / `.read_text()` on code) | e.g. `scistack-gui/tests/test_node_identity.py` |
| Complexity of knowledge | 383 `.claude/` plan files, 241 memory files, 34.6k lines of `docs/claude`. Finding the *current* truth is itself complex. | — |
| Hygiene (minor) | `tmp.py` and `test-failures.txt` are tracked in git; 9 MB `scistack-debug.log` at repo root | `git ls-files` |

## Plan (named stages, each its own commit; user runs tests)

### split_run_in_thread
Break `scistack-gui/scistack_gui/services/run_service.py::_run_in_thread` (798 lines)
into named phase functions: `_resolve_function`, `_prepare_thread_context`,
`_execute`, `_report_outcome`. No behaviour change.
- Logging: keep every existing `[run_thread]` line and add one timing line per phase.
- Tests: existing run tests must pass unchanged. Add one test per phase seam only
  where a phase gains a return value that wasn't observable before.

### for_each_api_layers
Collapse the private hooks into one `RunHooks` dataclass (`progress`, `cancel_check`,
`pre_combo`, `inject_combo_metadata`) and the GUI-only arguments into one
`GuiRunContext` (`glue`, `parameter_names`, `step`, `share_limits`, `pipeline`).
The user-facing `for_each` then reads as: fn, inputs, outputs, schema filters,
`where`/`locations`, save/dry_run/on_error, `as_table`/`distribute`, plus
`hooks=` and `gui=`.
- Beta rule: no deprecation shims (see memory feedback_beta_no_deprecation).
- Find all call sites with LSP `findReferences` first: GUI run_service, MATLAB
  bridge `scimatlab/bridge/entry.py`, scifor parity (project_for_each_kwarg_parity).
- Tests: kwarg-parity test between scifor/scidb/MATLAB updated, not removed.

### plotstudio_split
Split `PlotStudio.tsx` along its existing rail groups (project_plot_studio_rail_groups):
one component per group, with state in one `usePlotStudioState` reducer that is
the single owner of that state. Do the split one rail group per commit and rebuild
the bundle each time (project_frontend_bundle_rebuild).
- Tests: existing frontend tests plus one render test per extracted group.
- Manual: add items to `docs/gui-manual-testing-todo.md`.

### explicit_db_at_thread_entry
80/20 rather than removing the thread-local: add one context manager,
`scidb.using_db(db)`, that sets and restores the current DB. It becomes the only
way threads enter a DB. Replace the scattered `db.set_current_db()` calls at thread
starts with it.
- Logging: DEBUG line on enter/exit with thread name.
- Tests: background-thread test that fails if the DB is not set (regression for the
  trap the comments describe).

### lazy_import_triage
Don't sweep the whole codebase. Only fix the lazy imports that dodge real cycles,
in the files touched by the stages above, and leave the rest. Re-measure the counts
at the end, as information only: no line-count or import pin tests
(feedback_no_import_guard_tests).

### source_text_test_review
Go through the 33 tests that read source text. Keep the owner/parity guards
(memory says keep). Convert the ones that assert on implementation wording into
behaviour tests. Mark the rest hidden or leave them, never silently delete
(feedback_never_delete_mark_hidden).

### gui_smoke_e2e
Add 3–5 automated end-to-end paths only: open project → canvas renders; run one
function; open Plot Studio on a variable; export to plain Python. Use
`@vscode/test-electron` or a browser harness against the headless server.

### knowledge_consolidation
- Move superseded `.claude/plan-*.md` files to `.claude/archive/`. They are
  moved, not deleted.
- Add a short per-layer "current truth" index to `docs/claude/README.md` that
  points to the one live doc per concept.
- Merge memory entries already marked SUPERSEDED.

### repo_hygiene
Untrack `tmp.py` and `test-failures.txt`, and add `*.log`, `tmp.py`,
`test-failures.txt` and `timing-logs/` to `.gitignore`. Ask before untracking:
the user may use `tmp.py` on purpose, per CLAUDE.md NOTE 1.

### saying_no (process, user decision)
For the next N sessions, don't start new feature work in scidb `for_each`,
`DatabaseManager` or `PlotStudio` until the stages above land. New features in
those areas go to a backlog first.

## Not recommended
- No microservice-style splitting and no new packages: there are already 12, and
  `scistackstatsdb` is 404 lines.
- No line-count or complexity pin tests (user rule).

---

## Status (2026-11-10, all stages written, uncommitted, tests to be run by the user)

Restore point: `e4ebbf93`.

| Stage | Status | Notes |
|---|---|---|
| explicit_db_at_thread_entry | done | `scidb.using_db` (store/database.py), exported from `scidb`, documented in docs/api/database.md. `_start_run_thread(..., db=)` takes `db` as a required keyword and is the only thread entry. Tests: `scidb/tests/test_using_db.py`, `scistack-gui/tests/test_run_thread_entry.py`. |
| split_run_in_thread | done | `_run_in_thread` is now under 80 lines, plus `_resolve_function`, `_prepare_run`, `_execute_targets`, `_build_target_call`, `_run_target`, `_report_run_done`, and `_SingleRun` / `_PreparedRun` / `_TargetCall`. Phase timing: `[run_thread] phase <name> done in N ms`. `_RunLogRelay` is one module-level owner (the pipeline run had a second copy). |
| for_each_api_layers | done, **narrowed** | Only the 4 private hooks moved, into `hooks=scifor.LoopHooks(...)`, reusing scifor's object rather than a new `RunHooks`. **Correction:** `glue`, `step`, `share_limits`, `pipeline` are NOT GUI-only: code export writes `glue=`/`step=`, users set `share_limits`, `pipeline` is the Pipeline API. No `GuiRunContext`. Leftover `_x=` keywords raise (`scifor.options.reject_private_options`). |
| plotstudio_split | done, GUI check pending | **Part 1** (0da9f017): types, controls and styles files. **Part 2** (2026-11-10): the component body above `// --- render` moved verbatim into `usePlotStudio`, which composes 8 sequential sub-hooks, one per labelled section: `useStudioCore`, `useStepByStep`, `useCombines`, `usePinReport`, `useFigureSize`, `useExport`, `useSavedPlots`, `usePresets`. Each one lists what it reads from the earlier ones and returns what it declares. The 5 rail groups are components in `PlotStudioRail.tsx` taking `s: PlotStudioState`. PlotStudio.tsx went from 6970 to 1374 lines. **Correction:** no single reducer. Turning 63 `useState` into one reducer changes update semantics (stale closures, effect identity), and only an eye could check that. Hooks moved into functions behave identically. A script confirmed that no section reads a later section's names (zero forward references), so the order-preserving split is exact. tsc clean, 743 frontend tests pass, both bundles rebuilt. |
| lazy_import_triage | done | run_service.py 44 → 33 function-level imports (the leaf ones dodged no cycle), plus scidb foreach.py's `scifor.options` import. `execution_service` and the MATLAB modules stay lazy: nothing imports them at module level, a fence not yet understood. |
| source_text_test_review | done | 2 wording tests became behaviour tests (`test_save_figure_uses_full_resolution`, `test_exported_header_imports_what_repr_emits`). Kept as owner/parity guards: cross-language spelling checks (TSX handle ids, node types), the AST thread guard, lock-check guard, MATLAB source checks (MATLAB cannot run in CI). |
| gui_smoke_e2e | done (opt-in) | `scistack-gui/tests/e2e/test_gui_smoke.py`: canvas renders, ▶ Run reaches `run_done` success (read off the WebSocket), Plot Studio reaches `/api/plot/describe`. Skips without Playwright. Needs port 8765 because `useWebSocket.ts` hard-codes it, which is itself a finding. |
| knowledge_consolidation | done | 142 plans with last commit before 2026-08-01, plus plan-topologies-panel, moved to `.claude/archive/` (moved, with a README). `docs/claude/README.md` index by layer. Memory: topologies note merged into project_variants_popup. |
| repo_hygiene | done | `tmp.py` and `test-failures.txt` untracked and ignored; the files stay on disk. `*.log` was already ignored. |
| saying_no | recorded | Memory `project_grugbrain_review`: no new features in foreach.py / DatabaseManager / PlotStudio.tsx until the reducer step and the test run land. |

### Backlog (found during the stages)
- `useWebSocket.ts` hard-codes port 8765 while `--port` is configurable. Two owners of one fact.
- The remaining long phase: `_prepare_run` is about 250 lines of sequential steps. Split it if it changes again.
