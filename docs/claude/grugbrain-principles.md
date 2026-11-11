# Grugbrain principles, applied to SciStack

Source: https://grugbrain.dev (its text lives in `bigskysoftware/grugbrain.dev`,
`index.md`). Review and stage plan: `.claude/plan-grugbrain-review.md`.

This page maps grug's advice to this project's own rules (CLAUDE.md NOTES 1-4) so
that later sessions can apply it without re-reading the site.

## The one idea

Complexity is the enemy. Every principle below is a way of keeping it out, or
of keeping it in one place where it can be seen.

## Principle → SciStack rule

| Grug | What it means here |
|---|---|
| **Saying no / saying ok (80/20)** | Before adding a feature to a hot spot (`scidb/run/foreach.py`, `store/database.py`, `PlotStudio.tsx`, `run_service.py`), ask whether it can live elsewhere or be an 80% version. While a consolidation stage is open on a hot spot, new features for it go to a backlog first. |
| **Factor at natural cut points** | Split a function only along seams that already exist, such as the phases of a run or the rail groups of Plot Studio. Never split to hit a line count. |
| **Simple APIs, layered** | A user-facing function's signature carries what a scientist writes. Machinery a caller uses to drive the loop goes into one named object (`hooks=scifor.LoopHooks(progress_fn=..., cancel_check=...)`, the same object in scifor and scidb), not into more keyword arguments. Every user-facing `for_each` kwarg costs edits in five places (scidb, scifor, MATLAB scifor, MATLAB scidb, the GUI MATLAB generator). Before calling an option "GUI-only", check whether exported code or users write it: `glue=`, `step=`, `share_limits=` and `pipeline=` all turned out to be user-facing. |
| **One owner (NOTE 4) ≈ narrow interface hiding complexity** | Same idea as grug's. Grug's "DRY, but not too much" applies to *trivial code* only. For *concepts* (identity, options, spellings), NOTE 4 wins: one owner, always. |
| **Expression complexity** | Name intermediate values, so that a log line or a debugger can show them. |
| **Closures like salt** | A nested `def` inside a long function usually means the function wants splitting. Lift the closure into a named function at module level. |
| **Concurrency simple; thread-locals are framework code** | Threads enter a database only through `scidb.using_db(db)`. Never call bare `set_current_db()` at a thread start. |
| **Logging: log the major branches** | Already the house style (NOTE 2). Each named phase of a long operation logs entry/exit plus its elapsed time, so the next "slow" or "hangs" report names its phase. |
| **Testing: integration sweet spot, few e2e, minimal mocks** | Already the house style: real DuckDB, no `unittest.mock`. The missing tier is a *handful* of GUI end-to-end smoke paths (`scistack-gui/tests/e2e/`, Playwright, opt-in). Tests should assert behaviour. Tests that read source text are kept only as owner/parity guards. |
| **Refactor small, keep it working** | One stage = one commit, and the user runs the suites between commits. Beta rule: no deprecation shims. |
| **Chesterton's fence** | Before removing anything, read its memory/doc entry. "Remove" means mark hidden or archive, never delete (memory `feedback_never_delete_mark_hidden`). |
| **Optimize from a profile** | Already done (`timing-logs/`, scidb CLI). Keep doing it. |
| **Front end** | The VS Code webview forces two codebases. Keep the endpoint count from growing; prefer adding a field to an existing handler. In React, one reducer owns a panel's state; components don't each hold a slice. |
| **Fear of looking dumb** | If a design takes more than a paragraph to explain, say so and simplify it first. |

## Knowledge is complexity too

`docs/claude`, `.claude/plan-*.md` and the memory notes are this project's
Chesterton fences, but too many of them is a cost of its own. Rules:

- **One live doc per concept.** `docs/claude/README.md` indexes the current one.
- **Superseded plans move to `.claude/archive/`.** They are moved, not deleted.

## Where grug does not apply

- **Two codebases.** Grug's "don't split front end and back end" can't be
  followed: the webview requires the split.
- **Don't refactor too early.** It applies to code that is still being designed.
  `for_each` and Plot Studio are mature, so they get split now, behind a pause on
  new features in those areas.
- **No new packages and no microservice-style splitting.** There are already 12
  packages.
- **No line-count or import-pin tests** (memory `feedback_no_import_guard_tests`).
  Size numbers are re-measured as information only.
