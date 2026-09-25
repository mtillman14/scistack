# Plan: discovery runs analysis scripts on import and blows the startup timeout

## Incident (2026-09-25, Stroke-R01-Aim1)

- The startup server sent ONE `progress` notification ("Auto-discovering pipeline code...")
  before importing 25 `.py` files (`server.py:727`). The extension's ready timer is an
  inactivity timer, and only `progress` resets it (`pythonProcess.ts:213`), so every
  import together had to finish within 60 s.
- `create_change_score_df.py` (13 s) and `create_cohensd_df.py` (8 s) passed
  `find_top_level_side_effects` and ran their analysis during import, writing CSVs to
  `results/stats/...` relative to the server's cwd. `csv-stats-change-score.py` then used
  up the rest of the 60 s → SIGTERM.
- Probable cause: the documented gap in `scifor/src/scifor/discovery.py`, where a bare
  top-level `for`/`while`/`with` runs but is not reported. **Confirm first (Step 0).**
- Side issue: the new `scistack.toml` was seeded with the DB directory
  (`/Documents/Aim1`), which holds no `.py`/`.m` files → noisy "contains no .py/.m files" WARNs.

## Step 0: diagnostics (user runs on the Mac, in Stroke-R01-Aim1)

Top-level statement kinds of the 3 files that ran:

    python -c "import ast,sys; [print(p, [(n.lineno, type(n).__name__) for n in ast.parse(open(p).read()).body if not isinstance(n,(ast.Import,ast.ImportFrom,ast.FunctionDef,ast.ClassDef))]) for p in sys.argv[1:]]" src/stats/create_change_score_df.py src/stats/create_cohensd_df.py src/stats/csv-stats-change-score.py

What the screen says today (expected `[]` for all three):

    python -c "import sys; from scifor.discovery import find_top_level_side_effects as f; [print(p, f(open(p).read())) for p in sys.argv[1:]]" src/stats/create_change_score_df.py src/stats/create_cohensd_df.py src/stats/csv-stats-change-score.py

If the kinds are `For`/`With`/`If`, Step 1 covers it. If they are something else (for
example an assignment calling an *imported* heavy function), adjust Step 1 before coding.

## Step 1 (scifor, owner of the side-effect rule): recurse into top-level compound statements

`find_top_level_side_effects` in `scifor/src/scifor/discovery.py`:

- Apply the SAME two forms (bare non-benign call; assignment calling a local fn) to the
  bodies of top-level `For`/`AsyncFor`/`While`/`With`/`AsyncWith`/`Try` (body, handlers,
  orelse, finalbody) and `If`, recursively through nested compound statements.
- Do not descend into the `if __name__ == "__main__":` guard (including the reversed
  `"__main__" == __name__` spelling). Other `If`s are descended.
- Never descend into `def`/`class` bodies (unchanged).
- The `reason` names the context, e.g. "its result is discarded, inside a top-level for
  loop at line 12", so the refusal message tells the user where to look.
- Also flag a local-function call in a loop's iterator / `with` context expression
  (`for r in compute_all():`).
- Update the docstring: remove the "Known gaps" bullet for loops and document the
  recursion rule.
- One rule, one owner: `registry._screen_for_side_effects` stays a pure consumer.

Tests (`scifor/tests/test_discovery.py::TestFindTopLevelSideEffects`):
- `for` body with `df.to_csv(...)` → flagged, lineno is the inner call.
- `for` body with `x = local_fn(...)` → flagged.
- `with open(...) as f: f.write(...)` → flagged.
- `if DEBUG: run()` (local) → flagged; `if __name__ == "__main__": run()` → clean
  (both spellings).
- nested `for` inside `for` → inner call flagged.
- `for` with only `print(...)` / logger calls → clean.
- `for n in names: P = Parameter(...)` (imported callee) → clean.
- `try: import x\nexcept ImportError: x = None` → clean.
- `for r in local_fn():` → flagged.

## Step 2 (GUI registry + server): progress and timing for each module

- `registry.load_from_config(config, on_progress=None)` → `_exec_file_modules(paths,
  on_progress)` calls `on_progress(f"Importing {i}/{n}: {path.name}")` BEFORE each
  module (refused files included, since they are cheap). The default `None` keeps
  Refresh Code and other callers unchanged.
- `server.py` startup discovery passes `_send_progress`. The 60 s window then applies to
  each file instead of all of them, and a stuck file is named in the extension's
  progress/failure output.
- Time each `exec_module` with `perf_counter`. Include the duration in the existing INFO
  "Loaded module file: ... (N functions, 12.8s)". Above `SLOW_IMPORT_WARN_S = 5.0`, log a
  WARN naming the file and saying its top-level code probably runs work and should go
  under `if __name__ == "__main__":`.
- Log the total discovery time at INFO once.

Tests (`scistack-gui/tests/test_registry.py`, `test_startup.py`):
- `on_progress` gets one message per module, in order, including a refused file.
- Slow-import WARN fires when the threshold is monkeypatched to 0 (caplog) and not at the
  default for a trivial module.
- Startup emits one `progress` notification per module (capture `_send`).

## Step 3 (GUI config): seed the DB dir only when it has sources

`_first_write_seed_roots` (`scistack-gui/scistack_gui/config.py:1405`) seeds the database
directory to keep what folder-scan mode found there. If that directory holds no `.py`/`.m`
files there is nothing to keep, so skip it and log at INFO why. Reuse the file enumeration
config already uses for directory entries. Do not add a second walker.

Tests (`test_config.py`):
- Update `test_first_write_seeds_both_the_db_folder_and_the_project_root` to put a `.py`
  in the datasets dir. It still seeds both.
- New: an empty DB dir → only the project root is seeded.
- New: a DB dir with only a `.m` → seeded.

## Step 4: docs

- `docs/claude/gui-extension-startup-path.md`: the inactivity timer resets only on
  `progress`; discovery now reports each module.
- `docs/claude/code-discovery-categories.md` (or the discovery section it points to): the
  side-effect rule now recurses into top-level compound statements.
- `docs/gui-manual-testing-todo.md`: the startup progress shows "Importing i/n: file"; a
  project with a slow script names it in the failure report.

## Out of scope

- A hard per-file time budget (exec_module cannot be interrupted safely in-thread).
- Surfacing slow imports in the Discovered Code panel.

## Commands for the user (one package at a time)

    pytest scifor/tests/test_discovery.py -k TopLevel
    pytest scistack-gui/tests/test_registry.py scistack-gui/tests/test_startup.py scistack-gui/tests/test_config.py
