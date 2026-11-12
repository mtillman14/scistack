# Real-Project Readiness: Zero-Install Discovery, Built-in Functions + Sidebar Reconciliation

## Context

So far the GUI has been exercised against `gui_test_data.py` — a single hand-crafted demo file. The user is now moving to a **real project**, and confirmed the actual shape of that project: **Python and MATLAB code mixed in the same project, written as loose scripts by scientists with minimal technical/software background** — not a formally packaged `pyproject.toml` + `src/{name}/` + `uv.lock` layout. Formal packaging may matter someday but is explicitly not a priority now.

Investigation (3 parallel Explore passes + direct reads of `config.py`, `registry.py`, `matlab_parser.py`, `matlab_registry.py`, `api/project.py`) found the codebase is much further along than expected — Python "point at loose files" discovery mostly already works, MATLAB discovery already supports non-packaged `scistack.toml` configs, and the sidebar already has real (not toy) UI for Variables/Functions/Constants/PathInputs/Libraries. The actual gaps, in priority order for this audience:

1. **Zero-config is impossible today.** `config.load_config` hard-raises `FileNotFoundError` if no `pyproject.toml`/`scistack.toml` exists anywhere above the `.duckdb`. A non-technical user cannot hand-author TOML — this is the #1 blocker to opening a real project at all.
2. **MATLAB requires manually pre-splitting files into `functions` vs `variables` lists.** There's no per-file content classification, so a true zero-config default (point both lists at the same root) breaks the existing dedup logic (it would deduplicate away every function). Python doesn't have this problem — it already classifies dynamically per imported object.
3. **The sidebar's "Discovered Code" panel (`ProjectConfigPanel`) is powered by a completely different scanner (`scidb.discover.scan_project`) than the one that actually loads pipeline functions (`registry.py`/`matlab_registry.py`, driven by `config.py`).** `scan_project` only understands the packaged `pyproject.toml` + `src/{name}/` + `uv.lock` layout and requires an `@scistack` decorator. For a loose-script project it silently returns empty/near-empty — even though functions are actually loaded and runnable. This is the single biggest source of "GUI says nothing found, but my pipeline works" confusion.
4. **Discovery failures are swallowed.** `registry.py`'s three loaders (`_load_file_modules`, `_load_packages`, `_load_entry_points`) catch exceptions and only `logger.exception` them — nothing reaches the frontend. `EditTab.tsx` (the primary palette) does the same client-side (`.catch(console.error)`). For a non-technical user, "my function just doesn't show up, with zero explanation" is the worst possible failure mode.
5. **MATLAB parser has real false-positive/false-negative risk** on patterns common in real (if unsophisticated) lab code: `%{ %}` block comments, multi-line signatures using `...` continuation, and sweeping up `@ClassName/` method files or `private/` helpers as if they were pipeline functions.
6. A working `refresh_module`/`refresh_all` RPC exists server-side but has **zero UI callers** — the only refresh path is a full process restart, overkill for someone iterating on a script.
7. **No way to reference a built-in/library function.** Auto-discovery correctly registers only functions *defined* in the user's own files (verified: `registry._scan_module_functions` filters by `__module__`, so `numpy`/`pandas`/stdlib calls imported into a user script are never swallowed in as if user-defined — this is correct and should stay). But there's also no *deliberate* path today for a user to say "this pipeline step calls `numpy.mean`" or a native MATLAB command like `sum`. Dragging a palette item is the only way to create a function node, the palette only lists auto-discovered functions, nothing validates a typed-in name, and execution hard-fails (`KeyError`) on anything not in the registry. Confirmed via a follow-up audit of `EditTab.tsx`, `layout_service.py`, `pipeline_store.py`, `run.py`, and `matlab_command.py` — no validation or built-in-resolution path exists anywhere in that chain today.

## Architectural decision

Add a **folder-scan fallback** beneath the existing single-file/project-config modes: when no `pyproject.toml`/`scistack.toml` is found, `load_config` returns a config built by recursively scanning the project root for `*.py` and `*.m` files (skipping `.git`, `__pycache__`, `.venv`/`venv`, `node_modules`, `build`, `dist`, and dot-directories) instead of raising. This reuses the *already-working* directory-walk-and-import machinery in `config.py`/`registry.py` for Python — no changes needed there beyond supplying the root as an implicit `modules` entry.

For MATLAB, give it Python's per-file classification model instead of Python's two pre-split lists: a new **unified MATLAB source list** where each `.m` file is parsed once and routed to variable-vs-function based on content (classdef-under-BaseVariable → variable; has a `function` declaration → function; neither → skipped), rather than requiring the config to already know the split. The existing explicit `matlab.functions`/`matlab.variables` config keys remain supported unchanged for advanced/opt-in cases (backward compatible), but folder-scan mode and any future simplified config use the unified path.

For the sidebar, stop treating `scidb.discover.scan_project` as the source of truth for what the GUI displays as "discovered code." Repoint `api/project.py`'s `/code` endpoint at the same registry (`registry.py` + `matlab_registry.py`) that already loads runnable functions, so the sidebar always matches what's actually usable in the DAG. `scan_project` itself is untouched (it's a scidb-layer capability that may matter again once formal packaging returns as a priority).

## Stage 1 — Zero-config folder-scan fallback (Python + MATLAB unified classification)

**`scistack-gui/scistack_gui/config.py`**
- `_locate_pyproject`: on the "no config file found" path (both the explicit-`project_path`-is-a-directory branch and the upward-search branch), instead of raising, return a sentinel (e.g. `None`) that `load_config` recognizes as "use folder-scan defaults," rooted at `project_path` (if given) or `db_path.parent`. Log at INFO that no config was found and folder-scan is being used, mirroring the existing logging style throughout this file.
- New helper `_scan_folder_defaults(root: Path) -> tuple[list[Path], list[Path]]`: recursively walks `root`, returns absolute `.py` paths and `.m` paths, pruning the noise directories listed above. Reuse `_normalize`.
- `.py` results feed into `modules` exactly like today's directory-entry handling (no change to `_load_file_modules`/import logic — it already takes arbitrary file-path lists).
- `.m` results become a new `matlab_sources: list[Path]` field on `SciStackConfig` (in addition to, not replacing, `matlab_functions`/`matlab_variables` — those stay for explicit config). Populate `matlab_functions`/`matlab_variables` as empty in fallback mode; the unified list is consumed separately (see below).
- Keep the explicit-config path (pyproject.toml/scistack.toml found) completely unchanged.

**`scistack-gui/scistack_gui/matlab_parser.py`**
- Add `classify_matlab_file(path) -> Literal["variable", "function", None]` (or equivalent) that tries `parse_matlab_variable` first, then `parse_matlab_function`, returning whichever matched (mirrors how Python's `_scan_module_functions` classifies dynamically).
- Strip `%{ ... %}` block comments before both regexes run (new helper, applied once per file read) — fixes false-positive risk from example code inside block comments.
- Fix `_FUNCTION_RE`'s param-list capture so a `...` line-continuation inside the parens doesn't leak into a param name (collapse `...\s*\n` to a single space before matching, done on the same text block before the search).
- Skip files under `private/`, `@*/`, or `+*/` directory segments in the new folder-scan path specifically (defensive default — these are real MATLAB conventions where sweeping the file in as a standalone function is wrong; full package/class-folder support is Stage 4).

**`scistack-gui/scistack_gui/matlab_registry.py`**
- New `load_from_sources(paths: list[Path])` (or extend `load_from_config` to also consume `config.matlab_sources` when present) that calls `classify_matlab_file` per file and registers into the existing `_matlab_functions`/`_matlab_variables` dicts — same registration/collision-warning code path as today, just a different file-list source.

**`scistack-gui/scistack_gui/server.py` / `__main__.py`**
- Confirm both entry points call `load_config` without pre-checking for the config file's existence (so the new fallback engages transparently); update any place that currently catches `FileNotFoundError` from `load_config` as a hard-exit condition.

**Tests** (new, in `scistack-gui/tests/`)
- `test_config.py`: no config file anywhere → folder-scan defaults populate `modules` and `matlab_sources` correctly; noise directories excluded; explicit-config path still takes precedence when a `scistack.toml` exists.
- `test_matlab_parser.py`: block-comment false positive suppressed; multi-line `...` signature parses correctly; `classify_matlab_file` on a function file, a classdef file, and a `private/`/`@Class/`/`+pkg/` file.
- `test_matlab_registry.py` (or extend existing): `load_from_sources` on a mixed folder registers the right functions/variables.

## Stage 2 — Manual built-in/library function references

Gives users a deliberate, validated way to add a function node for something they didn't write themselves — `numpy.mean`, `pandas.read_csv`, a Python stdlib call, or a native MATLAB command — alongside (never instead of) auto-discovery of their own code.

**UI (`EditTab.tsx`)**
- Add a "+ Add built-in function" affordance to the Functions section, matching the existing add-new pattern already used for Variables/Constants/PathInputs (`EditTab.tsx` — those sections have a `+`/text-input flow the Functions section currently lacks). Form: a language toggle (Python/MATLAB) and one text field — for Python, a dotted reference (`numpy.mean`, `pandas.read_csv`, or a bare name like `len` for a stdlib builtin); for MATLAB, a bare identifier (`mean`, `sum`, `zeros`).
- On submit, call a new validation+create endpoint; show the real error message inline (invalid reference, module not allowed, MATLAB not found) rather than swallowing it — consistent with the error-surfacing work in Stage 3 below.
- Successful creation drops a normal `functionNode` onto the palette/canvas via the existing manual-node path (`layout_service.put_layout` → `pipeline_store.write_manual_node`) — no new node type needed, just a new way to get a valid `label` before dragging it.

**Backend — Python side (new, e.g. `scistack_gui/api/builtin_functions.py` + `registry.py` additions)**
- Parse the typed reference: no dot → look up in the `builtins` module; one or more dots → split into module path + attribute name.
- Restrict the importable module root to an allow-list: Python stdlib (check via `sys.stdlib_module_names`, Python 3.10+, already the project's floor per `pyproject.toml`/`requires-python`) plus `numpy` and `pandas` — matches exactly what the user asked to support, and avoids this becoming a general arbitrary-import backdoor.
- `importlib.import_module(module_path)`, then `getattr(mod, attr, None)`, then `callable(...)` — reject with a clear message if any step fails.
- On success, register the callable into the existing `registry._functions`/`_function_sources` under its full dotted name (avoids collisions between e.g. `numpy.mean`/`pandas.mean` and with user-defined functions) — `registry.get_function()` and `api/run.py`'s existing `for_each(fn, ...)` call then work completely unchanged, since a validated `numpy.mean` is just as much a real Python callable as anything from `_load_file_modules`.

**Backend — MATLAB side**
- Validate by shelling out to MATLAB (per the user's choice): `matlab -batch "disp(exist('<name>'))"`, one-shot at creation time only (not per pipeline run, so the existing "avoid the MATLAB engine for repeated execution" guidance from `docs/claude/archive/matlab-gui-support-analysis.md` doesn't apply here). `exist()` returns 0 (not found) vs. non-zero (file/built-in/class/etc.) — treat any non-zero as valid.
- **Security:** validate the typed name against a strict MATLAB-identifier regex (`^[A-Za-z]\w*$`) *before* it ever touches the constructed shell command — never interpolate raw user input into the `matlab -batch` string.
- If no `matlab` executable is discoverable on PATH, fail creation with a clear message that MATLAB must be installed and on PATH to validate a built-in reference (per the user's choice — no curated-list fallback).
- On success, register into `matlab_registry._matlab_functions` with a synthetic `MatlabFunctionInfo` — `file_path` needs to become optional (`Path | None`) to represent "no backing .m file, this is a builtin"; `matlab_command.py`'s command generation is already name-based (confirmed: it doesn't read the file), so no changes needed there.

**Persistence**
- Manually-declared builtins aren't rediscovered by scanning disk, so `load_from_config`/`refresh_all` clearing `_functions`/`_matlab_functions` would silently drop them on every refresh unless replayed. Persist `(name, language, qualified_reference)` in the existing DuckDB store (`pipeline_store.py` already owns a `_pipeline_nodes`-style table for manual nodes — add a small sibling table there rather than inventing a new storage mechanism) and re-apply them at the end of every `load_from_config`/`refresh_all`, same lifecycle as the rest of the registry reload.

**Tests**
- Python: valid stdlib/numpy/pandas reference registers and is callable via `registry.get_function`; disallowed module (e.g. `os.system`) is rejected; nonexistent attribute is rejected.
- MATLAB: mock/stub the `matlab -batch` subprocess call in tests (don't require a real MATLAB install in CI) — valid/invalid identifier, MATLAB-not-on-PATH message.
- Restart/refresh: a manually-declared builtin survives `refresh_module`/`refresh_all`.

## Stage 3 — Sidebar/registry reconciliation + error surfacing + refresh button

**`scistack-gui/scistack_gui/registry.py`**
- Add `_load_errors: list[dict]` (fields: `source`, `path`, `error`) appended alongside the existing `logger.exception` calls in `_load_file_modules`, `_load_packages`, `_load_entry_points`. Add `get_load_errors() -> list[dict]` accessor. Clear it at the start of `load_from_config`/`refresh_module`, same lifecycle as `_functions.clear()`.
- Track per-function source file (already have `_function_sources`) so results can be grouped by originating file for display.

**`scistack-gui/scistack_gui/matlab_registry.py`**
- Same pattern: record parse failures (currently `logger.warning`-only when `parse_matlab_function`/`parse_matlab_variable` return `None`, or when a file can't be read) into a queryable list.

**`scistack-gui/scistack_gui/api/project.py`**
- Add a helper that builds the same shape `_serialise_module_exports`/`_serialise_module_error` already produce, but sourced from `registry.py` + `matlab_registry.py` (function/variable names grouped by source file, plus `_load_errors`) instead of `scan_project`.
- `_run_scan`/`get_project_code`: if `scan_project` finds no `pyproject.toml` (i.e. loose-script/folder-scan mode), use the registry-backed helper instead of the scidb-layer scanner. Packaged projects keep using `scan_project` unchanged. This keeps the existing `ProjectConfigPanel` rendering code working as-is — only the data source switches.

**Frontend**
- `EditTab.tsx`: the `.catch(console.error)` handlers on `fetchRegistry`/`fetchConstants`/etc. get a visible error state (reuse `ProjectConfigPanel`'s existing `error`/`errorBanner` pattern rather than inventing a new one) so palette-level discovery failures are seen where the user is actually looking, not just in the popup.
- Add a "🔄 Refresh code" action (calls the existing `POST /api/refresh` → `registry.refresh_module`/`refresh_all`, already implemented and already unused) somewhere visible in the sidebar/header, distinct from the existing heavy "Restart" (full process respawn) button. Check `api.ts` for whether a client wrapper already exists for `refresh_module`/`refresh_all`; if not, add one alongside the existing `restart_python`-style call.

**Tests**
- Backend: a fixture with a loose (non-packaged) mixed Python+MATLAB folder, assert `/api/project/code` returns the same functions/variables/constants that `/api/registry` returns, and that a deliberately broken module (syntax error) shows up in the errors list via the API (not just server logs).
- Frontend (if a test harness exists for these components — check existing patterns before deciding scope) or at minimum a manual verification step in the verification checklist below.

## Stage 4 — MATLAB parser Tier 2 (lower priority for this audience, still staged in)

- `+package/` folders: register as `pkg.fn_name` instead of skipping; update `matlab_command.py`'s generated call strings to qualify package-prefixed names so "copy command" / execution isn't broken for namespaced functions.
- `@ClassName/` folders: recognize as class-method files and exclude from top-level function registration (currently just skipped defensively in Stage 1; this tier could optionally surface them as class methods later, out of scope here).
- `arguments ... end` block parsing for parameter defaults/types (nice-to-have metadata, not required for basic discovery to work).
- `classdef (Abstract, ...) Foo < Base` attribute-list support in `_CLASSDEF_RE`.
- MATLAB Constants: **first confirm whether `scimatlab`/`scidb` has any constant-equivalent for MATLAB at all** (grep `scimatlab`/`scidb` for a MATLAB-side `constant`/`Constant` pattern before writing any parser code). If none exists, this is a cross-layer (scidb/scimatlab) feature gap, not a GUI parsing gap — flag it back to the user as a separate follow-up plan rather than building a GUI-only stub with nothing to parse.

## Future considerations (not planned in detail here, flagged per the "anything else" ask)

- **Real MATLAB execution (next plan, not this one).** Today, clicking "Run" on a MATLAB function node only generates a command string for the user to copy-paste into their own MATLAB session (`matlab_command_service.py`) — there's no in-GUI execution. Since most of the user's real pipelines are MATLAB, this matters more than anything staged above, but it's architecturally large and distinct enough (new process lifecycle, a JSON-RPC protocol, VS Code-extension coordination) to warrant its own dedicated plan, written after this one ships. Shape already discussed and agreed:
  - **VS Code extension** (the deployment target): coordinate with the MathWorks VS Code extension's own MATLAB terminal (`terminal.sendText()`) so breakpoint debugging works via its existing DAP integration — this is "Tier 2" in `docs/claude/archive/matlab-gui-support-analysis.md`. Trade-offs already documented there: no structured progress feedback, DAG refresh via a debounced `.duckdb`/`.duckdb.wal` file-watcher instead of an immediate event, hard dependency on the MathWorks extension being installed.
  - **Standalone/browser** (also required, since browser access with minimal setup impact is a stated goal): no MathWorks terminal exists in this context, so a persistent MATLAB sidecar process we manage (lazy-started on first MATLAB run, kept warm afterward — the user's own proposal) is the only real path to execution instead of copy-paste. No breakpoint debugging in this mode (acceptable — that requirement is specifically tied to the VS Code target).
  - Graceful degradation across both: MathWorks extension present → Tier 2; standalone with MATLAB on PATH → sidecar; MATLAB unavailable entirely → today's copy-paste fallback.
  - Before designing further: verify Python's existing `debugpy` auto-attach (`dagPanel.ts:142-174`) still works given the heavy uncommitted churn in `App.tsx`/`PipelineDAG.tsx`/`run.py` right now — it may have silently regressed, and it's the reference implementation the MATLAB debugging story needs to match in spirit.
- File-watching auto-refresh (watch the project folder for `.py`/`.m` changes, prompt or auto-trigger `refresh_module`) — would reduce friction further for non-technical users who edit-and-rerun constantly, once the manual refresh button from Stage 3 is in place and proven useful.
- A guided "pick a folder" first-run flow (GUI creates the `.duckdb` + a starter `scistack.toml` capturing the auto-detected root, rather than requiring the file to pre-exist) — noted because `__main__.py`'s CLI path currently hard-exits if the `.duckdb` doesn't exist yet, which is a related but separate onboarding gap from code discovery itself.
- Visible duplicate-name-collision warnings in the GUI (currently server-log-only, same swallowing pattern as load errors) — could piggyback on the Stage 3 error-surfacing plumbing.

## Verification

- Stage 1: build a scratch folder with a handful of loose `.py` files (including one importing `scidb`/`scifor` normally, no `pyproject.toml`) and loose `.m` files (a plain function, a `classdef < scidb.BaseVariable`, one with a `%{ %}` block comment containing a fake function signature, one with a `...`-continued multi-line signature) and confirm `scistack-gui <db>` with no `--project`/`--module` at all discovers everything correctly via the new fallback. Hand this off as a copy-paste Python snippet per CLAUDE.md (user runs Python themselves) plus a `scistack-gui` launch command for the user to try in the actual GUI.
- Stage 2: in the GUI, add a built-in function node for `numpy.mean` and for a MATLAB builtin (e.g. `mean`), confirm both validate and appear as normal function nodes; try an invalid reference and confirm the rejection message is clear; restart/refresh and confirm the manual builtin is still there.
- Stage 3: in the same scratch project, verify the sidebar's "Discovered Code" panel (behind 📁 Paths) matches the palette's Variables/Functions list; introduce a syntax error in one script and confirm it surfaces as a visible error in the GUI, not just `scidb.log`.
- Stage 4: verify with a `+pkg/` folder and a `%{ %}`-commented function that they're now handled as designed.
- Add regression tests per stage as described above; user runs `pytest`/`npm test` themselves (per CLAUDE.md, Python isn't available in this environment) — hand over copy-paste commands.
