# Translate pipelines to code (to-do #6)

Goal: generate a standalone script (never data/records — a script that
RECOMPUTES them by running the same wiring against the same database)
reproducing a GUI-authored pipeline's wiring outside the GUI. Builds on
`docs/claude/archive/gui-export-to-plain-python.md`'s disconnected-wiring decision
(warn-comment, reusing `execution_service.disconnected_reason`) and reuses
#7's recursive closure walk (`portability_service._closure_pipeline_ids`)
for nested submodules.

## Language handling (per user decision, 2026-08-13)

Detect the closure's function languages via `matlab_registry.is_matlab_function`:
- **All Python** → generate a `.py` script.
- **All MATLAB** → generate a native `.m` script.
- **Mixed** → explicitly NOT supported yet (raises a clear error naming
  which functions are which language) — left unimplemented per explicit
  instruction, for simplicity. (Note: at the EXECUTION level a Python
  script can already call MATLAB-registered functions transparently via
  scimatlab's bridge, so this restriction is about the code GENERATOR,
  not a real capability gap — a future version could lift it easily once
  wanted.)

## Key finding that shaped the design: `build_run_inputs` is already
## language-agnostic

Went in planning to reuse `execution_service.build_backend_pipeline`
(which compiles a real `scidb.Pipeline`) for BOTH languages, the same way
#7 reuses `_clone_nodes`. Turns out `build_backend_pipeline` only works
for Python: it resolves the callable via `registry.get_function`, which
only ever contains Python functions (`registry._functions`) — MATLAB
functions live in a completely separate `matlab_registry._matlab_functions`
dict, so a MATLAB function node is silently `continue`d past today
(caught `KeyError`, logged, skipped). There is no existing "compile a
MATLAB step" path to reuse.

However, digging into what `build_backend_pipeline` and
`execution_service.build_run_inputs` actually depend on turned up that
almost NONE of the target/input RESOLUTION logic is Python-specific:

- `derive_target_for_node`/`derive_fn_targets` never call
  `registry.get_function` at all — wiring/DB-history-based target
  derivation is purely data-driven.
- `_fn_params_from_registry` (used by both, to find "missing" params for
  PathInput/Sweep elimination) already **falls back to the MATLAB
  registry** when a name isn't a Python function.
- `registry.get_variable_class` already resolves MATLAB variable types
  too — MATLAB `classdef` variables get a Python surrogate registered
  into `BaseVariable._all_subclasses` at discovery time (`matlab_registry.py`),
  so the SAME class-resolution path works for both languages.
- `build_run_inputs` itself only ever calls `registry.get_variable_class`
  and `_fn_params_from_registry` — both MATLAB-aware — and constructs
  generic `scifor.EachOf`/`scifor.PathInput` VALUE OBJECTS, not anything
  Python-execution-specific.

**Consequence: `build_run_inputs` is directly reusable for MATLAB targets
too**, unmodified. The only genuinely NEW code needed for the MATLAB path
is (a) a driving loop that walks the closure without a compiled Pipeline
(mirrors `build_backend_pipeline`'s own loop body almost line-for-line,
minus the `for_each(...)` call), (b) a topological sort at the
input/output-TYPE level since there's no compiled `Pipeline._topo_order`
to lean on, and (c) a MATLAB-syntax serializer for the SAME resolved
Python objects (classes / constants / `EachOf` / `PathInput`) the Python
path already gets from `build_run_inputs` — not a re-derivation of
resolution semantics, just a different text renderer for the same data.

## Python generator

Reuses `execution_service.build_backend_pipeline` to get a real compiled
`scidb.Pipeline`, then `pipe._composed_steps()` + `pipe._topo_order(pairs)`
— the SAME "private" methods `Pipeline.plan()` itself calls internally —
for a correctly dependency-ordered `list[StepSpec]` spanning the whole
closure (uses recurse for free, since that's what a compiled Pipeline
already does). Each `StepSpec` carries `.fn`, `.inputs` (the exact
already-resolved `for_each` inputs dict — real classes/constants/
`EachOf`/`PathInput` objects), `.outputs`, `.metadata_iterables`.

Serialization exploits that `EachOf.__repr__`/`PathInput.__repr__`
**already produce valid, readable Python constructor syntax**
(`EachOf(RawSignal, PathInput('t.csv', root_folder=None))`) — a single
`_py_literal(value)` helper (`value.__name__` for a bare class, `repr(value)`
otherwise) handles every resolved input value with no per-type branching
needed for the interesting cases.

```python
for_each(bandpass_filter, {"signal": RawSignal, "low_hz": 20}, [FilteredSignal], **SCHEMA_ITERABLES)
```

Header: `configure_database(db_path, schema_keys)` + `sys.path`-based
import of the user's own module(s) (`registry._module_path` for
single-file mode, `registry._config.modules` for project mode).

## MATLAB generator

Since there's no compiled Pipeline to walk, this drives its OWN loop:
for each pipeline_id in the closure, `execution_service._scope_function_node_ids`
+ `derive_target_for_node` + `apply_pending_overrides` +
`filter_hidden_targets` (verbatim — the exact sequence
`build_backend_pipeline`'s loop body already runs, minus the final
`for_each(...)` call) collects `(fn_label, target)` pairs. `build_run_inputs`
resolves each target's inputs (reused as-is, per the finding above).

**Topological order**: a small type-level Kahn's algorithm (a step
depends on any other step whose `output_type` appears among its own
`input_types`) — the MATLAB-side equivalent of `Pipeline._topo_order`,
since no compiled Pipeline object exists to ask.

**MATLAB serialization** of the SAME resolved values `build_run_inputs`
already returns: a bare class → `ClassName()` (constructed instance —
MATLAB's own convention, confirmed against scimatlab's test suite, differs
from Python's bare class reference); `EachOf` → `scifor.EachOf(alt1, alt2, ...)`;
`PathInput` → `scifor.PathInput(template, 'root_folder', root)`; strings →
double-quoted MATLAB string literals; numbers/bools → MATLAB literals.
Inputs become a `struct('param', value, ...)`, outputs a `{OutputCls()}`
cell, iteration kwargs become trailing Name/Value pairs
(`'subject', ["1","2"], 'session', ["pre","post"]`):

```matlab
scidb.for_each(@bandpass_filter, struct('signal', RawSignal(), 'low_hz', 20), {FilteredSignal()}, 'subject', ["1","2"], 'session', ["pre","post"]);
```

Header: `addpath` for scimatlab's own `matlab/` directory (resolved via
`Path(scimatlab.__file__).parent / "matlab"` if importable, else a
placeholder comment) + `matlab_registry._config.matlab_addpath` entries +
`scidb.configure_database(db_path, schema_keys)`.

## Shared across both

- **Disconnected wirings**: `execution_service.disconnected_report_entries(db, pipeline_id)`
  (language-agnostic — driven by hidden edges, not function language) —
  anything it reports gets a `# SKIPPED: '<fn>' — <reason>` (or MATLAB
  `%`) comment instead of a call, per the existing design doc's
  recommendation.
- **Recursive submodules**: the Python path gets this for free via
  `Pipeline.use()`; the MATLAB path walks
  `portability_service._closure_pipeline_ids` explicitly.
- **File output**: writes to `{project_dir}/exports/` and returns the
  script text too (same "write into project dir, return the content"
  pattern as `endpoint_service.write_report` and #7's JSON export).

## Known v1 limitations (explicitly not handling, to keep scope bounded)

- Mixed-language pipelines: explicit error, per the language decision above.
- A `use` with a non-trivial binding (`key_map`/`params`/`iterate`): the
  Python path handles this correctly (compiled `Pipeline.use(child.bind(...))`
  is language-agnostic machinery, already exercised elsewhere); the
  MATLAB path does NOT apply bindings (there's no compiled-Pipeline
  equivalent to lean on for it) — a bound submodule's steps are skipped
  with a warning comment, same "warn, don't silently omit" philosophy as
  disconnected wiring.

## Effort shape

Python generator: small — almost entirely reuses existing, already-tested
machinery (`build_backend_pipeline`, `Pipeline._composed_steps`/
`_topo_order`, `EachOf`/`PathInput`'s own good `__repr__`). MATLAB
generator: medium — reuses target/input resolution (turned out to be
fully language-agnostic) but needs its own driving loop, its own
topological sort, and a full MATLAB-syntax serializer.

## Status: BUILT (2026-08-13)

Implemented per the design above:

- **Backend** — new `services/code_export_service.py`:
  `export_pipeline_to_code` (language detection + dispatch),
  `_generate_python_script` (reuses `build_backend_pipeline` +
  `Pipeline._composed_steps`/`_topo_order` directly), `_generate_matlab_script`
  (own driving loop via `_matlab_steps` + own `_topo_sort_targets` Kahn's-
  algorithm implementation, reusing `derive_target_for_node`/
  `apply_pending_overrides`/`filter_hidden_targets`/`build_run_inputs`
  verbatim), `_py_literal`/`_matlab_literal` serializers, shared
  `_skip_comments` (disconnected wiring, reusing
  `disconnected_report_entries`). Writes to `{project_dir}/exports/`
  (same pattern as #7's JSON export and `endpoint_service.write_report`).
  Thin delegate in `scope_service.py`; `GET /api/pipelines/{id}/export-code`
  in `api/scopes.py` (400 on mixed-language, via the existing `_guard`
  ValueError mapping); wired through `server.py` and `api.ts`.
- **Frontend** — a `</>` button added next to the JSON Export button in
  both `HypothesisTabs.tsx` and `PipelineNode.tsx`; downloads the returned
  script client-side (`.py` or `.m` by `language`) in standalone mode,
  shows the written path + any skipped-step warnings in both modes.
- **Tests** — `tests/test_code_export.py`: Python generator end-to-end
  against conftest's real seeded wiring (call shape, iteration kwargs,
  disconnected-wiring skip comment, file written to `exports/`);
  mixed-language rejection (service-level `ValueError` and REST 400);
  an all-MATLAB pipeline end-to-end using a FAKE MATLAB registration
  (`matlab_registry` is a plain dict — no real MATLAB environment needed
  to exercise the resolution/serialization code paths, though this means
  the actual `.m` output has never been run through a real MATLAB
  interpreter — flagging that residual risk explicitly); direct unit
  coverage of the serialization/topo-sort helpers (the genuinely new code
  this feature adds, as opposed to the reused resolution machinery). Not
  yet run by the user.

This closes out Phase D and the full to-do list from
`spec/to-dos-26.08.12.md`.
