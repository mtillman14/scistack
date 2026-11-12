# Make PathInput, Sweep, and Submodules Translatable to Source Code

## Context

The framework's core priority: everything expressible in the GUI must be
expressible as plain source code, so the GUI can be added or removed from a
project at any time without losing information. Functions, Variables, and
Constants already honor this — they're discovered by scanning real `.py`/`.m`
files (`docs/claude/code-discovery-categories.md`). Two categories currently
break the rule outright:

- **PathInputs** only exist in the GUI after a pipeline using them has *run*
  (reconstructed from DB `__inputs` history) or been hand-edited in
  `layout.json` — even though `scifor.PathInput` is already a normal,
  importable Python object.
- **Sweeps** are pure `layout.json` state with no source-code counterpart at
  all, despite already being converted to `EachOf(...)` internally at
  execution time (`execution_service.py:643`) — there's no reason a user
  can't just write the `EachOf` themselves.

**Submodules** (`pipelineNode`) are a third, larger gap: GUI composition
(`_pipeline_uses` rows, `binding_json = {key_map, params, iterate}`) has no
source-code equivalent today, even though `scidb.Pipeline.use()`/`.bind()`
(`scidb/src/scidb/pipeline.py`) already implements *exactly* that shape —
apparently the GUI's schema was deliberately modeled on it. `execution_service.
build_backend_pipeline` already compiles a GUI scope into a live in-memory
`Pipeline` for execution; what's missing is turning that same object graph
into readable source text (export) and reading it back in (import).

Decisions locked in with the user before writing this plan:
- PathInput: **clean break** — named top-level `PathInput`/`Sweep` objects
  become the *only* discovery path. No DB-history fallback, no
  `layout.json`-authored versions kept alongside. Matches the project's
  existing "beta: no deprecation, clean breaks" convention
  (`[[feedback_beta_no_deprecation]]`).
- `Sweep`: a trivial `class Sweep(EachOf)` subclass. No `name=` field — its
  identity is whatever module-level name it's bound to, exactly like
  `Constant`.
- Submodules: build directly on `scidb.Pipeline`/`StepSpec`/
  `PipelineBinding` as the canonical source-code shape, not a new
  representation.

### How this interacts with the two EXISTING GUI<->code mechanisms

There are already two separate bidirectional mechanisms in this codebase,
and this plan must fit both without breaking either:

1. **Code discovery** (this plan's Workstreams A/B): scanning `.py`/`.m`
   files -> GUI registry, already how Functions/Variables/Constants work.
2. **Cross-user pipeline portability** (`portability_service.py`,
   `export_pipeline`/`import_pipeline_document`, already built): exports one
   pipeline + its submodule closure as a self-contained JSON document for
   sharing between users/databases. Its module docstring is explicit about
   the existing split: a `functionNode`/`variableNode`'s `label` is "just a
   name the importing user's OWN registry resolves locally" (their source
   code is assumed to already define it) — **not bundled**. Only
   `_GLOBAL_NODE_TYPES = ("constantNode", "pathInputNode", "sweepNode")` get
   their values bundled into the document today, because PathInput/Sweep
   currently have no other source of truth, and (for constants) because the
   bundled value is actually the GUI-staged *pending override*
   (`ps.get_pending_constants`), not the `Constant`'s real definition — a
   real `Constant` is already resolved locally exactly like a Function.

**Consequence for this plan**: once PathInput/Sweep become source-scanned
(Workstream A/B), local *discovery* works exactly like Function/Variable/
Constant (scan source, no DB/layout fallback — the clean break stands).
Cross-user *portability* is a different concern, though: the importing
user's project may simply not have the exporter's source file at all, and
unlike a function (logic that genuinely cannot be reconstructed from a graph
node), a `PathInput`/`Sweep` is fully specified by its value — so it CAN be
losslessly reconstructed on import. `portability_service.py` therefore keeps
bundling PathInput/Sweep definitions, but the semantics shift from "the only
copy" to "an import-time fallback, used only on a local cache-miss":

- `export_pipeline` (portability_service.py:96-237): `path_inputs`/`sweeps`
  bundling (lines 189-201, 227-229) stays, now reading from
  `registry.get_path_inputs_registry()`/`get_sweeps_registry()` instead of
  `layout_store.read_all_*` — the resolved value is always available since
  every PathInput/Sweep is backed by real source on the exporting side by
  construction (even GUI-created ones, since `create_path_input`/
  `create_sweep` write to source immediately, never to GUI-only state).
- `import_pipeline_document` (replaces layout.py:617-634's
  `write_path_input`/`write_sweep` calls): for each referenced name, check
  the LOCAL registry first — already-defined name -> reuse the local
  definition untouched (same "shared by name" precedent already established
  for constants' pending-values, module docstring line ~10-13). Only on a
  local miss, call the new `create_path_input`/`create_sweep` service
  functions (Workstream A/B, item 6/7) with the bundled fallback values —
  this WRITES the object into the importer's own configured source file
  (`variable_file` or MATLAB equivalent) and refreshes the registry, so the
  imported pipeline ends up backed by real local source too, never by a
  phantom GUI-only value. If no writable target is configured on the
  importer's side, `create_path_input`/`create_sweep` fails the same way
  `create_variable` already does today (variable_service.py:61-69) — surface
  that as a new `materialization_errors` list alongside `unresolved_labels`,
  not a silent drop.
- `_unresolved_labels` (portability_service.py:463-479): PathInput/Sweep do
  NOT join this list — they auto-materialize instead (see above). It stays
  scoped to `functionNode`/`variableNode`, the two categories that carry
  actual logic an import can't synthesize from a graph node alone.
- **Submodules are unaffected** by this reclassification — `_pipeline_uses`
  closure export/import (the "Identity-based reuse" `pipeline_id` model in
  `docs/claude/pipeline-import-identity.md`) keeps working exactly as today
  regardless of whether a submodule's DB rows originated from hand-drawn
  GUI wiring or (new, Workstream C) a source-code import — both just write
  into the same tables.

**Consequence for the not-yet-built "GUI -> plain .py script" export**
(Workstream C, Direction 2): PathInput/Sweep flow through it for free once
A/B land — the codegen emits an `import` of the name from wherever it's
defined (exactly how it must already handle Function/Variable imports),
never an inlined literal. This is also why Workstream C is sequenced after
A/B rather than before.

---

## Workstream A — PathInput becomes source-scanned

### Python

1. **`scistack_gui/registry.py`**: add `_scan_module_path_inputs` /
   `_register_path_input` / `get_path_inputs_registry()`, mirroring
   `_scan_module_constants` (registry.py:471-525) exactly — scan
   `vars(module).items()`, keep non-`_`-prefixed names bound to a
   `PathInput` **or** an `EachOf` whose every alternative is a `PathInput`
   (this is how "alternate templates" now expresses itself — no special
   case needed, `EachOf` already supports wrapping `PathInput`, per
   `scifor/src/scifor/each_of.py`'s docstring). No `__module__` filtering,
   same reasoning as Constants (`PathInput` doesn't reliably expose one).
2. **`scidb/src/scidb/discover.py`**: add the same scan to `discover_module`
   (for packaged-project `scan_project`), alongside the existing
   Variable/Function/Constant checks (discover.py:130-172).
3. **`scistack_gui/domain/graph_builder.py`**: delete
   `overlay_saved_path_inputs`, `_parse_pathinput_value`, and the legacy
   `PathInput(...)` repr regex (graph_builder.py:387-409). `build_path_input_nodes`
   now takes `registry.get_path_inputs_registry()` directly — same shape
   change `const__` nodes already get from `registry.get_constants_registry()`.
4. **`scistack_gui/services/execution_service.py`**: the "missing param ->
   PathInput" branch (execution_service.py:600-630) swaps
   `layout_store.read_all_path_input_names()` for
   `registry.get_path_inputs_registry()`; multi-template `EachOf(PathInput,
   PathInput)` case falls out for free since it's now just what discovery
   already found, no re-wrapping needed at execution time.
5. **`scistack_gui/layout.py`**: delete `read_all_path_input_names`,
   `write_path_input`, `add_path_input_alternate`,
   `remove_path_input_alternate`, `deep_copy_path_input`,
   `delete_path_input` (layout.py:405-528). PathInputs are no longer
   GUI-owned state.
6. **GUI-side creation**: add `create_path_input(name, template,
   root_folder=None)` to a new small service, following
   `variable_service.create_variable`'s exact pattern (append `NAME =
   scidb.PathInput(...)` to `config.variable_file`, then
   `registry.refresh_all()`). Keeps "create from the GUI" working — it's a
   source-code write, not a GUI-only store, same as variables today. This is
   also the exact function `import_pipeline_document` calls to materialize
   a bundled PathInput the importer doesn't already have locally (see
   interaction section above) — one code path serves both callers.
7. **`scistack_gui/services/portability_service.py`**: switch
   `export_pipeline`'s `path_inputs` source from `layout_store.
   read_all_path_input_names()` to `registry.get_path_inputs_registry()`;
   switch `import_pipeline_document`'s reconciliation from `layout_store.
   write_path_input` to "reuse if locally defined, else `create_path_input`
   with the bundled value" (interaction section above).

### MATLAB

MATLAB has no module-level globals, so a `.py`-style `NAME = PathInput(...)`
binding doesn't translate directly. Convention: a **PathInput getter** — a
zero-argument `.m` function whose single output is a `scifor.PathInput(...)`
(or `scidb.PathInput(...)`) construction, named after the object it exposes
(mirrors the existing one-function-per-file rule already enforced for
regular functions).

1. **`scistack_gui/matlab_parser.py`**: add `_PATHINPUT_GETTER_RE` matching
   `<out> = (scifor\.|scidb\.)?PathInput\(` inside a zero-arg function body,
   and a `parse_matlab_path_input(path)` that returns the object's name if
   matched (static regex only — never runs MATLAB, matching this file's
   existing "extract without running MATLAB" principle at the top of the
   file). Add this check to `classify_matlab_file` (before the plain-function
   check, since a getter also matches `_FUNCTION_RE`).
2. **`scistack_gui/matlab_registry.py`**: new `_matlab_path_inputs: dict[str,
   Path]`, registered the same way `_matlab_variables` is.
3. **`config.py`**: optional explicit `[tool.scistack.matlab] path_inputs =
   [...]` list, parallel to `matlab.functions`/`matlab.variables`
   (`_resolve_glob_paths`, config.py:320-382) — folder-scan still
   auto-classifies via `classify_matlab_file` without it.

---

## Workstream B — `scifor.Sweep` as `EachOf` sugar

1. **`scifor/src/scifor/each_of.py`**: add
   ```python
   class Sweep(EachOf):
       """Named sugar for EachOf: a fixed list of alternatives for one
       constant parameter, discoverable as a top-level object (see
       docs/claude/code-discovery-categories.md)."""
   ```
   No new behavior — `isinstance(x, EachOf)` stays `True` everywhere
   downstream (foreach.py's expansion, `scidb/foreach.py`'s expansion) needs
   zero changes.
2. **`scidb/src/scidb/__init__.py`**: re-export `Sweep` next to the existing
   `EachOf` re-export (mirrors how every other scifor modifier class is
   re-exported today).
3. **Discovery** (Python): same pattern as Workstream A — add
   `_scan_module_sweeps` to `registry.py` and `discover.py`, `isinstance(obj,
   Sweep)` (not `EachOf`, so a bare inline `EachOf` used only at a call site
   stays un-discovered, same as an unwrapped literal constant today).
4. **`graph_builder.build_sweep_nodes`**: source list becomes
   `registry.get_sweeps_registry()` instead of the `sweeps` param
   (currently fed from `layout.json`, graph_builder.py:819-841).
5. **`execution_service.py`**: the "missing param -> Sweep" branch
   (execution_service.py:632-649) already does `EachOf(*values)` —
   simplifies to `inputs[param] = registry.get_sweep(param)` directly (a
   `Sweep` *is* an `EachOf`, no reconstruction needed).
6. **`layout.py`**: delete `read_all_sweep_names`, `write_sweep`,
   `delete_sweep` (layout.py:534-581).
7. **GUI creation**: `create_sweep(name, values)` appends `NAME =
   scidb.Sweep(v1, v2, ...)` to `variable_file`, same pattern as
   `create_path_input` — and, same as PathInput, doubles as the function
   `import_pipeline_document` calls to materialize a bundled Sweep missing
   locally.
8. **`portability_service.py`**: same treatment as PathInput — `export_pipeline`'s
   `sweeps` source becomes `registry.get_sweeps_registry()`;
   `import_pipeline_document` reuses-if-local-else-materializes via
   `create_sweep`.

### MATLAB

1. **`scimatlab/src/scimatlab/matlab/+scifor/Sweep.m`**: trivial subclass of
   `+scifor/EachOf.m`, mirroring the Python side exactly (same file already
   exists as the precedent for adding a new modifier class to MATLAB — see
   `docs/claude/each-of-variant-expansion.md`'s "MATLAB bridge" section for
   the shape `EachOf.m` took).
2. Same "getter function" convention as PathInput: a zero-arg `.m` function
   whose output is `scifor.Sweep(...)`, detected by
   `matlab_parser._SWEEP_GETTER_RE`, registered in `matlab_registry.py`.

---

## Workstream C — Submodule bidirectional translation via `scidb.Pipeline`

This is the large piece; sequence it last since A and B establish the
scan-a-module -> populate-registry -> build-graph-nodes pattern this reuses
at a larger grain, and since Direction 2 codegen needs A/B's named
PathInput/Sweep objects to emit clean references instead of literals.

### Direction 1: Source -> GUI (import)

`scidb.Pipeline` registration is already side-effect-free until `run_*` is
called (`pipeline.py:739`, "Zero side effects beyond the log") — the same
property that lets functions/variables/constants be discovered by *importing*
a file. Reuse this:

1. New `registry.py` (or a sibling `pipeline_registry.py`) function
   `discover_pipelines(module)`: after a user module defines
   `pipe = db.pipeline("name", uses=[...])` and registers steps at import
   time (top-level code, same as today's convention for `for_each` calls
   used in scripts), walk `scidb.pipeline._all_pipelines` (pipeline.py:514)
   collected during that import.
2. For each discovered `Pipeline`: `pipe.steps` -> manual function nodes;
   `pipe.uses` (list of `PipelineBinding`) -> `_pipeline_uses` rows, with
   `binding.key_map/.params/.iterate` written straight into
   `binding_json` — **no translation needed**, the schemas already match
   field-for-field (`pipeline_store.py:26-27` confirms `binding_json` holds
   `{key_map, params, iterate}`).
3. This becomes a new project-mode source type, alongside `modules`/
   `packages`: something like `[tool.scistack] pipelines = [...]` (files
   expected to define one or more `Pipeline`s at import time), reusing
   `_load_file_modules`'s existing import machinery
   (`registry.py:263-298`) plus the new discovery step.
4. Existing manual GUI wiring (nodes/edges created by hand, not import-
   derived) must co-exist — imported `Pipeline`s seed `_pipeline_uses`/
   manual node rows the same way a discovered `BaseVariable` seeds
   `var__` nodes without deleting hand-placed ones; identical
   already-solved problem to the current registry-vs-manual-nodes split
   (see `[[project_hypothesis_tabs_and_submodules]]` for the existing
   placement-id model this must not break).
5. Because Direction 1's import writes into the exact same
   `_pipeline_uses`/manual-node tables that `portability_service.py`
   already reads/writes, **no portability_service.py changes are needed**
   for submodules — a source-imported submodule and a hand-drawn one are
   indistinguishable to the existing export/import closure logic.

### Direction 2: GUI -> Source (export)

`execution_service.build_backend_pipeline` (execution_service.py:757) already
constructs the exact in-memory object graph to serialize — codegen is a
render of an existing structure, not new logic:

1. New `codegen.py` (scistack_gui): given a `pipeline_id`, call
   `build_backend_pipeline` (or a non-executing variant of it that skips the
   `for_each(..., pipeline=pipe)` registration side but keeps the walk) to
   get the `Pipeline` object graph, then emit:
   - one `for_each(fn, inputs={...}, outputs=[...], ...)` line per
     `StepSpec` (`spec.fn`, `spec.inputs`, `spec.outputs`,
     `spec.metadata_iterables`, `spec.options` — all already plain data on
     `StepSpec`, `pipeline.py:117-164`), with `inputs` referencing named
     PathInput/Sweep objects by import, per the interaction section above,
   - one `child.use(parent_ref.bind(key_map=..., params=..., iterate=...))`
     line per `PipelineBinding` in `pipe.uses`.
2. Reuse the disconnected-wiring decision already written up in
   `docs/claude/archive/gui-export-to-plain-python.md` for how to handle hidden/
   disconnected edges at export time (option 2 in that doc: emit a warning
   comment, don't silently skip) — this plan doesn't need to re-decide that.
3. **Round-trip fidelity for a submodule that originated from a source
   import (Direction 1) and was then hand-edited in the GUI**: export
   always regenerates fresh code from current DB/GUI state; it never
   attempts to diff against or preserve the original source file's exact
   structure/formatting. Matches the project's existing "DB is the live
   source of truth over any cached artifact" pattern used elsewhere (e.g.
   hide-not-delete). A user who wants the regenerated file to replace the
   hand-written one re-runs export and reviews the diff themselves.

### MATLAB

`+scidb/Pipeline.m` already exists (confirmed in-tree) and
`Pipeline.execution_order()` (pipeline.py:1117) is already described as "the
seam an external driver (the MATLAB bridge) runs steps through" — so the
MATLAB side of both directions rides the same `Pipeline`/`StepSpec` shape
without a parallel implementation; only the codegen target language differs
(emit `.m` `scidb.for_each(...)` / `pipe.use(...)` calls instead of `.py`
ones). Confirm exact MATLAB `for_each`/`use`/`bind` call syntax against
`+scidb/for_each.m` and `+scidb/Pipeline.m` before writing the `.m` emitter.

---

## Sequencing

1. **A (PathInput)** — smallest, establishes the "scan for isinstance(X)"
   pattern a second time (first was Constant), touches registry/discover/
   graph_builder/execution_service/layout/portability in a well-understood
   shape.
2. **B (Sweep)** — same shape as A, plus the trivial `EachOf` subclass;
   can start once A's registry-scanning helper exists to copy from.
3. **C (Submodules)** — largest; do import direction first (read-only,
   lower risk — worst case a discovered `Pipeline` just doesn't show up
   right), then export (write path, higher blast radius: generated code the
   user will run).

## Verification

- Per project convention (`CLAUDE.md`), every workstream lands with logging
  at each discovery/registration step (already the pattern in
  `registry.py`/`matlab_registry.py` — extend, don't invent a new style) and
  regression tests:
  - Workstream A/B: unit tests in `scistack-gui/tests/` scanning a fixture
    module with a named `PathInput`/`Sweep` and asserting it appears in
    `registry.get_path_inputs_registry()`/`get_sweeps_registry()` and as a
    `pathInput__`/`sweep__` graph node; a portability round-trip test
    (export a pipeline referencing a named PathInput/Sweep, import into a
    fixture DB that already has the same name locally -> local definition
    reused untouched; import into one that doesn't -> `create_path_input`/
    `create_sweep` materializes it into the importer's configured source
    file, registry refresh picks it up, node resolves normally; import into
    one with no writable target configured -> surfaces in
    `materialization_errors`); MATLAB
    fixture `.m` getter files under `scimatlab/tests/matlab/` parsed by
    `matlab_parser` tests.
  - Workstream C: round-trip test — define a `Pipeline` with a `use()` in a
    fixture module, import it, assert `_pipeline_uses`/manual nodes match;
    then export that same GUI state and assert the generated script,
    re-imported, produces an equivalent `Pipeline` graph (steps + bindings
    match by content, not by object identity).
- Because the user doesn't have Python installed locally, all test commands
  are handed over as copy-paste `pytest`/terminal commands per
  `[[feedback_user_runs_tests]]` — not run via Bash by Claude.
- After each workstream, pause and ask whether to write a
  `docs/claude/*.md` note capturing the final shape (per `CLAUDE.md`'s
  standing instruction), and update
  `docs/claude/code-discovery-categories.md`'s summary table to move
  PathInput/Sweep from "not scanned" to "scanned from source."
