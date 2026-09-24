# Plan: one owner for "which call sites form one pipeline step" (F38)

Status: BUILT 2026-09-24 (option A chosen by user; uncommitted, pytest unrun). Owner of the name: `scidb.parameter.declared_input_names` (renamed from declared_parameter_names, now Parameters + PathInputs; stamped via `stamp_path_input_name`). Step rule: `scidb.database.call_site_wiring_ids`. GUI collectors renamed `build_run_declared_names` / `_collect_declared_names`. MATLAB needed no change (parameter_names struct already generic). Tests: scidb/tests/test_step_grouping_f38.py, scistack-gui/tests/test_step_grouping_parity_f38.py.

## Finding
Two rules group recorded call sites into steps:

| | GUI `graph_builder.group_call_sites_by_wiring` | CLI `inspect.graph.build_pipeline_graph` |
|---|---|---|
| recipe | `scidb.provenance.compute_wiring_id` | its own tuple, hashed by `_step_id` |
| outputs | in the key | not in the key |
| PathInputs | `{param: DECLARED PathInput name}` | param names only |
| input spelling | normalized (`"X"` = `["X"]`, specs stripped) | raw |
| grain | per call site (aggregate first), then wiring | per variant row |

So `scidb graph` and the canvas can disagree about how many steps a
pipeline has: same inputs but different outputs, or different PathInputs on
one argument = 2 canvas nodes, 1 CLI step. The CLI docstring also cites a
2026-06-21 "template change does not fork" decision that the GUI has since
superseded (it forks by declared PathInput).

Root cause: the GUI key needs the DECLARED PathInput name, and the database
does not record it. The GUI reconstructs it from its registry plus its own
`_pipeline_path_input_history`; the CLI has neither, so it cannot compute
the same key. Same shape as B1 (Parameter declared name), fixed there by
recording the name at write time.

## Decision needed: where the PathInput name comes from
**A. Record it (recommended).** Stamp the declared PathInput name on the
PathInput edge (`_invocation_input.declared_name`, the column B1 added),
from a scidb-owned `declared_path_input_names(inputs, explicit)` beside
`declared_parameter_names`. Sources: `PathInput.name` (set by the entities
loader / discovery, like `Parameter.name`) or the caller stating it (GUI
wiring, generated MATLAB via the existing `parameter_names` option or a new
`path_input_names`). Then `scidb.pipeline_steps(...)` computes every step
key from facts, and the GUI prefers the recorded name over its registry
match. Existing records have no name: they fall back to the spec (beta, no
backfill).
Cost: save path + bridge + both generators + reader; ~B1-sized.

**B. Share only the recipe.** The CLI calls `compute_wiring_id` per call
site, identifying PathInputs by spec (it has no names). Outputs and
spelling then agree, but PathInputs still diverge: a template edit forks the
CLI step but not the canvas node, and one spec shared by two declared names
does the opposite. Cost: CLI only; small.

## Stages (for A)
1. scidb: `PathInput.name`; `declared_path_input_names` owner; the save path
   stamps it on PathInput edges; `invocation_edges_batch` reports it;
   `aggregate_pipeline_variants` carries `{param: declared name}` per call site.
2. scidb: `pipeline_steps(variants)`, a pure function: per call site ->
   `compute_wiring_id(fn, inputs, outputs, path_input_names)` with the spec
   fallback. The CLI's `build_pipeline_graph` groups through it; `_step_id`
   and the private tuple are deleted.
3. GUI: `wiring_id` inputs take the recorded name first, registry second
   (one source per fact). `group_call_sites_by_wiring` keeps its
   GUI-only part (allocation tokens / `is_current` = placement), which is
   presentation, not step identity.
4. MATLAB: `+scidb/for_each.m` / bridge / generators pass the names.
5. Tests: the same history grouped by the CLI and the GUI gives the same
   partition (with outputs and PathInput differences); a recorded name
   survives a template edit; logging of which name source each PathInput
   used.

## Not in scope
The GUI's node allocation (`token_for`): several wirings may map to one
node by user intent; that is placement, and stays GUI-owned.
