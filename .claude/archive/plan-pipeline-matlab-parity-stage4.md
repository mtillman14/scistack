# Plan: Pipeline Registry MATLAB Parity (Endpoint-First, Stage 4)

Status: IMPLEMENTED 2026-07-16 (M1–M4 as proposed); **Python-side tests
green** (user-run: test_bridge_pipeline.py + full test_pipeline_registry.py
regression). **MATLAB test run DEFERRED by user** —
`TestPipelineRegistry.m` (8 tests) written but not yet executed; run it
before relying on the MATLAB surface. As-built deltas and v1 limitations:

- **No separate factory function**: `scidb.pipeline(name)` would collide
  with `+scidb/Pipeline.m` on case-insensitive filesystems (macOS!), so
  creation is the constructor: `pipe = scidb.Pipeline("gait", 'db', db,
  'uses', {...})` — creates, activates, self-registers in the MATLAB
  wrapper registry.
- Python side: `_select()` extracted (shared by run_*/execution_order);
  `_execute_step()` extracted from `_run`'s loop (shared with the bridge's
  mixed-pipeline driver); `execution_order()` public descriptor API; the
  MATLAB-step guard raises from `_run` on `__matlab__` options.
- Bridge caches: `_pipeline_cache`, `_binding_cache`, `_pipeline_run_cache`
  (execution_order returns run_handle + descriptors; run cache holds the
  live pairs/order for `pipeline_run_python_step`; `pipeline_run_free`
  releases — MATLAB's drive() does so via onCleanup).
- **v1 limitations** (documented in Pipeline.m / here):
  - key_map bindings do NOT rewrite a MATLAB step's `where=` filter on
    replay (MATLAB replays its stored filter object; Python's rewrite
    applies only to its spec copy).
  - `plan()` state for MATLAB steps can read red/stale even when current:
    `check_node_state` compares the SENTINEL's function hash, not the
    stored `__matlab_fn_hash__`. call_id matches (it excludes __fn_hash),
    so step identity is right; only the green/red verdict is pessimistic.
    Fix later by feeding `__matlab_fn_hash__` through to the state check.
  - `share_limits`/`introspect` are preserved via MATLAB-stored opts (not
    part of the Python descriptor) — full fidelity on replay, but a
    binding cannot rewrite share_limits keys (key_map limitation above
    applies).
Builds on stages 1–3 (all verified; latest commit `44f27bf`). Follows the
D7 principle: **MATLAB touches the local environment; Python owns graph
correctness.** Precedent machinery reused: `_make_matlab_fn_sentinel`,
kind-tagged `inputs_spec` + `_reconstruct_input_for_keys`, surrogate
variable classes (`get_surrogate_class`), the two-pass
`for_each_prepare`/`for_each_save` flow.

## The one hard constraint (drives the whole design)

A Python-side StepSpec must hold `fn`, but **MATLAB function handles
cannot cross the bridge for Python-side replay** (same wall as D7's
"figure handles never cross"). Consequence:

- **Registration, graph, topo order, plan, bindings, dedup: Python-side**
  — MATLAB steps register real StepSpecs whose `fn` is the existing
  bridge sentinel (`fn.__name__` = MATLAB fn name; erroring if invoked)
  and whose inputs are reconstructed Python objects (surrogate classes →
  type-edge inference works, INCLUDING edges between MATLAB and Python
  steps).
- **Execution: MATLAB-driven** — MATLAB asks Python for the topo-ordered
  run list and drives each step through its normal two-pass
  `scidb.for_each`. Python's `Pipeline._run` gains a guard: a sentinel
  `fn` raises "this pipeline contains MATLAB steps — run it from MATLAB".

## MATLAB-side surface

- **`+scidb/Pipeline.m`** (handle classdef): wraps the Python Pipeline;
  stores what cannot cross the bridge — per-step MATLAB fn handles + the
  raw MATLAB call args (inputs struct, outputs cell, opts, meta args) —
  keyed by the Python-assigned step index. Methods: `run_all`,
  `run_until(target)`, `plan`, `deactivate`, `bind`, `use`, `endpoints`,
  `run_endpoints`, `show`. A MATLAB-side registry
  (`+scidb/+internal/pipeline_registry.m`, persistent map name→wrapper)
  lets `for_each` and composed runs find owner wrappers.
- **`scidb.pipeline(name, ...)`** function: bridge-creates + activates the
  Python pipeline, wraps it, registers the wrapper.
- **`+scidb/for_each.m` registration seam:** one cheap bridge call at
  entry (`pipeline_active_name()`); if a pipeline is active (or a
  `'pipeline'` name-value names one), marshal args exactly as the
  existing prepare path does (reuse that marshalling) and call
  `pipeline_register_step` instead of `for_each_prepare`; store fn handle
  + raw args in the owner wrapper at the returned index; return a
  lightweight deferred-step struct (`.deferred = true`, erroring accessor
  behavior is MATLAB-idiomatic: document, don't emulate `__getattr__`).
  `'pipeline','none'` forces eager (Python `pipeline=None` analog).

## Bridge entries (new, in `scimatlab/src/scimatlab/bridge.py`)

- `pipeline_create(name, db) -> handle` — creates + activates; returns a
  cache id (same server-side cache pattern as for_each handles).
- `pipeline_active_name() -> str | ""` — the ambient check.
- `pipeline_register_step(pipe_handle, fn_name, fn_hash, inputs_spec,
  output_class_names, metadata_iterables, options...) -> step_index` —
  reconstructs inputs, builds sentinel, registers; the StepSpec's options
  carry `__matlab__: True` + fn_hash so replay descriptors and
  skip_computed hashing stay MATLAB-correct.
- `pipeline_bind(pipe_handle, key_map, params, iterate) -> binding_handle`
  and `pipeline_use(pipe_handle, other_or_binding_handle)` — forwarders;
  bind-time validation errors surface as MATLAB errors.
- `pipeline_execution_order(pipe_handle, target?, mode, finalized?) ->
  list of descriptors` — mode ∈ {all, until, endpoints, show}. Resolves
  targets/ancestors/topo Python-side (deactivates, acknowledges — the
  same semantics as `_run` minus execution), then per ordered step
  returns: `{owner_pipeline, step_index, fn_name, is_matlab,
  apply_finalized, skip_computed, metadata_iterables (POST-binding),
  constant_overrides {input: value}, path_templates {input: rewritten
  template}}` — i.e. a bound step's rewritten surface crosses as plain
  data; MATLAB substitutes constants into its stored inputs struct,
  rebuilds `scifor.PathOutput`s from returned templates, and passes the
  returned iterables.
- `pipeline_run_python_step(pipe_handle, owner, step_index, opts)` —
  executes a REAL-callable (Python-registered) step Python-side, so
  mixed-language pipelines work: MATLAB drives the order; each step runs
  in its home language.
- `pipeline_plan(pipe_handle, target?)` / `pipeline_endpoints(...)` —
  forwarders returning MATLAB-friendly structs.

## Decisions needed / proposed

- **M1. Execution locus.** MATLAB drives; Python returns order; Python
  `_run` errors on sentinel steps with a pointed message. → **Confirm.**
- **M2. Mixed-language pipelines in v1.** Include
  `pipeline_run_python_step` so a MATLAB-driven run executes
  Python-registered steps in Python (type edges already connect them).
  Cheap (the spec's fn is a real callable) and it is the payoff of
  Python-side graph ownership. → **Confirm.**
- **M3. Bindings in v1.** Include key_map/params/iterate: the rewrite
  already happens Python-side; MATLAB only consumes rewritten DATA
  (iterables, constant values, template strings). → **Confirm.**
- **M4. Deferred-step return shape in MATLAB.** A struct with
  `.deferred=true`, `.pipeline`, `.step_index`, `.fn_name` — no
  fail-fast emulation (MATLAB has no cheap `__getattr__` analog; the
  Python-side never-run warning still fires). → **Confirm.**

## Python-side changes (small)

- `Pipeline._run`: sentinel guard (detect `__lineage_wrapper__` sentinel /
  `__matlab__` option → raise with "run from MATLAB" message).
- `Pipeline` helpers the bridge needs: `execution_order(...)` extracting
  the descriptor list from the existing `_topo_order`/`_resolve_target`/
  binding-rewrite machinery (refactor `_run`'s selection logic out;
  execution loop unchanged).
- Nothing else — registration/graph/plan/bindings already work on specs
  whose fn is a sentinel.

## Tests

- **Python-side (pytest, `scimatlab/tests/test_bridge_pipeline.py`):**
  register via bridge entries with kind-tagged specs → graph edges via
  surrogate classes; execution_order correctness incl. bound steps'
  rewritten descriptors; sentinel guard on Python run_*; mixed pipeline:
  Python step executes via pipeline_run_python_step, MATLAB step
  descriptor returned unexecuted.
- **MATLAB-side (user-run, `scimatlab/tests/matlab/scidb/`):**
  `TestPipelineRegistry.m` — register/defer, run_until order + skip on
  second run, plan, composition, a bound (params) variant, plot endpoint
  `show`. Mirrors the Python suite's core cases at smaller scale.

## Execution order

M-decisions → Python-side refactor (execution_order + guard) → bridge
entries + pytest → MATLAB classdef/function files → MATLAB tests handed
to user → docs (`endpoint-first-pipelines.md` MATLAB section, D7-style
"as built") + memory.
