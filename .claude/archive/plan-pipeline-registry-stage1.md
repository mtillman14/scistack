# Plan: Pipeline Registry + Deferred Registration (Endpoint-First, Stage 1)

Status: APPROVED 2026-07-16 (all three N-decisions confirmed by user),
IMPLEMENTED and **VERIFIED same day — full user test run green** (one test
assertion fix: `Speed.load()` returns a record object, data on `.data`).
The run also surfaced two pre-existing dev failures in
`scihist/tests/test_generates_file.py` (D1 auto-split cross-producting
save-kwarg variant groups; `_restore_schema_column_dtypes` crashing on
duplicate labels) — both fixed same day, see
[endpoints-viz-and-stats-design.md](../docs/claude/endpoints-viz-and-stats-design.md)
D1 save-kwarg alignment note. As-built deltas from the draft are marked
**[as built]** below.
Concept doc: `docs/claude/endpoint-first-pipelines.md` (Option D′ chosen:
ambient current pipeline + `pipeline=` kwarg override; C-style spec objects
internally).

## Scope

Python scidb only. Deliverables:

1. A `Pipeline` object that collects deferred `for_each` calls as step specs.
2. Ambient activation (`db.pipeline(name)` activates; `run_*` deactivates)
   with the `pipeline=` kwarg as explicit override.
3. Graph inference from variable types, `run_all()` / `run_until(target)`,
   and a `plan()` dry-run surface.
4. All four footgun mitigations from the concept doc.
5. Logging + tests (user runs tests; commands handed over at the end).

**Out of scope (later stages):** composition (`uses=`), spec persistence /
replay-from-DB (Option E self-registration), MATLAB parity, GUI surface.
MATLAB note for the future: registration would route through the bridge's
`for_each_prepare` with a pipeline flag; nothing in stage 1 blocks that.

## Grounding in existing code (verified 2026-07-16)

- `for_each(fn, inputs: dict, outputs: list[type], ..., db=None,
  **metadata_iterables)` — `scidb/foreach.py:183`. Real call shape (dict
  inputs, list outputs), so specs capture exactly these arguments.
- `ForEachConfig` (`scidb/foreach_config.py:68`) already serializes call
  identity (`__fn`, `__fn_hash`, `__inputs`, `__constants`, `__where`) —
  the StepSpec reuses it for identity rather than inventing a second
  serialization.
- Ambient-state precedent: `db.set_current_db()` (`database.py:4088`) and
  `active_db` resolution inside `for_each` — the active-pipeline global
  follows the same pattern.
- `check_node_state` / `check_combo_state` (`scidb/state.py`) — existing
  staleness API; `plan()` builds on it instead of new bookkeeping.
- **Naming collision:** `@pipeline` (step-function marker) already lives in
  `scidb/pipeline.py`. See decision N1.

## Decisions needed / proposed

- **N1. Naming — CONFIRMED, amended by user.** `Pipeline` class in the
  existing `scidb/pipeline.py`; factory `db.pipeline(name)`. The decorator
  is renamed **`@scistack`** ("telling scistack to pay attention to that
  function") **in stage 1, with NO deprecated alias** — the project is in
  beta, clean breaks preferred. The dead `unpack_output` option (set but
  never consumed anywhere in the repo; scifor spreads tuples by output
  count) is removed. `is_pipeline_function` → `is_scistack_function`;
  attr flags `SCISTACK_FLAG`/`GENERATES_FILE_ATTR`.
- **N2. `skip_computed=True` default for pipeline runs — CONFIRMED.**
  Per-step registered `skip_computed=True` always wins; steps registered
  with `track_lineage=False` are left alone (skip requires lineage).
- **N3. Non-blocking `plan()` — CONFIRMED.** Separate call returning
  structured data + readable log; `run_*` logs the plan and never prompts.

## Implementation

### 1. `StepSpec` + `Pipeline` (in `scidb/pipeline.py`)

`StepSpec`: dataclass holding the full deferred call — `fn`, `inputs`
(dict as passed), `outputs`, `metadata_iterables`, and the passthrough
flags (`where`, `as_table`, `distribute`, `save`, `finalized`,
`skip_computed`, `schema_filter`, `schema_level`, `share_limits`, `db`).
Live object refs in-session (stage 1 registry is in-memory);
`to_manifest()` returns a JSON-able projection (fn name + module, input
type names, constants via `ForEachConfig.to_version_keys()`, iteration
keys) for display and future GUI/persistence — no load-from-JSON replay in
stage 1.

`Pipeline(name, db)`:
- `steps: list[StepSpec]`; `register(spec) -> Step` (the handle).
- `_deps()`/`_topo_order()` — infer edges by matching output variable
  classes to input variable classes (unwrapping input markers — `Variant`,
  `Fixed`, `EachOf`, `Merge`, `AcrossVariants`, `ColumnSelection`;
  `PathInput`/constants/DataFrames contribute no edge). **[as built]
  Multiple producers of one type is NOT an error** — variant branches
  legitimately produce the same type from different constants, so all
  producers become prerequisites of that type's consumers (fan-in);
  variant disambiguation stays at load time (`AmbiguousVersionError`),
  its existing layer. Only cycles error → `PipelineCycleError`
  (`scidb.exceptions`; the planned `AmbiguousStepError` was dropped).
- `plan(target=None) -> list[dict]` — topological order (ancestors of
  target, or all steps), each with node state from `check_node_state`
  (`green`/`red`) and combo counts. Logged as a readable table under
  operation name `pipeline_plan`.
- `run_all(**overrides)` / `run_until(target, finalized=None, **overrides)`
  — topo-sort, then call the real `for_each` per spec with
  `skip_computed=True` default (N2). `target` accepts the function, the
  Step handle, or the function name string. `finalized` applies to the
  target step only; other steps keep their registered flags. Execution
  temporarily deactivates the pipeline so inner `for_each` calls run eager.
- `deactivate()` — pop without running (explicit escape).

`Step` handle: wraps its `StepSpec`; `__repr__` identifies the pipeline and
fn; any DataFrame-ish attribute access raises with a pointed message
("step is deferred — call pipe.run_until(...)"), so code that used
`for_each`'s return value fails fast, not silently.

### 2. Ambient activation

Module-level activation **stack** in `scidb/pipeline.py` (pattern-matched
to `set_current_db`): `db.pipeline(name)` creates a `Pipeline`, pushes it,
returns it. `run_all`/`run_until`/`deactivate` pop it. Accessor
`scidb.active_pipeline()`.

### 3. `for_each` integration (`scidb/foreach.py`)

New kwarg `pipeline=_UNSET` (sentinel):
- `_UNSET` → if an active pipeline exists, build a `StepSpec` from the
  call args, `register`, log INFO `pipeline_step_registered` (fn name,
  pipeline name, "deferred"), return the `Step` handle. Otherwise eager as
  today.
- `None` → force eager (mid-file sanity checks) regardless of ambient
  state.
- a `Pipeline` instance → register into that pipeline (non-ambient target).

Registration happens **before** any loading/DB work — a deferred call must
have zero side effects.

### 4. Footgun mitigations (all ship together)

- INFO log per deferred registration (above).
- `atexit` check: any pipeline with registered steps where none of
  `plan`/`run_all`/`run_until`/`deactivate` was called → WARNING
  `pipeline_never_run` naming the pipeline and step count.
- `Step` handle fails fast on data-like use (above).
- `pipeline=None` eager override (above).

### 5. Logging

Follow the scistacklog conventions (`scidb/log.py`); snake_case operation
names (`pipeline_step_registered`, `pipeline_plan`, `pipeline_run_started`,
`pipeline_step_skipped_current`, `pipeline_never_run`) — no numeric step
prefixes. `plan()` output includes per-step green/red state so staleness
internals are observable (CLAUDE.md NOTE 2).

### 6. Tests — `scidb/tests/test_pipeline_registry.py`

- Ambient registration: `db.pipeline` + plain `for_each` calls register,
  nothing executes, `Step` handles returned.
- `pipeline=None` forces eager mid-pipeline; `pipeline=other` targets a
  non-ambient pipeline; no active pipeline → unchanged eager behavior
  (regression guard on the sentinel default).
- Graph: edges inferred through bare types and wrappers; multi-producer
  fan-in ordering; `PipelineCycleError` on cycles.
- `run_all` executes in topo order regardless of registration order;
  results land in the DB identically to the same calls run eagerly
  (byte-identical version_keys — the spec must not perturb identity).
- `run_until(plot_x)` runs only ancestors + target; unrelated branch
  untouched; second `run_until` skips current steps (`skip_computed`
  integration).
- `finalized=True` on `run_until` reaches the target endpoint step only.
- `plan()` reports green/red correctly before/after runs.
- Never-run warning fires; `deactivate()` suppresses it.
- `Step` handle raises on data-like use.

### 7. Docs

Update `docs/claude/endpoint-first-pipelines.md` status (concept → stage 1
implemented) and cross-link from `gui-readiness.md` item 4.

## Execution order

1. `StepSpec` + `Pipeline` skeleton + activation stack (no for_each hook) —
   unit-testable graph/topo logic first.
2. `for_each` `pipeline=` sentinel hook + `db.pipeline()` factory.
3. `run_all`/`run_until` + `plan()` on `check_node_state`.
4. Mitigations + logging.
5. Tests (user runs: `pytest scidb/tests/test_pipeline_registry.py -v`).
6. Doc updates.
