# Plan: Pipeline Composition via `uses=` (Endpoint-First, Stage 2)

Status: APPROVED 2026-07-16 (C1–C4 all confirmed by user), IMPLEMENTED and
**VERIFIED same day — full user test run green** (the run also re-exercised
all of stage 1 through the composed-graph refactor, since a pipeline with
no `uses=` composes with itself). As built matches the plan; the one
addition is a `pipeline_run_skipped` WARN when `run_all()` is called on a
pipeline with no own steps (umbrella case) so the C1 scoping is loud.
Concept: `docs/claude/endpoint-first-pipelines.md` (Composition section);
builds directly on stage 1 (`.claude/plan-pipeline-registry-stage1.md`,
verified 2026-07-16).

## Scope

Pipelines declare other pipelines as dependencies and inherit their steps
"for free" — the graphs union, and `run_until`/`plan` walk across pipeline
boundaries:

```python
# loading.py
loading = db.pipeline("loading")
for_each(load_raw, {"path": PathInput("data/{subject}.csv")}, [RawSignal],
         subject=SUBJECTS)
for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
         subject=SUBJECTS)
loading.deactivate()          # declared-only is a valid end state now

# gait.py
analysis = db.pipeline("gait_analysis", uses=[loading])
for_each(compute_speed, {"filtered": Filtered}, [Speed], subject=SUBJECTS)
analysis.run_until(compute_speed)
# resolves Filtered's producer inside `loading`, runs bandpass ← load_raw
# first (skip_computed-memoized as always)
```

**Out of scope (later stages):** parameterized binding
(`uses=[loading.with_params(low_hz=20)]` — the variant-scoping open
question stays deliberately open), spec persistence / Option E, MATLAB,
GUI surface.

## Why this is cheap (stage 1 groundwork)

Edges come from variable types, not pipeline membership — the union of two
pipelines' step lists is just a bigger graph, and stage 1's `_deps` /
`_topo_order` / fan-in semantics apply unchanged. Because pipelines are
in-session objects, a shared sub-pipeline is the SAME object everywhere it
is used, so diamond dependencies (A uses B and C; B and C use D) dedupe by
object identity — no spec-hash identity machinery needed until pipelines
persist (Option E stage).

## Decisions needed / proposed

- **C1. `run_all()` scope on a composed pipeline.** Proposed: run this
  pipeline's OWN steps plus their ancestors from used pipelines — NOT
  unrelated steps a used pipeline happens to contain (a loading pipeline
  may load ten signals; an analysis using two shouldn't compute ten).
  `run_all()` thus becomes "run_until(all of my own steps)". A used
  pipeline's own `run_all()` still runs all of its steps. → **Confirm.**
- **C2. Target resolution crosses boundaries.** `run_until(bandpass)` /
  `plan(bandpass)` on `analysis` may name a step that lives in `loading`.
  Proposed: yes — targets resolve over the composed graph; ambiguous names
  (same fn name in two pipelines) match all, consistent with stage 1
  multi-registration semantics. → **Confirm.**
- **C3. Cross-database composition is an error.** If a used pipeline is
  bound to a different DatabaseManager than the user, raise at `uses=`
  time (fail fast; per-step `db=` overrides still win at run time as in
  stage 1). A used pipeline with `db=None` inherits the user's db.
  → **Confirm.**
- **C4. Being used + parent run acknowledges the never-run warning.**
  A pipeline that exists only as a dependency should not warn at session
  end when a parent ran (its steps executed); explicit `deactivate()`
  also remains a valid end state for declaration-only files. → **Confirm.**

## Implementation (all in `scidb/pipeline.py` + tests)

1. **`Pipeline(name, db=None, uses=())`** and **`pipe.use(other)`**.
   Validation at declaration time: every entry is a `Pipeline`;
   pipeline-level cycle check (walk the `uses` closure, error
   `PipelineCycleError` on self-reachability); C3 db check. `db.pipeline()`
   factory grows the `uses=` passthrough.
2. **Composed step list.** `_composed_steps() -> list[tuple[Pipeline,
   StepSpec]]`: depth-first over `uses` closure then own steps, deduped by
   `id(spec)` (diamond case), stable order (used pipelines in declaration
   order, then own registration order). `_deps`/`_topo_order`/
   `_resolve_target`/`_ancestors` refactor from `self.steps` indices to
   composed-list indices — mechanical, the algorithms are unchanged.
3. **Execution.** `_run` resolves each spec's db as: spec option → owner
   pipeline's db → self db. Logs gain the owner: `pipeline_step_run:
   'loading:bandpass' (via pipeline 'gait_analysis', ...)`. Running marks
   `_acknowledged` on self AND on every used pipeline whose steps executed
   (C4). `run_all()` = own steps as targets (C1).
4. **`plan()`** over the composed graph; each entry gains a `"pipeline"`
   field (owner name); `to_manifest()` likewise.
5. **Ambient interplay:** none — `uses=` never activates anything;
   registration always targets the active pipeline itself, exactly as in
   stage 1.
6. **Tests** (`scidb/tests/test_pipeline_registry.py`, new
   `TestComposition` class):
   - `run_until` resolves a producer inside a used pipeline and runs it
     first; unrelated used-pipeline steps untouched (C1).
   - Target inside the used pipeline (C2).
   - Diamond: D's steps appear once in `_composed_steps()` and execute
     once.
   - Pipeline-level cycle (A uses B, B uses A) raises at declaration.
   - Cross-db uses= raises (C3); db=None used pipeline inherits.
   - Parent run suppresses the used pipeline's never-run flag; a used-but-
     never-executed pipeline still flags (C4).
   - `plan()` entries carry owner pipeline names; skip_computed second
     composed run skips across the boundary.
7. **Docs:** composition section of `endpoint-first-pipelines.md` →
   as-built; memory update.

## Execution order

Decisions C1–C4 → implementation → tests handed to user → docs/status.
