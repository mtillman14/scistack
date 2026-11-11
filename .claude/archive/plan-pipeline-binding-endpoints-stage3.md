# Plan: Use-Edge Bindings + Endpoint Verbs (Endpoint-First, Stage 3)

Status: APPROVED 2026-07-16 (E1–E4 all confirmed by user), IMPLEMENTED and
**VERIFIED same day — full user test run green**. As-built deltas:
- `compose()` carries already-resolved param maps instead of re-resolving
  (outer params may target steps a deeper subtree doesn't contain) and
  memoizes composed bindings so repeated closure walks return stable spec
  objects.
- Rewritten specs carry an `origin` attr so Step-handle targeting follows
  rewrites.
- `_endpoint_kind(fn_name)` extracted in foreach.py (side-effect-free
  subset of `_endpoint_policy`) — shared detection, no duplication.
- `run_endpoints` with zero targets warns `pipeline_run_skipped` naming
  the include_used escape hatch.
Concept: `docs/claude/endpoint-first-pipelines.md` ("Cross-project reuse"
and "Elevating endpoints" sections). Builds on stages 1+2 (both verified;
commits `7f77ece` and predecessors).

## Scope

Two features, one stage (they share the composition seam):

**A. `PipelineBinding`** — adapt a used pipeline without touching its
source: `uses=[loading.bind(key_map=..., params=..., iterate=...)]`.
**B. Endpoint verbs** — `endpoints()`, `run_endpoints()`, `show()`,
`endpoint` flag in `plan()`.

**Explicitly out of scope:** spec persistence / cross-session pipeline
discovery (user decision: NOT planned); aggregation-mode delivered-column
renaming (key_map v2 — deferred until that reuse case materializes);
MATLAB parity; GUI surface.

## A. PipelineBinding

### API

```python
binding = loading.bind(
    key_map={"session": "subject"},   # native key -> project key
    params={"low_hz": 30},            # constant overrides (bare or "fn.param")
    iterate={"subject": SUBJECTS},    # iteration-value overrides (post-map keys)
)
analysis = db.pipeline("gait", uses=[binding])   # or analysis.use(binding)
```

`bind()` returns a lightweight `PipelineBinding(pipeline, key_map, params,
iterate)` — no mutation of the bound pipeline, so different parents can
bind the same pipeline differently. `uses=`/`use()` accept bindings and
bare pipelines interchangeably (a bare pipeline is the identity binding).

### Semantics

- **Non-mutating spec rewrite at composition.** `_composed_steps()`
  materializes rewritten StepSpec COPIES for bound edges (cached per
  binding). The original pipeline's specs are untouched; its own
  `run_all()` still runs the unbound versions.
- **`key_map` rewrite surface (v1):** iteration kwarg names, `PathOutput`/
  `PathInput` template placeholders (`{session}` → `{subject}`; branch-
  param placeholders like `{fn.low_hz}` untouched), `Fixed(...)` metadata
  keys (rebuilt wrapper), structured `where=` filters (`schema_key`-based),
  `schema_filter`/`schema_level` keys. **Raw-SQL `where` strings cannot be
  safely rewritten → WARN naming the step, leave as-is** (observability
  over silent breakage). Records save under PROJECT keys — identity
  follows the project schema, correct by construction.
- **`params` = constant overrides = new variants.** Overridden constants
  flow into `ForEachConfig` → different version keys → a distinct branch,
  exactly the load-time variant model. Targeting: bare names
  (`low_hz`) suffix-match against the subtree's constant-input names,
  consistent with load's branch-param suffix matching; `"fn.param"`
  disambiguates; a bare name matching constants in multiple functions →
  `AmbiguousParamError`; a name matching nothing → `ValueError` at bind
  time (fail fast, not at run).
- **`iterate` value overrides** replace `metadata_iterables[key]` for
  subtree steps that iterate that (post-key_map) key — the bound
  pipeline's hardcoded lists rarely match the new project's data.
- **Transitivity:** a binding applies to the entire subtree reached
  through its edge (the whole subtree was written in the foreign
  vocabulary); chained key_maps compose (outer ∘ inner).
- **Dedup interaction:** identity for diamond dedup becomes
  `(id(spec), binding signature)` — the same sub-pipeline bound with
  DIFFERENT params through two parents is two genuinely different
  computations and must run twice (they are two variants); bound
  identically (or unbound) through two paths still dedupes.
- **Acknowledgment (C4)** passes through to the underlying pipeline.

### Logging (NOTE 2)

`pipeline_bound` at use time (key_map/params/iterate summary);
`pipeline_binding_rewrite` DEBUG per rewritten spec naming what changed;
the raw-SQL where WARN.

## B. Endpoint verbs

- `Pipeline.endpoints() -> list[Step-like info]` — composed-graph steps
  whose fn is an endpoint per `scidb.foreach._endpoint_policy` (the
  existing single source of truth; no new detection logic).
- `run_endpoints(finalized=False, skip_computed=True, include_used=False)`
  — run_until over every endpoint target at once (one topo pass, union of
  ancestries). Default scope: OWN endpoints (C1-consistent);
  `include_used=True` widens to the composed graph.
- `show(target, skip_computed=True)` — resolve target (must be an
  endpoint; pointed error otherwise), draft-run it + ancestors
  (`finalized=False`), collect and RETURN the rendered artifact paths
  (from the endpoint step's result rows), logging each
  (`pipeline_show: rendered -> <path>`). No OS-open in v1 — the caller
  (user/GUI) opens.
- `plan()` entries gain `"endpoint": bool`.

## Decisions needed / proposed

- **E1.** `run_endpoints` scope: own endpoints by default,
  `include_used=True` opt-in. → **Confirm.**
- **E2.** `params` targeting: bare + suffix matching, `"fn.param"` to
  disambiguate, `AmbiguousParamError`/`ValueError` at bind time.
  → **Confirm.**
- **E3.** key_map v1 surface as listed; raw-SQL `where` = WARN not error;
  delivered-column renaming deferred. → **Confirm.**
- **E4.** Method name `bind()` (subsumes the earlier `with_params`
  working name). → **Confirm.**

## Implementation order

1. `PipelineBinding` + `bind()` + `uses=`/`use()` acceptance + validation
   (param resolution, cycle/db checks through bindings).
2. Spec-rewrite machinery (key_map / params / iterate), cached per
   binding; `_composed_steps()` returns (owner, binding, rewritten-spec).
3. Dedup + acknowledgment through bindings.
4. Endpoint verbs (independent of 1–3; can land first if convenient).
5. Tests (`TestBinding`, `TestEndpointVerbs` in
   `scidb/tests/test_pipeline_registry.py`):
   - params: distinct variant records; original pipeline unaffected; two
     parents/two params → both variants computed; suffix + namespaced
     targeting; ambiguity + unknown-name errors at bind time.
   - key_map: renamed iteration keys drive project schema end-to-end;
     PathOutput placeholder rename; Fixed kwargs; raw-SQL where warns;
     transitive through nested uses; iterate value override.
   - endpoints(): detection via prefix policy; run_endpoints scope both
     ways; show() returns real rendered paths (draft mode, no records);
     plan endpoint flag.
6. Docs/status/memory updates; user runs
   `pytest scidb/tests/test_pipeline_registry.py -v`.
