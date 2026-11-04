# Endpoint-First Pipelines — Concept

Status: **stage 1 IMPLEMENTED + VERIFIED** (2026-07-16, user test run
green). The
declaration-shape decision landed on **Option D′** (ambient current
pipeline + `pipeline=` kwarg override); see
`.claude/plan-pipeline-registry-stage1.md` for the as-built record and
`scidb/run/pipeline.py` for the implementation (`Pipeline`, `StepSpec`, `Step`,
`db.pipeline(name)`, `run_all`/`run_until`/`plan`). **Stage 2** (composition,
`uses=`) implemented + verified same day. **Stage 3** (use-edge bindings +
endpoint verbs) implemented + verified same day — see
`.claude/plan-pipeline-binding-endpoints-stage3.md` for the as-built
record (`PipelineBinding`, `bind()`, `endpoints()`/`run_endpoints()`/
`show()`, plan `endpoint` flag). **Stage 4** (MATLAB parity) implemented
2026-07-16, pending user test runs — see
`.claude/plan-pipeline-matlab-parity-stage4.md`: Python owns the graph
(MATLAB steps are StepSpecs with bridge sentinels + `__matlab__` options;
type edges connect across languages via surrogate classes), MATLAB drives
execution via `Pipeline.execution_order()` descriptors through its normal
two-pass for_each; mixed pipelines run each step in its home language
(`pipeline_run_python_step`); `scidb.Pipeline` MATLAB classdef
(constructor creates+activates — no `scidb.run.pipeline` factory: it would
case-collide with the classdef file on macOS), `pipeline=` seam in
`+scidb/for_each.m`. MATLAB parity and the
GUI surface remain future stages; Option E spec persistence is deliberately
NOT planned (user decision 2026-07-16). Also landed with stage 1: the `@pipeline` step-function
marker was renamed **`@scistack`** (no alias — beta) and its dead
`unpack_output` option removed. This doc captures the design conversation
that led here.

## Motivation

With complex pipelines, the framework's primitives (variables, `for_each`
calls, variants) become hard to track. What the user actually wants is to
*look at their data*: figures and statistical results. The proposal is that
pipelines — especially GUI-built ones — should be expressed by chaining
endpoints, with the framework knowing that "plot X requires function Y to
have run first."

## The model: goal-directed (pull) execution

Today execution is **push-based**: the user runs `for_each` calls in script
order and the endpoint happens to be the last one. The proposal is the
Make/Snakemake model — **pull-based**: the user names a *target* (a figure, a
stats table), and the system walks backward through the dependency graph and
runs whatever is missing.

The endpoint design already half-commits to this worldview
([endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md)):
endpoints are leaf nodes, `scidb report` treats them as the payoff, and the
design doc's own framing is "endpoints exist because of a paper/report." What
is missing is only the backward walk.

**Core principle: this is a new execution + presentation layer ON TOP of the
record/variable model, not a replacement for it.** Variables and computations
stay first-class underneath — they are the provenance substrate (lineage,
branch_params, artifact stamps) that makes an endpoint trustworthy at all.
The analogy is Make: the user thinks in targets; the engine's model is still
the dependency graph.

## What already exists vs. the one gap

Pull execution needs three things. Two are done:

1. **Memoization — done.** `skip_computed` already makes "run everything
   upstream of `plot_gait`" cheap when things are current. Lazy semantics
   fall out of recursion + `skip_computed`; no new caching concept.
2. **Graph knowledge — done, but only descriptively.**
   `db.get_pipeline_structure()` and `db.list_pipeline_variants()`
   reconstruct the graph *from records of past runs*. They cannot describe a
   step that has never run.
3. **A prescriptive registry — THE GAP.** To know "plot X requires function
   Y" *before* anything has run, the pipeline must be declared, not just
   observed. Today a pipeline is a Python/MATLAB script whose `for_each`
   calls execute as they are read. The missing piece is a way to declare
   those same calls without executing them.

Note: [gui-readiness.md](gui-readiness.md) item 4 concluded a
function/pipeline registry was "not needed." That conclusion was correct for
a *visualization* GUI over an existing project. It flips once the goal is
goal-directed execution or a GUI pipeline **builder** — both need the graph
before first execution.

Each `for_each` call already names its input types, output type, constants,
and variants — so a declaration is the same call with execution deferred.
Edges between declared steps are inferred by matching output variable types
to input variable types (the same join `get_pipeline_structure()` does on
historical records, applied to declarations instead).

With the registry, `run_until(plot_gait, ...)` is:

1. Find registered ancestors of the target step (walk input-type ←
   output-type edges).
2. Topologically sort.
3. Run each with `skip_computed=True` (endpoints record like any
   other step).

A GUI pipeline builder then falls out: every node the user places is an
endpoint (or step) spec; edges are inferred from variable types; "show me
this plot" = `run_until`.

## Design requirements

### R1. Targets must be variant-scoped

"Plot Speed" is ambiguous when Speed exists at `low_hz=20` and `low_hz=30`.
A target is **endpoint + schema scope + variant signature**, not just a
function name. The disambiguation machinery already exists — `branch_params`
filtering, `AmbiguousVersionError`, D1's aggregation auto-split — the same
discipline carries into target specs. For the GUI this is a feature: the
variant picker is part of placing a plot node.

### R2. Dry-run plan before execution

Endpoints are "cheap to re-run by construction" only because upstream is
precomputed. Under pull execution, requesting a plot can trigger hours of
processing. The registry makes the fix free: a plan preview ("this will run
3 functions across 40 combos; 2 steps already current") shown before
execution. The GUI needs this anyway to render pending-vs-current state.

### R3. Don't let the abstraction hide the science

The risk of "user only sees plots" is that filter cutoffs and processing
choices become invisible plumbing. The existing antidotes — variant identity,
artifact stamping (D4), report captions with branch_params — must stay one
click away in any endpoint-first surface, not hidden behind it.

### R4. MATLAB parity shapes the declaration API

Whatever declaration form is chosen must cross the bridge (or have a natural
MATLAB analog). Explicit registration calls port trivially; Python context
managers do not. Same split as always: MATLAB touches the local environment,
Python owns graph correctness.

### R5. Serializability (for the GUI)

A GUI pipeline builder will need to emit and load pipeline definitions.
Whatever the user-facing API is, the internal representation of a declared
step should be a serializable spec (plain data: function ref, input markers,
constants, iteration kwargs), so the GUI can generate the same thing users
write in code.

## Layer placement

Per the layering rule (CLAUDE.md NOTE 3): the registry, graph inference, and
`run_until` orchestration belong in **scidb** — they need `skip_computed`,
branch_params, variable types, and the provenance graph, and nothing about
them is GUI-specific. scifor stays the single-step orchestrator, unchanged.
The GUI layer renders registered endpoints as top-level cards and calls
`run_until`.

## Open design question: shape of the pipeline declaration

The framing that emerged in review (2026-07-16): **the existing `for_each`
syntax already states everything a declaration needs** — function, input
types, constants, iteration keys. The only insufficiency is eager execution.
So the question is the smallest change that decouples *stating* a step from
*running* it. Candidate shapes, smallest delta first (code sketches;
signatures illustrative). Decision pending — see the corresponding plan file
once one is chosen.

### Option E — zero syntax change: self-registration on eager runs

`for_each` keeps executing as today, but persists its own call spec
(function, input markers, constants, iteration kwargs) to the DB as a side
effect. After one eager run of the script, the prescriptive graph exists and
`db.run_until(plot_gait)` replays from remembered specs. Matches the
original wish ("the framework would just know") with zero user-facing
change. Costs: a step must have run once to be known (GUI-created steps
need another entry path); specs go stale when the script is edited without
re-running (function-hash detectable, but a real semantic); replay requires
serializing/resolving function refs and iteration values — the "replay
registry" [gui-readiness.md](gui-readiness.md) item 4 warned about.

### Option D — one kwarg: `pipeline=` on `for_each`

```python
pipe = db.pipeline("gait_analysis")
for_each(bandpass_filter, RawSignal, low_hz=20, subject=SUBJECTS,
         pipeline=pipe)
for_each(compute_speed, FilteredSignal, subject=SUBJECTS, pipeline=pipe)
for_each(plot_gait, Speed, PathOutput("figs/gait_{subject}.png"),
         subject=SUBJECTS, pipeline=pipe)

pipe.run_until(plot_gait)
```

With `pipeline=`, the call **registers instead of executing**. This is
Option B without the context-manager magic: the behavior switch is visible
in the call itself, not ambient; converting a script is one kwarg per line;
eager and declared calls can mix in one file; and MATLAB ports trivially
(`pipeline` is an argument, not a `with` block — satisfies R4). Also covers
never-run steps, which E cannot.

### Option D′ — ambient current pipeline, kwarg as override (user proposal 2026-07-16)

> **Revised 2026-11-02 (user decision, `.claude/plan-p6-behaviour.md` stage 9).**
> Python now uses Option B's `with` block for the scope, plus an explicit
> `pipe.for_each(...)`: `db.pipeline(name)` no longer activates, and leaving
> the block always ends registration, including on an exception. The
> footgun below (registration leaking into every later call, including
> later notebook cells) happened, so the scope is now visible in the code.
> MATLAB has no `with`, so `scidb.Pipeline(...)` still activates until a
> run or `deactivate()`, and `pipe.for_each(...)` is its leak-free form. A
> `db.pipeline()` that is never entered or used logs a warning with the
> new form, because a script written the old way runs its `for_each` calls
> immediately.

Per-call `pipeline=` is noisy when a whole file is one pipeline (the common
case for this user base: linear scientist scripts). Instead, creating a
pipeline **activates** it as the ambient registration target; subsequent
`for_each` calls register into it; `run_*()` (or explicit deactivation)
ends registration. The file then reads: declare pipeline, list steps as
plain `for_each` calls, run — Snakemake-file readability with zero per-call
noise:

```python
pipe = db.pipeline("gait_analysis")          # activates

for_each(bandpass_filter, RawSignal, low_hz=20, subject=SUBJECTS)
for_each(compute_speed, FilteredSignal, subject=SUBJECTS)
for_each(plot_gait, Speed, PathOutput("figs/gait_{subject}.png"),
         subject=SUBJECTS)

pipe.run_until(plot_gait)                    # executes + deactivates
```

Unlike Option B's context manager, ambient state via a global ports to
MATLAB fine (a persistent/session variable; registration happens
Python-side through the bridge anyway) — this rescues B's ergonomics in an
R4-compatible way.

The footgun is action at a distance: create a pipeline, forget to run it,
and every `for_each` in the file silently computes nothing. Mitigations
(all should ship together if D′ is chosen):

- Loud INFO log per deferred registration ("registered step
  `compute_speed` into pipeline `gait_analysis` (deferred)").
- A warning at session end / pipeline GC if a pipeline registered steps but
  was never run or saved.
- Deferred `for_each` returns a Step handle, not results — downstream code
  using the return value fails fast rather than silently.
- Keep D's kwarg as the **explicit override**: `for_each(...,
  pipeline=None)` forces eager execution mid-file (sanity checks);
  `pipeline=other_pipe` targets a non-ambient pipeline. Ambient is the
  default, never the only way.
- Multiple pipelines in one session: activation is a stack (last created
  wins; `run_*`/deactivate pops). Composition (below) reduces the need for
  this.

### Option A — explicit `Pipeline.step()`, same signature as `for_each`

(Same mechanism as D with a different spelling: the pipeline moves from
kwarg to receiver.)

```python
pipe = db.pipeline("gait_analysis")
pipe.step(bandpass_filter, RawSignal, low_hz=20, subject=SUBJECTS)
pipe.step(compute_speed, FilteredSignal, subject=SUBJECTS)
pipe.step(plot_gait, Speed, PathOutput("figs/gait_{subject}.png"),
          subject=SUBJECTS)

pipe.run_until(plot_gait)   # or pipe.run_all()
```

`step()` takes exactly the `for_each` signature but defers execution. Zero
new concepts; MATLAB port is mechanical (`pipe.step(...)` method calls);
steps stored internally as serializable specs (satisfies R5). Cost: the
pipeline definition is a second way to write what exploration scripts
already say with `for_each`.

### Option B — capture mode over existing `for_each` calls

```python
with db.pipeline("gait_analysis") as pipe:
    for_each(bandpass_filter, RawSignal, low_hz=20, subject=SUBJECTS)
    for_each(compute_speed, FilteredSignal, subject=SUBJECTS)
    for_each(plot_gait, Speed, PathOutput(...), subject=SUBJECTS)
# inside the block, for_each records instead of executing

pipe.run_until(plot_gait)
```

Existing scripts become pipelines by indenting under one line. Cost: action
at a distance (`for_each` behaves differently depending on ambient context);
loops/conditionals in the script run at declaration time, which can confuse;
no MATLAB context-manager analog (violates R4 without a separate mechanism).

### Option C — declarative spec objects (data-first)

```python
pipe = Pipeline("gait_analysis", steps=[
    Step(bandpass_filter, inputs=[RawSignal], constants={"low_hz": 20}),
    Step(compute_speed,   inputs=[FilteredSignal]),
    Step(plot_gait,       inputs=[Speed],
         outputs=[PathOutput("figs/gait_{subject}.png")]),
], iterate={"subject": SUBJECTS})

pipe.run_until("plot_gait")
```

The pipeline IS the serializable data structure the GUI would emit —
strongest R5 story; trivially diffable/storable. Cost: a second vocabulary
diverging from `for_each`'s call shape; per-step iteration overrides get
awkward; function refs still need resolving from names when loaded from
serialized form (re-opens the registry question one level down).

**Leaning (2026-07-16, revised twice, unconfirmed):** **D′** (ambient
current pipeline with D's kwarg as explicit override) as the user-facing
form — with all four footgun mitigations shipped alongside — and C's spec
objects as the internal stored representation (R5). **E**
(self-registration) remains a possible later layer once replay/staleness
semantics are worth designing (it is the piece that would surface
already-written scripts in the GUI with zero edits). A is equivalent to D
in mechanism; B rejected on R4 grounds but its ergonomics survive in D′.

## Composition: pipelines as dependencies of pipelines (user proposal 2026-07-16)

**Stage 2 IMPLEMENTED + VERIFIED 2026-07-16** (user test run green; plan:
`.claude/plan-pipeline-composition-stage2.md`). As built:
`Pipeline(name, db, uses=[...])` / `pipe.use(other)`; graphs union as
`(owner, spec)` pairs with edges crossing boundaries; `run_until`/`plan`
resolve targets anywhere in the composed graph; `run_all` = own steps +
ancestors only (used-pipeline steps nothing consumes stay untouched);
diamond dedup by object identity; pipeline-level cycles and cross-db
`uses=` error at declaration; `db=None` sub-pipelines inherit the runner's
db (step option → owner → runner); a parent run acknowledges used
pipelines whose steps executed (never-run warning). Parameterized binding
(`with_params`) remains the open question below. Original design
discussion follows.

Pipelines should compose: a self-contained load+filter pipeline, which
analysis pipelines declare as a dependency, inheriting its steps "for
free":

```python
# loading.py
loading = db.pipeline("loading")
for_each(load_raw, PathInput("data/{subject}.csv"), subject=SUBJECTS)
for_each(bandpass_filter, RawSignal, low_hz=20)

# gait.py
from loading import loading
analysis = db.pipeline("gait_analysis", uses=[loading])
for_each(compute_speed, FilteredSignal)
for_each(plot_gait, Speed, PathOutput("figs/gait_{subject}.png"))
analysis.run_until(plot_gait)
# walk: plot_gait ← Speed ← compute_speed ← FilteredSignal
#       ← (resolved in `loading`) bandpass_filter ← load_raw
```

Why this is nearly free given the registry design: dependency edges come
from **variable types, not step order or pipeline membership** — the union
of two pipelines' step sets is just a bigger graph, and `run_until`'s
backward walk doesn't care which pipeline a producer came from. Steps are
not copied; the graphs union. The genuinely new machinery is small:

- **Explicit `uses=`, not global auto-resolution.** `run_until` could in
  principle search every pipeline registered in the DB for a producer of a
  needed type, but explicit dependencies keep pipelines self-contained,
  make ambiguity manageable (two pipelines producing the same type →
  error naming both, consistent with `AmbiguousVersionError` philosophy),
  and give the GUI a real containment hierarchy. Auto-resolution could be
  a later convenience.
- **Step identity / dedup.** Two analysis pipelines both using `loading`
  must see its steps as *the same* steps (spec identity ≈ function hash +
  inputs + constants), so graphs dedupe for display. At execution this is
  already harmless — `skip_computed` dedupes at the record level.
- **Cycle detection** across composed pipelines (error at `uses=` time or
  first walk).
- **Variant scoping at pipeline granularity (open design question).** If
  `loading` has variants (`low_hz=20` vs `30`), what does the consumer
  bind? This is R1 again, one level up. A plausible shape is parameterized
  pipelines — `uses=[loading.with_params(low_hz=20)]` — mirroring
  `Variant()` on inputs; a consumer that binds nothing fans out across the
  sub-pipeline's variants exactly as aggregation auto-split (D1) does.
  Deliberately undecided.

GUI payoff: composition gives the pipeline map its hierarchy — sub-pipelines
render as collapsible boxes, and the "loading" box is reused across every
analysis view that depends on it.

## Cross-project reuse: bindings on the use edge (2026-07-16)

A pipeline written against schema `[session, trial]` reused in a
`[subject, trial]` project must be adapted **without touching its
source**. Schema keys leak into a declaration in a bounded set of places:
iteration kwargs, `PathOutput`/`PathInput` templates, `Fixed(...)` kwargs,
`where=` filters, `schema_filter`/`schema_keys` — and, only in
aggregation mode, inside user functions (`df["session"]`; full-iteration
functions receive bare data and never see key names).

Decision: adaptation is a **binding on the use edge**, so one pipeline can
be used under different vocabularies/parameters by different parents,
non-mutating:

```python
analysis = db.pipeline("gait", uses=[
    loading.bind(key_map={"session": "subject"},
                 params={"low_hz": 30},
                 iterate={"subject": SUBJECTS}),
])
```

- `key_map` rewrites the declaration surface at composition time; records
  save under the PROJECT's keys (identity follows the project schema).
  Renaming delivered DataFrame columns into the pipeline's native
  vocabulary (the aggregation-mode fn-internals case) is deferred until
  that reuse case materializes.
- `params` overrides constants in the used pipeline's specs — a different
  computation identity by construction (constants are version keys), i.e.
  the variant-scoping answer: two parents binding different params compute
  two variants.
- `iterate` overrides iteration values (the used pipeline's hardcoded
  lists rarely match the new project's data).
- Bindings apply transitively through the used pipeline's own `uses`
  subtree; chained key_maps compose.

Convention worth documenting for NEW pipelines: declare iteration
structurally (`schema_keys=`, empty-list resolution) rather than by key
name — a portably-written pipeline needs no `key_map` at all. `schema_keys`
(renamed from `schema_level` — the old name read as numeric/positional but
the value is a list of key names) is implemented once, in scifor
(`expand_schema_keys()` in `scifor/src/scifor/schema.py`), and reused by
scidb's DB-backed resolution (`scidb/src/scidb/run/foreach.py`) — in both
Python and MATLAB (`+scifor/for_each.m`, `+scidb/for_each.m`).

## Elevating endpoints: the thin surface (2026-07-16)

Pull execution made every step target-addressable but not endpoints
*privileged* — half-intentional: a plot being an ordinary step is what
gives it lineage, skip_computed, variant identity, and stamping for free.
The endpoint machinery (drafts, PathOutput, stamps, report CLI)
already covers their leaf semantics. What remains is a thin vocabulary on
`Pipeline`, cheap because `_endpoint_policy` already detects endpoints:

- `pipe.endpoints()` — the endpoint steps (the GUI's top-level cards).
- `pipe.run_endpoints()` — "make all my figures and stats";
  each endpoint pulls its own ancestry.
- `pipe.show(plot_gait)` — draft-run one endpoint + ancestors, return the
  rendered artifact paths (the everyday "let me look at it" verb).
- `plan()` entries flagged `endpoint: True` so the GUI can invert the
  display: endpoints as entry points, processing as collapsible ancestry.

The real elevation happens in GUI/report presentation on top of these —
correct layer split (CLAUDE.md NOTE 3).

## Database interaction in pull mode (2026-07-16, clarification)

- **Registration: zero.** The for_each hook returns before any DB work;
  `db.pipeline(name)` only stores the reference.
- **Graph inference / topo: zero.** Edges come from variable classes in
  memory — why never-run steps have a graph at all.
- **`plan()`: read-only.** `check_node_state` per step: output records by
  fn/call_id, lineage staleness, and — for never-run steps — expected
  combos from the input variables' existing records. Safe on a read-only
  connection.
- **`run_*`: normal for_each traffic per step** (skip_computed queries,
  loads, saves, lineage writes). Byte-identical to eager runs.
- **Pipelines themselves are never stored in the DB** — in-session
  objects only. Cross-session pipeline discovery would require spec
  persistence (Option E territory), which is deliberately NOT planned.

## Related docs

- [endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md) —
  plot_/stat_ leaf semantics this layer targets
- [gui-readiness.md](gui-readiness.md) — the registry-not-needed conclusion
  this supersedes for the pipeline-builder case
- [variant-branch-param-pinning.md](variant-branch-param-pinning.md) —
  variant identity that target specs must carry (R1)
- [archive/scidb-for-each-internals.md](archive/scidb-for-each-internals.md) — the
  single-step machinery `run_until` composes
