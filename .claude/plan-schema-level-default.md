# Plan: one owner for "which schema keys does this run iterate"

Status: BUILT 2026-09-23 (all 6 stages; Python tests unrun, frontend tsc + 207 unit tests pass, both bundles rebuilt). Deviation: Stage 5 display comes from a `get_schema_level` RPC that runs the SAME derivation as a Run (not stamped in the graph build) — exact parity, zero cost on get_pipeline. Found + fixed on the way: F33 (panel omitted null location keys, so clearing never saved). Decisions: (1) recorded level only while the node's current wiring equals the wiring it ran as — YES; (2) alternate templates contribute the union of placeholders — YES; (3) already-stated levels stay stated, "Use automatic" clears — YES. Evidence and findings:
docs/claude/cleanup-audit.md §4.2 (F22–F27).

## The problem in one paragraph

The rule (stated on node > where it last ran > what its inputs imply > all
keys) exists once, in `scistack_gui.services.execution_service.default_schema_level`,
but only the Python routes call it. MATLAB routes read a null level as "every
key" (`loadDemographics`, one placeholder-less PathInput, ran 714 times on one
file). The panel shows null as every box ticked, so getting any other level
requires a click that becomes a permanent stated level. "Where it last ran" is
looked up by function NAME, so a new node inherits another node's level and a
node rewired to a finer input keeps its old level. And the value `[]` ("no
levels ticked") means one call (MATLAB), every key (Python Run → `for_each
(schema_keys=[])`), or unset (pipeline route), depending on the route.

## Target shape

```
                     scidb.schema_level  (NEW, the owner — NOTE 3)
                     ───────────────────
 SchemaLevel = Unset | OneCall | Keys([k...])        one type, three states
 resolve_schema_level(db, fn, stated, node_run_ids, input_levels)
     -> (SchemaLevel, rule)                           the precedence
 input_levels(db, variable_types, path_inputs)       variable + PathInput levels
 to_for_each(level) -> schema_keys arg                None / [k...] spelled once
 to_matlab_kwargs(level) -> "'k', [], ..." | ""       MATLAB spelled once
        ▲                    ▲                   ▲                   ▲
 Python Run        Python pipeline      MATLAB Run/pipeline    graph build
 (api/run.py)      (build_backend_…)    (matlab_command_*)     (node display)
```

`default_schema_level` in the GUI shrinks to gathering the node's inputs and
node-scoped run ids, then calling the owner.

## Stages

### Stage 0: diagnostics first (NOTE 2)
- Every route logs one line: `[schema-level] <fn> node=<id> stated=<unset|[]|[..]>
  -> iterating <...> (<rule>) via <route>`. The MATLAB routes do not log any
  of this today.
- `test_schema_level_routes.py`: pins today's behaviour of all four routes for
  stated ∈ {unset, [], [subject]} so later stages show exactly what changed.

### Stage 1: one value type for a level (fixes F26)
- `scidb.schema_level.SchemaLevel` with `from_stated(raw)`: `None` → Unset,
  `[]` → OneCall, `[k...]` → Keys. The GUI's stored `schemaLevel` keeps its
  JSON shape (null / [] / [k]); only its reading goes through `from_stated`.
- `to_for_each`: OneCall → `schema_keys=None` (and no iterables); Keys → list.
  `api/run.py` and `build_backend_pipeline` pass `to_for_each(level)`, never
  the raw list. Test: `[]` on the node gives one call on all three routes.

### Stage 2: move the precedence into scidb
- `resolve_schema_level` = today's rules 1–4, taking `SchemaLevel` and
  returning `(SchemaLevel, rule)`. `finest_schema_keys`,
  `variable_schema_keys` stay in `provenance_query`; the owner composes them.
- `input_levels` takes PathInput OBJECTS; an `EachOf(PathInput, ...)`
  (alternate templates) contributes the union of its alternatives'
  placeholders, with a WARN if the alternatives disagree (fixes F25).

### Stage 3: "where it last ran" becomes node-scoped (fixes F24)
- Recorded level = the level of the latest run whose `run_id` this node's
  CURRENT wiring holds in `_node_wiring`. A rewired node has no run under its
  new wiring → falls to the inputs rule. A new node of an already-run
  function → inputs rule.
- Runs with no node (scripts, terminal MATLAB) are attributed by
  `domain.node_identity` inference, which already writes `_node_wiring`, so
  they count once inferred.
- `provenance_query.recorded_schema_keys` gains a `run_ids=` form; the
  name-scoped form stays only where no node exists (`scidb` CLI).

### Stage 4: MATLAB routes use the owner (fixes F22)
- `matlab_command_service.generate_matlab_command` and the pipeline command
  resolve the level per node/step via the owner (same inputs as Python), and
  pass the resolved `SchemaLevel` to the generator.
- `api/matlab_command._resolve_iterate_keys` loses its "None = all keys"
  branch: it receives a resolved level and renders it via `to_matlab_kwargs`.
- The template (first-run) branch gets the same treatment. That is where
  `loadDemographics` fanned out.
- Test: a PathInput-only, placeholder-less function generates a command with
  NO schema kwargs; a trial-level input generates subject..trial only.

### Stage 5: the node shows what will run (fixes F23)
- The graph build stamps `data.schemaLevelResolved = {keys, one_call, rule}`
  on every function node, computed by the owner in ONE batched pass
  (`variable_schema_keys` is already batched; the node-scoped run lookup is
  one query over `_node_wiring`). Must not add per-node queries to
  `get_pipeline` (F15).
- Panel: when nothing is stated, the resolved keys show ticked in a muted
  style with "automatic: <rule>". The first click states a level copied from
  the RESOLVED keys, not from "all". A "Use automatic" button clears the stated
  level (writes null). Frontend bundles rebuilt (both vite targets).
- Manual-testing item in docs/gui-manual-testing-todo.md.

### Stage 6: remove the rival copies (F27)
- Delete `variant_resolver.build_schema_kwargs` (test-only) and its tests.
- AST/grep guard test: no module outside `scidb.schema_level` compares a
  schema level to `None`/`[]` to decide iteration.

## Decisions needed before building
1. **Stage 3 scope:** should "where it last ran" apply only when the node's
   current wiring equals the wiring of that run? Recommended: yes. Otherwise a
   rewired node silently keeps its old level.
2. **Alternate templates (Stage 2):** union of placeholders (recommended)
   or refuse when the alternatives disagree?
3. **Existing stated levels:** nodes you already clicked keep their stated
   level (recommended; the panel will show them as stated, with "Use
   automatic" to go back). Or clear every stored level once?

## Tests (per stage, run one package at a time)
scidb: `tests/test_schema_level.py` (type, precedence, input levels incl.
EachOf PathInput, to_for_each, to_matlab_kwargs).
scistack-gui: route parity (`test_schema_level_routes.py`), node display,
MATLAB command for loadDemographics-shaped and trial-level functions.
MATLAB: none needed (the generator is Python), but a manual check is in the
GUI testing doc.
