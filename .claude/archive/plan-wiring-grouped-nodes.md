# Plan: Wiring-grouped function nodes + pending constants in pull runs

Status: Stages 1 AND 2 IMPLEMENTED + VERIFIED 2026-07-18 (full GUI suite
green + visual check, user-run: call sites grouped into one node, staged
values as rows, pull run materialized the staged variant and turned the
cone green). Stage 2 trigger: user's run-until left the pending cone
yellow — pull runs compiled from DB history only. Decision made by user:
function nodes group by WIRING (one node per function + loadable-input/
output shape); constant-value call sites render inside the node as variant
rows with per-call-site state chips. scidb call_id semantics (constants
INCLUDED — foreach_config._CALL_ID_INCLUDED_KEYS) stay untouched: state
isolation per call site is preserved in the backend; only GUI presentation
groups. Background: eager run with pending sample_interval=10 minted
(ws,10) call sites → duplicate-looking canvas nodes (2026-07-18).

## Stage 1 — wiring-grouped nodes (GUI layer only)

**Identity.** `wiring_id = sha256(canonical json of {fn_name, input_params
(param→type/types), outputs, distribute?, as_table?})[:16]` — the call_id
recipe minus `__constants` — computed in `domain/graph_builder`. Node id
becomes `fn__{fn_name}__{wiring_id}`. Deterministic across builds.

**graph_builder.build_function_nodes.** Group the aggregated call sites
(keyed (fn_name, call_id)) by wiring_id. Node data:
- `variants`: one row per member call site {call_id, constants,
  state} — state from the per-call-site run_states map (unchanged
  computation), rendered as chips (`ws=30 si=5 ● green`).
- plus SYNTHESIZED rows for staged pending values that have no call site
  yet: {constants with pending value, state: "pending"} — the staged value
  is visible inside the node it will land in.
- node-level `run_state` = worst member state (red < pending < green).
- `call_id` field (used by FE for source lookup) → keep the FIRST member's
  for compatibility; the run path derives by fn name anyway.

**run_state.py** — unchanged (per-call-site propagation). graph_builder
maps call-site states onto grouped nodes afterward.

**Edges** (edge builder + scope filter): emit against wiring node ids;
dedupe identical edges from multiple member call sites.

**Migration (positions + scope membership + manual edges).** Node ids
change, and saved-position location IS scope membership. During
`_build_graph`, for each wiring node with no saved position: if any member
call-site's legacy id (`fn__{fn}__{call_id}`) has one, ADOPT it — rewrite
the position under the new id in the same scope, drop the legacy keys
(first member wins). Same rewrite for manual-edge endpoints that reference
legacy fn node ids. One-time, idempotent, logged.

**Frontend FunctionNode.tsx.** Variant rows under the label: compact
constants summary + state dot per row (green/pending/red — pending style
from the 2026-07-18 rename). Run button unchanged (derives all targets for
the fn). Sidebar FunctionSettingsPanel keeps its variant list (gains the
state chips for free if it reads the same rows).

**Tests.** Rewrite `test_pipeline_call_sites.py` expectations (ONE node
per wiring; the two-window seed = one node with two variant rows carrying
INDEPENDENT states — the no-blur guarantee moves to the chip level);
graph_builder unit tests for grouping/wiring_id/migration adoption;
scihist/scidb call-site state tests untouched (backend semantics
unchanged).

## Stage 2 — pending constants in pull runs (GUI service layer)

**Shared override helper.** Extract api/run.py's inline pending-override
block (first staged value per constant replaces the DB value — Strategy 2,
unchanged) into `execution_service.apply_pending_overrides(targets,
pending)`; run.py's eager thread uses the same helper (no duplication).

**Compiler.** `build_backend_pipeline` applies the overrides to derived
targets, so compiled steps carry staged constants →
- `plan` shows the staged variant honestly (red, real combo count) —
  the plan dialog becomes the preview of what materializing will run;
- pull runs (until/all/endpoints) WRITE the staged variant's records;
- next graph build auto-cleans the pending value → node goes green.
With Stage 1, the new call sites this mints land INSIDE the existing
wiring node as new variant rows — no node proliferation.

**Tests.** Compile-with-pending (step constants overridden); service-level
run materializes records + pending auto-cleans; plan reflects staged
variant as red.

## Stage 1 as built (deltas from plan)

- `graph_builder`: `wiring_id` (fn + sorted loadable-input shape + sorted
  outputs, sha256[:16]); `group_call_sites_by_wiring(agg, run_states,
  pending)` returns (grouped agg, node_states with worst-member group
  state, member_map for migration); `legacy_position_adoptions` /
  `legacy_edge_rewrites` are PURE plan-builders — `_build_graph` executes
  the side effects. Synthesized staged rows: `{constants: {name: value},
  state: "pending", staged: True}` — group state downgrades green→pending.
- `_build_graph`: grouping right after `_compute_run_states`;
  `filter_hidden` runs TWICE (pre-grouping for legacy per-call-site hidden
  ids, post-grouping for wiring-id deletions); migration block sits after
  graduations, patches the in-memory edges too so the first response is
  already correct.
- `execution_service`: `_scope_function_labels` judges "placed" by parsed
  fn NAME (position keys may be either id vintage); `derive_fn_targets`
  adopts manual-edge endpoints whose parsed fn name matches (rewritten
  edges reference wiring ids, which are no call_id).
- `data.call_id` on grouped nodes = the wiring id (node id suffix);
  per-member call_ids live on the variant rows.
- FunctionNode.tsx: variant rows with colored state dots (green/pending/
  red), staged rows italic amber "(staged)"; shown when >1 constant-bearing
  variant or any staged row.
- Tests: test_pipeline_call_sites.py rewritten (grouping, per-chip no-blur,
  single deduped edge set, migration adoption via TestClient);
  test_graph_builder.py gained TestWiringId/TestGroupCallSitesByWiring/
  TestLegacyMigrationHelpers; conftest `bp_node_id` now wiring-based.

## Stage 2 as built (deltas from plan)

- `execution_service.apply_pending_overrides(targets, pending)`: pure,
  Strategy 2 (first staged value wins, replacing), literal_eval typing.
  Callers re-deduplicate AFTER overriding (collisions possible).
- Eager thread (api/run.py): override moved BEFORE dedup (old inline
  post-dedup override could run duplicate targets); inline block deleted;
  labels/run_start now read the already-final target constants.
- Compiler: overrides applied to derived targets; local dedup key =
  (constants, output_type) — NOT variant_resolver.deduplicate_variants,
  whose constants-only key would drop a multi-output fn's second target.
- Tests (TestExecutionCompiler): compile carries overridden typed constant;
  plan previews staged variant red with full grid; full loop — pull run
  materializes 4 combos, variant appears in history, pending auto-cleans,
  node green with both chips.

## Out of scope
- Changing scidb call_id semantics (constants stay in call identity).
- Multi-value pending strategies (still first-value-wins).
- Variant-level run buttons inside the node (run derives all targets).
