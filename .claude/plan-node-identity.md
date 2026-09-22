# Plan: allocated node identity

Implements **D-2026-09-22-1** (facts get computed ids, intents get allocated
ids) and **D-2026-09-22-2** (how a node claims an invocation). The argument is
`docs/claude/node-identity.md`; this is the work.

## What changes, in one sentence

A canvas function node stops being identified by `fn__{fn}__{wiring_id}` — a
hash of its *recorded* input bindings — and gets an allocated id minted once,
with `wiring_id` demoted from identity to attribute.

## Why it is worth doing

Three symptoms, all one cause:

- a node duplicates after a run through a drawn edge (the run records the
  edge, the hash moves, the old hash is still a node);
- every statement keyed by that id is stranded when it moves — today only
  `columnSelections` is rescued, and since 2026-09-22 the rest via
  `_migrate_node_statements`, which this change makes unnecessary;
- `intent-and-fact.md` §5 makes `wiring` an aspect and §3 makes `subject_ref`
  a `wiring_id`, so the statement that sets the wiring is keyed by the wiring.

## The shape

| | before | after |
|---|---|---|
| node id | `fn__{fn}__{wiring_id}` | `fn__{label}__{uuid4[:8]}` — the shape manual nodes already use |
| current wiring | implied by the id | resolved from the node's `wiring` statements |
| recorded wirings | implied by the id (exactly one) | `_node_wiring` rows |
| attaching history | hash equality | recorded at dispatch, else inferred |

**`_node_wiring`** — the one new table:
`(node_id, wiring_id, run_id, first_seen, last_seen)`, append-only, one row
per distinct wiring a node has run as. It lives GUI-side beside `_intent`,
never in provenance (`intent-and-fact.md` §8 — scidb must not import
`pipeline_store`, and fact is never edited).

---

## Stages

### Stage 1 — the table and its accessors

`_node_wiring` + `ensure_tables`, read/write helpers next to `intent_store`'s.
No behaviour change; nothing reads it yet.

**Test:** round-trip, append-only semantics, `last_seen` advancing on a repeat.

### Stage 2 — allocation and the bootstrap

Mint an id when a node first appears, and seed `_node_wiring` from existing
history on first open: one node per distinct `(wiring_id, scope)` in
`list_pipeline_variants`, id allocated, association written.

**This stage IS the migration.** Seeding reproduces today's derived behaviour
exactly, persisted once instead of recomputed per build — so at the end of
Stage 2 the canvas is unchanged and every existing id-keyed row still
resolves, because the seeded id is the old `fn__{fn}__{wiring_id}` string.
Allocation only applies to nodes created *after* it.

Marker-guarded so it cannot run twice, the same way `intent_store`'s one-time
imports are (`_imported` / `_mark_imported`).

**Test:** a database with history opens to the same node ids as before;
re-opening does not re-seed or churn ids.

### Stage 3 — attribution (D-2026-09-22-2)

**Recorded at dispatch.** The run request already carries `node_id` (and
`matlab_command_service.scope_variants_to_node` already depends on it): write
the `(node_id, wiring_id, run_id)` row when the run is sent, for both the
Python and MATLAB routes.

**Inferred for the rest**, on graph build, for any recorded wiring with no
association in that scope:

1. exactly one node in the scope **states** W → attribute to it;
2. else exactly one node has **previously run as** W → attribute to it;
3. else allocate a new node.

Rule 1 is what stops the duplicate forming, and it does so from the *stated*
wiring — the thing the user controls.

**Test:** the grSides shape end to end — draw an edge onto an unbound param,
run, assert **one** node afterwards and that it carries the run. Plus a
script-run (no node id) attributing by rule 1, and a genuinely new wiring
allocating a node.

### Stage 4 — ambiguity + popup

Scope first (already a node property, D-2026-09-20-9). Within a scope: prefer
a node that has already run as W, else the oldest. Either way raise a **GUI
popup** naming both nodes and saying runs will attribute to one until they
differ. Not auto-merged.

**Test:** two nodes, identical stated wiring, one scope → deterministic
attribution, stable across rebuilds, and the warning emitted once rather than
per build.

### Stage 5 — cut the derivation over

Replace `wiring_id`-parsed-from-the-id with a lookup at each site. Known
callers: `derive_target_for_node` (parses `(fn, wid)` out of the node id to
match variants), `manual_edge_handle_index`, `hidden_wirings` /
`wiring_disconnected_fkeys`, `group_call_sites_by_wiring`,
`superseded_manual_input_overrides`, `input_params_with_manual_edges`.

`parse_fn_node_id` returning `(fn, wid)` is the seam: after this stage it
returns `(fn, node_id)` and the wiring comes from `_node_wiring` / the node's
statements.

**Test:** `test_identity_parity.py` gains a fifth identity — a node's stated
wiring, computed forward, equals the wiring its run recorded.

### Stage 6 — retire the repairs

Once identity no longer moves, these have nothing to do and should go rather
than linger as dead paths:

- `superseded_manual_input_overrides` and its edge rewriting;
- `_migrate_node_statements` (3b-i, added 2026-09-22 as the stopgap);
- the graduation id-swap — a manual node's allocated id simply *stays* its id,
  which removes the other half of the two-schemes problem.

**Test:** the existing suites for each must still pass with the path removed,
or be rewritten to assert the new behaviour. The three supersession-migration
cases in `test_api.py::TestManualInputEdgesOnHistoryNodes` should become
"nothing to carry, because the id did not move".

---

## Risks

**The seam is wide.** Stage 5 touches the run path we have just been fixing.
Do it after the current batch is committed and visually checked, not before.

**`parse_fn_node_id` is load-bearing in ~40 places** (`ids.py` says so).
`BareNodeId` / `PlacedNodeId` already made the id a type; this is the second
change to what the id *means*, and the type is what makes it findable.

**Bootstrap correctness is everything.** If Stage 2 seeds wrong ids, every
saved position, config and hide points at nothing. It must be reversible —
keep the marker row and the derived id so a bad seed can be dropped and redone.

**Do not let it grow.** Variant rows, the provenance view (Problem 9) and the
`Decision`/NOT REFLECTED chip all get easier once `_node_wiring` exists. They
are follow-ons, not part of this.

## Not in scope

- Content ids (`record_id`, `invocation_id`, `call_id`, `function_hash`,
  `wiring_id` itself) — all stay computed. `node-identity.md` §8.
- Display intent (positions, groupings) — keyed by node id, and benefits from
  the id being stable, but needs no model change.
- Variable / Parameter / PathInput node ids — `var__{Type}`, `param__{name}`
  are declared names, already stable.

## Sequencing against the open plan

`.claude/plan-run-state-and-duplicate-nodes.md` still has Problems 5, 6, 7, 8
and 9 outstanding. None depend on this. Problem 3b-ii (the visible duplicate)
**is** this — do not build the absorb patch.
