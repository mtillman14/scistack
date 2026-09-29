# Plan: a call site with more than one output must hash to one agreed wiring

Status: BUILT 2026-09-29 (option B; user approved). Tests written, NOT run
(the user runs them). Q2 taken as "hide the phantom by hand"; Q3's multi-output
tests were written alongside, not first.

## 0. What was built

- scidb `provenance.split_call_site_outputs` + `SPLIT_MAX_OUTPUTS`: the owner.
  `database.call_site_wiring_ids` (CLI) now calls it with no claims (= union).
- GUI `node_wiring.claimed_wirings(db)`: the claims, one query.
- GUI `graph_builder.split_call_sites_by_claims(agg, claims)` + `SPLIT_SEP` /
  `call_id_of`: applied ONCE in `api/pipeline.build_aggregate` (shared by the
  build, `ensure_node_identities`, `disconnected_report_entries`). A split
  call site becomes sub-sites `(fn, "{call_id}#{wiring}")`, one wiring each,
  so every per-FnKey loop downstream is correct unchanged. Unsplit call sites
  keep their FnKey exactly.
- `_compute_run_states`: asks scidb once per REAL call id (union of the
  sub-sites' outputs — what it asked before) and gives each sub-site that
  state. Variant rows carry the real call id.
- Run path: `execution_service.history_variant_wirings` (owner-backed) used by
  `derive_target_for_node`, `disconnected_reason`, `_scope_function_node_ids`,
  and — via the target key `variant_resolver.HISTORY_WIRING_KEY` —
  `reconcile_manual_inputs` on the name-scoped path.
- `record_dispatch_wirings`: ONE claim per input shape covering every output
  the Run writes (was one per target/output); log names each claim's outputs.
- Tests: scidb/tests/test_split_call_site_outputs.py,
  scistack-gui/tests/test_multi_output_wiring.py (rewire end-to-end,
  multi-output, pure split, AST guard on the run path).

Known leftover: `reconcile_manual_inputs` still falls back to a per-output
hash for targets WITHOUT a history wiring (never-run manual targets, one
output each) — harmless there, excluded from the guard (it lives in
variant_resolver).

## 1. The failure (scidb.log 2026-09-29)

1. On 2026-09-28, `pandas.read_csv(DemographicsPath)` ran into `Demographics`.
   Its node claimed wiring `15506ac7814b3a09`.
2. The user rewired the node's output to `DemographicsTable` and ran it (run
   n1irqety). The node, `cc6af15d…`, claimed wiring `7ade34e9ef0a9bdf`.
3. The next canvas build computed `1540882c6bfa05de` for that call site. No
   node had claimed it, so the build minted `fn__pandas.read_csv__4b9cac03…`
   and warned "The new node is a phantom if so".
4. Run on that node: "wiring 1540882c… matches none of the 2 candidate
   variant(s)", then no targets.

Verified with a node.js copy of `compute_wiring_id`:

| outputs hashed, `path_inputs={filepath_or_buffer: DemographicsPath}` | wiring |
|---|---|
| {Demographics} | 15506ac7814b3a09 |
| {DemographicsTable} | 7ade34e9ef0a9bdf |
| {Demographics, DemographicsTable} | **1540882c6bfa05de** |

## 2. Root cause: two owners of "the outputs in a wiring" (NOTE 4)

A call site is identified by `call_id`: the function, its inputs, its
constants and its run options. The output type is not part of it. So every
run that reads the same inputs, into any variable, is the same call site.

| Owner | Outputs it hashes |
|---|---|
| `scidb.database.aggregate_pipeline_variants` → `functions[fkey]["outputs"]`, the union of every output type ever recorded. Used by `graph_builder.group_call_sites_by_wiring` (canvas node grouping), `stated_wiring_claims`, `hidden_wirings` and similar, and by `scidb.database.call_site_wiring_ids` (the CLI step grouping). | ALL recorded outputs |
| `execution_service.derive_target_for_node` (:811), `record_dispatch_wirings` (:1001), `disconnected_reason` (:1078) and `variant_resolver` (:583) | one `{output_type}` per variant or target |

The two agree only when a call site has exactly one output type. They
diverge in two cases:

- **An output rewire, as here.** Different runs saved the same inputs into
  different variables.
- **A genuinely multi-output function.** This is suspected, not reproduced.
  - The Python GUI Run thread runs one `for_each` per target, with
    `outputs=[OutputCls]` (`api/run.py:486/604`), and the dispatch claims one
    wiring per output. The graph hashes the union, so the node graduates to a
    phantom after its first run.
  - A Python script run with `outputs=[A, B]`, then run from the GUI, would
    likewise never match.
  - MATLAB native multi-output collapses to one call (`execution_service`
    :2104), but its dispatch still claims one wiring per target.

Why it only bit now: until a node has run history, it "runs from its own
edges" whenever no wiring matches (`derive_target_for_node` :829-854, logged
at INFO). Once a node has run history, it is stuck.

## 3. Constraints

- The `_node_wiring` claims are GUI-side and never in provenance
  (node-identity.md §7a). scidb may own the RECIPE, but the claims come in as
  an argument.
- A node draws exactly one current wiring (`node_wiring.current_wiring`, the
  latest `last_seen`). A node that runs several outputs together must
  therefore claim ONE wiring that covers all of them.
- Script runs have no claims. They must keep today's behaviour: one node
  drawing every output recorded for the call site, with the INFO line
  "records 2 output types … its node draws every one".
- No new storage. No migration code (beta rule).

## 4. Options considered

**A. The run path hashes the union, like the graph.** Change the four
run-path sites to hash `aggregate[fkey]["outputs"]`.
- Rejected. A rewired node would keep drawing, and re-running, the output the
  user rewired away from (Demographics), which is exactly what node-identity.md
  §10 says a rewire must not do.
- The union also changes whenever a new output is written, so the dispatch
  (made before the run) could never predict what the next build computes.
  That misprediction is the mechanism that created the phantom.

**B (recommended). The outputs of a wiring are what one Run claimed
together.**
- Dispatch claims ONE wiring per call site: `hash(inputs, pi, S)`, where `S`
  is the set of output types this Run writes for that call site. For a
  one-output node that is exactly today's per-output hash, so existing claims
  stay valid.
- One new pure owner in scidb, `provenance.split_call_site_outputs(fn,
  input_params, path_inputs, outputs, claimed_wirings) -> {wiring: frozenset
  (outputs)}`:
  - For each subset `S` of the call site's recorded outputs (2^|O|; |O| is 1
    to 3 in practice, capped with a WARN above 6), check whether
    `compute_wiring_id(fn, inputs, S, pi)` is in `claimed_wirings`. Every
    claimed subset becomes its own wiring.
  - Outputs that no claimed subset covers are grouped as one remainder wiring.
    This is today's union, restricted to those outputs, which keeps
    script-run behaviour unchanged.
  - A claimed subset contained in a larger claimed subset: both are kept, and
    identity resolution decides which node each wiring lands on.
- Every consumer calls it:
  - **graph_builder** (`group_call_sites_by_wiring` and the other
    `wiring_id(fn, params, fn_outputs[fkey], …)` sites at :2495, :2519,
    :2567, :2595): iterate `(fkey, wiring, outputs_subset)` instead of
    `(fkey, union)`, and restrict each group's `fn_outputs` to its subset.
  - **execution_service.derive_target_for_node**: find the node's
    `current_wiring` among the split, and keep the variants whose
    `output_type` is in that wiring's subset.
  - **record_dispatch_wirings**: group targets by `call_id` and hash each
    group's union of `output_type`s.
  - **disconnected_reason** and **variant_resolver:583**: same split.
  - **scidb.database.call_site_wiring_ids**: calls it with
    `claimed_wirings=()`, so everything falls into the remainder = the union.
    The CLI output is unchanged.

**C. Put the output types in `call_id`.** Rejected: it changes record,
invocation and call ids everywhere (an identity break) to fix grouping.

## 5. What B does to this database

- `cc6af15d…` has claimed 15506ac (inherited through the rekey from
  `rlw90w`) and 7ade34. Split: {Demographics} → 15506ac, {DemographicsTable}
  → 7ade34. Both belong to `cc6af15d…`. Its current wiring is 7ade34, so it
  draws DemographicsTable, and Demographics is a history row. That is the
  designed rewire behaviour.
- The phantom `4b9cac03…` claimed 1540882c at build time (`run_id=None`).
  1540882c is also a valid claimed subset (the full set), so under B it would
  survive as a third wiring. Not handled by code (no migration): the user
  hides the phantom node, and `node_wiring.forget_node` drops its claim.
  **Open question Q2.**

## 6. Logging (NOTE 2)

- `split_call_site_outputs`: INFO whenever a call site splits into more than
  one wiring, naming the call site, each wiring → its outputs, and which came
  from a claim and which is the remainder. DEBUG listing every subset tried.
- `derive_target_for_node`: in the "matches none" WARN, add the split it
  compared against (wiring → outputs), not only the per-variant hashes.
- `record_dispatch_wirings`: log the output set of each claimed wiring.

## 7. Tests

Run one package per pytest invocation.

- scidb (pure): `split_call_site_outputs`
  - no claims → one wiring, the union;
  - a rewire: two single-output claims → two wirings;
  - a multi-output claim → one wiring covering both outputs;
  - a claim plus an unclaimed output → the claimed wiring plus a remainder;
  - |O| > 6 → WARN and the union.
- scidb: `call_site_wiring_ids` is unchanged. Guard: the CLI grouping is the
  same as before.
- scistack-gui, THIS regression, end to end: run a node into A; rewire its
  output to B; run it; build. Expect no minted node, the node's current wiring
  = hash(B), its Run derives one target with `output_type == B`, and A shows as
  a history row.
- scistack-gui: a Python function run from the node with two output edges
  → one dispatch claim covering {A, B}; after the build the same node draws
  both; Run → two targets. This is the suspected multi-output case: write the
  test first and confirm it fails today.
- scistack-gui: a script run with two output types and no claims → one node
  drawing both (unchanged).
- AST guard: no `wiring_id(`/`compute_wiring_id(` call whose third argument is
  `{…["output_type"]}` or `fn_outputs[...]` outside `split_call_site_outputs`,
  so the two-owner split cannot come back.

## 8. Questions for the user

- **Q1.** Is B the right model: a node's outputs are the ones it was run with
  together, and a rewire leaves the old output as history?
- **Q2.** The existing phantom `4b9cac03…`: hide it by hand (recommended, no
  migration), or have the build drop a build-time claim (`run_id=None`) on a
  wiring whose outputs are all covered by dispatch claims?
- **Q3.** Should the multi-output tests be written first (§7), to confirm the
  suspected breakage before building?

## 9. Order relative to other uncommitted work

The working tree also holds the uncommitted hidden-node identity follow-up
(`node_identity.py`, `execution_service.py`, `api/pipeline.py`). This plan
edits `execution_service.py` too. Commit this morning's fixes (schema-level
narrowing, warnings, same-invocation latest) first, separately from both.
