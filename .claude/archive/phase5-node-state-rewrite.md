# Phase 5 — Node-state (§9c) rewrite plan

## The problem that forced this

After dropping the `branch_params` column, node completeness (`check_node_state`)
broke for PathInput/MATLAB functions: `missing == 16` (all expected combos
reported missing). Root cause:

`check_node_state` matches *expected* vs *actual* combos on
`(schema_id, branch_params)`. `branch_params` now comes from
`derived_branch_params` (the graph). But **the graph stores every constant
identically** — a sweep constant (`low_hz=20`, a real branch) and a per-combo
resolved value (a PathInput filepath, 1:1 with the schema location) are
structurally indistinguishable. The old `branch_params` column could tell them
apart only because the for_each save path wrote ONLY explicit scalar `inputs`
into it (filepaths excluded).

### Why it can't be reconstructed from the graph

Consider one subject with a sweep `low_hz ∈ {20, 30}` vs one subject with a
per-combo filepath. In both cases each constant record is consumed by exactly one
invocation at one schema location. There is **no structural signal** separating
"sweep" from "per-combo". The distinction lives only in the *recipe* (for_each
declares `low_hz` as a scalar constant; `PathInput` as a path). So post-hoc
reconstruction is impossible.

### Why pure on-demand (§9c literal) doesn't cover PathInput

§11 said "delete `_for_each_expected`; compute completeness on demand (§9c)".
§9c's algorithm is `for combo in plan_combos(recipe, db)` — it needs the recipe
AND, for PathInput, the filesystem state *at for_each time* (which combos existed
on disk). A later GUI `check_node_state(fn, outputs)` call has neither. The
expected set for PathInput-only functions genuinely cannot be derived from
current DB state. This is precisely why `_for_each_expected` exists.

## RESOLUTION (chosen): tests use the real for_each path; no node-state special-case

Decisive finding: **real MATLAB for_each saves through `_save_results`**
(`for_each_save` → `_for_each_save_resolved` → `_save_results`), the same batch
path Python uses, which **already excludes PathInput** from graph constants
(`ForEachConfig._get_direct_constants` drops PathInput/PathOutput/ColName, and a
PathInput filepath is not a DB-variable input either → no edge). So for every real
flow, a PathInput function's invocation has no per-combo constant edge and
`derived_branch_params` is correctly `{}`.

The per-combo filepath only leaked via `record_run_from_lineage` (the
`db.save(lineage=...)` per-record path), and the only callers passing a
PathInput-resolved value through it were two **test simulations**
(`test_state_pathinput.py`, `test_state_matlab_pathinput.py`) that hand-built
per-combo lineage saves — NOT how real MATLAB for_each saves.

Chosen fix (user decision): update those tests to drive the real for_each /
for_each_save path. Then `derived_branch_params` is naturally `{}` for PathInput
records everywhere, no node-state special-casing, and the read layer stays clean
for all consumers. The read-layer heuristic below was reverted.

## (Reverted) surgical read-layer heuristic

Further analysis showed the regression is **confined to PathInput/MATLAB-only
functions**. Pure DB-input for_each functions never record per-combo constants in
the graph (the for_each save path excludes PathInput/PathOutput/ColName from
`__constants`), so their `derived_branch_params` is already correct sweep-only.
Per-combo pollution arises *only* via the lineage path (`record_run_from_lineage`,
used by MATLAB/PathInput), where scilineage classified the resolved filepath as a
constant. Those functions have **no variable inputs** and route through
`_for_each_expected` (bp `{}`) on the expected side.

So the fix is one rule in `_get_output_combos`: a record whose producing
invocation has **no variable inputs** gets `branch_params = {}` (its "constants"
are per-combo addressing, not sweeps), matching the expected side. Records with
≥1 variable input keep their graph-derived sweep branch_params. `_for_each_expected`
and `call_id` are retained as-is (they remain load-bearing for PathInput
enumeration and call-site scoping — not redundant with the graph).

The larger invocation-id table transform below was considered and rejected as
over-engineering for the confined regression; kept here for context.

## (Rejected) Decision: transform `_for_each_expected`, don't delete it

Keep a persisted expected set, but realign it to the graph: store the **expected
`invocation_id` per planned combo** instead of `(schema_id, branch_params)`.

- `invocation_id` folds in fn_hash + all input bindings + flags uniformly, so the
  sweep-vs-per-combo ambiguity disappears (it's all just "the call we expect").
- for_each computes every expected `invocation_id` at planning time (it resolves
  each combo's `__rid_*` bindings + constants before skip-filtering), so it can
  persist them — including combos that fail or are skipped.
- `call_id` is **kept** (still needed to scope expected rows per call site).

This is the faithful realization of §9c's intent (membership of expected
invocation_ids in `_invocation`), persisted because PathInput requires it. It is
a deliberate, documented deviation from §11's literal "delete `_for_each_expected`
and `call_id`" — those turned out to be load-bearing, not redundant with the graph.

## New `_for_each_expected`

```sql
CREATE TABLE _for_each_expected (
    function_name VARCHAR NOT NULL,
    call_id       VARCHAR NOT NULL,
    schema_id     INTEGER NOT NULL,
    invocation_id VARCHAR NOT NULL,   -- the call this combo should produce
    PRIMARY KEY (function_name, call_id, schema_id, invocation_id)
)
```

`_persist_expected_combos(db, fn_name, call_id, full_combos)` computes, per combo:
fn_hash, resolved bindings (`__rid_*` + constants→constant record_ids +
ColumnSelection selectors), as_table, distribute → `invocation_id`, and writes
one row. Replaces rows for `(fn_name, call_id)` only.

## New `check_node_state`

```
expected = SELECT schema_id, invocation_id FROM _for_each_expected
           WHERE function_name = ? [AND call_id = ?]
for (schema_id, inv_id) in expected:
    if inv_id in _invocation:
        # present → up_to_date or stale (function-source edit shifts the
        # EXPECTED id, so a present id is by-definition current → up_to_date;
        # staleness of inputs is already folded into inv_id, so "present" = fresh)
        state = up_to_date
    else:
        state = missing
green  = all up_to_date
grey   = some up_to_date AND some missing
red    = none present (never run) [stale folds into missing under this model]
```

Note: because `invocation_id` is content-addressed over fn_hash + input
record_ids, "stale" collapses into "missing" (§9c "stale collapses into not-run"):
a changed input or edited function shifts the expected id, the old one is absent,
so the node shows needs-run. The `combos`/`counts` dict keeps `stale` for shape
compatibility but it will generally be 0.

## Files touched

- `scidb/database.py` — `_ensure_for_each_expected_table` schema.
- `scidb/foreach.py` — `_persist_expected_combos` computes invocation_ids.
- `scidb/state.py` — `check_node_state` + helpers rewritten over invocation
  membership; delete `_get_output_combos`, `_get_expected_combos`,
  `_get_expected_combos_from_inputs`, `_records_schema_and_bp`,
  `check_combo_state`'s branch_params combo role (kept for direct callers).
- Tests: existing node-state tests should pass unchanged (same green/grey/red
  contract, same counts for the success/missing cases).
