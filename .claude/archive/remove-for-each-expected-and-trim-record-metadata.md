# Plan: binary node-state + remove `_for_each_expected` + trim `_record_metadata`

Branch: `dev-hist`. Continues the lineage-simplification / bipartite-provenance
migration. Driving principle: **the normalized graph (`_record`, `_invocation`,
edges) is the single source of truth. Eliminate persisted/derived state that must
be kept in sync by equality — that is the drift hazard.**

## Decisions (user-approved)

1. **Node state is BINARY: `green` | `red`.** Grey/partial removed. A node is
   green iff it has expected work AND every expected invocation is present; red
   otherwise (never run, partial, input re-saved-not-rerun, edited function). This
   makes decisions (3a)/(3b) below automatic — they just fall out of "any missing
   → red".
2. **Remove `_for_each_expected`** entirely — the only structure storing a
   *predicted* invocation_id that had to equal a *separately realized* one (drift
   hazard; caused the original `*pathinput` failures).
3a. **Edited function → red** (new function_hash ⇒ all expected ids shift ⇒ absent
   ⇒ red). No "realized done-work" folding needed.
3b. **Per-output-class exclusion is NOT tracked** — completeness is pure
   invocation membership. Excluding one output of a multi-output invocation does
   not change node state.
4. **Zero-input loaders (PathInput-only) are green-when-run / red-when-never-run**,
   never partial — no live source for the combos they *should* produce.
5. **Trim `_record_metadata`** to an audit trail (later phase).
6. **Fix `_current_records_by_schema` latest-record selection** (correctness:
   accurate counts + avoids a false-red in the re-save-before-first-run edge).

## What "prediction" means (reference)

Determining a call's invocation_id from `function_hash` + input bindings WITHOUT
reading the realized `_invocation` row. Needed only for the *expected* side of
completeness. Persisted prediction (`_for_each_expected`) drifts → removed. Live
prediction (recompute from current inputs each query) is safe because it shares
the one hash fn and `_compute_fn_hash == compute_function_hash(fn,16)` == the
graph's stored hash. Inputless loaders can't be predicted at all → expected =
their realized output locations.

---

## Phase 1 — Remove `_for_each_expected` + binary node-state — **IMPLEMENTED & VERIFIED (user ran full scidb + scihist suites: all green, 2026-06-19)**

### Removed the snapshot machinery
- `database.py` — deleted `_ensure_for_each_expected_table` + its call. (Orphan
  table in pre-existing DBs is harmless; no DROP/migration.)
- `foreach.py` — deleted `_persist_expected_combos` + the Step-13 call.
- `provenance_save.py` — deleted `expected_invocation_id` + its `__all__` entry.

### Reader rewrite (`provenance_query.py`)
- `expected_invocations_for_function`: dropped the `_for_each_expected` branch.
  Now = `realized_inputless_invocations` (new) ∪ live prediction over current
  inputs ∪ declared-inputs fallback.
- **New `realized_inputless_invocations(duck, fn_name)`** — pure structural read:
  invocations with no variable-input edges → their realized output schema
  locations. Makes a run loader green (expected == present by construction) and a
  never-run loader red. Without this, dropping the snapshot made loaders
  *always red*.

### Binary state (`state.py`)
- `NodeState = Literal["green", "red"]`.
- `check_node_state` aggregation: `green` iff `combo_results and missing == 0`,
  else `red`. (`counts` dict retained for diagnostics/GUI.)
- Docstrings + `check_multiple_nodes_state` doc updated.

### Latest-record fix (`provenance_query.py`)
- **New `_producing_variant_key(duck, rid)`** — constants of a record's producing
  invocation, or `None` for raw. Re-saves/re-runs under the same config share a
  key; different constant configs don't.
- `_current_records_by_schema` now returns the latest record per
  `(schema_id, producing-variant)` instead of all non-excluded records. Fixes
  inflated `up_to_date` counts on re-save (incl. the original `15 == 14`) and a
  false-red in the re-save-before-first-run edge. Only caller is
  `_predict_config_invocations`.

### Tests updated to the binary contract
- Deleted: `scidb/tests/test_call_id.py` (queried the removed table directly),
  `scihist/tests/test_state_matlab_pathinput.py` (asserted grey-for-partial-loader).
- `scihist/tests/test_state_pathinput.py` — rewritten: loader green-when-run
  (incl. partial), red-when-never-run.
- `scihist/tests/test_state.py`, `test_state_call_id.py`, `test_state_realworld.py`,
  `test_state_workflows.py` — every node-state `grey` assertion flipped:
  with-input partial/resave/edited-fn → **red**; PathInput-loader partial →
  **green**; multi-output single-exclusion → **green** (membership-only). Count
  assertions kept (now correct via the latest-record fix). `check_combo_state`
  tests (ComboState up_to_date/stale/missing) untouched — that per-combo API is
  unchanged.

### Reasoning notes (could not run pytest in this env — user runs tests)
- Loaders: expected = realized output locations = present ⇒ green; empty ⇒ red.
- With-input fully-run-unchanged: live reproduces realized ids (as_table/distribute/
  constants come from the realized `_invocation`; record_ids from current data;
  fn_hash identical) ⇒ green.
- With-input partial / new-combo / resave / edited-fn ⇒ ≥1 missing ⇒ red.
- Latest-record fix ⇒ superseded inputs not enumerated ⇒ counts exact.

---

## Phase 1.5 — GUI alignment (scistack-gui) — **NOT STARTED (separate layer, needs user)**

`scistack-gui` has its **own** grey-based run-state model
(`domain/run_state.py`, `api/pipeline.py`, `frontend/.../FunctionNode.tsx` /
`VariableNode.tsx`, `static/assets/*.js`) plus many tests asserting grey
(`tests/test_api.py`, `test_run_state.py`, `test_graph_builder.py`,
`test_pipeline_call_sites.py`). It consumes `check_node_state`, whose own-state
is now binary, and ADDS grey via DAG propagation + pending-constant downgrades.

Left untouched on purpose (GUI is its own layer per CLAUDE.md NOTE 3, and the
user directed the *node-state model*, i.e. scidb/scihist). **Impact to confirm
with user:** with scidb returning red (not grey) for partial runs, the GUI's
partial-run nodes will now show red unless the GUI keeps its own grey. Decide
whether to (a) carry grey purely in the GUI layer, or (b) make the GUI binary
too. Until then, expect GUI tests asserting grey-for-partial to fail.

---

## Phase 2 — (optional) structural-traversal expected side

Replace live prediction's hash-recompute with a pure `_invocation_input`
traversal, removing the last place two code paths must agree on bindings. Lower
priority now that the live path is the only predictor and shares the hash fn.

## Phase 3 — Trim `_record_metadata` to an audit trail — **NOT STARTED**

End state: drop `version_keys`, `variable_name`, `schema_id`, `content_hash`,
`lineage_hash`, `schema_version`, `excluded` (duplicated by `_record`/`_schema`/
`_invocation.pipeline_hash`); keep `(record_id, timestamp)` (+ maybe `user_id`).
Blocked on migrating `version_keys` readers onto the graph/`_schema` first —
`find_record_id`'s exact-match lookup (highest risk), `__where` load filtering,
`__upstream`-stripping variant grouping, the legacy `__fn_hash` staleness
fallback, `call_id` derivation, and raw-record variant identity. Separate plan.

## Risks / open items
- GUI grey behavior change (Phase 1.5) — needs a user call.
- `_current_records_by_schema` now does 2 extra queries/record — fine at current
  scale; candidate for a single-query optimization later.
- `check_combo_state` (per-combo deep API) and ComboState are intentionally
  unchanged.
