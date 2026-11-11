# Deep propagation of staleness in `scihist.state`

## Motivation

`docs/guide/node-states.md` defines red/grey/green for a single node based on its own combo counts. But a user editing real pipelines expects:

> _"When I re-save an upstream record, every downstream node that was produced from it should become red."_

Before this change, `_check_via_lineage` only walked one hop upstream (immediate `_lineage.inputs`). That meant `step1 → step2 → step3` only flipped `step1` to red when `WfRaw` was re-saved; `step2`'s and `step3`'s own state stayed green even though they were transitively derived from stale data.

The user's directive:

> _"Once upstream data/functions change, all downstream data should be marked as stale/out of date... I want state to be tracked as closely as possible by scihist. The GUI just visualizes it."_

## Decision — two different propagation strategies

| Kind of change            | Who handles it           | Why                                                                                                                                                          |
| ------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Ancestor data re-saved** | `scihist` — deep walk    | We have the ancestor's `record_id` in the `_lineage` graph. Walking the full graph and comparing against `get_latest_record_id_for_variant` is unambiguous. |
| **Ancestor fn code edited, not yet re-run** | GUI layer (shallow for scihist) | `check_node_state(fn, outputs)` only receives `fn` itself. It can detect a hash mismatch for `fn`, but has no handle on the current in-memory version of ancestor functions. |

The second case is intentionally out of scope for scihist. As soon as the edited ancestor is re-run, a new `record_id` appears and the deep data-change walk handles cascade automatically.

## Implementation — `_has_superseded_ancestor`

Lives in `scihist-lib/src/scihist/state.py`. BFS across `_lineage` from the output record_id, walking both `source_type == "variable"` and `source_type == "rid_tracking"` entries.

```python
def _has_superseded_ancestor(db, record_id, combo_str, visited=None, max_depth=50):
    ...
    queue = [(record_id, 0)]
    while queue:
        current_rid, depth = queue.pop(0)
        if current_rid in visited or depth > max_depth:
            continue
        visited.add(current_rid)
        lineage_inputs = db.get_lineage_inputs(current_rid)
        for inp in lineage_inputs:
            if inp["source_type"] not in ("variable", "rid_tracking"):
                continue
            used_rid = inp.get("record_id")
            if not used_rid:
                continue
            current_latest = db.get_latest_record_id_for_variant(used_rid)
            if current_latest != used_rid:
                return True   # found a superseded ancestor
            queue.append((used_rid, depth + 1))
    return False
```

Key points:

- `visited` guards against cycles.
- `max_depth=50` bounds cost on pathological graphs.
- If `get_lineage_inputs` fails, we conservatively return True (stale) — lineage corruption should surface as a user-visible red rather than silent false-green.

`_check_via_lineage` now calls `_has_superseded_ancestor` instead of the shallow one-hop check.

## PathInput + Variable fix in `_get_lineage_variants`

Parallel bug surfaced while writing `TestMixedInputTypes`: a function like

```python
@lineage_fcn
def mixed_inputs(filepath, baseline, scale): ...

for_each(mixed_inputs,
         inputs={"filepath": PathInput(...), "baseline": WfBaseline, "scale": 2.0},
         ...)
```

resolves `filepath` differently per combo ("sub01/trial01.csv", "sub01/trial02.csv", ...). scilineage classifies the resolved string as a CONSTANT. Therefore each row in `_lineage` has a distinct `constants` JSON.

The old `_get_lineage_variants` deduped by `(inputs, constants)` → produced one variant _per combo_ (4 variants for 2×2). `_get_expected_combos` then merged these pseudo-constants into the expected `branch_params` template, producing 16 phantom expected combos vs 4 actual output records → falsely grey.

Fix:

1. `_get_lineage_variants` no longer uses `_lineage.constants` as a variant discriminator. It returns only `input_types`.
2. `_get_expected_combos` no longer merges `own_constants` into `expected_bp` for lineage variants. It uses `input_bp` directly.

Safe because `scidb.save(variable, metadata, ...)` (called by scihist's `_save_with_lineage`) writes `branch_params={}` for scihist records — user constants like `scale=2.0` go into `version_keys`, not `branch_params`. So `expected_bp = {**input_bp, **{}} == input_bp` matches actual.

The scidb-variants path (functions saved via `scidb.for_each`) is unchanged — it still namespaces and merges `own_constants` because `scidb.for_each` does put them into `branch_params`.

## Tests added — `scihist-lib/tests/test_state_workflows.py`

Four test classes, 12 tests:

1. **TestMultiStepPropagation** — `WfRaw → step1 → step2 → step3` chain.
   - `test_stale_propagates_through_three_hops` — re-save root → all three steps red.
   - `test_midchain_fn_change_affects_only_checked_node` — codifies the "shallow fn-code" scope.
   - `test_new_upstream_combo_only_greys_direct_consumer` — new input combo greys the direct consumer only.

2. **TestForkJoinPropagation** — `WfRaw → {fork_left, fork_right} → join_sides`.
   - `test_fork_one_upstream_taints_both_branches` — re-save root → both branches red.
   - `test_join_cascades_from_root` — deep 2-hop cascade root → branches → join.
   - `test_join_red_when_direct_input_resaved` — direct input re-save → join red.

3. **TestMixedInputTypes** — `mixed_inputs(filepath=PathInput, baseline=Variable, scale=2.0)`.
   - `test_green_after_full_run_with_all_three_input_types`
   - `test_grey_when_pathinput_file_missing`
   - `test_red_when_variable_input_resaved`

4. **TestMultiOutputSingleFunction** — single `@lineage_fcn(unpack_output=True)` returning a 3-tuple.
   - `test_green_when_all_three_outputs_present`
   - `test_missing_when_one_output_class_lacks_a_combo`
   - `test_all_outputs_go_stale_together_on_input_resave`

Run:

```bash
cd /workspace/scihist-lib && pytest tests/test_state_workflows.py -v
```

## What this does _not_ cover

- **Ancestor fn-code changes not yet re-run.** By design — GUI layer concern (see `docs/guide/node-states.md`, "Function-code-change propagation").
- **Multi-database / cross-project lineage.** `get_latest_record_id_for_variant` queries the same DB handle.
- **Deleted ancestors.** If an ancestor record is hard-deleted (not just excluded), `get_lineage_inputs` may still point at it and `get_latest_record_id_for_variant` returns None. Treated as superseded → stale, which is the safe default.
