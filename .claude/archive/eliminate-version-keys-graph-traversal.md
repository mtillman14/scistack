# Plan: eliminate `version_keys` — strict graph traversal

Branch: `dev-hist`. Final step of the normalization thesis: remove the
`version_keys` JSON column from `_record_metadata` and derive everything it
carried from the bipartite graph + `_schema`. After this, `_record_metadata` is
just an audit trail (`record_id, timestamp, user_id`) — Phase 3 of the prior plan
becomes trivial.

## Core idea (user-proposed)

Everything in `version_keys` already has a graph home EXCEPT genuine non-schema
direct-save kwargs. Fold those into the graph too: a direct `.save(..., kw=v)`
with non-schema kwargs creates a **synthetic invocation**:

- `_invocation` row: `function_name = NULL`, `function_hash = NULL`,
  `as_table = []`, `distribute = FALSE`.
- `save_invocation_id = sha16("__save__" | output_record_id)` — 1:1 with the
  saved record (output_num 0), idempotent, no cross-variable/re-save collisions.
- `_invocation_input` edge per non-schema kwarg → a constant record
  (`compute_constant_record_id(value)` + `_constant` row), same machinery as
  for_each constants.
- `_invocation_output` edge → the saved record (output_num 0).

`derived_branch_params(record)` then surfaces these kwargs as branch params,
unifying direct-save variants and for_each sweep variants into ONE graph-derived
mechanism. Plain saves (only schema kwargs) get NO synthetic invocation — they're
distinguished by `schema_id` alone, as today.

## version_keys content → graph mapping

| content | replacement |
|---|---|
| schema keys | `schema_id` → `_schema` |
| `__fn`/`__fn_hash`/`__inputs`/`__constants`/`__distribute`/`__as_table` | `_invocation` + edges |
| `__upstream` | `_invocation_input` edges |
| `__where` | `_run.where_clause` via record→invocation→run; structurally also the input bindings |
| `__output_num` | `_invocation_output.output_num` |
| non-schema direct-save kwargs | synthetic NULL-fn invocation + constant edges |
| `__rid_*` | dead (skip_computed is graph-based; `get_record_version_keys` has no live callers) |

## Phases

### P0 — Synthetic save-invocation on the write path — **IMPLEMENTED (dual-write, pending user test run)**

Refinement vs original plan: P0 is **dual-write** (add the synthetic invocation
*alongside* the existing `version_keys`), NOT "stop writing version_keys" — readers
are still on `version_keys` until P1, so removing it now would break them. Writing
stops in P2. This keeps every slice green.

Done:
- `provenance.py`: `SAVE_FUNCTION_NAME = "__save__"` sentinel (function_name/hash
  are NOT NULL, so a sentinel string, not NULL) + `compute_save_invocation_id(rid)`
  = `sha16("__save__" | output_record_id)` (1:1 with the saved record → no
  cross-variable/re-save collisions). Both added to `__all__`.
- `provenance_save.record_direct_save(duck, rid, kwargs, created_at)` — inserts a
  constant `_record`/`_constant` per kwarg, the synthetic `_invocation`, a
  `_invocation_input` edge per kwarg, and one `_invocation_output` edge
  (output_num 0). Runs inside the caller's transaction; idempotent.
- `database.save()`: after `insert_record_entity`, when `lineage is None` and there
  are non-`__` non-schema kwargs, calls `record_direct_save`. (Lineage path writes
  its own constants via `record_run_from_lineage` → excluded to avoid double-write.)
- Guard: `provenance_query.pipeline_structure` excludes `function_name =
  SAVE_FUNCTION_NAME`. Other `_invocation` scans are already safe — they filter by a
  real `function_name` (`function_variant_configs`, `realized_inputless_invocations`)
  or by `pipeline_hash`/`invocation_id` (synthetic saves have neither).
- Verified by reasoning: `derived_branch_params` surfaces kwargs as
  `__save__.<kwarg>` (uniform `fn.param` namespacing; `_match_branch_param` suffix
  match on `.<kwarg>` still works), and the `_find_record` "latest" variant collapse
  stays equivalent (kwarg records now take the `inv is not None` branch but group by
  the same kwarg distinction). Test: `scidb/tests/test_direct_save_graph.py`.

### P1 — Reader migration (each independently testable)
- `find_record_id` / `_find_record` / `_match_row`: drop the `version_keys`
  branch; match on `schema_id` + `derived_branch_params` only.
- `_load_with_where` Strategy 1 (`__where` match): match record's producing
  invocation's `_run.where_clause` (or resolve where→input set) instead of the
  stored `__where` string. Strategy 2 (schema-id resolve) unchanged.
- `get_latest_record_id_for_variant`: latest per `(variable, schema_id,
  _producing_variant_key)` (already built) instead of `version_keys IS NOT
  DISTINCT FROM`.
- `list_pipeline_variants` / `get_aggregated_variants`: group via the graph
  (function + constants + input types) — the `function_variant_configs` shape —
  instead of `version_keys`-without-`__upstream`.
- `state.py` legacy `__fn_hash` fallback: remove (graph is authoritative now).
- `foreach_config.call_id` / `call_id_from_version_keys`: rederive from graph
  config signature, or retire if no longer needed.
- `filters.py`, `scilineage/core.py`, `scifor/pathinput.py`, GUI
  (`api/pipeline.py`, `domain/graph_builder.py`): audit + migrate.

### P2 — Drop the column
- Remove `version_keys` from the `_record_metadata` DDL and all
  INSERT/SELECT/`_fetchdf` sites. Slim `_record_metadata` to
  `(record_id, timestamp, variable_name?, schema_id?, user_id, ...)` — decide how
  much of the duplicated `_record` columns to also drop (separate, see prior plan
  Phase 3).
- Delete `get_record_version_keys` (no live callers).

## Risks / watchpoints
- **`find_record_id` is the highest-risk edit** (save/skip/load gate). Migrate it
  behind the existing tests; do it last in P1.
- **`__where` load semantics**: confirm graph match reproduces the variant
  selection in `docs/claude/where-provenance-and-merge.md` (the DeltaStepLength
  3-variant case). Add a regression test mirroring it.
- **derived_branch_params namespacing** for NULL-fn constants: pick a stable
  namespace (e.g. bare kwarg name, or `__save__.kw`) and ensure
  `_match_branch_param` suffix-matching still works.
- Cannot run pytest in this env — implement in small slices, user runs tests
  after each (see [[feedback_user_runs_tests]]).
- GUI layer migrated only as needed to not break; deeper GUI work stays in the
  GUI layer per CLAUDE.md NOTE 3.

## Sequencing
P0 first (write path + NULL-fn guards) so new saves stop populating version_keys;
then P1 readers one at a time (cheapest/safest first: `get_latest_record_id_for_variant`,
`list_pipeline_variants`, `state.py` fallback → then `_match_row`/`_find_record`
→ `_load_with_where` → `find_record_id` last); then P2 drop the column. Each slice
is independently green-able.
