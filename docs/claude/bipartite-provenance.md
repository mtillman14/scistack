# Bipartite Provenance — As-Built

> **⚠️ SUPERSEDED — see `database-model.md` for the canonical, up-to-date
> reference.** This doc is an earlier as-built note and is now partly stale: it
> still describes `_record_metadata` with `version_keys` / `lineage_hash`
> columns, which were since renamed/slimmed to `_record_save` (the blob columns
> were deleted). Read it only for the original design rationale.

> Status: **implemented** (branch `dev-hist`). This documents the model as it
> actually exists in code after the lineage-simplification migration, complementing
> the design in `lineage-simplification.md`. Read this to understand how provenance
> is stored and queried today.

## The graph

Provenance is a **bipartite graph**: *records* (entities) and *invocations*
(activities). Seven tables, all in DuckDB (see `scidb/provenance.py`):

- `_record` — every entity: variables AND constants. One immutable, content-
  addressed row each (`ON CONFLICT DO NOTHING`). Constants have `type =
  '__constant__'`, `schema_id NULL`.
- `_constant` — a constant's `value_repr` / `value_type` / `content_hash`.
- `_invocation` — one row per unique function call: `function_name`,
  `function_hash`, `as_table`, `distribute`.
- `_invocation_input` — edges call→input: `(invocation_id, param_name,
  input_record_id, selector)`. `selector` qualifies ColumnSelection (JSON
  `{"columns":[...]}`); NULL otherwise.
- `_invocation_output` — edges call→output: `(invocation_id, output_num,
  output_record_id)`.
- `_run` — append-only audit: one row per `for_each` execution (`run_id`,
  `timestamp`, `user_id`, `function_name`, `where_clause`).
- `_run_invocation` — many-to-many run↔invocation.

`_record_metadata` still exists (schema location via `schema_id`, `version_keys`,
`timestamp` audit trail, `excluded`, per-record `lineage_hash` for cache lookup).
Its `branch_params` column is **gone** — see below.

## Identity (`scidb/provenance.py`)

```
constant record_id = sha16("__constant__" | content:canonical_hash(value))
invocation_id      = sha16(fn_hash | as_table | distribute | sorted(bindings))
                     binding = (param_name, input_record_id, selector)
output record_id   = generate_record_id(...)  # UNCHANGED: from in-memory
                     # metadata at save time (class|schema_version|content|meta)
run_id             = uuid (fresh per execution; NOT content-addressed)
```

Note the output `record_id` is still produced by `generate_record_id` from the
in-memory metadata, **not** rederived from `invocation_id`. The §5 "output
record_id = hash(... | invocation_id | output_num)" change in the design doc was
not needed for the clean replacement: variant uniqueness is already achieved
because the metadata that feeds `generate_record_id` differs per variant, and the
graph independently provides structural traversal. Re-running an identical
pipeline still reproduces every id (idempotent), and `invocation_id` is the
content-addressed "computation id".

## Two save paths, one graph

1. **for_each batch** (`scidb/foreach.py::_save_results` → `provenance_save.record_run`):
   builds the graph from each saved row's `save_metadata` — `__fn`/`__fn_hash`,
   `__graph_var_bindings` (the COMPLETE per-row input edge set incl. Fixed rids +
   ColumnSelection selectors), `__constants`, `__as_table`/`__distribute`.
   Flatten/distribute emit many records per call → each gets the next free
   `output_num` (deterministic, idempotent).
2. **single-record lineage** (`db.save(lineage=...)` → `provenance_save.record_run_from_lineage`):
   the MATLAB bridge, `generates_file`, and direct lineage saves. Builds the graph
   from the scilineage `lineage` dict. Constants' `value_hash` is a
   `canonical_hash`, so constant record_ids match the for_each path.

Both pass `graph_function_hash = compute_function_hash(fn, 16)` so the graph's
stored `function_hash` matches the read side (NOT `LineageFcn.hash`, which is a
different sha256 scheme).

Every saved record (raw/manual included) gets a `_record` entity row via
`insert_record_entity(ies)`; raw records simply have no producing invocation and
terminate upstream walks.

## Read side (`scidb/provenance_query.py`)

Pure SQL traversal — no JSON parsing, no heuristics:

- `producing_invocation(rid)` → `(inv_id, fn_name, fn_hash)` or None.
- `invocation_inputs(inv_id)` → `(var_inputs, constants)`.
- `derived_branch_params(rid)` (§6) — walk upward, collect constant inputs
  namespaced `function.param`. **This replaces the deleted `branch_params`
  column everywhere** (load variant pinning, `BaseVariable.branch_params`,
  `list_versions`, exports, node-state).
- `upstream_provenance(rid)` (§8) — BFS, provably-correct edges (replaced the old
  `branch_params`-subset heuristic in `get_upstream_provenance`).
- `pipeline(rid)` — nodes+edges DAG. `execution_audit(rid)` (§9b).
- `provenance(rid)` / `pipeline_structure()` / `has_producing_invocation(rid)`.
- `stored_invocation_signature(rid)` — `{function_hash, var_inputs, const_hashes}`,
  used by `skip_computed` and node-state staleness.

## What `branch_params` became

The `branch_params` column on `_record_metadata` is **deleted** (not written, not
read). It was only ever the accumulated sweep constants, which is exactly
`derived_branch_params` (§6). Load (`_find_record`), the variant filter
(`_filter_records_by_branch_params`), the `"latest"` collapse, `list_versions`,
`BaseVariable.branch_params`, and exports all derive it from the graph now.

The `"latest"` variant collapse keys on `(variable_name, schema_id,
producing_function, derived_branch_params, output_num)`. `output_num` keeps the
distinct records of one flatten/distribute call separate; temporal re-saves share
it and collapse to the newest. Raw records (no invocation) fall back to their
`version_keys` for the variant key.

## skip_computed (`foreach.py::_build_skip_hook`)

The §11 "port" approach — reads the graph, not `_lineage`:
1. `find_record_id` gate: does an output for this variant (schema + constants +
   `__fn`) exist? No → compute silently (no `[recompute]` line).
2. Staleness: compare `stored_invocation_signature` to current inputs binding-by-
   binding (function hash, each input record_id + selector, constant content
   hashes). The self-referential guard (input rid == output rid → stable) keeps
   input==output pipelines from recomputing forever.

## Gotcha: per-combo constants vs sweep constants

The graph stores ALL constants identically — a sweep (`low_hz=20`) and a per-combo
resolved value (a PathInput filepath that scilineage classifies as a constant) are
indistinguishable structurally. The old `branch_params` column could tell them
apart (for_each only wrote explicit scalar `inputs`). This is why **node
completeness must NOT use branch_params combo-matching** — it uses the
invocation-membership test below.

## Node state (`state.py::check_node_state`) — binary, graph-derived

Node state is **binary: `green` | `red`** (grey/partial was removed). A node is
green iff it has expected work AND every expected invocation is present in
`_invocation`; red otherwise (never run, partial run, an input re-saved but not
re-run, or the function edited — any missing expected invocation reds the node).

There is **no persisted expected set**. `_for_each_expected` (a snapshot of
predicted invocation_ids written at for_each time) was deleted — it stored a
*predicted* id that had to equal a *separately realized* one, a drift hazard.
`expected_invocations_for_function` now derives the expected `{(invocation_id,
schema_id)}` set live (`provenance_query.py`):

1. `realized_inputless_invocations` — invocations with no variable-input edges
   (PathInput-only loaders) → their realized output schema locations. By
   construction expected == present, so a run loader is green and a never-run
   loader red. A **partially-run loader reads green from the graph** — with no
   DB input to enumerate, the un-run combos leave no trace. No longer the
   accepted limitation it was when this was written: `check_node_state` also
   asks the filesystem (`state._discovery_gate`, 2026-09-13), so the node a user
   sees is red when files on disk have never been loaded. The clause described
   here is unchanged — the gate is a second question asked beside it.
2. live prediction over current input data for each variant config the function
   has run with (`_predict_config_invocations`), plus a declared-inputs fallback.
   Safe because it shares the one hash fn — `_compute_fn_hash` ==
   `compute_function_hash(fn,16)` == the graph's stored `function_hash`.

`_current_records_by_schema` returns the **latest** record per `(schema_id,
producing-variant)` (variant = the producing invocation's constants, or raw),
so superseded inputs are not enumerated — keeping counts exact and avoiding a
false-red when an input is re-saved before the function's first run.

`check_combo_state` (the per-combo deep API, returning
`up_to_date`/`stale`/`missing`) is a separate, unchanged surface.

Note: `scistack-gui` keeps its own grey-based DAG-propagation run-state model on
top of this binary own-state; aligning it is a GUI-layer decision.
