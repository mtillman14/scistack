# Cleanup audit

*Started 2026-09-23 on `refactor/intent-and-fact` @ `23d3bfa3`. Companion plan:
`.claude/plan-cleanup-audit.md`. Builds on (does not replace)
`architecture-review-2026-09-20.md` and `layer-friction-analysis.md`.*

Four passes, each feeding one ranked findings register (§5):

1. **Measurements** (§1, done) — objective scans; they pick where to look.
2. **Concept-ownership table** (§2, done) — concept → owner → consumers → rivals (NOTE 4).
   Bugs it surfaced are in §3.
3. **Critical-path flow diagrams** (§4: 4.1 graph build, 4.2 schema level, 4.3 MATLAB run done; Python for_each + plot load need a measured run) — function-level, annotated with
   queries, locks, caches, and cross-layer reconstructions. Only for paths §1 flags.
4. **Re-check the 2026-09-20 review** (§4½, done) — what is closed, what is still open.

---

## 1. Measurements

Scanned 343 tracked source files (tests, docs, examples, bundles excluded).
Tooling was a rough indent-based Node scanner (no Python available), so
function spans for deeply nested closures are approximate. All numbers are
reproducible with `tools/audit/run.sh` (§6).

### 1.1 Size

| package | src py | test py | MATLAB src | MATLAB test | TS |
|---|---:|---:|---:|---:|---:|
| scistack-gui | 38,533 | 38,257 | | | 25,310 |
| scidb | 34,184 | 29,955 | | | |
| scistackplot | 15,247 | 15,633 | | | |
| scimatlab | 3,020 | 3,303 | 12,408 | 14,705 | |
| scifor | 5,626 | 6,165 | | | |
| scistackplotdb | 3,422 | 5,746 | | | |
| sciduckdb | 1,977 | 1,660 | | | |
| scidb-net | 1,193 | 324 | | | |
| scistack | 1,131 | 1,533 | | | |
| scistacklog | 552 | 452 | | | |
| scilineage | 276 | 420 | | | |
| scicanonicalhash | 185 | 181 | | | |
| scihist | **114** | **7,920** | | | |

`scihist` has 114 source lines and 7,920 lines of tests: its behaviour was
absorbed into scidb by the lineage simplification, but its test suite was not
moved. Those tests are testing scidb from the wrong package.

### 1.2 Hotspots (size × churn × fix-commits)

564 commits since 2025-07-08; 113 have fix-like messages. Fix counts are an
under-count (many commits bundle several fixes). Top files:

| commits | fix commits | lines | file |
|---:|---:|---:|---|
| 103 | 26 | 5,749 | `scidb/foreach.py` |
| 63 | 17 | 1,798 | `scistack_gui/api/pipeline.py` |
| 81 | 16 | 4,947 | `scidb/database.py` |
| 39 | 11 | 3,452 | `scidb/provenance_query.py` |
| 53 | 10 | 848 | `scistack_gui/server.py` |
| 37 | 10 | 2,234 | `scistack_gui/services/execution_service.py` |
| 35 | 9 | 2,876 | `scistack_gui/domain/graph_builder.py` |
| 30 | 9 | 2,003 | `scistack_gui/api/matlab_command.py` |
| 45 | 8 | 2,587 | `scifor/foreach.py` |
| 22 | 8 | 3,048 | `+scifor/for_each.m` |
| 31 | 8 | 302 | `frontend/src/api.ts` |
| 39 | 7 | 1,990 | `scistack_gui/api/run.py` |

### 1.3 Function shape

3,047 functions: 131 are over 100 lines, 35 over 200, 8 over 400; 24 take more than 10 parameters.

| lines | params | function |
|---:|---:|---|
| 1,522 | 18 | `scidb/foreach.py:1572 _for_each_prepare` |
| 1,039 | 29 | `scifor/foreach.py:86 for_each` |
| 951 | 2 | `scistack_gui/api/pipeline.py:705 _build_graph` |
| 684 | 9 | `scistack_gui/api/run.py:102 _run_in_thread` |
| 610 | 4 | `scidb/foreach.py:4998 _save_results` |
| 524 | 16 | `scimatlab/bridge.py:468 for_each_prepare` |
| 502 | 4 | `scidb/database.py:1299 save_batch` |
| 480 | 31 | `scidb/foreach.py:231 for_each` |
| 391 | 6 | `scidb/database.py:1920 _find_record` |
| 376 | 2 | `scistack_gui/config.py:296 load_config` |

The four largest hotspots in §1.2 each contain one of these functions.

### 1.4 Layering

**Package import graph is acyclic and points downward.** No package
imports up the stack.

**Private cross-package imports (11):** the ownership boundary exists on paper
but is crossed by reaching into internals:

- `scimatlab/bridge.py` imports **7** private `scidb.foreach` functions
  (`_for_each_prepare`, `_build_skip_hook`, `_resolve_for_columns`,
  `_endpoint_policy`, `_for_each_save_resolved`, `_apply_introspect`,
  `_load_var_type_all`). The MATLAB path assembles scidb's `for_each` from
  its private steps, so any internal refactor of `foreach.py` can break MATLAB
  without a signature change.
- `scidb` imports **8** private `sciduckdb` converters
  (`_storage_to_python`, `_dataframe_to_storage_rows`, `_unflatten_dict`, …).
  The storage-row encoding is shared but not public.
- `scidb.database._local` (thread-local current DB) is read by scidb-net and scistack.
- `scifor.foreach._merge_parts` → scidb; `scidb.pipeline._all_pipelines` → GUI.

**Lazy (function-level) imports:** 1,097 total, 758 in the GUI alone
(`scimatlab/bridge.py` 86, `execution_service.py` 76, `scope_service.py` 69).

**Ambient global database:** 60 `get_database()` calls, 21 in
`scidb/foreach.py`, several inside `try/except: pass` (§1.6).

### 1.5 Python ↔ MATLAB parity

`for_each` is implemented four times (scifor.py 2,587, scidb.py 5,749,
`+scifor/for_each.m` 3,048, `+scidb/for_each.m` 1,979). Option surfaces differ:

- Python `scidb.for_each` has `locations=` and `track_lineage=`; MATLAB `+scidb/for_each.m` parses neither.
- Python `scifor.for_each` has `locations=`; `+scifor/for_each.m` does not.
- MATLAB-only options (`categorical`, `nest_table_outputs`, `record_id_column`,
  `resolve_pathinput`, `fn_hash_override`, …) are either bridge plumbing or
  features with no Python counterpart. Needs a per-option classification.

*To verify:* whether `locations` is intentionally Python-only.

### 1.6 Silent failure

220 `except Exception` / bare `except` blocks; **37 swallow** (body is
`pass`/`return []`/`return None`). The ones on data paths matter most:

- **`scidb/intent.py:774 load_statements_sql` and `scistack_gui/intent_store.py:481 _rows`**:
  any SQL error, including a locked database or a missing table, returns
  `[]`. Intent statements are the user's wiring, hides, and pending
  constants. A transient lock makes them read as *absent*, so the canvas can
  silently drop edges or un-hide nodes. **Highest-priority item in this pass.**
- `scidb/foreach.py` × 9 (`_load_input` ×2, `_save_results`, `_get_schema_keys`,
  `_propagate_schema`, `_active_database`, `_load_var_type_as_spread` ×2,
  `_resolve_mapping_inputs`): mostly "fetch the ambient DB / schema, else
  carry on". A failure silently changes which schema keys the run uses.
- `sciduckdb.schema_keys_from_db`, `provenance_query._schema_ids_at_level`: same pattern at the storage layer.

The rest (rollback-after-failure, natural-sort keys, JSON fallbacks, WS client reaping) look benign.

### 1.7 Performance signals (static; unmeasured)

- **`database.list_versions`** (`database.py:3826`): per row, calls
  `_reconstruct_metadata_from_row` (which walks `derived_branch_params`) and
  then calls `derived_branch_params` again. That is 2N recursive graph walks,
  the batched-provenance N+1 trap. Reachable from MATLAB `list_versions`.
- `database.load` (`:3800`) calls `derived_branch_params` per record; confirm
  whether it runs in a loop on the `load_all` path.
- SQL call-site density: `sciduckdb.py` 37, `provenance_query.py` 35,
  `pipeline_store.py` 28, `database.py` 17. That's where query-count
  instrumentation would pay off.

### 1.8 Dead code (candidates; cross-checked against py/m/ts/toml/json)

31 functions (~680 lines) have no reference anywhere; 35 (~330 lines) are
referenced only by tests. Largest confirmed-looking:

- `scidb/database.py:2391 _load_by_record_row` (118 lines)
- `scimatlab/bridge.py`: `build_for_each_config_keys` (103),
  `get_data_column_name` (97), `discover_pathinput_combos` (84, left over
  from the PathInput resolution split), `load_var_type_all_as_df` (31)
- `scidb/database.py`: `invocation_exists`, `_is_tabular_dict`, `_get_variable_class`
- `scistack_gui/api/pipeline.py`: `_node_id_to_var_label`, `_get_record_counts`

False positives to ignore: FastAPI-decorated routes (`websocket_endpoint`,
`get_artifact_file`, `serve_frontend`).

### 1.9 Forwarding depth (GUI)

Name-collision scan found 90 names defined in 2+ files. Most are **pass-through
chains, not rival owners**: e.g. `unhide_edge` goes
`services/layout_service` → `pipeline_store` → `intent_store` (the owner), and
`write_manual_edge` goes `layout` → `pipeline_store` → `intent_store`. There's one
owner, reached through up to three wrapper layers. That's a readability and
maintenance cost, not a correctness bug. `api/*` ↔ `services/*` pairs are
likewise wrapper + implementation.

### 1.10 Markers

306 "trap/silently/workaround/hack" mentions and 317 date-stamped lines in
source; 4 TODO/FIXME. Top: `execution_service.py` 19, `api/matlab_command.py`
13, `scidb/foreach.py` 13, `scistackplotdb/source.py` 10.

### 1.11 Resolved since 2026-09-20

- GUI API is now one handler table (`api/tables.ALL_HANDLERS`); only 4
  FastAPI routes remain. The "108 RPC + 106 routes hand-mirrored" item is closed.

---

## 2. Concept-ownership table

One row per concept: who **owns** it (the single place its rule is spelled),
who **consumes** it, and any **rival** — a second place that spells the same
rule, or a gap where a consumer has to reconstruct it. Owners were verified by
grep on 2026-09-23, not taken from notes.

Status key: ✅ one owner, consumers route through it · ⚠️ one owner but a
leak (private access, ambient global, GUI-side reconstruction) · ❌ rival
owners or no owner, **bug class present**.

### 2.1 Identity

| concept | owner | consumers | rival / gap | status |
|---|---|---|---|---|
| Canvas node id + handle spelling | `scistack_gui/ids.py` (AST guard) | graph_builder, stores, services | — | ✅ |
| Scope-qualified (placement) id | `ids.placement_id` + `scope_filter.resolve_scope_view` | every node-keyed state lookup | lookup-before-resolve trap fixed 4× (review §5); typed `BareNodeId`/`PlacedNodeId` now | ✅ |
| Function-node identity | allocated id, minted once (`_node_wiring`, D-2026-09-22-1/3) | layout, intent statements | graduation id-swap kept on purpose (D-2026-09-22-4) | ✅ |
| Call-site identity (`call_id`) | `scidb.foreach_config.CallSite` | scidb forward + backward, GUI `variant_resolver.compute_call_id` | parity test `test_identity_parity.py` | ✅ |
| `wiring_id` (now an attribute) | `scidb.provenance.compute_wiring_id` | `derive_target_for_node`, hidden wirings, grouping | — | ✅ |
| rid / vsig spellings | `scidb/bindings.py` | foreach, provenance, plot loaders | — | ✅ |
| Variant coordinate (`branch_params`) | `scidb.variant.VariantAxes` | loaders, Topologies panel, plots | — | ✅ |
| Constant identity | `scidb.provenance.constants_identity_key` / `compute_constant_record_id` | variant grouping, save | — | ✅ |
| **Parameter identity (declared name ↔ argument name)** | forward: `edge_resolver.ResolvedWiring.parameter_params`; **backward: none** | execution_service (translates), matlab_command_service (translates), graph_builder (**doesn't**) | history keys constants by *argument* name; the canvas keys Parameters by *declared* name. `graph_builder.py:1618` builds `param__{argument}`. **B1** | ❌ |
| PathInput identity (declared ↔ argument) | forward: `ResolvedWiring.path_input_params`; backward: `graph_builder.convert_scidb_path_inputs` (GUI, reverse-matches by registry value + name history) | execution_service `_db_path_input_params`, build_edges | backward mapping is reconstructed in the GUI, not recorded by scidb (NOTE 3) | ⚠️ |
| Function hash | Python: `foreach_config.function_sources_for`; MATLAB: bridge-supplied digest | provenance, source capture, variant axes | two recipes. `foreach.py:5575` refused source capture for `calculateSymmetryOneVector` on 2026-09-23 ("the two recipes have drifted"). Possibly CRLF on Windows (cf. hash-recipes memory: agree on ASCII/**LF**) | ❌ |

### 2.2 Configuration and environment

| concept | owner | consumers | rival / gap | status |
|---|---|---|---|---|
| Project root | `scifor.pathinput.project_root()` (pin, else cwd; D-2026-09-23-1) | GUI registry (pins), MATLAB `load_entities`, PathInput | GUI `locate_config_at` counts a bare `pyproject.toml` as config; scifor's doesn't (open in the decision) | ⚠️ |
| Entities file path | `scidb.entities.resolve_entities_path` | GUI bootstrap, MATLAB | memory says `server.py` duplicates the bootstrap startup inline; *to verify* | ⚠️ |
| Entity editability | `target_file_service.entity_editability` | sidebar panels | — | ✅ |
| Dataset schema keys | `DatabaseManager.dataset_schema_keys` | foreach, loaders | **mirrored** into the `scifor.set_schema()` global in 3 places (`database.py:744`, `foreach.py:5664`, `:5677`); readers fall back to the global and swallow errors (§1.6) | ⚠️ |
| Current database | `scidb.database._local` / `get_database()` | 60 call sites | scidb-net and scistack read the private `_local` | ⚠️ |
| GUI default schema level | `execution_service.default_schema_level` | GUI runs, MATLAB command generation | — | ✅ |
| Load errors | `registry.all_load_errors` | sidebar, logs | — | ✅ |

### 2.3 Intent (what the user stated)

| concept | owner | consumers | rival / gap | status |
|---|---|---|---|---|
| Intent statements (wiring, hides, constants, columns) | `scidb.intent` + `scistack_gui/intent_store.py` | `pipeline_store` and `layout` (forwarders), services | reads swallow SQL errors as "no statements" (**F1**); up to 3 forwarding hops (F10) | ⚠️ |
| Current wiring of a node | `edge_resolver.ResolvedWiring.bindings` (one dict, three views) | execution, MATLAB command, call_id | — | ✅ |
| Manual edges vs history | `graph_builder.manual_input_overrides` (rule: visible edges outrank history) | build_edges, execution | **B1 breaks the rule**: a history constant edge supersedes the manual Parameter edge because the two have different source ids | ❌ |
| **Column-selection shape** | `scidb.intent.normalize_columns` / `same_columns` | GUI chip, `provenance_save.compute_input_selectors`, `mark_unused_intent` | "every column" is **symbolic** in intent (`{columns: [], iterate: true}`) but **resolved** in the recorded selector. `same_columns` compares literally, so every all-columns `for_columns` is flagged "not reflected by its last run" (seen 14:45:56). **B2** | ❌ |
| Entity values (Parameter values etc.) | the entities TOML via `scidb.entities` (only writable surface) | GUI panels, MATLAB | — | ✅ |

### 2.4 Execution

| concept | owner | consumers | rival / gap | status |
|---|---|---|---|---|
| `for_each` semantics | Python `scifor.for_each` + `scidb.for_each` | GUI runs, scripts | **MATLAB re-implements both** (`+scifor/for_each.m`, `+scidb/for_each.m`), hand-mirrored; option gaps (F6); bridge assembles scidb's run from 7 private steps (F2) | ❌ (by design, costly) |
| PathInput resolution + discovery | `scifor.PathInput.apply_discovery` | scifor (MATLAB side resolves), GUI | — | ✅ |
| Storage-row encoding | `sciduckdb` | scidb | consumed through 8 private functions (F7) | ⚠️ |
| Run completion status | `scidb/run_markers.py` | GUI matlab_watch | — | ✅ |
| Pinned loads | `scidb.variant.pin_loads_uncollapsed` | plot loaders, inspect | — | ✅ |
| Line recurrence (plots) | `scistackplot.roles.line_recurrence` | reduce, render | — | ✅ |

### 2.5 What the table says

- **Every ❌ row is a divergence between the forward (intent/run) path and
  the backward (history → canvas) path.** B1, B2 and the function-hash drift
  follow the same pattern as the 2026-09-22 `grSides` duplicate: the run records
  a *resolved* or *renamed* form, and the canvas reconstructs identity from
  that record without the mapping the forward path had.
- The ✅ rows that used to be ❌ (call_id, wiring, column shape, rid spellings)
  were fixed by making scidb record or own the rule, with a parity test. The
  pattern that worked: **record the mapping at write time** rather than reverse-engineer it on read.
- PathInput identity (⚠️) works today only because the GUI can reverse-match by
  value/history. Constants are content-addressed and can be shared by two
  Parameters with equal values, so that reverse-match is not safe for them. That
  is why B1 needs a recorded mapping, not a PathInput-style lookup.

---

## 3. Bugs found during the audit

### B1 — Running a function duplicates its Parameter node

**Symptom (user, 2026-09-23):** after a run, a second Parameter node appears;
only one of the two is connected to the function.

**Evidence (`scidb.log`):** each first successful run of a function raises
`build_parameter_nodes` from 4 to 5 names (11:55:54 `loadGaitRiteOneFile`,
14:45:56 `calculateSymmetryOneVector`); the user then hides the extra
(`filter_hidden … 1 const` at 11:56:57, `2 const` at 14:46:55). The run log
names the mismatch: `parameter 'gaitRiteConfig' (declared 'gaitrite_config')`.

**Mechanism:**
1. The canvas Parameter is `param__gaitrite_config` (declared name), wired by a
   manual edge into the `gaitRiteConfig` handle.
2. `execution_service` translates declared → argument name; `for_each` receives
   `gaitRiteConfig=<value>`. `scidb.Parameter` has no name of its own, so
   provenance records the constant under the argument name only.
3. Next graph build: `build_parameter_nodes` takes `const_counts ∪ declared`,
   so `gaitRiteConfig` becomes a new node. `build_edges`
   (`graph_builder.py:1618`) draws `param__gaitRiteConfig → fn`, and the
   manual edge from `param__gaitrite_config` is counted "superseded by
   DB-derived". The declared node is left without an edge.

When declared name == argument name the two ids coincide, which is why this
went unseen.

**Fix (2026-09-23, Option A, tests written, unrun).** The run records the
declared name; nothing reconstructs it on read.
- One owner: `scidb.parameter.declared_parameter_names(inputs, explicit)`
  merges an explicit `parameter_names=` (GUI wiring, generated MATLAB) with a
  named `Parameter` (`.name`, set by the entities loader and discovery).
  `parameter_node_name` owns the read-side fallback (recorded name, else
  argument).
- Storage: `_invocation_input.declared_name` (additive column, not identity;
  latest run wins via `SciDuck._bulk_update`).
- `for_each` resolves the names before EachOf expansion and carries them on
  `_ForEachState` to `record_run`. MATLAB: `+scidb/for_each.m`
  `parameter_names` option → bridge → same owner; pipeline replay forwards it.
- Read: `pipeline_variants` / `get_aggregated_variants` key constants by
  node and report `{argument: node}`; the GUI `AggregatedData` translates
  with `constant_node` / `constant_args` (edges leave the node, land on
  the argument handle; pending-value coverage translates too).
- Logs: `[provenance] … argument->Parameter`, `[graph_builder] … feed an
  argument of another name`, `build_parameter_nodes: … from run history
  alone`, DEBUG Parameter node ids after each build.
- Tests: `scidb/tests/test_parameter_declared_names.py`,
  `scistack-gui/tests/test_parameter_identity_b1.py`,
  `scimatlab/tests/test_bridge_parameter_names.py`.
- Old history keeps the argument name until that function is re-run.

### B2 — "Column selection not reflected by its last run" for all-columns `for_columns`

`same_columns({columns: [], iterate: true}, {columns: [<resolved>], iterate: true})`
is `False`. The unused-intent chip then lies on every all-columns node that ran.

**Fix (2026-09-23, tests written, unrun).** `scidb.intent.is_every_column`;
`same_columns` treats "every column" as matching any per-column selection.
Only diagnostic callers (the chip, `[selector-changed]`). Tests in
`scidb/tests/test_intent.py`.

### Also in today's log (not yet investigated)

- `get_pipeline` took **10.1 s** at 14:45:56 (2.2 s at 11:55:54). Most of it
  sits between `pathinput_discover` and `propagate_run_states`. Feeds §4 (flow
  diagrams, perf).
- Function-hash drift for a MATLAB function (§2.1).

---

## 4. Critical-path flow diagrams

Function-level traces of the paths §1 flagged. Each is annotated with where
the time goes (**measured** from `scidb.log` timestamps, or *unmeasured*), the
queries it issues, and where one layer reconstructs something another already
knows.

### 4.1 `get_pipeline` — the canvas graph build (F15, F16)

Measured on the real Stroke-R01-Aim2 database, 2026-09-23 14:45:50→56, right
after a MATLAB run: **10.1 s** total (`RPC << get_pipeline … (10078.0ms)`).
Gaps are the time between consecutive log lines, charged to the step that ends
them.

```
get_pipeline (api/pipeline._build_graph, 951 lines)            10.1 s
│
├─ list_hypotheses / get_hidden_pipelines                         ~0.1 s
├─ db.get_aggregated_variants()                                   2.8 s  MEASURED
│    └─ provenance_query.pipeline_variants
│         for each _invocation (non-save):                        N+1 ×4
│           invocation_inputs(inv)           ── 1+ query
│           selectors/declared_name          ── 1 query
│           invocation_path_inputs(inv)      ── 1 query
│           outputs JOIN _record             ── 1 query
│         _annotate_variants                  (batched ✓)
│    └─ per variable type: COUNT(*) _record    ── 1 query each (small N)
├─ build_aggregate → filter_hidden → run-state pass 1
│    └─ _compute_run_states
│         └─ scidb.state.check_multiple_nodes_state
│              for each fn call site:
│                check_node_state
│                  expected_invocations_for_function          unmeasured
│                    function_variant_configs(fn)  ── per-invocation reads?
│                  present_invocation_schema_pairs  (batched ✓)
│                  for each expected combo:                     N+1 ×1
│                    _schema_id_to_combo  ── SELECT … WHERE schema_id=?
│                    (result used only for per-combo detail; the canvas
│                     reads counts + state only)
│                  _discovery_gate → PathInput.discover()
│                    network walk, cached 190 dirs              ~1.4 s MEASURED
│              (everything after the last walk, no log lines)    4.4 s MEASURED
├─ group_call_sites_by_wiring → run-state pass 2 (propagate)       <0.01 s
├─ build_*_nodes / build_edges / merge_manual_nodes              <0.1 s
└─ scope filter, unused-intent marks, assemble                    <0.05 s
```

**Reading it.** About 7 of the 10 s is scidb answering two questions per
canvas refresh: "what has run" (`pipeline_variants`) and "what should have run"
(`check_node_state`). Both walk the provenance graph one invocation or one combo
at a time. The GUI steps are negligible. The same walk runs again on EVERY
`dag_updated` (each run, each edit), and a session log shows ~60
`get_pipeline` calls.

**Known N+1s on this path**
- `pipeline_variants`: ~4 queries per invocation (F16).
- `check_node_state`: 1 query per expected combo in `_schema_id_to_combo`, for
  data the canvas discards (new, **F21**).

**Instrumentation to add before fixing** (NOTE 2): `Log.timings` phases in
`get_aggregated_variants` (variants / var counts), `check_node_state`
(expected / present / combos / discovery) and a per-call-site line in
`check_multiple_nodes_state` with its combo count. That turns the 4.4 s gap
into named numbers and says whether `expected_invocations_for_function` or the
combo loop dominates.

### 4.2 Schema-level default — which keys a run iterates

The user asked (2026-09-23) whether the node's level is set from its inputs,
across mixed levels and for PathInput-only functions.

```
Python "Run" (api/run._run_in_thread) ─┐
Python pipeline (build_backend_pipeline)┼─► execution_service.default_schema_level
                                        │     1 stated on node (schemaLevel)
                                        │     2 recorded: provenance_query.recorded_schema_keys
                                        │         (latest run of the FUNCTION NAME)
                                        │     3 inputs: variable_schema_keys ∪ PathInput
                                        │         placeholder_keys → finest_schema_keys (union)
                                        │     4 all dataset keys
MATLAB "Run" (generate_matlab_command) ─┐
MATLAB pipeline script                  ┼─► params["schema_level"] (node's stated value or null)
                                        │     _resolve_iterate_keys: null → ALL keys
                                        │     emits 'key', [] for every key → scidb iterates
                                        │     every key that has values in the DB
Canvas (FunctionSettingsPanel)          ─► null shown as EVERY box ticked
```

**Status 2026-09-23:** F22–F27 fixed by `.claude/plan-schema-level-default.md`: one owner `scidb.schema_level`, all routes via `execution_service.default_schema_level`, panel via the `get_schema_level` RPC.

**Findings**
- **F22 (high): MATLAB runs never use the default.** Evidence: `loadDemographics`
  (one PathInput, no placeholders, i.e. the inputs rule says "one call") ran
  **714 iterations** of the same file at 11:57:55 (subject×session×speed×trial,
  whatever keys had values). It dropped to 1 only after the user deselected
  every level by hand (11:58:24). `calculateSymmetryOneVector` and `grSides`
  iterated 450 combos the same way. Two owners for one concept: the Python
  route asks `default_schema_level`, the MATLAB route asks nothing.
- **F23 (med): the canvas shows "all keys" for an unstated level**, while a
  Python run would iterate the inferred level and a MATLAB run every
  populated key. The node never shows what will actually happen.
- **F24 (med): `recorded_schema_keys` is function-name scoped**, not node or
  call-site scoped. Two canvas nodes of one function take the level of
  whichever ran last. It also outranks the inputs, so rewiring a node to a finer
  input keeps the old level until the user sets one.
- **F25 (low): alternate-template PathInputs are skipped.** The registry holds
  them as `EachOf(PathInput, …)`, which has no `placeholder_keys`; the
  `hasattr` guard drops them silently. As a function's only input, that
  falls through to "all keys".
- **F26 (high): `[]` means three things.** "Every level unticked" is stored as
  `schemaLevel: []`. `_resolve_iterate_keys` (MATLAB) reads it as one
  dataset-level call; `api/run.py` skips the default (`is None` test) and
  passes `schema_keys=[]` to `for_each`, which means EVERY key; the pipeline
  route calls `default_schema_level(stated=[])`, whose `if stated:` treats it
  as unset.
- **Nothing seeds a level on a new node** (verified: the panel writes
  `schemaLevel` only on a checkbox click; no backend writer). The precedence
  still misbehaves on a first run via F23 (the display invites a click that
  becomes a permanent stated level) and F24 (a new node of an already-run
  function takes the other node's level instead of its inputs').
- Correct as designed (tests exist, `scistack-gui/tests/test_execution_service.py`
  ~l.429–600): mixed levels iterate the union, and the coarser input broadcasts;
  a PathInput with no placeholder means one call; a dataset-level variable
  means one call; a recordless input is not a level.

### 4.3 A MATLAB run, end to end (load + save included)

Measured from `scidb.log` [timing] lines, 2026-09-23. Two runs:
**A** `loadGaitRiteOneFile` (PathInput loader, 150 combos, 450 records saved,
70.5 s script total) and **B** `calculateSymmetryOneVector` (one trial-level
`for_columns()` input, 450 combos, 420 records).

```
generated script (api/matlab_command) → MATLAB terminal
│
├─ matlab_preamble                                   A 4.72 s   B 4.63 s
│    addpath                                         A 4.50 s   B 4.38 s  ◄ 95%
│    entities / configure_database / register        ~0.2 s
│
├─ +scidb/for_each.m → bridge.for_each_prepare (Python)
│    A: PathInput discovery walk (network)           1.60 s
│    B: _resolve_all_columns(GAITRiteLoaded_UA)      1.10 s  ◄ FULL load, for column NAMES
│         _find_record 0.11 s (collapse 0.10 s) + load_all_as_df 1.09 s
│       combo pruning ("filtered 348 of 798")
│       _load_input(GAITRiteLoaded_UA)               1.03 s  ◄ SAME load again
│    prepare total                                    B 2.37 s
│
├─ MATLAB: convert inputs (py → MATLAB)              A 0.13 s   B 1.33 s (6.1 MB table)
├─ MATLAB: scifor.for_each loop (user code)          A 56.9 s   B 2.5 s
│    (A is xlsread over the network, not scistack)
├─ MATLAB → Python result transfer                   A ~4.3 s   B ~0.7 s  UNLOGGED gap
│    (between "done in" and the bridge's for_each_save line)
│
├─ bridge.for_each_save → scidb._save_results
│    save_batch                                      A 1.93 s   B 1.50 s
│      per_row_hashing (canonical_hash)              A 1.70 s   B 1.32 s  ◄ ~3 ms/row
│      inserts                                       ~0.2 s
│    record_run                                      A 0.08 s   B 0.33 s
│      (B: 420 invocations, assemble loop 0.25 s)
│    source capture: REFUSED for B (fn-hash recipes drifted, F13)
│
└─ db.close, marker, then GUI get_pipeline           10.1 s (§4.1)
```

**Reading it.** For a fast user function (B), scistack's own overhead is
about 4.6 s of preamble, 2.4 s of prepare, 1.3 s of conversion, 0.7 s of transfer
and 1.5 s of save, all around a 2.5 s computation. Then the canvas refresh costs
another 10 s. Largest items:
- **addpath ~4.4 s every run (F29).** 95% of the preamble, identical every
  time. This is the "~4-5 s unattributed" from the preamble-timing memory, now
  attributed. Next question: is it `genpath` over the UNC share, or the sheer
  number of folders?
- **Double full load for `for_columns()` (F28).** Resolving "every column"
  loads the whole variable to read its column names, then prepare loads it
  again. The names are available without loading any data (the storage
  table's columns); cost today ≈ one extra full load per all-columns input.
- **Per-row canonical hashing in `save_batch` (F30)** dominates the save,
  ~3 ms per record. Unmeasured whether it is the value hash or the metadata
  hash.
- **Result transfer is unlogged (F31)**: 4.3 s for A's 450×59 table.

**Instrumentation to add** (NOTE 2): a `[timing] addpath` breakdown (count of
folders, genpath vs explicit); a `for_each_result_transfer` phase in
`+scidb/for_each.m` before the save RPC; `save_batch` hashing split into
value vs metadata.

---

## 4½. Re-check of the 2026-09-20 architecture review

Status against the code on 2026-09-23 (numbers re-measured, not copied).

| # | review item | status | evidence now |
|---|---|---|---|
| 1 | Identity is two systems (move `wiring_id` into scidb + parity tests) | **closed** | `scidb.provenance.compute_wiring_id`; `scidb/tests/test_identity_parity.py`. The same *class* recurred for Parameters (B1, fixed) and schema levels (F22–F26, open) |
| 2 | `for_each` assembles provenance two ways; extract phases | **partial** | phases exist (`_for_each_prepare` / `_execute` / `_save_results`, typed `_ForEachState`, `RunBindings.for_combo` one edge function; `_save_results` 17 → 4 params), but `_for_each_prepare` is now a **1,522-line** body, larger than the 975-line `for_each` the review measured |
| 3 | Two hand-mirrored GUI transports | **closed** | `api/tables.ALL_HANDLERS`; 4 FastAPI routes remain |
| 4 | Lazy imports from cycles | **partial** | scidb 352 → 159, GUI 850 → 758; `scidb/tests/test_imports.py` guards scidb only; the GUI has no guard |
| 5 | Rules in comments, not types | **partial** | `BareNodeId`/`PlacedNodeId` exist; `[]` vs `None` for schema levels is still a paragraph, and it bit again (F26). `docs/claude` grew 27k → 28.9k lines; `.claude/` plans 127 → 267; no pruning happened |
| 6 | Scope-awareness half done | **closed** (per the 2026-09-21 commit; `_intent` writes carry `scope`, `copy_scope`, `scope_of_node` used by runs) | not re-verified end to end |
| 7 | MATLAB is a second implementation | **open**, and the biggest source of today's findings | F2, F6, F17, F18, F22 are all MATLAB-mirror drift |
| 8 | `DatabaseManager` god object | **open** | 73 → 71 methods |
| 9 | Frontend (PlotStudio size, untyped node data, two builds) | **open** | `PlotStudio.tsx` 3,916 → 3,986 lines; both vite targets still separate commands |
| 10 | Bespoke migrations | **open, growing** | 8 `ALTER TABLE` sites (B1 added one: `_invocation_input.declared_name`) |

**Pattern across §2–§4:** the review's item 7 (MATLAB as a hand-mirrored
second implementation) is now the dominant source of new bugs. Items 1–3 fixed
their instances, but the same "two owners" shape keeps reappearing wherever a
MATLAB route re-derives what a Python route asks scidb for.

---

## 5. Findings register (running)

### Ranked next actions (2026-09-23)

Ordered by *wrong results* first, then *silent data loss*, then *time spent
per canvas refresh or run*, then maintainability.

1. **Schema level (F22, F26, then F23–F25, F27)**: runs iterate the wrong keys
   (loadDemographics ×714; `[]` = every key on Python). Plan written:
   `.claude/plan-schema-level-default.md`.
2. **Intent reads swallow errors (F1)**: a lock makes wiring and hides read as
   absent. Small: raise or log with the lock holder, and never return `[]` for
   an error.
3. **Canvas refresh 10 s (F15, F16, F21)**: add the §4.1 timers, then batch
   `pipeline_variants` and `_schema_id_to_combo`. Runs about 60× a session.
4. **Double load for `for_columns()` (F28)**: read column names from storage
   metadata instead of loading the data.
5. **MATLAB preamble addpath 4.4 s (F29)**: measure folder count and genpath
   first.
6. **Function-hash drift MATLAB↔Python (F13)**: source is not being captured
   for MATLAB functions whose recipes disagree.
7. **Dropped kwargs on mirrored routes (F17, F18)**: glue in the MATLAB pipeline
   script and replay; `locations` on bridge real runs.
8. Maintainability: private cross-package imports (F2, F7), test-only rival
   copies (F19, F27), dead code (F9), scihist test placement (F8), forwarding
   depth (F10), the 1,522-line `_for_each_prepare` (F4), migrations (review #10).

Theme behind 1, 6 and 7: the MATLAB route re-derives what the Python route
asks scidb for (review item 7). Each fix above should route MATLAB through the
scidb owner rather than add a MATLAB-side rule.


| # | severity | kind | finding | evidence |
|---|---|---|---|---|
| F1 | high | silent failure — **fixed 2026-09-23, tests unrun** | Intent reads return `[]` on any SQL error, so wiring/hides vanish under a lock | §1.6 |
| F2 | high | coupling | MATLAB for_each built from 7 private scidb.foreach steps | §1.4 |
| F3 | med | perf (N+1) — **fixed 2026-09-23: one `branch_params_batch` pass; its silent `except: return []` now warns; tests unrun** | `list_versions` does 2 graph walks per row | §1.7 |
| F4 | med | structure | `_for_each_prepare` 1,522 lines; scifor `for_each` 1,039 lines / 29 params | §1.3 |
| F5 | med | silent failure — **fixed 2026-09-23: `database.database_or_none` (only DatabaseNotConfiguredError reads as None) replaces 13 swallowing lookups in foreach; tests unrun** | ambient `get_database()`/schema lookups swallowed in foreach | §1.4, §1.6 |
| F6 | med | parity — **locations fixed 2026-09-23: `+scidb/for_each.m` 'locations' (JSON) -> bridge filters combos; generator emits the exact selection. `track_lineage` remains Python-only; tests unrun** | `locations`/`track_lineage` missing from MATLAB for_each | §1.5 |
| F7 | med | coupling | scidb depends on 8 private sciduckdb converters | §1.4 |
| F8 | low | test placement | scihist: 114 src lines, 7,920 test lines | §1.1 |
| F9 | low | dead code — **done 2026-09-23 for private/internal functions: `_load_by_record_row`, 6 bridge functions, scifor `_describe_result`, `_is_tabular_dict`, 2 api/pipeline helpers, `get_all_path_input_names` (~480 lines). Kept: public methods / scistackplot API / decorator routes** | ~680 unreferenced lines, top 5 listed | §1.8 |
| F10 | low | readability | GUI forwarding chains (3 hops to intent_store) | §1.9 |
| F11 | high | owner gap (B1) — **fixed, tests unrun** | Parameter declared↔argument name not recorded; run duplicates the Parameter node | §2.1, §3 |
| F12 | med | owner split (B2) — **fixed, tests unrun** | symbolic vs resolved "all columns" gives a false unused-intent chip | §2.3, §3 |
| F13 | med | rival recipes — **root cause 2026-09-23: NOT a recipe drift. The MATLAB bridge passes a Python sentinel as `fn`; the save path hashed the sentinel's own body. Fixed: the sentinel carries the MATLAB digest as `source_hash`, and `function_sources_for` treats digest-only as nothing to capture; tests unrun** | MATLAB vs Python function hash drift blocks source capture | §2.1 |
| F14 | med | dual holder — **fixed 2026-09-23: scidb reads `database.dataset_schema_keys_of`, never scifor's copy back; the copy is still written for scifor; tests unrun** | dataset schema keys mirrored into the scifor global in 3 places | §2.2 |
| F15 | med | perf — **instrumented 2026-09-23, awaiting a measured run** | `get_pipeline` 10 s after a run (real data) | §3 |
| F16 | med | perf (N+1) — **instrumented 2026-09-23 (`[timing] pipeline_variants` by query kind), awaiting a measured run** | `provenance_query.pipeline_variants` runs ~4 queries per invocation — likely F15 | found fixing B1 |
| F17 | med | dropped kwarg — **fixed 2026-09-23: glue forwarded by Pipeline.m replay and per step in the MATLAB pipeline script; tests unrun** | MATLAB `Pipeline.m` replay forwards no `glue`; the MATLAB pipeline-script generator passes no `glue` to `_for_each_call_lines` | found fixing B1 |
| F18 | med | dropped kwarg — **2026-09-23: bridge half was a false alarm (bridge filters combos itself); Python register_call now keeps `locations` and key_map bindings rename them (`LocationFilter.renamed`); tests unrun** | bridge real-run `_for_each_prepare` gets no `locations` (dry run does); Python `register_call` options omit `locations` | found fixing B1 |
| F19 | low | rival converter | `graph_builder.aggregate_variants` is test-only; production uses `api/pipeline.build_aggregate` | found fixing B1 |
| F20 | med | owner gap | pending constants were looked up by argument name in the pending-row synthesis (fixed with B1); audit other name-keyed GUI state for the same split | found fixing B1 |
| F21 | med | perf (N+1) — **fixed 2026-09-23 (one batched `_schema_ids_to_combos`), tests unrun** | `state._schema_id_to_combo`: one query per expected combo, result unused by the canvas | §4.1 |
| F22 | high | two owners — **fixed 2026-09-23, tests unrun** | MATLAB runs ignore `default_schema_level`: null level means every populated key (loadDemographics ×714) | §4.2 |
| F23 | med | display ≠ behaviour — **fixed 2026-09-23, tests unrun** | unstated level shows all keys ticked; neither run route does that | §4.2 |
| F24 | med | scope — **fixed 2026-09-23, tests unrun** | recorded level is function-name scoped and outranks rewired inputs | §4.2 |
| F25 | low | silent skip — **fixed 2026-09-23, tests unrun** | alternate-template PathInputs (EachOf) contribute no level | §4.2 |
| F26 | high | three meanings — **fixed 2026-09-23, tests unrun** | a node level of `[]` ("no levels ticked"): MATLAB generator = one dataset call; Python Run passes `for_each(schema_keys=[])` = EVERY key; pipeline `default_schema_level` `if stated:` = unset | §4.2 |
| F27 | low | rival copy — **fixed 2026-09-23, tests unrun** | `variant_resolver.build_schema_kwargs` (None → all keys) is test-only | §4.2 |
| F28 | med | perf (duplicate) — **fixed 2026-09-23: `DatabaseManager.data_column_names` reads `_variables.dtype`; a load only as fallback. Note: direct-save kwarg columns are no longer counted as data columns; tests unrun** | `for_columns()` all-columns resolution loads the whole variable for its column names; prepare then loads it again (1.1 s ×2) | §4.3 |
| F29 | med | perf — **instrumented 2026-09-23: `[timing] matlab_addpath` (per-dir times, already_on_path, path size, slowest dir); awaiting a MATLAB run** | MATLAB preamble `addpath` ~4.4 s of 4.7 s, every run | §4.3 |
| F30 | low | perf | `save_batch` per-row canonical hashing ~3 ms/record | §4.3 |
| F31 | low | observability | MATLAB→Python result transfer unlogged (4.3 s for 450×59) | §4.3 |
| F32 | low | route-only rule — **decided 2026-09-23 (option A): rule dropped on every route; as_table is format only, aggregate by stating the level; tests unrun** | an unstated level on an `as_table` run pools into one call on the Python Run only; pipeline and MATLAB routes do not. Now one flag in `default_schema_level`, still undecided whether every route should apply it | found fixing F22 |
| F33 | med | silent no-op | the panel omitted null location keys, so `split_node_config` never cleared a stated level (fixed: all three keys always sent) | found fixing F23 |
| F34 | high | silent data loss | `intent_store._import_once` marked the one-time legacy import DONE on any error, so a lock during it dropped legacy hides/pending values/edges permanently (fixed with F1: only a missing source table counts as nothing to copy; other errors retry next open) | found fixing F1 |
| F35 | med | feature gap | MATLAB function source is never captured (`_function_source` rows exist only for Python): nothing sends the `.m` text. Needed for any code-version view of MATLAB functions (variant-selection). Bridge-side: send `source_text` with the digest, verify the digest matches before storing | found fixing F13 |
| F36 | high | ignored setting | a node's Schema Selection applied only to its own Run button: the Python pipeline passed no `locations`, the MATLAB pipeline one request-wide `schema_filter` for every step (fixed 2026-09-23: both read each node's stored `schemaSelection`; tests unrun) | found fixing F6 |

## 6. Reproducing

`tools/audit/run.sh` regenerates every number in §1 (see `tools/audit/README.md`).
