# Cleanup audit

*Started 2026-09-23 on `refactor/intent-and-fact` @ `23d3bfa3`. Companion plan:
`.claude/plan-cleanup-audit.md`. Builds on (does not replace)
`architecture-review-2026-09-20.md` and `layer-friction-analysis.md`.*

Four passes, each feeding one ranked findings register (§5):

1. **Measurements** (this section, done) — objective scans; they pick where to look.
2. **Concept-ownership table** — concept → owner → consumers → rivals (NOTE 4).
3. **Critical-path flow diagrams** — function-level, annotated with queries,
   locks, caches, and cross-layer reconstructions. Only for paths §1 flags.
4. **Re-check the 2026-09-20 review** — what is closed, what is still open.

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

## 5. Findings register (running)

| # | severity | kind | finding | evidence |
|---|---|---|---|---|
| F1 | high | silent failure | Intent reads return `[]` on any SQL error, so wiring/hides vanish under a lock | §1.6 |
| F2 | high | coupling | MATLAB for_each built from 7 private scidb.foreach steps | §1.4 |
| F3 | med | perf (N+1) | `list_versions` does 2 graph walks per row | §1.7 |
| F4 | med | structure | `_for_each_prepare` 1,522 lines; scifor `for_each` 1,039 lines / 29 params | §1.3 |
| F5 | med | silent failure | ambient `get_database()`/schema lookups swallowed in foreach | §1.4, §1.6 |
| F6 | med | parity | `locations`/`track_lineage` missing from MATLAB for_each | §1.5 |
| F7 | med | coupling | scidb depends on 8 private sciduckdb converters | §1.4 |
| F8 | low | test placement | scihist: 114 src lines, 7,920 test lines | §1.1 |
| F9 | low | dead code | ~680 unreferenced lines, top 5 listed | §1.8 |
| F10 | low | readability | GUI forwarding chains (3 hops to intent_store) | §1.9 |

## 6. Reproducing

`tools/audit/run.sh` regenerates every number in §1 (see `tools/audit/README.md`).
