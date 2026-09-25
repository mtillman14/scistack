# Database Model — Canonical Reference

> **This is the canonical, up-to-date reference for the scidb database structure
> and how every common operation works against it.** Branch `dev-hist`, as-built
> after the lineage-simplification migration (current as of 2026-06-21).
>
> It supersedes and consolidates:
> - `lineage-simplification.md` — the original *design* doc (pre-implementation).
> - `archive/bipartite-provenance.md` — an earlier as-built note (now partly stale: it
>   still describes `_record_metadata`/`version_keys`/`lineage_hash`, which were
>   renamed/slimmed to `_record_save` — see §1).
> - `archive/graph-database-state.md` — the detailed current-state + remaining-TODOs doc;
>   still the place to read for the TODO list, design notes, and module map.
>
> Read those for history, rationale, and open decisions. Read **this** for "what
> is the database and how do the operations work."

---

## 0. The big picture

The migration replaced the old dual-tracking system — the `_lineage` table, the
`version_keys` / `branch_params` JSON-blob columns, the `_for_each_expected`
snapshot table, the `@lineage_fcn` / `LineageFcnResult` wrapper, and the rerun
cache — with **a single normalized graph that is the only source of truth.**

Everything is now *derived* from the graph + `_schema`: variants, provenance,
`where` matching, staleness, node coloring, and the metadata a loaded record
exposes. The guiding principle is that **nothing is stored that duplicates or
predicts what the graph already encodes** — those duplications were the drift
hazards the migration removed.

Identity is **content-addressed**: re-running an identical pipeline reproduces
every id (idempotent, all inserts `ON CONFLICT DO NOTHING`) and only appends a
fresh `_run` audit row.

---

## 1. Schema

### Provenance graph (`scidb/provenance.py`)

A **bipartite graph** in the classic W3C-PROV shape: **records** (entities) and
**invocations** (activities), connected by edges `record → invocation → record`.
A single invocation can have many inputs and many outputs, which is why the call
is a first-class node rather than a flat edge table.

| Table | Purpose |
|---|---|
| `_record` | Every entity. `record_id, created_at, type, schema_id, content_hash, schema_version, excluded`. Variables: `type` = class name, `schema_id` set. Constants: `type = '__constant__'`, `schema_id` NULL. PathInput specs: `type = '__pathinput__'`, `schema_id` NULL. Content-addressed, immutable, one row each (`ON CONFLICT DO NOTHING`). |
| `_constant` | A constant/pathinput entity's value: `record_id, value_repr, value_type, content_hash`. |
| `_invocation` | One row per unique function call: `invocation_id, function_name, function_hash, as_table (VARCHAR[]), distribute, across_variants (VARCHAR[])`. (`as_table`/`distribute`/`across_variants` are identity-bearing run options, hence invariant per invocation, hence stored as queryable columns not JSON — `RunOptions` in `foreach_config`; `across_variants` added 2026-09-20 so an explicitly pooled input is a recorded fact, not a prediction.) |
| `_invocation_input` | Edges call→input: `(invocation_id, param_name, input_record_id, selector)`, PK `(invocation_id, param_name, input_record_id)`. `selector` carries ColumnSelection JSON, else NULL. |
| `_invocation_output` | Edges call→output: `(invocation_id, output_num, output_record_id)`, PK `(invocation_id, output_num)`. |
| `_run` | Append-only audit, one row per `for_each` **execution**: `run_id, timestamp, user_id, function_name, where_clause`. |
| `_run_invocation` | Many-to-many run↔invocation. |

### Supporting tables

- **`_record_save`** (renamed/slimmed from the old `_record_metadata`) — the
  append-only **save-event audit log**: `(record_id, timestamp, user_id)`, PK
  `(record_id, timestamp)`. Multiple timestamps per `record_id` = the re-save
  trail. This is the **only** source of per-save recency (the "latest" variant
  collapse orders by it), because `_record` is `ON CONFLICT DO NOTHING` so its
  `created_at` is frozen at first save. Everything else a reader needs
  (variable_name = `type`, `schema_id`, `content_hash`, `schema_version`,
  `excluded`) is **joined from `_record`**.
  - The old `_record_metadata` columns `version_keys`, `branch_params`, and
    `lineage_hash` are **gone** (not written, not read).
- **`_record.excluded`** — the mutable per-record exclusion flag lives here (one
  row per record_id). `exclude_variant` / `include_variant` do
  `UPDATE _record SET excluded`. **Consequence:** because `_record` is
  `ON CONFLICT DO NOTHING`, re-saving an excluded record no longer un-excludes
  it; exclusion persists until explicitly re-included.
- **`_schema`** — schema-key combinations → `schema_id` (+ `schema_level`).
- **`_variables`** — registered variable types + their `dtype` metadata.
- **`<Type>_data`** — per-type data tables (the actual values).
- **`<Type>` view** — joins data + `_schema` + `_record.excluded` (one row per
  record_id).
- **`__scidb_schema_overrides`** — persistent schema-level exclusions.

---

## 2. Identity model (`scidb/provenance.py`)

```
constant record_id  = sha16("__constant__" | "content:" canonical_hash(value))
pathinput record_id = sha16("__pathinput__" | "spec:" PathInput.to_key())   # to_key() = the NAME only
invocation_id       = sha16(fn_hash | as_table(sorted) | distribute | sorted(bindings)
                             [| across_variants(sorted), folded only when non-empty])
                      binding = (param_name, input_record_id, selector|"")
                      — PathInput-spec edges are bindings like constants (since 2026-09-25)
save_invocation_id  = sha16("__save__" | output_record_id)   # synthetic direct-save anchor
output record_id    = generate_record_id(class | schema_version | content_hash | nested_metadata)
                      (scicanonicalhash; computed from in-memory metadata at save time)
run_id              = uuid (fresh per execution; NOT content-addressed)
```

One deliberate exclusion from `invocation_id`:

- **`where=` is NOT in the identity.** Its only effect on the computation is
  *which inputs survive*, and that surviving set already **is** the bindings.
  `where` lives on `_run.where_clause` for audit/display only.

**A PathInput IS in the identity, by its NAME** (since 2026-09-25, reversing
§11 item 6). `name=` is required; `to_key()` is the name alone and the edge
`(param, pathinput_rid)` — one record per name — is a binding exactly like a
constant edge on the save, predict and skip sides. The template and root_folder
(`to_spec()`) are stored on the record for display and re-discovery but never
hashed, so moved data keeps every id. See
`docs/claude/identity-layers-pathinput.md`.

Note the output `record_id` still comes from `generate_record_id` over in-memory
metadata, **not** rederived from `invocation_id` — variant uniqueness already
falls out because the metadata differs per variant, and the graph independently
provides structural traversal.

---

## 3. How each input kind is represented

`for_each(fn, inputs={...}, outputs=[...], **metadata_iterables, where=, as_table=, distribute=)`.
Inputs are classified per realized combo and turned into graph edges.

| Input kind | Graph representation |
|---|---|
| **Variable type** (`X: RawSignal`) | Loaded per combo; the consumed record's id → a variable input edge `(inv, "X", record_id, NULL)`. Folded into `invocation_id`. |
| **Constant** (`low_hz: 20`) | A `__constant__` record/`_constant` (content-addressed, schema-global) + edge `(inv, "low_hz", const_rid, NULL)`. Folded into identity. Surfaces in `derived_branch_params` as `fn.low_hz`. |
| **`Fixed(X, **meta)`** | Resolves to a specific upstream record (metadata override) → a real variable input edge, not a constant. |
| **`Variant(X, **branch_params)`** | Pins which variant of `X` to load (matched via graph `derived_branch_params`, suffix-aware). The pinned record becomes a normal variable input edge. |
| **`ColumnSelection(X, [cols])`** | Selecting different columns of the *same* record is a different computation, so the chosen columns go in the edge's **`selector`** (JSON `{"columns":[...]}`) and are folded into `invocation_id`. `compute_input_selectors` derives it (also through `Fixed(ColumnSelection(...))`). |
| **`Merge(A, B, …)`** | Each constituent resolves to records; all contribute input edges. `where=`/SchemaKey restrictions on a constituent apply via resolved-id selection. |
| **`ColName`** | A column-name marker resolved to a string (no variable/constant edge beyond its resolved use). `for_columns` (iterate-mode ColumnSelection) fans out per column. |
| **`PathInput(template, root_folder)`** | The per-combo resolved filepath is deliberately **not** in the graph (per-combo addressing). The **spec** (identical across combos) is a `__pathinput__` input record + edge, **folded into `invocation_id` by its NAME like a constant edge (since 2026-09-25; the spec is stored, never hashed)**, surfaced as `derived_branch_params` `fn.<param>` and via `invocation_path_inputs`. Loaded per-combo via `PerComboLoader`. |
| **`PathOutput`** | Resolution-only marker (where to write a file); excluded from constants/identity. |
| **direct-save non-schema kwarg** (`Var.save(d, subject=1, run="x")`) | Anchored on a synthetic **`__save__` invocation** (`function_name="__save__"`, keyed by record_id): each kwarg → a constant record + input edge. Surfaces as `derived_branch_params` `__save__.run`. This is the variant role the old `version_keys` column held. |
| **`as_table=[params]`** (aggregation) | Resolved aggregated param names folded into `invocation_id` (`normalize_as_table`, order-insensitive). The aggregating call consumes *all* contributing upstream records → those ids are its bindings, so the input set captures the aggregation exactly. |
| **`distribute=True`** (fan-out) | One call emits many records; each gets the next free `output_num` on the shared `invocation_id`. `distribute` is folded into identity. |

The `__save__` and `__pathinput__` records are treated like constants
(non-variable, non-sweep) in every edge-bucketing read, so they never pollute
`constants` / `branch_params` matching or "has variable inputs" checks.

---

## 4. save

### Direct save (`db.save` / `Var.save`)
Writes the data row + `_record_save` event + `_record` entity. If the save
carries **non-schema kwargs**, `record_direct_save` adds the synthetic `__save__`
invocation (each kwarg → constant + edge).

### for_each batch (`foreach.py::_save_results` → `provenance_save.record_run`)
1. for_each plans combos, loads inputs, runs the **plain** function per combo
   (scifor drives the loop; tuple returns spread across outputs; **no** function
   wrapping anymore).
2. Each output row builds `save_metadata` carrying `__fn`, `__fn_hash`
   (`compute_function_hash(fn, 16)`), `__graph_var_bindings` (the complete
   variable input-edge set incl. Fixed rids + ColumnSelection selectors),
   `__constants`, `__inputs` (incl. PathInput specs), `__as_table`,
   `__distribute`.
3. Records save via `save_batch` → `_record_save` + `_record` + data table.
4. `record_run` builds the graph from each row's `save_metadata`: constant +
   pathinput entities/edges, the `_invocation` (id via `invocation_id_for_meta`),
   input/output edges, and a fresh `_run` (+ `_run_invocation`).

### generates_file (`@scistack(generates_file=True)`)
Side-effect functions (write a file, return nothing) save **lineage-only**: a
`generated:{invocation_id}` record (no data row) + `_record_save` + `_record`
entity + graph edges via `record_run`. `invocation_id_for_meta` is the single id
source shared with `record_run`, so the generated id and the realized invocation
can't diverge.

---

## 5. load & "versioning"

Records are content-addressed and immutable. "Versioning" just means **multiple
records at the same `(variable, schema location, variant)` over time, collapsed
to the latest.**

### Variant identity (graph-derived)
A record's variant = its **producing invocation's constants** (sweep params,
incl. `__save__` kwargs), or `None` for raw records (see
`provenance_query._producing_variant_key`). Re-saves/re-runs under the same
config share a variant (newest wins); different constants/kwargs are distinct
coexisting variants. **Input record_ids are deliberately excluded from the
variant key** — re-running on changed input is the *same* variant, just newer.

### `find_record_id` / `_find_record(version_id="latest")`
1. SQL: `_record_save` joined to `_record` (type/schema/content/excluded) +
   `_schema` (schema columns); filter by `type` + schema columns; dedup to one
   row per `record_id` (newest `_record_save.timestamp`).
2. **"latest" collapse**: group by `(variable_name, schema_id, variant)` where
   variant = `(producing fn, derived_branch_params, output_num)` for computed
   records, or `("__raw__", None)` for raw — keep newest per group.
3. **Non-schema metadata + branch-param filters** match against
   `derived_branch_params` (graph), with bare-name **suffix matching**
   (`low_hz` → `bandpass.low_hz`, `run` → `__save__.run`) and list = membership.
   Ambiguous bare names raise `AmbiguousParamError`; pass the namespaced form via
   `Variant(X, fn=..., **params)` or `branch_param(fn, **params)` (the non-dotted
   escape hatch — a kwarg can't contain `.`).

### What a loaded record exposes
- `.metadata` = schema keys + direct-save kwargs (de-namespaced `__save__.*` from
  the graph). Internal `__`-markers are not exposed.
- `.branch_params` = `derived_branch_params` (accumulated upstream constants).
- `as_df` load exposes direct-save kwargs as columns + a graph-derived
  `__branch_params` column.

---

## 6. `where=` filter

`where=` carries two roles, now cleanly split (the old Strategy-1/2 string-matching
split is gone):

- **variant role** — the variable-level portion of the filter; selects *which
  variant*.
- **row role** — the SchemaKey portion (and a Merge constituent's pre-resolved
  ids); restricts *which rows*.

Matching is **semantic, by the consumed-input schema_id set** — no more brittle
string equality on `to_key()`. `_load_with_where`:

1. `S_var = var_filter.resolve()` — the schema_id set the variant filter selects
   *now*.
2. A candidate record matches the variant iff:
   - it has a producing invocation AND every schema_id that invocation
     **consumed** (`consumed_input_schema_ids`) is ⊆ `S_var` (aggregation consumed
     all of `S_var`; a per-combo output consumed one location in it); **or**
   - it has no producing invocation (raw/direct save) AND its own
     schema_id ∈ `S_var`.
3. Intersect survivors with the row restriction.

**Consequences:**
- Two filters that select the *same* inputs are the **same** variant — including
  `A & B` vs `B & A`, which now correctly match. To distinguish same-input
  variants, use a constant/branch-param (`Variant(fn=..., **params)` /
  `branch_param`), **not** `where=`.
- The stored `where_clause` string is **display-only** (surfaced via
  `get_execution_audit`); no matching logic reads it.
- The only behavior change vs the old string match: if the underlying filter
  data has since changed so the filter now selects *different* locations, the old
  stored variant won't match a load by that filter — arguably correct, and an
  accepted trade for real semantic matching.

**Named filters** are first-class values: `clean_gr = GAITRiteLoadedCycle["StepLengths_GR"] != 0`,
composable with `& | ~`, reusable in any `where=`.

---

## 7. ColumnSelection

Selecting different columns of the *same* record is a **different computation**,
so the chosen columns are stored in the edge's **`selector`** field
(`{"columns":[...]}` JSON) and **folded into `invocation_id`**.
`compute_input_selectors` derives this — also through
`Fixed(ColumnSelection(...))`. `for_columns` (iterate-mode ColumnSelection) fans
out one call per column.

---

## 8. provenance (read side — `scidb/provenance_query.py`)

Pure-SQL traversal — no JSON parsing, no heuristics:

- `producing_invocation(rid)` → `(inv_id, fn_name, fn_hash)` or None.
- `invocation_inputs(inv_id)` → `(var_inputs, constants)`.
- `derived_branch_params(rid)` — walks upward, collects constant inputs
  namespaced `function.param`. **Replaces the deleted `branch_params` column
  everywhere** (load pinning, `.branch_params`, `list_versions`, exports,
  node-state).
- `upstream_provenance(rid)` — BFS over **provably-correct stored edges**
  (replaced the old `branch_params`-subset *guessing* heuristic).
- `pipeline(rid)` — the full nodes+edges DAG. **The headline new feature:** "show
  me the entire pipeline, down to `record_id`, that produced X."
- `execution_audit(rid)` — every run that produced/reproduced a record, with
  timestamp, user, and the `where` filter as issued. Because re-runs append `_run`
  rows, a filter change (`>0.1` → `>0.05`) shows up as two audit rows; nothing is
  lost to first-wins.
- `stored_invocation_signature(rid)` → `{function_hash, var_inputs,
  const_hashes}`, used by skip_computed and node-state staleness.
- `pipeline_variants` / `pipeline_structure` / `has_producing_invocation`.

---

## 9. caching (skip_computed) & node state

### skip_computed (`foreach.py::_build_skip_hook`)
Per combo, before running:
1. **Gate** (`_find_skip_gate_record`): is there an output of `OutputCls` at this
   schema location whose **producing invocation is `fn`** with matching constant
   hashes? (Filtering by producing function is essential for self-referential
   input==output pipelines — a re-saved raw input must not be mistaken for the
   output.) No record → compute.
2. **Staleness** (`stored_invocation_signature` vs current combo): compare
   `function_hash`, each variable-input record_id (+ selector), and constant
   content hashes. Self-referential guard: input rid == output rid → stable. Any
   mismatch → recompute; else skip.

### Node state (`state.py`) — binary
**`green` | `red`** (grey/partial removed). Green iff the node has expected work
AND every expected invocation is present in `_invocation`; red otherwise (never
run, partial, input re-saved-not-rerun, or function edited — an edit shifts
`function_hash` → new expected ids → absent → red). There is **no persisted
expected set**; `expected_invocations_for_function` derives the expected
`{(invocation_id, schema_id)}` set live from current inputs.

- PathInput-only loaders: green when run (even partially), red when never run —
  un-run combos leave no trace to count as missing (accepted limitation).
  `check_pathinput_node_state` is the dedicated on-demand check that closes the
  partial-vs-complete gap by comparing schema_id sets (`PathInput.discover() ∩
  grid − exclusions` vs realized).
- `check_combo_state` (per-combo `up_to_date`/`stale`/`missing`) is a separate,
  unchanged API.

Note: `scistack-gui` still keeps its own grey-based DAG-propagation run-state
model layered on top of this binary own-state — aligning it is a pending
GUI-layer decision.

---

## 10. Key modules

- `scidb/provenance.py` — schema (DDL) + identity helpers + entity inserts.
- `scidb/provenance_save.py` — graph writers (`record_run`,
  `record_direct_save`, `invocation_id_for_meta`, `_commit_graph`).
- `scidb/provenance_query.py` — pure-SQL read side.
- `scidb/database.py` — save/load/find_record_id/save_batch/`_load_with_where` /
  variant queries.
- `scidb/foreach.py` — for_each orchestration, `_save_results`,
  `_build_skip_hook`.
- `scidb/state.py` — node-state (binary) + `check_combo_state`.
- `scidb/pipeline.py` — the `@scistack` marker (formerly `@pipeline`, replaces `@lineage_fcn`).
- `scilineage/` — reduced to `hashing.py` (`compute_function_hash`,
  `canonical_hash`).

---

## 11. Migration status & remaining TODOs

The Python-side migration is **complete and test-verified** (the user runs the
suite each round — there is no Python env in the assistant's tooling). What
remains is cross-layer work against the now-stable Python API.

### Completed (for the record)
Ordered as they were sequenced — by dependency, then risk (low→high). All
verified green on `dev-hist`.

1. **Commit the migration baseline.** Landed a clean baseline before layering
   further refactors (so a later regression is bisectable).
2. **Python-side cleanup.** Removed dead `compute_output_record_id` (function,
   `__all__`, tests), the unused `parse_version_keys` helper + `json` import, the
   stale `lineage_hashes=None` param on `save_batch` test mocks, and stale
   `version_keys`-as-stored-column comments in `_find_record` / load docstrings.
   Zero behavior change.
3. **Branch-param disambiguation API.** Implemented `Variant(X, fn="detect_spikes",
   threshold=0.5)` (namespaces params under `fn` → `"detect_spikes.threshold"`,
   matched by the existing exact-match path) and a `branch_param(fn, **params)`
   factory (exported from `scidb`) that builds the same namespaced dict to
   `**`-unpack into load kwargs. Both are sugar over the already-working
   namespaced form — no consumer change. Tests in `test_variant_pinning.py`.
4. **`where=` redesign (breaking).** (B) Semantic matching: `_load_with_where`
   matches purely by the consumed-input schema_id set (`consumed_input_schema_ids`)
   under the subset-+-raw-fallback rule (§6). `where_clause` demoted to
   display-only; `record_where_clauses`, the `__where` augmented key,
   `_where_key_from_filter`, and `_merge_constituent_where_key` **deleted**. (A)
   Role split realized through the existing SchemaKey split (variable-level portion
   = variant filter, SchemaKey portion / metadata-kwarg lists = row selector).
   `__where` removed from call_id identity and from `pipeline_variants` grouping —
   where= variants are no longer separate config variants. A dedicated `select=`
   kwarg was **not** added (out of scope this round). Tests rewritten across
   `test_variable_filter_merge.py`, `test_schema_key_filter.py`,
   `test_unified_variant_tracking.py`; new `consumed_input_schema_ids` unit tests
   in `test_provenance_read.py`.
5. **Slim & rename `_record_metadata` → `_record_save` (breaking).** Now
   `(record_id, timestamp, user_id)`; the duplicated
   `variable_name`/`schema_id`/`content_hash`/`schema_version` columns gone, and
   the mutable `excluded` flag moved to `_record`. Every reader now joins `_record`
   (`_find_record` via shared `meta_select`/`meta_from` fragments; plus
   `_create_variable_view`, `get_latest_record_id_for_variant`, `_any_records_exist`,
   the variable record-count, `filters.py` ×3, `state._get_latest_record_at_location`,
   `provenance_query._current_records_by_schema`, `provenance_save._fetch_record_meta`).
   `_save_record_metadata` → `_save_record_event`. **Behavior change:** re-saving an
   excluded record no longer un-excludes it (`_record` is `ON CONFLICT DO NOTHING`).
6. **PathInput template in identity — WON'T DO** (decided 2026-06-21). The spec
   stays **excluded** from `invocation_id`; a template change does not fork a
   variant. Rationale: its only benefit (template-as-variant) the user did not
   want, and it has a real cost — it would force the predict side
   (`_predict_config_invocations` + its feeding configs) and the skip/staleness
   side to re-add the spec to their recomputed hash in lockstep with the save
   side, widening the save-vs-predict drift surface (the exact hazard the
   no-stored-prediction design exists to avoid). A prototype across save + predict
   + skip was implemented and **reverted in full** after review. The
   `__pathinput__` edge is still stored (queryable via `invocation_path_inputs`);
   it is simply not part of identity. Reopen only with a concrete need for template
   variants.

   **REOPENED AND DONE 2026-09-25.** The concrete need: a function whose ONLY
   input is a PathInput (a library loader, `pandas.read_csv`) got one invocation
   for every file it read. Two canvas nodes reading two files merged into one
   call site in `pipeline_variants` ("1 invocation(s) -> 2 variant(s)"), its
   wiring matched neither node's dispatch claim, both graduated onto a new
   node and their edges were rewritten — the wiring "changed between runs".
   Now the spec edge is a binding (`provenance_save._pathinput_bindings`) on
   save, predict and skip alike. The binding is the PathInput's required
   `name=` (`to_key()`), never its template or root_folder, which is what
   answers the June concern: moved data and other machines keep every id.
   Tests: `scidb/tests/test_pathinput_invocation_identity.py`.

### Remaining — cross-layer migrations (#7)
Do these last, against the now-stable Python API, so each is migrated once against
final shapes. Each needs its own runtime to verify.

- **MATLAB `.m`** (needs a MATLAB env to verify):
  - `+scidb/LineageFcn.m` / `+scidb/LineageFcnResult.m` reference the removed
    `make_lineage_fcn_result` (the per-call lineage classes; likely delete).
  - `scimatlab/tests/matlab/scidb/TestForEachWhere.m` still `SELECT version_keys`.
  - Any `.m` consuming the removed `lineage_hashes` bridge output key.
  - The literal `_record_metadata` name + slimmed columns are still referenced by
    `scimatlab/.../BaseVariable.m`, `raw_sql.m` (parked from #5).
- **scidb-net** (optional networking layer): `server.py` imports `LineageRecord`,
  `__init__.py` calls `configure_backend`, `client.py` does `from scilineage import
  LineageFcnResult, extract_lineage, get_raw_value` and has its own `save_variable`
  LineageFcnResult handling — needs the network-layer analogue of the Cluster-1
  removal.
- **scistack-gui**:
  - `scistack-gui/.../variables.py` still references `_record_metadata` (parked
    from #5); a test fixture (`tests/test_project_api.py`) uses `@lineage_fcn` →
    `@scistack`.
  - Its **grey-based DAG run-state model** (`domain/run_state.py`,
    `api/pipeline.py`, frontend `FunctionNode.tsx`/`VariableNode.tsx`) sits on top
    of the now-**binary** scidb node-state. Decide: keep grey purely in the GUI
    layer (DAG propagation / pending-constant downgrades), or make the GUI binary
    too. Until then, GUI tests asserting grey-for-partial will fail.
- Also parked: `examples/`, `debug_variants.py` still reference `_record_metadata`.

### Not done (deliberately deferred, low priority)
- The optional `select=` kwarg from the `where=` redesign (#4A) — threading it
  through the whole load stack was out of scope.

---

## 12. Design-note history

### Branch-param disambiguation syntax (`.` in kwargs) — RESOLVED (option 3)
Branch params are namespaced `function.param` because `derived_branch_params`
accumulates constants up the whole ancestry, and two pipeline steps may share a
bare param name (`test_ambiguous_param_error_when_same_name_in_two_steps`). But a
kwarg name can't contain `.`, so the namespaced form could previously only be
passed via dict-unpacking (`**{"bandpass.low_hz": 30}`).

```python
Filtered.load(subject="S01", session="1", low_hz=20)                 # bare (ambiguous if shared)
Filtered.load(subject="S01", session="1", **branch_param("bandpass", low_hz=30))  # clean
Variant(X, fn="detect_spikes", threshold=0.5)                       # clean input pinning
```

The options considered were: (1) keep suffix-match with the `**{...}` escape
hatch; (2) own-constants-only matching (simplest, but drops pinning a downstream
variant by an upstream constant — a tested capability); (3) keep
accumulation/namespacing but replace the dotted-string hatch with a non-dotted
call. **Chose option 3** — bare stays the default and suffix-matches (raising
`AmbiguousParamError` on collision); `Variant(X, fn=..., **params)` and
`branch_param(fn, **params)` are the non-dotted escape hatch. No matching-logic
change was needed — both produce the already-supported namespaced exact-match key.

### `where=` redesign — RESOLVED (split roles + semantic matching)
`where=` carried two roles resolved via an internal heuristic + fragile string
matching. Two improvements were made together:

- **(A) Split the roles.** A `where=` filter is sometimes a *variant/computation
  selector* (variable-level conditions like `UAStartFoot()=='U'` — identity-bearing)
  and sometimes a *row/schema selector* (SchemaKey conditions — never provenance).
  Splitting them removes the dual-role ambiguity and the save/load key-divergence
  footgun (a SchemaKey mixed into a `for_each where=` made the stored key and the
  load-time key diverge, silently breaking the old string match).
- **(B) Semantic matching.** The semantic identity of a `where=` variant is *which
  inputs it selected*, which the graph already encodes — equivalent filters that
  select the same inputs already produce the **same** `invocation_id`. So identify
  a variant by the **set of input schema_ids it consumed**; `where_clause` becomes
  display-only.

Resolved sub-decisions:
1. **Granularity = schema_id, not record_id.** record_ids are exact but change on
   every input re-save, so a freshly-resolved filter wouldn't match. schema_ids
   are stable locations: `UAStartFoot()=='U'` resolves to the same locations
   regardless of content edits.
2. **Matching is now sensitive to the filter data's current state.** The old
   string match found a variant by its text no matter what; the new match
   re-runs the filter *now* and sees if it still picks the same locations. Normal
   case: filter data unchanged → same set → matches (and `A & B` == `B & A`). Only
   change: if the filter data has since changed so the filter now selects
   *different* locations, the old stored variant won't match — arguably correct,
   and an accepted trade.

### Function-source edit recolors a GUI node — RESOLVED
The GUI colors on the full `invocation_id` (which includes `function_hash`), so a
source edit shifts the expected ids → absent → node shows needs-run. This does
**not** contradict the "function-hash mismatch is traceability-only, not stale"
decision, because they answer different questions: an existing output whose
producing function no longer matches current source is still valid, traceable
lineage (record level — old data preserved); the node colors red because the
*current recipe* has not been run on these inputs (coverage of the current
recipe). Both hold simultaneously.

### Future enhancements (not required)
- ~~PathInput template in identity — WON'T DO~~ — DONE 2026-09-25 (see §11 item 6).
- `_record_save` **cannot be removed entirely** — it is the only source of
  per-save recency (`_record` is `ON CONFLICT DO NOTHING`, so its `created_at` is
  frozen at first save; the "latest" collapse orders by `_record_save.timestamp`).
