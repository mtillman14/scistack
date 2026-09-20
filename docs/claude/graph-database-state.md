# Graph Database — Current State & Remaining TODOs

> **⚠️ SUPERSEDED — see `database-model.md` for the canonical, up-to-date
> reference.** The data-model description, the remaining-TODO list, and the
> design-note history have all been folded into `database-model.md` (§11–§12).
> This doc is retained for its fuller as-of-2026-06-20 prose, but the canonical
> doc is authoritative.

> Branch `dev-hist`, as of 2026-06-20. This documents the data model after the
> lineage-simplification migration: a **bipartite provenance graph** is the single
> source of truth. The old `_lineage` table, `version_keys` / `branch_params`
> columns, `_for_each_expected` table, `@lineage_fcn` / `LineageFcnResult` system,
> and the rerun cache are all **gone**. Everything — variants, provenance, `where`,
> staleness, node-state, loaded metadata — is derived from the graph + `_schema`.

---

## 1. Guiding principle

The normalized graph is authoritative. Nothing is stored that duplicates or
*predicts* what the graph already encodes (those were the drift hazards we removed).
Identity is content-addressed; re-running an identical pipeline reproduces every id
(idempotent) and only appends a fresh `_run` audit row.

---

## 2. Schema

### Provenance graph (`scidb/provenance.py`)

- **`_record`** — every entity (variables, constants, PathInput specs). One
  immutable content-addressed row each (`ON CONFLICT DO NOTHING`).
  Columns: `record_id, created_at, type, schema_id, content_hash, schema_version, excluded`.
  - Variables: `type` = class name, `schema_id` set.
  - Constants: `type = '__constant__'`, `schema_id NULL`.
  - PathInput specs: `type = '__pathinput__'`, `schema_id NULL`.
- **`_constant`** — value of a constant/pathinput entity:
  `record_id, value_repr, value_type, content_hash`.
- **`_invocation`** — one row per unique function call (an *activity*):
  `invocation_id, function_name, function_hash, as_table (VARCHAR[]), distribute`.
  (`pipeline_hash` was removed with the rerun cache.)
- **`_invocation_input`** — edges call→input:
  `(invocation_id, param_name, input_record_id, selector)`, PK
  `(invocation_id, param_name, input_record_id)`. `selector` carries
  ColumnSelection JSON (else NULL).
- **`_invocation_output`** — edges call→output:
  `(invocation_id, output_num, output_record_id)`, PK `(invocation_id, output_num)`.
- **`_run`** — append-only audit, one row per for_each *execution*:
  `run_id, timestamp, user_id, function_name, where_clause`.
- **`_run_invocation`** — many-to-many run↔invocation.

### Supporting tables

- **`_record_save`** (TODO #5; was `_record_metadata`) — the append-only
  **save-event audit log**: `record_id, timestamp, user_id`, PK
  `(record_id, timestamp)`. Multiple timestamps per record_id = re-save trail. This
  is the *only* source of per-save recency (the "latest" variant collapse orders by
  it), because `_record` is inserted `ON CONFLICT DO NOTHING` so its `created_at` is
  frozen at the first save. Everything else a reader needs (variable_name=`type`,
  `schema_id`, `content_hash`, `schema_version`, `excluded`) is **joined from
  `_record`** on `record_id`.
- **`_record.excluded`** — the mutable per-record exclusion flag now lives here
  (one row per record_id), not in the save log. `exclude_variant`/`include_variant`
  do `UPDATE _record SET excluded`. Note: because `_record` is `ON CONFLICT DO
  NOTHING`, **re-saving an excluded record no longer un-excludes it** (the old
  append-a-fresh-row-with-default-FALSE behavior is gone); exclusion persists until
  explicitly re-included.
- **`_schema`** — schema-key combinations → `schema_id` (+ `schema_level`).
- **`_variables`** — registered variable types + their `dtype` metadata.
- **`<Type>_data`** — per-type data tables (the actual values).
- **`<Type>` view** — joins data + `_schema` + `_record.excluded` (one row per
  record_id; no latest-by-timestamp CTE needed).
- **`__scidb_schema_overrides`** — persistent schema-level exclusions.

---

## 3. Identity model (`scidb/provenance.py`)

```
constant record_id   = sha16("__constant__" | "content:" canonical_hash(value))
pathinput record_id  = sha16("__pathinput__" | "spec:" PathInput.to_key())
invocation_id        = sha16(fn_hash | as_table(sorted) | distribute | sorted(bindings))
                       binding = (param_name, input_record_id, selector|"")
                       — PathInput-spec edges are EXCLUDED from this hash
save_invocation_id   = sha16("__save__" | output_record_id)   # synthetic direct-save anchor
output record_id     = generate_record_id(class | schema_version | content_hash | nested_metadata)
                       (scicanonicalhash; computed from in-memory metadata at save time)
run_id               = uuid (fresh per execution; not content-addressed)
```

Key points:
- **`where` is deliberately NOT in `invocation_id`** — its only effect on the
  computation is the surviving input set, which *is* the bindings. `where` lives on
  `_run.where_clause`.
- **PathInput specs are NOT in `invocation_id`** — they are recorded as input edges
  for queryability but excluded from identity (preserves identity behavior; a
  template change does not currently fork a variant).
- Output `record_id` comes from `generate_record_id` over in-memory metadata
  (class, schema_version, content_hash, nested schema+non-schema metadata). (The
  dead `compute_output_record_id` helper has been removed — TODO #2.)

---

## 4. How each input kind is handled

`for_each(fn, inputs={...}, outputs=[...], **metadata_iterables, where=, as_table=, distribute=)`.
Inputs are classified and turned into graph edges per realized call (combo).

| Input kind | At save / graph representation |
|---|---|
| **Variable type** (`X: RawSignal`) | Loaded per combo; the consumed record's id becomes a **variable input edge** `(inv, "X", record_id, NULL)`. Folded into `invocation_id`. |
| **Constant** (`low_hz: 20`) | A `__constant__` `_record`/`_constant` (content-addressed, schema-global) + an **input edge** `(inv, "low_hz", const_rid, NULL)`. Folded into `invocation_id`. Surfaces in `derived_branch_params` as `fn.low_hz`. |
| **`Fixed(X, **meta)`** | Resolves to a specific upstream record (fixed metadata override); contributes a **variable input edge** to that record_id — a real upstream edge, not a constant. |
| **`Variant(X, **branch_params)`** | Pins which variant of `X` to load (matched via graph `derived_branch_params`, suffix-aware). The pinned record becomes a normal variable input edge. |
| **`ColumnSelection(X, [cols])`** | Selecting different columns of the *same* record is a different computation, so the chosen columns are stored in the edge's **`selector`** (JSON `{"columns":[...]}`) and folded into `invocation_id`. `compute_input_selectors` derives it (also through `Fixed(ColumnSelection(...))`). |
| **`Merge(A, B, ...)`** | Each constituent resolves to records; all contribute input edges. `where=`/SchemaKey restrictions on a constituent apply via resolved-id selection. |
| **`ColName`** | A column-name marker; resolved to a string (no graph edge as a variable/constant beyond its resolved use). `for_columns` (iterate-mode ColumnSelection) fans out per column. |
| **`PathInput(template, root_folder)`** | The per-combo *resolved filepath* is deliberately **not** in the graph (per-combo addressing). The **spec** (template+root_folder, identical across combos) is recorded as a `__pathinput__` input record + edge, **excluded from `invocation_id`**, surfaced as `derived_branch_params` `fn.<param>` and via `invocation_path_inputs`. Loaded per-combo via `PerComboLoader`. |
| **`PathOutput`** | Resolution-only marker (where to write a file); excluded from constants/identity. |
| **direct-save non-schema kwarg** (`Var.save(d, subject=1, run="x")`) | Anchored on a **synthetic `__save__` invocation** (`function_name="__save__"`, keyed by record_id): each kwarg → a constant `_record`/`_constant` + input edge. Surfaces as `derived_branch_params` `__save__.run`. This is the variant role the old `version_keys` column held. |
| **`where=`** | Filters which records load; not in `invocation_id`. Stored as `_run.where_clause` (= the filter's `to_key()`). Used at load via `record_where_clauses`. SchemaKey portions select rows, not variants. |
| **`as_table=[params]`** (aggregation) | Resolved aggregated param names; folded into `invocation_id` (`normalize_as_table`, order-insensitive). An aggregating call consumes *all* contributing upstream records → those record_ids are its input bindings, so the input set precisely captures the aggregation. |
| **`distribute=True`** (fan-out) | One call emits many records; each gets the next free `output_num` on the shared `invocation_id`. `distribute` is folded into `invocation_id`. |

The `__save__` and `__pathinput__` records are treated like constants (non-variable,
non-sweep) in every edge-bucketing read (`invocation_inputs`,
`stored_invocation_signature`, `pipeline_structure`, `realized_inputless_invocations`,
`upstream_provenance`), so they never pollute `constants` / `branch_params` matching
or "has variable inputs" checks.

---

## 5. Save workflow

### for_each batch path (`foreach.py::_save_results` → `provenance_save.record_run`)
1. for_each plans combos, loads inputs, runs the (plain) function per combo (scifor
   drives the loop; tuple returns spread across outputs; no function wrapping).
2. Each output row builds `save_metadata` carrying `__fn`, `__fn_hash`
   (`compute_function_hash(fn,16)`), `__graph_var_bindings` (the complete variable
   input edge set incl. Fixed rids + ColumnSelection selectors), `__constants`,
   `__inputs` (incl. PathInput specs), `__as_table`, `__distribute`.
3. Records save via `save_batch` → `_record_metadata` + `_record` + data table.
4. `record_run` builds the graph from each row's `save_metadata`: constant +
   pathinput entities/edges, the `_invocation` (id via `invocation_id_for_meta`),
   input/output edges, and a fresh `_run` (+ `_run_invocation`).

### Direct save (`db.save` / `Var.save`)
- Writes data + `_record_metadata` + `_record` entity. If the save carries
  non-schema kwargs, `record_direct_save` adds the synthetic `__save__` invocation.

### generates_file (`@scistack(generates_file=True)`)
- Side-effect functions (write a file, return nothing) save **lineage-only**: a
  `generated:{invocation_id}` record (no data row), `_record_metadata` + `_record`
  entity + graph edges via `record_run`. Metadata-injection passes combo schema
  keys into the function. `invocation_id_for_meta` is the single id source shared
  with `record_run`, so the generated id and the realized invocation can't diverge.

---

## 6. Load & versioning workflow

Records are **content-addressed and immutable**; "versioning" = multiple records at
the same `(variable, schema location, variant)` over time, collapsed to the latest.

### Variant identity (graph-derived)
A record's variant = its **producing invocation's constants** (sweep params, incl.
`__save__` kwargs), or `None` for raw records — see
`provenance_query._producing_variant_key`. Re-saves/re-runs under the same config
share a variant (newest wins); different constants/kwargs are distinct coexisting
variants. Input record_ids are deliberately excluded from the variant key (re-running
on changed input is the *same* variant, newer).

### `find_record_id` / `_find_record(version_id="latest")`
1. SQL: `_record_save` joined to `_record` (for type/schema/content/excluded) +
   `_schema` (for schema columns); filter by `type` + schema columns, dedup to one
   row per `record_id` (newest `_record_save.timestamp`).
2. **"latest" collapse**: group by `(variable_name, schema_id, variant)` where
   variant = `(producing fn, derived_branch_params, output_num)` for computed
   records, or `("__raw__", None)` for raw — keep newest per group.
3. **Non-schema metadata + `branch_params_filter`** are matched against
   `derived_branch_params` (graph), with bare-name **suffix matching**
   (`low_hz` → `bandpass.low_hz`, `run` → `__save__.run`) and list = membership.
   Ambiguous bare names raise `AmbiguousParamError`; the namespaced form must be
   passed via `**{"fn.param": v}` because a kwarg can't contain `.` — see the
   "branch-param disambiguation syntax" design note in §10 (leaning toward a
   `Variant(fn=..., **params)`-style API).

### `where=` load (`_load_with_where`) — semantic (TODO #4 DONE)
`where=` is split into its **variant role** (variable-level portion) and its **row
role** (SchemaKey portion / a Merge constituent's pre-resolved ids). One unified rule
(no more Strategy 1/2 split, no `where_clause` string read):

1. `S_var = var_filter.resolve()` — the schema_id set the variant filter selects *now*.
2. A candidate record matches the variant iff:
   - it has a producing invocation AND every schema_id that invocation **consumed**
     (`consumed_input_schema_ids`) is ⊆ `S_var` (aggregation consumed all of `S_var`;
     a per-combo output consumed one location in it); **or**
   - it has no producing invocation (raw/direct save) AND its own schema_id ∈ `S_var`.
3. Intersect the survivors with the row restriction (`row_ids`: SchemaKey portion /
   pre-resolved Merge ids).

The producing run's `where_clause` **string is display-only** (surfaced via
`get_execution_audit`); no matching logic reads it. Consequence: two filters that
select the *same* inputs are the **same** variant — distinguish same-input variants by
a constant/branch_param (`Variant(fn=..., **params)` / `branch_param`), not by where=.

### Loaded record exposure
- `.metadata` = schema keys + direct-save kwargs (de-namespaced `__save__.*` from
  the graph). Internal `__`-markers are no longer exposed.
- `.branch_params` = `derived_branch_params` (accumulated upstream constants).
- `as_df` load exposes direct-save kwargs as columns + a graph-derived
  `__branch_params` column.

---

## 7. skip_computed (`foreach.py::_build_skip_hook`)

Per combo, before running:
1. **Gate** (`_find_skip_gate_record`): is there an output of `OutputCls` at this
   schema location whose **producing invocation is `fn`** with matching constant
   hashes? (Filtering by producing function is essential for input==output
   self-referential pipelines — a re-saved raw input must not be mistaken for the
   output.) No record → compute.
2. **Staleness** (`stored_invocation_signature` vs current combo): compare
   `function_hash`, each variable input record_id (+ selector), and constant content
   hashes. Self-referential guard: input rid == output rid → stable. Any mismatch →
   recompute; else skip.

---

## 8. Node state (`state.py`)

**Binary: `green` | `red`** (grey removed). green iff the node has expected work AND
every expected invocation is present; red otherwise (never run, partial, input
re-saved-not-rerun, or function edited — any missing expected invocation reds it).

- Expected set (`expected_invocations_for_function`): realized inputless invocations
  (PathInput loaders) ∪ live prediction over current inputs per known variant config
  ∪ declared-inputs fallback. No persisted snapshot.
- PathInput-only loaders get **no expected set from the graph** — un-run combos leave
  no trace, so `expected_invocations_for_function` alone reads green when run (even
  partially). `check_node_state` therefore asks the filesystem as well
  (`state._discovery_gate`: `PathInput.discover()` − exclusions, TTL-cached, and
  stood down when the walk finds nothing while outputs exist), so the node a user
  sees is red when files on disk have never been loaded.
- Edited function → new `function_hash` → expected ids shift → red.
- `check_combo_state` (per-combo: `up_to_date` / `stale` / `missing`) is a separate,
  unchanged API; staleness uses the graph + a deep ancestry walk.

### `check_pathinput_node_state` — explicit outdated check for PathInput loaders
The generic node-state can't tell a partially-run PathInput loader from a complete one
(no DB-variable input enumerates what *should* exist). This dedicated check
(`state.check_pathinput_node_state(fn, outputs, inputs, db=, **iteration)`) closes that
gap **on demand** (GUI right-click / programmatic), without the #6 identity drift:

- **should-run set** = `PathInput.discover()` **∩** the `iteration` grid, minus
  `filter_excluded_combos` (the exclusion list). This is exactly what for_each would
  *produce output for* now: discovered combos restricted to the grid (empty/omitted
  grid keys are wildcards → pure-discovery mode keeps every discovered combo). A grid
  combo with no file, and a file outside the grid, are both **excluded** (and neither
  reds the node). With no PathInput at all (pure constants over a grid), `should` is
  just the grid.
- **realized**: schema locations the loader has produced under the **current constants**
  — `provenance_query.realized_inputless_schema_ids` (pure graph read; constants matched
  by content-addressed record_id, *no* invocation_id recompute).
- **red** iff any should-combo is unrealized (a new **in-grid** file appeared and hasn't
  been run); **green** otherwise. Excluding unwanted in-grid data drops it from `should`
  → back to green.
- This compares **schema_id sets**, not recomputed identity hashes — a discrepancy is an
  inspectable list of missing combos, which is why it's safe where #6 was not.

---

## 9. Variant queries (GUI-facing)

- **`list_pipeline_variants`** → `provenance_query.pipeline_variants`: distinct
  `(output_type, function_name, input_types, constants, output_num)` config-level
  variants, with `record_count`, `output_num`, and `call_id`.
- **`call_id`**: a stable for_each call-site hash — `scidb.foreach_config.CallSite`
  filled from the graph (`(__fn, __inputs, __constants, __distribute, __as_table,
  __across_variants, __glue)`), the same type the forward `ForEachConfig.to_call_id`
  fills from live inputs, so the two agree by construction. NOTE: `to_version_keys`
  still exists as an **in-memory config carrier** (it builds `save_metadata`) — it
  is NOT the removed storage column.
- **`get_aggregated_variants`** builds the GUI pipeline graph from
  `list_pipeline_variants`, parsing PathInput specs from `input_types`.

---

## 10. Remaining TODOs

### Suggested implementation order

Ordered by dependency, then risk (low→high), then value. Rationale per step.

1. **Commit the completed migration first. (DONE) ** Everything done so far is verified
   green but uncommitted on `dev-hist`. Land a clean baseline before layering new
   refactors on top — otherwise a regression in the steps below is hard to bisect.
2. **Python-side cleanup (DONE).** Removed dead `compute_output_record_id` (function,
   `__all__`, tests), the unused `parse_version_keys` helper + `json` import, the
   stale `lineage_hashes=None` param on the `save_batch` test mocks, and swept the
   clearly-stale `version_keys`-as-stored-column comments in `_find_record` / the
   load docstrings. Zero behavior change. (`_save_lineage_rows_batch` mock methods
   were left — harmless dead doubles the real DB never calls.)
3. **Branch-param disambiguation API (DONE).** Implemented **option 3**:
   `Variant(X, fn="detect_spikes", threshold=0.5)` namespaces the params under `fn`
   (→ `"detect_spikes.threshold"`, matched by the existing exact-match path), and a
   new `branch_param(fn, **params)` factory (exported from `scidb`) builds the same
   namespaced dict to `**`-unpack into load kwargs (e.g.
   `Var.load(subject="S01", **branch_param("bandpass", low_hz=30))`; non-schema
   kwargs become the branch-params filter) without a dotted-string kwarg. Both
   are sugar over the already-working namespaced form, so no consumer change was
   needed. Tests in `test_variant_pinning.py`.
4. **`where=` redesign (DONE — breaking).**
   - **(B) semantic matching.** `_load_with_where` now matches purely by the
     **consumed input schema_id set** (`consumed_input_schema_ids`) under the
     subset-+-raw-fallback rule above. `where_clause` is **demoted to display-only**;
     `record_where_clauses` (its only matching consumer) was **deleted**, as was the
     `__where` augmented key, `_where_key_from_filter`, and
     `_merge_constituent_where_key`.
   - **(A) role split.** Realized functionally through the existing SchemaKey-split:
     `where=`'s variable-level portion = variant filter, its SchemaKey portion (and
     metadata-kwarg lists) = row selector. A dedicated `select=` kwarg was **not**
     added (doc lists it as optional; threading it through the whole load stack was
     out of scope for this round). `__where` was removed from call_id identity
     (`_CALL_ID_INCLUDED_KEYS`) and from `pipeline_variants` grouping — where=
     variants are no longer separate config variants.
   - Tests rewritten: the all-pass-flag fixtures in `test_variable_filter_merge.py`
     (now distinguish variants by the `factor` constant via `branch_param`/`Variant`),
     `test_schema_key_filter.py` (`_PreresolvedFilter(variable_filter=...)`),
     `test_unified_variant_tracking.py` (reads `_run.where_clause` directly for the
     display-only assertion). New unit tests for `consumed_input_schema_ids` in
     `test_provenance_read.py`.
5. **Slim & rename `_record_metadata` → `_record_save` (DONE — breaking).** The
   table is now `(record_id, timestamp, user_id)`; the duplicated
   `variable_name`/`schema_id`/`content_hash`/`schema_version` columns are gone, and
   the mutable `excluded` flag moved to `_record`. Every reader now joins `_record`
   for those columns (`_find_record` via the shared `meta_select`/`meta_from`
   fragments; plus `_create_variable_view`, `get_latest_record_id_for_variant`,
   `_any_records_exist`, the variable record-count, `filters.py` ×3,
   `state.py::_get_latest_record_at_location`, `provenance_query._current_records_by_schema`,
   `provenance_save._fetch_record_meta`). `_save_record_metadata` → `_save_record_event`
   (slim). Tests migrated: `test_orphaned_records` (injects `_record`+`_record_save`),
   `test_integration`, `test_unified_variant_tracking`, `test_state_workflows`.
   **Behavior change:** re-saving an excluded record no longer un-excludes it
   (`_record` is `ON CONFLICT DO NOTHING`). **Parked (#7):** the literal `_record_metadata`
   name + slimmed columns still referenced by `scimatlab/.../BaseVariable.m`,
   `raw_sql.m`, `scistack-gui/.../variables.py`, `examples/`, `debug_variants.py`.
6. **PathInput template in identity — WON'T DO (decided 2026-06-21).** The PathInput
   spec stays **excluded** from `invocation_id`; changing a template does NOT fork a
   variant. Rationale: it was the one item whose only benefit (template-as-variant)
   the user did not want, and it has a real cost — it would force the **predict**
   side (`_predict_config_invocations` + the configs feeding it) and the
   **skip/staleness** side to re-add the spec to their recomputed hash in lockstep
   with the save side, widening the save-vs-predict drift surface (the exact hazard
   the no-stored-prediction design exists to avoid). A prototype across save +
   predict + skip was implemented and then **reverted in full** after review. The
   `__pathinput__` edge is still stored (queryable via `invocation_path_inputs`); it
   is simply not part of identity. Reopen only with a concrete need for template
   variants.
7. **Cross-layer migrations** (scidb-net, scistack-gui grey model, MATLAB `.m`) —
   last, against a now-stable Python API, so each is migrated once against final
   shapes. Each needs its own runtime to verify (network layer / GUI / MATLAB).

**Status:** #2, #3, #4 (A+B), #5 implemented & test-verified by the user (no Python
env here — the user runs the suite each round). **#6 won't-do** (decided — keeps
PathInput out of identity; see item 6). #7 parked (needs its own runtimes; #5
expanded its scope — see above). `select=` kwarg (optional part of #4A) not added.

### Python-side cleanup (optional, low-risk)
- **`compute_output_record_id`** (`provenance.py`) is **dead** — output ids come
  from `generate_record_id`. Remove the function, its `__all__` entry, and the
  unused `compute_save_invocation_id` import sites if any.
- Sweep remaining stale doc comments referencing the old `_lineage` table /
  "transition-era" / `version_keys` filtering (e.g. some `_find_record` internal
  comments, a couple of load docstrings).
- `test_foreach.py` mock `save_batch(..., lineage_hashes=None)` signatures carry a
  now-unused param (harmless test doubles; tidy if touching the file).
- The unused `parse_version_keys` helper in
  `scihist/tests/test_unified_variant_tracking.py`.

### Parked non-Python / cross-layer work (user-deferred)
- **MATLAB `.m`** (needs a MATLAB env to verify):
  - `+scidb/LineageFcn.m` / `+scidb/LineageFcnResult.m` reference the removed
    `make_lineage_fcn_result` (the per-call lineage classes; likely delete).
  - `scimatlab/tests/matlab/scidb/TestForEachWhere.m` still `SELECT version_keys`.
  - Any `.m` consuming the removed `lineage_hashes` bridge output key.
- **scidb-net** (optional networking layer): `server.py` imports `LineageRecord`,
  `__init__.py` calls `configure_backend`, `client.py` does
  `from scilineage import LineageFcnResult, extract_lineage, get_raw_value` and has
  its own `save_variable` LineageFcnResult handling — needs its own migration
  (the network-layer analogue of the Cluster-1 removal).
- **scistack-gui**:
  - a test fixture (`tests/test_project_api.py`) uses `@lineage_fcn` → `@scistack`.
  - its grey-based DAG run-state model (`domain/run_state.py`, `api/pipeline.py`,
    frontend `FunctionNode.tsx`/`VariableNode.tsx`) sits on top of the now-**binary**
    scidb node-state. Decide: keep grey purely in the GUI layer (DAG propagation /
    pending-constant downgrades), or make the GUI binary too. Until then, GUI tests
    asserting grey-for-partial will fail.

### Design note: branch-param disambiguation syntax (`.` in kwargs)

**Limitation.** Branch params are namespaced `function.param` (because
`derived_branch_params` accumulates constants up the whole ancestry, and two
pipeline steps may share a bare param name — see
`test_ambiguous_param_error_when_same_name_in_two_steps`). But a kwarg name can't
contain a `.` (not a valid Python identifier), so the namespaced (disambiguating)
form can only be passed via dict-unpacking:

```python
Filtered.load(subject="S01", session="1", low_hz=20)                 # bare (ambiguous if shared)
Filtered.load(subject="S01", session="1", **{"bandpass.low_hz": 30}) # namespaced — awkward
Variant(X, **{"detect_spikes.threshold": 0.5})                       # same wart for input pinning
```

Today the **bare** form is the ergonomic default and the suffix-match raises
`AmbiguousParamError` when it collides; the namespaced escape hatch is the only fix
but is syntactically painful.

**Resolution options:**
1. **Keep suffix-match** (current): bare default + `**{...}` only when ambiguous.
2. **Own-constants-only**: match a record by its own producing invocation's
   constants (bare, no namespacing, no accumulation). Simplest; **drops** pinning a
   downstream variant by an upstream constant (a tested capability).
3. **Clean disambiguation API (preferred):** keep accumulation/namespacing, but
   replace the dotted-string escape hatch with a non-dotted call, e.g.
   `Variant(X, fn="detect_spikes", threshold=0.5)` (and an equivalent for
   `load`/`branch_params_filter`, e.g. `load(..., where=branch_param("detect_spikes", threshold=0.5))`).
   Bare stays the default for the unambiguous case; the explicit `fn=` form resolves
   collisions without ever needing a `.` in a kwarg. Full capability, clean syntax.

**Leaning: option 3** (the `Variant(fn=..., **params)` style). It preserves
upstream-param pinning while removing the dict-unpacking wart.

> **IMPLEMENTED (TODO #3).** `Variant(X, fn=..., **params)` namespaces the params
> under `fn`; `branch_param(fn, **params)` builds the same namespaced dict to
> `**`-unpack into `load(...)` kwargs. Bare names stay the default and still suffix-match
> (raising `AmbiguousParamError` on collision); `fn=` / `branch_param(...)` are the
> non-dotted escape hatch. No matching-logic change was needed — both produce the
> already-supported namespaced exact-match key.

### Design note: `where=` redesign — split the two roles + semantic matching

Today `where=` carries **two distinct roles**, and resolves them via an internal
heuristic (`split_schema_key_filters`) + fragile string matching. Two improvements,
to be done together:

**(A) Split the two roles into separate concerns.**
A `where=` filter is sometimes a *variant/computation selector* and sometimes a
*row/schema selector*:
- **Variant/computation selector** (variable-level conditions, e.g.
  `UAStartFoot()=='U'`): in `for_each` it shapes which inputs are consumed → defines
  the output variant → recorded as provenance. In `load` it picks the variant that
  was computed under it. Identity-bearing.
- **Row/schema selector** (SchemaKey conditions, e.g.
  `schema_key("session").isin(["A","B"])`): just restricts which schema
  locations/rows are returned. Never provenance.

Proposal: **`where=` becomes the variant/computation filter only**; schema/row
selection moves to the metadata kwargs (already support list = "match any") plus an
optional `select=`-style kwarg for complex schema conditions (ranges, OR). Benefits:
removes the dual-role ambiguity (the user knows which they mean by which kwarg), and
eliminates the **save/load key-divergence footgun** — a SchemaKey mixed into a
`for_each where=` makes the stored key (full filter) and load-time key
(variable-level portion) diverge, silently breaking Strategy-1 matching. The
codebase is already moving SchemaKey out of `for_each where=` for this reason.

**(B) Make variant matching semantic (consumed input schema_id set), not string.**
The current load Strategy 1 matches a record's stored `where_clause` by **string
equality** of `filter.to_key()`. That's brittle: a semantically-identical but
textually-different filter (`A & B` vs `B & A`, formatting, equivalent
constructions) won't match.

The semantic identity of a `where=` variant is *which inputs it selected*. The graph
already encodes this — an aggregation's invocation consumes all surviving input
records (its `_invocation_input` edges, folded into `invocation_id`), so equivalent
filters that select the same inputs already produce the **same invocation_id**. The
gap is purely the load-side mapping `where=` → invocation.

Proposal: identify a variant by the **set of input *schema_ids* it consumed**
(hash it for a fast keyed lookup; the hash is a denormalization of the input edges →
their schema_ids). Load then does: `where.resolve()` → schema_id set → hash → match
the invocation that consumed that set. `where_clause` is **demoted to a
human-readable reference column that no logic reads**.

Resolved design decisions:
1. **Granularity = schema_id, not record_id.** record_ids are exact (and already in
   `invocation_id`) but change on every input re-save, so a freshly-resolved filter
   wouldn't match the consumed set — too brittle. schema_ids are stable locations:
   `UAStartFoot()=='U'` resolves to the same locations regardless of content edits.
2. **Accept that matching is now sensitive to the filter data's current state.**
   Plain-English version: the old string match would find your variant by its
   *name/text* no matter what. The new match finds it by *re-running the filter now*
   and seeing if it still picks the same locations. In the normal case the filter
   data is unchanged, so it resolves to the same set and matches — and it now
   correctly treats `A & B` and `B & A` as the same. The only behavior change: if the
   underlying filter-variable data has since changed so the filter now selects
   *different* locations, the old stored variant won't match a load by that filter.
   That's arguably correct (the variant no longer corresponds to what the filter
   means today), and it's an accepted trade for getting real semantic matching.

**Bonus:** (A)+(B) let load Strategy 1 and Strategy 2 **collapse into one mechanism**
— resolve `where=` → schema_id set → match invocations that consumed it; directly-
saved data (no producing invocation) naturally falls out as the select-by-schema_id
case. `where_clause` survives only as display/audit.

### Possible future enhancements (not required)
- **PathInput template in identity — WON'T DO** (decided 2026-06-21; see §10 item 6).
  The spec stays out of `invocation_id`; a template change does not fork a variant.
  Reopen only with a concrete need (and accept the save-vs-predict drift surface it
  adds).
- **`_record_save` (was `_record_metadata`) — DONE (#5):** slimmed to
  `(record_id, timestamp, user_id)` + `excluded` moved to `_record`; readers join
  `_record`. The table **cannot be removed entirely** — it is the only source of
  per-save recency (`_record` is `ON CONFLICT DO NOTHING`, so its `created_at` is
  frozen at first save; the "latest" collapse orders by `_record_save.timestamp`).
- **Commit**: all of this migration work is on the `dev-hist` working tree,
  uncommitted.

---

## 11. Key modules

- `scidb/provenance.py` — schema (DDL) + identity helpers + entity inserts.
- `scidb/provenance_save.py` — graph writers (`record_run`, `record_direct_save`,
  `invocation_id_for_meta`, `_commit_graph`).
- `scidb/provenance_query.py` — pure-SQL read side (`derived_branch_params`,
  `invocation_inputs`, `stored_invocation_signature`, `producing_invocation`,
  `pipeline_variants`, `record_where_clauses`, `upstream_provenance`,
  `expected_invocations_for_function`, `_producing_variant_key`, …).
- `scidb/database.py` — save/load/find_record_id/save_batch/`_load_with_where`/
  variant queries.
- `scidb/foreach.py` — for_each orchestration, `_save_results`, `_build_skip_hook`.
- `scidb/state.py` — node-state (binary) + `check_combo_state`.
- `scidb/pipeline.py` — the `@scistack` marker (formerly `@pipeline`, replaces `@lineage_fcn`).
- `scilineage/` — reduced to `hashing.py` (`compute_function_hash`, `canonical_hash`).
