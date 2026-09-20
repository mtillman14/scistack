# Plan — architecture follow-through (items 1–6 of the 2026-09-20 review)

Reference: `docs/claude/architecture-review-2026-09-20.md`. Each stage is
its own branch or its own run of commits on a refactor branch; none depends
on another except where stated. Every stage ends green on the suites it
names, and the user runs them.

## Stage 1 — One owner for call-site identity, and parity tests

* Move the wiring recipe into scidb: `scidb.provenance_query.wiring_id(fn,
  input_types, out_types, path_inputs)` — `call_id` minus constants — with
  the `strip_path_input_params` normalisation INSIDE it. `graph_builder.
  wiring_id` becomes an import.
* `scidb/tests/test_identity_parity.py`: for each of `invocation_id`,
  `call_id`, `wiring_id`, and the selector, run a real `for_each`, then
  reconstruct the id from provenance (`config_call_id`, the new wiring
  recipe, `stored_invocation_signature`) and assert equality. Cover: plain
  input, column selection, `for_columns`, aggregation, PathInput-fed, EachOf,
  distribute, as_table.
* GUI: `scistack-gui/tests/test_binding_identity.py` grows the same
  assertion against `variant_resolver.compute_call_id`.

Verification: `scidb/tests/test_identity_parity.py`, the two GUI identity
suites, `tests/integration/test_dag_runs.py`.

## Stage 2 — `for_each`: three phases, one row→edges assembly

*(rewritten 2026-09-20 after the parity suite's first two runs; the
"first decision" is done — aggregation edges carry the real parameter
name, `Fixed` is the edge — and the typed spine below is what the phase
extraction is built on)*

### 2a — The typed spine: how a rid travels (do this first)

Today a record id is carried through `foreach.py` as `__record_id` on a
frame, `__rid_{param}` on a combo, `__rid_{param}_{i}` in `__upstream`, a
bare string in the skip gate, and a `(param, rid, selector)` tuple in the
graph — across SIXTEEN differently-named containers (`rid_keys`,
`rid_to_bp`, `combo_to_rids`, `fixed_rid_values`, `lineage_fixed_rids`,
…), each keyed by one of those spellings with nothing saying which. The
`Fixed`-on-aggregation edge was missing for as long as the graph existed
because the rid sat in one container (bare keys) while the save read
another (prefixed keys, a different source) and no type told them apart.

New leaf module `scidb/src/scidb/bindings.py` (no scidb imports; scifor
may import it later for the MATLAB bridge):

* `RID_PREFIX = "__rid_"`, `rid_column(param) -> str`,
  `param_of(column) -> str` — the ONLY two places the prefix is spelled.
  The 15 hand-rolled `key[len("__rid_"):]` slices become calls.
* `InputKind` — `ITERATE` (a plain variable: every record at a location
  is its own call), `PINNED` (`Fixed`: one rid, injected everywhere),
  `LINEAGE_ONLY` (`ColumnSelection`: prunes, never expands),
  `AGGREGATED` (records below the iterated level, pooled) — decided ONCE
  at Step 12 and carried, so the save, the skip gate and the predictor
  read a field instead of each re-deriving "is this Fixed?" with
  `isinstance` + `hasattr("fixed_metadata")` (the predictor re-derives it
  a fourth way today, from schema levels).
* `Binding(param, rid, selector)` — a frozen dataclass replacing the
  two-arity tuple `compute_invocation_id` accepts; `compute_invocation_id`
  takes `Iterable[Binding]` and the tuple form is gone.
* `InputBinding(param, kind, type_name, selector, rids: tuple[str, ...],
  rid_to_bp)` — one input's whole story after Step 12.
* `RunBindings` — `{param: InputBinding}` plus the per-combo view
  (`for_combo(combo) -> list[Binding]`), which is the ONE row→edges
  assembly: full iteration reads `__rid_*` off the combo, aggregation
  reads the pooled set, pinned rids are appended, and NOTHING else builds
  an edge list. `__graph_var_bindings` is written from it;
  `_variable_bindings`' `__upstream` fallback goes.

**Guard:** `test_identity_parity.py` before and after every commit of
2a; `compute_invocation_id` must produce identical ids for every shape
(the spelling of a `Binding` is the same bytes as the tuple).

### 2b — Three phases over a typed `RunState`

* `RunState` gains `bindings: RunBindings` and the sixteen containers
  are retired one at a time (each retirement is a commit; each ends
  green on the parity suite and `test_aggregation*`).
* Extract `_prepare(...)`, `_execute(...)`, `_save(...)` from the 975-line
  body — pure signature moves first, no behaviour change, one commit.
* `_save_results`'s 17 parameters collapse to
  `(result_tbl, outputs, state)`.

### 2c — What stays stringly-typed, and why

A newtype cannot reach inside a pandas column name, so `__rid_x` on a
frame stays a string at the scifor boundary. 2a makes the boundary ONE
place (`rid_column` / `param_of`) instead of fifteen; moving the binding
off the frame entirely — scifor's filter asking `RunBindings.for_combo`
rather than reading `__rid_*` columns — is the last step, and it is what
retires the column convention for good. Do it after 2b, when there is
one consumer left.

Verification: the whole scidb suite, one file at a time; `tests/integration`.

## Stage 3 — Import cycles

* `python -X importtime`-free approach, since Python is not available here:
  a script the user runs that imports every module in `scidb`, `scifor`,
  `scistack_gui` at top level and prints failures; then a test that does
  the same (`scidb/tests/test_imports.py`, GUI likewise).
* Break each cycle by moving the shared type/helper down (candidates seen
  this week: `Log` accessor, `ColumnSelection` normalisers → `scidb.intent`;
  `_duck` accessor; `parse_fn_node_id`/`strip_placement` → a leaf
  `scistack_gui.ids` module). Hoist imports. Mechanical; many small commits.

Verification: the import tests plus every suite.

## Stage 4 — One handler table for both GUI transports

* `scistack_gui/api/registry.py`: `HANDLERS = [Handler(name, params_model,
  service_fn, holds_db_lock: bool), ...]`. `server.py` builds its `_h_*`
  dispatch from it; `api/*.py` builds its routes from it; the "does not
  hold the lock" list is the `holds_db_lock` field.
* A test asserts every `Handler` is reachable through both transports and
  that no `_h_*` or `@router` exists outside the table.
* Migrate handler by handler; `plot_*` first (newest, smallest).

Verification: `scistack-gui/tests/test_api.py`, the RPC tests.

## Stage 5 — Rules into types

* `scistack_gui/ids.py`: `BareNodeId(str)` / `PlacedNodeId(str)` newtypes
  with `strip()` / `place(scope)`; `parse_placement_id` returns them. Any
  function that today calls `strip_placement` defensively takes a
  `BareNodeId` instead — the trap becomes a type error under the LSP.
* Assertions at seams for: indexed binding names (fold on read only),
  ColumnSelection never in `rid_keys`, `schema_keys` `[]` vs `None`.
* `docs/claude`: move superseded narratives to `docs/claude/archive/`;
  keep conceptual references + write one-page ADRs for the decisions this
  week made (intent/fact, schema_keys spelling, origin).

Verification: the GUI suite; no behaviour change.

## Stage 6 — Finish scope-awareness (decision first)

Ask: is a run scoped to the hypothesis it was started in?

* **If yes:** `pipeline_id` travels with `RunRequest` → derivation →
  `default_schema_level` → `resolve(scope=)`; `intent_store` writes rows at
  that scope (`set_column_selections` etc. gain `scope=`); hidden lookups
  stop fail-opening; duplication uses `copy_scope`. Migration: existing
  `global` rows stay global (they applied everywhere before).
* **If no:** drop `scope` from resolution (keep the column, always
  `global`), delete the "deferred follow-up" docstrings, and say so in
  `intent-and-fact.md`.

Verification: `test_intent_store.py`, `test_api.py` hypothesis tests,
`tests/integration/test_dag_runs.py`.

## Not in this plan

Items 7 (MATLAB as a bridge), 8 (`DatabaseManager` split), 9 (frontend), 10
(migrations) — each its own plan when taken up.

---

## Status

### Stage 1 — built 2026-09-20, **green** (106 passed, 5 pinned xfails in the parity suite; user-run)

* `scidb.provenance.compute_wiring_id` / `parse_path_input_spec` /
  `strip_path_input_specs` are the owners; `graph_builder.wiring_id`,
  `parse_path_input`, `strip_path_input_params` are re-exports, and
  `scidb.inspect.graph` + `scidb.database` dropped their copies (THREE
  copies of the PathInput spec parser existed).
* Writing the call_id parity test exposed a latent bug: the forward
  `ForEachConfig.to_call_id` folded `ColumnSelection.to_key()` (the columns)
  into `__inputs` while the backward `config_call_id` uses the bare type —
  so `check_node_state(call_id=manifest["call_id"])` never matched a
  column-selected pipeline step, which planned red forever. Fixed:
  `to_call_id` hashes `call_site_inputs()` (a ColumnSelection is the type it
  wraps); `to_version_keys` is unchanged so a column change still re-runs.
* **Decision needed (pinned as `xfail(strict=True)` in
  `test_identity_parity.py::TestCallIdForwardEqualsBackward::test_fixed`):**
  `Fixed(subject=1)` vs `Fixed(subject=2)` forks the forward call_id
  (`test_unified_modifier_classes::test_different_fixed_metadata_forks_call_id`)
  but the backward id collapses them (the pinned record is an edge, not a
  config). One call site or two? The canvas says one.
* Not unified: `scidb.inspect.graph._step_id` has its OWN wiring key for
  report step ids (a fourth identity, display-only). Noted, not touched.
* **First run of the suite found the aggregation vocabulary split** (four
  more `xfail(strict=True)`, marker `INDEXED_AGGREGATION_EDGES`): a combo
  that consumes several records of one parameter records its edges under
  INDEXED names (`value_0`, `value_1`), nothing in scidb folds them on read,
  so every backward reconstruction — `pipeline_variants`,
  `function_variant_configs`/`config_call_id`, the recorded selectors —
  names parameters the forward call does not have. Consequences today: a
  pipeline step that aggregates (`as_table`, any pooling) can never plan
  green through `check_node_state(call_id=...)`; the GUI folds on its run
  path only. Save-time folding was tried and reverted on 2026-09-19 (the
  skip predictor assumed indexed names). **This is Stage 2's first
  decision**: edges should carry the real parameter name with several
  edges per parameter (the PK already allows it), the predictor enumerates
  N bindings per parameter, and every aggregation `invocation_id` migrates
  once.

Verify: `scidb/tests/test_identity_parity.py`, `test_unified_modifier_classes.py`,
`test_parameter.py`, `test_state*.py`; GUI `test_binding_identity.py`,
`test_column_selection_binding.py`, `test_pipeline_call_sites.py`, `test_graph_builder.py`;
`tests/integration/test_dag_runs.py`.

### Stage 2, first decision — built 2026-09-20, tests unrun

Both decisions taken with the user (2026-09-20):

1. **Aggregation edges carry the real parameter name**, several edges per
   parameter. `_save_results` writes `__graph_var_bindings` for every
   aggregation row (unconditionally now); `_variable_bindings`' `__upstream`
   fallback folds index groups (legacy metas only); `stored_invocation_
   signature.var_inputs` is `param -> [(rid, selector), ...]` and both
   consumers (`_should_skip`, `_find_skip_gate_record`) read the list — the
   thing that made the 2026-09-19 fold attempt break skip_computed.
   `_predict_config_invocations` binds by iteration level:
   `function_variant_configs` now attaches `iterated_keys` (read off each
   config's outputs, `iterated_keys_for_invocations`) and the predictor pools
   below-level inputs into one edge set, cross-products at-level ones, and
   broadcasts coarser ones. Known gap, documented in the predictor: a call
   that auto-splits pooled records by variant group writes one invocation
   per group; the predictor pools them into one and such a config reads as
   missing. **Every aggregation `invocation_id` (and its record ids) moves
   once; a re-run supersedes.** The GUI's `_fold_indexed_params` is now a
   legacy-row repair.
2. **A `Fixed` pin is the edge, not the call site.** `call_site_inputs`
   unwraps `Fixed` (and `ColumnSelection`) to the type; the old
   forks-call_id test is inverted with the reason; version keys still fork.

Parity suite: the five pins are gone; two consumer tests added (an
aggregating step plans green through `check_node_state`; an aggregating
re-run skips every combo).

Verify: the WHOLE scidb suite one file at a time is the honest ask here —
this touches identity for every aggregation — but at minimum:
`test_identity_parity.py`, `test_aggregation.py`,
`test_aggregation_with_variants.py`, `test_stat_leaves.py`,
`test_column_selection_lineage.py`, `test_unified_modifier_classes.py`,
`test_variant_pin_node_state.py`, `test_for_each_caching*.py`,
`test_glue_identity.py`; GUI `test_execution_service.py`, `test_api.py`,
`test_pipeline_call_sites.py`; `tests/integration` (all three files).

### Stage 2, first decision — **green** 2026-09-20 (user-run)

Three gaps closed on the way: a partially pinned `Fixed` input recorded
no edge (one-row check → pin applied at Step 12), `{}` was not `None`
(save fallback), and the fallback's `db` was `None` under the global
database (Step 12's `fixed_rid_values` now reaches the save). Stage 2a
(the typed spine) is next; nothing of it is built yet.

### Stage 2a — built 2026-09-20, tests unrun

`scidb/bindings.py` (c69f1b5f): the prefix has one owner, 15 slices and
every `f"__rid_{...}"` replaced. `Binding` is the graph edge (a62c7b12):
`compute_invocation_id`, `_variable_bindings`, `record_run` and
`invocation_id_for_meta` speak it (tuples still coerce at the metadata
boundary — same bytes, pinned by `test_bindings.py`). `RunBindings` is
built once at the end of prepare (`_build_run_bindings`: kind decided from
what Step 12 sorted each input into) and carried as `state.bindings`; the
save path writes `__graph_var_bindings` from `RunBindings.for_combo` in
BOTH modes — the two hand-written assemblies (full-iteration rows vs the
aggregation block) are gone. `__upstream` remains as the dict-shaped view
older readers expect. `_save_results` gained a `run_bindings=` kwarg; the
17→3 parameter collapse is 2b.

Verify: `test_bindings.py`, `test_identity_parity.py`, `test_aggregation*.py`,
`test_column_selection_lineage.py`, `test_glue_identity.py`,
`test_variant_pin_node_state.py`, `test_selector_round_trip.py`;
`tests/integration`.
