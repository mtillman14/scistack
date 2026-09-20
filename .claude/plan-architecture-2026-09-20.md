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

* Introduce `RunState` (typed; the `state` object already half exists) and
  extract `_prepare(...)`, `_execute(...)`, `_save(...)` from the 975-line
  body. Pure signature moves first — no behaviour change — one commit.
* `provenance_save.row_input_edges(row, rid_keys, fixed_rids,
  combo_to_rids, selectors) -> list[(param, rid, selector)]`: the ONE
  function that turns a result row into its input edges, used by both the
  full-iteration and the aggregation paths. `__graph_var_bindings` is then
  always set; the `__upstream` fallback in `_variable_bindings` becomes
  dead and is removed.
* **Guard:** `compute_invocation_id` must produce identical ids before and
  after for every shape in Stage 1's parity suite — run it first, run it
  last. `check_selector_round_trip` must never fire.
* `_save_results`'s 17 parameters collapse to `(result_tbl, outputs,
  state)`.

Verification: the whole scidb suite (this touches everything), one package
at a time; `tests/integration`.

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
