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

### `__upstream` removed — built 2026-09-20 (d6920016), tests unrun

A record's identity is its invocation: `invocation_identity(meta, bindings)`
is the one recipe, `__invocation_id` is a version key, `record_run` refuses
a graph that disagrees with the stamped id ("identity drift"). `GraphRecord`
carries typed `bindings`. Verify: `test_provenance_identity.py`,
`test_provenance_graph.py`, `test_identity_parity.py`, `test_bindings.py`,
then the whole scidb suite one file at a time, then `tests/integration`.

### 2a, variant groups — built 2026-09-20, tests unrun

`__vsig_{param}` was the last stringly-typed rid-shaped thing: the
aggregation auto-split's variant-group signature (JSON of a record's
derived branch params) riding on the frame and the combo the way a rid
does, with `_combo_to_rids` — a dict keyed by "iterated keys + `__vsig_*`
values", rebuilt with the same tuple recipe at FOUR readers (two in
`_save_results`, the draft endpoint stamp, the skip gate) — as the only
link from a combo to its consumed records. Now in `scidb.bindings`:

* `variant_signature(bp)` / `EMPTY_SIGNATURE` / `vsig_column` /
  `param_of_vsig` / `is_internal_column` / `signature_conflicts_with`
  (`__save__.<key>` alignment) / `merge_branch_params` — the spellings and
  the recipe, one owner. `variant_signature` normalises through one JSON
  round trip so the save path (branch params read back from a JSON
  column) and the predictor (branch params straight from the graph) sign
  identically.
* `VariantGroup(signature, rids)` and `RecordPool(location_keys,
  groups_by_location, split)` on `InputBinding.pool` for every
  `AGGREGATED` input. The pool is keyed by the iterated keys the input
  POPULATES, so an input coarser than the iterated level is found beneath
  its location (the old `_sig_rids_by_combo[c][ck]` lookup compared the
  full iterated tuple and matched nothing — no edge; regression test
  `test_coarse_input_provenance.py::test_input_coarser_than_the_iterated_level_records_its_edge`).
* `RunBindings.rids_for_combo(combo)` answers "what did this call consume"
  in every mode (pool by location + `__vsig_` group / pooled / `__rid_*`
  off the combo / pinned); `for_combo` types it; `branch_params_for`
  merges; `pin` binds a late-resolved Fixed rid. `combo_to_rids`,
  `iterated_keys_ordered`, `lineage_fixed_rids` (a `for_each` kwarg
  nothing passed) and `input_selectors` are gone; the hook holder is
  `{"bindings": RunBindings}`; `_save_results` is
  `(result_tbl, outputs, output_names, config_keys, db, run_bindings, *, …)`
  (17 → 6 + 5 keyword; the rest is 2b's `state`). `RunBindings` is built
  at Step 13, before the skip hook (its first reader).
* The predictor's "variant auto-split" gap is closed:
  `_predict_config_invocations` groups an aggregated input's current
  records by `variant_signature(branch_params_batch(...))`, one predicted
  invocation per group, Cartesian across split inputs, `__save__`
  alignment applied; a ColumnSelection input pools (as the save side
  does). Remaining gap, documented in the docstring: `AcrossVariants` is
  not in the graph, so an explicitly pooled input is predicted split
  (conservative). `test_identity_parity.py::TestVariantSplitIsPredicted`
  (5 tests: 8 invocations written, predicted == written, plans green,
  rerun skips 8/8, ids rebuild).
* MATLAB bridge: `is_internal_column` / `rid_column` from `scidb.bindings`;
  `lineage_fixed_rids=None` dropped from its save call. scifor's own
  `"__rid_" not in k and "__vsig_" not in k` stays (scifor cannot import
  scidb; that boundary is 2c).

Verify (one package at a time):
`cd /workspace/scidb && pytest tests/test_bindings.py tests/test_identity_parity.py tests/test_coarse_input_provenance.py tests/test_aggregation.py tests/test_aggregation_with_variants.py tests/test_provenance_identity.py tests/test_provenance_graph.py tests/test_glue_identity.py tests/test_stat_leaves.py tests/test_column_selection_lineage.py -q`
then `cd /workspace/scidb && pytest tests/ -q -x`, then
`cd /workspace/tests/integration && pytest -q`, then
`cd /workspace/scimatlab && pytest tests/ -q -x`.

### AcrossVariants is a fact — built 2026-09-20, tests unrun

The "remaining gap" above is closed: pooling is a recorded run option, not
a call-site wrapper the graph forgets. `ForEachConfig.across_variants` →
`__across_variants` version key (in `_CALL_ID_INCLUDED_KEYS`;
`call_site_inputs` unwraps the wrapper to the type, as for Fixed /
ColumnSelection) → `record_run` stores `_invocation.across_variants` and
`compute_invocation_id(..., across_variants=)` folds it in only when
non-empty (every pre-existing id unchanged; `run_options_label` shows it)
→ `function_variant_configs` / `pipeline_variants` / `config_call_id` /
`config_from_inputs` carry `across_variants` → the predictor pools those
params. GUI: `variable_binding(pool_variants=)` set by
`_attach_db_bindings` from the config, wrapped by `build_run_inputs`
(`_apply_pooling`), rendered by `variable_inputs_view` /
`_variable_binding_parts` (4-tuple) / `_format_variable_class`
(`scidb.AcrossVariants(...)`), both code exports (`_pooled_inner`), and
`variant_resolver.compute_call_id` (`__across_variants`). Parity:
`TestAcrossVariantsIsAFact` (6 tests). Verify with the Stage 2a list plus
`cd /workspace/scistack-gui && pytest tests/test_code_export.py tests/test_entity_round_trip.py tests/test_matlab.py tests/test_execution_service.py -q`.

Noted for Stage 3: `variant_resolver.compute_call_id` is a THIRD spelling
of the call-id recipe (after `ForEachConfig.to_call_id` and
`config_call_id`); every run option added now touches all three.

### CallSite — the one assembly of the call-id payload — built 2026-09-20, tests unrun

The "third spelling" noted above was really a FOURTH (`pipeline_variants`
had an inline copy too); only the hash was shared. `scidb.foreach_config.
CallSite(fn_name, inputs, constants, distribute, as_table, across_variants,
glue)` now owns `version_keys()` and `call_id`; `ForEachConfig.call_site()`,
`config_call_id`, `pipeline_variants` and the GUI's `compute_call_id` map
their shape onto it and spell no rule. `ForEachConfig.to_version_keys` is
the CallSite keys plus the version-only ones (`__inputs` with wrappers,
`__fn_hash`, `__glue_hashes`, `__where`). The hash is private
(`_hash_call_site`). Two drifts this removed: `as_table=True` hashed as the
literal `True` forward and as the resolved names backward (that call site
never matched its records — `test_as_table_true`), and the GUI id left glue
chains out. GUI tests now compare against `ForEachConfig.to_call_id`, the
real forward id, instead of a hand-built payload.

---

## The duplicated-recipe sweep — built 2026-09-20, tests unrun

Answering "are there other duplicated recipes?" turned up five, plus three
live defects they were causing. Doc first: `docs/claude/variant-space.md`
(the coordinate/selector distinction, the three axes, and which of the four
"which variant is this?" questions each function answers).

**1. Canonical variant signature.** `bindings.variant_signature` had four
hand-rolled twins — `database.py`'s collapse key, `foreach.py`'s PathOutput
`{variant}` text (×2), the GUI's variant-summary grouping — and they had
drifted: `{variant}` digested `json.dumps(merged_bp)` under full iteration
and `"|".join(signatures)` under aggregation, so ONE template wrote to two
directories for the same group (`test_pathoutput_variants.py::
TestVariantTokenIsOneDigest`). The recipe also simplified: the JSON round
trip was a no-op (a tuple and a list both emit `[1, 2]`), so it is one
`dumps` with `default=str` — cheaper on the collapse's hot path and no
longer raises on a datetime. `merge_branch_params` absorbed
`foreach._merge_group_bp` and now returns `{key: [every value seen]}`, which
serves both callers.

**2. Bare-name suffix match.** `variant.match_bare_name` — exact first, then
`.{name}` suffix, ambiguity raises naming the candidates. Was spelled in
`database._match_branch_param` (load filtering) and
`foreach._resolve_bp_placeholder` (PathOutput placeholders).

**3. Unwrap spec → variable type.** NEW leaf module `scidb/input_spec.py`
(`peel` / `variable_type` / `type_name` / `find_wrapper` / `wrappers_of`),
driven by one wrapper table. Six copies before, each knowing a different
subset — which is not tidiness: `config_from_inputs` did not know about
`Variant`, so a pinned input vanished from the predicted config, and
`compute_input_selectors` enumerated two stackings by hand, so a column
selection under a `Variant` reached the graph as "no selection". Both
closed; `test_variant_pin_node_state.py`'s xfail reason now names only the
half that remains (narrowing the PREDICTION, a design question — the pin is
a load-time filter like `where=`, whose effect is already on the edges).

**4. Variant space is typed.** `variant.VariantAxes` splits a branch_params
dict into `constants` / `code` / `run` once, instead of in the loader's
three-way split, the code filter, the run filter and the plotting layer's
column mapping. The two filters now take `{fn_name: value}` and never see a
prefix.

**5. `RunOptions`.** `distribute` / `as_table` / `across_variants` as one
value (`foreach_config`), held by `CallSite`, built from a node config with
`from_config`, resolving `as_table=True` against the call's own params.
Fixes two live defects: `build_backend_pipeline` and `_matlab_steps` each
hardcoded `distribute=False, as_table=None` beside a node config they were
already reading, so a compiled pipeline and an exported script ran every
step with defaults AND filtered hidden combos against an id no record would
carry. Both code exporters now emit the options too.

Verify (one package at a time):
`cd /workspace/scidb && pytest tests/ -q -x`
`cd /workspace/scistack-gui && pytest tests/ -q -x`
`cd /workspace/scistackplotdb && pytest tests/ -q`
`cd /workspace/tests/integration && pytest -q`

---

## Status 2026-09-20 (end of session): **green on all four suites**

`scidb`, `scistack-gui` (2002 passed), `scimatlab`, `tests/integration` — all
user-run, all passing, on `refactor/intent-and-fact`.

Done: Stage 1, Stage 2a (typed rid spine + variant groups + `__upstream`
removed + `AcrossVariants` as a fact), `CallSite`, and the
duplicated-recipe sweep (signature / bare name / unwrap / variant axes /
run options).

Open, in the order the plan lists them:

* **2b** — `RunState` retires the remaining containers; extract
  `_prepare` / `_execute` / `_save`; `_save_results` to
  `(result_tbl, outputs, state)`.
* **2c** — scifor asks `RunBindings` instead of reading `__rid_*` /
  `__vsig_*` columns; retires the column convention. scifor cannot import
  scidb, so this is the layering question, not a rename.
* **3** import cycles — the lazy `from .foreach import _is_loadable`
  inside functions is the symptom; `scidb/input_spec.py` is the shape of
  the answer (a near-leaf that both sides may import at the top).
* **4** one handler table for both GUI transports.
* **5** types for placement ids.
* **6** scope-awareness (decision first).

Also open, deliberately not done here:

* The **Variant-pin** node-state xfail keeps only its design half: the pin
  is a load-time filter like `where=`, whose effect is already on the
  edges, so narrowing the PREDICTION cannot come from recording it as a
  fact. Stage 4 of `.claude/plan-variant-selection.md`.
* `variant_resolver.compute_call_id` still exists as a GUI-side entry
  point, but it now only maps bindings onto `CallSite` — no recipe of its
  own.
* The six superseded GUI tables are still in place (drop only after the
  import has been seen complete on a real database).
* GUI never visually checked: `docs/gui-manual-testing-todo.md` items 0b
  and 0a. Branch unmerged.

### Stage 2b — built 2026-09-20 (550d700e), tests unrun

* `_save_results(result_tbl, outputs, state, db)` — from seventeen
  parameters. `generates_file` / `endpoint_kind` moved onto the state (set
  at prepare from both entry points; the MATLAB bridge's second RPC reads
  them off the cached state instead of re-sending).
* Retired from `_ForEachState`: `rid_to_bp` → `bindings.rid_to_bp`,
  `fixed_rid_values` → `bindings.pinned_rids`, `rid_keys` →
  `bindings.tracked_columns` (ITERATE ∪ AGGREGATED). `rid_keys_for_schema`
  stays — a fact about the scifor call, not the inputs. `state.bindings` is
  never None.
* `_for_each_execute(state, ...)` extracted — Steps 16-17 (fn wrapping,
  scifor delegation, run summary). Pure signature move; every name passed
  explicitly. `for_each`'s body: 630 → 465 lines.

**Deliberately not done:** extracting the ~220-line pre-prepare setup
block (where/db/EachOf/endpoint/for_columns/skip-hook). It is linear
normalisation, not duplication; it early-returns through the EachOf
recursion; and a pure move would hand back a ten-field tuple. If it is
ever extracted it should come back as a `CallSetup` value — a design
step, not a signature move.

Verify: `cd /workspace/scidb && pytest tests/ -q`, then
`cd /workspace/scimatlab && pytest tests/ -q` (the bridge reads
`bindings.tracked_columns` / `pinned_rids` for its rename map and passes
`endpoint_kind` at prepare), then `cd /workspace/tests/integration && pytest -q`.

Stage 2b: **green on all four suites 2026-09-20** (user-run).

### Stage 2c — built 2026-09-20, tests unrun (`.claude/plan-2c-row-selection-seam.md`)

The selection is a value. `scidb.bindings.Selection` (per input, the
record ids; per split aggregated input, the group's signature) is built
once per combination at expansion and held on `RunBindings.selections`;
the combo carries ONE key, `COMBO_KEY = "__combo"`, its index. scifor
grew `_select_rows(param, frame, combo)`, called after its schema filter,
and scidb's hook keeps the rows the Selection names and drops
`__record_id`. The frame is never renamed, scifor's schema is never
extended (Step 15 and Step 18 are gone, with `rid_keys_for_schema` and
the iterables padding), and nothing parses a prefix off a combo or a row:
the save (`for_combo`), the draft stamp, the skip gate, the normaliser,
introspect and the PathOutput collision guard all read the Selection.
scifor lost its four prefix strips; MATLAB's `for_each.m` lost its four
and gained `_row_selection` / `_record_id_column` (the bridge serialises
`selections` aligned with `full_combos`; `x__combo` / `x__record_id` are
the only two sanitised names left). `rid_per_combo` and friends are keyed
by PARAM.

Verify: `cd /workspace/scifor && pytest tests/ -q`, then the whole scidb
suite (`test_identity_parity.py` must show byte-identical edges;
`test_column_selection_combo_pruning.py` guards that `__record_id` never
reaches a function), then `cd /workspace/scimatlab && pytest tests/ -q`,
then the MATLAB suite (the `.m` loop changed), then `tests/integration`.

Stage 2c: **green** 2026-09-20 (scidb, scimatlab Python tests, tests/integration — user-run).
Two things surfaced by the first runs, both fixed: the bridge's row_selection
must be aligned with full_combos AFTER the skip hook (f101aeaf), and a Fixed
input's `__rid_ref` had been leaking into the function as a data column —
every Fixed record arrived one column wider than a plain one (4c245d43).

### Stage 3 — built 2026-09-20, tests unrun

Classified every function-level sibling import in `scidb` by "would
hoisting it create a cycle?" (a node script, then `scidb/tests/test_imports.py`
which does the same with `ast`). The real cycle-dodges were four shared
things living in a module above their readers, plus one asymmetry:

* the function-role classifier (`function_role`, `endpoint_kind`,
  `ROLE_PREFIX`, `FUNCTION_ROLES`) — `discover.py` and `foreach.py` each
  held half → **`scidb/roles.py`**, a leaf. `discover`, `foreach`,
  `pipeline`, `inspect/report` and the GUI import it at the top.
* `_is_loadable` / `_input_type_name` / `_find_pathinput` in `foreach`,
  imported lazily by `foreach_config` (×3), `glue`, `pipeline`,
  `provenance_query`, `across_variants`, `variant`, `state` →
  **`input_spec.is_loadable` / `spec_name` / `find_pathinput`**. For the
  wrapper modules to import their display name at the top, `input_spec`
  became a true leaf: the wrapper types are imported at call time (the one
  allow-listed dodge, documented in the module).
* `_schema_str` / `_from_schema_str` / `_canonical_numeric_value` in
  `database`, which is why `provenance_query` top-imported `database` and
  `database` imported `provenance_query` inside twenty functions →
  **`scidb/schema_values.py`** (`schema_str`, `from_schema_str`,
  `canonical_numeric_value`, `VALID_SCHEMA_KEY_TYPES`). `filters` had its
  own copy of the write rule (`_to_schema_str`, "mirrors
  database._schema_str") — one owner now. `database` imports
  `provenance_query` at the top.
* `PerComboLoader` / `PerComboLoaderMerge` in `foreach`, imported lazily by
  `glue` and the MATLAB bridge → **`scidb/per_combo.py`**.

What stays lazy, on purpose (`ALLOWED_CYCLE_DODGES` in the test, each with
its reason): `variable → database` (a variable reaches the ambient database
at call time), `pipeline → foreach` (a pipeline RUNS for_each; for_each
consults the active pipeline at import time), `input_spec → variant /
across_variants` (above). Top-level cycles: none, asserted.

`test_imports.py` also imports every `scidb` module first in a fresh
interpreter — the only honest probe for an order-dependent cycle, since
`conftest` has already imported everything by the time a test runs.

The GUI's id helpers (`strip_placement` / `parse_fn_node_id`) are in
`domain/graph_builder`, which is already pure; their lazy importers are
not cycle-dodges. They move in Stage 5, where `ids.py` gets the newtypes.

Verify: `cd /workspace/scidb && pytest tests/test_imports.py -q` first
(it is slow — one interpreter per module), then the whole scidb suite,
then `cd /workspace/scimatlab && pytest tests/ -q` (the bridge import
moved), then `cd /workspace/scistack-gui && pytest tests/ -q`.

### Stage 4 — built 2026-09-20, tests unrun

`scistack_gui/api/handlers.py`: `Handler(name, path, params, call,
holds_db_lock, needs_db, http_errors, http_method)` and three builders —
`rpc_methods(table)` (the `name -> callable(params)` entries for
`server.METHODS`; params are validated through the same pydantic model the
HTTP body uses), `self_managed(table)` (the names whose row says
`holds_db_lock=False`), `install_routes(router, table)` (one FastAPI route
per row, with the row's exception → status map). The module is named
`handlers.py`, not the plan's `registry.py` — `api/registry.py` is already
the `/api/registry` route.

Migrated: the plot family + the webview error report (14 methods) —
`api/plot.py` is now the table, `server.py` lost its fourteen `_h_plot_*`
and splices `**rpc_methods(PLOT_HANDLERS)` / `| self_managed(PLOT_HANDLERS)`
in. `variable_provenance` and `node_location_tree` stay hand-written (they
sat in the plot block but are not plot handlers; next to move, with the
rest, one family per commit).

`tests/test_api_handlers.py` asserts, per row: it is in `METHODS` AND is
the table's own entry; it is a route in `create_app().openapi()`; it is in
`frontend/src/api.ts` with the same path, method and body-ness; its lock
policy is what the dispatch loop applies; and that no `_h_<table name>`
or `@router.post` survives outside the table. The frontend check found a
real gap on its first run-by-eye: `plot_variant_sets_save` had no browser
route (added to `api.ts`; **both vite bundles need a rebuild**).

Verify: `cd /workspace/scistack-gui && pytest tests/test_api_handlers.py
tests/test_plot_service.py tests/test_client_errors.py -q`, then the whole
GUI suite.

### Stage 5 — built 2026-09-20, tests unrun

* `scistack_gui/ids.py` (leaf): `BareNodeId` / `PlacedNodeId` (`str`
  subclasses; placing a placed id raises), `placement_id` /
  `parse_placement_id` / `strip_placement` / `fn_node_id` /
  `parse_fn_node_id`, the prefixes, `DB_DERIVED_PREFIXES`, and ONE
  `ROOT_SCOPE` (it was `scope_filter.ROOT`, `pipeline_store.ROOT_PIPELINE_ID`
  and `graph_builder._ROOT_PIPELINE_ID`). Every importer moved
  (`graph_builder` included — 23 files); `tests/test_ids.py`.
* Seam assertions: `for_each` refuses `schema_keys="subject"` (would have
  iterated by character) and an unknown key by name; `InputBinding.
  __post_init__` refuses a pinned rid on a non-PINNED binding and a pool on
  a non-AGGREGATED one (a ColumnSelection never pools outside aggregation).
  The indexed-binding-name fold is by construction now (edges are keyed by
  the Selection's params) — no assertion needed.
* Dead spellings deleted: `rid_column` / `param_of` / `is_rid_column` /
  `rid_columns` / `RID_PREFIX` / the whole `vsig_*` family /
  `InputBinding.column` / `tracked_columns` (→ `tracked_params`); the
  Fixed-rid lookup is keyed by param.
* Docs: `docs/claude/archive/` holds the five self-declared superseded
  narratives (links repointed); `docs/claude/decisions.md` is the one-page
  ADR index for the week's nine decisions; `database-model.md` carries the
  `across_variants` column.

### Stage 6 — decided YES and built 2026-09-20, tests unrun

The user was away; the decision is recorded as D-2026-09-20-9 in
`docs/claude/decisions.md` with the reasoning. In one line: a statement is
made ON a canvas and applies there; the scope is read off the node id
(`intent_store.scope_of_node` — placement suffix, manual row, else root
`main`, the rule the `hidden` aspect always used), so NO request carries
it; reads resolve `scope -> global` with `global` as the legacy layer; a
duplicate copies the source as resolved into its own scope; runs, compiled
pipelines and both code exports read the hides of the canvas they run from.

Code: `intent_store.scope_of_node` / `scopes` / `node_config_overlay(db,
scope)` / `node_config_overlay_every_scope` / setters with `scope=` /
`copy_subject(src_scope=, dst_scope=)`; `pipeline_store.manual_node_scope`,
`get_node_config` resolves the id's scope, `get_node_configs(db,
pipeline_id)`, `get_manual_nodes` overlays per scope; `_build_graph` passes
its scope; `execution_service._hidden_constant_values(db, pipeline_id)`
and the four `get_hidden_node_ids` callers pass the canvas;
`scope_service._clone_nodes` copies into the target scope.
`tests/test_intent_store.py::TestScopeAware`.

Verify: `cd /workspace/scistack-gui && pytest tests/test_ids.py
tests/test_intent_store.py -q`, then the whole GUI suite, then
`tests/integration`. GUI manual check: `docs/gui-manual-testing-todo.md`
items 0c and 0d.
