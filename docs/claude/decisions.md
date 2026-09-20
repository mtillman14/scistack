# Decisions — one page each, newest first

Architecture decisions taken on `refactor/intent-and-fact` (2026-09-19/20),
in the ADR shape: context, decision, consequences, the doc that argues it in
full. The narratives stay where they are; this page is the index a reader
checks before reopening one of them. A decision here is CLOSED — reopen it
by adding a new entry that supersedes it, not by editing the old one.

---

## D-2026-09-20-9 — A statement is made on a canvas and applies there

**Context.** `_intent` rows carried `scope` from the start, but every
node-config write landed at `global` and execution's hidden-value lookups
unioned every scope ("fail open"), so two placements of one wiring in two
hypotheses shared their run options and a value unchecked in one hypothesis
was excluded from another's run. The plan's Stage 6 asked: is a run scoped
to the hypothesis it was started in?

**Decision.** Yes. The scope of a statement about a node is a property of
the node id (`intent_store.scope_of_node`): a placement suffix names it, a
manual node's row names it, anything else is the root canvas `main` — the
same rule the `hidden` aspect always used. No request carries a scope. Reads
resolve `scope -> global`; `global` is not a canvas but the legacy layer
(rows written before 2026-09-20 applied everywhere and still do, until the
canvas shadows them). A duplicate copies the source AS RESOLVED into its
own scope (`copy_subject(src_scope, dst_scope)`) and is independent from
then on. Runs, compiled pipelines and both code exports read the hides of
the canvas they run from; only the name-scoped fallback (a run request with
no node id) still unions.

**Consequences.** A hypothesis can hold its own run options, schema level,
column selections and hidden values for a node the root also has. Editing
root after duplicating does not reach the duplicate (it copied). The
`_node_config` legacy import now lands a placed id's blob at its scope.
`docs/claude/intent-and-fact.md` §7, §11.

## D-2026-09-20-8 — A node id is a type

**Context.** ~40 call sites told a bare id from a placed one by a substring
test; every store keyed by the bare id had, at some point, been handed a
placed one and silently missed (the placement-id lookup trap, fixed twice).

**Decision.** `scistack_gui/ids.py` (a leaf) owns the vocabulary:
`BareNodeId` / `PlacedNodeId` (`str` subclasses — JSON, DuckDB and
`startswith` unchanged), `placement_id` / `parse_placement_id` /
`strip_placement` / `fn_node_id` / `parse_fn_node_id`, the prefixes, and
ONE root-scope spelling (`ROOT_SCOPE`; it had three). Placing an already
placed id is a `ValueError` at the seam that would have written `a::x::y`.

**Consequences.** `graph_builder` imports its ids like everyone else. A
store's seam strips once and says so; the type is information, not a
wrapper. `tests/test_ids.py`.

## D-2026-09-20-7 — One handler table serves both GUI transports

**Context.** Every GUI operation was written twice — an `_h_*` JSON-RPC
function reading a raw dict, a `@router.post` reading a pydantic model —
plus a third hand-kept list of which ones must not hold the DuckDB lock,
plus the frontend's own route map. Each could drift silently.

**Decision.** `scistack_gui/api/handlers.py`: a `Handler(name, path,
params, call, holds_db_lock, needs_db, http_errors)` row per operation;
`rpc_methods` / `self_managed` / `install_routes` derive the three
server-side artefacts from it, and RPC params are validated through the
same pydantic model as the HTTP body. `tests/test_api_handlers.py` checks
every row against `server.METHODS`, `create_app().openapi()`, the lock
policy AND `frontend/src/api.ts`. Migrated family by family; plots first.

**Consequences.** Adding a method is one row. The first run of the test
found `plot_variant_sets_save` had no browser route.

## D-2026-09-20-6 — Shared things live below their readers

**Context.** `database` imported `provenance_query` inside twenty
functions because `provenance_query` needed one string rule from
`database`; five modules imported `foreach` inside functions for three
helpers; the role classifier was split between `discover` and `foreach`.

**Decision.** Move the shared thing down, never import lazily to dodge:
`roles.py`, `input_spec.py` (now a true leaf), `schema_values.py`,
`per_combo.py`. `scidb/tests/test_imports.py` imports every module first
in a fresh interpreter and allow-lists the three lazies that are genuinely
runtime inversions (`variable -> database`, `pipeline -> foreach`,
`input_spec -> the wrapper types`), each with its reason.

**Consequences.** A new cycle-dodge fails a test until it is argued for.

## D-2026-09-20-5 — The selection is a value; the combo carries one handle

**Context.** Which records a combination reads was encoded as
`__rid_{param}` / `__vsig_{param}` keys on the combo, on the frame, on the
result row and in scifor's global schema — a dict whose key prefix said what
the value was, parsed back out in eight places in two languages.

**Decision.** `scidb.bindings.Selection` (per input the rids; per split
aggregated input the group signature), built once per combination and held
on `RunBindings.selections`; the combo carries `__combo`, its index. scifor
grew `_select_rows(param, frame, combo)`; the frame keeps `__record_id`;
the schema is never extended. MATLAB gets the selection pre-computed
(`row_selection`, aligned with the combos it loops over).

**Consequences.** One rule selects the rows AND writes the edges. The
per-input spellings are deleted, not kept. `.claude/plan-2c-row-selection-seam.md`,
`docs/claude/scidb-for-each-internals.md`.

## D-2026-09-20-4 — `AcrossVariants` is a fact

**Context.** The graph could not tell an explicitly pooled input from an
auto-split one, so the predictor always assumed split (conservative) and an
`AcrossVariants` step never planned green.

**Decision.** `_invocation.across_variants VARCHAR[]`, folded into
`invocation_id` only when non-empty (every existing id unchanged), carried
by `RunOptions` beside `distribute` / `as_table` and therefore in the call
id and the skip gate's run-option comparison.

**Consequences.** No gap between what ran and what the graph can say.

## D-2026-09-20-3 — One assembly of the call-site identity

**Context.** The call-id payload was assembled in four places
(`ForEachConfig`, `provenance_query.config_call_id`, `pipeline_variants`,
the GUI's `variant_resolver.compute_call_id`); every new run option touched
all of them.

**Decision.** `scidb.foreach_config.CallSite(fn_name, inputs, constants,
options, glue)` with `version_keys()` / `call_id`; the forward path fills
it from live inputs, the backward paths from the graph, the GUI imports it.
Inputs are named by `input_spec.type_name` (one unwrap of the wrapper
stack). A `Fixed` pin is an edge, not part of the call site.

**Consequences.** `test_identity_parity.py` is the guard: forward and
backward ids agree by construction.

## D-2026-09-20-2 — Edges carry the real parameter name

**Context.** An aggregating call wrote its N input records under indexed
names (`value_0`, `value_1`); nothing folded them on read, so every
backward reconstruction named parameters the forward call did not have and
an aggregating step could never plan green.

**Decision.** Several edges per parameter, the parameter's own name (the
PK already allowed it); the predictor enumerates N bindings; every
aggregation `invocation_id` moved once. `execution_service._fold_indexed_params`
is a legacy-row repair only.

## D-2026-09-19-1 — Intent vs fact

Three decisions, argued in `docs/claude/intent-and-fact.md` §7:
a script run ignores GUI intent (origin `script` reads source only, and the
canvas must say "stated here, not used by the last run"); no write-back
into `.py` / `.m` (surfaces are peers; code export is the only bridge);
scope on every row, resolution walks `scope -> global`, duplication copies
(not `derived_from` inheritance). D-2026-09-20-9 finishes the third.

## Also this week, recorded elsewhere

* `schema_keys=None` is one call over the dataset; `[]` is every key
  (`for_each` refuses a bare string and an unknown key, 2026-09-20) —
  `docs/claude/scidb-for-each-internals.md`.
* The schema level default has one owner (node > last run > inputs union
  > all keys) — `execution_service.default_schema_level`.
* Variant space is three dimensions in one dict; run options are not a
  coordinate — `docs/claude/variant-space.md`.
