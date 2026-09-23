# Decisions — one page each, newest first

Architecture decisions taken on `refactor/intent-and-fact` (2026-09-19/20),
in the ADR shape: context, decision, consequences, the doc that argues it in
full. The narratives stay where they are; this page is the index a reader
checks before reopening one of them. A decision here is CLOSED — reopen it
by adding a new entry that supersedes it, not by editing the old one.

---

## D-2026-09-22-3 — A minted node id says only which function it runs

**Context.** D-2026-09-22-1 says a function node's id is allocated, and its
table proposes `fn__{label}__{uuid4[:8]}` — the shape manual nodes use. The
first implementation (2026-09-22) instead minted the *derived* spelling
`fn__{fn}__{wiring_id}` when it was free, reasoning that what is load-bearing
is that the id is minted once and **persisted**, not that it is random — and
that keeping the old spelling made an existing database open to exactly the
ids it already had.

**Decision.** Reverted the same day. `node_wiring.mint_node_id` mints
`fn__{fn}__{uuid4[:16]}`, always. The only readable thing in a node id is the
function it runs; its wiring, its constants and its scope are all looked up,
because every one of them can change while the node stays the same node.

Sixteen hex, not eight: `ids.parse_fn_node_id` recognises a DB-derived
function node by a 16-hex trailing segment in roughly forty places. That
length is a contract with the id grammar, not part of the decision.

**Consequences.** There is **no migration**. A database built before this
change has no associations, so the first build mints a fresh id for every
function node and every saved position, node config, hide and scope membership
keyed by `fn__{fn}__{wiring_id}` stops resolving. That churn is paid once and
is the point of the clean break: the alternative leaves ids that *look* like
wiring hashes and are not, which is exactly the confusion
`node-identity.md` §1 is about and exactly the mistake a reader would make in
six months. `node_wiring.forget_all` is the deliberate, manual way to pay it
again; nothing calls it.

The rejected alternative also cost more than compatibility. Minting the old
spelling only helps if the oldest wiring mints first, which forced
`resolve_identities` into a mint-one-then-re-resolve loop and made it read
`first_saved` out of `scidb.get_aggregated_variants` — a field added to a
shared layer on the canvas's hottest read purely to serve the shim. All three
went away with it.

## D-2026-09-22-4 — No absorb, and the graduation id-swap stays

**Context.** Stage 6 of `.claude/plan-node-identity.md` lists three repairs to
retire once identity stops moving: `superseded_manual_input_overrides`,
`_migrate_node_statements`, and the graduation id-swap — a manual node's
allocated id could simply *stay* its id once it has history, removing the
other half of the two-schemes problem (`node-identity.md` §4). The plan also
says, of the duplicate `grSides` already on a real canvas, *"do not build the
absorb patch."*

**Decision.** The first two are deleted. **No absorb was kept**: a first
implementation folded a pre-existing duplicate back together and carried its
statements over, and that is a migration — this is a beta project that takes
clean breaks (`feedback_beta_no_deprecation`), and under D-2026-09-22-3 there
is nothing to fold into anyway, since every node gets a fresh id on first
build. The graduation id-swap is **kept**, with `node_wiring.rekey_node` added
beside `intent_store.rekey_subject` in `pipeline_store.graduate_manual_node`
so a manual node that was RUN before it graduated carries its dispatch record
across.

**Consequences.** A duplicate that formed before this change stays two nodes
until the user hides or rewires one; it can no longer form again. Dropping the
absorb also removed the reason `resolve_identities` needed chronology: nothing
minted in a pass can claim anything in the same pass, so the whole thing is
one pass with no ordering.

A manual node still swaps identity once, at graduation, and that swap is still
where a pre-graduation id can be orphaned. Against that: `graduate_manual_node`
is asserted by roughly 130 assertions across twelve test files, and rewriting
them blind — in the same change as the identity model itself, before the suite
has been run and the canvas looked at — trades a known, contained defect for an
unknown one. Reopen this as its own change once `plan-node-identity.md`'s tests
are green and item 0p of `docs/gui-manual-testing-todo.md` has been checked.

## D-2026-09-22-5 — Identity failures stop the GUI

**Context.** The first implementation wrapped both halves of the identity
machinery — resolution on the graph build, recording at dispatch — in
`except Exception`, falling back to the derived id and logging a warning. The
reasoning was that a canvas that will not draw is worse than a node id that
moves.

**Decision.** Reverted (user decision 2026-09-23). Nothing swallows anything.
`node_wiring`'s reads raise, `_resolve_node_identity` raises, and
`record_dispatch_wirings` raises rather than letting a run write records that
nothing can attribute — and the identity seam takes no defaults, so a caller
that forgets to pass the mapping fails loudly too.

**Consequences.** If the association table cannot be read, the GUI cannot say
which node a run belongs to — and node state, the Run button, the variant rows
and every saved setting are then describing something unverified. Presenting
that as a working canvas is worse than presenting an error, because the user
has no way to tell the two apart. There is also no "old behaviour" left to
fall back to: the derivation the fallback used has been removed for being
wrong, so it would have been a fallback onto the bug.

The same reasoning reaches a **default argument**, which is a silent fallback
with no exception to catch. `token_for` and `is_current` on `graph_builder`'s
node-deriving functions are therefore required: forgetting one is a
`TypeError` at the call site, not a quiet return to deriving ids from wirings.
`identity_token` and the tests' `_all_current` are passed explicitly wherever
the equivalence genuinely holds — a stated assumption rather than an inherited
default.

## D-2026-09-22-2 — How a node claims an invocation

**Context.** D-2026-09-22-1 gives a function node an allocated id and demotes
`wiring_id` to an attribute. Hash equality then stops answering "which node
does this recorded invocation belong to", which it answered for free before,
and two nodes become able to share a wiring for the first time. Those were
listed as gating the implementation.

**Decision, three parts.**

*Attribution is recorded, not inferred, wherever it can be.* A GUI-started run
already carries the node id in its request — `matlab_command_service.
scope_variants_to_node` depends on it — so it writes the
`(node_id, wiring_id, run_id, first_seen, last_seen)` association at dispatch.
Only runs the GUI did not originate are inferred, on the next graph build:
one node in the scope **states** W → it; else one node has **previously run
as** W → it; else allocate. The first rule is what stops a node duplicating
after a run through a drawn edge, and it does so from the *stated* wiring —
what the user controls — rather than by repairing afterwards. The association
lives beside `_intent`, never in provenance (`intent-and-fact.md` §8; fact is
never edited). `run_id` is carried so the row also says *when and under which
run*, which makes a node's wiring chronology a query.

*Ambiguity resolves by scope, then history, then age — and raises a popup.*
Different scopes are already separated (D-2026-09-20-9). Within one scope,
prefer a node that has already run as W, else the oldest; either way warn the
user in the GUI, not only in `scidb.log`, because two nodes stating identical
wiring compute identical things and the state is almost certainly unintended.
**Not auto-merged**: silently collapsing two nodes a person created is worse
than a message they can act on, and the existing
`_wiring_conflicts_with_candidate` already declines to merge nodes the user
distinguished.

*Cross-database identity is a non-issue.* `portability_service` already mints
fresh node ids on import and remaps through `node_id_map`; DB-derived nodes
are not exported at all. The surviving constraint is intra-database: ids are
persisted and never re-derived, so reopening cannot churn them.

**Consequences.** D-2026-09-22-1 is no longer gated. The bootstrap doubles as
the migration: first open allocates one node per distinct `(wiring, scope)` in
history and seeds the association, which is today's derived behaviour
persisted once. A new GUI popup is needed for the ambiguity case. The
association table gives `scidb variants`' chronology (plan Problem 9) most of
what it needs for free. `docs/claude/node-identity.md` §7.

## D-2026-09-22-1 — Facts get computed ids, intents get allocated ids

**Context.** A `grSides` node duplicated itself after a run. The node id is
`fn__{fn}__{wiring_id}`, and `wiring_id` hashes the function's *recorded*
input bindings, so drawing an edge leaves the id alone (deliberate — position,
scope and config key off it) but *running* the node records that edge and
moves the id. Same canvas picture, two ids, depending on whether you have run
yet. The duplicate is the visible half; the invisible half is that every
statement keyed by the id is orphaned when it moves, and only
`columnSelections` is migrated — the user in that session silently lost
`schemaLevel` and re-entered it six minutes later.

The system already has two kinds of id and has never named the difference.
Content-derived ids (`record_id`, `invocation_id`, `function_hash`, `call_id`,
`wiring_id`) answer *"same content?"* and must be independently recomputable —
node state literally predicts `invocation_id`s forward and checks which are
present. Allocated ids (`run_id`, `pipeline_id`, manual node ids, edge ids)
answer *"same thing a person made?"* and must survive their content changing.

**Decision.** The two kinds are distinct and are assigned by the intent/fact
cut: **a fact is identified by its content; an intent is identified by
allocation.** A canvas function node is an intent entity — created,
configured and rewired by a person — and is therefore misclassified today. It
gains an allocated id, minted once. `wiring_id` is not removed: it stays a
content id answering "what shape did this run have?", demoted from *identity*
to *attribute*, with a node ↔ wiring association carrying the link that the
id used to carry implicitly.

This is the same decision already taken for Parameters (`ids.py`,
`PARAM_ID_PREFIX`: "the id no longer encodes which form the source currently
uses") and already true of manual nodes, which carry allocated ids until
graduation swaps them for a hash.

**Consequences.** `intent-and-fact.md` §3's "`subject_ref` is a STABLE id — a
`wiring_id`" is the assumption this corrects; §5's `wiring` aspect stops being
circular, because the statement that sets the wiring is no longer keyed by the
wiring. "I drew this" and "this has run" become the same node in two states
rather than two nodes, reportable through the `Decision` object and the
existing `mark_unused_intent` chip.

Three sub-decisions followed — attribution, ambiguity and cross-database
identity — and were **taken the same day in D-2026-09-22-2**
(`node-identity.md` §7). Implementation is not gated.

Argued in full: `docs/claude/node-identity.md`. Symptom it came from:
`.claude/plan-run-state-and-duplicate-nodes.md` Problem 3.

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
policy AND `frontend/src/api.ts`. Plots first (2026-09-20), every other
family on 2026-09-21; `server.py` holds no handler at all. Rows that the
rest of the API needed: `path=None` (RPC-only), path placeholders filled
from URL + query + body into one model, `notify_dag_updated`,
`wants_transport`.

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
