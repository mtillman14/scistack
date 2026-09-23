# Node identity: which ids are computed and which are allocated

*Written 2026-09-22, from the investigation of a duplicated `grSides` node
(`.claude/plan-run-state-and-duplicate-nodes.md` Problem 3). The duplicate
turned out to be a symptom of a category error in how one entity kind is
identified. Companion to `docs/claude/intent-and-fact.md`, whose model rests
on the assumption this document examines.*

---

## 1. Why this document exists

A user drew `Demographics → grSides.side` on the canvas, ran the node, and got
two `grSides` nodes. Nothing was duplicated in the database; the second node
was the same node under a different id.

The node id is `fn__{fn_name}__{wiring_id}`, and `wiring_id` hashes the
function's **recorded** input bindings. Drawing the edge does not change it —
that is deliberate, and stated as a rule in
`manual-edges-on-history-nodes.md`, because saved position, scope membership
and node config all key off the id. But *running* the node records the drawn
edge, which changes the recorded bindings, which changes the hash.

So the id is invariant under configuring and not under running. Same picture
on the canvas, two ids, depending on whether you have run yet.

The duplicate is the visible half. The invisible half is worse: every
statement keyed by that id is orphaned the moment it moves. Only
`columnSelections` is migrated, deliberately
(`api/pipeline._migrate_column_selections`: *"The other saved settings were
never applied to that run and stay where they were."*). In the session that
prompted this, the user re-set `schemaLevel` on the new node six minutes after
the run, having been given no indication that their setting had stopped
applying.

---

## 2. Two questions that both look like identity

**"Are these the same thing because their content is the same?"**
Answered by a hash. Two independent runs of the same code over the same
inputs must produce the *same* id — on another machine, in another database,
a year later. That is what makes a re-run cheap (`0 new rows`), what makes
provenance matchable, and what makes node state possible: `provenance_query.
expected_invocations_for_function` **predicts** invocation ids from current
data and checks which are present. You cannot predict an allocated id.

**"Are these the same thing because it is the same thing a person made?"**
Answered by an allocated id. A node you dragged onto a canvas is the same node
tomorrow even if you rewire it, rename its inputs and run it three ways.
Nothing about its content establishes that. Only continuity does.

These cannot be unified. A content hash cannot survive its content changing —
that is what it is for. An allocated id cannot be independently recomputed —
that is what *it* is for.

**This is the intent/fact cut, applied to identity.** A fact is the same fact
because its content is the same; an intent is the same intent because it is
the same statement by the same person.

---

## 3. The inventory

### Content-derived — must stay hashes

| id | derived from | owner |
|---|---|---|
| `record_id` | type + schema version + content hash + location | `scicanonicalhash.generate_record_id` |
| `content_hash` | the serialized data | `scicanonicalhash` |
| `invocation_id` | fn hash + run options + input bindings + constants | `provenance.compute_invocation_id` |
| `function_hash` | the function's source AST | `scilineage.hashing` |
| `call_id` | the call-site config | `foreach_config.CallSite` |
| `wiring_id` | fn name + input params + outputs + path inputs | `provenance.compute_wiring_id` |
| constant / PathInput / glue record ids | the value / template / chain | `provenance.compute_*_record_id` |
| `chain_hash` | the glue chain | `scidb.glue` |

### Allocated — already UUIDs

| id | shape | where |
|---|---|---|
| `run_id` | `uuid4().hex[:16]` | `provenance.generate_run_id` |
| `pipeline_id` (scope / hypothesis) | `pipe_{uuid4[:12]}` | `pipeline_store.create_pipeline` |
| manual & duplicated node ids | `{prefix}__{label}__{uuid4[:8]}` | `scope_service`, `portability_service` |
| manual edge ids | `edge_{uuid4[:12]}`, `manual__…` | `scope_service`, layout store |
| plot save job ids | `uuid4[:8]` | `plot_service` |

`generate_run_id`'s own docstring states the rule this table makes explicit:

> Unlike everything else here this is intentionally unique per call so the
> `_run` audit log captures *every* execution event — even a re-run that
> reproduces existing (deduped) invocations.

### One hybrid

`schema_id` is an allocated integer (`MAX(schema_id) + 1`) **interned by
content**: you look it up by the schema key values and get back a stable
surrogate (`schema-id-allocation.md`). Allocated id, immutable natural key
behind it. Worth naming because a function node is *nearly* this shape and
differs in the one way that matters: its natural key (the wiring) is mutable,
so there is no content to intern on. What makes a node "the same node" is
continuity of intent, and nothing else.

---

## 4. The category error

A canvas function node is an **intent** entity. A person creates it,
configures it, rewires it, and expects it to persist through all of that.

It is the only such entity identified by a **fact** id.

Every symptom follows mechanically:

| symptom | mechanism |
|---|---|
| a node duplicates after a run | the run records a new wiring; the hash moves; the old hash is still a node |
| saved settings silently stop applying | statements are keyed by the id that moved |
| a `wiring` statement is keyed by the wiring | `intent-and-fact.md` §5 makes `wiring` an aspect and §3 makes `subject_ref` a `wiring_id` — the statement that changes the wiring is keyed by it |
| identity cannot follow the visible wiring | `manual_edge_handle_index` finds an overlay **by** the node id in the edge's `target`; move the id and the lookup misses, the overlay vanishes, the id reverts — it oscillates |

The two halves of the canvas already disagree with each other. A manual node
carries an allocated id; the moment it graduates into a DB-derived node it
swaps to a hash. Same entity, two identity schemes, and the swap is where
config is orphaned — `fn__grSides__5c9r0r` in the 2026-09-22 orphan warning is
exactly that, a pre-graduation id nothing adopts.

**The project has already made this decision twice, elsewhere.** From
`ids.py`, on `param__` replacing `const__`/`sweep__`:

> One prefix is what lets a Parameter keep its identity when a second value
> turns its declaration from a Constant into a Sweep: the id no longer encodes
> which form the source currently uses.

Identical reasoning. Stop encoding a mutable property in the id so the id
survives that property changing. Function nodes are the kind it was not
applied to.

---

## 5. What changes

`wiring_id` does **not** go away. It is a good content id answering a real
question — *what shape did this run have?* — and `derive_target_for_node`,
`hidden_wirings` and the variant grouping all legitimately ask it.

It stops being **identity** and becomes an **attribute**:

| | before | after |
|---|---|---|
| node id | `fn__{fn}__{wiring_id}` | allocated, minted once |
| a node's current wiring | implied by the id | resolved from its `wiring` statements |
| a node's recorded wirings | implied by the id (exactly one) | an association: node ↔ the wirings its invocations used, with the run that did it |
| attaching history to a node | hash equality | recorded at dispatch, or inferred by §7a |

A node then has one stated wiring and a history of wirings it has run as.
`grSides` would be one node with two entries in that history — the honest
description of what happened to it.

---

## 6. How the two kinds of edge are documented

Three things, not two, and the third is what replaces *"the id **is** the
wiring"*:

| | what it is | where | mutable |
|---|---|---|---|
| **stated edge** | "on node N, `side` is fed by `Demographics`" | `_intent`, `aspect="wiring"`, `subject_ref` = node id | yes — you drew it, you can redraw it |
| **recorded edge** | "invocation X consumed record R as `side`" | provenance, `_invocation_input` | never |
| **node ↔ wiring** | "node N ran as wiring W, under run R, between T1 and T2" | new association beside `_intent` (§7a) | append-only |

The circularity in §4 disappears: with an allocated `subject_ref`, the
statement that sets the wiring is no longer keyed by the wiring.

**What the canvas draws is unchanged in rule** — the resolved stated edges,
because visible edges are ground truth. What becomes *expressible* is the
difference between "I drew this" and "this has run", which today is
represented by a second node appearing. The vocabulary already exists:
`intent-and-fact.md` defines `Decision` for exactly this and
`graph_builder.mark_unused_intent` already renders it as an amber
NOT REFLECTED chip.

---

## 7. How a node claims an invocation — settled 2026-09-22 (D-2026-09-22-2)

### 7a. Attribution: recorded at dispatch, inferred only for the rest

The question "how do we infer which node an invocation belongs to?" concedes
too much. For a GUI-started run there is nothing to infer — **the GUI already
holds the node id**; it is in the run request, and
`matlab_command_service.scope_variants_to_node` already depends on it.

**A GUI-originated run records the association at dispatch.** No inference,
ever.

**A script or terminal run is inferred on the next graph build**, for any
recorded wiring with no association in that scope:

1. exactly one node in the scope **states** wiring W — its resolved intent
   wiring hashes to W → attribute to that node;
2. else exactly one node in the scope has **previously run as** W → attribute
   to that node;
3. else → allocate a new node.

Step 1 is what closes the case this document came from. Before the run the
node *states* W2, because the drawn edge is part of its stated wiring; the run
records W2; same node. The duplicate never forms, and it is prevented by the
**stated** wiring — the thing the user controls — rather than by a repair
applied afterwards.

**Where it lives: GUI-side, beside `_intent` — never in provenance.**
`intent-and-fact.md` §8 is explicit ("If scidb ever had to import
`pipeline_store`, the design is wrong") and fact is never edited. The
association is a statement about which canvas node means which wiring, so it
is intent-adjacent, not fact.

**Grain:** `(node_id, wiring_id, run_id, first_seen, last_seen)`, append-only,
one row per distinct wiring a node has run as. `wiring_id` is the right grain
because it is what the node *is*; `call_id` is finer (a node has one per
variant row) and `invocation_id` is per-execution. `run_id` is carried so the
row also answers *when, and under which run* — which makes "last run as W2,
before that W1" a query, and gives Problem 9's variant view most of its
chronology for free.

**This doubles as the migration.** First open of an existing database
allocates one node per distinct `(wiring, scope)` in history and seeds the
association from it — exactly today's derived behaviour, persisted once
instead of recomputed on every build.

### 7b. Ambiguity: scope, then history, then age — and tell the user

Two nodes can share a wiring in two ways.

**Different scopes** — a duplicated hypothesis. Already handled: scope is a
property of the node id (D-2026-09-20-9) and runs are already scope-aware.
Attribution considers only nodes in the scope the run started from.

**Same scope, identical stated wiring** — the user dragged a second node and
wired it the same way. Resolution, in order:

1. prefer a node that has already run as W (association history);
2. else the **oldest** node — deterministic and stable across rebuilds.

**And raise a popup, not just a log line** (user decision 2026-09-22). Two
nodes stating identical wiring compute identical things, so the state is
almost certainly unintended, and a warning buried in `scidb.log` is a warning
nobody reads. It is also recoverable — rewire one and the ambiguity resolves
itself — so the popup names both nodes and says that runs will be attributed
to one of them until they differ.

**What is deliberately not done: auto-merge.** Silently collapsing two nodes a
person created is a worse failure than a message they can act on. The existing
instinct points the same way — `api/pipeline._wiring_conflicts_with_candidate`
already refuses to merge a fresh node whose wiring conflicts with a
candidate's, and merges only a *bare* (unwired) one.

### 7c. Cross-database identity: already answered, and not a blocker

Nothing crosses a database boundary relying on node ids being reproducible.
`portability_service._resolve_pipeline` **already mints fresh ids on import** —

```python
new_id = f"{prefix}__{n['label']}__{uuid.uuid4().hex[:8]}"
node_id_map[n["node_id"]] = new_id
```

— and rewrites every reference through `node_id_map`. `document["nodes"]`
holds *manual* nodes only; DB-derived nodes are not exported and are
reconstructed from provenance in the target database, so under this model they
are allocated on first open there.

The remaining constraint is narrower and intra-database: **ids are persisted
and never re-derived**, so reopening the same database twice cannot churn
them. That is a storage requirement, not a design question.

---

## 8. What does not change

* Every content id in §3 stays a content id. Making `record_id` a UUID would
  write a duplicate on every re-run and break
  `variable_content_fingerprint`; making `invocation_id` one would make node
  state impossible, since the whole mechanism is predicting ids forward and
  reconstructing them backward (`scidb/tests/test_identity_parity.py`).
* Provenance is still never edited. Fact remains the floor.
* Display intent (positions, groupings) is still out of the intent model's
  scope; it is keyed by node id and benefits from the id being stable.

---

## 9. Ground truth

| what | where |
|---|---|
| the id constructors | `scidb/src/scidb/provenance.py`, `scicanonicalhash/hashing.py` |
| node id shapes and the placement suffix | `scistack_gui/ids.py`, `placement-qualified-ids.md` |
| `subject_ref` as a stable id | `intent-and-fact.md` §3 |
| `wiring` as an aspect | `intent-and-fact.md` §5 |
| the overlay keyed by node id | `graph_builder.manual_edge_handle_index` |
| the partial migration today | `api/pipeline._migrate_column_selections` |
| the same decision, for Parameters | `ids.py`, `PARAM_ID_PREFIX` |
| the symptom this came from | `.claude/plan-run-state-and-duplicate-nodes.md` Problem 3 |

---

## 10. What was built — 2026-09-22/23

`.claude/plan-node-identity.md`, all six stages but one piece of Stage 6.
The full `scistack-gui` suite passes as of 2026-09-23; the canvas has not been
looked at (`docs/gui-manual-testing-todo.md` item 0p).

### The table

`_node_wiring (node_id, wiring_id, run_id, first_seen, last_seen, scope)`,
primary key `(node_id, wiring_id)`, append-only. Created by
`pipeline_store._ensure_tables` beside `_intent`, never in provenance (§7a).
Accessors in `scistack_gui/node_wiring.py`; a repeat advances `last_seen` and
leaves `first_seen` alone, because rewriting it would erase the chronology
that makes *"what did it run as before?"* answerable.

### Two questions, two answers

Attribution gives a node a growing list of wirings. That list is **history**.
The node's shape on the canvas is its **current** wiring — the latest one it
ran as, plus any edge drawn since.

| | current wiring | every wiring |
|---|---|---|
| lookup | `node_wiring.current_wiring` | `node_wiring.wirings_for_node` |
| decides | handles, edges, constants, what a Run executes | attribution, the Variants view, provenance |

Conflating them is how a node ends up drawing a handle for a shape the user
rewired away from, or re-running records it deliberately moved on from. The
graph build takes a node's shape from its current wiring only
(`IdentityPlan.is_current` → `group_call_sites_by_wiring`), and
`derive_target_for_node` runs that one wiring, not the union.

### The id that is minted

`fn__{fn}__{uuid4[:16]}`, always (D-2026-09-22-3). The only readable thing in
a node id is the function it runs. Sixteen hex because `ids.parse_fn_node_id`
recognises a DB-derived function node by exactly that, in ~40 places — a
contract with the id grammar, not part of the decision.

**There is no migration.** A database built before this change has no
associations, so the first build mints a fresh id for every function node and
every saved position, node config, hide and scope membership keyed by
`fn__{fn}__{wiring_id}` stops resolving. Paid once, deliberately: an id that
*looks* like a wiring hash and is not one is the confusion §1 is about.
`node_wiring.forget_all` pays it again on purpose; nothing calls it.

### The seam

`graph_builder`'s node-deriving functions no longer spell
`fn__{fn}__{wiring_id}`. They take **`token_for(fn_name, wiring) -> token`**
(and `group_call_sites_by_wiring` also takes `is_current`), and both are
**REQUIRED, with no default**. Deriving a node id from its wiring is the bug
this removes; a default that quietly did it would make the regression
invisible — no error, no log line, just the old behaviour. A missing argument
is a `TypeError` at the call site instead. `identity_token` and the test
helper `_all_current` exist for callers where the equivalence is TRUE and
worth stating: the pure-domain tests, which have no nodes and no database and
are testing graph shape rather than identity.

The seam covers:

| function | what the token keys |
|---|---|
| `group_call_sites_by_wiring` | the canvas node each call site lands in |
| `hidden_wirings` / `wiring_disconnected_fkeys` | reconstructed edge ids vs stored hidden edge ids |
| `input_params_with_manual_edges` | the manual-edge handle index |
| `wirings_downstream_of` | the cascade, keyed the same as its seed |

`manual_edge_handle_index`'s key was ALWAYS the node's token — it is read off
the edge's `target` — which is why the parameter was called `wid` everywhere.
Outside the build, `node_wiring.token_resolver(db)` is the same seam read from
the table, and `variant_resolver.reconcile_manual_inputs` takes a `token_for`
too — the name-scoped run path has no node id, so it maps each target's wiring
to whichever node holds it. Without that, a drawn edge is looked for under the
wiring rather than the node id it was stored against, finds nothing, and the
run silently ignores it.

**A caller that needs node ids but not the graph** asks
`api/pipeline.ensure_node_identities(db)`, which runs the same one resolution
`_build_graph` does and is idempotent. `execution_service._scope_function_node_ids`
is the case: it lists the nodes a scope compiles, from HISTORY, and history can
hold a wiring written since the canvas last refreshed. Fabricating an id there
would be two surfaces inventing an id for one node — the bug this removes — and
under allocation a fabricated id names no node at all, so the step compiles to
nothing.

### Attribution

`domain/node_identity.resolve_identities` — pure, no database, returns an
`IdentityPlan` the caller persists. In order: recorded association → stated by
exactly one existing node → stated by several (§7b) → mint.

**One pass, no ordering.** A node can only state a wiring if it already
exists, and it exists exactly when `_node_wiring` knows it — so rule 2's
candidates are read from the table (`stated_by`, built by
`graph_builder.stated_wiring_claims` from `current_wiring_by_node`), never
from the assignment being built. Nothing minted in a pass can claim anything
in the same pass, because a node whose id did not exist a moment ago has no
edges drawn onto it.

An earlier draft minted one wiring at a time so a freshly minted node could
absorb a wiring in the next round. That existed only to fold in a duplicate
formed *before* this change — a migration — and dropping it (D-2026-09-22-4)
removed the iteration, the ordering, and the `first_saved` field it had been
reading out of `scidb.get_aggregated_variants`.

An `EachOf` overlay claims **every** source's wiring, not just the first: a
run splits into one wiring per source, and claiming only the first would let
the second fork a node.

Dispatch recording (`execution_service.record_dispatch_wirings`) covers the
Python run thread, the pipeline compiler and both MATLAB command routes.

### Nothing is silent

Neither half swallows a failure (D-2026-09-22-5). `node_wiring`'s reads raise,
`_resolve_node_identity` raises, `record_dispatch_wirings` raises. If identity
cannot be resolved the GUI cannot say which node a run belongs to, and node
state, the Run button and every saved setting are then describing something
unverified — a canvas that draws but cannot be trusted is worse than an error,
because the user cannot tell the two apart.

### Ambiguity

`Ambiguity` rides out of the plan and onto the graph response as
`warnings: [{kind: "wiring_ambiguity", ...}]`. `PipelineDAG` raises a dialog
naming both nodes and saying runs go to one of them until they differ;
dismissal is per tab and per pair. The log says it once per process per pair
(`_should_log_ambiguity`) — a repeated warning buries the next one. Nothing is
merged. The tie-break is: a node that has already run as that wiring, else the
oldest by `first_seen` in `_node_wiring`, else the id.

### What was retired

`graph_builder.superseded_manual_input_overrides` and
`api/pipeline._migrate_node_statements` are **deleted**. Under allocated ids
the wiring a run records is claimed by the node that stated it, so there is no
second node to detect, no edge to rewrite and no statement to carry.

### Deliberately NOT retired: the graduation id-swap

Stage 6 also listed it (D-2026-09-22-4). What was added instead is
`node_wiring.rekey_node`, called from `pipeline_store.graduate_manual_node`, so
a manual node that was RUN before it graduated takes its dispatch record with
it. Do this one on its own, after the suite is green and the canvas has been
looked at.

### Tests

| file | what it pins |
|---|---|
| `scistack-gui/tests/test_node_wiring.py` | the table: round trip, append-only, current-shape vs history, the mint rule, re-keying |
| `scistack-gui/tests/test_node_identity.py` | the rule (pure), current-vs-history, the grSides shape end to end, dispatch recording, and that the repair paths are gone |
| `scistack-gui/tests/test_wiring_parity.py` | the fifth identity — a node's stated wiring, computed forward, equals the wiring its run recorded |
| `scistack-gui/tests/test_graph_builder.py::TestStatedWiringClaims` | rule 2's input, including the EachOf case |
| `scistack-gui/tests/test_api.py::TestManualInputEdgesOnHistoryNodes` | "nothing to carry, because the id did not move" |

`tests/conftest.py::bp_node_id` no longer computes an id — it LOOKS ONE UP
(and allocates if the GUI has not opened the database yet). Any test that
spells `fn_node_id(fn, wiring_id(...))` against a real database has to do the
same; the pure-domain tests, which use the default `token_for`, are unaffected
because there the token still is the wiring.
