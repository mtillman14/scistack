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
