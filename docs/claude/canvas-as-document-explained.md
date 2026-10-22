# The canvas as a document, explained from the ground up

*Written 2026-10-21 as a plain-language companion to
`.claude/plan-canvas-as-document.md`. Built 2026-10-22 (all stages; pytest
unrun at the time of writing): see Part 8 for where each piece lives. Parts
1-7 describe the design as it was proposed and decided.*

This document has four parts:

1. **A glossary.** Every non-standard term, with what it means *in this project*.
2. **A running example.** One small pipeline, used all the way through.
3. **How the canvas works today, and what goes wrong.** Using the example.
4. **The proposed design, one step at a time.** Each step with the example,
   then where it differs from the written plan, and the decisions that are
   yours.

---

## Part 1: Glossary

Grouped so that each term only depends on the ones above it.

### Basic things on the screen

**Canvas.** The DAG picture in the GUI: boxes connected by arrows. There is
one canvas per tab (the root pipeline, each hypothesis, each submodule).

**Node.** One box on the canvas. There are several kinds:

| kind | what it stands for | example |
|---|---|---|
| variable node | a variable type (a `BaseVariable` subclass) | `RawGait` |
| function node | one step that calls a function | `filter_gait` |
| Parameter node | a named constant that feeds functions, with one or more values | `cutoff` with values 6 and 10 |
| PathInput node | a file-path template that feeds a loader | `gait_files` = `{subject}/{session}.csv` |
| glue node | a small transient reshaping function between two steps | `pick_left_side` |
| pipeline node | a submodule used as a single box | `Preprocessing` |

**Edge.** One arrow. It goes from a source node to a target node, and when it
enters a function it lands on one **port** (handle), `in__<argument>`. An
example: `RawGait → filter_gait` on port `in__data`.

**Node id.** The string the code uses to name a node: `var__RawGait`,
`param__cutoff`, `pathInput__gait_files`, or `fn__filter_gait__3f9a…` (the
function name plus a 16-hex token). Saved positions, settings, hides and
edges all refer to nodes by this id. That makes **an id that changes** one of
the worst things that can happen: everything attached to the old id is
silently orphaned.

**Scope.** Which tab (canvas) something belongs to. The root canvas is `main`,
and each hypothesis or submodule has its own `pipe_…` id.

**Placement id.** A node id with `::<scope>` appended, such as
`fn__filter_gait__3f9a…::pipe_abc`. It is used when one node appears on more
than one tab. `strip_placement` removes the suffix to get the **bare id**.

### Fact versus intent (the core distinction)

**Fact / history / provenance.** What actually ran, recorded by scidb as the
run happened. It is never edited. It lives in tables like `_record` (the saved
data), `_invocation` (one function call on one schema location),
`_invocation_input` / `_invocation_output` (which records went in and came
out), and `_run` (one press of Run, or one execution of a script).

**Intent.** What *you* say should happen: which edges exist, which values a
Parameter has, which columns an input reads, which nodes are hidden. You can
change it any time. Most of it lives in one table, `_intent`, as
**statements**: rows saying "about this subject, this aspect has this value,
in this scope".

**Origin.** Who started a run. `gui` means the Run button; `script` means
`python pipeline.py` or a MATLAB terminal. A script run does not read the
GUI's intent (decided 2026-09-19); it only reads the source code.

### How a run is described

**Call site.** One distinct way a function was called: the function, which
variable type fed each argument, which constant values, and which run options.
`filter_gait(data=RawGait, cutoff=6)` and `filter_gait(data=RawGait, cutoff=10)`
are two call sites.

**call_id.** A hash of a call site (scidb computes it). Constants are in it.

**Wiring.** The *shape* of a step, with constants left out: the function, which
variable type or PathInput feeds each argument, and which output types it
writes. Both call sites above share one wiring:
`filter_gait(data ← RawGait) → FilteredGait`. On today's canvas, one function
node is drawn per wiring, and its different constant values become **variant
rows** inside that node.

**wiring_id.** A hash of a wiring. scidb owns the recipe
(`scidb.provenance.compute_wiring_id`). If you rewire a node (for example, feed
it `RawGait_v2` instead), the wiring_id changes.

**Target.** What the GUI hands to `for_each` for one run: the bindings
(argument → source), the constant values and the output type. A Run button
press computes a list of targets.

**Recorded row / recorded target.** A target rebuilt from history: "this call
site ran, with exactly these typed constant values, these column selectors,
this pooling."

### Node identity (already built, 2026-09-22)

**Allocated id.** An id minted once (random hex) and then never recomputed.
Function nodes have used allocated ids since 2026-09-22, so that rewiring a node
doesn't change its id.

**`_node_wiring`.** The table that remembers which allocated node id ran as
which wiring_id. One row per (node, wiring), with first-seen and last-seen times.

**Claim / attribution.** Deciding which node a recorded wiring belongs to.

**Dispatch recording.** When you press Run on a node, the GUI writes the
`_node_wiring` row *before* running. The GUI knows the node id, so nothing has
to be guessed.

**Identity resolution / minting.** On every build, for each wiring in history
the GUI asks whether some node already claims it. If not, it **mints** a new
allocated id and records the claim. This is how a script-run step gets a node
id.

**Current wiring.** The latest wiring a node ran as. It decides which handles
and edges today's build draws for that node.

### How today's build assembles the canvas

**Build / graph build.** One call to `api/pipeline._build_graph`, which runs on
every refresh and returns the nodes and edges to draw.

**Derived (history-derived, DB-derived) node or edge.** One that today's build
*computes from history* on every build. If you ran `filter_gait` from a script,
nothing about that node is stored in the GUI's tables. It is rebuilt from
`_invocation*` each time.

**Manual / hand-placed node.** One you dragged onto the canvas. It is stored as
a row in `_pipeline_nodes` (`node_id, node_type, label, pipeline_id`).

**Drawn (manual) edge.** An edge you drew. It is stored as a `wiring` statement
in `_intent`.

**History edge.** A derived edge: today's build recreates it from what a run
recorded.

**Connection / connection_id.** What an edge *means*, independent of its id:
`e__<source>__<target>__<port>`, for example
`e__var:RawGait__fn:filter_gait:3f9a…__data`. A drawn edge and a history edge
with the same connection_id are the same wire. Since 2026-10-01, history edges
use the connection_id as their id.

**Hide.** Removing something from view without deleting it (the project's
never-delete rule). Removing a history edge stores a hide of its connection;
removing a node stores a hide of its id. Hides are per scope.

**Graduation.** Today, when a hand-placed node turns out to be "the same" as a
derived node, the build deletes the hand-placed row and moves its edges,
settings and position onto the derived node's id. This id swap is a
long-standing bug source (orphaned settings, lost positions).

**Overlay.** Today, if you draw an edge onto a node that came from history, the
build patches the derived node's displayed inputs to include your edge
(`overlay_manual_inputs`), and the run path patches the recorded targets to
match (`reconcile_manual_inputs`). There are two sources for one input, kept in
agreement by hand.

**Disconnected.** A function node is disconnected when one of its required
inputs has been hidden with nothing drawn in its place. It shows red and
refuses to run.

### Parameter values

**Declared value.** A value written in source code (`cutoff = Parameter([6, 10])`)
or in the entities TOML file.

**Recorded value.** A value that appears in history because a run used it.

**Pending constant (pending value).** A value you typed into a Parameter node
in the GUI that hasn't run yet. **Important existing behaviour:** a pending
value *overrides* recorded values when you press Run: the first pending value
replaces the recorded one (`apply_pending_overrides`). Once every node that
uses it has run with it, `auto_clean_pending_constants` deletes the pending
row, because the value is now in history.

**Cartesian product.** All combinations. With `cutoff ∈ {6, 10}` and
`order ∈ {2, 4}`, the product is 4 combinations.

**Non-Cartesian combos.** History that ran only *some* combinations, for
example only (6, 2) and (10, 4). Two of the four combinations never ran.

**Hidden combo.** A stored hide of one combination, by its call_id, so that
combination doesn't run. It already exists (`_pipeline_hidden_combos`, now in
`_intent`).

### Terms introduced by the proposal

**Document.** The stored canvas: all the GUI's rows that describe what is on
the canvas (`_pipeline_nodes`, drawn edges, Parameter value statements,
positions). "The canvas is a document" means *the picture is whatever is
stored, like a drawing file*, instead of being recomputed from history.

**Document row.** One stored node or edge.

**Adoption.** Copying something that history would have drawn into the
document as rows, **once**. After that it is an ordinary stored node or edge,
like one you drew yourself.

**`_adopted` marker.** A new table that remembers what has been adopted, so it
is never adopted a second time (see Step 1 below for why that matters).

**Subject (of the marker).** The thing a marker row is about: a node id or a
connection_id.

**Annotation.** Information history *adds to* a node that is already in the
document, without deciding whether the node exists: its colour (run state),
record counts, variant rows, the NOT REFLECTED chip. It is matched to the
node by id.

**Stated value.** (Proposed.) A Parameter value that is part of the document.
It stays even if history is deleted. This is different from a *pending* value,
which means "run this next, overriding history".

**Golden.** A saved snapshot of what a canvas looks like (Stage 0's
`canvas_shape`: nodes by type and label, edges by endpoints and port, values,
positions, colours), used to check that a change didn't alter the picture.

**Idempotent.** Doing it twice has the same effect as doing it once. Adoption
must be idempotent, because it runs on every build.

**Entity node / entity folding.** Variable, Parameter and PathInput nodes name
*one thing* (there is only one `RawGait` type), so two boxes for `RawGait` on
one canvas mean nothing extra. Today, a hand-placed `var__RawGait__a1b2c3`
folds into the canonical `var__RawGait`. That is one kind of graduation.

**Name-scoped vs node-scoped run.** Node-scoped means "run *this* node" (you
clicked it). Name-scoped means "run function `filter_gait`" with no node named,
which some paths use: the API without a node id, some MATLAB commands, variant
cards.

**Step name (Q6).** A proposed optional `step="filter"` argument to `for_each`,
so that a script run can say *which canvas node* it is. Not answered yet.

---

## Part 2: The running example

A gait project. `pipeline.py`:

```python
gait_files = PathInput("{subject}/{session}.csv", name="gait_files")
cutoff = Parameter([6, 10], name="cutoff")

for_each(load_gait, {"path": gait_files}, [RawGait])
for_each(filter_gait, {"data": RawGait, "cutoff": cutoff}, [FilteredGait])
```

You run it **from the terminal** (`python pipeline.py`, origin `script`). Then
you open the GUI for the first time.

The history now holds:

- records of `RawGait` and `FilteredGait`;
- `load_gait` ran with one wiring: `path ← gait_files → RawGait`;
- `filter_gait` ran with one wiring: `data ← RawGait → FilteredGait`, at two
  call sites, `cutoff=6` and `cutoff=10`.

The canvas you expect:

```
[gait_files] ──path──▶ (load_gait) ──▶ [RawGait] ──data──▶ (filter_gait) ──▶ [FilteredGait]
                                                    [cutoff: 6, 10] ──cutoff──▶ ┘
```

That is 6 nodes and 5 edges.

---

## Part 3: How it works today, and what goes wrong

### What is stored in the GUI's tables right now

| table | rows for this example |
|---|---|
| `_pipeline_nodes` (hand-placed nodes) | **none** |
| drawn edges (`_intent`, aspect `wiring`) | **none** |
| `_node_wiring` | 2 rows, minted on the first build: `fn__load_gait__aaaa… ↔ wiring L`, `fn__filter_gait__bbbb… ↔ wiring F` |
| positions | whatever the frontend saved after laying it out |

So **all 6 nodes and 5 edges are recomputed from history on every build.** The
Stage 0 log line already measures this. For a script-run project it says 100%
derived.

### The formula every build runs

```
canvas = (nodes and edges computed from history)
       + (your hand-placed nodes and drawn edges)
       − (your hides)
       then: graduation (merge hand-placed nodes into history twins)
       then: overlays (patch history nodes with edges you drew)
```

### What goes wrong, using the example

**A. "Delete the data, keep the pipeline" (clear data).** You clear the
records. Every node above came from history, so **the canvas is now empty.**
The pipeline you see was never stored anywhere. This is what prompted the plan.

**B. Two sources for one input.** You draw a *new* edge `RawGait_v2 → filter_gait.data`
onto the history node. Its displayed input now comes from history (`RawGait`)
patched by your edge (overlay). The Run button runs the *recorded* targets with
your edge spliced in (reconcile). Every bug in this area has been one of those
two patches disagreeing with the other.

**C. Graduation id swaps.** You drag a fresh `filter_gait` box from the
sidebar. It has a random id `fn__filter_gait__x7y8z9`. You set its run options
and run it. The build then decides it is "the same" as a history node and
**swaps** its id, and anything keyed to the old id is orphaned (D-2026-09-22-4,
still open).

**D. "Exists" is decided by history.** Whether a node is on the canvas at all
depends on whether history mentions it, filtered by hides, positions and
declared-only rules. Several separate bugs (phantom PathInput nodes, declared
Parameters strewn on a fresh canvas) came from that.

---

## Part 4: The proposed design, step by step

There are three steps, which match plan stages 2, 3 and 4.

### Step 1: Adoption (plan Stage 2)

**In one sentence:** the first time a build sees something history would draw
that isn't stored yet, it stores it, and from then on it is an ordinary stored
node or edge.

**With the example.** You open the GUI. The build computes the history-derived
canvas exactly as today (6 nodes, 5 edges). Then, for each of them with no
stored row, it writes one:

| written to | rows |
|---|---|
| `_pipeline_nodes` | `pathInput__gait_files`, `fn__load_gait__aaaa…`, `var__RawGait`, `fn__filter_gait__bbbb…`, `param__cutoff`, `var__FilteredGait` (all in scope `main`) |
| drawn edges | 5 edges, each with its connection_id as its id |
| Parameter values | `cutoff`: 6 and 10, as **stated** values (see below) |
| `_adopted` | one marker per node and per edge |
| `_canvas_change` | one row, operation `adopt` |
| GUI | a toast: "2 steps from a script run were added to the canvas" |

**Why use the same ids?** Because today's build is already built around those
ids. A stored row whose id equals a history node's id is simply "the same node"
(today's merge step already skips such rows). So **the very next build draws
exactly the same picture whether or not it trusts the stored rows**, and that
is checked with a golden: shape before adoption equals shape after. Nothing
shifts, no ids move, positions stay attached.

**Why a marker table, if the rows already exist?** Because of what happens when
you remove something. Suppose you delete the edge `cutoff → filter_gait`. If
the only rule were "adopt anything history draws that isn't stored", the next
build would look at history, see that edge, find no row, and **adopt it back**.
The marker says "this connection was already adopted once; don't do it again".
Anything you remove therefore stays removed.

**What isn't adopted:**
- Hidden nodes and edges. They aren't in the view, so they aren't adopted or
  marked. If you unhide one later, it is adopted then.
- Declared-only Parameters and PathInputs that are on no canvas (same rule as
  today).

**Parameter values: why a new "stated" state is needed.** The obvious move
would be to write history's values (6, 10) as *pending* values. That would be
wrong, because pending means "override": the next Run would replace the
recorded value with the first pending one and collapse the two variants into
one. So the proposal adds a second state to the same storage:

| state | meaning | example |
|---|---|---|
| pending (today's rows) | "run this next" | you type 14 into `cutoff`; it hasn't run |
| stated (new) | "this value belongs to the Parameter" | 6 and 10, adopted; or 14 after it has run |

Today, once 14 has run everywhere it is used, its pending row is **deleted**
(`auto_clean_pending_constants`). The proposal changes it from pending to
stated instead. The point is that after a clear-data, `cutoff` still shows 6,
10 and 14. Today it would show only the source-declared values.

**Non-Cartesian combos (your Q2 answer: hide the missing ones).** Suppose
`filter_gait` also had `order`, and history ran only (cutoff=6, order=2) and
(cutoff=10, order=4). The adopted node now states cutoff ∈ {6, 10} and
order ∈ {2, 4}. If it ever runs from values alone, it would run 4 combinations
instead of 2. So adoption also writes **hidden combos** for (6, 4) and (10, 2).
To do that it has to compute those combinations' call_ids from a recorded row.
If it gets one wrong (for example, mismatched run options), the effect is that
the combination shows up again, never that the wrong one is hidden.

**Adoption happens inside the build.** That sounds odd ("a refresh writes to
the database"), but today's build already writes: identity resolution mints ids
and records claims, and graduation deletes rows. It is not an undo step:
undoing it would just re-adopt on the next build. To get rid of an adopted
node, you hide it.

### Step 2: The build draws only stored rows (plan Stage 3)

**In one sentence:** once adoption has run, the build throws away anything that
isn't a stored row, and uses history only to decorate what's left.

The new formula:

```
canvas = (stored nodes and edges)
       − (your hides)
       + annotations from history, matched by node id
         (colour, record counts, variant rows, NOT REFLECTED chip)
```

**With the example**, on the second build: nothing new to adopt. The build
still computes the history-derived version internally, because that's where
colours and variant rows come from. But it now **keeps only nodes whose id is a
stored row, and only stored edges.** The picture is the same as before, since
everything was adopted.

**Where it starts to differ from today:**

| you do | today | proposed |
|---|---|---|
| delete edge `cutoff → filter_gait` | stored as a hide | stored as a hide (same) |
| delete an edge you drew yourself | the stored row is deleted outright; if history has the same wire, history redraws it | stored as a hide; nothing redraws it |
| clear data | canvas empties | canvas stays; nodes turn red/grey (no records) |
| drag a fresh `filter_gait` box and run it | may graduate (id swap) into the history node | stays its own node with its own id; its run is claimed at dispatch, so there is no twin to merge with |
| edit `pipeline.py` to feed `RawGait_v2` and run the script | the history node quietly re-shapes | a **second** `filter_gait` node is adopted beside the old one, with a toast (see Q6) |

One technical point I found at the end of the reading pass: when a drawn edge
and a history edge are the same connection, today's edge builder keeps the
**history** copy and drops the drawn one. So "keep only stored edges" has to
mean "keep edges whose *connection* is stored". Otherwise it would drop both.

**What becomes dead code** (deleted in plan Stage 5):
- function-node graduation (three passes in `_build_graph` plus its helpers);
- duplicate-edge cleanup after graduation (`drop_superseded_manual_edges`);
- overlays: a function node's handles come from its stored edges, as for a
  hand-placed node today.

**What I would keep, unlike the plan:** entity folding. If you drag a second
`RawGait` box onto a canvas that already has `var__RawGait`, they should still
fold into one. That rule isn't "history draws the canvas"; it's "one entity per
canvas". I would change it so it keeps a stored row for the canonical id
(today it only deletes the hand-placed row, which in the new world would leave
the node with no row and the filter would drop it).

### Step 3: Running from the document (plan Stage 4)

**In one sentence:** when you press Run on a node, the GUI works out what to
run from that node's **stored edges**, not from what it ran last time.

**Today**, for a history node: start from the recorded targets of its *last
recorded* wiring, then patch them with any edges you drew (reconcile), then
apply pending overrides.

**Proposed**, for every node:

1. Read the node's stored edges: `data ← RawGait`, `cutoff ← param__cutoff`,
   `→ FilteredGait`. That gives the **stated wiring**.
2. Look in history for recorded rows **of that exact wiring**. In the example
   there are two: cutoff=6 and cutoff=10.
3. If there are some, run those rows, plus pending overrides (as today).
4. If there are none (never run, rewired, or data cleared), build targets from
   the edges and the Parameter's values (pending, then stated, then declared).
   This path exists today for hand-placed nodes.

The overlay/reconcile patching disappears, because step 1 already uses only the
edges you see.

**Why step 2 still uses recorded rows (a deliberate difference from the plan).**
The plan says every node runs from "edges + values". That would rebuild the
constants from the stored *text* of each value. Two concrete problems:

- **Exact values.** History recorded `cutoff = 6.0` (a float). Its text is
  `"6.0"`, which reads back as 6.0, but a declared `6` reads back as the int 6.
  Ints and floats hash differently, so the call_id changes and *everything
  reruns* even though nothing changed. Array or dict constants are worse
  (memory: "constants are not scalars").
- **Things edges can't say.** A recorded run may have pooled variants
  (`AcrossVariants`) or used a column selector authored in Python. There is no
  edge or value for those yet.

Using the recorded rows *of the stated wiring* keeps a re-run of an unchanged
pipeline at "0 new records", and the stored edges still decide *what* runs. The
values only fill in exact details.

**Disconnection, restated for the document.** A node is disconnected if one of
its inputs has a hidden edge and no visible stored edge replaces it. It shows
red and won't run.

**Name-scoped runs** ("run function `filter_gait`") become "run every stored
`filter_gait` node", using the node-scoped logic above for each. The pipeline
compiler likewise runs the stored function nodes of its tab.

### Stage 6: MATLAB terminal runs

A MATLAB terminal run is a script run. Adoption doesn't look at where a run
came from: anything in history that isn't stored gets adopted. So nothing
MATLAB-specific is needed. It still needs a check with a real MATLAB run, which
you would do.

---

## Part 5: Where the proposal departs from the written plan

| # | plan says | proposal does | why |
|---|---|---|---|
| 1 | every node runs from edges + values | stored edges decide the wiring; recorded rows of that wiring supply exact values | avoids reruns from float/int and non-scalar mismatches; pooling and selectors have no edge form |
| 2 | adoption marker keyed by (scope, wiring_id) | keyed by node id and by connection_id | lets a deleted edge stay deleted even when its wiring is unchanged |
| 3 | Parameter value statements (unspecified) | a "stated" state beside "pending" in the existing `constants` storage | pending means "override"; reusing it would change what runs |
| 4 | delete graduation entirely | delete function-node graduation; keep entity folding | one box per variable type per canvas is still wanted |
| 5 | Q6: step names, option A + C | not built | not answered; without it, an edited script wiring adds a second node (with a toast) |
| 6 | delete `_db_path_input_params` | kept | follows from #1: reading recorded rows needs it |

## Part 6: The decisions that are yours

1. **Recorded rows supply exact values (#1).** Accept this, or require pure
   "edges + values" and accept reruns (or a separate project to store constants
   exactly)?
2. **Removing an edge you drew becomes a hide** instead of a real delete. This
   follows your never-delete rule, but it changes current behaviour and several
   tests.
3. **Keep entity folding (#4)?**
4. **Q6: build `step="name"` now**, or ship with "a changed script wiring
   adds a second node, with a toast"?

## Part 7: Risks, plainly

- **A build writes to the database** (adoption). Today's build already does
  this (identity minting, graduation), so it isn't a new kind of side effect,
  but it adds more of it.
- **Many existing tests assert today's behaviour** (graduation, edges deleted
  outright, edges coming back from history). They have to be changed, and I
  can't run them, so expect a round of fixes after you run the suites.
- **The second-node behaviour (Part 4, Step 2 table)** is visible to users and
  is correct only by the "script edits are new steps" reading. Q6 is the real
  fix.

---

## Part 8: What was built (2026-10-22), and where

Decisions taken after Part 6: D1 recorded rows supply exact values; D2
removing an edge (or node) hides it and keeps the row; D3 entity folding
kept; Q6 step names deferred.

| piece | where |
|---|---|
| the recorded step shape, non-Cartesian measure (Stage 1) | `scidb/src/scidb/wirings.py` (`recorded_wirings`, `RecordedWiring.missing_combinations`, `non_cartesian_report`); tests `scidb/tests/test_recorded_wirings.py`; CLI `python -m scistack_gui.canvas_shape_cli combos DB` |
| adoption rule (pure) | `scistack_gui/domain/adoption.py` (`plan_adoption`, `visibility_by_node`) |
| adoption writer | `scistack_gui/services/adoption.py` (`plan_for_build`, `write`, `missing_combo_hides`, `notification`) |
| the marker | `_adopted` in `node_wiring.py` (`adopted_subjects`, `mark_adopted`); not undoable (`history.UNTRACKED_TABLES`) |
| stated values | `intent_store.CONSTANT_STATED`, `state_constants`, `promote_pending_constant`, `stated_constants`; shown by `graph_builder.build_parameter_nodes(stated_values=)` |
| audit row | `history.log_event` (state `event`, never loaded as an undo record) |
| not an undo step | `history.is_recording()`: a build inside an undoable edit draws the plan (`document_view(pending=)`) and stores nothing |
| the document view (Stage 3) | `domain/adoption.document_view`, called in `api/pipeline._build_graph` after manual nodes are built |
| inputs from stored edges | `graph_builder.document_input_params` (replaced the overlay) |
| membership of stored rows | `scope_filter._resolve_in_scope` / `node_scope`: a qualified `{id}::{scope}` position, then a bare position, then the row's scope |
| run from the document (Stage 4) | `execution_service.derive_target_for_node` (stated wiring → recorded rows of it → else edges + values), `derive_fn_targets` (union over stored nodes), `document_disconnected_ports`, `ensure_document`, `_scope_function_node_ids` (reads the built canvas) |
| hides, never deletes | `layout_service.delete_edge`, `layout.delete_node` |
| deleted (Stage 5) | function-node graduation passes, `_wiring_conflicts_with_candidate`, `_find_db_fn_candidate`, `_own_state_for_function`, `drop_superseded_manual_edges`, `collect_manual_input_overrides`, `overlay_manual_inputs`, `reconcile_manual_inputs`, `HISTORY_WIRING_KEY`, `_apply_manual_output_types` |
| clear data (GUI half) | `services/clear_service.py`, RPCs `clear_data_plan` / `clear_data`, dialog `frontend/src/components/DAG/ClearDataDialog.tsx` ("Clear data…" in the canvas toolbar) |
| MATLAB terminal runs (Stage 6) | nothing MATLAB-specific: they are recorded history, adopted like a script run. Needs a real MATLAB run to confirm. |
| tests | `scistack-gui/tests/test_canvas_document.py`, updated `test_canvas_shape.py`, `test_graph_builder.py`, `test_api.py`, `test_pipeline_call_sites.py`, `test_pipeline_scopes.py`, `test_variant_resolver.py`, `test_constants_identity_gui.py` |

**Reading scidb.log.** `[adoption] plan: …` on every build (DEBUG when
empty), `[adoption] adopted step <fn> (<id>) into scope <s> with N edge(s)`
once per adopted step, `[adoption] step <fn> … ran k of n combinations`,
`[pipeline] document view scope=<s>: {...}`, `[pipeline] adoption deferred`
inside an undoable edit, `[execution] node <id> ('<fn>'): stated wiring <w>
… N recorded row(s) of it`, and `[execution] … disconnected: input port(s)`.

**Known gaps, recorded.** After a clear-data the fallback path has no
recorded selectors or `AcrossVariants` pooling (D1 follow-up). A script that
records a new wiring for an existing step adds a second node (Q6).

### Rules settled by the first full-suite run (2026-10-22)

- **Output rewiring.** A drawn output edge beside a recorded one ADDS an
  output; it no longer replaces it (the old `_apply_manual_output_types`
  rule). Visible edges are the outputs, so rewiring is: draw the new edge,
  remove the old one.
- **A hidden node's edges bind nothing** (`edge_view.effective_edges`): hiding
  a PathInput and drawing another on the same port runs the new one, not an
  EachOf of both.
- **A never-run stored node claims the recorded wiring it states**
  (`graph_builder.document_stated_claims`, node-identity §7a rule 1), so a
  hand-placed node whose exact wiring a script ran is that step, re-keyed into
  the id grammar. Graduation used to do this.
- **Duplicates are forks.** A duplicated tab's function nodes are new nodes
  with their own ids (no graduation onto the original's placement); entity
  boxes still fold into the canonical node. Their stated wiring matches the
  original's, so they run its recorded rows and show its colour.
- **Redrawing a removed edge under its own id unhides it**
  (`layout_service.put_edge`), even with no port.

### Follow-ups built 2026-10-22 (after v0.1.52)

- **Recorded settings travel with an adopted node**
  (`services/adoption.recorded_node_settings`): column selectors and the run
  options `distribute` / `as_table` / `across_variants`, so a run after a
  clear-data computes the same call. Only what every recorded row of the
  wiring agrees on.
- **Step names** (Q6 option A): `for_each(..., step="filter")` →
  `_run_step`; the canvas node whose step name is `filter`
  (`scistack_gui/step_names.py`, its config blob's `step`) claims that run's
  wiring even when the script changed it, and its old stored edges are hidden
  (`adoption.retire_rewired_edges`). Code export gives every exported node a
  step name and writes it; GUI runs pass it. Unnamed script runs still add a
  second node when they change a step's wiring.
- **Export round trip** is tested end to end (`tests/test_export_round_trip.py`).
