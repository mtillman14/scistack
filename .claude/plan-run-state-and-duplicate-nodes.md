# Plan: fix node colours and the duplicated grSides node

From `/workspace/scidb.log`, session of 2026-09-22 13:14–14:09.

Eight problems. Each one below says: what you saw, why it happens, what it
should do instead, and how to fix it.

---

## Problem 1: red doesn't travel downstream past an edge you drew yourself

### What you saw

`loadGaitRiteOneFile` went red. `grSides` and `calculateSymmetryOneVector`,
which both depend on it, stayed green.

### Why

Node colours are computed from the wiring **recorded in the database by past
runs**. Edges you draw by hand are added to the picture afterwards — about 200
lines later in `api/pipeline.py`, long after the colours are decided.

So when the colour calculator looks at `grSides`, it asks "what feeds
`grTableIn`?" and history answers "nothing" — because that connection only
exists as an edge you drew. No connection means no upstream to inherit red
from, so `grSides` stays green.

The numbers confirm it exactly. That build had 8 call sites + 7 variables =
15 nodes, of which 3 were red:

- `loadGaitRiteOneFile` (it has two call sites, both red)
- `GAITRiteLoaded`, the variable it produces

and then it stops. `grSides` reads `GAITRiteLoaded` through a hand-drawn edge,
so the chain breaks there. After grouping, the two `loadGaitRiteOneFile` call
sites merge into one node and the count becomes 2 red — which is exactly what
the log says.

This is the bigger version of a limitation already written down in
`docs/claude/manual-edges-on-history-nodes.md`:

> The run state of a node with an overlay still reflects its history (green)
> although the effective wiring never ran; a "needs run" badge is a follow-up.

That note is about the node's *own* colour. Nobody noticed that the same gap
also stops red from cascading to everything downstream.

### What it should do

The rule the project already committed to is: **the edges visible on the DAG
are the ground truth, for display and for execution.** Colour is a third
consumer of that rule and has to follow it too. If an edge is on screen, red
travels along it.

### The fix — DONE 2026-09-22, not yet run

New `graph_builder.input_params_with_manual_edges(...)` returns a COPY of
`fn_input_params` with manual variable edges folded in, and that copy goes to
the DAG cascade and nowhere else. It reuses `manual_input_overrides` — the
existing one owner of the rule — so there is no second answer to "what feeds
this handle".

Applied at **both** propagation passes, because there are two and each rebuilds
its input mapping from the recorded call sites:

- pass 1, per call site — `api/pipeline._build_graph` computes the overlay and
  hands it to `_compute_run_states(..., propagation_input_params=...)`;
- pass 2, on the grouped wiring — `group_call_sites_by_wiring` applies the same
  helper before its re-propagation. It gained three optional kwargs
  (`manual_edges`, `manual_nodes`, `hidden_edge_ids`) to do so.

No flag is needed to serve both: the helper recomputes the wiring id from each
entry, which for a call-site key derives the group it belongs to and for an
already-grouped key returns that key's own wid (members of a group share their
params, because the id hashes exactly those).

Two things deliberately **not** touched:

1. **Identity.** `wiring_id` hashes `input_params`, so folding an override into
   the dict node ids derive from would rename the node and orphan its saved
   position, scope and config (the placement-id lookup trap). The doc's rule —
   node identity does not change when an edge is drawn — is why this returns a
   copy. There is a test for it.
2. **The own-state check.** Pass 1 asks scidb "has this call site done its
   recorded work", and a drawn edge does not change that question. The overlay
   reaches the cascade only; `check_multiple_nodes_state` never sees it.

One supporting change: `propagate_run_states` now flattens a LIST binding.
A manual edge beside a still-visible history edge is an `EachOf`, and
`set(params.values())` raised on an unhashable list. Every source counts —
if any producer is red the consumer cannot be current. Empty strings (unwired
params) are dropped rather than treated as root variables; both spellings
already resolved to green, so nothing else changes.

`manual_nodes = _ps.get_manual_nodes(db)` moved up ~75 lines to sit beside the
manual-edge read, since resolving an edge whose source is a hand-dragged node
needs it and the overlay now runs earlier.

### Test

`scistack-gui/tests/test_graph_builder.py::TestRunStatePropagationFollowsManualEdges`
— five cases: the old behaviour pinned (red stops at the history edge), red
crossing the drawn edge and on downstream, identity untouched, no-manual-edges
short-circuits to the same object, and an EachOf list not breaking the cascade.

---

## Problem 2: grSides can never go green

### What you saw

`grSides: red — 130 expected invocation(s) not present`, unchanged after four
clean re-runs that each reported `completed=420, failed=0`.

### Why

To decide if a node is done, scidb lists the work it *should* have done and
checks each item off. It builds that list from the current records of the
input variable.

But "current records" is computed in three different places, and they don't
agree:

- the **load** path (what a run actually reads)
- the **display** path (what the variant labels show)
- the **colour** path (this list)

In 2026-09-14 a rule was added: if a function has been run under different
`distribute`/`as_table` settings, records from the older setting are stale and
get dropped. That rule went into load and display. It never went into the
colour path.

`loadGaitRiteOneFile` on your database has been run both ways. The log shows
load dropping the old set:

```
_find_record(GAITRiteLoaded, latest): 140 record(s) built under a superseded
    run-option set dropped (current: {'loadGaitRiteOneFile': 'distribute=true'})
... returned=450 rows
```

The colour path keeps those 140. It adds them to the "should have done" list.
`grSides` will never process them, because the run never sees them. So they sit
as permanently unfinished work, and the node is permanently red.

(My first reading blamed the 182 `NoData` combos. That was wrong — the list is
built only from locations where an input actually has records, so `NoData`
combos never enter it.)

### What it should do

**One rule for "which records count as current", used by load, display and
colour alike.** The list of expected work should be exactly the work a run
today would do. A node that has processed everything its inputs currently hold
is green, no matter what settings it was run under in the past.

### Confirmed 2026-09-22 by Problem 10's test

`TestNodeStateAgreesWithTheLoadPath` failed exactly as predicted: node state
counted **1** record the load path drops, saw **4** current records where load
sees 3, and the completed downstream step reported **1 invocation missing**.
The parity guards passed, so the diagnosis holds and legitimate coexisting
variants were never at risk.

### The fix — DONE 2026-09-22, not yet run

New `provenance_query.run_option_superseded_records(duck, record_ids, *,
inv_map=None, run_map=None) -> set[record_id]`. One owner for the **global**
per-function rule; both callers now ask it:

- `database._find_record`'s inline copy is replaced by a call, handing over the
  `inv_map` / `run_map` it has already batched (collapse hot path).
- `current_records_by_schema_batch` calls it and filters before its
  per-`(location, variant)` collapse.

Three deliberate boundaries, all written into the docstring:

1. **The per-LOCATION family rule stays in `_find_record`.** It is not
   duplicated anywhere — that caller's variant key splits the two option sets
   apart (a distributed run's `output_num` is the slice index), so it has to
   reconcile them within a location before the global rule applies.
   `current_records_by_schema_batch` keys more coarsely and needs only the
   global rule.
2. **Scope is the record's DIRECT producer**, matching the load path exactly.
   `variant_identity_batch` applies the same test over the whole upstream
   *chain* because it answers a display question ("may these coexist on
   screen?"), where an upstream flip does make a record stale. That difference
   is intentional and is now stated in the code; merging the two needs a
   decision about which question node state is asking, not a refactor.
3. **The helper logs at DEBUG.** Node state calls it per variable per canvas
   refresh, so an INFO line would be a per-refresh multiple — the shape that
   made scidb.log 24 MB. Callers that want a summary log their own once.

### Blast radius

The cheap gate (`run_option_axes`) returns empty for any database where no
function has run under two option sets, so nothing changes for the ordinary
case — including every existing location-picker test, which pins on constants.

One other consumer moves: `locations._present_by_location` (the location
picker). Its docstring already claims it uses *"the same latest-per-(location,
producing variant) collapse the load path uses ... so the picker and a
`Variant(...).load()` cannot disagree"* — it was silently wrong in the same
way, and this makes it true. Note the consequence: pinning a superseded run in
the picker now returns nothing rather than one stray orphan record. That is
more consistent, but it is a behaviour change with no test covering it. Worth
one before this is called finished.

### Follow-up noticed while fixing

`run_option_axes` logs at INFO on every call and is now called more often.
It was already per-latest-load; this roughly doubles it. Fold into Problem 8's
logging pass rather than changing another function's log level here.

### Test

`scidb/tests/test_run_option_variants.py` — run a function with
`distribute=False`, re-run with `distribute=True`, run a downstream function to
completion, assert the downstream node is green. Fails today.

---

## Problem 3: the duplicated grSides node

### What you saw

After a run, two `grSides` nodes.

### Why

Each function node's identity is a hash of how it's wired. Your `grSides` node
was wired by hand (`side` ← `Demographics`), giving hash `a50088e2…`. When it
ran, the run was recorded under the wiring it *actually used* — a different
hash, `f99f8a48…`.

The code notices this and migrates the hand-drawn edges and column selections
to the new node. What it doesn't do is retire the old one. So both stay:

```
13:50:39  wiring grouping: 8 call site(s) -> 7 node(s)     before the run
13:51:39  wiring grouping: 9 call site(s) -> 8 node(s)     after
```

Two knock-on effects, both visible in the log:

- **Every later run executes grSides twice.** Two `for_each(grSides)` passes
  per run, the second writing `0 new rows` — about 75 wasted seconds per run.
- **MATLAB holds the database lock across both passes**, so any refresh landing
  in between fails outright: 12 × `ERROR RPC << get_pipeline DB LOCKED`. A
  failed refresh leaves the old colours on screen, which is another reason the
  colours looked wrong.

Separately, the migration ran twice and wrote four edges where two were needed,
because the index it uses keeps only one edge per input handle and there were
already two.

### What it should do

**A wiring you replaced should stop being a separate runnable node.** It keeps
its history and stays inspectable — the project rule is hide, never delete —
but it appears as a *variant row inside* the new node rather than as a node of
its own. The GUI already does this for call sites that differ only by their
constants, so it's the same mechanism and the same visual, not a new concept.

Running the function once should run it once.

And migrating twice should be harmless: the second pass should write nothing.

### The fix

### It is two defects, not one — found while fixing

The plan assumed the double execution was a *consequence* of the duplicated
node. It is not. They are independent:

**3a — the MATLAB script runs every wiring of the name. DONE 2026-09-22.**
`matlab_command_service` chose the script's variants with `[v for v in
all_variants if v["function_name"] == function_name]` — by NAME. Meanwhile the
node-scoped derivation (`derive_target_for_node`) was computed fifty lines
below and spent only on variable bindings. The log shows both in one run:
`'side' restricted to "PareticSide" on 1 target(s)` (the derivation, correct)
and `_group_variants: 391 variant row(s) -> 2 for_each call(s)` (the script,
wrong).

So this is not really about duplicates. **Any** function with two legitimate
wirings on one canvas ran both — exactly the RawVO2/RawHeartRate bug
`derive_target_for_node`'s docstring describes as fixed. It was fixed for the
Python path and never for MATLAB; the duplicate node only made it visible.

Fix: `scope_variants_to_node(fn_variants, targets, node_id, function_name)`,
a named pure rule the generator defers to. Extracted rather than left inline
so the reasoning has somewhere to live and so it is testable without a
database. Falls back to the name-scoped list when the request names no node,
and when a node derives nothing at all (never run, no edges) — refusing to run
there would be a regression.

Two gaps close as a side effect, because `derive_target_for_node` is the only
derivation that applies them: **hidden constant values** and **manual-edge
reconciliation** now reach the MATLAB script. Before this, hiding a constant
value excluded a combo from a Python run and not from a MATLAB one.

Test: `test_matlab.py::TestScopeVariantsToNode` — five cases, including the
bug pinned through `_group_variants` itself (two calls before, one after) and
both fallbacks.

**3b — the superseded wiring stays as a second node. NOT DONE, and the
approach changed.** 2026-09-22.

The first design was to absorb the old node into the new one at display time.
Working through it with the user turned up that this treats the symptom: node
identity would still be derived from the recorded wiring, so the same class of
surprise returns in other forms. Two findings moved it:

1. **The harmful half is not the duplicate.** When the id moves, every
   statement keyed by it is orphaned — schemaLevel, runOptions, whereFilters,
   hidden values. Only `columnSelections` migrates, deliberately
   (`_migrate_column_selections`: *"The other saved settings were never applied
   to that run and stay where they were."*). The 2026-09-22 log shows the user
   re-setting `schemaLevel` on the new node six minutes after the run, with no
   indication their setting had stopped applying.
2. **It is a category error, not a bug.** A canvas node is an *intent* entity
   wearing a *fact* id. Written up as `docs/claude/node-identity.md` and
   decided as **D-2026-09-22-1** — facts get computed ids, intents get
   allocated ids; a function node gains an allocated id and `wiring_id` is
   demoted from identity to attribute.

So 3b splits again:

**3b-i — migrate ALL statements on supersession. DONE 2026-09-22.**
`_migrate_column_selections` became `_migrate_node_statements` and delegates to
`intent_store.rekey_subject`, which already existed for GRADUATION — the same
id change from the other direction, so no new mechanism was needed. Every
aspect moves, the old rows are deleted (one-shot), and `old_wins=False` keeps
anything the new node already has: at migration time it cannot have any, but a
repair path that can only ADD is the safer default if that assumption breaks.

Test: `test_api.py::TestManualInputEdgesOnHistoryNodes`, three cases added
beside the existing supersession end-to-end — every setting follows (not just
the columns), the superseded id keeps nothing, and a rebuild does not
overwrite what the user has since set on the new node.

This is a stopgap by design. Under D-2026-09-22-1 the id stops moving and
there is nothing to migrate; `plan-node-identity.md` Stage 6 retires it.

**3b-ii — the visible duplicate.** It falls out for free once D-2026-09-22-1
is implemented: the node *states* the drawn wiring before the run, the run
records that wiring, attribution rule 1 matches, and no second node is ever
created. The absorb-at-display-time patch is therefore **not worth building** —
it would repair a duplicate that the identity change stops forming. With 3a
fixed the duplicate no longer double-runs or holds the lock, so waiting costs
confusion rather than data.

Separately, still true and worth doing on its own:
`manual_edge_handle_index` keeps one edge per handle, so the supersession
migration wrote 4 manual edges where 2 were needed. Index all edges per
handle, and skip writing one already pointing at the target. This is a bug in
today's code regardless of which identity scheme wins.

**D-2026-09-22-1 and -2 are both settled** (`node-identity.md` §7): attribution
is recorded at dispatch and inferred only for script runs; ambiguity resolves
scope → history → age with a **GUI popup** warning; cross-database identity is
a non-issue because import already re-mints ids. Implementation is not gated.

Sizing note: the identity change is its own plan, not a step in this one. It
needs a new association table, an allocation + bootstrap path, the attribution
rule, a popup, and re-keying of every id-keyed store. It should be drafted as
`.claude/plan-node-identity.md` before any code.

---

## Problem 4: the canvas isn't told when a terminal MATLAB run finishes

### What you saw

Colours that don't update after a run until you poke something.

### Why

Four "refresh the canvas" messages in 55 minutes, against nine runs — and none
of them after a run.

When a MATLAB function is run through the terminal, the Python side hands the
script off and returns immediately. Every other run path ends by announcing
"records changed, go refresh". This one doesn't.

There's a new watcher (`matlab_run_watch.py`) meant to close this, but it also
only announces "run finished", not "records changed" — and it never ran in your
session at all, because your GUI is a separate clone that doesn't have it yet.

### What it should do

**Every path that can write records ends the same way**, by announcing it.
No path-specific exceptions.

### The fix — DONE 2026-09-22, not yet run

`matlab_run_watch._finish` now calls `_notify_records_changed()` after
delivering its verdict — on **every** verdict, not just success. The message
means "records may have changed", which is true of a run that failed halfway
(it saved what it got to) and of one we cannot classify. Announcing too often
costs a refetch; announcing too rarely is the bug. Wrapped so a failed
announcement can never bury the verdict: a missed refresh is a stale canvas,
not a lost run.

**`_notify_records_changed` gained a third job** while in there:
`scidb.state.clear_discovery_cache()`. That function's docstring has said
"For tests, and after a run" since it was written, and only the tests ever
called it. A loader's discovery cache holds "files on disk minus locations
already realized" — a run changes the second half while the filesystem sits
still, so nothing but the 5-second TTL ever refreshed it. Putting it here
fixes every run path at once rather than just this one.

Checked while wiring it: the two `push_message` functions do converge.
`matlab_run_watch` pushes through `notify.push_message` (JSON-RPC) and
`api/run` binds `ws.push_message` at import, but `ws.push_message` delegates to
the JSON-RPC one when the extension host is driving. No routing gap — worth
recording because `project_plot_panel_notification_routing` is a case where
there was one.

Not every `run_done` needs this. The two pre-execution refusals in `api/run`
(function not found, no targets) never ran anything, so there is nothing to
announce, and the test below is scoped so it does not demand it of them.

### Test

`scistack-gui/tests/test_matlab_run_markers.py` — three:

- `test_every_verdict_announces_that_records_changed`, parametrised over
  success / failure / unknown, also asserting the verdict is pushed *before*
  the refresh so the refetch cannot race the run's own result;
- `test_the_announcement_never_buries_the_verdict` — the notify call explodes,
  `run_done` still goes out alone;
- `test_no_run_completion_path_here_forgets_to_announce` — AST, following the
  pattern already in that file. Any function in the module that emits
  `run_done` must also call `_notify_records_changed`, and there must be
  exactly one such function. This is the shape the gap had: a new completion
  path written without carrying the announcement over.

---

## Problem 5: 97% of the log is one repeated line

### What you saw

A 24 MB log file.

### Why

57,342 of 58,276 lines are this, once per database record:

```
matlab_param_to_class: fn=loadGaitRiteOneFile output_num=N is out of range
    for its 1 declared output name(s) ['grTable'] — DB source skipped
```

It is not just noise. `output_num` means two different things:

- for an ordinary function, which output slot a record came from
- for a **distributed** run, which slice of the output a record is

The GUI only knows the first meaning. So for every distributed MATLAB function
it looks up slot 450 in a list of 1 output name, fails, and gives up. The
function's own comment says what happens next: the node draws one output
handle, the edge points at a different one, React Flow silently drops the edge,
and **the function looks disconnected from an output that is marked green**.
That is a colour symptom too.

### What it should do

Read `output_num` as an output slot only where it *is* one. And log a summary
per function per build, not a line per record.

### The fix — DONE 2026-09-22

Simpler than the plan assumed, and the log said why: **every one of the
100,758 flood lines is for a function declaring exactly ONE output.** With one
declared output there is no slot to guess — the single name maps to the
variant's `output_type` and `output_num` must not be consulted at all. That
one condition removes 100% of the flood and populates `from_db` for all seven
MATLAB functions in the affected project.

No `distribute` flag is needed. `distribute` is not the only case where
`output_num` is not a slot — a batch loader sharing one invocation across runs
gives a re-run's record the next free slot too (`provenance_save`, Fix B) — so
keying on it would have fixed one cause and left the other.

The original rule ("fall through rather than guess a slot") is kept where it
means something: a genuinely multi-output function with an out-of-range number
still contributes nothing, and now says so **once per function at DEBUG**
instead of once per record at INFO. Conflicting types under one declared
output warn and resolve deterministically.

`variant_resolver`'s 390 identical lines per run are now one line per wiring
with a target count. It stays at INFO deliberately — its *absence* is the
documented diagnostic for "the run ignored the edge I drew"
(`manual-edges-on-history-nodes.md` §Reading scidb.log).

Root cause of the original mistake, fixed in the same pass: the
`list_pipeline_variants` docstring called `output_num` *"0-based position in
the fn signature"*. It now says what it actually is, and names the two cases
where it is not a signature position.

### Tests

`test_matlab.py::TestMatlabParamToClassFromDb` — two tests that encoded the
old single-output behaviour were rewritten (with the reasoning for the
reversal); five added: the number is ignored for one declared output whatever
it says, a 450-slice distributed run maps cleanly and logs **nothing**,
conflicting types warn, a multi-output fn still refuses to guess, and the
unmapped summary is one line per function.

---

## Problem 6: two loader nodes permanently say "can't tell"

### What you saw

```
WARN node loadDemographics: PathInput discovery found no files at all …
     Check the data root is reachable and that scistack.toml's paths resolve
     on this OS.
```

44 times, for `loadDemographics` and `loadFunctionalOutcomes`.

### Why

The file search actually **succeeded** — it found one match. These two
PathInputs point at a single fixed spreadsheet with no `{placeholders}` in the
path.

A path with no placeholders produces a match with no schema keys — an empty
dict. And the counting code skips empty ones:

```python
if c and key not in seen:      # an empty match is falsy, so it's dropped
```

So the count comes out zero, which trips a safety check meant for a genuinely
broken path (unmounted drive, Windows path read on a Mac). Those two nodes then
report "can't tell" forever. The warning's advice is also wrong — the path
resolved fine.

### What it should do

**A path with no placeholders describes exactly one location, and it counts.**
The safety check should distinguish "the search returned nothing" from "the
search returned a location with no keys".

### The fix — DONE 2026-09-22

Two changes in `scidb/state.py`:

1. `check_pathinput_node_state._add` admits the keyless combo. The dedup key
   `()` is already unique, so only the `if c` falsiness test had to go. The
   downstream semantics were already right and needed no change:
   `_is_realized({})` is `any(...)` over an empty key set — true iff the
   function has realized ANY location, which is exactly the question for a
   single fixed file. So the node reads red before its first run and green
   after, with no special case.
2. The result now carries `discovered` — the RAW number of matches the walk
   returned, before the grid intersection and before exclusions — and
   `_discovery_gate` bases its credibility check on that instead of on how
   many survived. "The walk found nothing" (unreachable root, Windows path on
   POSIX) and "everything found was excluded" are different situations, and
   only the first should disable discovery.

Warning reworded to "matched nothing on disk", which is what it now means.

### Tests

`test_locations.py::TestKeylessPathInput` — a template naming one fixed file:
the match counts, never-run is red, after the run it is green, the credibility
guard does not fire, and (the case the guard exists for) an unreachable root
still trips it.

---

## Problem 7: saving a plot fails once a variable has two variants

### What you saw

Three `save job … failed` at 14:07–14:09.

### Why

The filename is built from the figure's label, and for a variant fan-out that
label contains the entire `gaitRiteConfig` dictionary. The result is a
630-character path; Windows allows 260. It fails with `FileNotFoundError`,
which reads like a missing folder and isn't.

It only started failing now because Problem 3 gave `GAITRiteLoaded_UA` a second
variant. Single-figure saves earlier the same day all worked.

### What it should do

A save either writes a usable file or fails with a message that names the real
cause. Filenames stay **short**, nothing is silently overwritten, and what each
file holds is recorded somewhere durable (user, 2026-09-22).

### The fix — DONE 2026-09-22

**Numbered, not slugged.** A fanned-out save writes `<stem>_v1`, `<stem>_v2`, …
The number is keyed on **(plot settings, variant)** — the user's own framing:
a figure is the same figure only if both match. `PlotSpec.to_dict()` is the
normalised settings form and `scicanonicalhash.canonical_hash` is the project's
one hasher, so the key needed nothing new.

Consequences of that key, both wanted:
- re-saving the same figure after a re-render **reuses its number**, so the
  file already referenced in a talk updates in place instead of multiplying;
- changing any setting **takes a new number**, so a tweak never silently
  overwrites the figure someone already has.

**A sidecar manifest, not a database row.** `<stem>.figures.json` beside the
images records `{n, file, key, figure label, kind, full settings, saved_at}`
per figure. Deliberately not the database: these files go into a talk or a
paper directory and get copied around, and *"what is v2?"* has to be
answerable months later without the right database open. One manifest per
(directory, stem), so the same database saving to two folders keeps two
independent numberings.

**A lost manifest cannot cause an overwrite.** Numbering floors on the `_v{n}`
files actually present in the directory as well as on what the manifest
remembers, so deleting or corrupting it costs the descriptions, never the
figures.

**Two guards behind that.** `_slug` is capped at 48 characters with a
6-hex digest appended when truncated, so two long labels sharing a prefix
cannot collapse onto one name; and `_path_too_long` refuses an over-long path
*before* rendering, with a message naming the real cause. `savefig`'s own
`FileNotFoundError` reads as "the folder is missing", which is why the real
cause took a traceback to find.

**Trade-off accepted:** `emg_v1.png` is less self-describing than
`emg_subject_1.png` was. Short won because the label is unbounded and the
manifest is a better answer than a filename ever was — it carries the full
settings, not just a label.

### Tests

`test_plot_service.py` — two tests asserting the old slugged names were
rewritten; seven added: the manifest names each file, re-saving reuses the
number, changed settings take a new one, a deleted manifest does not
overwrite, a single named save keeps its name and writes no manifest, the slug
cap keeps long labels distinct, and an over-long path is refused with a
message that does not say "not found".

---

## Problem 8: small things

**Every project MATLAB function is registered twice. DONE 2026-09-22.**
The shadow check compared file paths with `==`, so `y:\…\grSides.m` and
`Y:\…\grSides.m` looked like two files: 20 functions registered twice, each
with a WARN telling the user their own code shadowed itself.

Fixed at the existing owner rather than in the registry. `config._same_path`
was already the comparison for this family — it was written for the
mapped-drive-vs-UNC form — so the registry now calls it. But `_same_path`
alone was not enough: its `_identity_key` test returns `None` when a
filesystem reports `st_ino == 0`, which a mapped SMB share frequently does,
and that is exactly the setup where this bit. `_same_path` gained an
`os.path.normcase` comparison — lowercases on Windows, identity on POSIX — so
two spellings compare equal precisely where the OS says they are one file.

Tests in `test_config.py` assert the *platform's* rule rather than one
platform's answer, so they mean the same thing on macOS and on Windows.

**The directory-listing cache never hits. DONE 2026-09-22.**
`0 served from the listing cache` on all 79 searches. `_dir_cache` lived on
the PathInput object and the canvas rebuilds those every refresh, so it was
born empty every time — ~344 network directory reads per refresh.

Moved to module level (`pathinput._DIR_CACHE`), with `clear_listing_cache()`
exported. Safe to share because every entry is mtime-validated on read: a
stale listing can never be served, only re-read. That is also why no
invalidation hook is needed after a run — directory mtime changes when entries
appear or vanish, which is all discovery looks at.

`test_pathinput_discover_cache.py` had a test asserting the old behaviour
verbatim ("The cache is per instance — a new PathInput knows nothing"); it now
asserts the opposite, with a sibling proving a changed directory is still
re-read across instances. An autouse fixture clears the cache between tests
now that it outlives them.

**A canvas refresh takes 5.7–21 seconds** (median 9.8). Mostly the two items
above plus Problem 2's work. **Still to re-measure** — the next `scidb.log`
from a real session is the measurement; do not optimise further on guesses.

**Not a bug — leave it alone.** Colours are computed twice per refresh (9 call
sites, then 8 after grouping). That's deliberate: the second pass exists so a
staged constant value cascades downstream. It looks like a race and isn't.

---

---

# Part 2: stop this class of bug recurring

Problems 2, 3, 5 and 7 all have one shape: **several places independently
derive one fact, and agree only by convention.** When one of them learns a new
rule and the others don't, nothing fails loudly — the answers just quietly
diverge.

This is already a named pattern in the codebase. `docs/claude/variant-space.md`
§4 is called *"Four answers to 'which variant is this record?'"* and lists four
functions, defending them as answering genuinely different questions. Three of
the four are supersession-ish and are meant to agree about staleness. Problem 2
is two of those three disagreeing.

And `scidb/tests/test_identity_parity.py` opens with:

> Every bug of the 2026-09-19 session had one shape — two derivations of one
> fact, agreeing by convention — and this is the test tier that would have
> caught each of them at authoring time.

That tier exists and covers three identities: `invocation_id`, `call_id`,
`selector`. "Which records are current" is a fourth identity of the same shape
and is not in it. That is why Problem 2 survived.

Two pieces of work follow: a way to **see** variants (Problem 9) and a way to
**catch** divergence automatically (Problem 10).

---

## Problem 9: you can't see a variable's variants in a form that explains them

### What you have today

`scidb variants <name>` exists. It returns one row per call site:
`function_name`, `call_id`, `output_type`, `output_num`, `input_types`,
`constants`, `record_count`, `run_options`.

It answers "what call sites produced this type". It does not answer the
questions you actually have when something looks wrong:

- **No time axis.** No first/last saved, so no chronology.
- **No locations.** Just a count — you can't see *where* a variant ran.
- **No code version.** `fn_hash` / `fn_version` aren't in the summary.
- **Grouped by `call_id`**, which folds in constants and run options, so two
  rows that are the same topology run two ways look unrelated.
- **No verdict** — and this is the important one. It shows you what exists. It
  does not show you which of those records a load would actually read, so a
  view of Problem 2's database would have looked entirely reasonable.

`Inspector.provenance` (the 2026-09-15 work) is the complementary tool and has
the same blind spot from the other direction: it is *top-down*. You pin a
variant and it traces where that one came from. It can't tell you a variant you
didn't know about exists — and "why are there more variants than I expected"
is the question that keeps coming up.

### What it should do

Two levels, exactly as described:

**Level 1 — topology, chronological.** One entry per distinct DAG shape that
has ever produced this variable, oldest first. Topology means
`(function_name, input_types, output_type)` — the connections, independent of
which constants or run options were used, and independent of schema location.

**Level 2 — within each topology, the variants**, each with when it ran, how
many records, where those records sit, and **what the system currently thinks
of it**.

Sketch of the output:

```
GAITRiteLoaded — 1 topology, 2 variants

topology  loadGaitRiteOneFile(gaitRitePath: PathInput, gaitRiteConfig: dict)
                                                          → GAITRiteLoaded

  [1] run=distribute=false   code v1   records 560
      first 2026-09-14 11:02   last 2026-09-19 16:41
      load:  SUPERSEDED (older run-option set)
      state: COUNTED AS EXPECTED          ← these disagree
      420 locations   subject×session×speed
                      SS01/BL/SSV, SS01/BL/FV, SS01/MID24/SSV, … (+417)

  [2] run=distribute=true    code v1   records 450
      first 2026-09-22 13:49   last 2026-09-22 13:50
      load:  CURRENT
      state: COUNTED AS EXPECTED
      450 locations   subject×session×speed×trial
                      SS01/BL/SSV/1, SS01/BL/SSV/2, … (+448)
```

The `← these disagree` line is the whole of Problem 2, visible in one command.
Everything above it is context you'd want anyway.

### Design decisions

**Do not add a `wiring_id` to scidb.** The GUI has one
(`graph_builder.wiring_id`, from fn name + input params + outputs + path
inputs); scidb's nearest equivalent is `call_id`, which is a different thing
(it folds in constants and run options). Inventing a second topology hash in
scidb would be *exactly* the duplicated-derivation pattern this work exists to
stop. Group by `(function_name, input_types, output_type)`, all of which
`VariantSummary` already carries. `input_types` already renders PathInput
params as their spec string, so the grouping covers them.

**The verdict column depends on Problem 2's fix.** It needs a function that
answers "would a load read this record, and does node state count it" — which
is precisely the one-owner helper Problem 2 extracts. So Problem 2 lands first
and this reads its helper; the view must never compute a third opinion of its
own.

**Locations are summarised, not dumped.** Show the key tuple shape and the
first few, with a count. A full listing goes behind `--locations` (and `--json`
always carries everything).

### The fix — Python + CLI DONE 2026-09-22; GUI panel outstanding

1. **`provenance_query.pipeline_variants`** now carries `first_saved`,
   `last_saved`, `schema_ids`, `function_hash`, `current` and
   `current_record_count`. Added in one batched pass
   (`_annotate_variants`): one query for the save log, one for the schema
   ids, one supersession sweep over every record at once — the per-record
   shape would be an N+1 on a canvas path.
2. **`VariantSummary`** gained the same fields, each documented with the
   question it answers.
3. **`Inspector.variants`** now sorts **chronologically** (was output/fn/slot
   — stable but answering no question). New **`Inspector.topologies`** groups
   by `(function_name, input_types, output_type)`, oldest first.
4. **`render.render_topologies`** is the two-level view;
   `render_variants_table` stays for `--flat`.
5. **`scidb variants <name>`** is two-level by default, with `--flat` and
   `--locations`.

**The verdict column is the point.** `load: CURRENT` /
`PARTIALLY SUPERSEDED (n of m still returned)` / `SUPERSEDED` reads straight
off `run_option_superseded_records` — the one owner Problem 2 created. A
variant node state counts and a load drops is invisible in every other view,
and is exactly the 2026-09-22 bug. Partial supersession is kept rather than
rounded to a boolean because run options are judged per function *globally*,
so a variant can lose some locations and keep others — which is the trial-4
orphan that started all this.

**No wiring id was invented.** The GUI has one and scidb's nearest equivalent
(`call_id`) is a different thing — it folds in constants and run options. The
grouping uses fields already present, so this adds no third answer to "which
node is this" (`node-identity.md`).

The `output_num` docstring was corrected under Problem 5.

**Outstanding:** the GUI panel. `Inspector.topologies` is the one
implementation and the CLI was built first, so the panel is a thin shell over
the same method — `services/provenance_service.py` is the pattern. Planned
separately as **`.claude/plan-topologies-panel.md`** (4 stages: service+RPC,
panel, canvas context-menu entry, docs), because it needs a frontend component
that has to be looked at rather than only tested.

### Tests

- `test_inspect_pipeline.py::TestTopologies` — two constant variants are ONE
  topology, every variant appears exactly once, chronological order, locations
  present, unsuperseded reads current, the renderer names the shape and the
  verdict.
- `test_run_option_variants.py::TestTheVariantsViewShowsTheSupersession` — on
  the database that motivated it: the older run-option set is marked
  not-current, both runs are still one topology (the *shape* never changed),
  the rendered view distinguishes them, and the orphaned-trial case reports
  partial supersession.

---

## Problem 10: nothing checks that the "which records are current" rules agree

### What it should do

`test_identity_parity.py` already has the right shape for this: run something
for real, reconstruct the fact from two directions, assert the same bytes. Add
a fourth identity to it.

> **current records** — does the set of records the load path returns equal the
> set node state counts as expected work?

### Status: WRITTEN 2026-09-22, not yet run

Two files, because the reproduction and the invariant want different homes.

**`scidb/tests/test_run_option_variants.py` — the reproduction.**
New class `TestNodeStateAgreesWithTheLoadPath`, placed directly after
`TestCurrencyIsPerFunctionNotPerLocation`, which already pins the load and
display paths against the same seven records. It reuses that class's
`stale_trial` shape — whole-file run over trials 1-4, distributed re-run
producing three slices, trial 4 orphaned — because that is the production
shape in miniature. Three tests:

- `test_node_state_counts_no_record_the_load_path_drops` — the inclusion.
- `test_the_orphaned_record_is_the_one_it_counts` — expects 3 current records,
  should get 4 today. Names which one, so the failure is legible.
- `test_a_downstream_step_that_ran_completely_is_green` — the symptom: a
  downstream step runs over all three trials the load offers, nothing fails,
  and the node still reads red.

**`scidb/tests/test_identity_parity.py` — the invariant.**
New `TestCurrentRecordsAgreeAboutStaleness` plus a reusable
`assert_current_records_agree(db, type_name)` helper, and a fourth bullet in
the module docstring beside `invocation_id` / `call_id` / `selector`. These
cases should **pass today**: they are the guard that the Problem 2 fix does not
over-correct. Coexisting variants (`Scaled` at factor 2 and factor 3) are
legitimate and both sides must keep all sixteen; an ordinary re-save must
supersede on both sides. A supersession rule made too aggressive would break
these, and that is the likeliest way to get Problem 2 wrong.

**Why an inclusion, not an equality.** The two keys differ in scope by design
(variant-space.md §4): the load path keys on `(fn_name, branch_params,
consumed input locations)`, node state on the directly producing invocation's
constants, one hop. They are not obliged to partition records identically.
They are obliged to agree about which records are *dead*, because every record
node state counts and the load path drops becomes an expected invocation that
can never be satisfied. Asserting equality would fail for reasons unrelated to
this bug.

### Expected result on first run

`test_run_option_variants.py` — the three new tests fail.
`test_identity_parity.py` — the three new tests pass.

If the parity ones fail, the diagnosis is wrong somewhere and Problem 2 needs
re-reading before any fix.

Follow the file's existing convention for the rest of the supersession family:
where two rules are known to disagree and the decision isn't made yet, pin it
`xfail(strict=True)` with the disagreement stated, so it's an executable to-do
rather than a comment that rots.

### Why this and not more unit tests

Each rule already has unit tests, and every one of them passes. They pass
*because* they're tested in isolation — which is the same reason the rules
drifted. Only a test that compares two owners can catch two owners diverging.

---

## Order of work

1. **Problem 10's test first** — it should fail. That's the reproduction, and
   it's the thing that proves Problem 2 is fixed when it goes green.
2. **Problems 1 and 2** — together these are "the colours are wrong". Problem 1
   is why red doesn't spread; Problem 2 is why grSides is red at all. Neither
   depends on anything else. Problem 2 also produces the one-owner helper that
   Problem 9 needs.
3. **Problem 4** — "the colours don't update". Small and independent.
4. **Problem 3** — the duplicate node. Also unblocks Problem 7.
5. **Problem 5** — correctness plus the 24 MB log.
6. **Problem 9** — the variants view. After 2 (needs its helper) and ideally
   after 5 (shares the `output_num` correction).
7. Problems 6, 7, 8.

A note on sequencing: Problems 9 and 10 are the ones most likely to prevent the
*next* session like this one, but neither fixes anything you can see today. If
the priority is getting the GUI trustworthy again, 1, 2 and 4 are the short
path and they're independent of everything else here.

## Documentation to update

- Problem 1 → `manual-edges-on-history-nodes.md`: colour is a third consumer of
  the visible-edges rule. Closes the Known limitation about overlay run state.
- Problem 2 → `run-option-variants.md`: one current-records rule.
- Problem 3 → `manual-edges-on-history-nodes.md` §Lifecycle: the retirement rule.
- Problem 4 → `matlab-run-completion.md`, `gui-notification-routing.md`.
- Problem 5 → `matlab-output-handle-contract.md`: the two meanings of `output_num`.
- Problem 9 → `variant-provenance-introspection.md`: the bottom-up view beside
  the existing top-down one. Cross-link from `variant-space.md` §4, which is
  where someone looking for "how many answers are there?" will land.
- Problem 10 → the header comment of `test_identity_parity.py`, listing the
  fourth identity beside the three already there.

Problems 1, 3, 4, 7 and 9 are GUI-visible, so they need entries in
`docs/gui-manual-testing-todo.md`.

## Commands

Python isn't available in the agent's environment — run these in a terminal
with the project venv active.

Confirm Problem 2 before changing code. The first number should be much bigger
than the second if the diagnosis is right:

```
python -c "
from scidb import configure_database
from scidb.provenance_query import current_records_by_schema_batch
db = configure_database(r'C:\Users\mtillman\Datasets\Stroke Aim 2\Stroke-R01-Aim2.duckdb', ['subject','session','speed','trial','cycle'])
pred = current_records_by_schema_batch(db._duck, 'GAITRiteLoaded')
print('colour path sees:', sum(len(v) for v in pred.values()), 'records at', len(pred), 'locations')
"
```

```
scidb show GAITRiteLoaded --count
```

Tests, one package at a time:

```
cd /workspace/scidb && python -m pytest tests/ -x -q
cd /workspace/scistack-gui && python -m pytest tests/ -x -q
```

---

## Problem 11: a distributed run is reported as one variant per slice

Found 2026-09-22 while building Problem 9's view — which is the point of that
view, but it is not fixed.

### What it is

`provenance_query.pipeline_variants` includes **`output_num` in its group
key**. `output_num` names which output of the invocation a record is, so:

- an ordinary multi-output call (`outputs=[A, B]`) splits into two groups —
  but those already differ by `output_type`, which is in the key too, so
  `output_num` contributes nothing there;
- a **distributed** run emits one record per slice, each with its own
  `output_num`, so one call becomes N variants;
- a batch loader that shares an invocation across runs takes the next free
  slot each time, so re-runs also split.

`output_num` therefore only ever splits things that are not different
variants. On the 2026-09-22 database `loadGaitRiteOneFile` has ~21,000 variant
rows for what a user would call two — that is the same count that produced
Problem 5's 100,758-line log flood, and it is the literal answer to "why does
this variable have more variants than I expected".

### What it should do

One variant per genuine variant: `(output_type, function, inputs, constants,
glue, run options)`. A distributed run is ONE variant holding N records at N
locations — which is exactly what `schema_ids` and `record_count` already say.

### The fix — DONE 2026-09-22

`output_num` dropped from the group key in `pipeline_variants`. Still
**reported**, as the LOWEST slot in the group rather than the first seen, so
the number is deterministic instead of row-order dependent — and for the only
case where it still means anything (a multi-output call, where each
`output_type` is its own group) that is the type's own slot.

Nothing else moved. `call_id` is computed from `CallSite(fn_name, inputs,
constants, options, glue)` and never included `output_num`, so call-site
identity is untouched and `test_identity_parity.py` was never at risk. What
changes is how many groups share one call_id: N, now 1.

The two consumers checked first both hold:
- `inspect/graph.py` builds `outputs: {output_type: set(output_num)}` — a
  set of one per type now, which is what it wanted;
- `database.py`'s aggregation sums `record_count` across variants, so N
  one-record groups and one N-record group give the same total. Its comment
  claiming `output_num` is "the only thing that tells the GUI which slot
  produced this output" is now qualified: true for multi-output, and Problem 5
  already made single-output functions ignore it.

### Tests

`test_run_option_variants.py` — the pinned 6 became 2, with a sibling
asserting the distributed variant still holds all its slices (grouped, not
dropped), and a new multi-output case proving the slot still distinguishes two
declared outputs. `test_variant_queries.py`'s two `output_num` contract tests
are multi-output and unaffected by construction.
