# What "green" means, and at what granularity

> Written 2026-09-13, ahead of the schema location picker
> (`.claude/plan-schema-location-picker.md`).
>
> **Status: implemented as `scidb/locations.py` (`location_states`) the same
> day; tests in `scidb/tests/test_locations.py` are written but have not been
> run.** The batch primitives named in §"Batching is the whole implementation"
> are real functions in `provenance_query.py`: `variant_keys_batch`,
> `current_records_by_schema_batch`, `latest_at_location_batch`,
> `superseded_batch`.
>
> Prerequisite reading:
> `bipartite-provenance.md` (the graph these all query),
> `schema-hierarchy-contiguity.md` (what a location *is*).
>
> The system has **three different green/red answers** living in three modules,
> each correct for its own question and none substitutable for another. They were
> written at different times for different callers and nothing states their
> relationship, so this document does. The picker needs a **fourth** granularity
> that did not exist — per (variable, variant, location) — and the point of
> writing this first is that the fourth must be *derived from* the same
> primitives rather than become a fourth opinion.

## The three that exist

| # | Question | Granularity | Answer | Home |
|---|---|---|---|---|
| 1 | Does this pipeline node need running? | per **function** (per call site) | `green` \| `red`, binary | `state.check_node_state` |
| 2 | Is this one output current? | per **(function, schema combo, branch params)** | `up_to_date` \| `stale` \| `missing` | `state.check_combo_state` |
| 3 | Is there data here at all? | per **(variable, schema_id)** | a count | `Inspector.schema_tree` / `_record` counts |

### 1. Node state is binary *on purpose*, and it is about the recipe

`check_node_state` is green iff **every expected invocation is present**, where
the expected set is derived live by `expected_invocations_for_function` — never
persisted (`_for_each_expected` was deleted precisely because a predicted id
that must later equal a separately realized id is a drift hazard).

Consequences that surprise people:

- Editing a function body changes `function_hash`, which changes every expected
  `invocation_id`, so the node goes red **even though every output record is
  still there and still valid**. Red here means "the current recipe has not been
  run on these inputs", not "your data is wrong".
- Grey/partial was removed. A node that ran 19 of 20 subjects is red, the same
  as one that never ran. This is the deliberate trade that makes node state
  cheap and unambiguous, and it is exactly the resolution the picker exists to
  restore — *which* subject, and why.
- A **PathInput-only loader's expected set cannot come from the graph**. With no
  DB input to enumerate, un-run combos leave no trace, so expected == realized
  by construction and the graph alone can never report one partially run. Since
  Stage 1c, `check_node_state` asks the **filesystem** as well
  (`state._discovery_gate`), so the badge a user sees does go red when files on
  disk have never been loaded. The graph's own answer is unchanged and still
  reads green there — the two are deliberately separate, and
  `test_the_graph_alone_still_cannot_see_the_shortfall` pins that.

### 2. Combo state is the per-location answer, and it costs

`check_combo_state(fn, outputs, schema_combo, branch_params)` is the only
existing per-location verdict. It is three-state, and its two halves have very
different characters:

- **`missing`** — `find_record_id` returns nothing for the combo. A pure
  presence test; exact and cheap.
- **`stale`** — either (a) the function's own hash differs from the stored one,
  or (b) `_has_superseded_ancestor` finds an ancestor record_id whose location
  now holds a newer record.

Half (a) is **not trusted for every function**: `trusts_hash = not hasattr(fn,
"hash")` excludes MATLAB proxies, whose hashing pipeline produces false
mismatches (`.claude/defer-function-hash-staleness.md`). Half (b) is a BFS with
per-ancestor DB round trips — correct, and unusable per-location across a study
(the N+1 rule in `batched-provenance-hot-paths`).

### 3. Record counts answer a different question entirely

`schema_tree` counts `_record` rows per `schema_id` across **all** variables and
**all** variants, excluding `excluded` rows. It answers "is this location
populated", which is not "is my variable correct here". It is listed because its
*tree-building* is reusable even though its *numbers* are not.

## The fourth: per (variable, variant, location)

This is what a scientist actually asks — "is `FilteredEMG`, as produced by the
variant I am looking at, good at subject 03 trial 7?" — and no existing call
signature expresses it. `check_combo_state` comes closest but is keyed on the
*function*, takes one combo, and knows nothing about a plotting-layer variant
selection.

### The four states, and why exactly these

| state | means | computed from |
|---|---|---|
| **green** | a record exists here for this variant, and every input it was computed from is still the newest at its own location | present ∧ ¬superseded |
| **amber** | a record exists, but something upstream has been **re-saved** since it was computed | the superseded-ancestor relation, set-wise |
| **red** | expected here, no record for this variant | expected − present |
| **grey** | deliberately excluded (`exclusions.exclude_schema`, with its reason) | `_schema_overrides` |

Two deliberate exclusions from this list, both load-bearing:

- **A function-body edit is NOT amber.** `check_combo_state` would call it
  stale; this granularity does not, for three reasons. It is untrustworthy for
  MATLAB functions (above); it is already the canvas's job, where the node goes
  red for exactly this; and the record-level decision was made and recorded —
  a lineage-path function-hash mismatch is a traceability fact, not staleness
  (`defer-content-staleness`). What the picker shows instead is the **version
  actually used per location** (`v1`/`v2` from `provenance_query.function_versions`),
  which is strictly more informative than a colour and is the same fact the
  variant-span banner reports (`plot-variant-rows.md` §3).
- **Grey does not count in the denominator.** An exclusion is a decision the
  user already made and justified; rendering it red would make their own
  bookkeeping read as a permanent defect, and the header verdict would never go
  green again on any real project.

This leaves amber as a **single, pure graph relation** — "an input record this
was computed from is no longer the latest at its location" — with no hashing,
no heuristics, and no per-function trust rules. That is what makes four states
as reliable as two.

### The denominator

`red` requires knowing what *should* be here, which is the same question
`check_node_state` answers, so it uses the same source and cannot disagree with
the canvas:

- variable-input functions → `expected_invocations_for_function` (live
  prediction over current input data, per variant config, scoped by `call_id`);
- **inputless / PathInput-only functions → `check_pathinput_node_state`'s
  should-run set** (`PathInput.discover()` ∩ grid − exclusions).

The second line is the interesting one: it is the only live source for "what
should exist" that a zero-input function has, and consulting it is what lets
either surface count the subjects that sit on disk and were never loaded.

**Both surfaces now use it.** The picker had it first (it opens on demand, so
the filesystem walk was cheap to justify); `state._discovery_gate` then brought
the same rule to `check_node_state`, which runs on **every canvas refresh** and
so needed three things the picker did not:

- a short TTL cache (`state.DISCOVERY_CACHE_SECONDS`) so a burst of refreshes
  collapses into one walk;
- a **credibility guard** — discovery finding nothing while the function has
  realized outputs means the walk is broken here (Windows separators read on
  POSIX, an unmounted drive, a moved data root), so the gate stands down rather
  than reporting an entire study missing;
- the discipline that it may only ever ADD red. Nothing in the gate can turn a
  red node green.

The escape hatch for a file you never intend to load is an **exclusion**, and
deliberately only that: there is no recorded grid to consult
(`_run.where_clause` is display-only by design), and an exclusion makes the user
write down why. It is the same mechanism the picker subtracts from its
denominator, so the badge and the pane agree by construction.

**The hole this does not close**: expected sets are derived one function at a
time, so a *downstream* variable inherits its loader's blind spot. If eight
subjects were never loaded, `FilteredEMG`'s expected set — predicted from the
`RawEMG` records that exist — never mentions them, and its picker reads `12/12`.
Propagating a loader's discovery set down the chain means mapping combos across
schema levels and is unbuilt; until then, integrity is checked at the loader,
which is where the missing data actually is.

- Records with **no producing invocation** (raw, manual, MATLAB direct saves)
  have no expected set at all. Their denominator is the locations where the
  variable already has records: every location reads green or grey, and `n/n` is
  honest only in the weak sense that nothing claims to know better. The picker
  must say so rather than imply coverage it cannot verify.

### Roll-up

A parent's dot is green iff no descendant is amber or red; its counts are
`green / (green + amber + red)` over all leaves beneath it, grey excluded. A
parent with **zero** non-grey descendants is grey itself, not green — green with
an empty denominator says "all of nothing is fine", which is not an answer. It is the
one arithmetic mistake that would make the header verdict meaningless.

Non-contiguous saves (`schema_tree`'s NULL-key path handling) put a
`speed`-keyed node directly under `subject`, as a sibling of `timepoint` nodes.
The tree carries `key` as well as `value` per node for exactly this reason;
rows must render the key name, not just the value, or two different dimensions
appear as one list.

## Batching is the whole implementation

Per-location status over a study is four set operations plus one propagation,
not a loop over `check_combo_state`:

1. **expected** — one call per producing function (already set-returning);
2. **present** — the `_find_record` latest-collapse per
   `(variable, schema_id, variant)`, in one query, masked by the variant
   selection;
3. **superseded** — build the upstream closure of *all* present record_ids at
   once (`_build_upstream_closure`), compute latest-per-location for every
   referenced rid in one query, mark the directly-dirty inputs, then propagate
   dirtiness forward through the closure edges in a single pass. Linear, and it
   reproduces `_has_superseded_ancestor` exactly — including its glue-chain rule
   (a virtual glue record has no `_record_save` row, so it must be walked
   through, never treated as superseded);
4. **excluded** — `exclusions.list_exclusions`.

The correctness anchor is that the batch result must agree, location for
location, with `check_combo_state` on a fixture holding one of each state — a
test that survives any later rewrite of either side.

## Where this must live

`scidb`. It is a statement about the provenance graph, and the GUI is one of at
least three callers (the picker, `scidb locations` on the CLI, and `scidb
report`). CLAUDE.md NOTE 3, and the standing invariant from
`observability-api-design.md`: the facade shapes and renders, it does not
compute a second opinion. If a question here cannot be answered from the
primitives, that is a gap in the graph model — not something to patch in a
webview.

## The fifth question: several variables at once

> Added 2026-09-14 with `intersect_location_states`, Stage 2 of
> `.claude/plan-schema-key-picker-and-level-order.md`.

A **function node** has no single variable. It has inputs, and the question it
asks is "where can this run?" — which is the INNER JOIN of its inputs' location
sets. `location_states` cannot answer it at any granularity, because every one
of its four states is scoped to one variable.

`intersect_location_states(variables, …)` runs `location_states` per variable
and merges:

| | |
|---|---|
| **which locations** | only those EVERY variable has. A location one input lacks is one the function cannot be called at, so it is absent — not present-and-red. |
| **what state** | the worst across the variables, so green-here-and-red-there reads red. |
| **except grey** | an excluded location stays grey. The exclusion is a decision the user made and justified; reddening it because some variable has no record there argues with them. |
| **`record_id` / `code_version`** | `None`. Several records sit at an intersected location and naming one would be a lie the renderer cannot qualify. |

Three consequences worth knowing before calling it:

- **Cost is linear in the inputs.** `location_states` measured 9.5 s on a
  419-location variable (2026-09-13), so a four-input node is four of those.
  Per-variable timings are logged (`[timing] intersect_location_states(A ∩ B):
  …`) precisely so "the pane is slow" can be answered with "because of input
  B", rather than guessed at.
- **One variable delegates**, returning `location_states`' own tree rather than
  a wrapper — so a one-input node costs exactly what it used to.
- **The variant applies to every variable.** A per-variable variant selection is
  the plotting layer's question; a node runs on whatever its inputs currently
  are.

Leaves, not parents, are intersected. Intersecting interior nodes as well would
keep a subject whose every trial was dropped — the tree is rebuilt from the
surviving leaves by the same `_build_tree` the single-variable path uses, so
the roll-up rules above hold unchanged.
