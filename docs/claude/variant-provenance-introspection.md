# Variant provenance introspection — "which run produced the records I am looking at?"

*Built 2026-09-15 from `.claude/plan-variant-provenance-introspection.md`. Pick a
variant of any variable the same way the plot UI does, and get its provenance
to invocation/run level — in Python, at the CLI, and in the GUI, with **one**
implementation behind all three.*

## The question, and why it needed hand-written SQL before

`trace` keyed on a record_id or on plain metadata. It had no way to say "the
variant I pinned", although `load()` already resolved exactly such a pin. And
its `TraceNode` stopped above the run: `function_hash` and `run_count` were
there, but not `invocation_id`, `call_id`, `run_id`, or the run-options label.
So "which run produced this?" was a dozen `scidb sql` joins through
`_invocation_output` → `_invocation` → `_run_invocation` → `_run`.

## One owner: `scidb.inspect.Inspector.provenance`

```
Inspector.provenance(variable, selection=None, record_id=None,
                     include_runs=True, include_audit=False, **schema)
    → ProvenanceTree
```

`trace()` gained the same `selection=` and `include_runs=` parameters;
`provenance()` is `trace()` with `include_runs` defaulting to True. There is
one implementation. The three surfaces are:

| surface | call |
|---|---|
| Python | `db.inspect.provenance("GAITRiteSymmetry", {"Code:grSides": "v2"})` |
| CLI | `scidb trace GAITRiteSymmetry --variant Code:grSides=v2 --runs [--json]` |
| GUI | `variable_provenance` RPC → `provenance_service.variable_provenance` → the same method on the same `DatabaseManager` |

**"The CLI powers the GUI" was read as a shared API, not a subprocess** (user
decision 2026-09-15). The GUI shelling out to `scidb --json` would open a
second connection to a single-writer DuckDB file — the write-lock contention
the MATLAB run-ownership work removed. Parity is kept structurally instead:
the CLI command was built first, and the panel prints the `scidb trace` line
that reproduces what it shows.

## Selection vocabulary — one canonical form, two accepted spellings

A `selection` is the dict a `Variant` carries, i.e. what `load()` already
honours:

```python
{"__code__.grSides": "v2", "__run__.loadGaitRiteOneFile": "distribute=true",
 "bandpass.low_hz": 20}
```

The display spelling the Plot Studio picker uses (`Code:grSides`,
`Run:loader`, `CodeIsLatest`) is accepted everywhere and canonicalized by
`scidb.variant.normalize_selection`. That function is the **only** owner of
the mapping: `scistackplotdb.variants.branch_params_for` now delegates to it,
and a module-level check in `scistackplotdb.variants` raises at import if
`scistackplot`'s column prefixes ever diverge from scidb's copies
(`CODE_COLUMN_PREFIX`, `RUN_COLUMN_PREFIX`, `LATEST_COLUMN_NAME`). scidb holds
the strings because it owns the pin vocabulary and cannot import the display
layer (`scistackplot` must stay scidb-free for the CSV path).

The CLI's `--variant KEY=VALUE` is repeatable; a repeated key accumulates a
list, which scidb reads as membership. Code/run values stay verbatim
(`v2`, `distribute=true`, `latest`); branch-param values are literal-eval'd so
`low_hz=20` matches the stored int.

## Stage 1 — `provenance_query.records_for_variant(db, variable, selection, **schema)`

Returns the record_ids a pin names. It is the for_each input loader's own
resolution and nothing beside it: one `_find_record` call with the same
`branch_params_filter` and the same version rule.

That version rule is the trap worth knowing. `version_id="latest"` collapses on
`(fn_name, branch_params, output_num, consumed_locations)` — `function_hash`
is deliberately not in the key, and since 2026-09-14 the collapse also
supersedes older run-option sets. So two code versions (or two option sets)
**merge before any filter runs**, and a code/run pin against a collapsed load
always matches nothing. The for_each loader already loaded `"all"` in that
case; the rule now lives once, in `scidb.variant.pin_loads_uncollapsed`, and
both the loader and `records_for_variant` read it. A branch-param pin keeps
the collapsed load (branch_params IS in the key, so those never merged).

Invariant, tested: `records_for_variant(pin)` ≡ the `__record_id`s of
`load_all_as_df(branch_params_filter=pin, version_id=<same rule>)`.

## Stage 2 — `TraceNode` down to the run

New fields on `TraceNode`:

| field | meaning |
|---|---|
| `invocation_id` | the producer `function_hash` belongs to (the lowest-id one) |
| `invocation_ids` | **every** producer, ascending |
| `call_id` | the for_each call site (`config_call_id`, reconstructed) — None for raw saves, glue hops, `__save__` |
| `run_options` | `distribute=…[, as_table=[…]]` for `invocation_id` |
| `code_version` | per-function ordinal (`v1`/`v2`); None for a single-version function (the ordinal map omits those on purpose) |
| `runs: list[RunRef]` | every run that produced the record, oldest first; only with `include_runs=True` |

`RunRef = (run_id, timestamp, user_id, where_clause, invocation_id,
function_hash, run_options)`. The per-run invocation/hash/options are carried
so that, should a record ever have a second producer (see the next section
for why the save path never writes one), each run still says which
invocation it belongs to.

`ProvenanceTree` gained `selection` (the canonical pin, echoed back) and
`matched_record_ids` (every record the pin matched).

### "Reproduced" means a second run, not a second producer — measured

The plan assumed a re-run under edited code could land on the *same* record
with a *second* producing invocation, and that `producing_invocation_batch`'s
lowest-id pick was therefore losing producers. **That premise is wrong**, and
the first version of the Stage 2 fixture failed on it (2026-09-16).
`generate_record_id` hashes the content **plus the save metadata**
(`nested["version"]`: `__fn_hash`, the input rids, `__distribute`, …), so a
different body computing the same value, or the same body under a different
flag, writes a **new record from a new invocation** — the diagnostic showed
`2 new rows` on the second save and four distinct `_invocation_output` rows.

What a re-run of the *same* recipe does is reuse the record and the
invocation and append a `_run` row. So "which run produced this record?" is a
list of **runs on one invocation**, which is exactly what `TraceNode.runs`
carries. The Stage 2 fixture now runs one body twice and asserts two `RunRef`s
on one `invocation_id`.

The plural `producing_invocations_batch` stays, for a different reason than
the plan gave: the schema permits several producers (`_invocation_output` has
no uniqueness on `output_record_id`) and a writer outside this save path — the
MATLAB bridge, an import, a future identity change — could create them. The
trace reads the plural so such a row is reported rather than halved away; a
test pins that against a hand-inserted second producer. The singular is a
documented, deliberately lossy `producers[0]` wrapper, and its tie-break was
**not** changed: variant identity, the `_find_record` collapse and
`function_hash` on a node each need one stable value.

### Batched helpers (the N+1 rule)

All new enrichments are keyed by invocation and resolved over the whole tree
at once: `runs_for_invocations_batch` (1 query),
`invocation_call_ids_batch` (≤ 4 queries whatever the input size),
`glue_source_batch`, plus the existing `invocation_run_options_batch` and
`code_version_ordinals`. Tests count `_fetchall` calls.

## Stage 3 — CLI

- `scidb trace` gained `--variant KEY=VALUE` (repeatable) and `--runs`.
  `--json` is unchanged in shape except for the new fields.
- The human render only changes under `--runs`: a `variant:` header, an
  `(N records match; tracing X)` line when the pin spans locations, and per
  node an identity line (`invocation … call … run options: …`) followed by
  one `run <id> <ts> by <user> [where …]` line per run. A run whose invocation
  or hash differs from the node's headline one is tagged in place.
- `scidb variants` now reports `run_options` per variant (closes the "Not
  done" item in `run-option-variants.md`). The human table adds the column
  only when more than one option set is present — a project that never
  flipped a flag would otherwise read `distribute=false` on every row.

### Ambiguity: pin vs. plain metadata, handled differently on purpose

`_resolve_pin` distinguishes two kinds of "several matches":

- **no explicit pin** (`trace Filtered low_hz=20`) — under-specification; it
  raises `AmbiguousVersionError` with candidates listed, as it always has.
  A branch param passed as a plain kwarg is a *filter*, not a pin, and does
  not change this.
- **an explicit `selection=` / `--variant`** — a variant spans schema
  locations by design (`--variant Code:grSides=v2` with no `subject=` is the
  headline case). It roots at the most recently saved match, logs that it did,
  and returns every match in `matched_record_ids`.

`--record-id` together with `--variant` is refused (`ValueError` → CLI usage
error): the record is already named, so a pin could only agree or be ignored.

## Stage 4 — GUI

- RPC `variable_provenance(variable, selection, schema_keys, include_runs)`
  and `POST /api/provenance/variable`, both → `provenance_service.
  variable_provenance`, which calls `db.inspect.provenance` and returns
  `dataclasses.asdict(tree)` plus `variable` and a flat, newest-first,
  deduplicated `runs` list. A pin that matches nothing returns
  `{"error": …, nodes: []}` rather than a 500 — clicking around the picker is
  not a server fault. Listed in `SELF_MANAGED_DB_METHODS` (short hold).
- `VariantDagPopup` was **not** copied. It already took `variable`/
  `selection`/`onApply` rather than a `PlotSpec`; the PlotSpec residue was the
  row-name field and the wording. It gained `showName`, `title`, `subtitle`,
  `applyLabel`, and the Provenance panel is its second consumer.
- `ProvenancePanel` (header button 🔍 Provenance): pick a variable and pin a
  variant on the canvas → the upstream node cards (fn, code version, call,
  run options, constants, "reproduced: N producing invocations") and the run
  list. It shows the equivalent `scidb trace … --runs` command.

Both vite bundles were rebuilt 2026-09-15 (see memory
`project-frontend-bundle-rebuild`). **Not yet visually checked.**

## Tests

- `scidb/tests/test_variant_provenance.py` — Stages 1–3: loader invariant,
  collapse rule, display-spelling equivalence, list membership, plural
  producers + singular tie-break, run refs, identity fields, query counts,
  ambiguity rules, CLI round-trips, run-option column, shared-API parity.
- `scistack-gui/tests/test_provenance_service.py` — Stage 4: RPC ≡
  `Inspector.provenance` field by field, HTTP route, JSON-RPC handler, empty
  pin as an answer.

## Open

- The `_run_lines` render shows every run; a record re-run 50× prints 50
  lines. A `--runs=N` cap was not built — wait for a real need.
- The GUI panel roots at one record when a pin spans locations. A "trace all
  matched" view would be a tree per record; not built.
- The MATLAB bridge has no `provenance` entry yet (same gap as
  `Variant(run_options=)` there).

---

## The bottom-up view — `scidb variants` and the Variants panel

*Added 2026-09-22 (`.claude/plan-topologies-panel.md`).*

`provenance` and `topologies` are the **two directions**, and neither answers
the other's question:

|  | `Inspector.provenance` | `Inspector.topologies` |
|---|---|---|
| direction | top-down | bottom-up |
| question | "where did **this** variant come from?" | "what is in here, and which of it is still live?" |
| you must already know | which variant you mean | nothing but the variable's name |
| surfaces | `scidb trace`, `ProvenancePanel` | `scidb variants`, `TopologiesPanel` |

**You cannot pin a variant you do not know exists.** That is the whole reason
the second one exists: on 2026-09-22 a run split `grSides` into two nodes and
the existing tooling could not say so — the flat variants table shows variants,
and a second *topology* (a node the user did not know existed) looks exactly
like a second *variant* (a run they forgot about).

### The two levels

A **topology** is `(function_name, input_types, output_type)` — which
function, fed by what, producing what, independent of constants, run options,
code version and schema location. It is the level a user reasons about when
they look at the canvas: **one node**. Within a topology, the variants are the
runs of it.

No wiring id is computed in `topologies`: the GUI has one
(`graph_builder.wiring_id`) and scidb's nearest equivalent (`call_id`) folds in
constants and run options, so inventing a third answer to "which node is this"
is exactly the failure mode `docs/claude/node-identity.md` is about.

### `load:` is the column that matters

Everything above it is context the reader wanted anyway. That line is the only
place in the GUI where *"this variant is not what a run will read"* is visible,
and two variants that look equally alive is the shape of the bug. The
vocabulary is closed (`scidb.inspect.api.VERDICT_*`) and the GUI styles off the
code, never off the wording:

* `current` — a `latest` load returns these records;
* `partially_superseded` — some locations still returned, some not. **Not a
  rounding of the other two**: run options are judged per function, globally,
  so a variant can lose some locations and keep others — the trial-4 orphan
  that started the investigation;
* `superseded` — an older run-option set; nothing here reaches a load.

### One owner per rule, across both surfaces

`variant_verdict` and `location_sample` live in `scidb/inspect/api.py`, beside
the method that produces the summaries. `render._location_sample` is the text
form of the second; `services/variants_service.py` ships both to the panel.
Neither surface decides for itself what "SUPERSEDED" means or which locations
to show (CLAUDE.md NOTE 3).

The GUI calls the shared API rather than shelling out to `scidb variants
--json` for the reason `provenance_service` states: a subprocess would open a
second connection to a single-writer DuckDB file.

### GUI

* RPC `variable_topologies(variable, all_locations, max_locations)` and
  `POST /api/provenance/variable-topologies` → `variants_service.
  variable_topologies`. `NotFoundError` → HTTP 400: a name that is not a
  variable is a typo in the box, not a server fault. A real variable with no
  producing pipeline steps returns zero topologies, which is the true answer,
  not an error.
* `schema_ids` is dropped from the reply and replaced by `locations` (its
  sampled view) — a loader has hundreds and the panel never shows the ids.
* `TopologiesPanel` reaches the user two ways, deliberately kept apart from
  Provenance: the header's 🧬 **Variants** button, and **🧬 Variants…** on a
  variable node's context menu — *the question arises at a node*.
* Read-only by design. No "delete this variant", no "re-run under these
  options": the project's ethos is hide, never delete, and hiding a variant is
  a separate decision with its own consequences for node state.

### Tests

* `scistack-gui/tests/test_variants_service.py` — grouping ≡
  `Inspector.topologies`, field-for-field; the verdict is scidb's verdict; the
  run-option flip leaving an older variant superseded; location sampling; an
  unknown name as a 400; a raw-saved variable as zero topologies; both
  transports.
* `frontend/src/components/Variants/topologies.test.ts` — the wording rules
  over a fixture shaped like the reply (one topology, two variants, one
  superseded), run by `npm test` under `node --test`.
