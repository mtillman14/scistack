# Provenance of any pinned variant, down to the run

*2026-09-15. Requested after the ColumnSelection lineage investigation, where
answering "which run produced the records I am looking at?" took a dozen
hand-written `scidb sql` joins. The capability: pick a variant of any variable
the same way the plot UI does, and get its provenance to invocation/run level —
in Python, in the CLI, and in the GUI, with one implementation behind all three.*

## What already exists

| piece | where | gives |
|---|---|---|
| `Inspector.trace(type, **md)` / `--record-id` | `scidb.inspect.api` | `ProvenanceTree` of `TraceNode`: fn name, fn hash, constants, path inputs, inputs, saved/by, run_count, last_run |
| `trace --audit` | CLI | `RunRecord`s (who/when/where=) for the tree |
| `Inspector.variants(name)` | `scidb.inspect` | coexisting variants: fn, `call_id`, output type/num, input types, constants |
| `Inspector.pick(type, **md)` | `scidb.inspect` | candidate records + branch params, for selection |
| `_run` / `_run_invocation` | provenance tables | `run_id`, timestamp, user, function, `where_clause` ↔ invocations |
| `variant_graph(db, frame, fns)` | `scistackplotdb.variants` | axes + versions + latest flag — powers the Plot Studio popup |
| `Variant(X, code_version=, run_options=, fn=)`, `__code__` / `__run__` | `scidb.variant` | the pin vocabulary `load()` already honours |
| `VariantDagPopup` + `VariantSelectionContext` | GUI frontend | the canvas picker, already mode-switchable by design |

## The four gaps

1. **No variant-pinned entry point.** `trace` keys on a record_id or on metadata.
   There is no "trace the variant I pinned", although `load()` already resolves
   exactly such a pin.
2. **`TraceNode` stops above the run.** It carries `function_hash`,
   `run_count`, `last_run` — but not `invocation_id`, `call_id`, `run_id`, or
   the run-options label. "Which specific run produced this?" is unanswerable
   without hand-written SQL.
3. **`scidb variants` omits run-option sets** (already noted as open in
   `docs/claude/run-option-variants.md`).
4. **No GUI surface at all**, and the picker that exists is wired to a
   `PlotSpec`.

## Design

**One owner: `scidb.inspect`.** The CLI is a thin shell over `Inspector`; the
GUI RPC is another thin shell over the same object. See the open question below
on "the CLI powers the GUI" — this plan reads it as *one shared implementation*,
not as the GUI shelling out to a subprocess.

### Stage 1 — resolve a variant pin to records (scidb)

`provenance_query.records_for_variant(duck, variable, selection, **schema)`
→ `[record_id]`. `selection` uses the vocabulary `load()` already honours:
`{"__code__.grSides": "v2", "__run__.loadGaitRiteOneFile": "distribute=true",
"bandpass.low_hz": 20}`. Reuses `database._pin_values` and the existing
code/run filters rather than re-deriving them — the rule that a list value means
membership lives in one place and must keep doing so.

### Stage 2 — take `TraceNode` down to the run

Add to `TraceNode`: `invocation_id`, `call_id`, `run_options` (the
`run_options_label`), `code_version` ordinal, and `runs: list[RunRef]` where
`RunRef` is `(run_id, timestamp, user_id, where_clause)` from
`_run_invocation` ⨝ `_run`. Batched (`_chunked_in`), never per node — the
N+1 trap this codebase has hit before.

`producing_invocation_batch` currently keeps the **lowest** invocation_id when a
record has several producers, which is its own open bug; this stage must return
**all** producing invocations per record, since "which run produced this" has a
list answer once a record has been reproduced. Fixing the lowest-id pick is a
prerequisite, not a side quest.

### Stage 3 — Python + CLI surface

- `Inspector.provenance(type, selection=None, include_runs=True, **metadata)`
  → the enriched `ProvenanceTree`.
- `scidb trace` gains `--variant KEY=VALUE` (repeatable) and `--runs`.
  `--json` already exists and is what the GUI consumes.
- `scidb variants` gains run-option sets (gap 3).
- Rendering: the tree gains a run line per node under `--runs`.

### Stage 4 — GUI

- New RPC `variable_provenance(variable, selection, schema)` → calls
  `Inspector.provenance`. No provenance logic in the GUI layer (NOTE 3).
- Extract the selection half of `VariantDagPopup` from its `PlotSpec` wiring so
  a second consumer can mount the same canvas. `VariantSelectionContext` already
  carries the mode; this adds a consumer, not a mode.
- A Provenance panel: pick a variable → pin on the canvas → see the tree, with
  each node showing fn, version, constants, invocation, and the runs that
  produced it.

## Open question for the user

**"The CLI should power the GUI"** — two readings:

* **(a) Shared API (recommended).** CLI and GUI both call `Inspector`. One
  implementation, no subprocess, no serialisation boundary, and the GUI keeps
  the DatabaseManager it already holds (a second DuckDB handle would reintroduce
  the write-lock contention the MATLAB run-ownership work resolved).
* **(b) Literal subprocess.** The GUI shells out to `scidb ... --json`. Real
  benefit: the GUI can only ever see what the CLI can produce, so the two cannot
  drift. Real costs: process spawn per request, a second connection to a
  single-writer database, and error handling across a process boundary.

This plan assumes (a). If the intent is "whatever the GUI shows, I must be able
to reproduce at the terminal", (a) delivers that as long as every GUI panel maps
to a CLI command — which Stage 3 ensures by building the CLI first.

## Tests

- Stage 1: a pin resolves to the same records `load()` returns for the same
  `Variant(...)` — the two must never disagree.
- Stage 2: a record reproduced by two runs lists both; batching issues one query
  per depth, not per node.
- Stage 3: `--variant` + `--json` round-trips; `scidb variants` lists run-option
  sets.
- Stage 4: the RPC returns what `Inspector.provenance` returns (same shapes).

## Sequencing

1 → 2 → 3 are strictly ordered. 4 depends on 3's JSON shape being settled.
The `producing_invocation_batch` fix lands inside Stage 2.

Suggested first cut: Stages 1–3 only. That alone turns tonight's dozen hand-written
joins into `scidb trace GAITRiteSymmetry --variant Code:grSides=v2 --runs`,
and it is the part that has to exist before the GUI can be a thin shell over it.

---

## Status — 2026-09-15: all four stages built, tests unrun

User decisions: **all four stages**; open question resolved as **(a) shared
Inspector API** (no subprocess).

| stage | landed as |
|---|---|
| 1 | `provenance_query.records_for_variant(db, variable, selection, **schema)`; `scidb.variant.normalize_selection` (one owner of `Code:fn`↔`__code__.fn`, `scistackplotdb.branch_params_for` delegates + import-time drift check); `scidb.variant.pin_loads_uncollapsed` shared with the for_each loader |
| 2 | `TraceNode` + `invocation_id`, `invocation_ids`, `call_id`, `run_options`, `code_version`, `runs: [RunRef]`; `ProvenanceTree` + `selection`, `matched_record_ids`; `producing_invocations_batch` (plural, all producers), `runs_for_invocations_batch`, `invocation_call_ids_batch`, `glue_source_batch` |
| 3 | `Inspector.provenance(...)`; `scidb trace --variant KEY=VALUE --runs`; `VariantSummary.run_options` + `scidb variants` column |
| 4 | `variable_provenance` RPC + `/api/provenance/variable` → `services/provenance_service.py`; `VariantDagPopup` `showName/title/subtitle/applyLabel`; `components/Provenance/ProvenancePanel.tsx` + 🔍 Provenance header button; both vite bundles rebuilt |

Deviation from the plan, recorded: the "lowest-id pick" in
`producing_invocation_batch` was **not** changed — it is now a documented lossy
wrapper over the plural. Its callers (variant identity, the latest collapse,
`function_hash` on a node) need one stable value; the plural is what the
provenance path reads. The plan's example `--variant Code:grSides=v2` with no
schema keys is honoured by rooting at the newest-saved match and returning all
matches, rather than by raising.

Tests to run (user runs them — one package at a time):

```
cd scidb && python -m pytest tests/test_variant_provenance.py tests/test_inspect_cli.py tests/test_inspect_api.py tests/test_inspect_phase3.py tests/test_code_version_pin.py tests/test_run_option_variants.py tests/test_provenance_read.py -q
cd scistack-gui && python -m pytest tests/test_provenance_service.py tests/test_plot_service.py -q
cd scistackplotdb && python -m pytest tests/test_run_option_variants.py -q
```

Design write-up: `docs/claude/variant-provenance-introspection.md`.

**Correction 2026-09-16 (diagnostic run, `tmp.py`):** Stage 2's premise —
that a re-run under edited code lands a second producing invocation on the
same record — is false. `generate_record_id` hashes content + the save
metadata (`__fn_hash`, input rids, `__distribute`), so a changed body or flag
writes a new record. "Reproduced" means one invocation with several `_run`
rows, which `TraceNode.runs` carries. The plural helper is kept because the
schema permits a second producer from another writer; the fixture now re-runs
one body and a test hand-inserts a second producer. Docs, docstrings and the
panel tooltip corrected.
