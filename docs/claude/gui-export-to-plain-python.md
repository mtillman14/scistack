# GUI → plain .py export (future feature): notes on the disconnected-wiring mechanism

Status: **not yet built.** This doc exists to capture how the "disconnected
wiring" mechanism (delete/hide an edge → function turns red and refuses to
run — see `pipeline_store.hide_edge`, `graph_builder.hidden_wirings`,
`run_state.propagate_run_states(disconnected_fkeys=...)`,
`execution_service.reconcile_manual_inputs`/`disconnected_reason`,
2026-08-09) interacts with the eventual "convert a GUI-authored pipeline
into a standalone runnable .py script" feature, so that work doesn't have
to re-derive this from scratch.

## The short answer

Nothing about the disconnected-wiring mechanism blocks or complicates a
plain scidb script today, and it shouldn't complicate the export feature
either — it's pure orchestration-layer state, not persisted anywhere scidb
core reads.

Confirmed by inspection: nothing in `scidb/src/scidb/*.py` references
`scistack_gui`, `hidden_edge`, or `disconnected` — the coupling is
one-directional (scistack-gui reads scidb data; scidb has zero awareness
of the GUI's document).

## Why plain scidb code is unaffected

- The mechanism lives entirely in GUI-only bookkeeping tables
  (`_pipeline_hidden_edges`, alongside `_pipeline_hidden_nodes`,
  `_pipeline_edges`, etc.) in the same `.duckdb` file as the real data —
  but scidb's own APIs (`for_each`, `Pipeline`, `.load()`,
  provenance/lineage) never query them. A plain script calling
  `scidb.for_each(fn, inputs={...}, outputs=[...])` directly never touches
  `derive_fn_targets`/`reconcile_manual_inputs` — those are
  scistack-gui orchestration functions.
- `hide_edge` never deletes or alters a DB record, provenance row, or
  lineage entry (project-wide "never delete, mark hidden" ethos) — it only
  marks a row in a GUI-only table. Any data a plain script `.load()`s is
  identical regardless of the GUI's current hidden-edge state.
- The "un-runnable" block is enforced in
  `execution_service.build_backend_pipeline`, which constructs an
  **in-session, unpersisted** `scidb.Pipeline` fresh on every GUI request
  (see `pipeline_store.py`'s module docstring: "spec persistence stays
  deliberately unbuilt"). Nothing about the block is written to disk or
  referenced by scidb core.

## The one real consequence: "disconnected" is a GUI opinion, not a data fact

A user can hide an edge (function shows red, GUI refuses to run it) and
still successfully run that exact same function from a plain script or a
Python REPL in the same session — scidb has no concept of "disconnected."
That asymmetry is intentional given the layering (this is explicitly a
GUI-authored-document concept, not a scidb-core one), but it means:

- Inspecting the DB or run history directly will never show "disconnected"
  anywhere — it only exists in the GUI's document (`_pipeline_hidden_edges`
  + the function's current wiring shape).
- An exported .py script has no natural way to represent "this wiring is
  currently disconnected in the GUI" as a runtime concept, because scidb
  itself has no such concept. The export step has to resolve it at
  **export time**, not runtime.

## What the export feature needs to decide

When exporting a GUI pipeline to a standalone script, for each function
node the exporter has to choose one of:

1. **Skip disconnected wirings entirely** — don't emit a `for_each(...)`
   line for a function whose wiring is currently disconnected. Simplest,
   matches what `build_backend_pipeline` already does at run time.
2. **Emit a warning comment** instead of skipping silently — e.g.
   `# SKIPPED: 'compute_rolling_vo2' has a disconnected input 'sample_interval' — reconnect it in the GUI before export`.
   Probably the better default, consistent with the "explicit error, not
   silent skip" decision already made for the run-blocking UX (see
   `execution_service.disconnected_reason`).
3. **Refuse to export at all** while anything in scope is disconnected,
   forcing the user to resolve it first. Heavier-handed; only worth it if
   a partially-broken exported script would be actively misleading rather
   than just incomplete.

(2) is the closest match to the existing run-blocking precedent and is the
current best guess for the default — revisit when the export feature is
actually designed.

## Reuse, don't re-derive

The pure functions built for the disconnected-wiring feature already
answer "is this wiring exportable/runnable?" — the exporter should call
these rather than reinventing the check:

- `graph_builder.hidden_wirings(fn_input_params, fn_outputs, fn_constants, path_inputs, hidden_edge_ids)`
  → the `{(fn_name, wiring_id)}` set of directly-disconnected wirings.
- `graph_builder.wirings_downstream_of(fn_input_params, fn_outputs, seed_wirings)`
  → everything transitively starved by a disconnected wiring (for a
  "cascaded — upstream unavailable" comment on downstream steps, same
  distinction already surfaced in the run report via
  `execution_service.disconnected_report_entries`).
- `execution_service.disconnected_reason(db, function_name, node_id=None)`
  → a ready-made human-readable message ("input 'sample_interval' is
  disconnected — reconnect it before running") — reuse verbatim as the
  warning-comment text in option (2) above.
- `variant_resolver.reconcile_manual_inputs(targets, function_name, hidden_edge_ids)`
  → if the exporter walks `derive_fn_targets` output directly, this is
  the same filter `build_backend_pipeline` already applies; calling it
  keeps "what the GUI would actually run" and "what gets exported"
  guaranteed identical by construction, instead of two implementations
  that can drift apart.

## Things that are NOT a concern for export

- Output-edge (fn→var) hides are cosmetic-only and never affect
  run_state/execution/exportability — no special handling needed.
- `_pipeline_hidden_edges` and friends are namespaced GUI tables; an
  exported script doesn't need to know they exist, since a
  non-disconnected function's target derivation (DB history or resolved
  manual edges) is completely unchanged by this feature — only the
  disconnected subset needs special export-time handling.
