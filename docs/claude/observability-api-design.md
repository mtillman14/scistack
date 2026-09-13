# Observability API — question → primitive mapping

> Written 2026-07-04 (branch `dev`), alongside the plan in
> `.claude/plan-db-observability-cli.md`. This documents the conceptual core of
> the observability effort: **every user-facing question the API answers, and
> exactly which existing scidb read primitive answers it.** The design invariant
> is that the facade (`scidb.inspect.Inspector`) computes nothing new — it only
> shapes and renders what `provenance_query.py`, `state.py`, `database.py`, and
> the core tables already encode. If a question can't be answered by the
> primitives below, that's a gap in the *graph model*, not something to patch in
> the presentation layer.
>
> Companion reading: `database-model.md` (canonical data model),
> `scistack-gui-backend-internals.md` (the GUI's existing graph consumption,
> which Phase 2 partially moves down into scidb).

## The question inventory

Each row: the plain-English question a user asks → the facade method → the CLI
command → the primitive(s) that answer it. (CLI entry point is `scidb …`;
`scistack db …` is an alias.)

| # | User question | Facade (`db.inspect.…`) | CLI (`scidb …`) | Primitives |
|---|---|---|---|---|
| 1 | What is in this database at all? | `overview()` | `status` | Counts over `_schema`, `_variables`, `_record`, `_invocation`, `_run`; last activity = max `_record_save.timestamp` / `_run.timestamp` |
| 2 | What variable types exist, and how much data does each hold? | `variables()` / `variable(X)` | `vars [Type]` | `_variables` (dtype metadata) joined to `_record` counts by `type`; last-saved from `_record_save`; variant count via `pipeline_variants(output_type=X)` |
| 3 | How is the experiment structured (schema keys, hierarchy, coverage)? | `schema_tree()` | `schema --tree` | `_schema` (`schema_id`, `schema_level`, key columns) + per-node record counts from `_record` |
| 4 | Show me the whole pipeline — functions, variables, variants, input values, what's been run | `pipeline(expand_variants=, include_values=)` | `pipeline [--variants] [--values]` | `pipeline_variants()` (config-level variants grouped by `(fn, call_id)`), `pipeline_structure()` (edge skeleton), variant aggregation ported down from scistack-gui `domain/variant_resolver.aggregate_variants`; node coloring from `state.check_node_state` (binary green/red, see #9); PathInput specs via `invocation_path_inputs` |
| 5 | What variants of X (or of function f) coexist, and how do they differ? | `variants(X \| fn)` | `variants <Type\|fn>` | `pipeline_variants(output_type=X)` / `function_variant_configs(fn)`. Variant identity = producing invocation's constants incl. `__save__.*` kwargs (`_producing_variant_key`); record counts per variant via the latest-collapse grouping in `_find_record` |
| 6 | What produced this exact record — function, constants, upstream inputs, all the way down? | `trace(X, **metadata)` | `trace <Type> key=val …` | `find_record_id` (resolve metadata → record_id, latest collapse), then `provenance_query.pipeline(rid)` (full nodes+edges DAG — the headline post-migration feature), `producing_invocation`, `invocation_inputs` (var inputs vs constants split), `branch_params_batch` for accumulated params. **Batch rule:** multi-record traversal goes through `_build_upstream_closure` / `*_batch` helpers, never per-record loops |
| 7 | Which settings/branch params does this record carry (accumulated from all ancestors)? | part of `trace` result; also on loaded records as `.branch_params` | shown in `trace` / `variants` | `derived_branch_params(rid)` — walks upward, namespaces constants `fn.param` (incl. `__save__.kwarg` for direct-save metadata) |
| 8 | Who ran what, when, with what `where=` filter? | `runs(fn=)` / `audit(X, **metadata)` | `runs [--fn]` / `trace --audit` | `_run` + `_run_invocation` scan; per-record `execution_audit(rid)`. `where_clause` here is **display-only** by design — never parsed or matched |
| 9 | What needs (re-)running? Is this node up to date? | `node_state(fn)` | `state [fn]` | `state.check_node_state` — binary green/red: green iff every expected invocation (derived live by `expected_invocations_for_function`, no persisted snapshot) is present in `_invocation`. Function edit ⇒ new `function_hash` ⇒ expected ids shift ⇒ red |
| 10 | Which *specific combos* are stale/missing, and why? | `combo_state(fn, outputs, inputs, **grid)` | `state --combos key=val…` | `state.check_combo_state` (per-combo `up_to_date`/`stale`/`missing`; staleness = `stored_invocation_signature` vs current fn_hash/input rids/constant hashes). PathInput loaders: `check_pathinput_node_state` (should-run = `PathInput.discover()` ∩ grid − exclusions, vs `realized_inputless_schema_ids`) |
| 11 | Show me the records (and their superseded versions) at a location | `records(X, latest=, **metadata)` | `show <Type> key=val … [--versions]` | `_find_record` machinery: `_record_save` ⋈ `_record` ⋈ `_schema`, latest collapse by `(variable, schema_id, variant)` ordered on `_record_save.timestamp`; `latest=False` skips the collapse to expose the re-save trail |
| 12 | Anything else (ad-hoc) | — | `sql "SELECT …"` | Raw read-only DuckDB; the per-type `<Type>` views already exist for human-readable rows |
| 13 | Which record_id is *this specific* variable output (so another tool can open its plot)? | `records(X, **metadata)` + `variants(X)` shaped as a candidate table | `pick <Type> [key=val …] [--interactive]` | Same primitives as #5/#11 — no new query logic. Metadata is only ambiguous when variants coexist; the table disambiguates by showing branch params per candidate. Picker *selects*, never displays data (that's the GUI's job) |
| 14b | Is my variable good at *every* schema location, and if not, which ones? | `locations(X, variant=, problems_only=)` | `locations <Type> [--variant …] [--problems] [--depth N]` | `scidb.locations.location_states` — the fourth granularity, between #9 (per function, binary) and #3 (per location, all variables, counts only). Four states: green / **amber** (an input was re-saved since) / red (expected, absent) / grey (excluded, so in neither numerator nor denominator). Denominator is #9's own `expected_invocations_for_function`, so the pane and the canvas cannot disagree — except for a **PathInput-only loader**, where it is `check_pathinput_node_state`'s discovery set, which is what lets this surface see a file that was never loaded. Batched throughout (`variant_keys_batch`, `current_records_by_schema_batch`, `latest_at_location_batch`, `superseded_batch`); a loop over #10 would be the N+1 this whole row exists to avoid. Full rationale: `schema-location-status.md` |
| 14 | Give me all my finalized figures + stats as one shareable page | `report(fn=, variable=)` / `write_report(dir, …)` | `report [--fn] [--var] [-o dir] [--all-versions] [--no-copy] [--no-embed] [--json]` | `inspect/report.py`: endpoint records = producing `_invocation.function_name LIKE 'plot\_%'/'stat\_%'` (no extra bookkeeping); `LEFT JOIN _schema` (root-level grand-aggregation outputs have NULL schema_id); latest collapse via `_find_record` intersect; `branch_params_batch` (N+1 rule); artifact stamps verified via `read_artifact_stamp` (mismatch ⇒ STALE warning). Output: self-contained `index.html` (inline CSS, embedded/copied artifacts) + `manifest.json` + `stats.csv` (per-test-family columns, D5's no-universal-schema). Drafts never appear (no records) |

## Write-side mapping (Phase 5 — declarative flags only)

The one class of mutation the CLI performs: flags the pipeline already
consults. All primitives pre-exist in `scidb/exclusions.py`; the CLI adds no
write logic. These live in a **separate write facade** (not `Inspector`, which
stays structurally read-only); write connections are opened per-transaction
and every mutation is logged. Bright line: no record/invocation/lineage
mutation, ever.

| # | User intent | CLI (`scidb …`) | Primitives |
|---|---|---|---|
| W1 | Exclude this schema location from runs (with a why) | `exclude key=val … --reason "…"` | `exclusions.exclude_schema(reason, **schema_keys)` — reason is already mandatory in the primitive |
| W2 | Re-include it | `include key=val … --reason "…"` | `exclusions.include_schema(reason, **schema_keys)` |
| W3 | What's currently excluded, and why? | `exclusions` (read-only) | `exclusions.list_exclusions()` |

## Cross-cutting mapping notes

- **"Latest" is always the `_record_save` collapse.** Any facade method that
  returns "the" record for a location must reuse `_find_record`'s grouping —
  `_record.created_at` is frozen at first save (`ON CONFLICT DO NOTHING`) and
  must never be used for recency.
- **Constants vs variable inputs vs markers.** `invocation_inputs` already
  buckets edges; `__save__` and `__pathinput__` records are treated as
  constants-like (non-variable) everywhere. Renderers must respect that split —
  e.g. `--values` shows constant values inline but only *counts/specs* for
  variable inputs and PathInputs.
- **Variant ≠ where-filter.** Since the where= redesign, `where_clause` is
  audit text only (#8). Questions about "which variant" always route through
  constants/branch-params (#5, #7) or consumed-input schema_ids — never through
  the stored filter string.
- **Function-hash mismatch is not staleness** at the record level (existing
  outputs remain valid, traceable lineage — see the memory/decision
  `defer-content-staleness`), but it *is* red at the node level (#9): the
  current recipe hasn't been run on these inputs. The API deliberately exposes
  both views without conflating them.
- **Graph corner cases** the renderers must handle: multi-producer variables
  (worst-state wins in the GUI model), self-referential input==output
  functions (render as back-reference, no recursion), `distribute=True`
  fan-out (many outputs, one invocation), `generates_file` lineage-only
  records (`generated:{invocation_id}`, no data row).

## Identified gaps (questions the primitives can't yet answer)

Recorded so future work targets the model, not the shell:

- **"What did this value used to be?" (content diff across re-saves).** #11
  lists versions, but there is no value-diff primitive; would need data-table
  time-travel by record_id — feasible, unbuilt.
- **"Which downstream results depend on X?" (forward/impact query).** The graph
  stores the edges, but there is no `downstream_provenance` mirror of
  `upstream_provenance`. Cheap to add in `provenance_query` when needed.
- **Downstream variables inherit a loader's blind spot.** #14b fixes the
  denominator where the missing data is (the loader), but expected sets are
  derived one function at a time: if eight subjects were never loaded,
  `FilteredEMG`'s expected set — predicted from the `RawEMG` that exists —
  never mentions them, and its pane reads `12/12`. Propagating a loader's
  discovery set down the chain needs combo mapping across schema levels.
- ~~The canvas badge still reads green for a partially-run loader.~~ **Closed**
  by `state._discovery_gate`: `check_node_state` now consults discovery for
  inputless functions too, behind a short TTL cache and a guard that stands the
  gate down when the walk finds nothing while the function has realized outputs
  (a path that cannot resolve must not red a whole study). The badge and #14b's
  denominator now use one rule.
- **Old-hash latest-record selection** — `find_record_id` can return lineage
  rows from a superseded function hash (see memory
  `latest-record-selection-future-issue`); `trace` output should surface the
  producing `function_hash` so this is at least visible.
- **Branch/variant-level include/exclude.** `exclusions.py` is schema-keyed
  only — it has no notion of variant, `call_id`, or branch params, so
  "exclude this computation branch from runs" cannot be expressed today. This
  is graph-model work, not CLI work: it has to interact with
  `expected_invocations_for_function` (what counts as expected when a variant
  is excluded?) and with variant identity (`_producing_variant_key`). Scope
  separately before proposing a mechanism.
- **Other write intents deliberately not built** (recorded so the bright line
  is a decision, not an omission): record annotation/tagging, retract/
  supersede a bad record_save, force-recompute by invalidating an invocation,
  canonical-variant marking, database merge/import, prune/vacuum of
  superseded versions. Each would need its own audit + safety design; none
  may piggyback on the Phase 5 exclusion commands.
