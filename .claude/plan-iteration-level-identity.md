# Plan: iteration level is part of invocation identity (+ plot default, per-record delete)

*2026-10-01. STATUS: built, pytest NOT yet run (user runs tests); both bundles rebuilt; pinned defaults in Plot Studio still deferred. User-approved ("Yes to fix 1, add per-record delete, write the doc";
"Don't worry about migration … If iteration level really should factor into
invocation identity, then please implement that").*

## The bug (from the user's GUI session, scidb.log 2026-10-01)

`DemographicsTable`: 34 records at 18 subjects, but the Variants popup showed **one**
card, and a one-subject plot drew every subject's row.

| run | how | saved |
|---|---|---|
| `n1irqety` (09-29 09:49) | `pandas.read_csv`, iterated **[subject]**, no distribute | the whole 16-row CSV at each of 18 subjects |
| `m8ch1rlz` (09-29 11:51) | same node, **one call** | the CSV flattened into 16 one-row records |

Confirmed by `scidb sql`: SS02 and SS04 hold one record each, the other 16 subjects
two; `COUNT(DISTINCT invocation_id) = 1`.

**Cause 1.** Invocation identity = function hash + edges (incl. PathInput NAME) +
`as_table`/`distribute`/`across_variants`. A PathInput-only loader has no variable
edges, so nothing in identity records WHERE the call iterated. Both runs are one
invocation, one coordinate, one card. `_supersede_same_invocation` then picks the
newest per location, which leaves SS02/SS04's whole-table records "current".

**Cause 2.** `scistackplot.variants.apply_variant_sets` returns the table unchanged when
a spec has no variant rows, so the per-location latest flag (`CodeIsLatest`, 48 of
304 rows here) is never applied, and both generations draw.

## Decisions

- **D1. The iteration level is identity.** Two calls of one function that iterate
  different schema keys are different computations, like `distribute`. It is folded
  into `invocation_id` (always, `[]` = one call), stored on `_invocation.iteration_level`,
  carried as the version key `__level` (so it is in the record id too), and shown in
  `run_options_label` as `level=subject` / `level=(one call)`.
- **D2. Not in `call_id`.** A call site is a canvas node. Changing the node's level makes
  a new VARIANT of that node (and `current_run_options` supersedes the older one), not a
  new node. `_CALL_ID_INCLUDED_KEYS` does not list `__level`.
- **D3. No migration** (memory `beta-no-deprecation`). The column goes into CREATE TABLE;
  databases created before this break and are rebuilt by re-running.
- **D4. Level = the final iterated keys** (after `_resolve_iterables`, so PathInput
  discovery is included), as dataset schema keys in dataset order.

## Changes

### scidb, identity
1. `foreach_config`: `RunOptions` unchanged (call-site shape); `ForEachConfig(level=)`
   and `to_version_keys()["__level"] = list(level)`.
2. `provenance.compute_invocation_id(…, iteration_level=None)`: folded in when not None
   (`level:<canonical_hash>`); logged in its DEBUG line.
3. `provenance_save`: `invocation_identity` and `record_run` read `__level` (helper
   `_iteration_level(meta)`, one owner); it goes in the cache key, the identity, and the
   `_invocation` row (new 8th column; the glue row is widened too).
4. `provenance.ensure_provenance_tables`: `iteration_level VARCHAR[]` on `_invocation`.
5. `foreach`: `_for_each_prepare` computes the level after `_resolve_iterables` and passes
   it to `_build_call_identity`. The skip hook gets it through a `level_ref` dict, as with
   `agg_binding_ref`, because the hook is built before the iterables resolve.

### scidb, readers
6. `provenance_query.run_options_label(…, iteration_level=None)`: appends
   `level=a/b` or `level=(one call)` when not None. Every reader that selects the
   run-option columns also selects `iteration_level` and passes it: `_build_upstream_closure`,
   `invocation_run_options_batch`, `run_option_axes`, `current_run_options`,
   `stored_invocation_signature`, `function_variant_configs`, `pipeline_variants`, and
   any other `distribute, as_table, for_columns, across_variants` SELECT.
7. `function_variant_configs`: the level is part of the config key and the config
   (`cfg["iteration_level"]`). `cfg["iterated_keys"]` prefers the stored level (fact) over
   reading it off the outputs; the old inference remains for a NULL (glue / `__save__`).
8. `_predict_config_invocations`: passes the config's level to `compute_invocation_id`.

### Plot Studio, the unnamed default (cause 2)
9. `scistackplot.variants.apply_variant_sets`: with no defined sets and a latest column
   present, keep only latest rows (scidb's per-location answer), and log how many went.
   The variant selection a plot resolved with is logged at INFO in either case.
10. A pinned default (`variant_pins.effective_default`) reaching the plotted variable is
    applied the same way by `scistackplotdb` (its `default_pin`), so an unnamed plot reads
    what an unnamed load reads.

### Per-record delete
11. `variant_delete`: target `{"record_ids": [...]}` (downstream still cascades).
    `VariantCard.location_record_ids` (aligned with `locations`) lets the GUI target one
    location's record(s). The popup's location-tree leaves get a 🗑 that opens the same
    plan → reason → confirm dialog.

### Logging
12. `configure_database`'s line logs `sys.executable` beside `python=3.11`.

### Docs and tests
13. `docs/claude/iteration-level-identity.md`; updates to `variant-space.md` §6,
    `variant-pins-and-deletion.md` and GUI manual testing (new item).
14. Tests (scidb): a loader at [subject] vs one call gives two invocations and two cards,
    and the older card is superseded everywhere (incl. a location only the old run wrote);
    the same level re-run is the same invocation (skip works); a new level is NOT skipped;
    the predictor stays green after a level change; the label reads `level=…`; `call_id`
    is unchanged by the level. scistackplot: an unnamed plot drops superseded rows.
    Delete: record_ids target. GUI: per-record delete plan via RPC.

## What the user does after

Re-create the database (D3) and re-run the pipeline. Or, if keeping it, nothing
works until it is rebuilt: inserts into `_invocation` fail on the missing column.
