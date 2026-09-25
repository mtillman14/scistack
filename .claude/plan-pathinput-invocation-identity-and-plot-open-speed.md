# PathInput invocation identity + Plot Studio first-open speed (2026-09-25)

## 1. Wiring corruption between runs — diagnosis

Evidence (scidb.log after the 11:40:44 DB recreation):

- Run 7fkzxdzs: node `u5t0m0` = `read_csv(Symmetry) -> SymmetryTable`.
- Run pj1u0v41: node `4nojr3` = `read_csv(Unmatched) -> UnmatchedTable`.
- Next build: `pipeline_variants: 1 invocation(s) -> 2 variant(s)` and
  `call site pandas.read_csv/770b29ef records 2 output types ... path inputs
  {'filepath_or_buffer': 'Unmatched'}`. Both runs landed on ONE invocation.
- That merged call site's wiring (b71efb80…) matches neither dispatch claim
  (fe7e44bf…, 458e571b…), so a fresh node `09622ff8…` is allocated, both manual
  nodes try to graduate onto it -> "graduation collision", the edges are
  rewritten onto the merged node, and the Symmetry PathInput edge disappears
  (screenshot 1). Run nm1l8gik (GaitSpeed) repeats this with 3 outputs
  (screenshot 2).

Root cause (scidb): `compute_invocation_id` gets variable and constant edges
only. The PathInput edge is added in `record_run` "AFTER inv_id is computed →
deliberately NOT part of identity" (docs/claude/database-model.md §11 item 6,
WON'T DO 2026-06-21). A function whose only input is a PathInput (a library
loader like `pandas.read_csv`) therefore gets the SAME invocation_id for every
file it reads. Every run appends outputs onto that one `_invocation`, and
`pipeline_variants` keeps one PathInput per param (the last one read).

Note that `call_id` (CallSite) ALREADY includes the PathInput `to_key()`, so the
call site forks when the template changes. The invocation is the only layer
that doesn't.

## 2. Proposed fix (option A — recommended)

Treat the PathInput edge `(param, compute_pathinput_record_id(spec))` exactly
like a constant edge inside the identity:

- `provenance_save.invocation_identity` — add the PathInput bindings (single
  recipe; the forward/skip path `foreach.py:5525/5784` inherits it).
- `provenance_save.record_run` — append the PathInput bindings BEFORE
  `compute_invocation_id` (drift check keeps both sides honest).
- `provenance_query._predict_config_invocations` — add
  `cfg["path_inputs"]` bindings for mixed PathInput+variable configs.
  (PathInput-only configs already use realized invocations.)
- Update database-model.md §2/§3/§11 item 6 (decision reversed, with reason),
  and the docstring in `tests/test_rerun_output_edges.py` (it cites the old
  rule; the test itself uses constants and is unaffected).

Consequences: changing the template or root_folder forks the invocation.
call_id already does this. Clean break for existing PathInput-fed records
(beta, no migration).

Option B (declared-name in identity): don't take it. A rename would fork, and an
undeclared PathInput needs a second rule.

Tests (scidb/tests/test_pathinput_invocation_identity.py):
- two for_each runs of one fn over two different PathInputs -> 2 invocations,
  each with its own PathInput edge and output edges;
- `pipeline_variants` returns 2 call sites with the right path input each;
- re-running the same PathInput reproduces the same invocation_id (idempotent);
- mixed PathInput + variable input: predicted ids == realized ids
  (check_node_state green after run).
- GUI: two read_csv nodes over different PathInputs keep their own edges after
  both run (scistack-gui/tests).

Logging: INFO in record_run naming each PathInput edge folded into identity;
WARN in pipeline_variants when one invocation carries >1 PathInput spec for
one param (should now be impossible — tripwire).

## 3. Plot Studio first open — diagnosis

SymmetryTable: 11,609 records × 80 fields -> melted 928,720 rows, default spec
facets ColName (80 panels) and fans out 1,195 figures.

| phase | time | cause |
|---|---|---|
| get_table (load+melt) | 4.2s | attach_variants 1.35s, measure_extent 0.77s |
| build_plan y_limits | 18.7s | `_raw_extents`: Python loop over all 928,720 rows (~11.5s) + a DEBUG line per scope group (95,585 groups, ~7s) |
| render_mpl | 10.3s | 80-panel figure: repeated tight_layout + tick fitting |

Fixes:
1. `ylimits._raw_extents`: vectorise — groupby(scope) min/max over the
   lows/highs arrays; keys via `_as_key`. Parity test against the old loop
   output (NaN levels, log mode, mixed scalar/array cells).
2. `ylimits.finish_extents`: per-key DEBUG capped to the first 20 + "… N more".
3. Proposed default: when a variable has more than N (say 12) fields, a newly
   opened plot starts with ONE field selected (ColName filter) instead of all 80
   faceted. That is the case that already renders in 0.7s. It needs your call.

## 4. Seen in passing (not fixed here)
- `[aliases]` read hits "Connection already closed" (`_variables` query) during
  plot resolve — connection released before the alias lookup.
- Run saves: 36–47s for 11–15k rows (batch_save prep 21s, per_row_hashing 13.5s).
