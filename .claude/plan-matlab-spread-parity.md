# Plan: MATLAB spread parity + distribute column-reading in Python

*2026-09-15. Triggered by a `loadFunctionalOutcomes` MATLAB run (scidb.log
19:16) that returned a 73x27 table carrying `subject`/`session` columns with
every schema level deselected and `distribute=false`, and saved ONE record
at the dataset level instead of 73 addressed records.*

## Findings

1. **Spread rule is Python-only.** `_spread_decision` (scifor/foreach.py) runs
   inside Python scifor's `_results_to_output_dataframe`. On the MATLAB path
   `+scidb/for_each.m` forces `_nest_table_outputs=true`, MATLAB scifor nests
   every returned table as one cell per combo, and `bridge.for_each_save`
   hands that nested (n_combos x outputs) frame straight to
   `_for_each_save_resolved`. Nothing re-applies the spread decision, so a
   labelled table saves as one blob. The post-save
   `flatten_nested_table_outputs` only shapes the *return value* (and is
   skipped at nargout=0).
2. **distribute disagrees across languages.** MATLAB
   (`+scifor/for_each.m:987`) reads the target key's values from a returned
   column when present (and drops the column); Python stamps 1-based row
   ordinals regardless.

## Changes

### scifor (owner of both rules)
- `spread_nested_results(result_tbl, output_names, schema_keys, col_dtypes=None)`
  — new public helper. Converts a nested-mode result table (metadata columns
  + one DataFrame-valued column per output) back into `collected_rows` and
  runs the existing `_results_to_output_dataframe`, so Python's loop and the
  MATLAB bridge produce the same shape and the same log lines
  (`discriminated by unpinned schema key(s)` / `saving each whole table as
  ONE record`).
- `_distribute_pieces(value, distribute_key)` — when a DataFrame output
  carries the target key as a column, its values address the pieces and the
  column is stripped (MATLAB behaviour); otherwise row ordinals as before.
  INFO log the first time a column is used in a run.

### scimatlab bridge
- `for_each_save`: after merging the per-output frames, call
  `spread_nested_results(result_tbl, state.output_names,
  state.current_schema_keys)` before `_for_each_save_resolved`. The existing
  `[bridge] for_each_save: ... shape=` INFO line now reports the spread
  shape, which is the diagnostic that was missing.

### Tests
- scifor: distribute reads the key column (values + column stripped;
  ordinals when absent); `spread_nested_results` spreads a labelled nested
  table, leaves an unlabelled one as one row per combo, passes non-DataFrame
  cells through unchanged.
- scimatlab (pytest, simulated MATLAB loop): static PathInput, no iterables,
  nested (1,1) frame whose inner table carries `subject`/`session` → N
  records at their own addresses; an inner table with no schema-key column
  → 1 record.
- MATLAB (`tests/matlab/scidb/TestSpreadParity.m`): same two cases end to end
  through `scidb.for_each`, user-run.

### Docs
- `docs/claude/distribute-vs-spread.md`: the two mechanisms, the parity
  gap, where the one owner lives.

## Status (2026-09-15)

Built, tests unrun (user runs pytest/MATLAB):
- scifor: `spread_nested_results` (exported), `_distribute_pieces`
  (column-reading), INFO/DEBUG logging, dry-run text.
- bridge: `for_each_save` applies the spread for SINGLE-output runs; multi-
  output stays nested (Python's flatten cannot attribute columns to outputs)
  and logs why. `nested shape=` added to the shape INFO line.
- tests: 8 scifor, 3 bridge (pytest), 3 MATLAB (TestSpreadParity.m) +
  helper `table_with_subject_session_cols.m`.
- docs: docs/claude/distribute-vs-spread.md.

Open: multi-table-output spread (needs per-output column attribution in
scifor's flatten mode before the bridge can apply the rule there).
