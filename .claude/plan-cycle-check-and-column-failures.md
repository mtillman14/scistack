# Plan: cycle check on visible edges; per-column failures fail one combo

Source: scidb.log 2026-10-22, the 17:51 session (SRALab TSS data pull).

## 1. put_edge cycle check reads hidden edges (scistack-gui)

**Symptom.** 17:51:36 the user removed `LoadedData -> parse_loaded_data_per_subject.df_in`
(`manual__hlpxsa`, "hidden ... stored row kept"), rewired `df_in` to `data_path`, then
drew `parse_loaded_data_per_subject -> LoadedData` three times. Each was rejected:
`fn -> var__LoadedData::main -> fn`, a cycle through the hidden edge.

**Cause.** `layout_service.put_edge` ran `find_cycle` over
`pipeline_store.get_manual_edges(db)`: every stored row, hidden or not. Since D2
(2026-10-22) every removal is a hide that keeps the row, so a removed edge kept
blocking. The rule (manual-edges-on-history-nodes.md, edge_view.py): the drawn edges
are the ground truth, and `edge_view.effective_edges` is the one view of them.

**Fix.** The cycle check takes `effective_edges(db, scope_id, caller="put_edge cycle
check").drawn`, minus the edge being upserted. The rejection WARN names the edge ids
along the cycle, so a future false cycle shows which stored row closed it. The
guard's allow-list reason for put_edge is updated (it still reads
`get_hidden_edge_ids` for unhide-on-redraw).

**Test.** `test_api.py::test_put_edge_ignores_a_removed_edge_in_the_cycle_check`:
draw n1->n2, remove it, draw n2->n1 -> 200. The existing transitive/self-loop tests
still reject.

## 2. ColumnFunctionError aborts the whole run (scifor)

**Symptom.** Runs 3g16wi19 / u8l73j91 / s03ak99k stopped on subject 2204674
("failed with exception"); 32 subjects never ran. In dataframe mode the same function
failing ran every subject and finished "33 failed combo(s)".

**Cause.** `scifor.for_each` re-raised `ColumnFunctionError` on the assumption that a
column failure is the same for every combo. A data-dependent failure (one subject's
cell) breaks that assumption, and the two modes disagreed about what one failure means.
MATLAB `run_column_iteration` already lets a column error fall through to the per-combo
skip, so Python was the odd one out.

**Fix.** `ColumnFunctionError` goes through `_record_iteration_failure` like any other
function error: that combo fails (no partial row is saved), the run continues, and the
end-of-run summary groups identical reasons ("33 x ColumnFunctionError: ... columns
...") so a column bad everywhere still reads as one message. The error itself is
unchanged: it still names every failing column. `ForColumnsError` (structural
reassembly bug) stays a hard error.

Also: the docstring said the per-column slice is "a bare numpy array"; the hint said
"ensure these columns are numeric". Both are corrected with item 3 (pending decision).

**Tests (scifor, clean break, beta rule):**
- `test_iterate_function_failure_lists_all_bad_columns`: the reason recorded for the
  combo names every bad column and no good one (read via the run's failure summary).
- `test_iterate_function_failure_propagates_not_skipped` -> replaced by
  `test_iterate_function_failure_fails_the_combo_not_the_run`: two subjects, the
  function fails only on subject 1's value; subject 2 still produces its row.
- `test_iterate_excl_columns_lets_run_succeed_after_failure`: the first run now
  returns empty with the reported columns, instead of raising.

## 3. Function gets a StringArray: option A (user decision 2026-10-22)

DB holds plain VARCHAR. The user's env: pandas 3.0.3, duckdb 1.4.4. pandas 3 types
text as `str` dtype, so `.values` is a pandas `StringArray`/`ArrowStringArray`. And the
slice was always 1-D, even for a one-row record.

**Rule (option A).** One selected column of one combo arrives as the cell itself when
the combo has one row, else as a numpy array (`.to_numpy()`, never an extension
array). One owner: `scifor.column_value` (column_selection.py). Consumers:
`_run_column_iteration` (for_columns), `_apply_column_selection` (single-column
ColumnSelection), scidb `_apply_per_combo_col_selection` (was a duplicate of the rule).
It matches the existing 1-row unwrap of a plain input (`_extract_data`). MATLAB: a
1-row table column is already 1x1, so no change.

The `ColumnFunctionError` hint no longer says "ensure these columns are numeric".

**Tests.** scifor: `test_column_selection_single` (scalar), `test_column_value_*`
(str dtype -> str / numpy), `test_column_selection_multirow_single_is_an_array`,
`test_iterate_one_row_text_columns_get_plain_strs`; updated
`test_as_table_false_with_column_selection_returns_array` (now multi-row), two `v[0]`
lambdas. scidb: `TestForColumnsOneRowRecords` (save/load round trip -> str);
`test_unified_modifier_classes` lambdas.

**Risk.** A user function written for a 1-element array (`v[0]`, `len(v)`) on one-row
records now gets a scalar. This is a beta clean break.
