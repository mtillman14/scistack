# Plan: column groups in `for_columns` (combine + split)

Status: DRAFT, awaiting approval (2026-10-28). All design questions answered.
Design discussion: whiteboard photo `spec/images/example_data_and_syntax.HEIC`
(an `Obj1` with `Subject`, `6MWT`, `6MWT_Date`; two rows per subject, blanks
for subject 2).

## Goal

Let `for_columns` pass **sets of columns** to the function together (a score
with its date), instead of only one column at a time, and let the user choose
whether the per-group results are **combined** into one output variable or
**split** into one output variable per group. Split mode must support
**sparse** datasets: a subject with no data for one group gets no record for
that group's output, while its other groups still save.

## Syntax (agreed)

```python
# Combine: ONE output. Dict keys name the output's columns, in key order.
for_each(fn,
         inputs={"in1": Obj1.for_columns({"6MWT":  ["6MWT",  "6MWT_Date"],
                                          "10MWT": ["10MWT", "10MWT_Date"]})},
         outputs=[Obj2])

# Split: N groups, N outputs, paired by POSITION (like a multi-output fn's returns).
for_each(fn,
         inputs={"in1": Obj1.for_columns([["6MWT",  "6MWT_Date"],
                                          ["10MWT", "10MWT_Date"]])},
         outputs=[SixMWT, TenMWT])
```

```matlab
% Split
scidb.for_each(@fn, struct('in1', Obj1().for_columns({["6MWT","6MWT_Date"], ["10MWT","10MWT_Date"]})), ...
    {SixMWT(), TenMWT()})
% Combine (dictionary keeps insertion order; containers.Map sorts -> rejected)
scidb.for_each(@fn, struct('in1', Obj1().for_columns(dictionary( ...
    ["6MWT","10MWT"], {["6MWT","6MWT_Date"], ["10MWT","10MWT_Date"]}))), {Obj2()})
```

## Rules

### Mode is derived from the number of outputs. No flag.

| outputs | groups given as | mode |
|---|---|---|
| 1 | flat list of names (today) | combine, one column each (unchanged) |
| 1 | dict `{name: [cols]}` | combine, column per key, key order |
| 1 | list containing a multi-column sublist | **error**: a multi-column group needs a dict key to name its column |
| N > 1 | list of N groups (a plain string = 1-column group) | split, group i → `outputs[i]` |
| N > 1 | list with ≠ N groups | **error** naming both counts |
| N > 1 | dict | **error**: in split mode the outputs name the groups, keys would name nothing |

- Today `for_columns` with >1 output is already an error
  (`scifor/foreach.py:692`, MATLAB `+scifor/for_each.m:656`), so split mode
  claims syntax that currently has no meaning: nothing existing changes.
- One group saved to one variable is not a for_columns case: that's
  `Obj1[["6MWT","6MWT_Date"]]` in a normal for_each.

### What the function receives per group

Exactly what bracket syntax gives (`_apply_column_selection`): a 1-column group
→ the bare column (`column_value`); a multi-column group → a DataFrame/table of
those columns. With `as_table`: schema keys + the group's columns. Rows that
are partly or wholly blank are passed through unchanged as long as the group
has at least one non-missing value (never drop data).

### Outputs

- **Combine:** today's reassembly (`_expand_column_result`), with the group key
  in place of the source column name: scalar → `<key>`; dict/Series/1-row frame
  → `<key>__<subkey>`; multi-row → `ForColumnsError` (unchanged). Columns follow
  dict key order.
- **Split:** group i's return is `outputs[i]`'s data for that combo, saved by
  the ordinary multi-output path, any shape (multi-row allowed). Internally the
  group loop returns an N-tuple, as a multi-output function does, so the save
  path, provenance and GUI wiring are the existing multi-output machinery.

### No data in a group → no output (sparse datasets)

The group-level analogue of the existing combo-level `NoDataError` ("no rows
for this combo → fn not called, counted as *no data*, not *failed*").

- **Missing** (one owner, `scifor.missing.is_missing_value`, ported to MATLAB):
  pandas `isna` (`NaN`, `None`, `NaT`, `pd.NA`); MATLAB `missing` / `NaN` /
  `NaT` / `<missing>` string; **and** empty or whitespace-only strings (`""`,
  `"  "`). Placeholder text such as `"—"` is data, not missing: converting it
  is the importer's decision.
- Before calling fn for a group, the group's slice for the current combo is
  checked. If **every value in every row of every column** of the group is
  missing *for that combo* (e.g. both of subject 2's rows, in both `6MWT` and
  `6MWT_Date`), fn is NOT called for that group. The check is per combo, never
  across the whole dataset:
  - **split:** that output's slot is the internal marker `NO_OUTPUT` → no
    record for that output at that combo;
  - **combine:** that group's column(s) are left missing in the combo's row.
- If **all** groups of a combo are missing, the combo is treated exactly like
  today's `NoDataError` (no call, no records, *no data*).
- **Function raises on a group (split):** that output's slot is `NO_OUTPUT`
  too, the other groups still save, and the group is reported as **failed**
  with its error. Combine keeps today's rule (any failing group → the combo
  saves nothing; one `ColumnFunctionError` naming every failing group).
- **Summary per output:** `SixMWT: 2 saved, 1 no data (subject=2)`,
  `TenMWT: 3 saved`, and failures with their reason, grouped like today.
  Failures also reach the run journal as today.

**The output's table always exists after the run.** If an output ends a run
with zero records (every combo was no data / failed for that group), scidb
still registers the variable and creates its table, so `SixMWT.load()`,
plots, the Data inventory and downstream for_each see an *empty* variable
rather than a missing table. The table is created with its schema-key and
metadata columns but no data columns, because the data column set is unknown
until a value exists. This relies on the existing rule in
`DatabaseManager._check_column_set` (database.py ~1509): an EMPTY table is
dropped and rebuilt with the first real save's columns, so the placeholder
never conflicts. Logged at INFO:
`[save] SixMWT: 0 records this run (3 no data); created empty table`.

`NO_OUTPUT` is **internal**: not exported, never returned by user code. It is
owned by scifor (`scifor/foreach.py`), carried through the scifor result table
to scidb, and skipped by scidb's save loop (`_save_results`, ~5906, where today
every slot is saved unconditionally). Standalone scifor (no DB) replaces it with
a missing value in the table it returns.

### Other rules

- **Overlap:** a column may appear in more than one group. Allowed, with one
  `Log.warn` per run naming the column and the groups that share it.
- **Several iterate inputs** must declare the same group structure (same keys /
  same per-position column lists), extending today's zip-by-name check.
- **Deferred `ColName()` / PathOutput `{ColName}`** (decided: option A):
  resolves to the group's dict key (combine) or the paired **output variable's
  name** (split, e.g. `"SixMWT"`). A 1-column group in a flat list resolves to
  the column name as today.
- **Logging (every run, INFO):**
  `[for_columns] 'in1' combine → Obj2: 6MWT ← [6MWT, 6MWT_Date]; 10MWT ← [...]`
  `[for_columns] 'in1' split: SixMWT ← [6MWT, 6MWT_Date]; TenMWT ← [10MWT, 10MWT_Date]`
  This is the safeguard against a mis-ordered split. A DEBUG line per skipped
  group: `[for_columns] subject=2: group SixMWT has no data (all 2 column(s) missing) — not called`.

## Ownership (NOTE 4)

- **Group parsing + mode derivation:** ONE function in scifor,
  `scifor.column_selection.resolve_column_groups(columns, output_names)` →
  `(mode, groups: list[(name, [cols])])`. scidb, the bridge and the GUI call it;
  nothing re-derives the mode. MATLAB standalone scifor has its own port
  (`+scifor/for_each.m`) as it does for every scifor rule; the MATLAB **scidb**
  path goes through the bridge, so it uses the Python owner. The GUI reaches it
  through a backend RPC, never a TypeScript copy.
- **"Is this value missing":** ONE function, `scifor.missing.is_missing_value`
  (+ MATLAB port). Check first whether an existing helper already owns this
  (e.g. the NULL/NaN round-trip code, blank-column typing) and extend that one
  instead of creating a second owner.
- **`NO_OUTPUT`:** defined once in scifor; scidb only tests identity against it.
- **Stored selector shape:** `scidb.intent.normalize_columns` /
  `selector_json` / `describe_columns` stay the single owner. Grouped
  selections add a `"groups"` key that is emitted ONLY when grouped, so the bytes
  of every existing selector (and therefore every `invocation_id`) are
  unchanged. A dict whose every group is `{c: [c]}` normalizes to the flat list
  (same meaning → same identity).
- **`ColumnSelection.to_key()`** gains the groups only when grouped, for the
  same reason.

## Stages

### Stage 0: diagnostics + facts to confirm before building
- Write the `[for_columns]` mode/pairing log line first so every later stage is
  observable.
- Test: combine output re-run with the **same keys in a different order** saves
  by column name and doesn't fail (`Obj2`'s physical columns are fixed on first
  save). If it fails, align by name at save.
- Find the existing missing-value helper(s) to extend (see Ownership).
- Find what each consumer does today with a registered variable that has **no
  table** vs an **empty table with no data columns**: `BaseVariable.load`,
  `load_all`/`as_table`, `data_column_names`, the Data inventory, plot
  describe, and a downstream `for_each` input. Log the result. If any consumer
  breaks on an empty table without data columns, fix it in that consumer's
  scidb owner, not in for_columns.

### Stage 0 results (2026-10-28, `tmp_stage0_empty_variable.py`, user-run)
- Never saved (A) vs registered + empty `record_id`-only table (B): every
  Python reader behaves the same. `load()` / `load(as_df=True)` raise
  `NotFoundError`, `load_all_as_df` returns an empty frame,
  `data_column_names` is `None`, `for_each(input=X)` runs 0 iterations.
  These readers decide from `_record`/`_variables`, never the data table.
- Only raw SQL differs: on the view/table it raises `CatalogException` in A,
  and returns 0 rows with `record_id, schema_level, <schema keys>, excluded`
  in B. So the empty table serves SQL/DuckDB queries; `load()` raising on
  an absent location is unchanged and consistent with sparse data (a
  missing subject raises the same way when other subjects exist).
- C: the first real save after B logs "its table is empty, so it is rebuilt"
  and stores the real columns. The placeholder is safe.
- Bug found and fixed: `for_columns()` over a variable with no records raised
  a plain `ValueError` (journal: "SciStack internal error") whose message
  referenced `loaded`, which is undefined on the metadata path. Now
  `ForColumnsNoColumnsError(SciStackError, ValueError)` with a "run the
  producing step first" message. Test:
  `test_every_column_of_an_unsaved_variable_is_actionable`.
- Missing-value helpers: four storage-side ones already exist
  (`scifor/locations._is_null`, sciduckdb `_is_missing_cell` +
  `datetimes._is_missing`, scistackplot `temporal._is_missing`), all
  "stored NULL" where `""` is data. The no-input check is a different concept
  and gets its own scifor owner; consolidating the four is separate work.

### Stage 1: `NO_OUTPUT` + per-output summary (scifor + scidb, Python)
Built first because split mode depends on it; it is independent of groups.
- scifor: internal `NO_OUTPUT` marker; result table carries it; standalone
  return converts it to missing; per-output saved / no data / failed counts in
  the end-of-run summary.
- scidb `_save_results`: skip a `NO_OUTPUT` slot for that output only; count it.
- scidb: an output with zero records after the run gets an empty table
  (one owner in `DatabaseManager`, e.g. `ensure_empty_variable_table`, reusing
  the create-if-missing code of the save paths rather than a second CREATE).
- Tests: a synthetic multi-output call with one slot `NO_OUTPUT` saves the
  other output and writes no record for the skipped one; summary counts;
  standalone table shows missing; `None` returned by a user function is still
  saved as before (regression, `NO_OUTPUT` ≠ `None`); an output skipped at
  EVERY combo leaves an empty table that loads as empty (not an error), and a
  later run that does produce data rebuilds it with real columns.

### Stage 2: scifor groups (Python)
- `scifor/column_selection.py`: `ColumnSelection.columns` accepts flat list,
  list with sublists, or dict; `resolve_column_groups`; `__name__`/`to_key`/`__hash__`.
- `scifor/missing.py` (or the extended existing helper): `is_missing_value`.
- `scifor/foreach.py`: Step 6.5 replaces the `n_outputs != 1` error with
  `resolve_column_groups`; `_resolve_iterate_columns` works per group (the
  all-columns `[]` expansion stays flat = 1-column groups); `_run_column_iteration`
  loops groups, slices via `_apply_column_selection`, skips all-missing groups,
  names combine columns by key, returns an N-tuple with `NO_OUTPUT` slots in
  split mode; overlap warning; zip check across inputs.
- Tests (`scifor/tests/test_foreach_standalone.py`): combine dict naming + key
  order; combine dict-return `<key>__<sub>`; split pairing; split multi-row
  output; the three errors; overlap warn; partly blank rows passed through;
  all-missing group not called (NaN, None, `""`, whitespace) while `"—"` is
  passed; all groups missing = no data; split group raising → other groups
  still return; as_table per group; deferred ColName per mode; two iterate
  inputs with matching / mismatching groups; flat list unchanged (regression).

### Stage 3: scidb (Python)
- `scidb/variable.py` `for_columns` accepts the new shapes.
- `scidb/foreach.py` `_resolve_for_columns`: groups survive resolution;
  `_make_raw_value_wrapper` handles the split tuple; dry-run display shows
  pairing.
- `scidb/intent.py`: `normalize_columns` / `selector_json` / `describe_columns`
  carry `"groups"` (chip text `per group (2)`); `run_options_label`.
- Tests (`scidb/tests/test_for_columns.py`, new `TestForColumnsGroups`): the
  whiteboard dataset end to end in both modes; split writes N variables with
  correct provenance; **subject 2 has a TenMWT record and no SixMWT record**;
  selector bytes for existing flat selections unchanged (identity regression);
  cache hit on identical re-run; key reorder.

### Stage 4: MATLAB
- `+scifor/ColumnSelection.m`, `+scidb/BaseVariable.m` `for_columns`: cell of
  string arrays (split) and `dictionary` (combine); `containers.Map` → clear
  error.
- `+scifor/for_each.m`: Step 6.5 port of mode derivation; `run_column_iteration`
  per group with the missing check; split returns `cell(1, n_outputs)` with a
  `NO_OUTPUT` marker in skipped slots; overlap warn; error ids
  `scifor:for_each:forColumnsGroups*`.
- `+scidb/for_each.m` `describe_input_for_python` / `build_scifor_input_from_desc`
  and `bridge.py` (`_reconstruct_input_for_keys`, `for_each_describe_loaded_input`,
  `for_each_prepare`) carry groups; the MATLAB save hand-off maps the marker to
  the Python `NO_OUTPUT`.
- Tests: `TestSciforForEachFeatures.m` (`test_groups_*`), `TestForColumns.m`
  (`test_groups_*`, including the sparse subject 2 case).

### Stage 5: GUI
**Column picker** (FunctionSettingsPanel's Inputs section, under the existing
**Run once per column** checkbox):

```
☑ Run once per column   ☑ Group columns
  Group 1  [6MWT ▾ 6MWT_Date ▾]        → SixMWT   ↑↓ ✕
  Group 2  [10MWT ▾ 10MWT_Date ▾]      → TenMWT   ↑↓ ✕
  + Add group
  Split: each group saves to its own output
```

- Mode is shown, not chosen: it follows from the wired output count, via the
  backend owner. With several outputs each row shows its paired output and
  ↑↓ reorders the pairing; with one output the `→` column becomes a name field.
- Inline errors (group count ≠ outputs, missing name) and the overlap warning
  appear before Run.
- Storage: `_node_config.columnSelections` through `normalize_columns` (no
  parallel store); FunctionSettingsPanel stays the only writer.

**Round trip and display:**
- Source-declared discovery reads grouped `for_columns(...)` calls.
- Code export: Python and MATLAB `for_columns(...)` emit both shapes
  (`execution_service` ~1431, `api/matlab_command.py` ~688).
- Split run = one function node, N output edges, via the existing multi-output
  wiring (`split_call_site_outputs`, `record_dispatch_wirings`); each output
  edge's tooltip lists its group's columns. Node chip: `per group (2)`.
- Run options section text: "Run once per group: combine into Obj2" /
  "split into SixMWT, TenMWT".
- Tests: `scistack-gui/tests/test_column_selection_binding.py`,
  `test_column_selection_run_inputs.py` (grouped round trip, export, N edges);
  frontend unit tests for the picker's row ↔ selection conversion.
- Rebuild both vite bundles (frontend bundle rebuild trap); update the GUI
  manual testing doc.

### Stage 6: docs
- `docs/claude/for-columns-iteration.md`: new "Column groups" section (rules
  table, no-data rule + missing definition, `NO_OUTPUT`, ownership, why split
  is positional, why "Why not EachOf" still holds: split fans out to
  *different variables*, not variant records).
- `scidb/README.md` for_columns section; GUI tutorial docs for the picker.

## Testing note
You run the tests. One package per pytest invocation (scifor, then scidb, then
scistack-gui); MATLAB suites `TestSciforForEachFeatures`, `TestForColumns`.
