# Plan: Fix the 2 failing TestSaveFromTable tests

## Failures observed

1. `test_auto_distribute_no_schema_cols_falls_through` (line 217)
   - `verifyEqual(numel(id), 1)` → actual **16**, expected **1**.
2. `test_auto_distribute_multiple_data_cols` (line 208)
   - `verifyTrue(istable(v.data))` → actual **false**.

## Root causes

### Failure 2 — CODE bug (BaseVariable.save return type)
A table with no schema-key columns falls through to the plain-save path:
`BaseVariable.m:194  record_id = char(py_record_id);`
`char` of a 16-char id is a `1x16` char array, so `numel == 16`. The test's
comment (`% single record_id string`) shows the intended contract is a **string
scalar** (`numel == 1`). The auto-distribute path already returns a `string`
array, so single-record saves are inconsistent.

### Failure 1 — TEST bug (table has only one data column)
DB schema keys are configured `["subject","session"]` (setup). The test table is
`{subject, session, MyVar}`, so `data_cols = ["MyVar"]` — a **single** data
column. `save()` takes the single-column branch (BaseVariable.m:163) and stores
the scalar `0.5`, so `v.data` is `0.5`, not a table. The test *intends* to
exercise the multiple-data-column sub-table branch (BaseVariable.m:165-174) but
its table never had multiple non-schema columns. No test currently reaches that
branch.

## Fixes

### Fix A (code) — `sci-matlab/.../+scidb/BaseVariable.m`
- Line 194: `record_id = char(py_record_id);` → `record_id = string(py_record_id);`
- Line 142 (LineageFcnResult path): `char(...)` → `string(...)` for consistency.
- Verified safe: every other test wraps the return in `string(...)`/`char(...)`
  or checks `ischar || isstring`, so returning a string scalar breaks nothing.
- Per CLAUDE.md note 2, add a `scidb.Log.info` line in the table-distribute block
  recording which branch was taken (single-col / multi-col / fall-through) so the
  dispatch is observable in the timing logs.

### Fix B (test) — `tests/matlab/scidb/TestSaveFromTable.m`
Rewrite `test_auto_distribute_multiple_data_cols` so the table genuinely has two
non-schema data columns, keeping schema keys `["subject","session"]`:
```matlab
tbl = table([1;2], ["A";"B"], [0.5;0.6], [1.0;2.0], ...
    'VariableNames', {'subject','session','MyVar','MyVar2'});
ids = ScalarVar().save(tbl);
verifyEqual(numel(ids), 2);
v = ScalarVar().load('subject', 1, 'session', 'A');
verifyTrue(istable(v.data));      % sub-table of [MyVar, MyVar2]
verifyEqual(height(v.data), 1);
```
This is the first test to actually exercise the multi-column sub-table branch,
serving as its regression test.

## Regression coverage
- Failure 2's own test becomes the regression test once Fix A lands.
- Fix B's rewritten test is the regression test for the multi-column branch.

## Notes / scope
- These two are the long-standing "all tests pass except 2" failures; they are
  independent of the uncommitted `load`/bridge/Merge work in the tree, which I
  will not touch.
