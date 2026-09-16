# `distribute` vs the spread rule — two ways rows become records

*Written 2026-09-15 after a `loadFunctionalOutcomes` MATLAB run saved a
73x27 `(subject, session)`-labelled table as ONE dataset-level record.
Concerns `scifor` (owner), `scimatlab` (bridge), and how the GUI's
"Distribute" checkbox relates to either.*

## The question that prompted this

> Deselect every schema key (run once), tick Distribute, return a table with
> `subject` and `session` columns under schema
> `[subject, session, speed, trial, cycle]`. Is it distributed to
> `subject & session`, or just `subject`?

Neither is quite right, because the question conflates two mechanisms.

## Mechanism 1: `distribute=True` — positional fan-out to ONE key

`scifor/foreach.py`, target resolution at "Step 3" (`resolve_distribute_target`):

- The target is exactly one schema key: **the key one level below the
  deepest iterated key**. Nothing iterated → the top of the schema.
- Each output is split into pieces (`_distribute_pieces`): a DataFrame by
  row, a 1-D array by element, a 2-D array by row, a list by item.
- **Addressing the pieces** (since 2026-09-15, both languages agree):
  - if the output is a DataFrame that **carries the target key as a
    column**, that column's values address the pieces and the column is
    stripped from the data (`INFO: distribute: output carries a 'cycle'
    column — its values address the N piece(s)…`);
  - otherwise the pieces get **1-based row ordinals**.
- `distribute` is identity-bearing (in `invocation_id` / `call_id`).

`distribute` is for outputs that have *no address of their own* — "row 3 →
cycle 3". It never targets two keys.

## Mechanism 2: the spread rule — address-by-returned-columns, no flag

`scifor/foreach.py`, `_spread_decision` (added 2026-08-25, b493ff67):

A returned DataFrame's rows become separate records **iff the DataFrame
carries a schema key the combination did not already pin** (or has ≤1 row,
which cannot multiply anything). Every unpinned schema-key column becomes
part of the row's address; every pinned one is replicated from the combo.

So the answer to the prompting question is: leave Distribute **off**. With
nothing iterated, `subject` and `session` are both unpinned, the rule spreads
the 73 rows, and each record lands at its own `(subject, session)`. Logged as
`output X: … discriminated by unpinned schema key(s) ['session', 'subject']
— spreading rows into separate records`. A table with *no* schema-key column
is instead one record per combination (`saving each whole table as ONE
record per combination`).

With Distribute **on** in that shape, `subject` is the target: the table's
`subject` column addresses the pieces (column-reading), the `session`
column is unpinned so the spread rule still files each piece at
`(subject, session)`. Same records, but via a path that exists for
unlabelled outputs; prefer the flag off.

| | `distribute=True` | spread rule |
|---|---|---|
| trigger | explicit flag | returned table carries an unpinned schema key |
| keys addressed | exactly one (next below deepest iterated) | every unpinned key column |
| unlabelled output | ordinals | one record per combo (no spread) |
| identity | in `invocation_id` | none (it's just how rows are filed) |

## The 2026-09-15 parity gap (fixed)

The spread rule ran only inside Python scifor's result collector
(`_results_to_output_dataframe`). MATLAB's `+scidb/for_each.m` forces
`_nest_table_outputs=true`, so its scifor loop nests each returned table as
one cell per combo, and `bridge.for_each_save` passed that nested `(1, 1)`
frame straight to `_for_each_save_resolved` → one blob record. The post-save
`flatten_nested_table_outputs` shapes only the *return value* (and is skipped
at `nargout=0`).

**Fix, one owner:** `scifor.spread_nested_results(result_tbl, output_names,
schema_keys)` converts a nested-mode table back into `collected_rows` and
runs `_results_to_output_dataframe`, so the bridge produces the same shape
and the same log lines as the Python loop. `for_each_save` now logs
`nested shape=(1, 1), result_tbl shape=(73, N)`; the `nested shape` →
`result_tbl shape` change is the diagnostic that was missing.

**Scope:** single-output runs only. scifor's flatten lays every output's
columns side by side and `_save_results` then files *all* data columns
under *each* output — a pre-existing Python limitation for functions that
return two tables. The bridge keeps multi-output runs nested (and says so at
INFO) rather than importing that limitation into MATLAB.

**Distribute column-reading:** MATLAB (`+scifor/for_each.m`,
`ismember(dist_key_char, VariableNames)`) had always read the target key
from a returned column; Python stamped ordinals regardless. Python now
matches MATLAB (`_distribute_pieces`).

## Reading a run in `scidb.log`

1. `resolve_distribute_target: '<key>'` — only if `distribute` is on.
2. `distribute: output carries a '<key>' column …` — column-reading took
   effect (else ordinals; no line).
3. `output X: … discriminated by unpinned schema key(s) [...]` **or**
   `… saving each whole table as ONE record per combination` — the spread
   decision, in either language.
4. MATLAB only: `[bridge] for_each_save: … nested shape=(a, b), result_tbl
   shape=(c, d)` — `(1, 1) → (73, 29)` means the spread happened;
   `(1, 1) → (1, 1)` with a multi-row table means the rows carried no
   unpinned key.
5. `for_each_save: … columns=[...]` / `[save] … (DataFrame RxC)` — ground
   truth: per-record row counts.

## Tests

- `scifor/tests/test_foreach_standalone.py` —
  `test_distribute_reads_target_key_column_when_present`,
  `test_distribute_uses_ordinals_when_target_key_column_absent`,
  `test_distribute_column_reading_top_of_schema_nothing_iterated`,
  `test_spread_nested_results_*`.
- `scimatlab/tests/test_bridge_spread_parity.py` — simulated MATLAB loop
  through `for_each_prepare`/`for_each_save`.
- `scimatlab/tests/matlab/scidb/TestSpreadParity.m` — end to end in MATLAB.
- `scimatlab/tests/matlab/scidb/TestForEach.m::test_distribute_session_column_in_output`
  — the MATLAB column-reading behaviour Python now mirrors.

## Related

- `docs/claude/gui-run-options-flow.md` — how `distribute` reaches a run.
- `docs/claude/for-each-kwargs.md` — option semantics.
- `scidb/tests/test_record_granularity.py` — the vo2max explosion that
  motivated the spread rule.

## Schema-key columns are never stored as data (2026-09-16)

A schema key is an address, never payload. Both result paths now strip
returned schema-key columns before the save:

- **spread path** — always did: scidb's flatten save reads every non-`__`,
  non-schema-key column as data (`scidb/foreach.py`, `data_cols = ...`), so
  the 73x27 `FunctionalOutcomes` sheet filed as 73 `1x25` records.
- **one-record-per-combo path** — did NOT until 2026-09-16. A multi-row table
  whose only schema-key columns were already *pinned* by the combination
  (`collisions` in `_spread_decision`) went into the record whole, keys
  included. `_results_to_output_dataframe` now drops those columns
  (`_strip_pinned_key_columns`) and the WARN says so: `… the combination's
  address wins and the data column(s) are dropped before saving`.

### The lasting damage, and where it is absorbed

The original 2026-09-15 19:16 save (pre-parity MATLAB nested path, since
removed) wrote one dataset-level 73x27 record carrying `subject`/`session`.
That put `subject` and `session` **data columns** into the variable's DuckDB
table permanently: records are excluded, never deleted, and a table's columns
outlive every record. Re-saving does not remove them.

`scistackplotdb.load.data_column_types_for` — the one owner of "which data
columns does this variable have" — therefore ignores any data column named
after a schema key (WARN `… named after schema key(s) — ignoring them`).
Without that, `load_variable` selected both the data copy and the real key,
`frame["subject"]` was a two-column DataFrame, and `plot_describe` failed with
`The truth value of a Series is ambiguous`.

Tests: `scifor/tests/test_foreach_standalone.py::test_pinned_schema_column_*`,
`scistackplotdb/tests/test_schema_key_data_columns.py`.
