# Plan: Fix 497s `6c_meta_insert` in save_batch

## Symptom
`py.scimatlab.bridge.for_each_save()` takes 500+s. `save_batch(GAITRiteLoadedCycle)`
for 8040 rows spends 497.270s in `6c_meta_insert` (total 506.437s). All other
steps are sub-3s; data insert of the same 8040 rows is 0.010s.

## Root cause
`scidb/src/scidb/provenance.py:253`, `insert_record_entities`:

```python
duck.con.executemany(_RECORD_INSERT, rows)
```

DuckDB's `executemany` re-runs the single-row prepared `INSERT ... VALUES (?...)`
once per row. `_record` has `record_id VARCHAR PRIMARY KEY` (ART index), so each
of the 8040 statements pays per-row `ON CONFLICT` index probe + maintenance.
~62 ms/row x 8040 = ~497s. This is the known DuckDB per-row-insert anti-pattern.

The rest of `save_batch` already avoids this: `_record_save` and the data table
use `INSERT INTO ... SELECT * FROM <registered_df>` (single vectorized insert).

## Fix
Rewrite `insert_record_entities` to use the same bulk pattern:

```python
def insert_record_entities(duck, rows: list[tuple]) -> None:
    if not rows:
        return
    record_df = pd.DataFrame(rows, columns=[
        "record_id", "created_at", "type", "schema_id",
        "content_hash", "schema_version", "excluded",
    ])
    duck.con.execute(
        "INSERT INTO _record "
        "(record_id, created_at, type, schema_id, content_hash, schema_version, excluded) "
        "SELECT * FROM record_df "
        "ON CONFLICT (record_id) DO NOTHING"
    )
```

Notes:
- Uses DuckDB replacement scan on the local `record_df` (same idiom as
  database.py:1123/1147). Add `import pandas as pd` at module top if absent.
- Keep `insert_record_entity` (singular) on `_execute` for single-row callers.
- pandas may widen `schema_id`/`schema_version` (None) to float; DuckDB casts
  back to INTEGER (NaN -> NULL) on the SELECT. `excluded` all-False -> bool, fine.

## Observability (CLAUDE.md note 2)
Split the `6c` timer in database.py into two sub-steps so a regression is
visible and the diagnosis is self-documenting:
- `6c1_record_save_insert` (the save_df insert)
- `6c2_record_entities_insert` (insert_record_entities)

## Regression test (CLAUDE.md note 2)
Add a test that saves a few thousand rows in one `save_batch` and asserts:
1. correctness — all `_record` rows present, dedup/ON CONFLICT still holds on
   re-save (no duplicate rows, idempotent).
2. that `insert_record_entities` issues a single `execute` (not `executemany`)
   — guards the bulk path. Can monkeypatch/spy on `duck.con`.
Performance is implied by correctness + bulk path; avoid a wall-clock assert
(flaky), or gate a generous ceiling (e.g. < 10s for 8040 rows) behind a marker.

## Verification
- User runs MATLAB + Python test suites (no Python in assistant env).
- Re-run the original for_each_save workload; confirm `6c2...` drops from ~497s
  to sub-second and totals are dominated by hashing/commit again.

## Risk
Low. Pure swap to an already-proven pattern used twice in the same function.
Behavior identical (ON CONFLICT DO NOTHING preserved); only the execution
strategy changes.

---

## IMPLEMENTED (2026-06-22) — broader scope than original plan

Audit (`grep executemany`) found the same anti-pattern in **9 sites**, all in the
provenance write path, all scaling with the for_each record count — not just the
one `6c2` site in the logs. `_commit_graph` (the for_each graph commit) alone had
6. Left as-is they were the next 497s. Fixed all of them.

### Shared helper (sciduckdb layer, per CLAUDE.md note 3)
`SciDuck._bulk_insert(table, columns, rows, conflict_cols=None)` in
`sciduckdb/src/sciduckdb/sciduckdb.py` — registers rows as one DataFrame and does
a single `INSERT INTO <table> (...) SELECT * FROM df [ON CONFLICT (...) DO NOTHING]`.
Takes `_lock`; safe inside an open transaction (register/insert/unregister on the
same connection).

### Call sites converted to `_bulk_insert`
- `provenance.py::insert_record_entities` — `_record` (the logged 497s site)
- `provenance_save.py::record_direct_save` — `_record`, `_constant`,
  `_invocation_input`
- `provenance_save.py::_commit_graph` — `_record`, `_constant`, `_invocation`,
  `_invocation_input`, `_invocation_output`, `_run_invocation`
Single-row inserts (`_invocation`, `_invocation_output`, `_run` in the direct/run
paths) left on `con.execute`.

### Observability
`database.py` save_batch 6c timer split into `6c1_record_save_insert` +
`6c2_record_entities_insert` (kept `6c_meta_insert` as the combined total).

### Regression tests
`scidb/tests/test_batch_save_regression.py::TestBulkInsertProvenance`:
- `test_record_entities_use_bulk_insert` — `_record` and `_invocation` go
  through `_bulk_insert`.
- `test_no_per_row_executemany_in_provenance` — spies `SciDuck._executemany`
  and a `con` proxy; asserts zero per-row executemany in the save path.
- `test_bulk_insert_is_idempotent` — re-run yields no duplicate records.
- `test_bulk_insert_moderate_batch_correctness` — 60 records round-trip
  (None schema_id, list/`as_table` columns).

### Watch item
`_invocation.as_table` is `VARCHAR[]`. Bulk insert relies on DuckDB's pandas
scanner converting an object column of Python lists to `LIST`. Believed fine;
the tests above exercise it. If it fails, that one site can revert to
`con.executemany` (or build the column via Arrow).

### Verify (user runs — no Python in assistant env)
`pytest scidb/tests/test_batch_save_regression.py -v` plus the full suite, then
re-run the real for_each_save workload and confirm `6c2...` is sub-second.
