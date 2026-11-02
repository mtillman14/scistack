# Plan P0 — `for_each` saves: provenance failures fail the run, and data + graph commit together

*Index: `.claude/plan-tidy-00-index.md` (global rules apply). Evidence:
`docs/design-review/04b-scidb-write-path.md` weakness 1, refactor W1.
Theme T8 in `09-synthesis.md`.*

## The defect

In `scidb/src/scidb/foreach.py` `_save_results` (starts at line ~5724):

- **PHASE 2** calls `db.save_batch(...)` per output (~6107 / ~6119). Each
  call opens and commits its **own** transaction (`database.py` ~2734
  `_begin`, ~2841 `_commit`). Per-output failures are collected in
  `save_failures` (~6073), not raised yet.
- **PHASE 4** calls `provenance_save.record_run(...)` (~6345), which opens a
  **second** transaction (`provenance_save.py` ~1226 `_begin`, ~1410
  `_commit`, rollback in `except`). The call sits inside
  `try: … except Exception as e: Log.error(f"[provenance] record_run failed …")`
  (~6334–6366). The comment there reads "Provenance graph is additive during
  migration — never fail the save". That migration finished in 2026-06.
- `OutputSaveError` is raised only at the end (~6421), after phase 4.

Result: a `record_run` exception leaves data rows with no invocation edges,
and the run reports success. Contrast: the direct save (`DatabaseManager.save`,
~3780 `_begin` … `record_direct_save` … ~3855 `_commit`) is one transaction.

DuckDB cannot nest `BEGIN TRANSACTION`, and `SciDuck._begin` (sciduckdb.py
~1374) issues a raw `BEGIN` every time. An outer transaction therefore needs
a re-entrant helper.

Line numbers drift. Locate each site by the quoted text, not by number.

## Stage 1 — a provenance failure fails the run (small; do first)

1. In `_save_results`, replace the swallowing `except Exception as e:
   Log.error(...)` around `record_run` with:
   - `Log.error(..., exc_info=True)` (keep the traceback in the debug file);
   - append a failure entry to `save_failures` and the exception to
     `save_causes`, worded like the existing per-output entries
     (e.g. `"provenance graph not recorded: <ExcType>: <msg>"`), so the
     existing `OutputSaveError` at the end of the function raises with it.

   Do **not** re-raise immediately. Let the function's existing end-of-save
   reporting run.
2. Delete the stale comments: the "additive during migration" one, and
   "Runs additively alongside the legacy _lineage writes above (Phase 3 of the
   lineage-simplification migration)" in the PHASE 4 banner. Replace them with
   one sentence: the graph is the only source of truth, so failing to write it
   fails the save.
3. Check the MATLAB path, which reaches the same code through
   `_for_each_save_resolved` → `_save_results`. Confirm with
   `grep -n "_save_results(" scidb/src/scidb/foreach.py` that there is no
   second call site with its own swallow. Then check `scimatlab/bridge.py`
   `for_each_save` for an `except` that would hide `OutputSaveError` from
   MATLAB. If one exists, make it propagate. MATLAB already surfaces Python
   exceptions as `MException`.
4. **Logging:** the `Log.error` line is the observable. Add one INFO line on
   success that already exists (`[provenance] recorded run_id=…`). Keep it.
5. **Test:** add
   `scidb/tests/test_save_provenance_failure.py::test_record_run_failure_fails_the_run`.
   It monkeypatches `scidb.provenance_save.record_run` to raise
   `RuntimeError("boom")`, runs a tiny `for_each` (copy the smallest fixture
   pattern from an existing `scidb/tests/test_foreach*.py`), and asserts:
   - `OutputSaveError` is raised;
   - its message names the provenance failure;
   - the run is not reported as success.
6. User runs: `python -m pytest scidb/tests/test_save_provenance_failure.py -x -q`,
   then `python -m pytest scidb/tests -x -q`.

**Done when:** the new test passes, and the scidb suite is green.

## Stage 2 — one transaction for data + graph (decision D9: all-or-nothing)

1. **sciduckdb: a re-entrant transaction context manager** (this is also
   refactor D2's first piece). In `SciDuck`:
   ```python
   @contextmanager
   def transaction(self, label: str = ""):
       # depth counter per connection; only the outermost BEGIN/COMMIT/ROLLBACK
       # touches DuckDB. Inner levels are no-ops; an exception at any level
       # rolls back the outermost.
   ```
   - Track `_tx_depth` beside `_tx_owner`.
   - Log `transaction(label) BEGIN depth=…` / `COMMIT` / `ROLLBACK` at DEBUG,
     and at WARN when the owner thread differs (reuse the existing
     overlapping-owner message).
   - Make `_begin`/`_commit`/`_rollback` depth-aware, or route them through
     `transaction`. If they stay, a `_begin` at depth>0 must not issue SQL.
   - Test (`sciduckdb/tests/test_transaction.py`):
     - nested commit writes once;
     - an inner exception rolls back the outer;
     - depth returns to 0;
     - a statement after rollback works (DuckDB requires the ROLLBACK).
2. **Use it on both sides.** `DatabaseManager.save_batch` and
   `provenance_save._commit_graph` switch from `_begin`/`_commit`/`_rollback`
   to `with self._duck.transaction("save_batch(<type>)")` /
   `with duck.transaction("record_run")`.

   Keep each function's existing `except` logging. Re-raise inside the `with`
   so the context manager rolls back.
3. **Wrap phases 2–4 of `_save_results`** in one
   `with db._duck.transaction(f"for_each_save({fn_name})"):`. Resolve the db
   the same way the function already does: `db` or `get_database()`. Then:
   - Inside the block, if `save_failures` is non-empty at the end of phase 4,
     raise `OutputSaveError` **inside** the `with`, so everything rolls back
     (decision D9). Move the existing raise there. Keep the end-of-function
     summary log *before* the raise.
   - The per-run function-**source capture** after `record_run`
     (`_function_source`) belongs inside the transaction too. It is part of
     the run's record.
   - `Log.info` one line per run: `"[batch_save] {fn}: committed N record(s) +
     graph in one transaction"`. On rollback, log at ERROR:
     `"[batch_save] {fn}: rolled back — nothing from this call was saved"`.
4. **If D9 is changed to "keep partial":** instead, wrap each output's
   `save_batch` + that output's share of the graph. That needs `record_run`
   called per output, which is a larger change; re-plan before doing it.
5. **Tests** (`scidb/tests/test_save_provenance_failure.py`):
   - `test_record_run_failure_rolls_back_data`: same monkeypatch as stage 1.
     After the raise, `MyOut.load(...)` finds nothing, and
     `SELECT COUNT(*) FROM _record WHERE type='MyOut'` is 0.
   - `test_output_save_failure_rolls_back_other_outputs`: two outputs, force
     the second to fail (monkeypatch `save_batch` to raise on the second
     type). Assert the first output was not kept and the error names the
     second.
   - `test_no_record_without_output_edge`: after a normal 2-output run, every
     non-raw `_record` row of the output types has an `_invocation_output`
     edge. This is the invariant guard; keep it general.
6. **Journal / docs:**
   - check that `scistacklog`'s journal still says "Failed combos saved
     nothing; completed combos were kept" only for combo failures, and that a
     rolled-back save reads as a failed run;
   - update `docs/claude/error-surfacing.md` and
     `docs/claude/duplicate-location-vs-stale.md` if they describe partial
     saves;
   - add a decision line to `docs/claude/decisions.md`
     (D-2026-11-xx: for_each save is atomic).
7. User runs, one package at a time:
   - `python -m pytest sciduckdb/tests -x -q`
   - `python -m pytest scidb/tests -x -q`
   - `python -m pytest scimatlab/tests -x -q`
   - `python -m pytest scistack-gui/tests -x -q`

   The GUI shows `OutputSaveError` remedies, so check its tests.
8. Mark W1 done in `docs/design-review/04b-scidb-write-path.md`, and add a
   GUI manual-test entry: run a function whose output save fails, and expect
   the failure popup with nothing saved.

## Progress

**2026-11-01: stages 1 and 2 done; all four suites green (user run).**

Done:
- `_save_results`:
  - `record_run` failure → `save_failures` + `save_causes`, logged with
    `exc_info`;
  - phases 2–4 inside one `db.transaction(f"for_each_save({fn})")`;
  - `OutputSaveError(saved=0)` raised inside it;
  - `db` resolved once at the top (three `get_database()` branches gone);
  - the parameter is typed `DatabaseManager | None`.
- `sciduckdb`:
  - `SciDuck.transaction`, with depth-aware `_begin`/`_commit`/`_rollback`;
  - `TransactionRolledBackError` (exported);
  - `add_rollback_listener`.
- `DatabaseManager`:
  - public `transaction()`;
  - the rollback listener `_forget_uncommitted_caches` clears
    `_registered_types` and the schema-key kinds. Without it, a type first
    registered inside a rolled-back save would be trusted from memory with
    no DB row.
- `save_batch`, `_commit_graph` and `rename._write` use `with
  transaction(...)`. `save_batch` and `rename` used to issue a raw
  `_execute("ROLLBACK")`, which would have ended an outer transaction behind
  the depth counter (and left `_tx_owner` set).
- `OutputSaveError.blame` is `internal` when a cause is not an
  `ActionableError` (the provenance case).
- Tests:
  - new: `sciduckdb/tests/test_transaction.py`,
    `scidb/tests/test_save_provenance_failure.py`;
  - `test_output_save_error.py::test_the_other_output_still_saves` is now
    `test_a_failed_output_rolls_back_the_others` (contract changed by D9).
- Docs:
  - `decisions.md` D-2026-11-01-2;
  - `matlab-run-completion.md`;
  - 04b W1 done, 03 D2 partly done;
  - GUI manual-test item 0zzzzzzz.

Deviations from the literal steps (per the index's design bar):
- **Stop at the first failed output.** The plan's steps imply attempting the
  other outputs. Once one fails, the transaction is doomed: DuckDB has no
  savepoints, and if a SQL statement failed the transaction is already
  aborted. So later outputs, the generates_file phase and `record_run` are
  skipped (WARN logged), not attempted for spurious "transaction aborted"
  failures.
- **Source capture is now `_capture_function_source`.** Reading the source
  back stays best-effort. A failed INSERT fails the save, because swallowing
  it would surface later as an obscure commit error.
- **`test_output_save_failure_rolls_back_other_outputs` is not duplicated.**
  The updated `test_output_save_error.py` test covers it.
- **`DatabaseManager.save` (direct save) and four other `_begin` callers**
  (variant_delete, clear_data, GUI history, the row-list conversion) keep
  `_begin`/`_commit`/`_rollback`. These are now depth-aware and safe; they
  migrate with D2.
- **No change needed:** `scimatlab.bridge.for_each_save` has no swallowing
  `except`, and `_save_results` has one call site. The journal's
  "completed combos were kept" line only fires for combo failures with no
  run-level error, so it is already correct.

User to run (one package per invocation):
- `python -m pytest sciduckdb/tests -x -q`
- `python -m pytest scidb/tests -x -q`
- `python -m pytest scimatlab/tests -x -q`
- `python -m pytest scistack-gui/tests -x -q`

Last result: all four suites pass (2026-11-01), after updating two stale
expectations from other work: the `combine_rows` remedy step
(scimatlab) and the `opening_spec` describe phase (scistack-gui).
