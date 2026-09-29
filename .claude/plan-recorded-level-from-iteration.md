# Plan: never re-run finer than the inputs carry (recorded level narrowed)

## Bug (scidb.log, run n1irqety, 2026-09-29 09:49:40)

`pandas.read_csv` <- `DemographicsPath` (no placeholders) re-ran at `[subject]`:

    [schema-level] ... stated=unset -> iterating [subject] (the level it last ran at)
      [call sites 1, recorded ['subject'], input levels [[]]]

18 iterations each read the whole CSV; the pinned `subject` column was dropped;
18 copies of the 16-row table saved; Plot Studio's `Intervention Group` join
"took the first" row -> every subject = 'Digitimer'.

## Root cause

`provenance_query.recorded_schema_keys` reads the level off where the last
run's RECORDS landed. One call returning a table with a `subject` column is
spread into one record per subject (runs oxbyiebp / ogrvlj29, 2026-09-28), so
the one-call run was read back as "ran at subject". Recorded outranks inputs.

## Fix chosen (user, 2026-09-29): no new storage

Rejected: a `_run_schema_level` table recording the iterated keys (exact, but
new storage; a `_run` column would need a migration).

`scidb.schema_level.resolve_schema_level` (the one owner): when input levels
are known, the recorded level is narrowed to the keys the inputs' union
carries. New rule `RULE_RECORDED_NARROWED`; INFO log names dropped keys.
Constants-only (no input levels) is not narrowed; a stated level still wins.

## Logging
- scidb `[schema-level] recorded level [...] narrowed to ...` (INFO).
- scifor `_warn_repeated_static_path_inputs`: WARN when >1 iteration and no
  input varies (all placeholder-free PathInputs / constants).
- scistackplotdb `_one_label_per_location`: names an example location's
  labels; hints "one whole table saved at every location" when all ambiguous
  locations hold the same set.

## Tests (one package per pytest run)
- scidb/tests/test_schema_level.py: TestRecordedNarrowedToInputs +
  test_a_spread_one_call_run_reruns_as_one_call (end to end).
- scifor/tests/test_static_pathinput_warning.py.
- scistackplotdb/tests/test_factor_variables.py: whole-table case + extended
  unpinned-grouping assertion.

## Follow-up built 2026-09-29: same-invocation newest-save-wins ("option 3")
`provenance_query._supersede_same_invocation`, called from
`variant_identity_batch`: of the still-latest records ONE invocation wrote to a
location, only the newest save is latest; same-instant saves stay latest
together. Per location — a location the newer run never wrote keeps its record
(hide it). INFO log with an example location. Tests:
scidb/tests/test_latest_same_invocation.py (incl. an xfail marker for the
skip-gate gap -> .claude/plan-input-file-fingerprints.md). All tests pass.

## User cleanup
Re-run read_csv (now one call); hide the 18 stale whole-table
DemographicsTable records (never delete).
