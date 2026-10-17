# scidb.log triage — 2026-10-17 session (best_popcorn.duckdb)

## E1. GUI write produced invalid TOML (00:09:50, log line 228)
- Cause: `scidb.entities.upsert_entry` -> `_section_insert_offset` -> `_back_up_over_blanks(text, n)`
  returns `n` when the file has no trailing newline; the new `popcorn_path = ""` line is glued onto
  `cbrace_cosmed_path = "data/...6MWT.xlsx"` (78 chars -> error at column 79).
- Effect: the entities file was rejected wholesale; registry dropped CosmedData, cbrace_cosmed_path;
  the write still returned success.
- Fix (scidb owner): `upsert_entry` (and `add_variable`/`rename_entry` audit) inserts at a line start,
  adding `\n` if `text[insert_at-1] != "\n"`. Add one guard in scidb, e.g. `entities.checked(text)`,
  that `tomllib.loads` the result before any GUI write; `write_entity`/`update_declaration` refuse
  to write unparseable text (ValueError -> error dict), logging the offending line.
- Tests: upsert into a `[path_inputs]` section at EOF without trailing newline; parse round-trip;
  write_entity refuses an unparseable result and leaves the file untouched.

## E2. ColumnSetChangedError on RatingsData (00:13:11, 00:13:30)
- Root cause: the first run (00:11:31) loaded a CSV whose person-ID column was `participant`,
  not the schema key `person`, so rows spread by `brand` only: 100 records per brand location.
  Renaming the column (`people`, then `person`) changed the column set -> guard fired as designed.
- The guard and its advice were correct (user followed it: new variable `Rating`, run succeeded).
- Fix (scifor owner, `_spread_decision` / spread path): WARN when a spread still leaves >1 row per
  address, naming the unpinned schema keys absent from the DataFrame (`person`) — the same
  "silently multiplied records" bug class the docstring already describes. Optional: the
  ColumnSetChangedError text names a missing column whose replacement is now a schema key.
- Tests: scifor spread with one of two keys present -> WARN names the absent key.

## E3. Function node green with only an empty output (00:13:50)
- `fn__pandas.read_csv` was DB-derived from the RatingsData call site. User hid RatingsData + its
  edge and wired a manual edge to new, empty `Rating`. State stayed green: computed from the hidden
  RatingsData records; the manual output edge was not considered.
- Fix (owner: run_state inputs): output-side mirror of `disconnected_fkeys` /
  `manual_input_overrides` — visible edges are ground truth. The outputs checked for a call site's
  own state = DB outputs minus hidden output edges/nodes, plus manually wired outputs. An output
  with no records -> red. Log the decision per node (like the manual-fn-node INFO line).
- Tests: DB-derived fn, hide its only output, wire a new empty var -> red; run -> green.

## E4. Plot Studio render crash (00:16:30)
- `TypeError: Cannot convert undefined or null to object` in `dictId` (differenceBars.ts:78) via
  `barsForPanel` in DifferenceBarsSection: a bar's or panel's `match` was undefined. Python always
  emits `match` (single panel => `{}`), so something client-side lost it. Not reproduced.
- Fix: `dictId(values ?? {})` plus a client WARN (reportClientError path) naming which object
  lacked `match` and its keys, so the next occurrence names its source. Test in differenceBars.test.ts.
- Need from user: what was clicked at 00:16:30 (after adding Butterburst–SeaSaltCo bar).
