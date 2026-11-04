# Plan: distribute replaces combine, plus the multi-output fixes

Status: drafted 2026-11-04, awaiting approval. Nothing built.
Evidence: `scistack-debug.log` (project SRALab-TSS-data-pull, runs 97c4f75e to 041973b8) and `parse_data.py`.
Supersedes: `.claude/plan-combine-duplicate-locations.md`.

## Problem

A CSV has 34 rows for 33 subjects, under schema `[subject, bout, date]`. `parse_data` returns one frame per
`[subject, bout]` and one per `[subject, bout, date]`. Today:

1. `on_duplicate="combine"` puts the second stay into list cells (76 columns), so functions must unpack them.
2. Two-output runs never spread, so both outputs save per `subject`.
3. A two-output run claims only its first output, so the canvas grows a duplicate node.
4. Saving node settings erases the node's declared output ports.
5. A datetime schema key is stored with plain `str()` and loads as a string.

## Decisions (user, 2026-11-04)

| Topic | Decision |
|---|---|
| `distribute` level | Always one level below the deepest key that is iterated OR carried as a column. Never depends on whether duplicates exist; single-row subjects still get `bout=1`. |
| Bout numbering | File row order, 1-based within each group, logged per group. |
| `combine` | Removed completely: `on_duplicate`, `row_list` storage, Plot Studio's `Row` factor. No deprecation (beta). |
| Missing key in spread rows | Raise an error naming the rows. |
| `date` key | May be a calendar day or an exact timestamp. Must round-trip idempotently. |

## Expected result on the user's project

| Variable | Records | Level |
|---|---|---|
| `LoadedData` (`read_csv`, `distribute=True`) | 34 | `[subject, bout]` |
| `ParsedSubjectData` | 34 | `[subject, bout]` |
| `ParsedDateData` | one per distinct date | `[subject, bout, date]` |

All three need clearing and re-running. `parse_data` drops `iterrows`, `explode` and its `bout` column, and gets
the no-date row fixes (one row per date, not per measure).

## Stages

Each stage: logging for the decision it makes, plus regression tests. Tests are written here and run by the user,
one package at a time. Stages are committed separately. The uncommitted submodule work and the root `parse_data.py`
stay out of these commits.

### Stage 1: duplicate node

- Cause: `run_service.py:381` passes `record_dispatch_wirings` the targets AFTER `deduplicate_variants`, which keys
  on constants only (`variant_resolver.py:114`) and so drops all but the first output. The claim is hash(first output);
  `split_call_site_outputs` then treats the other output as unclaimed and a new node is minted.
- Fix: claim from the targets before de-duplication.
- Test: through the real start-run path, a first run of a two-output node leaves ONE node with both outputs, and
  stays one node after a re-run. The existing test calls `record_dispatch_wirings` directly, past the dedupe.
- Log: the existing claim line must show both outputs.

### Stage 2: erased ports

- Cause: `layout_service.put_node_config` calls `pipeline_store.update_node_config`, which replaces the whole blob.
  The settings save sends four keys, so `outputPorts` (and probably `shownArguments`) are lost.
- Fix: `put_node_config` merges into the stored config. First confirm the frontend sends every settings key on each
  save, so clearing a setting still works (it sends an empty value, not an absent key).
- Test: save settings on a node with declared ports and revealed arguments; both survive. Clearing a filter still clears.
- Log: `update_node_config` logs keys written and keys preserved.

### Stage 3: per-output spreading and the missing-key error

- Cause: `scifor/foreach.py:3319` turns spreading off whenever there is more than one output, because spreading
  concatenated all outputs into one row set (2026-10-28).
- Fix: keep one result row per combination with each output's frame in its own cell (as now), and expand each output
  on its own at save time. One owner in scifor decides, per frame, using the existing `_spread_decision` rule, and
  returns `(address, piece)` pairs. scidb saves them; each piece keeps its combination's provenance edges.
- A blank value in a spreading key column raises, naming output, combination and row positions. Nothing from the call is saved.
- Tests extend `scidb/tests/test_multi_output_frames.py`: the two levels above; same-named columns stay separate; a
  one-row and a spreading output together; the blank-key error. MATLAB parity through `spread_nested_results`, which
  currently keeps multi-output runs nested.
- Log: one line per output with its level and record count.

### Stage 4: the new `distribute` rule

- One owner, new `scifor/distribute.py`: given schema keys, pinned keys and the frame's columns, return the target
  key, and number rows within each group of keys above it.
- Target = the first schema key below the iterated keys that the frame does not carry. No key columns: same as today.
- Errors: nothing left below (message: turn `distribute` off, the spread rule files labelled rows); a carried key
  below the target (`subject` and `date` without `bout`); a blank carried key (names the rows); different targets
  across combinations of one output.
- The target now depends on returned columns, so resolve it per output at run time. `plan.distribute_key` stays as the
  key for unlabelled outputs. Update the dry-run wording and check `filter_keys` in `matlab_loop.py:1109`.
- MATLAB: `collect_distributed` in `+scifor/for_each.m` uses the same owner. Shared cases in a new
  `docs/claude/distribute-cases.json`, read by both test suites (the `key-match-cases.json` pattern).
- Check first with a test: `_distribute_rows` appends `(piece,)` for every output, which looks like it files every
  output under the first output name. Fix it here if the test confirms it.
- Update tests that pin the old column-reading behaviour, including
  `test_distribute_reads_target_key_column_when_present`, `test_distribute_column_reading_top_of_schema_nothing_iterated`
  and MATLAB `test_distribute_session_column_in_output`.
- Log: target, why it was chosen, and a per-group row count at INFO.
- GUI: the Distribute tooltip and the output panel say where rows go, for example "rows go to `bout` = 1, 2".

### Stage 5: remove `combine`

Only after Stage 4 passes. Remove:

- scifor: `combine.py`, the option in `options.py`, README.
- scidb: `run/combine.py`, and uses in `run/foreach.py`, `run/foreach_call.py`, `store/database.py`,
  `store/record_store.py`, `model/variable.py`, `model/exceptions.py`, `__init__.py`.
- sciduckdb: `row_lists.py`, uses in `codec.py`, `__init__.py`, README.
- MATLAB: `+scidb/for_each.m`, `+scidb/BaseVariable.m`, `bridge/entry.py`.
- Plot: `scistackplot/reduce/rows.py`, `__init__.py`, `export/codegen.py`; `scistackplotdb/load.py`, `source.py`.
- GUI: `domain/run_options.py`, `services/run_failures.py` and the other services that pass it, `api/layout.py`,
  `RemedyDialog.tsx`, `FunctionNode.tsx`, `sidebarBadges.ts`, `runFailures.ts`, `api.ts`, generated API files.
  Rebuild both bundles.

`DuplicateLocationError` stays. Its message and remedy popup offer `distribute` when a lower key exists, naming the
target; otherwise a schema or data change. Replace `docs/claude/duplicate-location-vs-stale.md` §6.

Keep a parity guard that a duplicate location is refused in both languages. Do not add a test pinning that the removed
modules are gone.

### Stage 6: datetime schema keys

Today one concept has several spellings: `scidb` `schema_str`, `sciduckdb` `_schema_str`, `_stringify_meta` in
`run/foreach.py` (plain `str`), and scifor's `key_text`. Merge them to one owner.

| Value | Stored text | Loads as |
|---|---|---|
| `datetime.date` | `2026-07-09` | `datetime.date` |
| naive `Timestamp`, `datetime`, `np.datetime64` | `2026-07-09T15:12:00` | naive `Timestamp` |
| aware value | `2026-07-09T15:12:00-05:00[America/Chicago]` | aware `Timestamp` |

- Fractional seconds appear only when non-zero, to 9 digits. Zone labels come from `scicanonicalhash.datetimes`.
- A midnight timestamp and a day are different addresses, deliberately.
- Whether a key is datetime is a property of the KEY, like `schema_key_is_numeric`: all stored values have the exact spelling.
- Invariant, tested as a property: `text(restore(text)) == text`, and save then load then save gives "0 new rows" at the same address.
- `scifor/keys.py` adds a datetime kind to the key-match rule and `key-match-cases.json`. MATLAB datetimes cross using the same spelling.
- User-side note: `_parse_dt` turns a `%m/%d/%Y` value into an aware midnight timestamp. To keep a calendar day, return a `datetime.date` for that format.

### Stage 7: data preview

- Owner: `scidb.inspect`. For a variable or a column selection, at a chosen location: per column the stored type, the
  loaded type, non-null count and sample values, plus the rows. It reads through the same loader as `for_each`, so it
  shows what a function receives.
- CLI: `scidb show ... --types`. GUI: a popup from a variable node and from an input port. GUI only displays it.

### Stage 8: debugger (prototype first)

- Attach with `justMyCode: true` (`dagPanel.ts:819`) and send `setExceptionBreakpoints` with `userUnhandled` through
  `session.customRequest`, so VS Code stops on the failing line with live variables.
- Name the worker thread after the combination, shown in the Call Stack panel; show a "stopped at subject=..." message
  from the `run_progress` metadata already sent.
- Prototype questions: does the filter survive VS Code re-sending breakpoints; editable installs count scifor as user
  code; `pandas.read_csv` has no user frame. If the prototype fails, stop and report options.

### Small fix, with Stage 3

Failure messages say `your function fn`: the wrapper at `scidb/run/foreach.py:1049` hides the name. Report `parse_data`.

## Not in this plan

- `declared_python_ports` keys ports by function name, so two nodes of one function share ports. Recorded as a follow-up.
- The `source NOT captured ... recipes have drifted` warning needs its own diagnosis.
- Overlapping `set_output_ports` requests at 13:48:10, which settled correctly.

## Docs

Update `docs/claude/distribute-vs-spread.md`, `output-order.md`, `docs/guide/for_each.md`, `docs/guide/variables.md`,
and `docs/gui-manual-testing-todo.md` for each GUI stage. New `docs/claude/run-claims-and-splitting.md` if the user wants it.
