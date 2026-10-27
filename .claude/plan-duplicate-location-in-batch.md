# Plan: refuse a save batch that puts two records at one location

Status: BUILT 2026-10-27, all stages; Python + MATLAB tests not yet run by the user.
Frontend: 650 TS tests pass, tsc clean, both bundles rebuilt.

**Change during the build:** schema keys are the columns of `_schema`, fixed at
database creation (no add-key API), NOT `scistack.toml` (`[schema_keys]` there is
level ORDER only). User decision: the "Change the dataset schema" step only
explains (action null); there is no Open-schema-keys button. Text below that
says otherwise is superseded.
Concept doc: `docs/claude/duplicate-location-vs-stale.md`

## Problem

A CSV is loaded once (no schema-key iteration) and its output DataFrame has a
`subject` column, so the save splits it into one record per row
(`BaseVariable.save` -> `save_from_dataframe`, `variable.py:430-447`; for_each
and MATLAB reach the same `save_batch`). Subject 2455369 has two differing rows,
so it gets two records at one location, saved at the same instant by one
invocation. `_supersede_same_invocation` keeps a batch's own records latest
together, so both stay "latest" and readers report a stale value. It is really
a duplicate row in the input data.

## Decisions

- Check at **save time** in `DatabaseManager.save_batch`
  (`scidb/src/scidb/database.py:2009`), the one place all three save paths go
  through (`variable.py:763`, `foreach.py:5987/5998`, `scimatlab/bridge.py:2205`).
- **Refuse the batch before anything is written** (pattern: `ColumnSetChangedError`).
- Q1: there will be no "allow duplicates / keep last" option.
- Q2: databases from before this fix get no detection or migration tool (beta).
- The GUI shows a **popup** that explains the problem and gives two ways to
  fix it: change the dataset schema, or change the data.

## Stage 1: the rule (scidb)

Two items in one batch **collide** when they have all of these:
1. the same schema location: the `(schema_level, key_tuple)` that `save_batch`
   already builds as `combo_key` (around line 2243)
2. the same `nested["version"]` (includes `__invocation_id`)
3. the same `__branch_params` (stripped before step 2, so compare it separately;
   different constants at one location are separate variants, which is allowed)
4. **different** record_ids (byte-identical rows already merge into one
   record, so there is nothing ambiguous to report)

This is a per-location rule, not a content comparison across locations. It does
not bring back the removed "IDENTICAL data" warning.

- A pure helper, `_colliding_locations(nested_list, branch_params, record_ids,
  data_items)`, is the only implementation of the rule. It also works out which
  data columns differ between the colliding items (DataFrame/dict mode; for
  single_column the answer is "the values differ").
- `save_batch` calls it after the record_ids are hashed and before the first
  INSERT, and raises `DuplicateLocationError`.

## Stage 2: the error owns its explanation (scidb + scifor)

`DuplicateLocationError(SciStackError)` in `scidb/exceptions.py`, with fields
`variable`, `collisions` (list of `{location: {key: value}, count, differing_columns}`),
`n_locations` and `schema_keys` (the dataset's declared keys).

**The wording has one owner, the exception.** The terminal, the journal, the
FailuresPanel, the toast and the popup all read it from there:
- `str(exc)`: a one-paragraph text message (shown in the terminal, MATLAB and the journal).
- `exc.remedy`: a structured dict that the GUI renders without rewording it:
  ```
  {kind: "duplicate_location",
   title: "Two rows for one <deepest key> in <Variable>",
   explanation: "...the dataset's schema keys (<keys>) give these rows the same
                 location, so SciStack can't tell which one is the value...",
   items: [{location: "subject=2455369", count: 2, differing_columns: [...]}, ...≤20],
   n_more: int,
   steps: [
     {title: "Change the dataset schema",
      text:  "If the rows are separate observations (e.g. two visits), add a
              schema key that tells them apart, and make sure the data has
              that column.",
      action: "open_schema_keys"},
     {title: "Change the data",
      text:  "If only one row is right, remove the other in the source file. If
              they should be combined, aggregate them in your function before
              returning.",
      action: "reveal_function"}
   ]}
  ```
- Generic carrier: `ActionableError` (scifor) gets an optional `remedy` property
  (default `None`), so later errors can opt in with no GUI change.
  `ComboFailure` gains `remedy` (filled in by `failure_from_exception`, which
  walks `__cause__`/`__context__` as it already does), and
  `FailureGroup.to_dict` carries it. `OutputSaveError` passes through the remedy
  of its first cause that has one.
- `groups_from_wire` keeps `remedy` when present (it is a dict, so it goes through JSON unchanged).

## Stage 3: run path

- for_each: `DuplicateLocationError` from `save_batch` goes into
  `OutputSaveError` failures. The other outputs still save, and the run is
  marked failed. No new catch.
- GUI server: `RunFailures.add_exception` already turns the escaped
  `OutputSaveError` into a run-level group. Because of Stage 2 the group
  carries `remedy` with no new code here. Add one log line:
  `[run_failures] remedy=<kind> on node <id>`.

## Stage 4: GUI popup (frontend + extension)

- **Frontend `RemedyDialog.tsx`** (uses `modalStyles.ts`, like the other popups):
  - Opens automatically when `run_done.failures` has a group with `remedy`.
    `NodeFailuresContext` is the one place that decides to open it
    (rule in `DAG/runFailures.ts`: `firstRemedy(failures)`).
  - Shows the title, explanation, a table of duplicate locations (location,
    count, differing columns, "+N more"), and the two steps, each with its
    button:
    - **Open schema keys**: asks the server to reveal the project's
      `scistack.toml` at the `[schema_keys]` line. The path comes from the
      existing project-config owner (`schema_order.project_level_order`
      locator); the reveal uses the existing `reveal_in_editor`. If there is no
      declaration file, the button says where the keys come from instead.
    - **Open function**: reveals the node's function source (the same reveal as
      the FailuresPanel).
  - Also has "Show failures" (opens the FailuresPanel for the node) and Close.
  - The FailuresPanel entry for a group with `remedy` gets a "How to fix…"
    link that reopens the dialog.
- **Extension toast**: it stays as it is (blame label + "Show failures"). It
  does not get its own remedy text, so the only copy of the wording is in the
  error.
- Rebuild both bundles (see memory project_frontend_bundle_rebuild).

## Stage 5: MATLAB

- The text message (`str(exc)`) already reaches the MATLAB console through the
  bridge. Confirm with a test.
- GUI popup for MATLAB runs: the bridge raises an MException with the
  identifier `scistack:duplicateLocation`, and `failure_groups` asks Python for
  the remedy from the error text, the same way it already asks for
  `blame_label`. If that turns out to need its own wire field, it can become a
  separate follow-up, but it is in scope here.

## Logging (NOTE 2)

- DEBUG in `save_batch`: `save_batch(X): N item(s) -> L location(s), V variant
  group(s), R distinct record_id(s)`.
- WARN before the raise, with the full list of collisions (the error message
  shows at most 5).
- `failure_from_exception: ... remedy=<kind>` added to the existing DEBUG line.
- Frontend: `[remedy] open kind=<k> node=<id>` and `[remedy] action=<a>` client log lines.

## Tests

Python (`scidb/tests/test_duplicate_location_in_batch.py`):
1. A DataFrame with two differing rows for one subject raises
   `DuplicateLocationError`, and the variable has 0 records afterwards.
2. The message and `remedy` name `subject=...` and the differing column(s);
   `remedy.steps` has exactly the two actions.
3. Two byte-identical rows save fine as 1 record.
4. Two items at one location with different `__branch_params` save fine as 2 records.
5. A re-run in a separate batch raises no error, and the old record is
   superseded (guards `test_latest_same_invocation.py`).
6. A for_each output with a duplicated subject raises an `OutputSaveError` that
   carries the remedy; the other outputs are saved.
7. Unit tests of the helper: a single item, NULL schema keys at a coarse level
   (the dropna trap).

scifor (`scifor/tests/test_failures*.py`): `remedy` round-trips through
`failure_from_exception` -> `FailureGroup.to_dict` -> `groups_from_wire`; a
cause chain finds it.

GUI Python (`scistack-gui/tests`): `RunFailures.add_exception(OutputSaveError(...))`
-> the wire group has `remedy`.

Frontend TS: `firstRemedy` picks the group; the dialog renders the items and
"+N more"; each button sends the right request.

MATLAB: a for_each saving a table with a duplicated subject shows the
message, and the failure group carries the remedy.

The user runs all tests, one package at a time. The manual GUI check is added
to the GUI manual-testing doc.
