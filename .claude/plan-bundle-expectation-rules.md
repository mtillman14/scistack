# Plan: expected-locations rules through export / import

Status: changes 1 and 2 implemented 2026-11-08; change 3 is discussion only.

## What already happens (read from scidb/src/scidb/project/bundle.py)

The tables:
- Dataset rules (BUNDLE_INTENT): `_schema_key_expectation`,
  `_expectation_mark`, `__scidb_schema_overrides` (exclusions) and
  `_schema_key_type`.
- `_run_outcome` is BUNDLE_HISTORY.

| Bundle / choice | Rules | Run outcomes |
|---|---|---|
| history + data, same schema | live (verbatim) | live |
| history only, same schema | live | archived |
| no history, or history declined | warned, not applied | none |
| another schema | warned, not applied | archived |

## Change 1: a test per row (scidb/tests/test_bundle.py)

A source project with:
- a key setting with declared values;
- a value-naming mark and a variable-wide mark;
- an exclusion;
- a key type;
- a recorded run outcome.

Then one test per row:
- the rules are live or not, and the outcomes are live or archived;
- the warning names "Expected / Not expected mark(s)" and "schema key
  expectation setting(s)" and how to restore them.

## Change 2: `value_rules` at import

`import_project(..., value_rules=True)`. False means "this project will run on
other data". The rules that name the exporter's VALUES are then left out. The
rules that name only KEYS still go live.

| Table | Value-free part kept | Left out |
|---|---|---|
| `_schema_key_type` | all | nothing |
| `_schema_key_expectation` | sharing (shared / per parent) | `declared_values` (set NULL) |
| `_expectation_mark` | marks with every key NULL (none today: a mark must name a key) | every mark |
| `__scidb_schema_overrides` | rows with every key NULL (none today) | every exclusion |

Decisions:
- **Exclusions are included.** Exclusions name values exactly as marks do: an
  exclusion of S03 in someone else's dataset excludes their S03. Flip by
  moving the table to "all" in `_VALUE_FREE`.
- **One owner.** `bundle._value_free(schema_keys)` classifies every intent
  table. A test fails when an intent table is unclassified, so a new rule
  table must decide.
- **Loading.** `table_copy.load(keep=, clear=)` filters while loading and
  returns `left_out` / `cleared` counts. Nothing is loaded and then deleted.
  The left-out rows stay in the archive, verbatim.
- **Data.** With `value_rules=False`, a bundle's derived data is not imported.
  It describes the exporter's dataset, the same reason as another schema.
- **Export.** The manifest gains `value_rule_rows` (rows naming values, per
  table), so the import dialog asks only when there is something to leave out.
- **Surfaces.**
  - CLI: `--value-rules / --no-value-rules` (default from the signature,
    like `--history`).
  - Headless: `import_project_bundle(value_rules=)`.
  - The extension's import dialog asks "Same data or other data?" when the
    bundle has value rules and history is imported.
  - The report section `history.value_rules_left_out`.
  - One warning naming each count and the archive.

## Change 3 (discussion, not built)

When the rules convert through a key map, and doing it after the import from
the archive: see the chat answer of 2026-11-08. The summary:
- Convert per RULE, not per map. A rule converts when every key it NAMES maps
  to a recipient key.
- Recipient-only keys are NULL ("every value"), which is correct.
- A rule naming a dropped key is left out and reported.
- Doing it later from the archive (`.scistack/archive/<stamp>/` + `import.json`
  key map) through the normal writers is possible and is the better home.
