# Plan — a dataset-intent bundle section (always exported, always applied)

*Status: written 2026-11-01, not started. Follow-up to P3 stage 6 step 7
(`.claude/plan-tidy-p3-homes.md`). User decision 2026-11-01: do "option 1 +
warnings" now (schema key types classified `BUNDLE_INTENT`, warnings when
intent is not applied), this plan later. Goal: bundles are truly portable.
A project's dataset rules always come with it; history and data are
optional extras.*

## Why

Dataset intent is the rules that govern a re-run: schema exclusions
(`__scidb_schema_overrides`) and schema key types (`_schema_key_type`). In
`scidb.tables` these are the tables declared `bundle=BUNDLE_INTENT`. Today
they ride inside the history section, so they are lost when:

1. the export has `include_history=False`;
2. the import has `import_history=False`;
3. the import remaps schema keys (`key_map`). Intent rows name the
   exporter's keys and are skipped.

Since 2026-11-01 each of these cases is a warning naming what was left
behind (`bundle._intent_not_applied`, manifest `intent_rows`). This plan
removes the loss itself.

Not in scope: canvas intent (`_intent`, GUI statements). It already travels
in the GUI section, resolved per pipeline scope. Keep the two names apart in
docs: *dataset intent* (scidb's rules about the data) vs *canvas intent*.

## Stage 1 — the section

1. `bundle.py`: `INTENT_SECTION = "intent"`, always written (no option),
   from `table_copy.dump(db._duck, INTENT_TABLES)`. Take `INTENT_TABLES`
   out of `HISTORY_TABLES`: `tables.history_tables()` returns
   `BUNDLE_HISTORY` only, and a new `tables.intent_tables()` consumer
   writes the section. One owner of membership: `TableDecl.bundle`.
2. Bump `FORMAT_VERSION` (beta: older bundles refused with the usual
   message, no shim).
3. Delete the archived-history special case (the `intent` subset block in
   `_import_history_and_data`) and `manifest["intent_rows"]`. The section
   itself says what there is. Keep the warning only for case 3's leftovers
   (Stage 2).
4. Import order: load the intent section right after the database opens
   and BEFORE history/data and the "database" phase providers, then
   `_refresh_intent(db)`, so key types canonicalize anything saved later.
   Log `[bundle] intent: N row(s) from M table(s), remapped K`.

## Stage 2 — remap by key map

`KeyMap` (`kmap.key(old) -> new | None`) already drives the config and
PathInput rewrites. Apply it here too:

- `_schema_key_type`: rewrite the `key` column through `kmap.key`.
- `__scidb_schema_overrides`: one column per schema key. Rename the columns
  through the map, then insert. `table_copy.load` keeps the target's DDL
  (the recipient's keys) and inserts shared columns, so rename the parquet
  columns before load (a `SELECT old AS new` in `table_copy`, or a
  `rename=` argument; decide on reading `table_copy.load`).
- **A dropped key** (`kmap.key(k) is None`):
  - a key type on that key is dropped;
  - an exclusion row with a non-null value on that key is NOT applied
    (it would widen to every location).
  Both are reported per row in the import report and as one warning. Never
  guessed. This is the one rule needing user sign-off; ask before Stage 2.
- **An added key** (recipient-only): exclusion rows get NULL there,
  meaning "any", matching how an exclusion on fewer keys reads today.
  Verify against `scidb.exclusions` before relying on it.

## Stage 3 — export options and UI

- No option to drop intent. If a user later wants a copy without
  exclusions, add `ExportOptions.include_dataset_rules` then, not now.
- Import preview (`preview_import`, GUI import dialog): list "Dataset rules:
  N exclusions, M key types", plus the remapped/dropped counts.
- Update `docs/sharing/export-import.md` + `.ui.md`, `docs/claude/portability.md`
  (E13, I14 rows, the import-modes table) and `bundleImportCore.ts` only if
  the summary needs a new line (warnings already list).

## Tests (scidb `tests/test_bundle.py`; one per row)

| export | import | expect |
|---|---|---|
| default (history, no data) | same schema | intent live, history archived |
| history + data | same schema | intent live (once, not twice), history live |
| `include_history=False` | same schema | intent live, no warning |
| default | `import_history=False` | intent live, no warning |
| default | renamed keys | intent live under new names |
| default | key map dropping a key with exclusions on it | those rows reported, not applied |
| — | old format version | refused |

Plus `test_table_registry`: `history_tables()` and `intent_tables()`
disjoint. Plus a key-types-canonicalize-after-import test: save `"001"`
in the imported project, find the exporter's `"1"` location.

## Run (user, one package at a time)

`scidb`, `scistack`, `scistack-gui` (bundle section tests, extension
`bundleImportCore.test.ts` if touched).
