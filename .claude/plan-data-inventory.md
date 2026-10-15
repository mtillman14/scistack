# Plan: Data inventory (missing schema-key combinations)

Drafted 2026-10-15. Status: all 6 stages implemented 2026-10-15; pytest unrun; GUI §0zzzt unchecked.

## What it answers

"Which combinations of the schema keys I chose have no `_schema` row?"
The set of all combinations is the Cartesian product of each chosen key's
values, and the result is that product minus the combinations that exist.
The check looks only at the database, not at any Variable.

## Decisions (from the user, 2026-10-15)

1. The user picks which keys to cross. Default: every schema key.
2. The report is at exactly the chosen key set. A deeper `_schema` row counts
   for its projection onto the chosen keys (`S01/T2/trial3/cycle5` makes
   `S01/T2/trial3` present when cycle is unchecked).
3. A `_schema` row is enough to count as present. Record counts don't matter.
4. Combinations matched by an active `exclude_schema` get the status
   **excluded** (not missing).
5. Rows where any chosen key is NULL can't fill a cell and are ignored.
6. No Variable mode for now.
7. In the popup, the user chooses between "missing only" and "all
   combinations with a `status` column". The CSV export follows that choice.
8. Entry points: a GUI toolbar button and a `scidb inventory` CLI command. No
   MATLAB entry point.

## Decisions I made (flag if wrong)

- **Where each key's values come from**: every value the key has anywhere in
  `_schema` (`db.distinct_schema_values`). This means a subject that exists
  only at subject level shows all of its deeper combinations as missing. It
  also uses the declared `[schema_keys]` level order, so rows sort the same way
  as for_each and the figures.
- **Excluded takes precedence over present**: an excluded combination reads
  `excluded` whether or not it has a row.
- **Size guard**: the product size is computed *before* building it. Above
  1,000,000 combinations the call raises an error naming the size and the
  per-key value counts, and suggests unchecking a key.
- **Popup display cap**: the backend sends at most 5,000 rows to the popup,
  plus the full counts. The CSV export always writes the full set.

## Owners (NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| Each key's values (in declared order) | `DatabaseManager.distinct_schema_values` (existing) | inventory |
| Combinations that exist | `DatabaseManager.distinct_schema_combinations` (existing) | inventory |
| Is a combination excluded? | NEW `scidb.exclusions.exclusion_status(combos, ...)` → `list[bool]` | `filter_excluded_combos` (refactored to use it), inventory |
| The inventory itself | NEW `scidb/inventory.py::schema_inventory(db, keys=None)` | `Inspector.inventory`, CLI, GUI handler |
| CSV layout | NEW `scidb/inventory.py::write_inventory_csv(inv, path, missing_only)` | CLI `--csv`, GUI export |

The CLI and the GUI never compute statuses or format CSV rows themselves.

## Stages

### Stage 1: scidb owner
- `exclusions.py`: extract the per-combination matching loop out of
  `filter_excluded_combos` into `exclusion_status`. `filter_excluded_combos`
  becomes `[c for c, ex in zip(combos, exclusion_status(...)) if not ex]`, and
  its INFO log is unchanged.
- `inventory.py`:
  - `SchemaInventory` dataclass: `keys`, `levels: dict[key, list]`,
    `rows: list[tuple[values..., status]]`, `counts: {present, missing,
    excluded}`, `product_size`.
  - `schema_inventory(db, keys=None)` validates the keys (unknown key →
    ValueError; empty list → ValueError), puts them in schema order, applies
    the size guard, builds the product with `itertools.product` over the
    levels, fills in present (a set of tuples) and excluded, and returns the
    result.
  - `write_inventory_csv(inv, path, missing_only)`: long format, one column per
    chosen key in schema order, plus `status` unless `missing_only`.
- Logging (NOTE 2): INFO `schema_inventory`: keys, per-key value counts,
  product size, present/missing/excluded counts, elapsed ms. A size-guard
  refusal is logged as WARNING before raising.
- `Inspector.inventory(keys=None)` delegates to the owner (`@_timed`).

### Stage 2: CLI
`scidb inventory [--keys subject,session,trial] [--all] [--csv PATH] [--json]`
- Default: a summary line of counts, then a table of the missing combinations.
- `--all`: every combination, with a status column.
- `--csv PATH`: writes through `write_inventory_csv` (respects `--all`).
- Also available as `scistack db inventory` through the existing subparser.

### Stage 3: GUI backend
`scistack_gui/api/schema.py` gets two new handlers:
- `schema_inventory {keys, missing_only, limit=5000}` →
  `{keys, columns, rows, counts, product_size, truncated}`
- `export_schema_inventory {keys, missing_only, out_path}` → `{path, rows}`

The size-guard ValueError reaches the popup as a normal RPC error.

### Stage 4: GUI frontend
- A `📋 Inventory` toolbar button in `App.tsx`, next to 📁 Paths.
- `components/InventoryPopup.tsx` (modal, uses `modalStyles`) contains:
  - one checkbox per schema key, in schema order, all checked by default
  - a Missing only / All toggle
  - a counts line
  - a scrollable table with a sticky header and colour-coded status
  - a "showing first 5,000 of N" note when truncated
  - an "Export CSV…" button that calls `pick_save_path` (filterName "CSV")
    and then `export_schema_inventory`; in the browser it falls back to
    `window.prompt`, as `ProjectExportButton` does.
- The query re-runs when the key checkboxes or the toggle change.
  Run is disabled when no key is checked.
- Rebuild BOTH vite targets.

### Stage 5: Tests (the user runs them, one package at a time)
- `scidb/tests/test_inventory.py`:
  - deeper rows count once the extra key is dropped
  - rows with a NULL chosen key are ignored
  - values come from rows at every level
  - excluded via a wildcard row; `include_schema` overrides it
  - an exclusion on an unchosen deeper key does NOT exclude the coarser
    combination
  - excluded takes precedence over present
  - declared level order and zero-padded values are kept as stored
  - the size guard raises
  - an empty DB gives an empty inventory
  - an unknown or empty key list raises
  - CSV layout in both modes
- The existing exclusion tests must still pass unchanged (they cover the
  refactor).
- A CLI test for `inventory` with `--all`, `--csv` and `--json`.
- `scistack-gui/tests`: handler tests for truncation and for export.

### Stage 6: Docs
- `docs/claude/data-inventory.md`: semantics, owners, and the
  excluded-over-present rule.
- `scidb/README.md`: a short section.
- `docs/gui-manual-testing-todo.md`: a new § for the popup.
