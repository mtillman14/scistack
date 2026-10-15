# Data inventory (missing schema-key combinations)

Plan: `.claude/plan-data-inventory.md` (2026-10-15). Code: `scidb/inventory.py`.

## The question

"Which combinations of the schema keys I chose have no `_schema` row?"
The answer only looks at the database. It never looks at a Variable, and
there is no per-Variable mode.

## Semantics

1. **Keys.** The caller picks the keys to cross; `None` means every schema key.
   They are always put back in schema order. An empty list or an unknown key
   raises `ValueError`.
2. **Universe.** It is the Cartesian product of each chosen key's values, and
   the values come from `DatabaseManager.distinct_schema_values`. That means
   (a) every value the key has in **any** `_schema` row, at any depth, so a
   subject that exists only at subject level shows all of its deeper
   combinations as missing, and (b) the declared `[schema_keys]` level order,
   so rows sort the way `for_each` and the figures do. Values are the stored
   strings (`"01"` stays `"01"`).
3. **Present.** Some `_schema` row projects onto the combination
   (`DatabaseManager.distinct_schema_combinations`). A deeper row counts:
   `S01/T2/trial3/cycle5` makes `S01/T2/trial3` present when cycle is not
   chosen. A row that is NULL at any chosen key fills nothing. Record counts
   don't matter; one `_schema` row is enough.
4. **Excluded.** `scidb.exclusions.exclusion_status` says an active exclusion
   matches the combination. **Excluded wins over present.** The combination
   holds only the chosen keys, so an exclusion row that names an unchosen,
   deeper key (`trial=3`) does **not** exclude the coarser combination. This is
   the same rule `for_each` applies when it iterates a subset of keys.
5. **Missing.** Neither of the above.

## Size guard

The product size is computed from the per-key value counts **before**
anything is built. Above `MAX_COMBINATIONS` (1,000,000) the call logs a
WARNING and raises `ValueError`. The message names the size and the per-key
counts and suggests unchecking a key. Tests pass `max_combinations=` to
exercise this.

## Owners (CLAUDE.md NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| Each key's values, in declared order | `DatabaseManager.distinct_schema_values` | inventory, `for_each` |
| Combinations that exist | `DatabaseManager.distinct_schema_combinations` | inventory |
| Is a combination excluded? | `scidb.exclusions.exclusion_status` | `filter_excluded_combos` (`for_each`), inventory |
| Statuses | `scidb.inventory.schema_inventory` | `Inspector.inventory`, CLI, GUI handlers |
| Which columns/rows a view shows | `scidb.inventory.inventory_table` | `SchemaInventory.to_dict`, `write_inventory_csv`, `render.render_inventory` |
| CSV layout | `scidb.inventory.write_inventory_csv` | CLI `--csv`, GUI export |

There are two views. **Missing only** shows only the missing rows, with key
columns and no status column (every row would say "missing"). **All** shows
every row plus a `status` column. The CLI, the GUI and the CSV export all get
both views from `inventory_table` and never filter or format rows themselves.
`SchemaInventory.to_dict(missing_only, limit)` is the JSON shape used by both
`scidb inventory --json` and the GUI. It returns `columns`, `rows`,
`total_rows`, `truncated`, `counts`, `product_size`, `levels` and `keys`.

## Entry points

- Python: `schema_inventory(db, keys)` and `db.inspect.inventory(keys)`.
- CLI: `scidb inventory [--keys a,b] [--all] [--csv PATH] [--json]`, also
  available as `scistack db inventory`. It opens the database read-only, like
  every Inspector command.
- GUI: the 📋 Inventory header button opens `InventoryPopup.tsx`. It calls the
  `schema_inventory` RPC (`POST /api/inventory`), which caps the popup at
  5,000 rows (`INVENTORY_ROW_LIMIT`) while `counts` stays complete, and the
  `export_schema_inventory` RPC (`POST /api/inventory/export`), which writes
  the full table and is never truncated. A size-guard refusal reaches the
  popup as an ordinary RPC error (HTTP 400).
- There is no MATLAB entry point, by decision.

## Observability

- INFO `schema_inventory: keys=… values={key: n} product=… present=…
  missing=… excluded=… in … ms`
- WARNING `schema_inventory: refused — …` comes before the size-guard raise.
- INFO `write_inventory_csv: N row(s), columns=[…] -> path`
- GUI: `[schema] schema_inventory … -> sent/total row(s) (truncated)`.

## Notes

- Excluded combinations are checked with a per-combination Python loop over
  the override rows (`exclusion_status`). That is O(combinations × override
  rows), which is fine at the 1M cap with a handful of exclusions. If it
  becomes slow, change the owner and leave the callers alone.
- A database that predates `__scidb_schema_overrides` (opened read-only) has
  no exclusions. `exclusion_status` checks whether the table exists, so that
  case no longer raises.
