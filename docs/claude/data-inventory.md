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

## Within keys

Plan: `.claude/plan-inventory-within.md`. Each schema key has one role:

| Role | Meaning | Argument |
|---|---|---|
| **Cross** | Every value is expected at every combination. | `keys` |
| **Within** | The inventory runs separately for each existing combination of these keys' values (a *stratum*). The key is a column but is never crossed. | `within` |
| **Off** | Projected out. | (neither) |

**The rule.** The strata are the distinct combinations of the Within keys
that exist in `_schema`. Inside a stratum, the expected set is the product of
each Cross key's values **found in rows of that stratum**.

**Worked example (two-arm study).** Schema `[group, subject, session,
minute]`. Traditional subjects T01/T02 have sessions BL and POST; CBrace
subjects C01/C02 have BL, FIT and POST; minutes are 1 and 2. Crossing every
key gives 2 × 4 × 3 × 2 = 48 combinations and 29 false gaps (C01 "missing" in
Traditional, T01 "missing" at FIT). Unchecking `group` doesn't help: T01 is
still crossed with FIT. With `within=["group"]`, CBrace crosses
2 × 3 × 2 = 12 and Traditional 2 × 2 × 2 = 8, so only real gaps show.

Details:
- `within=None` (or empty) means no strata: exactly the behaviour without it,
  and `inv.strata == []`. With `keys=None` and `within` given, every schema
  key not in `within` is crossed.
- `ValueError` for an unknown Within key, a key that is both Cross and
  Within, or no Cross key left.
- Strata are ordered by product order over the Within keys' global values,
  keeping only combinations that exist. A stratum's values for a key are the
  global `distinct_schema_values(key)` list filtered to the values it has, so
  declared order still has one owner.
- A row NULL at any Within key belongs to no stratum and adds nothing (the
  same rule as a NULL at a Cross key).
- Exclusions need nothing new: each combination includes the Within keys, so
  `exclude_schema(group="CBrace")` excludes that whole stratum and
  `exclude_schema(subject="C02")` only that subject.
- Columns (`SchemaInventory.columns`, the one owner of column order): every
  Cross and Within key in schema order, then `status` in the All view. Rows
  are grouped by stratum, then in product order. With schema
  `[subject, group, ...]` the `group` column is second although rows are
  grouped by it.
- `levels` stays the global per-column list; `product_size` and `counts` are
  totals; each `StratumSummary` has its own `values`, `levels`,
  `product_size` and `counts`. `to_dict` adds `within` and `strata`
  (values, product_size, counts per stratum).
- Owner of the strata: `scidb.inventory._strata` (private, called only by
  `schema_inventory`). It uses one `distinct_schema_combinations(within)` call
  plus one `distinct_schema_combinations(within + [key])` call per Cross key,
  grouped in Python. No new SQL or tables.

**Limitation (documented, not solved).** A value counts as expected in a
stratum only if something in that stratum has it. If no Traditional subject
ever recorded session 3, the inventory can't know session 3 was expected
there. So only design keys (arm, site, protocol) should be Within; a key whose
gaps you want to find must be Cross. A second nesting level ("minute nested in
session") is out of scope.

## Size guard

The product size is computed from the per-key value counts **before**
anything is built. Above `MAX_COMBINATIONS` (1,000,000) the call logs a
WARNING and raises `ValueError`. The message names the size and the per-key
counts and suggests unchecking a key. With Within keys the size is the sum of
the per-stratum products, and the message names the largest strata. Tests pass
`max_combinations=` to exercise this.

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
`total_rows`, `truncated`, `counts`, `product_size`, `levels`, `keys`,
`within` and `strata`.

## Entry points

- Python: `schema_inventory(db, keys, within)` and
  `db.inspect.inventory(keys, within)`.
- CLI: `scidb inventory [--keys a,b] [--within g] [--all] [--csv PATH] [--json]`.
  With `--within`, the counts line is followed by one line per stratum
  (`group=CBrace: 12 combination(s): 11 present, 1 missing, 0 excluded`). Also
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

- INFO `schema_inventory: keys=… within=[…] strata=N values={key: n}
  product=… present=… missing=… excluded=… in … ms`
- DEBUG, one per stratum: `schema_inventory: stratum group=CBrace
  values={key: n} product=… present=… missing=… excluded=…`
- WARNING `schema_inventory: refused — …` comes before the size-guard raise.
- INFO `write_inventory_csv: N row(s), columns=[…] -> path`
- GUI: `[schema] schema_inventory keys=… within=… strata=N … ->
  sent/total row(s) (truncated)`.
- GUI popup: each key has a Cross / Within / Off segmented control (all Cross
  by default), sent as `keys` and `within`; one sub-line per stratum under the
  counts line.

## Notes

- Excluded combinations are checked with a per-combination Python loop over
  the override rows (`exclusion_status`). That is O(combinations × override
  rows), which is fine at the 1M cap with a handful of exclusions. If it
  becomes slow, change the owner and leave the callers alone.
- A database that predates `__scidb_schema_overrides` (opened read-only) has
  no exclusions. `exclusion_status` checks whether the table exists, so that
  case no longer raises.
