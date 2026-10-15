# Plan: "Within" keys for the data inventory

Drafted 2026-10-15. Status: all 6 stages implemented 2026-10-15 (uncommitted; pytest unrun).
Builds on `.claude/plan-data-inventory.md` (committed 54283da6, v0.1.35).

## Problem

In a two-arm study (`group` = Traditional / CBrace), the arms have different
sessions, and each subject belongs to one arm. Crossing every key against
every other reports false gaps, because a CBrace subject is "missing" from
Traditional-only sessions. Unchecking `group` doesn't fix this: the sessions
still cross across arms.

## Concept

Each schema key gets one of three roles:

| Role | Meaning |
|---|---|
| **Cross** | Every value is expected at every combination (this is what "checked" means today). |
| **Within** | The inventory runs separately for each combination of these keys' values (each one is called a *stratum*). The key appears as a column but is never crossed. |
| **Off** | Projected out (this is what "unchecked" means today). |

The rule: the strata are the distinct combinations of the Within keys that
exist in `_schema`. Within each stratum, the expected set is the product of
each Cross key's values **found in rows of that stratum**.

## Decisions (from the user, 2026-10-15)

1. Use the case above: `group` = Within; subject, session and minute = Cross.
2. Exclusions are only made by subject, but group-level exclusions should
   still be handled correctly (they will be, because the Within keys are part
   of each combination).
3. Minute levels are the same across sessions, so there is no second nesting
   level ("minute nested in session"). A per-key `nested_in` is out of scope.

## Decisions I made (flag if wrong)

- **Values per stratum keep the declared order.** A stratum's values for a
  key are the global `distinct_schema_values(key)` list filtered to the values
  seen in that stratum. Declared order still has one owner.
- **Strata are ordered** by product order over the Within keys' global
  values, keeping only the combinations that exist.
- **A row that is NULL at any Within key** belongs to no stratum and adds
  nothing to the expected values or to present. This is the same rule as a
  NULL at a Cross key today.
- **Defaults**: `within=None` means no strata, which is exactly today's
  behaviour. When `keys=None` and `within` is given, Cross = every schema key
  not in `within`.
- **Validation** (ValueError): unknown Within key; a key that is both Cross
  and Within; Within given with no Cross key left.
- **Columns**: every Cross and Within key, in schema order, then `status`
  (All view only). **Rows**: grouped by stratum, then in product order.
  With the schema `[subject, group, ...]` the `group` column therefore comes
  second, even though rows are grouped by it.
- **Size guard**: the sum of the per-stratum products, computed before any
  combination is built. The refusal message names the largest strata.
- **Limitation (documented, not solved)**: a value counts as expected only if
  something in its stratum has it. If no Traditional subject ever recorded
  session 3, the inventory can't know session 3 was expected there. That is
  why only design keys (arm, site, protocol) should be Within.

## Owners (NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| Strata and per-stratum values | `scidb.inventory._strata(db, within, cross, levels)` (private, called only by `schema_inventory`) | `schema_inventory` |
| Statuses | `scidb.inventory.schema_inventory(db, keys=None, within=None)` | Inspector, CLI, GUI |
| View columns/rows | `inventory_table` (unchanged signature; it now uses `inv.columns`) | `to_dict`, CSV, CLI render |
| Is a combination excluded? | `exclusion_status` (unchanged; combinations now include the Within keys) | inventory, `for_each` |

Values per stratum come from one existing DB call per Cross key:
`distinct_schema_combinations(within + [key])`, grouped in Python. No new
SQL or tables are needed.

## Stages

### Stage 1: scidb owner (`scidb/inventory.py`)
- `schema_inventory(db, keys=None, within=None, *, max_combinations=...)`.
- `SchemaInventory` gains:
  - `within: list[str]`
  - `columns: list[str]` (the chosen keys in schema order; one owner of
    column order)
  - `strata: list[StratumSummary]`, where each `StratumSummary` holds
    `values: dict[key, str]`, `levels: dict[key, list[str]]`,
    `product_size` and `counts`
- `levels` stays the global per-key list. `product_size` is the sum over
  strata, and `counts` is the total.
- `rows` become tuples in `columns` order followed by status.
  `inventory_table` slices on `len(inv.columns)` instead of `len(inv.keys)`.
- `to_dict` adds `within` and `strata` (values, product_size, counts).
- Logging: the INFO line adds `within=` and the number of strata. A DEBUG
  line per stratum gives its values, per-key value counts, product size and
  counts. Size-guard refusals are logged as a WARNING, as today.
- `Inspector.inventory(keys=None, within=None)`.

### Stage 2: CLI
- `scidb inventory --within group[,site]`. `--keys` still names the Cross
  keys; without it, every non-Within key is crossed.
- The human render prints the counts line, then one sub-line per stratum
  (`group=CBrace: 40 combination(s): 38 present, 2 missing, 0 excluded`),
  then the table.

### Stage 3: GUI backend
- `schema_inventory` and `export_schema_inventory` accept `within:
  list[str] | None`. Truncation is unchanged (it applies across all strata).

### Stage 4: GUI frontend
- Each key's checkbox becomes a 3-way segmented control: **Cross / Within /
  Off**, all Cross by default. The choices are sent as `keys` (Cross) and
  `within`.
- The counts line gets one sub-line per stratum.
- Run and Export are disabled when no key is Cross.
- Rebuild BOTH vite targets.

### Stage 5: Tests
`scidb/tests/test_inventory.py`, new class `TestWithin`, on a two-arm fixture
with schema `[group, subject, session, minute]`. Traditional has sessions
{BL, POST}; CBrace has {BL, FIT, POST}; subjects are suffixed per arm.
- Without `within`, false gaps appear: CB subjects are missing in Traditional
  combinations and T subjects at FIT. This documents why Within is needed.
- With `within=["group"]`: no cross-arm gaps; a real missing minute in one
  arm is reported; per-stratum counts are correct.
- The non-adjacent schema `[subject, group, session, minute]` gives the same
  statuses, and its columns come out in schema order.
- A row that is NULL at the Within key adds nothing.
- `exclude_schema(group="CBrace")` marks that whole stratum excluded; a
  subject exclusion marks only that subject.
- Validation: a key that is both Cross and Within, an unknown Within key,
  and no Cross key left.
- The size guard sums the strata (the limit falls between one stratum's size
  and the total).
- `keys=None` with `within` crosses every other key.
- CSV and `to_dict` include the Within column and the `strata` summaries.
- CLI: `--within group --json`, and the per-stratum lines in the human render.
- `scistack-gui/tests/test_schema_inventory_api.py`: `within` passes through
  both handlers.

### Stage 6: Docs
- `docs/claude/data-inventory.md`: a Within section covering roles, the rule,
  the limitation, and the two-arm worked example.
- The `scidb/README.md` inventory section: one example with `--within`.
- `docs/gui-manual-testing-todo.md`: a new § for the 3-way control.
