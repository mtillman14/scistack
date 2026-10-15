# Plan: collapse fully-missing blocks in the data inventory

Date: 2026-10-15. Extends docs/claude/data-inventory.md.

## Request

When every level of the deepest Cross key(s) is missing for one combination of
the shallower keys, show ONE row with `(all)` in those columns instead of one row
per level. Recursive: if every session × minute is missing for a subject, that
subject is one row with `session=All, minute=All`. Generic over any keys.

## Semantics

- Collapse runs over the **Cross keys in schema order, deepest first**. A block
  is "every combination sharing a prefix of the Cross keys". The shallowest
  (shortest-prefix) block that is entirely one status becomes one row.
- Within keys never collapse. They identify the stratum, and each stratum
  collapses on its own. `All` means every level **of that stratum**
  (`StratumSummary.levels`), e.g. CBrace session=All = BL, FIT and POST, while
  Traditional session=All = BL and POST.
- Missing-only view: a block collapses only if every row in it is `missing`. If
  any row in it is `excluded` or `present`, it is not uniform, so we recurse
  into it.
- All view: a block collapses when all its rows share ONE status (present,
  missing or excluded), and the collapsed row keeps that status. (Decision Q3.)
- If a whole stratum is uniform, every Cross column says `All`. With no strata
  and everything missing, the result is one row with `All` everywhere.
- `counts`, `product_size` and the per-stratum counts stay per combination and
  do not change. Only the displayed rows change. `total_rows` counts collapsed
  rows.

## Owner (NOTE 4)

`scidb.inventory.inventory_table(inv, missing_only, collapse=...)` already owns
"which rows a view shows", so it gains the collapse. It is a pure function over
`inv.rows` plus each stratum's levels, and it has no DB access. The CLI, CSV,
`to_dict` (JSON + GUI) and `render_inventory` all already consume it, so none
of them gets its own copy. The marker string is a module constant
`ALL_LEVELS`, the one owner, which the GUI receives in the JSON (`all_marker`).

Algorithm: per stratum, count rows per (prefix, status) for every prefix of
the Cross-key tuple. Then walk depth-first in level order. At each prefix, if
one status count equals the product of the remaining stratum level sizes, emit
one row. Otherwise recurse (in the missing view, skip prefixes with 0 missing).
O(rows × n_cross).

To make this possible, SchemaInventory has to say which stratum each row
belongs to and expose the Cross levels when there are no strata. It always
keeps the internal stratum list (today it is dropped when `within` is empty)
in a private field, and the public `strata` stays the same.

## Surfaces

- CLI: collapsed by default; `--expand` shows one row per combination.
  `--csv` and `--json` follow the same flag.
- `to_dict(missing_only, limit, collapse=True)`, `write_inventory_csv(..., collapse=True)`.
- GUI: a "Collapse fully-missing blocks" checkbox (default on) sent as
  `collapse` in both RPCs. Both vite bundles rebuilt; add a
  docs/gui-manual-testing-todo.md section.

## Logging

DEBUG `inventory_table: collapse rows N -> M (blocks collapsed by depth:
{session: a, minute: b})`. This line shows which depth each collapse happened at.

## Tests (scidb/tests/test_inventory.py)

1. A missing session collapses its minutes into one row with `minute=All`.
2. Recursive: a subject with no sessions gives one row, `session=All, minute=All`.
3. A partial block (one minute present) does not collapse; the missing minutes are listed one per row.
4. A block of missing rows plus an excluded row does not collapse in the missing view.
5. All view: a uniform present block collapses with status `present`.
6. Within: `All` is per stratum, and Within columns keep their values.
7. Interleaved schema `[subject, group, ...]`: the `All` values land in the Cross columns only.
8. `collapse=False` matches today's output exactly.
9. CSV and to_dict follow the flag, and counts are unchanged.
10. GUI API test: `collapse` is passed through.

## Decisions (user, 2026-10-15)

- Q1 marker: `(all)` (constant ALL_LEVELS; WARN if a real level equals it).
- Q2: collapsed by default; `--expand` / GUI checkbox to undo.
- Q3: All view collapses any uniform status.
