"""Data inventory: which schema-key combinations have no ``_schema`` row?

The question is answered at exactly the keys the caller chooses (default:
every schema key). The universe is the Cartesian product of each chosen key's
values, and each combination in it gets one status:

* ``present``  — some ``_schema`` row projects onto it. A deeper row counts
  for its projection: ``S01/T2/trial3/cycle5`` makes ``S01/T2/trial3`` present
  when ``cycle`` is not chosen. A row that is NULL at any chosen key can't fill
  a combination and is ignored.
* ``excluded`` — an active ``exclude_schema`` matches it (rules owned by
  :func:`scidb.exclusions.exclusion_status`). Excluded wins over present.
* ``missing``  — neither.

Only the database is consulted, never a Variable. Each key's values are every
value it has anywhere in ``_schema`` (``DatabaseManager.distinct_schema_values``,
which also applies the declared ``[schema_keys]`` level order), so a subject
that exists only at subject level shows every deeper combination as missing.

Owners (CLAUDE.md NOTE 4): :func:`schema_inventory` computes statuses,
:func:`inventory_table` decides which columns and rows a "missing only" / "all"
view shows, and :func:`write_inventory_csv` writes that table. The CLI, the
Inspector and the GUI only consume these. Design notes:
docs/claude/data-inventory.md.
"""

from __future__ import annotations

import csv
import itertools
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .log import Log

if TYPE_CHECKING:
    from .database import DatabaseManager

PRESENT = "present"
MISSING = "missing"
EXCLUDED = "excluded"
STATUSES = (PRESENT, MISSING, EXCLUDED)

#: Above this many combinations the product is refused before it is built.
MAX_COMBINATIONS = 1_000_000


@dataclass
class SchemaInventory:
    """Every combination of the chosen keys, each with its status.

    ``rows`` holds one tuple per combination: the chosen keys' values (in
    ``keys`` order, as stored — always strings) followed by the status. Rows
    are in product order of ``levels``, i.e. the declared level order.
    """

    keys: list[str]
    levels: dict[str, list[str]]
    rows: list[tuple] = field(default_factory=list)
    counts: dict[str, int] = field(
        default_factory=lambda: {s: 0 for s in STATUSES}
    )
    product_size: int = 0

    def to_dict(self, missing_only: bool = True, limit: int | None = None) -> dict:
        """JSON shape shared by ``scidb inventory --json`` and the GUI."""
        columns, rows = inventory_table(self, missing_only)
        total = len(rows)
        if limit is not None and total > limit:
            rows = rows[:limit]
        return {
            "keys": list(self.keys),
            "levels": {k: list(v) for k, v in self.levels.items()},
            "columns": columns,
            "rows": [list(r) for r in rows],
            "total_rows": total,
            "truncated": len(rows) < total,
            "counts": dict(self.counts),
            "product_size": self.product_size,
            "missing_only": missing_only,
        }


def schema_inventory(
    db: DatabaseManager,
    keys: list[str] | None = None,
    *,
    max_combinations: int = MAX_COMBINATIONS,
) -> SchemaInventory:
    """Cross the chosen schema keys and give every combination a status.

    Args:
        db: DatabaseManager.
        keys: Schema keys to cross. ``None`` = every schema key. Returned in
            schema order whatever order they are given in.
        max_combinations: Refuse (ValueError) when the product would be larger.

    Raises:
        ValueError: ``keys`` is empty, names an unknown key, or the product
            exceeds ``max_combinations``.
    """
    from .exclusions import exclusion_status

    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    chosen = _resolve_keys(schema_keys, keys)

    levels = {k: [str(v) for v in db.distinct_schema_values(k)] for k in chosen}
    sizes = {k: len(v) for k, v in levels.items()}
    product_size = math.prod(sizes.values())
    if product_size > max_combinations:
        msg = (
            f"Inventory of {chosen} would have {product_size:,} combinations "
            f"(values per key: {sizes}), above the limit of "
            f"{max_combinations:,}. Uncheck a key to cross fewer."
        )
        Log.warn(f"schema_inventory: refused — {msg}")
        raise ValueError(msg)

    inv = SchemaInventory(keys=chosen, levels=levels, product_size=product_size)
    if product_size == 0:
        Log.info(
            f"schema_inventory: keys={chosen} values={sizes} product=0 "
            f"(no _schema values) in {(time.perf_counter() - t0) * 1000:.1f} ms"
        )
        return inv

    present = {
        tuple(str(v) for v in combo)
        for combo in db.distinct_schema_combinations(chosen)
    }
    combos = list(itertools.product(*(levels[k] for k in chosen)))
    excluded = exclusion_status(
        [dict(zip(chosen, combo)) for combo in combos], schema_keys, db
    )

    rows = []
    for combo, is_excluded in zip(combos, excluded):
        if is_excluded:
            status = EXCLUDED
        elif combo in present:
            status = PRESENT
        else:
            status = MISSING
        inv.counts[status] += 1
        rows.append((*combo, status))
    inv.rows = rows

    Log.info(
        f"schema_inventory: keys={chosen} values={sizes} product={product_size} "
        f"present={inv.counts[PRESENT]} missing={inv.counts[MISSING]} "
        f"excluded={inv.counts[EXCLUDED]} "
        f"in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return inv


def inventory_table(
    inv: SchemaInventory, missing_only: bool
) -> tuple[list[str], list[tuple]]:
    """The columns and rows one view shows: the one owner of both modes.

    ``missing_only``: only ``missing`` rows, key columns only (no status —
    every row would say the same thing). Otherwise every row plus a
    ``status`` column.
    """
    n = len(inv.keys)
    if missing_only:
        return list(inv.keys), [r[:n] for r in inv.rows if r[n] == MISSING]
    return [*inv.keys, "status"], list(inv.rows)


def write_inventory_csv(
    inv: SchemaInventory, path: str | Path, missing_only: bool
) -> int:
    """Write the inventory as long-format CSV; return the number of data rows.

    One column per chosen key (schema order), plus ``status`` unless
    ``missing_only`` — exactly :func:`inventory_table`.
    """
    columns, rows = inventory_table(inv, missing_only)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)
    Log.info(
        f"write_inventory_csv: {len(rows)} row(s), columns={columns} -> {path}"
    )
    return len(rows)


def _resolve_keys(schema_keys: list[str], keys: list[str] | None) -> list[str]:
    if keys is None:
        return list(schema_keys)
    keys = list(keys)
    if not keys:
        raise ValueError(
            f"Choose at least one schema key to cross. Schema keys: {schema_keys}"
        )
    unknown = [k for k in keys if k not in schema_keys]
    if unknown:
        raise ValueError(
            f"Unknown schema key(s): {unknown}. Schema keys: {schema_keys}"
        )
    return [k for k in schema_keys if k in keys]
