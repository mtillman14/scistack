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

**Within keys.** Each chosen key is either *crossed* (``keys``) or *within*
(``within``). The inventory runs separately for every combination of the
Within keys' values that exists in ``_schema`` (a *stratum*); inside a
stratum the universe is the product of each Cross key's values **found in
that stratum's rows**. So in a two-arm study, ``within=["group"]`` stops a
CBrace subject from being reported missing at Traditional-only sessions. A
row NULL at a Within key belongs to no stratum. Limitation: a value only
counts as expected in a stratum if something in that stratum has it.

**Collapsed blocks.** A view collapses by default: when every combination
sharing a prefix of the Cross keys (schema order) has one status, the block
is shown as ONE row with :data:`ALL_LEVELS` in the deeper Cross columns. The
shallowest uniform block wins, so a subject with no data at all is one row
``(all), (all)``. ``(all)`` means every level of that key in the stratum.
Statuses and counts are per combination and do not change.

Owners (CLAUDE.md NOTE 4): :func:`schema_inventory` computes statuses,
:func:`_strata` the strata and their per-key values, :func:`inventory_table`
decides which columns and rows a "missing only" / "all" view shows (and
collapses them, via :func:`_collapse_rows`), and
:func:`write_inventory_csv` writes that table. The CLI, the Inspector and the
GUI only consume these. Design notes: docs/claude/data-inventory.md.
"""

from __future__ import annotations

import csv
import itertools
import math
import time
from collections import Counter
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

#: Value shown in a Cross column a collapsed row stands for every level of.
ALL_LEVELS = "(all)"

#: Above this many combinations the product is refused before it is built.
MAX_COMBINATIONS = 1_000_000


def _zero_counts() -> dict[str, int]:
    return {s: 0 for s in STATUSES}


@dataclass
class StratumSummary:
    """One combination of the Within keys' values and its own inventory.

    ``levels`` holds each Cross key's values seen in this stratum, in the
    global (declared) order.
    """

    values: dict[str, str]
    levels: dict[str, list[str]]
    product_size: int = 0
    counts: dict[str, int] = field(default_factory=_zero_counts)

    def label(self) -> str:
        return ", ".join(f"{k}={v}" for k, v in self.values.items())

    def to_dict(self) -> dict:
        return {
            "values": dict(self.values),
            "product_size": self.product_size,
            "counts": dict(self.counts),
        }


@dataclass
class SchemaInventory:
    """Every combination of the chosen keys, each with its status.

    ``keys`` are the Cross keys and ``within`` the Within keys (both in schema
    order). ``columns`` — every Cross and Within key, in schema order — is the
    one owner of column order. ``rows`` holds one tuple per combination: the
    values in ``columns`` order (as stored — always strings) followed by the
    status. Rows are grouped by stratum, then in product order of the
    declared level order.

    ``levels`` is each column's global value list. ``strata`` is empty when
    there are no Within keys; ``product_size`` and ``counts`` are totals over
    all strata.

    ``row_blocks`` is how ``rows`` is laid out: consecutive runs of
    ``product_size`` rows, one per block, in order. It equals ``strata`` with
    Within keys and is one stratum (no values, global levels) without them.
    :func:`_collapse_rows` reads it.
    """

    keys: list[str]
    levels: dict[str, list[str]]
    within: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    strata: list[StratumSummary] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=_zero_counts)
    product_size: int = 0
    row_blocks: list[StratumSummary] = field(default_factory=list, repr=False)

    def __post_init__(self):
        if not self.columns:
            self.columns = list(self.keys)

    def to_dict(
        self,
        missing_only: bool = True,
        limit: int | None = None,
        collapse: bool = True,
    ) -> dict:
        """JSON shape shared by ``scidb inventory --json`` and the GUI."""
        columns, rows = inventory_table(self, missing_only, collapse=collapse)
        total = len(rows)
        if limit is not None and total > limit:
            rows = rows[:limit]
        return {
            "keys": list(self.keys),
            "within": list(self.within),
            "levels": {k: list(v) for k, v in self.levels.items()},
            "columns": columns,
            "rows": [list(r) for r in rows],
            "total_rows": total,
            "truncated": len(rows) < total,
            "counts": dict(self.counts),
            "product_size": self.product_size,
            "strata": [s.to_dict() for s in self.strata],
            "missing_only": missing_only,
            "collapse": collapse,
            "all_marker": ALL_LEVELS,
        }


def schema_inventory(
    db: DatabaseManager,
    keys: list[str] | None = None,
    within: list[str] | None = None,
    *,
    max_combinations: int = MAX_COMBINATIONS,
) -> SchemaInventory:
    """Cross the chosen schema keys and give every combination a status.

    Args:
        db: DatabaseManager.
        keys: Schema keys to cross. ``None`` = every schema key not in
            ``within``. Returned in schema order whatever order they are
            given in.
        within: Schema keys to run the inventory within (one stratum per
            existing combination of their values). ``None``/empty = no strata.
        max_combinations: Refuse (ValueError) when the product (summed over
            strata) would be larger.

    Raises:
        ValueError: ``keys`` is empty, names an unknown key, a key is both
            Cross and Within, no Cross key is left, or the product exceeds
            ``max_combinations``.
    """
    from .exclusions import exclusion_status

    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    within_keys = _resolve_within(schema_keys, within)
    chosen = _resolve_cross(schema_keys, keys, within_keys)
    columns = [k for k in schema_keys if k in chosen or k in within_keys]

    levels = {k: [str(v) for v in db.distinct_schema_values(k)] for k in columns}
    sizes = {k: len(levels[k]) for k in chosen}

    if within_keys:
        strata = _strata(db, within_keys, chosen, levels)
    else:
        strata = [StratumSummary(values={}, levels={k: levels[k] for k in chosen})]
    for s in strata:
        s.product_size = math.prod(len(s.levels[k]) for k in chosen)
    product_size = sum(s.product_size for s in strata)

    if product_size > max_combinations:
        if within_keys:
            largest = sorted(strata, key=lambda s: -s.product_size)[:3]
            detail = (
                f"{len(strata)} strata of {within_keys}; largest: "
                + "; ".join(f"{s.label()}: {s.product_size:,}" for s in largest)
            )
        else:
            detail = f"values per key: {sizes}"
        msg = (
            f"Inventory of {chosen} would have {product_size:,} combinations "
            f"({detail}), above the limit of {max_combinations:,}. "
            f"Uncheck a key to cross fewer"
            + (", or make a key Within." if not within_keys else ".")
        )
        Log.warn(f"schema_inventory: refused — {msg}")
        raise ValueError(msg)

    inv = SchemaInventory(
        keys=chosen,
        levels=levels,
        within=within_keys,
        columns=columns,
        strata=strata if within_keys else [],
        product_size=product_size,
        row_blocks=strata,
    )
    if product_size == 0:
        Log.info(
            f"schema_inventory: keys={chosen} within={within_keys} "
            f"strata={len(inv.strata)} values={sizes} product=0 "
            f"(no _schema values) in {(time.perf_counter() - t0) * 1000:.1f} ms"
        )
        return inv

    present = {
        tuple(str(v) for v in combo)
        for combo in db.distinct_schema_combinations(columns)
    }

    # Every combination, in columns order, tagged with its stratum.
    combos: list[tuple] = []
    owners: list[StratumSummary] = []
    for s in strata:
        for cross_combo in itertools.product(*(s.levels[k] for k in chosen)):
            values = {**s.values, **dict(zip(chosen, cross_combo))}
            combos.append(tuple(values[k] for k in columns))
            owners.append(s)
    excluded = exclusion_status(
        [dict(zip(columns, combo)) for combo in combos], schema_keys, db
    )

    rows = []
    for combo, stratum, is_excluded in zip(combos, owners, excluded):
        if is_excluded:
            status = EXCLUDED
        elif combo in present:
            status = PRESENT
        else:
            status = MISSING
        inv.counts[status] += 1
        stratum.counts[status] += 1
        rows.append((*combo, status))
    inv.rows = rows

    for s in inv.strata:
        Log.debug(
            f"schema_inventory: stratum {s.label()} "
            f"values={ {k: len(v) for k, v in s.levels.items()} } "
            f"product={s.product_size} present={s.counts[PRESENT]} "
            f"missing={s.counts[MISSING]} excluded={s.counts[EXCLUDED]}"
        )
    Log.info(
        f"schema_inventory: keys={chosen} within={within_keys} "
        f"strata={len(inv.strata)} values={sizes} product={product_size} "
        f"present={inv.counts[PRESENT]} missing={inv.counts[MISSING]} "
        f"excluded={inv.counts[EXCLUDED]} "
        f"in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return inv


def _strata(
    db: DatabaseManager,
    within: list[str],
    cross: list[str],
    levels: dict[str, list[str]],
) -> list[StratumSummary]:
    """The strata (existing combinations of the Within keys' values) and each
    one's Cross-key values. The one owner of both.

    Strata are in product order of the Within keys' global values; a stratum's
    values for a key keep the global (declared) order. A row NULL at a Within
    key or at the Cross key belongs to no stratum (``distinct_schema_combinations``
    skips NULLs).
    """
    existing = {
        tuple(str(v) for v in combo)
        for combo in db.distinct_schema_combinations(within)
    }
    keys_in_stratum: dict[tuple, dict[str, set[str]]] = {
        combo: {k: set() for k in cross} for combo in existing
    }
    n = len(within)
    for key in cross:
        for combo in db.distinct_schema_combinations([*within, key]):
            combo = tuple(str(v) for v in combo)
            keys_in_stratum[combo[:n]][key].add(combo[n])

    strata = []
    for combo in itertools.product(*(levels[k] for k in within)):
        if combo not in existing:
            continue
        seen = keys_in_stratum[combo]
        strata.append(
            StratumSummary(
                values=dict(zip(within, combo)),
                levels={k: [v for v in levels[k] if v in seen[k]] for k in cross},
            )
        )
    return strata


def inventory_table(
    inv: SchemaInventory, missing_only: bool, collapse: bool = True
) -> tuple[list[str], list[tuple]]:
    """The columns and rows one view shows: the one owner of both modes.

    ``missing_only``: only ``missing`` rows, key columns only (no status —
    every row would say the same thing). Otherwise every row plus a
    ``status`` column. Key columns are ``inv.columns`` (Cross and Within keys
    in schema order).

    ``collapse``: each block of combinations that share a prefix of the Cross
    keys and all have one status is shown as one row, with
    :data:`ALL_LEVELS` in the deeper Cross columns (:func:`_collapse_rows`).
    ``False`` = one row per combination.
    """
    n = len(inv.columns)
    rows = _collapse_rows(inv) if collapse else inv.rows
    if missing_only:
        return list(inv.columns), [r[:n] for r in rows if r[n] == MISSING]
    return [*inv.columns, "status"], list(rows)


def _collapse_rows(inv: SchemaInventory) -> list[tuple]:
    """``inv.rows`` with every uniform block folded into one row.

    Per row block (stratum), a *block* is every combination sharing a prefix
    of the Cross-key values. Walking prefixes shallow-to-deep in level order,
    the first prefix whose rows all have one status becomes one row: its own
    values, :data:`ALL_LEVELS` in the deeper Cross columns (the value itself
    for a key with one level in the stratum), the Within values of the
    stratum, and that status. Otherwise it recurses one key deeper. A
    single combination is always uniform, so every row is accounted for, and
    rows keep product order. O(rows × Cross keys).
    """
    n = len(inv.columns)
    cross = list(inv.keys)
    m = len(cross)
    cross_pos = [inv.columns.index(k) for k in cross]
    blocks = inv.row_blocks or [
        StratumSummary(values={}, levels={k: inv.levels[k] for k in cross},
                       product_size=len(inv.rows))
    ]

    clash = [k for k in cross if ALL_LEVELS in inv.levels.get(k, ())]
    if clash:
        Log.warn(
            f"inventory_table: schema key(s) {clash} have a real level "
            f"{ALL_LEVELS!r}; collapsed rows use the same text"
        )

    out: list[tuple] = []
    collapsed_at: Counter = Counter()  # first collapsed key -> blocks
    start = 0
    for block in blocks:
        block_rows = inv.rows[start:start + block.product_size]
        start += block.product_size
        if not block_rows:
            continue
        levels = [block.levels[k] for k in cross]
        # Rows still to come at depth d, for one prefix: product of the
        # remaining level counts.
        remaining = [math.prod(len(lv) for lv in levels[d:]) for d in range(m + 1)]
        tally: Counter = Counter()
        for r in block_rows:
            values = tuple(r[p] for p in cross_pos)
            for d in range(m + 1):
                tally[(values[:d], r[n])] += 1

        def walk(prefix: tuple, depth: int) -> None:
            for status in STATUSES:
                if tally[(prefix, status)] == remaining[depth]:
                    row = [None] * n
                    for k, v in block.values.items():
                        row[inv.columns.index(k)] = v
                    for d, p in enumerate(cross_pos):
                        row[p] = prefix[d] if d < depth else _all_of(levels[d])
                    out.append((*row, status))
                    folded = [k for k, lv in zip(cross[depth:], levels[depth:]) if len(lv) > 1]
                    if folded:
                        collapsed_at[folded[0]] += 1
                    return
            for v in levels[depth]:
                walk((*prefix, v), depth + 1)

        walk((), 0)

    if collapsed_at:
        Log.debug(
            f"inventory_table: collapse rows {len(inv.rows)} -> {len(out)} "
            f"(blocks collapsed from key: {dict(collapsed_at)})"
        )
    return out


def _all_of(levels: list[str]) -> str:
    """A collapsed cell: :data:`ALL_LEVELS`, or the value itself when the key
    has only one level in the stratum (nothing was folded, so say which)."""
    return levels[0] if len(levels) == 1 else ALL_LEVELS


def write_inventory_csv(
    inv: SchemaInventory, path: str | Path, missing_only: bool, collapse: bool = True
) -> int:
    """Write the inventory as long-format CSV; return the number of data rows.

    One column per chosen key (schema order), plus ``status`` unless
    ``missing_only`` — exactly :func:`inventory_table`, collapsed the same way.
    """
    columns, rows = inventory_table(inv, missing_only, collapse=collapse)
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


def _resolve_within(schema_keys: list[str], within: list[str] | None) -> list[str]:
    if not within:
        return []
    unknown = [k for k in within if k not in schema_keys]
    if unknown:
        raise ValueError(
            f"Unknown Within schema key(s): {unknown}. Schema keys: {schema_keys}"
        )
    return [k for k in schema_keys if k in within]


def _resolve_cross(
    schema_keys: list[str], keys: list[str] | None, within: list[str]
) -> list[str]:
    if keys is None:
        chosen = [k for k in schema_keys if k not in within]
        if not chosen:
            raise ValueError(
                f"Every schema key is Within {within}; at least one must be "
                f"crossed. Schema keys: {schema_keys}"
            )
        return chosen
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
    both = [k for k in schema_keys if k in keys and k in within]
    if both:
        raise ValueError(
            f"Schema key(s) {both} are both Cross and Within; choose one role."
        )
    return [k for k in schema_keys if k in keys]
