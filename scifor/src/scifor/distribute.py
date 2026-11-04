"""Where ``distribute=True`` files a returned table's rows. The one owner.

The target is the first schema key below the iterated keys that the table does
not carry as a column. Rows are numbered 1, 2, ... in table order, restarting
within each group of carried key values. A table with no key columns is the
plain case: numbered from 1 per combination, at the key below the iterated ones.

The target never depends on whether duplicates exist. If it did, one new
duplicate row in a CSV would move every record down a level on the next run.
Both languages use these functions (``scifor.matlab_loop`` for MATLAB). The
shared cases are in ``docs/claude/distribute-cases.json``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from typing import Any


class DistributeError(ValueError):
    """``distribute=True`` cannot file this table. Deterministic across
    combinations, so it stops the run instead of skipping each one."""

    scifor_fatal = True


class MissingSchemaKeyError(ValueError):
    """Rows of a returned table have a blank value in a schema-key column, so
    they have no address to be saved at."""

    scifor_fatal = True


def resolve_target(
    schema_keys: Sequence[str],
    start_key: str,
    columns: Iterable[str],
    *,
    output_name: str = "output",
) -> "tuple[str, list[str]]":
    """``(target, carried)`` for one table.

    *start_key* is the key below the iterated ones (the run plan's
    ``distribute_key``). *carried* are the table's key columns from there down
    to the target; the rows' values in them are part of each row's address.
    """
    keys = list(schema_keys)
    present = {str(c) for c in columns}
    index = keys.index(start_key)
    carried: list[str] = []
    while index < len(keys) and keys[index] in present:
        carried.append(keys[index])
        index += 1
    if index >= len(keys):
        raise DistributeError(
            f"distribute=True but {output_name} carries {carried}, down to the "
            f"deepest schema key. There is no lower level to distribute to. "
            f"Schema order: {keys}. Turn distribute off: rows that carry their "
            f"schema keys are filed by those columns without it."
        )
    target = keys[index]
    below = [k for k in keys[index + 1 :] if k in present]
    if below:
        raise DistributeError(
            f"distribute=True but {output_name} carries {below} and not {target!r}, "
            f"which is above them. Schema order: {keys}. Add a {target!r} column, or "
            f"remove {below}, or turn distribute off."
        )
    return target, carried


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def group_ordinals(
    carried: Sequence[str],
    columns: Sequence[Sequence[Any]],
    *,
    output_name: str = "output",
    where: str = "",
) -> "list[int]":
    """1-based position of each row within its group of carried key values, in
    table order. *columns* holds one sequence per key in *carried*. A blank
    value raises :class:`MissingSchemaKeyError` naming the rows."""
    details = []
    for key, values in zip(carried, columns, strict=True):
        rows = [i for i, value in enumerate(values) if _is_blank(value)]
        if rows:
            details.append(f"{key}: row(s) {rows[:20]}")
    if details:
        raise MissingSchemaKeyError(
            f"{output_name}{f' at {where}' if where else ''}: blank schema key "
            f"value(s), so these rows cannot be distributed ({'; '.join(details)}; "
            f"0-based rows of the returned table). Nothing from this call was "
            f"saved. Give every row a value, or leave these rows out."
        )
    seen: Counter = Counter()
    ordinals: list[int] = []
    for group in zip(*columns, strict=True):
        seen[group] += 1
        ordinals.append(seen[group])
    return ordinals


def multi_row_groups(
    carried: Sequence[str],
    columns: Sequence[Sequence[Any]],
) -> "list[tuple[dict, int]]":
    """``[({key: value}, row count)]`` of the groups with more than one row, in
    table order, for the log: these are the rows the numbering tells apart."""
    counts = Counter(zip(*columns, strict=True))
    return [
        (dict(zip(carried, group, strict=True)), count)
        for group, count in counts.items()
        if count > 1
    ]
