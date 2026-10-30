"""Combining table rows that share key columns — the one owner of the rule.

A table loaded once and saved one record per row (a CSV with a ``subject``
column, schema ``[subject, date]`` and no ``date`` column) can hold two rows
for one location: one subject, two hospital stays. Saved as they are, they
are two different records at one location, which scidb refuses
(``DuplicateLocationError``). With ``on_duplicate="combine"`` scidb asks this
module to make the table one row per location first:

- rows are grouped by ``key_columns`` (a NULL key groups with NULL);
- per value column of a group: all values equal (NaN equals NaN) -> that one
  value; otherwise the list of EVERY row's value, in row order, so position i
  is "row i" in every list column of the row;
- only scalar cells (number, bool, string, datetime, missing) are combined;
  a group holding an array / list / dict / DataFrame cell is left as it was
  and reported (scidb's duplicate check then refuses it);
- a list column must hold one kind of value (numbers, bools or strings), and
  datetimes cannot go into a list yet -> CombineMixedTypesError.

Pure pandas, no database: which columns are keys is the caller's decision
(scidb's ``combine_key_columns`` adds the variant columns, so two variants of
one subject are never merged). Plan: ``.claude/plan-combine-duplicate-locations.md``.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from .failures import UserValueError

#: Groups shown in the WARN line; every group is in the report.
_LOG_GROUPS_MAX = 10


@dataclass
class CombineReport:
    """What :func:`combine_duplicate_rows` did to one table."""

    n_rows_in: int = 0
    n_rows_out: int = 0
    #: One entry per combined group: ``{"key": {col: value}, "rows": n,
    #: "list_columns": [...], "collapsed_columns": [...]}``.
    combined: list = field(default_factory=list)
    #: Columns where any group needed a list of several values.
    list_columns: list = field(default_factory=list)
    #: Columns where a group's rows agreed and collapsed to one value (and
    #: no group needed a list).
    collapsed_columns: list = field(default_factory=list)
    #: Groups left as they were: ``{"key": {...}, "rows": n, "column": c,
    #: "kind": type name}`` for the first non-scalar cell found.
    uncombinable: list = field(default_factory=list)
    #: For each input row (by position), the position of the output row it
    #: went into: a caller that returns one answer per input row (the MATLAB
    #: bridge's record ids) maps them back with it.
    output_row: list = field(default_factory=list)


class CombineMixedTypesError(UserValueError):
    """A column that needs a list of combined values holds more than one kind
    of value (numbers and text, say), or a kind a list cannot hold yet
    (datetimes). The list would have no one column type, so nothing is saved.

    ``problems``: ``[{"column", "kinds", "key"}]``, ``key`` naming one group
    where the column needed a list. Owns its message and its ``remedy``."""

    def __init__(self, problems: "list[dict]", *, variable: str | None = None) -> None:
        self.problems = list(problems)
        self.variable = variable
        lines = [
            f"  {p['column']}: {', '.join(p['kinds'])}"
            + (f" (combined at {_key_text(p['key'])})" if p.get("key") else "")
            for p in self.problems
        ]
        target = f"{variable}: " if variable else ""
        super().__init__(
            f"{target}combining rows with the same location needs a list in "
            f"{len(self.problems)} column(s) that hold more than one kind of "
            f"value, so nothing was saved:\n" + "\n".join(lines) + "\n"
            "A combined column must hold one kind of value (numbers, true/false "
            "or text), and dates/times cannot be combined into a list yet. "
            "Fix the values in the source file, or remove the extra rows.",
            output=variable,
            stage="combine",
        )

    @property
    def remedy(self) -> dict:  # type: ignore[override]
        return {
            "kind": "combine_mixed_types",
            "title": "Rows could not be combined",
            "explanation": (
                "Combining rows that share a location puts each column's "
                "different values into one list. These columns hold more than "
                "one kind of value (or dates/times, which cannot be combined "
                "yet), so the list would have no single type. Nothing was saved."
            ),
            "items": [
                {
                    "location": _key_text(p.get("key") or {}),
                    "count": len(p["kinds"]),
                    "differing_columns": [p["column"]],
                }
                for p in self.problems
            ],
            "n_more": 0,
            "steps": [
                {
                    "title": "Change the data",
                    "text": (
                        "Make each listed column hold one kind of value (for "
                        "example, only numbers), or remove the extra row."
                    ),
                    "action": "reveal_function",
                },
            ],
        }


def _key_text(key: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in key.items()) or "(no key columns)"


_MISSING = ("__scifor_missing__",)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return True
    except ImportError:  # pragma: no cover - pandas is required by callers
        pass
    if isinstance(value, float):
        return value != value
    try:
        import numpy as np

        if isinstance(value, np.floating):
            return bool(np.isnan(value))
        if isinstance(value, np.datetime64):
            return bool(np.isnat(value))
    except ImportError:  # pragma: no cover
        pass
    return False


def _key_token(value: Any) -> Any:
    """A hashable stand-in for one key cell: NULLs are one token, an
    unhashable value (a dict of bookkeeping, say) its repr."""
    if _is_missing(value):
        return _MISSING
    try:
        hash(value)
    except TypeError:
        return ("__scifor_unhashable__", repr(value))
    return value


def _kind(value: Any) -> str:
    """The kind of a scalar cell: bool / number / text / datetime / other."""
    import numpy as np

    if isinstance(value, (bool, np.bool_)):
        return "bool"
    if isinstance(value, (int, float, np.integer, np.floating)):
        return "number"
    if isinstance(value, str):
        return "text"
    if isinstance(value, (_dt.date, _dt.datetime, _dt.time, np.datetime64)):
        return "datetime"
    try:
        import pandas as pd

        if isinstance(value, pd.Timestamp):
            return "datetime"
    except ImportError:  # pragma: no cover
        pass
    return "other"


def _is_scalar_cell(value: Any) -> bool:
    return _is_missing(value) or _kind(value) != "other"


def _same(a: Any, b: Any) -> bool:
    """Equal for collapsing: NaN equals NaN, and values of different kinds
    (``1`` and ``True``, ``5`` and ``"5"``) are never equal."""
    ma, mb = _is_missing(a), _is_missing(b)
    if ma or mb:
        return ma and mb
    if _kind(a) != _kind(b):
        return False
    try:
        return bool(a == b)
    except (TypeError, ValueError):
        return False


def _object_series(values: list, index, name):
    import numpy as np
    import pandas as pd

    out = np.empty(len(values), dtype=object)
    for i, v in enumerate(values):
        out[i] = v
    return pd.Series(out, index=index, name=name, dtype=object)


def combine_duplicate_rows(
    df,
    key_columns: "list[str]",
    value_columns: "list[str] | None" = None,
):
    """One row per distinct ``key_columns`` combination of ``df``.

    ``value_columns`` defaults to every column that is not a key. A column
    that is neither (``value_columns`` given) also keeps rows apart, as a
    key does: grouping on fewer columns than the table has would drop the
    others' values. Returns
    ``(combined_df, CombineReport)``; a table without duplicate keys comes
    back unchanged (the same object). The combined row takes the place of the
    group's first row; the index is reset when anything was combined.

    Raises :class:`CombineMixedTypesError` before returning anything when a
    column that needs a list holds more than one kind of value.
    """
    import pandas as pd

    if value_columns is None:
        keys = [c for c in key_columns if c in df.columns]
        values = [c for c in df.columns if c not in keys]
    else:
        values = [c for c in value_columns if c in df.columns]
        keys = [c for c in df.columns if c not in values]
    n = len(df)
    report = CombineReport(n_rows_in=n, n_rows_out=n, output_row=list(range(n)))
    if n < 2 or not values:
        return df, report

    key_cells = [df[c].tolist() for c in keys]
    groups: dict[tuple, list[int]] = {}
    for i in range(n):
        groups.setdefault(tuple(_key_token(col[i]) for col in key_cells), []).append(i)
    multi = [idx for idx in groups.values() if len(idx) > 1]
    if not multi:
        Log.debug(
            f"combine_duplicate_rows: {n} row(s), keys {keys}: no duplicate "
            f"keys, table unchanged",
            layer="scifor",
        )
        return df, report

    value_cells = {c: df[c].tolist() for c in values}

    def key_of(i: int) -> dict:
        return {c: key_cells[j][i] for j, c in enumerate(keys)}

    # Decide every multi-row group: its combined cells, or "leave it".
    combined_cells: dict[int, dict] = {}  # first row -> {column: cell}
    list_cols: set = set()
    collapsed_cols: set = set()
    first_list_key: dict = {}  # column -> key of one group that needed a list
    for idx in multi:
        blocker = next(
            (
                (c, value_cells[c][i])
                for c in values
                for i in idx
                if not _is_scalar_cell(value_cells[c][i])
            ),
            None,
        )
        if blocker is not None:
            report.uncombinable.append(
                {
                    "key": key_of(idx[0]),
                    "rows": len(idx),
                    "column": blocker[0],
                    "kind": type(blocker[1]).__name__,
                }
            )
            continue
        cells: dict = {}
        g_lists: list = []
        g_collapsed: list = []
        for c in values:
            vals = [value_cells[c][i] for i in idx]
            if all(_same(vals[0], v) for v in vals[1:]):
                cells[c] = vals[0]
                g_collapsed.append(c)
            else:
                cells[c] = vals
                g_lists.append(c)
                list_cols.add(c)
                first_list_key.setdefault(c, key_of(idx[0]))
        collapsed_cols.update(g_collapsed)
        combined_cells[idx[0]] = cells
        report.combined.append(
            {
                "key": key_of(idx[0]),
                "rows": len(idx),
                "list_columns": g_lists,
                "collapsed_columns": g_collapsed,
            }
        )

    # Rows of combined groups other than the first are dropped (merged).
    merged_away = {
        i for idx in multi if idx[0] in combined_cells for i in idx[1:]
    }
    out_rows = [i for i in range(n) if i not in merged_away]
    position = {i: j for j, i in enumerate(out_rows)}
    first_of = {i: idx[0] for idx in multi if idx[0] in combined_cells for i in idx}
    report.output_row = [position[first_of.get(i, i)] for i in range(n)]

    # A list column must hold one kind of value across the WHOLE table, the
    # single values of other locations included: they share its column type.
    problems = []
    for c in values:
        if c not in list_cols:
            continue
        kinds: set = set()
        for i in out_rows:
            cell = combined_cells.get(i, {}).get(c, value_cells[c][i])
            for v in cell if isinstance(cell, list) else [cell]:
                if not _is_missing(v):
                    kinds.add(_kind(v))
        if len(kinds) > 1 or "datetime" in kinds:
            problems.append(
                {"column": c, "kinds": sorted(kinds), "key": first_list_key.get(c)}
            )
    if problems:
        err = CombineMixedTypesError(problems)
        Log.warn(f"[combine] {err}", layer="scifor")
        raise err

    columns = {}
    index = pd.RangeIndex(len(out_rows))
    for c in df.columns:
        if c in values:
            cells = [combined_cells.get(i, {}).get(c, value_cells[c][i]) for i in out_rows]
        else:
            col = df[c].tolist()
            cells = [col[i] for i in out_rows]
        if c in list_cols:
            columns[c] = _object_series(cells, index, c)
        else:
            try:
                columns[c] = pd.Series(cells, index=index, name=c, dtype=df[c].dtype)
            except (TypeError, ValueError):
                columns[c] = _object_series(cells, index, c)
    out = pd.DataFrame(columns, index=index)[list(df.columns)]

    report.n_rows_out = len(out)
    report.list_columns = [c for c in values if c in list_cols]
    report.collapsed_columns = [
        c for c in values if c in collapsed_cols and c not in list_cols
    ]

    Log.debug(
        f"combine_duplicate_rows: {n} row(s) -> {len(out)}, "
        f"{len(report.combined)} group(s) combined, list column(s) "
        f"{report.list_columns}, keys {keys}",
        layer="scifor",
    )
    if report.combined:
        shown = "; ".join(
            f"{_key_text(g['key'])}: {g['rows']} rows, list {g['list_columns']}"
            f", one value {g['collapsed_columns']}"
            for g in report.combined[:_LOG_GROUPS_MAX]
        )
        more = len(report.combined) - _LOG_GROUPS_MAX
        Log.warn(
            f"[combine] combined {len(report.combined)} location(s) with more "
            f"than one row into one row each: {shown}"
            + (f"; ... +{more} more" if more > 0 else ""),
            layer="scifor",
        )
    for g in report.uncombinable:
        Log.warn(
            f"[combine] {_key_text(g['key'])}: {g['rows']} rows NOT combined - "
            f"column {g['column']!r} holds a {g['kind']} (only single values "
            f"can be combined); the save will refuse this location",
            layer="scifor",
        )
    return out, report
