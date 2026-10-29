"""Column selection wrapper for DataFrame inputs in for_each."""

from collections.abc import Mapping
from typing import Any

from .failures import ActionableError


class ColumnSelection:
    """
    Wraps a DataFrame with column selection for use in for_each() inputs.

    After filtering the DataFrame for the current combo, extracts only the
    specified columns. Single column -> its value for a one-row combo, else a
    numpy array (``column_value``). Multiple columns -> a sub-DataFrame.

    Example:
        for_each(
            fn,
            inputs={"speed": ColumnSelection(data_df, ["speed"])},
            subject=[1, 2, 3],
        )

    When ``iterate=True`` the selection means "run fn once per column" rather
    than "pass these columns as one argument." The per-combo loop feeds each
    column (``column_value``, as above) to fn in turn and reassembles the
    per-column results into one wide row. All iterate selections in a single
    for_each() call must share the same column set (zipped by name).

    An **empty** ``columns`` (the default, e.g. ``ColumnSelection(df)``) means
    "all data columns" — every column that is not a schema key — resolved at
    for_each time from the DataFrame. This mirrors how an empty iteration list
    (``subject=[]``) means "all values from the data". (``None`` is accepted as
    an alias for the empty/all sentinel for backward compatibility.)

    ``excl_columns`` drops named columns from whatever ``columns`` resolves to
    (explicit list or the all-columns expansion). This is the natural companion
    to the all-columns/iterate mode: take the non-numeric (or otherwise
    unwanted) columns surfaced in a failure and exclude them, so they are
    skipped during iteration and absent from the aggregated result.
    """

    def __init__(
        self,
        data: Any,
        columns: "list[str] | list[str | list[str]] | dict[str, list[str]] | None" = None,
        iterate: bool = False,
        excl_columns: "list[str] | None" = None,
    ):
        """
        Args:
            data: A pandas DataFrame.
            columns: List of column names to extract after filtering. An empty
                list ``[]`` (the default) means "all data columns", resolved at
                for_each time. ``None`` is accepted as an alias for ``[]``.
                With ``iterate=True`` it may also give column GROUPS, each
                passed to the function together (``parse_column_groups``):
                a list holding sublists (``[["a", "a_date"], "b"]``, paired
                with the outputs by position) or a dict naming each group
                (``{"A": ["a", "a_date"]}``, the names of one output's
                columns).
            iterate: If True, iterate over the columns (one fn call each) and
                reassemble; if False (default), pass the column(s) as a single
                argument.
            excl_columns: Column names to remove from the resolved selection
                (after the all-columns expansion). Useful for dropping the
                non-data columns reported by a ``ColumnFunctionError`` without
                having to enumerate every column you *do* want.
        """
        self.data = data
        # Normalize the all-columns sentinel to an empty list (copying any
        # provided list so a shared default object is never mutated).
        self.columns, self.groups = parse_column_groups(columns)
        self.iterate = iterate
        self.excl_columns = list(excl_columns) if excl_columns else []
        if self.groups is not None and not iterate:
            raise ColumnGroupsError(
                "Column groups run the function once per group, so they need "
                "for_columns(...) (iterate=True). To pass several columns as "
                "one argument, use a flat list: MyVar[['a', 'b']]."
            )
        if self.groups is not None and self.excl_columns:
            raise ColumnGroupsError(
                "excl_columns removes columns from the every-column expansion; "
                "column groups list their columns explicitly, so leave the "
                "unwanted column out of its group instead."
            )

    @property
    def column_spec(self) -> "list[str] | list[list[str]] | dict[str, list[str]]":
        """``columns`` as it would be passed in: the flat list, the list of
        group lists, or the dict of named groups. What every rebuild of a
        selection over new data passes, so the groups survive it."""
        if self.groups is None:
            return list(self.columns)
        if self.groups[0][0] is None:
            return [list(cols) for _, cols in self.groups]
        return {name: list(cols) for name, cols in self.groups}

    @property
    def __name__(self) -> str:
        """Return a display name for format_inputs and error messages."""
        data_name = _display_name(self.data)
        suffix = ", iterate" if self.iterate else ""
        if self.excl_columns:
            excl = ", ".join(f'"{c}"' for c in self.excl_columns)
            suffix = f"{suffix}, excl=[{excl}]"
        if self.groups is not None:
            return f"{data_name}[{describe_column_groups(self.groups)}{suffix}]"
        if not self.columns:
            return f"{data_name}[<all columns>{suffix}]"
        if len(self.columns) == 1 and not self.iterate and not self.excl_columns:
            return f'{data_name}["{self.columns[0]}"]'
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f"{data_name}[{cols}{suffix}]"

    def __hash__(self):
        return hash(
            (
                id(self.data),
                tuple(self.columns),
                self.iterate,
                tuple(self.excl_columns),
                _groups_identity(self.groups),
            )
        )

    def to_key(self) -> str:
        """Return a canonical string for use as a version key.

        Includes ``iterate`` and the resolved column list so that changing the
        iterated column set (including the empty ``[]`` -> all-columns
        resolution done before this is called) invalidates cached results.
        ``excl_columns`` is only appended when non-empty, and ``groups`` only
        when grouped, so existing keys without them are unaffected.
        """
        name = getattr(self.data, "__name__", repr(self.data))
        if self.groups is not None:
            return (
                f"{name}[{self.columns!r}, iterate={self.iterate}, "
                f"groups={_groups_identity(self.groups)!r}]"
            )
        if self.excl_columns:
            return (
                f"{name}[{self.columns!r}, iterate={self.iterate}, "
                f"excl={self.excl_columns!r}]"
            )
        return f"{name}[{self.columns!r}, iterate={self.iterate}]"


def column_value(column: Any) -> Any:
    """What a function receives for ONE selected column of one combo: the
    cell itself when the combo has one row, else a numpy array of the column.

    The one owner for every single-column delivery: a single-column
    ``ColumnSelection``, each pass of ``for_columns`` (scifor), and scidb's
    per-combo selection. A record saved as a one-row table (a CSV spread one
    record per subject) gives each column one value per subject, and that
    value is what the function means (scistack.log 2026-10-22: ``value.split``
    on a pandas ``StringArray``). ``to_numpy`` rather than ``.values``: on
    pandas 3 a text column is a string dtype, and ``.values`` returns a pandas
    extension array, not numpy. A plain input with one data column already
    unwraps one row the same way (``foreach._extract_data``).
    """
    if len(column) == 1:
        return column.iloc[0]
    return column.to_numpy()


def _display_name(obj: Any) -> str:
    """Get a display name for an object -- the one copy; ``merge`` imports it."""
    try:
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            return f"DataFrame{list(obj.columns)}"
    except ImportError:
        pass
    return getattr(obj, "__name__", type(obj).__name__)


# ---------------------------------------------------------------------------
# Column groups (for_columns): several columns passed to the function together
# ---------------------------------------------------------------------------

#: for_columns modes, decided by :func:`resolve_column_groups`.
COMBINE = "combine"
SPLIT = "split"


class ColumnGroupsError(ActionableError, ValueError):
    """A for_columns column-group specification that cannot run as written:
    a malformed group, groups without names where names are needed, or a
    group count that does not match the outputs. Raised before any call;
    the message says what to change."""


def parse_column_groups(
    columns: Any,
) -> "tuple[list[str], list[tuple[str | None, list[str]]] | None]":
    """``(columns, groups)`` for whatever ``ColumnSelection(columns=...)``
    was given. THE owner of what a column spec means.

    - ``None`` / ``[]`` -> ``([], None)``: every data column.
    - ``"a"`` / ``["a", "b"]`` -> ``(["a", "b"], None)``: flat, one column each.
    - a list holding sublists -> unnamed groups ``[(None, [...]), ...]``; a
      plain string in it is a one-column group.
    - a dict -> named groups ``[(name, [...]), ...]`` in key order.

    ``columns`` is always the ordered union of every group's columns, so a
    reader that only needs "which columns" never looks at the groups.
    Groups that add nothing to a flat list normalize back to it, so one
    meaning has one identity: ``{"a": ["a"]}`` and ``[["a"], ["b"]]`` ARE
    ``["a"]`` / ``["a", "b"]``.
    """
    if columns is None:
        return [], None
    if isinstance(columns, str):
        return ([columns] if columns else []), None
    if isinstance(columns, Mapping):
        groups: "list[tuple[str | None, list[str]]]" = []
        for name, cols in columns.items():
            if not isinstance(name, str) or not name:
                raise ColumnGroupsError(
                    f"for_columns group names must be non-empty strings (they "
                    f"name the output's columns); got {name!r}."
                )
            groups.append((name, _group_columns(cols, name)))
        if all(cols == [name] for name, cols in groups):
            return [name for name, _ in groups], None
        return _union(groups), groups
    items = list(columns)
    if not any(isinstance(item, (list, tuple)) for item in items):
        return items, None
    groups = [(None, _group_columns(item, f"#{i + 1}")) for i, item in enumerate(items)]
    if all(len(cols) == 1 for _, cols in groups):
        return [cols[0] for _, cols in groups], None
    return _union(groups), groups


def _group_columns(cols: Any, label: str) -> "list[str]":
    """One group's column list, validated: non-empty, strings, no repeats."""
    if isinstance(cols, str):
        cols = [cols]
    if not isinstance(cols, (list, tuple)) or not cols:
        raise ColumnGroupsError(
            f"for_columns group {label} must list at least one column name; "
            f"got {cols!r}."
        )
    bad = [c for c in cols if not isinstance(c, str) or not c]
    if bad:
        raise ColumnGroupsError(
            f"for_columns group {label}: column names must be non-empty "
            f"strings; got {bad!r}."
        )
    repeated = sorted({c for c in cols if list(cols).count(c) > 1})
    if repeated:
        raise ColumnGroupsError(
            f"for_columns group {label} lists {repeated} more than once."
        )
    return list(cols)


def _union(groups: "list[tuple[str | None, list[str]]]") -> "list[str]":
    return list(dict.fromkeys(c for _, cols in groups for c in cols))


def _groups_identity(groups: "list[tuple[str | None, list[str]]] | None") -> Any:
    """A hashable, order-preserving spelling of the groups (``None`` when
    flat): named -> ``(("A", ("a", "a_date")), ...)``; unnamed -> the
    column tuples."""
    if groups is None:
        return None
    if groups[0][0] is None:
        return tuple(tuple(cols) for _, cols in groups)
    return tuple((name, tuple(cols)) for name, cols in groups)


def describe_column_groups(groups: "list[tuple[str | None, list[str]]]") -> str:
    """The groups as they read in code: ``{"A": ["a", "a_date"]}`` or
    ``[["a", "a_date"], ["b"]]``."""

    def cols_text(cols: "list[str]") -> str:
        return "[" + ", ".join(f'"{c}"' for c in cols) + "]"

    if groups[0][0] is None:
        return "[" + ", ".join(cols_text(cols) for _, cols in groups) + "]"
    return "{" + ", ".join(f'"{name}": {cols_text(cols)}' for name, cols in groups) + "}"


def resolve_column_groups(
    columns: "list[str]",
    groups: "list[tuple[str | None, list[str]]] | None",
    output_names: "list[str]",
    *,
    every_column: bool = False,
) -> "tuple[str, list[tuple[str, list[str]]]]":
    """``(mode, [(name, [columns])])`` for a for_columns call. THE owner of
    combine vs split; the GUI, scidb and the bridge ask this, never re-derive.

    The number of outputs decides the mode:

    - ONE output -> ``COMBINE``: each group becomes column(s) of that output,
      named by the group (a flat column names itself; a multi-column group
      needs a dict key).
    - SEVERAL outputs -> ``SPLIT``: group *i* is saved as ``output_names[i]``
      (the name each group carries), so the counts must match and the groups
      come as a list: a dict's names would name nothing.

    ``columns`` is the resolved flat column list (used when ``groups`` is
    ``None``). ``every_column``: it came from ``for_columns()`` (all columns),
    whose order is the data's, so pairing it with outputs is refused.
    """
    n_out = len(output_names)
    if groups is None:
        flat = [(c, [c]) for c in columns]
        if n_out <= 1:
            return COMBINE, flat
        if every_column:
            raise ColumnGroupsError(
                f"for_columns() with every column cannot be split across "
                f"{n_out} outputs {output_names}: the pairing would follow the "
                f"data's column order. List the columns (or groups) in the "
                f"same order as the outputs."
            )
        groups = [(None, cols) for _, cols in flat]
    named = groups[0][0] is not None
    if n_out <= 1:
        if not named:
            multi = [cols for _, cols in groups if len(cols) > 1]
            raise ColumnGroupsError(
                f"With one output, each for_columns group becomes a column of "
                f"it, so a group of several columns needs a name: pass a dict, "
                f"e.g. {{\"name\": {multi[0]!r}}}. (Several outputs would split "
                f"the groups into one output each.)"
            )
        return COMBINE, [(name, cols) for name, cols in groups]  # type: ignore[misc]
    if named:
        raise ColumnGroupsError(
            f"With {n_out} outputs {output_names}, each for_columns group is "
            f"saved as one of them, paired in order, so the group names "
            f"{[n for n, _ in groups]} would name nothing. Pass the groups as "
            f"a list, in the same order as the outputs."
        )
    if len(groups) != n_out:
        raise ColumnGroupsError(
            f"for_columns has {len(groups)} group(s) but there are {n_out} "
            f"outputs {output_names}; with several outputs each group is saved "
            f"as one output, paired in order, so the counts must match."
        )
    return SPLIT, [(name, cols) for name, (_, cols) in zip(output_names, groups, strict=True)]


def overlapping_columns(
    groups: "list[tuple[str, list[str]]]",
) -> "dict[str, list[str]]":
    """``{column: [group names]}`` for every column in more than one group."""
    owners: "dict[str, list[str]]" = {}
    for name, cols in groups:
        for c in cols:
            owners.setdefault(c, []).append(name)
    return {c: names for c, names in owners.items() if len(names) > 1}
