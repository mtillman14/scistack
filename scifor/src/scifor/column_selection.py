"""Column selection wrapper for DataFrame inputs in for_each."""

from typing import Any


class ColumnSelection:
    """
    Wraps a DataFrame with column selection for use in for_each() inputs.

    After filtering the DataFrame for the current combo, extracts only the
    specified columns. Single column -> returns numpy array of values.
    Multiple columns -> returns a sub-DataFrame.

    Example:
        for_each(
            fn,
            inputs={"speed": ColumnSelection(data_df, ["speed"])},
            subject=[1, 2, 3],
        )

    When ``iterate=True`` the selection means "run fn once per column" rather
    than "pass these columns as one argument." The per-combo loop feeds each
    column (as a single-column array) to fn in turn and reassembles the
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
        columns: "list[str] | None" = None,
        iterate: bool = False,
        excl_columns: "list[str] | None" = None,
    ):
        """
        Args:
            data: A pandas DataFrame.
            columns: List of column names to extract after filtering. An empty
                list ``[]`` (the default) means "all data columns", resolved at
                for_each time. ``None`` is accepted as an alias for ``[]``.
            iterate: If True, iterate over the columns (one fn call each) and
                reassemble; if False (default), pass the column(s) as a single
                argument.
            excl_columns: Column names to remove from the resolved selection
                (after the all-columns expansion). Useful for dropping the
                non-data columns reported by a ``ColumnFunctionError`` without
                having to enumerate every column you *do* want.
        """
        if columns is None:
            columns = []
        self.data = data
        # Normalize the all-columns sentinel to an empty list (copying any
        # provided list so a shared default object is never mutated).
        self.columns = list(columns) if columns else []
        self.iterate = iterate
        self.excl_columns = list(excl_columns) if excl_columns else []

    @property
    def __name__(self) -> str:
        """Return a display name for format_inputs and error messages."""
        data_name = _display_name(self.data)
        suffix = ", iterate" if self.iterate else ""
        if self.excl_columns:
            excl = ", ".join(f'"{c}"' for c in self.excl_columns)
            suffix = f"{suffix}, excl=[{excl}]"
        if not self.columns:
            return f"{data_name}[<all columns>{suffix}]"
        if len(self.columns) == 1 and not self.iterate and not self.excl_columns:
            return f'{data_name}["{self.columns[0]}"]'
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f"{data_name}[{cols}{suffix}]"

    def __hash__(self):
        return hash(
            (id(self.data), tuple(self.columns), self.iterate, tuple(self.excl_columns))
        )

    def to_key(self) -> str:
        """Return a canonical string for use as a version key.

        Includes ``iterate`` and the resolved column list so that changing the
        iterated column set (including the empty ``[]`` -> all-columns
        resolution done before this is called) invalidates cached results.
        ``excl_columns`` is only appended when non-empty, so existing keys
        without it are unaffected.
        """
        name = getattr(self.data, "__name__", repr(self.data))
        if self.excl_columns:
            return (
                f"{name}[{self.columns!r}, iterate={self.iterate}, "
                f"excl={self.excl_columns!r}]"
            )
        return f"{name}[{self.columns!r}, iterate={self.iterate}]"


def _display_name(obj: Any) -> str:
    """Get a display name for an object -- the one copy; ``merge`` imports it."""
    try:
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            return f"DataFrame{list(obj.columns)}"
    except ImportError:
        pass
    return getattr(obj, "__name__", type(obj).__name__)
