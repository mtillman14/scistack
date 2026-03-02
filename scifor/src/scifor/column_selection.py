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
    """

    def __init__(self, data: Any, columns: list[str]):
        """
        Args:
            data: A pandas DataFrame.
            columns: List of column names to extract after filtering.
        """
        self.data = data
        self.columns = columns

    @property
    def __name__(self) -> str:
        """Return a display name for format_inputs and error messages."""
        data_name = _display_name(self.data)
        if len(self.columns) == 1:
            return f'{data_name}["{self.columns[0]}"]'
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f'{data_name}[{cols}]'

    def __hash__(self):
        return hash((id(self.data), tuple(self.columns)))


def _display_name(obj: Any) -> str:
    """Get a display name for an object."""
    try:
        import pandas as pd
        if isinstance(obj, pd.DataFrame):
            return f"DataFrame{list(obj.columns)}"
    except ImportError:
        pass
    return getattr(obj, '__name__', type(obj).__name__)
