"""Merge input wrapper for for_each: combines multiple DataFrames into one."""

from typing import Any


class Merge:
    """
    Combines 2+ DataFrame inputs into a single pandas DataFrame for use
    in for_each() inputs.

    Each constituent is filtered individually per iteration and merged
    column-wise.

    Constituents can be:
    - Plain pandas DataFrames
    - Fixed wrappers (DataFrames with overridden metadata)
    - ColumnSelection wrappers (DataFrame with column extraction)

    Example:
        for_each(
            analyze,
            inputs={
                "combined": Merge(gait_df, force_df),
            },
            subject=[1, 2, 3],
        )

        # With Fixed
        for_each(
            analyze,
            inputs={
                "combined": Merge(
                    gait_df,
                    Fixed(paretic_df, session="BL"),
                ),
            },
            subject=[1, 2, 3],
            session=["A", "B"],
        )
    """

    def __init__(self, *tables: Any):
        if len(tables) < 2:
            raise ValueError(
                f"Merge requires at least 2 inputs, got {len(tables)}."
            )
        for t in tables:
            if isinstance(t, Merge):
                raise TypeError("Cannot nest Merge inside another Merge.")
        self.tables = tables

    @property
    def __name__(self) -> str:
        """Display name for format_inputs and error messages."""
        from .column_selection import ColumnSelection
        from .fixed import Fixed

        parts = []
        for spec in self.tables:
            if isinstance(spec, Fixed):
                inner = spec.data
                if isinstance(inner, ColumnSelection):
                    inner_name = inner.__name__
                else:
                    inner_name = _display_name(inner)
                fixed_str = ", ".join(
                    f"{k}={v}" for k, v in spec.fixed_metadata.items()
                )
                parts.append(f"Fixed({inner_name}, {fixed_str})")
            elif isinstance(spec, ColumnSelection):
                parts.append(spec.__name__)
            else:
                parts.append(_display_name(spec))
        return f"Merge({', '.join(parts)})"


def _display_name(obj: Any) -> str:
    """Get a display name for an object."""
    try:
        import pandas as pd
        if isinstance(obj, pd.DataFrame):
            return f"DataFrame{list(obj.columns)}"
    except ImportError:
        pass
    return getattr(obj, '__name__', type(obj).__name__)
