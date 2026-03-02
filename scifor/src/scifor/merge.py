"""Merge input wrapper for for_each: combines multiple variables into one DataFrame."""

from typing import Any


class Merge:
    """
    Combines 2+ variable inputs into a single pandas DataFrame for use
    in for_each() inputs.

    Each constituent is loaded individually per iteration and merged
    column-wise. DataFrame variables contribute all their columns.
    Array and scalar variables are added as a column named after their
    variable class name.

    Constituents can be:
    - Variable types (classes with .load())
    - Fixed wrappers (loaded with overridden metadata)
    - ColumnSelection wrappers (MyVar["col"] or MyVar[["a", "b"]])
    - Combinations: Fixed(MyVar["col"], session="BL")

    Each constituent must match exactly one record per iteration.

    Example:
        for_each(
            analyze,
            inputs={
                "combined": Merge(GaitData, ForceData),
            },
            outputs=[Result],
            subject=[1, 2, 3],
        )

        # With Fixed and ColumnSelection
        for_each(
            analyze,
            inputs={
                "combined": Merge(
                    GaitData["force"],
                    Fixed(PareticSide, session="BL"),
                ),
            },
            outputs=[Result],
            subject=[1, 2, 3],
            session=["A", "B"],
        )
    """

    def __init__(self, *var_specs: Any):
        if len(var_specs) < 2:
            raise ValueError(
                f"Merge requires at least 2 variable inputs, got {len(var_specs)}."
            )
        for spec in var_specs:
            if isinstance(spec, Merge):
                raise TypeError("Cannot nest Merge inside another Merge.")
        self.var_specs = var_specs

    def to_key(self) -> str:
        """Return a canonical string for use as a version key."""
        parts = []
        for spec in self.var_specs:
            if hasattr(spec, 'to_key'):
                parts.append(spec.to_key())
            elif isinstance(spec, type):
                parts.append(spec.__name__)
            else:
                parts.append(repr(spec))
        return f"Merge({', '.join(parts)})"

    @property
    def __name__(self) -> str:
        """Display name for format_inputs and error messages."""
        from .column_selection import ColumnSelection
        from .fixed import Fixed

        parts = []
        for spec in self.var_specs:
            if isinstance(spec, Fixed):
                inner = spec.var_type
                if isinstance(inner, ColumnSelection):
                    inner_name = inner.__name__
                else:
                    inner_name = getattr(inner, '__name__', type(inner).__name__)
                fixed_str = ", ".join(
                    f"{k}={v}" for k, v in spec.fixed_metadata.items()
                )
                parts.append(f"Fixed({inner_name}, {fixed_str})")
            elif isinstance(spec, ColumnSelection):
                parts.append(spec.__name__)
            else:
                parts.append(getattr(spec, '__name__', type(spec).__name__))
        return f"Merge({', '.join(parts)})"
