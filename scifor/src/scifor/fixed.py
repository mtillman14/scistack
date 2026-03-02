"""Fixed metadata wrapper for for_each inputs."""

from typing import Any


class Fixed:
    """
    Wrapper to specify fixed metadata overrides for an input.

    Use this when an input should be loaded with different metadata
    than the current iteration's metadata.

    Works with both variable types (classes with .load()) and plain
    DataFrames (filtered per-iteration using schema key columns).

    Example:
        # Always load baseline from session="BL", regardless of current session
        for_each(
            compare_to_baseline,
            inputs={
                "baseline": Fixed(StepLength, session="BL"),
                "current": StepLength,
            },
            outputs=[Delta],
            subject=subjects,
            session=sessions,
        )

        # DataFrame input fixed to session="baseline"
        for_each(
            compare,
            inputs={
                "baseline": Fixed(raw_df, session="baseline"),
                "current":  raw_df,
            },
            outputs=[],
            subject=[1, 2],
            session=["post"],
        )
    """

    def __init__(self, var_type: Any, **fixed_metadata: Any):
        """
        Args:
            var_type: The variable type to load (must have a .load() method)
                      or a pandas DataFrame to filter per iteration.
            **fixed_metadata: Metadata values that override the iteration metadata
        """
        self.var_type = var_type
        self.fixed_metadata = fixed_metadata

    def to_key(self) -> str:
        """Return a canonical string for use as a version key."""
        from .column_selection import ColumnSelection
        if isinstance(self.var_type, ColumnSelection):
            inner_key = self.var_type.to_key()
        elif isinstance(self.var_type, type):
            inner_key = self.var_type.__name__
        else:
            inner_key = repr(self.var_type)
        sorted_kv = ", ".join(
            f"{k}={v!r}" for k, v in sorted(self.fixed_metadata.items())
        )
        if sorted_kv:
            return f"Fixed({inner_key}, {sorted_kv})"
        return f"Fixed({inner_key})"
