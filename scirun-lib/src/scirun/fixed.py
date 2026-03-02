"""Fixed metadata wrapper for variable type inputs in for_each (DB-backed)."""

from typing import Any


class Fixed:
    """
    Wrapper to specify fixed metadata overrides for an input variable type.

    Use this when an input should be loaded with different metadata
    than the current iteration's metadata.

    Works with variable types (classes with .load()) for DB-backed for_each.
    For standalone DataFrame usage, see scifor.Fixed.

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
    """

    def __init__(self, var_type: Any, **fixed_metadata: Any):
        """
        Args:
            var_type: The variable type to load (must have a .load() method),
                      or a ColumnSelection or PathInput wrapper.
            **fixed_metadata: Metadata values that override the iteration metadata.
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
