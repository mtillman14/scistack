"""Fixed metadata wrapper for DataFrame inputs in for_each."""

from typing import Any


class Fixed:
    """
    Wrapper to specify fixed metadata overrides for a DataFrame input.

    Use this when an input should be filtered with different metadata
    than the current iteration's metadata.

    Works with plain DataFrames (filtered per-iteration using schema key columns).

    Example:
        # Always filter baseline to session="pre", regardless of current session
        for_each(
            compare_to_baseline,
            inputs={
                "baseline": Fixed(raw_df, session="pre"),
                "current": raw_df,
            },
            subject=subjects,
            session=sessions,
        )
    """

    def __init__(self, data: Any, **fixed_metadata: Any):
        """
        Args:
            data: A pandas DataFrame to filter per iteration,
                  or another scifor wrapper (Merge, ColumnSelection).
            **fixed_metadata: Metadata values that override the iteration metadata.
        """
        self.data = data
        self.fixed_metadata = fixed_metadata
