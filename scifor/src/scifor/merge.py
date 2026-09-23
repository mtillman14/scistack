"""Merge input wrapper for for_each: combines multiple DataFrames into one."""

from typing import Any

from .column_selection import _display_name


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
            raise ValueError(f"Merge requires at least 2 inputs, got {len(tables)}.")
        for t in tables:
            if isinstance(t, Merge):
                raise TypeError("Cannot nest Merge inside another Merge.")
        self.tables = tables

    def to_key(self) -> str:
        """Return a canonical string for use as a version key."""
        parts = []
        for spec in self.tables:
            if hasattr(spec, "to_key"):
                parts.append(spec.to_key())
            elif isinstance(spec, type):
                parts.append(spec.__name__)
            else:
                parts.append(repr(spec))
        return f"Merge({', '.join(parts)})"

    def to_csv(
        self, filename: str, where=None, verbose: bool = False, **metadata: Any
    ) -> None:
        """Inner-join the constituents and write the result to a flat CSV.

        Each constituent DataFrame is row-filtered by ``**metadata`` (scalar →
        equality, list/tuple/set → membership) on any matching schema column and
        then the constituents are inner-joined on their shared schema columns —
        keeping a single copy of those columns — and an optional ``where=``
        ColName/Col filter is applied to the joined table (so it may reference a
        column from any constituent). Non-schema columns are assumed not to
        overlap. ``filename`` must end with ``.csv``.

        Unlike the ``for_each`` merge path, this is a real key-based inner join
        (no per-combo iteration); see ``csv_export.export_merge_csv``.

        Args:
            filename: Output path; must end with ``.csv``.
            where: Optional scifor ColName/Col filter applied to every constituent.
            verbose: If True, log the join diagnostics at INFO (visible at
                the default log level) instead of DEBUG (NOTE 2).
            **metadata: Per-constituent row filters on matching schema columns.

        Example:
            # subject,trial,StepLength,Speed
            Merge(step_df, speed_df).to_csv("gait.csv", subject=[1, 2])
        """
        from .csv_export import export_merge_csv

        export_merge_csv(
            self,
            filename,
            where=where,
            verbose=verbose,
            **metadata,
        )

    def as_df(self, where=None, verbose: bool = False, **metadata: Any):
        """Inner-join the constituents and return the result as a pandas DataFrame.

        Same join and filter semantics as :meth:`to_csv` (per-constituent
        ``**metadata`` filters, post-join ``where=``, one copy of the shared
        schema columns), but returns the DataFrame instead of writing a file.
        ``pd.DataFrame(Merge(...))`` is intentionally not supported — use this.

        Args:
            where: Optional scifor ColName/Col filter applied to the joined table.
            verbose: If True, log the join diagnostics at INFO (visible at
                the default log level) instead of DEBUG (NOTE 2).
            **metadata: Per-constituent row filters on matching schema columns.

        Example:
            merged = Merge(step_df, speed_df)
            df = merged.as_df(subject=[1, 2])
        """
        from .csv_export import merge_to_dataframe

        return merge_to_dataframe(
            self,
            where=where,
            verbose=verbose,
            **metadata,
        )

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

