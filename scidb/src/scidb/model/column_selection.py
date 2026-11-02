"""scidb's DB-aware extension of scifor.ColumnSelection.

The container (data/columns/iterate/excl_columns, to_key(), __hash__,
__name__) lives entirely in scifor.ColumnSelection now -- this subclass adds
only the surface that genuinely needs a live BaseVariable class reference and
has no scifor equivalent: comparison operators that build scidb.model.filters
objects for where=, plus .load()/.to_csv().

Created automatically by BaseVariable.__class_getitem__ (``MyVar["col"]``)
and ``for_columns()``. Since this is a subclass, isinstance(x,
scifor.ColumnSelection) is True for instances of this class too -- scidb's
internal loading/conversion code checks against the scifor base so a bare
scifor.ColumnSelection(df, ...) (no scidb-specific surface needed) is
handled identically.
"""

from typing import Any

from scifor import ColumnSelection as _SciforColumnSelection


class ColumnSelection(_SciforColumnSelection):
    """``scifor.ColumnSelection`` plus DB-only comparison operators and
    ``.load()``/``.to_csv()``. See module docstring."""

    # Overriding __eq__ below would otherwise make instances unhashable
    # (Python clears __hash__ on any class that defines __eq__ without also
    # redeclaring it) -- keep the base class's identity-based hash.
    __hash__ = _SciforColumnSelection.__hash__

    def load(self, **metadata: Any) -> Any:
        """Load from the underlying var_type, then apply column selection."""
        return self.data.load(**metadata)

    def to_csv(self, filename: str, *args, **kwargs) -> None:
        """Export the selected column(s) to a CSV file in flat table format.

        Writes one row per schema_id with the selected columns as value
        columns. The underlying variable must be a single-row table per
        schema_id. ``filename`` must end with ``.csv``. ``kwargs`` mirror
        ``load()`` (``where=``, ``version=``, ``db=``, metadata).

        Example:
            GaitData["Speed"].to_csv("speed.csv", subject=1)
            GaitData[["Speed", "Cadence"]].to_csv("gait.csv", subject=1)
        """
        from scidb.csv_export import export_csv

        export_csv(self, filename, *args, **kwargs)

    # --- Comparison operators that produce ColumnFilter objects ---

    def __eq__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], "==", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def __ne__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], "!=", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def __lt__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], "<", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def __le__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], "<=", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def __gt__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], ">", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def __ge__(self, other):
        try:
            from scidb.model.filters import ColumnFilter

            return ColumnFilter(self.data, self.columns[0], ">=", other)
        except ImportError:
            raise NotImplementedError(
                "Comparison operators on ColumnSelection require scidb."
            )

    def isin(self, values):
        """Create an InFilter for set membership testing."""
        try:
            from scidb.model.filters import InFilter

            return InFilter(self.data, self.columns[0], list(values))
        except ImportError:
            raise NotImplementedError("isin() on ColumnSelection requires scidb.")
