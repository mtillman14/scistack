"""scidb's DB-aware extension of scifor.Merge.

The container (.tables, .to_key(), .__name__) lives entirely in
scifor.Merge now -- this subclass restores only the one DB-only method with
no scifor equivalent: .to_csv(), which loads each constituent variable type
from the database and inner-joins on shared schema keys (schema-id-keyed
export), as opposed to scifor's Merge.to_csv() (inherited by this class
unless overridden), which does a generic in-memory pd.merge join of
DataFrames and has no notion of loading from a database at all.

Mirrors the same "unify the container, subclass for DB-only surface"
pattern as column_selection.py.
"""

from typing import Any

from scifor import Merge as _SciforMerge


class Merge(_SciforMerge):
    """``scifor.Merge`` plus a DB-aware ``.to_csv()``. See module docstring."""

    def to_csv(self, filename: str, *args: Any, **kwargs: Any) -> None:
        """Export the merged variables to a CSV file in flat table format.

        Each constituent is loaded independently and inner-joined on its
        shared schema keys, producing one row per schema_id with one value
        column per constituent (scalar variables) or per table column. Every
        constituent must reduce to one row per schema_id. ``filename`` must
        end with ``.csv``. ``kwargs`` mirror ``load()`` (``where=``,
        ``version=``, ``db=``, metadata).

        Example:
            # subject,trial,StepLength,Speed
            Merge(StepLength, Speed).to_csv("gait.csv", subject=[1, 2])
        """
        from scidb.csv_export import export_csv

        export_csv(self, filename, *args, **kwargs)
