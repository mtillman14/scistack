"""SciRun: Batch execution utilities for data pipelines.

This package provides utilities for running functions over combinations
of metadata, automatically loading inputs and saving outputs.

Most classes are now defined in scifor and re-exported here for
backwards compatibility.

Example:
    from scirun import for_each, Fixed

    for_each(
        process_data,
        inputs={"raw": RawData, "calibration": Fixed(Calibration, session="baseline")},
        outputs=[ProcessedData],
        subject=[1, 2, 3],
        session=["A", "B", "C"],
    )
"""

from .column_selection import ColumnSelection
from .fixed import Fixed
from .foreach import for_each
from .merge import Merge
from .pathinput import PathInput

__version__ = "0.1.0"

__all__ = [
    "for_each",
    "Fixed",
    "ColumnSelection",
    "Merge",
    "PathInput",
]
