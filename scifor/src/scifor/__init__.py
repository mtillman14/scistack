"""scifor: Standalone for_each batch execution for data pipelines.

Works with plain DataFrames. No database required.

Example:
    import pandas as pd
    from scifor import set_schema, for_each, Col

    set_schema(["subject", "session"])

    raw_df = pd.DataFrame({
        "subject": [1, 1, 2, 2],
        "session": ["pre", "post", "pre", "post"],
        "emg":     [0.1,  0.2,  0.3,  0.4],
    })

    result = for_each(
        lambda signal: signal.mean(),
        inputs={"signal": raw_df},
        subject=[1, 2],
        session=["pre", "post"],
    )
"""

from .schema import set_schema, get_schema
from .foreach import for_each
from .fixed import Fixed
from .merge import Merge
from .column_selection import ColumnSelection
from .pathinput import PathInput
from .filters import Col, ColFilter, CompoundFilter, NotFilter
from .files import MatFile, CsvFile

__version__ = "0.1.0"

__all__ = [
    # Schema
    "set_schema",
    "get_schema",
    # Batch execution
    "for_each",
    # Input wrappers
    "Fixed",
    "Merge",
    "ColumnSelection",
    "PathInput",
    # Filters
    "Col",
    "ColFilter",
    "CompoundFilter",
    "NotFilter",
    # File I/O
    "MatFile",
    "CsvFile",
]
