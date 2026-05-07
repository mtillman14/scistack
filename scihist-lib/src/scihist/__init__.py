"""SciHist: Lineage-tracked batch execution for scientific data pipelines.

This package adds lineage tracking on top of scidb. It provides the same
for_each() interface as scidb, but automatically wraps functions in
LineageFcn for provenance recording.

Example:
    from scihist import for_each, Fixed, configure_database
    from scilineage import lineage_fcn

    configure_database("experiment.duckdb", ["subject", "session"])

    @lineage_fcn
    def process_data(raw, calibration):
        return raw * calibration

    for_each(
        process_data,
        inputs={"raw": RawData, "calibration": Fixed(Calibration, session="baseline")},
        outputs=[ProcessedData],
        subject=[1, 2, 3],
        session=["A", "B", "C"],
    )
"""

from .foreach import for_each, save
from .database import configure_database, find_by_lineage
from .state import check_combo_state, check_node_state, check_multiple_nodes_state

# Re-export DB wrappers from scidb
from scidb import Fixed, Merge, ColumnSelection, ForEachConfig

# Re-export scifor helpers
from scifor import Col, set_schema, get_schema, PathInput

# Re-export scilineage system
from scilineage import lineage_fcn, LineageFcn, LineageFcnResult, LineageFcnInvocation

__version__ = "0.1.0"

__all__ = [
    # Core batch execution
    "for_each",
    "save",
    # Configuration
    "configure_database",
    # Lineage query
    "find_by_lineage",
    # Node staleness
    "check_combo_state",
    "check_node_state",
    "check_multiple_nodes_state",
    # DB wrappers
    "Fixed",
    "Merge",
    "ColumnSelection",
    "ForEachConfig",
    "PathInput",
    # Schema helpers
    "Col",
    "set_schema",
    "get_schema",
    # Lineage system
    "lineage_fcn",
    "LineageFcn",
    "LineageFcnResult",
    "LineageFcnInvocation",
]
