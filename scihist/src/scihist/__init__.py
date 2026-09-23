"""SciHist (deprecated) — thin shim over the consolidated scidb API.

Lineage-tracked batch execution, the node-staleness API, and lineage-aware
save now live in **scidb** (``scidb.for_each`` tracks lineage by default).
This package remains only as a backward-compatible shim for existing imports
(e.g. ``scistack-gui`` and the MATLAB ``+scihist`` bridge). Prefer importing
from ``scidb`` directly; ``scihist`` will be removed in a future release.

Behavioral nuances preserved by the shim:
- ``scihist.for_each`` defaults ``skip_computed=True`` (scidb defaults False).
- ``scihist.configure_database`` is ``scidb.configure_database`` (a re-export).
"""

import warnings as _warnings

# Core batch execution + lineage-aware save (shimmed to preserve scihist defaults)
# Re-export DB wrappers from scidb
# Re-export the step-function marker (replaces the removed @lineage_fcn).
from scidb import ColumnSelection, Fixed, ForEachConfig, Merge, scistack

# Re-export scifor helpers
from scifor import Col, PathInput, get_schema, set_schema

from .database import configure_database
from .foreach import for_each, save
from .state import (
    check_combo_state,
    check_multiple_nodes_state,
    check_node_state,
    check_pathinput_node_state,
)

_warnings.warn(
    "scihist is deprecated; its functionality has moved to scidb. "
    "Import from scidb instead (e.g. `from scidb import for_each, save, "
    "configure_database`).",
    DeprecationWarning,
    stacklevel=2,
)

__version__ = "0.1.0"

__all__ = [
    # Core batch execution
    "for_each",
    "save",
    # Configuration
    "configure_database",
    # Node staleness
    "check_combo_state",
    "check_node_state",
    "check_pathinput_node_state",
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
    # Step-function marker
    "scistack",
]
