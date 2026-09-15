"""
SciStack: Scientific Data Versioning Framework

A lightweight database framework for scientific computing that provides:
- Type-safe serialization of numpy arrays, DataFrames, and custom types
- Automatic content-based versioning
- Flexible metadata-based addressing
- DuckDB storage for data and lineage (via SciDuck)

Example:
    from scidb import configure_database, BaseVariable
    import numpy as np

    # Setup (single DuckDB file for data + lineage, auto-registers types)
    db = configure_database("experiment.duckdb", ["subject", "session"])

    class RawSignal(BaseVariable):
        schema_version = 1

    # Save/load
    RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
    raw = RawSignal.load(subject=1, session="A")
"""

# From scifor (Layer 1) — Fixed/ColName/EachOf are the same classes scidb
# constructs and reads; scidb has no class definitions of its own for these
# anymore, only the DB-aware loading/orchestration around them (see
# foreach.py). ColumnSelection and Merge are the two exceptions: scidb
# exports its own thin subclass of each (see column_selection.py, merge.py)
# adding DB-only methods with no scifor equivalent -- comparison operators
# (MyVar["col"] == value) + .load() for ColumnSelection, a schema-id-keyed
# .to_csv() for Merge (scifor's own .to_csv() only knows how to join
# already-in-memory DataFrames, not load variable types from a database).
from scifor import (
    Col,
    ColName,
    EachOf,
    Fixed,
    PathInput,
    PathOutput,
    get_schema,
    set_schema,
)

from .across_variants import AcrossVariants
from .artifact_stamp import read_artifact_stamp, stamp_artifact
from .column_selection import ColumnSelection
from .parameter import Parameter
from .database import configure_database, get_database, get_user_id
from .discover import (
    FUNCTION_ROLES,
    DiscoveryResult,
    ModuleError,
    ModuleExports,
    PackageResult,
    discover_module,
    function_role,
    scan_package,
    scan_project,
)
from .exceptions import (
    AmbiguousParamError,
    AmbiguousVersionError,
    DatabaseLockedError,
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
    PipelineCycleError,
    ReservedMetadataKeyError,
    SciStackError,
)
from .exclusions import exclude_schema, include_schema, list_exclusions
from .filters import raw_sql, schema_key

# Batch execution (Layer 2 — DB-backed, no lineage)
from .foreach import for_each
from .foreach_config import ForEachConfig
from .glue import (
    GlueChainOrderError,
    GlueError,
    GlueLanguageMismatchError,
    GlueRowsChangedError,
    GlueSchemaKeysAlteredError,
    GlueSpec,
    GlueUnsupportedInputError,
)
from .lineage_save import save
from .log import Log
from .merge import Merge
from .pipeline import Pipeline, PipelineBinding, Step, active_pipeline, scistack
from .state import (
    check_combo_state,
    check_multiple_nodes_state,
    check_node_state,
    check_pathinput_node_state,
)
from .variable import BaseVariable
from .variant import Variant, branch_param

# Imported last, and as a module rather than its contents: `entities` builds
# BaseVariable subclasses and Parameters, so it must come after both exist,
# and `from scidb import entities; entities.WINDOW` needs the module object
# itself -- attribute access on it is what resolves a declared name.
from . import entities  # noqa: E402
from . import schema_order  # noqa: E402

__version__ = "0.1.0"

__all__ = [
    # Core classes
    "BaseVariable",
    "Parameter",
    # Entities file (TOML) -- the writable declaration surface
    "entities",
    "schema_order",
    # Discovery
    "scan_project",
    "scan_package",
    "discover_module",
    "DiscoveryResult",
    "PackageResult",
    "ModuleExports",
    "ModuleError",
    # Function role (process / plot / stat / glue) — one classifier, shared
    "function_role",
    "FUNCTION_ROLES",
    # Configuration
    "configure_database",
    "get_database",
    "get_user_id",
    # Save
    "save",
    # Pipeline node staleness
    "check_combo_state",
    "check_node_state",
    "check_pathinput_node_state",
    "check_multiple_nodes_state",
    # Logging
    "Log",
    # Batch execution
    "for_each",
    "scistack",
    "Pipeline",
    "PipelineBinding",
    "Step",
    "active_pipeline",
    "Fixed",
    "Variant",
    "AcrossVariants",
    "stamp_artifact",
    "read_artifact_stamp",
    "branch_param",
    "Merge",
    "ColumnSelection",
    "ColName",
    "EachOf",
    "ForEachConfig",
    "PathInput",
    "PathOutput",
    # Glue nodes (transient in-memory reshaping; never saved)
    "GlueSpec",
    # Standalone / DataFrame support
    "Col",
    "set_schema",
    "get_schema",
    # Filter utilities
    "raw_sql",
    "schema_key",
    # Schema exclusions
    "exclude_schema",
    "include_schema",
    "list_exclusions",
    # Exceptions
    "SciStackError",
    "NotRegisteredError",
    "NotFoundError",
    "DatabaseNotConfiguredError",
    "ReservedMetadataKeyError",
    "AmbiguousVersionError",
    "AmbiguousParamError",
    "DatabaseLockedError",
    "PipelineCycleError",
    "GlueError",
    "GlueRowsChangedError",
    "GlueSchemaKeysAlteredError",
    "GlueLanguageMismatchError",
    "GlueUnsupportedInputError",
    "GlueChainOrderError",
]
