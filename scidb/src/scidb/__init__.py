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

Public surface:
    ``scidb`` (this module) is the user API and ``scidb.inspect`` the read
    API. Other SciStack packages may also import the internal modules listed
    in ``SUPPORTED_MODULES`` below, and nothing else. Each consumer package
    has a boundary test that enforces this. The internal half of the list is
    what consumers use today; it is meant to shrink, not grow.
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

from scidb.model.across_variants import AcrossVariants
from scidb.artifact_stamp import read_artifact_stamp, stamp_artifact
from scidb.model.column_selection import ColumnSelection
from scidb.model.parameter import Parameter
from scidb.store.database import configure_database, get_database, using_db
from scidb.project.discover import (
    DiscoveryResult,
    ModuleError,
    ModuleExports,
    PackageResult,
    discover_module,
    scan_package,
    scan_project,
)
from scidb.model.roles import FUNCTION_ROLES, function_role
from scidb.model.exceptions import (
    AmbiguousParamError,
    AmbiguousVersionError,
    ColumnSetChangedError,
    ColumnTypeChangedError,
    DatabaseLockedError,
    DatabaseNotConfiguredError,
    DuplicateLocationError,
    NotFoundError,
    NotRegisteredError,
    OutputSaveError,
    PipelineCycleError,
    ReservedMetadataKeyError,
    SchemaKeyTypeChangeError,
    SchemaKeyTypeConflictError,
    SchemaKeyTypeError,
    SciStackError,
)
from scidb.store.exclusions import exclude_schema, include_schema, list_exclusions
from scidb.model.filters import raw_sql, schema_key

# Batch execution (Layer 2 — DB-backed, no lineage)
from scidb.run.foreach import for_each
from scidb.run.foreach_config import ForEachConfig
from scidb.run.glue import (
    GlueChainOrderError,
    GlueError,
    GlueLanguageMismatchError,
    GlueRowsChangedError,
    GlueSchemaKeysAlteredError,
    GlueSpec,
    GlueUnsupportedInputError,
)
from scidb.model.lineage_save import save
from scidb.model.log import Log
from scidb.model.merge import Merge
from scidb.run.pipeline import Pipeline, PipelineBinding, Step, active_pipeline, scistack
from scidb.store.rename import RenameReport, rename_functions
from scidb.provenance.state import (
    check_combo_state,
    check_multiple_nodes_state,
    check_node_state,
    check_pathinput_node_state,
)
from scidb.model.variable import BaseVariable
from scidb.model.variant import Variant, branch_param

# Imported last, and as a module rather than its contents: `entities` builds
# BaseVariable subclasses and Parameters, so it must come after both exist,
# and `from scidb import entities; entities.WINDOW` needs the module object
# itself -- attribute access on it is what resolves a declared name.
from scidb.project import entities  # noqa: E402
from scidb.provenance import intent  # noqa: E402

# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scistack-db")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

# THE list of scidb modules other packages may import (see the docstring).
# Read by every consumer's test_scidb_boundary.py through tools/import_layers.py.
SUPPORTED_MODULES: tuple[str, ...] = (
    # Public API
    "scidb",
    "scidb.inspect",
    # Internals consumers import today; remove an entry once nothing needs it
    "scidb.model.across_variants",
    "scidb.run.bindings",
    "scidb.store.clear_data",
    "scidb.model.column_selection",
    "scidb.store.database",
    "scidb.model.exceptions",
    "scidb.store.exclusions",
    "scidb.run.external_loop",
    "scidb.model.filters",
    "scidb.run.foreach",
    "scidb.run.foreach_config",
    "scidb.run.glue",
    "scidb.inspect.cli",
    "scidb.inspect.mutate",
    "scidb.inspect.variant_cards",
    "scidb.provenance.intent",
    # parse_values_text: the one owner of how a typed list of expected key
    # values is read, shared by `scidb expect` (inspect.cli) and the GUI's
    # expectation popup (expectation_service), so both accept the same text.
    "scidb.provenance.expectation",
    "scidb.provenance.inventory",
    "scidb.provenance.locations",
    "scidb.model.log",
    "scidb.model.parameter",
    "scidb.run.per_combo",
    "scidb.run.pipeline",
    "scidb.provenance.graph",
    "scidb.provenance.query",
    "scidb.model.roles",
    "scidb.run_markers",
    "scidb.run.schema_level",
    "scidb.provenance.state",
    "scidb.store.tables",
    "scidb.model.variable",
    "scidb.model.variant",
    "scidb.store.variant_pins",
    "scidb.store.versioned_store",
    "scidb.provenance.wirings",
    # Project runtime, model-bound (the model-free half is scifor.project)
    "scidb.project.bundle",
    "scidb.project.discover",
    "scidb.project.entities",
    "scidb.project.environment",
    "scidb.project.init",
    "scidb.project.library",
    "scidb.project.schema_map",
    "scidb.project.table_copy",
    "scidb.project.verify",
)

__all__ = [
    "RenameReport",
    "rename_functions",
    # Core classes
    "BaseVariable",
    "Parameter",
    # Entities file (TOML) -- the writable declaration surface
    "entities",
    # Intent vs fact: statements about runs, resolved against provenance
    "intent",
    "schema_order",
    "aliases",
    "colors",
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
    "using_db",
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
    "OutputSaveError",
    "ColumnSetChangedError",
    "ColumnTypeChangedError",
    "DuplicateLocationError",
    "NotRegisteredError",
    "NotFoundError",
    "DatabaseNotConfiguredError",
    "ReservedMetadataKeyError",
    "AmbiguousVersionError",
    "AmbiguousParamError",
    "DatabaseLockedError",
    "PipelineCycleError",
    "SchemaKeyTypeError",
    "SchemaKeyTypeConflictError",
    "SchemaKeyTypeChangeError",
    "GlueError",
    "GlueRowsChangedError",
    "GlueSchemaKeysAlteredError",
    "GlueLanguageMismatchError",
    "GlueUnsupportedInputError",
    "GlueChainOrderError",
]
