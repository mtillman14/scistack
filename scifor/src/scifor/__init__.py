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

from .colname import ColName
from .column_selection import ColumnSelection, column_value
from .discovery import (
    BENIGN_TOPLEVEL_CALLS,
    ModuleWalkError,
    PathInsert,
    PathInsertAll,
    TopLevelSideEffect,
    WalkResult,
    find_top_level_side_effects,
    headless_matplotlib,
    is_test_modname,
    is_test_path,
    purge_module,
    read_project_name,
    sibling_import_dirs,
    walk_package,
)
from .each_of import EachOf, require_alternatives
from .filters import Col, ColFilter, CompoundFilter, NotFilter
from .fixed import Fixed
from .foreach import (
    ColumnFunctionError,
    ForColumnsError,
    NoDataError,
    for_each,
    spread_nested_results,
)
from .locations import LocationFilter, filter_combo_indices, filter_combos
from .merge import Merge
from .outputs import COLUMN_NAME_ATTR, DB_ONLY_KWARGS, output_column_name
from .pathinput import (
    PathInput,
    clear_listing_cache,
    key_regex_is_digit_only,
    key_regex_pattern_error,
    clear_project_root,
    enter_project_root,
    get_project_root,
    project_root,
    set_project_root,
)
from .pathoutput import PathOutput
from .schema import expand_schema_keys, get_schema, set_schema
from .signature import Argument, function_arguments
from .schema_types import (
    VALID_SCHEMA_KEY_TYPES,
    SchemaKeyTypeError,
    canonical_numeric_value,
)

# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scifor")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    # Schema
    "set_schema",
    "get_schema",
    "expand_schema_keys",
    # Schema-key types (numeric / string)
    "VALID_SCHEMA_KEY_TYPES",
    "SchemaKeyTypeError",
    "canonical_numeric_value",
    # Batch execution
    "for_each",
    "ColumnFunctionError",
    "ForColumnsError",
    "NoDataError",
    "spread_nested_results",
    "DB_ONLY_KWARGS",
    "output_column_name",
    "COLUMN_NAME_ATTR",
    # Input wrappers
    "Fixed",
    "Merge",
    "Argument",
    "function_arguments",
    "ColumnSelection",
    "column_value",
    "ColName",
    "EachOf",
    "require_alternatives",
    "PathInput",
    "PathOutput",
    # Location selection
    "LocationFilter",
    "filter_combos",
    "filter_combo_indices",
    # Rootless-PathInput resolution base
    "set_project_root",
    "get_project_root",
    "project_root",
    "clear_listing_cache",
    "key_regex_pattern_error",
    "key_regex_is_digit_only",
    "clear_project_root",
    "enter_project_root",
    # Filters
    "Col",
    "ColFilter",
    "CompoundFilter",
    "NotFilter",
    # Discovery
    "walk_package",
    "WalkResult",
    "ModuleWalkError",
    "is_test_path",
    "is_test_modname",
    "read_project_name",
    "PathInsert",
    "PathInsertAll",
    "sibling_import_dirs",
    "purge_module",
    "BENIGN_TOPLEVEL_CALLS",
    "TopLevelSideEffect",
    "find_top_level_side_effects",
    "headless_matplotlib",
]
