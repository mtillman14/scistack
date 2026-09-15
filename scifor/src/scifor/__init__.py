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
from .column_selection import ColumnSelection
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
from .foreach import ColumnFunctionError, ForColumnsError, NoDataError, for_each
from .locations import LocationFilter, filter_combos
from .merge import Merge
from .pathinput import (
    PathInput,
    clear_project_root,
    get_project_root,
    set_project_root,
)
from .pathoutput import PathOutput
from .schema import expand_schema_keys, get_schema, set_schema

__version__ = "0.1.0"

__all__ = [
    # Schema
    "set_schema",
    "get_schema",
    "expand_schema_keys",
    # Batch execution
    "for_each",
    "ColumnFunctionError",
    "ForColumnsError",
    "NoDataError",
    # Input wrappers
    "Fixed",
    "Merge",
    "ColumnSelection",
    "ColName",
    "EachOf",
    "require_alternatives",
    "PathInput",
    "PathOutput",
    # Location selection
    "LocationFilter",
    "filter_combos",
    # Rootless-PathInput resolution base
    "set_project_root",
    "get_project_root",
    "clear_project_root",
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
