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

from .database import configure_database, get_database, get_user_id
from .exceptions import (
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
    ReservedMetadataKeyError,
    SciStackError,
    UnsavedIntermediateError,
)

# Batch execution (Layer 2 — DB-backed, no lineage)
from .foreach import for_each
from .fixed import Fixed
from .merge import Merge
from .column_selection import ColumnSelection
from .foreach_config import ForEachConfig

# From scifor (Layer 1)
from scifor import Col, set_schema, get_schema, PathInput

from .variable import BaseVariable
from .filters import raw_sql

__version__ = "0.1.0"

__all__ = [
    # Core classes
    "BaseVariable",
    # Configuration
    "configure_database",
    "get_database",
    "get_user_id",
    # Batch execution
    "for_each",
    "Fixed",
    "Merge",
    "ColumnSelection",
    "ForEachConfig",
    "PathInput",
    # Standalone / DataFrame support
    "Col",
    "set_schema",
    "get_schema",
    # Filter utilities
    "raw_sql",
    # Exceptions
    "SciStackError",
    "NotRegisteredError",
    "NotFoundError",
    "DatabaseNotConfiguredError",
    "ReservedMetadataKeyError",
    "UnsavedIntermediateError",
]
