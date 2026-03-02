"""
SciDB: Scientific Data Versioning Framework

A lightweight database framework for scientific computing that provides:
- Type-safe serialization of numpy arrays, DataFrames, and custom types
- Automatic content-based versioning
- Flexible metadata-based addressing
- DuckDB storage for data and lineage (via SciDuck)
- Automatic lineage tracking via thunks

Example:
    from scidb import configure_database, BaseVariable, thunk
    import numpy as np

    # Setup (single DuckDB file for data + lineage, auto-registers types)
    db = configure_database("experiment.duckdb", ["subject", "session"])

    class RawSignal(BaseVariable):
        schema_version = 1

    @thunk
    def calibrate(signal, factor):
        return signal * factor

    # Save/load
    RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
    raw = RawSignal.load(subject=1, session="A")

    # Thunk caching works automatically
    result = calibrate(raw, 2.5)
    CalibratedSignal.save(result, subject=1, session="A")
"""

from .database import configure_database, get_database, get_user_id
from .exceptions import (
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
    ReservedMetadataKeyError,
    SciDBError,
    UnsavedIntermediateError,
)

# Re-export from scirun (for_each) and scifor (wrappers)
from scirun import for_each
from scifor import Fixed, Merge, ColumnSelection, PathInput, Col, MatFile, CsvFile, set_schema, get_schema

from .thunk import ThunkOutput, Thunk, thunk
from .variable import BaseVariable
from .filters import raw_sql

__version__ = "0.1.0"

__all__ = [
    # Core classes
    "BaseVariable",
    # Configuration
    "configure_database",
    "get_database",
    # Batch execution
    "for_each",
    "Fixed",
    "Merge",
    "ColumnSelection",
    "PathInput",
    # Standalone / DataFrame support
    "Col",
    "MatFile",
    "CsvFile",
    "set_schema",
    "get_schema",
    # Thunk system
    "thunk",
    "Thunk",
    "ThunkOutput",
    # Filter utilities
    "raw_sql",
    # Exceptions
    "SciDBError",
    "NotRegisteredError",
    "NotFoundError",
    "DatabaseNotConfiguredError",
    "ReservedMetadataKeyError",
    "UnsavedIntermediateError",
]
