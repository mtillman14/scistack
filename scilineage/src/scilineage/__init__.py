"""SciLineage: Lineage Tracking for Python.

A lightweight library for building data processing pipelines with automatic
provenance tracking.

Features:
- Full lineage tracking for reproducibility
- Automatic input capture and output wrapping
- Lightweight (core dependency: canonicalhash)

Example:
    from scilineage import lineage_fcn

    @lineage_fcn
    def process(data, factor):
        return data * factor

    result = process(input_data, 2.5)  # Returns LineageFcnResult
    print(result.data)  # The computed value
    print(result.invoked.inputs)  # Captured inputs for provenance

For multi-output functions, use unpack_output=True:

    @lineage_fcn(unpack_output=True)
    def split(data):
        return data[:len(data)//2], data[len(data)//2:]

    first_half, second_half = split(my_data)  # Each is a LineageFcnResult
"""

from .backend import configure_backend, _clear_backend
from .core import (
    LineageFcnResult,
    LineageFcnInvocation,
    LineageFcn,
    lineage_fcn,
    manual,
    make_tuple_unpacking_wrapper,
)
from .hashing import canonical_hash
from .inputs import InputKind, ClassifiedInput, classify_input, is_trackable_variable
from .lineage import (
    LineageRecord,
    extract_lineage,
    get_raw_value,
    get_upstream_lineage,
)

__version__ = "0.1.0"

__all__ = [
    # Backend registry
    "configure_backend",
    # Core classes
    "LineageFcn",
    "LineageFcnInvocation",
    "LineageFcnResult",
    # Decorator
    "lineage_fcn",
    # Manual intervention
    "manual",
    # Tuple unpacking
    "make_tuple_unpacking_wrapper",
    # Input classification
    "InputKind",
    "ClassifiedInput",
    "classify_input",
    "is_trackable_variable",
    # Lineage
    "LineageRecord",
    "extract_lineage",
    "get_raw_value",
    "get_upstream_lineage",
    # Hashing
    "canonical_hash",
]
