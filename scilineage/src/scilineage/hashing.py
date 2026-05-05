"""Deterministic hashing for arbitrary Python objects.

This module re-exports the canonical hashing functionality from the
canonicalhash package for convenience, and provides function hashing utilities.
"""

import hashlib
from typing import Callable

from canonicalhash import canonical_hash

__all__ = ["canonical_hash", "compute_function_hash"]


def compute_function_hash(fn: Callable, *, truncate: int = 16) -> str:
    """Compute a stable hash of a function's bytecode and constants.

    Uses bytecode-based hashing, which only changes when the function's actual
    logic changes. Ignores:
    - Whitespace and formatting changes
    - Comments and docstrings (unless they affect bytecode)
    - Variable name changes (within the function body)

    This is preferable to source-based hashing for cache invalidation, as
    cosmetic code changes (running Black, adding comments) should not invalidate
    scientific results.

    Args:
        fn: The function to hash. Can be a regular function or a LineageFcn wrapper.
        truncate: Number of hex characters to return (default 16, sufficient entropy).

    Returns:
        Hex string hash of the function, truncated to `truncate` characters.

    Example:
        >>> def add(x, y): return x + y
        >>> hash1 = compute_function_hash(add)
        >>> # Reformatting doesn't change hash:
        >>> def add(x, y):
        ...     return x + y
        >>> hash2 = compute_function_hash(add)
        >>> hash1 == hash2
        True
    """
    # Unwrap LineageFcn if needed
    actual_fn = fn.fcn if hasattr(fn, "fcn") else fn

    try:
        # Hash bytecode + constants (stable under reformatting)
        fcn_code = actual_fn.__code__.co_code
        fcn_consts = str(actual_fn.__code__.co_consts).encode()
        combined_code = fcn_code + fcn_consts
        full_hash = hashlib.sha256(combined_code).hexdigest()
    except AttributeError:
        # Fallback for built-ins, C extensions, lambdas without __code__
        name = getattr(actual_fn, "__name__", repr(actual_fn))
        full_hash = hashlib.sha256(name.encode()).hexdigest()

    return full_hash[:truncate]
