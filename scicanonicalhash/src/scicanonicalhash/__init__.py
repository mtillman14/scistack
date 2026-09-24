"""Deterministic hashing for arbitrary Python objects.

This package provides utilities for creating stable, deterministic hashes
of Python objects, essential for cache key computation, data versioning,
and reproducibility in data pipelines.
"""

from scicanonicalhash.hashing import (
    canonical_hash,
    frame_path_counts,
    generate_record_id,
)

__all__ = ["canonical_hash", "frame_path_counts", "generate_record_id"]
__version__ = "0.1.0"
