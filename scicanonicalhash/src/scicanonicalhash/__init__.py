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
# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scicanonicalhash")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"
