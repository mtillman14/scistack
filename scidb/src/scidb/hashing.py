"""Deterministic hashing for arbitrary Python objects.

This module re-exports hashing functionality from the scicanonicalhash package
for convenience within scidb.
"""

from scicanonicalhash import canonical_hash, frame_path_counts, generate_record_id

__all__ = ["canonical_hash", "frame_path_counts", "generate_record_id"]
