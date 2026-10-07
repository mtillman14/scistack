"""Path generation utilities for data pipelines.

This package provides template-based path generation for creating file paths
organized by metadata combinations (subject, trial, session, etc.).
"""

from scipathgen.generator import PathGenerator

__all__ = ["PathGenerator"]
# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata  # noqa: E402

try:
    __version__ = _metadata.version("scipathgen")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"
