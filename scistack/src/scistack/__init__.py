"""
scistack — the ``scistack`` CLI for SciStack scientific pipelines.

``scistack init`` (:mod:`scistack.__main__`) is a thin front end over
``scidb.project.init_project``, the one owner of creating a project.

SciStack does not manage Python environments (no uv, 2026-10-08): a project
is an ordinary package, installed with whatever tool the user prefers.
"""

# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed.
from importlib import metadata as _metadata

try:
    __version__ = _metadata.version("scistack")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"
