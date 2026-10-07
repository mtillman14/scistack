# The release tag owns the version (hatch-vcs writes it into the installed
# metadata); "0.0.0" marks a source tree that was never installed. The VS Code
# extension is stamped from the same tag and compares against this value.
from importlib import metadata as _metadata

try:
    __version__ = _metadata.version("scistack-gui")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"
