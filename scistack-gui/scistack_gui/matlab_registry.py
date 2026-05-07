"""
MATLAB function and variable registry.

Mirrors the role of :mod:`scistack_gui.registry` for Python code, but for
MATLAB .m files declared in ``[tool.scistack.matlab]``.

Module-level state tracks discovered MATLAB functions and variables.
On load, Python surrogate classes are created for each MATLAB variable via
:func:`sci_matlab.bridge.register_matlab_variable` so they participate in
the DAG graph (which is built from DB history that references these types).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from scistack_gui.matlab_parser import (
    MatlabFunctionInfo,
    parse_matlab_function,
    parse_matlab_variable,
)

if TYPE_CHECKING:
    from scistack_gui.config import SciStackConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_matlab_functions: dict[str, MatlabFunctionInfo] = {}
"""function_name -> parsed info."""

_matlab_variables: dict[str, Path] = {}
"""variable_class_name -> .m file path."""

_config: SciStackConfig | None = None
"""Stored config for refresh_all()."""


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_from_config(config: SciStackConfig) -> dict:
    """Scan configured MATLAB paths, parse .m files, populate registries.

    Also creates Python surrogate classes for each MATLAB variable so the
    DAG graph builder can reference them.

    Returns a summary dict.
    """
    logger.info("[matlab_registry] Step 1: Loading MATLAB config")
    global _config
    _config = config

    logger.info("[matlab_registry] Step 2: Clearing registries")
    _matlab_functions.clear()
    _matlab_variables.clear()

    # --- Function files ---
    logger.info("[matlab_registry] Step 3: Parsing %d MATLAB function files", len(config.matlab_functions))
    for idx, path in enumerate(config.matlab_functions):
        logger.debug("[matlab_registry] Parsing function file %d/%d: %s", idx + 1, len(config.matlab_functions), path)
        info = parse_matlab_function(path)
        if info is not None:
            if info.name in _matlab_functions:
                logger.warning(
                    "[matlab_registry] MATLAB function '%s' from %s shadows previous definition from %s",
                    info.name, path, _matlab_functions[info.name].file_path,
                )
            _matlab_functions[info.name] = info
            logger.info("[matlab_registry] Registered MATLAB function: %s (%s)", info.name, path)
        else:
            logger.warning("[matlab_registry] Could not parse MATLAB function from %s", path)

    # --- Variable classdef files ---
    logger.info("[matlab_registry] Step 4: Parsing %d MATLAB variable files", len(config.matlab_variables))
    for idx, path in enumerate(config.matlab_variables):
        logger.debug("[matlab_registry] Parsing variable file %d/%d: %s", idx + 1, len(config.matlab_variables), path)
        var_name = parse_matlab_variable(path)
        if var_name is not None:
            # Store the path as-is (already absolute & normalized by
            # config._normalize). Calling .resolve() here would undo that
            # by canonicalizing mapped drives → UNC on Windows.
            _matlab_variables[var_name] = path
            # Create a Python surrogate so BaseVariable._all_subclasses
            # contains this type and the DAG builder can reference it.
            logger.debug("[matlab_registry] Creating Python surrogate for MATLAB variable: %s", var_name)
            try:
                from sci_matlab.bridge import register_matlab_variable
                register_matlab_variable(var_name)
                logger.info("[matlab_registry] Registered MATLAB variable: %s (%s)", var_name, path)
            except Exception:
                logger.exception(
                    "[matlab_registry] Failed to create surrogate for MATLAB variable '%s'", var_name
                )
        else:
            logger.warning("[matlab_registry] Could not parse MATLAB variable classdef from %s", path)

    logger.info("[matlab_registry] Step 5: MATLAB registry loading complete - %d functions, %d variables",
                len(_matlab_functions), len(_matlab_variables))
    return {
        "matlab_functions": sorted(_matlab_functions.keys()),
        "matlab_variables": sorted(_matlab_variables.keys()),
    }


def refresh_all() -> dict:
    """Re-scan all configured MATLAB paths."""
    logger.info("[matlab_registry] Starting refresh_all")
    if _config is None:
        logger.warning("[matlab_registry] No MATLAB config loaded; nothing to refresh.")
        return {"matlab_functions": [], "matlab_variables": []}
    return load_from_config(_config)


# ---------------------------------------------------------------------------
# Lookup API
# ---------------------------------------------------------------------------

def get_matlab_function(name: str) -> MatlabFunctionInfo:
    """Return info for a registered MATLAB function, or raise KeyError."""
    info = _matlab_functions.get(name)
    if info is None:
        raise KeyError(f"MATLAB function '{name}' not found in registry.")
    return info


def is_matlab_function(name: str) -> bool:
    """Return True if *name* is a registered MATLAB function."""
    return name in _matlab_functions


def get_all_function_names() -> list[str]:
    """Return sorted list of all registered MATLAB function names."""
    return sorted(_matlab_functions.keys())


def get_all_variable_names() -> list[str]:
    """Return sorted list of all registered MATLAB variable names."""
    return sorted(_matlab_variables.keys())


def get_mismatched_function_names() -> list[str]:
    """Return sorted list of MATLAB function names where the function name
    does not match the stem of its .m file (a MATLAB requirement)."""
    mismatched = [
        name
        for name, info in _matlab_functions.items()
        if info.file_path.stem != name
    ]
    return sorted(mismatched)


def has_matlab_config() -> bool:
    """Return True if a MATLAB config section was loaded."""
    return _config is not None and bool(
        _config.matlab_functions or _config.matlab_variables
    )
