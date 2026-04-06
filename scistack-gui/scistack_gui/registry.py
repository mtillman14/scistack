"""
Function and variable class registry.

Supports two modes:
  1. **Single-file mode** (legacy): populated via --module at startup.
  2. **Project mode**: populated from a [tool.scistack] config that lists
     multiple .py files, pip packages, and auto-discovered entry-point plugins.

Gives the backend access to the actual Python objects needed to reconstruct
for_each calls.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import inspect
import logging
import pkgutil
from pathlib import Path
from typing import TYPE_CHECKING

from scidb import BaseVariable

if TYPE_CHECKING:
    from scistack_gui.config import SciStackConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_functions: dict[str, callable] = {}
_function_sources: dict[str, str] = {}  # function_name -> source description

# Single-file mode state (legacy)
_module_path: Path | None = None
_module_name: str = "user_pipeline"

# Project mode state
_config: SciStackConfig | None = None

ENTRY_POINT_GROUP = "scistack.plugins"


# ---------------------------------------------------------------------------
# Single-file mode (legacy, backward-compatible)
# ---------------------------------------------------------------------------

def register_module(module, *, module_path: Path | None = None) -> None:
    """
    Scan a user module for pipeline functions and BaseVariable subclasses.

    Functions: any top-level callable that doesn't start with '_'.
    Variable classes: all BaseVariable subclasses currently in memory
      (they self-register on definition via BaseVariable._all_subclasses).

    If module_path is provided, it is stored so that refresh_module() can
    re-import the file later without restarting the server.
    """
    global _module_path
    if module_path is not None:
        _module_path = module_path

    _scan_module_functions(module, source=str(module_path or "<unknown>"))


def refresh_module() -> dict:
    """
    Re-import the user module from disk and re-register all functions.

    Returns a summary dict with the old and new function/variable counts
    so the caller can log what changed.
    """
    if _module_path is None:
        raise RuntimeError(
            "No module was loaded at startup (--module not passed). "
            "Nothing to refresh."
        )

    old_fns = set(_functions.keys())
    old_vars = set(BaseVariable._all_subclasses.keys())

    # Clear the function registry so removed functions don't linger.
    _functions.clear()
    _function_sources.clear()

    # Re-execute the module file. This will re-define all functions and
    # BaseVariable subclasses (which auto-register via the metaclass).
    spec = importlib.util.spec_from_file_location(_module_name, _module_path)
    user_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_mod)

    _scan_module_functions(user_mod, source=str(_module_path))

    new_fns = set(_functions.keys())
    new_vars = set(BaseVariable._all_subclasses.keys())

    return _diff_summary(old_fns, new_fns, old_vars, new_vars)


# ---------------------------------------------------------------------------
# Project mode (multi-source)
# ---------------------------------------------------------------------------

def load_from_config(config: SciStackConfig) -> dict:
    """
    Load all functions and variables from a [tool.scistack] config.

    This is the project-mode equivalent of register_module(). It loads
    from three sources in order:

      1. Explicit .py module files listed in ``config.modules``
      2. Explicit pip-installed packages listed in ``config.packages``
      3. Auto-discovered ``scistack.plugins`` entry points (if enabled)

    Returns a summary dict with discovered function/variable names.
    """
    global _config
    _config = config

    old_fns = set(_functions.keys())
    old_vars = set(BaseVariable._all_subclasses.keys())

    _functions.clear()
    _function_sources.clear()

    _load_file_modules(config.modules)
    _load_packages(config.packages)
    if config.auto_discover:
        _load_entry_points()

    new_fns = set(_functions.keys())
    new_vars = set(BaseVariable._all_subclasses.keys())

    return _diff_summary(old_fns, new_fns, old_vars, new_vars)


def refresh_all() -> dict:
    """
    Re-load everything from the stored config (project mode).

    Equivalent of refresh_module() but for multi-source configs.
    """
    if _config is None:
        raise RuntimeError("No project config loaded. Nothing to refresh.")
    return load_from_config(_config)


def _load_file_modules(paths: list[Path]) -> None:
    """Import each .py file and scan for functions."""
    for i, path in enumerate(paths):
        if not path.exists():
            logger.warning("Skipping missing module: %s", path)
            continue
        mod_name = f"scistack_user_{i}_{path.stem}"
        try:
            spec = importlib.util.spec_from_file_location(mod_name, path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            _scan_module_functions(mod, source=str(path))
            logger.info("Loaded module file: %s", path)
        except Exception:
            logger.exception("Failed to load module file: %s", path)


def _load_packages(names: list[str]) -> None:
    """Import each named package and walk its submodules for functions."""
    for pkg_name in names:
        try:
            pkg = importlib.import_module(pkg_name)
        except ImportError:
            logger.exception("Failed to import package: %s", pkg_name)
            continue

        # Scan the top-level package module itself.
        _scan_module_functions(pkg, source=f"package:{pkg_name}")
        logger.info("Loaded package: %s", pkg_name)

        # Walk submodules if it's a package (has __path__).
        pkg_path = getattr(pkg, "__path__", None)
        if pkg_path is not None:
            for importer, modname, ispkg in pkgutil.walk_packages(
                pkg_path, prefix=pkg_name + "."
            ):
                try:
                    submod = importlib.import_module(modname)
                    _scan_module_functions(submod, source=f"package:{modname}")
                except Exception:
                    logger.exception("Failed to import submodule: %s", modname)


def _load_entry_points() -> None:
    """Auto-discover installed packages with scistack.plugins entry points."""
    try:
        eps = importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
    except TypeError:
        # Python 3.9/3.10 compat: entry_points() doesn't accept group kwarg
        all_eps = importlib.metadata.entry_points()
        eps = all_eps.get(ENTRY_POINT_GROUP, [])

    for ep in eps:
        try:
            mod = ep.load()
            # entry point value can be a module or a callable; if it's a
            # module we scan it, otherwise we treat it as a single function.
            if inspect.ismodule(mod):
                _scan_module_functions(mod, source=f"entrypoint:{ep.name}")
            elif callable(mod):
                _register_function(ep.name, mod, source=f"entrypoint:{ep.name}")
            logger.info("Loaded entry point: %s = %s", ep.name, ep.value)
        except Exception:
            logger.exception("Failed to load entry point: %s", ep.name)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _scan_module_functions(module, *, source: str) -> None:
    """Scan a module for top-level callables and register them."""
    for name, obj in inspect.getmembers(
        module, lambda o: callable(o) and not inspect.isclass(o)
    ):
        if not name.startswith('_'):
            _register_function(name, obj, source=source)


def _register_function(name: str, fn, *, source: str) -> None:
    """Register a single function, warning on name collisions."""
    existing_source = _function_sources.get(name)
    if existing_source is not None and existing_source != source:
        logger.warning(
            "Function '%s' from %s shadows previous definition from %s",
            name, source, existing_source,
        )
    _functions[name] = fn
    _function_sources[name] = source


def _diff_summary(
    old_fns: set[str], new_fns: set[str],
    old_vars: set[str], new_vars: set[str],
) -> dict:
    """Build a summary dict of what changed."""
    added_fns = new_fns - old_fns
    removed_fns = old_fns - new_fns
    added_vars = new_vars - old_vars

    if added_fns:
        logger.info("Added functions: %s", added_fns)
    if removed_fns:
        logger.info("Removed functions: %s", removed_fns)
    if added_vars:
        logger.info("Added variable classes: %s", added_vars)

    return {
        "functions": sorted(new_fns),
        "variables": sorted(new_vars),
        "added_functions": sorted(added_fns),
        "removed_functions": sorted(removed_fns),
        "added_variables": sorted(added_vars),
    }


# ---------------------------------------------------------------------------
# Lookup API (unchanged)
# ---------------------------------------------------------------------------

def get_function(name: str):
    fn = _functions.get(name)
    if fn is None:
        raise KeyError(
            f"Function '{name}' not found in registry. "
            f"Did you pass --module or --project with the script that defines it?"
        )
    return fn


def get_variable_class(name: str) -> type:
    cls = BaseVariable._all_subclasses.get(name)
    if cls is None:
        raise KeyError(
            f"Variable class '{name}' not found. "
            f"Did you pass --module or --project with the script that defines it?"
        )
    return cls
