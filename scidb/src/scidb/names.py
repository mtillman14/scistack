"""
THE recorded name of a pipeline function.

A function from a SciStack LIBRARY the project uses is named
``<package>.<function>`` (``preprocessing.filter_emg``); the project's own
code keeps its bare name. Decided 2026-10-08 (``docs/claude/portability.md``,
"Reusing code"): two libraries can then each have a ``filter`` and the canvas
and the run history say which one ran.

**Why it lives here.** The name is recorded at run time
(``_invocation.function_name``, the call site, node state lookups). If only
the GUI qualified it, a script calling ``for_each(preprocessing.filter_emg,
...)`` would record ``filter_emg`` while the GUI recorded
``preprocessing.filter_emg`` -- the same function under two names, the
failure ``library-function-name-identity.md`` documents for library
functions. So every recording site and the GUI registry ask
:func:`function_name`.

**Which packages are libraries -- opt-in, never guessed.** A package is a
library when the project lists it under ``packages`` in ``scistack.toml`` or
it advertises a ``scistack.plugins`` entry point, and it is not the
project's own package (``scifor.discovery.own_package_dir``). "Anything
installed" would be wrong: stubs and wrappers built inside SciStack's own
packages (a MATLAB proxy, ``state.py``'s stub) carry the user function's
name and SciStack's module.

**A library the project copied keeps its names** (``copied_libraries`` in
``scistack.toml``, portability Stage 10c, D-2026-10-08-11): its source now
lives in the project's own package at ``<pkg>/<lib>/``, and a function there
is still recorded as ``<lib>.fn``, so its history stays current.

A name that already contains a dot is already qualified (the GUI's
library-function wrapper, ``library_functions.with_qualified_name``) and is
returned as it is.
"""

from __future__ import annotations

import importlib.metadata
import inspect
import time

from scifor.discovery import own_package_dir, read_scistack_section
from scifor.pathinput import project_root
from scistacklog import Log

from .schema_order import locate_config

ENTRY_POINT_GROUP = "scistack.plugins"

#: Seconds the library set is cached: it is read on every recorded run, and
#: the config read and entry-point scan are not free.
LIBRARY_TTL = 5.0

_cache: dict[str, tuple[float, frozenset[str]]] = {}
_copied_cache: dict[str, tuple[float, "tuple[str | None, frozenset[str]]"]] = {}

#: The scistack.toml key listing libraries copied into the project's package.
COPIED_LIBRARIES_KEY = "copied_libraries"


def clear_cache() -> None:
    """Forget the cached library set (tests, a project switch)."""
    _cache.clear()
    _copied_cache.clear()


def _entry_point_packages() -> set[str]:
    try:
        eps = importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
    except TypeError:  # pragma: no cover - Python < 3.10 API
        eps = importlib.metadata.entry_points().get(ENTRY_POINT_GROUP, [])
    return {ep.value.split(":")[0].split(".")[0] for ep in eps}


def library_packages() -> frozenset[str]:
    """Top-level import names of the libraries THIS project uses."""
    root = project_root()
    key = str(root)
    now = time.monotonic()
    hit = _cache.get(key)
    if hit is not None and now - hit[0] < LIBRARY_TTL:
        return hit[1]

    packages: set[str] = set()
    config = locate_config()
    if config is not None:
        listed = (read_scistack_section(config) or {}).get("packages") or []
        packages |= {str(p).split(".")[0] for p in listed if isinstance(p, str) and p}
    try:
        packages |= _entry_point_packages()
    except Exception as e:  # never let naming fail a run
        Log.warn(f"[names] could not read {ENTRY_POINT_GROUP} entry points: {e}")
    own = own_package_dir(root)
    if own is not None:
        packages.discard(own[0])

    result = frozenset(packages)
    if hit is None or hit[1] != result:
        Log.info(f"[names] library packages for {root}: {sorted(result) or 'none'}")
    _cache[key] = (now, result)
    return result


def copied_libraries() -> "tuple[str | None, frozenset[str]]":
    """``(own package, libraries copied into it)`` for THIS project."""
    root = project_root()
    key = str(root)
    now = time.monotonic()
    hit = _copied_cache.get(key)
    if hit is not None and now - hit[0] < LIBRARY_TTL:
        return hit[1]
    copied: set[str] = set()
    config = locate_config()
    if config is not None:
        listed = (read_scistack_section(config) or {}).get(COPIED_LIBRARIES_KEY) or []
        copied |= {str(p) for p in listed if isinstance(p, str) and p}
    own = own_package_dir(root)
    result = (own[0] if own else None, frozenset(copied))
    if copied and (hit is None or hit[1] != result):
        Log.info(f"[names] copied libraries in {result[0]}: {sorted(copied)}")
    _copied_cache[key] = (now, result)
    return result


def library_of(fn) -> "str | None":
    """The library *fn* comes from, or ``None`` for the project's own code
    (and anything that is not a library function)."""
    try:
        inner = inspect.unwrap(fn)
    except ValueError:  # pragma: no cover - a cyclic __wrapped__
        inner = fn
    module = getattr(inner, "__module__", None) or ""
    parts = module.split(".")
    top = parts[0]
    if top and top in library_packages():
        return top
    if len(parts) > 1:
        own, copied = copied_libraries()
        if own is not None and top == own and parts[1] in copied:
            return parts[1]
    return None


def function_name(fn) -> str:
    """THE name a run of *fn* is recorded under, and the GUI shows."""
    inner = getattr(fn, "fcn", fn)  # a legacy wrapper's payload
    name = getattr(inner, "__name__", None) or getattr(fn, "__name__", None)
    if not name:
        return repr(fn)
    if "." in name:
        return name
    library = library_of(inner)
    return f"{library}.{name}" if library else name
