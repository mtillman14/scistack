"""
Discovery scanner for scistack projects.

Walks a project's ``src/{project}/`` tree and all packages listed in
``uv.lock`` (that are currently importable in the environment), and
collects the pipeline-relevant exports of each module:

* :class:`BaseVariable` subclasses (via ``issubclass`` check)
* ``@scistack``-tagged plain functions (pipeline steps)
* :class:`Constant` instances (wrapped via :func:`constant`)

The scan imports modules for real — it never parses source text — so the
objects returned are the live runtime instances the GUI can execute against.
The generic package-walking mechanics (importing, per-module error capture,
test-file exclusion, sys.path management) live in ``scifor.discovery`` —
this module only supplies the scidb-specific classifier (``discover_module``)
that knows what a BaseVariable/Constant/PathInput/Sweep/``@scistack``
function actually is.

Import failures are captured per-module as :class:`ModuleError` entries;
the scan never aborts on a single bad module.

Typical use::

    from scidb.discover import scan_project
    result = scan_project(Path("/path/to/my-study"))

    for mod in result.project_code.modules:
        print(mod.module_name, len(mod.variables), len(mod.functions))

    for lib_name, pkg in result.non_empty_libraries().items():
        print(lib_name, pkg.variable_count, pkg.function_count)
"""

from __future__ import annotations

import importlib.metadata
import logging
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    try:
        import tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

from scifor import EachOf, PathInput
from scifor.discovery import PathInsert, purge_module, read_project_name, walk_package

from .parameter import Parameter
from .pipeline import is_scistack_function
from .roles import FunctionRole, function_role
from .variable import BaseVariable

logger = logging.getLogger(__name__)



def is_parameter(obj: Any) -> bool:
    """True for a Parameter -- one value or many.

    A bare EachOf is deliberately NOT a Parameter: only a named, top-level
    declaration is GUI-visible, the same rule that applied to Sweep before
    the merge (see docs/claude/entity-editability-model.md D6).
    """
    return isinstance(obj, Parameter)


def is_path_input(obj: Any) -> bool:
    """True for a PathInput, or an EachOf whose every alternative is a
    PathInput (the "alternate templates" convention).

    The ``obj.alternatives`` test is load-bearing, not a redundant guard:
    ``all([])`` is True, so without it an EachOf with NO alternatives --
    which is exactly what a ``Parameter`` declared with no value yet is --
    would satisfy "every alternative is a PathInput" vacuously and register
    as a PathInput.
    """
    return isinstance(obj, PathInput) or (
        isinstance(obj, EachOf)
        and bool(obj.alternatives)
        and all(isinstance(alt, PathInput) for alt in obj.alternatives)
    )


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class ModuleExports:
    """Exports discovered in a single imported module."""

    module_name: str
    variables: list[type] = field(default_factory=list)
    functions: list[Any] = field(default_factory=list)
    parameters: list[tuple[str, Parameter]] = field(default_factory=list)
    path_inputs: list[tuple[str, Any]] = field(default_factory=list)

    def function_roles(self) -> dict[str, FunctionRole]:
        """``{function_name: role}`` for this module's functions.

        Stated here so consumers (the GUI sidebar's role filter) never
        re-derive a role from a name — the prefix strings belong to
        :func:`function_role` alone.
        """
        return {
            getattr(fn, "__name__", repr(fn)): function_role(
                getattr(fn, "__name__", "")
            )
            for fn in self.functions
        }

    @property
    def is_empty(self) -> bool:
        return not (
            self.variables
            or self.functions
            or self.parameters
            or self.path_inputs
        )

    @property
    def total_count(self) -> int:
        return (
            len(self.variables)
            + len(self.functions)
            + len(self.parameters)
            + len(self.path_inputs)
        )


@dataclass
class ModuleError:
    """Import failure for a single module; the scan continues past it."""

    module_name: str
    traceback: str


@dataclass
class PackageResult:
    """Result of scanning one package (project code or an installed library)."""

    name: str
    modules: list[ModuleExports] = field(default_factory=list)
    errors: list[ModuleError] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True when no scistack-relevant exports were found (ignores errors)."""
        return all(m.is_empty for m in self.modules)

    @property
    def variable_count(self) -> int:
        return sum(len(m.variables) for m in self.modules)

    @property
    def function_count(self) -> int:
        return sum(len(m.functions) for m in self.modules)

    @property
    def parameter_count(self) -> int:
        return sum(len(m.parameters) for m in self.modules)

    @property
    def path_input_count(self) -> int:
        return sum(len(m.path_inputs) for m in self.modules)


@dataclass
class DiscoveryResult:
    """Top-level result for :func:`scan_project`."""

    project_code: PackageResult
    libraries: dict[str, PackageResult] = field(default_factory=dict)

    def non_empty_libraries(self) -> dict[str, PackageResult]:
        """Libraries that contributed at least one Variable/Function/Constant."""
        return {name: pkg for name, pkg in self.libraries.items() if not pkg.is_empty}


# ---------------------------------------------------------------------------
# Per-module scanner
# ---------------------------------------------------------------------------
def discover_module(module: ModuleType) -> ModuleExports:
    """
    Scan a single already-imported module for scistack-relevant exports.

    A BaseVariable subclass or ``@scistack`` function is only attributed to
    ``module`` if it was *defined* there — re-exports (e.g. ``from .other import
    X``) are filtered out by comparing ``__module__``. This prevents the same
    object from being listed twice in the project panel.

    ``Constant`` instances don't have a ``__module__`` attribute we can
    trust, so they are attributed to the module in which the name is
    exposed. Callers that walk multiple modules should deduplicate by
    ``id()`` if that matters for their UI.
    """
    module_name = module.__name__
    exports = ModuleExports(module_name=module_name)

    for name, obj in vars(module).items():
        if name.startswith("_"):
            continue

        # --- BaseVariable subclasses ---
        if (
            isinstance(obj, type)
            and obj is not BaseVariable
            and issubclass(obj, BaseVariable)
        ):
            if getattr(obj, "__module__", None) == module_name:
                exports.variables.append(obj)
            continue

        # --- @scistack-tagged plain functions ---
        if is_scistack_function(obj):
            if getattr(obj, "__module__", None) == module_name:
                exports.functions.append(obj)
            continue

        # --- Parameter instances. Checked BEFORE PathInput: a Parameter is
        # an EachOf, and is_path_input accepts an EachOf whose alternatives
        # are all PathInputs, so a Parameter wrapping PathInputs would
        # otherwise be classified as a PathInput. ---
        if is_parameter(obj):
            exports.parameters.append((name, obj))
            continue

        # --- PathInput instances, or an EachOf of PathInputs (alternate
        # templates) ---
        if is_path_input(obj):
            exports.path_inputs.append((name, obj))
            continue

    return exports


# ---------------------------------------------------------------------------
# Package-level scanner
# ---------------------------------------------------------------------------
def scan_package(package_name: str) -> PackageResult:
    """
    Import ``package_name`` and all its submodules and run
    :func:`discover_module` on each.

    Per-module import failures are captured as :class:`ModuleError` entries
    on the returned :class:`PackageResult`; the scan continues past them.
    If the top-level package itself cannot be imported, the result contains
    a single error entry and no module exports.
    """
    wr = walk_package(package_name, discover_module)
    return PackageResult(
        name=package_name,
        modules=[exports for _modname, exports in wr.per_module],
        errors=[
            ModuleError(module_name=e.module_name, traceback=e.traceback)
            for e in wr.errors
        ],
    )


# ---------------------------------------------------------------------------
# Project-level scanner
# ---------------------------------------------------------------------------
def _read_uv_lock_packages(project_root: Path) -> list[str]:
    """Return all package names listed in ``uv.lock``.

    The project's own package and virtual dependency-group packages are
    NOT filtered out here — callers should skip by name if needed.
    """
    lock_path = project_root / "uv.lock"
    if not lock_path.exists():
        return []
    try:
        with open(lock_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.warning("Failed to parse %s", lock_path, exc_info=True)
        return []

    names: list[str] = []
    for entry in data.get("package", []):
        name = entry.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return names


def _dist_to_import_names(dist_name: str) -> list[str]:
    """Resolve a PyPI distribution name to the top-level import names it provides.

    Uses ``top_level.txt`` if available, otherwise scans the RECORD for
    top-level ``__init__.py`` files, otherwise falls back to normalizing
    the distribution name (``python-dateutil`` -> ``python_dateutil``).
    """
    try:
        dist = importlib.metadata.distribution(dist_name)
    except importlib.metadata.PackageNotFoundError:
        logger.debug("Distribution not found in env: %s", dist_name)
        return []

    # 1. top_level.txt (legacy wheels / setuptools)
    try:
        top_level = dist.read_text("top_level.txt")
    except Exception:
        top_level = None
    if top_level:
        names = [line.strip() for line in top_level.splitlines() if line.strip()]
        if names:
            return names

    # 2. Scan dist.files for top-level packages.
    import_names: set[str] = set()
    for file in dist.files or []:
        parts = file.parts
        if len(parts) >= 2 and parts[-1] == "__init__.py":
            top = parts[0]
            # Skip dist-info / data dirs.
            if top.endswith(".dist-info") or top.endswith(".data"):
                continue
            import_names.add(top)
    if import_names:
        return sorted(import_names)

    # 3. Ultimate fallback: PEP 503 normalized name with underscores.
    return [dist_name.replace("-", "_")]


def scan_project(
    project_root: Path,
    *,
    skip_dists: Iterable[str] = (),
    library_filter: Callable[[str], bool] | None = None,
) -> DiscoveryResult:
    """
    Scan a scistack project for all pipeline-relevant exports.

    Walks ``{project_root}/src/{project_name}/`` (adding ``src/`` to
    ``sys.path`` for the duration of the call) and every top-level package
    provided by each distribution listed in ``{project_root}/uv.lock``.

    Args:
        project_root: The project directory (containing ``pyproject.toml``).
        skip_dists: Optional iterable of distribution names to skip entirely
            (e.g. ``["scidb", "scistack-gui"]`` to hide framework packages
            from the library panel).
        library_filter: Optional predicate run on each distribution name;
            if it returns False the distribution is not scanned. Applied
            after ``skip_dists``.

    Returns:
        A :class:`DiscoveryResult` with ``project_code`` and ``libraries``
        populated. Import errors are captured per-module and are never
        raised out of this function.
    """
    project_root = Path(project_root).resolve()
    logger.debug("scan_project: root=%s", project_root)

    # --- Project code ---
    project_name = read_project_name(project_root)
    project_src_parent = project_root / "src"

    if project_name is None:
        logger.debug("scan_project: no project.name in pyproject.toml")
        project_result = PackageResult(name="<unknown>")
    elif not (project_src_parent / project_name).exists():
        logger.debug(
            "scan_project: src/%s does not exist under %s",
            project_name,
            project_root,
        )
        project_result = PackageResult(name=project_name)
    else:
        with PathInsert(str(project_src_parent.resolve())):
            # Ensure a stale cached import of the same package name is
            # dropped — this matters when the same package_name is used
            # across multiple test-fixture projects.
            purge_module(project_name)
            project_result = scan_package(project_name)

    # --- Libraries from uv.lock ---
    libraries: dict[str, PackageResult] = {}
    skip_set = set(skip_dists)
    if project_name is not None:
        skip_set.add(project_name)

    for dist_name in _read_uv_lock_packages(project_root):
        if dist_name in skip_set:
            continue
        if library_filter is not None and not library_filter(dist_name):
            continue

        import_names = _dist_to_import_names(dist_name)
        if not import_names:
            libraries[dist_name] = PackageResult(
                name=dist_name,
                errors=[
                    ModuleError(
                        module_name=dist_name,
                        traceback=(
                            f"Distribution {dist_name!r} from uv.lock is not "
                            f"installed in the current environment."
                        ),
                    )
                ],
            )
            continue

        # A single distribution may expose multiple top-level packages
        # (e.g. ``setuptools`` -> pkg_resources, setuptools, ...). Merge
        # all of them into one PackageResult keyed by the dist name.
        merged = PackageResult(name=dist_name)
        for import_name in import_names:
            sub = scan_package(import_name)
            merged.modules.extend(sub.modules)
            merged.errors.extend(sub.errors)
        libraries[dist_name] = merged

    return DiscoveryResult(project_code=project_result, libraries=libraries)
