"""
Discovery scanner for scistack projects.

Walks a project's ``src/{project}/`` tree and every package listed under
``packages`` in its ``scistack.toml``, and collects the pipeline-relevant
exports of each module:

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

import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

from scifor import EachOf, PathInput
from scifor.discovery import PathInsert, purge_module, read_project_name, walk_package

from .parameter import Parameter, path_input_declaration
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
    unbound: dict[str, str] = {}  # binding -> PathInput name it does not match

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
            # The binding name IS the declared name; stamp it on the object
            # so a script's for_each records it (declared_input_names).
            # First binding wins: `B = A` re-exports A, it does not rename it.
            if not getattr(obj, "name", None):
                obj.name = name
            exports.parameters.append((name, obj))
            continue

        # --- PathInput instances, or an EachOf of PathInputs (alternate
        # templates) ---
        if is_path_input(obj):
            # The declared name is the object's own `name=` (its identity).
            # Exported only under a binding that matches it; any other binding
            # is a re-export or a mismatch (checked after the loop).
            declared, problem = path_input_declaration(obj, name)
            if problem:
                logger.warning(f"[discover] {module_name}: {problem}")
            elif declared == name:
                exports.path_inputs.append((name, obj))
            else:
                unbound[name] = declared
            continue

    exported = {n for n, _ in exports.path_inputs}
    for binding, declared in unbound.items():
        if declared in exported:
            logger.debug(f"[discover] {module_name}: {binding} re-exports PathInput {declared!r}")
        else:
            logger.warning(
                f"[discover] {module_name}: {binding} is a PathInput named {declared!r} "
                f"but nothing binds it as {declared} — declare it as "
                f"{declared} = PathInput(..., name={declared!r}) so the name and "
                f"the declaration agree"
            )
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
def _configured_packages(project_root: Path) -> list[str]:
    """The ``packages`` the project's ``scistack.toml`` lists (import names),
    or ``[]``. The same list the GUI registry loads, so the two never
    disagree about which libraries a project uses. Never a lockfile: SciStack
    does not depend on uv (2026-10-08)."""
    from scifor.discovery import project_config_at, read_scistack_section

    config = project_config_at(project_root)
    if config is None:
        return []
    packages = (read_scistack_section(config) or {}).get("packages", [])
    if not isinstance(packages, list):
        logger.warning("%s: packages must be a list; ignoring %r", config, packages)
        return []
    return [p for p in packages if isinstance(p, str) and p]


def scan_project(project_root: Path) -> DiscoveryResult:
    """
    Scan a scistack project for all pipeline-relevant exports.

    Walks ``{project_root}/src/{project_name}/`` (adding ``src/`` to
    ``sys.path`` for the duration of the call; the name is
    ``pyproject.toml``'s ``[project].name``, packaging metadata) and every
    package listed under ``packages`` in the project's ``scistack.toml``.

    Args:
        project_root: The project directory.

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

    # --- Libraries: scistack.toml `packages` ---
    libraries: dict[str, PackageResult] = {}
    for name in _configured_packages(project_root):
        if name == project_name:
            continue
        libraries[name] = scan_package(name)
    logger.info(
        "scan_project(%s): project %s, %d configured library package(s)",
        project_root,
        project_name,
        len(libraries),
    )

    return DiscoveryResult(project_code=project_result, libraries=libraries)
