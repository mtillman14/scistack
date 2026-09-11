"""
Generic package-walking harness for code discovery.

This module knows nothing about scidb-specific concepts (``BaseVariable``,
``Constant``, ``@scistack``-tagged functions, ...) — it only knows how to
import a package and its submodules, capture per-module errors without
aborting, skip test-only modules/files by naming convention, and manage
``sys.path``/``sys.modules`` for repeated scans. Consumers (``scidb.discover``,
``scistack_gui.registry``) supply their own per-module callback to decide
what "discovery" means for them.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import importlib.util
import logging
import os
import pkgutil
import re
import sys
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Generic, TypeVar

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    try:
        import tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

T = TypeVar("T")

_PY_TEST_FILE_RE = re.compile(r"^(test_.+|.+_test)\.py$", re.IGNORECASE)
_TEST_DIR_NAMES = {"test", "tests"}


def is_test_path(path: Path | str) -> bool:
    """True if *path* belongs to a test suite by naming convention: any
    directory component (case-insensitive) is ``test``/``tests``, or the
    filename matches a Python test-file convention (``test_*.py`` /
    ``*_test.py``)."""
    p = Path(path)
    if any(part.lower() in _TEST_DIR_NAMES for part in p.parts[:-1]):
        return True
    return bool(_PY_TEST_FILE_RE.match(p.name))


def is_test_modname(modname: str) -> bool:
    """Same rule as :func:`is_test_path`, applied to a dotted module name.

    ``pkgutil.walk_packages`` only ever yields dotted names, not filesystem
    paths, so this mirrors the directory-component and filename checks
    against the name's dot-separated parts.
    """
    parts = modname.split(".")
    last = parts[-1]
    if any(part.lower() in _TEST_DIR_NAMES for part in parts[:-1]):
        return True
    if last.lower() in _TEST_DIR_NAMES:
        return True
    return bool(_PY_TEST_FILE_RE.match(last + ".py"))


def read_project_name(project_root: Path) -> str | None:
    """Return ``project.name`` from ``project_root/pyproject.toml``, or
    None if absent/unparseable."""
    pyproject = project_root / "pyproject.toml"
    if not pyproject.exists():
        return None
    try:
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.warning("Failed to parse %s", pyproject, exc_info=True)
        return None
    name = data.get("project", {}).get("name")
    return name if isinstance(name, str) else None


CONFIG_FILENAMES = ("pyproject.toml", "scistack.toml")
"""Project config files, in precedence order: ``pyproject.toml`` always
wins over ``scistack.toml`` in the same directory."""


def extract_scistack_section(data: dict, filename: str) -> dict | None:
    """The scistack config section of already-parsed TOML *data*.

    ``[tool.scistack]`` for pyproject.toml; the whole file for
    scistack.toml (an empty one parses to ``{}``, which is a valid
    all-defaults config, and is deliberately distinct from the ``None``
    returned when there is no section at all).
    """
    if filename == "scistack.toml":
        return data
    section = data.get("tool", {}).get("scistack")
    return section if isinstance(section, dict) else None


def read_scistack_section(toml_path: Path) -> dict | None:
    """Parse *toml_path* and return its scistack section, or ``None`` if it
    is unparseable or has none."""
    try:
        with open(toml_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.debug("Failed to parse %s while reading scistack config", toml_path)
        return None
    return extract_scistack_section(data, toml_path.name)


_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


def is_windows_absolute(raw: str) -> bool:
    """Whether *raw* is a Windows-absolute path (``Y:\\...`` or ``\\\\host``).

    On POSIX neither form means anything -- ``Path.is_absolute()`` says
    False, so the value gets silently joined onto the project root and
    produces nonsense like ``/Users/me/project/y:\\LabMembers\\...``. Callers
    use this to say so instead.
    """
    return bool(_WINDOWS_DRIVE_RE.match(raw)) or raw.startswith("\\\\")


def normalize_config_separators(raw: str) -> str:
    """*raw* with separators this platform understands.

    Backslash → forward slash, on POSIX only, and never for a
    Windows-absolute value (there is nothing to salvage in ``Y:\\...`` here).
    Split out from :func:`resolve_config_path` for the callers that need the
    STRING rather than a Path -- glob patterns, which are expanded as text.
    """
    if os.sep != "\\" and "\\" in raw and not is_windows_absolute(raw):
        return raw.replace("\\", "/")
    return raw


def resolve_config_path(root: Path, raw: str) -> Path:
    """Resolve *raw*, a path written in a project config, against *root*.

    Config files travel between machines -- the same repo checked out on
    Windows and macOS, a scistack.toml committed to git -- and the GUI wrote
    them with the *host's* separator. A Windows session recording
    ``entities_file = "src\\scistack_entities.toml"`` produced a value that,
    read back on macOS, names a FILE called ``src\\scistack_entities.toml``
    in the project root: ``\\`` is a legal filename character on POSIX, so
    nothing errors, the wrong file is created and read, and everything
    derived from its location moves with it (the MATLAB classdef stub
    directory lands in the project root instead of ``src/``). Backslash is
    therefore treated as a separator here whenever this is not Windows.

    The reverse direction needs nothing: Windows accepts ``/`` natively,
    which is why the writers now record ``/`` (see
    ``scistack_gui.config._portable_relpath``).

    A Windows-*absolute* value (``Y:\\LabMembers\\...``) is a different
    problem -- there is no such drive here and no sane way to guess what it
    should be -- so it is returned as-is for the caller to report as
    missing, rather than being joined onto *root*.
    """
    if os.sep != "\\" and "\\" in raw:
        if is_windows_absolute(raw):
            logger.warning(
                "Config path %r is a Windows absolute path and cannot resolve "
                "on this platform", raw
            )
            return Path(raw)
        converted = normalize_config_separators(raw)
        logger.info(
            "Config path %r was written on Windows; reading it as %r",
            raw,
            converted,
        )
        # Reading it literally, as every version before this one did, CREATES
        # things: a "src\scistack_entities.toml" in the project root that the
        # GUI then wrote declarations into. Say so, loudly and with both
        # paths -- the alternative is the user watching entities disappear
        # the moment this fix lands, with nothing in the log to explain it.
        literal = root / raw
        if literal.exists():
            logger.warning(
                "%s exists: it was created by reading the Windows path %r "
                "literally on this platform. It is NOT being read any more -- "
                "%s is. Move anything you need out of it, then delete it.",
                literal,
                raw,
                root / converted,
            )
        raw = converted
    p = Path(raw)
    return p if p.is_absolute() else root / p


def find_project_config(start: Path) -> Path | None:
    """Walk up from *start* for the nearest config file that actually
    carries a scistack section, or ``None``.

    *start* may be a file or a directory. This is the upward-search half of
    ``scistack_gui.config._locate_pyproject`` and the lookup behind
    ``scidb.entities``' project resolution -- one implementation, so the
    GUI and a plain script can never disagree about which project a path
    belongs to (CLAUDE.md NOTE 3; ``feedback_avoid_scifor_scidb_duplication``).
    """
    start = Path(os.path.abspath(str(start)))
    search_dir = start if start.is_dir() else start.parent
    while True:
        for name in CONFIG_FILENAMES:
            candidate = search_dir / name
            if candidate.exists() and read_scistack_section(candidate) is not None:
                logger.debug("Found project config %s", candidate)
                return candidate
        parent = search_dir.parent
        if parent == search_dir:
            logger.debug("No project config found in any ancestor of %s", start)
            return None
        search_dir = parent


@dataclass
class PathInsert:
    """Context manager that inserts a directory onto sys.path.

    Idempotent: does nothing on enter/exit if the directory was already on
    sys.path. Always invalidates import caches on exit so a subsequent
    import sees any newly-visible modules.
    """

    directory: str
    _inserted: bool = False

    def __enter__(self) -> "PathInsert":
        if self.directory not in sys.path:
            sys.path.insert(0, self.directory)
            self._inserted = True
        return self

    def __exit__(self, *exc) -> None:
        if self._inserted:
            try:
                sys.path.remove(self.directory)
            except ValueError:
                pass
        importlib.invalidate_caches()


@dataclass
class PathInsertAll:
    """Insert several directories onto sys.path for the duration of a block.

    Composes :class:`PathInsert` over each directory, so the same
    idempotence and restore-on-exit rules apply per entry. Directories are
    entered in the given order; on exit they unwind in reverse.

    Used when a batch of loose files is imported together and they import
    *each other* by bare module name -- no single directory suffices,
    because a file in a subdirectory may import one from its parent and
    vice versa.
    """

    directories: list[str]
    _stack: contextlib.ExitStack | None = None

    def __enter__(self) -> "PathInsertAll":
        self._stack = contextlib.ExitStack()
        for directory in self.directories:
            self._stack.enter_context(PathInsert(directory))
        return self

    def __exit__(self, *exc) -> None:
        if self._stack is not None:
            self._stack.close()
            self._stack = None


def sibling_import_dirs(paths: list[Path] | list[str]) -> list[str]:
    """Directories that must be on sys.path for *paths* to import each other.

    Returns the distinct, resolved parent directory of every path, sorted
    for determinism. Loose (non-packaged) project files are imported by
    absolute location rather than by package name, so Python has no anchor
    for a bare ``import sibling`` inside them unless the containing
    directories are on sys.path.

    The *union* is deliberate rather than each file's own parent: within one
    discovered file set, imports cross directory boundaries in both
    directions (``src/a.py`` importing ``src/sub/b.py`` and ``src/sub/c.py``
    importing ``src/a.py`` are both common), so every contributing directory
    has to be visible for the whole batch.
    """
    dirs = {str(Path(p).resolve().parent) for p in paths}
    return sorted(dirs)


BENIGN_TOPLEVEL_CALLS = frozenset(
    {
        # Backend / style selection -- the whole point is to run at import.
        # ``mpl`` is listed alongside ``matplotlib`` because the aliased
        # import is at least as common as the full name.
        "matplotlib.use",
        "mpl.use",
        "matplotlib.style.use",
        "style.use",
        "plt.style.use",
        # Logging / warnings configuration.
        "logging.basicConfig",
        "logging.captureWarnings",
        "logging.disable",
        "warnings.filterwarnings",
        "warnings.simplefilter",
        # Library-wide options.
        "pandas.set_option",
        "pd.set_option",
        "numpy.seterr",
        "np.seterr",
        "seaborn.set_theme",
        "seaborn.set_style",
        "seaborn.set_context",
        "seaborn.set_palette",
        "sns.set_theme",
        "sns.set_style",
        "sns.set_context",
        "sns.set_palette",
        "sys.setrecursionlimit",
        # Output-only: emits text and does nothing else. Refusing a file over
        # a stray debug ``print`` would cost every function it defines, and
        # the console noise is already handled -- discovery imports run inside
        # a stdout/stderr redirect that routes this to the debug log.
        "print",
        "pprint",
        "pprint.pprint",
        "stdout.write",
        "stderr.write",
    }
)
"""Top-level calls that are configuration or console output, not work -- see
:func:`find_top_level_side_effects`."""

_LOGGING_METHODS = frozenset(
    {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
)
"""Emission methods treated as output-only on any *dotted* callee, so a
module-level ``logger.info("loaded")`` doesn't get a file refused regardless of
what the logger is named (``log``, ``LOGGER``, ``_log``, ...)."""


def _is_benign_call(callee: str, allow: frozenset[str]) -> bool:
    """True if *callee* is configuration or output rather than work."""
    if callee in allow:
        return True
    parts = callee.split(".")
    if ".".join(parts[-2:]) in allow:
        return True
    # Requires a dot, so a locally-defined ``def error(...)`` invoked bare is
    # still flagged -- only attribute calls read as logger emission.
    return len(parts) > 1 and parts[-1] in _LOGGING_METHODS


@dataclass
class TopLevelSideEffect:
    """A module-level call that runs for its side effect on import."""

    lineno: int
    call: str
    """Rendered callee, e.g. ``plot_gait`` or ``fig.savefig``."""
    reason: str = "its result is discarded"

    def describe(self) -> str:
        return f"top-level call {self.call}() at line {self.lineno} ({self.reason})"


def _render_callee(node: ast.expr) -> str:
    """Render a call's callee as a dotted name (``a.b.c``), best-effort.

    Returns ``"<expr>"`` for callees that aren't a plain name/attribute chain
    (e.g. ``funcs[0]()``), which are never allowlisted.
    """
    parts: list[str] = []
    cur: ast.expr | None = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
        return ".".join(reversed(parts))
    return "<expr>"


def find_top_level_side_effects(
    source: str, *, allow: frozenset[str] = BENIGN_TOPLEVEL_CALLS
) -> list[TopLevelSideEffect]:
    """Find module-level calls that do the file's own work, in line order.

    Discovery has to *import* a file to read its definitions, which also runs
    every statement in the module body. Importing a file with module-level
    work therefore does that work: renders plots, writes files, hits the
    network.

    Two forms are reported:

    1. **A bare call** -- an ``ast.Expr`` wrapping an ``ast.Call``. The result
       is discarded, so the call exists only for what it does.
    2. **An assignment that calls a function defined in this same file** --
       ``data = plot_gait(sex, age, speed)``.

    Form 2 is why the callee, not the statement shape, is the discriminator.
    ``RATE = Parameter(1, 2, 3)`` and ``data = plot_gait(...)`` are both
    Assign-of-Call and indistinguishable by form; what separates them is that
    ``Parameter`` is *imported* while ``plot_gait`` is ``def``'d right there.
    A file that defines work and then does it is the exact shape this exists
    to catch.

    That also means entity construction needs no allowlist and keeps working
    for entity types that don't exist yet: ``Parameter``, ``PathInput``,
    ``EachOf``, ``Sweep`` and friends are imported names, so they never match.

    Never flagged:

    - ``RATE = Parameter(1, 2, 3)`` -- imported callee; executing it is the
      entire point of discovery.
    - ``logger = logging.getLogger(__name__)``, ``HERE = Path(__file__).parent``
      -- likewise imported, and assignments are *only* flagged for local
      functions precisely so these stay silent without maintenance.
    - Docstrings: ``Expr`` wrapping a ``Constant``, not a ``Call``.
    - ``if __name__ == "__main__": main()`` -- the ``If`` is the module-body
      child and only direct children are inspected, never descended into.

    ``allow`` suppresses form-1 calls that are configuration or console output
    rather than work (see :data:`BENIGN_TOPLEVEL_CALLS`); a callee matches if
    its rendered dotted name, or that name's last two segments, is in the set,
    or if it is an attribute call naming a logging method.

    Output-only calls are deliberately benign: a stray ``print`` says nothing
    about whether a file does *work*, discovery already imports inside a
    stdout/stderr redirect that routes such output to the debug log, and
    refusing over one would cost every function the file defines.

    Known gaps, all chosen for a low false-positive rate:

    - A bare top-level ``for``/``while``/``with`` executes and is not reported.
    - ``df = pd.read_csv("huge.csv")`` reads a file, but flagging *imported*
      callees in assignments would also flag every ``logging.getLogger`` and
      ``Path(...)`` in the wild -- far too noisy to be useful.
    - Locally-defined *classes* are excluded, so ``CONFIG = Config(a=1)`` for a
      local ``@dataclass`` stays silent. Instantiation is usually cheap;
      functions are where the work lives.

    Raises ``SyntaxError`` if *source* does not parse.
    """
    tree = ast.parse(source)
    local_fns = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    found: list[TopLevelSideEffect] = []

    for node in tree.body:
        # Form 1: a bare call. The result is thrown away, so the call exists
        # only for what it does.
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            callee = _render_callee(node.value.func)
            if _is_benign_call(callee, allow):
                logger.debug(
                    "Allowing benign top-level call %s at line %d", callee, node.lineno
                )
                continue
            found.append(TopLevelSideEffect(lineno=node.lineno, call=callee))

        # Form 2: an assignment that calls one of *this file's own*
        # functions. Statement form alone can't separate
        # ``RATE = Parameter(1, 2, 3)`` from ``data = plot_gait(...)`` --
        # both are Assign-of-Call -- so the callee decides.
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            if node.value is None:  # bare annotation: ``x: int``
                continue
            for sub in ast.walk(node.value):
                if not isinstance(sub, ast.Call):
                    continue
                callee = _render_callee(sub.func)
                if callee in local_fns:
                    found.append(
                        TopLevelSideEffect(
                            lineno=sub.lineno,
                            call=callee,
                            reason="calls a function defined in this file",
                        )
                    )

    # ast.walk is unordered, and a nested call (``x = f(f(1))``) can report
    # the same site twice.
    seen: set[tuple[int, str]] = set()
    unique: list[TopLevelSideEffect] = []
    for effect in found:
        key = (effect.lineno, effect.call)
        if key not in seen:
            seen.add(key)
            unique.append(effect)
    unique.sort(key=lambda e: (e.lineno, e.call))
    return unique


@contextlib.contextmanager
def headless_matplotlib():
    """Force a non-interactive matplotlib backend for the duration of a block.

    Discovery imports user code, and a plotting script that builds figures at
    module level will otherwise try to open a GUI window on whatever backend
    matplotlib picked -- slow, and on some platforms it steals focus or hangs
    a headless server.

    Sets ``MPLBACKEND=Agg`` so a matplotlib imported *during* the block picks
    Agg, and additionally switches an already-imported matplotlib, restoring
    its previous backend on exit. Never imports matplotlib itself -- projects
    that don't use it pay nothing and it stays an optional dependency.

    This does not stop ``savefig`` or other file writes; it only removes the
    interactive-backend hazard. Refusing to execute side-effecting files at all
    is :func:`find_top_level_side_effects`' job.

    Note: a matplotlib first imported *inside* the block stays on Agg
    afterward -- there was no prior backend to restore, and Agg is the correct
    steady state for a server process.
    """
    prev_env = os.environ.get("MPLBACKEND")
    os.environ["MPLBACKEND"] = "Agg"

    mpl = sys.modules.get("matplotlib")
    prev_backend: str | None = None
    if mpl is not None:
        try:
            current = mpl.get_backend()
            if current.lower() != "agg":
                mpl.use("Agg", force=True)
                prev_backend = current
                logger.debug("Switched matplotlib backend %s -> Agg", current)
        except Exception:
            logger.debug("Could not switch matplotlib backend", exc_info=True)

    try:
        yield
    finally:
        if prev_env is None:
            os.environ.pop("MPLBACKEND", None)
        else:
            os.environ["MPLBACKEND"] = prev_env
        if prev_backend is not None:
            try:
                mpl.use(prev_backend, force=True)
                logger.debug("Restored matplotlib backend -> %s", prev_backend)
            except Exception:
                logger.debug("Could not restore matplotlib backend", exc_info=True)


def purge_module(package_name: str) -> None:
    """Remove a package and all its submodules from ``sys.modules``.

    Needed so that two successive scans of projects that happen to share a
    package name don't return stale cached modules — a rescan should pick
    up any edits made since the last one.
    """
    prefix = package_name + "."
    to_drop = [
        name for name in sys.modules if name == package_name or name.startswith(prefix)
    ]
    for name in to_drop:
        sys.modules.pop(name, None)


@dataclass
class ModuleWalkError:
    """Import (or callback) failure for a single module; the walk continues."""

    module_name: str
    traceback: str


@dataclass
class WalkResult(Generic[T]):
    """Result of :func:`walk_package`."""

    package_name: str
    per_module: list[tuple[str, T]] = field(default_factory=list)
    errors: list[ModuleWalkError] = field(default_factory=list)


def walk_package(
    package_name: str, on_module: Callable[[ModuleType], T]
) -> WalkResult[T]:
    """
    Import ``package_name`` and all its submodules, calling ``on_module`` on
    each importable, non-test module.

    ``on_module`` may return a value (recorded per-module in the result) or
    mutate external state and return None — either way its return value is
    stored alongside the module's dotted name. Import failures and
    exceptions raised by ``on_module`` are both captured as
    :class:`ModuleWalkError` entries; the walk never aborts on a single bad
    module. If the top-level package itself cannot be imported, the result
    contains a single error entry and no modules.

    Callers that need output suppression around imports (e.g. a discovered
    file with stray top-level ``print()`` calls) should wrap the entire
    call in their own redirect, since that only needs to be scoped for the
    duration of this call, not per-import.
    """
    result: WalkResult[T] = WalkResult(package_name=package_name)

    try:
        pkg = importlib.import_module(package_name)
    except Exception:
        logger.debug(
            "Failed to import top-level package %s", package_name, exc_info=True
        )
        result.errors.append(
            ModuleWalkError(module_name=package_name, traceback=traceback.format_exc())
        )
        return result

    try:
        result.per_module.append((package_name, on_module(pkg)))
    except Exception:
        logger.exception("on_module failed for %s", package_name)
        result.errors.append(
            ModuleWalkError(module_name=package_name, traceback=traceback.format_exc())
        )

    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        return result

    for _importer, modname, _ispkg in pkgutil.walk_packages(
        pkg_path, prefix=package_name + "."
    ):
        if is_test_modname(modname):
            logger.debug("Skipping test module during discovery: %s", modname)
            continue

        try:
            submod = importlib.import_module(modname)
        except Exception:
            logger.debug("Failed to import submodule %s", modname, exc_info=True)
            result.errors.append(
                ModuleWalkError(module_name=modname, traceback=traceback.format_exc())
            )
            continue

        try:
            result.per_module.append((modname, on_module(submod)))
        except Exception:
            logger.exception("on_module failed for %s", modname)
            result.errors.append(
                ModuleWalkError(module_name=modname, traceback=traceback.format_exc())
            )

    return result
