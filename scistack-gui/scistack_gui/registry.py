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

import contextlib
import importlib.metadata
import importlib.util
import inspect
import io
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from scidb import BaseVariable, EachOf, Parameter, PathInput
from scidb.discover import is_parameter, is_path_input
from scifor.discovery import (
    PathInsert,
    PathInsertAll,
    find_top_level_side_effects,
    headless_matplotlib,
    sibling_import_dirs,
    walk_package,
)

from scistack_gui import library_functions

if TYPE_CHECKING:
    from scistack_gui.config import SciStackConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_functions: dict[str, callable] = {}
_function_sources: dict[str, str] = {}  # function_name -> source description
_parameters: dict[str, Parameter] = {}
_parameter_sources: dict[str, str] = {}  # parameter_name -> source description
_path_inputs: dict[str, PathInput | EachOf] = {}
"""Top-level ``PathInput`` (or ``EachOf`` of ``PathInput``, i.e. alternate
templates) objects, keyed by the module-level name they're bound to."""
_path_input_sources: dict[str, str] = {}
_variable_sources: dict[str, str] = {}
"""Variable class name -> the source this registry registered it from.

The bookkeeping that makes ``BaseVariable._all_subclasses`` prunable. That
dict is global and append-only by construction (``__init_subclass__`` fires
on class creation and there is no matching hook for "this class went away"),
so a reload can only withdraw registrations it can *prove* it made -- hence
tracking them here, exactly as ``_function_sources``/``_parameter_sources``/
``_path_input_sources`` already do for the other three kinds.

Anything absent from this dict is left alone on reload: scidb's own types,
classes defined by a test fixture, or a variable some other importer
registered are none of this registry's business to delete.
"""
_module_paths: dict[str, str] = {}
"""Synthetic module name (``scistack_user_{i}_{stem}``, from
``_load_file_modules``) -> source file path. A BaseVariable subclass's
``__module__`` is that synthetic name, not the file path — this lets
callers (e.g. api/project.py's registry-backed discovery panel) resolve it
back to something human-readable, matching how functions are already
attributed via ``_function_sources``.
"""
_load_errors: list[dict] = []
"""Discovery failures from the most recent load/refresh — [{"source", "error"}, ...].

The DEBUG-level log line next to each of these only lands in scidb.log
(Log.set_level("DEBUG", sink="file")) — this list is what actually surfaces
a failed import to the user, queryable via ``get_load_errors``/
``api/project.py`` (📁 Paths -> Discovered Code), so a failure doesn't just
vanish with the user none the wiser about why a function didn't show up.
"""

# Single-file mode state (legacy)
_module_path: Path | None = None
_module_name: str = "user_pipeline"

# Project mode state
_config: SciStackConfig | None = None

ENTRY_POINT_GROUP = "scistack.plugins"


@contextlib.contextmanager
def _suppress_user_code_output():
    """Redirect stdout/stderr while executing discovered/imported user code.

    Discovery only wants a file's top-level definitions, but there is no
    side-effect-free way to inspect a module in Python -- importing it also
    *runs* it. If that file has real top-level code (a stray debug script,
    a missing ``if __name__ == "__main__":`` guard, ...), its print()s and
    any console-sink log lines it triggers (scidb/scifor route through
    scistacklog's console handler, which resolves ``sys.stderr`` dynamically
    at emit time -- see scistacklog._StderrHandler -- so they're caught by
    this redirect too) would otherwise land in the GUI's own terminal and
    read as if the GUI itself had broken.

    Captured text is logged at DEBUG rather than discarded, so it's still
    recoverable via ``scidb.log`` (``Log.set_level("DEBUG", sink="file")``)
    if a user's script misbehaves and someone needs to see why.

    Best-effort: redirect_stdout/stderr swap process-wide streams, so a
    concurrent request producing console output during the same brief
    window would have it captured too. Discovery runs are short and
    infrequent (startup, explicit "Refresh Code"), so this is an accepted
    tradeoff rather than a correctness concern here.
    """
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            yield
    finally:
        if out.getvalue():
            logger.debug(
                "[registry] captured stdout during import:\n%s", out.getvalue()
            )
        if err.getvalue():
            logger.debug(
                "[registry] captured stderr during import:\n%s", err.getvalue()
            )


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
    logger.info("[registry] Registering module from %s", module_path or "<unknown>")
    global _module_path
    if module_path is not None:
        _module_path = module_path
        logger.debug("[registry] Stored module path for refresh: %s", module_path)

    _scan_module_functions(module, source=str(module_path or "<unknown>"))
    _scan_module_parameters(module, source=str(module_path or "<unknown>"))
    _scan_module_path_inputs(module, source=str(module_path or "<unknown>"))
    _scan_module_variables(module, source=str(module_path or "<unknown>"))
    logger.info(
        "[registry] Module registration complete - %d functions registered",
        len(_functions),
    )


def refresh_module() -> dict:
    """
    Re-import the user module from disk and re-register all functions.

    Returns a summary dict with the old and new function/variable counts
    so the caller can log what changed.
    """
    logger.info("[registry] Starting module refresh from %s", _module_path)
    if _module_path is None:
        raise RuntimeError(
            "No module was loaded at startup (--module not passed). Nothing to refresh."
        )

    old_fns = set(_functions.keys())
    old_vars = set(BaseVariable._all_subclasses.keys())
    logger.debug(
        "[registry] Before refresh: %d functions, %d variables",
        len(old_fns),
        len(old_vars),
    )

    # Clear the function registry so removed functions don't linger.
    logger.info("[registry] Clearing function registry")
    _functions.clear()
    _function_sources.clear()
    _parameters.clear()
    _parameter_sources.clear()
    _path_inputs.clear()
    _path_input_sources.clear()
    _load_errors.clear()
    _unregister_tracked_variables()

    # Re-execute the module file. This will re-define all functions and
    # BaseVariable subclasses (which auto-register via the metaclass).
    logger.info("[registry] Re-importing module from %s", _module_path)
    spec = importlib.util.spec_from_file_location(_module_name, _module_path)
    user_mod = importlib.util.module_from_spec(spec)
    with _suppress_user_code_output():
        spec.loader.exec_module(user_mod)

    logger.info("[registry] Scanning module for functions")
    _scan_module_functions(user_mod, source=str(_module_path))
    _scan_module_parameters(user_mod, source=str(_module_path))
    _scan_module_path_inputs(user_mod, source=str(_module_path))
    _scan_module_variables(user_mod, source=str(_module_path))

    new_fns = set(_functions.keys())
    new_vars = set(BaseVariable._all_subclasses.keys())
    logger.debug(
        "[registry] After refresh: %d functions, %d variables",
        len(new_fns),
        len(new_vars),
    )

    logger.info("[registry] Module refresh complete")
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
    logger.info("[registry] Loading from config at %s", config.project_root)
    global _config
    _config = config

    old_fns = set(_functions.keys())
    old_vars = set(BaseVariable._all_subclasses.keys())
    logger.debug(
        "[registry] Before load: %d functions, %d variables",
        len(old_fns),
        len(old_vars),
    )

    logger.info("[registry] Clearing function registry")
    _functions.clear()
    _function_sources.clear()
    _parameters.clear()
    _parameter_sources.clear()
    _path_inputs.clear()
    _path_input_sources.clear()
    _module_paths.clear()
    _load_errors.clear()
    # Variables are cleared the same way everything else here is. They were
    # the one registry that never was, so a declaration deleted from disk
    # stayed live until the server restarted -- see _variable_sources.
    _unregister_tracked_variables()

    logger.info("[registry] Loading %d file modules", len(config.modules))
    _load_file_modules(config.modules)

    logger.info("[registry] Loading %d packages", len(config.packages))
    src_dir = config.project_root / "src"
    if src_dir.is_dir():
        # A packaged project's own code (auto-folded into config.packages
        # by config.load_config) is only importable by name if src/ is on
        # sys.path -- mirrors scidb.discover.scan_project's precondition.
        # Idempotent/no-op for projects already pip install -e'd.
        with PathInsert(str(src_dir.resolve())):
            _load_packages(config.packages)
    else:
        _load_packages(config.packages)

    if config.auto_discover:
        logger.info("[registry] Auto-discovering entry points")
        _load_entry_points()
    else:
        logger.info("[registry] Skipping entry point discovery (disabled)")

    # Last, so a TOML declaration wins over a same-named one discovered in a
    # module: the entities file is the file the GUI writes, so if the two
    # disagree, what the user just edited is what they should see.
    if config.entities_file is not None:
        _load_entities_file(config.entities_file)
    else:
        # Logged, not silent: "my entities file's declarations don't show up
        # in the GUI" is answered by this line, and used to have no record at
        # all -- config.load_config missed scidb's conventional-path fallback,
        # so a real file on disk landed here as None. See
        # .claude/plan-preexisting-entities-on-db-create-26-09-10.md.
        logger.info(
            "[registry] No entities file configured for %s; nothing to load "
            "from one", config.project_root,
        )

    new_fns = set(_functions.keys())
    new_vars = set(BaseVariable._all_subclasses.keys())
    logger.debug(
        "[registry] After load: %d functions, %d variables", len(new_fns), len(new_vars)
    )

    # Baseline for the entities file's stale-write guard: the GUI is about to
    # display values read from this exact content, so an edit is only safe
    # while the file still matches it (target_file_service.update_declaration).
    from scistack_gui.services.target_file_service import record_source_hash

    record_source_hash(config.entities_file)

    logger.info("[registry] Config loading complete")
    return _diff_summary(old_fns, new_fns, old_vars, new_vars)


def refresh_all() -> dict:
    """
    Re-load everything from the stored config (project mode).

    Equivalent of refresh_module() but for multi-source configs.
    """
    logger.info("[registry] Starting refresh_all")
    if _config is None:
        raise RuntimeError("No project config loaded. Nothing to refresh.")
    return load_from_config(_config)


def _load_file_modules(paths: list[Path]) -> None:
    """Import each .py file and scan for functions.

    Loose files are loaded by location (``spec_from_file_location``) rather
    than by package name, so nothing anchors a bare ``import sibling``
    inside them. Every directory contributing a file is placed on sys.path
    for the duration of the batch -- see ``sibling_import_dirs`` for why the
    union is needed rather than each file's own parent.
    """
    logger.debug("[registry] Importing %d module files", len(paths))
    search_dirs = sibling_import_dirs(paths)
    logger.info(
        "[registry] Adding %d source directory(ies) to sys.path for sibling "
        "imports: %s",
        len(search_dirs),
        search_dirs,
    )
    with PathInsertAll(search_dirs), headless_matplotlib():
        _exec_file_modules(paths)


def _screen_for_side_effects(path: Path) -> str | None:
    """Return a load-error message if *path* must not be imported, else None.

    Importing is how discovery reads a file's definitions, so a file with
    module-level work (an unguarded plotting script dropped in a
    folder-scanned tree) would do that work during a scan. One real project
    spent 13 of its 15 startup seconds rendering figures this way before
    dying on a hardcoded output path. Refuse rather than execute.

    A file that doesn't parse is also refused here: importing it would only
    raise ``SyntaxError`` anyway, so there's nothing to gain by trying.
    """
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        return f"Could not read file: {e}"

    try:
        effects = find_top_level_side_effects(source)
    except SyntaxError as e:
        return f"SyntaxError: {e}"

    if not effects:
        return None

    detail = ", ".join(e.describe() for e in effects)
    return (
        f"Skipped: {detail} would execute on import. Wrap it in "
        '`if __name__ == "__main__":` to make this file discoverable.'
    )


def _exec_file_modules(paths: list[Path]) -> None:
    """Import/scan each file. Assumes sys.path is already prepared."""
    for i, path in enumerate(paths):
        logger.debug("[registry] Processing module %d/%d: %s", i + 1, len(paths), path)
        if not path.exists():
            logger.warning("[registry] Skipping missing module: %s", path)
            _record_load_error(str(path), "File does not exist")
            continue

        refusal = _screen_for_side_effects(path)
        if refusal is not None:
            # INFO, not WARNING: a stray script sitting in a folder-scanned
            # tree is routine and must not read as a GUI failure on the
            # console -- same reasoning as the failed-import path below. The
            # Paths -> Discovered Code panel is where this surfaces to users.
            logger.info("[registry] Refusing to import %s -- %s", path, refusal)
            _record_load_error(str(path), refusal)
            continue

        mod_name = f"scistack_user_{i}_{path.stem}"
        _module_paths[mod_name] = str(path)
        try:
            spec = importlib.util.spec_from_file_location(mod_name, path)
            mod = importlib.util.module_from_spec(spec)
            with _suppress_user_code_output():
                spec.loader.exec_module(mod)
            fn_count_before = len(_functions)
            _scan_module_functions(mod, source=str(path))
            fn_count_after = len(_functions)
            _scan_module_parameters(mod, source=str(path))
            _scan_module_path_inputs(mod, source=str(path))
            _scan_module_variables(mod, source=str(path))
            logger.info(
                "[registry] Loaded module file: %s (%d functions)",
                path,
                fn_count_after - fn_count_before,
            )
        except Exception as e:
            # DEBUG, not ERROR: a failed import during discovery is routine
            # (framework/example/debug files sitting in a folder-scanned
            # tree were never meant to be pipeline code) and must not read
            # as a GUI failure on the console. Still fully recorded via
            # _record_load_error for the 📁 Paths -> Discovered Code panel,
            # and the traceback is recoverable via scidb.log at DEBUG.
            logger.debug(
                "[registry] Failed to load module file: %s", path, exc_info=True
            )
            _record_load_error(str(path), str(e))


def _load_packages(names: list[str]) -> None:
    """Import each named package and walk its submodules for functions.

    Walking mechanics (pkgutil, import-error capture, test-module exclusion)
    are shared with scidb.discover.scan_package via scifor.discovery.walk_package;
    this function only supplies the "what to do with a successfully imported
    module" callback (mutate the flat registry dicts, unlike scidb.discover's
    pure per-module scanner).
    """
    logger.debug("[registry] Importing %d packages", len(names))
    for pkg_idx, pkg_name in enumerate(names):
        logger.debug(
            "[registry] Processing package %d/%d: %s", pkg_idx + 1, len(names), pkg_name
        )

        def on_module(mod) -> None:
            source = f"package:{mod.__name__}"
            _scan_module_functions(mod, source=source)
            _scan_module_parameters(mod, source=source)
            _scan_module_path_inputs(mod, source=source)
            _scan_module_variables(mod, source=source)

        fn_count_before = len(_functions)
        with _suppress_user_code_output(), headless_matplotlib():
            wr = walk_package(pkg_name, on_module)

        if not wr.per_module and wr.errors and wr.errors[0].module_name == pkg_name:
            logger.debug(
                "[registry] Failed to import package: %s\n%s",
                pkg_name,
                wr.errors[0].traceback,
            )
            _record_load_error(f"package:{pkg_name}", wr.errors[0].traceback)
            continue

        for e in wr.errors:
            logger.debug(
                "[registry] Failed to import submodule: %s\n%s",
                e.module_name,
                e.traceback,
            )
            _record_load_error(f"package:{e.module_name}", e.traceback)

        fn_count_after = len(_functions)
        logger.info(
            "[registry] Loaded package: %s (%d functions total, %d submodule(s) walked)",
            pkg_name,
            fn_count_after - fn_count_before,
            len(wr.per_module) - 1,
        )


def _load_entry_points() -> None:
    """Auto-discover installed packages with scistack.plugins entry points."""
    logger.debug("[registry] Discovering entry points in group: %s", ENTRY_POINT_GROUP)
    try:
        eps = importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
    except TypeError:
        # Python 3.9/3.10 compat: entry_points() doesn't accept group kwarg
        all_eps = importlib.metadata.entry_points()
        eps = all_eps.get(ENTRY_POINT_GROUP, [])

    eps_list = list(eps)
    logger.debug("[registry] Found %d entry points", len(eps_list))
    for ep_idx, ep in enumerate(eps_list):
        logger.debug(
            "[registry] Processing entry point %d/%d: %s",
            ep_idx + 1,
            len(eps_list),
            ep.name,
        )
        try:
            with _suppress_user_code_output():
                mod = ep.load()
            # entry point value can be a module or a callable; if it's a
            # module we scan it, otherwise we treat it as a single function.
            if inspect.ismodule(mod):
                fn_count_before = len(_functions)
                _scan_module_functions(mod, source=f"entrypoint:{ep.name}")
                fn_count_after = len(_functions)
                _scan_module_parameters(mod, source=f"entrypoint:{ep.name}")
                _scan_module_path_inputs(mod, source=f"entrypoint:{ep.name}")
                _scan_module_variables(mod, source=f"entrypoint:{ep.name}")
                logger.info(
                    "[registry] Loaded entry point: %s = %s (%d functions)",
                    ep.name,
                    ep.value,
                    fn_count_after - fn_count_before,
                )
            elif callable(mod):
                _register_function(ep.name, mod, source=f"entrypoint:{ep.name}")
                logger.info(
                    "[registry] Loaded entry point: %s = %s (1 function)",
                    ep.name,
                    ep.value,
                )
        except Exception as e:
            logger.debug(
                "[registry] Failed to load entry point: %s", ep.name, exc_info=True
            )
            _record_load_error(f"entrypoint:{ep.name}", str(e))


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _record_load_error(source: str, error: str) -> None:
    entry = {"source": source, "error": error}
    _load_errors.append(entry)
    logger.debug("[registry] Recorded load error: %s", entry)


def get_load_errors() -> list[dict]:
    """Return discovery failures from the most recent load/refresh."""
    return list(_load_errors)


def resolve_module_source(module_name: str) -> str:
    """Map a (possibly synthetic, file-import-only) module name back to a
    human-readable source: the file path for file-imported modules, or the
    module name itself for real importable modules (packages/entry
    points already have a meaningful ``__module__``)."""
    return _module_paths.get(module_name, module_name)


def _scan_module_functions(module, *, source: str) -> None:
    """Scan a module for top-level callables and register them.

    Only callables actually *defined* in ``module`` are registered — objects
    merely imported into its namespace (e.g. a pipeline file doing
    ``from scidb import for_each, configure_database``) are skipped by
    comparing ``__module__``. Without this, internal scistack helpers
    re-exported into a user file leak into the GUI's discovered-functions
    list as if they were pipeline steps. Mirrors the same filter already
    used by :func:`scidb.discover.discover_module`.
    """
    module_name = getattr(module, "__name__", None)
    logger.debug("[registry] Scanning module for functions: %s", source)
    discovered = []
    skipped_reexports = []
    for name, obj in inspect.getmembers(
        module, lambda o: callable(o) and not inspect.isclass(o)
    ):
        if name.startswith("_"):
            continue
        if getattr(obj, "__module__", None) != module_name:
            skipped_reexports.append(name)
            continue
        _register_function(name, obj, source=source)
        discovered.append(name)
    if discovered:
        logger.debug(
            "[registry] Discovered %d functions from %s: %s",
            len(discovered),
            source,
            discovered,
        )
    if skipped_reexports:
        logger.debug(
            "[registry] Skipped %d imported/re-exported callables from %s (not defined there): %s",
            len(skipped_reexports),
            source,
            skipped_reexports,
        )


def _register_variable(name: str, *, source: str) -> None:
    """Record that *this* registry registered variable *name* from *source*.

    The class itself is already in ``BaseVariable._all_subclasses`` by the
    time this is called -- ``__init_subclass__`` put it there when the class
    statement executed. This only records the attribution that lets
    :func:`_unregister_tracked_variables` withdraw it later.
    """
    previous = _variable_sources.get(name)
    if previous is not None and previous != source:
        logger.warning(
            "[registry] Variable '%s' is declared in more than one place: %s "
            "and %s. The last one scanned wins; remove one to make which is "
            "used deterministic.",
            name,
            previous,
            source,
        )
    _variable_sources[name] = source


def _unregister_tracked_variables(sources: "set[str] | None" = None) -> list[str]:
    """Withdraw variables this registry registered. Returns the names dropped.

    With *sources*, only variables attributed to those exact sources go --
    that is what makes a narrow single-file reload possible. Without it,
    every tracked variable goes, which is what a full reload wants before it
    rebuilds from scratch.
    """
    from scidb import BaseVariable

    doomed = [
        name
        for name, src in _variable_sources.items()
        if sources is None or src in sources
    ]
    for name in doomed:
        BaseVariable.unregister(name)
        del _variable_sources[name]
    if doomed:
        logger.info(
            "[registry] Unregistered %d variable(s) before reload: %s",
            len(doomed),
            ", ".join(sorted(doomed)),
        )
    return doomed


def _scan_module_variables(module, *, source: str) -> None:
    """Attribute a module's BaseVariable subclasses to it.

    Registration itself already happened at import (``__init_subclass__``);
    this records *which file* each one came from. Filtered by ``__module__``
    for the same reason :func:`_scan_module_functions` is: a pipeline file
    doing ``from scidb import BaseVariable`` or importing a sibling's
    variable class would otherwise claim a type it does not declare, and
    then a reload of that file would unregister someone else's variable.
    """
    from scidb import BaseVariable

    module_name = getattr(module, "__name__", None)
    discovered = []
    for name, cls in vars(module).items():
        if name.startswith("_") or not inspect.isclass(cls):
            continue
        if not issubclass(cls, BaseVariable) or cls is BaseVariable:
            continue
        if getattr(cls, "__module__", None) != module_name:
            continue
        _register_variable(cls.__name__, source=source)
        discovered.append(cls.__name__)
    if discovered:
        logger.debug(
            "[registry] Discovered %d variable(s) from %s: %s",
            len(discovered),
            source,
            discovered,
        )


def _prune_sources(sources: set[str]) -> None:
    """Drop every entity this registry attributed to *sources*.

    The shared "forget one file" step behind both the full reload and
    :func:`reload_entities_file`. Load errors recorded against those sources
    go too, otherwise a fixed file keeps showing its old error forever.

    Known narrow gap: when two sources declare the same name, the last one
    scanned wins (see :func:`_register_parameter`'s shadowing warning). If
    the winner was the entities file and the entry is *deleted* from it, the
    shadowed declaration from the other source does not come back until a
    full reload -- pruning knows what to remove, but only a re-scan of that
    other file could restore it. Refresh Code fixes it, the collision is
    already warned about, and re-importing every module to cover it would
    reintroduce exactly the cost this function exists to avoid.
    """
    for registry_dict, source_map in (
        (_parameters, _parameter_sources),
        (_path_inputs, _path_input_sources),
        (_functions, _function_sources),
    ):
        for name in [n for n, s in source_map.items() if s in sources]:
            registry_dict.pop(name, None)
            source_map.pop(name, None)
    _unregister_tracked_variables(sources)

    # Entities-file errors are recorded as "{path}:{line}" (see
    # _load_entities_file), so match on the path prefix rather than splitting
    # on ":" -- a Windows path is "Y:\..." and splitting would reduce every
    # source to its drive letter and drop unrelated errors wholesale.
    def _from_pruned_source(entry: dict) -> bool:
        recorded = str(entry.get("source", ""))
        return any(
            recorded == src or recorded.startswith(f"{src}:") for src in sources
        )

    _load_errors[:] = [e for e in _load_errors if not _from_pruned_source(e)]


def reload_entities_file() -> "str | None":
    """Re-read *only* the TOML entities file. Returns an error, or ``None``.

    The narrow counterpart to :func:`refresh_all`, for the one case that
    dominates GUI writes: the user created or edited an entity, so the only
    file whose contents can have changed is the one the GUI just wrote.

    A full reload re-imports every configured module and re-parses every
    MATLAB source to learn the same thing. On a real project that is
    ~16.5 s (2.5 s config + 1.6 s Python modules + 14.9 s for 303 MATLAB
    sources, measured 2026-09-01); this is a single TOML parse. Creating a
    variable used to pay the former.

    Deliberately does NOT re-read scistack.toml: nothing here can change
    *which* files are configured. A change to that is
    ``services.registry_reload_service.reload_registries_from_disk``, and a
    change to a module's contents is the Refresh Code button.
    """
    if _config is None or _config.entities_file is None:
        return "No entities file is configured for this project."

    path = _config.entities_file
    before = set(_variable_sources)
    _prune_sources({str(path)})
    try:
        _load_entities_file(path)
    except Exception as e:
        logger.exception("[registry] Narrow entities reload failed for %s", path)
        return f"Entities file was written but re-reading it failed: {e}"

    from scistack_gui.services.target_file_service import record_source_hash

    record_source_hash(path)

    added = sorted(set(_variable_sources) - before)
    removed = sorted(before - set(_variable_sources))
    logger.info(
        "[registry] Narrow entities reload of %s: +%d variable(s) %s, "
        "-%d variable(s) %s (no modules re-imported, no MATLAB sources re-parsed)",
        path,
        len(added),
        added or "[]",
        len(removed),
        removed or "[]",
    )
    return None


def _load_entities_file(path: Path) -> None:
    """Register everything the TOML entities file declares.

    The read half of ``scidb.entities`` -- the format's owner does the
    parsing and construction, this only registers the results and records
    per-entry failures the way a failed module load is recorded, so a bad
    declaration shows up in the GUI's load-errors panel instead of
    disappearing.

    Variables need no registration call: constructing the class registers
    it in ``BaseVariable._all_subclasses`` via ``__init_subclass__``, which
    is the same registry a ``class X(BaseVariable)`` statement lands in.
    Their *source* is recorded here, though -- that is what lets a later
    reload withdraw them again (:func:`_unregister_tracked_variables`).
    """
    from scidb.entities import load

    logger.info("[registry] Loading entities file: %s", path)
    result = load(path)

    for name, param in result.parameters.items():
        _register_parameter(name, param, source=str(path))
    for name, pi in result.path_inputs.items():
        _register_path_input(name, pi, source=str(path))
    for name in result.variables:
        _register_variable(name, source=str(path))

    for err in result.errors:
        _record_load_error(f"{path}:{err.line}" if err.line else str(path), err.describe())

    logger.info(
        "[registry] Entities file gave %d variable(s), %d parameter(s), "
        "%d path input(s), %d rejected",
        len(result.variables),
        len(result.parameters),
        len(result.path_inputs),
        len(result.errors),
    )


def _scan_module_parameters(module, *, source: str) -> None:
    """Scan a module for ``scidb.Parameter`` instances and register them.

    Unlike functions/variable classes, a ``Parameter`` is attributed to
    wherever its name is *exposed*, not filtered by ``__module__`` —
    ``Parameter`` doesn't reliably expose one (unknown attribute access
    proxies through to the wrapped value via ``__getattr__``, so
    ``getattr(const, "__module__", None)`` would silently return the
    *wrapped value's* ``__module__`` if it happens to have one, which is
    meaningless here). This mirrors the same documented tradeoff already
    made by :func:`scidb.discover.discover_module` — a Parameter imported
    into multiple scanned modules can appear attributed to more than one
    of them, on purpose.
    """
    logger.debug("[registry] Scanning module for parameters: %s", source)
    discovered = []
    for name, obj in vars(module).items():
        if name.startswith("_"):
            continue
        if is_parameter(obj):
            _register_parameter(name, obj, source=source)
            discovered.append(name)
    if discovered:
        logger.debug(
            "[registry] Discovered %d parameter(s) from %s: %s",
            len(discovered),
            source,
            discovered,
        )


def _register_parameter(name: str, param: Parameter, *, source: str) -> None:
    """Register a single discovered Parameter, warning on name collisions."""
    existing_source = _parameter_sources.get(name)
    if existing_source is not None and existing_source != source:
        logger.warning(
            "[registry] Parameter '%s' from %s shadows previous definition from %s",
            name,
            source,
            existing_source,
        )
    _parameters[name] = param
    _parameter_sources[name] = source
    logger.debug("[registry] Registered parameter: %s from %s", name, source)


def get_parameters_registry() -> dict[str, Parameter]:
    """Return all discovered ``scidb.Parameter`` values, keyed by name.

    Used by ``api/project.py``'s registry-backed "Discovered Code" panel
    for loose-script/folder-scan projects — the equivalent of what
    ``scidb.discover.scan_project`` already provides for packaged projects.
    """
    return dict(_parameters)


def _scan_module_path_inputs(module, *, source: str) -> None:
    """Scan a module for top-level ``scifor.PathInput`` objects (or an
    ``EachOf`` whose every alternative is a ``PathInput`` — the "alternate
    templates" case) and register them.

    Mirrors :func:`_scan_module_parameters` exactly, including the
    no-``__module__``-filtering rationale: neither ``PathInput`` nor
    ``EachOf`` reliably expose one. See
    ``docs/claude/code-discovery-categories.md`` — this is the *only* way a
    ``PathInput`` becomes visible in the GUI now (no DB-history fallback).
    """
    logger.debug("[registry] Scanning module for path inputs: %s", source)
    discovered = []
    for name, obj in vars(module).items():
        if name.startswith("_"):
            continue
        if is_parameter(obj):
            # A Parameter IS an EachOf, and is_path_input accepts an EachOf
            # whose alternatives are all PathInputs -- so disambiguate before
            # the PathInput check below or a Parameter wrapping PathInputs
            # would register as both.
            continue
        if is_path_input(obj):
            _register_path_input(name, obj, source=source)
            discovered.append(name)
    if discovered:
        logger.debug(
            "[registry] Discovered %d path input(s) from %s: %s",
            len(discovered),
            source,
            discovered,
        )


def _register_path_input(name: str, pi: "PathInput | EachOf", *, source: str) -> None:
    """Register a single discovered PathInput, warning on name collisions."""
    existing_source = _path_input_sources.get(name)
    if existing_source is not None and existing_source != source:
        logger.warning(
            "[registry] PathInput '%s' from %s shadows previous definition from %s",
            name,
            source,
            existing_source,
        )
    _path_inputs[name] = pi
    _path_input_sources[name] = source
    logger.debug("[registry] Registered path input: %s from %s", name, source)


def get_path_inputs_registry() -> dict[str, "PathInput | EachOf"]:
    """Return all discovered top-level PathInput objects, keyed by name."""
    return dict(_path_inputs)


def get_path_input(name: str) -> "PathInput | EachOf | None":
    """Look up one discovered PathInput by name, or ``None``."""
    return _path_inputs.get(name)


def get_project_root() -> "Path | None":
    """The loaded project's root directory, or ``None`` before any config is
    loaded. One accessor so callers that only need the root (the generated
    MATLAB command's resolution pin, ``graph_builder``'s project-rooted
    PathInput matching) don't each reach into ``_config``."""
    return getattr(_config, "project_root", None)


def _source_tier(source: str, project_root: "Path | None") -> str:
    """``"project"``, ``"external"``, or ``"unknown"`` for a definition source.

    Only an absolute filesystem path can be placed relative to the project.
    Everything else -- ``package:pkg.mod``, ``entrypoint:name``,
    ``<unknown>``, a builtin with no backing file -- is ``"unknown"``, and
    deliberately so: a packaged project's OWN code is scanned as
    ``package:...`` (config.load_config folds ``src/{name}/`` into
    ``config.packages``), so treating an unplaceable source as external would
    let a real library outrank the project's own function.
    """
    if project_root is None:
        return "unknown"
    candidate = Path(source)
    if not candidate.is_absolute():
        return "unknown"
    try:
        if candidate.is_relative_to(project_root):
            return "project"
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return "unknown"

    # A mapped drive and its UNC target are the same directory spelled two
    # ways (and a symlinked project dir is the POSIX equivalent), so the
    # containment check above can miss a file that IS in the project --
    # config._same_path exists for exactly this class of bug. Canonicalize
    # both and retry. Resolving is wrong for *storing* a path, which is why
    # config._normalize preserves spelling and matlab_registry stores paths
    # as-is; it is exactly right for *comparing* two.
    try:
        real_candidate = Path(os.path.realpath(candidate))
        real_root = Path(os.path.realpath(project_root))
    except OSError:  # pragma: no cover - defensive
        return "external"
    return "project" if real_candidate.is_relative_to(real_root) else "external"


def resolve_definition_shadowing(
    name: str,
    *,
    incoming: str,
    existing: "str | None",
    project_root: "Path | None",
    kind: str = "Function",
) -> bool:
    """Whether *incoming* should replace *existing* as the definition of *name*.

    Precedence is stated, not accidental: **a definition inside the project
    beats one outside it**, in either scan order. Before this, registration
    was an unconditional overwrite, so a shared code library that happened to
    be walked after the project silently won -- editing your own copy of the
    function then did nothing, and the only trace was one WARN line.

    Same-tier collisions (two project files, or two libraries) keep the old
    last-one-wins behaviour, because there the choice really is arbitrary --
    that is the case worth a warning. Shared by the Python and MATLAB
    registries so the rule cannot drift between them.
    """
    if existing is None or existing == incoming:
        return True

    incoming_tier = _source_tier(incoming, project_root)
    existing_tier = _source_tier(existing, project_root)

    if incoming_tier == "external" and existing_tier == "project":
        logger.info(
            "[registry] %s '%s' from %s ignored -- the project's own definition "
            "at %s takes precedence",
            kind,
            name,
            incoming,
            existing,
        )
        return False
    if incoming_tier == "project" and existing_tier == "external":
        logger.info(
            "[registry] %s '%s' from %s takes precedence over the definition "
            "outside the project at %s",
            kind,
            name,
            incoming,
            existing,
        )
        return True

    logger.warning(
        "[registry] %s '%s' from %s shadows previous definition from %s "
        "(neither is more specific to the project; the last one scanned wins)",
        kind,
        name,
        incoming,
        existing,
    )
    return True


def _register_function(name: str, fn, *, source: str) -> None:
    """Register a single function, applying project-over-library precedence."""
    if not resolve_definition_shadowing(
        name,
        incoming=source,
        existing=_function_sources.get(name),
        project_root=get_project_root(),
    ):
        return
    _functions[name] = fn
    _function_sources[name] = source
    logger.debug("[registry] Registered function: %s from %s", name, source)


def _diff_summary(
    old_fns: set[str],
    new_fns: set[str],
    old_vars: set[str],
    new_vars: set[str],
) -> dict:
    """Build a summary dict of what changed."""
    added_fns = new_fns - old_fns
    removed_fns = old_fns - new_fns
    added_vars = new_vars - old_vars

    logger.info(
        "[registry] Registry summary: %d functions, %d variables",
        len(new_fns),
        len(new_vars),
    )
    if added_fns:
        logger.info("[registry] Added functions: %s", sorted(added_fns))
    if removed_fns:
        logger.info("[registry] Removed functions: %s", sorted(removed_fns))
    if added_vars:
        logger.info("[registry] Added variable classes: %s", sorted(added_vars))

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


def lookup_function(name: str):
    """Resolve *name* to a callable, or return ``None``.

    Two sources, in order:

    1. ``_functions`` — functions discovered by scanning the user's source.
    2. :func:`scistack_gui.library_functions.resolve` — a library reference
       (``pandas.read_csv``, ``numpy.mean``, a stdlib call), imported on
       demand.

    Library functions are deliberately never stored in ``_functions``: this
    dict is cleared by every refresh, and the replay that used to restore
    them was missing from several refresh paths, so a Parameter edit or a
    Variable creation silently evicted them (see library_functions'
    module docstring). Importing on demand has no such failure mode.
    """
    fn = _functions.get(name)
    if fn is not None:
        return fn
    if not library_functions.is_library_reference(name):
        return None
    fn = library_functions.resolve(name)
    if fn is not None:
        logger.debug("[registry] Resolved '%s' as a library function by import", name)
    return fn


def get_function(name: str):
    fn = lookup_function(name)
    if fn is None:
        raise KeyError(
            f"Function '{name}' not found in registry, and is not an importable "
            f"library function (numpy/pandas/stdlib). "
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
