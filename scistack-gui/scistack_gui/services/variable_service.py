"""
Variable service — single source of truth for variable creation.

Used by both server.py's JSON-RPC handler (VS Code extension) and
api/variables.py's FastAPI route, so validation/target-file/MATLAB-fallback
behavior can't drift between the two transports.
"""

from __future__ import annotations

import keyword
import logging

logger = logging.getLogger(__name__)


def create_variable(
    name: str, docstring: str | None = None, language: str = "python"
) -> dict:
    """Validate, write a new BaseVariable subclass, and refresh the registry.

    Works for both Python and MATLAB variables.

    Returns:
        {"ok": True, "name": name} on success,
        {"ok": False, "error": message} on failure.
    """
    from scidb import BaseVariable
    from scistack_gui import matlab_registry, registry

    name = name.strip()
    logger.debug("create_variable: name=%r, language=%r", name, language)

    # --- Validation ---
    if not name or not name.isidentifier() or keyword.iskeyword(name):
        return {"ok": False, "error": f"'{name}' is not a valid class name."}
    if name.startswith("_"):
        return {
            "ok": False,
            "error": "Variable names must not start with an underscore.",
        }
    if name in BaseVariable._all_subclasses:
        # Always say WHERE. This message used to be the bare "already
        # exists", which was unfalsifiable from the user's side when the
        # registry held a name nothing on disk declared -- the reason
        # BaseVariable.unregister and registry._variable_sources now exist.
        # Naming the source makes a stale registration visibly wrong instead
        # of just baffling.
        source = registry._variable_sources.get(name)
        where = f" It is declared in {source}." if source else (
            " Nothing in this project declares it, so it is a stale "
            "registration — hit 🔄 Refresh Code and try again."
        )
        logger.info(
            "[variable_service] Refusing to create '%s': already registered "
            "(source=%s)",
            name,
            source or "<untracked>",
        )
        return {
            "ok": False,
            "error": f"A variable named '{name}' already exists.{where}",
        }

    # MATLAB variable creation. When the project has a TOML entities file,
    # the declaration goes THERE and the classdef is materialized from it --
    # one declaration, in the language-neutral file, plus the stub MATLAB
    # needs to reference the type. Without one (a MATLAB-only project that
    # never configured an entities file), the classdef is still the
    # declaration, as before.
    if language == "matlab":
        return _create_matlab_variable(name, docstring)

    # Python variable creation.
    from scistack_gui.services.target_file_service import (
        ensure_scidb_import,
        get_or_create_target_file,
        is_toml_target,
        write_variable,
    )

    target_file, target_err = get_or_create_target_file()

    if target_file is None:
        # No Python target — fall back to MATLAB if configured.
        if matlab_registry.has_matlab_config() and matlab_registry._config is not None:
            # No longer requires a configured variable_dir: the classdef
            # destination falls back to scimatlab.stubs.variable_stub_dir.
            return _create_matlab_variable(name, docstring)
        return {"ok": False, "error": target_err}

    if is_toml_target(target_file):
        if docstring:
            # A value-less list has nowhere to put one (plan D4). Logged
            # rather than dropped silently.
            logger.warning(
                "[variable_service] Dropping docstring %r for %r: the TOML "
                "entities file declares variables as bare names; declare the "
                "class in Python if you need a docstring",
                docstring,
                name,
            )
        logger.info(
            "[variable_service] create_variable: name=%r target=%s", name, target_file
        )
        err = write_variable(target_file, name)
        if err:
            return err
        return {"ok": True, "name": name}

    lines = ["\n"]
    if docstring:
        escaped = docstring.replace('"""', '\\"\\"\\"')
        lines.append(
            f'class {name}(scidb.BaseVariable):\n    """{escaped}"""\n    pass\n'
        )
    else:
        lines.append(f"class {name}(scidb.BaseVariable):\n    pass\n")

    try:
        ensure_scidb_import(target_file)
        with open(target_file, "a") as f:
            f.writelines(lines)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to module file: {e}"}

    # Narrow by default: the append above went into one file, so re-reading
    # that file is the whole reload. Never refresh_all() here -- see
    # target_file_service._reload_after_write for the dispatch and the cost.
    from scistack_gui.services.target_file_service import _reload_after_write

    error = _reload_after_write(target_file)
    if error is not None:
        return {"ok": False, "error": error}

    return {"ok": True, "name": name}


def _create_matlab_variable(name: str, docstring: str | None = None) -> dict:
    """Create a MATLAB classdef variable file and register the surrogate.

    With a TOML entities file configured, the *declaration* is written there
    first and the classdef becomes a materialized stub of it -- so the same
    variable is visible to Python without being declared twice. The classdef
    is still written here rather than left to the next scan, because the
    user expects the file to exist the moment they hit create.
    """
    from scistack_gui import matlab_registry
    from scistack_gui.services.target_file_service import (
        get_or_create_target_file,
        is_toml_target,
        write_variable,
    )

    if matlab_registry._config is None:
        return {
            "ok": False,
            "error": "No MATLAB configuration loaded for this project.",
        }

    entities_file, _ = get_or_create_target_file()
    if is_toml_target(entities_file):
        err = write_variable(entities_file, name)
        if err:
            return err
        logger.info(
            "[variable_service] Declared MATLAB variable %r in the entities "
            "file; materializing its classdef",
            name,
        )

    # An explicit [matlab] variable_dir wins; otherwise the classdef goes
    # where scimatlab materializes stubs, which is the directory
    # +scidb/entities.m adds to the MATLAB path at run time. Falling back
    # rather than refusing is the point: a project can declare its
    # variables in the entities file and never configure a variable_dir.
    target_dir = matlab_registry._config.matlab_variable_dir
    if target_dir is None:
        from scimatlab.stubs import variable_stub_dir

        target_dir = variable_stub_dir(matlab_registry._config.project_root)
    if target_dir is None:
        return {
            "ok": False,
            "error": (
                "Nowhere to write the classdef: configure "
                "[tool.scistack.matlab] variable_dir, or an entities_file "
                "for its default location."
            ),
        }
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{name}.m"

    if target_file.exists():
        return {"ok": False, "error": f"File already exists: {target_file}"}

    # scimatlab owns the stub's text, so a GUI-created classdef and one
    # materialized from the entities file at run time are byte-identical
    # (CLAUDE.md NOTE 3). A docstring is appended as a comment because the
    # shared text has nowhere to carry one.
    from scimatlab.stubs import classdef_text, invalid_name_reason

    reason = invalid_name_reason(name)
    if reason is not None:
        return {
            "ok": False,
            "error": f"'{name}' cannot be a MATLAB class: {reason}.",
        }

    body = classdef_text(name)
    if docstring:
        body = body.replace("end\n", f"    % {docstring}\nend\n")

    try:
        target_file.write_text(body, encoding="utf-8")
    except OSError as e:
        return {"ok": False, "error": f"Failed to write .m file: {e}"}

    # Overwriting an existing .m could reuse the (mtime_ns, size) a previous
    # scan cached this path under, which would make a LATER refresh_all()
    # serve the pre-write classification. Registration below is direct, so
    # this only guards that future scan -- see matlab_parser's cache.
    from scistack_gui.matlab_parser import invalidate_classify_cache

    invalidate_classify_cache(target_file)

    # Register just this one file. The full matlab_registry.refresh_all()
    # this used to call re-classified every configured .m source (14.9 s for
    # 303 files on a real project) to learn about a file whose path we are
    # holding -- see target_file_service._reload_after_write.
    try:
        matlab_registry._register_matlab_variable(name, target_file)
    except Exception as e:
        return {"ok": False, "error": f"File written but registration failed: {e}"}

    return {"ok": True, "name": name}


def get_variable_records(variable_name: str, db) -> dict:
    """Return records and variant summary for a variable type.

    Delegates to the query logic in api/variables.py.
    """
    from scistack_gui.api.variables import get_variable_records as _get_var_records

    return _get_var_records(variable_name, db)


def get_variable_plot_data(variable_name: str, db) -> dict:
    """Raw points for the sidebar's default plot (to-do #4).

    Delegates to the query logic in api/variables.py.
    """
    from scistack_gui.api.variables import get_variable_plot_data as _get_plot_data

    return _get_plot_data(variable_name, db)


def input_columns(variable_type: str, db=None) -> dict:
    """The columns a consumer of *variable_type* will actually receive.

    Not guessable from the canvas — it depends on how the variable stores its
    data:

    * a **DataFrame**-stored variable arrives under the user's own column
      names; there is no column named after the class;
    * a **dict**-stored (multi_column) variable arrives as one column per key;
    * a **scalar or array** arrives as schema-key columns plus ONE data column
      named after the class (``view_name()``).

    Read live from ``_variables.dtype`` on every panel open rather than
    scaffolded anywhere — a cached list goes stale the moment the variable is
    re-saved with a different shape, and both consumers of this (the glue
    panel and the function node's Inputs picker) re-read it every time.

    Variables own this, not glue. It grew up inside ``glue_service`` because
    glue was the first caller; a second caller with the same question is a
    reason to move it to the layer that owns the answer, not to copy it
    (``feedback_avoid_scifor_scidb_duplication``).

    Returns ``{"ok": True, "variable_type", "schema_keys", "data_columns",
    "note"}``, or ``{"ok": False, "error"}`` when no database is open.
    """
    from scistack_gui import db as db_module

    try:
        active = db if db is not None else db_module.get_db()
    except Exception:
        active = None
    if active is None:
        return {"ok": False, "error": "No database is open."}

    schema_keys = list(active.dataset_schema_keys)
    try:
        dtype = active.get_dtype_meta(variable_type)
    except Exception as exc:
        logger.debug(
            "[variables] dtype lookup failed for %s: %s", variable_type, exc
        )
        dtype = None

    mode = (dtype or {}).get("mode") if isinstance(dtype, dict) else None
    if mode == "dataframe":
        data_columns = list((dtype or {}).get("columns", {}) or {})
        note = "This variable stores a DataFrame, so its own column names arrive."
    elif mode == "multi_column":
        data_columns = list((dtype or {}).get("columns", {}) or {})
        note = "This variable stores a dict; one column per key arrives."
    else:
        data_columns = [variable_type]
        note = (
            f"This variable stores a scalar or array, so its data arrives in one "
            f"column named '{variable_type}'."
        )

    logger.info(
        "[variables] input_columns(%s): mode=%s, %d data column(s), "
        "%d schema key(s)",
        variable_type,
        mode or "scalar/array",
        len(data_columns),
        len(schema_keys),
    )
    return {
        "ok": True,
        "variable_type": variable_type,
        "schema_keys": schema_keys,
        "data_columns": data_columns,
        "note": note,
    }
