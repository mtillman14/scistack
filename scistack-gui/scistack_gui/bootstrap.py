"""
Shared "open or create a SciStack project" sequence.

Both entry points that can load a database run the exact same steps: import
user pipeline code (project / module / auto-discover), open or create the
DuckDB file, restore manually-declared builtin function references, bridge
Python logging into scidb.log, and check for a stale uv.lock.

Historically this only ran once, inline in ``__main__.py``'s ``main()``,
because the browser CLI always loaded a database before starting uvicorn.
Now that the frontend can also launch with no database yet and trigger this
sequence later from a request handler (see ``api/bootstrap.py``), the logic
lives here so both callers behave identically.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("scistack_gui.bootstrap")


@dataclass
class BootstrapResult:
    db_name: str
    schema_keys: list[str]
    functions_loaded: int = 0
    variables_loaded: int = 0
    matlab_functions_loaded: int = 0
    matlab_variables_loaded: int = 0
    warnings: list[str] = field(default_factory=list)


def open_or_create_project(
    db_path: Path,
    *,
    schema_keys: list[str] | None = None,
    module: Path | None = None,
    project: Path | None = None,
    init_project_files: bool = True,
    entities_file: "str | Path | None" = None,
) -> BootstrapResult:
    """Import pipeline code, then open (or create) ``db_path``.

    If ``schema_keys`` is given and ``db_path`` does not exist yet, the
    database is created with those schema keys. Otherwise ``db_path`` must
    already exist and is opened as-is, reading its schema keys back out of
    the ``_schema`` table.

    Raises
    ------
    ValueError
        Both ``module`` and ``project`` were given, or ``schema_keys`` is
        empty when creating a new database.
    FileNotFoundError
        ``module`` doesn't exist, or ``db_path`` doesn't exist and no
        ``schema_keys`` were given to create it.
    FileExistsError
        ``schema_keys`` given but ``db_path`` already exists.
    """
    if module and project:
        raise ValueError("module and project are mutually exclusive.")

    db_path = Path(db_path)
    exists = db_path.exists()

    # Whether the caller passed schema_keys at all (not their truthiness) is
    # the signal for create-vs-open intent — `schema_keys=[]` must still be
    # treated as "asked to create, with invalid keys" (ValueError), not
    # silently reinterpreted as "open" just because the list is empty.
    if schema_keys is not None:
        if exists:
            raise FileExistsError(f"Database already exists: {db_path}")
        if not schema_keys:
            raise ValueError("schema_keys must not be empty")
        create_new = True
    else:
        if not exists:
            raise FileNotFoundError(f"Database not found: {db_path}")
        create_new = False

    logger.info(
        "[bootstrap] open_or_create_project: db_path=%s create_new=%s module=%s project=%s",
        db_path,
        create_new,
        module,
        project,
    )

    warnings: list[str] = []

    # Make the project's declaration surfaces exist BEFORE anything reads the
    # config, so the load below picks them up and nothing has to reload to
    # discover them. See services.project_init_service.
    #
    # Skipped in single-file (--module) mode, which has no project root to
    # initialize, and when the caller opts out: ``POST /api/bootstrap/create``
    # with an explicit ``entities_file: null`` means "leave this project in
    # the pure folder-scan state", and initializing anyway would override a
    # choice the user made on purpose.
    #
    # ``entities_file`` is where to put one if the project has none -- the
    # creation wizard's field. It is create-only: a project that already
    # declares an entities file keeps it, so creating a database in a project
    # that already has declarations cannot re-point them at an empty file.
    if not module and init_project_files:
        from scistack_gui.services.project_init_service import ensure_project_files

        init = ensure_project_files(db_path, project, entities_file)
        warnings.extend(init.warnings)

    # Import user code first so that configure_database() can auto-register
    # the user's variable classes.
    from scistack_gui import registry

    functions_loaded = 0
    variables_loaded = 0
    matlab_functions_loaded = 0
    matlab_variables_loaded = 0
    loaded_config = None

    if project:
        from scistack_gui.config import load_config

        logger.info("[bootstrap] project mode: loading config from %s", project)
        config = load_config(project, db_path)
        loaded_config = config
        result = registry.load_from_config(config)
        functions_loaded = len(result["functions"])
        variables_loaded = len(result["variables"])
        logger.info(
            "[bootstrap] project mode: %d functions, %d variables",
            functions_loaded,
            variables_loaded,
        )
        if config.has_matlab:
            from scistack_gui import matlab_registry

            matlab_result = matlab_registry.load_from_config(config)
            matlab_functions_loaded = len(matlab_result["matlab_functions"])
            matlab_variables_loaded = len(matlab_result["matlab_variables"])
            logger.info(
                "[bootstrap] MATLAB: %d functions, %d variables",
                matlab_functions_loaded,
                matlab_variables_loaded,
            )
    elif module:
        module_path = Path(module).resolve()
        if not module_path.exists():
            raise FileNotFoundError(f"Module not found: {module_path}")
        import importlib.util

        logger.info("[bootstrap] single-file mode: loading module %s", module_path)
        spec = importlib.util.spec_from_file_location("user_pipeline", module_path)
        user_mod = importlib.util.module_from_spec(spec)
        with registry._suppress_user_code_output():
            spec.loader.exec_module(user_mod)
        registry.register_module(user_mod, module_path=module_path)
        logger.info("[bootstrap] loaded module: %s", module_path)
    else:
        from scistack_gui.config import load_config

        logger.info(
            "[bootstrap] no module/project given: auto-discovering near %s", db_path
        )
        try:
            config = load_config(None, db_path)
            loaded_config = config
            result = registry.load_from_config(config)
            functions_loaded = len(result["functions"])
            variables_loaded = len(result["variables"])
            logger.info(
                "[bootstrap] auto-discovered: %d functions, %d variables",
                functions_loaded,
                variables_loaded,
            )
            if config.has_matlab:
                from scistack_gui import matlab_registry

                matlab_result = matlab_registry.load_from_config(config)
                matlab_functions_loaded = len(matlab_result["matlab_functions"])
                matlab_variables_loaded = len(matlab_result["matlab_variables"])
                logger.info(
                    "[bootstrap] MATLAB: %d functions, %d variables",
                    matlab_functions_loaded,
                    matlab_variables_loaded,
                )
        except Exception as e:
            msg = f"Auto-discovery failed ({e}); starting with an empty registry."
            logger.warning("[bootstrap] %s", msg)
            warnings.append(msg)

    # After the load, because which stubs are wanted depends on what the
    # config actually resolved. A fresh stub declares nothing, so this adds
    # no reload -- see services.project_init_service.
    if loaded_config is not None:
        from scistack_gui.services.project_init_service import ensure_language_stubs

        try:
            warnings.extend(ensure_language_stubs(loaded_config).warnings)
        except Exception:
            logger.exception("[bootstrap] failed to create language entities stubs")

    from scistack_gui.db import create_db, init_db

    if create_new:
        logger.info(
            "[bootstrap] creating new database %s (schema_keys=%s)",
            db_path,
            schema_keys,
        )
        db = create_db(db_path, schema_keys)
    else:
        logger.info("[bootstrap] opening existing database %s", db_path)
        db = init_db(db_path)

    try:
        from scistack_gui.services.builtin_function_service import (
            replay_persisted_builtins,
        )

        replay_persisted_builtins(db)
    except Exception:
        logger.exception("[bootstrap] failed to restore builtin function references")
        warnings.append("Failed to restore builtin function references.")

    try:
        from scistack_gui.pipeline_discovery import discover_and_seed_pipelines

        pipeline_result = discover_and_seed_pipelines(db)
        if pipeline_result["created"]:
            logger.info(
                "[bootstrap] seeded %d pipeline(s) from source: %s",
                len(pipeline_result["created"]),
                pipeline_result["created"],
            )
    except Exception:
        logger.exception("[bootstrap] failed to discover/seed pipelines from source")
        warnings.append("Failed to discover pipelines defined in source.")

    from scidb.log import Log

    Log.bridge_python_logging()

    from scistack_gui import startup as _startup

    if loaded_config is not None:
        _startup.check_windows_config_paths(loaded_config)
    _startup.check_lockfile_staleness(db_path.parent)
    for err in _startup.get_startup_errors():
        logger.warning("[bootstrap] startup warning [%s]: %s", err.kind, err.message)

    logger.info(
        "[bootstrap] project ready: %s (schema_keys=%s)", db_path, db.dataset_schema_keys
    )
    return BootstrapResult(
        db_name=db_path.name,
        schema_keys=list(db.dataset_schema_keys),
        functions_loaded=functions_loaded,
        variables_loaded=variables_loaded,
        matlab_functions_loaded=matlab_functions_loaded,
        matlab_variables_loaded=matlab_variables_loaded,
        warnings=warnings,
    )
