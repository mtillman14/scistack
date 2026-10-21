"""
The "re-read config from disk, reload both registries" sequences.

**The** owner of every full registry reload. Two entry points:

- :func:`refresh_registries` -- Refresh Code, the discovery scan's
  ``force_refresh``, and the entity-write fallback. Re-resolves which files
  the project's config names (directory entries and globs are expanded
  again), so a ``.py``/``.m`` file created after startup is found without a
  restart. Replaying the config captured at startup -- what all three used
  to do via ``registry.refresh_all()``/``matlab_registry.refresh_all()`` --
  only re-read files that existed at startup.
- :func:`reload_registries_from_disk` -- after ``add_path``/``remove_path``/
  ``set_entities_file`` (and creating the entities file) change *which
  paths are configured*; re-resolves the project root itself too.
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path

from scistack_gui.config import SciStackConfig

logger = logging.getLogger(__name__)


def reload_registries_from_disk(db_path: Path) -> SciStackConfig:
    """Re-parse scistack.toml from disk and reload both the Python and
    MATLAB registries against the fresh config. Returns the new config."""
    from scistack_gui import matlab_registry, registry
    from scistack_gui.config import load_config

    new_config = load_config(None, db_path)
    try:
        registry.load_from_config(new_config)
    except Exception:
        logger.exception("Failed to reload Python registry after config change")
    try:
        matlab_registry.load_from_config(new_config)
    except Exception:
        logger.exception("Failed to reload MATLAB registry after config change")
    return new_config


def refresh_registries() -> dict:
    """Re-import the project's code from disk, Python then MATLAB.

    Returns the Python registry's diff summary. Raises ``RuntimeError`` when
    nothing was ever loaded; any other exception is the reload's own.

    Config mode re-reads the config at the SAME project root the loaded
    config came from (never re-resolving the root -- a refresh must not
    move the project), so new files under a configured directory, glob, or
    folder-scan root are picked up. Single-file ``--module`` mode has no
    config to re-read and re-imports that one file.

    Both registries, always: ``registry.load_from_config`` clears the
    shared ``_path_inputs``/``_parameters`` dicts that MATLAB declarations
    are registered into, so a Python-only reload would drop every MATLAB
    entity (see ``target_file_service._refresh_registries``).
    """
    from scistack_gui import matlab_registry, registry
    from scistack_gui.config import load_config

    # Several callers can trigger this; one frame of context says which.
    caller = "".join(traceback.format_stack(limit=3)[:-1]).strip().replace("\n", " | ")
    old_config = registry._config
    if old_config is None:
        logger.info("[registry_reload] Single-file refresh — requested by: %s", caller)
        result = registry.refresh_module()
        if matlab_registry._config is not None:
            matlab_registry.load_from_config(matlab_registry._config)
        return result

    root = old_config.project_root
    logger.info(
        "[registry_reload] Refresh from disk at %s — requested by: %s", root, caller
    )
    # project_path is an existing directory here, so load_config never
    # consults db_path (resolve_project_root's last-resort fallback only).
    new_config = load_config(root, root)
    _log_file_changes(old_config, new_config)
    result = registry.load_from_config(new_config)
    matlab_registry.load_from_config(new_config)
    return result


def _log_file_changes(old: SciStackConfig, new: SciStackConfig) -> None:
    """Which configured source files appeared/disappeared since the last
    load -- the answer to "did Refresh Code see my new file?"."""
    for label, before, after in (
        ("Python module", old.modules, new.modules),
        ("MATLAB source", _matlab_files(old), _matlab_files(new)),
    ):
        added = sorted(set(map(str, after)) - set(map(str, before)))
        removed = sorted(set(map(str, before)) - set(map(str, after)))
        logger.info(
            "[registry_reload] %s files: %d now (%d added, %d removed)",
            label, len(after), len(added), len(removed),
        )
        for path in added:
            logger.info("[registry_reload]   + %s", path)
        for path in removed:
            logger.info("[registry_reload]   - %s", path)


def _matlab_files(config: SciStackConfig) -> list[Path]:
    return [
        *config.matlab_functions,
        *config.matlab_variables,
        *config.matlab_sources,
    ]
