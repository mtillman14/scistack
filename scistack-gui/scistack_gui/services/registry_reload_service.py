"""
Shared "re-read config from disk, reload both registries" sequence.

Extracted out of ``api/project.py``'s ``_reload_config_and_rescan`` so the
same reload logic can also be triggered by
``target_file_service.get_or_create_target_file`` (a newly-created
``variable_file`` changes *which paths are configured*, exactly like
``add_path``/``remove_path`` do -- reusing the stale in-memory
``registry._config`` via ``refresh_all()`` would silently fail to discover
the new file until the server restarts, same reasoning as the module
docstring on the original function explained).
"""

from __future__ import annotations

import logging
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
