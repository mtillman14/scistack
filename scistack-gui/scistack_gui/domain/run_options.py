"""
The default value of every GUI run option -- read from scidb, never restated.

A function node's Run Options (dry run, save, distribute, as table, stop at
first error) are ``scidb.for_each`` keyword
arguments, so ``for_each``'s signature is where their defaults are decided.
Before this module the GUI restated them twice -- ``run_service`` had its own
``opts.get("save", True)`` literals and ``Sidebar.tsx`` had
``{ dry_run: false, save: true, ... }`` -- and the sidebar's non-default
badges need ONE answer to "is this the default", so both now read this
(``.claude/plan-palette-order-and-delete.md``, step ``run_option_defaults``).

``as_table`` is ``None`` in ``for_each`` (meaning "no inputs as tables"); the
GUI exposes it as a checkbox, so it is served as ``bool(None) == False``.
"""

from __future__ import annotations

import functools
import inspect
import logging

logger = logging.getLogger(__name__)

#: GUI run-option key -> the ``for_each`` keyword it is passed as.
RUN_OPTION_KWARGS = {
    "dry_run": "dry_run",
    "save": "save",
    "distribute": "distribute",
    "as_table": "as_table",
    "on_error": "on_error",
}

#: Options the GUI shows as a checkbox: the default is coerced to bool.
_BOOLEAN_OPTIONS = {"dry_run", "save", "distribute", "as_table"}


@functools.lru_cache(maxsize=1)
def run_option_defaults() -> dict:
    """``{option: default}`` for every GUI run option, from the signature of
    ``scidb.for_each``. Cached: the signature cannot change in a session."""
    from scidb.run.foreach import for_each

    params = inspect.signature(for_each).parameters
    defaults = {}
    for option, kwarg in RUN_OPTION_KWARGS.items():
        default = params[kwarg].default
        defaults[option] = bool(default) if option in _BOOLEAN_OPTIONS else default
    logger.debug("[run_options] run option defaults from scidb.for_each: %s", defaults)
    return defaults
