"""Backward-compatibility shim — the Log facade lives in ``scistacklog``.

This module is kept so one public contract stays intact:

- Python imports: ``from scidb.log import Log`` (used across scidb,
  scistack-gui, and scimatlab's bridge).

**Python callers only.** MATLAB must not reach the class through this path:
``py.scidb.log.Log.info(...)`` fails with "Dot indexing into the result of a
function call requires parentheses after the function name", because MATLAB
resolves ``py.<module>.<Class>.<method>`` statically only when the class is
*defined* in that module, and ``Log.__module__`` is ``"scistacklog"``. MATLAB
code calls ``py.scistacklog.Log.*`` (see ``+scidb/Log.m`` and ``+scifor/Log.m``).

See the ``scistacklog`` package for the implementation and format docs.
"""

from pathlib import Path

from scistacklog import LAYERS, Log

__all__ = ["Log", "LAYERS", "LOG_FILE_NAME", "log_path_for", "attach_log_file"]

#: The file name of the log beside every database. The one owner: MATLAB's
#: configure_database calls log_path_for, and the extension's
#: failureReporter.ts copy is pinned to this by a scistack-gui test.
LOG_FILE_NAME = "scistack.log"


def log_path_for(db_path) -> Path:
    """The log file that belongs to *db_path*: ``scistack.log`` beside it.

    One definition of the convention, so every caller that needs the path
    before (or without) ``configure_database`` agrees with it.
    """
    return Path(db_path).parent / LOG_FILE_NAME


def attach_log_file(db_path) -> Path:
    """Point the file sink at :func:`log_path_for` and return that path.

    Idempotent: re-attaching the same path is a no-op, so a caller may
    attach early (before any work worth logging) without
    ``configure_database`` later tearing the handler down and reopening it.

    Attaching early is the point of this helper. Anything logged before the
    first call goes to stderr only, which is how the GUI's whole startup
    discovery pass — the registry scan that decides which functions,
    variables and PathInputs exist — used to be missing from ``scistack.log``.
    """
    log_path = log_path_for(db_path)
    if Log.get_path() == str(log_path):
        return log_path
    Log.set_path(str(log_path))
    return log_path
