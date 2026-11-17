"""
Where an import keeps what came from the sender's bundle -- the one owner of
the layout (``.claude/plan-imported-folder-layout.md``).

An import always makes a NEW project, so a project holds at most one import
and there is no per-export subfolder (the export time is in ``import.json``)::

    .scistack/imported/
        import.json        the import record: bundle, key map, options (verify reads it)
        environment.json   the sender's Python and packages (install reads it)
        wheelhouse/        the sender's library wheels (install reads it)
        run-history/       the sender's run history when it was not loaded live
                           (``scistack verify`` compares against it)
        logs/              the end of the sender's logs (issue #20), for people

Nothing here is written by anything but an import, and nothing here is ever
mixed into the project's own database or logs.
"""

from __future__ import annotations

from pathlib import Path

IMPORTED_DIR = ".scistack/imported"
IMPORT_RECORD = "import.json"
ENVIRONMENT_FILE = "environment.json"
WHEELHOUSE_DIR = "wheelhouse"
RUN_HISTORY_DIR = "run-history"
LOGS_DIR = "logs"


def imported_dir(root: "Path | str") -> Path:
    """``<root>/.scistack/imported``."""
    return Path(root) / IMPORTED_DIR


def run_history_dir(root: "Path | str") -> Path:
    """The sender's run history, kept as files when not loaded live."""
    return imported_dir(root) / RUN_HISTORY_DIR
