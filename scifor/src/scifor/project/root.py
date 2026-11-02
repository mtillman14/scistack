"""THE owner of "which directory is this project?" (the project root).

Model-free: ``scifor.project`` imports nothing from the scifor engine, so
config, discovery and display grammars can depend on the root without
pulling in ``for_each``. ``scifor.pathinput`` reads the root from here.
"""

import os
from pathlib import Path

from scistacklog import Log

_project_root_override: Path | None = None


def set_project_root(root: "str | Path | None") -> "Path | None":
    """Pin the directory a *rootless* PathInput resolves against.

    A ``PathInput`` with no ``root_folder`` resolves relative paths against
    :func:`project_root`, which is the **cwd** unless pinned.  That is right
    for a script run from its project folder and wrong for every embedded
    interpreter: MATLAB's cwd is wherever the user's MATLAB happens to be
    sitting (for a generated command, a temp script directory), so the cwd
    names the wrong folder.

    Callers that already know which project they are running — ``scidb.project.entities``
    over the MATLAB bridge, the GUI's generated command preamble — set it here
    once, and every rootless PathInput in the process resolves against it.

    This deliberately changes *resolution only*.  ``root_folder`` stays ``None``
    and ``to_key()`` is untouched, so the recorded identity of a PathInput is
    the same no matter where it ran.  Writing the project root into
    ``root_folder`` instead is what produced ``__unresolved__`` ghost nodes in
    the GUI (a run recorded under a key its own declaration does not have) and
    would bake a machine-specific absolute path into portable history.

    Passing ``None`` clears the override.  Returns the resolved override.
    """
    global _project_root_override
    _project_root_override = Path(root).resolve() if root is not None else None
    Log.info("[pathinput] project root override set to %s", _project_root_override)
    return _project_root_override


def get_project_root() -> "Path | None":
    """The pinned project root, or ``None`` when :func:`project_root` falls
    back to the cwd.  See :func:`set_project_root`."""
    return _project_root_override


def enter_project_root() -> Path:
    """Make the pinned project root the process's working directory.

    Every execution starts from the project root, so a user function that
    opens a relative path finds it in the same place no matter who launched
    the run.  Called at the start of each run rather than once at startup:
    a user function is free to ``os.chdir`` and the next run must not
    inherit that.  The process stays there afterwards (there is no
    "previous" directory worth restoring in a server process), matching the
    generated MATLAB script, whose ``cd`` also sticks.

    With no pin, :func:`project_root` *is* the cwd, so nothing moves.  A
    pinned root that is not a directory is logged and left alone rather
    than failing the run -- resolution against it would fail loudly anyway.
    Returns the working directory the run starts in.
    """
    root = _project_root_override
    try:
        before = Path.cwd().resolve()
    except FileNotFoundError:
        # The old cwd was deleted out from under the process.
        before = None
    if root is None:
        Log.info("[pathinput] working directory %s (no project root pinned)", before)
        return before if before is not None else Path.cwd()
    if not root.is_dir():
        Log.warn(
            "[pathinput] project root %s is not a directory -- run starts in %s",
            root,
            before,
        )
        return before if before is not None else Path.cwd()
    if before != root:
        os.chdir(root)
        Log.info("[pathinput] working directory %s -> %s (project root)", before, root)
    else:
        Log.info("[pathinput] working directory %s (project root)", root)
    return root


def clear_project_root() -> None:
    """Drop the pinned project root (tests, and any caller switching projects
    inside one process)."""
    set_project_root(None)


def project_root() -> Path:
    """**The** project root: the pinned root, else the working directory.

    The one owner of "which directory is this project?" for every layer --
    PathInput resolution here, config lookup in ``scidb.project.entities`` and
    ``scifor.project.schema_order``, MATLAB stub placement. The GUI decides its root
    (``scistack_gui.project.config.resolve_project_root``) and pins it here, so a GUI
    run resolves exactly where the canvas matched.

    It deliberately does NOT walk up looking for ``pyproject.toml`` or
    ``scistack.toml``. That walk made a stray config in a parent folder, or
    an unrelated ``pyproject.toml`` beneath the project, silently decide
    where every rootless PathInput resolved -- and each layer walked by a
    different rule, so they could disagree.
    """
    if _project_root_override is not None:
        return _project_root_override
    return Path.cwd().resolve()

