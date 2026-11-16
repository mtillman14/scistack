"""The project's output folder: ``[outputs] folder`` in ``scistack.toml`` — THE owner.

Every ``PathOutput`` a project writes (figures from ``plot_`` endpoints, any
file a function is handed an output path for) usually shares one root
folder. Repeating that folder in every template means editing every node to
move them, so it is stated ONCE:

.. code-block:: toml

    # scistack.toml
    [outputs]
    folder = "outputs"            # relative to the project root
    # folder = "/data/aim2/out"   # or absolute
    # folder = "{results}/aim2"   # or a machine root (~/.scistack/machine.toml)

and each ``PathOutput`` template stays relative (``figures/{subject}.png``).
:func:`output_folder` is what ``PathOutput.resolve`` joins a relative result
onto. An absolute template is never moved.

**Unset means unchanged.** With no ``[outputs] folder`` this returns ``None``
and a relative path stays relative — resolved by the working directory, which
every GUI and generated MATLAB run sets to the project root
(:func:`scifor.project.root.enter_project_root`). So a project that never
states a folder writes exactly where it always did.

Read live, like ``[aliases]`` and ``[schema_keys]``: the config is located
through :func:`scifor.project.schema_order.locate_config` (the pinned project
root, else the cwd; cached for a few seconds) and parsed on its mtime, so an
edit reaches the next run with no restart. ``PathOutput.resolve`` calls this
once per combo; both caches keep that to a ``stat``.

**Not part of identity.** scidb leaves ``PathOutput`` out of version keys, so
changing the folder does not make anything stale: ``skip_computed`` keeps the
files where the last run wrote them, and a forced run writes to the new folder.
"""

from __future__ import annotations

from pathlib import Path

from scistacklog import Log

from .config import read_scistack_section, resolve_config_path

#: The TOML table and its one key.
SECTION = "outputs"
FOLDER_KEY = "folder"

#: ``{config path: (mtime, raw folder or None)}``.
_cache: dict[str, tuple[float, "str | None"]] = {}
#: The last answer logged, so the INFO line appears when it changes, not per combo.
_logged: dict[str, "str | None"] = {}


def declared_folder(section: "dict | None") -> "str | None":
    """``[outputs] folder`` as written in a parsed config, or ``None``.

    THE reader of that key's shape: the GUI's Paths popup and
    :func:`output_folder` both call it. A non-string or blank value is
    treated as unset, with a WARN naming it.
    """
    table = (section or {}).get(SECTION)
    if table is None:
        return None
    if not isinstance(table, dict):
        Log.warn("[outputs] scistack.toml: [%s] must be a table; ignored", SECTION)
        return None
    raw = table.get(FOLDER_KEY)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        Log.warn(
            "[outputs] scistack.toml: [%s] %s must be a non-empty path string, got %r; "
            "ignored — relative output paths stay relative to the project root",
            SECTION,
            FOLDER_KEY,
            raw,
        )
        return None
    return raw.strip()


def _folder_in(config: Path) -> "str | None":
    key = str(config)
    try:
        mtime = config.stat().st_mtime
    except OSError:
        mtime = 0.0
    cached = _cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    raw = declared_folder(read_scistack_section(config))
    _cache[key] = (mtime, raw)
    return raw


def output_folder() -> "Path | None":
    """The folder relative ``PathOutput`` paths are written under, or ``None``
    when the project states none (see the module docstring).

    Relative to the project root when the value is relative; machine roots
    and paths written on another platform resolve as every config path does
    (:func:`scifor.project.config.resolve_config_path`). Logs one INFO line
    whenever the answer changes.
    """
    from .schema_order import locate_config

    config = locate_config()
    if config is None:
        return None
    raw = _folder_in(config)
    resolved = resolve_config_path(config.parent, raw) if raw is not None else None
    answer = str(resolved) if resolved is not None else None
    if _logged.get(str(config), "<unset>") != answer:
        _logged[str(config)] = answer
        if resolved is None:
            Log.info(
                "[outputs] %s states no [%s] %s — relative output paths are "
                "written relative to the working directory (the project root in a run)",
                config,
                SECTION,
                FOLDER_KEY,
            )
        else:
            Log.info(
                "[outputs] relative output paths are written under %s (%s: [%s] %s = %r)",
                resolved,
                config,
                SECTION,
                FOLDER_KEY,
                raw,
            )
    return resolved


def under_output_folder(path: "str | Path") -> "str | Path":
    """*path* joined onto :func:`output_folder` when it is relative and a
    folder is stated; otherwise *path* unchanged. Keeps the input's type
    (``str`` in, ``str`` out) — the contract ``PathOutput.resolve`` has."""
    if Path(str(path)).is_absolute():
        return path
    folder = output_folder()
    if folder is None:
        return path
    joined = folder / str(path)
    return joined if isinstance(path, Path) else str(joined)


def clear_cache() -> None:
    """Forget every parsed config (tests, the config writer, a project switch)."""
    _cache.clear()
    _logged.clear()
