"""Machine-specific path roots: ``{rto}/LabMembers/...`` in a shared config.

``scistack.toml`` is committed and opened on several computers, so an
absolute path in it is right on one machine only (``Y:\\`` on Windows,
``/home/me/mnt/rto`` on Linux). Everywhere else it was a "File does not
exist" load error on every registry refresh — 22 in scidb.log 2026-10-19.

The config now writes such a path once, portably, as ``{name}/rest``. Each
machine says what ``name`` means in a file OUTSIDE the repo::

    # ~/.scistack/machine.toml   (Windows: %USERPROFILE%\\.scistack\\machine.toml)
    machine_id = "6f1c..."      # generated on first use; for logs only
    [roots]
    rto = "Y:/"

This module is the one owner of that file and of the ``{name}`` grammar.
``scifor.discovery.resolve_config_path`` / ``normalize_config_separators``
expand through :func:`expand_roots`, so every config reader gets it; config
WRITERS record :func:`portable_form`; the GUI's startup notice and its
config loops ask :func:`unresolvable_here` which entries to skip and why.

Design: ``.claude/plan-machine-paths.md``; format: docs/claude/config-file-formats.md.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    try:
        import tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

#: Overrides the location — tests, and a machine whose home is not writable.
MACHINE_FILE_ENV = "SCISTACK_MACHINE_FILE"

#: ``{name}`` at the very start of a value; nowhere else is a root.
_ROOT_RE = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_-]*)\}")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


@dataclass(frozen=True)
class MachineConfig:
    path: Path
    machine_id: str
    #: ``{name: absolute path on this machine}``, as written.
    roots: dict[str, str] = field(default_factory=dict)


# (path, mtime_ns) -> MachineConfig. Re-read when the file changes, so an
# edit takes effect at the next registry refresh without a restart.
_cache: dict[tuple[str, int], MachineConfig] = {}


def machine_file_path() -> Path:
    """``$SCISTACK_MACHINE_FILE``, else ``~/.scistack/machine.toml``."""
    override = os.environ.get(MACHINE_FILE_ENV)
    if override:
        return Path(override)
    return Path.home() / ".scistack" / "machine.toml"


def _render(machine_id: str, roots: dict[str, str]) -> str:
    lines = [
        "# SciStack machine-specific settings. Not part of any project: each",
        "# computer has its own. A project path written as \"{name}/rest\" in",
        "# scistack.toml resolves through [roots] below.",
        f'machine_id = "{machine_id}"',
        "",
        "[roots]",
    ]
    lines += [f'{name} = "{value}"' for name, value in sorted(roots.items())]
    return "\n".join(lines) + "\n"


def machine_config() -> MachineConfig:
    """This machine's config, creating the file (with a ``machine_id``) on
    first use. Never raises: an unreadable file is logged and treated as
    having no roots, so a bad edit costs the roots, not the session."""
    path = machine_file_path()
    if not path.exists():
        machine_id = uuid.uuid4().hex
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(_render(machine_id, {}), encoding="utf-8")
            logger.info("[machine] created %s (machine_id=%s)", path, machine_id)
        except OSError as exc:
            logger.warning("[machine] could not create %s: %s", path, exc)
            return MachineConfig(path=path, machine_id=machine_id)
    try:
        key = (str(path), path.stat().st_mtime_ns)
    except OSError:
        key = (str(path), -1)
    cached = _cache.get(key)
    if cached is not None:
        return cached

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except Exception as exc:
        logger.warning("[machine] %s is not valid TOML (%s); no roots mapped", path, exc)
        data = {}
    raw_roots = data.get("roots") or {}
    roots = {
        str(name): str(value)
        for name, value in raw_roots.items()
        if isinstance(value, str) and value
    }
    rejected = sorted(set(raw_roots) - set(roots))
    if rejected:
        logger.warning("[machine] %s: roots %s are not non-empty strings; ignored", path, rejected)
    config = MachineConfig(
        path=path, machine_id=str(data.get("machine_id") or ""), roots=roots
    )
    _cache.clear()
    _cache[key] = config
    logger.info(
        "[machine] %s: machine_id=%s, %d root(s) %s",
        path, config.machine_id or "(none)", len(roots),
        {name: value for name, value in sorted(roots.items())},
    )
    return config


def root_name(raw: str) -> str | None:
    """The ``name`` of a ``{name}/...`` value, else None."""
    match = _ROOT_RE.match(raw)
    return match.group(1) if match else None


def expand_roots(raw: str) -> str:
    """*raw* with a leading ``{name}`` replaced by this machine's root.

    Unmapped → returned unchanged; :func:`unresolvable_here` says why, and
    callers that iterate config entries skip it rather than report a
    missing file.
    """
    name = root_name(raw)
    if name is None:
        return raw
    base = machine_config().roots.get(name)
    if base is None:
        return raw
    rest = raw[len(name) + 2:].lstrip("/\\")
    expanded = base if not rest else base.rstrip("/\\") + "/" + rest
    logger.debug("[machine] expanded %r -> %r", raw, expanded)
    return expanded


def _is_windows_absolute(raw: str) -> bool:
    return bool(_WINDOWS_DRIVE_RE.match(raw)) or raw.startswith("\\\\")


def unresolvable_here(raw: str) -> str | None:
    """Why config value *raw* cannot name anything on this machine, or None.

    - ``{name}`` with no ``name`` in this machine's ``[roots]``;
    - a Windows-absolute path on Linux/macOS;
    - a Linux/macOS-absolute path (``/home/...``) on Windows, where it would
      otherwise be read as relative to the current DRIVE — scidb.log
      2026-10-19's ``y:\\home\\mtillman\\mnt\\rto\\...``.
    """
    name = root_name(raw)
    if name is not None:
        config = machine_config()
        if name in config.roots:
            return None
        return f"root {{{name}}} is not mapped on this machine (add it under [roots] in {config.path})"
    if os.sep != "\\" and _is_windows_absolute(raw):
        return "a Windows path cannot resolve on this machine"
    if os.sep == "\\" and raw.startswith("/") and not raw.startswith("//"):
        return "a Linux/macOS absolute path cannot resolve on Windows"
    return None


def portable_form(path: "Path | str") -> str | None:
    """``{name}/rest`` for an absolute *path* under one of this machine's
    roots (the longest matching root wins), else None. What config writers
    record, so the value works on every machine that maps ``name``."""
    target = _comparable(str(path))
    best: tuple[int, str, str] | None = None
    for name, base in machine_config().roots.items():
        root = _comparable(base)
        if target == root or target.startswith(root.rstrip("/") + "/"):
            if best is None or len(root) > best[0]:
                best = (len(root), name, target[len(root.rstrip("/")):].lstrip("/"))
    if best is None:
        return None
    _, name, rest = best
    # The original spelling of the remainder, not the case-folded one.
    original = str(path).replace("\\", "/").rstrip("/")
    rest = original[len(original) - len(rest):] if rest else ""
    return f"{{{name}}}/{rest}" if rest else f"{{{name}}}"


def _comparable(raw: str) -> str:
    """Forward slashes, no trailing slash; case-folded on Windows, whose
    paths are case-insensitive."""
    value = raw.replace("\\", "/").rstrip("/") or "/"
    if os.sep == "\\" or _is_windows_absolute(raw):
        value = str(PureWindowsPath(value).as_posix()).lower()
    else:
        value = str(PurePosixPath(value))
    return value
