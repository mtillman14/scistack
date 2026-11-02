"""THE reader of ``scistack.toml``: where it is, how it is read, and what
each section means.

Three steps, each with one owner here:

1. **Locate**: :func:`config_path_at` (where the config of a root lives,
   existing or not) and :func:`project_config_at` (the parseable one AT a
   root, never above it).
2. **Read**: :func:`read_scistack_section` (lenient: ``None`` for a broken
   file, for callers that treat that as "no config") and
   :func:`load_config_file` (strict: raises, for writers and the GUI's
   loader, which must report a broken file rather than overwrite it).
3. **Meaning**: :func:`read_project_config` validates every section the
   project runtime uses and returns a typed :class:`ProjectConfig`. A path
   written in the file is resolved by :func:`resolve_config_path`, which
   also expands machine roots (``{name}/...``, ``scifor.project.machine``)
   and tolerates a config written on another platform.

The TEXT of the file (rendering, writing every key back) is
``scifor.project.config_file``'s. Expanding a ``modules`` directory or glob
into source files is code discovery, which the caller does
(``scistack_gui.project.config.load_config``) -- this module says what an
entry IS, not which files it matches today.

``tests/test_toml_reader_owner.py`` (scifor) fails on a TOML read anywhere
else in the repo, so a new reader cannot drift from this one.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    try:
        import tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

from .machine import expand_roots

logger = logging.getLogger(__name__)


CONFIG_FILENAME = "scistack.toml"
"""THE project config file, and the only one (user decision 2026-10-08).

``pyproject.toml`` is packaging metadata only and is never read for
configuration: no ``[tool.scistack]`` table, no precedence rule between two
files. The GUI owns and writes ``scistack.toml``; it never edits a
``pyproject.toml``. Neither file ships in a wheel, so a project installed into
another never brings a second config. See docs/claude/config-file-formats.md.
"""


def config_path_at(root: Path) -> Path:
    """Where the config of the project rooted at *root* lives, whether or not
    it exists yet. The one spelling of that location: readers and the GUI's
    writer both start here, so they cannot pick different files.

    *root* may be an existing file (its folder is used) or a directory --
    including one that does not exist YET: a folder about to become a
    project (``scistack init new_dir``, a bundle import into a new folder)
    has its config inside it, never in its parent. Treating every
    non-directory as a file used to send that lookup to the PARENT's
    scistack.toml, so creating a project inside another one was refused or
    read the wrong config.
    """
    root = Path(os.path.abspath(str(root)))
    search_dir = root.parent if root.is_file() else root
    return search_dir / CONFIG_FILENAME


def read_scistack_section(toml_path: Path) -> dict | None:
    """Parse the config file *toml_path* and return its contents (the whole
    file is the config), or ``None`` if it is unparseable. An empty file
    parses to ``{}``: a valid all-defaults config, deliberately distinct from
    ``None``."""
    try:
        with open(toml_path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        logger.debug("Failed to parse %s while reading scistack config", toml_path)
        return None


_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


def is_windows_absolute(raw: str) -> bool:
    """Whether *raw* is a Windows-absolute path (``Y:\\...`` or ``\\\\host``).

    On POSIX neither form means anything -- ``Path.is_absolute()`` says
    False, so the value gets silently joined onto the project root and
    produces nonsense like ``/Users/me/project/y:\\LabMembers\\...``. Callers
    use this to say so instead.
    """
    return bool(_WINDOWS_DRIVE_RE.match(raw)) or raw.startswith("\\\\")


def normalize_config_separators(raw: str) -> str:
    """*raw* with separators this platform understands.

    Backslash → forward slash, on POSIX only, and never for a
    Windows-absolute value (there is nothing to salvage in ``Y:\\...`` here).
    Split out from :func:`resolve_config_path` for the callers that need the
    STRING rather than a Path -- glob patterns, which are expanded as text.
    """
    raw = expand_roots(raw)
    if os.sep != "\\" and "\\" in raw and not is_windows_absolute(raw):
        return raw.replace("\\", "/")
    return raw


def resolve_config_path(root: Path, raw: str) -> Path:
    """Resolve *raw*, a path written in a project config, against *root*.

    Config files travel between machines -- the same repo checked out on
    Windows and macOS, a scistack.toml committed to git -- and the GUI wrote
    them with the *host's* separator. A Windows session recording
    ``entities_file = "src\\scistack_entities.toml"`` produced a value that,
    read back on macOS, names a FILE called ``src\\scistack_entities.toml``
    in the project root: ``\\`` is a legal filename character on POSIX, so
    nothing errors, the wrong file is created and read, and everything
    derived from its location moves with it (the MATLAB classdef stub
    directory lands in the project root instead of ``src/``). Backslash is
    therefore treated as a separator here whenever this is not Windows.

    The reverse direction needs nothing: Windows accepts ``/`` natively,
    which is why the writers now record ``/`` (see
    ``scistack_gui.project.config._portable_relpath``).

    A Windows-*absolute* value (``Y:\\LabMembers\\...``) is a different
    problem -- there is no such drive here and no sane way to guess what it
    should be -- so it is returned as-is for the caller to report as
    missing, rather than being joined onto *root*.
    """
    # `{name}/...` first: a machine root expands to an absolute path in this
    # machine's spelling, which the rules below then treat like any other.
    raw = expand_roots(raw)
    if os.sep != "\\" and "\\" in raw:
        if is_windows_absolute(raw):
            logger.warning(
                "Config path %r is a Windows absolute path and cannot resolve "
                "on this platform", raw
            )
            return Path(raw)
        converted = normalize_config_separators(raw)
        logger.info(
            "Config path %r was written on Windows; reading it as %r",
            raw,
            converted,
        )
        # Reading it literally, as every version before this one did, CREATES
        # things: a "src\scistack_entities.toml" in the project root that the
        # GUI then wrote declarations into. Say so, loudly and with both
        # paths -- the alternative is the user watching entities disappear
        # the moment this fix lands, with nothing in the log to explain it.
        literal = root / raw
        if literal.exists():
            logger.warning(
                "%s exists: it was created by reading the Windows path %r "
                "literally on this platform. It is NOT being read any more -- "
                "%s is. Move anything you need out of it, then delete it.",
                literal,
                raw,
                root / converted,
            )
        raw = converted
    p = Path(raw)
    return p if p.is_absolute() else root / p


def project_config_at(root: Path) -> Path | None:
    """The parseable ``scistack.toml`` in *root*, or ``None``.

    **Looks in *root* and nowhere else.** The project root is decided once,
    by ``scifor.project.root.project_root`` (the pinned root, else the cwd), and
    the project's config lives there by definition. This replaced an upward
    walk (``find_project_config``) that let a config in a parent folder claim
    every project beneath it, and that the GUI had already abandoned -- so
    scidb and the GUI could read two different configs for one folder.

    *root* may be a file (its folder is used) or a directory.
    """
    candidate = config_path_at(root)
    if candidate.exists() and read_scistack_section(candidate) is not None:
        logger.debug("Found project config %s", candidate)
        return candidate
    logger.debug("No parseable %s in %s", CONFIG_FILENAME, candidate.parent)
    return None


def load_config_file(toml_path: Path) -> dict:
    """The whole config at *toml_path* as a dict. Raises ``OSError`` or
    ``tomllib.TOMLDecodeError``: a writer that read a broken file as ``{}``
    would replace it with an empty config, so the strict read never guesses.
    """
    with open(toml_path, "rb") as f:
        return tomllib.load(f)


# ---------------------------------------------------------------------------
# What each section means
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatlabSection:
    """``[matlab]``: entries as written (paths, directories or globs)."""

    functions: tuple[str, ...] = ()
    variables: tuple[str, ...] = ()
    #: Unclassified ``.m`` paths, each file classified on its own.
    sources: tuple[str, ...] = ()
    variable_dir: str | None = None
    #: The read-only ``.m`` entities script (the TOML file replaced it).
    entities_file: str | None = None


@dataclass(frozen=True)
class ProjectConfig:
    """The validated sections of one ``scistack.toml``.

    Path-valued fields hold the entry AS WRITTEN; resolve one with
    :meth:`resolve`. *section* is the whole parsed file, for readers of keys
    with an owner elsewhere (``entities_file`` -> ``entities_file``,
    ``[schema_keys]`` -> ``schema_order``, ``[aliases]``, ``[colors]``).
    """

    path: Path
    #: ``.py`` files, directories or globs to discover.
    modules: tuple[str, ...] = ()
    #: Installed packages to discover: the project's libraries, as listed.
    packages: tuple[str, ...] = ()
    auto_discover: bool = True
    #: The GUI's code surfaces (one file per glue node / created function).
    glue_dir: str | None = None
    functions_dir: str | None = None
    #: Legacy ``.py`` declarations, read-only.
    variable_file: str | None = None
    matlab: MatlabSection = field(default_factory=MatlabSection)
    section: Mapping[str, Any] = field(default_factory=dict)

    @property
    def root(self) -> Path:
        """The project root: the folder holding the config."""
        return self.path.parent

    def resolve(self, raw: str) -> Path:
        """An entry of this config as a path on this machine."""
        return resolve_config_path(self.root, str(raw))


def read_project_config(toml_path: Path) -> ProjectConfig:
    """Read and validate the config at *toml_path*.

    Raises ``ValueError`` naming the key for a value of the wrong type, and
    ``OSError`` / ``tomllib.TOMLDecodeError`` for a file that cannot be read
    (see :func:`load_config_file`).
    """
    toml_path = Path(toml_path)
    section = load_config_file(toml_path)
    matlab = section.get("matlab", {})
    if not isinstance(matlab, dict):
        raise ValueError("scistack.toml: [matlab] must be a table.")
    auto_discover = section.get("auto_discover", True)
    if not isinstance(auto_discover, bool):
        raise ValueError("scistack.toml: auto_discover must be true or false.")
    config = ProjectConfig(
        path=toml_path,
        modules=_path_list(section, "modules", "modules"),
        packages=_name_list(section, "packages"),
        auto_discover=auto_discover,
        glue_dir=_optional_path(section, "glue_dir"),
        functions_dir=_optional_path(section, "functions_dir"),
        variable_file=_optional_path(section, "variable_file"),
        matlab=MatlabSection(
            functions=_path_list(matlab, "functions", "matlab.functions"),
            variables=_path_list(matlab, "variables", "matlab.variables"),
            sources=_path_list(matlab, "sources", "matlab.sources"),
            variable_dir=_optional_path(matlab, "variable_dir"),
            entities_file=_optional_path(matlab, "entities_file"),
        ),
        section=section,
    )
    logger.info(
        "[config] read %s: %d module entr(ies), %d package(s), auto_discover=%s, "
        "%d MATLAB function / %d variable / %d source entr(ies)",
        toml_path,
        len(config.modules),
        len(config.packages),
        config.auto_discover,
        len(config.matlab.functions),
        len(config.matlab.variables),
        len(config.matlab.sources),
    )
    return config


def _path_list(table: Mapping[str, Any], key: str, label: str) -> tuple[str, ...]:
    raw = table.get(key, [])
    if not isinstance(raw, list):
        raise ValueError(f"scistack.toml: {label} must be a list of file paths.")
    return tuple(str(entry) for entry in raw)


def _name_list(table: Mapping[str, Any], key: str) -> tuple[str, ...]:
    raw = table.get(key, [])
    if not isinstance(raw, list):
        raise ValueError(f"scistack.toml: {key} must be a list of package names.")
    return tuple(str(name) for name in raw)


def _optional_path(table: Mapping[str, Any], key: str) -> str | None:
    raw = table.get(key)
    return None if raw is None else str(raw)
