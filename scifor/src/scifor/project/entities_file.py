"""
The entities file: declaring Variables, Parameters and PathInputs in TOML.

This is the single writable declaration surface for all three kinds, in
both languages -- it replaces the Python ``variable_file`` (a ``.py``
module that was *executed* to get its declarations) and the MATLAB
``[matlab] entities_file`` (a ``.m`` script that was *statically parsed*).
One file, one format, read identically by Python and, over the bridge, by
MATLAB. See ``.claude/plan-entities-toml-26-08-31.md`` and
``docs/claude/entity-editability-model.md``.

This module is the GRAMMAR half and owns the format: locating the file,
parsing it into plain frozen dataclasses (:class:`DeclaredEntities`),
scanning where each entry sits, and rendering and editing the text. It
builds no scidb object, so it lives in the model-free ``scifor.project``.
``scidb.project.entities`` is the MATERIALIZE half: it turns a
:class:`DeclaredEntities` into ``BaseVariable`` subclasses, ``Parameter``
and ``PathInput``/``EachOf`` objects and caches them. ``scistack_gui``
keeps the *policy* around writing (which file may be written, staleness,
atomic replacement, rollback), exactly as ``source_edit`` does for the
Python form.

The format::

    # Variables are a value-less list, so they come first: a bare
    # top-level key placed after a [section] header would bind to it.
    variables = ["StepLength", "EmgEnvelope"]

    [parameters]
    SAMPLING_RATE_HZ = 1000                    # one value
    WINDOW_SECONDS   = [10, 20, 30]            # three values (fan-out)
    SUBJECT_IDS      = ["01", "02"]            # stays string, never 1/2
    CONFIG           = { fld1 = 1, fld2 = 2 }  # the dict IS the value
    CUTOFF_HZ        = []                      # declared, not yet valued

    [path_inputs]
    EMG_FILE = "{subject}/{session}_emg.csv"
    RAW_FILE = { template = "{subject}/raw.csv", root_folder = "/data/raw" }
    XSENS    = { template = "{session}/Xsens/{subject}_{session}.xlsx", aliases = { session = { BL = ["Baseline"] } } }
    GAIT     = { template = "{subject}_{speed}{trial}.csv", key_regex = { subject = 'R2_\\d{3}', speed = '[A-Za-z]+' } }

    [library]                                  # only in a LIBRARY's file
    schema_keys = ["subject", "session"]       # its own schema vocabulary

The ``[library]`` table is what makes a package's entities file a
library's (portability Stage 10): ``schema_keys`` names the vocabulary its
shared pipelines are written in, so placing one into a project with other
keys can suggest a key map (``scidb.project.schema_map.KeyMap.auto``).

Rules worth stating once, because they are what make the format
unambiguous:

- **An array is always the alternative list.** ``[10, 20, 30]`` is three
  Parameter values; a Parameter whose single value is genuinely a list
  nests it as ``[[1, 2, 3]]``. The EMPTY array is a Parameter declared but
  not yet given a value -- legal here, refused at ``for_each`` expansion
  (see scidb's ``parameter.py``); it is what the GUI writes for a new
  Parameter, which used to be a placeholder ``0`` indistinguishable from a
  real value.
- **An inline table means the value under ``[parameters]``, and the field
  set under ``[path_inputs]``.** A PathInput has no dict-shaped value, so
  a table there is unambiguously ``{template, root_folder, aliases,
  key_regex}``. ``key_regex`` is ``{key = 'pattern'}`` -- what ``{key}``
  matches on disk (``scifor.PathInput(key_regex=)``); single-quote it so
  ``\\d`` is not a TOML escape. An aliased key also matches its declared
  spellings, then canonicalizes.
  ``aliases`` is ``{key = {canonical = [spellings]}}`` -- several on-disk
  spellings of one schema-key value (``scifor.PathInput(aliases=)``,
  match-only). A canonical that is not a bare TOML key is quoted
  (``{ "01" = ["Baseline"] }``); a lone spelling may be a bare string.
- **Values are never re-parsed.** TOML types are taken as they come, so a
  zero-padded ``"01"`` stays the string ``"01"`` -- there is no literal
  parser in the path to turn it into ``1``
  (``feedback_zero_padded_schema_keys``).
- **Anything this shape cannot say stays in Python, read-only**: custom
  ``to_db``/``from_db``, a non-default ``schema_version``, a whole-template
  PathInput ``regex``, or computed Parameter values.

Errors are **per entry**: one bad declaration is recorded with its name
and line and skipped, never raised. The ``.py`` entities file it replaces
could not do that -- an exception anywhere in it took every entity in the
file down at once, silently (module-load failures are logged at DEBUG).
"""

from __future__ import annotations

import keyword
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scistacklog import Log

from .discovery import own_package_dir
from .config import project_config_at, read_scistack_section, resolve_config_path
from .root import project_root
from .source_edit import Span, line_number, splice

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    try:
        import tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]


PARAMETERS = "parameters"
PATH_INPUTS = "path_inputs"
VARIABLES = "variables"

SECTIONS = (PARAMETERS, PATH_INPUTS)
"""The two table sections. Variables are a top-level array, not a table --
they have no value to carry."""

LIBRARY = "library"
"""The library table: ``schema_keys`` (see the module docstring). Not one of
``SECTIONS``: it declares no entities and the GUI never writes it."""

DEFAULT_ENTITIES_FILENAME = "scistack_entities.toml"
DEFAULT_ENTITIES_RELPATH = Path("src") / DEFAULT_ENTITIES_FILENAME


def default_entities_relpath(root: "Path | str", package: "str | None" = None) -> Path:
    """Where a NEW entities file goes for the project at *root*, relative to
    it: inside the project's own package when it is one -- *package* when
    the caller is creating that package (``scidb.project.init.init_project``),
    else ``scifor.project.discovery.own_package_dir``
    (``src/<name>/scistack_entities.toml``, so it is package data and ships
    in the project's wheel), else :data:`DEFAULT_ENTITIES_RELPATH`.

    The one owner of that default: ``scidb.project.init.init_project`` and the
    GUI's ``config.set_entities_file`` both ask here. It decides only where a
    file is CREATED; which file a project uses is
    :func:`resolve_entities_path`.
    """

    if package is None:
        own = own_package_dir(Path(root))
        if own is None:
            return DEFAULT_ENTITIES_RELPATH
        package = own[0]
    return Path("src") / package / DEFAULT_ENTITIES_FILENAME

_PATH_INPUT_KEYS = frozenset({"template", "root_folder", "aliases", "key_regex"})


# ---------------------------------------------------------------------------
# Results: what a file declares, as plain data
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntityError:
    """One rejected declaration. *line* is 1-based, or 0 when the entry
    could not be located in the text (a value the scanner cannot span, for
    instance) -- the GUI shows ``file:line`` when it has one."""

    name: str
    line: int
    message: str

    def describe(self) -> str:
        where = f" (line {self.line})" if self.line else ""
        return f"{self.name}{where}: {self.message}"


@dataclass(frozen=True)
class DeclaredVariable:
    """``variables = ["NAME", ...]``: a Variable type by name, nothing more."""

    name: str
    line: int


@dataclass(frozen=True)
class DeclaredParameter:
    """``NAME = value`` / ``NAME = [v1, v2]`` under ``[parameters]``.

    *values* is the alternative list exactly as TOML typed it; empty means
    declared but not yet given a value (see the module docstring)."""

    name: str
    values: tuple[Any, ...]
    line: int


@dataclass(frozen=True)
class PathInputArm:
    """One template of a PathInput declaration, with the fields TOML can say.

    *aliases* is already normalized to ``{key: {canonical: [spellings]}}``.
    What this layer cannot check (an alias key that is not a placeholder, a
    ``key_regex`` that does not compile) is ``scifor.PathInput``'s to refuse
    when the declaration is materialized."""

    template: str
    root_folder: str | None = None
    aliases: dict | None = None
    key_regex: dict | None = None


@dataclass(frozen=True)
class DeclaredPathInput:
    """A ``[path_inputs]`` entry: one arm, or several (alternate templates,
    an ``EachOf`` once materialized). The TOML key is the PathInput's name."""

    name: str
    arms: tuple[PathInputArm, ...]
    line: int


@dataclass(frozen=True)
class DeclaredEntities:
    """Everything one entities file declares, plus what it got wrong.

    Plain data: no Variable class, Parameter or PathInput exists yet.
    ``scidb.project.entities.materialize`` builds those from it.
    """

    path: Path
    variables: tuple[DeclaredVariable, ...] = ()
    parameters: tuple[DeclaredParameter, ...] = ()
    path_inputs: tuple[DeclaredPathInput, ...] = ()
    #: ``{"section.name" | "name": 1-based line}`` for every located entry.
    lines: Mapping[str, int] = field(default_factory=dict)
    errors: tuple[EntityError, ...] = ()
    #: ``[library] schema_keys``, or ``None`` when the file has no
    #: ``[library]`` table (a project's own file).
    library_schema_keys: tuple[str, ...] | None = None

    def names(self) -> list[str]:
        """Every successfully-declared name, in section order."""
        return [
            *(v.name for v in self.variables),
            *(p.name for p in self.parameters),
            *(p.name for p in self.path_inputs),
        ]
def _decode_error_line(exc: Exception) -> int:
    """The line a ``tomllib`` decode error points at.

    Python 3.14+ and tomli 2.1+ (the 3.10 fallback) set ``lineno`` on the
    exception; Python 3.11-3.13 and tomli 2.0 put it in the message alone,
    as ``(at line N, column M)``. Missing is 0, meaning "no location",
    never a misleading line 1."""
    lineno = getattr(exc, "lineno", None)
    if lineno:
        return int(lineno)
    match = re.search(r"\(at line (\d+), column \d+\)", str(exc))
    return int(match.group(1)) if match else 0


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_entities_file(path: Path | str) -> DeclaredEntities:
    """Parse every declaration in the entities file at *path*.

    A missing file is not an error -- it is a project that has not created
    an entity yet, and returns an empty result. A file that will not parse
    as TOML at all yields one error carrying the parser's own line number,
    because there are no entries to attribute individual failures to.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        Log.debug("[entities] %s does not exist yet; nothing to load", path)
        return DeclaredEntities(path=path)
    except OSError as e:
        Log.error("[entities] Cannot read %s: %s", path, e)
        return DeclaredEntities(
            path=path, errors=(EntityError(str(path), 0, f"cannot read file: {e}"),)
        )
    return parse_entities(text, path)


def parse_entities(text: str, path: Path | str) -> DeclaredEntities:
    """Parse entities-file *text* (read from *path*, which is only recorded)."""
    path = Path(path)
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        Log.error("[entities] %s is not valid TOML: %s", path, e)
        return DeclaredEntities(
            path=path,
            errors=(EntityError(path.name, _decode_error_line(e), f"invalid TOML: {e}"),),
        )

    parse = _Parse(
        lines={key: line_number(text, span.start) for key, span in _scan_entries(text).items()}
    )
    parse.variables(data)
    parse.parameters(data)
    parse.path_inputs(data)
    parse.library(data)
    declared = parse.result(path)
    Log.debug(
        "[entities] Parsed %s: %d variable(s), %d parameter(s), %d path input(s), "
        "%d rejected",
        path,
        len(declared.variables),
        len(declared.parameters),
        len(declared.path_inputs),
        len(declared.errors),
    )
    return declared


@dataclass
class _Parse:
    """The accumulator one parse fills, section by section."""

    lines: dict[str, int]
    declared: set[str] = field(default_factory=set)
    errors: list[EntityError] = field(default_factory=list)
    found_variables: list[DeclaredVariable] = field(default_factory=list)
    found_parameters: list[DeclaredParameter] = field(default_factory=list)
    found_path_inputs: list[DeclaredPathInput] = field(default_factory=list)
    library_schema_keys: list[str] | None = None

    def result(self, path: Path) -> DeclaredEntities:
        return DeclaredEntities(
            path=path,
            variables=tuple(self.found_variables),
            parameters=tuple(self.found_parameters),
            path_inputs=tuple(self.found_path_inputs),
            lines=dict(self.lines),
            errors=tuple(self.errors),
            library_schema_keys=(
                None if self.library_schema_keys is None else tuple(self.library_schema_keys)
            ),
        )

    def _name_error(self, name: str) -> str | None:
        """Why *name* cannot be declared, or ``None``.

        Duplicates are checked across all three kinds together: the name is
        how every consumer -- the canvas, the registry, ``scidb.project.entities.X``
        -- refers to the entity, so the same name meaning two things has no
        resolution.
        """
        if not name or not name.isidentifier() or keyword.iskeyword(name):
            return f"'{name}' is not a valid Python identifier"
        if name.startswith("_"):
            return "names must not start with an underscore"
        if name in self.declared:
            return "duplicate name -- already declared in this file"
        return None

    def variables(self, data: dict) -> None:
        """``variables = ["A", "B"]`` -> a :class:`DeclaredVariable` per name."""
        for section in SECTIONS:
            if isinstance(data.get(section), dict) and VARIABLES in data[section]:
                # TOML binds a bare key to whatever table precedes it, so a
                # `variables` list written below [parameters] silently becomes
                # parameters.variables and every name in it vanishes. Say so.
                self.errors.append(
                    EntityError(
                        VARIABLES,
                        self.lines.get(f"{section}.{VARIABLES}", 0),
                        f"'{VARIABLES}' is inside [{section}] -- move it above the "
                        f"first section header, or it binds to that table",
                    )
                )

        raw = data.get(VARIABLES)
        if raw is None:
            return
        line = self.lines.get(VARIABLES, 0)
        if not isinstance(raw, list):
            self.errors.append(
                EntityError(VARIABLES, line, f"must be a list of names, got {type(raw).__name__}")
            )
            return

        for entry in raw:
            if not isinstance(entry, str):
                self.errors.append(
                    EntityError(
                        str(entry),
                        line,
                        f"variable names must be strings, got {type(entry).__name__}",
                    )
                )
                continue
            name = entry.strip()
            err = self._name_error(name)
            if err is not None:
                self.errors.append(EntityError(name, line, err))
                continue
            self.declared.add(name)
            self.found_variables.append(DeclaredVariable(name, line))
            self.lines.setdefault(name, line)
            Log.debug("[entities] Declared variable %s", name)

    def parameters(self, data: dict) -> None:
        """``NAME = value`` / ``NAME = [v1, v2]`` -> a :class:`DeclaredParameter`."""
        section = data.get(PARAMETERS)
        if section is None:
            return
        if not isinstance(section, dict):
            self.errors.append(EntityError(PARAMETERS, 0, f"[{PARAMETERS}] must be a table"))
            return

        for name, raw in section.items():
            if name == VARIABLES:
                continue  # already reported by variables()
            line = self.lines.get(f"{PARAMETERS}.{name}", 0)
            err = self._name_error(name)
            if err is not None:
                self.errors.append(EntityError(name, line, err))
                continue
            # An array is the alternative list; everything else -- including
            # an inline table -- is one value as-is.
            #
            # An EMPTY array is not an error: `NAME = []` is a Parameter that
            # has been declared but not yet given a value, which is what the
            # GUI's "New parameter" form writes (it collects a name and
            # nothing else). It is refused at for_each expansion, not here --
            # see scidb's parameter.py.
            values = tuple(raw) if isinstance(raw, list) else (raw,)
            self.declared.add(name)
            self.found_parameters.append(DeclaredParameter(name, values, line))
            self.lines[name] = line
            Log.debug("[entities] Declared parameter %s with %d value(s)", name, len(values))

    def path_inputs(self, data: dict) -> None:
        """``NAME = template`` / ``{template, root_folder, ...}`` / a list of
        either -> a :class:`DeclaredPathInput`."""
        section = data.get(PATH_INPUTS)
        if section is None:
            return
        if not isinstance(section, dict):
            self.errors.append(EntityError(PATH_INPUTS, 0, f"[{PATH_INPUTS}] must be a table"))
            return

        for name, raw in section.items():
            if name == VARIABLES:
                continue
            line = self.lines.get(f"{PATH_INPUTS}.{name}", 0)
            err = self._name_error(name)
            if err is not None:
                self.errors.append(EntityError(name, line, err))
                continue

            # A list is alternate templates -- the EachOf-of-PathInputs form
            # the registry scanner already accepts. Only portability's
            # importer writes one, but the format has to be able to hold it.
            raw_alts = raw if isinstance(raw, list) else [raw]
            if not raw_alts:
                self.errors.append(
                    EntityError(name, line, "a PathInput needs at least one template")
                )
                continue

            arms, arm_err = _parse_path_input_arms(raw_alts)
            if arm_err is not None:
                self.errors.append(EntityError(name, line, arm_err))
                continue

            self.declared.add(name)
            self.found_path_inputs.append(DeclaredPathInput(name, tuple(arms), line))
            self.lines[name] = line
            Log.debug(
                "[entities] Declared path input %s with %d template(s), alias keys %s",
                name,
                len(arms),
                [sorted(a.aliases or {}) for a in arms],
            )
            if any(a.key_regex for a in arms):
                Log.info(
                    "[entities] path input %s: key_regex %s",
                    name,
                    [a.key_regex for a in arms],
                )

    def library(self, data: dict) -> None:
        keys, errors = parse_library_table(data)
        line = self.lines.get(LIBRARY, 0)
        for message in errors:
            self.errors.append(EntityError(LIBRARY, line, message))
        if keys is not None:
            self.library_schema_keys = keys
            Log.debug("[entities] library schema keys %s", keys)


def _parse_path_input_arms(raw_alts: list) -> tuple[list[PathInputArm], str | None]:
    """``(arms, error)`` -- exactly one is meaningful."""
    arms = []
    for alt in raw_alts:
        if isinstance(alt, str):
            arms.append(PathInputArm(alt))
            continue
        if not isinstance(alt, dict):
            return [], (
                f"expected a template string or a {{template, root_folder}} "
                f"table, got {type(alt).__name__}"
            )
        unknown = sorted(set(alt) - _PATH_INPUT_KEYS)
        if unknown:
            return [], (
                f"unknown key(s) {', '.join(unknown)} -- a PathInput accepts "
                f"only template, root_folder, aliases and key_regex here; "
                f"declare a whole-template regex in Python"
            )
        template = alt.get("template")
        if not isinstance(template, str) or not template:
            return [], "missing a 'template' string"
        root = alt.get("root_folder")
        if root is not None and not isinstance(root, str):
            return [], f"root_folder must be a string, got {type(root).__name__}"
        aliases, alias_err = _parse_path_input_aliases(alt.get("aliases"))
        if alias_err is not None:
            return [], alias_err
        key_regex = alt.get("key_regex")
        if key_regex is not None and not (
            isinstance(key_regex, dict)
            and all(isinstance(v, str) for v in key_regex.values())
        ):
            return [], (
                "key_regex must be a table {key = 'pattern'} of strings "
                "(single-quoted, so \\d is not a TOML escape)"
            )
        arms.append(PathInputArm(template, root, aliases, key_regex))
    return arms, None
#: The keys a ``[library]`` table accepts. ``derived_from`` (A8,
#: plan-library-fork-chain): the libraries this one was copied from, newest
#: first -- where it came from, never who.
LIBRARY_KEYS = ("schema_keys", "derived_from")


def parse_derived_from(data: dict) -> "tuple[list[str], list[str]]":
    """``[library] derived_from = [...]`` from parsed TOML *data*:
    ``(names, [error, ...])``; ``[]`` when absent."""
    section = data.get(LIBRARY)
    if not isinstance(section, dict) or section.get("derived_from") is None:
        return [], []
    raw = section["derived_from"]
    if not isinstance(raw, list) or not all(isinstance(n, str) and n.strip() for n in raw):
        return [], [f"[{LIBRARY}] derived_from must be a list of library names"]
    return [n.strip() for n in raw], []


def parse_library_table(data: dict) -> "tuple[list[str] | None, list[str]]":
    """``[library] schema_keys = [...]`` from parsed TOML *data*:
    ``(schema_keys or None, [error, ...])``. A list of distinct, non-empty
    strings; any other key in the table is an error (a typo would otherwise
    silently mean "no schema keys"). Side-effect free: ``scidb.project.library``
    reads an installed library's table without constructing its Variables."""
    section = data.get(LIBRARY)
    if section is None:
        return None, []
    if not isinstance(section, dict):
        return None, [f"[{LIBRARY}] must be a table"]
    errors = []
    unknown = sorted(set(section) - set(LIBRARY_KEYS))
    if unknown:
        errors.append(
            f"[{LIBRARY}] unknown key(s) {', '.join(unknown)}; it accepts "
            f"{', '.join(LIBRARY_KEYS)}"
        )
    errors += parse_derived_from(data)[1]
    keys = section.get("schema_keys")
    if keys is None:
        return None, errors
    if (
        not isinstance(keys, list)
        or not all(isinstance(k, str) and k.strip() for k in keys)
        or len({k.strip() for k in keys}) != len(keys)
    ):
        errors.append(f"[{LIBRARY}] schema_keys must be a list of distinct, non-empty key names")
        return None, errors
    return [k.strip() for k in keys], errors


def _parse_path_input_aliases(raw) -> "tuple[dict | None, str | None]":
    """``aliases = {key = {canonical = [spelling, ...]}}`` -> the
    ``scifor.PathInput(aliases=)`` dict, or ``(None, error)``.

    A lone spelling may be written as a bare string; it is normalized to a
    one-element list, so the object (and anything rendered back from it)
    always holds lists. Canonical values and spellings are strings: TOML
    keys always are, and a spelling is text found on disk.
    """
    if raw is None:
        return None, None
    if not isinstance(raw, dict):
        return None, (
            f"aliases must be a table {{key = {{canonical = [spellings]}}}}, "
            f"got {type(raw).__name__}"
        )
    out: dict = {}
    for key, canon_map in raw.items():
        if not isinstance(canon_map, dict):
            return None, (
                f"aliases.{key} must be a table {{canonical = [spellings]}}, "
                f"got {type(canon_map).__name__}"
            )
        out[key] = {}
        for canonical, spellings in canon_map.items():
            if isinstance(spellings, str):
                spellings = [spellings]
            if not isinstance(spellings, list) or not all(
                isinstance(s, str) and s for s in spellings
            ):
                return None, (
                    f"aliases.{key}.{canonical} must be a list of non-empty "
                    f"strings (on-disk spellings)"
                )
            out[key][canonical] = list(spellings)
    return out, None


# ---------------------------------------------------------------------------
# Locating the project's entities file
# ---------------------------------------------------------------------------


def _project_config(start: "Path | str | None") -> "Path | None":
    """The ``scistack.toml`` of the project rooted at
    *start* (default: ``scifor.project_root()``), or ``None`` -- with the one
    debug log both callers below want. *start* IS the root: nothing walks up
    from it (see ``scifor.project.config.project_config_at``)."""

    root = Path(start) if start is not None else project_root()
    config = project_config_at(root)
    if config is None:
        Log.debug("[entities] No project config at project root %s", root)
    return config


def resolve_entities_path(
    root: "Path | str", section: "dict | None"
) -> "Path | None":
    """The entities file of a project rooted at *root* whose scistack
    section is *section*.

    The rule itself, factored out of :func:`entities_path` so that a caller
    which has *already* located the project config and parsed its section --
    ``scistack_gui.project.config.load_config`` -- asks this function rather than
    reimplementing it. The two used to disagree: only this side had the
    conventional fallback, so a project whose entities file sat at
    ``src/scistack_entities.toml`` with no ``entities_file`` key was read by
    scidb and by MATLAB, and invisible to the GUI's registry -- its
    Variables, Parameters and PathInputs simply never appeared.

    The rule, in order:

    - ``entities_file = "some/path.toml"`` wins, resolved against *root*.
    - ``entities_file = ""`` is an **explicit opt-out**: this project has no
      entities file and the conventional path must NOT be adopted. It is
      what the GUI's "clear entities file" writes, which is the only way to
      say "stop reading that file" about a file sitting at the default
      location (deleting the key would just fall through to the next rule).
    - no key at all: the conventional ``src/scistack_entities.toml``,
      accepted **only if it already exists** -- guessing a path that doesn't
      would report a missing-file error against a file the user never asked
      for.
    - a key that is not a string: warned about, then treated as absent.

    The declared path is resolved through
    :func:`scifor.project.config.resolve_config_path`, so a value written by a
    Windows session (``src\\scistack_entities.toml``) still finds
    ``src/scistack_entities.toml`` here instead of naming a backslashed file
    in the project root -- which is what put MATLAB classdef stubs in the
    root rather than beside the entities file.
    """
    root = Path(root)
    section = section or {}
    raw = section.get("entities_file")
    if isinstance(raw, str) and raw:
        resolved = Path(
            os.path.normpath(os.path.abspath(str(resolve_config_path(root, raw))))
        )
        Log.debug("[entities] %s declares entities_file=%s", root, resolved)
        return resolved
    if isinstance(raw, str):
        Log.info(
            "[entities] %s declares entities_file = \"\" (explicit opt-out); "
            "no entities file for this project",
            root,
        )
        return None
    if raw is not None:
        # Not a string at all. Falling through to the conventional path is
        # the right recovery, but silently would leave the user staring at
        # entities that load from a file they did not name.
        Log.warn(
            "[entities] %s declares entities_file = %r, which is not a path "
            "string; ignoring it",
            root,
            raw,
        )

    fallback = Path(
        os.path.normpath(os.path.abspath(str(root / DEFAULT_ENTITIES_RELPATH)))
    )
    if fallback.exists():
        Log.info(
            "[entities] %s declares no entities_file; using the conventional %s",
            root,
            fallback,
        )
        return fallback
    Log.debug("[entities] %s declares no entities_file and %s does not exist",
              root, fallback)
    return None


def is_entities_opt_out(section: "dict | None") -> bool:
    """Whether *section* says, explicitly, that this project has no entities
    file -- ``entities_file = ""``.

    :func:`resolve_entities_path` returns ``None`` for this and for "nothing
    is configured and nothing is at the conventional path", which are the
    same answer to "what should I read?" but opposite answers to "should I
    create one?". Only a caller asking the second question needs this
    (``scistack_gui.services.project_init_service``, which would otherwise
    re-create and re-point at an entities file on every project open, undoing
    the user's clear).
    """
    return section is not None and section.get("entities_file") == ""


def entities_path(start: "Path | str | None" = None) -> "Path | None":
    """The entities file for the project rooted at *start* (default:
    ``scifor.project_root()``).

    Locates the project config, then applies :func:`resolve_entities_path`
    -- which is where the rule lives, and is documented.
    """
    config = _project_config(start)
    if config is None:
        return None
    return resolve_entities_path(config.parent, read_scistack_section(config))


# ---------------------------------------------------------------------------
# Scanning: where each entry lives in the text
# ---------------------------------------------------------------------------
#
# tomllib gives values but no positions, and positions are what the GUI
# needs for "declared at file:line" and what an in-place edit splices
# against. So the text is scanned once, independently: tomllib stays
# authoritative for what a value IS, and this only answers where it sits.
# An entry the scanner misses degrades to line 0 / not-editable, never to a
# wrong value.


def _scan_entries(text: str) -> "dict[str, Span]":
    """``{"section.name" | "name": span-of-its-value}`` for every top-level
    key assignment, keyed by section-qualified name (unqualified for keys
    above the first section header, i.e. ``variables``)."""
    return {qualified: value for qualified, (_key, value) in _scan(text).items()}


def _scan(text: str) -> "dict[str, tuple[Span, Span]]":
    """``{qualified-name: (span-of-its-key, span-of-its-value)}`` -- the one
    scanner. The key span covers the key as written, quotes included, so a
    rename replaces exactly the token and nothing around it."""
    spans: dict[str, tuple[Span, Span]] = {}
    section: str | None = None
    i = 0
    n = len(text)

    while i < n:
        line_end = text.find("\n", i)
        if line_end == -1:
            line_end = n
        stripped = text[i:line_end].strip()

        if not stripped or stripped.startswith("#"):
            i = line_end + 1
            continue

        if stripped.startswith("["):
            header = stripped.split("#", 1)[0].strip()
            section = header.strip("[]").strip().strip("\"'") or None
            i = line_end + 1
            continue

        eq = _key_assignment_end(text, i, line_end)
        if eq is None:
            i = line_end + 1
            continue
        key, key_span, value_start = eq
        value_end = _value_end(text, value_start)
        qualified = f"{section}.{key}" if section else key
        spans[qualified] = (key_span, Span(value_start, value_end))
        i = max(value_end, line_end) + 1

    return spans


def _key_assignment_end(
    text: str, start: int, line_end: int
) -> "tuple[str, Span, int] | None":
    """``(key, span-of-the-key-token, offset-of-value)`` for a ``key = `` at
    *start*, else None."""
    line = text[start:line_end]
    eq = line.find("=")
    if eq == -1:
        return None
    raw = line[:eq]
    key = raw.strip().strip("\"'")
    if not key or "." in key:
        # Dotted keys (`a.b = 1`) address a nested table; nothing in this
        # format uses them, and pretending otherwise would mis-key the span.
        return None
    key_span = Span(start + len(raw) - len(raw.lstrip()), start + len(raw.rstrip()))
    value_start = start + eq + 1
    while value_start < line_end and text[value_start] in " \t":
        value_start += 1
    return key, key_span, value_start


def _value_end(text: str, start: int) -> int:
    """Offset just past the value beginning at *start*.

    Handles the two things a naive end-of-line scan gets wrong: a value
    that spans lines inside ``[...]``/``{...}``, and a trailing ``#``
    comment, which is not part of the value.
    """
    i = start
    depth = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c in "\"'":
            i = _skip_string(text, i)
            continue
        if c in "[{":
            depth += 1
        elif c in "]}":
            depth -= 1
            if depth <= 0:
                i += 1
                break
        elif c == "#":
            if depth == 0:
                break
            nl = text.find("\n", i)
            i = n if nl == -1 else nl
            continue
        elif c == "\n" and depth == 0:
            break
        i += 1
    return len(text[:i].rstrip())


def _skip_string(text: str, i: int) -> int:
    """Offset just past the string literal starting at *i*."""
    quote = text[i]
    triple = text[i : i + 3]
    if triple in ('"""', "'''"):
        end = text.find(triple, i + 3)
        return len(text) if end == -1 else end + 3
    j = i + 1
    n = len(text)
    while j < n:
        c = text[j]
        if c == "\\" and quote == '"':
            j += 2
            continue
        if c == quote:
            return j + 1
        if c == "\n":  # unterminated: don't run off the end of the file
            return j
        j += 1
    return n


def find_entry_span(text: str, section: "str | None", name: str) -> "Span | None":
    """Span of the *value* of ``name`` in *section*, or ``None``.

    The counterpart of ``source_edit.find_binding_span``: everything
    outside the returned span -- comments, blank lines, other entries --
    survives an edit byte for byte.
    """
    qualified = f"{section}.{name}" if section else name
    span = _scan_entries(text).get(qualified)
    if span is None:
        Log.debug("[entities] No entry %s found in the text", qualified)
    return span


# ---------------------------------------------------------------------------
# Rendering and writing
# ---------------------------------------------------------------------------


def render_value(value: Any) -> str:
    """*value* as TOML source.

    Raises ``ValueError`` for anything TOML cannot hold -- ``None`` above
    all, which has no representation at all and would otherwise be written
    as something that reads back as a different value.
    """
    if isinstance(value, bool):
        # Before int: bool IS an int in Python, and `True` must not be
        # written as `1` -- that reads back as a different type.
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError(f"{value!r} has no TOML representation")
        return repr(value)
    if isinstance(value, str):
        return _render_string(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(render_value(v) for v in value) + "]"
    if isinstance(value, dict):
        items = []
        for k, v in value.items():
            if not isinstance(k, str) or not k.isidentifier():
                raise ValueError(f"{k!r} is not usable as a TOML key")
            items.append(f"{k} = {render_value(v)}")
        return "{ " + ", ".join(items) + " }"
    if value is None:
        raise ValueError(
            "TOML has no null; a Parameter value of None cannot be declared "
            "in the entities file"
        )
    raise ValueError(f"{type(value).__name__} has no TOML representation")


_STRING_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def _render_string(s: str) -> str:
    out = ['"']
    for ch in s:
        if ch in _STRING_ESCAPES:
            out.append(_STRING_ESCAPES[ch])
        elif ord(ch) < 0x20 or ord(ch) == 0x7F:
            out.append(f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def render_parameter_value(values: list) -> str:
    """A Parameter's RHS: a bare scalar for one value, an array for more.

    One value is deliberately NOT written as a one-element array. The file
    should read the way the concept does -- ``RATE = 1000`` -- and the
    single/multi distinction carries no meaning downstream:
    ``Parameter(30)`` and a bare ``30`` produce byte-identical
    ``version_keys`` (see parameter.py). Adding a value changes the form
    here from scalar to array, but never the kind, node, or history.
    """
    if len(values) == 1:
        return render_value(values[0])
    return render_value(list(values))


def render_path_input_value(
    template: str,
    root_folder: "str | None" = None,
    alternates: "list[dict] | None" = None,
    aliases: "dict | None" = None,
    key_regex: "dict | None" = None,
) -> str:
    """A PathInput's RHS: a bare template string when that is all there is,
    a ``{template, root_folder, aliases, key_regex}`` table when there is
    more, an array of either when there are alternates. *aliases* and
    *key_regex* belong to the primary template; each alternate dict carries
    its own."""
    arms = [_path_input_arm(template, root_folder, aliases, key_regex)]
    arms.extend(
        _path_input_arm(
            alt.get("template", ""),
            alt.get("root_folder"),
            alt.get("aliases"),
            alt.get("key_regex"),
        )
        for alt in (alternates or [])
    )
    if len(arms) == 1:
        return arms[0]
    return "[" + ", ".join(arms) + "]"


def _path_input_arm(
    template: str,
    root_folder: "str | None",
    aliases: "dict | None" = None,
    key_regex: "dict | None" = None,
) -> str:
    if not root_folder and not aliases and not key_regex:
        return render_value(template)
    fields = [f"template = {render_value(template)}"]
    if root_folder:
        fields.append(f"root_folder = {render_value(root_folder)}")
    if aliases:
        fields.append(f"aliases = {_render_aliases(aliases)}")
    if key_regex:
        fields.append(f"key_regex = {_render_key_regex(key_regex)}")
    return "{ " + ", ".join(fields) + " }"


def _render_key_regex(key_regex: dict) -> str:
    """``{ subject = 'R2_\\d{3}' }`` -- literal (single-quoted) strings, so a
    pattern's backslashes are written as typed, not as TOML escapes. A
    pattern holding ``'`` (or a control character) cannot be a literal
    string and falls back to the escaped basic form; both read back
    identically."""
    fields = []
    for key, pattern in key_regex.items():
        pattern = str(pattern)
        literal_ok = "'" not in pattern and not any(
            ord(ch) < 0x20 or ord(ch) == 0x7F for ch in pattern
        )
        rendered = f"'{pattern}'" if literal_ok else _render_string(pattern)
        fields.append(f"{_render_key(str(key))} = {rendered}")
    return "{ " + ", ".join(fields) + " }"


_BARE_TOML_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _render_key(key: str) -> str:
    """A TOML key: bare when TOML allows it, quoted otherwise. Alias
    canonicals are schema-key VALUES ("01", "1. Baseline"), not
    identifiers, so :func:`render_value`'s identifier-only rule (right for a
    Parameter dict, which must also be a MATLAB struct) does not apply."""
    return key if _BARE_TOML_KEY.fullmatch(key) else _render_string(key)


def _render_aliases(aliases: dict) -> str:
    """``{ session = { BL = ["Baseline"] } }`` -- the inverse of
    :func:`_parse_path_input_aliases`."""
    keys = []
    for key, canon_map in aliases.items():
        canons = [
            f"{_render_key(str(canonical))} = "
            f"{render_value([str(s) for s in spellings])}"
            for canonical, spellings in canon_map.items()
        ]
        keys.append(f"{_render_key(str(key))} = {{ {', '.join(canons)} }}")
    return "{ " + ", ".join(keys) + " }"


def initial_text() -> str:
    """The contents of a freshly-created entities file.

    ``variables`` is written up front, empty, for a reason the format makes
    unforgiving: a bare top-level key added later, below a section header,
    binds to that table instead. Creating the file with the key already in
    the right place means neither the GUI nor a hand-edit has to get that
    ordering right afterwards.
    """
    return (
        "# SciStack entities -- Variables, Parameters and PathInputs.\n"
        "# Written by the GUI; hand-editing is fine, it is re-read on every\n"
        "# scan. `variables` must stay above the first [section] header.\n"
        "\n"
        f"{VARIABLES} = []\n"
        "\n"
        f"[{PARAMETERS}]\n"
        "\n"
        f"[{PATH_INPUTS}]\n"
    )


def declared_names(text: str) -> set[str]:
    """Every name *text* declares, across all three kinds -- the namespace a
    new name must not collide with (see :meth:`_Parse._name_error`). Unparseable
    TOML declares nothing it can vouch for, so the scanner's keys are used
    instead of guessing."""
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return {q.rsplit(".", 1)[-1] for q in _scan(text) if q != VARIABLES}
    names = {str(v) for v in data.get(VARIABLES) or [] if isinstance(v, str)}
    for section in (PARAMETERS, PATH_INPUTS):
        table = data.get(section)
        if isinstance(table, dict):
            names.update(table)
    return names


def rename_entry(text: str, section: str, old: str, new: str) -> str:
    """*text* with the key ``old`` in *section* renamed to ``new``.

    Only the key token is replaced: the value, any trailing comment and
    every other entry survive byte for byte -- the same guarantee
    :func:`find_entry_span` gives a value edit. Raises ``ValueError`` when
    ``old`` is not in *section*, or when ``new`` is not a valid name or is
    already declared as ANY kind (one name, one entity -- see
    :meth:`_Parse._name_error`).
    """
    entry = _scan(text).get(f"{section}.{old}")
    if entry is None:
        raise ValueError(f"No entry '{old}' in [{section}].")
    if new == old:
        return text
    if not new or not new.isidentifier() or keyword.iskeyword(new):
        raise ValueError(f"'{new}' is not a valid name.")
    if new.startswith("_"):
        raise ValueError("Names must not start with an underscore.")
    if new in declared_names(text):
        raise ValueError(f"'{new}' is already declared in the entities file.")
    key_span, _value = entry
    Log.info(
        "[entities] rename_entry: [%s] %s -> %s (key at %d:%d)",
        section,
        old,
        new,
        key_span.start,
        key_span.end,
    )
    return splice(text, key_span, new)


def remove_entry(text: str, section: str, name: str) -> str:
    """*text* with the entry ``name`` in *section* removed.

    The whole entry goes: its key, its value (however many lines it spans)
    and a comment on the line the value ends on, which describes that entry.
    Every other byte -- neighbours, blank lines, comments on lines of their
    own, the section header even if it is left empty -- survives. Raises
    ``ValueError`` when ``name`` is not in *section*, so a caller can never
    report a delete that removed nothing.

    The undo of this lives in the GUI's history (the file's bytes are
    snapshotted before the write), not here: a removed entry is not kept
    anywhere in the file.
    """
    entry = _scan(text).get(f"{section}.{name}")
    if entry is None:
        raise ValueError(f"No entry '{name}' in [{section}].")
    key_span, value_span = entry
    start = text.rfind("\n", 0, key_span.start) + 1
    nl = text.find("\n", value_span.end)
    end = len(text) if nl == -1 else nl + 1
    Log.info(
        "[entities] remove_entry: [%s] %s (lines %d-%d): %r",
        section,
        name,
        line_number(text, start),
        line_number(text, max(start, end - 1)),
        text[start:end],
    )
    return text[:start] + text[end:]


def remove_variable(text: str, name: str) -> str:
    """*text* with *name* removed from the ``variables`` array.

    Only that element goes. On a multi-line array its whole line goes with
    it (a trailing ``# comment`` there describes the element); on a single
    line the element and one adjoining comma go. Comments on other lines,
    the other elements and their order survive. Removing the only element
    leaves ``variables = []``, which is valid and declares nothing. Raises
    ``ValueError`` when *name* is not in the array.
    """
    span = find_entry_span(text, None, VARIABLES)
    if span is None:
        raise ValueError(f"No variable '{name}' in the entities file.")
    current = text[span.start : span.end]
    element = _array_element_span(current, name)
    if element is None:
        raise ValueError(f"No variable '{name}' in the entities file.")
    start, end = element

    line_start = current.rfind("\n", 0, start) + 1
    nl = current.find("\n", end)
    line_end = len(current) if nl == -1 else nl
    after = current[end:line_end]
    after_code = _code_part(after).strip()
    if (
        "\n" in current
        and not current[line_start:start].strip()
        and after_code in ("", ",")
    ):
        # The element owns its line: drop the line, newline included.
        replacement = current[:line_start] + current[line_end + 1 :]
    else:
        # Shares a line with other elements: drop it and ONE adjoining
        # comma -- the following one, else the preceding one (it was last).
        j = end
        while j < len(current) and current[j] in " \t":
            j += 1
        if j < len(current) and current[j] == ",":
            j += 1
            while j < len(current) and current[j] in " \t":
                j += 1
            replacement = current[:start] + current[j:]
        else:
            i = start
            while i > 0 and current[i - 1] in " \t":
                i -= 1
            if i > 0 and current[i - 1] == ",":
                i -= 1
            replacement = current[:i] + current[end:]
    Log.info(
        "[entities] remove_variable: %s (line %d)",
        name,
        line_number(text, span.start + start),
    )
    return splice(text, span, replacement)


def _array_element_span(rendered: str, name: str) -> "tuple[int, int] | None":
    """``(start, end)`` of the string literal for *name* inside the
    rendered array *rendered* (offsets into *rendered*), or None.

    Literals are found with the same string skipper the scanner uses and
    decoded by tomllib, so ``"A"`` and ``'A'`` both match and a ``#`` or
    comma inside a string is never mistaken for structure.
    """
    i = 0
    n = len(rendered)
    while i < n:
        c = rendered[i]
        if c == "#":
            nl = rendered.find("\n", i)
            i = n if nl == -1 else nl
            continue
        if c in "\"'":
            end = _skip_string(rendered, i)
            try:
                value = tomllib.loads(f"v = {rendered[i:end]}")["v"]
            except Exception:
                value = None
            if value == name:
                return i, end
            i = end
            continue
        i += 1
    return None


def upsert_entry(text: str, section: str, name: str, rendered: str) -> str:
    """*text* with ``name = rendered`` set in *section*.

    Replaces the value in place when the entry exists (one span, so
    comments and neighbours survive), otherwise appends it at the end of
    the section, creating the section header if the file has none.
    """
    span = find_entry_span(text, section, name)
    if span is not None:
        return splice(text, span, rendered)

    line = f"{name} = {rendered}\n"
    insert_at = _section_insert_offset(text, section)
    if insert_at is None:
        prefix = "" if text.endswith("\n") or not text else "\n"
        return f"{text}{prefix}\n[{section}]\n{line}"
    # The entry must start its own line. A section that ends the file
    # without a trailing newline puts the insertion point at the END of its
    # last entry, and gluing onto it -- `a = "x"b = ""` -- made the whole
    # file unparseable (scistack.log 2026-10-17, "line 12, column 79").
    if insert_at > 0 and text[insert_at - 1] != "\n":
        line = "\n" + line
    return text[:insert_at] + line + text[insert_at:]


def checked_text(text: str, path: "Path | str | None" = None) -> str:
    """*text*, after confirming it still parses as TOML; ``ValueError`` if not.

    The one gate every GUI write of the entities file passes before the
    bytes reach disk. An unparseable file is rejected whole by :func:`load`,
    so writing one silently drops EVERY declaration in it, not just the one
    being edited. Refusing here leaves the file as it was.
    """
    try:
        tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        line = _decode_error_line(e)
        lines = text.splitlines()
        offending = lines[line - 1] if 0 < line <= len(lines) else ""
        Log.error(
            "[entities] refusing to write %s: the result would not be valid "
            "TOML (%s); line %d would read %r",
            path or "the entities file",
            e,
            line,
            offending,
        )
        raise ValueError(
            f"The edit would make the entities file invalid TOML ({e}), so "
            "nothing was written."
        ) from e
    return text


def _section_insert_offset(text: str, section: str) -> "int | None":
    """Offset to insert a new entry at the end of *section*'s block, or
    ``None`` if the section header is absent.

    Trailing blank lines belong to the gap before the next section, not to
    this one, so the insertion point backs up over them -- otherwise every
    added entry drifts one line further from its neighbours.
    """
    header = None
    i = 0
    n = len(text)
    while i < n:
        line_end = text.find("\n", i)
        if line_end == -1:
            line_end = n
        stripped = text[i:line_end].strip()
        if stripped.startswith("["):
            name = stripped.split("#", 1)[0].strip().strip("[]").strip().strip("\"'")
            if header is None and name == section:
                header = line_end + 1
            elif header is not None:
                return _back_up_over_blanks(text, i)
        i = line_end + 1
    if header is None:
        return None
    return _back_up_over_blanks(text, n)


def _back_up_over_blanks(text: str, offset: int) -> int:
    while offset > 0:
        prev_start = text.rfind("\n", 0, offset - 1) + 1
        if text[prev_start:offset].strip():
            break
        offset = prev_start
    return offset


def add_variable(text: str, name: str) -> str:
    """*text* with *name* added to the ``variables`` array.

    Appends before the closing bracket of a multi-line array so interior
    comments survive; re-renders a single-line array multi-line, which is
    the shape every array grows into anyway. A file with no ``variables``
    key at all gets one above the first section header -- never below,
    where TOML would bind it to that table.
    """
    span = find_entry_span(text, None, VARIABLES)
    if span is None:
        entry = f"{VARIABLES} = [\n    {render_value(name)},\n]\n"
        insert_at = _first_section_offset(text)
        prefix = "" if insert_at == 0 or text[:insert_at].endswith("\n\n") else "\n"
        return text[:insert_at] + prefix + entry + "\n" + text[insert_at:]

    current = text[span.start : span.end]
    if name in _parse_name_array(current):
        Log.debug("[entities] Variable %s is already declared; not re-adding", name)
        return text

    if "\n" in current:
        close = current.rfind("]")
        head = _terminate_last_element(current[:close].rstrip())
        replacement = f"{head}\n    {render_value(name)},\n{current[close:]}"
    else:
        names = [*_parse_name_array(current), name]
        inner = "".join(f"    {render_value(v)},\n" for v in names)
        replacement = f"[\n{inner}]"
    return splice(text, span, replacement)


def _terminate_last_element(head: str) -> str:
    """*head* (an array body with its closing bracket removed) with a
    trailing comma after its last element, if it needs one.

    The comma goes after the *code*, not after the line: appending it
    blindly would push it inside a trailing ``# comment`` and edit the
    user's prose instead of the array.
    """
    lines = head.split("\n")
    last = lines[-1]
    code = _code_part(last).rstrip()
    if code.endswith(",") or code.endswith("["):
        return head
    lines[-1] = last[: len(code)] + "," + last[len(code) :]
    return "\n".join(lines)


def _code_part(line: str) -> str:
    """*line* up to its first comment ``#``, ignoring one inside a string."""
    in_string: str | None = None
    i = 0
    while i < len(line):
        ch = line[i]
        if in_string is not None:
            if ch == "\\" and in_string == '"':
                i += 2  # skip the escaped character, which may be the quote
                continue
            if ch == in_string:
                in_string = None
        elif ch in "\"'":
            in_string = ch
        elif ch == "#":
            return line[:i]
        i += 1
    return line


def _parse_name_array(rendered: str) -> list[str]:
    """The names in an already-rendered ``variables`` array. Falls back to
    ``[]`` for anything unparseable -- a caller uses this to avoid a
    duplicate, and the loader rejects duplicates anyway."""
    try:
        value = tomllib.loads(f"v = {rendered}")["v"]
    except Exception:
        return []
    return [v for v in value if isinstance(v, str)] if isinstance(value, list) else []


def _first_section_offset(text: str) -> int:
    """Offset of the first ``[section]`` header line, or the end of the
    text. Where a top-level key can still safely go."""
    i = 0
    n = len(text)
    while i < n:
        line_end = text.find("\n", i)
        if line_end == -1:
            line_end = n
        if text[i:line_end].strip().startswith("["):
            return i
        i = line_end + 1
    return n
