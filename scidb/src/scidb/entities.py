"""
The entities file: declaring Variables, Parameters and PathInputs in TOML.

This is the single writable declaration surface for all three kinds, in
both languages -- it replaces the Python ``variable_file`` (a ``.py``
module that was *executed* to get its declarations) and the MATLAB
``[matlab] entities_file`` (a ``.m`` script that was *statically parsed*).
One file, one format, read identically by Python and, over the bridge, by
MATLAB. See ``.claude/plan-entities-toml-26-08-31.md`` and
``docs/claude/entity-editability-model.md``.

It lives in ``scidb`` because ``scidb`` owns the entity classes --
``BaseVariable``, ``Parameter``, and (re-exported from scifor)
``PathInput``/``EachOf`` -- so it is the layer that knows what a
declaration of one means (CLAUDE.md NOTE 3). ``scistack_gui`` keeps the
*policy* around writing (which file may be written, staleness, atomic
replacement, rollback); this module owns the *grammar*, exactly as
``source_edit`` does for the Python form.

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

Rules worth stating once, because they are what make the format
unambiguous:

- **An array is always the alternative list.** ``[10, 20, 30]`` is three
  Parameter values; a Parameter whose single value is genuinely a list
  nests it as ``[[1, 2, 3]]``. The EMPTY array is a Parameter declared but
  not yet given a value -- legal here, refused at ``for_each`` expansion
  (see ``parameter.py``); it is what the GUI writes for a new Parameter,
  which used to be a placeholder ``0`` indistinguishable from a real value.
- **An inline table means the value under ``[parameters]``, and the field
  set under ``[path_inputs]``.** A PathInput has no dict-shaped value, so
  a table there is unambiguously ``{template, root_folder}``.
- **Values are never re-parsed.** TOML types are taken as they come, so a
  zero-padded ``"01"`` stays the string ``"01"`` -- there is no literal
  parser in the path to turn it into ``1``
  (``feedback_zero_padded_schema_keys``).
- **Anything this shape cannot say stays in Python, read-only**: custom
  ``to_db``/``from_db``, a non-default ``schema_version``, PathInput
  ``aliases``/``key_regex``/``regex``, or computed Parameter values.

Errors are **per entry**: one bad declaration is recorded with its name
and line and skipped, never raised. The ``.py`` entities file it replaces
could not do that -- an exception anywhere in it took every entity in the
file down at once, silently (module-load failures are logged at DEBUG).
"""

from __future__ import annotations

import keyword
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scifor import EachOf, PathInput
from scifor.discovery import (
    find_project_config,
    read_scistack_section,
    resolve_config_path,
)
from scistacklog import Log

from .parameter import Parameter
from .source_edit import Span, line_number, splice
from .variable import BaseVariable

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

DEFAULT_ENTITIES_FILENAME = "scistack_entities.toml"
DEFAULT_ENTITIES_RELPATH = Path("src") / DEFAULT_ENTITIES_FILENAME

_PATH_INPUT_KEYS = frozenset({"template", "root_folder"})


# ---------------------------------------------------------------------------
# Results
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


@dataclass
class EntitiesFile:
    """Everything one entities file declares, plus what it got wrong.

    ``variables`` holds dynamically-created ``BaseVariable`` subclasses;
    creating one registers it in ``BaseVariable._all_subclasses`` through
    ``__init_subclass__``, exactly as a ``class X(BaseVariable)`` statement
    in a module would.
    """

    path: Path
    variables: dict[str, type] = field(default_factory=dict)
    parameters: dict[str, Parameter] = field(default_factory=dict)
    path_inputs: dict[str, Any] = field(default_factory=dict)
    lines: dict[str, int] = field(default_factory=dict)
    errors: list[EntityError] = field(default_factory=list)

    def names(self) -> list[str]:
        """Every successfully-declared name, in section order."""
        return [*self.variables, *self.parameters, *self.path_inputs]

    def get(self, name: str) -> Any:
        """The declared object for *name*, or ``None``.

        The lookup form MATLAB uses over the bridge, and the fallback for a
        Python name that cannot be reached by attribute access (see
        :func:`__getattr__`).
        """
        for group in (self.variables, self.parameters, self.path_inputs):
            if name in group:
                return group[name]
        return None

    def as_dict(self) -> dict[str, Any]:
        """Flat ``{name: object}`` of everything declared -- what
        ``+scidb/entities.m`` marshals into a MATLAB struct."""
        return {name: self.get(name) for name in self.names()}


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def load(path: "Path | str") -> EntitiesFile:
    """Parse and construct every entity declared in *path*.

    A missing file is not an error -- it is a project that has not created
    an entity yet, and returns an empty result. A file that will not parse
    as TOML at all yields one error carrying the parser's own line number,
    because there are no entries to attribute individual failures to.
    """
    path = Path(path)
    result = EntitiesFile(path=path)

    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        Log.debug("[entities] %s does not exist yet; nothing to load", path)
        return result
    except OSError as e:
        Log.error("[entities] Cannot read %s: %s", path, e)
        result.errors.append(EntityError(str(path), 0, f"cannot read file: {e}"))
        return result

    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        Log.error("[entities] %s is not valid TOML: %s", path, e)
        result.errors.append(
            EntityError(path.name, _decode_error_line(e), f"invalid TOML: {e}")
        )
        return result

    spans = _scan_entries(text)
    for key, span in spans.items():
        result.lines[key] = line_number(text, span.start)

    _load_variables(data, text, result)
    _load_parameters(data, result)
    _load_path_inputs(data, result)

    Log.info(
        "[entities] Loaded %s: %d variable(s), %d parameter(s), "
        "%d path input(s), %d rejected",
        path,
        len(result.variables),
        len(result.parameters),
        len(result.path_inputs),
        len(result.errors),
    )
    for err in result.errors:
        Log.warn("[entities] %s: rejected %s", path, err.describe())
    _warn_shadowed_names(result)
    return result


def _warn_shadowed_names(result: EntitiesFile) -> None:
    """Warn about a name this module already defines.

    Module ``__getattr__`` is consulted only after normal lookup fails, so
    an entity called ``load`` or ``names`` would silently resolve to the
    function of that name on ``scidb.entities``. The declaration is
    perfectly valid everywhere else (the canvas, the registry,
    ``.get(name)``) -- only attribute access is shadowed, so this warns
    rather than rejects.
    """
    for name in result.names():
        if name in globals():
            Log.warn(
                "[entities] %s: '%s' shadows scidb.entities.%s, so "
                "scidb.entities.%s returns the module's own attribute -- "
                "reach the entity with load_for_project().get('%s') or rename it",
                result.path,
                name,
                name,
                name,
                name,
            )


def _decode_error_line(exc: Exception) -> int:
    """``tomllib``'s decode errors carry a ``lineno`` on 3.11+; older
    backports only put it in the message. Missing is 0, meaning "no
    location", never a misleading line 1."""
    return int(getattr(exc, "lineno", 0) or 0)


def _load_variables(data: dict, text: str, result: EntitiesFile) -> None:
    """``variables = ["A", "B"]`` -> a ``BaseVariable`` subclass per name."""
    for section in SECTIONS:
        if isinstance(data.get(section), dict) and VARIABLES in data[section]:
            # TOML binds a bare key to whatever table precedes it, so a
            # `variables` list written below [parameters] silently becomes
            # parameters.variables and every name in it vanishes. Say so.
            result.errors.append(
                EntityError(
                    VARIABLES,
                    result.lines.get(f"{section}.{VARIABLES}", 0),
                    f"'{VARIABLES}' is inside [{section}] -- move it above the "
                    f"first section header, or it binds to that table",
                )
            )

    raw = data.get(VARIABLES)
    if raw is None:
        return
    line = result.lines.get(VARIABLES, 0)
    if not isinstance(raw, list):
        result.errors.append(
            EntityError(VARIABLES, line, f"must be a list of names, got {type(raw).__name__}")
        )
        return

    for entry in raw:
        if not isinstance(entry, str):
            result.errors.append(
                EntityError(
                    str(entry), line, f"variable names must be strings, got "
                    f"{type(entry).__name__}"
                )
            )
            continue
        name = entry.strip()
        err = _name_error(name, result)
        if err is not None:
            result.errors.append(EntityError(name, line, err))
            continue
        result.variables[name] = _make_variable_class(name)
        result.lines.setdefault(name, line)
        Log.debug("[entities] Declared variable %s", name)


def _make_variable_class(name: str) -> type:
    """A ``BaseVariable`` subclass named *name*, created dynamically.

    Built through ``type(BaseVariable)`` rather than ``type`` directly:
    ``BaseVariable`` carries the ``VariableMeta`` metaclass (class-level
    comparison operators -- ``Side == "L"`` builds a filter), and calling
    plain ``type`` with a metaclassed base raises "metaclass conflict".
    Going through the base's own metaclass gives a class indistinguishable
    from a ``class X(BaseVariable)`` statement, including registration via
    ``__init_subclass__`` and a working ``==``.
    """
    return type(BaseVariable)(name, (BaseVariable,), {"__module__": __name__})


def _load_parameters(data: dict, result: EntitiesFile) -> None:
    """``NAME = value`` / ``NAME = [v1, v2]`` -> ``Parameter(*values)``."""
    section = data.get(PARAMETERS)
    if section is None:
        return
    if not isinstance(section, dict):
        result.errors.append(
            EntityError(PARAMETERS, 0, f"[{PARAMETERS}] must be a table")
        )
        return

    for name, raw in section.items():
        if name == VARIABLES:
            continue  # already reported by _load_variables
        line = result.lines.get(f"{PARAMETERS}.{name}", 0)
        err = _name_error(name, result)
        if err is not None:
            result.errors.append(EntityError(name, line, err))
            continue
        # An array is the alternative list; everything else -- including an
        # inline table -- is one value as-is.
        #
        # An EMPTY array is not an error: `NAME = []` is a Parameter that has
        # been declared but not yet given a value, which is what the GUI's
        # "New parameter" form writes (it collects a name and nothing else).
        # It is refused at for_each expansion, not here -- see parameter.py.
        values = list(raw) if isinstance(raw, list) else [raw]
        param = Parameter(*values)
        param.source_file = str(result.path)
        param.source_line = line
        result.parameters[name] = param
        result.lines[name] = line
        Log.debug("[entities] Declared parameter %s with %d value(s)", name, len(values))


def _load_path_inputs(data: dict, result: EntitiesFile) -> None:
    """``NAME = template`` / ``{template, root_folder}`` / a list of either
    -> ``PathInput`` or ``EachOf(PathInput, ...)``."""
    section = data.get(PATH_INPUTS)
    if section is None:
        return
    if not isinstance(section, dict):
        result.errors.append(
            EntityError(PATH_INPUTS, 0, f"[{PATH_INPUTS}] must be a table")
        )
        return

    for name, raw in section.items():
        if name == VARIABLES:
            continue
        line = result.lines.get(f"{PATH_INPUTS}.{name}", 0)
        err = _name_error(name, result)
        if err is not None:
            result.errors.append(EntityError(name, line, err))
            continue

        # A list is alternate templates -- the EachOf-of-PathInputs form
        # the registry scanner already accepts. Only portability's importer
        # writes one, but the format has to be able to hold it.
        raw_alts = raw if isinstance(raw, list) else [raw]
        if not raw_alts:
            result.errors.append(
                EntityError(name, line, "a PathInput needs at least one template")
            )
            continue

        arms, arm_err = _build_path_input_arms(raw_alts)
        if arm_err is not None:
            result.errors.append(EntityError(name, line, arm_err))
            continue

        obj = arms[0] if len(arms) == 1 else EachOf(*arms)
        result.path_inputs[name] = obj
        result.lines[name] = line
        Log.debug(
            "[entities] Declared path input %s with %d template(s)", name, len(arms)
        )


def _build_path_input_arms(raw_alts: list) -> "tuple[list, str | None]":
    """``(arms, error)`` -- exactly one is meaningful."""
    arms = []
    for alt in raw_alts:
        if isinstance(alt, str):
            arms.append(PathInput(alt))
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
                f"only template and root_folder here; declare richer forms "
                f"(aliases, key_regex, regex) in Python"
            )
        template = alt.get("template")
        if not isinstance(template, str) or not template:
            return [], "missing a 'template' string"
        root = alt.get("root_folder")
        if root is not None and not isinstance(root, str):
            return [], f"root_folder must be a string, got {type(root).__name__}"
        arms.append(PathInput(template, root_folder=root))
    return arms, None


def _name_error(name: str, result: EntitiesFile) -> "str | None":
    """Why *name* cannot be declared, or ``None``.

    Duplicates are checked across all three kinds together: the name is how
    every consumer -- the canvas, the registry, ``scidb.entities.X`` --
    refers to the entity, so the same name meaning two things has no
    resolution.
    """
    if not name or not name.isidentifier() or keyword.iskeyword(name):
        return f"'{name}' is not a valid Python identifier"
    if name.startswith("_"):
        return "names must not start with an underscore"
    if result.get(name) is not None:
        return "duplicate name -- already declared in this file"
    return None


# ---------------------------------------------------------------------------
# Locating the project's entities file
# ---------------------------------------------------------------------------


def _project_config(start: "Path | str | None") -> "Path | None":
    """The ``scistack.toml``/``pyproject.toml`` governing *start* (default:
    cwd), or ``None`` -- with the one debug log both callers below want."""
    config = find_project_config(Path(start) if start is not None else Path.cwd())
    if config is None:
        Log.debug("[entities] No project config found from %s", start or Path.cwd())
    return config


def project_root(start: "Path | str | None" = None) -> "Path | None":
    """The root of the project containing *start* (default: cwd) -- the
    directory holding its ``scistack.toml``/``pyproject.toml``, or ``None``
    when *start* is not inside a project.

    Shared so callers that need the root but not the entities file (the
    MATLAB bridge pinning ``scifor``'s PathInput resolution base) don't
    re-derive "the config file's parent" for themselves.
    """
    config = _project_config(start)
    return config.parent if config is not None else None


def resolve_entities_path(
    root: "Path | str", section: "dict | None"
) -> "Path | None":
    """The entities file of a project rooted at *root* whose scistack
    section is *section*.

    The rule itself, factored out of :func:`entities_path` so that a caller
    which has *already* located the project config and parsed its section --
    ``scistack_gui.config.load_config`` -- asks this function rather than
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
    :func:`scifor.discovery.resolve_config_path`, so a value written by a
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
    """The entities file for the project containing *start* (default: cwd).

    Locates the project config, then applies :func:`resolve_entities_path`
    -- which is where the rule lives, and is documented.
    """
    config = _project_config(start)
    if config is None:
        return None
    return resolve_entities_path(config.parent, read_scistack_section(config))


_cache: "dict[str, tuple[float, EntitiesFile]]" = {}


def load_for_project(start: "Path | str | None" = None) -> EntitiesFile:
    """The loaded entities of the project containing *start*.

    Cached on the file's mtime, so repeated attribute access costs one
    ``stat`` rather than a re-parse, while an edit (from the GUI or by
    hand) is picked up on the next access with nothing to invalidate
    manually. This is also the entry point ``+scidb/entities.m`` calls over
    the bridge -- MATLAB has no TOML reader, and every other ``+scidb``
    entry point already routes through Python.
    """
    path = entities_path(start)
    if path is None:
        return EntitiesFile(path=Path())

    key = str(path)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    cached = _cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    loaded = load(path)
    _cache[key] = (mtime, loaded)
    return loaded


def clear_cache() -> None:
    """Drop the mtime cache. For tests and for a caller that has just
    written the file and cannot wait for mtime resolution to tick."""
    _cache.clear()


def __getattr__(name: str) -> Any:
    """``from scidb import entities; entities.WINDOW_SECONDS``.

    Module-level ``__getattr__`` (PEP 562) is only consulted after normal
    lookup fails, so the module's own functions always win -- which is why
    an entity named ``load`` or ``names`` is warned about at load time and
    reachable only through ``load_for_project().get(...)``.
    """
    if name.startswith("__"):
        raise AttributeError(name)
    obj = load_for_project().get(name)
    if obj is None:
        raise AttributeError(
            f"No entity named {name!r} is declared in this project's entities "
            f"file. Declared: {', '.join(load_for_project().names()) or '(none)'}"
        )
    return obj


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
    spans: dict[str, Span] = {}
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
        key, value_start = eq
        value_end = _value_end(text, value_start)
        qualified = f"{section}.{key}" if section else key
        spans[qualified] = Span(value_start, value_end)
        i = max(value_end, line_end) + 1

    return spans


def _key_assignment_end(text: str, start: int, line_end: int) -> "tuple[str, int] | None":
    """``(key, offset-of-value)`` for a ``key = `` at *start*, else None."""
    line = text[start:line_end]
    eq = line.find("=")
    if eq == -1:
        return None
    key = line[:eq].strip().strip("\"'")
    if not key or "." in key:
        # Dotted keys (`a.b = 1`) address a nested table; nothing in this
        # format uses them, and pretending otherwise would mis-key the span.
        return None
    value_start = start + eq + 1
    while value_start < line_end and text[value_start] in " \t":
        value_start += 1
    return key, value_start


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
) -> str:
    """A PathInput's RHS: a bare template string when that is all there is,
    a ``{template, root_folder}`` table when there is a root, an array of
    either when there are alternates."""
    arms = [_path_input_arm(template, root_folder)]
    arms.extend(
        _path_input_arm(alt.get("template", ""), alt.get("root_folder"))
        for alt in (alternates or [])
    )
    if len(arms) == 1:
        return arms[0]
    return "[" + ", ".join(arms) + "]"


def _path_input_arm(template: str, root_folder: "str | None") -> str:
    if not root_folder:
        return render_value(template)
    return render_value({"template": template, "root_folder": root_folder})


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
    return text[:insert_at] + line + text[insert_at:]


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
