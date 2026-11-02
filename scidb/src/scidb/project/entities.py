"""
The declared entities of a project, as scidb objects.

This is the MATERIALIZE half of the entities file. The grammar -- locating
the file, parsing it, scanning, rendering and editing the text -- is
``scifor.project.entities_file``, which returns plain frozen dataclasses
(``DeclaredEntities``). This module turns those into the objects a
pipeline uses: a ``BaseVariable`` subclass per declared Variable, a
``Parameter`` per declared Parameter, and a ``PathInput`` (or ``EachOf`` of
them) per declared PathInput. It lives in ``scidb`` because ``scidb`` owns
those classes (CLAUDE.md NOTE 3).

It also owns the per-project cache (so a declared Variable is ONE class
for the life of the process, not a new one per read), attribute access
(``from scidb import entities; entities.WINDOW_SECONDS``), and the
recorded spelling of a declared Parameter value (:func:`declared_spelling`).

Errors stay **per entry**: a declaration ``PathInput`` itself refuses (an
alias key that is not a placeholder, a ``key_regex`` that does not
compile) is recorded with its name and line beside the parse errors, and
only that entry is skipped.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scifor import EachOf, PathInput
from scifor.project.entities_file import (
    DeclaredEntities,
    DeclaredPathInput,
    EntityError,
    entities_path,
    read_entities_file,
)
from scistacklog import Log

from ..parameter import Parameter
from ..variable import BaseVariable


@dataclass
class EntitiesFile:
    """Everything one entities file declares, materialized, plus what it got
    wrong.

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
    #: ``[library] schema_keys``, or ``None`` when the file has no
    #: ``[library]`` table (a project's own file).
    library_schema_keys: list[str] | None = None

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
# Materializing
# ---------------------------------------------------------------------------


def load(path: Path | str) -> EntitiesFile:
    """Parse the entities file at *path* and construct every entity in it."""
    return materialize(read_entities_file(path))


def materialize(declared: DeclaredEntities) -> EntitiesFile:
    """The scidb objects *declared* names, with every rejection recorded."""
    result = EntitiesFile(
        path=declared.path,
        lines=dict(declared.lines),
        errors=list(declared.errors),
        library_schema_keys=(
            None if declared.library_schema_keys is None else list(declared.library_schema_keys)
        ),
    )
    for variable in declared.variables:
        result.variables[variable.name] = _make_variable_class(variable.name)
    for declared_param in declared.parameters:
        result.parameters[declared_param.name] = _make_parameter(
            declared_param.name, declared_param.values, declared_param.line, declared.path
        )
    for declared_input in declared.path_inputs:
        try:
            result.path_inputs[declared_input.name] = _make_path_input(declared_input)
        except ValueError as exc:
            # PathInput owns the rest of the validation: an alias key that is
            # not a placeholder, a spelling claimed by two canonicals, a
            # key_regex pattern that does not compile or captures.
            result.errors.append(EntityError(declared_input.name, declared_input.line, str(exc)))
            result.lines.pop(declared_input.name, None)

    Log.info(
        "[entities] Loaded %s: %d variable(s), %d parameter(s), "
        "%d path input(s), %d rejected",
        result.path,
        len(result.variables),
        len(result.parameters),
        len(result.path_inputs),
        len(result.errors),
    )
    for err in result.errors:
        Log.warn("[entities] %s: rejected %s", result.path, err.describe())
    _warn_shadowed_names(result)
    return result


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
    # `_declared_only`: a TOML declaration names a type, it does not define a
    # codec, so it agrees with any definition of the name
    # (`variable.same_definition`).
    return type(BaseVariable)(
        name, (BaseVariable,), {"__module__": __name__, "_declared_only": True}
    )


def _make_parameter(name: str, values: tuple, line: int, path: Path) -> Parameter:
    param = Parameter(*values)
    param.source_file = str(path)
    param.source_line = line
    # The declared name travels with the object so a run records which
    # Parameter fed each argument (parameter.declared_input_names).
    param.name = name
    return param


def _make_path_input(declared: DeclaredPathInput) -> Any:
    """``PathInput``, or ``EachOf(PathInput, ...)`` for alternate templates.
    The TOML key IS the name (``name=``, the PathInput's identity); every arm
    of an EachOf carries it. Raises ``ValueError`` for what PathInput
    refuses."""
    arms = [
        PathInput(
            arm.template,
            root_folder=arm.root_folder,
            aliases=arm.aliases,
            key_regex=arm.key_regex,
            name=declared.name,
        )
        for arm in declared.arms
    ]
    return arms[0] if len(arms) == 1 else EachOf(*arms)


def _warn_shadowed_names(result: EntitiesFile) -> None:
    """Warn about a name this module already defines.

    Module ``__getattr__`` is consulted only after normal lookup fails, so
    an entity called ``load`` or ``names`` would silently resolve to the
    function of that name on ``scidb.project.entities``. The declaration is
    perfectly valid everywhere else (the canvas, the registry,
    ``.get(name)``) -- only attribute access is shadowed, so this warns
    rather than rejects.
    """
    for name in result.names():
        if name in globals():
            Log.warn(
                "[entities] %s: '%s' shadows scidb.project.entities.%s, so "
                "scidb.project.entities.%s returns the module's own attribute -- "
                "reach the entity with load_for_project().get('%s') or rename it",
                result.path,
                name,
                name,
                name,
                name,
            )


# ---------------------------------------------------------------------------
# The project's entities, cached
# ---------------------------------------------------------------------------


_cache: "dict[str, tuple[float, EntitiesFile]]" = {}


def load_for_project(start: "Path | str | None" = None) -> EntitiesFile:
    """The loaded entities of the project rooted at *start*.

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


def declared_spelling(
    inputs: "dict[str, Any]",
    parameter_names: "dict[str, str] | None",
    declared: "dict[str, Parameter] | None" = None,
) -> "dict[str, Any]":
    """*inputs* with each Parameter-fed value replaced by the DECLARED value
    it equals, when the two are spelled differently.

    THE owner of "is this the same Parameter value" for recording. MATLAB has
    no integers: a declared ``formulaNum = 6`` reaches ``for_each`` from MATLAB
    as ``6.0``. Recorded as given, that value was ``"6.0"`` in history but
    ``6`` in the declaration, so the canvas drew two "6" rows
    (scistack.log 2026-10-01). It was also a different constant identity
    (``constants_identity_key`` is repr-based) from a Python run of the same
    declared ``6``. The declaration owns the value; the transport's spelling
    is not part of it.

    Applied wherever a call's identity is first derived from its inputs: the
    top of ``_for_each_prepare`` (both run paths), and the MATLAB bridge
    before it builds its skip hook. It is idempotent, so applying it twice is
    harmless.

    *declared* is ``{declared name: Parameter}``; it defaults to the project's
    entities file (:func:`load_for_project`), the only place a
    MATLAB project declares Parameters. A value with no declared Parameter,
    no equal declared value, or one already spelled as declared is left
    exactly as it is.
    """
    if not parameter_names:
        return inputs
    if declared is None:
        try:
            declared = load_for_project().parameters
        except Exception as exc:  # noqa: BLE001 - recording must not fail on this
            Log.warn(
                f"[parameter] declared_spelling: could not read declared "
                f"Parameters ({type(exc).__name__}: {exc}); values recorded as passed"
            )
            return inputs
    out = dict(inputs)
    for arg, name in parameter_names.items():
        if arg not in out:
            continue
        value = out[arg]
        if isinstance(value, EachOf):
            continue
        param = declared.get(name)
        if param is None:
            continue
        match = _declared_equal(value, param.values)
        if match is _NO_MATCH:
            continue
        out[arg] = match
        Log.info(
            f"[parameter] {name} (argument {arg!r}): recording the declared "
            f"value {match!r} ({type(match).__name__}) for the passed "
            f"{value!r} ({type(value).__name__})"
        )
    return out


_NO_MATCH = object()


def _declared_equal(value: Any, declared_values: list) -> Any:
    """The declared value equal to *value* but spelled differently, or
    ``_NO_MATCH``. An identical spelling (same ``repr``) is no change, so it
    returns ``_NO_MATCH`` too. ``bool`` never matches a number: ``True == 1``
    holds in Python, but they are different Parameter values."""
    if any(repr(d) == repr(value) for d in declared_values):
        return _NO_MATCH
    for d in declared_values:
        if isinstance(d, bool) != isinstance(value, bool):
            continue
        try:
            if bool(d == value):
                return d
        except Exception:  # noqa: BLE001 - e.g. an array compared to a list
            continue
    return _NO_MATCH


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
