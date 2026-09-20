"""
Generate ready-to-paste MATLAB commands for running pipeline functions.

The generated script configures the database, registers variable types,
and calls ``scidb.for_each`` with the correct inputs, outputs, and
schema arguments — all formatted as MATLAB syntax.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Preamble timing instrumentation
#
# Measured from a real session's scidb.log: a short MATLAB run spent ~4.4s
# between the GUI emitting this script and MATLAB's FIRST scidb log line, then
# ~0.43s of logged scidb work. Everything in that first window — VS Code
# writing/dispatching the temp script, MATLAB parsing it, the pyenv preamble,
# the addpath block — was unlogged, so it could not be attributed to any one
# cause. These lines split it up.
#
# `script_start` is a wall-clock stamp in scidb.log's own format
# (`yyyy-mm-dd HH:MM:SS.FFF`) so it can be diffed directly against the
# extension's `runInMatlabTerminal` timestamps — that difference IS the
# dispatch latency, which nothing else can observe.
#
# Per-section lines use `fprintf` because the early ones run before Python
# (and therefore scidb.Log) is reachable; the consolidated summary goes
# through scidb.Log.info in the same `[timing] name: TOTAL=…s (phase=…s)`
# shape scistacklog's Log.timer emits, so it greps out of scidb.log next to
# for_each's existing timing lines.
# ---------------------------------------------------------------------------


def _timing_init_lines() -> list[str]:
    """Start the script clock and stamp the wall clock.

    Names no section variable: a script that emits no pyenv preamble must
    contain no mention of pyenv at all (pinned by
    ``test_pyenv_preamble_omitted_when_none``), and the same reasoning holds
    for every other optional section. Each section's variable is created by
    the section itself — see :class:`_PreambleTiming`.
    """
    return [
        "% --- preamble timing --------------------------------------------",
        "% Wall-clock stamp first: the gap between the GUI dispatching this",
        "% script and this line is otherwise invisible in every log we keep.",
        "scistack_script_t0__ = tic;",
        "fprintf('[SciStack][timing] script_start %s\\n', "
        "datestr(now, 'yyyy-mm-dd HH:MM:SS.FFF'));",
        "",
    ]


class _PreambleTiming:
    """Times the generated script's preamble, section by section.

    Tracks which sections were actually emitted, so the summary and the
    closing ``clear`` name only those. A script generated without a pyenv
    preamble (``python_executable=None`` — the non-GUI callers) must not so
    much as mention pyenv: reporting ``pyenv=0.000s`` for a section that does
    not exist reads as "ran and was free" rather than "wasn't there", and it
    reintroduces the very word the omitted-preamble contract says will be
    absent.

    One instance per generated script; ``section`` must be called for every
    section before ``summary_lines``.
    """

    def __init__(self) -> None:
        self._emitted: list[tuple[str, str]] = []  # (variable, label)

    def section(self, body: list[str], var: str, label: str) -> list[str]:
        """``body`` wrapped in tic/toc, recording its duration into ``var``.

        An empty body emits nothing and records nothing.
        """
        if not body:
            return []
        self._emitted.append((var, label))
        return [
            "scistack_section_t0__ = tic;",
            *body,
            f"{var} = toc(scistack_section_t0__);",
            f"fprintf('[SciStack][timing] {label} %.3fs\\n', {var});",
            "",
        ]

    def summary_lines(self) -> list[str]:
        """One consolidated preamble line, emitted through scidb.Log.

        Placed after configure_database — the first point where both Python
        and the +scidb package are guaranteed reachable — and before the
        first for_each, so the summary lands in scidb.log immediately ahead
        of ``for_each_prepare returned in …``. Mirrors the
        ``[timing] name: TOTAL=…s (phase=…s, …)`` shape scistacklog's
        Log.timer emits, so one grep finds both.
        """
        if not self._emitted:
            return [
                "scidb.Log.info('[timing] matlab_preamble: TOTAL=%.3fs', ...",
                "    toc(scistack_script_t0__));",
                "",
            ]
        phases = ", ".join(f"{label}=%.3fs" for _, label in self._emitted)
        args = ", ".join(var for var, _ in self._emitted)
        return [
            "% Consolidated preamble timing (mirrors Log.timer's [timing] shape).",
            f"scidb.Log.info('[timing] matlab_preamble: TOTAL=%.3fs ({phases})', ...",
            f"    toc(scistack_script_t0__), {args});",
            "",
        ]

    def total_lines(self) -> list[str]:
        """Whole-script total, then clear the instrumentation's variables.

        Emitted after the try/catch block, so it is reached on the success
        path only — the catch rethrows, and a failed run's duration is not a
        measurement worth recording. ``run(...)`` evaluates in the caller's
        workspace, so the temporaries are cleared rather than left sitting
        next to the user's own variables.
        """
        temps = ["scistack_script_t0__"]
        if self._emitted:
            temps.append("scistack_section_t0__")
            temps.extend(var for var, _ in self._emitted)
        return [
            "fprintf('[SciStack][timing] script_total %.3fs\\n', "
            "toc(scistack_script_t0__));",
            "scidb.Log.info('[timing] matlab_script: TOTAL=%.3fs', "
            "toc(scistack_script_t0__));",
            "clear " + " ".join(temps) + ";",
        ]


def _entities_script_lines(
    entities_script: "str | None",
    entities_file: "str | None" = None,
    project_root: "str | None" = None,
) -> list[str]:
    """Lines that bring declared entities into scope, or ``[]``.

    Two sources, because two coexist: the TOML entities file (the one the
    GUI writes, read through ``scidb.entities()``) and a legacy MATLAB
    entities script, which stays readable. Both are emitted when both are
    configured — they declare different names, and dropping either would
    make a name silently undefined at the point the generated command uses
    it.

    ``project_root`` is passed to ``scidb.entities`` explicitly. Without it
    MATLAB resolves the project by walking up from its own **cwd**, which is
    wherever the user's MATLAB happens to be sitting — outside the project
    that finds no config at all, and every declared Parameter/PathInput is
    then silently out of scope (the load logs ``0 variable(s), 0
    parameter(s), 0 path input(s) ... from .``). It is also what tells the
    self-healing classdef materialization in ``+scidb/entities.m`` which
    project's entities file to read.

    Must be emitted AFTER the addpath block (both live on one of those
    directories) and before anything that references a declared entity by
    name.

    Re-run on every generated command rather than once per session. A
    MATLAB script is re-read from disk each time, and ``scidb.entities()``
    re-reads whenever the file's mtime changes, so either way a GUI entity
    edit is visible to a KEPT-WARM session (``matlab_sidecar``) with no
    cache-clearing. That property is why the entities file was a plain
    script rather than a classdef whose Constant properties would be cached
    for the life of the session; TOML keeps it — see
    docs/claude/entity-editability-model.md.
    """
    lines: list[str] = []
    if entities_file:
        call = (
            f"scidb.entities('{_escape_matlab_string(project_root)}');"
            if project_root
            else "scidb.entities();"
        )
        lines += [
            "% Entity declarations (Variables/Parameters/PathInputs) from "
            f"{Path(entities_file).name}",
            call,
            "",
        ]
    if entities_script:
        lines += [
            "% Entity declarations from the MATLAB entities script",
            f"{Path(entities_script).stem};",
            "",
        ]
    return lines




def _project_root_lines(
    project_root: "str | None", path_inputs: "dict | None" = None
) -> list[str]:
    """Lines pinning scifor's resolution base for rootless PathInputs, or ``[]``.

    A ``PathInput`` declared with no ``root_folder`` resolves its relative
    template against the nearest project root found by walking up from the
    **cwd**. Under MATLAB the cwd is a temp script directory, so that walk
    finds the wrong project or none at all and every relative template misses.

    The generated script therefore states the project explicitly. This is
    emitted for every command that knows its project root, not only ones with
    an entities file, so the guarantee never depends on unrelated config.
    ``scidb.entities`` pins the same thing for hand-written scripts (see
    ``scimatlab.bridge.set_pathinput_project_root``); pinning twice is
    idempotent.

    Must come after the pyenv preamble (it is a ``py.*`` call) and before the
    first ``for_each``. Deliberately NOT written into each PathInput's
    ``root_folder``: that is part of the recorded identity of the input, and
    rewriting it is what produced ``__unresolved__`` ghost nodes on the canvas
    — see ``_format_path_input``.
    """
    if not project_root:
        rootless = [
            p for p, pi in (path_inputs or {}).items() if not pi.get("root_folder")
        ]
        if rootless:
            logger.warning(
                "generate_matlab_command: no project root, but %s use a PathInput "
                "with no root_folder — those will resolve against MATLAB's cwd "
                "(a temp script dir) and their relative templates will not be found",
                ", ".join(sorted(rootless)),
            )
        return []
    return [
        "% Resolve rootless PathInput templates against this project, not MATLAB's cwd",
        "py.scimatlab.bridge.set_pathinput_project_root("
        f"'{_escape_matlab_string(project_root)}');",
        "",
    ]


def _unresolvable_var_types(var_types) -> list[str]:
    """Variable types the script is about to call as ``Type()`` that nothing
    in this project accounts for. Empty when everything resolves.

    A MATLAB variable resolves only if a classdef file for it is on the
    path. Two things can supply one: a .m file the registry parsed, or a
    declaration in the entities file, which ``+scidb/entities.m``
    materializes a stub for at the top of the generated script. A name with
    neither will fail at the ``for_each`` call with ``Unrecognized function
    or variable 'X'`` and no indication of why — which is exactly the
    failure this preflight exists to pre-empt, so it is reported at
    generation time, in the script, and in the log.

    Declared-but-not-yet-materialized names are NOT reported: the entities
    call fixes those before the run reaches them.

    **Advisory, never a refusal.** It is tempting to block the run on this
    -- on 2026-09-01 the warning fired at 15:33:03 and MATLAB failed at
    15:35:02 with ``Unrecognized function or variable 'Raw_EMG'``, two
    minutes of nothing. But this check is not authoritative and cannot be:
    it sees classdefs the GUI's registry happened to parse plus entities-file
    declarations, while a user's own ``startup.m`` can ``addpath`` a
    perfectly good ``RawEMG.m`` that nothing here will ever know about.
    Refusing would break that working setup.

    ``scimatlab.stubs`` states the same rule for the same reason: MATLAB's
    path is the only authority on whether a class resolves, which is why
    ``+scidb/entities.m`` asks MATLAB itself (``exist(name, 'class')``)
    rather than deciding from Python.

    So the answer is surfaced as loudly as possible -- in the log, as a
    comment in the script, and in the run console before MATLAB starts (see
    ``api/run.py``) -- and the authority stays with MATLAB.
    """
    names = sorted({str(t) for t in var_types if t})
    if not names:
        return []

    known: set[str] = set()
    try:
        from scistack_gui import matlab_registry

        known |= set(matlab_registry._matlab_variables)
        config = getattr(matlab_registry, "_config", None)
    except ImportError:  # pragma: no cover - GUI always has this
        config = None

    entities_file = getattr(config, "entities_file", None)
    if entities_file is not None:
        try:
            from scidb.entities import load as _load_entities

            known |= set(_load_entities(entities_file).variables)
        except Exception as e:
            logger.warning(
                "generate_matlab_command: could not read declared variables "
                "from %s (%s); preflight may over-report",
                entities_file,
                e,
            )

    return [n for n in names if n not in known]


def _unresolvable_var_type_lines(var_types) -> list[str]:
    """Comment lines naming the types that will not resolve, or ``[]``.

    Emitted into the generated script so a user reading or pasting it sees
    the problem in place. The run path surfaces the same answer in the run
    console — see :func:`unresolvable_var_type_warning`.
    """
    unknown = _unresolvable_var_types(var_types)
    if not unknown:
        return []

    logger.warning(
        "generate_matlab_command: no MATLAB classdef and no entity declaration "
        "for %s — this script will fail with 'Unrecognized function or "
        "variable' unless these resolve on the MATLAB path",
        ", ".join(unknown),
    )
    return [
        f"% WARNING: no classdef and no entity declaration found for: "
        f"{', '.join(unknown)}",
        "%          Create the variable in the GUI, or add its .m file to a "
        "configured MATLAB path,",
        "%          or this run will fail with 'Unrecognized function or "
        "variable'.",
        "",
    ]


def unresolvable_var_type_warning(var_types) -> "str | None":
    """A one-line, actionable warning naming the types that will not resolve,
    or ``None``.

    For the run console: the user is about to wait on MATLAB, so the thing
    they need in front of them is the name, where to declare it, and where
    the classdef would go. Advisory, not a gate -- see
    :func:`_unresolvable_var_types`.
    """
    unknown = _unresolvable_var_types(var_types)
    if not unknown:
        return None

    from scistack_gui import matlab_registry

    config = getattr(matlab_registry, "_config", None)
    entities_file = getattr(config, "entities_file", None)
    stub_dir = None
    try:
        from scimatlab.stubs import variable_stub_dir

        stub_dir = variable_stub_dir(getattr(config, "project_root", None))
    except Exception:  # pragma: no cover - reporting only
        pass

    detail = (
        f"{', '.join(unknown)} "
        f"{'has' if len(unknown) == 1 else 'have'} no MATLAB classdef and no "
        f"entity declaration that this project knows about. Unless "
        f"{'it resolves' if len(unknown) == 1 else 'they resolve'} on "
        f"MATLAB's own path, this run will fail with 'Unrecognized function "
        f"or variable'. Declare "
        f"{'it' if len(unknown) == 1 else 'them'} in "
        f"{entities_file or 'the project entities file'}"
        + (f" (a classdef stub is materialized under {stub_dir})" if stub_dir else "")
        + ", or add an existing .m classdef to a configured MATLAB path."
    )
    logger.warning("generate_matlab_command: %s", detail)
    return detail


def _matlab_signature_params(function_name: str) -> list[str]:
    """The MATLAB function's declared input parameter names, in signature
    order — or ``[]`` when this project has no parsed signature for it.

    Load-bearing for correctness, not for display. MATLAB has no keyword
    arguments, so ``+scifor/for_each.m`` binds the inputs struct to the
    function's arguments **by field order**::

        input_names = fieldnames(inputs);   % for_each.m
        ...
        call_args = loaded;                 % same order
        result = {fn(call_args{:})};

    The field NAMES are documentation; only their ORDER reaches the function.
    See :func:`_order_inputs_by_signature`.
    """
    try:
        from scistack_gui import matlab_registry
    except ImportError:  # pragma: no cover - GUI always has this
        return []
    try:
        if not matlab_registry.is_matlab_function(function_name):
            return []
        return list(matlab_registry.get_matlab_function(function_name).params)
    except Exception as e:  # pragma: no cover - registry shape is stable
        logger.warning(
            "generate_matlab_command: could not read %s's signature (%s) — "
            "inputs will be emitted in collection order, which MATLAB binds "
            "positionally",
            function_name,
            e,
        )
        return []


def _order_inputs_by_signature(
    function_name: str, inputs_dict: dict[str, str]
) -> dict[str, str]:
    """*inputs_dict* reordered to the MATLAB function's parameter order.

    The single place the emitted ``struct(...)`` field order is decided, for
    both the template branch and :func:`_for_each_call_lines`.

    Without this the struct came out in COLLECTION order — variables, then
    PathInputs, then Parameters, then constants, each in canvas-edge order —
    and ``for_each`` fed those to the function positionally. On 2026-09-02
    ``filterDelsys(loaded_data, config, Fs)`` was called with the sampling
    frequency in ``loaded_data`` and nothing in ``Fs``, because the two
    Parameter edges happened to resolve before the variable edge. Nothing on
    either side can detect that: MATLAB sees three well-typed positional
    arguments, so the failure surfaces (if at all) as a type error deep inside
    the user's function.

    Unknown signature -> insertion order is kept unchanged; the function may
    live only on MATLAB's own path, and reordering on a guess would be worse
    than leaving what the caller assembled.

    Params the signature does not name are appended after the known ones —
    they would shift every argument if interleaved, and they are warned about
    rather than dropped, since the registry's parse is not the authority on
    what MATLAB will accept.
    """
    params = _matlab_signature_params(function_name)
    if not params:
        logger.info(
            "generate_matlab_command: no parsed signature for %s — emitting "
            "inputs in collection order %s (MATLAB binds these positionally)",
            function_name,
            list(inputs_dict),
        )
        return dict(inputs_dict)

    ordered = {p: inputs_dict[p] for p in params if p in inputs_dict}
    extra = [k for k in inputs_dict if k not in ordered]
    for k in extra:
        ordered[k] = inputs_dict[k]

    if extra:
        logger.warning(
            "generate_matlab_command: %s: input(s) %s are not parameters of "
            "the parsed signature (%s) — appended last, but they will bind to "
            "whatever positional arguments follow",
            function_name,
            extra,
            params,
        )

    # A signature param left unbound BEFORE the last bound one is a gap, and a
    # gap shifts every argument after it onto the wrong parameter. A trailing
    # unbound param is legal MATLAB (the function can branch on nargin), so the
    # two are reported at different volumes.
    bound_positions = [i for i, p in enumerate(params) if p in ordered]
    last_bound = max(bound_positions) if bound_positions else -1
    gaps = [p for i, p in enumerate(params) if i < last_bound and p not in ordered]
    trailing = [p for i, p in enumerate(params) if i > last_bound and p not in ordered]
    if gaps:
        logger.warning(
            "generate_matlab_command: %s: parameter(s) %s have no wiring, but "
            "later parameter(s) do — MATLAB binds by position, so every "
            "argument after the gap will receive the WRONG value. Connect them "
            "on the canvas. Emitting %s for signature %s",
            function_name,
            gaps,
            list(ordered),
            params,
        )
    elif trailing:
        logger.info(
            "generate_matlab_command: %s: trailing parameter(s) %s left unbound "
            "— the function must default them via nargin",
            function_name,
            trailing,
        )

    logger.info(
        "generate_matlab_command: %s: inputs bound positionally as %s "
        "(signature %s)",
        function_name,
        [f"{i + 1}:{p}" for i, p in enumerate(ordered)],
        params,
    )
    return ordered


def _variable_binding_parts(ref) -> "tuple[list[str], list[str], bool]":
    """``(type_names, columns, iterate)`` for one ``variable_inputs`` entry.

    The ONE parser for the two shapes an entry may take:

    * ``"RawEMG"`` / ``["RawEMG", "RawVO2"]`` — the plain binding;
    * ``{"types": [...], "columns": [...], "iterate": bool}`` — the same
      binding with a GUI column selection on it.

    Both :func:`_variable_input_items` (which renders expressions) and
    :func:`_variable_input_type_names` (which the unresolvable-classdef
    preflight needs) go through here, so the map stays SINGLE. A parallel
    ``column_selections`` dict alongside ``variable_inputs`` would be the
    "one concept, two representations" trap this subsystem keeps falling into
    — the two then have to be threaded through three emit sites each and only
    one of them gets updated.
    """
    if isinstance(ref, dict):
        types = ref.get("types") or []
        if isinstance(types, str):
            types = [types]
        columns = ref.get("columns") or []
        if isinstance(columns, str):
            columns = [columns]
        return (
            [str(t) for t in types if t],
            [str(c) for c in columns if c],
            bool(ref.get("iterate")),
        )
    names = [ref] if isinstance(ref, str) else [n for n in (ref or []) if n]
    return [str(n) for n in names], [], False


def _format_variable_class(name: str, columns: list[str], iterate: bool) -> str:
    """One variable type as the MATLAB expression ``for_each`` loads from.

    MATLAB has no separate ``ColumnSelection`` wrapper — the column names go
    to the ``BaseVariable`` constructor (``selected_columns``) and ``iterate``
    comes from ``for_columns`` (``+scidb/BaseVariable.m``), so the four shapes
    are:

    ====================================  =================================
    Selection                             MATLAB
    ====================================  =================================
    none                                  ``Trials()``
    ``["filename"]``                      ``Trials("filename")``
    ``["a", "b"]``                        ``Trials(["a", "b"])``
    iterate, ``["a", "b"]``               ``Trials().for_columns(["a", "b"])``
    iterate, all columns                  ``Trials().for_columns()``
    ====================================  =================================
    """
    if iterate:
        if not columns:
            return f"{name}().for_columns()"
        return f"{name}().for_columns({_format_matlab_string_array(columns)})"
    if not columns:
        return f"{name}()"
    if len(columns) == 1:
        return f'{name}("{_escape_matlab_dq(columns[0])}")'
    return f"{name}({_format_matlab_string_array(columns)})"


def _format_variable_input(type_names, columns=None, iterate: bool = False) -> str:
    """A variable binding as the MATLAB expression ``for_each`` loads from:
    ``RawEMG()`` for one type, ``scifor.EachOf(A(), B())`` for several
    (mirrors ``execution_service.build_run_inputs``, which builds ``EachOf``
    for the same multi-type binding on the Python side).

    A column selection applies to EVERY alternative of a multi-type binding —
    one selection per PARAMETER, not per edge, matching the Python side.
    """
    names = [type_names] if isinstance(type_names, str) else list(type_names)
    cols = list(columns or [])
    if len(names) == 1:
        return _format_variable_class(names[0], cols, iterate)
    items = ", ".join(_format_variable_class(n, cols, iterate) for n in names)
    return f"scifor.EachOf({items})"


def _variable_input_items(variable_inputs: "dict | None"):
    """``(param_name, matlab_expression)`` pairs for a ``variable_inputs`` map.

    A binding with no type names is skipped rather than emitted: it would
    become ``scifor.EachOf()``, an axis with nothing to expand, and there is
    no value to bind anyway. Skipping shifts the argument it would have
    filled — :func:`_order_inputs_by_signature` reports that as a gap.
    (Constructing an empty ``EachOf`` is legal now, since that is what a
    value-less ``Parameter`` is; it is ``for_each`` expansion that refuses
    one, which would be a failure at run time rather than here.)
    """
    for param, ref in (variable_inputs or {}).items():
        names, columns, iterate = _variable_binding_parts(ref)
        if not names:
            logger.warning(
                "generate_matlab_command: variable binding for parameter %r "
                "names no type — leaving it unbound",
                param,
            )
            continue
        if columns or iterate:
            logger.info(
                "generate_matlab_command: parameter %r restricted to "
                "column(s) %s (iterate=%s)",
                param,
                columns or "<all data columns>",
                iterate,
            )
        yield param, _format_variable_input(names, columns, iterate)


def _variable_input_type_names(variable_inputs: "dict | None") -> list[str]:
    """Flat list of class names referenced by a ``variable_inputs`` map — for
    the unresolvable-classdef preflight, which needs names, not expressions."""
    names: list[str] = []
    for ref in (variable_inputs or {}).values():
        names.extend(_variable_binding_parts(ref)[0])
    return names


def _run_origin_lines() -> list[str]:
    """Label the runs this script starts as GUI runs.

    A command the GUI generated read the intent store (column selections, run
    options, hidden values are all baked into it), so its runs are ``gui``
    runs even though MATLAB executes them — rule 3 of
    docs/claude/intent-and-fact.md. Must come after the pyenv preamble (it
    is a ``py.*`` call); ``setenv`` would not do, because Python's
    ``os.environ`` is a snapshot taken at interpreter start.
    """
    return ["py.scidb.intent.set_ambient_origin('gui');"]


def _run_origin_reset_lines() -> list[str]:
    """Put the origin back to ``script`` once the generated run is over.

    ``set_ambient_origin`` has no scope to exit, and a MATLAB session outlives
    the script: without this, a user's own ``scidb.for_each`` typed at the
    prompt afterwards would be recorded as a GUI run. Emitted on BOTH the
    success path and the catch path, since the catch rethrows.
    """
    return ["py.scidb.intent.set_ambient_origin('script');"]


def generate_matlab_command(
    function_name: str,
    db_path: str,
    schema_keys: list[str],
    variants: list[dict] | None = None,
    schema_filter: dict[str, list] | None = None,
    schema_level: list[str] | None = None,
    addpath_dirs: list[str] | None = None,
    python_executable: str | None = None,
    path_inputs: dict[str, dict] | None = None,
    sweeps: dict[str, list] | None = None,
    output_types: list[str] | None = None,
    project_root: str | None = None,
    entities_script: str | None = None,
    entities_file: str | None = None,
    variable_inputs: "dict | None" = None,
    glue: dict[str, list[dict]] | None = None,
    run_options: dict | None = None,
) -> str:
    """Generate a complete MATLAB script to run a pipeline function.

    Parameters
    ----------
    function_name
        Name of the MATLAB function to run (e.g. ``"bandpass_filter"``).
    db_path
        Absolute path to the .duckdb file.
    schema_keys
        Dataset schema keys (e.g. ``["subject", "session"]``).
    variants
        List of variant dicts from the DAG. Each variant has keys:
        ``input_types``, ``output_type``, ``constants``, ``record_count``.
        If empty/None, generates a template command.
    schema_filter
        Optional ``{key: [selected_values]}`` for filtering.
    schema_level
        Optional list of schema keys to iterate over.
    addpath_dirs
        Optional list of directories to add to MATLAB path.
    python_executable
        Optional absolute path to the Python interpreter that MATLAB's
        Python bridge should bind to. When provided, the generated script
        emits a guarded ``pyenv`` preamble that binds (or verifies) the
        interpreter before any ``py.*`` call is made. When ``None``/empty,
        no preamble is emitted (preserves behavior for non-GUI callers).
    glue
        Optional ``{param_name: [{"name", "language", "source_file"}, ...]}``
        in application order — the glue chains wired into this function's
        input bindings. Emitted as ``+scidb/for_each.m``'s ``glue`` option.

        Omitting it is not neutral: a MATLAB run then executes with the glue
        SILENTLY DROPPED, which is the exact failure
        ``docs/claude/free-code-glue-nodes.md`` §2 exists to prevent. The
        MATLAB half was built with the feature; only this thread was missing
        (2026-09-10).
    sweeps
        Optional ``{param_name: [values]}`` — a Sweep has no DB-history
        representation (always fans out to ``EachOf``/``Sweep`` fresh at
        execution time), so unlike ``path_inputs`` this only ever comes
        from the registry via manual-edge wiring, never DB variants — see
        ``matlab_command_service``'s collection logic.
    variable_inputs
        Optional ``{param_name: type_name | [type_names] | {"types": [...],
        "columns": [...], "iterate": bool}}`` for the canvas's VARIABLE
        bindings — the third binding kind alongside ``path_inputs``
        and ``sweeps``, and the one this generator used to have no source for
        at all. A function with DB history got its variables from each
        variant's ``input_types``; one that had never run got none, and its
        variable-fed parameters were simply absent from the emitted struct
        (see ``_order_inputs_by_signature`` for what MATLAB then does with the
        arguments that remain). Supplied for both cases now, as the live
        overlay on top of recorded ``input_types`` — the same role
        edge-resolved ``path_inputs`` already played.

        The dict form carries the GUI's column selection on the SAME entry
        rather than in a parallel map, so every emit site picks it up through
        :func:`_variable_binding_parts` without a second thing to remember.

    Returns
    -------
    str
        A complete, self-contained MATLAB script.
    """
    lines: list[str] = []
    lines.append(f"%% SciStack: Run {function_name}")
    lines.append("% Generated by SciStack GUI — paste into MATLAB Command Window")
    lines.append("")
    timing = _PreambleTiming()
    lines.extend(_timing_init_lines())

    # pyenv preamble — must come before any py.* call (i.e. before
    # scihist.configure_database, which internally calls py.scidb.*).
    pyenv_lines = (
        _format_pyenv_preamble(python_executable) if python_executable else []
    )
    lines.extend(timing.section(pyenv_lines, "scistack_t_pyenv__", "pyenv_preamble"))

    # addpath entries
    addpath_lines = [
        f"addpath('{_escape_matlab_string(d)}');" for d in (addpath_dirs or [])
    ]
    lines.extend(timing.section(addpath_lines, "scistack_t_addpath__", "addpath"))
    lines.extend(
        timing.section(_run_origin_lines(), "scistack_t_origin__", "run_origin")
    )

    lines.extend(
        timing.section(
            _project_root_lines(project_root, path_inputs),
            "scistack_t_project_root__",
            "project_root",
        )
    )
    lines.extend(
        timing.section(
            _entities_script_lines(entities_script, entities_file, project_root),
            "scistack_t_entities__",
            "entities",
        )
    )

    # Configure database
    schema_keys_str = _format_matlab_string_array(schema_keys)
    lines.extend(
        timing.section(
            [
                "% Configure database (skip if already configured)",
                f"db = scihist.configure_database("
                f"'{_escape_matlab_string(db_path)}', {schema_keys_str});",
            ],
            "scistack_t_configure_db__",
            "configure_database",
        )
    )

    if not variants:
        # No variant info — generate a template from the canvas wiring alone:
        # variable/PathInput/Parameter bindings and edge-inferred output types.
        # This branch is the FIRST run of a function, so the wiring is the only
        # source there is; a binding kind missing here is silently missing from
        # the call (see _order_inputs_by_signature).
        template_inputs: dict[str, str] = {}
        for p, expr in _variable_input_items(variable_inputs):
            template_inputs[p] = expr
        if path_inputs:
            for p, pi in path_inputs.items():
                template_inputs[p] = _format_path_input(pi)
        if sweeps:
            for p, values in sweeps.items():
                template_inputs[p] = _format_sweep(values, p)
        template_inputs = _order_inputs_by_signature(function_name, template_inputs)
        inputs_str = (
            _format_matlab_struct(template_inputs) if template_inputs else "struct()"
        )
        lines.extend(
            _unresolvable_var_type_lines(
                list(output_types or []) + _variable_input_type_names(variable_inputs)
            )
        )
        if output_types:
            outputs_str = _format_matlab_cell([f"{t}()" for t in output_types])
        else:
            outputs_str = "{}"
            logger.warning(
                "generate_matlab_command: no output_types for %s — "
                "outputs will be empty, saves will be skipped",
                function_name,
            )
        # Schema kwargs matter MORE here, not less. This branch is the FIRST
        # run of a function, so there is no DB history to iterate from; without
        # 'key', [] for each schema key, for_each has no iterables at all,
        # collapses to a single combo, and hands the function every loaded
        # record at once as one table instead of calling it per combo. That is
        # only invisible for a function whose PathInput template contains a
        # {key} placeholder, because PathInput discovery then supplies the
        # iterable — which is exactly why this went unnoticed until a
        # never-run function consumed a DB variable instead of a file.
        # See .claude/plan-matlab-struct-and-iteration-26-09-02.md defect 2.
        #
        # An EXPLICITLY empty schema_level is still honoured here: "iterate
        # nothing" is a deliberate choice (a dataset-level call), not the
        # absence of one — see _resolve_iterate_keys.
        template_schema_str = _format_schema_kwargs(
            _resolve_iterate_keys(schema_level, schema_keys, function_name),
            schema_filter,
            {},
            function_name,
        )
        lines.extend(timing.summary_lines())
        template_glue_str = _format_glue_struct(glue)
        template_tail = (
            f", ...\n        'glue', {template_glue_str}" if template_glue_str else ""
        )
        # The first-run branch needs the run options as much as the variant
        # branch does -- more, arguably, since distribute/as_table shape the
        # invocation_id this very first run establishes.
        template_opts_str = _format_run_option_pairs(run_options, function_name)
        if template_opts_str:
            template_tail += f", ...\n        {template_opts_str}"
        lines.append("try")
        lines.append("    % Run (fill in inputs/outputs)")
        lines.append(f"    scidb.for_each(@{function_name}, ...")
        lines.append(f"        {inputs_str}, ...")
        if template_schema_str:
            lines.append(f"        {outputs_str}, ...")
            lines.append(f"        {template_schema_str}{template_tail});")
        else:
            lines.append(f"        {outputs_str}{template_tail});")
        lines.append("    scidb.close_database(db);")
        lines.append("catch scistack_err__")
        lines.append(
            "    scidb.Log.err('MATLAB: for_each FAILED: %s', scistack_err__.message);"
        )
        lines.append("    try")
        lines.append("        scidb.close_database(db);")
        lines.append("    catch")
        lines.append(
            "        % close already logged its own error; don't mask the original"
        )
        lines.append("    end")
        lines.extend("    " + line for line in _run_origin_reset_lines())
        lines.append("    rethrow(scistack_err__);")
        lines.append("end")
        lines.extend(_run_origin_reset_lines())
        lines.extend(timing.total_lines())
        return "\n".join(lines)

    # Register variable types
    all_var_types = _collect_var_types(variants)
    all_var_types |= set(_variable_input_type_names(variable_inputs))
    lines.extend(_unresolvable_var_type_lines(all_var_types))
    register_lines: list[str] = []
    if all_var_types:
        register_lines.append("% Register variable types")
        for vtype in sorted(all_var_types):
            register_lines.append(f"scidb.register_variable({vtype}());")
    lines.extend(
        timing.section(register_lines, "scistack_t_register__", "register_variables")
    )

    lines.extend(timing.summary_lines())

    # Wrap all for_each calls in a try/catch so db.close() always runs,
    # even if the run errors out or is interrupted.
    lines.append("try")

    # Generate for_each call for each unique (inputs, constants) group.
    lines.extend(
        _for_each_call_lines(
            function_name,
            _group_variants(variants),
            schema_keys,
            schema_filter,
            schema_level,
            path_inputs,
            matlab_fn="scidb.for_each",
            sweeps=sweeps,
            variable_inputs=variable_inputs,
            glue=glue,
            run_options=run_options,
        )
    )

    lines.append("    scidb.close_database(db);")
    lines.append("catch scistack_err__")
    lines.append(
        "    scidb.Log.err('MATLAB: for_each FAILED: %s', scistack_err__.message);"
    )
    lines.append("    try")
    lines.append("        scidb.close_database(db);")
    lines.append("    catch")
    lines.append(
        "        % close already logged its own error; don't mask the original"
    )
    lines.append("    end")
    lines.extend("    " + line for line in _run_origin_reset_lines())
    lines.append("    rethrow(scistack_err__);")
    lines.append("end")
    lines.extend(_run_origin_reset_lines())
    lines.extend(timing.total_lines())
    return "\n".join(lines)


def _collect_var_types(variants: list[dict]) -> set[str]:
    """All BaseVariable class names referenced (as an input or output)
    across a function's variant rows — the set that needs a
    ``scidb.register_variable(...)`` call.

    PathInput params are **excluded**. A PathInput is recorded in
    ``input_types`` as its ``PathInput.to_key()`` JSON blob, not as a class
    name, so emitting it here produced
    ``scidb.register_variable({"__type": "PathInput", ...}());`` — which MATLAB
    rejects at parse time with "Invalid expression. When calling a function or
    indexing a variable, use parentheses."

    That only bit on the SECOND run of a function: the first run has no DB
    variants and takes ``generate_matlab_command``'s template branch, which
    never reaches this collector. The first run then records the variant that
    breaks the second one.

    Every other consumer of ``input_types`` already applies this rule —
    ``_for_each_call_lines`` skips PathInputs when building the inputs struct,
    and ``scidb``'s ``get_aggregated_variants`` routes them into a separate
    ``path_inputs`` bucket. This was the one place that did not.
    """
    from scistack_gui.api.pipeline import _parse_path_input

    all_var_types: set[str] = set()
    excluded_path_inputs = 0
    for v in variants:
        input_types = v.get("input_types", {})
        if isinstance(input_types, dict):
            for type_val in input_types.values():
                if _parse_path_input(str(type_val)) is not None:
                    excluded_path_inputs += 1
                    continue
                all_var_types.add(type_val)
        output_type = v.get("output_type", "")
        if output_type:
            all_var_types.add(output_type)
    logger.info(
        "_collect_var_types: %d variable type(s) %s (excluded %d PathInput param(s))",
        len(all_var_types),
        sorted(all_var_types),
        excluded_path_inputs,
    )
    return all_var_types


def _group_variants(variants: list[dict]) -> list[dict]:
    """Group variant rows by (input_types, constants). Multi-output MATLAB
    functions (e.g. load_csv -> [Time, Force_Left, Force_Right]) surface in
    the DB as one variant row per output_type, all sharing the same inputs
    and constants. They must collapse into a single for_each call whose
    outputs cell lists every output_type.

    The grouping key runs the constants through
    :func:`scidb.provenance.constants_identity_key` rather than
    ``tuple(sorted(constants.items()))``. A constant is not necessarily a
    scalar: an inline table under ``[parameters]`` in
    ``scistack_entities.toml`` *is* the value, so a project with e.g.
    ``delsys_config = {...}`` puts a ``dict`` in that map and the bare tuple
    is then unhashable — ``TypeError: unhashable type: 'dict'``, raised from
    ``setdefault`` below, which took down every "Run in MATLAB" for the
    function. The rest of this module already handles dict constants
    (``_format_matlab_value`` renders them as MATLAB structs); this key was
    the one place that did not.
    """
    from scidb.provenance import constants_identity_key

    grouped: dict[tuple, dict] = {}
    for v in variants:
        input_types = v.get("input_types", {}) or {}
        constants = v.get("constants", {}) or {}
        key = (
            tuple(sorted(input_types.items())) if isinstance(input_types, dict) else (),
            constants_identity_key(constants),
        )
        entry = grouped.setdefault(
            key,
            {
                "input_types": input_types,
                "constants": constants,
                "output_types": [],
            },
        )
        output_type = v.get("output_type", "")
        if output_type and output_type not in entry["output_types"]:
            entry["output_types"].append(output_type)
    logger.info(
        "_group_variants: %d variant row(s) -> %d for_each call(s); "
        "non-scalar constants: %s",
        len(variants),
        len(grouped),
        sorted(
            {
                k
                for v in variants
                if isinstance(v.get("constants"), dict)
                for k, val in v["constants"].items()
                if isinstance(val, (dict, list, tuple, set))
            }
        )
        or "none",
    )
    return list(grouped.values())


def _for_each_call_lines(
    function_name: str,
    grouped_entries: list[dict],
    schema_keys: list[str],
    schema_filter: dict[str, list] | None,
    schema_level: list[str] | None,
    path_inputs: dict[str, dict] | None,
    matlab_fn: str = "scidb.for_each",
    indent: str = "    ",
    sweeps: dict[str, list] | None = None,
    variable_inputs: "dict | None" = None,
    glue: dict[str, list[dict]] | None = None,
    run_options: dict | None = None,
) -> list[str]:
    """One (indented) ``<matlab_fn>(@function_name, ...)`` block per grouped
    (inputs, constants) entry — the call body shared between a single
    function's ready-to-paste command and one node's step registration
    inside a whole-pipeline script (``generate_matlab_pipeline_command``).

    The assembled struct is ordered by the function's parsed MATLAB signature
    before it is formatted: ``for_each`` binds these fields to arguments
    positionally, so collection order (variables, PathInputs, Parameters,
    constants — each in canvas-edge order) is not a safe order to emit. See
    :func:`_order_inputs_by_signature`.
    """
    from scistack_gui.api.pipeline import _parse_path_input

    # Resolved once, outside the loop: it does not vary per group, and it logs.
    # schema_level is tri-state — None = all keys, [k...] = those keys,
    # [] = deselect all (no iteration at all).
    iterate_keys = _resolve_iterate_keys(schema_level, schema_keys, function_name)

    lines: list[str] = []
    for entry in grouped_entries:
        input_types = entry["input_types"]
        output_types_list = entry["output_types"]
        constants = entry["constants"]

        # Build inputs struct — skip PathInput entries (handled via path_inputs).
        inputs_dict = {}
        if isinstance(input_types, dict):
            for param_name, type_name in input_types.items():
                if _parse_path_input(str(type_name)) is None:
                    inputs_dict[param_name] = f"{type_name}()"
        # Overlay the canvas's current variable wiring on top of whatever
        # history recorded — same live-overlay rule as path_inputs below, and
        # the only source at all for a param the DB variant never carried.
        for param_name, expr in _variable_input_items(variable_inputs):
            inputs_dict[param_name] = expr
        # Add path inputs as scifor.PathInput(...) expressions.
        if path_inputs:
            for param_name, pi in path_inputs.items():
                inputs_dict[param_name] = _format_path_input(pi)
        # Add Parameter values as scidb.Parameter(...) expressions.
        if sweeps:
            for param_name, values in sweeps.items():
                inputs_dict[param_name] = _format_sweep(values, param_name)
        # Add constants as scalar values
        for k, val in constants.items():
            inputs_dict[k] = _format_matlab_value(val)

        inputs_dict = _order_inputs_by_signature(function_name, inputs_dict)
        inputs_str = _format_matlab_struct(inputs_dict)
        outputs_str = (
            _format_matlab_cell([f"{t}()" for t in output_types_list])
            if output_types_list
            else "{}"
        )

        # Build schema kwargs (iterate_keys resolved above the loop).
        schema_str = _format_schema_kwargs(
            iterate_keys, schema_filter, constants, function_name
        )

        # Glue rides on the call as a trailing 'glue' name/value pair — it is
        # a property of an INPUT BINDING, never a step, so it never appears in
        # the inputs struct and never gets a for_each call of its own.
        glue_str = _format_glue_struct(glue)

        # Run options ride on the same trailing name/value tail as glue. They
        # are NOT part of the inputs struct: for_each reads them as options,
        # and distribute/as_table are identity-bearing (they change the
        # invocation_id), so a dropped pair silently produces a different run
        # than the one requested -- see docs/claude/gui-run-options-flow.md.
        opts_str = _format_run_option_pairs(run_options, function_name)

        lines.append(f"{indent}% Run")
        lines.append(f"{indent}{matlab_fn}(@{function_name}, ...")
        lines.append(f"{indent}    {inputs_str}, ...")
        tail = f", ...\n{indent}    'glue', {glue_str}" if glue_str else ""
        if opts_str:
            tail += f", ...\n{indent}    {opts_str}"
        if schema_str:
            lines.append(f"{indent}    {outputs_str}, ...")
            lines.append(f"{indent}    {schema_str}{tail});")
        else:
            lines.append(f"{indent}    {outputs_str}{tail});")
        lines.append("")
    return lines


def _format_glue_struct(glue: dict[str, list[dict]] | None) -> str:
    """``struct(...)`` for ``+scidb/for_each.m``'s ``glue`` option, or ``""``.

    Each param maps to a cell array of chain elements in application order.
    An element is either a MATLAB function handle (``@glue_x``) or, for
    **Python** glue, a struct naming the body and its file. Python glue
    reaches a MATLAB run only on a constant-fed parameter, where
    ``scidb.glue.apply_constant_glue`` applies it inside ``for_each_prepare``
    — see ``build_glue_chains`` in ``+scidb/for_each.m``.

    A one-element cell must be written ``{{...}}`` inside ``struct()``:
    ``struct('p', {x})`` builds a 1x1 STRUCT ARRAY with field ``p = x``, not a
    field holding a cell. The extra brace is what makes it a cell-valued
    field, and getting it wrong turns the chain into a scalar MATLAB silently
    accepts.
    """
    if not glue:
        return ""
    parts = []
    for param, chain in glue.items():
        elements = []
        for spec in chain:
            if (spec.get("language") or "matlab") == "python":
                elements.append(
                    "struct('name', {}, 'language', 'python', "
                    "'source_file', {})".format(
                        _format_matlab_value(spec.get("name", "")),
                        _format_matlab_value(spec.get("source_file") or ""),
                    )
                )
            else:
                elements.append(f"@{spec.get('name', '')}")
        if elements:
            parts.append(
                "{}, {{{{{}}}}}".format(
                    _format_matlab_value(param), ", ".join(elements)
                )
            )
    if not parts:
        return ""
    return f"struct({', '.join(parts)})"


def generate_matlab_pipeline_command(
    pipeline_id: str,
    steps: list[dict],
    db_path: str,
    schema_keys: list[str],
    mode: str = "all",
    target: str = "",
    finalized: bool | None = None,
    skip_computed: bool = True,
    addpath_dirs: list[str] | None = None,
    python_executable: str | None = None,
    project_root: str | None = None,
    entities_script: str | None = None,
    entities_file: str | None = None,
) -> str:
    """Generate a complete MATLAB script that runs a whole GUI pipeline
    scope through ``scidb.Pipeline`` — deferred registration of every
    MATLAB function node's step(s), then one driven run — instead of a
    single function's ``for_each`` call.

    Parameters
    ----------
    pipeline_id
        The GUI pipeline's id/name; becomes the ``scidb.Pipeline(...)``
        name.
    steps
        One entry per MATLAB function node already resolved by the caller
        (``matlab_command_service.generate_matlab_pipeline_command`` — the
        same target-derivation used for per-node runs and Python pipeline
        compilation, see ``execution_service.derive_target_for_node`` /
        ``build_backend_pipeline``). Each entry:
        ``{"function_name": str, "variants": list[dict] | None,
        "schema_filter": dict | None, "schema_level": list[str] | None,
        "path_inputs": dict | None, "sweeps": dict | None,
        "variable_inputs": dict | None, "run_options": dict | None}``.
        ``run_options`` comes from that node's SAVED config, not from canvas
        node data -- a pipeline run touches nodes the user never selected, so
        there is no live React state to read (see
        docs/claude/gui-run-options-flow.md). A step with no resolvable variants
        (nothing derivable — never run and no output wiring) is skipped
        with a comment, mirroring the disconnected-wiring skip convention
        in ``code_export_service`` — not an error, since the rest of the
        pipeline can still run.

        Only MATLAB-language function nodes belong in ``steps`` — Python
        function nodes in the same GUI pipeline scope are NOT registered
        here (a MATLAB-only script has no way to register a Python
        ``for_each`` call into the same in-process ``scidb.Pipeline`` — the
        Python interpreter MATLAB loads via ``pyenv``/the sidecar is a
        fresh process with no memory of the GUI server's own compiled
        pipeline). Callers must filter ``steps`` to MATLAB functions only
        and separately warn about any excluded Python steps.
    mode, target, finalized, skip_computed
        Mirror ``execution_service.run_pipeline``'s dispatch: ``"all"`` ->
        ``pipe.run_all()``, ``"until"`` -> ``pipe.run_until(target)``,
        ``"endpoints"`` -> ``pipe.run_endpoints()``. ``"show"`` has no
        MATLAB ``Pipeline.m`` equivalent (no ``show()`` method there) and
        is rejected.

    Returns
    -------
    str
        A complete, self-contained MATLAB script.
    """
    if mode not in ("all", "until", "endpoints"):
        raise ValueError(
            f"generate_matlab_pipeline_command: unsupported mode {mode!r} — "
            "MATLAB Pipeline.m only supports 'all', 'until', 'endpoints' "
            "(mode='show' has no MATLAB Pipeline.m equivalent)"
        )
    if mode == "until" and not target:
        raise ValueError("mode='until' requires a target step name")

    lines: list[str] = []
    lines.append(f"%% SciStack: Run pipeline {pipeline_id}")
    lines.append("% Generated by SciStack GUI — paste into MATLAB Command Window")
    lines.append("")
    timing = _PreambleTiming()
    lines.extend(_timing_init_lines())

    # pyenv preamble — must come before any py.* call.
    pyenv_lines = (
        _format_pyenv_preamble(python_executable) if python_executable else []
    )
    lines.extend(timing.section(pyenv_lines, "scistack_t_pyenv__", "pyenv_preamble"))

    # addpath entries
    addpath_lines = [
        f"addpath('{_escape_matlab_string(d)}');" for d in (addpath_dirs or [])
    ]
    lines.extend(timing.section(addpath_lines, "scistack_t_addpath__", "addpath"))
    lines.extend(
        timing.section(_run_origin_lines(), "scistack_t_origin__", "run_origin")
    )

    all_step_path_inputs = {
        param: pi
        for step in steps
        for param, pi in (step.get("path_inputs") or {}).items()
    }
    lines.extend(
        timing.section(
            _project_root_lines(project_root, all_step_path_inputs),
            "scistack_t_project_root__",
            "project_root",
        )
    )
    lines.extend(
        timing.section(
            _entities_script_lines(entities_script, entities_file, project_root),
            "scistack_t_entities__",
            "entities",
        )
    )

    # Configure database
    schema_keys_str = _format_matlab_string_array(schema_keys)
    lines.extend(
        timing.section(
            [
                "% Configure database (skip if already configured)",
                f"db = scihist.configure_database("
                f"'{_escape_matlab_string(db_path)}', {schema_keys_str});",
            ],
            "scistack_t_configure_db__",
            "configure_database",
        )
    )

    all_var_types: set[str] = set()
    resolved_steps: list[tuple[str, list[dict], dict]] = []
    skip_comments: list[str] = []
    for step in steps:
        fn_name = step["function_name"]
        variants = step.get("variants")
        if not variants:
            skip_comments.append(
                f"% SKIPPED: '{fn_name}' — no runnable target derived "
                "(never run and no output wiring)"
            )
            continue
        all_var_types.update(_collect_var_types(variants))
        resolved_steps.append((fn_name, _group_variants(variants), step))

    if not resolved_steps:
        raise ValueError(
            f"generate_matlab_pipeline_command: no runnable MATLAB step in "
            f"pipeline {pipeline_id!r} — nothing to register"
        )

    lines.extend(_unresolvable_var_type_lines(all_var_types))
    register_lines: list[str] = []
    if all_var_types:
        register_lines.append("% Register variable types")
        for vtype in sorted(all_var_types):
            register_lines.append(f"scidb.register_variable({vtype}());")
    lines.extend(
        timing.section(register_lines, "scistack_t_register__", "register_variables")
    )

    lines.extend(timing.summary_lines())

    lines.append(f"pipe = scidb.Pipeline('{_escape_matlab_string(pipeline_id)}');")
    lines.append("")

    # Wrap registration + the driven run in a try/catch so db.close() always
    # runs, even if a step registration or the run itself errors out.
    lines.append("try")
    for fn_name, grouped_entries, step in resolved_steps:
        lines.append(f"    % Register {fn_name} (deferred — runs via pipe below)")
        lines.extend(
            _for_each_call_lines(
                fn_name,
                grouped_entries,
                schema_keys,
                step.get("schema_filter"),
                step.get("schema_level"),
                step.get("path_inputs"),
                matlab_fn="scidb.for_each",
                sweeps=step.get("sweeps"),
                variable_inputs=step.get("variable_inputs"),
                run_options=step.get("run_options"),
            )
        )
    for comment in skip_comments:
        lines.append(f"    {comment}")
    if skip_comments:
        lines.append("")

    skip_str = "true" if skip_computed else "false"
    if mode == "all":
        lines.append(f"    pipe.run_all('skip_computed', {skip_str});")
    elif mode == "until":
        run_line = (
            f"    pipe.run_until('{_escape_matlab_string(target)}', "
            f"'skip_computed', {skip_str}"
        )
        if finalized is not None:
            run_line += f", 'finalized', {'true' if finalized else 'false'}"
        lines.append(run_line + ");")
    else:  # "endpoints"
        run_line = (
            f"    pipe.run_endpoints('skip_computed', {skip_str}, "
            "'include_used', true"
        )
        if finalized is not None:
            run_line += f", 'finalized', {'true' if finalized else 'false'}"
        lines.append(run_line + ");")

    lines.append("    scidb.close_database(db);")
    lines.append("catch scistack_err__")
    lines.append(
        "    scidb.Log.err('MATLAB: pipeline run FAILED: %s', scistack_err__.message);"
    )
    lines.append("    try")
    lines.append("        scidb.close_database(db);")
    lines.append("    catch")
    lines.append(
        "        % close already logged its own error; don't mask the original"
    )
    lines.append("    end")
    lines.extend("    " + line for line in _run_origin_reset_lines())
    lines.append("    rethrow(scistack_err__);")
    lines.append("end")
    lines.extend(_run_origin_reset_lines())
    lines.extend(timing.total_lines())
    return "\n".join(lines)


def _format_path_input(pi: dict) -> str:
    """Format a PathInput info dict as a MATLAB ``scifor.PathInput(...)`` expression.

    The template stored in the layout may already include MATLAB double-quote
    delimiters (e.g. ``"C:\\data\\file.csv"``), or it may be a bare pattern
    string (e.g. ``{subject}/trial_{trial}.mat``). Both forms are handled.

    ``root_folder`` is emitted **only when the declaration has one**. This used
    to substitute the project root for a rootless declaration with a relative
    template, so MATLAB's cwd (a temp script dir) wouldn't decide resolution.
    That fixed resolution by rewriting identity: ``PathInput.to_key()``
    serializes ``(template, root_folder)`` and DB history carries no name, so
    ``graph_builder.resolve_path_input_name`` could no longer content-match the
    run it had just recorded against the declaration that produced it, and the
    canvas grew an ``__unresolved__`` ghost node next to the real one. It also
    meant the same PathInput recorded a different key from the GUI than from
    the user's own MATLAB script.

    Resolution is now pinned separately by ``_project_root_lines`` (see
    ``scifor.pathinput.set_project_root``), which leaves identity alone.
    """
    template = pi.get("template", "")
    root_folder = pi.get("root_folder")

    # If the template is already wrapped in MATLAB double quotes, use it as-is.
    # Otherwise wrap it ourselves.
    if template.startswith('"') and template.endswith('"'):
        matlab_template = template
    else:
        matlab_template = f'"{template}"'

    if root_folder:
        return f'scifor.PathInput({matlab_template}, root_folder="{root_folder}")'
    return f"scifor.PathInput({matlab_template})"


def _format_sweep(values: list, param_name: str = "") -> str:
    """Format a Parameter's value list as a MATLAB ``scidb.Parameter(...)``
    expression (mirrors ``_format_path_input``'s role for PathInput).

    ``isa(x, 'scifor.EachOf')`` covers a Parameter for free, so ``for_each``
    fans it out with no special handling -- see ``+scidb/Parameter.m``.

    An EMPTY *values* raises rather than emitting ``scidb.Parameter()``. That
    expression is perfectly constructible in MATLAB (a Parameter with no value
    yet is a legal object), so the script would generate, run, expand a
    zero-length axis and write nothing -- failing in the terminal, a long way
    from the click that caused it. The Python run path refuses the same state
    in ``execution_service.build_run_inputs``; this is the MATLAB route's copy
    of that refusal, at generation time.

    A dict-valued Parameter (``CONFIG = { fld1 = 1 }`` under ``[parameters]``)
    is logged: it renders to a nested ``struct(...)`` literal, and when one of
    these silently degraded to a quoted Python repr there was nothing in the
    log to see it by."""
    if not values:
        label = f"'{param_name}'" if param_name else "a wired Parameter"
        raise ValueError(
            f"parameter {label} has no value yet, so there is nothing to run "
            f"-- give it at least one value on its node."
        )
    items = ", ".join(_format_matlab_value(v) for v in values)
    n_struct = sum(1 for v in values if isinstance(v, dict))
    if n_struct:
        logger.info(
            "_format_sweep: %d of %d Parameter value(s) rendered as MATLAB "
            "struct literals",
            n_struct,
            len(values),
        )
    return f"scidb.Parameter({items})"


def _escape_matlab_string(s: str) -> str:
    """Escape single quotes for MATLAB string literals."""
    return s.replace("'", "''")


def _escape_matlab_dq(s: str) -> str:
    """Escape for a DOUBLE-quoted MATLAB string (``"..."``).

    The two quotings escape different characters and are not interchangeable:
    inside ``"..."`` a double quote is doubled and a single quote is literal;
    inside ``'...'`` it is the other way round. Running a value through
    :func:`_escape_matlab_string` and then wrapping it in double quotes —
    which is what :func:`_format_matlab_string_array` used to do — leaves an
    embedded ``"`` unescaped (it terminates the literal) and corrupts an
    embedded ``'`` into ``''``.

    Latent until column selection, whose values are user spreadsheet headers
    rather than the identifiers this was previously only ever called with.
    """
    return s.replace('"', '""')


def _format_pyenv_preamble(python_executable: str) -> list[str]:
    """Return MATLAB lines that bind AND force-load ``pyenv`` to the given interpreter.

    Emits three stages:
      1. Bind: if MATLAB's Python interface is ``NotLoaded``, call
         ``pyenv('Version', python_executable)``. If it is already loaded but
         points to a different interpreter, ``error`` out with
         ``SciStack:PyenvMismatch``.
      2. Force-load: call ``py.sys.version`` (a trivial Python call) inside
         a try/catch. This is required because ``pyenv('Version', ...)`` only
         *configures* Python; it does not actually load it. On some
         MATLAB+venv combinations, the first ``py.*`` call inside a package
         function (e.g. ``scihist.configure_database``) fails with
         "Unrecognized function or variable 'py'" because MATLAB's symbol
         resolver runs before the Python load. Loading Python here — from
         the script's top-level scope — avoids that path entirely.
      3. Diagnostic dump on failure: print MATLAB's ``pyenv`` state to
         stderr and suggest the ``'ExecutionMode', 'OutOfProcess'``
         workaround, then rethrow so the user sees the underlying MATLAB
         error.

    The path is converted to forward slashes (matching ``matlabTerminal.ts``
    convention for MATLAB single-quoted string literals on Windows) and
    single quotes are escaped via ``_escape_matlab_string``.

    Temporary variables use a ``scistack_*__`` namespace suffix and are
    ``clear``ed at the end so the script doesn't leak state into the
    caller's workspace (``run(...)`` evaluates in caller scope).
    """
    # Normalize backslashes (Windows) to forward slashes for the MATLAB literal.
    normalized = python_executable.replace("\\", "/")
    escaped = _escape_matlab_string(normalized)
    return [
        "% Bind MATLAB's Python interface to the scistack-gui interpreter.",
        f"scistack_pyenv_target__ = '{escaped}';",
        "scistack_pyenv__ = pyenv;",
        "% Normalize path separators (MATLAB returns `\\`, our target uses `/`)",
        "% and compare case-insensitively (Windows paths are case-insensitive;",
        "% MATLAB may lowercase what it stores).",
        "scistack_norm_path__ = @(p) strrep(char(p), '\\', '/');",
        'if scistack_pyenv__.Status == "NotLoaded"',
        "    scistack_pyenv__ = pyenv('Version', scistack_pyenv_target__);",
        "elseif ~strcmpi(scistack_norm_path__(scistack_pyenv__.Executable), ...",
        "                scistack_norm_path__(scistack_pyenv_target__))",
        "    error('SciStack:PyenvMismatch', ...",
        "        'MATLAB already loaded Python %s; restart MATLAB to switch to %s.', ...",
        "        scistack_pyenv__.Executable, scistack_pyenv_target__);",
        "end",
        "% Force Python to actually load NOW (at script top-level), so that",
        "% subsequent py.* calls inside package functions (e.g.",
        "% scihist.configure_database) resolve correctly. pyenv('Version', ...)",
        "% only configures Python; it does not load it.",
        "try",
        "    scistack_py_version__ = char(py.sys.version);",
        "    fprintf('[SciStack] Python loaded: %s (%s)\\n', ...",
        "        char(scistack_pyenv__.Executable), strtrim(scistack_py_version__));",
        "catch scistack_py_err__",
        "    fprintf(2, '[SciStack] py.sys.version failed: %s\\n', scistack_py_err__.message);",
        "    fprintf(2, ['[SciStack] pyenv state: Status=%s Version=%s ' ...",
        "        'Executable=%s Library=%s ExecutionMode=%s\\n'], ...",
        "        string(scistack_pyenv__.Status), string(scistack_pyenv__.Version), ...",
        "        string(scistack_pyenv__.Executable), string(scistack_pyenv__.Library), ...",
        "        string(scistack_pyenv__.ExecutionMode));",
        "    fprintf(2, ['[SciStack] Workaround to try: pyenv(''Version'', ''%s'', ' ...",
        "        '''ExecutionMode'', ''OutOfProcess''); py.sys.version\\n'], ...",
        "        scistack_pyenv_target__);",
        "    rethrow(scistack_py_err__);",
        "end",
        "% Pre-import the scidb module so py.scidb.* is warm, and so we can",
        "% distinguish 'py dispatch broken inside functions' from 'scidb module",
        "% not importable' when debugging.",
        "try",
        "    py.importlib.import_module('scidb');",
        "    fprintf('[SciStack] py.scidb import OK\\n');",
        "catch scistack_import_err__",
        "    fprintf(2, '[SciStack] py.importlib.import_module(''scidb'') failed: %s\\n', ...",
        "        scistack_import_err__.message);",
        "    rethrow(scistack_import_err__);",
        "end",
        "clear scistack_pyenv__ scistack_pyenv_target__ scistack_norm_path__ ...",
        "    scistack_py_version__ scistack_py_err__ scistack_import_err__;",
    ]


def _matlab_field_name(key: str) -> str:
    """Sanitize a dict key into a MATLAB struct field name.

    Mirrors ``matlab.lang.makeValidName``, which is what
    ``+scidb/+internal/pydict_to_struct.m`` applies to the same keys when a
    Parameter arrives through ``scidb.entities()`` instead of as an inlined
    literal. The two routes must agree: the same TOML declaration reaches
    MATLAB one way from a generated script and the other way from a
    hand-written one, and a field named differently between them is a
    pipeline that works from the prompt and fails from the GUI.

    Documented ``makeValidName`` behavior, in order: delete whitespace and
    capitalize the following character, replace remaining invalid characters
    with ``_``, prefix ``x`` when the first character is not a letter, then
    truncate to ``namelengthmax`` (63).
    """
    out: list[str] = []
    capitalize_next = False
    for ch in key:
        if ch.isspace():
            capitalize_next = True
            continue
        if ch.isalnum() and ch.isascii() or ch == "_":
            out.append(ch.upper() if capitalize_next else ch)
        else:
            out.append("_")
        capitalize_next = False
    name = "".join(out)
    if not name:
        name = "x"
    if not name[0].isalpha():
        name = "x" + name
    return name[:63]


def _format_matlab_value(val) -> str:
    """Format a Python value as a MATLAB literal.

    The mapping mirrors ``+scidb/+internal/from_python.m`` so that a value
    inlined into a generated script is byte-identical in MATLAB to the same
    value loaded through ``scidb.entities()``:

    ==========================  ===========================================
    Python                      MATLAB
    ==========================  ===========================================
    ``dict``                    ``struct('k', v, ...)`` (recursive)
    all-string ``list``         ``["a", "b"]`` string array
    all-numeric/bool ``list``   ``[1, 2]`` numeric/logical array
    mixed or nested ``list``    ``{...}`` cell array
    ``None``                    ``[]``
    ==========================  ===========================================

    Before this, everything that was not a scalar or a flat list fell through
    to ``str(val)`` wrapped in quotes, so a dict-valued Parameter declared in
    ``scistack_entities.toml`` reached the user's function as a char array
    holding a Python repr (see docs/claude/entities-toml-format.md rule 2 —
    an inline table under ``[parameters]`` *is* the value).
    """
    if val is None:
        return "[]"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        return f"'{_escape_matlab_string(val)}'"
    if isinstance(val, dict):
        return _format_matlab_struct_literal(val)
    if isinstance(val, (list, tuple)):
        return _format_matlab_list(list(val))
    return f"'{_escape_matlab_string(str(val))}'"


def _format_matlab_list(items: list) -> str:
    """Format a Python list as the MATLAB container ``from_python.m`` produces.

    A list of strings becomes a string array (``from_python.m``'s all-string
    collapse), a list of numbers or bools becomes a numeric/logical array, and
    anything heterogeneous or nested becomes a cell array. Emitting ``[...]``
    unconditionally — the old behavior — silently concatenated a list of
    strings: ``['HAM', 'RF']`` is the single char array ``'HAMRF'`` in MATLAB.
    """
    if not items:
        return "[]"
    if all(isinstance(v, str) for v in items):
        return "[" + ", ".join(f'"{_escape_matlab_double_quoted(v)}"' for v in items) + "]"
    if all(isinstance(v, bool) for v in items):
        return "[" + ", ".join("true" if v else "false" for v in items) + "]"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in items):
        return "[" + ", ".join(str(v) for v in items) + "]"
    return "{" + ", ".join(_format_matlab_value(v) for v in items) + "}"


def _format_matlab_struct_literal(mapping: dict) -> str:
    """Format a Python dict as a scalar MATLAB ``struct(...)`` literal.

    The trap this exists to avoid: ``struct('a', {1, 2})`` does not build a
    scalar struct with a cell field, it builds a **1x2 struct array** — MATLAB
    treats a cell value as the per-element list. Every cell-valued field is
    therefore wrapped one extra level (``struct('a', {{1, 2}})``), which is the
    documented way to store a cell in a struct field.
    """
    if not mapping:
        return "struct()"
    pairs = []
    for key, value in mapping.items():
        rendered = _format_matlab_value(value)
        if rendered.startswith("{"):
            rendered = "{" + rendered + "}"
        pairs.append(f"'{_matlab_field_name(str(key))}', {rendered}")
    return f"struct({', '.join(pairs)})"


def _escape_matlab_double_quoted(s: str) -> str:
    """Escape a MATLAB double-quoted string literal (``"`` doubles itself)."""
    return s.replace('"', '""')


def _format_matlab_struct(inputs_dict: dict[str, str]) -> str:
    """Build a MATLAB ``struct(...)`` expression from a dict.

    Values are already formatted as MATLAB expressions (not quoted again).
    """
    if not inputs_dict:
        return "struct()"
    pairs = []
    for k, v in inputs_dict.items():
        pairs.append(f"'{k}', {v}")
    return f"struct({', '.join(pairs)})"


def _format_matlab_cell(items: list[str]) -> str:
    """Build a MATLAB cell array ``{Item1(), Item2()}``."""
    return "{" + ", ".join(items) + "}"


def _format_matlab_string_array(items: list[str]) -> str:
    """Format a Python list of strings as a MATLAB string array ``["a", "b"]``."""
    if not items:
        return "[]"
    escaped = [f'"{_escape_matlab_dq(s)}"' for s in items]
    return "[" + ", ".join(escaped) + "]"


#: MATLAB's own defaults for the four run options, from ``+scidb/for_each.m``
#: (``opts.as_table = string.empty``, ``opts.distribute = false`` at :1723).
#: A value equal to its default is NOT emitted: the generated call then says
#: nothing about it and MATLAB applies the same value, which keeps scripts
#: diffable against the ones generated before run options were plumbed.
_RUN_OPTION_DEFAULTS = {
    "dry_run": False,
    "save": True,
    "distribute": False,
    "as_table": False,
}


def _format_run_option_pairs(
    run_options: dict | None, function_name: str = ""
) -> str:
    """MATLAB ``'name', value`` pairs for the non-default run options, or ``""``.

    These are the GUI's four ``for_each`` toggles (``dry_run``, ``save``,
    ``distribute``, ``as_table``). They reach a Python run as keyword arguments
    (``execution_service``) but reach a MATLAB run only as text in a generated
    script, so an option this function does not render is an option MATLAB never
    hears about -- and it fails **silently**, because the script is still
    well-formed and the run still succeeds. That is exactly how ``distribute``
    went missing until 2026-09-14: the request logged ``distribute: True``, the
    generator never read the key, and the run saved 560 undistributed records.
    See docs/claude/gui-run-options-flow.md.

    ``as_table`` is the one with two shapes: ``for_each.m`` (:217-223) accepts a
    logical scalar or a string array of parameter names, so a list round-trips
    as ``["a", "b"]`` and a bool as ``true``.
    """
    if not run_options:
        return ""

    parts: list[str] = []
    emitted: dict[str, object] = {}
    for name, default in _RUN_OPTION_DEFAULTS.items():
        if name not in run_options:
            continue
        value = run_options[name]
        if name == "as_table" and isinstance(value, (list, tuple)):
            names = [str(v) for v in value if v]
            if not names:
                continue
            parts.append(f"'as_table', {_format_matlab_string_array(names)}")
            emitted[name] = names
            continue
        value = bool(value)
        if value == default:
            continue
        parts.append(f"'{name}', {'true' if value else 'false'}")
        emitted[name] = value

    if emitted:
        logger.info(
            "generate_matlab_command: %s: emitting non-default run option(s) %s "
            "into the for_each call",
            function_name or "<fn>",
            emitted,
        )

    # A key the caller asked about that produced no pair is either a default
    # (fine, and the common case) or a value this renderer does not understand
    # (not fine -- it would be dropped exactly the way distribute was).
    unknown = sorted(set(run_options) - set(_RUN_OPTION_DEFAULTS))
    if unknown:
        logger.warning(
            "generate_matlab_command: %s: run option(s) %s are not recognised "
            "and will NOT reach MATLAB -- the run will use for_each's defaults "
            "for them",
            function_name or "<fn>",
            unknown,
        )

    return ", ".join(parts)


def _resolve_iterate_keys(
    schema_level: list[str] | None,
    schema_keys: list[str],
    function_name: str,
) -> list[str]:
    """Which schema keys this run iterates — the tri-state of ``schema_level``.

    ``schema_level`` is **three**-valued, and the third value is the one that
    used to be lost:

    - ``None``  -> not specified; iterate ALL of ``schema_keys``.
    - ``[k...]`` -> iterate exactly those keys.
    - ``[]``     -> the user deselected EVERY level; iterate nothing, i.e. one
      dataset-level call.

    The old test was ``schema_level if schema_level else schema_keys``, which
    is a truthiness test, so the empty list folded back into "all keys" and a
    deselect-everything node ran the full schema grid anyway. With
    ``distribute=true`` that is not merely extra work — it changes the
    distribute target (scifor resolves it from the DEEPEST ITERATED key), so
    the run also saved at the wrong level. See
    ``.claude/plan-schema-level-empty-distribute.md`` and
    ``docs/claude/gui-run-options-flow.md``.

    ``domain/variant_resolver.resolve_variants`` and ``api/run.py``'s Python
    path already distinguish the three; this is the same contract, spelled the
    same way.
    """
    if schema_level is None:
        return list(schema_keys)
    if not schema_level:
        # INFO, not DEBUG: "no schema kwargs in the generated script" is
        # otherwise indistinguishable from a generator that dropped them
        # (which is exactly the bug this replaced), and it is the only
        # observable difference between a dataset-level run and a full-grid
        # run before MATLAB starts printing combos.
        logger.info(
            "generate_matlab_command: %s: schema_level=[] — every schema level "
            "deselected, emitting NO schema kwargs (one dataset-level call; "
            "distribute, if set, targets the top of the schema %s)",
            function_name or "<fn>",
            list(schema_keys),
        )
        return []
    return list(schema_level)


def _format_schema_kwargs(
    iterate_keys: list[str],
    schema_filter: dict[str, list] | None,
    constants: dict,
    function_name: str,
) -> str:
    """Build MATLAB name-value schema keyword arguments for for_each.

    Returns empty string if there are no schema arguments to pass.
    """
    if not iterate_keys:
        return ""

    parts = []
    for key in iterate_keys:
        if schema_filter and key in schema_filter and schema_filter[key]:
            values = schema_filter[key]
            # Format values: numbers as array, strings as string array.
            if all(isinstance(v, (int, float)) for v in values):
                formatted = "[" + " ".join(str(v) for v in values) + "]"
            else:
                formatted = _format_matlab_string_array([str(v) for v in values])
            parts.append(f"'{key}', {formatted}")
        else:
            # No filter for this key — emit [] so scidb.for_each resolves
            # all distinct values from the database.  Without this,
            # PathInput templates that reference {key} won't be substituted.
            parts.append(f"'{key}', []")

    if not parts:
        return ""
    return ", ".join(parts)
