"""The per-combo failure record — the ONE owner of "what failed, and where".

``scifor.for_each`` catches every per-combo exception (skip-and-continue).
Each one becomes a :class:`ComboFailure`: the exception's type and message,
the combo it failed on, a traceback trimmed to start at the user's own code,
and the innermost user frame (the line to jump to). Failures with the same
``reason`` (``"{Type}: {message}"``) are aggregated into a
:class:`FailureGroup`, whose ``to_dict()`` is the wire shape every consumer
reads: the end-of-run ``summary`` progress event, the scidb pipeline step
report, the GUI's ``run_done``, and the MATLAB bridge (which emits the same
shape from ``MException.stack``).

``frame_origin`` is the single place that decides whether a traceback frame
is the user's code, SciStack's own code ("framework"), or a third-party /
standard library ("library"). Nothing else classifies frames.

``on_error="stop"`` (see :func:`validate_on_error`) makes the loop stop at the
first genuine failure and re-raise the ORIGINAL exception, with a
:class:`StoppedRun` attached (read it back with :func:`stopped_run_of`) so a
database layer can still save the combos that completed before the stop.

``classify_blame`` is the single place that decides WHO can fix a failure
(``Blame``): the user's code, a value the user's function produced, the
user's setup, or SciStack itself. Every record carries the answer; the
journal, the GUI and MATLAB (``+scifor/failure_from_mexception.m``) only
read it. :class:`ActionableError` marks an error SciStack raises on purpose
about something the user can change; :func:`user_value_seam` turns an
unexpected failure while SciStack handles the user's values into a
:class:`UserValueError`.

See docs/claude/error-surfacing.md.
"""

from __future__ import annotations

import importlib.util
import inspect
import linecache
import os
import sysconfig
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Literal

from scistacklog import Log
from scistacklog.journal import blame_headline

if TYPE_CHECKING:
    import pandas as pd

FrameOrigin = Literal["user", "framework", "library"]
#: Who can fix a failure — see :func:`classify_blame`.
Blame = Literal["no_data", "user_value", "setup", "user_code", "internal"]
BLAME_VALUES = ("no_data", "user_value", "setup", "user_code", "internal")
# "run": an exception outside the per-combo loop (a save failure, a setup
# error), recorded by a caller that catches what escaped for_each.
FailureStage = Literal["prepare_inputs", "call", "run"]

# Wire-size caps: a failure travels in progress events and run_done messages.
MESSAGE_MAX_CHARS = 2000
FRAMES_MAX = 50
WIRE_COMBOS_MAX = 20

# Top-level packages whose frames are SciStack's own plumbing. A frame inside
# one of these is never "the line that raised" from the user's point of view.
FRAMEWORK_PACKAGES = (
    "scifor",
    "scidb",
    "scihist",
    "scilineage",
    "sciduckdb",
    "scistacklog",
    "scicanonicalhash",
    "scimatlab",
    "scistack_gui",
    "scistackplot",
    "scistackplotdb",
    "scistackstats",
    "scistackstatsdb",
)

ON_ERROR_VALUES = ("continue", "stop")


# ---------------------------------------------------------------------------
# Errors the user can act on
# ---------------------------------------------------------------------------


class ActionableError(Exception):
    """An error SciStack raises ON PURPOSE about something the user can
    change: their setup, their data, or what their function returned. Its
    message says what to change.

    The marker is what separates "SciStack refused, and says why" from
    "SciStack broke": an exception raised inside SciStack's own code without
    this marker is classified ``internal``. ``scidb.SciStackError`` and
    scifor's ``SchemaKeyTypeError`` subclass it.

    ``blame`` and ``probable`` are read by :func:`classify_blame`; a subclass
    whose blame depends on its contents (``scidb.OutputSaveError``) overrides
    them as properties.
    """

    blame: str = "setup"
    probable: bool = False


class UserValueError(ActionableError):
    """Raised inside SciStack, caused by a value the user's function
    produced (or was handed), fixable by the user.

    ``probable=False``: a check identified the cause, and the message names
    it (and the combos it applies to). ``probable=True``: an unexpected
    exception escaped one of SciStack's steps that handle the user's values
    (:func:`user_value_seam`); the cause is most likely the values, but the
    message says it may be a SciStack bug. The original exception is the
    ``__cause__`` either way, so the debug log keeps its traceback.
    """

    blame = "user_value"

    def __init__(
        self,
        message: str,
        *,
        fn_name: str | None = None,
        output: str | None = None,
        stage: str | None = None,
        probable: bool = False,
    ) -> None:
        super().__init__(message)
        self.fn_name = fn_name
        self.output = output
        self.stage = stage
        self.probable = probable


def validate_on_error(on_error: str) -> str:
    """The one check of the ``on_error`` run option (every layer calls it)."""
    if on_error not in ON_ERROR_VALUES:
        raise ValueError(
            f"on_error must be one of {ON_ERROR_VALUES}, got {on_error!r}"
        )
    return on_error


# ---------------------------------------------------------------------------
# Frame classification
# ---------------------------------------------------------------------------


def _norm(path: str) -> str:
    return os.path.normcase(os.path.realpath(path))


@lru_cache(maxsize=1)
def _framework_dirs() -> tuple[str, ...]:
    """Directories of the installed framework packages (found, not imported)."""
    dirs: list[str] = []
    for name in FRAMEWORK_PACKAGES:
        try:
            spec = importlib.util.find_spec(name)
        except (ImportError, ValueError):
            continue
        if spec is None:
            continue
        for loc in spec.submodule_search_locations or ():
            dirs.append(_norm(loc))
    return tuple(dirs)


@lru_cache(maxsize=1)
def _library_dirs() -> tuple[str, ...]:
    paths = sysconfig.get_paths()
    dirs = {
        _norm(paths[k])
        for k in ("stdlib", "platstdlib", "purelib", "platlib")
        if paths.get(k)
    }
    dirs.add(_norm(os.path.dirname(os.__file__)))
    return tuple(sorted(dirs))


def _under(path: str, dirs: tuple[str, ...]) -> bool:
    return any(path == d or path.startswith(d + os.sep) for d in dirs)


@lru_cache(maxsize=4096)
def frame_origin(filename: str) -> FrameOrigin:
    """Classify a traceback frame's file. The ONLY frame classifier.

    Framework is checked first: an editable install of a SciStack package
    lives outside site-packages, and a regular install lives inside it.
    Pseudo-files (``<string>``, ``<lambda>`` sources exec'd by glue code) are
    the user's.
    """
    if not filename or filename.startswith("<"):
        return "user"
    path = _norm(filename)
    if _under(path, _framework_dirs()):
        return "framework"
    if _under(path, _library_dirs()):
        return "library"
    return "user"


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FailureFrame:
    file: str
    line: int
    function: str
    code: str
    origin: FrameOrigin

    def to_dict(self) -> dict:
        return {
            "file": self.file,
            "line": self.line,
            "function": self.function,
            "code": self.code,
            "origin": self.origin,
        }


@dataclass(frozen=True)
class ComboFailure:
    fn_name: str
    metadata: dict[str, str]
    stage: FailureStage
    exc_type: str
    message: str
    frames: tuple[FailureFrame, ...]
    user_frame: FailureFrame | None
    no_data: bool
    blame: str = "internal"
    probable: bool = False

    @property
    def reason(self) -> str:
        """Grouping key — the same ``"{Type}: {message}"`` the log shows."""
        return f"{self.exc_type}: {self.message}"

    def to_dict(self) -> dict:
        return {
            "fn_name": self.fn_name,
            "metadata": dict(self.metadata),
            "stage": self.stage,
            "exc_type": self.exc_type,
            "message": self.message,
            "reason": self.reason,
            "frames": [f.to_dict() for f in self.frames],
            "user_frame": self.user_frame.to_dict() if self.user_frame else None,
            "no_data": self.no_data,
            "blame": self.blame,
            "probable": self.probable,
        }


@dataclass
class FailureGroup:
    """Every combo that failed with one ``reason``; ``example`` is the first."""

    example: ComboFailure
    combos: list[str] = field(default_factory=list)

    @property
    def reason(self) -> str:
        return self.example.reason

    @property
    def no_data(self) -> bool:
        return self.example.no_data

    @property
    def count(self) -> int:
        return len(self.combos)

    def to_dict(self) -> dict:
        return {
            "reason": self.reason,
            "fn_name": self.example.fn_name,
            "exc_type": self.example.exc_type,
            "message": self.example.message,
            "no_data": self.no_data,
            "count": self.count,
            "combos": self.combos[:WIRE_COMBOS_MAX],
            "user_frame": (
                self.example.user_frame.to_dict() if self.example.user_frame else None
            ),
            "blame": self.example.blame,
            "probable": self.example.probable,
            # Display text for the GUI; the wording is scistacklog.journal's.
            "blame_label": blame_headline(
                self.example.blame, self.example.probable, self.example.fn_name
            ),
            "example": self.example.to_dict(),
        }


def exception_reason(exc: BaseException) -> str:
    """``"{Type}: {message}"`` with the message capped — the grouping key."""
    return f"{type(exc).__name__}: {_cap(str(exc))}"


def _cap(text: str) -> str:
    if len(text) <= MESSAGE_MAX_CHARS:
        return text
    return text[: MESSAGE_MAX_CHARS - 1] + "…"


def _frames_of(exc: BaseException) -> list[FailureFrame]:
    frames = []
    for fs in traceback.extract_tb(exc.__traceback__):
        code = fs.line if fs.line is not None else linecache.getline(fs.filename, fs.lineno or 0)
        frames.append(
            FailureFrame(
                file=fs.filename,
                line=fs.lineno or 0,
                function=fs.name,
                code=(code or "").strip(),
                origin=frame_origin(fs.filename),
            )
        )
    return frames


# ---------------------------------------------------------------------------
# Who can fix it
# ---------------------------------------------------------------------------


def call_user_function(fn: Callable, /, *args: Any, **kwargs: Any) -> Any:
    """Call the user's function. Every for_each call of ``fn`` goes through
    here, because :func:`classify_blame` looks for this frame: everything
    deeper in a traceback is the user's function at work, even when that
    function is a library's (``pandas.read_csv`` wired up in the GUI)."""
    return fn(*args, **kwargs)


def _is_user_call_frame(frame: FailureFrame) -> bool:
    return frame.function == call_user_function.__name__ and frame.origin == "framework"


def classify_blame(
    exc: BaseException, frames: "list[FailureFrame]", *, no_data: bool = False
) -> "tuple[str, bool, str]":
    """Who can fix this failure: ``(blame, probable, rule)``. The ONE classifier.

    Rules, in order:

    1. ``no_data`` — a combo with no data (NoDataError). Not an error.
    2. The exception is an :class:`ActionableError` — its own ``blame``
       (``setup``; ``user_value`` for :class:`UserValueError`).
    3. Find the innermost frame that is not library code. If it is the
       user's, or it is the :func:`call_user_function` frame itself (a
       library function the user chose failed on what it was given) —
       ``user_code``.
    4. Otherwise (SciStack's own code raised, unmarked) — ``internal``.

    Only the exception itself is checked for the marker, not its
    ``__context__``: a bug raised while handling an ActionableError is
    still a bug. ``frames`` are the untrimmed frames the record was built
    from. ``rule`` says which rule decided (logged at DEBUG, so a wrong
    label can be traced to its cause).
    """
    if no_data:
        return "no_data", False, "NoDataError"
    if isinstance(exc, ActionableError):
        blame = str(getattr(exc, "blame", "setup"))
        if blame not in BLAME_VALUES:
            blame = "setup"
        return blame, bool(getattr(exc, "probable", False)), f"{type(exc).__name__} is an ActionableError"
    innermost = next((f for f in reversed(frames) if f.origin != "library"), None)
    if innermost is None:
        return "internal", False, "no non-library frame"
    where = f"{innermost.file}:{innermost.line}"
    if innermost.origin == "user":
        return "user_code", False, f"innermost non-library frame is user code ({where})"
    if _is_user_call_frame(innermost):
        return "user_code", False, f"raised inside library code the user's function called directly ({where})"
    return "internal", False, f"innermost non-library frame is SciStack code ({where})"


def _trim(frames: list[FailureFrame]) -> list[FailureFrame]:
    """Drop the framework frames that lead INTO the user's code (the loop
    plumbing); keep everything from the first non-framework frame inward.
    A traceback with no non-framework frame (a failure in scifor's own input
    preparation) is kept whole — then the framework frames ARE the story."""
    for i, f in enumerate(frames):
        if f.origin != "framework":
            frames = frames[i:]
            break
    return frames[-FRAMES_MAX:]


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain, seen = [], set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        chain.append(cur)
        cur = cur.__cause__ or (None if cur.__suppress_context__ else cur.__context__)
    return chain


def failure_from_exception(
    exc: BaseException,
    *,
    fn_name: str,
    metadata: dict,
    stage: FailureStage,
    no_data: bool = False,
) -> ComboFailure:
    """Build the record. The traceback comes from the first exception in the
    ``__cause__``/``__context__`` chain that reaches user code — e.g. a
    for_columns ``ColumnFunctionError`` is raised by scifor ``from`` the
    first per-column error, whose frames hold the user's line."""
    frames: list[FailureFrame] = []
    for link in _exception_chain(exc):
        link_frames = _frames_of(link)
        if any(f.origin == "user" for f in link_frames):
            frames = link_frames
            break
    else:
        frames = _frames_of(exc)
    # Blame follows only EXPLICIT chaining (`raise X from user_error`, as
    # ColumnFunctionError does). An exception raised while merely HANDLING a
    # user error (implicit __context__) is judged by its own frames: a
    # SciStack bug in an except block must not read as the user's fault.
    def _reaches_user(fs: "list[FailureFrame]") -> bool:
        return any(f.origin == "user" for f in fs)

    blame_frames = _frames_of(exc)
    link: BaseException = exc
    while not _reaches_user(blame_frames) and link.__cause__ is not None:
        link = link.__cause__
        blame_frames = _frames_of(link)
    if not _reaches_user(blame_frames):
        blame_frames = _frames_of(exc)
    blame, probable, rule = classify_blame(exc, blame_frames, no_data=no_data)
    Log.debug(
        "failure_from_exception: %s in %s -> blame=%s%s via %s",
        type(exc).__name__,
        fn_name,
        blame,
        " (probable)" if probable else "",
        rule,
        layer="scifor",
    )
    frames = _trim(frames)
    user_frame = next((f for f in reversed(frames) if f.origin == "user"), None)
    if user_frame is None and not no_data:
        Log.debug(
            "failure_from_exception: no user frame in %s traceback (%d frame(s)); "
            "innermost=%s",
            type(exc).__name__,
            len(frames),
            f"{frames[-1].file}:{frames[-1].line}" if frames else "none",
            layer="scifor",
        )
    return ComboFailure(
        fn_name=fn_name,
        metadata={str(k): str(v) for k, v in metadata.items()},
        stage=stage,
        exc_type=type(exc).__name__,
        message=_cap(str(exc)),
        frames=tuple(frames),
        user_frame=user_frame,
        no_data=no_data,
        blame=blame,
        probable=probable,
    )


def run_failure(exc: BaseException, *, fn_name: str) -> ComboFailure:
    """The record of an exception that ended a whole run (outside the
    per-combo loop): a save, a setup check, combining the results."""
    return failure_from_exception(exc, fn_name=fn_name, metadata={}, stage="run")


# ---------------------------------------------------------------------------
# Steps that handle the user's values
# ---------------------------------------------------------------------------


#: How each seam stage reads in a message: "SciStack couldn't <verb> ...".
_SEAM_VERBS = {"combine_outputs": "combine", "save": "save"}


def _type_label(value: Any) -> str:
    """A short, scientist-readable kind for a value: ``DataFrame``,
    ``ndarray (2-D)``, ``float``, ``None``."""
    if value is None:
        return "None"
    name = type(value).__name__
    ndim = getattr(value, "ndim", None)
    if name == "ndarray" and isinstance(ndim, int):
        return f"ndarray ({ndim}-D)"
    return name


def describe_value_kinds(
    labelled: "list[tuple[str, Any]]", *, fn_name: str, output: str | None = None
) -> str | None:
    """When one output's values are of different kinds across combos, the
    message that names them; ``None`` when they all agree.

    ``labelled`` is ``(combo label, value)`` per combo. Used as the
    ``diagnose`` of :func:`user_value_seam`: it runs only after the step has
    already failed, so a run that works is never refused by it.
    """
    by_kind: dict[str, list[str]] = {}
    for label, value in labelled:
        by_kind.setdefault(_type_label(value), []).append(label)
    if len(by_kind) < 2:
        return None
    ranked = sorted(by_kind.items(), key=lambda kv: -len(kv[1]))
    parts = []
    for kind, labels in ranked:
        shown = "; ".join(labels[:3])
        more = f"; +{len(labels) - 3} more" if len(labels) > 3 else ""
        parts.append(f"{kind} for {len(labels)} combo(s) ({shown}{more})")
    what = f"output {output!r} of {fn_name}" if output else f"{fn_name}'s output"
    return (
        f"{what} has different kinds of value in different combos: "
        + ", but ".join(parts)
        + ". SciStack can only combine one kind per output. Make the function "
        "return the same kind of value for every combo."
    )


def _definition_site(fn: Any) -> str:
    try:
        target = inspect.unwrap(fn)
        file = inspect.getsourcefile(target)
        line = inspect.getsourcelines(target)[1]
    except (TypeError, OSError, ValueError):
        return ""
    return f" (defined at {file}:{line})" if file else ""


def as_user_value_error(
    exc: BaseException,
    *,
    stage: str,
    fn_name: str,
    fn: Any = None,
    output: str | None = None,
    diagnose: "Callable[[], str | None] | None" = None,
) -> BaseException:
    """What an exception from a step that handles the user's values becomes.

    An :class:`ActionableError` is returned unchanged: it already says what
    to change. Anything else becomes a :class:`UserValueError` whose
    ``__cause__`` is the original: precise (``probable=False``) when
    ``diagnose`` names the cause, hedged (``probable=True``) when it can't.
    Each hedged one logs ``[seam_fallback]`` — a step that has no check for
    this cause yet; give it one, and a regression test.
    """
    if isinstance(exc, ActionableError):
        return exc
    precise = None
    if diagnose is not None:
        try:
            precise = diagnose()
        except Exception as diag_exc:  # noqa: BLE001 — diagnosis must not mask the error
            Log.debug(f"[seam] diagnose for {stage} raised {diag_exc!r}", layer="scifor")
    if precise:
        new = UserValueError(precise, fn_name=fn_name, output=output, stage=stage)
    else:
        what = f"the output {output!r} of {fn_name}" if output else f"the outputs of {fn_name}"
        new = UserValueError(
            f"SciStack couldn't {_SEAM_VERBS.get(stage, 'process')} {what}"
            f"{_definition_site(fn)}: {type(exc).__name__}: {exc}. This usually "
            f"means the function returns different kinds, shapes or columns "
            f"for different combos.",
            fn_name=fn_name,
            output=output,
            stage=stage,
            probable=True,
        )
        Log.warn(
            f"[seam_fallback] stage={stage} fn={fn_name} output={output} "
            f"{type(exc).__name__}: {exc} — no check names this cause yet",
            layer="scifor",
        )
    new.__cause__ = exc
    return new


@contextmanager
def user_value_seam(
    stage: str,
    *,
    fn_name: str,
    fn: Any = None,
    output: str | None = None,
    diagnose: "Callable[[], str | None] | None" = None,
):
    """Wrap a SciStack step that processes the user's values (combining
    outputs across combos, saving them). An unexpected exception escaping it
    is re-raised as a :class:`UserValueError` (see :func:`as_user_value_error`),
    so the journal says the user can fix it instead of blaming SciStack.

    Usage::

        with user_value_seam("combine_outputs", fn_name=name, diagnose=...):
            result = _results_to_output_dataframe(...)
    """
    try:
        yield
    except Exception as e:
        new = as_user_value_error(
            e, stage=stage, fn_name=fn_name, fn=fn, output=output, diagnose=diagnose
        )
        if new is e:
            raise
        raise new from e


# ---------------------------------------------------------------------------
# The journal entry of a run
# ---------------------------------------------------------------------------


@contextmanager
def journal_run(fn_name: str, *, enabled: bool = True):
    """The journal block for one for_each call (``Log.run``), with the
    exception that ends it classified here, where :func:`classify_blame`
    lives. Nested calls share the outermost block. ``enabled=False`` (a dry
    run) writes nothing.
    """
    if not enabled:
        yield None
        return
    with Log.run(fn_name) as run:
        try:
            yield run
        except Exception as e:
            if stopped_run_of(e) is not None:
                run.fail(None)  # on_error="stop": the summary already has it
            else:
                run.fail(run_failure(e, fn_name=fn_name).to_dict())
            raise


def journaled(impl: Callable) -> Callable:
    """Decorate a for_each so each call is one journal block (:func:`journal_run`).

    Reads ``fn`` and ``dry_run`` from the call (a dry run writes nothing);
    ``functools.wraps`` keeps the signature, so ``inspect.signature`` and the
    scifor/scidb parity tests see the real one. scifor and scidb both use it;
    a scidb call wrapping a scifor call is still one block.
    """
    import functools

    sig = inspect.signature(impl)

    @functools.wraps(impl)
    def wrapper(*args, **kwargs):
        try:
            bound = sig.bind_partial(*args, **kwargs).arguments
        except TypeError:
            bound = {}
        fn = bound.get("fn", args[0] if args else kwargs.get("fn"))
        name = getattr(fn, "__name__", None) or repr(fn)
        with journal_run(name, enabled=not bound.get("dry_run", False)):
            return impl(*args, **kwargs)

    return wrapper


def format_user_frame(frame: FailureFrame | None) -> str:
    """``at path:line in fn`` — the one rendering logs and the console use."""
    if frame is None:
        return "no user frame (failed inside SciStack — see traceback)"
    return f"at {frame.file}:{frame.line} in {frame.function}"


# ---------------------------------------------------------------------------
# on_error="stop"
# ---------------------------------------------------------------------------

_STOPPED_ATTR = "scifor_stopped"


@dataclass(frozen=True)
class StoppedRun:
    """Attached to the original exception when ``on_error="stop"`` fires."""

    failure: ComboFailure
    partial_result: "pd.DataFrame | None"
    completed: int


def attach_stopped_run(
    exc: BaseException, stopped: StoppedRun, *, note: bool = True
) -> BaseException:
    """Attach (or replace) the StoppedRun. ``note`` adds a one-line
    ``add_note`` to the traceback; pass False when re-attaching."""
    setattr(exc, _STOPPED_ATTR, stopped)
    if not note:
        return exc
    text = (
        f"scifor.for_each(on_error='stop'): stopped at "
        f"{', '.join(f'{k}={v}' for k, v in stopped.failure.metadata.items()) or 'the only combo'}"
        f" in {stopped.failure.fn_name}; {stopped.completed} combo(s) completed before it."
    )
    if hasattr(exc, "add_note"):  # Python 3.11+
        exc.add_note(text)
    return exc


def stopped_run_of(exc: BaseException) -> StoppedRun | None:
    """The StoppedRun an ``on_error="stop"`` exception carries, else None."""
    stopped = getattr(exc, _STOPPED_ATTR, None)
    return stopped if isinstance(stopped, StoppedRun) else None


def groups_to_wire(groups: "dict[str, FailureGroup]") -> list[dict]:
    """Failure groups in first-seen order, as the wire shape."""
    return [g.to_dict() for g in groups.values()]


# ---------------------------------------------------------------------------
# Wire groups from MATLAB
# ---------------------------------------------------------------------------


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, dict):
        return [value]  # MATLAB jsonencode writes a 1-element array as an object
    return list(value) if isinstance(value, (list, tuple)) else []


def _frame_or_none(value) -> "dict | None":
    if isinstance(value, list):  # jsonencode([]) for "no frame"
        value = value[0] if value else None
    return value if isinstance(value, dict) and value.get("file") is not None else None


def groups_from_wire(raw) -> list[dict]:
    """Failure groups read back from JSON — the MATLAB writer's
    (``+scifor/failure_from_mexception.m`` via ``+scidb/run_marker.m``) or
    Python's own — normalised to :meth:`FailureGroup.to_dict`'s shape.

    MATLAB's ``jsonencode`` writes a one-element array as a bare object and
    "no value" as ``[]``; both are undone here, so a consumer never knows
    which language failed. Malformed entries are dropped (logged), never
    raised: this reads diagnostics about a run, not the run itself.
    """
    out: list[dict] = []
    for g in _as_list(raw):
        if not isinstance(g, dict) or "reason" not in g:
            Log.debug(f"groups_from_wire: dropping malformed group {str(g)[:200]}", layer="scifor")
            continue
        example = g.get("example") if isinstance(g.get("example"), dict) else {}
        frames = [f for f in _as_list(example.get("frames")) if isinstance(f, dict)]
        combos = g.get("combos")
        no_data = bool(g.get("no_data"))
        user_frame = _frame_or_none(g.get("user_frame"))
        blame = _wire_blame(g.get("blame") or example.get("blame"), no_data, user_frame)
        probable = bool(g.get("probable") or example.get("probable"))
        out.append(
            {
                **g,
                "no_data": no_data,
                "count": int(g.get("count") or 0),
                "combos": [combos] if isinstance(combos, str) else [str(c) for c in _as_list(combos)],
                "user_frame": user_frame,
                "blame": blame,
                "probable": probable,
                "blame_label": blame_headline(blame, probable, str(g.get("fn_name") or "")),
                "example": {
                    **example,
                    "frames": frames,
                    "user_frame": _frame_or_none(example.get("user_frame")),
                    "blame": blame,
                    "probable": probable,
                },
            }
        )
    return out


def _wire_blame(raw, no_data: bool, user_frame) -> str:
    """A wire group's blame. A writer that predates the field (or sent an
    unknown value) gets the frame rule's answer, logged so it is visible."""
    if raw in BLAME_VALUES:
        return str(raw)
    inferred = "no_data" if no_data else ("user_code" if user_frame else "internal")
    Log.debug(f"groups_from_wire: blame {raw!r} not on the wire; inferred {inferred}", layer="scifor")
    return inferred
