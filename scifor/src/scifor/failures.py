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

See docs/claude/error-surfacing.md.
"""

from __future__ import annotations

import importlib.util
import linecache
import os
import sysconfig
import traceback
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

from scistacklog import Log

if TYPE_CHECKING:
    import pandas as pd

FrameOrigin = Literal["user", "framework", "library"]
FailureStage = Literal["prepare_inputs", "call"]

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
    )


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
