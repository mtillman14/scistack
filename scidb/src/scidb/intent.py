"""Intent and fact: statements about runs that should happen, resolved
against the record of runs that did.

Conceptual reference: ``docs/claude/intent-and-fact.md``. Plan:
``.claude/plan-intent-and-fact.md``. This module is Stage 1 — the vocabulary
and the resolver, with no behaviour change anywhere else.

**Fact** is provenance: what a run did. It is never edited and never
overridden; it is the floor resolution falls back to.

**Intent** is a statement about a run that has not happened yet — which
columns, which values are in play, which source feeds which parameter, which
schema levels iterate, which run options. It is authored by a person and is
revisable, so it may contradict what history recorded. That contradiction is
a user changing their mind, not corruption.

The five rules, implemented here:

1. A :class:`Statement` attaches to a SUBJECT (a call site, a parameter, an
   edge, a variable type), names an ASPECT, and carries a SCOPE.
2. A SURFACE is where statements are written — a source file, or the GUI's
   intent store. Surfaces are peers; neither is a view of the other.
3. A run's ORIGIN names which surfaces it reads, in order (``ORIGIN_SURFACES``).
   A statement on a surface this origin does not read is not applied — it is
   reported as *unread* so the canvas can say "stated here, not used by the
   last run".
4. RESOLUTION: nearest scope wins within a surface, then surfaces in origin
   order, then history as the floor. :func:`resolve` is the one place.
5. Fact is never intent. Provenance is not a surface and is not merged into
   one; it is compared against, and what it recorded is carried on the
   :class:`Decision` so a loss can be reported.

Pure: no I/O, no DB, no GUI import. The intent STORE stays in the GUI layer
(``scistack_gui.pipeline_store``); this module defines the shape, the aspect
normalizers and the resolver, and takes a plain payload — so MATLAB, the CLI
and plain Python can resolve without a GUI table. If scidb ever has to import
``pipeline_store``, the design is wrong.

**Batching is a design constraint, not an optimisation.** Resolution sits on
the hot path of every run, every canvas render and every export, so
:func:`resolve_many` (index once, resolve per subject) is the documented
entry point; calling :func:`resolve` per node inside a loop over a flat
statement list is the N+1 trap that the ``*_batch`` provenance helpers exist
to avoid.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

#: The scope every statement falls back to: "everywhere, in every pipeline".
#: Resolution walks ``scope -> GLOBAL_SCOPE`` and nothing else — there is no
#: inheritance between hypotheses (a duplicate COPIES its rows instead; see
#: the doc, §7 "Rejected").
GLOBAL_SCOPE = "global"

#: Surfaces: where statements are written. Fact is deliberately NOT one.
SURFACE_STORE = "store"
SURFACE_SOURCE = "source"
SURFACES = (SURFACE_STORE, SURFACE_SOURCE)

#: Reported as the "surface" of a value that came from provenance. Never a
#: legal ``Statement.surface`` — rule 5.
FROM_HISTORY = "history"

#: Subject kinds: what a statement attaches to.
SUBJECT_CALL_SITE = "call_site"
SUBJECT_PARAMETER = "parameter"
SUBJECT_EDGE = "edge"
SUBJECT_VARIABLE_TYPE = "variable_type"
SUBJECT_KINDS = (
    SUBJECT_CALL_SITE,
    SUBJECT_PARAMETER,
    SUBJECT_EDGE,
    SUBJECT_VARIABLE_TYPE,
)

#: Aspects: which normalizer owns the payload. One owner each.
ASPECT_COLUMNS = "columns"
ASPECT_RUN_OPTIONS = "run_options"
ASPECT_SCHEMA_LOCATION = "schema_location"
ASPECT_WIRING = "wiring"
ASPECT_HIDDEN = "hidden"
ASPECT_CONSTANTS = "constants"
#: A plot's pin over variant space — a named ``VariantSet`` — is intent about
#: which rows a figure reads, keyed by the set's name. It has no FACT side (a
#: pin is not something a run records), so it resolves with history absent.
ASPECT_VARIANT_SELECTION = "variant_selection"
ASPECTS = (
    ASPECT_COLUMNS,
    ASPECT_RUN_OPTIONS,
    ASPECT_SCHEMA_LOCATION,
    ASPECT_WIRING,
    ASPECT_HIDDEN,
    ASPECT_CONSTANTS,
    ASPECT_VARIANT_SELECTION,
)

#: Aspects whose value is a mapping KEYED BY PART (parameter, constant name,
#: edge id). The rest carry one value for the whole subject. The distinction
#: is explicit rather than inferred from the payload's type, because a column
#: selection is itself a mapping and "is this keyed or is this the value?"
#: cannot be answered by looking.
PER_KEY_ASPECTS = (
    ASPECT_COLUMNS,
    ASPECT_WIRING,
    ASPECT_CONSTANTS,
    ASPECT_HIDDEN,
    ASPECT_VARIANT_SELECTION,
)

#: Origins, and the surfaces each reads IN ORDER (rule 3).
#:
#: ``script`` reads source only, by decision (2026-09-19): a checkbox ticked
#: in the GUI last week must not silently change what ``python pipeline.py``
#: does. ``replay`` reads no surface at all — it reproduces a recorded
#: invocation, so history is the whole answer.
ORIGIN_GUI = "gui"
ORIGIN_SCRIPT = "script"
ORIGIN_REPLAY = "replay"
ORIGIN_SURFACES: dict[str, tuple[str, ...]] = {
    ORIGIN_GUI: (SURFACE_STORE, SURFACE_SOURCE),
    ORIGIN_SCRIPT: (SURFACE_SOURCE,),
    ORIGIN_REPLAY: (),
}


class IntentError(ValueError):
    """A malformed statement or an unknown origin/aspect/surface."""


@dataclass(frozen=True)
class Statement:
    """One unit of intent.

    ``key`` is the part of the aspect this statement addresses — a parameter
    name for ``columns``/``wiring``, a constant name for ``constants``, an
    edge id for ``hidden``. ``None`` means the statement is about the aspect
    as a whole (``run_options`` for a call site, say).

    ``stated_at`` is a monotonically comparable stamp (an ISO string or an
    int); later wins among otherwise equal statements, which is what makes
    "the user's most recent statement" a query rather than a code path.
    """

    subject_kind: str
    subject_ref: str
    aspect: str
    value: Any
    key: str | None = None
    scope: str = GLOBAL_SCOPE
    surface: str = SURFACE_STORE
    stated_at: Any = None

    def __post_init__(self) -> None:
        if self.surface not in SURFACES:
            raise IntentError(
                f"{self.surface!r} is not a surface ({', '.join(SURFACES)}). "
                f"Provenance is not a surface — see rule 5."
            )
        if self.aspect not in ASPECTS:
            raise IntentError(
                f"{self.aspect!r} is not an aspect ({', '.join(ASPECTS)})."
            )
        if self.subject_kind not in SUBJECT_KINDS:
            raise IntentError(
                f"{self.subject_kind!r} is not a subject kind "
                f"({', '.join(SUBJECT_KINDS)})."
            )

    @property
    def subject(self) -> tuple[str, str]:
        return (self.subject_kind, self.subject_ref)


@dataclass(frozen=True)
class Loser:
    """A value that did not win, kept so the winner can be explained."""

    value: Any
    surface: str  # a SURFACE, or FROM_HISTORY
    scope: str | None = None
    why: str = ""


@dataclass(frozen=True)
class Field:
    """One resolved ``(aspect, key)``: the winner, and what it beat."""

    aspect: str
    key: str | None
    value: Any
    surface: str  # a SURFACE, or FROM_HISTORY when no statement applied
    scope: str | None = None
    beat: tuple[Loser, ...] = ()

    @property
    def from_history(self) -> bool:
        return self.surface == FROM_HISTORY

    @property
    def recorded(self) -> Any:
        """What history recorded for this field, or ``None``.

        The read-side round-trip guard is one comparison against this: a
        field whose winner carries no selection while ``recorded`` holds one
        is a selection that was LOST, not changed.
        """
        if self.from_history:
            return self.value
        for loser in self.beat:
            if loser.surface == FROM_HISTORY:
                return loser.value
        return None


@dataclass
class Decision:
    """Why the plan says what it says.

    ``fields`` explains every resolved value; ``unread`` lists statements
    that were not applied because this origin does not read their surface
    (rule 3) — the input to the canvas marker "stated here, not used by the
    last run". ``out_of_scope`` lists statements dropped because they belong
    to another scope.
    """

    origin: str
    scope: str
    surfaces_read: tuple[str, ...]
    fields: dict[tuple[str, str | None], Field] = field(default_factory=dict)
    unread: list[Statement] = field(default_factory=list)
    out_of_scope: list[Statement] = field(default_factory=list)

    def get(self, aspect: str, key: str | None = None) -> Field | None:
        return self.fields.get((aspect, key))

    def for_aspect(self, aspect: str) -> list[Field]:
        return [f for (a, _), f in self.fields.items() if a == aspect]

    def lost(self, aspect: str = ASPECT_COLUMNS) -> list[Field]:
        """Fields where history recorded something and the winner carries
        nothing — the shape of a dropped selection."""
        return [f for f in self.for_aspect(aspect) if not f.value and f.recorded]


@dataclass
class RunPlan:
    """The resolved result: what actually runs.

    Aspect values are reachable generically (:meth:`get`) and by name, so no
    aspect is special-cased in the resolver while callers still read
    ``plan.columns`` rather than ``plan.aspects["columns"]``.
    """

    aspects: dict[str, dict[str | None, Any]] = field(default_factory=dict)
    decision: Decision | None = None

    def get(self, aspect: str, key: str | None = None, default: Any = None) -> Any:
        return self.aspects.get(aspect, {}).get(key, default)

    def by_key(self, aspect: str) -> dict[str, Any]:
        """``{key: value}`` for an aspect, dropping the whole-aspect entry."""
        return {k: v for k, v in self.aspects.get(aspect, {}).items() if k is not None}

    @property
    def columns(self) -> dict[str, Any]:
        return self.by_key(ASPECT_COLUMNS)

    @property
    def wiring(self) -> dict[str, Any]:
        return self.by_key(ASPECT_WIRING)

    @property
    def constants(self) -> dict[str, Any]:
        return self.by_key(ASPECT_CONSTANTS)

    @property
    def hidden(self) -> dict[str, Any]:
        return self.by_key(ASPECT_HIDDEN)

    @property
    def run_options(self) -> Any:
        return self.get(ASPECT_RUN_OPTIONS, None, {}) or {}

    @property
    def schema_location(self) -> Any:
        return self.get(ASPECT_SCHEMA_LOCATION, None)


# ---------------------------------------------------------------------------
# Aspect normalizers
# ---------------------------------------------------------------------------
# One owner per aspect: the payload shape, its "absent" rule, its comparison
# and (where it is stored) its exact serialisation. Aspects arrive here as
# the plan's stages land; `columns` is the first.


def normalize_columns(raw: Any) -> dict | None:
    """``{"columns": [...], "iterate": bool}`` for a column selection, or
    ``None`` when there is effectively no selection at all.

    THE owner of this shape. ``scistack_gui.domain.column_selection`` and
    ``provenance_save.compute_input_selectors`` both route through it — they
    used to normalize independently and agree by convention, which is how a
    ``for_columns`` selection reached storage as "no selection".

    Tolerates every shape the selection arrives in, because it crosses a JSON
    boundary written by a frontend that has been through several revisions:

    * ``"filename"`` — a bare column name;
    * ``["a", "b"]`` — a bare column list;
    * ``{"columns": [...], "iterate": bool}`` — the canonical dict;
    * ``{"columns": "a"}`` / ``{"iterate": true}`` — partial dicts;
    * a binding dict that also carries ``kind``/``ref``;
    * a ``ColumnSelection`` (or anything with ``.columns``/``.iterate``).

    Returns ``None`` for an EMPTY, non-iterate selection: no columns and no
    iteration means "the whole variable", which is what binding the bare
    class already does, and wrapping it anyway forks the version key for no
    change in what the function receives. An empty selection WITH ``iterate``
    is kept — ``MyVar.for_columns()`` means "every data column, one call
    each", resolved at for_each time.
    """
    if raw is None:
        return None
    if isinstance(raw, str):
        columns: list = [raw] if raw else []
        iterate = False
    elif isinstance(raw, (list, tuple)):
        columns = [str(c) for c in raw if c]
        iterate = False
    elif isinstance(raw, Mapping):
        cols = raw.get("columns")
        if isinstance(cols, str):
            cols = [cols] if cols else []
        columns = [str(c) for c in (cols or []) if c]
        iterate = bool(raw.get("iterate"))
    elif hasattr(raw, "columns") or hasattr(raw, "iterate"):
        # A live ColumnSelection (scifor's or scidb's subclass).
        columns = [str(c) for c in (getattr(raw, "columns", None) or []) if c]
        iterate = bool(getattr(raw, "iterate", False))
    else:
        logger.warning(
            "[intent] ignoring a column selection of unexpected type %s: %r",
            type(raw).__name__,
            raw,
        )
        return None

    if not columns and not iterate:
        return None
    return {"columns": columns, "iterate": iterate}


def describe_columns(sel: Any) -> str:
    """The ONE spelling of a selection used in logs and in the canvas chip.

    Two spellings of the same thing is how a user ends up reading a log line
    that does not match what the node shows, so there is exactly one.
    """
    sel = normalize_columns(sel)
    if not sel:
        return "whole variable"
    columns = sel.get("columns") or []
    if sel.get("iterate"):
        return "per column" if not columns else f"per column ({len(columns)})"
    if len(columns) == 1:
        return f'"{columns[0]}"'
    return f"{len(columns)} columns"


def selector_json(sel: Any) -> str | None:
    """The stored spelling of a column selection (``_invocation_input.selector``).

    **The bytes here are identity.** ``compute_invocation_id`` folds this
    string in, so a change to the serialisation re-identifies every recorded
    call: ``iterate`` is emitted ONLY when set, and keys are sorted, exactly
    as the save path has always written them.
    """
    sel = normalize_columns(sel)
    if sel is None:
        return None
    out: dict = {"columns": list(sel["columns"])}
    if sel["iterate"]:
        out["iterate"] = True
    return json.dumps(out, sort_keys=True)


def parse_selector(stored: Any) -> dict | None:
    """A stored selector back into the canonical shape.

    Accepts the JSON string the DB holds, an already-parsed dict (the
    pipeline-variants path parses, the config path does not), or ``None``.
    """
    if stored is None:
        return None
    if isinstance(stored, str):
        if not stored.strip():
            return None
        try:
            stored = json.loads(stored)
        except (TypeError, ValueError):
            logger.warning("[intent] unparseable stored selector: %r", stored)
            return None
    return normalize_columns(stored)


def is_every_column(sel: Any) -> bool:
    """Is *sel* the SYMBOLIC "every data column, one call each"
    (``MyVar.for_columns()``, stated as ``{"columns": [], "iterate": true}``)?

    Symbolic because it is resolved at for_each time: the run records the
    column list it found, so a statement and its own run's fact spell the
    same selection two ways.
    """
    n = normalize_columns(sel)
    return bool(n) and n["iterate"] and not n["columns"]


def same_columns(a: Any, b: Any) -> bool:
    """Do two column selections mean the same thing? (``None`` == whole
    variable, so absent and empty compare equal.)

    "Every column" (:func:`is_every_column`) matches ANY per-column selection:
    a stated ``for_columns()`` and the resolved list its run recorded are the
    same intent. Comparing them literally flagged every such node "not
    reflected by its last run" (cleanup-audit B2). The columns a variable
    HAS may have changed since that run; that is a data question, not an
    intent one, and is not what this answers.
    """
    na, nb = normalize_columns(a), normalize_columns(b)
    if na == nb:
        return True
    if na and nb and na["iterate"] and nb["iterate"]:
        return is_every_column(na) or is_every_column(nb)
    return False


#: ``{aspect: normalizer}`` — a value entering resolution is normalized by
#: its aspect's owner, so a statement and a fact are always comparable.
NORMALIZERS = {ASPECT_COLUMNS: normalize_columns}


def normalize(aspect: str, value: Any) -> Any:
    fn = NORMALIZERS.get(aspect)
    return fn(value) if fn else value


# ---------------------------------------------------------------------------
# Resolution (rules 3 and 4)
# ---------------------------------------------------------------------------


def surfaces_for(origin: str) -> tuple[str, ...]:
    try:
        return ORIGIN_SURFACES[origin]
    except KeyError:
        raise IntentError(
            f"{origin!r} is not an origin ({', '.join(ORIGIN_SURFACES)})."
        ) from None


def index_statements(
    statements: Iterable[Statement],
) -> dict[tuple[str, str], list[Statement]]:
    """Group statements by subject ONCE.

    The batching entry point: filtering a flat list per node is the N+1 trap
    this module's docstring warns about.
    """
    out: dict[tuple[str, str], list[Statement]] = {}
    for st in statements:
        out.setdefault(st.subject, []).append(st)
    return out


def _rank(st: Statement, surfaces: tuple[str, ...], scope: str) -> tuple:
    """Sort key: nearest surface, then nearest scope, then most recent.

    Higher is better. Surface rank inverts the origin order so the FIRST
    surface a run reads wins.
    """
    surface_rank = len(surfaces) - surfaces.index(st.surface)
    scope_rank = 1 if st.scope == scope else 0
    return (surface_rank, scope_rank, _stamp_key(st.stated_at))


def _stamp_key(stamp: Any) -> tuple:
    """Comparable key for a ``stated_at`` of mixed or missing type."""
    if stamp is None:
        return (0, "")
    if isinstance(stamp, (int, float)):
        return (1, f"{stamp:020.6f}")
    return (1, str(stamp))


def resolve(
    statements: Iterable[Statement],
    fact: Mapping[str, Any] | None = None,
    *,
    origin: str = ORIGIN_GUI,
    scope: str = GLOBAL_SCOPE,
) -> RunPlan:
    """Resolve one subject's statements against its fact into a
    :class:`RunPlan` carrying its :class:`Decision`.

    *fact* is what provenance recorded, as ``{aspect: {key: value}}`` (or
    ``{aspect: value}`` for a whole-aspect value). It is the FLOOR: it wins
    only where no readable statement applies, and it is always carried on the
    Decision so a value that replaced it — or silently failed to — can be
    reported.

    Nothing here raises on a disagreement. Resolution reports; the guards
    that turn a report into a WARN live at the call sites, because only they
    know whether the difference matters.
    """
    surfaces = surfaces_for(origin)
    decision = Decision(origin=origin, scope=scope, surfaces_read=surfaces)
    plan = RunPlan(decision=decision)

    # Candidate statements, grouped by the field they address.
    by_field: dict[tuple[str, str | None], list[Statement]] = {}
    for st in statements:
        if st.surface not in surfaces:
            decision.unread.append(st)
            continue
        if st.scope not in (scope, GLOBAL_SCOPE):
            decision.out_of_scope.append(st)
            continue
        by_field.setdefault((st.aspect, st.key), []).append(st)

    fact_fields = _fact_fields(fact)

    for fkey in set(by_field) | set(fact_fields):
        aspect, key = fkey
        recorded = normalize(aspect, fact_fields.get(fkey))
        candidates = sorted(
            by_field.get(fkey, ()),
            key=lambda st: _rank(st, surfaces, scope),
            reverse=True,
        )
        if candidates:
            winner, *rest = candidates
            beat = [
                Loser(
                    value=normalize(aspect, st.value),
                    surface=st.surface,
                    scope=st.scope,
                    why="a nearer statement won",
                )
                for st in rest
            ]
            if fkey in fact_fields:
                beat.append(
                    Loser(value=recorded, surface=FROM_HISTORY, why="fact is the floor")
                )
            resolved = Field(
                aspect=aspect,
                key=key,
                value=normalize(aspect, winner.value),
                surface=winner.surface,
                scope=winner.scope,
                beat=tuple(beat),
            )
        else:
            resolved = Field(
                aspect=aspect,
                key=key,
                value=recorded,
                surface=FROM_HISTORY,
                scope=None,
            )
        decision.fields[fkey] = resolved
        plan.aspects.setdefault(aspect, {})[key] = resolved.value

    return plan


def resolve_many(
    subjects: Mapping[tuple[str, str], Mapping[str, Any]],
    statements: Iterable[Statement],
    *,
    origin: str = ORIGIN_GUI,
    scope: str = GLOBAL_SCOPE,
) -> dict[tuple[str, str], RunPlan]:
    """Resolve a whole graph in one pass.

    *subjects* maps ``(subject_kind, subject_ref)`` to that subject's fact.
    Statements are indexed once and looked up per subject, so cost is linear
    in statements plus subjects rather than their product — the reason this,
    not :func:`resolve`, is the entry point on a canvas render.
    """
    indexed = index_statements(statements)
    return {
        subject: resolve(
            indexed.get(subject, ()), fact, origin=origin, scope=scope
        )
        for subject, fact in subjects.items()
    }


def _fact_fields(fact: Mapping[str, Any] | None) -> dict[tuple[str, str | None], Any]:
    """``{aspect: {key: value}}`` or ``{aspect: value}`` → ``{(aspect, key): value}``."""
    out: dict[tuple[str, str | None], Any] = {}
    for aspect, value in (fact or {}).items():
        if aspect not in ASPECTS:
            logger.warning("[intent] ignoring fact for unknown aspect %r", aspect)
            continue
        if aspect in PER_KEY_ASPECTS and isinstance(value, Mapping):
            for key, inner in value.items():
                out[(aspect, key)] = inner
        else:
            out[(aspect, None)] = value
    return out


def describe_plan(plan: RunPlan) -> str:
    """One line naming every resolved input — the run's bindings summary.

    ``value: TrialMeanSymmetry["ankle", "knee"] · cycles: CycleSymmetry (whole
    variable)``. Rendered from the plan so the log, the canvas chip and the
    export cannot disagree about what a run was fed.
    """
    wiring = plan.wiring
    columns = plan.columns
    params = sorted(set(wiring) | set(columns))
    if not params:
        return "(no inputs)"
    parts = []
    for param in params:
        src = wiring.get(param)
        if isinstance(src, (list, tuple)):
            label = " | ".join(str(s) for s in src)
        elif isinstance(src, Mapping):
            ref = src.get("ref")
            label = " | ".join(ref) if isinstance(ref, (list, tuple)) else str(ref)
        else:
            label = str(src) if src is not None else "?"
        sel = columns.get(param)
        parts.append(f"{param}: {label} ({describe_columns(sel)})")
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# The run's origin (rule 3), as ambient state
# ---------------------------------------------------------------------------
# A run's origin is decided by whoever STARTS it — the GUI's run thread, a
# MATLAB command the GUI generated, a script — and read by whoever RECORDS it,
# deep inside the save path. Threading a keyword through every for_each
# signature between the two would touch four call chains for one string, so
# it travels as ambient state instead: a context variable for in-process
# callers, with an environment variable as the cross-process fallback (the
# MATLAB sidecar is a different process; the generated command exports it).

import contextvars as _contextvars
import os as _os

ORIGIN_ENV_VAR = "SCIDB_RUN_ORIGIN"

_current_origin: _contextvars.ContextVar[str | None] = _contextvars.ContextVar(
    "scidb_run_origin", default=None
)


def current_origin() -> str:
    """The origin of the run in progress: the context variable if set, else
    ``$SCIDB_RUN_ORIGIN``, else ``script`` — a run nobody labelled came from
    code, and code reads source only (decision A, 2026-09-19)."""
    value = _current_origin.get() or _os.environ.get(ORIGIN_ENV_VAR) or ORIGIN_SCRIPT
    if value not in ORIGIN_SURFACES:
        logger.warning(
            "[intent] unknown run origin %r — treating it as %r", value, ORIGIN_SCRIPT
        )
        return ORIGIN_SCRIPT
    return value


class run_origin:
    """``with run_origin("gui"): ...`` — label every run started inside.

    Re-entrant and thread-safe by construction (a context variable), so the
    GUI's run thread can set it without touching what a concurrent script
    run records.
    """

    def __init__(self, origin: str):
        if origin not in ORIGIN_SURFACES:
            raise IntentError(
                f"{origin!r} is not an origin ({', '.join(ORIGIN_SURFACES)})."
            )
        self.origin = origin
        self._token = None

    def __enter__(self):
        self._token = _current_origin.set(self.origin)
        return self

    def __exit__(self, *exc):
        if self._token is not None:
            _current_origin.reset(self._token)
        return False


def set_ambient_origin(origin: str) -> str:
    """Set the origin for every later run on this thread, with no scope to
    exit — the cross-language entry point.

    A MATLAB session hosting Python via ``py.*`` cannot use ``with``, and a
    ``setenv`` from MATLAB is invisible to an already-started interpreter
    (``os.environ`` is a snapshot). The GUI's generated MATLAB command calls
    ``py.scidb.intent.set_ambient_origin('gui')`` once, after its pyenv
    preamble, so the runs it launches record where they came from.
    """
    if origin not in ORIGIN_SURFACES:
        raise IntentError(f"{origin!r} is not an origin ({', '.join(ORIGIN_SURFACES)}).")
    _current_origin.set(origin)
    return origin


# ---------------------------------------------------------------------------
# The stored shape (read contract)
# ---------------------------------------------------------------------------
# The intent STORE is GUI-owned (scistack_gui.intent_store creates and writes
# it), but its ROW SHAPE is this module's: the columns are Statement's
# fields. Naming them here is what lets scidb's own inspector read the table
# back without importing the GUI — a store that scidb could not read would
# make "why does the data not reflect the canvas" unanswerable from the CLI.

INTENT_TABLE = "_intent"
INTENT_COLUMNS = (
    "subject_kind",
    "subject_ref",
    "scope",
    "aspect",
    "aspect_key",
    "value_json",
    "origin",
    "stated_at",
)


def statements_from_rows(rows: Iterable[tuple]) -> list[Statement]:
    """Rows in ``INTENT_COLUMNS`` order → statements, skipping bookkeeping
    rows and anything malformed (logged, never raised — a bad row must not
    take the readable ones with it)."""
    out: list[Statement] = []
    for row in rows:
        try:
            kind, ref, scope, aspect, key, raw, origin, stated_at = row
        except (TypeError, ValueError):
            logger.warning("[intent] malformed intent row: %r", row)
            continue
        if kind not in SUBJECT_KINDS:
            continue  # migration markers and the like
        try:
            value = json.loads(raw) if raw else None
        except (TypeError, ValueError):
            logger.warning("[intent] unparseable value on %s/%s: %r", kind, ref, raw)
            continue
        try:
            out.append(
                Statement(
                    subject_kind=kind,
                    subject_ref=ref,
                    aspect=aspect,
                    value=normalize(aspect, value),
                    key=key or None,
                    scope=scope or GLOBAL_SCOPE,
                    surface=origin,
                    stated_at=stated_at,
                )
            )
        except IntentError as exc:
            logger.warning("[intent] skipping row: %s", exc)
    return out


def load_statements_sql(duck, *, aspect: str | None = None) -> list[Statement]:
    """Read the store through any object with ``_fetchall(sql, params)`` —
    ``[]`` when the table does not exist (a database no GUI has opened)."""
    cols = ", ".join(INTENT_COLUMNS)
    sql = f"SELECT {cols} FROM {INTENT_TABLE}"
    params: list = []
    if aspect:
        sql += " WHERE aspect = ?"
        params.append(aspect)
    try:
        rows = duck._fetchall(sql, params)
    except Exception:
        return []
    return statements_from_rows(rows)
