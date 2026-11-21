"""Did that step work? Per-location state and the step verdict (GitHub #28).

The one owner of the per-(variable, location) answer and of the step verdict
a scientist reads on every surface: the node chip, the run report header, the
data view and the Activity Bar tree. Companion doc:
``docs/claude/pipeline-status.md``. Plan: ``.claude/plan-pipeline-introspection.md``.

**Planned locations** of a step's output (the denominator, no guessing): the
Cartesian product of the levels of the step's iteration keys, where a key's
levels are every value it has in ``_schema``, taken separately within each
stratum of the between keys (``scidb.store.between_keys``,
``scidb.provenance.inventory.strata_of``). Never derived from the output's
own records (Resolved 5): the keys come from the step's iteration level
(``scidb.run.schema_level.resolve_schema_level``, the owner ``for_each``
uses), the levels live from ``_schema`` on every call. Exclusions that govern
a location (global, scoped to the output, or inherited from an input:
``scidb.store.exclusions.governing_exclusions``) make it ⊘.

**Location states** (glyphs are shapes, so they read without colour):

====  ==============  ====================================================
✓     present         a record exists, current, no findings
⚠     warning         present, with an anomaly finding (``findings``)
↻     outdated        present, but the code changed or an input was re-saved
✕     failed          the last run raised here (error text kept)
↓     missing_input   not computed: an input has no data here
＋    new             inputs present, no output, the step ran before and
                      the inputs arrived since (attention, not a problem)
○     not_run         inputs present, the step has never run here
⊘     excluded        an exclusion governs it
====  ==============  ====================================================

**Step verdict** over the planned locations (scoped to a run's
``locations=`` for a run verdict): ✅ succeeded (every counted location ✓),
⚠ succeeded with warnings (every one present, some ⚠), otherwise not
complete with reasons in a fixed priority (✕ > ↓ > ○ > ↻), plus ⏸ cancelled
/ stopped for a run. ＋ new is never part of the word: it is an attention
note beside it. Every input is a stored fact (records, ``_run_outcome``,
exclusions, ``_schema``), so no surface computes a private version; GUI-only
reasons (an unconnected input edge) are added by the GUI in the same chip.

Everything is batched: a fixed number of queries per output variable and
input variable, never one per location. Each phase is timed
(``[timing] step_status(<fn>)``) so a slow refresh names its phase (Stage 0).
"""

from __future__ import annotations

import itertools
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from scidb.model.log import Log

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

PRESENT = "present"
WARNING = "warning"
OUTDATED = "outdated"
FAILED = "failed"
MISSING_INPUT = "missing_input"
NEW = "new"
NOT_RUN = "not_run"
EXCLUDED = "excluded"
STATES = (PRESENT, WARNING, OUTDATED, FAILED, MISSING_INPUT, NEW, NOT_RUN, EXCLUDED)

#: THE glyph of each state.
GLYPHS = {
    PRESENT: "✓",
    WARNING: "⚠",
    OUTDATED: "↻",
    FAILED: "✕",
    MISSING_INPUT: "↓",
    NEW: "＋",
    NOT_RUN: "○",
    EXCLUDED: "⊘",
}

#: THE words of each state.
LABELS = {
    PRESENT: "present",
    WARNING: "warning",
    OUTDATED: "outdated",
    FAILED: "failed",
    MISSING_INPUT: "missing input",
    NEW: "new",
    NOT_RUN: "not run",
    EXCLUDED: "excluded",
}

#: Why a location is outdated.
CODE_CHANGED = "code_changed"
INPUTS_CHANGED = "inputs_changed"

#: Merging several outputs at one location: the worst wins. Excluded is
#: lowest so that one output excluded and another failed reads failed.
_SEVERITY = {
    EXCLUDED: 0, PRESENT: 1, WARNING: 2, NEW: 3, OUTDATED: 4,
    NOT_RUN: 5, MISSING_INPUT: 6, FAILED: 7,
}

# The verdict words.
SUCCEEDED = "succeeded"
SUCCEEDED_WITH_WARNINGS = "succeeded_with_warnings"
NOT_COMPLETE = "not_complete"
VERDICT_GLYPHS = {SUCCEEDED: "✅", SUCCEEDED_WITH_WARNINGS: "⚠", NOT_COMPLETE: "✕"}
VERDICT_LABELS = {
    SUCCEEDED: "Succeeded",
    SUCCEEDED_WITH_WARNINGS: "Succeeded with warnings",
    NOT_COMPLETE: "Not complete",
}

# Reason kinds, in THE priority order (a run adds cancelled / stopped first).
CANCELLED = "cancelled"
STOPPED = "stopped"
REASON_ORDER = (CANCELLED, STOPPED, FAILED, MISSING_INPUT, NOT_RUN, CODE_CHANGED, INPUTS_CHANGED)
REASON_GLYPHS = {
    CANCELLED: "⏸", STOPPED: "⏸", FAILED: "✕", MISSING_INPUT: "↓", NOT_RUN: "○",
    CODE_CHANGED: "✎", INPUTS_CHANGED: "↑", WARNING: "⚠", EXCLUDED: "⊘", NEW: "＋",
}

#: Above this many planned locations per output the product is not built.
MAX_PLANNED = 1_000_000


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


@dataclass
class LocationStatus:
    """One (variable, location): its state and everything that explains it."""

    location: dict[str, str]
    state: str
    #: The sentence a renderer shows beside the glyph.
    detail: str = ""
    record_ids: list[str] = field(default_factory=list)
    #: ``code_changed`` / ``inputs_changed`` for an outdated location.
    outdated_by: str | None = None
    #: The input with no data here (missing input).
    missing_input: str | None = None
    #: Finding texts (warning).
    findings: list[dict] = field(default_factory=list)
    #: The error text (failed).
    error: str | None = None
    #: The exclusion (excluded): reason, scope, inherited_from.
    exclusion: dict | None = None

    @property
    def glyph(self) -> str:
        return GLYPHS[self.state]

    def to_dict(self) -> dict:
        return {
            "location": dict(self.location),
            "state": self.state,
            "glyph": self.glyph,
            "label": LABELS[self.state],
            "detail": self.detail,
            "record_ids": list(self.record_ids),
            "outdated_by": self.outdated_by,
            "missing_input": self.missing_input,
            "findings": list(self.findings),
            "error": self.error,
            "exclusion": self.exclusion,
        }


@dataclass
class VariableStatus:
    """Every planned location of one output variable of one step."""

    variable: str
    function_name: str | None
    keys: list[str]
    locations: list[LocationStatus] = field(default_factory=list)
    #: False when the step's level could not be resolved: no planned set.
    resolved: bool = True
    notes: list[str] = field(default_factory=list)
    #: Locations whose records were saved outside ``for_each`` (no findings
    #: were checked there): they say "not checked", never ✓-with-no-findings.
    unchecked: int = 0

    @property
    def counts(self) -> dict[str, int]:
        out = {s: 0 for s in STATES}
        for loc in self.locations:
            out[loc.state] += 1
        return out

    def to_dict(self, limit: int | None = None) -> dict:
        locs = self.locations if limit is None else self.locations[:limit]
        return {
            "variable": self.variable,
            "function_name": self.function_name,
            "keys": list(self.keys),
            "resolved": self.resolved,
            "counts": self.counts,
            "planned": len(self.locations),
            "unchecked": self.unchecked,
            "notes": list(self.notes),
            "locations": [loc.to_dict() for loc in locs],
            "truncated": len(locs) < len(self.locations),
        }


@dataclass
class Reason:
    kind: str
    count: int | None
    #: Up to a few locations, for "where?" (the data view filters on them).
    locations: list[dict] = field(default_factory=list)

    @property
    def glyph(self) -> str:
        return REASON_GLYPHS[self.kind]

    def text(self) -> str:
        """``✕ 3 failed``, ``○ not run`` (no count when unknown)."""
        words = {
            CANCELLED: "cancelled", STOPPED: "stopped at the first error",
            FAILED: "failed", MISSING_INPUT: "missing input", NOT_RUN: "not run",
            CODE_CHANGED: "code changed", INPUTS_CHANGED: "inputs changed",
            WARNING: "warning" if self.count == 1 else "warnings",
            EXCLUDED: "excluded", NEW: "new",
        }[self.kind]
        if self.kind in (CANCELLED, STOPPED, CODE_CHANGED, INPUTS_CHANGED) or self.count is None:
            if self.kind in (CODE_CHANGED, INPUTS_CHANGED) and self.count:
                return f"{self.glyph} {words} ({self.count})"
            return f"{self.glyph} {words}"
        return f"{self.glyph} {self.count} {words}"

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "count": self.count,
            "glyph": self.glyph,
            "text": self.text(),
            "locations": [dict(loc) for loc in self.locations],
        }


@dataclass
class StepVerdict:
    """The one word for one step, why, and the per-output states behind it."""

    function_name: str
    word: str
    reasons: list[Reason] = field(default_factory=list)
    #: Locations with new input data waiting (never part of the word).
    new: int = 0
    warnings: int = 0
    excluded: int = 0
    #: Counted locations (planned minus excluded minus new).
    counted: int = 0
    present: int = 0
    #: Does this step need running? (red on the canvas)
    needs_running: bool = True
    outputs: list[VariableStatus] = field(default_factory=list)
    level: list[str] | None = None
    level_rule: str = ""
    notes: list[str] = field(default_factory=list)
    run_id: str | None = None

    @property
    def glyph(self) -> str:
        return VERDICT_GLYPHS[self.word]

    @property
    def label(self) -> str:
        return VERDICT_LABELS[self.word]

    def chip(self) -> str:
        """The node's ONE reason chip: the top reason, else warnings, else
        exclusions (informational), else ``""``. ＋ new is a second chip
        (:meth:`new_chip`), never folded in here."""
        if self.reasons:
            return self.reasons[0].text()
        if self.warnings:
            return Reason(WARNING, self.warnings).text()
        if self.excluded:
            return Reason(EXCLUDED, self.excluded).text()
        return ""

    def new_chip(self) -> str:
        return f"＋{self.new} new" if self.new else ""

    def status_line(self) -> str:
        """``✅ Succeeded · 40 of 40 present · ＋4 new to process``: THE line."""
        parts = [f"{self.glyph} {self.label}"]
        if self.counted:
            parts.append(f"{self.present} of {self.counted} present")
        parts += [r.text() for r in self.reasons]
        if self.word != NOT_COMPLETE and self.warnings:
            parts.append(Reason(WARNING, self.warnings).text())
        if self.excluded:
            parts.append(Reason(EXCLUDED, self.excluded).text())
        if self.new:
            parts.append(f"＋{self.new} new to process")
        return " · ".join(parts)

    def to_dict(self, location_limit: int | None = 2000) -> dict:
        return {
            "function_name": self.function_name,
            "word": self.word,
            "glyph": self.glyph,
            "label": self.label,
            "status_line": self.status_line(),
            "chip": self.chip(),
            "new_chip": self.new_chip(),
            "reasons": [r.to_dict() for r in self.reasons],
            "new": self.new,
            "warnings": self.warnings,
            "excluded": self.excluded,
            "counted": self.counted,
            "present": self.present,
            "needs_running": self.needs_running,
            "level": None if self.level is None else list(self.level),
            "level_rule": self.level_rule,
            "notes": list(self.notes),
            "run_id": self.run_id,
            "outputs": [o.to_dict(limit=location_limit) for o in self.outputs],
        }


# ---------------------------------------------------------------------------
# Planned locations
# ---------------------------------------------------------------------------


def planned_locations(
    db: "DatabaseManager", keys: list[str], *, max_planned: int = MAX_PLANNED
) -> list[dict[str, str]]:
    """The planned locations at *keys* (schema order), live from ``_schema``,
    the between keys among *keys* taken as strata. ``[{}]`` for no keys (one
    call over the whole dataset). Raises ``ValueError`` above *max_planned*.
    """
    from scidb.provenance.inventory import strata_of
    from scidb.store.between_keys import between_keys

    order = list(db.dataset_schema_keys)
    keys = [k for k in order if k in set(keys)]
    if not keys:
        return [{}]
    between = [k for k in between_keys(db) if k in keys]
    cross = [k for k in keys if k not in between]
    levels = {k: [str(v) for v in db.distinct_schema_values(k)] for k in keys}
    if between and cross:
        strata = [(s.values, s.levels) for s in strata_of(db, between, cross, levels)]
    elif between:
        # Every key is between: the existing combinations themselves.
        strata = [
            (dict(zip(between, (str(v) for v in combo))), {})
            for combo in db.distinct_schema_combinations(between)
        ]
    else:
        strata = [({}, levels)]
    size = sum(math.prod(len(lv.get(k, [])) for k in cross) for _v, lv in strata)
    if size > max_planned:
        raise ValueError(
            f"{size:,} planned locations at {keys} (above {max_planned:,}); "
            f"declare a between key or iterate fewer keys"
        )
    out: list[dict[str, str]] = []
    for values, lv in strata:
        for combo in itertools.product(*(lv.get(k, []) for k in cross)):
            loc = {**values, **dict(zip(cross, combo))}
            out.append({k: loc[k] for k in keys})
    Log.debug(
        f"[status] planned_locations({keys}): between={between} strata={len(strata)} "
        f"-> {len(out)} location(s)"
    )
    return out


def _loc_key(loc: dict, keys: list[str]) -> tuple:
    return tuple(loc.get(k) for k in keys)


# ---------------------------------------------------------------------------
# The step
# ---------------------------------------------------------------------------


@dataclass
class _Step:
    fn_name: str
    current_hash: str | None
    trusts_hash: bool
    outputs: list[str]
    inputs: list[str]
    invocation_ids: set | None
    level: list[str] | None
    level_rule: str
    last_run_utc: Any
    ever_ran: bool
    notes: list[str] = field(default_factory=list)


def _fn_identity(fn, fn_registry) -> tuple[str, Any]:
    from scifor.project.names import function_name

    if isinstance(fn, str):
        return fn, (fn_registry or {}).get(fn)
    name = function_name(fn) if getattr(fn, "__name__", None) else type(fn).__name__
    return name, fn


def _resolve_step(db, fn, outputs, call_id, fn_registry, inputs, level) -> _Step:
    from scidb.provenance import query as pq
    from scidb.provenance.run_summary import run_summaries
    from scidb.run.foreach_config import function_hash_for
    from scidb.run.schema_level import RULE_NOTHING, input_levels, resolve_schema_level

    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    fn_name, fn_obj = _fn_identity(fn, fn_registry)
    notes: list[str] = []
    current_hash = None
    if fn_obj is not None:
        try:
            current_hash = function_hash_for(fn_obj)
        except Exception as e:  # an unhashable object: cannot say "code changed"
            Log.debug(f"[status] {fn_name}: no current hash ({type(e).__name__}: {e})")
    trusts_hash = fn_obj is not None and not hasattr(fn_obj, "hash")

    configs = pq.function_variant_configs(duck, fn_name)
    if call_id:
        configs = [c for c in configs if pq.config_call_id(fn_name, c) == call_id]
    inv_ids: set | None = None
    if call_id:
        inv_ids = set()
        for c in configs:
            inv_ids |= set(c.get("invocation_ids") or ())

    input_types: list[str] = []
    for c in configs:
        for t in (c.get("input_types") or {}).values():
            if isinstance(t, str) and t not in input_types:
                input_types.append(t)
    path_inputs = []
    if inputs:
        from scidb.model.input_spec import type_name
        from scifor import PathInput

        for spec in inputs.values():
            t = type_name(spec)
            if t and t not in input_types:
                input_types.append(t)
            if isinstance(spec, PathInput):
                path_inputs.append(spec)

    if outputs:
        out_names = [getattr(o, "__name__", o) for o in outputs]
    else:
        out_names = _recorded_outputs(duck, fn_name, inv_ids)

    recorded = _recorded_level(duck, fn_name, inv_ids)
    lv = input_levels(duck, input_types, path_inputs, schema_keys)
    resolved, rule = resolve_schema_level(schema_keys, stated=level, recorded=recorded, levels=lv)
    if rule == RULE_NOTHING:
        step_level = None
        notes.append(
            f"{fn_name}: no level to read (never run, no bound input): no planned locations yet"
        )
    else:
        step_level = list(resolved.keys)

    summaries = run_summaries(duck, fn_name, outputs=out_names or None, limit=1)
    last_run = summaries[0].started_at if summaries else None
    ever_ran = bool(summaries) or bool(recorded is not None)
    Log.debug(
        f"[status] {fn_name} call_id={call_id or '-'}: outputs={out_names} inputs={input_types} "
        f"level={step_level} ({rule}) last_run={last_run} hash="
        f"{(current_hash or '-')[:12]}"
    )
    return _Step(
        fn_name=fn_name,
        current_hash=current_hash,
        trusts_hash=trusts_hash,
        outputs=out_names,
        inputs=input_types,
        invocation_ids=inv_ids,
        level=step_level,
        level_rule=rule,
        last_run_utc=last_run,
        ever_ran=ever_ran,
        notes=notes,
    )


def _recorded_outputs(duck, fn_name: str, inv_ids: set | None) -> list[str]:
    rows = duck.fetchall(
        "SELECT DISTINCT io.invocation_id, r.type FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "WHERE inv.function_name = ?",
        [fn_name],
    )
    out: list[str] = []
    for inv, t in rows:
        if inv_ids is not None and inv not in inv_ids:
            continue
        if str(t).startswith("__"):  # glue / constant / PathInput entities
            continue
        if t not in out:
            out.append(t)
    return sorted(out)


def _recorded_level(duck, fn_name: str, inv_ids: set | None) -> list[str] | None:
    """The keys this step iterated on its newest saved invocation (``None``:
    never ran under this wiring)."""
    rows = duck.fetchall(
        "SELECT inv.invocation_id, inv.iteration_level, MAX(rs.timestamp) AS ts "
        "FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record_save rs ON rs.record_id = io.output_record_id "
        "WHERE inv.function_name = ? AND inv.iteration_level IS NOT NULL "
        "GROUP BY inv.invocation_id, inv.iteration_level ORDER BY ts DESC",
        [fn_name],
    )
    for inv, level, _ts in rows:
        if inv_ids is None or inv in inv_ids:
            return list(level)
    return None


def _utc(ts: Any) -> datetime | None:
    """A ``_record_save`` timestamp (local-clock ISO text) as naive UTC, the
    clock ``_run_summary.started_at`` and ``_run_outcome.recorded_at`` use."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        try:
            dt = datetime.fromisoformat(str(ts))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.astimezone()  # naive = local
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _as_naive_utc(ts: Any) -> datetime | None:
    """A value already stored in naive UTC (``started_at`` / ``recorded_at``)."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts
    try:
        return datetime.fromisoformat(str(ts))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Per-variable facts
# ---------------------------------------------------------------------------


def _records_by_location(db, variable: str) -> dict[str, dict]:
    """``{record_id: location}`` of *variable*'s current records."""
    from scidb.provenance import query as pq
    from scidb.provenance.locations import _combos_for_schema_ids

    by_sid = pq.current_records_by_schema_batch(db._duck, variable)
    if not by_sid:
        return {}
    combos = _combos_for_schema_ids(db, by_sid.keys())
    out = {}
    for sid, rids in by_sid.items():
        combo = dict(combos.get(sid, ()))
        for rid in rids:
            out[rid] = combo
    return out


def _project(loc: dict, keys: list[str]) -> tuple | None:
    """A record's location at *keys*, or None when it lacks one of them."""
    if any(loc.get(k) is None for k in keys):
        return None
    return tuple(loc[k] for k in keys)


class _InputPresence:
    """Where an input has data, at the step's keys: a record matches a
    planned location when every key both name agrees (a coarser record
    broadcasts; one with no shared key matches everywhere)."""

    def __init__(self, records: dict[str, dict], keys: list[str], saved: dict):
        self.by_keyset: dict[tuple, dict[tuple, Any]] = {}
        for rid, loc in records.items():
            ks = tuple(k for k in keys if loc.get(k) is not None)
            proj = tuple(loc[k] for k in ks)
            at = _utc(saved.get(rid))
            slot = self.by_keyset.setdefault(ks, {})
            prev = slot.get(proj)
            if prev is None or (at is not None and (prev[1] is None or at > prev[1])):
                slot[proj] = (rid, at)
        self.empty = not records

    def newest_save(self, loc: dict) -> tuple[bool, datetime | None]:
        """(has data here, newest save of the matching records)."""
        found, newest = False, None
        for ks, slot in self.by_keyset.items():
            hit = slot.get(tuple(loc.get(k) for k in ks))
            if hit is not None:
                found = True
                if hit[1] is not None and (newest is None or hit[1] > newest):
                    newest = hit[1]
        return found, newest


def _failures(duck, schema_keys, variable: str, fn_name: str) -> list:
    from scidb.provenance.outcomes import FAILED as OUTCOME_FAILED
    from scidb.provenance.outcomes import latest_outcomes

    return [
        o for o in latest_outcomes(duck, schema_keys, variable)
        if o.function_name == fn_name and o.outcome == OUTCOME_FAILED
    ]


def variable_status(
    db: "DatabaseManager",
    step: _Step,
    variable: str,
    *,
    variant: dict | None = None,
    location_filter: Any = None,
    timings: dict | None = None,
) -> VariableStatus:
    """Every planned location of *variable* as *step* writes it."""
    from scidb.provenance import findings as fnd
    from scidb.provenance import query as pq
    from scidb.provenance.outcomes import outcome_for
    from scidb.store.exclusions import governing_exclusions

    timings = timings if timings is not None else {}
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    vs = VariableStatus(variable=variable, function_name=step.fn_name, keys=list(step.level or []))
    if step.level is None:
        vs.resolved = False
        vs.notes.extend(step.notes)
        return vs
    keys = list(step.level)

    def _tick(phase: str, t0: float) -> None:
        timings[phase] = timings.get(phase, 0.0) + time.perf_counter() - t0

    t0 = time.perf_counter()
    try:
        planned = planned_locations(db, keys)
    except ValueError as e:
        vs.resolved = False
        vs.notes.append(str(e))
        Log.warn(f"[status] {step.fn_name} -> {variable}: {e}")
        return vs
    if location_filter is not None:
        planned = [p for p in planned if location_filter.matches(p)]
    _tick("planned", t0)

    t0 = time.perf_counter()
    exclusions = governing_exclusions(
        planned, schema_keys, db, variables=[variable, *step.inputs]
    )
    _tick("exclusions", t0)

    # The step's own current records of this variable, projected onto keys.
    t0 = time.perf_counter()
    records = _records_by_location(db, variable)
    produced = pq.producing_invocation_batch(duck, list(records))
    mine = {
        rid: loc for rid, loc in records.items()
        if rid in produced and produced[rid][1] == step.fn_name
        and (step.invocation_ids is None or produced[rid][0] in step.invocation_ids)
    }
    if variant:
        import pandas as pd

        from scidb.store.database import _filter_records_by_branch_params

        kept = _filter_records_by_branch_params(pd.DataFrame({"record_id": list(mine)}), variant, duck)
        keep = set(kept["record_id"].tolist())
        mine = {r: loc for r, loc in mine.items() if r in keep}
    at_loc: dict[tuple, list[str]] = {}
    for rid, loc in mine.items():
        proj = _project(loc, keys)
        if proj is not None:
            at_loc.setdefault(proj, []).append(rid)
    _tick("present", t0)

    t0 = time.perf_counter()
    rid_list = list(mine)
    superseded = pq.superseded_batch(duck, rid_list) if rid_list else {}
    _tick("superseded", t0)
    t0 = time.perf_counter()
    saved = pq.saved_at_batch(duck, rid_list) if rid_list else {}
    finding_rows = fnd.findings_for_records(db, rid_list) if rid_list else {}
    failures = _failures(duck, schema_keys, variable, step.fn_name)
    _tick("facts", t0)

    t0 = time.perf_counter()
    presences: dict[str, _InputPresence] = {}
    for x in step.inputs:
        x_records = _records_by_location(db, x)
        x_saved = pq.saved_at_batch(duck, list(x_records)) if x_records else {}
        presences[x] = _InputPresence(x_records, keys, x_saved)
    _tick("inputs", t0)

    t0 = time.perf_counter()
    last_run = _as_naive_utc(step.last_run_utc)
    for loc, excl in zip(planned, exclusions):
        key = _loc_key(loc, keys)
        rids = at_loc.get(key, [])
        if excl is not None:
            vs.locations.append(LocationStatus(
                location=loc, state=EXCLUDED, record_ids=rids,
                detail=excl.describe(variable), exclusion=excl.to_dict(variable),
            ))
            continue
        failure = outcome_for(failures, loc)
        if rids:
            newest_save = max((_utc(saved.get(r)) for r in rids if saved.get(r)), default=None)
            failed_since = (
                failure is not None and newest_save is not None
                and _as_naive_utc(failure.recorded_at) is not None
                and _as_naive_utc(failure.recorded_at) > newest_save
            )
            code_changed = step.current_hash is not None and any(
                produced[r][2] and produced[r][2] != step.current_hash for r in rids
            )
            inputs_changed = any(superseded.get(r) for r in rids)
            found = [f for r in rids for f in finding_rows.get(r, [])]
            if failed_since:
                vs.locations.append(LocationStatus(
                    location=loc, state=FAILED, record_ids=rids, error=failure.reason,
                    detail=failure.describe() + " (an older record is still here)",
                ))
            elif code_changed or inputs_changed:
                by = CODE_CHANGED if code_changed else INPUTS_CHANGED
                vs.locations.append(LocationStatus(
                    location=loc, state=OUTDATED, record_ids=rids, outdated_by=by,
                    detail=(
                        f"{step.fn_name}'s code changed since this was made"
                        if code_changed else "an input was re-saved since this was made"
                    ),
                ))
            elif found:
                vs.locations.append(LocationStatus(
                    location=loc, state=WARNING, record_ids=rids, findings=found,
                    detail="; ".join(f["text"] for f in found),
                ))
            else:
                vs.locations.append(LocationStatus(location=loc, state=PRESENT, record_ids=rids))
            continue
        if failure is not None:
            vs.locations.append(LocationStatus(
                location=loc, state=FAILED, error=failure.reason, detail=failure.describe(),
            ))
            continue
        missing = None
        newest_input = None
        for x, pres in presences.items():
            has, newest = pres.newest_save(loc)
            if not has:
                missing = x
                break
            if newest is not None and (newest_input is None or newest > newest_input):
                newest_input = newest
        if missing is not None:
            vs.locations.append(LocationStatus(
                location=loc, state=MISSING_INPUT, missing_input=missing,
                detail=f"not computed: {missing} has no data here",
            ))
            continue
        is_new = (
            step.ever_ran and last_run is not None and newest_input is not None
            and newest_input > last_run
        )
        if is_new:
            vs.locations.append(LocationStatus(
                location=loc, state=NEW,
                detail="input data added since the last run; the next run processes it",
            ))
        else:
            vs.locations.append(LocationStatus(
                location=loc, state=NOT_RUN,
                detail=(
                    f"{step.fn_name} has never run here" if presences
                    else f"no {variable} here"
                ),
            ))
    _tick("states", t0)
    return vs


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


def _merge_locations(outputs: list[VariableStatus]) -> dict[tuple, LocationStatus]:
    """One state per location over every output: the worst wins."""
    merged: dict[tuple, LocationStatus] = {}
    for vs in outputs:
        for loc in vs.locations:
            key = tuple(sorted(loc.location.items()))
            prev = merged.get(key)
            if prev is None or _SEVERITY[loc.state] > _SEVERITY[prev.state]:
                merged[key] = loc
    return merged


def verdict_of(
    fn_name: str,
    outputs: list[VariableStatus],
    *,
    cancelled: bool = False,
    stopped: bool = False,
    ever_ran: bool = True,
) -> StepVerdict:
    """THE verdict rule over some outputs' location states."""
    v = StepVerdict(function_name=fn_name, word=NOT_COMPLETE, outputs=outputs)
    if not outputs or not all(o.resolved for o in outputs):
        v.reasons = [Reason(NOT_RUN, None)]
        v.needs_running = True
        for o in outputs:
            v.notes.extend(o.notes)
        return v
    merged = _merge_locations(outputs)
    counts = {s: 0 for s in STATES}
    where: dict[str, list[dict]] = {}
    by: dict[str, int] = {CODE_CHANGED: 0, INPUTS_CHANGED: 0}
    for loc in merged.values():
        counts[loc.state] += 1
        if loc.state == OUTDATED and loc.outdated_by:
            by[loc.outdated_by] += 1
            where.setdefault(loc.outdated_by, []).append(loc.location)
        else:
            where.setdefault(loc.state, []).append(loc.location)
    v.new = counts[NEW]
    v.warnings = counts[WARNING]
    v.excluded = counts[EXCLUDED]
    v.present = counts[PRESENT] + counts[WARNING]
    v.counted = len(merged) - counts[EXCLUDED] - counts[NEW]
    raw = {
        CANCELLED: 1 if cancelled else 0,
        STOPPED: 1 if stopped and not cancelled else 0,
        FAILED: counts[FAILED],
        MISSING_INPUT: counts[MISSING_INPUT],
        NOT_RUN: counts[NOT_RUN],
        CODE_CHANGED: by[CODE_CHANGED],
        INPUTS_CHANGED: by[INPUTS_CHANGED],
    }
    v.reasons = [
        Reason(kind, raw[kind], where.get(kind, [])[:20]) for kind in REASON_ORDER if raw[kind]
    ]
    if v.counted == 0 and not v.reasons:
        # Nothing to count: never "0 of 0 succeeded".
        v.reasons = [Reason(NOT_RUN, v.new or None, where.get(NEW, [])[:20])]
    if not v.reasons:
        v.word = SUCCEEDED_WITH_WARNINGS if v.warnings else SUCCEEDED
    v.needs_running = bool(
        counts[FAILED] or counts[NOT_RUN] or counts[NEW] or counts[OUTDATED]
        or not ever_ran
    )
    return v


def step_status(
    db: "DatabaseManager | None",
    fn,
    outputs=None,
    *,
    call_id: str | None = None,
    fn_registry: dict | None = None,
    inputs: dict | None = None,
    level: Any = None,
    locations: Any = None,
    variant: dict | None = None,
) -> StepVerdict:
    """The verdict of one step (a function, optionally one call site), with
    every output's location states.

    Args:
        fn: the function (callable: its current source decides "code
            changed") or its name (looked up in *fn_registry*; without it,
            a code edit cannot be seen and only re-saved inputs read ↻).
        outputs: output variable classes or names; default the recorded ones.
        call_id: restrict to one call site (``check_node_state``'s scope).
        inputs: the step's inputs as ``for_each`` takes them: the intent of
            a never-run node (its input types and PathInputs give the level).
        level: a stated iteration level (``SchemaLevel.from_stated`` shape).
        locations: a ``locations=`` selection; the verdict is scoped to it.
        variant: a ``branch_params_filter`` to judge one variant's records.
    """
    if db is None:
        from scidb.store.database import get_database

        db = get_database()
    t_all = time.perf_counter()
    timings: dict[str, float] = {}
    t0 = time.perf_counter()
    step = _resolve_step(db, fn, outputs, call_id, fn_registry, inputs, level)
    timings["resolve"] = time.perf_counter() - t0
    flt = None
    if locations is not None:
        from scifor.locations import LocationFilter

        flt = LocationFilter.of(locations)
        if flt.is_empty():
            flt = None
    outs = [
        variable_status(db, step, name, variant=variant, location_filter=flt, timings=timings)
        for name in step.outputs
    ]
    if not outs:
        outs = [VariableStatus(variable="", function_name=step.fn_name, keys=[], resolved=False,
                               notes=[f"{step.fn_name}: no output variable to judge"])]
    v = verdict_of(step.fn_name, outs, ever_ran=step.ever_ran)
    v.level = step.level
    v.level_rule = step.level_rule
    v.notes.extend(n for n in step.notes if n not in v.notes)
    Log.info(
        f"[status] {step.fn_name} call_id={call_id or '-'}: {v.status_line()} "
        f"(needs_running={v.needs_running})"
    )
    Log.timings(
        f"step_status({step.fn_name})",
        timings,
        extra=f"{sum(len(o.locations) for o in outs)} location(s), {len(step.inputs)} input(s)",
        total=time.perf_counter() - t_all,
    )
    return v


def run_verdict(db: "DatabaseManager", run_id: str, fn_registry: dict | None = None) -> StepVerdict:
    """The verdict of one stored run: its step's states over the run's
    ``locations=``, plus ⏸ cancelled / stopped."""
    from scidb.provenance.run_summary import run_summary

    summary = run_summary(db._duck, run_id)
    if summary is None:
        raise ValueError(f"No stored run {run_id!r}")
    base = step_status(
        db,
        summary.function_name,
        outputs=list(summary.output_types) or None,
        fn_registry=fn_registry,
        locations=(summary.settings or {}).get("locations"),
    )
    v = verdict_of(
        summary.function_name, base.outputs,
        cancelled=summary.cancelled, stopped=summary.stopped,
    )
    v.level, v.level_rule, v.notes, v.run_id = base.level, base.level_rule, base.notes, run_id
    return v


def location_status(
    db: "DatabaseManager",
    variable,
    *,
    variant: dict | None = None,
    fn_registry: dict | None = None,
) -> VariableStatus:
    """One variable's planned locations and states, as its producing step
    writes it (the data view's column). A variable no step produces (raw,
    manual) is judged at the keys its records carry: present or not, never
    "not run"-with-a-function."""
    name = getattr(variable, "__name__", variable)
    producers = _producers(db._duck, name)
    if producers:
        fn_name = producers[0]
        step = _resolve_step(db, fn_name, [name], None, fn_registry, None, None)
        vs = variable_status(db, step, name, variant=variant)
        if len(producers) > 1:
            vs.notes.append(
                f"{name} is written by {len(producers)} functions ({', '.join(producers)}); "
                f"judged as {fn_name} writes it (the newest)"
            )
        return vs
    return _raw_status(db, name)


def _producers(duck, variable: str) -> list[str]:
    """Functions that wrote *variable*, newest save first."""
    rows = duck.fetchall(
        "SELECT inv.function_name, MAX(rs.timestamp) AS ts FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "JOIN _record_save rs ON rs.record_id = r.record_id "
        "WHERE r.type = ? AND inv.function_hash <> '' AND inv.function_name <> '__save__' "
        "GROUP BY inv.function_name ORDER BY ts DESC",
        [variable],
    )
    return [r[0] for r in rows]


def _raw_status(db, name: str) -> VariableStatus:
    from scidb.store.exclusions import governing_exclusions

    schema_keys = list(db.dataset_schema_keys)
    records = _records_by_location(db, name)
    keys = [k for k in schema_keys if any(loc.get(k) is not None for loc in records.values())]
    vs = VariableStatus(variable=name, function_name=None, keys=keys)
    vs.notes.append(f"{name} is not made by a step (raw or saved by hand): nothing was checked")
    if not records:
        vs.resolved = False
        return vs
    planned = planned_locations(db, keys)
    at_loc: dict[tuple, list[str]] = {}
    for rid, loc in records.items():
        proj = _project(loc, keys)
        if proj is not None:
            at_loc.setdefault(proj, []).append(rid)
    for loc, excl in zip(planned, governing_exclusions(planned, schema_keys, db, variables=[name])):
        rids = at_loc.get(_loc_key(loc, keys), [])
        if excl is not None:
            vs.locations.append(LocationStatus(loc, EXCLUDED, excl.describe(name), rids,
                                               exclusion=excl.to_dict(name)))
        elif rids:
            vs.locations.append(LocationStatus(loc, PRESENT, "not checked (saved outside a run)", rids))
            vs.unchecked += 1
        else:
            vs.locations.append(LocationStatus(loc, NOT_RUN, f"no {name} here"))
    return vs
