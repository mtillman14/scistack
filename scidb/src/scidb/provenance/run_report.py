"""The run report: everything about one run of a function, for verifying it.

``.claude/plan-run-report-popup.md`` (GitHub issue #17). After a run, the
researcher needs to see what it did: each input and output described at every
schema-key combination, the settings used, the exclusions in force, NaNs and
other missing data. This module is the ONE owner of that report. The GUI
popup, ``Inspector`` and ``scidb run-report`` read it, and none of them
recomputes a status, a count or a sentence.

It is read **from the database afterwards** (user decision 2026-11-15), so it
describes what a run left behind rather than what it saw in passing:

* the run's invocations, their input and output records and the
  identity-bearing options come from the provenance graph (``_run`` →
  ``_run_invocation`` → ``_invocation_input`` / ``_invocation_output``);
* counts, timing and the options that are not identity come from
  ``_run_summary`` (``scidb.provenance.run_summary``, written by the run);
* failed and no-data locations come from ``_run_outcome`` rows naming the run;
* exclusions in force are read from the append-only override history as of
  the run's start;
* what is inside the cells is ``scidb.inspect.cells``' answer.

Two calls, by cost:

* :func:`run_report` — queries only. Header, settings, exclusions, the
  input and output variables with record counts, and the attention items
  that need no data.
* :func:`run_report_data` — loads the run's records. Per variable: columns,
  shapes, sizes, cell findings; per combination: every input's and output's
  shape and findings; and the attention items only the data can raise.

Limits, stated in the report's ``notes`` where they apply:

* A combination that was already up to date is counted, not listed: the run
  did not touch it, and its outputs belong to the earlier run that computed
  them.
* A run that saved nothing has no ``_run`` row, so no inputs or outputs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import timezone
from typing import TYPE_CHECKING, Any

from scidb.model.exceptions import NotFoundError
from scidb.model.log import Log
from scidb.provenance import query as provenance_query
from scidb.provenance.run_summary import RunSummary, run_summaries, run_summary

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

#: Attention severities. ``problem`` = look before trusting the result;
#: ``note`` = worth knowing.
PROBLEM = "problem"
NOTE = "note"

#: Row statuses of the per-combination table, and their one wording.
COMPUTED = "computed"
FAILED = "failed"
NO_DATA = "no_data"
STATUS_LABELS = {
    COMPUTED: "Computed",
    FAILED: "Failed",
    NO_DATA: "No data",
}

#: Rows of the per-combination table returned by default.
DEFAULT_ROW_LIMIT = 2000
#: Input records above which variants are not resolved (an upstream walk).
_VARIANT_RECORD_CAP = 20_000
#: Distinct variants / shapes listed per variable.
_LIST_MAX = 6


@dataclass
class Attention:
    """One thing to look at. ``text`` is THE wording."""

    severity: str
    kind: str
    text: str
    variable: str | None = None

    def to_dict(self) -> dict:
        return {
            "severity": self.severity,
            "kind": self.kind,
            "text": self.text,
            "variable": self.variable,
        }


@dataclass
class RunReport:
    summary: RunSummary
    #: The ``_run`` row: ``timestamp``, ``origin``, ``step``, ``where``; or
    #: ``None`` values when the run saved nothing.
    run: dict
    #: ``function_versions`` ([{hash, invocations, source}]), ``run_options``
    #: ([label]), ``constants`` ({param: [value text]}), ``path_inputs``,
    #: ``selectors``, plus the summary's non-identity settings.
    settings: dict
    #: [{param, variable, records, locations, through_glue, variants}]
    inputs: list[dict]
    #: [{variable, records, locations, new_records, resaved_records}]
    outputs: list[dict]
    #: Exclusions in force when the run started ([Override.to_dict()]).
    exclusions: list[dict]
    #: Expected / Not expected marks on the outputs ([Mark.to_dict()]).
    marks: list[dict]
    attention: list[Attention] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "summary": self.summary.to_dict(),
            "run": dict(self.run),
            "settings": self.settings,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "exclusions": self.exclusions,
            "marks": self.marks,
            "attention": [a.to_dict() for a in self.attention],
            "notes": list(self.notes),
        }


@dataclass
class RunReportData:
    run_id: str
    schema_keys: list[str]
    #: {variable: {role, records, nbytes, columns: {col: {dtypes, shapes,
    #: cells}}}}; ``shapes`` is [{shape, count}], most common first.
    variables: dict[str, dict]
    #: [{location, status, status_label, reason, inputs: {param: {...}},
    #: outputs: {variable: {...}}}]
    rows: list[dict]
    total_rows: int
    status_counts: dict[str, int]
    attention: list[Attention] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "schema_keys": list(self.schema_keys),
            "variables": self.variables,
            "rows": self.rows,
            "total_rows": self.total_rows,
            "truncated": len(self.rows) < self.total_rows,
            "status_counts": dict(self.status_counts),
            "status_labels": dict(STATUS_LABELS),
            "attention": [a.to_dict() for a in self.attention],
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# The run's graph (shared by both calls)
# ---------------------------------------------------------------------------


@dataclass
class _RunGraph:
    """The invocations of one run and what each read and wrote."""

    run_row: dict
    invocation_ids: list[str]
    #: provenance_query.invocation_edges_batch's result.
    edges: dict
    #: {invocation_id: [(output_type, record_id)]}: the record this run left
    #: at each output slot.
    outputs: dict[str, list[tuple[str, str]]]
    #: {record_id: {key: value}} for every input and output record.
    locations: dict[str, dict]
    #: {record_id: number of saves up to and including this run}.
    save_counts: dict[str, int]
    #: {invocation_id: (function_hash, run_options_label)}
    invocations: dict[str, tuple[str | None, str]]


def _run_graph(db: "DatabaseManager", summary: RunSummary) -> _RunGraph:
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    rows = duck.fetchall(
        "SELECT r.timestamp, r.where_clause, r.origin FROM _run r WHERE r.run_id = ?",
        [summary.run_id],
    )
    if not rows:
        return _RunGraph(
            run_row={"timestamp": None, "where": None, "origin": None, "step": None},
            invocation_ids=[], edges={}, outputs={}, locations={}, save_counts={},
            invocations={},
        )
    run_ts, where_clause, origin = rows[0]
    step = None
    if duck.table_exists("_run_step"):
        step_rows = duck.fetchall("SELECT step FROM _run_step WHERE run_id = ?", [summary.run_id])
        step = step_rows[0][0] if step_rows else None
    run_row = {"timestamp": run_ts, "where": where_clause, "origin": origin, "step": step}

    # The run's own invocations: a glue hop or a `__save__` activity recorded
    # in the same run is not a call of the function.
    inv_rows = duck.fetchall(
        "SELECT i.invocation_id, i.function_hash, i.distribute, i.as_table, "
        "i.for_columns, i.across_variants, i.iteration_level "
        "FROM _run_invocation ri JOIN _invocation i ON i.invocation_id = ri.invocation_id "
        "WHERE ri.run_id = ? AND i.function_name = ? ORDER BY i.invocation_id",
        [summary.run_id, summary.function_name],
    )
    invocations = {
        inv: (fn_hash, provenance_query.run_options_label(dist, at, fc, av, lvl))
        for inv, fn_hash, dist, at, fc, av, lvl in inv_rows
    }
    inv_ids = list(invocations)
    edges = provenance_query.invocation_edges_batch(duck, inv_ids)

    # An invocation's output slots append across runs, so "what this run
    # wrote" is, per slot location and type, the record with the newest save
    # at or before the run's own timestamp (both written from one clock).
    out_rows = provenance_query._chunked_in(
        duck,
        "SELECT io.invocation_id, rec.type, rec.schema_id, io.output_record_id, "
        "MAX(rs.timestamp), COUNT(rs.timestamp) "
        "FROM _invocation_output io "
        "JOIN _record rec ON rec.record_id = io.output_record_id "
        "JOIN _record_save rs ON rs.record_id = io.output_record_id "
        "WHERE io.invocation_id IN ({ph}) AND rs.timestamp <= ? "
        "GROUP BY io.invocation_id, rec.type, rec.schema_id, io.output_record_id",
        inv_ids,
        tail_params=[run_ts],
    ) if inv_ids else []
    newest: dict[tuple, tuple] = {}
    for inv, out_type, schema_id, rid, last_save, n_saves in out_rows:
        slot = (inv, out_type, schema_id)
        if slot not in newest or last_save > newest[slot][1]:
            newest[slot] = (rid, last_save, int(n_saves), schema_id)
    outputs: dict[str, list[tuple[str, str]]] = {}
    save_counts: dict[str, int] = {}
    schema_of: dict[str, Any] = {}
    for (inv, out_type, _sid), (rid, _ts, n_saves, schema_id) in sorted(
        newest.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2]))
    ):
        outputs.setdefault(inv, []).append((out_type, rid))
        save_counts[rid] = n_saves
        schema_of[rid] = schema_id

    input_rids = {
        _source_rid(v) for e in edges.values() for v in e["var_inputs"]
    }
    if input_rids:
        for rid, schema_id in provenance_query._chunked_in(
            duck,
            "SELECT record_id, schema_id FROM _record WHERE record_id IN ({ph})",
            sorted(input_rids),
        ):
            schema_of[rid] = schema_id
    by_schema = provenance_query._schema_locations(duck, schema_of.values(), schema_keys)
    locations = {
        rid: {k: str(v) for k, v in by_schema.get(sid, {}).items()}
        for rid, sid in schema_of.items()
    }
    return _RunGraph(
        run_row=run_row, invocation_ids=inv_ids, edges=edges, outputs=outputs,
        locations=locations, save_counts=save_counts, invocations=invocations,
    )


def _source_rid(var_input: dict) -> str:
    """The record that holds an input's data: a glued input's edge binds a
    virtual record, and the data is its source's."""
    return var_input.get("glue_source_record_id") or var_input["record_id"]


def _override_time_utc(at: Any) -> Any:
    """An exclusion's ``changed_at`` as naive UTC, the clock ``_run_summary``
    uses. ``exclusions._insert_row`` binds an AWARE datetime to a TIMESTAMP
    column, which DuckDB stores as the session's LOCAL wall time, so the
    value read back is naive local time (found 2026-11-15: an exclusion made
    after a run read as older than it, west of UTC). ``astimezone`` on a
    naive datetime assumes the system zone, DuckDB's default session zone."""
    if not hasattr(at, "astimezone"):
        return at
    return at.astimezone(timezone.utc).replace(tzinfo=None)


def _require_summary(db: "DatabaseManager", run_id: str) -> RunSummary:
    summary = run_summary(db._duck, run_id)
    if summary is None:
        raise NotFoundError(
            f"No run {run_id!r} is recorded in this database. Runs are recorded "
            f"from 2026-11-15 on; `scidb run-report <function>` lists them."
        )
    return summary


# ---------------------------------------------------------------------------
# run_report: queries only
# ---------------------------------------------------------------------------


def run_reports(
    db: "DatabaseManager",
    function_name: str | None = None,
    *,
    outputs: "list[str] | None" = None,
    limit: int = 50,
) -> list[RunSummary]:
    """The recorded runs of a function, newest first (the report picker)."""
    name = getattr(function_name, "__name__", function_name)
    return run_summaries(db._duck, name, outputs=outputs, limit=limit)


def run_report(db: "DatabaseManager", run_id: str) -> RunReport:
    """The report of one run, without loading any data."""
    t0 = time.perf_counter()
    summary = _require_summary(db, run_id)
    duck = db._duck
    graph = _run_graph(db, summary)
    t_graph = time.perf_counter()

    settings = _settings(duck, summary, graph)
    inputs = _input_cards(duck, graph)
    outputs = _output_cards(summary, graph)
    exclusions = _exclusions_in_force(db, summary)
    marks = _marks(db, summary)
    t_cards = time.perf_counter()

    notes: list[str] = []
    if graph.run_row["timestamp"] is None:
        notes.append(
            "This run saved nothing, so it has no inputs or outputs to describe."
        )
    if summary.up_to_date:
        n = summary.up_to_date
        notes.append(
            f"{n} combination{' was' if n == 1 else 's were'} already up to date "
            f"and not recomputed. Their outputs belong to the earlier runs that "
            f"computed them and are not listed here."
        )
    attention = _summary_attention(db, summary, graph, outputs)
    report = RunReport(
        summary=summary,
        run={**graph.run_row, "timestamp": _text(graph.run_row["timestamp"])},
        settings=settings,
        inputs=inputs,
        outputs=outputs,
        exclusions=exclusions,
        marks=marks,
        attention=attention,
        notes=notes,
    )
    Log.info(
        f"[run_report] run={run_id} fn={summary.function_name}: "
        f"{len(graph.invocation_ids)} invocation(s), {len(inputs)} input(s), "
        f"{len(outputs)} output(s), {len(exclusions)} exclusion(s), "
        f"{len(attention)} attention item(s); graph "
        f"{(t_graph - t0) * 1000:.1f} ms, cards {(t_cards - t_graph) * 1000:.1f} ms, "
        f"total {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return report


def _text(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _settings(duck, summary: RunSummary, graph: _RunGraph) -> dict:
    hashes: dict[str, int] = {}
    labels: dict[str, int] = {}
    for fn_hash, label in graph.invocations.values():
        if fn_hash:
            hashes[fn_hash] = hashes.get(fn_hash, 0) + 1
        labels[label] = labels.get(label, 0) + 1
    constants: dict[str, list[str]] = {}
    path_inputs: dict[str, list[str]] = {}
    selectors: dict[str, list[str]] = {}
    declared: dict[str, str] = {}
    for e in graph.edges.values():
        for param, value in (e.get("constants") or {}).items():
            _add_distinct(constants, param, repr(value))
        for param, spec in (e.get("path_inputs") or {}).items():
            _add_distinct(path_inputs, param, str(spec))
        for param, sel in (e.get("selectors") or {}).items():
            _add_distinct(selectors, param, str(sel))
        declared.update(e.get("declared_names") or {})
    return {
        "function_versions": [
            {
                "hash": h,
                "invocations": n,
                "source": provenance_query.function_source(duck, h),
            }
            for h, n in sorted(hashes.items(), key=lambda kv: -kv[1])
        ],
        "run_options": [label for label, _n in sorted(labels.items(), key=lambda kv: -kv[1])],
        "constants": constants,
        "declared_names": declared,
        "path_inputs": path_inputs,
        "selectors": selectors,
        "where": graph.run_row["where"] or summary.settings.get("where"),
        "locations": summary.settings.get("locations"),
        "on_error": summary.settings.get("on_error"),
        "skip_computed": summary.settings.get("skip_computed"),
        "save": summary.settings.get("save"),
    }


def _add_distinct(target: dict[str, list[str]], key: str, value: str) -> None:
    values = target.setdefault(key, [])
    if value not in values:
        values.append(value)


def _input_cards(duck, graph: _RunGraph) -> list[dict]:
    by_param: dict[str, dict] = {}
    for e in graph.edges.values():
        for v in e["var_inputs"]:
            card = by_param.setdefault(
                v["param_name"],
                {
                    "param": v["param_name"],
                    "variable": v["variable_type"],
                    "through_glue": bool(v.get("glue_chain")),
                    "_rids": set(),
                    "_edge_rids": set(),
                },
            )
            card["_rids"].add(_source_rid(v))
            card["_edge_rids"].add(v["record_id"])
    cards = []
    n_all = sum(len(c["_edge_rids"]) for c in by_param.values())
    for param in sorted(by_param):
        card = by_param[param]
        rids = card.pop("_rids")
        edge_rids = card.pop("_edge_rids")
        card["records"] = len(rids)
        card["locations"] = len({_loc_key(graph.locations.get(r, {})) for r in rids})
        card["variants"] = (
            _variants(duck, edge_rids) if n_all <= _VARIANT_RECORD_CAP else None
        )
        cards.append(card)
    return cards


def _variants(duck, record_ids) -> list[dict]:
    """The distinct variants (accumulated branch params) among some records,
    most common first, at most ``_LIST_MAX``."""
    params = provenance_query.branch_params_batch(duck, sorted(record_ids))
    counts: dict[tuple, int] = {}
    for rid in record_ids:
        key = tuple(sorted((str(k), repr(v)) for k, v in (params.get(rid) or {}).items()))
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [
        {"params": {k: v for k, v in key}, "records": n}
        for key, n in ordered[:_LIST_MAX]
    ] + (
        [{"more": len(ordered) - _LIST_MAX}] if len(ordered) > _LIST_MAX else []
    )


def _loc_key(location: dict) -> tuple:
    return tuple(sorted(location.items()))


def _output_cards(summary: RunSummary, graph: _RunGraph) -> list[dict]:
    by_type: dict[str, set] = {}
    for outs in graph.outputs.values():
        for out_type, rid in outs:
            by_type.setdefault(out_type, set()).add(rid)
    # Every output the run listed, written or not, in its `outputs=` order.
    names = list(summary.output_types) + sorted(set(by_type) - set(summary.output_types))
    cards = []
    for name in names:
        rids = by_type.get(name, set())
        new = sum(1 for r in rids if graph.save_counts.get(r, 1) <= 1)
        cards.append({
            "variable": name,
            "records": len(rids),
            "locations": len({_loc_key(graph.locations.get(r, {})) for r in rids}),
            # Saved for the first time by this run, vs. the same record (same
            # content, same lineage) saved again.
            "new_records": new,
            "resaved_records": len(rids) - new,
        })
    return cards


def _exclusions_in_force(db: "DatabaseManager", summary: RunSummary) -> list[dict]:
    """The exclusions that governed when the run started: per keyset the
    newest row at or before the start, kept when it excludes."""
    Log.debug(
        f"[run_report] exclusions as of {summary.started_at} UTC for run={summary.run_id}"
    )
    from scidb.store.exclusions import override_history

    start = summary.started_at
    current: dict[tuple, Any] = {}
    for o in override_history(db):
        at = _override_time_utc(o.changed_at)
        if at is not None and start is not None and at > start:
            continue
        current[_loc_key(o.keys)] = o  # oldest first, so the newest wins
    return [
        o.to_dict()
        for _k, o in sorted(current.items())
        if not o.included
    ]


def _marks(db: "DatabaseManager", summary: RunSummary) -> list[dict]:
    if not summary.output_types:
        return []
    from scidb.provenance.expectation import marks

    try:
        return [m.to_dict() for m in marks(db, list(summary.output_types))]
    except Exception as e:  # a report is never refused over its marks
        Log.debug(f"[run_report] marks not read for {summary.output_types}: {e}")
        return []


def _summary_attention(
    db: "DatabaseManager", summary: RunSummary, graph: _RunGraph, outputs: list[dict]
) -> list[Attention]:
    out: list[Attention] = []
    s = summary
    if s.cancelled:
        out.append(Attention(PROBLEM, "cancelled", "The run was cancelled before it finished."))
    if s.stopped:
        out.append(Attention(
            PROBLEM, "stopped",
            "The run stopped at the first error (on_error=stop); later combinations did not run.",
        ))
    for g in s.notes.get("failures") or []:
        n = g.get("count") or 0
        times = f"{n} combination{'' if n == 1 else 's'}"
        if g.get("no_data"):
            out.append(Attention(NOTE, "no_data", f"No input data at {times}: {g.get('reason')}"))
        else:
            label = g.get("blame_label") or "Failed"
            out.append(Attention(PROBLEM, "failed", f"{label} at {times}: {g.get('reason')}"))
    for name, skips in (s.notes.get("output_skips") or {}).items():
        for skip in skips:
            n = skip.get("count") or 0
            detail = skip.get("detail") or skip.get("reason") or "no output returned"
            out.append(Attention(
                NOTE, "output_skipped",
                f"{name} was not written at {n} combination{'' if n == 1 else 's'}: {detail}",
                variable=name,
            ))
    if s.settings.get("save") is False:
        out.append(Attention(NOTE, "draft", "This was a draft run (save=False): nothing was stored."))
    elif s.computed and not s.saved_records:
        out.append(Attention(
            PROBLEM, "nothing_saved",
            f"{s.computed} combination{'' if s.computed == 1 else 's'} computed, "
            f"but no record was saved.",
        ))
    if len({h for h, _l in graph.invocations.values() if h}) > 1:
        out.append(Attention(
            NOTE, "function_versions",
            "The run's records come from more than one version of the function.",
        ))
    out.extend(_expectation_attention(db, [c["variable"] for c in outputs]))
    return out


def _expectation_attention(db: "DatabaseManager", variables: list[str]) -> list[Attention]:
    """Locations each output is expected to have and lacks NOW
    (``expectation()``, the one owner of "should exist")."""
    from scidb.provenance import expectation as ex

    out = []
    for name in variables:
        t0 = time.perf_counter()
        try:
            n = ex.expectation(db, name).counts.get(ex.MISSING_UNEXPECTED, 0)
        except Exception as e:  # an unregistered output of a failed run
            Log.debug(f"[run_report] expectation({name}) not available: {e}")
            continue
        Log.debug(
            f"[run_report] expectation({name}): {n} missing-unexpected in "
            f"{(time.perf_counter() - t0) * 1000:.1f} ms"
        )
        if n:
            out.append(Attention(
                PROBLEM, "missing_locations",
                f"{name} still lacks {n} location{'' if n == 1 else 's'} it is "
                f"expected to have (see Expected locations).",
                variable=name,
            ))
    return out


# ---------------------------------------------------------------------------
# run_report_data: loads the run's records
# ---------------------------------------------------------------------------


def run_report_data(
    db: "DatabaseManager",
    run_id: str,
    *,
    status: str | None = None,
    limit: int = DEFAULT_ROW_LIMIT,
) -> RunReportData:
    """What the run's records hold: per variable and per combination.

    *status* keeps one row status (``computed`` / ``failed`` / ``no_data``);
    *limit* caps the rows returned (``total_rows`` is the uncapped count).
    """
    from scidb.inspect.cells import record_cells

    if status is not None and status not in STATUS_LABELS:
        raise ValueError(f"status must be one of {sorted(STATUS_LABELS)}, got {status!r}")
    t0 = time.perf_counter()
    summary = _require_summary(db, run_id)
    graph = _run_graph(db, summary)
    schema_keys = list(db.dataset_schema_keys)

    # Which records of which variable, and in which role.
    wanted: dict[str, set] = {}
    roles: dict[str, str] = {}
    for e in graph.edges.values():
        for v in e["var_inputs"]:
            wanted.setdefault(v["variable_type"], set()).add(_source_rid(v))
            roles.setdefault(v["variable_type"], "input")
    for outs in graph.outputs.values():
        for out_type, rid in outs:
            wanted.setdefault(out_type, set()).add(rid)
            roles[out_type] = "both" if roles.get(out_type) == "input" else "output"
    t_graph = time.perf_counter()

    cells: dict[str, Any] = {}
    notes: list[str] = []
    for variable in sorted(wanted):
        try:
            cells.update(record_cells(db, variable, wanted[variable]))
        except Exception as e:  # one unreadable variable must not hide the rest
            Log.warn(
                f"[run_report] run={run_id}: could not read {variable}: "
                f"{type(e).__name__}: {e}"
            )
            notes.append(f"{variable} could not be read: {type(e).__name__}: {e}")
    t_load = time.perf_counter()

    variables = {
        name: _variable_profile(roles[name], [cells[r] for r in sorted(rids) if r in cells], len(rids))
        for name, rids in sorted(wanted.items())
    }
    rows = _computed_rows(graph, cells, schema_keys)
    rows += _outcome_rows(db, summary, schema_keys)
    status_counts = {k: 0 for k in STATUS_LABELS}
    for row in rows:
        status_counts[row["status"]] += 1
    attention = _data_attention(variables, rows)
    if status is not None:
        rows = [r for r in rows if r["status"] == status]
    rows.sort(key=lambda r: [str(r["location"].get(k, "")) for k in schema_keys])
    total = len(rows)
    data = RunReportData(
        run_id=run_id,
        schema_keys=schema_keys,
        variables=variables,
        rows=rows[: max(int(limit), 0)],
        total_rows=total,
        status_counts=status_counts,
        attention=attention,
        notes=notes,
    )
    Log.info(
        f"[run_report] data run={run_id} fn={summary.function_name}: "
        f"{len(wanted)} variable(s), {sum(len(r) for r in wanted.values())} record(s), "
        f"{total} row(s) {status_counts}, {len(attention)} attention item(s); graph "
        f"{(t_graph - t0) * 1000:.1f} ms, load {(t_load - t_graph) * 1000:.1f} ms, "
        f"total {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return data


def _variable_profile(role: str, records: list, n_wanted: int) -> dict:
    """One variable's records in the run, column by column."""
    from scicanonicalhash._cells import CellStats

    columns: dict[str, dict] = {}
    for rec in records:
        for col, stats in rec.cells.items():
            entry = columns.setdefault(
                col, {"_stats": CellStats(), "_shapes": {}, "_dtypes": {}}
            )
            _merge_stats(entry["_stats"], stats)
            shape = rec.shapes.get(col)
            shape_text = shape.shape_text if shape is not None else f"{rec.n_rows} rows"
            entry["_shapes"][shape_text] = entry["_shapes"].get(shape_text, 0) + 1
            if shape is not None and shape.kind != "blank":
                entry["_dtypes"][shape.dtype] = entry["_dtypes"].get(shape.dtype, 0) + 1
    out_columns = {}
    for col, entry in columns.items():
        shapes = sorted(entry["_shapes"].items(), key=lambda kv: (-kv[1], kv[0]))
        out_columns[col] = {
            "dtypes": [d for d, _n in sorted(entry["_dtypes"].items(), key=lambda kv: -kv[1])],
            "shapes": [{"shape": s, "count": n} for s, n in shapes[:_LIST_MAX]],
            "distinct_shapes": len(shapes),
            "cells": entry["_stats"].to_dict(),
        }
    return {
        "role": role,
        "records": n_wanted,
        "records_with_data": len(records),
        "nbytes": sum(r.nbytes for r in records) or None,
        "columns": out_columns,
    }


def _merge_stats(into, other) -> None:
    """Add *other*'s counts into *into* (two ``CellStats``)."""
    into.cells += other.cells
    into.array_cells += other.array_cells
    into.elements += other.elements
    into.nan_elements += other.nan_elements
    for bound, pick in (("min_length", min), ("max_length", max)):
        a, b = getattr(into, bound), getattr(other, bound)
        setattr(into, bound, b if a is None else a if b is None else pick(a, b))
    for kind, n in other.kinds.items():
        into.kinds[kind] += n


def _side(records: list) -> dict:
    """What one argument or one output held at one combination: its one
    record's description, or a summary of several (a pooled input)."""
    if len(records) == 1:
        return {**records[0].to_dict(), "records": 1}
    shapes: dict[str, int] = {}
    for r in records:
        shapes[r.shape_text()] = shapes.get(r.shape_text(), 0) + 1
    ordered = sorted(shapes.items(), key=lambda kv: (-kv[1], kv[0]))
    shape_text = (
        ordered[0][0] if len(ordered) == 1
        else f"{len(ordered)} shapes ({', '.join(s for s, _n in ordered[:3])}"
        + ("…)" if len(ordered) > 3 else ")")
    )
    nan = sum(r.nan_elements for r in records)
    problems = sum(r.problems for r in records)
    return {
        "records": len(records),
        "shape_text": f"{len(records)} records: {shape_text}",
        "nbytes": sum(r.nbytes for r in records) or None,
        "value": "",
        "dtype": "",
        "nan_elements": nan,
        "problems": problems,
        "findings": (
            f"{problems} suspect cell{'' if problems == 1 else 's'}" if problems else ""
        ),
        "columns": {},
    }


def _computed_rows(graph: _RunGraph, cells: dict, schema_keys: list[str]) -> list[dict]:
    rows = []
    for inv in graph.invocation_ids:
        outs = graph.outputs.get(inv, [])
        if not outs:
            continue  # the invocation's records were all saved by other runs
        edges = graph.edges.get(inv) or {"var_inputs": []}
        inputs: dict[str, list] = {}
        for v in edges["var_inputs"]:
            rec = cells.get(_source_rid(v))
            if rec is not None:
                inputs.setdefault(v["param_name"], []).append(rec)
        # One row per output location: a distributed call writes many.
        by_location: dict[tuple, dict] = {}
        for out_type, rid in outs:
            location = graph.locations.get(rid, {})
            entry = by_location.setdefault(_loc_key(location), {"location": location, "outputs": {}})
            if rid in cells:
                entry["outputs"].setdefault(out_type, []).append(cells[rid])
        input_side = {param: _side(recs) for param, recs in sorted(inputs.items())}
        for entry in by_location.values():
            rows.append({
                "location": {k: entry["location"][k] for k in schema_keys if k in entry["location"]},
                "status": COMPUTED,
                "status_label": STATUS_LABELS[COMPUTED],
                "reason": "",
                "inputs": input_side,
                "outputs": {t: _side(recs) for t, recs in sorted(entry["outputs"].items())},
            })
    return rows


def _outcome_rows(db: "DatabaseManager", summary: RunSummary, schema_keys: list[str]) -> list[dict]:
    """The run's failed and no-data locations (``_run_outcome``)."""
    from scidb.provenance.outcomes import NO_DATA as OUTCOME_NO_DATA
    from scidb.provenance.outcomes import TABLE

    duck = db._duck
    if not duck.table_exists(TABLE):
        return []
    key_cols = "".join(f'"{k}", ' for k in schema_keys)
    rows = duck.fetchall(
        f"SELECT {key_cols}outcome, blame, probable, reason FROM {TABLE} WHERE run_id = ?",
        [summary.run_id],
    )
    from scistacklog.journal import blame_headline

    n = len(schema_keys)
    out = []
    for row in rows:
        outcome, blame, probable, reason = row[n:]
        no_data = outcome == OUTCOME_NO_DATA
        status = NO_DATA if no_data else FAILED
        out.append({
            "location": {k: str(v) for k, v in zip(schema_keys, row[:n]) if v is not None},
            "status": status,
            "status_label": STATUS_LABELS[status],
            "reason": (
                "No input data here" if no_data
                else f"{blame_headline(blame, bool(probable), summary.function_name)}: {reason}"
            ),
            "inputs": {},
            "outputs": {},
        })
    return out


def _data_attention(variables: dict, rows: list[dict]) -> list[Attention]:
    out: list[Attention] = []
    for name, profile in variables.items():
        is_output = profile["role"] in ("output", "both")
        for col, info in profile["columns"].items():
            label = name if len(profile["columns"]) == 1 else f"{name}.{col}"
            cells = info["cells"]
            if cells["problems"] or cells["nan_elements"]:
                out.append(Attention(
                    PROBLEM if is_output else NOTE,
                    "cells",
                    f"{'Output' if is_output else 'Input'} {label}: {cells['description']}",
                    variable=name,
                ))
            if info["distinct_shapes"] > 1:
                shown = ", ".join(f"{s['shape']} ({s['count']})" for s in info["shapes"])
                more = "…" if info["distinct_shapes"] > len(info["shapes"]) else ""
                out.append(Attention(
                    PROBLEM if is_output else NOTE,
                    "shapes",
                    f"{'Output' if is_output else 'Input'} {label} has "
                    f"{info['distinct_shapes']} different shapes across its records: {shown}{more}",
                    variable=name,
                ))
        if profile["records_with_data"] < profile["records"]:
            n = profile["records"] - profile["records_with_data"]
            out.append(Attention(
                NOTE, "no_stored_data",
                f"{name}: {n} record{'' if n == 1 else 's'} hold no stored data "
                f"(lineage only, or cleared).",
                variable=name,
            ))
    introduced = carried = 0
    for row in rows:
        if row["status"] != COMPUTED:
            continue
        in_bad = any(s["nan_elements"] or s["problems"] for s in row["inputs"].values())
        out_bad = any(s["nan_elements"] or s["problems"] for s in row["outputs"].values())
        if out_bad and not in_bad:
            introduced += 1
        elif out_bad and in_bad:
            carried += 1
    if introduced:
        out.append(Attention(
            PROBLEM, "introduced",
            f"At {introduced} combination{'' if introduced == 1 else 's'} the output has "
            f"NaN, blank or constant cells although no input there does.",
        ))
    if carried:
        out.append(Attention(
            NOTE, "carried",
            f"At {carried} combination{'' if carried == 1 else 's'} both an input and the "
            f"output have NaN, blank or constant cells.",
        ))
    out.sort(key=lambda a: a.severity != PROBLEM)
    return out
