"""The run report in the GUI: thin wrappers over ``scidb.inspect``.

Every count, status, sentence and attention item comes from scidb
(``scidb.provenance.run_report``, through ``Inspector``), the same calls
``scidb run-report`` makes (docs/claude/run-report.md). This module only
picks the part asked for and shapes ``run_done.reports``.

One handler (``run_report``) with a ``part``:

* ``list`` — the recorded runs of a function node, newest first;
* ``summary`` — one run without loading data (the popup's first paint);
* ``data`` — what the run's records hold (loaded when the popup asks).
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

PARTS = ("list", "summary", "data")

#: Runs the picker lists.
LIST_LIMIT = 50


def _inspector(db):
    from scidb.inspect import Inspector

    return Inspector(db)


def run_report(
    db,
    part: str,
    *,
    function_name: str | None = None,
    outputs: list[str] | None = None,
    run_id: str | None = None,
    status: str | None = None,
    limit: int = 2000,
) -> dict:
    """The reply for one *part* (see the module docstring)."""
    if part not in PARTS:
        raise ValueError(f"run_report part must be one of {list(PARTS)}, got {part!r}")
    t0 = time.perf_counter()
    insp = _inspector(db)
    if part == "list":
        if not function_name:
            raise ValueError("run_report part 'list' needs a function_name")
        runs = insp.run_reports(function_name, outputs=outputs or None, limit=LIST_LIMIT)
        out = {"function_name": function_name, "runs": [r.to_dict() for r in runs]}
        detail = f"{len(runs)} run(s) of {function_name}"
    else:
        if not run_id:
            raise ValueError(f"run_report part {part!r} needs a run_id")
        if part == "summary":
            out = insp.run_report(run_id).to_dict()
            detail = f"run={run_id} {len(out['attention'])} attention item(s)"
        else:
            out = insp.run_report_data(run_id, status=status or None, limit=max(0, limit)).to_dict()
            detail = f"run={run_id} {len(out['rows'])}/{out['total_rows']} row(s) sent"
    logger.info(
        "[run_report] part=%s %s in %.1f ms", part, detail, (time.perf_counter() - t0) * 1000
    )
    return out


def matlab_reports_since(dispatched_at: float, node_id: str | None) -> list[dict]:
    """``run_done.reports`` for a MATLAB run the GUI only watched: the MATLAB
    runs stored since it was dispatched (the bridge writes them; nothing
    tells this process their ids). A single-node run's rows all belong to
    its node; a pipeline run's are one entry per function, in start order,
    with no node. Never raises: a report that cannot be offered is not a
    failed run."""
    from datetime import datetime, timezone

    try:
        from scidb.provenance.run_summary import run_summaries
        from scistack_gui.runtime.db import db_connection, get_db

        since = datetime.fromtimestamp(dispatched_at, timezone.utc).replace(tzinfo=None)
        with db_connection("matlab_reports_since"):
            runs = run_summaries(get_db()._duck, since=since, language="matlab", limit=LIST_LIMIT)
    except Exception as e:
        logger.info("[run_report] MATLAB runs since dispatch not read: %s: %s", type(e).__name__, e)
        return []
    runs = list(reversed(runs))  # start order
    by_fn: dict[str, list[str]] = {}
    for r in runs:
        by_fn.setdefault(r.function_name, []).append(r.run_id)
    if node_id and len(by_fn) > 1:
        logger.info(
            "[run_report] node %s: MATLAB runs of %d functions since dispatch (%s); "
            "all listed without a node", node_id, len(by_fn), sorted(by_fn),
        )
    entries = [
        {"node_id": node_id if len(by_fn) == 1 else None, "function_name": fn, "run_ids": ids}
        for fn, ids in by_fn.items()
    ]
    logger.info(
        "[run_report] MATLAB run dispatched %.1f s ago stored %d run(s) of %d function(s)",
        time.time() - dispatched_at, len(runs), len(by_fn),
    )
    return reports_wire(entries)


def reports_wire(entries: list[dict]) -> list[dict]:
    """``run_done.reports``: the function nodes a run covered that left a
    stored run, in run order — ``[{node_id, function_name, run_ids}]``. An
    entry with no run id is left out: there is no report to open for it (a
    MATLAB step, a step whose prepare failed). The frontend opens one tab per
    entry; which run ids belong to which node is decided here, once."""
    out = []
    for e in entries:
        run_ids = [r for r in (e.get("run_ids") or []) if r]
        if run_ids:
            out.append({
                "node_id": e.get("node_id"),
                "function_name": e.get("function_name") or "",
                "run_ids": run_ids,
            })
    return out
