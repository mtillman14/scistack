"""
Provenance introspection for the GUI — a thin shell over ``scidb.inspect``.

Nothing here computes provenance. ``Inspector.provenance`` does, and the
``scidb trace --variant … --runs`` command calls the same method on the same
object, so the panel and the terminal cannot answer differently (CLAUDE.md
NOTE 3: the solution lives in the owning layer).

That is the *shared API* reading of "the CLI should power the GUI", and it was
chosen over literally shelling out to ``scidb --json``: a subprocess would open
a SECOND connection to a single-writer DuckDB file, which is exactly the
write-lock contention the MATLAB run-ownership work removed
(docs/claude/matlab-run-database-ownership.md). Parity is kept instead by
building the CLI command first and giving the panel no question the command
cannot also answer.
"""

import dataclasses
import logging

from scidb.exceptions import AmbiguousVersionError, NotFoundError

logger = logging.getLogger("scistack_gui.provenance")


def variable_provenance(
    db,
    variable: str,
    *,
    selection: dict | None = None,
    schema: dict | None = None,
    include_runs: bool = True,
) -> dict:
    """Provenance of one pinned variant, down to the runs that produced it.

    ``selection`` is the picker's own column-keyed dict (``{"Code:grSides":
    "v2"}``) — scidb canonicalizes it, so the panel hands over exactly what it
    is already holding and no translation lives here.

    ``schema`` narrows to one location (``{"subject": "S01"}``). Omitted, the
    pin usually matches one record per location; the reply's
    ``matched_record_ids`` says how many, and the tree is rooted at the most
    recently saved of them.

    Returns ``dataclasses.asdict`` of the ``ProvenanceTree`` — the same shape
    ``scidb trace --json`` prints, which is what makes the two checkable
    against each other.
    """
    from scistack_gui.db import db_connection

    with db_connection("variable_provenance"):
        try:
            tree = db.inspect.provenance(
                variable,
                selection or None,
                include_runs=include_runs,
                **(schema or {}),
            )
        except (NotFoundError, AmbiguousVersionError, ValueError) as exc:
            # A pin that matches nothing is the ordinary outcome of clicking
            # around the picker, not a server fault: report it as an answer the
            # panel can render in place, so the user sees WHICH selection came
            # up empty instead of a red toast with a traceback.
            logger.info(
                "[provenance] %s %s: %s", variable, selection or "(no pin)", exc
            )
            return {
                "variable": variable,
                "selection": selection or {},
                "error": str(exc),
                "nodes": [],
                "edges": [],
                "runs": [],
                "matched_record_ids": [],
                "root_record_id": None,
            }

    payload = dataclasses.asdict(tree)
    payload["variable"] = variable
    # Flat, newest-first run list for the panel's summary strip. Derived here
    # rather than in TSX because "which runs produced what I am looking at" is
    # the question the panel exists for, and a rule written in the webview has
    # no test.
    seen: set = set()
    runs: list[dict] = []
    for node in payload["nodes"]:
        for run in node["runs"]:
            key = (run["run_id"], run["invocation_id"])
            if key in seen:
                continue
            seen.add(key)
            runs.append({**run, "function_name": node["function_name"]})
    runs.sort(key=lambda r: r["timestamp"], reverse=True)
    payload["runs"] = runs

    logger.info(
        "[provenance] %s pin=%s schema=%s: %d node(s), %d edge(s), %d run(s), "
        "%d matched record(s), root=%s",
        variable,
        selection or "(no pin)",
        schema or "(any location)",
        len(payload["nodes"]),
        len(payload["edges"]),
        len(runs),
        len(payload["matched_record_ids"]),
        (payload["root_record_id"] or "")[:8],
    )
    return payload
