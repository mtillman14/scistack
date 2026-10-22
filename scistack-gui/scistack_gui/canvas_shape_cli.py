"""
``python -m scistack_gui.canvas_shape_cli``: record and compare what a
project's canvases show (plan ``.claude/plan-canvas-as-document.md``
Stage 0).

The regression oracle for the canvas-as-document stages, run against REAL
project databases, not only the test fixtures::

    python -m scistack_gui.canvas_shape_cli capture my.duckdb shapes.json
    # ... upgrade SciStack ...
    python -m scistack_gui.canvas_shape_cli compare my.duckdb shapes.json
    python -m scistack_gui.canvas_shape_cli combos my.duckdb

``combos`` (Stage 1, plan risk 2) lists every recorded step whose constant
combinations are not the full product of its values, with the combinations
it never ran: what adoption hides so a run from stated values does not widen
the step.

Since Stage 2 a build ADOPTS (stores what history would draw), so the first
``capture`` on a database also stores its canvas, exactly as opening it in
the GUI would.

``capture`` opens the project WITH discovery (a canvas is built with the code
registry, like an export), builds every visible pipeline's graph and writes
each one's :func:`~scistack_gui.domain.canvas_shape.canvas_shape` plus its
``canvas_sources`` counts. ``compare`` rebuilds and prints every difference
(exit code 1 when there are any). Neither writes to the database beyond what
opening the GUI already writes.

Close the GUI on that project first: DuckDB allows one writer.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

FORMAT = 1


def capture_shapes(db) -> dict:
    """``{"format", "pipelines": {pipeline_id: {"name", "shape", "sources"}}}``
    for every visible pipeline of the open project."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.canvas_shape import canvas_shape, canvas_sources
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    out: dict = {"format": FORMAT, "pipelines": {}}
    for p in ps.list_pipelines(db):
        pid = p["pipeline_id"]
        graph = get_pipeline_graph(db, pid)
        # Read AFTER the build: the first build on a database adopts (stores
        # rows), so a set read before it counts the just-stored nodes as
        # derived. Positions likewise, after anything the build moved.
        positions = layout_store.read_positions_by_scope()
        manual = set(ps.get_manual_nodes(db))
        out["pipelines"][pid] = {
            "name": p["name"],
            "shape": canvas_shape(graph, positions.get(pid, {})),
            "sources": canvas_sources(graph, manual),
        }
    return out


def non_cartesian(db) -> list[dict]:
    """``scidb.wirings.non_cartesian_report`` for the open project, with the
    GUI's PathInput names and claims (``services.adoption.gui_recorded_wirings``)."""
    from scidb.wirings import non_cartesian_report

    from scistack_gui.api.pipeline import build_aggregate
    from scistack_gui.services.adoption import gui_recorded_wirings

    scidb_agg = db.get_aggregated_variants()
    agg = build_aggregate(db, scidb_agg)
    return non_cartesian_report(gui_recorded_wirings(db, scidb_agg, agg))


def compare_shapes(before: dict, after: dict) -> list[str]:
    """Every difference between two :func:`capture_shapes` results."""
    from scistack_gui.domain.canvas_shape import diff_shapes

    out: list[str] = []
    b, a = before.get("pipelines", {}), after.get("pipelines", {})
    for pid in sorted(set(b) | set(a)):
        if pid not in a:
            out.append(f"[{pid}] pipeline missing now")
            continue
        if pid not in b:
            out.append(f"[{pid}] pipeline is new")
            continue
        out += [f"[{b[pid]['name']}] {line}" for line in diff_shapes(b[pid]["shape"], a[pid]["shape"])]
    return out


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m scistack_gui.canvas_shape_cli")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_ in (
        ("capture", "write every pipeline's canvas shape to a JSON file"),
        ("compare", "rebuild and compare against a captured JSON file"),
        ("combos", "list recorded steps whose constant combinations are not a full product"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("database", type=Path)
        if name != "combos":
            p.add_argument("shapes", type=Path)
        p.add_argument("--project", type=Path, default=None,
                       help="project root (default: the working directory)")
    args = parser.parse_args(argv)

    from scistack_gui.headless import open_for_export

    db = open_for_export(args.database, project=args.project)
    if args.command == "combos":
        report = non_cartesian(db)
        for entry in report:
            print(
                f"{entry['fn_name']} (wiring {entry['wiring_id']}): ran {entry['rows']} "
                f"combination(s) of {entry['axes']}; never ran {entry['missing']}"
            )
        print(f"{len(report)} non-Cartesian step(s)")
        return 0
    now = capture_shapes(db)
    for pid, entry in now["pipelines"].items():
        print(f"{entry['name']} ({pid}): {entry['sources']}", file=sys.stderr)

    if args.command == "capture":
        args.shapes.write_text(json.dumps(now, indent=2, sort_keys=True))
        print(f"wrote {len(now['pipelines'])} pipeline shape(s) to {args.shapes}")
        return 0

    before = json.loads(args.shapes.read_text())
    diffs = compare_shapes(before, now)
    for line in diffs:
        print(line)
    print(f"{len(diffs)} difference(s)")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
