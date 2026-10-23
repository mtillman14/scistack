"""One person's session on their own computer, for ``test_library_fork_chain.py``.

Not a test module (pytest collects ``test_*.py`` only). The fork-chain tests
run every step of every person in a separate process through this driver, so
that person sees only the libraries THEY installed (the test puts each
installed library's ``src/`` on ``PYTHONPATH``, which is what
``pip install -e`` does for imports) and nothing leaks between people through
``sys.modules``, the code registry, the pinned project root or the names
caches.

    python library_fork_actor.py SESSION.json

``SESSION.json``::

    {"project": "<root>", "db": "<root>/<pkg>.duckdb",
     "front_end": "gui" | "code",
     "schema_keys": ["subject"],          # only with a first "init" step
     "steps": [{"op": ..., ...}, ...]}

* ``front_end == "gui"``: the project is opened the way the GUI opens it
  (``headless.open_for_export``: same bootstrap, with discovery) and the
  ``http`` steps go through the GUI's own routes (``create_app`` +
  ``TestClient``), i.e. exactly the calls the frontend makes.
* ``front_end == "code"``: nothing is opened up front; ``cli`` steps run the
  ``scistack library ...`` commands (the parsers ``scistack`` mounts) as a
  terminal user would, and ``script`` steps run a plain Python file.

The last stdout line is ``RESULT {json}``: one entry per step, in order. A
step that raises stops the session; its entry is ``{"error": ...,
"traceback": ...}`` so the test can say which person, which step and why.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import subprocess
import sys
import traceback
from pathlib import Path

logger = logging.getLogger("library_fork_actor")


def _open_gui(session: dict):
    from fastapi.testclient import TestClient

    from scistack_gui.app import create_app
    from scistack_gui.headless import open_for_export

    db = open_for_export(session["db"], project=Path(session["project"]))
    client = TestClient(create_app())
    client.__enter__()
    return db, client


def _init(session: dict, step: dict) -> dict:
    from scistack_gui.headless import open_for_import

    from scistack_gui import db as gui_db

    open_for_import(
        session["db"], project=Path(session["project"]), schema_keys=step["schema_keys"]
    )
    # Creating the project is all init does: release the file, so a later
    # step that opens it in ANOTHER process (a terminal script) can lock it.
    gui_db.close_db()
    return {"ok": True}


def _canvas(db, step: dict) -> dict:
    """Setup only (not under test): a submodule built node by node, as the
    existing share tests build theirs."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.ids import new_manual_edge_id, new_manual_node_id

    pid = ps.create_pipeline(db, step["pipeline"])
    ids = {}
    for i, (key, ntype, label) in enumerate(step["nodes"]):
        ids[key] = new_manual_node_id(ntype, label)
        ps.write_manual_node(db, ids[key], ntype, label, pid)
        layout_store.write_node_position(ids[key], 150.0 * i, 0.0, pipeline_id=pid)
    for s, t, sh, th in step["edges"]:
        ps.write_manual_edge(db, {"id": new_manual_edge_id(), "source": ids[s],
                                  "target": ids[t], "sourceHandle": sh, "targetHandle": th})
    return {"pipeline_id": pid, "ids": ids}


def _http(client, step: dict) -> dict:
    r = client.request(step["method"], "/api" + step["path"], json=step.get("json"),
                       params=step.get("params"))
    try:
        body = r.json()
    except ValueError:
        body = r.text
    return {"status": r.status_code, "body": body}


def _add_node(client, step: dict) -> dict:
    """What a drop from the sidebar palette sends (PUT /layout/{id} with a
    type and label), then the edges the user draws (PUT /edges/{id})."""
    from scistack_gui.ids import new_manual_edge_id, new_manual_node_id

    ids = dict(step.get("existing", {}))
    out = {"nodes": {}, "edges": []}
    for i, (key, ntype, label) in enumerate(step["nodes"]):
        nid = new_manual_node_id(ntype, label)
        ids[key] = nid
        r = client.put(f"/api/layout/{nid}", json={
            "node_id": nid, "x": 900.0 + 150 * i, "y": 200.0, "node_type": ntype,
            "label": label, "pipeline_id": step["pipeline_id"]})
        out["nodes"][key] = {"id": nid, "status": r.status_code, "body": r.text[:500]}
    for s, t, sh, th in step["edges"]:
        eid = new_manual_edge_id()
        r = client.put(f"/api/edges/{eid}", json={
            "edge_id": eid, "source": ids[s], "target": ids[t],
            "source_handle": sh, "target_handle": th})
        out["edges"].append({"id": eid, "status": r.status_code, "body": r.text[:500]})
    return out


def _cli(session: dict, step: dict) -> dict:
    """``scistack library ...`` exactly as the terminal runs it (the parsers
    ``scistack/__main__.py`` mounts; scistack-gui never imports scistack)."""
    import argparse

    from scistack_gui.library_cli import add_library_subparsers

    parser = argparse.ArgumentParser(prog="scistack")
    add_library_subparsers(parser.add_subparsers(dest="command"))
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:  # argparse's usage error is a SystemExit too: report it, don't die
            args = parser.parse_args(step["argv"])
            code = args._library_cmd(args)
        except SystemExit as e:  # argparse / a command's own exit
            code = e.code
    stdout = out.getvalue()
    parsed = None
    for line in reversed(stdout.strip().splitlines()):
        try:
            parsed = json.loads(line)
            break
        except ValueError:
            continue
    return {"code": code, "stdout": stdout[-4000:], "stderr": err.getvalue()[-4000:],
            "json": parsed}


def _script(session: dict, step: dict) -> dict:
    """A plain user script, run the way the user runs it: its own process,
    from the project folder."""
    path = Path(session["project"]) / step["name"]
    path.write_text(step["source"])
    proc = subprocess.run([sys.executable, str(path)], cwd=session["project"],
                          capture_output=True, text=True, timeout=600)
    return {"code": proc.returncode, "stdout": proc.stdout[-4000:],
            "stderr": proc.stderr[-4000:]}


def _save(step: dict) -> dict:
    import numpy as np

    from scistack_gui.db import get_db
    from scistack_gui.registry import get_variable_class

    cls = get_variable_class(step["variable"])
    items = [(np.asarray(v, dtype=float), meta) for v, meta in step["items"]]
    return {"record_ids": get_db().save_batch(cls, items)}


def _run(db, step: dict) -> dict:
    """▶ Run on a placed pipeline node: the frontend asks for a plan of
    ``child_pipeline_id`` in mode "all"; the run API executes it in a
    background thread through ``execution_service.run_pipeline``, called
    here synchronously."""
    from scistack_gui.services.execution_service import run_pipeline

    result = run_pipeline(db, step["pipeline_id"], mode="all")
    return json.loads(json.dumps(result, default=str))


def _load(step: dict) -> dict:
    from scistack_gui.registry import get_variable_class

    cls = get_variable_class(step["variable"])
    out = {}
    for subject in step["subjects"]:
        try:
            out[subject] = [float(x) for x in cls.load(subject=subject).data]
        except Exception as e:  # reported, not raised: "no result" is a finding
            out[subject] = f"{type(e).__name__}: {e}"
    return out


def _invocations(db) -> dict:
    rows = db._duck._fetchall("SELECT function_name FROM _invocation")
    return {"function_names": sorted({r[0] for r in rows}), "count": len(rows)}


def _graph(db, step: dict) -> dict:
    """Labels and label edges of one canvas; with ``run_states`` also each
    function node's run state (the colour the user sees)."""
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    with_states = bool(step.get("run_states"))
    g = get_pipeline_graph(db, step["pipeline_id"], run_states=with_states)
    label = {n["id"]: n["data"].get("label") for n in g["nodes"]}
    out = {"labels": sorted(str(v) for v in label.values()),
           "edges": sorted([str(label.get(e["source"])), str(label.get(e["target"]))]
                           for e in g["edges"])}
    if with_states:
        out["states"] = {str(n["data"].get("label")): n["data"].get("run_state")
                         for n in g["nodes"] if n.get("type") == "functionNode"}
    return out


def _latest(db, step: dict) -> dict:
    """Which function (its producing invocation's ``function_name``) made the
    NEWEST record of *variable* at each schema location (newest save event,
    ``_record_save``): what a load returns today. (Version keys are not
    stored since the lineage simplification; the graph names the producer.)"""
    rows = db._duck._fetchall(
        "SELECT r.record_id, inv.function_name FROM _record r "
        "JOIN _record_save s ON s.record_id = r.record_id "
        "LEFT JOIN _invocation_output io ON io.output_record_id = r.record_id "
        "LEFT JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "WHERE r.type = ? AND s.timestamp = (SELECT max(s2.timestamp) FROM _record_save s2 "
        "JOIN _record r2 ON r2.record_id = s2.record_id "
        "WHERE r2.type = r.type AND r2.schema_id IS NOT DISTINCT FROM r.schema_id)",
        [step["variable"]],
    )
    fns = {fn for _rid, fn in rows}
    return {"functions": sorted(str(f) for f in fns),
            "count": len({rid for rid, _fn in rows})}


def _pipelines(db) -> dict:
    from scistack_gui import pipeline_store as ps

    rows = []
    for p in ps.list_pipelines(db):
        owner = ps.library_owner(db, p["pipeline_id"])
        rows.append({"pipeline_id": p["pipeline_id"], "name": p["name"],
                     "library": owner["library"] if owner else None})
    return {"pipelines": rows}


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(levelname)s %(name)s: %(message)s")
    session = json.loads(Path(argv[1]).read_text())
    results: list = []
    db = client = None
    steps = list(session["steps"])
    try:
        if steps and steps[0]["op"] == "init":
            results.append(_init(session, steps.pop(0)))
        if session["front_end"] == "gui" and any(s["op"] != "cli" for s in steps):
            db, client = _open_gui(session)
        elif any(s["op"] in ("canvas", "save", "run", "load", "invocations", "graph",
                             "pipelines", "latest") for s in steps):
            from scistack_gui.headless import open_for_export

            db = open_for_export(session["db"], project=Path(session["project"]))
        for step in steps:
            op = step["op"]
            logger.info("[actor] %s: %s", Path(session["project"]).name, op)
            if op == "canvas":
                results.append(_canvas(db, step))
            elif op == "http":
                results.append(_http(client, step))
            elif op == "add_nodes":
                results.append(_add_node(client, step))
            elif op == "cli":
                if db is not None:  # a terminal command opens the project itself
                    from scistack_gui import db as gui_db

                    gui_db.close_db()
                    db = None
                results.append(_cli(session, step))
            elif op == "script":
                if db is not None:  # the script is another process: release the file
                    from scistack_gui import db as gui_db

                    gui_db.close_db()
                    db = None
                results.append(_script(session, step))
            elif op == "save":
                results.append(_save(step))
            elif op == "run":
                results.append(_run(db, step))
            elif op == "load":
                results.append(_load(step))
            elif op == "invocations":
                results.append(_invocations(db))
            elif op == "graph":
                results.append(_graph(db, step))
            elif op == "pipelines":
                results.append(_pipelines(db))
            elif op == "latest":
                results.append(_latest(db, step))
            else:
                raise ValueError(f"unknown op {op!r}")
    except Exception as e:
        results.append({"error": f"{type(e).__name__}: {e}",
                        "traceback": traceback.format_exc()[-4000:]})
    finally:
        if client is not None:
            client.__exit__(None, None, None)
    print("RESULT " + json.dumps(results, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
