"""Step names: which canvas node a NAMED script run belongs to.

plan-canvas-as-document Q6, option A. A GUI run is attributed to its node at
dispatch (the GUI holds the node id). A script run carries no node id, so a
script that changed a step's wiring used to add a second node beside the
first. ``for_each(..., step="filter")`` names the step; scidb records it on
the run (``_run_step``); the node whose step name is ``filter`` claims that
run's wiring, whatever it is, and its stored edges follow it
(``services.adoption.retire_rewired_edges``).

**Where a node's step name lives:** the ``step`` key of its ``_node_config``
blob. It is not an intent aspect (it changes nothing a run computes, and it
is never identity: renaming a step re-runs nothing), so it stays in the blob,
which undo already tracks.

**Who gives a node one:** code export, for every node it writes a ``for_each``
for (:func:`ensure_step_names`), so the exported script and the canvas agree
from then on. A GUI run passes the node's step name to ``for_each`` too, so
a named step's history is named however it was run.

One name per function label per database: two ``scale`` nodes are ``scale``
and ``scale_2``. Names are unique ACROSS functions only by convention (a
claim also requires the function to match).
"""

from __future__ import annotations

import json
import logging
import re

from scistack_gui.ids import strip_placement

logger = logging.getLogger(__name__)

STEP_KEY = "step"


def _duck(db):
    return db._duck


def step_names(db) -> dict[str, str]:
    """``{bare node id: step name}`` for every node that has one."""
    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(db)
    out: dict[str, str] = {}
    for node_id, raw in _duck(db)._fetchall("SELECT node_id, config FROM _node_config"):
        try:
            step = (json.loads(raw) or {}).get(STEP_KEY) if raw else None
        except (TypeError, ValueError):
            continue
        if isinstance(step, str) and step.strip():
            out[str(strip_placement(node_id))] = step.strip()
    return out


def step_name_of(db, node_id: str) -> "str | None":
    return step_names(db).get(str(strip_placement(node_id)))


def set_step_name(db, node_id: str, step: "str | None") -> None:
    """Write (or, with ``None``, remove) a node's step name, keeping the rest
    of its config blob as it is."""
    from scidb.intent import check_step

    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(db)
    bare = str(strip_placement(node_id))
    row = _duck(db)._fetchone("SELECT config FROM _node_config WHERE node_id = ?", [bare])
    blob = {}
    if row and row[0]:
        try:
            blob = json.loads(row[0]) or {}
        except (TypeError, ValueError):
            blob = {}
    if step:
        blob[STEP_KEY] = check_step(step)
    else:
        blob.pop(STEP_KEY, None)
    _duck(db)._execute(
        "INSERT INTO _node_config (node_id, config) VALUES (?, ?) "
        "ON CONFLICT (node_id) DO UPDATE SET config = excluded.config",
        [bare, json.dumps(blob)],
    )
    logger.info("[step_names] %s is step %r", bare, blob.get(STEP_KEY))


def _slug(label: str) -> str:
    """A function label as a step name: its last dotted part, word characters."""
    tail = label.rsplit(".", 1)[-1]
    return re.sub(r"\W+", "_", tail).strip("_") or "step"


def ensure_step_names(db, nodes: "list[tuple[str, str]]") -> dict[str, str]:
    """Give every ``(node_id, function label)`` in *nodes* a step name if it
    has none, unique per function label; return ``{node id: step name}`` for
    all of them (existing names kept)."""
    names = step_names(db)
    taken_by_label: dict[str, set[str]] = {}
    labels = dict(nodes)
    for nid, step in names.items():
        label = labels.get(nid)
        if label is not None:
            taken_by_label.setdefault(label, set()).add(step)
    out: dict[str, str] = {}
    for node_id, label in nodes:
        bare = str(strip_placement(node_id))
        if bare in names:
            out[node_id] = names[bare]
            continue
        base = _slug(label)
        taken = taken_by_label.setdefault(label, set())
        step, k = base, 2
        while step in taken:
            step, k = f"{base}_{k}", k + 1
        taken.add(step)
        set_step_name(db, bare, step)
        names[bare] = step
        out[node_id] = step
    return out


def step_claims(
    agg,
    wiring_of,
    manual_nodes: "dict[str, dict]",
    names: "dict[str, str]",
) -> dict[tuple[str, str], list[str]]:
    """``{(fn_name, wiring): [node id]}``: recorded wirings whose runs carry a
    step name some stored node of that function has.

    *wiring_of* is ``fkey -> wiring id`` (the one recipe the build uses). Pure.
    """
    from scistack_gui.domain.graph_builder import call_id_of

    if not names or not getattr(agg, "fn_steps", None):
        return {}
    by_step: dict[tuple[str, str], list[str]] = {}
    for nid, step in names.items():
        meta = manual_nodes.get(nid) or {}
        if meta.get("type") == "functionNode":
            by_step.setdefault((meta.get("label"), step), []).append(nid)
    claims: dict[tuple[str, str], list[str]] = {}
    for fkey in agg.fn_input_params:
        fn, cid = fkey
        steps = agg.fn_steps.get((fn, call_id_of(cid))) or []
        # The NEWEST step name the call site ran under decides.
        for step in reversed(steps):
            holders = by_step.get((fn, step))
            if holders:
                key = (fn, wiring_of(fkey))
                merged = claims.setdefault(key, [])
                for h in holders:
                    if h not in merged:
                        merged.append(h)
                break
    if claims:
        logger.info("[step_names] named runs claim wiring(s): %s", claims)
    return claims
