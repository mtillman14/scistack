"""Canvas adoption: the one writer that turns recorded runs into document rows.

Plan ``.claude/plan-canvas-as-document.md`` Stage 2; plain-language
walkthrough ``docs/claude/canvas-as-document-explained.md`` Step 1. The pure
rule is ``domain/adoption.py``; this module reads the inputs it needs and
persists its plan.

**When.** Inside every graph build (``api/pipeline._build_graph``), after the
build has assembled what history would draw. On most builds there is nothing
new and nothing is written. It covers, with one code path, the three cases the
user framed on 2026-10-21: a canvas used before (only what history added since
is stored), a canvas that is up to date (nothing), and a database the GUI has
never opened (everything). Script runs and MATLAB-terminal runs are recorded
history like any other, so they arrive the same way (Stage 6).

**What is written, all in the project's one .duckdb:**

* ``_pipeline_nodes`` rows with the id the build already gives each node;
* drawn edges (``_intent``, aspect ``wiring``) whose id is the connection id;
* STATED Parameter values (``intent_store.CONSTANT_STATED``, never pending:
  pending overrides what a run computes);
* hidden combos for the combinations a non-Cartesian step never ran (Q2a);
* ``_adopted`` marker rows, so nothing is adopted twice;
* one audit row in ``_canvas_change`` (``method="adopt"``).

**Not an undo step** (plan §8b). A build that runs inside an undoable
handler's recording window does not write: the rows would join the user's undo
step, undoing it would delete them while the marker says they were adopted,
and they would never come back. The build then draws the plan without storing
it (``document_view(pending=...)``) and the next plain build stores it.
"""

from __future__ import annotations

import json
import logging
import time

from scistack_gui.domain.adoption import (
    AdoptionPlan,
    combo_subject,
    plan_adoption,
    visibility_by_node,
)
from scistack_gui.ids import ROOT_SCOPE, strip_placement

logger = logging.getLogger(__name__)


def gui_recorded_wirings(db, scidb_agg: dict, agg) -> list:
    """``scidb.wirings.recorded_wirings`` with the GUI's PathInput names and
    ``_node_wiring`` claims, so each wiring id equals the one the canvas uses
    for that step."""
    from scidb.wirings import recorded_wirings

    from scistack_gui import node_wiring
    from scistack_gui.domain import graph_builder as gb

    names: dict = {}
    for fkey, params in gb.path_input_bindings_by_fkey(agg.path_inputs).items():
        fn, cid = fkey
        names.setdefault((fn, gb.call_id_of(cid)), {}).update(params)
    return recorded_wirings(scidb_agg, node_wiring.claimed_wirings(db), names)


def missing_combo_hides(db, wirings, adopted: set) -> list[dict]:
    """``[{node_id, function_name, variant_key, call_id}]``: one hidden combo
    per combination a recorded step never ran (``RecordedWiring.
    missing_combinations``), not yet adopted.

    The call id is computed the way a fresh run of that combination would be
    (``variant_resolver.compute_call_id``) from the step's recorded bindings
    with default run options. When a step ran with other options the id simply
    does not match, and the combination shows up again: it can never hide the
    wrong one.
    """
    from scidb.foreach_config import RunOptions

    from scistack_gui.domain.edge_resolver import pathinput_binding, variable_binding
    from scistack_gui.domain.variant_resolver import compute_call_id
    from scistack_gui.ids import fn_node_id

    out: list[dict] = []
    for w in wirings:
        missing = w.missing_combinations()
        if not missing:
            continue
        bindings = {
            p: variable_binding(list(t) if isinstance(t, (list, tuple)) else [t])
            for p, t in w.input_params.items()
        }
        for p, name in (w.path_inputs or {}).items():
            bindings[p] = pathinput_binding(name)
        for combo in missing:
            target = {
                "bindings": bindings,
                "constants": combo,
                "glue_chains": dict(w.glue_chains or {}),
            }
            cid = compute_call_id(w.fn_name, target, RunOptions())
            if cid is None or combo_subject(w.fn_name, cid) in adopted:
                continue
            out.append(
                {
                    "node_id": fn_node_id(w.fn_name, cid),
                    "function_name": w.fn_name,
                    "variant_key": {k: v for k, v in combo.items()},
                    "call_id": cid,
                }
            )
        logger.info(
            "[adoption] step %s (wiring %s) ran %d of %d combinations of %s; "
            "the %d it never ran are hidden so a run from the stated values "
            "does not widen it",
            w.fn_name,
            w.wiring_id,
            len(w.constant_rows),
            len(w.constant_rows) + len(missing),
            sorted(w.constant_axes),
            len(missing),
        )
    return out


def recorded_node_settings(db, labels: "dict[str, str]") -> dict[str, dict]:
    """``{node_id: {"columnSelections": ..., "runOptions": ...}}``: what the
    recorded runs of each adopted function node's CURRENT wiring agree on.

    Plan D1 follow-up (2026-10-22). A node's re-run uses its recorded rows,
    which carry their selectors and options; after a clear-data there are no
    rows, and the fallback builds targets from the node's edges, values and
    settings. So the settings must hold what the rows held:

    * the column selector of each input (a Python ``Var["knee"]``);
    * ``distribute``, ``as_table`` and ``across_variants`` (AcrossVariants
      pooling), the identity-bearing run options.

    Only what EVERY recorded row of the wiring agrees on is kept; a
    disagreement (one call site selected columns, another did not) is logged
    and left unset, since no single setting reproduces both.
    """
    from scistack_gui import node_wiring
    from scistack_gui.domain import column_selection as _cs
    from scistack_gui.services.execution_service import (
        _attach_db_path_inputs,
        history_variant_wirings,
    )

    wanted: dict[str, str] = {}
    for node_id in labels:
        wiring = node_wiring.current_wiring(db, node_id)
        if wiring is not None:
            wanted[node_id] = wiring
    if not wanted:
        return {}
    by_fn: dict[str, list[dict]] = {}
    for v in db.list_pipeline_variants():
        by_fn.setdefault(v["function_name"], []).append(v)

    out: dict[str, dict] = {}
    for node_id, wiring in sorted(wanted.items()):
        fn = labels[node_id]
        variants = _attach_db_path_inputs(db, fn, [dict(v) for v in by_fn.get(fn, [])])
        rows = [v for v, wid in history_variant_wirings(db, fn, variants) if wid == wiring]
        if not rows:
            continue
        settings: dict = {}

        selections: dict = {}
        params = sorted({p for r in rows for p in (r.get("input_types") or {})})
        for param in params:
            seen = {
                json.dumps(_cs.normalize((r.get("selectors") or {}).get(param)), sort_keys=True)
                for r in rows
            }
            if len(seen) > 1:
                logger.warning(
                    "[adoption] %s: recorded runs used different column selections "
                    "for '%s' (%s); none is stored on the node",
                    node_id,
                    param,
                    sorted(seen),
                )
                continue
            sel = json.loads(next(iter(seen)))
            if sel is not None:
                selections[param] = sel
        if selections:
            settings["columnSelections"] = selections

        options: dict = {}
        for key, default in (("distribute", False), ("as_table", []), ("across_variants", [])):
            seen = {json.dumps(r.get(key, default), sort_keys=True, default=str) for r in rows}
            if len(seen) > 1:
                logger.warning(
                    "[adoption] %s: recorded runs used different '%s' (%s); not stored",
                    node_id,
                    key,
                    sorted(seen),
                )
                continue
            value = json.loads(next(iter(seen)))
            if value and value != default:
                options[key] = value
        if options:
            settings["runOptions"] = {"distribute": False, **options}
        if settings:
            out[node_id] = settings
    return out


def _scopes(db, positions_by_scope: dict) -> set[str]:
    from scistack_gui import pipeline_store as ps

    return (
        {p["pipeline_id"] for p in ps.list_all_pipelines(db)}
        | set(positions_by_scope or {})
        | {ROOT_SCOPE}
    )


def plan_for_build(
    db,
    *,
    nodes: list[dict],
    edges: list[dict],
    manual_nodes: dict,
    drawn_edges,
    positions_by_scope: dict,
    agg,
    scidb_agg: dict,
) -> tuple[AdoptionPlan, list[dict]]:
    """What this build would adopt: ``(plan, combo hides)``. Writes nothing."""
    from scistack_gui import node_wiring
    from scistack_gui import pipeline_store as ps

    t0 = time.perf_counter()
    adopted = node_wiring.adopted_subjects(db)
    have: dict[str, set[str]] = {}
    for source in (ps.get_pending_constants(db), ps.get_stated_constants(db)):
        for name, values in source.items():
            have.setdefault(name, set()).update(values)
    # The common build adopts nothing. Ask the rule first with everything
    # treated as visible: if even that adopts nothing, the per-scope
    # visibility pass (one resolve_scope_view per canvas) is skipped.
    everywhere = {str(strip_placement(n["id"])): [ROOT_SCOPE] for n in nodes}
    probe = plan_adoption(
        nodes,
        edges,
        manual_nodes=manual_nodes,
        drawn_edges=drawn_edges,
        adopted=adopted,
        visible=everywhere,
        visible_edge_ids={e["id"] for e in edges},
        recorded_values=agg.const_counts,
        stated_values=have,
    )
    if probe.empty:
        logger.debug(
            "[adoption] nothing to adopt (%.1f ms)", (time.perf_counter() - t0) * 1000.0
        )
        return probe, []
    visible, visible_edges = visibility_by_node(
        nodes, edges, _scopes(db, positions_by_scope), manual_nodes, positions_by_scope
    )
    plan = plan_adoption(
        nodes,
        edges,
        manual_nodes=manual_nodes,
        drawn_edges=drawn_edges,
        adopted=adopted,
        visible=visible,
        visible_edge_ids=visible_edges,
        recorded_values=agg.const_counts,
        stated_values=have,
    )
    combos: list[dict] = []
    if plan.nodes:
        # Only on a build that adopts steps: the combinations of a step are
        # settled when the step itself is stored.
        combos = missing_combo_hides(db, gui_recorded_wirings(db, scidb_agg, agg), adopted)
    logger.info(
        "[adoption] plan: %s; %d hidden combo(s) (%.1f ms)",
        plan.summary(),
        len(combos),
        (time.perf_counter() - t0) * 1000.0,
    )
    return plan, combos


def write(db, plan: AdoptionPlan, combos: list[dict]) -> "str | None":
    """Persist *plan*. Returns the audit event id, or ``None`` when nothing
    was written. One INFO line per adopted step (NOTE 2)."""
    from scistack_gui import history, intent_store, library_lock, node_wiring

    if plan.empty and not combos:
        return None
    t0 = time.perf_counter()
    with library_lock.library_writes():
        duck = db._duck
        for node_id, node_type, label, scope in plan.nodes:
            duck._execute(
                "INSERT INTO _pipeline_nodes (node_id, node_type, label, pipeline_id) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (node_id) DO NOTHING",
                [node_id, node_type, label, scope],
            )
        for edge in plan.edges:
            intent_store.write_manual_edge(db, edge)
        # What the recorded runs of each adopted step agree on (selectors, run
        # options) becomes the node's own settings, never overriding one the
        # user already made. Written with the node, so once.
        from scistack_gui import pipeline_store as ps

        fn_labels = {nid: label for nid, t, label, _s in plan.nodes if t == "functionNode"}
        for node_id, recorded in recorded_node_settings(db, fn_labels).items():
            existing = ps.get_node_config(db, node_id) or {}
            new = {k: v for k, v in recorded.items() if not existing.get(k)}
            if not new:
                continue
            ps.update_node_config(db, node_id, {**existing, **new})
            logger.info(
                "[adoption] %s keeps its recorded settings: %s", node_id, new
            )
        written_values = intent_store.state_constants(db, plan.values)
        for c in combos:
            intent_store.hide_combo(db, c["node_id"], c["function_name"], c["variant_key"])
        node_wiring.mark_adopted(
            db,
            [*plan.subjects, *[
                (combo_subject(c["function_name"], c["call_id"]), c["node_id"], ROOT_SCOPE)
                for c in combos
            ]],
        )
    # One line per adopted step, naming what it was stored with.
    edges_by_node: dict[str, list[str]] = {}
    for e in plan.edges:
        for end in (e["source"], e["target"]):
            edges_by_node.setdefault(str(strip_placement(end)), []).append(e["id"])
    for node_id, node_type, label, scope in plan.nodes:
        if node_type != "functionNode":
            continue
        logger.info(
            "[adoption] adopted step %s (%s) into scope %s with %d edge(s): %s",
            label,
            node_id,
            scope,
            len(edges_by_node.get(node_id, [])),
            sorted(edges_by_node.get(node_id, [])),
        )
    event = history.log_event(
        db,
        f"Added {len(plan.functions())} step(s) from recorded runs",
        "adopt",
        f"{plan.summary()}; {written_values} value(s) stated; {len(combos)} combo(s) hidden",
    )
    logger.info(
        "[adoption] wrote %s; %d value(s) stated, %d combo(s) hidden (%.1f ms)",
        plan.summary(),
        written_values,
        len(combos),
        (time.perf_counter() - t0) * 1000.0,
    )
    return event


def retire_rewired_edges(
    db,
    rewired: "dict[str, str]",
    edges: list[dict],
    drawn_edges,
    manual_nodes: dict,
) -> list[str]:
    """Hide the stored edges of each node a NAMED script run rewired
    (``IdentityPlan.rewired_by_step``) that its new wiring does not have.
    Returns the hidden connection ids.

    *edges* is the build's edge list, whose history edges for the node are
    its NEW wiring's (the build draws a node's current wiring). Every stored
    edge into or out of the node whose connection is not among them is the
    old wiring's and is hidden (never deleted: the restore panel lists it).
    The new wiring's edges are then adopted like any other.

    Once per (node, wiring) (``_adopted`` subject ``rewire:...``); not inside
    an undoable edit's recording (same reason as adoption itself).
    """
    from scistack_gui import history, library_lock, node_wiring
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.graph_builder import edge_connection_id, is_manual_edge

    if not rewired or history.is_recording():
        return []
    adopted = node_wiring.adopted_subjects(db)
    hidden: list[str] = []
    marks = []
    for node_id, wiring in sorted(rewired.items()):
        subject = f"rewire:{node_id}:{wiring}"
        if subject in adopted:
            continue
        keep = {
            edge_connection_id(e, manual_nodes)
            for e in edges
            if not is_manual_edge(e)
            and node_id in (str(strip_placement(e["source"])), str(strip_placement(e["target"])))
        }
        scope = (manual_nodes.get(node_id) or {}).get("pipeline_id") or ROOT_SCOPE
        for e in drawn_edges:
            ends = (str(strip_placement(e.get("source") or "")), str(strip_placement(e.get("target") or "")))
            if node_id not in ends:
                continue
            connection = edge_connection_id(e, manual_nodes)
            if connection in keep:
                continue
            with library_lock.library_writes():
                ps.hide_edge(
                    db,
                    e["id"],
                    e.get("source"),
                    e.get("target"),
                    e.get("sourceHandle"),
                    e.get("targetHandle"),
                    scope,
                )
            hidden.append(connection)
        marks.append((subject, node_id, scope))
        logger.info(
            "[adoption] node %s follows its named script run to wiring %s: %d old "
            "edge(s) hidden",
            node_id,
            wiring,
            sum(1 for _ in hidden),
        )
    if marks:
        node_wiring.mark_adopted(db, marks)
    return hidden


def notification(plan: AdoptionPlan, event: "str | None") -> "dict | None":
    """The graph-response warning that tells the user steps were added
    (user decision Q1: automatic, with a notification)."""
    steps = plan.functions()
    if not plan.nodes:
        return None
    if steps:
        message = (
            f"{len(steps)} step(s) from recorded runs were added to the canvas: "
            f"{', '.join(steps)}."
        )
    else:
        message = f"{len(plan.nodes)} node(s) from recorded runs were added to the canvas."
    return {
        "kind": "adopted",
        "signature": f"adopted:{event or ','.join(sorted(plan.node_ids))}",
        "message": message,
        "functions": steps,
        "nodes": len(plan.nodes),
        "edges": len(plan.edges),
    }
