"""Step verdicts, the data view and "Clean up outdated…" in the GUI (GitHub #28).

Thin wrappers over scidb: every state, word, chip and count comes from
``scidb.provenance.status`` / ``data_view`` / ``inspect.variant_cards``
(docs/claude/pipeline-status.md). This module only:

* attaches each canvas function node's verdict to the graph payload
  (:func:`attach_step_verdicts`), adding the one GUI-only reason (an input
  edge not connected) in the same chip;
* answers the ``data_view`` handler's parts;
* runs the bulk outdated clean-up as ONE delete (one trash batch, one Ctrl Z).
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

#: The GUI-only reason (an input edge not connected), shown in the same chip.
DISCONNECTED_REASON = {"kind": "disconnected", "count": None, "glyph": "✂", "text": "✂ input not connected"}

#: Variable nodes whose outdated variants are counted on a canvas build.
OUTDATED_VARIABLE_CAP = 200

PARTS = ("view", "step", "outdated", "cleanup_plan", "between", "cell")


def _compact(v) -> dict:
    """What a node carries: the words and counts, no per-location lists."""
    d = v.to_dict(location_limit=0)
    d.pop("outputs", None)
    for r in d.get("reasons", []):
        r["locations"] = r.get("locations", [])[:5]
    return d


def attach_step_verdicts(db, nodes: list[dict]) -> None:
    """``node.data.verdict`` for every function node that names its outputs,
    and ``node.data.outdated_variants`` for variable nodes. Never raises: a
    node whose verdict fails keeps its colour and gets no chip."""
    from scidb import BaseVariable
    from scidb.provenance.status import step_status

    from scistack_gui.services.graph_service import fn_registry_for

    t0 = time.perf_counter()
    fn_nodes = [
        n for n in nodes
        if n.get("type") == "functionNode" and (n.get("data") or {}).get("output_types")
    ]
    registry = fn_registry_for({n["data"].get("label") for n in fn_nodes if n["data"].get("label")})
    done = failed = 0
    slowest = (0.0, "")
    for node in fn_nodes:
        data = node["data"]
        fn_name = data.get("label")
        t1 = time.perf_counter()
        try:
            inputs = {
                p: BaseVariable._all_subclasses[t]
                for p, t in (data.get("input_params") or {}).items()
                if isinstance(t, str) and t in BaseVariable._all_subclasses
            }
            v = step_status(
                db,
                registry.get(fn_name) or fn_name,
                outputs=list(data["output_types"]),
                fn_registry=registry,
                inputs=inputs or None,
                level=data.get("schemaLevel"),
            )
            verdict = _compact(v)
            if data.get("disconnected"):
                verdict["reasons"] = [dict(DISCONNECTED_REASON), *verdict["reasons"]]
                verdict["chip"] = DISCONNECTED_REASON["text"]
                verdict["needs_running"] = True
            data["verdict"] = verdict
            done += 1
        except Exception as e:  # a chip is never worth a failed canvas
            failed += 1
            logger.warning("[step_status] %s (%s): verdict not computed: %s: %s",
                           node["id"], fn_name, type(e).__name__, e)
        dt = time.perf_counter() - t1
        if dt > slowest[0]:
            slowest = (dt, str(fn_name))
    _attach_outdated_variants(db, nodes, registry)
    logger.info(
        "[step_status] %d verdict(s), %d failed, in %.1f ms (slowest %s %.1f ms)",
        done, failed, (time.perf_counter() - t0) * 1000, slowest[1], slowest[0] * 1000,
    )


def exclusions_wire(db) -> list[dict]:
    """The exclusions in force, for the Activity Bar tree ("Exclusions (N)"
    with reason and time). Read through Inspector (scidb owns them)."""
    from scidb.inspect import Inspector

    try:
        return [
            {"keys": e.schema, "reason": e.reason, "changed_at": e.changed_at, "variable": e.variable}
            for e in Inspector(db).exclusions()
        ]
    except Exception as e:  # the canvas never fails over its side panel
        logger.debug("[step_status] exclusions not read: %s", e)
        return []


def _attach_outdated_variants(db, nodes: list[dict], registry: dict) -> None:
    """``"2 outdated variants"`` on a variable node (scidb owns the flag)."""
    from scidb.inspect.variant_cards import variant_outdated

    var_nodes = [n for n in nodes if n.get("type") == "variableNode"][:OUTDATED_VARIABLE_CAP]
    t0 = time.perf_counter()
    n_flagged = 0
    for node in var_nodes:
        name = (node.get("data") or {}).get("label")
        if not name or not (node["data"].get("total_records") or node["data"].get("record_count")):
            continue
        try:
            rows = variant_outdated(db, name, registry)
        except Exception as e:
            logger.debug("[step_status] %s: outdated flag not computed: %s", name, e)
            continue
        n = sum(1 for r in rows if r["outdated"] is True)
        if n:
            node["data"]["outdated_variants"] = n
            n_flagged += 1
    logger.info(
        "[step_status] outdated variants: %d of %d variable node(s) flagged in %.1f ms",
        n_flagged, len(var_nodes), (time.perf_counter() - t0) * 1000,
    )


def data_view(
    db,
    part: str,
    *,
    variables: list[str] | None = None,
    variants: dict | None = None,
    function_name: str | None = None,
    outputs: list[str] | None = None,
    call_id: str | None = None,
    variable: str | None = None,
    record_ids: list[str] | None = None,
) -> dict:
    """The reply for one *part*:

    * ``view`` — every data-view column, footer and drop-off;
    * ``step`` — one step's verdict with its locations (the run report's
      reasons and the Activity Bar tree's expansion);
    * ``outdated`` — one variable's cards, outdated or not;
    * ``cleanup_plan`` — every outdated variant with what deleting it removes;
    * ``between`` — the declared between keys;
    * ``cell`` — what made one cell's record(s) ("made by f v3 with …").
    """
    from scidb.inspect import Inspector

    from scistack_gui.services.graph_service import fn_registry_for

    if part not in PARTS:
        raise ValueError(f"data_view part must be one of {list(PARTS)}, got {part!r}")
    t0 = time.perf_counter()
    insp = Inspector(db)
    if part == "cell":
        from scidb.provenance.data_view import cell_detail

        if not variable:
            raise ValueError("data_view part 'cell' needs a variable")
        out = cell_detail(db, variable, list(record_ids or []))
    elif part == "between":
        from scidb.store.between_keys import between_keys

        out = {"between": between_keys(db), "schema_keys": list(db.dataset_schema_keys)}
    elif part == "view":
        from scidb.provenance.query import variable_type_edges, variable_types_in_pipeline_order
        from scidb.provenance.status import _producers

        duck = db._duck
        names = variables or variable_types_in_pipeline_order(duck, variable_type_edges(duck))
        fns = {p for n in names for p in _producers(duck, n)[:1]}
        out = insp.data_view(names, fn_registry=fn_registry_for(fns), variants=variants)
    elif part == "step":
        if not function_name:
            raise ValueError("data_view part 'step' needs a function_name")
        registry = fn_registry_for([function_name])
        v = insp.step_status(
            registry.get(function_name) or function_name,
            outputs or None,
            call_id=call_id,
            fn_registry=registry,
        )
        out = v.to_dict(location_limit=500)
    elif part == "outdated":
        if not variable:
            raise ValueError("data_view part 'outdated' needs a variable")
        from scidb.provenance.status import _producers

        out = {"variable": variable,
               "cards": insp.variant_outdated(variable, fn_registry_for(_producers(db._duck, variable)))}
    else:
        from scidb.provenance.query import variable_type_edges, variable_types_in_pipeline_order
        from scidb.provenance.status import _producers

        duck = db._duck
        names = variables or variable_types_in_pipeline_order(duck, variable_type_edges(duck))
        fns = {p for n in names for p in _producers(duck, n)}
        plan = insp.outdated_cleanup(fn_registry_for(fns), names)
        targets = [c["target"] for c in plan["candidates"]]
        fingerprint = None
        if targets:
            fingerprint = insp.delete_plan(targets).fingerprint
        out = {**plan, "fingerprint_all": fingerprint}
    logger.info("[data_view] part=%s in %.1f ms", part, (time.perf_counter() - t0) * 1000)
    return out


def cleanup_outdated(db, targets: list[dict], reason: str) -> dict:
    """Delete the ticked outdated variants in ONE ``delete_variant`` call:
    one tombstone, one trash batch, one Ctrl Z."""
    from scidb.inspect.mutate import Mutator

    from scistack_gui.services.variant_cards_service import _jsonable
    from scistack_gui.stores import history

    if not targets:
        raise ValueError("Tick at least one variant to clean up.")
    reason = (reason or "").strip() or "clean up outdated variants"
    plan = db.inspect.delete_plan(targets)
    result = Mutator(db).delete_variant(targets, reason, expect_fingerprint=plan.fingerprint)
    if result.extra.get("undoable"):
        history.note_trash(result.extra["tombstone_id"])
    logger.info("[data_view] cleanup_outdated: %d target(s), %d record(s)", len(targets), plan.total_records)
    return _jsonable(result)


def set_between_keys(db, keys: list[str]) -> dict:
    """The Schema Keys dialog's between boxes (scidb owns the setting)."""
    from scidb.store.between_keys import between_keys, set_between_keys as _set

    changed = _set(db, list(keys), source="gui")
    return {"between": between_keys(db), "changed": changed}
