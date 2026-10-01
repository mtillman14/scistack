"""
The Variants popup's backend: a thin shell over scidb.

``.claude/plan-variants-popup.md`` Stage 4; the rules are in
``docs/claude/variant-pins-and-deletion.md``. Every answer comes from scidb:

* cards, from ``Inspector.variant_cards``, the same object
  ``scidb variants X --cards`` prints;
* pins, from ``Mutator.pin_variant`` / ``release_pin`` and
  ``scidb.variant_pins.pin_newest``;
* deletion, from ``Inspector.delete_plan`` / ``Mutator.delete_variant``.

Nothing about variants is decided here (CLAUDE.md NOTE 3/4). What this module
adds is GUI-layer knowledge scidb cannot have:

* **Parameter offers.** For each constant on a card, it says whether the
  canvas Parameter that fed it (scidb names it, ``UpstreamStep.
  parameter_names``) still declares that value, and whether its source is
  editable. Those are facts about source files, which the GUI owns
  (``registry`` and ``target_file_service.entity_editability``).
* **Source edits.** Removing a value from a Parameter goes through the existing
  ``layout_service.update_parameter`` path, never a new write.
* **Run conflicts.** Which pinned variables a GUI run of these canvas nodes
  would write to (``execution_service.derive_target_for_node``).

**Not a subprocess**: shelling out to ``scidb variants --cards --json`` would open
a second connection to a single-writer DuckDB file, the write-lock contention
the MATLAB run-ownership work removed
(``docs/claude/matlab-run-database-ownership.md``). Parity with the CLI is kept by
sharing the Inspector instead.
"""

from __future__ import annotations

import dataclasses
import json
import logging

logger = logging.getLogger("scistack_gui.variant_cards")


def _jsonable(obj):
    """A dataclass tree as plain JSON. Selection values are kept as stored
    (ints stay ints), and anything exotic becomes its string.

    Dataclasses are converted at ANY depth, including inside a list
    (``pin_history`` is a list of ``VariantPin``). Converting only the top
    level let ``json.dumps(default=str)`` turn each pin into its repr string.
    """

    def default(value):
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return dataclasses.asdict(value)
        return str(value)

    return json.loads(json.dumps(obj, default=default))


def _same_value(a, b) -> bool:
    from scidb.bindings import variant_signature

    if variant_signature({"v": a}) == variant_signature({"v": b}):
        return True
    return str(a) == str(b)


# ---------------------------------------------------------------------------
# Read: the cards
# ---------------------------------------------------------------------------


def parameter_offers(card) -> list[dict]:
    """``[{parameter, value, label, declared, editable, message}]``: the
    constants on ``card`` that a canvas Parameter fed. The delete dialog's
    "also remove <value> from <Parameter>" checkboxes are built from these.

    ``declared`` says whether the Parameter still lists the value: there is no
    point offering to remove a value that is no longer there. ``editable`` comes
    from the one confinement owner, so a Parameter declared outside the
    entities file is offered greyed out, with the reason, rather than failing
    at write time.
    """
    from scidb.variant import is_code_or_run_pin

    from scistack_gui import registry
    from scistack_gui.services.target_file_service import entity_editability

    steps = {s.function_name: s for s in card.upstream.steps}
    params = registry.get_parameters_registry()
    offers: list[dict] = []
    seen: set = set()
    for key, value in card.selection.items():
        if is_code_or_run_pin(key) or "." not in key:
            continue
        fn, _, arg = key.rpartition(".")
        step = steps.get(fn)
        if step is None:
            continue  # e.g. a direct save's __save__ kwarg: no Parameter fed it
        name = step.parameter_names.get(arg)
        if not name or (name, json.dumps(value, default=str)) in seen:
            continue
        seen.add((name, json.dumps(value, default=str)))
        declared_values = params[name].values if name in params else []
        declared = any(_same_value(value, v) for v in declared_values)
        edit = entity_editability("parameter", name) if name in params else {
            "editable": False,
            "message": f"No Parameter named '{name}' is on the canvas.",
        }
        offers.append(
            {
                "parameter": name,
                "value": value,
                "label": f"{name}={value}",
                "declared": declared,
                "editable": bool(edit.get("editable")),
                "message": edit.get("message") or "",
            }
        )
    return offers


def variable_variants(db, variable: str) -> dict:
    """``Inspector.variant_cards(variable)`` as JSON, plus each card's
    Parameter offers, the pin history and the deletion history."""
    from scistack_gui.db import db_connection

    with db_connection("variable_variants"):
        result = db.inspect.variant_cards(variable)
        history = db.inspect.variant_pins(variable, history=True)
        stones = db.inspect.tombstones(variable)

    payload = _jsonable(result)
    for card_json, card in zip(payload["cards"], result.cards):
        card_json["parameter_offers"] = _jsonable(parameter_offers(card))
    payload["pin_history"] = _jsonable(history)
    # Record ids are for the database, not the popup; counts are what it shows.
    payload["tombstones"] = [
        {k: v for k, v in _jsonable(t).items() if k != "record_ids"} for t in stones
    ]
    logger.info(
        "[variants] %s: %d card(s), varying=%s, pin=%s, default from %s, "
        "%d pin row(s), %d tombstone(s)",
        variable,
        len(result.cards),
        result.varying_axes or "none",
        result.pin.pin_id if result.pin else None,
        result.default_sources or "none",
        len(history),
        len(stones),
    )
    return payload


# ---------------------------------------------------------------------------
# Write: pins
# ---------------------------------------------------------------------------


def pin_variant(db, variable: str, selection: dict, reason: str) -> dict:
    from scidb.inspect.mutate import Mutator

    result = Mutator(db).pin_variant(variable, selection, reason)
    return _jsonable(result)


def release_pin(db, variable: str, reason: str) -> dict:
    from scidb.inspect.mutate import Mutator

    return _jsonable(Mutator(db).release_pin(variable, reason))


def pin_newest_variant(db, variable: str, reason: str) -> dict:
    """The before-run dialog's "Move pin to the new output", once the run has
    finished (``scidb.variant_pins.pin_newest``)."""
    from scidb.variant_pins import pin_newest

    pin = pin_newest(db, variable, reason)
    return {"ok": True, "pin": _jsonable(pin)}


# ---------------------------------------------------------------------------
# Write: delete
# ---------------------------------------------------------------------------


def _delete_targets(variable: str, card_id: str, remove_parameter_values) -> list[dict]:
    targets = [{"variable": variable, "card_id": card_id}]
    for item in remove_parameter_values or ():
        targets.append({"parameter": item["parameter"], "value": item["value"]})
    return targets


def delete_variant_plan(
    db, variable: str, card_id: str, remove_parameter_values=None
) -> dict:
    """The confirmation dialog's contents: ``Inspector.delete_plan`` for the
    card, widened by any "also remove from the Parameter" choices. Record ids
    are dropped from the reply (counts are what is shown); the fingerprint is
    kept, so the confirm step can refuse if the database changed."""
    from scistack_gui.db import db_connection

    targets = _delete_targets(variable, card_id, remove_parameter_values)
    with db_connection("delete_variant_plan"):
        plan = db.inspect.delete_plan(targets)
    payload = _jsonable(plan)
    payload.pop("record_ids", None)
    payload.pop("invocation_ids", None)
    payload.pop("run_ids", None)
    payload["total_records"] = plan.total_records
    payload["invocation_count"] = len(plan.invocation_ids)
    payload["run_count"] = len(plan.run_ids)
    return payload


def delete_variant(
    db,
    variable: str,
    card_id: str,
    reason: str,
    fingerprint: str,
    remove_parameter_values=None,
) -> dict:
    """Delete the card (and, if asked, every variant built with the removed
    Parameter values), then take those values out of the Parameters' source.

    The database delete happens first and is all-or-nothing, refused if the
    plan's ``fingerprint`` changed. The source edits come after, through the
    existing Parameter-edit path. A refused edit (a read-only source, say) is
    reported in ``parameter_edits`` rather than undoing a delete that already
    happened, which could not be undone anyway.
    """
    from scidb.inspect.mutate import Mutator

    from scistack_gui import registry
    from scistack_gui.services.layout_service import update_parameter

    targets = _delete_targets(variable, card_id, remove_parameter_values)
    result = Mutator(db).delete_variant(targets, reason, expect_fingerprint=fingerprint)

    edits: list[dict] = []
    params = registry.get_parameters_registry()
    for item in remove_parameter_values or ():
        name, value = item["parameter"], item["value"]
        current = params[name].values if name in params else None
        if current is None:
            edits.append({"parameter": name, "value": value, "ok": False, "reason": "unknown"})
            continue
        remaining = [v for v in current if not _same_value(v, value)]
        if len(remaining) == len(current):
            edits.append({"parameter": name, "value": value, "ok": True, "reason": "already_absent"})
            continue
        outcome = update_parameter(name, remaining)
        edits.append({"parameter": name, "value": value, **outcome})
        logger.info(
            "[variants] removed %s=%r from the Parameter's source: %s",
            name, value, outcome,
        )
    payload = _jsonable(result)
    payload["parameter_edits"] = _jsonable(edits)
    return payload


# ---------------------------------------------------------------------------
# Run: does this run write to a pinned variable? (D6)
# ---------------------------------------------------------------------------


def run_pin_conflicts(
    db, node_ids: list[str], function_names: list[str] | None = None
) -> dict:
    """``{"conflicts": [{node_id, function_name, variable, pin}]}`` for the
    canvas function nodes (a node's Run button) or the plan steps (a pipeline
    run, which names steps by function) about to run. The frontend asks before
    starting a run and shows the Keep pin / Move pin to the new output / Cancel
    dialog when this is non-empty."""
    from scidb.variant_pins import active_pins

    from scistack_gui.db import db_connection
    from scistack_gui.services.execution_service import (
        derive_fn_targets,
        derive_target_for_node,
    )

    sources = [("node", n) for n in node_ids or ()] + [
        ("function", f) for f in function_names or ()
    ]
    with db_connection("run_pin_conflicts"):
        pins = active_pins(db)
        conflicts: list[dict] = []
        if pins:
            seen: set = set()
            for kind, ref in sources:
                targets = (
                    derive_target_for_node(db, ref)
                    if kind == "node"
                    else derive_fn_targets(db, ref)
                )
                outputs = {t.get("output_type") for t in targets if t.get("output_type")}
                for variable in sorted(outputs):
                    if variable in pins and (ref, variable) not in seen:
                        seen.add((ref, variable))
                        conflicts.append(
                            {
                                "node_id": ref if kind == "node" else None,
                                "function_name": ref if kind == "function" else None,
                                "variable": variable,
                                "pin": _jsonable(pins[variable]),
                            }
                        )
    logger.info(
        "[variants] run_pin_conflicts(%d source(s)): %d active pin(s), conflicts=%s",
        len(sources),
        len(pins),
        [c["variable"] for c in conflicts] or "none",
    )
    return {"conflicts": conflicts}


__all__ = [
    "variable_variants",
    "parameter_offers",
    "pin_variant",
    "release_pin",
    "pin_newest_variant",
    "delete_variant_plan",
    "delete_variant",
    "run_pin_conflicts",
]
