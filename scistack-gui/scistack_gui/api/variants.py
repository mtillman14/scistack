"""
The Variants popup: cards, pins, deletion, run conflicts. This is the handler
table for both transports (``api/handlers.py``).

    POST /api/variants/cards                variable_variants
    POST /api/variants/pin                  pin_variant
    POST /api/variants/release-pin          release_pin
    POST /api/variants/pin-newest           pin_newest_variant
    POST /api/variants/delete-plan          delete_variant_plan
    POST /api/variants/delete               delete_variant
    POST /api/variants/run-pin-conflicts    run_pin_conflicts

Everything is answered by ``services/variant_cards_service.py``, a shell over
``scidb.inspect`` and ``scidb.variant_pins`` / ``variant_delete``
(``.claude/plan-variants-popup.md`` Stage 4).

The reads take the connection inside the service (``holds_db_lock=False``):
cards build one closure per variable, and the popup stays open while a user
reads it. The writes hold it for the call, like every other write. Pins and
deletes broadcast ``dag_updated``, because both change downstream node states.
"""

import logging
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from scidb.exceptions import NotFoundError

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


class VariableRef(BaseModel):
    variable: str


class PinRequest(BaseModel):
    variable: str
    #: A card's ``selection``, forwarded as the card carried it. The GUI never
    #: builds one.
    selection: dict
    reason: str


class ReleaseRequest(BaseModel):
    variable: str
    reason: str


class ParameterValue(BaseModel):
    parameter: str
    value: Any = None


class DeletePlanRequest(BaseModel):
    variable: str
    card_id: str
    #: The "also remove <value> from <Parameter>" choices; each widens the
    #: delete to every variant built with that value.
    remove_parameter_values: list[ParameterValue] = []


class DeleteRequest(DeletePlanRequest):
    reason: str
    #: The plan the user confirmed. The delete is refused if it changed.
    fingerprint: str


class RunConflictRequest(BaseModel):
    #: Canvas function nodes (a node's Run button).
    node_ids: list[str] = []
    #: Plan steps by function name (a pipeline run).
    function_names: list[str] = []


def _values(req: DeletePlanRequest) -> list[dict]:
    return [{"parameter": p.parameter, "value": p.value} for p in req.remove_parameter_values]


def _variable_variants(db, req: VariableRef) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.variable_variants(db, req.variable)


def _pin_variant(db, req: PinRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.pin_variant(db, req.variable, req.selection, req.reason)


def _release_pin(db, req: ReleaseRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.release_pin(db, req.variable, req.reason)


def _pin_newest_variant(db, req: ReleaseRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.pin_newest_variant(db, req.variable, req.reason)


def _delete_variant_plan(db, req: DeletePlanRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.delete_variant_plan(
        db, req.variable, req.card_id, _values(req)
    )


def _delete_variant(db, req: DeleteRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.delete_variant(
        db, req.variable, req.card_id, req.reason, req.fingerprint, _values(req)
    )


def _run_pin_conflicts(db, req: RunConflictRequest) -> dict:
    from scistack_gui.services import variant_cards_service

    return variant_cards_service.run_pin_conflicts(
        db, req.node_ids, req.function_names
    )


#: A refused write (no reason, a selection matching two cards, a changed plan)
#: is the user's to fix, not a server fault: 400, rendered in place.
_USER_ERRORS = {ValueError: 400, NotFoundError: 400, KeyError: 400}

VARIANT_HANDLERS: tuple[Handler, ...] = (
    Handler("variable_variants", "/variants/cards", VariableRef, _variable_variants, holds_db_lock=False, http_errors=_USER_ERRORS),
    Handler("pin_variant", "/variants/pin", PinRequest, _pin_variant, http_errors=_USER_ERRORS, notify_dag_updated=True),
    Handler("release_pin", "/variants/release-pin", ReleaseRequest, _release_pin, http_errors=_USER_ERRORS, notify_dag_updated=True),
    Handler("pin_newest_variant", "/variants/pin-newest", ReleaseRequest, _pin_newest_variant, http_errors=_USER_ERRORS, notify_dag_updated=True),
    Handler("delete_variant_plan", "/variants/delete-plan", DeletePlanRequest, _delete_variant_plan, holds_db_lock=False, http_errors=_USER_ERRORS),
    Handler("delete_variant", "/variants/delete", DeleteRequest, _delete_variant, http_errors=_USER_ERRORS, notify_dag_updated=True),
    Handler("run_pin_conflicts", "/variants/run-pin-conflicts", RunConflictRequest, _run_pin_conflicts, holds_db_lock=False),
)

install_routes(router, VARIANT_HANDLERS)
