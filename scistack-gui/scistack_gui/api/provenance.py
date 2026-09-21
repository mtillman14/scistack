"""
Provenance and location trees — the handler table for both transports
(``api/handlers.py``).

    POST /api/provenance/variable             variable_provenance
    POST /api/provenance/node-location-tree   node_location_tree

Both take the connection inside the service for exactly as long as the
queries need it (``holds_db_lock=False``): the provenance walk is O(depth)
queries and the panel is opened while a user is reading; the node location
tree can spend seconds in ``location_states`` per input variable. Neither
may hold the file against MATLAB for the whole round trip.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


class VariableProvenanceRequest(BaseModel):
    variable: str
    #: The picker's column-keyed selection (``{"Code:grSides": "v2"}``) — sent
    #: as the panel holds it; scidb canonicalizes it into ``__code__.grSides``.
    selection: dict | None = None
    #: Schema keys narrowing to one location. Omitted, the pin is traced at its
    #: most recently saved match and the rest are listed.
    schema_keys: dict | None = None
    include_runs: bool | None = True


class NodeLocationTreeRequest(BaseModel):
    node_id: str
    problems_only: bool | None = False


def _variable_provenance(db, req: VariableProvenanceRequest) -> dict:
    from scistack_gui.services import provenance_service

    return provenance_service.variable_provenance(
        db,
        req.variable,
        selection=req.selection or None,
        schema=req.schema_keys or None,
        include_runs=True if req.include_runs is None else req.include_runs,
    )


def _node_location_tree(db, req: NodeLocationTreeRequest) -> dict:
    from scistack_gui.services.node_location_service import node_location_tree

    return node_location_tree(db, req.node_id, problems_only=bool(req.problems_only))


PROVENANCE_HANDLERS: tuple[Handler, ...] = (
    Handler("variable_provenance", "/provenance/variable", VariableProvenanceRequest, _variable_provenance, holds_db_lock=False, http_errors={KeyError: 400}),
    Handler("node_location_tree", "/provenance/node-location-tree", NodeLocationTreeRequest, _node_location_tree, holds_db_lock=False),
)

install_routes(router, PROVENANCE_HANDLERS)
