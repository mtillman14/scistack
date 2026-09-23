"""
Provenance and location trees — the handler table for both transports
(``api/handlers.py``).

    POST /api/provenance/variable             variable_provenance
    POST /api/provenance/variable-topologies  variable_topologies
    POST /api/provenance/node-location-tree   node_location_tree

``variable_topologies`` belongs here rather than in a module of its own: it
answers the same question area from the other direction (bottom-up — what
variants exist and which are still live — against ``variable_provenance``'s
top-down "where did this pinned variant come from"), over a service of the
same shell shape.

Both take the connection inside the service for exactly as long as the
queries need it (``holds_db_lock=False``): the provenance walk is O(depth)
queries and the panel is opened while a user is reading; the node location
tree can spend seconds in ``location_states`` per input variable. Neither
may hold the file against MATLAB for the whole round trip.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scidb.exceptions import NotFoundError

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


class VariableTopologiesRequest(BaseModel):
    variable: str
    #: How many schema locations to sample per variant. ``None`` means every
    #: one — the panel's "show all locations", ``scidb variants --locations``
    #: at the prompt.
    max_locations: int | None = None
    #: The panel sends nothing on first open and ``all_locations=True`` when
    #: the user expands a row. Kept separate from ``max_locations`` so the
    #: default lives in ONE place (``variants_service.DEFAULT_MAX_LOCATIONS``)
    #: rather than being restated by every caller.
    all_locations: bool | None = False


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


def _variable_topologies(db, req: VariableTopologiesRequest) -> dict:
    from scistack_gui.services import variants_service

    if req.all_locations:
        limit = None
    elif req.max_locations is not None:
        limit = req.max_locations
    else:
        limit = variants_service.DEFAULT_MAX_LOCATIONS
    return variants_service.variable_topologies(
        db, req.variable, max_locations=limit
    )


def _node_location_tree(db, req: NodeLocationTreeRequest) -> dict:
    from scistack_gui.services.node_location_service import node_location_tree

    return node_location_tree(db, req.node_id, problems_only=bool(req.problems_only))


PROVENANCE_HANDLERS: tuple[Handler, ...] = (
    Handler("variable_provenance", "/provenance/variable", VariableProvenanceRequest, _variable_provenance, holds_db_lock=False, http_errors={KeyError: 400}),
    # NotFoundError -> 400: "that is not a variable" is a typo in the box, not
    # a server fault, and the panel renders it in place.
    Handler("variable_topologies", "/provenance/variable-topologies", VariableTopologiesRequest, _variable_topologies, holds_db_lock=False, http_errors={KeyError: 400, NotFoundError: 400}),
    Handler("node_location_tree", "/provenance/node-location-tree", NodeLocationTreeRequest, _node_location_tree, holds_db_lock=False),
)

install_routes(router, PROVENANCE_HANDLERS)
