"""
The canvas node's location tree: the handler table for both transports
(``api/handlers.py``).

    POST /api/provenance/node-location-tree  node_location_tree

The variant questions this module used to answer (``variable_provenance``,
``variable_topologies``) moved to ``api/variants.py`` with the Variants popup
(``.claude/plan-variants-popup.md`` Stage 5), which replaced both panels.

The service takes the connection itself (``holds_db_lock=False``): the tree
can spend seconds in ``location_states`` per input variable and must not hold
the file against MATLAB for the whole round trip.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


class NodeLocationTreeRequest(BaseModel):
    node_id: str
    problems_only: bool | None = False


def _node_location_tree(db, req: NodeLocationTreeRequest) -> dict:
    from scistack_gui.services.node_location_service import node_location_tree

    return node_location_tree(db, req.node_id, problems_only=bool(req.problems_only))


PROVENANCE_HANDLERS: tuple[Handler, ...] = (
    Handler("node_location_tree", "/provenance/node-location-tree", NodeLocationTreeRequest, _node_location_tree, holds_db_lock=False, undoable=False),
)

install_routes(router, PROVENANCE_HANDLERS)
