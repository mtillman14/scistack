"""
Provenance API (HTTP transport).

Thin wrapper over ``services.provenance_service`` — the JSON-RPC handler in
``server.py`` calls the same function, so the browser GUI and the VS Code
extension cannot diverge.

    POST /api/provenance/variable  — provenance of one pinned variant,
                                     down to the runs that produced it
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui.db import get_db
from scistack_gui.services import provenance_service

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
    include_runs: bool = True


@router.post("/provenance/variable")
def variable_provenance(
    req: VariableProvenanceRequest, db: DatabaseManager = Depends(get_db)
) -> dict:
    try:
        return provenance_service.variable_provenance(
            db,
            req.variable,
            selection=req.selection,
            schema=req.schema_keys,
            include_runs=req.include_runs,
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
