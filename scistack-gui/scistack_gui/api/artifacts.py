"""
Endpoint presentation (plan-endpoint-presentation.md): the handler table
for both transports (``api/handlers.py``), plus one HTTP-only route.

    GET  /api/endpoints/{fn_name}/artifacts   get_endpoint_artifacts — finalized figures/stats manifest
    POST /api/report                          write_report — write the endpoint report, return its index.html path
    GET  /api/artifacts/file?path=            (HTTP only) serve one artifact — project-dir guarded;
                                              403 outside, 404 missing. A file response has no
                                              JSON-RPC shape; the extension opens artifacts through
                                              the host instead.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.db import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


class FunctionName(BaseModel):
    fn_name: str


def _get_endpoint_artifacts(db, req: FunctionName) -> dict:
    from scistack_gui.services.endpoint_service import endpoint_artifacts

    return endpoint_artifacts(db, req.fn_name)


def _write_report(db) -> dict:
    from scistack_gui.services.endpoint_service import write_report

    return write_report(db)


ARTIFACT_HANDLERS: tuple[Handler, ...] = (
    Handler("get_endpoint_artifacts", "/endpoints/{fn_name}/artifacts", FunctionName, _get_endpoint_artifacts, http_method="GET"),
    Handler("write_report", "/report", None, _write_report, http_errors={Exception: 500}),
)

install_routes(router, ARTIFACT_HANDLERS)


@router.get("/artifacts/file")
def get_artifact_file(path: str, db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.endpoint_service import artifact_file_path

    try:
        resolved = artifact_file_path(db, path)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return FileResponse(resolved)
