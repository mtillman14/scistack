"""
Database metadata — the handler table for both transports (``api/handlers.py``).

    GET  /api/info              get_info        — metadata about the open database (header)
    GET  /api/schema            get_schema      — schema keys and every key's distinct values
    GET  /api/variables         get_variables_list
    POST /api/inventory         schema_inventory        — missing schema-key combinations
    POST /api/inventory/export  export_schema_inventory — the same table as CSV

The inventory statuses, the two views ("missing only" / "all") and the CSV
layout are owned by ``scidb.inventory``; these handlers only cap the rows the
popup is sent (docs/claude/data-inventory.md).
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()

#: Rows the popup is sent at most; the CSV export always writes them all.
INVENTORY_ROW_LIMIT = 5000


def _get_info() -> dict:
    """Metadata about the open database (used by the frontend header)."""
    from scistack_gui.services.pipeline_service import get_info

    return get_info()


def _get_schema(db) -> dict:
    """Schema keys and all distinct values for each key."""
    from scistack_gui.services.pipeline_service import get_schema

    return get_schema(db)


def _get_variables_list() -> list:
    """All registered variable type names."""
    from scistack_gui.services.pipeline_service import get_variables_list

    return get_variables_list()


class InventoryRequest(BaseModel):
    #: Schema keys to cross; None = every schema key not in ``within``.
    keys: list[str] | None = None
    #: Schema keys to run the inventory within (one stratum per combination).
    within: list[str] | None = None
    missing_only: bool = True
    limit: int = INVENTORY_ROW_LIMIT


class ExportInventory(BaseModel):
    keys: list[str] | None = None
    within: list[str] | None = None
    missing_only: bool = True
    out_path: str


def _schema_inventory(db, req: InventoryRequest) -> dict:
    """The popup's table: at most ``limit`` rows, plus the full counts.

    A size-guard refusal is a ValueError and reaches the popup as an RPC error.
    """
    from scidb.inventory import schema_inventory

    inv = schema_inventory(db, req.keys, req.within)
    out = inv.to_dict(missing_only=req.missing_only, limit=max(0, req.limit))
    logger.info(
        "[schema] schema_inventory keys=%s within=%s strata=%d missing_only=%s -> %d/%d row(s) sent%s",
        out["keys"], out["within"], len(out["strata"]), req.missing_only, len(out["rows"]), out["total_rows"],
        " (truncated)" if out["truncated"] else "",
    )
    return out


def _export_schema_inventory(db, req: ExportInventory) -> dict:
    """Write the whole inventory (never truncated) as CSV to ``out_path``."""
    from scidb.inventory import schema_inventory, write_inventory_csv

    out = Path(req.out_path).expanduser()
    if not out.is_absolute():
        raise ValueError(f"export path must be absolute, got {req.out_path!r}")
    inv = schema_inventory(db, req.keys, req.within)
    n = write_inventory_csv(inv, out, missing_only=req.missing_only)
    logger.info("[schema] export_schema_inventory -> %s (%d row(s))", out, n)
    return {"path": str(out), "rows": n}


_BAD_REQUEST = {ValueError: 400}

SCHEMA_HANDLERS: tuple[Handler, ...] = (
    Handler("get_info", "/info", None, _get_info, needs_db=False, http_method="GET"),
    Handler("get_schema", "/schema", None, _get_schema, http_method="GET"),
    Handler("get_variables_list", "/variables", None, _get_variables_list, needs_db=False, http_method="GET"),
    Handler("schema_inventory", "/inventory", InventoryRequest, _schema_inventory, http_errors=_BAD_REQUEST, undoable=False),
    Handler("export_schema_inventory", "/inventory/export", ExportInventory, _export_schema_inventory, http_errors=_BAD_REQUEST, undoable=False),
)

install_routes(router, SCHEMA_HANDLERS)
