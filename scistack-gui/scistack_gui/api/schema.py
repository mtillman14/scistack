"""
Database metadata — the handler table for both transports (``api/handlers.py``).

    GET  /api/info              get_info        — metadata about the open database (header)
    GET  /api/schema            get_schema      — schema keys and every key's distinct values
    GET  /api/variables         get_variables_list
    POST /api/inventory         schema_inventory        — missing schema-key combinations
    POST /api/inventory/export  export_schema_inventory — the same table as CSV
    POST /api/inventory/row-override  inventory_row_override — what decides one row
    POST /api/inventory/exclude       exclude_schema_combo   — exclude one row
    POST /api/inventory/include       include_schema_combo   — re-include one row
    GET  /api/schema/key-types          get_schema_key_types   — declared numeric/string types
    POST /api/schema/key-types/impact   schema_key_type_impact — what a change would re-spell
    PUT  /api/schema/key-types/{key}    set_schema_key_type    — the Numeric checkbox

The inventory statuses, the two views ("missing only" / "all") and the CSV
layout are owned by ``scidb.inventory``; these handlers only cap the rows the
popup is sent (docs/claude/data-inventory.md). A row becomes schema keys in
``scidb.inventory.row_keyset`` and the exclusion rules are
``scidb.exclusions``; the row handlers only wire them together.
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
    #: Fold uniform blocks into one row with "(all)" (scidb.inventory).
    collapse: bool = True
    limit: int = INVENTORY_ROW_LIMIT


class ExportInventory(BaseModel):
    keys: list[str] | None = None
    within: list[str] | None = None
    missing_only: bool = True
    #: Fold uniform blocks into one row with "(all)" (scidb.inventory).
    collapse: bool = True
    out_path: str


def _schema_inventory(db, req: InventoryRequest) -> dict:
    """The popup's table: at most ``limit`` rows, plus the full counts.

    A size-guard refusal is a ValueError and reaches the popup as an RPC error.
    """
    from scidb.inventory import schema_inventory

    inv = schema_inventory(db, req.keys, req.within)
    out = inv.to_dict(
        missing_only=req.missing_only, limit=max(0, req.limit), collapse=req.collapse
    )
    logger.info(
        "[schema] schema_inventory keys=%s within=%s strata=%d missing_only=%s collapse=%s -> %d/%d row(s) sent%s",
        out["keys"], out["within"], len(out["strata"]), req.missing_only, req.collapse, len(out["rows"]), out["total_rows"],
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
    n = write_inventory_csv(
        inv, out, missing_only=req.missing_only, collapse=req.collapse
    )
    logger.info(
        "[schema] export_schema_inventory collapse=%s -> %s (%d row(s))", req.collapse, out, n
    )
    return {"path": str(out), "rows": n}


class InventoryRow(BaseModel):
    #: One view's header and row, exactly as ``schema_inventory`` sent them;
    #: ``scidb.inventory.row_keyset`` turns them into schema keys.
    columns: list[str]
    row: list[str]


class InventoryRowChange(InventoryRow):
    reason: str


def _inventory_row_override(db, req: InventoryRow) -> dict:
    """The keys a row stands for and the override that decides it (the
    popup's confirm panel)."""
    from scidb.exclusions import governing_overrides
    from scidb.inventory import row_keyset

    keys = row_keyset(req.columns, req.row)
    (governing,) = governing_overrides([keys], db.dataset_schema_keys, db)
    effective = "excluded" if governing is not None and not governing.included else "included"
    logger.info(
        "[schema] inventory_row_override keys=%s -> %s (governed by %s)",
        keys, effective, governing.keys if governing else None,
    )
    return {
        "keys": keys,
        "effective": effective,
        "governed_by": governing.to_dict() if governing else None,
    }


def _change_row(db, req: InventoryRowChange, *, exclude: bool) -> dict:
    from scidb.exclusions import exclude_schema, include_schema
    from scidb.inventory import row_keyset

    reason = req.reason.strip()
    if not reason:
        raise ValueError("A reason is required.")
    keys = row_keyset(req.columns, req.row)
    (exclude_schema if exclude else include_schema)(reason, db=db, **keys)
    logger.info(
        "[schema] %s keys=%s reason=%r",
        "exclude_schema_combo" if exclude else "include_schema_combo", keys, reason,
    )
    return {"keys": keys}


def _exclude_schema_combo(db, req: InventoryRowChange) -> dict:
    """Exclude the combination(s) one inventory row stands for."""
    return _change_row(db, req, exclude=True)


def _include_schema_combo(db, req: InventoryRowChange) -> dict:
    """Re-include the combination(s) one inventory row stands for."""
    return _change_row(db, req, exclude=False)


class SchemaKeyTypeQuery(BaseModel):
    key: str
    #: "numeric" | "string", or None to clear the declaration.
    key_type: str | None = None


class SchemaKeyTypeUpdate(BaseModel):
    key: str
    key_type: str | None = None
    #: Accept a change that re-spells stored values (the confirm dialog).
    confirm_respelling: bool = False


def _get_schema_key_types(db) -> dict:
    """Every schema key's declared type (stored in the database)."""
    from scistack_gui.services.schema_key_type_service import get_schema_key_types

    return get_schema_key_types(db)


def _schema_key_type_impact(db, req: SchemaKeyTypeQuery) -> dict:
    from scistack_gui.services.schema_key_type_service import schema_key_type_impact

    return schema_key_type_impact(db, req.key, req.key_type)


def _set_schema_key_type(db, req: SchemaKeyTypeUpdate) -> dict:
    from scistack_gui.services.schema_key_type_service import set_schema_key_type

    return set_schema_key_type(db, req.key, req.key_type, req.confirm_respelling)


_BAD_REQUEST = {ValueError: 400}

SCHEMA_HANDLERS: tuple[Handler, ...] = (
    Handler("get_schema_key_types", "/schema/key-types", None, _get_schema_key_types, http_method="GET"),
    Handler("schema_key_type_impact", "/schema/key-types/impact", SchemaKeyTypeQuery, _schema_key_type_impact, http_errors=_BAD_REQUEST, undoable=False),
    # Not undoable: a database write, like every other DB-owned fact. Unticking
    # the checkbox is the way back.
    Handler("set_schema_key_type", "/schema/key-types/{key}", SchemaKeyTypeUpdate, _set_schema_key_type, http_method="PUT", http_errors=_BAD_REQUEST, undoable=False),
    Handler("get_info", "/info", None, _get_info, needs_db=False, http_method="GET"),
    Handler("get_schema", "/schema", None, _get_schema, http_method="GET"),
    Handler("get_variables_list", "/variables", None, _get_variables_list, needs_db=False, http_method="GET"),
    Handler("schema_inventory", "/inventory", InventoryRequest, _schema_inventory, http_errors=_BAD_REQUEST, undoable=False),
    Handler("export_schema_inventory", "/inventory/export", ExportInventory, _export_schema_inventory, http_errors=_BAD_REQUEST, undoable=False),
    Handler("inventory_row_override", "/inventory/row-override", InventoryRow, _inventory_row_override, http_errors=_BAD_REQUEST, undoable=False),
    # Not undoable: database writes (the overrides table is its own audit
    # trail; Include/Exclude on the same row is the way back). dag_updated so
    # the canvas re-reads run state.
    Handler("exclude_schema_combo", "/inventory/exclude", InventoryRowChange, _exclude_schema_combo, http_errors=_BAD_REQUEST, undoable=False, notify_dag_updated=True),
    Handler("include_schema_combo", "/inventory/include", InventoryRowChange, _include_schema_combo, http_errors=_BAD_REQUEST, undoable=False, notify_dag_updated=True),
)

install_routes(router, SCHEMA_HANDLERS)
