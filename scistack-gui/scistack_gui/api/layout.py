"""
GET  /api/layout          — return saved node positions
PUT  /api/layout/{node_id} — persist a single node's position (and optionally
                             register it as a manually-placed node)
"""

import logging
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from scidb.database import DatabaseManager
from scistack_gui import layout as layout_store, pipeline_store
from scistack_gui.db import get_db

logger = logging.getLogger(__name__)

class ConstantCreate(BaseModel):
    name: str


class PathInputCreate(BaseModel):
    name: str
    template: str = ""
    root_folder: str | None = None


class EdgeCreate(BaseModel):
    source: str
    target: str
    source_handle: str | None = None
    target_handle: str | None = None

router = APIRouter()


class PositionUpdate(BaseModel):
    x: float
    y: float
    # Present only when the node was just dragged from the sidebar palette.
    node_type: str | None = None
    label: str | None = None


class NodeConfigUpdate(BaseModel):
    config: dict


@router.get("/layout")
def get_layout() -> dict:
    return layout_store.read_layout()


@router.put("/layout/{node_id}")
def put_layout(node_id: str, body: PositionUpdate):
    if body.node_type and body.label:
        layout_store.write_manual_node(node_id, body.x, body.y,
                                       body.node_type, body.label)
    else:
        layout_store.write_node_position(node_id, body.x, body.y)
    return {"ok": True}


@router.delete("/layout/{node_id}")
def delete_layout(node_id: str):
    layout_store.delete_node(node_id)
    return {"ok": True}


@router.get("/constants")
def get_constants() -> list[str]:
    return layout_store.read_all_constant_names()


@router.post("/constants")
def post_constant(body: ConstantCreate):
    layout_store.write_constant(body.name)
    return {"ok": True}


@router.delete("/constants/{name}")
def delete_constant(name: str):
    layout_store.delete_constant(name)
    return {"ok": True}


@router.get("/path-inputs")
def get_path_inputs() -> list[dict]:
    result = layout_store.read_all_path_input_names()
    logger.info("GET /path-inputs → %s", result)
    return result


@router.post("/path-inputs")
def post_path_input(body: PathInputCreate):
    logger.info("POST /path-inputs name=%r template=%r root_folder=%r",
                body.name, body.template, body.root_folder)
    layout_store.write_path_input(body.name, body.template, body.root_folder)
    return {"ok": True}


@router.put("/path-inputs/{name}")
def put_path_input(name: str, body: PathInputCreate):
    layout_store.write_path_input(name, body.template, body.root_folder)
    return {"ok": True}


@router.delete("/path-inputs/{name}")
def delete_path_input(name: str):
    layout_store.delete_path_input(name)
    return {"ok": True}


@router.put("/edges/{edge_id}")
def put_edge(edge_id: str, body: EdgeCreate):
    layout_store.write_manual_edge({
        "id": edge_id,
        "source": body.source,
        "target": body.target,
        "sourceHandle": body.source_handle,
        "targetHandle": body.target_handle,
    })
    return {"ok": True}


@router.put("/layout/{node_id}/config")
def put_node_config(node_id: str, body: NodeConfigUpdate,
                    db: DatabaseManager = Depends(get_db)):
    pipeline_store.update_node_config(db, node_id, body.config)
    return {"ok": True}


@router.delete("/edges/{edge_id}")
def delete_edge(edge_id: str):
    layout_store.delete_manual_edge(edge_id)
    return {"ok": True}
