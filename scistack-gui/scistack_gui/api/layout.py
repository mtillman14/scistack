"""
GET  /api/layout          — return saved node positions
PUT  /api/layout/{node_id} — persist a single node's position (and optionally
                             register it as a manually-placed node)
"""

from fastapi import APIRouter
from pydantic import BaseModel
from scistack_gui import layout as layout_store

class ConstantCreate(BaseModel):
    name: str


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


@router.delete("/edges/{edge_id}")
def delete_edge(edge_id: str):
    layout_store.delete_manual_edge(edge_id)
    return {"ok": True}
