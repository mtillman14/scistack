"""
Node position and pipeline structure persistence.

Positions (x/y) are stored in a JSON file alongside the .duckdb file:
  experiment.duckdb  →  experiment.layout.json

Manual pipeline nodes and edges are stored in the DuckDB database via
pipeline_store.  The JSON file retains only positions and the migration
sentinel; all structural pipeline data lives in DuckDB.

JSON format (post-migration):
{
  "positions": { "node_id": { "x": float, "y": float }, ... },
  "pipeline_db_migrated": true
}
"""

import json
from pathlib import Path
from scistack_gui.db import get_db_path, get_db
from scistack_gui import pipeline_store


def _layout_path() -> Path:
    return get_db_path().with_suffix('.layout.json')


def _load() -> dict:
    """Load and normalise the layout file (positions only, post-migration)."""
    p = _layout_path()
    if not p.exists():
        return {"positions": {}, "constants": []}
    with p.open() as f:
        raw = json.load(f)
    # Migrate legacy flat format: { "node_id": {"x":..,"y":..}, ... }
    if raw and "positions" not in raw:
        return {"positions": raw, "constants": []}
    raw.setdefault("positions", {})
    raw.setdefault("constants", [])
    return raw


def _save(data: dict) -> None:
    p = _layout_path()
    with p.open("w") as f:
        json.dump(data, f, indent=2)


def read_layout() -> dict:
    """Return the full layout dict (positions + manual nodes/edges from DB)."""
    data = _load()
    db = get_db()
    return {
        "positions": data["positions"],
        "manual_nodes": pipeline_store.get_manual_nodes(db),
        "manual_edges": pipeline_store.get_manual_edges(db),
    }


def write_node_position(node_id: str, x: float, y: float) -> None:
    data = _load()
    data["positions"][node_id] = {"x": x, "y": y}
    _save(data)


def write_manual_node(node_id: str, x: float, y: float,
                      node_type: str, label: str) -> None:
    # Position goes to JSON; structural info goes to DB.
    data = _load()
    data["positions"][node_id] = {"x": x, "y": y}
    _save(data)
    pipeline_store.write_manual_node(get_db(), node_id, node_type, label)


def get_manual_nodes() -> dict[str, dict]:
    return pipeline_store.get_manual_nodes(get_db())


def delete_node(node_id: str) -> None:
    """Remove a node's position (JSON) and manual-node entry (DB)."""
    data = _load()
    data["positions"].pop(node_id, None)
    _save(data)
    pipeline_store.delete_node(get_db(), node_id)


def read_constants() -> list[str]:
    return _load()["constants"]


def read_all_constant_names() -> list[str]:
    """All constant names visible in the palette or already on the canvas.

    Sources (unioned):
    - ``constants[]``: palette items created via the "+" button.
    - manual constant nodes in DB (type ``constantNode``).
    - Canonical DB-derived constant IDs in positions (``const__name``).
    """
    data = _load()
    names: set[str] = set(data["constants"])
    manual_nodes = pipeline_store.get_manual_nodes(get_db())
    # Manually dragged constant nodes — label is the true constant name.
    for meta in manual_nodes.values():
        if meta.get("type") == "constantNode":
            names.add(meta["label"])
    # Canonical DB-derived constant nodes not already covered by manual_nodes.
    for node_id in data["positions"]:
        if node_id.startswith("const__") and node_id not in manual_nodes:
            names.add(node_id[len("const__"):])
    return sorted(names)


def write_constant(name: str) -> None:
    data = _load()
    if name not in data["constants"]:
        data["constants"].append(name)
    _save(data)


def delete_constant(name: str) -> None:
    data = _load()
    data["constants"] = [c for c in data["constants"] if c != name]
    _save(data)


def read_manual_edges() -> list[dict]:
    return pipeline_store.get_manual_edges(get_db())


def write_manual_edge(edge: dict) -> None:
    pipeline_store.write_manual_edge(get_db(), edge)


def delete_manual_edge(edge_id: str) -> None:
    pipeline_store.delete_manual_edge(get_db(), edge_id)


def add_pending_constant(const_name: str, value: str) -> None:
    pipeline_store.add_pending_constant(get_db(), const_name, value)


def remove_pending_constant(const_name: str, value: str) -> None:
    pipeline_store.remove_pending_constant(get_db(), const_name, value)


def get_pending_constants() -> dict[str, set[str]]:
    return pipeline_store.get_pending_constants(get_db())


def graduate_manual_node(old_id: str, new_id: str) -> None:
    """Transfer position from a manual node to a DB-derived node ID and remove the manual entry."""
    data = _load()
    old_pos = data["positions"].get(old_id)
    if old_pos and new_id not in data["positions"]:
        data["positions"][new_id] = old_pos
    data["positions"].pop(old_id, None)
    _save(data)
    pipeline_store.graduate_manual_node(get_db(), old_id, new_id)
