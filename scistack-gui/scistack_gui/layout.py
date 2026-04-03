"""
Node position persistence.

Positions are stored in a JSON file alongside the .duckdb file:
  experiment.duckdb  →  experiment.layout.json

Format:
{
  "positions": { "node_id": { "x": float, "y": float }, ... },
  "manual_nodes": {
    "node_id": { "type": "functionNode"|"variableNode", "label": str },
    ...
  }
}

The legacy flat format (just positions at the top level) is read and migrated
automatically on first access.
"""

import json
from pathlib import Path
from scistack_gui.db import get_db_path


def _layout_path() -> Path:
    return get_db_path().with_suffix('.layout.json')


def _load() -> dict:
    """Load and normalise the layout file to the current format."""
    p = _layout_path()
    if not p.exists():
        return {"positions": {}, "manual_nodes": {}, "constants": []}
    with p.open() as f:
        raw = json.load(f)
    # Migrate legacy flat format: { "node_id": {"x":..,"y":..}, ... }
    if raw and "positions" not in raw:
        return {"positions": raw, "manual_nodes": {}, "constants": []}
    raw.setdefault("positions", {})
    raw.setdefault("manual_nodes", {})
    raw.setdefault("constants", [])
    return raw


def _save(data: dict) -> None:
    p = _layout_path()
    with p.open("w") as f:
        json.dump(data, f, indent=2)


def read_layout() -> dict:
    """Return the full layout dict (positions + manual_nodes)."""
    return _load()


def write_node_position(node_id: str, x: float, y: float) -> None:
    data = _load()
    data["positions"][node_id] = {"x": x, "y": y}
    _save(data)


def write_manual_node(node_id: str, x: float, y: float,
                      node_type: str, label: str) -> None:
    data = _load()
    data["positions"][node_id] = {"x": x, "y": y}
    data["manual_nodes"][node_id] = {"type": node_type, "label": label}
    _save(data)


def get_manual_nodes() -> dict[str, dict]:
    return _load()["manual_nodes"]


def delete_node(node_id: str) -> None:
    """Remove a node's position and manual-node entry (if any) from the layout file."""
    data = _load()
    data["positions"].pop(node_id, None)
    data["manual_nodes"].pop(node_id, None)
    _save(data)


def read_constants() -> list[str]:
    return _load()["constants"]


def write_constant(name: str) -> None:
    data = _load()
    if name not in data["constants"]:
        data["constants"].append(name)
    _save(data)


def delete_constant(name: str) -> None:
    data = _load()
    data["constants"] = [c for c in data["constants"] if c != name]
    _save(data)


def graduate_manual_node(old_id: str, new_id: str) -> None:
    """Transfer position from a manual node to a DB-derived node ID and remove the manual entry."""
    data = _load()
    old_pos = data["positions"].get(old_id)
    if old_pos and new_id not in data["positions"]:
        data["positions"][new_id] = old_pos
    data["positions"].pop(old_id, None)
    data["manual_nodes"].pop(old_id, None)
    _save(data)
