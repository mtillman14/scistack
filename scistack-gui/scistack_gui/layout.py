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
        return {"positions": {}, "manual_nodes": {}}
    with p.open() as f:
        raw = json.load(f)
    # Migrate legacy flat format: { "node_id": {"x":..,"y":..}, ... }
    if raw and "positions" not in raw:
        return {"positions": raw, "manual_nodes": {}}
    raw.setdefault("positions", {})
    raw.setdefault("manual_nodes", {})
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
