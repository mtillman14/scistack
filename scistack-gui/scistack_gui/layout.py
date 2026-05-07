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
import logging
from pathlib import Path
from scistack_gui.db import get_db_path, get_db
from scistack_gui import pipeline_store

logger = logging.getLogger(__name__)


def _layout_path() -> Path:
    return get_db_path().with_suffix('.layout.json')


def _load() -> dict:
    """Load and normalise the layout file (positions only, post-migration)."""
    p = _layout_path()
    logger.debug("[layout] Loading layout file from %s", p)
    if not p.exists():
        logger.debug("[layout] Layout file does not exist, returning empty structure")
        return {"positions": {}, "constants": [], "path_inputs": []}
    with p.open() as f:
        raw = json.load(f)
    logger.debug("[layout] Loaded layout file with %d top-level keys", len(raw))
    # Migrate legacy flat format: { "node_id": {"x":..,"y":..}, ... }
    if raw and "positions" not in raw:
        logger.debug("[layout] Migrating legacy flat format to nested positions structure")
        return {"positions": raw, "constants": [], "path_inputs": []}
    raw.setdefault("positions", {})
    raw.setdefault("constants", [])
    raw.setdefault("path_inputs", [])
    logger.debug("[layout] Layout has %d positions, %d constants, %d path_inputs",
                 len(raw["positions"]), len(raw["constants"]), len(raw["path_inputs"]))
    return raw


def _save(data: dict) -> None:
    p = _layout_path()
    logger.debug("[layout] Saving layout file to %s", p)
    logger.debug("[layout] Writing %d positions, %d constants, %d path_inputs",
                 len(data.get("positions", {})), len(data.get("constants", [])), len(data.get("path_inputs", [])))
    with p.open("w") as f:
        json.dump(data, f, indent=2)
    logger.debug("[layout] Layout file saved successfully")


def read_layout() -> dict:
    """Return the full layout dict (positions + manual nodes/edges from DB)."""
    data = _load()
    db = get_db()
    return {
        "positions": data["positions"],
        "manual_nodes": pipeline_store.get_manual_nodes(db),
        "manual_edges": pipeline_store.get_manual_edges(db),
        "constants": data.get("constants", []),
    }


def write_node_position(node_id: str, x: float, y: float) -> None:
    logger.info("[layout] Step 1: write_node_position called (node_id=%r, x=%.1f, y=%.1f)", node_id, x, y)
    data = _load()
    logger.info("[layout] Step 2: Writing position to JSON")
    data["positions"][node_id] = {"x": x, "y": y}
    _save(data)
    logger.info("[layout] Step 3: Node position written successfully")


def write_manual_node(node_id: str, x: float, y: float,
                      node_type: str, label: str) -> None:
    # Position goes to JSON; structural info goes to DB.
    logger.info("[layout] Step 1: write_manual_node called (node_id=%r, type=%r, label=%r, x=%.1f, y=%.1f)",
                node_id, node_type, label, x, y)
    logger.info("[layout] Step 2: Writing position to JSON")
    data = _load()
    data["positions"][node_id] = {"x": x, "y": y}
    _save(data)
    logger.info("[layout] Step 3: Writing node metadata to DuckDB")
    db = get_db()
    pipeline_store.write_manual_node(db, node_id, node_type, label)
    # If the user is re-adding a node that was previously hidden, unhide it.
    # Also unhide the canonical DB-derived ID for this type/label.
    logger.info("[layout] Step 4: Unhiding node (in case it was previously deleted)")
    pipeline_store.unhide_node(db, node_id)
    prefix_map = {
        "variableNode": "var__",
        "functionNode": "fn__",
        "constantNode": "const__",
        "pathInputNode": "pathInput__",
    }
    prefix = prefix_map.get(node_type)
    if prefix:
        logger.debug("[layout] Step 5: Unhiding canonical DB-derived nodes for type=%r, label=%r", node_type, label)
        if node_type == "functionNode":
            # DB-derived function nodes use composite ``fn__{label}__{call_id}``
            # IDs — there can be multiple canonical nodes per label.  Unhide
            # every call-site node sharing the label.
            pipeline_store.unhide_nodes_by_prefix(db, f"fn__{label}__")
            # Also unhide the legacy fn__{label} form for older layouts.
            pipeline_store.unhide_node(db, f"fn__{label}")
            logger.debug("[layout] Unhid all function nodes with label=%r", label)
        else:
            canonical_id = f"{prefix}{label}"
            pipeline_store.unhide_node(db, canonical_id)
            logger.debug("[layout] Unhid canonical node %r", canonical_id)
    logger.info("[layout] Step 6: Manual node written successfully")


def get_manual_nodes() -> dict[str, dict]:
    return pipeline_store.get_manual_nodes(get_db())


def delete_node(node_id: str) -> None:
    """Remove a node's position (JSON) and manual-node entry (DB).

    For DB-derived nodes (var__, fn__, const__, pathInput__), also mark them
    as hidden so _build_graph won't recreate them from pipeline history.
    """
    logger.info("[layout] Step 1: delete_node called (node_id=%r)", node_id)
    logger.info("[layout] Step 2: Removing position from JSON")
    data = _load()
    data["positions"].pop(node_id, None)
    _save(data)
    logger.info("[layout] Step 3: Deleting node metadata from DuckDB")
    db = get_db()
    pipeline_store.delete_node(db, node_id)
    # Hide DB-derived nodes so they don't reappear from list_pipeline_variants().
    logger.info("[layout] Step 4: Marking node as hidden (so it won't be auto-recreated)")
    pipeline_store.hide_node(db, node_id)
    logger.info("[layout] Step 5: Node deleted successfully")


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


def read_all_path_input_names() -> list[dict]:
    """All path inputs visible in the palette or already on the canvas.

    Sources (unioned):
    - ``path_inputs[]``: palette items created via the "+" button.
    - Canonical DB-derived pathInput IDs in positions (``pathInput__name``).
    """
    data = _load()
    by_name: dict[str, dict] = {}
    for pi in data["path_inputs"]:
        by_name[pi["name"]] = pi
    for node_id in data["positions"]:
        if node_id.startswith("pathInput__"):
            # Node IDs are "pathInput__<name>__<random>"; extract just <name>.
            parts = node_id.split("__")
            name = parts[1] if len(parts) >= 2 else node_id[len("pathInput__"):]
            if name not in by_name:
                by_name[name] = {"name": name, "template": "", "root_folder": None}
    return sorted(by_name.values(), key=lambda p: p["name"])


def write_path_input(name: str, template: str, root_folder: str | None = None) -> None:
    data = _load()
    # Update existing or append new.
    for pi in data["path_inputs"]:
        if pi["name"] == name:
            pi["template"] = template
            pi["root_folder"] = root_folder
            _save(data)
            return
    data["path_inputs"].append({"name": name, "template": template, "root_folder": root_folder})
    _save(data)


def delete_path_input(name: str) -> None:
    data = _load()
    data["path_inputs"] = [p for p in data["path_inputs"] if p["name"] != name]
    _save(data)


def read_manual_edges() -> list[dict]:
    return pipeline_store.get_manual_edges(get_db())


def write_manual_edge(edge: dict) -> None:
    logger.info("[layout] Step 1: write_manual_edge called (edge_id=%r, source=%r, target=%r)",
                edge.get("id"), edge.get("source"), edge.get("target"))
    pipeline_store.write_manual_edge(get_db(), edge)
    logger.info("[layout] Step 2: Edge written to DuckDB successfully")


def delete_manual_edge(edge_id: str) -> None:
    logger.info("[layout] Step 1: delete_manual_edge called (edge_id=%r)", edge_id)
    pipeline_store.delete_manual_edge(get_db(), edge_id)
    logger.info("[layout] Step 2: Edge deleted from DuckDB successfully")


def add_pending_constant(const_name: str, value: str) -> None:
    logger.info("[layout] Step 1: add_pending_constant called (name=%r, value=%r)", const_name, value)
    pipeline_store.add_pending_constant(get_db(), const_name, value)
    logger.info("[layout] Step 2: Pending constant added to DuckDB successfully")


def remove_pending_constant(const_name: str, value: str) -> None:
    logger.info("[layout] Step 1: remove_pending_constant called (name=%r, value=%r)", const_name, value)
    pipeline_store.remove_pending_constant(get_db(), const_name, value)
    logger.info("[layout] Step 2: Pending constant removed from DuckDB successfully")


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
