"""
DuckDB-backed store for manually-declared pipeline nodes and edges.

Replaces the manual_nodes / manual_edges sections of the JSON layout file so
that DuckDB is the single source of truth for pipeline structure.  Node
positions (x/y) remain in the JSON file as cosmetic data only.

Tables created in the user's .duckdb file:

    _pipeline_nodes (node_id, node_type, label)
    _pipeline_edges (edge_id, source, target, source_handle, target_handle)

These tables are created lazily on first access so they are always present
regardless of whether init_db() or configure_database() was used to open the DB.

Migration
---------
On first access (detected by the migration sentinel key in the JSON layout),
any manual_nodes and manual_edges entries in the JSON are written to the DB
and removed from the JSON.  This is a one-time, idempotent operation.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_MIGRATION_SENTINEL = "pipeline_db_migrated"


def _duck(db):
    """Return the SciDuck instance from a DatabaseManager."""
    return db._duck


def _ensure_tables(db) -> None:
    """Create pipeline tables if they don't already exist."""
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_nodes (
            node_id   VARCHAR PRIMARY KEY,
            node_type VARCHAR NOT NULL,
            label     VARCHAR NOT NULL
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_edges (
            edge_id       VARCHAR PRIMARY KEY,
            source        VARCHAR NOT NULL,
            target        VARCHAR NOT NULL,
            source_handle VARCHAR,
            target_handle VARCHAR
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_pending_constants (
            constant_name VARCHAR NOT NULL,
            value         VARCHAR NOT NULL,
            PRIMARY KEY (constant_name, value)
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_nodes (
            node_id VARCHAR PRIMARY KEY
        )
    """)
    # Add config column if missing (migration for existing DBs).
    try:
        _duck(db)._execute(
            "ALTER TABLE _pipeline_nodes ADD COLUMN config VARCHAR DEFAULT '{}'"
        )
    except Exception:
        pass  # Column already exists


def migrate_from_json(db, layout_path: Path) -> None:
    """One-time migration: move manual_nodes/manual_edges from JSON into DB.

    Safe to call repeatedly — checks the migration sentinel before acting.
    """
    logger.info("[pipeline_store] Step 1: migrate_from_json called (layout_path=%s)", layout_path)
    _ensure_tables(db)

    if not layout_path.exists():
        logger.debug("[pipeline_store] Layout file does not exist, skipping migration")
        return

    logger.info("[pipeline_store] Step 2: Loading layout JSON file")
    with layout_path.open() as f:
        try:
            data = json.load(f)
        except Exception:
            logger.debug("[pipeline_store] Failed to parse JSON, skipping migration")
            return

    if data.get(_MIGRATION_SENTINEL):
        logger.debug("[pipeline_store] Migration already completed (sentinel found), skipping")
        return  # Already migrated.

    logger.info("[pipeline_store] Step 3: Migrating manual_nodes and manual_edges to DuckDB")
    manual_nodes: dict = data.get("manual_nodes", {})
    manual_edges: list = data.get("manual_edges", [])
    logger.debug("[pipeline_store] Found %d manual nodes and %d manual edges in JSON",
                 len(manual_nodes), len(manual_edges))

    migrated_nodes = 0
    for node_id, meta in manual_nodes.items():
        node_type = meta.get("type", "")
        label = meta.get("label", "")
        if node_type and label:
            _upsert_node(db, node_id, node_type, label)
            migrated_nodes += 1

    migrated_edges = 0
    for edge in manual_edges:
        edge_id = edge.get("id", "")
        if edge_id:
            _upsert_edge(db, edge_id, edge.get("source", ""), edge.get("target", ""),
                         edge.get("sourceHandle"), edge.get("targetHandle"))
            migrated_edges += 1

    logger.info("[pipeline_store] Step 4: Writing migration sentinel to JSON and removing migrated data")
    # Clear migrated keys from JSON and write sentinel.
    data.pop("manual_nodes", None)
    data.pop("manual_edges", None)
    data[_MIGRATION_SENTINEL] = True
    with layout_path.open("w") as f:
        json.dump(data, f, indent=2)

    logger.info(
        "[pipeline_store] Step 5: Migration complete - migrated %d nodes and %d edges from JSON to DuckDB",
        migrated_nodes, migrated_edges,
    )


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def get_manual_nodes(db) -> dict[str, dict]:
    """Return {node_id: {"type": ..., "label": ..., "config": ...}} for all manual nodes."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT node_id, node_type, label, config FROM _pipeline_nodes"
    )
    result = {}
    for row in rows:
        entry: dict = {"type": row[1], "label": row[2]}
        if row[3] and row[3] != '{}':
            try:
                entry["config"] = json.loads(row[3])
            except (json.JSONDecodeError, TypeError):
                pass
        result[row[0]] = entry
    return result


def write_manual_node(db, node_id: str, node_type: str, label: str) -> None:
    logger.info("[pipeline_store] Step 1: write_manual_node called (node_id=%r, type=%r, label=%r)",
                node_id, node_type, label)
    _ensure_tables(db)
    logger.info("[pipeline_store] Step 2: Upserting node into _pipeline_nodes table")
    _upsert_node(db, node_id, node_type, label)
    logger.info("[pipeline_store] Step 3: Node written to DuckDB successfully")


def update_node_config(db, node_id: str, config: dict) -> None:
    """Update just the config JSON for an existing node."""
    _ensure_tables(db)
    _duck(db)._execute(
        "UPDATE _pipeline_nodes SET config = ? WHERE node_id = ?",
        [json.dumps(config), node_id]
    )


def delete_node(db, node_id: str) -> None:
    logger.info("[pipeline_store] Step 1: delete_node called (node_id=%r)", node_id)
    _duck(db)._execute(
        "DELETE FROM _pipeline_nodes WHERE node_id = ?", [node_id]
    )
    logger.info("[pipeline_store] Step 2: Node deleted from _pipeline_nodes table")


def graduate_manual_node(db, old_id: str, new_id: str) -> None:
    """Remove the manual node entry for old_id (the DB-derived node takes over).

    Also rewrites any manual edges that reference old_id so they point to
    new_id instead of becoming dangling.
    """
    _duck(db)._execute(
        "DELETE FROM _pipeline_nodes WHERE node_id = ?", [old_id]
    )
    # Update edges that reference the old node ID.
    _duck(db)._execute(
        "UPDATE _pipeline_edges SET source = ? WHERE source = ?",
        [new_id, old_id],
    )
    _duck(db)._execute(
        "UPDATE _pipeline_edges SET target = ? WHERE target = ?",
        [new_id, old_id],
    )


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------

def get_manual_edges(db) -> list[dict]:
    """Return all manual edges as a list of dicts."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT edge_id, source, target, source_handle, target_handle "
        "FROM _pipeline_edges"
    )
    result = []
    for edge_id, source, target, source_handle, target_handle in rows:
        entry: dict = {"id": edge_id, "source": source, "target": target}
        if source_handle is not None:
            entry["sourceHandle"] = source_handle
        if target_handle is not None:
            entry["targetHandle"] = target_handle
        result.append(entry)
    return result


def write_manual_edge(db, edge: dict) -> None:
    logger.info("[pipeline_store] Step 1: write_manual_edge called (edge_id=%r, source=%r, target=%r, source_handle=%r, target_handle=%r)",
                edge.get("id"), edge.get("source"), edge.get("target"),
                edge.get("sourceHandle") or edge.get("source_handle"),
                edge.get("targetHandle") or edge.get("target_handle"))
    _ensure_tables(db)
    logger.info("[pipeline_store] Step 2: Upserting edge into _pipeline_edges table")
    _upsert_edge(
        db,
        edge["id"],
        edge.get("source", ""),
        edge.get("target", ""),
        edge.get("sourceHandle") or edge.get("source_handle"),
        edge.get("targetHandle") or edge.get("target_handle"),
    )
    logger.info("[pipeline_store] Step 3: Edge written to DuckDB successfully")


def delete_manual_edge(db, edge_id: str) -> None:
    logger.info("[pipeline_store] Step 1: delete_manual_edge called (edge_id=%r)", edge_id)
    _duck(db)._execute(
        "DELETE FROM _pipeline_edges WHERE edge_id = ?", [edge_id]
    )
    logger.info("[pipeline_store] Step 2: Edge deleted from _pipeline_edges table")


# ---------------------------------------------------------------------------
# Pending constants
# ---------------------------------------------------------------------------

def add_pending_constant(db, const_name: str, value: str) -> None:
    logger.info("[pipeline_store] Step 1: add_pending_constant called (const_name=%r, value=%r)",
                const_name, value)
    _ensure_tables(db)
    logger.info("[pipeline_store] Step 2: Inserting pending constant into _pipeline_pending_constants table")
    _duck(db)._execute(
        "INSERT INTO _pipeline_pending_constants (constant_name, value) VALUES (?, ?) "
        "ON CONFLICT DO NOTHING",
        [const_name, value],
    )
    logger.info("[pipeline_store] Step 3: Pending constant added successfully")


def remove_pending_constant(db, const_name: str, value: str) -> None:
    logger.info("[pipeline_store] Step 1: remove_pending_constant called (const_name=%r, value=%r)",
                const_name, value)
    _duck(db)._execute(
        "DELETE FROM _pipeline_pending_constants WHERE constant_name = ? AND value = ?",
        [const_name, value],
    )
    logger.info("[pipeline_store] Step 2: Pending constant removed from _pipeline_pending_constants table")


def get_pending_constants(db) -> dict[str, set[str]]:
    """Return {constant_name: {value, ...}} for all pending constant values."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT constant_name, value FROM _pipeline_pending_constants"
    )
    result: dict[str, set[str]] = {}
    for const_name, value in rows:
        result.setdefault(const_name, set()).add(value)
    return result


# ---------------------------------------------------------------------------
# Hidden nodes (user-deleted DB-derived nodes)
# ---------------------------------------------------------------------------

def hide_node(db, node_id: str) -> None:
    """Mark a DB-derived node as hidden so _build_graph won't recreate it."""
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _pipeline_hidden_nodes (node_id) VALUES (?) "
        "ON CONFLICT DO NOTHING",
        [node_id],
    )


def unhide_node(db, node_id: str) -> None:
    """Remove a node from the hidden list (e.g. when user re-adds it)."""
    _duck(db)._execute(
        "DELETE FROM _pipeline_hidden_nodes WHERE node_id = ?", [node_id]
    )


def unhide_nodes_by_prefix(db, prefix: str) -> None:
    """Remove all hidden nodes whose IDs start with ``prefix``.

    Used when a user re-adds a function node by label: composite DB-derived
    IDs (``fn__{label}__{call_id}``) don't match a single canonical ID, so
    we unhide every call-site node sharing the prefix.
    """
    _duck(db)._execute(
        "DELETE FROM _pipeline_hidden_nodes WHERE node_id LIKE ?",
        [prefix + "%"],
    )


def get_hidden_node_ids(db) -> set[str]:
    """Return the set of node IDs that the user has explicitly deleted."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT node_id FROM _pipeline_hidden_nodes"
    )
    return {row[0] for row in rows}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _upsert_node(db, node_id: str, node_type: str, label: str) -> None:
    _duck(db)._execute(
        "INSERT INTO _pipeline_nodes (node_id, node_type, label) VALUES (?, ?, ?) "
        "ON CONFLICT (node_id) DO UPDATE SET node_type = excluded.node_type, "
        "label = excluded.label",
        [node_id, node_type, label],
    )


def _upsert_edge(db, edge_id: str, source: str, target: str,
                 source_handle, target_handle) -> None:
    _duck(db)._execute(
        "INSERT INTO _pipeline_edges "
        "(edge_id, source, target, source_handle, target_handle) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (edge_id) DO UPDATE SET source = excluded.source, "
        "target = excluded.target, source_handle = excluded.source_handle, "
        "target_handle = excluded.target_handle",
        [edge_id, source, target, source_handle, target_handle],
    )
