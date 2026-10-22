"""
Node positions, the Parameter palette and item notes.

All three live in the project's ONE .duckdb file (plan
``.claude/plan-canvas-as-document.md`` Stage 0a, 2026-10-21):

    _pipeline_node_positions    (pipeline_id, node_id, x, y)
    _pipeline_palette_parameters (name, seq)    -- palette order = seq
    _pipeline_notes             (note_key, text)

They used to be a ``<name>.layout.json`` sidecar. That file is no longer read
or written; positions in an old one are lost once (clean break, user
decision 2026-10-21, ``feedback_beta_no_deprecation``) and those nodes are
laid out again.

Manual pipeline nodes and edges are stored via ``pipeline_store``. This
module keeps its public functions; only the storage underneath changed. Being
DuckDB rows, every write is undoable like any other GUI table
(``UNDOABLE_TABLES``, read by ``history.tracked_tables``).
"""

import logging
import threading
import time
from contextlib import contextmanager

from scistack_gui import pipeline_store
from scistack_gui import library_lock as _library_lock
from scistack_gui.ids import ROOT_SCOPE
from scistack_gui.db import get_db

logger = logging.getLogger(__name__)

POSITIONS_TABLE = "_pipeline_node_positions"
PALETTE_TABLE = "_pipeline_palette_parameters"
NOTES_TABLE = "_pipeline_notes"

#: Every table this module creates, for undo (`scistack_gui.history`).
UNDOABLE_TABLES: tuple[str, ...] = (POSITIONS_TABLE, PALETTE_TABLE, NOTES_TABLE)

#: How each table travels on export (see ``pipeline_store.PORTABILITY``).
#: Positions are captured per node by ``services.canvas_snapshot``; notes and
#: the palette are project-wide.
PORTABILITY: dict[str, str] = {
    POSITIONS_TABLE: "canvas",
    PALETTE_TABLE: "global",
    NOTES_TABLE: "global",
}

# Serializes this module's multi-statement mutations (move, rebase,
# graduate: read rows, then write rows) within one process. Each statement is
# already atomic in DuckDB; the lock keeps two such sequences from
# interleaving. server.py runs one thread per RPC, and a single node drop
# sends TWO put_layout calls ~1ms apart (the create, then the re-centre), see
# .claude/plan-layout-write-race-and-duplicate-seed-roots.md. Reentrant
# because write_manual_node and delete_node call other writers inside it.
_layout_lock = threading.RLock()

# Contention is expected and harmless; only a wait long enough to be worth
# explaining gets promoted out of debug.
_CONTENTION_LOG_THRESHOLD = 0.05


@contextmanager
def _layout_write(operation: str):
    """Hold :data:`_layout_lock` for one multi-statement mutation."""
    start = time.monotonic()
    _layout_lock.acquire()
    waited = time.monotonic() - start
    try:
        if waited > _CONTENTION_LOG_THRESHOLD:
            logger.info(
                "[layout] %s thread=%d waited=%.4fs for the layout lock",
                operation,
                threading.get_ident(),
                waited,
            )
        else:
            logger.debug(
                "[layout] %s thread=%d waited=%.4fs",
                operation,
                threading.get_ident(),
                waited,
            )
        yield
    finally:
        _layout_lock.release()


def ensure_tables(db) -> None:
    """Create this module's tables if absent. Called from
    ``pipeline_store._ensure_tables``, so every path that touches the GUI
    database gets them, like ``_intent`` and ``_node_wiring``."""
    duck = db._duck
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_node_positions (
            pipeline_id VARCHAR NOT NULL,
            node_id     VARCHAR NOT NULL,
            x           DOUBLE NOT NULL,
            y           DOUBLE NOT NULL,
            PRIMARY KEY (pipeline_id, node_id)
        )
    """)
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_palette_parameters (
            name VARCHAR PRIMARY KEY,
            seq  INTEGER NOT NULL
        )
    """)
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_notes (
            note_key VARCHAR PRIMARY KEY,
            text     VARCHAR NOT NULL
        )
    """)


def _duck():
    db = get_db()
    pipeline_store._ensure_tables(db)
    return db._duck


# ---------------------------------------------------------------------------
# Position rows
# ---------------------------------------------------------------------------


def _upsert_position(duck, pipeline_id: str, node_id: str, x: float, y: float) -> None:
    duck._execute(
        f"INSERT INTO {POSITIONS_TABLE} (pipeline_id, node_id, x, y) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (pipeline_id, node_id) DO UPDATE SET x = excluded.x, y = excluded.y",
        [pipeline_id, node_id, float(x), float(y)],
    )


def _delete_position(duck, pipeline_id: str, node_id: str) -> None:
    duck._execute(
        f"DELETE FROM {POSITIONS_TABLE} WHERE pipeline_id = ? AND node_id = ?",
        [pipeline_id, node_id],
    )


def _positions_by_scope(duck) -> dict:
    out: dict = {}
    for pid, nid, x, y in duck._fetchall(
        f"SELECT pipeline_id, node_id, x, y FROM {POSITIONS_TABLE} "
        "ORDER BY pipeline_id, node_id"
    ):
        out.setdefault(pid, {})[nid] = {"x": float(x), "y": float(y)}
    return out


def read_positions_by_scope() -> dict:
    """All saved positions, scope-keyed: {pipeline_id: {node_id: {x, y}}}.

    The scope a DB-derived node's position lives in IS its scope membership
    (see domain.scope_filter.node_scope), so graph filtering reads this.
    """
    positions = _positions_by_scope(_duck())
    logger.debug(
        "[layout] read_positions_by_scope: %d scope(s), %d position(s)",
        len(positions),
        sum(len(v) for v in positions.values()),
    )
    return positions


def drop_scope_positions(pipeline_id: str) -> None:
    """Remove a deleted scope's positions (scope teardown)."""
    _duck()._execute(f"DELETE FROM {POSITIONS_TABLE} WHERE pipeline_id = ?", [pipeline_id])
    logger.info("[layout] drop_scope_positions: scope %r", pipeline_id)


def drop_node_positions(node_id: str) -> None:
    """Remove one node's position from every scope (position only — no
    manual-node/hide side effects, unlike delete_node)."""
    _duck()._execute(f"DELETE FROM {POSITIONS_TABLE} WHERE node_id = ?", [node_id])


def move_node_position(
    node_id: str,
    new_pipeline_id: str,
    default_x: float = 0.0,
    default_y: float = 0.0,
    new_node_id: str | None = None,
) -> dict:
    """Move one node's saved position into a new scope (extract-to-submodule).

    Position IS the scope-membership record for DB-derived nodes (see
    domain.scope_filter.node_scope), so this alone re-scopes them; manual
    nodes additionally need their ``_pipeline_nodes.pipeline_id`` column
    rewritten (pipeline_store.move_node_scope), which takes priority when
    both exist. Returns the position that was moved (or the default, if the
    node had no saved position in any scope).

    ``node_id`` is placement-qualified for an already-graduated DB-derived
    node (``{canonical}::{old_scope}`` — see domain.graph_builder.
    placement_id) — its embedded scope would go STALE if the position were
    simply re-written under the same key in a new scope bucket (node_scope
    trusts that embedded suffix over which bucket holds it). Callers moving
    such a node must pass ``new_node_id`` (the re-keyed placement id for
    the destination scope); it defaults to ``node_id`` for manual nodes,
    which carry no scope in their id at all.
    """
    with _layout_write("move_node_position"):
        duck = _duck()
        row = duck._fetchone(
            f"SELECT pipeline_id, x, y FROM {POSITIONS_TABLE} WHERE node_id = ? "
            "ORDER BY pipeline_id LIMIT 1",
            [node_id],
        )
        if row is not None:
            _delete_position(duck, row[0], node_id)
            pos = {"x": float(row[1]), "y": float(row[2])}
        else:
            pos = {"x": default_x, "y": default_y}
        node_id = new_node_id or node_id
        _upsert_position(duck, new_pipeline_id, node_id, pos["x"], pos["y"])
    logger.info(
        "[layout] move_node_position: %r -> scope %r (%.1f, %.1f)",
        node_id, new_pipeline_id, pos["x"], pos["y"],
    )
    return pos


def read_layout(pipeline_id: str = ROOT_SCOPE) -> dict:
    """Return one SCOPE's layout (positions + manual nodes from DB).

    Defaults to the root scope. ``manual_edges`` is intentionally unfiltered
    here: DB-derived nodes' scope membership is graph_builder's business
    (service layer filters edges when composing a scoped canvas).
    """
    duck = _duck()
    db = get_db()
    return {
        "pipeline_id": pipeline_id,
        "positions": dict(_positions_by_scope(duck).get(pipeline_id, {})),
        "manual_nodes": pipeline_store.get_manual_nodes(db, pipeline_id),
        "manual_edges": pipeline_store.get_manual_edges(db),
        "constants": read_constants(),
    }


def write_node_position(
    node_id: str, x: float, y: float, pipeline_id: str = ROOT_SCOPE
) -> None:
    logger.info(
        "[layout] write_node_position (node_id=%r, x=%.1f, y=%.1f, scope=%r)",
        node_id,
        x,
        y,
        pipeline_id,
    )
    _upsert_position(_duck(), pipeline_id, node_id, x, y)


def write_manual_node(
    node_id: str,
    x: float,
    y: float,
    node_type: str,
    label: str,
    pipeline_id: str = ROOT_SCOPE,
) -> None:
    # The structural row goes FIRST, and that order is load-bearing: it is
    # what makes the node exist, while the position is cosmetic. A node at a
    # default position is recoverable; a node that was never created is not
    # (the user re-dropped the same node three times in one session when the
    # position write ran first and failed, 2026-09-01).
    logger.info(
        "[layout] write_manual_node called (node_id=%r, type=%r, label=%r, x=%.1f, y=%.1f, scope=%r)",
        node_id,
        node_type,
        label,
        x,
        y,
        pipeline_id,
    )
    db = get_db()
    pipeline_store.write_manual_node(db, node_id, node_type, label, pipeline_id)
    # If the user is re-adding a node that was previously hidden, unhide it
    # — scoped to THIS pipeline_id only, so re-adding a node here doesn't
    # resurrect another hypothesis pipeline's independent placement of the
    # same shared wiring (see plan-scope-hidden-nodes-edges.md).
    # Also unhide the canonical DB-derived ID for this type/label.
    logger.info(
        "[layout] Unhiding node in scope=%r (in case it was previously deleted)",
        pipeline_id,
    )
    pipeline_store.unhide_node(db, node_id, pipeline_id)
    from scistack_gui.ids import (
        NODE_TYPE_PREFIXES,
        fn_nodes_prefix,
        legacy_fn_node_id,
    )

    prefix = NODE_TYPE_PREFIXES.get(node_type)
    if prefix:
        logger.debug(
            "[layout] Unhiding canonical DB-derived nodes for type=%r, label=%r",
            node_type,
            label,
        )
        if node_type == "functionNode":
            # DB-derived function nodes use composite ``fn__{label}__{call_id}``
            # IDs — there can be multiple canonical nodes per label.  Unhide
            # every call-site node sharing the label.
            pipeline_store.unhide_nodes_by_prefix(db, fn_nodes_prefix(label), pipeline_id)
            # Also unhide the legacy fn__{label} form for older layouts.
            pipeline_store.unhide_node(db, legacy_fn_node_id(label), pipeline_id)
            logger.debug("[layout] Unhid all function nodes with label=%r", label)
        else:
            canonical_id = f"{prefix}{label}"
            pipeline_store.unhide_node(db, canonical_id, pipeline_id)
            logger.debug("[layout] Unhid canonical node %r", canonical_id)
    _upsert_position(_duck(), pipeline_id, node_id, x, y)
    logger.info("[layout] Manual node written successfully")


def delete_node(node_id: str) -> None:
    """Remove a node's position and manual-node entry.

    For DB-derived nodes (var__, fn__, param__, pathInput__), also mark them
    as hidden so _build_graph won't recreate them from pipeline history.
    Hiding is scoped to the SCOPE ``node_id`` currently belongs to (resolved
    via domain.scope_filter.node_scope, the same "what scope is this node
    in" logic every other consumer trusts) — not global — so a delete in
    one pipeline never hides another pipeline's independent placement of
    the same shared wiring (graph_builder.wiring_id is scope-independent by
    design; see plan-scope-hidden-nodes-edges.md). The id is stripped to
    the bare canonical id before being stored/matched, since
    domain.graph_builder.filter_hidden checks bare prefixes.
    """
    from scistack_gui.ids import strip_placement
    from scistack_gui.domain.scope_filter import node_scope

    logger.info("[layout] delete_node called (node_id=%r)", node_id)
    db = get_db()
    # Refuse BEFORE the position is removed: a refused delete inside a
    # library pipeline must leave the node exactly where it was.
    from scistack_gui import library_lock

    library_lock.check_node(db, node_id, "removing a node")
    # Resolve scope BEFORE removing the position — node_scope's fallback
    # for a bare (not placement-qualified) id scans saved positions.
    manual_nodes = pipeline_store.get_manual_nodes(db)
    positions_by_scope = read_positions_by_scope()
    scope_id = node_scope(node_id, manual_nodes, positions_by_scope)
    drop_node_positions(node_id)
    # The stored row is KEPT (canvas-as-document, 2026-10-22; never delete,
    # mark hidden). Since adoption, a node from a run is a stored row too, and
    # the `_adopted` marker stops it from being adopted again; deleting the
    # row as well would leave a hidden node that could never be unhidden. The
    # build skips hidden rows (api/pipeline._build_graph).
    bare_id = strip_placement(node_id)
    logger.info(
        "[layout] Marking node as hidden in scope=%r (so it won't be auto-recreated there)",
        scope_id,
    )
    pipeline_store.hide_node(db, bare_id, scope_id)
    logger.info("[layout] Node deleted successfully")


# ---------------------------------------------------------------------------
# The Parameter palette
# ---------------------------------------------------------------------------


def read_constants() -> list[str]:
    """Palette Parameter names, in the order they were added."""
    return [n for (n,) in _duck()._fetchall(f"SELECT name FROM {PALETTE_TABLE} ORDER BY seq")]


def read_all_constant_names() -> list[str]:
    """All Parameter names visible in the palette or already on the canvas.

    Sources (unioned):
    - the palette: items created via the "+" button.
    - manual Parameter nodes in DB (type ``parameterNode``).
    - Canonical DB-derived Parameter IDs in positions (``param__name``,
      possibly placement-qualified as ``param__name::{pipeline_id}``).
    """
    from scistack_gui.ids import PARAM_ID_PREFIX, strip_placement

    names: set[str] = set(read_constants())
    manual_nodes = pipeline_store.get_manual_nodes(get_db())
    # Manually dragged Parameter nodes — label is the true parameter name.
    for meta in manual_nodes.values():
        if meta.get("type") == "parameterNode":
            names.add(meta["label"])
    # Canonical DB-derived Parameter nodes not already covered by manual_nodes.
    for scope in read_positions_by_scope().values():
        for node_id in scope:
            bare_id = strip_placement(node_id)
            if bare_id.startswith(PARAM_ID_PREFIX) and node_id not in manual_nodes:
                names.add(bare_id[len(PARAM_ID_PREFIX) :])
    return sorted(names)


def write_constant(name: str) -> None:
    duck = _duck()
    duck._execute(
        f"INSERT INTO {PALETTE_TABLE} (name, seq) "
        f"SELECT ?, COALESCE(MAX(seq), 0) + 1 FROM {PALETTE_TABLE} "
        "ON CONFLICT (name) DO NOTHING",
        [name],
    )


def delete_parameter_from_palette(name: str) -> None:
    """Remove a name from the palette list. Distinct from
    layout_service.delete_parameter, which hides the NODE."""
    _duck()._execute(f"DELETE FROM {PALETTE_TABLE} WHERE name = ?", [name])


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------


def read_notes() -> dict[str, str]:
    return {k: t for k, t in _duck()._fetchall(f"SELECT note_key, text FROM {NOTES_TABLE}")}


def write_note(key: str, text: str) -> None:
    """Persist (or clear) one item's free-text note.

    ``key`` is ``"{kind}:{name}"`` (see api/layout.py's ``PUT /notes/{key}``)
    — e.g. ``"variable:Position"`` or ``"submodule:pipe_abc123"`` (submodules
    key by pipeline_id, which survives renames; every other kind keys by its
    registered name). An empty/whitespace-only ``text`` removes the entry
    entirely, so the table doesn't accumulate empty-string notes.
    """
    logger.info("[layout] write_note called (key=%r, len(text)=%d)", key, len(text))
    duck = _duck()
    if text.strip():
        duck._execute(
            f"INSERT INTO {NOTES_TABLE} (note_key, text) VALUES (?, ?) "
            "ON CONFLICT (note_key) DO UPDATE SET text = excluded.text",
            [key, text],
        )
    else:
        duck._execute(f"DELETE FROM {NOTES_TABLE} WHERE note_key = ?", [key])


# ---------------------------------------------------------------------------
# Id changes
# ---------------------------------------------------------------------------


def rebase_node_positions(
    old_bare: str,
    new_bare: str,
    note_keys: "tuple[str, str] | None" = None,
) -> int:
    """Move the saved position of EVERY placement of *old_bare* (bare and
    ``::scope``-qualified) onto the same placement of *new_bare* — the
    layout half of a canonical id changing (a rename; the node-row half is
    ``pipeline_store.rebase_node``). ``note_keys=(old, new)`` also moves
    that item's note, which is keyed by name (see :func:`write_note`).
    Returns how many positions moved."""
    from scistack_gui.intent_store import _rebase

    moved = 0
    note_moved = False
    with _layout_write("rebase_node_positions"):
        duck = _duck()
        for pid, scope in _positions_by_scope(duck).items():
            for nid, pos in scope.items():
                new_id = _rebase(nid, old_bare, new_bare)
                if new_id is None:
                    continue
                _delete_position(duck, pid, nid)
                if new_id not in scope:
                    _upsert_position(duck, pid, new_id, pos["x"], pos["y"])
                moved += 1
        if note_keys is not None:
            old_key, new_key = note_keys
            notes = read_notes()
            if old_key in notes:
                duck._execute(f"DELETE FROM {NOTES_TABLE} WHERE note_key = ?", [old_key])
                if new_key not in notes:
                    write_note(new_key, notes[old_key])
                note_moved = True
    logger.info(
        "[layout] rebase_node_positions %s -> %s: %d position(s), note moved=%s",
        old_bare, new_bare, moved, note_moved,
    )
    return moved


@_library_lock.internal
def graduate_manual_node(old_id: str, new_id: str, take_old_position: bool = False) -> None:
    """Transfer position from a manual node to a DB-derived node ID and
    remove the manual entry. Scope-aware: the new id stays on whichever
    canvas the old node was placed on.

    ``take_old_position``: when the target already has a position in a scope,
    replace it with the manual node's (``GraduationAction.take_old_position``,
    the entity-node case). Default False keeps the target's position.
    """
    with _layout_write("graduate_manual_node"):
        duck = _duck()
        for pid, scope in _positions_by_scope(duck).items():
            old_pos = scope.get(old_id)
            if old_pos and (new_id not in scope or take_old_position):
                if new_id in scope:
                    logger.info(
                        "[layout] graduate_manual_node %s -> %s: replacing the "
                        "target's position %s with the hand-placed %s",
                        old_id, new_id, scope[new_id], old_pos,
                    )
                _upsert_position(duck, pid, new_id, old_pos["x"], old_pos["y"])
            if old_id in scope:
                _delete_position(duck, pid, old_id)
    pipeline_store.graduate_manual_node(get_db(), old_id, new_id)
