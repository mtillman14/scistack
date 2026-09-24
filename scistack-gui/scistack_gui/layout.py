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
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from scistack_gui import pipeline_store
from scistack_gui.ids import ROOT_SCOPE
from scistack_gui.db import get_db, get_db_path

logger = logging.getLogger(__name__)

# Serializes read-modify-write of the layout document. Every mutator below
# loads the whole file, edits it and writes it back, so two overlapping
# mutators mean the later write silently discards the earlier one's edit.
#
# This is NOT a Windows-only problem, though that is where it was first
# caught (a sharing violation, errno 13, on an SMB share). The old _save
# opened the target with "w", which truncates immediately, so on POSIX the
# same overlap instead hands a CONCURRENT READER an empty or half-written
# file: reverting this module and running tests/test_layout_concurrency.py
# raises JSONDecodeError("Expecting value: line 1 column 1") out of _load,
# verified on macOS 2026-09-01. Every platform was exposed; only the symptom
# differed. That is why _load takes this lock too.
#
# This overlap is routine, not exotic: server.py runs one thread per RPC,
# and a single node drop sends TWO put_layout calls ~1ms apart (the create
# from PipelineDAG's onDrop, then the position-only re-center once React
# Flow has measured the node). That collision lost three freshly dropped
# nodes in one two-minute session -- see
# .claude/plan-layout-write-race-and-duplicate-seed-roots.md.
#
# Reentrant because delete_node and write_manual_node call helpers that load
# the document again inside the critical section.
#
# Process-local by decision (2026-09-01): a second GUI backend on the same
# project would still interleave, and the DuckDB lock would not stop it
# either (the GUI drops that lock between requests on purpose, db.py:40-46).
# Not designed around for now; the real fix would be moving positions into
# DuckDB, finishing the migration this module's docstring describes.
_layout_lock = threading.RLock()

# Contention is expected and harmless; only a wait long enough to be worth
# explaining gets promoted out of debug.
_CONTENTION_LOG_THRESHOLD = 0.05

# The layout file lives next to the database, which in real deployments is a
# network share. A sync client, an AV scanner or a stale SMB handle can deny
# access for a moment with no SciStack process involved, so the final rename
# is retried briefly before giving up.
_SAVE_RETRY_ATTEMPTS = 5
_SAVE_RETRY_INTERVAL = 0.05


@contextmanager
def _layout_write(operation: str):
    """Hold :data:`_layout_lock` for one read-modify-write of the file."""
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


def _layout_path() -> Path:
    return get_db_path().with_suffix(".layout.json")


def _load() -> dict:
    """Load and normalise the layout file (positions only).

    Positions are PER-SCOPE (nested pipelines): ``positions`` maps
    ``pipeline_id -> {node_id: {x, y}}``. No older layout shape is upgraded:
    beta, no installed base (migrations removed 2026-09-23).
    """
    p = _layout_path()
    logger.debug("[layout] Loading layout file from %s", p)
    # The read holds the lock too (reentrantly — mutators call this inside
    # their own critical section). Windows refuses to replace a file another
    # handle has open, so an unsynchronized reader would make a concurrent
    # save burn its retries for no reason. Only the file access needs
    # guarding; everything below it is in-memory.
    with _layout_lock:
        if not p.exists():
            # Deliberately NOT a hand-maintained duplicate of the defaults set
            # below (a second "what are the defaults" list next to the
            # setdefault calls has already drifted out of sync once — missing
            # path_inputs/sweeps after their removal, and separately missing
            # "notes" entirely, both silent until a fresh/never-written project
            # hit this branch). Empty dict + fall through to the same
            # normalization every other path goes through.
            logger.debug("[layout] Layout file does not exist, using empty defaults")
            raw = {}
        else:
            with p.open() as f:
                raw = json.load(f)
    logger.debug("[layout] Loaded layout file with %d top-level keys", len(raw))
    raw.setdefault("positions", {})
    raw.setdefault("constants", [])
    raw.setdefault("notes", {})
    logger.debug(
        "[layout] Layout has %d scope(s), %d constants",
        len(raw["positions"]),
        len(raw["constants"]),
    )
    return raw


def _scope_positions(data: dict, pipeline_id: str) -> dict:
    """The (mutable) position dict for one scope, created on demand."""
    return data["positions"].setdefault(pipeline_id, {})


def _positions_all(data: dict) -> dict:
    """Merged {node_id: {x, y}} across all scopes (node ids are unique) —
    for the name-derivation helpers that scan canonical node ids."""
    merged: dict = {}
    for scope in data["positions"].values():
        merged.update(scope)
    return merged


def _replace_with_retry(tmp: Path, target: Path) -> None:
    """``os.replace(tmp, target)``, retrying transient permission denials."""
    last: PermissionError | None = None
    for attempt in range(1, _SAVE_RETRY_ATTEMPTS + 1):
        try:
            os.replace(tmp, target)
            if attempt > 1:
                logger.info(
                    "[layout] save succeeded on attempt %d/%d",
                    attempt,
                    _SAVE_RETRY_ATTEMPTS,
                )
            return
        except PermissionError as exc:
            last = exc
            logger.warning(
                "[layout] save retry %d/%d after %s: %s",
                attempt,
                _SAVE_RETRY_ATTEMPTS,
                type(exc).__name__,
                exc,
            )
            if attempt < _SAVE_RETRY_ATTEMPTS:
                time.sleep(_SAVE_RETRY_INTERVAL)
    logger.error(
        "[layout] save FAILED for %s after %d attempts: %s",
        target,
        _SAVE_RETRY_ATTEMPTS,
        last,
    )
    assert last is not None  # only reachable via the except branch
    raise last


def _save(data: dict) -> None:
    """Write the layout document atomically.

    ``json.dump`` straight into the target truncated it in place, so a write
    that failed partway (or a process killed mid-write) left an unparseable
    layout file -- every position lost, not just the one being written.
    Serialize into a sibling temp file and ``os.replace`` it in, so a reader
    sees either the old document or the new one and never a partial one.

    The temp name carries pid + thread id so concurrent writers never share
    one, whether or not they share :data:`_layout_lock`.
    """
    p = _layout_path()
    logger.debug("[layout] Saving layout file to %s", p)
    logger.debug(
        "[layout] Writing %d positions, %d constants",
        len(data.get("positions", {})),
        len(data.get("constants", [])),
    )
    tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        with tmp.open("w") as f:
            json.dump(data, f, indent=2)
        _replace_with_retry(tmp, p)
    finally:
        # A failed dump or an exhausted retry must not leave the temp behind
        # (a successful replace already moved it, hence missing_ok).
        tmp.unlink(missing_ok=True)
    logger.debug("[layout] Layout file saved successfully")


def read_positions_by_scope() -> dict:
    """All saved positions, scope-keyed: {pipeline_id: {node_id: {x, y}}}.

    The scope a DB-derived node's position lives in IS its scope membership
    (see domain.scope_filter.node_scope), so graph filtering reads this.
    """
    return {k: dict(v) for k, v in _load()["positions"].items()}


def drop_scope_positions(pipeline_id: str) -> None:
    """Remove a deleted scope's position map (scope teardown)."""
    with _layout_write("drop_scope_positions"):
        data = _load()
        if data["positions"].pop(pipeline_id, None) is not None:
            _save(data)


def drop_node_positions(node_id: str) -> None:
    """Remove one node's position from every scope (position only — no
    manual-node/hide side effects, unlike delete_node)."""
    with _layout_write("drop_node_positions"):
        data = _load()
        changed = False
        for scope in data["positions"].values():
            changed = scope.pop(node_id, None) is not None or changed
        if changed:
            _save(data)


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
        data = _load()
        pos = None
        for scope in data["positions"].values():
            found = scope.pop(node_id, None)
            if found is not None:
                pos = found
                break
        node_id = new_node_id or node_id
        if pos is None:
            pos = {"x": default_x, "y": default_y}
        _scope_positions(data, new_pipeline_id)[node_id] = pos
        _save(data)
    logger.info(
        "[layout] move_node_position: %r -> scope %r (%.1f, %.1f)",
        node_id, new_pipeline_id, pos["x"], pos["y"],
    )
    return pos


def read_layout(pipeline_id: str = ROOT_SCOPE) -> dict:
    """Return one SCOPE's layout (positions + manual nodes from DB).

    Defaults to the root scope, which is where every pre-scoping document
    lives — existing callers see exactly what they saw before nesting.
    ``manual_edges`` is intentionally unfiltered here: DB-derived nodes'
    scope membership is graph_builder's business (service layer filters
    edges when composing a scoped canvas).
    """
    data = _load()
    db = get_db()
    return {
        "pipeline_id": pipeline_id,
        "positions": dict(_scope_positions(data, pipeline_id)),
        "manual_nodes": pipeline_store.get_manual_nodes(db, pipeline_id),
        "manual_edges": pipeline_store.get_manual_edges(db),
        "constants": data.get("constants", []),
    }


def write_node_position(
    node_id: str, x: float, y: float, pipeline_id: str = ROOT_SCOPE
) -> None:
    logger.info(
        "[layout] write_node_position called (node_id=%r, x=%.1f, y=%.1f, scope=%r)",
        node_id,
        x,
        y,
        pipeline_id,
    )
    with _layout_write("write_node_position"):
        data = _load()
        logger.info("[layout] Writing position to JSON")
        _scope_positions(data, pipeline_id)[node_id] = {"x": x, "y": y}
        _save(data)
    logger.info("[layout] Node position written successfully")


def write_manual_node(
    node_id: str,
    x: float,
    y: float,
    node_type: str,
    label: str,
    pipeline_id: str = ROOT_SCOPE,
) -> None:
    # Position goes to JSON; structural info goes to DB.
    #
    # The DB write goes FIRST, and that order is load-bearing: the structural
    # row is what makes the node exist, while the position is cosmetic. When
    # the JSON write ran first and raised (a concurrent writer's sharing
    # violation on the share), it took the whole function down before the DB
    # write, so the node was never created at all and vanished on the next
    # DAG refresh -- the user re-dropped the same node three times in one
    # session. A node at a default position is recoverable; a node that was
    # never created is not.
    logger.info(
        "[layout] write_manual_node called (node_id=%r, type=%r, label=%r, x=%.1f, y=%.1f, scope=%r)",
        node_id,
        node_type,
        label,
        x,
        y,
        pipeline_id,
    )
    logger.info("[layout] Writing node metadata to DuckDB")
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
    logger.info("[layout] Writing position to JSON")
    with _layout_write("write_manual_node"):
        data = _load()
        _scope_positions(data, pipeline_id)[node_id] = {"x": x, "y": y}
        _save(data)
    logger.info("[layout] Manual node written successfully")


def delete_node(node_id: str) -> None:
    """Remove a node's position (JSON) and manual-node entry (DB).

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
    # Resolve scope BEFORE removing the position — node_scope's fallback
    # for a bare (not placement-qualified) id scans saved positions.
    manual_nodes = pipeline_store.get_manual_nodes(db)
    positions_by_scope = read_positions_by_scope()
    scope_id = node_scope(node_id, manual_nodes, positions_by_scope)
    logger.info("[layout] Removing position from JSON")
    with _layout_write("delete_node"):
        data = _load()
        for scope in data["positions"].values():
            scope.pop(node_id, None)
        _save(data)
    logger.info("[layout] Deleting node metadata from DuckDB")
    pipeline_store.delete_node(db, node_id)
    # Hide DB-derived nodes so they don't reappear from list_pipeline_variants().
    bare_id = strip_placement(node_id)
    logger.info(
        "[layout] Marking node as hidden in scope=%r (so it won't be auto-recreated there)",
        scope_id,
    )
    pipeline_store.hide_node(db, bare_id, scope_id)
    logger.info("[layout] Node deleted successfully")


def read_constants() -> list[str]:
    return _load()["constants"]


def read_all_constant_names() -> list[str]:
    """All Parameter names visible in the palette or already on the canvas.

    Sources (unioned):
    - ``constants[]``: palette items created via the "+" button.
    - manual Parameter nodes in DB (type ``parameterNode``).
    - Canonical DB-derived Parameter IDs in positions (``param__name``,
      possibly placement-qualified as ``param__name::{pipeline_id}``).
    """
    from scistack_gui.ids import PARAM_ID_PREFIX, strip_placement

    data = _load()
    names: set[str] = set(data["constants"])
    manual_nodes = pipeline_store.get_manual_nodes(get_db())
    # Manually dragged Parameter nodes — label is the true parameter name.
    for meta in manual_nodes.values():
        if meta.get("type") == "parameterNode":
            names.add(meta["label"])
    # Canonical DB-derived Parameter nodes not already covered by manual_nodes.
    for node_id in _positions_all(data):
        bare_id = strip_placement(node_id)
        if bare_id.startswith(PARAM_ID_PREFIX) and node_id not in manual_nodes:
            names.add(bare_id[len(PARAM_ID_PREFIX) :])
    return sorted(names)


def write_constant(name: str) -> None:
    with _layout_write("write_constant"):
        data = _load()
        if name not in data["constants"]:
            data["constants"].append(name)
        _save(data)


def delete_parameter_from_palette(name: str) -> None:
    """Remove a name from the layout.json palette list. Distinct from
    layout_service.delete_parameter, which hides the NODE."""
    with _layout_write("delete_parameter_from_palette"):
        data = _load()
        data["constants"] = [c for c in data["constants"] if c != name]
        _save(data)


def read_notes() -> dict[str, str]:
    return dict(_load()["notes"])


def write_note(key: str, text: str) -> None:
    """Persist (or clear) one item's free-text note.

    ``key`` is ``"{kind}:{name}"`` (see api/layout.py's ``PUT /notes/{key}``)
    — e.g. ``"variable:Position"`` or ``"submodule:pipe_abc123"`` (submodules
    key by pipeline_id, which survives renames; every other kind keys by its
    registered name). An empty/whitespace-only ``text`` removes the entry
    entirely, so the file doesn't accumulate empty-string notes.
    """
    logger.info("[layout] write_note called (key=%r, len(text)=%d)", key, len(text))
    with _layout_write("write_note"):
        data = _load()
        stripped = text.strip()
        if stripped:
            data["notes"][key] = text
        else:
            data["notes"].pop(key, None)
        _save(data)
    logger.debug("[layout] Note written successfully (key=%r)", key)


def graduate_manual_node(old_id: str, new_id: str) -> None:
    """Transfer position from a manual node to a DB-derived node ID and
    remove the manual entry. Scope-aware: the new id stays on whichever
    canvas the old node was placed on."""
    with _layout_write("graduate_manual_node"):
        data = _load()
        for scope in data["positions"].values():
            old_pos = scope.get(old_id)
            if old_pos and new_id not in scope:
                scope[new_id] = old_pos
            scope.pop(old_id, None)
        _save(data)
    pipeline_store.graduate_manual_node(get_db(), old_id, new_id)
