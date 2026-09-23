"""``_node_wiring`` — which canvas node has run as which wiring.

Implements D-2026-09-22-2 (`docs/claude/node-identity.md` §7a). The argument
is that document; this module is the storage and the accessors, and
``domain/node_identity.py`` is the rule that uses them.

**The problem it solves.** A canvas function node used to be identified by
``fn__{fn}__{wiring_id}`` — a hash of its *recorded* input bindings. Drawing
an edge does not change that hash, but RUNNING through the drawn edge records
the new binding, which rehashes it. Same picture on the canvas, two ids,
depending on whether you have run yet: a duplicate node, and every statement
keyed by the old id silently stranded.

So the id stops being derived and starts being **allocated once and
remembered**, with ``wiring_id`` demoted from identity to attribute. This
table is where it is remembered:

    (node_id, wiring_id, run_id, first_seen, last_seen, scope)

append-only, one row per distinct wiring a node has run as. ``wiring_id`` is
the right grain because it is what the node *is*; ``call_id`` is finer (a node
has one per variant row) and ``invocation_id`` is per-execution. ``run_id``
is carried so the row also answers *when, and under which run* — which makes
"last ran as W2, before that W1" a query.

**Where it lives: GUI-side, beside ``_intent`` — never in provenance.**
``docs/claude/intent-and-fact.md`` §8 is explicit ("if scidb ever had to
import ``pipeline_store``, the design is wrong") and fact is never edited.
The association is a statement about which canvas node means which wiring, so
it is intent-adjacent, not fact.

**A minted id carries no meaning.** ``fn__{fn}__{uuid4[:16]}``, always. It was
briefly the derived spelling-when-free, which would have let an existing
database open to the ids it already had — but an id that *looks* like a wiring
hash and is not one is the exact confusion this area keeps having, and the
project takes clean breaks over compatibility shims. The only thing readable
in a node id now is the function it runs (D-2026-09-22-3).

Sixteen hex, not the eight a manual node uses: ``ids.parse_fn_node_id``
recognises a DB-derived function node by a 16-hex trailing segment, in roughly
forty places. That length is a contract with the id grammar, not part of the
decision.

**Two questions, and they have different answers.** A node's *current* wiring
is the latest one it ran as (:func:`current_wiring`), which is its shape on the
canvas and what a Run executes. A node's *wirings* are every shape it has ever
run as (:func:`wirings_for_node`), which is its history and what attribution
matches against. Conflating them is how a node ends up drawing handles for a
shape it no longer has.

**Nothing here swallows a failure.** If this table cannot be read, the GUI
cannot say which node a run belongs to — and then node state, the Run button
and every saved setting are all describing something unverified. Failing the
build loudly is the correct outcome; a canvas that draws but cannot be trusted
is worse than no canvas.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime

from scistack_gui.ids import (
    FN_ID_PREFIX,
    ROOT_SCOPE,
    BareNodeId,
    strip_placement,
)

logger = logging.getLogger(__name__)


def _duck(db):
    from scistack_gui.pipeline_store import _duck as _d

    return _d(db)


def _now() -> str:
    return datetime.now().isoformat()


def ensure_tables(db) -> None:
    """Create ``_node_wiring`` if absent.

    Called from ``pipeline_store._ensure_tables``, so every path that touches
    the GUI database gets it — exactly like ``_intent``.
    """
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _node_wiring (
            node_id    VARCHAR NOT NULL,
            wiring_id  VARCHAR NOT NULL,
            run_id     VARCHAR,
            first_seen VARCHAR NOT NULL DEFAULT '',
            last_seen  VARCHAR NOT NULL DEFAULT '',
            scope      VARCHAR NOT NULL DEFAULT 'main',
            PRIMARY KEY (node_id, wiring_id)
        )
    """)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------
#
# These raise. See the module docstring: an unreadable association table means
# the GUI cannot say which node anything belongs to, and every answer that
# follows would be a guess presented as a fact.


def _rows(db, sql: str, params=None) -> list:
    return _duck(db)._fetchall(sql, params or [])


def associations(db) -> list[dict]:
    """Every ``(node_id, wiring_id, run_id, first_seen, last_seen, scope)``
    row, oldest first.

    Oldest first because the order is the answer to half the questions asked
    of this table ("what did it run as before?"), and because ambiguity
    resolution prefers the oldest node (§7b).
    """
    rows = _rows(
        db,
        "SELECT node_id, wiring_id, run_id, first_seen, last_seen, scope "
        "FROM _node_wiring ORDER BY first_seen, node_id, wiring_id",
    )
    return [
        {
            "node_id": r[0],
            "wiring_id": r[1],
            "run_id": r[2],
            "first_seen": r[3],
            "last_seen": r[4],
            "scope": r[5],
        }
        for r in rows
    ]


def wirings_for_node(db, node_id: str) -> list[str]:
    """Every wiring *node_id* has run as, oldest first — its HISTORY.

    For attribution and for the Variants view. **Not** for deciding what the
    node currently is: see :func:`current_wiring`.
    """
    bare = strip_placement(node_id)
    return [
        r[0]
        for r in _rows(
            db,
            "SELECT wiring_id FROM _node_wiring WHERE node_id = ? "
            "ORDER BY first_seen, wiring_id",
            [str(bare)],
        )
    ]


def current_wiring(db, node_id: str) -> str | None:
    """The wiring *node_id* most recently ran as — its shape NOW, before any
    edge the user has drawn since. None if it has never run.

    This is what the canvas draws and what a Run executes. A node that was
    rewired owns its older shapes too (:func:`wirings_for_node`), but they are
    history: drawing handles for them, or re-running them from the node's own
    Run button, would be running something the node no longer says it is.
    """
    bare = strip_placement(node_id)
    rows = _rows(
        db,
        "SELECT wiring_id FROM _node_wiring WHERE node_id = ? "
        "ORDER BY last_seen DESC, wiring_id DESC LIMIT 1",
        [str(bare)],
    )
    return rows[0][0] if rows else None


def current_wiring_by_node(db) -> dict[str, str]:
    """``{node_id: current wiring}`` for every node, in one query.

    The graph build needs this for every node at once; asking per node would
    be an N+1 on the canvas path.
    """
    rows = _rows(
        db,
        "SELECT node_id, wiring_id, last_seen FROM _node_wiring "
        "ORDER BY last_seen, wiring_id",
    )
    latest: dict[str, str] = {}
    for node_id, wiring_id, _last_seen in rows:
        latest[node_id] = wiring_id  # ascending order: the last write wins
    return latest


def nodes_for_wiring(db, wiring_id: str) -> list[str]:
    """Every node that has run as *wiring_id*, oldest first."""
    return [
        r[0]
        for r in _rows(
            db,
            "SELECT node_id FROM _node_wiring WHERE wiring_id = ? "
            "ORDER BY first_seen, node_id",
            [wiring_id],
        )
    ]


def node_for_wiring(db, wiring_id: str) -> str | None:
    """The single node *wiring_id* belongs to, or None.

    Several nodes for one wiring is the §7b ambiguity and is resolved by
    ``domain.node_identity``, which has the scope and the age to decide with;
    this accessor returns the oldest and logs, so a caller that only needs "a
    node" is never wrong about which one the canvas will use.
    """
    found = nodes_for_wiring(db, wiring_id)
    if len(found) > 1:
        logger.info(
            "[node_wiring] wiring %s is claimed by %d nodes (%s) — taking the "
            "oldest; see node_identity for the resolution that decides",
            wiring_id,
            len(found),
            found,
        )
    return found[0] if found else None


def known_node_ids(db) -> set[str]:
    """Every node id this table has ever minted or recorded."""
    return {r[0] for r in _rows(db, "SELECT DISTINCT node_id FROM _node_wiring")}


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def record(
    db,
    node_id: str,
    wiring_id: str,
    *,
    run_id: str | None = None,
    scope: str = ROOT_SCOPE,
    seen: str | None = None,
) -> bool:
    """Record that *node_id* has run as *wiring_id*. Returns True if this is
    the first time.

    **Append-only.** A repeat advances ``last_seen`` (and fills ``run_id`` if
    the first record did not have one) and touches nothing else: the row is
    a fact about what happened, and rewriting ``first_seen`` would erase the
    chronology that makes "what did it run as BEFORE?" answerable.

    ``last_seen`` is also what :func:`current_wiring` orders by, so a re-run of
    an older shape legitimately makes that shape current again — the node has
    just run that way.

    The placement suffix is stripped from *node_id* for the same reason
    ``_intent`` strips it: placement lives in ``scope``, its own column —
    which is what retires the placement-id lookup trap as a class rather than
    as an instance (``docs/claude/placement-qualified-ids.md``).
    """
    ensure_tables(db)
    bare = str(strip_placement(node_id))
    stamp = seen or _now()
    existing = _rows(
        db,
        "SELECT first_seen, run_id FROM _node_wiring WHERE node_id = ? AND wiring_id = ?",
        [bare, wiring_id],
    )
    if existing:
        _duck(db)._execute(
            "UPDATE _node_wiring SET last_seen = ?, run_id = COALESCE(run_id, ?) "
            "WHERE node_id = ? AND wiring_id = ?",
            [stamp, run_id, bare, wiring_id],
        )
        logger.debug(
            "[node_wiring] %s ran as %s again (run_id=%s)", bare, wiring_id, run_id
        )
        return False
    _duck(db)._execute(
        "INSERT INTO _node_wiring (node_id, wiring_id, run_id, first_seen, "
        "last_seen, scope) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        [bare, wiring_id, run_id, stamp, stamp, scope or ROOT_SCOPE],
    )
    # INFO, not DEBUG: this row is what stops a node duplicating, and its
    # absence is the first thing to check when one does anyway.
    logger.info(
        "[node_wiring] %s now runs as wiring %s (run_id=%s, scope=%s)",
        bare,
        wiring_id,
        run_id,
        scope,
    )
    return True


def token_resolver(db):
    """``(fn_name, wiring_id) -> node token``, read once from the table.

    The ``token_for`` seam for every caller OUTSIDE the graph build. The build
    has an ``IdentityPlan`` (it is deciding the assignment, and may mint); a
    service that only needs to look one up reads the recorded answer here.

    A wiring with no node is its own token. That is not a fallback to the old
    behaviour — under random ids it never matches a real node id — it is the
    answer for a wiring nothing has claimed yet, which every caller already
    treats as "no node".

    Read eagerly into a dict rather than queried per call: the callers are
    loops over every call site, and this is on the Run path.
    """
    by_wiring: dict[str, str] = {}
    for row in associations(db):
        by_wiring.setdefault(row["wiring_id"], row["node_id"])

    def token_for(fn_name: str, wiring: str) -> str:
        node_id = by_wiring.get(wiring)
        prefix = f"{FN_ID_PREFIX}{fn_name}__"
        if node_id and node_id.startswith(prefix):
            return node_id[len(prefix) :]
        return wiring

    return token_for


def mint_node_id(fn_name: str, taken: "set[str] | frozenset[str]" = frozenset()) -> BareNodeId:
    """Allocate a function node's id, once. ``fn__{fn}__{uuid4[:16]}``.

    Nothing is readable in the suffix and nothing is meant to be. The id says
    which function the node runs, and everything else about it — its wiring,
    its constants, its scope — is looked up, because all of those can change
    while the node stays the same node (D-2026-09-22-3).

    ``taken`` only guards against the astronomically unlikely; it is passed so
    a caller minting several in one pass cannot collide with itself.
    """
    while True:
        candidate = BareNodeId(f"{FN_ID_PREFIX}{fn_name}__{uuid.uuid4().hex[:16]}")
        if candidate not in taken:
            return candidate


def forget_node(db, node_id: str) -> int:
    """Drop every association for *node_id*. Returns the row count.

    For the one legitimate caller: a node the user deletes outright. It is
    NOT a hide — hiding keeps the node and its history (project ethos: hide,
    never delete) — and nothing on the build path calls it.
    """
    bare = str(strip_placement(node_id))
    rows = _rows(db, "SELECT COUNT(*) FROM _node_wiring WHERE node_id = ?", [bare])
    n = int(rows[0][0]) if rows else 0
    if n:
        _duck(db)._execute("DELETE FROM _node_wiring WHERE node_id = ?", [bare])
        logger.info("[node_wiring] forgot %d association(s) for %s", n, bare)
    return n


def rekey_node(db, old_id: str, new_id: str) -> int:
    """Move every association from *old_id* to *new_id*.

    The counterpart of ``intent_store.rekey_subject``, for the one id change
    that survives this plan: a manual node graduating into a DB-derived one.
    Rows already held by *new_id* win, so a repeat moves nothing.
    """
    old = str(strip_placement(old_id))
    new = str(strip_placement(new_id))
    if old == new:
        return 0
    moved = 0
    for row in _rows(
        db,
        "SELECT wiring_id, run_id, first_seen, last_seen, scope FROM _node_wiring "
        "WHERE node_id = ?",
        [old],
    ):
        wiring, run_id, first_seen, last_seen, scope = row
        clash = _rows(
            db,
            "SELECT 1 FROM _node_wiring WHERE node_id = ? AND wiring_id = ?",
            [new, wiring],
        )
        if not clash:
            _duck(db)._execute(
                "INSERT INTO _node_wiring (node_id, wiring_id, run_id, first_seen, "
                "last_seen, scope) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                [new, wiring, run_id, first_seen, last_seen, scope],
            )
            moved += 1
    _duck(db)._execute("DELETE FROM _node_wiring WHERE node_id = ?", [old])
    if moved:
        logger.info(
            "[node_wiring] moved %d association(s) from %s to %s", moved, old, new
        )
    return moved


def forget_all(db) -> int:
    """Delete every association, so the next build mints fresh node ids.

    **Destructive and manual.** Nothing calls it. New ids mean every saved
    position, node config, hide and scope membership keyed by an old id stops
    resolving — which is the price of the clean break, paid once, and paying
    it twice is not something a startup path should be able to do on its own.
    """
    rows = _rows(db, "SELECT COUNT(*) FROM _node_wiring")
    n = int(rows[0][0]) if rows else 0
    _duck(db)._execute("DELETE FROM _node_wiring")
    logger.warning(
        "[node_wiring] dropped the whole association table (%d row(s)) — the "
        "next graph build mints a fresh id for every function node, and every "
        "setting keyed by an old one stops resolving",
        n,
    )
    return n
