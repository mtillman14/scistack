"""The intent store: one table, one shape, for every statement about a run.

Conceptual reference: ``docs/claude/intent-and-fact.md``. Plan:
``.claude/plan-intent-and-fact.md`` Stage 5.

A **statement** is a piece of intent — which columns an input is restricted
to, which run options a call site uses, which values are hidden. Before this
module each kind lived in its own table with its own key shape, its own
answer to "is this scoped to a hypothesis?", and its own story for what
happens when the thing it attaches to changes identity. Seven tables, seven
answers. This is the one shape:

    (subject_kind, subject_ref, scope, aspect, aspect_key, value_json,
     origin, stated_at)

* ``subject_ref`` is the BARE canonical id (``fn__{fn}__{cid}``) with any
  ``::{pipeline_id}`` placement stripped — placement lives in ``scope``, its
  own column, which is what retires the placement-id trap
  (``docs/claude/placement-qualified-ids.md``) as a class rather than as an
  instance. A node id whose MEANING changes — a manual node graduating into
  its wiring id — is still a re-key, but now one call
  (:func:`rekey_subject`) covers every aspect instead of one migration per
  table.
* ``scope`` is on every row. Resolution walks ``scope -> global`` and
  nothing else; a duplicated hypothesis COPIES rows (:func:`copy_scope`)
  rather than inheriting them, so editing the copy can never reach the
  original.
* ``origin`` records which surface a statement came from, so precedence is
  data rather than convention (``scidb.intent``).

**scidb owns the meaning, this module owns the storage.** Shapes,
normalizers and the resolver are ``scidb.intent``; if that layer ever had to
import this one, the design would be wrong.

**Aspect by aspect.** ``columns`` has graduated: it is stored here, read from
here, and ``_node_config`` no longer carries it. The other aspects still live
in their original tables and move one commit at a time.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from scidb.intent import (
    ASPECT_COLUMNS,
    GLOBAL_SCOPE,
    SUBJECT_CALL_SITE,
    Statement,
    normalize,
)

logger = logging.getLogger(__name__)

#: Aspects that have moved into this table. An aspect NOT listed here is
#: still owned by its original table — the list is the migration's progress
#: bar, and the one place to look when asking "where does X live now?".
GRADUATED_ASPECTS = (ASPECT_COLUMNS,)

#: The `_node_config` key each graduated aspect used to be stored under.
NODE_CONFIG_KEYS = {ASPECT_COLUMNS: "columnSelections"}

#: Sentinel row marking the one-time import from `_node_config` as done, so
#: it cannot run twice and re-create statements the user has since deleted.
_MIGRATION_SUBJECT = ("migration", "node_config_import")


def _duck(db):
    from scistack_gui.pipeline_store import _duck as _d

    return _d(db)


def ensure_tables(db) -> None:
    """Create ``_intent`` if absent and run the one-time import.

    Called from ``pipeline_store._ensure_tables`` so every path that touches
    the GUI database gets it, exactly like the tables it replaces.
    """
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _intent (
            subject_kind VARCHAR NOT NULL,
            subject_ref  VARCHAR NOT NULL,
            scope        VARCHAR NOT NULL DEFAULT 'global',
            aspect       VARCHAR NOT NULL,
            aspect_key   VARCHAR NOT NULL DEFAULT '',
            value_json   VARCHAR NOT NULL DEFAULT 'null',
            origin       VARCHAR NOT NULL DEFAULT 'store',
            stated_at    VARCHAR NOT NULL DEFAULT '',
            PRIMARY KEY (subject_kind, subject_ref, scope, aspect, aspect_key)
        )
    """)
    _import_from_node_config(db)


# ---------------------------------------------------------------------------
# Reading and writing statements
# ---------------------------------------------------------------------------


def put_statements(db, statements) -> int:
    """Upsert statements. Returns how many rows were written."""
    rows = [
        (
            st.subject_kind,
            st.subject_ref,
            st.scope,
            st.aspect,
            st.key or "",
            json.dumps(st.value),
            st.surface,
            str(st.stated_at or datetime.now().isoformat()),
        )
        for st in statements
    ]
    for row in rows:
        _duck(db)._execute(
            """
            INSERT INTO _intent (subject_kind, subject_ref, scope, aspect,
                                 aspect_key, value_json, origin, stated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (subject_kind, subject_ref, scope, aspect, aspect_key)
            DO UPDATE SET value_json = excluded.value_json,
                          origin = excluded.origin,
                          stated_at = excluded.stated_at
            """,
            list(row),
        )
    if rows:
        logger.info(
            "[intent_store] wrote %d statement(s): %s",
            len(rows),
            sorted({(r[1], r[3], r[4]) for r in rows}),
        )
    return len(rows)


def load_statements(
    db,
    *,
    aspect: str | None = None,
    subject_refs=None,
    scopes=None,
) -> list[Statement]:
    """Statements, filtered in SQL rather than in Python.

    Filtering here and indexing once
    (``scidb.intent.index_statements``) is what keeps resolution off the N+1
    path when a canvas render resolves every node.
    """
    sql = "SELECT subject_kind, subject_ref, scope, aspect, aspect_key, value_json, origin, stated_at FROM _intent"
    clauses: list[str] = []
    params: list = []
    if aspect:
        clauses.append("aspect = ?")
        params.append(aspect)
    refs = sorted({r for r in (subject_refs or []) if r})
    if refs:
        clauses.append(f"subject_ref IN ({', '.join('?' * len(refs))})")
        params.extend(refs)
    wanted_scopes = sorted({s for s in (scopes or []) if s})
    if wanted_scopes:
        wanted_scopes = sorted(set(wanted_scopes) | {GLOBAL_SCOPE})
        clauses.append(f"scope IN ({', '.join('?' * len(wanted_scopes))})")
        params.extend(wanted_scopes)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    out: list[Statement] = []
    for kind, ref, scope, asp, key, raw, origin, stated_at in _duck(db)._fetchall(
        sql, params
    ):
        if kind == _MIGRATION_SUBJECT[0]:
            continue  # bookkeeping, not a statement about any subject
        try:
            value = json.loads(raw) if raw else None
        except (TypeError, ValueError):
            logger.warning(
                "[intent_store] unparseable value for %s/%s %s%s — ignoring",
                kind,
                ref,
                asp,
                f".{key}" if key else "",
            )
            continue
        try:
            out.append(
                Statement(
                    subject_kind=kind,
                    subject_ref=ref,
                    aspect=asp,
                    value=normalize(asp, value),
                    key=key or None,
                    scope=scope,
                    surface=origin,
                    stated_at=stated_at,
                )
            )
        except Exception as exc:  # a bad row must not break every other one
            logger.warning("[intent_store] skipping malformed row: %s", exc)
    return out


def clear_aspect(db, subject_ref: str, aspect: str, scope: str = GLOBAL_SCOPE) -> int:
    """Delete one subject's statements for one aspect in one scope."""
    before = _duck(db)._fetchone(
        "SELECT count(*) FROM _intent WHERE subject_ref = ? AND aspect = ? AND scope = ?",
        [subject_ref, aspect, scope],
    )
    _duck(db)._execute(
        "DELETE FROM _intent WHERE subject_ref = ? AND aspect = ? AND scope = ?",
        [subject_ref, aspect, scope],
    )
    return int(before[0]) if before else 0


def rekey_subject(db, old_ref: str, new_ref: str) -> int:
    """Move every statement from *old_ref* to *new_ref* — what graduation
    needs when a manual node's id becomes its wiring id.

    One call for every aspect, replacing one migration per table. Rows
    already present under *new_ref* win: a statement made about the real
    call site is never overwritten by one made about its placeholder.
    """
    from scistack_gui.domain.graph_builder import strip_placement

    old_ref, new_ref = strip_placement(old_ref), strip_placement(new_ref)
    if old_ref == new_ref:
        return 0
    moved = 0
    for st in load_statements(db, subject_refs=[old_ref]):
        existing = _duck(db)._fetchone(
            "SELECT 1 FROM _intent WHERE subject_kind = ? AND subject_ref = ? "
            "AND scope = ? AND aspect = ? AND aspect_key = ?",
            [st.subject_kind, new_ref, st.scope, st.aspect, st.key or ""],
        )
        if existing is None:
            from dataclasses import replace

            put_statements(db, [replace(st, subject_ref=new_ref)])
            moved += 1
    _duck(db)._execute("DELETE FROM _intent WHERE subject_ref = ?", [old_ref])
    if moved:
        logger.info(
            "[intent_store] rekeyed %d statement(s): %s -> %s",
            moved,
            old_ref,
            new_ref,
        )
    return moved


def copy_subject(db, src_ref: str, dst_ref: str) -> int:
    """Copy one subject's statements onto another — what DUPLICATING a node
    does, as against :func:`rekey_subject`'s move.

    Copy, never share: the duplicate owns its rows from the moment it exists,
    so configuring it cannot reach back into the original. (Before the intent
    store, duplicating a node copied only the legacy ``_pipeline_nodes.config``
    column, so a column selection saved on the original was silently absent
    from the copy — see ``scope_service._clone_nodes``.)
    """
    from dataclasses import replace

    from scistack_gui.domain.graph_builder import strip_placement

    src_ref, dst_ref = strip_placement(src_ref), strip_placement(dst_ref)
    if src_ref == dst_ref:
        return 0
    rows = [
        replace(st, subject_ref=dst_ref)
        for st in load_statements(db, subject_refs=[src_ref])
    ]
    put_statements(db, rows)
    if rows:
        logger.info(
            "[intent_store] copied %d statement(s): %s -> %s",
            len(rows),
            src_ref,
            dst_ref,
        )
    return len(rows)


def copy_scope(db, src_scope: str, dst_scope: str) -> int:
    """Copy a scope's statements to another — what duplicating a hypothesis
    does.

    Copy, never inherit: the duplicate owns its rows, so a later edit to the
    original cannot reach it and no ``derived_from`` chain has to be walked,
    guarded against cycles, or detached. ``scope_service._clone_nodes``
    already copied node config this way; this makes that the rule.
    """
    from dataclasses import replace

    rows = [
        replace(st, scope=dst_scope)
        for st in load_statements(db)
        if st.scope == src_scope
    ]
    put_statements(db, rows)
    if rows:
        logger.info(
            "[intent_store] copied %d statement(s) from scope %s to %s",
            len(rows),
            src_scope,
            dst_scope,
        )
    return len(rows)


# ---------------------------------------------------------------------------
# The `columns` aspect (the first to graduate)
# ---------------------------------------------------------------------------


def set_column_selections(db, node_id: str, selections: dict | None) -> None:
    """Replace a call site's column selections — one statement per parameter.

    Per parameter, not one blob: the resolver's unit is a field, so a
    selection on one input can be overridden, reported or dropped without
    touching another's.
    """
    from scistack_gui.domain.graph_builder import parse_placement_id, strip_placement

    bare = strip_placement(node_id)
    placement = parse_placement_id(node_id)
    if placement:
        # Recorded for display/diagnosis only. The stored SCOPE stays global
        # because execution is not scope-aware yet (see `get_hidden_node_ids`'
        # fail-open convention): narrowing a statement to one pipeline before
        # runs are scope-aware would silently stop applying it. The column is
        # here, and the resolver walks it, for when that changes.
        logger.debug(
            "[intent_store] column selections for %s stored at global scope "
            "(placement %s kept as display only)",
            bare,
            placement[1],
        )
    clear_aspect(db, bare, ASPECT_COLUMNS)
    statements = []
    for param, raw in (selections or {}).items():
        sel = normalize(ASPECT_COLUMNS, raw)
        if sel is None:
            continue
        statements.append(
            Statement(
                subject_kind=SUBJECT_CALL_SITE,
                subject_ref=bare,
                aspect=ASPECT_COLUMNS,
                value=sel,
                key=param,
                scope=GLOBAL_SCOPE,
            )
        )
    put_statements(db, statements)


def column_selections_by_node(db) -> dict[str, dict]:
    """``{bare_node_id: {param: selection}}`` — the display view.

    The panel reads this back through ``pipeline_store.get_node_config``, so
    a selection made on a node still shows on that node, exactly as when it
    lived in ``_node_config``.
    """
    out: dict[str, dict] = {}
    for st in load_statements(db, aspect=ASPECT_COLUMNS):
        if st.key and st.value:
            out.setdefault(st.subject_ref, {})[st.key] = st.value
    return out


#: ``{aspect: writer}`` for the graduated aspects — how a ``_node_config``
#: blob key becomes statements. One line per aspect as each graduates, so
#: ``update_node_config`` never grows an ``if`` per aspect.
SETTERS = {ASPECT_COLUMNS: set_column_selections}


def _import_from_node_config(db) -> None:
    """One-time: carry every stored ``columnSelections`` into ``_intent``.

    Every row is copied and the source is left alone (a user's saved
    selection is data — see the project rule that "remove" means hide, never
    delete). The marker row makes this idempotent, so a selection deleted
    after the import is not resurrected on the next start.
    """
    marker = _duck(db)._fetchone(
        "SELECT 1 FROM _intent WHERE subject_kind = ? AND subject_ref = ?",
        list(_MIGRATION_SUBJECT),
    )
    if marker is not None:
        return

    imported = 0
    try:
        rows = _duck(db)._fetchall("SELECT node_id, config FROM _node_config")
    except Exception:
        rows = []
    for node_id, raw in rows:
        try:
            config = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            continue
        selections = (config or {}).get(NODE_CONFIG_KEYS[ASPECT_COLUMNS]) or {}
        if not selections:
            continue
        set_column_selections(db, node_id, selections)
        imported += 1

    _duck(db)._execute(
        """
        INSERT INTO _intent (subject_kind, subject_ref, scope, aspect,
                             aspect_key, value_json, origin, stated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [
            _MIGRATION_SUBJECT[0],
            _MIGRATION_SUBJECT[1],
            GLOBAL_SCOPE,
            ASPECT_COLUMNS,
            "",
            json.dumps({"imported_nodes": imported}),
            "migration",
            datetime.now().isoformat(),
        ],
    )
    logger.info(
        "[intent_store] imported column selections from %d node config(s)",
        imported,
    )
