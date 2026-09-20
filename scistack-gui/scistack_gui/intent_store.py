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

**Aspect by aspect.** Every execution-intent aspect has graduated — see
``GRADUATED_ASPECTS`` for which table each replaced. ``pipeline_store`` keeps
its public accessors and delegates to the functions here, so no caller
changed; the old tables are left in place, rows untouched, until the copy has
been seen complete on a real database.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from scidb.intent import (
    ASPECT_COLUMNS,
    ASPECT_CONSTANTS,
    ASPECT_HIDDEN,
    ASPECT_RUN_OPTIONS,
    ASPECT_SCHEMA_LOCATION,
    ASPECT_VARIANT_SELECTION,
    ASPECT_WIRING,
    GLOBAL_SCOPE,
    SUBJECT_CALL_SITE,
    SUBJECT_EDGE,
    SUBJECT_PARAMETER,
    SUBJECT_VARIABLE_TYPE,
    Statement,
    normalize,
)

logger = logging.getLogger(__name__)

#: Aspects that have moved into this table, with the table each replaced.
#: Every execution-intent table has moved; the old tables are left in place
#: (rows untouched) until the copy has been seen complete on a real database.
#: Display intent (layout, value groups, hidden ports, hypothesis prose) is
#: deliberately not here — see docs/claude/intent-and-fact.md §4.
GRADUATED_ASPECTS = {
    ASPECT_COLUMNS: "_node_config.columnSelections",
    ASPECT_RUN_OPTIONS: "_node_config.runOptions",
    ASPECT_SCHEMA_LOCATION: "_node_config.schemaSelection/schemaLevel/whereFilters",
    ASPECT_HIDDEN: "_pipeline_hidden_nodes/_combos/_constant_values/_edges",
    ASPECT_WIRING: "_pipeline_edges",
    ASPECT_CONSTANTS: "_pipeline_pending_constants",
}

#: The `_node_config` blob key each single-key node aspect used to live under.
NODE_CONFIG_KEYS = {
    ASPECT_COLUMNS: "columnSelections",
    ASPECT_RUN_OPTIONS: "runOptions",
}

#: Marker rows recording each one-time import (subject_ref = import name), so
#: none can run twice and re-create statements the user has since deleted.
_MIGRATION_SUBJECT = ("migration", "")


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
    run_imports(db)


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
    subject_kind: str | None = None,
    key: str | None = None,
    ref_prefix: str | None = None,
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
    if subject_kind:
        clauses.append("subject_kind = ?")
        params.append(subject_kind)
    if key is not None:
        clauses.append("aspect_key = ?")
        params.append(key)
    if ref_prefix:
        clauses.append("subject_ref LIKE ?")
        params.append(ref_prefix.replace("%", "\\%") + "%")
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
    for kind, ref, scope, asp, akey, raw, origin, stated_at in _duck(db)._fetchall(
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
                f".{akey}" if akey else "",
            )
            continue
        try:
            out.append(
                Statement(
                    subject_kind=kind,
                    subject_ref=ref,
                    aspect=asp,
                    value=normalize(asp, value),
                    key=akey or None,
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



def delete_statements(
    db,
    *,
    aspect: str,
    subject_ref: str | None = None,
    scope: str | None = None,
    key: str | None = None,
    subject_kind: str | None = None,
    ref_prefix: str | None = None,
) -> int:
    """Delete the statements matching every given filter. Returns the count.

    ``scope=None`` means every scope — the fail-open union the hidden-state
    accessors have always offered — and ``key=None`` means every key.
    """
    clauses = ["aspect = ?"]
    params: list = [aspect]
    if subject_ref is not None:
        clauses.append("subject_ref = ?")
        params.append(subject_ref)
    if scope is not None:
        clauses.append("scope = ?")
        params.append(scope)
    if key is not None:
        clauses.append("aspect_key = ?")
        params.append(key)
    if subject_kind is not None:
        clauses.append("subject_kind = ?")
        params.append(subject_kind)
    if ref_prefix:
        clauses.append("subject_ref LIKE ?")
        params.append(ref_prefix.replace("%", "\\%") + "%")
    where = " AND ".join(clauses)
    before = _duck(db)._fetchone(f"SELECT count(*) FROM _intent WHERE {where}", params)
    _duck(db)._execute(f"DELETE FROM _intent WHERE {where}", params)
    return int(before[0]) if before else 0

def rekey_subject(db, old_ref: str, new_ref: str, *, old_wins: bool = True) -> int:
    """Move every statement from *old_ref* to *new_ref* — what graduation
    needs when a manual node's id becomes its wiring id.

    One call for every aspect, replacing one migration per table. With
    ``old_wins`` (the default, and what graduation wants) the moved
    statements overwrite whatever *new_ref* already had, and each replaced
    value is logged verbatim: the realistic route to a conflict is a wired
    fresh node the user configured and ran, whose settings produced the very
    history it graduates into — after graduation it must run the way it
    just ran (``.claude/plan-graduation-config-migration.md``). Pass
    ``old_wins=False`` to keep *new_ref*'s statements instead.
    """
    from scistack_gui.domain.graph_builder import strip_placement

    old_ref, new_ref = strip_placement(old_ref), strip_placement(new_ref)
    if old_ref == new_ref:
        return 0
    from dataclasses import replace

    moved = 0
    for st in load_statements(db, subject_refs=[old_ref]):
        existing = _duck(db)._fetchone(
            "SELECT value_json FROM _intent WHERE subject_kind = ? AND subject_ref = ? "
            "AND scope = ? AND aspect = ? AND aspect_key = ?",
            [st.subject_kind, new_ref, st.scope, st.aspect, st.key or ""],
        )
        if existing is not None and not old_wins:
            continue
        if existing is not None and existing[0] != json.dumps(st.value):
            logger.info(
                "[intent_store] rekey %s -> %s: %s%s previous value %s replaced by %r",
                old_ref,
                new_ref,
                st.aspect,
                f".{st.key}" if st.key else "",
                existing[0],
                st.value,
            )
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
# One-time imports
# ---------------------------------------------------------------------------
# Each source table is carried over exactly once, guarded by a marker row, so
# a statement the user deletes afterwards is never resurrected on the next
# start. The source rows are left alone: a user's saved state is data, and
# "remove" means hide, never delete. Dropping the old tables is a separate,
# deliberate step after the copy has been seen to be complete on a real
# database — not something a startup path does on its own.


def _imported(db, name: str) -> bool:
    return (
        _duck(db)._fetchone(
            "SELECT 1 FROM _intent WHERE subject_kind = ? AND subject_ref = ?",
            [_MIGRATION_SUBJECT[0], name],
        )
        is not None
    )


def _mark_imported(db, name: str, aspect: str, detail: dict) -> None:
    _duck(db)._execute(
        """
        INSERT INTO _intent (subject_kind, subject_ref, scope, aspect,
                             aspect_key, value_json, origin, stated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [
            _MIGRATION_SUBJECT[0],
            name,
            GLOBAL_SCOPE,
            aspect,
            "",
            json.dumps(detail),
            "migration",
            datetime.now().isoformat(),
        ],
    )


def _import_once(db, name: str, aspect: str, importer) -> None:
    """Run *importer(db) -> int* once, ever, under *name*."""
    if _imported(db, name):
        return
    try:
        count = importer(db)
    except Exception:
        # A source table this database never had (fresh DB): nothing to copy.
        logger.debug("[intent_store] import %s skipped", name, exc_info=True)
        count = 0
    _mark_imported(db, name, aspect, {"imported": count})
    logger.info("[intent_store] import %s: %d row(s) carried over", name, count)


def _rows(db, sql: str, params=None) -> list:
    try:
        return _duck(db)._fetchall(sql, params or [])
    except Exception:
        return []


def _subject_kind_for_node(node_id: str) -> str:
    """The subject kind a canvas node id names, by its prefix."""
    bare = node_id.split("::", 1)[0]
    if bare.startswith("var__"):
        return SUBJECT_VARIABLE_TYPE
    if bare.startswith("param__") or bare.startswith("pathInput__"):
        return SUBJECT_PARAMETER
    return SUBJECT_CALL_SITE


def _now() -> str:
    return datetime.now().isoformat()


# ---------------------------------------------------------------------------
# Node config aspects: `columns`, `run_options`, `schema_location`
# ---------------------------------------------------------------------------
# What FunctionSettingsPanel saves as one blob is three statements about one
# call site. `update_node_config` splits them on the way in; `get_node_config`
# / `get_node_configs` merge them back for display. `_node_config` itself no
# longer owns any of it.

#: The whole-aspect statement `schema_location` carries the panel's three
#: location-related settings under their own names — one owner for the
#: spelling is the panel, and re-spelling them here would only add a seam.
SCHEMA_LOCATION_KEYS = ("schemaSelection", "schemaLevel", "whereFilters")


def set_column_selections(db, node_id: str, selections: dict | None) -> None:
    """Replace a call site's column selections — one statement per parameter.

    Per parameter, not one blob: the resolver's unit is a field, so a
    selection on one input can be overridden, reported or dropped without
    touching another's.
    """
    from scistack_gui.domain.graph_builder import strip_placement

    bare = strip_placement(node_id)
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


def set_run_options(db, node_id: str, options: dict | None) -> None:
    """Replace a call site's run options (dry_run / save / distribute /
    as_table) — one whole-aspect statement."""
    from scistack_gui.domain.graph_builder import strip_placement

    bare = strip_placement(node_id)
    clear_aspect(db, bare, ASPECT_RUN_OPTIONS)
    if options:
        put_statements(
            db,
            [
                Statement(
                    subject_kind=SUBJECT_CALL_SITE,
                    subject_ref=bare,
                    aspect=ASPECT_RUN_OPTIONS,
                    value=dict(options),
                    scope=GLOBAL_SCOPE,
                )
            ],
        )


def set_schema_location(db, node_id: str, location: dict | None) -> None:
    """Replace a call site's location settings — which locations it runs at,
    which keys iterate, which where-filters apply — as one statement."""
    from scistack_gui.domain.graph_builder import strip_placement

    bare = strip_placement(node_id)
    clear_aspect(db, bare, ASPECT_SCHEMA_LOCATION)
    value = {k: v for k, v in (location or {}).items() if k in SCHEMA_LOCATION_KEYS}
    if value:
        put_statements(
            db,
            [
                Statement(
                    subject_kind=SUBJECT_CALL_SITE,
                    subject_ref=bare,
                    aspect=ASPECT_SCHEMA_LOCATION,
                    value=value,
                    scope=GLOBAL_SCOPE,
                )
            ],
        )


def _set_schema_location_from_blob(db, node_id: str, blob_part) -> None:
    """Adapter for the SETTERS table: the blob carries the three location
    keys at top level, so the writer receives the whole blob."""
    set_schema_location(db, node_id, blob_part)


#: ``{aspect: writer}`` for the graduated aspects — how a ``_node_config``
#: blob becomes statements. One entry per aspect as each graduates, so
#: ``update_node_config`` never grows an ``if`` per aspect.
SETTERS = {
    ASPECT_COLUMNS: set_column_selections,
    ASPECT_RUN_OPTIONS: set_run_options,
    ASPECT_SCHEMA_LOCATION: _set_schema_location_from_blob,
}


def split_node_config(db, node_id: str, config: dict) -> dict:
    """Write every graduated aspect of *config* as statements and return what
    is LEFT for the blob (nothing, once every aspect has moved).

    The `update_node_config` seam calls this. `schema_location` is three
    top-level blob keys, so it is gathered rather than popped by one name.
    """
    rest = dict(config)
    if NODE_CONFIG_KEYS[ASPECT_COLUMNS] in rest:
        set_column_selections(db, node_id, rest.pop(NODE_CONFIG_KEYS[ASPECT_COLUMNS]))
    if NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS] in rest:
        set_run_options(db, node_id, rest.pop(NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS]))
    if any(k in rest for k in SCHEMA_LOCATION_KEYS):
        set_schema_location(db, node_id, {k: rest.pop(k) for k in SCHEMA_LOCATION_KEYS if k in rest})
    return rest


def node_config_overlay(db) -> dict[str, dict]:
    """``{bare_node_id: {blob_key: value}}`` — every graduated aspect, in the
    blob spelling the panel reads.

    One query per aspect for the whole graph, never one per node.
    """
    out: dict[str, dict] = {}
    for st in load_statements(db, aspect=ASPECT_COLUMNS, subject_kind=SUBJECT_CALL_SITE):
        if st.key and st.value:
            out.setdefault(st.subject_ref, {}).setdefault(
                NODE_CONFIG_KEYS[ASPECT_COLUMNS], {}
            )[st.key] = st.value
    for st in load_statements(db, aspect=ASPECT_RUN_OPTIONS, subject_kind=SUBJECT_CALL_SITE):
        if st.value:
            out.setdefault(st.subject_ref, {})[NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS]] = st.value
    for st in load_statements(
        db, aspect=ASPECT_SCHEMA_LOCATION, subject_kind=SUBJECT_CALL_SITE
    ):
        if isinstance(st.value, dict):
            out.setdefault(st.subject_ref, {}).update(
                {k: v for k, v in st.value.items() if k in SCHEMA_LOCATION_KEYS}
            )
    return out


def column_selections_by_node(db) -> dict[str, dict]:
    """``{bare_node_id: {param: selection}}`` — the display view of the
    `columns` aspect alone."""
    key = NODE_CONFIG_KEYS[ASPECT_COLUMNS]
    return {
        nid: cfg[key] for nid, cfg in node_config_overlay(db).items() if cfg.get(key)
    }


def _import_node_config(db) -> int:
    imported = 0
    for node_id, raw in _rows(db, "SELECT node_id, config FROM _node_config"):
        try:
            config = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            continue
        if not config:
            continue
        before = len(config)
        rest = split_node_config(db, node_id, config)
        if len(rest) != before:
            imported += 1
    return imported


# ---------------------------------------------------------------------------
# `hidden`: nodes, one-combo-of-a-node, parameter values, edges
# ---------------------------------------------------------------------------
# Four tables became four SHAPES of one aspect, distinguished by subject kind
# and key. Scope is the pipeline for nodes, values and edges — what those
# tables already did — and GLOBAL for combos, which were global by design
# (a still-deferred follow-up the old table documented).

HIDDEN_NODE_KEY = "node"
HIDDEN_COMBO_KEY = "combo"
HIDDEN_EDGE_KEY = "edge"


def _hidden_statement(kind, ref, key, value, scope) -> Statement:
    return Statement(
        subject_kind=kind,
        subject_ref=ref,
        aspect=ASPECT_HIDDEN,
        value=value,
        key=key,
        scope=scope,
        stated_at=_now(),
    )


def hide_node(db, node_id: str, pipeline_id: str) -> None:
    put_statements(
        db,
        [_hidden_statement(_subject_kind_for_node(node_id), node_id, HIDDEN_NODE_KEY, True, pipeline_id)],
    )


def unhide_node(db, node_id: str, pipeline_id: str) -> int:
    return delete_statements(
        db, aspect=ASPECT_HIDDEN, subject_ref=node_id, scope=pipeline_id, key=HIDDEN_NODE_KEY
    )


def unhide_nodes_by_prefix(db, prefix: str, pipeline_id: str) -> int:
    return delete_statements(
        db, aspect=ASPECT_HIDDEN, ref_prefix=prefix, scope=pipeline_id, key=HIDDEN_NODE_KEY
    )


def hidden_node_ids(db, pipeline_id: str | None) -> set[str]:
    """Hidden node ids in *pipeline_id* (``None`` = every scope), always
    unioned with the hidden combos, which are global."""
    nodes = load_statements(
        db,
        aspect=ASPECT_HIDDEN,
        key=HIDDEN_NODE_KEY,
        scopes=[pipeline_id] if pipeline_id else None,
    )
    out = {s.subject_ref for s in nodes}
    out.update(s.subject_ref for s in load_statements(db, aspect=ASPECT_HIDDEN, key=HIDDEN_COMBO_KEY))
    return out


def hide_combo(db, node_id: str, function_name: str, variant_key: dict) -> None:
    put_statements(
        db,
        [
            _hidden_statement(
                SUBJECT_CALL_SITE,
                node_id,
                HIDDEN_COMBO_KEY,
                {"function_name": function_name, "variant_key": variant_key},
                GLOBAL_SCOPE,
            )
        ],
    )


def unhide_combo(db, node_id: str) -> int:
    return delete_statements(db, aspect=ASPECT_HIDDEN, subject_ref=node_id, key=HIDDEN_COMBO_KEY)


def hidden_combos(db, function_name: str) -> list[dict]:
    return [
        {"node_id": s.subject_ref, "variant_key": (s.value or {}).get("variant_key") or {}}
        for s in load_statements(db, aspect=ASPECT_HIDDEN, key=HIDDEN_COMBO_KEY)
        if (s.value or {}).get("function_name") == function_name
    ]


def hide_parameter_values(db, const_name: str, values, pipeline_id: str) -> int:
    statements = [
        _hidden_statement(SUBJECT_PARAMETER, const_name, str(v), True, pipeline_id)
        for v in values
    ]
    return put_statements(db, statements)


def unhide_parameter_values(db, const_name: str, values, pipeline_id: str) -> int:
    n = 0
    for v in values:
        n += delete_statements(
            db,
            aspect=ASPECT_HIDDEN,
            subject_kind=SUBJECT_PARAMETER,
            subject_ref=const_name,
            scope=pipeline_id,
            key=str(v),
        )
    return n


def hidden_parameter_values(db, pipeline_id: str | None) -> list[dict]:
    rows = load_statements(
        db,
        aspect=ASPECT_HIDDEN,
        subject_kind=SUBJECT_PARAMETER,
        scopes=[pipeline_id] if pipeline_id else None,
    )
    return [{"const_name": s.subject_ref, "value": s.key} for s in rows if s.key]


def hide_edge(db, edge_id: str, source, target, source_handle, target_handle, pipeline_id: str) -> None:
    put_statements(
        db,
        [
            _hidden_statement(
                SUBJECT_EDGE,
                edge_id,
                HIDDEN_EDGE_KEY,
                {
                    "source": source or "",
                    "target": target or "",
                    "source_handle": source_handle,
                    "target_handle": target_handle,
                },
                pipeline_id,
            )
        ],
    )


def unhide_edge(db, edge_id: str, pipeline_id: str) -> int:
    return delete_statements(
        db, aspect=ASPECT_HIDDEN, subject_ref=edge_id, scope=pipeline_id, key=HIDDEN_EDGE_KEY
    )


def hidden_edges(db, pipeline_id: str | None) -> list[dict]:
    rows = load_statements(
        db,
        aspect=ASPECT_HIDDEN,
        key=HIDDEN_EDGE_KEY,
        scopes=[pipeline_id] if pipeline_id else None,
    )
    return [
        {
            "edge_id": s.subject_ref,
            "source": (s.value or {}).get("source", ""),
            "target": (s.value or {}).get("target", ""),
            "source_handle": (s.value or {}).get("source_handle"),
            "target_handle": (s.value or {}).get("target_handle"),
        }
        for s in rows
    ]


def _import_hidden(db) -> int:
    n = 0
    for pipeline_id, node_id in _rows(db, "SELECT pipeline_id, node_id FROM _pipeline_hidden_nodes"):
        hide_node(db, node_id, pipeline_id)
        n += 1
    for node_id, fn, vk in _rows(db, "SELECT node_id, function_name, variant_key FROM _pipeline_hidden_combos"):
        try:
            hide_combo(db, node_id, fn, json.loads(vk) if vk else {})
            n += 1
        except (TypeError, ValueError):
            continue
    for pipeline_id, const_name, value in _rows(
        db, "SELECT pipeline_id, const_name, value FROM _pipeline_hidden_constant_values"
    ):
        hide_parameter_values(db, const_name, [value], pipeline_id)
        n += 1
    for pipeline_id, edge_id, src, tgt, sh, th in _rows(
        db,
        "SELECT pipeline_id, edge_id, source, target, source_handle, target_handle "
        "FROM _pipeline_hidden_edges",
    ):
        hide_edge(db, edge_id, src, tgt, sh, th, pipeline_id)
        n += 1
    return n


# ---------------------------------------------------------------------------
# `wiring`: manual edges
# ---------------------------------------------------------------------------
# A manual edge is a statement that one handle feeds one parameter. Global
# scope, as `_pipeline_edges` was (edges never carried a pipeline_id — the
# nodes they join do).


def write_manual_edge(db, edge: dict) -> None:
    put_statements(
        db,
        [
            Statement(
                subject_kind=SUBJECT_EDGE,
                subject_ref=edge["id"],
                aspect=ASPECT_WIRING,
                value={
                    "source": edge.get("source", ""),
                    "target": edge.get("target", ""),
                    "sourceHandle": edge.get("sourceHandle") or edge.get("source_handle"),
                    "targetHandle": edge.get("targetHandle") or edge.get("target_handle"),
                },
                scope=GLOBAL_SCOPE,
                stated_at=_now(),
            )
        ],
    )


def delete_manual_edge(db, edge_id: str) -> int:
    return delete_statements(db, aspect=ASPECT_WIRING, subject_kind=SUBJECT_EDGE, subject_ref=edge_id)


def manual_edges(db) -> list[dict]:
    out = []
    for s in load_statements(db, aspect=ASPECT_WIRING, subject_kind=SUBJECT_EDGE):
        v = s.value or {}
        entry: dict = {"id": s.subject_ref, "source": v.get("source", ""), "target": v.get("target", "")}
        if v.get("sourceHandle") is not None:
            entry["sourceHandle"] = v["sourceHandle"]
        if v.get("targetHandle") is not None:
            entry["targetHandle"] = v["targetHandle"]
        out.append(entry)
    return out


def rename_edge_endpoints(db, old_id: str, new_id: str) -> int:
    """Re-point manual edges that name *old_id* as an endpoint (graduation)."""
    from dataclasses import replace

    n = 0
    for s in load_statements(db, aspect=ASPECT_WIRING, subject_kind=SUBJECT_EDGE):
        v = dict(s.value or {})
        changed = False
        for side in ("source", "target"):
            if v.get(side) == old_id:
                v[side] = new_id
                changed = True
        if changed:
            put_statements(db, [replace(s, value=v)])
            n += 1
    return n



def delete_edges_touching(db, node_id: str) -> int:
    """Delete manual edges with *node_id* as either endpoint — the REAL
    delete a hard-rollback of a never-valid pipeline needs (never the
    user-facing path, which hides)."""
    n = 0
    for s in load_statements(db, aspect=ASPECT_WIRING, subject_kind=SUBJECT_EDGE):
        v = s.value or {}
        if v.get("source") == node_id or v.get("target") == node_id:
            n += delete_manual_edge(db, s.subject_ref)
    return n

def _import_edges(db) -> int:
    n = 0
    for edge_id, src, tgt, sh, th in _rows(
        db, "SELECT edge_id, source, target, source_handle, target_handle FROM _pipeline_edges"
    ):
        write_manual_edge(
            db, {"id": edge_id, "source": src, "target": tgt, "sourceHandle": sh, "targetHandle": th}
        )
        n += 1
    return n


# ---------------------------------------------------------------------------
# `constants`: pending (staged) constant values
# ---------------------------------------------------------------------------


def add_pending_constant(db, const_name: str, value: str) -> None:
    put_statements(
        db,
        [
            Statement(
                subject_kind=SUBJECT_PARAMETER,
                subject_ref=const_name,
                aspect=ASPECT_CONSTANTS,
                value=True,
                key=str(value),
                scope=GLOBAL_SCOPE,
                stated_at=_now(),
            )
        ],
    )


def remove_pending_constant(db, const_name: str, value: str) -> int:
    return delete_statements(
        db,
        aspect=ASPECT_CONSTANTS,
        subject_kind=SUBJECT_PARAMETER,
        subject_ref=const_name,
        key=str(value),
    )


def pending_constants(db) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for s in load_statements(db, aspect=ASPECT_CONSTANTS, subject_kind=SUBJECT_PARAMETER):
        if s.key is not None:
            out.setdefault(s.subject_ref, set()).add(s.key)
    return out


def _import_pending_constants(db) -> int:
    n = 0
    for const_name, value in _rows(db, "SELECT constant_name, value FROM _pipeline_pending_constants"):
        add_pending_constant(db, const_name, value)
        n += 1
    return n


# ---------------------------------------------------------------------------
# `variant_selection`: a plot's named pins over variant space
# ---------------------------------------------------------------------------
# Plot Studio's Variants section edits a list of VariantSets — "baseline" is
# `Code:bandpass == v1` — that used to live only in the panel's in-session
# spec (and in source once a plot was added to the pipeline). Each set is a
# statement about the plotted VARIABLE, keyed by the set's name, so pins
# survive the panel closing and travel with the same scope and precedence as
# every other statement. There is no fact side: a pin is not something a run
# records.


def set_variant_selections(db, variable: str, variant_sets, scope: str = GLOBAL_SCOPE) -> int:
    """Replace the pins stored for *variable* with *variant_sets* (the
    ``VariantSet.to_dict()`` list a spec carries). An unnamed set is keyed by
    its position, so it round-trips without forcing a name on it."""
    delete_statements(
        db,
        aspect=ASPECT_VARIANT_SELECTION,
        subject_kind=SUBJECT_VARIABLE_TYPE,
        subject_ref=variable,
        scope=scope,
    )
    statements = []
    for i, raw in enumerate(variant_sets or []):
        if not isinstance(raw, dict):
            continue
        name = raw.get("name")
        statements.append(
            Statement(
                subject_kind=SUBJECT_VARIABLE_TYPE,
                subject_ref=variable,
                aspect=ASPECT_VARIANT_SELECTION,
                value=dict(raw),
                key=str(name) if name else f"#{i}",
                scope=scope,
                stated_at=_now(),
            )
        )
    put_statements(db, statements)
    logger.info(
        "[intent_store] %s: %d variant pin(s) stored", variable, len(statements)
    )
    return len(statements)


def variant_selections(db, variable: str, scope: str | None = None) -> list[dict]:
    """The stored pins for *variable*, in the order they were stated."""
    rows = load_statements(
        db,
        aspect=ASPECT_VARIANT_SELECTION,
        subject_kind=SUBJECT_VARIABLE_TYPE,
        subject_refs=[variable],
        scopes=[scope] if scope else None,
    )
    rows.sort(key=lambda s: (str(s.stated_at or ""), s.key or ""))
    return [dict(s.value) for s in rows if isinstance(s.value, dict)]


# ---------------------------------------------------------------------------
# Startup: every import, once
# ---------------------------------------------------------------------------

#: ``(name, aspect, importer)`` — the migration's progress bar. An aspect is
#: graduated when its importer is here and its accessors above are what
#: `pipeline_store` delegates to.
IMPORTS = (
    ("node_config_import", ASPECT_COLUMNS, _import_node_config),
    ("hidden_import", ASPECT_HIDDEN, _import_hidden),
    ("edges_import", ASPECT_WIRING, _import_edges),
    ("pending_constants_import", ASPECT_CONSTANTS, _import_pending_constants),
)


def run_imports(db) -> None:
    for name, aspect, importer in IMPORTS:
        _import_once(db, name, aspect, importer)
