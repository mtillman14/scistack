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

**Every execution-intent aspect lives here.** ``pipeline_store`` keeps its
public accessors and delegates to the functions here. The per-aspect tables
this replaced, and the one-time import from them, were removed 2026-09-23:
beta, no databases to migrate (cleanup-audit, migrations).
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

from scistack_gui.ids import PARAM_ID_PREFIX, PATH_INPUT_ID_PREFIX, VAR_ID_PREFIX

logger = logging.getLogger(__name__)

#: The `_node_config` blob key each single-key node aspect used to live under.
NODE_CONFIG_KEYS = {
    ASPECT_COLUMNS: "columnSelections",
    ASPECT_RUN_OPTIONS: "runOptions",
}

def _duck(db):
    from scistack_gui.pipeline_store import _duck as _d

    return _d(db)


def ensure_tables(db) -> None:
    """Create ``_intent`` if absent.

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
    from scistack_gui.ids import strip_placement

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
            try:
                previous = json.loads(existing[0])
            except (TypeError, ValueError):
                previous = existing[0]
            logger.info(
                "[intent_store] rekey %s -> %s: %s%s previous value %r replaced by %r",
                old_ref,
                new_ref,
                st.aspect,
                f".{st.key}" if st.key else "",
                previous,
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


def copy_subject(
    db, src_ref: str, dst_ref: str, *, src_scope: str | None = None, dst_scope: str | None = None
) -> int:
    """Copy one subject's statements onto another — what DUPLICATING a node
    does, as against :func:`rekey_subject`'s move.

    Copy, never share: the duplicate owns its rows from the moment it exists,
    so configuring it cannot reach back into the original. (Before the intent
    store, duplicating a node copied only the legacy ``_pipeline_nodes.config``
    column, so a column selection saved on the original was silently absent
    from the copy — see ``scope_service._clone_nodes``.)

    What is copied is the source AS RESOLVED in its scope (its own canvas's
    statements over legacy ``global`` ones), written at the destination's
    scope — so the copy carries everything the original ran with, as its own
    rows, and a later edit to the original does not reach it. Scopes default
    to :func:`scope_of_node` of each id.
    """
    from dataclasses import replace

    from scistack_gui.ids import strip_placement

    src_scope = src_scope or scope_of_node(db, src_ref)
    dst_scope = dst_scope or scope_of_node(db, dst_ref)
    src_ref, dst_ref = strip_placement(src_ref), strip_placement(dst_ref)
    if src_ref == dst_ref and src_scope == dst_scope:
        return 0
    resolved: dict[tuple[str, str | None], Statement] = {}
    for st in _in_scope_order(load_statements(db, subject_refs=[src_ref], scopes=[src_scope])):
        resolved[(st.aspect, st.key)] = st
    rows = [
        replace(st, subject_ref=dst_ref, scope=dst_scope) for st in resolved.values()
    ]
    put_statements(db, rows)
    if rows:
        logger.info(
            "[intent_store] copied %d statement(s): %s@%s -> %s@%s",
            len(rows),
            src_ref,
            src_scope,
            dst_ref,
            dst_scope,
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
# Helpers
# ---------------------------------------------------------------------------


def _subject_kind_for_node(node_id: str) -> str:
    """The subject kind a canvas node id names, by its prefix."""
    bare = node_id.split("::", 1)[0]
    if bare.startswith(VAR_ID_PREFIX):
        return SUBJECT_VARIABLE_TYPE
    if bare.startswith(PARAM_ID_PREFIX) or bare.startswith(PATH_INPUT_ID_PREFIX):
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


def scope_of_node(db, node_id: str) -> str:
    """The scope a statement about *node_id* is made at — the ONE owner of
    "which hypothesis was this said in".

    * a placement-qualified id (``bare::pipe_x``) carries its scope;
    * a manual node's row says which canvas it was dragged onto;
    * anything else lives on the root canvas, ``main``.

    A statement is made ON a canvas and applies on that canvas — root
    included, the same rule the `hidden` aspect has always used. ``global``
    is not a canvas: it holds the rows written before execution was
    scope-aware (2026-09-20), which applied everywhere then and still do —
    ``resolve`` walks ``scope -> global``, so a canvas's own statement
    shadows a legacy one. Decided 2026-09-20 (plan Stage 6;
    `docs/claude/decisions.md`).
    """
    from scistack_gui.ids import ROOT_SCOPE, parse_placement_id

    parsed = parse_placement_id(node_id)
    if parsed is not None:
        scope = parsed[1]
    else:
        from scistack_gui import pipeline_store

        scope = pipeline_store.manual_node_scope(db, node_id) or ROOT_SCOPE
    return scope


def scopes(db) -> list[str]:
    """Every scope any statement is made at, ``global`` first."""
    rows = _duck(db)._fetchall("SELECT DISTINCT scope FROM _intent ORDER BY scope")
    found = {r[0] for r in rows if r[0]}
    return [GLOBAL_SCOPE, *sorted(found - {GLOBAL_SCOPE})]


def set_column_selections(
    db, node_id: str, selections: dict | None, *, scope: str | None = None
) -> None:
    """Replace a call site's column selections — one statement per parameter.

    Per parameter, not one blob: the resolver's unit is a field, so a
    selection on one input can be overridden, reported or dropped without
    touching another's. ``scope`` defaults to :func:`scope_of_node`.
    """
    from scistack_gui.ids import strip_placement

    bare = strip_placement(node_id)
    scope = scope or scope_of_node(db, node_id)
    clear_aspect(db, bare, ASPECT_COLUMNS, scope)
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
                scope=scope,
            )
        )
    put_statements(db, statements)


def set_run_options(
    db, node_id: str, options: dict | None, *, scope: str | None = None
) -> None:
    """Replace a call site's run options (dry_run / save / distribute /
    as_table) — one whole-aspect statement."""
    from scistack_gui.ids import strip_placement

    bare = strip_placement(node_id)
    scope = scope or scope_of_node(db, node_id)
    clear_aspect(db, bare, ASPECT_RUN_OPTIONS, scope)
    if options:
        put_statements(
            db,
            [
                Statement(
                    subject_kind=SUBJECT_CALL_SITE,
                    subject_ref=bare,
                    aspect=ASPECT_RUN_OPTIONS,
                    value=dict(options),
                    scope=scope,
                )
            ],
        )


def set_schema_location(
    db, node_id: str, location: dict | None, *, scope: str | None = None
) -> None:
    """Replace a call site's location settings — which locations it runs at,
    which keys iterate, which where-filters apply — as ONE statement.

    One, not one per key (the shape ``set_column_selections`` uses): where a
    node runs is a single decision with three parts, and the panel always
    writes all three together from the node's own state, so they are never
    independently authored. A partial statement cannot arise, and merging an
    older one into a newer one would produce a location nobody asked for —
    which is why graduation replaces this aspect whole and logs what it
    replaced (``rekey_subject``), and why a hypothesis's statement shadows
    root's whole, never key by key.
    """
    from scistack_gui.ids import strip_placement

    bare = strip_placement(node_id)
    scope = scope or scope_of_node(db, node_id)
    clear_aspect(db, bare, ASPECT_SCHEMA_LOCATION, scope)
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
                    scope=scope,
                )
            ],
        )


def _set_schema_location_from_blob(db, node_id: str, blob_part, *, scope=None) -> None:
    """Adapter for the SETTERS table: the blob carries the three location
    keys at top level, so the writer receives the whole blob."""
    set_schema_location(db, node_id, blob_part, scope=scope)


#: ``{aspect: writer}`` for the graduated aspects — how a ``_node_config``
#: blob becomes statements. One entry per aspect as each graduates, so
#: ``update_node_config`` never grows an ``if`` per aspect.
SETTERS = {
    ASPECT_COLUMNS: set_column_selections,
    ASPECT_RUN_OPTIONS: set_run_options,
    ASPECT_SCHEMA_LOCATION: _set_schema_location_from_blob,
}


def split_node_config(db, node_id: str, config: dict, *, scope: str | None = None) -> dict:
    """Write every graduated aspect of *config* as statements and return what
    is LEFT for the blob (nothing, once every aspect has moved).

    The `update_node_config` seam calls this. `schema_location` is three
    top-level blob keys, so it is gathered rather than popped by one name.
    The scope is resolved ONCE here (:func:`scope_of_node`) so the three
    writers cannot disagree about where the panel's one save lands.
    """
    scope = scope or scope_of_node(db, node_id)
    rest = dict(config)
    if NODE_CONFIG_KEYS[ASPECT_COLUMNS] in rest:
        set_column_selections(db, node_id, rest.pop(NODE_CONFIG_KEYS[ASPECT_COLUMNS]), scope=scope)
    if NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS] in rest:
        set_run_options(db, node_id, rest.pop(NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS]), scope=scope)
    if any(k in rest for k in SCHEMA_LOCATION_KEYS):
        set_schema_location(
            db, node_id, {k: rest.pop(k) for k in SCHEMA_LOCATION_KEYS if k in rest}, scope=scope
        )
    return rest


def _in_scope_order(statements: list[Statement]) -> list[Statement]:
    """Global statements first, the scope's own last — so a dict built by
    iterating them ends with the scope's value: ``resolve``'s
    ``scope -> global`` walk, applied to a whole overlay at once."""
    return sorted(statements, key=lambda s: s.scope != GLOBAL_SCOPE)


def node_config_overlay(db, scope: str = GLOBAL_SCOPE) -> dict[str, dict]:
    """``{bare_node_id: {blob_key: value}}`` — every graduated aspect, in the
    blob spelling the panel reads, RESOLVED for *scope* (a canvas id, root
    being ``main``): a statement made on that canvas shadows a legacy
    ``global`` one; `columns` per parameter, `runOptions` and the location
    whole. ``scope=global`` reads the legacy rows alone.

    One query per aspect for the whole graph, never one per node.
    """
    out: dict[str, dict] = {}
    wanted = [scope or GLOBAL_SCOPE]  # load_statements adds `global` itself
    for st in _in_scope_order(
        load_statements(db, aspect=ASPECT_COLUMNS, subject_kind=SUBJECT_CALL_SITE, scopes=wanted)
    ):
        if st.key and st.value:
            out.setdefault(st.subject_ref, {}).setdefault(
                NODE_CONFIG_KEYS[ASPECT_COLUMNS], {}
            )[st.key] = st.value
    for st in _in_scope_order(
        load_statements(
            db, aspect=ASPECT_RUN_OPTIONS, subject_kind=SUBJECT_CALL_SITE, scopes=wanted
        )
    ):
        if st.value:
            out.setdefault(st.subject_ref, {})[NODE_CONFIG_KEYS[ASPECT_RUN_OPTIONS]] = st.value
    for st in _in_scope_order(
        load_statements(
            db, aspect=ASPECT_SCHEMA_LOCATION, subject_kind=SUBJECT_CALL_SITE, scopes=wanted
        )
    ):
        if isinstance(st.value, dict):
            entry = out.setdefault(st.subject_ref, {})
            for k in SCHEMA_LOCATION_KEYS:
                entry.pop(k, None)  # the whole aspect is replaced, never merged
            entry.update({k: v for k, v in st.value.items() if k in SCHEMA_LOCATION_KEYS})
    return out


def node_config_overlay_every_scope(db) -> dict[str, dict]:
    """The overlay for EVERY scope at once, keyed by the id a node shows
    under in that scope: root's (``main`` over legacy ``global``) under the
    bare id, another canvas's own rows under ``bare::scope``.

    For the readers that match by function NAME across the whole project
    (``execution_service.column_selections_for_nodes`` with a name, the
    export paths' diagnostics); a canvas or a run knows its scope and asks
    :func:`node_config_overlay` for it.
    """
    from scistack_gui.ids import ROOT_SCOPE, placement_id

    out = dict(node_config_overlay(db, ROOT_SCOPE))
    for scope in scopes(db):
        if scope in (GLOBAL_SCOPE, ROOT_SCOPE):
            continue
        for bare, cfg in node_config_overlay(db, scope).items():
            if cfg != out.get(bare):
                out[placement_id(bare, scope)] = cfg
    return out


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
