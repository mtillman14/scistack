"""
DuckDB-backed store for manually-declared pipeline nodes and edges.

Replaces the manual_nodes / manual_edges sections of the JSON layout file so
that DuckDB is the single source of truth for pipeline structure.  Node
positions (x/y) remain in the JSON file as cosmetic data only.

Tables created in the user's .duckdb file:

    _pipelines      (pipeline_id, name)
    _pipeline_nodes (node_id, node_type, label, config, pipeline_id)
    _pipeline_edges (edge_id, source, target, source_handle, target_handle)
    _pipeline_uses  (use_id, parent_pipeline_id, child_pipeline_id, binding_json)
    _hypotheses     (pipeline_id, research_question, hypothesis_statement,
                      evidence_for, evidence_against)

These tables are created lazily on first access so they are always present
regardless of whether init_db() or configure_database() was used to open the DB.

Nested pipelines (GUI stage, plan-gui-nested-pipelines.md)
----------------------------------------------------------
Every node belongs to exactly one pipeline SCOPE (``pipeline_id``); the
reserved root scope is ``main`` and always exists.  A pipeline placed as a
node on a parent canvas is one ``_pipeline_uses`` row: the canvas node's
``node_id`` IS the ``use_id`` (``node_type='pipelineNode'``), so the same
child pipeline placed twice — with different bindings = two backend
variants — is two nodes.  ``binding_json`` holds ``{key_map, params,
iterate}`` (empty = identity), mirroring scidb's ``Pipeline.bind()``.

These tables are the GUI's DOCUMENT (what the user drew) — NOT backend
spec persistence, which is deliberately unbuilt: at run time the GUI
constructs in-session ``scidb.Pipeline`` objects from this document.

Hypothesis tabs
---------------
A "hypothesis" is not a separate structure — it is a top-level pipeline
scope (a row in ``_pipelines``) tagged with a row in ``_hypotheses``
(research question, hypothesis statement, evidence for/against). The
reserved root scope (``main``) is tagged as a hypothesis too, the same as
any other — it is simply the default one, not a special "scratch" scope.
A pipeline used purely as a submodule (placed via ``_pipeline_uses`` and
never tagged) has no ``_hypotheses`` row and does not appear as a tab.

Edges carry no scope column: an edge lives in the scope of the nodes it
connects (both endpoints are always in one scope; service-level queries
filter edges via node membership).

Migration
---------
On first access (detected by the migration sentinel key in the JSON layout),
any manual_nodes and manual_edges entries in the JSON are written to the DB
and removed from the JSON.  This is a one-time, idempotent operation.

``_ensure_tables`` itself creates every table with its final schema
directly — no ALTER-TABLE migration steps. This is a beta project with no
installed base to migrate (see ``feedback_beta_no_deprecation`` in
project memory); once real databases exist that need a schema change,
add a guarded ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` at that point.
"""

import json
import logging
import uuid
from pathlib import Path

from scistack_gui.ids import ROOT_SCOPE

logger = logging.getLogger(__name__)

_MIGRATION_SENTINEL = "pipeline_db_migrated"

# The reserved root scope is ``scistack_gui.ids.ROOT_SCOPE``.


def _duck(db):
    """Return the SciDuck instance from a DatabaseManager."""
    return db._duck


def _ensure_tables(db) -> None:
    """Create pipeline tables if they don't already exist.

    Beta project, no pre-existing databases to migrate: every table is
    created with its final schema directly rather than via CREATE-then-
    ALTER migration steps. If a schema change is ever needed against a
    real installed base, add a guarded
    ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` at that point —
    sciduckdb's ``_execute`` et al. already recover the shared connection
    from a failed autocommit statement, so a migration that fails safely
    won't cascade into breaking unrelated queries.
    """
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_nodes (
            node_id     VARCHAR PRIMARY KEY,
            node_type   VARCHAR NOT NULL,
            label       VARCHAR NOT NULL,
            config      VARCHAR DEFAULT '{}',
            pipeline_id VARCHAR NOT NULL DEFAULT 'main'
        )
    """)
    # Node config (schemaSelection / schemaLevel / whereFilters / runOptions)
    # keyed by node_id, deliberately NOT a column on _pipeline_nodes.
    #
    # _pipeline_nodes holds MANUALLY PLACED nodes, and merge_manual_nodes
    # graduates those rows into DB-derived nodes as functions acquire history.
    # Config stored there is therefore reachable only while a node has never
    # run; writing config for a graduated node (id `fn__{fn}__{call_id}`)
    # either updates nothing or manufactures a manual-node row that
    # merge_manual_nodes then renders as a duplicate. Before 2026-09-14 it was
    # the former: a bare UPDATE matching zero rows, reported as success, which
    # is why a distribute checkbox never survived a refresh.
    #
    # A table with no lifecycle coupling to node placement has neither
    # problem. See docs/claude/gui-run-options-flow.md.
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _node_config (
            node_id VARCHAR PRIMARY KEY,
            config  VARCHAR NOT NULL DEFAULT '{}'
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
        CREATE TABLE IF NOT EXISTS _pipeline_builtin_functions (
            name     VARCHAR PRIMARY KEY,
            language VARCHAR NOT NULL
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_nodes (
            pipeline_id VARCHAR NOT NULL DEFAULT 'main',
            node_id     VARCHAR NOT NULL,
            PRIMARY KEY (pipeline_id, node_id)
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_combos (
            node_id       VARCHAR PRIMARY KEY,
            function_name VARCHAR NOT NULL,
            variant_key   VARCHAR NOT NULL
        )
    """)
    # Constant-value hides (checkbox in ConstantNode.tsx) — a coarser
    # granularity than _pipeline_hidden_combos: hiding (const_name, value)
    # excludes every call site across every function that uses that pair,
    # not just one function's one Cartesian-product row. Pipeline-id scoped
    # from the start, unlike _pipeline_hidden_combos which was left globally
    # scoped (see get_hidden_node_ids docstring) as a still-deferred
    # follow-up — see .claude/plan-constant-source-of-truth-26-08-22.md
    # design item 2.
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_constant_values (
            pipeline_id VARCHAR NOT NULL DEFAULT 'main',
            const_name  VARCHAR NOT NULL,
            value       VARCHAR NOT NULL,
            PRIMARY KEY (pipeline_id, const_name, value)
        )
    """)
    # Values written in one go by ParameterSettingsPanel's Generate section
    # ("Replace values"), as opposed to added one at a time ("Add value").
    # Purely a DISPLAY grouping: the set collapses to a single compact row,
    # with one checkbox, in both the sidebar list and the canvas node.
    #
    # It lives here rather than in source because source is the flat list of
    # values in three languages, and grouping is not something a pipeline
    # means -- keeping it out of the declaration is what leaves
    # render_parameter/render_parameter_value/render_matlab_parameter,
    # version_keys and MATLAB parity untouched by this feature (CLAUDE.md
    # NOTE 3: only the GUI concern lives in the GUI layer).
    #
    # ONE group per Parameter: "Replace values" replaces every value, so a
    # generation always defines the whole set. Values added afterwards sit
    # alongside it as ordinary rows, which is why param_name is the PK.
    #
    # ``member_values`` holds the members as RENDERED STRINGS -- the same
    # form build_parameter_nodes puts on a node and
    # _pipeline_hidden_constant_values stores -- so membership, hidden state
    # and history rows all key alike. (Named ``member_values`` and not
    # ``values`` because VALUES is a SQL keyword and would need quoting at
    # every reference.) ``spec`` is kept as well as the members so the
    # compact repr can be rendered from the generation itself, and so
    # reopening Generate can re-seed its start/end/step inputs.
    #
    # Not scoped by pipeline_id: what a Parameter's values ARE does not vary
    # by scope, the same reasoning as _pipeline_path_input_history below.
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_parameter_value_groups (
            param_name    VARCHAR PRIMARY KEY,
            kind          VARCHAR NOT NULL,
            spec          VARCHAR NOT NULL DEFAULT '{}',
            member_values VARCHAR NOT NULL DEFAULT '[]'
        )
    """)
    # Templates a PathInput has been declared with and the GUI has since
    # overwritten. Append-only, never deleted.
    #
    # Run history is attributed to a canvas node by CONTENT-matching the
    # recorded template against the registry (graph_builder.
    # resolve_path_input_name), because PathInput.to_key() serialises only
    # template/root_folder and never the bound name. That content-addressing
    # is CORRECT for provenance and must not change -- it is what keeps two
    # different templates from collapsing into one provenance node. But it
    # means a GUI template edit would otherwise detach every run recorded
    # against the old value. This table is the attribution index for exactly
    # that, written from ONE place: target_file_service, just before a
    # write-back overwrites a template. A template edited directly in source
    # still detaches -- unchanged, pre-existing behaviour for a deliberate
    # hand-edit, and not what this table set out to fix.
    # See docs/claude/entity-editability-model.md Rule 2 (D7).
    #
    # Deliberately NOT scoped by pipeline_id: what a recorded template MEANT
    # does not vary by scope.
    #
    # ``root_folder`` is stored as '' rather than NULL when unset: it is part
    # of the primary key, and a NULL in a PK is both rejected by some engines
    # and never equal to itself, which would silently defeat the ON CONFLICT
    # dedup. Normalised in record_path_input_value/lookup_path_input_name.
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_path_input_history (
            name        VARCHAR NOT NULL,
            template    VARCHAR NOT NULL,
            root_folder VARCHAR NOT NULL DEFAULT '',
            PRIMARY KEY (name, template, root_folder)
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_edges (
            pipeline_id   VARCHAR NOT NULL DEFAULT 'main',
            edge_id       VARCHAR NOT NULL,
            source        VARCHAR NOT NULL,
            target        VARCHAR NOT NULL,
            source_handle VARCHAR,
            target_handle VARCHAR,
            PRIMARY KEY (pipeline_id, edge_id)
        )
    """)
    # Hidden subpipeline ports (to-do #9) — a scope's exposed inputs/
    # outputs are computed automatically from wiring (see
    # domain.scope_filter.document_interface); this table is a per-scope
    # manual override suppressing one type's port, toggled by right-
    # clicking a variable node inside the subpipeline's own canvas.
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_hidden_ports (
            pipeline_id VARCHAR NOT NULL DEFAULT 'main',
            direction   VARCHAR NOT NULL,
            var_type    VARCHAR NOT NULL,
            PRIMARY KEY (pipeline_id, direction, var_type)
        )
    """)

    # --- Nested-pipeline scoping (GUI stage) ---
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipelines (
            pipeline_id VARCHAR PRIMARY KEY,
            name        VARCHAR NOT NULL,
            hidden      BOOLEAN DEFAULT FALSE
        )
    """)
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _pipeline_uses (
            use_id             VARCHAR PRIMARY KEY,
            parent_pipeline_id VARCHAR NOT NULL,
            child_pipeline_id  VARCHAR NOT NULL,
            binding_json       VARCHAR DEFAULT '{}'
        )
    """)
    # The root scope always exists; pre-scoping nodes backfill into it.
    _duck(db)._execute(
        "INSERT INTO _pipelines (pipeline_id, name) VALUES (?, ?) "
        "ON CONFLICT DO NOTHING",
        [ROOT_SCOPE, ROOT_SCOPE],
    )

    # --- Hypothesis tabs (a hypothesis is a tagged top-level pipeline) ---
    _duck(db)._execute("""
        CREATE TABLE IF NOT EXISTS _hypotheses (
            pipeline_id          VARCHAR PRIMARY KEY,
            research_question    VARCHAR DEFAULT '',
            hypothesis_statement VARCHAR DEFAULT '',
            evidence_for         VARCHAR DEFAULT '[]',
            evidence_against     VARCHAR DEFAULT '[]'
        )
    """)
    # The root pipeline is the default hypothesis, tagged the same as any
    # other (one-time, idempotent — existing DBs backfill on next access).
    _duck(db)._execute(
        "INSERT INTO _hypotheses (pipeline_id) VALUES (?) ON CONFLICT DO NOTHING",
        [ROOT_SCOPE],
    )

    # The intent store (`_intent`): one table, one shape, for statements
    # about runs — replacing the tables above one ASPECT at a time.
    # `columns` has moved; the rest still live where they always did. See
    # docs/claude/intent-and-fact.md and scistack_gui/intent_store.py.
    from scistack_gui import intent_store

    intent_store.ensure_tables(db)


def migrate_from_json(db, layout_path: Path) -> None:
    """One-time migration: move manual_nodes/manual_edges from JSON into DB.

    Safe to call repeatedly — checks the migration sentinel before acting.
    """
    logger.info(
        "[pipeline_store] migrate_from_json called (layout_path=%s)", layout_path
    )
    _ensure_tables(db)

    if not layout_path.exists():
        logger.debug("[pipeline_store] Layout file does not exist, skipping migration")
        return

    logger.info("[pipeline_store] Loading layout JSON file")
    with layout_path.open() as f:
        try:
            data = json.load(f)
        except Exception:
            logger.debug("[pipeline_store] Failed to parse JSON, skipping migration")
            return

    if data.get(_MIGRATION_SENTINEL):
        logger.debug(
            "[pipeline_store] Migration already completed (sentinel found), skipping"
        )
        return  # Already migrated.

    logger.info("[pipeline_store] Migrating manual_nodes and manual_edges to DuckDB")
    manual_nodes: dict = data.get("manual_nodes", {})
    manual_edges: list = data.get("manual_edges", [])
    logger.debug(
        "[pipeline_store] Found %d manual nodes and %d manual edges in JSON",
        len(manual_nodes),
        len(manual_edges),
    )

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
            write_manual_edge(db, edge)
            migrated_edges += 1

    logger.info(
        "[pipeline_store] Writing migration sentinel to JSON and removing migrated data"
    )
    # Clear migrated keys from JSON and write sentinel.
    data.pop("manual_nodes", None)
    data.pop("manual_edges", None)
    data[_MIGRATION_SENTINEL] = True
    with layout_path.open("w") as f:
        json.dump(data, f, indent=2)

    logger.info(
        "[pipeline_store] Migration complete - migrated %d nodes and %d edges from JSON to DuckDB",
        migrated_nodes,
        migrated_edges,
    )


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def manual_node_scope(db, node_id: str) -> "str | None":
    """The canvas a manual node was dragged onto, or ``None`` if *node_id*
    is not a manual node. One row, not the whole manual-node map — this is
    on the path of every config read (``intent_store.scope_of_node``).

    Deliberately NOT ``_ensure_tables``: that runs the intent store's one-time
    import, whose node-config importer asks this very question for each
    legacy row — the import would re-enter itself. A database with no
    ``_pipeline_nodes`` yet has no manual nodes, which is the ``None`` answer.
    """
    try:
        row = _duck(db)._fetchone(
            "SELECT pipeline_id FROM _pipeline_nodes WHERE node_id = ?", [node_id]
        )
    except Exception:  # the table does not exist yet
        return None
    if row is None:
        return None
    return row[0] or ROOT_SCOPE


def get_manual_nodes(db, pipeline_id: "str | None" = None) -> dict[str, dict]:
    """Return {node_id: {"type", "label", "pipeline_id"[, "config"]}}.

    ``pipeline_id=None`` returns ALL scopes (pre-scoping behavior — every
    existing caller keeps working); pass a scope to filter to one canvas.
    """
    _ensure_tables(db)
    if pipeline_id is None:
        rows = _duck(db)._fetchall(
            "SELECT node_id, node_type, label, config, pipeline_id FROM _pipeline_nodes"
        )
    else:
        rows = _duck(db)._fetchall(
            "SELECT node_id, node_type, label, config, pipeline_id "
            "FROM _pipeline_nodes WHERE pipeline_id = ?",
            [pipeline_id],
        )
    # _node_config is the live store; the row's own `config` column is the
    # legacy one. Overlaying here rather than at each call site keeps every
    # existing consumer (export, copy/paste, graph build) reading one
    # "config" key that means the same thing everywhere -- otherwise each
    # would have to learn about the split and one of them would be missed.
    # Resolved per scope: a manual node's statements are made on the canvas
    # it was dragged onto (intent_store.scope_of_node), so its config is the
    # overlay for THAT scope — one overlay per scope, not one per node.
    overrides_by_scope: dict[str, dict] = {}

    def _overrides(scope: str) -> dict:
        if scope not in overrides_by_scope:
            overrides_by_scope[scope] = get_node_configs(db, scope)
        return overrides_by_scope[scope]

    result = {}
    for row in rows:
        entry: dict = {
            "type": row[1],
            "label": row[2],
            "pipeline_id": row[4] or ROOT_SCOPE,
        }
        overrides = _overrides(entry["pipeline_id"])
        if row[3] and row[3] != "{}":
            try:
                entry["config"] = json.loads(row[3])
            except (json.JSONDecodeError, TypeError):
                pass
        override = overrides.get(row[0])
        if override:
            entry["config"] = override
        result[row[0]] = entry
    return result


def write_builtin_function(db, name: str, language: str) -> None:
    """Persist a manually-declared built-in/library function reference
    (e.g. ``numpy.mean``, MATLAB ``mean``) so it survives registry
    refreshes and server restarts — unlike file-based functions, there's
    no source file on disk to rediscover it from."""
    logger.info(
        "[pipeline_store] write_builtin_function called (name=%r, language=%r)",
        name,
        language,
    )
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _pipeline_builtin_functions (name, language) VALUES (?, ?) "
        "ON CONFLICT (name) DO UPDATE SET language = excluded.language",
        [name, language],
    )


def get_builtin_functions(db) -> list[dict]:
    """Return every persisted built-in/library function reference as
    ``[{"name": ..., "language": ...}, ...]``."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall("SELECT name, language FROM _pipeline_builtin_functions")
    return [{"name": row[0], "language": row[1]} for row in rows]


def write_manual_node(
    db, node_id: str, node_type: str, label: str, pipeline_id: str = ROOT_SCOPE
) -> None:
    logger.info(
        "[pipeline_store] write_manual_node called (node_id=%r, type=%r, "
        "label=%r, pipeline_id=%r)",
        node_id,
        node_type,
        label,
        pipeline_id,
    )
    _ensure_tables(db)
    logger.info("[pipeline_store] Upserting node into _pipeline_nodes table")
    _upsert_node(db, node_id, node_type, label, pipeline_id)
    logger.info("[pipeline_store] Node written to DuckDB successfully")


def update_node_config(db, node_id: str, config: dict) -> None:
    """Write a node's saved config (schemaSelection, schemaLevel, whereFilters,
    runOptions), keyed by ``node_id``.

    An **upsert** into ``_node_config``, for any node id -- manual
    (``fn__{fn}``) or DB-derived (``fn__{fn}__{call_id}``) alike. It used to be
    a bare ``UPDATE _pipeline_nodes ... WHERE node_id = ?``, which matched zero
    rows for every node that had ever run and reported success anyway; the
    frontend cleared its dirty flag and the setting was gone on the next
    rebuild. See docs/claude/gui-run-options-flow.md.
    """
    _ensure_tables(db)
    logger.info(
        "[pipeline_store] update_node_config (node_id=%r, keys=%s)",
        node_id,
        sorted(config),
    )
    # Aspects that have graduated to the intent store are written THERE and
    # stripped from the blob, so there is exactly one owner per aspect and
    # the two can never disagree. The read paths below put them back for
    # display. See scistack_gui/intent_store.py.
    from scistack_gui import intent_store

    config = intent_store.split_node_config(db, node_id, dict(config))

    _duck(db)._execute(
        """
        INSERT INTO _node_config (node_id, config) VALUES (?, ?)
        ON CONFLICT (node_id) DO UPDATE SET config = excluded.config
        """,
        [node_id, json.dumps(config)],
    )


def get_node_config(db, node_id: str) -> dict:
    """One node's saved config, or ``{}``.

    Falls back to the legacy ``_pipeline_nodes.config`` column so configs saved
    before the ``_node_config`` split are not orphaned. Nothing writes that
    column any more.
    """
    _ensure_tables(db)
    row = _duck(db)._fetchone(
        "SELECT config FROM _node_config WHERE node_id = ?", [node_id]
    )
    if row is None:
        row = _duck(db)._fetchone(
            "SELECT config FROM _pipeline_nodes WHERE node_id = ?", [node_id]
        )
    config: dict = {}
    if row is not None and row[0]:
        try:
            config = json.loads(row[0]) or {}
        except (ValueError, TypeError):
            logger.warning(
                "[pipeline_store] node %r has unparseable config JSON — ignoring it",
                node_id,
            )
            config = {}
    return _with_graduated_aspects(db, node_id, config)


def _with_graduated_aspects(db, node_id: str, config: dict) -> dict:
    """*config* plus the aspects now stored in ``_intent``.

    The panel reads a node's settings as one blob, and it still does. What
    changed is where each part is kept: an aspect that has graduated is read
    from the intent store and put back here for DISPLAY only — the blob is
    never the source of truth for it again, and nothing writes it back (see
    ``update_node_config``, which strips it on the way in).
    """
    from scistack_gui import intent_store
    from scistack_gui.ids import strip_placement

    # Resolved for the scope the id names (a hypothesis's own statements over
    # root's) — one overlay for one node; the whole-graph readers ask for
    # the whole overlay once (`get_node_configs`).
    scope = intent_store.scope_of_node(db, node_id)
    out = dict(config)
    out.update(intent_store.node_config_overlay(db, scope).get(strip_placement(node_id), {}))
    return out


def get_node_configs(db, pipeline_id: "str | None" = None) -> dict[str, dict]:
    """``{node_id: config}`` for every node that has one, legacy column
    included (``_node_config`` wins where both exist).

    With *pipeline_id*, the graduated aspects are RESOLVED for that scope
    and keyed by the bare id — what a canvas build wants. Without it they
    come for every scope at once, a hypothesis's own rows keyed
    ``bare::scope`` (``intent_store.node_config_overlay_every_scope``) — for
    the readers that match by function name across the project.
    """
    _ensure_tables(db)
    configs: dict[str, dict] = {}
    for table in ("_pipeline_nodes", "_node_config"):
        for node_id, raw in _duck(db)._fetchall(
            f"SELECT node_id, config FROM {table}"  # noqa: S608 - fixed literals
        ):
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                logger.warning(
                    "[pipeline_store] node %r has unparseable config JSON — "
                    "ignoring it",
                    node_id,
                )
                continue
            if parsed:
                configs[node_id] = parsed

    # Graduated aspects, merged back in for display — including for nodes
    # that have NO `_node_config` row at all, which is the normal case once
    # an aspect has moved out of the blob entirely.
    from scistack_gui import intent_store

    overlay_by_node = (
        intent_store.node_config_overlay(db, pipeline_id)
        if pipeline_id
        else intent_store.node_config_overlay_every_scope(db)
    )
    for node_id, overlay in overlay_by_node.items():
        configs.setdefault(node_id, {}).update(overlay)

    logger.debug("[pipeline_store] loaded config for %d node(s)", len(configs))
    return configs


def delete_node(db, node_id: str) -> None:
    logger.info("[pipeline_store] delete_node called (node_id=%r)", node_id)
    _duck(db)._execute("DELETE FROM _pipeline_nodes WHERE node_id = ?", [node_id])
    logger.info("[pipeline_store] Node deleted from _pipeline_nodes table")


def move_pipeline_use_parent(db, use_id: str, new_parent_pipeline_id: str) -> None:
    """Move an existing pipeline-node PLACEMENT to a new parent scope
    (extract-to-submodule regrouping a selection that includes one).

    A placed pipeline node's rendering scope is ``_pipeline_uses.
    parent_pipeline_id`` (see api/pipeline.py's ``_build_graph`` docstring
    — pipelineNode entries come from ``scope_service.build_pipeline_nodes``,
    driven by this column), NOT its ``_pipeline_nodes.pipeline_id`` — the
    two must move together (pair with move_node_scope) or the node ends up
    inconsistent: present in the new scope's node list but still rendered
    as a use of the old parent.
    """
    _duck(db)._execute(
        "UPDATE _pipeline_uses SET parent_pipeline_id = ? WHERE use_id = ?",
        [new_parent_pipeline_id, use_id],
    )


def move_node_scope(db, node_id: str, new_pipeline_id: str) -> None:
    """Move an existing node's scope (extract-to-submodule).

    A no-op if ``node_id`` has no ``_pipeline_nodes`` row (a pure
    DB-derived node whose scope is entirely position-based — the caller
    also moves its saved position, which is what actually re-scopes it).
    """
    _duck(db)._execute(
        "UPDATE _pipeline_nodes SET pipeline_id = ? WHERE node_id = ?",
        [new_pipeline_id, node_id],
    )


def rename_edge_endpoints(db, old_id: str, new_id: str) -> None:
    """Rewrite any manual edges referencing old_id to point to new_id
    instead of becoming dangling — shared by graduation and by re-keying
    an already-placement-qualified node moved to a new scope (extraction).
    """
    from scistack_gui import intent_store

    intent_store.rename_edge_endpoints(db, old_id, new_id)


def migrate_node_config(db, old_id: str, new_id: str) -> dict:
    """Move a node's saved settings from *old_id* to *new_id* — the third
    thing graduation has to carry across, beside position and edges.

    Sources, in increasing precedence: the legacy ``_pipeline_nodes.config``
    column, then ``_node_config`` rows whose bare id is *old_id*'s (a fresh
    node's config is usually keyed bare, sometimes ``::scope``-qualified).
    The merged result is written OVER whatever *new_id* already has — the
    fresh node's settings win: the realistic route to a conflict is a wired
    fresh node the user configured and ran, whose settings produced the very
    history it graduates into, so after graduation the node must run the way
    it just ran (``.claude/plan-graduation-config-migration.md``). Values
    that get replaced are logged verbatim, so nothing is silently gone.

    The migrated ``_node_config`` row(s) are renamed away: this is a move —
    the content lives on under *new_id* — and leaving the source row would
    keep ``apply_placement_configs``' orphan WARN firing forever for a
    setting that did rehydrate. Must run BEFORE the ``_pipeline_nodes`` row
    is deleted, or the legacy column is gone.

    Returns ``{"moved": [keys], "replaced": {key: previous_value}}``; both
    empty when there was nothing to move.
    """
    from scistack_gui.ids import strip_placement

    _ensure_tables(db)
    old_bare = strip_placement(old_id)

    def _parse(raw, label):
        if not raw:
            return {}
        try:
            return json.loads(raw) or {}
        except (ValueError, TypeError):
            logger.warning(
                "[pipeline_store] %s has unparseable config JSON — not migrated", label
            )
            return {}

    merged: dict = {}
    legacy = _duck(db)._fetchone(
        "SELECT config FROM _pipeline_nodes WHERE node_id = ?", [old_id]
    )
    if legacy is not None:
        merged.update(_parse(legacy[0], f"legacy row {old_id!r}"))
    source_rows: list[str] = []
    for nid, raw in _duck(db)._fetchall("SELECT node_id, config FROM _node_config"):
        if strip_placement(nid) != old_bare:
            continue
        source_rows.append(nid)
    # Bare first, qualified after, so a scope-specific save wins over a bare one.
    for nid in sorted(source_rows, key=lambda n: (n != old_bare, n)):
        raw = _duck(db)._fetchone(
            "SELECT config FROM _node_config WHERE node_id = ?", [nid]
        )
        merged.update(_parse(raw[0] if raw else None, f"config row {nid!r}"))

    if not merged:
        logger.debug(
            "[pipeline_store] graduation %s -> %s: no saved config to move",
            old_id,
            new_id,
        )
        return {"moved": [], "replaced": {}}

    target = get_node_config(db, new_id)
    replaced = {k: target[k] for k in merged if k in target and target[k] != merged[k]}
    update_node_config(db, new_id, {**target, **merged})
    for nid in source_rows:
        _duck(db)._execute("DELETE FROM _node_config WHERE node_id = ?", [nid])

    logger.info(
        "[pipeline_store] graduation: config keys %s moved from %s to %s",
        sorted(merged),
        old_id,
        new_id,
    )
    if replaced:
        logger.info(
            "[pipeline_store] graduation: %s's previous values replaced by the "
            "fresh node's — previous: %s",
            new_id,
            replaced,
        )
    return {"moved": sorted(merged), "replaced": replaced}


def graduate_manual_node(db, old_id: str, new_id: str) -> None:
    """Remove the manual node entry for old_id (the DB-derived node takes over).

    Also rewrites any manual edges that reference old_id so they point to
    new_id instead of becoming dangling, and moves the node's saved config
    across (migrate_node_config — before the row delete, which would take
    the legacy config column with it).
    """
    migrate_node_config(db, old_id, new_id)
    # The same move, for every aspect that has graduated to the intent store
    # — ONE call rather than one migration per table, which is the point of
    # keying statements by a subject rather than by whatever id a table
    # happened to use.
    from scistack_gui import intent_store

    intent_store.rekey_subject(db, old_id, new_id)
    _duck(db)._execute("DELETE FROM _pipeline_nodes WHERE node_id = ?", [old_id])
    rename_edge_endpoints(db, old_id, new_id)


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------


def get_manual_edges(db) -> list[dict]:
    """Return all manual edges as a list of dicts (the `wiring` aspect of the
    intent store — `_pipeline_edges` no longer owns them)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.manual_edges(db)


def write_manual_edge(db, edge: dict) -> None:
    logger.info(
        "[pipeline_store] write_manual_edge called (edge_id=%r, source=%r, target=%r, source_handle=%r, target_handle=%r)",
        edge.get("id"),
        edge.get("source"),
        edge.get("target"),
        edge.get("sourceHandle") or edge.get("source_handle"),
        edge.get("targetHandle") or edge.get("target_handle"),
    )
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.write_manual_edge(db, edge)
    logger.info("[pipeline_store] Edge written as a wiring statement")


def delete_manual_edge(db, edge_id: str) -> None:
    logger.info("[pipeline_store] delete_manual_edge called (edge_id=%r)", edge_id)
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.delete_manual_edge(db, edge_id)
    logger.info("[pipeline_store] Edge statement deleted")


# ---------------------------------------------------------------------------
# Pending constants
# ---------------------------------------------------------------------------


def add_pending_constant(db, const_name: str, value: str) -> None:
    logger.info(
        "[pipeline_store] add_pending_constant called (const_name=%r, value=%r)",
        const_name,
        value,
    )
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.add_pending_constant(db, const_name, value)
    logger.info("[pipeline_store] Pending constant added as a constants statement")


def remove_pending_constant(db, const_name: str, value: str) -> None:
    logger.info(
        "[pipeline_store] remove_pending_constant called (const_name=%r, value=%r)",
        const_name,
        value,
    )
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.remove_pending_constant(db, const_name, value)
    logger.info("[pipeline_store] Pending constant statement removed")


def get_pending_constants(db) -> dict[str, set[str]]:
    """Return {constant_name: {value, ...}} for all pending constant values
    (the `constants` aspect of the intent store)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.pending_constants(db)


# ---------------------------------------------------------------------------
# Pipelines (nested-pipeline scopes)
# ---------------------------------------------------------------------------


def list_pipelines(db) -> list[dict]:
    """All VISIBLE pipeline scopes: [{"pipeline_id", "name"}], root first.
    Hidden pipelines (see hide_pipeline) are excluded — use
    list_hidden_pipelines for those."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT pipeline_id, name FROM _pipelines WHERE NOT hidden "
        "ORDER BY (pipeline_id != ?), name",
        [ROOT_SCOPE],
    )
    return [{"pipeline_id": r[0], "name": r[1]} for r in rows]


def list_all_pipelines(db) -> list[dict]:
    """Every pipeline scope regardless of hidden state:
    [{"pipeline_id", "name", "hidden"}], root first. Used where a name
    collision must be avoided against hidden pipelines too (see
    ``list_pipelines``'s docstring and ``create_pipeline``'s uniqueness
    check, which already does this)."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT pipeline_id, name, hidden FROM _pipelines "
        "ORDER BY (pipeline_id != ?), name",
        [ROOT_SCOPE],
    )
    return [{"pipeline_id": r[0], "name": r[1], "hidden": bool(r[2])} for r in rows]


def get_pipeline(db, pipeline_id: str) -> "dict | None":
    """Single pipeline scope by id, regardless of hidden state —
    {"pipeline_id", "name", "hidden"}, or None if no such pipeline
    exists locally."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT pipeline_id, name, hidden FROM _pipelines WHERE pipeline_id = ?",
        [pipeline_id],
    )
    if not rows:
        return None
    r = rows[0]
    return {"pipeline_id": r[0], "name": r[1], "hidden": bool(r[2])}


def create_pipeline(db, name: str, pipeline_id: "str | None" = None) -> str:
    """Create a new (empty) pipeline scope; returns its pipeline_id.

    ``pipeline_id``: explicit id to use instead of minting a fresh one —
    used by pipeline import to PRESERVE a document's portable identity
    (see ``portability_service._resolve_pipeline``) rather than always
    generating a new one. Defaults to minting fresh, as before.
    """
    _ensure_tables(db)
    name = str(name).strip()
    if not name:
        raise ValueError("pipeline name must be non-empty")
    # Uniqueness is checked against ALL pipelines, hidden included — two
    # pipelines sharing a name would be confusing the moment either is
    # unhidden, even if only one is visible right now.
    existing = {
        r[0] for r in _duck(db)._fetchall("SELECT name FROM _pipelines")
    }
    if name in existing:
        raise ValueError(f"a pipeline named '{name}' already exists")
    pipeline_id = pipeline_id or f"pipe_{uuid.uuid4().hex[:12]}"
    _duck(db)._execute(
        "INSERT INTO _pipelines (pipeline_id, name) VALUES (?, ?)",
        [pipeline_id, name],
    )
    logger.info("[pipeline_store] create_pipeline: '%s' -> %s", name, pipeline_id)
    return pipeline_id


def rename_pipeline(db, pipeline_id: str, name: str) -> None:
    """Rename any pipeline scope, including the root — 'main' is just the
    default hypothesis, not a special scratch scope (see module docstring)."""
    _ensure_tables(db)
    name = str(name).strip()
    if not name:
        raise ValueError("pipeline name must be non-empty")
    _duck(db)._execute(
        "UPDATE _pipelines SET name = ? WHERE pipeline_id = ?",
        [name, pipeline_id],
    )
    # Pipeline-node labels on parent canvases display the child's name.
    _duck(db)._execute(
        "UPDATE _pipeline_nodes SET label = ? WHERE node_id IN "
        "(SELECT use_id FROM _pipeline_uses WHERE child_pipeline_id = ?)",
        [name, pipeline_id],
    )


def _hard_delete_pipeline(db, pipeline_id: str) -> None:
    """Internal-only, REAL delete of a pipeline scope and its contents.

    Not the user-facing "delete" operation (see hide_pipeline for that —
    per project ethos, user-facing removal never deletes data). This exists
    only to roll back a pipeline that never became valid, e.g.
    duplicate_pipeline's post-copy compile-sanity-check failure: there is no
    user-visible content to preserve, so a real delete is correct here.
    """
    _ensure_tables(db)
    node_rows = _duck(db)._fetchall(
        "SELECT node_id FROM _pipeline_nodes WHERE pipeline_id = ?",
        [pipeline_id],
    )
    node_ids = [r[0] for r in node_rows]
    from scistack_gui import intent_store

    for nid in node_ids:
        intent_store.delete_edges_touching(db, nid)
    _duck(db)._execute(
        "DELETE FROM _pipeline_nodes WHERE pipeline_id = ?", [pipeline_id]
    )
    _duck(db)._execute(
        "DELETE FROM _pipeline_uses WHERE parent_pipeline_id = ?", [pipeline_id]
    )
    _duck(db)._execute("DELETE FROM _pipelines WHERE pipeline_id = ?", [pipeline_id])
    logger.info(
        "[pipeline_store] _hard_delete_pipeline: %s (%d node(s) removed)",
        pipeline_id,
        len(node_ids),
    )


def hide_pipeline(db, pipeline_id: str) -> None:
    """Hide a pipeline scope (user-facing "delete") without touching its
    contents — never delete data, per project ethos (see hide_node/
    hide_edge/hide_combo for the same pattern at node/edge granularity).

    Refuses any pipeline still placed on another canvas (remove those
    pipeline nodes first — fail fast beats a canvas silently pointing at
    invisible content) and refuses to hide the last remaining VISIBLE
    pipeline — 'main' has no special protection beyond that; it is simply
    the default hypothesis (see module docstring). Positions are left
    intact so unhide_pipeline fully restores the canvas.
    """
    _ensure_tables(db)
    consumers = _duck(db)._fetchall(
        "SELECT p.name FROM _pipeline_uses u JOIN _pipelines p "
        "ON p.pipeline_id = u.parent_pipeline_id WHERE u.child_pipeline_id = ?",
        [pipeline_id],
    )
    if consumers:
        names = sorted({r[0] for r in consumers})
        raise ValueError(
            f"pipeline is still used by {names} — remove those pipeline nodes first"
        )
    visible_count = _duck(db)._fetchall(
        "SELECT COUNT(*) FROM _pipelines WHERE NOT hidden"
    )[0][0]
    if visible_count <= 1:
        raise ValueError("cannot hide the last remaining pipeline")
    _duck(db)._execute(
        "UPDATE _pipelines SET hidden = TRUE WHERE pipeline_id = ?", [pipeline_id]
    )
    logger.info("[pipeline_store] hide_pipeline: %s", pipeline_id)


def unhide_pipeline(db, pipeline_id: str) -> None:
    _ensure_tables(db)
    _duck(db)._execute(
        "UPDATE _pipelines SET hidden = FALSE WHERE pipeline_id = ?", [pipeline_id]
    )
    logger.info("[pipeline_store] unhide_pipeline: %s", pipeline_id)


def list_hidden_pipelines(db) -> list[dict]:
    """Hidden pipelines: [{"pipeline_id", "name", "is_hypothesis"}] — the
    restore panel's data (both the hypothesis-tab strip and the plain
    Submodules list draw from this same set)."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT p.pipeline_id, p.name, h.pipeline_id IS NOT NULL "
        "FROM _pipelines p LEFT JOIN _hypotheses h ON h.pipeline_id = p.pipeline_id "
        "WHERE p.hidden ORDER BY p.name"
    )
    return [
        {"pipeline_id": r[0], "name": r[1], "is_hypothesis": bool(r[2])}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Pipeline uses (pipeline-as-node edges between scopes)
# ---------------------------------------------------------------------------


def _uses_reachable(db, start_id: str) -> set:
    """Pipeline ids reachable from start_id through _pipeline_uses edges."""
    reachable: set = set()
    frontier = [start_id]
    while frontier:
        current = frontier.pop()
        rows = _duck(db)._fetchall(
            "SELECT child_pipeline_id FROM _pipeline_uses WHERE parent_pipeline_id = ?",
            [current],
        )
        for (child,) in rows:
            if child not in reachable:
                reachable.add(child)
                frontier.append(child)
    return reachable


def add_pipeline_use(
    db, parent_pipeline_id: str, child_pipeline_id: str, binding: "dict | None" = None
) -> str:
    """Place ``child`` as a pipeline NODE on ``parent``'s canvas.

    One _pipeline_uses row + one canvas node whose node_id IS the use_id
    (same child twice = two nodes; bindings live on the use edge — G1).
    Rejects cycles at creation (mirrors scidb's PipelineCycleError).
    Returns the use_id.
    """
    _ensure_tables(db)
    known = {p["pipeline_id"] for p in list_pipelines(db)}
    for pid in (parent_pipeline_id, child_pipeline_id):
        if pid not in known:
            raise ValueError(f"unknown pipeline_id '{pid}'")
    if child_pipeline_id == parent_pipeline_id or parent_pipeline_id in _uses_reachable(
        db, child_pipeline_id
    ):
        raise ValueError(
            f"placing this pipeline would create a dependency cycle "
            f"('{child_pipeline_id}' already reaches '{parent_pipeline_id}')"
        )
    use_id = f"use_{uuid.uuid4().hex[:12]}"
    _duck(db)._execute(
        "INSERT INTO _pipeline_uses "
        "(use_id, parent_pipeline_id, child_pipeline_id, binding_json) "
        "VALUES (?, ?, ?, ?)",
        [use_id, parent_pipeline_id, child_pipeline_id, json.dumps(binding or {})],
    )
    child_name = next(
        p["name"] for p in list_pipelines(db) if p["pipeline_id"] == child_pipeline_id
    )
    _upsert_node(db, use_id, "pipelineNode", child_name, parent_pipeline_id)
    logger.info(
        "[pipeline_store] add_pipeline_use: '%s' placed on '%s' "
        "(use_id=%s, binding=%s)",
        child_pipeline_id,
        parent_pipeline_id,
        use_id,
        binding or {},
    )
    return use_id


def remove_pipeline_use(db, use_id: str) -> None:
    """Remove a pipeline node: the use row, its canvas node, its edges."""
    _ensure_tables(db)
    _duck(db)._execute("DELETE FROM _pipeline_uses WHERE use_id = ?", [use_id])
    _duck(db)._execute("DELETE FROM _pipeline_nodes WHERE node_id = ?", [use_id])
    from scistack_gui import intent_store

    intent_store.delete_edges_touching(db, use_id)
    logger.info("[pipeline_store] remove_pipeline_use: %s", use_id)


def get_pipeline_uses(db, parent_pipeline_id: "str | None" = None) -> list[dict]:
    """Use edges: [{"use_id", "parent_pipeline_id", "child_pipeline_id",
    "binding"}] — all of them, or one parent's."""
    _ensure_tables(db)
    if parent_pipeline_id is None:
        rows = _duck(db)._fetchall(
            "SELECT use_id, parent_pipeline_id, child_pipeline_id, "
            "binding_json FROM _pipeline_uses"
        )
    else:
        rows = _duck(db)._fetchall(
            "SELECT use_id, parent_pipeline_id, child_pipeline_id, "
            "binding_json FROM _pipeline_uses WHERE parent_pipeline_id = ?",
            [parent_pipeline_id],
        )
    result = []
    for use_id, parent_id, child_id, binding_json in rows:
        try:
            binding = json.loads(binding_json) if binding_json else {}
        except (json.JSONDecodeError, TypeError):
            binding = {}
        result.append(
            {
                "use_id": use_id,
                "parent_pipeline_id": parent_id,
                "child_pipeline_id": child_id,
                "binding": binding,
            }
        )
    return result


def update_use_binding(db, use_id: str, binding: dict) -> None:
    """Replace a use edge's binding ({key_map, params, iterate} subset)."""
    _ensure_tables(db)
    allowed = {"key_map", "params", "iterate"}
    unknown = set(binding) - allowed
    if unknown:
        raise ValueError(
            f"unknown binding key(s) {sorted(unknown)} — allowed: {sorted(allowed)}"
        )
    _duck(db)._execute(
        "UPDATE _pipeline_uses SET binding_json = ? WHERE use_id = ?",
        [json.dumps(binding), use_id],
    )
    logger.info("[pipeline_store] update_use_binding: %s -> %s", use_id, binding)


# ---------------------------------------------------------------------------
# Hypotheses (tagged top-level pipelines, rendered as tabs)
# ---------------------------------------------------------------------------


def _loads_list(raw: "str | None") -> list:
    try:
        return json.loads(raw) if raw else []
    except (json.JSONDecodeError, TypeError):
        return []


def list_hypotheses(db) -> list[dict]:
    """VISIBLE hypothesis-tagged pipelines, root first: [{"pipeline_id",
    "name", "research_question", "hypothesis_statement", "evidence_for",
    "evidence_against"}]. Hidden ones (see hide_pipeline) are excluded —
    their _hypotheses metadata row is left untouched so unhide_pipeline
    brings the tab back with research question/evidence intact."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT p.pipeline_id, p.name, h.research_question, "
        "h.hypothesis_statement, h.evidence_for, h.evidence_against "
        "FROM _hypotheses h JOIN _pipelines p ON p.pipeline_id = h.pipeline_id "
        "WHERE NOT p.hidden ORDER BY (p.pipeline_id != ?), p.name",
        [ROOT_SCOPE],
    )
    return [
        {
            "pipeline_id": pipeline_id,
            "name": name,
            "research_question": question or "",
            "hypothesis_statement": statement or "",
            "evidence_for": _loads_list(ev_for),
            "evidence_against": _loads_list(ev_against),
        }
        for pipeline_id, name, question, statement, ev_for, ev_against in rows
    ]


def tag_as_hypothesis(db, pipeline_id: str) -> None:
    """Tag an EXISTING pipeline as a hypothesis (e.g. after duplicating
    one) — a no-op if it's already tagged."""
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _hypotheses (pipeline_id) VALUES (?) ON CONFLICT DO NOTHING",
        [pipeline_id],
    )
    logger.info("[pipeline_store] tag_as_hypothesis: %s", pipeline_id)


def create_hypothesis(db, name: str) -> str:
    """Create a new pipeline and tag it as a hypothesis; returns its pipeline_id."""
    _ensure_tables(db)
    pipeline_id = create_pipeline(db, name)
    _duck(db)._execute("INSERT INTO _hypotheses (pipeline_id) VALUES (?)", [pipeline_id])
    logger.info("[pipeline_store] create_hypothesis: '%s' -> %s", name, pipeline_id)
    return pipeline_id


def update_hypothesis(
    db,
    pipeline_id: str,
    research_question: "str | None" = None,
    hypothesis_statement: "str | None" = None,
    evidence_for: "list | None" = None,
    evidence_against: "list | None" = None,
) -> None:
    """Update whichever fields are provided; ``None`` leaves a field unchanged."""
    _ensure_tables(db)
    known = _duck(db)._fetchall(
        "SELECT 1 FROM _hypotheses WHERE pipeline_id = ?", [pipeline_id]
    )
    if not known:
        raise ValueError(f"'{pipeline_id}' is not a hypothesis pipeline")
    fields, values = [], []
    if research_question is not None:
        fields.append("research_question = ?")
        values.append(research_question)
    if hypothesis_statement is not None:
        fields.append("hypothesis_statement = ?")
        values.append(hypothesis_statement)
    if evidence_for is not None:
        fields.append("evidence_for = ?")
        values.append(json.dumps(evidence_for))
    if evidence_against is not None:
        fields.append("evidence_against = ?")
        values.append(json.dumps(evidence_against))
    if not fields:
        return
    values.append(pipeline_id)
    _duck(db)._execute(
        f"UPDATE _hypotheses SET {', '.join(fields)} WHERE pipeline_id = ?", values
    )
    logger.info("[pipeline_store] update_hypothesis: %s", pipeline_id)


def hide_hypothesis(db, pipeline_id: str) -> None:
    """Hide a hypothesis tab (reuses hide_pipeline's consumer/last-visible
    guards). Its _hypotheses metadata row is left untouched — unhiding
    brings the tab back with research question/evidence intact."""
    _ensure_tables(db)
    hide_pipeline(db, pipeline_id)
    logger.info("[pipeline_store] hide_hypothesis: %s", pipeline_id)


# ---------------------------------------------------------------------------
# Hidden nodes (user-deleted DB-derived nodes)
# ---------------------------------------------------------------------------


def hide_node(db, node_id: str, pipeline_id: str = ROOT_SCOPE) -> None:
    """Mark a DB-derived node as hidden IN ``pipeline_id`` so that scope's
    _build_graph won't recreate it — a canonical id shared by another
    pipeline scope's independent placement of the same wiring (see
    graph_builder.wiring_id) is untouched (plan-scope-hidden-nodes-edges.md).
    """
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.hide_node(db, node_id, pipeline_id)


def unhide_node(db, node_id: str, pipeline_id: str = ROOT_SCOPE) -> None:
    """Remove a node from ``pipeline_id``'s hidden list (e.g. re-added there)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.unhide_node(db, node_id, pipeline_id)


def unhide_nodes_by_prefix(
    db, prefix: str, pipeline_id: str = ROOT_SCOPE
) -> None:
    """Remove all of ``pipeline_id``'s hidden nodes whose IDs start with
    ``prefix``.

    Used when a user re-adds a function node by label: composite DB-derived
    IDs (``fn__{label}__{call_id}``) don't match a single canonical ID, so
    we unhide every call-site node sharing the prefix.
    """
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.unhide_nodes_by_prefix(db, prefix, pipeline_id)


def get_hidden_node_ids(db, pipeline_id: "str | None" = None) -> set[str]:
    """Return the set of node IDs hidden in ``pipeline_id``.

    ``pipeline_id=None`` returns every scope's hidden ids unioned. Since
    2026-09-20 execution is scope-aware: a run, a compiled pipeline and both
    code exports pass the canvas they run from (a click's node id names it —
    ``intent_store.scope_of_node``), so a node deleted in one hypothesis
    still runs in another. Only the name-scoped fallback (a run request
    with no node id) still passes ``None``. Canvas rendering
    (api.pipeline._build_graph) has always passed its own scope.

    Pending-constant combo hides remain globally scoped by design and are
    always unioned in regardless of ``pipeline_id``.
    """
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.hidden_node_ids(db, pipeline_id)


# ---------------------------------------------------------------------------
# Hidden combos (one specific constant-value row of a function's Cartesian
# product, never a whole node) — never deletes data, only hides it. Reuses
# hide_node/unhide_node for the shared node_id space (fn__{fn}__{call_id}) so
# a combo hidden before it's ever run and later actually run land on the
# same id; the structural variant_key is kept alongside so a hidden combo
# can be shown back (a call_id hash can't be reversed) for the restore UI.
# ---------------------------------------------------------------------------


def hide_combo(db, node_id: str, function_name: str, variant_key: dict) -> None:
    """Hide one call-site's Cartesian-product row without deleting anything."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    hide_node(db, node_id)
    intent_store.hide_combo(db, node_id, function_name, variant_key)


def unhide_combo(db, node_id: str) -> None:
    """Restore a previously hidden combo."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    unhide_node(db, node_id)
    intent_store.unhide_combo(db, node_id)


def list_hidden_combos(db, function_name: str) -> list[dict]:
    """Return hidden combos for one function as {"node_id", "variant_key"}."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.hidden_combos(db, function_name)


# ---------------------------------------------------------------------------
# PathInput name history — see _pipeline_path_input_history above (D7).
# ---------------------------------------------------------------------------


def record_path_input_value(
    db, name: str, template: str, root_folder: "str | None" = None
) -> None:
    """Remember that *name* has been declared with this template.

    Idempotent and append-only. Called from exactly one place —
    ``target_file_service``, immediately before a GUI write-back overwrites
    a template.
    """
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _pipeline_path_input_history "
        "(name, template, root_folder) VALUES (?, ?, ?) "
        "ON CONFLICT DO NOTHING",
        [name, template, root_folder or ""],
    )


def lookup_path_input_name(
    db, template: str, root_folder: "str | None" = None
) -> "str | None":
    """The PathInput name historically declared with this template, or
    ``None`` — the fallback ``resolve_path_input_name`` consults when a
    recorded template matches no CURRENT declaration."""
    _ensure_tables(db)
    row = _duck(db)._fetchone(
        "SELECT name FROM _pipeline_path_input_history "
        "WHERE template = ? AND root_folder = ? LIMIT 1",
        [template, root_folder or ""],
    )
    return row[0] if row else None


def path_input_history_index(db) -> dict:
    """``{(template, root_folder): name}`` — the lookup shape
    ``graph_builder.resolve_path_input_name`` consults as its fallback.

    ``root_folder`` comes back as ``None`` (not ``''``) so the key matches
    what DB history records, which stores a genuine NULL when unset.
    """
    return {
        (row["template"], row["root_folder"]): row["name"]
        for row in list_path_input_history(db)
    }


def list_path_input_history(db, name: "str | None" = None) -> list[dict]:
    """Every recorded (name, template, root_folder), optionally for one
    name — the rows a PathInput node renders as its historical values."""
    _ensure_tables(db)
    sql = (
        "SELECT name, template, root_folder FROM _pipeline_path_input_history"
    )
    params: list = []
    if name is not None:
        sql += " WHERE name = ?"
        params.append(name)
    sql += " ORDER BY name, template"
    return [
        {"name": r[0], "template": r[1], "root_folder": r[2] or None}
        for r in _duck(db)._fetchall(sql, params)
    ]


# ---------------------------------------------------------------------------
# Hidden constant values — see _pipeline_hidden_constant_values above.
# ---------------------------------------------------------------------------


def hide_parameter_value(
    db, const_name: str, value: str, pipeline_id: str = ROOT_SCOPE
) -> None:
    """Mark one constant value as excluded from future runs, without
    deleting anything (per-value analog of ``hide_node``)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.hide_parameter_values(db, const_name, [value], pipeline_id)


def unhide_parameter_value(
    db, const_name: str, value: str, pipeline_id: str = ROOT_SCOPE
) -> None:
    """Restore a previously hidden constant value."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.unhide_parameter_values(db, const_name, [value], pipeline_id)


def hide_parameter_values(
    db, const_name: str, values: "list[str]", pipeline_id: str = ROOT_SCOPE
) -> None:
    """Hide several values of one Parameter in a single statement.

    The bulk form exists for the generated-set checkbox, which toggles every
    member of a range at once: one round-trip instead of one per value
    (``project_batched_provenance_hot_paths`` — a 50-value range is exactly
    the N+1 shape that memory is about).
    """
    if not values:
        return
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.hide_parameter_values(db, const_name, values, pipeline_id)
    logger.info(
        "[pipeline_store] hid %d value(s) of parameter %r", len(values), const_name
    )


def unhide_parameter_values(
    db, const_name: str, values: "list[str]", pipeline_id: str = ROOT_SCOPE
) -> None:
    """Restore several hidden values of one Parameter in a single statement.
    Bulk counterpart to :func:`hide_parameter_values`."""
    if not values:
        return
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.unhide_parameter_values(db, const_name, values, pipeline_id)
    logger.info(
        "[pipeline_store] unhid %d value(s) of parameter %r", len(values), const_name
    )


def list_hidden_parameter_values(
    db, pipeline_id: "str | None" = ROOT_SCOPE
) -> list[dict]:
    """Return hidden constant values as [{"const_name", "value"}, ...].

    ``pipeline_id=None`` returns every scope's hidden values unioned — same
    fail-open convention as ``get_hidden_node_ids``.
    """
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.hidden_parameter_values(db, pipeline_id)


# ---------------------------------------------------------------------------
# Generated value sets — see _pipeline_parameter_value_groups above.
# ---------------------------------------------------------------------------


def set_parameter_value_group(
    db, param_name: str, *, kind: str, spec: dict, values: "list[str]"
) -> None:
    """Record *values* as one generated set for *param_name*, replacing any
    previous one (one group per Parameter — see the table comment)."""
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _pipeline_parameter_value_groups "
        "(param_name, kind, spec, member_values) VALUES (?, ?, ?, ?) "
        "ON CONFLICT (param_name) DO UPDATE SET "
        "kind = excluded.kind, spec = excluded.spec, "
        "member_values = excluded.member_values",
        [param_name, kind, json.dumps(spec), json.dumps(list(values))],
    )
    logger.debug(
        "[pipeline_store] recorded generated set for %r: kind=%s, %d member(s)",
        param_name,
        kind,
        len(values),
    )


def clear_parameter_value_group(db, param_name: str) -> None:
    """Forget the generated set for *param_name*, if any. The values
    themselves live in source and are untouched — only the grouping goes."""
    _ensure_tables(db)
    _duck(db)._execute(
        "DELETE FROM _pipeline_parameter_value_groups WHERE param_name = ?",
        [param_name],
    )


def get_parameter_value_groups(db) -> dict:
    """``{param_name: {"kind", "spec", "values"}}`` for every recorded
    generated set.

    A row whose JSON no longer parses is skipped with a warning rather than
    raised: the grouping is cosmetic, and a corrupt one must not be able to
    take down the whole graph build.
    """
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT param_name, kind, spec, member_values "
        "FROM _pipeline_parameter_value_groups"
    )
    out: dict = {}
    for param_name, kind, spec, member_values in rows:
        try:
            out[param_name] = {
                "kind": kind,
                "spec": json.loads(spec),
                "values": list(json.loads(member_values)),
            }
        except (ValueError, TypeError) as e:
            logger.warning(
                "[pipeline_store] unreadable value group for %r (%s) — the "
                "values still render individually",
                param_name,
                e,
            )
    return out


# ---------------------------------------------------------------------------
# Hidden edges (user-deleted DB-derived edges) — never deletes data, only
# hides it, same ethos as hide_node/hide_combo. Hiding an INBOUND edge
# (variable/constant/pathInput -> function) additionally makes the target
# function's wiring "disconnected" for run-state and execution purposes
# (see domain.graph_builder.hidden_wirings) — hiding an OUTBOUND edge
# (function -> variable) is purely cosmetic. Manual (``manual__``) edges
# are hard-deleted instead of hidden (see layout_service.delete_edge);
# this table is for DB-derived edges only.
# ---------------------------------------------------------------------------


def hide_edge(
    db,
    edge_id: str,
    source: str = "",
    target: str = "",
    source_handle: "str | None" = None,
    target_handle: "str | None" = None,
    pipeline_id: str = ROOT_SCOPE,
) -> None:
    """Mark a DB-derived edge as hidden IN ``pipeline_id`` so that scope's
    build_edges won't recreate it — an edge id shared by another pipeline
    scope's independent placement of the same wiring is untouched (see
    hide_node and plan-scope-hidden-nodes-edges.md)."""
    logger.info(
        "[pipeline_store] hide_edge called (edge_id=%r, source=%r, target=%r, "
        "pipeline_id=%r)",
        edge_id,
        source,
        target,
        pipeline_id,
    )
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.hide_edge(
        db, edge_id, source, target, source_handle, target_handle, pipeline_id
    )


def unhide_edge(db, edge_id: str, pipeline_id: str = ROOT_SCOPE) -> None:
    """Restore a previously hidden edge in ``pipeline_id``."""
    logger.info(
        "[pipeline_store] unhide_edge called (edge_id=%r, pipeline_id=%r)",
        edge_id,
        pipeline_id,
    )
    _ensure_tables(db)
    from scistack_gui import intent_store

    intent_store.unhide_edge(db, edge_id, pipeline_id)


def get_hidden_edge_ids(db, pipeline_id: "str | None" = None) -> set[str]:
    """Return the set of edge IDs hidden in ``pipeline_id``.

    ``pipeline_id=None`` (default) returns every scope's hidden ids unioned
    — see get_hidden_node_ids for why (same execution-path caveat)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    return {e["edge_id"] for e in intent_store.hidden_edges(db, pipeline_id)}


def list_hidden_edges(db, pipeline_id: "str | None" = None) -> list[dict]:
    """Hidden edges with enough context to label a restore-list entry.

    ``pipeline_id=None`` (default) lists every scope's hidden edges; pass a
    scope to restrict to just that pipeline's own hidden edges (the restore
    panel — see plan-scope-hidden-nodes-edges.md)."""
    _ensure_tables(db)
    from scistack_gui import intent_store

    return intent_store.hidden_edges(db, pipeline_id)


# ---------------------------------------------------------------------------
# Hidden subpipeline ports (to-do #9) — a manual override on top of
# domain.scope_filter.document_interface's automatic type-level port
# computation. Never deletes wiring, only suppresses one type's exposed
# dot on a scope's interface; the internal node that produces/consumes
# that type stays fully visible on its own canvas at all times, so
# there's no separate restore list (unlike hidden edges/nodes) — un-hiding
# is just toggling the same right-click item again.
# ---------------------------------------------------------------------------


def hide_port(db, pipeline_id: str, direction: str, var_type: str) -> None:
    """Suppress ``var_type``'s exposed ``direction`` ('input'|'output')
    port on ``pipeline_id``'s subpipeline interface."""
    logger.info(
        "[pipeline_store] hide_port called (pipeline_id=%r, direction=%r, var_type=%r)",
        pipeline_id,
        direction,
        var_type,
    )
    _ensure_tables(db)
    _duck(db)._execute(
        "INSERT INTO _pipeline_hidden_ports (pipeline_id, direction, var_type) "
        "VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
        [pipeline_id, direction, var_type],
    )


def unhide_port(db, pipeline_id: str, direction: str, var_type: str) -> None:
    """Restore a previously hidden port."""
    logger.info(
        "[pipeline_store] unhide_port called (pipeline_id=%r, direction=%r, var_type=%r)",
        pipeline_id,
        direction,
        var_type,
    )
    _duck(db)._execute(
        "DELETE FROM _pipeline_hidden_ports "
        "WHERE pipeline_id = ? AND direction = ? AND var_type = ?",
        [pipeline_id, direction, var_type],
    )


def get_hidden_ports(db, pipeline_id: str) -> dict:
    """One scope's hidden ports: ``{"input": {type, ...}, "output": {type, ...}}``
    — what the right-click context menu needs to decide Show vs Hide."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT direction, var_type FROM _pipeline_hidden_ports WHERE pipeline_id = ?",
        [pipeline_id],
    )
    result: dict[str, set[str]] = {"input": set(), "output": set()}
    for direction, var_type in rows:
        result[direction].add(var_type)
    return result


def get_hidden_ports_by_scope(db) -> dict[str, dict[str, set[str]]]:
    """Every scope's hidden ports, keyed by pipeline_id — the shape
    domain.scope_filter.document_interface needs (it recurses across
    scopes via nested ``uses``, so it needs every scope's own hides on
    hand at once, not just the scope being asked about)."""
    _ensure_tables(db)
    rows = _duck(db)._fetchall(
        "SELECT pipeline_id, direction, var_type FROM _pipeline_hidden_ports"
    )
    result: dict[str, dict[str, set[str]]] = {}
    for pid, direction, var_type in rows:
        scope = result.setdefault(pid, {"input": set(), "output": set()})
        scope[direction].add(var_type)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _upsert_node(
    db, node_id: str, node_type: str, label: str, pipeline_id: str = ROOT_SCOPE
) -> None:
    _duck(db)._execute(
        "INSERT INTO _pipeline_nodes (node_id, node_type, label, pipeline_id) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT (node_id) DO UPDATE SET node_type = excluded.node_type, "
        "label = excluded.label, pipeline_id = excluded.pipeline_id",
        [node_id, node_type, label, pipeline_id],
    )
