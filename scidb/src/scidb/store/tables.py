"""THE registry of the project database's fixed tables (D3: scidb owns the DDL).

Every table in a project ``.duckdb`` that is not a per-variable
``<Type>_data`` table is declared here as a :class:`TableDecl`: scidb's own
tables in this module, every other package's through :func:`declare` (the
GUI's stores, scistackplotdb's and scistackstatsdb's ``VersionedStore``s).
:func:`declared` is therefore the one list of what a project database holds.

Creating them:

* :func:`ensure_core` creates scidb's own tables (owner ``"scidb"``), in
  declaration order, whenever a writable ``SciDuck`` opens. They are what a
  database IS, so they never wait for first use. sciduckdb creates none.
* :func:`ensure` creates named tables, for an owner whose table is made on
  its first write (a saved plot, a canvas edit). Their owners may be
  imported after the database opened, and a read must never create one.

Every statement is ``CREATE ... IF NOT EXISTS`` (or another idempotent
form), so opening an existing database runs them as no-ops. Read-only
connections run none: DuckDB refuses DDL there and the tables already exist.

Classifying them (plan P3 stage 6 step 7): each scidb table also says which
bundle section carries its rows (history, intent, data, or none) and whether
``clear_data`` empties it or keeps it, and why. The bundle's and the clear's
table lists are read from here (:func:`history_tables`,
:func:`intent_tables`, :func:`data_tables`, :func:`cleared_tables`,
:func:`kept_tables`), so a new table is classified once, where it is
declared, and :func:`declare` refuses one that is neither cleared nor kept
with a reason.

Guard: ``tests/test_table_registry.py`` fails on a ``CREATE TABLE`` anywhere
else in scidb, sciduckdb, scistackplotdb, scistackstatsdb or the GUI.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from scistacklog import Log

from scidb.model.exceptions import TableDeclarationError

#: A statement, or a function of the dataset schema keys returning one (a
#: table with one column per schema key).
Statement = str | Callable[[Sequence[str]], str]


#: ``TableDecl.bundle``: the rows travel in a bundle's history section.
BUNDLE_HISTORY = "history"
#: ``TableDecl.bundle``: history rows that are dataset INTENT, not facts.
#: They govern a re-run, so they go live in the new project even when the
#: rest of the history is only archived.
BUNDLE_INTENT = "intent"
#: ``TableDecl.bundle``: the rows travel in a bundle's (opt-in) data section,
#: beside every variable's ``<Type>_data`` table.
BUNDLE_DATA = "data"

#: The owner whose tables carry a bundle/clear classification. Every other
#: owner's rows travel in its own bundle section and a clear never touches
#: them, so a flag there would be silently ignored, and it is refused.
CORE_OWNER = "scidb"


@dataclass(frozen=True)
class TableDecl:
    """One fixed table: its name, the statements that create it, who
    declared it (a package name, for logs), and, for scidb's own tables,
    what a bundle and ``clear_data`` do with its rows.

    This is THE classification of scidb's tables. ``scidb.project.bundle``
    (``HISTORY_TABLES`` / ``INTENT_TABLES``) and ``scidb.store.clear_data``
    (``CLEARED_TABLES`` / ``KEPT_TABLES``) are read from it via
    :func:`history_tables`, :func:`intent_tables`, :func:`cleared_tables`
    and :func:`kept_tables`, never restated."""

    name: str
    statements: tuple[Statement, ...]
    owner: str
    #: :data:`BUNDLE_HISTORY`, :data:`BUNDLE_INTENT`, :data:`BUNDLE_DATA`, or None (no bundle carries it).
    bundle: str | None = None
    #: ``clear_data`` empties the table.
    cleared: bool = False
    #: Why ``clear_data`` leaves the table alone (scidb tables not cleared).
    kept_reason: str = ""

    def sql(self, schema_keys: Sequence[str]) -> list[str]:
        """The statements to run, with schema-keyed ones rendered."""
        return [s(schema_keys) if callable(s) else s for s in self.statements]


_DECLS: dict[str, TableDecl] = {}

#: ``(id(connection), table)`` already logged as ensured. The GUI ensures its
#: tables on every request, so only the first time per connection is logged.
_LOGGED: set[tuple[int, str]] = set()


def declare(decl: TableDecl) -> TableDecl:
    """Register *decl*. Declaring the same table again is a no-op; declaring
    a different one under a taken name raises :class:`TableDeclarationError`
    (two owners for one table is the bug this registry exists to stop).
    A classification that contradicts itself is refused too (see
    :func:`_check_classification`)."""
    _check_classification(decl)
    existing = _DECLS.get(decl.name)
    if existing is not None:
        if existing == decl:
            return existing
        raise TableDeclarationError(
            f"Table {decl.name!r} is declared by {existing.owner} and again, "
            f"differently, by {decl.owner}. One table has one owner."
        )
    _DECLS[decl.name] = decl
    Log.debug("[tables] declared %s (owner %s)", decl.name, decl.owner)
    return decl


def _check_classification(decl: TableDecl) -> None:
    """A scidb table is either emptied by a clear or kept with a reason,
    never both or neither; another owner's table carries no flags."""
    if decl.bundle not in (None, BUNDLE_HISTORY, BUNDLE_INTENT, BUNDLE_DATA):
        raise TableDeclarationError(
            f"Table {decl.name!r}: bundle={decl.bundle!r}; expected "
            f"{BUNDLE_HISTORY!r}, {BUNDLE_INTENT!r}, {BUNDLE_DATA!r} or None"
        )
    if decl.owner != CORE_OWNER:
        if decl.bundle is not None or decl.cleared or decl.kept_reason:
            raise TableDeclarationError(
                f"Table {decl.name!r} (owner {decl.owner}): only {CORE_OWNER}'s "
                f"tables are classified for bundles and clear_data; this "
                f"owner's rows travel in its own bundle section and a clear "
                f"never touches them"
            )
        return
    if decl.cleared == bool(decl.kept_reason):
        raise TableDeclarationError(
            f"Table {decl.name!r}: a scidb table is either cleared=True or "
            f"kept with a kept_reason, exactly one "
            f"(cleared={decl.cleared}, kept_reason={decl.kept_reason!r})"
        )


def declared() -> list[TableDecl]:
    """Every declared table, in declaration order."""
    return list(_DECLS.values())


def _core_decls() -> list[TableDecl]:
    return [d for d in _DECLS.values() if d.owner == CORE_OWNER]


def history_tables() -> tuple[str, ...]:
    """scidb tables a bundle's history section carries (intent included),
    in declaration order."""
    return tuple(d.name for d in _core_decls() if d.bundle in (BUNDLE_HISTORY, BUNDLE_INTENT))


def intent_tables() -> tuple[str, ...]:
    """History tables that go live even when the history is archived."""
    return tuple(d.name for d in _core_decls() if d.bundle == BUNDLE_INTENT)


def data_tables() -> tuple[str, ...]:
    """scidb tables a bundle's data section carries, besides the dynamic
    ``<Type>_data`` tables."""
    return tuple(d.name for d in _core_decls() if d.bundle == BUNDLE_DATA)


def cleared_tables() -> tuple[str, ...]:
    """scidb tables ``clear_data`` empties (besides every ``<Type>_data``)."""
    return tuple(d.name for d in _core_decls() if d.cleared)


def kept_tables() -> dict[str, str]:
    """``{table: reason}``: scidb tables ``clear_data`` leaves alone."""
    return {d.name: d.kept_reason for d in _core_decls() if not d.cleared}


def ensure(duck, *names: str, schema_keys: Sequence[str] = ()) -> int:
    """Create the declared tables *names* on *duck* if absent; returns how
    many were ensured. Nothing runs on a read-only connection. An undeclared
    name raises :class:`TableDeclarationError`."""
    missing = [n for n in names if n not in _DECLS]
    if missing:
        raise TableDeclarationError(f"No table declaration for {missing}")
    return _run(duck, [_DECLS[n] for n in names], schema_keys)


def ensure_core(duck, schema_keys: Sequence[str]) -> int:
    """Create every table scidb itself declares, in declaration order.
    Called by ``SciDuck`` on every writable open."""
    decls = [d for d in _DECLS.values() if d.owner == "scidb"]
    started = time.perf_counter()
    count = _run(duck, decls, schema_keys)
    if count:
        Log.info(
            "[tables] ensured %d tables from %d owner(s) in %.0f ms",
            count,
            len({d.owner for d in decls}),
            (time.perf_counter() - started) * 1000,
        )
    return count


def _run(duck, decls: Iterable[TableDecl], schema_keys: Sequence[str]) -> int:
    if getattr(duck, "read_only", False):
        Log.debug("[tables] read-only connection: no DDL run")
        return 0
    count = 0
    for decl in decls:
        for statement in decl.sql(schema_keys):
            duck.execute(statement)
        if (id(duck), decl.name) not in _LOGGED:
            _LOGGED.add((id(duck), decl.name))
            Log.debug("[tables] ensured %s (owner %s)", decl.name, decl.owner)
        count += 1
    return count


def _core(
    name: str,
    *statements: Statement,
    bundle: str | None = None,
    cleared: bool = False,
    kept: str = "",
) -> TableDecl:
    return declare(
        TableDecl(
            name=name,
            statements=tuple(statements),
            owner=CORE_OWNER,
            bundle=bundle,
            cleared=cleared,
            kept_reason=kept,
        )
    )


#: The run log a clear keeps (user decision 2026-10-21).
_RUN_LOG = "the run log: that a run happened, when, and its origin (user decision 2026-10-21)"


def _schema_columns(schema_keys: Sequence[str]) -> str:
    return ", ".join(f'"{k}" VARCHAR' for k in schema_keys)


# ---------------------------------------------------------------------------
# Schema locations and variables (formerly sciduckdb's SciDuck)
# ---------------------------------------------------------------------------

SCHEMA = _core(
    "_schema",
    lambda keys: f"""
    CREATE TABLE IF NOT EXISTS _schema (
        schema_id INTEGER PRIMARY KEY,
        schema_level VARCHAR NOT NULL,
        {_schema_columns(keys)}
    )
    """,
    "CREATE SEQUENCE IF NOT EXISTS _schema_id_seq START 1",
    bundle=BUNDLE_HISTORY, kept="location interning; a re-save after the clear reuses the same schema ids",
)

_core(
    "_variables",
    """
    CREATE TABLE IF NOT EXISTS _variables (
        variable_name VARCHAR PRIMARY KEY,
        schema_level VARCHAR NOT NULL,
        dtype VARCHAR,
        created_at TIMESTAMP DEFAULT current_timestamp,
        description VARCHAR DEFAULT ''
    )
    """,
    bundle=BUNDLE_DATA, kept="variable registrations and dtype metadata",
)

_core(
    "_variable_groups",
    """
    CREATE TABLE IF NOT EXISTS _variable_groups (
        group_name VARCHAR NOT NULL,
        variable_name VARCHAR NOT NULL,
        PRIMARY KEY (group_name, variable_name)
    )
    """,
    bundle=BUNDLE_DATA, kept="variable groups (declarations)",
)

# ---------------------------------------------------------------------------
# Type registration and the save-event log (DatabaseManager)
# ---------------------------------------------------------------------------

# Only type_name is unique (PRIMARY KEY). table_name is not unique, to avoid
# DuckDB's ON CONFLICT ambiguity with multiple unique constraints.
_core(
    "_registered_types",
    """
    CREATE TABLE IF NOT EXISTS _registered_types (
        type_name VARCHAR PRIMARY KEY,
        table_name VARCHAR NOT NULL,
        schema_version INTEGER NOT NULL,
        registered_at TIMESTAMP DEFAULT current_timestamp
    )
    """,
    bundle=BUNDLE_DATA, kept="variable registrations (which classes exist)",
)

# The append-only save-event audit log: one row per save event
# ``(record_id, timestamp)``, nothing else. Everything else about a record
# lives on the content-addressed ``_record`` row. The only thing this table
# uniquely provides is per-save recency for the "latest" variant collapse
# (``_record`` is inserted ON CONFLICT DO NOTHING, so its ``created_at`` is
# frozen at the first save).
_core(
    "_record_save",
    """
    CREATE TABLE IF NOT EXISTS _record_save (
        record_id VARCHAR NOT NULL,
        timestamp VARCHAR NOT NULL,
        PRIMARY KEY (record_id, timestamp)
    )
    """,
    bundle=BUNDLE_DATA, cleared=True,
)

# Persistent schema-level exclusions (scidb.store.exclusions), one column per key.
SCHEMA_OVERRIDES = _core(
    "__scidb_schema_overrides",
    lambda keys: f"""
    CREATE TABLE IF NOT EXISTS __scidb_schema_overrides (
        {_schema_columns(keys)},
        status     BOOLEAN   NOT NULL,
        reason     TEXT      NOT NULL,
        changed_at TIMESTAMP NOT NULL
    )
    """,
    bundle=BUNDLE_INTENT, kept="dataset intent: governs a re-run",
)

# ---------------------------------------------------------------------------
# The bipartite provenance graph (scidb.provenance.graph; see
# docs/claude/lineage-simplification.md §4 for bipartite vs a flat edge table)
# ---------------------------------------------------------------------------

# Entities: variables AND constants. Variables have a schema_id + a type
# (class name) and their data lives in the per-type "<Type>_data" tables.
# Constants have schema_id NULL, type '__constant__', value in _constant.
# Content-addressed and immutable → ONE row, ON CONFLICT DO NOTHING.
_core(
    "_record",
    """
    CREATE TABLE IF NOT EXISTS _record (
        record_id      VARCHAR PRIMARY KEY,
        created_at     VARCHAR NOT NULL,
        type           VARCHAR NOT NULL,
        schema_id      INTEGER,
        content_hash   VARCHAR,
        schema_version INTEGER,
        excluded       BOOLEAN DEFAULT FALSE
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# Constant values (parallel to a variable's "<Type>_data" table).
_core(
    "_constant",
    """
    CREATE TABLE IF NOT EXISTS _constant (
        record_id    VARCHAR PRIMARY KEY,
        value_repr   VARCHAR,
        value_type   VARCHAR,
        content_hash VARCHAR
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# Activities: one row per UNIQUE function call (content-addressed).
# as_table/distribute are identity-bearing and stored as queryable columns.
# where is NOT here — it is batch-level (see _run).
#
# ``for_columns``: which params ran once per column. Purely DESCRIPTIVE —
# derived from the edges' selectors and NOT folded into invocation_id (the
# selector already is, so adding it would count the same fact twice). It
# exists so a run-option query does not have to parse selector JSON.
#
# ``across_variants``: which params pooled every variant group into the
# one call. NOT derivable from the edges (a pooled call and a one-group
# split call write the same edges), so it is the stored fact the call id
# and the expected-invocation predictor rebuild from; it IS folded into
# invocation_id (only when non-empty).
#
# ``iteration_level``: the schema keys the call ITERATED (``[]`` = one
# call; NULL for glue hops and ``__save__``). Folded into invocation_id
# (2026-10-01) because nothing else records it: a PathInput-only loader run
# per subject and run once write identical edges. Stored so the run-option
# label, the predictor and the skip gate can rebuild it
# (docs/claude/iteration-level-identity.md).
_core(
    "_invocation",
    """
    CREATE TABLE IF NOT EXISTS _invocation (
        invocation_id VARCHAR PRIMARY KEY,
        function_name VARCHAR NOT NULL,
        function_hash VARCHAR NOT NULL,
        as_table      VARCHAR[],
        distribute    BOOLEAN DEFAULT FALSE,
        for_columns   VARCHAR[],
        across_variants VARCHAR[],
        iteration_level VARCHAR[]
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# The code behind a ``function_hash``. Purely additive: the hash is already
# the key, so nothing about identity changes and an absent row means only
# "not captured", never "different code".
#
# One row per *unit*, not per function, because the Python hash is recursive
# over user-defined callees (``scicanonicalhash.functions._hash_source``): a helper's
# body is part of what the hash identifies, so the entry point alone would
# not reconstitute the hashed code. ``is_entry`` marks the function the hash
# is named for; the rest are its closure.
#
# Deliberately NOT content-addressed with a separate unit table. A shared
# helper is therefore stored once per calling hash rather than once
# globally, which is duplication — but source is kilobytes, the dedup would
# cost a second table and a join on every read, and nothing in the read path
# is hot. Revisit only if a real database shows the size mattering.
_core(
    "_function_source",
    """
    CREATE TABLE IF NOT EXISTS _function_source (
        function_hash VARCHAR NOT NULL,
        unit_name     VARCHAR NOT NULL,
        unit_source   VARCHAR NOT NULL,
        is_entry      BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (function_hash, unit_name)
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# What was fed into a call, and the argument slot it filled. ``selector``
# qualifies wrappers that change which data is consumed without changing the
# record (ColumnSelection: JSON like {"columns": [...]}); NULL otherwise.
# It is folded into invocation_id so different selections don't collide.
#
# ``declared_name`` names the declared Parameter that filled a CONSTANT's
# slot when it differs from, or is merely unknown from, ``param_name``
# (a Parameter declared ``gaitrite_config`` feeding ``gaitRiteConfig``).
# Descriptive only, NOT part of invocation_id: the value is the identity,
# the name is what the canvas calls it. NULL = not recorded (older rows,
# a bare value in a script); readers fall back to ``param_name``.
_core(
    "_invocation_input",
    """
    CREATE TABLE IF NOT EXISTS _invocation_input (
        invocation_id   VARCHAR NOT NULL,
        param_name      VARCHAR NOT NULL,
        input_record_id VARCHAR NOT NULL,
        selector        VARCHAR,
        declared_name   VARCHAR,
        PRIMARY KEY (invocation_id, param_name, input_record_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_inv_input_inv ON _invocation_input (invocation_id)",
    bundle=BUNDLE_HISTORY, cleared=True,
)

# What a call produced. Multiple rows for multi-output functions.
_core(
    "_invocation_output",
    """
    CREATE TABLE IF NOT EXISTS _invocation_output (
        invocation_id    VARCHAR NOT NULL,
        output_num       INTEGER NOT NULL,
        output_record_id VARCHAR NOT NULL,
        PRIMARY KEY (invocation_id, output_num)
    )
    """,
    # Indexes for upward/downward traversal: the recursive CTEs join
    # output_record_id -> invocation_id -> input_record_id repeatedly.
    "CREATE INDEX IF NOT EXISTS idx_inv_output_rid ON _invocation_output (output_record_id)",
    bundle=BUNDLE_HISTORY, cleared=True,
)

# Audit log: one row per for_each EXECUTION (fresh row every run, even when
# it reproduces existing invocations). Captures when/where (never who: docs/claude/decisions.md, no user identity).
# ``origin`` on _run — which surfaces the run read (rule 3 of
# docs/claude/intent-and-fact.md): ``gui`` / ``script`` / ``replay``. NULL
# is treated by every consumer as "unknown", never as any origin.
_core(
    "_run",
    """
    CREATE TABLE IF NOT EXISTS _run (
        run_id        VARCHAR PRIMARY KEY,
        timestamp     VARCHAR NOT NULL,
        function_name VARCHAR NOT NULL,
        where_clause  VARCHAR,
        origin        VARCHAR
    )
    """,
    bundle=BUNDLE_HISTORY, kept=_RUN_LOG,
)

# The step name a run was given (`for_each(..., step="filter")`), when it
# was given one. Its own table rather than a `_run` column: an existing
# database gains it on open, with no migration (beta rule). Not identity.
_core(
    "_run_step",
    """
    CREATE TABLE IF NOT EXISTS _run_step (
        run_id VARCHAR PRIMARY KEY,
        step   VARCHAR NOT NULL
    )
    """,
    bundle=BUNDLE_HISTORY, kept="part of the run log: the step name a run was given (for_each(step=...))",
)

# The output variable types a run wrote, in its ``outputs=`` ORDER (the
# function's return order; a for_columns split's group pairing). Its own
# table like `_run_step`: an existing database gains it on open. NOT
# identity, and per RUN rather than per invocation: outputs are not part
# of invocation_id, so a re-run listing them in another order reuses the
# invocations and only a run row can say what that run's order was.
# `_invocation_output.output_num` cannot answer this: slots append across
# runs and a distributed run's slot is the slice index
# (.claude/plan-output-order-owner.md). Reader:
# provenance_query.recorded_output_orders.
#
# ``output_params``: a MATLAB run's signature output NAMES at run time,
# position for position with ``outputs`` (``function [time, force] = f``
# wrote ``[Time, Force]``). Maps each output name to the variable it saved
# to, which survives a later reordering of the signature; NULL for Python
# (its functions declare no outputs). Added the same day as the table, so
# an ADD COLUMN IF NOT EXISTS keeps a store opened in between working.
_core(
    "_run_outputs",
    """
    CREATE TABLE IF NOT EXISTS _run_outputs (
        run_id        VARCHAR PRIMARY KEY,
        outputs       VARCHAR[] NOT NULL,
        output_params VARCHAR[]
    )
    """,
    "ALTER TABLE _run_outputs ADD COLUMN IF NOT EXISTS output_params VARCHAR[]",
    bundle=BUNDLE_HISTORY, kept="part of the run log: the order a run listed its outputs (outputs=)",
)

# Many-to-many: which invocations a run (re)produced.
_core(
    "_run_invocation",
    """
    CREATE TABLE IF NOT EXISTS _run_invocation (
        run_id        VARCHAR NOT NULL,
        invocation_id VARCHAR NOT NULL,
        PRIMARY KEY (run_id, invocation_id)
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# A function RENAMED (Share as library / Make my own copy:
# ``scidb.store.rename.rename_functions``, plan-library-fork-chain R2). The
# recorded name is identity, so after a rename the old name's records are
# a different variant from the new name's; this fact is what makes the
# old name's records saved before ``renamed_at`` stale for "current"
# (``provenance_query.renamed_superseded_records``) -- kept, never latest.
_core(
    "_function_rename",
    """
    CREATE TABLE IF NOT EXISTS _function_rename (
        old_name   VARCHAR NOT NULL,
        new_name   VARCHAR NOT NULL,
        renamed_at VARCHAR NOT NULL,
        PRIMARY KEY (old_name, new_name, renamed_at)
    )
    """,
    bundle=BUNDLE_HISTORY, cleared=True,
)

# ---------------------------------------------------------------------------
# Variant pins, delete tombstones, schema-key types, intent
# ---------------------------------------------------------------------------

VARIANT_PIN = _core(
    "_variant_pin",
    """
    CREATE TABLE IF NOT EXISTS _variant_pin (
        pin_id         VARCHAR PRIMARY KEY,
        variable       VARCHAR NOT NULL,
        selection_json VARCHAR NOT NULL,
        reason         VARCHAR NOT NULL,
        pinned_at      VARCHAR NOT NULL,
        released_at    VARCHAR,
        release_reason VARCHAR
    )
    """,
    bundle=BUNDLE_HISTORY, kept="audit trail; active pins are RELEASED, rows kept",
)

VARIANT_TOMBSTONE = _core(
    "_variant_tombstone",
    """
    CREATE TABLE IF NOT EXISTS _variant_tombstone (
        tombstone_id    VARCHAR PRIMARY KEY,
        deleted_at      VARCHAR NOT NULL,
        reason          VARCHAR NOT NULL,
        targets_json    VARCHAR NOT NULL,
        counts_json     VARCHAR NOT NULL,
        record_ids_json VARCHAR NOT NULL
    )
    """,
    bundle=BUNDLE_HISTORY, kept="the audit trail of every real delete, this clear included",
)

# Schema key types are dataset INTENT (user decision 2026-11-01): the type
# defines stored identity ("1" vs "001"), so an imported project needs them
# live even when the history is only archived. They ride in the history
# section for now; plan-dataset-intent-section.md gives them their own.
SCHEMA_KEY_TYPE = _core(
    "_schema_key_type",
    """
    CREATE TABLE IF NOT EXISTS _schema_key_type (
        key         VARCHAR NOT NULL,
        key_type    VARCHAR,
        declared_at VARCHAR NOT NULL,
        source      VARCHAR NOT NULL
    )
    """,
    bundle=BUNDLE_INTENT, kept="schema key types (declarations)",
)

# Statements about runs (scidb.provenance.intent; docs/claude/intent-and-fact.md). The
# GUI writes most of them, scidb reads them, so scidb declares the table
# (design review 07, G4).
INTENT = _core(
    "_intent",
    """
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
    """,
    kept="statements about runs (what the canvas asks for): intent, not results",
)
