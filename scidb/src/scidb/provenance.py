"""Bipartite provenance model — identity helpers and schema.

This module implements the storage-side primitives for the simplified
provenance system described in ``docs/claude/lineage-simplification.md``.

The graph is **bipartite**: *records* (entities — variables and constants)
and *invocations* (activities — unique function calls). Data flow is captured
entirely by edges in ``_invocation_input`` / ``_invocation_output``, so
traversal is a plain recursive SQL join rather than Python-side JSON parsing.

Everything here is **content-addressed and idempotent**: re-running an
identical pipeline reproduces every id, so all inserts are
``ON CONFLICT DO NOTHING`` and no duplicate provenance is written. The one
exception is ``_run`` (the audit log), which gets a fresh row per execution.

Identity (see §5 of the design doc)::

    constant record_id = hash("__constant__" | content_hash(value))
    invocation_id      = hash(function_hash | as_table | distribute
                              | sorted(input bindings))
    output record_id   = hash(type | schema_version | content_hash(data)
                              | invocation_id | output_num)
    run_id             = fresh unique id per for_each execution (NOT addressed)
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Iterable
from typing import Any

from scicanonicalhash import canonical_hash

logger = logging.getLogger(__name__)

__all__ = [
    "CONSTANT_TYPE",
    "compute_wiring_id",
    "parse_path_input_spec",
    "path_input_key_of",
    "strip_path_input_specs",
    "GLUE_INPUT_PARAM",
    "GLUE_TYPE",
    "PATHINPUT_TYPE",
    "PATHINPUT_VALUE_TYPE",
    "SAVE_FUNCTION_NAME",
    "compute_constant_record_id",
    "constant_record_id_from_hash",
    "compute_glue_invocation_id",
    "compute_glue_record_id",
    "compute_invocation_id",
    "compute_pathinput_record_id",
    "compute_save_invocation_id",
    "generate_run_id",
    "normalize_as_table",
    "constant_value_repr",
    "constant_value_type",
    "ensure_provenance_tables",
    "insert_record_entity",
    "insert_function_sources",
    "insert_record_entities",
]

# Sentinel ``type`` value for constant records in ``_record``. Constants have
# no schema_id (schema-global) and no producing invocation.
CONSTANT_TYPE = "__constant__"

# Sentinel ``type`` for a *PathInput spec* input record. A PathInput resolves to a
# per-combo filepath (deliberately NOT in the graph), but its SPEC (template +
# root_folder) is config-level and recorded as a distinctly-typed input edge so
# the GUI/variant queries can surface it. Treated like a constant everywhere edges
# are bucketed into variables/constants, and — like a constant — PART of
# ``invocation_id`` since 2026-09-25 (``provenance_save._pathinput_bindings``):
# one function reading two files is two invocations.
PATHINPUT_TYPE = "__pathinput__"

# The ``value_type`` a PathInput spec row carries in ``_constant`` — the
# constant *value* type column, NOT the ``_record.type`` sentinel above (the
# two live in different tables and must not be swapped). Defined once so the
# writer and anything that counts/filters those rows can't drift apart.
PATHINPUT_VALUE_TYPE = "PathInput"

# Sentinel ``function_name`` for a *synthetic save invocation* — the activity row
# that anchors a direct ``.save(..., kw=v)`` call's non-schema kwargs as constant
# inputs (so they become graph-derivable branch params instead of living in
# ``version_keys``). Not a real pipeline function: queries that enumerate pipeline
# nodes / function variants must exclude ``function_name = SAVE_FUNCTION_NAME``.
# (``_invocation.function_name``/``function_hash`` are NOT NULL, so we use a
# sentinel string rather than NULL.)
SAVE_FUNCTION_NAME = "__save__"

# Sentinel ``type`` for a *virtual glue record* — the provenance node standing
# in for a glue chain's in-memory, never-saved output (see
# ``docs/claude/free-code-glue-nodes.md`` §2). It has the same ``schema_id`` as
# the record it reshapes, but **no** ``_record_save`` row, no data-table row
# and nothing loadable: the data is not saved, only the node is. That is what
# lets an edited glue change the consumer's ``invocation_id`` — ``skip_computed``
# compares bindings, and this is the binding that moves.
#
# Anything that enumerates real records must skip it. Most paths already do,
# because they either filter on a concrete variable type or join ``_record_save``
# (which a virtual record never has).
GLUE_TYPE = "__glue__"

# ``param_name`` on a glue invocation's single input edge. Fixed rather than
# taken from the glue's own signature: parameter names never bind anything
# (edges do), and a stable name keeps the id reproducible when the glue body is
# reformatted.
GLUE_INPUT_PARAM = "input"

# All record_ids (variables, constants, outputs) are 16 hex chars so they join
# uniformly across the bipartite graph. Matches scicanonicalhash.generate_record_id.
_ID_LEN = 16


def _sha16(*parts: str) -> str:
    """SHA-256 of ``parts`` joined by ``|``, truncated to 16 hex chars."""
    combined = "|".join(parts).encode("utf-8")
    return hashlib.sha256(combined).hexdigest()[:_ID_LEN]


# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------
def constant_record_id_from_hash(content_hash: str) -> str:
    """Content-addressed constant id from a precomputed content hash.

    The hash must be ``canonical_hash(value)`` so this agrees with
    :func:`compute_constant_record_id`. Used by the lineage save path, which has
    the constant's hash (scilineage's ``value_hash`` == ``canonical_hash``) but
    not always the raw value.
    """
    return _sha16(CONSTANT_TYPE, f"content:{content_hash}")


def compute_constant_record_id(value: Any) -> str:
    """Content-addressed id for a constant value.

    Constants are schema-global: the same value reused anywhere in any pipeline
    maps to one ``_record`` / ``_constant`` row. The id depends only on the
    value's canonical content hash, not on where or how it is consumed.
    """
    rid = constant_record_id_from_hash(canonical_hash(value))
    logger.debug("compute_constant_record_id(%r) = %s", value, rid)
    return rid


def compute_invocation_id(
    function_hash: str,
    as_table: Iterable[str] | None,
    distribute: bool,
    input_bindings: Iterable[tuple],
    across_variants: Iterable[str] | None = None,
) -> str:
    """Content-addressed id for a unique function call (an *activity*).

    Args:
        function_hash: AST hash of the function source (``compute_function_hash``).
        as_table: Resolved aggregated param names. Order-insensitive (sorted in).
        distribute: Post-call fan-out flag.
        across_variants: Params whose records were pooled across every variant
            group into the one call (``AcrossVariants``) instead of the
            default split. Identity-bearing like ``as_table``: the same edge
            set called pooled and called split are different computations
            (the pooled frame carries the branch params as columns). Folded in
            ONLY when non-empty, so every id computed before 2026-09-20 is
            unchanged.
        input_bindings: Iterable of input edges, one per realized input (variable
            *and* constant): ``bindings.Binding`` objects, or the legacy
            ``(param_name, input_record_id)`` / ``(param_name, input_record_id,
            selector)`` tuples they replace. ``selector`` qualifies
            wrappers that change *which* data is consumed without changing the
            record — notably ``ColumnSelection`` (e.g. ``{"columns": [...]}``) —
            so two calls selecting different columns of the same record get
            distinct ids. Order-insensitive.

    ``where`` is deliberately excluded — it only filters which records load, and
    its whole effect on the computation is the surviving input set = these very
    bindings (see §10.1). Re-running the same call reproduces this id exactly.
    """
    # Every edge is a ``bindings.Binding``; the two tuple arities the caller
    # may still hand in are coerced (same bytes — the parity suite pins it).
    from .bindings import Binding

    norm: list[tuple[str, str, str]] = []
    for b in input_bindings:
        edge = Binding.coerce(b)
        norm.append((edge.param, edge.rid, "" if edge.selector is None else edge.selector))
    bindings = sorted(norm)
    parts = [
        f"fn_hash:{function_hash}",
        f"as_table:{canonical_hash(sorted(as_table or []))}",
        f"distribute:{bool(distribute)}",
        f"inputs:{canonical_hash(bindings)}",
    ]
    pooled = sorted(str(p) for p in (across_variants or []))
    if pooled:
        parts.append(f"across_variants:{canonical_hash(pooled)}")
    inv_id = _sha16(*parts)
    logger.debug(
        "compute_invocation_id(fn_hash=%s, as_table=%s, distribute=%s, %d bindings) = %s",
        function_hash,
        sorted(as_table or []),
        bool(distribute),
        len(bindings),
        inv_id,
    )
    return inv_id


def compute_pathinput_record_id(value: str) -> str:
    """Id of the PathInput input record (see :data:`PATHINPUT_TYPE`) — ONE per
    PathInput NAME.

    *value* may be the identity key (``PathInput.to_key()``) or a full stored
    spec (``PathInput.to_spec()``): both reduce to the name's key
    (:func:`path_input_key_of`), so the save side (which holds keys), the
    predict side (which reads stored specs) and the skip side agree by
    construction, and a PathInput whose files moved keeps its id.
    """
    key = path_input_key_of(value)
    if key is None:
        raise ValueError(f"not a named PathInput key or spec: {value!r}")
    return _sha16(PATHINPUT_TYPE, f"spec:{key}")


def compute_glue_record_id(
    chain_hash: str, input_record_id: str, input_set_signature: str
) -> str:
    """Content-addressed id for a *virtual glue record* (see :data:`GLUE_TYPE`).

    A glue node's output is never saved, but its provenance node is: this id
    stands in for the reshaped table so the consuming function's
    ``invocation_id`` changes when the glue body changes. That is the whole
    point — ``skip_computed`` compares *bindings*, never version_keys, so glue
    that only entered version_keys would leave every downstream record green
    and stale.

    Three components, each load-bearing:

    * ``chain_hash`` — the glue bodies, in order. An edit is a new record.
    * ``input_record_id`` — the record being reshaped. One virtual record per
      input record, so per-row provenance survives the hop.
    * ``input_set_signature`` — :func:`scidb.glue.input_set_signature` over the
      whole input rid set. A whole-table glue may legitimately read across
      rows, so its result depends on the entire set; without this a *growing*
      input set would not force a recompute (the same hole that was already
      closed for aggregation ``skip_computed``).
    """
    return _sha16(
        GLUE_TYPE,
        f"chain:{chain_hash}",
        f"input:{input_record_id}",
        f"set:{input_set_signature}",
    )


def compute_glue_invocation_id(
    chain_hash: str, input_record_id: str, input_set_signature: str
) -> str:
    """Activity id for the glue application that produced a virtual record.

    Keyed on the same three components as :func:`compute_glue_record_id`, via
    the ordinary :func:`compute_invocation_id` recipe so the glue hop is a
    normal activity to every traversal that walks the graph.
    """
    return compute_invocation_id(
        f"{chain_hash}:{input_set_signature}",
        None,
        False,
        [(GLUE_INPUT_PARAM, input_record_id, None)],
    )


def compute_save_invocation_id(output_record_id: str) -> str:
    """Content-addressed id for a *synthetic save invocation* (see
    :data:`SAVE_FUNCTION_NAME`).

    Keyed 1:1 by the saved record's id (not by the kwargs) so that two different
    variables saved at the same schema with the same kwarg, and re-saves of the
    same record, never collide on ``invocation_id`` / ``_invocation_output`` PK.
    Idempotent: identical content → same ``output_record_id`` → same id → ON
    CONFLICT DO NOTHING. The kwargs still ride on the constant input edges, so
    ``derived_branch_params`` recovers them.
    """
    return _sha16(SAVE_FUNCTION_NAME, output_record_id)


def normalize_as_table(as_table_value, loadable_params) -> list[str]:
    """Resolve an ``as_table`` flag to a sorted list of aggregated param names.

    Shared by the save path and the skip/predict path so both compute the same
    ``invocation_id``. ``True`` means "aggregate every loadable input"; a list is
    used as-is. Order-insensitive (sorted).
    """
    if as_table_value is True:
        return sorted(str(p) for p in loadable_params)
    if isinstance(as_table_value, (list, tuple)):
        return sorted(str(x) for x in as_table_value)
    return []


def generate_run_id() -> str:
    """Fresh, non-content-addressed id for one ``for_each`` execution.

    Unlike everything else here this is intentionally unique per call so the
    ``_run`` audit log captures *every* execution event — even a re-run that
    reproduces existing (deduped) invocations.
    """
    return uuid.uuid4().hex[:_ID_LEN]


# ---------------------------------------------------------------------------
# Constant value rendering
# ---------------------------------------------------------------------------
def constant_value_repr(value: Any) -> str:
    """Human-readable rendering of a constant value for ``_constant.value_repr``."""
    return repr(value)


def constant_value_type(value: Any) -> str:
    """Type label for ``_constant.value_type`` (e.g. ``"int"``, ``"str"``)."""
    return type(value).__name__


def constants_identity_key(constants: Any) -> tuple:
    """A hashable, order-independent key for a ``{param: value}`` constants map.

    Every consumer that groups or de-duplicates variants needs the *same*
    notion of "these two calls used the same constants", and the value side of
    that map is **not** restricted to scalars: an inline table under
    ``[parameters]`` in ``scistack_entities.toml`` *is* the value, so a
    constant can legitimately be a ``dict`` (or a ``list``) — see
    docs/claude/entities-toml-format.md rule 2 and ``_format_matlab_value``,
    which renders exactly those into MATLAB structs/arrays.

    A plain ``tuple(sorted(constants.items()))`` is therefore a trap: it is
    hashable only while every value happens to be a scalar, and raises
    ``TypeError: unhashable type: 'dict'`` the first time a real project
    declares a config-table parameter. ``repr`` is deterministic for a given
    content and order, so identical maps collapse and different ones don't —
    the same recipe ``provenance_save.record_run`` already uses for its
    invocation cache key.

    Returns ``()`` for anything that is not a dict, so callers can pass a
    possibly-missing field straight through.
    """
    if not isinstance(constants, dict):
        return ()
    return tuple(sorted((str(k), constant_value_repr(v)) for k, v in constants.items()))


# ---------------------------------------------------------------------------
# Record entity writes
# ---------------------------------------------------------------------------
# Every saved record — raw/manual AND computed — gets one ``_record`` row so the
# entities table is the complete node set for graph traversal. Computed records
# additionally get invocation/edge rows (see provenance_save.record_run); raw
# records have no producing invocation and terminate the upward walk.
_RECORD_INSERT = (
    "INSERT INTO _record "
    "(record_id, created_at, type, schema_id, content_hash, schema_version, excluded) "
    "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (record_id) DO NOTHING"
)


def insert_record_entity(
    duck,
    record_id: str,
    created_at: str,
    type_name: str,
    schema_id: int | None,
    content_hash: str | None,
    schema_version: int | None,
    excluded: bool = False,
) -> None:
    """Insert one ``_record`` entity row (idempotent)."""
    duck._execute(
        _RECORD_INSERT,
        [
            record_id,
            created_at,
            type_name,
            schema_id,
            content_hash,
            schema_version,
            excluded,
        ],
    )


_RECORD_COLUMNS = (
    "record_id",
    "created_at",
    "type",
    "schema_id",
    "content_hash",
    "schema_version",
    "excluded",
)


def insert_record_entities(duck, rows: list[tuple]) -> None:
    """Bulk-insert ``_record`` rows. Each row is
    ``(record_id, created_at, type, schema_id, content_hash, schema_version, excluded)``.

    Uses a single vectorized ``INSERT ... SELECT`` (see ``SciDuck._bulk_insert``);
    the previous per-row ``executemany`` cost ~497s for 8k rows into this
    PK-indexed table."""
    duck._bulk_insert("_record", _RECORD_COLUMNS, rows, conflict_cols=["record_id"])


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def insert_function_sources(
    duck, function_hash: str, units: dict, entry_name: str | None = None
) -> int:
    """Store the source behind ``function_hash``. Returns the row count written.

    Idempotent (``ON CONFLICT DO NOTHING``) and content-keyed, so re-running an
    unchanged function rewrites nothing and re-running an edited one files its
    source under the new hash beside the old. Never overwrites: a captured
    version stays captured, which is the point — the old body is otherwise
    unrecoverable the moment the file is saved.

    Failure here must never fail a save. The caller treats it as best-effort:
    losing source costs traceability, losing the run costs the user's work.
    """
    if not function_hash or not units:
        return 0
    rows = [
        (function_hash, name, source, name == entry_name)
        for name, source in units.items()
    ]
    duck._bulk_insert(
        "_function_source",
        ("function_hash", "unit_name", "unit_source", "is_entry"),
        rows,
        conflict_cols=["function_hash", "unit_name"],
    )
    logger.debug(
        "insert_function_sources: %s -> %d unit(s) (entry=%s)",
        function_hash[:12],
        len(rows),
        entry_name,
    )
    return len(rows)


def ensure_provenance_tables(duck) -> None:
    """Create the bipartite provenance tables if absent.

    ``duck`` is a ``SciDuck`` instance (exposes ``_execute``). Tables:

    Identity/data: ``_record``, ``_constant``, ``_invocation``,
    ``_invocation_input``, ``_invocation_output``.
    Code: ``_function_source``.
    Audit: ``_run``, ``_run_invocation``.

    See §4 of the design doc for the rationale (bipartite vs. flat edge table).
    """
    logger.debug("ensure_provenance_tables: creating bipartite provenance schema")

    # Entities: variables AND constants. Variables have a schema_id + a type
    # (class name) and their data lives in the per-type "<Type>_data" tables.
    # Constants have schema_id NULL, type '__constant__', value in _constant.
    # Content-addressed and immutable → ONE row, ON CONFLICT DO NOTHING.
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _record (
            record_id      VARCHAR PRIMARY KEY,
            created_at     VARCHAR NOT NULL,
            type           VARCHAR NOT NULL,
            schema_id      INTEGER,
            content_hash   VARCHAR,
            schema_version INTEGER,
            excluded       BOOLEAN DEFAULT FALSE
        )
    """)

    # Constant values (parallel to a variable's "<Type>_data" table).
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _constant (
            record_id    VARCHAR PRIMARY KEY,
            value_repr   VARCHAR,
            value_type   VARCHAR,
            content_hash VARCHAR
        )
    """)

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
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _invocation (
            invocation_id VARCHAR PRIMARY KEY,
            function_name VARCHAR NOT NULL,
            function_hash VARCHAR NOT NULL,
            as_table      VARCHAR[],
            distribute    BOOLEAN DEFAULT FALSE,
            for_columns   VARCHAR[],
            across_variants VARCHAR[]
        )
    """)

    # The code behind a ``function_hash``. Purely additive: the hash is already
    # the key, so nothing about identity changes and an absent row means only
    # "not captured", never "different code".
    #
    # One row per *unit*, not per function, because the Python hash is recursive
    # over user-defined callees (``scilineage.hashing._hash_source``): a helper's
    # body is part of what the hash identifies, so the entry point alone would
    # not reconstitute the hashed code. ``is_entry`` marks the function the hash
    # is named for; the rest are its closure.
    #
    # Deliberately NOT content-addressed with a separate unit table. A shared
    # helper is therefore stored once per calling hash rather than once
    # globally, which is duplication — but source is kilobytes, the dedup would
    # cost a second table and a join on every read, and nothing in the read path
    # is hot. Revisit only if a real database shows the size mattering.
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _function_source (
            function_hash VARCHAR NOT NULL,
            unit_name     VARCHAR NOT NULL,
            unit_source   VARCHAR NOT NULL,
            is_entry      BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (function_hash, unit_name)
        )
    """)

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
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _invocation_input (
            invocation_id   VARCHAR NOT NULL,
            param_name      VARCHAR NOT NULL,
            input_record_id VARCHAR NOT NULL,
            selector        VARCHAR,
            declared_name   VARCHAR,
            PRIMARY KEY (invocation_id, param_name, input_record_id)
        )
    """)

    # What a call produced. Multiple rows for multi-output functions.
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _invocation_output (
            invocation_id    VARCHAR NOT NULL,
            output_num       INTEGER NOT NULL,
            output_record_id VARCHAR NOT NULL,
            PRIMARY KEY (invocation_id, output_num)
        )
    """)

    # Audit log: one row per for_each EXECUTION (fresh row every run, even when
    # it reproduces existing invocations). Captures when/who/where.
    # ``origin`` on _run — which surfaces the run read (rule 3 of
    # docs/claude/intent-and-fact.md): ``gui`` / ``script`` / ``replay``. NULL
    # is treated by every consumer as "unknown", never as any origin.
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _run (
            run_id        VARCHAR PRIMARY KEY,
            timestamp     VARCHAR NOT NULL,
            user_id       VARCHAR,
            function_name VARCHAR NOT NULL,
            where_clause  VARCHAR,
            origin        VARCHAR
        )
    """)

    # Many-to-many: which invocations a run (re)produced.
    duck._execute("""
        CREATE TABLE IF NOT EXISTS _run_invocation (
            run_id        VARCHAR NOT NULL,
            invocation_id VARCHAR NOT NULL,
            PRIMARY KEY (run_id, invocation_id)
        )
    """)

    # Indexes for upward/downward traversal (the recursive CTEs in §6/§8 join
    # output_record_id → invocation_id → input_record_id repeatedly).
    duck._execute(
        "CREATE INDEX IF NOT EXISTS idx_inv_output_rid "
        "ON _invocation_output (output_record_id)"
    )
    duck._execute(
        "CREATE INDEX IF NOT EXISTS idx_inv_input_inv "
        "ON _invocation_input (invocation_id)"
    )
    logger.debug("ensure_provenance_tables: done")


# ---------------------------------------------------------------------------
# Call-site identity: the WIRING (Stage 1 of plan-architecture-2026-09-20)
# ---------------------------------------------------------------------------
# A call site is a function plus the SHAPE of what feeds it — which variable
# types, which PathInputs, which outputs — and nothing about the values: the
# constants vary per variant, the data per record. It is the identity the
# canvas keys a node by, and the subject every `_intent` statement is about.
#
# This recipe used to live only in scistack_gui.domain.graph_builder, the GUI
# predicting an id nothing in scidb computed; each disagreement between the
# canvas and the run path grew a normalisation patch on the GUI side
# (`strip_path_input_params`). One owner now, below every caller, with the
# normalisation INSIDE the recipe so there is exactly one view to hash. The
# bytes are unchanged: node ids key saved layout positions and scope
# membership, so the payload here is the GUI's, verbatim.


def parse_path_input_spec(value) -> "dict | None":
    """``{"name", "template", "root_folder"}`` when *value* (an ``__inputs`` /
    ``input_types`` entry, or a stored ``__pathinput__`` value) is a PathInput,
    else ``None``.

    The JSON of ``PathInput.to_spec()`` (everything) or ``PathInput.to_key()``
    (the name alone — ``template`` and ``root_folder`` come back ``None``).
    The ONE parser — `scidb.inspect.graph`, `scidb.database` and the GUI's
    `graph_builder` each carried a copy before 2026-09-20.
    """
    import json as _json

    if not isinstance(value, str) or not value.startswith("{"):
        return None
    try:
        parsed = _json.loads(value)
    except (_json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, dict) or parsed.get("__type") != "PathInput":
        return None
    return {
        "name": parsed.get("name"),
        "template": parsed.get("template"),
        "root_folder": parsed.get("root_folder"),
    }


def path_input_key_of(value) -> "str | None":
    """The identity key (``PathInput.to_key()``) of a stored PathInput spec or
    key, or ``None`` when *value* is not a NAMED PathInput.

    Every reader that turns a recorded PathInput back into identity — a call
    id, a predicted invocation, a skip comparison — goes through this, so the
    name is the only thing any of them can see.
    """
    from scifor.pathinput import path_input_key

    parsed = parse_path_input_spec(value)
    if parsed is None or not parsed.get("name"):
        return None
    return path_input_key(parsed["name"])


def strip_path_input_specs(input_types: dict) -> dict:
    """*input_types* without the entries that are really PathInput specs.

    A raw variant row records a PathInput-fed parameter inside
    ``input_types`` beside the genuine variable inputs; the canvas view
    partitions it out. Anything that hashes an input shape has to agree on
    which view it is using — so the hash strips it itself (see
    :func:`compute_wiring_id`) and callers may pass either view.
    """
    return {
        k: v
        for k, v in (input_types or {}).items()
        if parse_path_input_spec(v) is None
    }


def _wiring_input_term(value):
    """One input's term in the wiring payload: a one-item collection
    collapses to its item (so ``["X"]`` hashes as ``"X"``), a multi-type
    (EachOf) input is sorted (so edge order doesn't matter), anything else is
    used as-is. The bare form is the one every recorded id was computed from,
    so collapsing toward it changes no existing id."""
    if isinstance(value, (list, set, tuple)):
        items = sorted(value)
        return items[0] if len(items) == 1 else items
    return value


def compute_wiring_id(
    fn_name: str, input_types: dict, out_types, path_inputs: "dict | None"
) -> str:
    """16-hex id for a function's WIRING: name + loadable-input shape +
    output types — the call_id recipe minus constants, so constant-value
    variants of one call share one canvas node. Deterministic across graph
    builds.

    ``path_inputs`` is ``{param_name: declared PathInput name}`` and is part
    of the shape: two call sites of one function fed by DIFFERENT PathInputs
    into the same output hashed identically without it. Omitted from the
    payload when empty — mirroring ``to_version_keys``, which drops
    ``__inputs`` rather than emitting ``{}`` — so only PathInput-fed call
    sites have their ids affected by the term.

    A PathInput is represented by that term and ONLY that term: any spec
    left in ``input_types`` is stripped here rather than counted twice, so
    the canvas's partitioned view and a raw variant's ``input_types`` hash
    alike. That disagreement is what once made a graduated PathInput-fed
    node unrunnable ("No pipeline history or output connections found" for
    a green, fully wired node).

    A single-type input hashes the same whether it is spelled ``"X"`` or
    ``["X"]`` (:func:`_wiring_input_term`). History always spells it bare,
    while an edge-derived candidate list spells it as a one-item list; the
    equivalence lives here so no caller has to remember to flatten first.
    """
    import json as _json

    input_types = strip_path_input_specs(input_types)
    payload_obj: dict = {
        "fn": fn_name,
        "inputs": {k: _wiring_input_term(v) for k, v in sorted(input_types.items())},
        "outputs": sorted(out_types),
    }
    if path_inputs:
        payload_obj["path_inputs"] = dict(sorted(path_inputs.items()))
    payload = _json.dumps(payload_obj, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
