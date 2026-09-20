"""Write the bipartite provenance graph from the for_each save path.

This is the save-side companion to ``scidb.provenance`` (identity + schema).
It writes the structural provenance graph:

- ``_record``     — one entity row per output and per constant
- ``_constant``   — value/repr/type for each constant entity
- ``_invocation`` — one activity row per unique function call
- ``_invocation_input``  — edges: call → its inputs (variables AND constants)
- ``_invocation_output`` — edges: call → its outputs (by output_num)
- ``_run`` / ``_run_invocation`` — append-only audit of this execution

Everything is content-addressed and inserted ``ON CONFLICT DO NOTHING``, so
re-running an identical pipeline writes no duplicate provenance — only a fresh
``_run`` row.

The graph is built from the per-record ``save_metadata`` that for_each already
assembles, which carries ``__fn`` / ``__fn_hash`` (function identity),
``__upstream`` (``{__rid_<param>: record_id}`` — variable input edges),
``__constants`` (``{param: value}`` — constant inputs), and the
``__as_table`` / ``__distribute`` identity flags.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from scicanonicalhash import canonical_hash

from .provenance import (
    CONSTANT_TYPE,
    PATHINPUT_TYPE,
    PATHINPUT_VALUE_TYPE,
    compute_constant_record_id,
    compute_invocation_id,
    compute_pathinput_record_id,
    constant_record_id_from_hash,
    constant_value_repr,
    constant_value_type,
    generate_run_id,
    normalize_as_table,
)

logger = logging.getLogger(__name__)

__all__ = [
    "GraphRecord",
    "record_run",
    "compute_input_selectors",
    "record_direct_save",
    "invocation_id_for_meta",
]


def record_direct_save(
    duck, output_record_id: str, kwargs: dict, created_at: str
) -> None:
    """Anchor a direct ``.save(..., kw=v)`` call's non-schema kwargs in the graph
    as a *synthetic save invocation* (see ``provenance.SAVE_FUNCTION_NAME``).

    Inserts: one constant ``_record`` + ``_constant`` per kwarg, a synthetic
    ``_invocation`` (``function_name = SAVE_FUNCTION_NAME``, no real function), a
    ``_invocation_input`` edge per kwarg, and one ``_invocation_output`` edge
    (output_num 0) to the saved record. ``derived_branch_params`` then recovers
    the kwargs — replacing the old ``version_keys`` variant-distinguisher role.

    Runs **inside the caller's transaction** (no begin/commit). Idempotent via
    ON CONFLICT DO NOTHING. No-op when ``kwargs`` is empty.
    """
    from .provenance import (
        CONSTANT_TYPE,
        SAVE_FUNCTION_NAME,
        compute_save_invocation_id,
    )

    if not kwargs:
        return

    save_inv_id = compute_save_invocation_id(output_record_id)
    entity_rows = []
    constant_rows = []
    input_edges = []
    for name, value in kwargs.items():
        chash = canonical_hash(value)
        crid = compute_constant_record_id(value)
        entity_rows.append((crid, created_at, CONSTANT_TYPE, None, chash, None, False))
        constant_rows.append(
            (crid, constant_value_repr(value), constant_value_type(value), chash)
        )
        input_edges.append((save_inv_id, str(name), crid, None))

    con = duck.con
    duck._bulk_insert(
        "_record",
        (
            "record_id",
            "created_at",
            "type",
            "schema_id",
            "content_hash",
            "schema_version",
            "excluded",
        ),
        entity_rows,
        conflict_cols=["record_id"],
    )
    duck._bulk_insert(
        "_constant",
        ("record_id", "value_repr", "value_type", "content_hash"),
        constant_rows,
        conflict_cols=["record_id"],
    )
    con.execute(
        "INSERT INTO _invocation "
        "(invocation_id, function_name, function_hash, as_table, distribute) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT (invocation_id) DO NOTHING",
        [save_inv_id, SAVE_FUNCTION_NAME, "", [], False],
    )
    duck._bulk_insert(
        "_invocation_input",
        ("invocation_id", "param_name", "input_record_id", "selector"),
        input_edges,
        conflict_cols=["invocation_id", "param_name", "input_record_id"],
    )
    con.execute(
        "INSERT INTO _invocation_output (invocation_id, output_num, output_record_id) "
        "VALUES (?, ?, ?) ON CONFLICT (invocation_id, output_num) DO NOTHING",
        [save_inv_id, 0, output_record_id],
    )
    logger.debug(
        "record_direct_save: %s → save_inv %s with %d kwarg constant(s)",
        output_record_id,
        save_inv_id,
        len(kwargs),
    )


def compute_input_selectors(inputs: dict) -> dict:
    """Map each input param to its identity-affecting ``selector`` JSON, or None.

    Currently only ``ColumnSelection`` (directly, or wrapped in ``Fixed``)
    produces a selector — the chosen columns — because selecting different
    columns of the same record is a different computation (§ ColumnSelection
    decision). Fixed/Variant/Merge resolve to whole records and need no selector;
    their effect is captured by *which* record_id the edge points at.
    """
    from scifor import ColumnSelection, Fixed

    out: dict = {}
    for param, spec in inputs.items():
        cs = None
        if isinstance(spec, ColumnSelection):
            cs = spec
        elif isinstance(spec, Fixed) and isinstance(
            getattr(spec, "data", None), ColumnSelection
        ):
            cs = spec.data
        if cs is None:
            out[param] = None
            continue
        columns = list(getattr(cs, "columns", None) or [])
        iterate = bool(getattr(cs, "iterate", False))
        if not columns and not iterate:
            out[param] = None  # a plain whole-variable input
            continue
        # `iterate` (for_columns) rides along since 2026-09-19: a per-column
        # run over every column has no column list, so it had no selector
        # at all and a GUI re-run from history handed the function the
        # whole table (integration suite, test_dag_runs). Only written when
        # set, so an ordinary column selection keeps its old identity.
        selector = {"columns": columns}
        if iterate:
            selector["iterate"] = True
        out[param] = json.dumps(selector, sort_keys=True)
    return out


# A saved output record awaiting graph insertion. ``meta`` is its
# ``save_metadata`` dict (carrying __fn/__fn_hash/__upstream/__constants/flags).
class GraphRecord:
    __slots__ = ("type_name", "schema_version", "output_num", "record_id", "meta")

    def __init__(self, type_name, schema_version, output_num, record_id, meta):
        self.type_name = type_name
        self.schema_version = schema_version
        self.output_num = output_num
        self.record_id = record_id
        self.meta = meta


# ---------------------------------------------------------------------------
# meta → identity inputs
# ---------------------------------------------------------------------------
def _parse_json_dict(val: Any) -> dict:
    """Coerce a value that may be a dict or a JSON string into a dict."""
    if val is None:
        return {}
    if isinstance(val, str):
        try:
            return dict(json.loads(val or "{}"))
        except (json.JSONDecodeError, TypeError):
            return {}
    if isinstance(val, dict):
        return dict(val)
    return {}


def _variable_bindings(meta: dict) -> list[tuple[str, str, str | None]]:
    """Variable input edges as ``(param_name, record_id, selector)`` triples.

    Prefers ``__graph_var_bindings`` — the *complete* per-row binding set the
    save path assembles from every consumed input record (variables, Fixed,
    Variant, Merge constituents) with ColumnSelection selectors. Falls back to
    ``__upstream`` (``{__rid_<param>: record_id}``, no selectors) for the
    aggregation path, which builds upstream edges separately.
    """
    gvb = meta.get("__graph_var_bindings")
    if gvb:
        out = []
        for entry in gvb:
            param, rid, selector = entry
            if rid is None:
                continue
            out.append((param, str(rid), selector))
        return out

    upstream = _parse_json_dict(meta.get("__upstream"))
    out = []
    for key, rid in upstream.items():
        if rid is None:
            continue
        # The aggregation path stores one key per consumed record
        # (`__rid_<param>_<i>`), so these edges carry INDEXED names. They are
        # deliberately left as they are: `compute_invocation_id` hashes the
        # binding names, and `_predict_config_invocations` (skip_computed)
        # predicts one binding per param from the stored config — folding
        # here made every second identical aggregation run recompute.
        # Consumers that need the real parameter fold on READ instead
        # (`execution_service._fold_indexed_params`).
        param = key[len("__rid_") :] if key.startswith("__rid_") else key
        out.append((param, str(rid), None))
    return out


def _constant_bindings(meta: dict) -> dict[str, Any]:
    """Constant inputs from ``__constants`` → ``{param_name: value}``."""
    return _parse_json_dict(meta.get("__constants"))


def _pathinput_specs(meta: dict) -> dict[str, str]:
    """PathInput specs from ``__inputs`` → ``{param_name: spec_json_str}``.

    ``__inputs`` carries each loadable input's ``to_key()``; PathInput's is a JSON
    string ``{"__type": "PathInput", ...}``. Returns those entries verbatim (the
    exact spec string) so they can be stored as PathInput input records.
    """
    inputs = _parse_json_dict(meta.get("__inputs"))
    out: dict[str, str] = {}
    for param, val in inputs.items():
        if not isinstance(val, str) or not val.startswith("{"):
            continue
        try:
            parsed = json.loads(val)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(parsed, dict) and parsed.get("__type") == "PathInput":
            out[param] = val
    return out


def variable_input_params(meta: dict) -> list[str]:
    """Params in ``__inputs`` bound to a **scidb variable**, not a PathInput.

    ``__inputs`` carries every loadable input's ``to_key()`` plus PathInput's
    (whose per-combo resolution lives in scifor, not scidb's variable loader).
    Only the non-PathInput ones consume stored records, so only they are
    expected to produce ``_invocation_input`` edges. Lets the save path tell
    "no input edges because the inputs were files/constants" (normal) from
    "no input edges although a stored record was consumed" (severed lineage).
    """
    pathinputs = _pathinput_specs(meta)
    return [p for p in _parse_json_dict(meta.get("__inputs")) if p not in pathinputs]


def _normalize_as_table(meta: dict, loadable_params: list[str]) -> list[str]:
    """Resolve the ``__as_table`` flag to a sorted list of aggregated params.

    Delegates to :func:`scidb.provenance.normalize_as_table` so the save path and
    the skip/predict path (§9c) compute identical ``invocation_id``s.
    """
    return normalize_as_table(meta.get("__as_table"), loadable_params)


def invocation_id_for_meta(meta: dict) -> str:
    """The ``invocation_id`` for a record's ``save_metadata`` — identical to what
    :func:`record_run` computes for it (same binding assembly via the shared
    ``_variable_bindings`` / ``_constant_bindings`` / ``_normalize_as_table``
    helpers). Pure function of the metadata; used by ``record_run`` itself and by
    the generates_file lineage-only save to key its ``generated:{invocation_id}``
    record.
    """
    from .provenance import compute_invocation_id

    var_b = _variable_bindings(meta)
    const_b = _constant_bindings(meta)
    loadable_params = list(_parse_json_dict(meta.get("__inputs")).keys()) or [
        p for p, _r, _s in var_b
    ]
    as_table = _normalize_as_table(meta, loadable_params)
    distribute = bool(meta.get("__distribute", False))
    bindings: list[tuple[str, str, str | None]] = list(var_b)
    for param, value in const_b.items():
        bindings.append((param, compute_constant_record_id(value), None))
    return compute_invocation_id(
        meta.get("__fn_hash") or "", as_table, distribute, bindings
    )


# ---------------------------------------------------------------------------
# Output record metadata lookup
# ---------------------------------------------------------------------------
def _fetch_record_meta(duck, rids: list[str]) -> dict[str, dict]:
    """``content_hash`` / ``schema_id`` / ``schema_version`` (from the ``_record``
    entity) + latest save ``timestamp`` (from the ``_record_save`` event log) per
    output record_id."""
    uniq = list(dict.fromkeys(rids))
    if not uniq:
        return {}
    placeholders = ", ".join(["?"] * len(uniq))
    rows = duck._fetchall(
        f"""
        SELECT r.record_id, r.content_hash, r.schema_id, r.schema_version,
               MAX(rs.timestamp) AS timestamp
        FROM _record r
        JOIN _record_save rs ON rs.record_id = r.record_id
        WHERE r.record_id IN ({placeholders})
        GROUP BY r.record_id, r.content_hash, r.schema_id, r.schema_version
        """,
        uniq,
    )
    return {
        r[0]: {
            "content_hash": r[1],
            "schema_id": r[2],
            "schema_version": r[3],
            "timestamp": r[4],
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def record_run(
    db,
    graph_records: list[GraphRecord],
    *,
    function_name: str,
    where_clause: str | None,
    user_id: str | None,
    glue_virtual: dict | None = None,
    glue_chains: dict | None = None,
) -> str | None:
    """Write the bipartite graph for ``graph_records`` plus a fresh ``_run`` row.

    Returns the ``run_id`` (or ``None`` if there was nothing to record).

    Idempotent for the graph (``ON CONFLICT DO NOTHING``); the ``_run`` /
    ``_run_invocation`` rows are always appended so the audit log captures this
    execution even when it reproduced existing invocations.

    ``glue_virtual`` / ``glue_chains`` add the glue hop: the consumer's
    bindings already point at virtual glue rids (``scidb.glue.fuse_glue``
    rewrote ``__record_id`` before Step 11), so the matching virtual ``_record``
    rows and their producing ``_invocation``s are written here. See
    :func:`_write_glue_nodes`.
    """
    if not graph_records:
        return None

    import time

    from .log import Log

    timings: dict[str, float] = {}
    _t_start = time.perf_counter()

    duck = db._duck
    created_at = datetime.now().isoformat()
    _t = time.perf_counter()
    meta_map = _fetch_record_meta(duck, [g.record_id for g in graph_records])
    timings["1_meta_fetch"] = time.perf_counter() - _t
    _t_assemble = time.perf_counter()

    # Accumulators (deduped by key so we can ON CONFLICT DO NOTHING cheaply).
    entity_rows: dict[str, tuple] = {}  # record_id -> _record row
    constant_rows: dict[str, tuple] = {}  # record_id -> _constant row
    invocation_rows: dict[str, tuple] = {}  # invocation_id -> _invocation row
    input_edges: dict[
        tuple[str, str, str], str | None
    ] = {}  # (inv,param,rid) -> selector
    # ``output_edges`` is the WORKING slot map used for collision-free assignment
    # (seeded below with already-COMMITTED edges so a cross-run save appends fresh
    # slots instead of colliding). ``run_output_edges`` is the subset actually
    # produced by THIS run — only those are inserted, so committed edges are never
    # rewritten (immutable: we append, never overwrite or exclude).
    output_edges: dict[tuple[str, int], str] = {}  # (inv_id, output_num) -> rid
    run_output_edges: dict[tuple[str, int], str] = {}
    seeded_invs: set[str] = set()  # inv_ids whose committed edges are loaded
    run_inv_ids: set[str] = set()

    # Invocation-level memo. The whole assembly above (binding set, constant
    # hashing, invocation_id, invocation/input/constant rows) depends only on a
    # record's identity-determining meta — never on which output slot it is. A
    # distribute/flatten fan-out emits THOUSANDS of output records that all share
    # one invocation, so without this every one re-derives the identical id
    # (canonical_hash ×N + SHA): that recomputation was ~all of record_run's time
    # (22.9s for 14253 records → 1 invocation). The key is built from the raw
    # meta strings (no parse/hash), and two metas that match it provably produce
    # the same invocation_id, so the memo only ever collapses true duplicates.
    inv_cache: dict = {}
    # Per-invocation output-slot allocation state (see the output-edge block).
    inv_cursor: dict = {}  # inv_id -> next slot to try
    rid_slot: dict = {}  # (inv_id, record_id) -> assigned slot

    for g in graph_records:
        meta = g.meta

        # repr() every field that can be a live dict/list (e.g. __constants /
        # __upstream / __inputs may arrive parsed, not as JSON strings) so the key
        # is always hashable. repr is deterministic for a given content+order, so
        # identical metas (a fan-out) share a key; any ordering difference only
        # costs a recompute (compute_invocation_id sorts bindings → same id).
        cache_key = (
            meta.get("__fn_hash"),
            repr(meta.get("__graph_var_bindings")),
            repr(meta.get("__upstream")),
            repr(meta.get("__inputs")),
            repr(meta.get("__constants")),
            repr(meta.get("__as_table")),
            bool(meta.get("__distribute", False)),
        )
        inv_id = inv_cache.get(cache_key)
        if inv_id is None:
            fn_name = meta.get("__fn") or function_name or "unknown"
            fn_hash = meta.get("__fn_hash") or ""

            var_b = _variable_bindings(meta)  # list of (param, rid, selector)
            const_b = _constant_bindings(meta)
            loadable_params = list(_parse_json_dict(meta.get("__inputs")).keys()) or [
                p for p, _r, _s in var_b
            ]
            as_table = _normalize_as_table(meta, loadable_params)
            distribute = bool(meta.get("__distribute", False))

            # Assemble the full binding set (variables + constants) and the
            # constant entity/value rows it implies. Bindings are
            # (param, record_id, selector) triples; constants carry no selector.
            bindings: list[tuple[str, str, str | None]] = list(var_b)
            for param, value in const_b.items():
                # canonical_hash drives the record id too, so hash once and derive
                # the id from it instead of calling compute_constant_record_id
                # (which would re-hash the value).
                ch = canonical_hash(value)
                crid = constant_record_id_from_hash(ch)
                bindings.append((param, crid, None))
                constant_rows[crid] = (
                    crid,
                    constant_value_repr(value),
                    constant_value_type(value),
                    ch,
                )
                entity_rows.setdefault(
                    crid, (crid, created_at, CONSTANT_TYPE, None, ch, None, False)
                )

            # Identity. ``bindings`` (var inputs + constant rids), ``as_table`` and
            # ``distribute`` here are assembled identically to invocation_id_for_meta,
            # so compute the id directly from them rather than re-deriving the whole
            # binding set + re-hashing every constant per record. The generates_file
            # lineage-only save calls invocation_id_for_meta, which shares these
            # same helpers, so the two paths still agree.
            inv_id = compute_invocation_id(
                meta.get("__fn_hash") or "", as_table, distribute, bindings
            )
            # Store NULL (not []) for "no aggregation" — avoids empty-list bind
            # ambiguity on the VARCHAR[] column; identity hashing treats them alike.
            invocation_rows[inv_id] = (
                inv_id,
                fn_name,
                fn_hash,
                as_table or None,
                distribute,
            )
            for param, rid, selector in bindings:
                input_edges[(inv_id, param, rid)] = selector

            # PathInput-spec edges: config-level (template+root_folder), recorded as
            # distinctly-typed input records so variant queries can surface them.
            # Added AFTER inv_id is computed → deliberately NOT part of identity.
            for param, spec in _pathinput_specs(meta).items():
                prid = compute_pathinput_record_id(spec)
                ch = canonical_hash(spec)
                constant_rows[prid] = (prid, spec, PATHINPUT_VALUE_TYPE, ch)
                entity_rows.setdefault(
                    prid, (prid, created_at, PATHINPUT_TYPE, None, ch, None, False)
                )
                input_edges[(inv_id, param, prid)] = None

            inv_cache[cache_key] = inv_id

        # Fix B (immutable cross-run slot assignment): the first time this run
        # touches an invocation, load its already-COMMITTED output slots into the
        # working state. New output records then probe past them and take the next
        # FREE output_num (append) instead of colliding on the
        # (invocation_id, output_num) PK and being silently dropped/orphaned — the
        # cause of the cross-run orphans when a deterministic for_each is invoked
        # again (e.g. a second PathInput over disjoint locations, sharing one
        # invocation_id). Nothing committed is overwritten or excluded; the slot
        # range simply grows. An idempotent re-save (same record_id) recognises its
        # committed slot via ``rid_slot`` and re-inserts the identical edge (a
        # DO NOTHING no-op).
        if inv_id not in seeded_invs:
            seeded_invs.add(inv_id)
            for _onum, _orid in duck._fetchall(
                "SELECT output_num, output_record_id FROM _invocation_output "
                "WHERE invocation_id = ?",
                [inv_id],
            ):
                output_edges.setdefault((inv_id, _onum), _orid)
                rid_slot.setdefault((inv_id, _orid), _onum)
                if _onum + 1 > inv_cursor.get(inv_id, 0):
                    inv_cursor[inv_id] = _onum + 1

        # Output edge. One call can emit MANY records that share an invocation
        # and arrive with the same nominal output_num — notably flatten/distribute
        # modes (a returned DataFrame spread into one record per row). They are
        # genuinely distinct outputs, so assign each the next free output_num for
        # this invocation rather than colliding on the _invocation_output PK. The
        # order is the deterministic collection (row) order, so re-runs reproduce
        # the same assignment. An idempotent re-save (same record_id) is not a
        # collision and keeps its slot.
        #
        # A monotonic per-invocation cursor makes this O(1) amortized: a big
        # distribute fan-out shares one base output_num, so probing from
        # g.output_num every time was O(n²). The cursor skips already-filled
        # slots; the while-loop only ever runs for interleaved multi-output bases,
        # so uniqueness/idempotency are preserved while the common path stays flat.
        existing = rid_slot.get((inv_id, g.record_id))
        if existing is not None:
            okey = (inv_id, existing)  # idempotent re-save keeps its slot
        else:
            n = max(g.output_num, inv_cursor.get(inv_id, 0))
            while (inv_id, n) in output_edges and output_edges[
                (inv_id, n)
            ] != g.record_id:
                n += 1
            okey = (inv_id, n)
            inv_cursor[inv_id] = n + 1
            rid_slot[(inv_id, g.record_id)] = n
        output_edges[okey] = g.record_id
        run_output_edges[okey] = g.record_id  # only this run's edges are inserted
        run_inv_ids.add(inv_id)

        # Output entity row (pull content_hash/schema_id from _record + latest save ts).
        cm = meta_map.get(g.record_id, {})
        entity_rows[g.record_id] = (
            g.record_id,
            cm.get("timestamp") or created_at,
            g.type_name,
            cm.get("schema_id"),
            cm.get("content_hash"),
            cm.get("schema_version")
            if cm.get("schema_version") is not None
            else g.schema_version,
            False,
        )

    timings["2_assemble_loop"] = time.perf_counter() - _t_assemble

    # Virtual glue nodes: the data is not saved, but the provenance node is.
    _t_glue = time.perf_counter()
    _write_glue_nodes(
        duck,
        created_at,
        glue_virtual or {},
        glue_chains or {},
        entity_rows,
        invocation_rows,
        input_edges,
        run_output_edges,
        run_inv_ids,
    )
    timings["2b_glue_nodes"] = time.perf_counter() - _t_glue

    run_id = generate_run_id()
    # constant_rows holds two kinds of input record: real constants and
    # PathInput specs (both are non-variable inputs, stored the same way and
    # distinguished by their value type). Count them apart — reporting a
    # PathInput-only run as "1 constant(s)" reads as a constant having leaked
    # into a run whose ``__constants`` was empty.
    pathinput_row_count = sum(
        1 for row in constant_rows.values() if row[2] == PATHINPUT_VALUE_TYPE
    )
    constant_row_count = len(constant_rows) - pathinput_row_count
    logger.debug(
        "record_run: run_id=%s fn=%s records=%d invocations=%d constants=%d "
        "pathinput_specs=%d edges_in=%d",
        run_id,
        function_name,
        len(graph_records),
        len(invocation_rows),
        constant_row_count,
        pathinput_row_count,
        len(input_edges),
    )
    _t_commit = time.perf_counter()
    _commit_graph(
        duck,
        run_id,
        created_at,
        user_id,
        function_name,
        where_clause,
        entity_rows,
        constant_rows,
        invocation_rows,
        input_edges,
        run_output_edges,
        run_inv_ids,
        timings=timings,
    )
    timings["3_commit"] = time.perf_counter() - _t_commit
    timings["total"] = time.perf_counter() - _t_start

    # Phase breakdown at INFO, not DEBUG. 67.1s for 419 records / 1 invocation
    # (2026-09-13) is ~100x worse per record than this function's own documented
    # optimized case (22.9s for 14253 records -> 1 invocation, see the inv_cache
    # comment above), and a single total cannot say whether that is the meta
    # fetch, the assemble loop, or the commit.
    Log.timings(
        f"record_run(fn={function_name})",
        timings,
        extra=(
            f"{len(graph_records)} record(s), {len(invocation_rows)} invocation(s), "
            f"{constant_row_count} constant(s), "
            f"{pathinput_row_count} PathInput spec(s), "
            f"{len(input_edges)} input edge(s)"
        ),
    )
    return run_id


def _write_glue_nodes(
    duck,
    created_at: str,
    glue_virtual: dict,
    glue_chains: dict,
    entity_rows: dict,
    invocation_rows: dict,
    input_edges: dict,
    run_output_edges: dict,
    run_inv_ids: set,
) -> None:
    """Add the virtual glue records + their invocations and edges.

    **The data is not saved, but the provenance node is.** For each input
    record flowing through a glue chain this writes:

    * one ``_record`` row, ``type = '__glue__'``, same ``schema_id`` as the
      record it reshapes — and *no* ``_record_save`` row and no data-table row,
      so it is never loadable, never "latest", and never in ``scidb report``;
    * one ``_invocation`` whose ``function_name`` is the chain (``glue_a >
      glue_b``) and whose ``function_hash`` is the chain hash;
    * its input edge (real record → glue) and output edge (glue → virtual).

    The consuming function's edges already point at the virtual rid, so
    ``invocation_id``, ``skip_computed`` staleness, ``upstream_provenance``'s
    BFS and node state all traverse **one extra hop with no special-casing**,
    and the pipeline reads honestly as ``RawEMG → glue_drop_baseline →
    analyze_emg``.

    Cost is ~4 metadata rows per (glue chain, input record) and zero data bytes.
    """
    if not glue_virtual:
        return

    from .log import Log
    from .provenance import (
        GLUE_INPUT_PARAM,
        GLUE_TYPE,
        compute_glue_invocation_id,
    )
    from .provenance_query import GLUE_NAME_SEPARATOR

    # schema_id of each source record — the virtual record sits at the same
    # schema location, so upstream_provenance and node state line up.
    source_rids = sorted(
        {src for _ch, _sig, mapping in glue_virtual.values() for src in mapping}
    )
    schema_by_rid: dict[str, Any] = {}
    if source_rids:
        placeholders = ", ".join(["?"] * len(source_rids))
        for rid, sid in duck._fetchall(
            f"SELECT record_id, schema_id FROM _record "
            f"WHERE record_id IN ({placeholders})",
            source_rids,
        ):
            schema_by_rid[rid] = sid

    n_records = 0
    for param, (chain_h, set_sig, mapping) in sorted(glue_virtual.items()):
        chain = glue_chains.get(param) or []
        display = (
            GLUE_NAME_SEPARATOR.join(getattr(s, "name", "glue") for s in chain)
            or "glue"
        )
        for src_rid, virt_rid in sorted(mapping.items()):
            inv_id = compute_glue_invocation_id(chain_h, src_rid, set_sig)
            entity_rows.setdefault(
                virt_rid,
                (
                    virt_rid,
                    created_at,
                    GLUE_TYPE,
                    schema_by_rid.get(src_rid),
                    None,
                    None,
                    False,
                ),
            )
            invocation_rows.setdefault(
                inv_id, (inv_id, display, chain_h, None, False)
            )
            input_edges.setdefault((inv_id, GLUE_INPUT_PARAM, src_rid), None)
            run_output_edges.setdefault((inv_id, 0), virt_rid)
            run_inv_ids.add(inv_id)
            n_records += 1

    Log.debug(
        f"[glue] wrote {n_records} virtual record(s) across "
        f"{len(glue_virtual)} glued param(s)"
    )


def _commit_graph(
    duck,
    run_id,
    created_at,
    user_id,
    function_name,
    where_clause,
    entity_rows,
    constant_rows,
    invocation_rows,
    input_edges,
    run_output_edges,
    run_inv_ids,
    timings: dict | None = None,
) -> None:
    """Transactionally insert the assembled graph rows + the append-only run.

    Used by :func:`record_run` (the for_each save path). All graph inserts are
    idempotent (``ON CONFLICT DO NOTHING``); the ``_run`` row is always appended.
    ``timings`` (optional) receives per-table elapsed times for diagnostics.
    """
    import time as _time

    timings = timings if timings is not None else {}

    def _timed(label, fn):
        _t = _time.perf_counter()
        fn()
        timings[label] = _time.perf_counter() - _t

    duck._begin()
    try:
        # Bulk vectorized inserts (see SciDuck._bulk_insert): per-row executemany
        # against these PK/composite-PK tables scaled to ~hundreds of seconds for
        # a for_each over thousands of records.
        _timed(
            "3a_record",
            lambda: duck._bulk_insert(
                "_record",
                (
                    "record_id",
                    "created_at",
                    "type",
                    "schema_id",
                    "content_hash",
                    "schema_version",
                    "excluded",
                ),
                entity_rows.values(),
                conflict_cols=["record_id"],
            ),
        )
        _timed(
            "3b_constant",
            lambda: duck._bulk_insert(
                "_constant",
                ("record_id", "value_repr", "value_type", "content_hash"),
                constant_rows.values(),
                conflict_cols=["record_id"],
            ),
        )
        _timed(
            "3c_invocation",
            lambda: duck._bulk_insert(
                "_invocation",
                (
                    "invocation_id",
                    "function_name",
                    "function_hash",
                    "as_table",
                    "distribute",
                ),
                invocation_rows.values(),
                conflict_cols=["invocation_id"],
            ),
        )
        _timed(
            "3d_invocation_input",
            lambda: duck._bulk_insert(
                "_invocation_input",
                ("invocation_id", "param_name", "input_record_id", "selector"),
                [
                    (inv, param, rid, sel)
                    for (inv, param, rid), sel in input_edges.items()
                ],
                conflict_cols=["invocation_id", "param_name", "input_record_id"],
            ),
        )
        # Invariant check (Fix B): with cross-run slot seeding, a NEW output record
        # is always assigned a FREE output_num, so an incoming edge must never
        # collide with a committed slot pointing at a DIFFERENT record_id. The only
        # legitimate "conflict" below is an idempotent re-insert of an IDENTICAL
        # edge (same record_id), which DO NOTHING no-ops. If a true collision is
        # seen here it means slot seeding failed (a Fix-B regression) and a record
        # would be orphaned — so warn loudly rather than dropping silently.
        if run_output_edges and run_inv_ids:
            from .log import Log

            _inv_list = list(run_inv_ids)
            _ph = ", ".join(["?"] * len(_inv_list))
            _existing = {
                (row[0], row[1]): row[2]
                for row in duck._fetchall(
                    f"SELECT invocation_id, output_num, output_record_id "
                    f"FROM _invocation_output WHERE invocation_id IN ({_ph})",
                    _inv_list,
                )
            }
            _dropped = [
                (inv, onum, rid, _existing[(inv, onum)])
                for (inv, onum), rid in run_output_edges.items()
                if (inv, onum) in _existing and _existing[(inv, onum)] != rid
            ]
            if _dropped:
                _samp = "; ".join(
                    f"(inv={i[:8]}…, output_num={o}): new={n[:8]}… vs committed={k[:8]}…"
                    for i, o, n, k in _dropped[:5]
                )
                Log.warn(
                    f"[provenance] INVARIANT VIOLATION: {len(_dropped)} output edge(s) "
                    f"collide with a committed slot at a DIFFERENT record_id and would "
                    f"be dropped (orphaning those records). Cross-run slot seeding "
                    f"(Fix B) should have assigned them fresh output_nums — this "
                    f"indicates a regression. Examples: {_samp}"
                )

        _timed(
            "3e_invocation_output",
            lambda: duck._bulk_insert(
                "_invocation_output",
                ("invocation_id", "output_num", "output_record_id"),
                [(inv, onum, rid) for (inv, onum), rid in run_output_edges.items()],
                conflict_cols=["invocation_id", "output_num"],
            ),
        )
        duck.con.execute(
            "INSERT INTO _run (run_id, timestamp, user_id, function_name, where_clause) "
            "VALUES (?, ?, ?, ?, ?)",
            [run_id, created_at, user_id, function_name, where_clause],
        )
        _timed(
            "3f_run_invocation",
            lambda: duck._bulk_insert(
                "_run_invocation",
                ("run_id", "invocation_id"),
                [(run_id, inv) for inv in run_inv_ids],
                conflict_cols=["run_id", "invocation_id"],
            ),
        )
        _timed("3g_commit", lambda: duck._commit())
    except Exception:
        logger.exception("graph commit failed; rolling back for run_id=%s", run_id)
        try:
            duck._rollback()
        except Exception:
            pass
        raise
