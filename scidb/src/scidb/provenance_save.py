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

The graph is built from each ``GraphRecord`` the save path hands over: its
TYPED input edges (``bindings``, from ``RunBindings.for_combo``) plus the
``save_metadata`` carrying ``__fn`` / ``__fn_hash`` (function identity),
``__constants`` (``{param: value}`` — constant inputs), the ``__as_table`` /
``__distribute`` identity flags, and ``__invocation_id`` — the identity the
save stamped into the record, which :func:`record_run` recomputes from the
edges and refuses to disagree with.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from scicanonicalhash import canonical_hash

from .bindings import Binding
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
    "check_selector_round_trip",
    "record_direct_save",
    "invocation_identity",
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

    Only ``ColumnSelection`` produces a selector — the chosen columns —
    because selecting different columns of the same record is a different
    computation (§ ColumnSelection decision). It is found ANYWHERE in the
    wrapper stack (``input_spec.find_wrapper``): ``Fixed(Var["a"], …)`` and
    ``Variant(Var["a"], …)`` both carry one, and enumerating the stackings
    by hand covered the first and not the second. Fixed/Variant/Merge
    themselves resolve to whole records and need no selector; their effect
    is captured by *which* record_id the edge points at.

    The shape and the stored spelling belong to ``scidb.intent`` (the
    ``columns`` aspect): this function decides WHICH inputs carry a selection,
    ``intent.selector_json`` decides what one looks like. They used to be two
    independent normalizers — this one and the GUI's — agreeing by convention,
    which is how a ``for_columns`` selection reached storage as "no selection".

    ``iterate`` (for_columns) rides along since 2026-09-19: a per-column run
    over every column has no column list, so it had no selector at all and a
    GUI re-run from history handed the function the whole table (integration
    suite, test_dag_runs). Only written when set, so an ordinary column
    selection keeps its old identity.
    """
    from scifor import ColumnSelection

    from .input_spec import find_wrapper
    from .intent import selector_json

    out: dict = {}
    for param, spec in inputs.items():
        # Anywhere in the wrapper stack (`input_spec.find_wrapper`): a
        # selection under a `Fixed` was handled, one under a `Variant` was
        # not, and reached the graph as "no selection".
        cs = find_wrapper(spec, ColumnSelection)
        # `None` for a whole-variable input, and for an empty non-iterate
        # selection — normalize_columns folds those together.
        out[param] = selector_json(cs) if cs is not None else None
    return out


def check_selector_round_trip(
    fn_name: str,
    asked: dict | None,
    recorded: dict | None,
    *,
    context: str = "",
) -> list[str]:
    """WARN for every param whose column selection was ASKED for and did not
    reach the recorded provenance edges. Returns the lost param names.

    The write half of the input-binding round trip
    (``docs/claude/input-binding-round-trip.md`` §5). *asked* is
    :func:`compute_input_selectors` — what the call requested. *recorded* is
    what the edges actually carry, ``{param: selector-or-None}``.

    Why this exists at all: until 2026-09-20 the save path had two edge
    assemblies, and the aggregation one wrote its edges from a dict with
    nowhere to put a selector, so every such edge carried ``selector=NULL``
    and a ``for_columns`` step re-ran from the canvas as a whole-table step.
    There is one assembly now (``RunBindings.for_combo``); this stays as the
    guard that an input the call selected on always reaches an edge.

    Log-only, and never raises: a lost selector is a diagnosis, not a reason
    to fail a run that has already computed its results. Same shape as the
    ``[coarse-input]`` line — both facts on one line, because a narrowing (or
    a widening) of what a function receives must never be silent.
    """
    from .log import Log

    asked = asked or {}
    recorded = recorded or {}
    lost = [
        param
        for param, sel in asked.items()
        if sel and not recorded.get(param)
    ]
    if not lost:
        return []
    where = f" {context}" if context else ""
    for param in lost:
        Log.warn(
            f"[selector-lost] {fn_name}{where}: input '{param}' was called with "
            f"{asked[param]} but its provenance edge recorded no selector, so "
            f"every reader of this run — a GUI re-run, an export, skip_computed "
            f"— will bind the WHOLE variable. See "
            f"docs/claude/input-binding-round-trip.md §4."
        )
    return lost


# A saved output record awaiting graph insertion.
#
# ``meta`` is its ``save_metadata`` dict (``__fn`` / ``__fn_hash`` /
# ``__constants`` / ``__as_table`` / ``__distribute`` / ``__invocation_id``).
# ``bindings`` is the TYPED edge list the save path assembled
# (``RunBindings.for_combo``) — the record's consumed inputs live here and
# nowhere else since 2026-09-20; ``__upstream`` and ``__graph_var_bindings``
# no longer exist. ``invocation_id`` is what the save stamped into the
# record's version keys; ``record_run`` recomputes it from ``bindings`` and
# refuses to write a graph that disagrees with the record's own identity.
class GraphRecord:
    __slots__ = (
        "type_name",
        "schema_version",
        "output_num",
        "record_id",
        "meta",
        "bindings",
        "invocation_id",
    )

    def __init__(
        self,
        type_name,
        schema_version,
        output_num,
        record_id,
        meta,
        bindings=(),
        invocation_id=None,
    ):
        self.type_name = type_name
        self.schema_version = schema_version
        self.output_num = output_num
        self.record_id = record_id
        self.meta = meta
        self.bindings = [Binding.coerce(b) for b in bindings]
        self.invocation_id = invocation_id


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


def _constant_bindings(meta: dict) -> dict[str, Any]:
    """Constant inputs from ``__constants`` → ``{param_name: value}``."""
    return _parse_json_dict(meta.get("__constants"))


def _pathinput_specs(meta: dict) -> dict[str, str]:
    """PathInput identity keys from ``__inputs`` → ``{param_name: key_json_str}``.

    ``__inputs`` carries each loadable input's ``to_key()``; a PathInput's is its
    NAME as JSON (``{"__type": "PathInput", "name": ...}``). The full spec is
    not in the metadata (it would reach the record id) — ``record_run`` gets it
    as ``path_input_specs``.
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


def _log_moved_path_inputs(duck, pathinput_spec_rows: dict[str, str]) -> None:
    """INFO for every PathInput whose stored spec this run changes — the
    "data moved / other machine" case. Identity (the name) is unchanged, so
    nothing re-runs because of it; this line is how the move is visible."""
    if not pathinput_spec_rows:
        return
    from .log import Log
    from .provenance import parse_path_input_spec

    ids = list(pathinput_spec_rows)
    stored = dict(
        duck._fetchall(
            f"SELECT record_id, value_repr FROM _constant WHERE record_id IN "
            f"({', '.join(['?'] * len(ids))})",
            ids,
        )
    )
    for rid, spec in pathinput_spec_rows.items():
        old = stored.get(rid)
        if old is None or old == spec:
            continue
        before, after = parse_path_input_spec(old) or {}, parse_path_input_spec(spec) or {}
        Log.info(
            f"[provenance] PathInput {after.get('name')!r}: location changed "
            f"({before.get('template')!r}, root {before.get('root_folder')!r}) -> "
            f"({after.get('template')!r}, root {after.get('root_folder')!r}); "
            f"identity is the name, so nothing re-runs because of it"
        )


def _pathinput_bindings(meta: dict) -> list[Binding]:
    """The PathInput edges of a call, as identity bindings — one per
    PathInput-fed parameter, bound to the spec's ``__pathinput__`` record.

    Part of ``invocation_id`` since 2026-09-25 (reversing the 2026-06-21
    WON'T DO, ``docs/claude/database-model.md`` §11 item 6): without it a
    function whose only input is a PathInput — a library loader such as
    ``pandas.read_csv`` — got ONE invocation for every file it ever read, so
    two canvas nodes reading two files merged into one call site and their
    wiring was rewritten onto it. The binding is the PathInput's NAME (its
    ``to_key()``), never its template or root_folder, so moved data keeps its
    invocations.
    """
    return [
        Binding(param, compute_pathinput_record_id(spec), None)
        for param, spec in sorted(_pathinput_specs(meta).items())
    ]


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


def invocation_identity(meta: dict, bindings) -> str:
    """The ``invocation_id`` of a call, from its save metadata and its TYPED
    input edges — the ONE recipe.

    Two callers, and they must agree by construction: the save path stamps
    the result into the record's version keys as ``__invocation_id`` (so a
    record's identity depends on the exact invocation that produced it —
    function hash, run options, every edge with its selector, every
    constant), and :func:`record_run` recomputes it from the edges it is
    about to write and refuses to write a graph that disagrees. Before
    2026-09-20 the record side used a second, dict-shaped spelling of the
    same rids (``__upstream``, with indexed keys) that was included in
    identity while the typed edge list was excluded; there is one spelling
    now.

    *bindings* are the variable edges (``bindings.Binding``); the constants
    come from ``meta["__constants"]`` and the PathInput specs from
    ``meta["__inputs"]`` (:func:`_pathinput_bindings`), and become edges here.
    """
    from .provenance import compute_invocation_id

    var_b = [Binding.coerce(b) for b in bindings]
    const_b = _constant_bindings(meta)
    loadable_params = list(_parse_json_dict(meta.get("__inputs")).keys()) or [
        b.param for b in var_b
    ]
    as_table = _normalize_as_table(meta, loadable_params)
    distribute = bool(meta.get("__distribute", False))
    edges: list[Binding] = list(var_b)
    for param, value in const_b.items():
        edges.append(Binding(param, compute_constant_record_id(value), None))
    edges.extend(_pathinput_bindings(meta))
    return compute_invocation_id(
        meta.get("__fn_hash") or "",
        as_table,
        distribute,
        edges,
        across_variants=_across_variants(meta),
    )


def _across_variants(meta: dict) -> list[str]:
    """The params ``__across_variants`` names (pooled across every variant
    group), or ``[]``."""
    raw = meta.get("__across_variants")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except (json.JSONDecodeError, TypeError):
            raw = []
    return sorted(str(p) for p in (raw or []))


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
    parameter_names: dict | None = None,
    path_input_specs: dict | None = None,
) -> str | None:
    """Write the bipartite graph for ``graph_records`` plus a fresh ``_run`` row.

    ``path_input_specs`` is ``{argument: PathInput.to_spec()}`` — what each
    PathInput-fed argument's record STORES (template, root_folder, ...). The
    identity is the name in ``__inputs``; the spec rides beside it, like
    ``parameter_names``, because anything in the metadata would reach the
    record id. The stored spec is refreshed to this run's, so re-discovery
    looks where the files are now.

    Returns the ``run_id`` (or ``None`` if there was nothing to record).

    Idempotent for the graph (``ON CONFLICT DO NOTHING``); the ``_run`` /
    ``_run_invocation`` rows are always appended so the audit log captures this
    execution even when it reproduced existing invocations.

    ``glue_virtual`` / ``glue_chains`` add the glue hop: the consumer's
    bindings already point at virtual glue rids (``scidb.glue.fuse_glue``
    rewrote ``__record_id`` before Step 11), so the matching virtual ``_record``
    rows and their producing ``_invocation``s are written here. See
    :func:`_write_glue_nodes`.

    ``parameter_names`` (``{argument: declared Parameter or PathInput name}``,
    from :func:`scidb.parameter.declared_input_names`) is stamped on each
    CONSTANT or PathInput edge's ``declared_name`` so history can name the Parameter the
    canvas shows. Descriptive, not identity; the latest run's name wins on an
    edge an earlier run already wrote.
    """
    if not graph_records:
        return None
    parameter_names = dict(parameter_names or {})
    path_input_specs = dict(path_input_specs or {})

    import time

    from .intent import parse_selector
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
    pathinput_spec_rows: dict[str, str] = {}  # PathInput record_id -> spec to store
    invocation_rows: dict[str, tuple] = {}  # invocation_id -> _invocation row
    input_edges: dict[
        tuple[str, str, str], str | None
    ] = {}  # (inv,param,rid) -> selector
    # (inv,param,rid) -> declared Parameter name, constant edges only
    declared_edges: dict[tuple[str, str, str], str] = {}
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
        # __inputs may arrive parsed, not as JSON strings) so the key is always
        # hashable. repr is deterministic for a given content+order, so
        # identical metas (a fan-out) share a key; any ordering difference only
        # costs a recompute (compute_invocation_id sorts bindings → same id).
        cache_key = (
            meta.get("__fn_hash"),
            tuple(sorted(tuple(b) for b in g.bindings)),
            repr(meta.get("__inputs")),
            repr(meta.get("__constants")),
            repr(meta.get("__as_table")),
            bool(meta.get("__distribute", False)),
            tuple(_across_variants(meta)),
        )
        inv_id = inv_cache.get(cache_key)
        if inv_id is None:
            fn_name = meta.get("__fn") or function_name or "unknown"
            fn_hash = meta.get("__fn_hash") or ""

            var_b = list(g.bindings)  # the typed edges the save path assembled
            const_b = _constant_bindings(meta)
            loadable_params = list(_parse_json_dict(meta.get("__inputs")).keys()) or [
                b.param for b in var_b
            ]
            as_table = _normalize_as_table(meta, loadable_params)
            distribute = bool(meta.get("__distribute", False))
            across_variants = _across_variants(meta)

            # Assemble the full binding set (variables + constants) and the
            # constant entity/value rows it implies; constants carry no selector.
            bindings: list[Binding] = list(var_b)
            for param, value in const_b.items():
                # canonical_hash drives the record id too, so hash once and derive
                # the id from it instead of calling compute_constant_record_id
                # (which would re-hash the value).
                ch = canonical_hash(value)
                crid = constant_record_id_from_hash(ch)
                bindings.append(Binding(param, crid, None))
                constant_rows[crid] = (
                    crid,
                    constant_value_repr(value),
                    constant_value_type(value),
                    ch,
                )
                entity_rows.setdefault(
                    crid, (crid, created_at, CONSTANT_TYPE, None, ch, None, False)
                )
            # PathInput-spec edges are identity too (`_pathinput_bindings`):
            # two files read by one function are two invocations.
            pathinput_b = _pathinput_bindings(meta)
            bindings.extend(pathinput_b)
            if pathinput_b:
                Log.debug(
                    f"[provenance] fn={fn_name}: {len(pathinput_b)} PathInput "
                    f"edge(s) folded into invocation "
                    f"{dict((b.param, b.rid) for b in pathinput_b)}"
                )

            # Identity — the same recipe as `invocation_identity`, spelled with
            # the constant hashes already in hand. The save path stamped ITS
            # answer into the record's version keys (`__invocation_id`), and a
            # graph that disagreed with the record's own identity would be a
            # bug of exactly the class this check exists to catch, so it is a
            # hard error rather than a warning.
            inv_id = compute_invocation_id(
                meta.get("__fn_hash") or "",
                as_table,
                distribute,
                bindings,
                across_variants=across_variants,
            )
            stamped = g.invocation_id or meta.get("__invocation_id")
            if stamped and stamped != inv_id:
                raise RuntimeError(
                    f"identity drift for {fn_name} record {g.record_id}: the save "
                    f"stamped invocation {stamped} but the edges it handed over "
                    f"compute {inv_id} — {len(var_b)} variable edge(s) "
                    f"{sorted((b.param, b.rid[:8]) for b in var_b)}, constants "
                    f"{sorted(const_b)}. The record and the graph would name "
                    f"different invocations; nothing was written."
                )
            # Which params ran once per column, DERIVED from the edges'
            # selectors rather than carried separately — one source of truth,
            # so the column can never disagree with the selector it describes.
            # Descriptive only: `iterate` is already inside the selector that
            # compute_invocation_id folds in, so recording it again as an
            # identity term would count the same fact twice.
            for_columns = sorted(
                {
                    b.param
                    for b in var_b
                    if (parse_selector(b.selector) or {}).get("iterate")
                }
            )
            # Store NULL (not []) for "no aggregation" — avoids empty-list bind
            # ambiguity on the VARCHAR[] column; identity hashing treats them alike.
            invocation_rows[inv_id] = (
                inv_id,
                fn_name,
                fn_hash,
                as_table or None,
                distribute,
                for_columns or None,
                across_variants or None,
            )
            for b in bindings:
                input_edges[(inv_id, b.param, b.rid)] = b.selector
            for param in const_b:
                declared = parameter_names.get(param)
                if declared:
                    crid = next(b.rid for b in bindings if b.param == param)
                    declared_edges[(inv_id, param, crid)] = declared

            # PathInput records: ONE per name (the identity key), distinctly
            # typed so variant queries can surface them. The stored value is
            # the full SPEC (template, root_folder, ...) the run actually used,
            # for display and re-discovery — never hashed. Their edges were
            # written with `bindings` above — they are part of identity.
            for param, key in _pathinput_specs(meta).items():
                prid = compute_pathinput_record_id(key)
                spec = path_input_specs.get(param) or key
                ch = canonical_hash(key)
                constant_rows[prid] = (prid, spec, PATHINPUT_VALUE_TYPE, ch)
                pathinput_spec_rows[prid] = spec
                entity_rows.setdefault(
                    prid, (prid, created_at, PATHINPUT_TYPE, None, ch, None, False)
                )
                # Which DECLARED PathInput fed this argument — what groups
                # PathInput-fed steps, on the canvas and in `scidb graph`
                # alike (cleanup-audit F38). Same column as a Parameter's.
                declared = parameter_names.get(param)
                if declared:
                    declared_edges[(inv_id, param, prid)] = declared

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
    # The argument -> Parameter naming this run recorded (cleanup-audit B1).
    # INFO: it is the one line that says why the canvas does, or does not,
    # draw a second Parameter node after a run.
    if parameter_names:
        renamed = {a: d for a, d in sorted(parameter_names.items()) if a != d}
        n_pathinput = sum(1 for (_i, _p, rid) in declared_edges if rid in constant_rows
                          and constant_rows[rid][2] == PATHINPUT_VALUE_TYPE)
        Log.info(
            f"[provenance] fn={function_name}: {len(declared_edges) - n_pathinput} "
            f"constant edge(s) named by declared Parameter, {n_pathinput} "
            f"PathInput edge(s) named by declared PathInput; argument->declared "
            f"{renamed or 'all same-named'}"
        )
    _log_moved_path_inputs(duck, pathinput_spec_rows)
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
        declared_edges=declared_edges,
        pathinput_spec_rows=pathinput_spec_rows,
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
    declared_edges: dict | None = None,
    pathinput_spec_rows: dict | None = None,
) -> None:
    """Transactionally insert the assembled graph rows + the append-only run.

    Used by :func:`record_run` (the for_each save path). All graph inserts are
    idempotent (``ON CONFLICT DO NOTHING``); the ``_run`` row is always appended.
    ``timings`` (optional) receives per-table elapsed times for diagnostics.
    """
    import time as _time

    timings = timings if timings is not None else {}
    declared_edges = declared_edges or {}

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
        # A PathInput's record is keyed by its NAME, so a run over moved files
        # hits the existing row (DO NOTHING above); its stored spec is
        # refreshed here so display and re-discovery follow the files.
        if pathinput_spec_rows:
            _timed(
                "3b_pathinput_spec",
                lambda: duck._bulk_update(
                    "_constant",
                    ("record_id",),
                    ("value_repr",),
                    [(rid, spec) for rid, spec in pathinput_spec_rows.items()],
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
                    "for_columns",
                    "across_variants",
                ),
                invocation_rows.values(),
                conflict_cols=["invocation_id"],
            ),
        )
        _timed(
            "3d_invocation_input",
            lambda: duck._bulk_insert(
                "_invocation_input",
                (
                    "invocation_id",
                    "param_name",
                    "input_record_id",
                    "selector",
                    "declared_name",
                ),
                [
                    (inv, param, rid, sel, declared_edges.get((inv, param, rid)))
                    for (inv, param, rid), sel in input_edges.items()
                ],
                conflict_cols=["invocation_id", "param_name", "input_record_id"],
            ),
        )
        # An edge an EARLIER run wrote keeps its row (DO NOTHING above), so
        # its declared name is refreshed here: the latest run's naming is the
        # one the canvas should show.
        if declared_edges:
            _timed(
                "declared_names",
                lambda: duck._bulk_update(
                    "_invocation_input",
                    ("invocation_id", "param_name", "input_record_id"),
                    ("declared_name",),
                    [(i, p, r, d) for (i, p, r), d in declared_edges.items()],
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
        # `origin` is ambient (scidb.intent.current_origin): set by whoever
        # started the run, read here by the one place that records it.
        from .intent import current_origin

        duck.con.execute(
            "INSERT INTO _run (run_id, timestamp, user_id, function_name, "
            "where_clause, origin) VALUES (?, ?, ?, ?, ?, ?)",
            [run_id, created_at, user_id, function_name, where_clause, current_origin()],
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
