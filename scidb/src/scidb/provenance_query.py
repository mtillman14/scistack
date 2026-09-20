"""Read side of the bipartite provenance graph.

Pure SQL traversal over ``_invocation`` / ``_invocation_input`` /
``_invocation_output`` (plus ``_record`` / ``_constant``), replacing the old
``version_keys`` / ``branch_params`` JSON-parsing heuristics with
provably-correct, indexable edge walks. See ``docs/claude/lineage-simplification.md``
§6 (derived branch_params), §8 (pipeline reconstruction), §9b (execution audit).

``DatabaseManager`` methods delegate here; the functions take a ``DatabaseManager``
(for ``_duck`` and ``dataset_schema_keys``) or a raw ``SciDuck``.
"""

from __future__ import annotations

import ast
import json
import logging

from .bindings import signature_conflicts_with, variant_signature
from .database import _from_schema_str
from .provenance import (
    CONSTANT_TYPE,
    GLUE_TYPE,
    PATHINPUT_TYPE,
    SAVE_FUNCTION_NAME,
    constants_identity_key,
)

logger = logging.getLogger(__name__)

# How a glue chain's node names are joined into ``_invocation.function_name``
# (``glue_a > glue_b``). One separator, defined once, so the writer in
# provenance_save and the reader below cannot drift.
GLUE_NAME_SEPARATOR = " > "


def _safe_literal(value_repr):
    """Recover a typed constant value from its ``repr`` (``_constant.value_repr``).

    ``value_repr`` is ``repr(value)``, so ``ast.literal_eval`` round-trips
    int/float/str/bool/None/tuple/list/dict-of-literals back to the original
    value. Non-literal reprs (e.g. numpy) fall back to the raw string.
    """
    if value_repr is None:
        return None
    try:
        return ast.literal_eval(value_repr)
    except (ValueError, SyntaxError):
        return value_repr


# ---------------------------------------------------------------------------
# Primitive lookups
# ---------------------------------------------------------------------------
def producing_invocation(duck, record_id: str):
    """The invocation that produced ``record_id`` → ``(inv_id, fn_name, fn_hash)``
    or ``None`` for raw/manual records (no producing invocation)."""
    rows = duck._fetchall(
        "SELECT io.invocation_id, inv.function_name, inv.function_hash "
        "FROM _invocation_output io "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "WHERE io.output_record_id = ? "
        "ORDER BY io.invocation_id LIMIT 1",
        [record_id],
    )
    return rows[0] if rows else None


def output_num_for(duck, record_id: str):
    """The ``output_num`` slot this record occupies on its producing invocation,
    or ``None`` if it has no producing invocation. Distinguishes the multiple
    records a single flatten/distribute call emits."""
    rows = duck._fetchall(
        "SELECT output_num FROM _invocation_output WHERE output_record_id = ? LIMIT 1",
        [record_id],
    )
    return rows[0][0] if rows else None


# ---------------------------------------------------------------------------
# Batched lookups — same results as the per-record primitives above, but built
# with O(depth) bulk queries instead of O(records × depth) round-trips. Used by
# the hot load paths (_find_record collapse, _assemble_df_from_records_and_data)
# where the per-record form was the dominant cost on large result sets.
# ---------------------------------------------------------------------------
def _chunked_in(duck, sql_template: str, ids, tail_params=None, chunk: int = 900):
    """Run ``sql_template`` (containing a single ``{ph}`` placeholder-list slot)
    over ``ids`` in chunks, returning the concatenated rows.

    ``tail_params`` are appended after the id placeholders on every chunk (for
    queries with trailing constant params, e.g. type filters).
    """
    tail_params = list(tail_params or [])
    out: list = []
    ids = list(ids)
    for start in range(0, len(ids), chunk):
        chunk_ids = ids[start : start + chunk]
        placeholders = ", ".join(["?"] * len(chunk_ids))
        sql = sql_template.format(ph=placeholders)
        out.extend(duck._fetchall(sql, chunk_ids + tail_params))
    return out


def producing_invocations_batch(duck, record_ids) -> dict:
    """**Every** invocation that produced each record →
    ``{record_id: [(inv_id, fn_name, fn_hash), ...]}``, ascending by ``inv_id``.

    Records with no producing invocation (raw/manual saves) are absent.

    Today's save path writes at most one producer per record: ``record_id``
    hashes the content **and** the save metadata (``__fn_hash``, the input
    rids, the run options), so a re-run after a code edit or under a different
    ``distribute``/``as_table`` lands on a *new* record rather than on this one
    (measured 2026-09-16). Re-running the *same* recipe reuses both the record
    and the invocation and appends a ``_run`` row — that reproduction is read
    off :func:`runs_for_invocations_batch`, not here.

    The schema still permits several producers (``_invocation_output`` has no
    uniqueness on ``output_record_id``), and a writer outside this save path —
    the MATLAB bridge, an import, a future identity change — could create
    them. This is the read that would report them; the singular
    :func:`producing_invocation` / :func:`producing_invocation_batch` keep
    picking one (the lowest id) because variant *identity* has to be a single
    value.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT io.output_record_id, io.invocation_id, inv.function_name, inv.function_hash "
        "FROM _invocation_output io "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "WHERE io.output_record_id IN ({ph})",
        ids,
    )
    out: dict = {}
    for out_rid, inv_id, fn_name, fn_hash in rows:
        out.setdefault(out_rid, []).append((inv_id, fn_name, fn_hash))
    for producers in out.values():
        producers.sort(key=lambda p: p[0])
    multi = sum(1 for producers in out.values() if len(producers) > 1)
    if multi:
        logger.info(
            "producing_invocations_batch: %d/%d record(s) have more than one "
            "producing invocation (a second writer claimed the record; the "
            "for_each save path never does this)",
            multi,
            len(out),
        )
    return out


def producing_invocation_batch(duck, record_ids) -> dict:
    """Batched :func:`producing_invocation`.

    ``{record_id: (inv_id, fn_name, fn_hash)}`` for records that have a producing
    invocation (raw/manual records are absent from the map). Matches the
    per-record function's "lowest invocation_id wins" tie-break.

    **Deliberately lossy**: should a record ever carry a second producing
    invocation (see :func:`producing_invocations_batch` for when that can and
    cannot happen), the later one is dropped here. That is right for the
    callers of this function — variant identity, the ``_find_record`` latest
    collapse, the ``function_hash`` a trace node reports — each of which needs
    ONE value and must keep reporting the one it always has. A caller that
    wants every producer reads the plural function instead.
    """
    return {
        rid: producers[0]
        for rid, producers in producing_invocations_batch(duck, record_ids).items()
    }


def runs_for_invocations_batch(duck, invocation_ids) -> dict:
    """``{invocation_id: [(run_id, timestamp, user_id, where_clause), ...]}``,
    oldest run first — the ``_run_invocation`` ⨝ ``_run`` join, batched.

    One (chunked) query per call, never one per node: a trace tree resolves
    every invocation in it together. ``_run`` appends a row per *execution*, so
    an invocation re-produced by five runs has five rows here, and
    ``where_clause`` is display-only audit text (§10) — never parsed, never
    used to decide what a record is.
    """
    ids = list(dict.fromkeys(i for i in invocation_ids if i))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT ri.invocation_id, run.run_id, run.timestamp, run.user_id, "
        "run.where_clause "
        "FROM _run_invocation ri "
        "JOIN _run run ON run.run_id = ri.run_id "
        "WHERE ri.invocation_id IN ({ph})",
        ids,
    )
    out: dict = {}
    for inv_id, run_id, ts, uid, where in rows:
        out.setdefault(inv_id, []).append((run_id, ts, uid, where))
    for runs in out.values():
        runs.sort(key=lambda r: (r[1] or "", r[0] or ""))
    missing = [i for i in ids if i not in out]
    if missing:
        # Not an error: a terminal MATLAB run and any pre-``_run`` database
        # leave invocations nothing ever claimed. Logged so "no runs" is never
        # read as "the join is broken".
        logger.info(
            "runs_for_invocations_batch: %d/%d invocation(s) have no _run row "
            "(produced outside a tracked for_each execution)",
            len(missing),
            len(ids),
        )
    return out


def glue_source_batch(duck, virtual_record_ids) -> dict:
    """Batched :func:`glue_source` — ``{virtual_record_id: {record_id,
    variable_type, chain_names, chain_hash}}``, with the same
    lowest-``input_record_id`` tie-break as the per-record function."""
    ids = list(dict.fromkeys(virtual_record_ids))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT io.output_record_id, ii.input_record_id, inv.function_name, "
        "inv.function_hash, r.type "
        "FROM _invocation_output io "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "JOIN _invocation_input ii ON ii.invocation_id = io.invocation_id "
        "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE io.output_record_id IN ({ph})",
        ids,
    )
    best: dict = {}
    for out_rid, src_rid, fn_name, fn_hash, src_type in rows:
        prev = best.get(out_rid)
        if prev is None or src_rid < prev[0]:
            best[out_rid] = (src_rid, fn_name, fn_hash, src_type)
    return {
        out_rid: {
            "record_id": src_rid,
            "variable_type": src_type,
            "chain_names": (fn_name or "").split(GLUE_NAME_SEPARATOR),
            "chain_hash": fn_hash or "",
        }
        for out_rid, (src_rid, fn_name, fn_hash, src_type) in best.items()
    }


def invocation_call_ids_batch(duck, invocation_ids) -> dict:
    """``{invocation_id: call_id | None}`` — the for_each call site each
    invocation belongs to, reconstructed from its stored wiring.

    ``call_id`` is not a column: it is derived from the call's config
    (``__fn`` / ``__inputs`` / ``__constants`` / ``__distribute`` /
    ``__as_table`` / ``__glue``), and :func:`config_call_id` owns that recipe so
    this reverse direction keeps matching the forward
    ``ForEachConfig.to_call_id``. Batched — a bounded number of queries however
    many invocations are asked about, because a trace tree resolves them all at
    once (the N+1 rule).

    ``None`` for a glue invocation and for the synthetic ``__save__`` anchor:
    neither is a pipeline step (D5), so neither has a call site.
    """
    ids = list(dict.fromkeys(i for i in invocation_ids if i))
    if not ids:
        return {}
    inv_rows = _chunked_in(
        duck,
        "SELECT invocation_id, function_name, as_table, distribute "
        "FROM _invocation WHERE invocation_id IN ({ph})",
        ids,
    )
    edge_rows = _chunked_in(
        duck,
        "SELECT ii.invocation_id, ii.param_name, ii.input_record_id, r.type, "
        "c.value_repr "
        "FROM _invocation_input ii "
        "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "LEFT JOIN _constant c ON c.record_id = ii.input_record_id "
        "WHERE ii.invocation_id IN ({ph})",
        ids,
    )
    glue_srcs = glue_source_batch(
        duck, {row[2] for row in edge_rows if row[3] == GLUE_TYPE}
    )
    glue_invs = glue_invocation_ids(duck)

    def _empty_cfg() -> dict:
        return {
            "input_types": {},
            "constants": {},
            "path_inputs": {},
            "glue_chains": {},
        }

    cfgs: dict = {}
    for inv_id, param, in_rid, rtype, value_repr in edge_rows:
        cfg = cfgs.setdefault(inv_id, _empty_cfg())
        if rtype == PATHINPUT_TYPE:
            cfg["path_inputs"][param] = value_repr
        elif rtype == CONSTANT_TYPE:
            cfg["constants"][param] = _safe_literal(value_repr)
        elif rtype == GLUE_TYPE:
            src = glue_srcs.get(in_rid) or {}
            cfg["input_types"][param] = src.get("variable_type") or GLUE_TYPE
            cfg["glue_chains"][param] = (
                src.get("chain_hash") or "",
                tuple(src.get("chain_names") or ()),
            )
        else:
            cfg["input_types"][param] = rtype

    out: dict = {}
    for inv_id, fn_name, as_table, distribute in inv_rows:
        if inv_id in glue_invs or fn_name == SAVE_FUNCTION_NAME:
            out[inv_id] = None
            continue
        cfg = {
            **cfgs.get(inv_id, _empty_cfg()),
            "as_table": sorted(as_table) if as_table else [],
            "distribute": bool(distribute),
        }
        out[inv_id] = config_call_id(fn_name, cfg)
    return out


def output_num_batch(duck, record_ids) -> dict:
    """Batched :func:`output_num_for` — ``{record_id: output_num}`` for records
    with a producing invocation (lowest invocation_id wins, mirroring LIMIT 1)."""
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT output_record_id, invocation_id, output_num "
        "FROM _invocation_output WHERE output_record_id IN ({ph})",
        ids,
    )
    best: dict = {}
    out: dict = {}
    for out_rid, inv_id, onum in rows:
        if out_rid not in best or inv_id < best[out_rid]:
            best[out_rid] = inv_id
            out[out_rid] = onum
    return out


def _build_upstream_closure(duck, seed_record_ids, max_depth: int = 20):
    """Load the full upstream subgraph reachable from ``seed_record_ids`` into
    in-memory adjacency maps using O(max_depth) batched queries.

    Returns ``(rec_to_inv, inv_constants, inv_var_inputs, inv_fn_hash,
    inv_run_options)`` where:

    * ``rec_to_inv``: ``{record_id: (inv_id, fn_name)}`` for produced records
    * ``inv_constants``: ``{inv_id: {f"{fn_name}.{param}": value}}``
    * ``inv_var_inputs``: ``{inv_id: [input_record_id, ...]}`` (variable inputs
      only; constants and PathInput specs excluded — matching
      :func:`invocation_inputs`)
    * ``inv_fn_hash``: ``{inv_id: function_hash}`` — what :func:`code_versions_batch`
      needs, carried here so the two walks share one closure build rather than
      querying the same subgraph twice.
    * ``inv_run_options``: ``{inv_id: label}`` — the invocation's identity-bearing
      run options (``distribute``/``as_table``) as :func:`run_options_label`
      spells them. Same reasoning as ``inv_fn_hash``: a re-run under different
      options is a different invocation writing to the same location, and the
      chain walk is where that becomes visible (:func:`run_options_batch`).

    Together these let a caller reproduce :func:`derived_branch_params` for every
    seed with a pure-Python walk and zero further DB round-trips.
    """
    rec_to_inv: dict = {}
    inv_constants: dict = {}
    inv_var_inputs: dict = {}
    inv_fn_name: dict = {}  # invocation_id -> function_name (for constant namespacing)
    inv_fn_hash: dict = {}  # invocation_id -> function_hash (for code versions)
    inv_run_options: dict = {}  # invocation_id -> run-options label

    seen_records: set = set()
    frontier = list(dict.fromkeys(seed_record_ids))
    depth = 0
    while frontier and depth <= max_depth:
        new_records = [r for r in frontier if r not in seen_records]
        seen_records.update(new_records)
        if not new_records:
            break

        # 1) producing invocation (+ fn_name) for each frontier record.
        inv_rows = _chunked_in(
            duck,
            "SELECT io.output_record_id, io.invocation_id, inv.function_name, "
            "inv.function_hash, inv.distribute, inv.as_table, inv.for_columns, "
            "inv.across_variants "
            "FROM _invocation_output io "
            "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
            "WHERE io.output_record_id IN ({ph})",
            new_records,
        )
        for (
            out_rid,
            inv_id,
            fn_name,
            fn_hash,
            distribute,
            as_table,
            for_columns,
            across_variants,
        ) in inv_rows:
            prev = rec_to_inv.get(out_rid)
            if prev is None or inv_id < prev[0]:
                rec_to_inv[out_rid] = (inv_id, fn_name)
            inv_fn_name[inv_id] = fn_name
            inv_fn_hash[inv_id] = fn_hash
            inv_run_options[inv_id] = run_options_label(
                distribute, as_table, for_columns, across_variants
            )

        # 2) inputs for the newly discovered invocations (skip ones already loaded).
        inv_ids = list(
            dict.fromkeys(
                rec_to_inv[r][0]
                for r in new_records
                if r in rec_to_inv and rec_to_inv[r][0] not in inv_var_inputs
            )
        )
        if not inv_ids:
            depth += 1
            frontier = []
            continue

        in_rows = _chunked_in(
            duck,
            "SELECT ii.invocation_id, ii.param_name, ii.input_record_id, r.type, c.value_repr "
            "FROM _invocation_input ii "
            "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
            "LEFT JOIN _constant c ON c.record_id = ii.input_record_id "
            "WHERE ii.invocation_id IN ({ph})",
            inv_ids,
        )
        for inv_id in inv_ids:
            inv_var_inputs.setdefault(inv_id, [])
            inv_constants.setdefault(inv_id, {})
        next_frontier: list = []
        var_pairs: dict = {}  # inv_id -> [(param_name, in_rid), ...] for stable sort
        for inv_id, param_name, in_rid, rtype, value_repr in in_rows:
            if rtype == PATHINPUT_TYPE:
                continue  # PathInput spec — neither variable nor sweep constant
            if rtype == CONSTANT_TYPE:
                # Namespace by the producing function name (as derived_branch_params).
                fn_name = inv_fn_name.get(inv_id)
                inv_constants[inv_id][f"{fn_name}.{param_name}"] = _safe_literal(
                    value_repr
                )
            else:
                var_pairs.setdefault(inv_id, []).append((param_name, in_rid))
                next_frontier.append(in_rid)
        # Match invocation_inputs' sort (param_name, record_id) so the per-record
        # DFS visits ancestors in the same order as derived_branch_params.
        for inv_id, pairs in var_pairs.items():
            inv_var_inputs[inv_id] = [rid for _p, rid in sorted(pairs)]

        depth += 1
        frontier = next_frontier

    return rec_to_inv, inv_constants, inv_var_inputs, inv_fn_hash, inv_run_options


def branch_params_batch(duck, record_ids, max_depth: int = 20) -> dict:
    """Batched :func:`derived_branch_params` — ``{record_id: {fn.param: value}}``.

    Builds the upstream closure once (O(max_depth) bulk queries), then accumulates
    each requested record's branch params with an in-memory walk identical in
    semantics to the per-record version (same DFS order, same last-write-wins on
    a namespaced-key collision), so results match byte-for-byte.
    """
    seeds = list(dict.fromkeys(record_ids))
    if not seeds:
        return {}
    rec_to_inv, inv_constants, inv_var_inputs, _fn_hash, _run = _build_upstream_closure(
        duck, seeds, max_depth
    )
    out: dict = {}
    for seed in seeds:
        bp: dict = {}
        visited: set = set()
        stack = [(seed, 0)]
        while stack:
            cur, depth = stack.pop()
            if cur in visited or depth > max_depth:
                continue
            visited.add(cur)
            inv = rec_to_inv.get(cur)
            if inv is None:
                continue
            inv_id, _fn_name = inv
            for nkey, value in inv_constants.get(inv_id, {}).items():
                bp[nkey] = value
            for child in inv_var_inputs.get(inv_id, ()):
                stack.append((child, depth + 1))
        out[seed] = bp
    return out


def run_options_label(
    distribute, as_table, for_columns=None, across_variants=None
) -> str:
    """One string for an invocation's identity-bearing run options.

    ``distribute`` and ``as_table`` are the two ``for_each`` flags folded into
    ``invocation_id`` (``provenance.compute_invocation_id``): flipping either
    names a *different* run, and a re-run under different options writes a
    second record to the same schema location. This is the label those records
    are told apart by — in ``is_latest``, in the ``Run:<fn>`` plot axis, and in
    ``Variant(..., run_options=...)`` — so it is spelled exactly once, here.

    ``distribute=false`` / ``distribute=true``, with ``, as_table=[a, b]``
    appended only when something is aggregated — the common no-options case
    reads as ``distribute=false`` rather than as an empty string, because it is
    a level a user will see in a dropdown next to its alternative.

    ``for_columns=[param]`` joins them (2026-09-19). It is an execution MODE
    like its two siblings — "run the function once per column and reassemble"
    — and it was the only one not reported anywhere, which is how it stayed
    invisible while being silently dropped. Unlike them it is NOT a separate
    identity term: the per-param selector already carries ``iterate`` and
    ``compute_invocation_id`` folds that in, so adding it again would count
    one fact twice. It IS in this label, because two records at one location
    — one per-column, one whole-table — are different runs a reader has to be
    able to tell apart.

    ``across_variants=[param]`` (2026-09-20): the input pooled every variant
    group into the one call (``AcrossVariants``) instead of one call per
    group. Stored on ``_invocation.across_variants`` and folded into
    ``invocation_id`` like its siblings — it cannot be derived from the
    edges, since a pooled call and a one-group split call write the same
    ones.
    """
    names = sorted(str(x) for x in (as_table or ()))
    per_col = sorted(str(x) for x in (for_columns or ()))
    pooled = sorted(str(x) for x in (across_variants or ()))
    label = f"distribute={'true' if distribute else 'false'}"
    if names:
        label += f", as_table=[{', '.join(names)}]"
    if per_col:
        label += f", for_columns=[{', '.join(per_col)}]"
    if pooled:
        label += f", across_variants=[{', '.join(pooled)}]"
    return label


def invocation_run_options_batch(duck, invocation_ids) -> dict:
    """``{invocation_id: run_options_label}`` for the given invocations."""
    ids = list(dict.fromkeys(invocation_ids))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT invocation_id, distribute, as_table, for_columns, across_variants "
        "FROM _invocation WHERE invocation_id IN ({ph})",
        ids,
    )
    return {
        inv_id: run_options_label(dist, at, fc, av) for inv_id, dist, at, fc, av in rows
    }


def run_option_axes(duck, fn_names) -> dict:
    """``{fn_name: [label, ...]}`` for functions that have been invoked under
    **more than one** run-option set — the run-options counterpart of
    :func:`code_version_ordinals`, with the same omission rule: a function
    that only ever ran one way is not an axis and is absent, so a caller can
    treat presence as "this is a real choice".

    Labels are sorted for stable presentation; there is no ordinal here because
    run options have no version order — ``distribute=true`` is not "newer"
    than ``distribute=false``, it is different. Which one is *current* at a
    location is answered by ``is_latest``, not by the label.
    """
    names = sorted({n for n in fn_names if n and n != SAVE_FUNCTION_NAME})
    if not names:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT DISTINCT function_name, distribute, as_table, for_columns, "
        "across_variants FROM _invocation WHERE function_name IN ({ph})",
        names,
    )
    labels: dict = {}
    for fn_name, dist, at, fc, av in rows:
        labels.setdefault(fn_name, set()).add(run_options_label(dist, at, fc, av))
    out = {fn: sorted(levels) for fn, levels in labels.items() if len(levels) > 1}
    if out:
        logger.info(
            "run_option_axes: %d function(s) ran under >1 run-option set — "
            "records differing only by distribute/as_table are distinguishable "
            "through them: %s",
            len(out),
            "; ".join(f"{fn} {levels}" for fn, levels in sorted(out.items())),
        )
    return out


def current_run_options(duck, fn_names) -> dict:
    """``{fn_name: label}`` — the run-option set each function was **most
    recently run under**, judged by the newest ``_record_save`` among the
    outputs of its invocations.

    This is what "current" means for run options, and it is deliberately
    **per function, not per schema location** — the opposite scope from code
    versions. A code edit is re-run incrementally, subject by subject, and a
    subject never re-run under the newest body should keep contributing its
    own newest record. A run-option flip is different in kind: it changes what
    a record MEANS (a ``distribute=true`` slice vs. the whole file dumped at
    every trial), so an older option set is stale everywhere — including at
    locations the newer run never produced. Observed 2026-09-14: a loader run
    non-distributed over trials 1–4 (discovery said four), then distributed
    (the file held three); per-location "latest" kept the whole-file record
    at every trial 4 as "current" and the figure mixed the two.

    Only functions in ``fn_names`` that have any saved output appear.
    """
    names = sorted({n for n in fn_names if n and n != SAVE_FUNCTION_NAME})
    if not names:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT inv.function_name, inv.distribute, inv.as_table, inv.for_columns, "
        "inv.across_variants, MAX(rs.timestamp) "
        "FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record_save rs ON rs.record_id = io.output_record_id "
        "WHERE inv.function_name IN ({ph}) "
        "GROUP BY inv.function_name, inv.distribute, inv.as_table, inv.for_columns, "
        "inv.across_variants",
        names,
    )
    newest: dict = {}  # fn -> (timestamp, label)
    for fn_name, dist, at, fc, av, ts in rows:
        label = run_options_label(dist, at, fc, av)
        # Label breaks a timestamp tie deterministically, as `_order_versions`
        # does with the hash.
        candidate = (ts or "", label)
        if fn_name not in newest or candidate > newest[fn_name]:
            newest[fn_name] = candidate
    return {fn: label for fn, (_ts, label) in newest.items()}


def chain_batch(duck, record_ids, max_depth: int = 20) -> dict:
    """``{record_id: {"code": {fn_name: fn_hash}, "run": {fn_name: label}}}`` —
    every function in a record's upstream chain, including the one that
    produced it directly, with the code it ran and the run options it ran under.

    The chain counterpart to :func:`branch_params_batch`, and deliberately the
    same shape of walk over the same closure. Constants have accumulated
    upstream since the beginning; code versions did not, and that asymmetry is a
    correctness bug rather than a gap in polish: two records that differ *only*
    by the version of some upstream function arrive at the display layer
    indistinguishable and overplot as replicates. Run options had the same gap
    one axis over (2026-09-14: a ``distribute=false`` run and a
    ``distribute=true`` re-run of one loader, same code, same constants, two
    records per trial, drawn on top of each other) — hence both are collected
    from one walk rather than two.

    :func:`producing_function_versions_batch` answers the one-hop question
    ("which code made this record?") and is still the right read for labelling a
    single production step. This answers the transitive one ("which code is this
    record made *of*?"), which is what a figure spanning several pipeline layers
    needs. See ``docs/claude/variant-selection.md`` §2.

    Excluded, matching every other function-enumerating query here: the synthetic
    ``__save__`` anchor, and invocations with an empty ``function_hash``.

    A function appearing at two depths of one chain contributes a single entry.
    That is not a collision — the same function at the same version has the same
    hash, so there is nothing to disambiguate. Should a function genuinely run at
    two *different* versions within one chain, the shallower (later-applied) one
    wins, matching ``branch_params``' last-write-wins on a key collision.
    """
    seeds = list(dict.fromkeys(record_ids))
    if not seeds:
        return {}
    rec_to_inv, _consts, inv_var_inputs, inv_fn_hash, inv_run = (
        _build_upstream_closure(duck, seeds, max_depth)
    )
    out: dict = {}
    for seed in seeds:
        code: dict = {}
        run: dict = {}
        visited: set = set()
        # Depth-ordered so a shallower occurrence of a function overwrites a
        # deeper one rather than the reverse (BFS, unlike branch_params' DFS —
        # which is free to use a stack because its keys are already namespaced
        # per invocation and cannot collide across depths).
        frontier = [seed]
        depth = 0
        while frontier and depth <= max_depth:
            next_frontier: list = []
            for cur in frontier:
                if cur in visited:
                    continue
                visited.add(cur)
                inv = rec_to_inv.get(cur)
                if inv is None:
                    continue
                inv_id, fn_name = inv
                fn_hash = inv_fn_hash.get(inv_id)
                if fn_name != SAVE_FUNCTION_NAME and fn_hash:
                    code.setdefault(fn_name, fn_hash)
                    run.setdefault(fn_name, inv_run.get(inv_id))
                next_frontier.extend(inv_var_inputs.get(inv_id, ()))
            frontier = next_frontier
            depth += 1
        out[seed] = {"code": code, "run": run}
    return out


def code_versions_batch(duck, record_ids, max_depth: int = 20) -> dict:
    """``{record_id: {fn_name: fn_hash}}`` — the code half of :func:`chain_batch`.

    Kept as the name every existing caller uses; see :func:`chain_batch` for the
    walk and its rules.
    """
    chains = chain_batch(duck, record_ids, max_depth)
    return {rid: chain["code"] for rid, chain in chains.items()}


def run_options_batch(duck, record_ids, max_depth: int = 20) -> dict:
    """``{record_id: {fn_name: label}}`` — the run-options half of
    :func:`chain_batch`: for every upstream function, the
    :func:`run_options_label` it ran under on the way to this record."""
    chains = chain_batch(duck, record_ids, max_depth)
    return {rid: chain["run"] for rid, chain in chains.items()}


def function_source(duck, function_hash: str) -> dict:
    """The code behind a stored ``function_hash`` →
    ``{"entry": name_or_None, "units": {unit_name: source_text}}``.

    Empty ``units`` means **not captured**, never "no code": source capture
    started partway through this project's life and MATLAB does not supply it at
    all yet, so old records legitimately have none. Callers must distinguish the
    two — offering to re-run a version whose source was never stored is the one
    thing this API must not enable.

    ``units`` is the whole closure the hash covers, not just the entry point:
    the Python hash is recursive over user-defined callees, so the helpers are
    part of what it identifies. ``entry`` names the function the hash is filed
    under, which is the one a reader wants shown first.
    """
    if not function_hash:
        return {"entry": None, "units": {}}
    rows = duck._fetchall(
        "SELECT unit_name, unit_source, is_entry FROM _function_source "
        "WHERE function_hash = ? ORDER BY unit_name",
        [function_hash],
    )
    units = {name: source for name, source, _is_entry in rows}
    entry = next((name for name, _s, is_entry in rows if is_entry), None)
    return {"entry": entry, "units": units}


def function_versions(duck, fn_names) -> dict:
    """``{fn_name: [{"version": "v1", "function_hash": …, "first_saved": …}]}`` —
    every recorded version of each named function, oldest first.

    The unfiltered counterpart to :func:`code_version_ordinals`, which drops
    single-version functions because *as a plot axis* they are not interesting.
    A version **picker** needs the opposite: a function that has run once still
    offers a choice ("latest", or that one version by name), and a dropdown that
    silently omitted it would present the user with an empty list for the most
    common case in any project.

    Ordinals are identical to ``code_version_ordinals``' — the same ordering, in
    fact the same computation, since that function is now expressed in terms of
    this one. Two implementations of "which version is v2" that could disagree
    would be a labelling bug that only appears once somebody edits a function
    twice, which is exactly when it is hardest to notice.

    ``first_saved`` is the earliest save timestamp of any record the version
    produced — what a picker shows beside the ordinal so "v1" has a date on it.
    """
    names = [n for n in dict.fromkeys(fn_names) if n and n != SAVE_FUNCTION_NAME]
    if not names:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT inv.function_name, inv.function_hash, MIN(rs.timestamp) "
        "FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record_save rs ON rs.record_id = io.output_record_id "
        "WHERE inv.function_name IN ({ph}) AND inv.function_hash <> '' "
        "GROUP BY inv.function_name, inv.function_hash",
        names,
    )
    by_name: dict = {}
    for fn_name, fn_hash, first_saved in rows:
        by_name.setdefault(fn_name, []).append((first_saved or "", fn_hash))

    out: dict = {}
    for fn_name, versions in by_name.items():
        # (timestamp, hash) sort: hash breaks ties deterministically when two
        # versions share a timestamp, so ordinals never shuffle between reads.
        out[fn_name] = [
            {
                "version": f"v{ordinal}",
                "function_hash": fn_hash,
                "first_saved": first_saved or None,
            }
            for ordinal, (first_saved, fn_hash) in enumerate(
                sorted(versions), start=1
            )
        ]
    logger.debug(
        "function_versions: %d function(s) resolved: %s",
        len(out),
        {name: len(v) for name, v in sorted(out.items())},
    )
    return out


def code_version_ordinals(duck, fn_names) -> dict:
    """``{fn_name: {fn_hash: "vN"}}`` for functions holding **more than one**
    version, numbered by each version's earliest save.

    **Scoped per function, deliberately** — unlike ``fn_version`` in
    :func:`variant_identity_batch`, which is numbered per *variable type*. Once
    versions accumulate along a chain, one column exists per upstream function
    and its levels must mean the same thing wherever that function appears; a
    per-type ordinal would let one hash be ``v1`` in one variable's column and
    ``v2`` in another's, which is the same incoherence the per-type scope was
    originally chosen to avoid one level down.

    Functions with a single version are **omitted entirely**, so a caller can
    treat presence in this map as "this function is a real axis". That keeps the
    ordinary single-version project free of columns whose only level is ``v1``.

    Ordering is by earliest save, so a new version appends ``v3`` rather than
    renumbering ``v1``/``v2`` under the user — the same stability guarantee
    :func:`_order_versions` makes. ``fn_hash`` breaks ties deterministically
    when two versions share a timestamp.
    """
    out: dict = {}
    for fn_name, versions in function_versions(duck, fn_names).items():
        if len(versions) < 2:
            continue  # single-version functions are not an axis
        out[fn_name] = {v["function_hash"]: v["version"] for v in versions}

    if out:
        logger.info(
            "code_version_ordinals: %d function(s) hold >1 version — records "
            "differing only by upstream code are distinguishable through them: %s",
            len(out),
            "; ".join(
                f"{name} [{len(versions)}]" for name, versions in sorted(out.items())
            ),
        )
    return out


def saved_at_batch(duck, record_ids) -> dict:
    """``{record_id: latest_save_timestamp}`` from ``_record_save``.

    ``_record_save`` is the only per-save recency source: ``_record`` is written
    ``ON CONFLICT DO NOTHING``, so its ``created_at`` freezes at first save. A
    record can have several save events; the newest is its recency. Records with
    no save row are absent from the map.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT record_id, MAX(timestamp) FROM _record_save "
        "WHERE record_id IN ({ph}) GROUP BY record_id",
        ids,
    )
    return {rid: ts for rid, ts in rows}


def producing_function_versions_batch(duck, record_ids) -> dict:
    """``{record_id: {"fn_name", "fn_hash", "saved_at"}}`` for records produced by
    a **real** function call.

    Absent from the map (i.e. "has no function version") are:

    - raw/manual records — no producing invocation at all;
    - records whose producing invocation is the synthetic ``__save__`` anchor for
      direct-save kwargs (``function_hash`` is ``""`` there — see
      ``provenance.SAVE_FUNCTION_NAME``, which every function-enumerating query
      is required to exclude).

    Both read as "(raw)" downstream, which is what they are.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    inv_map = producing_invocation_batch(duck, ids)
    saved = saved_at_batch(duck, ids)
    out: dict = {}
    for rid in ids:
        inv = inv_map.get(rid)
        if inv is None:
            continue
        _inv_id, fn_name, fn_hash = inv
        if fn_name == SAVE_FUNCTION_NAME or not fn_hash:
            continue
        out[rid] = {
            "fn_name": fn_name,
            "fn_hash": fn_hash,
            "saved_at": saved.get(rid),
        }
    return out


def _chain_signature(chain: dict, run_chain: dict | None = None) -> str:
    """A deterministic scalar standing for a whole ``{fn_name: fn_hash}`` chain,
    optionally with each hop's run-options label folded in.

    Lets :func:`_order_versions` treat "this record's entire upstream code
    story" exactly as it already treats a single producing hash — grouping and
    recency logic stay in one place instead of gaining a chain-shaped twin.
    Never stored or shown; two chains are equal iff their signatures are.

    With ``run_chain`` the same hash under ``distribute=true`` and under
    ``distribute=false`` are two signatures — the whole reason the run options
    are in here (see ``is_latest`` in :func:`variant_identity_batch`).
    """
    run_chain = run_chain or {}
    return "|".join(
        f"{name}={chain[name]}"
        + (f"@{run_chain[name]}" if run_chain.get(name) is not None else "")
        for name in sorted(chain)
    )


def _order_versions(records, versions) -> tuple[dict, list, str | None]:
    """Group ``records`` by producing ``function_hash`` and order the versions →
    ``(by_hash, ordered_hashes, latest_hash)``.

    Called at two different scopes on purpose: over a whole variable type (to
    number the ordinals coherently) and over one schema location (to resolve
    which version is latest *there*). See :func:`variant_identity_batch`.

    Ordinals order by each version's **earliest** save so they stay stable as new
    versions appear (a third version appends ``v3`` rather than renumbering the
    first two under the user). The **latest** version is instead the one holding
    the most recently saved record — normally the same as last-by-ordinal, but
    deliberately not when an older version is re-run on purpose: whatever was
    written most recently is what "latest" should mean.

    ``fn_hash`` breaks ties deterministically when two saves share a timestamp
    (``_record_save.timestamp`` is a string with finite resolution, and a batch
    save writes many rows within it).
    """
    by_hash: dict = {}
    for rid in records:
        info = versions.get(rid)
        if info is not None:
            by_hash.setdefault(info["fn_hash"], []).append(rid)
    if not by_hash:
        return {}, [], None

    def _saved_at(rid: str) -> str:
        return (versions.get(rid) or {}).get("saved_at") or ""

    latest_hash = max(
        by_hash, key=lambda h: (max(_saved_at(r) for r in by_hash[h]), h)
    )
    ordered = sorted(by_hash, key=lambda h: (min(_saved_at(r) for r in by_hash[h]), h))
    return by_hash, ordered, latest_hash


def variant_identity_batch(duck, record_ids, max_depth: int = 20) -> dict:
    """What distinguishes each record from the others **at its own location** —
    ``{record_id: {branch_params, fn_name, fn_hash, fn_version, is_latest,
    saved_at}}``.

    This is the *display* discriminator, and it is deliberately NOT the same
    thing as the supersession keys used by ``load`` and node state
    (``_producing_variant_key``, ``database._find_record``'s latest-collapse).
    Those answer "does this new record replace that old one?" — and a re-run
    after a function-body edit correctly answers *yes*, which is why downstream
    consumers see only the newest. This answers the different question the UI
    has to ask: "these records coexist on screen — what do I call each one?"
    Two records that the supersession key calls identical still need distinct
    names, or the UI shows the same label twice and a plot overplots two
    function versions as if they were replicates. See
    ``docs/claude/function-version-variants.md``.

    Fields:

    ``branch_params``
        Exactly :func:`branch_params_batch` — upstream constants. Unchanged.
    ``fn_name`` / ``fn_hash``
        The producing function and the hash of the source that produced it, or
        ``None`` for raw records (see
        :func:`producing_function_versions_batch`).
    ``fn_version``
        A stable ordinal — ``"v1"``, ``"v2"``, … — over the distinct producing
        ``function_hash`` values of this record's **variable type**. **``None``
        unless the type actually holds more than one version**, so the ordinary
        single-version case keeps today's clean labels and nothing existing
        changes appearance. Ordered by each version's *earliest* save, so a new
        version appends ``v3`` rather than renumbering ``v1``/``v2`` under the
        user.
    ``is_latest``
        Whether this record's **whole upstream chain** — the code of every
        function in it AND the run options each ran under — is the most
        recently written one **at its own schema location**. A property of the
        chain, not the record: two records of the same chain (a plain re-save)
        are both latest, which is what pinning wants — pin a version, keep all
        of its rows. ``None`` for raw records.

        Chain-wide rather than one-hop since 2026-09-08. A record whose own
        producing function never changed is still stale when something upstream
        of it did; the one-hop test called both such records current and let
        them overplot (``docs/claude/variant-selection.md`` §2).

        Run-option-aware since 2026-09-14. ``distribute``/``as_table`` are
        folded into ``invocation_id``, so a re-run under different options is a
        different invocation writing a SECOND record to the same location — same
        code, same constants. Without the options in the signature both records
        were "latest" and a ``distribute=false`` loader run overplotted its
        ``distribute=true`` re-run trial for trial.

        Run options are judged **per function, globally**, on top of the
        per-location chain rule: a record whose chain used an option set that
        is not the one its function was most recently run under is NOT latest,
        even where it is the only record (:func:`current_run_options` says
        why the scope differs from code's).
    ``code_chain``
        ``{fn_name: "vN"}`` over the record's upstream functions, restricted to
        those holding more than one version (:func:`code_version_ordinals`).
        Empty for a project where nothing was ever edited, so this costs the
        ordinary case nothing. Ordinals here are scoped **per function**, unlike
        ``fn_version`` above — see that function's docstring for why the two
        scopes differ.
    ``run_chain``
        ``{fn_name: label}`` over the record's upstream functions, restricted to
        those that have been invoked under more than one run-option set
        (:func:`run_option_axes`) — the same presence-means-axis rule as
        ``code_chain``, so the ordinary project sees an empty dict. Labels are
        :func:`run_options_label` strings (``distribute=true``).
    ``saved_at``
        The record's newest ``_record_save`` timestamp.

    **The two scopes differ on purpose.** Ordinals are numbered per *type*
    because Plot Studio turns them into one factor column spanning every
    location at once, and per-location numbering would let a single hash be
    ``v1`` at one subject and ``v2`` at another — an incoherent column and a
    wrong figure. ``is_latest`` is resolved per *location* because pinning "the
    latest" must keep each location's own newest version, or a subject never
    re-run under the newest code silently disappears from the figure.

    Both are computed over **every** non-excluded record of the relevant type,
    not just over ``record_ids``, so a caller asking about a subset gets the same
    labels as one asking about everything.
    """
    seeds = list(dict.fromkeys(record_ids))
    if not seeds:
        return {}

    # --- locations of the requested records ---
    loc_rows = _chunked_in(
        duck,
        "SELECT record_id, type, schema_id FROM _record WHERE record_id IN ({ph})",
        seeds,
    )
    loc_of: dict = {rid: (vtype, sid) for rid, vtype, sid in loc_rows}
    locations = set(loc_of.values())

    # --- every non-excluded record sharing those locations ---
    # Queried by type (few) then filtered to the requested locations in Python;
    # a pairwise (type, schema_id) IN-list is awkward to chunk and buys nothing,
    # since the display callers already load a whole type's records anyway.
    types = sorted({vtype for vtype, _sid in locations})
    peer_rows = _chunked_in(
        duck,
        "SELECT record_id, type, schema_id FROM _record "
        "WHERE type IN ({ph}) AND COALESCE(excluded, FALSE) = FALSE",
        types,
    )
    peers: dict = {}  # requested location -> [record_id, ...]
    by_type: dict = {}  # type -> [record_id, ...] (every location of that type)
    for rid, vtype, sid in peer_rows:
        by_type.setdefault(vtype, []).append(rid)
        loc = (vtype, sid)
        if loc in locations:
            peers.setdefault(loc, []).append(rid)
    # A requested record that is itself excluded won't come back above; keep it
    # so the caller always gets an entry for what it asked about.
    for rid, loc in loc_of.items():
        bucket = peers.setdefault(loc, [])
        if rid not in bucket:
            bucket.append(rid)

    all_rids = list(
        dict.fromkeys(
            [rid for bucket in by_type.values() for rid in bucket]
            + [rid for bucket in peers.values() for rid in bucket]
        )
    )
    versions = producing_function_versions_batch(duck, all_rids)

    # --- version ordinals: numbered once per TYPE ---
    # Deliberately type-wide rather than per schema location. The ordinal is a
    # LABEL, and Plot Studio turns it into a factor column spanning every
    # location in one frame; numbering per location would let one hash be "v1"
    # at subject=1 and "v2" at subject=2, making that column incoherent and the
    # resulting figure wrong. Computed over every non-excluded record of the
    # type — not just the requested ones — so a subset request labels the same
    # way a full one does.
    fn_version_of: dict = {}  # record_id -> "vN"
    ambiguous: list = []  # types holding more than one function version
    for vtype, bucket in by_type.items():
        by_hash, ordered, _latest = _order_versions(bucket, versions)
        if len(ordered) < 2:
            continue
        ambiguous.append((vtype, ordered))
        for ordinal, fn_hash in enumerate(ordered, start=1):
            for rid in by_hash[fn_hash]:
                fn_version_of[rid] = f"v{ordinal}"

    # --- code chains: the whole upstream version story, not just one hop ---
    # Scoped to the requested LOCATIONS, not to `all_rids`. The type-wide set
    # exists to number `fn_version` coherently, which needs only the cheap
    # one-hop `versions` map; a chain walk over every record of every type would
    # multiply the cost of opening a panel for no gain, since `code_chain` is
    # reported for seeds and `is_latest` compares within a location.
    chain_rids = list(
        dict.fromkeys([rid for bucket in peers.values() for rid in bucket])
    )
    full_chains = chain_batch(duck, chain_rids, max_depth)
    chains = {rid: c["code"] for rid, c in full_chains.items()}
    run_chains = {rid: c["run"] for rid, c in full_chains.items()}
    chain_fn_names = {name for chain in chains.values() for name in chain}
    ordinals = code_version_ordinals(duck, chain_fn_names)
    run_axes = run_option_axes(duck, chain_fn_names)
    saved = saved_at_batch(duck, all_rids)

    # --- is_latest: resolved per LOCATION, over the WHOLE chain ---
    # Deliberately NOT type-wide, unlike the ordinals above. Pinning "the
    # latest" has to keep each location's own newest version, or a subject that
    # was never re-run under the newest code silently vanishes from the figure.
    #
    # Keyed on the whole chain rather than the producing hash alone: a record
    # whose own function never changed is still stale if something upstream of
    # it did, and the one-hop version of this check called both records current
    # and let them overplot. Reuses `_order_versions` by handing it a synthetic
    # per-record "hash" that stands for the entire chain, so latest-ness keeps
    # meaning "newest thing here" and nothing about the ordering rules changes.
    #
    # Each hop's run options ride along in the signature (`_chain_signature`),
    # so a re-run of unchanged code under different distribute/as_table is a
    # different chain here and the older one stops being "latest" — which is
    # what makes the plot default drop it and `code_version="latest"` loads skip
    # it. Two records of identical code AND options remain both-latest.
    chain_versions = {
        rid: {
            "fn_hash": _chain_signature(chain, run_chains.get(rid)),
            "saved_at": saved.get(rid),
        }
        for rid, chain in chains.items()
        if chain
    }
    is_latest_of: dict = {}  # record_id -> bool
    for bucket in peers.values():
        by_hash, _ordered, latest_hash = _order_versions(bucket, chain_versions)
        for chain_sig, rids in by_hash.items():
            for rid in rids:
                is_latest_of[rid] = chain_sig == latest_hash

    # --- run options: current per FUNCTION, over the whole database ---
    # The per-location rule above cannot see a location the newer run never
    # produced (a trial that only ever existed under the old option set), and
    # there the stale record is the newest thing present. So a second, global
    # test: any hop that ran under an option set other than the one its
    # function was most recently run under makes the record not-latest.
    current_run = current_run_options(duck, run_axes) if run_axes else {}
    stale_by_run = 0
    if current_run:
        for rid, is_latest in list(is_latest_of.items()):
            if not is_latest:
                continue
            hops = run_chains.get(rid, {})
            if any(
                hops.get(fn) is not None and hops.get(fn) != label
                for fn, label in current_run.items()
            ):
                is_latest_of[rid] = False
                stale_by_run += 1
        if stale_by_run:
            logger.info(
                "variant_identity: %d record(s) are the newest at their location "
                "but were built under a superseded run-option set (current: %s) "
                "— marked not-latest",
                stale_by_run,
                current_run,
            )

    # One summary line per call, not one per type: this runs on every
    # variable-panel open. The detail that matters is which types became
    # ambiguous and what produced them, so a sample is spelled out.
    if ambiguous:
        sample = "; ".join(
            "{} [{}]".format(
                vtype,
                " ".join(f"v{i}={h[:12]}" for i, h in enumerate(ordered, start=1)),
            )
            for vtype, ordered in ambiguous[:3]
        )
        logger.info(
            "variant_identity: %d type(s) hold >1 function version — their "
            "records are distinguishable only by the code that produced them: "
            "%s%s",
            len(ambiguous),
            sample,
            f" (+{len(ambiguous) - 3} more)" if len(ambiguous) > 3 else "",
        )

    # --- assemble, for the requested records only ---
    # saved_at comes from its own lookup, not from `versions`: a raw record has
    # no function version but still has a save time, and the UI sorts on it.
    # `all_rids` is a superset of `seeds`, so one lookup serves both.
    bp_map = branch_params_batch(duck, seeds, max_depth)
    out: dict = {}
    for rid in seeds:
        info = versions.get(rid) or {}
        chain = chains.get(rid, {})
        out[rid] = {
            "branch_params": bp_map.get(rid, {}),
            "fn_name": info.get("fn_name"),
            "fn_hash": info.get("fn_hash"),
            "fn_version": fn_version_of.get(rid),
            "is_latest": is_latest_of.get(rid),
            "saved_at": saved.get(rid),
            # Only multi-version functions appear — `code_version_ordinals`
            # omits the rest — so this is empty for the ordinary project and
            # a caller can treat a key's presence as "this is a real axis".
            "code_chain": {
                fn_name: ordinals[fn_name][fn_hash]
                for fn_name, fn_hash in chain.items()
                if fn_name in ordinals and fn_hash in ordinals[fn_name]
            },
            # Same presence rule as code_chain: only functions that actually
            # ran more than one way appear, so a key here IS a real axis.
            "run_chain": {
                fn_name: label
                for fn_name, label in run_chains.get(rid, {}).items()
                if fn_name in run_axes and label is not None
            },
        }
    return out


def glue_source(duck, virtual_record_id: str) -> dict | None:
    """Resolve a virtual glue record to what it reshapes.

    Returns ``{record_id, variable_type, chain_names, chain_hash}`` — the real
    upstream record, its type, the glue node names in application order and the
    chain's content hash — or ``None`` if ``virtual_record_id`` is not a glue
    record (or its producing invocation is missing).

    This is the one place the extra hop is *undone*. Read paths that care about
    which variable *type* feeds a parameter (config variants, pipeline
    structure, expected-invocation prediction) go through here, so glue never
    leaks into them as a pseudo variable type named ``__glue__``.
    """
    rows = duck._fetchall(
        "SELECT ii.input_record_id, inv.function_name, inv.function_hash, r.type "
        "FROM _invocation_output io "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "JOIN _invocation_input ii ON ii.invocation_id = io.invocation_id "
        "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE io.output_record_id = ? "
        "ORDER BY ii.input_record_id LIMIT 1",
        [virtual_record_id],
    )
    if not rows:
        return None
    src_rid, fn_name, fn_hash, src_type = rows[0]
    return {
        "record_id": src_rid,
        "variable_type": src_type,
        "chain_names": (fn_name or "").split(GLUE_NAME_SEPARATOR),
        "chain_hash": fn_hash or "",
    }


def glue_invocation_ids(duck) -> set:
    """Every invocation that produced a virtual glue record.

    A glue hop is **not a pipeline step** (D5): it has no run button, no node
    state and no place in the compiled pipeline. Queries that enumerate
    pipeline steps or function variants must skip these, the same way they
    already skip synthetic ``__save__`` invocations. Identified by their output
    record's type rather than by ``function_name``, which is the user's glue
    name and could collide with a real function.
    """
    return {
        row[0]
        for row in duck._fetchall(
            "SELECT DISTINCT io.invocation_id FROM _invocation_output io "
            "JOIN _record r ON r.record_id = io.output_record_id "
            "WHERE r.type = ?",
            [GLUE_TYPE],
        )
    }


def invocation_inputs(duck, invocation_id: str):
    """Split an invocation's input edges into variable inputs and constants.

    Returns ``(var_inputs, constants)`` where ``var_inputs`` is a list of
    ``{record_id, param_name, variable_type}`` (sorted by param) and
    ``constants`` is ``{param_name: typed_value}``.

    A param fed through a glue chain has a **virtual glue record** on its edge
    (see ``docs/claude/free-code-glue-nodes.md`` §2). ``record_id`` stays the
    virtual rid — that is the real edge, and the one traversals must follow —
    but ``variable_type`` reports the *source* variable's type, plus
    ``glue_chain`` / ``glue_source_record_id``. Callers that ask "which type
    feeds this param?" therefore keep working unchanged, and callers that care
    about the glue can see it.
    """
    rows = duck._fetchall(
        "SELECT ii.param_name, ii.input_record_id, r.type, c.value_repr "
        "FROM _invocation_input ii "
        "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "LEFT JOIN _constant c ON c.record_id = ii.input_record_id "
        "WHERE ii.invocation_id = ?",
        [invocation_id],
    )
    var_inputs = []
    constants = {}
    for param_name, in_rid, rtype, value_repr in rows:
        if rtype == PATHINPUT_TYPE:
            continue  # PathInput spec — not a variable nor a sweep constant
        if rtype == CONSTANT_TYPE:
            constants[param_name] = _safe_literal(value_repr)
        elif rtype == GLUE_TYPE:
            src = glue_source(duck, in_rid)
            var_inputs.append(
                {
                    "record_id": in_rid,
                    "param_name": param_name,
                    "variable_type": (src or {}).get("variable_type") or GLUE_TYPE,
                    "glue_chain": (src or {}).get("chain_names") or [],
                    "glue_chain_hash": (src or {}).get("chain_hash") or "",
                    "glue_source_record_id": (src or {}).get("record_id"),
                }
            )
        else:
            var_inputs.append(
                {
                    "record_id": in_rid,
                    "param_name": param_name,
                    "variable_type": rtype,
                }
            )
    var_inputs.sort(key=lambda d: (d["param_name"], d["record_id"]))
    return var_inputs, constants


def invocation_path_inputs(duck, invocation_id: str) -> dict[str, str]:
    """``{param_name: spec_json_str}`` for an invocation's PathInput inputs.

    The spec string is ``PathInput.to_key()`` (JSON: template, root_folder, …),
    stored as a distinctly-typed (:data:`PATHINPUT_TYPE`) input record.
    """
    rows = duck._fetchall(
        "SELECT ii.param_name, c.value_repr "
        "FROM _invocation_input ii "
        "JOIN _record r ON r.record_id = ii.input_record_id "
        "JOIN _constant c ON c.record_id = ii.input_record_id "
        "WHERE ii.invocation_id = ? AND r.type = ?",
        [invocation_id, PATHINPUT_TYPE],
    )
    return dict(rows)


def stored_invocation_signature(duck, record_id: str):
    """Signature of the invocation that produced ``record_id``, for skip_computed.

    Returns ``None`` if the record has no producing invocation (raw/manual), else
    ``{"function_hash", "var_inputs", "const_hashes", "run_options"}`` —
    ``run_options`` being :func:`run_options_label` of the invocation, so the
    gate can refuse a record produced under other options (pooled vs split,
    distributed vs not) that happens to share the edge set — where ``var_inputs`` maps
    ``param -> [(input_record_id, selector), ...]`` — a LIST, because an
    aggregating call consumes several records under one parameter and every
    one of them is an edge (since 2026-09-20 under the real parameter name;
    keeping one edge per param here is what made folding the names break
    skip_computed the first time) — and ``const_hashes`` maps
    ``param -> content_hash``. This is the new-table replacement for the old
    ``_lineage`` reads (function hash + input edges + constant records).
    """
    inv = producing_invocation(duck, record_id)
    if inv is None:
        return None
    inv_id, _fn_name, fn_hash = inv
    opts = duck._fetchone(
        "SELECT distribute, as_table, for_columns, across_variants FROM _invocation "
        "WHERE invocation_id = ?",
        [inv_id],
    )
    rows = duck._fetchall(
        "SELECT ii.param_name, ii.input_record_id, ii.selector, r.type, c.content_hash "
        "FROM _invocation_input ii "
        "LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "LEFT JOIN _constant c ON c.record_id = ii.input_record_id "
        "WHERE ii.invocation_id = ?",
        [inv_id],
    )
    var_inputs: dict[str, list] = {}
    const_hashes: dict[str, str] = {}
    for param, in_rid, selector, rtype, chash in rows:
        if rtype == PATHINPUT_TYPE:
            continue  # PathInput spec — excluded from identity / staleness
        if rtype == CONSTANT_TYPE:
            const_hashes[param] = chash
        else:
            var_inputs.setdefault(param, []).append((in_rid, selector))
    return {
        "function_hash": fn_hash,
        "var_inputs": var_inputs,
        "const_hashes": const_hashes,
        "run_options": run_options_label(*opts) if opts else run_options_label(False, None),
    }


def _fetch_record_node(duck, record_id: str, schema_keys: list[str], restore=None):
    """``{type, schema}`` for a variable record, or ``None`` if absent.

    Constants are not pipeline nodes, so callers filter them out by ``type``.
    ``restore(key, value)`` gives each schema value its key's type
    (``DatabaseManager.restore_schema_value``); callers holding a manager
    pass it, so a trace shows ``cycle=10`` spelled as the record was saved.
    """
    restore = restore or (lambda _key, value: _from_schema_str(value))
    schema_cols = ", ".join(f's."{k}"' for k in schema_keys)
    select_extra = (", " + schema_cols) if schema_keys else ""
    rows = duck._fetchall(
        f"SELECT r.type{select_extra} "
        f"FROM _record r LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE r.record_id = ?",
        [record_id],
    )
    if not rows:
        return None
    row = rows[0]
    schema = {}
    for i, key in enumerate(schema_keys):
        val = row[1 + i]
        if val is not None:
            schema[key] = restore(key, val)
    return {"type": row[0], "schema": schema}


# ---------------------------------------------------------------------------
# Derived branch_params (§6)
# ---------------------------------------------------------------------------
def derived_branch_params(duck, record_id: str, max_depth: int = 20) -> dict:
    """Accumulated constants up the ancestry, namespaced ``function.param``.

    The exact ``{fn.param: value}`` map the old system stored in the
    ``branch_params`` column, now derived from the graph instead of stored.
    """
    bp: dict = {}
    visited: set = set()
    stack = [(record_id, 0)]
    while stack:
        cur, depth = stack.pop()
        if cur in visited or depth > max_depth:
            continue
        visited.add(cur)
        inv = producing_invocation(duck, cur)
        if inv is None:
            continue
        inv_id, fn_name, _ = inv
        var_inputs, constants = invocation_inputs(duck, inv_id)
        for param, value in constants.items():
            bp[f"{fn_name}.{param}"] = value
        for inp in var_inputs:
            stack.append((inp["record_id"], depth + 1))
    return bp


# ---------------------------------------------------------------------------
# Upstream provenance — provably-correct edge walk (replaces the heuristic)
# ---------------------------------------------------------------------------
def upstream_provenance(db, record_id: str, max_depth: int = 20) -> list[dict]:
    """BFS over the bipartite graph from ``record_id`` toward its roots.

    Same node shape as the legacy heuristic implementation::

        {record_id, variable_type, schema, branch_params, function_name,
         constants, depth, inputs:[{record_id, param_name, variable_type}]}

    but every edge is a stored fact (no ``branch_params``-subset guessing).
    Returns ``[]`` for an unknown ``record_id``.
    """
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)

    visited: set = set()
    result: list = []
    queue: list = [(record_id, 0)]

    while queue:
        rid, depth = queue.pop(0)
        if rid in visited or depth > max_depth:
            continue
        visited.add(rid)

        node = _fetch_record_node(duck, rid, schema_keys, restore=db.restore_schema_value)
        if node is None or node["type"] in (CONSTANT_TYPE, PATHINPUT_TYPE):
            continue

        inv = producing_invocation(duck, rid)
        if inv is not None:
            inv_id, fn_name, _fn_hash = inv
            var_inputs, constants = invocation_inputs(duck, inv_id)
        else:
            fn_name, var_inputs, constants = None, [], {}

        result.append(
            {
                "record_id": rid,
                "variable_type": node["type"],
                "schema": node["schema"],
                "branch_params": derived_branch_params(duck, rid, max_depth),
                "function_name": fn_name,
                "constants": constants,
                "depth": depth,
                "inputs": var_inputs,
            }
        )

        for inp in var_inputs:
            queue.append((inp["record_id"], depth + 1))

    return result


# ---------------------------------------------------------------------------
# Variant pin → records
# ---------------------------------------------------------------------------
def records_for_variant(
    db,
    variable,
    selection: dict | None = None,
    *,
    version_id: str | None = None,
    include_excluded: bool = False,
    **schema,
) -> list[str]:
    """The record_ids a **variant pin** names → ``[record_id, ...]``.

    ``selection`` is the vocabulary ``load()`` already honours — the dict a
    :class:`~scidb.variant.Variant` carries::

        {"__code__.grSides": "v2",
         "__run__.loadGaitRiteOneFile": "distribute=true",
         "bandpass.low_hz": 20}

    Display spellings (``Code:grSides``, ``Run:loader``, ``CodeIsLatest``) are
    accepted too and canonicalized by
    :func:`~scidb.variant.normalize_selection`, so a selection held by the Plot
    Studio picker or typed at the CLI resolves to the same records as the
    ``Variant`` a scientist writes in ``for_each``.

    ``**schema`` are schema keys (``subject="S01"``); a non-schema kwarg is
    folded into the pin, exactly as ``BaseVariable.load`` folds it.

    This is the for_each input loader's own resolution and nothing beside it:
    one :meth:`~scidb.database.DatabaseManager._find_record` call with the
    same ``branch_params_filter`` and the same ``version_id`` rule
    (:func:`~scidb.variant.pin_loads_uncollapsed`: a code or run pin loads
    ``"all"``, because the latest-collapse merges code versions and run-option
    sets before any filter runs; anything else loads ``"latest"``). It exists
    so that "trace the variant I pinned" cannot answer about different records
    than feeding that variant to a function would — the rule that a list value
    means membership, that ``"latest"`` resolves per schema location, and that
    a bare name is suffix-matched all keep living in one place. Pass
    ``version_id`` only to override that rule deliberately.
    """
    from .variant import normalize_selection, pin_loads_uncollapsed

    type_name = getattr(variable, "__name__", variable)
    nested = db._split_metadata(schema)
    pins = {
        **normalize_selection(nested.get("version") or {}),
        **normalize_selection(selection or {}),
    }
    if version_id is None:
        version_id = "all" if pin_loads_uncollapsed(pins) else "latest"
    df = db._find_record(
        type_name,
        nested_metadata={"schema": nested.get("schema", {}), "version": {}},
        version_id=version_id,
        branch_params_filter=pins or None,
        include_excluded=include_excluded,
    )
    rids = [] if df.empty else df["record_id"].tolist()
    logger.info(
        "records_for_variant(%s, schema=%s, pin=%s, version_id=%s): %d record(s)",
        type_name,
        nested.get("schema", {}) or "(any)",
        pins or "(none)",
        version_id,
        len(rids),
    )
    return rids


# ---------------------------------------------------------------------------
# Pipeline reconstruction (§8) — nodes + edges DAG for the queried record
# ---------------------------------------------------------------------------
def pipeline(db, record_id: str, max_depth: int = 20) -> dict:
    """Full upstream DAG for ``record_id`` as ``{"nodes": [...], "edges": [...]}``.

    ``nodes`` reuses :func:`upstream_provenance`'s node shape; ``edges`` are
    ``{from_record_id, to_record_id, param_name}`` (from upstream input → the
    record that consumed it). Provably correct — every edge is stored.
    """
    nodes = upstream_provenance(db, record_id, max_depth)
    edges = []
    for node in nodes:
        for inp in node["inputs"]:
            edges.append(
                {
                    "from_record_id": inp["record_id"],
                    "to_record_id": node["record_id"],
                    "param_name": inp["param_name"],
                }
            )
    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# Execution audit (§9b) — who/when/which filter produced a record
# ---------------------------------------------------------------------------
def execution_audit(duck, record_id: str) -> list[dict]:
    """Every run that (re)produced ``record_id``, oldest first.

    Each entry: ``{timestamp, user_id, where_clause, function_name}``. Because
    re-runs append ``_run`` rows, a changed ``where=`` filter shows up as
    distinct audit rows rather than being lost to first-wins.
    """
    rows = duck._fetchall(
        "SELECT run.timestamp, run.user_id, run.where_clause, inv.function_name "
        "FROM _invocation_output io "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "JOIN _run_invocation ri ON ri.invocation_id = io.invocation_id "
        "JOIN _run run ON run.run_id = ri.run_id "
        "WHERE io.output_record_id = ? "
        "ORDER BY run.timestamp",
        [record_id],
    )
    return [
        {"timestamp": ts, "user_id": uid, "where_clause": wc, "function_name": fn}
        for ts, uid, wc, fn in rows
    ]


# ---------------------------------------------------------------------------
# Single-record provenance (§7) — flat dict for a record's producing call
# ---------------------------------------------------------------------------
def provenance(duck, record_id: str) -> dict | None:
    """``{function_name, function_hash, inputs, constants}`` for ``record_id``.

    ``inputs`` is the list of variable input descriptors; ``constants`` the
    ``{param: value}`` map. ``None`` if the record has no producing invocation.
    """
    inv = producing_invocation(duck, record_id)
    if inv is None:
        return None
    inv_id, fn_name, fn_hash = inv
    var_inputs, constants = invocation_inputs(duck, inv_id)
    return {
        "function_name": fn_name,
        "function_hash": fn_hash,
        "inputs": var_inputs,
        "constants": constants,
    }


# ---------------------------------------------------------------------------
# Abstract pipeline structure (§ schema-blind) — unique fn/type wiring
# ---------------------------------------------------------------------------
def pipeline_structure(duck) -> list[dict]:
    """Unique ``(function_name, function_hash, output_type, input_types)`` tuples.

    Describes how variable *types* flow through functions, independent of data
    instances or schema locations.
    """
    # Exclude synthetic save invocations — they anchor direct-save kwargs, not
    # pipeline functions, and must not appear as nodes in the structure.
    inv_rows = duck._fetchall(
        "SELECT invocation_id, function_name, function_hash FROM _invocation "
        "WHERE function_name != ?",
        [SAVE_FUNCTION_NAME],
    )
    seen: set = set()
    results: list = []
    glue_invs = glue_invocation_ids(duck)
    for inv_id, fn_name, fn_hash in inv_rows:
        if inv_id in glue_invs:
            continue  # a glue hop is not a pipeline step (D5)
        out_types = [
            r[0]
            for r in duck._fetchall(
                "SELECT DISTINCT r.type FROM _invocation_output io "
                "JOIN _record r ON r.record_id = io.output_record_id "
                "WHERE io.invocation_id = ?",
                [inv_id],
            )
        ]
        # Via invocation_inputs (not a raw type join) so a param fed through a
        # glue chain reports its SOURCE variable type — the structure describes
        # how variable types flow, and glue is not a variable type.
        _var_inputs, _consts = invocation_inputs(duck, inv_id)
        in_types = tuple(sorted(i["variable_type"] for i in _var_inputs))
        for out_type in out_types:
            key = (fn_name, fn_hash, out_type, in_types)
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "function_name": fn_name,
                    "function_hash": fn_hash,
                    "output_type": out_type,
                    "input_types": list(in_types),
                }
            )
    return results


def has_producing_invocation(duck, record_id: str) -> bool:
    """True if ``record_id`` was produced by a recorded invocation."""
    rows = duck._fetchall(
        "SELECT 1 FROM _invocation_output WHERE output_record_id = ? LIMIT 1",
        [record_id],
    )
    return bool(rows)


def consumed_input_schema_ids(duck, record_ids) -> dict[str, frozenset]:
    """``{record_id: frozenset(schema_id, ...)}`` — the *schema locations* of the
    variable inputs each record's producing invocation consumed.

    This is the semantic identity of a ``where=`` variant (§10, "where= redesign"
    (B)): a ``where=`` filter's only effect on the computation is which input
    records survive it, and the graph already records those as the invocation's
    input edges. Reducing them to their **schema_ids** (stable locations, unlike
    record_ids which change on every input re-save) gives a content-edit-stable key
    that load can match against a freshly-resolved filter — making ``A & B`` and
    ``B & A`` (and other textually-different but equivalent filters) match, which
    the brittle ``where_clause`` string comparison cannot.

    Constants and PathInput specs are excluded (no schema location); NULL schema_ids
    are dropped. Raw records (no producing invocation) get no entry.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    placeholders = ", ".join(["?"] * len(ids))
    rows = duck._fetchall(
        f"SELECT io.output_record_id, r.schema_id "
        f"FROM _invocation_output io "
        f"JOIN _invocation_input ii ON ii.invocation_id = io.invocation_id "
        f"JOIN _record r ON r.record_id = ii.input_record_id "
        f"WHERE io.output_record_id IN ({placeholders}) "
        f"AND r.type NOT IN (?, ?) AND r.schema_id IS NOT NULL",
        ids + [CONSTANT_TYPE, PATHINPUT_TYPE],
    )
    acc: dict[str, set] = {}
    for rid, schema_id in rows:
        acc.setdefault(rid, set()).add(schema_id)
    return {rid: frozenset(s) for rid, s in acc.items()}


# ---------------------------------------------------------------------------
# Node completeness (§9c) — expected vs. present invocation membership
# ---------------------------------------------------------------------------
def present_invocation_schema_pairs(duck, inv_ids) -> set:
    """``{(invocation_id, schema_id)}`` actually produced — i.e. each invocation
    paired with the schema locations where it emitted an output record.

    Granularity is per (invocation, schema_id), not per invocation, because a
    PathInput-only function has no per-combo bindings → all its combos share one
    invocation_id, distinguished only by the output's schema location. For
    variable-input functions each combo already has a distinct invocation_id, so
    this reduces to plain invocation presence.
    """
    ids = list(inv_ids)
    if not ids:
        return set()
    placeholders = ", ".join(["?"] * len(ids))
    rows = duck._fetchall(
        f"SELECT DISTINCT io.invocation_id, r.schema_id "
        f"FROM _invocation_output io "
        f"JOIN _record r ON r.record_id = io.output_record_id "
        f"WHERE io.invocation_id IN ({placeholders})",
        ids,
    )
    return {(r[0], r[1]) for r in rows}


def function_variant_configs(duck, fn_name: str) -> list[dict]:
    """Distinct config "shapes" ``fn_name`` has been invoked with (from the graph).

    Each config: ``{input_types: {param: type}, selectors: {param: selector},
    across_variants: [param, ...],
    constants: {param: value}, path_inputs: {param: to_key-json},
    as_table: [...], distribute: bool, invocation_ids: {inv_id, ...}}``.
    Deduped fn-hash-independently — a config is the call's wiring (which
    input *types*, which constants, which flags), the graph-derived
    equivalent of the old ``list_pipeline_variants`` grouping. Used to
    predict expected invocation_ids for input data that exists now but may
    not have been run yet, and (via :func:`config_call_id` +
    ``invocation_ids``) to scope node-state checks to one call site.
    """
    inv_rows = duck._fetchall(
        "SELECT invocation_id, as_table, distribute, across_variants FROM _invocation "
        "WHERE function_name = ?",
        [fn_name],
    )
    configs: dict = {}
    glue_invs = glue_invocation_ids(duck)
    for inv_id, as_table, distribute, across_variants in inv_rows:
        if inv_id in glue_invs:
            continue  # a glue hop is not a pipeline step (D5)
        var_inputs, constants = invocation_inputs(duck, inv_id)
        pooled = sorted(across_variants or [])
        # selectors per param from the edges
        sel_rows = duck._fetchall(
            "SELECT param_name, selector FROM _invocation_input "
            "WHERE invocation_id = ? AND selector IS NOT NULL",
            [inv_id],
        )
        selectors = dict(sel_rows)
        input_types = {i["param_name"]: i["variable_type"] for i in var_inputs}
        # Glue chains fed into this param, recovered from the virtual record's
        # producing invocation. Part of the config key: two call sites differing
        # only by their glue are different wirings.
        glue_chains = {
            i["param_name"]: (i["glue_chain_hash"], tuple(i["glue_chain"]))
            for i in var_inputs
            if i.get("glue_chain")
        }
        path_inputs = invocation_path_inputs(duck, inv_id)
        at = sorted(as_table) if as_table else []
        key = (
            tuple(sorted(input_types.items())),
            tuple(sorted(selectors.items())),
            constants_identity_key(constants),
            tuple(sorted(path_inputs.items())),
            tuple(sorted(glue_chains.items())),
            tuple(at),
            bool(distribute),
            tuple(pooled),
        )
        if key not in configs:
            configs[key] = {
                "input_types": input_types,
                "selectors": selectors,
                "constants": constants,
                "path_inputs": path_inputs,
                "glue_chains": glue_chains,
                "as_table": at,
                "distribute": bool(distribute),
                # The inputs this call pooled across every variant group
                # (AcrossVariants) — the stored fact the predictor and the
                # backward call id read; nothing on the edges says it.
                "across_variants": pooled,
                "invocation_ids": set(),
            }
        configs[key]["invocation_ids"].add(inv_id)

    # Where each config ITERATED, read off its own outputs — what the
    # predictor needs to bind an aggregated input as one edge set rather than
    # one edge per invocation (see _predict_config_invocations). One batched
    # query per config, never per record.
    schema_keys = list(getattr(duck, "dataset_schema", None) or [])
    for cfg in configs.values():
        cfg["schema_keys"] = schema_keys
        cfg["iterated_keys"] = (
            iterated_keys_for_invocations(duck, cfg["invocation_ids"], schema_keys)
            if schema_keys
            else None
        )
    return list(configs.values())


def check_recorded_selectors(
    duck,
    fn_name: str,
    asked: "dict | None",
    *,
    context: str = "",
) -> list[str]:
    """Read half of the input-binding round trip: what history RECORDED for
    this function versus what this run is about to BIND. Returns the params
    whose recorded selection is being dropped.

    Three outcomes, and only one of them is a problem
    (``docs/claude/input-binding-round-trip.md`` §5):

    * recorded a selection, this run binds none → **WARN**. The selection was
      LOST — a GUI re-run that failed to carry it, a node config that was
      never read — and the function is about to receive the whole variable.
    * both present and different → **INFO**. A user changing their mind, which
      ``_build_skip_hook`` already treats as ordinary ("selector for X
      changed" → recompute).
    * this run binds one and history has none → silent. A new selection.

    Deliberately NOT part of ``_build_skip_hook``, which does the same
    comparison per combo: that hook exists only when ``skip_computed and not
    dry_run and outputs and active_db is not None``, and a lost selection has
    nothing to do with caching. One query at run entry, log-only, never
    changes what runs.
    """
    from .intent import describe_columns, parse_selector, same_columns
    from .log import Log

    asked = asked or {}
    try:
        configs = function_variant_configs(duck, fn_name)
    except Exception as exc:  # pragma: no cover - diagnostics must never fail a run
        Log.debug(f"[selector-check] {fn_name}: no recorded configs ({exc})")
        return []

    # The union across recorded configs: a param that EVER ran with a
    # selection is one whose selection this run can lose. Two configs
    # disagreeing about a param is itself ordinary (the user changed it), so
    # the first recorded selection is enough to ask the question.
    recorded: dict = {}
    for cfg in configs:
        for param, sel in (cfg.get("selectors") or {}).items():
            parsed = parse_selector(sel)
            if parsed:
                recorded.setdefault(param, parsed)

    where = f" {context}" if context else ""
    lost = []
    for param, rec in recorded.items():
        if param not in asked:
            continue  # different wiring; not this call site's business
        current = parse_selector(asked.get(param))
        if current is None:
            lost.append(param)
            Log.warn(
                f"[selector-dropped] {fn_name}{where}: input '{param}' ran "
                f"before with {describe_columns(rec)} ({rec}) and this run "
                f"binds the WHOLE variable. If that is deliberate, nothing is "
                f"wrong; if not, the selection was lost between history and "
                f"this call — see docs/claude/input-binding-round-trip.md §4."
            )
        elif not same_columns(current, rec):
            Log.info(
                f"[selector-changed] {fn_name}{where}: input '{param}' "
                f"{describe_columns(rec)} → {describe_columns(current)}; "
                f"this is a different computation and will recompute."
            )
    return lost


def latest_runs(duck, fn_names) -> dict:
    """``{fn_name: {"run_id", "timestamp", "origin", "selectors"}}`` — each
    function's most recent execution and what it actually bound.

    ``origin`` is which surfaces that run read (``gui`` / ``script`` /
    ``replay``; ``None`` for a row older than the column). ``selectors`` is
    ``{param: selection}`` unioned over the run's invocations, parsed into
    the canonical shape.

    This is the fact a canvas needs to say "stated here, not used by the
    last run": a statement in the intent store beside a latest run whose
    origin does not read the store, or whose recorded selection differs from
    the statement, is intent the user can see and the data does not reflect.
    Batched by function so a canvas render asks once, not once per node.
    """
    from .intent import parse_selector

    names = sorted({n for n in fn_names if n and n != SAVE_FUNCTION_NAME})
    if not names:
        return {}
    rows = _chunked_in(
        duck,
        "SELECT r.function_name, r.run_id, r.timestamp, r.origin "
        "FROM _run r "
        "JOIN (SELECT function_name, MAX(timestamp) AS ts FROM _run "
        "      WHERE function_name IN ({ph}) GROUP BY function_name) latest "
        "  ON latest.function_name = r.function_name AND latest.ts = r.timestamp",
        names,
    )
    out: dict = {}
    for fn_name, run_id, ts, origin in rows:
        # Two runs in one timestamp: keep the greater run_id, deterministically.
        prev = out.get(fn_name)
        if prev is not None and prev["run_id"] >= run_id:
            continue
        out[fn_name] = {
            "run_id": run_id,
            "timestamp": ts,
            "origin": origin,
            "selectors": {},
        }
    if not out:
        return out
    sel_rows = _chunked_in(
        duck,
        "SELECT r.function_name, ii.param_name, ii.selector "
        "FROM _run r "
        "JOIN _run_invocation ri ON ri.run_id = r.run_id "
        "JOIN _invocation_input ii ON ii.invocation_id = ri.invocation_id "
        "WHERE r.run_id IN ({ph}) AND ii.selector IS NOT NULL",
        [v["run_id"] for v in out.values()],
    )
    for fn_name, param, selector in sel_rows:
        parsed = parse_selector(selector)
        if parsed and fn_name in out:
            out[fn_name]["selectors"].setdefault(param, parsed)
    return out


def recorded_schema_keys(duck, fn_name: str, schema_keys) -> list[str] | None:
    """The schema keys *fn_name* ITERATED on its most recent run, in dataset
    order — or ``None`` when it has never run.

    Read off the records that run produced: the schema columns populated on
    their ``_schema`` rows are the keys the run iterated (a ``distribute``
    run saves one level below where it iterated, so its deepest populated
    key is dropped). A run that produced DATASET-level records — no schema
    key at all, one call over everything — answers ``[]``, which is a real
    level and not the same as ``None`` (no history). Note the for_each
    spelling is the other way round: ``schema_keys=None`` iterates nothing
    (one call), ``schema_keys=[]`` iterates every key — the caller that
    turns this answer into a for_each argument maps ``[]`` to ``None``. This is the ``schema_location`` aspect's FACT — history
    as the floor (rule 4 of docs/claude/intent-and-fact.md): a re-run from
    the canvas with no level chosen on the node should run where the
    function ran, not at every key the dataset has. Defaulting to every key
    is how a trial-level step re-run from the GUI fanned out to cycle level
    and wrote ten records per trial (test_dag_runs, 2026-09-19).
    """
    keys = list(schema_keys or [])
    if not keys:
        return None
    last = latest_runs(duck, [fn_name]).get(fn_name)
    if last is None:
        return None
    cols = ", ".join(f's."{k}"' for k in keys)
    rows = duck._fetchall(
        f"SELECT DISTINCT inv.distribute, {cols} "  # noqa: S608 - keys are the dataset's own
        "FROM _run_invocation ri "
        "JOIN _invocation inv ON inv.invocation_id = ri.invocation_id "
        "JOIN _invocation_output io ON io.invocation_id = ri.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "LEFT JOIN _schema s ON s.schema_id = r.schema_id "
        "WHERE ri.run_id = ?",
        [last["run_id"]],
    )
    if not rows:
        return None
    populated: set[str] = set()
    distributed = False
    for distribute, *values in rows:
        distributed = distributed or bool(distribute)
        populated.update(k for k, v in zip(keys, values) if v is not None)
    level = [k for k in keys if k in populated]
    if distributed and level:
        level = level[:-1]
    return level


def variable_schema_keys(duck, type_names, schema_keys) -> dict[str, list[str]]:
    """``{type_name: [schema keys its records populate, in dataset order]}``
    — a variable's inherent LEVEL, read off its records.

    A subject-level variable answers ``["subject"]``; a cycle-level one every
    key; a DATASET-level one (saved with no keys at all) ``[]``. A type with
    no records is absent — the two are different answers: "no key" is a
    level, "no records" is no information. Batched over *type_names* in one
    query, because a canvas render asks this for every input of every node.
    """
    keys = list(schema_keys or [])
    names = sorted({n for n in (type_names or []) if n})
    if not keys or not names:
        return {}
    cols = ", ".join(f'MAX(CASE WHEN s."{k}" IS NOT NULL THEN 1 ELSE 0 END)' for k in keys)
    rows = _chunked_in(
        duck,
        f"SELECT r.type, {cols} "  # noqa: S608 - keys are the dataset's own
        "FROM _record r LEFT JOIN _schema s ON s.schema_id = r.schema_id "
        "WHERE r.type IN ({ph}) GROUP BY r.type",
        names,
    )
    return {
        type_name: [k for k, flag in zip(keys, flags) if flag]
        for type_name, *flags in rows
    }


def finest_schema_keys(levels, schema_keys) -> list[str]:
    """The iteration level a set of inputs implies: every key ANY of them
    carries, in dataset order.

    Two inputs at different levels do not conflict — the run iterates the
    union, and an input without a key broadcasts across it
    (``docs/claude/coarse-level-inputs.md``). ``[subject, trial]`` beside
    ``[subject, session]`` iterates ``subject, session, trial``.
    """
    wanted: set[str] = set()
    for level in levels or []:
        wanted.update(k for k in (level or []) if k)
    return [k for k in (schema_keys or []) if k in wanted]


def config_call_id(fn_name: str, cfg: dict) -> str:
    """The call-site id a variant config reconstructs to.

    The stored config mapped onto :class:`scidb.foreach_config.CallSite` —
    the same type the forward ``ForEachConfig.to_call_id`` fills from live
    inputs, so the two directions agree by construction (this function
    only says which cfg field is which). PathInput specs ride in ``inputs``
    as their ``to_key()``; glue contributes NAMES, not hashes (an edited
    glue body is a new version at the same call site).
    """
    from .foreach_config import CallSite, RunOptions

    return CallSite(
        fn_name=fn_name,
        inputs={**cfg.get("input_types", {}), **cfg.get("path_inputs", {})},
        constants=cfg.get("constants", {}),
        options=RunOptions.from_config(cfg),
        glue={p: names for p, (_h, names) in (cfg.get("glue_chains") or {}).items()},
    ).call_id


def pipeline_variants(duck, output_type: str | None = None) -> list[dict]:
    """Distinct pipeline step variants, derived from the graph.

    Graph-native replacement for the old ``version_keys``-grouped
    ``list_pipeline_variants``. A variant is one ``(output_type, function_name,
    input_types, constants, output_num)`` combination — config-level
    (fn-hash- and instance-independent). Synthetic ``__save__`` invocations are
    excluded (they are not pipeline steps).

    Each dict: ``function_name``, ``output_type``, ``call_id`` (the
    reconstructed ``CallSite``'s id — the same type the forward
    ``ForEachConfig.to_call_id`` fills, so they match by construction),
    ``input_types`` (param→type), ``constants`` (param→typed value),
    ``run_options`` (:func:`run_options_label`), ``output_num`` (int|None),
    ``record_count`` (distinct output records).
    """
    from .foreach_config import CallSite, RunOptions

    inv_rows = duck._fetchall(
        "SELECT invocation_id, function_name, as_table, distribute, across_variants "
        "FROM _invocation WHERE function_name != ?",
        [SAVE_FUNCTION_NAME],
    )

    groups: dict = {}  # group_key -> info dict
    group_records: dict = {}  # group_key -> set(output_record_id)
    glue_invs = glue_invocation_ids(duck)

    for inv_id, fn_name, as_table, distribute, across_variants in inv_rows:
        if inv_id in glue_invs:
            continue  # a glue hop is not a pipeline step (D5)
        var_inputs, constants = invocation_inputs(duck, inv_id)
        input_types = {i["param_name"]: i["variable_type"] for i in var_inputs}
        pooled = sorted(across_variants or [])
        # The column selections the call used (`_invocation_input.selector`,
        # `{"columns": [...], "iterate": bool}`), so a target derived from
        # history re-runs with the columns it ran with — a Python
        # `Var["col"]` was invisible to the GUI's Run button until 2026-09-19.
        selectors = {
            param: json.loads(sel)
            for param, sel in duck._fetchall(
                "SELECT param_name, selector FROM _invocation_input "
                "WHERE invocation_id = ? AND selector IS NOT NULL",
                [inv_id],
            )
        }
        # PathInput specs ride in input_types as their to_key() JSON string —
        # preserves the legacy contract (get_aggregated_variants parses them) and
        # call_id parity (forward to_call_id includes them in __inputs).
        input_types.update(invocation_path_inputs(duck, inv_id))
        glue_names = {
            i["param_name"]: list(i["glue_chain"])
            for i in var_inputs
            if i.get("glue_chain")
        }
        at = sorted(as_table) if as_table else []

        # NB: where= is NOT part of config-variant identity. A where= filter's
        # only effect on the computation is the surviving input set, already folded
        # into invocation_id; its where_clause string is display-only (§10 where=
        # redesign). So two for_each calls differing only by where= are the same
        # config variant here.

        out_rows = duck._fetchall(
            "SELECT io.output_num, io.output_record_id, rec.type "
            "FROM _invocation_output io JOIN _record rec ON rec.record_id = io.output_record_id "
            "WHERE io.invocation_id = ?",
            [inv_id],
        )
        for output_num, out_rid, out_type in out_rows:
            if output_type is not None and out_type != output_type:
                continue
            gkey = (
                out_type,
                fn_name,
                tuple(sorted(input_types.items())),
                constants_identity_key(constants),
                tuple(sorted((k, tuple(v)) for k, v in glue_names.items())),
                output_num,
                tuple(at),
                bool(distribute),
                tuple(pooled),
            )
            if gkey not in groups:
                site = CallSite(
                    fn_name=fn_name,
                    inputs=input_types,
                    constants=constants,
                    options=RunOptions(
                        distribute=bool(distribute),
                        as_table=at or None,
                        across_variants=pooled,
                    ),
                    glue=glue_names,
                )
                groups[gkey] = {
                    "function_name": fn_name,
                    "output_type": out_type,
                    "call_id": site.call_id,
                    "input_types": input_types,
                    "constants": constants,
                    # The run-option set this variant ran under. Already part of
                    # the group key above (``distribute``/``as_table`` are
                    # folded into invocation_id, so two runs differing only by a
                    # flag are two variants) — it was simply never reported, so
                    # `scidb variants` showed them as indistinguishable rows.
                    # `for_columns` is derived from the selectors right here
                    # rather than read from `_invocation.for_columns`: one
                    # source of truth (the selector), so the reported mode can
                    # never disagree with the selection it describes, and a
                    # record written before the column existed still reports
                    # correctly.
                    "run_options": run_options_label(
                        distribute,
                        at,
                        sorted(
                            p
                            for p, s in selectors.items()
                            if isinstance(s, dict) and s.get("iterate")
                        ),
                        pooled,
                    ),
                    "across_variants": pooled,
                    # {param: [glue node name, ...]} — the reshaping this call
                    # site interposed on its inputs. Part of the variant so a
                    # GUI target derived from history re-runs WITH its glue.
                    "glue_chains": glue_names,
                    "selectors": selectors,
                    "output_num": output_num,
                }
                group_records[gkey] = set()
            group_records[gkey].add(out_rid)

    return [
        {**groups[gkey], "record_count": len(group_records[gkey])} for gkey in groups
    ]


def _producing_variant_key(duck, record_id: str):
    """A hashable key for the producing *variant* of a record: the constant
    bindings (sweep params) of its producing invocation, or ``None`` for raw
    records (no producing invocation).

    Re-saves and re-runs under the same constant config share a key (one is the
    superseding version of the other); genuinely different variants — the same
    type produced with different constants at the same schema — get different
    keys and so coexist. Input record_ids are deliberately excluded: re-running
    on a changed upstream input is the *same* variant, just newer.
    """
    inv = producing_invocation(duck, record_id)
    if inv is None:
        return None
    inv_id = inv[0]
    _var_inputs, constants = invocation_inputs(duck, inv_id)
    return constants_identity_key(constants)


def variant_keys_batch(duck, record_ids) -> dict:
    """Batched :func:`_producing_variant_key` — ``{record_id: key_or_None}``.

    Three queries regardless of how many records are asked for, against the two
    the per-record function costs *each* (``producing_invocation`` +
    ``invocation_inputs``). Anything enumerating a whole variable type's records
    must use this: the per-record form is the N+1 trap that
    ``docs/claude/schema-location-status.md`` names.

    The key is byte-identical to the per-record function's — same
    ``_safe_literal`` decode, same ``repr``, same sort — so the two can be mixed
    in one comparison without a conversion.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    inv_map = producing_invocation_batch(duck, ids)
    inv_ids = list(dict.fromkeys(inv[0] for inv in inv_map.values()))

    consts: dict = {inv_id: {} for inv_id in inv_ids}
    if inv_ids:
        rows = _chunked_in(
            duck,
            "SELECT ii.invocation_id, ii.param_name, c.value_repr "
            "FROM _invocation_input ii "
            "JOIN _record r ON r.record_id = ii.input_record_id "
            "JOIN _constant c ON c.record_id = ii.input_record_id "
            "WHERE ii.invocation_id IN ({ph}) AND r.type = ?",
            inv_ids,
            tail_params=[CONSTANT_TYPE],
        )
        for inv_id, param_name, value_repr in rows:
            consts[inv_id][param_name] = _safe_literal(value_repr)

    out: dict = {}
    for rid in ids:
        inv = inv_map.get(rid)
        if inv is None:
            out[rid] = None  # raw/manual record — no producing variant
            continue
        out[rid] = constants_identity_key(consts[inv[0]])
    return out


def identical_content_groups(duck, variable_name: str) -> list[tuple[str, list[str]]]:
    """``[(content_hash, [record_id, ...]), ...]`` for every group of two or
    more non-excluded records of ``variable_name`` holding **byte-identical**
    payloads.

    Duplicate content across schema locations is legal and often correct (a
    constant re-saved per subject, say), so this is an observation, not an
    error. It is worth observing because it is the exact fingerprint of a
    ``distribute=False`` run that should have been ``distribute=True``: the
    function was handed the same input at every location of the iterated key
    and returned the whole thing each time, so every location got the same
    payload instead of its own piece. Downstream that is invisible — a plot
    colored by that key draws N identical overlapping traces and shows what
    looks like a single line in whichever colour was drawn last.

    One query. Read-only; callers decide whether to warn.
    """
    rows = duck._fetchall(
        "SELECT content_hash, record_id FROM _record "
        "WHERE type = ? AND COALESCE(excluded, FALSE) = FALSE "
        "AND content_hash IS NOT NULL "
        "ORDER BY content_hash, record_id",
        [variable_name],
    )
    groups: dict = {}
    for content_hash, record_id in rows:
        groups.setdefault(content_hash, []).append(record_id)
    return [(h, rids) for h, rids in groups.items() if len(rids) > 1]


def current_records_by_schema_batch(duck, variable_name: str) -> dict:
    """Batched :func:`_current_records_by_schema`, which delegates here.

    Same contract — ``{schema_id: [record_id, ...]}``, latest per ``(schema
    location, producing variant)`` — computed with a fixed number of queries
    instead of two per candidate record. This is the "optimization later" the
    per-record docstring anticipated; it arrived when the location picker made
    the cost visible on every popup open.
    """
    rows = duck._fetchall(
        "SELECT rm.record_id, r.schema_id, rm.timestamp FROM _record_save rm "
        "JOIN _record r ON r.record_id = rm.record_id "
        "WHERE r.type = ? AND COALESCE(r.excluded, FALSE) = FALSE",
        [variable_name],
    )
    if not rows:
        return {}
    vkeys = variant_keys_batch(duck, [rid for rid, _sid, _ts in rows])

    best: dict = {}  # (schema_id, variant_key) -> (timestamp, record_id)
    for rid, sid, ts in rows:
        key = (sid, vkeys.get(rid))
        prev = best.get(key)
        if prev is None or ts > prev[0]:
            best[key] = (ts, rid)
    out: dict = {}
    for (sid, _vkey), (_ts, rid) in best.items():
        out.setdefault(sid, []).append(rid)
    return out


def latest_at_location_batch(duck, record_ids) -> dict:
    """For each record, what is now newest at its own ``(type, schema_id)``.

    ``{record_id: {"latest_variant", "latest_any", "type", "schema_id"}}`` where

    * ``latest_variant`` mirrors ``DatabaseManager.get_latest_record_id_for_variant``
      — newest non-excluded record at the same ``(type, schema_id)`` **sharing the
      producing variant**, NULL-safe on ``schema_id``;
    * ``latest_any`` mirrors ``state._get_latest_record_at_location`` — newest
      non-excluded record at the same ``(type, schema_id)`` regardless of
      variant, and ``None`` when ``schema_id`` is NULL (that helper compares with
      ``=``, which never matches NULL, so it finds nothing there).

    Both are what :func:`superseded_batch` tests against, kept here as one query
    pair because the two per-record originals differ in exactly one NULL rule and
    that difference is load-bearing.

    Ties on ``timestamp`` break on ``record_id`` descending. The per-record SQL
    leaves ties to DuckDB's row order; making it deterministic is the only
    intentional divergence, and it removes a source of flapping results rather
    than changing which record is "current" in any non-tied case.
    """
    ids = list(dict.fromkeys(record_ids))
    if not ids:
        return {}
    meta_rows = _chunked_in(
        duck,
        "SELECT record_id, type, schema_id FROM _record WHERE record_id IN ({ph})",
        ids,
    )
    meta = {rid: (rtype, sid) for rid, rtype, sid in meta_rows}
    # No early return on an empty `meta`: a record that has vanished from
    # `_record` must still get an entry below, because both per-record helpers
    # return None for it and None != rid is what makes it read as superseded.
    types = list({rtype for rtype, _sid in meta.values()})

    # Every candidate at every location of interest, in one sweep per type.
    cand_rows = _chunked_in(
        duck,
        "SELECT r.type, r.schema_id, rm.record_id, MAX(rm.timestamp) "
        "FROM _record_save rm JOIN _record r ON r.record_id = rm.record_id "
        "WHERE r.type IN ({ph}) AND COALESCE(r.excluded, FALSE) = FALSE "
        "GROUP BY r.type, r.schema_id, rm.record_id",
        types,
    )
    wanted = {(rtype, sid) for rtype, sid in meta.values()}
    by_loc: dict = {}
    for rtype, sid, rid, ts in cand_rows:
        if (rtype, sid) in wanted:
            by_loc.setdefault((rtype, sid), []).append((ts, rid))
    for pairs in by_loc.values():
        pairs.sort(key=lambda p: (p[0], p[1]), reverse=True)

    cand_ids = [rid for pairs in by_loc.values() for _ts, rid in pairs]
    vkeys = variant_keys_batch(duck, cand_ids + ids)

    out: dict = {}
    for rid in ids:
        if rid not in meta:
            # The record no longer exists; both per-record helpers return None.
            out[rid] = {
                "latest_variant": None,
                "latest_any": None,
                "type": None,
                "schema_id": None,
            }
            continue
        rtype, sid = meta[rid]
        pairs = by_loc.get((rtype, sid), [])
        want = vkeys.get(rid)
        latest_variant = next(
            (c for _ts, c in pairs if vkeys.get(c) == want),
            None,
        )
        latest_any = None if sid is None else (pairs[0][1] if pairs else None)
        out[rid] = {
            "latest_variant": latest_variant,
            "latest_any": latest_any,
            "type": rtype,
            "schema_id": sid,
        }
    return out


def superseded_batch(duck, record_ids, max_depth: int = 50) -> dict:
    """Batched :func:`~scidb.state._has_superseded_ancestor` —
    ``{record_id: bool}``: has anything this record was computed from been
    re-saved since?

    The per-record original BFSes the graph with two DB round-trips per ancestor
    edge, which is fine for one combo and unusable across a study. This builds
    the upstream closure **once** for every seed
    (:func:`_build_upstream_closure`), resolves "is this input still the newest
    at its location" for every referenced record in one batch
    (:func:`latest_at_location_batch`), marks each invocation dirty if any of its
    variable inputs is not, and then answers each seed by memoised reachability.

    The three rules that must not drift from the original:

    * **Glue records are walked through, never tested.** A virtual ``__glue__``
      record has no ``_record_save`` row, so both latest-lookups return None and
      it would read as superseded. Its supersession is its source's.
    * **A raw/manual ancestor terminates the walk** — nothing produced it, so
      nothing about it can be stale.
    * **A self-referential input (input rid == output rid) is stable**, which
      falls out of the in-progress marking below rather than needing its own
      case.

    ``max_depth`` matches the per-record default (50) rather than the closure
    builder's 20; a deeper chain than that is truncated identically to the
    original, i.e. it stops looking rather than guessing.
    """
    seeds = list(dict.fromkeys(record_ids))
    if not seeds:
        return {}
    rec_to_inv, _consts, inv_var_inputs, _fn_hash, _run = _build_upstream_closure(
        duck, seeds, max_depth
    )

    referenced = list(
        dict.fromkeys(rid for rids in inv_var_inputs.values() for rid in rids)
    )
    if not referenced:
        return dict.fromkeys(seeds, False)

    types = dict(
        _chunked_in(
            duck,
            "SELECT record_id, type FROM _record WHERE record_id IN ({ph})",
            referenced,
        )
    )
    testable = [rid for rid in referenced if types.get(rid) != GLUE_TYPE]
    latest = latest_at_location_batch(duck, testable)

    def _is_superseded_input(rid: str) -> bool:
        info = latest.get(rid)
        if info is None:
            return False
        if info["latest_variant"] != rid:
            return True
        return info["latest_any"] is not None and info["latest_any"] != rid

    dirty_inputs = {rid for rid in testable if _is_superseded_input(rid)}
    if dirty_inputs:
        logger.debug(
            "superseded_batch: %d of %d referenced input record(s) are no longer "
            "the newest at their location",
            len(dirty_inputs),
            len(referenced),
        )

    memo: dict = {}

    def _walk(rid: str, stack: set) -> bool:
        if rid in memo:
            return memo[rid]
        if rid in stack:
            # Cycle / self-referential input==output: contributes nothing, the
            # same as the original's `visited` guard.
            return False
        inv = rec_to_inv.get(rid)
        if inv is None:
            memo[rid] = False  # raw/manual terminus
            return False
        stack.add(rid)
        inputs = inv_var_inputs.get(inv[0], ())
        result = any(r in dirty_inputs for r in inputs) or any(
            _walk(r, stack) for r in inputs
        )
        stack.discard(rid)
        memo[rid] = result
        return result

    return {seed: _walk(seed, set()) for seed in seeds}


def _current_records_by_schema(duck, variable_name: str) -> dict:
    """``{schema_id: [record_id, ...]}`` for the *current* records of a variable
    type — the latest non-excluded record per ``(schema location, producing
    variant)``.

    A re-save creates a new record_id at the same ``(schema, variant)``; only the
    newest is current. Superseded records MUST NOT be enumerated: each one would
    otherwise contribute a stale invocation to the expected set (inflating
    ``counts`` and, in the re-save-before-first-run edge case, producing a false
    "missing" → false red). Distinct variants at one schema are kept separately —
    they are concurrently valid.

    Kept as the name node-state already calls; the work is
    :func:`current_records_by_schema_batch`, which resolves every candidate's
    producing variant in a fixed number of queries rather than two per record.
    """
    return current_records_by_schema_batch(duck, variable_name)


def _glue_virtualize(
    per_param: dict, cfg: dict
) -> dict:
    """Rewrite predicted source rids to virtual glue rids, per glued param.

    The save path routes a glued param's binding through a virtual record
    (``scidb.glue.fuse_glue``), so a prediction built from the raw records
    would compute a *different* ``invocation_id`` and report every node red.
    Both sides derive the id from :func:`scidb.glue.virtual_rid_map`, so they
    agree as long as they see the same input rid set.

    That set is the one caveat: prediction enumerates the source variable's
    **current** records, while the run saw whatever it actually loaded. A run
    narrowed by ``where=`` or ``schema_filter`` therefore has a different
    ``input_set_signature`` and its node reads as needing a re-run. That is a
    *conservative* failure (falsely red, never falsely green), and the two
    signatures are logged below so it is diagnosable rather than mysterious.
    """
    from .glue import input_set_signature
    from .provenance import compute_glue_record_id

    glue_chains = cfg.get("glue_chains") or {}
    if not glue_chains:
        return per_param

    out = dict(per_param)
    for param, (chain_h, _names) in glue_chains.items():
        by_schema = per_param.get(param)
        if not by_schema:
            continue
        all_rids = [rid for rids in by_schema.values() for rid in rids]
        # The chain's *hash* is stored on the glue invocation, so reuse it
        # rather than re-hashing bodies this process may not have imported.
        sig = input_set_signature(all_rids)
        out[param] = {
            sid: [compute_glue_record_id(chain_h, rid, sig) for rid in rids]
            for sid, rids in by_schema.items()
        }
        logger.debug(
            "glue prediction: param=%s chain_hash=%s input_set=%s over %d record(s)",
            param,
            chain_h,
            sig,
            len(all_rids),
        )
    return out


def _schema_locations(duck, schema_ids, schema_keys) -> dict:
    """``{schema_id: {key: value, ...}}`` (populated keys only) for the given
    schema rows — one query, so a prediction never asks per record."""
    ids = [s for s in dict.fromkeys(schema_ids) if s is not None]
    if not ids or not schema_keys:
        return {}
    cols = ", ".join(f'"{k}"' for k in schema_keys)
    rows = _chunked_in(
        duck,
        f"SELECT schema_id, {cols} FROM _schema WHERE schema_id IN ({{ph}})",  # noqa: S608
        ids,
    )
    return {
        sid: {k: v for k, v in zip(schema_keys, values) if v is not None}
        for sid, *values in rows
    }


def _schema_ids_at_level(duck, iterated, schema_keys) -> dict:
    """``{(value, ...) over the iterated keys: schema_id}`` for every schema
    row populated at EXACTLY that level — one query over `_schema`, which
    holds one row per location and is small."""
    keys = list(schema_keys or [])
    iterated_set = set(iterated or [])
    if not keys:
        return {}
    cols = ", ".join(f'"{k}"' for k in keys)
    try:
        rows = duck._fetchall(f"SELECT schema_id, {cols} FROM _schema", [])  # noqa: S608
    except Exception:
        return {}
    out: dict = {}
    for sid, *values in rows:
        loc = {k: v for k, v in zip(keys, values) if v is not None}
        if set(loc) == iterated_set:
            out[tuple(loc.get(k) for k in iterated)] = sid
    return out


def iterated_keys_for_invocations(duck, invocation_ids, schema_keys) -> "list[str] | None":
    """The schema keys a set of invocations ITERATED, read off the records
    they produced — the populated keys of their output locations, in dataset
    order; a ``distribute`` invocation saves one level below where it
    iterated, so its deepest key is dropped. ``None`` when nothing was
    produced (nothing to read a level from). Batched: one query."""
    ids = [i for i in dict.fromkeys(invocation_ids) if i]
    keys = list(schema_keys or [])
    if not ids or not keys:
        return None
    cols = ", ".join(f'MAX(CASE WHEN s."{k}" IS NOT NULL THEN 1 ELSE 0 END)' for k in keys)
    rows = _chunked_in(
        duck,
        f"SELECT MAX(CASE WHEN inv.distribute THEN 1 ELSE 0 END), {cols} "  # noqa: S608
        "FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "LEFT JOIN _schema s ON s.schema_id = r.schema_id "
        "WHERE inv.invocation_id IN ({ph})",
        ids,
    )
    populated: set[str] = set()
    distributed = False
    saw_any = False
    for dist, *flags in rows:
        if dist is None and not any(f for f in flags if f):
            continue
        saw_any = True
        distributed = distributed or bool(dist)
        populated.update(k for k, f in zip(keys, flags) if f)
    if not saw_any:
        return None
    level = [k for k in keys if k in populated]
    if distributed and level:
        level = level[:-1]
    return level


def _predict_config_invocations(duck, fn_hash: str, cfg: dict, into: set) -> None:
    """Add expected ``(invocation_id, schema_id)`` pairs for one config × current
    input data into ``into``.

    The prediction has to bind inputs exactly as the save path did, and that
    depends on where the call ITERATED relative to where each input's records
    sit (``cfg["iterated_keys"]``, read off the config's own outputs by
    :func:`function_variant_configs`):

    * an input whose records sit AT the iterated level binds one record per
      call — several records at one location are variants, and each is its
      own invocation (a cross product, as before);
    * an input whose records sit BELOW it (cycles pooled per trial) is
      AGGREGATED: every record under the location is one edge of ONE
      invocation, under the real parameter name (the save path's
      the typed ``GraphRecord.bindings`` since 2026-09-20);
    * an input COARSER than the level broadcasts: the record at the enclosing
      location binds at every location beneath it.

    Without a known level (a config with no outputs yet, or the never-run
    fallback) the old per-location cross product is used.

    An aggregating call auto-splits its pooled records by variant group
    (``scidb.bindings.variant_signature`` of each record's derived branch
    params — one call per group, Cartesian across split inputs, exactly as
    the save path expands its combos), so the prediction is grouped the same
    way with the same recipe. A ``ColumnSelection`` input pools every group
    (it never splits on the save side either), and so does an input the run
    pooled explicitly — ``cfg["across_variants"]``, read back from
    ``_invocation.across_variants``, the fact the save wrote for exactly this
    reader (until 2026-09-20 it was not recorded, and such a config predicted
    split invocations that were never written).
    """
    import itertools

    from .provenance import compute_constant_record_id, compute_invocation_id

    input_types = cfg["input_types"]
    if not input_types:
        return  # PathInput/no-DB-input config — realized_inputless_invocations covers it
    selectors = cfg["selectors"]
    across_variants = sorted(cfg.get("across_variants") or [])
    const_bindings = [
        (p, compute_constant_record_id(v)) for p, v in cfg["constants"].items()
    ]
    per_param = {
        param: _current_records_by_schema(duck, vtype)
        for param, vtype in input_types.items()
    }
    # Every candidate record's variant identity, in one closure build — the
    # aggregation grouping below reads it. Computed on the RAW rids; a glued
    # param's virtual rid inherits its source's (positional: the virtual map
    # preserves order).
    raw = dict(per_param)
    bp_by_rid = branch_params_batch(
        duck, [rid for m in raw.values() for rids in m.values() for rid in rids]
    )
    # Glued params bind to a virtual record, not the raw one — predict the
    # same id the save path wrote.
    per_param = _glue_virtualize(per_param, cfg)
    for p, by_sid in per_param.items():
        if by_sid is raw.get(p):
            continue
        for sid, rids in by_sid.items():
            for raw_rid, virt in zip(raw[p].get(sid, []), rids):
                bp_by_rid[virt] = bp_by_rid.get(raw_rid, {})
    param_names = list(input_types.keys())

    def _emit(choices_by_param: dict, sid) -> None:
        choices = [choices_by_param[p] for p in param_names]
        for combo in itertools.product(*choices):
            bindings = [
                (p, rid, selectors.get(p)) for p, rids in combo for rid in rids
            ]
            bindings += [(p, crid, None) for p, crid in const_bindings]
            inv_id = compute_invocation_id(
                fn_hash,
                cfg["as_table"],
                cfg["distribute"],
                bindings,
                across_variants=across_variants,
            )
            into.add((inv_id, sid))

    iterated = cfg.get("iterated_keys")
    schema_keys = list(cfg.get("schema_keys") or [])
    if iterated is None or not schema_keys:
        common_schema = (
            set.intersection(*[set(m.keys()) for m in per_param.values()])
            if per_param
            else set()
        )
        for sid in common_schema:
            # one record per choice: (param, [rid])
            _emit({p: [(p, [rid]) for rid in per_param[p][sid]] for p in param_names}, sid)
        return

    # Where every record of every param sits, in one query.
    all_sids = {sid for m in per_param.values() for sid in m}
    locations = _schema_locations(duck, all_sids, schema_keys)
    iterated_set = set(iterated)

    def _project(loc: dict) -> tuple:
        return tuple(loc.get(k) for k in iterated)

    # For each param: the locations (projected onto the iterated keys) it can
    # serve, and how — one record per call (at level), all records pooled
    # (below), or the enclosing record broadcast (coarser).
    at_level: dict[str, dict] = {}  # param -> {L: [rid, ...]}  (cross product)
    pooled: dict[str, dict] = {}  # param -> {L: [rid, ...]}  (one edge set)
    coarse: dict[str, list] = {}  # param -> [(loc, [rid, ...])]
    for p, by_sid in per_param.items():
        for sid, rids in by_sid.items():
            loc = locations.get(sid, {})
            populated = set(loc)
            if populated == iterated_set:
                at_level.setdefault(p, {}).setdefault(_project(loc), []).extend(rids)
            elif populated > iterated_set:
                pooled.setdefault(p, {}).setdefault(_project(loc), []).extend(rids)
            else:
                coarse.setdefault(p, []).append((loc, list(rids)))

    # Candidate locations: every projected location a non-coarse param has
    # data at; each param must be able to serve it.
    candidates: set = set()
    for m in list(at_level.values()) + list(pooled.values()):
        candidates |= set(m)
    # The OUTPUT location's schema row — the pair `present_invocation_schema_
    # pairs` will hold once the call has run. An aggregated input's records
    # sit below it, so it is looked up at the iterated level itself; before
    # the first run no row exists and the location travels as a tuple
    # (`state._schema_id_to_combo` reads either), which can only ever count
    # as missing — correct, since nothing was produced there.
    sid_by_location = _schema_ids_at_level(duck, iterated, schema_keys)
    for L in candidates:
        choices_by_param: dict = {}
        ok = True
        for p in param_names:
            if p in at_level and L in at_level[p]:
                choices_by_param[p] = [(p, [rid]) for rid in at_level[p][L]]
            elif p in pooled and L in pooled[p]:
                rids = pooled[p][L]
                if selectors.get(p) or p in across_variants:
                    choices_by_param[p] = [(p, sorted(rids))]
                    continue
                groups: dict = {}
                for rid in rids:
                    groups.setdefault(variant_signature(bp_by_rid.get(rid, {})), []).append(rid)
                location = dict(zip(iterated, L))
                aligned = [
                    sig for sig in sorted(groups) if not signature_conflicts_with(sig, location)
                ]
                if not aligned:
                    ok = False  # every group contradicts the location: no call ran here
                    break
                choices_by_param[p] = [(p, sorted(groups[sig])) for sig in aligned]
            elif p in coarse:
                serving = [
                    (p, [rid])
                    for loc, rids in coarse[p]
                    if all(loc.get(k) == v for k, v in zip(iterated, L) if k in loc)
                    for rid in rids
                ]
                if not serving:
                    ok = False
                    break
                choices_by_param[p] = serving
            else:
                ok = False
                break
        if not ok:
            continue
        _emit(choices_by_param, sid_by_location.get(L, tuple(zip(iterated, L))))


def config_from_inputs(inputs: dict, glue: dict | None = None) -> dict:
    """Build a variant config (same shape as :func:`function_variant_configs`
    entries) from a for_each-style ``inputs`` dict — used to predict expected
    invocations for a function that has never run yet.

    Mirrors ``ForEachConfig``'s own call-site view: a loadable spec becomes
    its variable TYPE name (every wrapper peeled through
    :mod:`scidb.input_spec`, the one unwrap), ColumnSelection contributes a
    selector, AcrossVariants a run option, PathInput/PathOutput/ColName are
    excluded, everything else is a constant. ``as_table``/``distribute``
    aren't expressible here → defaults.

    A ``Variant`` pin narrows WHICH records a run consumes but does not
    change its type, so — like ``where=`` — it is absent from the config: the
    edges of the run it produced already name exactly the pinned records.
    What it does NOT do is narrow the PREDICTION built from this config,
    which enumerates every current record of the type; see
    ``scidb/tests/test_variant_pin_node_state.py``.

    ``glue`` (the same value passed to ``for_each``) is normalized into the
    ``glue_chains`` shape ``function_variant_configs`` produces, so a glued
    never-run node predicts the virtual rids the first run will actually write.
    """
    from scifor import ColName

    from .across_variants import AcrossVariants
    from .foreach import _is_loadable
    from .input_spec import type_name
    from .provenance_save import compute_input_selectors

    try:
        from scifor import PathInput as _PathInput
        from scifor import PathOutput as _PathOutput
    except ImportError:
        _PathInput = _PathOutput = None

    input_types: dict = {}
    constants: dict = {}
    across_variants: list = []
    for name, spec in inputs.items():
        if _PathInput is not None and isinstance(spec, _PathInput):
            continue
        if _PathOutput is not None and isinstance(spec, _PathOutput):
            continue
        if isinstance(spec, ColName):
            continue
        if _is_loadable(spec):
            if isinstance(spec, AcrossVariants):
                across_variants.append(name)
            # ONE unwrap (`input_spec`), so this never again knows about a
            # narrower set of wrappers than the forward `call_site_inputs`
            # does — a `Variant`-pinned input used to vanish from here
            # entirely, because a Variant is not a type.
            vt = type_name(spec)
            if vt is not None:
                input_types[name] = vt
        else:
            constants[name] = spec
    from .glue import chain_hash, chain_names, normalize_glue

    glue_chains = {
        param: (chain_hash(chain), tuple(chain_names(chain)))
        for param, chain in normalize_glue(glue).items()
    }
    return {
        "input_types": input_types,
        "selectors": compute_input_selectors(inputs),
        "constants": constants,
        "glue_chains": glue_chains,
        "as_table": [],
        "distribute": False,
        "across_variants": sorted(across_variants),
    }


def realized_inputless_invocations(
    duck, fn_name: str, fn_hash: str | None = None
) -> set:
    """``{(invocation_id, schema_id)}`` for invocations of ``fn_name`` that have
    **no variable inputs** (only constants, or nothing) — i.e. PathInput-only
    loaders and similar source nodes.

    These have no DB input data to predict an expected set from, so their
    *realized* output locations ARE their expected set: present == expected →
    the node reports green when run, red when never run.

    That alone cannot see a partial run — un-run combos leave no trace in the
    graph. The live source those functions DO have is the filesystem, and
    ``state._discovery_gate`` consults it on top of this, so the node a user
    sees is red when files on disk have never been loaded. Nothing here
    changed: this is still the graph's honest answer, and the gate is a second
    question asked beside it.

    ``fn_hash`` restricts the result to invocations produced by that version of
    the function's source. **This is what makes an inputless loader notice a
    body edit.** Without it the realized set is whatever exists under any
    historical version, so expected ≡ present unconditionally and the node
    reports green forever however the code changes — the one route by which a
    function could be edited and leave its node still claiming to be current.
    A function *with* variable inputs has always been covered, because
    ``_predict_config_invocations`` folds the hash into ``invocation_id``; this
    closes the same loop for the case that never reaches prediction.

    Passing None keeps the old any-version behaviour, which is what the
    PathInput discovery check wants (``realized_inputless_schema_ids`` asks
    "where has this loader produced output", not "under which code").

    Pure structural read from the graph — no invocation_id recomputation, so no
    predicted-vs-realized drift.
    """
    out: set = set()
    if fn_hash is None:
        rows = duck._fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?",
            [fn_name],
        )
    else:
        rows = duck._fetchall(
            "SELECT invocation_id FROM _invocation "
            "WHERE function_name = ? AND function_hash = ?",
            [fn_name, fn_hash],
        )
    for (inv_id,) in rows:
        has_var_input = duck._fetchall(
            "SELECT 1 FROM _invocation_input ii "
            "JOIN _record r ON r.record_id = ii.input_record_id "
            "WHERE ii.invocation_id = ? AND r.type NOT IN (?, ?) LIMIT 1",
            [inv_id, CONSTANT_TYPE, PATHINPUT_TYPE],
        )
        if has_var_input:
            continue  # has variable inputs → live prediction handles it
        for (sid,) in duck._fetchall(
            "SELECT DISTINCT r.schema_id FROM _invocation_output io "
            "JOIN _record r ON r.record_id = io.output_record_id "
            "WHERE io.invocation_id = ?",
            [inv_id],
        ):
            out.add((inv_id, sid))
    return out


def is_inputless_function(duck, fn_name: str) -> bool:
    """True when no invocation of ``fn_name`` has a *variable* input.

    A PathInput / constant-only loader — the shape whose expected set cannot be
    predicted from upstream data, because there is no upstream data. Both
    callers that care (``scidb.locations`` for the picker's denominator, and
    ``state.check_node_state`` for the canvas badge) need the same answer, so it
    lives here with the rest of the graph queries rather than in either of them.

    A function with **no recorded invocations at all** also returns True — it
    has no variable inputs in the trivial sense. Callers that act on this must
    check for invocations separately; "never run" is not "inputless".
    """
    rows = duck._fetchall(
        "SELECT 1 FROM _invocation inv "
        "JOIN _invocation_input ii ON ii.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE inv.function_name = ? AND r.type NOT IN (?, ?, ?) LIMIT 1",
        [fn_name, CONSTANT_TYPE, PATHINPUT_TYPE, GLUE_TYPE],
    )
    return not rows


def function_versions_recorded(duck, fn_name: str) -> set:
    """Every distinct ``function_hash`` ever recorded for ``fn_name``.

    Diagnostic only: lets a red node say "the source changed since this last
    ran" instead of leaving the user to guess between that, never-run, and
    partially-run — which all look identical from outside.
    """
    return {
        row[0]
        for row in duck._fetchall(
            "SELECT DISTINCT function_hash FROM _invocation WHERE function_name = ?",
            [fn_name],
        )
        if row[0]
    }


def realized_inputless_schema_ids(duck, fn_name: str, const_rids: dict) -> set:
    """Schema_ids where ``fn_name`` produced output via an **inputless** invocation
    whose constant inputs exactly match ``const_rids`` (``{param: constant_record_id}``).

    Used by the PathInput-node outdated check (``state.check_pathinput_node_state``)
    to find the locations the loader has *actually* produced under the current
    constant config. Constants are content-addressed, so matching is a plain
    record_id dict-equality — no value round-trip and no invocation_id recompute
    (the only hashing is the caller's ``compute_constant_record_id`` on the live
    constants, which is identical everywhere). PathInput specs are deliberately
    NOT part of this match: a template change does not fork a variant (see §10 #6).
    """
    by_inv: dict = {}
    for inv_id, sid in realized_inputless_invocations(duck, fn_name):
        by_inv.setdefault(inv_id, set()).add(sid)
    out: set = set()
    for inv_id, sids in by_inv.items():
        rows = duck._fetchall(
            "SELECT ii.param_name, ii.input_record_id FROM _invocation_input ii "
            "JOIN _record r ON r.record_id = ii.input_record_id "
            "WHERE ii.invocation_id = ? AND r.type = ?",
            [inv_id, CONSTANT_TYPE],
        )
        if dict(rows) == const_rids:
            out |= sids
    return out


def expected_invocations_for_function(
    db,
    fn_name: str,
    fn_hash: str,
    inputs_fallback: dict | None = None,
    call_id: str | None = None,
    glue_fallback: dict | None = None,
) -> set:
    """Expected ``{(invocation_id, schema_id)}`` pairs for ``fn_name`` (§9c).

    Derived live from the graph (no persisted snapshot — see the removal of
    ``_for_each_expected``). Union of:
      * the realized inputless invocations of the function (zero-DB-input loaders
        whose expected set is exactly what they have produced — see
        :func:`realized_inputless_invocations`),
      * a live prediction from current input data for each variant config the
        function has already been run with (so input data added *after* the last
        run still surfaces as missing), and
      * a live prediction from ``inputs_fallback`` when provided — lets a
        never-run function enumerate its expected combos from its declared inputs.

    ``call_id`` scopes the check to ONE call site: only variant configs whose
    :func:`config_call_id` matches contribute (predictions AND realized
    inputless pairs, the latter restricted to matching configs'
    ``invocation_ids``), so a fn reused across call sites never blurs — one
    site's partial run cannot redden another's fully-run node.

    Each pair's presence (an output of that invocation at that schema location)
    is the completeness signal — see :func:`present_invocation_schema_pairs`.

    Note: a zero-input function (e.g. a PathInput-only loader) has no input data
    to enumerate, so it contributes only the invocations it has already realized
    **under the current ``fn_hash``**. From this function alone such a node
    reports **green** (run, even partially, with the code as it stands) or
    **red** (never run, or run only by a since-edited version).

    "Even partially" is the limit of what the graph can say, and it is not the
    answer a user gets: ``state.check_node_state`` asks the filesystem too
    (``_discovery_gate``), so a loader with files it has never loaded reads red.
    Callers reading this function directly get the graph's answer and should
    not present it as node state.
    """
    duck = db._duck
    expected: set = set()

    configs = function_variant_configs(duck, fn_name)
    if call_id is not None:
        matched = [c for c in configs if config_call_id(fn_name, c) == call_id]
        logger.debug(
            "expected_invocations(%s): call_id=%s matched %d/%d config(s)",
            fn_name,
            call_id,
            len(matched),
            len(configs),
        )
        configs = matched
        scoped_inv_ids = (
            set().union(*[c["invocation_ids"] for c in configs]) if configs else set()
        )

    # (a) realized inputless invocations (PathInput-only loaders, etc.),
    # restricted to the CURRENT version of the function's source. An edited body
    # empties this set, so every previously-realized combo drops out of
    # `expected`, the node reports needs-run, and re-running refills it under
    # the new hash. Without the restriction a PathInput-only loader could never
    # go red however much its code changed.
    realized = realized_inputless_invocations(duck, fn_name, fn_hash)
    if call_id is not None:
        realized = {(i, s) for i, s in realized if i in scoped_inv_ids}
    expected |= realized

    # (b) live prediction per known variant config × current input data
    for cfg in configs:
        _predict_config_invocations(duck, fn_hash, cfg, expected)

    # (c) live prediction from the declared inputs — the NEVER-RUN fallback,
    # and only that. A declaration says which types feed which parameters; it
    # cannot say at which schema level the call iterates, because that is
    # decided per run (`for_each(subject=[], trial=[])`). So a fallback
    # prediction always takes `_predict_config_invocations`' no-level branch
    # and enumerates one invocation per input LOCATION — right for a node
    # that has never run, and wrong for an aggregating one that has: those
    # per-location invocations were never written, so the node counted them
    # missing and could never plan green. (It went unnoticed because a call
    # site with `as_table` or `distribute` has a different call id from the
    # option-less `config_from_inputs` shape, so the fallback was skipped
    # there — the only aggregating node-state test in the suite.)
    #
    # History is authoritative about the level, so the fallback contributes
    # only when this call site has no recorded config of its own.
    if inputs_fallback:
        fallback_cfg = config_from_inputs(inputs_fallback, glue=glue_fallback)
        fallback_cid = config_call_id(fn_name, fallback_cfg)
        already_run = any(config_call_id(fn_name, c) == fallback_cid for c in configs)
        if (call_id is None or fallback_cid == call_id) and not already_run:
            _predict_config_invocations(duck, fn_hash, fallback_cfg, expected)
        elif already_run:
            logger.debug(
                "expected_invocations(%s): declared inputs match a recorded "
                "config (call_id=%s) — history decides the iteration level, "
                "not the declaration",
                fn_name,
                fallback_cid,
            )

    return expected


def variable_content_fingerprint(duck, variable: str) -> tuple[int, int]:
    """``(n_records, fingerprint)`` over a variable's non-excluded records.

    A cheap answer to "has this variable's CONTENT changed?", for caches that
    hold a whole variable and cannot be told when someone writes to it. Both
    halves derive from ``record_id``, which is a content hash, so:

    * re-running a function over unchanged inputs produces the SAME record_ids
      and leaves the fingerprint alone — a cache built before that run is still
      correct and must not be thrown away;
    * a run whose output differs anywhere writes new record_ids and changes the
      fingerprint, as does excluding or un-excluding a record.

    Deliberately NOT ``max(timestamp)`` over ``_record_save``: every execution
    appends a save event even when it stores nothing new (that table is the
    audit trail), so a timestamp watermark would invalidate on every re-run
    including the no-op ones.

    ``bit_xor`` over per-id hashes is order-independent and needs no sort, so
    this stays one aggregate scan whatever the row count.
    """
    row = duck._fetchone(
        "SELECT count(*), coalesce(bit_xor(hash(record_id)), 0) "
        "FROM _record WHERE type = ? AND excluded IS DISTINCT FROM TRUE",
        [variable],
    )
    if not row:
        return (0, 0)
    # Folded in: the schema-exclusion registry. A plot leaves excluded
    # locations out (scistackplotdb.load), so excluding or re-including a
    # trial must invalidate a cached frame exactly as a new record does.
    overrides = duck._fetchone(
        "SELECT coalesce(bit_xor(hash(CAST(changed_at AS VARCHAR) || CAST(status AS VARCHAR))), 0) "
        "FROM __scidb_schema_overrides"
    )
    salt = int(overrides[0] or 0) if overrides else 0
    return (int(row[0]), int(row[1] or 0) ^ salt)
