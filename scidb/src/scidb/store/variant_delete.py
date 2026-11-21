"""Variant deletion: the ONE sanctioned real delete in scidb.

``.claude/plan-variants-popup.md`` Stage 3; the rules are in
``docs/claude/variant-pins-and-deletion.md`` §3. The project never deletes
data. This module exists for **erroneous runs that shouldn't be part of the
record** (user decision, 2026-09-30) and must not be generalised to other
surfaces.

Two steps, always:

1. :func:`delete_plan` is a **dry run**. It resolves the targets to records,
   adds everything computed from them (the downstream closure), and works out
   which invocations and runs are left with nothing. It writes nothing. The GUI
   and the CLI show it before anything is deleted.
2. :func:`delete_variant` re-plans **inside one transaction**, refuses if the
   result differs from the plan the user saw (``expect_fingerprint``), deletes
   exactly that, writes a tombstone and releases any pin left pointing at
   nothing. Any failure rolls the whole thing back.

**Undo (GitHub #26, 2026-11-19).** Every deleted row is first saved to the
trash (``scidb.store.trash``: Parquet files beside the database), inside the
same transaction. :func:`restore_deleted` puts them back and re-activates the
released pins; :func:`delete_again` redoes it. Both are all or nothing and
refuse (``trash.TrashConflict``) rather than corrupt: a restore when a re-run
wrote the same records, a redo when the plan is no longer the same. The trash
lasts until ``trash.purge_trash``. See
``docs/claude/deleting-entries-with-data.md``.

What is removed, for the planned records:

* ``<Type>_data`` rows, ``_record_save`` and ``_record``;
* ``_invocation_output`` edges, and ``_invocation_input`` edges into them;
* invocations left with no output, with their ``_invocation_input`` and
  ``_run_invocation`` rows;
* runs left with no invocation.

What survives: shared ``__constant__`` / ``__pathinput__`` records (other
variants use them), ``_function_source``, ``_schema`` rows, files on disk
(``generates_file``), and any invocation or run that also produced a record
that is not being deleted.

Target kinds (a list of dicts, combined as a union):

* ``{"variable": V, "record_ids": [...]}``: exactly these records of V (the
  popup's per-location delete, e.g. a stale record a newer run never replaced);
* ``{"variable": V, "card_id": C}``: one variant card, every record on it;
* ``{"variable": V, "selection": S}``: every card of V whose coordinate
  satisfies S;
* ``{"function": F, "param": P, "value": X}``: every output of an invocation
  of F that bound constant P to X (D9: "remove the value from the Parameter");
* ``{"parameter": N, "value": X}``: the same, for every constant edge that
  belongs to Parameter N by ``parameter.parameter_node_name`` (its recorded
  ``declared_name``, else its argument name). This is the canvas Parameter
  node's identity, so "remove 10 from this Parameter" needs no canvas wiring.

Whole-entity targets (the sidebar's Delete, GitHub #26):

* ``{"variable": V}``: every record of V, ``exclude_variant``-hidden ones too;
* ``{"parameter": N}`` (no ``value``): every value of Parameter N;
  ``{"function": F, "param": P}`` (no ``value``) likewise;
* ``{"function": F}``: every output of every invocation of F;
* ``{"path_input": N}``: everything computed from PathInput N's
  ``__pathinput__`` record(s). It is keyed by name, so unlike a constant it
  is never shared, and the downstream walk is exact.

Known gap: a Parameter target matches only plain constant edges; a value that
reaches its function through constant-fed glue binds a ``__constant_glue__``
record and is not seen.

Card and selection targets use whole cards, not ``records_for_variant``,
because a ``latest`` load returns one record per location. Deleting only that
one would promote an older save of the same variant to "latest", bringing
deleted data back.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from scidb.model.log import Log
from scidb.store.tables import VARIANT_TOMBSTONE

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

TOMBSTONE_TABLE = VARIANT_TOMBSTONE.name


@dataclass
class DeletePlan:
    targets: list[dict]
    #: Every record that would be deleted, sorted.
    record_ids: list[str]
    #: ``{variable: count}`` over all planned records (glue values excluded).
    by_variable: dict[str, int]
    #: The part of ``by_variable`` the targets named directly.
    seed_by_variable: dict[str, int]
    #: The part computed from the seeds (downstream).
    downstream_by_variable: dict[str, int]
    #: ``{variable: [location, ...]}`` where nothing of that variable would be
    #: left at all after the delete.
    lost_locations: dict[str, list[dict]]
    invocation_ids: list[str]
    run_ids: list[str]
    #: Variables whose active pin would match nothing afterwards; released.
    pins_to_release: list[str]
    #: Hash of the planned record, invocation and run ids. ``delete_variant``
    #: refuses when its own plan's fingerprint differs.
    fingerprint: str
    warnings: list[str] = field(default_factory=list)

    @property
    def total_records(self) -> int:
        return len(self.record_ids)


@dataclass
class Tombstone:
    tombstone_id: str
    deleted_at: str
    reason: str
    targets: list[dict]
    by_variable: dict[str, int]
    record_ids: list[str]
    #: ``scidb.store.trash`` state: ``"trashed"`` (undoable), ``"restored"``,
    #: ``"purged"``, or None (a hard delete: a clear, a database with no file,
    #: or one made before the trash existed).
    trash_state: str | None = None


@dataclass
class DeleteResult:
    tombstone: Tombstone
    #: ``{table: rows deleted}``.
    deleted_rows: dict[str, int]
    released_pins: list[str]
    plan: DeletePlan
    #: True when the rows were saved to the trash, so ``restore_deleted`` can
    #: undo this delete.
    undoable: bool = False


@dataclass
class RestoreResult:
    tombstone_id: str
    #: ``{table: rows put back}``.
    restored_rows: dict[str, int]
    #: Variables whose released pin was made active again.
    repinned: list[str]
    #: Pins that could not be restored, and why. The data is restored anyway.
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def write_tombstone(
    duck,
    reason: str,
    targets: list[dict],
    by_variable: dict,
    record_ids: list[str],
    *,
    tombstone_id: str | None = None,
) -> Tombstone:
    """Write one tombstone row: the audit trail of every real delete (this
    module's variant delete and ``scidb.store.clear_data``'s clear). The caller
    holds the transaction. ``tombstone_id`` is passed when the trash folder
    was named before the tombstone was written."""
    tomb = Tombstone(
        tombstone_id=tombstone_id or uuid.uuid4().hex[:16],
        deleted_at=datetime.now().isoformat(),
        reason=str(reason),
        targets=list(targets),
        by_variable=dict(by_variable),
        record_ids=list(record_ids),
    )
    duck.execute(
        f"INSERT INTO {TOMBSTONE_TABLE} (tombstone_id, deleted_at, "
        "reason, targets_json, counts_json, record_ids_json) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            tomb.tombstone_id,
            tomb.deleted_at,
            tomb.reason,
            json.dumps(tomb.targets, sort_keys=True, default=str),
            json.dumps(tomb.by_variable, sort_keys=True),
            json.dumps(tomb.record_ids),
        ],
    )
    return tomb


def tombstones(db: "DatabaseManager", variable: str | None = None) -> list[Tombstone]:
    """Every deletion ever made, oldest first (only those that touched
    ``variable`` when given)."""
    duck = db._duck
    if not duck.table_exists(TOMBSTONE_TABLE):
        return []
    rows = duck.fetchall(
        "SELECT tombstone_id, deleted_at, reason, targets_json, "
        f"counts_json, record_ids_json FROM {TOMBSTONE_TABLE} ORDER BY deleted_at"
    )
    from scidb.store import trash

    trash_states = trash.states(db)
    out = []
    for tid, at, reason, targets, counts, rids in rows:
        t = Tombstone(
            tombstone_id=tid,
            deleted_at=at,
            reason=reason,
            targets=json.loads(targets),
            by_variable=json.loads(counts),
            record_ids=json.loads(rids),
            trash_state=trash_states.get(tid),
        )
        if variable is None or variable in t.by_variable:
            out.append(t)
    return out


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _same_value(a, b) -> bool:
    from scidb.run.bindings import variant_signature

    return variant_signature({"v": a}) == variant_signature({"v": b})


def _parameter_edges(duck, name: str) -> list[tuple[str, str]]:
    """``[(invocation_id, value_repr)]``: every constant edge that belongs to
    Parameter ``name``. Which Parameter an edge belongs to is
    ``parameter.parameter_node_name``'s rule (the recorded declared name, else
    the argument it filled), the same identity the canvas keys Parameter nodes
    by. SQL narrows the candidates; the owner decides."""
    from scidb.model.parameter import parameter_node_name
    from scidb.provenance.graph import CONSTANT_TYPE

    candidates = duck.fetchall(
        "SELECT ii.invocation_id, c.value_repr, ii.param_name, "
        "ii.declared_name FROM _invocation_input ii "
        "JOIN _constant c ON c.record_id = ii.input_record_id "
        "JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE r.type = ? AND (ii.declared_name = ? OR "
        "(ii.declared_name IS NULL AND ii.param_name = ?))",
        [CONSTANT_TYPE, name, name],
    )
    return [
        (inv, value_repr)
        for inv, value_repr, param, declared in candidates
        if parameter_node_name(param, {param: declared} if declared else None) == name
    ]


def parameter_values(db: "DatabaseManager", name: str) -> list:
    """The distinct values Parameter ``name`` has run with (what a
    ``{"parameter": name, "value": v}`` target can delete), in first-seen
    order. For the sidebar Delete's value checkboxes."""
    from scidb.provenance import query as pq

    out: list = []
    for _inv, value_repr in _parameter_edges(db._duck, name):
        value = pq._safe_literal(value_repr)
        if not any(_same_value(value, seen) for seen in out):
            out.append(value)
    return out


def _outputs_of(duck, invs) -> set:
    from scidb.provenance.query import _chunked_in

    return {
        out_rid
        for (out_rid,) in _chunked_in(
            duck,
            "SELECT output_record_id FROM _invocation_output WHERE invocation_id IN ({ph})",
            sorted(invs),
        )
    }


def _seed_records(db, targets: list[dict]) -> tuple[set, list[str]]:
    """Resolve the targets to record ids. Returns ``(rids, warnings)``."""
    from scidb.provenance import query as pq
    from scidb.inspect.variant_cards import build_variant_cards, selection_matches
    from scidb.model.variant import normalize_selection

    duck = db._duck
    seeds: set = set()
    warnings: list[str] = []
    cards_cache: dict = {}

    def cards_of(variable):
        if variable not in cards_cache:
            cards_cache[variable] = build_variant_cards(db, variable, include_runs=False)
        return cards_cache[variable].cards

    for target in targets:
        if "record_ids" in target:
            # Named records (the popup's per-location delete). Only records of
            # the named variable, so a stale id from another type cannot ride in.
            wanted = [str(r) for r in target["record_ids"] or ()]
            variable = target.get("variable")
            if not wanted:
                raise ValueError("delete: a record_ids target names no records")
            rows = pq._chunked_in(
                duck,
                "SELECT record_id, type FROM _record WHERE record_id IN ({ph})",
                wanted,
            )
            found = {rid for rid, rtype in rows if variable is None or rtype == variable}
            missing = sorted(set(wanted) - found)
            if missing:
                raise ValueError(
                    f"delete: {len(missing)} record(s) are not "
                    f"{variable or 'records'} in this database: {missing[:5]}"
                )
            seeds.update(found)
        elif "card_id" in target:
            variable = target["variable"]
            hit = [c for c in cards_of(variable) if c.card_id == target["card_id"]]
            if not hit:
                raise ValueError(
                    f"delete: {variable} has no card {target['card_id']!r} (the "
                    f"database changed since the cards were read?)"
                )
            seeds.update(hit[0].record_ids)
        elif "selection" in target:
            variable = target["variable"]
            selection = normalize_selection(target["selection"] or {})
            hit = [c for c in cards_of(variable) if selection_matches(selection, c.selection)]
            if not hit:
                raise ValueError(f"delete: no {variable} card matches {selection}")
            if len(hit) > 1:
                warnings.append(
                    f"{variable}: selection {selection} matches {len(hit)} cards "
                    f"{[c.card_id for c in hit]}; all are planned"
                )
            for card in hit:
                seeds.update(card.record_ids)
        elif "function" in target and "param" not in target:
            # A whole function (the sidebar's Delete): every output it made.
            invs = [
                inv
                for (inv,) in duck.fetchall(
                    "SELECT invocation_id FROM _invocation WHERE function_name = ?",
                    [target["function"]],
                )
            ]
            if not invs:
                warnings.append(f"function {target['function']}: it never ran")
                continue
            seeds.update(_outputs_of(duck, invs))
        elif "path_input" in target:
            # A whole PathInput. Its `__pathinput__` record is keyed by NAME
            # (`provenance.save._pathinput_bindings`), so unlike a constant it
            # is never shared: everything computed from it, glue hops
            # included, is exactly what it fed. The spec record itself stays,
            # like every `__constant__` / `__pathinput__` record.
            from scidb.provenance.graph import PATHINPUT_TYPE, parse_path_input_spec

            name = target["path_input"]
            heads = {
                rid
                for rid, value_repr in duck.fetchall(
                    "SELECT r.record_id, c.value_repr FROM _record r "
                    "JOIN _constant c ON c.record_id = r.record_id WHERE r.type = ?",
                    [PATHINPUT_TYPE],
                )
                if (parse_path_input_spec(value_repr) or {}).get("name") == name
            }
            fed = _downstream(duck, heads) if heads else set()
            if not fed:
                warnings.append(f"PathInput {name}: nothing was computed from it")
                continue
            seeds.update(fed)
        elif "variable" in target:
            # A whole variable: every record of the type, records hidden with
            # `exclude_variant` included (they are on no card).
            rids = [
                rid
                for (rid,) in duck.fetchall(
                    "SELECT record_id FROM _record WHERE type = ?", [target["variable"]]
                )
            ]
            if not rids:
                warnings.append(f"{target['variable']}: no records")
                continue
            seeds.update(rids)
        elif "function" in target or "parameter" in target:
            if "function" in target:
                rows = duck.fetchall(
                    "SELECT ii.invocation_id, c.value_repr FROM _invocation_input ii "
                    "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
                    "JOIN _constant c ON c.record_id = ii.input_record_id "
                    "WHERE inv.function_name = ? AND ii.param_name = ?",
                    [target["function"], target["param"]],
                )
                label = f"{target['function']}.{target['param']}"
            else:
                name = target["parameter"]
                rows = _parameter_edges(duck, name)
                label = f"Parameter {name}"
            # No "value": every value (a whole Parameter).
            every = "value" not in target
            invs = sorted(
                {
                    inv
                    for inv, value_repr in rows
                    if every or _same_value(pq._safe_literal(value_repr), target["value"])
                }
            )
            if not invs:
                what = label if every else f"{label}={target['value']!r}"
                warnings.append(f"{what}: no invocation used it")
                continue
            seeds.update(_outputs_of(duck, invs))
        else:
            raise ValueError(f"delete: unknown target shape {target!r}")
    return seeds, warnings


def _downstream(duck, seeds: set) -> set:
    """Every record computed, at any depth, from ``seeds`` (seeds excluded)."""
    from scidb.provenance.query import _chunked_in

    seen = set(seeds)
    frontier = list(seeds)
    out: set = set()
    while frontier:
        rows = _chunked_in(
            duck,
            "SELECT DISTINCT io.output_record_id FROM _invocation_input ii "
            "JOIN _invocation_output io ON io.invocation_id = ii.invocation_id "
            "WHERE ii.input_record_id IN ({ph})",
            frontier,
        )
        frontier = [r for (r,) in rows if r not in seen]
        seen.update(frontier)
        out.update(frontier)
    return out


def delete_plan(db: "DatabaseManager", targets: list[dict]) -> DeletePlan:
    """What :func:`delete_variant` would remove. Writes nothing."""
    from scidb.provenance import query as pq
    from scidb.provenance.graph import GLUE_RECORD_TYPES
    from scidb.store.variant_pins import active_pins

    duck = db._duck
    t0 = time.perf_counter()
    if not targets:
        raise ValueError("delete: no targets")
    seeds, warnings = _seed_records(db, targets)
    downstream = _downstream(duck, seeds) if seeds else set()
    rids = sorted(seeds | downstream)
    rid_set = set(rids)

    types: dict = {}
    sids: dict = {}
    for rid, rtype, sid in pq._chunked_in(
        duck, "SELECT record_id, type, schema_id FROM _record WHERE record_id IN ({ph})", rids
    ):
        types[rid] = rtype
        sids[rid] = sid

    def count(ids):
        out: dict = {}
        for rid in ids:
            t = types.get(rid)
            if t and t not in GLUE_RECORD_TYPES:
                out[t] = out.get(t, 0) + 1
        return dict(sorted(out.items()))

    # Invocations: every one that produced or consumed a planned record, and
    # of those, the ones with no surviving output.
    # Two queries, not a UNION: `_chunked_in` binds its ids once, so a second
    # `{ph}` in one statement would be short of parameters.
    touched: set = set()
    if rids:
        touched |= {
            inv
            for (inv,) in pq._chunked_in(
                duck,
                "SELECT invocation_id FROM _invocation_output "
                "WHERE output_record_id IN ({ph})",
                rids,
            )
        }
        touched |= {
            inv
            for (inv,) in pq._chunked_in(
                duck,
                "SELECT invocation_id FROM _invocation_input "
                "WHERE input_record_id IN ({ph})",
                rids,
            )
        }
    outputs = pq.invocation_outputs_batch(duck, sorted(touched))
    dead_invs = sorted(
        inv
        for inv in touched
        if all(out_rid in rid_set for _n, out_rid, _t in outputs.get(inv, ()))
    )
    run_rows = pq._chunked_in(
        duck,
        "SELECT run_id, invocation_id FROM _run_invocation WHERE run_id IN ("
        "SELECT run_id FROM _run_invocation WHERE invocation_id IN ({ph}))",
        dead_invs,
    ) if dead_invs else []
    dead_inv_set = set(dead_invs)
    runs: dict = {}
    for run_id, inv in run_rows:
        runs.setdefault(run_id, set()).add(inv)
    dead_runs = sorted(r for r, invs in runs.items() if invs <= dead_inv_set)

    # Locations each variable would lose entirely.
    lost: dict = {}
    affected_types = sorted({t for t in types.values() if t not in GLUE_RECORD_TYPES})
    if affected_types:
        remaining = pq._chunked_in(
            duck,
            "SELECT type, schema_id, record_id FROM _record WHERE type IN ({ph})",
            affected_types,
        )
        kept: dict = {}
        for t, sid, rid in remaining:
            if rid not in rid_set:
                kept.setdefault(t, set()).add(sid)
        lost_sids: dict = {}
        for rid in rids:
            t = types.get(rid)
            if t and t not in GLUE_RECORD_TYPES and sids.get(rid) not in kept.get(t, set()):
                lost_sids.setdefault(t, set()).add(sids.get(rid))
        all_lost = {s for v in lost_sids.values() for s in v if s is not None}
        locs = pq._schema_locations(duck, all_lost, list(db.dataset_schema_keys))
        keys = list(db.dataset_schema_keys)
        for t, sset in sorted(lost_sids.items()):
            rows = [locs.get(s, {}) for s in sset if s is not None]
            rows.sort(key=lambda loc: tuple(str(loc.get(k, "")) for k in keys))
            lost[t] = rows

    # Pins on affected variables that would be left matching nothing.
    pins_to_release = []
    pins = active_pins(db)
    for variable in affected_types:
        pin = pins.get(variable)
        if pin is None:
            continue
        pinned = set(pq.records_for_variant(db, variable, pin.selection))
        if pinned and pinned <= rid_set:
            pins_to_release.append(variable)

    fingerprint = hashlib.sha1(
        json.dumps([rids, dead_invs, dead_runs]).encode()
    ).hexdigest()[:16]
    plan = DeletePlan(
        targets=list(targets),
        record_ids=rids,
        by_variable=count(rids),
        seed_by_variable=count(seeds),
        downstream_by_variable=count(downstream),
        lost_locations=lost,
        invocation_ids=dead_invs,
        run_ids=dead_runs,
        pins_to_release=pins_to_release,
        fingerprint=fingerprint,
        warnings=warnings,
    )
    Log.info(
        f"delete_plan: {len(rids)} record(s) ({len(seeds)} named, "
        f"{len(downstream)} downstream) by variable={plan.by_variable}; "
        f"{len(dead_invs)} invocation(s), {len(dead_runs)} run(s) left empty; "
        f"locations lost={ {k: len(v) for k, v in lost.items()} }; "
        f"pins to release={pins_to_release}; fingerprint={fingerprint} "
        f"({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    for w in warnings:
        Log.warn(f"delete_plan: {w}")
    return plan


# ---------------------------------------------------------------------------
# Executing
# ---------------------------------------------------------------------------


def _delete_in(duck, table: str, column: str, ids) -> int:
    """``DELETE FROM table WHERE column IN ids`` in chunks; returns rows deleted
    (counted first, because DuckDB's DELETE does not return a count here)."""
    from scidb.provenance.query import _chunked_in

    ids = list(ids)
    if not ids:
        return 0
    n = sum(
        c
        for (c,) in _chunked_in(
            duck, f'SELECT COUNT(*) FROM "{table}" WHERE {column} IN ({{ph}})', ids
        )
    )
    for start in range(0, len(ids), 900):
        chunk = ids[start : start + 900]
        ph = ", ".join(["?"] * len(chunk))
        duck.execute(f'DELETE FROM "{table}" WHERE {column} IN ({ph})', chunk)
    return int(n)


def _remove_rows(duck, plan: DeletePlan, writer) -> dict:
    """Delete the plan's rows, table by table, each saved to the trash first
    (``writer.save``; a no-op writer makes it a hard delete). Returns
    ``{table: rows deleted}``. The caller holds the transaction.

    The order matters for the trash: each step's rows are saved AFTER the
    earlier steps deleted theirs, so every row lands in exactly one file."""
    from scidb.provenance.graph import GLUE_RECORD_TYPES
    from scidb.provenance.query import _chunked_in

    def move(table, column, ids):
        writer.save(table, column, ids)
        return _delete_in(duck, table, column, ids)

    types = {
        t
        for (t,) in _chunked_in(
            duck,
            "SELECT DISTINCT type FROM _record WHERE record_id IN ({ph})",
            plan.record_ids,
        )
    }

    deleted: dict = {}
    deleted["_invocation_output"] = move("_invocation_output", "output_record_id", plan.record_ids)
    deleted["_invocation_input"] = move(
        "_invocation_input", "input_record_id", plan.record_ids
    ) + move("_invocation_input", "invocation_id", plan.invocation_ids)
    deleted["_invocation_output"] += move("_invocation_output", "invocation_id", plan.invocation_ids)
    deleted["_run_invocation"] = move("_run_invocation", "invocation_id", plan.invocation_ids)
    deleted["_invocation_pooled"] = move("_invocation_pooled", "invocation_id", plan.invocation_ids)
    deleted["_invocation"] = move("_invocation", "invocation_id", plan.invocation_ids)
    deleted["_run_outputs"] = move("_run_outputs", "run_id", plan.run_ids)
    deleted["_run"] = move("_run", "run_id", plan.run_ids)
    for t in sorted(types):
        if t in GLUE_RECORD_TYPES:
            continue
        table = f"{t}_data"
        if duck.table_exists(table):
            deleted[table] = move(table, "record_id", plan.record_ids)
    deleted["_record_save"] = move("_record_save", "record_id", plan.record_ids)
    deleted["_record"] = move("_record", "record_id", plan.record_ids)
    return deleted


def delete_variant(
    db: "DatabaseManager",
    targets: list[dict],
    reason: str,
    *,
    expect_fingerprint: str | None = None,
) -> DeleteResult:
    """Delete what :func:`delete_plan` names, in one transaction; leaves a
    tombstone. The rows are saved to the trash first (``scidb.store.trash``),
    so :func:`restore_deleted` can undo it until the trash is purged. A
    database with no file has no trash, and its delete cannot be undone."""
    if not reason or not str(reason).strip():
        raise ValueError("delete_variant needs a reason; it goes on the tombstone")
    return _execute(db, targets, str(reason), expect_fingerprint=expect_fingerprint)


def _execute(
    db: "DatabaseManager",
    targets: list[dict],
    reason: str,
    *,
    expect_fingerprint: str | None,
    redo_of: str | None = None,
) -> DeleteResult:
    """The one delete path: a first delete, or (``redo_of``) the redo of a
    restored one, which reuses its tombstone instead of writing another."""
    from scidb.store import trash
    from scidb.store.variant_pins import forget_type_graph, release_pin

    duck = db._duck
    t0 = time.perf_counter()
    tombstone_id = redo_of or uuid.uuid4().hex[:16]
    root = trash.trash_root(db)
    writer = trash.TrashWriter(duck, root / tombstone_id if root is not None else None)
    if root is None:
        Log.warn(
            "delete_variant: this database has no file, so it has no trash; "
            "this delete cannot be undone"
        )
    label = "delete_again" if redo_of else "delete_variant"
    try:
        with duck.transaction(label):
            plan = delete_plan(db, targets)
            if expect_fingerprint is not None and plan.fingerprint != expect_fingerprint:
                raise ValueError(
                    f"{label}: the database changed since the plan was shown "
                    f"(fingerprint {expect_fingerprint} → {plan.fingerprint}); "
                    f"nothing was deleted. Re-plan and confirm again."
                )
            if not plan.record_ids:
                raise ValueError(f"{label}: the targets match no records")

            deleted = _remove_rows(duck, plan, writer)

            if redo_of is None:
                tomb = write_tombstone(
                    duck, reason, targets, plan.by_variable, plan.record_ids,
                    tombstone_id=tombstone_id,
                )
            else:
                tomb = next(t for t in tombstones(db) if t.tombstone_id == redo_of)
            released: list[str] = []
            released_pins: list[dict] = []
            for variable in plan.pins_to_release:
                pin = release_pin(db, variable, f"variant deleted (tombstone {tomb.tombstone_id})")
                if pin:
                    released.append(variable)
                    released_pins.append({"variable": variable, "selection": pin.selection})
            if writer.directory is not None:
                trash.write_state(
                    duck,
                    tombstone_id,
                    writer,
                    record_ids=plan.record_ids,
                    invocation_ids=plan.invocation_ids,
                    run_ids=plan.run_ids,
                    released_pins=released_pins,
                    fingerprint=plan.fingerprint,
                )
                tomb.trash_state = trash.STATE_TRASHED
    except Exception:
        writer.discard()
        Log.warn(f"{label}: failed; rolled back, nothing was deleted")
        raise
    forget_type_graph(duck)
    Log.info(
        f"{label}: tombstone {tomb.tombstone_id}: deleted rows={deleted}; "
        f"released pins={released}; trash={writer.directory} "
        f"({len(writer.files)} file(s), {writer.bytes} bytes); reason={reason!r} "
        f"({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    return DeleteResult(
        tombstone=tomb,
        deleted_rows=deleted,
        released_pins=released,
        plan=plan,
        undoable=writer.directory is not None,
    )


# ---------------------------------------------------------------------------
# Undo / redo
# ---------------------------------------------------------------------------


def _live(duck, table: str, column: str, ids) -> set:
    from scidb.provenance.query import _chunked_in

    ids = sorted({str(i) for i in ids if i is not None})
    if not ids:
        return set()
    return {
        v
        for (v,) in _chunked_in(
            duck, f'SELECT DISTINCT {column} FROM "{table}" WHERE {column} IN ({{ph}})', ids
        )
    }


def _restore_conflicts(db, e) -> list[str]:
    """Why restoring trash entry ``e`` would corrupt the live database; empty
    when it is safe. Reads only."""
    from scidb.store import trash

    duck = db._duck
    missing = trash.missing_files(e)
    if missing:
        return [f"{len(missing)} trash file(s) are missing, e.g. {missing[:3]}"]
    conflicts: list[str] = []
    for table in sorted({f["table"] for f in e.files}):
        if not duck.table_exists(table):
            conflicts.append(f"table {table} no longer exists")
    for table, column, ids, what in (
        ("_record", "record_id", e.record_ids, "record"),
        ("_invocation", "invocation_id", e.invocation_ids, "invocation"),
        ("_run", "run_id", e.run_ids, "run"),
    ):
        back = _live(duck, table, column, ids)
        if back:
            conflicts.append(
                f"{len(back)} {what}(s) exist again (re-run since the delete?), "
                f"e.g. {sorted(back)[:3]}"
            )
    # Edges must point at something: a restored row or a live one.
    restored_invs = set(e.invocation_ids)
    for table in ("_invocation_input", "_invocation_output", "_run_invocation", "_invocation_pooled"):
        wanted = trash.distinct_in_files(duck, e, table, "invocation_id") - restored_invs
        gone = wanted - _live(duck, "_invocation", "invocation_id", wanted)
        if gone:
            conflicts.append(
                f"{len(gone)} {table} row(s) belong to invocation(s) deleted since, "
                f"e.g. {sorted(gone)[:3]}"
            )
    wanted = trash.distinct_in_files(duck, e, "_invocation_input", "input_record_id") - set(e.record_ids)
    gone = wanted - _live(duck, "_record", "record_id", wanted)
    if gone:
        conflicts.append(
            f"{len(gone)} input record(s) of restored invocations were deleted since, "
            f"e.g. {sorted(gone)[:3]}"
        )
    restored_runs = set(e.run_ids)
    wanted = (
        trash.distinct_in_files(duck, e, "_run_invocation", "run_id")
        | trash.distinct_in_files(duck, e, "_run_outputs", "run_id")
    ) - restored_runs
    gone = wanted - _live(duck, "_run", "run_id", wanted)
    if gone:
        conflicts.append(f"{len(gone)} run(s) were deleted since, e.g. {sorted(gone)[:3]}")
    return conflicts


def restore_conflicts(db: "DatabaseManager", tombstone_id: str) -> list[str]:
    """Why :func:`restore_deleted` would refuse; empty when it would succeed.
    Read-only, for a caller that must check every precondition before writing
    anything (the GUI's undo)."""
    from scidb.store import trash

    e = trash.entry(db, tombstone_id)
    if e is None:
        return [f"delete {tombstone_id} has no trash (a hard delete, or the database was cleared)"]
    if e.state != trash.STATE_TRASHED:
        return [f"delete {tombstone_id}: its trash is {e.state}"]
    return _restore_conflicts(db, e)


def redo_conflicts(db: "DatabaseManager", tombstone_id: str) -> list[str]:
    """Why :func:`delete_again` would refuse; empty when it would succeed.
    Read-only."""
    from scidb.store import trash

    e = trash.entry(db, tombstone_id)
    if e is None or e.state != trash.STATE_RESTORED:
        return [f"delete {tombstone_id}: its trash is {e.state if e else 'absent'}, not restored"]
    try:
        plan = delete_plan(db, [{"record_ids": e.record_ids}])
    except ValueError as exc:
        return [f"delete {tombstone_id}: {exc}"]
    if plan.fingerprint != e.fingerprint:
        return [
            f"delete {tombstone_id}: the data changed since it was restored "
            f"(something was computed from it); redoing would delete more than was planned"
        ]
    return []


def restore_deleted(db: "DatabaseManager", tombstone_id: str) -> RestoreResult:
    """Undo a delete: put every trashed row back, re-activate the pins it
    released, and mark it ``restored``. All or nothing: any conflict (a re-run
    wrote the same records, something it pointed at was deleted since, a file
    is missing) raises ``trash.TrashConflict`` and writes nothing."""
    from scidb.store import trash
    from scidb.store.variant_pins import active_pins, forget_type_graph, pin_variant

    duck = db._duck
    t0 = time.perf_counter()
    e = trash.entry(db, tombstone_id)
    if e is None:
        raise trash.TrashConflict(
            f"restore {tombstone_id}: not undoable",
            ["no trash for this delete (a hard delete, a cleared database, or an unknown id)"],
        )
    if e.state != trash.STATE_TRASHED:
        raise trash.TrashConflict(
            f"restore {tombstone_id}: not undoable", [f"its trash is {e.state}"]
        )
    with duck.transaction("restore_deleted"):
        conflicts = _restore_conflicts(db, e)
        if conflicts:
            Log.warn(f"restore_deleted {tombstone_id}: refused: {conflicts}")
            raise trash.TrashConflict(
                f"restore {tombstone_id}: refused, nothing was restored", conflicts
            )
        restored = trash.insert_files(duck, e)
        forget_type_graph(duck)
        repinned: list[str] = []
        warnings: list[str] = []
        active = active_pins(db)
        for p in e.released_pins:
            variable = p["variable"]
            if variable in active:
                warnings.append(f"{variable}: another pin is active now; the released pin stays released")
                continue
            try:
                pin_variant(
                    db, variable, p["selection"], f"restored: undo of delete (tombstone {tombstone_id})"
                )
                repinned.append(variable)
            except ValueError as exc:
                warnings.append(f"{variable}: pin not restored: {exc}")
        trash.set_state(duck, tombstone_id, trash.STATE_RESTORED)
    trash.remove_dir(e)
    Log.info(
        f"restore_deleted {tombstone_id}: restored rows={restored}; repinned={repinned}; "
        f"warnings={warnings} ({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    for w in warnings:
        Log.warn(f"restore_deleted {tombstone_id}: {w}")
    return RestoreResult(
        tombstone_id=tombstone_id, restored_rows=restored, repinned=repinned, warnings=warnings
    )


def delete_again(db: "DatabaseManager", tombstone_id: str) -> DeleteResult:
    """Redo a restored delete: the same rows, trashed again under the same
    tombstone. Refused (``trash.TrashConflict``, nothing written) unless the
    plan is exactly what it was, so a record computed from the restored data
    since is never deleted by a redo the user did not see planned."""
    from scidb.store import trash

    e = trash.entry(db, tombstone_id)
    if e is None or e.state != trash.STATE_RESTORED:
        raise trash.TrashConflict(
            f"redo {tombstone_id}: not redoable",
            [f"its trash is {e.state if e else 'absent'}; only a restored delete can be redone"],
        )
    reason = next(t.reason for t in tombstones(db) if t.tombstone_id == tombstone_id)
    try:
        return _execute(
            db,
            [{"record_ids": e.record_ids}],
            reason,
            expect_fingerprint=e.fingerprint,
            redo_of=tombstone_id,
        )
    except trash.TrashConflict:
        raise
    except ValueError as exc:
        raise trash.TrashConflict(f"redo {tombstone_id}: refused, nothing was deleted", [str(exc)]) from exc
