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

from .log import Log

if TYPE_CHECKING:
    from .database import DatabaseManager

TOMBSTONE_TABLE = "_variant_tombstone"


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
    deleted_by: str | None
    reason: str
    targets: list[dict]
    by_variable: dict[str, int]
    record_ids: list[str]


@dataclass
class DeleteResult:
    tombstone: Tombstone
    #: ``{table: rows deleted}``.
    deleted_rows: dict[str, int]
    released_pins: list[str]
    plan: DeletePlan


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def ensure_tombstone_table(duck) -> None:
    duck._execute(f"""
        CREATE TABLE IF NOT EXISTS {TOMBSTONE_TABLE} (
            tombstone_id    VARCHAR PRIMARY KEY,
            deleted_at      VARCHAR NOT NULL,
            deleted_by      VARCHAR,
            reason          VARCHAR NOT NULL,
            targets_json    VARCHAR NOT NULL,
            counts_json     VARCHAR NOT NULL,
            record_ids_json VARCHAR NOT NULL
        )
    """)


def tombstones(db: "DatabaseManager", variable: str | None = None) -> list[Tombstone]:
    """Every deletion ever made, oldest first (only those that touched
    ``variable`` when given)."""
    duck = db._duck
    if not duck._table_exists(TOMBSTONE_TABLE):
        return []
    rows = duck._fetchall(
        "SELECT tombstone_id, deleted_at, deleted_by, reason, targets_json, "
        f"counts_json, record_ids_json FROM {TOMBSTONE_TABLE} ORDER BY deleted_at"
    )
    out = []
    for tid, at, by, reason, targets, counts, rids in rows:
        t = Tombstone(
            tombstone_id=tid,
            deleted_at=at,
            deleted_by=by,
            reason=reason,
            targets=json.loads(targets),
            by_variable=json.loads(counts),
            record_ids=json.loads(rids),
        )
        if variable is None or variable in t.by_variable:
            out.append(t)
    return out


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _same_value(a, b) -> bool:
    from .bindings import variant_signature

    return variant_signature({"v": a}) == variant_signature({"v": b})


def _seed_records(db, targets: list[dict]) -> tuple[set, list[str]]:
    """Resolve the targets to record ids. Returns ``(rids, warnings)``."""
    from . import provenance_query as pq
    from .inspect.variant_cards import build_variant_cards, selection_matches
    from .provenance import CONSTANT_TYPE
    from .variant import normalize_selection

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
        elif "function" in target or "parameter" in target:
            if "function" in target:
                rows = duck._fetchall(
                    "SELECT ii.invocation_id, c.value_repr FROM _invocation_input ii "
                    "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
                    "JOIN _constant c ON c.record_id = ii.input_record_id "
                    "WHERE inv.function_name = ? AND ii.param_name = ?",
                    [target["function"], target["param"]],
                )
                label = f"{target['function']}.{target['param']}"
            else:
                # Which Parameter an edge belongs to is
                # `parameter.parameter_node_name`'s rule (the recorded declared
                # name, else the argument it filled), the same identity the
                # canvas keys Parameter nodes by. SQL narrows the candidates;
                # the owner decides.
                from .parameter import parameter_node_name

                name = target["parameter"]
                candidates = duck._fetchall(
                    "SELECT ii.invocation_id, c.value_repr, ii.param_name, "
                    "ii.declared_name FROM _invocation_input ii "
                    "JOIN _constant c ON c.record_id = ii.input_record_id "
                    "JOIN _record r ON r.record_id = ii.input_record_id "
                    "WHERE r.type = ? AND (ii.declared_name = ? OR "
                    "(ii.declared_name IS NULL AND ii.param_name = ?))",
                    [CONSTANT_TYPE, name, name],
                )
                rows = [
                    (inv, value_repr)
                    for inv, value_repr, param, declared in candidates
                    if parameter_node_name(param, {param: declared} if declared else None)
                    == name
                ]
                label = f"Parameter {name}"
            invs = sorted(
                {
                    inv
                    for inv, value_repr in rows
                    if _same_value(pq._safe_literal(value_repr), target["value"])
                }
            )
            if not invs:
                warnings.append(f"{label}={target['value']!r}: no invocation used it")
                continue
            for _inv, _num, out_rid in pq._chunked_in(
                duck,
                "SELECT invocation_id, output_num, output_record_id "
                "FROM _invocation_output WHERE invocation_id IN ({ph})",
                invs,
            ):
                seeds.add(out_rid)
        else:
            raise ValueError(f"delete: unknown target shape {target!r}")
    return seeds, warnings


def _downstream(duck, seeds: set) -> set:
    """Every record computed, at any depth, from ``seeds`` (seeds excluded)."""
    from .provenance_query import _chunked_in

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
    from . import provenance_query as pq
    from .provenance import GLUE_TYPE
    from .variant_pins import active_pins

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
            if t and t != GLUE_TYPE:
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
    affected_types = sorted({t for t in types.values() if t != GLUE_TYPE})
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
            if t and t != GLUE_TYPE and sids.get(rid) not in kept.get(t, set()):
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
    from .provenance_query import _chunked_in

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
        duck._execute(f'DELETE FROM "{table}" WHERE {column} IN ({ph})', chunk)
    return int(n)


def delete_variant(
    db: "DatabaseManager",
    targets: list[dict],
    reason: str,
    *,
    expect_fingerprint: str | None = None,
) -> DeleteResult:
    """Delete what :func:`delete_plan` names, in one transaction. Cannot be
    undone; leaves a tombstone."""
    from .database import get_user_id
    from .provenance import GLUE_TYPE
    from .variant_pins import release_pin

    if not reason or not str(reason).strip():
        raise ValueError("delete_variant needs a reason; it goes on the tombstone")
    duck = db._duck
    t0 = time.perf_counter()
    duck._begin()
    try:
        plan = delete_plan(db, targets)
        if expect_fingerprint is not None and plan.fingerprint != expect_fingerprint:
            raise ValueError(
                f"delete_variant: the database changed since the plan was shown "
                f"(fingerprint {expect_fingerprint} → {plan.fingerprint}); "
                f"nothing was deleted. Re-plan and confirm again."
            )
        if not plan.record_ids:
            raise ValueError("delete_variant: the targets match no records")

        from .provenance_query import _chunked_in

        types = {
            t
            for (t,) in _chunked_in(
                duck,
                "SELECT DISTINCT type FROM _record WHERE record_id IN ({ph})",
                plan.record_ids,
            )
        }

        deleted: dict = {}
        deleted["_invocation_output"] = _delete_in(
            duck, "_invocation_output", "output_record_id", plan.record_ids
        )
        deleted["_invocation_input"] = _delete_in(
            duck, "_invocation_input", "input_record_id", plan.record_ids
        ) + _delete_in(duck, "_invocation_input", "invocation_id", plan.invocation_ids)
        deleted["_invocation_output"] += _delete_in(
            duck, "_invocation_output", "invocation_id", plan.invocation_ids
        )
        deleted["_run_invocation"] = _delete_in(
            duck, "_run_invocation", "invocation_id", plan.invocation_ids
        )
        deleted["_invocation"] = _delete_in(
            duck, "_invocation", "invocation_id", plan.invocation_ids
        )
        deleted["_run"] = _delete_in(duck, "_run", "run_id", plan.run_ids)
        for t in sorted(types):
            if t == GLUE_TYPE:
                continue
            table = f"{t}_data"
            if duck._table_exists(table):
                deleted[table] = _delete_in(duck, table, "record_id", plan.record_ids)
        deleted["_record_save"] = _delete_in(
            duck, "_record_save", "record_id", plan.record_ids
        )
        deleted["_record"] = _delete_in(duck, "_record", "record_id", plan.record_ids)

        tomb = Tombstone(
            tombstone_id=uuid.uuid4().hex[:16],
            deleted_at=datetime.now().isoformat(),
            deleted_by=get_user_id(),
            reason=str(reason),
            targets=list(targets),
            by_variable=plan.by_variable,
            record_ids=plan.record_ids,
        )
        duck._execute(
            f"INSERT INTO {TOMBSTONE_TABLE} (tombstone_id, deleted_at, deleted_by, "
            "reason, targets_json, counts_json, record_ids_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                tomb.tombstone_id,
                tomb.deleted_at,
                tomb.deleted_by,
                tomb.reason,
                json.dumps(tomb.targets, sort_keys=True, default=str),
                json.dumps(tomb.by_variable, sort_keys=True),
                json.dumps(tomb.record_ids),
            ],
        )
        released = []
        for variable in plan.pins_to_release:
            if release_pin(db, variable, f"variant deleted (tombstone {tomb.tombstone_id})"):
                released.append(variable)
        duck._commit()
    except Exception:
        Log.warn("delete_variant: failed; rolling back, nothing was deleted")
        try:
            duck._rollback()
        except Exception:  # pragma: no cover - rollback of a dead txn
            pass
        raise
    Log.info(
        f"delete_variant: tombstone {tomb.tombstone_id}: deleted rows={deleted}; "
        f"released pins={released}; reason={reason!r} "
        f"({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    return DeleteResult(tombstone=tomb, deleted_rows=deleted, released_pins=released, plan=plan)
