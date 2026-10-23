"""
Results across a rename: re-record a function's results under its new name
without computing anything (``.claude/plan-library-fork-chain.md`` R2, O16).

Share as library and Make my own copy rename functions (``filter_emg`` ->
``emg_alice.filter_emg``, ``emg_alice.filter_emg`` -> ``emg_bob.filter_emg``).
The recorded name is identity -- it is in every record id (``__fn``) and in
every invocation id (``provenance.compute_invocation_id``) -- so without this
every renamed step would have to run again to have results under its new
name, although its code did not change.

**The rule.** An invocation of an OLD name is carried when the function now
recorded under the NEW name has the SAME code hash
(``scilineage.hashing`` / MATLAB's own source hash: the caller passes the
current hash, see :func:`rename_functions`) and every one of its variable
inputs is either untouched or was itself carried in this pass. Carrying
writes, through the owners the save path uses:

* a new invocation, id from ``provenance.compute_invocation_id`` with the NEW
  name and the (re-mapped) inputs -- exactly the id the next run of the new
  name predicts, so its skip gate and node state find it;
* one new record per output, holding the SAME data (a copy of the data-table
  rows), with its own content-addressed id
  (``scicanonicalhash.generate_record_id``) and a save event NOW, so it is
  the newest at its location;
* the input / output edges and one ``_run`` row (origin ``replay``: it
  reproduces recorded invocations, nothing ran).

Old records and invocations are kept (history is never rewritten). Downstream
renamed functions are processed after their upstream ones and consume the
carried records. Anything not carried is reported with the reason, never
silently skipped. Idempotent: a second pass finds every new invocation
present and writes nothing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class RenameReport:
    #: ``{new name: records re-recorded}`` for every name carried.
    carried: dict = field(default_factory=dict)
    #: ``[{"function": new name, "reason": why}]``.
    not_carried: list = field(default_factory=list)
    invocations: int = 0
    #: The ``_run`` row(s) the pass wrote (one per new name), comma-joined.
    run_id: "str | None" = None

    def to_dict(self) -> dict:
        return asdict(self)


def _old_invocations(duck, names) -> list[tuple]:
    if not names:
        return []
    ph = ", ".join("?" for _ in names)
    return duck._fetchall(
        "SELECT invocation_id, function_name, function_hash, as_table, distribute, "
        "for_columns, across_variants, iteration_level FROM _invocation "
        f"WHERE function_name IN ({ph}) ORDER BY invocation_id",
        list(names),
    )


def _inputs(duck, inv_id: str) -> list[tuple]:
    return duck._fetchall(
        "SELECT ii.param_name, ii.input_record_id, ii.selector, ii.declared_name, r.type "
        "FROM _invocation_input ii LEFT JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE ii.invocation_id = ? ORDER BY ii.param_name, ii.input_record_id",
        [inv_id],
    )


def _outputs(duck, inv_id: str) -> list[tuple]:
    return duck._fetchall(
        "SELECT io.output_num, io.output_record_id, r.type, r.schema_id, r.content_hash, "
        "r.schema_version, COALESCE(r.excluded, FALSE) FROM _invocation_output io "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "WHERE io.invocation_id = ? ORDER BY io.output_num",
        [inv_id],
    )


def _schema_keys_of(db, schema_id) -> dict:
    if schema_id is None:
        return {}
    keys = list(db.dataset_schema_keys)
    cols = ", ".join(f'"{k}"' for k in keys)
    row = db._duck._fetchone(f"SELECT {cols} FROM _schema WHERE schema_id = ?", [schema_id])
    return {k: v for k, v in zip(keys, row or ()) if v is not None}


def _copy_data_rows(duck, type_name: str, old_rid: str, new_rid: str) -> int:
    """Copy *old_rid*'s rows of ``<type>_data`` to *new_rid* (all of them: a
    custom codec stores several rows per record). ``0`` when the record has
    no data row (a ``generates_file`` output) or *new_rid* already has some."""
    table = f"{type_name}_data"
    if not duck._table_exists(table):
        return 0
    if duck._fetchone(f'SELECT 1 FROM "{table}" WHERE record_id = ? LIMIT 1', [new_rid]):
        return 0
    cols = [
        r[0]
        for r in duck._fetchall(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? "
            "AND column_name <> 'record_id' ORDER BY ordinal_position",
            [table],
        )
    ]
    quoted = ", ".join(f'"{c}"' for c in cols)
    duck._execute(
        f'INSERT INTO "{table}" (record_id{", " if cols else ""}{quoted}) '
        f'SELECT ?{", " if cols else ""}{quoted} FROM "{table}" WHERE record_id = ?',
        [new_rid, old_rid],
    )
    row = duck._fetchone(f'SELECT COUNT(*) FROM "{table}" WHERE record_id = ?', [new_rid])
    return int(row[0]) if row else 0


def _write(db, records, invocation_rows, input_rows, output_rows, new_inv_of, report) -> None:
    """Write a pass in ONE transaction. Each new record's save event is NOW,
    in the order its original was saved (a microsecond apart), so the new
    records are the newest at their locations and keep the originals'
    relative order -- an older result never becomes "latest" over a newer
    one."""
    from .intent import ORIGIN_REPLAY
    from .provenance import generate_run_id, insert_record_entities

    duck = db._duck
    saved_at = {}
    for old_rid, *_ in records:
        row = duck._fetchone("SELECT max(timestamp) FROM _record_save WHERE record_id = ?", [old_rid])
        saved_at[old_rid] = (row[0] if row else None) or ""
    start = datetime.now()
    order = sorted(records, key=lambda r: (saved_at[r[0]], r[0]))
    stamp = {r[1]: (start + timedelta(microseconds=i)).isoformat() for i, r in enumerate(order)}
    runs = {name: generate_run_id() for name in sorted(set(new_inv_of.values()))}
    duck._begin()
    try:
        for old_rid, new_rid, type_name, *_ in records:
            _copy_data_rows(duck, type_name, old_rid, new_rid)
        insert_record_entities(
            duck,
            [(new, stamp[new], t, sid, ch, sv, ex) for _old, new, t, sid, ch, sv, ex in records],
        )
        duck._bulk_insert("_record_save", ["record_id", "timestamp"],
                          [(r[1], stamp[r[1]]) for r in records],
                          conflict_cols=["record_id", "timestamp"])
        for row in invocation_rows:
            duck._execute(
                "INSERT INTO _invocation (invocation_id, function_name, function_hash, as_table, "
                "distribute, for_columns, across_variants, iteration_level) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                list(row),
            )
        for row in input_rows:
            duck._execute(
                "INSERT INTO _invocation_input (invocation_id, param_name, input_record_id, "
                "selector, declared_name) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                list(row),
            )
        for row in output_rows:
            duck._execute(
                "INSERT INTO _invocation_output (invocation_id, output_num, output_record_id) "
                "VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                list(row),
            )
        now = start.isoformat()
        for name, run_id in runs.items():
            duck._execute(
                "INSERT INTO _run (run_id, timestamp, function_name, where_clause, origin) "
                "VALUES (?, ?, ?, ?, ?)",
                [run_id, now, name, None, ORIGIN_REPLAY],
            )
        for inv, name in new_inv_of.items():
            duck._execute(
                "INSERT INTO _run_invocation (run_id, invocation_id) VALUES (?, ?) "
                "ON CONFLICT DO NOTHING",
                [runs[name], inv],
            )
        duck._commit()
    except Exception:
        try:
            duck._execute("ROLLBACK")
        except Exception:
            pass
        raise
    report.run_id = ", ".join(runs.values())
    logger.info(
        "[rename_functions] wrote %d invocation(s), %d record(s), %d input edge(s) "
        "in run(s) %s", len(invocation_rows), len(records), len(input_rows), runs,
    )


def rename_functions(db, mapping: "dict[str, str]", hashes: "dict[str, str | None]") -> RenameReport:
    """Carry the results of every OLD name in *mapping* (``{old: new}``) to
    its new name where the code did not change.

    *hashes* is ``{new name: the hash the save path records for it now}``
    (``scidb.foreach_config.function_hash_for`` of the live callable; for
    MATLAB the bridge's source hash). A missing / ``None`` hash means the
    caller could not resolve the function: nothing of it is carried, and the
    report says so. Returns a :class:`RenameReport`; every new name is in
    exactly one of ``carried`` / ``not_carried``.
    """
    from scicanonicalhash import generate_record_id

    from .bindings import Binding
    from .provenance import compute_invocation_id

    duck = db._duck
    mapping = {o: n for o, n in mapping.items() if o and n and o != n}
    report = RenameReport()
    if not mapping:
        return report
    rows = _old_invocations(duck, sorted(mapping))
    by_name: dict[str, list] = {}
    for row in rows:
        by_name.setdefault(row[1], []).append(row)

    # Which old invocation wrote each record: to order the pass (upstream
    # first) and to know when an input is a renamed function's output.
    producer: dict[str, str] = {}
    for inv in rows:
        for out in _outputs(duck, inv[0]):
            producer[out[1]] = inv[0]

    reasons: dict[str, str] = {}
    candidates = []
    for old, new in sorted(mapping.items()):
        current = hashes.get(new)
        invs = by_name.get(old, [])
        if not invs:
            reasons[new] = f"no results recorded under {old}"
            continue
        if not current:
            reasons[new] = f"the code of {new} could not be hashed here, so it cannot be compared"
            continue
        same = [inv for inv in invs if inv[2] == current]
        if not same:
            stored = sorted({inv[2] for inv in invs})
            reasons[new] = (
                f"its code changed: {old} was recorded with hash(es) {stored}, "
                f"{new} hashes {current}; it needs a run"
            )
            continue
        candidates += same
        logger.info(
            "[rename_functions] %s -> %s: hash %s unchanged, %d of %d recorded "
            "invocation(s) to carry",
            old, new, current, len(same), len(invs),
        )

    rid_map: dict[str, str] = {}
    carried_invs: set[str] = set()
    pending = {inv[0]: inv for inv in candidates}
    blocked: dict[str, str] = {}
    counts: dict[str, int] = {}
    # Everything is collected first and written in ONE transaction below.
    records: list[tuple] = []  # (old rid, new rid, type, schema_id, content_hash, sv, excluded)
    invocation_rows: list[tuple] = []
    input_rows: list[tuple] = []
    output_rows: list[tuple] = []
    new_inv_of: dict[str, str] = {}  # new invocation -> new name
    while pending:
        progressed = False
        for inv_id, inv in sorted(pending.items()):
            ins = _inputs(duck, inv_id)
            waiting = [
                rid for _p, rid, _s, _d, _t in ins
                if rid in producer and producer[rid] in pending and producer[rid] != inv_id
            ]
            if waiting:
                continue
            progressed = True
            del pending[inv_id]
            _, old, fn_hash, as_table, distribute, for_columns, across, level = inv
            new = mapping[old]
            # An input written by an old invocation that was NOT carried
            # (changed code, or itself blocked) cannot feed a carried result:
            # the new upstream has no such record.
            stale = [
                rid for _p, rid, _s, _d, _t in ins
                if rid in producer and producer[rid] not in carried_invs and rid not in rid_map
            ]
            if stale:
                blocked[inv_id] = new
                logger.info(
                    "[rename_functions] %s: invocation %s not carried, input(s) %s come "
                    "from a step that was not carried", new, inv_id, stale[:3],
                )
                continue
            edges = [Binding(p, rid_map.get(rid, rid), sel) for p, rid, sel, _d, _t in ins]
            new_inv = compute_invocation_id(
                fn_hash,
                list(as_table or []),
                bool(distribute),
                edges,
                across_variants=list(across or []),
                iteration_level=None if level is None else list(level),
                function_name=new,
            )
            outs = _outputs(duck, inv_id)
            exists = db.invocation_exists(new_inv)
            for num, rid, type_name, schema_id, content_hash, sv, excluded in outs:
                nested = {
                    "schema": _schema_keys_of(db, schema_id),
                    "version": {"__fn": new, "__invocation_id": new_inv,
                                "__output_num": num, "__carried_from": rid},
                }
                new_rid = generate_record_id(
                    class_name=type_name, schema_version=sv, content_hash=content_hash,
                    metadata=nested,
                )
                rid_map[rid] = new_rid
                if not exists:
                    records.append((rid, new_rid, type_name, schema_id, content_hash, sv,
                                    bool(excluded)))
                    output_rows.append((new_inv, num, new_rid))
            carried_invs.add(inv_id)
            counts[new] = counts.get(new, 0) + len(outs)
            if exists:
                logger.info("[rename_functions] %s: invocation %s already carried (from %s)",
                            new, new_inv, inv_id)
                continue
            new_inv_of[new_inv] = new
            invocation_rows.append(
                (new_inv, new, fn_hash, as_table, bool(distribute), for_columns, across, level)
            )
            input_rows += [
                (new_inv, p, edge.rid, sel, declared)
                for (p, _rid, sel, declared, _t), edge in zip(ins, edges)
            ]
            report.invocations += 1
            logger.info(
                "[rename_functions] %s: invocation %s -> %s, %d record(s) re-recorded, "
                "%d input(s) re-mapped",
                new, inv_id, new_inv, len(outs),
                sum(1 for e, i in zip(edges, ins) if e.rid != i[1]),
            )
        if not progressed:  # provenance has no cycles; never loop forever
            for inv_id, inv in pending.items():
                blocked[inv_id] = mapping[inv[1]]
            break

    # The rename itself is a fact, carried or not: from now on the old
    # name's earlier records are history, never "current" (they would be a
    # second variant beside the new name's -- an ambiguous load).
    renamed_at = datetime.now().isoformat()
    for old, new in sorted(mapping.items()):
        duck._execute(
            "INSERT INTO _function_rename (old_name, new_name, renamed_at) VALUES (?, ?, ?) "
            "ON CONFLICT DO NOTHING",
            [old, new, renamed_at],
        )
    if invocation_rows:
        _write(db, records, invocation_rows, input_rows, output_rows, new_inv_of, report)

    for new in sorted(set(blocked.values())):
        if new not in counts:
            reasons.setdefault(
                new, "an input it consumed was written by a step that was not carried; it needs a run"
            )
    for old, new in sorted(mapping.items()):
        if new in counts:
            report.carried[new] = counts[new]
        else:
            report.not_carried.append(
                {"function": new, "reason": reasons.get(new, f"nothing of {old} could be carried")}
            )
    logger.info(
        "[rename_functions] %s: carried %s, not carried %s (run %s)",
        json.dumps(mapping, sort_keys=True), report.carried,
        [(x["function"], x["reason"]) for x in report.not_carried], report.run_id,
    )
    return report
