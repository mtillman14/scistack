"""The data view: what data do I have, and where did my subjects go? (GitHub #28)

The schema location picker in status mode shows one narrow column per
variable, in pipeline order, after an "any data" column. This module is what
it reads; it computes nothing of its own:

* each column is ``scidb.provenance.status.location_status`` (the one owner
  of per-location state), keyed by :func:`location_key`;
* "any data" is the inventory's present set (``scidb.provenance.inventory``);
* the footer is each column's counts, and the **drop-off** from column to
  column at the first schema key (``40 → 38 → 31`` subjects), each loss with
  the state that explains it there.

It replaces Inventory, Coverage, View Schema Locations and Expected
locations in the GUI (those stay in scidb and the CLI: hidden, not deleted).
"""

from __future__ import annotations

import time
from collections import Counter
from typing import TYPE_CHECKING

from scidb.model.log import Log
from scidb.provenance import status as st

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

#: Lost values listed per drop-off step.
LOSS_LIST_MAX = 50


def location_key(location: dict, keys: list[str]) -> str:
    """THE text key of a location at *keys*: ``subject=01|trial=3``. The GUI
    builds the same string for a row's projection onto a column's keys."""
    return "|".join(f"{k}={location[k]}" for k in keys if location.get(k) is not None)


def data_view(
    db: "DatabaseManager",
    variables=None,
    *,
    fn_registry: dict | None = None,
    variants: dict | None = None,
) -> dict:
    """Every column of the data view, its footer and the drop-off.

    *variables*: names or classes (default: every variable, pipeline order).
    *variants*: ``{variable: branch_params_filter}``, a column's chosen variant.
    """
    from scidb.provenance.query import variable_type_edges, variable_types_in_pipeline_order

    t0 = time.perf_counter()
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    order = variable_types_in_pipeline_order(duck, variable_type_edges(duck))
    if variables is not None:
        wanted = [getattr(v, "__name__", v) for v in variables]
        order = [v for v in order if v in wanted] + [v for v in wanted if v not in order]
    columns = []
    timings: dict[str, float] = {}
    for name in order:
        t1 = time.perf_counter()
        try:
            vs = st.location_status(db, name, variant=(variants or {}).get(name), fn_registry=fn_registry)
        except Exception as e:  # one column must never hide the others
            Log.warn(f"[data_view] {name}: {type(e).__name__}: {e}")
            columns.append({"variable": name, "error": f"{type(e).__name__}: {e}"})
            continue
        timings[name] = time.perf_counter() - t1
        columns.append(_column(vs))
    any_data = _any_data(db, schema_keys)
    dropoff = _dropoff(columns, schema_keys)
    Log.info(
        f"[data_view] {len(columns)} column(s), {len(any_data)} location(s) with any data, "
        f"drop-off {[d['count'] for d in dropoff]}"
    )
    Log.timings("data_view", timings, extra=f"{len(columns)} column(s)", total=time.perf_counter() - t0)
    return {
        "schema_keys": schema_keys,
        "glyphs": dict(st.GLYPHS),
        "labels": dict(st.LABELS),
        "any_data": any_data,
        "columns": columns,
        "dropoff": dropoff,
    }


def _column(vs: "st.VariableStatus") -> dict:
    cells = {}
    for loc in vs.locations:
        cells[location_key(loc.location, vs.keys)] = {
            "state": loc.state,
            "glyph": loc.glyph,
            "detail": loc.detail,
            "record_ids": loc.record_ids[:5],
        }
    counts = vs.counts
    counted = len(vs.locations) - counts[st.EXCLUDED]
    present = counts[st.PRESENT] + counts[st.WARNING] + counts[st.OUTDATED]
    return {
        "variable": vs.variable,
        "function_name": vs.function_name,
        "keys": list(vs.keys),
        "resolved": vs.resolved,
        "cells": cells,
        "counts": counts,
        "footer": f"{present}/{counted}" if vs.resolved else "—",
        "present": present,
        "counted": counted,
        "unchecked": vs.unchecked,
        "notes": list(vs.notes),
    }


def _any_data(db, schema_keys: list[str]) -> list[str]:
    """Location keys (every schema key a row has) where anything is saved."""
    out = []
    for combo in db.distinct_schema_combinations(schema_keys) if schema_keys else []:
        loc = {k: str(v) for k, v in zip(schema_keys, combo) if v is not None}
        out.append(location_key(loc, schema_keys))
    return out


def _dropoff(columns: list[dict], schema_keys: list[str]) -> list[dict]:
    """Per column, how many values of the first schema key it has data at,
    and the values lost since the column before, each with the state that
    explains the loss in this column."""
    if not schema_keys:
        return []
    top = schema_keys[0]
    out = []
    prev: set | None = None
    for col in columns:
        if col.get("error") or not col.get("resolved") or top not in col.get("keys", []):
            continue
        have: set = set()
        why: dict[str, Counter] = {}
        for key, cell in col["cells"].items():
            value = dict(part.split("=", 1) for part in key.split("|") if "=" in part).get(top)
            if value is None:
                continue
            if cell["state"] in (st.PRESENT, st.WARNING, st.OUTDATED):
                have.add(value)
            else:
                why.setdefault(value, Counter())[cell["state"]] += 1
        lost = sorted(prev - have) if prev is not None else []
        out.append({
            "variable": col["variable"],
            "key": top,
            "count": len(have),
            "lost": [
                {
                    "value": v,
                    "state": (why.get(v) or Counter({st.NOT_RUN: 1})).most_common(1)[0][0],
                }
                for v in lost[:LOSS_LIST_MAX]
            ],
            "lost_count": len(lost),
        })
        prev = have
    return out


def cell_detail(db: "DatabaseManager", variable: str, record_ids: list[str]) -> dict:
    """What made the record(s) in one data-view cell: "made by f v3 with
    low_hz=20 from RawEMG@subject=S03/trial=T2", when, and its findings.
    Queries only (the value preview is ``inspect.preview``'s)."""
    from scidb.provenance import findings as fnd
    from scidb.provenance import query as pq

    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    rids = [r for r in record_ids if r][:5]
    produced = pq.producing_invocation_batch(duck, rids)
    inv_ids = sorted({v[0] for v in produced.values()})
    edges = pq.invocation_edges_batch(duck, inv_ids) if inv_ids else {}
    fns = {v[1] for v in produced.values()}
    ordinals = pq.code_version_ordinals(duck, fns) if fns else {}
    saved = pq.saved_at_batch(duck, rids)
    found = fnd.findings_for_records(db, rids)
    in_rids = sorted({v["record_id"] for e in edges.values() for v in e.get("var_inputs", [])})
    in_sids = dict(pq._chunked_in(
        duck, "SELECT record_id, schema_id FROM _record WHERE record_id IN ({ph})", in_rids
    )) if in_rids else {}
    in_locs = pq._schema_locations(duck, set(in_sids.values()), schema_keys)
    out = []
    for rid in rids:
        inv = produced.get(rid)
        if inv is None:
            out.append({"record_id": rid, "made_by": f"{variable} was saved by hand or loaded raw",
                        "saved_at": saved.get(rid), "findings": found.get(rid, []), "checked": False})
            continue
        inv_id, fn_name, fn_hash = inv
        e = edges.get(inv_id) or {}
        version = (ordinals.get(fn_name) or {}).get(fn_hash) or ""
        consts = ", ".join(f"{k}={v!r}" for k, v in sorted((e.get("constants") or {}).items()))
        froms = []
        for v in e.get("var_inputs", []):
            loc = in_locs.get(in_sids.get(v["record_id"]), {})
            where = "/".join(f"{k}={val}" for k, val in loc.items() if val is not None)
            froms.append(f"{v['variable_type']}@{where}" if where else v["variable_type"])
        text = f"made by {fn_name}" + (f" {version}" if version else "")
        if consts:
            text += f" with {consts}"
        if froms:
            text += " from " + ", ".join(froms[:6]) + (" …" if len(froms) > 6 else "")
        out.append({
            "record_id": rid,
            "function_name": fn_name,
            "version": version or None,
            "constants": {k: repr(v) for k, v in (e.get("constants") or {}).items()},
            "inputs": froms,
            "made_by": text,
            "saved_at": saved.get(rid),
            "findings": found.get(rid, []),
            "checked": True,
        })
    Log.debug(f"[data_view] cell_detail({variable}, {len(rids)} record(s))")
    return {"variable": variable, "records": out}
