"""Variant pins: the DEFAULT variant of a variable.

``.claude/plan-variants-popup.md`` Stage 2; the rules are in
``docs/claude/variant-pins-and-deletion.md`` §2. In short:

* A pin says *which variant an unnamed load gets*. It hides nothing. A load
  that names a variant (``Variant(X, ...)``, Plot Studio variant rows) still
  reaches every other one, and ``AcrossVariants`` still pools them all. That
  is why a pin is NOT ``_record.excluded``: that flag removes records from
  every load, named or not.
* A pin stores a variant card's ``selection``: canonical coordinate keys
  (``fn.param``, ``__code__.fn``, ``__run__.fn``). A downstream record's
  coordinate holds its upstream keys, so a pin on an upstream variable narrows
  downstream defaults through the same filter, with no extra machinery
  (:func:`effective_default`).
* Pins on several variables of one chain merge per key, and the pin NEAREST to
  the loaded variable wins (user decision D5).
* Gaps are strict: where the pinned variant does not exist, the default load
  returns nothing (D3).

**One owner.** Every surface that resolves "the default" calls
:func:`default_load_args` (loaders) or :func:`default_records_by_schema` (the
node-state predictor). Both sit on :func:`effective_default`. Nothing else
re-derives it, so a run and the node state it is judged by cannot disagree
about what the default is.

Storage: ``_variant_pin``, one row per pin ever made. Moving or releasing a pin
sets ``released_at`` on the old row and never removes it, so the history of
"what was the default, when, and why" stays in the database.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from .log import Log

if TYPE_CHECKING:
    from .database import DatabaseManager

TABLE = "_variant_pin"


@dataclass
class VariantPin:
    pin_id: str
    variable: str
    #: Canonical selection (``variant.normalize_selection``), values as stored.
    selection: dict
    reason: str
    pinned_by: str | None
    pinned_at: str
    released_at: str | None = None
    release_reason: str | None = None

    @property
    def active(self) -> bool:
        return self.released_at is None


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def ensure_variant_pin_table(duck) -> None:
    """Create ``_variant_pin`` if missing (idempotent; skipped read-only)."""
    duck._execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            pin_id         VARCHAR PRIMARY KEY,
            variable       VARCHAR NOT NULL,
            selection_json VARCHAR NOT NULL,
            reason         VARCHAR NOT NULL,
            pinned_by      VARCHAR,
            pinned_at      VARCHAR NOT NULL,
            released_at    VARCHAR,
            release_reason VARCHAR
        )
    """)


def _row_to_pin(row) -> VariantPin:
    pin_id, variable, selection_json, reason, by, at, released, rel_reason = row
    return VariantPin(
        pin_id=pin_id,
        variable=variable,
        selection=json.loads(selection_json),
        reason=reason,
        pinned_by=by,
        pinned_at=at,
        released_at=released,
        release_reason=rel_reason,
    )


_COLUMNS = (
    "pin_id, variable, selection_json, reason, pinned_by, pinned_at, "
    "released_at, release_reason"
)


def active_pins(db: "DatabaseManager") -> dict[str, VariantPin]:
    """``{variable: VariantPin}`` for every variable with an active pin.

    Read fresh every time: the table is tiny, and caching it would let one
    process miss a pin another wrote between its sessions.
    """
    duck = db._duck
    if not duck._table_exists(TABLE):
        return {}
    rows = duck._fetchall(
        f"SELECT {_COLUMNS} FROM {TABLE} WHERE released_at IS NULL "
        f"ORDER BY pinned_at"
    )
    out: dict[str, VariantPin] = {}
    for row in rows:
        pin = _row_to_pin(row)
        if pin.variable in out:
            # pin_variant releases the old row before writing the new one, so
            # this is another writer's doing. Newest wins, and say so.
            Log.warn(
                f"variant_pins: {pin.variable} has more than one active pin; "
                f"using the newest ({pin.pin_id}, {pin.pinned_at})"
            )
        out[pin.variable] = pin
    return out


def pin_history(db: "DatabaseManager", variable: str | None = None) -> list[VariantPin]:
    """Every pin ever made (active and released), oldest first."""
    duck = db._duck
    if not duck._table_exists(TABLE):
        return []
    if variable is None:
        rows = duck._fetchall(f"SELECT {_COLUMNS} FROM {TABLE} ORDER BY pinned_at")
    else:
        rows = duck._fetchall(
            f"SELECT {_COLUMNS} FROM {TABLE} WHERE variable = ? ORDER BY pinned_at",
            [variable],
        )
    return [_row_to_pin(r) for r in rows]


def _now() -> str:
    return datetime.now().isoformat()


def pin_variant(
    db: "DatabaseManager", variable, selection: dict, reason: str
) -> VariantPin:
    """Make ``selection`` the default variant of ``variable``.

    The selection must name exactly one variant card. If its records span two
    cards (a selection the pin vocabulary cannot make exact), the pin is
    refused, because it would quietly make two variants the default. An active
    pin on the same variable is released first, with a reason that names the
    new pin.
    """
    from .database import get_user_id
    from .inspect.variant_cards import build_variant_cards
    from .provenance_query import records_for_variant
    from .variant import normalize_selection

    name = getattr(variable, "__name__", variable)
    if not reason or not str(reason).strip():
        raise ValueError("pin_variant needs a reason; it is the audit trail")
    canonical = normalize_selection(selection or {})

    rids = set(records_for_variant(db, name, canonical))
    if not rids:
        raise ValueError(
            f"pin_variant({name}): the selection {canonical} matches no records"
        )
    cards = build_variant_cards(db, name, include_runs=False)
    hit = [c for c in cards.cards if rids & set(c.record_ids)]
    if len(hit) != 1:
        raise ValueError(
            f"pin_variant({name}): the selection {canonical} matches records of "
            f"{len(hit)} variant cards {[c.card_id for c in hit]}; pin exactly "
            f"one (use a card's own selection)"
        )

    previous = active_pins(db).get(name)
    if previous is not None:
        _release(db, previous, f"replaced by a new pin: {reason}")

    pin = VariantPin(
        pin_id=uuid.uuid4().hex[:16],
        variable=name,
        selection=canonical,
        reason=str(reason),
        pinned_by=get_user_id(),
        pinned_at=_now(),
    )
    db._duck._execute(
        f"INSERT INTO {TABLE} ({_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)",
        [
            pin.pin_id,
            pin.variable,
            json.dumps(pin.selection, sort_keys=True, default=str),
            pin.reason,
            pin.pinned_by,
            pin.pinned_at,
        ],
    )
    Log.info(
        f"variant_pins: pinned {name} -> card {hit[0].card_id} "
        f"selection={canonical} ({len(rids)} record(s) now the default"
        f"{'; replaced pin ' + previous.pin_id if previous else ''}) reason={reason!r}"
    )
    return pin


def pin_newest(db: "DatabaseManager", variable, reason: str) -> VariantPin:
    """Move ``variable``'s pin to its most recently saved variant card.

    The GUI's "Move pin to the new output" choice before a run (D6), applied
    once the run has finished. "Newest" is the card with the latest
    ``last_saved``. A tie between two cards is refused rather than guessed,
    since one run can write two variants at the same instant.
    """
    from .inspect.variant_cards import build_variant_cards

    name = getattr(variable, "__name__", variable)
    cards = [c for c in build_variant_cards(db, name, include_runs=False).cards if c.last_saved]
    if not cards:
        raise ValueError(f"pin_newest({name}): no saved variants")
    newest = max(c.last_saved for c in cards)
    tied = [c for c in cards if c.last_saved == newest]
    if len(tied) > 1:
        raise ValueError(
            f"pin_newest({name}): {len(tied)} variants were saved last at the same "
            f"instant {[c.card_id for c in tied]}; pick one"
        )
    Log.info(f"variant_pins: pin_newest({name}) -> card {tied[0].card_id} ({newest})")
    return pin_variant(db, name, tied[0].selection, reason)


def release_pin(db: "DatabaseManager", variable, reason: str) -> VariantPin | None:
    """Release ``variable``'s active pin. Returns it, or None if there was none."""
    name = getattr(variable, "__name__", variable)
    if not reason or not str(reason).strip():
        raise ValueError("release_pin needs a reason; it is the audit trail")
    pin = active_pins(db).get(name)
    if pin is None:
        Log.info(f"variant_pins: release_pin({name}): no active pin")
        return None
    return _release(db, pin, str(reason))


def _release(db, pin: VariantPin, reason: str) -> VariantPin:
    released_at = _now()
    db._duck._execute(
        f"UPDATE {TABLE} SET released_at = ?, release_reason = ? WHERE pin_id = ?",
        [released_at, reason, pin.pin_id],
    )
    Log.info(f"variant_pins: released {pin.variable} pin {pin.pin_id} reason={reason!r}")
    pin.released_at = released_at
    pin.release_reason = reason
    return pin


# ---------------------------------------------------------------------------
# Which pins reach a variable: the variable-type ancestry
# ---------------------------------------------------------------------------

#: Attribute on the connection object holding ``(invocation_count, {out_type:
#: {in_type, ...}})``. The type graph only changes when an invocation is
#: written, so the invocation count is its fingerprint. Stored ON the
#: connection (not in a dict keyed by ``id()``) so it dies with it: a new
#: database can reuse a closed one's id, and with an equal invocation count it
#: would read the other database's graph.
_TYPE_GRAPH_ATTR = "_scidb_variant_pin_type_graph"


def _type_parents(duck) -> dict[str, set]:
    """``{variable type: {direct input variable types}}`` from the graph.

    A glued input reports its SOURCE type, matching how ``pipeline_structure``
    describes type flow. Two queries plus one glue lookup, cached on the
    invocation count.
    """
    from . import provenance_query as pq
    from .provenance import CONSTANT_TYPE, GLUE_TYPE, PATHINPUT_TYPE

    (count,) = duck._fetchone("SELECT COUNT(*) FROM _invocation")
    cached = getattr(duck, _TYPE_GRAPH_ATTR, None)
    if cached is not None and cached[0] == count:
        Log.debug(f"variant_pins: type graph cache hit ({count} invocations)")
        return cached[1]
    t0 = time.perf_counter()
    rows = duck._fetchall(
        "SELECT DISTINCT ro.type, ri.type, "
        "CASE WHEN ri.type = ? THEN ii.input_record_id END "
        "FROM _invocation_input ii "
        "JOIN _record ri ON ri.record_id = ii.input_record_id "
        "JOIN _invocation_output io ON io.invocation_id = ii.invocation_id "
        "JOIN _record ro ON ro.record_id = io.output_record_id "
        "WHERE ri.type NOT IN (?, ?)",
        [GLUE_TYPE, CONSTANT_TYPE, PATHINPUT_TYPE],
    )
    glue_rids = [g for _o, _i, g in rows if g is not None]
    glue_src = pq.glue_source_batch(duck, glue_rids) if glue_rids else {}
    parents: dict[str, set] = {}
    for out_type, in_type, glue_rid in rows:
        if glue_rid is not None:
            in_type = (glue_src.get(glue_rid) or {}).get("variable_type") or in_type
        if in_type == out_type or in_type == GLUE_TYPE:
            continue
        parents.setdefault(out_type, set()).add(in_type)
    try:
        setattr(duck, _TYPE_GRAPH_ATTR, (count, parents))
    except AttributeError:  # pragma: no cover - a slotted connection: no cache
        Log.debug("variant_pins: connection takes no attributes; type graph uncached")
    Log.debug(
        f"variant_pins: type graph rebuilt ({count} invocations, "
        f"{sum(len(v) for v in parents.values())} edge(s)) in "
        f"{(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return parents


def _ancestors_by_distance(duck, variable: str) -> dict[str, int]:
    """``{ancestor type: shortest distance}``, ``variable`` itself at 0."""
    parents = _type_parents(duck)
    dist = {variable: 0}
    frontier = [variable]
    while frontier:
        nxt = []
        for t in frontier:
            for p in parents.get(t, ()):
                if p not in dist:
                    dist[p] = dist[t] + 1
                    nxt.append(p)
        frontier = nxt
    return dist


# ---------------------------------------------------------------------------
# The default
# ---------------------------------------------------------------------------


def effective_default(
    db: "DatabaseManager", variable, *, upstream_only: bool = False
) -> tuple[dict, list[str]] | None:
    """``(selection, [pinned variables it came from])`` for an unnamed load of
    ``variable``, or None when no pin reaches it.

    The pins on ``variable`` and on every variable upstream of it merge per
    key, farthest first, so the nearest pin wins a key both set (D5).
    ``upstream_only`` leaves ``variable``'s own pin out: what the default would
    be without it, which is how a card detects a pin that disagrees upstream.
    """
    name = getattr(variable, "__name__", variable)
    pins = active_pins(db)
    if not pins:
        return None
    dist = _ancestors_by_distance(db._duck, name)
    reaching = sorted(
        (v for v in pins if v in dist and not (upstream_only and v == name)),
        key=lambda v: (-dist[v], v),
    )
    if not reaching:
        return None
    merged: dict = {}
    for v in reaching:
        for key, value in pins[v].selection.items():
            if key in merged and merged[key] != value:
                Log.info(
                    f"variant_pins: default for {name}: {v}'s pin overrides "
                    f"{key}={merged[key]!r} with {value!r} (nearer pin wins)"
                )
            merged[key] = value
    return merged, reaching


def default_load_args(
    db, variable, branch_params_filter: dict | None, version_id: str
) -> tuple[dict | None, str]:
    """The ``(branch_params_filter, version_id)`` a load should use.

    Unchanged unless the load is UNNAMED (no filter) and ``latest``, and a pin
    reaches the variable. Then the default selection becomes the filter, and
    the version rule follows ``variant.pin_loads_uncollapsed`` exactly as for
    an explicit pin.
    """
    if branch_params_filter or version_id != "latest" or db is None:
        return branch_params_filter, version_id
    if not hasattr(db, "_duck"):
        return branch_params_filter, version_id
    found = effective_default(db, variable)
    if found is None:
        return branch_params_filter, version_id
    from .variant import pin_loads_uncollapsed

    selection, sources = found
    new_version = "all" if pin_loads_uncollapsed(selection) else "latest"
    Log.info(
        f"[default] {getattr(variable, '__name__', variable)}: no variant named; "
        f"loading the pinned default {selection} (from pin(s) on {sources}, "
        f"version_id={new_version})"
    )
    return selection, new_version


def default_records_by_schema(db, variable) -> dict | None:
    """``{schema_id: [record_id, ...]}`` of the default records, or None when no
    pin reaches ``variable``. The node-state predictor's view of the default,
    resolved through the loader's own ``records_for_variant``."""
    found = effective_default(db, variable)
    if found is None:
        return None
    from .provenance_query import _chunked_in, records_for_variant

    selection, sources = found
    name = getattr(variable, "__name__", variable)
    rids = records_for_variant(db, name, selection)
    out: dict = {}
    if rids:
        for rid, sid in _chunked_in(
            db._duck,
            "SELECT record_id, schema_id FROM _record WHERE record_id IN ({ph})",
            rids,
        ):
            out.setdefault(sid, []).append(rid)
    Log.info(
        f"[default] node-state prediction for {name}: {len(rids)} default "
        f"record(s) at {len(out)} location(s) (pin(s) on {sources})"
    )
    return out
