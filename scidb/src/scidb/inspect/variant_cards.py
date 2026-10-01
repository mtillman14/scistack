"""Variant cards: every full-chain variant of one variable, and what defines it.

The read side of ``.claude/plan-variants-popup.md`` (Stage 1); the rules are in
``docs/claude/variant-pins-and-deletion.md``. ``Inspector.variant_cards`` is
the entry point, ``scidb variants X --cards`` renders it, and the GUI Variants
popup shows it. None of them groups or labels anything itself.

**Why this is not** ``Inspector.variants``. That groups by the producing call
(``call_id``) and reports only that call's own constants. So two records that
differ only in a setting two steps upstream (``filter.low_hz=10`` vs ``20``)
come out as one row. A card groups by the record's whole coordinate in variant
space (``docs/claude/variant-space.md``):

* upstream constants (``branch_params_batch``);
* per-function code version, for functions holding more than one version
  (``code_version_ordinals``);
* per-function run options, for functions run more than one way
  (``run_option_axes``).

The two omission rules mean a single-version, single-option function adds no
key. That matches what the pin vocabulary can say: a selection naming a choice
nobody ever had would be noise.

**A card's ``selection``** is that coordinate written as a pin dict, so
``records_for_variant(variable, card.selection)`` resolves back to this card's
records. When another card's coordinate also satisfies it (the vocabulary
cannot say "and no other keys"), ``selection_exact`` is False and
``overlaps_with`` names the other cards. That is reported, never hidden: a pin
built from it would select both.

All graph reads are batched over the whole variable at once. One upstream
closure feeds both coordinate walks and the per-card upstream graph, so the
query count does not grow with the record count (the N+1 rule,
``project_batched_provenance_hot_paths``).
"""

from __future__ import annotations

import hashlib
import time
from collections import Counter
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import TYPE_CHECKING

from ..exceptions import NotFoundError
from ..log import Log
from .api import RunRef, _iso, variant_verdict

if TYPE_CHECKING:
    from ..database import DatabaseManager


#: Node-id prefixes for :class:`VariantUpstream`. Ids are local to one card's
#: graph and never stored; they only have to be unique and readable.
VARIABLE_NODE_PREFIX = "var:"
STEP_NODE_PREFIX = "fn:"


@dataclass
class CodeRef:
    """One version of a step's source seen in a card's chain."""

    function_hash: str | None
    #: The per-function ordinal (``v1``/``v2``). None when the function only
    #: has one version, the same rule as ``TraceNode.code_version``.
    version: str | None
    invocations: int


@dataclass
class UpstreamStep:
    """One function in a card's upstream chain, aggregated over every
    invocation of it that fed the card's records."""

    node_id: str
    function_name: str
    output_type: str
    #: ``{param: variable type}`` of the variable inputs.
    inputs: dict[str, str]
    #: ``{param: {value: invocations}}``. One value per param is the normal
    #: case; more than one makes the step non-uniform.
    constants: dict[str, dict[str, int]]
    #: ``{param: {PathInput spec: invocations}}``.
    path_inputs: dict[str, dict[str, int]]
    code: list[CodeRef]
    #: ``{run options label: invocations}``.
    run_options: dict[str, int]
    invocations: int
    #: False when any setting above took more than one value across the card's
    #: records. By construction the coordinate fixes them, so False is logged
    #: at WARN and is worth reading.
    uniform: bool
    #: ``{param: Parameter node name}`` for each constant, by
    #: ``parameter.parameter_node_name`` (the recorded declared name, else the
    #: argument). What the GUI needs to offer "remove this value from the
    #: Parameter" without re-deriving which Parameter fed which port.
    parameter_names: dict[str, str] = field(default_factory=dict)


@dataclass
class UpstreamEdge:
    source: str  # node_id
    target: str  # node_id
    #: The function argument for a variable → step edge. Empty for step → output.
    param: str = ""


@dataclass
class VariantUpstream:
    """The whole upstream pipeline of one card, as a small DAG."""

    variables: list[str]  # variable node ids are VARIABLE_NODE_PREFIX + name
    steps: list[UpstreamStep]
    edges: list[UpstreamEdge]


@dataclass
class VariantCard:
    #: Stable within one database state; for UI keys, never stored.
    card_id: str
    #: The direct producing function, or None for a raw / direct save.
    function_name: str | None
    #: The canonical pin naming this variant (``fn.param``, ``__code__.fn``,
    #: ``__run__.fn``). Values are kept as stored, not stringified.
    selection: dict
    #: ``{axis key: display value}`` for the axes that differ between this
    #: variable's cards. The display value is ``"—"`` where this card has no
    #: value on that axis.
    distinguishing: dict[str, str]
    selection_exact: bool
    overlaps_with: list[str]
    record_ids: list[str]
    record_count: int
    first_saved: str | None
    last_saved: str | None
    #: ``scidb.inspect.api.VERDICT_*``: does a plain ``latest`` load still
    #: return this card's records, judged per location.
    verdict: str
    verdict_label: str
    current_location_count: int
    #: Ordered as the dataset's schema keys; only keys that are populated.
    location_keys: list[str]
    #: Every location, sorted, as ``{key: value}``.
    locations: list[dict]
    #: Runs of the direct producing invocations, newest first.
    runs: list[RunRef]
    upstream: VariantUpstream
    #: Whether an unnamed load gets this card (``variant_pins``). With no pin
    #: reaching the variable, every card a plain load still returns is a
    #: default (today's rule: every current variant flows downstream).
    is_default: bool = False
    #: This variable's own pin names this card.
    is_pinned: bool = False
    #: Set on a pinned card whose selection disagrees with a pin upstream
    #: (allowed, the nearer pin wins, D5): ``"key: upstream=… here=…; …"``.
    pin_conflict: str | None = None


@dataclass
class VariantCards:
    variable: str
    cards: list[VariantCard]
    #: The axes that differ between cards, in display order.
    varying_axes: list[str]
    #: True when cards differ by their direct producing function (a second
    #: topology), which no selection axis can show.
    producer_varies: bool
    #: Records of this variable hidden with ``exclude_variant``. They are on no
    #: card, because no load returns them.
    excluded_record_count: int = 0
    command: str = ""
    timings_ms: dict[str, float] = field(default_factory=dict)
    #: This variable's active pin (``variant_pins.VariantPin``), if any.
    pin: object | None = None
    #: The selection an unnamed load uses, and the pinned variables it comes
    #: from. None / [] when no pin reaches this variable.
    default_selection: dict | None = None
    default_sources: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure helpers (no database): kept separate so the rules are testable alone
# ---------------------------------------------------------------------------

_ABSENT = object()


def coordinate_selection(
    branch_params: dict, chain: dict, ordinals: dict, run_axes: dict
) -> dict:
    """A record's coordinate as a canonical selection dict.

    ``branch_params`` from ``branch_params_batch``; ``chain`` is one
    ``chain_batch`` entry (``{"code": {fn: hash}, "run": {fn: label}}``);
    ``ordinals`` is ``code_version_ordinals`` and ``run_axes`` is
    ``run_option_axes``, over the variable's whole chain. Only functions with a
    real choice contribute a code or run key.
    """
    from ..variant import CODE_PIN_PREFIX, RUN_PIN_PREFIX

    selection = dict(branch_params or {})
    for fn_name, fn_hash in sorted((chain or {}).get("code", {}).items()):
        version = ordinals.get(fn_name, {}).get(fn_hash)
        if version is not None:
            selection[f"{CODE_PIN_PREFIX}.{fn_name}"] = version
    for fn_name, label in sorted((chain or {}).get("run", {}).items()):
        if fn_name in run_axes and label is not None:
            selection[f"{RUN_PIN_PREFIX}.{fn_name}"] = label
    return dict(sorted(selection.items()))


def selection_matches(selection: dict, coordinate: dict) -> bool:
    """Whether a coordinate satisfies a selection: every selected key is present
    with an equal value. Canonical keys only, so there is no suffix matching
    here; that rule belongs to the loader and is never needed between two
    canonical dicts."""
    from ..bindings import variant_signature

    for key, value in selection.items():
        other = coordinate.get(key, _ABSENT)
        if other is _ABSENT:
            return False
        if variant_signature({"v": other}) != variant_signature({"v": value}):
            return False
    return True


def varying_axes(selections: list[dict]) -> list[str]:
    """The keys whose value is not the same on every selection (a key missing
    from one selection counts as a different value). Sorted: constants first,
    then code, then run options, each alphabetical."""
    from ..bindings import variant_signature
    from ..variant import CODE_PIN_PREFIX, RUN_PIN_PREFIX

    if len(selections) < 2:
        return []
    keys = sorted({k for s in selections for k in s})
    out = []
    for key in keys:
        values = {
            variant_signature({"v": s[key]}) if key in s else "\0absent"
            for s in selections
        }
        if len(values) > 1:
            out.append(key)

    def order(key):
        if key.startswith(CODE_PIN_PREFIX):
            return (1, key)
        if key.startswith(RUN_PIN_PREFIX):
            return (2, key)
        return (0, key)

    return sorted(out, key=order)


def _display(value) -> str:
    from .graph import _value_str

    return value if isinstance(value, str) else _value_str(value)


def _card_id(function_name, chain_fns, selection) -> str:
    from ..bindings import variant_signature

    text = f"{function_name}|{sorted(chain_fns)}|{variant_signature(selection)}"
    return hashlib.sha1(text.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def build_variant_cards(
    db: "DatabaseManager", variable, *, include_runs: bool = True
) -> VariantCards:
    """Every full-chain variant of ``variable``, oldest first. See the module
    docstring for what a card is and why."""
    from .. import provenance_query as pq
    from ..provenance import SAVE_FUNCTION_NAME

    name = getattr(variable, "__name__", variable)
    duck = db._duck
    timings: dict[str, float] = {}
    t_all = time.perf_counter()

    def lap(label, t0):
        timings[label] = round((time.perf_counter() - t0) * 1000.0, 1)

    # -- the records ---------------------------------------------------------
    t0 = time.perf_counter()
    known = duck._fetchall("SELECT 1 FROM _variables WHERE variable_name = ?", [name])
    rows = duck._fetchall(
        "SELECT r.record_id, r.schema_id, MIN(rm.timestamp), MAX(rm.timestamp), "
        "COALESCE(r.excluded, FALSE) "
        "FROM _record r JOIN _record_save rm ON rm.record_id = r.record_id "
        "WHERE r.type = ? GROUP BY r.record_id, r.schema_id, r.excluded",
        [name],
    )
    if not known and not rows:
        raise NotFoundError(f"{name!r} is not a variable type in this database")
    excluded = sum(1 for *_r, ex in rows if ex)
    rows = [r for r in rows if not r[4]]
    rids = [r[0] for r in rows]
    lap("records", t0)

    command = f"scidb variants {name} --cards"
    if not rids:
        Log.info(f"variant_cards({name}): no records ({excluded} excluded)")
        return VariantCards(
            variable=name,
            cards=[],
            varying_axes=[],
            producer_varies=False,
            excluded_record_count=excluded,
            command=command,
            timings_ms=timings,
        )

    # -- one closure, three walks -------------------------------------------
    t0 = time.perf_counter()
    closure = pq._build_upstream_closure(duck, rids)
    rec_to_inv, _consts, _inv_inputs, inv_fn_hash, inv_run = closure
    bp_map = pq.branch_params_batch(duck, rids, closure=closure)
    chain_map = pq.chain_batch(duck, rids, closure=closure)
    chain_fns = {fn for chain in chain_map.values() for fn in chain["code"]}
    ordinals = pq.code_version_ordinals(duck, chain_fns)
    run_axes = pq.run_option_axes(duck, chain_fns)
    lap("coordinates", t0)

    # -- group into cards ----------------------------------------------------
    t0 = time.perf_counter()
    groups: dict = {}
    for rid, schema_id, first_ts, last_ts, _ex in rows:
        inv = rec_to_inv.get(rid)
        producer = inv[1] if inv and inv[1] != SAVE_FUNCTION_NAME else None
        chain = chain_map.get(rid, {"code": {}, "run": {}})
        selection = coordinate_selection(
            bp_map.get(rid, {}), chain, ordinals, run_axes
        )
        key = _card_id(producer, chain["code"].keys(), selection)
        group = groups.setdefault(
            key,
            {
                "producer": producer,
                "selection": selection,
                "rids": [],
                "schema_ids": set(),
                "first": first_ts,
                "last": last_ts,
            },
        )
        group["rids"].append(rid)
        group["schema_ids"].add(schema_id)
        if first_ts is not None and (group["first"] is None or first_ts < group["first"]):
            group["first"] = first_ts
        if last_ts is not None and (group["last"] is None or last_ts > group["last"]):
            group["last"] = last_ts
    # Oldest first. One for_each run saves its whole batch at one instant, so
    # variants it wrote together tie; the selection's canonical text breaks the
    # tie, which keeps the order deterministic (low_hz=10 before 20), where the
    # card id, being a hash, would not be meaningful.
    from ..bindings import variant_signature

    ordered = sorted(
        groups.items(),
        key=lambda kv: (
            str(kv[1]["first"] or "9999"),
            str(kv[1]["producer"]),
            variant_signature(kv[1]["selection"]),
            kv[0],
        ),
    )
    selections = [g["selection"] for _k, g in ordered]
    axes = varying_axes(selections)
    producer_varies = len({g["producer"] for _k, g in ordered}) > 1
    lap("grouping", t0)

    # -- what a plain load returns (the load path itself, not a re-derivation)
    t0 = time.perf_counter()
    latest_df = db._find_record(name, version_id="latest")
    latest = set() if latest_df.empty else set(latest_df["record_id"])
    lap("latest_load", t0)

    # -- locations -----------------------------------------------------------
    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    all_sids = {sid for _k, g in ordered for sid in g["schema_ids"]}
    loc_map = pq._schema_locations(duck, all_sids, schema_keys)
    lap("locations", t0)

    # -- upstream wiring + runs, batched over every invocation in the closure
    t0 = time.perf_counter()
    every_inv = sorted({inv[0] for inv in rec_to_inv.values()})
    edges_map = pq.invocation_edges_batch(duck, every_inv)
    direct_invs = sorted({rec_to_inv[r][0] for r in rids if r in rec_to_inv})
    inv_runs = (
        pq.runs_for_invocations_batch(duck, direct_invs) if include_runs else {}
    )
    lap("wiring_and_runs", t0)

    t0 = time.perf_counter()
    rid_sid = {r[0]: r[1] for r in rows}
    cards: list[VariantCard] = []
    for key, g in ordered:
        sids = sorted(g["schema_ids"], key=lambda s: (s is None, str(s)))
        locations = [loc_map.get(sid, {}) for sid in sids if sid is not None]
        location_keys = [k for k in schema_keys if any(k in loc for loc in locations)]
        locations.sort(key=lambda loc: tuple(str(loc.get(k, "")) for k in location_keys))
        current_sids = {rid_sid[r] for r in g["rids"] if r in latest}
        live, total = len(current_sids), len(g["schema_ids"])
        verdict, _label = variant_verdict(
            SimpleNamespace(
                current=live == total, current_record_count=live, record_count=total
            )
        )
        if live == total:
            label = f"a plain load returns this variant at all {total} location(s)"
        elif live:
            label = (
                f"a plain load returns this variant at {live} of {total} "
                f"location(s); newer records replace it elsewhere"
            )
        else:
            label = "a plain load returns none of it; newer records replace it"

        overlaps = [
            other_key
            for other_key, other in ordered
            if other_key != key and selection_matches(g["selection"], other["selection"])
        ]
        runs = _card_runs(g["rids"], rec_to_inv, inv_runs, inv_fn_hash, inv_run)
        upstream = _card_upstream(
            name, g["rids"], rec_to_inv, edges_map, inv_fn_hash, inv_run, ordinals
        )
        cards.append(
            VariantCard(
                card_id=key,
                function_name=g["producer"],
                selection=g["selection"],
                distinguishing={
                    axis: _display(g["selection"][axis]) if axis in g["selection"] else "—"
                    for axis in axes
                },
                selection_exact=not overlaps,
                overlaps_with=overlaps,
                record_ids=sorted(g["rids"]),
                record_count=len(g["rids"]),
                first_saved=_iso(g["first"]),
                last_saved=_iso(g["last"]),
                verdict=verdict,
                verdict_label=label,
                current_location_count=live,
                location_keys=location_keys,
                locations=locations,
                runs=runs,
                upstream=upstream,
            )
        )
    lap("cards", t0)

    # -- pins: which card an unnamed load gets (scidb.variant_pins owns it) --
    t0 = time.perf_counter()
    pin, default_selection, default_sources = _mark_defaults(db, name, cards)
    lap("pins", t0)
    timings["total"] = round((time.perf_counter() - t_all) * 1000.0, 1)

    inexact = [c.card_id for c in cards if not c.selection_exact]
    Log.info(
        f"variant_cards({name}): {len(cards)} card(s) over {len(rids)} record(s)"
        f" ({excluded} excluded); varying axes={axes or 'none'}"
        f"{'; producer varies' if producer_varies else ''}; "
        f"records per card={[c.record_count for c in cards]}; timings_ms={timings}"
    )
    if inexact:
        Log.warn(
            f"variant_cards({name}): {len(inexact)} card(s) have a selection that "
            f"other cards also satisfy {inexact}; a pin built from them would "
            f"select more than one variant"
        )
    return VariantCards(
        variable=name,
        cards=cards,
        varying_axes=axes,
        producer_varies=producer_varies,
        excluded_record_count=excluded,
        command=command,
        timings_ms=timings,
        pin=pin,
        default_selection=default_selection,
        default_sources=default_sources,
    )


def _mark_defaults(db, name: str, cards: list[VariantCard]):
    """Set ``is_default`` / ``is_pinned`` / ``pin_conflict`` on every card.

    The default selection comes from ``variant_pins.effective_default``, the
    same call the loaders make. A card is the default when its coordinate
    satisfies that selection, and, unless the selection loads uncollapsed (a
    code or run pin), when a plain load still returns it: a constants-only pin
    still goes through the ``latest`` collapse, so a superseded code version
    that matches the constants is not what the load returns.
    """
    from ..variant import pin_loads_uncollapsed
    from ..variant_pins import active_pins, effective_default

    pin = active_pins(db).get(name)
    found = effective_default(db, name)
    if found is None:
        for card in cards:
            card.is_default = card.verdict != "superseded"
        return None, None, []
    selection, sources = found
    uncollapsed = pin_loads_uncollapsed(selection)
    for card in cards:
        matches = selection_matches(selection, card.selection)
        card.is_default = matches and (uncollapsed or card.verdict != "superseded")
        card.is_pinned = pin is not None and selection_matches(pin.selection, card.selection)
    if pin is not None:
        upstream = effective_default(db, name, upstream_only=True)
        if upstream is not None:
            up_sel, up_sources = upstream
            for card in cards:
                if not card.is_pinned:
                    continue
                diffs = [
                    f"{k}: upstream={_display(v)} here="
                    + (_display(card.selection[k]) if k in card.selection else "—")
                    for k, v in up_sel.items()
                    if not selection_matches({k: v}, card.selection)
                ]
                if diffs:
                    card.pin_conflict = f"disagrees with pin(s) on {up_sources}: " + "; ".join(diffs)
                    Log.info(
                        f"variant_cards({name}): pinned card {card.card_id} "
                        f"{card.pin_conflict} (allowed; the nearer pin wins)"
                    )
    n_default = sum(c.is_default for c in cards)
    Log.info(
        f"variant_cards({name}): default {selection} from pin(s) on {sources}; "
        f"{n_default} card(s) are the default"
    )
    if n_default == 0:
        Log.warn(
            f"variant_cards({name}): the default selection matches no card, so an "
            f"unnamed load returns nothing"
        )
    return pin, selection, sources


def _card_runs(rids, rec_to_inv, inv_runs, inv_fn_hash, inv_run) -> list[RunRef]:
    """Runs of the card's direct producing invocations, newest first, one per
    (run, invocation)."""
    seen: set = set()
    out: list[RunRef] = []
    for inv_id in sorted({rec_to_inv[r][0] for r in rids if r in rec_to_inv}):
        for run_id, ts, uid, where in inv_runs.get(inv_id, ()):
            if (run_id, inv_id) in seen:
                continue
            seen.add((run_id, inv_id))
            out.append(
                RunRef(
                    run_id=run_id,
                    timestamp=_iso(ts) or "",
                    user_id=uid,
                    where_clause=where,
                    invocation_id=inv_id,
                    function_hash=inv_fn_hash.get(inv_id),
                    run_options=inv_run.get(inv_id),
                )
            )
    out.sort(key=lambda r: (r.timestamp, r.run_id), reverse=True)
    return out


def _card_upstream(
    variable, rids, rec_to_inv, edges_map, inv_fn_hash, inv_run, ordinals
) -> VariantUpstream:
    """The card's upstream DAG: steps keyed by (function, output type), each
    aggregating every invocation of it that fed the card's records."""
    from ..parameter import parameter_node_name
    from ..provenance import SAVE_FUNCTION_NAME

    variables: dict[str, None] = {VARIABLE_NODE_PREFIX + variable: None}
    steps: dict[str, dict] = {}
    edges: dict[tuple, UpstreamEdge] = {}
    seen_inv_per_step: dict[str, set] = {}

    frontier = [(rid, variable) for rid in rids]
    visited: set = set()
    while frontier:
        rid, var_type = frontier.pop()
        if rid in visited:
            continue
        visited.add(rid)
        inv = rec_to_inv.get(rid)
        if inv is None:
            continue
        inv_id, fn_name = inv
        if fn_name == SAVE_FUNCTION_NAME:
            continue  # a direct save's kwargs are on the coordinate, not a step
        step_id = f"{STEP_NODE_PREFIX}{fn_name}->{var_type}"
        var_id = VARIABLE_NODE_PREFIX + var_type
        variables.setdefault(var_id, None)
        edges.setdefault((step_id, var_id, ""), UpstreamEdge(step_id, var_id, ""))
        step = steps.setdefault(
            step_id,
            {
                "function_name": fn_name,
                "output_type": var_type,
                "inputs": {},
                "constants": {},
                "path_inputs": {},
                "parameter_names": {},
                "code": Counter(),
                "run": Counter(),
            },
        )
        wiring = edges_map.get(inv_id) or {}
        for vin in wiring.get("var_inputs", ()):
            in_type = vin["variable_type"]
            param = vin["param_name"]
            step["inputs"][param] = in_type
            in_id = VARIABLE_NODE_PREFIX + in_type
            variables.setdefault(in_id, None)
            edges.setdefault((in_id, step_id, param), UpstreamEdge(in_id, step_id, param))
            frontier.append((vin["record_id"], in_type))
        invs = seen_inv_per_step.setdefault(step_id, set())
        if inv_id in invs:
            continue  # one invocation counted once, however many records it made
        invs.add(inv_id)
        for param, value in (wiring.get("constants") or {}).items():
            step["constants"].setdefault(param, Counter())[_display(value)] += 1
            step["parameter_names"].setdefault(
                param, parameter_node_name(param, wiring.get("declared_names"))
            )
        for param, spec in (wiring.get("path_inputs") or {}).items():
            step["path_inputs"].setdefault(param, Counter())[str(spec)] += 1
        step["code"][inv_fn_hash.get(inv_id)] += 1
        run_label = inv_run.get(inv_id)
        if run_label is not None:
            step["run"][run_label] += 1

    out_steps: list[UpstreamStep] = []
    for step_id, s in steps.items():
        code = [
            CodeRef(
                function_hash=h,
                version=ordinals.get(s["function_name"], {}).get(h),
                invocations=n,
            )
            for h, n in sorted(s["code"].items(), key=lambda kv: str(kv[0]))
        ]
        uniform = (
            len(code) <= 1
            and len(s["run"]) <= 1
            and all(len(v) <= 1 for v in s["constants"].values())
            and all(len(v) <= 1 for v in s["path_inputs"].values())
        )
        if not uniform:
            Log.warn(
                f"variant_cards({variable}): step {s['function_name']} -> "
                f"{s['output_type']} is not uniform across one variant: code="
                f"{[c.version or (c.function_hash or '')[:8] for c in code]} "
                f"run={dict(s['run'])} constants="
                f"{ {k: dict(v) for k, v in s['constants'].items()} }"
            )
        out_steps.append(
            UpstreamStep(
                node_id=step_id,
                function_name=s["function_name"],
                output_type=s["output_type"],
                inputs=dict(sorted(s["inputs"].items())),
                constants={k: dict(v) for k, v in sorted(s["constants"].items())},
                path_inputs={k: dict(v) for k, v in sorted(s["path_inputs"].items())},
                parameter_names=dict(sorted(s["parameter_names"].items())),
                code=code,
                run_options=dict(s["run"]),
                invocations=len(seen_inv_per_step.get(step_id, ())),
                uniform=uniform,
            )
        )
    out_steps.sort(key=lambda s: s.node_id)
    return VariantUpstream(
        variables=sorted(v[len(VARIABLE_NODE_PREFIX):] for v in variables),
        steps=out_steps,
        edges=sorted(edges.values(), key=lambda e: (e.source, e.target, e.param)),
    )
