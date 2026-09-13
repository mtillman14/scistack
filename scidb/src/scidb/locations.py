"""Per-(variable, variant, schema location) status — the fourth granularity.

``docs/claude/schema-location-status.md`` is the companion document and states
*why* this exists beside :func:`scidb.state.check_node_state` (per function,
binary) and :func:`scidb.state.check_combo_state` (per function + combo, one at
a time). Read it before changing what a colour means here.

The short version:

* **green** — a record exists at this location for this variant, and everything
  it was computed from is still the newest at its own location;
* **amber** — a record exists, but something upstream has been **re-saved**
  since. Deliberately *not* "the function body changed": that is untrustworthy
  for MATLAB functions, it is already what reddens the canvas node, and the
  record-level decision was made in ``defer-content-staleness``. The body
  actually used is reported per location as ``code_version`` instead;
* **red** — expected here, and no record for this variant;
* **grey** — deliberately excluded (``scidb.exclusions``), and therefore counted
  in neither the numerator nor the denominator.

Everything is computed in batch. A loop over ``check_combo_state`` would issue
two DB round-trips per ancestor edge per location; this issues a fixed number of
queries for the whole tree. That is not an optimisation, it is the reason the
feature is possible at study scale.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Literal

from . import provenance_query
from .log import Log
from .provenance import SAVE_FUNCTION_NAME

LocationState = Literal["green", "amber", "red", "grey"]

#: Order used when a parent summarises its children: the worst state wins.
_SEVERITY: dict[str, int] = {"grey": 0, "green": 1, "amber": 2, "red": 3}

#: Where the denominator came from. Surfaced so a caller can say how much the
#: count is worth — ``present_only`` in particular means "nothing knows what
#: *should* be here", which is not the same as complete.
Basis = Literal["expected", "discovery", "mixed", "present_only"]


@dataclass
class LocationNode:
    """One node of the location tree — a schema key/value at some depth.

    ``key`` is carried beside ``value`` because a non-contiguous save
    (``docs/claude/schema-hierarchy-contiguity.md``) hangs a ``speed`` node
    directly under ``subject``, as a sibling of ``timepoint`` nodes. A renderer
    showing only values would draw two different dimensions as one list.
    """

    key: str
    value: str
    state: LocationState
    counts: dict[str, int]
    is_leaf: bool
    #: The full location as ``(("subject","01"),("trial","03"))`` — what a
    #: caller turns into a filter when this row is clicked.
    path: tuple = ()
    schema_id: int | None = None
    record_id: str | None = None
    #: ``"v2"`` etc. — only when the producing function has more than one
    #: recorded version, matching ``code_version_ordinals``' "is this an axis"
    #: rule. This is how a body edit surfaces here instead of as a colour.
    code_version: str | None = None
    children: list[LocationNode] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "state": self.state,
            "counts": dict(self.counts),
            "is_leaf": self.is_leaf,
            "path": [list(pair) for pair in self.path],
            "schema_id": self.schema_id,
            "record_id": self.record_id,
            "code_version": self.code_version,
            "children": [c.to_dict() for c in self.children],
        }


@dataclass
class LocationTree:
    variable: str
    schema_keys: list[str]
    variant: dict
    roots: list[LocationNode] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    basis: Basis = "present_only"
    #: Plain-English caveats about what the numbers can and cannot be trusted
    #: to mean. Renderers must show these — several of them are the difference
    #: between a count and a claim.
    notes: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Locations that count — excluded ones are deliberately out."""
        return sum(self.counts.get(k, 0) for k in ("green", "amber", "red"))

    @property
    def green(self) -> int:
        return self.counts.get("green", 0)

    @property
    def verdict(self) -> LocationState:
        """The single dot at the top of the pane."""
        if self.total == 0:
            return "grey"
        return "green" if self.green == self.total else "red"

    def to_dict(self) -> dict:
        """JSON-ready, **including** the derived header numbers.

        ``dataclasses.asdict`` would silently drop ``total`` / ``green`` /
        ``verdict`` — they are properties, deliberately, so there is one
        definition of "how many count" rather than fields that can drift from
        the tree they summarise. Every consumer that serialises this (the CLI's
        ``--json``, the GUI's RPC) needs them, so they are added here rather
        than recomputed at each edge.
        """
        return {
            "variable": self.variable,
            "schema_keys": list(self.schema_keys),
            "variant": dict(self.variant),
            "counts": dict(self.counts),
            "total": self.total,
            "green": self.green,
            "verdict": self.verdict,
            "basis": self.basis,
            "notes": list(self.notes),
            "roots": [r.to_dict() for r in self.roots],
        }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def location_states(
    variable,
    *,
    variant: dict | None = None,
    db=None,
    fn_registry: dict | None = None,
    **grid: list,
) -> LocationTree:
    """Status of every schema location for one variable, under one variant.

    Args:
        variable: a variable class or its type name.
        variant: a ``branch_params_filter`` dict in scidb's own vocabulary —
            ``{"bandpass.low_hz": 20}``, plus optional ``__code__`` pins (see
            :mod:`scidb.variant`). ``None`` means every variant of the variable,
            in which case a location is present if *any* variant is.
        db: DatabaseManager (global DB if omitted).
        fn_registry: ``{fn_name: callable}`` for live source-edit detection when
            deriving the expected set, exactly as ``inspect.graph._node_states``
            takes it. Omitted ⇒ the most recently run stored hash.
        **grid: iteration grid for PathInput discovery (``subject=["01","02"]``).
            Omitted keys are wildcards, matching ``for_each``.

    Returns:
        A :class:`LocationTree`, collapsed nowhere — the caller decides what to
        show. Leaves carry a state; interior nodes carry the worst state beneath
        them and the roll-up counts.
    """
    if db is None:
        from .database import get_database

        db = get_database()

    name = getattr(variable, "__name__", variable)
    variant = dict(variant or {})
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    notes: list[str] = []
    timings: dict[str, float] = {}
    # Wall clock for the whole function, so the reported TOTAL covers the
    # untimed work between phases (notably the leaf_states loop) instead of
    # being the sum of the phases and quietly understating itself.
    _t_all = time.perf_counter()

    t0 = time.perf_counter()
    present = _present_by_location(db, name, variant)
    timings["present"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    expected, basis, expected_notes = _expected_locations(db, name, fn_registry, grid)
    notes.extend(expected_notes)
    timings["expected"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    amber = _superseded_locations(duck, present)
    timings["superseded"] = time.perf_counter() - t0

    universe = set(expected) | set(present)
    t0 = time.perf_counter()
    excluded = _excluded_locations(db, universe, schema_keys)
    timings["excluded"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    versions = _code_versions(duck, present)
    timings["versions"] = time.perf_counter() - t0

    leaf_states: dict[tuple, LocationState] = {}
    for combo in universe:
        if combo in excluded:
            leaf_states[combo] = "grey"
        elif combo not in present:
            leaf_states[combo] = "red"
        elif combo in amber:
            leaf_states[combo] = "amber"
        else:
            leaf_states[combo] = "green"

    tree = LocationTree(
        variable=name,
        schema_keys=schema_keys,
        variant=variant,
        basis=basis,
        notes=notes,
    )
    t0 = time.perf_counter()
    tree.roots, tree.counts = _build_tree(
        leaf_states, present, versions, _schema_ids_by_combo(db)
    )
    timings["tree"] = time.perf_counter() - t0

    Log.info(
        f"location_states({name}): {tree.green}/{tree.total} green "
        f"(amber={tree.counts.get('amber', 0)}, red={tree.counts.get('red', 0)}, "
        f"grey={tree.counts.get('grey', 0)}) basis={basis} "
        f"variant={variant or '(any)'}",
    )
    # Phase breakdown at INFO, not DEBUG. The location tree is one of two
    # surfaces that timed out on a 419-location variable (2026-09-13,
    # .claude/plot-at-scale-plan.md) and these six numbers were the only thing
    # that could have said which phase — invisible, because the file sink runs
    # at INFO. Same collect-then-hide shape save_batch and record_run had.
    Log.timings(
        f"location_states({name})",
        timings,
        extra=f"{tree.total} location(s)",
        total=time.perf_counter() - _t_all,
    )
    return tree


def prune_to_problems(tree: LocationTree) -> LocationTree:
    """A copy of ``tree`` holding only the branches with an amber or red leaf.

    "Show me what isn't green" is the query a scientist actually runs — more
    often than any name search, because a healthy study is mostly green and the
    exceptions are the whole point of opening the pane. Shared rather than
    reimplemented per surface so the CLI's ``--problems`` and the GUI's toggle
    cannot mean subtly different things.

    **Counts are left untouched**, on purpose: a pruned node still reports
    ``1/8``, because the denominator is what makes the numerator mean anything.
    Hiding the healthy rows must not also hide how many there were.
    """

    def keep(node: LocationNode) -> LocationNode | None:
        if node.counts.get("amber", 0) == 0 and node.counts.get("red", 0) == 0:
            return None
        clone = replace(node, children=[])
        clone.children = [c for c in (keep(child) for child in node.children) if c]
        return clone

    pruned = replace(tree, roots=[])
    pruned.roots = [r for r in (keep(root) for root in tree.roots) if r]
    return pruned


# ---------------------------------------------------------------------------
# The four set operations
# ---------------------------------------------------------------------------


def _present_by_location(db, name: str, variant: dict) -> dict:
    """``{combo: record_id}`` — the current record of ``name`` at each location,
    restricted to ``variant``.

    "Current" is the same latest-per-(location, producing variant) collapse the
    load path uses; the variant filter is the load path's own
    ``_filter_records_by_branch_params``, so the picker and a
    ``Variant(...).load()`` cannot disagree about which records a selection
    names.
    """
    import pandas as pd

    from .database import _filter_records_by_branch_params

    duck = db._duck
    by_sid = provenance_query.current_records_by_schema_batch(duck, name)
    all_rids = [rid for rids_ in by_sid.values() for rid in rids_]
    if not all_rids:
        return {}

    keep = set(all_rids)
    if variant:
        kept = _filter_records_by_branch_params(
            pd.DataFrame({"record_id": all_rids}), variant, duck
        )
        keep = set(kept["record_id"].tolist())
        if not keep:
            Log.info(
                f"location_states({name}): variant {variant} matched no records; "
                f"every expected location will read red",
            )
            return {}

    combos = _combos_for_schema_ids(db, by_sid.keys())
    saved_at = provenance_query.saved_at_batch(duck, sorted(keep))

    # One record per location. Several can survive when the variant selection is
    # partial (two genuinely distinct variants both match) — the newest wins, so
    # the dot describes the record the plot would open on.
    best: dict = {}
    for sid, sid_rids in by_sid.items():
        combo = combos.get(sid)
        if combo is None:
            continue
        for rid in sid_rids:
            if rid not in keep:
                continue
            rank = (saved_at.get(rid), rid)
            prev = best.get(combo)
            if prev is None or rank > prev[0]:
                best[combo] = (rank, rid)
    return {combo: rid for combo, (_rank, rid) in best.items()}


def _expected_locations(
    db, name: str, fn_registry: dict | None, grid: dict
) -> tuple[set, Basis, list[str]]:
    """The denominator: where a record *should* exist.

    Union over every function recorded as producing ``name``:

    * a **PathInput-only loader** enumerates via discovery
      (``check_pathinput_node_state``'s should-run set). This is the half that
      closes the partial-loader hole for this surface — see
      ``.claude/plan-pathinput-loader-staleness-gap.md``. Note it does not (and
      must not) change ``check_node_state``, so the canvas badge is unaffected;
    * anything else uses ``expected_invocations_for_function``, the same live
      derivation the canvas's red/green uses, so the two cannot disagree.

    A variable with **no producing function at all** (raw, manual, MATLAB direct
    save) has no derivable expected set. Its denominator falls back to where it
    already has records, and a note says so — ``n/n`` there means "nothing knows
    better", not "complete".
    """
    from .foreach_config import function_hash_for
    from .inspect.graph import _stored_function_hashes

    duck = db._duck
    notes: list[str] = []
    fns = _producing_functions(duck, name)
    if not fns:
        notes.append(
            f"{name} has no producing function recorded (raw, manual, or a direct "
            f"save), so there is no source for what *should* exist here. The total "
            f"counts only the locations that already hold a record."
        )
        return set(), "present_only", notes

    stored = _stored_function_hashes(duck)
    expected: set = set()
    bases: set = set()

    for fn_name in sorted(fns):
        if provenance_query.is_inputless_function(duck, fn_name):
            combos, note = _discovery_expected(db, fn_name, grid)
            if note:
                notes.append(note)
            if combos is None:
                bases.add("present_only")
                continue
            expected |= combos
            bases.add("discovery")
            continue

        fn_obj = (fn_registry or {}).get(fn_name)
        if fn_obj is not None:
            fn_hash = function_hash_for(fn_obj)
        elif fn_name in stored:
            fn_hash = stored[fn_name]
        else:
            notes.append(
                f"No stored function hash for {fn_name}; its expected locations "
                f"could not be derived."
            )
            bases.add("present_only")
            continue

        pairs = provenance_query.expected_invocations_for_function(
            db, fn_name, fn_hash
        )
        sids = {sid for _inv, sid in pairs if sid is not None}
        expected |= set(_combos_for_schema_ids(db, sids).values())
        bases.add("expected")

    if not expected:
        return set(), "present_only", notes
    if bases == {"expected"}:
        return expected, "expected", notes
    if bases == {"discovery"}:
        return expected, "discovery", notes
    return expected, "mixed", notes


def _superseded_locations(duck, present: dict) -> set:
    """Locations whose current record was computed from inputs that have since
    been re-saved. One closure build for the whole tree."""
    if not present:
        return set()
    flags = provenance_query.superseded_batch(duck, list(present.values()))
    return {combo for combo, rid in present.items() if flags.get(rid)}


def _excluded_locations(db, universe: set, schema_keys: list[str]) -> set:
    """Locations the user has deliberately excluded, with a reason.

    Uses ``exclusions.filter_excluded_combos`` — the same wildcard/specificity
    resolution ``for_each`` applies — rather than re-reading the overrides table,
    so an exclusion means here exactly what it means to a run.
    """
    if not universe:
        return set()
    from .exclusions import filter_excluded_combos

    combos = [dict(c) for c in universe]
    kept = filter_excluded_combos(combos, schema_keys, db)
    kept_keys = {tuple(sorted(c.items())) for c in kept}
    excluded = {c for c in universe if tuple(sorted(dict(c).items())) not in kept_keys}
    if excluded:
        Log.debug(
            f"location_states: {len(excluded)} location(s) excluded and therefore "
            f"counted in neither the numerator nor the denominator",
        )
    return excluded


def _code_versions(duck, present: dict) -> dict:
    """``{combo: "v2"}`` for locations whose producing function has more than one
    recorded version.

    This is what carries a body edit, in place of a colour (see the module
    docstring). Absent for single-version functions, matching
    ``code_version_ordinals``' rule that those are not an axis.
    """
    if not present:
        return {}
    rids = list(present.values())
    produced = provenance_query.producing_function_versions_batch(duck, rids)
    if not produced:
        return {}
    ordinals = provenance_query.code_version_ordinals(
        duck, {info["fn_name"] for info in produced.values()}
    )
    out: dict = {}
    for combo, rid in present.items():
        info = produced.get(rid)
        if info is None:
            continue
        label = ordinals.get(info["fn_name"], {}).get(info["fn_hash"])
        if label:
            out[combo] = label
    return out


# ---------------------------------------------------------------------------
# Graph shape helpers
# ---------------------------------------------------------------------------


def _producing_functions(duck, variable_name: str) -> set:
    """Every real function recorded as producing this variable type.

    Excludes the synthetic ``__save__`` anchor and empty-hash invocations,
    matching every other function-enumerating query in ``provenance_query``.
    Glue chains need no exclusion here: a glue invocation's output record is
    typed ``__glue__``, so the ``r.type = variable_name`` filter has already
    dropped it.
    """
    rows = duck._fetchall(
        "SELECT DISTINCT inv.function_name FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "WHERE r.type = ? AND inv.function_name <> ? AND inv.function_hash <> ''",
        [variable_name, SAVE_FUNCTION_NAME],
    )
    return {row[0] for row in rows}


def pathinput_configs(duck, fn_name: str) -> list[tuple[dict, dict]]:
    """``[(inputs, constants)]`` reconstructed from the stored ``__pathinput__``
    specs of ``fn_name`` — one entry per distinct (spec set, constants) config.

    ``inputs`` is shaped for :func:`scidb.state.check_pathinput_node_state`: the
    constants plus a live :class:`scifor.PathInput` per PathInput parameter. The
    spec is stored, so this works standalone — but ``discover()`` walks the
    *current* filesystem, so it must run where the data folders are reachable.

    Shared with ``Inspector.pathinput_state``, which had the only copy of this
    reconstruction.
    """
    from scifor import PathInput

    from .inspect.graph import parse_path_input

    inv_rows = duck._fetchall(
        "SELECT invocation_id FROM _invocation WHERE function_name = ?", [fn_name]
    )
    configs: dict = {}
    for (inv_id,) in inv_rows:
        specs = provenance_query.invocation_path_inputs(duck, inv_id)
        if not specs:
            continue
        _var_inputs, constants = provenance_query.invocation_inputs(duck, inv_id)
        key = (
            tuple(sorted(specs.items())),
            tuple(sorted((k, repr(v)) for k, v in constants.items())),
        )
        configs.setdefault(key, (specs, constants))

    out: list[tuple[dict, dict]] = []
    for specs, constants in configs.values():
        inputs: dict = dict(constants)
        for param, spec in specs.items():
            info = parse_path_input(spec)
            if info is None:
                raise ValueError(
                    f"Unparseable PathInput spec for {fn_name}.{param}: {spec!r}"
                )
            inputs[param] = (
                PathInput(info["template"], root_folder=info["root_folder"])
                if info.get("root_folder")
                else PathInput(info["template"])
            )
        out.append((inputs, constants))
    return out


def _discovery_expected(db, fn_name: str, grid: dict) -> tuple[set | None, str | None]:
    """The should-run set of a PathInput-only loader, as location combos.

    ``None`` (plus a note) when the loader records no PathInput at all — a
    constant-only function over no grid has nothing to enumerate, and guessing
    would be worse than saying so.
    """
    from .state import check_pathinput_node_state

    duck = db._duck
    try:
        configs = pathinput_configs(duck, fn_name)
    except ValueError as exc:
        return None, str(exc)
    if not configs:
        return None, (
            f"{fn_name} takes no variable inputs and records no PathInput, so the "
            f"locations it should produce cannot be enumerated."
        )

    def _stub():  # check_pathinput_node_state only reads fn.__name__
        pass

    _stub.__name__ = fn_name

    combos: set = set()
    for inputs, _constants in configs:
        res = check_pathinput_node_state(_stub, [], inputs, db=db, **grid)
        for entry in res["combos"]:
            combos.add(_as_combo(entry["schema_combo"], db.dataset_schema_keys))
    Log.info(
        f"location_states: {fn_name} is a PathInput-only loader — {len(combos)} "
        f"location(s) enumerated by discovery rather than by what it has already "
        f"produced (closes the partial-loader gap for this view)",
    )
    return combos, None


# ---------------------------------------------------------------------------
# Combos and tree building
# ---------------------------------------------------------------------------


def _as_combo(values: dict, schema_keys) -> tuple:
    """A hashable location: ``(("subject","01"),("trial","03"))``, in schema
    order, NULL/absent keys omitted (so a non-contiguous save is a shorter
    tuple rather than one carrying holes)."""
    from .database import _schema_str

    return tuple(
        (k, _schema_str(values[k]))
        for k in schema_keys
        if values.get(k) is not None
    )


def _combos_for_schema_ids(db, schema_ids) -> dict:
    """``{schema_id: combo}`` in one query — the batch form of
    ``state._schema_id_to_combo``."""
    sids = [int(s) for s in schema_ids if s is not None]
    if not sids:
        return {}
    schema_keys = list(db.dataset_schema_keys)
    cols = ", ".join(f'"{k}"' for k in schema_keys)
    rows = provenance_query._chunked_in(
        db._duck,
        f"SELECT schema_id, {cols} FROM _schema WHERE schema_id IN ({{ph}})",
        sids,
    )
    out: dict = {}
    for row in rows:
        sid, values = row[0], row[1:]
        mapping = dict(zip(schema_keys, values, strict=False))
        out[int(sid)] = _as_combo(mapping, schema_keys)
    return out


def _schema_ids_by_combo(db) -> dict:
    """``{combo: schema_id}`` for every row of ``_schema``, in one query.

    A location that has never been saved to has no ``_schema`` row and is simply
    absent — which is why the tree is keyed on combos and carries ``schema_id``
    as an annotation rather than the other way round.
    """
    schema_keys = list(db.dataset_schema_keys)
    if not schema_keys or not db._duck._table_exists("_schema"):
        return {}
    cols = ", ".join(f'"{k}"' for k in schema_keys)
    out: dict = {}
    for row in db._duck._fetchall(f"SELECT schema_id, {cols} FROM _schema"):
        mapping = dict(zip(schema_keys, row[1:], strict=False))
        out[_as_combo(mapping, schema_keys)] = int(row[0])
    return out


def _build_tree(
    leaf_states: dict, present: dict, versions: dict, schema_ids: dict
) -> tuple[list[LocationNode], dict[str, int]]:
    """Assemble the nested tree and roll the counts up.

    Roll-up rules (``schema-location-status.md`` §"Roll-up"):

    * a parent's counts are the sum over the leaves beneath it;
    * its state is the **worst** state present beneath it, with grey ignored
      unless it is all there is;
    * a parent with no non-grey descendants is itself grey — green over an empty
      denominator says "all of nothing is fine", which is not an answer.
    """
    nodes: dict[tuple, LocationNode] = {}
    roots: list[LocationNode] = []

    def node_at(path: tuple) -> LocationNode:
        if path in nodes:
            return nodes[path]
        key, value = path[-1]
        node = LocationNode(
            key=key,
            value=value,
            state="grey",
            counts={"green": 0, "amber": 0, "red": 0, "grey": 0},
            is_leaf=False,
            path=path,
            schema_id=schema_ids.get(path),
        )
        nodes[path] = node
        if len(path) == 1:
            roots.append(node)
        else:
            node_at(path[:-1]).children.append(node)
        return node

    for combo, state in leaf_states.items():
        if not combo:
            continue
        node = node_at(combo)
        node.is_leaf = True
        node.state = state
        node.record_id = present.get(combo)
        node.code_version = versions.get(combo)

    def roll(node: LocationNode) -> dict[str, int]:
        if node.is_leaf and not node.children:
            node.counts = {"green": 0, "amber": 0, "red": 0, "grey": 0}
            node.counts[node.state] = 1
            return node.counts
        totals = {"green": 0, "amber": 0, "red": 0, "grey": 0}
        # A node that is both a leaf (a record saved at this level) and a parent
        # (deeper records beneath it) counts itself too — non-contiguous schemas
        # make that ordinary, not exotic.
        if node.is_leaf:
            totals[node.state] += 1
        for child in node.children:
            for k, v in roll(child).items():
                totals[k] += v
        node.counts = totals
        node.state = _worst(totals)
        return totals

    grand = {"green": 0, "amber": 0, "red": 0, "grey": 0}
    for root in roots:
        for k, v in roll(root).items():
            grand[k] += v

    def sort_rec(children: list[LocationNode]):
        children.sort(key=lambda n: (n.key, n.value))
        for c in children:
            sort_rec(c.children)

    sort_rec(roots)
    return roots, grand


def _worst(counts: dict[str, int]) -> LocationState:
    live = {k: v for k, v in counts.items() if k != "grey" and v}
    if not live:
        return "grey"
    return max(live, key=lambda k: _SEVERITY[k])  # type: ignore[return-value]
