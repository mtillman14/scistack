"""
Pipeline node staleness API.

Provides per-combo and per-node run state queries that use the full lineage
provenance graph rather than simple record-count approximations.

Staleness check priority (most to least authoritative):

1. **Lineage record exists** (scihist.for_each output):
   - Function staleness: stored ``function_hash`` vs current ``LineageFcn.hash``.
   - Input staleness: stored input ``record_id`` vs current latest record_id.
   - No timestamps used.

2. **No lineage, but ``__fn_hash`` in version_keys** (scidb.for_each output):
   - Function staleness: stored ``__fn_hash`` vs ``_compute_fn_hash(fn)``.
   - Input staleness: output record timestamp vs latest input record timestamp
     at the same schema_id.  Timestamps used only here, as the minimum
     unavoidable fallback when exact input record_ids are unavailable.

Typical usage::

    from scidb import check_node_state

    result = check_node_state(bandpass_filter, outputs=[FilteredSignal])
    print(result["state"])    # "green" | "red"
    for combo in result["combos"]:
        print(combo["schema_combo"], combo["state"])
"""

import time
import logging
from typing import Literal

logger = logging.getLogger(__name__)

ComboState = Literal["up_to_date", "stale", "missing"]
# Node state is BINARY: a node is either fully computed-and-current ("green") or
# needs attention ("red"). "grey"/partial was removed — "needs attention" is one
# state regardless of whether the node never ran, ran partially, has a re-saved
# input, or had its function edited.
NodeState = Literal["green", "red"]


def check_combo_state(
    fn,
    outputs: list[type],
    schema_combo: dict,
    branch_params: dict | None = None,
    db=None,
) -> ComboState:
    """Check the staleness of a single (function, schema_combo) pair.

    Args:
        fn: The pipeline function (plain callable or LineageFcn).
        outputs: List of output variable classes produced by fn.
        schema_combo: Dict of schema key → value identifying the specific
            data location, e.g. ``{"subject": 1, "session": "pre"}``.
        branch_params: Optional constants dict to disambiguate which variant
            to check when multiple variants exist for the same schema_combo,
            e.g. ``{"bandpass_filter.low_hz": 20}``.
        db: DatabaseManager instance.  Uses the global DB if omitted.

    Returns:
        ``"up_to_date"``  — output exists and full upstream provenance is unchanged.
        ``"stale"``       — output exists but upstream has changed (input record
                           updated or function code changed).
        ``"missing"``     — no output record exists for this combo.
    """
    if db is None:
        from scidb.database import get_database

        db = get_database()

    combo_str = _combo_str(schema_combo, branch_params)

    # Step 1: all outputs must have a record for this combo.
    # Pass branch_params separately so namespaced keys (e.g. "fn.param") go
    # through the suffix-matching path rather than the version_keys filter,
    # which would fail because version_keys stores un-namespaced param names.
    output_record_id = None
    for OutputCls in outputs:
        rid = db.find_record_id(
            OutputCls, schema_combo, branch_params_filter=branch_params or None
        )
        if rid is None:
            logger.debug(
                "missing: %s — no output record for %s", combo_str, OutputCls.__name__
            )
            return "missing"
        output_record_id = rid

    # Staleness over the bipartite provenance graph (records produced by
    # for_each). A record with no producing invocation is raw/manual — there is
    # no function or input to be stale against, so it is up_to_date.
    from . import provenance_query

    sig = provenance_query.stored_invocation_signature(db._duck, output_record_id)
    if sig is None:
        logger.debug("up_to_date: %s — raw record (no producing invocation)", combo_str)
        return "up_to_date"
    return _check_via_graph(fn, db, output_record_id, sig, combo_str)


def _check_via_graph(
    fn, db, output_record_id: str, sig: dict, combo_str: str
) -> ComboState:
    """Staleness check over the bipartite provenance graph.

    ``sig`` is the producing invocation's signature
    (``provenance_query.stored_invocation_signature``). A descendant is stale if
    its own function hash changed, or if ANY ancestor record_id in its
    provenance has been superseded — cascading data changes through arbitrarily
    deep chains and DAG shapes (fork/join).

    Scope (see docs/guide/node-states.md, "Propagation"):

    - ✅ Ancestor data re-saved (record_id superseded) → stale.
    - ✅ Python fn's own function hash mismatched → stale (Python ``LineageFcn``
      only; MATLAB proxies use a different hashing pipeline that can produce
      false mismatches — see ``.claude/defer-function-hash-staleness.md``).
    - ❌ Ancestor function code changed but not yet re-run → NOT detected here
      (only ``fn`` itself is passed in). The GUI DAG walk handles that, or the
      user re-runs the changed ancestor (creating a new record_id that cascades).
    """
    from scidb.foreach_config import function_hash_for

    # Function's own code changed since the output was saved?
    # The graph stores ``function_hash`` = the ``__fn_hash`` the save path
    # wrote, so compare against the same recipe (``function_hash_for``).
    # ``trusts_hash`` still excludes objects carrying their own ``.hash`` from a
    # different pipeline; a ``MatlabLineageFcn`` now also exposes ``source_hash``,
    # which IS the stored recipe, so its comparison is meaningful even though
    # this branch does not act on it (see .claude/defer-function-hash-staleness.md).
    trusts_hash = not hasattr(fn, "hash")
    stored_hash = sig.get("function_hash")
    current_hash = function_hash_for(fn)
    if stored_hash is not None and stored_hash != current_hash:
        if trusts_hash:
            logger.debug(
                "stale: %s — function hash changed: stored=%s current=%s",
                combo_str,
                stored_hash[:12],
                current_hash[:12],
            )
            return "stale"
        logger.debug(
            "function hash differs for %s (non-Python fn): stored=%s current=%s "
            "— not treated as stale",
            combo_str,
            stored_hash[:12],
            current_hash[:12],
        )

    # Deep walk: is ANY ancestor record_id superseded?
    if _has_superseded_ancestor(db, output_record_id, combo_str):
        return "stale"

    logger.debug("up_to_date: %s (graph, deep walk clean)", combo_str)
    return "up_to_date"


def _has_superseded_ancestor(
    db, record_id: str, combo_str: str, visited: set | None = None, max_depth: int = 50
) -> bool:
    """BFS across the bipartite provenance graph from ``record_id`` backwards.

    Returns True as soon as an ancestor record is found whose latest
    variant-version differs from the record_id referenced as a variable input of
    its producing invocation — i.e., something upstream has been re-saved since
    the descendant was computed.

    ``visited`` guards against cycles; ``max_depth`` bounds cost on pathological
    graphs (matches ``get_upstream_provenance`` default × 2).
    """
    from . import provenance_query

    if visited is None:
        visited = set()

    queue: list[tuple[str, int]] = [(record_id, 0)]
    while queue:
        current_rid, depth = queue.pop(0)
        if current_rid in visited or depth > max_depth:
            continue
        visited.add(current_rid)

        inv = provenance_query.producing_invocation(db._duck, current_rid)
        if inv is None:
            continue  # raw/manual record — terminus, nothing to supersede
        var_inputs, _constants = provenance_query.invocation_inputs(db._duck, inv[0])

        for inp in var_inputs:
            used_rid = inp.get("record_id")
            if not used_rid:
                continue
            if inp.get("glue_chain"):
                # A virtual glue record is never "re-saved": it has no
                # _record_save row, so both latest-lookups below would return
                # None and read as superseded. Its own supersession is the
                # supersession of the record it wraps — keep walking.
                queue.append((used_rid, depth + 1))
                continue
            current_latest = db.get_latest_record_id_for_variant(used_rid)
            if current_latest != used_rid:
                logger.debug(
                    "stale: %s — upstream %s at depth %d superseded (was %s, now %s)",
                    combo_str,
                    inp.get("variable_type", "unknown"),
                    depth + 1,
                    used_rid,
                    current_latest,
                )
                return True
            # Also catch direct .save() updates at the same (variable_name,
            # schema_id) that don't go through an invocation.
            latest_any = _get_latest_record_at_location(db, used_rid)
            if latest_any is not None and latest_any != used_rid:
                logger.debug(
                    "stale: %s — upstream %s at depth %d superseded by different "
                    "variant (was %s, now %s)",
                    combo_str,
                    inp.get("variable_type", "unknown"),
                    depth + 1,
                    used_rid,
                    latest_any,
                )
                return True
            queue.append((used_rid, depth + 1))

    return False


def check_multiple_nodes_state(
    nodes: list[dict],
    fn_registry: dict | None = None,
    db=None,
) -> dict[str, dict]:
    """Check run state for multiple function nodes in a single call.

    Optimizes database access by sharing the connection across all node checks.
    Useful for GUI graph building where many nodes need state computation.

    Args:
        nodes: List of dicts with keys:
            - ``fn`` or ``fn_name`` (callable or str): The function object or name
            - ``call_id`` (str): 16-hex-char call site identifier
            - ``outputs`` (list[type]): Output variable classes
            When ``fn`` is not provided, ``fn_name`` is looked up in ``fn_registry``.
        fn_registry: Optional dict mapping function names to function objects.
            Used when nodes specify ``fn_name`` instead of ``fn``.
        db: DatabaseManager instance. Uses the global DB if omitted.

    Returns:
        Dict mapping node_id (``fn__{fn_name}__{call_id}``) to state result:
        {
            "state": "green" | "red",
            "counts": {"up_to_date": N, "stale": N, "missing": N},
        }

    Example:
        >>> nodes = [
        ...     {"fn": process_emg, "call_id": "abc123", "outputs": [FilteredEMG]},
        ...     {"fn": compute_stats, "call_id": "def456", "outputs": [Stats]},
        ... ]
        >>> states = check_multiple_nodes_state(nodes, db=db)
        >>> states["fn__process_emg__abc123"]["state"]
        'green'
    """
    if db is None:
        from scidb.database import get_database

        db = get_database()

    result: dict[str, dict] = {}

    for node in nodes:
        # Get function object
        fn = node.get("fn")
        if fn is None:
            fn_name = node.get("fn_name")
            if fn_name is None:
                logger.warning(
                    "check_multiple_nodes_state: node missing both 'fn' and 'fn_name', skipping"
                )
                continue
            if fn_registry is None:
                logger.warning(
                    "check_multiple_nodes_state: fn_name=%r but no fn_registry provided, skipping",
                    fn_name,
                )
                continue
            fn = fn_registry.get(fn_name)
            if fn is None:
                # Function not in registry — cannot run state check, mark as red
                fn_name_safe = fn_name
                call_id = node.get("call_id", "")
                node_id = f"fn__{fn_name_safe}__{call_id}"
                result[node_id] = {
                    "state": "red",
                    "counts": {"up_to_date": 0, "stale": 0, "missing": 0},
                }
                continue
        else:
            fn_name = getattr(fn, "__name__", None) or type(fn).__name__

        outputs = node.get("outputs", [])
        call_id = node.get("call_id")

        if not outputs:
            # No outputs specified — mark as red
            node_id = f"fn__{fn_name}__{call_id or ''}"
            result[node_id] = {
                "state": "red",
                "counts": {"up_to_date": 0, "stale": 0, "missing": 0},
            }
            continue

        # Call check_node_state for this node
        try:
            state_result = check_node_state(fn, outputs, db=db, call_id=call_id)
            node_id = f"fn__{fn_name}__{call_id or ''}"
            result[node_id] = {
                "state": state_result["state"],
                "counts": state_result.get(
                    "counts", {"up_to_date": 0, "stale": 0, "missing": 0}
                ),
            }
        except Exception:
            logger.exception(
                "check_multiple_nodes_state: check_node_state failed for %s call_id=%s — marking as red",
                fn_name,
                call_id,
            )
            node_id = f"fn__{fn_name}__{call_id or ''}"
            result[node_id] = {
                "state": "red",
                "counts": {"up_to_date": 0, "stale": 0, "missing": 0},
            }

    logger.debug(
        "check_multiple_nodes_state: checked %d nodes, %d results",
        len(nodes),
        len(result),
    )

    return result


def check_node_state(
    fn,
    outputs: list[type],
    inputs: dict | None = None,
    db=None,
    call_id: str | None = None,
    glue: dict | None = None,
) -> dict:
    """Aggregate run state across all known combos for a pipeline function.

    Enumerates combos by comparing:
    - *actual* combos: output records in the DB whose version_keys.__fn matches fn.
    - *expected* combos: schema_ids present in the input variables for each variant.

    Combos in actual → checked via :func:`check_combo_state` (up_to_date or stale).
    Combos in expected but absent from actual → "missing".

    Args:
        fn: The pipeline function (plain callable or LineageFcn).
        outputs: List of output variable classes produced by fn.
        inputs: Optional dict mapping parameter names to input variable types
            (same format as ``for_each``'s ``inputs``).  Used as a fallback to
            determine expected combos when the function has never been run and
            no pipeline variants are registered in the DB.
        db: DatabaseManager instance.  Uses the global DB if omitted.
        call_id: Optional 16-hex-char identifier for a specific for_each call
            site (see :func:`scidb.foreach_config.call_id_from_version_keys`).
            When provided, both actual and expected combos are restricted to
            records produced by that call site.  Allows the same function to
            be reused across multiple call sites without their states
            blurring together.  When omitted, behaves as the union across
            all call sites.
        glue: Optional ``{param: [GlueSpec, ...]}`` glue chains attached to
            this call (same value passed to ``for_each``).  Only affects the
            never-run fallback prediction built from ``inputs``: a glued param
            binds to a virtual glue record, so predicting from the raw records
            would report the node red forever.  Configs recovered from the
            graph already carry their glue.

    Returns:
        A dict with keys:

        ``"state"`` (:data:`NodeState`)
            Overall node state (binary):

            - ``"green"`` — the node has expected work and every expected combo
              is present (fully computed and current).
            - ``"red"``   — anything else: never run, partially run, an input was
              re-saved but not re-run, or the function was edited. Any missing
              expected invocation makes the whole node red.

        ``"combos"`` (list of dict)
            Per-combo breakdown.  Each entry has:
            ``schema_combo`` (dict), ``branch_params`` (dict), ``state`` (ComboState).

        ``"counts"`` (dict)
            ``{"up_to_date": N, "stale": N, "missing": N}``.
    """
    if db is None:
        from scidb.database import get_database

        db = get_database()

    fn_name = getattr(fn, "__name__", None) or type(fn).__name__

    # Node completeness = invocation membership (§9c). Expected invocation_ids
    # are derived LIVE from current input data over each variant config the
    # function has run with (plus the declared-inputs fallback); "present" =
    # those in _invocation. There is no persisted snapshot — `_for_each_expected`
    # was removed because the predicted-vs-realized id pair was a drift hazard.
    # A zero-input function (PathInput-only loader) has no DB input to predict
    # from, so this step alone can never report it partially run — its expected
    # set IS what it has produced. `_discovery_gate` below closes that, using
    # the filesystem as the live source it does have.
    # "stale" collapses into "missing": a changed input or edited function shifts
    # the EXPECTED id, so the old one drops out of the expected set and the new
    # (absent) one shows as needs-run (see §9c / §10.4). ``call_id`` scopes the
    # expected set to one call site's variant config (config_call_id matching)
    # — invocation_id alone is config-specific, but the EXPECTED-set union
    # across configs is not: without the scope, one call site's partial run
    # reddens every other call site of the same function.
    from scidb.foreach_config import function_hash_for

    from . import provenance_query

    # Must reproduce the hash the save path STORED — see function_hash_for.
    # This used to AST-hash `fn.fcn` unconditionally, which for a MATLAB
    # function is a bare name-holder: the resulting hash matched nothing ever
    # written, so every MATLAB node with variable inputs was permanently red.
    fn_hash = function_hash_for(fn)
    expected = provenance_query.expected_invocations_for_function(
        db,
        fn_name,
        fn_hash,
        inputs_fallback=inputs,
        call_id=call_id,
        glue_fallback=glue,
    )
    present = provenance_query.present_invocation_schema_pairs(
        db._duck,
        {inv_id for inv_id, _sid in expected},
    )

    counts: dict[str, int] = {"up_to_date": 0, "stale": 0, "missing": 0}
    combo_results: list[dict] = []
    for inv_id, schema_id in expected:
        state: ComboState = (
            "up_to_date" if (inv_id, schema_id) in present else "missing"
        )
        counts[state] += 1
        combo_results.append(
            {
                "schema_combo": _schema_id_to_combo(db, schema_id),
                "branch_params": {},
                "state": state,
            }
        )

    # --- The PathInput-only loader gate ---
    # For a function with no database inputs the expected set above is derived
    # from what it has ALREADY produced, so `missing` can never exceed zero and
    # a partially-run loader reads green. Discovery is the one live source for
    # what should exist; `_discovery_gate` consults it, guards against a walk
    # that cannot resolve, and only ever adds. See its docstring.
    for combo in _discovery_gate(fn_name, db, realized_count=len(combo_results)):
        counts["missing"] += 1
        combo_results.append(
            {"schema_combo": combo, "branch_params": {}, "state": "missing"}
        )

    # --- Aggregate to node state (binary: green | red) ---
    # green iff the node has expected work AND all of it is present; red otherwise
    # (never run / no input data, partially run, input re-saved but not re-run, or
    # edited function — all leave >=1 expected invocation missing).
    if combo_results and counts["missing"] == 0:
        overall: NodeState = "green"
    else:
        overall = "red"

    # "Red" has several causes that look identical from outside — never run,
    # partially run, an input re-saved, or the body edited. Say which, at INFO,
    # because the edited-body case is new and otherwise indistinguishable from
    # "this node has simply never worked".
    reason = ""
    if overall == "red":
        prior = provenance_query.function_versions_recorded(db._duck, fn_name)
        if not combo_results and prior and fn_hash not in prior:
            reason = (
                f" — the function's source has changed since it last ran "
                f"(now {fn_hash[:12]}, recorded {[h[:12] for h in sorted(prior)]}); "
                f"re-run to bring it up to date"
            )
        elif not combo_results:
            reason = " — nothing expected yet (never run, or no input data)"
        else:
            reason = f" — {counts['missing']} expected invocation(s) not present"
        logger.info("node %s: red%s", fn_name, reason)
    else:
        logger.debug(
            "node %s: green (up_to_date=%d)", fn_name, counts["up_to_date"]
        )

    return {
        "state": overall,
        "combos": combo_results,
        "counts": counts,
    }


# ---------------------------------------------------------------------------
# Discovery gate for PathInput-only loaders (the canvas half of the loader gap)
# ---------------------------------------------------------------------------

#: How long a loader's filesystem walk is reused.
#:
#: ``check_node_state`` runs on every canvas refresh, and a refresh arrives in
#: bursts (a scope switch redraws every node at once). ``PathInput.discover()``
#: is an uncached recursive directory walk — a very different cost profile from
#: the DuckDB queries around it, and the first risk
#: ``.claude/plan-pathinput-loader-staleness-gap.md`` lists.
#:
#: A few seconds collapses a burst into one walk while keeping the thing a user
#: actually does — drop a file in, refresh, see red — responsive. It is
#: deliberately short rather than event-invalidated: the filesystem changes
#: behind our back by definition, so any invalidation hook would be a guess
#: about when, and a stale green is exactly the bug being fixed.
DISCOVERY_CACHE_SECONDS = 5.0

#: ``{(fn_name, spec_key): (monotonic_deadline, combos)}``
_discovery_cache: dict = {}


def clear_discovery_cache() -> None:
    """Forget every cached filesystem walk. For tests, and after a run."""
    _discovery_cache.clear()


def _discovered_combos(fn_name: str, db) -> tuple[int, list[dict]] | None:
    """``(on_disk_count, never_run_combos)`` for a PathInput-only loader, cached.

    ``None`` means "do not act on this": the function records no PathInput, so
    there is nothing to enumerate and the caller must fall back to the
    invocation-membership answer.

    The diff is :func:`check_pathinput_node_state`'s, not a second one written
    here. It already computes should-run ∩ grid − exclusions against realized
    locations, and its realized-matching is subset-based (a combo is covered if
    some realized location agrees on all the keys it names), which an exact-key
    comparison here would get subtly wrong for a loader that saves deeper than
    it discovers.
    """
    from .locations import pathinput_configs

    try:
        configs = pathinput_configs(db._duck, fn_name)
    except ValueError as exc:  # an unparseable stored spec
        logger.warning("discovery skipped for %s: %s", fn_name, exc)
        return None
    if not configs:
        return None

    key = (
        fn_name,
        tuple(sorted(str(v) for inputs, _c in configs for v in inputs.values())),
    )
    cached = _discovery_cache.get(key)
    now = time.monotonic()
    if cached is not None and cached[0] > now:
        logger.debug("discovery cache hit for %s", fn_name)
        return cached[1]

    def _stub():  # check_pathinput_node_state only reads fn.__name__
        pass

    _stub.__name__ = fn_name

    started = time.perf_counter()
    on_disk = 0
    missing: list[dict] = []
    for inputs, _constants in configs:
        # No grid: the canvas has no for_each call in hand, so this is pure
        # discovery — every file the template matches, minus exclusions. See
        # the docstring of `_discovery_gate` for why exclusions are the escape
        # hatch rather than a remembered grid.
        result = check_pathinput_node_state(_stub, [], inputs, db=db)
        on_disk += len(result["combos"])
        missing.extend(c["schema_combo"] for c in result["combos"] if c["state"] == "missing")
    elapsed = time.perf_counter() - started

    _discovery_cache[key] = (now + DISCOVERY_CACHE_SECONDS, (on_disk, missing))
    logger.debug(
        "discovery for %s: %d location(s) on disk, %d never run, in %.3fs "
        "(cached %.0fs)",
        fn_name,
        on_disk,
        len(missing),
        elapsed,
        DISCOVERY_CACHE_SECONDS,
    )
    return on_disk, missing


def _discovery_gate(fn_name: str, db, realized_count: int) -> list[dict]:
    """Locations a loader's files imply but that it has never produced.

    **This is the canvas half of the PathInput loader gap.** Without it a
    zero-input function reports green the moment *one* combo exists under the
    current hash: its expected set is derived from what it has already produced
    (``realized_inputless_invocations``), so un-run combos leave no trace and
    ``counts["missing"]`` can never exceed zero. Three real states read green —
    a run that covered only some files, a run that died partway, and new files
    nobody has loaded yet.

    Discovery is the one live source for "what should exist" that a function
    with no database inputs has, so this consults it and reports the shortfall.

    **The credibility guard is the important part.** If discovery finds *nothing*
    while the function has realized outputs, the walk is broken here — a
    ``root_folder`` written with Windows separators and read on POSIX
    (``project_windows_config_paths``), an unmounted drive, a moved data root —
    and the honest answer is "cannot tell", not "every location is missing".
    Turning a whole study red because a path failed to resolve would be far
    worse than the stale green this fixes, so that case falls back to the
    previous behaviour and says so loudly.

    **Only ever adds red.** A shortfall can turn a green node red; nothing here
    can turn a red node green.

    **The escape hatch is exclusions, deliberately.** Pure discovery means a
    file you never intend to load reads red forever. There is no recorded grid
    to consult — ``_run.where_clause`` is display-only by design — so the answer
    is ``exclusions.exclude_schema(reason, ...)``, which
    ``check_pathinput_node_state`` already subtracts and which forces the user
    to write down *why*. That is the same mechanism the location picker uses,
    so the canvas badge and the picker's denominator agree by construction.
    """
    from . import provenance_query

    if not provenance_query.is_inputless_function(db._duck, fn_name):
        return []
    found = _discovered_combos(fn_name, db)
    if found is None:
        return []
    on_disk, shortfall = found

    if on_disk == 0 and realized_count:
        logger.warning(
            "node %s: PathInput discovery found no files at all, but the "
            "function has %d realized location(s) — treating discovery as "
            "unavailable rather than reporting everything missing. Check the "
            "data root is reachable and that scistack.toml's paths resolve on "
            "this OS.",
            fn_name,
            realized_count,
        )
        return []

    if shortfall:
        logger.info(
            "node %s: %d of %d location(s) on disk have never been run "
            "(exclude them with a reason if that is deliberate)",
            fn_name,
            len(shortfall),
            on_disk,
        )
    return shortfall


def check_pathinput_node_state(
    fn,
    outputs: list[type],
    inputs: dict,
    db=None,
    **iteration: list,
) -> dict:
    """Outdated check for a PathInput / constant-only function (no variable inputs).

    A loader whose only inputs are a ``PathInput`` (+ optional constants) has no
    DB-variable input to predict an expected set from, so the generic
    :func:`check_node_state` can only report green-when-run / red-when-never-run
    on its own. This check closes that gap by reconstructing the combos a run
    *would* produce **now** and diffing them against what the loader has
    actually produced.

    Since the discovery gate landed, :func:`check_node_state` calls this for
    every inputless function, so the canvas badge already reflects it. This
    remains the explicit, grid-aware entry point: pass ``**iteration`` to ask
    the question for one declared grid rather than for every file on disk.

    The should-run set is exactly the combos a run would *produce output for* now —
    the **intersection** of the files on disk and the declared grid::

        should = PathInput.discover()  ∩  Cartesian product of `iteration`
               − schema-excluded combos

    i.e. discovered combos restricted to the grid (empty/unspecified grid keys are
    wildcards, so pure-discovery mode keeps every discovered combo). A grid combo
    with no file produces nothing, and a file outside the grid is not iterated — so
    neither is in ``should`` (and neither makes the node red). When there is no
    PathInput at all (pure constants over a grid), ``should`` is just the grid.

    Realized = the schema locations this function has produced output at under the
    **current constants** (graph ground truth, content-addressed match — no
    invocation_id recompute). The node is **red** iff any should-combo is not
    realized (a new in-grid file appeared and hasn't been run); **green** otherwise.
    Adding unwanted new data to the exclusion list drops it from ``should`` and
    flips the node back to green, as if it did not exist.

    Args:
        fn: the pipeline function (plain callable).
        outputs: output variable classes (accepted for signature parity; unused —
            realized locations are read per producing function).
        inputs: the ``for_each`` ``inputs`` dict (PathInput + any constants).
        db: DatabaseManager (global DB if omitted).
        **iteration: the iteration grid (e.g. ``subject=["1", "2"]``) — the same
            metadata_iterables passed to ``for_each``. Empty/omitted keys fall back
            to filesystem discovery, exactly like ``for_each``.

    Returns the same dict shape as :func:`check_node_state`: ``{state, combos, counts}``.
    """
    import itertools

    if db is None:
        from scidb.database import get_database

        db = get_database()
    from scidb.database import _schema_str
    from scidb.exclusions import filter_excluded_combos

    from . import provenance_query
    from .foreach import _find_pathinput
    from .provenance import compute_constant_record_id

    fn_name = getattr(fn, "__name__", None) or type(fn).__name__
    schema_keys = list(db.dataset_schema_keys)

    def _norm(combo: dict) -> dict:
        return {k: _schema_str(v) for k, v in combo.items() if v is not None}

    # --- should-run set: PathInput.discover() ∩ iteration grid, dedup, then exclude.
    # This is exactly what for_each would *produce output for* now: a discovered
    # file only counts if its combo is within the declared grid, and a grid combo
    # only counts if a file exists for it. Unspecified/empty grid keys are
    # wildcards, so pure-discovery mode (no grid) keeps every discovered combo. ---
    should: list[dict] = []
    seen: set = set()

    def _add(combo: dict) -> None:
        c = _norm(combo)
        key = tuple(sorted(c.items()))
        if c and key not in seen:
            seen.add(key)
            should.append(c)

    grid_keys = [k for k, v in iteration.items() if v]
    grid_sets = {k: {_schema_str(x) for x in iteration[k]} for k in grid_keys}

    pi = _find_pathinput(inputs)
    if pi is not None:
        # Discovered combos that satisfy the grid (the intersection).
        for combo in pi.discover():
            c = _norm(combo)
            if all(c.get(k) in grid_sets[k] for k in grid_keys):
                _add(c)
    elif grid_keys:
        # No PathInput (pure constant inputs over a grid): there is no filesystem
        # to intersect with, so the declared grid itself is the should-run set.
        for prod in itertools.product(*[iteration[k] for k in grid_keys]):
            _add(dict(zip(grid_keys, prod, strict=False)))

    should = filter_excluded_combos(should, schema_keys, db)

    # --- realized locations produced under the current constants (graph truth) ---
    cfg = provenance_query.config_from_inputs(inputs)
    const_rids = {p: compute_constant_record_id(v) for p, v in cfg["constants"].items()}
    realized_sids = provenance_query.realized_inputless_schema_ids(
        db._duck,
        fn_name,
        const_rids,
    )
    realized = [_norm(_schema_id_to_combo(db, sid)) for sid in realized_sids]

    def _is_realized(c: dict) -> bool:
        # a should-combo is covered if some realized location agrees on all its keys
        return any(all(r.get(k) == v for k, v in c.items()) for r in realized)

    counts: dict[str, int] = {"up_to_date": 0, "stale": 0, "missing": 0}
    combo_results: list[dict] = []
    for c in should:
        st: ComboState = "up_to_date" if _is_realized(c) else "missing"
        counts[st] += 1
        combo_results.append({"schema_combo": c, "branch_params": {}, "state": st})

    overall: NodeState = (
        "green" if (combo_results and counts["missing"] == 0) else "red"
    )
    logger.debug(
        "pathinput node %s: %s (should=%d, up_to_date=%d, missing=%d)",
        fn_name,
        overall,
        len(should),
        counts["up_to_date"],
        counts["missing"],
    )
    return {"state": overall, "combos": combo_results, "counts": counts}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _combo_str(schema_combo: dict, branch_params: dict | None = None) -> str:
    parts = [f"{k}={v}" for k, v in sorted(schema_combo.items())]
    if branch_params:
        parts += [f"{k}={v}" for k, v in sorted(branch_params.items())]
    return ", ".join(parts)


def _schema_id_to_combo(db, schema_id) -> dict:
    """Convert a schema_id to a dict of schema key → value."""
    schema_keys = db.dataset_schema_keys
    if not schema_keys:
        return {}

    col_select = ", ".join(f'"{k}"' for k in schema_keys)
    rows = db._duck._fetchall(
        f"SELECT {col_select} FROM _schema WHERE schema_id = ?",
        [int(schema_id)],
    )
    if not rows:
        return {}

    return {k: v for k, v in zip(schema_keys, rows[0], strict=False) if v is not None}


def _get_latest_record_at_location(db, record_id: str) -> str | None:
    """Get the latest record_id at the same (variable_name, schema_id),
    ignoring version_keys.

    Used by ``_has_superseded_ancestor`` to detect direct ``.save()``
    updates that don't carry ``__fn`` in version_keys — they would be
    in a different partition from pipeline-produced records and invisible
    to ``get_latest_record_id_for_variant``.
    """
    rows = db._duck._fetchall(
        "SELECT type, schema_id FROM _record WHERE record_id = ? LIMIT 1",
        [record_id],
    )
    if not rows:
        return None
    vn, sid = rows[0]
    # Recency from the save-event log; type/schema/excluded from the _record entity.
    latest = db._duck._fetchall(
        "SELECT rm.record_id FROM _record_save rm "
        "JOIN _record r ON r.record_id = rm.record_id "
        "WHERE r.type = ? AND r.schema_id = ? "
        "AND COALESCE(r.excluded, FALSE) = FALSE "
        "ORDER BY rm.timestamp DESC LIMIT 1",
        [vn, int(sid)],
    )
    if not latest:
        return None
    return latest[0][0]
