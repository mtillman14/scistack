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

    from scihist import check_node_state

    result = check_node_state(bandpass_filter, outputs=[FilteredSignal])
    print(result["state"])    # "green" | "grey" | "red"
    for combo in result["combos"]:
        print(combo["schema_combo"], combo["state"])
"""

import json
import logging
from typing import Literal

logger = logging.getLogger(__name__)

ComboState = Literal["up_to_date", "stale", "missing"]
NodeState = Literal["green", "grey", "red"]


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
    from scilineage import LineageFcn
    from scidb.foreach_config import _compute_fn_hash

    if db is None:
        from scidb.database import get_database
        db = get_database()

    if not isinstance(fn, LineageFcn):
        fn = LineageFcn(fn)

    combo_str = _combo_str(schema_combo, branch_params)

    # Step 1: all outputs must have a record for this combo.
    # Pass branch_params separately so namespaced keys (e.g. "fn.param") go
    # through the suffix-matching path rather than the version_keys filter,
    # which would fail because version_keys stores un-namespaced param names.
    output_record_id = None
    output_timestamp = None
    for OutputCls in outputs:
        rid = db.find_record_id(OutputCls, schema_combo, branch_params_filter=branch_params or None)
        if rid is None:
            logger.debug("missing: %s — no output record for %s", combo_str, OutputCls.__name__)
            return "missing"
        output_record_id = rid

    # Fetch the output record's timestamp (needed for fallback path).
    ts_rows = db._duck._fetchall(
        "SELECT timestamp FROM _record_metadata WHERE record_id = ? "
        "ORDER BY timestamp DESC LIMIT 1",
        [output_record_id],
    )
    output_timestamp = ts_rows[0][0] if ts_rows else None

    # --- Priority 1: lineage-based check (scihist.for_each outputs) ---
    stored_lineage_hash = db.get_function_hash_for_record(output_record_id)
    if stored_lineage_hash is not None:
        return _check_via_lineage(fn, db, output_record_id, stored_lineage_hash, combo_str)

    # --- Priority 2: version_keys __fn_hash fallback (scidb.for_each outputs) ---
    return _check_via_fn_hash(fn, db, output_record_id, output_timestamp,
                               schema_combo, combo_str)


def _check_via_lineage(fn, db, output_record_id: str, stored_hash: str,
                        combo_str: str) -> ComboState:
    """Staleness check using full scihist lineage records.

    Uses exact record_ids for input freshness — no timestamps needed.
    """
    # a. Function hash.
    if stored_hash != fn.hash:
        logger.debug("stale: %s — function hash changed (lineage)", combo_str)
        return "stale"

    # b. Walk upstream provenance; check each input record_id is still current.
    try:
        nodes = db.get_upstream_provenance(output_record_id)
    except Exception as e:
        logger.debug("stale: %s — provenance lookup failed: %s", combo_str, e)
        return "stale"

    for node in nodes:
        lineage_inputs = db.get_lineage_inputs(node["record_id"])
        for inp in lineage_inputs:
            source_type = inp.get("source_type")
            if source_type not in ("variable", "rid_tracking"):
                continue
            used_rid = inp.get("record_id")
            if not used_rid:
                continue
            current_rid = db.get_latest_record_id_for_variant(used_rid)
            if current_rid != used_rid:
                var_type = inp.get("type") or inp.get("name") or "unknown"
                logger.debug(
                    "stale: %s — upstream %s updated (was %s, now %s)",
                    combo_str, var_type, used_rid, current_rid,
                )
                return "stale"

    logger.debug("up_to_date: %s (lineage)", combo_str)
    return "up_to_date"


def _check_via_fn_hash(fn, db, output_record_id: str, output_timestamp: str | None,
                        schema_combo: dict, combo_str: str) -> ComboState:
    """Staleness check using __fn_hash from version_keys + record_id/timestamp for inputs.

    Used when the output was saved via scidb.for_each (no lineage record).

    Input freshness priority:
    1. __upstream record_ids (preferred): exact record_id comparison per variant,
       avoids false "stale" when new records are added for a *different* constant
       variant of the same input type.
    2. Timestamp comparison (fallback): used only when __upstream is absent.
       This is less precise — it compares against the MAX timestamp across ALL
       records of the input type at the schema_id, regardless of variant.
    """
    from scidb.foreach_config import _compute_fn_hash

    # Read version_keys from the output record.
    vk_rows = db._duck._fetchall(
        "SELECT version_keys FROM _record_metadata WHERE record_id = ? LIMIT 1",
        [output_record_id],
    )
    if not vk_rows:
        logger.debug("stale: %s — could not read version_keys", combo_str)
        return "stale"

    vk = json.loads(vk_rows[0][0] or "{}") if vk_rows[0][0] else {}
    stored_fn_hash = vk.get("__fn_hash")

    # a. Function hash check.
    if stored_fn_hash is None:
        # Pre-Phase-0 record: no hash stored, cannot verify function identity.
        logger.warning(
            "up_to_date (unverified): %s — no __fn_hash in version_keys "
            "(record predates Phase 0; function staleness cannot be checked)",
            combo_str,
        )
    else:
        current_hash = _compute_fn_hash(fn.fcn if hasattr(fn, "fcn") else fn)
        if stored_fn_hash != current_hash:
            logger.debug("stale: %s — function hash changed (__fn_hash)", combo_str)
            return "stale"

    # b. Input freshness via __upstream record_ids (preferred path).
    # __upstream stores the exact record_ids of the inputs that were used.
    # get_latest_record_id_for_variant checks whether a newer record now exists
    # for the same (variable_name, schema_id, version_keys) — i.e., the same
    # variant.  This is variant-precise: records added for a different constant
    # variant of the same type do not trigger staleness here.
    upstream_raw = vk.get("__upstream")
    if upstream_raw:
        upstream: dict = json.loads(upstream_raw) if isinstance(upstream_raw, str) else (upstream_raw or {})
        for rid_col, used_rid in upstream.items():
            if not used_rid:
                continue
            current_rid = db.get_latest_record_id_for_variant(used_rid)
            if current_rid != used_rid:
                logger.debug(
                    "stale: %s — upstream %s updated (was %s, now %s)",
                    combo_str, rid_col, used_rid, current_rid,
                )
                return "stale"
        logger.debug("up_to_date: %s (__fn_hash + __upstream record_ids)", combo_str)
        return "up_to_date"

    # c. Fallback: timestamp comparison when __upstream is absent.
    # For each input variable type referenced in __inputs, find the latest
    # record at the same schema_id. If that record was saved after the output,
    # the output is stale.  Note: this is variant-unaware and may produce false
    # positives when multiple variants of the same input type exist.
    if output_timestamp is None:
        logger.debug("up_to_date (unverified): %s — no output timestamp available", combo_str)
        return "up_to_date"

    inputs_raw = vk.get("__inputs", "{}")
    input_types_map: dict = json.loads(inputs_raw) if isinstance(inputs_raw, str) else {}

    schema_id_rows = db._duck._fetchall(
        "SELECT schema_id FROM _record_metadata WHERE record_id = ? LIMIT 1",
        [output_record_id],
    )
    if not schema_id_rows:
        return "up_to_date"
    output_schema_id = schema_id_rows[0][0]

    for itype in input_types_map.values():
        latest_ts_rows = db._duck._fetchall(
            "SELECT MAX(timestamp) FROM _record_metadata "
            "WHERE variable_name = ? AND schema_id = ? AND excluded = FALSE",
            [itype, output_schema_id],
        )
        if not latest_ts_rows or latest_ts_rows[0][0] is None:
            continue
        latest_input_ts = latest_ts_rows[0][0]
        if latest_input_ts > output_timestamp:
            logger.debug(
                "stale: %s — upstream %s re-saved after output (timestamp fallback)",
                combo_str, itype,
            )
            return "stale"

    logger.debug("up_to_date: %s (__fn_hash + timestamp)", combo_str)
    return "up_to_date"


def check_node_state(
    fn,
    outputs: list[type],
    inputs: dict | None = None,
    db=None,
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

    Returns:
        A dict with keys:

        ``"state"`` (:data:`NodeState`)
            Overall node state:

            - ``"green"``  — every expected combo is up_to_date.
            - ``"grey"``   — some combos up_to_date, some missing (partially run).
            - ``"red"``    — never run, or any combo is stale.

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

    # --- Actual combos: output records produced by this function ---
    output_combos = _get_output_combos(db, fn_name, outputs)

    # --- Expected combos: (schema_id, branch_params) from input variables ---
    # Using full branch_params (not just schema_id) so that a new upstream
    # variant (e.g. window_seconds=90 added after the function was last run)
    # is detected as missing even when all schema_ids are already covered by
    # other variants.
    expected_combos = _get_expected_combos(db, fn_name, inputs_fallback=inputs)

    # --- Determine missing combos ---
    actual_combo_keys = {
        (c["schema_id"], json.dumps(c["branch_params"], sort_keys=True))
        for c in output_combos
    }
    missing_combo_keys = expected_combos - actual_combo_keys

    # --- Check each actual combo ---
    counts: dict[str, int] = {"up_to_date": 0, "stale": 0, "missing": 0}
    combo_results: list[dict] = []

    for combo_info in output_combos:
        schema_combo = _schema_id_to_combo(db, combo_info["schema_id"])
        bp = combo_info["branch_params"]
        state = check_combo_state(fn, outputs, schema_combo, branch_params=bp or None, db=db)
        counts[state] += 1
        combo_results.append({
            "schema_combo": schema_combo,
            "branch_params": bp,
            "state": state,
        })

    for schema_id, bp_json in missing_combo_keys:
        schema_combo = _schema_id_to_combo(db, schema_id)
        bp = json.loads(bp_json)
        counts["missing"] += 1
        combo_results.append({
            "schema_combo": schema_combo,
            "branch_params": bp,
            "state": "missing",
        })

    # --- Aggregate to node state ---
    if not combo_results:
        # No output records and no expected inputs — function never run and
        # no input data exists yet.
        overall: NodeState = "red"
    elif counts["stale"] > 0:
        overall = "red"
    elif counts["missing"] > 0 and counts["up_to_date"] == 0:
        overall = "red"
    elif counts["missing"] > 0:
        overall = "grey"
    else:
        overall = "green"

    logger.debug(
        "node %s: %s (up_to_date=%d, stale=%d, missing=%d)",
        fn_name, overall, counts["up_to_date"], counts["stale"], counts["missing"],
    )

    return {
        "state": overall,
        "combos": combo_results,
        "counts": counts,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _combo_str(schema_combo: dict, branch_params: dict | None = None) -> str:
    parts = [f"{k}={v}" for k, v in sorted(schema_combo.items())]
    if branch_params:
        parts += [f"{k}={v}" for k, v in sorted(branch_params.items())]
    return ", ".join(parts)


def _get_output_combos(db, fn_name: str, outputs: list[type]) -> list[dict]:
    """Return distinct (schema_id, branch_params) pairs from output records
    produced by fn_name.

    Two sources identify a record as belonging to fn_name:

    - ``_record_metadata.version_keys.__fn`` matches (scidb.for_each outputs).
    - ``_lineage.function_name`` matches (scihist.for_each outputs, which do
      not write ``__fn`` into version_keys but instead write a row to the
      dedicated ``_lineage`` table).
    """
    result: list[dict] = []
    seen: set = set()

    for OutputCls in outputs:
        rows = db._duck._fetchall(
            "SELECT DISTINCT rm.schema_id, rm.branch_params, rm.version_keys, "
            "       l.function_name "
            "FROM _record_metadata rm "
            "LEFT JOIN _lineage l ON rm.record_id = l.output_record_id "
            "WHERE rm.variable_name = ? AND rm.excluded = FALSE",
            [OutputCls.__name__],
        )
        for schema_id, bp_raw, vk_raw, lineage_fn_name in rows:
            vk = json.loads(vk_raw or "{}") if vk_raw else {}
            vk_fn = vk.get("__fn")
            # Match if either the version_keys __fn or the _lineage function_name
            # identifies this record as produced by fn_name.
            if vk_fn != fn_name and lineage_fn_name != fn_name:
                continue
            bp = json.loads(bp_raw or "{}") if bp_raw else {}
            key = (schema_id, json.dumps(bp, sort_keys=True))
            if key not in seen:
                seen.add(key)
                result.append({"schema_id": schema_id, "branch_params": bp})

    return result


def _get_expected_combos(db, fn_name: str, inputs_fallback: dict | None = None) -> set[tuple]:
    """Return the set of (schema_id, branch_params_json) combos that should have
    been produced by fn_name.

    For each variant of fn_name, queries its input variable types to find all
    (schema_id, input_branch_params) combinations that exist in the DB.

    Two sources of variants are consulted:

    - ``list_pipeline_variants`` (scidb.for_each outputs).  Their output
      branch_params = input_branch_params + own constants namespaced as
      ``fn_name.param`` (matching how scidb.for_each builds them).
    - ``_lineage`` rows for fn_name (scihist.for_each outputs).  These do
      not namespace constants — output branch_params = input_branch_params
      merged with own constants un-namespaced.

    Using (schema_id, branch_params) rather than schema_id alone lets us detect
    when a new upstream variant (e.g. a new constant value) exists in the inputs
    but hasn't been processed by fn_name yet, even if all schema_ids are already
    covered by other variants.

    When no variants are registered from either source (function never run) and
    ``inputs_fallback`` is provided, falls back to querying the input variable
    types directly.
    """
    scidb_variants = [v for v in db.list_pipeline_variants() if v["function_name"] == fn_name]
    lineage_variants = _get_lineage_variants(db, fn_name)

    if not scidb_variants and not lineage_variants:
        if inputs_fallback:
            return _get_expected_combos_from_inputs(db, inputs_fallback)
        return set()

    expected: set[tuple] = set()
    fn_prefix = f"{fn_name}."

    # scidb variants: own constants are namespaced in the output's branch_params.
    for variant in scidb_variants:
        input_types: dict = variant["input_types"]    # param_name → type_name
        own_constants: dict = variant["constants"]    # un-namespaced direct constants
        namespaced_own = {f"{fn_prefix}{k}": v for k, v in own_constants.items()}

        for itype in input_types.values():
            rows = db._duck._fetchall(
                "SELECT DISTINCT schema_id, branch_params FROM _record_metadata "
                "WHERE variable_name = ? AND excluded = FALSE",
                [itype],
            )
            for schema_id, bp_raw in rows:
                input_bp = json.loads(bp_raw or "{}") if bp_raw else {}
                expected_bp = {**input_bp, **namespaced_own}
                expected.add((schema_id, json.dumps(expected_bp, sort_keys=True)))

    # _lineage variants: own constants are NOT namespaced in the output's
    # branch_params (scihist.for_each merges them in directly).
    for variant in lineage_variants:
        input_types = variant["input_types"]
        own_constants = variant["constants"]

        for itype in input_types.values():
            rows = db._duck._fetchall(
                "SELECT DISTINCT schema_id, branch_params FROM _record_metadata "
                "WHERE variable_name = ? AND excluded = FALSE",
                [itype],
            )
            for schema_id, bp_raw in rows:
                input_bp = json.loads(bp_raw or "{}") if bp_raw else {}
                expected_bp = {**input_bp, **own_constants}
                expected.add((schema_id, json.dumps(expected_bp, sort_keys=True)))

    return expected


def _get_lineage_variants(db, fn_name: str) -> list[dict]:
    """Extract variants for fn_name from the ``_lineage`` table.

    Returns a list of dicts with:
        ``input_types``  (dict: param_name → type_name) — variable inputs only.
        ``constants``    (dict: param_name → value) — direct constants from
                         lineage records, excluding any that are actually
                         variable inputs (see below).

    Pulls distinct ``(inputs, constants)`` JSON pairs from ``_lineage`` rows
    whose ``function_name`` equals ``fn_name``.  Used for scihist.for_each
    outputs, which write to ``_lineage`` but not to ``version_keys.__fn``.

    Variable input detection — three sources, in priority order:

    1. ``inputs`` entries with ``source_type == "variable"`` (rare for scihist
       since the @lineage_fcn receives raw numpy arrays, not BaseVariables —
       those get classified as CONSTANT by scilineage).
    2. ``inputs`` entries with ``source_type == "rid_tracking"`` (added by
       scihist.for_each via :func:`_append_rid_tracking`).  The entry name
       is ``__rid_<param>``; the variable type is recovered by looking up
       ``record_id`` in ``_record_metadata.variable_name``.
    3. ``constants`` entries whose name appears among the rid_tracking
       params are stripped — they were variable inputs misclassified by
       scilineage as constants.
    """
    rows = db._duck._fetchall(
        "SELECT DISTINCT inputs, constants FROM _lineage WHERE function_name = ?",
        [fn_name],
    )

    variants: list[dict] = []
    seen: set = set()
    for inputs_json, constants_json in rows:
        try:
            inputs_list = json.loads(inputs_json or "[]")
        except (json.JSONDecodeError, TypeError):
            inputs_list = []
        try:
            constants_list = json.loads(constants_json or "[]")
        except (json.JSONDecodeError, TypeError):
            constants_list = []

        input_types: dict = {}

        # Source 1: explicit variable entries.
        for inp in inputs_list:
            if not isinstance(inp, dict):
                continue
            if inp.get("source_type") != "variable":
                continue
            name = inp.get("name")
            type_name = inp.get("type")
            if name and type_name:
                input_types[name] = type_name

        # Source 2: rid_tracking entries — recover variable type via lookup.
        for inp in inputs_list:
            if not isinstance(inp, dict):
                continue
            if inp.get("source_type") != "rid_tracking":
                continue
            rid_name = inp.get("name") or ""
            record_id = inp.get("record_id")
            if not rid_name.startswith("__rid_") or not record_id:
                continue
            param_name = rid_name[len("__rid_"):]
            if param_name in input_types:
                continue
            vn_rows = db._duck._fetchall(
                "SELECT variable_name FROM _record_metadata "
                "WHERE record_id = ? LIMIT 1",
                [record_id],
            )
            if vn_rows and vn_rows[0][0]:
                input_types[param_name] = vn_rows[0][0]

        # Direct constants: name → value_repr (used to disambiguate variants).
        constants: dict = {}
        if isinstance(constants_list, list):
            for c in constants_list:
                if isinstance(c, dict) and "name" in c:
                    constants[c["name"]] = c.get("value_repr")

        # Source 3: strip names that are actually variable inputs.
        for name in input_types:
            constants.pop(name, None)

        if not input_types and not constants:
            continue

        key = (
            json.dumps(input_types, sort_keys=True),
            json.dumps(constants, sort_keys=True),
        )
        if key in seen:
            continue
        seen.add(key)
        variants.append({"input_types": input_types, "constants": constants})

    return variants


def _get_expected_combos_from_inputs(db, inputs: dict) -> set[tuple]:
    """Fallback for _get_expected_combos when no pipeline variants are registered.

    Extracts variable type names from the inputs dict (same format as for_each)
    and queries the DB for all (schema_id, branch_params) combos of those types.
    """
    expected: set[tuple] = set()
    for value in inputs.values():
        if not isinstance(value, type):
            continue
        type_name = value.__name__
        rows = db._duck._fetchall(
            "SELECT DISTINCT schema_id, branch_params FROM _record_metadata "
            "WHERE variable_name = ? AND excluded = FALSE",
            [type_name],
        )
        for schema_id, bp_raw in rows:
            bp = json.loads(bp_raw or "{}") if bp_raw else {}
            expected.add((schema_id, json.dumps(bp, sort_keys=True)))
    return expected


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

    return {k: v for k, v in zip(schema_keys, rows[0]) if v is not None}
