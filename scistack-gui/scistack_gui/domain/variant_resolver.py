"""
Pure variant resolution, deduplication, and pending-constant merging.

Builds the list of for_each targets from DB variants, manual edges, and
pending constant values. No I/O — works entirely on plain Python data.
"""

from __future__ import annotations

import ast
import logging
from itertools import product as _product

logger = logging.getLogger(__name__)


def build_inferred_variants(
    input_types: dict[str, list[str]],
    output_types: list[str],
    inferred_constants: dict[str, list],
) -> list[dict]:
    """Build synthetic variants from edge-inferred inputs/outputs/constants.

    Used when a function has no DB history yet (first run).

    Args:
        input_types: {param_name: [variable_type_names]}.
        output_types: List of output variable type names.
        inferred_constants: {const_name: [typed_values]} — cross-product is taken.

    Returns:
        List of variant dicts with input_types, output_type, constants.
    """
    logger.info(
        "[variant_resolver] build_inferred_variants: building variants from %d input(s), %d output(s), %d constant(s)",
        len(input_types),
        len(output_types),
        len(inferred_constants),
    )

    if inferred_constants:
        const_names_list = sorted(inferred_constants.keys())
        const_value_lists = [inferred_constants[c] for c in const_names_list]
        logger.debug(
            "[variant_resolver] computing cross-product of %d constant(s)",
            len(const_names_list),
        )
        variants = []
        for combo in _product(*const_value_lists):
            constants = dict(zip(const_names_list, combo, strict=False))
            for out in output_types:
                variants.append(
                    {
                        "input_types": input_types,
                        "output_type": out,
                        "constants": constants,
                    }
                )
        logger.info(
            "[variant_resolver] build_inferred_variants complete: built %d variant(s) from cross-product",
            len(variants),
        )
        return variants
    else:
        variants = [
            {"input_types": input_types, "output_type": out, "constants": {}}
            for out in output_types
        ]
        logger.info(
            "[variant_resolver] build_inferred_variants complete: built %d variant(s) (no constants)",
            len(variants),
        )
        return variants


def filter_variants(
    fn_variants: list[dict],
    selected_variants: list[dict],
) -> list[dict]:
    """Filter fn_variants to only those matching any of the selected variants.

    Falls back to all fn_variants if no match is found.
    """
    logger.info(
        "[variant_resolver] filter_variants: filtering %d variant(s) using %d selected variant(s)",
        len(fn_variants),
        len(selected_variants),
    )
    targets = [
        v
        for v in fn_variants
        if any(constants_match(v["constants"], sel) for sel in selected_variants)
    ]
    if not targets:
        logger.debug(
            "[variant_resolver] filter_variants: no match for selected=%r — returning all %d variants",
            selected_variants,
            len(fn_variants),
        )
    else:
        logger.info(
            "[variant_resolver] filter_variants complete: %d variant(s) matched",
            len(targets),
        )
    return targets if targets else fn_variants


def deduplicate_variants(targets: list[dict]) -> list[dict]:
    """Deduplicate variants by their constants dict.

    list_pipeline_variants may return duplicates across different output types
    for the same function.
    """
    logger.info(
        "[variant_resolver] deduplicate_variants: deduplicating %d variant(s)",
        len(targets),
    )
    seen: set[tuple] = set()
    unique: list[dict] = []
    for v in targets:
        key = tuple(sorted(v["constants"].items()))
        if key not in seen:
            seen.add(key)
            unique.append(v)
    duplicates_removed = len(targets) - len(unique)
    if duplicates_removed > 0:
        logger.debug("[variant_resolver] removed %d duplicate(s)", duplicates_removed)
    logger.info(
        "[variant_resolver] deduplicate_variants complete: %d unique variant(s)",
        len(unique),
    )
    return unique


def merge_pending_constants(
    fn_variants: list[dict],
    pending_constants: dict[str, set[str]],
) -> list[dict]:
    """Add synthetic targets for pending constant values not yet in the DB.

    For each pending value, cross-products with existing combinations of all
    other constants. The pending value itself is stored as a string, so we
    coerce it back to a Python literal where possible.

    Args:
        fn_variants: Current list of variant dicts (may be mutated list from
            deduplicate_variants).
        pending_constants: {constant_name: {pending_value_str, ...}}.

    Returns:
        Extended list of unique variant targets (appends to the input list).
    """
    logger.info(
        "[variant_resolver] merge_pending_constants: merging pending constants into %d variant(s)",
        len(fn_variants),
    )

    if not fn_variants or not pending_constants:
        logger.debug(
            "[variant_resolver] no variants or pending constants, skipping merge"
        )
        return fn_variants

    fn_const_names = {k for v in fn_variants for k in v["constants"]}
    pending_for_fn = {
        k: vals for k, vals in pending_constants.items() if k in fn_const_names
    }

    if not pending_for_fn:
        logger.debug(
            "[variant_resolver] no pending constants match function's constant parameters"
        )
        return fn_variants

    logger.info(
        "[variant_resolver] adding pending values for %d constant(s): %s",
        len(pending_for_fn),
        sorted(pending_for_fn),
    )

    existing_keys = {
        tuple(sorted((k, str(v)) for k, v in t["constants"].items()))
        for t in fn_variants
    }
    template = fn_variants[0]
    initial_variant_count = len(fn_variants)

    for const_name, pending_values in pending_for_fn.items():
        logger.debug(
            "[variant_resolver] processing %d pending value(s) for constant '%s'",
            len(pending_values),
            const_name,
        )
        # Collect unique combinations of other constants (typed).
        other_seen: set[tuple] = set()
        other_combos: list[dict] = []
        for v in fn_variants:
            other = {k: val for k, val in v["constants"].items() if k != const_name}
            okey = tuple(sorted((k, str(val)) for k, val in other.items()))
            if okey not in other_seen:
                other_seen.add(okey)
                other_combos.append(other)

        for pval_str in pending_values:
            pval = _coerce(pval_str)
            for other in other_combos:
                new_constants = dict(other)
                new_constants[const_name] = pval
                key = tuple(sorted((k, str(v)) for k, v in new_constants.items()))
                if key not in existing_keys:
                    existing_keys.add(key)
                    fn_variants.append(
                        {
                            # Carry the template's whole binding set: a pending
                            # combo of an existing call site is the SAME wiring
                            # with a different constant value, so it must hash
                            # to the same wiring/call identity.
                            "bindings": template.get("bindings", {}),
                            "input_types": template.get("input_types", {}),
                            "constants": new_constants,
                            "output_type": template["output_type"],
                        }
                    )

    added_variant_count = len(fn_variants) - initial_variant_count
    logger.info(
        "[variant_resolver] merge_pending_constants complete: added %d variant(s), total %d",
        added_variant_count,
        len(fn_variants),
    )
    return fn_variants


def build_schema_kwargs(
    schema_level: list[str] | None,
    all_schema_keys: list[str],
    schema_filter: dict[str, list] | None,
    distinct_values: dict[str, list],
) -> dict[str, list]:
    """Build the schema kwargs dict for for_each.

    Args:
        schema_level: Which schema keys to iterate; None = all.
        all_schema_keys: All schema keys from the DB.
        schema_filter: {key: [selected values]}; None = all.
        distinct_values: {key: [all_values]} from db.distinct_schema_values.

    Returns:
        {schema_key: [values_to_iterate]}.
    """
    logger.info(
        "[variant_resolver] build_schema_kwargs: building schema kwargs for iteration"
    )
    iterate_keys = schema_level if schema_level is not None else list(all_schema_keys)
    logger.debug(
        "[variant_resolver] iterating over %d schema key(s): %s",
        len(iterate_keys),
        iterate_keys,
    )

    if schema_filter:
        logger.debug(
            "[variant_resolver] applying schema filter with %d key(s)",
            len(schema_filter),
        )
        schema_kwargs = {}
        for key in iterate_keys:
            if key in schema_filter and schema_filter[key]:
                schema_kwargs[key] = schema_filter[key]
                logger.debug(
                    "[variant_resolver] schema key '%s': using %d filtered value(s)",
                    key,
                    len(schema_filter[key]),
                )
            else:
                schema_kwargs[key] = distinct_values.get(key, [])
                logger.debug(
                    "[variant_resolver] schema key '%s': using %d distinct value(s)",
                    key,
                    len(distinct_values.get(key, [])),
                )
        logger.info(
            "[variant_resolver] build_schema_kwargs complete: %d schema key(s) configured",
            len(schema_kwargs),
        )
        return schema_kwargs
    else:
        logger.debug("[variant_resolver] no schema filter, using all distinct values")
        schema_kwargs = {key: distinct_values.get(key, []) for key in iterate_keys}
        logger.info(
            "[variant_resolver] build_schema_kwargs complete: %d schema key(s) configured",
            len(schema_kwargs),
        )
        return schema_kwargs


def constants_match(db_constants: dict, selected: dict) -> bool:
    """True if selected is a subset of db_constants (value equality as strings)."""
    return all(str(db_constants.get(k)) == str(v) for k, v in selected.items())


def _path_input_version_key(declared_name: str) -> "str | None":
    """The live PathInput's ``to_key()`` — the same string scidb writes into
    ``__inputs``. None when the declaration no longer exists in source."""
    from scistack_gui import registry

    pi = registry.get_path_inputs_registry().get(declared_name)
    if pi is None:
        return None
    return pi.to_key()


def compute_call_id(
    function_name: str,
    target: dict,
    distribute: bool = False,
    as_table=None,
) -> str | None:
    """Deterministic call_id for a target, matching scidb's real
    ForEachConfig.to_version_keys() shape exactly (see
    scidb.foreach_config), so a combo hidden before it's ever run lands on
    the same id as the real record it eventually produces if it is run.

    Returns None (fail-safe: "unknown, don't filter") for a target with an
    unresolved multi-type input (EachOf) — there's no single call site to
    hash yet. Hiding a specific combo is scoped to constant-value axes only
    (see plan-combo-hiding.md).

    **PathInputs belong in ``__inputs``.** scidb's
    ``ForEachConfig._serialize_inputs`` puts them there via their own
    ``to_key()``, explicitly so two templates cannot collapse into one
    version-key group. Reading only the variable bindings here meant the
    predicted id for every PathInput-fed function differed from the id scidb
    actually wrote — so a combo hidden before its first run was hidden under
    an id no record would ever carry. The key comes from the live PathInput
    object so there is exactly one spelling of the recipe, scidb's.
    """
    from scidb.foreach_config import call_id_from_version_keys

    from scistack_gui.domain.edge_resolver import BINDING_PATHINPUT, BINDING_VARIABLE

    inputs: dict = {}
    across_variants: list = []
    for param, binding in (target.get("bindings") or {}).items():
        kind = binding.get("kind")
        if kind == BINDING_VARIABLE:
            type_val = binding["ref"]
            if isinstance(type_val, list):
                if len(type_val) != 1:
                    return None
                inputs[param] = type_val[0]
            else:
                inputs[param] = type_val
            if binding.get("pool_variants"):
                # scidb's ForEachConfig writes `__across_variants` beside
                # `__as_table` (a run option, not an input type).
                across_variants.append(param)
        elif kind == BINDING_PATHINPUT:
            pi_key = _path_input_version_key(binding["ref"])
            if pi_key is None:
                # Declared PathInput is gone from source — we cannot
                # reproduce scidb's key, so fail safe rather than hash a
                # value we know is wrong.
                logger.debug(
                    "[variant_resolver] no call_id for '%s': PathInput %r is no "
                    "longer declared in source",
                    function_name,
                    binding["ref"],
                )
                return None
            inputs[param] = pi_key
        # BINDING_PARAMETER contributes nothing: its concrete values travel
        # in __constants, exactly as in scidb's ForEachConfig.

    keys: dict = {
        "__fn": function_name,
        "__inputs": inputs,
        "__constants": dict(target.get("constants", {})),
    }
    if distribute:
        keys["__distribute"] = True
    if as_table:
        keys["__as_table"] = sorted(as_table) if isinstance(as_table, list) else True
    if across_variants:
        keys["__across_variants"] = sorted(across_variants)
    return call_id_from_version_keys(keys)


def hidden_call_ids_for_fn(hidden_node_ids: set[str], function_name: str) -> set[str]:
    """Hidden ``fn__{function_name}__{call_id}`` ids for one function, as
    bare call_ids (the id shape hiding a whole node already uses — see
    graph_builder.filter_hidden)."""
    from scistack_gui.domain.graph_builder import parse_fn_node_id

    out: set[str] = set()
    for nid in hidden_node_ids:
        parsed = parse_fn_node_id(nid)
        if parsed is not None and parsed[0] == function_name:
            out.add(parsed[1])
    return out


def resolve_target_call_id(
    function_name: str,
    target: dict,
    pending_constant_names: set[str],
    distribute: bool = False,
    as_table=None,
) -> str | None:
    """A target's EFFECTIVE call_id — reuses its real DB-history ``call_id``
    directly, except when ``apply_pending_overrides`` may have changed its
    identity (any of its constants share a name with a staged pending
    value), in which case it's never safe to trust a possibly-stale
    ``call_id`` field and it's recomputed fresh via ``compute_call_id``.
    """
    touched = bool(pending_constant_names & set(target.get("constants", {})))
    cid = target.get("call_id") if (not touched and target.get("call_id")) else None
    if cid is None:
        cid = compute_call_id(function_name, target, distribute=distribute, as_table=as_table)
    return cid


def filter_hidden_targets(
    targets: list[dict],
    function_name: str,
    hidden_call_ids: set[str],
    pending_constants: dict,
    distribute: bool = False,
    as_table=None,
) -> list[dict]:
    """Drop targets whose effective call_id (see ``resolve_target_call_id``)
    is hidden."""
    if not hidden_call_ids:
        return targets
    pending_names = set(pending_constants or {})
    kept = []
    for t in targets:
        cid = resolve_target_call_id(
            function_name, t, pending_names, distribute=distribute, as_table=as_table
        )
        if cid is not None and cid in hidden_call_ids:
            continue
        kept.append(t)
    return kept


def is_hidden_value(value, hidden_for_name) -> bool:
    """Whether *value* is one of the unchecked values for its parameter.

    Compared as strings, because the hidden-value store keeps them that way
    (it is fed by the node checkbox, whose rows are rendered strings) while a
    Parameter holds real numbers. Numeric values are ALSO matched against
    their int/float alternate spelling. A value declared 20 can reach here
    as ``20.0`` (JSON has one number type, the ``/api/parameters`` model
    accepts ``float | int``, and DB history may round-trip differently), and
    a plain ``str()`` comparison would then never match a hidden ``'20'`` --
    the checkbox would appear to do nothing.

    Lives here, in the pure domain layer, because BOTH routes a Parameter
    can take to execution have to apply the identical rule:
    ``filter_hidden_constant_value_targets`` below (values fanned out into
    targets) and ``execution_service._apply_hidden_values`` (a Parameter
    handed to ``for_each`` whole). It used to be private to the latter, and
    the former did a naive ``str()`` compare — so the same checkbox worked
    on one route and silently did nothing on the other, depending only on
    whether the value happened to be an int or a float.
    """
    candidates = {str(value)}
    if isinstance(value, bool):
        pass  # bool is an int subclass; its str() form is the only sane one
    elif isinstance(value, float) and value.is_integer():
        candidates.add(str(int(value)))
    elif isinstance(value, int):
        candidates.add(str(float(value)))
    return bool(candidates & set(hidden_for_name))


def filter_hidden_constant_value_targets(
    targets: list[dict],
    hidden_values: dict[str, set[str]],
) -> list[dict]:
    """Drop targets whose constants include any hidden ``(name, value)``
    pair — the ``ConstantNode.tsx`` checkbox's effect on execution.

    Coarser than ``filter_hidden_targets``' per-call_id combo hiding: a
    hidden constant value excludes every target across every function that
    uses it, not one function's one Cartesian-product row.

    Unlike ``filter_hidden_targets``, no call_id hashing is involved — a
    target's hidden-ness is fully determined by which constants it already
    carries, so it's a direct content match against ``hidden_values``. This
    also means it's automatically correct for combos that have never run:
    ``derive_fn_targets``/``derive_target_for_node`` already materialize
    never-run pending/inferred combos as real target dicts (via
    ``execution_service._infer_wired_constants``) before this filter ever
    runs, so there's no separate "hidden but not yet materialized" case to
    special-case here.

    **The two names must be translated, not compared directly.**
    ``hidden_values`` is keyed by the Parameter node's DECLARED name (the
    per-value checkbox writes that), while ``constants`` is keyed by the
    FUNCTION PARAMETER the value was bound to. They coincide only when the
    declaration happens to be named after the parameter it feeds; when they
    differ, comparing them directly silently matches nothing and every
    unchecked value runs anyway. The target's Parameter bindings
    (``{param_name: declared_name}``, put on the target by the wiring) are
    the translation, defaulting to the param name for DB-history targets
    where the two are the same string by construction.

    Value matching is ``is_hidden_value``'s, shared with
    ``execution_service._apply_hidden_values`` — a naive ``str()`` compare
    here would make the checkbox work on one route and not the other purely
    on whether the value arrived as ``5`` or ``5.0``.
    """
    if not hidden_values:
        return targets
    from scistack_gui.domain.edge_resolver import BINDING_PARAMETER, bindings_of_kind

    kept = []
    for t in targets:
        constants = t.get("constants", {})
        declared_of = bindings_of_kind(t.get("bindings") or {}, BINDING_PARAMETER)
        if any(
            is_hidden_value(value, hidden_values.get(declared_of.get(param, param), ()))
            for param, value in constants.items()
        ):
            continue
        kept.append(t)
    return kept


def reconcile_manual_inputs(
    targets: list[dict],
    function_name: str,
    hidden_edge_ids: "set[str] | None" = None,
    manual_edges: "list[dict] | tuple" = (),
    manual_nodes: "dict[str, dict] | None" = None,
) -> list[dict]:
    """Reconcile each target's RECORDED wiring with the manual edges the
    user has drawn onto its node, in one pass per target.

    Two things happen here, both keyed on the target's history wiring id
    (function name + variable input/output types — not constants, see
    graph_builder.wiring_id), which is the id a manual edge's ``target``
    names (graph_builder.manual_edge_handle_index):

    1. **Drop** a target whose wiring has a user-hidden required inbound edge
       (graph_builder.hide_edge) that is NOT covered by a manual reconnect.
       Unlike ``filter_hidden_targets`` (one constant-value combo at a
       time), this considers every hidden handle on the wiring, since a
       missing required input makes the WHOLE wiring un-runnable. A target
       with even one hidden handle NOT covered by a manual edge is dropped
       entirely — partial reconnection doesn't make a wiring runnable.

    2. **Substitute** the bindings that a manual variable edge is
       authoritative for (graph_builder.manual_input_overrides — the one
       owner of that rule, shared with the display overlay): a hidden handle
       the user reconnected to a different variable, AND a parameter history
       never bound at all (added to the signature after the recorded runs).
       The original DB target's recorded input is stale for both — that
       historical call was never run with the new variable — so re-admitting
       it unchanged would run the wrong thing. Any stale ``call_id`` is
       dropped so it gets recomputed from the substituted bindings
       (compute_call_id), and ``input_types`` is refreshed so the wire view
       never drifts from the bindings it is a view of.

    ONE pass, not two, on purpose: substituting changes the wiring id, so a
    second pass keyed on the new id would miss both the remaining hidden
    edges (re-admitting a partially reconnected wiring) and any further
    manual edge on the same node.

    Each execution-service target IS its own call site, so this checks
    candidate inbound edge ids directly per target rather than going through
    graph_builder.hidden_wirings' multi-call-site grouping (that path is for
    the GUI graph endpoint, which has a full agg).
    """
    hidden_edge_ids = hidden_edge_ids or set()
    if not targets or (not hidden_edge_ids and not manual_edges):
        return targets
    from scistack_gui.domain.edge_resolver import (
        BINDING_PATHINPUT,
        BINDING_VARIABLE,
        bindings_of_kind,
        variable_types_view,
    )
    from scistack_gui.domain.graph_builder import (
        inbound_edge_candidates_by_handle,
        manual_edge_handle_index,
        manual_input_overrides,
        wiring_id,
    )

    manual_index = manual_edge_handle_index(manual_edges)
    manual_nodes = manual_nodes or {}
    kept = []
    for t in targets:
        bindings = t.get("bindings") or {}
        # Bare-string-when-single: wiring_id hashes the value as written, and
        # DB history spells single types bare.
        input_types = variable_types_view(bindings)
        const_names = list((t.get("constants") or {}).keys())
        wid = wiring_id(
            function_name,
            input_types,
            {t.get("output_type")},
            bindings_of_kind(bindings, BINDING_PATHINPUT),
        )
        handle_map = inbound_edge_candidates_by_handle(
            function_name, wid, input_types, const_names=const_names
        )
        hidden_handles = {h for cid_, h in handle_map.items() if cid_ in hidden_edge_ids}
        uncovered = [h for h in hidden_handles if (function_name, wid, h) not in manual_index]
        if uncovered:
            logger.debug(
                "[variant_resolver] target for '%s' (wiring %s) stays excluded — "
                "hidden handle(s) %s not covered by a manual edge",
                function_name,
                wid,
                sorted(uncovered),
            )
            continue

        overrides = manual_input_overrides(
            function_name,
            wid,
            input_types,
            const_names,
            manual_index,
            manual_nodes,
            hidden_edge_ids,
        )
        if not overrides:
            kept.append(t)
            continue

        new_bindings = dict(bindings)
        for param, sources in overrides.items():
            # ``ref`` is always a list; >1 entry is EachOf (edge_resolver.
            # variable_binding) — a manual edge beside a visible history edge.
            ref = list(sources) if isinstance(sources, list) else [sources]
            new_bindings[param] = {"kind": BINDING_VARIABLE, "ref": ref}
        new_target = {**t, "bindings": new_bindings}
        if "input_types" in t:
            # Keep the display/wire view in step with the bindings it is a
            # view OF. Leaving it stale is how the two drifted apart in the
            # first place.
            new_target["input_types"] = variable_types_view(new_bindings)
        new_target.pop("call_id", None)
        logger.info(
            "[variant_resolver] target for '%s' (wiring %s): manual edge(s) "
            "override %s — substituting bindings=%s (call_id recomputed)",
            function_name,
            wid,
            overrides,
            new_bindings,
        )
        kept.append(new_target)
    return kept


def _coerce(s: str):
    """Coerce a string to a Python literal if possible."""
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return s
