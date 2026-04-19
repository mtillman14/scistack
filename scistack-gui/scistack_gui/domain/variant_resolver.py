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
    if inferred_constants:
        const_names_list = sorted(inferred_constants.keys())
        const_value_lists = [inferred_constants[c] for c in const_names_list]
        variants = []
        for combo in _product(*const_value_lists):
            constants = dict(zip(const_names_list, combo))
            for out in output_types:
                variants.append({
                    "input_types": input_types,
                    "output_type": out,
                    "constants": constants,
                })
        return variants
    else:
        return [
            {"input_types": input_types, "output_type": out, "constants": {}}
            for out in output_types
        ]


def filter_variants(
    fn_variants: list[dict],
    selected_variants: list[dict],
) -> list[dict]:
    """Filter fn_variants to only those matching any of the selected variants.

    Falls back to all fn_variants if no match is found.
    """
    targets = [
        v for v in fn_variants
        if any(_constants_match(v["constants"], sel) for sel in selected_variants)
    ]
    if not targets:
        logger.debug(
            "filter_variants: no match for selected=%r — returning all %d variants",
            selected_variants, len(fn_variants),
        )
    return targets if targets else fn_variants


def deduplicate_variants(targets: list[dict]) -> list[dict]:
    """Deduplicate variants by their constants dict.

    list_pipeline_variants may return duplicates across different output types
    for the same function.
    """
    seen: set[tuple] = set()
    unique: list[dict] = []
    for v in targets:
        key = tuple(sorted(v["constants"].items()))
        if key not in seen:
            seen.add(key)
            unique.append(v)
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
    if not fn_variants or not pending_constants:
        return fn_variants

    fn_const_names = {k for v in fn_variants for k in v["constants"]}
    pending_for_fn = {
        k: vals for k, vals in pending_constants.items()
        if k in fn_const_names
    }

    if not pending_for_fn:
        return fn_variants

    logger.debug(
        "merge_pending_constants: adding pending values for constants %s",
        sorted(pending_for_fn),
    )

    existing_keys = {
        tuple(sorted((k, str(v)) for k, v in t["constants"].items()))
        for t in fn_variants
    }
    template = fn_variants[0]

    for const_name, pending_values in pending_for_fn.items():
        # Collect unique combinations of other constants (typed).
        other_seen: set[tuple] = set()
        other_combos: list[dict] = []
        for v in fn_variants:
            other = {k: val for k, val in v["constants"].items()
                     if k != const_name}
            okey = tuple(sorted((k, str(val)) for k, val in other.items()))
            if okey not in other_seen:
                other_seen.add(okey)
                other_combos.append(other)

        for pval_str in pending_values:
            pval = _coerce(pval_str)
            for other in other_combos:
                new_constants = dict(other)
                new_constants[const_name] = pval
                key = tuple(sorted(
                    (k, str(v)) for k, v in new_constants.items()
                ))
                if key not in existing_keys:
                    existing_keys.add(key)
                    fn_variants.append({
                        "input_types": template["input_types"],
                        "constants": new_constants,
                        "output_type": template["output_type"],
                    })

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
    iterate_keys = schema_level if schema_level is not None else list(all_schema_keys)

    if schema_filter:
        schema_kwargs = {}
        for key in iterate_keys:
            if key in schema_filter and schema_filter[key]:
                schema_kwargs[key] = schema_filter[key]
            else:
                schema_kwargs[key] = distinct_values.get(key, [])
        return schema_kwargs
    else:
        return {
            key: distinct_values.get(key, [])
            for key in iterate_keys
        }


def _constants_match(db_constants: dict, selected: dict) -> bool:
    """True if selected is a subset of db_constants (value equality as strings)."""
    return all(str(db_constants.get(k)) == str(v) for k, v in selected.items())


def _coerce(s: str):
    """Coerce a string to a Python literal if possible."""
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return s
