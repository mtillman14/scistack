"""Reduction steps printed as plain pandas — one emitter per step type.

``reduce.reduction`` defines each step and ``reduce`` applies it; this module
prints the SAME step, so an exported script reduces its rows by the rule the
preview used rather than by a second derivation of it (design review 06, P2;
the standing rule: export equals preview). Every emitter writes code over a
frame named ``df`` and leaves the result in ``df``.

``scistackplot/tests/test_reduction_code.py`` executes each emitter's output
against ``reduce``'s mask on the same frames.
"""

from __future__ import annotations

from ..model.spec import value_spellings
from ..reduce.reduction import LocationMask, ReductionPlan, ReductionStep, ValueFilter


def emit_plan(plan: ReductionPlan) -> list[str]:
    """Every step of ``plan``, in order."""
    lines: list[str] = []
    for step in plan.steps:
        lines.extend(emit_step(step))
    return lines


def emit_step(step: ReductionStep) -> list[str]:
    """The pandas for one step."""
    if isinstance(step, LocationMask):
        return emit_location_mask(step)
    if isinstance(step, ValueFilter):
        return emit_value_filter(step)
    raise TypeError(f"no emitter for reduction step {type(step).__name__}")


def emit_location_mask(step: LocationMask) -> list[str]:
    """``reduce.resolve._location_mask`` as code — empty when it is inert.

    One vectorised comparison per (prefix, key): a ragged selection cannot be
    expressed as ``for_each(subject=[...], trial=[...])``, whose keys
    cross-product, so it has to be a mask inside the function body. The
    ``if _k in df.columns`` guard is how a key the frame lacks goes
    unconstrained, matching the interactive path.
    """
    prefixes = step.location_filter.prefixes()
    excluded = step.location_filter.excluded()
    if not prefixes and not excluded:
        return []

    lines: list[str] = []

    # The spellings are BAKED IN rather than recomputed by generated code: the
    # exported script must not carry a copy of `value_spellings` that can drift
    # from the one the live figure used. One spec therefore always produces
    # byte-identical source, which is why `value_spellings` sorts.
    if prefixes:
        baked = [
            [[key, list(value_spellings(value))] for key, value in prefix]
            for prefix in prefixes
        ]
        plural = "" if len(prefixes) == 1 else "s"
        lines += [
            f"# schema locations: {len(prefixes)} selection{plural}",
            f"_loc_prefixes = {baked!r}",
            "_loc_mask = pd.Series(False, index=df.index)",
            "for _p in _loc_prefixes:",
            "    _m = pd.Series(True, index=df.index)",
            "    for _k, _vs in _p:",
            "        if _k in df.columns:",
            "            _m &= df[_k].notna() & df[_k].astype(str).isin(_vs)",
            "    _loc_mask |= _m",
            "df = df[_loc_mask]",
            "",
        ]

    if excluded:
        baked_levels = {
            key: sorted({s for value in values for s in value_spellings(value)})
            for key, values in excluded.items()
        }
        lines += [
            f"# schema levels omitted everywhere: {len(baked_levels)} key(s)",
            f"_loc_excluded = {baked_levels!r}",
            "for _k, _vs in _loc_excluded.items():",
            "    if _k in df.columns:",
            "        df = df[~(df[_k].notna() & df[_k].astype(str).isin(_vs))]",
            "",
        ]

    return lines


def emit_value_filter(step: ValueFilter) -> list[str]:
    """``reduction.ValueFilter.mask`` as code: levels compared as text, bounds
    on the column coerced to numbers, nothing done when the column is absent."""
    col = repr(step.column)
    conditions: list[str] = []
    if step.include is not None:
        conditions.append(f"df[{col}].astype(str).isin({list(step.include)!r})")
    if step.exclude is not None:
        conditions.append(f"~df[{col}].astype(str).isin({list(step.exclude)!r})")
    if step.minimum is not None:
        conditions.append(f"(pd.to_numeric(df[{col}], errors='coerce') >= {step.minimum!r})")
    if step.maximum is not None:
        conditions.append(f"(pd.to_numeric(df[{col}], errors='coerce') <= {step.maximum!r})")
    if not conditions:
        return []
    return [
        f"if {col} in df.columns:",
        *(f"    df = df[{condition}]" for condition in conditions),
    ]
