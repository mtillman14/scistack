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

import keyword
import re

from ..model.spec import CategoricalStat, Statistic, value_spellings
from ..model.table import MISSING_LEVEL
from ..reduce.compare import CompareMode, ComparisonPlan
from ..reduce.reduction import (
    CATEGORY_TEXT_COLUMN,
    CategoricalRewrite,
    CollapseCells,
    CollapseLevels,
    CombineLevels,
    LocationMask,
    ReductionPlan,
    ReductionStep,
    ValueFilter,
)


def identifier(name: str) -> str:
    """A variable name as a Python identifier fragment.

    Generated helpers are named after the variable they act on — a reader of an
    exported endpoint should see ``_collapse_rawemg`` rather than ``_collapse``
    and know which measure it belongs to without scrolling.
    """
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower() or "value"
    return f"v_{slug}" if slug[0].isdigit() or keyword.iskeyword(slug) else slug


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
    if isinstance(step, CombineLevels):
        return emit_combine(step)
    if isinstance(step, CollapseCells):
        return emit_collapse_cells(step)
    if isinstance(step, CategoricalRewrite):
        return emit_categorical(step)
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


def emit_combine(step: CombineLevels) -> list[str]:
    """``reduction.CombineLevels.apply`` as code, behind the same guard as
    ``skip_reason``: nothing happens when the source column is missing or the
    combined name is already a column (the preview skips it and says why).
    The source is read as text, so ``1`` and ``"1"`` reach the same bucket."""
    name, source = repr(step.name), repr(step.source)
    mapping = dict(step.mapping)
    lines = [
        f"# {step.source} -> {step.name}",
        f"if {source} in df.columns and {name} not in df.columns:",
        f"    df[{name}] = df[{source}].astype(str).map({mapping!r})",
    ]
    if step.unmatched is None:
        lines.append(f"    df = df[df[{name}].notna()]")
    else:
        lines.append(f"    df[{name}] = df[{name}].fillna({step.unmatched!r})")
    return [*lines, ""]


def emit_collapse_cells(step: CollapseCells) -> list[str]:
    """``reduction.CollapseCells.apply`` as code: one value per record.

    Written as a named helper rather than ``df[y].map(np.nanmean)``: the
    export is meant to READ as what it does, and ``np.nanmean`` warns on an
    all-NaN cell (the preview silences it). A cell is read as
    ``series_stats.cell_array`` reads it: an array or list of numbers is its
    samples, anything else (a lone number, None, text) has none and becomes a
    NaN gap. NaN samples are skipped, as in pandas' ``Series.mean()``.
    """
    y = step.measure
    centre = "median" if step.statistic is Statistic.MEDIAN else "mean"
    if step.exploded:
        # One row per sample already: the rows of one record (every other
        # column equal) become one row.
        within = repr(step.index_column)
        return [
            f"# {y}: one value per record, the {centre} of its sample rows",
            f"_keys = [c for c in df.columns if c not in ({y!r}, {within})]",
            "if _keys:",
            f"    df = df.groupby(_keys, dropna=False, sort=False)[{y!r}].{centre}().reset_index()",
            "else:",
            f"    df = pd.DataFrame({{{y!r}: [df[{y!r}].{centre}()]}})",
            "",
        ]
    name = f"_collapse_{identifier(y)}"
    reduction = "np.median(_samples)" if centre == "median" else "_samples.mean()"
    return [
        f"# {y}: one value per record, the {centre} of each vector",
        f"def {name}(_cell):",
        "    if isinstance(_cell, np.ndarray) and _cell.ndim == 0:",
        '        return float("nan")',
        "    if not isinstance(_cell, (np.ndarray, list, tuple)):",
        '        return float("nan")',
        "    try:",
        '        _samples = np.asarray(_cell, dtype="float64").ravel()',
        "    except (TypeError, ValueError):",
        '        return float("nan")',
        "    _samples = _samples[~np.isnan(_samples)]",
        f'    return float({reduction}) if _samples.size else float("nan")',
        "",
        f"df[{y!r}] = df[{y!r}].map({name})",
        "",
    ]


def emit_categorical(step: CategoricalRewrite) -> list[str]:
    """``reduction.CategoricalRewrite.apply`` as code: the category text (the
    same two pandas calls as ``table.category_text``), the filters on the
    measure (records), then the positions or the one-hot over the step's
    stated pairs."""
    y = step.measure
    lines = [
        f"# {y} holds text: drawn as {step.words} "
        f"(collapsed keys are pooled, never averaged)",
        f"_text = df[{y!r}].astype(object).where(df[{y!r}].notna(), "
        f"{MISSING_LEVEL!r}).astype(str)",
    ]
    for include, exclude in step.record_filters:
        if include is not None:
            lines.append(
                f"df, _text = df[_text.isin({list(include)!r})], "
                f"_text[_text.isin({list(include)!r})]"
            )
        if exclude is not None:
            lines.append(
                f"df, _text = df[~_text.isin({list(exclude)!r})], "
                f"_text[~_text.isin({list(exclude)!r})]"
            )
    if step.mode is CategoricalStat.VALUE:
        return [
            *lines,
            f"_categories = {list(step.levels)!r}",
            "# each category's position on the y axis (0 = the first)",
            f"df[{y!r}] = pd.Categorical(_text, categories=_categories).codes.astype(float)",
            f"df.loc[df[{y!r}] < 0, {y!r}] = float(\"nan\")",
            "",
        ]
    columns = [*step.fields, step.level_name]
    literal = {column: [row[i] for row in step.pairs] for i, column in enumerate(columns)}
    merge = (
        f"df = df.merge(_observed, on={list(step.fields)!r}, how=\"inner\", sort=False)"
        if step.fields
        else 'df = df.merge(_observed, how="cross")'
    )
    text_column = CATEGORY_TEXT_COLUMN
    dropped = [text_column, *([step.level_name] if step.level is not None else [])]
    return [
        *lines,
        "# one row per (record, category of its field): 1 where the record "
        "IS that category, else 0" + (" — x100, a %" if step.scale != 1.0 else ""),
        f"df[{text_column!r}] = _text.to_numpy()",
        f"_observed = pd.DataFrame({literal!r}, columns={columns!r})",
        merge,
        f"df[{y!r}] = (df[{text_column!r}] == df[{step.level_name!r}]).astype(float)"
        f" * {step.scale!r}",
        f"df = df.drop(columns={dropped!r})",
        "",
    ]


def emit_compare(
    plan: ComparisonPlan,
    measure: str,
    columns: list[str],
    index_column: str | None = None,
    frames: tuple[str, ...] = ("df",),
) -> list[str]:
    """``compare.baseline`` + ``compare.apply`` as code: the reference values
    from the first frame (the SAMPLE rows), then each frame compared against
    them (a "Show sample" frame shares its marks' baseline).

    ``columns`` are the factor columns the frames hold when this runs (the
    exported code knows them when it is generated); the baseline is keyed by
    ``ComparisonPlan.keys_among`` of them, the preview's rule. ``index_column``
    is set for a 1-D measure: a series then keeps exactly its reference's
    positions, or is dropped whole (never resampled). A level is matched as
    ``panels.panel_key_text`` spells it (None and NaN read ``"nan"``).
    """
    y = measure
    keys = plan.keys_among([*columns, *([index_column] if index_column else [])], index_column)
    unit = [name for name in keys if name != index_column]
    centre = "median" if (not plan.paired and plan.statistic == "median") else "mean"
    first = frames[0]
    lines = [
        f"# compare to reference: {plan.describe()}",
        f"# ({plan.reason})",
        f"_is_ref = {first}[{plan.layer!r}].map("
        "lambda v: 'nan' if v is None or (isinstance(v, float) and v != v) else str(v)"
        f") == {plan.level!r}",
    ]
    if keys:
        lines.append(f"_compare_keys = {keys!r}")
        lines.append(
            f"_base = {first}[_is_ref].groupby(_compare_keys, dropna=False)[{y!r}]"
            f".{centre}().rename('_ref').reset_index()"
        )
    else:
        lines.append(
            f"_base = pd.DataFrame({{'_ref': [{first}.loc[_is_ref, {y!r}].{centre}()]}})"
        )
    if plan.mode is CompareMode.PERCENT:
        lines.append(
            "_base = _base[_base['_ref'] > 0]  # % change is undefined for a reference <= 0"
        )

    def compared(frame: str, series: list[str]) -> list[str]:
        out = (
            [f"{frame} = {frame}.merge(_base, on=_compare_keys, how='left')"]
            if keys
            else [f"{frame}['_ref'] = _base['_ref'].iloc[0] if len(_base) else float('nan')"]
        )
        if index_column:
            series_ids = f"{frame}.groupby({series!r}, dropna=False).ngroup()" if series else "0"
            out += [
                f"_series_id = pd.Series({series_ids}, index={frame}.index)",
                "_rows = _series_id.map(_series_id.value_counts())",
                f"_with_ref = {frame}['_ref'].notna().groupby(_series_id).transform('sum')",
                (
                    f"_base_n = {frame}[{unit!r}].merge(_base.groupby({unit!r}, dropna=False)"
                    f".size().rename('_n').reset_index(), on={unit!r}, how='left')['_n']"
                    f".fillna(0).to_numpy()"
                    if unit
                    else "_base_n = len(_base)"
                ),
                f"{frame} = {frame}[(_with_ref > 0) & (_with_ref == _rows) & (_rows == _base_n)]",
            ]
        else:
            out.append(f"{frame} = {frame}[{frame}['_ref'].notna()]")
        if plan.mode is CompareMode.PERCENT:
            out.append(
                f"{frame}[{y!r}] = 100 * ({frame}[{y!r}] - {frame}['_ref']) / {frame}['_ref']"
            )
        elif plan.scale != 1.0:
            # A datetime difference is a duration, in the plan's unit (hours).
            out.append(
                f"{frame}[{y!r}] = ({frame}[{y!r}] - {frame}['_ref']) * {plan.scale!r}"
            )
        else:
            out.append(f"{frame}[{y!r}] = {frame}[{y!r}] - {frame}['_ref']")
        out.append(f"{frame} = {frame}.drop(columns='_ref').reset_index(drop=True)")
        return out

    lines += compared(first, [name for name in columns if name != index_column])
    for other in frames[1:]:
        lines.append("# the sample points, against the same reference")
        lines += compared(other, [])
    lines.append("")
    return lines


def emit_collapse_levels(
    step: CollapseLevels,
    columns: list[str],
    *,
    frame: str = "df",
    source: str = "df",
    note: str = "",
) -> list[str]:
    """``reduction.CollapseLevels.apply`` as code: one groupby over the
    columns the pass keeps among ``columns`` (the frame's columns when it
    runs, known when the code is generated), missing levels kept as groups
    and groups in order of appearance — or, with nothing left to group by,
    one row. ``note`` ends the line (a comment)."""
    keep = step.keep_among(columns)
    measures = list(step.measures)
    tail = f"  {note}" if note else ""
    if keep:
        return [
            f"{frame} = {source}.groupby({keep!r}, dropna=False, sort=False)"
            f"[{measures!r}].agg({step.how!r}).reset_index(){tail}"
        ]
    return [f"{frame} = {source}[{measures!r}].agg({step.how!r}).to_frame().T{tail}"]
