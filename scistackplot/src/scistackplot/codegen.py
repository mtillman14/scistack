"""
Code generation: a ``PlotSpec`` becomes readable seaborn/matplotlib source.

Export emits **literal plotting code**, not a call back into this package. The
alternative — ``return scistackplot.render(df, spec_path)`` — is more compact
and stays re-editable in the GUI, but it makes every exported pipeline depend
on this package at runtime and hides the figure's definition behind an opaque
call. Literal code matches the project's "minimize lock-in" goal and the
precedent in ``docs/claude/gui-export-to-plain-python.md``.

The spec is emitted as a docstring block so the GUI can round-trip it back out
of a file the user has since hand-edited.
"""

from __future__ import annotations

import json
import keyword
import re
from typing import NamedTuple

from .cell import apply_cell_collapse, cell_collapses, effective_shape
from .groups import apply_level_groups
from .reduce import plan_layout
from .resolved import DASH_CYCLE
from .roles import (
    collapse_steps,
    complete_roles,
    fanout_keys,
    grouping_layers,
    overlay_join,
    overlay_steps,
    overlay_unavailable,
)
from .shape import Shape
from .spec import (
    ErrorBand,
    FactorVariable,
    PlotKind,
    PlotSpec,
    Role,
    Statistic,
    value_spellings,
)
from .table import MISSING_LEVEL, LongTable
from .ylimits import eligible_scope, limits_by_scope
from .variants import (
    LATEST,
    VARIANT_FACTOR,
    apply_variant_sets,
    defined_sets,
    set_name,
)

#: Marker delimiting the embedded spec inside a generated docstring.
SPEC_BEGIN = "scistackplot-spec:"

_SEABORN_ERRORBAR = {
    ErrorBand.SD: '"sd"',
    ErrorBand.SEM: '"se"',
    ErrorBand.CI95: '("ci", 95)',
    ErrorBand.IQR: '("pi", 50)',
    ErrorBand.NONE: "None",
}

_SERIES_COLUMN = "_series"
#: Column the generated code builds for the UNCOLOURED series layers — what
#: seaborn's ``style=`` splits on, with ``dashes=`` mapping each level to the
#: same style the preview drew (``resolved.DASH_CYCLE``, restated below in
#: seaborn's (segment, gap) spelling; ``""`` is solid).
_DASH_COLUMN = "_dash"
_SEABORN_DASHES: dict[str, tuple | str] = {
    "solid": "",
    "dash": (4, 1.5),
    "dot": (1, 1),
    "dashdot": (3, 1.25, 1.5, 1.25),
    "longdash": (8, 3),
    "longdashdot": (8, 3, 2, 3),
}

#: Column the generated code creates when NO grouping layer is a tick and the
#: measure is not 1-D: every point sits at one categorical position, which is
#: exactly what ``reduce._panel_frame`` does (``out[X] = ""``).
#:
#: This used to fall back to ``table.factor_names[0]``, which was wrong two ways.
#: It drew a different figure from the preview for any scalar spec with no x
#: factor, and that first factor is usually an iteration key, so it is NOT a
#: column of the frame the endpoint receives and every combo raised.
_X_CONSTANT = "Observation"

#: Separator joining a nested axis's layer values in generated code. Readable on
#: purpose — a reader of the exported figure sees "stim · pre" as a tick, where
#: the interactive path uses an invisible control character it never displays.
_NESTED_JOIN = " · "


def generate_plot_function(
    spec: PlotSpec,
    table: LongTable,
    *,
    function_name: str | None = None,
) -> str:
    """
    Generate a ``plot_*`` function body for a scidb endpoint.

    The signature is ``(df, filename)`` — the shape scidb's ``plot_`` contract
    passes — and the function returns a Figure, which the framework saves to
    ``filename`` and closes.
    """
    name = function_name or default_function_name(spec)
    table = apply_level_groups(spec, apply_variant_sets(spec, table))
    # The collapse is NOT applied to `table` — the endpoint receives the frame
    # as the data holds it, so the reduction has to be EMITTED (`_preamble`),
    # exactly as the melt and the variant concat are. What the rest of codegen
    # needs is the shape the figure is drawn from, which is scalar once a
    # scalar kind has been chosen for a 1-D measure: the x expression, the
    # nested-x layers and the plot call are then the scalar ones, and the
    # exported figure is the previewed figure.
    roles = complete_roles(spec, table)
    shape = effective_shape(spec, table)
    collapsing = cell_collapses(spec, table)

    body: list[str] = []
    body.extend(_variant_preamble(spec, table))
    body.extend(_preamble(spec, table, roles, shape))
    body.extend(_plot_call(spec, table, roles, shape))

    lines = [
        f"def {name}({', '.join(function_params(spec))}):",
        f'    """{_docstring(spec, table, roles)}"""',
        "    import matplotlib.pyplot as plt",
        *(["    import numpy as np"] if collapsing else []),
        *(
            ["    import re"]
            if spec.kind is PlotKind.SPAGHETTI
            or _uses_dashes(spec, table, roles, shape)
            or _overlay_of(spec, table, roles, shape) is not None
            else []
        ),
        "    import pandas as pd",
        "    import seaborn as sns",
        "",
        # The body runs under rc_context rather than setting plt.rcParams: this
        # function runs inside a pipeline, and a global font.size would leak
        # into every figure drawn after it. One key scales every text size
        # (ticks, labels, legend, title), matching render_matplotlib.
        f'    with plt.rc_context({{"font.size": {spec.style.font_size}}}):',
    ]
    # A body line that carries its own newline continues at the body indent,
    # so the extra level has to be applied to the continuation too.
    lines.extend(
        "        " + line.replace("\n    ", "\n        ") if line else ""
        for line in body
    )
    return "\n".join(lines) + "\n"


class VariantInput(NamedTuple):
    """One generated ``for_each`` input: its parameter, label, and variable."""

    param: str
    label: str
    variable: str


def variant_params(spec: PlotSpec) -> list[VariantInput]:
    """One input per named variant row.

    Counted over :func:`~scistackplot.variants.defined_sets` — a row the user
    added but has not filled in yet selects nothing and must not become an
    input.

    Empty below two variants: one variant is a *filter*, fully expressed by the
    ``Variant(...)`` wrapper on the single ``df`` input, and giving it its own
    parameter would rename ``df`` for no gain.

    Two or more is a comparison, and it needs one input each. The endpoint
    cannot receive them as one frame and sort them out afterwards: ``as_table``
    hands a function schema keys and data columns only (``scifor``'s
    ``_extract_data``), never the branch-param or code-version columns that
    distinguish variants — those exist only in ``scistackplotdb``'s loader. So
    the split has to happen where the *load* happens, one input per variant, and
    the labels are re-attached here.

    Each row also carries **its own variable**, which is what makes "Raw vs
    Filtered" the same generated shape as "v1 vs v2": two inputs, two
    ``Variant(...)`` wrappers, one concat.
    """
    sets = defined_sets(spec.variant_sets)
    if len(sets) < 2:
        return []
    used: set[str] = set()
    params: list[VariantInput] = []
    for index, variant in enumerate(sets):
        # `latest_column` is the table's to know and there is no table here, so
        # a row pinned to the latest flag slugifies the raw column name. It
        # only affects the generated PARAMETER name, never which rows load.
        label = set_name(variant, index, primary=spec.y_measure)
        base = re.sub(r"[^0-9a-zA-Z]+", "_", label).strip("_").lower() or "variant"
        if base[0].isdigit() or keyword.iskeyword(base):
            base = f"v_{base}"
        candidate, suffix = base, 2
        while candidate in used:
            candidate, suffix = f"{base}_{suffix}", suffix + 1
        used.add(candidate)
        params.append(
            VariantInput(candidate, label, variant.variable or spec.y_measure)
        )
    return params


def single_variant_variable(spec: PlotSpec) -> str | None:
    """The lone row's variable when it is not the primary measure.

    A one-row spec keeps the plain ``df`` input, but that input may still be a
    *different variable* than ``measures[0]`` — in which case its data column
    arrives under that variable's name and has to be renamed into the one the
    plot call uses.
    """
    sets = defined_sets(spec.variant_sets)
    if len(sets) != 1:
        return None
    variable = sets[0].variable
    return variable if variable and variable != spec.y_measure else None


def _slug(name: str) -> str:
    """A variable name as a Python identifier fragment.

    Generated helpers are named after the variable they act on — a reader of an
    exported endpoint should see ``_collapse_rawemg`` rather than ``_collapse``
    and know which measure it belongs to without scrolling.
    """
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower() or "value"
    return f"v_{slug}" if slug[0].isdigit() or keyword.iskeyword(slug) else slug


def group_param(group: FactorVariable) -> str:
    """Parameter name a grouping variable arrives under.

    Prefixed so it cannot collide with ``df``/``df_x`` or with a variant row's
    parameter, and named after the variable AND column so two columns of one
    demographics sheet do not land on the same parameter. Defined here, beside
    the signature it appears in, so ``scistackplotdb.endpoint`` and this module
    cannot disagree about it.
    """
    text = group.variable if group.column is None else f"{group.variable}_{group.column}"
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower() or "group"
    return f"group_{slug}"


def function_params(spec: PlotSpec) -> list[str]:
    """The generated function's signature, in for_each input order."""
    variants = variant_params(spec)
    data = [variant.param for variant in variants] if variants else ["df"]
    groups = [group_param(group) for group in spec.factor_variables]
    return [*data, *groups, "filename"]


def _variant_preamble(spec: PlotSpec, table: LongTable) -> list[str]:
    """Concatenate the per-variant inputs into one labelled ``df``."""
    variants = variant_params(spec)
    y = spec.y_measure
    # Dict/struct variables arrive as one column PER FIELD, so their value
    # column is not named after the variable and there is nothing to rename —
    # the fields align across inputs on their own, and the melt further down
    # turns them into the value column. Emitting the rename anyway would put a
    # line in the user's code referring to a column that is not there.
    melts_fields = bool(table.field_factors)
    if not variants:
        # One row, but possibly of another variable: its data column arrives
        # under that variable's name.
        other = single_variant_variable(spec)
        if not other or melts_fields:
            return []
        return [
            f"# This row plots {other}; the call below reads one value column.",
            f"df = df.rename(columns={{{other!r}: {y!r}}})",
            "",
        ]

    spans_variables = len({variant.variable for variant in variants}) > 1
    lines = [
        "# One input per named variant (each loaded through its own",
        "# Variant(...) filter), labelled and stacked into one frame.",
    ]
    if spans_variables and melts_fields:
        lines.append(
            "# These variables store one column per field; the fields align "
            f"across\n    # inputs, and the melt below turns them into "
            f"{y!r} + a field factor."
        )
    elif spans_variables:
        # Each variable's data column arrives under its own name; the figure
        # draws ONE value column and tells the rows apart by their label.
        lines.append(
            f"# Each variable's values are renamed into {y!r} so they stack; "
            f"the {VARIANT_FACTOR!r} column is what keeps them apart."
        )
    lines.extend(["df = pd.concat(", "    ["])
    for variant in variants:
        frame = variant.param
        if variant.variable != y and not melts_fields:
            frame = f"{frame}.rename(columns={{{variant.variable!r}: {y!r}}})"
        lines.append(
            f"        {frame}.assign(**{{{VARIANT_FACTOR!r}: {variant.label!r}}}),"
        )
    lines.extend(["    ],", "    ignore_index=True,", ")", ""])
    return lines


def default_function_name(spec: PlotSpec) -> str:
    """A valid ``plot_``-prefixed identifier derived from the measure name."""
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", spec.y_measure).strip("_").lower()
    return f"plot_{slug or 'figure'}"


def extract_spec(source: str) -> PlotSpec | None:
    """
    Recover the embedded spec from generated source.

    Lets the GUI reopen a figure the user has since hand-edited: the code is
    the source of truth for rendering, the embedded spec only for repopulating
    the controls. Returns None when no spec is present.
    """
    start = source.find(SPEC_BEGIN)
    if start == -1:
        return None
    brace = source.find("{", start)
    if brace == -1:
        return None
    depth = 0
    for position in range(brace, len(source)):
        if source[position] == "{":
            depth += 1
        elif source[position] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return PlotSpec.from_json(source[brace : position + 1])
                except (ValueError, KeyError):
                    return None
    return None


# ---------------------------------------------------------------------------


def _docstring(spec: PlotSpec, table: LongTable, roles: dict) -> str:
    # The keys the for_each will actually carry — defaulted ones included, in
    # schema order — not the ones the spec literally names.
    iterate = fanout_keys(spec, table)
    note = ""
    if iterate:
        note = (
            f"\n\n    One figure per {', '.join(iterate)} — these are the "
            f"for_each iteration keys, so they are NOT columns here."
        )
    # The shape the FIGURE is drawn from: a 1-D measure under a scalar kind has
    # been collapsed by the time any of these decisions apply, and asking the
    # table would describe the data instead of the plot — nested-x groups would
    # go unmentioned and the y-limit plan would be computed for a line.
    shape_of_y = effective_shape(spec, table)
    if cell_collapses(spec, table):
        note += (
            f"\n\n    Each {spec.y_measure} vector is reduced to its "
            f"{spec.cell_statistic} first, one value per record — so every "
            f"point\n    below is a summary of a whole observation, not a sample."
        )
    if _nested_x_layers(spec, table, roles, shape_of_y):
        note += (
            "\n\n    The x axis nests "
            f"{' > '.join(_nested_x_layers(spec, table, roles, shape_of_y))}; "
            "seaborn has no\n    empty category, so the groups are separated by "
            "ORDER here rather than\n    by the gaps the interactive view draws."
        )
    if spec.facet.has_rules and not _seaborn_can_express_layout(spec, table, roles):
        note += (
            "\n\n    NOTE: the interactive layout arranged the subplots by "
            "matching\n    rules across two grid axes, which seaborn cannot "
            "express — this code\n    wraps them in order instead. The spec "
            "below still carries the rules."
        )
    steps = collapse_steps(spec, roles, table)
    if steps.all:
        chain = " -> ".join([*steps.pre, " x ".join(steps.sample)])
        if spec.kind in (PlotKind.BAR, PlotKind.BAND):
            what = (
                f"error bars: {spec.aggregate.error} across "
                f"{' x '.join(steps.sample)}{' (pooled)' if len(steps.sample) > 1 else ''}"
            )
        elif spec.kind in (PlotKind.BOX, PlotKind.VIOLIN):
            what = f"the distribution is over {' x '.join(steps.sample)}"
        else:
            what = "each mark is the mean over the whole chain"
        note += f"\n\n    Collapsed {chain}, deepest first, each within the rest; {what}."
    if shape_of_y is not Shape.MATRIX_2D:
        note += _y_limit_plan(spec, table, roles)[2]
    return (
        f"{spec.kind} of {spec.y_measure}. Generated by scistackplot.{note}\n\n"
        f"    {SPEC_BEGIN}\n    {spec.to_json(indent=None)}\n    "
    )


def _preamble(spec, table, roles, shape) -> list[str]:
    """Melt, filters, 1-D explosion, aggregation — the order resolve() uses."""
    lines: list[str] = []

    # Grouping variables arrive as their own inputs (a subject-level Condition
    # cannot ride along on a trial-level measure's frame) and are merged back on
    # whatever schema keys they share. The join keys are computed from the
    # frames rather than hard-coded so the generated code stays readable and
    # keeps working if the variable is later saved at a different level.
    for group in spec.factor_variables:
        param = group_param(group)
        factor = group.factor_name
        # The input arrives as schema keys plus its data column(s): a
        # single-column variable's column is renamed to the variable by scidb's
        # loader, and a ColumnSelection keeps the column's own name
        # (`scifor.foreach._prepare_input`). Either way the column is already
        # named `factor`, which is why nothing is renamed here and the
        # interactive path's column name is the exported one.
        lines.extend(
            [
                f"# {group.label}: one value per {param}'s schema level, "
                f"broadcast to every row",
                f"_on = [c for c in {param}.columns if c in df.columns]",
                f"_group = {param}[[*_on, {factor!r}]].drop_duplicates(subset=_on)",
                'df = df.merge(_group, on=_on, how="left")',
                # Mirrors ScidbSource._attach_factor_variables: a row the
                # grouping says nothing about is labelled, not dropped, or the
                # exported figure would hold fewer observations than the
                # previewed one.
                f"df[{factor!r}] = df[{factor!r}].fillna({MISSING_LEVEL!r}).astype(str)",
                "",
            ]
        )

    # A dict/struct variable arrives at the endpoint as one column per field
    # (scidb's multi_column storage). The interactive path melts it in
    # ScidbSource.get_table, so the generated code has to melt it too — the
    # exported figure must be the previewed figure.
    for field in table.field_factors:
        levels = [str(level) for level in field.levels]
        lines.extend(
            [
                f"# one row per field of {spec.y_measure} "
                f"({len(levels)} field(s))",
                f"_fields = {levels!r}",
                "df = df.melt(",
                "    id_vars=[c for c in df.columns if c not in _fields],",
                "    value_vars=_fields,",
                f"    var_name={field.name!r},",
                f"    value_name={spec.y_measure!r},",
                ")",
                "",
            ]
        )

    # A 1-D measure drawn by a scalar kind is collapsed to one value per record
    # BEFORE anything is filtered — the same order `reduce._build_plan_timed`
    # uses, and the reason is the same: a range filter on the measure filters
    # the collapsed value, which is the only thing it could mean.
    #
    # Written as a named helper rather than `df[y].map(np.nanmean)` for two
    # reasons: `np.nanmean` warns on an all-NaN cell (the interactive path
    # silences it, and a generated endpoint printing RuntimeWarnings per record
    # is noise nobody can act on), and the export is meant to READ as what it
    # does. The semantics are pandas' `Series.mean()` — NaN samples skipped —
    # which is what `series_stats.collapse_cells` computes.
    if cell_collapses(spec, table):
        y = spec.y_measure
        reduction = (
            "np.median(_samples)"
            if spec.cell_statistic is Statistic.MEDIAN
            else "_samples.mean()"
        )
        lines.extend(
            [
                f"# {y}: one value per record, the {spec.cell_statistic} of "
                f"each vector",
                f"def _collapse_{_slug(y)}(_cell):",
                '    _samples = np.asarray(_cell, dtype="float64").ravel()',
                "    _samples = _samples[~np.isnan(_samples)]",
                f'    return float({reduction}) if _samples.size else float("nan")',
                "",
                f"df[{y!r}] = df[{y!r}].map(_collapse_{_slug(y)})",
                "",
            ]
        )

    # Schema locations. Emitted as one vectorised comparison per (prefix, key),
    # which is the SAME rule reduce._location_mask applies — a ragged selection
    # cannot be expressed as `for_each(subject=[...], trial=[...])`, whose keys
    # cross-product, so it has to be a mask inside the function body. The
    # `if _k in df.columns` guard is not defensive clutter: it is how a key the
    # frame lacks goes unconstrained, matching the interactive path exactly.
    lines.extend(_location_lines(spec))

    filter_lines: list[str] = []
    for flt in spec.filters:
        if flt.include is not None:
            filter_lines.append(f"df = df[df[{flt.column!r}].isin({list(flt.include)!r})]")
        if flt.exclude is not None:
            filter_lines.append(f"df = df[~df[{flt.column!r}].isin({list(flt.exclude)!r})]")
        if flt.minimum is not None:
            filter_lines.append(f"df = df[df[{flt.column!r}] >= {flt.minimum!r}]")
        if flt.maximum is not None:
            filter_lines.append(f"df = df[df[{flt.column!r}] <= {flt.maximum!r}]")
    if filter_lines:
        lines.extend([*filter_lines, ""])

    # Derived grouping factors. The interactive path builds these in
    # `groups.apply_level_groups`; the endpoint receives the raw table, so the
    # same mapping has to be emitted here or the exported figure is not the one
    # that was previewed.
    for group in spec.level_groups:
        if not group.name or not group.source:
            continue
        mapping = {str(key): value for key, value in group.mapping.items()}
        lines.append(f"# {group.source} -> {group.name}")
        lines.append(f"_groups = {mapping!r}")
        lines.append(
            f"df[{group.name!r}] = df[{group.source!r}].astype(str).map(_groups)"
        )
        if group.unmatched is None:
            lines.append(f"df = df[df[{group.name!r}].notna()]")
        else:
            lines.append(
                f"df[{group.name!r}] = df[{group.name!r}].fillna({group.unmatched!r})"
            )
        lines.append("")

    # No factor on x and not a 1-D measure: every point shares one categorical
    # position. The preview builds that column in reduce; the endpoint has to
    # build it too, or seaborn is handed an x that is not in the frame.
    if _x_expression(spec, table, roles, shape) == _X_CONSTANT:
        lines.extend([f"df[{_X_CONSTANT!r}] = \"\"", ""])

    # Nested x: one position per combination of the layers. The ORDER is
    # emitted as a resolved list rather than re-derived — replaying the nesting
    # rules in generated code would be a second implementation that can drift
    # from the preview (same reason the facet layout emits `col_order`).
    layers = _nested_x_layers(spec, table, roles, shape)
    if layers:
        lines.extend(
            [
                f"# nested x axis: {' > '.join(layers)}",
                f"_layers = {layers!r}",
                f"df[{_X_NESTED!r}] = df[_layers].astype(str).agg("
                f"{_NESTED_JOIN!r}.join, axis=1)",
                "",
            ]
        )

    index_column = spec.index_column or table.index_column or "index"
    if shape is Shape.SERIES_1D and not table.measure(spec.y_measure).exploded:
        y = spec.y_measure
        lines.extend(
            [
                "# 1-D measure: one row per sample",
                f"df[{index_column!r}] = df[{y!r}].map(lambda v: list(range(len(v))))",
                f"df = df.explode([{y!r}, {index_column!r}], ignore_index=True)",
                f"df[{y!r}] = pd.to_numeric(df[{y!r}])",
                f"df[{index_column!r}] = pd.to_numeric(df[{index_column!r}])",
                "",
            ]
        )

    # "Show sample" (PlotSpec.show_sample): the overlay's rows come from the
    # frame BEFORE the marks' chain consumes it — the same cut
    # `reduce._build_figure` draws — so `_sample` is built here, and drawn
    # after the plot call (`_sample_draw_lines`).
    lines.extend(_sample_preamble_lines(spec, table, roles, shape, layers))

    # The collapse chain (roles.collapse_steps): one groupby-mean per collapsed
    # key, deepest first, each grouping on every other factor the frame still
    # holds — trial within subject, then subject. Only `pre` is emitted: the
    # SAMPLE is what every kind draws (schema-level parity, 2026-09-19). Bar
    # and band leave the sample rows to seaborn's estimator and errorbar,
    # which then compute exactly what the preview's `_summarize` did; the
    # other kinds draw the rows themselves. Pooled specs collapse nothing here
    # for the same reason.
    steps = collapse_steps(spec, roles, table)
    kept = [
        name
        for name, role in roles.items()
        if role is not Role.ITERATE and table.has_factor(name)
    ]
    # `final` is non-empty only for a spaghetti whose sample cannot be joined
    # across x (roles.spaghetti_sample_repeats): each line is its mean.
    for key in [*steps.pre, *steps.final]:
        if key not in kept:
            continue
        kept = [name for name in kept if name != key]
        keep = list(kept)
        if shape is Shape.SERIES_1D:
            keep.append(index_column)
        if layers:
            # The composed nested-x column is built above from layer
            # columns that are all kept, so it is constant within each
            # group — but pandas drops any column not named here, and the
            # plot call then asked for an `_x` that no longer existed.
            keep.append(_X_NESTED)
        elif _x_expression(spec, table, roles, shape) == _X_CONSTANT:
            keep.append(_X_CONSTANT)
        measures = [spec.y_measure, *([spec.x_measure] if spec.x_measure else [])]
        within = f" within {', '.join(kept)}" if kept else ""
        what = "each line's mean" if key in steps.final else "averaged away"
        lines.extend(
            [
                f"# collapse {key}{within} — {what}",
                f"df = df.groupby({keep!r}, as_index=False)[{measures!r}].mean()",
                "",
            ]
        )
    if steps.sample and spec.kind in (PlotKind.BAR, PlotKind.BAND):
        lines.append(
            f"# the sample: {' x '.join(steps.sample)} — seaborn's estimator and "
            f"errorbar run over its levels{' (pooled)' if len(steps.sample) > 1 else ''}"
        )
        lines.append("")

    grouping = grouping_layers(spec, table, roles, spec.kind, shape=shape)
    series_layers = [name for name in grouping.series if table.has_factor(name)]
    # The series id is the units (one line per sample level, line and
    # spaghetti only) inside the grouping's series layers — the same
    # `GroupingLayers.identity` the preview composes.
    identity_layers = [name for name in grouping.identity if table.has_factor(name)]
    if spec.kind in (PlotKind.LINE, PlotKind.SPAGHETTI, PlotKind.BAND) and identity_layers:
        # One line / band per leaf group. Composed OUTERMOST FIRST, exactly as
        # `reduce._series_key` composes the preview's series ids.
        #
        # Vectorized, not `.agg(' | '.join, axis=1)`. That form reads better
        # and costs a Python call PER ROW, which for a 1-D measure is per
        # sample: a 24-row frame of EMG traces is 8.9 million rows once
        # exploded, and the join alone ran for minutes (scidb.log 2026-09-11).
        lines.extend(
            [
                (
                    f"# one line per {' x '.join(reversed(grouping.units))} within each "
                    "leaf group of the grouping"
                    if grouping.units
                    else "# one line per leaf group of the grouping"
                ),
                f"df[{_SERIES_COLUMN!r}] = {_composed(list(reversed(identity_layers)))}",
                "",
            ]
        )
    elif spec.kind is PlotKind.SPAGHETTI:
        # Nothing left to join by (reduce emits an empty series id too).
        lines.extend([f"df[{_SERIES_COLUMN!r}] = \"\"", ""])

    dash_layers = [name for name in series_layers if name != grouping.color]
    if spec.kind in (PlotKind.LINE, PlotKind.BAND) and dash_layers:
        # The uncoloured part of the identity, told apart by dash style — the
        # rule is restated (natural-sorted ids, cycling) rather than frozen as
        # a literal, because under ITERATE the endpoint sees one figure's ids
        # and the preview assigns styles per figure (`reduce._dash_styles`).
        cycle = [_SEABORN_DASHES[name] for name in DASH_CYCLE]
        lines.extend(
            [
                f"# dash style per {', '.join(reversed(dash_layers))} (uncoloured series layer)",
                f"df[{_DASH_COLUMN!r}] = {_composed(list(reversed(dash_layers)))}",
                "_natural = lambda s: [(0, int(c)) if c.isdigit() else (1, c) "
                'for c in re.split(r"(\\d+)", s) if c]',
                f"_dash_ids = sorted(df[{_DASH_COLUMN!r}].astype(str).unique(), key=_natural)",
                f"_cycle = {cycle!r}",
                "_dashes = {s: _cycle[i % len(_cycle)] for i, s in enumerate(_dash_ids)}",
                "",
            ]
        )

    if spec.kind is PlotKind.SPAGHETTI:
        lines.extend(_spaghetti_position_lines(spec, table, roles, shape))

    return lines


def _composed(columns: list[str], frame: str = "df") -> str:
    """Generated pandas composing ``columns`` of ``frame`` into one
    ``" | "``-joined id."""
    head, *rest = columns
    composed = f"{frame}[{head!r}].astype(str)"
    if rest:
        composed += f".str.cat({frame}[{rest!r}].astype(str), sep=' | ')"
    return composed


def _uses_dashes(spec, table, roles, shape) -> bool:
    if spec.kind not in (PlotKind.LINE, PlotKind.BAND):
        return False
    grouping = grouping_layers(spec, table, roles, spec.kind, shape=shape)
    return any(
        name != grouping.color and table.has_factor(name) for name in grouping.series
    )


def _spaghetti_position_lines(spec, table: LongTable, roles, shape) -> list[str]:
    """Generated pandas that places each series at ``level index + offset``.

    The offset rule is restated here in five lines of plain Python rather than
    frozen as a literal (as the y limits are) or imported from this package
    (which an exported endpoint must not depend on). A literal would be keyed
    by series id, and under ``Role.ITERATE`` the endpoint sees ONE figure's
    subjects — the preview spreads those across the band, and a set-wide
    literal would bunch them in one part of it. Computing per frame is what
    the preview does (``reduce._spaghetti_offsets`` runs per figure), so the
    export matches it; ``test_spaghetti`` pins the two against
    :func:`scistackplot.spaghetti.series_offsets`.
    """
    from .spaghetti import SPAGHETTI_SPREAD

    x = _x_expression(spec, table, roles, shape)
    layers = _nested_x_layers(spec, table, roles, shape)
    if layers:
        # The composed, spacer-free order the nested axis is exported with.
        declared = _nested_x_order(spec, table, roles, shape)
    elif x == _X_CONSTANT:
        declared = [""]
    else:
        declared = [str(level) for level in table.factor(x).levels]
    return [
        "# spaghetti: each level at its index, each series shifted a fixed",
        "# fraction of a tick so every line ends on its own markers",
        f"_declared = {declared!r}",
        f"_present = set(df[{x!r}].astype(str))",
        "_order = [v for v in _declared if v in _present]",
        "_natural = lambda s: [(0, int(c)) if c.isdigit() else (1, c) "
        'for c in re.split(r"(\\d+)", s) if c]',
        f"_ids = sorted(df[{_SERIES_COLUMN!r}].astype(str).unique(), key=_natural)",
        f"_step = {2.0 * SPAGHETTI_SPREAD} / (len(_ids) - 1) if len(_ids) > 1 else 0.0",
        f"_offset = {{s: round({-SPAGHETTI_SPREAD} + i * _step, 6) "
        "for i, s in enumerate(_ids)}",
        f"df[{_X_POSITION!r}] = df[{x!r}].astype(str).map("
        "{v: i for i, v in enumerate(_order)}) "
        f"+ df[{_SERIES_COLUMN!r}].astype(str).map(_offset)",
        "",
    ]


# ---------------------------------------------------------------------------
# "Show sample": the overlay, emitted
# ---------------------------------------------------------------------------

#: Column the generated code builds for the overlay's identity (the shown keys
#: composed outermost first) and its numeric position.
_SAMPLE_FRAME = "_sample"


def _overlay_of(spec, table: LongTable, roles, shape):
    """``(OverlaySteps, OverlayJoin)`` for the export, or None — the same
    availability rule the figure path applies (``roles.overlay_unavailable``),
    so the export carries an overlay exactly when the preview did."""
    if not spec.show_sample or overlay_unavailable(spec, roles, shape) is not None:
        return None
    steps = overlay_steps(spec, roles, table)
    if steps is None:
        return None
    return steps, overlay_join(spec, roles, table, steps)


def _sample_preamble_lines(spec, table: LongTable, roles, shape, layers) -> list[str]:
    """``_sample``: the overlay chain restated as groupby means, mirroring the
    marks' chain emitted just below it — nested (one groupby per averaged
    key, deepest first, on every other factor still present) or pooled (one
    groupby dropping them all) — then the identity column."""
    found = _overlay_of(spec, table, roles, shape)
    if found is None:
        return []
    steps, join = found
    y = spec.y_measure
    kept = [
        name
        for name, role in roles.items()
        if role is not Role.ITERATE and table.has_factor(name)
    ]
    extra = [_X_NESTED] if layers else (
        [_X_CONSTANT] if _x_expression(spec, table, roles, shape) == _X_CONSTANT else []
    )
    averaged = [key for key in steps.averaged if key in kept]
    what = f"one point per {' · '.join(steps.shown)}"
    if averaged:
        what += f"; {', '.join(averaged)} averaged within it"
    lines = [f"# show sample: {what}"]
    if not averaged:
        lines.append(f"{_SAMPLE_FRAME} = df.copy()")
    elif spec.aggregate.pooled:
        keep = [name for name in kept if name not in averaged] + extra
        lines.append(
            f"{_SAMPLE_FRAME} = df.groupby({keep!r}, as_index=False)[[{y!r}]].mean()"
            f"  # pooled (weight by N)"
        )
    else:
        source = "df"
        for key in averaged:
            kept = [name for name in kept if name != key]
            keep = [*kept, *extra]
            lines.append(
                f"{_SAMPLE_FRAME} = {source}.groupby({keep!r}, as_index=False)[[{y!r}]].mean()"
            )
            source = _SAMPLE_FRAME
    shown = [name for name in steps.shown if table.has_factor(name)]
    lines.append(
        f"{_SAMPLE_FRAME}[{_SERIES_COLUMN!r}] = "
        + (_composed(shown, _SAMPLE_FRAME) if shown else '""')
    )
    lines.append("")
    return lines


def _sample_draw_lines(spec, table: LongTable, roles, shape) -> list[str]:
    """Draw ``_sample`` on the seaborn grid: each point at its level's index
    plus its hue's dodge slot plus its identity's offset — the arithmetic of
    ``render.base.sample_positions`` restated in plain pandas — joined into a
    line per identity when ``roles.overlay_join`` said so.

    The offset rule is restated rather than frozen (see
    ``_spaghetti_position_lines`` for why: under ITERATE the endpoint sees one
    figure's identities). The dodge is seaborn's own: ``width=0.8`` split
    evenly over the hue levels, in their order of appearance — which is what
    ``catplot`` draws, and what ``render.base.dodge_offset`` draws.
    """
    from .render.base import (
        SAMPLE_ALPHA,
        SAMPLE_EDGE_COLOR,
        SAMPLE_LINE_WIDTH,
        SAMPLE_MARKER_FRACTION,
    )
    from .spaghetti import SPAGHETTI_SPREAD

    found = _overlay_of(spec, table, roles, shape)
    if found is None:
        return []
    steps, join = found
    y = spec.y_measure
    x = _x_expression(spec, table, roles, shape)
    hue = _color_of(spec, table, roles, shape)
    facets = [name for name, role in roles.items() if role is Role.FACET]
    # seaborn keys `axes_dict` by (row, col) when both are set, else by the one.
    facet_names = [facets[1], facets[0]] if len(facets) > 1 else facets[:1]
    nested = _nested_x_order(spec, table, roles, shape)
    palette = spec.style.palette
    marker = float(spec.style.marker_size * SAMPLE_MARKER_FRACTION) ** 0.5
    linestyle = "-" if join.join else "none"

    lines = [
        f"# show sample: {'lines join each ' + steps.deepest_shown if join.join else 'points only'}"
        f" — {join.reason}",
        # The x positions catplot used: the nested order it was given, the
        # declared order it was given (`_level_order_lines`), or — with neither
        # — the order of appearance seaborn infers.
        f"_x_levels = {nested!r}" if nested else (
            "_x_levels = [str(v) for v in _x_order]"
            if _emits_x_order(spec, table, roles, shape, _seaborn_call(spec, table, roles, shape))
            else f"_x_levels = list(dict.fromkeys(df[{x!r}].astype(str)))"
        ),
    ]
    if hue:
        lines.extend(
            [
                # hue_order was stated on the call, so the dodge slots and the
                # palette follow it too.
                "_hue_levels = [str(v) for v in _hue_order]",
                "_slot = 0.8 / len(_hue_levels)",
                "_dodge = {h: (i - (len(_hue_levels) - 1) / 2) * _slot "
                "for i, h in enumerate(_hue_levels)}",
                f"_palette = dict(zip(_hue_levels, sns.color_palette({palette!r}, len(_hue_levels))))",
            ]
        )
    else:
        lines.extend(
            [
                "_hue_levels = [None]",
                f"_color = sns.color_palette({palette!r})[0]",
            ]
        )
    lines.extend(
        [
            "_natural = lambda s: [(0, int(c)) if c.isdigit() else (1, c) "
            'for c in re.split(r"(\\d+)", s) if c]',
            f"_ids = sorted({_SAMPLE_FRAME}[{_SERIES_COLUMN!r}].astype(str).unique(), key=_natural)",
            f"_step = {2.0 * SPAGHETTI_SPREAD} / (len(_ids) - 1) if len(_ids) > 1 else 0.0",
            f"_offset = {{s: round(round({-SPAGHETTI_SPREAD} + i * _step, 6) * (1.0 / len(_hue_levels)), 6) "
            "for i, s in enumerate(_ids)}",
            f"{_SAMPLE_FRAME}[{_X_POSITION!r}] = ("
            f"{_SAMPLE_FRAME}[{x!r}].astype(str).map({{v: i for i, v in enumerate(_x_levels)}})",
            f"    + {_SAMPLE_FRAME}[{_SERIES_COLUMN!r}].astype(str).map(_offset)",
            *(
                [f"    + {_SAMPLE_FRAME}[{hue!r}].astype(str).map(_dodge)"] if hue else []
            ),
            ")",
            "_axes = list(g.axes_dict.items()) if g.axes_dict else [((), g.ax)]",
            f"_facets = {facet_names!r}",
            "for _key, _ax in _axes:",
            f"    _rows = {_SAMPLE_FRAME}",
            "    for _name, _value in zip(_facets, _key if isinstance(_key, tuple) else (_key,)):",
            "        _rows = _rows[_rows[_name].astype(str) == str(_value)]",
            f"    for _id, _part in _rows.groupby({[_SERIES_COLUMN, *([hue] if hue else [])]!r}):",
            f"        _part = _part.sort_values({_X_POSITION!r})",
            "        _ax.plot(",
            f"            _part[{_X_POSITION!r}], _part[{y!r}],",
            f"            linestyle={linestyle!r}, marker='o', markersize={marker:.3f},",
            f"            markeredgecolor={SAMPLE_EDGE_COLOR!r}, markeredgewidth=0.5,",
            (
                f"            color=_palette[str(_id[1])], alpha={SAMPLE_ALPHA}, "
                f"linewidth={SAMPLE_LINE_WIDTH}, zorder=3,"
                if hue else
                f"            color=_color, alpha={SAMPLE_ALPHA}, linewidth={SAMPLE_LINE_WIDTH}, zorder=3,"
            ),
            "        )",
        ]
    )
    return lines




def _location_lines(spec: PlotSpec) -> list[str]:
    """Generated pandas for ``spec.location_filter`` — empty when it is inert.

    Kept beside the other preamble emitters rather than inlined so the one test
    that matters can compare its output against
    :func:`scistackplot.reduce._location_mask` on the same frame.
    """
    prefixes = spec.location_filter.prefixes()
    excluded = spec.location_filter.excluded()
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


def _facet_layout_args(spec, table: LongTable, facets: list[str]) -> list[str]:
    """
    seaborn arguments that reproduce the interactive facet arrangement.

    A single faceted factor is a strip of panels seaborn wraps at ``col_wrap``,
    and the order it wraps them in is ``col_order`` — so a rule-defined layout
    IS expressible whenever the panels fill the grid without holes. Replaying
    ``plan_layout`` here (rather than re-deriving an order) is what keeps the
    exported figure identical to the preview; when the arrangement cannot be
    expressed, ``_docstring`` says so instead of quietly differing.
    """
    if len(facets) != 1:
        return []
    try:
        levels = [str(level) for level in table.factor(facets[0]).levels]
    except KeyError:
        return []
    if not levels:
        return []

    plan = plan_layout(levels, spec.facet)
    args = []
    if plan.n_cols < len(levels):
        args.append(f"col_wrap={plan.n_cols}")
    if spec.facet.has_rules and plan.fills_row_major:
        args.append(f"col_order={plan.labels_in_grid_order(levels)!r}")
    return args


def _nested_x_args(spec, table: LongTable, roles, shape) -> list[str]:
    """``order=[...]`` reproducing the composed nested axis.

    The RESOLVED order, computed by the same :func:`~scistackplot.xaxis.plan_x_axis`
    the preview used, rather than the nesting rules re-applied in generated
    code. A second implementation is a second thing that can drift, and the
    failure would be a re-ordered axis nobody notices.

    Spacers are dropped: seaborn has no concept of an empty category, so the
    exported figure groups by ordering alone. The docstring says so — the
    interactive view's gaps are the one thing the export cannot reproduce.
    """
    order = _nested_x_order(spec, table, roles, shape)
    return [f"order={order!r}"] if order else []


def _nested_x_order(spec, table: LongTable, roles, shape) -> list[str]:
    """The composed nested-axis order, spacers dropped (see ``_nested_x_args``)."""
    layers = _nested_x_layers(spec, table, roles, shape)
    if not layers:
        return []

    from .xaxis import LEAF_SEPARATOR, is_spacer, plan_x_axis

    frame = table.frame
    present = [name for name in layers if name in frame.columns]
    if len(present) != len(layers):
        return []
    combinations = list(
        frame[layers].astype(str).drop_duplicates().itertuples(index=False, name=None)
    )
    plan = plan_x_axis(
        combinations,
        [[str(level) for level in table.factor(name).levels] for name in layers],
    )
    return [
        key.replace(LEAF_SEPARATOR, _NESTED_JOIN)
        for key in plan.order
        if not is_spacer(key)
    ]


def _seaborn_can_express_layout(spec, table: LongTable, roles) -> bool:
    """Whether ``_facet_layout_args`` reproduced the rules (see ``_docstring``)."""
    facets = [name for name, role in roles.items() if role is Role.FACET]
    if len(facets) != 1:
        return False
    try:
        levels = [str(level) for level in table.factor(facets[0]).levels]
    except KeyError:
        return False
    return bool(levels) and plan_layout(levels, spec.facet).fills_row_major


def _plot_call(spec, table, roles, shape) -> list[str]:
    if shape is Shape.MATRIX_2D:
        return _heatmap_call(spec)

    kind = spec.kind
    x = _x_expression(spec, table, roles, shape)
    color = _color_of(spec, table, roles, shape)
    facets = [name for name, role in roles.items() if role is Role.FACET]
    dashes = _uses_dashes(spec, table, roles, shape)

    # A spaghetti plot is drawn on the numeric POSITION column the preamble
    # built (level index + per-series offset); `x` stays the factor, which is
    # what the axis is labelled with and what the ticks are named from.
    x_column = _X_POSITION if kind is PlotKind.SPAGHETTI else x
    args = [f"data=df", f"x={x_column!r}", f"y={spec.y_measure!r}"]
    if color:
        args.append(f"hue={color!r}")
        if _color_level_count(spec, table, color) < 2:
            # Same rule the renderers apply (render.base.shows_legend): one
            # colour level means the legend restates what every mark on the
            # figure has in common. The exported figure must be the previewed
            # figure, so it has to be decided here too, not just at render time.
            args.append("legend=False")
    # seaborn takes one factor per grid axis: the first faceted factor drives
    # the columns, a second one the rows.
    if facets:
        args.append(f"col={facets[0]!r}")
    if len(facets) > 1:
        args.append(f"row={facets[1]!r}")
    args.extend(_facet_layout_args(spec, table, facets))
    if kind is not PlotKind.SPAGHETTI:
        # relplot has no `order=`; the spaghetti preamble applies it itself.
        args.extend(_nested_x_args(spec, table, roles, shape))

    estimator = (
        '"median"' if spec.aggregate.statistic is Statistic.MEDIAN else '"mean"'
    )
    errorbar = _SEABORN_ERRORBAR[spec.aggregate.error]

    if kind in (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR, PlotKind.STRIP):
        seaborn_kind = {
            PlotKind.BOX: "box",
            PlotKind.VIOLIN: "violin",
            PlotKind.BAR: "bar",
            PlotKind.STRIP: "strip",
        }[kind]
        args.append(f'kind="{seaborn_kind}"')
        if kind is PlotKind.BAR:
            args.append(f"estimator={estimator}")
            args.append(f"errorbar={errorbar}")
        call = "sns.catplot"
    elif kind is PlotKind.SPAGHETTI:
        # Markers and lines in one call: one polyline per series, no estimator,
        # on the numeric position axis. The tick labels are restored below.
        args.extend(
            [
                'kind="line"',
                "estimator=None",
                f"units={_SERIES_COLUMN!r}",
                'marker="o"',
            ]
        )
        call = "sns.relplot"
    elif kind is PlotKind.SCATTER:
        if _x_is_categorical(spec, table, roles, shape):
            args.extend(['kind="strip"', "jitter=False"])
            call = "sns.catplot"
        else:
            args.append('kind="scatter"')
            call = "sns.relplot"
    elif kind is PlotKind.LINE:
        args.append('kind="line"')
        args.append("estimator=None")
        grouping = grouping_layers(spec, table, roles, kind, shape=shape)
        if any(table.has_factor(name) for name in grouping.identity):
            args.append(f"units={_SERIES_COLUMN!r}")
        if dashes:
            args.extend([f"style={_DASH_COLUMN!r}", "dashes=_dashes"])
        call = "sns.relplot"
    elif kind is PlotKind.BAND:
        args.extend([f'kind="line"', f"estimator={estimator}", f"errorbar={errorbar}"])
        if dashes:
            # `style=` is a semantic grouping: seaborn estimates one band per
            # (hue, style) — one per uncoloured series layer, as the preview.
            args.extend([f"style={_DASH_COLUMN!r}", "dashes=_dashes"])
        call = "sns.relplot"
    else:  # pragma: no cover - every kind is covered above
        args.append('kind="scatter"')
        call = "sns.relplot"

    order_lines, order_args = _level_order_lines(
        spec, table, roles, shape, call, x, color, facets
    )
    args.extend(order_args)

    style = spec.style
    if style.palette:
        args.append(f"palette={style.palette!r}")

    # seaborn shares y across facets by DEFAULT, so per-panel autoscale has to
    # be asked for explicitly or the export quietly draws a different figure
    # from the preview. This is the same class of bug the facet `col_order`
    # replay exists to prevent.
    share_y, ylim, _note = _y_limit_plan(spec, table, roles)
    if not share_y:
        args.append('facet_kws={"sharey": False}')

    lines = [*order_lines, f"g = {call}(", *[f"    {arg}," for arg in args], ")"]
    if kind is PlotKind.SPAGHETTI:
        # Positions back to level names, and the range a categorical axis
        # would have had — the same geometry the preview draws.
        lines.append(
            "g.set(xticks=list(range(len(_order))), xticklabels=_order, "
            "xlim=(-0.5, len(_order) - 0.5))"
        )
    if ylim is not None:
        lines.append(f"g.set(ylim={(float(ylim[0]), float(ylim[1]))!r})")
    # The constant-x column is scaffolding, not a variable anyone measured —
    # reduce._labels_for leaves the label empty in that case, so this must too.
    x_label = style.x_label or ("" if x == _X_CONSTANT else x)
    lines.append(
        f"g.set_axis_labels({x_label!r}, "
        f"{(style.y_label or spec.y_measure)!r})"
    )
    if facets:
        # Same rule as the preview (render.base.panel_y_title): the facet values
        # ARE the panel's y-axis title, and no caption sits above it. seaborn
        # does the opposite by default, so both halves have to be said here or
        # the exported figure spends vertical room the preview gave to the data.
        # A two-factor grid keys axes_dict by (row, col), which is the reverse
        # of the panel key's order — hence the reversed().
        lines.extend(
            [
                'g.set_titles("")',
                "for _key, _ax in g.axes_dict.items():",
                "    _values = _key if isinstance(_key, tuple) else (_key,)",
                '    _ax.set_ylabel(" · ".join(str(v) for v in reversed(_values)))',
            ]
        )
    lines.extend(_sample_draw_lines(spec, table, roles, shape))
    if style.log_x:
        lines.append('g.set(xscale="log")')
    if style.log_y:
        lines.append('g.set(yscale="log")')
    if style.title:
        lines.append(f"g.figure.suptitle({style.title!r})")
    lines.append(f"g.figure.set_size_inches({style.width}, {style.height})")
    lines.append("return g.figure")
    return lines


#: The generated helper that orders a column's levels. Emitted into the
#: function body (an exported endpoint must not import this package).
_IN_ORDER = "_in_order"

#: seaborn calls that take ``order=`` for a categorical x. relplot has none.
_ORDERED_X_CALLS = {"sns.catplot"}


def _in_order_lines() -> list[str]:
    """The helper every emitted ``*_order`` goes through.

    Takes the DECLARED levels (the table's factor order: ``[schema_keys]``
    first, then the source's natural sort) and keeps the ones this frame
    holds. Filtering at run time, not here, is what makes it right under
    ITERATE: each figure sees its own subset, and the preview orders exactly
    that subset (``reduce._level_order`` reads the figure's panels). It
    returns the frame's OWN values, so a numeric column is ordered by numbers
    seaborn will recognise rather than by their text.
    """
    return [
        f"def {_IN_ORDER}(values, declared):",
        "    # Declared order first; a level it never named goes last.",
        "    rank = {level: i for i, level in enumerate(declared)}",
        "    present = list(dict.fromkeys(values.dropna()))",
        "    return sorted(present, key=lambda v: rank.get(str(v), len(rank)))",
    ]


def _declared_levels(table: LongTable, name: str) -> list[str]:
    try:
        return [str(level) for level in table.factor(name).levels]
    except KeyError:
        return []


def _seaborn_call(spec, table, roles, shape) -> str:
    """Which seaborn function ``_plot_call`` draws with (heatmaps aside)."""
    if spec.kind in (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR, PlotKind.STRIP):
        return "sns.catplot"
    if spec.kind is PlotKind.SCATTER and _x_is_categorical(spec, table, roles, shape):
        return "sns.catplot"
    return "sns.relplot"


def _emits_x_order(spec, table, roles, shape, call: str) -> bool:
    """Whether the call gets ``order=_x_order`` — a flat categorical x.

    Nested x has its own composed ``order=`` (``_nested_x_args``) and the
    constant x has one level; spaghetti places levels itself.
    """
    if call not in _ORDERED_X_CALLS or _nested_x_layers(spec, table, roles, shape):
        return False
    x = _x_expression(spec, table, roles, shape)
    return x != _X_CONSTANT and table.has_factor(x)


def _level_order_lines(
    spec, table, roles, shape, call: str, x: str, color, facets
) -> tuple[list[str], list[str]]:
    """``order=`` / ``hue_order=`` / ``col_order=`` / ``row_order=``, STATED.

    Left unsaid, seaborn orders every one of them by first appearance in
    ``df`` — the database's row order — so the exported figure drew
    ``post, pre`` beside a preview drawing the project's declared
    ``pre, post``. Returns the lines computing each order (before the
    call, so the "Show sample" overlay reuses the same ``_x_order`` /
    ``_hue_order``) and the call arguments naming them.
    """
    lines: list[str] = []
    call_args: list[str] = []

    def order(variable: str, column: str, arg: str) -> None:
        declared = _declared_levels(table, column)
        lines.append(f"{variable} = {_IN_ORDER}(df[{column!r}], {declared!r})")
        call_args.append(f"{arg}={variable}")

    if _emits_x_order(spec, table, roles, shape, call):
        order("_x_order", x, "order")
    if color:
        order("_hue_order", color, "hue_order")
    # A RULED layout is `_facet_layout_args`' business: it emits its own
    # col_order when seaborn can express the arrangement, and says so in the
    # docstring when it cannot — a declared-order col_order there would claim
    # a layout the preview does not draw.
    if facets and not spec.facet.has_rules:
        order("_col_order", facets[0], "col_order")
    if len(facets) > 1:
        order("_row_order", facets[1], "row_order")
    if not lines:
        return [], []
    return [*_in_order_lines(), *lines], call_args


def _heatmap_call(spec) -> list[str]:
    return [
        "import numpy as np",
        "",
        f"matrix = np.mean(np.stack([np.asarray(v, dtype=float) "
        f"for v in df[{spec.y_measure!r}]]), axis=0)",
        f"fig, ax = plt.subplots(figsize=({spec.style.width}, {spec.style.height}))",
        'image = ax.imshow(matrix, aspect="auto", origin="lower")',
        "fig.colorbar(image, ax=ax)",
        f"ax.set_title({(spec.style.title or spec.y_measure)!r})",
        "return fig",
    ]


def _y_limit_plan(spec: PlotSpec, table: LongTable, roles: dict) -> tuple:
    """``(share_y, ylim, note)`` — how the endpoint reproduces the y scale.

    A generated endpoint sees **one iteration's frame**: one figure, and no way
    to look at the others. That single fact decides everything here.

    * A scope naming every ITERATE factor means limits never cross figures, so
      the figure's own data defines them — which is exactly what seaborn does
      by itself. Nothing to emit.
    * A scope naming FACET factors too means each panel scales to itself:
      ``sharey=False``, and again nothing to emit. (seaborn shares y by default,
      so this one has to be said out loud or the export silently differs.)
    * A scope that does NOT name every ITERATE factor means the limits are a
      property of data this endpoint cannot see — the whole point of asking for
      one scale across every subject. That value is computed HERE, at generation
      time, and baked in as a **literal**.

    **Why a literal rather than scifor's ``share_limits``.** ``for_each`` can
    already coordinate ranges across separately-iterated figures
    (``share_limits={"df": ["subject"]}``), and that maps onto ``scope``
    exactly. It was not used here because the two answer different questions: a
    literal reproduces THE FIGURE THE USER APPROVED, byte for byte, while
    ``share_limits`` recomputes from whatever the data holds at run time — so
    adding a subject would silently rescale a recorded artifact, and the export
    would stop matching the preview it was generated from. For a
    lineage-tracked figure, the frozen number is the honest one; the spec in the
    docstring still carries the scope, so regenerating after new data is one
    click and an explicit act. Revisit if a user wants a living endpoint rather
    than a reproducible one.
    """
    y_axis = spec.y_axis
    if y_axis.is_manual:
        return True, (float(y_axis.minimum), float(y_axis.maximum)), ""

    # Baked-in limits must be the limits of what is DRAWN. For a collapsed 1-D
    # measure that is the range of the per-record means, not of every sample —
    # off by the whole within-record spread, which is the bulk of it. The
    # collapse is recomputed here rather than passed in because this is the only
    # step of generation that touches values at all.
    table = apply_cell_collapse(spec, table)

    scope = eligible_scope(y_axis.scope, roles, table)
    iterate = fanout_keys(spec, table)
    facets = [name for name, role in roles.items() if role is Role.FACET]

    per_panel = any(name in scope for name in facets)
    spans_figures = not all(name in scope for name in iterate)

    if not spans_figures:
        # Within one figure: seaborn's own scaling is the right answer, shared
        # across facets or not depending on the scope.
        return not per_panel, None, ""

    limits = limits_by_scope(table, spec, scope, roles)
    if not limits:
        return not per_panel, None, ""

    distinct = {value for value in limits.values()}
    if len(distinct) == 1:
        return not per_panel, distinct.pop(), ""

    # Several groups with different ranges, and the endpoint has only one
    # figure's rows to decide between them. The global range keeps every
    # exported figure comparable, which is what asking for a cross-figure scope
    # was for; say so rather than let the export differ in silence.
    note = (
        "\n\n    NOTE: the y limits separate by "
        f"{', '.join(scope)}, which spans figures this endpoint\n    cannot "
        "see. The widest range across the whole dataset is used here, so "
        "every\n    exported figure stays on one comparable scale."
    )
    lows = [low for low, _ in limits.values()]
    highs = [high for _, high in limits.values()]
    return not per_panel, (min(lows), max(highs)), note


def _color_level_count(spec: PlotSpec, table: LongTable, color: str) -> int:
    """
    How many colour levels the generated code will actually draw.

    Filters are applied here because they are applied in the generated
    preamble: filtering a two-level factor down to one must drop the legend in
    the export exactly as it drops it in the preview. An unknown factor keeps
    its legend — omitting one that was wanted is worse than keeping one that
    was not.
    """
    try:
        levels = [str(level) for level in table.factor(color).levels]
    except KeyError:
        return 2
    levels = _levels_after_location(spec, color, levels)
    for flt in spec.filters:
        if flt.column != color:
            continue
        if flt.include is not None:
            keep = {str(value) for value in flt.include}
            levels = [level for level in levels if level in keep]
        if flt.exclude is not None:
            drop = {str(value) for value in flt.exclude}
            levels = [level for level in levels if level not in drop]
    return len(levels)


def _levels_after_location(spec: PlotSpec, column: str, levels: list[str]) -> list[str]:
    """``levels`` narrowed by ``spec.location_filter``, for one schema key.

    The rule follows from prefixes constraining only the keys they name: a
    prefix that never mentions this column says nothing about it, so if any
    such prefix is selected the column keeps every level. Only when *every*
    prefix names it do the values they give become the surviving set.

    Concretely, selecting all of subject 01 plus subject 02's trial 3 leaves
    ``trial`` with all its levels — because "all of subject 01" did not name a
    trial — which is what the figure will actually draw.
    """
    # The level RULE applies first and unconditionally: an omitted level is
    # gone from this key's axis however the prefixes fall out, and leaving it
    # in the generated `order=` would reserve an empty slot on the exported
    # figure that the preview does not have.
    dropped = {
        spelling
        for value in spec.location_filter.excluded().get(column, ())
        for spelling in value_spellings(value)
    }
    if dropped:
        levels = [level for level in levels if str(level) not in dropped]

    prefixes = spec.location_filter.prefixes()
    if not prefixes:
        return levels
    named = [dict(prefix) for prefix in prefixes]
    if any(column not in prefix for prefix in named):
        return levels
    keep = {
        spelling
        for prefix in named
        for spelling in value_spellings(prefix[column])
    }
    return [level for level in levels if str(level) in keep]


#: Column the generated code builds for a nested x axis.
_X_NESTED = "_x"
#: Column the generated code builds for a spaghetti plot: numeric position
#: (level index + per-series offset) on what is otherwise a categorical axis.
_X_POSITION = "_xpos"


def _tick_layers(spec, table, roles, shape) -> list[str]:
    """The tick layers in drawing order (outermost first) — the grouping
    list as this kind reads it (``roles.grouping_layers``), minus the
    coloured layer, which seaborn dodges by ``hue``."""
    if spec.x_measure or shape is not Shape.SCALAR:
        return []
    layers = grouping_layers(spec, table, roles, spec.kind, shape=shape)
    return [name for name in layers.ticks if table.has_factor(name)]


def _nested_x_layers(spec, table, roles, shape) -> list[str]:
    """The factors sharing the x axis, when there is more than one."""
    layers = _tick_layers(spec, table, roles, shape)
    return layers if len(layers) > 1 else []


def _color_of(spec, table, roles, shape) -> str | None:
    color = grouping_layers(spec, table, roles, spec.kind, shape=shape).color
    return color if color and table.has_factor(color) else None


def _x_expression(spec, table, roles, shape) -> str:
    if spec.x_measure:
        return spec.x_measure
    if shape is Shape.SERIES_1D:
        return spec.index_column or table.index_column or "index"
    ticks = _tick_layers(spec, table, roles, shape)
    if len(ticks) > 1:
        return _X_NESTED
    return ticks[0] if ticks else _X_CONSTANT


def _x_is_categorical(spec, table, roles, shape) -> bool:
    if spec.x_measure or shape is Shape.SERIES_1D:
        return False
    # The constant fallback is a single categorical position, so it is
    # categorical too — the interactive renderers draw it that way.
    return True


def generate_script(
    spec: PlotSpec,
    table: LongTable,
    *,
    source_expression: str = 'pd.read_csv("data.csv")',
    function_name: str | None = None,
) -> str:
    """
    A complete runnable script — the standalone (no scidb) export.

    Same generated function, plus the few lines that load a table and save the
    figure, so a CSV user gets something they can run immediately.
    """
    name = function_name or default_function_name(spec)
    function = generate_plot_function(spec, table, function_name=name)
    call_args, setup = _script_inputs(spec)
    return (
        '"""Generated by scistackplot."""\n'
        "import matplotlib.pyplot as plt\n"
        "import pandas as pd\n"
        "import seaborn as sns\n\n\n"
        f"{function}\n\n"
        'if __name__ == "__main__":\n'
        f"    df = {source_expression}\n"
        + "".join(f"    {line}\n" if line else "\n" for line in setup)
        + f'    figure = {name}({call_args}"figure.png")\n'
        '    figure.savefig("figure.png", dpi=150, bbox_inches="tight")\n'
    )


def _script_inputs(spec: PlotSpec) -> tuple[str, list[str]]:
    """The standalone script's per-variant frames.

    The endpoint path gets its variants from the database, one ``Variant(...)``
    load per input. A script has one flat frame instead, so the same split is
    done here with literal ``pandas`` — the variant columns are ordinary columns
    of whatever was loaded.

    A ``"latest"`` selection cannot be honoured standalone: which record is
    newest at a schema location is a provenance question, and a CSV carries no
    provenance. Rather than silently pinning nothing, the generated line says so.
    """
    variants = variant_params(spec)
    if not variants:
        return "df, ", []

    lines: list[str] = [""]
    for generated, variant in zip(
        variants, defined_sets(spec.variant_sets), strict=True
    ):
        param, label = generated.param, generated.label
        lines.append(f"# variant {label!r}")
        if generated.variable != spec.y_measure:
            # A standalone script starts from ONE flat table, so a row drawing
            # on another variable can only mean another column of it.
            lines.append(
                f"{param} = df.rename(columns="
                f"{{{generated.variable!r}: {spec.y_measure!r}}})"
            )
        else:
            lines.append(f"{param} = df")
        for column, value in variant.selection.items():
            if isinstance(value, str) and value == LATEST:
                lines.append(
                    f"# NOTE: {column!r} asked for the latest version, which "
                    f"needs provenance a flat table does not carry — not applied."
                )
                continue
            if isinstance(value, (list, tuple, set, frozenset)):
                levels = [str(v) for v in value]
                test = f"{param}[{column!r}].astype(str).isin({levels!r})"
            else:
                test = f"{param}[{column!r}].astype(str) == {str(value)!r}"
            lines.append(f"{param} = {param}[{test}]")
    lines.append("")
    return "".join(f"{generated.param}, " for generated in variants), lines


def spec_json_block(spec: PlotSpec) -> str:
    """The spec as a pretty JSON block, for writing next to generated code."""
    return json.dumps(spec.to_dict(), indent=2, sort_keys=True)
