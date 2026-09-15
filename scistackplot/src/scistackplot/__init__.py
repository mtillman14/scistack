"""
scistackplot — spec-driven plotting for long-format scientific data.

Standalone: point it at a CSV or a DataFrame and it works, with no database
and no configuration. Compatible: the object it is built around, ``PlotSpec``,
is exactly what the body of a scidb ``plot_`` endpoint needs, so an interactive
exploration can be frozen into a lineage-tracked pipeline step.

Typical standalone use::

    import pandas as pd
    from scistackplot import DataFrameSource, PlotSpec, Role, PlotKind, render

    source = DataFrameSource(pd.read_csv("gait.csv"))
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.X, "limb": Role.COLOR, "subject": Role.FREE},
        kind=PlotKind.BOX,
    )
    figure = render(source, spec)

Typical pipeline use (as a scidb endpoint body)::

    def plot_step_length(df, filename):
        return render(df, spec)     # framework saves + closes the Figure

See ``docs/claude/plotting-library-design.md`` for the architecture.
"""

from __future__ import annotations

from typing import Any

from .capability import (
    available_plots,
    capabilities,
    default_plot,
    factors_menu,
    grouping_summary,
    role_hint,
    role_label,
    role_options,
    variant_summary,
    why_unavailable,
)
from .collapse import apply_collapse, collapse_note, collapses, effective_shape
from .codegen import (
    default_function_name,
    extract_spec,
    generate_plot_function,
    generate_script,
)
from .reduce import MAX_TRANSPORT_POINTS, resolve, resolve_one
from .render import render_matplotlib, render_plotly
from .resolved import Encoding, Labels, Panel, ResolvedPlot
from .roles import (
    RoleError,
    complete_roles,
    default_roles,
    default_spec,
    fanout_keys,
    role_for_new_grouping,
    roles_for_kind,
    validate,
)
from .shape import Shape, classify_column, classify_value, is_plottable
from .sources import BaseSource, CsvSource, DataFrameSource, DataSource
from .spec import (
    Aggregation,
    ErrorBand,
    FacetOptions,
    FactorVariable,
    Filter,
    LevelGroup,
    LocationFilter,
    MatchOp,
    Matcher,
    PlotKind,
    PlotSpec,
    Role,
    SCALAR_KINDS,
    Statistic,
    StyleOptions,
    VariantSet,
    YAxis,
    grid_shape_for,
)
from .xaxis import XGroup, XPlan, leaf_key, plan_x_axis
from .ylimits import describe as describe_y_limits
from .ylimits import eligible_scope, limits_by_scope
from .table import (
    CODE_FACTOR_PREFIX,
    MISSING_LEVEL,
    RUN_FACTOR_PREFIX,
    FactorInfo,
    LongTable,
    MeasureInfo,
    natural_sort_key,
)
from .variants import (
    LATEST,
    VARIANT_FACTOR,
    apply_variant_sets,
    auto_label,
    SPAN_LOCATION_LIMIT,
    default_selection,
    defined_sets,
    describe_span,
    spanned_code_axes,
    strip_answered_roles,
    variant_set_mask,
)

__all__ = [
    # spec
    "PlotSpec",
    "Role",
    "PlotKind",
    "Statistic",
    "ErrorBand",
    "Aggregation",
    "FacetOptions",
    "YAxis",
    "eligible_scope",
    "limits_by_scope",
    "describe_y_limits",
    "grid_shape_for",
    "Matcher",
    "MatchOp",
    "StyleOptions",
    "Filter",
    "FactorVariable",
    "LevelGroup",
    "LocationFilter",
    "VariantSet",
    "SCALAR_KINDS",
    # 1-D -> scalar collapse
    "collapses",
    "effective_shape",
    "apply_collapse",
    "collapse_note",
    # data
    "LongTable",
    "FactorInfo",
    "MISSING_LEVEL",
    "MeasureInfo",
    "Shape",
    "classify_value",
    "classify_column",
    "is_plottable",
    "natural_sort_key",
    # sources
    "DataSource",
    "BaseSource",
    "CsvSource",
    "DataFrameSource",
    # policy
    "available_plots",
    "variant_summary",
    "CODE_FACTOR_PREFIX",
    "RUN_FACTOR_PREFIX",
    # named variants
    "VARIANT_FACTOR",
    "LATEST",
    "apply_variant_sets",
    "default_selection",
    "variant_set_mask",
    "auto_label",
    "defined_sets",
    "spanned_code_axes",
    "describe_span",
    "SPAN_LOCATION_LIMIT",
    "strip_answered_roles",
    "default_plot",
    "why_unavailable",
    "role_options",
    "role_label",
    "role_hint",
    "grouping_summary",
    "factors_menu",
    "capabilities",
    "default_roles",
    "default_spec",
    "complete_roles",
    "fanout_keys",
    "role_for_new_grouping",
    "roles_for_kind",
    "validate",
    "RoleError",
    # resolution + rendering
    "resolve",
    "resolve_one",
    "XPlan",
    "XGroup",
    "plan_x_axis",
    "leaf_key",
    "ResolvedPlot",
    "Panel",
    "Encoding",
    "Labels",
    "render",
    "render_all",
    "render_matplotlib",
    "render_plotly",
    "MAX_TRANSPORT_POINTS",
    # export
    "generate_plot_function",
    "generate_script",
    "default_function_name",
    "extract_spec",
]

__version__ = "0.1.0"


def as_table(data: Any) -> LongTable:
    """
    Coerce whatever the caller has into a :class:`LongTable`.

    Accepts a LongTable, a DataSource, or a plain DataFrame — the last being
    what a scidb ``plot_`` endpoint receives, so ``render(df, spec)`` just works
    inside a pipeline step.
    """
    if isinstance(data, LongTable):
        return data
    if hasattr(data, "describe") and hasattr(data, "get_table"):  # DataSource
        # A scidb source needs to be told which variable to load; a flat source
        # already holds one table. default_measure() answers both.
        measure = getattr(data, "default_measure", lambda: None)()
        return data.get_table([measure] if measure else [])
    return LongTable.from_frame(data)


def render(data: Any, spec: PlotSpec, *, backend: str = "matplotlib"):
    """
    Resolve and render a single figure.

    Raises if the spec fans out into several figures — in a pipeline that
    fan-out belongs to ``for_each``'s iteration keys, not to one endpoint call,
    and silently returning only the first figure would hide the mistake.
    Use :func:`render_all` when you deliberately want the whole set.
    """
    figures = render_all(data, spec, backend=backend)
    if len(figures) > 1:
        raise ValueError(
            f"This spec produces {len(figures)} figures (iterating over "
            f"{', '.join(spec.iterate_factors)}). Inside a pipeline, pass those "
            f"as for_each iteration keys instead of Role.ITERATE; outside one, "
            f"call render_all()."
        )
    return figures[0]


def render_all(data: Any, spec: PlotSpec, *, backend: str = "matplotlib") -> list:
    """Resolve and render every figure in the fan-out."""
    table = as_table(data)
    resolved = resolve(spec, table)
    renderer = render_plotly if backend == "plotly" else render_matplotlib
    return [renderer(item) for item in resolved]
