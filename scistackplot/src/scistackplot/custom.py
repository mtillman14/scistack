"""
Custom plot kinds: the user's own ``plot_`` function draws the figure.

Plot Studio opens on a ``plot_`` node of the canvas. The node's edges bind its
inputs to variables, and :class:`~scistackplot.spec.CustomPlot` names the
function in the spec. Everything else is the same Plot Studio: variant rows,
the location picker, filters, roles, size, text and colours
(``.claude/plan-custom-plot-kinds.md``, ``docs/claude/custom-plot-kinds.md``).

This module is the ONE owner of how the function is called:

* :func:`inspect_function` reads the signature. A function that accepts
  ``ax`` is **panel-level**: scistackplot builds the figure, the facet grid,
  the ticks, the legend and the text, and the function draws one panel's
  marks onto ``ax``. Without ``ax`` it is **figure-level** and returns a whole
  matplotlib or plotly figure.
* :func:`prepare` derives everything the calls need, and refuses (with the
  fix in the message) what cannot be drawn. :func:`custom_requirement` is that
  refusal as text, for the panel.
* :class:`CustomDraw` builds each call's arguments and makes the call.

**What one call receives.** Each variable input gets its OWN table, as in
``for_each``: rows selected by the figure's ITERATE values and the panel's
FACET values (only the keys that input carries: a subject-level input is
passed whole to every trial-level call, the coarse-input broadcast), then the
collapse chain's inner keys averaged away (``roles.collapse_steps``, nested
and unweighted, the same chain the built-in kinds use). The sample key and the
GROUP keys stay as columns. ``as_table`` decides between that table (schema
keys as columns, the data column named after the variable) and the bare value,
which needs one row per call.

**The layout input.** The DEEPEST input (most schema keys) is the one the
figure is laid out from: its factors are the ones roles can name, and its
table is what the ordinary ``resolve`` reduces to place panels, ticks and the
legend. It is drawn as the built-in kind for its shape (:func:`layout_kind`)
with every mark-only setting removed, and the renderer calls the function
instead of drawing that kind's marks.
"""

from __future__ import annotations

import contextlib
import contextvars
import inspect
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterator, Mapping

import numpy as np
import pandas as pd

from scistacklog import Log

from .groups import apply_level_groups
from .roles import (
    CollapseSteps,
    RoleError,
    collapse_steps,
    complete_roles,
    custom_kind_problem,
    validate,
)
from .shape import Shape
from .spec import PlotKind, PlotSpec, Role, VariantSet
from .table import LongTable
from .variants import apply_variant_sets, strip_answered_roles

LAYER = "scistackplot"

#: Parameters the CALL supplies rather than an edge: the panel's axes and the
#: :class:`PlotContext`. A function opts into each by naming it.
AX_PARAM = "ax"
CONTEXT_PARAM = "context"
RESERVED_PARAMS = (AX_PARAM, CONTEXT_PARAM)

#: The level a function draws at, from its signature.
PANEL_LEVEL = "panel"
FIGURE_LEVEL = "figure"


class CustomFunctionError(RuntimeError):
    """The user's function raised, or returned something that is not a figure.

    The message names the function and the call (figure and panel), so the
    panel can say which call failed; the original exception is ``__cause__``
    and its traceback is in scistack.log.
    """


# ---------------------------------------------------------------------------
# The function
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CustomSignature:
    """What the function's signature says about how to call it."""

    function: str
    #: Every named parameter, in order (``*args`` / ``**kwargs`` excluded).
    parameters: tuple[str, ...]
    #: Parameters with a default: the call may leave them out.
    optional: frozenset[str] = frozenset()
    accepts_ax: bool = False
    accepts_context: bool = False

    @property
    def level(self) -> str:
        """``"panel"`` when the function draws onto a given ``ax``, else
        ``"figure"``."""
        return PANEL_LEVEL if self.accepts_ax else FIGURE_LEVEL


def inspect_function(fn: Callable) -> CustomSignature:
    """Read ``fn``'s signature. The precedent is ``share_limits``: an argument
    the call can supply is passed only when the signature names it."""
    params = inspect.signature(fn).parameters
    named = [
        p
        for p in params.values()
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    ]
    return CustomSignature(
        function=getattr(fn, "__name__", repr(fn)),
        parameters=tuple(p.name for p in named),
        optional=frozenset(p.name for p in named if p.default is not p.empty),
        accepts_ax=AX_PARAM in params,
        accepts_context=CONTEXT_PARAM in params,
    )


@dataclass
class CustomCall:
    """The function and what its node's edges bind, before any spec.

    ``inputs`` maps each VARIABLE parameter to that variable's table, and
    ``variables`` to its name (the table's data column). ``constants`` are
    passed as they are: a constant edge's value, or ``None`` for the
    ``PathOutput`` parameter (Plot Studio writes the file itself).
    """

    fn: Callable
    inputs: dict[str, LongTable]
    variables: dict[str, str]
    constants: dict[str, Any] = field(default_factory=dict)
    signature: CustomSignature | None = None

    def __post_init__(self) -> None:
        if self.signature is None:
            self.signature = inspect_function(self.fn)
        missing = set(self.inputs) ^ set(self.variables)
        if missing:
            raise ValueError(
                f"CustomCall: inputs and variables must name the same "
                f"parameters; these differ: {sorted(missing)}"
            )

    @property
    def variable_params(self) -> list[str]:
        """The variable parameters, in signature order."""
        order = {name: i for i, name in enumerate(self.signature.parameters)}
        return sorted(self.inputs, key=lambda p: order.get(p, len(order)))

    @property
    def primary(self) -> str:
        """The layout input: the deepest one, ties to the first in signature
        order. See the module docstring."""
        params = self.variable_params
        if not params:
            raise RoleError(
                f"{self.signature.function} has no variable input wired. Connect a "
                f"variable to one of its inputs on the canvas."
            )
        return max(params, key=lambda p: (_depth(self.inputs[p]), -params.index(p)))

    def as_table_params(self, as_table: bool | list[str]) -> set[str]:
        """Which variable parameters get a table, from ``CustomPlot.as_table``."""
        if isinstance(as_table, list):
            return {p for p in as_table if p in self.inputs}
        return set(self.inputs) if as_table else set()


def _depth(table: LongTable) -> int:
    return len(table.schema_levels) if table.schema_levels else len(table.factor_names)


# ---------------------------------------------------------------------------
# Preparation: the one place every refusal is decided
# ---------------------------------------------------------------------------


def layout_kind(shape: Shape) -> PlotKind:
    """The built-in kind the layout input is laid out as.

    Scalars get categorical ticks from the GROUP layers (a scatter's layout);
    1-D data a numeric x and one series per GROUP level (a line's); 2-D a
    heatmap's, which cannot be grouped. Never cell-collapses: the function
    receives the arrays.
    """
    if shape is Shape.SERIES_1D:
        return PlotKind.LINE
    if shape is Shape.MATRIX_2D:
        return PlotKind.HEATMAP
    return PlotKind.SCATTER


@dataclass
class Prepared:
    """Everything decided before any call is made, shared by every figure."""

    spec: PlotSpec
    call: CustomCall
    #: The spec the layout input is resolved with (a built-in kind, with the
    #: mark-only settings removed).
    layout_spec: PlotSpec
    layout_table: LongTable
    roles: dict[str, Role]
    steps: CollapseSteps
    #: Per variable parameter: its rows after its variant pin, combines and
    #: filters, cut to its factor columns plus its data column.
    frames: dict[str, pd.DataFrame]
    #: Per variable parameter: its factor columns, in table order.
    factors: dict[str, list[str]]
    tables: set[str]
    notes: list[str] = field(default_factory=list)

    @property
    def level(self) -> str:
        return self.call.signature.level


def variant_sets_for(spec: PlotSpec, variable: str, primary_variable: str) -> list[VariantSet]:
    """The spec's variant rows that select ``variable``'s records. A row with
    no ``variable`` is the primary's, as for a built-in kind."""
    return [s for s in spec.variant_sets if (s.variable or primary_variable) == variable]


def _input_spec(spec: PlotSpec, variable: str, primary_variable: str, kind: PlotKind) -> PlotSpec:
    """``spec`` as it applies to one input: that input is the measure, and
    every setting that only means something for built-in marks is removed."""
    return replace(
        spec,
        kind=kind,
        custom=None,
        measures=[variable],
        x_measure=None,
        variant_sets=variant_sets_for(spec, variable, primary_variable),
        show_sample=[],
        join_sample=None,
        sample_color=None,
        comparison=None,
        difference_bars=[],
    )


def prepare(spec: PlotSpec, call: CustomCall) -> Prepared:
    """Derive every input and the layout, or raise :class:`RoleError` naming
    the fix. The ONE statement of what a custom figure needs; the panel's
    reasons (:func:`custom_requirement`) are this function's refusals."""
    from .reduce import apply_filters

    problem = custom_kind_problem(spec)
    if problem is not None:
        raise RoleError(problem)
    signature = call.signature
    if spec.custom.function != signature.function:
        raise RoleError(
            f"The spec names the function {spec.custom.function!r}, but the call "
            f"was made with {signature.function!r}."
        )

    # --- every parameter is supplied ------------------------------------
    supplied = set(call.inputs) | set(call.constants) | set(RESERVED_PARAMS)
    unwired = [
        p for p in signature.parameters if p not in supplied and p not in signature.optional
    ]
    if unwired:
        raise RoleError(
            f"{signature.function}: input(s) {', '.join(repr(p) for p in unwired)} "
            f"are not wired. Connect them on the canvas."
        )

    primary = call.primary
    primary_variable = call.variables[primary]
    primary_table = call.inputs[primary]
    kind = layout_kind(primary_table.shape_of(primary_variable))

    # --- the layout input, exactly as resolve will derive it ------------
    layout_spec = _input_spec(spec, primary_variable, primary_variable, kind)
    derived = apply_level_groups(layout_spec, apply_variant_sets(layout_spec, primary_table))
    layout_spec = strip_answered_roles(layout_spec, primary_table, derived)
    validate(layout_spec, derived)
    roles = complete_roles(layout_spec, derived)
    steps = collapse_steps(layout_spec, roles, derived)

    if signature.level == FIGURE_LEVEL:
        faceted = [name for name, role in roles.items() if role is Role.FACET]
        if faceted:
            raise RoleError(
                f"{signature.function} draws a whole figure (it has no 'ax' "
                f"parameter), so it cannot be split into panels by "
                f"{', '.join(repr(f) for f in faceted)}. Make "
                f"{'it' if len(faceted) == 1 else 'them'} 'separate figures', or "
                f"add an 'ax' parameter to draw one panel per call."
            )

    tables = call.as_table_params(spec.custom.as_table)
    allowed_for_value = (
        {Role.ITERATE, Role.FACET} if signature.level == PANEL_LEVEL else {Role.ITERATE}
    )

    frames: dict[str, pd.DataFrame] = {}
    factors: dict[str, list[str]] = {}
    notes: list[str] = []
    for param in call.variable_params:
        variable = call.variables[param]
        table = call.inputs[param]
        if param == primary:
            input_table, input_spec = derived, layout_spec
        else:
            input_spec = _input_spec(spec, variable, primary_variable, kind)
            input_table = apply_level_groups(
                input_spec, apply_variant_sets(input_spec, table)
            )
        frame = apply_filters(input_table.frame, input_spec)
        columns = [name for name in input_table.factor_names if name in frame.columns]
        # A schema column the variable never populated means "no axis there",
        # and its rows broadcast across it (docs/claude/coarse-level-inputs.md).
        # Left in, every selection on it would match nothing.
        empty = [name for name in columns if frame[name].isna().all()]
        if empty:
            Log.debug(
                "[custom] %s: dropped unpopulated column(s) %s (broadcast)",
                param,
                empty,
                layer=LAYER,
            )
            columns = [name for name in columns if name not in empty]
        frames[param] = frame[[*columns, variable]]
        factors[param] = columns

        # --- D3: averaging needs numbers ---------------------------------
        averaged = [key for key in steps.pre if key in columns]
        shape = table.shape_of(variable)
        if averaged:
            numeric = shape is Shape.SERIES_1D or (
                shape is Shape.SCALAR and pd.api.types.is_numeric_dtype(frame[variable])
            )
            if not numeric:
                raise RoleError(
                    f"{param} ({variable}) is {shape} data, which cannot be averaged, "
                    f"but {', '.join(repr(k) for k in averaged)} "
                    f"{'is' if len(averaged) == 1 else 'are'} collapsed. Make "
                    f"{'it' if len(averaged) == 1 else 'them'} 'separate figures', "
                    f"'separate panels' or a grouping instead."
                )

        # --- a bare value needs one row per call -------------------------
        if param not in tables:
            loose = [
                name for name in columns if roles.get(name) not in allowed_for_value
            ]
            if loose:
                placed = "'separate figures' or 'separate panels'" if (
                    signature.level == PANEL_LEVEL
                ) else "'separate figures'"
                described = ", ".join(
                    f"{name!r} ({roles[name] if name in roles else 'not placed'})"
                    for name in loose
                )
                raise RoleError(
                    f"{param} is passed as a value (as_table is off for it), so each "
                    f"call needs exactly one {variable} record, but {described} "
                    f"leave{'s' if len(loose) == 1 else ''} several. Make "
                    f"{'it' if len(loose) == 1 else 'them'} {placed}, or pass "
                    f"{param} as a table."
                )

    Log.info(
        "[custom] %s: %s-level, layout from %s (%s, drawn as %s), inputs %s, "
        "as_table %s, collapse %s",
        signature.function,
        signature.level,
        primary,
        primary_variable,
        kind,
        {p: f"{call.variables[p]}[{len(frames[p])} row(s), keys {factors[p]}]" for p in frames},
        sorted(tables),
        steps.all or "nothing",
        layer=LAYER,
    )
    return Prepared(
        spec=spec,
        call=call,
        layout_spec=layout_spec,
        layout_table=primary_table,
        roles=roles,
        steps=steps,
        frames=frames,
        factors=factors,
        tables=tables,
        notes=notes,
    )


def custom_requirement(spec: PlotSpec, call: CustomCall) -> str | None:
    """Why this custom figure cannot be drawn, or None when it can."""
    try:
        prepare(spec, call)
    except RoleError as exc:
        return str(exc)
    return None


#: Why each built-in-only section of the panel is off for a custom figure.
#: The function draws the marks, so settings that re-express or decorate the
#: layout input's own values would draw over marks they know nothing about.
_NOT_FOR_CUSTOM = {
    "sample_overlay": "Show sample draws the layout input's own values; a plot_ "
    "function draws its own marks.",
    "difference_bars": "Difference bars are placed on a built-in kind's marks.",
    "comparison": "Compare to reference re-expresses a built-in kind's values; "
    "compute the comparison in the plot_ function instead.",
    "data_export": "Save data is not available for a plot_ function's figure yet.",
}


def custom_capabilities(spec: PlotSpec, call: CustomCall) -> dict:
    """The capability report for a custom figure.

    The roles, factors, grouping and variants are the LAYOUT input's
    (``capability.capabilities`` on it, so the panel's controls behave as
    for any figure). The kind list is the function alone, so the panel's kind
    grid cannot switch a ``plot_`` node's figure to a built-in kind, and
    ``custom`` says how the function is called and why it cannot be, if so.
    """
    from .capability import capabilities

    primary = call.primary
    variable = call.variables[primary]
    table = call.inputs[primary]
    kind = layout_kind(table.shape_of(variable))
    caps = capabilities(_input_spec(spec, variable, variable, kind), table)
    reason = custom_requirement(spec, call)
    signature = call.signature
    entry = {
        "kind": str(PlotKind.CUSTOM),
        "available": reason is None,
        "reason": reason,
        "collapses": False,
        "assignment": None,
        "label": signature.function,
    }
    caps["kinds"] = [entry]
    caps["available"] = [str(PlotKind.CUSTOM)]
    caps["default"] = str(PlotKind.CUSTOM)
    for section, why in _NOT_FOR_CUSTOM.items():
        caps[section] = {**caps.get(section, {}), "available": False, "reason": why}
    tables = call.as_table_params(spec.custom.as_table if spec.custom else True)
    caps["custom"] = {
        "function": signature.function,
        "level": signature.level,
        "layout_input": primary,
        "layout_kind": str(kind),
        "reason": reason,
        "accepts_context": signature.accepts_context,
        "inputs": [
            {
                "param": param,
                "variable": call.variables[param],
                "shape": str(call.inputs[param].shape_of(call.variables[param])),
                "keys": list(call.inputs[param].schema_levels),
                "as_table": param in tables,
            }
            for param in call.variable_params
        ],
        "constants": sorted(call.constants),
    }
    return caps


# ---------------------------------------------------------------------------
# Resolving: the built-in layout, with the function attached
# ---------------------------------------------------------------------------


def resolve_custom(
    spec: PlotSpec,
    call: CustomCall,
    *,
    max_points: int | None = None,
    narrate: bool = False,
    on_figure=None,
):
    """Every figure of the fan-out, each carrying its :class:`CustomDraw`.

    The same figure set ``resolve`` builds for the layout input: one figure
    per ITERATE combination, in the same order, so the panel's navigator and
    Save all work unchanged.
    """
    from .reduce import resolve

    prepared = prepare(spec, call)
    figures = resolve(
        prepared.layout_spec,
        prepared.layout_table,
        max_points=max_points,
        narrate=narrate,
        on_figure=on_figure,
    )
    return [_attach(figure, prepared) for figure in figures]


def resolve_custom_one(
    spec: PlotSpec,
    call: CustomCall,
    index: int,
    *,
    max_points: int | None = None,
    narrate: bool = False,
):
    """One figure of the fan-out, as ``resolve_one``: ``(figure, labels, index)``."""
    from .reduce import resolve_one

    prepared = prepare(spec, call)
    figure, labels, position = resolve_one(
        prepared.layout_spec,
        prepared.layout_table,
        index,
        max_points=max_points,
        narrate=narrate,
    )
    return _attach(figure, prepared), labels, position


def _attach(figure, prepared: Prepared):
    """Put the draw on the figure and give its panels the custom y rule."""
    from .panels import override_for
    from .reduce import _figure_limits
    from .ylimits import limits_for

    spec = prepared.layout_spec
    # The layout input's computed range describes ITS values, not what the
    # function draws. Only a range the user pinned (both ends, per figure or
    # per panel) is kept; everything else autoscales to the drawn marks, and
    # a single pinned end is applied after the call (`CustomDraw.draw_panel`).
    for panel in figure.panels:
        panel.y_limits = limits_for({}, {}, [], spec.y_axis, override_for(spec, panel.key))
    figure.y_limits = _figure_limits(figure.panels)
    figure.custom = CustomDraw(prepared=prepared, figure_key=dict(figure.figure_key))
    return figure


# ---------------------------------------------------------------------------
# The context a function may ask for
# ---------------------------------------------------------------------------


@dataclass
class PlotContext:
    """What scistackplot decided, handed to a function that names ``context``.

    Use it rather than re-deriving any of it, so the marks agree with the
    ticks, the legend and the colours the user pinned:

    * ``x(levels)``: where a mark for these GROUP levels goes on a categorical
      x axis (``levels`` is a mapping or a table row);
    * ``color_of(level)``: the colour of a level of the coloured layer, with
      pinned and project colours applied;
    * ``label(thing)`` / ``level_label(factor, level)``: what a factor and a
      level read as, with the display aliases applied.
    """

    figure_key: dict[str, Any]
    panel_key: dict[str, Any]
    #: The GROUP factors, innermost first (``PlotSpec.groups``).
    groups: list[str]
    #: The factors sharing the x axis, outermost first.
    x_layers: list[str]
    #: The factor the marks are coloured by, or None.
    color: str | None
    #: The sample key(s): what one row of a collapsed input stands for.
    sample: list[str]
    #: Width and height in inches, text sizes, palette, weights.
    style: Any
    resolved: Any = field(default=None, repr=False)

    def x(self, levels: Mapping[str, Any] | pd.Series) -> float:
        """The x position of the mark for ``levels``."""
        from .render.base import is_categorical_x, x_positions
        from .xaxis import leaf_key

        resolved = self.resolved
        if resolved is None or not is_categorical_x(resolved):
            raise ValueError(
                "context.x: this figure's x axis is numeric (no grouping on x), "
                "so place marks at their own x values."
            )
        missing = [name for name in self.x_layers if name not in levels]
        if missing:
            raise KeyError(
                f"context.x needs a level for every x layer {self.x_layers}; "
                f"missing {missing}"
            )
        key = leaf_key([levels[name] for name in self.x_layers])
        position = x_positions(pd.Series([key]), resolved)[0][0]
        if np.isnan(position):
            raise KeyError(f"context.x: {dict((n, levels[n]) for n in self.x_layers)} "
                           f"is not on this figure's x axis")
        return float(position)

    def color_of(self, level: Any = None) -> str:
        """The colour of ``level`` of the coloured layer (or the one mark colour)."""
        from .render.base import palette_for

        order = [str(v) for v in (self.resolved.color_order or [])] if self.resolved else []
        fallback = order.index(str(level)) if str(level) in order else 0
        return palette_for(self.resolved, level, fallback)

    def label(self, thing: str) -> str:
        """What a factor or variable reads as."""
        text = getattr(self.resolved, "text", None)
        return text.name(thing, thing) if text is not None else thing

    def level_label(self, factor: str, level: Any) -> str:
        """What one level reads as."""
        text = getattr(self.resolved, "text", None)
        return text.level(factor, level) if text is not None else str(level)


# ---------------------------------------------------------------------------
# Making the calls
# ---------------------------------------------------------------------------

#: Set while autosize lays a figure out to measure it. The marks do not move
#: the labels it measures, so the function is not called for a trial layout:
#: only the final render calls it.
_MEASURING: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "scistackplot_custom_measuring", default=False
)


@contextlib.contextmanager
def measuring() -> Iterator[None]:
    token = _MEASURING.set(True)
    try:
        yield
    finally:
        _MEASURING.reset(token)


def _describe_value(value: Any) -> str:
    """An argument's shape for the log: never its contents."""
    if isinstance(value, pd.DataFrame):
        return f"DataFrame[{len(value)} row(s) x {list(value.columns)}]"
    if isinstance(value, np.ndarray):
        return f"ndarray{value.shape}"
    return type(value).__name__


@dataclass
class CustomDraw:
    """The function, ready to be called for one figure's panels."""

    prepared: Prepared
    figure_key: dict[str, Any]
    #: How many calls were made and the seconds spent inside the function.
    calls: int = 0
    seconds: float = 0.0

    @property
    def function(self) -> str:
        return self.prepared.call.signature.function

    @property
    def level(self) -> str:
        return self.prepared.level

    # --- arguments ------------------------------------------------------

    def arguments(self, panel_key: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """The variable and constant arguments of the call for one panel."""
        prepared = self.prepared
        selection = {**self.figure_key, **dict(panel_key or {})}
        kwargs: dict[str, Any] = dict(prepared.call.constants)
        for param, frame in prepared.frames.items():
            variable = prepared.call.variables[param]
            columns = prepared.factors[param]
            rows = _select(frame, selection, columns)
            averaged = [key for key in prepared.steps.pre if key in columns]
            if averaged:
                rows = _average(rows, averaged, columns, variable, param)
            if param in prepared.tables:
                kwargs[param] = rows.reset_index(drop=True)
            else:
                kwargs[param] = _single_value(rows, variable, param, selection)
        return kwargs

    def context(self, resolved, panel_key: Mapping[str, Any] | None = None) -> PlotContext:
        prepared = self.prepared
        return PlotContext(
            figure_key=dict(self.figure_key),
            panel_key=dict(panel_key or {}),
            groups=[g for g in prepared.layout_spec.groups if prepared.roles.get(g) is Role.GROUP],
            x_layers=list(resolved.x_layers),
            color=resolved.color_factor,
            sample=list(prepared.steps.sample),
            style=prepared.spec.style,
            resolved=resolved,
        )

    def _call(self, kwargs: dict[str, Any], where: str):
        Log.debug(
            "[custom] calling %s (%s): %s",
            self.function,
            where,
            {name: _describe_value(value) for name, value in kwargs.items()},
            layer=LAYER,
        )
        started = time.perf_counter()
        try:
            return self.prepared.call.fn(**kwargs)
        except Exception as exc:
            Log.error(
                "[custom] %s raised in %s: %s: %s",
                self.function,
                where,
                type(exc).__name__,
                exc,
                layer=LAYER,
                exc_info=True,
            )
            raise CustomFunctionError(
                f"{self.function} raised {type(exc).__name__} in {where}: {exc}"
            ) from exc
        finally:
            self.calls += 1
            self.seconds += time.perf_counter() - started

    # --- panel level ----------------------------------------------------

    def draw_panel(self, ax, panel, resolved) -> None:
        """Call the function for one panel, onto ``ax``."""
        from .panels import override_for
        from .ylimits import pinned_ends

        if _MEASURING.get():
            return
        kwargs = self.arguments(panel.key)
        if all(_is_empty(kwargs.get(p)) for p in self.prepared.frames):
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    transform=ax.transAxes, color="#888888")
            return
        kwargs[AX_PARAM] = ax
        if self.prepared.call.signature.accepts_context:
            kwargs[CONTEXT_PARAM] = self.context(resolved, panel.key)
        where = _where(self.figure_key, panel.key)
        self._call(kwargs, where)
        # One pinned end, against the drawn marks (both ends are on the panel).
        low, high = pinned_ends(
            self.prepared.layout_spec.y_axis,
            override_for(self.prepared.layout_spec, panel.key),
        )
        if (low is None) != (high is None):
            ax.set_ylim(bottom=low, top=high)

    # --- figure level ---------------------------------------------------

    def draw_figure(self, resolved):
        """Call the function once for the whole figure; return what it drew,
        sized to ``resolved``'s style (matplotlib) or left as returned
        (plotly, which is sized when it is written)."""
        kwargs = self.arguments({})
        if self.prepared.call.signature.accepts_context:
            kwargs[CONTEXT_PARAM] = self.context(resolved, {})
        where = _where(self.figure_key, {})
        result = self._call(kwargs, where)
        style = resolved.spec.style
        if is_matplotlib_figure(result):
            result.set_size_inches(style.width, style.height)
        elif not is_plotly_figure(result):
            raise CustomFunctionError(
                f"{self.function} returned {type(result).__name__} for {where}; a "
                f"function without an 'ax' parameter must return a matplotlib or "
                f"plotly figure."
            )
        Log.info(
            "[custom] %s drew %s as a %s figure in %.3fs",
            self.function,
            where,
            "matplotlib" if is_matplotlib_figure(result) else "plotly",
            self.seconds,
            layer=LAYER,
        )
        return result


def is_matplotlib_figure(value: Any) -> bool:
    return hasattr(value, "savefig") and hasattr(value, "set_size_inches")


def is_plotly_figure(value: Any) -> bool:
    return hasattr(value, "to_plotly_json") and hasattr(value, "write_image")


def _where(figure_key: Mapping[str, Any], panel_key: Mapping[str, Any]) -> str:
    parts = [f"{k}={v}" for k, v in {**figure_key, **panel_key}.items()]
    return ", ".join(parts) if parts else "the figure"


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    return isinstance(value, pd.DataFrame) and value.empty


def _select(frame: pd.DataFrame, selection: Mapping[str, Any], columns: list[str]) -> pd.DataFrame:
    """The rows matching ``selection`` on the keys this input carries, compared
    as text (a selection crosses JSON as strings, a column may hold ints)."""
    mask = pd.Series(True, index=frame.index)
    for name, value in selection.items():
        if name in columns:
            mask &= frame[name].astype(str) == str(value)
    return frame[mask]


def _average(
    rows: pd.DataFrame, keys: list[str], columns: list[str], variable: str, param: str
) -> pd.DataFrame:
    """Average ``variable`` over each of ``keys`` in turn, deepest first, within
    every other factor still present: the nested, unweighted collapse
    (``roles.collapse_steps``). Scalars average as numbers; 1-D cells average
    sample by sample and must have equal lengths."""
    for key in keys:
        if key not in rows.columns:
            continue
        keep = [name for name in columns if name in rows.columns and name != key]
        if rows.empty:
            rows = rows.drop(columns=[key])
            continue
        if keep:
            # Row by row rather than `.agg`: pandas refuses an aggregation
            # that returns an array, which is what a 1-D cell's mean is.
            records = []
            for values, group in rows.groupby(keep, dropna=False, sort=False):
                values = values if isinstance(values, tuple) else (values,)
                record = dict(zip(keep, values))
                record[variable] = _mean_cells(group[variable], variable, param)
                records.append(record)
            rows = pd.DataFrame(records, columns=[*keep, variable])
        else:
            rows = pd.DataFrame({variable: [_mean_cells(rows[variable], variable, param)]})
    return rows


def _mean_cells(cells: pd.Series, variable: str, param: str) -> Any:
    first = cells.iloc[0] if len(cells) else None
    if first is None or np.isscalar(first):
        return pd.to_numeric(cells, errors="coerce").mean()
    arrays = [np.asarray(cell, dtype=float) for cell in cells]
    lengths = {a.shape for a in arrays}
    if len(lengths) > 1:
        raise CustomFunctionError(
            f"{param} ({variable}): cannot average records of different lengths "
            f"{sorted(lengths)}. Collapse fewer keys, or resample the data to one "
            f"length first."
        )
    return np.mean(np.stack(arrays), axis=0)


def _single_value(rows: pd.DataFrame, variable: str, param: str, selection) -> Any:
    if rows.empty:
        return None
    if len(rows) > 1:
        # `prepare` refuses the roles that allow this; reaching it means a
        # factor the layout does not know (a second input's own variants).
        raise CustomFunctionError(
            f"{param} is passed as a value, but {len(rows)} {variable} records match "
            f"{dict(selection)}. Pass {param} as a table, or pin its variant."
        )
    return rows[variable].iloc[0]


# ---------------------------------------------------------------------------
# Drawing, previewing and writing a custom figure
# ---------------------------------------------------------------------------

#: Preview images are drawn at this many pixels per inch: 1 pt = 1 px (the
#: panel's convention, ``PREVIEW_PX_PER_IN`` in the GUI) at 2x for sharp text
#: on a high-density screen.
PREVIEW_DPI = 144

#: plotly measures in CSS pixels, 96 to the inch.
PLOTLY_PX_PER_IN = 96

#: What to install when a plotly figure cannot be written.
PLOTLY_EXPORT_FIX = (
    "Writing a plotly figure to a file needs kaleido: "
    "pip install 'scistackplot[plotly-export]'. kaleido also needs Chrome; if it "
    "says Chrome is missing, run: plotly_get_chrome"
)


def resized(resolved, width_in: float | None, height_in: float | None):
    """``resolved`` drawn at another size (``None`` keeps that side), through
    the one owner of resizing a laid-out figure, ``autosize.at_size``."""
    from .autosize import at_size

    style = resolved.spec.style
    return at_size(
        resolved,
        style.width if width_in is None else float(width_in),
        style.height if height_in is None else float(height_in),
    )


def draw(resolved, *, width_in: float | None = None, height_in: float | None = None):
    """A custom figure, drawn: a matplotlib ``Figure``, or the plotly figure a
    figure-level function returned. The caller closes a matplotlib one."""
    if resolved.custom is None:
        raise ValueError("draw: not a custom figure (ResolvedPlot.custom is None)")
    resolved = resized(resolved, width_in, height_in)
    if resolved.custom.level == PANEL_LEVEL:
        from .render import render_matplotlib

        return render_matplotlib(resolved)

    import matplotlib

    if matplotlib.get_backend().lower() not in ("agg", "template"):
        matplotlib.use("Agg", force=False)
    import matplotlib.pyplot as plt

    from .autosize import settle
    from .paper import figure_rc_params
    from .textsize import sizes_for

    resolved = settle(resolved)
    # The paper and the text sizes are in force while the function draws, so
    # its own artists are made on the same page as a built-in kind's.
    with plt.rc_context(figure_rc_params(sizes_for(resolved))):
        return resolved.custom.draw_figure(resolved)


def preview_payload(
    resolved, *, width_in: float | None = None, height_in: float | None = None
) -> dict:
    """What the panel shows for a custom figure.

    ``{"kind": "image", ...}``: a PNG of exactly what Save writes (matplotlib).
    ``{"kind": "plotly", "figure": {...}}``: the plotly figure itself, which
    the panel draws interactively.
    """
    import base64
    import io
    import json

    started = time.perf_counter()
    figure = draw(resolved, width_in=width_in, height_in=height_in)
    if is_plotly_figure(figure):
        payload = {"kind": "plotly", "figure": json.loads(figure.to_json())}
        Log.info(
            "[custom] preview: plotly figure from %s in %.3fs",
            resolved.custom.function,
            time.perf_counter() - started,
            layer=LAYER,
        )
        return payload

    import matplotlib.pyplot as plt

    from .paper import PAPER

    try:
        width, height = (float(v) for v in figure.get_size_inches())
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png", dpi=PREVIEW_DPI, facecolor=PAPER.background)
    finally:
        plt.close(figure)
    Log.info(
        "[custom] preview: %.2f x %.2f in PNG (%d bytes) from %s in %.3fs "
        "(%d call(s), %.3fs in the function)",
        width,
        height,
        buffer.tell(),
        resolved.custom.function,
        time.perf_counter() - started,
        resolved.custom.calls,
        resolved.custom.seconds,
        layer=LAYER,
    )
    return {
        "kind": "image",
        "mime": "image/png",
        "data": base64.b64encode(buffer.getvalue()).decode("ascii"),
        "width_in": width,
        "height_in": height,
        "dpi": PREVIEW_DPI,
    }


def write_plotly(figure, target, *, width_in: float, height_in: float, dpi: float,
                 image_format: str | None = None) -> None:
    """Write a plotly figure at ``width_in x height_in`` through kaleido.

    plotly sizes in CSS pixels (96 per inch); ``scale`` makes a raster
    ``dpi`` pixels per inch. A missing kaleido or Chrome fails with the fix.
    """
    options = {
        "width": round(width_in * PLOTLY_PX_PER_IN),
        "height": round(height_in * PLOTLY_PX_PER_IN),
        "scale": float(dpi) / PLOTLY_PX_PER_IN,
    }
    if image_format:
        options["format"] = image_format.lstrip(".")
    try:
        figure.write_image(target, **options)
    except Exception as exc:
        text = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, ImportError) or "kaleido" in text.lower() or "chrome" in text.lower():
            Log.error("[custom] plotly write failed: %s", text, layer=LAYER)
            raise RuntimeError(f"{PLOTLY_EXPORT_FIX}. ({text})") from exc
        raise
