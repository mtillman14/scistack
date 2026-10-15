"""
Plot Studio opened on a ``plot_`` node: the GUI half of custom plot kinds.

The figure is drawn by the user's own ``plot_`` function
(``scistackplot.custom`` owns every rule about calling it). This module only
adapts the canvas to it:

* :func:`node_wiring` reads the node's input edges through the same owners a
  run uses (``edge_view.effective_edges`` + ``edge_resolver``), so Plot Studio
  can never bind an input differently from the canvas;
* :func:`build_call` turns that wiring plus the loaded tables into a
  ``scistackplot.CustomCall``;
* :func:`describe_function`, :func:`resolve_figures`, :func:`capabilities_for`
  and :func:`resolve_for_save` are the Plot Studio entry points for a spec
  whose kind is ``custom``. ``plot_service`` routes to them.

The edges are read again on every request (``CustomPlot.node`` names the
node), so a node rewired while Plot Studio is open draws its new inputs.
See ``.claude/plan-custom-plot-kinds.md`` and docs/claude/custom-plot-kinds.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import Any

from scistack_gui.ids import parse_fn_node_id, strip_placement

logger = logging.getLogger(__name__)


class CustomPlotError(ValueError):
    """A ``plot_`` node Plot Studio cannot open, with the fix in the message."""


@dataclass
class NodeWiring:
    """What a ``plot_`` node's edges bind."""

    node_id: str
    function: str
    #: ``{param: variable type name}``
    variables: dict[str, str] = field(default_factory=dict)
    #: ``{param: value}``: Parameter values, and ``None`` for what Plot Studio
    #: does not supply (an unwired parameter, a PathInput).
    constants: dict[str, Any] = field(default_factory=dict)
    #: Shown in the panel: every argument passed something the canvas did not
    #: say, and why.
    notes: list[str] = field(default_factory=list)


def node_wiring(db, node_id: str) -> NodeWiring:
    """The function and input bindings of the ``plot_`` node ``node_id``."""
    from scidb import function_role

    from scistack_gui import pipeline_store, registry
    from scistack_gui.domain.edge_resolver import (
        BINDING_PATHINPUT,
        BINDING_PARAMETER,
        BINDING_VARIABLE,
        resolve_function_edges,
    )
    from scistack_gui.domain.edge_view import effective_edges, run_scope

    view = effective_edges(db, run_scope(db, node_id), caller=f"plot_custom({node_id})")
    meta = view.manual_nodes.get(node_id) or view.manual_nodes.get(strip_placement(node_id))
    parsed = parse_fn_node_id(node_id)
    if meta is not None:
        if meta.get("type") != "functionNode":
            raise CustomPlotError(f"Node {node_id} is a {meta.get('type')}, not a plot_ function.")
        function = meta["label"]
    elif parsed is not None:
        function = parsed[0]
    else:
        raise CustomPlotError(
            f"Node {node_id} is not a function node this GUI recognises. Refresh the canvas "
            f"and open Plot Studio from the plot_ node again."
        )
    if function_role(function) != "plot":
        raise CustomPlotError(
            f"{function} is not a plot_ function: Plot Studio draws a node with your own "
            f"function only when its name starts with 'plot_'."
        )

    resolved = resolve_function_edges(
        fn_node_ids={node_id},
        manual_edges=view.drawn_list,
        manual_nodes=view.manual_nodes,
        existing_node_labels={},
    )
    bindings = dict(resolved.bindings)
    history_constants: dict[str, Any] = {}
    if not bindings:
        # A node that has run with record=True draws its inputs from history
        # rather than from drawn edges: take the one target THIS node runs.
        from scistack_gui.services.execution_service import derive_target_for_node

        targets = derive_target_for_node(db, node_id)
        if targets:
            target = targets[0]
            bindings = dict(target.get("bindings") or {})
            if not bindings:
                bindings = {
                    p: {"kind": BINDING_VARIABLE, "ref": t}
                    for p, t in (target.get("input_types") or {}).items()
                    if t
                }
            history_constants = dict(target.get("constants") or {})
            logger.info(
                "[plot] custom %s: no drawn input edges; wiring from history %s",
                node_id,
                {p: b.get("ref") for p, b in bindings.items()},
            )

    wiring = NodeWiring(node_id=node_id, function=function)
    parameters: dict[str, str] = {}
    for param, binding in bindings.items():
        kind, ref = binding.get("kind"), binding.get("ref")
        if kind == BINDING_VARIABLE:
            refs = list(ref) if isinstance(ref, (list, tuple)) else [ref]
            if len(refs) != 1:
                raise CustomPlotError(
                    f"{function}: input {param!r} is fed by {len(refs)} variables "
                    f"({', '.join(map(str, refs))}). Plot Studio draws one variable per "
                    f"input; connect a single variable."
                )
            if binding.get("columns"):
                wiring.notes.append(
                    f"{param}: the column selection on its edge is not applied in Plot "
                    f"Studio; the whole variable is passed."
                )
            wiring.variables[param] = str(refs[0])
        elif kind == BINDING_PARAMETER:
            parameters[param] = str(ref)
        elif kind == BINDING_PATHINPUT:
            wiring.constants[param] = None
            wiring.notes.append(
                f"{param}: fed by the PathInput {ref!r}, which Plot Studio does not "
                f"resolve; passed None."
            )

    if parameters:
        from scistack_gui.services.execution_service import _infer_wired_constants

        values = _infer_wired_constants(
            parameters,
            pipeline_store.get_pending_constants(db),
            registry.get_parameters_registry(),
            log_context=f"plot_custom '{function}'",
        )
        for param in parameters:
            options = values.get(param) or (
                [history_constants[param]] if param in history_constants else []
            )
            if not options:
                wiring.constants[param] = None
                wiring.notes.append(f"{param}: its Parameter has no value yet; passed None.")
                continue
            wiring.constants[param] = options[0]
            if len(options) > 1:
                wiring.notes.append(
                    f"{param}: its Parameter has {len(options)} values; Plot Studio draws "
                    f"the first ({options[0]!r})."
                )

    logger.info(
        "[plot] custom node %s (%s): variables %s, constants %s, %d note(s)",
        node_id,
        function,
        wiring.variables,
        sorted(wiring.constants),
        len(wiring.notes),
    )
    return wiring


def build_call(db, source, spec, *, wiring: NodeWiring | None = None):
    """``(CustomCall, notes)`` for a custom ``spec``, its node's edges read now.

    Runs inside the caller's database hold: it reads edges and loads tables.
    """
    from scistackplot import CustomCall, inspect_function

    from scistack_gui import registry

    if spec.custom is None or not spec.custom.node:
        raise CustomPlotError(
            "This plot names no plot_ node to read its inputs from. Open Plot Studio "
            "from the plot_ node on the canvas."
        )
    wiring = wiring or node_wiring(db, spec.custom.node)
    if wiring.function != spec.custom.function:
        raise CustomPlotError(
            f"The node {spec.custom.node} now runs {wiring.function}, but this plot was "
            f"drawn by {spec.custom.function}. Open Plot Studio from that node again."
        )
    try:
        fn = registry.get_function(wiring.function)
    except KeyError as exc:
        raise CustomPlotError(str(exc)) from exc

    signature = inspect_function(fn)
    notes = list(wiring.notes)
    constants = dict(wiring.constants)
    supplied = set(wiring.variables) | set(constants) | {"ax", "context"}
    for param in signature.parameters:
        if param not in supplied and param not in signature.optional:
            # The output path of a plot_ endpoint is never an edge in the GUI
            # (scidb takes it as a PathOutput input); Plot Studio writes the
            # file itself. Passed None, and said so.
            constants[param] = None
            notes.append(f"{param}: not wired on the canvas; passed None.")

    tables = {}
    for param, variable in wiring.variables.items():
        tables[param] = source.get_table([variable])
    call = CustomCall(
        fn=fn,
        inputs=tables,
        variables=dict(wiring.variables),
        constants=constants,
        signature=signature,
    )
    if spec.factor_variables and call.variable_params:
        # Grouping variables join onto the layout input only: the factors a
        # role can name are the layout input's.
        primary = call.primary
        tables[primary] = source.get_table(
            [wiring.variables[primary]], factor_variables=list(spec.factor_variables)
        )
    return call, notes


def _load(db, spec_payload: dict, *, label: str):
    """``(source, spec, call, notes)``, holding the database only for the load
    (the same narrow hold as ``plot_service._load``)."""
    from scistack_gui.db import db_connection
    from scistack_gui.services import plot_service

    with db_connection(label, needed=True):
        source = plot_service.get_source(db)
        spec = plot_service._spec_from_payload(spec_payload)
        call, notes = build_call(db, source, spec)
    return source, spec, call, notes


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def describe_function(db, node_id: str, *, refresh: bool = False) -> dict:
    """Everything the panel needs to open on a ``plot_`` node.

    The same shape as ``plot_service.describe`` for the LAYOUT input (so the
    variant rows, groupings and pickers work unchanged), with the spec, the
    capability report and ``plot_node`` describing the function.
    """
    from scistackplot import CustomPlot, PlotKind, PlotSpec, custom_capabilities

    from scistack_gui.db import db_connection
    from scistack_gui.services import plot_service

    with db_connection("plot_describe_function", needed=True):
        source = plot_service.get_source(db, refresh=refresh)
        wiring = node_wiring(db, node_id)
        if not wiring.variables:
            raise CustomPlotError(
                f"{wiring.function} has no variable input wired. Connect a variable to "
                f"one of its inputs on the canvas, then open Plot Studio again."
            )
        probe = PlotSpec(
            measures=[next(iter(wiring.variables.values()))],
            kind=PlotKind.CUSTOM,
            custom=CustomPlot(
                function=wiring.function,
                inputs=dict(wiring.variables),
                node=node_id,
            ),
        )
        call, notes = build_call(db, source, probe, wiring=wiring)
        primary_variable = call.variables[call.primary]
        described = plot_service._describe(db, primary_variable, refresh=False, csv_path=None)

    if not described.get("eligible"):
        return {**described, "plot_node": _plot_node(wiring, call, notes)}
    base = PlotSpec.from_dict(described["spec"])
    spec = replace(
        base,
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(
            function=wiring.function,
            as_table=True,
            inputs=dict(call.variables),
            node=node_id,
        ),
    )
    caps = custom_capabilities(spec, call)
    logger.info(
        "[plot] describe_function(%s): %s, layout input %s (%s), %s-level, reason=%s",
        node_id,
        wiring.function,
        call.primary,
        primary_variable,
        caps["custom"]["level"],
        caps["custom"]["reason"],
    )
    return {
        **described,
        "spec": spec.to_dict(),
        "capabilities": caps,
        "plot_node": _plot_node(wiring, call, notes),
    }


def _plot_node(wiring: NodeWiring, call, notes: list[str]) -> dict:
    return {
        "node_id": wiring.node_id,
        "function": wiring.function,
        "level": call.signature.level,
        "inputs": dict(call.variables),
        "layout_input": call.primary if call.variable_params else None,
        "notes": list(notes),
    }


def capabilities_for(db, spec_payload: dict) -> dict:
    from scistackplot import custom_capabilities

    _, spec, call, notes = _load(db, spec_payload, label="plot_capabilities")
    caps = custom_capabilities(spec, call)
    caps["custom"]["notes"] = notes
    return caps


def _preview_size(spec, preview: dict | None) -> tuple[float | None, float | None, str]:
    """``(width_in, height_in, mode)`` to draw the preview at: the pane's size
    in pane mode (1 pt = 1 px, ``plot_service.PREVIEW_PX_PER_IN``), else the
    spec's own."""
    from scistack_gui.services.plot_service import PREVIEW_PX_PER_IN

    preview = preview or {}
    if preview.get("mode") == "pane" and preview.get("width_px") and preview.get("height_px"):
        return (
            float(preview["width_px"]) / PREVIEW_PX_PER_IN,
            float(preview["height_px"]) / PREVIEW_PX_PER_IN,
            "pane",
        )
    return None, None, "export"


def _figure_entry(figure, index: int, preview: dict | None) -> dict:
    from scistackplot import custom_preview

    width_in, height_in, mode = _preview_size(figure.spec, preview)
    payload = custom_preview(figure, width_in=width_in, height_in=height_in)
    style = figure.spec.style
    payload["meta"] = {
        "preview": {
            "mode": mode,
            "width_in": round(width_in if width_in else style.width, 2),
            "height_in": round(height_in if height_in else style.height, 2),
        },
    }
    return {
        "index": index,
        "key": figure.to_dict()["figure_key"],
        "label": figure.figure_label,
        # No plotly layout: the panel draws `custom_preview` instead.
        "figure": None,
        "custom_preview": payload,
        "row_count": figure.row_count,
        "downsampled_from": figure.downsampled_from,
    }


def resolve_figures(
    db,
    spec_payload: dict,
    *,
    max_points: int | None = None,
    figure_index: int | None = None,
    preview: dict | None = None,
) -> dict:
    """``plot_service.resolve_figures`` for a custom spec. The function is
    called while the preview is drawn, outside the database hold."""
    from scistackplot import (
        MAX_TRANSPORT_POINTS,
        CustomFunctionError,
        RoleError,
        resolve_custom,
        resolve_custom_one,
    )

    from scistack_gui.services.plot_service import _invalid_spec

    budget = MAX_TRANSPORT_POINTS if max_points is None else max_points
    try:
        _, spec, call, notes = _load(db, spec_payload, label="plot_resolve")
    except CustomPlotError as exc:
        return _invalid_spec(exc)
    try:
        if figure_index is not None:
            figure, labels, index = resolve_custom_one(spec, call, figure_index, max_points=budget)
            selected = [(index, figure)]
        else:
            figures = resolve_custom(spec, call, max_points=budget)
            labels = [f.figure_label for f in figures]
            selected = list(enumerate(figures))
            index = 0
        rendered = [_figure_entry(figure, position, preview) for position, figure in selected]
    except (RoleError, CustomFunctionError) as exc:
        result = _invalid_spec(exc)
        result["notes"] = notes
        return result
    logger.info(
        "[plot] resolved custom %s: figure %d of %d (%s-level, %d call(s), %.3fs in the function)",
        spec.custom.function,
        index,
        len(labels),
        call.signature.level,
        sum(f[1].custom.calls for f in selected),
        sum(f[1].custom.seconds for f in selected),
    )
    return {
        "ok": True,
        "error": None,
        "figures": rendered,
        "figure_labels": labels,
        "figure_count": len(labels),
        "figure_index": index,
        "notes": list(notes),
    }


def resolve_for_save(db, spec_payload: dict, *, figure_index: int | None, narrate: bool, on_figure):
    """``(spec, figures)`` for ``plot_service.save_figure``. ``write_figure``
    draws a custom figure with its function (``scistackplot.custom.draw``)."""
    from scistackplot import resolve_custom, resolve_custom_one

    _, spec, call, _notes = _load(db, spec_payload, label="plot_save_start")
    if figure_index is None:
        return spec, list(
            resolve_custom(spec, call, narrate=narrate, on_figure=on_figure)
        )
    if on_figure is not None:
        on_figure(1, 1, None)
    figure, _labels, _position = resolve_custom_one(spec, call, figure_index, narrate=narrate)
    return spec, [figure]


def is_custom(spec_payload: dict | None) -> bool:
    """Whether a request's spec is drawn by a ``plot_`` function. Read off the
    payload so the router never parses a spec twice."""
    return bool(spec_payload) and spec_payload.get("kind") == "custom"
