"""
Plot Studio backend.

One service, two transports: the FastAPI routes in ``api/plot.py`` and the
JSON-RPC handlers in ``server.py`` both call these functions, so the web GUI
and the VS Code extension can never drift apart.

Everything plot-related that is *policy* — which kinds are available, what the
default roles are, how data is reduced — lives in ``scistackplot`` /
``scistackplotdb`` (CLAUDE.md NOTE 3). This module only adapts: it turns JSON
payloads into specs, holds the source cache, and hands results back JSON-safe.

The database handle is the ONE the GUI already owns. A second DuckDB
connection would reintroduce the write-lock contention the MATLAB
run-ownership work resolved — see docs/claude/matlab-run-database-ownership.md.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Cached sources, keyed by ``("db", <database file path>)`` or
#: ``("csv", <path>)``.
#:
#: A run that writes records makes a db source stale — it caches whole frames,
#: so a stale one serves PRE-RUN data and the user plots the wrong thing. That
#: is what ``invalidate()`` is for; ``api.run._notify_records_changed`` calls it
#: wherever a run announces ``dag_updated``.
#:
#: Keyed by path rather than by ``id(db)``: CPython reuses ids after garbage
#: collection, so a freshly-opened DatabaseManager could land on a dead entry's
#: key and inherit another database's cached frames.
_sources: dict[tuple, Any] = {}


def _database_path(db) -> str:
    """The database file a source is keyed on.

    Accepts a ``DatabaseManager`` or a plain path, so callers that hold no
    connection (the MATLAB run threads release it to the sidecar) can still
    name the cache entry.
    """
    return str(getattr(db, "dataset_db_path", None) or db)


def _require_scistackplot():
    """Import the plotting packages, with an actionable message if missing."""
    try:
        import scistackplot  # noqa: F401
        import scistackplotdb  # noqa: F401
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Plotting needs the scistackplot and scistackplotdb packages: "
            "pip install scistackplot scistackplotdb"
        ) from exc


def get_source(db, *, refresh: bool = False, csv_path: str | None = None):
    """
    The cached source for this request.

    ``csv_path`` selects the standalone CSV implementation instead of the
    scidb one. Both satisfy the same ``DataSource`` protocol, so nothing below
    this line changes — which is the point: the CSV path stays a first-class
    entry point (right-click a .csv in the Explorer, no project, no database)
    rather than a degraded mode, and keeps the standalone claim exercised.
    """
    _require_scistackplot()

    if csv_path:
        from scistackplot import CsvSource

        key = ("csv", csv_path)
        if refresh or key not in _sources:
            _sources[key] = CsvSource(csv_path)
            logger.info("[plot] built CsvSource for %s", csv_path)
        return _sources[key]

    from scistackplotdb import ScidbSource

    key = ("db", _database_path(db))
    if refresh or key not in _sources:
        _sources[key] = ScidbSource(db)
        logger.info("[plot] built ScidbSource for %s (refresh=%s)", key[1], refresh)
    return _sources[key]


def invalidate(db=None) -> dict:
    """Drop cached frames — call after a run may have written records.

    ``db`` may be a ``DatabaseManager`` or a plain path; None drops every
    cached source. Dropping a source that was never built is a no-op, so this
    is safe to call unconditionally after any run.
    """
    if db is None:
        dropped = len(_sources)
        _sources.clear()
    else:
        dropped = 1 if _sources.pop(("db", _database_path(db)), None) else 0
    logger.info(
        "[plot] source cache invalidated (%s, %d source(s) dropped)",
        "all" if db is None else _database_path(db),
        dropped,
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Describe
# ---------------------------------------------------------------------------


def describe(
    db,
    variable: str | None = None,
    *,
    refresh: bool = False,
    csv_path: str | None = None,
) -> dict:
    """
    Everything the panel needs to open on a variable.

    With no ``variable``, returns just the catalog (what is plottable at all).
    With one, adds that variable's factors, a default spec, and the capability
    report — one round trip from "user right-clicked a node" to a rendered
    panel.
    """
    from scistack_gui.db import db_connection

    # Unlike the spec-driven entry points there is no clean load/compute split
    # here — `describe` interleaves source queries (catalog, stackable,
    # groupable, joinable) with cheap in-memory work, and it runs once when the
    # panel opens rather than on every control change. So the whole body takes
    # the hold; there is nothing worth narrowing further.
    with db_connection("plot_describe", needed=not csv_path):
        return _describe(db, variable, refresh=refresh, csv_path=csv_path)


def _describe(db, variable, *, refresh, csv_path) -> dict:
    """:func:`describe`'s body, run with the database connection held."""
    from scistackplot import capabilities, default_spec

    source = get_source(db, refresh=refresh, csv_path=csv_path)
    catalog = source.describe()

    # Entry points that name no variable (the CSV command, the palette with an
    # empty box) open on the first plottable measure rather than on nothing.
    if not variable:
        variable = source.default_measure()
    if not variable:
        return {"catalog": catalog, "variable": None}

    table = source.get_table([variable])
    shape = table.shape_of(variable)
    if not _plottable(shape):
        return {
            "catalog": catalog,
            "variable": variable,
            "eligible": False,
            "reason": _ineligible_reason(shape, len(table.frame)),
            "table": table.describe(),
        }

    # default_spec owns roles + kind + facet wrap together: a 13-field struct
    # opens as a wrapped grid of subplots, not one overplotted axis.
    spec = default_spec(table, variable)

    # Offers AND refusals: the variant picker draws every variable node on the
    # canvas, so one it cannot offer has to say why in place.
    stacking = (
        source.stackable_report(variable)
        if hasattr(source, "stackable_report")
        else {"offered": source.stackable_with(variable), "rejected": {}}
    )
    stackable = stacking["offered"]
    groupable = source.groupable_with(variable)
    # Which optional sections the panel can show, and why. Each of these gates a
    # control that is simply absent when the list is empty, so an empty list has
    # to be visible somewhere — otherwise "no other variable qualifies" and "the
    # feature is missing" look identical from the outside.
    logger.info(
        "[plot] describe(%s): %d stackable %s, %d groupable %s, %d joinable",
        variable,
        len(stackable),
        stackable,
        len(groupable),
        groupable,
        len(source.joinable_with(variable)),
    )

    return {
        "catalog": catalog,
        "variable": variable,
        "eligible": True,
        "reason": None,
        "table": table.describe(),
        "spec": spec.to_dict(),
        "capabilities": capabilities(spec, table),
        # Two different offers, deliberately kept apart: variables that can
        # STACK with this one (another series — a variant row) and variables
        # that can be JOINED as its x axis (a relational plot).
        "joinable_with": source.joinable_with(variable),
        "stackable_with": stackable,
        # Why each remaining variable is NOT offered, so the picker can draw it
        # refused-with-a-reason instead of leaving it inert and unexplained.
        "stackable_refused": stacking["rejected"],
        # Variables usable as a grouping FACTOR — recorded at or above this
        # variable's schema level, so each row gets exactly one of their values.
        "groupable_with": groupable,
    }


def _plottable(shape) -> bool:
    from scistackplot import Shape

    return shape in (Shape.SCALAR, Shape.SERIES_1D, Shape.MATRIX_2D)


def _ineligible_reason(shape, row_count: int) -> str:
    from scistackplot import Shape

    if row_count == 0:
        # The empty state the design doc insists on naming explicitly: a
        # variable whose pipeline has never run must say so, not draw blank axes.
        return "This variable has no records yet — run the pipeline first."
    if shape is Shape.CATEGORICAL:
        return "This variable holds text, which has no numeric axis to plot."
    return f"Not plottable: values classify as {shape}."


# ---------------------------------------------------------------------------
# Capabilities and resolution
# ---------------------------------------------------------------------------


def capabilities_for(db, spec_payload: dict, *, csv_path: str | None = None) -> dict:
    """Recompute available plot kinds after the user moves a factor's role."""
    from scistackplot import capabilities

    _, spec, table = _load(
        db, spec_payload, csv_path=csv_path, label="plot_capabilities"
    )
    return capabilities(spec, table)


def variant_graph(
    db,
    variable: str,
    *,
    functions: list[str] | None = None,
    csv_path: str | None = None,
) -> dict:
    """
    Variant axes and per-function versions, for the variant-selection popup.

    ``functions`` is every function node currently on the canvas: the popup
    mirrors the whole pipeline, so a node outside this variable's chain still
    has to be able to say what versions it has run (and, by their absence, that
    selecting one would not affect this figure).

    A CSV has no provenance and therefore no variants — it returns the empty
    graph rather than an error, so the popup can open and say so.
    """
    from scistack_gui.db import db_connection

    # The hold spans the whole body: `axis_node_bindings` below builds the
    # pipeline graph, which is its own set of database reads.
    with db_connection("plot_variant_graph", needed=not csv_path):
        return _variant_graph(db, variable, functions=functions, csv_path=csv_path)


def _variant_graph(db, variable, *, functions, csv_path) -> dict:
    """:func:`variant_graph`'s body, run with the database connection held."""
    source = get_source(db, csv_path=csv_path)
    if csv_path or not hasattr(source, "variant_graph"):
        return {
            "variable": variable,
            "axes": [],
            "versions": {},
            "chain_functions": [],
            # Present-but-empty, not absent: the picker reads this key on every
            # reply, and a CSV having no variants is an answer, not a gap.
            "default_selection": {},
        }
    graph = source.variant_graph(variable, functions or [])

    # What a NEW row for this variable should open on. Sent with the graph
    # because the picker needs it at exactly the moment it has the graph: a row
    # created by "+ Add variant" must arrive already pinned to one variant, or
    # clicking + silently adds every variant of that variable to the figure.
    #
    # Read from scistackplot rather than recomputed here (CLAUDE.md NOTE 3) —
    # the panel's opening pin and a row added later must be the same rule, or
    # the two disagree about what "one variant" means.
    from scistackplot import default_selection

    graph["default_selection"] = default_selection(source.get_table([variable]))

    logger.info(
        "[plot] variant_graph(%s): %d axes, %d function(s) with versions, "
        "default selection %s",
        variable,
        len(graph["axes"]),
        len(graph["versions"]),
        graph["default_selection"] or "none",
    )
    graph["node_bindings"] = axis_node_bindings(db, graph["axes"])
    return graph


#: Prefix ``graph_builder`` puts on a function's input-port handle. The suffix is
#: the function's own ARGUMENT name.
PARAM_HANDLE_PREFIX = "param__"


def axis_node_bindings(db, axes: list[dict], pipeline_id: str = "main") -> dict:
    """Which canvas node supplies each variant axis — ``{column: node_id}``.

    **Bound by PORT, never by name.** An edge into a function carries
    ``targetHandle = "param__<the function's own argument name>"``
    (``graph_builder.build_edges``, both the DB-derived and the manual path),
    and that argument name is exactly ``VariantAxis.param``. So the node feeding
    that port is the node holding the axis — whatever the node is called, and
    whatever type it is.

    The popup used to do this itself by comparing ``axis.param`` against a
    node's **label**, which is the Parameter ENTITY's name. Those are two
    different namespaces and they coincide only until someone renames a
    Parameter or wires a port from a glue node; after that the axis silently
    dropped out of the dialog, its node dimmed to "not a variant here", and the
    axis was listed as living in a nested pipeline. Measured on a real project
    2026-09-11 — see ``.claude/plan-plot-studio-variant-axis-fixes.md``
    Finding 2.

    It lives here, not in the webview, because it is a rule about what scidb's
    namespacing means (CLAUDE.md NOTE 3) — and because a rule in TSX has no
    test. Code axes are absent from the result: they bind to a function node by
    function NAME, one namespace, no port involved.

    Failure-tolerant: a dialog that cannot bind its axes is still worth opening
    with everything inert, and the caller sees an empty mapping.
    """
    if not axes:
        return {}
    try:
        from scistack_gui.services.pipeline_service import get_pipeline_graph

        built = get_pipeline_graph(db, pipeline_id)
        nodes = built.get("nodes") or []
        edges = built.get("edges") or []
        label_of = {n["id"]: (n.get("data") or {}).get("label", "") for n in nodes}
        type_of = {n["id"]: n.get("type") for n in nodes}

        params = [a for a in axes if a.get("kind") == "param"]
        bindings: dict[str, str] = {}
        for edge in edges:
            handle = edge.get("targetHandle") or ""
            if not handle.startswith(PARAM_HANDLE_PREFIX):
                continue
            argument = handle[len(PARAM_HANDLE_PREFIX) :]
            function = label_of.get(edge.get("target"), "")
            for axis in params:
                if axis.get("function") == function and axis.get("param") == argument:
                    bindings[axis["column"]] = edge.get("source")
                    break

        unbound = [a["column"] for a in params if a["column"] not in bindings]
        logger.info(
            "[plot] axis bindings: %s%s",
            {
                column: f"{label_of.get(node, '?')} [{type_of.get(node, '?')}]"
                for column, node in bindings.items()
            }
            or "none",
            f" — {len(unbound)} axis/axes feed no port on this canvas: {unbound}"
            if unbound
            else "",
        )
        return bindings
    except Exception:
        logger.exception("[plot] axis/node binding failed — axes will be inert")
        return {}


def _invalid_spec(exc) -> dict:
    """A role conflict is a user-correctable state, not a server fault.

    The panel shows the message — which names the one-line fix — instead of an
    error toast. Shared by both resolve paths so the two cannot report the same
    conflict in two shapes.
    """
    logger.info("[plot] invalid spec: %s", exc)
    return {
        "ok": False,
        "error": str(exc),
        "figures": [],
        "figure_labels": [],
        "figure_count": 0,
        "figure_index": 0,
        "notes": [],
    }


def resolve_figures(
    db,
    spec_payload: dict,
    *,
    max_points: int | None = None,
    figure_index: int | None = None,
    csv_path: str | None = None,
) -> dict:
    """
    Reduce and render for the interactive panel.

    Returns plotly.js figure dicts — plain JSON, built without the plotly
    package. Downsampling is applied here and only here: the export path
    (``export_code``) never reduces data for transport.

    ``figure_index`` selects ONE figure out of an ITERATE fan-out, for the
    panel's next/previous navigator — and now only that figure is **built**, not
    merely the only one serialized. Building a figure is where the cost is
    (panels, aggregation, downsampling over the whole group), so reducing all of
    them to show one meant a two-figure fan-out cost twice what the user was
    looking at, on every control change. The labels the navigator needs come
    from the group keys, which never required the figures
    (``reduce.resolve_one``). ``None`` returns them all, which is what a library
    caller or a test wants.
    """
    from scistackplot import (
        MAX_TRANSPORT_POINTS,
        RoleError,
        render_plotly,
        resolve,
        resolve_one,
    )

    _, spec, table = _load(db, spec_payload, csv_path=csv_path, label="plot_resolve")

    budget = MAX_TRANSPORT_POINTS if max_points is None else max_points
    if figure_index is not None:
        try:
            figure, labels, index = resolve_one(
                spec, table, figure_index, max_points=budget
            )
        except RoleError as exc:
            return _invalid_spec(exc)
        rendered = [
            {
                "index": index,
                "key": figure.to_dict()["figure_key"],
                "label": figure.figure_label,
                "figure": render_plotly(figure),
                "row_count": figure.row_count,
                "downsampled_from": figure.downsampled_from,
            }
        ]
        logger.info(
            "[plot] resolved %s: figure %d of %d rendered, %d row(s)",
            spec.kind,
            index,
            len(labels),
            figure.row_count,
        )
        return {
            "ok": True,
            "error": None,
            "figures": rendered,
            "figure_labels": labels,
            "figure_count": len(labels),
            "figure_index": index,
            "notes": list(figure.fanout_notes),
        }

    try:
        resolved = resolve(spec, table, max_points=budget)
    except RoleError as exc:
        return _invalid_spec(exc)

    labels = [item.figure_label for item in resolved]
    selected = list(enumerate(resolved))
    index = 0

    figures = [
        {
            "index": position,
            "key": item.to_dict()["figure_key"],
            "label": item.figure_label,
            "figure": render_plotly(item),
            "row_count": item.row_count,
            "downsampled_from": item.downsampled_from,
        }
        for position, item in selected
    ]
    logger.info(
        "[plot] resolved %s: %d of %d figure(s) rendered (index=%s), %d row(s)",
        spec.kind,
        len(figures),
        len(resolved),
        index,
        sum(f["row_count"] for f in figures),
    )
    return {
        "ok": True,
        "error": None,
        "figures": figures,
        # Every label, always: the navigator has to name the figures it is not
        # showing, and they are cheap next to the payloads.
        "figure_labels": labels,
        "figure_count": len(resolved),
        "figure_index": index,
        # Identical on every figure — it describes the fan-out, not a figure.
        "notes": list(resolved[0].fanout_notes) if resolved else [],
    }


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def export_code(
    db,
    spec_payload: dict,
    *,
    function_name: str | None = None,
    output_variable: str | None = None,
    path_template: str | None = None,
    finalized: bool = True,
    csv_path: str | None = None,
) -> dict:
    """Generate the ``plot_`` function and its ``for_each`` call, without writing."""
    from scistackplotdb import generate_endpoint

    _, spec, table = _load(db, spec_payload, csv_path=csv_path, label="plot_export")

    code = generate_endpoint(
        spec,
        table,
        input_variable=spec.measures[0],
        x_variable=spec.x_measure,
        function_name=function_name,
        output_variable=output_variable,
        path_template=path_template,
        finalized=finalized,
    )
    return {
        "ok": True,
        "function_name": code.function_name,
        "function_source": code.function_source,
        "foreach_source": code.foreach_source,
        "source": code.source,
        "iterate_keys": code.iterate_keys,
        "path_template": code.path_template,
        "output_variable": code.output_variable,
    }


def save_figure(
    db,
    spec_payload: dict,
    path: str,
    *,
    dpi: int = 200,
    figure_index: int | None = None,
    csv_path: str | None = None,
    on_progress=None,
) -> dict:
    """
    Render the current spec to an image file.

    Rendered with **matplotlib**, not by asking the browser to download the
    plotly view. Two reasons: a webview cannot save a file (its download is
    blocked, which is what made plotly's camera button fail silently), and the
    matplotlib renderer is the one the pipeline uses — so the file you save by
    hand is the same figure ``for_each`` would produce from the exported code.

    Deliberately NO downsampling (``max_points=None``): the interactive view is
    reduced for transport, a saved figure must not be.

    ``figure_index`` saves ONE figure of an ITERATE fan-out — the one on screen
    — and is what "Save current figure" sends. ``None`` saves every figure,
    which is the slow path: it was the default, and it is why saving failed.
    A save of a two-figure fan-out at full resolution is more work than the
    interactive resolve beside it, and that resolve was already taking 25-27s
    against a **30s** client timeout (``api.ts``), so the save crossed it and
    surfaced as "Request plot_save_figure timed out" with nothing in the log to
    say why — this function reported nothing at all until it succeeded.
    See ``.claude/plan-plot-studio-fixes.md`` Stage 3.

    ``on_progress(done, total, path)`` is called after each file is written, for
    a caller that wants to report progress while it happens.
    """
    import time
    from pathlib import Path

    from scidb.log import Log
    from scistackplot import RoleError, render_matplotlib, resolve, resolve_one

    with Log.timer(
        "save_figure",
        layer="scistack_gui",
        extra=f"index={'all' if figure_index is None else figure_index}",
    ) as timing:
        with timing.phase("load"):
            _, spec, table = _load(
                db, spec_payload, csv_path=csv_path, label="plot_save_figure"
            )

        # Resolving is the dominant cost and the reason a save can outlast the
        # client's patience, so it is timed as its own phase rather than folded
        # into the total.
        with timing.phase("resolve"):
            try:
                if figure_index is None:
                    resolved = list(resolve(spec, table))
                else:
                    figure, _labels, _position = resolve_one(
                        spec, table, figure_index
                    )
                    resolved = [figure]
            except RoleError as exc:
                # A role conflict is a user-correctable state, not a fault —
                # same treatment as the resolve path (`_invalid_spec`).
                logger.info("[plot] save refused an invalid spec: %s", exc)
                return {"ok": False, "error": str(exc), "files": []}

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        suffix = target.suffix or ".png"

        logger.info(
            "[plot] saving %d figure(s) of %s to %s (dpi=%d)",
            len(resolved),
            spec.kind,
            target,
            dpi,
        )

        written: list[str] = []
        with timing.phase("render_and_write"):
            for item in resolved:
                # A spec with ITERATE roles is several figures; give each its
                # own file rather than silently saving only the first. One
                # explicitly-indexed figure is still one file, named for the
                # figure it holds — `figure_1.png` for a fan-out the user is
                # saving one frame of at a time would be a lie.
                if len(resolved) > 1 or figure_index is not None:
                    slug = _slug(item.figure_label) or f"{len(written) + 1}"
                    out = target.with_name(f"{target.stem}_{slug}{suffix}")
                else:
                    out = target.with_suffix(suffix)

                # Rendering and writing are timed apart because we did not know
                # which dominates, and the answer decides where any further
                # work goes.
                started = time.perf_counter()
                figure = render_matplotlib(item)
                rendered = time.perf_counter()
                try:
                    figure.savefig(out, dpi=dpi, bbox_inches="tight")
                finally:
                    import matplotlib.pyplot as plt

                    plt.close(figure)
                finished = time.perf_counter()

                written.append(str(out))
                logger.info(
                    "[plot] saved figure %d/%d: %s — %d row(s), "
                    "render=%.3fs savefig=%.3fs",
                    len(written),
                    len(resolved),
                    out.name,
                    item.row_count,
                    rendered - started,
                    finished - rendered,
                )
                if on_progress is not None:
                    on_progress(len(written), len(resolved), str(out))

    logger.info("[plot] saved %d figure(s): %s", len(written), written)
    return {"ok": True, "error": None, "files": written}


def _slug(text: str) -> str:
    import re

    return re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")


def start_save_job(
    db,
    spec_payload: dict,
    path: str,
    *,
    dpi: int = 200,
    csv_path: str | None = None,
    job_id: str | None = None,
) -> dict:
    """Save every figure of a fan-out on a background thread.

    Returns ``{"job_id": ...}`` at once; the work reports itself through three
    messages, delivered over whichever transport is active
    (``ws.push_message`` picks):

    * ``plot_save_progress`` — ``job_id``, ``done``, ``total``, ``path``
    * ``plot_save_complete`` — ``job_id``, ``files``, ``elapsed``
    * ``plot_save_failed``   — ``job_id``, ``error``

    **Why a thread rather than a longer timeout.** Saving N figures at full
    resolution is N times the work of the interactive resolve, and that resolve
    already ran to 25-27s against a fixed 30s client timeout. No timeout value
    makes "save 30 subjects" a request/response operation; the only honest
    shape is a job that reports progress.

    **The database is not held while this runs.** ``save_figure`` takes the
    connection only to load the frames (Stage 2's ``db_connection``) and
    releases it before the first figure renders, so a long save leaves the
    ``.duckdb`` file free for MATLAB — which is the whole reason the user asked
    for a background job rather than a spinner.

    Modelled on ``server._h_start_run``: a daemon thread, an id returned
    immediately, and every outcome announced. Deliberately NOT cancellable —
    a save is bounded and the run service's cancel machinery is heavier than
    this needs. If that changes, it gets a real cancel rather than a flag.
    """
    import threading
    import time
    import uuid

    from scistack_gui.api.ws import push_message

    job = job_id or str(uuid.uuid4())[:8]

    def _worker() -> None:
        started = time.monotonic()
        try:
            result = save_figure(
                db,
                spec_payload,
                path,
                dpi=dpi,
                csv_path=csv_path,
                on_progress=lambda done, total, written: push_message(
                    {
                        "type": "plot_save_progress",
                        "job_id": job,
                        "done": done,
                        "total": total,
                        "path": written,
                    }
                ),
            )
        except Exception as exc:  # noqa: BLE001 — a thread announces its own death
            # Without this the thread dies silently and the panel waits for a
            # completion that can never arrive. Same reasoning as
            # `_handle_request`'s outer safety net.
            logger.exception("[plot] save job %s failed", job)
            push_message(
                {"type": "plot_save_failed", "job_id": job, "error": str(exc)}
            )
            return

        elapsed = time.monotonic() - started
        if not result.get("ok"):
            # An invalid spec is a refusal, not a crash, but it still has to
            # end the job — the panel cannot tell the two apart from silence.
            push_message(
                {
                    "type": "plot_save_failed",
                    "job_id": job,
                    "error": result.get("error") or "Could not save.",
                }
            )
            return

        logger.info(
            "[plot] save job %s complete: %d file(s) in %.1fs",
            job,
            len(result["files"]),
            elapsed,
        )
        push_message(
            {
                "type": "plot_save_complete",
                "job_id": job,
                "files": result["files"],
                "elapsed": elapsed,
            }
        )

    logger.info("[plot] starting save job %s -> %s", job, path)
    threading.Thread(target=_worker, daemon=True, name=f"plot-save-{job}").start()
    return {"job_id": job}


def add_to_pipeline(
    db,
    spec_payload: dict,
    *,
    function_name: str | None = None,
    output_variable: str | None = None,
    path_template: str | None = None,
    finalized: bool = True,
) -> dict:
    """
    Write the generated endpoint into the project and refresh the registry.

    The function is appended to ``scistack_plots.py`` beside the entities file
    — the project root, which is inside the discovery scope — and the output
    variable type is declared through the normal entity-creation path, so a
    generated endpoint never references an undeclared type.

    The ``for_each`` call itself is returned rather than written: the GUI
    builds pipelines from the DAG, so the user wires the newly discovered
    function up on the canvas (or pastes the snippet into a script).
    """
    from pathlib import Path

    from scistack_gui.services.target_file_service import (
        _reload_after_write,
        get_or_create_target_file,
        validate_entity_name,
    )
    from scistack_gui.services.variable_service import create_variable

    generated = export_code(
        db,
        spec_payload,
        function_name=function_name,
        output_variable=output_variable,
        path_template=path_template,
        finalized=finalized,
    )

    name_error = validate_entity_name(generated["output_variable"])
    if name_error:
        return {"ok": False, "error": name_error}

    declared = create_variable(generated["output_variable"])
    if not declared.get("ok") and "already exists" not in str(declared.get("error", "")):
        return {"ok": False, "error": declared.get("error")}

    target_file, target_err = get_or_create_target_file()
    if target_file is None:
        return {"ok": False, "error": target_err}

    plots_file = Path(target_file).parent / "scistack_plots.py"
    existing = plots_file.read_text(encoding="utf-8") if plots_file.exists() else ""
    if f"def {generated['function_name']}(" in existing:
        return {
            "ok": False,
            "error": (
                f"{plots_file.name} already defines {generated['function_name']}. "
                f"Choose a different function name, or edit the existing one."
            ),
        }

    header = '"""Plot endpoints generated by scistackplot."""\n' if not existing else ""
    with open(plots_file, "a", encoding="utf-8") as handle:
        handle.write(header)
        handle.write("\n\n" if existing else "\n")
        handle.write(generated["function_source"])

    logger.info(
        "[plot] wrote %s to %s (output=%s)",
        generated["function_name"],
        plots_file,
        generated["output_variable"],
    )
    reload_error = _reload_after_write(plots_file)
    if reload_error:
        return {**generated, "ok": False, "error": reload_error, "file": str(plots_file)}

    return {**generated, "file": str(plots_file)}


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------


def _table_for(source, spec):
    """Load every variable this spec plots, in one table.

    ``spec.variant_variables()`` is the union — the primary measure plus any
    variable a variant row names — so adding a row for another variable is all
    it takes to get that variable loaded and stacked. ``x_measure`` is passed
    separately because it joins rather than stacks.
    """
    return source.get_table(
        spec.variant_variables(),
        x_measure=spec.x_measure,
        factor_variables=list(spec.factor_variables),
    )


def _load(db, spec_payload: dict, *, csv_path: str | None, label: str):
    """``(source, spec, table)``, holding the database only while it loads.

    **This is the whole DuckDB-touching phase of a plot request.** Everything
    after it — reducing, faceting, downsampling, rendering — runs on the
    in-memory frame the table already holds and never reaches back to the
    source, which is what makes the narrow hold correct rather than merely
    shorter. Keep it that way: a lazy field added to ``LongTable`` that queried
    on access would fail here with a closed connection, and it would fail in the
    render path where it is hardest to read.

    The connection is only taken under the ``per_request`` policy, so the
    standalone process is unaffected (:func:`scistack_gui.db.db_connection`).
    A CSV spec takes no connection at all — there is no database in that path.
    """
    from scistack_gui.db import db_connection

    with db_connection(label, needed=not csv_path):
        source = get_source(db, csv_path=csv_path)
        spec = _spec_from_payload(spec_payload)
        table = _table_for(source, spec)
    return source, spec, table


def _spec_from_payload(payload: dict):
    from scistackplot import PlotSpec

    _require_scistackplot()
    if not payload or not payload.get("measures"):
        raise ValueError("A plot spec needs at least one measure (variable name).")
    return PlotSpec.from_dict(payload)

