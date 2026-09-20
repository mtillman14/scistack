"""
Plot Studio backend.

One service, two transports, ONE declaration: ``api/plot.py``'s handler
table (``api/handlers.py``) builds both the FastAPI routes and the JSON-RPC
methods in ``server.py`` from the same rows, so the web GUI and the VS Code
extension cannot drift apart — ``tests/test_api_handlers.py`` checks the
table against both transports and the frontend's route map.

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
from dataclasses import replace
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
    # Deliberately NOT clearing scidb's discovery cache here. It is TTL-based
    # (a few seconds) by a documented decision — the filesystem changes behind
    # our back, so an event hook would only guess when — and that TTL already
    # expires well inside any run. See ``scidb.state.DISCOVERY_CACHE_SECONDS``.
    return {"ok": True}


# ---------------------------------------------------------------------------
# Describe
# ---------------------------------------------------------------------------


def _with_stored_variant_sets(db, variable: str, spec):
    """*spec* with the pins stored for *variable* in place of its default
    ``variant_sets`` — or unchanged when none are stored, or when the store
    is unavailable (a CSV source has no database)."""
    if db is None or not variable:
        return spec
    try:
        from scistackplot.spec import VariantSet

        from scistack_gui import intent_store

        stored = intent_store.variant_selections(db, variable)
    except Exception:
        logger.debug("[plot] stored variant pins unavailable for %s", variable, exc_info=True)
        return spec
    if not stored:
        return spec
    try:
        sets = [VariantSet.from_dict(s) for s in stored]
    except Exception:
        logger.warning(
            "[plot] %s: %d stored variant pin(s) could not be parsed — using the "
            "default pin instead",
            variable,
            len(stored),
            exc_info=True,
        )
        return spec
    logger.info(
        "[plot] %s: %d stored variant pin(s) replace the default: %s",
        variable,
        len(sets),
        [s.name for s in sets],
    )
    return replace(spec, variant_sets=sets)


def save_variant_sets(db, variable: str, variant_sets: list) -> dict:
    """Persist a plot's pins as `variant_selection` statements about
    *variable* (the RPC behind every edit of the Variants section)."""
    from scistack_gui import intent_store

    n = intent_store.set_variant_selections(db, variable, variant_sets)
    return {"ok": True, "variable": variable, "stored": n}


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
    from scistackplot import capabilities, default_spec, presets_payload

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
    # A plot's named pins over variant space are statements about the plotted
    # variable (`variant_selection` aspect of the intent store), so they
    # survive the panel closing. Stored pins replace the default's — the
    # user's most recent statement wins over a computed default.
    spec = _with_stored_variant_sets(db, variable, spec)

    # Offers AND refusals: the variant picker draws every variable node on the
    # canvas, so one it cannot offer has to say why in place.
    stacking = (
        source.stackable_report(variable)
        if hasattr(source, "stackable_report")
        else {"offered": source.stackable_with(variable), "rejected": {}}
    )
    stackable = stacking["offered"]
    # Which VARIABLES may group this figure — never their columns.
    #
    # `groupable_report` used to answer both here, which meant every wide
    # variable's per-column work ran on every panel open whether or not anyone
    # looked: 4.2 s for FilteredDelsys and 5.4 s for RawEMG on the user's
    # project (scidb.log 2026-09-15), both offering nothing. The columns are now
    # `plot_grouping_columns`, asked once per variable the user opens.
    grouping = source.groupable_variables(variable)
    groupable = grouping["offered"]
    # Which optional sections the panel can show, and why. Each of these gates a
    # control that is simply absent when the list is empty, so an empty list has
    # to be visible somewhere — otherwise "no other variable qualifies" and "the
    # feature is missing" look identical from the outside.
    logger.info(
        "[plot] describe(%s): %d stackable %s, %d groupable variable(s) %s "
        "(no column query), %d joinable",
        variable,
        len(stackable),
        stackable,
        len(groupable),
        [(offer["label"], offer["kind"]) for offer in groupable],
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
        # VARIABLES usable as a grouping — one recorded at or above this
        # variable's schema level, so each row gets exactly one of its values.
        # One entry per variable, never per column: a wide sheet arrives with
        # `kind: "columns"` and a count, and WHICH of its columns qualify is
        # `plot_grouping_columns`, asked when the user opens that variable.
        "groupable_variables": groupable,
        "groupable_refused": grouping["rejected"],
        # What this matplotlib can write, so the format dropdown offers exactly
        # what the save will accept rather than a second list that can drift.
        "image_formats": supported_formats(),
        # The aspect-ratio dropdown, in order. Owned by scistackplot (one
        # vocabulary with `aspect_name`, which is what the renderer logs), so
        # the GUI cannot carry a second list that drifts from it.
        "figure_presets": presets_payload(),
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
    #
    # Over `variant_table`, NOT `get_table`. `default_selection` reads
    # `default_pin`, `latest_column` and the variant factors' levels and never
    # touches a measure column, and `variant_table` answers exactly that from a
    # query selecting no data columns — which is the whole reason it exists
    # (`.claude/plot-at-scale-plan.md` §7: reading a handful of variant levels
    # through `get_table` meant loading 174 M samples, and the schema-location
    # picker timed out on a 419-location variable).
    #
    # It was survivable while the popup only ever opened over the variable the
    # panel had already loaded and cached. It stops being survivable now that
    # the grouping picker asks this for a variable nobody has plotted.
    from scistackplot import default_selection

    graph["default_selection"] = default_selection(
        source.variant_table(variable)
        if hasattr(source, "variant_table")
        else source.get_table([variable])
    )

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


def grouping_graph(db, variable: str, *, csv_path: str | None = None) -> dict:
    """Which variable nodes may group ``variable``'s figure, and why not.

    The canvas half of the Grouping picker, and the reason it is its own call:
    drawing it must cost no value read and no ``DISTINCT`` on a wide table
    (``ScidbSource.groupable_variables``). Its companion
    :func:`grouping_columns` is the expensive half, asked once per variable the
    user opens rather than once per panel.

    Read-only, like every other picker call. Nothing here touches execution
    state, which is the property ``test_plot_service`` asserts for the variant
    popup and which this one has to keep: a dialog about what to LOOK at must
    never change what a run does.
    """
    from scistack_gui.db import db_connection

    with db_connection("plot_grouping_graph", needed=not csv_path):
        source = get_source(db, csv_path=csv_path)
        report = source.groupable_variables(variable)
        logger.info(
            "[plot] grouping_graph(%s): %d offered %s, %d refused",
            variable,
            len(report["offered"]),
            [(o["label"], o["kind"]) for o in report["offered"]],
            len(report["rejected"]),
        )
        return report


def grouping_columns(
    db, variable: str, group_variable: str, *, csv_path: str | None = None
) -> dict:
    """Which columns of ``group_variable`` may group ``variable``'s figure.

    The click half (see :func:`grouping_graph`). One ``SELECT DISTINCT … LIMIT``
    per candidate column, so it is paid for the variable the user actually
    opened and for no other.
    """
    from scistack_gui.db import db_connection

    with db_connection("plot_grouping_columns", needed=not csv_path):
        source = get_source(db, csv_path=csv_path)
        report = source.groupable_columns(variable, group_variable)
        logger.info(
            "[plot] grouping_columns(%s of %s): %d offered %s, %d refused %s",
            group_variable,
            variable,
            len(report["offered"]),
            [o["label"] for o in report["offered"]],
            len(report["rejected"]),
            report["rejected"] or "",
        )
        return report


def grouping_default_variant(
    db, group_variable: str, *, csv_path: str | None = None
) -> dict:
    """The variant a grouping by ``group_variable`` should open on: latest.

    Separate from the graph so it is asked once per variable the user picks,
    not once per variable drawn — and answered by
    ``ScidbSource.default_variant_for``, which is
    ``scistackplot.default_selection`` over that variable's cheap variant table.
    One rule for "latest", shared with the Variants section (CLAUDE.md NOTE 3).
    """
    from scistack_gui.db import db_connection

    with db_connection("plot_grouping_default_variant", needed=not csv_path):
        source = get_source(db, csv_path=csv_path)
        if not hasattr(source, "default_variant_for"):
            # A CSV has no provenance and so no variants. An empty selection is
            # the honest answer and is inert everywhere downstream.
            return {"variable": group_variable, "selection": {}}
        selection = source.default_variant_for(group_variable)
        logger.info(
            "[plot] grouping_default_variant(%s): %s",
            group_variable,
            selection or "none",
        )
        return {"variable": group_variable, "selection": selection}


def location_tree(
    db,
    variable: str,
    *,
    selection: dict | None = None,
    problems_only: bool = False,
    csv_path: str | None = None,
) -> dict:
    """Per-location status for one variable under one variant — the picker's data.

    ``selection`` is the plotting layer's **column-keyed** variant selection
    (``{"Code:bandpass": "v1"}``), exactly as a ``VariantSet`` holds it, because
    that is what both callers already have: Plot Studio has the open row, and
    the canvas popup has ``default_selection``. It is translated to scidb's
    ``branch_params_filter`` by ``scistackplotdb.branch_params_for`` — the layer
    that knows both vocabularies — so the dots and a ``Variant(...).load()``
    cannot disagree about which records a selection names.

    ``selection=None`` on the **canvas** path means "no spec is open": the
    default is the same ``default_selection`` a panel opens on, so the two entry
    points cannot show different variants of the same variable.

    A CSV has no provenance, so it has no locations to verify: the empty tree is
    returned with a note, rather than an error, so the popup can open and say so.
    """
    from scistack_gui.db import db_connection

    with db_connection("plot_location_tree", needed=not csv_path):
        return _location_tree(
            db,
            variable,
            selection=selection,
            problems_only=problems_only,
            csv_path=csv_path,
        )


def _location_tree(db, variable, *, selection, problems_only, csv_path) -> dict:
    """:func:`location_tree`'s body, run with the database connection held."""
    _require_scistackplot()
    if csv_path:
        return {
            "variable": variable,
            "schema_keys": [],
            "variant": {},
            "counts": {"green": 0, "amber": 0, "red": 0, "grey": 0},
            "total": 0,
            "green": 0,
            "verdict": "grey",
            "basis": "present_only",
            "notes": [
                "A CSV carries no provenance, so there is nothing to verify "
                "here — schema locations and their status come from the "
                "project database."
            ],
            "roots": [],
            "selection": {},
        }

    from scidb.locations import location_states, prune_to_problems
    from scistackplotdb import branch_params_for

    source = get_source(db)
    if selection is None:
        from scistackplot import default_selection

        # The default selection is a question about VARIANTS — `default_selection`
        # reads `default_pin`, `latest_column` and the variant factors' levels and
        # never touches a measure column. Asking `get_table` for it loaded the
        # whole variable: 174 million samples / ~5.2 GB on 2026-09-13, which is
        # why this view timed out while `location_states` itself took 9.5s
        # (.claude/plot-at-scale-plan.md §7). `variant_table` answers it from a
        # query that selects no data columns.
        #
        # Falls back for a source that has no such method — the DataFrame and CSV
        # sources are in-memory anyway, so there is nothing to save there.
        if hasattr(source, "variant_table"):
            selection = default_selection(source.variant_table(variable))
        else:
            selection = default_selection(source.get_table([variable]))

    tree = location_states(
        variable, variant=branch_params_for(selection) or None, db=db
    )
    if problems_only:
        tree = prune_to_problems(tree)

    payload = tree.to_dict()
    # Echoed back so the picker can label itself with the variant it is
    # describing without having to re-derive the default it did not send.
    payload["selection"] = dict(selection)
    logger.info(
        "[plot] location_tree(%s): %d/%d green (amber=%d, red=%d, excluded=%d) "
        "basis=%s selection=%s%s",
        variable,
        payload["green"],
        payload["total"],
        payload["counts"].get("amber", 0),
        payload["counts"].get("red", 0),
        payload["counts"].get("grey", 0),
        payload["basis"],
        selection or "none",
        " [problems only]" if problems_only else "",
    )
    for note in payload["notes"]:
        logger.warning("[plot] location_tree(%s): %s", variable, note)
    return payload


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

    budget = MAX_TRANSPORT_POINTS if max_points is None else max_points

    # The hold ends with the load (see `_load`); reducing and rendering are
    # pure memory.
    _, spec, table = _load(db, spec_payload, csv_path=csv_path, label="plot_resolve")
    try:
        if figure_index is not None:
            figure, labels, index = resolve_one(
                spec, table, figure_index, max_points=budget
            )
        else:
            resolved = resolve(spec, table, max_points=budget)
    except RoleError as exc:
        return _invalid_spec(exc)

    if figure_index is not None:
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
    image_format: str | None = None,
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
    — and is what "Save current figure" sends. ``None`` saves every figure.

    **``path`` may be a file or a folder**, decided by :func:`_destination`:
    an existing directory, or a path with no suffix, is a folder; anything else
    is a file. Saving a fan-out asks for a folder, because its N filenames are
    the figure labels rather than anything the user chose — offering a name
    there only raised "which of the thirty is that?". The files are then
    ``<y measure>_<figure label>.<format>``, which is what the filename-plus-
    suffix scheme produced anyway, so a caller passing a filename keeps its
    exact old behaviour.

    ``image_format`` is the extension to write (``png``, ``svg``, ``pdf``,
    ``eps``, …), validated against what this matplotlib build actually supports
    rather than a list hard-coded here. It wins over any suffix on ``path``;
    with neither, a file keeps its own suffix and a folder gets PNG.

    **Both are slow, and how slow was the thing nobody could see.** A
    full-resolution resolve of a two-figure fan-out took **1543s** (scidb.log
    2026-09-11 12:54) — ~12 minutes per figure, against a 30s client timeout in
    ``api.ts``. Everything here therefore reports itself *while it runs*:
    ``resolve`` narrates its phases (``narrate=True``), and every figure
    resolved and every file written is announced to the log and to
    ``on_progress``. The previous shape — one ``Log.timer`` summary on exit —
    produced nothing at all for a save that timed out or was still running,
    which is exactly the case worth diagnosing.

    ``on_progress(stage, done, total, detail)`` is called as the work proceeds:
    ``stage`` is ``"resolving"`` or ``"writing"``, ``detail`` is the figure
    label or the file just written. Resolving is the dominant cost, so a caller
    that only reported written files said nothing for the first twelve minutes.
    """
    import time

    from scidb.log import Log
    from scistackplot import RoleError, render_matplotlib, resolve, resolve_one

    def report(stage: str, done: int, total: int, detail: str | None = None) -> None:
        if on_progress is not None:
            on_progress(stage, done, total, detail)

    # FIRST, before the database is touched and long before anything is drawn.
    # Neither of these needs the spec, and a destination this process cannot
    # write is the one failure worth finding instantly: discovered at `savefig`
    # it costs the twelve minutes of rendering first, and inside a save job it
    # arrives as a crashed thread rather than a message.
    directory, named = _destination(path)
    try:
        suffix = _image_suffix(image_format, path, is_folder=named is None)
    except ValueError as exc:
        logger.info("[plot] save refused a format: %s", exc)
        return {"ok": False, "error": str(exc), "files": []}

    # live=True: every phase says when it starts, not only what it cost once it
    # is over. See _Timer in scistacklog.
    with Log.timer(
        "save_figure",
        layer="scistack_gui",
        extra=f"index={'all' if figure_index is None else figure_index}",
        live=True,
    ) as timing:
        # The database is held for the load only (see `_load`); resolving and
        # rendering — the minutes of a save — leave the file free for MATLAB.
        # Resolving is timed as its own phase because it is what can outlast a
        # client, and narrated figure by figure inside it.
        with timing.phase("load"):
            _, spec, table = _load(
                db, spec_payload, csv_path=csv_path, label="plot_save_start"
            )
        with timing.phase("resolve"):
            try:
                if figure_index is None:
                    resolved = list(
                        resolve(
                            spec,
                            table,
                            narrate=True,
                            on_figure=lambda position, total, label: report(
                                "resolving", position, total, label
                            ),
                        )
                    )
                else:
                    report("resolving", 1, 1, None)
                    figure, _labels, _position = resolve_one(
                        spec, table, figure_index, narrate=True
                    )
                    resolved = [figure]
            except RoleError as exc:
                # A role conflict is a user-correctable state, not a fault —
                # same treatment as the resolve path (`_invalid_spec`).
                logger.info("[plot] save refused an invalid spec: %s", exc)
                return {"ok": False, "error": str(exc), "files": []}

        # The stem is the only part that needed the spec, which is why it is
        # here and the format check is at the top.
        stem = named or _slug(spec.y_measure) or "figure"
        directory.mkdir(parents=True, exist_ok=True)

        logger.info(
            "[plot] saving %d figure(s) of %s into %s as %s (dpi=%d)",
            len(resolved),
            spec.kind,
            directory,
            suffix.lstrip("."),
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
                    out = directory / f"{stem}_{slug}{suffix}"
                else:
                    out = directory / f"{stem}{suffix}"

                # Announced BEFORE the work, not only after: rendering a
                # full-resolution panel grid is seconds to minutes, and a log
                # that only speaks on success cannot say which figure a save
                # died in.
                logger.info(
                    "[plot] rendering figure %d/%d (%s) to %s",
                    len(written) + 1,
                    len(resolved),
                    item.figure_label or "single figure",
                    out.name,
                )

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
                report("writing", len(written), len(resolved), str(out))

    logger.info("[plot] saved %d figure(s): %s", len(written), written)
    # `directory` alongside `files`: a thirty-figure save has thirty paths that
    # all share one folder, and the folder is what the user wants told back.
    return {
        "ok": True,
        "error": None,
        "files": written,
        "directory": str(directory),
    }


def save_plot_data(
    db,
    spec_payload: dict,
    path: str,
    *,
    depth: str | None = None,
    fields_as_columns: bool = True,
    csv_path: str | None = None,
    on_progress=None,
) -> dict:
    """Write the long table the plot is drawn from to a CSV file.

    ``fields_as_columns`` (default on, as the panel's checkbox) writes a
    struct variable's fields one column each instead of a ``ColName``
    column; inert for a variable with no fields.

    The rows are ``scistackplot.plot_data`` — the SAME plan and the same
    sample frame the figure is built from, every figure of an ITERATE fan-out
    in one file with the figure keys as columns — so the statistics run on
    exactly what the plot shows (docs/claude/plot-data-export.md). ``depth``
    is a key from the capability report's ``data_export.depths``; None is
    the plotted sample.

    ``path`` is the file to write. An existing folder gets
    ``<measure>_data.csv`` inside it, and a path with no suffix gets
    ``.csv`` — the same file-or-folder reading as :func:`save_figure`.

    Returns ``{"ok", "error", "files", "directory", "rows", "columns"}``;
    a spec that cannot be exported (a raw 1-D measure, an invalid role
    assignment, an unknown depth) is ``ok: False`` with the reason, never
    a raised exception — it is a user-correctable state.
    """
    import time
    from pathlib import Path

    from scidb.log import Log
    from scistackplot import plot_data

    def report(stage: str, done: int, total: int, detail: str | None = None) -> None:
        if on_progress is not None:
            on_progress(stage, done, total, detail)

    # The destination FIRST, before the database is touched — the same order
    # as save_figure, for the same reason: an unwritable folder found after
    # the load is a wasted load.
    target = Path(path)
    if target.is_dir():
        out = None  # named after the measure once the spec is parsed
        directory = target
    else:
        out = target if target.suffix else target.with_suffix(".csv")
        directory = out.parent

    started = time.perf_counter()
    with Log.timer("save_plot_data", layer="scistack_gui", extra=f"depth={depth or 'sample'}") as timing:
        report("resolving", 0, 1, None)
        with timing.phase("load"):
            _, spec, table = _load(
                db, spec_payload, csv_path=csv_path, label="plot_save_start"
            )
        with timing.phase("plot_data"):
            try:
                frame = plot_data(
                    spec, table, depth=depth, fields_as_columns=fields_as_columns
                )
            except ValueError as exc:
                # RoleError is a ValueError too: an invalid spec, a non-scalar
                # measure, or a depth the spec does not offer.
                logger.info("[plot] data save refused: %s", exc)
                return {"ok": False, "error": str(exc), "files": []}
        if out is None:
            out = directory / f"{_slug(spec.y_measure) or 'plot'}_data.csv"
        with timing.phase("write"):
            directory.mkdir(parents=True, exist_ok=True)
            frame.to_csv(out, index=False)

    logger.info(
        "[plot] saved data of %s to %s: %d row(s) x %d column(s) %s, depth=%s, "
        "fields_as_columns=%s, %.3fs",
        spec.y_measure,
        out,
        len(frame),
        len(frame.columns),
        list(frame.columns),
        depth or "sample",
        fields_as_columns,
        time.perf_counter() - started,
    )
    report("writing", 1, 1, str(out))
    return {
        "ok": True,
        "error": None,
        "files": [str(out)],
        "directory": str(directory),
        "rows": len(frame),
        "columns": [str(c) for c in frame.columns],
    }


def _slug(text: str) -> str:
    import re

    return re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")


def supported_formats() -> list[str]:
    """The file types this matplotlib build can actually write, sorted.

    Asked of matplotlib rather than hard-coded, so the GUI's dropdown and the
    backend's validation cannot disagree, and neither can go stale when a
    backend gains or loses a writer. (``.fig`` will never appear here — it is
    MATLAB's own format and matplotlib has no writer for it.)
    """
    import matplotlib

    matplotlib.use("Agg", force=False)
    from matplotlib.backend_bases import FigureCanvasBase

    return sorted(FigureCanvasBase.get_supported_filetypes())


def _destination(path: str) -> tuple:
    """``(directory, stem_or_None)`` — whether ``path`` names a file or a folder.

    An existing directory is a folder, and so is a path with no suffix;
    anything else is a file whose stem the caller chose.

    The existence check comes FIRST and is not redundant: a real folder can
    carry a dot (``~/analysis.v2``), and reading ``.v2`` as an image format
    would turn a chosen destination into a guess. A folder that does not exist
    yet cannot be told apart that way, which is why the suffix rule backs it up
    — and why the GUI's folder picker only ever returns existing directories.
    """
    from pathlib import Path

    target = Path(path)
    if target.is_dir() or not target.suffix:
        return target, None
    return target.parent, target.stem


def _image_suffix(image_format: str | None, path: str, *, is_folder: bool) -> str:
    """``.png`` and friends — what to write, or ValueError naming the choices.

    Order of authority: an explicit ``image_format``, then the suffix the caller
    put on ``path``, then PNG. A folder skips the middle one on purpose — there
    is no filename to read a format off, and ``~/analysis.v2`` must not be
    mistaken for one.
    """
    from pathlib import Path

    allowed = supported_formats()
    chosen = (image_format or "").strip().lstrip(".").lower()
    if not chosen and not is_folder:
        chosen = Path(path).suffix.lstrip(".").lower()
    if not chosen:
        chosen = "png"

    if chosen not in allowed:
        raise ValueError(
            f"Cannot save as {chosen!r} — this matplotlib writes: "
            f"{', '.join(allowed)}."
        )
    return f".{chosen}"


def start_save_job(
    db,
    spec_payload: dict,
    path: str,
    *,
    dpi: int = 200,
    figure_index: int | None = None,
    image_format: str | None = None,
    csv_path: str | None = None,
    job_id: str | None = None,
    what: str = "image",
    depth: str | None = None,
    fields_as_columns: bool = True,
) -> dict:
    """Save on a background thread — one figure of a fan-out, or all of them;
    or, with ``what="data"``, the plot's long table as CSV
    (:func:`save_plot_data`, ``depth`` passed through).

    The data save rides the same job and the same three messages: its load can
    outlast the RPC clock just as a figure's resolve can, and one progress
    readout in the panel is simpler than two.

    ``figure_index`` is passed straight through to :func:`save_figure`: an
    index saves that one figure, ``None`` saves every figure.

    **Both go through a job. Neither is answerable in one round trip.** The
    single-figure save used to be a request/response RPC on the theory that one
    figure is cheap next to a fan-out. It is not: a full-resolution figure of a
    1-D measure took ~12 minutes (scidb.log 2026-09-11, 1543s for two of them)
    against a 30s client timeout in ``api.ts``, so "Save current figure" failed
    exactly as reliably as "Save all" ever did — with the backend still working
    on a file nobody was waiting for any more. There is no timeout value that
    fixes that; the shape has to change.

    Returns ``{"job_id": ...}`` at once; the work reports itself through three
    messages, delivered over whichever transport is active
    (``ws.push_message`` picks):

    * ``plot_save_progress`` — ``job_id``, ``stage``, ``done``, ``total``,
      ``path`` (the file just written, or the figure label while resolving)
    * ``plot_save_complete`` — ``job_id``, ``files``, ``directory``, ``elapsed``
    * ``plot_save_failed``   — ``job_id``, ``error``

    Exactly one of ``plot_save_complete`` / ``plot_save_failed`` is always sent,
    on every path including an unhandled exception — the panel disables its save
    buttons for the duration and has no other way to learn it is over.

    ``stage`` distinguishes ``"resolving"`` from ``"writing"`` because the two
    are not comparable: resolving is minutes and writing is seconds, so a panel
    that showed only "0 of 2 saved" for twelve minutes was reporting the cheap
    half of the job.

    **Why a thread rather than a longer timeout.** Saving N figures at full
    resolution is N times the work of the interactive resolve, and one figure of
    it is already minutes. No timeout value makes "save 30 subjects" — or, as it
    turns out, "save this one" — a request/response operation; the only honest
    shape is a job that reports progress.

    **The database is not held while this runs.** ``save_figure`` takes the
    connection only to load the frames (``_load``) and releases it before
    anything is reduced or rendered, so a long save leaves the ``.duckdb``
    file free for MATLAB — which is the whole reason the user asked for a
    background job rather than a spinner.

    Modelled on ``server._h_start_run``: a daemon thread, an id returned
    immediately, and every outcome announced. Deliberately NOT cancellable —
    a save is bounded and the run service's cancel machinery is heavier than
    this needs. If that changes, it gets a real cancel rather than a flag.
    """
    import threading
    import time
    import uuid

    from scistack_gui.api.ws import push_message

    if what not in ("image", "data"):
        raise ValueError(f"Unknown save kind {what!r}; expected 'image' or 'data'.")
    job = job_id or str(uuid.uuid4())[:8]

    def _worker() -> None:
        started = time.monotonic()

        def progress(stage, done, total, detail) -> None:
            push_message(
                {
                    "type": "plot_save_progress",
                    "job_id": job,
                    "stage": stage,
                    "done": done,
                    "total": total,
                    "path": detail,
                }
            )

        try:
            if what == "data":
                result = save_plot_data(
                    db,
                    spec_payload,
                    path,
                    depth=depth,
                    fields_as_columns=fields_as_columns,
                    csv_path=csv_path,
                    on_progress=progress,
                )
            else:
                result = save_figure(
                    db,
                    spec_payload,
                    path,
                    dpi=dpi,
                    figure_index=figure_index,
                    image_format=image_format,
                    csv_path=csv_path,
                    on_progress=progress,
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
                # The one folder they all share — thirty full paths is not a
                # message anyone reads.
                "directory": result.get("directory"),
                "elapsed": elapsed,
                # A data save's row count, for "Saved … — 48 row(s)"; absent
                # for an image save.
                **({"rows": result["rows"]} if "rows" in result else {}),
            }
        )

    logger.info(
        "[plot] starting save job %s (%s) -> %s",
        job,
        f"data, depth={depth or 'sample'}"
        if what == "data"
        else ("every figure" if figure_index is None else f"figure {figure_index}"),
        path,
    )
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
    in-memory frame the table holds (its cells are ndarrays; the reducer is
    ``scistackplot.NumpyReducer``) and never reaches back to the source. That
    is what makes the narrow hold correct rather than merely shorter, and it is
    the window MATLAB gets back under the per-request policy.

    History, because this went back and forth in one day (2026-09-13): a
    DuckDB-SQL reducer briefly ran queries from inside ``resolve``; released
    first, it found the connection closed and silently fell back to pandas for
    375 s, so the hold was widened to cover the reduction. The SQL reducer then
    lost every measurement to numpy over the loaded cells and was removed
    (.claude/plan-plot-minimal-load-examples.md §8), which returns the hold to
    the load only. Keep it that way: a lazy field added to ``LongTable`` that
    queried on access would fail here with a closed connection, and it would
    fail in the render path where it is hardest to read.
    ``test_plot_service.py::test_resolve_drops_the_connection_before_reducing``
    pins it.

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

