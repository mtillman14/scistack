"""
``resolve(spec, table)`` — turn a spec plus data into renderer-ready panels.

Everything semantic happens here: filtering, variant handling, exploding 1-D
measures, the collapse chain, fanning out ITERATE factors into separate
figures, faceting, and summarizing the sample into a statistic with an error
band. Renderers below this line only translate.

Two reductions are easy to confuse, so they are named apart deliberately
(docs/claude/grouping-and-collapse.md):

* **The collapse chain (roles)** averages COLLAPSE factors away, deepest
  first — "trial within subject, then subject" — and removes them from the
  data. The LAST collapsed key is the **sample**: its levels are what is left
  at each mark. ``roles.collapse_steps`` owns the order; ``_collapse_levels``
  runs it.
* **Summarizing (a plot kind)** turns the sample rows at each mark into a
  centre and an error band. This is what BAR and BAND do. Box and violin draw
  the sample rows as a distribution; scatter and strip draw one point per
  sample row; line and spaghetti one polyline per sample level. No kind
  averages the sample further (schema-level parity, 2026-09-19).

You can have either, both, or neither: a bar with nothing collapsed is a bar
of single values with no error bar.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scistacklog import Log

from .framesize import format_extent, frame_extent
from .numeric import coerce_numeric
from .resolved import (
    COLOR,
    DASH,
    DASH_CYCLE,
    SAMPLE_COLOR,
    SAMPLE_LINE,
    RUN,
    SERIES,
    UNLABELLED_X,
    X,
    Y,
    Y_HIGH,
    Y_LOW,
    Z,
)
from .aliases import DisplayText, check_distinct, display_text, labelable, log_summary
from .resolved import Encoding, Labels, Panel, ResolvedPlot
from .roles import (
    CollapseSteps,
    OverlaySteps,
    collapse_steps,
    complete_roles,
    fanout_keys,
    grouping_layers,
    overlay_color,
    overlay_join,
    overlay_steps,
    overlay_unavailable,
    spaghetti_sample_repeats,
    validate,
)
from .shape import Shape
from .spec import (
    Matcher,
    PlotKind,
    PlotSpec,
    Role,
    Statistic,
    grid_shape_for,
    value_spellings,
)
from .table import LongTable, natural_sort_key
from .xaxis import LEAF_SEPARATOR, XPlan, plan_x_axis
from .spaghetti import overlay_offsets, series_offsets
from .reducer import reducer_for
from .ylimits import ExtentMode, eligible_scope, limits_for, panel_factors, spread_bounds
from .groups import apply_level_groups
from .cell import apply_cell_collapse, cell_collapse_note, cell_collapses
from .variants import apply_variant_sets, strip_answered_roles

LAYER = "scistackplot"

#: Default index column name created when a 1-D measure is exploded.
DEFAULT_INDEX_COLUMN = "index"

#: Joins the factor values identifying one polyline (``__series``). Only ever
#: built and compared, never parsed. Two levels whose text contains it can
#: compose to the same key and be drawn as one line — inherited from the join
#: this replaced, and not something a separator choice can rule out.
SERIES_SEPARATOR = " | "

#: How a missing factor value appears in a composed key. It needs SOME text:
#: a row whose subject is null is still a row, and the alternative — pandas 3's
#: `astype(str)` preserving NA — is a TypeError in the middle of a figure.
MISSING_LEVEL_TEXT = "nan"

#: Rows per figure above which the GUI path downsamples before serializing.
#: 1-D data over hundreds of trials is megabytes, and it crosses the webview
#: boundary on every interaction. Export never downsamples (max_points=None).
MAX_TRANSPORT_POINTS = 20_000

#: Spec fields that change how a figure LOOKS but not what data goes into it.
#: Excluded from the plan cache key, which is the whole reason the cache pays
#: off: these are the controls a user drags, and none of them should re-filter,
#: re-fold variants or re-group anything.
#:
#: Everything else is included by omission, on purpose. A field added to
#: ``PlotSpec`` later lands in the key automatically, so the worst a forgotten
#: update can do is miss the cache — never serve a plan built for a different
#: question.
#:
#: ``y_axis`` is deliberately NOT here: the plan carries the fan-out's computed
#: y limits (``_Plan.y_limits``), so a changed scope has to build a new one. It
#: is a checkbox and two boxes rather than a dragged slider, so the re-plan is
#: per click, not per frame.
_PLAN_IRRELEVANT_FIELDS = ("kind", "facet", "style", "aliases")

#: How many plans are kept. Two: the pattern being served is narrow — the panel
#: re-resolves the SAME data repeatedly while the user changes how it is drawn.
#:
#: Cheap to hold, because the frames in a plan are the NESTED ones. Exploding
#: moved into the figures (``_Plan.explode``), so a plan for a 1-D measure keeps
#: the 24-row frame rather than the 17-million-sample one. Had these two changes
#: landed the other way round, this cache would have been the memory problem.
_PLAN_CACHE_ENTRIES = 2

#: ``(id(table), spec key) -> (table, plan)``.
#:
#: The table is kept in the VALUE, not just the key, and that is load-bearing:
#: a strong reference pins the object so CPython cannot recycle its ``id`` onto
#: a different table and serve this plan for it. (The same id-reuse trap the
#: GUI's source cache documents.) Lookups still verify identity before
#: returning, so the pinning is belt-and-braces rather than the only guard.
_plan_cache: dict[tuple, tuple[LongTable, "_Plan"]] = {}
_plan_cache_lock = threading.Lock()


def _plan_cache_key(spec: PlotSpec, table: LongTable) -> "tuple | None":
    """A hashable identity for "this spec's data question, against this table".

    Returns None when the spec cannot be serialized, which disables caching for
    that call rather than failing it — a plan that cannot be keyed is still a
    perfectly good plan.
    """
    try:
        raw = spec.to_dict()
        for field_name in _PLAN_IRRELEVANT_FIELDS:
            raw.pop(field_name, None)
        # `kind` is dropped above as presentation — and for a 1-D measure it is
        # NOT, because the kind decides whether the measure is collapsed to a
        # scalar first (`collapse.collapses`). So the DECISION goes in the key,
        # rather than the kind: box and violin still share one plan, and a line
        # and a violin of the same 1-D measure — different frames, different
        # roles, different y limits — can never share one.
        collapsing = cell_collapses(spec, table)
        if not collapsing:
            # Only meaningful while collapsing; in the key unconditionally it
            # would invalidate a line's plan when the statistic dropdown moved.
            raw.pop("cell_statistic", None)
        return (
            id(table),
            json.dumps(raw, sort_keys=True, default=str),
            collapsing,
        )
    except Exception:  # pragma: no cover - a spec that will fail louder later
        return None


def _short(value: Any, limit: int = 60) -> str:
    """One spec field, short enough to sit in a log line."""
    text = json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _differing_fields(wanted: str, cached: str) -> "list[str] | None":
    """Top-level spec field names that differ, or None if either key is not
    a spec JSON (nothing but ``_plan_cache_key`` should ever put one there)."""
    try:
        a, b = json.loads(wanted), json.loads(cached)
        return [n for n in sorted(set(a) | set(b)) if a.get(n) != b.get(n)]
    except Exception:
        return None


def _describe_differences(wanted: str, cached: str, limit: int = 3) -> list[str]:
    """``field: cached -> wanted``, capped so one miss is one log line."""
    names = _differing_fields(wanted, cached) or []
    a, b = json.loads(wanted), json.loads(cached)
    shown = [f"{n}: {_short(b.get(n))} -> {_short(a.get(n))}" for n in names[:limit]]
    if len(names) > limit:
        shown.append(f"(+{len(names) - limit} more)")
    return shown


def _plan_cache_miss_reason(key: tuple, table: LongTable) -> str:
    """Why this lookup missed — the one thing a ``build_plan`` line cannot say.

    On 2026-09-13 the log showed four consecutive resolves of the SAME figure,
    each rebuilding the plan: identical roles, identical output, four misses.
    A hit logged at DEBUG and a miss logged nothing at all, so there was no way
    to tell a GUI firing four times from a spec field churning between four
    otherwise-identical requests — two very different bugs. This names which.
    """
    with _plan_cache_lock:
        entries = list(_plan_cache)
    if not entries:
        return "cache empty (first resolve of this table)"

    table_id, wanted, collapsing = key
    same_table = [k for k in entries if k[0] == table_id]
    if not same_table:
        return (
            f"no entry for this table ({len(entries)} cached for other table(s)) "
            f"— the table object changed: a rebuilt source, an invalidate(), or "
            f"a plan evicted by another variable (cache holds "
            f"{_PLAN_CACHE_ENTRIES})"
        )
    # Same table, so the SPEC is what differs. Report the CLOSEST cached spec:
    # the fields it disagrees on are exactly what is churning between requests.
    comparable = [k for k in same_table if _differing_fields(wanted, k[1]) is not None]
    if not comparable:
        return f"{len(same_table)} entry(ies) for this table, none comparable"
    closest = min(comparable, key=lambda k: len(_differing_fields(wanted, k[1]) or []))
    differences = _describe_differences(wanted, closest[1])
    if not differences and closest[2] != collapsing:
        # The spec JSON matches and the plans still differ: the plot KIND moved
        # a 1-D measure into or out of its collapsed form, which is a different
        # frame rather than a different drawing (`_plan_cache_key`). Said plainly
        # because "identical spec, cache miss" otherwise reads as a cache bug.
        return (
            "same spec, different collapse: the kind "
            f"{'now collapses' if collapsing else 'no longer collapses'} this "
            "1-D measure to a scalar"
        )
    if not differences:
        # Same table id AND same spec, yet the lookup missed. Either CPython
        # recycled the id onto a different object (the trap `_plan_cache`'s
        # value-side reference exists to make impossible — worth shouting about
        # if it ever appears), or another thread evicted the entry in between.
        with _plan_cache_lock:
            stored = _plan_cache.get(closest)
        if stored is not None and stored[0] is not table:
            return "same key, different table object — an id was reused"
        return "key matched but the entry was evicted concurrently"
    return f"spec differs in {len(differences)} field(s): " + "; ".join(differences)


def clear_plan_cache() -> None:
    """Drop every cached plan.

    The data underneath a plan cannot change without the table object changing
    too (sources rebuild tables rather than mutating them), so this exists for
    tests and for callers that would rather not hold the frames.
    """
    with _plan_cache_lock:
        _plan_cache.clear()


def resolve(
    spec: PlotSpec,
    table: LongTable,
    *,
    max_points: int | None = None,
    narrate: bool = False,
    on_figure=None,
) -> list[ResolvedPlot]:
    """
    Reduce ``spec`` against ``table``.

    Returns one :class:`ResolvedPlot` per combination of the spec's ITERATE
    factors — the interactive equivalent of the pipeline's ``for_each`` fan-out
    over iterated schema keys. The two must always produce the same figure set;
    ``tests/test_fanout_parity.py`` asserts it.

    ``narrate`` logs each figure and each phase **while it runs** rather than
    only on exit — for the interactive save, where one figure is minutes of work
    and the silence was indistinguishable from a hang. It is opt-in, not implied
    by ``max_points=None``: a pipeline endpoint also resolves at full resolution,
    once per iteration, and a 500-iteration run does not want eight lines each.

    ``on_figure(position, total, label)`` is called **as each figure starts**,
    for a caller reporting progress to a user. Before, not after: a fan-out of
    two figures that reports only completions is silent for the entire first
    figure, which is the half of the time the user is actually waiting.
    """
    with Log.timer(
        "resolve", layer=LAYER, extra=str(spec.kind), live=narrate
    ) as timing:
        with timing.phase("plan"):
            plan = _plan(spec, table)
        # Merged once per resolve (the project layer is read live, here) and
        # summarised once; each figure is told its roles by `_build_figure`.
        text = display_text(plan.spec, plan.table)
        log_summary(text, plan.table)

        total = len(plan.groups)
        Log.info(
            "resolving %s of %r: %d figure(s) over %s%s",
            spec.kind,
            plan.spec.y_measure,
            total,
            plan.iterate or "no fan-out",
            "" if max_points else " at full resolution (no downsampling)",
            layer=LAYER,
        )

        figures: list[ResolvedPlot] = []
        for position, (key, group) in enumerate(plan.groups, start=1):
            timing.note("figure %d/%d: %s", position, total, _figure_label(key) or "-")
            if on_figure is not None:
                on_figure(position, total, _figure_label(key))
            figures.append(
                _build_figure(
                    group,
                    plan.spec,
                    plan.table,
                    plan.roles,
                    plan.shape,
                    plan.index_column,
                    figure_key=key,
                    max_points=max_points,
                    explode=plan.explode,
                    narrate=narrate,
                    y_scope=plan.y_scope,
                    y_limits=plan.y_limits,
                    text=text,
                )
            )
        for figure in figures:
            figure.fanout_notes = plan.notes

        Log.info(
            "resolved %s of %r: %d figure(s) over %s, %d panel(s), %d row(s)",
            spec.kind,
            plan.spec.y_measure,
            len(figures),
            plan.iterate or "no fan-out",
            sum(len(f.panels) for f in figures),
            sum(f.row_count for f in figures),
            layer=LAYER,
        )
        return figures


@dataclass
class _Plan:
    """Everything decided before any figure is built.

    Split out so one figure of a fan-out can be built without building the
    others — every step here is shared by all of them, and every step below is
    per figure.
    """

    spec: PlotSpec
    table: LongTable
    roles: dict
    shape: Shape
    index_column: str | None
    #: Whether each figure must explode its own 1-D rows into samples.
    #:
    #: The explode used to happen once, HERE, over the whole frame — and that
    #: made a fan-out pay for every figure in it on every resolve. The
    #: 2026-09-11 log shows the cost plainly: ``exploded 1-D measure
    #: 'FilteredEMG': 48 row(s) -> 17119200 sample(s)`` immediately followed by
    #: ``downsampled 8906400 row(s)`` — half the samples were built for a figure
    #: the panel was not showing and threw away. At 30 subjects it is 30x.
    #:
    #: So the frames below stay nested and each figure explodes its own group.
    #: Grouping first is safe because the fan-out keys are ordinary factor
    #: columns, present and unchanged before the explode; exploding only ever
    #: multiplies rows WITHIN a group.
    explode: bool
    iterate: list[str]
    notes: list[str]
    #: ``(figure_key, frame)`` per figure, IN ORDER. The frames are views, and
    #: still nested: the expensive per-figure work (exploding, aggregating,
    #: panels, downsampling) has not happened yet.
    groups: list[tuple[dict, "pd.DataFrame"]]
    #: The post-filter frame the groups were cut from — what the y limits are
    #: computed over, kept so a second extent mode can be answered later
    #: without re-running the filters.
    frame: "pd.DataFrame" = field(default_factory=pd.DataFrame)
    #: The y-limit scope, after ineligible factors were dropped.
    y_scope: list[str] = field(default_factory=list)
    #: ``{scope values: (low, high)}`` for the WHOLE fan-out, computed here
    #: rather than per figure — a scope of ``[]`` means one range across every
    #: figure, including the ones ``resolve_one`` never builds. Computed off the
    #: table (see :mod:`scistackplot.ylimits`), so knowing it costs a numpy pass
    #: over 48 rows rather than a reduction of 17 million.
    #:
    #: This is the entry for the CALLER's extent mode, attached by
    #: `_with_y_limits`; `y_limits_by_mode` is the memo behind it.
    y_limits: dict = field(default_factory=dict)
    #: ``{ExtentMode: y_limits}``. The plan is keyed by the data question and
    #: deliberately not by the plot kind — but a band draws ``centre ± spread``
    #: and a line draws the observations, so the same rows give different
    #: limits per kind. Until 2026-09-14 a LINE -> BAND switch was a cache HIT
    #: that drew the band inside the line's limits. Memoised per mode rather
    #: than added to the key, so a kind switch still costs only the limits
    #: (25 ms in scidb.log) and never the variants, filters and fan-out.
    #:
    #: The one field of a cached plan that is written after construction —
    #: under `_plan_cache_lock`, and only ever ADDED to; `replace()` copies
    #: share the dict, which is what lets a copy fill the cached plan's memo.
    y_limits_by_mode: dict = field(default_factory=dict)

    @property
    def labels(self) -> list[str]:
        """Every figure's label — knowable without building any of them."""
        return [
            ", ".join(f"{k}={v}" for k, v in key.items()) for key, _ in self.groups
        ]


def _plan(spec: PlotSpec, table: LongTable) -> _Plan:
    """Resolve everything up to, but not including, per-figure work.

    Memoized. Every control change re-resolves, and most of them — plot kind,
    grid shape, colours — do not change a single row of what is planned here,
    yet each one re-folded the variants, re-ran the filters and re-grouped the
    frame. Concurrent duplicates made it worse: the 2026-09-11 log shows four
    resolves in flight at once, each redoing all of it.

    A cached plan is shared, never copied, so the same rule the tables rely on
    applies here too: nothing downstream mutates it. ``_build_figure`` takes the
    group frames and builds new ones (explode, collapse, downsample, panels).
    The single exception is the y-limit memo (`_Plan.y_limits_by_mode`), which
    is only ever added to, under the cache lock.
    """
    key = _plan_cache_key(spec, table)
    if key is not None:
        with _plan_cache_lock:
            hit = _plan_cache.get(key)
        # Identity re-checked under the assumption that ids can be recycled;
        # the strong reference held below should make this impossible.
        if hit is not None and hit[0] is table:
            # INFO, not DEBUG. This cache is the difference between a control
            # change costing nothing and costing a full plan, and the only
            # visible trace of a miss was a `build_plan` timing line that never
            # said it WAS a miss. One line per resolve, either way.
            Log.info(
                "plan cache: HIT (%d entries) — no plan rebuilt",
                len(_plan_cache),
                layer=LAYER,
            )
            plan = _with_presentation(hit[1], spec)
            # The kind is not in the key, and `validate` has a kind rule (a
            # box needs a sample, a spaghetti two layers): a plan built for a
            # bar must not quietly serve a box that could not have been built.
            validate(plan.spec, plan.table)
            return _with_y_limits(plan, spec)
        Log.info("plan cache: MISS — %s", _plan_cache_miss_reason(key, table), layer=LAYER)
    else:
        Log.info(
            "plan cache: DISABLED for this call — the spec did not serialize",
            layer=LAYER,
        )

    plan = _build_plan(spec, table)

    if key is not None:
        with _plan_cache_lock:
            _plan_cache[key] = (table, plan)
            while len(_plan_cache) > _PLAN_CACHE_ENTRIES:
                _plan_cache.pop(next(iter(_plan_cache)))
    return _with_y_limits(_with_presentation(plan, spec), spec)


def planned_y_limits(
    spec: PlotSpec, table: LongTable
) -> tuple[list[str], dict[tuple, tuple[float, float]]]:
    """``(scope, {scope values: (low, high)})`` — the limits the preview draws.

    The one owner of "what y range does this spec get". ``codegen`` bakes these
    numbers into an exported figure, and it used to recompute them itself from
    the table it was handed — which had never been through ``apply_filters``,
    so a location filter that dropped the lowest subject left the export's floor
    at that subject's value while the preview's floor had moved up (2026-09-24).
    Reading the plan instead means the export cannot see different rows, a
    different collapse, or a different scope from the preview.

    ``table`` is the BASE table (before variant sets and level groups), exactly
    as :func:`resolve` takes it. With manual limits the dict is empty — the
    data is never consulted — and callers read ``spec.y_axis`` instead.
    """
    plan = _plan(spec, table)
    return list(plan.y_scope), dict(plan.y_limits)


def _with_presentation(plan: "_Plan", spec: PlotSpec) -> "_Plan":
    """Put the look-only fields back onto a plan's spec.

    Necessary because ``_build_figure`` reads ``plan.spec`` — not the caller's
    spec — for the plot kind, the facet grid and the style. Those are exactly
    the fields the cache key ignores, so without this a cached plan would render
    the FIRST kind it was built with and switching from lines to a box plot
    would silently do nothing.

    Driven off the same constant as the key, so the two cannot drift: a field
    excluded from the key is, by construction, restored here.
    """
    changed = {
        name: getattr(spec, name)
        for name in _PLAN_IRRELEVANT_FIELDS
        if getattr(plan.spec, name) != getattr(spec, name)
    }
    if not changed:
        return plan
    return replace(plan, spec=replace(plan.spec, **changed))


def _with_y_limits(plan: "_Plan", spec: PlotSpec) -> "_Plan":
    """Attach the y limits for THIS spec's extent mode, computing them once.

    The companion of :func:`_with_presentation` for the one thing a kind
    switch does change about the data: the drawn extent. ``plan.spec`` already
    carries the caller's kind and style by the time this runs, so the mode is
    read off it.

    Manual limits short-circuit: with both ends typed by hand the data is never
    consulted, and ``y_axis`` is in the plan key anyway.
    """
    if spec.y_axis.is_manual:
        return replace(plan, y_limits={}) if plan.y_limits else plan
    mode = ExtentMode.for_spec(plan.spec, plan.roles)
    with _plan_cache_lock:
        found = plan.y_limits_by_mode.get(mode)
    if found is None:
        # INFO on a miss only: a hit is the normal case and the plan-cache line
        # above already says one resolve happened. A miss is the moment a kind
        # switch costs something, and this line is what it cost.
        Log.info(
            "y limits: MISS for mode (%s) — computing over %d row(s)",
            mode.describe(),
            len(plan.frame),
            layer=LAYER,
        )
        with Log.timer("y_limits", layer=LAYER, extra=mode.describe()):
            found = _compute_y_limits(plan)
        with _plan_cache_lock:
            plan.y_limits_by_mode.setdefault(mode, found)
    if plan.y_limits is found:
        return plan
    return replace(plan, y_limits=found)


def _compute_y_limits(plan: "_Plan") -> dict:
    """The whole fan-out's limits for `plan.spec`'s extent mode, off its frame."""
    return reducer_for(plan.table).y_extents(
        plan.frame, plan.spec, plan.table, plan.y_scope, plan.roles
    )


def _build_plan(spec: PlotSpec, table: LongTable) -> _Plan:
    """:func:`_plan` without the cache — always does the full work.

    Phase-timed end to end: on 2026-09-13 a ``plot_resolve`` cleared its whole
    DuckDB phase in 16 ms and then never reached the render, so every candidate
    for the missing time was in here — and all of it logged at DEBUG, with the
    file sink at INFO (.claude/plot-at-scale-plan.md §1).
    """
    with Log.timer("build_plan", layer=LAYER, extra=str(spec.kind)) as timer:
        return _build_plan_timed(spec, table, timer)


def _build_plan_timed(spec: PlotSpec, table: LongTable, timer) -> _Plan:
    """:func:`_build_plan`'s body, with ``timer`` supplying the phases."""
    # Named variants become a ``Variant`` factor BEFORE anything else looks
    # at the table, so validation, roles, faceting and rendering all see one
    # ordinary factor rather than each needing a variant special case.
    base = table
    with timer.phase("variant_sets"):
        table = apply_variant_sets(spec, table)
    # Derived grouping factors, after the variants have claimed their
    # columns. Both are derived tables built BEFORE validation and role
    # completion, so nothing below this line knows either factor was
    # synthesized (docs/claude/synthetic-factors.md).
    with timer.phase("level_groups"):
        table = apply_level_groups(spec, table)
    # The third derived table, and the only one that rewrites the MEASURE: a
    # scalar kind selected for a 1-D measure reduces every cell to one value
    # here, and everything below this line sees an ordinary scalar measure.
    #
    # Its position is load-bearing. Before `validate`, so the rules that refuse
    # a factor on X for a 1-D measure (whose x axis is its own index) permit one
    # once the measure genuinely is scalar. Before `apply_filters`, so a range
    # filter on the measure filters the collapsed value — the only thing it
    # could mean. Before `y_limits`, so extents are taken over one float per
    # record rather than every sample.
    with timer.phase("collapse_1d"):
        # Taken BEFORE the collapse — afterwards the measure is scalar and the
        # table can no longer say it was ever anything else. The figure has to
        # carry it: a violin of trial means and a violin of raw samples look
        # identical and mean entirely different things.
        collapsed_note = cell_collapse_note(spec, table)
        table = apply_cell_collapse(spec, table)
    with timer.phase("roles"):
        spec = strip_answered_roles(spec, base, table)
        validate(spec, table)
        roles = complete_roles(spec, table)

    frame = table.frame
    with timer.phase("apply_filters"):
        frame = apply_filters(frame, spec)

    y_measure = spec.y_measure
    shape = table.shape_of(y_measure)
    index_column = spec.index_column or table.index_column or DEFAULT_INDEX_COLUMN

    # Whether each figure has to explode its own rows. Deliberately NOT done
    # here — see `_Plan.explode`.
    explode = shape is Shape.SERIES_1D and not table.measure(y_measure).exploded
    if shape is not Shape.SERIES_1D:
        index_column = None

    # INFO, not DEBUG: this one line says how much data survived the filter, and
    # it is the first thing anyone needs when a resolve does not come back. The
    # extent reports cells and samples, because `rows` alone cannot tell a frame
    # of short arrays from a frame of quarter-million-sample ones.
    Log.info(
        "resolve: measure=%s shape=%s kind=%s post-filter %s roles=%s",
        y_measure,
        shape,
        spec.kind,
        format_extent(frame_extent(frame, [y_measure])),
        {k: str(v) for k, v in roles.items()},
        layer=LAYER,
    )

    # A factor can be absent from the frame if it was filtered to nothing;
    # grouping by it would raise rather than degrade. (An ITERATE factor is
    # never aggregated away — a factor carries one role — so moving the
    # collapse into the figures below does not widen what this can miss.)
    with timer.phase("group_fanout"):
        iterate = [name for name in fanout_keys(spec, table) if name in frame.columns]
        if iterate:
            groups = [
                (dict(zip(iterate, key_values, strict=True)), group)
                for key_values, group in _ordered_groups(frame, iterate, table)
            ]
            if not groups:
                # Filtered to nothing UNDER a fan-out. Without a fan-out this
                # case already yields one empty figure (the branch below), and
                # the panel relies on that: "Deselect all" is a legitimate
                # state to pass through while clicking, and an empty figure
                # explains itself (row_count == 0). With ITERATE factors the
                # groupby produced NO groups, `resolve_one` indexed
                # `plan.groups[-1]` and the panel showed "list index out of
                # range" instead (2026-09-14). One empty figure, same as the
                # non-iterating case.
                Log.info(
                    "fan-out over %s has no groups after filtering — one empty "
                    "figure instead of none",
                    iterate,
                    layer=LAYER,
                )
                groups = [({}, frame)]
        else:
            groups = [({}, frame)]

    # Computed HERE, over the filtered frame, for the whole fan-out at once —
    # a scope of [] means one range across every figure, including the ones
    # `resolve_one` deliberately never builds. Off the table rather than off the
    # figures, so it stays a numpy pass over 48 rows (see `ylimits`).
    #
    # Timed separately because that "48 rows" assumption is exactly what a
    # 419-location variable breaks: `_raw_extents` walks every cell of the
    # measure column in a Python loop, and it is the one step here that
    # deliberately spans figures `resolve_one` will never build.
    #
    # Only THIS spec's extent mode is computed; another kind's is answered by
    # `_with_y_limits` on first use and memoised on the plan.
    plan = _Plan(
        spec=spec,
        table=table,
        roles=roles,
        shape=shape,
        index_column=index_column,
        explode=explode,
        iterate=iterate,
        notes=[collapsed_note] if collapsed_note else [],
        groups=groups,
        frame=frame,
        y_scope=eligible_scope(spec.y_axis.scope, roles, table),
    )
    with timer.phase("y_limits"):
        if not spec.y_axis.is_manual:
            mode = ExtentMode.for_spec(spec, roles)
            plan.y_limits = _compute_y_limits(plan)
            plan.y_limits_by_mode[mode] = plan.y_limits
    return plan


def resolve_one(
    spec: PlotSpec,
    table: LongTable,
    index: int,
    *,
    max_points: int | None = None,
    narrate: bool = False,
) -> tuple[ResolvedPlot, list[str], int]:
    """Build ONE figure of a fan-out. Returns ``(figure, every label, index)``.

    The interactive panel shows one figure at a time and serializes only that
    one, but it was reducing all of them to get there: ``resolve`` builds every
    figure, and building a figure is where the cost is — panels, aggregation and
    downsampling over the whole group. A two-figure fan-out therefore cost twice
    what the user was looking at, every time any control moved.

    Nothing is lost by deferring the rest. The fan-out's SIZE and LABELS come
    from the group keys (:attr:`_Plan.labels`), which are known as soon as the
    frame is grouped — the labels never depended on the figures being built.

    ``index`` is clamped rather than rejected: the fan-out shrinks whenever a
    filter or a variant selection narrows the data, and the panel's cursor is a
    moment behind the spec it is already re-resolving. An out-of-range index is
    a normal transient.

    ``narrate`` is opt-in, exactly as in :func:`resolve`.
    """
    with Log.timer(
        "resolve_one", layer=LAYER, extra=str(spec.kind), live=narrate
    ) as timing:
        with timing.phase("plan"):
            plan = _plan(spec, table)
        text = display_text(plan.spec, plan.table)
        log_summary(text, plan.table)
        position = max(0, min(int(index), len(plan.groups) - 1))
        key, group = plan.groups[position]
        if narrate:
            Log.info(
                "resolving %s of %r: figure %d of %d (%s) at full resolution "
                "(no downsampling)",
                spec.kind,
                plan.spec.y_measure,
                position + 1,
                len(plan.groups),
                _figure_label(key) or "no fan-out",
                layer=LAYER,
            )
        figure = _build_figure(
            group,
            plan.spec,
            plan.table,
            plan.roles,
            plan.shape,
            plan.index_column,
            figure_key=key,
            max_points=max_points,
            explode=plan.explode,
            narrate=narrate,
            y_scope=plan.y_scope,
            y_limits=plan.y_limits,
            text=text,
        )
        figure.fanout_notes = plan.notes
        Log.info(
            "resolved %s of %r: figure %d of %d over %s, %d panel(s), %d row(s) "
            "(%d figure(s) not built)",
            spec.kind,
            plan.spec.y_measure,
            position + 1,
            len(plan.groups),
            plan.iterate or "no fan-out",
            len(figure.panels),
            figure.row_count,
            len(plan.groups) - 1,
            layer=LAYER,
        )
        return figure, plan.labels, position


# ---------------------------------------------------------------------------
# Stage helpers
# ---------------------------------------------------------------------------


def _location_mask(frame: pd.DataFrame, spec: PlotSpec) -> "pd.Series | None":
    """Rows selected by ``spec.location_filter``, or None if it is inert.

    Two clauses, and a row survives both or neither (the whole rule, with the
    cases this is tested against, is docs/claude/location-filter-semantics.md):

    * ``include`` prefixes — a set of PLACES, possibly ragged.
    * ``exclude_levels`` — a standing RULE per key, applied AFTER coverage and
      always winning. "All of subject 01" plus "BL is out" draws subject 01
      minus its BL sessions.

    NULL at a key is not a level: a null row matches no prefix step and is
    dropped by no rule. Cross-cutting records (saved at subject+speed with
    ``timepoint`` NULL) make that routine rather than exotic — without it, the
    first level rule anyone writes would delete every one of them.

    One prefix constrains only the keys it names **that the frame actually
    has**. A key the frame lacks is simply not constrained — the graceful answer
    for a shallower variable (a subject-level Mass has one value for "subject 02
    trial 3", and that is the value that contributes).

    Each prefix carries its own keys, so a non-contiguous location
    (``subject`` + ``speed``, ``timepoint`` NULL) matches the keys it names
    rather than whatever sits at that position in the schema — see
    :class:`~scistackplot.spec.LocationFilter`.

    Written as one vectorised comparison per (prefix, key) rather than a row-wise
    tuple match, because :func:`scistackplot.codegen` has to emit the *same*
    rule as readable pandas and the two must not be able to disagree. That makes
    the cost O(prefixes x depth) column comparisons; a single-select is one
    prefix, and a ticked subtree collapses to its own short prefix, so the
    common cases are small by construction.
    """
    prefixes = spec.location_filter.prefixes()
    excluded = spec.location_filter.excluded()
    if not prefixes and not excluded:
        return None  # inert: clicking into a picker is not a statement

    mask: "pd.Series | None" = None

    if prefixes:
        named = {key for prefix in prefixes for key, _value in prefix}
        known = [key for key in named if key in frame.columns]
        if not known:
            # Inert rather than empty: a subject-level Mass contributes its one
            # value to every trial, and a trial prefix must not delete it.
            Log.warn(
                "location filter names none of this table's columns (%s) — "
                "those prefixes are ignored",
                ", ".join(sorted(named)),
                layer=LAYER,
            )
        else:
            as_text = {key: frame[key].astype(str) for key in known}
            present = {key: frame[key].notna() for key in known}
            mask = pd.Series(False, index=frame.index)
            for prefix in prefixes:
                matched = pd.Series(True, index=frame.index)
                for key, value in prefix:
                    if key in as_text:
                        matched &= present[key] & as_text[key].isin(
                            value_spellings(value)
                        )
                mask |= matched

    for key, values in excluded.items():
        if key not in frame.columns:
            # The rule's twin of the prefix case above: a table that never
            # carried `trial` must not lose every row to a trial rule.
            Log.debug(
                "location filter excludes %s levels of '%s', a column this "
                "table does not have — no rows dropped",
                len(values),
                key,
                layer=LAYER,
            )
            continue
        spellings: set[str] = set()
        for value in values:
            spellings.update(value_spellings(value))
        drop = frame[key].notna() & frame[key].astype(str).isin(spellings)
        mask = ~drop if mask is None else (mask & ~drop)

    return mask


def apply_filters(frame: pd.DataFrame, spec: PlotSpec) -> pd.DataFrame:
    """Rows surviving ``spec.filters`` and ``spec.location_filter``.

    Public because the GUI's pickers report "3 of 12 selected" and that readout
    has to be measured with exactly the rule the figure uses — the same reason
    ``variants.row_mask`` is shared. A count the figure disagrees with is worse
    than no count.

    Level membership is compared **as text**, matching ``variant_set_mask``: a
    selection crosses JSON as strings while the column may hold ``01`` (string)
    or ``1`` (int) depending on the source, and a silently empty figure is the
    worst possible answer to a picker the user just clicked.
    """
    location = _location_mask(frame, spec)
    if not spec.filters and location is None:
        return frame
    mask = pd.Series(True, index=frame.index)
    if location is not None:
        mask &= location
        kept = int(mask.sum())
        excluded = spec.location_filter.excluded()
        Log.debug(
            "location filter: %d -> %d row(s) over %d prefix(es), "
            "omitting %s",
            len(frame),
            kept,
            len(spec.location_filter.prefixes()),
            {key: list(values) for key, values in excluded.items()} or "nothing",
            layer=LAYER,
        )
        # An empty figure has two opposite causes — no data, or a selection
        # that kept none of it — and they look identical on screen. Say which,
        # the way scifor does for a run that filters away every combo.
        if len(frame) and not kept:
            Log.warn(
                "location filter kept none of the %d row(s): %d prefix(es), "
                "omitting %s. The figure will be empty — this is the "
                "selection, not missing data.",
                len(frame),
                len(spec.location_filter.prefixes()),
                {key: list(values) for key, values in excluded.items()} or "nothing",
                layer=LAYER,
            )
    for flt in spec.filters:
        if flt.column not in frame.columns:
            # A spec outlives the table it was written against — a filter naming
            # a column this table lacks is stale, not fatal.
            Log.warn(
                "filter on unknown column %r ignored", flt.column, layer=LAYER
            )
            continue
        column = frame[flt.column]
        before = int(mask.sum())
        if flt.include is not None:
            as_text = column.astype(str)
            mask &= as_text.isin({str(v) for v in flt.include})
        if flt.exclude is not None:
            as_text = column.astype(str)
            mask &= ~as_text.isin({str(v) for v in flt.exclude})
        if flt.minimum is not None:
            mask &= pd.to_numeric(column, errors="coerce") >= flt.minimum
        if flt.maximum is not None:
            mask &= pd.to_numeric(column, errors="coerce") <= flt.maximum
        # Per column, so an empty figure names the filter that emptied it
        # rather than only the total.
        Log.debug(
            "filter on %s: %d -> %d row(s)",
            flt.column,
            before,
            int(mask.sum()),
            layer=LAYER,
        )
    filtered = frame[mask]
    Log.info(
        "filters kept %d of %d row(s)", len(filtered), len(frame), layer=LAYER
    )
    return filtered


def _explode_1d(
    frame: pd.DataFrame, measure: str, index_column: str
) -> tuple[pd.DataFrame, str]:
    """
    Turn one array per row into one row per sample, adding an index column.

    The index is positional (0..n-1 within each original row). A source that
    knows a real axis — time in seconds, percent of gait cycle — supplies it as
    an ordinary column and sets ``LongTable.index_column``, in which case the
    measure arrives already exploded and this never runs.
    """
    if index_column in frame.columns:
        # Caller supplied a real axis but left the arrays nested: unusual, but
        # exploding would misalign it, so refuse loudly rather than corrupt.
        raise ValueError(
            f"Cannot explode 1-D measure {measure!r}: column {index_column!r} "
            f"already exists. Set LongTable.index_column and pre-explode, or "
            f"choose a different PlotSpec.index_column."
        )

    working = frame.copy()
    working[index_column] = working[measure].map(
        lambda v: list(range(len(v))) if _is_sequence(v) else []
    )
    exploded = working.explode([measure, index_column], ignore_index=True)
    exploded = exploded.dropna(subset=[measure])
    exploded[measure] = coerce_numeric(exploded[measure])
    exploded[index_column] = coerce_numeric(exploded[index_column])

    # INFO, not DEBUG: this is the dominant cost of a resolve and the one number
    # that explains a slow panel. A 12-field struct of 1-D arrays goes from a
    # 24-row frame to several million here, and nothing caches the result.
    # Without this line at INFO the only visible trace is the downsample
    # warning, which reports the figure's rows rather than the frame's.
    #
    # Now emitted once per FIGURE BUILT rather than once per resolve (see
    # `_Plan.explode`), which is the point: a fan-out whose figures are not
    # being looked at should produce no line here at all, and a second line
    # appearing is a fan-out actually being rendered, not waste.
    Log.info(
        "exploded 1-D measure %r: %d row(s) -> %d sample(s) (x%d)",
        measure,
        len(frame),
        len(exploded),
        round(len(exploded) / len(frame)) if len(frame) else 0,
        layer=LAYER,
    )
    return exploded, index_column


def _is_sequence(value: Any) -> bool:
    return isinstance(value, (list, tuple, np.ndarray))


def _collapse_levels(
    frame: pd.DataFrame,
    keys: list[str],
    spec: PlotSpec,
    table: LongTable,
    index_column: str | None,
    *,
    pooled: bool = False,
) -> pd.DataFrame:
    """Average the measures over each of ``keys`` in turn — the collapse chain.

    Each step groups on EVERY other factor still in the frame (plus the
    sample index of a 1-D measure), so a key averages away *within* every
    combination of the keys outside it: trial within subject, then subject.
    That is the nested, unweighted mean — each subject counts once however
    many trials it has. ``keys`` comes from ``roles.collapse_steps`` (deepest
    first), never from the roles dict's order.

    ``pooled`` drops every key in ONE step instead — the "weight by N"
    reading, one groupby over everything else. The marks' own chain never
    needs it (``collapse_steps`` empties ``pre`` and lets ``_summarize`` pool),
    but the "Show sample" overlay averages its cut-off keys explicitly and has
    to pool them the same way the marks did, or the points would sit around a
    centre they were not averaged into.

    A step that finds its key already absent (filtered away, or a variable
    saved above that level) is a no-op, logged.
    """
    if not keys:
        return frame
    measures = [
        m for m in dict.fromkeys([*spec.measures, spec.x_measure]) if m and m in frame.columns
    ]
    factor_columns = [name for name in table.factor_names if name in frame.columns]
    present = [key for key in keys if key in frame.columns]
    for key in keys:
        if key not in present:
            Log.debug("collapse %s: not in the frame, nothing to do", key, layer=LAYER)
    if not present:
        return frame
    # One pass per key (nested), or one pass dropping them all (pooled).
    passes = [[key] for key in present] if not pooled else [present]
    for dropped in passes:
        keep = [name for name in factor_columns if name not in dropped and name in frame.columns]
        if index_column and index_column in frame.columns:
            keep.append(index_column)
        before = len(frame)
        if keep:
            frame = (
                frame.groupby(keep, dropna=False, sort=False)[measures]
                .mean()
                .reset_index()
            )
        else:
            # Everything collapses to a single value.
            frame = frame[measures].mean().to_frame().T
        Log.debug(
            "collapse %s within %s: %d -> %d row(s)",
            " x ".join(dropped) + (" (pooled)" if pooled else ""),
            keep or "nothing",
            before,
            len(frame),
            layer=LAYER,
        )
    return frame


def _sample_frame(
    frame: pd.DataFrame,
    steps: CollapseSteps,
    spec: PlotSpec,
    table: LongTable,
    index_column: str | None,
) -> pd.DataFrame:
    """The SAMPLE rows of one figure: its frame with the pre-collapse run
    (``steps.pre``, deepest first, nested and unweighted).

    What every kind draws (schema-level parity, 2026-09-19) and what "Save
    data" writes by default. One function so the two cannot drift: the
    figure path (``_build_figure``) and the export (``export.plot_data``)
    both call this, never ``_collapse_levels`` with a chain of their own.
    """
    return _collapse_levels(frame, steps.pre, spec, table, index_column)


# ---------------------------------------------------------------------------
# Figure construction
# ---------------------------------------------------------------------------


def _describe_steps(steps: CollapseSteps) -> str:
    """``trial -> subject (sample)`` — the chain, for a timing line."""
    if not steps.all:
        return "nothing collapsed"
    parts = [*steps.pre, " x ".join(steps.sample) + " (sample)"]
    text = " -> ".join(parts)
    if steps.final:
        text += ", averaged into each spaghetti line (not repeated across x)"
    return text


def _figure_label(figure_key: dict[str, Any]) -> str:
    """``subject=03, pass=1`` — the same text ``ResolvedPlot.figure_label``
    produces, available before the figure exists so the log can name what it is
    working on rather than what it finished."""
    return ", ".join(f"{k}={v}" for k, v in figure_key.items())


def _build_figure(
    frame: pd.DataFrame,
    spec: PlotSpec,
    table: LongTable,
    roles: dict[str, Role],
    shape: Shape,
    index_column: str | None,
    *,
    figure_key: dict[str, Any],
    max_points: int | None,
    explode: bool = False,
    narrate: bool = False,
    y_scope: list[str] | None = None,
    y_limits: dict | None = None,
    text: DisplayText | None = None,
) -> ResolvedPlot:
    # ``narrate`` makes this function announce each phase as it starts. A
    # full-resolution figure is minutes of work (scidb.log 2026-09-11: 1543s for
    # two of them), and until the phases said so while they ran, the only
    # evidence available was one summary line that a timed-out save never
    # reached. The interactive path leaves it off: at 20k rows the narration
    # would outnumber the work.
    with Log.timer(
        "build_figure",
        layer=LAYER,
        extra=_figure_label(figure_key) or str(spec.kind),
        live=narrate,
    ) as timing:
        # This figure's own rows, expanded and collapsed here rather than once
        # over the whole fan-out (see `_Plan.explode`). The collapse chain
        # (`roles.collapse_steps`): `pre` keys average away first, deepest
        # first; what remains is the SAMPLE, which every kind draws. Three
        # routes for a 1-D measure that is still nested one cell per row:
        #
        # * BAND / BAR: never exploded. Each panel is summarised straight from
        #   its cells (`reducer.summarize_series`) — the pre-collapse, then
        #   centre ± spread at every position over the sample — and the
        #   transport stride is applied to the SUMMARY afterwards. The old
        #   order (explode, stride, summarise) drew a band over 1/8707th of
        #   the samples at scale, and built 174 M rows to do it.
        # * a pre-collapse, any other kind: `reducer.collapse_series` gives
        #   the per-position means, already exploded (sample levels x kept
        #   factor combinations x positions).
        # * otherwise: `reducer.explode_series`, which may apply the transport
        #   stride itself so only the kept rows' labels are ever gathered.
        reducer = reducer_for(table)
        steps = collapse_steps(spec, roles, table)
        if steps.final:
            # The one exception to schema-level parity — said at INFO, because
            # the figure then draws means where every other kind draws rows.
            Log.info(
                "spaghetti draws the mean of %s per line: %s",
                " x ".join(steps.final),
                spaghetti_sample_repeats(spec, roles, table, steps.sample)[1],
                layer=LAYER,
            )
        # "Show sample" (PlotSpec.show_sample): the same chain cut before the
        # deepest shown key, run on THIS figure's rows before the marks' own
        # chain consumes them, and drawn as points on top of the marks.
        overlay = _plan_overlay(spec, roles, table, shape, explode)
        overlay_frame: pd.DataFrame | None = None
        summarize_nested = explode and spec.kind in (PlotKind.BAND, PlotKind.BAR)
        pre_strided_from: int | None = None
        if explode and not summarize_nested:
            if steps.pre or steps.final:
                with timing.phase("collapse_series", extra=f"{len(frame)} row(s)"):
                    frame, index_column = reducer.collapse_series(
                        frame, spec, roles, index_column, table
                    )
            else:
                with timing.phase("explode", extra=f"{len(frame)} row(s)"):
                    frame, index_column, total = reducer.explode_series(
                        frame, spec.y_measure, index_column, table, max_points=max_points
                    )
                    if len(frame) < total:
                        pre_strided_from = total  # the reducer logged the stride
        elif not explode and shape is not Shape.MATRIX_2D:
            # (A 2-D measure's panel is the elementwise mean of its matrices —
            # `matrix_mean` — which pools; the chain has nothing to add there.)
            if overlay is not None:
                # From the rows the marks' chain is about to consume — the same
                # filtered, faceted figure — so a point and its mark agree.
                with timing.phase(
                    "sample_overlay",
                    extra=f"average {overlay.averaged or 'nothing'}, show {overlay.shown}",
                ):
                    overlay_frame = _collapse_levels(
                        frame,
                        overlay.averaged,
                        spec,
                        table,
                        index_column,
                        pooled=spec.aggregate.pooled,
                    )
                    timing.note("%d overlay row(s)", len(overlay_frame))
            with timing.phase("collapse_levels", extra=_describe_steps(steps)):
                # The sample rows — the SAME call "Save data" makes
                # (`export.plot_data`), so the CSV is what the marks see.
                frame = _sample_frame(frame, steps, spec, table, index_column)
                # Only a spaghetti whose sample cannot be joined across x
                # (roles.spaghetti_sample_repeats); empty for every other kind.
                frame = _collapse_levels(frame, steps.final, spec, table, index_column)
                if steps.sample:
                    timing.note(
                        "sample = %s%s",
                        " x ".join(steps.sample),
                        " (pooled)" if len(steps.sample) > 1 else "",
                    )

        # The grouping list as THIS kind reads it (`roles.grouping_layers`):
        # ticks in drawing order (outermost first), series ids, the colour.
        # Never reversed here — the spec is innermost-first, and the one place
        # the two orders meet is that function.
        layers = grouping_layers(spec, table, roles, spec.kind)
        color = layers.color if layers.color in frame.columns else None
        x_layers = [name for name in layers.ticks if name in frame.columns]
        series_layers = [name for name in layers.series if name in frame.columns]
        # The sample keys a line / spaghetti draws one polyline each for
        # (`GroupingLayers.units`): part of the series id, never a dash.
        unit_layers = [name for name in layers.units if name in frame.columns]
        if unit_layers:
            timing.note("one line per %s", " x ".join(reversed(unit_layers)))
        # What PARTITIONS the marks, stated apart from what PAINTS them: the
        # coloured layer is one of the ticks / series, so the partition is the
        # same with the colour tag on or off. A log reader comparing two
        # resolves that differ only in `color` should see this line unchanged.
        Log.info(
            "grouping: ticks=%s (outermost first) series=%s units=%s -> marks "
            "partitioned by %s; colour=%s paints only",
            x_layers,
            series_layers,
            unit_layers,
            [*x_layers, *series_layers, *unit_layers],
            color,
            layer=LAYER,
        )
        x_factor = x_layers[0] if x_layers else None
        # Several factors may be faceted at once; their combined levels are the
        # panels, and FacetOptions decides how those panels are arranged.
        facet_names = [
            name
            for name, role in roles.items()
            if role is Role.FACET and name in frame.columns
        ]

        original_rows = pre_strided_from or len(frame)
        if (
            max_points is not None
            and pre_strided_from is None
            and not summarize_nested
            and original_rows > max_points
        ):
            with timing.phase("downsample"):
                frame = reducer.downsample(frame, max_points, index_column)

        panels: list[Panel] = []

        with timing.phase("facet_groups"):
            if facet_names:
                groups = _ordered_groups(frame, facet_names, table)
            else:
                groups = [((), frame)]

        # The dominant phase at full resolution, and the one worth a per-panel
        # heartbeat: a figure that stops here has stopped in a specific panel.
        with timing.phase("panel_frames", extra=f"{len(groups)} panel(s)"):
            for position, (key_values, group) in enumerate(groups, start=1):
                key = dict(zip(facet_names, key_values, strict=True))
                timing.note(
                    "panel %d/%d (%s): %d row(s)",
                    position,
                    len(groups),
                    ", ".join(str(v) for v in key.values()) or "unfaceted",
                    len(group),
                )
                if summarize_nested:
                    panel_frame = reducer.summarize_series(
                        group, spec, roles, index_column, table
                    )
                else:
                    panel_frame = _panel_frame(
                        group,
                        spec,
                        table,
                        shape,
                        x_layers,
                        color,
                        index_column,
                        series_layers=series_layers,
                        unit_layers=unit_layers,
                    )
                panels.append(
                    Panel(
                        frame=panel_frame,
                        key=key,
                        # The panel's OWN limits, from the figure's key plus its
                        # own facet values: the scope decides which of those two
                        # actually separate anything.
                        y_limits=limits_for(
                            y_limits or {},
                            {**figure_key, **key},
                            y_scope or [],
                            spec.y_axis,
                        ),
                    )
                )

        sample_color = overlay_color(spec, overlay) if overlay_frame is not None else None
        if overlay_frame is not None:
            with timing.phase("sample_panels", extra=f"{len(panels)} panel(s)"):
                _attach_overlay(
                    panels,
                    overlay_frame,
                    facet_names,
                    table,
                    spec,
                    x_layers,
                    color,
                    overlay.shown,
                    sample_color,
                    # On a spaghetti a point belongs to a LINE, not a tick:
                    # the marks' own series id, recomposed on the overlay rows.
                    line_layers=(
                        [*unit_layers, *series_layers]
                        if spec.kind is PlotKind.SPAGHETTI
                        else []
                    ),
                )

        if summarize_nested:
            # The transport stride, over the SUMMARY rows of the whole figure —
            # the same figure-level stride `_downsample` applies, now after the
            # statistics rather than before them.
            original_rows = sum(len(panel.frame) for panel in panels)
            if max_points is not None and original_rows > max_points:
                with timing.phase("downsample", extra=f"{original_rows} summary row(s)"):
                    stride = max(1, original_rows // max_points)
                    for panel in panels:
                        panel.frame = panel.frame.iloc[::stride].reset_index(drop=True)
                    Log.warn(
                        "downsampled %d summary row(s) to %d for transport (stride=%d)",
                        original_rows,
                        sum(len(panel.frame) for panel in panels),
                        stride,
                        layer=LAYER,
                    )

        with timing.phase("grid_layout"):
            n_rows, n_cols, row_labels, col_labels, layout_notes = _assign_grid(
                panels, spec.facet
            )

            dash_layers = [name for name in series_layers if name != color]
            dash_styles = (
                _dash_styles(panels) if spec.kind in (PlotKind.LINE, PlotKind.BAND) else {}
            )
            encoding = _encoding_for(
                spec.kind, color, shape, bool(series_layers or unit_layers), bool(dash_styles)
            )
            # What this figure's text reads as (`aliases`): merged once per
            # resolve, told here which factor plays which role. Checked before
            # anything is drawn: two levels that would read the same are refused.
            figure_text = (text or display_text(spec, table)).for_figure(
                x_layers=x_layers,
                color=color,
                sample_color=sample_color,
                # Outermost first, the order dash ids are composed in.
                dash_layers=list(reversed(dash_layers)) if dash_styles else [],
            )
            check_distinct(
                figure_text,
                table,
                [*x_layers, color, sample_color, *dash_layers, *facet_names, *figure_key],
            )
            labels = _labels_for(
                spec, table, x_layers, color, index_column, figure_key,
                dash_layers=dash_layers if dash_styles else [],
                sample_color=sample_color,
                text=figure_text,
            )

            # A nested axis is composed once, here, from labels only — so both
            # renderers draw the same brackets and codegen can replay the result
            # instead of re-deriving it (same bargain as plan_layout).
            # Its TEXT is aliased; its `order` (the keys marks are placed by) is not.
            x_plan = (
                figure_text.x_plan(_plan_nested_x(panels, table, x_layers))
                if len(x_layers) > 1
                else None
            )

        # The FIGURE's limits: the panels' own, when they all agree. Not a
        # second computation — every consumer that reads one number per figure
        # (the GUI, `render.base.shared_y_limits`) has to get the same answer
        # the panels got, and `None` here now means "the panels differ", which
        # is exactly when a renderer must stop sharing an axis.
        figure_limits = _figure_limits(panels)
        color_order = _level_order(table, color, [p.frame for p in panels], COLOR) if color else None

        # The overlay's join decision and offsets, once per FIGURE (see
        # ResolvedPlot.sample_offsets): a subject keeps its place in every panel.
        join = overlay_join(spec, roles, table, overlay) if overlay_frame is not None else None
        # Once per figure, like the offsets themselves: on a spaghetti the
        # marks are points on lines spread over the tick, so an overlay point
        # must stay inside its own line's gap (`spaghetti.overlay_offsets`).
        line_offsets = (
            _spaghetti_offsets(panels) if spec.kind is PlotKind.SPAGHETTI else {}
        )
        sample_offsets = (
            _overlay_offsets(panels, max(len(line_offsets), 1))
            if overlay_frame is not None
            else {}
        )
        # The overlay's own palette order, figure-wide (see
        # ResolvedPlot.sample_color_order): read off the panels' SAMPLE
        # frames the way color_order reads off their mark frames.
        sample_color_order = (
            _level_order(
                table,
                sample_color,
                [p.sample for p in panels if p.sample is not None],
                SAMPLE_COLOR,
            )
            if sample_color
            else None
        )
        if sample_color:
            from .render.base import SAMPLE_PALETTE

            Log.info(
                "sample overlay coloured by %r: %d level(s)%s",
                sample_color,
                len(sample_color_order or []),
                " — joined across colour levels" if join is not None and join.join else "",
                layer=LAYER,
            )
            if len(sample_color_order or []) > len(SAMPLE_PALETTE):
                Log.warn(
                    "%d %s level(s) share %d overlay colours — colours will repeat; "
                    "filter the locations or separate panels to tell them apart",
                    len(sample_color_order or []),
                    sample_color,
                    len(SAMPLE_PALETTE),
                    layer=LAYER,
                )
        elif color and join is not None and join.join:
            from .render.base import SAMPLE_CROSS_LINE_COLOR

            # Why the lines are grey: in the marks' colour a line that spans
            # the coloured layer has no one colour (render.base.sample_runs).
            Log.info(
                "sample overlay in the marks' colour (%r): each point its mark's colour; "
                "a line crossing %r levels is drawn in %s",
                color,
                color,
                SAMPLE_CROSS_LINE_COLOR,
                layer=LAYER,
            )

        return ResolvedPlot(
            kind=spec.kind,
            panels=panels,
            encoding=encoding,
            labels=labels,
            spec=spec,
            figure_key=figure_key,
            x_order=(
                list(x_plan.order)
                if x_plan
                else (
                    _level_order(table, x_factor, [p.frame for p in panels], X)
                    if x_factor
                    else _unlabelled_x_order(panels)
                )
            ),
            x_plan=x_plan,
            x_layers=list(x_layers),
            color_factor=color,
            color_order=color_order,
            grid_rows=n_rows,
            grid_cols=n_cols,
            row_labels=row_labels,
            col_labels=col_labels,
            layout_notes=layout_notes,
            y_limits=figure_limits,
            y_scope=list(y_scope or []),
            # The figure's own ITERATE keys (fan-out order) then its facets:
            # the factors a y-limit scope may name, as RESOLVED — a defaulted
            # iterate or facet is in here and not in `spec.roles`.
            panel_factors=[*figure_key.keys(), *facet_names],
            downsampled_from=(
                original_rows if max_points and original_rows > max_points else None
            ),
            series_offsets=line_offsets,
            dash_styles=dash_styles,
            sample_shown=list(overlay.shown) if overlay_frame is not None else [],
            sample_join=bool(join.join) if join is not None else False,
            sample_join_reason=join.reason if join is not None else "",
            sample_offsets=sample_offsets,
            text=figure_text,
            labelable=labelable(
                figure_text,
                table,
                measure=spec.y_measure,
                factors=[
                    *((name, "x axis") for name in x_layers),
                    (color, "colour"),
                    (sample_color, "sample colour"),
                    *((name, "dash") for name in dash_layers),
                    *((name, "panels") for name in facet_names),
                    *((name, "figures") for name in figure_key),
                ],
            ),
            sample_color=sample_color,
            sample_color_order=list(sample_color_order or []),
        )


def _dash_styles(panels: list[Panel]) -> dict[str, str]:
    """One dash style per uncoloured series id across the WHOLE figure, in
    natural order of the ids, cycling through ``DASH_CYCLE``. Figure-wide so
    a subject keeps its dash in every panel and every colour. Warns past the
    cycle: seven dashed lines are not distinguishable, and Separate panels is
    the honest fix."""
    ids: set[str] = set()
    for panel in panels:
        if DASH in panel.frame.columns and not panel.frame.empty:
            ids.update(str(v) for v in panel.frame[DASH].dropna().unique())
    ordered = sorted(ids, key=natural_sort_key)
    styles = {sid: DASH_CYCLE[i % len(DASH_CYCLE)] for i, sid in enumerate(ordered)}
    if len(ordered) > len(DASH_CYCLE):
        Log.warn(
            "%d uncoloured series share %d dash styles — lines will repeat a "
            "style; give the layer Separate panels or a colour to tell them apart",
            len(ordered),
            len(DASH_CYCLE),
            layer=LAYER,
        )
    return styles


def _spaghetti_offsets(panels: list[Panel]) -> dict[str, float]:
    """One offset per series across the WHOLE figure (see ResolvedPlot)."""
    ids: set[str] = set()
    for panel in panels:
        if SERIES in panel.frame.columns and not panel.frame.empty:
            ids.update(str(v) for v in panel.frame[SERIES].unique())
    offsets = series_offsets(ids)
    Log.debug(
        f"[spaghetti] {len(offsets)} series offset across "
        f"{len(panels)} panel(s): "
        + ", ".join(f"{k}={v:+.3f}" for k, v in list(offsets.items())[:8])
        + (" …" if len(offsets) > 8 else "")
    )
    return offsets


# ---------------------------------------------------------------------------
# "Show sample": the overlay
# ---------------------------------------------------------------------------


def _plan_overlay(
    spec: PlotSpec,
    roles: dict[str, Role],
    table: LongTable,
    shape: Shape,
    explode: bool,
) -> OverlaySteps | None:
    """The overlay chain for this figure, or None when none is drawn.

    ``roles.overlay_unavailable`` is the one statement of what an overlay
    needs; an exploded 1-D figure never qualifies (its shape is not scalar),
    and the check is repeated here only so the figure path cannot drift from
    the capability report's answer.
    """
    if not spec.show_sample or explode:
        return None
    reason = overlay_unavailable(spec, roles, shape)
    if reason is not None:
        Log.debug("show_sample %s: no overlay — %s", spec.show_sample, reason, layer=LAYER)
        return None
    return overlay_steps(spec, roles, table)


def _attach_overlay(
    panels: list[Panel],
    overlay_frame: pd.DataFrame,
    facet_names: list[str],
    table: LongTable,
    spec: PlotSpec,
    x_layers: list[str],
    color: str | None,
    shown: list[str],
    sample_color: str | None = None,
    line_layers: list[str] = (),
) -> None:
    """Split the overlay rows into the panels the marks were split into and
    give each panel its ``sample`` frame (:func:`_overlay_frame`)."""
    if facet_names:
        by_key = {
            tuple(key_values): group
            for key_values, group in _ordered_groups(overlay_frame, facet_names, table)
        }
    else:
        by_key = {(): overlay_frame}
    for panel in panels:
        group = by_key.get(tuple(panel.key.get(name) for name in facet_names))
        if group is None or group.empty:
            # A panel whose overlay rows all filtered to nothing: no points,
            # not an error — the marks are still drawn.
            panel.sample = _overlay_frame(
                overlay_frame.iloc[:0], spec, x_layers, color, shown, sample_color, line_layers
            )
            continue
        panel.sample = _overlay_frame(
            group, spec, x_layers, color, shown, sample_color, line_layers
        )
        # Runs per identity: >1 means the brackets split a subject's line —
        # the span rule at work; 1 everywhere means a flat axis.
        runs = panel.sample.groupby([SERIES, RUN], sort=False).ngroups
        Log.debug(
            "sample overlay: %d point(s) in panel %s; %d run(s) over %d identity(ies), "
            "span=%r brackets=%s",
            len(panel.sample),
            panel.title or "unfaceted",
            runs,
            panel.sample[SERIES].nunique(),
            x_layers[-1] if x_layers else None,
            list(x_layers[:-1]),
            layer=LAYER,
        )


def _overlay_frame(
    group: pd.DataFrame,
    spec: PlotSpec,
    x_layers: list[str],
    color: str | None,
    shown: list[str],
    sample_color: str | None = None,
    line_layers: list[str] = (),
) -> pd.DataFrame:
    """The overlay's tidy frame: the SAME ``__x`` / ``__color`` the marks use
    (so a renderer places a point by the mark's own position and paints it
    the mark's colour), the value, a ``__series`` id from the shown keys
    (outermost first, like every other composed id) and the shown key columns
    themselves for hover. ``__sample_color`` (``sample_color``, a shown key)
    is the point's OWN colour level, beside the mark's — see
    ``resolved.SAMPLE_COLOR``. On a spaghetti, ``__line`` (``line_layers``:
    the marks' units + lines layer, innermost first — ``GroupingLayers.identity``)
    is the id of the line the point sits on, composed exactly as the marks'
    ``__series`` is (``_series_key``) so ``series_offsets`` finds it.
    ``__run`` (``resolved.RUN``) is the bracket layers — ``x_layers``
    minus the innermost — plus that line, composed: a joined line spans the
    innermost tick only, so the renderers draw one polyline per
    ``(__series, __run)``."""
    out = pd.DataFrame(index=group.index)
    if len(x_layers) > 1:
        out[X] = _composed_key(group, x_layers, LEAF_SEPARATOR)
    elif x_layers:
        out[X] = group[x_layers[0]].values
    else:
        out[X] = UNLABELLED_X
    out[Y] = coerce_numeric(group[spec.y_measure])
    if color:
        out[COLOR] = group[color].values
    present = [name for name in shown if name in group.columns]
    out[SERIES] = _composed_key(group, present, SERIES_SEPARATOR) if present else ""
    for name in present:
        out[name] = group[name].values
    if sample_color and sample_color in group.columns:
        out[SAMPLE_COLOR] = group[sample_color].values
    if line_layers:
        out[SAMPLE_LINE] = _series_key(group, list(line_layers))
    out[RUN] = _run_key(group, x_layers, list(line_layers))
    return out.dropna(subset=[Y]).reset_index(drop=True)


def _run_key(
    group: pd.DataFrame, x_layers: list[str], line_layers: list[str]
) -> "np.ndarray | str":
    """``resolved.RUN`` for overlay rows: the bracket layers (every
    tick above the innermost, outermost first — ``GroupingLayers.brackets``)
    and, on a spaghetti, the line the point sits on (``line_layers``,
    innermost first, reversed like ``_series_key``), composed with
    ``LEAF_SEPARATOR``. ``""`` when nothing splits the runs, so a groupby on
    it is a no-op rather than a missing column."""
    brackets = list(x_layers[:-1])
    parts = [*brackets, *reversed([name for name in line_layers if name in group.columns])]
    if not parts:
        return ""
    return _composed_key(group, parts, LEAF_SEPARATOR)


def _overlay_offsets(panels: list[Panel], n_slots: int) -> dict[str, float]:
    """One offset per overlay identity across the WHOLE figure, inside its
    mark (``spaghetti.overlay_offsets``): the whole tick, or one line's
    share of it on a spaghetti (``n_slots`` = the number of lines)."""
    ids: set[str] = set()
    for panel in panels:
        if panel.sample is not None and not panel.sample.empty:
            ids.update(str(v) for v in panel.sample[SERIES].unique())
    offsets = overlay_offsets(ids, n_slots)
    Log.debug(
        "sample overlay: %d identity offset(s) across %d panel(s), %d slot(s)",
        len(offsets),
        len(panels),
        n_slots,
        layer=LAYER,
    )
    return offsets


def _panel_frame(
    group: pd.DataFrame,
    spec: PlotSpec,
    table: LongTable,
    shape: Shape,
    x_layers: list[str],
    color: str | None,
    index_column: str | None,
    series_layers: list[str] = (),
    unit_layers: list[str] = (),
) -> pd.DataFrame:
    """Build the canonical ``__x``/``__y``/… frame the renderers consume.

    ``x_layers`` are the tick layers in drawing order (outermost first) and
    ``series_layers`` the series-identity layers — both already read off the
    grouping list by ``roles.grouping_layers`` for this kind; ``unit_layers``
    the sample keys a line kind draws one polyline each for
    (``GroupingLayers.units``). The rows arriving here are what the collapse
    chain left: the sample.
    """
    y_measure = spec.y_measure
    x_factor = x_layers[0] if x_layers else None

    if shape is Shape.MATRIX_2D:
        return reducer_for(table).matrix_mean(group, y_measure)

    out = pd.DataFrame(index=group.index)

    # --- x --------------------------------------------------------------
    if spec.x_measure is not None:
        out[X] = coerce_numeric(group[spec.x_measure])
    elif shape is Shape.SERIES_1D and index_column and index_column in group.columns:
        out[X] = coerce_numeric(group[index_column])
    elif len(x_layers) > 1:
        # Nested axis: one position per COMBINATION of the layers, identified
        # by a composed key. The layer values stay as their own columns too, so
        # `plan_x_axis` can order them and the renderers can label the groups.
        out[X] = _composed_key(group, x_layers, LEAF_SEPARATOR)
        for name in x_layers:
            out[name] = group[name].values
    elif x_factor:
        out[X] = group[x_factor].values
    else:
        # No x at all: a single categorical position, matching the proof of
        # concept's "Observation" fallback when no tick factor was chosen.
        # `_unlabelled_x_order` reads this back as the axis' one level.
        out[X] = UNLABELLED_X

    out[Y] = coerce_numeric(group[y_measure])

    if color:
        out[COLOR] = group[color].values

    # --- one polyline / band / point set per leaf group -------------------
    # The series identity is the grouping's series layers and nothing else:
    # a factor that separates figures or panels is constant here, and one
    # that is collapsed is gone. (Spaghetti's offsets are computed per figure,
    # so a subject must carry the same id in every facet — which this gives
    # for free, where the old "every non-x factor" key did not.)
    if spec.kind in (PlotKind.LINE, PlotKind.SPAGHETTI, PlotKind.BAND):
        # Units inside the grouping layers (`GroupingLayers.identity`): one
        # polyline per subject, within its group's colour and dash.
        out[SERIES] = _series_key(group, [*unit_layers, *series_layers])
    # A spaghetti polyline spans the innermost tick only: the brackets are
    # the run it stays inside (`resolved.RUN`; `render.base.series_runs`).
    if spec.kind is PlotKind.SPAGHETTI and len(x_layers) > 1:
        out[RUN] = _run_key(group, x_layers, [])
    # The uncoloured part of that identity gets a dash style (D4). Spaghetti
    # is left out: its lines are already one per level, joined by markers.
    if spec.kind in (PlotKind.LINE, PlotKind.BAND):
        uncoloured = [name for name in series_layers if name != color and name in group.columns]
        if uncoloured:
            out[DASH] = _series_key(group, uncoloured)

    out = out.dropna(subset=[Y])

    # --- summarize the sample into centre + error ------------------------
    if spec.kind in (PlotKind.BAR, PlotKind.BAND):
        # Carry the nested axis's LAYER columns through the summary. They are a
        # function of the composed key, so grouping by them as well changes no
        # group — but without them `_summarize`'s `reset_index` returns three
        # columns and the layer values are gone, and `_plan_nested_x` (which
        # requires every layer to be present in a panel) then skips every panel
        # and composes an EMPTY axis: no order, no ticks, no brackets.
        #
        # That was the whole nested-x feature silently absent for exactly one
        # kind — bar — which is the kind people group bars with. Every existing
        # nested test used box, which does not summarize, so nothing caught it.
        out = _summarize(
            out,
            spec,
            color,
            carry=[n for n in x_layers if n in out.columns],
            series=SERIES in out.columns,
        )

    if spec.kind is PlotKind.LINE or spec.kind is PlotKind.BAND:
        out = out.sort_values(X, kind="stable")

    return out.reset_index(drop=True)


def _series_key(frame: pd.DataFrame, series_layers: list[str]) -> "np.ndarray | str":
    """The series id, composed **outermost first** — ``"01 | 1"``, subject
    then trial — so it reads like a path in a hover and a legend. The layers
    arrive innermost-first (the spec's order); this is the one reversal, and
    ``codegen`` composes ``_series`` the same way."""
    present = [name for name in series_layers if name in frame.columns]
    if not present:
        return ""
    return _composed_key(frame, list(reversed(present)), SERIES_SEPARATOR)


def _composed_key(
    frame: pd.DataFrame, columns: list[str], separator: str
) -> np.ndarray:
    """One string per row, composed from ``columns`` — **per distinct
    combination, never per row**.

    This is the identity of a polyline (which replicate is this sample part of?)
    and of a nested-x leaf (which combination of layers is this tick?). Both are
    categorical facts about the row's FACTORS, and a 1-D measure explodes into
    hundreds of thousands of samples that all share them: a 24-row frame of EMG
    traces becomes 8.9 million rows carrying 24 distinct answers.

    The previous implementation asked each row: ``group[cols].astype(str).agg("
    | ".join, axis=1)`` is a Python call **per row**, and the interactive path
    never paid it because ``_downsample`` runs first (20 015 rows instead of
    8 906 400). At full resolution — the save path, and only the save path — one
    figure took ~770s (scidb.log 2026-09-11, 1543s for two).

    So: factorize each column (one C-level hash pass), fold the codes together
    into a dense combination id, build the text ONCE per distinct combination,
    and take. The Python loop below runs over combinations — 24 of them, not 8.9
    million — and everything touching every row is a numpy/pandas primitive.

    The strings are identical to the join it replaces — by construction, since
    the text still comes from pandas' own ``astype(str)``, just applied to the
    levels. The one deliberate difference is a **missing** factor value, which
    the old form could not survive at all under pandas 3 (see below): here it is
    a level like any other (``use_na_sentinel=False``, rather than a -1 sentinel
    that would index the text backwards), named ``MISSING_LEVEL_TEXT``.
    """
    rows = len(frame)
    if not columns:
        return np.full(rows, "", dtype=object)

    # Dense combination id per row, rebuilt column by column. Re-factorizing
    # after each fold keeps the id in [0, distinct_so_far) — without it the
    # composed key is a product of level counts and overflows int64 on a wide
    # enough frame.
    combined = np.zeros(rows, dtype=np.int64)
    labels: list[str] = [""]

    for position, column in enumerate(columns):
        # The Series, not `.to_numpy()`: pandas factorizes an arrow-backed
        # string column in place, where materializing it as objects would build
        # 8.9 million Python strings on the way to counting 24 of them.
        codes, uniques = pd.factorize(frame[column], use_na_sentinel=False)
        width = max(len(uniques), 1)
        combined, keys = pd.factorize(combined * width + codes)

        # `.astype(str)`, not `str(value)` — on the LEVELS rather than the rows.
        # Delegating to pandas is what keeps the text identical to the join this
        # replaces for every dtype it renders differently from Python (a numpy
        # scalar, an extension dtype).
        #
        # Except for missing values, where there is nothing to be identical to:
        # pandas 3's `astype(str)` PRESERVES NA rather than writing "nan", so the
        # join this replaces raised `TypeError: sequence item: expected str` on
        # any factor column with a gap in it. A missing level is a level here.
        text = list(pd.Index(uniques).astype(str).fillna(MISSING_LEVEL_TEXT))
        labels = [
            text[code]
            if position == 0
            else labels[previous] + separator + text[code]
            for previous, code in (divmod(int(key), width) for key in keys)
        ]

    return np.asarray(labels, dtype=object)[combined]


def _matrix_frame(group: pd.DataFrame, measure: str) -> pd.DataFrame:
    """One row holding the (possibly averaged) matrix for a heatmap panel."""
    matrices = [np.asarray(v, dtype=float) for v in group[measure] if _is_sequence(v)]
    if not matrices:
        return pd.DataFrame({Z: []})
    shapes = {m.shape for m in matrices}
    if len(shapes) > 1:
        Log.warn(
            "heatmap: %d matrices with differing shapes %s — using the first",
            len(matrices),
            sorted(shapes),
            layer=LAYER,
        )
        stacked = matrices[0]
    elif len(matrices) > 1:
        Log.debug("heatmap: averaging %d matrices", len(matrices), layer=LAYER)
        stacked = np.mean(np.stack(matrices), axis=0)
    else:
        stacked = matrices[0]
    return pd.DataFrame({Z: [stacked]})


def _summarize(
    frame: pd.DataFrame,
    spec: PlotSpec,
    color: str | None,
    carry: list[str] | None = None,
    series: bool = False,
) -> pd.DataFrame:
    """Collapse the sample rows at each mark into centre + error.

    A mark is an x position and — for a band — a series: the rows that share
    both are the sample (the outermost collapsed key's levels, after the
    inner collapses; or every pooled row). ``carry`` names columns to keep
    alongside the result — the nested axis's layer columns. They are
    functionally determined by ``__x`` (the composed leaf key IS their
    combination), so grouping by them splits nothing that was not already
    split; it only keeps them from being dropped by the ``reset_index`` below,
    which is what the renderers and ``_plan_nested_x`` read the group labels
    from.

    ``__color`` is carried the same way, and for the same reason: the
    coloured layer is one of the tick layers (colour is paint —
    ``roles.GroupingLayers``), so its level is a function of ``__x`` and
    grouping by it splits nothing. It is NOT a partition key of its own;
    toggling the colour tag must leave every centre and spread untouched
    (``test_colour_is_paint.py``).

    With no sample (nothing collapsed) every group holds one row: the centre
    is the value and the spread is zero — a bar with no error bar.
    """
    group_cols = [X, *(carry or [])]
    if color and COLOR in frame.columns:
        group_cols.append(COLOR)
    if series and SERIES in frame.columns:
        group_cols.append(SERIES)
        if DASH in frame.columns:
            # A function of the series id — grouping by it splits nothing;
            # it only survives the reset_index for the renderers.
            group_cols.append(DASH)
    grouped = frame.groupby(group_cols, dropna=False, sort=False)[Y]

    statistic = spec.aggregate.statistic
    centre = grouped.median() if statistic is Statistic.MEDIAN else grouped.mean()

    # ONE definition of the error band, shared with the y limits
    # (`ylimits.spread_bounds`): two definitions would put the limits and the
    # drawing at odds, and the symptom is a band clipped by its own axis.
    low, high = spread_bounds(grouped, centre, spec)

    out = pd.concat(
        {Y: centre, Y_LOW: low, Y_HIGH: high}, axis=1
    ).reset_index()
    return out


def _summarize_exploded(
    frame: pd.DataFrame,
    spec: PlotSpec,
    color: str | None,
    index_column: str,
    series_layers: list[str] = (),
) -> pd.DataFrame:
    """A BAND/BAR panel frame from an exploded, pre-collapsed 1-D frame — the
    summary tail of ``_panel_frame`` at full resolution. The pandas reference
    for ``Reducer.summarize_series``."""
    if color is not None and color not in frame.columns:
        color = None
    out = pd.DataFrame(index=frame.index)
    out[X] = coerce_numeric(frame[index_column])
    out[Y] = coerce_numeric(frame[spec.y_measure])
    if color:
        out[COLOR] = frame[color].values
    if any(name in frame.columns for name in series_layers):
        out[SERIES] = _series_key(frame, series_layers)
        uncoloured = [name for name in series_layers if name != color and name in frame.columns]
        if uncoloured:
            out[DASH] = _series_key(frame, uncoloured)
    out = out.dropna(subset=[Y])
    out = _summarize(out, spec, color, series=SERIES in out.columns)
    if spec.kind is PlotKind.BAND:
        out = out.sort_values(X, kind="stable")
    return out.reset_index(drop=True)


def _downsample(
    frame: pd.DataFrame, max_points: int, index_column: str | None
) -> pd.DataFrame:
    """
    Reduce row count for transport.

    Striding (rather than random sampling) preserves the visual shape of 1-D
    traces, which is the case that actually gets big. The caller records the
    original size on the ResolvedPlot so the GUI can say so.
    """
    stride = max(1, len(frame) // max_points)
    reduced = frame.iloc[::stride]
    Log.warn(
        "downsampled %d row(s) to %d for transport (stride=%d)",
        len(frame),
        len(reduced),
        stride,
        layer=LAYER,
    )
    return reduced


# ---------------------------------------------------------------------------
# Ordering, encoding, labels
# ---------------------------------------------------------------------------


def _ordered_groups(
    frame: pd.DataFrame, columns: list[str], table: LongTable
) -> list[tuple[tuple, pd.DataFrame]]:
    """
    Group by ``columns`` in the factors' declared level order.

    Declared order matters: zero-padded schema keys ("01", "02", … "10") sort
    lexicographically into 1, 10, 2 under pandas' default, which is a visible
    bug on an axis and in a facet strip. ``LongTable`` carries the real order.
    """
    present = [c for c in columns if c in frame.columns]
    if not present:
        return [((), frame)]

    groups = {key: group for key, group in frame.groupby(present, dropna=False, sort=False)}
    ordered_keys = sorted(
        groups.keys(),
        key=lambda key: tuple(
            _level_rank(table, column, value)
            for column, value in zip(present, _as_tuple(key), strict=True)
        ),
    )
    return [(_as_tuple(key), groups[key]) for key in ordered_keys]


def _as_tuple(key: Any) -> tuple:
    return key if isinstance(key, tuple) else (key,)


def _level_rank(table: LongTable, column: str, value: Any) -> tuple:
    """Position of ``value`` in the factor's declared levels, else natural sort."""
    try:
        levels = table.factor(column).levels
    except KeyError:
        return (1,) + natural_sort_key(value)
    for position, level in enumerate(levels):
        if level == value or str(level) == str(value):
            return (0, position)
    return (1,) + natural_sort_key(value)


def _plan_nested_x(
    panels: list[Panel], table: LongTable, x_layers: list[str]
) -> XPlan:
    """Compose the nested axis from the combinations the panels actually hold.

    Observed combinations, never the Cartesian product: real designs are ragged
    — a sham group with no post session — and reserving a position for a
    combination nobody ran leaves a hole that reads as missing data.

    Taken across ALL panels so a faceted figure shares one axis; a panel missing
    a combination gets a gap in the same place as its neighbours rather than a
    differently-shaped axis.
    """
    combinations: list[tuple] = []
    for panel in panels:
        present = [name for name in x_layers if name in panel.frame.columns]
        if len(present) != len(x_layers) or panel.frame.empty:
            continue
        combinations.extend(
            panel.frame[x_layers].astype(str).drop_duplicates().itertuples(
                index=False, name=None
            )
        )
    layer_orders = [
        [str(level) for level in _factor_levels(table, name)] for name in x_layers
    ]
    return plan_x_axis(combinations, layer_orders)


def _factor_levels(table: LongTable, name: str) -> list[Any]:
    try:
        return table.factor(name).levels
    except KeyError:
        return []


def _unlabelled_x_order(panels: list[Panel]) -> list[Any] | None:
    """``[UNLABELLED_X]`` when the marks sit at the one unlabelled categorical
    position (no tick layer — nothing groups; since colour became paint the
    coloured layer is a tick too); ``None`` for a numeric x (a 1-D index, an
    x measure, a 2-D matrix). Read off the panel frames' ``__x`` so the
    answer is the one the frames were actually built with. Without this the axis had no level
    order at all and read as NUMERIC: plotly still drew the bars (it builds
    its own category axis from the ``""`` strings) but every "Show sample"
    overlay was silently dropped, and the matplotlib export placed the bars
    at NaN (scidb.log 2026-09-21: colour by ColName with ColName the only
    grouping layer).
    """
    for panel in panels:
        if X not in panel.frame.columns or panel.frame.empty:
            continue
        column = panel.frame[X]
        return [UNLABELLED_X] if bool((column.astype(str) == UNLABELLED_X).all()) else None
    return None


def _level_order(
    table: LongTable, column: str | None, frames: list[pd.DataFrame], frame_column: str
) -> list[Any] | None:
    """The levels of ``column`` present across ``frames`` (the panels' mark
    frames, or their sample frames), in the declared level order."""
    if not column:
        return None
    present: list[Any] = []
    for frame in frames:
        if frame_column in frame.columns:
            present.extend(frame[frame_column].dropna().unique().tolist())
    unique = list(dict.fromkeys(present))
    return sorted(unique, key=lambda v: _level_rank(table, column, v))


@dataclass(frozen=True)
class GridPlan:
    """Where each labelled panel sits, and how big the grid ended up."""

    #: (row, col) per input label, in the same order.
    cells: list[tuple[int, int]]
    n_rows: int
    n_cols: int
    row_labels: list[str]
    col_labels: list[str]
    notes: list[str]

    @property
    def fills_row_major(self) -> bool:
        """
        True when the occupied cells are a gapless left-to-right, top-to-bottom
        prefix of the grid — the panels may be in any ORDER, but there are no
        holes. That is exactly the case seaborn's ``col_wrap`` + ``col_order``
        can reproduce, so ``codegen`` asks before claiming the exported figure
        matches the preview.
        """
        return sorted(self.cells) == [
            divmod(index, self.n_cols) for index in range(len(self.cells))
        ]

    def labels_in_grid_order(self, labels: list[str]) -> list[str]:
        """``labels`` re-ordered the way the grid reads: row by row."""
        return [label for _, label in sorted(zip(self.cells, labels, strict=True))]


def plan_layout(labels: list[str], facet) -> GridPlan:
    """
    Decide the facet grid from panel labels alone — no data involved.

    Separate from :func:`_assign_grid` so the same placement can be replayed by
    ``codegen`` (to emit a matching ``col_order``) and asserted in tests without
    building frames. The rules below are the whole layout contract.

    The grid is ``n_rows x n_cols`` (see ``spec.grid_shape_for``: naming one
    dimension computes the other). Each row and column slot may carry a matcher
    that claims the panels whose label it matches — which is what makes a layout
    describable ("left column = names starting with L") and therefore reusable
    across variables, rather than a hand-arrangement of one figure. Blank slots
    take whatever is left over, in resolution order.

    Two invariants, both of them things the user has been bitten by:

    * **No cell is ever claimed twice.** Every placement goes through
      ``occupied``; a panel whose ruled cell is taken spills to the next free
      cell (growing the grid if it must) and says so in the returned notes.
      Two panels in one cell means two plotly axis pairs with an identical
      domain, i.e. traces drawn on top of each other.
    * **Nothing is dropped.** A panel matching nothing is free to take any
      remaining cell, and the grid grows a trailing "other" row/column if there
      is none. Silently losing a muscle because a pattern had a typo is the
      worst possible failure here.

    """
    if not labels:
        return GridPlan(cells=[], n_rows=1, n_cols=1, row_labels=[], col_labels=[], notes=[])
    # One panel is always a 1 x 1 grid: nothing separates panels, or the
    # factors that do have one level between them. A pinned N rows/cols or
    # written slots would otherwise lay the one plot out in a grid of empty
    # cells (user, 2026-09-26). The pins stay in the spec, so a second panel
    # brings the layout back.
    if len(labels) == 1:
        if facet.n_rows or facet.n_cols or facet.rows or facet.cols:
            Log.debug(
                "facet grid 1x1 for a single panel %r: ignoring n_rows=%s n_cols=%s, "
                "%d row slot(s), %d column slot(s)",
                labels[0],
                facet.n_rows,
                facet.n_cols,
                len(facet.rows),
                len(facet.cols),
                layer=LAYER,
            )
        return GridPlan(
            cells=[(0, 0)], n_rows=1, n_cols=1, row_labels=[], col_labels=[], notes=[]
        )

    # Slot POSITION is meaningful, so the blanks stay in the list: rules[1] is
    # row 2 whether or not row 1 was filled in. Blank matchers never match (see
    # Matcher.is_blank), so an unset slot claims nothing and simply receives
    # whatever is still unplaced.
    row_slots, col_slots = list(facet.rows), list(facet.cols)
    notes: list[str] = []

    n_rows, n_cols = grid_shape_for(len(labels), facet.n_rows, facet.n_cols)
    # A pinned dimension wins over leftover slots: shrinking a 4-row grid to 2
    # must not be undone by the two rules the user had already written into
    # rows 3 and 4. They stay in the spec (re-widening brings them back) but
    # they are not rows. An unpinned dimension does the opposite — it widens to
    # hold every slot that was written.
    if facet.n_rows:
        row_slots = row_slots[:n_rows]
    else:
        n_rows = max(n_rows, len(row_slots))
    if facet.n_cols:
        col_slots = col_slots[:n_cols]
    else:
        n_cols = max(n_cols, len(col_slots))

    has_row_rules = any(not m.is_blank for m in row_slots)
    has_col_rules = any(not m.is_blank for m in col_slots)
    row_slot = [_match_index(row_slots, label) for label in labels]
    col_slot = [_match_index(col_slots, label) for label in labels]

    occupied: dict[tuple[int, int], str] = {}
    cells: list[tuple[int, int] | None] = [None] * len(labels)
    spilled: list[int] = []

    def claim(index: int, row: int, col: int) -> bool:
        if (row, col) in occupied:
            return False
        occupied[(row, col)] = labels[index]
        cells[index] = (row, col)
        return True

    # Pass A: both axes ruled — the panel has an exact address.
    for index, (row, col) in enumerate(zip(row_slot, col_slot, strict=True)):
        if row is None or col is None:
            continue
        if not claim(index, row, col):
            spilled.append(index)
            notes.append(
                f"{labels[index]!r} and {occupied[(row, col)]!r} both match row "
                f"{row + 1} and column {col + 1}; {labels[index]!r} was moved to "
                f"the next free cell."
            )

    # Pass B: one axis ruled — first FREE cell along the other, so panels stack
    # down a ruled column / flow across a ruled row without ever colliding.
    for index, (row, col) in enumerate(zip(row_slot, col_slot, strict=True)):
        if (row is None) == (col is None):
            continue
        if row is None:
            free = next((r for r in range(n_rows) if (r, col) not in occupied), None)
            placed = free is not None and claim(index, free, col)
        else:
            free = next((c for c in range(n_cols) if (row, c) not in occupied), None)
            placed = free is not None and claim(index, row, free)
        if not placed:
            spilled.append(index)
            axis = "column" if row is None else "row"
            notes.append(
                f"{labels[index]!r} matches a {axis} rule but that {axis} is "
                f"full; it was moved to the next free cell."
            )

    # Pass C: unconstrained panels fill what is left, in resolution order. With
    # no rules at all this is the plain wrapped flow.
    free_cells = (
        (r, c) for r in range(n_rows) for c in range(n_cols) if (r, c) not in occupied
    )
    for index, (row, col) in enumerate(zip(row_slot, col_slot, strict=True)):
        if row is not None or col is not None:
            continue
        cell = next(free_cells, None)
        if cell is None:
            spilled.append(index)
        else:
            claim(index, *cell)

    # Pass D: everything that could not take its own cell. The grid grows rather
    # than letting two panels share one.
    for index in spilled:
        row, col = _first_free_cell(occupied, n_rows, n_cols)
        if row >= n_rows:
            n_rows = row + 1
            notes.append(
                f"The grid grew to {n_rows} rows so that {labels[index]!r} could "
                f"have a cell of its own."
            )
        claim(index, row, col)

    # Shrink a dimension the user did NOT pin down to what the panels actually
    # used: "2 rows of muscles" should not leave a trailing empty column just
    # because the starting estimate was wider. A pinned dimension is honoured
    # as given — the user asked for that much room.
    placed = [cell for cell in cells if cell is not None]
    if facet.n_rows is None and placed:
        n_rows = max(len(row_slots), max(row for row, _ in placed) + 1)
    if facet.n_cols is None and placed:
        n_cols = max(len(col_slots), max(col for _, col in placed) + 1)

    # Anything past the declared slots holds panels no rule claimed. Label it,
    # so a typo in a pattern shows up as an "other" column rather than as a
    # muscle mysteriously sitting on the end.
    row_labels = _rule_labels(row_slots)
    col_labels = _rule_labels(col_slots)
    if has_row_rules and n_rows > len(row_slots):
        row_labels += ["other"] * (n_rows - len(row_slots))
        notes.append("Some panels matched no row rule — see the 'other' row(s).")
    if has_col_rules and n_cols > len(col_slots):
        col_labels += ["other"] * (n_cols - len(col_slots))
        notes.append("Some panels matched no column rule — see the 'other' column(s).")

    return GridPlan(
        # Every pass above ends by claiming a cell, so None is unreachable —
        # but a panel with no cell would be a panel the renderer never draws.
        cells=[(0, 0) if cell is None else cell for cell in cells],
        n_rows=n_rows,
        n_cols=n_cols,
        row_labels=row_labels,
        col_labels=col_labels,
        notes=notes,
    )


def _assign_grid(panels, facet) -> tuple[int, int, list[str], list[str], list[str]]:
    """
    Place every panel in the grid, and report its shape.

    Thin wrapper over :func:`plan_layout` — the placement decision is made from
    panel labels alone so that ``codegen`` can replay it, and applied to the
    Panel objects here.

    Returns ``(n_rows, n_cols, row_labels, col_labels, notes)``.
    """
    plan = plan_layout([panel.title for panel in panels], facet)
    for panel, (row, col) in zip(panels, plan.cells, strict=True):
        panel.grid_row, panel.grid_col = row, col

    for note in plan.notes:
        Log.warn("facet layout: %s", note, layer=LAYER)
    Log.debug(
        "facet grid %dx%d from %d row rule(s), %d column rule(s): %s",
        plan.n_rows,
        plan.n_cols,
        len(facet.row_rules),
        len(facet.col_rules),  # non-blank only: the rules the user actually wrote
        ", ".join(f"{p.title or '?'}@({p.grid_row},{p.grid_col})" for p in panels),
        layer=LAYER,
    )
    return plan.n_rows, plan.n_cols, plan.row_labels, plan.col_labels, plan.notes


def _first_free_cell(
    occupied: dict[tuple[int, int], str], n_rows: int, n_cols: int
) -> tuple[int, int]:
    """
    First unoccupied cell in row-major order, growing past the last row when the
    grid is full — the caller widens the grid rather than overlap two panels.
    """
    for row in range(n_rows):
        for col in range(n_cols):
            if (row, col) not in occupied:
                return row, col
    return n_rows, 0


def _match_index(rules: list[Matcher], label: str) -> int | None:
    """First rule that matches, or None. First match wins — order is the tie-break."""
    for index, rule in enumerate(rules):
        if rule.matches(label):
            return index
    return None


def _rule_labels(rules: list[Matcher]) -> list[str]:
    return [rule.display for rule in rules]


def _encoding_for(
    kind: PlotKind,
    color: str | None,
    shape: Shape,
    has_series: bool = False,
    has_dash: bool = False,
) -> Encoding:
    if shape is Shape.MATRIX_2D:
        return Encoding(x=None, y=None, z=Z)
    # A band carries a series column whenever an uncoloured grouping layer
    # splits it (one band per leaf group); a line or spaghetti always does.
    series = kind in (PlotKind.LINE, PlotKind.SPAGHETTI) or (
        kind is PlotKind.BAND and has_series
    )
    return Encoding(
        x=X,
        y=Y,
        color=COLOR if color else None,
        y_low=Y_LOW if kind in (PlotKind.BAR, PlotKind.BAND) else None,
        y_high=Y_HIGH if kind in (PlotKind.BAR, PlotKind.BAND) else None,
        series=SERIES if series else None,
        dash=DASH if has_dash else None,
    )


def x_axis_title(
    spec: PlotSpec,
    table: LongTable,
    x_layers: list[str],
    index_column: str | None,
    text: DisplayText | None = None,
) -> str:
    """The x axis title — the ONE owner, read by both renderers (through
    ``Labels.x``) and by ``codegen``.

    A **nested** categorical axis gets none. Its tick labels and bracket
    rows already name every level, so "session / InterventionGroup" only
    repeated what sits directly above and below it, and in a saved figure it
    collided with the bracket row (user, 2026-09-23; spec/images/graph1.png).
    A single-layer axis keeps its factor's name: its tick labels (``BL``,
    ``MID24``) do not say what they are. ``StyleOptions.x_label`` always wins;
    otherwise a name alias (``aliases``) does. ``text`` is the figure's merged
    aliases; without one (``codegen``) they are merged here.
    """
    if spec.style.x_label:
        return spec.style.x_label
    if spec.x_measure is None and len(x_layers) > 1:
        return ""
    if text is None:
        text = display_text(spec, table)
    if spec.x_measure:
        return text.name(spec.x_measure, table.measure(spec.x_measure).display)
    if x_layers:
        return text.name(x_layers[0], table.factor(x_layers[0]).display)
    return index_column or ""


def _labels_for(
    spec: PlotSpec,
    table: LongTable,
    x_layers: list[str],
    color: str | None,
    index_column: str | None,
    figure_key: dict[str, Any],
    dash_layers: list[str] = (),
    sample_color: str | None = None,
    text: DisplayText | None = None,
) -> Labels:
    """Every title the figure draws, with name and level aliases applied
    (``text``: the figure's :class:`aliases.DisplayText`)."""
    style = spec.style
    text = text if text is not None else display_text(spec, table)
    x_label = x_axis_title(spec, table, x_layers, index_column, text)

    y_label = style.y_label or text.name(spec.y_measure, table.measure(spec.y_measure).display)

    title = style.title
    if title is None and figure_key:
        title = text.figure_title(figure_key)

    def named(factor: str) -> str:
        return text.name(factor, table.factor(factor).display)

    return Labels(
        x=x_label,
        y=y_label,
        color=named(color) if color else None,
        # Outermost first, as the dash ids themselves are composed.
        dash=(" / ".join(named(name) for name in reversed(list(dash_layers))) or None),
        sample=named(sample_color) if sample_color else None,
        title=title,
    )


def _figure_limits(panels: list[Panel]) -> tuple[float, float] | None:
    """The figure's y limits, or None when its panels do not share one range.

    Read off the panels rather than recomputed, so the figure-level number and
    the panel-level ones cannot disagree. ``None`` is meaningful: it is how a
    renderer learns it must give each panel its own axis instead of sharing one
    (``render.base.shares_y_axis``).
    """
    if not panels:
        return None
    first = panels[0].y_limits
    if first is None:
        return None
    return first if all(panel.y_limits == first for panel in panels) else None


def unique_values(frame: pd.DataFrame, column: str) -> list[Any]:
    """Distinct values of a column, natural-sorted. Used by sources for levels."""
    if column not in frame.columns:
        return []
    return sorted(frame[column].dropna().unique().tolist(), key=natural_sort_key)


def iter_columns(names: Iterable[str]) -> list[str]:
    return [n for n in names if n]
