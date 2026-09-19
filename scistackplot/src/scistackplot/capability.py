"""
Which plot kinds are available, and which one to pick by default.

This is the single rule behind two of the requirements that look separate:
"different data types get different default plots", and "collapsing a factor
unlocks the summative plot types". Both fall out of one observation — a
distribution needs a SAMPLE, and a sample exists only when some factor is
collapsed: the outermost collapsed key's levels, after every inner collapse,
are what a box, a violin or an error band is drawn over
(docs/claude/grouping-and-collapse.md).

The GUI must render only what ``available_plots`` returns. Plot policy lives
here, not in TypeScript (CLAUDE.md NOTE 3).
"""

from __future__ import annotations

from dataclasses import replace

from .roles import (
    collapse_order,
    complete_assignment,
    grouping_layers,
    has_sample,
    overlay_granularity,
    overlay_join,
    overlay_steps,
    overlay_unavailable,
    kind_requirement,
    role_for_new_grouping,
    roles_for_kind,
    sample_key,
)
from .shape import Shape
from .spec import MAX_X_LAYERS, SCALAR_KINDS, PlotKind, PlotSpec, Role
from .table import CODE_FACTOR_PREFIX, RUN_FACTOR_PREFIX, LongTable

#: Kinds that summarize several rows per x position into one mark.
DISTRIBUTION_KINDS = (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR, PlotKind.BAND)

#: Kinds that cannot be drawn without a sample — see ``roles.kind_requirement``,
#: which is the one statement of what each kind needs. BAR is deliberately NOT
#: here: a bar of single values (no error bar) is a legitimate figure, and the
#: opening state of a scalar measure.
SAMPLE_KINDS = (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAND)

#: The order the Factors dropdown lists roles in: the same verb at decreasing
#: granularity — figure, panel — then the one option that removes levels
#: instead of placing them. GROUP is absent on purpose: a factor is moved into
#: the Grouping section, not assigned "group" from a dropdown (see
#: ``factors_menu``).
ROLE_ORDER: tuple[Role, ...] = (
    Role.ITERATE,
    Role.FACET,
    Role.COLLAPSE,
)

#: What each role is CALLED. The role names are the library's vocabulary;
#: these are the user's — reported by the backend rather than hardcoded in the
#: panel so the words and the behaviour stay together (CLAUDE.md NOTE 3).
#: "Separate figures" / "Separate panels" read as a family with the Grouping
#: section's "one bar per…"; "Collapse" says what happens to the levels.
_ROLE_LABELS: dict[Role, str] = {
    Role.GROUP: "Group",
    Role.FACET: "Separate panels",
    Role.ITERATE: "Separate figures",
    Role.COLLAPSE: "Collapse (average)",
}

_ROLE_HINTS: dict[Role, str] = {
    Role.GROUP: "One mark per level — set in the Grouping section",
    Role.FACET: "One subplot per level — arrange them under Layout",
    Role.ITERATE: "One whole figure per level",
    Role.COLLAPSE: "Averaged away. The LAST collapsed key (outermost) is the "
    "sample: its levels are what error bars, boxes and bands are drawn over",
}

#: Only the entries that differ from the defaults need listing. A user looking
#: at 1-D data is looking at traces, so the collapse is described as one.
_ROLE_HINTS_BY_SHAPE: dict[Shape, dict[Role, str]] = {
    Shape.SERIES_1D: {
        Role.COLLAPSE: "Traces averaged together sample by sample. The LAST "
        "collapsed key (outermost) is the sample the error band is drawn over",
    },
}

#: What the Grouping section's list MEANS for each kind, in the words the
#: figure shows. The list is the user's (innermost first); the reading is the
#: kind's (``roles.grouping_layers``); this is the reading, said out loud.
_GROUPING_HINTS: dict[PlotKind, str] = {
    PlotKind.BAR: "One bar per combination — first entry innermost",
    PlotKind.BOX: "One box per combination — first entry innermost",
    PlotKind.VIOLIN: "One violin per combination — first entry innermost",
    PlotKind.STRIP: "One point per combination — first entry innermost",
    PlotKind.SCATTER: "One point per combination — first entry innermost",
    PlotKind.SPAGHETTI: "First entry = the lines (one per level); the rest are "
    "the x positions each line joins across",
    PlotKind.LINE: "One line per combination",
    PlotKind.BAND: "One band per combination",
    PlotKind.HEATMAP: "A heatmap cannot be grouped — separate panels or figures",
}


def role_label(role: Role, shape: Shape) -> str:
    """What to call this role for a measure of this shape."""
    return _ROLE_LABELS[role]


def role_hint(role: Role, shape: Shape) -> str:
    """The one-line explanation under the label."""
    return _ROLE_HINTS_BY_SHAPE.get(shape, {}).get(role) or _ROLE_HINTS[role]


def grouping_hint(kind: PlotKind, shape: Shape, has_x_measure: bool = False) -> str:
    """What the grouping list means for this kind, for the section header."""
    if shape is Shape.MATRIX_2D:
        return _GROUPING_HINTS[PlotKind.HEATMAP]
    if has_x_measure and kind is not PlotKind.LINE:
        return "One point set per combination (x comes from the x measure)"
    if shape is Shape.SERIES_1D and kind in (PlotKind.LINE, PlotKind.BAND):
        return _GROUPING_HINTS[kind]
    return _GROUPING_HINTS.get(kind, "One mark per combination — first entry innermost")


def _validation_error(spec: PlotSpec, table: LongTable) -> str | None:
    """``validate``'s complaint about this spec, or None if it has none."""
    from .roles import RoleError, validate

    try:
        validate(spec, table)
    except RoleError as exc:
        return str(exc)
    return None


def _candidate(spec: PlotSpec, factor: str, role: Role) -> PlotSpec:
    """``spec`` with ``factor`` moved to ``role`` — and the colour dropped if
    the factor was the coloured layer and is leaving the grouping, so the
    candidate is judged on the role and not on a colour that no longer names
    a layer."""
    color = spec.color
    if role is not Role.GROUP and color == factor:
        color = None
    return replace(spec, roles={**spec.roles, factor: role}, color=color)


def role_options(
    spec: PlotSpec,
    table: LongTable,
    factor: str,
    *,
    roles: "tuple[Role, ...] | None" = None,
) -> list[dict]:
    """Every role for one factor: its label, whether it is legal, and why not.

    **Derived by asking** :func:`~scistackplot.roles.validate`, one candidate
    spec per role, rather than by restating its rules. That is the whole design:
    the panel used to offer every role unconditionally while ``validate``
    refused several of them, so a user could pick an option and be told it was
    impossible. Any rule expressed here in parallel would be a second copy free
    to drift from the one that actually decides. Asking cannot drift, and a
    new rule in ``validate`` reaches the panel for free.

    Cheap enough to do per factor: ``validate`` compares names, shapes and
    roles and never touches the frame.

    A spec that is ALREADY invalid for an unrelated reason (the panel shows
    that as its own error) must not make every role look forbidden, so a role
    counts as unavailable only when it fails for a reason the spec does not
    already have.

    ``roles`` limits which to report; the default is all of them in
    :data:`ROLE_ORDER`.
    """
    shape = table.shape_of(spec.y_measure)
    base_error = _validation_error(spec, table)

    options = []
    for role in roles if roles is not None else ROLE_ORDER:
        error = _validation_error(_candidate(spec, factor, role), table)
        blocked = error is not None and error != base_error
        options.append(
            {
                "role": str(role),
                "label": role_label(role, shape),
                "hint": role_hint(role, shape),
                "available": not blocked,
                # validate's own message, which always names the one-line fix.
                "reason": error if blocked else None,
            }
        )
    return options


def available_plots(
    shape: Shape,
    roles: dict[str, Role],
    *,
    has_x_measure: bool = False,
    collapsible: bool = False,
    n_groups: int = 0,
) -> list[PlotKind]:
    """Plot kinds that can be rendered for this shape and role assignment.

    ``collapsible`` says a 1-D measure may be reduced to one value per record
    first (:mod:`scistackplot.cell`), which makes the scalar kinds selectable
    for it — picking one IS the request to collapse the cells.

    ``shape`` must then be the **raw** shape, not the effective one. Computing
    this from a table whose measure has already been cell-collapsed would drop
    LINE and BAND the moment a violin was selected, leaving no way back to a
    line.

    ``n_groups`` is how many grouping layers the spec has — spaghetti needs
    two (the lines, and the positions they join across).
    """
    if shape is Shape.MATRIX_2D:
        return [PlotKind.HEATMAP]

    if has_x_measure:
        # x comes from a second measure: a relational scatter, optionally with
        # a connecting line when the x measure is ordered.
        return [PlotKind.SCATTER, PlotKind.LINE]

    def scalar_kinds() -> list[PlotKind]:
        return [
            kind
            for kind in (
                PlotKind.SCATTER,
                PlotKind.STRIP,
                PlotKind.BAR,
                PlotKind.SPAGHETTI,
                PlotKind.BOX,
                PlotKind.VIOLIN,
            )
            if kind_requirement(kind, Shape.SCALAR, roles, n_groups) is None
        ]

    if shape is Shape.SERIES_1D:
        kinds = [PlotKind.LINE]
        if kind_requirement(PlotKind.BAND, shape, roles, n_groups) is None:
            kinds.append(PlotKind.BAND)
        # The union, not a switch: the 1-D kinds draw the samples and the
        # scalar ones draw a summary of them, and both are legitimate views of
        # the same variable at the same time.
        if collapsible:
            kinds.extend(scalar_kinds())
        return kinds

    if shape is Shape.SCALAR:
        return scalar_kinds()

    return []


def default_plot(
    shape: Shape,
    roles: dict[str, Role],
    *,
    has_x_measure: bool = False,
    n_groups: int = 0,
) -> PlotKind | None:
    """
    The kind to select when a table is first opened.

    scalar → scatter, or box once there is a sample to distribute;
    1-D → one line per observation, or a mean line with a shaded error region
    once there is a sample; 2-D → heatmap.
    """
    kinds = available_plots(shape, roles, has_x_measure=has_x_measure, n_groups=n_groups)
    if not kinds:
        return None

    sample = has_sample(roles)
    if has_x_measure:
        return PlotKind.SCATTER
    if shape is Shape.SERIES_1D:
        return PlotKind.BAND if sample else PlotKind.LINE
    if shape is Shape.SCALAR:
        return PlotKind.BOX if sample else PlotKind.SCATTER
    return kinds[0]


def why_unavailable(
    kind: PlotKind,
    shape: Shape,
    roles: dict[str, Role],
    *,
    collapsible: bool = False,
    n_groups: int = 0,
) -> str | None:
    """
    Explain a kind's absence, for GUI tooltips on disabled options.

    Returns None when the kind IS available. ``collapsible`` and ``n_groups``
    carry the same meaning as in :func:`available_plots` and must be passed
    the same way, or a kind the panel offers would come with a reason it is
    refused.
    """
    if kind in available_plots(shape, roles, collapsible=collapsible, n_groups=n_groups):
        return None
    if shape is Shape.MATRIX_2D:
        return "2-D measures render as a heatmap."
    # Shape mismatches first: a scalar band is refused for being a band on a
    # scalar, not for lacking a sample it could never use.
    if kind is PlotKind.BAND and shape is not Shape.SERIES_1D:
        return "Error bands apply to 1-D measures."
    if kind is PlotKind.LINE and shape is Shape.SCALAR:
        return "Lines need a 1-D measure or a second measure for the x axis."
    requirement = kind_requirement(kind, shape, roles, n_groups)
    if requirement is not None:
        return requirement
    return f"Not available for a {shape} measure."


def capabilities(spec: PlotSpec, table: LongTable) -> dict:
    """
    The full JSON-serializable capability report for the GUI.

    One call gives the panel everything it needs to render its controls:
    which kinds are selectable, why the others are not, and what the default
    would be for the current role assignment.
    """
    from .cell import apply_cell_collapse, cell_collapses
    from .groups import apply_level_groups
    from .variants import apply_variant_sets, strip_answered_roles

    # The kinds and roles reported must be the ones the figure will actually be
    # built with, so the derived table (named variants folded into a ``Variant``
    # factor) is what they are computed against — exactly as ``resolve`` does,
    # stale-role drop included, or the panel would offer a role selector for a
    # factor the render is about to reject.
    derived = apply_level_groups(spec, apply_variant_sets(spec, table))
    spec = strip_answered_roles(spec, table, derived)

    # TWO tables, deliberately. ``derived`` still holds the measure as the data
    # holds it; ``collapsed`` holds it as the figure draws it. Everything about
    # the CURRENT figure — roles, factors, grouping, the reported shape — is
    # computed from ``collapsed``, and only the KIND LIST is computed from the
    # raw shape. Computing the kind list from ``collapsed`` would drop LINE and
    # BAND as soon as a violin was selected, stranding the user on the scalar
    # kinds with no way back (docs/claude/measure-shape-and-collapse.md).
    collapsed = apply_cell_collapse(spec, derived)
    assignment = complete_assignment(spec, collapsed)
    roles = assignment.roles
    raw_shape = derived.shape_of(spec.y_measure)
    shape = collapsed.shape_of(spec.y_measure)
    collapsing = cell_collapses(spec, derived)
    collapsible = raw_shape is Shape.SERIES_1D and spec.x_measure is None
    has_x_measure = spec.x_measure is not None

    # Each kind is judged against THE ASSIGNMENT IT WOULD BE APPLIED WITH,
    # which is its own suggestion when it has one and the current one
    # otherwise.
    #
    # Without this the re-roll is unreachable from the state it was written for.
    # A 1-D measure opens with every key iterated, so there is no sample, so
    # box/violin are refused — and the user cannot click the kind whose
    # selection would have supplied the sample. Judging a kind by the
    # assignment it brings with it closes that loop: clicking Violin both
    # collapses a factor and draws the distribution, in one click.
    entries = []
    for kind in PlotKind:
        suggested = roles_for_kind(spec, derived, kind)
        kind_roles = suggested.roles if suggested else roles
        kind_groups = suggested.groups if suggested else assignment.groups
        allowed_here = available_plots(
            raw_shape,
            kind_roles,
            has_x_measure=has_x_measure,
            collapsible=collapsible,
            n_groups=len(kind_groups),
        )
        entries.append(
            {
                "kind": str(kind),
                "available": kind in allowed_here,
                "reason": why_unavailable(
                    kind,
                    raw_shape,
                    kind_roles,
                    collapsible=collapsible,
                    n_groups=len(kind_groups),
                ),
                # Whether picking this kind collapses the measure's CELLS, so
                # the panel can say so on the option rather than only after
                # the click.
                "collapses": collapsible and kind in SCALAR_KINDS,
                # The assignment this kind would open with, when selecting it
                # should re-default it (``roles_for_kind``). Carried on the
                # option so the GUI applies it SYNCHRONOUSLY with the click: an
                # RPC would race the panel's resolve queue and could land after
                # the user had set a role, overwriting the one decision the
                # rule promises never to touch.
                "assignment": None if suggested is None else suggested.to_dict(),
            }
        )
    # Derived from the entries rather than computed a second time: two lists
    # that could disagree about one kind is the bug, not the saving.
    allowed = [PlotKind(entry["kind"]) for entry in entries if entry["available"]]

    return {
        # What the FIGURE is — scalar while a cell collapse is in effect. The
        # panel keys its scalar-only controls (the measure range filter) off
        # this, and they apply to the collapsed value, which is the only thing
        # they could mean.
        "shape": str(shape),
        # What the DATA is. The two differ only during a cell collapse, and the
        # badge shows both so a figure never silently claims to be drawing
        # samples.
        "raw_shape": str(raw_shape),
        "cell_collapse": {
            # Whether this variable can be cell-collapsed at all — i.e. whether
            # the statistic dropdown has anything to control.
            "applies": collapsible,
            "active": collapsing,
            "statistic": str(spec.cell_statistic),
        },
        # The collapse chain as the figure will run it, and its sample — so
        # the panel can say "error bars: SD across subject" next to the
        # statistic controls instead of leaving the user to work it out.
        "collapse": {
            "order": collapse_order(roles, collapsed),
            "sample": sample_key(roles, collapsed),
            "pooled": spec.aggregate.pooled,
        },
        "has_sample": has_sample(roles),
        # "Show sample": what the checkboxes can offer (the collapsed keys, in
        # chain order, deepest first), what is ticked and what the ticks
        # imply, the sentence saying what one point is, and the join
        # decision with its reason — all decided in `roles`, so the panel
        # displays and never re-derives.
        "sample_overlay": sample_overlay_summary(spec, roles, collapsed, shape),
        "default": str(
            default_plot(
                shape, roles, has_x_measure=has_x_measure, n_groups=len(assignment.groups)
            )
            or ""
        ),
        "available": [str(k) for k in allowed],
        "kinds": entries,
        "roles": {name: str(role) for name, role in roles.items()},
        "factors": factor_summary(spec, collapsed),
        "grouping": grouping_summary(spec, collapsed),
        "variants": variant_summary(spec, table),
    }


def sample_overlay_summary(
    spec: PlotSpec, roles: dict[str, Role], table: LongTable, shape: Shape
) -> dict:
    """The "Show sample" section of the capability report.

    ``factors`` lists every collapsed key in collapse order (deepest first —
    the order the checkboxes read best in, raw data at the top), each with
    ``checked`` (named in ``spec.show_sample``) and ``shown`` (drawn as part
    of a point's identity, which a deeper tick implies for every shallower
    key). ``ignored`` are ticked names that are not collapsed right now.
    """
    reason = overlay_unavailable(spec, roles, shape)
    order = collapse_order(roles, table)
    steps = overlay_steps(spec, roles, table) if reason is None else None
    join = overlay_join(spec, roles, table, steps) if steps is not None else None
    shown = set(steps.shown) if steps is not None else set()
    return {
        "available": reason is None,
        "reason": reason,
        "factors": [
            {
                "name": name,
                "checked": name in spec.show_sample,
                "shown": name in shown,
            }
            for name in order
        ],
        "ignored": [
            name for name in spec.show_sample if name not in order and table.has_factor(name)
        ],
        "shown": list(steps.shown) if steps is not None else [],
        "averaged": list(steps.averaged) if steps is not None else [],
        "join": {
            "join": join.join if join is not None else False,
            "automatic": join.automatic if join is not None else True,
            "reason": join.reason if join is not None else "",
            "setting": spec.join_sample,
        },
        "granularity": overlay_granularity(steps, join) if steps is not None else "",
    }


def factors_menu(spec: PlotSpec, table: LongTable, factor: str) -> list[dict]:
    """What the **Factors** dropdown lists for one factor.

    :func:`role_options` over :data:`ROLE_ORDER`, plus GROUP **only when this
    factor already holds it**. A ``<select>`` whose value is not among its
    options renders blank, and a factor the Grouping section holds still
    appears in this list (greyed, with the reason) so the control can display
    its own value — while *setting* GROUP belongs to the Grouping section.
    Keeping it selectable in both places is how the two controls would start
    disagreeing.
    """
    roles = (
        (Role.GROUP, *ROLE_ORDER) if spec.roles.get(factor) is Role.GROUP else ROLE_ORDER
    )
    options = role_options(spec, table, factor, roles=roles)
    for option in options:
        if option["role"] == str(Role.GROUP):
            option["available"] = False
            option["reason"] = (
                "This factor groups the marks — it is managed in the Grouping "
                "section; remove it there to give it another role."
            )
    return options


def grouping_summary(spec: PlotSpec, table: LongTable) -> dict:
    """The Grouping section's whole data model.

    ``layers`` is the user's list, innermost first, membership and order
    reconciled the same way the figure does it (``PlotSpec.ordered_groups``)
    so the control cannot show an order the renderer disagrees with.
    ``ticks`` / ``series`` are how the current kind READS that list
    (``roles.grouping_layers``: nested x ticks, outermost first, or series
    ids), and ``hint`` says so in words. ``labelled_layers`` counts against
    ``max_labelled_layers`` — the tick layers minus the coloured one.
    """
    shape = table.shape_of(spec.y_measure)
    assignment = complete_assignment(spec, table)
    layers = grouping_layers(spec, table, assignment.roles)
    reason = grouping_refusal(spec, table)
    return {
        "available": reason is None,
        "reason": reason,
        "layers": list(assignment.groups),
        "color": assignment.color,
        "ticks": layers.ticks,
        "series": layers.series,
        "labelled_layers": len(layers.labelled_ticks),
        "max_labelled_layers": MAX_X_LAYERS,
        "hint": grouping_hint(spec.kind, shape, spec.x_measure is not None),
        # The role a grouping ticked RIGHT NOW would take, so the panel applies
        # one rule rather than inventing a second (`roles.role_for_new_grouping`
        # says why). It does not depend on which grouping: the factor is not in
        # the table yet, so the answer is a property of the spec — which is why
        # it is published once here instead of costing a round trip per
        # checkbox.
        "new_grouping_role": str(role_for_new_grouping(spec, table)),
    }


def grouping_refusal(spec: PlotSpec, table: LongTable) -> str | None:
    """Why factors may not group this measure at all, or None when they may.

    Only a 2-D measure refuses: a heatmap's axes come from the matrix. A 1-D
    measure and an x-y plot group happily — each layer is a series rather than
    a tick, which ``grouping_summary.hint`` says.
    """
    shape = table.shape_of(spec.y_measure)
    if shape is Shape.MATRIX_2D:
        return "This measure is 2-D: a heatmap's axes come from the matrix."
    if shape not in (Shape.SCALAR, Shape.SERIES_1D):
        return f"Grouping needs a scalar or 1-D measure; this one is {shape}."
    return None


def factor_summary(spec: PlotSpec, derived: LongTable) -> list[dict]:
    """Every factor the panel renders, plus which of its levels survive filters.

    ``levels`` is what the table holds; ``selected`` is what ``spec.filters``
    leaves, measured through :func:`~scistackplot.reduce.apply_filters` — the
    function the figure itself uses. That shared rule is the point: a picker
    reading "3 of 12 selected" beside a figure built from a different 3 would be
    worse than showing no count at all.

    A filter that empties a factor is reported honestly as zero selected. It is
    a legitimate state to be in while clicking, and ``resolve`` renders the
    empty figure rather than raising.

    ``roles`` is what the panel's Factors dropdown renders: each role labelled
    for this measure's shape, flagged available or not, and carrying
    ``validate``'s own message when not (:func:`role_options`). GROUP is not
    among them — see :data:`ROLE_ORDER` — so ``group_available`` /
    ``group_reason`` report separately whether this factor may join the
    grouping, which is the Grouping section's question.
    """
    from .reduce import apply_filters

    factors = derived.describe()["factors"]
    for entry in factors:
        name = entry["name"]
        entry["roles"] = factors_menu(spec, derived, name)
        as_group = role_options(spec, derived, name, roles=(Role.GROUP,))[0]
        entry["group_available"] = as_group["available"]
        entry["group_reason"] = as_group["reason"]

    if not spec.filters and spec.location_filter.is_empty():
        # Nothing filtered: everything is selected, and no frame scan is needed
        # on the common path. The location filter has to be checked here too —
        # it narrows rows exactly as a Filter does, and a fast path that only
        # knew about one of them would report "all 12 selected" beside a figure
        # drawing 3, which is the precise failure this readout exists to avoid.
        for entry in factors:
            entry["selected"] = list(entry["levels"])
        return factors

    kept = apply_filters(derived.frame, spec)
    for entry in factors:
        name = entry["name"]
        surviving = (
            set(kept[name].astype(str)) if name in kept.columns else set()
        )
        entry["selected"] = [
            level for level in entry["levels"] if str(level) in surviving
        ]
    return factors


def variant_summary(spec: PlotSpec, table: LongTable) -> dict:
    """The variant picker's whole data model, and the combination readout.

    Three things, all measured against the same frame the renderer will use:

    ``sets``
        One entry per named variant — its label (the user's, or the auto one it
        would carry), the selection it holds, and **how many rows it actually
        matched**. That last number is the one that catches real mistakes: a
        variant selecting a combination nobody ever ran is indistinguishable
        from a working one until the series silently fails to appear.
    ``factors``
        Every variant axis in the data with its levels and its ``origin``, so
        the popup can map an axis to the pipeline node that produced it without
        parsing ``Code:`` or ``fn.param`` out of a column name. Taken from the
        table BEFORE selection, deliberately: an axis a set has already pinned
        is precisely the one the user needs to be able to re-open and change.
    ``total_combinations`` / ``selected_combinations``
        How much of the variant space the current selection covers.

    **Why the counts are measured, not computed.** Multiplying level counts
    would be wrong in two ways that matter. The default selection is on
    ``CodeIsLatest``, which is deliberately *not* a variant factor, so a purely
    combinatorial count would report every combination as selected while the
    figure showed half of them. And real data is ragged: a location never re-run
    under the newest code has no row for that combination, so the Cartesian
    product overstates what exists. Both numbers therefore come from the frame,
    through the same :func:`~scistackplot.variants.variant_set_mask` the
    renderer applies — a readout the figure could disagree with would be worse
    than none.

    ``selected_combinations`` of 0 is a legitimate state to display (the user
    has deselected everything); it is ``roles.validate``'s job to refuse
    rendering it, not this function's to hide it.
    """
    import pandas as pd

    from .variants import (
        auto_label,
        defined_sets,
        is_stated,
        label_variable,
        resolve_selection,
        row_mask,
        set_name,
        spanned_code_axes,
    )

    frame = table.frame
    names = [f.name for f in table.variant_factors if f.name in frame.columns]

    sets = []
    # With nothing selected — no rows, or none filled in yet — every row is on
    # screen, which is what the figure is showing too.
    kept_mask = pd.Series(not defined_sets(spec.variant_sets), index=frame.index)
    claimed = pd.Series(False, index=frame.index)
    for index, variant in enumerate(spec.variant_sets):
        # "Says something", which row 0 always does — it is the figure's
        # subject whether or not it narrows anything (`variants.is_stated`).
        # NOT the same question as `defined_sets`, which decides whether the
        # figure needs a `Variant` factor at all: a lone empty row 0 is stated
        # and folds to nothing, because one series needs no factor.
        defined = is_stated(variant, index)
        if defined:
            mask = row_mask(
                frame, spec, variant, latest_column=table.latest_column
            )
            # First-match-wins, mirroring apply_variant_sets: the count shown
            # must be the number of rows this variant contributes to the figure,
            # not the number it would match on its own.
            fresh = mask & ~claimed
            claimed |= fresh
            kept_mask |= fresh
        else:
            # An unfilled row is inert — it claims nothing and changes nothing,
            # so it must not be reported as having matched nothing either.
            fresh = pd.Series(False, index=frame.index)
        sets.append(
            {
                "name": set_name(
                    variant,
                    index,
                    primary=spec.y_measure,
                    latest_column=table.latest_column,
                ),
                # What the name BOX shows as its placeholder. Built from the
                # same rule as `name`, minus the user's override — the box has
                # to keep following the selection while it is being edited.
                "auto_label": auto_label(
                    variant.selection,
                    index=index,
                    variable=label_variable(variant, index, spec.y_measure),
                    latest_column=table.latest_column,
                ),
                "explicit_name": variant.name,
                "selection": dict(variant.selection),
                # None means the primary measure; the GUI shows that as the
                # dropdown's default rather than inventing a name for it.
                "variable": variant.variable,
                "defined": defined,
                "row_count": int(fresh.sum()),
                # Code axes this variant leaves open and disagrees on. Reported
                # per row because that is where the fix is (pin a version, or
                # split the row), and because the column itself is no longer
                # offered as a factor.
                "spans": spanned_code_axes(frame[fresh], variant.selection, table)
                if defined
                else {},
                # What this selection actually resolved to, and — when it
                # resolved to nothing — what it could have selected instead.
                #
                # A pin is applied blindly (`variants.default_selection`), so a
                # combination nobody ever ran selects zero rows and draws an
                # empty figure from controls that look correctly filled in.
                # "Empty" on its own is indistinguishable from a broken panel;
                # the attempted combination beside the available ones is what
                # turns it into something the user can act on in one step.
                #
                # Computed only for a defined row that matched nothing: on the
                # common path this is pure cost, and the answer ("what else is
                # there") is only ever interesting when the answer to "what did
                # I get" is nothing.
                "resolved": _jsonable_selection(
                    resolve_selection(
                        frame, variant.selection, latest_column=table.latest_column
                    )
                )
                if defined
                else {},
                "available": _available_combinations(frame, spec, variant, table)
                if defined and not fresh.any()
                else [],
            }
        )

    if not names:
        return {
            "sets": sets,
            "factors": [],
            "total_combinations": 0,
            "selected_combinations": 0,
        }

    kept = frame[kept_mask]
    as_text = frame[names].astype(str)
    total = len(as_text.drop_duplicates())
    selected = len(kept[names].astype(str).drop_duplicates()) if len(kept) else 0

    factors = []
    for factor in table.variant_factors:
        if factor.name not in frame.columns:
            continue
        levels = [str(level) for level in factor.levels]
        surviving = set(kept[factor.name].astype(str)) if len(kept) else set()
        factors.append(
            {
                "name": factor.name,
                "levels": levels,
                "selected": [level for level in levels if level in surviving],
                # Code axes read differently from experimental conditions —
                # one is usually pinned, the other usually faceted — so the GUI
                # needs to tell them apart without parsing the name itself.
                "is_code": factor.name.startswith(CODE_FACTOR_PREFIX),
                # Run-option axes present like code axes (pinned, function-owned)
                # but carry flag labels, not ordinals.
                "is_run": factor.name.startswith(RUN_FACTOR_PREFIX),
                "origin": factor.origin,
            }
        )

    return {
        "sets": sets,
        "factors": factors,
        "total_combinations": total,
        "selected_combinations": selected,
        "latest_column": table.latest_column,
    }


#: Variant combinations offered when a selection matched nothing. A list long
#: enough to find the near miss in, short enough to read without scrolling —
#: and bounded, because a ragged project can hold hundreds.
AVAILABLE_COMBINATION_LIMIT = 12


def _jsonable_selection(selection: dict) -> dict:
    """A resolved selection as plain JSON.

    ``resolve_selection`` hands back whatever the frame holds — numpy scalars,
    booleans, lists — and this crosses a JSON-RPC boundary.
    """
    from .table import _jsonable

    def one(value):
        if isinstance(value, (list, tuple, set, frozenset)):
            return [_jsonable(item) for item in value]
        return _jsonable(value)

    return {key: one(value) for key, value in selection.items()}


def _available_combinations(frame, spec, variant, table) -> list[dict]:
    """Variant combinations that DO exist, for a selection that matched none.

    Scoped to the row's own variable, because that is what the row is asking
    about: offering ``FilteredEMG``'s combinations to a row that plots
    ``RawEMG`` would send the user to fix the wrong thing.

    Ordered by the axes' declared level order so the list reads the same way
    twice, and capped at :data:`AVAILABLE_COMBINATION_LIMIT` — a ragged project
    can hold hundreds, and a wall of them answers nothing that the first dozen
    does not.
    """
    from .table import natural_sort_key
    from .variants import VARIABLE_COLUMN

    names = [f.name for f in table.variant_factors if f.name in frame.columns]
    if not names:
        return []

    rows = frame
    if VARIABLE_COLUMN in frame.columns:
        wanted = variant.variable or spec.y_measure
        rows = frame[frame[VARIABLE_COLUMN].astype(str) == wanted]
    if rows.empty:
        return []

    combos = rows[names].astype(str).drop_duplicates()
    ordered = sorted(
        (tuple(row) for row in combos.itertuples(index=False)),
        key=lambda values: tuple(natural_sort_key(v) for v in values),
    )
    return [
        dict(zip(names, values, strict=True))
        for values in ordered[:AVAILABLE_COMBINATION_LIMIT]
    ]
