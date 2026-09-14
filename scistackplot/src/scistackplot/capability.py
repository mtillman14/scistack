"""
Which plot kinds are available, and which one to pick by default.

This is the single rule behind two of the requirements that look separate:
"different data types get different default plots", and "iterating over a
higher schema level unlocks more summative plot types". Both fall out of one
observation — a distribution needs replicates, and replicates exist only when
some factor is left FREE (not mapped to a channel, not collapsed).

The GUI must render only what ``available_plots`` returns. Plot policy lives
here, not in TypeScript (CLAUDE.md NOTE 3).
"""

from __future__ import annotations

from dataclasses import replace

from .shape import Shape
from .spec import PlotKind, PlotSpec, Role
from .table import CODE_FACTOR_PREFIX, RUN_FACTOR_PREFIX, LongTable

#: Kinds that summarize several rows per x position into one mark.
DISTRIBUTION_KINDS = (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR, PlotKind.BAND)

#: What each role is CALLED, per measure shape.
#:
#: The role names are the library's vocabulary; these are the user's — and the
#: two are kept the SAME wherever a user has to search for one (see the FREE
#: entry in _ROLE_LABELS). What varies by shape is what a role DOES: "Average
#: over" describes a table, and a user looking at 1-D data is looking at
#: traces, so AGGREGATE is renamed for them. Reported by the backend rather
#: than hardcoded in the panel so the words and the behaviour stay together
#: (CLAUDE.md NOTE 3); the per-shape explanation is _ROLE_HINTS_BY_SHAPE.
#:
#: Only the entries that differ from :data:`_ROLE_LABELS` need listing.
_ROLE_LABELS_BY_SHAPE: dict[Shape, dict[Role, str]] = {
    Shape.SERIES_1D: {
        Role.AGGREGATE: "Average into one line",
    },
}

#: The order the dropdown lists roles in — NOT ``Role``'s declaration order,
#: which is grouped by what each role does to the data and puts "Separate
#: figures" first. This is the order a user reaches for: the channels that
#: place data on the page, then the two that reduce it.
ROLE_ORDER: tuple[Role, ...] = (
    Role.X,
    Role.COLOR,
    Role.FACET,
    Role.ITERATE,
    Role.AGGREGATE,
    Role.FREE,
)

#: The roles the **Factors** control offers. X is absent on purpose.
#:
#: "Which factors group the x axis, and in what order" is one question with two
#: halves, and it used to be asked in two places that could not see each other:
#: a per-factor dropdown for membership, and a separate "X grouping" list for
#: the order — which only appeared once two factors already held X, so it was
#: unreachable until the user had found the dropdown first. Both halves now live
#: in the Grouping section, and the x axis is chosen in exactly one place.
FACTOR_ROLE_ORDER: tuple[Role, ...] = tuple(r for r in ROLE_ORDER if r is not Role.X)

_ROLE_LABELS: dict[Role, str] = {
    Role.X: "X axis",
    Role.COLOR: "Color",
    Role.FACET: "Facet",
    Role.ITERATE: "Separate figures",
    Role.AGGREGATE: "Average over",
    # "Free", not "Replicates" / "One line each". The role's own name, kept the
    # same for every shape, because a user hunting for it in the dropdown has
    # read it in the docs, in a saved spec's TOML and in an exported `roles=`
    # argument — and found nothing matching (user, 2026-09-13). It is also the
    # role whose meaning a noun cannot carry on its own: what a FREE factor
    # does depends on the plot kind (one line each, a bar's error bars, a box's
    # distribution), which is what the HINT is for, per shape.
    Role.FREE: "Free",
}

_ROLE_HINTS: dict[Role, str] = {
    Role.X: "Position along the x axis",
    Role.COLOR: "One coloured series per level",
    Role.FACET: "One subplot per level — arrange them under Layout",
    Role.ITERATE: "One whole figure per level",
    Role.AGGREGATE: "Collapse to the mean first, so this factor does NOT "
    "widen the error bars",
    Role.FREE: "Keep each level as its own observation, so this factor DOES "
    "widen the error bars",
}

#: Stated as a CONTRAST, and deliberately in the same terms on both sides.
#:
#: These are the two roles a user cannot tell apart from the labels alone —
#: "Average over" and "Free" both sound like "not on an axis", and the earlier
#: hints described each one on its own, both using the word "average" (user,
#: 2026-09-13). The thing that actually distinguishes them is what they do to
#: the ERROR BARS: with schema [subject, trial] and a band, trial=AGGREGATE
#: averages each subject's trials into one trace and the band is then the
#: spread across SUBJECTS; trial=FREE pools every subject-trial trace, so the
#: band mixes within- and between-subject variability and a subject with more
#: trials weighs more. Same plot kind, same data, different published numbers —
#: which is why the plot-kind control cannot express this and both roles exist
#: (`reduce._collapse_aggregates` runs before `_summarize`; `has_replicates`
#: is true only when something is FREE, which is the part the kind list DOES
#: express — collapse everything and the summary kinds disappear).
_ROLE_HINTS_BY_SHAPE: dict[Shape, dict[Role, str]] = {
    Shape.SERIES_1D: {
        Role.AGGREGATE: "Average these traces together sample by sample "
        "first, so this factor does NOT widen the error band",
        Role.FREE: "One trace per level, each its own observation — so this "
        "factor DOES widen the error band",
    },
}


def role_label(role: Role, shape: Shape) -> str:
    """What to call this role for a measure of this shape."""
    return _ROLE_LABELS_BY_SHAPE.get(shape, {}).get(role) or _ROLE_LABELS[role]


def role_hint(role: Role, shape: Shape) -> str:
    """The one-line explanation under the label."""
    return _ROLE_HINTS_BY_SHAPE.get(shape, {}).get(role) or _ROLE_HINTS[role]


def _validation_error(spec: PlotSpec, table: LongTable) -> str | None:
    """``validate``'s complaint about this spec, or None if it has none."""
    from .roles import RoleError, validate

    try:
        validate(spec, table)
    except RoleError as exc:
        return str(exc)
    return None


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
    the panel used to offer all six roles unconditionally while ``validate``
    refused several of them, so a user could pick an option and be told it was
    impossible — X on a 1-D measure, a second factor on COLOR, X when an
    ``x_measure`` already supplies the axis. Any rule expressed here in parallel
    would be a second copy free to drift from the one that actually decides.
    Asking cannot drift, and a new rule in ``validate`` reaches the panel for
    free.

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
        candidate = replace(spec, roles={**spec.roles, factor: role})
        error = _validation_error(candidate, table)
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


def has_replicates(roles: dict[str, Role]) -> bool:
    """
    True when some factor's levels survive as multiple rows per plotted cell.

    AGGREGATE deliberately does not count: it collapses its factor to a mean
    *before* plotting, so it removes replicates rather than providing them.
    Its purpose is noise reduction ("average over trials"), after which a
    remaining FREE factor (e.g. subject) is what supplies the distribution.
    """
    return any(role is Role.FREE for role in roles.values())


def available_plots(
    shape: Shape,
    roles: dict[str, Role],
    *,
    has_x_measure: bool = False,
) -> list[PlotKind]:
    """Plot kinds that can be rendered for this shape and role assignment."""
    if shape is Shape.MATRIX_2D:
        return [PlotKind.HEATMAP]

    if has_x_measure:
        # x comes from a second measure: a relational scatter, optionally with
        # a connecting line when the x measure is ordered.
        return [PlotKind.SCATTER, PlotKind.LINE]

    replicates = has_replicates(roles)

    if shape is Shape.SERIES_1D:
        kinds = [PlotKind.LINE]
        if replicates:
            kinds.append(PlotKind.BAND)
        return kinds

    if shape is Shape.SCALAR:
        kinds = [PlotKind.SCATTER, PlotKind.STRIP]
        if replicates:
            kinds.extend([PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR])
        return kinds

    return []


def default_plot(
    shape: Shape,
    roles: dict[str, Role],
    *,
    has_x_measure: bool = False,
) -> PlotKind | None:
    """
    The kind to select when a table is first opened.

    scalar → scatter, or box once there are replicates to distribute;
    1-D → one line per observation, or a mean line with a shaded error region
    once there are replicates; 2-D → heatmap.
    """
    kinds = available_plots(shape, roles, has_x_measure=has_x_measure)
    if not kinds:
        return None

    replicates = has_replicates(roles)
    if has_x_measure:
        return PlotKind.SCATTER
    if shape is Shape.SERIES_1D:
        return PlotKind.BAND if replicates else PlotKind.LINE
    if shape is Shape.SCALAR:
        return PlotKind.BOX if replicates else PlotKind.SCATTER
    return kinds[0]


def why_unavailable(kind: PlotKind, shape: Shape, roles: dict[str, Role]) -> str | None:
    """
    Explain a kind's absence, for GUI tooltips on disabled options.

    Returns None when the kind IS available.
    """
    if kind in available_plots(shape, roles):
        return None
    if shape is Shape.MATRIX_2D:
        return "2-D measures render as a heatmap."
    if kind in DISTRIBUTION_KINDS and not has_replicates(roles):
        return (
            "Needs replicates: leave at least one factor 'free' (unassigned) so "
            "each x position has several values to summarize."
        )
    if kind is PlotKind.BAND and shape is not Shape.SERIES_1D:
        return "Error bands apply to 1-D measures."
    if kind is PlotKind.LINE and shape is Shape.SCALAR:
        return "Lines need a 1-D measure or a second measure for the x axis."
    return f"Not available for a {shape} measure."


def capabilities(spec: PlotSpec, table: LongTable) -> dict:
    """
    The full JSON-serializable capability report for the GUI.

    One call gives the panel everything it needs to render its controls:
    which kinds are selectable, why the others are not, and what the default
    would be for the current role assignment.
    """
    from .groups import apply_level_groups
    from .roles import complete_roles
    from .variants import apply_variant_sets, strip_answered_roles

    # The kinds and roles reported must be the ones the figure will actually be
    # built with, so the derived table (named variants folded into a ``Variant``
    # factor) is what they are computed against — exactly as ``resolve`` does,
    # stale-role drop included, or the panel would offer a role selector for a
    # factor the render is about to reject.
    derived = apply_level_groups(spec, apply_variant_sets(spec, table))
    spec = strip_answered_roles(spec, table, derived)
    roles = complete_roles(spec, derived)
    shape = derived.shape_of(spec.y_measure)
    has_x_measure = spec.x_measure is not None
    allowed = available_plots(shape, roles, has_x_measure=has_x_measure)

    return {
        "shape": str(shape),
        "has_replicates": has_replicates(roles),
        "default": str(default_plot(shape, roles, has_x_measure=has_x_measure) or ""),
        "available": [str(k) for k in allowed],
        "kinds": [
            {
                "kind": str(kind),
                "available": kind in allowed,
                "reason": why_unavailable(kind, shape, roles),
            }
            for kind in PlotKind
        ],
        "roles": {name: str(role) for name, role in roles.items()},
        "factors": factor_summary(spec, derived),
        "grouping": grouping_summary(spec, derived),
        "variants": variant_summary(spec, table),
    }


def factors_menu(spec: PlotSpec, table: LongTable, factor: str) -> list[dict]:
    """What the **Factors** dropdown lists for one factor.

    :func:`role_options` restricted to :data:`FACTOR_ROLE_ORDER`, plus X **only
    when this factor already holds it**. A ``<select>`` whose value is not among
    its options renders blank, and a scalar table opens with one factor on X by
    default (``roles.default_roles``) — so dropping X unconditionally would
    empty the control for exactly the factor the user is most likely to look at
    first.

    That X is then reported **unavailable even though it is perfectly legal**,
    which is the one place this report says something ``validate`` does not.
    The difference is deliberate and is presentation, not validity: X is listed
    here so the control can display its own value, while *setting* it belongs
    to the Grouping section. Keeping it selectable in both places is how the
    two controls would start disagreeing — the thing merging them was meant to
    stop.
    """
    roles = (
        (Role.X, *FACTOR_ROLE_ORDER)
        if spec.roles.get(factor) is Role.X
        else FACTOR_ROLE_ORDER
    )
    options = role_options(spec, table, factor, roles=roles)
    for option in options:
        if option["role"] == str(Role.X):
            option["available"] = False
            option["reason"] = (
                "The x axis is grouped in the Grouping section — untick this "
                "factor there to take it off the axis."
            )
    return options


def grouping_summary(spec: PlotSpec, table: LongTable) -> dict:
    """Whether the x axis can be grouped by factors, and how it is grouped now.

    A factor on the x axis IS a categorical grouping: ``xaxis.plan_x_axis``
    turns the observed level combinations into leaf positions with spacer
    categories between groups, which is what lets box, violin, bar and strip
    place themselves exactly as they already do. A continuous x never comes
    from a factor — it comes from ``x_measure``, or for 1-D data from the
    within-observation index — which is why this is offered for SCALAR measures
    and refused, with a reason, for everything else.
    """
    from .spec import MAX_X_LAYERS

    shape = table.shape_of(spec.y_measure)
    reason = None
    if spec.x_measure is not None:
        reason = (
            f"{spec.x_measure!r} already supplies the x axis, so it is a "
            f"measured value rather than groups of records."
        )
    elif shape is Shape.SERIES_1D:
        reason = (
            "This measure is 1-D: its x axis is the within-observation index "
            "(time, or percent of cycle). Separate the groups with colour or "
            "facets instead."
        )
    elif shape is Shape.MATRIX_2D:
        reason = "This measure is 2-D: a heatmap's axes come from the matrix."
    elif shape is not Shape.SCALAR:
        reason = f"Grouping the x axis needs a scalar measure; this one is {shape}."

    return {
        "available": reason is None,
        "reason": reason,
        # Membership and order reconciled the same way the figure does it, so
        # the control cannot show an order the renderer disagrees with.
        "layers": spec.ordered_x_layers(),
        "max_layers": MAX_X_LAYERS,
    }


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
    ``validate``'s own message when not (:func:`role_options`). X is not among
    them — see :data:`FACTOR_ROLE_ORDER` — so ``x_available``/``x_reason``
    report separately whether this factor may group the x axis, which is the
    Grouping section's question.
    """
    from .reduce import apply_filters

    factors = derived.describe()["factors"]
    for entry in factors:
        name = entry["name"]
        entry["roles"] = factors_menu(spec, derived, name)
        on_x = role_options(spec, derived, name, roles=(Role.X,))[0]
        entry["x_available"] = on_x["available"]
        entry["x_reason"] = on_x["reason"]

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
