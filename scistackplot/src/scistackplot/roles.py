"""
Role assignment: defaults, completion, and validation.

Every factor carries exactly one :class:`~scistackplot.spec.Role`. Enforcing
that here — once, in the library — is what lets the GUI be a thin renderer of
whatever ``capability.available_plots`` returns instead of re-deriving the
invariant in TypeScript.
"""

from __future__ import annotations

import math

from .shape import Shape
from .spec import MAX_X_LAYERS, SINGLE_ASSIGNMENT_ROLES, PlotSpec, Role, VariantPolicy
from .table import LongTable
from .variants import CURRENT_VARIANT_NAME, VARIANT_FACTOR


class RoleError(ValueError):
    """An invalid role assignment. Message names the one-line fix."""


def default_roles(table: LongTable, measure: str | None = None) -> dict[str, Role]:
    """
    A reasonable starting assignment for a freshly opened table.

    Mirrors the proof of concept's opening state (it preselected a factor for
    the x axis and left the rest available) but adds one thing it had no
    concept of: a variant factor defaults to COLOR so that two pipeline
    variants are visibly separated on first render rather than silently
    overplotted.
    """
    measure = measure or (table.measure_names[0] if table.measures else None)
    shape = table.shape_of(measure) if measure else Shape.UNKNOWN

    roles: dict[str, Role] = {}
    variants = [f for f in table.factors if f.is_variant and len(f.levels) > 1]
    fields = [f for f in table.factors if f.is_field]
    plain = [f for f in table.factors if not f.is_variant and not f.is_field]

    # A struct/dict variable's fields are parallel quantities (13 muscles, say),
    # not levels of one condition: one subplot each, never one overplotted axis.
    for factor in fields:
        roles[factor.name] = Role.FACET

    if variants:
        roles[variants[0].name] = Role.COLOR
        # FACET, not FREE, for the rest. A variant left FREE is pooled, which
        # `validate` refuses outright — so defaulting extras to FREE handed the
        # user an error instead of a plot the moment a table carried two variant
        # factors at once (e.g. a filter cutoff AND two versions of the
        # producing function's source). Faceting keeps them separated, which is
        # the same promise COLOR makes for the first one.
        for extra in variants[1:]:
            roles[extra.name] = Role.FACET

    # For 1-D measures the x axis is the within-observation index, so no factor
    # takes X; the leading factor becomes the colour channel instead.
    if shape is Shape.SERIES_1D:
        for factor in plain:
            if Role.COLOR not in roles.values():
                roles[factor.name] = Role.COLOR
            else:
                roles[factor.name] = Role.FREE
    else:
        for position, factor in enumerate(plain):
            if position == 0:
                roles[factor.name] = Role.X
            elif Role.COLOR not in roles.values():
                roles[factor.name] = Role.COLOR
            else:
                roles[factor.name] = Role.FREE

    return roles


def complete_roles(
    spec: PlotSpec, table: LongTable, *, promote: bool = True
) -> dict[str, Role]:
    """
    Every factor in ``table`` mapped to a role.

    Factors the spec doesn't mention default to FREE — they stay in the frame
    as replicate rows, which is the conservative choice: it never silently
    drops or averages data the user didn't ask to drop or average.

    The one exception is the synthetic ``Variant`` factor once it has more than
    one level, which defaults to COLOR. FREE is not the conservative choice
    *there*: it would overplot two variants the user has just gone to the
    trouble of naming, which is the exact failure this whole feature exists to
    prevent — and ``validate`` would refuse it a moment later anyway, so the
    alternative is an error message instead of the figure they asked for. Same
    reasoning as ``default_roles`` giving the first variant factor COLOR.

    Finally, schema keys nested above an iterated key are promoted to ITERATE
    (:func:`iterate_ancestors`). It happens **here**, rather than only where the
    fan-out is built, so that every consumer sees one consistent assignment: if
    ``reduce`` fanned out over a key that ``capability`` still believed was FREE,
    the panel would advertise a distribution for figures holding a single
    observation. ``promote=False`` returns the roles as declared, which is what
    reporting the promotion needs.
    """
    roles = {name: role for name, role in spec.roles.items() if table.has_factor(name)}
    for factor in table.factors:
        if (
            factor.name == VARIANT_FACTOR
            and factor.name not in roles
            and len(factor.levels) > 1
        ):
            taken = set(roles.values())
            roles[factor.name] = (
                Role.COLOR if Role.COLOR not in taken else Role.FACET
            )
            continue
        roles.setdefault(factor.name, Role.FREE)

    if promote:
        for name in iterate_ancestors(roles, table):
            roles[name] = Role.ITERATE
    return roles


def iterate_ancestors(roles: dict[str, Role], table: LongTable) -> list[str]:
    """
    Schema keys that must iterate because a key nested under them does.

    "One figure per trial" is never really one figure per trial: with a schema
    of ``[subject, trial]``, trial 1 belongs to a subject, and a figure holding
    every subject's trial 1 pools unrelated observations. So an iterated key
    iterates its ancestors too — the fan-out becomes one figure per
    (subject, trial), which is also what makes stepping past subject 1's last
    trial roll over to subject 2's first.

    Promoted **only from FREE**. A user who assigned an ancestor a channel meant
    it: ``subject=colour, trial=separate figures`` is a legitimate figure (every
    subject coloured, one figure per trial) and must survive. FREE is the one
    role that would silently pool, and pooling is the mistake this prevents;
    ``AGGREGATE`` remains the way to say "average the subjects away" on purpose.

    Pure, and called twice per resolve — once by :func:`complete_roles` to apply
    it and once by ``reduce._fanout_notes`` to report it — rather than threading
    the result through as state. The reporting call must pass **unpromoted**
    roles (``complete_roles(..., promote=False)``): read back off its own output
    this returns nothing, because every ancestor is ITERATE by then, and the
    note explaining a fan-out four times the expected size would never appear.
    """
    if not table.schema_levels:
        return []
    promoted: list[str] = []
    for name, role in roles.items():
        if role is not Role.ITERATE or name not in table.schema_levels:
            continue
        for ancestor in table.schema_levels[: table.schema_levels.index(name)]:
            if (
                table.has_factor(ancestor)
                and roles.get(ancestor, Role.FREE) is Role.FREE
                and ancestor not in promoted
            ):
                promoted.append(ancestor)
    return promoted


def fanout_keys(spec: PlotSpec, table: LongTable) -> list[str]:
    """
    The factors this spec fans out over, **in the order the figures run**.

    Ordered by the table's declared factor order — for a scidb source that is
    variants, then schema keys outermost-first — and NOT by the order the roles
    dict happens to hold. Dict order is whichever role the user clicked first,
    so assigning ``trial`` before ``subject`` produced a trial-major fan-out:
    figures ordered trial-1-subject-1, trial-1-subject-2, …, which makes the
    exported ``PathOutput`` template and the panel's next/previous arrows both
    run in an order nobody asked for.

    One definition, used by ``reduce.resolve`` (the interactive fan-out) and by
    ``scistackplotdb.endpoint`` (the ``for_each`` iteration keys), because those
    two disagreeing is this layer's worst failure — see
    ``scistackplotdb/tests/test_fanout_parity.py``.
    """
    roles = complete_roles(spec, table)
    order = table.factor_names
    names = [
        name
        for name, role in roles.items()
        if role is Role.ITERATE and table.has_factor(name)
    ]
    return sorted(names, key=order.index)


def validate(spec: PlotSpec, table: LongTable) -> None:
    """Raise :class:`RoleError` if the spec cannot be resolved against the table."""
    # --- measures exist -------------------------------------------------
    if not spec.measures:
        raise RoleError("PlotSpec.measures is empty — name at least a y measure.")
    if len(spec.measures) > 1:
        raise RoleError(
            f"PlotSpec.measures names one y measure; got {spec.measures}. "
            f"To plot several variables together, add a variant row per "
            f"variable (VariantSet(variable=...)) — they stack into the "
            f"'Variant' factor and can take a colour or a facet. For an x-y "
            f"plot, set x_measure."
        )
    for measure in [*spec.measures, spec.x_measure]:
        if measure is not None and measure not in table.measure_names:
            raise RoleError(
                f"Measure {measure!r} is not in the table. "
                f"Available measures: {table.measure_names}"
            )

    # --- roles name real factors ----------------------------------------
    unknown = [name for name in spec.roles if not table.has_factor(name)]
    if unknown:
        raise RoleError(
            f"Roles assigned to unknown factor(s) {unknown}. "
            f"Table factors: {table.factor_names}"
        )

    # --- single-assignment channels -------------------------------------
    for role in SINGLE_ASSIGNMENT_ROLES:
        holders = spec.factors_with_role(role)
        if len(holders) > 1:
            raise RoleError(
                f"Role {role} accepts one factor but got {holders}. "
                f"Move all but one to 'facet' or 'free'."
            )

    shape = table.shape_of(spec.y_measure)

    # --- x-axis ownership ------------------------------------------------
    x_layers = spec.ordered_x_layers()
    if len(x_layers) > MAX_X_LAYERS:
        raise RoleError(
            f"At most {MAX_X_LAYERS} factors can share the x axis; got "
            f"{len(x_layers)}: {x_layers}. A fourth level of nesting cannot be "
            f"read off an axis — move one to 'color', a facet role, or "
            f"'separate figures'."
        )
    if len(x_layers) > 1 and shape is not Shape.SCALAR:
        raise RoleError(
            f"Nested x grouping needs a categorical axis, but measure "
            f"{spec.y_measure!r} is {shape} — its x axis is "
            f"{'its within-observation index' if shape is Shape.SERIES_1D else 'the matrix itself'}. "
            f"Group a 1-D measure with 'color' and facets instead."
        )
    x_holder = spec.first_with_role(Role.X)
    if spec.x_measure is not None and x_holder is not None:
        raise RoleError(
            f"Measure {spec.x_measure!r} already supplies the x axis, so factor "
            f"{x_holder!r} cannot also hold role 'x'. Give it 'color', a facet "
            f"role, or 'free'."
        )
    if shape is Shape.SERIES_1D and x_holder is not None and spec.x_measure is None:
        raise RoleError(
            f"Measure {spec.y_measure!r} is 1-D, so the x axis is its "
            f"within-observation index ({spec.index_column or 'index'}); factor "
            f"{x_holder!r} cannot hold role 'x'. Give it 'color', a facet role, "
            f"or 'free'."
        )
    if shape is Shape.MATRIX_2D and x_holder is not None:
        raise RoleError(
            f"Measure {spec.y_measure!r} is 2-D (heatmap); its axes come from the "
            f"matrix itself, so factor {x_holder!r} cannot hold role 'x'."
        )

    # --- variants must not be pooled by accident -------------------------
    if spec.variant_policy is VariantPolicy.FACET:
        # Completed roles, not the spec's raw ones: the synthetic ``Variant``
        # factor is defaulted rather than assigned (see complete_roles), and
        # reading spec.roles here would reject the very figure that defaulting
        # exists to produce.
        assigned = complete_roles(spec, table)
        pooled = [
            f.name
            for f in table.variant_factors
            if len(f.levels) > 1
            and assigned.get(f.name, Role.FREE) in (Role.FREE, Role.AGGREGATE)
        ]
        if pooled:
            raise RoleError(
                f"Variant factor(s) {pooled} would be pooled: their levels are "
                f"different pipeline variants, not replicates, so averaging or "
                f"overplotting them silently mixes results. Assign them "
                f"'color'/'facet'/'iterate', select the variants you want with "
                f"PlotSpec.variant_sets, or opt in with variant_policy='pool'."
            )

    # --- 1-D needs an index ---------------------------------------------
    if shape is Shape.SERIES_1D and spec.index_column:
        if (
            spec.index_column not in table.frame.columns
            and spec.index_column != table.index_column
        ):
            raise RoleError(
                f"index_column {spec.index_column!r} is neither a column of the "
                f"table nor its declared index column ({table.index_column!r})."
            )


def default_spec(table: LongTable, measure: str | None = None) -> PlotSpec:
    """
    The spec a table opens on: default roles, the matching default kind, and a
    facet grid wide enough for however many fields the measure has.

    Lives here rather than in the GUI so the panel and a library caller open on
    the same figure (CLAUDE.md NOTE 3).
    """
    from .capability import default_plot
    from .spec import FacetOptions, PlotKind, VariantSet, grid_shape_for

    from .variants import apply_variant_sets, default_selection

    measure = measure or (table.measure_names[0] if table.measures else None)
    if measure is None:
        raise RoleError("This table has no measures to plot.")

    # Open on ONE variant, as a single named row.
    #
    # Every variant axis is pinned, not just the code ones: latest body, first
    # parameter value (`variants.default_selection` owns the rule and states it
    # in full). Pinning only "current code" left a swept parameter unanswered,
    # so `default_roles` found a multi-level variant factor with no role and put
    # it on COLOUR — a variable produced at 5 cutoffs opened as 5 overlaid
    # series before the user had said anything at all. Comparing is what a
    # second row is for.
    #
    # A single row is deliberately the opening state rather than zero: the GUI's
    # Variants section always has a row to edit, and this pin is a variant like
    # any other — the user adds a second row to compare against it instead of
    # first discovering a hidden pin and clearing it.
    #
    # The name stays "current" even though the row now also pins parameters,
    # which under-describes it. Fixing that means teaching `auto_label` to spell
    # the latest FLAG as "current" (it would otherwise render the raw
    # `CodeIsLatest=True`), and the label belongs with the rest of the Variants
    # section work rather than here.
    selection = default_selection(table)
    variant_sets = (
        [VariantSet(name=CURRENT_VARIANT_NAME, selection=selection)]
        if selection
        else []
    )

    # Roles describe the table AS RESOLVED, so they are derived after the
    # variants are chosen — never before. Defaulting against the undecided table
    # put a role on a `Code:<fn>` factor that the opening variant then answered,
    # and `validate` calls a role on a missing factor an unknown factor and
    # refuses to draw anything. (`strip_answered_roles` catches the same thing
    # arriving from a saved spec; this stops it being created here at all.)
    resolved = apply_variant_sets(
        PlotSpec(measures=[measure], variant_sets=variant_sets), table
    )
    roles = default_roles(resolved, measure)
    kind = default_plot(resolved.shape_of(measure), roles) or PlotKind.SCATTER

    # A 13-muscle struct wants a grid, not a 13-wide strip of subplots. The
    # arithmetic lives in grid_shape_for so the panel, the renderer and this
    # default cannot disagree about what "auto" means. Only the width is pinned:
    # leaving n_rows open lets the height follow the panel count if the data
    # gains a field.
    facet_panels = math.prod(
        [len(f.levels) for f in resolved.factors if roles.get(f.name) is Role.FACET]
        or [0]
    )
    _, n_cols = grid_shape_for(facet_panels) if facet_panels > 1 else (1, None)

    return PlotSpec(
        measures=[measure],
        roles=roles,
        kind=kind,
        facet=FacetOptions(n_cols=n_cols),
        variant_sets=variant_sets,
    )
