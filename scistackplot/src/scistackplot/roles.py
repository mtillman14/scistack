"""
Role assignment: defaults, completion, validation — and the two orders that
follow from an assignment.

Every factor carries exactly one :class:`~scistackplot.spec.Role`. Enforcing
that here — once, in the library — is what lets the GUI be a thin renderer of
whatever ``capability.available_plots`` returns instead of re-deriving the
invariant in TypeScript.

Two orders are derived here and nowhere else (docs/claude/grouping-and-collapse.md):

* :func:`collapse_order` — which ``COLLAPSE`` keys average away first. Deepest
  first, so "trial within subject, then subject" — and the LAST one is the
  **sample** (:func:`sample_key`) the kind's statistic is computed over.
* :func:`grouping_layers` — how the ordered ``GROUP`` list (innermost first)
  reads for a kind: which layers are nested x ticks and which are series.
  The list is the user's; the reading is the kind's; both are decided here so
  ``reduce``, ``xaxis``, ``ylimits`` and ``codegen`` cannot disagree.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace

from scistacklog import Log

from .shape import Shape
from .spec import MAX_X_LAYERS, OVERLAY_KINDS, PlotKind, PlotSpec, Role
from .table import LongTable
from .variants import VARIABLE_COLUMN, VARIANT_FACTOR

LAYER = "scistackplot"


class RoleError(ValueError):
    """An invalid role assignment. Message names the one-line fix."""


@dataclass(frozen=True)
class Assignment:
    """Roles plus the two things a roles dict cannot carry: the grouping order
    and which grouping layer is coloured.

    The unit ``default_assignment`` returns and ``roles_for_kind`` suggests —
    a kind re-defaulting a figure has to say all three at once, or the GUI
    would apply the roles and be left with a stale order and a colour naming a
    factor that no longer groups.
    """

    roles: dict[str, Role] = field(default_factory=dict)
    #: Innermost first; only ``GROUP`` holders, every one of them.
    groups: list[str] = field(default_factory=list)
    color: str | None = None

    def apply(self, spec: PlotSpec) -> PlotSpec:
        return replace(
            spec, roles=dict(self.roles), groups=list(self.groups), color=self.color
        )

    def to_dict(self) -> dict:
        return {
            "roles": {name: str(role) for name, role in self.roles.items()},
            "groups": list(self.groups),
            "color": self.color,
        }


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


def _plain(table: LongTable):
    return [f for f in table.factors if not f.is_variant and not f.is_field]


def _deepest(names: list[str], table: LongTable) -> str | None:
    """The deepest of ``names`` by ``factor_depths`` — a schema key
    preferred over a joined factor variable at the same depth, since the key
    is what the record is actually stored under. None if ``names`` is empty."""
    if not names:
        return None
    depths = table.factor_depths
    return max(
        names,
        key=lambda n: (
            depths.get(n) if depths.get(n) is not None else -1,
            n in table.schema_levels,
        ),
    )


def default_assignment(
    table: LongTable, measure: str | None = None, *, shape: Shape | None = None
) -> Assignment:
    """
    A reasonable starting assignment for a freshly opened table.

    **Granular and fast** (user, 2026-09-17): the finest split first, and the
    user pools deliberately.

    * **A scalar measure** groups its own level — the deepest schema key
      present, ``trial`` for a trial-level variable — and iterates everything
      else: one mark per trial, one figure per (subject, session, speed).
      Nothing is collapsed, so the bar has no error bars and the distribution
      kinds stay greyed out until the user collapses something; the
      greyed-out reason says exactly that.
    * **A 1-D or 2-D measure opens on ONE record**: every schema key is
      ITERATE, nothing grouped. Decided 2026-09-13 for the opening cost — a
      variable of 419 records x 10 fields x 41 k samples opened as a band over
      all 174 M samples (15 s once the reductions were in numpy, minutes
      before), where one record's figure resolves in 0.4 s. The fan-out
      navigator steps through the rest, and pooling is one role change away.

    Either way a variant factor becomes the innermost, COLOURED grouping
    layer so that two pipeline variants are visibly separated on first render
    rather than silently overplotted, and a struct's fields become one subplot
    each.

    ``shape`` overrides what the table says the measure holds. That is for the
    one caller who knows better: a 1-D measure drawn by a scalar kind is
    cell-collapsed before it is plotted, so it should open the way a SCALAR
    measure does (:func:`roles_for_kind`).
    """
    measure = measure or (table.measure_names[0] if table.measures else None)
    if shape is None:
        shape = table.shape_of(measure) if measure else Shape.UNKNOWN

    roles: dict[str, Role] = {}
    groups: list[str] = []
    color: str | None = None
    variants = [f for f in table.factors if f.is_variant and len(f.levels) > 1]
    fields = [f for f in table.factors if f.is_field]
    plain = _plain(table)

    # A struct/dict variable's fields are parallel quantities (13 muscles, say),
    # not levels of one condition: one subplot each, never one overplotted axis.
    for factor in fields:
        roles[factor.name] = Role.FACET

    if variants:
        roles[variants[0].name] = Role.GROUP
        groups.append(variants[0].name)
        color = variants[0].name
        # FACET for the rest. A second variant factor (a filter cutoff AND two
        # versions of the producing function's source) is kept apart the same
        # way the first one is, just on another channel.
        for extra in variants[1:]:
            roles[extra.name] = Role.FACET

    if shape in (Shape.SERIES_1D, Shape.MATRIX_2D):
        for factor in plain:
            roles[factor.name] = Role.ITERATE
    else:
        own_level = _deepest([f.name for f in plain], table)
        for factor in plain:
            roles[factor.name] = Role.GROUP if factor.name == own_level else Role.ITERATE
        if own_level is not None:
            groups.append(own_level)

    return Assignment(roles=roles, groups=groups, color=color)


def default_roles(
    table: LongTable, measure: str | None = None, *, shape: Shape | None = None
) -> dict[str, Role]:
    """The roles half of :func:`default_assignment`."""
    return default_assignment(table, measure, shape=shape).roles


# ---------------------------------------------------------------------------
# Completion
# ---------------------------------------------------------------------------


def complete_assignment(spec: PlotSpec, table: LongTable) -> Assignment:
    """
    Every factor in ``table`` mapped to a role, the grouping order reconciled,
    and the colour checked against it.

    Factors the spec doesn't mention default to **ITERATE**: a factor that
    appeared after the spec was saved fans out visibly rather than pooling
    silently. This is what let ``iterate_ancestors`` (the promotion of FREE
    ancestors of an iterated key) be deleted — there is no silent role left to
    defend against.

    Two exceptions, both because ITERATE would hide the comparison the user
    just built:

    * the synthetic ``Variant`` factor once it has more than one level becomes
      the innermost grouping layer and takes the colour if nothing has it —
      the user named two variants to see them side by side;
    * ``Variable`` (several variables in one figure) becomes FACET: "two mean
      ± error plots", one panel each, the shape the request came in as.

    ``color`` is kept only if it names a grouping layer; otherwise it is
    dropped here (``validate`` still refuses the spec — dropping is for the
    consumers that must draw something, e.g. the capability report).
    """
    roles = {name: role for name, role in spec.roles.items() if table.has_factor(name)}
    defaulted_variant: str | None = None
    for factor in table.factors:
        if factor.name in roles:
            continue
        if factor.name == VARIANT_FACTOR and len(factor.levels) > 1:
            roles[factor.name] = Role.GROUP
            defaulted_variant = factor.name
            continue
        if factor.name == VARIABLE_COLUMN:
            roles[factor.name] = Role.FACET
            continue
        roles[factor.name] = Role.ITERATE

    groups = spec.ordered_groups(roles, table.factor_depths)
    if defaulted_variant is not None and defaulted_variant in groups:
        # Innermost, whatever the depth rule said: the comparison sits inside
        # every tick.
        groups = [defaulted_variant, *[g for g in groups if g != defaulted_variant]]

    color = spec.color if spec.color in groups else None
    if color is None and defaulted_variant is not None:
        color = defaulted_variant
    return Assignment(roles=roles, groups=groups, color=color)


def complete_roles(spec: PlotSpec, table: LongTable) -> dict[str, Role]:
    """The roles half of :func:`complete_assignment`."""
    return complete_assignment(spec, table).roles


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


# ---------------------------------------------------------------------------
# The collapse chain
# ---------------------------------------------------------------------------


def collapse_order(roles: dict[str, Role], table: LongTable) -> list[str]:
    """
    The ``COLLAPSE`` factors in the order they average away: **deepest first**.

    With ``[subject, session, trial]`` and subject and trial both collapsed,
    trial averages within each subject first, then subject — so each subject
    counts once however many trials it has. Field factors (``ColName``) sit
    inside a record and go first of all; a joined factor variable carries the
    depth of the key it hangs off (``FactorInfo.depth``) and collapses with
    it; a factor with no depth (a derived bucket) is not in the hierarchy and
    goes last.

    The LAST entry is the sample (:func:`sample_key`). One owner, read by
    ``reduce``, ``reducer``, ``ylimits`` and ``codegen``: a chain computed two
    ways is a preview that disagrees with its own export.
    """
    depths = table.factor_depths
    fields = {f.name for f in table.field_factors}
    names = [
        name
        for name, role in roles.items()
        if role is Role.COLLAPSE and table.has_factor(name)
    ]

    def rank(name: str) -> tuple[int, int]:
        if name in fields:
            return (0, 0)
        depth = depths.get(name)
        return (1, -depth) if depth is not None else (2, 0)

    ordered = sorted(names, key=rank)  # stable: equal ranks keep declaration order
    if ordered:
        Log.debug(
            "collapse order %s (sample = %s)", ordered, ordered[-1], layer=LAYER
        )
    return ordered


def sample_key(roles: dict[str, Role], table: LongTable) -> str | None:
    """The outermost collapsed key — whose per-level values, after every
    inner collapse, are the sample the kind's statistic is computed over.
    None when nothing is collapsed: a bar of single values, no error bar."""
    order = collapse_order(roles, table)
    return order[-1] if order else None


#: Kinds that draw each level of the SAMPLE as its own polyline — one line
#: per subject, inside its colour / dash (seaborn's ``units=``). A line needs
#: one y per x within a series, so without this the sample's rows would share
#: one series id and the line would zigzag between subjects at every x.
#: (Schema-level parity, user 2026-09-19: every kind draws the sample rows;
#: none averages them further. The kinds that did — scatter, strip, line,
#: spaghetti — were ``MEAN_DRAWING_KINDS``, deleted.)
UNIT_KINDS = (PlotKind.LINE, PlotKind.SPAGHETTI)


def spaghetti_sample_repeats(
    spec: PlotSpec, roles: dict[str, Role], table: LongTable, sample: list[str]
) -> tuple[bool, str]:
    """Whether a spaghetti's SAMPLE recurs across its x ticks — so each sample
    level can be its own joined line — and why, in words.

    A spaghetti line joins one identity across the tick layers. A subject has
    a value at every session, so "subject 01" is a real line; a trial belongs
    to one session, so "trial 1" at pre and "trial 1" at post are different
    trials and a line through them would be invented. The rule is the one
    ``overlay_join`` uses for "Show sample": the deepest sample key must sit
    ABOVE the deepest tick layer in the schema hierarchy. A key with no depth
    on either side cannot be placed, and the answer is no.

    When the answer is no, the sample cannot be drawn as lines and the kind
    averages it into its line instead (``CollapseSteps.final``) — the one
    exception to schema-level parity, logged where it is taken.
    """
    if not sample:
        return False, "Nothing is collapsed."
    depths = table.factor_depths
    ticks = spec.ordered_groups(roles, depths)[1:]
    sample_depths = [depths.get(name) for name in sample]
    tick_depths = [depths[name] for name in ticks if name in depths]
    if any(depth is None for depth in sample_depths):
        return False, (
            f"{' x '.join(sample)} has no place in the schema hierarchy, so whether "
            f"it recurs across the x axis cannot be told — each line is its mean."
        )
    if not tick_depths:
        return False, (
            "No x layer is a schema key, so repeated measures cannot be told — "
            "each line is the sample's mean."
        )
    deepest_tick = max(ticks, key=lambda name: depths.get(name, -1))
    if max(sample_depths) < max(tick_depths):
        return True, (
            f"Each {' x '.join(sample)} has a value at every {deepest_tick}: "
            f"one line each."
        )
    return False, (
        f"A {sample[-1]} belongs to one {deepest_tick}, so no line can join it "
        f"across the axis — each line is the mean of its {' x '.join(sample)}."
    )


@dataclass(frozen=True)
class CollapseSteps:
    """The collapse chain as a figure runs it — the same for every kind.

    ``pre`` are averaged away first, deepest first, each one grouping on every
    other factor still present — the nested, unweighted means. What remains has
    one row per level of the sample key(s): **the sample, which every kind
    draws** (schema-level parity, 2026-09-19), each in its own geometry:

    * a summary kind (bar, band) computes centre ± spread over the sample rows
      in its own panel step (``reduce._summarize``);
    * a distribution kind (box, violin) draws the sample rows as they are;
    * scatter and strip draw one point per sample row;
    * line and spaghetti draw one polyline per sample level
      (:data:`UNIT_KINDS`, ``GroupingLayers.units``).

    There is no step after the sample — with ONE exception. The old
    ``final`` (the sample's own mean, for scatter / strip / line /
    spaghetti) now runs only for a **spaghetti whose sample does not recur
    across its x ticks** (:func:`spaghetti_sample_repeats`: trials under a
    session tick cannot be joined, so each line is their mean). Everywhere
    else a bar of subjects and a scatter of subjects are drawn from the same
    rows — and the "Save data" CSV (``export.plot_data``) is those rows.

    ``pooled`` (``Aggregation.pooled``) empties ``pre``: every collapsed key
    is the sample at once, one groupby, weighted by N.

    One owner, read by ``reduce``, ``reducer`` and ``ylimits`` — a chain
    computed two ways is a preview that disagrees with its own axis.
    """

    pre: list[str]
    sample: list[str]
    #: The sample averaged too — ONLY for a spaghetti whose sample cannot be
    #: joined across its ticks (see above). Empty for every other figure.
    final: list[str] = field(default_factory=list)

    @property
    def all(self) -> list[str]:
        return [*self.pre, *self.sample]

    @property
    def sample_key(self) -> str | None:
        """The one sample key, or None when pooled (several) or absent."""
        return self.sample[0] if len(self.sample) == 1 else None


def collapse_steps(
    spec: PlotSpec, roles: dict[str, Role], table: LongTable
) -> CollapseSteps:
    """:class:`CollapseSteps` — see :func:`collapse_order`. The same for every
    kind (every kind draws the same sample), except the spaghetti case
    documented on the class."""
    order = collapse_order(roles, table)
    if not order:
        return CollapseSteps(pre=[], sample=[])
    if spec.aggregate.pooled:
        pre, sample = [], list(order)
    else:
        pre, sample = list(order[:-1]), [order[-1]]
    final: list[str] = []
    if spec.kind is PlotKind.SPAGHETTI:
        repeats, reason = spaghetti_sample_repeats(spec, roles, table, sample)
        if not repeats:
            final = list(sample)
        Log.debug("spaghetti sample %s: %s", sample, reason, layer=LAYER)
    return CollapseSteps(pre=pre, sample=sample, final=final)


def has_sample(roles: dict[str, Role]) -> bool:
    """Whether any factor is collapsed — i.e. whether there is a sample for a
    distribution kind to summarise. Cheaper than :func:`sample_key` and needs
    no table, for the capability report's per-kind loop."""
    return any(role is Role.COLLAPSE for role in roles.values())



# ---------------------------------------------------------------------------
# "Show sample": the collapse chain, cut early, drawn on top of the marks
# ---------------------------------------------------------------------------


def overlay_unavailable(
    spec: PlotSpec, roles: dict[str, Role], shape: Shape
) -> str | None:
    """Why this figure cannot carry a "Show sample" overlay, or None.

    The one statement of what an overlay needs — the kind draws summative
    marks on a categorical x (:data:`OVERLAY_KINDS`, no ``x_measure``), the
    drawn shape is scalar, and something is collapsed so there is a sample
    to show. Shared by ``validate`` (which ignores the names, with this
    reason logged) and the capability report (which greys the section out
    with it), so the two can never disagree.
    """
    if spec.kind not in OVERLAY_KINDS:
        return (
            f"A {spec.kind} plot draws the sample itself; the overlay is for "
            f"the summative kinds (bar, box, violin, scatter, strip)."
        )
    if spec.x_measure is not None:
        return "An x-y plot has no categorical slot to place sample points in."
    if shape is not Shape.SCALAR:
        return f"The drawn shape is {shape}; the overlay needs one value per row."
    if not has_sample(roles):
        return "Nothing is collapsed, so there is no sample to show."
    return None


@dataclass(frozen=True)
class OverlaySteps:
    """The overlay chain: which collapsed keys average away, which remain.

    ``averaged`` is the collapse chain up to (not including) the deepest key
    the user checked — deepest first, run exactly as the marks' own chain is
    (``reduce._collapse_levels``). ``shown`` is the rest, **outermost first**:
    the identity of one overlay point, and the columns its hover names.
    """

    averaged: list[str]
    shown: list[str]

    @property
    def deepest_shown(self) -> str:
        return self.shown[-1]


def chain_cut(order: list[str], key: str) -> tuple[list[str], list[str]]:
    """``(averaged, kept)``: the collapse chain ``order`` (deepest first) cut
    just before ``key`` — every key deeper than it averages away, ``key``
    and everything shallower stays.

    One rule shared by "Show sample" (:func:`overlay_steps`) and the "Save
    data" depth (``export.plot_data``): checking ``trial`` in either means
    "cycles averaged within each trial, every trial of every subject kept".
    A deeper key implies the shallower ones, because they are its identity.
    """
    cut = order.index(key)
    return list(order[:cut]), list(order[cut:])


def overlay_steps(
    spec: PlotSpec, roles: dict[str, Role], table: LongTable
) -> OverlaySteps | None:
    """The overlay chain for ``spec.show_sample``, or None when there is none.

    The rule is a cut: with the chain ``[cycle, trial, subject]`` (deepest
    first), checking ``trial`` averages ``cycle`` and shows ``trial`` and
    ``subject`` — a trial is a trial *of* a subject, so a deeper key implies
    every shallower collapsed key. Checked names that are not collapsed right
    now (the user moved the factor to a grouping, say) are ignored and logged;
    ``validate`` refuses names that are not factors at all.
    """
    if not spec.show_sample:
        return None
    order = collapse_order(roles, table)
    checked = [name for name in spec.show_sample if name in order]
    ignored = [name for name in spec.show_sample if name not in order]
    if ignored:
        Log.debug(
            "show_sample: %s not collapsed right now, ignored", ignored, layer=LAYER
        )
    if not checked:
        return None
    averaged, kept = chain_cut(order, min(checked, key=order.index))
    steps = OverlaySteps(averaged=averaged, shown=list(reversed(kept)))
    Log.debug(
        "show_sample %s: average %s, one point per %s",
        spec.show_sample,
        steps.averaged or "nothing",
        " x ".join(steps.shown),
        layer=LAYER,
    )
    return steps


def overlay_color(spec: PlotSpec, steps: OverlaySteps | None) -> str | None:
    """The shown key colouring the overlay (``PlotSpec.sample_color``), or
    None when the points take their mark's colour.

    The ONE statement of when the setting is active: the key must be one of
    the overlay's shown keys right now. A setting that names a key not shown
    (unticked, or no longer collapsed) is inert — kept in the spec, listed by
    the capability report — the same contract as ``show_sample`` entries,
    so the dropdown's state is never something the spec must adjudicate.
    ``reduce``, ``codegen`` and ``capability`` all read this, never the
    field.
    """
    if spec.sample_color is None or steps is None:
        return None
    if spec.sample_color not in steps.shown:
        Log.debug(
            "sample_color %r not shown right now (shown: %s), inert",
            spec.sample_color,
            steps.shown,
            layer=LAYER,
        )
        return None
    return spec.sample_color


@dataclass(frozen=True)
class OverlayJoin:
    """Whether overlay points are joined into lines, and why."""

    join: bool
    #: True when the rule decided (``PlotSpec.join_sample`` is None).
    automatic: bool
    reason: str


def overlay_join(
    spec: PlotSpec,
    roles: dict[str, Role],
    table: LongTable,
    steps: OverlaySteps | None = None,
) -> OverlayJoin:
    """Join the overlay points across x, or leave them as points.

    The automatic rule compares hierarchy depths (``LongTable.factor_depths``):
    when the **deepest shown key** sits ABOVE the **deepest grouping layer**
    (ticks and colour alike), each point identity recurs at every x position
    — a subject has a value at every session — so the points are repeated
    measures and are joined. When it sits at or below the layers, a point
    belongs to one x position (a trial is *of* one session) and nothing is
    joined. A key with no depth on either side (a field, a derived bucket,
    the Variant axis) cannot be placed, and the rule declines: points only,
    with the reason. ``PlotSpec.join_sample`` overrides the rule either way.
    """
    if steps is None:
        steps = overlay_steps(spec, roles, table)
    if steps is None:
        return OverlayJoin(join=False, automatic=True, reason="No overlay.")
    if spec.join_sample is not None:
        return OverlayJoin(
            join=spec.join_sample,
            automatic=False,
            reason="Set by hand." if spec.join_sample else "Points only, set by hand.",
        )

    depths = table.factor_depths
    layers = grouping_layers(spec, table, roles)
    layer_names = [*layers.ticks, *([layers.color] if layers.color else [])]
    shown_depth = depths.get(steps.deepest_shown)
    layer_depths = [depths[name] for name in layer_names if name in depths]
    if shown_depth is None:
        return OverlayJoin(
            join=False,
            automatic=True,
            reason=(
                f"{steps.deepest_shown!r} has no place in the schema hierarchy, so "
                f"whether its points repeat across the x axis cannot be told."
            ),
        )
    if not layer_depths:
        return OverlayJoin(
            join=False,
            automatic=True,
            reason="No grouping layer is a schema key, so repeated measures cannot be told.",
        )
    deepest_layer = max(layer_names, key=lambda n: depths.get(n, -1))
    if shown_depth < max(layer_depths):
        return OverlayJoin(
            join=True,
            automatic=True,
            reason=(
                f"Repeated measures: each {steps.deepest_shown} has a value at "
                f"every {deepest_layer}, so its points are joined."
            ),
        )
    return OverlayJoin(
        join=False,
        automatic=True,
        reason=(
            f"Not repeated measures: a {steps.deepest_shown} belongs to one "
            f"{deepest_layer}, so nothing joins its points across the axis."
        ),
    )


def overlay_granularity(steps: OverlaySteps, join: OverlayJoin) -> str:
    """The sentence the panel shows under the checkboxes: what one point is,
    what was averaged to get it, and whether the points are joined."""
    what = f"One point per {' · '.join(steps.shown)}"
    if steps.averaged:
        text = f"{what}; {', '.join(steps.averaged)} averaged within it."
    else:
        text = f"{what} — the raw data."
    lines = "Lines join the points." if join.join else "Points only."
    return f"{text} {lines} {join.reason}"


# ---------------------------------------------------------------------------
# How the grouping list reads for a kind
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GroupingLayers:
    """One reading of the grouping list for one kind.

    ``ticks`` is in **drawing order — outermost first** — because that is
    what ``xaxis.plan_x_axis``, the bracket arithmetic and the composed leaf
    key were written against; ``series`` keeps the spec's innermost-first
    order. Nobody reverses either list ad hoc; this is the one place the
    innermost-first spec meets the outermost-first axis.

    The coloured layer is **not a tick**: a mark's colour level is drawn by
    dodging inside its tick and labelled by the legend (``COLOR`` in the
    panel frame), never by composing into the leaf key — so ``ticks`` is
    exactly what the axis labels, and what ``MAX_X_LAYERS`` caps. It IS part
    of a series id: one line per leaf group needs every layer.

    ``units`` are the SAMPLE keys a line / spaghetti draws one polyline each
    for (:data:`UNIT_KINDS`), innermost first like ``series``. They are part
    of the series id (:attr:`identity`) and nothing else: never a dash style,
    never a legend entry — thirty subjects' lines share their group's colour
    and dash, exactly as seaborn's ``units=`` draws them.
    """

    ticks: list[str]
    series: list[str]
    color: str | None
    units: list[str] = field(default_factory=list)

    @property
    def identity(self) -> list[str]:
        """Every layer of one polyline's id, innermost first: the units inside
        the grouping's series layers (``"groupA | 01"`` once composed
        outermost-first)."""
        return [*self.units, *self.series]

    @property
    def labelled_ticks(self) -> list[str]:
        """The tick layers labelled below the axis — ``ticks`` itself; kept
        as a name because that is what ``MAX_X_LAYERS`` is a cap on."""
        return list(self.ticks)


def grouping_layers(
    spec: PlotSpec,
    table: LongTable,
    roles: dict[str, Role] | None = None,
    kind: PlotKind | None = None,
    shape: Shape | None = None,
) -> GroupingLayers:
    """
    How ``spec.groups`` reads for ``kind`` on this table's measure.

    * categorical x (a scalar measure, no ``x_measure``): every layer is a
      nested tick, first entry innermost;
    * **spaghetti**: the first entry is the lines — one polyline per level,
      joining its points across the layer just above it — and the rest are
      ticks;
    * ``x_measure`` set, or a 1-D measure (x is the sample index): every
      layer is a series, one line / band / point set per leaf group;
    * 2-D: nothing may group (``validate`` says so); both lists empty.

    ``table`` must be the table the figure is drawn from — after the cell
    collapse — so a 1-D measure drawn as a violin reads as the scalar it has
    become; a caller holding the raw table passes the effective ``shape``
    instead (``codegen`` does).
    """
    assignment = complete_assignment(spec, table)
    roles = roles if roles is not None else assignment.roles
    groups = spec.ordered_groups(roles, table.factor_depths)
    color = spec.color if spec.color in groups else (
        assignment.color if assignment.color in groups else None
    )
    kind = kind or spec.kind
    if shape is None:
        shape = table.shape_of(spec.y_measure) if spec.measures else Shape.UNKNOWN

    if shape is Shape.MATRIX_2D:
        return GroupingLayers(ticks=[], series=[], color=color)
    # One polyline per sample level for the kinds that draw lines — see
    # UNIT_KINDS. Read off the one collapse chain, so a pooled spec draws one
    # line per (trial, subject) row, which is what its sample is.
    units: list[str] = []
    if kind in UNIT_KINDS:
        sample = collapse_steps(spec, roles, table).sample
        # A spaghetti joins its lines across the ticks: only a sample that
        # recurs there can be a line (see spaghetti_sample_repeats).
        if kind is not PlotKind.SPAGHETTI or spaghetti_sample_repeats(
            spec, roles, table, sample
        )[0]:
            units = [name for name in sample if name not in groups]
    if spec.x_measure is not None or shape is Shape.SERIES_1D:
        return GroupingLayers(ticks=[], series=list(groups), color=color, units=units)
    if kind is PlotKind.SPAGHETTI:
        lines, rest = groups[:1], groups[1:]
        ticks = [name for name in reversed(rest) if name != color]
        return GroupingLayers(ticks=ticks, series=list(lines), color=color, units=units)
    ticks = [name for name in reversed(groups) if name != color]
    return GroupingLayers(ticks=ticks, series=[], color=color, units=units)


def tick_layers(
    spec: PlotSpec, table: LongTable, roles: dict[str, Role] | None = None
) -> list[str]:
    """The nested x layers, outermost first (:class:`GroupingLayers`)."""
    return grouping_layers(spec, table, roles).ticks


def series_layers(
    spec: PlotSpec, table: LongTable, roles: dict[str, Role] | None = None
) -> list[str]:
    """The series-identity layers, innermost first (:class:`GroupingLayers`)."""
    return grouping_layers(spec, table, roles).series


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def layer_cap_reason(
    spec: PlotSpec,
    table: LongTable,
    roles: dict[str, Role] | None = None,
    kind: "PlotKind | None" = None,
    shape: Shape | None = None,
) -> str | None:
    """Why the grouping has too many labelled tick layers for ``kind``, or None.

    The one statement of the ``MAX_X_LAYERS`` cap, shared by ``validate``
    (which refuses the spec) and ``capability.capabilities`` (which greys the
    kind out with this text). It is per KIND because the cap is on labelled
    TICKS, and a kind decides whether the grouping list is ticks or series:
    four grouping layers are four series on a 1-D line and four nested ticks
    on the box that cell-collapses the same measure. Until 2026-09-19 only
    ``validate`` applied it, so the panel offered a bar that then failed to
    draw (found by the role-assignment sweep, plan B1).
    """
    layers = grouping_layers(spec, table, roles, kind, shape=shape)
    if len(layers.labelled_ticks) > MAX_X_LAYERS:
        return (
            f"At most {MAX_X_LAYERS} labelled tick layers fit on the x axis; "
            f"got {len(layers.labelled_ticks)}: {layers.labelled_ticks}. A "
            f"fourth level of nesting cannot be read off an axis -- colour one "
            f"of them, or move one to 'facet' (separate panels)."
        )
    return None


def kind_requirement(
    kind: PlotKind, shape: Shape, roles: dict[str, Role], n_groups: int
) -> str | None:
    """Why ``kind`` cannot be drawn with these roles, or None when it can.

    The one statement of what each kind needs from the assignment, shared by
    ``validate`` (which refuses the spec) and ``capability.why_unavailable``
    (which greys the option out with this text) — so the kind list and the
    renderer can never disagree about a kind.
    """
    if kind in (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAND) and not has_sample(roles):
        what = "an error band" if kind is PlotKind.BAND else "a distribution"
        return (
            f"Needs a sample: set at least one factor to 'collapse' — its "
            f"levels (after any inner collapse) are what {what} is drawn over."
        )
    if kind is PlotKind.SPAGHETTI and n_groups < 2:
        return (
            "Needs two grouping layers: the first is the lines (one per "
            "subject, say) and the second is the x positions they join across."
        )
    return None


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
            f"'Variant' factor and can take a grouping layer or a facet. For "
            f"an x-y plot, set x_measure."
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

    shape = table.shape_of(spec.y_measure)
    assignment = complete_assignment(spec, table)
    roles = assignment.roles

    # --- colour names a grouping layer -----------------------------------
    if spec.color is not None and spec.color not in assignment.groups:
        raise RoleError(
            f"color={spec.color!r} is not a grouping layer (groups: "
            f"{assignment.groups}). Colour labels a layer that already groups "
            f"the marks — add {spec.color!r} to the grouping first."
        )

    # --- grouping per shape ----------------------------------------------
    if shape is Shape.MATRIX_2D and assignment.groups:
        raise RoleError(
            f"Measure {spec.y_measure!r} is 2-D (heatmap); its axes come from "
            f"the matrix itself, so {assignment.groups} cannot group it. Use "
            f"'facet' (separate panels) or 'iterate' (separate figures)."
        )
    cap = layer_cap_reason(spec, table, roles)
    if cap is not None:
        raise RoleError(cap)

    # --- the kind's own needs --------------------------------------------
    requirement = kind_requirement(spec.kind, shape, roles, len(assignment.groups))
    if requirement is not None:
        raise RoleError(f"Kind {spec.kind} cannot be drawn: {requirement}")

    # --- "Show sample" names real factors --------------------------------
    #
    # A name that is a factor but not collapsed right now is INERT (ignored
    # by `overlay_steps`, reported by capability), the same way
    # `cell_statistic` is inert on a line — the checkbox state must never be
    # a state the spec has to adjudicate. A name that is no factor at all is
    # a typo or a stale spec, and that IS refused.
    unknown_shown = [name for name in spec.show_sample if not table.has_factor(name)]
    if unknown_shown:
        raise RoleError(
            f"show_sample names unknown factor(s) {unknown_shown}. "
            f"Table factors: {table.factor_names}"
        )
    if spec.show_sample:
        from .cell import effective_shape  # the shape the FIGURE draws

        reason = overlay_unavailable(spec, roles, effective_shape(spec, table))
        if reason is not None:
            Log.debug("show_sample %s inert: %s", spec.show_sample, reason, layer=LAYER)
    # Same split for the overlay's own colour: a typo is refused, a key that
    # is a factor but not shown right now is inert (`overlay_color`).
    if spec.sample_color is not None and not table.has_factor(spec.sample_color):
        raise RoleError(
            f"sample_color names unknown factor {spec.sample_color!r}. "
            f"Table factors: {table.factor_names}"
        )

    # --- variants are never replicates -----------------------------------
    #
    # Two pipelines' results averaged into one number is not a figure anyone
    # asked for; a variant factor takes a grouping layer, a facet or a figure.
    # (Defaults never collapse a variant — `complete_assignment` — so this can
    # only be reached by an explicit role.)
    pooled = [
        f.name
        for f in table.variant_factors
        if len(f.levels) > 1 and roles.get(f.name) is Role.COLLAPSE
    ]
    if pooled:
        raise RoleError(
            f"Variant factor(s) {pooled} cannot be collapsed: their levels are "
            f"different pipeline variants, not replicates, so averaging them "
            f"mixes results. Give them a grouping layer, 'facet' or 'iterate', "
            f"or select the variant you want with PlotSpec.variant_sets."
        )

    # --- variables must never be pooled at all ----------------------------
    #
    # `Variable` is a factor only when the figure draws more than one (see
    # `variants._answered`), and then it must SEPARATE them. Averaging EMG with
    # force is not a figure anyone wants.
    if table.has_factor(VARIABLE_COLUMN) and roles.get(VARIABLE_COLUMN) is Role.COLLAPSE:
        raise RoleError(
            f"{VARIABLE_COLUMN!r} cannot be collapsed: its levels are different "
            f"variables, so averaging them combines unrelated quantities. Give "
            f"it a grouping layer, 'facet' or 'iterate' to keep them apart."
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


# ---------------------------------------------------------------------------
# Kind changes
# ---------------------------------------------------------------------------


def roles_for_kind(
    spec: PlotSpec, table: LongTable, kind: "PlotKind"
) -> Assignment | None:
    """The assignment ``kind`` should open with, or None to keep the current one.

    A 1-D measure opens with every schema key on ITERATE (one record per
    figure); a scalar measure opens with its own level grouped and the rest
    iterated. Selecting a scalar kind for a 1-D measure moves it from the
    first world to the second — and with everything iterated there is no
    sample, so box and violin would stay greyed out and the first click on
    Violin would appear to do nothing.

    So the assignment is re-defaulted, but **only when it is still the
    untouched default for the shape it was built for**. A user who has
    assigned roles by hand keeps every one of them; this never overwrites a
    decision, which is why the test is equality against
    ``default_assignment`` rather than a heuristic about which roles "look
    default".

    ``table`` is the derived table BEFORE any cell collapse — this has to see
    the measure as the data holds it to know that a shape change is happening.
    """
    from .cell import effective_shape

    if not spec.measures or spec.y_measure not in table.measure_names:
        return None

    current = effective_shape(spec, table)
    proposed = effective_shape(replace(spec, kind=kind), table)
    if current is proposed:
        return None

    measure = spec.y_measure
    if Assignment(spec.roles, spec.groups, spec.color) != default_assignment(
        table, measure, shape=current
    ):
        return None
    return with_requirements_for(
        default_assignment(table, measure, shape=proposed), table, kind
    )


def with_requirements_for(
    assignment: Assignment, table: LongTable, kind: "PlotKind"
) -> Assignment:
    """Give ``kind`` what it needs to draw anything, changing as little as possible.

    * A distribution kind (box, violin, band) needs a sample. If nothing is
      collapsed, the **deepest plain factor not already grouping** is
      collapsed — with the opening state that is the key just outside the
      grouped one (``speed`` beside a grouped ``trial``), so the sample is the
      closest-to-the-data key still available and the figure count shrinks by
      exactly one factor. Not the grouped key itself: that is the mark the
      user is looking at.
    * Spaghetti needs two grouping layers. If it has fewer, the deepest plain
      iterated factor becomes the innermost layer — the lines.

    Only a PLAIN factor is eligible. Collapsing a variant factor pools two
    pipelines' results, which ``validate`` refuses outright — doing it here
    would answer a greyed-out kind with an error message, which is worse.
    """
    roles = dict(assignment.roles)
    groups = list(assignment.groups)
    plain = [f.name for f in _plain(table)]
    candidates = [n for n in plain if roles.get(n) is Role.ITERATE]

    if kind in (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAND) and not has_sample(roles):
        chosen = _deepest(candidates, table)
        if chosen is not None:
            roles[chosen] = Role.COLLAPSE
    elif kind is PlotKind.SPAGHETTI and len(groups) < 2:
        chosen = _deepest(candidates, table)
        if chosen is not None:
            roles[chosen] = Role.GROUP
            groups.insert(0, chosen)
    return Assignment(roles=roles, groups=groups, color=assignment.color)


def role_for_new_grouping(
    spec: PlotSpec, table: LongTable, factor: str | None = None
) -> Role:
    """The role a grouping the user has just ticked should take.

    ``factor`` names it when it is already in the spec; ``None`` asks the same
    question about a grouping not yet added, which is what ``capability``
    publishes so the panel can apply the answer without a round trip per
    checkbox.

    A grouping ticked in the Grouping section IS a request for one mark per
    level, so the answer is ``GROUP`` — placed innermost by ``ordered_groups``'
    depth rule — unless the labelled tick layers are already at
    ``MAX_X_LAYERS``, in which case it becomes a facet (separate panels),
    which still keeps the levels apart. Returning a role that quietly does
    nothing is the failure this function exists to prevent.

    A factor that already carries a role keeps it: re-ticking something, or
    re-applying a picker, must never overwrite a decision already made.
    """
    existing = spec.roles.get(factor) if factor else None
    if existing is not None:
        return existing

    layers = grouping_layers(spec, table)
    if layers.ticks and len(layers.labelled_ticks) >= MAX_X_LAYERS:
        return Role.FACET
    return Role.GROUP


def default_spec(table: LongTable, measure: str | None = None) -> PlotSpec:
    """
    The spec a table opens on: default roles, the matching default kind, and a
    facet grid wide enough for however many fields the measure has.

    Lives here rather than in the GUI so the panel and a library caller open on
    the same figure (CLAUDE.md NOTE 3).
    """
    from .capability import default_plot
    from .spec import FacetOptions, PlotKind, VariantSet, YAxis, grid_shape_for

    from .variants import apply_variant_sets, default_selection

    measure = measure or (table.measure_names[0] if table.measures else None)
    if measure is None:
        raise RoleError("This table has no measures to plot.")

    # Open on ONE variant, as a single row — ALWAYS, even when there is nothing
    # to pin.
    #
    # Every variant axis is pinned, not just the code ones: latest body, first
    # parameter value (`variants.default_selection` owns the rule and states it
    # in full). Pinning only "current code" left a swept parameter unanswered,
    # so `default_assignment` found a multi-level variant factor with no role
    # and grouped it — a variable produced at 5 cutoffs opened as 5 overlaid
    # series before the user had said anything at all. Comparing is what a
    # second row is for.
    #
    # The row is seeded unconditionally because it is what the Variants section
    # SHOWS: a project with no variant axes at all used to open with an empty
    # list, so the section said nothing about the variable being plotted and
    # the user's first row appeared only once they added a second (the GUI was
    # seeding row 0 lazily, in TypeScript — a NOTE 3 violation this removes).
    #
    # It carries no `variable`: None means "the primary measure", which keeps
    # the row INERT while its selection is empty (`defined_sets`). Naming the
    # measure explicitly would make every table grow a one-level `Variant`
    # factor that says nothing.
    #
    # The name is left to `set_name`, which builds it from the variable and the
    # selection — "FilteredEMG", or "FilteredEMG · current" once there are code
    # versions to be current among. The old hardcoded "current" named the pin
    # after the least interesting thing about it.
    variant_sets = [VariantSet(selection=default_selection(table))]

    # Roles describe the table AS RESOLVED, so they are derived after the
    # variants are chosen — never before. Defaulting against the undecided table
    # put a role on a `Code:<fn>` factor that the opening variant then answered,
    # and `validate` calls a role on a missing factor an unknown factor and
    # refuses to draw anything. (`strip_answered_roles` catches the same thing
    # arriving from a saved spec; this stops it being created here at all.)
    resolved = apply_variant_sets(
        PlotSpec(measures=[measure], variant_sets=variant_sets), table
    )
    assignment = default_assignment(resolved, measure)
    roles = assignment.roles
    kind = default_plot(
        resolved.shape_of(measure), roles, n_groups=len(assignment.groups)
    ) or PlotKind.SCATTER

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

    # Y limits per PANEL from the start: every factor that separates figures or
    # subplots is in the scope, so each panel autoscales to its own data and
    # the limits are computed over one panel's samples at a time — not one
    # range over the whole dataset, which on a large variable was most of the
    # opening cost (`build_plan y_limits`). The user unchecks factors to share.
    y_scope = [
        f.name for f in resolved.factors if roles.get(f.name) in (Role.ITERATE, Role.FACET)
    ]

    return assignment.apply(
        PlotSpec(
            measures=[measure],
            kind=kind,
            facet=FacetOptions(n_cols=n_cols),
            variant_sets=variant_sets,
            y_axis=YAxis(scope=y_scope),
        )
    )
