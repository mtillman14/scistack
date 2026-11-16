"""The opening figure of a ``where=`` check (issue #13).

A check is an ordinary Plot Studio figure plus Yes/No factors
(``scistackplot.model.where_check``; the source attaches them). This module
is the ONE place that decides how such a figure OPENS, from three facts the
canvas hands over: the node's filters, which filter was clicked, and whether
the click means "histogram of the filter's variable" or "plot the node's input
against the filter's variable".

Not the owner of anything else: the verdict is scidb's
(``filter_report``), the factors are the source's, the drawing is
scistackplot's. See docs/claude/where-in-gui.md.

Opening specs, by what the click asked:

* **histogram** (click on a chip): measure = the filter's variable, Histogram,
  every schema key collapsed, painted by Matching;
* **against, numeric filter**: measure = the node's input, ``x_measure`` = the
  filter's variable, Scatter, keys collapsed (a point per location);
* **against, categorical filter**: measure = the node's input, the filter's
  variable joined as a grouping (x ticks), Box, "Show sample" on the keys.
"""

from __future__ import annotations

import logging
from dataclasses import replace

logger = logging.getLogger(__name__)

__all__ = ["check_from_payload", "opening_check_spec"]


def check_from_payload(payload: dict):
    """The :class:`WhereCheck` of a request's ``where_check`` dict."""
    from scistackplot.model.where_check import WhereCheck

    return WhereCheck.from_dict(payload)


def _is_numeric_filter(row) -> bool:
    """Whether the filter's variable is a number: read from the comparison
    value, so no data is touched. ``classify_value`` is the one rule for
    numeric vs categorical (``scistackplot.model.shape``)."""
    from scidb.model.filters import filter_value
    from scistackplot.model.shape import Shape, classify_value

    return classify_value(filter_value(row.value)) is Shape.SCALAR


def opening_check_spec(
    source,
    variable: str,
    base_spec,
    check_payload: dict,
    *,
    table_for,
):
    """``(spec, table)``: the opening spec of a check on *variable*, and the
    table it is drawn from.

    *base_spec* is the variable's ordinary opening spec (kept for everything
    this module does not decide: variant pins, wide-field filters, facet
    wrap). *table_for* is ``plot_service._table_for``, passed in so this
    module need not import the hot spot.
    """
    from scistackplot import default_spec
    from scistackplot.model.roles import Role
    from scistackplot.model.shape import Shape
    from scistackplot.model.spec import FactorVariable, PlotKind
    from scistackplot.model.where_check import (
        CHECK_COLORS,
        MATCHING_FACTOR,
    )

    check = check_from_payload(check_payload)
    if not check:
        raise ValueError("A where= check needs at least one filter.")
    index = int(check_payload.get("filter_index") or 0)
    against = bool(check_payload.get("against"))
    index = min(max(index, 0), len(check.filters) - 1)
    focus = check.filters[index]

    # The deciding facts, BEFORE any table: x / grouping / kind are inputs to
    # the table build, not edits after it.
    x_measure = None
    factor_variables: list = []
    kind = PlotKind.HISTOGRAM
    show_sample_keys = False
    if against:
        if _is_numeric_filter(focus):
            x_measure = focus.variable
            kind = PlotKind.SCATTER
        else:
            factor_variables = [FactorVariable(focus.variable, focus.column)]
            kind = PlotKind.BOX
            show_sample_keys = True

    seeded = replace(
        base_spec,
        where_check=check,
        x_measure=x_measure,
        factor_variables=factor_variables,
    )
    table = table_for(source, seeded)
    # Roles of the OTHER factors (variant axes, wide fields) come from the
    # ordinary defaults over this table; the check only decides the keys and
    # its own factors.
    defaults = default_spec(table, variable)
    if kind is PlotKind.HISTOGRAM and table.shape_of(variable) is not Shape.SCALAR:
        # A text variable has no bins: it opens the way it ordinarily would
        # (a Count bar), still painted by Matching.
        kind = defaults.kind
    roles = dict(defaults.roles)
    keys = [f.name for f in table.factors if table.is_schema_key(f.name)]
    for key in keys:
        roles[key] = Role.COLLAPSE
    roles[MATCHING_FACTOR] = Role.GROUP
    groups = [MATCHING_FACTOR]
    for group in factor_variables:
        roles[group.factor_name] = Role.GROUP
        groups.append(group.factor_name)
    # One layer per filter beyond Matching: separate figures, so the default
    # draw is one clean figure per question rather than a pile of panels.
    for factor in table.factors:
        if factor.name not in roles:
            roles[factor.name] = Role.ITERATE

    spec = replace(
        seeded,
        kind=kind,
        categorical=defaults.categorical,
        roles=roles,
        groups=groups,
        color=MATCHING_FACTOR,
        show_sample=list(keys) if show_sample_keys else [],
        colors={**base_spec.colors, MATCHING_FACTOR: dict(CHECK_COLORS)},
        facet=defaults.facet,
        variant_sets=defaults.variant_sets,
        filters=defaults.filters,
        y_axis=replace(
            defaults.y_axis,
            scope=[n for n, r in roles.items() if r in (Role.ITERATE, Role.FACET)],
        ),
    )
    logger.info(
        "[where_check] open variable=%s node=%s filters=%s focus=%d against=%s "
        "kind=%s x=%s groups=%s",
        variable,
        check.node_id,
        [f.to_dict() for f in check.filters],
        index,
        against,
        kind,
        x_measure,
        groups,
    )
    return spec, table
