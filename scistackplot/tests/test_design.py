"""design.py: the one owner of "which plot settings are the data design"."""

from __future__ import annotations

from dataclasses import replace

from scistackplot.design import (
    DESIGN_FIELDS,
    SyncClass,
    copy_design,
    design_diff,
    design_fingerprint,
    design_of,
    unclassified_fields,
)
from scistackplot.spec import (
    Aggregation,
    ErrorBand,
    Filter,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
)

from test_restore import full_spec


def _spec(**changes) -> PlotSpec:
    base = dict(
        measures=["Ratings"],
        roles={"brand": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["brand"],
        color="brand",
        kind=PlotKind.BAR,
    )
    base.update(changes)
    return PlotSpec(**base)


def test_every_plot_field_is_classified():
    """A new PlotSpec field must say whether it is design (syncs) or display."""
    missing, stale = unclassified_fields()
    assert missing == [], f"classify these in design.DESIGN_FIELDS: {missing}"
    assert stale == [], f"these DESIGN_FIELDS entries name no field: {stale}"


def test_display_changes_do_not_move_the_fingerprint():
    base = _spec()
    for changed in (
        replace(base, kind=PlotKind.BOX),
        replace(base, aggregate=Aggregation(error=ErrorBand.CI95)),
        replace(base, style=StyleOptions(title="Popcorn")),
        replace(base, color=None),
    ):
        assert design_fingerprint(changed) == design_fingerprint(base)
        assert design_diff(base, changed) == []


def test_design_changes_move_the_fingerprint_and_are_labelled():
    base = _spec()
    changed = replace(
        base,
        roles={"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["brand", "session"],
        aggregate=Aggregation(pooled=True),
        filters=[Filter(column="subject", exclude=["S03"])],
    )
    assert design_fingerprint(changed) != design_fingerprint(base)
    diff = {d.path: d for d in design_diff(base, changed)}
    assert set(diff) == {"roles", "groups", "aggregate.pooled", "filters"}
    assert diff["groups"].label == "Grouping order"
    assert diff["groups"].a == "brand"
    assert diff["groups"].b == "session × brand", "read outermost first"
    assert diff["aggregate.pooled"].b == "on"


def test_role_order_is_not_a_difference():
    a = _spec(roles={"brand": Role.GROUP, "subject": Role.COLLAPSE})
    b = _spec(roles={"subject": Role.COLLAPSE, "brand": Role.GROUP})
    assert design_diff(a, b) == []


def test_copy_design_takes_design_and_keeps_display():
    source = _spec(
        roles={"brand": Role.ITERATE, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        aggregate=Aggregation(pooled=True),
    )
    target = _spec(kind=PlotKind.BOX, style=StyleOptions(title="kept"))
    result, notes = copy_design(source, target)
    assert design_diff(result, source) == []
    assert result.kind is PlotKind.BOX and result.style.title == "kept"
    assert result.aggregate.error == target.aggregate.error, "display half of a split block stays"
    assert result.color is None, "brand is no longer a grouping layer"
    assert [n.path for n in notes] == ["color"]


def test_copy_design_of_every_field():
    """Every design field of a fully non-default spec arrives."""
    source = full_spec()
    result, _ = copy_design(source, PlotSpec(measures=["Other"]))
    assert design_of(result) == design_of(source)


def test_design_paths_cover_the_documented_sync_list():
    design = {p for p, c in DESIGN_FIELDS.items() if c is SyncClass.DESIGN}
    for path in ("measures", "variant_sets", "roles", "groups", "aggregate.pooled", "filters",
                 "location_filter", "factor_variables", "level_groups"):
        assert path in design
    for path in ("kind", "color", "style", "aggregate.error", "aliases", "colors"):
        assert path not in design
