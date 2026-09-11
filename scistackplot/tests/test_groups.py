"""
Derived grouping factors: bucketing a factor's levels into a new one.

``session ∈ {pre, post1, post2}`` → ``Phase ∈ {baseline, post}``, without
editing data. A derived table like ``apply_variant_sets``, so everything
downstream sees one ordinary factor (docs/claude/synthetic-factors.md).
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, capabilities, resolve
from scistackplot.groups import apply_level_groups
from scistackplot.spec import LevelGroup

PHASES = {"pre": "baseline", "post": "post"}


@pytest.fixture
def phased_table(scalar_frame) -> LongTable:
    """Four session levels that bucket into two phases."""
    frames = []
    for session in ["pre", "post1", "post2"]:
        frames.append(scalar_frame.assign(session=session))
    return LongTable.from_frame(
        pd.concat(frames, ignore_index=True),
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        schema_levels=["subject", "session", "trial"],
    )


def _group(**kwargs) -> LevelGroup:
    base = dict(
        name="Phase",
        source="session",
        mapping={"pre": "baseline", "post1": "post", "post2": "post"},
        unmatched=None,
    )
    base.update(kwargs)
    return LevelGroup(**base)


def test_levels_bucket_into_a_new_factor(phased_table):
    spec = PlotSpec(measures=["StepLength"], level_groups=[_group()])

    derived = apply_level_groups(spec, phased_table)

    assert derived.has_factor("Phase")
    assert [str(level) for level in derived.factor("Phase").levels] == [
        "baseline",
        "post",
    ]
    mapped = derived.frame.groupby("Phase")["session"].nunique().to_dict()
    assert mapped == {"baseline": 1, "post": 2}


def test_the_source_factor_survives(phased_table):
    """Unlike a variant selection, both are independently useful.

    Sessions along x, phases in colour is a real figure — and neither is a
    variant factor, so no pooling guard is involved.
    """
    spec = PlotSpec(measures=["StepLength"], level_groups=[_group()])

    derived = apply_level_groups(spec, phased_table)

    assert derived.has_factor("session")
    assert derived.has_factor("Phase")


def test_unmatched_levels_are_dropped_by_default(phased_table):
    """"Just these two groups, ignore the rest" is the common intent."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"})],
    )

    derived = apply_level_groups(spec, phased_table)

    assert set(derived.frame["session"]) == {"pre"}
    assert set(derived.frame["Phase"]) == {"baseline"}


def test_unmatched_levels_can_go_to_a_bucket(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"}, unmatched="other")],
    )

    derived = apply_level_groups(spec, phased_table)

    assert set(derived.frame["Phase"]) == {"baseline", "other"}
    # The catch-all sorts last, after the buckets the user named.
    assert [str(v) for v in derived.factor("Phase").levels] == ["baseline", "other"]
    assert len(derived.frame) == len(phased_table.frame)


def test_a_group_never_leaves_rows_unlabelled(phased_table):
    """A NaN group would become its own silent series in every legend."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"}, unmatched="other")],
    )

    derived = apply_level_groups(spec, phased_table)

    assert derived.frame["Phase"].notna().all()


def test_level_order_is_declared_not_observed(phased_table):
    """A filter removing a group's last row must not reorder the legend."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[
            _group(mapping={"post1": "post", "post2": "post", "pre": "baseline"})
        ],
    )

    derived = apply_level_groups(spec, phased_table)

    assert [str(v) for v in derived.factor("Phase").levels] == ["post", "baseline"]


def test_a_group_reading_a_missing_column_is_skipped(phased_table):
    """A spec outlives the table it was written against."""
    spec = PlotSpec(
        measures=["StepLength"], level_groups=[_group(source="gone")]
    )

    derived = apply_level_groups(spec, phased_table)

    assert not derived.has_factor("Phase")
    assert len(derived.frame) == len(phased_table.frame)


def test_no_groups_returns_the_same_table(phased_table):
    """A project that buckets nothing pays nothing."""
    spec = PlotSpec(measures=["StepLength"])

    assert apply_level_groups(spec, phased_table) is phased_table


# --- it is an ordinary factor from here on ---------------------------------


def test_the_derived_factor_takes_a_role(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "Phase": Role.COLOR,
            "session": Role.X,
            "subject": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )

    figures = resolve(spec, phased_table)

    assert len(figures) == 1
    assert figures[0].encoding.color is not None
    assert {str(v) for v in figures[0].color_order} == {"baseline", "post"}


def test_the_derived_factor_can_iterate(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Phase": Role.ITERATE, "session": Role.X},
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )

    figures = resolve(spec, phased_table)

    assert {f.figure_key["Phase"] for f in figures} == {"baseline", "post"}


def test_capabilities_offer_the_derived_factor(phased_table):
    """The panel builds its role dropdowns from this; a factor missing here
    cannot be assigned at all."""
    spec = PlotSpec(
        measures=["StepLength"], roles={"session": Role.X}, level_groups=[_group()]
    )

    names = [f["name"] for f in capabilities(spec, phased_table)["factors"]]

    assert "Phase" in names


def test_generated_code_reproduces_the_bucketing(phased_table):
    """The endpoint receives the RAW table, so codegen must emit the same map.

    Reshaping done only on the interactive path is how an exported figure stops
    being the previewed one.
    """
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")

    from scistackplot import generate_plot_function

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Phase": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )
    source = generate_plot_function(spec, phased_table)

    assert "'Phase'" in source and "map(_groups)" in source

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_steplength"](phased_table.frame.copy(), "figure.png")
    assert figure.axes
    matplotlib.pyplot.close(figure)
