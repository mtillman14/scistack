"""
Grouping variables: a variable joined in as a FACTOR rather than plotted.

A subject-level ``Condition`` holding stim/sham is grouping the study already
records. It classifies as CATEGORICAL — rightly refused as a *measure*, since
there is no numeric axis for it — but as a factor it is exactly what "plot stim
against sham" needs, with no new concept beyond the broadcast join
:mod:`scistackplotdb.hierarchy` already performs for an x measure.
"""

from __future__ import annotations

import pytest
from scistackplot import PlotKind, PlotSpec, Role, Shape, capabilities, resolve

from scistackplotdb import ScidbSource

pytest.importorskip("seaborn")


def _spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        factor_variables=["Condition"],
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_a_subject_level_variable_broadcasts_to_every_row(seeded):
    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=["Condition"]
    )

    assert table.has_factor("Condition")
    assert len(table.frame) == 12, "the join must not multiply rows"
    # One condition per subject, reused across that subject's 4 trial rows.
    assert table.frame.groupby("subject")["Condition"].nunique().max() == 1
    assert set(table.frame["Condition"]) == {"stim", "sham"}


def test_the_grouping_variable_takes_a_role(seeded):
    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=["Condition"]
    )
    spec = _spec(
        roles={
            "Condition": Role.COLOR,
            "session": Role.X,
            "subject": Role.FREE,
            "trial": Role.FREE,
        }
    )

    figures = resolve(spec, table)

    assert len(figures) == 1
    assert {str(v) for v in figures[0].color_order} == {"stim", "sham"}


def test_an_uneven_split_keeps_every_row(seeded):
    """2 stim subjects, 1 sham — a merge that dropped rows would still look
    plausible on symmetric counts."""
    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=["Condition"]
    )

    counts = table.frame["Condition"].value_counts().to_dict()
    assert counts == {"stim": 8, "sham": 4}


def test_a_categorical_variable_is_still_not_a_measure(seeded):
    """Right as a factor, wrong as something to put on a numeric axis."""
    source = ScidbSource(seeded)

    assert source._shape_of("Condition") is Shape.CATEGORICAL
    catalog = {m["name"]: m for m in source.describe()["measures"]}
    assert catalog["Condition"]["plottable"] is False


def test_groupable_with_offers_it_and_leads_with_categoricals(seeded):
    partners = ScidbSource(seeded).groupable_with("StepLength")

    assert partners[0] == "Condition", "categorical groups lead"
    # Mass is subject-level and numeric: offered, not guessed at — a group
    # coded 1/2 is still a group.
    assert "Mass" in partners
    # Deeper than the data has no unambiguous per-row value.
    assert "Emg" not in partners


def test_a_deeper_variable_cannot_group(seeded):
    """Grouping trial-level data by something recorded per trial-and-more would
    multiply rows; say so instead of quietly inflating n."""
    with pytest.raises(ValueError, match="not a prefix"):
        ScidbSource(seeded).get_table(["Mass"], factor_variables=["StepLength"])


def test_a_struct_variable_cannot_group(seeded):
    with pytest.raises(ValueError, match="no single value to group by"):
        ScidbSource(seeded).get_table(["StepLength"], factor_variables=["Emg"])


def test_capabilities_offer_the_joined_factor(seeded):
    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=["Condition"]
    )

    names = [f["name"] for f in capabilities(_spec(), table)["factors"]]

    assert "Condition" in names


def test_the_endpoint_passes_the_group_as_its_own_input(seeded):
    """`as_table` hands a function schema keys and data columns only, so a
    subject-level Condition cannot ride along on a trial-level frame."""
    from scistackplotdb import generate_endpoint

    table = ScidbSource(seeded).get_table(
        ["StepLength"], factor_variables=["Condition"]
    )
    code = generate_endpoint(_spec(), table, input_variable="StepLength")

    assert '"group_condition": Condition,' in code.foreach_source
    assert "group_condition" in code.function_source
    assert "merge(" in code.function_source
    compile(code.source, "<generated>", "exec")


def test_generated_code_reproduces_the_join(seeded):
    """The exported figure must be the previewed one."""
    import matplotlib.pyplot as plt

    from scistackplot import generate_plot_function

    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"], factor_variables=["Condition"])
    spec = _spec(
        roles={
            "Condition": Role.X,
            "subject": Role.FREE,
            "session": Role.FREE,
            "trial": Role.FREE,
        }
    )
    generated = generate_plot_function(spec, table)

    namespace: dict = {}
    exec(compile(generated, "<generated>", "exec"), namespace)  # noqa: S102

    # The frames the endpoint would receive: the measure without the joined
    # column, and the grouping variable on its own.
    keys = ["subject", "session", "trial"]
    df = table.frame[[*keys, "StepLength"]]
    groups = table.frame[["subject", "Condition"]].drop_duplicates()

    figure = namespace["plot_steplength"](df, groups, "figure.png")
    assert figure.axes
    plt.close(figure)
