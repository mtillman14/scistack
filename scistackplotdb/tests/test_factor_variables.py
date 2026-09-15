"""
Grouping variables: a variable — or one column of one — joined in as a FACTOR
rather than plotted.

A subject-level ``Condition`` holding stim/sham is grouping the study already
records. It classifies as CATEGORICAL — rightly refused as a *measure*, since
there is no numeric axis for it — but as a factor it is exactly what "plot stim
against sham" needs, with no new concept beyond the broadcast join
:mod:`scistackplotdb.hierarchy` already performs for an x measure.

The same mechanism reaches a **column** of a wide sheet
(``Demographics.InterventionGroup``), which is the only way a spreadsheet of
participant demographics can stratify a figure: the variable as a whole holds
Age, Sex and InterventionGroup at once and so has no single value to group by.
"""

from __future__ import annotations

import pytest
from scistackplot import (
    MISSING_LEVEL,
    FactorVariable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    capabilities,
    resolve,
)

from scistackplotdb import ScidbSource

pytest.importorskip("seaborn")

CONDITION = [FactorVariable("Condition")]
GROUP = [FactorVariable("Demographics", "InterventionGroup")]


def _spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
        kind=PlotKind.BOX,
        factor_variables=CONDITION,
    )
    base.update(kwargs)
    return PlotSpec(**base)


# ---------------------------------------------------------------------------
# A whole variable as a factor
# ---------------------------------------------------------------------------


def test_a_subject_level_variable_broadcasts_to_every_row(seeded):
    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)

    assert table.has_factor("Condition")
    assert len(table.frame) == 12, "the join must not multiply rows"
    # One condition per subject, reused across that subject's 4 trial rows.
    assert table.frame.groupby("subject")["Condition"].nunique().max() == 1
    assert set(table.frame["Condition"]) == {"stim", "sham"}


def test_the_grouping_variable_takes_a_role(seeded):
    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)
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
    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)

    counts = table.frame["Condition"].value_counts().to_dict()
    assert counts == {"stim": 8, "sham": 4}


def test_a_categorical_variable_is_still_not_a_measure(seeded):
    """Right as a factor, wrong as something to put on a numeric axis."""
    source = ScidbSource(seeded)

    assert source._shape_of("Condition") is Shape.CATEGORICAL
    catalog = {m["name"]: m for m in source.describe()["measures"]}
    assert catalog["Condition"]["plottable"] is False


def test_a_deeper_variable_cannot_group(seeded):
    """Grouping trial-level data by something recorded per trial-and-more would
    multiply rows; say so instead of quietly inflating n."""
    with pytest.raises(ValueError, match="not a prefix"):
        ScidbSource(seeded).get_table(
            ["Mass"], factor_variables=[FactorVariable("StepLength")]
        )


def test_a_struct_variable_cannot_group_without_a_column(seeded):
    """The refusal has to name the way forward — the column — or it reads as
    "wide tables cannot group", which is exactly what this feature does."""
    with pytest.raises(ValueError, match="no single value to group by"):
        ScidbSource(seeded).get_table(
            ["StepLength"], factor_variables=[FactorVariable("Emg")]
        )


def test_capabilities_offer_the_joined_factor(seeded):
    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)

    names = [f["name"] for f in capabilities(_spec(), table)["factors"]]

    assert "Condition" in names


# ---------------------------------------------------------------------------
# One COLUMN of a wide sheet as a factor
# ---------------------------------------------------------------------------


def test_a_column_of_a_wide_sheet_becomes_a_factor(with_demographics):
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )

    assert table.has_factor("InterventionGroup")
    assert len(table.frame) == 12, "the join must not multiply rows"
    # The factor carries the COLUMN's own name, not a qualified one: that is
    # also the name the endpoint's `as_table` input arrives under, which is why
    # the generated merge needs no rename.
    assert "Demographics.InterventionGroup" not in table.frame.columns
    assert table.frame.groupby("subject")["InterventionGroup"].nunique().max() == 1


def test_only_the_named_column_is_joined(with_demographics):
    """The sheet's other columns are not the question being asked, and each one
    carried in would be another factor demanding a role."""
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )

    assert "Age" not in table.frame.columns
    assert "Sex" not in table.frame.columns


def test_a_subject_missing_from_the_sheet_keeps_its_rows(with_demographics):
    """Subject 03 is in the study and not in the spreadsheet. Dropping their
    trials would answer a grouping question by deleting data, silently."""
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )

    counts = table.frame["InterventionGroup"].value_counts().to_dict()
    assert counts == {"Onward": 4, "Digitimer": 4, MISSING_LEVEL: 4}


def test_the_missing_level_sorts_last(with_demographics):
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )

    assert table.factor("InterventionGroup").levels[-1] == MISSING_LEVEL


def test_missing_values_are_reported(with_demographics, caplog):
    import logging

    source = ScidbSource(with_demographics)
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        source.get_table(["StepLength"], factor_variables=GROUP)

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "Demographics.InterventionGroup" in text
    assert "4 of 12" in text


def test_the_column_takes_a_role_like_any_factor(with_demographics):
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )
    spec = _spec(
        factor_variables=GROUP,
        kind=PlotKind.BAR,
        roles={
            "InterventionGroup": Role.X,
            "subject": Role.FREE,
            "session": Role.FREE,
            "trial": Role.FREE,
        },
    )

    figures = resolve(spec, table)

    assert len(figures) == 1
    # One bar per group — the figure the feature exists to draw.
    assert [str(v) for v in figures[0].x_order] == [
        "Digitimer",
        "Onward",
        MISSING_LEVEL,
    ]


def test_the_column_can_be_filtered_to_one_group(with_demographics):
    """Stratifying and filtering are the same mechanism once the column is a
    factor: the Filters section writes an ordinary row filter."""
    from scistackplot import Filter

    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )
    spec = _spec(
        factor_variables=GROUP,
        filters=[Filter(column="InterventionGroup", include=["Onward"])],
        roles={
            "InterventionGroup": Role.X,
            "subject": Role.FREE,
            "session": Role.FREE,
            "trial": Role.FREE,
        },
        kind=PlotKind.BAR,
    )

    figures = resolve(spec, table)

    assert [str(v) for v in figures[0].x_order] == ["Onward"]


def test_two_columns_of_one_sheet_are_two_factors(with_demographics):
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"],
        factor_variables=[
            FactorVariable("Demographics", "InterventionGroup"),
            FactorVariable("Demographics", "Sex"),
        ],
    )

    assert table.has_factor("InterventionGroup")
    assert table.has_factor("Sex")
    assert len(table.frame) == 12


def test_an_unknown_column_says_what_the_sheet_holds(with_demographics):
    with pytest.raises(ValueError, match="no column 'Group'"):
        ScidbSource(with_demographics).get_table(
            ["StepLength"],
            factor_variables=[FactorVariable("Demographics", "Group")],
        )


def test_a_colliding_factor_name_is_refused(seeded):
    """Two groupings that land on one column name: pandas would suffix one of
    them (`Condition_x`) and every role, filter and y-scope naming `Condition`
    would then point at a column nobody meant."""
    with pytest.raises(ValueError, match="already has"):
        ScidbSource(seeded).get_table(
            ["StepLength"],
            factor_variables=[FactorVariable("Condition"), FactorVariable("Condition")],
        )


# ---------------------------------------------------------------------------
# What the Grouping section offers, and what it refuses out loud
# ---------------------------------------------------------------------------


def test_groupable_offers_whole_variables_categoricals_first(seeded):
    report = ScidbSource(seeded).groupable_report("StepLength")
    offered = [o["label"] for o in report["offered"]]

    assert offered[0] == "Condition", "categorical groups lead"
    # Mass is subject-level and numeric: offered whole, not guessed at — a group
    # coded 1/2 is still a group, and it is what the user saved per subject.
    assert "Mass" in offered
    # Deeper than the data has no unambiguous per-row value.
    assert "Emg" not in offered


def test_groupable_offers_the_categorical_columns_of_a_sheet(with_demographics):
    report = ScidbSource(with_demographics).groupable_report("StepLength")
    offered = {o["label"]: o for o in report["offered"]}

    assert "Demographics.InterventionGroup" in offered
    assert "Demographics.Sex" in offered
    entry = offered["Demographics.InterventionGroup"]
    assert entry["variable"] == "Demographics"
    assert entry["column"] == "InterventionGroup"
    assert entry["name"] == "InterventionGroup"
    assert entry["level_count"] == 2
    assert entry["levels"] == ["Digitimer", "Onward"]


def test_a_struct_of_signals_makes_no_refusal_noise(with_demographics):
    """Emg sits at the measure's own level and holds one signal per muscle. It
    is not a near miss, and naming every muscle as a rejected grouping would
    bury the refusals that are."""
    report = ScidbSource(with_demographics).groupable_report("StepLength")

    assert not [label for label in report["rejected"] if label.startswith("Emg")]


def test_a_numeric_column_is_refused_with_a_reason(with_demographics):
    """Grouping by Age means grouping by RANGES of it. Offering the raw column
    would produce one level per distinct age; saying nothing at all would look
    like a bug, because the user can see the column in their spreadsheet."""
    report = ScidbSource(with_demographics).groupable_report("StepLength")

    assert "not offered yet" in report["rejected"]["Demographics.Age"]


def test_a_constant_column_is_refused(with_demographics):
    """A factor with one level is not a condition, it is a constant."""
    report = ScidbSource(with_demographics).groupable_report("StepLength")

    assert "one value" in report["rejected"]["Demographics.Site"]


def test_a_high_cardinality_column_is_refused(with_demographics, monkeypatch):
    import scistackplotdb.source as source_module

    monkeypatch.setattr(source_module, "MAX_GROUP_LEVELS", 1)
    report = ScidbSource(with_demographics).groupable_report("StepLength")

    assert "identifier, not a group" in report["rejected"]["Demographics.RecordId"]


def test_groupable_with_returns_specs(with_demographics):
    offers = ScidbSource(with_demographics).groupable_with("StepLength")

    assert FactorVariable("Demographics", "InterventionGroup") in offers
    assert FactorVariable("Condition") in offers


# ---------------------------------------------------------------------------
# Export: the exported figure must be the previewed figure
# ---------------------------------------------------------------------------


def test_the_endpoint_passes_the_group_as_its_own_input(seeded):
    """`as_table` hands a function schema keys and data columns only, so a
    subject-level Condition cannot ride along on a trial-level frame."""
    from scistackplotdb import generate_endpoint

    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)
    code = generate_endpoint(_spec(), table, input_variable="StepLength")

    assert '"group_condition": Condition,' in code.foreach_source
    assert "group_condition" in code.function_source
    assert "merge(" in code.function_source
    compile(code.source, "<generated>", "exec")


def test_the_endpoint_passes_a_column_as_a_column_selection(with_demographics):
    """`Demographics["InterventionGroup"]` under `as_table` hands the function
    the schema keys plus that one column (`scifor.foreach._prepare_input`) —
    which is what the generated merge expects to find."""
    from scistackplotdb import generate_endpoint

    spec = _spec(factor_variables=GROUP)
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=GROUP
    )
    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert (
        '"group_demographics_interventiongroup": '
        "Demographics['InterventionGroup']," in code.foreach_source
    )
    assert "group_demographics_interventiongroup" in code.source
    assert "as_table=" in code.foreach_source
    compile(code.source, "<generated>", "exec")


def test_generated_code_reproduces_the_join(seeded):
    """The exported figure must be the previewed one."""
    import matplotlib.pyplot as plt

    from scistackplot import generate_plot_function

    source = ScidbSource(seeded)
    table = source.get_table(["StepLength"], factor_variables=CONDITION)
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


def test_generated_code_reproduces_the_column_join_and_the_missing_level(
    with_demographics,
):
    """The reshape exists twice — here and in ``_attach_factor_variables`` — so
    the exported figure must label subject 03 the same way the preview did."""
    import matplotlib.pyplot as plt
    import pandas as pd

    from scistackplot import generate_plot_function

    source = ScidbSource(with_demographics)
    table = source.get_table(["StepLength"], factor_variables=GROUP)
    spec = _spec(
        factor_variables=GROUP,
        kind=PlotKind.BAR,
        roles={
            "InterventionGroup": Role.X,
            "subject": Role.FREE,
            "session": Role.FREE,
            "trial": Role.FREE,
        },
    )
    generated = generate_plot_function(spec, table)

    namespace: dict = {}
    exec(compile(generated, "<generated>", "exec"), namespace)  # noqa: S102

    keys = ["subject", "session", "trial"]
    df = table.frame[[*keys, "StepLength"]]
    # What `Demographics["InterventionGroup"]` delivers under `as_table`: the
    # schema keys it is recorded at, plus the selected column — and nothing for
    # subject 03.
    groups = pd.DataFrame(
        {
            "subject": ["01", "02"],
            "InterventionGroup": ["Onward", "Digitimer"],
        }
    )

    figure = namespace["plot_steplength"](df, groups, "figure.png")

    ticks = [label.get_text() for label in figure.axes[0].get_xticklabels()]
    assert MISSING_LEVEL in ticks, "the missing group must survive the export"
    assert set(ticks) == {"Onward", "Digitimer", MISSING_LEVEL}
    plt.close(figure)
