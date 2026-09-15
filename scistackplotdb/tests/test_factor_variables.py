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

from conftest import scheme_axis as _scheme_axis

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


# ---------------------------------------------------------------------------
# Classifying a wide table's columns without reading its data
# ---------------------------------------------------------------------------
#
# Refusing a signal column used to cost one `sample_column_value` per muscle —
# a whole EMG trace fetched out of DuckDB, classified as SERIES_1D and thrown
# away. Measured on the user's project (scidb.log 2026-09-15): 4.2 s for
# FilteredDelsys and 5.4 s for RawEMG, ~8 s added to EVERY panel open by two
# variables that offer no groupings at all.
#
# The declared DuckDB type answers "does one cell hold a signal" for free
# (docs/claude/duckdb-column-types.md: column type IS the cell value type), so
# it is now a pre-filter in front of the sampling. Only a pre-filter: it never
# ACCEPTS a column, so an unrecognised type spelling costs speed, never
# correctness — see `load.is_container_type`.


@pytest.fixture
def sampled_columns(monkeypatch):
    """Records every ``(variable, column)`` whose VALUE gets read."""
    import scistackplotdb.source as source_module

    calls: list[tuple[str, str]] = []
    real = source_module.sample_column_value

    def spy(db, variable, column):
        calls.append((variable, column))
        return real(db, variable, column)

    monkeypatch.setattr(source_module, "sample_column_value", spy)
    return calls


def test_signal_columns_are_refused_without_reading_a_value(
    with_demographics, sampled_columns
):
    """The regression test for the 5.4 s. Emg holds one signal per muscle and
    is rightly not offered — but it must reach that answer from the schema."""
    ScidbSource(with_demographics).groupable_report("StepLength")

    assert not [
        call for call in sampled_columns if call[0] in ("Emg", "EmgFiltered")
    ], f"a signal column's value was read: {sampled_columns}"


def test_a_signal_struct_is_still_absent_and_still_silent(with_demographics):
    """Same ANSWER as before, reached more cheaply: not offered, and not named
    as a refusal either — a struct of signals is not a near miss."""
    report = ScidbSource(with_demographics).groupable_report("StepLength")

    assert not [o for o in report["offered"] if o["variable"] == "Emg"]
    assert not [label for label in report["rejected"] if label.startswith("Emg")]


def test_the_sheet_s_own_columns_are_still_sampled(
    with_demographics, sampled_columns
):
    """The pre-filter must not over-reject. Demographics holds scalar-typed
    columns, so every one of them still goes to the value classifier — that is
    what separates a numeric column from a categorical one."""
    ScidbSource(with_demographics).groupable_report("StepLength")

    sampled = {
        column for variable, column in sampled_columns if variable == "Demographics"
    }
    assert {"Age", "Sex", "InterventionGroup", "Site", "RecordId"} <= sampled


def test_the_offers_are_unchanged_by_the_pre_filter(with_demographics):
    """Belt and braces around the two tests above: the whole report is what it
    was, so the speed-up cannot have quietly changed an answer."""
    report = ScidbSource(with_demographics).groupable_report("StepLength")
    offered = {o["label"] for o in report["offered"]}

    assert "Demographics.InterventionGroup" in offered
    assert "Demographics.Sex" in offered
    assert "Condition" in offered
    assert "not offered yet" in report["rejected"]["Demographics.Age"]
    assert "one value" in report["rejected"]["Demographics.Site"]


# --- the type predicate itself ---------------------------------------------


@pytest.mark.parametrize(
    "data_type",
    [
        "DOUBLE[]",       # a 1-D signal per cell
        "DOUBLE[][]",     # a matrix per cell
        "DOUBLE[3]",      # a fixed-size array
        "VARCHAR[]",
        "STRUCT(a INTEGER, b VARCHAR)",
        "MAP(VARCHAR, INTEGER)",
        "BLOB",
        "JSON",
        "double[]",       # spelling is normalised before matching
    ],
)
def test_container_types_are_recognised(data_type):
    from scistackplotdb.load import is_container_type

    assert is_container_type(data_type)


@pytest.mark.parametrize(
    "data_type",
    [
        "DOUBLE",
        "BIGINT",
        "VARCHAR",
        "BOOLEAN",
        "DATE",
        # Punctuation, but one value per cell — the reason the match is on the
        # leading word rather than on "has a bracket or paren".
        "DECIMAL(18,3)",
        "TIMESTAMP WITH TIME ZONE",
    ],
)
def test_scalar_types_are_not_containers(data_type):
    from scistackplotdb.load import is_container_type

    assert not is_container_type(data_type)


@pytest.mark.parametrize("data_type", ["", None, "SOMETHING_NEW"])
def test_an_unknown_type_falls_through_to_sampling(data_type):
    """The safety property, stated as a test because it is the whole reason
    this predicate is allowed to exist beside `sample_value`'s deliberate
    "classify by VALUE, not by type name" rule. Refusing on an unrecognised
    spelling would mean a real label column silently missing from the list;
    falling through costs only the query it was trying to save."""
    from scistackplotdb.load import is_container_type

    assert not is_container_type(data_type)


def test_the_type_query_reports_what_the_column_list_reports(with_demographics):
    """`data_columns_for` is now `data_column_types_for`'s keys, so the two can
    never disagree about which columns exist or what order they are in."""
    from scistackplotdb.load import data_column_types_for, data_columns_for

    assert data_columns_for(with_demographics, "Demographics") == list(
        data_column_types_for(with_demographics, "Demographics")
    )


def test_a_signal_column_declares_a_container_type(with_demographics):
    """The assumption the pre-filter rests on, checked against a real DuckDB
    table rather than taken from the docs: a dict-of-signals variable stores
    one array-typed column per field."""
    from scistackplotdb.load import data_column_types_for, is_container_type

    types = data_column_types_for(with_demographics, "Emg")

    assert types, "Emg should have data columns"
    assert all(is_container_type(t) for t in types.values()), types


# ---------------------------------------------------------------------------
# The two halves: which VARIABLES, then one variable's COLUMNS
# ---------------------------------------------------------------------------
#
# `groupable_report` answered both at once, so opening the panel paid every
# wide variable's per-column queries whether or not anyone was going to look.
# The picker asks `groupable_variables` to draw the canvas and
# `groupable_columns` once per variable clicked. `groupable_report` survives as
# the composition of the two — `groupable_with` is the library API and the CSV
# source answers in one call — so the pair and the whole must not drift.


def test_groupable_variables_makes_no_column_query(
    with_demographics, sampled_columns, monkeypatch
):
    """The point of the split: drawing the canvas reads no values and counts no
    distinct levels of any wide table's column."""
    import scistackplotdb.source as source_module

    levelled: list[tuple[str, str]] = []
    real = source_module.column_levels

    def spy(db, variable, column, *, limit):
        levelled.append((variable, column))
        return real(db, variable, column, limit=limit)

    monkeypatch.setattr(source_module, "column_levels", spy)

    ScidbSource(with_demographics).groupable_variables("StepLength")

    assert not sampled_columns, f"a column value was read: {sampled_columns}"
    assert not [call for call in levelled if call[0] == "Demographics"], (
        f"a wide table's columns were counted: {levelled}"
    )


def test_groupable_variables_offers_the_sheet_as_a_clickable_node(
    with_demographics,
):
    report = ScidbSource(with_demographics).groupable_variables("StepLength")
    by_name = {o["variable"]: o for o in report["offered"]}

    assert by_name["Demographics"]["kind"] == "columns"
    # Five label-capable columns; which of them QUALIFY is the click's question.
    assert by_name["Demographics"]["column_count"] == 5


def test_groupable_variables_offers_a_single_column_variable_whole(seeded):
    report = ScidbSource(seeded).groupable_variables("StepLength")
    by_name = {o["variable"]: o for o in report["offered"]}

    assert by_name["Condition"]["kind"] == "categorical"
    assert by_name["Condition"]["offer"]["levels"] == ["sham", "stim"]
    # Numeric, offered whole: a group coded 1/2 is still a group.
    assert by_name["Mass"]["kind"] == "numeric"


def test_a_struct_of_signals_is_not_a_clickable_node(with_demographics):
    """Every column holds a signal, so there is nothing to click into — and it
    is silent, not refused, for the same reason the per-column branch is."""
    report = ScidbSource(with_demographics).groupable_variables("StepLength")

    assert "Emg" not in {o["variable"] for o in report["offered"]}
    assert "Emg" not in report["rejected"]


def test_groupable_columns_answers_for_one_variable(with_demographics):
    report = ScidbSource(with_demographics).groupable_columns(
        "StepLength", "Demographics"
    )
    offered = {o["label"] for o in report["offered"]}

    assert offered == {
        "Demographics.InterventionGroup",
        "Demographics.Sex",
        # Offered, and rightly: the fixture holds two subjects, so RecordId has
        # two distinct values and cannot trip the cardinality guard. It is the
        # identifier-shaped column, which `test_a_high_cardinality_column_is_
        # refused` covers by lowering MAX_GROUP_LEVELS rather than by needing a
        # fifty-row sheet here.
        "Demographics.RecordId",
    }
    assert "not offered yet" in report["rejected"]["Demographics.Age"]
    assert "one value" in report["rejected"]["Demographics.Site"]


def test_the_pair_reassembles_into_the_whole(with_demographics):
    """The anti-drift test. `groupable_report` is now literally the two halves
    composed, so anything that makes them disagree breaks the library API and
    the picker at the same time."""
    source = ScidbSource(with_demographics)
    whole = source.groupable_report("StepLength")

    variables = source.groupable_variables("StepLength")
    rebuilt: list[dict] = []
    rejected: dict[str, str] = dict(variables["rejected"])
    for kind in ("categorical", "numeric"):
        rebuilt.extend(
            entry["offer"] for entry in variables["offered"] if entry["kind"] == kind
        )
    for entry in variables["offered"]:
        if entry["kind"] == "columns":
            columns = source.groupable_columns("StepLength", entry["variable"])
            rebuilt.extend(columns["offered"])
            rejected.update(columns["rejected"])

    assert [o["label"] for o in rebuilt] == [o["label"] for o in whole["offered"]]
    assert rejected == whole["rejected"]


def test_the_composition_asks_each_wide_variable_once(
    with_demographics, monkeypatch
):
    """`groupable_report` needs both the offers and the refusals of each wide
    variable. Fetching them in two passes would double the very queries this
    split exists to ration."""
    source = ScidbSource(with_demographics)
    calls: list[str] = []
    real = source.groupable_columns

    def spy(measure, variable):
        calls.append(variable)
        return real(measure, variable)

    monkeypatch.setattr(source, "groupable_columns", spy)
    source.groupable_report("StepLength")

    assert calls == ["Demographics"]


def test_categorical_groups_still_lead_after_the_split(with_demographics):
    """`kind` carries the sort bucket so the two halves cannot disagree about
    what comes first — the ordering `groupable_report` always had."""
    offered = [
        o["label"]
        for o in ScidbSource(with_demographics).groupable_report("StepLength")["offered"]
    ]

    assert offered[0] == "Condition"
    assert offered.index("Mass") < offered.index("Demographics.InterventionGroup")


def test_a_csv_source_answers_the_new_pair_too(tmp_path):
    """No `hasattr` guard at the call site: every source answers, and a flat
    table has nothing to join in."""
    import pandas as pd
    from scistackplot import CsvSource

    path = tmp_path / "flat.csv"
    pd.DataFrame({"subject": ["01"], "StepLength": [1.0]}).to_csv(path, index=False)
    source = CsvSource(path)

    assert source.groupable_variables("StepLength") == {"offered": [], "rejected": {}}
    assert source.groupable_columns("StepLength", "anything") == {
        "offered": [],
        "rejected": {},
    }


# ---------------------------------------------------------------------------
# Which VARIANT of the grouping variable supplies the labels
# ---------------------------------------------------------------------------
#
# A grouping variable has variants like any other: edit the loader that reads
# the demographics sheet and two records per subject coexist, each with its own
# labels. `_attach_factor_variables` merged them together and `drop_duplicates`
# kept whichever came first — a figure stratified by a version of the
# spreadsheet nobody chose, with nothing on screen saying so.




def test_the_fixture_really_has_two_variants(two_label_variants):
    """Precondition. Without it every assertion below could pass by the
    grouping simply having one record per subject."""
    loaded = ScidbSource(two_label_variants)._variable_frame("GroupLabel")
    # `value_column`, not the variable's name: a VariableFrame carries the
    # STORAGE column. It is `_attach_factor_variables` that renames it to the
    # factor's name, which is why every other test here reads "GroupLabel".
    frame = loaded.frame

    assert len(frame) == 6, "3 subjects x 2 schemes"
    assert frame.groupby("subject")[loaded.value_column].nunique().min() == 2


@pytest.mark.parametrize("scheme", ["a", "b"])
def test_the_pin_decides_which_labels_reach_the_figure(two_label_variants, scheme):
    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)

    table = source.get_table(
        ["StepLength"],
        factor_variables=[FactorVariable("GroupLabel", variant={axis: scheme})],
    )

    labels = set(table.frame["GroupLabel"])
    assert labels and all(label.startswith(f"{scheme}-") for label in labels), labels
    assert len(table.frame) == 12, "pinning must not drop or multiply rows"


def test_the_two_pins_give_different_figures(two_label_variants):
    """The point of the feature: if both pins produced the same labels, the
    control would be doing nothing."""
    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)

    def labels(scheme):
        table = source.get_table(
            ["StepLength"],
            factor_variables=[FactorVariable("GroupLabel", variant={axis: scheme})],
        )
        return set(table.frame["GroupLabel"])

    assert labels("a") != labels("b")


def test_an_unpinned_grouping_says_it_had_to_choose(two_label_variants, caplog):
    """Empty selection stays inert — nothing is filtered, as with an unfilled
    variant row — but the ambiguity it leaves is no longer silent."""
    import logging

    source = ScidbSource(two_label_variants)
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        table = source.get_table(
            ["StepLength"], factor_variables=[FactorVariable("GroupLabel")]
        )

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "more than one" in text
    assert "GroupLabel" in text
    # Still attached, still one label per subject: the warning explains the
    # choice, it does not refuse to make one.
    assert table.frame.groupby("subject")["GroupLabel"].nunique().max() == 1


def test_a_single_variant_grouping_warns_about_nothing(with_demographics, caplog):
    """The ordinary case must stay quiet, or the warning above is noise that
    teaches people to ignore it."""
    import logging

    source = ScidbSource(with_demographics)
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        source.get_table(["StepLength"], factor_variables=GROUP)

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "more than one" not in text


def test_a_pin_that_matches_nothing_says_so(two_label_variants, caplog):
    """The pin is applied blindly (`variants.default_selection`), so an empty
    result is reachable — and it looks exactly like a sheet with no matching
    subjects unless it is announced."""
    import logging

    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)

    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        table = source.get_table(
            ["StepLength"],
            factor_variables=[FactorVariable("GroupLabel", variant={axis: "z"})],
        )

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "matched no records" in text
    assert set(table.frame["GroupLabel"]) == {MISSING_LEVEL}


def test_a_list_pin_widens_the_candidates_and_says_it_had_to_choose(
    two_label_variants, caplog
):
    """Membership, the same semantics a list-valued ``Variant(...)`` kwarg has
    in scidb — but a GROUPING is one label per schema location by definition,
    so selecting both schemes leaves the join under-determined.

    That is not a reason to refuse a list pin: an axis with three levels where
    each subject only ran two of them resolves fine. It IS a reason the warning
    has to fire, because otherwise the figure is stratified by whichever record
    the database happened to return first.
    """
    import logging

    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)

    group = FactorVariable("GroupLabel", variant={axis: ["a", "b"]})
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        table = source.get_table(["StepLength"], factor_variables=[group])

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "more than one" in text
    # Still exactly one label per subject — the merge cannot do anything else.
    assert table.frame.groupby("subject")["GroupLabel"].nunique().max() == 1


# --- the default: latest, spelled the way the Variants section spells it ----


def test_the_default_pin_is_the_latest_flag_never_the_string(two_label_variants):
    """The trap `plot-variant-rows.md` §2 documents, as a test. A code axis
    selected as the STRING "latest" stops resolving through the per-location
    flag the moment anything else is pinned beside it, and silently drops every
    schema location never re-run under the newest code. The bool is not a
    latest-axis, so it survives whatever else joins the selection."""
    selection = ScidbSource(two_label_variants).default_variant_for("GroupLabel")

    assert "latest" not in [str(value) for value in selection.values()]


def test_the_default_pin_resolves_to_one_variant(two_label_variants):
    """Whatever it picks, it has to pick ONE — that is what makes it a usable
    default for a grouping the user has not thought about yet."""
    source = ScidbSource(two_label_variants)
    default = source.default_variant_for("GroupLabel")

    table = source.get_table(
        ["StepLength"],
        factor_variables=[FactorVariable("GroupLabel", variant=default)],
    )

    assert table.frame.groupby("subject")["GroupLabel"].nunique().max() == 1
    assert MISSING_LEVEL not in set(table.frame["GroupLabel"])


def test_a_variable_with_no_variants_defaults_to_no_pin(with_demographics):
    """Nothing to choose between, so nothing is pinned — and the empty
    selection is inert, which is the same figure as before this existed."""
    assert ScidbSource(with_demographics).default_variant_for("Demographics") in (
        {},
        None,
    )


# --- the spec field --------------------------------------------------------


def test_a_pinned_grouping_is_hashable_and_memoizes(two_label_variants):
    """`BaseSource.get_table` memoizes on a tuple of FactorVariables, so an
    unhashable field would break the cache the moment anyone pinned one — and a
    list-valued selection is the shape that would have done it."""
    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)
    one = FactorVariable("GroupLabel", variant={axis: ["a", "b"]})
    same = FactorVariable("GroupLabel", variant={axis: ["b", "a"]})
    other = FactorVariable("GroupLabel", variant={axis: "a"})

    assert hash(one) == hash(same), "level ORDER is not part of the selection"
    assert one == same
    assert one != other
    assert source.get_table(["StepLength"], factor_variables=[one]) is source.get_table(
        ["StepLength"], factor_variables=[same]
    )
    assert source.get_table(
        ["StepLength"], factor_variables=[one]
    ) is not source.get_table(["StepLength"], factor_variables=[other])


def test_a_pinned_grouping_round_trips_through_the_spec():
    group = FactorVariable("Demographics", "Sex", variant={"CodeIsLatest": True})
    spec = PlotSpec(measures=["StepLength"], factor_variables=[group])

    restored = PlotSpec.from_dict(spec.to_dict())

    assert restored.factor_variables == [group]
    assert restored.factor_variables[0].selection == {"CodeIsLatest": True}


def test_an_unpinned_grouping_still_round_trips():
    """Specs written before this field exists must keep parsing — the field is
    additive, not a new requirement."""
    restored = FactorVariable.from_dict({"variable": "Condition", "column": None})

    assert restored == FactorVariable("Condition")
    assert restored.selection == {}


def test_the_pin_reaches_the_generated_endpoint(two_label_variants):
    """Invariant 4: the exported figure is the previewed figure. An endpoint
    that loaded every variant would be stratified by a different sheet than the
    panel was."""
    from scistackplotdb import generate_endpoint

    source = ScidbSource(two_label_variants)
    axis = _scheme_axis(source)
    group = FactorVariable("GroupLabel", variant={axis: "a"})
    table = source.get_table(["StepLength"], factor_variables=[group])
    spec = _spec(factor_variables=[group])

    code = generate_endpoint(spec, table, input_variable="StepLength")

    assert "Variant(GroupLabel" in code.foreach_source
    assert "'a'" in code.foreach_source
    compile(code.source, "<generated>", "exec")


def test_an_unpinned_grouping_exports_no_variant_wrapper(seeded):
    """The wrapper appears because a pin was made, not by default — an
    unpinned grouping's generated code must be exactly what it always was."""
    from scistackplotdb import generate_endpoint

    table = ScidbSource(seeded).get_table(["StepLength"], factor_variables=CONDITION)
    code = generate_endpoint(_spec(), table, input_variable="StepLength")

    # A wrapped one would read `"group_condition": Variant(Condition, ...)`.
    assert '"group_condition": Condition,' in code.foreach_source
