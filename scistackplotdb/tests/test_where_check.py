"""A where= check is an ordinary figure plus Yes/No factors (issue #13).

The verdict is scidb's (``filter_report``); the source joins it onto the rows
as ``Matching`` (and ``Filter 1``, ``Filter 2``... for two or more filters).
The test that matters: the figure's Yes rows are exactly the rows
``load(where=...)`` hands a function.
"""

from __future__ import annotations

import pytest
from scistackplot import PlotSpec
from scistackplot.model.where_check import (
    LEVEL_NO,
    LEVEL_NO_VALUE,
    LEVEL_YES,
    MATCHING_FACTOR,
    WhereCheck,
    WhereCheckFilter,
)

from scistackplotdb import ScidbSource

from conftest import Condition, Mass, StepLength  # noqa: F401  (registered by import)

# Mass is 71, 72, 73 for subjects 01..03; Condition is stim, sham, stim.
HEAVY = WhereCheckFilter("Mass", None, ">", "71.5")
STIM = WhereCheckFilter("Condition", None, "==", "stim")


def _levels_by_subject(table, factor: str = MATCHING_FACTOR) -> dict[str, set[str]]:
    frame = table.frame
    return {
        subject: set(group[factor])
        for subject, group in frame.groupby("subject")
    }


def test_matching_marks_each_row_by_the_filter_at_its_subject(seeded):
    check = WhereCheck("fn", (HEAVY,))
    table = ScidbSource(seeded).get_table(["StepLength"], where_check=check)

    assert table.has_factor(MATCHING_FACTOR)
    assert _levels_by_subject(table) == {
        "01": {LEVEL_NO},
        "02": {LEVEL_YES},
        "03": {LEVEL_YES},
    }


def test_the_yes_rows_are_exactly_the_rows_a_run_would_load(seeded):
    check = WhereCheck("fn", (HEAVY,))
    table = ScidbSource(seeded).get_table(["StepLength"], where_check=check)
    yes = int((table.frame[MATCHING_FACTOR] == LEVEL_YES).sum())

    loaded = StepLength.load(where=Mass > 71.5)
    loaded = loaded if isinstance(loaded, list) else [loaded]
    assert yes == len(loaded) == 8


def test_two_filters_get_a_factor_each_and_matching_is_their_and(seeded):
    check = WhereCheck("fn", (HEAVY, STIM))
    table = ScidbSource(seeded).get_table(["StepLength"], where_check=check)

    assert table.has_factor("Filter 1") and table.has_factor("Filter 2")
    assert _levels_by_subject(table, "Filter 1") == {"01": {LEVEL_NO}, "02": {LEVEL_YES}, "03": {LEVEL_YES}}
    assert _levels_by_subject(table, "Filter 2") == {"01": {LEVEL_YES}, "02": {LEVEL_NO}, "03": {LEVEL_YES}}
    # Only subject 03 is heavy AND stim.
    assert _levels_by_subject(table) == {"01": {LEVEL_NO}, "02": {LEVEL_NO}, "03": {LEVEL_YES}}


def test_an_unticked_filter_is_a_factor_but_not_part_of_matching(seeded):
    off = WhereCheckFilter("Condition", None, "==", "stim", enabled=False)
    table = ScidbSource(seeded).get_table(["StepLength"], where_check=WhereCheck("fn", (HEAVY, off)))

    assert _levels_by_subject(table, "Filter 2") == {"01": {LEVEL_YES}, "02": {LEVEL_NO}, "03": {LEVEL_YES}}
    # Matching counts only the ticked heavy filter.
    assert _levels_by_subject(table) == {"01": {LEVEL_NO}, "02": {LEVEL_YES}, "03": {LEVEL_YES}}


def test_a_location_with_no_filter_value_says_so_instead_of_dropping_the_row(seeded):
    # Subject 04 has a step length and no Mass at all.
    for session in ("pre",):
        StepLength.save(1.0, subject="04", session=session, trial="1")
    table = ScidbSource(seeded).get_table(["StepLength"], where_check=WhereCheck("fn", (HEAVY,)))

    assert _levels_by_subject(table)["04"] == {LEVEL_NO_VALUE}


def test_an_ordinary_figure_has_no_check_columns(seeded):
    table = ScidbSource(seeded).get_table(["StepLength"])
    assert not table.has_factor(MATCHING_FACTOR)


def test_the_spec_round_trips_its_check():
    spec = PlotSpec(measures=["StepLength"], where_check=WhereCheck("fn", (HEAVY, STIM), function_name="f"))
    again = PlotSpec.from_dict(spec.to_dict())
    assert again.where_check == spec.where_check
    assert PlotSpec.from_dict(PlotSpec(measures=["StepLength"]).to_dict()).where_check is None


def test_a_check_refuses_stacked_variables(seeded):
    with pytest.raises(ValueError, match="ONE variable"):
        ScidbSource(seeded).get_table(["StepLength", "Signal"], where_check=WhereCheck("fn", (HEAVY,)))
