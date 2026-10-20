"""Covariates for StatStudio's mixed model (plan-statstudio-model-formula,
stage 3): a numeric variable, or one numeric column of a wide one, loaded
through the SAME loader grouping variables use (``_keyed_values``: variant
pin, one value per schema location), and offered from the DAG canvas as the
mirror of the grouping offers (numeric offered, text refused)."""

from __future__ import annotations

import pytest
from scistackplot import FactorVariable

from scistackplotdb import ScidbSource


def test_a_whole_numeric_variable_is_keyed_by_its_schema_level(seeded):
    keys, frame = ScidbSource(seeded).covariate_values(FactorVariable("Mass"))
    assert keys == ["subject"]
    assert list(frame.columns) == ["subject", "Mass"]
    assert len(frame) == frame["subject"].nunique(), "one value per subject"
    assert frame["Mass"].dtype.kind == "f"


def test_one_column_of_a_wide_sheet_and_a_missing_subject(with_demographics):
    keys, frame = ScidbSource(with_demographics).covariate_values(FactorVariable("Demographics", "Age"))
    assert keys == ["subject"] and list(frame.columns) == ["subject", "Age"]
    assert sorted(frame["Age"]) == [64.0, 71.0]
    assert "03" not in set(frame["subject"]), "absent stays absent: the join leaves that row NaN"


def test_a_text_column_is_not_a_covariate(with_demographics):
    with pytest.raises(ValueError, match="a covariate is numeric"):
        ScidbSource(with_demographics).covariate_values(FactorVariable("Demographics", "Sex"))
    with pytest.raises(ValueError, match="a covariate is numeric"):
        ScidbSource(with_demographics).covariate_values(FactorVariable("Condition"))


def test_the_offers_are_the_mirror_of_the_grouping_offers(with_demographics):
    source = ScidbSource(with_demographics)
    variables = source.covariate_variables("StepLength")
    offered = {o["label"]: o["kind"] for o in variables["offered"]}
    assert offered.get("Mass") == "numeric" and offered.get("Demographics") == "columns"
    assert "Condition" not in offered and "a covariate is numeric" in variables["rejected"]["Condition"]
    columns = source.covariate_columns("StepLength", "Demographics")
    assert [o["name"] for o in columns["offered"]] == ["Age"]
    assert "a covariate is numeric" in columns["rejected"]["Demographics.Sex"]


def test_grouping_still_joins_through_the_shared_loader(with_demographics):
    """The loader was extracted from _attach_factor_variables; a grouping
    column still broadcasts per subject and keeps the missing subject."""
    table = ScidbSource(with_demographics).get_table(
        ["StepLength"], factor_variables=[FactorVariable("Demographics", "InterventionGroup")])
    assert table.frame.groupby("subject")["InterventionGroup"].nunique().max() == 1
    assert len(table.frame) == 12
