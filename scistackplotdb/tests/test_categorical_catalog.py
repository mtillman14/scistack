"""Text fields of a wide variable are plotted, not dropped.

A wide variable with numeric AND text fields is two measures in the catalog
(user decision, 2026-11-01): ``Gait`` (its numeric fields under ``ColName``,
as ever) and ``Gait (text)`` (its text fields). ``scistackplotdb.text_fields``
owns the name; ``ScidbSource._melt_fields`` owns which field goes where.
docs/claude/categorical-measures.md.
"""

from __future__ import annotations

import logging

import pytest
from scidb import BaseVariable
from scistackplot import (
    LEVEL_FACTOR,
    CategoricalOptions,
    CategoricalStat,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    default_spec,
    resolve,
)
from scistackplot.reduce.resolved import X, Y

from scistackplotdb import ScidbSource, base_variable, generate_endpoint, text_measure_name
from scistackplotdb.source import MAX_GROUP_LEVELS


class Survey(BaseVariable):
    """Every field text: the variable itself is the text measure."""


class Note(BaseVariable):
    """Free text: one distinct value per record."""


def test_a_mixed_variable_is_listed_twice(with_gait):
    catalog = {m["name"]: m for m in ScidbSource(with_gait).describe()["measures"]}
    assert catalog["Gait"]["shape"] == str(Shape.SCALAR)
    assert set(catalog["Gait"]["columns"]) == {"StepLength", "StepWidth"}
    text = catalog[text_measure_name("Gait")]
    assert text["shape"] == str(Shape.CATEGORICAL)
    assert text["plottable"] is True
    assert text["columns"] == ["Side"]
    assert text["text_of"] == "Gait"


def test_the_text_measure_loads_only_the_text_fields(with_gait, caplog):
    source = ScidbSource(with_gait)
    with caplog.at_level(logging.INFO, logger="scistackplotdb"):
        numeric = source.get_table(["Gait"])
    assert set(numeric.frame["ColName"]) == {"StepLength", "StepWidth"}
    assert any("Gait (text)" in r.getMessage() for r in caplog.records)

    text = source.get_table(["Gait (text)"])
    assert text.shape_of("Gait (text)") is Shape.CATEGORICAL
    assert set(text.frame["ColName"]) == {"Side"}
    assert text.measure("Gait (text)").levels == ["L", "R"]


def test_the_text_measure_opens_on_a_count_per_category(with_gait):
    table = ScidbSource(with_gait).get_table(["Gait (text)"])
    spec = default_spec(table, "Gait (text)")
    assert spec.kind is PlotKind.BAR
    assert spec.roles[LEVEL_FACTOR] is Role.GROUP
    [figure] = resolve(spec, table)
    counts = dict(zip(figure.panels[0].frame[X], figure.panels[0].frame[Y]))
    # One "L" and one "R" trial per (subject, session): equal counts.
    assert counts["L"] == counts["R"] == len(table.frame) / 2


def test_text_measure_names_map_to_their_variable():
    assert base_variable("Gait (text)") == "Gait"
    assert base_variable("Gait") == "Gait"
    assert text_measure_name("Gait") == "Gait (text)"


def test_a_text_measure_neither_stacks_nor_joins(with_gait):
    source = ScidbSource(with_gait)
    assert source.joinable_with("Gait (text)") == []
    report = source.stackable_report("Gait (text)")
    assert report["offered"] == []
    assert report["rejected"] and "Gait" not in report["rejected"]
    assert all("plotted on its own" in reason for reason in report["rejected"].values())


def test_an_all_text_variable_is_itself_categorical(db):
    for subject, answer in (("01", "yes"), ("02", "no"), ("03", "yes")):
        Survey.save({"Q1": answer, "Q2": "maybe"}, subject=subject, session="pre", trial="1")
    source = ScidbSource(db)
    catalog = {m["name"]: m for m in source.describe()["measures"]}
    assert catalog["Survey"]["shape"] == str(Shape.CATEGORICAL)
    assert text_measure_name("Survey") not in catalog
    table = source.get_table(["Survey"])
    assert set(table.frame["ColName"]) == {"Q1", "Q2"}
    spec = default_spec(table, "Survey")
    [figure] = resolve(spec, table)
    by_field = {
        panel.key.get("ColName"): dict(zip(panel.frame[X], panel.frame[Y]))
        for panel in figure.panels
    }
    assert by_field["Q1"] == {"no": 1.0, "yes": 2.0}
    assert by_field["Q2"] == {"maybe": 3.0}


def test_free_text_is_refused_in_place(db):
    for index in range(MAX_GROUP_LEVELS + 3):
        Note.save(f"note {index}", subject=f"{index:03d}", session="pre", trial="1")
    from scistackplot.reduce.categorical import free_text_reason

    table = ScidbSource(db).get_table(["Note"])
    assert "free text" in free_text_reason(table, "Note")


def test_the_endpoint_reads_the_variable_of_a_text_measure(with_gait):
    table = ScidbSource(with_gait).get_table(["Gait (text)"])
    spec = default_spec(table, "Gait (text)")
    code = generate_endpoint(spec, table, input_variable="Gait (text)")
    assert "Gait (text)" not in code.foreach_source
    assert '"df": Gait' in code.foreach_source
    # The function melts the text fields, as the source did.
    assert "Side" in code.function_source


@pytest.mark.parametrize("stat", [CategoricalStat.COUNT, CategoricalStat.PROPORTION])
def test_the_text_measure_resolves_both_bar_types(with_gait, stat):
    table = ScidbSource(with_gait).get_table(["Gait (text)"])
    spec = default_spec(table, "Gait (text)")
    spec = PlotSpec.from_dict({**spec.to_dict(), "categorical": CategoricalOptions(stat=stat).to_dict()})
    [figure] = resolve(spec, table)
    values = figure.panels[0].frame[Y].tolist()
    assert len(values) == 2
    if stat is CategoricalStat.PROPORTION:
        assert sum(values) == pytest.approx(100.0)
