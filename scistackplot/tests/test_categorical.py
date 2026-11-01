"""Text (categorical) measures: counts, proportions and categories.

docs/claude/categorical-measures.md; .claude/plan-categorical-measures.md.
Decisions under test (user, 2026-11-01): a text measure opens on a COUNT bar
with one bar per category; "Collapse" means POOL for every text plot; a
Proportion bar is the mean of per-unit %s and carries "Show sample".

The data is chosen so that pooling and nested averaging DISAGREE: subject 2
has an unequal number of trials per session, so "pool trials within subject"
(20 % falls) differs from "average the per-session %s" (25 %).
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    LEVEL_FACTOR,
    BarOptions,
    CategoricalOptions,
    CategoricalStat,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    available_plots,
    capabilities,
    default_spec,
    mode_for_kind,
    plot_data,
    resolve,
    stack_bases,
)
from scistackplot.categorical import MAX_CATEGORY_LEVELS, apply_categorical
from scistackplot.resolved import GRID_COLUMNS, GRID_ROWS, X, Y, Y_HIGH, Y_LOW, Z
from scistackplot.roles import RoleError
from scistackplot.spec import Filter

KEYS = ["subject", "session", "trial"]

#: (subject, session) -> each trial's outcome. Falls: subject 1 4 of 8 (50 %),
#: subject 2 1 of 5 (20 %) — nested per session it would be 25 %.
OUTCOMES = {
    ("1", "1"): ["fall", "fall", "fall", "ok"],
    ("1", "2"): ["fall", "ok", "ok", "ok"],
    ("2", "1"): ["ok", "ok", "ok"],
    ("2", "2"): ["fall", "ok"],
}


def _frame(outcomes=OUTCOMES) -> pd.DataFrame:
    rows = [
        {"subject": subject, "session": session, "trial": str(trial), "Outcome": outcome}
        for (subject, session), values in outcomes.items()
        for trial, outcome in enumerate(values, start=1)
    ]
    return pd.DataFrame(rows)


def _table(frame: pd.DataFrame | None = None) -> LongTable:
    return LongTable.from_frame(
        _frame() if frame is None else frame,
        factors=KEYS,
        measures=["Outcome"],
        schema_levels=KEYS,
    )


def _spec(stat=CategoricalStat.COUNT, kind=PlotKind.BAR, **roles) -> PlotSpec:
    base = {LEVEL_FACTOR: Role.GROUP, "subject": Role.COLLAPSE, "session": Role.COLLAPSE, "trial": Role.COLLAPSE}
    base.update(roles)
    return PlotSpec(
        measures=["Outcome"],
        roles=base,
        groups=[LEVEL_FACTOR] if base.get(LEVEL_FACTOR) is Role.GROUP else [],
        color=LEVEL_FACTOR if base.get(LEVEL_FACTOR) is Role.GROUP else None,
        kind=kind,
        categorical=CategoricalOptions(stat=stat),
    )


def _bars(spec: PlotSpec, table: LongTable | None = None) -> dict[str, float]:
    [figure] = resolve(spec, table or _table())
    frame = figure.panels[0].frame
    return {str(x): float(y) for x, y in zip(frame[X], frame[Y])}


# --- shape, levels, the kind list ------------------------------------------


def test_a_text_measure_is_categorical_with_ordered_levels():
    table = _table()
    assert table.shape_of("Outcome") is Shape.CATEGORICAL
    assert table.measure("Outcome").levels == ["fall", "ok"]
    assert table.describe()["measures"][0]["plottable"] is True


def test_the_kind_list_is_the_text_kinds():
    roles = {LEVEL_FACTOR: Role.GROUP, "subject": Role.COLLAPSE}
    kinds = available_plots(Shape.CATEGORICAL, roles, n_groups=1)
    assert PlotKind.BAR in kinds and PlotKind.HBAR in kinds and PlotKind.STRIP in kinds
    assert PlotKind.BOX not in kinds and PlotKind.VIOLIN not in kinds


def test_the_kind_decides_the_mode():
    assert mode_for_kind(PlotKind.STRIP, CategoricalStat.COUNT) is CategoricalStat.VALUE
    assert mode_for_kind(PlotKind.BAR, CategoricalStat.VALUE) is CategoricalStat.COUNT
    assert mode_for_kind(PlotKind.SPAGHETTI, CategoricalStat.COUNT) is CategoricalStat.VALUE
    assert (
        mode_for_kind(PlotKind.SPAGHETTI, CategoricalStat.PROPORTION)
        is CategoricalStat.PROPORTION
    )


def test_a_text_measure_opens_on_a_count_bar_per_category():
    spec = default_spec(_table(), "Outcome")
    assert spec.kind is PlotKind.BAR
    assert spec.categorical.stat is CategoricalStat.COUNT
    assert spec.roles[LEVEL_FACTOR] is Role.GROUP
    assert all(spec.roles[key] is Role.COLLAPSE for key in KEYS)
    assert _bars(spec) == {"fall": 5.0, "ok": 8.0}


# --- count ------------------------------------------------------------------


def test_a_count_bar_counts_the_pooled_records():
    assert _bars(_spec()) == {"fall": 5.0, "ok": 8.0}


def test_a_count_per_session_pools_subjects_and_trials():
    spec = _spec(session=Role.FACET)
    figures = resolve(spec, _table())
    by_panel = {
        panel.key["session"]: dict(zip(panel.frame[X], panel.frame[Y]))
        for panel in figures[0].panels
    }
    assert by_panel == {"1": {"fall": 3.0, "ok": 4.0}, "2": {"fall": 2.0, "ok": 4.0}}


def test_collapsing_the_categories_counts_every_record():
    spec = replace(_spec(**{LEVEL_FACTOR: Role.COLLAPSE}), groups=[], color=None)
    [figure] = resolve(spec, _table())
    assert float(figure.panels[0].frame[Y].sum()) == 13.0


def test_one_category_only():
    spec = replace(_spec(), categorical=CategoricalOptions(level="fall"))
    [figure] = resolve(spec, _table())
    assert float(figure.panels[0].frame[Y].sum()) == 5.0
    assert "fall" in figure.labels.y


def test_a_missing_value_is_counted_as_its_own_category():
    frame = _frame()
    frame.loc[0, "Outcome"] = None
    bars = _bars(_spec(), _table(frame))
    assert bars == {"fall": 4.0, "ok": 8.0, "(missing)": 1.0}


def test_a_count_has_no_sample_to_show():
    spec = replace(_spec(), show_sample=["subject"])
    report = capabilities(spec, _table())
    assert report["sample_overlay"]["available"] is False
    assert "count" in report["sample_overlay"]["reason"].lower()


# --- proportion -------------------------------------------------------------


def test_a_proportion_pools_within_each_unit_then_averages_the_units():
    bars = _bars(_spec(CategoricalStat.PROPORTION))
    # subject 1: 4/8 falls = 50 %; subject 2: 1/5 = 20 % (pooled, not the
    # 25 % a nested per-session mean would give) -> 35 %.
    assert bars["fall"] == pytest.approx(35.0)
    assert bars["ok"] == pytest.approx(65.0)


def test_proportions_sum_to_100_and_carry_an_error_band():
    [figure] = resolve(_spec(CategoricalStat.PROPORTION), _table())
    frame = figure.panels[0].frame
    assert float(frame[Y].sum()) == pytest.approx(100.0)
    assert (frame[Y_HIGH] > frame[Y]).all() and (frame[Y_LOW] < frame[Y]).all()


def test_show_sample_draws_each_units_percent():
    spec = replace(_spec(CategoricalStat.PROPORTION), show_sample=["subject"])
    [figure] = resolve(spec, _table())
    sample = figure.panels[0].sample
    falls = sorted(sample[sample[X] == "fall"][Y].tolist())
    assert falls == pytest.approx([20.0, 50.0])


def test_a_proportion_needs_a_sample():
    spec = _spec(CategoricalStat.PROPORTION, subject=Role.ITERATE, session=Role.ITERATE, trial=Role.ITERATE)
    with pytest.raises(RoleError, match="needs a sample"):
        resolve(spec, _table())


def test_a_proportion_refuses_to_collapse_the_categories():
    spec = replace(
        _spec(CategoricalStat.PROPORTION, **{LEVEL_FACTOR: Role.COLLAPSE}), groups=[], color=None
    )
    with pytest.raises(RoleError, match="100"):
        resolve(spec, _table())


def test_a_filter_on_the_text_filters_records():
    spec = replace(
        _spec(CategoricalStat.PROPORTION), filters=[Filter(column="Outcome", include=["fall"])]
    )
    assert _bars(spec) == {"fall": pytest.approx(100.0)}


def test_a_range_filter_on_text_is_refused():
    spec = replace(_spec(), filters=[Filter(column="Outcome", minimum=0.0)])
    with pytest.raises(RoleError, match="numeric range"):
        resolve(spec, _table())


# --- categories as positions (strip, spaghetti) -----------------------------


def test_a_strip_draws_every_record_at_its_categorys_position():
    spec = _spec(kind=PlotKind.STRIP)
    [figure] = resolve(spec, _table())
    frame = figure.panels[0].frame
    assert figure.y_categories == ["fall", "ok"]
    assert len(frame) == 13  # collapse pools: every record drawn
    assert sorted(set(frame[Y])) == [0.0, 1.0]
    assert tuple(figure.y_limits) == pytest.approx((-0.5, 1.5))


def test_a_spaghetti_of_categories_refuses_a_collapsed_key():
    spec = PlotSpec(
        measures=["Outcome"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
    )
    with pytest.raises(RoleError, match="cannot be averaged"):
        resolve(spec, _table())


# --- per-field one-hot ------------------------------------------------------


def test_each_field_is_one_hot_over_its_own_categories():
    frame = pd.DataFrame(
        {
            "subject": ["1", "1", "2", "2"],
            "ColName": ["Outcome", "Side", "Outcome", "Side"],
            "Gait (text)": ["fall", "L", "ok", "R"],
        }
    )
    table = LongTable.from_frame(
        frame,
        factors=["subject", "ColName"],
        measures=["Gait (text)"],
        field_factors=["ColName"],
        schema_levels=["subject"],
    )
    spec = PlotSpec(
        measures=["Gait (text)"],
        roles={LEVEL_FACTOR: Role.GROUP, "ColName": Role.FACET, "subject": Role.COLLAPSE},
        groups=[LEVEL_FACTOR],
        kind=PlotKind.BAR,
    )
    _spec_out, rewritten = apply_categorical(spec, table)
    pairs = set(zip(rewritten.frame["ColName"], rewritten.frame[LEVEL_FACTOR]))
    assert pairs == {("Outcome", "fall"), ("Outcome", "ok"), ("Side", "L"), ("Side", "R")}
    [figure] = resolve(spec, table)
    counts = {
        panel.key["ColName"]: dict(zip(panel.frame[X], panel.frame[Y])) for panel in figure.panels
    }
    assert counts == {"Outcome": {"fall": 1.0, "ok": 1.0}, "Side": {"L": 1.0, "R": 1.0}}


# --- free text --------------------------------------------------------------


def test_free_text_is_refused_with_a_reason():
    many = {("1", "1"): [f"note {i}" for i in range(MAX_CATEGORY_LEVELS + 5)]}
    table = _table(_frame(many))
    report = capabilities(_spec(), table)
    assert not any(entry["available"] for entry in report["kinds"])
    assert "free text" in report["categorical"]["problem"]
    with pytest.raises(RoleError, match="free text"):
        resolve(_spec(), table)


# --- the capability report --------------------------------------------------


def test_count_and_proportion_are_two_bar_tiles():
    report = capabilities(_spec(), _table())
    tiles = {entry.get("id"): entry for entry in report["kinds"] if entry.get("id")}
    assert tiles["bar:count"]["selected"] is True
    assert tiles["bar:proportion"]["selected"] is False
    assert tiles["bar:proportion"]["available"] is True
    assert tiles["strip:value"]["available"] is True
    assert report["default"] == "bar"
    assert report["categorical"]["levels"] == ["fall", "ok"]


def test_the_report_names_the_category_factor():
    report = capabilities(_spec(), _table())
    assert LEVEL_FACTOR in {factor["name"] for factor in report["factors"]}


# --- stacked bars -----------------------------------------------------------


def test_stack_bases_pile_in_colour_order():
    frame = pd.DataFrame({"x": ["a", "a", "b"], "y": [30.0, 70.0, 5.0], "c": ["ok", "fall", "fall"]})
    assert stack_bases(frame, "x", "y", "c", ["fall", "ok"]).tolist() == [70.0, 0.0, 0.0]


def test_a_100_percent_stacked_bar_reaches_100_per_tick():
    spec = replace(
        _spec(CategoricalStat.PROPORTION, session=Role.GROUP),
        groups=[LEVEL_FACTOR, "session"],
        bar=BarOptions(stack=True),
    )
    [figure] = resolve(spec, _table())
    frame = figure.panels[0].frame
    tops = frame.groupby(X)[Y].sum()
    assert tops.tolist() == pytest.approx([100.0, 100.0])
    assert figure.y_limits[1] >= 100.0
    report = capabilities(spec, _table())
    assert report["bar"]["stack"] is True and report["bar"]["note"]


# --- the grid heatmap -------------------------------------------------------


def test_a_count_heatmap_is_categories_by_session():
    spec = replace(
        _spec(session=Role.GROUP), kind=PlotKind.HEATMAP, groups=[LEVEL_FACTOR, "session"], color=None
    )
    [figure] = resolve(spec, _table())
    frame = figure.panels[0].frame
    assert frame[GRID_COLUMNS].iloc[0] == ["1", "2"]
    assert frame[GRID_ROWS].iloc[0] == ["fall", "ok"]
    np.testing.assert_allclose(frame[Z].iloc[0], [[3.0, 2.0], [4.0, 4.0]])


def test_a_numeric_heatmap_needs_two_grouping_layers():
    frame = _frame()
    frame["Speed"] = np.arange(len(frame), dtype=float)
    table = LongTable.from_frame(frame.drop(columns="Outcome"), factors=KEYS, measures=["Speed"], schema_levels=KEYS)
    two = available_plots(Shape.SCALAR, {"subject": Role.GROUP, "session": Role.GROUP}, n_groups=2)
    one = available_plots(Shape.SCALAR, {"subject": Role.GROUP}, n_groups=1)
    assert PlotKind.HEATMAP in two and PlotKind.HEATMAP not in one
    spec = PlotSpec(
        measures=["Speed"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        kind=PlotKind.HEATMAP,
    )
    [figure] = resolve(spec, table)
    matrix = figure.panels[0].frame[Z].iloc[0]
    assert matrix.shape == (2, 2)
    expected = frame.groupby(["session", "subject"])["Speed"].mean().unstack("subject").to_numpy()
    np.testing.assert_allclose(matrix, expected)


# --- Save data --------------------------------------------------------------


def test_save_data_names_what_the_column_holds():
    data = plot_data(_spec(), _table())
    assert "Outcome, Count" in data.columns
    assert sorted(data["Outcome, Count"].tolist()) == [5.0, 8.0]


def test_save_data_writes_categories_as_text():
    data = plot_data(_spec(kind=PlotKind.STRIP), _table())
    assert set(data["Outcome"]) == {"fall", "ok"}
