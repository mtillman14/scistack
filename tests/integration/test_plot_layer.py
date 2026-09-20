"""
scistackplot + scistackplotdb over the stored records.

The plotting layer's own suites use hand-built tables. These run it over a
table that scidb actually produced from the example pipeline — five schema
keys, a struct variable melted into a ``ColName`` factor, a 1-D variable, a
categorical subject-level column — and check what the Plot Studio would
show and what "Save data (CSV)" would write.
"""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    FactorVariable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    capabilities,
    data_export_options,
    default_spec,
    plot_data,
    resolve,
)
from scistackplot.resolved import X, Y, Y_HIGH  # noqa: E402
from scistackplot.roles import complete_roles  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

from conftest import JOINTS, N_CYCLES, SESSIONS, SUBJECTS  # noqa: E402


@pytest.fixture(scope="module")
def source(example_db):
    return ScidbSource(example_db)


@pytest.fixture(scope="module")
def symmetry(source):
    return source.get_table(["CycleSymmetry"])


@pytest.fixture(scope="module")
def waveform(source):
    return source.get_table(["CycleWaveform"])


# --- the table scidb hands the plot layer ------------------------------------------


def test_the_struct_variable_arrives_melted(symmetry):
    """Three joint columns become one measure plus a ``ColName`` factor —
    the field factor the plot layer facets by default."""
    assert "CycleSymmetry" in symmetry.measure_names
    fields = [f.name for f in symmetry.field_factors]
    assert len(fields) == 1
    assert set(symmetry.factor(fields[0]).levels) == set(JOINTS)
    assert len(symmetry.frame) == N_CYCLES * len(JOINTS)
    assert symmetry.schema_levels == ["subject", "session", "speed", "trial", "cycle"]


def test_schema_key_depths_follow_the_hierarchy(symmetry):
    depths = symmetry.factor_depths
    assert depths["subject"] < depths["session"] < depths["speed"] < depths["trial"] < depths["cycle"]


def test_the_1d_variable_is_a_series(waveform):
    assert waveform.shape_of("CycleWaveform") is Shape.SERIES_1D


# --- what opens, and what is drawn -----------------------------------------------


def _bar_spec(table) -> PlotSpec:
    """session grouped, one figure per speed, subject/trial/cycle collapsed —
    the user's own example, with the joints as separate panels."""
    field = table.field_factors[0].name
    return PlotSpec(
        measures=["CycleSymmetry"],
        roles={
            field: Role.FACET,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )


def test_the_default_spec_resolves(symmetry):
    spec = default_spec(symmetry)
    figures = resolve(spec, symmetry)
    assert figures and all(f.panels for f in figures)


def test_bar_over_the_collapse_chain_draws_one_bar_per_session(symmetry):
    figures = resolve(_bar_spec(symmetry), symmetry)
    assert len(figures) == 2, "one figure per speed"
    for figure in figures:
        assert len(figure.panels) == len(JOINTS), "one panel per joint"
        for panel in figure.panels:
            assert set(panel.frame[X]) == set(SESSIONS)
            assert (panel.frame[Y_HIGH] >= panel.frame[Y]).all()


def test_save_data_matches_the_drawn_bars(symmetry):
    """The parity the CSV feature promises, on real data: mean and SD over
    the exported subject rows ARE the bar and its error bar, per joint."""
    spec = _bar_spec(symmetry)
    field = symmetry.field_factors[0].name
    long = plot_data(spec, symmetry, fields_as_columns=False)
    assert list(long.columns) == ["subject", "session", "speed", field, "CycleSymmetry"]
    assert len(long) == len(SUBJECTS) * len(SESSIONS) * 2 * len(JOINTS)
    for figure in resolve(spec, symmetry):
        rows = long[long["speed"] == figure.figure_key["speed"]]
        for panel in figure.panels:
            joint_rows = rows[rows[field] == panel.key[field]]
            drawn = panel.frame.set_index(X)
            for session, sample in joint_rows.groupby("session"):
                assert drawn.loc[session, Y] == pytest.approx(sample["CycleSymmetry"].mean())
                sd = sample["CycleSymmetry"].std(ddof=1)
                assert drawn.loc[session, Y_HIGH] - drawn.loc[session, Y] == pytest.approx(sd)


def test_save_data_spreads_the_joints_one_column_each(symmetry):
    wide = plot_data(_bar_spec(symmetry), symmetry)
    assert list(wide.columns) == ["subject", "session", "speed", *JOINTS]
    assert len(wide) == len(SUBJECTS) * len(SESSIONS) * 2
    assert list(dict.fromkeys(wide["subject"])) == SUBJECTS, "declared subject order"


def test_save_data_offers_every_collapsed_depth(symmetry):
    spec = _bar_spec(symmetry)
    options = data_export_options(spec, complete_roles(spec, symmetry), symmetry)
    assert options.available
    assert [d.key for d in options.depths] == ["subject", "trial", "cycle"]
    assert options.field_factor == symmetry.field_factors[0].name


def test_a_1d_measure_draws_a_band_and_refuses_the_csv(waveform):
    field = waveform.field_factors[0].name
    spec = PlotSpec(
        measures=["CycleWaveform"],
        roles={
            field: Role.FACET,
            "subject": Role.ITERATE,
            "session": Role.ITERATE,
            "speed": Role.GROUP,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["speed"],
        color="speed",
        kind=PlotKind.BAND,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    figures = resolve(spec, waveform)
    assert len(figures) == len(SUBJECTS) * len(SESSIONS)
    panel = figures[0].panels[0]
    assert panel.frame[X].nunique() == 51, "one position per sample"
    report = capabilities(spec, waveform)
    assert report["shape"] == "1d"
    assert report["data_export"]["available"] is False
    with pytest.raises(ValueError, match="scalar"):
        plot_data(spec, waveform)


def test_a_scalar_kind_on_the_1d_measure_collapses_each_cell_first(waveform):
    """Choosing a box for the waveform reduces each curve to one value (the
    cell statistic) — and THEN the CSV is available, with that value."""
    field = waveform.field_factors[0].name
    spec = PlotSpec(
        measures=["CycleWaveform"],
        roles={
            field: Role.FACET,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BOX,
    )
    report = capabilities(spec, waveform)
    assert report["cell_collapse"]["active"] is True
    assert report["data_export"]["available"] is True
    data = plot_data(spec, waveform)
    assert list(data.columns) == ["subject", "session", "speed", *JOINTS]


def test_a_subject_level_column_groups_the_plot(source):
    """``Demographics.group`` (control / treatment) joined in as a factor —
    stratifying a cycle-level measure by something recorded per subject."""
    table = source.get_table(
        ["CycleSymmetry"],
        factor_variables=[FactorVariable(variable="Demographics", column="group")],
    )
    names = [f.name for f in table.factors]
    group = next(n for n in names if "group" in n.lower())
    assert set(table.factor(group).levels) <= {"control", "treatment"}
    # Every cycle row carries its subject's group.
    assert table.frame[group].notna().all()
    field = table.field_factors[0].name
    spec = PlotSpec(
        measures=["CycleSymmetry"],
        roles={
            field: Role.FACET,
            group: Role.GROUP,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session", group],
        color=group,
        kind=PlotKind.BAR,
    )
    figures = resolve(spec, table)
    assert figures and figures[0].panels
    data = plot_data(spec, table, fields_as_columns=False)
    assert group in data.columns
