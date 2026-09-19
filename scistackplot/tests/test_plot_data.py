"""
"Save data": the long table a plot is drawn from (``scistackplot.plot_data``).

The point of the feature is parity — the statistics run on the rows the figure
shows — so most of these tests recompute a figure's marks FROM the exported
frame and compare them to what ``resolve`` drew. See
docs/claude/plot-data-export.md.

The main fixture is the user's example: ``[subject, session, speed, trial,
cycle]``, session grouped, one figure per speed, subject / trial / cycle
collapsed. It is unbalanced on purpose (subject 02 has two trials, the others
three), so a nested mean and a pooled one differ, and its subject IDs include
"10" so a lexicographic sort would put it before "02".
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    capabilities,
    data_export_options,
    plot_data,
    resolve,
)
from scistackplot.resolved import X, Y, Y_HIGH, Y_LOW
from scistackplot.roles import complete_roles

SUBJECTS = ["01", "02", "10"]
SESSIONS = ["pre", "post"]
SPEEDS = ["slow", "fast"]


@pytest.fixture
def gait() -> LongTable:
    rows = []
    rng = np.random.default_rng(11)
    for subject in SUBJECTS:
        trials = ["1", "2"] if subject == "02" else ["1", "2", "3"]
        for session in SESSIONS:
            for speed in SPEEDS:
                for trial in trials:
                    for cycle in ["1", "2"]:
                        rows.append(
                            {
                                "subject": subject,
                                "session": session,
                                "speed": speed,
                                "trial": trial,
                                "cycle": cycle,
                                "M": float(rng.normal(10.0, 2.0)),
                            }
                        )
    # Shuffled: database order is not level order.
    frame = pd.DataFrame(rows).sample(frac=1.0, random_state=5).reset_index(drop=True)
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "speed", "trial", "cycle"],
        measures=["M"],
        name="M",
        level_order={"session": SESSIONS, "speed": SPEEDS},
        schema_levels=["subject", "session", "speed", "trial", "cycle"],
    )


def _spec(kind=PlotKind.BAR, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _expected_sample(table: LongTable) -> pd.DataFrame:
    """The nested chain written out by hand: cycle within trial, then trial
    within subject."""
    raw = table.frame
    per_trial = raw.groupby(["subject", "session", "speed", "trial"], as_index=False)["M"].mean()
    return per_trial.groupby(["subject", "session", "speed"], as_index=False)["M"].mean()


# --- the default: the plotted sample -----------------------------------------


def test_the_default_is_the_sample_with_the_figure_keys(gait):
    data = plot_data(_spec(), gait)
    assert list(data.columns) == ["subject", "session", "speed", "M"]
    assert len(data) == len(SUBJECTS) * len(SESSIONS) * len(SPEEDS)
    merged = data.merge(_expected_sample(gait), on=["subject", "session", "speed"])
    assert merged["M_x"].to_numpy() == pytest.approx(merged["M_y"].to_numpy())


def test_one_csv_covers_every_figure_of_the_fan_out(gait):
    figures = resolve(_spec(), gait)
    data = plot_data(_spec(), gait)
    assert len(figures) == len(SPEEDS)
    assert sorted(data["speed"].unique()) == sorted(SPEEDS)
    assert {f.figure_key["speed"] for f in figures} == set(data["speed"])


def test_bar_marks_recompute_from_the_csv(gait):
    """For every figure and every mark: the CSV's mean and SD over the sample
    ARE the drawn bar and its error bar."""
    data = plot_data(_spec(), gait)
    for figure in resolve(_spec(), gait):
        rows = data[data["speed"] == figure.figure_key["speed"]]
        (panel,) = figure.panels
        drawn = panel.frame.set_index(X)
        for session, sample in rows.groupby("session"):
            assert drawn.loc[session, Y] == pytest.approx(sample["M"].mean())
            sd = sample["M"].std(ddof=1)
            assert drawn.loc[session, Y_HIGH] - drawn.loc[session, Y] == pytest.approx(sd)
            assert drawn.loc[session, Y] - drawn.loc[session, Y_LOW] == pytest.approx(sd)


@pytest.mark.parametrize("kind", [PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP])
def test_every_non_summary_kind_draws_exactly_the_csv_rows(gait, kind):
    data = plot_data(_spec(kind=kind), gait)
    for figure in resolve(_spec(kind=kind), gait):
        rows = data[data["speed"] == figure.figure_key["speed"]]
        (panel,) = figure.panels
        for session, sample in rows.groupby("session"):
            drawn = panel.frame[panel.frame[X] == session][Y]
            assert sorted(drawn) == pytest.approx(sorted(sample["M"]))


def test_the_csv_does_not_depend_on_the_kind(gait):
    """Every kind draws the same sample (schema-level parity), so the file is
    the same whichever kind is on screen."""
    bar = plot_data(_spec(kind=PlotKind.BAR), gait)
    for kind in (PlotKind.BOX, PlotKind.SCATTER, PlotKind.STRIP, PlotKind.VIOLIN):
        pd.testing.assert_frame_equal(plot_data(_spec(kind=kind), gait), bar)


# --- nested vs pooled (the worked example in grouping-and-collapse.md) -------


@pytest.fixture
def unbalanced() -> LongTable:
    rows = [
        ("01", "pre", "1", 1.0),
        ("01", "pre", "2", 2.0),
        ("01", "pre", "3", 3.0),
        ("02", "pre", "1", 9.0),
        ("01", "post", "1", 10.0),
        ("01", "post", "2", 20.0),
        ("02", "post", "1", 30.0),
    ]
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        level_order={"session": ["pre", "post"]},
        schema_levels=["subject", "session", "trial"],
    )


def _worked(pooled: bool) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD, pooled=pooled),
    )


def test_nested_exports_one_row_per_subject(unbalanced):
    data = plot_data(_worked(False), unbalanced)
    assert list(data.columns) == ["subject", "session", "M"]
    pre = data[data["session"] == "pre"]
    assert sorted(pre["M"]) == pytest.approx([2.0, 9.0])


def test_pooled_exports_every_trial(unbalanced):
    data = plot_data(_worked(True), unbalanced)
    assert list(data.columns) == ["subject", "session", "trial", "M"]
    pre = data[data["session"] == "pre"]
    assert sorted(pre["M"]) == pytest.approx([1.0, 2.0, 3.0, 9.0])
    assert pre["M"].mean() == pytest.approx(3.75), "the pooled bar's centre"


def test_pooled_offers_no_deeper_depth(unbalanced):
    table = unbalanced
    spec = _worked(True)
    options = data_export_options(spec, complete_roles(spec, table), table)
    assert len(options.depths) == 1
    assert options.pooled


# --- depth: keep the lower levels ---------------------------------------------


def test_the_depth_picker_offers_the_sample_then_each_deeper_key(gait):
    spec = _spec()
    options = data_export_options(spec, complete_roles(spec, gait), gait)
    assert options.available
    assert [d.key for d in options.depths] == ["subject", "trial", "cycle"]
    assert options.default.key == "subject"
    assert options.depths[0].averaged == ["cycle", "trial"]
    assert options.depths[1].averaged == ["cycle"]
    assert options.depths[2].averaged == []
    assert options.depths[1].columns == ["subject", "session", "speed", "trial", "M"]
    assert "raw" in options.depths[2].label


def test_depth_trial_keeps_every_trial_with_cycles_averaged(gait):
    data = plot_data(_spec(), gait, depth="trial")
    assert list(data.columns) == ["subject", "session", "speed", "trial", "M"]
    expected = gait.frame.groupby(
        ["subject", "session", "speed", "trial"], as_index=False
    )["M"].mean()
    assert len(data) == len(expected)
    merged = data.merge(expected, on=["subject", "session", "speed", "trial"])
    assert merged["M_x"].to_numpy() == pytest.approx(merged["M_y"].to_numpy())


def test_a_deeper_csv_re_collapses_to_the_default(gait):
    """Averaging trial back out of the depth=trial file gives the default
    file — the two depths are cuts of ONE chain."""
    deeper = plot_data(_spec(), gait, depth="trial")
    back = deeper.groupby(["subject", "session", "speed"], as_index=False, sort=False)["M"].mean()
    default = plot_data(_spec(), gait)
    merged = default.merge(back, on=["subject", "session", "speed"])
    assert len(merged) == len(default)
    assert merged["M_x"].to_numpy() == pytest.approx(merged["M_y"].to_numpy())


def test_the_deepest_depth_is_the_raw_rows(gait):
    data = plot_data(_spec(), gait, depth="cycle")
    assert list(data.columns) == ["subject", "session", "speed", "trial", "cycle", "M"]
    assert len(data) == len(gait.frame)
    assert sorted(data["M"]) == pytest.approx(sorted(gait.frame["M"]))


def test_an_unknown_depth_names_the_choices(gait):
    with pytest.raises(ValueError, match="subject"):
        plot_data(_spec(), gait, depth="session")


# --- nothing collapsed -------------------------------------------------------


def test_with_nothing_collapsed_the_rows_are_the_marks(unbalanced):
    spec = PlotSpec(
        measures=["M"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session"],
        kind=PlotKind.BAR,
    )
    options = data_export_options(spec, complete_roles(spec, unbalanced), unbalanced)
    assert [d.key for d in options.depths] == [None]
    data = plot_data(spec, unbalanced)
    assert len(data) == len(unbalanced.frame)
    drawn = sum(len(p.frame) for f in resolve(spec, unbalanced) for p in f.panels)
    assert drawn == len(data), "one bar per row"


# --- order and spelling ------------------------------------------------------


def test_rows_follow_the_declared_level_order(gait):
    data = plot_data(_spec(), gait)
    assert list(dict.fromkeys(data["subject"])) == ["01", "02", "10"], "natural, not lexicographic"
    first = data[data["subject"] == "01"]
    assert list(dict.fromkeys(first["session"])) == SESSIONS
    assert list(dict.fromkeys(first["speed"])) == SPEEDS


def test_zero_padded_keys_survive_the_csv(gait):
    buffer = io.StringIO()
    plot_data(_spec(), gait).to_csv(buffer, index=False)
    text = buffer.getvalue()
    assert "\n01," in text, "written as the stored string, not 1"
    back = pd.read_csv(io.StringIO(text), dtype={"subject": str})
    assert set(back["subject"]) == set(SUBJECTS)


def test_rows_the_figure_does_not_draw_are_not_written(unbalanced):
    frame = unbalanced.frame.copy()
    frame.loc[frame["subject"] == "02", "M"] = np.nan
    table = LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )
    data = plot_data(_worked(False), table)
    assert set(data["subject"]) == {"01"}


def test_filters_apply(gait):
    from scistackplot import Filter

    spec = _spec(filters=[Filter(column="subject", include=["01", "10"])])
    data = plot_data(spec, gait)
    assert set(data["subject"]) == {"01", "10"}


# --- what cannot be exported ---------------------------------------------------


@pytest.fixture
def series() -> LongTable:
    rows = [
        {"subject": s, "trial": t, "S": [1.0, 2.0, float(i)]}
        for i, (s, t) in enumerate((s, t) for s in ["01", "02"] for t in ["1", "2"])
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial"],
        measures=["S"],
        name="S",
        schema_levels=["subject", "trial"],
    )


def test_a_raw_1d_plot_is_refused_with_the_reason(series):
    spec = PlotSpec(
        measures=["S"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAND,
    )
    with pytest.raises(ValueError, match="scalar"):
        plot_data(spec, series)
    report = capabilities(spec, series)["data_export"]
    assert report["available"] is False
    assert "1-D" in report["reason"]


def test_a_1d_measure_drawn_as_a_scalar_kind_is_exported(series):
    """The cell collapse made it scalar: each record's cell statistic is what
    the box draws, and what is written."""
    spec = PlotSpec(
        measures=["S"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BOX,
    )
    data = plot_data(spec, series)
    assert list(data.columns) == ["subject", "trial", "S"]
    assert len(data) == 4
    assert data["S"].to_numpy() == pytest.approx([1.0, 4.0 / 3.0, 5.0 / 3.0, 2.0])


def test_the_capability_report_carries_the_depths(gait):
    report = capabilities(_spec(), gait)["data_export"]
    assert report["available"] is True
    assert report["default"] == "subject"
    assert [d["key"] for d in report["depths"]] == ["subject", "trial", "cycle"]
    assert report["depths"][0]["columns"] == ["subject", "session", "speed", "M"]


def test_the_export_logs_what_it_wrote(gait, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="scistackplot"):
        plot_data(_spec(), gait)
    lines = [r.getMessage() for r in caplog.records if "[plot-data]" in r.getMessage()]
    assert any("cycle -> trial -> subject (sample)" in line for line in lines), lines
    assert any("depth=subject" in line for line in lines), lines


# --- struct / table variables: one column per field ---------------------------
#
# A dict/struct variable with scalar fields reaches the plot melted: one
# measure column plus a `ColName` field factor (ScidbSource._melt_fields). The
# CSV spreads it back to one column per field by default (the user's choice,
# 2026-09-19) — a pure reshape of the long rows — and only ever spreads the
# FIELD factor.

FIELDS = ["RTA", "RMG", "LTA"]  # declared order, deliberately not alphabetical


@pytest.fixture
def struct_scalar() -> LongTable:
    rows = []
    rng = np.random.default_rng(21)
    for subject in SUBJECTS:
        trials = ["1"] if subject == "02" else ["1", "2"]
        for session in SESSIONS:
            for trial in trials:
                for field in FIELDS:
                    rows.append(
                        {
                            "subject": subject,
                            "session": session,
                            "trial": trial,
                            "ColName": field,
                            "Peak": float(rng.normal(1.0, 0.2)),
                        }
                    )
    return _struct_table(pd.DataFrame(rows))


def _struct_table(frame: pd.DataFrame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial", "ColName"],
        measures=["Peak"],
        field_factors=["ColName"],
        name="Peak",
        level_order={"session": SESSIONS, "ColName": [f for f in FIELDS if f in set(frame["ColName"])]},
        schema_levels=["subject", "session", "trial"],
    )


def _struct_spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["Peak"],
        roles={
            "ColName": Role.FACET,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _trial_means(table: LongTable) -> pd.DataFrame:
    return table.frame.groupby(["subject", "session", "ColName"], as_index=False)["Peak"].mean()


def test_a_struct_is_written_one_column_per_field_by_default(struct_scalar):
    data = plot_data(_struct_spec(), struct_scalar)
    assert list(data.columns) == ["subject", "session", *FIELDS], "declared field order"
    assert len(data) == len(SUBJECTS) * len(SESSIONS)
    expected = _trial_means(struct_scalar)
    for _, row in expected.iterrows():
        cell = data[(data["subject"] == row["subject"]) & (data["session"] == row["session"])]
        assert cell[row["ColName"]].item() == pytest.approx(row["Peak"])


def test_the_long_form_is_one_row_per_field(struct_scalar):
    data = plot_data(_struct_spec(), struct_scalar, fields_as_columns=False)
    assert list(data.columns) == ["subject", "session", "ColName", "Peak"]
    assert len(data) == len(SUBJECTS) * len(SESSIONS) * len(FIELDS)


def test_the_wide_file_is_a_pure_reshape_of_the_long_one(struct_scalar):
    long = plot_data(_struct_spec(), struct_scalar, fields_as_columns=False)
    wide = plot_data(_struct_spec(), struct_scalar)
    pivoted = long.pivot(index=["subject", "session"], columns="ColName", values="Peak")
    for _, row in wide.iterrows():
        for field in FIELDS:
            assert row[field] == pytest.approx(pivoted.loc[(row["subject"], row["session"]), field])


def test_every_field_panel_recomputes_from_its_column(struct_scalar):
    """Parity per field: each field's panel is a bar per session over the
    subjects, and the field's column holds exactly those subject values."""
    wide = plot_data(_struct_spec(), struct_scalar)
    (figure,) = resolve(_struct_spec(), struct_scalar)
    assert {p.key["ColName"] for p in figure.panels} == set(FIELDS)
    for panel in figure.panels:
        field = panel.key["ColName"]
        drawn = panel.frame.set_index(X)
        for session, rows in wide.groupby("session"):
            assert drawn.loc[session, Y] == pytest.approx(rows[field].mean())
            sd = rows[field].std(ddof=1)
            assert drawn.loc[session, Y_HIGH] - drawn.loc[session, Y] == pytest.approx(sd)


def test_the_options_report_the_wide_header(struct_scalar):
    spec = _struct_spec()
    options = data_export_options(spec, complete_roles(spec, struct_scalar), struct_scalar)
    assert options.field_factor == "ColName"
    assert options.depths[0].columns == ["subject", "session", "ColName", "Peak"]
    assert options.depths[0].wide_columns == ["subject", "session", *FIELDS]
    trial = options.depth("trial")
    assert trial.wide_columns == ["subject", "session", "trial", *FIELDS]
    report = capabilities(spec, struct_scalar)["data_export"]
    assert report["field_factor"] == "ColName"
    assert report["depths"][0]["wide_columns"] == ["subject", "session", *FIELDS]


def test_a_deeper_depth_keeps_trials_in_the_wide_file(struct_scalar):
    data = plot_data(_struct_spec(), struct_scalar, depth="trial")
    assert list(data.columns) == ["subject", "session", "trial", *FIELDS]
    # subject 02 has one trial, the others two, in each of two sessions.
    assert len(data) == (2 + 1 + 2) * len(SESSIONS)


def test_a_field_a_record_lacks_is_an_empty_cell(struct_scalar):
    frame = struct_scalar.frame
    frame = frame[~((frame["subject"] == "10") & (frame["session"] == "pre") & (frame["ColName"] == "LTA"))]
    data = plot_data(_struct_spec(), _struct_table(frame.reset_index(drop=True)))
    row = data[(data["subject"] == "10") & (data["session"] == "pre")]
    assert len(row) == 1, "the record is still written"
    assert np.isnan(row["LTA"].item())
    assert not np.isnan(row["RTA"].item())
    assert len(data) == len(SUBJECTS) * len(SESSIONS)


def test_a_field_filter_limits_the_columns(struct_scalar):
    from scistackplot import Filter

    spec = _struct_spec(filters=[Filter(column="ColName", include=["RTA", "LTA"])])
    data = plot_data(spec, struct_scalar)
    assert list(data.columns) == ["subject", "session", "RTA", "LTA"]
    options = data_export_options(spec, complete_roles(spec, struct_scalar), struct_scalar)
    assert options.default.wide_columns == ["subject", "session", "RTA", "LTA"]


def test_a_collapsed_field_is_averaged_and_never_spread(struct_scalar):
    """ColName collapsed: fields average away FIRST (they sit inside a record),
    so the default file has no field to spread; the raw depth keeps them and
    is spread."""
    spec = _struct_spec(
        roles={
            "ColName": Role.COLLAPSE,
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        }
    )
    options = data_export_options(spec, complete_roles(spec, struct_scalar), struct_scalar)
    assert options.default.wide_columns is None
    default = plot_data(spec, struct_scalar)
    assert list(default.columns) == ["subject", "session", "Peak"]
    raw = plot_data(spec, struct_scalar, depth="ColName")
    assert list(raw.columns) == ["subject", "session", "trial", *FIELDS]


def test_a_field_named_like_a_column_gets_the_measure_prefix(struct_scalar):
    frame = struct_scalar.frame.copy()
    frame["ColName"] = frame["ColName"].replace({"LTA": "session"})
    table = LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial", "ColName"],
        measures=["Peak"],
        field_factors=["ColName"],
        name="Peak",
        level_order={"session": SESSIONS, "ColName": ["RTA", "RMG", "session"]},
        schema_levels=["subject", "session", "trial"],
    )
    data = plot_data(_struct_spec(), table)
    assert list(data.columns) == ["subject", "session", "Peak.RTA", "Peak.RMG", "Peak.session"]


def test_a_non_struct_variable_ignores_the_option(gait):
    assert list(plot_data(_spec(), gait, fields_as_columns=True).columns) == list(
        plot_data(_spec(), gait, fields_as_columns=False).columns
    )
    spec = _spec()
    assert data_export_options(spec, complete_roles(spec, gait), gait).field_factor is None


def test_the_spread_is_logged(struct_scalar, caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="scistackplot"):
        plot_data(_struct_spec(), struct_scalar)
    assert any(
        "one column per ColName" in r.getMessage() and "RTA" in r.getMessage()
        for r in caplog.records
    )
