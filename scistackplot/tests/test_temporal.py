"""Datetimes in plots (``scistackplot.temporal``; plan .claude/plan-plot-datetimes.md,
doc docs/claude/plot-datetimes.md).

User rules (2026-10-22): a datetime follows the same rules as every other
data type — a measure (y; x later) or a factor; mixed zones are a per-plot
choice (``PlotSpec.time_zone``: "local" = each value's own clock, default, or a
zone to convert to); a datetime measure offers no bar.

The fixture, one value per subject x session:

    subject 01 (Europe/Berlin):    s1 2026-07-01 14:00 CEST   s2 2026-07-08 14:30 CEST
    subject 02 (America/New_York): s1 2026-07-01 08:00 EDT    s2 2026-07-08 09:00 EDT
    subject 03 (no zone):          s1 2026-07-01 12:00        s2 NaT

Local time: 14:00 / 08:00 / 12:00 at s1. Converted to UTC every s1 value is
12:00 (the naive one placed as recorded, with a note). A paired difference
from s1: 01 -> 168.5 h, 02 -> 169 h, 03 dropped (NaT at s2).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    CompareMode,
    Comparison,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    capabilities,
    plot_data,
    resolve,
)
from scistackplot.aliases import display_text
from scistackplot.reduce import clear_plan_cache
from scistackplot.roles import RoleError
from scistackplot.shape import Shape
from scistackplot.spec import Filter, YAxis
from scistackplot.table import MISSING_LEVEL
from scistackplot.temporal import (
    BAR_REASON,
    LOCAL,
    LOG_REASON,
    MPL_DATE_AXIS_SOURCE,
    PERCENT_REASON,
    conversion_code,
    display_levels,
    factor_label_code,
    from_axis_ms,
    level_labels,
    normalize_mode,
    parse_limit,
    tick_format,
    wall_clock_ms,
)
from scistackplot.ylimits import pinned_ends


def ts(text, tz=None):
    return pd.Timestamp(text, tz=tz) if tz else pd.Timestamp(text)


def wall(text) -> float:
    return pd.Timestamp(text).value / 1e6


VALUES = {
    ("01", "s1"): ts("2026-07-01 14:00", "Europe/Berlin"),
    ("01", "s2"): ts("2026-07-08 14:30", "Europe/Berlin"),
    ("02", "s1"): ts("2026-07-01 08:00", "America/New_York"),
    ("02", "s2"): ts("2026-07-08 09:00", "America/New_York"),
    ("03", "s1"): ts("2026-07-01 12:00"),
    ("03", "s2"): pd.NaT,
}


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


@pytest.fixture
def frame() -> pd.DataFrame:
    rows = [(s, sess, v) for (s, sess), v in VALUES.items()]
    return pd.DataFrame(
        {
            "subject": [r[0] for r in rows],
            "session": [r[1] for r in rows],
            "When": pd.Series([r[2] for r in rows], dtype=object),
        }
    )


@pytest.fixture
def table(frame) -> LongTable:
    # The schema hierarchy is what lets a comparison pair each subject with
    # its own reference (roles.line_recurrence).
    return LongTable.from_frame(
        frame,
        factors=["subject", "session"],
        measures=["When"],
        schema_levels=["subject", "session"],
    )


def spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["When"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.STRIP,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _value(data: pd.DataFrame, column: str, subject: str, session: str):
    row = data[(data["subject"] == subject) & (data["session"] == session)]
    assert len(row) == 1, row
    return row[column].iloc[0]


# ---------------------------------------------------------------------------
# The table
# ---------------------------------------------------------------------------


class TestTable:
    def test_datetime_measure_is_temporal_scalar(self, table):
        info = table.measure("When")
        assert info.temporal and info.shape is Shape.SCALAR
        assert table.describe()["measures"][0]["temporal"] is True

    def test_inferred_as_a_measure(self, frame):
        inferred = LongTable.from_frame(frame)
        assert "When" in inferred.measure_names

    def test_datetime64_column(self):
        f = pd.DataFrame({"g": ["a", "b"], "t": pd.to_datetime(["2026-01-01", "2026-01-02"])})
        t = LongTable.from_frame(f, factors=["g"], measures=["t"])
        assert t.measure("t").temporal


# ---------------------------------------------------------------------------
# The conversion
# ---------------------------------------------------------------------------


class TestWallClock:
    def test_local_is_each_values_own_clock(self, frame):
        ms, report = wall_clock_ms(frame["When"], LOCAL)
        assert ms[0] == wall("2026-07-01 14:00")
        assert ms[2] == wall("2026-07-01 08:00")
        assert ms[4] == wall("2026-07-01 12:00")
        assert math.isnan(ms[5])
        assert report.n_aware == 4 and report.n_naive == 1
        assert set(report.zones) == {"Europe/Berlin", "America/New_York"}
        assert any("2 time zones" in n for n in report.notes("When"))

    def test_converted_instants_line_up(self, frame):
        ms, report = wall_clock_ms(frame["When"], "UTC")
        assert ms[0] == ms[2] == wall("2026-07-01 12:00")
        assert ms[4] == wall("2026-07-01 12:00")  # naive: placed as recorded
        (note,) = report.notes("When")
        assert "1 value(s) of When have no time zone" in note

    def test_vectorised_datetime64_path_agrees(self):
        s = pd.Series(pd.to_datetime(["2026-07-01 14:00", None])).dt.tz_localize("Europe/Berlin")
        local, _ = wall_clock_ms(s, LOCAL)
        utc, _ = wall_clock_ms(s, "UTC")
        assert local[0] == wall("2026-07-01 14:00") and math.isnan(local[1])
        assert utc[0] == wall("2026-07-01 12:00")

    def test_offset_mode(self, frame):
        ms, _ = wall_clock_ms(frame["When"], "+02:00")
        assert ms[2] == wall("2026-07-01 14:00")  # 08:00 EDT = 12:00 UTC = 14:00 +02

    def test_unknown_zone(self):
        with pytest.raises(ValueError, match="Mars/Olympus"):
            normalize_mode("Mars/Olympus")

    def test_round_trip_ms(self):
        # Float ms hold ~0.24 us in 2026: a figure's resolution, not storage's.
        back = from_axis_ms(wall("2026-07-01 12:34:56.789"))
        assert abs(back - pd.Timestamp("2026-07-01 12:34:56.789")) < pd.Timedelta("1us")


class TestFormats:
    @pytest.mark.parametrize(
        "span_ms,strf",
        [
            (3 * 365 * 86_400_000, "%Y"),
            (90 * 86_400_000, "%Y-%m"),
            (7 * 86_400_000, "%Y-%m-%d"),
            (6 * 3_600_000, "%m-%d %H:%M"),
            (30 * 60_000, "%H:%M"),
            (10_000, "%H:%M:%S"),
            (100, "%H:%M:%S.%f"),
        ],
    )
    def test_tick_format_by_span(self, span_ms, strf):
        assert tick_format(span_ms)[0] == strf

    def test_parse_limit(self):
        assert parse_limit("2026-07-01 12:00") == wall("2026-07-01 12:00")
        assert parse_limit(5.0) == 5.0
        assert parse_limit("") is None and parse_limit(None) is None

    def test_y_axis_keeps_date_text(self):
        y = YAxis.from_dict({"minimum": "2026-07-01 12:00", "maximum": "nonsense"})
        assert y.minimum == "2026-07-01 12:00" and y.maximum is None
        assert pinned_ends(y) == (wall("2026-07-01 12:00"), None)


# ---------------------------------------------------------------------------
# The figure
# ---------------------------------------------------------------------------


class TestResolve:
    def test_axis_is_temporal_and_titled(self, table):
        (figure,) = resolve(spec(), table)
        assert figure.temporal is not None
        assert figure.temporal.mode == LOCAL
        assert figure.labels.y.endswith("(local time)")

    def test_values_in_both_modes(self, table):
        local = plot_data(spec(), table)
        assert list(local.columns).count("When (local time)") == 1
        assert _value(local, "When (local time)", "01", "s1") == "2026-07-01T14:00:00"
        assert _value(local, "When (local time)", "02", "s1") == "2026-07-01T08:00:00"
        converted = plot_data(spec(time_zone="UTC"), table)
        assert _value(converted, "When (UTC)", "01", "s1") == "2026-07-01T12:00:00"
        assert _value(converted, "When (UTC)", "02", "s1") == "2026-07-01T12:00:00"

    def test_time_zone_is_part_of_the_plan(self, table):
        a = resolve(spec(), table)[0]
        b = resolve(spec(time_zone="UTC"), table)[0]
        assert a.temporal.mode == LOCAL and b.temporal.mode == "UTC"
        assert a.y_limits != b.y_limits

    def test_notes_reach_the_figure(self, table):
        (figure,) = resolve(spec(time_zone="UTC"), table)
        assert any("have no time zone" in n for n in figure.fanout_notes)

    def test_no_bar(self, table):
        caps = capabilities(spec(), table)
        bar = next(k for k in caps["kinds"] if k["kind"] == "bar")
        assert not bar["available"] and bar["reason"] == BAR_REASON
        assert "box" in caps["available"]
        with pytest.raises(RoleError, match="1970"):
            resolve(spec(kind=PlotKind.BAR), table)

    def test_log_axis_is_inert(self, table):
        (figure,) = resolve(spec(style=StyleOptions(log_y=True)), table)
        assert figure.spec.style.log_y is False
        assert LOG_REASON in figure.layout_notes

    def test_unknown_zone_refused(self, table):
        with pytest.raises(RoleError, match="time_zone"):
            resolve(spec(time_zone="Mars/Olympus"), table)

    def test_capability_summary(self, table):
        tz = capabilities(spec(), table)["time_zone"]
        assert tz["applies"] and tz["measure"] and tz["mode"] == LOCAL
        assert tz["zones"] == ["America/New_York", "Europe/Berlin"]

    def test_manual_date_limits(self, table):
        (figure,) = resolve(
            spec(y_axis=YAxis(minimum="2026-07-01 00:00", maximum="2026-07-09 00:00")), table
        )
        assert figure.y_limits == (wall("2026-07-01 00:00"), wall("2026-07-09 00:00"))

    def test_temporal_x_measure_refused(self, frame):
        f = frame.assign(X=frame["When"], Y=1.0)
        t = LongTable.from_frame(f, factors=["subject", "session"], measures=["Y", "X", "When"])
        with pytest.raises(RoleError, match="datetime x axis"):
            resolve(PlotSpec(measures=["Y"], x_measure="X", kind=PlotKind.SCATTER), t)


class TestCompare:
    def test_difference_is_hours(self, table):
        s = spec(comparison=Comparison(layer="session", level="s1"))
        data = plot_data(s, table)
        column = "Δ When (h, from session s1)"
        assert _value(data, column, "01", "s2") == pytest.approx(168.5)
        assert _value(data, column, "02", "s2") == pytest.approx(169.0)
        (figure,) = resolve(s, table)
        assert figure.temporal is None  # a duration: a number axis
        assert "(h, from session s1)" in figure.labels.y

    def test_percent_is_inert(self, table):
        s = spec(comparison=Comparison(layer="session", level="s1", mode=CompareMode.PERCENT))
        state = capabilities(s, table)["comparison"]
        assert PERCENT_REASON in str(state)
        (figure,) = resolve(s, table)
        assert figure.temporal is not None


class TestRender:
    def test_plotly_date_axis(self, table):
        pytest.importorskip("matplotlib")  # the auto text size lays out with it
        from scistackplot import render_plotly

        payload = render_plotly(resolve(spec(), table)[0])
        layout = payload["layout"]
        assert layout["yaxis"]["type"] == "date"
        assert layout["meta"]["temporal"]["mode"] == LOCAL

    def test_matplotlib_ticks_are_dates(self, table):
        pytest.importorskip("matplotlib")
        import matplotlib.pyplot as plt

        from scistackplot import render_matplotlib

        figure = resolve(spec(), table)[0]
        fig = render_matplotlib(figure)
        try:
            ax = fig.axes[0]
            value = wall("2026-07-03 00:00")
            assert ax.yaxis.get_major_formatter()(value, 0) == figure.temporal.format(value)
            ticks = ax.yaxis.get_major_locator()()
            assert len(ticks) >= 2
        finally:
            plt.close(fig)

    def test_mpl_source_runs_alone(self):
        pytest.importorskip("matplotlib")
        import matplotlib.pyplot as plt

        namespace: dict = {}
        exec(MPL_DATE_AXIS_SOURCE, namespace)  # noqa: S102
        fig, ax = plt.subplots()
        try:
            ax.set_ylim(wall("2026-07-01"), wall("2026-07-08"))
            namespace["_scistack_date_axis"](ax.yaxis, "%Y-%m-%d")
            assert ax.yaxis.get_major_formatter()(wall("2026-07-03"), 0) == "2026-07-03"
        finally:
            plt.close(fig)


class TestExport:
    def test_conversion_code_matches(self, frame):
        for mode in (LOCAL, "UTC", "+02:00", "Europe/Berlin"):
            namespace = {"pd": pd, "df": frame.copy()}
            exec("\n".join(conversion_code("When", mode)), namespace)  # noqa: S102
            expected, _ = wall_clock_ms(frame["When"], mode)
            np.testing.assert_array_equal(namespace["df"]["When"].to_numpy(), expected)

    def test_generated_function_draws_the_same_ticks(self, table, frame):
        pytest.importorskip("seaborn")
        import matplotlib.pyplot as plt

        from scistackplot import generate_plot_function

        source = generate_plot_function(spec(kind=PlotKind.BOX), table)
        assert "_scistack_date_axis" in source and "_wall_ms" in source
        namespace: dict = {}
        exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
        name = next(k for k in namespace if k.startswith("plot_"))
        fig = namespace[name](frame.copy(), "figure.png")
        try:
            figure = resolve(spec(kind=PlotKind.BOX), table)[0]
            value = wall("2026-07-03 00:00")
            ax = fig.axes[0]
            assert ax.yaxis.get_major_formatter()(value, 0) == figure.temporal.format(value)
            assert ax.get_ylabel().endswith("(local time)")
        finally:
            plt.close(fig)


# ---------------------------------------------------------------------------
# Factors
# ---------------------------------------------------------------------------

VISITS = [
    ("01", ts("2026-07-01 14:00", "Europe/Berlin"), 1.0),
    ("02", ts("2026-07-01 08:00", "America/New_York"), 2.0),  # the same instant
    ("03", ts("2026-06-30 09:00"), 3.0),
]


@pytest.fixture
def visit_table() -> LongTable:
    f = pd.DataFrame(
        {
            "subject": [v[0] for v in VISITS],
            "Visit": pd.Series([v[1] for v in VISITS], dtype=object),
            "Score": [v[2] for v in VISITS],
        }
    )
    return LongTable.from_frame(f, factors=["subject", "Visit"], measures=["Score"])


class TestFactor:
    def test_identity_labels_in_time_order(self, visit_table):
        info = visit_table.factor("Visit")
        assert info.temporal
        assert info.levels == [
            "2026-06-30 09:00",
            "2026-07-01 08:00 EDT",
            "2026-07-01 14:00 CEST",
        ]
        assert set(visit_table.frame["Visit"]) == set(info.levels)
        assert visit_table.describe()["factors"][1]["temporal"] is True

    def test_converted_mode_relabels_display_only(self, visit_table):
        text = display_text(PlotSpec(measures=["Score"], time_zone="UTC"), visit_table)
        berlin = text.level("Visit", "2026-07-01 14:00 CEST")
        new_york = text.level("Visit", "2026-07-01 08:00 EDT")
        # Same instant: one text, told apart by the original zone.
        assert berlin == "2026-07-01 12:00 UTC (from Europe/Berlin)"
        assert new_york == "2026-07-01 12:00 UTC (from America/New_York)"
        assert text.level("Visit", "2026-06-30 09:00") == "2026-06-30 09:00"  # naive
        local = display_text(PlotSpec(measures=["Score"]), visit_table)
        assert local.level("Visit", "2026-07-01 14:00 CEST") == "2026-07-01 14:00 CEST"

    def test_filter_keys_on_identity_in_any_mode(self, visit_table):
        for mode in (LOCAL, "UTC"):
            s = PlotSpec(
                measures=["Score"],
                roles={"Visit": Role.GROUP, "subject": Role.COLLAPSE},
                groups=["Visit"],
                kind=PlotKind.STRIP,
                time_zone=mode,
                filters=[Filter(column="Visit", include=["2026-07-01 14:00 CEST"])],
            )
            data = plot_data(s, visit_table)
            assert list(data["subject"]) == ["01"], mode

    def test_group_by_a_datetime(self, visit_table):
        s = PlotSpec(
            measures=["Score"],
            roles={"Visit": Role.GROUP, "subject": Role.COLLAPSE},
            groups=["Visit"],
            kind=PlotKind.STRIP,
        )
        (figure,) = resolve(s, visit_table)
        assert [str(v) for v in figure.x_order] == visit_table.factor("Visit").levels
        assert capabilities(s, visit_table)["time_zone"]["factors"] == ["Visit"]

    def test_precision_follows_the_data(self):
        labels = level_labels(pd.Series([ts("2026-07-01"), ts("2026-07-02")], dtype=object))
        assert labels.order == ["2026-07-01", "2026-07-02"]
        labels = level_labels(pd.Series([ts("2026-07-01 10:00:05"), ts("2026-07-01")], dtype=object))
        assert labels.order == ["2026-07-01 00:00:00", "2026-07-01 10:00:05"]

    def test_abbreviation_clash_uses_full_names(self):
        dublin = ts("2026-07-01 12:00", "Europe/Dublin")  # IST, UTC+1
        kolkata = ts("2026-07-01 12:00", "Asia/Kolkata")  # IST, UTC+5:30
        labels = level_labels(pd.Series([dublin, kolkata], dtype=object))
        assert set(labels.order) == {
            "2026-07-01 12:00 Europe/Dublin",
            "2026-07-01 12:00 Asia/Kolkata",
        }

    def test_named_missing_level(self):
        f = pd.DataFrame(
            {
                "g": pd.Series([ts("2026-07-01"), MISSING_LEVEL], dtype=object),
                "y": [1.0, 2.0],
            }
        )
        t = LongTable.from_frame(f, factors=["g"], measures=["y"])
        assert t.factor("g").levels == ["2026-07-01", MISSING_LEVEL]
        assert list(t.frame["g"]) == ["2026-07-01", MISSING_LEVEL]

    def test_factor_label_code_matches(self):
        series = pd.Series([v[1] for v in VISITS] + [None], dtype=object)
        made = level_labels(series)
        namespace = {"pd": pd, "df": pd.DataFrame({"Visit": series})}
        exec(  # noqa: S102
            "\n".join(factor_label_code("Visit", made.values, made.strftime, MISSING_LEVEL)),
            namespace,
        )
        assert list(namespace["df"]["Visit"]) == [*made.labels[:3], MISSING_LEVEL]

    def test_display_levels_local_is_empty(self, visit_table):
        info = visit_table.factor("Visit")
        assert display_levels(info.temporal_values, info.temporal_format, LOCAL) == {}


class TestSpec:
    def test_round_trip(self):
        s = PlotSpec(measures=["When"], time_zone="UTC")
        assert PlotSpec.from_dict(s.to_dict()).time_zone == "UTC"
        assert PlotSpec.from_dict({"measures": ["When"]}).time_zone == LOCAL
