"""``PlotSpec.show_missing_levels``: an x level the PLOT holds but a figure
has no data for keeps an empty tick (on, the default), or is left off that
figure's axis (off).

"Missing" is relative to the plot's FILTERED rows across every figure of the
fan-out (``reduce._x_axis_levels`` reads ``_Plan.frame``): subject 02 never
ran ``mid``, so subject 02's figure is missing ``mid``; a level the user
filtered out is not missing, it is gone. The figure lists its missing levels
either way (``ResolvedPlot.missing_x``) so the GUI offers the box only when it
would change something, and the exported code draws the same ticks as the
preview under both settings.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from scistackplot import PlotKind, PlotSpec, Role, generate_plot_function, resolve
from scistackplot.codegen import default_function_name
from scistackplot.resolved import X
from scistackplot.spec import Filter
from scistackplot.table import LongTable
from scistackplot.xaxis import LEAF_SEPARATOR, is_spacer

SESSIONS = ["pre", "mid", "post"]
SIDES = ["L", "R"]


@pytest.fixture
def table() -> LongTable:
    rows = []
    for subject in ["01", "02"]:
        for session in SESSIONS:
            if subject == "02" and session == "mid":
                continue  # subject 02 never ran mid
            for side in SIDES:
                for trial in ["1", "2"]:
                    rows.append(
                        {
                            "subject": subject,
                            "session": session,
                            "side": side,
                            "trial": trial,
                            "StepLength": 1.0 + len(rows) * 0.1,
                        }
                    )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session", "side", "trial"],
        measures=["StepLength"],
        level_order={"session": SESSIONS, "side": SIDES},
        schema_levels=["subject", "session", "side", "trial"],
    )


def _flat(**changes) -> PlotSpec:
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.ITERATE,
            "session": Role.GROUP,
            "side": Role.COLLAPSE,
            "trial": Role.COLLAPSE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
    )
    return replace(spec, **changes)


def _nested(**changes) -> PlotSpec:
    # side ticks inside session brackets (groups are innermost first).
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.ITERATE,
            "session": Role.GROUP,
            "side": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["side", "session"],
        kind=PlotKind.BAR,
    )
    return replace(spec, **changes)


def _figure(spec: PlotSpec, table: LongTable, subject: str):
    [figure] = [f for f in resolve(spec, table) if f.figure_key.get("subject") == subject]
    return figure


def _ticks(figure) -> list[str]:
    """The preview's leaves, spacers dropped, as the export names them."""
    return [
        str(v).replace(LEAF_SEPARATOR, " · ")
        for v in figure.x_order or []
        if not is_spacer(str(v))
    ]


# --- the spec field ---------------------------------------------------------


def test_defaults_on_and_round_trips():
    assert _flat().show_missing_levels is True
    off = _flat(show_missing_levels=False)
    assert PlotSpec.from_dict(off.to_dict()).show_missing_levels is False
    raw = _flat().to_dict()
    raw.pop("show_missing_levels")
    assert PlotSpec.from_dict(raw).show_missing_levels is True, "absent means on"


# --- the preview ------------------------------------------------------------


def test_a_flat_figure_keeps_an_empty_tick_for_a_missing_level(table):
    figure = _figure(_flat(), table, "02")
    assert _ticks(figure) == ["pre", "mid", "post"]
    assert figure.missing_x == ["mid"]
    rows = pd.concat([p.frame for p in figure.panels])
    assert "mid" not in set(rows[X].astype(str)), "the tick is empty: no row draws there"


def test_off_leaves_the_missing_level_off_but_still_lists_it(table):
    figure = _figure(_flat(show_missing_levels=False), table, "02")
    assert _ticks(figure) == ["pre", "post"]
    assert figure.missing_x == ["mid"], "listed either way, so the GUI can offer the box"


def test_a_complete_figure_has_nothing_missing(table):
    for on in (True, False):
        figure = _figure(_flat(show_missing_levels=on), table, "01")
        assert _ticks(figure) == SESSIONS
        assert figure.missing_x == []


def test_a_filtered_out_level_is_not_missing(table):
    spec = _flat(filters=[Filter(column="session", exclude=["mid"])])
    for subject in ("01", "02"):
        figure = _figure(spec, table, subject)
        assert _ticks(figure) == ["pre", "post"]
        assert figure.missing_x == []


def test_a_nested_figure_keeps_empty_leaves(table):
    figure = _figure(_nested(), table, "02")
    assert _ticks(figure) == [f"{s} · {d}" for s in SESSIONS for d in SIDES]
    assert figure.missing_x == ["mid / L", "mid / R"]
    # The bracket over the missing session is still drawn.
    assert "mid" in [group.label for group in figure.x_plan.groups]

    off = _figure(_nested(show_missing_levels=False), table, "02")
    assert _ticks(off) == [f"{s} · {d}" for s in ["pre", "post"] for d in SIDES]
    assert off.missing_x == ["mid / L", "mid / R"]


def test_the_figure_reports_missing_levels_to_the_gui(table):
    from scistackplot.render.plotly_ import render as render_plotly

    figure = _figure(_flat(), table, "02")
    assert figure.to_dict()["missing_x"] == ["mid"]
    assert render_plotly(figure)["layout"]["meta"]["missing_x"] == ["mid"]


def test_the_choice_is_logged(table, caplog):
    import logging

    with caplog.at_level(logging.INFO):
        _figure(_flat(), table, "02")
    assert any("no data in this figure" in r.getMessage() for r in caplog.records)


# --- the export draws what the preview draws --------------------------------


def _exported_ticks(spec: PlotSpec, table: LongTable, subject: str) -> list[str]:
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")

    namespace: dict = {}
    exec(compile(generate_plot_function(spec, table), "<generated>", "exec"), namespace)  # noqa: S102
    rows = table.frame[table.frame["subject"] == subject].copy()
    fig = namespace[default_function_name(spec)](rows, "figure.png")
    try:
        return [t.get_text() for t in fig.axes[0].get_xticklabels()]
    finally:
        matplotlib.pyplot.close(fig)


@pytest.mark.parametrize("on", [True, False])
@pytest.mark.parametrize("make", [_flat, _nested], ids=["flat", "nested"])
def test_export_matches_preview(table, make, on):
    spec = make(show_missing_levels=on)
    for subject in ("01", "02"):
        assert _exported_ticks(spec, table, subject) == _ticks(_figure(spec, table, subject))


def test_the_export_bakes_in_the_plot_levels_only_when_on(table):
    on = generate_plot_function(_flat(), table)
    off = generate_plot_function(_flat(show_missing_levels=False), table)
    assert "order=_x_order" in on and "order=_x_order" in off
    assert "'mid'" in on.split("_x_order = ", 1)[1].splitlines()[0]
    assert "_x_order = _in_order(df['session']" in off
