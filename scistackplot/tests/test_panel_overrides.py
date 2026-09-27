"""
Per-panel overrides, Stage 1 (2026-09-27): the spec fields and the one
matcher. ``scistackplot.panels`` owns which override belongs to which panel;
these tests pin its identity rules (plan D1: facet values as TEXT, exact
match, last wins) and the spec round trip. Limits, titles, export and GUI
are later stages (``.claude/plan-per-panel-overrides.md``).
"""

from __future__ import annotations

import logging
import math

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, StyleOptions, resolve
from scistackplot.panels import (
    Y_TITLES,
    Y_TITLES_EVERY_PANEL,
    Y_TITLES_FIRST_COLUMN,
    PanelOverride,
    override_for,
    panel_key_text,
    unmatched,
    y_titles_problem,
)
from scistackplot.roles import RoleError

LAYER = "scistackplot"


def _spec(*overrides: PanelOverride, **style) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        kind=PlotKind.BAR,
        roles={"muscle": Role.FACET, "subject": Role.COLLAPSE},
        panel_overrides=list(overrides),
        style=StyleOptions(**style),
    )


# --- key text ------------------------------------------------------------------


def test_key_text_keeps_zero_padding():
    assert panel_key_text("01") == "01"


def test_key_text_spells_a_missing_level_as_nan():
    assert panel_key_text(None) == "nan"
    assert panel_key_text(math.nan) == "nan"


def test_key_text_is_str_of_a_number():
    assert panel_key_text(3) == "3"
    assert panel_key_text(2.5) == "2.5"


# --- matching ------------------------------------------------------------------


def test_for_key_stores_the_values_as_text():
    override = PanelOverride.for_key({"session": 1}, y_maximum=4.0)
    assert override.match == {"session": "1"}
    assert override.matches({"session": 1})


def test_an_exact_key_matches():
    override = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=400.0)
    assert override_for(_spec(override), {"muscle": "RQUAD"}) == override


def test_a_different_level_does_not_match():
    override = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=400.0)
    assert override_for(_spec(override), {"muscle": "LQUAD"}) is None


def test_a_partial_key_does_not_match():
    override = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=400.0)
    spec = _spec(override)
    assert override_for(spec, {"muscle": "RQUAD", "side": "L"}) is None
    wider = PanelOverride(match={"muscle": "RQUAD", "side": "L"}, y_maximum=1.0)
    assert override_for(_spec(wider), {"muscle": "RQUAD"}) is None


def test_zero_padded_text_does_not_match_the_integer_text():
    """``"01"`` is a level of its own; it is not the level ``1``."""
    override = PanelOverride(match={"session": "01"}, y_label="S1")
    spec = _spec(override)
    assert override_for(spec, {"session": "01"}) == override
    assert override_for(spec, {"session": 1}) is None


def test_an_unfaceted_panel_takes_no_override():
    """An empty match names no panel: the figure's own settings cover it."""
    override = PanelOverride(match={}, y_label="anything")
    assert override_for(_spec(override), {}) is None


def test_an_empty_override_is_ignored():
    empty = PanelOverride(match={"muscle": "RQUAD"})
    assert empty.is_empty
    assert override_for(_spec(empty), {"muscle": "RQUAD"}) is None


def test_hidden_false_is_not_empty():
    """``y_label_hidden=False`` forces the title ON against the grid toggle,
    so it is a setting, not an inherit."""
    assert not PanelOverride(match={"muscle": "RQUAD"}, y_label_hidden=False).is_empty


def test_the_last_match_wins_and_warns(caplog):
    first = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=1.0)
    last = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=2.0)
    with caplog.at_level(logging.WARNING, logger=LAYER):
        assert override_for(_spec(first, last), {"muscle": "RQUAD"}) == last
    assert "2 entries match panel muscle=RQUAD" in caplog.text


def test_unmatched_lists_the_inert_overrides():
    here = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=1.0)
    gone = PanelOverride(match={"muscle": "RHAM"}, y_maximum=1.0)
    empty = PanelOverride(match={"muscle": "XYZ"})
    spec = _spec(here, gone, empty)
    assert unmatched(spec, [{"muscle": "RQUAD"}, {"muscle": "LQUAD"}]) == [gone]


# --- spec round trip -------------------------------------------------------------


def test_overrides_round_trip_through_json():
    spec = _spec(
        PanelOverride(match={"muscle": "RQUAD"}, y_minimum=0.0, y_maximum=400.0),
        PanelOverride(match={"muscle": "LHAM"}, y_label="LHAM (uV)", y_label_hidden=True),
        PanelOverride(match={"session": "01"}, y_label_hidden=False),
        y_titles=Y_TITLES_FIRST_COLUMN,
    )
    back = PlotSpec.from_json(spec.to_json())
    assert back.panel_overrides == spec.panel_overrides
    assert back.style.y_titles == Y_TITLES_FIRST_COLUMN
    assert back == spec


def test_overrides_round_trip_through_toml():
    pytest.importorskip("tomli_w")
    spec = _spec(
        PanelOverride(match={"muscle": "RQUAD"}, y_maximum=400.0),
        PanelOverride(match={"session": "01"}, y_label="S 01"),
    )
    back = PlotSpec.from_toml(spec.to_toml())
    assert back.panel_overrides == spec.panel_overrides
    # "01" is still a string after TOML.
    assert back.panel_overrides[1].match == {"session": "01"}


def test_a_spec_without_the_fields_reads_as_the_defaults():
    raw = _spec().to_dict()
    raw.pop("panel_overrides", None)
    raw["style"].pop("y_titles", None)
    back = PlotSpec.from_dict(raw)
    assert back.panel_overrides == []
    assert back.style.y_titles == Y_TITLES_EVERY_PANEL


def test_unset_fields_are_dropped_from_the_dict():
    """TOML has no null: an inheriting field is absent, not None."""
    raw = _spec(PanelOverride(match={"muscle": "RQUAD"}, y_maximum=1.0)).to_dict()
    assert raw["panel_overrides"] == [{"match": {"muscle": "RQUAD"}, "y_maximum": 1.0}]


# --- y_titles validation ---------------------------------------------------------


@pytest.mark.parametrize("good", Y_TITLES)
def test_y_titles_problem_accepts_the_known_values(good):
    assert y_titles_problem(good) is None


@pytest.mark.parametrize("bad", ["first_row", "", None, "First_Column"])
def test_y_titles_problem_names_bad_values(bad):
    assert y_titles_problem(bad) is not None


def test_validate_refuses_an_unknown_y_titles():
    frame = pd.DataFrame(
        [("01", "RQUAD", 1.0), ("02", "LQUAD", 2.0)], columns=["subject", "muscle", "M"]
    )
    table = LongTable.from_frame(
        frame, factors=["subject", "muscle"], measures=["M"], name="M",
        schema_levels=["subject"],
    )
    with pytest.raises(RoleError, match="style.y_titles"):
        resolve(_spec(y_titles="first_row"), table)


# =================================================================================
# Stage 2: limits in the preview (D2: panel end > figure end > computed)
# =================================================================================

import scistackplot.reduce as reduce_mod  # noqa: E402
from scistackplot import YAxis, resolve_one  # noqa: E402
from scistackplot.render.base import shares_y_axis  # noqa: E402
from scistackplot.ylimits import limits_for, pinned_ends  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()


@pytest.fixture
def emg_table() -> LongTable:
    """Two subjects x two muscles on different scales (as in test_ylimits):
    subject 02 is 100x subject 01, SOL is a tenth of TA."""
    rows = []
    for subject, scale in (("01", 1.0), ("02", 100.0)):
        for muscle, factor in (("TA", 1.0), ("SOL", 0.1)):
            for trial, wobble in (("1", 0.8), ("2", 1.0)):
                peak = scale * factor * wobble
                rows.append(
                    {"subject": subject, "muscle": muscle, "trial": trial,
                     "EMG": [0.0, 0.5 * peak, peak]}
                )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "muscle", "trial"],
    )


def _emg(*overrides: PanelOverride, y_axis: YAxis | None = None) -> PlotSpec:
    """A figure per subject, a panel per muscle, one line per trial."""
    return PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.ITERATE, "muscle": Role.FACET, "trial": Role.GROUP},
        kind=PlotKind.LINE,
        y_axis=y_axis or YAxis(),
        panel_overrides=list(overrides),
    )


def _limits_by_panel(figures) -> dict[tuple, tuple]:
    return {
        (figure.figure_key.get("subject"), panel.key["muscle"]): panel.y_limits
        for figure in figures
        for panel in figure.panels
    }


def test_limits_for_panel_both_ends_need_no_data():
    both = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=5.0)
    assert limits_for({}, {"muscle": "SOL"}, [], YAxis(), both) == (0.0, 5.0)


def test_limits_for_panel_one_end_without_data_autoscales():
    one = PanelOverride(match={"muscle": "SOL"}, y_maximum=5.0)
    assert limits_for({}, {"muscle": "SOL"}, [], YAxis(), one) is None


def test_limits_for_orders_swapped_panel_ends():
    swapped = PanelOverride(match={"muscle": "SOL"}, y_minimum=5.0, y_maximum=0.0)
    assert limits_for({}, {"muscle": "SOL"}, [], YAxis(), swapped) == (0.0, 5.0)


def test_pinned_ends_panel_over_figure_end_by_end():
    figure = YAxis(minimum=-1.0, maximum=9.0)
    panel = PanelOverride(match={"muscle": "SOL"}, y_maximum=5.0)
    assert pinned_ends(figure, panel) == (-1.0, 5.0)
    assert pinned_ends(figure, None) == (-1.0, 9.0)
    assert pinned_ends(YAxis(), None) == (None, None)


def test_an_override_changes_only_its_panel_in_every_figure(emg_table):
    """D1: matched on the facet value alone, so SOL is overridden in BOTH
    subjects' figures; TA keeps the dataset-wide range."""
    baseline = _limits_by_panel(resolve(_emg(), emg_table))
    sol = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=5.0)
    figures = resolve(_emg(sol), emg_table)
    got = _limits_by_panel(figures)

    assert got[("01", "SOL")] == (0.0, 5.0)
    assert got[("02", "SOL")] == (0.0, 5.0)
    assert got[("01", "TA")] == baseline[("01", "TA")]
    assert got[("02", "TA")] == baseline[("02", "TA")]
    # The panels now differ, so no figure shares one axis: every panel shows
    # its own tick numbers (render.base.shows_y_labels).
    for figure in figures:
        assert figure.y_limits is None
        assert not shares_y_axis(figure)


def test_one_panel_end_keeps_the_computed_other_end(emg_table):
    baseline = _limits_by_panel(resolve(_emg(), emg_table))
    top = PanelOverride(match={"muscle": "SOL"}, y_maximum=500.0)
    got = _limits_by_panel(resolve(_emg(top), emg_table))

    assert got[("01", "SOL")] == (baseline[("01", "SOL")][0], 500.0)


def test_the_panel_end_beats_the_figure_end(emg_table):
    figure = YAxis(minimum=-10.0, maximum=10.0)
    top = PanelOverride(match={"muscle": "SOL"}, y_maximum=3.0)
    got = _limits_by_panel(resolve(_emg(top, y_axis=figure), emg_table))

    assert got[("01", "SOL")] == (-10.0, 3.0)
    assert got[("01", "TA")] == (-10.0, 10.0)


def test_an_override_for_a_missing_panel_changes_nothing(emg_table, caplog):
    baseline = _limits_by_panel(resolve(_emg(), emg_table))
    gone = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=1.0)
    with caplog.at_level(logging.INFO, logger=LAYER):
        got = _limits_by_panel(resolve(_emg(gone), emg_table))

    assert got == baseline
    assert "0 applied (none), 1 not in this figure (muscle=RQUAD)" in caplog.text


def test_applied_overrides_are_logged_with_their_limits(emg_table, caplog):
    sol = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=5.0)
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(_emg(sol), emg_table)

    assert "panel overrides [subject=01]: 1 applied (muscle=SOL y 0 to 5)" in caplog.text


def test_no_override_logs_nothing(emg_table, caplog):
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(_emg(), emg_table)
    assert "panel overrides" not in caplog.text


def test_changing_only_an_override_reuses_the_plan_and_draws_the_new_one(
    emg_table, monkeypatch
):
    """`panel_overrides` is plan-irrelevant: a typed Max re-renders, never
    re-plans. The trap that sets (`_build_figure` reads `plan.spec`) is that a
    cached plan would draw the FIRST override; `_with_presentation` puts the
    new one back."""
    builds = []
    original = reduce_mod._build_plan

    def spy(spec, table):
        builds.append(1)
        return original(spec, table)

    monkeypatch.setattr(reduce_mod, "_build_plan", spy)

    first = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=5.0)
    second = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=7.0)
    figure_a = resolve_one(_emg(first), emg_table, 0)[0]
    figure_b = resolve_one(_emg(second), emg_table, 0)[0]
    figure_c = resolve_one(_emg(), emg_table, 0)[0]

    assert len(builds) == 1
    sol = lambda figure: next(p for p in figure.panels if p.key["muscle"] == "SOL")  # noqa: E731
    assert sol(figure_a).y_limits == (0.0, 5.0)
    assert sol(figure_b).y_limits == (0.0, 7.0)
    assert sol(figure_c).y_limits != (0.0, 7.0)


# =================================================================================
# Stage 3: the y title (D5 hide, D6 first-column toggle)
# =================================================================================

from scistackplot import FacetOptions, render_plotly  # noqa: E402
from scistackplot.render.base import is_leftmost, panel_y_title  # noqa: E402


@pytest.fixture
def row_table() -> LongTable:
    """Three muscles, one scale: a 1 x 3 grid whose panels share one axis, so
    the inner panels carry a y title and no tick numbers."""
    rows = [
        {"subject": s, "muscle": m, "trial": t, "EMG": [0.0, 1.0, 2.0]}
        for s in ("01", "02")
        for m in ("TA", "SOL", "GAS")
        for t in ("1", "2")
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "muscle", "trial"],
        level_order={"muscle": ["TA", "SOL", "GAS"]},
    )


def _row(*overrides: PanelOverride, facet=None, **style) -> PlotSpec:
    return PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.GROUP, "muscle": Role.FACET, "trial": Role.GROUP},
        kind=PlotKind.LINE,
        facet=facet or FacetOptions(),
        style=StyleOptions(**style),
        panel_overrides=list(overrides),
    )


def _titles(resolved) -> dict[str, str]:
    return {
        panel.key["muscle"]: panel_y_title(resolved, panel, leftmost=True)
        for panel in resolved.panels
    }


def test_every_panel_shows_its_facet_text_by_default(row_table):
    resolved = resolve(_row(), row_table)[0]
    assert _titles(resolved) == {"TA": "TA", "SOL": "SOL", "GAS": "GAS"}


def test_an_override_replaces_one_panels_title_as_typed(row_table):
    sol = PanelOverride(match={"muscle": "SOL"}, y_label="Soleus (uV)")
    resolved = resolve(_row(sol), row_table)[0]
    assert _titles(resolved) == {"TA": "TA", "SOL": "Soleus (uV)", "GAS": "GAS"}


def test_a_hidden_title_is_empty_and_keeps_its_text(row_table):
    sol = PanelOverride(match={"muscle": "SOL"}, y_label="Soleus", y_label_hidden=True)
    resolved = resolve(_row(sol), row_table)[0]
    assert _titles(resolved)["SOL"] == ""
    # Showing it again brings the typed text back.
    shown = resolve(_row(PanelOverride(match={"muscle": "SOL"}, y_label="Soleus")), row_table)[0]
    assert _titles(shown)["SOL"] == "Soleus"


def test_first_column_keeps_only_the_first_columns_titles(row_table):
    resolved = resolve(_row(y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0]
    assert _titles(resolved) == {"TA": "TA", "SOL": "", "GAS": ""}


def test_a_panel_forced_on_beats_the_first_column_toggle(row_table):
    gas = PanelOverride(match={"muscle": "GAS"}, y_label_hidden=False)
    resolved = resolve(_row(gas, y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0]
    assert _titles(resolved) == {"TA": "TA", "SOL": "", "GAS": "GAS"}


def test_a_wrapped_grid_counts_the_second_rows_first_panel_as_first(row_table):
    """3 panels in 2 columns: (0,0) (0,1) / (1,0)."""
    resolved = resolve(
        _row(facet=FacetOptions(n_cols=2), y_titles=Y_TITLES_FIRST_COLUMN), row_table
    )[0]
    at = {(p.grid_row, p.grid_col): p.key["muscle"] for p in resolved.panels}
    assert is_leftmost(resolved, 1, 0) and not is_leftmost(resolved, 0, 1)
    titles = _titles(resolved)
    assert titles[at[(1, 0)]] == at[(1, 0)]
    assert titles[at[(0, 1)]] == ""


def test_an_unfaceted_figure_ignores_overrides_and_the_toggle(emg_table):
    spec = PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.ITERATE, "muscle": Role.ITERATE, "trial": Role.GROUP},
        kind=PlotKind.LINE,
        style=StyleOptions(y_titles=Y_TITLES_FIRST_COLUMN),
        panel_overrides=[PanelOverride(match={}, y_label_hidden=True)],
    )
    resolved = resolve(spec, emg_table)[0]
    (panel,) = resolved.panels
    assert panel_y_title(resolved, panel, leftmost=True) == resolved.labels.y


def test_both_renderers_draw_the_decided_titles(row_table):
    pytest.importorskip("matplotlib")
    import matplotlib.pyplot as plt

    from scistackplot import render_matplotlib

    sol = PanelOverride(match={"muscle": "SOL"}, y_label="Soleus")
    resolved = resolve(_row(sol, y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0]
    figure = render_matplotlib(resolved)
    try:
        assert [ax.get_ylabel() for ax in figure.axes[:3]] == ["TA", "", ""]
    finally:
        plt.close(figure)
    layout = render_plotly(resolved)["layout"]
    assert [layout[k]["title"]["text"] for k in ("yaxis", "yaxis2", "yaxis3")] == [
        "TA", "", ""
    ]
    # Forced on, the override's text is drawn.
    forced = PanelOverride(match={"muscle": "SOL"}, y_label="Soleus", y_label_hidden=False)
    resolved = resolve(_row(forced, y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0]
    assert render_plotly(resolved)["layout"]["yaxis2"]["title"]["text"] == "Soleus"


def test_meta_reports_the_drawn_title_the_override_and_the_unmatched(row_table):
    sol = PanelOverride(match={"muscle": "SOL"}, y_label="Soleus")
    gone = PanelOverride(match={"muscle": "RQUAD"}, y_maximum=1.0)
    meta = resolve(_row(sol, gone, y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0].to_dict()
    by_muscle = {p["key"]["muscle"]: p for p in meta["panels"]}

    assert by_muscle["TA"]["y_title"] == "TA"
    assert by_muscle["SOL"]["y_title"] == ""  # the toggle hides column 2
    assert by_muscle["SOL"]["override"]["y_label"] == "Soleus"
    assert by_muscle["TA"]["override"] is None
    assert [o["match"] for o in meta["unmatched_overrides"]] == [{"muscle": "RQUAD"}]



# =================================================================================
# Stage 5: what the GUI's Panels section reads (layout.meta.panel_overrides)
# =================================================================================


def test_plotly_meta_lists_panels_with_text_matches_and_drawn_state(row_table):
    sol = PanelOverride(match={"muscle": "SOL"}, y_minimum=0.0, y_maximum=9.0, y_label="Soleus")
    gone = PanelOverride(match={"muscle": "RQUAD"}, y_label_hidden=True)
    resolved = resolve(_row(sol, gone, y_titles=Y_TITLES_FIRST_COLUMN), row_table)[0]
    meta = render_plotly(resolved)["layout"]["meta"]["panel_overrides"]

    by_title = {p["display_title"]: p for p in meta["panels"]}
    assert [p["match"] for p in meta["panels"]] == [
        {"muscle": "TA"}, {"muscle": "SOL"}, {"muscle": "GAS"}
    ]
    assert by_title["TA"]["y_title"] == "TA"
    assert by_title["SOL"]["y_title"] == ""  # first column only
    assert by_title["SOL"]["y_limits"] == [0.0, 9.0]
    assert by_title["SOL"]["override"]["y_label"] == "Soleus"
    assert by_title["GAS"]["override"] is None
    assert [o["match"] for o in meta["unmatched"]] == [{"muscle": "RQUAD"}]
    assert meta["y_titles"] == Y_TITLES_FIRST_COLUMN
    assert meta["shares_y"] is False


def test_plotly_meta_for_an_unfaceted_figure_lists_no_panels(emg_table):
    spec = PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.ITERATE, "muscle": Role.ITERATE, "trial": Role.GROUP},
        kind=PlotKind.LINE,
    )
    meta = render_plotly(resolve(spec, emg_table)[0])["layout"]["meta"]["panel_overrides"]
    assert meta["panels"] == []
    assert meta["shares_y"] is True
