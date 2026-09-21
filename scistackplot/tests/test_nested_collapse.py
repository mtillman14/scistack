"""
The collapse chain, numerically: nested and unweighted by default, pooled on
request, and the outermost collapsed key is the sample.

The fixture is deliberately UNBALANCED — subject 01 has three trials, subject
02 has one — because on a balanced design the nested mean and the pooled mean
coincide and nothing here would be able to tell them apart:

    subject 01: trials {1, 2, 3}   -> per-subject mean 2
    subject 02: trials {9}         -> per-subject mean 9

    nested (default): mean of {2, 9} = 5.5,  SD over {2, 9}
    pooled ("weight by N"): mean of {1, 2, 3, 9} = 3.75, SD over four values

See docs/claude/grouping-and-collapse.md.
"""

from __future__ import annotations

from dataclasses import replace

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
    resolve,
)
from scistackplot.resolved import COLOR, SERIES, X, Y, Y_HIGH, Y_LOW


@pytest.fixture
def unbalanced() -> LongTable:
    rows = [
        ("01", "pre", "1", 1.0),
        ("01", "pre", "2", 2.0),
        ("01", "pre", "3", 3.0),
        ("02", "pre", "1", 9.0),
        # A second session so a tick layer has something to separate.
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
        schema_levels=["subject", "session", "trial"],
    )


def _spec(kind, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _panel(spec, table) -> pd.DataFrame:
    (figure,) = resolve(spec, table)
    (panel,) = figure.panels
    return panel.frame


def _row(frame, x) -> pd.Series:
    rows = frame[frame[X] == x]
    assert len(rows) == 1, f"expected one mark at {x!r}, got {len(rows)}"
    return rows.iloc[0]


# --- nested is the default ---------------------------------------------------


def test_bar_is_the_mean_of_per_subject_means(unbalanced):
    frame = _panel(_spec(PlotKind.BAR), unbalanced)
    pre = _row(frame, "pre")
    assert pre[Y] == pytest.approx(5.5), "mean of {2, 9}, not of {1,2,3,9}"


def test_bar_error_is_the_spread_across_subjects(unbalanced):
    frame = _panel(_spec(PlotKind.BAR), unbalanced)
    pre = _row(frame, "pre")
    sd = np.std([2.0, 9.0], ddof=1)
    assert pre[Y_HIGH] - pre[Y] == pytest.approx(sd)
    assert pre[Y] - pre[Y_LOW] == pytest.approx(sd)


def test_nested_differs_from_pooled(unbalanced):
    nested = _panel(_spec(PlotKind.BAR), unbalanced)
    pooled = _panel(
        _spec(PlotKind.BAR, aggregate=Aggregation(error=ErrorBand.SD, pooled=True)),
        unbalanced,
    )
    assert _row(nested, "pre")[Y] == pytest.approx(5.5)
    assert _row(pooled, "pre")[Y] == pytest.approx(3.75)
    sd = np.std([1.0, 2.0, 3.0, 9.0], ddof=1)
    assert _row(pooled, "pre")[Y_HIGH] - _row(pooled, "pre")[Y] == pytest.approx(sd)


def test_box_distribution_is_the_per_subject_means(unbalanced):
    """A box draws the sample rows: one per subject, each its trial mean."""
    frame = _panel(_spec(PlotKind.BOX), unbalanced)
    pre = sorted(frame[frame[X] == "pre"][Y].tolist())
    assert pre == pytest.approx([2.0, 9.0])


def test_pooled_box_distribution_is_every_trial(unbalanced):
    frame = _panel(
        _spec(PlotKind.BOX, aggregate=Aggregation(pooled=True)), unbalanced
    )
    pre = sorted(frame[frame[X] == "pre"][Y].tolist())
    assert pre == pytest.approx([1.0, 2.0, 3.0, 9.0])


def test_scatter_draws_the_sample_itself(unbalanced):
    """Schema-level parity (2026-09-19): a scatter draws the sample — one
    point per subject, each its trial mean — exactly the rows the bar's
    5.5 ± SD is computed over. It never averages the subjects too."""
    frame = _panel(_spec(PlotKind.SCATTER), unbalanced)
    pre = sorted(frame[frame[X] == "pre"][Y].tolist())
    assert pre == pytest.approx([2.0, 9.0])


def test_strip_and_box_draw_the_same_sample(unbalanced):
    strip = _panel(_spec(PlotKind.STRIP), unbalanced)
    box = _panel(_spec(PlotKind.BOX), unbalanced)
    assert sorted(strip[Y]) == pytest.approx(sorted(box[Y]))


def test_dict_order_does_not_change_the_numbers(unbalanced):
    """Which role the user clicked first is not a statistical decision."""
    a = _spec(PlotKind.BAR)
    b = _spec(
        PlotKind.BAR,
        roles={"trial": Role.COLLAPSE, "session": Role.GROUP, "subject": Role.COLLAPSE},
    )
    assert _row(_panel(a, unbalanced), "pre")[Y] == _row(_panel(b, unbalanced), "pre")[Y]


# --- no sample ---------------------------------------------------------------


def test_a_bar_with_nothing_collapsed_has_no_error_bar(unbalanced):
    spec = PlotSpec(
        measures=["M"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session"],
        kind=PlotKind.BAR,
    )
    figures = resolve(spec, unbalanced)
    for figure in figures:
        (panel,) = figure.panels
        frame = panel.frame
        assert (frame[Y_LOW] == frame[Y]).all()
        assert (frame[Y_HIGH] == frame[Y]).all()


def test_only_the_sample_key_widens_the_error_bar(unbalanced):
    """subject grouped + trial collapsed: the bar's sample is the trials of
    that subject, so subject 02 (one trial) has a zero-width bar."""
    spec = PlotSpec(
        measures=["M"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    frame = _panel(spec, unbalanced)
    two = frame[frame["subject"] == "02"]
    assert (two[Y_LOW] == two[Y]).all()
    one_pre = frame[(frame["subject"] == "01") & (frame["session"] == "pre")]
    assert one_pre[Y].item() == pytest.approx(2.0)
    assert one_pre[Y_HIGH].item() - 2.0 == pytest.approx(np.std([1.0, 2.0, 3.0], ddof=1))


# --- colour on an outer layer -------------------------------------------------


def test_the_coloured_layer_keeps_its_place_in_the_nesting(unbalanced):
    """Colouring session on a `[subject, session]` grouping: the bars are the
    SAME four bars at the SAME composed ticks as without the colour — colour
    is paint (2026-09-21) — and `__color` just names what paints each."""
    plain = PlotSpec(
        measures=["M"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.BAR,
    )
    coloured = replace(plain, color="session")
    frame_plain = _panel(plain, unbalanced)
    frame = _panel(coloured, unbalanced)
    assert len(frame) == 4, "one bar per (subject, session)"
    assert set(frame[COLOR]) == {"pre", "post"}
    keep = [X, Y, Y_LOW, Y_HIGH]
    pd.testing.assert_frame_equal(
        frame[keep].sort_values(X).reset_index(drop=True),
        frame_plain[keep].sort_values(X).reset_index(drop=True),
    )


# --- 1-D: one band per leaf group ---------------------------------------------


def test_band_per_uncoloured_series_layer():
    rows = []
    rng = np.random.default_rng(0)
    for subject in ["01", "02"]:
        for trial in ["1", "2", "3"]:
            rows.append(
                {"subject": subject, "trial": trial, "S": list(rng.normal(size=5))}
            )
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "trial"], measures=["S"],
        schema_levels=["subject", "trial"], name="S",
    )
    spec = PlotSpec(
        measures=["S"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAND,
    )
    (figure,) = resolve(spec, table)
    frame = figure.panels[0].frame
    assert figure.encoding.series == SERIES
    assert set(frame[SERIES]) == {"01", "02"}, "one band per subject"
    assert len(frame) == 10, "5 positions x 2 bands"
    one = frame[frame[SERIES] == "01"].sort_values(X)
    expected = np.mean([r["S"] for r in rows if r["subject"] == "01"], axis=0)
    assert one[Y].to_numpy() == pytest.approx(expected)


# --- line kinds: one line per sample level --------------------------------------


@pytest.fixture
def series_by_subject() -> tuple[LongTable, list[dict]]:
    rows = []
    rng = np.random.default_rng(3)
    for group in ["A", "B"]:
        for subject in (["01", "02"] if group == "A" else ["03"]):
            for trial in ["1", "2"]:
                rows.append(
                    {
                        "group": group,
                        "subject": subject,
                        "trial": trial,
                        "S": list(rng.normal(size=4)),
                    }
                )
    table = LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "subject", "trial"],
        measures=["S"],
        schema_levels=["subject", "trial"],
        name="S",
    )
    return table, rows


def test_a_line_draws_one_line_per_subject(series_by_subject):
    """subject + trial collapsed, group coloured: trials average within each
    subject and each SUBJECT is its own line inside its group's colour — not
    one mean line per group (the deleted `final` step)."""
    table, rows = series_by_subject
    spec = PlotSpec(
        measures=["S"],
        roles={"group": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["group"],
        color="group",
        kind=PlotKind.LINE,
    )
    (figure,) = resolve(spec, table)
    frame = figure.panels[0].frame
    assert set(frame[SERIES]) == {"A | 01", "A | 02", "B | 03"}
    one = frame[frame[SERIES] == "A | 01"].sort_values(X)
    expected = np.mean([r["S"] for r in rows if r["subject"] == "01"], axis=0)
    assert one[Y].to_numpy() == pytest.approx(expected)
    assert set(one[COLOR]) == {"A"}


def test_subject_lines_are_never_dashed(series_by_subject):
    """Units are part of the series id and nothing else: thirty subjects
    must not cycle through six dash styles or fill the legend."""
    table, _ = series_by_subject
    spec = PlotSpec(
        measures=["S"],
        roles={"group": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["group"],
        color="group",
        kind=PlotKind.LINE,
    )
    (figure,) = resolve(spec, table)
    assert figure.dash_styles == {}
    assert figure.encoding.dash is None


def test_a_pooled_line_draws_one_line_per_sample_row(series_by_subject):
    """Pooled: every collapsed key is the sample, so each (subject, trial)
    record is its own line."""
    table, _ = series_by_subject
    spec = PlotSpec(
        measures=["S"],
        roles={"group": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["group"],
        kind=PlotKind.LINE,
        aggregate=Aggregation(pooled=True),
    )
    (figure,) = resolve(spec, table)
    assert figure.panels[0].frame[SERIES].nunique() == 6

