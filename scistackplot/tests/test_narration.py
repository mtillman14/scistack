"""What a long resolve says about itself WHILE it runs.

A full-resolution resolve of two 1-D figures took 1543 s (scidb.log 2026-09-11,
12:28 -> 12:54) and emitted exactly one line — on exit. Every save that timed
out, and every save still running when the log was read, was therefore
indistinguishable from a hang: no phase, no figure number, no panel.

These tests pin the narration that fixes that. They assert on log TEXT, which is
unusual and deliberate: the text is the diagnostic interface, the phase names are
what a future reader greps for, and renaming one silently is exactly the
regression worth catching.
"""

from __future__ import annotations

import logging

import pytest

from scistackplot import PlotKind, PlotSpec, Role, resolve, resolve_one

LAYER = "scistackplot"

#: Every phase `_build_figure` reports, in the order it reports them. A phase
#: that disappears from this list has stopped being measurable.
#:
#: No `y_limits` here on purpose: the limits are computed once for the WHOLE
#: fan-out, inside the `plan` phase, because a scope of `[]` means one range
#: across figures this function never sees (`scistackplot.ylimits`). What is
#: left per figure is reading the panels' own limits back, which is not work.
#:
#: No `collapse_levels` for this spec: a nested 1-D measure takes ONE of
#: three per-sample routes (`explode`, `collapse_series` when a factor is
#: collapsed, `summarize_series` inside `panel_frames` for BAND/BAR), and the
#: pandas collapse phase only runs for measures that were never nested.
#: `test_a_collapsed_line_narrates_the_collapse` pins the second route.
BUILD_PHASES = [
    "explode",
    "facet_groups",
    "panel_frames",
    "grid_layout",
]


def _lines(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records]


@pytest.fixture
def fanout_spec() -> PlotSpec:
    """A fan-out of a 1-D measure — the shape that gets slow."""
    return PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session"],
        color="session",
        kind=PlotKind.LINE,
    )


def test_narrated_resolve_names_every_phase_of_every_figure(
    series_table, fanout_spec, caplog
):
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table, narrate=True)

    text = "\n".join(_lines(caplog))
    for phase in BUILD_PHASES:
        assert f"{phase} started" in text, f"phase {phase!r} never announced itself"
        assert f"{phase} done in" in text


def test_a_collapsed_line_narrates_the_collapse(series_table, caplog):
    """A pre-collapse sends a 1-D line through `collapse_series` instead of
    `explode` — that phase must announce itself the same way. Two collapsed
    keys: with only one, it is the sample and the line draws it unaveraged
    (one line per trial), which is the explode path."""
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.LINE,
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(spec, series_table, narrate=True)

    text = "\n".join(_lines(caplog))
    assert "collapse_series started" in text
    assert "collapse_series done in" in text
    assert "explode started" not in text


def test_narration_says_which_figure_before_building_it(
    series_table, fanout_spec, caplog
):
    """The figure number has to arrive BEFORE the work, not after.

    Reported after, a two-figure fan-out is silent for the whole first figure —
    which is half the wait and the half a stuck save sits in.
    """
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table, narrate=True)

    lines = _lines(caplog)
    starting = [i for i, line in enumerate(lines) if "figure 1/3" in line]
    exploding = [i for i, line in enumerate(lines) if "explode started" in line]
    assert starting, "no figure heartbeat"
    assert starting[0] < exploding[0]


def test_narration_counts_panels_within_the_slow_phase(
    series_table, caplog
):
    """A figure that stops has stopped in a specific panel; the log has to say
    which. `panel_frames` is where a full-resolution figure spends its time."""
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.FACET, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject"],
        color="subject",
        kind=PlotKind.LINE,
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(spec, series_table, narrate=True)

    text = "\n".join(_lines(caplog))
    assert "panel 1/2" in text
    assert "panel 2/2" in text


def test_resolve_announces_the_work_before_starting_it(
    series_table, fanout_spec, caplog
):
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table, narrate=True)

    lines = _lines(caplog)
    assert any(
        "resolving line of 'Signal': 3 figure(s)" in line
        and "at full resolution (no downsampling)" in line
        for line in lines
    )
    # …and the existing past-tense summary still closes it out.
    assert any(line.startswith("resolved line of 'Signal'") for line in lines)


def test_on_figure_fires_as_each_figure_starts(series_table, fanout_spec):
    seen = []
    resolve(
        fanout_spec,
        series_table,
        on_figure=lambda position, total, label: seen.append((position, total, label)),
    )

    assert seen == [
        (1, 3, "subject=01"),
        (2, 3, "subject=02"),
        (3, 3, "subject=03"),
    ]


def test_the_quiet_path_stays_quiet(series_table, fanout_spec, caplog):
    """Narration is opt-in, NOT implied by full resolution.

    A pipeline `plot_` endpoint also resolves with `max_points=None`, once per
    iteration; a 500-iteration run does not want eight lines each. The
    interactive path is quiet for the same reason — at 20k rows the narration
    would outnumber the work.
    """
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(fanout_spec, series_table)
        resolve_one(fanout_spec, series_table, 0, max_points=20_000)

    text = "\n".join(_lines(caplog))
    assert "started" not in text
    assert "panel 1/" not in text
    # One summary per call is still emitted, as before.
    assert text.count("[timing] resolve:") == 1
    assert text.count("[timing] resolve_one:") == 1


def test_resolve_one_narrates_the_single_figure_it_builds(
    series_table, fanout_spec, caplog
):
    """The indexed save takes this path, and it is 12 minutes of work."""
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve_one(fanout_spec, series_table, 1, narrate=True)

    text = "\n".join(_lines(caplog))
    assert "figure 2 of 3 (subject=02) at full resolution" in text
    assert "panel_frames started" in text
